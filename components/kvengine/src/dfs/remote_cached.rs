// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{convert::TryFrom, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use http::Uri;
use security::HttpClient;
use tikv_util::time::Instant;
use tokio::runtime::Runtime;
use txn_types::TimeStamp;

use crate::dfs::{Dfs, Options};

pub const DFS_REMOTE_CACHE_ADDR_HEADER: &str = "x-dfs-remote-cache-addr";
const READ_REMOTE_CACHE_TIMEOUT: Duration = Duration::from_secs(10);

pub struct RemoteCachedDfs {
    inner: Arc<dyn Dfs>,
    remote_cache_addr: String,
    remote_cache_ttl: Duration,
    http_client: Arc<HttpClient>,
    scheme: &'static str,
}

impl RemoteCachedDfs {
    pub fn new(
        inner: Arc<dyn Dfs>,
        remote_cache_addr: String,
        http_client: Arc<HttpClient>,
        cache_ttl: Duration,
    ) -> Self {
        let scheme = match http_client.as_ref() {
            HttpClient::Http(_) => "http",
            HttpClient::Https(_) => "https",
        };
        Self {
            inner,
            remote_cache_addr,
            http_client,
            remote_cache_ttl: cache_ttl,
            scheme,
        }
    }

    // We only read the recent files to mitigate compaction/flush caused cache
    // misses. If the file is too old, it means the file is not frequently
    // accessed, we read the file from the inner Dfs directly to void
    // overloading the cache.
    fn cache_expired(&self, file_id: u64) -> bool {
        let now = TimeStamp::physical_now();
        let file_ts = TimeStamp::new(file_id).physical();
        now.saturating_sub(file_ts) > self.remote_cache_ttl.as_millis() as u64
    }

    async fn try_read_remote_cache(&self, file_id: u64, opts: Options) -> Option<Bytes> {
        if self.cache_expired(file_id) {
            debug!("cache expired"; "file_id" => file_id);
            return None;
        }
        let start = Instant::now_coarse();
        let end_off = opts
            .end_off
            .map(|end| format!("&end_off={}", end))
            .unwrap_or_default();
        let uri = Uri::try_from(format!(
            "{}://{}/dfs/{}?file_type={}&start_off={}{}",
            self.scheme, &self.remote_cache_addr, file_id, opts.file_type, opts.start_off, end_off
        ))
        .unwrap();
        let res = tokio::time::timeout(READ_REMOTE_CACHE_TIMEOUT, self.http_client.get(uri)).await;
        if res.is_err() {
            warn!("read from cache timeout";
                "file_id" => file_id,
                "addr" => &self.remote_cache_addr,
            );
            return None;
        }
        let resp_res = res.unwrap();
        if resp_res.is_err() {
            warn!("read cache failed";
                "file_id" => file_id,
                "addr" => &self.remote_cache_addr,
                "err" => ?resp_res.err(),
            );
            return None;
        }
        let resp = resp_res.unwrap();
        if !resp.status().is_success() {
            warn!("read cache failed";
                "file_id" => file_id,
                "addr" => &self.remote_cache_addr,
                "status" => ?resp.status(),
            );
            return None;
        }
        let res = tokio::time::timeout(
            READ_REMOTE_CACHE_TIMEOUT,
            hyper::body::to_bytes(resp.into_body()),
        )
        .await;
        if res.is_err() {
            warn!("read body from cache timeout";
                "file_id" => file_id,
                "addr" => &self.remote_cache_addr,
            );
            return None;
        }
        let body_res = res.unwrap();
        if body_res.is_err() {
            warn!("read cache body failed";
                "file_id" => file_id,
                "addr" => &self.remote_cache_addr,
                "err" => ?body_res.err(),
            );
            return None;
        }
        let body = body_res.unwrap();
        info!(
            "read file from remote cache";
            "file_id" => file_id,
            "addr" => &self.remote_cache_addr,
            "size" => body.len(),
            "takes" => ?start.saturating_elapsed(),
        );
        Some(body)
    }
}

#[async_trait]
impl Dfs for RemoteCachedDfs {
    async fn read_file(&self, file_id: u64, opts: Options) -> crate::dfs::Result<Bytes> {
        if let Some(data) = self.try_read_remote_cache(file_id, opts).await {
            return Ok(data);
        }
        self.inner.read_file(file_id, opts).await
    }

    async fn create(&self, _: u64, _: Bytes, _: Options) -> crate::dfs::Result<()> {
        unreachable!()
    }

    async fn remove(&self, _: u64, _: Option<u64>, _: Options) {
        unreachable!()
    }

    async fn permanently_remove(&self, _: u64, _: Options) -> crate::dfs::Result<()> {
        unreachable!()
    }

    fn get_runtime(&self) -> &Runtime {
        self.inner.get_runtime()
    }

    fn get_prefix(&self) -> String {
        self.inner.get_prefix()
    }
}
