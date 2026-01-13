// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use http::Uri;
use hyper::client::HttpConnector;
use kvengine::{
    dfs,
    dfs::{Dfs, FileType, Options},
};
use kvproto::metapb::{Region, Store};
use pd_client::PdClient;
use rand::seq::SliceRandom;
use tikv_util::{HandyRwLock, debug, error, time::Instant, warn};
use tokio::runtime::Runtime;

const PD_CLIENT_INTERVAL: Duration = Duration::from_secs(2);
const PD_CLIENT_TIMEOUT: Duration = Duration::from_secs(30);

pub struct BuiltinDfs {
    pd: Arc<dyn PdClient>,
    runtime: Runtime,
    http_client: Arc<hyper::Client<HttpConnector>>,
    store_cache: RwLock<HashMap<u64, Store>>,
    region_cache: RwLock<HashMap<u64, Region>>,
    // update_all_stores_single_flight ensure update_all_stores is not executed concurrently.
    update_all_stores_single_flight: tokio::sync::Mutex<dfs::Result<()>>,
}

impl BuiltinDfs {
    pub fn new(pd: Arc<dyn PdClient>) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("builtin_dfs")
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        Self {
            pd,
            runtime,
            http_client: Arc::new(hyper::Client::new()),
            store_cache: RwLock::new(HashMap::new()),
            region_cache: RwLock::new(HashMap::new()),
            update_all_stores_single_flight: tokio::sync::Mutex::new(Ok(())),
        }
    }

    async fn get_store_addr(&self, store_id: u64) -> dfs::Result<String> {
        {
            let guard = self.store_cache.rl();
            if let Some(store) = guard.get(&store_id) {
                // TODO: handle store address refresh.
                return Ok(store.get_status_address().to_string());
            }
        }

        let start_time = Instant::now_coarse();
        let mut retry_count = 0;
        loop {
            match self.pd.get_store_async(store_id).await {
                Err(err) => {
                    debug!(
                        "pd client get store {} failed {} retry {}",
                        store_id, err, retry_count
                    );
                }
                Ok(store) => {
                    let status_address = store.get_status_address().to_string();
                    {
                        let mut guard = self.store_cache.wl();
                        guard.insert(store_id, store);
                    }
                    return Ok(status_address);
                }
            }

            if start_time.saturating_elapsed() > PD_CLIENT_TIMEOUT {
                break;
            }

            retry_count += 1;
            tokio::time::sleep(PD_CLIENT_INTERVAL).await;
        }

        error!(
            "pd client get store {} timed out, retry {}",
            store_id, retry_count
        );
        Err(dfs::Error::Other("get store timed out".to_string()))
    }

    async fn update_all_stores(&self) -> dfs::Result<()> {
        let Ok(mut guard) = self.update_all_stores_single_flight.try_lock() else {
            // If try_lock fails, another thread is already running update_all_stores.
            // Reuse its result directly.
            return self.update_all_stores_single_flight.lock().await.clone();
        };

        let start_time = Instant::now_coarse();
        let mut retry_count = 0;
        loop {
            // retrieves information of all kv stores from PD,
            // excluding Tombstone stores and TiFlash.
            match pd_client::util::get_all_stores_except_tiflash_async(self.pd.as_ref(), true).await
            {
                Err(err) => {
                    debug!(
                        "pd client get all stores failed {} retry {}",
                        err, retry_count
                    );
                }
                Ok(stores) => {
                    {
                        let mut store_cache = self.store_cache.wl();
                        for store in stores {
                            store_cache.insert(store.id, store);
                        }
                    }
                    *guard = Ok(());
                    return Ok(());
                }
            }

            if start_time.saturating_elapsed() > PD_CLIENT_TIMEOUT {
                break;
            }

            retry_count += 1;
            tokio::time::sleep(PD_CLIENT_INTERVAL).await;
        }

        error!("pd client get all stores timed out, retry {}", retry_count);
        *guard = Err(dfs::Error::Other("get all stores timed out".to_string()));
        guard.clone()
    }

    async fn get_all_stores(&self, force_update: bool) -> dfs::Result<Vec<u64>> {
        if force_update || self.store_cache.rl().is_empty() {
            self.update_all_stores().await?;
        }
        let store_ids: Vec<u64> = self.store_cache.rl().keys().cloned().collect();
        Ok(store_ids)
    }

    async fn get_region_stores(&self, opts: &Options, retry: bool) -> dfs::Result<Vec<u64>> {
        if opts.shard_id == 0 {
            // opts.end_off.is_some(): IA segments.
            debug_assert!(
                opts.file_type == FileType::TxnChunk
                    || opts.file_type == FileType::Schema
                    || opts.end_off.is_some(),
                "shard_id is missing: {:?}",
                opts
            );
            let mut store_ids = self.get_all_stores(false).await?;
            store_ids.sort();
            store_ids.truncate(3);
            return Ok(store_ids);
        }

        {
            let guard = self.region_cache.read().unwrap();
            if let Some(region) = guard.get(&opts.shard_id) {
                let region_ver = region.get_region_epoch().get_version();
                if region_ver >= opts.shard_ver {
                    return Ok(region.get_peers().iter().map(|p| p.store_id).collect());
                }
            }
        }

        self.update_region(opts.shard_id, retry).await
    }

    async fn update_region(&self, region_id: u64, retry: bool) -> dfs::Result<Vec<u64>> {
        let start_time = Instant::now_coarse();
        let mut retry_count = 0;
        loop {
            match self.pd.get_region_by_id(region_id).await {
                Err(err) => {
                    debug!(
                        "pd client get region {} failed {}, retry {}",
                        region_id, err, retry_count
                    );
                }
                Ok(region_opt) => {
                    if region_opt.is_none() {
                        return Err(dfs::Error::Other(format!("region {} not found", region_id)));
                    }
                    let region = region_opt.unwrap();
                    let mut guard = self.region_cache.wl();
                    guard.insert(region_id, region.clone());
                    return Ok(region.get_peers().iter().map(|p| p.store_id).collect());
                }
            }

            if !retry || start_time.saturating_elapsed() > PD_CLIENT_TIMEOUT {
                break;
            }

            retry_count += 1;
            tokio::time::sleep(PD_CLIENT_INTERVAL).await;
        }

        error!(
            "pd client update region {} timed out, retry {}",
            region_id, retry_count
        );
        Err(dfs::Error::Other("get region timed out".to_string()))
    }

    async fn read_file_from_store(
        &self,
        file_id: u64,
        opts: Options,
        store_id: u64,
    ) -> dfs::Result<Bytes> {
        let store_addr = self.get_store_addr(store_id).await?;
        let end_off = opts
            .end_off
            .map(|end| format!("&end_off={}", end))
            .unwrap_or_default();
        let uri = Uri::try_from(format!(
            "http://{}/dfs/{}?file_type={}&start_off={}{}",
            store_addr, file_id, opts.file_type, opts.start_off, end_off
        ))
        .unwrap();
        let resp = self.http_client.get(uri).await?;
        if !resp.status().is_success() {
            return Err(dfs::Error::Other(format!(
                "read file failed: {}",
                resp.status()
            )));
        }
        let data = hyper::body::to_bytes(resp.into_body()).await?;
        Ok(data)
    }

    async fn read_file_inner(&self, file_id: u64, opts: Options) -> dfs::Result<Bytes> {
        let mut errs = vec![];
        // First try to read the file from the store where the peer is located
        match self.get_region_stores(&opts, false).await {
            Ok(mut stores) => {
                stores.shuffle(&mut rand::thread_rng());
                for &store_id in &stores {
                    match self.read_file_from_store(file_id, opts, store_id).await {
                        Ok(data) => return Ok(data),
                        Err(err) => errs.push(format!("read file failed: {}", err)),
                    }
                }
                warn!(
                    "builtin_dfs get file from region {} stores failed, read from all stores",
                    opts.shard_id
                );
            }
            Err(_) => {
                warn!(
                    "builtin_dfs get region {} failed, read from all stores",
                    opts.shard_id
                );
            }
        }

        // Concurrently query all stores for the existence of the file,
        // and then read the file from the stores where the file exists in sequence.
        // Since it is possible that the store has not been recorded yet,
        // it is necessary to force update the stores' information.
        let stores = self.get_all_stores(true).await?;
        let (tx, mut rx) = tokio::sync::mpsc::channel(stores.len());
        for &store_id in &stores {
            let store_addr = self.get_store_addr(store_id).await?;
            let uri = Uri::try_from(format!(
                "http://{}/dfs/{}?file_type={}&check_exists_only=true",
                store_addr, file_id, opts.file_type
            ))
            .unwrap();
            let http_client = self.http_client.clone();
            let tx = tx.clone();
            let resp_store_id = store_id;
            self.get_runtime().spawn(async move {
                let res = http_client.get(uri).await;
                let _ = tx.send((resp_store_id, res)).await;
            });
        }
        for _ in 0..stores.len() {
            let (store_id, res) = rx.recv().await.unwrap();
            match res {
                Ok(resp) => {
                    if !resp.status().is_success() {
                        errs.push(format!("read file failed: {}", resp.status()));
                    } else {
                        match self.read_file_from_store(file_id, opts, store_id).await {
                            Ok(data) => return Ok(data),
                            Err(err) => errs.push(format!("read file failed: {}", err)),
                        }
                    }
                }
                Err(err) => errs.push(format!("read file failed: {}", err)),
            }
        }
        Err(dfs::Error::Other(errs.join(", ")))
    }

    async fn create_file_inner(&self, file_id: u64, data: Bytes, opts: Options) -> dfs::Result<()> {
        let stores = match self.get_region_stores(&opts, true).await {
            Ok(stores) => stores,
            Err(_) => {
                warn!(
                    "builtin_dfs get region {} failed, write to the first 3 stores",
                    opts.shard_id
                );
                let mut store_ids = self.get_all_stores(false).await?;
                store_ids.sort();
                store_ids.truncate(3);
                store_ids
            }
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel(stores.len());
        for &store_id in &stores {
            let store_addr = self.get_store_addr(store_id).await?;
            let url = format!(
                "http://{}/dfs/{}?file_type={}",
                store_addr, file_id, opts.file_type
            );
            let req = hyper::Request::builder()
                .method("POST")
                .uri(url)
                .body(data.clone().into())
                .unwrap();
            let http_client = self.http_client.clone();
            let tx = tx.clone();
            self.get_runtime().spawn(async move {
                let res = http_client.request(req).await;
                let _ = tx.send(res).await;
            });
        }
        let mut errors = vec![];
        let mut remaining_success_cnt = stores.len() / 2 + 1;
        for _ in 0..stores.len() {
            let resp = match rx.recv().await.unwrap() {
                Ok(resp) => resp,
                Err(err) => {
                    errors.push(dfs::Error::Other(format!("create file failed: {}", err)));
                    continue;
                }
            };
            let status = resp.status();
            if !status.is_success() {
                let err_msg = hyper::body::to_bytes(resp.into_body())
                    .await
                    .map(|b| String::from_utf8_lossy(&b).to_string())?;
                errors.push(dfs::Error::Other(format!(
                    "create file failed: {}:{}",
                    status, err_msg,
                )));
            } else {
                remaining_success_cnt -= 1;
                if remaining_success_cnt == 0 {
                    break;
                }
            }
        }
        // No need for errors when there is only 1 replica
        if remaining_success_cnt > 0 && stores.len() > 1 {
            return Err(errors.pop().unwrap());
        }
        Ok(())
    }
}

#[async_trait]
impl Dfs for BuiltinDfs {
    async fn read_file(&self, file_id: u64, opts: Options) -> dfs::Result<Bytes> {
        self.read_file_inner(file_id, opts).await
    }

    async fn create(&self, file_id: u64, data: Bytes, opts: Options) -> dfs::Result<()> {
        self.create_file_inner(file_id, data, opts).await
    }

    async fn remove(&self, _file_id: u64, _file_len: Option<u64>, _opts: Options) {
        // Do nothing, file is removed by GC.
    }

    async fn permanently_remove(&self, _file_id: u64, _opts: Options) -> kvengine::dfs::Result<()> {
        // Do nothing, file is removed by GC.
        Ok(())
    }

    fn get_runtime(&self) -> &Runtime {
        &self.runtime
    }

    fn get_prefix(&self) -> String {
        "builtin_dfs".to_string()
    }
}
