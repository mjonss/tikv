// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use api_version::ApiV1;
use async_trait::async_trait;
use bytes::Bytes;
use kvproto::{
    coprocessor::{KeyRange, Response},
    kvrpcpb::ExecDetailsV2,
};
use protobuf::Message;
use tidb_query_common::storage::{
    Range,
    scanner::{RangesScanner, RangesScannerOptions},
};
use tikv_alloc::trace::MemoryTraceGuard;
use tikv_util::time::Instant;
use tipb::{ChecksumAlgorithm, ChecksumRequest, ChecksumResponse};

use crate::{
    coprocessor::{
        dag::TikvStorage,
        remote_dispatcher::{
            RemoteContext, RemoteRequest, encode_remote_request_body,
            remote_handle_request_with_retry,
        },
        *,
    },
    storage::{Snapshot, Statistics, txn::CloudStore},
};

// `ChecksumContext` is used to handle `ChecksumRequest`
pub struct ChecksumContext<S: Snapshot> {
    req: ChecksumRequest,
    scanner: RangesScanner<TikvStorage<CloudStore<S>>, ApiV1>,
    remote_ctx: Option<RemoteContext>,
    remote_req: RemoteRequest,
    remote_exec_details: Option<ExecDetailsV2>,
}

impl<S: Snapshot> ChecksumContext<S> {
    pub fn new(
        req: ChecksumRequest,
        ranges: Vec<KeyRange>,
        start_ts: u64,
        snap: S,
        req_ctx: &ReqContext,
        remote_ctx: Option<RemoteContext>,
        mut remote_req: RemoteRequest,
    ) -> Result<Self> {
        let remote_ctx = remote_ctx.inspect(|_ctx| {
            let mut kv_ranges = Vec::with_capacity(ranges.len());
            for range in &ranges {
                let kv_range = (
                    Bytes::copy_from_slice(&range.start),
                    Bytes::copy_from_slice(&range.end),
                );
                kv_ranges.push(kv_range)
            }
            let (key, req_body) = encode_remote_request_body(
                &remote_req.cop_req,
                kv_ranges.as_slice(),
                start_ts,
                snap.get_kvengine_snap().unwrap(),
                true,
            );
            remote_req.key = format!("checksum:{}", key);
            remote_req.req_body = Bytes::from(req_body);
        });
        let store = CloudStore::new(
            snap,
            start_ts,
            req_ctx.bypass_locks.clone(),
            !req_ctx.context.get_not_fill_cache(),
        );
        info!("checksum ranges"; "ranges" => ?ranges);
        let scanner = RangesScanner::new(RangesScannerOptions {
            storage: TikvStorage::new(store, false),
            ranges: ranges
                .into_iter()
                .map(|r| Range::from_pb_range(r, false))
                .collect(),
            scan_backward_in_range: false,
            is_key_only: false,
        });
        Ok(Self {
            req,
            scanner,
            remote_ctx,
            remote_req,
            remote_exec_details: None,
        })
    }

    async fn local_handle_request(&mut self) -> Result<Vec<u8>> {
        let algorithm = self.req.get_algorithm();
        if algorithm != ChecksumAlgorithm::Crc64Xor {
            return Err(box_err!("unknown checksum algorithm {:?}", algorithm));
        }

        let mut checksum = 0;
        let mut total_kvs = 0;
        let mut total_bytes = 0;
        let (old_prefix, new_prefix) = if self.req.has_rule() {
            let mut rule = self.req.get_rule().clone();
            (rule.take_old_prefix(), rule.take_new_prefix())
        } else {
            (vec![], vec![])
        };

        let mut prefix_digest = crc64fast::Digest::new();
        prefix_digest.write(&old_prefix);

        while let Some(row) = self.scanner.next().await? {
            let (k, v) = (row.key(), row.value());
            if !k.starts_with(&new_prefix) {
                return Err(box_err!("Wrong prefix expect: {:?}", new_prefix));
            }
            checksum =
                checksum_crc64_xor(checksum, prefix_digest.clone(), &k[new_prefix.len()..], v);
            total_kvs += 1;
            total_bytes += k.len() + v.len() + old_prefix.len() - new_prefix.len();
        }

        let mut resp = ChecksumResponse::default();
        resp.set_checksum(checksum);
        resp.set_total_kvs(total_kvs);
        resp.set_total_bytes(total_bytes as u64);
        let data = box_try!(resp.write_to_bytes());
        Ok(data)
    }
}

#[async_trait]
impl<S: Snapshot> RequestHandler for ChecksumContext<S> {
    async fn handle_request(&mut self) -> Result<MemoryTraceGuard<Response>> {
        let start_time = Instant::now_coarse();
        let is_remote = self.remote_ctx.is_some();
        let remote_tag = self
            .remote_ctx
            .as_ref()
            .map(|ctx| {
                format!(
                    " url [{}] [{}]",
                    ctx.remote_worker_url.clone(),
                    self.remote_req.key.clone(),
                )
            })
            .unwrap_or_default();
        let tag = format!("is remote {}{}", is_remote, remote_tag);
        let ret = if let Some(remote_ctx) = &self.remote_ctx {
            remote_handle_request_with_retry("checksum", &tag, remote_ctx, &self.remote_req).await
            // Do not fallback to local if remote checksum failed. Or else it
            // may cause server overloaded.
        } else {
            self.local_handle_request().await.map(|data| {
                let mut resp = Response::default();
                resp.set_data(data);
                resp
            })
        };
        match ret {
            Ok(mut resp) => {
                info!(
                    "checksum response data size {}, takes {:?}, {}",
                    resp.data.len(),
                    start_time.saturating_elapsed(),
                    tag,
                );
                if is_remote {
                    self.remote_exec_details = resp.exec_details_v2.take();
                }
                Ok(resp.into())
            }
            Err(Error::Other(e)) => {
                error!("checksum other failed, {} error {}", tag, e,);
                let mut resp = Response::default();
                resp.set_other_error(e);
                Ok(resp.into())
            }
            Err(e) => {
                error!("checksum failed, {} error {:?}", tag, e,);
                Err(e)
            }
        }
    }

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        if let Some(exec_details_v2) = self.remote_exec_details.as_ref() {
            if let Some(scan_detail_v2) = exec_details_v2.scan_detail_v2.as_ref() {
                dest.processed_size = scan_detail_v2.processed_versions_size as usize;
                dest.write.processed_keys = scan_detail_v2.processed_versions as usize;
            }
            return;
        }
        self.scanner.collect_storage_stats(dest)
    }
}

pub fn checksum_crc64_xor(
    checksum: u64,
    mut digest: crc64fast::Digest,
    k_suffix: &[u8],
    v: &[u8],
) -> u64 {
    digest.write(k_suffix);
    digest.write(v);
    checksum ^ digest.sum64()
}
