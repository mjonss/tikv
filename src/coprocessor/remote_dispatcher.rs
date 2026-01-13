// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::VecDeque, fmt::Write, ops::Deref, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes};
use codec::number::NumberDecoder;
use http::{StatusCode, header};
use kvengine::{LOCK_CF, SnapAccess, dfs::DFS_REMOTE_CACHE_ADDR_HEADER};
use kvproto::{
    coprocessor::{KeyRange, Response},
    kvrpcpb::ExecDetailsV2,
};
use protobuf::Message;
use security::SecurityManager;
use tidb_query_common::execute_stats::ExecSummary;
use tikv_alloc::MemoryTraceGuard;
use tikv_kv::Statistics;
use tikv_util::{
    backoff::ExponentialBackoff,
    deadline::Deadline,
    http::{CONTENT_TYPE_PROTOBUF, HeaderExt},
    retry::sleep_async,
};
use tipb::{DagRequest, ExecType, Executor, ExprType::ColumnRef};
use txn_types::{TimeStamp, TsSet};

use crate::{
    coprocessor::{
        Error, MEMTRACE_ROOT, REQ_TYPE_DAG, ReqContext, RequestHandler, Result,
        metrics::{COPR_REMOTE_DAG_ESTIMATE_BLOCKS_HISTOGRAM, COPR_REMOTE_PROCESSED_SIZE},
    },
    storage::txn::check_locks,
};

pub const REMOTE_REQUEST_TIMEOUT: Duration = Duration::from_secs(60 * 5);

pub const REMOTE_COP_FORMAT_V1: u32 = 1;

const RETRY_MAX_ATTEMPTS: usize = 10;
const REMOTE_MIN_PAGING_SIZE: u64 = 4096;
const RETRY_BASE_DELAY: Duration = Duration::from_secs(1);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(30);

#[derive(Default, Debug)]
pub struct RemoteRequest {
    pub key: String,
    pub cop_req: Vec<u8>,
    pub req_body: Bytes,
}

pub async fn remote_request(
    remote_ctx: &RemoteContext,
    remote_addr: &str,
    tag: &str,
    req_body: Bytes,
    deadline: Deadline,
) -> Result<Vec<u8>> {
    let req = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(remote_addr)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::ACCEPT, CONTENT_TYPE_PROTOBUF)
        .header(DFS_REMOTE_CACHE_ADDR_HEADER, remote_ctx.status_addr.clone())
        .body(hyper::Body::from(req_body))
        .map_err(|e| Error::Other(e.to_string()))?;
    let client = remote_ctx.client.clone();
    remote_ctx
        .runtime
        .spawn(async move {
            tokio::time::timeout_at(deadline.to_tokio_instant(), async move {
                let response = client
                    .request(req)
                    .await
                    .map_err(|e| Error::RemoteNetwork(e.to_string()))?;
                let is_pb_resp = response.headers().is_content_type_protobuf();
                let status = response.status();
                let body = hyper::body::to_bytes(response.into_body())
                    .await
                    .map_err(|e| Error::RemoteNetwork(e.to_string()))?;
                if !status.is_success() && !is_pb_resp {
                    let msg = String::from_utf8_lossy(body.chunk()).to_string();
                    let err = if status == StatusCode::SERVICE_UNAVAILABLE {
                        Error::RemoteServiceUnavailable(msg)
                    } else {
                        Error::Other(msg)
                    };
                    return Err(err);
                }
                Ok(body.to_vec())
            })
            .await
            .map_err(|_| Error::DeadlineExceeded)?
        })
        .await
        .map_err(|err| Error::Other(format!("{tag} join remote request task failed: {err:?}")))?
}

pub async fn remote_handle_request(
    req_type: &str,
    tag: &str,
    remote_ctx: &RemoteContext,
    remote_req: &RemoteRequest,
    deadline: Deadline,
) -> Result<Response> {
    info!("handle {} request, {}", req_type, tag);
    let req_body = remote_req.req_body.clone();
    let resp_body = remote_request(
        remote_ctx,
        &remote_ctx.remote_worker_url,
        tag,
        req_body,
        deadline,
    )
    .await?;
    let mut resp = Response::default();
    resp.merge_from_bytes(&resp_body)
        .map_err(|err| Error::Other(format!("{tag}: decode response failed: {err:?}")))?;

    debug_assert!(
        !resp.has_region_error() && !resp.has_locked(),
        "unexpected error in remote cop resp: {:?}",
        resp
    );
    Ok(resp)
}

pub async fn remote_handle_request_with_retry(
    req_type: &str,
    tag: &str,
    remote_ctx: &RemoteContext,
    remote_req: &RemoteRequest,
) -> Result<Response> {
    let deadline = Deadline::from_now(REMOTE_REQUEST_TIMEOUT);
    let mut backoff =
        ExponentialBackoff::new(RETRY_BASE_DELAY, RETRY_MAX_DELAY, RETRY_MAX_ATTEMPTS);
    loop {
        match remote_handle_request(req_type, tag, remote_ctx, remote_req, deadline).await {
            Ok(resp) => return Ok(resp),
            Err(err @ Error::RemoteNetwork(_) | err @ Error::RemoteServiceUnavailable(_)) => {
                if deadline.check().is_err() {
                    warn!("{}:{}: deadline is exceeded", tag, req_type);
                    return Err(err);
                }

                if let Ok(delay) = backoff.next_delay() {
                    sleep_async(delay).await;
                } else {
                    warn!("{}:{}: retry limit exceeded", tag, req_type; "backoff" => ?backoff);
                    return Err(err);
                }
            }
            Err(err) => return Err(err),
        }
    }
}

#[derive(Clone)]
pub struct RemoteContext {
    pub core: Arc<RemoteContextCore>,
}

impl Deref for RemoteContext {
    type Target = RemoteContextCore;
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

pub struct RemoteContextCore {
    pub remote_worker_url: String,
    pub cop_worker_provider: Arc<dyn CopWorkerProvider>,
    pub cop_min_blocks_size: usize,
    pub cop_num_ranges: usize,
    pub cop_min_process_duration: Duration,
    pub runtime: tokio::runtime::Handle,
    pub client: security::HttpClient,
    lazy_remote_patterns: dashmap::DashMap<String, RemotePatternStats>,
    status_addr: String,
}

pub trait CopWorkerProvider: Send + Sync {
    fn get(&self, keyspace_id: u32, start_ts: u64) -> Option<String>;
}

pub struct StaticCopWorkerProvider {
    worker_url: String,
}

impl CopWorkerProvider for StaticCopWorkerProvider {
    fn get(&self, _keyspace_id: u32, _start_ts: u64) -> Option<String> {
        if self.worker_url.is_empty() {
            return None;
        }
        Some(self.worker_url.clone())
    }
}

impl RemoteContext {
    pub fn new(
        remote_worker_url: String,
        cop_worker_url: String,
        cop_min_blocks_size: usize,
        cop_num_ranges: usize,
        cop_min_process_duration: Duration,
        security_mgr: Arc<SecurityManager>,
        runtime: tokio::runtime::Handle,
        status_addr: String,
    ) -> Option<Self> {
        if remote_worker_url.is_empty() && cop_worker_url.is_empty() {
            return None;
        }
        let client = security_mgr
            .http_client(hyper::Client::builder().pool_max_idle_per_host(0).clone())
            .unwrap();
        let cop_worker_provider = Arc::new(StaticCopWorkerProvider {
            worker_url: cop_worker_url,
        });
        let lazy_remote_patterns = dashmap::DashMap::new();
        Some(Self {
            core: Arc::new(RemoteContextCore {
                remote_worker_url,
                cop_min_blocks_size,
                cop_num_ranges,
                cop_min_process_duration,
                cop_worker_provider,
                runtime,
                client,
                lazy_remote_patterns,
                status_addr,
            }),
        })
    }

    pub fn report_lazy_remote_pattern(
        &self,
        pattern: String,
        processed_size: usize,
        processed_duration: Duration,
    ) {
        let mut stats = self.lazy_remote_patterns.entry(pattern).or_default();
        stats.push(processed_size, processed_duration);
    }

    pub fn should_offload(&self, pattern: &str) -> bool {
        if let Some(stats) = self.lazy_remote_patterns.get(pattern) {
            stats.should_offload(self.cop_min_blocks_size, self.cop_min_process_duration)
        } else {
            false
        }
    }
}

pub(crate) fn try_remote_dag_handler(
    snap: Option<&kvengine::SnapAccess>,
    dag: &DagRequest,
    req_ctx: &ReqContext,
    remote_ctx: Option<RemoteContext>,
    paging_size: Option<u64>,
) -> Option<Box<dyn RequestHandler>> {
    let remote_ctx = remote_ctx?;
    let start_ts = req_ctx.txn_start_ts.into_inner();
    let snap = snap.cloned()?;
    let worker_addr = remote_ctx
        .cop_worker_provider
        .get(snap.get_keyspace_id(), start_ts)?;
    let keyspace_id = snap.get_keyspace_id();
    if let Some(paging_size) = paging_size {
        if is_index_full_scan(dag) && paging_size <= REMOTE_MIN_PAGING_SIZE {
            // Do not offload index full scan with small paging size because the upper layer
            // operator may quickly finish the query.
            return None;
        }
    }
    let lazy_remote_pattern = if let Some(lazy_remote_pattern) = &req_ctx.lazy_remote_pattern {
        // Dag that extracts lazy remote pattern should be offloaded to remote
        // coprocessor based on actual execution cost.
        if !remote_ctx.should_offload(lazy_remote_pattern) {
            return None;
        }
        lazy_remote_pattern
    } else {
        let last_executor = dag.get_executors().last().unwrap();
        if last_executor.has_limit() {
            // Do not offload coprocessor with limit because the actual cost may be much
            // smaller.
            return None;
        }
        ""
    };
    let mut ranges = Vec::with_capacity(req_ctx.ranges.len());
    for ran in &req_ctx.ranges {
        let start = Bytes::copy_from_slice(&ran.start);
        let end = Bytes::copy_from_slice(&ran.end);
        ranges.push((start, end))
    }
    let blocks_size = if ranges.len() < remote_ctx.cop_num_ranges {
        snap.estimated_range_blocks_size(&ranges)
    } else {
        // skip calculate blocks size if the number of ranges is too large.
        remote_ctx.cop_min_blocks_size
    };
    let min_blocks_size =
        calc_min_blocks_size(req_ctx.txn_start_ts, remote_ctx.cop_min_blocks_size);
    if blocks_size < min_blocks_size {
        return None;
    }
    COPR_REMOTE_DAG_ESTIMATE_BLOCKS_HISTOGRAM.observe(blocks_size as f64);
    let tag = format!(
        "ks{}:{}:{}:{}",
        keyspace_id,
        snap.get_id(),
        snap.get_version(),
        req_ctx.txn_start_ts.into_inner()
    );
    info!(
        "{} {} send remote coprocessor blocks_size:{} ranges:{}",
        tag,
        lazy_remote_pattern,
        blocks_size,
        ranges.len(),
    );
    // reassemble a coprocessor request.
    let mut cop_req = kvproto::coprocessor::Request::default();
    cop_req.set_context(req_ctx.context.clone());
    cop_req.tp = REQ_TYPE_DAG;
    cop_req.start_ts = req_ctx.txn_start_ts.into_inner();
    cop_req.data = dag.write_to_bytes().unwrap();
    cop_req.ranges = req_ctx.ranges.clone().into();
    // Set the `paging_size` to a large enough value to reduce the number of
    // requests.
    cop_req.paging_size = u32::MAX as u64;
    Some(
        RemoteDagDispatcher::new(
            cop_req,
            snap,
            ranges,
            req_ctx.bypass_locks.clone(),
            remote_ctx,
            worker_addr,
            tag,
            req_ctx.deadline,
        )
        .into_boxed(),
    )
}

fn is_index_full_scan(dag: &DagRequest) -> bool {
    let executors = dag.get_executors();
    executors.len() == 1 && executors[0].get_tp() == ExecType::TypeIndexScan
}

pub struct RemoteDagDispatcher {
    req: kvproto::coprocessor::Request,
    snap: SnapAccess,
    ranges: Vec<(Bytes, Bytes)>,
    bypass_locks: TsSet,
    remote_ctx: RemoteContext,
    worker_addr: String,
    tag: String,
    exec_details: Option<ExecDetailsV2>,
    deadline: Deadline,
}

impl RemoteDagDispatcher {
    fn new(
        req: kvproto::coprocessor::Request,
        snap: SnapAccess,
        ranges: Vec<(Bytes, Bytes)>,
        bypass_locks: TsSet,
        remote_ctx: RemoteContext,
        worker_addr: String,
        tag: String,
        deadline: Deadline,
    ) -> Self {
        Self {
            req,
            snap,
            ranges,
            bypass_locks,
            remote_ctx,
            worker_addr,
            tag,
            exec_details: None,
            deadline,
        }
    }

    fn check_locks(&self) -> Result<()> {
        let mut stats = Statistics::default();
        let mut lock_iter = self.snap.new_iterator(LOCK_CF, false, false, None, true);
        for (start, end) in &self.ranges {
            lock_iter.set_range(start.clone(), end.clone());
            check_locks(
                &mut lock_iter,
                &self.bypass_locks,
                self.req.start_ts,
                &mut stats,
            )?;
        }
        Ok(())
    }

    async fn dispatch(&self, deadline: Deadline) -> Result<Vec<u8>> {
        let cop_req: Vec<u8> = self.req.write_to_bytes().unwrap();
        let (_, req_body) = encode_remote_request_body(
            &cop_req,
            &self.ranges,
            self.req.start_ts,
            &self.snap,
            false,
        );
        let req_body = Bytes::from(req_body);

        let mut backoff = ExponentialBackoff::new(RETRY_BASE_DELAY, RETRY_MAX_DELAY, usize::MAX);
        loop {
            match remote_request(
                &self.remote_ctx,
                &self.worker_addr,
                &self.tag,
                req_body.clone(),
                deadline,
            )
            .await
            {
                Ok(data) => return Ok(data),
                Err(err @ Error::RemoteNetwork(_) | err @ Error::RemoteServiceUnavailable(_)) => {
                    let dur_left: Duration =
                        deadline.check().map_err(|_| Error::DeadlineExceeded)?;

                    error!(
                        "{} dispatch remote coprocessor error {:?}, attempts {}, {:?} until deadline",
                        self.tag,
                        err,
                        backoff.current_attempts(),
                        dur_left,
                    );
                    if let Ok(delay) = backoff.next_delay() {
                        sleep_async(delay).await;
                    } else {
                        // Should not reach here. But still handle the error for safety.
                        error!("{} dispatch remote coprocessor: retry limit exceeded", self.tag; "backoff" => ?backoff);
                        return Err(err);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }
}

#[async_trait]
impl RequestHandler for RemoteDagDispatcher {
    async fn handle_request(&mut self) -> Result<MemoryTraceGuard<kvproto::coprocessor::Response>> {
        self.check_locks()?;
        let ret = self.dispatch(self.deadline).await;
        match ret {
            Ok(data) => {
                let memory_size = data.capacity();
                let mut resp = kvproto::coprocessor::Response::default();
                resp.merge_from_bytes(&data).unwrap();
                let processed_size = resp
                    .get_exec_details_v2()
                    .get_scan_detail_v2()
                    .get_processed_versions_size();
                COPR_REMOTE_PROCESSED_SIZE.inc_by(processed_size);
                self.exec_details = Some(resp.get_exec_details_v2().clone());
                Ok(MEMTRACE_ROOT.trace_guard(resp, memory_size))
            }
            Err(Error::Other(e)) => {
                error!("{} remote coprocessor failed, error {}", self.tag, e);
                let mut resp = kvproto::coprocessor::Response::default();
                resp.set_other_error(e);
                Ok(resp.into())
            }
            Err(e) => {
                error!("{} remote coprocessor failed, error {:?}", self.tag, e);
                Err(e)
            }
        }
    }

    async fn handle_streaming_request(
        &mut self,
    ) -> Result<(Option<kvproto::coprocessor::Response>, bool)> {
        unimplemented!()
    }

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        if let Some(exec_details_v2) = self.exec_details.as_ref() {
            if let Some(scan_detail_v2) = exec_details_v2.scan_detail_v2.as_ref() {
                dest.processed_size = scan_detail_v2.processed_versions_size as usize;
                dest.write.processed_keys = scan_detail_v2.processed_versions as usize;
            }
        }
    }

    fn collect_scan_summary(&mut self, dest: &mut ExecSummary) {
        if let Some(exec_details_v2) = self.exec_details.as_ref() {
            if let Some(time_details) = exec_details_v2.time_detail.as_ref() {
                dest.time_processed_ns = time_details.process_wall_time_ms as usize * 1000000;
            }
        }
    }
}

impl RemoteContext {
    pub fn extract(
        &self,
        keyspace_id: u32,
        ranges: &[KeyRange],
        dag: &DagRequest,
    ) -> Option<String> {
        let executors = dag.get_executors();
        if executors.is_empty() {
            return None;
        }
        let scan_executor = &executors[0];
        if ranges.len() >= self.cop_num_ranges {
            // on large range pattern.
            let mut str_buf = String::new();
            write!(str_buf, "k{}", keyspace_id).unwrap();
            Self::write_scan_digest(&mut str_buf, scan_executor);
            write!(str_buf, "r").unwrap();
            return Some(str_buf);
        }
        if executors.len() < 3 {
            return None;
        }

        let last_executor = &executors[executors.len() - 1];
        let second_last_executor = &executors[executors.len() - 2];
        if !last_executor.has_limit() || !second_last_executor.has_selection() {
            return None;
        }
        // on selection with limit pattern.
        let mut str_buf = String::new();
        write!(str_buf, "k{}", keyspace_id).unwrap();
        Self::write_scan_digest(&mut str_buf, scan_executor);
        let selection = second_last_executor.get_selection();
        Self::write_selection_digest(&mut str_buf, selection);
        Some(str_buf)
    }

    fn write_scan_digest(s: &mut String, executor: &Executor) {
        match executor.get_tp() {
            ExecType::TypeTableScan => {
                let tbl_scan = executor.get_tbl_scan();
                write!(s, "t{}", tbl_scan.get_table_id()).unwrap();
                for col in tbl_scan.get_columns() {
                    write!(s, "c{}", col.get_column_id()).unwrap();
                }
            }
            ExecType::TypeIndexScan => {
                let idx_scan = executor.get_idx_scan();
                write!(s, "t{}", idx_scan.get_table_id()).unwrap();
                write!(s, "i{}", idx_scan.get_index_id()).unwrap();
                for col in idx_scan.get_columns() {
                    write!(s, "c{}", col.get_column_id()).unwrap();
                }
            }
            _ => {}
        }
    }

    fn write_selection_digest(s: &mut String, selection: &tipb::Selection) {
        s.push('s');
        for expr in selection.get_conditions() {
            Self::extract_expr(s, expr);
        }
    }

    fn extract_expr(s: &mut String, expr: &tipb::Expr) {
        if expr.get_tp() == ColumnRef {
            write!(s, "c{}", expr.get_val().read_i64().unwrap_or_default()).unwrap();
        }
        if !expr.get_children().is_empty() {
            for child in expr.get_children() {
                Self::extract_expr(s, child);
            }
        }
    }
}

#[derive(Default)]
struct RemotePatternStats {
    process_stats: VecDeque<ProcessStat>,
}

#[derive(Default, Copy, Clone)]
struct ProcessStat {
    size: usize,
    duration: Duration,
}

impl RemotePatternStats {
    fn push(&mut self, processed_size: usize, process_duration: Duration) {
        self.process_stats.push_back(ProcessStat {
            size: processed_size,
            duration: process_duration,
        });
        if self.process_stats.len() > 5 {
            self.process_stats.pop_front();
        }
    }

    fn should_offload(&self, threshold_size: usize, threshold_dur: Duration) -> bool {
        if self.process_stats.len() < 2 {
            return false;
        }
        let sum_size: usize = self.process_stats.iter().map(|x| x.size).sum();
        let avg_size = sum_size / self.process_stats.len();
        let sum_duration: Duration = self.process_stats.iter().map(|stat| stat.duration).sum();
        let avg_duration = sum_duration / self.process_stats.len() as u32;
        avg_size > threshold_size || avg_duration > threshold_dur
    }
}

/// If the query has already run for a long time, the additional latency for
/// offloading to remote coprocessor is non-significant, we can decreases the
/// min blocks size to reduce the tikv-server resource consumption.
fn calc_min_blocks_size(start_ts: TimeStamp, config_min_blocks_size: usize) -> usize {
    const LONG_QUERY_MS: u64 = 15 * 1000;
    let elapsed_ms = TimeStamp::physical_now().saturating_sub(start_ts.physical());
    if elapsed_ms > LONG_QUERY_MS * 4 {
        // 60s
        config_min_blocks_size / 8
    } else if elapsed_ms > LONG_QUERY_MS * 2 {
        // 30s
        config_min_blocks_size / 4
    } else if elapsed_ms > LONG_QUERY_MS {
        // 15s
        config_min_blocks_size / 2
    } else {
        config_min_blocks_size
    }
}

pub fn encode_remote_request_body(
    cop_req: &[u8],
    ranges: &[(Bytes, Bytes)],
    start_ts: u64,
    snap: &SnapAccess,
    cache_key: bool,
) -> (String, Vec<u8>) {
    let mem_data = snap.build_mem_data(ranges, start_ts);
    let (key, snap_data) = snap.marshal(ranges, true, cache_key);
    let req_body = encode_remote_cop_request(cop_req, &mem_data, &snap_data);
    (key, req_body)
}

pub fn encode_remote_cop_request(cop_req: &[u8], mem_data: &[u8], snap_data: &[u8]) -> Vec<u8> {
    let extra_len = 4 * 4;
    let mut req_body: Vec<u8> =
        Vec::with_capacity(extra_len + cop_req.len() + mem_data.len() + snap_data.len());
    req_body.put_u32_le(REMOTE_COP_FORMAT_V1);
    req_body.put_u32_le(cop_req.len() as u32);
    req_body.extend_from_slice(cop_req);
    req_body.put_u32_le(mem_data.len() as u32);
    req_body.extend_from_slice(mem_data);
    req_body.put_u32_le(snap_data.len() as u32);
    req_body.extend_from_slice(snap_data);
    req_body
}

pub fn decode_remote_cop_request(body: &[u8]) -> Result<(&[u8], &[u8], &[u8])> {
    let mut body_buf = body;
    let err = Error::Other("failed to decode cop request".to_string());
    if body_buf.len() < 4 {
        return Err(err);
    }
    let format = body_buf.get_u32_le();
    if format != REMOTE_COP_FORMAT_V1 {
        return Err(err);
    }
    if body_buf.len() < 4 {
        return Err(err);
    }
    let req_data_len = body_buf.get_u32_le() as usize;
    if body_buf.len() < req_data_len {
        return Err(err);
    }
    let req_data = &body_buf[..req_data_len];
    body_buf = &body_buf[req_data_len..];
    if body_buf.len() < 4 {
        return Err(Error::Other("invalid cop request".to_string()));
    }
    let mem_data_len = body_buf.get_u32_le() as usize;
    if body_buf.len() < mem_data_len {
        return Err(err);
    }
    let mem_data = &body_buf[..mem_data_len];
    body_buf = &body_buf[mem_data_len..];
    if body_buf.len() < 4 {
        return Err(Error::Other("invalid cop request".to_string()));
    }
    let snap_data_len = body_buf.get_u32_le() as usize;
    if body_buf.len() < snap_data_len {
        return Err(err);
    }
    let snap_data = &body_buf[..snap_data_len];
    Ok((req_data, mem_data, snap_data))
}

#[cfg(test)]
mod tests {
    use codec::number::NumberEncoder;
    use tidb_query_datatype::{FieldTypeAccessor, FieldTypeTp};
    use tipb::{Expr, ScalarFuncSig};

    use super::*;

    #[test]
    fn test_remote_cop_coded() {
        let cop_req = b"cop_req".to_vec();
        let mem_data = b"mem_data".to_vec();
        let snap_data = b"snap_data".to_vec();
        let req_body = encode_remote_cop_request(&cop_req, &mem_data, &snap_data);
        let (cop_req, mem_data, snap_data) = decode_remote_cop_request(&req_body).unwrap();
        assert_eq!(cop_req, "cop_req".as_bytes());
        assert_eq!(mem_data, "mem_data".as_bytes());
        assert_eq!(snap_data, "snap_data".as_bytes());
    }

    #[test]
    fn test_remote_cop_coded_with_empty_mem() {
        let cop_req = b"cop_req".to_vec();
        let mem_data = b"".to_vec();
        let snap_data = b"snap_data".to_vec();
        let req_body = encode_remote_cop_request(&cop_req, &mem_data, &snap_data);
        let (cop_req, mem_data, snap_data) = decode_remote_cop_request(&req_body).unwrap();
        assert_eq!(cop_req, "cop_req".as_bytes());
        assert_eq!(mem_data, "".as_bytes());
        assert_eq!(snap_data, "snap_data".as_bytes());
    }

    #[test]
    fn test_calc_min_blocks() {
        let conf_min_blocks_size = 512usize;
        let ts = TimeStamp::default();
        assert_eq!(calc_min_blocks_size(ts, conf_min_blocks_size), 64);
        let ts = TimeStamp::max();
        assert_eq!(calc_min_blocks_size(ts, conf_min_blocks_size), 512);

        let phys_now = TimeStamp::physical_now();
        let ts = TimeStamp::compose(phys_now - 70 * 1000, 1);
        assert_eq!(calc_min_blocks_size(ts, conf_min_blocks_size), 64);
        let ts = TimeStamp::compose(phys_now - 40 * 1000, 1);
        assert_eq!(calc_min_blocks_size(ts, conf_min_blocks_size), 128);
        let ts = TimeStamp::compose(phys_now - 20 * 1000, 1);
        assert_eq!(calc_min_blocks_size(ts, conf_min_blocks_size), 256);
        let ts = TimeStamp::compose(phys_now - 10 * 1000, 1);
        assert_eq!(calc_min_blocks_size(ts, conf_min_blocks_size), 512);
        let ts = TimeStamp::compose(phys_now + 20 * 1000, 1);
        assert_eq!(calc_min_blocks_size(ts, conf_min_blocks_size), 512);
    }

    #[test]
    fn test_remote_patten() {
        let mut dag = DagRequest::default();
        dag.mut_executors()
            .push(new_table_scan_executor(1, vec![1, 2, 3]));
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let remote_ctx = RemoteContext::new(
            "a".to_string(),
            "b".to_string(),
            100,
            2,
            Duration::from_millis(10),
            Arc::new(security::SecurityManager::default()),
            runtime.handle().clone(),
            String::new(),
        )
        .unwrap();
        let make_key_range = |start: u8, end: u8| KeyRange {
            start: vec![start],
            end: vec![end],
            ..Default::default()
        };
        let small_key_ranges = vec![make_key_range(1, 2)];
        assert!(remote_ctx.extract(1, &small_key_ranges, &dag).is_none());
        let large_key_ranges = vec![
            make_key_range(1, 2),
            make_key_range(3, 4),
            make_key_range(5, 6),
        ];
        let large_key_range_pattern = remote_ctx.extract(1, &large_key_ranges, &dag).unwrap();
        assert_eq!(large_key_range_pattern, "k1t1c1c2c3r");

        assert!(remote_ctx.extract(1, &small_key_ranges, &dag).is_none());
        dag.mut_executors().push(new_selection(vec![1, 2]));
        assert!(remote_ctx.extract(1, &small_key_ranges, &dag).is_none());
        dag.mut_executors().push(new_limit(100));
        let tbl_pattern = remote_ctx.extract(1, &small_key_ranges, &dag).unwrap();
        assert_eq!(tbl_pattern, "k1t1c1c2c3sc1c2");

        let mut idx_dag = dag.clone();
        let executors = idx_dag.mut_executors();
        executors[0] = new_index_scan_executor(3, 2, vec![1, 2, 3]);
        let idx_pattern = remote_ctx.extract(1, &small_key_ranges, &idx_dag).unwrap();
        assert_eq!(idx_pattern, "k1t3i2c1c2c3sc1c2");

        let short_process_dur = Duration::from_millis(5);
        let long_process_dur = Duration::from_millis(100);
        remote_ctx.report_lazy_remote_pattern(tbl_pattern.clone(), 101, long_process_dur);
        assert!(!remote_ctx.should_offload(&tbl_pattern));
        remote_ctx.report_lazy_remote_pattern(tbl_pattern.clone(), 101, long_process_dur);
        assert!(remote_ctx.should_offload(&tbl_pattern));
        #[derive(Debug)]
        struct Case {
            processed_size: usize,
            process_duration: Duration,
            should_offload: bool,
        }
        let cases = vec![
            Case {
                processed_size: 50,
                process_duration: short_process_dur,
                should_offload: false,
            },
            Case {
                processed_size: 50,
                process_duration: long_process_dur,
                should_offload: true,
            },
            Case {
                processed_size: 101,
                process_duration: short_process_dur,
                should_offload: true,
            },
            Case {
                processed_size: 101,
                process_duration: long_process_dur,
                should_offload: true,
            },
        ];
        for case in cases {
            for _ in 0..10 {
                remote_ctx.report_lazy_remote_pattern(
                    tbl_pattern.clone(),
                    case.processed_size,
                    case.process_duration,
                );
            }
            assert_eq!(
                remote_ctx.should_offload(&tbl_pattern),
                case.should_offload,
                "case: {:?}",
                case
            );
        }
    }

    fn new_table_scan_executor(tbl_id: i64, col_ids: Vec<i64>) -> tipb::Executor {
        let mut executor = tipb::Executor::default();
        executor.set_tp(tipb::ExecType::TypeTableScan);
        let tbl_scan = executor.mut_tbl_scan();
        tbl_scan.set_table_id(tbl_id);
        for col_id in col_ids {
            let mut col = tipb::ColumnInfo::new();
            col.set_column_id(col_id);
            tbl_scan.mut_columns().push(col);
        }
        executor
    }

    fn new_index_scan_executor(tbl_id: i64, index_id: i64, col_ids: Vec<i64>) -> tipb::Executor {
        let mut executor = tipb::Executor::default();
        executor.set_tp(tipb::ExecType::TypeIndexScan);
        let idx_scan = executor.mut_idx_scan();
        idx_scan.set_table_id(tbl_id);
        idx_scan.set_index_id(index_id);
        for col_id in col_ids {
            let mut col = tipb::ColumnInfo::new();
            col.set_column_id(col_id);
            idx_scan.mut_columns().push(col);
        }
        executor
    }

    fn new_selection(col_offs: Vec<i64>) -> tipb::Executor {
        let mut executor = tipb::Executor::default();
        executor.set_tp(tipb::ExecType::TypeSelection);
        let conditions = executor.mut_selection().mut_conditions();
        for col_off in col_offs {
            let mut condition = tipb::Expr::default();
            condition.set_tp(tipb::ExprType::Int64);
            condition.set_sig(ScalarFuncSig::EqInt);
            condition
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);

            let mut col = Expr::default();
            col.set_tp(tipb::ExprType::ColumnRef);
            col.mut_val().write_i64(col_off).unwrap();
            col.mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);

            let mut value = Expr::default();
            value.set_tp(tipb::ExprType::Int64);
            value.mut_val().write_i64(1).unwrap();
            value
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            condition.mut_children().push(col);
            condition.mut_children().push(value);
            conditions.push(condition);
        }
        executor
    }

    fn new_limit(limit: u64) -> tipb::Executor {
        let mut executor = tipb::Executor::default();
        executor.set_tp(tipb::ExecType::TypeLimit);
        let limit_executor = executor.mut_limit();
        limit_executor.set_limit(limit);
        executor
    }
}
