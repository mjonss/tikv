// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error::Error as StdError, future::Future, sync::Arc, time::Duration};

use bytes::{Buf, Bytes};
use cloud_encryption::MasterKey;
use cloud_server::StatusServer as CloudStatusServer;
use dashmap::DashMap;
use flate2::{write::GzEncoder, Compression};
use http::{
    header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_TYPE},
    request::Parts,
    HeaderValue, Method, Request, Response, StatusCode,
};
use hyper::{
    server::accept::Accept,
    service::{make_service_fn, service_fn},
    Body,
};
use kvengine::{
    context::{IaCtx, MetaFileCacheWeighter, PrepareType, SnapCtx},
    dfs,
    dfs::{Dfs, DFS_REMOTE_CACHE_ADDR_HEADER},
    local_compact,
    metrics::ENGINE_REMOTE_COMPACT_EXCEED_MEMORY_LIMIT_COUNTER,
    table::{
        columnar::ColumnarMetaCache, file::File, schema_file::SchemaFile, sstable::BlockCache,
        ChecksumType,
    },
    txn_chunk_manager::TxnChunkManager,
    CompactionCtx, CompactionRequest, CompactionType, IdAllocator, SnapAccess,
    CURRENT_COMPACTOR_VERSION, INCOMPATIBLE_COMPACTOR_ERROR_CODE,
};
use pd_client::PdClient;
use prometheus::TEXT_FORMAT;
use protobuf::Message;
use replication_worker::ReplicationScheduler;
use rfstore::store::{PdIdAllocator, RegionSnapshot};
use security::HttpClient;
use tikv::{
    coprocessor::{
        remote_dispatcher::decode_remote_cop_request, REQ_TYPE_ANALYZE, REQ_TYPE_CHECKSUM,
        REQ_TYPE_DAG,
    },
    server::status_server::StatusServer,
};
use tikv_util::{
    deadline::Deadline,
    error,
    http::{HeaderExt, CONTENT_TYPE_PROTOBUF},
    info,
    memory::MemoryLimiter,
    metrics::{dump, dump_to},
    quota_limiter::QuotaLimiter,
    time::Instant,
    warn,
};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    load_data::{self, LoadDataManager},
    metrics::*,
    native_br::{self, NativeBrManager},
    txn_chunk,
    txn_chunk::TxnChunkHandler,
    worker_limiter::WorkerLimiter,
    Config,
};

pub(crate) struct Context {
    pub config: Config,
    pub compression_lvl: i32,
    pub checksum_type: ChecksumType,
    pub thread_pool: tokio::runtime::Handle,
    pub dfs: Arc<dyn Dfs>,
    pub load_manager: Arc<LoadDataManager>,
    pub br_manager: Arc<NativeBrManager>,
    pub rep_scheduler: Option<ReplicationScheduler>,
    pub txn_chunk_handler: Arc<TxnChunkHandler>,
    pub pd: Arc<dyn PdClient>,
    pub master_key: MasterKey,
    pub quota_limiter: Arc<QuotaLimiter>,
    pub block_cache: BlockCache,
    pub columnar_meta_cache: ColumnarMetaCache,
    pub schema_files: Option<Arc<DashMap<u64, SchemaFile>>>,
    pub worker_limiter: WorkerLimiter,
    pub memory_limiter: MemoryLimiter,
    pub txn_chunk_manager: TxnChunkManager,
    pub ia_ctx: IaCtx,
    pub read_columnar: bool,
    pub meta_file_cache: Arc<quick_cache::sync::Cache<u64, Arc<dyn File>, MetaFileCacheWeighter>>,
    pub http_client: Arc<HttpClient>,
    pub remote_cache_ttl: Duration,
}

impl Context {
    pub(crate) fn get_snap_ctx(&self, dfs_remote_cache_addr: Option<&str>) -> SnapCtx {
        let dfs: Arc<dyn dfs::Dfs> = if let Some(dfs_cache_addr) = dfs_remote_cache_addr {
            Arc::new(dfs::RemoteCachedDfs::new(
                self.dfs.clone(),
                dfs_cache_addr.to_string(),
                self.http_client.clone(),
                self.remote_cache_ttl,
            ))
        } else {
            self.dfs.clone()
        };
        let prepare_type = if self.read_columnar {
            PrepareType::All
        } else {
            PrepareType::SstOnly
        };
        SnapCtx {
            dfs,
            master_key: self.master_key.clone(),
            block_cache: self.block_cache.clone(),
            columnar_meta_cache: self.columnar_meta_cache.clone(),
            vector_index_cache: None,
            columnar_file_cache: None,
            schema_files: self.schema_files.clone(),
            txn_chunk_manager: self.txn_chunk_manager.clone(),
            ia_ctx: self.ia_ctx.clone(),
            prepare_type,
            read_columnar: self.read_columnar,
            meta_file_cache: self.meta_file_cache.clone(),
        }
    }
}

#[macro_export]
macro_rules! start_serve {
    ($ctx:expr, $acceptor:expr) => {{
        match $acceptor {
            tikv_util::Either::Left(acceptor) => $crate::server::start(
                $ctx,
                hyper::server::Server::builder(acceptor)
                    .http1_header_read_timeout(SERVER_READ_TIMEOUT),
            ),
            tikv_util::Either::Right(acceptor) => $crate::server::start(
                $ctx,
                hyper::server::Server::builder(acceptor)
                    .http1_header_read_timeout(SERVER_READ_TIMEOUT),
            ),
        }
    }};
}

pub(crate) fn start<I, C>(
    ctx: Arc<Context>,
    builder: hyper::server::Builder<I>,
) -> Box<dyn Future<Output = hyper::Result<()>> + Send + Unpin>
where
    I: Accept<Conn = C, Error = std::io::Error> + Send + Unpin + 'static,
    I::Error: Into<Box<dyn StdError + Send + Sync>>,
    I::Conn: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let server = builder.serve(make_service_fn(move |_| {
        let ctx = ctx.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req: hyper::Request<hyper::Body>| {
                let ctx = ctx.clone();
                tikv_util::init_task_local(async move {
                    let path = req.uri().path().to_owned();
                    match path.as_ref() {
                        "/healthz" => Ok(hyper::Response::builder()
                            .status(200)
                            .body(hyper::Body::from("ok"))
                            .unwrap()),
                        "/compact" => {
                            let ob_start = Instant::now_coarse();

                            let allocator = Arc::new(PdIdAllocator::new(ctx.pd.clone()));
                            let resp = handle_remote_compaction(ctx, req, allocator).await;

                            if resp.is_ok() && resp.as_ref().unwrap().status().is_success() {
                                REMOTE_COMPACT_REQ_HANDLE_HISTOGRAM
                                    .observe(ob_start.saturating_elapsed().as_secs_f64());
                            }
                            resp
                        }
                        path if path.starts_with("/cdc") => {
                            replication_worker::handle_cdc_request(ctx.rep_scheduler.as_ref(), req)
                                .await
                        }
                        "/coprocessor" => handle_coprocessor(ctx, req).await,
                        "/load_data" => handle_load_data(ctx, req).await,
                        "/metrics" => handle_get_metrics(req).await,
                        "/debug/pprof/profile" => {
                            StatusServer::<u8, u8>::dump_cpu_prof_to_resp(req).await
                        }
                        "/debug/pprof/heap" => CloudStatusServer::dump_heap_prof_to_resp(req),
                        "/debug/pprof/cmdline" => CloudStatusServer::get_cmdline(req),
                        "/debug/pprof/symbol" => {
                            if req.method() == Method::GET {
                                CloudStatusServer::get_symbol_count(req)
                            } else if req.method() == Method::POST {
                                CloudStatusServer::get_symbol(req).await
                            } else {
                                Ok(hyper::Response::builder()
                                    .status(404)
                                    .body(hyper::Body::from("Not Found"))
                                    .unwrap())
                            }
                        }
                        #[cfg(debug_assertions)]
                        "/debug/sleep" => handle_sleep(ctx).await,
                        native_br::BACKUPS_API_PATH => {
                            native_br::handle_backup(ctx.br_manager.clone(), req).await
                        }
                        path if path.starts_with(native_br::RESTORE_KEYSPACE_API_PATH) => {
                            handle_restore_keyspace(ctx, req).await
                        }
                        "/txn_chunk" => handle_txn_chunk(ctx, req).await,
                        _ => Ok(hyper::Response::builder()
                            .status(404)
                            .body(hyper::Body::from("Not Found"))
                            .unwrap()),
                    }
                })
            }))
        }
    }));
    Box::new(server)
}

// Helper function to spawn a task on the thread pool and await its result
async fn spawn_and_await<F, T>(thread_pool: tokio::runtime::Handle, f: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static + std::fmt::Debug,
{
    let handle = thread_pool.spawn(tikv_util::init_task_local(f));
    match handle.await {
        Ok(res) => res,
        Err(err) => {
            panic!("spawn and await failed: {:?}", err);
        }
    }
}

async fn handle_coprocessor(
    ctx: Arc<Context>,
    req: hyper::Request<hyper::Body>,
) -> hyper::Result<hyper::Response<hyper::Body>> {
    let (parts, body) = req.into_parts();
    let body = hyper::body::to_bytes(body).await?;
    spawn_and_await(
        ctx.thread_pool.clone(),
        handle_remote_coprocessor(ctx, parts, body),
    )
    .await
}

async fn handle_load_data(
    ctx: Arc<Context>,
    req: hyper::Request<hyper::Body>,
) -> hyper::Result<hyper::Response<hyper::Body>> {
    let load_data_manager = ctx.load_manager.clone();
    let (parts, body) = req.into_parts();
    let body = hyper::body::to_bytes(body).await?;
    spawn_and_await(
        ctx.thread_pool.clone(),
        load_data::handle_load_data(load_data_manager, parts, body),
    )
    .await
}

async fn handle_restore_keyspace(
    ctx: Arc<Context>,
    req: hyper::Request<hyper::Body>,
) -> hyper::Result<hyper::Response<hyper::Body>> {
    let br_manager = ctx.br_manager.clone();
    let (parts, _) = req.into_parts();
    spawn_and_await(
        ctx.thread_pool.clone(),
        native_br::handle_restore_keyspace(br_manager, parts),
    )
    .await
}

async fn handle_txn_chunk(
    ctx: Arc<Context>,
    req: hyper::Request<hyper::Body>,
) -> hyper::Result<hyper::Response<hyper::Body>> {
    let (parts, body) = req.into_parts();
    let body = hyper::body::to_bytes(body).await?;
    spawn_and_await(
        ctx.thread_pool.clone(),
        txn_chunk::handle_txn_chunk(ctx, parts, body),
    )
    .await
}

// Debug API to sleep for 5 seconds
#[cfg(debug_assertions)]
async fn handle_sleep(_ctx: Arc<Context>) -> hyper::Result<hyper::Response<hyper::Body>> {
    info!("sleep for 5 seconds");
    std::thread::sleep(Duration::from_secs(5));
    info!("sleep done");
    Ok(hyper::Response::builder()
        .status(200)
        .body(hyper::Body::from("ok"))
        .unwrap())
}

const DEFAULT_COP_TIMEOUT: Duration = Duration::from_secs(20);

// Return `StatusCode::SERVICE_UNAVAILABLE` to indicate that the request can be
// retried later.
async fn handle_remote_coprocessor(
    ctx: Arc<Context>,
    parts: Parts,
    req_body: Bytes,
) -> hyper::Result<hyper::Response<hyper::Body>> {
    let accept_pb = parts.headers.is_accept_protobuf();
    let decode_res = decode_remote_cop_request(req_body.chunk());
    if let Err(err) = decode_res {
        let body = hyper::Body::from(format!("{:?}", err));
        return Ok(hyper::Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(body)
            .unwrap());
    }
    let (req_data, mem_data, snap_data) = decode_res.unwrap();
    let mut cop_req = kvproto::coprocessor::Request::default();
    if let Err(err) = cop_req.merge_from_bytes(req_data) {
        let body = hyper::Body::from(format!("{:?}", err));
        return Ok(hyper::Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(body)
            .unwrap());
    }
    let cop_ctx = cop_req.get_context();
    let keyspace_id = cop_ctx.keyspace_id;
    let handle_start = Instant::now_coarse();
    let tag = get_cop_req_tag(&cop_req);
    let mut timeout = Duration::from_millis(cop_ctx.get_max_execution_duration_ms());
    if timeout.is_zero() {
        timeout = DEFAULT_COP_TIMEOUT;
    }
    let deadline = Deadline::from_now(timeout);

    let res = tokio::time::timeout(timeout, ctx.worker_limiter.acquire_permit(keyspace_id)).await;
    if res.is_err() {
        info!(
            "wait permit timeout";
            "tag" => tag,
        );
        let body = hyper::Body::from("wait permit timeout");
        return Ok(hyper::Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(body)
            .unwrap());
    }
    let _permit = res.unwrap();

    let req_type = cop_req.get_tp();
    let snap_start = Instant::now_coarse();
    let snap_ctx = ctx.get_snap_ctx(get_dfs_remote_cache_addr(&parts));
    let mem_limiter = ctx.memory_limiter.clone();
    let snap_access_res =
        SnapAccess::construct_snapshot(&tag, &snap_ctx, mem_data, snap_data, mem_limiter).await;
    if let Err(err) = snap_access_res.as_ref() {
        let body = hyper::Body::from(format!("{:?}", &err));
        let status_code = if matches!(err, kvengine::Error::MemoryLimitExceeded(_)) {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        return Ok(hyper::Response::builder()
            .status(status_code)
            .body(body)
            .unwrap());
    }
    if handle_start.saturating_elapsed() > timeout {
        info!("construct snapshot timeout"; "tag" => tag);
        let body = hyper::Body::from("construct snapshot timeout");
        return Ok(hyper::Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(body)
            .unwrap());
    }
    let (snap_access, mem_limiter_guard) = snap_access_res.unwrap();

    let prefetch_start = Instant::now_coarse();
    if matches!(req_type, REQ_TYPE_DAG if ctx.ia_ctx.is_enabled()) {
        match tikv::coprocessor::prefetch_ia_remote_segments(
            &tag,
            &snap_ctx,
            &snap_access,
            cop_req.get_ranges(),
            deadline,
        )
        .await
        {
            Ok(cache_hit) => {
                if let Some(cache_hit) = cache_hit {
                    REMOTE_COPR_PREFETCH_CACHE_HIT_PERCENT_HISTOGRAM.observe(cache_hit * 100.0);
                }
            }
            Err(err) => {
                error!("{} prefetch failed, error {:?}", tag, err);
                let body = hyper::Body::from("prefetch segments failed");
                return Ok(hyper::Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(body)
                    .unwrap());
            }
        }
        if deadline.check().is_err() {
            info!("prefetch timeout"; "tag" => tag);
            let body = hyper::Body::from("prefetch segments timeout");
            return Ok(hyper::Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(body)
                .unwrap());
        }
    }

    let snap = RegionSnapshot::from_snapshot(snap_access, None);
    let process_start = Instant::now_coarse();
    let result = tikv::coprocessor::parse_request_and_handle_remote_cop(
        cop_req,
        None,
        Duration::from_secs(60),
        ctx.config.cop_max_resp_size.0,
        ctx.quota_limiter.clone(),
        snap,
    )
    .await;
    drop(mem_limiter_guard);
    if let Err(err) = result {
        error!("{} remote coprocessor failed, error {:?}", tag, err);
        if accept_pb {
            let cop_resp = tikv::coprocessor::make_error_response(err);
            let body = hyper::Body::from(cop_resp.write_to_bytes().unwrap());
            return Ok(hyper::Response::builder()
                .status(500)
                .header(CONTENT_TYPE, CONTENT_TYPE_PROTOBUF)
                .body(body)
                .unwrap());
        } else {
            let body = hyper::Body::from(format!("{:?}", err));
            return Ok(hyper::Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(body)
                .unwrap());
        }
    }

    let finish_time = Instant::now_coarse();
    let permit_wait_duration = snap_start.saturating_duration_since(handle_start);
    let snap_duration = prefetch_start.saturating_duration_since(snap_start);
    let prefetch_duration = process_start.saturating_duration_since(prefetch_start);
    let process_duration = finish_time.saturating_duration_since(process_start);
    let handle_duration = finish_time.saturating_duration_since(handle_start);
    REMOTE_COPR_SNAPSHOT_HISTOGRAM.observe(snap_duration.as_secs_f64());
    REMOTE_COPR_PREFETCH_HISTOGRAM.observe(prefetch_duration.as_secs_f64());
    REMOTE_COPR_REQ_HANDLE_HISTOGRAM.observe(handle_duration.as_secs_f64());
    let response = result.unwrap();
    info!(
        "finished remote coprocessor";
        "tag" => &tag,
        "req_size" => req_body.len(),
        "resp_size" => response.data.len(),
        "timeout" => ?timeout,
        "permit_wait_duration" => ?permit_wait_duration,
        "snap_duration" => ?snap_duration,
        "prefetch_duration" => ?prefetch_duration,
        "process_duration" => ?process_duration,
        "handle_duration" => ?handle_duration,
    );

    match req_type {
        REQ_TYPE_DAG => {
            REMOTE_COPR_DAG_REQ_COUNTER.inc();
            REMOTE_COPR_DAG_RESP_SIZE.inc_by(response.data.len() as u64);
        }
        REQ_TYPE_ANALYZE => {
            REMOTE_ANALYZE_REQ_COUNTER.inc();
            REMOTE_ANALYZE_RESP_SIZE.inc_by(response.data.len() as u64);
        }
        REQ_TYPE_CHECKSUM => {
            REMOTE_CHECKSUM_REQ_COUNTER.inc();
            REMOTE_CHECKSUM_RESP_SIZE.inc_by(response.data.len() as u64);
        }
        _ => {}
    }
    match response.write_to_bytes() {
        Ok(response_data) => {
            let resp = hyper::Response::builder()
                .status(200)
                .body(response_data.into())
                .unwrap();
            Ok(resp)
        }
        Err(err) => {
            error!("{} serialize response failed, error {:?}", tag, err);
            let body = hyper::Body::from(format!("{:?}", err));
            Ok(hyper::Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(body)
                .unwrap())
        }
    }
}

fn get_dfs_remote_cache_addr(parts: &http::request::Parts) -> Option<&str> {
    parts
        .headers
        .get(DFS_REMOTE_CACHE_ADDR_HEADER)?
        .to_str()
        .ok()
}

pub fn get_cop_req_tag(cop_req: &kvproto::coprocessor::Request) -> String {
    let cop_ctx = cop_req.get_context();
    let keyspace_id = cop_ctx.keyspace_id;
    let start_ts = cop_req.get_start_ts();
    let req_type = cop_req.get_tp();
    let req_type_str = match req_type {
        REQ_TYPE_DAG => "dag".to_string(),
        REQ_TYPE_ANALYZE => "analyze".to_string(),
        REQ_TYPE_CHECKSUM => "checksum".to_string(),
        _ => "".to_string(),
    };
    format!(
        "{} ks{}:{}:{}:{}",
        req_type_str,
        keyspace_id,
        cop_ctx.get_region_id(),
        cop_ctx.get_region_epoch().get_version(),
        start_ts,
    )
}

async fn handle_get_metrics(req: Request<Body>) -> hyper::Result<Response<Body>> {
    let gz_encoding = client_accept_gzip(&req);
    let metrics = if gz_encoding {
        // gzip can reduce the body size to less than 1/10.
        let mut encoder = GzEncoder::new(vec![], Compression::default());
        dump_to(&mut encoder, true);
        encoder.finish().unwrap()
    } else {
        dump(true).into_bytes()
    };
    let mut resp = Response::new(metrics.into());
    resp.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(TEXT_FORMAT));
    if gz_encoding {
        resp.headers_mut()
            .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }
    Ok(resp)
}

async fn handle_remote_compaction(
    ctx: Arc<Context>,
    req: hyper::Request<hyper::Body>,
    id_allocator: Arc<dyn IdAllocator>,
) -> hyper::Result<hyper::Response<hyper::Body>> {
    let thread_pool = ctx.thread_pool.clone();
    let dfs = ctx.dfs.clone();
    let compression_lvl = ctx.compression_lvl;
    let checksum_type = ctx.checksum_type;
    let master_key = ctx.master_key.clone();

    let req_body = hyper::body::to_bytes(req.into_body()).await?;
    let result = serde_json::from_slice(req_body.chunk());
    if result.is_err() {
        let err_str = result.unwrap_err().to_string();
        return Ok(hyper::Response::builder()
            .status(400)
            .body(err_str.into())
            .unwrap());
    }
    let comp_req: CompactionRequest = result.unwrap();

    if comp_req.compactor_version > CURRENT_COMPACTOR_VERSION {
        warn!(
            "received incompatible compactor-version({}). Upgrade tikv-worker (version:{}). Request: {:?}",
            comp_req.compactor_version, CURRENT_COMPACTOR_VERSION, comp_req,
        );
        let err_str = format!("incompatible compactor version ({CURRENT_COMPACTOR_VERSION})");
        return Ok(hyper::Response::builder()
            .status(INCOMPATIBLE_COMPACTOR_ERROR_CODE)
            .body(err_str.into())
            .unwrap());
    }

    if comp_req.input_size == 0 {
        warn!("input_size not set: {:?}", comp_req);
        // TODO: `debug_assert!(false)`.
    }

    let _permit = if let CompactionType::VectorIndex(vec_idx_update) = &comp_req.compaction_tp {
        // Try acquire permit without waiting. If failed, return ASAP to make the client
        // retry request to another worker instance.
        match ctx.worker_limiter.try_acquire_vector_index_permit() {
            Ok(permit) => Some(permit),
            Err(_) => {
                let err_str = "vector index compaction requests throttled";
                warn!("{}: {:?}", err_str, vec_idx_update);
                let body = hyper::Body::from(err_str);
                return Ok(hyper::Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .body(body)
                    .unwrap());
            }
        }
    } else {
        None
    };

    let memory_limiter = ctx.memory_limiter.clone();
    let request_size = comp_req.input_size * 2; // The memory usage is 2x input size for both reading and writing.
    let mem_limiter_guard = match memory_limiter.acquire(request_size) {
        Ok(guard) => guard,
        Err(exceeded_size) => {
            ENGINE_REMOTE_COMPACT_EXCEED_MEMORY_LIMIT_COUNTER.inc();
            warn!("{} memory limit exceeded", comp_req.get_tag();
                "request_size" => request_size, "exceeded" => exceeded_size,
                "limiter" => ?memory_limiter);
            let body = hyper::Body::from("memory limit exceeded");
            return Ok(hyper::Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(body)
                .unwrap());
        }
    };

    let encryption_key = if comp_req.exported_encryption_key.is_empty() {
        None
    } else {
        Some(
            master_key
                .decrypt_encryption_key(&comp_req.exported_encryption_key)
                .unwrap(),
        )
    };
    let comp_ctx = CompactionCtx {
        req: Arc::new(comp_req),
        dfs,
        compression_lvl,
        id_allocator,
        encryption_key,
        local_dirs: vec![],
        for_restore: false,
        checksum_type,
        columnar_meta_cache: ctx.columnar_meta_cache.clone(),
    };
    let task = thread_pool.spawn(tikv_util::init_task_local(async move {
        tikv_util::set_current_region(comp_ctx.req.shard_id);
        let _guard = mem_limiter_guard;
        local_compact(&comp_ctx).await
    }));
    match task.await {
        Ok(Ok(cs)) => {
            let data = cs.write_to_bytes().unwrap();
            Ok(hyper::Response::builder()
                .status(200)
                .body(data.into())
                .unwrap())
        }
        err @ Err(_) | err @ Ok(Err(_)) => {
            let err_str = format!("{:?}", err);
            error!("compaction failed {}", err_str);
            let body = hyper::Body::from(err_str);
            Ok(hyper::Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(body)
                .unwrap())
        }
    }
}

// check if the client allow return response with gzip compression
// the following logic is port from prometheus's golang:
// https://github.com/prometheus/client_golang/blob/24172847e35ba46025c49d90b8846b59eb5d9ead/prometheus/promhttp/http.go#L155-L176
fn client_accept_gzip(req: &Request<Body>) -> bool {
    let encoding = req
        .headers()
        .get(ACCEPT_ENCODING)
        .map(|enc| enc.to_str().unwrap_or_default())
        .unwrap_or_default();
    encoding
        .split(',')
        .map(|s| s.trim())
        .any(|s| s == "gzip" || s.starts_with("gzip;"))
}
