// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    borrow::Cow,
    cell::Cell,
    fmt::{self, Formatter},
    fs,
    fs::OpenOptions,
    io, ops,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use bstr::ByteSlice;
use bytes::{Buf, Bytes, BytesMut};
use chrono::{NaiveTime, Utc};
use collections::HashMap;
use engine_traits::{GetObjectOptions, ObjectCacheWithHook};
use etcd_client::{ConnectOptions, OpenSslClientConfig};
use grpcio::EnvBuilder;
use http::{Request, StatusCode};
use hyper::Body;
use kvengine::dfs::{self, Dfs};
use kvproto::{metapb, metapb::Store};
use pd_client::{PdClient, RpcClient};
use protobuf::Message;
use rfengine::{
    find_latest_snapshot, get_integral_wal_chunks, parse_delayed_to_epoch_from_snapshot_key,
    snapshot_store_meta_key, wal_chunk_file_prefix, wal_chunk_file_suffix, RfEngine, WalChunkMeta,
    MAX_EPOCH_BACKWARD,
};
use rfenginepb::{ClusterBackupMeta, StoreBackupMeta};
use rfstore::store::state::RaftState;
use security::{HttpClient, SecurityConfig, SecurityManager};
use slog_global::{error, warn};
use tikv_util::{
    box_err, box_try, box_try_join, codec::bytes::decode_bytes, debug, http::HeaderExt, info,
    time::Instant, Either,
};

use crate::{
    archive::{get_archived_wal_addresses, get_archived_wals_from_addresses, StoreMeta},
    backup::IncrementalBackupFile,
    error::{Error, HttpRequestError, Result},
    metrics::NATIVE_BR_RFENGINE_WAL_EPOCH_OVERWRITTEN_ERROR,
    wal::{
        assemble_wal_chunks_to_wal_file, AssembledWalData, LocalWal, LocalWalChunks, WalChunkData,
        WalOnlineChunk,
    },
};

const MAX_S3_REQ_BATCH_SIZE: usize = 1024;
pub const INCREMENTAL_BACKUP_FOLDER_FORMAT: &str = "%Y%m%d";
pub const INCREMENTAL_BACKUP_FILE_NAME_FORMAT: &str = "%H%M%S";

pub fn create_pd_client(security_conf: &SecurityConfig, pd_conf: &pd_client::Config) -> RpcClient {
    let security_mgr = Arc::new(
        SecurityManager::new(security_conf)
            .unwrap_or_else(|e| panic!("failed to create security manager: {:?}", e)),
    );
    let env = Arc::new(EnvBuilder::new().cq_count(1).build());
    RpcClient::new(pd_conf, Some(env), security_mgr)
        .unwrap_or_else(|e| panic!("failed to create rpc client: {:?}", e))
}

pub async fn send_request_to_store(
    req: Request<Body>,
    store: &Store,
    client: &HttpClient,
    timeout: Duration,
) -> Result<(StatusCode, Bytes)> {
    debug_assert!(!timeout.is_zero());
    let uri_str = format!("{}", req.uri());
    let resp = tokio::time::timeout(timeout, client.request(req))
        .await
        .map_err(|_| HttpRequestError::Timeout(format!("send request to {uri_str}"), timeout))?;
    if let Err(err) = resp {
        error!("send request to store failed"; "store" => store.id, "err" => ?err, "uri" => ?uri_str);
        return Err(HttpRequestError::Http(uri_str, err).into());
    }
    let resp = resp.unwrap();
    let is_pb_resp = resp.headers().is_content_type_protobuf();
    let status = resp.status();
    let body = tokio::time::timeout(timeout, hyper::body::to_bytes(resp.into_body()))
        .await
        .map_err(|_| HttpRequestError::Timeout(format!("read response from {uri_str}"), timeout))?;
    if !status.is_success() {
        let body = body.unwrap_or_default();
        if !is_pb_resp {
            let err_msg = body.to_str_lossy().to_string();
            error!("send request to store failed"; "store" => store.id, "uri" => ?uri_str,
                "err" => &err_msg, "status" => ?status);
            return Err(Error::HttpError(status, err_msg));
        } else {
            let mut err = kvproto::errorpb::Error::default();
            err.merge_from_bytes(body.as_ref()).map_err(|e| -> Error {
                debug_assert!(false, "body: {:?}: {:?}", body, e);
                box_err!("invalid errorpb::Error: {:?}", e)
            })?;
            error!("send request to store failed"; "store" => store.id, "uri" => ?uri_str,
                "err" => ?err, "status" => ?status);
            return Err(Error::HttpPbError(status, err));
        }
    }
    match body {
        Ok(body) => Ok((status, body)),
        Err(err) => {
            error!(
                "convert response failed, store {}, err {:?}, uri {:?}",
                store.id, err, uri_str,
            );
            Err(HttpRequestError::Http(uri_str, err).into())
        }
    }
}

/// Send request with timeout of `timeout / 2` for each retry.
pub async fn send_request_to_store_with_retry<F>(
    build_req: F,
    store: &Store,
    security_mgr: &SecurityManager,
    timeout: Duration,
) -> Result<Bytes>
where
    F: Fn() -> Request<Body>,
{
    send_request_to_store_with_retry_opt(build_req, store, Either::Left(security_mgr), timeout)
        .await
}

/// Send request with timeout of `timeout / 2` for each retry.
///
/// Perfer to `HttpClient`. Callers should reuse the client.
pub async fn send_request_to_store_with_retry_opt<F>(
    build_req: F,
    store: &Store,
    security_mgr_or_client: Either<&SecurityManager, &HttpClient>,
    timeout: Duration,
) -> Result<Bytes>
where
    F: Fn() -> Request<Body>,
{
    let is_error_retryable = |err: &Error| matches!(err, Error::HttpRequestError(_));
    let client = match security_mgr_or_client {
        Either::Left(security_mgr) => {
            Cow::Owned(security_mgr.http_client(hyper::Client::builder())?)
        }
        Either::Right(client) => Cow::Borrowed(client),
    };
    let mut last_err: Option<Error> = None;
    let start_time = Instant::now_coarse();
    while start_time.saturating_elapsed() < timeout {
        let req = build_req();
        match send_request_to_store(req, store, client.as_ref(), timeout / 2).await {
            Ok((_, resp)) => return Ok(resp),
            Err(err) if is_error_retryable(&err) => {
                last_err = Some(err);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(err) => return Err(err),
        }
    }
    Err(last_err.unwrap())
}

pub fn generate_etcd_connect_opt(security: &SecurityConfig) -> Result<ConnectOptions> {
    if security.ca_path.is_empty() {
        return Ok(ConnectOptions::new());
    }
    match security.load_certs() {
        Ok((ca, cert, key)) => Ok(ConnectOptions::new().with_openssl_tls(
            OpenSslClientConfig::default()
                .ca_cert_pem(&ca)
                .client_cert_pem_and_key(&cert, &key),
        )),
        Err(e) => Err(box_err!("Load security fail {:?}", e)),
    }
}

pub fn load_peer_raft_state(
    rf: &rfengine::RfEngine,
    peer_id: u64,
    region_version: u64,
) -> Option<RaftState> {
    let raft_state_key = rfengine::raft_state_key(region_version);
    let raft_state_val = rf.get_state(peer_id, &raft_state_key)?;
    let mut raft_state = RaftState::default();
    raft_state.unmarshal(raft_state_val.as_ref());
    Some(raft_state)
}

#[derive(Clone, Default, PartialEq)]
pub(crate) struct RawRegion {
    pub id: u64,
    pub raw_start: Vec<u8>,
    pub raw_end: Vec<u8>,
    pub epoch: metapb::RegionEpoch,
    pub peers: Vec<metapb::Peer>,
}

impl fmt::Debug for RawRegion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawRegion")
            .field("id", &self.id)
            .field(
                "raw_start",
                &log_wrappers::hex_encode_upper(&self.raw_start),
            )
            .field("raw_end", &log_wrappers::hex_encode_upper(&self.raw_end))
            .field("epoch", &self.epoch)
            .field("peers", &self.peers)
            .finish()
    }
}

impl RawRegion {
    pub fn get_start_key(&self) -> &[u8] {
        self.raw_start.as_slice()
    }

    pub fn get_end_key(&self) -> &[u8] {
        self.raw_end.as_slice()
    }

    /// Equal: `key` is within the boundary of the region.
    /// Less: region is to the left of `key`.
    /// Greater: region is to the right of `key`.
    pub fn compare_with_key(&self, key: &[u8]) -> std::cmp::Ordering {
        if self.raw_end.as_slice() <= key {
            std::cmp::Ordering::Less
        } else if self.raw_start.as_slice() > key {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    }
}

impl From<metapb::Region> for RawRegion {
    fn from(mut region: metapb::Region) -> Self {
        let raw_start = if region.start_key.is_empty() {
            vec![]
        } else {
            let mut slice = region.start_key.as_slice();
            decode_bytes(&mut slice, false).unwrap()
        };
        let raw_end = if region.end_key.is_empty() {
            vec![255; 8]
        } else {
            let mut slice = region.end_key.as_slice();
            decode_bytes(&mut slice, false).unwrap()
        };
        RawRegion {
            id: region.id,
            raw_start,
            raw_end,
            epoch: region.take_region_epoch(),
            peers: region.take_peers().into_vec(),
        }
    }
}

#[inline]
pub fn now() -> String {
    chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)
}

lazy_static::lazy_static! {
    pub static ref STEP_TO_STDOUT: AtomicBool = AtomicBool::new(false);
}

pub fn step_to_stdout() {
    STEP_TO_STDOUT.store(true, Ordering::Relaxed);
}

#[inline]
pub fn is_step_to_stdout() -> bool {
    STEP_TO_STDOUT.load(Ordering::Relaxed)
}

#[macro_export]
macro_rules! step( ($($args:tt)+) => {
    let msg = format!($($args)+);
    info!("{}", msg);
    if $crate::common::is_step_to_stdout() {
        println!("[{}] {}", now(), msg);
    }
};);

#[macro_export]
macro_rules! step_error( ($($args:tt)+) => {
    let msg = format!($($args)+);
    error!("{}", msg);
    if $crate::common::is_step_to_stdout() {
        eprintln!("[{}] {}", now(), msg);
    }
};);

pub fn retain_sst_files(files: Vec<TableFile>, dfs: Arc<dyn Dfs>) -> Result<usize> {
    let mut idx = 0;
    let mut total_cnt = 0;
    while idx < files.len() {
        let end_idx = std::cmp::min(files.len(), idx + MAX_S3_REQ_BATCH_SIZE);
        total_cnt += retain_sst_files_in_batch(&files[idx..end_idx], dfs.clone(), idx == 0)?;
        idx = end_idx;
    }
    Ok(total_cnt)
}

fn retain_sst_files_in_batch(
    files: &[TableFile],
    dfs: Arc<dyn Dfs>,
    first_batch: bool,
) -> Result<usize> {
    let runtime = dfs.get_runtime();
    let file_cnt = files.len();
    let mut handles = Vec::with_capacity(file_cnt);
    for f in files {
        let dfs = dfs.clone();
        let file_key = dfs.file_key(f.id, f.ftype);
        handles.push(runtime.spawn(async move { dfs.retain_file(&file_key).await }));
    }
    // To avoid too much request to cause s3 SlowDown issue.
    if !first_batch {
        std::thread::sleep(Duration::from_secs(1));
    }
    let mut succeed_cnt = 0;
    for (handle, f) in handles.into_iter().zip(files) {
        match runtime.block_on(handle).unwrap() {
            Ok(_) => succeed_cnt += 1,
            Err(e) => error!("Retain file {:?} fail {:?}", f, e),
        }
    }
    if succeed_cnt != file_cnt {
        return Err(box_err!(
            "Error occurs, succeed {}, totally {}",
            succeed_cnt,
            file_cnt
        ));
    }
    Ok(succeed_cnt)
}

async fn fetch_rfengine_wal_chunk(
    store: &Store,
    epoch_id: u32,
    start_off: u64,
    end_off: u64,
    security_mgr: &SecurityManager,
    timeout: Duration,
) -> Result<Bytes> {
    let uri = security_mgr.build_uri(format!(
        "{}/rfengine/wal_chunk?epoch_id={}&start_off={}&end_off={}",
        store.status_address, epoch_id, start_off, end_off
    ))?;
    let req = || Request::get(uri.clone()).body(Body::empty()).unwrap();
    // Use distinct error type for caller to decide whether to tolerate the error.
    send_request_to_store_with_retry(req, store, security_mgr, timeout)
        .await
        .map_err(|err| match err {
            Error::HttpError(status, msg) => {
                if status == StatusCode::GONE {
                    info!("fetch rfengine wal chunk: epoch overwritten"; "epoch_id" => epoch_id, "msg" => msg);
                    Error::RfengineWalEpochOverwritten {epoch_id}
                } else {
                    Error::RfengineHttpSvrError(format!("{status}:{msg}"))
                }
            }
            Error::HttpRequestError(e) => Error::RfengineHttpRequestError(e),
            _ => err,
        })
}

pub struct ReplayWalLogsContext<'a> {
    pub pd_client: Arc<dyn PdClient>,
    pub dfs: Arc<dyn Dfs>,
    pub store_id: u64,
    pub cluster_backup: &'a ClusterBackupMeta,
    pub rf_engine: &'a RfEngine,
    pub complete_wal_chunks: bool,
    pub full_restore: bool,
    pub fetch_wal_timeout: Duration,
    pub cache_dir: Option<PathBuf>,
    pub wal_chunks_cache: Option<ObjectCacheWithHook>,
    pub from_archive: bool,
}

// TODO: Filter out the write batches of specified keyspace to replay to save
// memory.
pub fn replay_wal_logs(
    tag: &str,
    ctx: ReplayWalLogsContext<'_>,
    archive_store_meta: Option<(String, StoreMeta)>, // archive date, archive store meta
    snap_epoch: u32,
) -> Result<()> {
    if let Some((date, store_meta)) = archive_store_meta {
        replay_wal_logs_from_archive(tag, &ctx, &date, store_meta)?;
    } else {
        replay_wal_logs_from_backup(tag, &ctx, snap_epoch)?;
    }
    Ok(())
}

pub fn replay_wal_logs_from_backup(
    tag: &str,
    ctx: &ReplayWalLogsContext<'_>,
    snap_epoch: u32,
) -> Result<()> {
    let store_meta = ctx
        .cluster_backup
        .get_stores()
        .iter()
        .find(|x| x.store_id == ctx.store_id)
        .expect("store not found");
    let backup_epoch = store_meta.get_epoch();
    let backup_offset = store_meta.get_offset();

    let collect_ctx = CollectWalChunksContext::from(ctx);

    // Replay epoch wal chunk files in order.
    // `snap_epoch` is the latest snapshot manifest epoch. If no snapshot found, the
    // `snap_epoch` is 0. Replay wal logs from `snap_epoch` + 1 to backup point.
    for epoch_id in snap_epoch + 1..=backup_epoch {
        let (chunks, online_chunk, _) = collect_wal_chunks_with_retry(
            tag,
            &collect_ctx,
            epoch_id,
            backup_epoch,
            backup_offset,
        )?;
        replay_wal_chunks(
            tag,
            ctx,
            chunks,
            online_chunk,
            epoch_id,
            backup_epoch,
            backup_offset,
        )?;
        info!("{} replay wal logs for epoch: done", tag; "epoch" => epoch_id);
    }
    Ok(())
}

fn replay_wal_logs_from_archive(
    tag: &str,
    ctx: &ReplayWalLogsContext<'_>,
    date: &str,
    store_meta: StoreMeta,
) -> Result<()> {
    let store_backup = ctx
        .cluster_backup
        .get_stores()
        .iter()
        .find(|x| x.store_id == ctx.store_id)
        .expect("store not found");
    let backup_epoch = store_backup.get_epoch();
    let backup_offset = store_backup.get_offset();

    let wal_addrs = get_archived_wal_addresses(&store_meta)?;
    for (epoch, addrs) in wal_addrs {
        let chunks =
            get_archived_wals_from_addresses(ctx.dfs.clone(), date, addrs, ctx.cache_dir.clone())?;
        replay_wal_chunks(tag, ctx, chunks, None, epoch, backup_epoch, backup_offset)?;
    }

    Ok(())
}

pub struct CollectWalChunksContext {
    pub pd_client: Arc<dyn PdClient>,
    pub dfs: Arc<dyn Dfs>,
    pub store_id: u64,
    pub complete_wal_chunks: bool,
    pub fetch_wal_timeout: Duration,
    pub cache_dir: Option<PathBuf>,
    pub wal_chunks_cache: Option<ObjectCacheWithHook>,
    /// Whether to retry for `Error::WalChunkIntegrityError`.
    pub retry_for_wal_chunk_integrity_error: bool,
}

impl From<&ReplayWalLogsContext<'_>> for CollectWalChunksContext {
    fn from(ctx: &ReplayWalLogsContext<'_>) -> Self {
        Self {
            pd_client: ctx.pd_client.clone(),
            dfs: ctx.dfs.clone(),
            store_id: ctx.store_id,
            complete_wal_chunks: ctx.complete_wal_chunks,
            fetch_wal_timeout: ctx.fetch_wal_timeout,
            cache_dir: ctx.cache_dir.clone(),
            wal_chunks_cache: ctx.wal_chunks_cache.clone(),
            retry_for_wal_chunk_integrity_error: true,
        }
    }
}

pub fn collect_wal_chunks_with_retry(
    tag: &str,
    ctx: &CollectWalChunksContext,
    epoch_id: u32,
    backup_epoch: u32,
    backup_offset: u64,
) -> Result<(
    Vec<WalChunkData>,
    Option<WalOnlineChunk>,
    bool, // has_last_chunk
)> {
    let start_time = Instant::now_coarse();

    let (chunk_metas, online_chunk) = if epoch_id == backup_epoch && !ctx.complete_wal_chunks {
        collect_wal_chunk_metas_with_online_rfengine(
            tag,
            ctx,
            backup_epoch,
            backup_offset,
            start_time,
        )?
    } else {
        let chunk_metas = collect_complete_wal_chunk_metas_with_retry(
            tag,
            ctx,
            epoch_id,
            backup_epoch,
            backup_offset,
            start_time,
        )?;
        (chunk_metas, None)
    };

    let chunks_data = with_retry(
        tag,
        || {
            if let Some(cache_dir) = &ctx.cache_dir {
                collect_all_chunk_files_with_cache_dir(
                    ctx.dfs.clone(),
                    epoch_id,
                    chunk_metas.clone(),
                    cache_dir,
                )
            } else {
                collect_all_chunk_files(
                    ctx.dfs.clone(),
                    epoch_id,
                    chunk_metas.clone(),
                    ctx.wal_chunks_cache.as_ref(),
                )
            }
        },
        |err| {
            warn!("{} collect wal chunks: collect chunk files failed", tag; "err" => ?err);
            true
        },
        |dur| dur + dur,
        Duration::from_secs(1),
        ctx.fetch_wal_timeout,
        Some(start_time),
    )?;

    let has_last_chunk = chunk_metas.last().is_some_and(|meta| meta.last);
    Ok((chunks_data, online_chunk, has_last_chunk))
}

/// `end_off`:
/// - 0: Get the current chunks (must be continuous).
/// - u64::MAX: Get all chunks of the epoch, the ".last" chunk must be included.
/// - Other value: Get all chunks with offset <= the value.
///
/// Return: the `end_off` of the last returned chunk.
fn collect_wal_chunk_metas(
    tag: &str,
    ctx: &CollectWalChunksContext,
    epoch_id: u32,
    end_off: u64,
) -> Result<(Vec<WalChunkMeta>, u64 /* last_end_off */)> {
    let dfs_prefix = format!("{}/", ctx.dfs.get_prefix());
    let scan_prefix = wal_chunk_file_prefix(ctx.store_id, epoch_id);
    let scan_start = wal_chunk_file_suffix(0, 0);
    info!(
        "{} replay_wal_logs list chunks with prefix {} start_after {} epoch {} end_off {}",
        tag, scan_prefix, scan_start, epoch_id, end_off
    );

    let chunk_keys = match ctx.dfs.list_objects(&scan_start, Some(&scan_prefix), None) {
        Ok((chunks, _)) => chunks
            .iter()
            .map(|chunk| {
                chunk
                    .key
                    .as_str()
                    .strip_prefix(&dfs_prefix)
                    .unwrap_or_default()
                    .to_string()
            })
            .collect::<Vec<_>>(),
        Err(err) => {
            error!("{} list wal chunk files failed: {:?}", tag, err);
            return Err(box_err!("list wal chunk files failed: {:?}", err));
        }
    };
    let mut chunk_metas: Vec<WalChunkMeta> = Vec::with_capacity(chunk_keys.len());
    for key in chunk_keys {
        chunk_metas.push(box_try!(key.try_into()));
    }

    // Get integral WAL chunk files.
    let check_last = end_off == u64::MAX;
    let (integral_chunks, last_end_off, has_last_chunk) =
        get_integral_wal_chunks(&chunk_metas, epoch_id);
    let ok = if check_last {
        has_last_chunk
    } else {
        last_end_off >= end_off
    };
    if ok {
        Ok((integral_chunks, last_end_off))
    } else {
        error!("{} collect wal chunk keys: chunks not ready", tag;
            "epoch" => epoch_id, "end_off" => end_off, "last_end_off" => last_end_off, "integral_chunks" => ?integral_chunks);
        Err(Error::WalChunkIntegrityError(format!(
            "chunks not ready for offset {end_off}"
        )))
    }
}

fn collect_complete_wal_chunk_metas_with_retry(
    tag: &str,
    ctx: &CollectWalChunksContext,
    epoch_id: u32,
    backup_epoch: u32,
    backup_offset: u64,
    start_time: Instant,
) -> Result<Vec<WalChunkMeta>> {
    let end_off = if epoch_id == backup_epoch {
        backup_offset
    } else {
        u64::MAX
    };

    let (chunk_metas, _) = with_retry(
        tag,
        || collect_wal_chunk_metas(tag, ctx, epoch_id, end_off),
        |err| -> bool {
            match err {
                Error::WalChunkIntegrityError(msg) => {
                    let retry = ctx.retry_for_wal_chunk_integrity_error;
                    warn!("{} collect complete wal chunk keys: wal chunk integrity check failed", tag;
                        "msg" => msg, "retry" => retry);
                    retry
                }
                _ => {
                    error!("{} collect complete wal chunk keys: failed", tag; "err" => ?err);
                    false
                }
            }
        },
        |dur| dur + dur,
        Duration::from_secs(1),
        ctx.fetch_wal_timeout,
        Some(start_time),
    )?;
    Ok(chunk_metas)
}

fn collect_wal_chunk_metas_with_online_rfengine(
    tag: &str,
    ctx: &CollectWalChunksContext,
    backup_epoch: u32,
    backup_offset: u64,
    start_time: Instant,
) -> Result<(Vec<WalChunkMeta>, Option<WalOnlineChunk>)> {
    // Note: the chunks of last epoch (i.e., backup_epoch) can be empty. All chunks
    // are fetched from online rfengine.
    let (chunk_metas, last_end_off) = with_retry(
        tag,
        || collect_wal_chunk_metas(tag, ctx, backup_epoch, 0),
        |err| -> bool {
            match err {
                Error::WalChunkIntegrityError(msg) => {
                    info!("{} collect wal chunk keys: wal chunk integrity check failed", tag; "msg" => msg);
                    true
                }
                _ => {
                    error!("{} collect wal chunk keys: failed", tag; "err" => ?err);
                    false
                }
            }
        },
        |dur| dur + dur,
        Duration::from_secs(1),
        ctx.fetch_wal_timeout,
        Some(start_time),
    )?;

    if last_end_off >= backup_offset {
        return Ok((chunk_metas, None));
    }

    let rf_epoch_unavailable = Cell::new(false);

    let store = ctx.pd_client.get_store(ctx.store_id).map_err(|err| {
        error!("{} collect wal chunks: get store failed", tag; "err" => ?err);
        Error::PdError(err)
    })?;
    let runtime = ctx.dfs.get_runtime();
    let security_mgr = ctx.pd_client.get_security_mgr();
    let fetch = || -> Result<Either<WalOnlineChunk, Vec<WalChunkMeta>>> {
        if !rf_epoch_unavailable.get() {
            let online_chunk = runtime.block_on(fetch_rfengine_wal_chunk(
                &store,
                backup_epoch,
                last_end_off,
                backup_offset,
                security_mgr.as_ref(),
                ctx.fetch_wal_timeout,
            ))?;
            info!("{} fetched online wal chunk from rfengine", tag;
                "start_off" => last_end_off, "end_off" => backup_offset, "data_len" => online_chunk.len());
            Ok(Either::Left(WalOnlineChunk {
                data: online_chunk,
                start_off: last_end_off,
            }))
        } else {
            let (chunk_metas, last_end_off) =
                collect_wal_chunk_metas(tag, ctx, backup_epoch, backup_offset)?;
            debug_assert!(last_end_off >= backup_offset);
            Ok(Either::Right(chunk_metas))
        }
    };

    let on_error = |err: &Error| -> bool {
        match err {
            Error::RfengineWalEpochOverwritten { epoch_id } => {
                debug_assert_eq!(*epoch_id, backup_epoch);
                // The epoch has been overwritten. Which means that the dfs worker of rfengine
                // is much slower than async WAL writer.
                // Retry to collect WAL chunk from DFS and wait for dfs worker to catch up.
                NATIVE_BR_RFENGINE_WAL_EPOCH_OVERWRITTEN_ERROR.inc();
                info!("{} collect wal chunk: epoch of rfengine is overwritten", tag; "epoch" => epoch_id);
                rf_epoch_unavailable.set(true);
                true
            }
            Error::RfengineHttpSvrError(msg) => {
                error!("{} collect wal chunk: rfengine svr error", tag; "msg" => msg);
                rf_epoch_unavailable.set(true);
                true
            }
            Error::WalChunkIntegrityError(msg) => {
                info!("{} collect wal chunk: chunk not ready", tag; "msg" => msg);
                true
            }
            err => {
                error!("{} collect wal chunk: failed", tag; "err" => ?err);
                false
            }
        }
    };
    Ok(
        match with_retry(
            tag,
            fetch,
            on_error,
            |dur| dur + dur,
            Duration::from_secs(1),
            ctx.fetch_wal_timeout,
            Some(start_time),
        )? {
            Either::Left(online_chunk) => (chunk_metas, Some(online_chunk)),
            Either::Right(new_chunk_metas) => (new_chunk_metas, None),
        },
    )
}

// Collect wal chunk files of the specified epoch concurrently and return the
// chunks data in order. The data is compressed with lz4 and all chunks data
// size about 512MB (see `target_file_size`) at most, so it's safe to keep all
// data in memory.
fn collect_all_chunk_files(
    dfs: Arc<dyn Dfs>,
    epoch_id: u32,
    chunk_metas: Vec<WalChunkMeta>,
    wal_chunks_cache: Option<&ObjectCacheWithHook>,
) -> Result<Vec<WalChunkData>> {
    let chunk_metas_with_option = chunk_metas
        .into_iter()
        .map(|chunk| (chunk, GetObjectOptions::default()))
        .collect::<Vec<_>>();

    let runtime = dfs.get_runtime();
    let mut chunks = runtime.block_on(get_objects_with_cache(
        dfs.clone(),
        chunk_metas_with_option,
        wal_chunks_cache,
    ))?;
    chunks.sort_by(|a, b| a.0.start_off.cmp(&b.0.start_off));
    let chunks_data = chunks
        .into_iter()
        .map(|(meta, bytes)| {
            info!(
                "collect wal chunk file {} epoch {} size {}",
                meta.key,
                epoch_id,
                bytes.len()
            );
            WalChunkData::MemoryWithMeta { meta, bytes }
        })
        .collect::<Vec<_>>();
    Ok(chunks_data)
}

fn collect_all_chunk_files_with_cache_dir(
    dfs: Arc<dyn Dfs>,
    epoch_id: u32,
    chunk_metas: Vec<WalChunkMeta>,
    cache_dir: &Path,
) -> Result<Vec<WalChunkData>> {
    let chunk_metas_with_option = chunk_metas
        .into_iter()
        .map(|chunk| (chunk, GetObjectOptions::default()))
        .collect::<Vec<_>>();

    let runtime = dfs.get_runtime();
    let mut chunks = runtime.block_on(get_objects_to_files(
        dfs.clone(),
        chunk_metas_with_option,
        cache_dir,
    ))?;
    chunks.sort_by(|a, b| a.0.start_off.cmp(&b.0.start_off));
    let chunks_data = chunks
        .into_iter()
        .map(|(meta, local_obj)| {
            info!(
                "collect wal chunk file {} epoch {} size {} (to local)",
                meta.key, epoch_id, local_obj.len
            );
            WalChunkData::LocalFile { meta, local_obj }
        })
        .collect::<Vec<_>>();
    Ok(chunks_data)
}

pub fn assemble_wal_chunks(
    chunks: Vec<WalChunkData>,
    use_object_cache: bool,
) -> Result<AssembledWalData> {
    if chunks.is_empty() {
        return Ok(AssembledWalData::BytesMut(BytesMut::new()));
    }

    let in_memory = chunks.first().unwrap().in_memory();
    Ok(match (in_memory, use_object_cache) {
        (true, false) => {
            let memory_chunks = chunks
                .into_iter()
                .map(|c| c.must_get_bytes())
                .collect::<Vec<_>>();
            let epoch_wal = rfengine::assemble_wal_chunks(memory_chunks)?;
            AssembledWalData::BytesMut(epoch_wal)
        }
        (true, true) => {
            let wal_chunks = chunks
                .into_iter()
                .map(|x| x.must_get_bytes_as_data_holder())
                .collect::<Vec<_>>();
            AssembledWalData::LocalChunks(LocalWalChunks::new(wal_chunks))
        }
        (false, _) => {
            let without_meta = chunks.first().unwrap().without_meta();
            if without_meta {
                let wal_chunks = chunks
                    .into_iter()
                    .map(|x| x.must_into_local_file_without_meta())
                    .collect::<Vec<_>>();
                let local_wal = assemble_wal_chunks_to_wal_file(wal_chunks)?;
                AssembledWalData::Local(LocalWal::new(local_wal))
            } else {
                let wal_chunks = chunks
                    .into_iter()
                    .map(|x| x.must_into_local_file())
                    .collect::<Vec<_>>();
                AssembledWalData::LocalChunks(LocalWalChunks::new(wal_chunks))
            }
        }
    })
}

fn replay_wal_chunks(
    tag: &str,
    ctx: &ReplayWalLogsContext<'_>,
    chunks: Vec<WalChunkData>,
    online_chunk: Option<WalOnlineChunk>,
    epoch_id: u32,
    backup_epoch: u32,
    backup_offset: u64,
) -> Result<()> {
    // Assemble WAL chunks in memory.
    // TODO: support archive.
    let mut epoch_wal =
        assemble_wal_chunks(chunks, ctx.wal_chunks_cache.is_some() && !ctx.from_archive)?;
    info!(
        "{} assemble wal from chunks done, epoch {} wal size {} backup_epoch {} backup_offset {}",
        tag,
        epoch_id,
        epoch_wal.len(),
        backup_epoch,
        backup_offset,
    );

    let end_offset = if backup_epoch == epoch_id {
        if let Some(online_chunk) = online_chunk {
            epoch_wal.push_online_chunk(online_chunk);
            if backup_offset != epoch_wal.len() as u64 {
                error!("{} replay wal chunks: unexpected length of epoch WAL", tag;
                    "epoch" => epoch_id, "epoch_wal" => epoch_wal.len(),
                    "backup_epoch" => backup_epoch, "backup_offset" => backup_offset);
                return Err(Error::Other(box_err!("unexpected length of WAL chunks")));
            }
        } else if backup_offset > epoch_wal.len() as u64 {
            error!("{} replay wal chunks: unexpected length of epoch WAL", tag;
                    "epoch" => epoch_id, "epoch_wal" => epoch_wal.len(),
                    "backup_epoch" => backup_epoch, "backup_offset" => backup_offset);
            return Err(Error::Other(box_err!("unexpected length of WAL chunks")));
        }
        backup_offset
    } else {
        // This is a previous epoch, replay all chunks.
        u64::MAX
    };

    info!(
        "{} replay wal chunk file for epoch {} size {} end_offset {}",
        tag,
        epoch_id,
        epoch_wal.len(),
        end_offset
    );
    epoch_wal.freeze();
    let wal_reader = epoch_wal.reader();
    ctx.rf_engine
        .replay_wal_file(wal_reader, epoch_id, end_offset, ctx.full_restore)?;

    Ok(())
}

pub fn collect_snapshot_meta_rlog_files(
    dfs: Arc<dyn Dfs>,
    prefix: &str,
    cluster_backup: &ClusterBackupMeta,
    store_id: u64,
    object_cache_no_hook: Option<&ObjectCacheWithHook>,
) -> Result<StoreRlog> {
    // Must have no hook.
    debug_assert!(object_cache_no_hook.map_or(true, |x| !x.has_hook()));

    let store_meta = cluster_backup
        .get_stores()
        .iter()
        .find(|x| x.store_id == store_id)
        .ok_or(rfengine::Error::Other(format!(
            "store {} not found",
            store_id
        )))?;
    let epoch_id = store_meta.get_epoch();

    info!(
        "try to find snapshot before backup epoch {} for store {} ",
        epoch_id, store_id,
    );
    let snap_key = find_latest_snapshot(dfs.clone(), prefix, store_id, epoch_id)?;
    let snap_delayed_to_epoch = parse_delayed_to_epoch_from_snapshot_key(snap_key.as_deref());
    if snap_delayed_to_epoch.is_none() {
        // If no snapshot available and the epoch_id > MAX_EPOCH_BACKWARD, it means
        // data incomplete.
        if epoch_id > MAX_EPOCH_BACKWARD {
            let msg = format!(
                "no snapshot available from epoch {} for store {}",
                epoch_id - MAX_EPOCH_BACKWARD,
                store_id
            );
            tikv_util::error!("{}", msg);
            return Err(Error::NoSnapshotAvailableError(msg));
        }
        info!(
            "no snapshot available from epoch 1 for store {}, create empty snap file",
            store_id
        );
        return Ok(StoreRlog {
            store_id,
            ..Default::default()
        });
    }
    let snap_delayed_to_epoch = snap_delayed_to_epoch.unwrap();
    info!(
        "fetch last snapshot {} snap_delayed_to_epoch {} before backup epoch {} for store {}",
        snap_key.as_deref().unwrap(),
        snap_delayed_to_epoch,
        epoch_id,
        store_id
    );
    let store_meta_key = snapshot_store_meta_key(store_id, snap_delayed_to_epoch);
    let full_key = format!("{}/{}", dfs.get_prefix(), store_meta_key);
    let meta_data = dfs
        .get_runtime()
        .block_on(dfs.get_object_with_cache(
            full_key,
            store_meta_key.clone(),
            GetObjectOptions::default(),
            object_cache_no_hook,
        ))
        .map_err(|e| {
            tikv_util::error!(
                "failed to collect snapshot meta {} for store {}, err {}",
                store_meta_key,
                store_id,
                e.to_string()
            );
            Error::DfsError(e)
        })?;
    let mut store_backup_meta = StoreBackupMeta::default();
    store_backup_meta
        .merge_from_bytes(meta_data.chunk())
        .unwrap();
    let snap_epoch = store_backup_meta.get_manifest().get_epoch_id();
    let delayed_epoches = store_backup_meta.get_manifest().get_delayed_epoches();
    assert_eq!(snap_delayed_to_epoch, snap_epoch + delayed_epoches);

    info!(
        "collect snapshot rlog {:?} for store {}",
        snap_key.as_deref().unwrap(),
        store_id,
    );
    let raft_file_key = snap_key.unwrap();
    let full_key = format!("{}/{}", dfs.get_prefix(), raft_file_key);
    let rlog_data = dfs
        .get_runtime()
        .block_on(dfs.get_object_with_cache(
            full_key,
            raft_file_key.clone(),
            GetObjectOptions::default(),
            object_cache_no_hook,
        ))
        .map_err(|e| {
            tikv_util::error!(
                "failed to collect snapshot rlog {} for store {}, err {}",
                raft_file_key,
                store_id,
                e.to_string()
            );
            Error::DfsError(e)
        })?;
    Ok(StoreRlog {
        store_id,
        snap_epoch,
        snap_meta: meta_data,
        snap_rlog: rlog_data,
    })
}

#[derive(Default, Clone)]
pub struct StoreWalRlog {
    pub store_id: u64,
    pub snap_epoch: u32,
    pub snap_meta: Bytes,
    pub snap_rlog: Bytes,
    pub wals: Vec<(u32, Vec<Bytes>)>, // Vec<(wal_epoch, Vec<wal_chunks>)>
}

impl fmt::Display for StoreWalRlog {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "store {} wal rlog files, snap epoch {}, meta {}, rlog {}, wals {}",
            self.store_id,
            self.snap_epoch,
            self.snap_meta.len(),
            self.snap_rlog.len(),
            self.wals.len()
        )
    }
}

impl StoreWalRlog {
    pub fn new(
        store_id: u64,
        snap_epoch: u32,
        snap_meta: Bytes,
        snap_rlog: Bytes,
        wals: Vec<(u32, Vec<Bytes>)>,
    ) -> Self {
        Self {
            store_id,
            snap_epoch,
            snap_meta,
            snap_rlog,
            wals,
        }
    }
}

#[derive(Default, Clone)]
pub struct StoreRlog {
    pub store_id: u64,
    pub snap_epoch: u32,
    pub snap_meta: Bytes,
    pub snap_rlog: Bytes,
}

impl fmt::Debug for StoreRlog {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreRlog")
            .field("store_id", &self.store_id)
            .field("snap_epoch", &self.snap_epoch)
            .field("snap_meta_len", &self.snap_meta.len())
            .field("snap_rlog_len", &self.snap_rlog.len())
            .finish()
    }
}

pub fn collect_store_wal_rlog_files(
    tag: &str,
    pd_client: Arc<dyn PdClient>,
    dfs: Arc<dyn Dfs>,
    cluster_backup: &ClusterBackupMeta,
    store_id: u64,
    timeout: Duration,
) -> Result<StoreWalRlog> {
    // collect snap files.
    let store_rlog = collect_snapshot_meta_rlog_files(
        dfs.clone(),
        &dfs.get_prefix(),
        cluster_backup,
        store_id,
        None,
    )?;

    // collect wal chunks files
    let store_meta = cluster_backup
        .get_stores()
        .iter()
        .find(|x| x.store_id == store_id)
        .expect("store not found");
    let backup_epoch = store_meta.get_epoch();
    let backup_offset = store_meta.get_offset();
    let mut wals = Vec::with_capacity((backup_epoch - store_rlog.snap_epoch) as usize);
    let ctx = CollectWalChunksContext {
        pd_client,
        dfs,
        store_id,
        complete_wal_chunks: true,
        fetch_wal_timeout: timeout,
        cache_dir: None, // TODO: cache_dir
        wal_chunks_cache: None,
        retry_for_wal_chunk_integrity_error: true,
    };
    // `snap_epoch` is the latest snapshot manifest epoch. If no snapshot found, the
    // `snap_epoch` is 0. Replay wal logs from `snap_epoch` + 1 to backup point.
    for epoch_id in store_rlog.snap_epoch + 1..=backup_epoch {
        let (epoch_wals, online_chunk, _) =
            collect_wal_chunks_with_retry(tag, &ctx, epoch_id, backup_epoch, backup_offset)?;
        debug_assert!(online_chunk.is_none());
        let epoch_wals = epoch_wals
            .into_iter()
            .map(|c| c.must_get_bytes())
            .collect::<Vec<_>>();
        wals.push((epoch_id, epoch_wals))
    }

    let store_wal_rlog_files = StoreWalRlog::new(
        store_id,
        store_rlog.snap_epoch,
        store_rlog.snap_meta,
        store_rlog.snap_rlog,
        wals,
    );
    info!("collect {}", store_wal_rlog_files);
    Ok(store_wal_rlog_files)
}

/// Return full path of incremental backups in S3.
///
/// Note: The `start_date + start_time` are inclusive.
pub async fn get_all_incremental_backups(
    dfs: &dyn Dfs,
    start_date: &chrono::NaiveDate,
    start_time: Option<&NaiveTime>,
    max_count: usize,
) -> dfs::Result<(Vec<IncrementalBackupFile>, bool)> {
    let mut files = Vec::with_capacity(std::cmp::min(max_count, 1000));
    // No ".meta" postfix to make `start_date + start_time` inclusive.
    let mut start_key = format!(
        "{}/{}",
        start_date.format(INCREMENTAL_BACKUP_FOLDER_FORMAT),
        start_time
            .map(|t| t.format(INCREMENTAL_BACKUP_FILE_NAME_FORMAT).to_string())
            .unwrap_or_default()
    );
    let prefix = "backup/";

    let mut reach_limit = false;
    loop {
        // TODO: pass in `max_count` for limit.
        match dfs.list(&start_key, Some(prefix), None).await {
            Ok((backup_files, more, next_start_after)) => {
                let mut inc_files = backup_files
                    .into_iter()
                    .filter_map(|f| IncrementalBackupFile::try_from_full_path(&f.key))
                    .collect::<Vec<_>>();
                files.append(&mut inc_files);
                if files.len() > max_count {
                    files.truncate(max_count);
                    reach_limit = true;
                }
                if reach_limit || !more {
                    break;
                }
                start_key = next_start_after.unwrap();
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok((files, reach_limit))
}

// If backup exist, return the latest one, else create a new ClusterBackupMeta.
pub async fn get_latest_backup_meta(dfs: &dyn Dfs, cluster_id: u64) -> Result<ClusterBackupMeta> {
    let now = Utc::now();
    let (files, _) = get_all_incremental_backups(dfs, &now.date_naive(), None, usize::MAX).await?;
    if files.is_empty() {
        return Err(Error::MetaNotFound(cluster_id));
    }
    // Incremental backup file name is generated with `backup_file_full_path` named
    // by creation time. The last should be the latest one.
    let last_file = files.last().unwrap();
    let full_path = last_file.full_path(&dfs.get_prefix());
    let object = dfs
        .get_object(
            full_path.clone(),
            full_path.clone(),
            engine_traits::GetObjectOptions::default(),
        )
        .await?;
    let mut meta = ClusterBackupMeta::new();
    meta.merge_from_bytes(&object).unwrap();
    if meta.cluster_id != cluster_id {
        return Err(Error::MetaNotFound(cluster_id));
    }

    info!(
        "Get cluster {} latest backup meta {}, store cnt {}",
        meta.cluster_id,
        full_path,
        meta.stores.len()
    );
    Ok(meta)
}

pub fn check_store_id_exists(dfs: &dyn Dfs, store_id: u64) -> Result<bool> {
    let prefix = format!("store_backup/{:016x}/", store_id);
    let (files, ..) = dfs
        .get_runtime()
        .block_on(dfs.list("", Some(&prefix), None))?;
    Ok(!files.is_empty())
}

#[derive(Debug)]
pub struct StorePeer {
    pub store_id: u64,
    pub peer_id: u64,
}

#[derive(Clone)]
pub struct RegionMetaGetter {
    pub shard_store_map: Arc<HashMap<u64 /* shard_id */, StorePeer>>,
    pub raft_engines: Arc<HashMap<u64 /* store_id */, rfengine::RfEngine>>,
}

impl fmt::Debug for RegionMetaGetter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardMetaGetter")
            .field("shard_store_map", &self.shard_store_map)
            .field(
                "raft_engines",
                &self.raft_engines.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl RegionMetaGetter {
    pub fn load_region_meta(
        &self,
        shard_id: u64,
        shard_ver: u64,
        kvengine_store_id: u64,
    ) -> Option<metapb::Region> {
        self.shard_store_map.get(&shard_id).and_then(|sp| {
            self.raft_engines.get(&sp.store_id).map(|rf| {
                let mut region_state =
                    rf.load_region_state(sp.peer_id, shard_ver)
                        .unwrap_or_else(|| {
                            panic!(
                                "{}:{} failed to get region state, state key {:?}",
                                shard_id,
                                shard_ver,
                                rfengine::region_state_key(shard_ver),
                            );
                        });
                let mut region = region_state.take_region();

                // Filter the leader peer by store id.
                let mut peers = region.take_peers().into_vec();
                peers.retain(|p| p.store_id == sp.store_id);
                assert!(
                    peers.len() == 1,
                    "leader peer not found, region {:?}, store_id {}",
                    rf.load_region_state(sp.peer_id, shard_ver).unwrap(),
                    sp.store_id
                );
                // Move the peer to the same store with kvengine.
                // To keep the region meta consistent with kvengine during applying committing
                // locks.
                peers[0].store_id = kvengine_store_id;
                region.mut_peers().push(peers.pop().unwrap());

                region
            })
        })
    }
}

#[derive(Clone, Debug)]
pub struct TableFile {
    pub(crate) id: u64,
    pub(crate) ftype: kvengine::dfs::FileType,
    pub(crate) shard_id: u64,
}

pub fn with_retry<T, E, F, FnOnError, FnNextSleep>(
    tag: &str,
    f: F,
    on_error: FnOnError,
    next_sleep: FnNextSleep,
    first_sleep: Duration,
    timeout: Duration,
    start_time: Option<Instant>,
) -> std::result::Result<T, E>
where
    E: std::fmt::Debug,
    F: Fn() -> std::result::Result<T, E>,
    FnOnError: Fn(&E) -> bool, // Return: retryable
    FnNextSleep: Fn(Duration) -> Duration,
{
    let start_time = start_time.unwrap_or_else(|| Instant::now_coarse());
    let mut sleep_dur = first_sleep;
    loop {
        match f() {
            Ok(t) => return Ok(t),
            Err(err) => {
                let retryable = on_error(&err);
                let elapsed = start_time.saturating_elapsed();
                debug!("{}: failed", tag; "err" => ?err, "retryable" => retryable, "takes" => ?elapsed);
                if !retryable || elapsed >= timeout {
                    return Err(err);
                }
                std::thread::sleep(sleep_dur);
                sleep_dur = next_sleep(sleep_dur);
            }
        }
    }
}

#[derive(Clone)]
pub struct LocalObject {
    pub file: Option<Arc<fs::File>>,
    pub path: Arc<PathBuf>,
    pub len: u64,
}

impl LocalObject {
    pub fn file(&mut self, read_only: bool) -> io::Result<&fs::File> {
        if self.file.is_none() {
            let file = fs::OpenOptions::new()
                .read(true)
                .write(!read_only)
                .open(self.path.as_ref())?;
            if self.len != file.metadata()?.len() {
                error!(
                    "file length changed, path {:?}, expect {}, got {}",
                    self.path.as_ref(),
                    self.len,
                    file.metadata()?.len()
                );
                debug_assert!(false);
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "file length changed".to_string(),
                ));
            }
            self.file = Some(Arc::new(file));
        }
        Ok(self.file.as_ref().unwrap())
    }

    pub fn close(&mut self) {
        let _ = self.file.take();
    }
}

// Note: Should not be clonable. Otherwise, the inner file will be removed more
// that once.
pub struct TempLocalObject {
    object: LocalObject,
}

impl ops::Deref for TempLocalObject {
    type Target = LocalObject;

    fn deref(&self) -> &Self::Target {
        &self.object
    }
}

impl ops::DerefMut for TempLocalObject {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.object
    }
}

impl Drop for TempLocalObject {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.path.as_ref());
    }
}

impl From<LocalObject> for TempLocalObject {
    fn from(object: LocalObject) -> Self {
        Self { object }
    }
}

impl TempLocalObject {
    pub fn create(path: PathBuf) -> io::Result<Self> {
        let f = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        let object = LocalObject {
            file: Some(Arc::new(f)),
            path: Arc::new(path),
            len: 0,
        };
        Ok(Self { object })
    }

    pub(crate) fn clone_inner(&self) -> LocalObject {
        self.object.clone()
    }
}

impl io::Write for TempLocalObject {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.object.file(false)?.write(buf)?;
        self.len += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.object.file(false)?.flush()
    }
}

pub trait ObjectMeta {
    fn key(&self) -> &str;
}

impl ObjectMeta for WalChunkMeta {
    fn key(&self) -> &str {
        &self.key
    }
}

// Note: The order of metas in result will change.
pub async fn get_objects_to_files<M>(
    dfs: Arc<dyn Dfs>,
    metas: Vec<(M, GetObjectOptions)>,
    dir: &Path,
) -> Result<Vec<(M, TempLocalObject)>>
where
    M: ObjectMeta + Clone + Send + 'static,
{
    let get_object = |meta: M, opts: GetObjectOptions| {
        let meta_key = meta.key().to_string();
        let full_key = format!("{}/{}", dfs.get_prefix(), meta_key);
        let path = dir.join(meta_key.replace('/', "_"));
        let dfs = dfs.clone();
        async move {
            match dfs
                .get_object_to_path(full_key, meta_key, opts, &path)
                .await
            {
                Ok(len) => {
                    let local_obj = LocalObject {
                        file: None,
                        path: Arc::new(path),
                        len,
                    };
                    Ok((meta, TempLocalObject::from(local_obj)))
                }
                Err(err) => Err(Error::DfsError(err)),
            }
        }
    };

    if metas.len() == 1 {
        let mut metas = metas;
        let (key, opts) = metas.pop().unwrap();
        return get_object(key, opts).await.map(|res| vec![res]);
    }

    let mut join_set = tokio::task::JoinSet::new();
    for (meta, opts) in metas {
        join_set.spawn_on(get_object(meta, opts), dfs.get_runtime().handle());
    }

    let mut objects = vec![];
    while let Some(res) = join_set.join_next().await {
        let res = box_try_join!(res)?;
        objects.push(res);
    }
    Ok(objects)
}

// Note: The order of metas in result will change.
// TODO: eliminate duplicated codes with `get_objects_to_files`.
pub async fn get_objects_with_cache<M>(
    dfs: Arc<dyn Dfs>,
    metas: Vec<(M, GetObjectOptions)>,
    cache: Option<&ObjectCacheWithHook>,
) -> Result<Vec<(M, Bytes)>>
where
    M: ObjectMeta + Clone + Send + 'static,
{
    let get_object = |meta: M, opts: GetObjectOptions, cache: Option<&ObjectCacheWithHook>| {
        let meta_key = meta.key().to_string();
        let full_key = format!("{}/{}", dfs.get_prefix(), meta_key);
        let dfs = dfs.clone();
        let cache = cache.cloned();
        async move {
            match dfs
                .get_object_with_cache(full_key, meta_key, opts, cache.as_ref())
                .await
            {
                Ok(data) => Ok((meta, data)),
                Err(err) => Err(Error::DfsError(err)),
            }
        }
    };

    if metas.len() == 1 {
        let mut metas = metas;
        let (key, opts) = metas.pop().unwrap();
        return get_object(key, opts, cache).await.map(|res| vec![res]);
    }

    let mut join_set = tokio::task::JoinSet::new();
    for (meta, opts) in metas {
        join_set.spawn_on(get_object(meta, opts, cache), dfs.get_runtime().handle());
    }

    let mut objects = vec![];
    while let Some(res) = join_set.join_next().await {
        let res = box_try_join!(res)?;
        objects.push(res);
    }
    Ok(objects)
}
