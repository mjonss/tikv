// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

mod profile;

use std::{
    borrow::Cow,
    convert::TryInto,
    env::args,
    error::Error as StdError,
    net::SocketAddr,
    str::{self, FromStr},
    sync::Arc,
    time::Duration,
};

use api_version::{ApiV2, api_v2::TXN_KEY_PREFIX};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use collections::{HashMap, HashMapExt, HashSet};
use concurrency_manager::ConcurrencyManager;
use flate2::{Compression, write::GzEncoder};
use futures::{compat::Compat01As03, future::ok, prelude::*};
use hyper::{
    self, Body, Method, Request, Response, Server, StatusCode, header,
    header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_TYPE, HeaderValue},
    server::{
        Builder as HyperBuilder,
        accept::Accept,
        conn::{AddrIncoming, AddrStream},
    },
    service::{make_service_fn, service_fn},
};
use hyper_rustls::acceptor::TlsStream;
use keys::next_key;
use kvengine::{
    ENCRYPTION_KEY, GLOBAL_SHARD_END_KEY, IdVer, Shard, ShardStats, ShardTag,
    dfs::FileType,
    table::{BoundedDataSet, InnerKey, SnapVersion},
};
use kvenginepb::ChangeSet;
use kvproto::{
    coprocessor::DelegateResponse,
    errorpb, metapb,
    metapb::BucketStats,
    pdpb,
    pdpb::{Peers, SyncRegionResponse},
    raft_serverpb::{PeerState, StoreIdent},
};
use online_config::OnlineConfig;
use pd_client::PdClient;
use profile::*;
use prometheus::TEXT_FORMAT;
use protobuf::Message;
use raftstore::{RegionInfo, RegionInfoAccessor, coprocessor::RegionInfoProvider};
use rfengine::{
    Error, RAFT_TRUNCATED_STATE_KEY, RfEngine, WriteBatch, load_store_ident, raft_state_key,
};
use rfstore::{
    RaftRouter, RaftStoreRouter,
    store::{
        Callback, CasualMessage, RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM, RegionSnapshot, StoreMsg,
        TERM_KEY,
        peer_storage::{collect_prefix_regions, load_raft_engine_meta, load_region_state},
        state::RaftState,
        write_engine_meta_bytes, write_peer_state,
    },
};
use security::{self, SecurityConfig};
use serde_json::Value;
use tidb_query_datatype::codec::table::encode_row_key_prefix;
use tikv::{
    config::{ConfigController, LogLevel},
    server::status_server::profile::start_one_cpu_profile,
    storage::CloudStore,
};
use tikv_util::{
    Either,
    codec::bytes::{decode_bytes, encode_bytes},
    config::{AbsoluteOrPercentSize, ReadableSize},
    future::paired_future_callback,
    http::{CONTENT_TYPE_PROTOBUF, HeaderExt},
    init_task_local_sync,
    logger::set_log_level,
    metrics::{dump, dump_to},
    store::find_peer,
    sys::thread::ThreadBuildWrapper,
    time::{Instant, UnixSecs},
    timer::GLOBAL_TIMER_HANDLE,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    runtime::{Builder, Runtime},
    sync::{
        Semaphore,
        oneshot::{self, Receiver, Sender},
    },
    task::JoinSet,
};
use txn_types::TsSet;

use crate::{
    server::Result, status_server::metrics::STATUS_REQ_HISTOGRAM_STATIC,
    tikv_util::codec::number::NumberEncoder,
};

mod metrics;

static TIMER_CANCELED: &str = "tokio timer canceled";

#[cfg(feature = "failpoints")]
static MISSING_NAME: &[u8] = b"Missing param name";
#[cfg(feature = "failpoints")]
static MISSING_ACTIONS: &[u8] = b"Missing param actions";
#[cfg(feature = "failpoints")]
static FAIL_POINTS_REQUEST_PATH: &str = "/fail";

const SERVER_READ_TIMEOUT: Duration = Duration::from_secs(600);
const SERVER_TCP_KEEPALIVE: Duration = Duration::from_secs(120);

const BACKUP_TS_WAIT_RETRY_INTERVAL: Duration = Duration::from_secs(1);

const STORES_INFO_CACHE_TTL: Duration = Duration::from_secs(5);
// Maximum TTL when fail to get stores info from PD.
const STORES_INFO_CACHE_MAX_TTL: Duration = Duration::from_secs(60);

// When doing unsafe recover, add this delta to term and last_index.
const UNSAFE_RECOVER_DELTA: u64 = 64;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct LogLevelRequest {
    pub log_level: LogLevel,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct SyncRegionRequest {
    pub start: String,
    pub end: String,
    pub limit: usize,
    pub reverse: bool,
    pub debug: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct SyncRegionByIdRequest {
    pub region_id: u64,
    #[serde(default)]
    pub debug: bool,
}

pub struct StatusServer {
    thread_pool: Arc<Runtime>,
    close_tx: Sender<()>,
    close_rx: Option<Receiver<()>>,
    close_handle: Option<std::thread::JoinHandle<()>>,
    addr: Option<SocketAddr>,
    cfg_controller: ConfigController,
    router: RaftRouter,
    security_config: Arc<SecurityConfig>,
    kvengine: kvengine::Engine,
    rfengine: rfengine::RfEngine,
    concurrency_manager: ConcurrencyManager,
    pd_client: Arc<dyn PdClient>,
    region_info_accessor: RegionInfoAccessor,
}

impl StatusServer {
    pub fn new(
        status_thread_pool_size: usize,
        cfg_controller: ConfigController,
        security_config: Arc<SecurityConfig>,
        router: RaftRouter,
        kvengine: kvengine::Engine,
        rfengine: rfengine::RfEngine,
        concurrency_manager: ConcurrencyManager,
        pd_client: Arc<dyn PdClient>,
        region_info_accessor: RegionInfoAccessor,
    ) -> Result<Self> {
        let thread_pool = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(status_thread_pool_size)
            .thread_name("status-server")
            .after_start_wrapper(|| debug!("Status server started"))
            .before_stop_wrapper(|| debug!("stopping status server"))
            .build()?;

        let (close_tx, close_rx) = oneshot::channel::<()>();
        Ok(StatusServer {
            thread_pool: Arc::new(thread_pool),
            close_tx,
            close_rx: Some(close_rx),
            close_handle: None,
            addr: None,
            cfg_controller,
            router,
            security_config,
            kvengine,
            rfengine,
            concurrency_manager,
            pd_client,
            region_info_accessor,
        })
    }

    fn load_raft_state(rf: &RfEngine, peer_id: u64, ver: u64) -> Option<RaftState> {
        let key = raft_state_key(ver);
        let raft_state_val = match rf.get_state(peer_id, key.chunk()) {
            Some(val) => val,
            None => {
                error!("failed to load raft state");
                return None;
            }
        };
        let mut raft_state = RaftState::default();
        raft_state.unmarshal(&raft_state_val);
        Some(raft_state)
    }

    fn write_empty_engine_meta(
        rf: &RfEngine,
        wb: &mut WriteBatch,
        peer_id: u64,
        region_id: u64,
        ver: u64,
        keyspace_id: u32,
        index: u64,
        term: u64,
        inner_key_off: u32,
        new_cs: Option<ChangeSet>,
        prop_encryption_val: Option<Bytes>,
    ) -> Result<kvenginepb::ChangeSet> {
        let region_local_state = match load_region_state(rf, peer_id, ver) {
            Some(val) => val,
            None => {
                return Err(crate::server::Error::Other(box_err!(
                    "region state not found"
                )));
            }
        };
        write_peer_state(
            wb,
            peer_id,
            region_local_state.get_region(),
            PeerState::Normal,
            None,
        );
        let mut enc_start_key = region_local_state.get_region().get_start_key();
        let mut enc_end_key = region_local_state.get_region().get_end_key();
        let start_key = decode_bytes(&mut enc_start_key, false).unwrap();
        let end_key = decode_bytes(&mut enc_end_key, false).unwrap();

        let mut cs = new_cs.unwrap_or_default();
        cs.set_shard_id(region_id);
        cs.set_shard_ver(ver);
        cs.set_sequence(index);
        let snap = cs.mut_snapshot();
        snap.set_outer_start(start_key.to_vec());
        snap.set_outer_end(end_key.to_vec());
        snap.set_inner_key_off(inner_key_off);
        snap.set_data_sequence(index);
        let props = snap.mut_properties();
        props.set_shard_id(region_id);
        props.mut_keys().push(TERM_KEY.to_string());
        props.mut_values().push(term.to_le_bytes().to_vec());
        if let Some(prop_encryption_val) = prop_encryption_val {
            props.mut_keys().push(ENCRYPTION_KEY.to_string());
            props.mut_values().push(prop_encryption_val.to_vec());
        }

        let cs_val = cs.write_to_bytes().unwrap();
        write_engine_meta_bytes(wb, peer_id, region_id, keyspace_id, &cs_val);
        Ok(cs)
    }

    fn write_truncated_state(
        wb: &mut WriteBatch,
        peer_id: u64,
        region_id: u64,
        keyspace_id: u32,
        index: u64,
        term: u64,
    ) {
        let mut ts_val = BytesMut::with_capacity(16);
        ts_val.put_u64_le(term);
        ts_val.put_u64_le(index);
        let ts = ts_val.freeze();
        wb.set_state(
            peer_id,
            region_id,
            keyspace_id,
            RAFT_TRUNCATED_STATE_KEY,
            ts.chunk(),
        );
    }

    fn write_raft_state(
        wb: &mut WriteBatch,
        peer_id: u64,
        region_id: u64,
        ver: u64,
        keyspace_id: u32,
        index: u64,
        term: u64,
    ) -> RaftState {
        let mut rs_val = BytesMut::with_capacity(40);
        rs_val.put_u64_le(term);
        rs_val.put_u64_le(0);
        rs_val.put_u64_le(index);
        rs_val.put_u64_le(index);
        rs_val.put_u64_le(index);
        let rs = rs_val.freeze();
        let key = raft_state_key(ver);
        wb.set_state(peer_id, region_id, keyspace_id, key.chunk(), rs.chunk());
        let mut raft_state = RaftState::default();
        raft_state.unmarshal(rs.chunk());
        raft_state
    }

    pub fn dump_heap_prof_to_resp(req: Request<Body>) -> hyper::Result<Response<Body>> {
        let query = req.uri().query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();

        let use_jeprof = query_pairs.get("jeprof").map(|x| x.as_ref()) == Some("true");
        if use_jeprof {
            return Ok(make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "jeprof=true is not supported on CSE",
            ));
        }

        let result = {
            let file = match dump_one_heap_profile() {
                Ok(file) => file,
                Err(e) => return Ok(make_response(StatusCode::INTERNAL_SERVER_ERROR, e)),
            };
            let path = file.path();
            read_file(path.to_string_lossy().as_ref())
            // Note: file, which is a NamedTempFile, is supposed to be unlinked
            // when it is dropped here.
        };

        match result {
            Ok(body) => {
                info!("dump or get heap profile successfully");
                let mut response = Response::builder()
                    .header("X-Content-Type-Options", "nosniff")
                    .header("Content-Disposition", "attachment; filename=\"profile\"")
                    .header("Content-Length", body.len());
                response = if use_jeprof {
                    response.header("Content-Type", mime::IMAGE_SVG.to_string())
                } else {
                    response.header("Content-Type", mime::APPLICATION_OCTET_STREAM.to_string())
                };
                Ok(response.body(body.into()).unwrap())
            }
            Err(e) => {
                info!("dump or get heap profile fail: {}", e);
                Ok(make_response(StatusCode::INTERNAL_SERVER_ERROR, e))
            }
        }
    }

    async fn get_config(
        req: Request<Body>,
        cfg_controller: &ConfigController,
    ) -> hyper::Result<Response<Body>> {
        let mut full = false;
        if let Some(query) = req.uri().query() {
            let query_pairs: HashMap<_, _> =
                url::form_urlencoded::parse(query.as_bytes()).collect();
            full = match query_pairs.get("full") {
                Some(val) => match val.parse() {
                    Ok(val) => val,
                    Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err.to_string())),
                },
                None => false,
            };
        }
        let encode_res = if full {
            // Get all config
            serde_json::to_string(&cfg_controller.get_current())
        } else {
            // Filter hidden config
            serde_json::to_string(&cfg_controller.get_current().get_encoder())
        };
        Ok(match encode_res {
            Ok(json) => Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json))
                .unwrap(),
            Err(_) => make_response(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error"),
        })
    }

    pub fn get_cmdline(_req: Request<Body>) -> hyper::Result<Response<Body>> {
        let args = args().fold(String::new(), |mut a, b| {
            a.push_str(&b);
            a.push('\x00');
            a
        });
        let response = Response::builder()
            .header("Content-Type", mime::TEXT_PLAIN.to_string())
            .header("X-Content-Type-Options", "nosniff")
            .body(args.into())
            .unwrap();
        Ok(response)
    }

    pub fn get_symbol_count(req: Request<Body>) -> hyper::Result<Response<Body>> {
        assert_eq!(req.method(), Method::GET);
        // We don't know how many symbols we have, but we
        // do have symbol information. pprof only cares whether
        // this number is 0 (no symbols available) or > 0.
        let text = "num_symbols: 1\n";
        let response = Response::builder()
            .header("Content-Type", mime::TEXT_PLAIN.to_string())
            .header("X-Content-Type-Options", "nosniff")
            .header("Content-Length", text.len())
            .body(text.into())
            .unwrap();
        Ok(response)
    }

    // The request and response format follows pprof remote server
    // https://gperftools.github.io/gperftools/pprof_remote_servers.html
    // Here is the go pprof implementation:
    // https://github.com/golang/go/blob/3857a89e7eb872fa22d569e70b7e076bec74ebbb/src/net/http/pprof/pprof.go#L191
    pub async fn get_symbol(req: Request<Body>) -> hyper::Result<Response<Body>> {
        assert_eq!(req.method(), Method::POST);
        let mut text = String::new();
        let body_bytes = hyper::body::to_bytes(req.into_body()).await?;
        let body = String::from_utf8(body_bytes.to_vec());
        if body.is_err() {
            return Ok(make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Body is not a valid UTF8",
            ));
        }
        let body = body.unwrap();

        // The request body is a list of addr to be resolved joined by '+'.
        // Resolve addrs with addr2line and write the symbols each per line in
        // response.
        for pc in body.split('+') {
            let addr = usize::from_str_radix(pc.trim_start_matches("0x"), 16).unwrap_or(0);
            if addr == 0 {
                info!("invalid addr: {}", addr);
                continue;
            }

            // Would be multiple symbols if inlined.
            let mut syms = vec![];
            backtrace::resolve(addr as *mut std::ffi::c_void, |sym| {
                let name = sym
                    .name()
                    .unwrap_or_else(|| backtrace::SymbolName::new(b"<unknown>"));
                syms.push(name.to_string());
            });

            if !syms.is_empty() {
                // join inline functions with '--'
                let f = syms.join("--");
                // should be <hex address> <function name>
                text.push_str(format!("{:#x} {}\n", addr, f).as_str());
            } else {
                info!("can't resolve mapped addr: {:#x}", addr);
                text.push_str(format!("{:#x} ??\n", addr).as_str());
            }
        }
        let response = Response::builder()
            .header("Content-Type", mime::TEXT_PLAIN.to_string())
            .header("X-Content-Type-Options", "nosniff")
            .header("Content-Length", text.len())
            .body(text.into())
            .unwrap();
        Ok(response)
    }

    async fn update_config(
        cfg_controller: ConfigController,
        req: Request<Body>,
    ) -> hyper::Result<Response<Body>> {
        let mut body = Vec::new();
        req.into_body()
            .try_for_each(|bytes| {
                body.extend(bytes);
                ok(())
            })
            .await?;
        Ok(match decode_json(&body) {
            Ok(change) => match cfg_controller.update(change) {
                Err(e) => make_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to update, error: {:?}", e),
                ),
                Ok(_) => {
                    let mut resp = Response::default();
                    *resp.status_mut() = StatusCode::OK;
                    resp
                }
            },
            Err(e) => make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to decode, error: {:?}", e),
            ),
        })
    }

    pub async fn dump_cpu_prof_to_resp(req: Request<Body>) -> hyper::Result<Response<Body>> {
        let query = req.uri().query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();

        let seconds: u64 = match query_pairs.get("seconds") {
            Some(val) => match val.parse() {
                Ok(val) => val,
                Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err.to_string())),
            },
            None => 10,
        };

        let frequency: i32 = match query_pairs.get("frequency") {
            Some(val) => match val.parse() {
                Ok(val) => val,
                Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err.to_string())),
            },
            None => 99, /* Default frequency of sampling. 99Hz to avoid coincide with special
                         * periods */
        };

        let prototype_content_type: hyper::http::HeaderValue =
            hyper::http::HeaderValue::from_str("application/protobuf").unwrap();
        let output_protobuf = req.headers().get("Content-Type") == Some(&prototype_content_type);

        let timer =
            GLOBAL_TIMER_HANDLE.delay(std::time::Instant::now() + Duration::from_secs(seconds));
        let end = async move {
            Compat01As03::new(timer)
                .await
                .map_err(|_| TIMER_CANCELED.to_owned())
        };
        match start_one_cpu_profile(end, frequency, output_protobuf).await {
            Ok(body) => {
                info!("dump cpu profile successfully");
                Ok(make_response(StatusCode::OK, body))
            }
            Err(e) => {
                info!("dump cpu profile fail: {}", e);
                Ok(make_response(StatusCode::INTERNAL_SERVER_ERROR, e))
            }
        }
    }

    async fn change_log_level(req: Request<Body>) -> hyper::Result<Response<Body>> {
        let mut body = Vec::new();
        req.into_body()
            .try_for_each(|bytes| {
                body.extend(bytes);
                ok(())
            })
            .await?;

        let log_level_request: std::result::Result<LogLevelRequest, serde_json::error::Error> =
            serde_json::from_slice(&body);

        match log_level_request {
            Ok(req) => {
                set_log_level(req.log_level.into());
                Ok(Response::new(Body::empty()))
            }
            Err(err) => Ok(make_response(StatusCode::BAD_REQUEST, err.to_string())),
        }
    }

    // URI: /kvengine/columnar_status?keyspace_id=xxx&table_id=xxx[&index_id=xxx]
    // Collect the columnar replica status of the table
    fn collect_columnar_status(
        req: Request<Body>,
        ctx: &StatusContext,
    ) -> hyper::Result<Response<Body>> {
        let query = req.uri().query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();

        if !query_pairs.contains_key("keyspace_id") || !query_pairs.contains_key("table_id") {
            return Ok(make_response(
                StatusCode::BAD_REQUEST,
                "keyspace_id and table_id are required".to_string(),
            ));
        }
        let keyspace_id = match u32::from_str(query_pairs.get("keyspace_id").unwrap()) {
            Ok(id) => id,
            Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err.to_string())),
        };
        let table_id = match i64::from_str(query_pairs.get("table_id").unwrap()) {
            Ok(id) => id,
            Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err.to_string())),
        };
        let index_id = if query_pairs.contains_key("index_id") {
            match i64::from_str(query_pairs.get("index_id").unwrap()) {
                Ok(id) => Some(id),
                Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err.to_string())),
            }
        } else {
            None
        };
        let mut table_prefix_key = api_version::ApiV2::get_txn_keyspace_prefix(keyspace_id);
        table_prefix_key.extend_from_slice(&encode_row_key_prefix(table_id));
        let encoded_table_prefix_key = encode_bytes(&table_prefix_key);
        let encoded_table_prefix_end_key = next_key(&encoded_table_prefix_key);
        let ids = ctx
            .region_info_accessor
            .get_region_ids_in_range(encoded_table_prefix_key, encoded_table_prefix_end_key);
        let resp = ctx
            .kvengine
            .collect_columnar_status(keyspace_id, table_id, index_id, ids);
        let resp_json = serde_json::to_string_pretty(&resp).unwrap();
        Ok(Response::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(resp_json))
            .unwrap())
    }

    /// URI: /kvengine/columnar_index_stats?keyspace_id=xxx
    /// Returns the columnar index coverage statistics for all tables and
    /// indexes in the keyspace
    async fn collect_columnar_index_stats(
        req: Request<Body>,
        ctx: &StatusContext,
    ) -> hyper::Result<Response<Body>> {
        let query = req.uri().query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        if !query_pairs.contains_key("keyspace_id") {
            return Ok(make_response(
                StatusCode::BAD_REQUEST,
                "keyspace_id is required".to_string(),
            ));
        }
        let keyspace_id = match u32::from_str(query_pairs.get("keyspace_id").unwrap()) {
            Ok(id) => id,
            Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err.to_string())),
        };
        // collect_columnar_index_stats may have blocking IO, so run it in a blocking
        // thread pool
        let engine = ctx.kvengine.clone();
        let join_handle = ctx.thread_pool.spawn_blocking(move || {
            init_task_local_sync(|| {
                engine
                    .collect_columnar_index_stats(keyspace_id)
                    .map(|stats| serde_json::to_string_pretty(&stats).unwrap())
            })
        });
        let stats_json = match join_handle.await {
            Ok(Ok(json)) => json,
            Ok(Err(err)) => {
                return Ok(make_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to collect columnar index stats: {}", err),
                ));
            }
            Err(err) => {
                return Ok(make_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to collect columnar index stats: {}", err),
                ));
            }
        };
        Ok(Response::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(stats_json))
            .unwrap())
    }

    // URI: /kvengine/snapshot/<shard_id>?start_ts=xxx&shard_ver=xxx[&start_key=xxx&
    // end_key=xxx][&meta_seq=xxx&write_seq=xxx]
    //
    // dump kvengine shard snapshot with start_ts
    // `start_key` and `end_key` are in the upper hex string format, e.g.
    // `7800000174800000000000007F5F`
    // `meta_seq` and `write_seq` are used for cache validation - if they match
    // the server's current values, a cache hit response is returned instead of
    // snapshot data
    async fn dump_kvengine_snapshot(
        req: Request<Body>,
        engine: &kvengine::Engine,
        router: &RaftRouter,
    ) -> hyper::Result<Response<Body>> {
        let path = req.uri().path();
        let last = get_last_path_segment(path);
        let query = req.uri().query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        if !query_pairs.contains_key("start_ts") {
            return Ok(make_response(
                StatusCode::BAD_REQUEST,
                "start_ts is required".to_string(),
            ));
        }
        if !query_pairs.contains_key("shard_ver") {
            return Ok(make_response(
                StatusCode::BAD_REQUEST,
                "shard_ver is required".to_string(),
            ));
        }
        let shard_ver = match u64::from_str(query_pairs.get("shard_ver").unwrap()) {
            Ok(ver) => ver,
            Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err.to_string())),
        };
        let shard_id = match u64::from_str(last) {
            Ok(id) => id,
            Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err.to_string())),
        };

        // Parse optional meta_seq and write_seq parameters for cache validation
        let client_meta_seq = query_pairs
            .get("meta_seq")
            .and_then(|s| u64::from_str(s).ok());
        let client_write_seq = query_pairs
            .get("write_seq")
            .and_then(|s| u64::from_str(s).ok());

        let (cb, fut) = paired_future_callback();
        let callback = Callback::Read(Box::new(move |res| {
            cb(res);
        }));
        router.send_casual_msg(
            shard_id,
            CasualMessage::CheckLeader {
                shard_ver,
                callback,
            },
        );
        let mut res = match fut.await {
            Ok(res) => res,
            Err(e) => {
                let err_msg = format!(
                    "{}:{}:{} check leader channel error: {:?}",
                    engine.get_engine_id(),
                    shard_id,
                    shard_ver,
                    e
                );
                error!("{}", err_msg);
                return Ok(make_response(StatusCode::INTERNAL_SERVER_ERROR, err_msg));
            }
        };
        let make_ok_response = |body: Vec<u8>| -> Response<Body> {
            Response::builder().body(Body::from(body)).unwrap()
        };
        let mut delegate_resp = DelegateResponse::default();
        let mut err = res.response.take_header().take_error();
        if err.has_not_leader() {
            delegate_resp
                .mut_region_error()
                .set_not_leader(err.take_not_leader());
            let body = delegate_resp.write_to_bytes().unwrap();
            return Ok(make_ok_response(body));
        } else if err.has_epoch_not_match() {
            delegate_resp
                .mut_region_error()
                .set_epoch_not_match(err.take_epoch_not_match());
            let body = delegate_resp.write_to_bytes().unwrap();
            return Ok(make_ok_response(body));
        }
        Ok(match engine.get_shard_with_ver(shard_id, shard_ver) {
            Ok(shard) => {
                // Check cache validity if both meta_seq and write_seq are provided
                if let (Some(client_meta_seq), Some(client_write_seq)) =
                    (client_meta_seq, client_write_seq)
                {
                    let server_meta_seq = shard.get_meta_sequence();
                    let server_write_seq = shard.get_write_sequence();

                    if client_meta_seq == server_meta_seq && client_write_seq == server_write_seq {
                        // Cache is valid, return empty response to indicate cache hit
                        debug!(
                            "cache hit for shard {} with meta_seq={}, write_seq={}",
                            shard_id, server_meta_seq, server_write_seq
                        );
                        return Ok(Response::builder()
                            .status(StatusCode::NOT_MODIFIED)
                            .body(Body::empty())
                            .unwrap());
                    }
                }

                let start = Instant::now_coarse();
                let snap_access = shard.new_snap_access();
                let outer_start = if query_pairs.contains_key("start_key") {
                    match hex::decode(query_pairs.get("start_key").unwrap().to_string()) {
                        Ok(v) => std::cmp::max(v.into(), shard.outer_start.clone()),
                        Err(_) => {
                            return Ok(make_response(
                                StatusCode::BAD_REQUEST,
                                "start_key is not a valid hex string",
                            ));
                        }
                    }
                } else {
                    shard.outer_start.clone()
                };
                let outer_end = if query_pairs.contains_key("end_key") {
                    match hex::decode(query_pairs.get("end_key").unwrap().to_string()) {
                        Ok(v) => std::cmp::min(v.into(), shard.outer_end.clone()),
                        Err(_) => {
                            return Ok(make_response(
                                StatusCode::BAD_REQUEST,
                                "end_key is not a valid hex string",
                            ));
                        }
                    }
                } else {
                    shard.outer_end.clone()
                };
                let start_ts = u64::from_str(query_pairs.get("start_ts").unwrap()).unwrap();
                let region_snapshot =
                    RegionSnapshot::from_snapshot(snap_access.clone(), engine.get_value_cache());
                let cloud_store =
                    CloudStore::new(region_snapshot, start_ts, TsSet::default(), true);
                // Check if the shard contains locks belongs to the ranges. If there is any
                // lock, return the first key as LockInfo. The client should retry in this case.
                if let Err(tikv::coprocessor::Error::Locked(lock_info)) = cloud_store
                    .check_locks_in_range(&outer_start, &outer_end)
                    .map_err(tikv::coprocessor::Error::from)
                {
                    delegate_resp.set_locked(lock_info);
                    let body = delegate_resp.write_to_bytes().unwrap();
                    return Ok(make_ok_response(body));
                }
                let outer_ranges = vec![(outer_start, outer_end)];
                let mem_data = snap_access.build_mem_data(&outer_ranges, start_ts);
                let (_, snap_data) = snap_access.marshal(&outer_ranges, false, false);
                let mem_data_len = mem_data.len();
                let snap_data_len = snap_data.len();
                delegate_resp.set_mem_table_data(mem_data);
                delegate_resp.set_snapshot(snap_data);
                delegate_resp.set_mem_table_sequence(snap_access.get_write_sequence());
                let body = delegate_resp.write_to_bytes().unwrap();
                debug!(
                    "dump_kvengine_snapshot time: {:?}, mem_data_size: {:?}, snap_data_size: {:?}, total_size: {:?}",
                    start.saturating_elapsed(),
                    mem_data_len,
                    snap_data_len,
                    body.len()
                );
                make_ok_response(body)
            }
            Err(e) => make_response(StatusCode::NOT_FOUND, e.to_string()),
        })
    }

    async fn dump_kvengine_meta(
        req: Request<Body>,
        engine: &kvengine::Engine,
    ) -> hyper::Result<Response<Body>> {
        let path = req.uri().path();
        let last = get_last_path_segment(path);
        let meta_bin = u64::from_str(last)
            .map(|id| {
                engine
                    .get_shard(id)
                    .map(|shard| {
                        let (_, meta_bin) = shard.new_snap_access().marshal(
                            &[(shard.outer_start.clone(), shard.outer_end.clone())],
                            false,
                            false,
                        );
                        meta_bin
                    })
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        Ok(Response::builder().body(Body::from(meta_bin)).unwrap())
    }

    async fn dump_kvengine_stats(
        req: Request<Body>,
        engine: &kvengine::Engine,
    ) -> hyper::Result<Response<Body>> {
        let path = req.uri().path();
        let last = get_last_path_segment(path);
        let res;
        if let Ok(id) = u64::from_str(last) {
            // get all shard state in given keyspace id.
            if path.starts_with("/kvengine/keyspace") {
                let range = ApiV2::get_txn_keyspace_range(id as u32);
                match Self::get_covered_shards_by_range(Some(range), engine) {
                    Ok(shards) => {
                        let states: Vec<ShardStats> =
                            shards.iter().map(|s| s.get_stats()).collect();
                        res = serde_json::to_string_pretty(&states);
                    }
                    Err(e) => {
                        return Ok(make_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            e.to_string(),
                        ));
                    }
                }
            } else {
                let shard_stats = engine.get_shard_stat(id);
                res = serde_json::to_string_pretty(&shard_stats);
            }
        } else if path.starts_with("/kvengine/all") {
            let all_shard_stats = engine.get_all_shard_stats();
            res = serde_json::to_string_pretty(&all_shard_stats);
        } else if path.starts_with("/kvengine/active_lite") {
            // get all active shard stats lite.
            let all_active_lite = engine.get_all_active_shard_stats_lite();
            res = serde_json::to_string(&all_active_lite);
        } else if path.starts_with("/kvengine/files_with_type") {
            let files_with_type = Self::dump_kvengine_files_with_type(req, engine);
            res = serde_json::to_string(&files_with_type);
        } else if path.starts_with("/kvengine/files") {
            // NOTE: Must be the last for /kvengine/files... paths.
            let mut all_shard_files: Vec<(u64, Vec<u64>)> = engine
                .get_all_shard_id_vers()
                .iter()
                .filter_map(|id_ver| {
                    engine
                        .get_shard(id_ver.id)
                        .map(|shard| (shard.id, shard.get_all_files()))
                })
                .collect();
            // add blacklist files to the collection of files in use,
            // for simplicity, zero shard id represents the blacklist files
            let blacklist_file_ids = engine
                .get_files_in_blacklist()
                .iter()
                .copied()
                .collect::<Vec<_>>();
            all_shard_files.push((0, blacklist_file_ids));
            res = serde_json::to_string(&all_shard_files);
        } else if path.starts_with("/kvengine/compactor") {
            let remote_urls = engine.comp_client.get_remote_compactors();
            res = serde_json::to_string_pretty(&remote_urls);
        } else {
            let all_shard_stats = engine.get_all_shard_stats();
            let engine_stats = engine.get_engine_stats(all_shard_stats);
            res = serde_json::to_string_pretty(&engine_stats);
        }
        Ok(match res {
            Ok(json) => Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json))
                .unwrap(),
            Err(_) => make_response(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error"),
        })
    }

    /// Request: /kvengine/files_with_type?filter=sst,sst_ia,txn,col
    /// Response: { "sst": [file_id1, file_id2], "blob": [file_id3], ... }
    /// Note: currently support "sst" only.
    fn dump_kvengine_files_with_type(
        req: Request<Body>,
        engine: &kvengine::Engine,
    ) -> HashMap<String /* file_suffix */, HashSet<u64 /* file_id */>> {
        let query = req.uri().query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        let Some(filter_str) = query_pairs.get("filter").map(|s| s.as_ref()) else {
            return HashMap::default();
        };
        let filter_types: HashSet<&str> = filter_str.split(',').collect();
        let mut files_with_type: HashMap<String, HashSet<u64>> =
            HashMap::with_capacity(filter_types.len());
        engine.get_all_shard_id_vers().iter().for_each(|id_ver| {
            let Some(shard) = engine.get_shard(id_ver.id) else {
                return;
            };
            if filter_types.contains("sst") {
                let (local_files, _) = shard.get_local_sst_files();
                files_with_type
                    .entry("sst".to_string())
                    .or_default()
                    .extend(local_files);
            }
        });
        files_with_type
    }

    async fn add_remote_compactor(
        req: Request<Body>,
        comp_client: &kvengine::CompactionClient,
    ) -> hyper::Result<Response<Body>> {
        let mut body = Vec::new();
        req.into_body()
            .try_for_each(|bytes| {
                body.extend(bytes);
                ok(())
            })
            .await?;
        Ok(match String::from_utf8(body) {
            Ok(remote_url) => {
                comp_client.add_remote_compactor(remote_url);
                let mut resp = Response::default();
                *resp.status_mut() = StatusCode::OK;
                resp
            }
            Err(e) => make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to decode, error: {:?}", e),
            ),
        })
    }

    async fn get_change_set_request(
        raw_req: Request<Body>,
    ) -> hyper::Result<kvenginepb::ChangeSet> {
        let mut body = Vec::new();
        raw_req
            .into_body()
            .try_for_each(|bytes| {
                body.extend(bytes);
                ok(())
            })
            .await?;
        let mut cs = kvenginepb::ChangeSet::default();
        cs.merge_from_bytes(&body).unwrap();
        Ok(cs)
    }

    async fn ingest_files(
        req: Request<Body>,
        ctx: &StatusContext,
    ) -> hyper::Result<Response<Body>> {
        let cs = Self::get_change_set_request(req).await?;
        let tag = tag_from_cs(&ctx.kvengine, &cs);
        info!("[{}] receive ingest_files request: {:?}", tag, cs);

        if let Err(errpb) = check_available_space(&ctx.kvengine) {
            warn!("[{}] reject ingest files, low space", tag);
            return Ok(make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                errpb.write_to_bytes().unwrap(),
            ));
        }
        if let Err(errpb) = check_follower_available_space(ctx, cs.shard_id, cs.shard_ver).await {
            warn!("[{}] reject ingest files, follower low space", tag; "err" => ?errpb);
            return Ok(make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                errpb.write_to_bytes().unwrap(),
            ));
        }

        let (cb, fut) = paired_future_callback();
        let callback = Callback::write(Box::new(move |res| {
            cb(res);
        }));
        ctx.router.send_casual_msg(
            cs.get_shard_id(),
            CasualMessage::IngestFiles { cs, callback },
        );

        let res = match fut.await {
            Ok(res) => res,
            Err(e) => {
                let err_msg = format!("{} ingest_files channel error: {:?}", tag, e);
                error!("{}", err_msg);
                // Return "not leader" to let callers retry.
                let mut errpb = kvproto::errorpb::Error::default();
                errpb.set_not_leader(Default::default());
                errpb.set_message(err_msg);
                return Ok(make_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    errpb.write_to_bytes().unwrap(),
                ));
            }
        };
        if res.response.get_header().has_error() {
            error!(
                "{} ingest_files error: {:?}",
                tag,
                res.response.get_header().get_error()
            );
            let err_data = res
                .response
                .get_header()
                .get_error()
                .write_to_bytes()
                .unwrap();
            Ok(make_response(StatusCode::INTERNAL_SERVER_ERROR, err_data))
        } else {
            Ok(make_response(StatusCode::OK, ""))
        }
    }

    async fn dump_rfengine_stats(
        req: Request<Body>,
        engine: &RfEngine,
    ) -> hyper::Result<Response<Body>> {
        let path = req.uri().path();
        let last = get_last_path_segment(path);
        let res = if let Ok(peer_id) = u64::from_str(last) {
            let peer_stats = engine.get_peer_stats(peer_id);
            serde_json::to_string_pretty(&peer_stats)
        } else {
            let engine_stats = engine.get_engine_stats();
            serde_json::to_string_pretty(&engine_stats)
        };
        Ok(match res {
            Ok(json) => Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json))
                .unwrap(),
            Err(_) => make_response(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error"),
        })
    }

    // URI: /rfengine/wal_chunk?epoch_id=xxx&start_off=xxx&end_off=xxx
    // If end_off is zero, it means to dump to the latest, then the response status
    // code maybe PARTIAL_CONTENT if the request epoch_id is not the latest.
    async fn rfengine_wal_chunk(
        req: Request<Body>,
        engine: &RfEngine,
    ) -> hyper::Result<Response<Body>> {
        let bad_request_resp = |msg: &str| make_response(StatusCode::BAD_REQUEST, msg.to_owned());
        if !engine.is_lightweight_backup_enabled() {
            return Ok(bad_request_resp("lightweight backup not enabled"));
        }

        let query = req.uri().query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        info!("/rfengine/wal_chunk {:?}", query_pairs);
        if !query_pairs.contains_key("epoch_id")
            || !query_pairs.contains_key("start_off")
            || !query_pairs.contains_key("end_off")
        {
            return Ok(bad_request_resp("query parameters invalid"));
        }
        let (callback, future) = paired_future_callback();
        let epoch_id = match u32::from_str(query_pairs.get("epoch_id").unwrap()) {
            Ok(epoch_id) => epoch_id,
            Err(err) => return Ok(bad_request_resp(err.to_string().as_str())),
        };
        let start_off = match u64::from_str(query_pairs.get("start_off").unwrap()) {
            Ok(start_off) => start_off,
            Err(err) => return Ok(bad_request_resp(err.to_string().as_str())),
        };
        let end_off = match u64::from_str(query_pairs.get("end_off").unwrap()) {
            Ok(end_off) => end_off,
            Err(err) => return Ok(bad_request_resp(err.to_string().as_str())),
        };
        engine.dump_wal_chunk(epoch_id, start_off, end_off, callback);

        Ok(match future.await {
            Ok(resp) => match resp {
                Ok((chunk, partial_content)) => {
                    let status = if partial_content {
                        StatusCode::PARTIAL_CONTENT
                    } else {
                        StatusCode::OK
                    };
                    Response::builder()
                        .status(status)
                        .body(Body::from(chunk))
                        .unwrap()
                }
                Err(Error::WalEpochOverwritten { epoch_id }) => make_response(
                    StatusCode::GONE,
                    format!("WAL epoch {epoch_id} is overwritten"),
                ),
                Err(err) => {
                    error!("rfengine_wal_chunk error: {}", err);
                    make_response(StatusCode::BAD_REQUEST, format!("bad request {}", err))
                }
            },
            Err(e) => make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal Server Error {}", e),
            ),
        })
    }

    // URI: /unsafe_recover/clear?cluster_id=xxx&[keyspace_id=xxx[&table_id=xxx]][&
    // region_id=xxx]
    // The request body can be empty or a DelegateResponse protobuf message.
    // If the request body is provided, it only support single region clear, and the
    // ChangeSet in the body will be used instead of empty ChangeSet.
    async fn unsafe_recover(
        req: Request<Body>,
        rf: &RfEngine,
        engine: &kvengine::Engine,
    ) -> hyper::Result<Response<Body>> {
        let bad_request_resp =
            |msg: &str| make_response(StatusCode::BAD_REQUEST, msg.to_owned() + "\n");
        let path = req.uri().path();
        if path != "/unsafe_recover/clear" {
            return Ok(bad_request_resp("bad request URI"));
        }
        let query = req.uri().query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        let cluster_id = query_pairs.get("cluster_id");
        if cluster_id.is_none() {
            return Ok(bad_request_resp("cluster_id not found"));
        }
        let cluster_id = match u64::from_str(cluster_id.unwrap()) {
            Ok(cluster_id) => cluster_id,
            Err(e) => return Ok(bad_request_resp(e.to_string().as_str())),
        };
        let store_ident = load_store_ident(rf).unwrap_or_default();
        if cluster_id != store_ident.cluster_id {
            return Ok(bad_request_resp("cluster_id not match"));
        }
        let region_id = query_pairs.get("region_id");
        let keyspace_id = query_pairs.get("keyspace_id");
        let table_id = query_pairs.get("table_id");

        let target_regions = if let Some(region_id) = region_id {
            let region_id = match u64::from_str(region_id) {
                Ok(region_id) => region_id,
                Err(err) => return Ok(bad_request_resp(err.to_string().as_str())),
            };
            match rf.get_region_peer_map().get(&region_id) {
                Some(&peer_id) => {
                    let cs = load_raft_engine_meta(rf, peer_id);
                    if cs.is_none() {
                        return Ok(bad_request_resp(
                            format!("region {} peer {} not exists", region_id, peer_id).as_str(),
                        ));
                    }
                    let cs = cs.unwrap();
                    let keyspace_id =
                        ApiV2::get_u32_keyspace_id_by_key(&cs.get_snapshot().outer_start)
                            .unwrap_or_default();
                    vec![(peer_id, region_id, cs.shard_ver, keyspace_id)]
                }
                None => {
                    return Ok(bad_request_resp(
                        format!("region {} not exists", region_id).as_str(),
                    ));
                }
            }
        } else if let Some(keyspace_id) = keyspace_id {
            let keyspace_id = match u32::from_str(keyspace_id) {
                Ok(keyspace_id) => keyspace_id,
                Err(e) => return Ok(bad_request_resp(e.to_string().as_str())),
            };
            let mut prefix = ApiV2::get_txn_keyspace_prefix(keyspace_id);
            if let Some(table_id) = table_id {
                let table_id = match u64::from_str(table_id) {
                    Ok(table_id) => table_id,
                    Err(e) => return Ok(bad_request_resp(e.to_string().as_str())),
                };
                prefix.put_u8(b't');
                prefix.encode_i64(table_id as i64).unwrap();
            }
            if let Some(regions) = collect_prefix_regions(rf, &prefix) {
                regions
            } else {
                return Ok(bad_request_resp("collect none region with prefix"));
            }
        } else {
            return Ok(bad_request_resp("query parameters invalid"));
        };

        let target_regions_len = target_regions.len();
        let mut wb = WriteBatch::new();
        let body = hyper::body::to_bytes(req.into_body()).await?;
        let cs_snap = if !body.is_empty() {
            if target_regions_len != 1 {
                return Ok(bad_request_resp(
                    "body not empty only support single region clear",
                ));
            }
            let (_, target_shard_id, target_shard_ver, _) = target_regions[0];
            let mut delegate_resp = DelegateResponse::default();
            delegate_resp.merge_from_bytes(body.chunk()).unwrap();
            let mut change_set = kvenginepb::ChangeSet::default();
            change_set
                .merge_from_bytes(delegate_resp.get_snapshot())
                .unwrap();
            if change_set.get_shard_id() != target_shard_id {
                return Ok(bad_request_resp(&format!(
                    "body shard_id {} not match target {}",
                    change_set.get_shard_id(),
                    target_shard_id
                )));
            }
            if change_set.get_shard_ver() != target_shard_ver {
                return Ok(bad_request_resp(&format!(
                    "body shard_ver {} not match target {}",
                    change_set.get_shard_ver(),
                    target_shard_ver
                )));
            }
            Some(change_set)
        } else {
            None
        };
        for (peer_id, region_id, region_version, keyspace_id) in target_regions {
            let shard_stats = engine.get_shard_stat(region_id);
            info!(
                "unsafe recover region {} peer {} shard_stats {:?}",
                region_id, peer_id, shard_stats
            );
            if shard_stats.id != 0 {
                return Ok(bad_request_resp(
                    format!("region {} not in blacklist", region_id).as_str(),
                ));
            }

            let old_raft_state = match Self::load_raft_state(rf, peer_id, region_version) {
                Some(state) => state,
                None => {
                    return Ok(bad_request_resp(
                        format!(
                            "region {} peer {} raft state not exists",
                            region_id, peer_id
                        )
                        .as_str(),
                    ));
                }
            };
            let raft_state_last_index = old_raft_state.get_last_index();
            let raft_log_last_index = rf.get_last_index(peer_id).unwrap_or(RAFT_INIT_LOG_INDEX);
            let origin_last_index = std::cmp::max(raft_state_last_index, raft_log_last_index);
            // Add a delta to last_index to replicate empty snapshot to followers.
            let last_index = origin_last_index + UNSAFE_RECOVER_DELTA;
            info!(
                "unsafe_recover region: {} peer: {} truncate raft log to {}",
                region_id, peer_id, last_index
            );
            wb.truncate_raft_log(peer_id, region_id, keyspace_id, last_index);

            let raft_log_term = rf
                .get_term(peer_id, origin_last_index)
                .unwrap_or(RAFT_INIT_LOG_TERM);
            let Some(old_cs) = load_raft_engine_meta(rf, peer_id) else {
                return Ok(bad_request_resp(
                    format!(
                        "region {} peer {} raft engine meta not exists",
                        region_id, peer_id
                    )
                    .as_str(),
                ));
            };
            let snap = old_cs.get_snapshot();
            let inner_key_off = snap.get_inner_key_off();
            let mut properties = kvengine::Properties::new();
            properties = properties.apply_pb(snap.get_properties());
            let meta_term = properties.get(TERM_KEY).unwrap().get_u64_le();
            let prop_encryption_val = properties.get(ENCRYPTION_KEY);

            // Add a delta to term make sure it's greater than term in pd cache.
            let term = std::cmp::max(raft_log_term, meta_term) + UNSAFE_RECOVER_DELTA;

            let _ = match Self::write_empty_engine_meta(
                rf,
                &mut wb,
                peer_id,
                region_id,
                region_version,
                keyspace_id,
                last_index,
                term,
                inner_key_off,
                cs_snap.clone(),
                prop_encryption_val,
            ) {
                Ok(cs) => {
                    info!(
                        "unsafe_recover region: {} peer: {} reset kv engine meta {:?} -> {:?}",
                        region_id, peer_id, old_cs, cs
                    );
                    cs
                }
                Err(e) => return Ok(bad_request_resp(e.to_string().as_str())),
            };

            Self::write_truncated_state(&mut wb, peer_id, region_id, keyspace_id, last_index, term);
            info!(
                "unsafe_recover region: {} peer: {} reset truncated state to truncated_index: {} truncated_term: {}",
                region_id, peer_id, last_index, term
            );

            let raft_state = Self::write_raft_state(
                &mut wb,
                peer_id,
                region_id,
                region_version,
                keyspace_id,
                last_index,
                term,
            );
            info!(
                "unsafe_recover region: {} peer: {} reset raft state {:?} -> {:?}",
                region_id, peer_id, old_raft_state, raft_state
            );
        }

        // Write batch to raft engine only when it's not empty. Or it will cause the wal
        // iteration interrupted.
        if !wb.is_empty() {
            rf.write(wb).unwrap();
        }

        Ok(make_response(
            StatusCode::OK,
            format!("{} region(s) clear success\n", target_regions_len),
        ))
    }

    // URI: /major_compact?major_compact=xxx[&keyspace_id=xxx[&table_id=xxx]][&
    // region_id=xxx][&columnar=true]
    // Note: return 404 when no match region is found.
    async fn major_compact(
        req: Request<Body>,
        rf: &RfEngine,
        router: &RaftRouter,
    ) -> hyper::Result<Response<Body>> {
        info!("major compact request: {:?}", req);
        let bad_request_resp = |msg: &str| make_response(StatusCode::BAD_REQUEST, msg.to_owned());
        let not_found_resp = |msg: &str| make_response(StatusCode::NOT_FOUND, msg.to_owned());

        let path = req.uri().path();
        if path != "/major-compact" {
            return Ok(bad_request_resp("bad request URI"));
        }
        let query = req.uri().query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        let major_compact = query_pairs.get("major_compact");
        if major_compact.is_none() {
            return Ok(bad_request_resp("major_compact not found"));
        }
        let major_compact = match bool::from_str(major_compact.unwrap()) {
            Ok(major_compact) => major_compact,
            Err(e) => return Ok(bad_request_resp(e.to_string().as_str())),
        };
        let region_id = query_pairs.get("region_id");
        let keyspace_id = query_pairs.get("keyspace_id");
        let table_id = query_pairs.get("table_id");
        let columnar = query_pairs
            .get("columnar")
            .is_some_and(|s| bool::from_str(s).unwrap_or(false));

        let target_regions = if let Some(region_id) = region_id {
            let region_id = match u64::from_str(region_id) {
                Ok(region_id) => region_id,
                Err(err) => return Ok(bad_request_resp(err.to_string().as_str())),
            };
            let region_to_peers = rf.get_region_peer_map();
            match region_to_peers.get(&region_id) {
                Some(&peer_id) => {
                    let cs = load_raft_engine_meta(rf, peer_id);
                    if cs.is_none() {
                        return Ok(not_found_resp(
                            format!("region {} peer {} not exists", region_id, peer_id).as_str(),
                        ));
                    }
                    vec![region_id]
                }
                None => {
                    return Ok(not_found_resp(
                        format!("region {} not exists", region_id).as_str(),
                    ));
                }
            }
        } else if let Some(keyspace_id) = keyspace_id {
            let keyspace_id = match u32::from_str(keyspace_id) {
                Ok(keyspace_id) => keyspace_id,
                Err(e) => return Ok(bad_request_resp(e.to_string().as_str())),
            };
            let mut prefix = ApiV2::get_txn_keyspace_prefix(keyspace_id);
            if let Some(table_id) = table_id {
                let table_id = match u64::from_str(table_id) {
                    Ok(table_id) => table_id,
                    Err(e) => return Ok(bad_request_resp(e.to_string().as_str())),
                };
                prefix.put_u8(b't');
                prefix.encode_i64(table_id as i64).unwrap();
            }
            if let Some(regions) = collect_prefix_regions(rf, &prefix) {
                regions
                    .into_iter()
                    .map(|(_, region_id, ..)| region_id)
                    .collect::<Vec<_>>()
            } else {
                return Ok(not_found_resp("collect none region with prefix"));
            }
        } else {
            return Ok(bad_request_resp("query parameters invalid"));
        };
        if target_regions.is_empty() {
            return Ok(not_found_resp("target regions is empty"));
        } else {
            info!("manual major compact regions {:?}", target_regions);
        }
        let mut region_futures = Vec::with_capacity(target_regions.len());
        for region_id in target_regions.iter() {
            let (cb, fu) = paired_future_callback();
            let callback = Callback::write(Box::new(move |_| {
                cb(());
            }));
            region_futures.push(fu);
            router.send_casual_msg(
                *region_id,
                CasualMessage::MajorCompact {
                    major_compact,
                    columnar,
                    callback,
                },
            );
        }
        let _ = futures::future::join_all(region_futures).await;
        Ok(make_response(
            StatusCode::OK,
            format!(
                "trigger manual major compaction on region(s) {:?} success",
                target_regions
            ),
        ))
    }

    // curl -X POST "http://127.0.0.1:20180/flush?keyspace_id=xxx&table_id=1,2,3"
    // Flush all regions of the specified table(s) for regions whose leader is the
    // current TiKV, and wait until flush finished. Maximum wait time is 60s.
    // table_id can be a single table ID or comma-separated list of table IDs.
    async fn flush(
        req: Request<Body>,
        router: &RaftRouter,
        kvengine: &kvengine::Engine,
    ) -> hyper::Result<Response<Body>> {
        let query = req.uri().query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        if !query_pairs.contains_key("keyspace_id") || !query_pairs.contains_key("table_id") {
            return Ok(make_response(
                StatusCode::BAD_REQUEST,
                "keyspace_id and table_id are required".to_string(),
            ));
        }
        let keyspace_id = match u32::from_str(query_pairs.get("keyspace_id").unwrap()) {
            Ok(keyspace_id) => keyspace_id,
            Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err.to_string())),
        };

        // Parse table_id parameter which can be a single ID or comma-separated list
        let table_id_str = query_pairs.get("table_id").unwrap();
        let mut table_ids = std::collections::HashSet::new();
        for id_str in table_id_str.split(',') {
            let id_str = id_str.trim();
            if id_str.is_empty() {
                continue;
            }
            match i64::from_str(id_str) {
                Ok(id) => {
                    table_ids.insert(id);
                }
                Err(err) => {
                    return Ok(make_response(
                        StatusCode::BAD_REQUEST,
                        format!("Invalid table_id format: {}", err),
                    ));
                }
            }
        }
        if table_ids.is_empty() {
            return Ok(make_response(
                StatusCode::BAD_REQUEST,
                "table_id cannot be empty".to_string(),
            ));
        }
        info!(
            "ManualFlush: Received flush request for keyspace_id={} table_ids={:?}",
            keyspace_id, table_ids
        );

        // Collect regions from keyspace id and table ids.
        // We will skip regions that are not leader on this TiKV, because this manual
        // flush is supposed to be run locally.
        let mut target_regions = Vec::new();
        let Some(ks_shards) = kvengine.get_keyspace_shards(keyspace_id) else {
            info!("ManualFlush: Keyspace {} not found", keyspace_id);
            return Ok(make_response(
                StatusCode::NOT_FOUND,
                format!("Keyspace {} not found", keyspace_id),
            ));
        };
        let mut table_key = Vec::new();
        for shard_id in ks_shards.iter().map(|r| *r) {
            let Some(shard) = kvengine.get_shard(shard_id) else {
                continue;
            };
            if !shard.is_active() {
                continue;
            }
            for table_id in table_ids.iter() {
                table_key.clear();
                table_key.put_u8(b't');
                table_key.encode_i64(*table_id).unwrap();
                if shard
                    .data_bound()
                    .overlap_key(InnerKey::from_inner_buf(&table_key))
                {
                    target_regions.push(shard_id);
                    break;
                }
            }
        }

        target_regions.sort();
        info!(
            "ManualFlush: will flush these local regions: {:?}",
            target_regions
        );
        if target_regions.is_empty() {
            info!("ManualFlush: skipped, no regions found");
            return Ok(make_response(
                StatusCode::OK,
                "Flush skipped, no regions found",
            ));
        }

        // For each shard, we need to wait for a version that signifies that the
        // memtable has been flushed.
        // Case 1. Writable memtable is not empty -- We don't know which version to wait
        // until we switched it. In this case, the memtable ref will be kept so that we
        // can know a version to wait after switch.
        // Case 2. Writable memtable is empty -- We can simply wait existing memtable
        // flush to complete. In this case we record which version to wait and do not
        // need to switch the memtable.

        // 1. Get the writable memtable for each shard, or fill the version to wait
        // if writable memtable is empty.
        let mut wait_versions_by_regions = HashMap::default();
        let mut writable_memtables = HashMap::default();
        target_regions.retain(|region_id| {
            let Some(shard) = kvengine.get_shard(*region_id) else {
                // Region changed
                return false;
            };
            let memtable = shard.get_writable_mem_table();
            if memtable.size() == 0 {
                let v = shard.get_mem_table_max_version();
                // Note, it is possible that v == 0, if there is only one writable memtable
                // and no sealed memtables (e.g. already flushed).
                info!(
                    "ManualFlush: region {} writable memtable is empty, will wait for version {}",
                    region_id, v
                );
                wait_versions_by_regions.insert(*region_id, v);
            } else {
                // which version to wait is only known after switch.
                // we keep a reference.
                info!(
                    "ManualFlush: region {} writable memtable is not empty, will switch",
                    region_id
                );
                writable_memtables.insert(*region_id, memtable);
            }
            true
        });

        if !writable_memtables.is_empty() {
            // 2. send FORCE_SWITCH_MEM_TABLE messages
            // Only shards with non-empty writable memtable need to switch.
            info!(
                "ManualFlush: calling FORCE_SWITCH_MEM_TABLE for regions: {:?}",
                writable_memtables.keys()
            );
            let mut region_futures = Vec::with_capacity(writable_memtables.len());
            for (&region_id, memtable) in writable_memtables.iter() {
                let (cb, fu) = paired_future_callback();
                let callback = Callback::write(Box::new(move |_| {
                    cb(());
                }));
                region_futures.push(fu);
                router.send_casual_msg(
                    region_id,
                    CasualMessage::ForceSwitchMemTable {
                        current_size: memtable.size(),
                        callback,
                    },
                );
            }
            let _ = futures::future::join_all(region_futures).await;

            // 3. At this moment, we should be able to know all versions to wait, including
            // those writable memtables (they have a version after switch).
            for (region_id, memtable) in writable_memtables {
                let v = memtable.get_snap_version();
                info!(
                    "ManualFlush: region {} writable memtable is switched, will wait for version {}",
                    region_id, v
                );
                wait_versions_by_regions.insert(region_id, v);
            }
        }

        let max_version_to_wait = wait_versions_by_regions
            .values()
            .max()
            .copied()
            .unwrap_or(SnapVersion::zero());
        if max_version_to_wait.is_zero() {
            info!(
                "ManualFlush: skipped, no memtable to wait on regions: {:?}",
                target_regions
            );
            return Ok(make_response(
                StatusCode::OK,
                format!(
                    "Flush skipped, no memtable to wait on regions: {:?}",
                    target_regions
                ),
            ));
        }

        // 4. Flush will be doing in background, after flush the changeset will be
        // applied, so here we wait for changeset to take effect.
        info!(
            "ManualFlush: waiting for flush to finish on regions: {:?}",
            wait_versions_by_regions.keys()
        );
        let now = Instant::now();
        loop {
            if now.saturating_elapsed_secs() > 60.0 {
                info!(
                    "ManualFlush: timed out waiting for flush to finish on regions: {:?}, will not wait any more",
                    wait_versions_by_regions.keys()
                );
                return Ok(make_response(
                    StatusCode::REQUEST_TIMEOUT,
                    format!(
                        "Timedout waiting flush to finish on regions: {:?}, all flush regions: {:?}",
                        wait_versions_by_regions.keys(),
                        target_regions
                    ),
                ));
            }
            let timer =
                GLOBAL_TIMER_HANDLE.delay(std::time::Instant::now() + Duration::from_secs(1));
            let end = async move {
                Compat01As03::new(timer)
                    .await
                    .map_err(|_| TIMER_CANCELED.to_owned())
            };
            let _ = end.await;
            wait_versions_by_regions.retain(|region_id, version| {
                let Some(shard) = kvengine.get_shard(*region_id) else {
                    return false;
                };
                if !shard.is_active() {
                    return false;
                }
                // Keep this region only if it's flushed version has not reached
                // what we want.
                shard.get_persisted_snap_version() < *version
            });
            if wait_versions_by_regions.is_empty() {
                break;
            }
        }

        info!("ManualFlush: all finished");
        Ok(make_response(
            StatusCode::OK,
            format!("Flush memtable succeeded on regions: {:?}", target_regions),
        ))
    }

    fn get_dfs_file_id(req: &Request<Body>) -> Option<u64> {
        let path = req.uri().path();
        let last = get_last_path_segment(path);
        u64::from_str(last).ok()
    }

    fn get_dfs_read_args(
        req: &Request<Body>,
    ) -> (
        FileType,
        u64,         // start_off
        Option<u64>, // end_off
        bool,        // check_exists_only
    ) {
        let mut file_type: Option<FileType> = None;
        let mut start_off: Option<u64> = None;
        let mut end_off: Option<u64> = None;
        let mut check_exists_only: Option<bool> = None;
        if let Some(query) = req.uri().query() {
            let query_pairs: HashMap<_, _> =
                url::form_urlencoded::parse(query.as_bytes()).collect();
            file_type = query_pairs
                .get("file_type")
                .and_then(|s| s.as_ref().try_into().ok());
            start_off = query_pairs
                .get("start_off")
                .and_then(|s| s.parse::<u64>().ok());
            end_off = query_pairs
                .get("end_off")
                .and_then(|s| s.parse::<u64>().ok());
            check_exists_only = query_pairs
                .get("check_exists_only")
                .and_then(|s| s.parse::<bool>().ok());
        }
        (
            file_type.unwrap_or(FileType::Sst),
            start_off.unwrap_or_default(),
            end_off,
            check_exists_only.unwrap_or_default(),
        )
    }

    const DFS_FILE_READ_CONCURRENCY: usize = 64;

    async fn handle_dfs_file_read(
        req: Request<Body>,
        ctx: &StatusContext,
    ) -> hyper::Result<Response<Body>> {
        let id_opt = Self::get_dfs_file_id(&req);
        if id_opt.is_none() {
            return Ok(make_response(StatusCode::BAD_REQUEST, "invalid file id"));
        }
        let id = id_opt.unwrap();
        let (file_type, start_off, end_off, check_exists_only) = Self::get_dfs_read_args(&req);
        let handle_res = tokio::runtime::Handle::try_current();
        if let Err(handle_err) = handle_res {
            error!("Failed to get tokio runtime handle: {:?}", handle_err);
            return Ok(make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to get tokio runtime handle",
            ));
        }
        let _permit = match ctx.dfs_file_read_semaphore.try_acquire() {
            Err(_) => {
                warn!("too many concurrent DFS file read requests");
                return Ok(make_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "too many concurrent DFS file read requests",
                ));
            }
            Ok(permit) => permit,
        };
        let start = Instant::now_coarse();
        let handle = handle_res.unwrap();
        let engine = ctx.kvengine.clone();
        let spawn_res = handle
            .spawn_blocking(move || match file_type {
                FileType::TxnChunk => {
                    debug_assert_eq!(start_off, 0);
                    engine
                        .get_txn_chunk_manager()
                        .read_local_chunk(id, check_exists_only)
                }
                _ => engine.read_local_file(id, file_type, start_off, end_off, check_exists_only),
            })
            .await;
        if let Err(handle_err) = spawn_res {
            error!("Failed to execute blocking task: {:?}", handle_err);
            return Ok(make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to execute blocking task",
            ));
        }
        let read_res = spawn_res.unwrap();
        if let Err(read_err) = read_res {
            error!("Failed to read file: {:?}", read_err);
            return Ok(make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to read file {:?}", read_err),
            ));
        }
        info!(
            "read local dfs file {} takes {:?}",
            id,
            start.saturating_elapsed()
        );
        let data = read_res.unwrap();
        Ok(Response::builder()
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(data))
            .unwrap())
    }

    async fn handle_dfs_file_create(
        req: Request<Body>,
        engine: kvengine::Engine,
    ) -> hyper::Result<Response<Body>> {
        let id_opt = Self::get_dfs_file_id(&req);
        if id_opt.is_none() {
            return Ok(make_response(StatusCode::BAD_REQUEST, "invalid file id"));
        }
        let id = id_opt.unwrap();
        let (file_type, ..) = Self::get_dfs_read_args(&req);
        let data = hyper::body::to_bytes(req.into_body()).await?;
        let (callback, future) = paired_future_callback();
        std::thread::spawn(move || {
            let res = match file_type {
                FileType::TxnChunk => engine.get_txn_chunk_manager().write_local_chunk(id, data),
                _ => engine.write_local_file_if_not_exists(id, data, file_type),
            };
            callback(res);
        });
        let res = future.await.unwrap();
        Ok(match res {
            Ok(_) => make_response(StatusCode::OK, ""),
            Err(err) => make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal Server Error {}", err),
            ),
        })
    }

    async fn handle_recovery(
        req: Request<Body>,
        engine: &kvengine::Engine,
        data_dir: &str,
    ) -> hyper::Result<Response<Body>> {
        let path = req.uri().path();
        match path {
            "/recovery/mode" => Ok(recovery::handle_mode(req, || {
                engine.unblock_keyspace_compaction()
            })),
            "/recovery/rejected" => {
                let pending_compact_shards = engine.get_pending_compaction_shards();
                Ok(recovery::handle_rejected(req, pending_compact_shards))
            }
            "/recovery/white_list" => Ok(recovery::handle_while_list(req, || {
                engine.unblock_keyspace_compaction()
            })),
            "/recovery/black_list" => Ok(recovery::handle_black_list(req, data_dir)),
            _ => Ok(make_response(StatusCode::NOT_FOUND, "Not Found")),
        }
    }

    async fn backup_rfengine(
        req: Request<Body>,
        ctx: &StatusContext,
    ) -> hyper::Result<Response<Body>> {
        let body = hyper::body::to_bytes(req.into_body()).await?;
        let backup_config: serde_json::Result<rfengine::BackupConfig> =
            serde_json::from_slice(&body);
        if backup_config.is_err() {
            return Ok(make_response(StatusCode::BAD_REQUEST, "Bad request body"));
        }
        let backup_config = backup_config.unwrap();

        // Check if lightweight backup enabled.
        if backup_config.lightweight && !ctx.rfengine.is_lightweight_backup_enabled() {
            return Ok(make_response(
                StatusCode::BAD_REQUEST,
                "lightweight backup not enabled",
            ));
        }

        let cluster_id = backup_config.cluster_id;
        let mut store_ident = StoreIdent::default();
        let data = ctx
            .rfengine
            .get_state(0, rfengine::STORE_IDENT_KEY)
            .unwrap_or_default();
        store_ident.merge_from_bytes(data.chunk()).unwrap();
        if store_ident.cluster_id != cluster_id {
            warn!(
                "{}: backup id mismatch expect {:?}, got {}",
                store_ident.store_id, store_ident.cluster_id, cluster_id
            );
            return Ok(make_response(
                StatusCode::BAD_REQUEST,
                "cluster id mismatch",
            ));
        }

        let (wait_backup_ts_ok, wait_backup_ts_dur) =
            if let Some(backup_ts) = backup_config.backup_ts {
                ctx.concurrency_manager.update_max_ts(backup_ts.into());

                ctx.concurrency_manager.replace_backup_ts(backup_ts.into());
                let start_time = Instant::now_coarse();
                let ok = ctx
                    .concurrency_manager
                    .wait_old_backup_ts_released(
                        backup_ts.into(),
                        backup_config.backup_ts_wait_timeout(),
                        backup_config.backup_ts_ttl(),
                        BACKUP_TS_WAIT_RETRY_INTERVAL,
                    )
                    .await;
                (ok, Some(start_time.saturating_elapsed()))
            } else {
                (true, None)
            };

        let s3fs =
            kvengine::dfs::S3Fs::new_from_config(ctx.cfg_controller.get_current().dfs.clone());
        let (callback, future) = paired_future_callback();
        let task = rfengine::BackupTask::new(Box::new(s3fs), callback, backup_config);
        ctx.rfengine.backup(task);
        Ok(match future.await {
            Ok(resp) => match resp {
                Ok(mut meta) => {
                    info!("{}: backup finished", meta.store_id; "wait_backup_ts" => ?wait_backup_ts_dur);
                    estimate_backup_size_by(&ctx.kvengine, meta.mut_keyspace_size());
                    meta.set_has_missing_commit_record(!wait_backup_ts_ok);
                    Response::builder()
                        .body(Body::from(meta.write_to_bytes().unwrap()))
                        .unwrap()
                }
                Err(err) => {
                    error!("{}: backup failed {:?}", store_ident.store_id, &err);
                    make_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Internal Server Error {}", err),
                    )
                }
            },
            Err(e) => make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal Server Error {}", e),
            ),
        })
    }

    async fn rfengine_track_wal_progress(
        req: Request<Body>,
        engine: &RfEngine,
    ) -> hyper::Result<Response<Body>> {
        let body = hyper::body::to_bytes(req.into_body()).await?;
        let _track_req: TrackWalProgressRequest = match serde_json::from_slice(&body) {
            Ok(track_req) => track_req,
            Err(err) => {
                return Ok(make_response(
                    StatusCode::BAD_REQUEST,
                    format!("bad request {}", err),
                ));
            }
        };
        // TODO: use track_req.ts to track async commit locks.

        let (callback, future) = paired_future_callback();
        engine.get_wal_progress(callback);

        Ok(match future.await {
            Ok(resp) => match resp {
                Ok(progress) => Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from(serde_json::to_string(&progress).unwrap()))
                    .unwrap(),
                Err(err) => {
                    error!("rfengine_track_wal_progress error: {:?}", err);
                    make_response(StatusCode::INTERNAL_SERVER_ERROR, format!("{}", err))
                }
            },
            Err(e) => make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal Server Error {}", e),
            ),
        })
    }

    fn get_covered_shards_by_range(
        range: Option<(Vec<u8>, Vec<u8>)>,
        engine: &kvengine::Engine,
    ) -> Result<Vec<Arc<Shard>>> {
        let shards = engine.get_all_shard_id_vers();
        let mut partial_covered_shard = None;
        let ret: Vec<Arc<Shard>> = shards
            .into_iter()
            .filter_map(|s| engine.get_shard(s.id))
            .filter(|s| {
                if range.is_none() {
                    return true;
                }
                let range = range.as_ref().unwrap();
                let full_cover = s.outer_start >= range.0 && s.outer_end <= range.1;
                if !full_cover && s.outer_start < range.1 && s.outer_end > range.0 {
                    partial_covered_shard = Some(s.clone());
                }
                full_cover
            })
            .collect();
        if let Some(s) = partial_covered_shard {
            return Err(box_err!(
                "Shard {} [{:?}-{:?}) is partial covered by range {:?}",
                s.tag(),
                s.outer_start,
                s.outer_end,
                range
            ));
        }
        Ok(ret)
    }

    async fn get_restore_shard_request(
        raw_req: Request<Body>,
    ) -> hyper::Result<RestoreShardRequest> {
        let mut body = Vec::new();
        raw_req
            .into_body()
            .try_for_each(|bytes| {
                body.extend(bytes);
                ok(())
            })
            .await?;
        let mut cs = kvenginepb::ChangeSet::default();
        cs.merge_from_bytes(&body).unwrap();
        Ok(RestoreShardRequest { cs })
    }

    async fn restore_shard(
        req: Request<Body>,
        ctx: &StatusContext,
    ) -> hyper::Result<Response<Body>> {
        let accept_pb = req.headers().is_accept_protobuf();
        let req = Self::get_restore_shard_request(req).await?;
        let tag = tag_from_cs(&ctx.kvengine, &req.cs);
        let shard_id = req.cs.get_shard_id();
        debug!("[{}] receive restore_shard request: {:?}", tag, req);

        if let Err(errpb) = check_available_space(&ctx.kvengine) {
            warn!("[{}] reject restore shard, low space", tag; "err" => ?errpb);
            return Ok(make_errpb_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &errpb,
                accept_pb,
            ));
        }
        if let Err(errpb) = check_follower_available_space(ctx, shard_id, req.cs.shard_ver).await {
            warn!("[{}] reject restore shard, follower low space", tag; "err" => ?errpb);
            return Ok(make_errpb_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &errpb,
                accept_pb,
            ));
        }

        let (cb, fut) = paired_future_callback();
        let callback = Callback::write(Box::new(move |res| {
            cb(res);
        }));
        ctx.router.send_casual_msg(
            req.cs.get_shard_id(),
            CasualMessage::RestoreShard {
                cs: req.cs,
                callback,
            },
        );

        let res = match fut.await {
            Ok(res) => res,
            Err(e) => {
                let err_msg = format!("{} restore_shard channel error: {:?}", shard_id, e);
                error!("{}", err_msg);
                return Ok(make_response(StatusCode::INTERNAL_SERVER_ERROR, err_msg));
            }
        };
        if res.response.get_header().has_error() {
            error!(
                "{} restore_shard error: {:?}",
                tag,
                res.response.get_header().get_error()
            );
            Ok(make_errpb_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                res.response.get_header().get_error(),
                accept_pb,
            ))
        } else {
            let stat = ctx.kvengine.get_shard_stat(shard_id);
            let resp = RestoreShardResponse {
                shard_id,
                restore_bytes: stat.kv_size,
            };
            let json = serde_json::to_string_pretty(&resp).unwrap();
            Ok(Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json))
                .unwrap())
        }
    }

    async fn handle_schema_file(
        req: Request<Body>,
        router: &RaftRouter,
        engine: &kvengine::Engine,
    ) -> hyper::Result<Response<Body>> {
        let bad_request_resp = |msg: &str| make_response(StatusCode::BAD_REQUEST, msg.to_owned());
        let query = req.uri().query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        let keyspace_id = match get_uint_param(&query_pairs, "keyspace_id") {
            Some(keyspace_id) => keyspace_id,
            None => {
                return Ok(bad_request_resp("keyspace_id not found"));
            }
        };
        let file_id = match get_uint_param(&query_pairs, "file_id") {
            Some(file_id) => file_id,
            None => {
                return Ok(bad_request_resp("file_id not found"));
            }
        };
        let schema_file = match engine.load_schema_file(file_id).await {
            Ok(file) => file,
            Err(e) => {
                let msg = format!("failed to load schema file {:?}", e);
                return Ok(make_response(StatusCode::INTERNAL_SERVER_ERROR, msg));
            }
        };
        let (start, end) = ApiV2::get_txn_keyspace_range(keyspace_id as u32);
        let (callback, future) = paired_future_callback();
        router.send_store_msg(StoreMsg::GetRegionsInRange {
            start,
            end,
            callback,
        });
        let res = future.await;
        if let Err(err) = res {
            return Ok(make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to get regions in range: {err:?}"),
            ));
        }
        let region_id_vers = res.unwrap();
        for region_id_ver in region_id_vers {
            router.send_casual_msg(
                region_id_ver.id(),
                CasualMessage::UpdateSchemaFile(schema_file.clone()),
            );
        }
        Ok(Response::new(Body::empty()))
    }

    /// Clear columnar data of the specified keyspace or shard.
    ///
    /// If keyspace_id is 0, clear all columnar data of all shards.
    /// POST /clear_columnar?[restore_version=xxxx]
    /// [&keyspace_id=xxx][&shard_id=1][&confirm_all=true|false]
    async fn handle_clear_columnar(
        req: Request<Body>,
        router: &RaftRouter,
    ) -> hyper::Result<Response<Body>> {
        let bad_request_resp = |msg: &str| make_response(StatusCode::BAD_REQUEST, msg.to_owned());
        let query = req.uri().query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        if !query_pairs.contains_key("keyspace_id") && !query_pairs.contains_key("shard_id") {
            return Ok(bad_request_resp("keyspace_id or shard_id not found"));
        }
        // If restore_version is not set, do no update the restore version.
        let restore_version = get_uint_param(&query_pairs, "restore_version");
        let mut shard_ids = vec![];
        if query_pairs.contains_key("keyspace_id") {
            let keyspace_id = match get_uint_param(&query_pairs, "keyspace_id") {
                Some(keyspace_id) => keyspace_id,
                None => {
                    return Ok(bad_request_resp("keyspace_id not found"));
                }
            };
            // If keyspace_id is 0, clear all columnar data of all shards. To avoid mistake,
            // we require confirm_all to be true.
            let (start, end) = if keyspace_id == 0 {
                // Check confirm_all to avoid mistake.
                if !query_pairs.contains_key("confirm_all") {
                    return Ok(bad_request_resp("param confirm_all not found"));
                }
                let confirmed = match get_bool_param(&query_pairs, "confirm_all") {
                    Some(confirmed) => confirmed,
                    None => return Ok(bad_request_resp("param confirm_all must be true or false")),
                };
                if !confirmed {
                    return Ok(bad_request_resp(
                        "please set param confirm_all to true to clear all columnar data",
                    ));
                }
                (vec![TXN_KEY_PREFIX], GLOBAL_SHARD_END_KEY.to_vec())
            } else {
                ApiV2::get_txn_keyspace_range(keyspace_id as u32)
            };
            let (callback, future) = paired_future_callback();
            router.send_store_msg(StoreMsg::GetRegionsInRange {
                start,
                end,
                callback,
            });
            let res = future.await;
            if let Err(err) = res {
                return Ok(make_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to get regions in range: {err:?}"),
                ));
            }
            let region_id_vers = res.unwrap();
            for region_id_ver in region_id_vers {
                shard_ids.push(region_id_ver.id());
                router.send_casual_msg(
                    region_id_ver.id(),
                    CasualMessage::ClearColumnar { restore_version },
                );
            }
        } else {
            let shard_id = match get_uint_param(&query_pairs, "shard_id") {
                Some(shard_id) => shard_id,
                None => {
                    return Ok(bad_request_resp("shard_id not found"));
                }
            };
            shard_ids.push(shard_id);
            router.send_casual_msg(shard_id, CasualMessage::ClearColumnar { restore_version });
        }

        Ok(Response::new(Body::from(format!(
            "clear columnar success, shard_ids: {:?}\n",
            shard_ids
        ))))
    }

    /// Enable/disable to build columnar tables
    ///
    /// POST /build_columnar?switch=true|false
    async fn handle_build_columnar(
        req: Request<Body>,
        engine: &kvengine::Engine,
    ) -> hyper::Result<Response<Body>> {
        let query = req.uri().query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        if let Some(switch) = get_bool_param(&query_pairs, "switch") {
            let previous = engine.opts.set_build_columnar(switch);
            info!("build columnar switch: {} -> {}", previous, switch);
            Ok(make_response(
                StatusCode::OK,
                format!("build columnar: {} -> {}", previous, switch),
            ))
        } else {
            Ok(make_response(
                StatusCode::BAD_REQUEST,
                "param switch not found",
            ))
        }
    }

    /// Enable/disable to build fts index
    ///
    /// POST /build_fts_index?switch=true|false
    async fn handle_build_fts_index(
        req: Request<Body>,
        engine: &kvengine::Engine,
    ) -> hyper::Result<Response<Body>> {
        let query = req.uri().query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        if let Some(switch) = get_bool_param(&query_pairs, "switch") {
            let previous = engine.opts.set_build_fts_index(switch);
            info!("build fts index switch: {} -> {}", previous, switch);
            Ok(make_response(
                StatusCode::OK,
                format!("build fts index: {} -> {}", previous, switch),
            ))
        } else {
            Ok(make_response(
                StatusCode::BAD_REQUEST,
                "param switch not found",
            ))
        }
    }

    pub fn stop(self) {
        let _ = self.close_tx.send(());
        let _ = self.close_handle.unwrap().join();
    }

    // Return listening address, this may only be used for outer test
    // to get the real address because we may use "127.0.0.1:0"
    // in test to avoid port conflict.
    #[allow(unused)]
    pub fn listening_addr(&self) -> SocketAddr {
        self.addr.unwrap()
    }
}

#[cfg(debug_assertions)]
impl StatusServer {
    async fn handle_debug_sleep() -> hyper::Result<Response<Body>> {
        info!("sleep for 5 seconds");
        std::thread::sleep(Duration::from_secs(5));
        info!("sleep done");
        Ok(Response::default())
    }
}

fn get_last_path_segment(path: &str) -> &str {
    let mut last = "";
    for i in (0..path.len()).rev() {
        if path.as_bytes()[i] == b'/' {
            last = &path[i + 1..];
            break;
        }
    }
    last
}

fn get_uint_param(query_pairs: &HashMap<Cow<'_, str>, Cow<'_, str>>, key: &str) -> Option<u64> {
    query_pairs.get(key).and_then(|v| u64::from_str(v).ok())
}

fn get_bool_param(query_pairs: &HashMap<Cow<'_, str>, Cow<'_, str>>, key: &str) -> Option<bool> {
    query_pairs.get(key).and_then(|v| bool::from_str(v).ok())
}

impl StatusServer {
    pub async fn dump_region_meta(
        _req: Request<Body>,
        _router: &RaftRouter,
    ) -> hyper::Result<Response<Body>> {
        // TODO(x)
        Ok(hyper::Response::new(Body::empty()))
    }

    async fn handle_sync_region_by_id(
        req: Request<Body>,
        ctx: &StatusContext,
    ) -> hyper::Result<Response<Body>> {
        let mut body = Vec::new();
        req.into_body()
            .try_for_each(|bytes| {
                body.extend(bytes);
                ok(())
            })
            .await?;

        let SyncRegionByIdRequest { region_id, debug } = match serde_json::from_slice(&body) {
            Ok(req) => req,
            Err(e) => {
                return Ok(make_response(
                    StatusCode::BAD_REQUEST,
                    format!("invalid request body: {:?}", e),
                ));
            }
        };

        let resp = ctx.region_info_accessor.find_region_by_id(region_id);
        let body = if debug {
            format!("{:?}", resp).into_bytes()
        } else {
            Self::make_sync_region_resp(
                ctx.stores_info.pd_client.get_cluster_id().unwrap(),
                ctx.rfengine.get_engine_id(),
                resp.as_slice(),
            )
            .write_to_bytes()
            .unwrap()
        };
        Ok(Response::new(body.into()))
    }

    async fn handle_sync_region(
        req: Request<Body>,
        ctx: &StatusContext,
    ) -> hyper::Result<Response<Body>> {
        let mut body = Vec::new();
        req.into_body()
            .try_for_each(|bytes| {
                body.extend(bytes);
                ok(())
            })
            .await?;

        let SyncRegionRequest {
            start,
            end,
            limit,
            reverse,
            debug,
        } = match serde_json::from_slice(&body) {
            Ok(req) => req,
            Err(e) => {
                return Ok(make_response(
                    StatusCode::BAD_REQUEST,
                    format!("invalid request body: {:?}", e),
                ));
            }
        };

        let (start, mut end) = match (hex::decode(&start), hex::decode(&end)) {
            (Ok(start), Ok(end)) if end.is_empty() || start < end => (start, end),
            _ => {
                return Ok(make_response(
                    StatusCode::BAD_REQUEST,
                    format!("invalid range: [{}, {})", start, end),
                ));
            }
        };

        let start = encode_bytes(&start);
        if end.is_empty() {
            end.extend(GLOBAL_SHARD_END_KEY);
        }
        let end = encode_bytes(&end);
        let resp = ctx
            .region_info_accessor
            .get_regions_in_range_opt(&start, &end, true, reverse, limit);
        let body = if debug {
            format!("{:#?}", resp).into_bytes()
        } else {
            Self::make_sync_region_resp(
                ctx.stores_info.pd_client.get_cluster_id().unwrap(),
                ctx.rfengine.get_engine_id(),
                &resp,
            )
            .write_to_bytes()
            .unwrap()
        };
        Ok(Response::new(body.into()))
    }

    fn make_sync_region_resp(
        cluster_id: u64,
        store_id: u64,
        region_infos: &[RegionInfo],
    ) -> SyncRegionResponse {
        let cap = region_infos.len();
        let mut resp_regions = Vec::with_capacity(cap);
        let mut resp_stats = Vec::with_capacity(cap);
        let mut resp_leaders = Vec::with_capacity(cap);
        let mut resp_buckets = Vec::with_capacity(cap);
        let mut resp_down_peers: Vec<pdpb::PeersStats> = Vec::with_capacity(cap);
        let mut resp_pending_peers: Vec<Peers> = Vec::with_capacity(cap);
        for info in region_infos {
            let region = &info.region;
            resp_regions.push(region.clone());
            // The stats are used along with region, we need to push a default one if not
            // found.
            let region_stat = pdpb::RegionStat::new();
            let down_peers = pdpb::PeersStats::new();
            let pending_peers = Peers::new();
            resp_stats.push(region_stat);
            resp_down_peers.push(down_peers);
            resp_pending_peers.push(pending_peers);
            let leader_peer = find_peer(region, store_id).cloned().unwrap_or_default();
            resp_leaders.push(leader_peer);
            let mut bucket = metapb::Buckets::new();
            bucket.set_region_id(region.id);
            bucket.set_version(info.buckets.version);
            bucket.set_keys(info.buckets.keys.clone().into());
            bucket.set_stats(BucketStats::new());
            resp_buckets.push(bucket);
        }
        let mut resp = SyncRegionResponse::new();
        resp.mut_header().set_cluster_id(cluster_id);
        resp.set_regions(resp_regions.into());
        resp.set_region_leaders(resp_leaders.into());
        resp.set_region_stats(resp_stats.into());
        resp.set_buckets(resp_buckets.into());
        resp.set_down_peers(resp_down_peers.into());
        resp.set_pending_peers(resp_pending_peers.into());
        resp
    }

    fn handle_get_metrics(
        req: Request<Body>,
        mgr: &ConfigController,
    ) -> hyper::Result<Response<Body>> {
        let should_simplify = mgr.get_current().server.simplify_metrics;
        let gz_encoding = client_accept_gzip(&req);
        let metrics = if gz_encoding {
            // gzip can reduce the body size to less than 1/10.
            let mut encoder = GzEncoder::new(vec![], Compression::default());
            dump_to(&mut encoder, should_simplify);
            encoder.finish().unwrap()
        } else {
            dump(should_simplify).into_bytes()
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

    fn start_serve<I, C>(&mut self, builder: HyperBuilder<I>)
    where
        I: Accept<Conn = C, Error = std::io::Error> + Send + 'static,
        I::Error: Into<Box<dyn StdError + Send + Sync>>,
        I::Conn: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        C: ServerConnection,
    {
        let ctx = Arc::new(StatusContext {
            security_config: self.security_config.clone(),
            cfg_controller: self.cfg_controller.clone(),
            router: self.router.clone(),
            kvengine: self.kvengine.clone(),
            rfengine: self.rfengine.clone(),
            concurrency_manager: self.concurrency_manager.clone(),
            dfs_file_read_semaphore: Semaphore::new(Self::DFS_FILE_READ_CONCURRENCY),
            stores_info: Arc::new(StoresInfo::new(self.pd_client.clone())),
            thread_pool: self.thread_pool.handle().clone(),
            region_info_accessor: self.region_info_accessor.clone(),
        });
        // Start to serve.
        let server = builder.serve(make_service_fn(move |conn: &C| {
            let x509 = conn.get_x509();
            let ctx = ctx.clone();
            async move {
                // Create a status service.
                Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
                    let start = Instant::now_coarse();
                    let x509 = x509.clone();
                    let ctx = ctx.clone();
                    tikv_util::init_task_local(async move {
                        let path = req.uri().path().to_owned();
                        let method = req.method().to_owned();

                        #[cfg(feature = "failpoints")]
                        {
                            if path.starts_with(FAIL_POINTS_REQUEST_PATH) {
                                return handle_fail_points_request(req).await;
                            }
                        }

                        // 1. POST "/config" will modify the configuration of TiKV.
                        // 2. GET "/region" will get start key and end key. These keys could be
                        // actual user data since in some cases the data
                        // itself is stored in the key.
                        let should_check_cert = !matches!(
                            (&method, path.as_ref()),
                            (&Method::GET, "/metrics")
                                | (&Method::GET, "/status")
                                | (&Method::GET, "/config")
                                | (&Method::GET, "/debug/pprof/profile")
                        );

                        if should_check_cert && !check_cert(&ctx.security_config, x509) {
                            return Ok(make_response(
                                StatusCode::FORBIDDEN,
                                "certificate role error",
                            ));
                        }

                        let resp = match (method.clone(), path.as_ref()) {
                            (Method::GET, "/metrics") => {
                                Self::handle_get_metrics(req, &ctx.cfg_controller)
                            }
                            (Method::GET, "/status") => Ok(Response::default()),
                            (Method::GET, "/debug/pprof/heap_list") => {
                                Ok(make_response(
                                    StatusCode::GONE,
                                    "Deprecated, heap profiling is always enabled by default, just use /debug/pprof/heap to get the heap profile when needed",
                                ))
                            }
                            (Method::GET, "/debug/pprof/heap_activate") => {
                                Ok(make_response(
                                    StatusCode::GONE,
                                    "Deprecated, use config `memory.enable_heap_profiling` to toggle",
                                ))
                            }
                            (Method::GET, "/debug/pprof/heap_deactivate") => {
                                Ok(make_response(
                                    StatusCode::GONE,
                                    "Deprecated, use config `memory.enable_heap_profiling` to toggle",
                                ))
                            }
                            (Method::GET, "/debug/pprof/heap") => {
                                Self::dump_heap_prof_to_resp(req)
                            }
                            (Method::GET, "/debug/pprof/cmdline") => Self::get_cmdline(req),
                            (Method::GET, "/debug/pprof/symbol") => {
                                Self::get_symbol_count(req)
                            }
                            (Method::POST, "/debug/pprof/symbol") => Self::get_symbol(req).await,
                            (Method::GET, "/config") => {
                                Self::get_config(req, &ctx.cfg_controller).await
                            }
                            (Method::POST, "/config") => {
                                Self::update_config(ctx.cfg_controller.clone(), req).await
                            }
                            (Method::GET, "/debug/pprof/profile") => {
                                Self::dump_cpu_prof_to_resp(req).await
                            }
                            (Method::GET, "/debug/fail_point") => {
                                info!("debug fail point API start");
                                fail_point!("debug_fail_point");
                                info!("debug fail point API finish");
                                Ok(Response::default())
                            }
                            #[cfg(debug_assertions)]
                            (Method::GET, "/debug/sleep") => Self::handle_debug_sleep().await,
                            (Method::GET, path) if path.starts_with("/region") => {
                                Self::dump_region_meta(req, &ctx.router).await
                            }
                            (Method::GET, path) if path.starts_with("/sync_region_by_id") => {
                                let resp = Self::handle_sync_region_by_id(req, ctx.as_ref()).await?;
                                STATUS_REQ_HISTOGRAM_STATIC
                                    .sync_region_by_id
                                    .observe(start.saturating_elapsed().as_secs_f64());
                                Ok(resp)
                            }
                            (Method::GET, path) if path.starts_with("/sync_region") => {
                                let resp = Self::handle_sync_region(req, ctx.as_ref()).await?;
                                STATUS_REQ_HISTOGRAM_STATIC
                                    .sync_region
                                    .observe(start.saturating_elapsed().as_secs_f64());
                                Ok(resp)
                            }
                            (Method::PUT, path) if path.starts_with("/log-level") => {
                                Self::change_log_level(req).await
                            }
                            (Method::GET, path) if path.starts_with("/kvengine") => {
                                let res = if path.starts_with("/kvengine/snapshot/") {
                                    Self::dump_kvengine_snapshot(req, &ctx.kvengine, &ctx.router).await
                                } else if path.starts_with("/kvengine/columnar_status") {
                                    Self::collect_columnar_status(req, &ctx)
                                } else if path.starts_with("/kvengine/columnar_index_stats") {
                                    Self::collect_columnar_index_stats(req, &ctx).await
                                } else if path.starts_with("/kvengine/meta/") {
                                    Self::dump_kvengine_meta(req, &ctx.kvengine).await
                                } else {
                                    Self::dump_kvengine_stats(req, &ctx.kvengine).await
                                };
                                STATUS_REQ_HISTOGRAM_STATIC
                                    .kvengine
                                    .observe(start.saturating_elapsed().as_secs_f64());
                                res
                            }
                            (Method::GET, path) if path.starts_with("/rfengine") => {
                                if path.starts_with("/rfengine/wal_chunk") {
                                    let res = Self::rfengine_wal_chunk(req, &ctx.rfengine).await;
                                    STATUS_REQ_HISTOGRAM_STATIC
                                        .rf_wal_chunk
                                        .observe(start.saturating_elapsed().as_secs_f64());
                                    res
                                } else {
                                    Self::dump_rfengine_stats(req, &ctx.rfengine).await
                                }
                            }
                            (Method::POST, path) if path.starts_with("/rfengine/backup") => {
                                let res = Self::backup_rfengine(req, &ctx).await;
                                STATUS_REQ_HISTOGRAM_STATIC
                                    .rf_backup
                                    .observe(start.saturating_elapsed().as_secs_f64());
                                res
                            }
                            (Method::POST, path) if path.starts_with("/rfengine/track_wal_progress") => {
                                let res = Self::rfengine_track_wal_progress(req, &ctx.rfengine).await;
                                STATUS_REQ_HISTOGRAM_STATIC
                                    .rf_track_wal_progress
                                    .observe(start.saturating_elapsed().as_secs_f64());
                                res
                            }
                            (Method::POST, path) if path.starts_with("/restore-shard") => {
                                let res = Self::restore_shard(req, &ctx).await;
                                STATUS_REQ_HISTOGRAM_STATIC
                                    .restore_shard
                                    .observe(start.saturating_elapsed().as_secs_f64());
                                res
                            }
                            (Method::POST, path) if path.starts_with("/kvengine/compactor") => {
                                Self::add_remote_compactor(req, &ctx.kvengine.comp_client).await
                            }
                            (Method::POST, path) if path.starts_with("/ingest_files") => {
                                let res = Self::ingest_files(req, &ctx).await;
                                STATUS_REQ_HISTOGRAM_STATIC
                                    .ingest_files
                                    .observe(start.saturating_elapsed().as_secs_f64());
                                res
                            }
                            (Method::POST, path) if path.starts_with("/unsafe_recover") => {
                                Self::unsafe_recover(req, &ctx.rfengine, &ctx.kvengine).await
                            }
                            (Method::POST, path) if path.starts_with("/major-compact") => {
                                Self::major_compact(req, &ctx.rfengine, &ctx.router).await
                            }
                            (Method::POST, path) if path.starts_with("/flush") => {
                                Self::flush(req, &ctx.router, &ctx.kvengine).await
                            }
                            (Method::POST, path) if path.starts_with("/schema_file") => {
                                let res = Self::handle_schema_file(req, &ctx.router, &ctx.kvengine).await;
                                STATUS_REQ_HISTOGRAM_STATIC
                                    .schema_file
                                    .observe(start.saturating_elapsed().as_secs_f64());
                                res
                            }
                            (Method::POST, path) if path.starts_with("/clear_columnar") => {
                                Self::handle_clear_columnar(req, &ctx.router).await
                            }
                            (Method::POST, path) if path.starts_with("/build_columnar") => {
                                Self::handle_build_columnar(req, &ctx.kvengine).await
                            }
                            (Method::POST, path) if path.starts_with("/build_fts_index") => {
                                Self::handle_build_fts_index(req, &ctx.kvengine).await
                            }
                            (Method::GET, path) if path.starts_with("/dfs/") => {
                                Self::handle_dfs_file_read(req, &ctx).await
                            }
                            (Method::POST, path) if path.starts_with("/dfs/") => {
                                Self::handle_dfs_file_create(req, ctx.kvengine.clone()).await
                            }
                            (Method::GET | Method::POST, path) if path.starts_with("/recovery/") => {
                                let data_dir = &ctx.cfg_controller.get_current().storage.data_dir;
                                Self::handle_recovery(req, &ctx.kvengine, data_dir).await
                            }
                            _ => Ok(make_response(StatusCode::NOT_FOUND, "path not found")),
                        };
                        let dur = start.saturating_elapsed();
                        if dur > Duration::from_millis(100) {
                            info!(
                                "slow status request";
                                "path" => %path,
                                "method" => ?method,
                                "elapsed" => ?dur,
                            );
                        }
                        resp
                    })
                }))
            }
        }));

        let close_rx = self.close_rx.take().unwrap();
        let thread_pool = self.thread_pool.clone();
        let close_handle = std::thread::spawn(move || {
            thread_pool.block_on(async move {
                tokio::select! {
                    res = server => {
                        res.expect("status server exit with error");
                    }
                    _ = close_rx => {}
                }
            })
        });
        self.close_handle = Some(close_handle);
    }

    pub fn start(&mut self, status_addr: String) -> Result<()> {
        let addr = SocketAddr::from_str(&status_addr)?;

        let mut incoming = {
            let _enter = self.thread_pool.enter();
            AddrIncoming::bind(&addr)
        }?;

        incoming.set_keepalive(Some(SERVER_TCP_KEEPALIVE));
        incoming.set_nodelay(true);

        self.addr = Some(incoming.local_addr());

        match self.pd_client.get_security_mgr().acceptor(incoming)? {
            Either::Left(addr_incoming) => {
                let server =
                    Server::builder(addr_incoming).http1_header_read_timeout(SERVER_READ_TIMEOUT);
                self.start_serve(server);
            }
            Either::Right(tls_acceptor) => {
                let server =
                    Server::builder(tls_acceptor).http1_header_read_timeout(SERVER_READ_TIMEOUT);
                self.start_serve(server);
            }
        };
        Ok(())
    }
}

struct StatusContext {
    security_config: Arc<SecurityConfig>,
    cfg_controller: ConfigController,
    router: RaftRouter,
    kvengine: kvengine::Engine,
    rfengine: RfEngine,
    concurrency_manager: ConcurrencyManager,
    dfs_file_read_semaphore: Semaphore,
    stores_info: Arc<StoresInfo>,
    thread_pool: tokio::runtime::Handle,
    region_info_accessor: RegionInfoAccessor,
}

// To unify TLS/Plain connection usage in start_serve function
trait ServerConnection {
    fn get_x509(&self) -> Option<X509>;
}

impl ServerConnection for TlsStream<AddrStream> {
    fn get_x509(&self) -> Option<X509> {
        // TODO: support return x509
        None
    }
}

impl ServerConnection for AddrStream {
    fn get_x509(&self) -> Option<X509> {
        None
    }
}

#[derive(Clone)]
struct X509 {
    common_name: String,
}

// Check if the peer's x509 certificate meets the requirements, this should
// be called where the access should be controlled.
//
// For now, the check only verifies the role of the peer certificate.
fn check_cert(security_config: &SecurityConfig, x509: Option<X509>) -> bool {
    // if `cert_allowed_cn` is empty, skip check and return true
    if !security_config.cert_allowed_cn.is_empty() {
        if let Some(x509) = x509 {
            // Check common name in peer cert
            return security::match_peer_names(&security_config.cert_allowed_cn, &x509.common_name);
        }
        false
    } else {
        true
    }
}
// For handling fail points related requests
#[cfg(feature = "failpoints")]
async fn handle_fail_points_request(req: Request<Body>) -> hyper::Result<Response<Body>> {
    let path = req.uri().path().to_owned();
    let method = req.method().to_owned();
    let fail_path = format!("{}/", FAIL_POINTS_REQUEST_PATH);
    let fail_path_has_sub_path: bool = path.starts_with(&fail_path);

    match (method, fail_path_has_sub_path) {
        (Method::PUT, true) => {
            let mut buf = Vec::new();
            req.into_body()
                .try_for_each(|bytes| {
                    buf.extend(bytes);
                    ok(())
                })
                .await?;
            let (_, name) = path.split_at(fail_path.len());
            if name.is_empty() {
                return Ok(Response::builder()
                    .status(StatusCode::UNPROCESSABLE_ENTITY)
                    .body(MISSING_NAME.into())
                    .unwrap());
            };

            let actions = String::from_utf8(buf).unwrap_or_default();
            if actions.is_empty() {
                return Ok(Response::builder()
                    .status(StatusCode::UNPROCESSABLE_ENTITY)
                    .body(MISSING_ACTIONS.into())
                    .unwrap());
            };

            if let Err(e) = fail::cfg(name.to_owned(), &actions) {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(e.into())
                    .unwrap());
            }
            let body = format!("Added fail point with name: {}, actions: {}", name, actions);
            Ok(Response::new(body.into()))
        }
        (Method::DELETE, true) => {
            let (_, name) = path.split_at(fail_path.len());
            if name.is_empty() {
                return Ok(Response::builder()
                    .status(StatusCode::UNPROCESSABLE_ENTITY)
                    .body(MISSING_NAME.into())
                    .unwrap());
            };

            fail::remove(name);
            let body = format!("Deleted fail point with name: {}", name);
            Ok(Response::new(body.into()))
        }
        (Method::GET, _) => {
            // In this scope the path must be like /fail...(/...), which starts with
            // FAIL_POINTS_REQUEST_PATH and may or may not have a sub path
            // Now we return 404 when path is neither /fail nor /fail/
            if path != FAIL_POINTS_REQUEST_PATH && path != fail_path {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap());
            }

            // From here path is either /fail or /fail/, return lists of fail points
            let list: Vec<String> = fail::list()
                .into_iter()
                .map(move |(name, actions)| format!("{}={}", name, actions))
                .collect();
            let list = list.join("\n");
            Ok(Response::new(list.into()))
        }
        _ => Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::empty())
            .unwrap()),
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

// Decode different type of json value to string value
fn decode_json(
    data: &[u8],
) -> std::result::Result<std::collections::HashMap<String, String>, Box<dyn std::error::Error>> {
    let json: Value = serde_json::from_slice(data)?;
    if let Value::Object(map) = json {
        let mut dst = std::collections::HashMap::new();
        for (k, v) in map.into_iter() {
            let v = match v {
                Value::Bool(v) => format!("{}", v),
                Value::Number(v) => format!("{}", v),
                Value::String(v) => v,
                Value::Array(_) => return Err("array type are not supported".to_owned().into()),
                _ => return Err("wrong format".to_owned().into()),
            };
            dst.insert(k, v);
        }
        Ok(dst)
    } else {
        Err("wrong format".to_owned().into())
    }
}

fn make_response<T>(status_code: StatusCode, message: T) -> Response<Body>
where
    T: Into<Body>,
{
    Response::builder()
        .status(status_code)
        .body(message.into())
        .unwrap()
}

fn make_errpb_response(
    status_code: StatusCode,
    errpb: &kvproto::errorpb::Error,
    accept_pb: bool,
) -> Response<Body> {
    if !accept_pb {
        make_response(status_code, errpb.get_message().to_string())
    } else {
        Response::builder()
            .status(status_code)
            .header(CONTENT_TYPE, CONTENT_TYPE_PROTOBUF)
            .body(errpb.write_to_bytes().unwrap().into())
            .unwrap()
    }
}

fn check_available_space(
    engine: &kvengine::Engine,
) -> std::result::Result<(), kvproto::errorpb::Error> {
    if engine.is_low_space() {
        Err(make_errpb_disk_full(
            vec![engine.get_engine_id()],
            format!(
                "available space is low: {}",
                ReadableSize(engine.available_space()).as_mb_f64()
            ),
        ))
    } else {
        Ok(())
    }
}

async fn check_follower_available_space(
    ctx: &StatusContext,
    region_id: u64,
    region_ver: u64,
) -> std::result::Result<(), errorpb::Error> {
    let (cb, fut) = paired_future_callback();
    ctx.router.send_store_msg(StoreMsg::GetRegionById {
        region_id,
        callback: cb,
    });
    let region = fut
        .await
        .map_err(|e| {
            // Return not leader to let caller retry.
            make_errpb_not_leader(
                region_id,
                None,
                format!("failed to get region {}: {}", region_id, e),
            )
        })?
        .ok_or_else(|| make_errpb_region_not_found(region_id))?;
    if region.get_region_epoch().version != region_ver {
        return Err(make_errpb_epoch_not_match(region));
    }

    let current_store_id = ctx.kvengine.get_engine_id();
    let followers = region
        .get_peers()
        .iter()
        .filter(|p| p.store_id != current_store_id && p.role != metapb::PeerRole::Learner);
    let mut join_set = JoinSet::new();
    for follower in followers {
        let stores_info = ctx.stores_info.clone();
        let store_id = follower.store_id;
        join_set.spawn_on(
            async move {
                stores_info
                    .get_store_stats(store_id)
                    .await
                    .map(|stats| (store_id, stats))
            },
            &ctx.thread_pool,
        );
    }

    let low_space_threshold = ctx.cfg_controller.get_current().storage.low_space_threshold;

    let mut disk_full_stores = vec![];
    let mut disk_full_stores_available_size = vec![];
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(Ok((store_id, stats))) => {
                if is_store_low_space(stats, low_space_threshold) {
                    disk_full_stores.push(store_id);
                    disk_full_stores_available_size.push(ReadableSize(stats.available).as_mb_f64());
                }
            }
            Ok(Err(pd_err)) => {
                // Return not leader to make caller retry.
                return Err(make_errpb_not_leader(
                    region_id,
                    None,
                    format!("failed to get store stats: {:?}", pd_err),
                ));
            }
            Err(e) => {
                // Return not leader to make caller retry.
                return Err(make_errpb_not_leader(
                    region_id,
                    None,
                    format!("failed to get store stats: {}", e),
                ));
            }
        }
    }

    if disk_full_stores.is_empty() {
        Ok(())
    } else {
        Err(make_errpb_disk_full(
            disk_full_stores,
            format!(
                "available space is low: {:?}",
                disk_full_stores_available_size
            ),
        ))
    }
}

fn is_store_low_space(stats: StoreStatsLite, low_space_threshold: AbsoluteOrPercentSize) -> bool {
    fail::fail_point!("is_store_low_space", |_| true);

    stats.available <= low_space_threshold.absolute(stats.capacity)
}

#[derive(Default, Clone, Copy, Debug)]
struct StoreStatsLite {
    update_time_secs: u64,
    capacity: u64,
    available: u64,
}

type StoresStatsCache =
    papaya::HashMap<u64 /* self */, Arc<tokio::sync::Mutex<StoreStatsLite>>>;

struct StoresInfo {
    pd_client: Arc<dyn PdClient>,
    cache: StoresStatsCache,
}

impl StoresInfo {
    fn new(pd_client: Arc<dyn PdClient>) -> Self {
        Self {
            pd_client,
            cache: StoresStatsCache::default(),
        }
    }

    async fn get_store_stats(&self, store_id: u64) -> pd_client::Result<StoreStatsLite> {
        let stats_lite = self
            .cache
            .pin()
            .get_or_insert_with(store_id, Default::default)
            .clone();
        let mut stats_lite = stats_lite.lock_owned().await;

        let now = UnixSecs::now().into_inner();
        if stats_lite.update_time_secs + STORES_INFO_CACHE_TTL.as_secs() > now {
            return Ok(*stats_lite);
        }

        let stats = match self.pd_client.get_store_stats_async(store_id).await {
            Ok(stats) => stats,
            Err(e) => {
                if stats_lite.update_time_secs + STORES_INFO_CACHE_MAX_TTL.as_secs() > now {
                    warn!("get_store_stats failed, use stale cache"; "store" => store_id, "err" => ?e);
                    return Ok(*stats_lite);
                } else {
                    error!("get_store_stats failed"; "store" => store_id, "err" => ?e);
                    return Err(e);
                }
            }
        };

        stats_lite.update_time_secs = now;
        stats_lite.capacity = stats.capacity;
        stats_lite.available = stats.available;
        info!("get_store_stats"; "store" => store_id, "stats" => ?stats_lite);
        Ok(*stats_lite)
    }
}

#[derive(Debug)]
struct RestoreShardRequest {
    pub cs: kvenginepb::ChangeSet,
}

#[derive(Default, Serialize, Deserialize, Debug, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct RestoreShardResponse {
    pub shard_id: u64,
    pub restore_bytes: u64,
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TruncateTsConfig {
    pub cluster_id: u64,
    pub ts: u64,
    pub range: Option<(Vec<u8>, Vec<u8>)>, // None means apply to all keyspaces.
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TrackWalProgressRequest {
    pub ts: u64,
}

fn estimate_backup_size_by(
    engine: &kvengine::Engine,
    result: &mut std::collections::HashMap<u32, rfenginepb::BackupSize>,
) {
    for shard in engine.shards() {
        let size = result.entry(shard.keyspace_id).or_default();
        size.size += shard.get_estimated_size();
    }
}

fn tag_from_cs(engine: &kvengine::Engine, cs: &kvenginepb::ChangeSet) -> ShardTag {
    ShardTag::new(
        engine.get_engine_id(),
        IdVer::new(cs.shard_id, cs.shard_ver),
    )
}

fn make_errpb_not_leader(
    region_id: u64,
    leader: Option<metapb::Peer>,
    msg: String,
) -> errorpb::Error {
    let mut errpb = errorpb::Error::default();
    errpb.set_message(msg);
    errpb.mut_not_leader().set_region_id(region_id);
    if let Some(leader) = leader {
        errpb.mut_not_leader().set_leader(leader);
    }
    errpb
}

fn make_errpb_region_not_found(region_id: u64) -> errorpb::Error {
    let mut errpb = errorpb::Error::default();
    errpb.mut_region_not_found().set_region_id(region_id);
    errpb
}

fn make_errpb_epoch_not_match(region: metapb::Region) -> errorpb::Error {
    let mut errpb = errorpb::Error::default();
    errpb
        .mut_epoch_not_match()
        .set_current_regions(vec![region].into());
    errpb
}

fn make_errpb_disk_full(stores_id: Vec<u64>, reason: String) -> errorpb::Error {
    let disk_full = errorpb::DiskFull {
        store_id: stores_id,
        reason,
        ..Default::default()
    };
    let mut errpb = errorpb::Error::default();
    errpb.set_disk_full(disk_full);
    errpb
}
