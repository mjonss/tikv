// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{hash_map::Entry, HashMap as StdHashMap},
    fs, mem,
    net::SocketAddr,
    ops,
    ops::Deref,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use api_version::ApiV2;
use bytes::{Buf, Bytes};
use cdc::{metrics::*, CdcEvent, Conn, ConnId};
use collections::{HashMap, HashMapEntry, HashMapExt, HashSet};
use futures::executor::block_on;
use grpcio::{ChannelBuilder, EnvBuilder, ServerBuilder};
use grpcio_health::{create_health, HealthService, ServingStatus};
use hyper::{http, StatusCode};
use kvengine::{
    dfs::Dfs,
    table::{tiny_meta, InnerKey, SnapVersion},
    Engine, IdVer, ShardMeta, ShardTag, SnapAccess, UserMeta, LOCK_CF, WRITE_CF,
};
use kvproto::{
    cdcpb,
    cdcpb::{
        create_change_data, ChangeDataRequest, Event, EventLogType, EventRow, EventRowOpType,
        ResolvedTs,
    },
    metapb, pdpb,
    pdpb::StoreStats,
    raft_cmdpb::AdminRequest,
    tikvpb::create_tikv,
};
use log_wrappers::Value as LogValue;
use merged_engine::{
    peer_is_skippable, ForceStop, MergedEngine, MergedEngineContext, StoreProgress,
};
use native_br::{
    common::{assemble_wal_chunks, collect_wal_chunks_with_retry, CollectWalChunksContext},
    error::Error as BrError,
    wal::AssembledWalData,
};
use pd_client::{
    util::{check_resp_header, get_all_stores_except_tiflash},
    PdClient, RegionStat,
};
use rfengine::{RfEngine, MIN_EPOCH_ROTATE_LEN, TRUNCATE_ALL_INDEX};
use rfstore::store::{ApplyContext, GcRunner};
use security::{HttpClient, SecurityConfig};
use serde_json::{json, Value};
use tikv_util::{
    box_err, box_try, codec, debug, error,
    future::paired_future_callback,
    info,
    mpsc::{Receiver, SendError, Sender},
    thd_name,
    time::Instant,
    trace, warn, worker,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use txn_types::{LockType, TimeStamp};

use crate::{
    apply_observer::{is_index_key, CdcApplyObserver, RegionEvents},
    delegate::{
        InitAlive, InitId, RegionDelegate, RegionResolver, RequestId, RequestInfo, RequestKey,
    },
    kube::{KeyspaceKubeService, KubeApi},
    metrics::*,
    provisioned::KeyspaceProvisionedService,
    safepoint::{KeyspaceChangefeeds, ServiceSafepointManager},
    scheduler::get_cdc_status,
    ticdc_util::TiCdcError,
    util::{
        build_request_range_for_keyspace, keyspace_prefix_len, post_to_ticdc,
        send_request_to_store, ArcTimeStamp, ResolvedTsStats, DISPATCH_CDC_TIMEOUT,
    },
    wal::{
        StoreTargetAndLag, StoreWalProgresses, UpdateWalError, UpdateWalResult, WalCache,
        WalProgressFetcher, WalProgressTargets,
    },
    CdcMsg, Deregister, Error, KeyspaceService, KeyspaceStates, ReplicationScheduler,
    ReplicationService, ReplicationWorkerConfig, Result,
};

const MAX_INITIALIZE_SCAN_BATCH_BYTES: usize = 1024 * 1024;
const FETCH_WAL_TIMEOUT: Duration = Duration::from_secs(30);
const TRACK_WAL_PROGRESS_TIMEOUT: Duration = Duration::from_secs(30);
const UPDATE_STORES_TIMEOUT: Duration = Duration::from_secs(120);

macro_rules! try_force_stop {
    ($self:ident, $expr:expr) => {{
        #[cfg(feature = "testexport")]
        if $self.force_stop.get() {
            info!("replication_worker force stopped");
            return $expr;
        }
    }};
    ($self:ident) => {{
        try_force_stop!($self, ());
    }};
}

macro_rules! try_force_stop_err {
    ($self:ident) => {{
        try_force_stop!($self, Err(Error::ForceStopped));
    }};
}

pub struct ReplicationWorker {
    ctx: MergedEngineContext,
    http_client: Arc<HttpClient>,
    config: ReplicationWorkerConfig,
    merged_engine: MergedEngine,
    grpc_server: Option<grpcio::Server>,
    health_service: Option<HealthService>,
    kube_api: Option<Arc<KubeApi>>,
    runtime: Arc<tokio::runtime::Runtime>,
    gc_worker: Option<worker::Worker>,
    safepoint_mgr: Option<ServiceSafepointManager>,

    keyspaces: HashMap<u32, Keyspace>,
    cdc_addrs: Arc<dashmap::DashMap<u32, String>>,

    conns: HashMap<ConnId, Conn>,

    tx: Sender<CdcMsg>,
    rx: Receiver<CdcMsg>,

    apply_ctx: ApplyContext,

    region_delegates: HashMap<u64 /* region_id */, RegionDelegate>,
    conn_regions: HashMap<ConnId, HashSet<u64 /* region_id */>>,
    region_to_keyspace: HashMap<u64 /* region_id */, u32 /* keyspace_id */>,

    resolved_regions: HashMap<(TimeStamp, RequestKey), Vec<u64 /* region_id */>>,

    last_update_ts: TimeStamp,

    /// `synced_target_ts` used for notify `WalProgressFetcher` to get next
    /// target from rfengine or backup (for the constraint of
    /// `max_wal_target_time_span`).
    ///
    /// `last_update_ts` is not used for the notification because the advance of
    /// `last_update_ts` depends on WAL targets.
    synced_target_ts: ArcTimeStamp,
    wal_progress_targets: WalProgressTargets,
    wal_cache: WalCache,
    update_stores_wal_size_limit: u64,
    incr_scan_concurrency_limit: Arc<Semaphore>,

    working_dir: PathBuf,
    stop: bool,
    force_stop: ForceStop,

    resolved_ts_stats: Option<ResolvedTsStats>,
}

impl Drop for ReplicationWorker {
    fn drop(&mut self) {
        // Call shutdown again for safety (and handle the case of force stop).
        self.shutdown();
    }
}

impl ReplicationWorker {
    pub fn new(
        pd: Arc<dyn PdClient>,
        fs: Arc<dyn Dfs>,
        data_dir: String,
        security: SecurityConfig,
        config: ReplicationWorkerConfig,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Result<Self> {
        let data_dir = PathBuf::from(data_dir);
        let merged_engine_dir = data_dir.join("merged_engine");
        box_try!(fs::create_dir_all(&merged_engine_dir));
        let worker_dir = data_dir.join("rep_worker");
        box_try!(fs::create_dir_all(&worker_dir));
        let master_key = runtime.block_on(security.new_master_key());
        let force_stop = ForceStop::default();
        let ctx = MergedEngineContext {
            pd,
            fs,
            local_dir: merged_engine_dir,
            security_config: Arc::new(security),
            config: config.merged_engine.clone(),
            master_key,
            force_stop: force_stop.clone(),
        };
        let http_client = ctx
            .pd
            .get_security_mgr()
            .http_client(hyper::Client::builder())
            .unwrap();
        let merged_engine = box_try!(MergedEngine::new(ctx.clone(), None));
        let synced_target_ts = merged_engine.get_synced_target_ts();
        let keyspace_ids = merged_engine.get_keyspaces();
        let mut keyspaces = HashMap::with_capacity(keyspace_ids.len());
        let cdc_addrs = Arc::new(dashmap::DashMap::new());
        let kube_api = if config.is_kube_mode() {
            info!("init k8s api");
            let api = runtime.block_on(KubeApi::new(
                config.pd_sts_name.clone(),
                config.cdc_sts_name.clone(),
                config.namespace.clone(),
            ))?;
            Some(Arc::new(api))
        } else {
            None
        };
        for keyspace_id in keyspace_ids {
            let states_bin = merged_engine.get_keyspace_states(keyspace_id).unwrap();
            let states: KeyspaceStates = serde_json::from_slice(&states_bin).unwrap();
            let cdc_addr = states.cdc_addr.clone();
            let mut task_service: Box<dyn KeyspaceService> = if states.is_provisioned() {
                Box::new(KeyspaceProvisionedService::new(
                    keyspace_id,
                    &config,
                    &ctx.security_config,
                    states,
                ))
            } else {
                Box::new(KeyspaceKubeService::new(
                    keyspace_id,
                    kube_api.clone().unwrap(),
                    &config,
                    &ctx.security_config,
                    states,
                ))
            };
            if let Err(err) = runtime.block_on(task_service.start()) {
                error!("keyspace start service error {:?}", err; "keyspace" => keyspace_id);
                continue;
            }
            cdc_addrs.insert(keyspace_id, cdc_addr);
            keyspaces.insert(keyspace_id, Keyspace::from(task_service));
        }
        let gc_runner = GcRunner::new(merged_engine.get_kv(), None, config.local_file_gc_timeout.0);
        let gc_worker = worker::Builder::new("rep-gc-worker").create();
        gc_worker.start_with_timer("rep-gc-worker", gc_runner);
        let interval = config.report_region_interval.0;
        let update_stores_wal_size_limit = config.update_stores_wal_size_limit.as_memory_size();
        let incr_scan_concurrency_limit =
            Arc::new(Semaphore::new(config.incr_scan_concurrency_limit));
        let mut keyspaces_feeds = HashMap::with_capacity(keyspaces.len());
        for (&keyspace_id, ks) in keyspaces.iter_mut() {
            let raft = merged_engine.get_raft();
            let kv = merged_engine.get_kv();
            let rep_pd_cli = ks.get_pd_client();
            let ks_feeds =
                KeyspaceChangefeeds::new(keyspace_id, ks.get_states(), rep_pd_cli.clone());
            keyspaces_feeds.insert(keyspace_id, ks_feeds);
            ks.report_loop = Some(runtime.spawn(async move {
                Self::report_regions_loop(keyspace_id, raft, kv, rep_pd_cli, interval).await;
            }));
        }
        let safepoint_mgr = box_try!(ServiceSafepointManager::new(
            ctx.config.merged_store_id,
            ctx.pd.clone(),
            &config,
            runtime.handle().clone(),
            keyspaces_feeds,
        ));
        let mut apply_ctx =
            ApplyContext::new(merged_engine.get_kv(), Some(merged_engine.get_router()));
        let (tx, rx) = tikv_util::mpsc::unbounded();
        let apply_observer =
            CdcApplyObserver::new(merged_engine.get_kv(), tx.clone(), runtime.handle().clone());
        apply_ctx.set_apply_observer(Box::new(apply_observer));
        let mut worker = Self {
            config,
            ctx,
            http_client: Arc::new(http_client),
            merged_engine,
            runtime: runtime.clone(),
            grpc_server: None,
            health_service: None,
            kube_api,
            gc_worker: Some(gc_worker),
            safepoint_mgr: Some(safepoint_mgr),
            keyspaces,
            cdc_addrs,
            conns: Default::default(),
            tx: tx.clone(),
            rx,
            apply_ctx,
            region_delegates: Default::default(),
            conn_regions: Default::default(),
            region_to_keyspace: Default::default(),
            resolved_regions: Default::default(),
            last_update_ts: TimeStamp::zero(),
            synced_target_ts: ArcTimeStamp::from(synced_target_ts),
            wal_progress_targets: WalProgressTargets::default(),
            wal_cache: WalCache::default(),
            update_stores_wal_size_limit,
            incr_scan_concurrency_limit,
            working_dir: worker_dir,
            stop: false,
            force_stop,
            resolved_ts_stats: Default::default(),
        };
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(2)
                .name_prefix(thd_name!("grpc-server"))
                .build(),
        );
        let channel_args = ChannelBuilder::new(env.clone())
            .stream_initial_window_size(2 * 1024 * 1024)
            .max_concurrent_stream(1024)
            .max_receive_message_len(-1)
            .max_send_message_len(-1)
            .http2_max_ping_strikes(i32::MAX) // For pings without data from clients.
            .keepalive_time(Duration::from_secs(10))
            .keepalive_timeout(Duration::from_secs(3))
            .build_args();
        let addr = SocketAddr::from_str(&worker.config.grpc_addr).unwrap();
        let security_mgr = worker.ctx.pd.get_security_mgr();
        let service = ReplicationService::new(
            worker.merged_engine.get_kv(),
            tx.clone(),
            runtime.handle().clone(),
        );
        let health_service = HealthService::default();
        let sb = ServerBuilder::new(env)
            .channel_args(channel_args)
            .register_service(create_change_data(service.clone()))
            .register_service(create_tikv(service))
            .register_service(create_health(health_service.clone()));
        let sb = security_mgr.bind(sb, &addr.ip().to_string(), addr.port());
        let mut grpc_server = sb.build().unwrap();
        grpc_server.start();
        health_service.set_serving_status("", ServingStatus::Serving);
        worker.grpc_server = Some(grpc_server);
        worker.health_service = Some(health_service);
        Ok(worker)
    }

    // Note: `shutdown` will be called more than once.
    fn shutdown(&mut self) {
        // grpc_server should be dropped before merged_engine.
        let _ = self.grpc_server.take();
        if let Some(health_service) = self.health_service.take() {
            health_service.shutdown();
        }
        if let Some(gc_worker) = self.gc_worker.take() {
            gc_worker.stop()
        }
        if let Some(sp_mgr) = self.safepoint_mgr.take() {
            sp_mgr.shutdown();
        }
        self.shutdown_report_region_loops();
        self.merged_engine.close();
    }

    fn shutdown_report_region_loops(&mut self) {
        let report_loop_handles = self
            .keyspaces
            .values_mut()
            .filter_map(|ks| ks.report_loop.take())
            .collect::<Vec<_>>();
        if !report_loop_handles.is_empty() {
            let mut join_set = tokio::task::JoinSet::new();
            for h in report_loop_handles {
                join_set.spawn_on(
                    async move {
                        h.abort();
                        if let Err(err) = h.await
                            && err.is_panic()
                        {
                            panic!("report region loop panic");
                        }
                    },
                    self.runtime.handle(),
                );
            }
            self.runtime
                .block_on(async { while (join_set.join_next().await).is_some() {} });
        }
    }

    #[inline]
    fn merged_store_id(&self) -> u64 {
        self.ctx.config.merged_store_id
    }

    fn tolerate_store_err(&self) -> usize {
        self.config.tolerate_store_err as usize
    }

    #[inline]
    fn get_region_tag(&self, region_id: u64, region_version: u64) -> ShardTag {
        ShardTag::new(
            self.merged_store_id(),
            IdVer::new(region_id, region_version),
        )
    }

    pub fn scheduler(&self) -> ReplicationScheduler {
        ReplicationScheduler::new(
            self.tx.clone(),
            self.cdc_addrs.clone(),
            self.http_client.clone(),
            self.force_stop.clone(),
        )
    }

    pub fn run(&mut self) {
        let _enter = self.runtime.enter();
        info!("replication worker started");

        WalProgressFetcher::run(
            self.ctx.pd.clone(),
            self.synced_target_ts.clone(),
            TRACK_WAL_PROGRESS_TIMEOUT,
            &self.config,
            self.runtime.handle().clone(),
            self.ctx.fs.clone(),
            self.wal_progress_targets.clone(),
        );

        loop {
            let res = self.rx.recv_timeout(Duration::from_millis(100));
            let has_msg = res.is_ok();
            match res {
                Ok(msg) => {
                    try_force_stop!(self);
                    self.handle_msg(msg);
                    while let Ok(msg) = self.rx.try_recv() {
                        try_force_stop!(self);
                        self.handle_msg(msg);
                    }
                }
                Err(err) => {
                    if err.is_disconnected() {
                        return;
                    }
                }
            }
            if self.stop {
                info!("replication_worker stopped");
                return;
            }
            try_force_stop!(self);
            let should_sync = self.should_sync();
            if has_msg || should_sync {
                // When has message (e.g. `CdcMsg::Applied`), send resolved_ts in time.
                if let Err(err) = self.send_resolved_ts() {
                    error!("send resolved ts error"; "err" => ?err);
                }
            }
            try_force_stop!(self);
            if should_sync {
                if let Err(err) = self.maybe_update_merged_engine() {
                    error!("update merged engine error"; "err" => ?err);
                }

                self.report_metrics();
            }
        }
    }

    fn handle_msg(&mut self, msg: CdcMsg) {
        match msg {
            CdcMsg::AddKeyspace {
                keyspace_id,
                pd_url,
                cdc_addr,
                cb,
            } => {
                self.handle_add_keyspace(keyspace_id, pd_url, cdc_addr, cb);
            }
            CdcMsg::GetKeyspaces { cb } => {
                cb(self.keyspaces.keys().cloned().collect());
            }
            CdcMsg::LoadKeyspaceShards {
                keyspace_id,
                task_service,
                cb,
            } => {
                let res = self.handle_load_keyspace_shards(keyspace_id, task_service);
                cb(res);
            }
            CdcMsg::LoadKeyspaceShardMetas { keyspace_id, cb } => {
                let res = self.handle_load_keyspace_shard_metas(keyspace_id);
                cb(res);
            }
            CdcMsg::RemoveKeyspace {
                keyspace_id,
                force,
                cb,
            } => {
                self.handle_remove_keyspace_service(keyspace_id, force, cb);
            }
            CdcMsg::NewTask {
                keyspace_id,
                changefeed_id,
                start_ts,
                body,
                cb,
            } => {
                self.handle_new_task(keyspace_id, changefeed_id, start_ts, body, cb);
            }
            CdcMsg::OpenConn(conn) => self.handle_open_conn(conn),
            CdcMsg::Register { request, conn_id } => {
                tikv_util::set_current_region(request.region_id);
                let res = self.handle_register(request, conn_id);
                self.handle_result(res, "register");
            }
            CdcMsg::SpawnRegisterHandler {
                request,
                conn_id,
                snap_access,
            } => {
                tikv_util::set_current_region(request.region_id);
                let res = self.handler_spawn_register_handler(request, conn_id, snap_access);
                self.handle_result(res, "spawn_register_handler");
            }
            CdcMsg::ResumeRegister {
                conn_id,
                request_id,
                snap_access,
                init_id,
            } => {
                tikv_util::set_current_region(snap_access.get_id());
                self.handle_resume_register(conn_id, request_id, snap_access, init_id);
            }
            CdcMsg::RegisterResult {
                region_id,
                conn_id,
                request_id,
                init_id,
                err_opt,
            } => {
                tikv_util::set_current_region(region_id);
                let res =
                    self.handle_register_result(region_id, conn_id, request_id, init_id, err_opt);
                if let Err(err) = &res {
                    self.deregister_region_on_error(conn_id, request_id, region_id, err);
                }
                self.handle_result(res.map_err(Into::into), "register_result");
            }
            CdcMsg::SpawnScanLocks { snap_access } => {
                tikv_util::set_current_region(snap_access.get_id());
                let res = self.handle_spawn_scan_locks(snap_access);
                self.handle_result(res, "spawn_scan_locks");
            }
            CdcMsg::ScanLocksResult {
                region_id,
                locks,
                snap_version,
            } => {
                tikv_util::set_current_region(region_id);
                let res = self.handle_scan_locks_result(region_id, locks, snap_version);
                self.handle_result(res, "scan_locks_result");
            }
            CdcMsg::Deregister(deregister) => {
                self.handle_deregister(deregister);
            }
            CdcMsg::Applied {
                region_id,
                region_events,
            } => {
                tikv_util::set_current_region(region_id);
                let sink_err_requests = self.handle_applied(region_id, region_events);
                for (req_key, err) in sink_err_requests {
                    self.deregister_region_on_error(
                        req_key.conn_id,
                        req_key.request_id,
                        region_id,
                        &err,
                    );
                }
            }
            CdcMsg::AppliedAdmin {
                region_id,
                region_version,
                admin,
            } => {
                tikv_util::set_current_region(region_id);
                self.handle_applied_admin(region_id, region_version, admin);
            }
            CdcMsg::RemoveTask {
                keyspace_id,
                changefeed_id,
                cb,
            } => {
                let res = self.handle_remove_task(keyspace_id, changefeed_id);
                cb(res);
            }
            CdcMsg::Stop => {
                self.stop = true;
                self.shutdown();
            }
        }
    }

    fn handle_new_task(
        &mut self,
        keyspace_id: u32,
        changefeed_id: String,
        origin_start_ts: u64,
        origin_body: Bytes,
        cb: Box<dyn FnOnce(Result<(StatusCode, Bytes)>) + Send>,
    ) {
        let tag = format!("{keyspace_id}:new_task");

        let (start_ts, body) = match Self::handle_start_ts(
            &tag,
            origin_start_ts,
            origin_body,
            self.last_update_ts.into_inner(),
        ) {
            Ok(body) => body,
            Err(err) => {
                cb(Err(err));
                return;
            }
        };
        let body_string = String::from_utf8_lossy(&body).to_string();
        info!("handle_new_task"; "keyspace" => keyspace_id, "changefeed" => &changefeed_id, "req" => &body_string);
        #[allow(clippy::map_entry)]
        if !self.keyspaces.contains_key(&keyspace_id) {
            cb(Err(Error::OtherError("keyspace not found".into())));
            return;
        }

        let Some(sp_mgr) = self.safepoint_mgr.as_mut() else {
            cb(Err(Error::OtherError("replication worker stopped".into())));
            return;
        };
        if let Err(err) =
            sp_mgr.ensure_changefeed_start_ts_safety(keyspace_id, &changefeed_id, start_ts)
        {
            cb(Err(err));
            return;
        }

        let merged_store_id = self.merged_store_id();
        let ks = self.keyspaces.get_mut(&keyspace_id).unwrap();
        let pd_client = ks.get_pd_client();
        Self::report_store_to_pd(&pd_client, merged_store_id);
        let keyspace_region_ids = self.merged_engine.get_keyspace_regions(keyspace_id);
        let raft = self.merged_engine.get_raft();
        for region_id in keyspace_region_ids {
            Self::report_region_to_rep_pd_by_id(&raft, &pd_client, region_id);
        }
        let states = ks.get_states_mut();
        let req_body = match states.feeds.entry(changefeed_id.clone()) {
            Entry::Vacant(e) => {
                // TODO: verify the request parameter.
                e.insert(body_string);
                self.safepoint_mgr
                    .as_ref()
                    .unwrap()
                    .add_changefeed(keyspace_id, changefeed_id);
                self.merged_engine
                    .set_keyspace_states(keyspace_id, states.marshal())
                    .unwrap();
                body
            }
            Entry::Occupied(e) => {
                // Consider as a retry request.
                // Use the saved body, to ensure that the request between replication worker &
                // TiCDC are the same.
                // TODO: verify the request parameter not changed.
                Bytes::copy_from_slice(e.get().as_bytes())
            }
        };

        // Still dispatch to TiCDC, as the previous request will failed.
        // TODO: Remove the changefeed in replication worker on error ?
        let sec_mgr = self.ctx.pd.get_security_mgr();
        let client = sec_mgr.http_client(hyper::Client::builder()).unwrap();
        let cdc_addr = states.cdc_addr.clone();
        self.cdc_addrs.insert(keyspace_id, cdc_addr.clone());
        tokio::spawn(async move {
            fn is_err_retryable(err: &TiCdcError) -> bool {
                matches!(err, TiCdcError::ServerIsNotReady(_))
            }
            let new_cdc_task_uri = sec_mgr
                .build_uri(format!("{}/api/v2/changefeeds", &cdc_addr))
                .unwrap();
            let res =
                post_to_ticdc(&tag, &client, &new_cdc_task_uri, req_body, is_err_retryable).await;
            cb(res);
        });
    }

    fn handle_start_ts(
        tag: &str,
        origin_start_ts: u64,
        origin_body: Bytes,
        last_update_ts: u64,
    ) -> Result<(u64 /* new_start_ts */, Bytes /* new_body */)> {
        if last_update_ts == 0 {
            return Err(Error::OtherError("replication worker not ready".into()));
        }
        if origin_start_ts > 0 {
            if origin_start_ts <= last_update_ts {
                Ok((origin_start_ts, origin_body))
            } else {
                error!("{}: start_ts too large", tag; "start_ts" => origin_start_ts, "last_update_ts" => last_update_ts);
                Err(Error::OtherError(
                    format!("start_ts too large (> {})", last_update_ts).into(),
                ))
            }
        } else {
            let Ok(Value::Object(mut js_value)) = serde_json::from_slice(&origin_body) else {
                // The format has been verified in scheduler.
                unreachable!();
            };
            debug!("{}: set start_ts as {}", tag, last_update_ts);
            js_value.insert("start_ts".into(), json!(last_update_ts));
            Ok((
                last_update_ts,
                serde_json::to_vec(&json!(js_value)).unwrap().into(),
            ))
        }
    }

    fn handle_open_conn(&mut self, conn: Conn) {
        let conn_id = conn.get_id();
        self.conns.insert(conn_id, conn);
        self.conn_regions.insert(conn_id, HashSet::default());
    }

    // The process of register:
    // 1. Check region existence and epoch.
    // 2. Flush observer for region.
    // 3. Get snap access.
    // 4. Send message to spawn register handler.
    // 5. In message handler, set state to Initializing.
    // 6. Spawn register handler.
    fn handle_register(&mut self, request: ChangeDataRequest, conn_id: ConnId) -> Result<()> {
        let region_id = request.region_id;
        let Some(shard) = self.merged_engine.get_kv().get_shard(request.region_id) else {
            self.send_region_not_found(conn_id, &request);
            return Ok(());
        };
        let request_ver = request.get_region_epoch().get_version();
        if shard.ver != request_ver {
            self.send_epoch_not_match(conn_id, &request);
            return Ok(());
        }
        let tag = shard.tag();
        info!("{} cdc register", tag; "req" => ?request, "conn" => ?conn_id);

        let (need_scan_locks, need_incremental_scan) = if let Some(delegate) =
            self.region_delegates.get(&region_id)
        {
            if delegate.region_ver != request_ver {
                // Delegate is stale. Return error to let client retry.
                warn!("{} cdc register: delegate is stale", tag; "req" => ?request, "conn" => ?conn_id,
                    "delegate.ver" => delegate.region_ver, "request_ver" => request_ver);
                self.send_server_is_busy(conn_id, &request, "delegate is stale".to_string());
                return Ok(());
            }

            let need_scan_locks = delegate.resolver.is_none();
            let check_duplicated = delegate.requests.check_duplicated(&request, conn_id);
            if let Err(err) = &check_duplicated {
                info!("{} cdc register: ignore duplicated request", tag; "err" => ?err);
            }
            (need_scan_locks, check_duplicated.is_ok())
        } else {
            // Create delegate after `flush_observer_region`.
            // Otherwise, the new delegate would receive stale applied entries.
            (true, true)
        };

        if need_scan_locks || need_incremental_scan {
            // Flush observer for region.
            // Otherwise, the scan locks/incremental scan may get data duplicated with
            // pending locks/events.
            self.apply_ctx.flush_observer_region(region_id);
        }

        let snap_access = shard.new_snap_access();
        if need_scan_locks
            && self
                .tx
                .send(CdcMsg::SpawnScanLocks {
                    snap_access: snap_access.clone(),
                })
                .is_err()
        {
            self.send_server_is_busy(
                conn_id,
                &request,
                "handle_register: spawn scan locks failed".to_string(),
            );
            return Err(box_err!("{} spawn scan locks failed", tag));
        }
        if need_incremental_scan {
            if let Err(SendError(msg)) = self.tx.send(CdcMsg::SpawnRegisterHandler {
                request,
                conn_id,
                snap_access,
            }) {
                let CdcMsg::SpawnRegisterHandler {
                    request, conn_id, ..
                } = msg
                else {
                    unreachable!()
                };
                self.send_server_is_busy(
                    conn_id,
                    &request,
                    "handle_register: spawn handler failed".to_string(),
                );
                return Err(box_err!("{} spawn register handler failed", tag));
            }
        }
        Ok(())
    }

    fn handler_spawn_register_handler(
        &mut self,
        request: ChangeDataRequest,
        conn_id: ConnId,
        snap_access: SnapAccess,
    ) -> Result<()> {
        let tag = snap_access.get_tag();
        let merged_store_id = self.merged_store_id();
        let region_id = request.region_id;

        let Some(conn_regions) = self.conn_regions.get_mut(&conn_id) else {
            info!("{} cdc register: conn is closed, skip", tag;
                "conn" => ?conn_id, "request" => %request.request_id);
            return Ok(());
        };
        conn_regions.insert(region_id);
        let Some(conn) = self.conns.get(&conn_id) else {
            warn!("{} cdc register: conn not found", tag; "conn" => ?conn_id);
            debug_assert!(false);
            return Ok(());
        };
        let sink = conn.get_sink().clone();

        let delegate = self.region_delegates.entry(region_id).or_insert_with(|| {
            RegionDelegate::new(merged_store_id, region_id, snap_access.get_version())
        });
        if delegate.region_ver != request.get_region_epoch().get_version() {
            info!("{} cdc register: delegate version not match, skip", tag;
                "conn" => ?conn_id, "request" => %request.request_id, "delegate.ver" => delegate.region_ver);
            self.send_epoch_not_match(conn_id, &request);
            return Ok(());
        }

        // Check if all transactions with `start_ts <= checkpoint_ts` have been
        // committed.
        let resolved_ts = delegate.resolver.as_ref().and_then(|x| x.resolved_ts());
        let checkpoint_resolved = resolved_ts
            .is_some_and(|resolved_ts| request.checkpoint_ts <= resolved_ts.into_inner());

        let (init_id, init_alive_opt) = match delegate.requests.add(
            &request,
            conn_id,
            checkpoint_resolved,
        ) {
            Ok(init_id) => init_id,
            Err(current_state) => {
                info!("{} cdc register: ignore duplicate request", tag;
                    "conn" => ?conn_id, "request" => %request.request_id, "current" => current_state);
                return Ok(());
            }
        };

        // `init_alive_opt` is `Some` when state is `Initializing`.
        if let Some(init_alive) = init_alive_opt {
            debug_assert!(checkpoint_resolved);
            let mut register_handler = RegisterHandler::new(
                conn_id,
                &request,
                snap_access,
                init_id,
                init_alive,
                self.incr_scan_concurrency_limit.clone(),
                self.tx.clone(),
                sink,
            );
            debug!("{} cdc register: spawn handler", tag; "req" => ?request, "conn" => ?conn_id);
            self.runtime
                .spawn(async move { register_handler.handle_register().await });
        } else {
            info!("{} cdc register: blocked", tag;
                "req" => ?request, "conn" => ?conn_id, "resolved_ts" => ?resolved_ts);
        }
        Ok(())
    }

    fn handle_resume_register(
        &mut self,
        conn_id: ConnId,
        request_id: RequestId,
        snap_access: SnapAccess,
        init_id: InitId,
    ) {
        let tag = snap_access.get_tag();
        let region_id = snap_access.get_id();

        let Some(conn) = self.conns.get(&conn_id) else {
            info!("{} handle_resume_result: conn is closed, skip", tag;
                "conn" => ?conn_id, "request" => %request_id);
            return;
        };
        let sink = conn.get_sink().clone();

        let Some(request_info) =
            self.mut_blocked_request_info(region_id, conn_id, request_id, init_id)
        else {
            info!("{} handle_resume_register: ignore stale resume", tag;
                "conn" => ?conn_id, "request" => %request_id, "init_id" => ?init_id);
            return;
        };

        let (request, init_id_, init_alive) = request_info.state.must_resume();
        debug_assert_eq!(init_id, init_id_);
        debug_assert_eq!(request.region_id, region_id);
        // Version has been checked in `send_resolved_ts`.
        debug_assert_eq!(
            request.get_region_epoch().get_version(),
            snap_access.get_version()
        );

        let mut register_handler = RegisterHandler::new(
            conn_id,
            &request,
            snap_access,
            init_id,
            init_alive,
            self.incr_scan_concurrency_limit.clone(),
            self.tx.clone(),
            sink,
        );
        debug!("{} cdc register: resume register spawn handler", tag; "req" => ?request, "conn" => ?conn_id);
        self.runtime
            .spawn(async move { register_handler.handle_register().await });
    }

    fn handle_register_result(
        &mut self,
        region_id: u64,
        conn_id: ConnId,
        request_id: RequestId,
        init_id: InitId,
        err_opt: Option<cdc::Error>,
    ) -> cdc::Result<()> {
        let tag = self.get_region_tag(region_id, 0);
        let Some(delegate) = self.region_delegates.get_mut(&region_id) else {
            info!("{} handle_register_result: ignore stale result", tag; "conn" => ?conn_id, "request" => %request_id);
            return Ok(());
        };
        let Some(request_info) = delegate.mut_initializing_request(conn_id, request_id, init_id)
        else {
            info!("{} handle_register_result: ignore stale result", tag;
                "conn" => ?conn_id, "request" => %request_id, "init_id" => ?init_id);
            return Ok(());
        };

        if let Some(err) = err_opt {
            return Err(err);
        }

        let Some(conn) = self.conns.get(&conn_id) else {
            warn!("{} handle_register_result: conn not found", tag; "conn" => ?conn_id, "request" => %request_id);
            debug_assert!(false);
            // Unexpected conn not found. Return error to deregister for safe.
            return Err(box_err!("conn not found: {:?}", conn_id));
        };
        let sink = conn.get_sink();
        let (pending_events, events_bytes) = request_info.state.must_finish_initialize();
        let events_count = pending_events.len();
        for pending_event in pending_events {
            trace!(
                "{} handle_register_result: send pending event {:?}",
                tag,
                pending_event
            );
            sink.unbounded_send(CdcEvent::Event(pending_event), false)
                .map_err(|e| cdc::Error::from(e))?;
        }
        info!("{} handle_register_result: initialized", tag;
            "pending_count" => events_count, "pending_bytes" => events_bytes,
            "conn" => ?conn_id, "request" => %request_id);
        Ok(())
    }

    fn handle_spawn_scan_locks(&mut self, snap_access: SnapAccess) -> Result<()> {
        let tag = snap_access.get_tag();
        let merged_store_id = self.merged_store_id();
        let region_id = snap_access.get_id();

        let delegate = self.region_delegates.entry(region_id).or_insert_with(|| {
            RegionDelegate::new(merged_store_id, region_id, snap_access.get_version())
        });
        if delegate.region_ver != snap_access.get_version() {
            // There should be a coming `CdcMsg::AppliedAdmin` which will remove this stale
            // delegate.
            info!("{} handle_spawn_scan_locks: delegate is stale, skip", tag;
                "delegate.ver" => delegate.region_ver);
            return Ok(());
        }

        if delegate.resolver.is_none() {
            debug!("{} cdc register: spawn scan locks", tag);
            delegate.resolver = Some(RegionResolver::new_pending(
                snap_access.get_mem_table_snap_version(),
            ));
            let mut locks_handler = ScanLocksHandler::new(snap_access, self.tx.clone());
            self.runtime.spawn_blocking(move || {
                locks_handler.scan_locks();
            });
        } else {
            info!("{} handle_spawn_scan_locks: duplicated, skip", tag);
        }
        Ok(())
    }

    fn handle_scan_locks_result(
        &mut self,
        region_id: u64,
        locks: Result<Vec<(Vec<u8>, TimeStamp)>>,
        snap_version: SnapVersion,
    ) -> Result<()> {
        let Some(delegate) = self.region_delegates.get_mut(&region_id) else {
            warn!(
                "{} handle_scan_locks_result: region delegate not found",
                region_id
            );
            return Ok(());
        };

        match locks {
            Ok(locks) => {
                delegate.handle_scan_locks(locks, snap_version);
            }
            Err(err) => {
                delegate.handle_scan_locks_error(&err, snap_version, &self.conns);
            }
        }

        Ok(())
    }

    fn send_error_event(&self, conn_id: ConnId, request: &ChangeDataRequest, error: cdcpb::Error) {
        let Some(conn) = self.conns.get(&conn_id) else {
            warn!("send_error_event: conn not found"; "conn" => ?conn_id);
            return;
        };

        let mut event = Event::new();
        event.set_region_id(request.region_id);
        event.set_request_id(request.request_id);
        event.set_error(error);
        if let Err(err) = conn.get_sink().unbounded_send(CdcEvent::Event(event), true) {
            warn!("send_error_event: failed"; "conn" => ?conn_id, "err" => ?err);
        }
    }

    fn send_region_not_found(&self, conn_id: ConnId, request: &ChangeDataRequest) {
        let mut error = cdcpb::Error::new();
        error
            .mut_region_not_found()
            .set_region_id(request.region_id);
        self.send_error_event(conn_id, request, error);
    }

    fn send_epoch_not_match(&self, conn_id: ConnId, request: &ChangeDataRequest) {
        let mut error = cdcpb::Error::new();
        let epoch_not_match = error.mut_epoch_not_match();
        let rep_region =
            Self::get_region_for_rep(&self.merged_engine.get_raft(), request.region_id);
        if let Some(rep_region) = rep_region {
            epoch_not_match.mut_current_regions().push(rep_region);
        }
        self.send_error_event(conn_id, request, error);
    }

    fn send_server_is_busy(&self, conn_id: ConnId, request: &ChangeDataRequest, reason: String) {
        let mut error = cdcpb::Error::new();
        error.mut_server_is_busy().set_reason(reason);
        self.send_error_event(conn_id, request, error);
    }

    fn deregister_region_on_error(
        &mut self,
        conn_id: ConnId,
        request_id: RequestId,
        region_id: u64,
        err: &cdc::Error,
    ) {
        let mut err_event = cdcpb::Error::new();
        match err {
            cdc::Error::Sink(_) => {
                err_event.mut_congested().set_region_id(region_id);
            }
            _ => {
                err_event.mut_server_is_busy().set_reason(err.to_string());
            }
        }
        self.handle_deregister_region(conn_id, request_id, region_id, Some(err_event));
    }

    fn handle_deregister(&mut self, deregister: Deregister) {
        match deregister {
            Deregister::Conn(conn_id) => self.handle_deregister_conn(conn_id),
            Deregister::Request {
                conn_id,
                request_id,
            } => self.handle_deregister_request(conn_id, request_id),
            Deregister::Region {
                conn_id,
                request_id,
                region_id,
            } => {
                tikv_util::set_current_region(region_id);
                self.handle_deregister_region(conn_id, request_id, region_id, None)
            }
        }
    }

    fn handle_deregister_conn(&mut self, conn_id: ConnId) {
        info!("deregister conn"; "conn" => ?conn_id);
        self.conns.remove(&conn_id);
        if let Some(conn_regions) = self.conn_regions.remove(&conn_id) {
            for region_id in conn_regions {
                if let Some(delegate) = self.region_delegates.get_mut(&region_id) {
                    delegate.requests.retain(|k, _| k.conn_id != conn_id);
                    if delegate.requests.is_empty() {
                        self.remove_region(region_id);
                    }
                }
            }
        }
    }

    fn handle_deregister_request(&mut self, conn_id: ConnId, request_id: RequestId) {
        info!("deregister request"; "conn" => ?conn_id, "request" => %request_id);
        let mut remove_regions = vec![];
        if let Some(conn_regions) = self.conn_regions.get_mut(&conn_id) {
            let sink = self.conns.get(&conn_id).map(|c| c.get_sink());
            for &region_id in conn_regions.iter() {
                if let Some(delegate) = self.region_delegates.get_mut(&region_id) {
                    delegate.unsubscribe(conn_id, request_id, sink, None);
                    if delegate.requests.is_empty() {
                        // To work around mutable borrow limitation.
                        remove_regions.push(region_id);
                    }
                }
            }
        }
        for region_id in remove_regions {
            self.remove_region(region_id);
        }
    }

    fn handle_deregister_region(
        &mut self,
        conn_id: ConnId,
        request_id: RequestId,
        region_id: u64,
        err_event: Option<cdcpb::Error>,
    ) {
        info!("deregister request"; "conn" => ?conn_id, "request" => %request_id, "region" => region_id);
        if let Some(delegate) = self.region_delegates.get_mut(&region_id) {
            let sink = self.conns.get(&conn_id).map(|c| c.get_sink());
            delegate.unsubscribe(conn_id, request_id, sink, err_event);
            if delegate.requests.is_empty() {
                self.remove_region(region_id);
            }
        }
    }

    fn handle_result(&mut self, res: Result<()>, tag: &str) {
        if let Err(err) = res {
            error!("handle {} error {:?}", tag, err);
        }
    }

    fn get_keyspace_id(&mut self, region_id: u64) -> Option<u32> {
        Some(match self.region_to_keyspace.entry(region_id) {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => {
                let keyspace_id = self
                    .merged_engine
                    .get_region_progress(region_id)?
                    .keyspace_id;
                *e.insert(keyspace_id)
            }
        })
    }

    fn send_resolved_ts(&mut self) -> Result<()> {
        if self.last_update_ts.is_zero() {
            debug!("send_resolved_ts: not initialized yet");
            return Ok(());
        }

        let merged_store_id = self.merged_store_id();
        info!("send_resolved_ts"; "last_update_ts" => self.last_update_ts, "store" => merged_store_id);

        if self.region_delegates.is_empty() {
            self.resolved_ts_stats = Some(ResolvedTsStats {
                min_ts: self.last_update_ts,
                ..Default::default()
            });
            return Ok(());
        }

        self.resolved_regions.clear();
        let mut stats = ResolvedTsStats::default();
        for (&region_id, delegate) in &mut self.region_delegates {
            try_force_stop_err!(self);
            tikv_util::set_current_region(region_id);
            let tag = ShardTag::new(merged_store_id, IdVer::new(region_id, 0));

            let Some(region_progress) = self.merged_engine.mut_region_progress(region_id) else {
                // The region is newly inserted but not start to sync yet.
                warn!(
                    "{} send_resolved_ts: region not in merged_engine, skip",
                    tag
                );
                stats.record_unresolved_region(region_id);
                continue;
            };
            if !region_progress.is_synced() {
                info!("{} send_resolved_ts: region is not synced, skip", tag;
                    "progress" => ?region_progress);
                if let Some(ts) = delegate.resolved_ts() {
                    stats.record_resolved_region(region_id, ts);
                } else {
                    stats.record_unresolved_region(region_id);
                }
                continue;
            }

            let safe_target_ts = region_progress.update_safe_target_ts(tag, self.last_update_ts);
            let Some(ts) = delegate
                .resolver
                .as_mut()
                .and_then(|r| r.resolve(self.last_update_ts.min(safe_target_ts)))
            else {
                stats.record_unresolved_region(region_id);
                continue;
            };
            stats.record_resolved_region(region_id, ts);

            debug!("{} send_resolved_ts: {}", tag, ts);
            let mut resume_reqs = vec![];
            for (req_key, req_info) in delegate.requests.iter_mut() {
                if req_info.resolved_ts != ts {
                    if req_info.state.is_initialized() {
                        debug!("{} send_resolved_ts: advance request resolved_ts: {} -> {}",
                            tag, req_info.resolved_ts, ts; "req" => ?req_key);
                        debug_assert!(req_info.resolved_ts < ts);
                        self.resolved_regions
                            .entry((ts, *req_key))
                            .or_default()
                            .push(region_id);
                        req_info.resolved_ts = ts;
                    } else if let Some(init_id) = req_info.state.can_resume_by_resolved_ts(ts) {
                        resume_reqs.push((*req_key, init_id));
                    }
                }
            }

            // Resume blocked requests.
            if !resume_reqs.is_empty() {
                let kv = self.merged_engine.get_kv();
                // If region not found / version not match, the delegate will be removed later.
                if let Ok(shard) = kv.get_shard_with_ver(region_id, delegate.region_ver) {
                    // Flush pending events in observer. Ref `handle_register`.
                    self.apply_ctx.flush_observer_region(region_id);

                    let snap_access = shard.new_snap_access();
                    for (req_key, init_id) in resume_reqs {
                        let conn_id = req_key.conn_id;
                        let request_id = req_key.request_id;
                        if self
                            .tx
                            .send(CdcMsg::ResumeRegister {
                                conn_id,
                                request_id,
                                snap_access: snap_access.clone(),
                                init_id,
                            })
                            .is_err()
                        {
                            // Just warning. The resume will be triggered again in next
                            // `send_resolved_ts`.
                            warn!("{} send_resolved_ts: send resume_register message failed", snap_access.get_tag();
                                "conn" => ?conn_id, "request" => %request_id);
                        }
                    }
                };
            }
        }

        // TODO: Send small ts with small batch first.
        // Ref: https://github.com/tikv/tikv/blob/release-7.5/components/cdc/src/endpoint.rs, on_min_ts.
        for ((ts, req_key), regions) in self.resolved_regions.drain() {
            try_force_stop_err!(self);
            let Some(conn) = self.conns.get(&req_key.conn_id) else {
                warn!("send_resolved_ts: conn not found"; "conn_id" => ?req_key.conn_id);
                continue;
            };

            let mut resolved_ts = ResolvedTs::default();
            resolved_ts.set_regions(regions);
            resolved_ts.set_request_id(req_key.request_id.into_inner());
            resolved_ts.set_ts(ts.into_inner());
            trace!("send_resolved_ts: msg: {:?}", resolved_ts; "conn" => ?req_key.conn_id);
            if let Err(err) = conn
                .get_sink()
                .unbounded_send(CdcEvent::ResolvedTs(resolved_ts), false)
            {
                // It's OK to drop resolved_ts event.
                warn!("send_resolved_ts: send failed: {:?}", err;
                    "conn" => ?req_key.conn_id, "request" => %req_key.request_id);
            }
        }

        self.resolved_ts_stats = Some(stats);
        Ok(())
    }

    fn maybe_update_merged_engine(&mut self) -> Result<()> {
        let Some((target_ts, target_progresses)) = self.wal_progress_targets.front() else {
            debug!("maybe_update_merged_engine: no new target"; "store" => self.merged_store_id());
            return Ok(());
        };
        let update_stores_res =
            self.update_stores_with_retry(UPDATE_STORES_TIMEOUT, target_progresses)?;
        debug!("maybe_update_merged_engine: update_stores: {:?}", update_stores_res; "store" => self.merged_store_id());

        // Handle errors on finished for easy.
        // Errors happen during not finished will be retried in next loop.
        let synced_target_ts = match update_stores_res {
            UpdateWalResult::Finished { errors, .. }
                if errors.len() <= self.tolerate_store_err() =>
            {
                for err in errors {
                    if let Some(target) = err.target {
                        let store_id_str = target.store_id.to_string();
                        REP_UPDATE_STORE_COUNTER
                            .with_label_values(&[&store_id_str, "on_error_advance_target"])
                            .inc();
                        warn!("maybe_update_merged_engine: update_stores finished with error, advance target: {:?}", target;
                                "err" => ?err.err, "store" => self.merged_store_id());
                        self.merged_engine.update_store_progress(
                            target.store_id,
                            target.epoch,
                            target.offset,
                        );
                    }
                }
                Some(target_ts)
            }
            UpdateWalResult::Finished { errors, .. } => {
                if errors
                    .iter()
                    .any(|err| matches!(err.err, Error::StoreUnhealthy { .. }))
                {
                    let target = self.wal_progress_targets.pop_front();
                    debug_assert!(target.is_some_and(|(ts, _)| ts == target_ts));
                }
                return Err(Error::UpdateStores(
                    errors.into_iter().map(|e| e.err).collect(),
                ));
            }
            UpdateWalResult::NotFinished { wal_size } => {
                info!("maybe_update_merged_engine: update_stores not finished";
                    "store" => self.merged_store_id(), "wal_size" => wal_size,
                    "last_update_ts" => self.last_update_ts, "target_ts" => target_ts);
                None
            }
        };

        self.merged_engine
            .sync_merged(&mut self.apply_ctx, synced_target_ts)?;
        self.apply_ctx.flush_observer();

        if let Some(synced_target_ts) = synced_target_ts {
            let target = self.wal_progress_targets.pop_front();
            debug_assert!(target.is_some_and(|(ts, _)| ts == synced_target_ts));
            self.last_update_ts = synced_target_ts;
            self.synced_target_ts.set(synced_target_ts);
        }
        Ok(())
    }

    fn should_sync(&self) -> bool {
        TimeStamp::physical_now().saturating_sub(self.last_update_ts.physical())
            >= self.config.sync_interval.as_millis()
    }

    async fn report_regions_loop(
        keyspace_id: u32,
        raft: RfEngine,
        kv: Engine,
        rep_pd_cli: Arc<dyn PdClient>,
        interval: Duration,
    ) {
        let merged_store_id = raft.get_engine_id();
        let handle_heartbeat_resp =
            Self::handle_heartbeat_response(merged_store_id, keyspace_id, rep_pd_cli.clone());
        tikv_util::defer!({
            handle_heartbeat_resp.abort();
        });

        // run a loop to report regions in case that the rep pd is restarted and lost
        // the region leader.
        loop {
            let start = Instant::now();
            let keyspace_shards = kv.get_keyspace_shards(keyspace_id).unwrap_or_default();
            if keyspace_shards.is_empty() {
                // keyspace shards not found means the keyspace is removed.
                return;
            }
            for region_id in keyspace_shards.iter().map(|r| *r) {
                let Some(mut region_local_state) =
                    rfstore::store::load_last_peer_state(&raft, region_id)
                else {
                    // The region may have been merged.
                    continue;
                };
                let tag = ShardTag::from_region(None, region_local_state.get_region());
                if peer_is_skippable(&region_local_state) {
                    debug!("{} report_region: skip", tag);
                    continue;
                }

                let mut region = region_local_state.take_region();
                region.set_start_key(Self::trim_keyspace_prefix(region.get_start_key()));
                region.set_end_key(Self::trim_keyspace_prefix(region.get_end_key()));
                debug!("{} report_region", tag; "region" => ?region);

                let leader = region.get_peers()[0].clone();
                let mut stats = RegionStat::default();
                stats.approximate_kv_size = 100 * 1024 * 1024;
                stats.approximate_keys = 1000000;
                stats.approximate_size = 100 * 1024 * 1024;
                let res = rep_pd_cli
                    .region_heartbeat(1, region, leader, stats, None)
                    .await;
                if let Err(err) = res {
                    if kv.get_keyspace_shards(keyspace_id).is_none() {
                        // The keyspace has been removed.
                        return;
                    }
                    warn!("{} report_region failed: {:?}", tag, err);
                    continue;
                }
            }
            let elapsed = start.saturating_elapsed();
            tokio::time::sleep(interval.saturating_sub(elapsed)).await;
        }
    }

    fn handle_heartbeat_response(
        merged_store_id: u64,
        keyspace_id: u32,
        rep_pd_cli: Arc<dyn PdClient>,
    ) -> tokio::task::JoinHandle<()> {
        let fut = rep_pd_cli.handle_region_heartbeat_response(
            merged_store_id,
            Box::new(move |resp: pdpb::RegionHeartbeatResponse| {
                if let Err(err) = check_resp_header(resp.get_header()) {
                    warn!("region heartbeat response error: {:?}", err;
                        "store" => merged_store_id, "keyspace" => keyspace_id);
                } else {
                    debug!("region heartbeat response: {:?}", resp;
                        "store" => merged_store_id, "keyspace" => keyspace_id);
                }
            }),
        );
        let f = async move {
            match fut.await {
                Ok(_) => {
                    info!("region heartbeat response handler exit";
                        "store" => merged_store_id, "keyspace" => keyspace_id);
                }
                Err(e) => panic!("unexpected error: {:?}", e),
            }
        };
        tokio::spawn(f)
    }

    fn update_stores_with_retry(
        &mut self,
        timeout: Duration,
        targets: StoreWalProgresses,
    ) -> Result<UpdateWalResult> {
        let mut last_err: Option<Error> = None;
        let start_time = Instant::now_coarse();
        while start_time.saturating_elapsed() < timeout {
            try_force_stop_err!(self);
            match self.update_stores(&targets) {
                Ok(x) => {
                    REP_UPDATE_STORE_COUNTER
                        .with_label_values(&["all", x.metric_label()])
                        .inc();
                    REP_UPDATE_STORE_DURATION
                        .with_label_values(&["all"])
                        .observe(start_time.saturating_elapsed().as_secs_f64());

                    return Ok(x);
                }
                Err(err) => {
                    last_err = Some(err);
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        }
        REP_UPDATE_STORE_COUNTER
            .with_label_values(&["all", "error"])
            .inc();
        Err(last_err.unwrap())
    }

    fn update_stores(&mut self, targets: &StoreWalProgresses) -> Result<UpdateWalResult> {
        let _enter = self.runtime.enter();
        let stores = get_all_stores_except_tiflash(self.ctx.pd.as_ref())?;
        let wal_size_limit = self.update_stores_wal_size_limit / targets.len() as u64;
        let mut total_wal_size = 0;
        let mut finished = true;
        let mut errors = vec![];
        let targets_and_lags = self.get_decreasing_stores_lag(targets);
        for (store_id, target_and_lag) in targets_and_lags {
            debug!("update_store_wal"; "store" => store_id, "lag" => ?target_and_lag);
            let target = target_and_lag.target;
            let Some(target) = target else {
                errors.push(UpdateWalError {
                    err: Error::StoreUnhealthy { store_id },
                    target: None,
                });
                continue;
            };
            let store_id_str = format!("{store_id}");
            REP_UPDATE_STORE_EPOCH_LAG
                .with_label_values(&[&store_id_str])
                .set(target_and_lag.epoch_lag as i64);

            let store = stores.iter().find(|s| s.id == store_id);
            let start_time = Instant::now_coarse();
            match self.update_store_wal(store_id, store, target, wal_size_limit) {
                Ok(x) => {
                    REP_UPDATE_STORE_COUNTER
                        .with_label_values(&[&store_id_str, x.metric_label()])
                        .inc();
                    match x {
                        UpdateWalResult::Finished { wal_size, .. } => total_wal_size += wal_size,
                        UpdateWalResult::NotFinished { wal_size } => {
                            total_wal_size += wal_size;
                            finished = false;
                        }
                    }
                }
                Err(err @ Error::BrError(BrError::WalChunkIntegrityError(_))) => {
                    REP_UPDATE_STORE_COUNTER
                        .with_label_values(&[&store_id_str, "wal_integrity_error"])
                        .inc();
                    warn!("update_store_wal: failed: {:?}", err; "store" => store_id);
                    errors.push(UpdateWalError {
                        err,
                        target: Some(target),
                    });
                }
                Err(err) => {
                    REP_UPDATE_STORE_COUNTER
                        .with_label_values(&[&store_id_str, "error"])
                        .inc();
                    warn!("update_store_wal: failed: {:?}", err; "store" => store_id);
                    errors.push(UpdateWalError { err, target: None });
                }
            }
            REP_UPDATE_STORE_DURATION
                .with_label_values(&[&store_id_str])
                .observe(start_time.saturating_elapsed().as_secs_f64());

            if total_wal_size >= self.update_stores_wal_size_limit {
                finished = false;
                break;
            }
        }
        let res = if finished {
            UpdateWalResult::Finished {
                wal_size: total_wal_size,
                errors,
            }
        } else {
            // The `errors` are dropped.
            // As the progress of stores with errors are not advanced,
            // they will be retried in next round.
            if !errors.is_empty() {
                info!("update_stores: not finished with errors"; "errors" => ?errors);
            }
            UpdateWalResult::NotFinished {
                wal_size: total_wal_size,
            }
        };
        Ok(res)
    }

    fn get_decreasing_stores_lag(
        &mut self,
        targets: &StoreWalProgresses,
    ) -> Vec<(
        u64, // store_id
        StoreTargetAndLag,
    )> {
        let mut lags = vec![];
        for (store_id, target) in targets {
            let target_and_lag = if let Some(target) = target {
                let current = self.merged_engine.get_or_insert_store_progress(*store_id);
                let epoch_lag = target.epoch.saturating_sub(current.epoch);
                let offset_lag = target.offset as i64 - current.offset as i64;
                StoreTargetAndLag {
                    target: Some(*target),
                    epoch_lag,
                    offset_lag,
                }
            } else {
                StoreTargetAndLag {
                    target: None,
                    epoch_lag: 0,
                    offset_lag: 0,
                }
            };
            lags.push((*store_id, target_and_lag))
        }
        lags.sort_by(|m, n| n.1.compare(&m.1)); // Decreasing sort.
        lags
    }

    fn update_store_wal(
        &mut self,
        store_id: u64,
        store: Option<&metapb::Store>,
        target: StoreProgress,
        wal_size_limit: u64,
    ) -> Result<UpdateWalResult> {
        let store_progress = self.merged_engine.get_or_insert_store_progress(store_id);
        info!("update_store_wal";
            "store" => store_id, "current" => %store_progress, "target" => %target);
        if store_progress >= target {
            debug!("update_store_wal: store is up-to-date"; "store" => store_id);
            return Ok(UpdateWalResult::Finished {
                wal_size: 0,
                errors: vec![],
            });
        }

        let get_end_off = |epoch: u32| {
            debug_assert!(epoch <= target.epoch);
            if epoch < target.epoch {
                0 // Means read to end.
            } else {
                target.offset
            }
        };
        let next_epoch_offset = |epoch: u32,
                                 end_off: u64,
                                 rotated: bool|
         -> (u32 /* next_epoch */, u64 /* next_start_off */) {
            if rotated {
                (epoch + 1, 0)
            } else {
                (epoch, end_off)
            }
        };

        let security_mgr = self.ctx.pd.get_security_mgr();
        let mut total_wal_size = 0;
        let mut epoch = store_progress.epoch;
        let mut start_off = store_progress.offset;
        while (epoch, start_off) < (target.epoch, target.offset) {
            try_force_stop_err!(self);

            if total_wal_size >= wal_size_limit {
                return Ok(UpdateWalResult::NotFinished {
                    wal_size: total_wal_size,
                });
            }

            let end_off = get_end_off(epoch);
            debug!("update_store_wal"; "store" => store_id, "epoch" => epoch,
                "start" => start_off, "end" => end_off);

            if store.is_none()
                || epoch <= near_overwritten_epoch(target.epoch, MIN_EPOCH_ROTATE_LEN)
            {
                let (rotated, wal_size) =
                    self.update_store_wal_from_s3(store_id, epoch, start_off, target)?;
                total_wal_size += wal_size;
                (epoch, start_off) = next_epoch_offset(epoch, end_off, rotated);
                continue;
            }

            let store = store.unwrap();
            let uri = security_mgr
                .build_uri(format!(
                    "{}/rfengine/wal_chunk?epoch_id={}&start_off={}&end_off={}",
                    &store.status_address, epoch, start_off, end_off
                ))
                .unwrap();
            let req = http::Request::get(uri.clone())
                .body(hyper::Body::empty())
                .unwrap();
            let http_client = self.http_client.clone();
            let (status, data) =
                block_on(send_request_to_store(req, &http_client, FETCH_WAL_TIMEOUT))?;
            if status == StatusCode::GONE {
                let (rotated, wal_size) =
                    self.update_store_wal_from_s3(store_id, epoch, start_off, target)?;
                total_wal_size += wal_size;
                (epoch, start_off) = next_epoch_offset(epoch, end_off, rotated);
                continue;
            }
            if !status.is_success() {
                let err_str = String::from_utf8_lossy(&data);
                return Err(box_err!("{}", err_str));
            }
            let end_off = start_off + data.len() as u64;
            total_wal_size += data.len() as u64;
            self.merged_engine
                .update_wal(store_id, epoch, start_off, end_off, data.reader())?;
            let rotate = if epoch < target.epoch {
                debug_assert_eq!(status, StatusCode::PARTIAL_CONTENT);
                true
            } else {
                // OK: means there is no more data for this epoch.
                status == StatusCode::OK
            };
            if rotate {
                self.merged_engine.rotate_wal(store_id, epoch, end_off)?;
            }
            (epoch, start_off) = next_epoch_offset(epoch, end_off, rotate);
        }
        Ok(UpdateWalResult::Finished {
            wal_size: total_wal_size,
            errors: vec![],
        })
    }

    fn update_store_wal_from_s3(
        &mut self,
        store_id: u64,
        epoch_id: u32,
        start_off: u64,
        target: StoreProgress,
    ) -> Result<(bool /* rotated */, u64 /* wal_size */)> {
        info!("update_store_wal_from_s3"; "store" => store_id,
            "epoch" => epoch_id, "start" => start_off, "target" => %target);
        let wal_data = match self.wal_cache.get_mut(store_id, epoch_id) {
            Some(wal_data) => wal_data,
            None => {
                let wal_data = self.fetch_store_wal_complete_epoch_from_s3(store_id, epoch_id)?;
                self.wal_cache.insert(store_id, epoch_id, wal_data);
                self.wal_cache.get_mut(store_id, epoch_id).unwrap()
            }
        };
        debug_assert!(wal_data.must_get_local_chunks().has_last_chunk());
        let end_off = if epoch_id == target.epoch {
            target.offset
        } else {
            wal_data.len()
        };
        let wal_size = end_off - start_off;
        let reader = box_try!(wal_data.range_reader(start_off, end_off));
        self.merged_engine
            .update_wal(store_id, epoch_id, start_off, end_off, reader)?;
        let rotate = end_off == wal_data.len(); // `wal_data` must be a complete epoch.
        if rotate {
            self.wal_cache.remove_cache(store_id);
            self.merged_engine.rotate_wal(store_id, epoch_id, end_off)?;
        }
        Ok((rotate, wal_size))
    }

    // Fetch the complete WAL of the epoch.
    fn fetch_store_wal_complete_epoch_from_s3(
        &mut self,
        store_id: u64,
        epoch_id: u32,
    ) -> Result<AssembledWalData> {
        info!("fetch_store_wal_from_s3"; "store" => store_id, "epoch" => epoch_id);
        let cache_dir = self.store_working_dir(store_id).join("cache");
        if !self.wal_cache.contains_store(store_id) {
            box_try!(fs::create_dir_all(&cache_dir));
        }

        // Note: No retry for WAL chunk integrity error. Replication worker handle this
        // error in other way.
        let collect_ctx = CollectWalChunksContext {
            pd_client: self.ctx.pd.clone(),
            dfs: self.ctx.fs.clone(),
            store_id,
            complete_wal_chunks: true,
            fetch_wal_timeout: FETCH_WAL_TIMEOUT,
            cache_dir: Some(cache_dir),
            wal_chunks_cache: None,
            retry_for_wal_chunk_integrity_error: false,
        };
        let tag = format!("{}:{}", store_id, epoch_id);
        // there is no online chunk for this epoch.
        let (chunks, online_chunk, has_last_chunk) =
            collect_wal_chunks_with_retry(&tag, &collect_ctx, epoch_id, epoch_id, u64::MAX)?;
        debug_assert!(online_chunk.is_none());
        debug_assert!(has_last_chunk);

        // Assemble WAL chunks.
        let mut epoch_wal = assemble_wal_chunks(chunks, false)?;
        epoch_wal.freeze();
        Ok(epoch_wal)
    }

    fn report_store_to_pd(pd_client: &Arc<dyn PdClient>, store_id: u64) {
        let mut store_stat = StoreStats::new();
        store_stat.set_store_id(store_id);
        store_stat.set_region_count(0);
        store_stat.set_capacity(100 * 1024 * 1024 * 1024);
        store_stat.set_used_size(50 * 1024 * 1024 * 1024);
        let resp = pd_client.store_heartbeat(store_stat, None, None);

        let new_pd_clinet = pd_client.clone();
        tokio::spawn(async move {
            if let Err(err) = resp.await {
                warn!("store heartbeat failed"; "err" => ?err);
                tokio::time::sleep(Duration::from_secs(10)).await;
                Self::report_store_to_pd(&new_pd_clinet, store_id);
            }
        });
    }

    fn report_region_to_rep_pd_by_id(
        raft: &RfEngine,
        rep_pd_cli: &Arc<dyn PdClient>,
        region_id: u64,
    ) {
        let Some(rep_region) = Self::get_region_for_rep(raft, region_id) else {
            return;
        };
        Self::report_region_to_rep_pd(rep_pd_cli, rep_region);
    }

    fn report_region_to_rep_pd(rep_pd_cli: &Arc<dyn PdClient>, region: metapb::Region) {
        let tag = ShardTag::from_region(None, &region);
        info!("{} report_region_to_rep_pd: {:?}", tag, region);
        let leader = region.get_peers()[0].clone();
        let mut stats = RegionStat::default();
        stats.approximate_kv_size = 100 * 1024 * 1024;
        stats.approximate_keys = 1000000;
        stats.approximate_size = 100 * 1024 * 1024;
        let resp = rep_pd_cli.region_heartbeat(1, region, leader, stats, None);
        tokio::spawn(async move {
            if let Err(err) = resp.await {
                warn!("{} report_region_to_rep_pd failed", tag; "err" => ?err);
            }
        });
    }

    // The keyspace prefix will be trimmed for `rep-pd` & `rep-cdc`.
    fn get_region_for_rep(raft: &RfEngine, region_id: u64) -> Option<metapb::Region> {
        if raft.get_truncated_index(region_id) == Some(TRUNCATE_ALL_INDEX) {
            // region has been merged.
            return None;
        }

        let mut region_local_state = rfstore::store::load_last_peer_state(raft, region_id)?;
        let mut region = region_local_state.take_region();
        region.set_start_key(Self::trim_keyspace_prefix(region.get_start_key()));
        region.set_end_key(Self::trim_keyspace_prefix(region.get_end_key()));
        Some(region)
    }

    fn trim_keyspace_prefix(mut region_key: &[u8]) -> Vec<u8> {
        let raw_key = codec::bytes::decode_bytes(&mut region_key, false).unwrap();
        if raw_key.len() == 4 {
            vec![]
        } else {
            codec::bytes::encode_bytes(&raw_key[4..])
        }
    }

    fn handle_add_keyspace(
        &self,
        keyspace_id: u32,
        pd_url: String,
        cdc_addr: String,
        cb: Box<dyn FnOnce(Result<()>) + Send>,
    ) {
        if self.keyspaces.contains_key(&keyspace_id) {
            // Accept duplicated add_keyspace request.
            info!("handle_add_keyspace: keyspace already exists"; "keyspace" => keyspace_id);
            cb(Ok(()));
            return;
        }
        let mut task_service: Box<dyn KeyspaceService> = if self.config.is_kube_mode() {
            let kube_api = self.kube_api.clone().unwrap();
            Box::new(KeyspaceKubeService::new(
                keyspace_id,
                kube_api,
                &self.config,
                &self.ctx.security_config,
                KeyspaceStates::default(),
            ))
        } else if pd_url.is_empty() || cdc_addr.is_empty() {
            cb(Err(Error::OtherError("pd_url or cdc_addr is empty".into())));
            return;
        } else {
            let mut states = KeyspaceStates::default();
            states.pd_url = pd_url;
            states.cdc_addr = cdc_addr;
            Box::new(KeyspaceProvisionedService::new(
                keyspace_id,
                &self.config,
                &self.ctx.security_config,
                states,
            ))
        };
        let runtime = self.runtime.handle().clone();
        let scheduler = self.scheduler();
        let kv = self.merged_engine.kv.clone();
        // Pause meta pack compaction during prepare shards. Otherwise, the prepared
        // files may be compacted before load shards into kvengine.
        let meta_pack_compact_keeper = self.merged_engine.get_meta_pack_compact_keeper();
        let http_client = self.http_client.clone();
        tokio::spawn(tikv_util::init_task_local(async move {
            let start_time = Instant::now_coarse();
            // Start service.
            let res = task_service.start().await;
            if res.is_err() {
                cb(res);
                return;
            }
            let cdc_addr = task_service.get_states().cdc_addr.clone();
            let res = get_cdc_status(&http_client, &cdc_addr, DISPATCH_CDC_TIMEOUT).await;
            if let Err(e) = res {
                cb(Err(box_err!("cdc_status failed: {:?}", e)));
                return;
            }

            // Prepare shards.
            // Note that the rfengine will update at the same time. So during load shards,
            // we will prepare again.
            let prepare_time = Instant::now_coarse();
            let compact_guard: Option<tiny_meta::CompactPauseGuard> =
                meta_pack_compact_keeper.map(|x| x.pause());
            {
                let scheduler = scheduler.clone();
                let kv = kv.clone();
                let prepare_res = runtime
                    .spawn_blocking(move || {
                        let mut metas = HashMap::default();
                        Self::prepare_keyspace_shard_metas(
                            keyspace_id,
                            &scheduler,
                            &kv,
                            &mut metas,
                        )?;
                        // Prepare again in case some shards are changed during last prepare.
                        Self::prepare_keyspace_shard_metas(keyspace_id, &scheduler, &kv, &mut metas)
                    })
                    .await;
                match prepare_res {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        cb(Err(box_err!("prepare keyspace metas failed: {:?}", e)));
                        return;
                    }
                    Err(e) => {
                        if e.is_panic() {
                            panic!("prepare_keyspace_shard_metas panicked");
                        }
                        cb(Err(box_err!("prepare keyspace metas failed: {:?}", e)));
                        return;
                    }
                }
            }

            // Load shards.
            let load_shards_time = Instant::now_coarse();
            let cb_with_log = move |res: Result<()>| {
                drop(compact_guard);

                let end_time = Instant::now_coarse();
                info!("add_keyspace: {:?}", &res;
                    "keyspace" => keyspace_id,
                    "takes" => ?end_time.saturating_duration_since(start_time),
                    "start_svc" => ?prepare_time.saturating_duration_since(start_time),
                    "prepare" => ?load_shards_time.saturating_duration_since(prepare_time),
                    "load_shards" => ?end_time.saturating_duration_since(load_shards_time),
                );
                cb(res);
            };
            scheduler.schedule(CdcMsg::LoadKeyspaceShards {
                keyspace_id,
                task_service,
                cb: Box::new(cb_with_log),
            });
        }));
    }

    fn upsert_keyspace_shard_metas(
        keyspace_id: u32,
        scheduler: &ReplicationScheduler,
        metas: &mut HashMap<u64 /* region_id */, ShardMeta>,
    ) -> Result<()> {
        let (cb, fut) = tikv_util::mpsc::paired_callback();
        scheduler.schedule(CdcMsg::LoadKeyspaceShardMetas { keyspace_id, cb });
        let new_metas = fut
            .recv()
            .map_err(|_| -> Error { box_err!("load keyspace metas canceled") })?
            .map_err(|e| -> Error { box_err!("load keyspace metas failed: {:?}", e) })?;

        for (region_id, meta) in new_metas {
            match metas.entry(region_id) {
                Entry::Vacant(e) => {
                    e.insert(meta);
                }
                Entry::Occupied(mut e) => {
                    let exist_meta = e.get_mut();
                    if exist_meta.seq < meta.seq {
                        *exist_meta = meta;
                    } else {
                        debug_assert_eq!(exist_meta.seq, meta.seq);
                        e.remove();
                    }
                }
            }
        }

        Ok(())
    }

    fn prepare_keyspace_shard_metas(
        keyspace_id: u32,
        scheduler: &ReplicationScheduler,
        kv: &Engine,
        metas: &mut HashMap<u64 /* region_id */, ShardMeta>,
    ) -> Result<()> {
        let start_time = Instant::now_coarse();
        Self::upsert_keyspace_shard_metas(keyspace_id, scheduler, metas)?;

        let prepare_time = Instant::now_coarse();
        if !metas.is_empty() {
            kv.prepare_shards(metas)
                .map_err(|e| -> Error { box_err!("prepare keyspace metas failed: {:?}", e) })?;
        }

        let end_time = Instant::now_coarse();
        info!("prepare_keyspace_shard_metas";
            "keyspace" => keyspace_id,
            "count" => metas.len(),
            "load" => ?prepare_time.saturating_duration_since(start_time),
            "prepare" => ?end_time.saturating_duration_since(prepare_time),
        );
        Ok(())
    }

    fn handle_load_keyspace_shards(
        &mut self,
        keyspace_id: u32,
        task_service: Box<dyn KeyspaceService>,
    ) -> Result<()> {
        let mut states = HashMap::default();
        states.insert(keyspace_id, Bytes::new());
        self.merged_engine.load_shards(&states)?;

        let states = task_service.get_states().marshal();
        let rep_pd_cli = task_service.get_pd_client();
        let raft = self.merged_engine.get_raft();
        let kv = self.merged_engine.get_kv();
        let interval = self.config.report_region_interval.0;
        let report_loop = self.runtime.spawn(async move {
            Self::report_regions_loop(keyspace_id, raft.clone(), kv.clone(), rep_pd_cli, interval)
                .await
        });
        let ks = Keyspace {
            service: task_service,
            report_loop: Some(report_loop),
        };
        if let Some(sp_mgr) = self.safepoint_mgr.as_ref() {
            sp_mgr.add_keyspace(
                keyspace_id,
                ks.get_states().cdc_addr.clone(),
                ks.get_pd_client(),
            );
        }
        self.keyspaces.insert(keyspace_id, ks);
        self.merged_engine
            .set_keyspace_states(keyspace_id, states)?;
        Ok(())
    }

    fn handle_load_keyspace_shard_metas(
        &mut self,
        keyspace_id: u32,
    ) -> Result<StdHashMap<u64 /* region_id */, ShardMeta>> {
        self.merged_engine
            .load_keyspace_shard_metas(keyspace_id)
            .map_err(Into::into)
    }

    // Use force with caution: it should only be used after confirming TiCDC has
    // no changefeeds for this keyspace. This is a workaround for states becoming
    // inconsistent between replication worker and TiCDC.
    fn handle_remove_keyspace_service(
        &mut self,
        keyspace_id: u32,
        force: bool,
        cb: Box<dyn FnOnce(Result<()>) + Send>,
    ) {
        info!("remove_keyspace"; "keyspace" => keyspace_id);
        let ks = match self.keyspaces.entry(keyspace_id) {
            HashMapEntry::Vacant(_) => {
                // Accept duplicated remove_keyspace request.
                info!("remove_keyspace: keyspace not found"; "keyspace" => keyspace_id);
                cb(Ok(()));
                return;
            }
            HashMapEntry::Occupied(e) => {
                let svc = &e.get().service;
                if !svc.get_states().feeds.is_empty() {
                    let feed_keys: Vec<String> = svc.get_states().feeds.keys().cloned().collect();
                    warn!(
                        "remove_keyspace: changefeeds not empty";
                        "keyspace" => keyspace_id,
                        "feeds" => ?feed_keys
                    );

                    if !force {
                        cb(Err(Error::OtherError("changefeeds not empty".into())));
                        return;
                    }
                }
                e.remove()
            }
        };
        if let Some(sp_mgr) = self.safepoint_mgr.as_ref() {
            sp_mgr.remove_keyspace(keyspace_id);
        }
        self.cdc_addrs.remove(&keyspace_id);
        let keyspace_regions = self.merged_engine.get_keyspace_regions(keyspace_id);
        keyspace_regions.iter().for_each(|&region_id| {
            self.remove_region(region_id);
        });
        self.merged_engine.remove_keyspace(keyspace_id);
        self.runtime.spawn(async move {
            let res = ks.destroy().await;
            cb(res);
        });
    }

    fn handle_remove_task(&mut self, keyspace_id: u32, changefeed_id: String) -> Result<()> {
        info!("remove_task"; "keyspace" => keyspace_id, "changefeed" => &changefeed_id);
        let Some(ks) = self.keyspaces.get_mut(&keyspace_id) else {
            return Err(Error::OtherError("keyspace service not found".into()));
        };
        if ks.get_states_mut().feeds.remove(&changefeed_id).is_some() {
            if let Some(sp_mgr) = self.safepoint_mgr.as_ref() {
                sp_mgr.remove_changefeed(keyspace_id, changefeed_id);
            }
            self.merged_engine
                .set_keyspace_states(keyspace_id, ks.get_states().marshal())?
        };
        Ok(())
    }

    fn handle_applied(
        &mut self,
        region_id: u64,
        region_events: RegionEvents,
    ) -> Vec<(RequestKey, cdc::Error)> /* sink_err_requests */ {
        let tag = self.get_region_tag(region_id, 0);
        let mut sink_err_requests: Vec<(RequestKey, cdc::Error)> = vec![];

        let Some(delegate) = self.region_delegates.get_mut(&region_id) else {
            debug!("{} handle_applied: region delegate not found, skip", tag; "events" => ?region_events);
            return sink_err_requests;
        };
        let tag = tag.with_region_version(delegate.region_ver);
        if region_events.region_version != delegate.region_ver {
            warn!("{} handle_applied: version not match", tag; "events.version" => region_events.region_version);
            debug_assert!(
                false,
                "{} version not match, events: {:?}",
                tag, region_events.events
            );
        }

        for (req_key, req_info) in delegate.requests.iter_mut() {
            let Some(conn) = self.conns.get(&req_key.conn_id) else {
                warn!("{} handle_applied: conn not found, skip", tag;
                    "conn" => ?req_key.conn_id, "request" => %req_key.request_id);
                continue;
            };
            'EVENTS_LOOP: for event in &region_events.events {
                // It's OK to not return error. Outer loop will exit before handle next message.
                try_force_stop!(self, vec![]);

                let mut event_to_send = Event::new();
                event_to_send.set_request_id(req_key.request_id.into_inner());
                event_to_send.set_region_id(region_id);
                event_to_send.set_index(event.get_index());
                if event.has_entries() {
                    let entries_to_send = event_to_send.mut_entries().mut_entries();
                    for entry in event.get_entries().get_entries() {
                        if req_info.in_range(entry.get_key()) {
                            entries_to_send.push(entry.clone());

                            trace!("send event";
                                "key" => LogValue::key(entry.get_key()),
                                "commit_ts" => entry.get_commit_ts(),
                                "region" => region_id,
                                "request" => %req_key.request_id,
                                "r_type" => ?entry.r_type,
                                "op_type" => ?entry.op_type);
                        }
                    }
                }
                if req_info.state.is_initialized() {
                    if let Err(err) = conn
                        .get_sink()
                        .unbounded_send(CdcEvent::Event(event_to_send), false)
                    {
                        error!("{} handle_applied: send event failed: {:?}", tag, err;
                            "conn" => ?req_key.conn_id, "request" => %req_key.request_id);
                        // When channel is full, simply send error to TiCDC may lead to cascade
                        // failure. Slow down would be better.
                        // TODO: find a better way to handle sink error.
                        sink_err_requests.push((*req_key, cdc::Error::from(err)));
                        break 'EVENTS_LOOP;
                    }
                } else if req_info.state.is_initializing() {
                    req_info.state.must_push_pending(event_to_send);
                }
            }
        }

        if let Some(resolver) = delegate.resolver.as_mut() {
            for (track_key, start_ts) in region_events.tracked_locks {
                if start_ts == 0 {
                    // TODO: handle error.
                    resolver.untrack_lock(&track_key).unwrap();
                } else {
                    // TODO: handle error.
                    resolver.track_lock(start_ts.into(), track_key).unwrap();
                }
            }
        }

        sink_err_requests
    }

    fn handle_applied_admin(&mut self, region_id: u64, region_version: u64, admin: AdminRequest) {
        let tag = self.get_region_tag(region_id, region_version);
        let raft = self.merged_engine.get_raft();
        let rep_region_opt = Self::get_region_for_rep(&raft, region_id);

        if let Some(rep_region) = &rep_region_opt {
            match self
                .get_keyspace_id(region_id)
                .map(|keyspace_id| (keyspace_id, self.keyspaces.get(&keyspace_id)))
            {
                None => {
                    let err_msg = format!("{} handle_applied_admin: region not found", tag);
                    warn!("{}", &err_msg);
                    debug_assert!(false, "{}", &err_msg);
                    // Still go on and sink error for safety.
                }
                Some((keyspace_id, None)) => {
                    // The keyspace is just removed.
                    warn!("{} handle_applied_admin: keyspace not found", tag; "keyspace" => keyspace_id);
                    debug_assert!(!self.region_delegates.contains_key(&region_id));
                    // Still go on and sink error for safety.
                    // TODO: return Ok(()).
                }
                Some((_, Some(task_ctx))) => {
                    let pd_client = task_ctx.get_pd_client();
                    Self::report_region_to_rep_pd(&pd_client, rep_region.clone());
                    if admin.has_splits() {
                        let split = admin.get_splits();
                        for req in split.get_requests() {
                            Self::report_region_to_rep_pd_by_id(
                                &raft,
                                &pd_client,
                                req.get_new_region_id(),
                            );
                        }
                    }
                }
            }
        } else {
            info!("{} handle_applied_admin: region is merged", tag);
        }

        if admin.has_commit_merge() {
            let source_region = admin.get_commit_merge().get_source();
            info!("{} handle_applied_admin: remove source region of merge", tag; "source" => ?source_region);

            let mut error = cdcpb::Error::new();
            error.mut_region_not_found().set_region_id(source_region.id);
            self.remove_stale_delegate(tag, source_region.id, error);
        }

        let mut error = cdcpb::Error::new();
        if let Some(rep_region) = rep_region_opt {
            error
                .mut_epoch_not_match()
                .mut_current_regions()
                .push(rep_region);
        } else {
            error.mut_region_not_found().set_region_id(region_id);
        }

        self.remove_stale_delegate(tag, region_id, error);
    }

    fn remove_stale_delegate(&mut self, tag: ShardTag, region_id: u64, error: cdcpb::Error) {
        if let Some(mut delegate) = self.region_delegates.remove(&region_id) {
            debug!("{} remove_stale_delegate: send error to requests", tag;
                "err" => ?error, "requests" => ?delegate.requests.keys());
            for (req_key, _) in delegate.requests.drain() {
                let Some(conn) = self.conns.get(&req_key.conn_id) else {
                    continue;
                };
                let mut event = Event::new();
                event.set_region_id(region_id);
                event.set_request_id(req_key.request_id.into_inner());
                event.set_error(error.clone());
                if let Err(err) = conn.get_sink().unbounded_send(CdcEvent::Event(event), true) {
                    warn!("{} remove_stale_delegate: send error failed: {:?}", tag, err;
                        "conn" => ?req_key.conn_id, "request" => %req_key.request_id);
                }
            }
            self.remove_region(region_id);
        }
    }

    fn remove_region(&mut self, region_id: u64) {
        self.region_to_keyspace.remove(&region_id);
        self.region_delegates.remove(&region_id);
        for conn_regions in self.conn_regions.values_mut() {
            conn_regions.remove(&region_id);
        }
    }

    fn store_working_dir(&self, store_id: u64) -> PathBuf {
        self.working_dir.join(store_id.to_string())
    }

    fn report_metrics(&self) {
        CDC_ENDPOINT_PENDING_TASKS.set(self.rx.len() as i64);
        CDC_CAPTURED_REGION_COUNT.set(self.region_delegates.len() as i64);

        let last_update_physical = self.last_update_ts.physical();
        if let Some(stats) = &self.resolved_ts_stats {
            CDC_REGION_RESOLVE_STATUS_GAUGE_VEC
                .with_label_values(&["resolved"])
                .set(stats.resolved_regions as i64);
            CDC_REGION_RESOLVE_STATUS_GAUGE_VEC
                .with_label_values(&["unresolved"])
                .set(stats.unresolved_regions as i64);
            if !stats.min_ts.is_zero() {
                CDC_MIN_RESOLVED_TS_REGION.set(stats.min_ts_region_id as i64);
                CDC_MIN_RESOLVED_TS.set(stats.min_ts.physical() as i64);
                let lag = last_update_physical.saturating_sub(stats.min_ts.physical());
                CDC_MIN_RESOLVED_TS_LAG.set(lag as i64);
                let lag_in_secs = lag as f64 / 1000.0;
                CDC_RESOLVED_TS_GAP_HISTOGRAM.observe(lag_in_secs);
            }
        }

        REP_SYNC_WAL_TS.set(last_update_physical as i64);
        let lag = TimeStamp::physical_now().saturating_sub(last_update_physical);
        REP_SYNC_WAL_TS_LAG.set(lag as i64);
        let lag_in_secs = lag as f64 / 1000.0;
        REP_SYNC_WAL_TS_LAG_HISTOGRAM.observe(lag_in_secs);
    }
}

// Misc helper functions.
// TODO: move to individual file.
impl ReplicationWorker {
    fn mut_blocked_request_info(
        &mut self,
        region_id: u64,
        conn_id: ConnId,
        request_id: RequestId,
        init_id: InitId,
    ) -> Option<&mut RequestInfo> {
        let delegate = self.region_delegates.get_mut(&region_id)?;
        delegate.mut_blocked_request(conn_id, request_id, init_id)
    }
}

struct RegisterHandler {
    conn_id: ConnId,
    request_id: RequestId,
    snap_access: SnapAccess,
    sender: Sender<CdcMsg>,
    sink: cdc::Sink,
    start_key: Bytes,
    end_key: Bytes,
    checkpoint_ts: u64,
    event_rows: Vec<EventRow>,
    initialized: bool,
    init_id: InitId,
    init_alive: InitAlive,
    concurrency_limit: Arc<Semaphore>,
}

impl RegisterHandler {
    fn new(
        conn_id: ConnId,
        request: &ChangeDataRequest,
        snap_access: SnapAccess,
        init_id: InitId,
        init_alive: InitAlive,
        concurrency_limit: Arc<Semaphore>,
        sender: Sender<CdcMsg>,
        sink: cdc::Sink,
    ) -> Self {
        let keyspace_id = snap_access.get_keyspace_id();
        let request_id = request.get_request_id().into();
        let (start_key, end_key) = build_request_range_for_keyspace(keyspace_id, request);
        let checkpoint_ts = request.get_checkpoint_ts();
        Self {
            conn_id,
            request_id,
            snap_access,
            sender,
            sink,
            start_key: start_key.into(),
            end_key: end_key.into(),
            checkpoint_ts,
            event_rows: vec![],
            initialized: false,
            init_id,
            init_alive,
            concurrency_limit,
        }
    }

    async fn handle_register(&mut self) {
        if let Err(err) = self.handle_register_impl().await {
            let tag = self.snap_access.get_tag();
            CDC_SCAN_TASKS.with_label_values(&["abort"]).inc();
            match err {
                Error::RegisterCancelled(msg) => {
                    info!("{} incremental scan cancelled: {}", tag, msg);
                }
                Error::CdcError(cdc_err) => {
                    error!("{} incremental scan failed: {:?}", tag, cdc_err;
                        "request" => %self.request_id, "conn" => ?self.conn_id);
                    if !self.send_register_result(Some(cdc_err)) {
                        warn!("{} incremental scan: send error result failed", tag);
                    }
                }
                err => {
                    // Should not reach here.
                    error!("{} incremental scan failed: {:?}", tag, err;
                        "request" => %self.request_id, "conn" => ?self.conn_id);
                    debug_assert!(false);
                    let cdc_err = cdc::Error::Other(box_err!(err));
                    if !self.send_register_result(Some(cdc_err)) {
                        warn!("{} incremental scan: send error result failed", tag);
                    }
                }
            }
        }
    }

    async fn handle_register_impl(&mut self) -> Result<()> {
        CDC_SCAN_TASKS.with_label_values(&["total"]).inc();

        let _permit: OwnedSemaphorePermit = self.acquire_permit().await?;
        self.check_alive()?;

        CDC_SCAN_TASKS.with_label_values(&["ongoing"]).inc();
        tikv_util::defer!({
            CDC_SCAN_TASKS.with_label_values(&["ongoing"]).dec();
        });

        let tag = self.snap_access.get_tag();
        let mut entries_bytes = 0;
        let keyspace_id = self.snap_access.get_keyspace_id();
        info!("{} start incremental scan", tag; "checkpoint_ts" => self.checkpoint_ts,
            "request" => %self.request_id, "conn" => ?self.conn_id);
        // scan incremental write after checkpoint ts;
        let start_time = Instant::now_coarse();
        let mut write_iter = self
            .snap_access
            .new_delta_write_iterator_async(self.checkpoint_ts)
            .await;
        write_iter
            .seek_async(InnerKey::from_outer_key(&self.start_key))
            .await;
        let end_key = self.end_key.clone();
        let inner_end_key = InnerKey::from_outer_end_key(&end_key);
        while write_iter.valid() {
            let key = write_iter.key();
            if key >= inner_end_key {
                break;
            }
            let val = write_iter.value();
            if is_index_key(key.deref()) || val.is_deleted() || val.version <= self.checkpoint_ts {
                write_iter.next_async().await;
                continue;
            }
            let um = UserMeta::from_slice(val.user_meta());
            let mut event_row = EventRow::new();
            event_row.set_start_ts(um.start_ts);
            event_row.set_commit_ts(um.commit_ts);
            event_row.set_key(key.deref().to_vec());
            if val.get_value().is_empty() {
                event_row.set_op_type(EventRowOpType::Delete);
            } else {
                event_row.set_op_type(EventRowOpType::Put);
            }
            event_row.set_value(val.get_value().to_vec());
            let mut outer_key = ApiV2::get_keyspace_prefix_by_id(keyspace_id);
            outer_key.extend_from_slice(key.deref());
            let old_value = self
                .snap_access
                .get_async(WRITE_CF, &outer_key, um.commit_ts - 1)
                .await;
            if !old_value.get_value().is_empty() {
                event_row.set_old_value(old_value.get_value().to_vec());
            }
            event_row.set_type(EventLogType::Committed);
            entries_bytes += event_row.get_key().len() + event_row.get_value().len();
            self.event_rows.push(event_row);
            if entries_bytes > MAX_INITIALIZE_SCAN_BATCH_BYTES {
                self.send_rows().await?;
                CDC_SCAN_BYTES.inc_by(entries_bytes as u64);
                entries_bytes = 0;
            }
            write_iter.next_all_version_async().await;
        }
        let mut init_row = EventRow::new();
        init_row.set_type(EventLogType::Initialized);
        self.event_rows.push(init_row);
        self.initialized = true;
        self.send_rows().await?;
        CDC_SCAN_BYTES.inc_by(entries_bytes as u64);

        let takes = start_time.saturating_elapsed();
        CDC_SCAN_DURATION_HISTOGRAM.observe(takes.as_secs_f64());
        CDC_SCAN_TASKS.with_label_values(&["finish"]).inc();
        info!("{} incremental scan finished", tag; "checkpoint_ts" => self.checkpoint_ts,
            "request" => %self.request_id, "conn" => ?self.conn_id, "takes" => ?takes,
            "entries_bytes" => entries_bytes);
        Ok(())
    }

    async fn send_rows(&mut self) -> Result<()> {
        self.check_alive()?;

        let event_rows = mem::take(&mut self.event_rows);
        let mut new_event = Event::new();
        new_event.set_region_id(self.snap_access.get_id());
        new_event.set_request_id(self.request_id.into_inner());
        new_event.mut_entries().set_entries(event_rows.into());

        let tag = self.snap_access.get_tag();
        trace!("{} incremental scan: send event {:?}", tag, new_event);
        let mut events = vec![CdcEvent::Event(new_event)];

        // CDC needs to make sure resolved ts events can only be sent after
        // incremental scan is finished.
        // Wait the barrier to ensure channel sends out all events.
        // Ref: cdc::Initializer::sink_scan_events.
        let barrier = if self.initialized {
            let (cb, fut) = paired_future_callback();
            events.push(CdcEvent::Barrier(Some(cb)));
            Some(fut)
        } else {
            None
        };

        // Use `send_all` for back-pressure.
        if let Err(e) = self.sink.send_all(events).await {
            return Err(Error::CdcError(cdc::Error::Sink(e)));
        }

        if let Some(barrier) = barrier {
            debug_assert!(self.initialized);
            if barrier.await.is_err() {
                // Should happen only when the sink is closed.
                return Err(Error::RegisterCancelled("barrier cancelled".into()));
            }

            // Send "initialized" event.
            if !self.send_register_result(None) {
                // Should happen only when the sender/receiver is closed.
                return Err(Error::RegisterCancelled(
                    "send register result failed".into(),
                ));
            }
        }

        Ok(())
    }

    fn send_register_result(&self, err_opt: Option<cdc::Error>) -> bool /* ok */ {
        debug!("{} incremental scan: send register result", self.snap_access.get_tag();
            "init_id" => ?self.init_id, "err" => ?err_opt,
            "request" => %self.request_id, "conn" => ?self.conn_id);
        self.sender
            .send(CdcMsg::RegisterResult {
                region_id: self.snap_access.get_id(),
                conn_id: self.conn_id,
                request_id: self.request_id,
                init_id: self.init_id,
                err_opt,
            })
            .is_ok()
    }

    async fn acquire_permit(&self) -> Result<OwnedSemaphorePermit> {
        self.concurrency_limit
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| {
                // Semaphore closed, replication worker should be dropped.
                Error::RegisterCancelled("semaphore closed".into())
            })
    }

    fn check_alive(&self) -> Result<()> {
        if !self.init_alive.ok() {
            Err(Error::RegisterCancelled("init not alive".into()))
        } else {
            Ok(())
        }
    }
}

struct ScanLocksHandler {
    snap_access: SnapAccess,
    sender: Sender<CdcMsg>,
    locks: Vec<(Vec<u8>, TimeStamp)>,
}

impl ScanLocksHandler {
    fn new(snap_access: SnapAccess, sender: Sender<CdcMsg>) -> Self {
        Self {
            snap_access,
            sender,
            locks: vec![],
        }
    }

    fn scan_locks(&mut self) {
        REP_SCAN_LOCKS_TASKS.with_label_values(&["total"]).inc();
        REP_SCAN_LOCKS_TASKS.with_label_values(&["ongoing"]).inc();
        tikv_util::defer!({
            REP_SCAN_LOCKS_TASKS.with_label_values(&["ongoing"]).dec();
        });

        let region_id = self.snap_access.get_id();
        let res = self.scan_locks_impl();
        let label = if res.is_ok() { "finished" } else { "abort" };
        REP_SCAN_LOCKS_TASKS.with_label_values(&[label]).inc();
        let locks = res.map(|_| mem::take(&mut self.locks));
        let snap_version = self.snap_access.get_mem_table_snap_version();
        if let Err(e) = self.sender.send(CdcMsg::ScanLocksResult {
            region_id,
            locks,
            snap_version,
        }) {
            warn!("{} failed to send scan locks result", self.snap_access.get_tag(); "err" => ?e);
        }
    }

    fn scan_locks_impl(&mut self) -> Result<()> {
        let keyspace_id = self.snap_access.get_keyspace_id();
        let tag = self.snap_access.get_tag();
        info!("{} cdc scan locks", tag; "keyspace" => keyspace_id);

        let start_time = Instant::now_coarse();
        let mut bytes = 0;
        let keyspace_prefix_len = keyspace_prefix_len(keyspace_id);
        let mut lock_iter = self
            .snap_access
            .new_iterator(LOCK_CF, false, false, None, false);
        lock_iter.set_range(
            self.snap_access.clone_start_key(),
            self.snap_access.clone_end_key(),
        );
        while lock_iter.valid() {
            let inner_lock_key = &lock_iter.key()[keyspace_prefix_len..];
            if is_index_key(inner_lock_key) {
                lock_iter.next();
                continue;
            }
            let lock_val = lock_iter.val();
            let lock = txn_types::Lock::parse(lock_val).unwrap();
            if !matches!(lock.lock_type, LockType::Put | LockType::Delete) {
                lock_iter.next();
                continue;
            }
            let key = lock_iter.key()[keyspace_prefix_len..].to_vec();
            bytes += key.len() as u64 + mem::size_of::<TimeStamp>() as u64;
            self.locks.push((key, lock.ts));
            lock_iter.next();
        }

        let takes = start_time.saturating_elapsed();
        info!("{} cdc scan locks", tag; "keyspace" => keyspace_id,
            "locks" => self.locks.len(), "bytes" => bytes, "takes" => ?takes);
        REP_SCAN_LOCKS_BYTES.inc_by(bytes);
        REP_SCAN_LOCKS_DURATION_HISTOGRAM.observe(takes.as_secs_f64());
        Ok(())
    }
}

// Ref: ObjectStorageWorker::near_overwritten_epoch
fn near_overwritten_epoch(current_epoch: u32, epoch_rotate_len: usize) -> u32 {
    current_epoch.saturating_sub(epoch_rotate_len as u32 - 2)
}

struct Keyspace {
    service: Box<dyn KeyspaceService>,
    report_loop: Option<tokio::task::JoinHandle<()>>,
}

impl From<Box<dyn KeyspaceService>> for Keyspace {
    fn from(service: Box<dyn KeyspaceService>) -> Self {
        Self {
            service,
            report_loop: None,
        }
    }
}

impl ops::Deref for Keyspace {
    type Target = Box<dyn KeyspaceService>;

    fn deref(&self) -> &Self::Target {
        &self.service
    }
}

impl ops::DerefMut for Keyspace {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.service
    }
}

impl Keyspace {
    async fn destroy(self) -> Result<()> {
        let Self {
            mut service,
            report_loop,
        } = self;
        let stop_report_loop = async {
            if let Some(report_loop) = report_loop {
                report_loop.abort();
                if let Err(e) = report_loop.await
                    && e.is_panic()
                {
                    warn!("report loop panic: {:?}", e);
                    debug_assert!(false);
                }
            }
        };
        let (res_svc, _) = tokio::join!(service.destroy(), stop_report_loop);
        res_svc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_start_ts() {
        {
            let req = json!({
                "changefeed_id": "cf1",
                "sink_uri": "sink",
                "a": 1,
                "b": "b",
            });
            let req_body = serde_json::to_vec(&req).unwrap();
            let (updated_start_ts, updated_body) =
                ReplicationWorker::handle_start_ts("tag", 0, req_body.into(), 1000).unwrap();
            assert_eq!(updated_start_ts, 1000);
            assert_eq!(
                String::from_utf8_lossy(&updated_body),
                r#"{"changefeed_id":"cf1","sink_uri":"sink","a":1,"b":"b","start_ts":1000}"#
            );
        }

        {
            let req = json!({
                "changefeed_id": "cf1",
                "sink_uri": "sink",
                "a": 1,
                "b": "b",
                "start_ts": 500,
            });
            let req_body = serde_json::to_vec(&req).unwrap();
            let err = ReplicationWorker::handle_start_ts("tag", 500, req_body.clone().into(), 499)
                .unwrap_err();
            assert!(err.to_string().contains("start_ts too large"));

            let (updated_start_ts, updated_body) =
                ReplicationWorker::handle_start_ts("tag", 500, req_body.into(), 1000).unwrap();
            assert_eq!(updated_start_ts, 500);
            assert_eq!(
                String::from_utf8_lossy(&updated_body),
                r#"{"changefeed_id":"cf1","sink_uri":"sink","a":1,"b":"b","start_ts":500}"#
            );
        }
    }
}
