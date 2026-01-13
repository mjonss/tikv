// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(let_chains)]

mod apply_observer;
mod delegate;
mod error;
mod kube;
mod metrics;
mod provisioned;
mod safepoint;
mod scheduler;
mod ticdc_util;
mod util;
mod wal;
mod worker;

use std::{
    collections::{HashMap, HashMap as StdHashMap},
    sync::Arc,
};

use api_version::ApiV2;
pub use apply_observer::{CdcApplyObserver, RegionEvents};
use async_trait::async_trait;
use bytes::Bytes;
use cdc::{Conn, ConnId, MemoryQuota};
pub use error::{Error, Result};
use futures::{SinkExt, TryFutureExt, TryStreamExt, future};
use grpcio::{DuplexSink, RequestStream, RpcContext, RpcStatus, RpcStatusCode, UnarySink};
use http::StatusCode;
use kvengine::{Shard, ShardMeta, SnapAccess, WRITE_CF, table::SnapVersion};
use kvproto::{
    cdcpb,
    cdcpb::{ChangeDataEvent, ChangeDataRequest},
    cdcpb_grpc::ChangeData,
    errorpb::EpochNotMatch,
    kvrpcpb::{
        GetRequest, GetResponse, ScanLockRequest, ScanLockResponse, ScanRequest, ScanResponse,
    },
    metapb,
    metapb::{NodeState, RegionEpoch},
    raft_cmdpb::AdminRequest,
    tikvpb_grpc::Tikv,
};
use merged_engine::MergedEngineConfig;
use pd_client::{PdClient, RpcClient};
#[cfg(feature = "testexport")]
pub use provisioned::local_provider::LocalProvider;
pub use scheduler::*;
use serde_derive::{Deserialize, Serialize};
use tikv::tikv_build_version;
use tikv_util::{
    config::{AbsoluteOrPercentSize, ReadableDuration},
    error, info, warn,
};
use txn_types::TimeStamp;
pub use worker::ReplicationWorker;

use crate::delegate::{InitId, RequestId};

pub(crate) const K8S_SERVICE_HOST: &str = "KUBERNETES_SERVICE_HOST";

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ReplicationWorkerConfig {
    pub enabled: bool,

    pub grpc_addr: String,
    pub advertise_addr: String,

    // used for k8s mode.
    pub pd_sts_name: String,
    pub cdc_sts_name: String,
    pub namespace: String,

    /// Whether to tolerate store errors (no more than 1 store) during update
    /// WAL.
    pub tolerate_store_err: bool,
    pub update_stores_wal_size_limit: AbsoluteOrPercentSize,

    /// The maximum concurrency for incremental scan.
    ///
    /// For default value 1024, the maximum memory usage of incremental scan is
    /// about:
    ///
    /// 1152(MB) = 128 * 1MB (in channel) + 1024 * 1MB (waiting for channel).
    pub incr_scan_concurrency_limit: usize,

    /// The interval to sync changes from WAL.
    pub sync_interval: ReadableDuration,
    pub report_region_interval: ReadableDuration,
    pub local_file_gc_timeout: ReadableDuration,

    /// The address keywords of stores to be skipped during replication.
    pub skip_store_addr_keywords: Vec<String>,

    /// The min/max time span of a WAL target since the last one. Ref:
    /// WalProgressFetcher.
    pub min_wal_target_time_span: ReadableDuration,
    pub max_wal_target_time_span: ReadableDuration,
    /// Whether to fetch WAL target from backup. Used to work around broken
    /// backups or bugs.
    pub fetch_wal_target_from_backup: bool,

    pub safepoint: SafepointConfig,
    pub merged_engine: MergedEngineConfig,
}

impl Default for ReplicationWorkerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            grpc_addr: "".to_string(),
            advertise_addr: "".to_string(),
            pd_sts_name: "".to_string(),
            cdc_sts_name: "".to_string(),
            namespace: "".to_string(),
            tolerate_store_err: false,
            update_stores_wal_size_limit: AbsoluteOrPercentSize::Percent(20.0),
            incr_scan_concurrency_limit: 1024,
            sync_interval: ReadableDuration::secs(3),
            report_region_interval: ReadableDuration::secs(60),
            local_file_gc_timeout: ReadableDuration::minutes(10),
            skip_store_addr_keywords: vec![],
            min_wal_target_time_span: ReadableDuration::minutes(5),
            max_wal_target_time_span: ReadableDuration::minutes(20),
            fetch_wal_target_from_backup: true,
            safepoint: Default::default(),
            merged_engine: Default::default(),
        }
    }
}

impl ReplicationWorkerConfig {
    pub fn override_from_env(&mut self) {
        Self::env_or_default("PD_STS_NAME", &mut self.pd_sts_name);
        Self::env_or_default("CDC_STS_NAME", &mut self.cdc_sts_name);
    }

    fn env_or_default(name: &str, val: &mut String) {
        if let Ok(v) = std::env::var(name) {
            *val = v;
        }
    }

    pub fn is_kube_mode(&self) -> bool {
        !self.pd_sts_name.is_empty()
            && !self.cdc_sts_name.is_empty()
            && !self.namespace.is_empty()
            && std::env::var(K8S_SERVICE_HOST).is_ok()
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct SafepointConfig {
    /// `gc_ttl` is the time-to-live when replication worker set service
    /// safepoint. It is defined the same as `gc-ttl` of TiCDC.
    pub gc_ttl: ReadableDuration,
    /// The time-to-live when replication worker set service safepoint during
    /// creating changefeed to ensure that the `start_ts` of the changefeed is
    /// available during changefeed initialization.
    ///
    /// The value should not be too small, or the safepoint will expire before
    /// changefeed initialized.
    ///
    /// And the value should not be too large, as we do not remove the safepoint
    /// for easier.
    pub create_changefeed_gc_ttl: ReadableDuration,
    /// The interval to sync GC safepoint of changefeeds from TiCDC.
    pub sync_safepoint_interval: ReadableDuration,
    pub sync_ticdc_timeout: ReadableDuration,
}

impl Default for SafepointConfig {
    fn default() -> Self {
        Self {
            gc_ttl: ReadableDuration::hours(24),
            create_changefeed_gc_ttl: ReadableDuration::minutes(10),
            sync_safepoint_interval: ReadableDuration::minutes(1),
            sync_ticdc_timeout: ReadableDuration::secs(30),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct KeyspaceStates {
    pub(crate) feeds: HashMap<String, String>,
    pub(crate) pd_url: String,
    pub(crate) cdc_addr: String,

    // used in k8s.
    pub(crate) pd_sts_name: String,
    pub(crate) cdc_sts_name: String,
}

impl KeyspaceStates {
    pub(crate) fn marshal(&self) -> Bytes {
        serde_json::to_vec(self).unwrap().into()
    }

    pub(crate) fn is_provisioned(&self) -> bool {
        self.pd_sts_name.is_empty()
    }
}

#[async_trait]
pub trait KeyspaceService: Send {
    fn keyspace_id(&self) -> u32;

    async fn start(&mut self) -> Result<()>;

    async fn destroy(&mut self) -> Result<()>;

    fn get_states(&self) -> &KeyspaceStates;

    fn get_states_mut(&mut self) -> &mut KeyspaceStates;

    fn get_pd_client(&self) -> Arc<dyn PdClient>;
}

pub enum CdcMsg {
    AddKeyspace {
        keyspace_id: u32,
        pd_url: String,
        cdc_addr: String,
        cb: Box<dyn FnOnce(Result<()>) + Send>,
    },
    LoadKeyspaceShards {
        keyspace_id: u32,
        task_service: Box<dyn KeyspaceService>,
        cb: Box<dyn FnOnce(Result<()>) + Send>,
    },
    LoadKeyspaceShardMetas {
        keyspace_id: u32,
        cb: Box<dyn FnOnce(Result<StdHashMap<u64 /* region_id */, ShardMeta>>) + Send>,
    },
    NewTask {
        keyspace_id: u32,
        changefeed_id: String,
        start_ts: u64,
        body: Bytes,
        cb: Box<dyn FnOnce(Result<(StatusCode, Bytes)>) + Send>,
    },
    OpenConn(cdc::Conn),
    Register {
        request: ChangeDataRequest,
        conn_id: ConnId,
    },
    SpawnRegisterHandler {
        request: ChangeDataRequest,
        conn_id: ConnId,
        snap_access: SnapAccess,
    },
    ResumeRegister {
        conn_id: ConnId,
        request_id: RequestId,
        snap_access: SnapAccess,
        init_id: InitId,
    },
    RegisterResult {
        region_id: u64,
        conn_id: ConnId,
        request_id: RequestId,
        init_id: InitId,
        err_opt: Option<cdc::Error>,
    },
    SpawnScanLocks {
        snap_access: SnapAccess,
    },
    ScanLocksResult {
        region_id: u64,
        locks: Result<Vec<(Vec<u8>, TimeStamp)>>,
        snap_version: SnapVersion,
    },
    Applied {
        region_id: u64,
        region_events: RegionEvents,
    },
    AppliedAdmin {
        region_id: u64,
        region_version: u64,
        admin: AdminRequest,
    },
    Deregister(Deregister),
    RemoveTask {
        keyspace_id: u32,
        changefeed_id: String,
        cb: Box<dyn FnOnce(Result<()>) + Send>,
    },
    RemoveKeyspace {
        keyspace_id: u32,
        force: bool,
        cb: Box<dyn FnOnce(Result<()>) + Send>,
    },
    GetKeyspaces {
        cb: Box<dyn FnOnce(Vec<u32>) + Send>,
    },
    Stop,
}

pub enum Deregister {
    Conn(ConnId),
    Request {
        conn_id: ConnId,
        request_id: RequestId,
    },
    Region {
        conn_id: ConnId,
        request_id: RequestId,
        region_id: u64,
    },
}

#[derive(Clone)]
struct ReplicationService {
    kv: kvengine::Engine,
    scheduler: tikv_util::mpsc::Sender<CdcMsg>,
    memory_quota: MemoryQuota,
    runtime: tokio::runtime::Handle,
}

impl ReplicationService {
    pub fn new(
        kv: kvengine::Engine,
        scheduler: tikv_util::mpsc::Sender<CdcMsg>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            kv,
            scheduler,
            memory_quota: MemoryQuota::new(1024 * 1024 * 1024),
            runtime,
        }
    }

    fn prepend_keyspace_prefix(shard: &Shard, req_key: &[u8]) -> Option<Bytes> {
        if req_key.is_empty() {
            None
        } else {
            let mut outer_req_key = ApiV2::get_keyspace_prefix_by_id(shard.keyspace_id);
            outer_req_key.extend_from_slice(req_key);
            Some(outer_req_key.into())
        }
    }
}

static CDC_CHANNEL_CAPACITY: usize = 128;

impl ReplicationService {
    fn handle_event_feed(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<ChangeDataRequest>,
        mut sink: DuplexSink<ChangeDataEvent>,
        _event_feed_v2: bool,
    ) {
        // TODO: parse header for event_feed_v2.

        let (event_sink, mut event_drain) =
            cdc::channel(CDC_CHANNEL_CAPACITY, self.memory_quota.clone());
        let peer = ctx.peer();
        let conn = Conn::new(event_sink, peer);
        let conn_id = conn.get_id();

        if let Err(status) = self.scheduler.send(CdcMsg::OpenConn(conn)).map_err(|e| {
            RpcStatus::with_message(RpcStatusCode::INVALID_ARGUMENT, format!("{:?}", e))
        }) {
            error!("cdc connection initiate failed"; "error" => ?status);
            ctx.spawn(
                sink.fail(status)
                    .unwrap_or_else(|e| error!("cdc failed to send error"; "error" => ?e)),
            );
            return;
        }

        let scheduler = self.scheduler.clone();
        let recv_req = stream.try_for_each(move |request| {
            info!("got event feed request {:?}", request);
            let ret = Self::handle_request(&scheduler, request, conn_id)
                .map_err(|rpc_status| grpcio::Error::RpcFailure(rpc_status));
            future::ready(ret)
        });

        let peer = ctx.peer();
        let scheduler = self.scheduler.clone();
        ctx.spawn(async move {
            let res = recv_req.await;
            // Unregister this downstream only.
            if let Err(e) = scheduler.send(CdcMsg::Deregister(Deregister::Conn(conn_id))) {
                error!("cdc deregister failed"; "error" => ?e, "conn" => ?conn_id);
            }
            match res {
                Ok(()) => {
                    info!("cdc receive closed"; "downstream" => peer, "conn" => ?conn_id);
                }
                Err(e) => {
                    warn!("cdc receive failed"; "error" => ?e, "downstream" => peer, "conn" => ?conn_id);
                }
            }
        });

        let peer = ctx.peer();
        let scheduler = self.scheduler.clone();

        ctx.spawn(async move {
            let res = event_drain.forward(&mut sink).await;
            // Unregister this downstream only.
            if let Err(e) = scheduler.send(CdcMsg::Deregister(Deregister::Conn(conn_id))) {
                error!("cdc deregister failed"; "error" => ?e, "conn" => ?conn_id);
            }
            match res {
                Ok(_s) => {
                    info!("cdc send closed"; "downstream" => peer, "conn" => ?conn_id);
                    let _ = sink.close().await;
                }
                Err(e) => {
                    warn!("cdc send failed"; "error" => ?e, "downstream" => peer, "conn" => ?conn_id);
                }
            }
        });
        info!("cdc event feed started"; "conn" => ?conn_id);
    }

    fn handle_request(
        scheduler: &tikv_util::mpsc::Sender<CdcMsg>,
        request: ChangeDataRequest,
        conn_id: ConnId,
    ) -> std::result::Result<(), RpcStatus> {
        match request.request {
            None | Some(cdcpb::ChangeDataRequest_oneof_request::Register(_)) => scheduler
                .send(CdcMsg::Register { request, conn_id })
                .map_err(|e| {
                    RpcStatus::with_message(
                        RpcStatusCode::RESOURCE_EXHAUSTED,
                        format!("replication worker is busy: {:?}", e),
                    )
                }),
            Some(cdcpb::ChangeDataRequest_oneof_request::Deregister(_)) => {
                let deregister = if request.region_id == 0 {
                    Deregister::Request {
                        conn_id,
                        request_id: request.request_id.into(),
                    }
                } else {
                    Deregister::Region {
                        conn_id,
                        request_id: request.request_id.into(),
                        region_id: request.region_id,
                    }
                };
                scheduler.send(CdcMsg::Deregister(deregister)).map_err(|e| {
                    RpcStatus::with_message(
                        RpcStatusCode::RESOURCE_EXHAUSTED,
                        format!("replication worker is busy: {:?}", e),
                    )
                })
            }
            _ => Err(RpcStatus::with_message(
                RpcStatusCode::INVALID_ARGUMENT,
                format!("request not supported: {:?}", request),
            )),
        }
    }
}

impl ChangeData for ReplicationService {
    fn event_feed(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<ChangeDataRequest>,
        sink: DuplexSink<ChangeDataEvent>,
    ) {
        self.handle_event_feed(ctx, stream, sink, false);
    }

    fn event_feed_v2(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<ChangeDataRequest>,
        sink: DuplexSink<ChangeDataEvent>,
    ) {
        self.handle_event_feed(ctx, stream, sink, true);
    }
}

impl Tikv for ReplicationService {
    fn kv_scan(&mut self, ctx: RpcContext<'_>, req: ScanRequest, sink: UnarySink<ScanResponse>) {
        if req.reverse {
            let status = RpcStatus::new(RpcStatusCode::INVALID_ARGUMENT);
            ctx.spawn(
                sink.fail(status.clone())
                    .unwrap_or_else(|e| error!("kv_scan failed"; "error" => ?e)),
            );
            return;
        }
        let mut resp = ScanResponse::default();
        let region_id = req.get_context().get_region_id();
        let region_version = req.get_context().get_region_epoch().get_version();
        let res = self.kv.get_shard_with_ver(region_id, region_version);
        if res.is_err() {
            let mut region_err = kvproto::errorpb::Error::default();
            let epoch_not_match = EpochNotMatch::default();
            region_err.set_epoch_not_match(epoch_not_match);
            resp.set_region_error(region_err);
            ctx.spawn(
                sink.success(resp)
                    .unwrap_or_else(|e| error!("kv_scan failed"; "error" => ?e)),
            );
            return;
        }
        let shard = res.unwrap();
        let outer_start_key = Self::prepend_keyspace_prefix(&shard, req.get_start_key())
            .unwrap_or_else(|| shard.outer_start.clone());
        let outer_end_key = Self::prepend_keyspace_prefix(&shard, req.get_end_key())
            .unwrap_or_else(|| shard.outer_end.clone());
        let read_ts = Some(req.get_version());
        let snap_access = shard.new_snap_access();
        let task = async move {
            tikv_util::set_current_region(region_id);
            // TODO: handle locks.
            let mut iter = snap_access
                .new_iterator_async(WRITE_CF, false, false, read_ts, false)
                .await;
            iter.set_range_async(outer_start_key, outer_end_key).await;
            let mut kv_pairs = vec![];
            let keyspace_prefix_len = ApiV2::get_keyspace_prefix_by_id(shard.keyspace_id).len();
            while iter.valid() {
                if kv_pairs.len() == req.get_limit() as usize {
                    break;
                }
                let mut kv_pair = kvproto::kvrpcpb::KvPair::default();
                let key = iter.key();
                // trim keyspace prefix.
                kv_pair.set_key(key[keyspace_prefix_len..].to_vec());
                let val = iter.val();
                if val.is_empty() {
                    iter.next_async().await;
                    continue;
                }
                kv_pair.set_value(val.to_vec());
                kv_pairs.push(kv_pair);
                iter.next_async().await;
            }
            resp.set_pairs(kv_pairs.into());
            resp
        };
        let resp = self.runtime.spawn(tikv_util::init_task_local(task));
        let fut = async move {
            match resp.await {
                Ok(resp) => sink.success(resp).await,
                Err(err) if err.is_panic() => {
                    panic!("kv_scan panic");
                }
                Err(err) => {
                    sink.fail(RpcStatus::with_message(
                        RpcStatusCode::CANCELLED,
                        format!("{}", err),
                    ))
                    .await
                }
            }
            .unwrap_or_else(|e| {
                error!("kv_scan rpc failed"; "error" => ?e);
            });
        };
        ctx.spawn(fut);
    }

    fn kv_get(&mut self, ctx: RpcContext<'_>, req: GetRequest, sink: UnarySink<GetResponse>) {
        let mut resp = GetResponse::default();
        let region_id = req.get_context().get_region_id();
        let region_version = req.get_context().get_region_epoch().get_version();
        let res = self.kv.get_shard_with_ver(region_id, region_version);
        if res.is_err() {
            let mut region_err = kvproto::errorpb::Error::default();
            let epoch_not_match = EpochNotMatch::default();
            region_err.set_epoch_not_match(epoch_not_match);
            resp.set_region_error(region_err);
            ctx.spawn(
                sink.success(resp)
                    .unwrap_or_else(|e| error!("kv_get failed"; "error" => ?e)),
            );
            return;
        }
        let shard = res.unwrap();
        let snap_access = shard.new_snap_access();
        let key = Self::prepend_keyspace_prefix(&shard, req.get_key()).unwrap();
        let task = async move {
            tikv_util::set_current_region(region_id);
            // TODO: handle locks.
            let item = snap_access
                .get_async(WRITE_CF, &key, req.get_version())
                .await;
            if !item.get_value().is_empty() {
                resp.set_value(item.get_value().to_vec());
            } else {
                resp.set_not_found(true);
            }
            resp
        };
        let resp = self.runtime.spawn(tikv_util::init_task_local(task));
        let fut = async move {
            match resp.await {
                Ok(resp) => sink.success(resp).await,
                Err(err) if err.is_panic() => {
                    panic!("kv_get panic");
                }
                Err(err) => {
                    sink.fail(RpcStatus::with_message(
                        RpcStatusCode::CANCELLED,
                        format!("{}", err),
                    ))
                    .await
                }
            }
            .unwrap_or_else(|e| {
                error!("kv_get rpc failed"; "error" => ?e);
            });
        };
        ctx.spawn(fut);
    }

    fn kv_scan_lock(
        &mut self,
        ctx: RpcContext<'_>,
        req: ScanLockRequest,
        sink: UnarySink<ScanLockResponse>,
    ) {
        // TODO: forward resolve lock request to upstream.
        warn!("received scan lock from CDC {:?}", req);
        let resp = ScanLockResponse::new();
        ctx.spawn(sink.success(resp).unwrap_or_else(|e| {
            error!("kv_scan_lock failed"; "error" => ?e);
        }));
    }
}

async fn bootstrap(pd_client: Arc<RpcClient>, store_id: u64, advertise_addr: String) -> Result<()> {
    let bootstrapped = pd_client.is_cluster_bootstrapped()?;
    if !bootstrapped {
        let mut store = metapb::Store::new();
        store.set_id(store_id);
        store.set_node_state(NodeState::Serving);
        store.set_address(advertise_addr);
        store.set_version(tikv_build_version().to_string());
        let mut initial_region = metapb::Region::new();
        initial_region.set_id(1);
        let mut initial_peer = metapb::Peer::new();
        initial_peer.set_id(1);
        initial_peer.set_store_id(store_id);
        initial_region.set_peers(vec![initial_peer].into());
        let mut initial_epoch = RegionEpoch::new();
        initial_epoch.set_version(1);
        initial_epoch.set_conf_ver(1);
        initial_region.set_region_epoch(initial_epoch);
        pd_client.bootstrap_cluster(store, initial_region)?;
    }
    Ok(())
}
