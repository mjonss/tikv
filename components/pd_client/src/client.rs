// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    convert::TryInto,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use cloud_encryption::KeyspaceEncryptionConfig;
use dashmap::DashMap;
use futures::{
    channel::mpsc,
    compat::{Compat, Future01CompatExt},
    executor::block_on,
    future::{self, BoxFuture, FutureExt, TryFutureExt},
    sink::SinkExt,
    stream::StreamExt,
};
use grpcio::{ClientSStreamReceiver, EnvBuilder, Environment, WriteFlags};
use kvproto::{
    keyspacepb,
    keyspacepb_grpc::KeyspaceClient,
    metapb,
    pdpb::{self, EventType, LoadGlobalConfigRequest, Member, WatchGcSafePointV2Response},
    replication_modepb::{RegionReplicationStatus, ReplicationStatus, StoreDrAutoSyncStatus},
};
use security::{GetSecurityManager, SecurityManager};
use tikv_util::{
    Either, HandyRwLock, box_err, debug, error, info, thd_name,
    time::{Instant, duration_to_sec},
    timer::GLOBAL_TIMER_HANDLE,
    warn,
};
use txn_types::TimeStamp;
use yatp::{ThreadPool, task::future::TaskCell};

use super::{
    BucketStat, Config, Error, FeatureGate, PdClient, PdFuture, REQUEST_TIMEOUT, RegionInfo,
    RegionStat, Result, UnixSecs,
    metrics::*,
    util::{Client, PdConnector, call_option_inner, check_resp_header, sync_request},
};
use crate::{BucketMeta, LEADER_CHANGE_RETRY};

pub const CQ_COUNT: usize = 1;
pub const CLIENT_PREFIX: &str = "pd";
const RETRY_INTERVAL: Duration = Duration::from_secs(1); // to consistent with pd_client

pub struct RpcClient {
    cluster_id: u64,
    pd_client: Arc<Client>,
    monitor: Arc<ThreadPool<TaskCell>>,
    // KS_SAFEPOINT_V2 is to cache keyspace id and gc safepoint v2.
    ks_safepoint_v2: Arc<DashMap<u32, u64>>,
    ks_gc_sp_revision: AtomicI64,
}

impl GetSecurityManager for RpcClient {
    fn get_security_mgr(&self) -> Arc<SecurityManager> {
        self.pd_client.inner.rl().security_mgr.clone()
    }
}

impl RpcClient {
    pub fn new(
        cfg: &Config,
        shared_env: Option<Arc<Environment>>,
        security_mgr: Arc<SecurityManager>,
    ) -> Result<RpcClient> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(Self::new_async(cfg, shared_env, security_mgr))
    }

    pub async fn new_async(
        cfg: &Config,
        shared_env: Option<Arc<Environment>>,
        security_mgr: Arc<SecurityManager>,
    ) -> Result<RpcClient> {
        let env = shared_env.unwrap_or_else(|| {
            Arc::new(
                EnvBuilder::new()
                    .cq_count(CQ_COUNT)
                    .name_prefix(thd_name!(CLIENT_PREFIX))
                    .build(),
            )
        });

        // -1 means the max.
        let retries = match cfg.retry_max_count {
            -1 => isize::MAX,
            v => v.saturating_add(1),
        };
        let monitor = Arc::new(
            yatp::Builder::new(thd_name!("pdmonitor"))
                .max_thread_count(1)
                .build_future_pool(),
        );
        let pd_connector = PdConnector::new(env.clone(), security_mgr.clone());
        for i in 0..retries {
            match pd_connector.validate_endpoints(cfg, true).await {
                Ok((client, target, members, tso)) => {
                    let cluster_id = members.get_header().get_cluster_id();
                    let new_tso = if cfg.force_legacy_tso {
                        None
                    } else {
                        // Create tso service discovery.
                        pd_connector
                            .init_tso_discovery(cluster_id, &client, target.call_option())
                            .await;
                        pd_connector.build_new_tso().await
                    };
                    let rpc_client = RpcClient {
                        cluster_id,
                        pd_client: Arc::new(Client::new(
                            security_mgr,
                            client,
                            members,
                            target,
                            tso.unwrap(),
                            new_tso,
                            cfg.enable_forwarding,
                            pd_connector,
                        )),
                        monitor: monitor.clone(),
                        ks_safepoint_v2: Arc::new(DashMap::default()),
                        ks_gc_sp_revision: AtomicI64::new(0),
                    };

                    // spawn a background future to update PD information periodically
                    let duration = cfg.update_interval.0;
                    let client = Arc::downgrade(&rpc_client.pd_client);
                    let update_loop = async move {
                        loop {
                            let ok = GLOBAL_TIMER_HANDLE
                                .delay(std::time::Instant::now() + duration)
                                .compat()
                                .await
                                .is_ok();

                            if !ok {
                                warn!("failed to delay with global timer");
                                continue;
                            }

                            match client.upgrade() {
                                Some(cli) => {
                                    let req = cli.reconnect(false).await;
                                    if let Err(e) = req {
                                        warn!("failed to update PD client"; "error"=> ?e);
                                        // will update later anyway
                                    }
                                }
                                // if the client has been dropped, we can stop
                                None => break,
                            }
                        }
                    };

                    // `update_loop` contains RwLock that may block the monitor.
                    // Since the monitor does not have other critical task, it
                    // is not a major issue.
                    rpc_client.monitor.spawn(update_loop);

                    let client = Arc::downgrade(&rpc_client.pd_client);
                    let retry_interval = cfg.retry_interval.0;
                    let tso_check = async move {
                        while let Some(cli) = client.upgrade() {
                            let closed_fut = cli.inner.rl().tso.closed();
                            closed_fut.await;
                            info!("TSO stream is closed, reconnect to PD");
                            while let Err(e) = cli.reconnect(true).await {
                                warn!("failed to update PD client"; "error"=> ?e);
                                let _ = GLOBAL_TIMER_HANDLE
                                    .delay(std::time::Instant::now() + retry_interval)
                                    .compat()
                                    .await;
                            }
                        }
                    };
                    rpc_client.monitor.spawn(tso_check);

                    if !cfg.force_legacy_tso {
                        // `new_tso_check`
                        let client = Arc::downgrade(&rpc_client.pd_client);
                        let new_tso_check = async move {
                            while let Some(cli) = client.upgrade() {
                                let tso_exists = cli.new_tso.rl().is_some();
                                if tso_exists {
                                    let closed_fut = cli.new_tso.rl().as_ref().unwrap().closed();
                                    closed_fut.await;
                                    // Reset the tso discover primary client, it's may be stale.
                                    cli.pd_connector.reset_tso_discover().await;
                                    info!("New TSO stream is closed, reconnect to PD");
                                }
                                // Try to build the new tso. If build failure, just update the
                                // client new_tso to None and wait for retry.
                                // NOTE: the new tso will be unavailable until next retry if build
                                // failure. It will use legacy tso instead.
                                let new_tso = cli.pd_connector.build_new_tso().await;
                                let build_success = new_tso.is_some();
                                cli.update_new_tso(new_tso);
                                if build_success {
                                    // new tso updated, wait for tso closed.
                                    info!("New TSO recovered, use new TSO instead of legacy TSO");
                                    continue;
                                }

                                warn!(
                                    "failed to update new TSO server, will fallback to legacy TSO"
                                );
                                let _ = GLOBAL_TIMER_HANDLE
                                    .delay(std::time::Instant::now() + duration)
                                    .compat()
                                    .await;
                            }
                        };
                        rpc_client.monitor.spawn(new_tso_check);
                    }
                    return Ok(rpc_client);
                }
                Err(e) => {
                    if i as usize % cfg.retry_log_every == 0 {
                        warn!("validate PD endpoints failed"; "err" => ?e);
                    }
                    let _ = GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + cfg.retry_interval.0)
                        .compat()
                        .await;
                }
            }
        }
        Err(box_err!("endpoints are invalid"))
    }

    /// Creates a new request header.
    fn header(&self) -> pdpb::RequestHeader {
        let mut header = pdpb::RequestHeader::default();
        header.set_cluster_id(self.cluster_id);
        header
    }

    /// Gets the leader of PD.
    pub fn get_leader(&self) -> Member {
        self.pd_client.get_leader()
    }

    /// Re-establishes connection with PD leader in synchronized fashion.
    pub fn reconnect(&self) -> Result<()> {
        block_on(self.pd_client.reconnect(true))
    }

    /// Gets given key's Region and Region's leader from PD.
    fn get_region_and_leader(
        &self,
        key: &[u8],
    ) -> PdFuture<(metapb::Region, Option<metapb::Peer>)> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_region"])
            .start_coarse_timer();

        let mut req = pdpb::GetRegionRequest::default();
        req.set_header(self.header());
        req.set_region_key(key.to_vec());

        let executor = move |client: &Client, req: pdpb::GetRegionRequest| {
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .get_region_async_opt(&req, call_option_inner(&inner))
                    .unwrap_or_else(|e| {
                        panic!("fail to request PD {} err {:?}", "get_region_async_opt", e)
                    })
            };

            Box::pin(async move {
                let mut resp = handler.await?;
                check_resp_header(resp.get_header())?;
                let region = if resp.has_region() {
                    resp.take_region()
                } else {
                    return Err(Error::RegionNotFound(req.region_key));
                };
                let leader = if resp.has_leader() {
                    Some(resp.take_leader())
                } else {
                    None
                };
                Ok((region, leader))
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn get_store_and_stats(&self, store_id: u64) -> PdFuture<(metapb::Store, pdpb::StoreStats)> {
        let timer = Instant::now();

        let mut req = pdpb::GetStoreRequest::default();
        req.set_header(self.header());
        req.set_store_id(store_id);

        let executor = move |client: &Client, req: pdpb::GetStoreRequest| {
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .get_store_async_opt(&req, call_option_inner(&inner))
                    .unwrap_or_else(|e| {
                        panic!("fail to request PD {} err {:?}", "get_store_async", e)
                    })
            };

            Box::pin(async move {
                let mut resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["get_store_async"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                let store = resp.take_store();
                if store.get_state() != metapb::StoreState::Tombstone {
                    Ok((store, resp.take_stats()))
                } else {
                    Err(Error::StoreTombstone(format!("{:?}", store)))
                }
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    // watch_gc_safepoint_v2 is used to request pd.
    fn watch_gc_safepoint_v2_from_pd(
        &self,
        revision: i64,
    ) -> Result<grpcio::ClientSStreamReceiver<WatchGcSafePointV2Response>> {
        use kvproto::pdpb::WatchGcSafePointV2Request;
        let mut req = WatchGcSafePointV2Request::default();
        info!("[gc safepoint watch] start watch gc safepoint v2"; "revision" => revision);
        req.set_revision(revision);
        sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client, _| {
            client.watch_gc_safe_point_v2(&req)
        })
    }

    // handle_watch_gc_safepoint_v2_stream is used to handle
    // ClientSStreamReceiver<WatchGcSafePointV2Response>.
    async fn handle_watch_gc_safepoint_v2_stream(
        &self,
        mut stream: ClientSStreamReceiver<WatchGcSafePointV2Response>,
    ) {
        while let Some(result) = stream.next().await {
            match result {
                Ok(r) => {
                    self.ks_gc_sp_revision
                        .store(r.get_revision(), Ordering::SeqCst);
                    self.handle_watch_gc_safepoint_v2_response(r);
                }
                Err(err) => {
                    error!("watch safe point v2 response error:{:?}", err);
                    let _ = GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + RETRY_INTERVAL)
                        .compat()
                        .await;
                }
            }
        }
    }

    // handle_watch_gc_safepoint_v2_response is used to handle
    // WatchGcSafePointV2Response.
    fn handle_watch_gc_safepoint_v2_response(&self, result: WatchGcSafePointV2Response) {
        result
            .get_events()
            .iter()
            .for_each(|item| match item.get_type() {
                EventType::Put => {
                    let keyspace_id = item.get_keyspace_id();
                    let gc_safe_point = item.get_safe_point();
                    self.ks_safepoint_v2.insert(keyspace_id, gc_safe_point);
                    info!(
                        "updated keyspace gc safe point, keyspace id:{}, gc safe point:{}",
                        keyspace_id, gc_safe_point
                    );
                }
                EventType::Delete => {
                    let keyspace_id = item.get_keyspace_id();
                    self.ks_safepoint_v2.remove(&keyspace_id);
                    info!(
                        "Removed keyspace gc safe point, keyspace id:{};",
                        keyspace_id
                    );
                }
            });
    }

    async fn load_all_gc_safe_point_v2(&self) {
        let mut req = pdpb::GetAllGcSafePointV2Request::default();
        req.set_header(self.header());

        let sync_resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client, option| {
            client.get_all_gc_safe_point_v2_opt(&req, option)
        });
        match sync_resp {
            Ok(mut resp) => {
                for gc_safe_point in resp.take_gc_safe_points().iter() {
                    self.ks_safepoint_v2
                        .insert(gc_safe_point.keyspace_id, gc_safe_point.gc_safe_point);
                    debug!(
                        "load keyspace gc safe point v2, update cache keyspace id:{}, gc safe point:{}",
                        gc_safe_point.keyspace_id, gc_safe_point.gc_safe_point
                    );
                }
                self.ks_gc_sp_revision
                    .store(resp.get_revision(), Ordering::SeqCst);
                debug!(
                    "load all keyspace gc safe point v2, revision:{}",
                    resp.get_revision()
                );
            }
            Err(err) => {
                error!("failed to load all gc safe point v2 {:?}", err);
            }
        }
    }
}

impl fmt::Debug for RpcClient {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("RpcClient")
            .field("cluster_id", &self.cluster_id)
            .field("leader", &self.get_leader())
            .finish()
    }
}

#[async_trait]
impl PdClient for RpcClient {
    fn load_global_config_by_names(&self, list: Vec<String>) -> PdFuture<HashMap<String, Vec<u8>>> {
        let mut req = LoadGlobalConfigRequest::new();
        req.set_names(list.into());
        self.load_global_config(req)
    }

    fn load_global_config_by_path(&self, path: String) -> PdFuture<HashMap<String, Vec<u8>>> {
        let mut req = LoadGlobalConfigRequest::new();
        req.set_config_path(path);
        self.load_global_config(req)
    }

    fn load_global_config(
        &self,
        req: LoadGlobalConfigRequest,
    ) -> PdFuture<HashMap<String, Vec<u8>>> {
        let executor = |client: &Client, req| match client
            .inner
            .rl()
            .client_stub
            .clone()
            .load_global_config_async(&req)
        {
            Ok(grpc_response) => Box::pin(async move {
                match grpc_response.await {
                    Ok(grpc_response) => {
                        let mut res = HashMap::with_capacity(grpc_response.get_items().len());
                        for c in grpc_response.get_items() {
                            if c.has_error() {
                                error!("failed to load global config with key {:?}", c.get_error());
                            } else {
                                res.insert(c.get_name().to_owned(), c.get_payload().to_owned());
                            }
                        }
                        Ok(res)
                    }
                    Err(err) => Err(box_err!("{:?}", err)),
                }
            }) as PdFuture<_>,
            Err(err) => Box::pin(async move { Err(box_err!("{:?}", err)) }) as PdFuture<_>,
        };
        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn watch_global_config(
        &self,
    ) -> Result<grpcio::ClientSStreamReceiver<pdpb::WatchGlobalConfigResponse>> {
        use kvproto::pdpb::WatchGlobalConfigRequest;
        let req = WatchGlobalConfigRequest::default();
        sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client, _| {
            client.watch_global_config(&req)
        })
    }

    // watch_gc_safepoint_v2 is used to start watch gc safepoint v2.
    async fn watch_gc_safepoint_v2(&self) {
        loop {
            self.load_all_gc_safe_point_v2().await;
            match self.watch_gc_safepoint_v2_from_pd(self.ks_gc_sp_revision.load(Ordering::SeqCst))
            {
                Ok(stream) => {
                    self.handle_watch_gc_safepoint_v2_stream(stream).await;
                    debug!("watch gc safe point v2 stream done.");
                }
                Err(Error::DataCompacted(msg)) => {
                    error!("watch gc safe point v2 required revision has been compacted"; "err" => ?msg);
                }
                Err(err) => {
                    error!("watch safe point v2 error:{:?}", err);
                    let _ = GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + RETRY_INTERVAL)
                        .compat()
                        .await;
                }
            }
        }
    }

    fn get_keyspace_gc_safepoint_v2_cache(&self) -> Arc<DashMap<u32, u64>> {
        self.ks_safepoint_v2.clone()
    }

    fn get_cluster_id(&self) -> Result<u64> {
        Ok(self.cluster_id)
    }

    fn bootstrap_cluster(
        &self,
        stores: metapb::Store,
        region: metapb::Region,
    ) -> Result<Option<ReplicationStatus>> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["bootstrap_cluster"])
            .start_coarse_timer();

        let mut req = pdpb::BootstrapRequest::default();
        req.set_header(self.header());
        req.set_store(stores);
        req.set_region(region);

        let mut resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client, option| {
            client.bootstrap_opt(&req, option)
        })?;
        check_resp_header(resp.get_header())?;
        Ok(resp.replication_status.take())
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["is_cluster_bootstrapped"])
            .start_coarse_timer();

        let mut req = pdpb::IsBootstrappedRequest::default();
        req.set_header(self.header());

        let resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client, option| {
            client.is_bootstrapped_opt(&req, option)
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.get_bootstrapped())
    }

    fn alloc_id(&self) -> Result<u64> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["alloc_id"])
            .start_coarse_timer();

        let mut req = pdpb::AllocIdRequest::default();
        req.set_header(self.header());

        let resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client, option| {
            client.alloc_id_opt(&req, option)
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.get_id())
    }

    fn put_store(&self, store: metapb::Store) -> Result<Option<ReplicationStatus>> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["put_store"])
            .start_coarse_timer();

        let mut req = pdpb::PutStoreRequest::default();
        req.set_header(self.header());
        req.set_store(store);

        let mut resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client, option| {
            client.put_store_opt(&req, option)
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.replication_status.take())
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_store"])
            .start_coarse_timer();

        let mut req = pdpb::GetStoreRequest::default();
        req.set_header(self.header());
        req.set_store_id(store_id);

        let mut resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client, option| {
            client.get_store_opt(&req, option)
        })?;
        check_resp_header(resp.get_header())?;

        let store = resp.take_store();
        if store.get_state() != metapb::StoreState::Tombstone {
            Ok(store)
        } else {
            Err(Error::StoreTombstone(format!("{:?}", store)))
        }
    }

    fn get_store_async(&self, store_id: u64) -> PdFuture<metapb::Store> {
        self.get_store_and_stats(store_id).map_ok(|x| x.0).boxed()
    }

    fn get_all_stores(&self, exclude_tombstone: bool) -> Result<Vec<metapb::Store>> {
        block_on(self.get_all_stores_async(exclude_tombstone))
    }

    fn get_all_stores_async(&self, exclude_tombstone: bool) -> PdFuture<Vec<metapb::Store>> {
        let timer = Instant::now();

        let mut req = pdpb::GetAllStoresRequest::default();
        req.set_header(self.header());
        req.set_exclude_tombstone_stores(exclude_tombstone);

        let executor = move |client: &Client, req: pdpb::GetAllStoresRequest| {
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .get_all_stores_async_opt(&req, call_option_inner(&inner))
                    .unwrap_or_else(|e| {
                        panic!(
                            "fail to request PD {} err {:?}",
                            "get_all_stores_async_opt", e
                        )
                    })
            };

            Box::pin(async move {
                let mut resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["get_all_stores_async"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(resp.take_stores().into())
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_cluster_config"])
            .start_coarse_timer();

        let mut req = pdpb::GetClusterConfigRequest::default();
        req.set_header(self.header());

        let mut resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client, option| {
            client.get_cluster_config_opt(&req, option)
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.take_cluster())
    }

    fn get_region(&self, key: &[u8]) -> Result<metapb::Region> {
        block_on(self.get_region_and_leader(key)).map(|x| x.0)
    }

    fn get_region_info(&self, key: &[u8]) -> Result<RegionInfo> {
        block_on(self.get_region_and_leader(key)).map(|x| RegionInfo::new(x.0, x.1))
    }

    fn get_region_async<'k>(&'k self, key: &'k [u8]) -> BoxFuture<'k, Result<metapb::Region>> {
        self.get_region_and_leader(key).map_ok(|x| x.0).boxed()
    }

    fn get_region_info_async<'k>(&'k self, key: &'k [u8]) -> BoxFuture<'k, Result<RegionInfo>> {
        self.get_region_and_leader(key)
            .map_ok(|x| RegionInfo::new(x.0, x.1))
            .boxed()
    }

    fn get_region_by_id(&self, region_id: u64) -> PdFuture<Option<metapb::Region>> {
        let timer = Instant::now();

        let mut req = pdpb::GetRegionByIdRequest::default();
        req.set_header(self.header());
        req.set_region_id(region_id);

        let executor = move |client: &Client, req: pdpb::GetRegionByIdRequest| {
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .get_region_by_id_async_opt(&req, call_option_inner(&inner))
                    .unwrap_or_else(|e| {
                        panic!("fail to request PD {} err {:?}", "get_region_by_id", e);
                    })
            };
            Box::pin(async move {
                let mut resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["get_region_by_id"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                if resp.has_region() {
                    Ok(Some(resp.take_region()))
                } else {
                    Ok(None)
                }
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn get_region_leader_by_id(
        &self,
        region_id: u64,
    ) -> PdFuture<Option<(metapb::Region, metapb::Peer)>> {
        let timer = Instant::now();

        let mut req = pdpb::GetRegionByIdRequest::default();
        req.set_header(self.header());
        req.set_region_id(region_id);

        let executor = move |client: &Client, req: pdpb::GetRegionByIdRequest| {
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .get_region_by_id_async_opt(&req, call_option_inner(&inner))
                    .unwrap_or_else(|e| {
                        panic!(
                            "fail to request PD {} err {:?}",
                            "get_region_leader_by_id", e
                        )
                    })
            };
            Box::pin(async move {
                let mut resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["get_region_leader_by_id"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                if resp.has_region() && resp.has_leader() {
                    Ok(Some((resp.take_region(), resp.take_leader())))
                } else {
                    Ok(None)
                }
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn region_heartbeat(
        &self,
        term: u64,
        region: metapb::Region,
        leader: metapb::Peer,
        region_stat: RegionStat,
        replication_status: Option<RegionReplicationStatus>,
    ) -> PdFuture<()> {
        PD_HEARTBEAT_COUNTER_VEC.with_label_values(&["send"]).inc();

        let mut req = pdpb::RegionHeartbeatRequest::default();
        req.set_term(term);
        req.set_header(self.header());
        req.set_region(region);
        req.set_leader(leader);
        req.set_down_peers(region_stat.down_peers.into());
        req.set_pending_peers(region_stat.pending_peers.into());
        req.set_bytes_written(region_stat.written_bytes);
        req.set_keys_written(region_stat.written_keys);
        req.set_bytes_read(region_stat.read_bytes);
        req.set_keys_read(region_stat.read_keys);
        req.set_query_stats(region_stat.query_stats);
        req.set_approximate_size(region_stat.approximate_size);
        req.set_approximate_keys(region_stat.approximate_keys);
        req.set_approximate_kv_size(region_stat.approximate_kv_size);
        req.set_approximate_columnar_kv_size(region_stat.approximate_columnar_kv_size);
        req.set_cpu_usage(region_stat.cpu_usage);
        if let Some(s) = replication_status {
            req.set_replication_status(s);
        }
        let mut interval = pdpb::TimeInterval::default();
        interval.set_start_timestamp(region_stat.last_report_ts.into_inner());
        interval.set_end_timestamp(UnixSecs::now().into_inner());
        req.set_interval(interval);

        let executor = |client: &Client, req: pdpb::RegionHeartbeatRequest| {
            let mut inner = client.inner.wl();
            if let Either::Left(ref mut left) = inner.hb_sender {
                debug!("heartbeat sender is refreshed");
                let sender = left.take().expect("expect region heartbeat sink");
                let (tx, rx) = mpsc::unbounded();
                let pending_heartbeat = Arc::new(AtomicU64::new(0));
                inner.hb_sender = Either::Right(tx);
                inner.pending_heartbeat = pending_heartbeat.clone();
                inner.client_stub.spawn(async move {
                    let mut sender = sender.sink_map_err(Error::Grpc);
                    let mut last_report = u64::MAX;
                    let result = sender
                        .send_all(&mut rx.map(|r| {
                            let last = pending_heartbeat.fetch_sub(1, Ordering::Relaxed);
                            // Sender will update pending at every send operation, so as long as
                            // pending task is increasing, pending count should be reported by
                            // sender.
                            if last + 10 < last_report || last == 1 {
                                PD_PENDING_HEARTBEAT_GAUGE.set(last as i64 - 1);
                                last_report = last;
                            }
                            if last > last_report {
                                last_report = last - 1;
                            }
                            fail::fail_point!("region_heartbeat_send_failed", |_| {
                                Err(Error::Grpc(grpcio::Error::RemoteStopped))
                            });
                            Ok((r, WriteFlags::default()))
                        }))
                        .await;
                    match result {
                        Ok(()) => {
                            sender.get_mut().cancel();
                            info!("cancel region heartbeat sender");
                        }
                        Err(e) => {
                            error!(?e; "failed to send heartbeat");
                        }
                    };
                });
            }

            let last = inner.pending_heartbeat.fetch_add(1, Ordering::Relaxed);
            PD_PENDING_HEARTBEAT_GAUGE.set(last as i64 + 1);
            let sender = inner
                .hb_sender
                .as_mut()
                .right()
                .expect("expect region heartbeat sender");
            let ret = sender
                .unbounded_send(req)
                .map_err(|e| Error::StreamDisconnect(e.into_send_error()));

            Box::pin(future::ready(ret)) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn handle_region_heartbeat_response(
        &self,
        _: u64,
        f: Box<dyn Fn(pdpb::RegionHeartbeatResponse) + Send + 'static>,
    ) -> PdFuture<()> {
        self.pd_client.handle_region_heartbeat_response(f)
    }

    fn ask_split(&self, region: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
        let timer = Instant::now();

        let mut req = pdpb::AskSplitRequest::default();
        req.set_header(self.header());
        req.set_region(region);

        let executor = move |client: &Client, req: pdpb::AskSplitRequest| {
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .ask_split_async_opt(&req, call_option_inner(&inner))
                    .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "ask_split", e))
            };

            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["ask_split"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(resp)
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn ask_batch_split(
        &self,
        region: metapb::Region,
        count: usize,
    ) -> PdFuture<pdpb::AskBatchSplitResponse> {
        let timer = Instant::now();

        let mut req = pdpb::AskBatchSplitRequest::default();
        req.set_header(self.header());
        req.set_region(region);
        req.set_split_count(count as u32);

        let executor = move |client: &Client, req: pdpb::AskBatchSplitRequest| {
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .ask_batch_split_async_opt(&req, call_option_inner(&inner))
                    .unwrap_or_else(|e| {
                        panic!("fail to request PD {} err {:?}", "ask_batch_split", e)
                    })
            };

            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["ask_batch_split"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(resp)
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn store_heartbeat(
        &self,
        mut stats: pdpb::StoreStats,
        store_report: Option<pdpb::StoreReport>,
        dr_autosync_status: Option<StoreDrAutoSyncStatus>,
    ) -> PdFuture<pdpb::StoreHeartbeatResponse> {
        let timer = Instant::now();

        let mut req = pdpb::StoreHeartbeatRequest::default();
        req.set_header(self.header());
        stats
            .mut_interval()
            .set_end_timestamp(UnixSecs::now().into_inner());
        req.set_stats(stats);
        if let Some(report) = store_report {
            req.set_store_report(report);
        }
        if let Some(status) = dr_autosync_status {
            req.set_dr_autosync_status(status);
        }
        let executor = move |client: &Client, req: pdpb::StoreHeartbeatRequest| {
            let feature_gate = client.feature_gate.clone();
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .store_heartbeat_async_opt(&req, call_option_inner(&inner))
                    .unwrap_or_else(|e| {
                        panic!("fail to request PD {} err {:?}", "store_heartbeat", e)
                    })
            };
            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["store_heartbeat"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                match feature_gate.set_version(resp.get_cluster_version()) {
                    Err(_) => warn!("invalid cluster version: {}", resp.get_cluster_version()),
                    Ok(true) => info!("set cluster version to {}", resp.get_cluster_version()),
                    _ => {}
                };
                Ok(resp)
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn report_batch_split(&self, regions: Vec<metapb::Region>) -> PdFuture<()> {
        let timer = Instant::now();

        let mut req = pdpb::ReportBatchSplitRequest::default();
        req.set_header(self.header());
        req.set_regions(regions.into());

        let executor = move |client: &Client, req: pdpb::ReportBatchSplitRequest| {
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .report_batch_split_async_opt(&req, call_option_inner(&inner))
                    .unwrap_or_else(|e| {
                        panic!("fail to request PD {} err {:?}", "report_batch_split", e)
                    })
            };
            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["report_batch_split"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(())
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn scatter_region(&self, mut region: RegionInfo) -> Result<()> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["scatter_region"])
            .start_coarse_timer();

        let mut req = pdpb::ScatterRegionRequest::default();
        req.set_header(self.header());
        req.set_region_id(region.get_id());
        if let Some(leader) = region.leader.take() {
            req.set_leader(leader);
        }
        req.set_region(region.region);

        let resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client, option| {
            client.scatter_region_opt(&req, option)
        })?;
        check_resp_header(resp.get_header())
    }

    fn scatter_regions_by_id(&self, regions_id: Vec<u64>) -> Result<()> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["scatter_regions_by_id"])
            .start_coarse_timer();

        let mut req = pdpb::ScatterRegionRequest::default();
        req.set_header(self.header());
        req.set_regions_id(regions_id);

        let resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client, option| {
            client.scatter_region_opt(&req, option)
        })?;
        check_resp_header(resp.get_header())
    }

    fn handle_reconnect(&self, f: Box<dyn Fn() + Sync + Send + 'static>) {
        self.pd_client.on_reconnect(Box::new(f))
    }

    fn get_gc_safe_point(&self) -> PdFuture<u64> {
        let timer = Instant::now();

        let mut req = pdpb::GetGcSafePointRequest::default();
        req.set_header(self.header());

        let executor = move |client: &Client, req: pdpb::GetGcSafePointRequest| {
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .get_gc_safe_point_async_opt(&req, call_option_inner(&inner))
                    .unwrap_or_else(|e| {
                        panic!("fail to request PD {} err {:?}", "get_gc_safe_point", e)
                    })
            };
            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["get_gc_safe_point"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(resp.get_safe_point())
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn get_store_stats_async(&self, store_id: u64) -> BoxFuture<'_, Result<pdpb::StoreStats>> {
        self.get_store_and_stats(store_id).map_ok(|x| x.1).boxed()
    }

    fn get_operator(&self, region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_operator"])
            .start_coarse_timer();

        let mut req = pdpb::GetOperatorRequest::default();
        req.set_header(self.header());
        req.set_region_id(region_id);

        let resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client, option| {
            client.get_operator_opt(&req, option)
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp)
    }

    fn batch_get_tso(&self, count: u32) -> PdFuture<TimeStamp> {
        let begin = Instant::now();
        let executor = move |client: &Client, _| {
            // Remove Box::pin and Compat when GLOBAL_TIMER_HANDLE supports futures 0.3
            let new_tso_guard = client.new_tso.rl();
            let ts_fut = Compat::new(Box::pin(if new_tso_guard.is_some() {
                new_tso_guard.as_ref().unwrap().get_timestamp(count)
            } else {
                drop(new_tso_guard);
                client.inner.rl().tso.get_timestamp(count)
            }));
            let with_timeout = GLOBAL_TIMER_HANDLE
                .timeout(
                    ts_fut,
                    std::time::Instant::now() + Duration::from_secs(REQUEST_TIMEOUT),
                )
                .compat();
            Box::pin(async move {
                let ts = with_timeout.await.map_err(|e| {
                    if let Some(inner) = e.into_inner() {
                        inner
                    } else {
                        box_err!("get timestamp timeout")
                    }
                })?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["tso"])
                    .observe(duration_to_sec(begin.saturating_elapsed()));
                Ok(ts)
            }) as PdFuture<_>
        };
        self.pd_client
            .request((), executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn get_min_tso(&self) -> PdFuture<TimeStamp> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_min_tso"])
            .start_coarse_timer();

        let mut req: pdpb::GetMinTsRequest = pdpb::GetMinTsRequest::default();
        req.set_header(self.header());

        let executor = move |client: &Client, req: pdpb::GetMinTsRequest| {
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .get_min_ts_async_opt(&req, call_option_inner(&inner))
                    .unwrap_or_else(|e| {
                        panic!("fail to request PD {} err {:?}", "get_min_ts_async_opt", e)
                    })
            };

            Box::pin(async move {
                let resp = handler.await?;
                check_resp_header(resp.get_header())?;
                let ts: TimeStamp = TimeStamp::compose(
                    resp.get_timestamp().get_physical().try_into().unwrap(),
                    resp.get_timestamp().get_logical().try_into().unwrap(),
                );
                Ok(ts)
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn update_service_safe_point(
        &self,
        name: String,
        safe_point: TimeStamp,
        ttl: Duration,
    ) -> PdFuture<()> {
        let begin = Instant::now();
        let mut req = pdpb::UpdateServiceGcSafePointRequest::default();
        req.set_header(self.header());
        req.set_service_id(name.into());
        req.set_ttl(ttl.as_secs() as _);
        req.set_safe_point(safe_point.into_inner());
        let executor = move |client: &Client, r: pdpb::UpdateServiceGcSafePointRequest| {
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .update_service_gc_safe_point_async_opt(&r, call_option_inner(&inner))
                    .unwrap_or_else(|e| {
                        panic!(
                            "fail to request PD {} err {:?}",
                            "update_service_safe_point", e
                        )
                    })
            };
            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["update_service_safe_point"])
                    .observe(duration_to_sec(begin.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                crate::check_update_service_safe_point_resp(&resp, safe_point.into_inner())?;
                Ok(())
            }) as PdFuture<_>
        };
        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn update_keyspace_service_safe_point(
        &self,
        keyspace_id: u32,
        name: String,
        safe_point: TimeStamp,
        ttl: Duration,
    ) -> PdFuture<u64 /* current_min_safe_point */> {
        let begin = Instant::now();
        let ttl_secs = if ttl.is_zero() {
            // Use -1 to remove the keyspace.
            // Ref: https://github.com/pingcap/kvproto/blob/master/proto/pdpb.proto, UpdateServiceSafePointV2Request
            -1
        } else {
            ttl.as_secs() as i64
        };
        let mut req = pdpb::UpdateServiceSafePointV2Request::default();
        req.set_header(self.header());
        req.set_keyspace_id(keyspace_id);
        req.set_service_id(name.into());
        req.set_ttl(ttl_secs);
        req.set_safe_point(safe_point.into_inner());
        let executor = move |client: &Client, r: pdpb::UpdateServiceSafePointV2Request| {
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .update_service_safe_point_v2_async_opt(&r, call_option_inner(&inner))
                    .unwrap_or_else(|e| {
                        panic!(
                            "fail to request PD {} err {:?}",
                            "update_service_safe_point_v2_async", e
                        )
                    })
            };
            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["update_service_safe_point_v2_async"])
                    .observe(duration_to_sec(begin.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                crate::check_update_keyspace_service_safe_point_resp(
                    &resp,
                    safe_point.into_inner(),
                )?;
                Ok(resp.min_safe_point)
            }) as PdFuture<_>
        };
        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn feature_gate(&self) -> &FeatureGate {
        &self.pd_client.feature_gate
    }

    fn report_min_resolved_ts(&self, store_id: u64, min_resolved_ts: u64) -> PdFuture<()> {
        let timer = Instant::now();

        let mut req = pdpb::ReportMinResolvedTsRequest::default();
        req.set_header(self.header());
        req.set_store_id(store_id);
        req.set_min_resolved_ts(min_resolved_ts);

        let executor = move |client: &Client, req: pdpb::ReportMinResolvedTsRequest| {
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .report_min_resolved_ts_async_opt(&req, call_option_inner(&inner))
                    .unwrap_or_else(|e| {
                        panic!("fail to request PD {} err {:?}", "min_resolved_ts", e)
                    })
            };
            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["min_resolved_ts"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(())
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn report_region_buckets(&self, bucket_stat: &BucketStat, period: Duration) -> PdFuture<()> {
        PD_BUCKETS_COUNTER_VEC.with_label_values(&["send"]).inc();

        let mut buckets = metapb::Buckets::default();
        buckets.set_region_id(bucket_stat.meta.region_id);
        buckets.set_version(bucket_stat.meta.version);
        buckets.set_keys(bucket_stat.meta.keys.clone().into());
        buckets.set_period_in_ms(period.as_millis() as u64);
        buckets.set_stats(bucket_stat.stats.clone());
        let mut req = pdpb::ReportBucketsRequest::default();
        req.set_header(self.header());
        req.set_buckets(buckets);
        req.set_region_epoch(bucket_stat.meta.region_epoch.clone());

        let executor = |client: &Client, req: pdpb::ReportBucketsRequest| {
            let mut inner = client.inner.wl();
            if let Either::Left(ref mut left) = inner.buckets_sender {
                debug!("region buckets sender is refreshed");
                let sender = left.take().expect("expect report region buckets sink");
                let (tx, rx) = mpsc::unbounded();
                let pending_buckets = Arc::new(AtomicU64::new(0));
                inner.buckets_sender = Either::Right(tx);
                inner.pending_buckets = pending_buckets.clone();
                let resp = inner.buckets_resp.take().unwrap();
                inner.client_stub.spawn(async {
                    let res = resp.await;
                    warn!("region buckets stream exited: {:?}", res);
                });
                inner.client_stub.spawn(async move {
                    let mut sender = sender.sink_map_err(Error::Grpc);
                    let mut last_report = u64::MAX;
                    let result = sender
                        .send_all(&mut rx.map(|r| {
                            let last = pending_buckets.fetch_sub(1, Ordering::Relaxed);
                            // Sender will update pending at every send operation, so as long as
                            // pending task is increasing, pending count should be reported by
                            // sender.
                            if last + 10 < last_report || last == 1 {
                                PD_PENDING_BUCKETS_GAUGE.set(last as i64 - 1);
                                last_report = last;
                            }
                            if last > last_report {
                                last_report = last - 1;
                            }
                            Ok((r, WriteFlags::default()))
                        }))
                        .await;
                    match result {
                        Ok(()) => {
                            sender.get_mut().cancel();
                            info!("cancel region buckets sender");
                        }
                        Err(e) => {
                            error!(?e; "failed to send region buckets");
                        }
                    };
                });
            }

            let last = inner.pending_buckets.fetch_add(1, Ordering::Relaxed);
            PD_PENDING_BUCKETS_GAUGE.set(last as i64 + 1);
            let sender = inner
                .buckets_sender
                .as_mut()
                .right()
                .expect("expect region buckets sender");
            let ret = sender
                .unbounded_send(req)
                .map_err(|e| Error::StreamDisconnect(e.into_send_error()));
            Box::pin(future::ready(ret)) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn split_regions_opt(
        &self,
        keys: Vec<Vec<u8>>,
        request_timeout: Duration,
        retry_limit: usize,
    ) -> PdFuture<(Vec<u64>, u64 /* finished_percent */)> {
        let timer = Instant::now();
        let mut req = pdpb::SplitRegionsRequest::default();
        req.set_header(self.header());
        req.set_split_keys(keys.into());
        req.set_retry_limit(retry_limit as u64); // PD server side retry when not all keys are split.
        let executor = move |client: &Client, req: pdpb::SplitRegionsRequest| {
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .split_regions_async_opt(
                        &req,
                        call_option_inner(&inner).timeout(request_timeout),
                    )
                    .unwrap_or_else(|e| {
                        panic!("fail to request PD {} err {:?}", "split regions", e)
                    })
            };
            Box::pin(async move {
                let mut resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["split_regions"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                Ok((resp.take_regions_id(), resp.finished_percentage))
            }) as PdFuture<_>
        };
        self.pd_client.request(req, executor, retry_limit).execute()
    }

    fn split_and_scatter_regions(&self, keys: Vec<Vec<u8>>) -> PdFuture<()> {
        let timer = Instant::now();
        let mut req = pdpb::SplitAndScatterRegionsRequest::default();
        req.set_header(self.header());
        req.set_split_keys(keys.into());
        req.set_retry_limit(LEADER_CHANGE_RETRY as u64);
        let executor = move |client: &Client, req: pdpb::SplitAndScatterRegionsRequest| {
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .split_and_scatter_regions_async_opt(&req, call_option_inner(&inner))
                    .unwrap_or_else(|e| {
                        panic!(
                            "fail to request PD {} err {:?}",
                            "split and scatter regions", e
                        )
                    })
            };
            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["split_and_scatter_regions"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(())
            }) as PdFuture<_>
        };
        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn scan_regions(
        &self,
        key: Vec<u8>,
        end_key: Vec<u8>,
        limit: usize,
    ) -> PdFuture<Vec<pdpb::Region>> {
        let timer = Instant::now();
        let mut req = pdpb::ScanRegionsRequest::default();
        req.set_header(self.header());
        req.set_start_key(key);
        req.set_end_key(end_key);
        req.set_limit(limit as i32);
        let executor = move |client: &Client, req: pdpb::ScanRegionsRequest| {
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .scan_regions_async_opt(&req, call_option_inner(&inner))
                    .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "scan regions", e))
            };
            Box::pin(async move {
                let mut resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["scan_regions"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(resp.take_regions().into())
            }) as PdFuture<_>
        };
        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn get_keyspace_encryption(&self, keyspace_id: u32) -> Result<KeyspaceEncryptionConfig> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_all_keyspace"])
            .start_coarse_timer();
        let mut req = keyspacepb::GetAllKeyspacesRequest::default();
        req.set_header(self.header());
        req.set_start_id(keyspace_id);
        req.set_limit(1);
        let resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client, option| {
            let keyspace_client = KeyspaceClient::new(client.client.channel().clone());
            keyspace_client.get_all_keyspaces_opt(&req, option)
        })?;
        check_resp_header(resp.get_header())?;
        if let Some(keyspace_meta) = resp.get_keyspaces().first() {
            if let Some(encryption_cfg) = keyspace_meta.get_config().get("encryption") {
                let cfg: KeyspaceEncryptionConfig =
                    serde_json::from_str(encryption_cfg).unwrap_or_default();
                info!("got keyspace {} encryption config {:?}", keyspace_id, cfg);
                return Ok(cfg);
            }
        }
        Ok(KeyspaceEncryptionConfig::default())
    }

    fn load_keyspace(&self, keyspace_name: String) -> Result<kvproto::keyspacepb::KeyspaceMeta> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["load_keyspace"])
            .start_coarse_timer();
        let mut req = keyspacepb::LoadKeyspaceRequest::default();
        req.set_header(self.header());
        req.set_name(keyspace_name);
        let mut resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client, option| {
            let keyspace_client = KeyspaceClient::new(client.client.channel().clone());
            keyspace_client.load_keyspace_opt(&req, option)
        })?;
        check_resp_header(resp.get_header())?;
        Ok(resp.take_keyspace())
    }

    /// Get buckets stat by region_id.
    ///
    /// Note: `BucketStat.meta.sizes` is empty.
    fn get_buckets_async(&self, region_id: u64) -> PdFuture<Option<BucketStat>> {
        let timer = Instant::now();

        let mut req = pdpb::GetRegionByIdRequest::default();
        req.set_header(self.header());
        req.set_region_id(region_id);
        req.set_need_buckets(true);

        let executor = move |client: &Client, req: pdpb::GetRegionByIdRequest| {
            let handler = {
                let inner = client.inner.rl();
                inner
                    .client_stub
                    .get_region_by_id_async_opt(&req, call_option_inner(&inner))
                    .unwrap_or_else(|e| {
                        panic!("fail to request PD {} err {:?}", "get_region_by_id", e);
                    })
            };
            Box::pin(async move {
                let mut resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["get_buckets"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                if resp.has_buckets() {
                    let mut region = resp.take_region();
                    let mut buckets = resp.take_buckets();
                    let stats = buckets.take_stats();
                    let meta = BucketMeta {
                        region_id: buckets.get_region_id(),
                        version: buckets.get_version(),
                        region_epoch: region.take_region_epoch(),
                        keys: buckets.take_keys().into(),
                        sizes: vec![], // Note: no `sizes` in PD.
                    };
                    let bucket_stat = BucketStat {
                        meta: Arc::new(meta),
                        stats,
                        create_time: Instant::now()
                            - Duration::from_millis(buckets.get_period_in_ms()),
                    };
                    Ok(Some(bucket_stat))
                } else {
                    Ok(None)
                }
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    /// Get buckets stat by region_id.
    ///
    /// Note: BucketStat.meta.sizes is empty.
    fn get_buckets(&self, region_id: u64) -> Option<BucketStat> {
        block_on(self.get_buckets_async(region_id)).unwrap()
    }
}
