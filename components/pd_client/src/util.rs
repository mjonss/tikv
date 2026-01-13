// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Cow,
    error, fmt,
    pin::Pin,
    sync::{Arc, RwLock, atomic::AtomicU64},
    thread,
    time::Duration,
};

use collections::HashSet;
use fail::fail_point;
use futures::{
    channel::mpsc::UnboundedSender,
    compat::Future01CompatExt,
    executor::block_on,
    future::{self, TryFutureExt},
    stream::{Stream, TryStreamExt},
    task::{Context, Poll, Waker},
};
use grpcio::{
    CallOption, ChannelBuilder, ClientCStreamReceiver, ClientDuplexReceiver, ClientDuplexSender,
    Environment, Error::RpcFailure, MetadataBuilder, Result as GrpcResult, RpcStatusCode,
};
use kvproto::{
    metapb::{self, BucketStats, Store},
    pdpb,
    pdpb::{
        ErrorType, GetClusterInfoRequest, GetMembersRequest, GetMembersResponse, Member,
        PdClient as PdClientStub, RegionHeartbeatRequest, RegionHeartbeatResponse,
        ReportBucketsRequest, ReportBucketsResponse, ResponseHeader,
    },
    tsopb::{FindGroupByKeyspaceIdRequest, KeyspaceGroup, TsoClient as TsoClientStub},
};
use log_wrappers::Value;
use security::SecurityManager;
use tikv_util::{
    Either, HandyRwLock, box_err,
    codec::bytes::{decode_bytes, encode_bytes},
    debug, error, info, slow_log,
    time::Instant,
    timer::GLOBAL_TIMER_HANDLE,
    warn,
};
use tokio::sync::Mutex;
use tokio_timer::timer::Handle;

use super::{
    BucketMeta, BucketStat, Config, Error, FeatureGate, PdClient, PdFuture, REQUEST_TIMEOUT,
    Result, metrics::*, tso::TimestampOracle,
};

const RETRY_INTERVAL: Duration = Duration::from_secs(1); // 1s
const MAX_RETRY_TIMES: u64 = 5;
// The max duration when retrying to connect to leader. No matter if the
// MAX_RETRY_TIMES is reached.
const MAX_RETRY_DURATION: Duration = Duration::from_secs(10);

// FIXME: Use a request-independent way to handle reconnection.
const GLOBAL_RECONNECT_INTERVAL: Duration = Duration::from_millis(100); // 0.1s
pub const REQUEST_RECONNECT_INTERVAL: Duration = Duration::from_secs(1); // 1s

#[derive(Clone)]
pub struct TargetInfo {
    target_url: String,
    via: String,
}

impl TargetInfo {
    pub(crate) fn new(target_url: String, via: &str) -> TargetInfo {
        TargetInfo {
            target_url,
            via: trim_http_prefix(via).to_string(),
        }
    }

    pub fn direct_connected(&self) -> bool {
        self.via.is_empty()
    }

    pub fn call_option(&self) -> CallOption {
        let opt = CallOption::default();
        if self.via.is_empty() {
            return opt;
        }

        let mut builder = MetadataBuilder::with_capacity(1);
        builder
            .add_str("pd-forwarded-host", &self.target_url)
            .unwrap();
        let metadata = builder.build();
        opt.headers(metadata)
    }
}

pub struct Inner {
    pub hb_sender: Either<
        Option<ClientDuplexSender<RegionHeartbeatRequest>>,
        UnboundedSender<RegionHeartbeatRequest>,
    >,
    pub hb_receiver: Either<Option<ClientDuplexReceiver<RegionHeartbeatResponse>>, Waker>,
    pub buckets_sender: Either<
        Option<ClientDuplexSender<ReportBucketsRequest>>,
        UnboundedSender<ReportBucketsRequest>,
    >,
    pub buckets_resp: Option<ClientCStreamReceiver<ReportBucketsResponse>>,
    pub client_stub: PdClientStub,
    target: TargetInfo,
    members: GetMembersResponse,
    pub security_mgr: Arc<SecurityManager>,
    on_reconnect: Option<Box<dyn Fn() + Sync + Send + 'static>>,
    pub pending_heartbeat: Arc<AtomicU64>,
    pub pending_buckets: Arc<AtomicU64>,
    pub tso: TimestampOracle,

    last_try_reconnect: Instant,
}

impl Inner {
    pub fn target_info(&self) -> &TargetInfo {
        &self.target
    }
}

pub struct HeartbeatReceiver {
    receiver: Option<ClientDuplexReceiver<RegionHeartbeatResponse>>,
    inner: Arc<Client>,
}

impl Stream for HeartbeatReceiver {
    type Item = Result<RegionHeartbeatResponse>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(ref mut receiver) = self.receiver {
                match Pin::new(receiver).poll_next(cx) {
                    Poll::Ready(Some(Ok(item))) => return Poll::Ready(Some(Ok(item))),
                    Poll::Pending => return Poll::Pending,
                    // If it's None or there's error, we need to update receiver.
                    _ => {}
                }
            }

            self.receiver.take();

            let mut inner = self.inner.inner.wl();
            let mut receiver = None;
            if let Either::Left(ref mut recv) = inner.hb_receiver {
                receiver = recv.take();
            }
            if receiver.is_some() {
                debug!("heartbeat receiver is refreshed");
                drop(inner);
                self.receiver = receiver;
            } else {
                inner.hb_receiver = Either::Right(cx.waker().clone());
                return Poll::Pending;
            }
        }
    }
}

pub(crate) struct TsoServiceDiscovery {
    cluster_id: u64,
    keyspace_id: u32, // use 0 as default keyspace group
    addrs: Vec<String>,
    selected_idx: usize,
    failure_count: usize,
    primary_addr: String,
    primary_tso_client: Option<TsoClientStub>,
    pd_client: PdClientStub,
    call_option: CallOption,
}

impl TsoServiceDiscovery {
    fn new(
        cluster_id: u64,
        keyspace_id: u32,
        addrs: Vec<String>,
        pd_client: PdClientStub,
        call_option: CallOption,
    ) -> TsoServiceDiscovery {
        TsoServiceDiscovery {
            cluster_id,
            keyspace_id,
            addrs,
            primary_addr: String::new(),
            primary_tso_client: None,
            selected_idx: 0,
            failure_count: 0,
            pd_client,
            call_option,
        }
    }

    pub(crate) fn get_cluster_id(&self) -> u64 {
        self.cluster_id
    }

    pub(crate) async fn get_tso_client(
        &mut self,
        connector: &PdConnector,
    ) -> Result<TsoClientStub> {
        if self.primary_tso_client.is_none() {
            self.update_member(connector).await?;
        }
        Ok(self.primary_tso_client.as_ref().unwrap().clone())
    }

    async fn get_tso_urls(&mut self) -> Result<Vec<String>> {
        let req = GetClusterInfoRequest::default();
        let mut resp = self
            .pd_client
            .get_cluster_info_async_opt(&req, self.call_option.clone())?
            .await?;
        if resp.get_header().has_error() {
            return Err(box_err!(
                "failed to get cluster info: {:?}",
                resp.get_header().get_error()
            ));
        }
        let urls = resp.take_tso_urls().into_vec();
        if urls.is_empty() {
            self.reset_primary_tso_client();
            return Err(Error::TsoServerNotFound);
        }
        Ok(urls)
    }

    async fn get_tso_server(&mut self) -> Result<String> {
        if self.addrs.is_empty() || self.failure_count >= self.addrs.len() {
            self.addrs = self.get_tso_urls().await?;
            self.failure_count = 0;
            self.selected_idx = 0;
            debug!("update tso server addrs, {:?}", self.addrs);
        }

        // `self.addrs` may be updated in update_member, so the selected_idx may be
        // invalid. We need modulus here.
        let idx = self.selected_idx % self.addrs.len();
        self.selected_idx = (self.selected_idx + 1) % self.addrs.len();
        Ok(self.addrs[idx].clone())
    }

    pub(crate) async fn update_member(&mut self, connector: &PdConnector) -> Result<bool> {
        let mut tso_server = self.get_tso_server().await?;

        // Find keyspace group by default keyspace id.
        // `find_group_by_keyspace_id` may be return error if some tso server is
        // unavailable, so we need to retry to all tso servers until find a
        // available tso server.
        let mut keyspace_group = None;
        for _ in 0..self.addrs.len() {
            info!(
                "pd_client: try to update member from tso server {:?}",
                tso_server
            );
            if let Ok(group) = self.find_group_by_keyspace_id(&tso_server, connector).await {
                keyspace_group = Some(group);
                break;
            }
            // If all tso servers fails, `get_tso_server()` will get tso urls from pd.
            self.failure_count += 1;
            // Try to use next tso server to find keyspace group.
            tso_server = self.get_tso_server().await?;
        }
        // All tso servers are unavailable, return TsoServerNotFound error.
        // NOTE: This will result to pd_client use the legacy tso mode. If tso server
        // comes backup later, the tso server will be updated in `update_loop`.
        if keyspace_group.is_none() {
            self.reset_primary_tso_client();
            return Err(Error::TsoServerNotFound);
        }
        self.failure_count = 0;

        let keyspace_group = keyspace_group.unwrap();
        if keyspace_group.get_members().is_empty() {
            self.reset_primary_tso_client();
            return Err(Error::TsoServerNotFound);
        }

        // Update addrs
        self.addrs = keyspace_group
            .get_members()
            .iter()
            .map(|member| member.address.clone())
            .collect();

        info!(
            "pd_client: find default keyspace group tso members {:?}",
            self.addrs
        );

        // Find primary tso server
        let primary_addr = keyspace_group
            .get_members()
            .iter()
            .find(|member| member.is_primary)
            .map(|member| member.address.clone());
        if primary_addr.is_none() {
            self.reset_primary_tso_client();
            return Err(Error::TsoServerNotFound);
        }
        let primary_addr = primary_addr.unwrap();
        if primary_addr != self.primary_addr {
            info!(
                "pd_client: update primary tso server, old: {:?}, new: {:?}",
                self.primary_addr, primary_addr
            );
            self.reset_primary_tso_client();
            let (client_stub, _) = connector
                .connect_tso(&primary_addr, self.cluster_id, self.keyspace_id)
                .await?;
            self.primary_tso_client = Some(client_stub);
            self.primary_addr = primary_addr;
            return Ok(true);
        }

        Ok(false)
    }

    fn reset_primary_tso_client(&mut self) {
        self.primary_tso_client = None;
        self.primary_addr.clear();
    }

    async fn find_group_by_keyspace_id(
        &self,
        tso_addr: &str,
        connector: &PdConnector,
    ) -> Result<KeyspaceGroup> {
        let (_, keyspace_group) = connector
            .connect_tso(tso_addr, self.cluster_id, self.keyspace_id)
            .await?;
        Ok(keyspace_group)
    }
}

/// A leader client doing requests asynchronous.
pub struct Client {
    timer: Handle,
    pub(crate) inner: RwLock<Inner>,
    pub feature_gate: FeatureGate,
    enable_forwarding: bool,
    pub(crate) pd_connector: PdConnector,
    pub(crate) new_tso: RwLock<Option<TimestampOracle>>,
}

impl Client {
    pub(crate) fn new(
        security_mgr: Arc<SecurityManager>,
        client_stub: PdClientStub,
        members: GetMembersResponse,
        target: TargetInfo,
        tso: TimestampOracle,
        new_tso: Option<TimestampOracle>,
        enable_forwarding: bool,
        pd_connector: PdConnector,
    ) -> Client {
        if !target.direct_connected() {
            REQUEST_FORWARDED_GAUGE_VEC
                .with_label_values(&[&target.via])
                .set(1);
        }
        let (hb_tx, hb_rx) = client_stub
            .region_heartbeat_opt(target.call_option())
            .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "region_heartbeat", e));
        let (buckets_tx, buckets_resp) = client_stub
            .report_buckets_opt(target.call_option())
            .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "report_buckets", e));
        Client {
            timer: GLOBAL_TIMER_HANDLE.clone(),
            inner: RwLock::new(Inner {
                hb_sender: Either::Left(Some(hb_tx)),
                hb_receiver: Either::Left(Some(hb_rx)),
                buckets_sender: Either::Left(Some(buckets_tx)),
                buckets_resp: Some(buckets_resp),
                client_stub,
                members,
                target,
                security_mgr,
                on_reconnect: None,
                pending_heartbeat: Arc::default(),
                pending_buckets: Arc::default(),
                last_try_reconnect: Instant::now(),
                tso,
            }),
            feature_gate: FeatureGate::default(),
            enable_forwarding,
            pd_connector,
            new_tso: RwLock::new(new_tso),
        }
    }

    fn update_client(
        &self,
        client_stub: PdClientStub,
        target: TargetInfo,
        members: GetMembersResponse,
        tso: TimestampOracle,
    ) {
        let start_refresh = Instant::now();
        let mut inner = self.inner.wl();

        let (hb_tx, hb_rx) = client_stub
            .region_heartbeat_opt(target.call_option())
            .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "region_heartbeat", e));
        info!("heartbeat sender and receiver are stale, refreshing ...");

        // Try to cancel an unused heartbeat sender.
        if let Either::Left(Some(ref mut r)) = inner.hb_sender {
            r.cancel();
        }
        inner.hb_sender = Either::Left(Some(hb_tx));
        let prev_receiver = std::mem::replace(&mut inner.hb_receiver, Either::Left(Some(hb_rx)));
        let _ = prev_receiver.right().map(|t| t.wake());

        let (buckets_tx, buckets_resp) = client_stub
            .report_buckets_opt(target.call_option())
            .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "region_buckets", e));
        info!("buckets sender and receiver are stale, refreshing ...");
        // Try to cancel an unused buckets sender.
        if let Either::Left(Some(ref mut r)) = inner.buckets_sender {
            r.cancel();
        }
        inner.buckets_sender = Either::Left(Some(buckets_tx));
        inner.buckets_resp = Some(buckets_resp);

        inner.client_stub = client_stub;
        inner.members = members;
        inner.tso = tso;
        if let Some(ref on_reconnect) = inner.on_reconnect {
            on_reconnect();
        }

        if !inner.target.via.is_empty() {
            REQUEST_FORWARDED_GAUGE_VEC
                .with_label_values(&[&inner.target.via])
                .set(0);
        }

        if !target.via.is_empty() {
            REQUEST_FORWARDED_GAUGE_VEC
                .with_label_values(&[&target.via])
                .set(1);
        }

        info!(
            "update pd client";
            "prev_leader" => &inner.target.target_url,
            "prev_via" => &inner.target.via,
            "leader" => &target.target_url,
            "via" => &target.via,
        );
        inner.target = target;
        slow_log!(
            start_refresh.saturating_elapsed(),
            "PD client refresh region heartbeat",
        );
    }

    pub fn update_new_tso(&self, tso: Option<TimestampOracle>) {
        let mut new_tso = self.new_tso.wl();
        *new_tso = tso;
        info!("update pd client new tso");
    }

    pub fn handle_region_heartbeat_response(
        self: &Arc<Self>,
        f: Box<dyn Fn(RegionHeartbeatResponse) + Send + 'static>,
    ) -> PdFuture<()> {
        let recv = HeartbeatReceiver {
            receiver: None,
            inner: self.clone(),
        };
        Box::pin(
            recv.try_for_each(move |resp| {
                f(resp);
                future::ready(Ok(()))
            })
            .map_err(|e| panic!("unexpected error: {:?}", e)),
        )
    }

    pub fn on_reconnect(&self, f: Box<dyn Fn() + Sync + Send + 'static>) {
        let mut inner = self.inner.wl();
        inner.on_reconnect = Some(f);
    }

    pub fn request<Req, Resp, F>(
        self: &Arc<Self>,
        req: Req,
        func: F,
        retry: usize,
    ) -> Request<Req, F>
    where
        Req: Clone + 'static,
        F: FnMut(&Client, Req) -> PdFuture<Resp> + Send + 'static,
    {
        Request {
            remain_reconnect_count: retry,
            request_sent: 0,
            client: self.clone(),
            req,
            func,
        }
    }

    pub fn get_leader(&self) -> Member {
        self.inner.rl().members.get_leader().clone()
    }

    /// Re-establishes connection with PD leader in asynchronized fashion.
    ///
    /// If `force` is false, it will reconnect only when members change.
    /// Note: Retrying too quickly will return an error due to cancellation.
    /// Please always try to reconnect after sending the request first.
    pub async fn reconnect(&self, force: bool) -> Result<()> {
        PD_RECONNECT_COUNTER_VEC.with_label_values(&["try"]).inc();
        let start = Instant::now();

        let future = {
            let inner = self.inner.rl();
            if start.saturating_duration_since(inner.last_try_reconnect) < GLOBAL_RECONNECT_INTERVAL
            {
                // Avoid unnecessary updating.
                // Prevent a large number of reconnections in a short time.
                PD_RECONNECT_COUNTER_VEC
                    .with_label_values(&["cancel"])
                    .inc();
                return Err(box_err!("cancel reconnection due to too small interval"));
            }
            let members = inner.members.clone();
            async move {
                let direct_connected = self.inner.rl().target_info().direct_connected();
                self.pd_connector
                    .reconnect_pd(
                        members,
                        direct_connected,
                        force,
                        self.enable_forwarding,
                        true,
                    )
                    .await
            }
        };

        {
            let mut inner = self.inner.wl();
            if start.saturating_duration_since(inner.last_try_reconnect) < GLOBAL_RECONNECT_INTERVAL
            {
                // There may be multiple reconnections that pass the read lock at the same time.
                // Check again in the write lock to avoid unnecessary updating.
                PD_RECONNECT_COUNTER_VEC
                    .with_label_values(&["cancel"])
                    .inc();
                return Err(box_err!("cancel reconnection due to too small interval"));
            }
            inner.last_try_reconnect = start;
        }

        slow_log!(start.saturating_elapsed(), "try reconnect pd");
        let (client, target_info, members, tso) = match future.await {
            Err(e) => {
                PD_RECONNECT_COUNTER_VEC
                    .with_label_values(&["failure"])
                    .inc();
                return Err(e);
            }
            Ok(None) => {
                PD_RECONNECT_COUNTER_VEC
                    .with_label_values(&["no-need"])
                    .inc();
                return Ok(());
            }
            Ok(Some(tuple)) => {
                PD_RECONNECT_COUNTER_VEC
                    .with_label_values(&["success"])
                    .inc();
                tuple
            }
        };

        fail_point!("pd_client_reconnect", |_| Ok(()));

        self.update_client(client, target_info, members, tso.unwrap());
        info!("trying to update PD client done"; "spend" => ?start.saturating_elapsed());
        Ok(())
    }
}

/// The context of sending requests.
pub struct Request<Req, F> {
    remain_reconnect_count: usize,
    request_sent: usize,
    client: Arc<Client>,
    req: Req,
    func: F,
}

const MAX_REQUEST_COUNT: usize = 3;

impl<Req, Resp, F> Request<Req, F>
where
    Req: Clone + Send + 'static,
    F: FnMut(&Client, Req) -> PdFuture<Resp> + Send + 'static,
{
    async fn reconnect_if_needed(&mut self) -> Result<()> {
        debug!("reconnecting ..."; "remain" => self.remain_reconnect_count);
        if self.request_sent < MAX_REQUEST_COUNT {
            return Ok(());
        }
        if self.remain_reconnect_count == 0 {
            return Err(box_err!("request retry exceeds limit"));
        }
        // Updating client.
        self.remain_reconnect_count -= 1;
        // FIXME: should not block the core.
        debug!("(re)connecting PD client");
        match self.client.reconnect(true).await {
            Ok(_) => {
                self.request_sent = 0;
            }
            Err(_) => {
                let _ = self
                    .client
                    .timer
                    .delay(std::time::Instant::now() + REQUEST_RECONNECT_INTERVAL)
                    .compat()
                    .await;
            }
        }
        Ok(())
    }

    async fn send_and_receive(&mut self) -> Result<Resp> {
        self.request_sent += 1;
        debug!("request sent: {}", self.request_sent);
        let r = self.req.clone();
        (self.func)(&self.client, r).await
    }

    fn should_not_retry(resp: &Result<Resp>) -> bool {
        match resp {
            Ok(_) => true,
            Err(err) => {
                // these errors are not caused by network, no need to retry
                if err.retryable() {
                    error!(?*err; "request failed, retry");
                    false
                } else {
                    true
                }
            }
        }
    }

    /// Returns a Future, it is resolves once a future returned by the closure
    /// is resolved successfully, otherwise it repeats `retry` times.
    pub fn execute(mut self) -> PdFuture<Resp> {
        Box::pin(async move {
            loop {
                {
                    let resp = self.send_and_receive().await;
                    if Self::should_not_retry(&resp) {
                        return resp;
                    }
                }
                self.reconnect_if_needed().await?;
            }
        })
    }
}

pub fn call_option_inner(inner: &Inner) -> CallOption {
    inner
        .target_info()
        .call_option()
        .timeout(Duration::from_secs(REQUEST_TIMEOUT))
}

/// Do a request in synchronized fashion.
pub fn sync_request<F, R>(client: &Client, mut retry: usize, func: F) -> Result<R>
where
    F: Fn(&PdClientStub, CallOption) -> GrpcResult<R>,
{
    loop {
        let ret = {
            // Drop the read lock immediately to prevent the deadlock between the caller
            // thread which may hold the read lock and wait for PD client thread
            // completing the request and the PD client thread which may block
            // on acquiring the write lock.
            let (client_stub, option) = {
                let inner = client.inner.rl();
                (inner.client_stub.clone(), call_option_inner(&inner))
            };

            func(&client_stub, option).map_err(Error::Grpc)
        };
        match ret {
            Ok(r) => {
                return Ok(r);
            }
            Err(e) => {
                error!(?e; "request failed");
                if retry == 0 {
                    return Err(e);
                }
            }
        }
        // try reconnect
        retry -= 1;
        if let Err(e) = block_on(client.reconnect(true)) {
            error!(?e; "reconnect failed");
            thread::sleep(REQUEST_RECONNECT_INTERVAL);
        }
    }
}

pub type StubTuple = (
    PdClientStub,
    TargetInfo,
    GetMembersResponse,
    // Only used by RpcClient, not by RpcClientV2.
    Option<TimestampOracle>,
);

#[derive(Clone)]
pub struct PdConnector {
    pub(crate) env: Arc<Environment>,
    tso_discovery: Arc<Mutex<Option<TsoServiceDiscovery>>>,
    pub(crate) security_mgr: Arc<SecurityManager>,
}

impl PdConnector {
    pub fn new(env: Arc<Environment>, security_mgr: Arc<SecurityManager>) -> PdConnector {
        PdConnector {
            env,
            security_mgr,
            tso_discovery: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn validate_endpoints(&self, cfg: &Config, build_tso: bool) -> Result<StubTuple> {
        let len = cfg.endpoints.len();
        let mut endpoints_set = HashSet::with_capacity_and_hasher(len, Default::default());
        let mut members = None;
        let mut cluster_id = None;
        for ep in &cfg.endpoints {
            if !endpoints_set.insert(ep) {
                return Err(box_err!("duplicate PD endpoint {}", ep));
            }

            let (_, resp) = match self.connect(ep).await {
                Ok(resp) => resp,
                // Ignore failed PD node.
                Err(e) => {
                    info!("PD failed to respond"; "endpoints" => ep, "err" => ?e);
                    continue;
                }
            };

            // Check cluster ID.
            let cid = resp.get_header().get_cluster_id();
            if let Some(sample) = cluster_id {
                if sample != cid {
                    return Err(box_err!(
                        "PD response cluster_id mismatch, want {}, got {}",
                        sample,
                        cid
                    ));
                }
            } else {
                cluster_id = Some(cid);
            }
            // TODO: check all fields later?
            if members.is_none() {
                members = Some(resp);
            }
        }

        match members {
            Some(members) => {
                let res = self
                    .reconnect_pd(members, true, true, cfg.enable_forwarding, build_tso)
                    .await?
                    .unwrap();
                info!("all PD endpoints are consistent"; "endpoints" => ?cfg.endpoints);

                Ok(res)
            }
            _ => Err(box_err!("PD cluster failed to respond")),
        }
    }

    pub async fn init_tso_discovery(
        &self,
        cluster_id: u64,
        pd_client: &PdClientStub,
        call_option: CallOption,
    ) {
        // Create tso service discovery
        let tso_discovery =
            TsoServiceDiscovery::new(cluster_id, 0, vec![], pd_client.clone(), call_option);

        self.tso_discovery.lock().await.replace(tso_discovery);
    }

    pub async fn build_new_tso(&self) -> Option<TimestampOracle> {
        let mut guard = self.tso_discovery.lock().await;
        assert!(guard.is_some());
        let tso_discovery = guard.as_mut().unwrap();
        if let Err(err) = tso_discovery.update_member(self).await {
            warn!("pd_client: tso service discover failed"; "err" => ?err);
            None
        } else if let Ok(tso_client) = tso_discovery.get_tso_client(self).await {
            let cluster_id = tso_discovery.get_cluster_id();
            Some(TimestampOracle::new(cluster_id, &tso_client, CallOption::default()).unwrap())
        } else {
            None
        }
    }

    pub async fn reset_tso_discover(&self) {
        let mut guard = self.tso_discovery.lock().await;
        assert!(guard.is_some());
        let tso_discovery = guard.as_mut().unwrap();
        tso_discovery.reset_primary_tso_client();
    }

    pub async fn connect(&self, addr: &str) -> Result<(PdClientStub, GetMembersResponse)> {
        info!("connecting to PD endpoint"; "endpoints" => addr);
        let addr_trim = trim_http_prefix(addr);
        let channel = {
            let cb = ChannelBuilder::new(self.env.clone())
                .max_send_message_len(-1)
                .max_receive_message_len(-1)
                .keepalive_time(Duration::from_secs(10))
                .keepalive_timeout(Duration::from_secs(3))
                .max_reconnect_backoff(Duration::from_secs(5))
                .initial_reconnect_backoff(Duration::from_secs(1));
            self.security_mgr.connect(cb, addr_trim)
        };
        fail_point!("cluster_id_is_not_ready", |_| {
            Ok((
                PdClientStub::new(channel.clone()),
                GetMembersResponse::default(),
            ))
        });
        let client = PdClientStub::new(channel.clone());
        let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
        let response = client
            .get_members_async_opt(&GetMembersRequest::default(), option)
            .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "get_members", e))
            .await;
        match response {
            Ok(resp) => Ok((client, resp)),
            Err(e) => Err(Error::Grpc(e)),
        }
    }

    pub async fn connect_tso(
        &self,
        addr: &str,
        cluster_id: u64,
        keyspace_id: u32,
    ) -> Result<(TsoClientStub, KeyspaceGroup)> {
        info!("connecting to PD TSO endpoint"; "endpoints" => addr);
        let addr_trim = trim_http_prefix(addr);
        let channel = {
            let cb = ChannelBuilder::new(self.env.clone())
                .max_send_message_len(-1)
                .max_receive_message_len(-1)
                .keepalive_time(Duration::from_secs(10))
                .keepalive_timeout(Duration::from_secs(3))
                .max_reconnect_backoff(Duration::from_secs(5))
                .initial_reconnect_backoff(Duration::from_secs(1));
            self.security_mgr.connect(cb, addr_trim)
        };

        let client = TsoClientStub::new(channel);
        let mut req = FindGroupByKeyspaceIdRequest::default();
        let header = req.mut_header();
        header.set_cluster_id(cluster_id);
        header.set_keyspace_id(keyspace_id);
        header.set_keyspace_group_id(0);
        req.set_keyspace_id(keyspace_id);
        let mut resp = client
            .find_group_by_keyspace_id_async_opt(
                &req,
                CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT)),
            )?
            .await?;

        if resp.get_header().has_error() {
            return Err(box_err!(
                "failed to find group by keyspace id: {:?}",
                resp.get_header().get_error()
            ));
        }
        Ok((client, resp.take_keyspace_group()))
    }

    // load_members returns the PD members by calling getMember, there are two
    // abnormal scenes for the response:
    // 1. header has an error: the PD is not ready to serve.
    // 2. cluster id is zero: etcd start server but the follower did not get
    // cluster id yet.
    // In this case, load_members should return an error, so the client
    // will not update client address.
    pub async fn load_members(&self, previous: &GetMembersResponse) -> Result<GetMembersResponse> {
        let previous_leader = previous.get_leader();
        let members = previous.get_members();
        let cluster_id = previous.get_header().get_cluster_id();

        // Try to connect to other members, then the previous leader.
        for m in members
            .iter()
            .filter(|m| *m != previous_leader)
            .chain(&[previous_leader.clone()])
        {
            for ep in m.get_client_urls() {
                match self.connect(ep.as_str()).await {
                    Ok((_, r)) => {
                        let header = r.get_header();
                        // Try next follower endpoint if the cluster has not ready since this pr:
                        // pd#5412.
                        if let Err(e) = check_resp_header(header) {
                            error!("connect pd failed";"endpoints" => ep, "error" => ?e);
                        } else {
                            let new_cluster_id = header.get_cluster_id();
                            // it is new cluster if the new cluster id is zero.
                            if cluster_id == 0 || new_cluster_id == cluster_id {
                                // check whether the response have leader info, otherwise continue
                                // to loop the rest members
                                if r.has_leader() {
                                    return Ok(r);
                                }
                            // Try next endpoint if PD server returns the
                            // cluster id is zero without any error.
                            } else if new_cluster_id == 0 {
                                error!("{} connect success, but cluster id is not ready", ep);
                            } else {
                                panic!(
                                    "{} no longer belongs to cluster {}, it is in {}",
                                    ep, cluster_id, new_cluster_id
                                );
                            }
                        }
                    }
                    Err(e) => {
                        error!("connect failed"; "endpoints" => ep, "error" => ?e);
                        continue;
                    }
                }
            }
        }
        Err(box_err!(
            "failed to connect to {:?}",
            previous.get_members()
        ))
    }

    // There are 3 kinds of situations we will return the new client:
    // 1. the force is true which represents the client is newly created or the
    // original connection has some problem 2. the previous forwarded host is
    // not empty and it can connect the leader now which represents the network
    // partition problem to leader may be recovered 3. the member information of
    // PD has been changed
    pub async fn reconnect_pd(
        &self,
        members_resp: GetMembersResponse,
        direct_connected: bool,
        force: bool,
        enable_forwarding: bool,
        build_tso: bool,
    ) -> Result<Option<StubTuple>> {
        let resp = self.load_members(&members_resp).await?;
        let leader = resp.get_leader();
        let members = resp.get_members();
        // Currently we connect to leader directly and there is no member change.
        // We don't need to connect to PD again.
        if !force && direct_connected && resp == members_resp {
            return Ok(None);
        }
        let (res, has_network_error) = self.reconnect_leader(leader).await?;
        match res {
            Some((client, target_url)) => {
                let info = TargetInfo::new(target_url, "");
                let tso = if build_tso {
                    Some(TimestampOracle::new_legacy(
                        resp.get_header().get_cluster_id(),
                        &client,
                        info.call_option(),
                    )?)
                } else {
                    None
                };
                return Ok(Some((client, info, resp, tso)));
            }
            None => {
                // If the force is false, we could have already forwarded the requests.
                // We don't need to try forwarding again.
                if !force && resp == members_resp {
                    return Err(box_err!("failed to connect to {:?}", leader));
                }
                if enable_forwarding && has_network_error {
                    if let Ok(Some((client, info))) = self.try_forward(members, leader).await {
                        let tso = if build_tso {
                            Some(TimestampOracle::new_legacy(
                                resp.get_header().get_cluster_id(),
                                &client,
                                info.call_option(),
                            )?)
                        } else {
                            None
                        };
                        return Ok(Some((client, info, resp, tso)));
                    }
                }
            }
        }
        Err(box_err!(
            "failed to connect to {:?}",
            members_resp.get_members()
        ))
    }

    pub async fn connect_member(
        &self,
        peer: &Member,
    ) -> Result<(Option<(PdClientStub, String, GetMembersResponse)>, bool)> {
        let mut network_fail_num = 0;
        let mut has_network_error = false;
        let client_urls = peer.get_client_urls();
        for ep in client_urls {
            match self.connect(ep.as_str()).await {
                Ok((client, resp)) => {
                    info!("connected to PD member"; "endpoints" => ep);
                    return Ok((Some((client, ep.clone(), resp)), false));
                }
                Err(Error::Grpc(e)) => {
                    if let RpcFailure(ref status) = e {
                        if status.code() == RpcStatusCode::UNAVAILABLE
                            || status.code() == RpcStatusCode::DEADLINE_EXCEEDED
                        {
                            network_fail_num += 1;
                        }
                    }
                    error!("failed to connect to PD member"; "endpoints" => ep, "error" => ?e);
                }
                _ => unreachable!(),
            }
        }
        let url_num = client_urls.len();
        if url_num != 0 && url_num == network_fail_num {
            has_network_error = true;
        }
        Ok((None, has_network_error))
    }

    pub async fn reconnect_leader(
        &self,
        leader: &Member,
    ) -> Result<(Option<(PdClientStub, String)>, bool)> {
        fail_point!("connect_leader", |_| Ok((None, true)));
        let mut retry_times = MAX_RETRY_TIMES;
        let timer = Instant::now();
        // Try to connect the PD cluster leader.
        loop {
            let (res, has_network_err) = self.connect_member(leader).await?;
            match res {
                Some((client, ep, _)) => {
                    return Ok((Some((client, ep)), has_network_err));
                }
                None => {
                    if has_network_err
                        && retry_times > 0
                        && timer.saturating_elapsed() <= MAX_RETRY_DURATION
                    {
                        let _ = GLOBAL_TIMER_HANDLE
                            .delay(std::time::Instant::now() + RETRY_INTERVAL)
                            .compat()
                            .await;
                        retry_times -= 1;
                        continue;
                    }
                    return Ok((None, has_network_err));
                }
            }
        }
    }

    pub async fn try_forward(
        &self,
        members: &[Member],
        leader: &Member,
    ) -> Result<Option<(PdClientStub, TargetInfo)>> {
        // Try to connect the PD cluster follower.
        for m in members.iter().filter(|m| *m != leader) {
            let (res, _) = self.connect_member(m).await?;
            match res {
                Some((client, ep, resp)) => {
                    let leader = resp.get_leader();
                    let client_urls = leader.get_client_urls();
                    for leader_url in client_urls {
                        let target = TargetInfo::new(leader_url.clone(), &ep);
                        let response = client
                            .get_members_async_opt(
                                &GetMembersRequest::default(),
                                target
                                    .call_option()
                                    .timeout(Duration::from_secs(REQUEST_TIMEOUT)),
                            )
                            .unwrap_or_else(|e| {
                                panic!("fail to request PD {} err {:?}", "get_members", e)
                            })
                            .await;
                        match response {
                            Ok(_) => return Ok(Some((client, target))),
                            Err(_) => continue,
                        }
                    }
                }
                _ => continue,
            }
        }
        Err(box_err!("failed to connect to followers"))
    }
}

pub fn trim_http_prefix(s: &str) -> &str {
    s.trim_start_matches("http://")
        .trim_start_matches("https://")
}

/// Convert a PD protobuf error to an `Error`.
pub fn check_resp_header(header: &ResponseHeader) -> Result<()> {
    if !header.has_error() {
        return Ok(());
    }
    let err = header.get_error();
    match err.get_type() {
        ErrorType::AlreadyBootstrapped => Err(Error::ClusterBootstrapped(header.get_cluster_id())),
        ErrorType::NotBootstrapped => Err(Error::ClusterNotBootstrapped(header.get_cluster_id())),
        ErrorType::IncompatibleVersion => Err(Error::Incompatible),
        ErrorType::StoreTombstone => Err(Error::StoreTombstone(err.get_message().to_owned())),
        ErrorType::RegionNotFound => Err(Error::RegionNotFound(vec![])),
        ErrorType::GlobalConfigNotFound => {
            Err(Error::GlobalConfigNotFound(err.get_message().to_owned()))
        }
        ErrorType::Ok => Ok(()),
        ErrorType::DuplicatedEntry | ErrorType::EntryNotFound => Err(box_err!(err.get_message())),
        ErrorType::Unknown => Err(box_err!(err.get_message())),
        ErrorType::InvalidValue => Err(box_err!(err.get_message())),
        ErrorType::DataCompacted => Err(box_err!(err.get_message())),
        // It will not happen, because we don't call `batch_scan_regions` in TiKV.
        ErrorType::RegionsNotContainAllKeyRange => Err(box_err!(err.get_message())),
    }
}

pub fn new_bucket_stats(meta: &BucketMeta) -> BucketStats {
    let count = meta.keys.len() - 1;
    let mut stats = BucketStats::default();
    stats.set_write_bytes(vec![0; count]);
    stats.set_read_bytes(vec![0; count]);
    stats.set_write_qps(vec![0; count]);
    stats.set_read_qps(vec![0; count]);
    stats.set_write_keys(vec![0; count]);
    stats.set_read_keys(vec![0; count]);
    stats
}

pub fn new_bucket_write_stats(meta: &BucketMeta) -> BucketStats {
    let count = meta.keys.len() - 1;
    let mut stats = BucketStats::default();
    stats.set_write_bytes(vec![0; count]);
    stats.set_write_keys(vec![0; count]);
    stats
}

pub fn find_bucket_index<S: AsRef<[u8]>>(key: &[u8], bucket_keys: &[S]) -> Option<usize> {
    let last_key = bucket_keys.last().unwrap().as_ref();
    let search_keys = &bucket_keys[..bucket_keys.len() - 1];
    search_keys
        .binary_search_by(|k| k.as_ref().cmp(key))
        .map_or_else(
            |idx| {
                if idx == 0 || (idx == search_keys.len() && !last_key.is_empty() && key >= last_key)
                {
                    None
                } else {
                    Some(idx - 1)
                }
            },
            Some,
        )
}

/// Merge incoming bucket stats. If a range in new buckets overlaps with
/// multiple ranges in current buckets, stats of the new range will be added to
/// the first overlapped range.
pub fn merge_bucket_stats<C: AsRef<[u8]>, I: AsRef<[u8]>>(
    cur: &[C],
    cur_stats: &mut BucketStats,
    incoming: &[I],
    delta_stats: &BucketStats,
) {
    // Return [start, end] of indices of buckets
    fn find_overlay_ranges<S: AsRef<[u8]>>(
        range: (&[u8], &[u8]),
        keys: &[S],
    ) -> Option<(usize, usize)> {
        let bucket_cnt = keys.len() - 1;
        let last_bucket_idx = bucket_cnt - 1;
        let start = match find_bucket_index(range.0, keys) {
            Some(idx) => idx,
            None => {
                if range.0 < keys[0].as_ref() {
                    0
                } else {
                    // Not in the bucket range.
                    return None;
                }
            }
        };

        let end = if range.1.is_empty() {
            last_bucket_idx
        } else {
            match find_bucket_index(range.1, keys) {
                Some(idx) => {
                    // If end key is the start key of a bucket, this bucket should not be included.
                    if range.1 == keys[idx].as_ref() {
                        if idx == 0 {
                            return None;
                        }
                        idx - 1
                    } else {
                        idx
                    }
                }
                None => {
                    if range.1 >= keys[keys.len() - 1].as_ref() {
                        last_bucket_idx
                    } else {
                        // Not in the bucket range.
                        return None;
                    }
                }
            }
        };
        Some((start, end))
    }

    macro_rules! stats_add {
        ($right:ident, $ridx:expr, $left:ident, $lidx:expr, $member:ident) => {
            if let Some(s) = $right.$member.get_mut($ridx) {
                *s += $left.$member.get($lidx).copied().unwrap_or_default();
            }
        };
    }

    for new_idx in 0..(incoming.len() - 1) {
        let start = &incoming[new_idx];
        let end = &incoming[new_idx + 1];
        if let Some((start_idx, _end_idx)) =
            find_overlay_ranges((start.as_ref(), end.as_ref()), cur)
        {
            let cur_idx = start_idx;
            stats_add!(cur_stats, cur_idx, delta_stats, new_idx, read_bytes);
            stats_add!(cur_stats, cur_idx, delta_stats, new_idx, write_bytes);

            stats_add!(cur_stats, cur_idx, delta_stats, new_idx, read_qps);
            stats_add!(cur_stats, cur_idx, delta_stats, new_idx, write_qps);

            stats_add!(cur_stats, cur_idx, delta_stats, new_idx, read_keys);
            stats_add!(cur_stats, cur_idx, delta_stats, new_idx, write_keys);
        }
    }
}

pub fn simple_merge_bucket_write_stats(cur: &mut BucketStat, incoming: &BucketStat) {
    if cur.meta != incoming.meta {
        return;
    }
    for (cur, incoming) in cur
        .stats
        .write_keys
        .iter_mut()
        .zip(incoming.stats.write_keys.iter())
    {
        *cur += incoming;
    }
    for (cur, incoming) in cur
        .stats
        .write_bytes
        .iter_mut()
        .zip(incoming.stats.write_bytes.iter())
    {
        *cur += incoming;
    }
}

pub fn grpc_error_is_unimplemented(e: &Error) -> bool {
    if let Error::Grpc(grpcio::Error::RpcFailure(ref status)) = e {
        status.code() == grpcio::RpcStatusCode::UNIMPLEMENTED
    } else {
        false
    }
}

pub fn compare_region_end_key(lhs: &[u8], rhs: &[u8]) -> std::cmp::Ordering {
    if lhs.is_empty() {
        if rhs.is_empty() {
            return std::cmp::Ordering::Equal;
        }
        return std::cmp::Ordering::Greater;
    }
    if rhs.is_empty() {
        return std::cmp::Ordering::Less;
    }
    lhs.cmp(rhs)
}

// `key_is_encoded` indicates that whether `start_key` & `end_key` are encoded.
pub fn check_regions_boundary<R: RegionLike + fmt::Debug>(
    start_key: &[u8],
    end_key: &[u8],
    key_is_encoded: bool,
    regions: &[R],
) -> crate::pd_control::Result<()> {
    if !end_key.is_empty() && start_key >= end_key && regions.is_empty() {
        return Ok(());
    }
    if regions.is_empty() {
        return Err(box_err!("no region"));
    }

    let start_key = convert_key_encoding(start_key, key_is_encoded, R::KEY_ENCODED)?;
    let end_key = convert_key_encoding(end_key, key_is_encoded, R::KEY_ENCODED)?;

    let first_region = regions.first().unwrap();
    let last_region = regions.last().unwrap();
    if first_region.start_key() > start_key.as_ref() {
        return Err(box_err!(
            "unexpected start key of first region: {:?}, start_key: {:?}",
            first_region,
            start_key
        ));
    } else if compare_region_end_key(last_region.end_key(), end_key.as_ref()).is_lt() {
        return Err(box_err!(
            "unexpected end key of last region: {:?}, end_key: {:?}",
            last_region,
            end_key
        ));
    }

    for region in regions.windows(2) {
        if region[0].end_key() != region[1].start_key() {
            return Err(box_err!(
                "region boundary not match: {:?}, {:?}",
                region[0],
                region[1]
            ));
        }
    }

    Ok(())
}

pub type ConvertError = Box<dyn error::Error + Sync + Send>;

pub fn convert_key_encoding(
    mut key: &[u8],
    key_is_encoded: bool,
    expected_encoding: bool,
) -> std::result::Result<Cow<'_, [u8]>, ConvertError> {
    if key.is_empty() {
        return Ok(Cow::Borrowed(key));
    }

    Ok(match (key_is_encoded, expected_encoding) {
        (true, true) | (false, false) => Cow::Borrowed(key),
        (true, false) => Cow::Owned(decode_bytes(&mut key, false).map_err(
            |err| -> ConvertError { box_err!("decode error, key {}: {:?}", Value::key(key), err) },
        )?),
        (false, true) => Cow::Owned(encode_bytes(key)),
    })
}

pub trait RegionLike {
    /// Indicate whether keys in region is encoded.
    const KEY_ENCODED: bool;
    fn id(&self) -> u64;
    fn epoch(&self) -> &metapb::RegionEpoch;
    fn start_key(&self) -> &[u8];
    fn end_key(&self) -> &[u8];
}

impl RegionLike for metapb::Region {
    const KEY_ENCODED: bool = true;
    fn id(&self) -> u64 {
        self.id
    }
    fn epoch(&self) -> &metapb::RegionEpoch {
        self.get_region_epoch()
    }
    fn start_key(&self) -> &[u8] {
        &self.start_key
    }
    fn end_key(&self) -> &[u8] {
        &self.end_key
    }
}

impl RegionLike for pdpb::Region {
    const KEY_ENCODED: bool = true;
    fn id(&self) -> u64 {
        self.get_region().id
    }
    fn epoch(&self) -> &metapb::RegionEpoch {
        self.get_region().get_region_epoch()
    }
    fn start_key(&self) -> &[u8] {
        &self.get_region().start_key
    }
    fn end_key(&self) -> &[u8] {
        &self.get_region().end_key
    }
}

pub fn get_all_stores_except_tiflash(pd_client: &dyn PdClient) -> Result<Vec<Store>> {
    block_on(get_all_stores_except_tiflash_async(pd_client, true))
}

pub async fn get_all_stores_except_tiflash_async(
    pd_client: &dyn PdClient,
    exclude_tombstone: bool,
) -> Result<Vec<Store>> {
    Ok(pd_client
        .get_all_stores_async(exclude_tombstone)
        .await?
        .into_iter()
        .filter(|s| {
            !s.get_labels().iter().any(|l| {
                // including "tiflash" & "tiflash_compute"
                l.key.to_lowercase() == "engine" && l.value.to_lowercase().starts_with("tiflash")
            })
        })
        .collect())
}

pub fn get_tiflash_storage_stores(pd_client: &dyn PdClient) -> Result<Vec<Store>> {
    Ok(pd_client
        .get_all_stores(true)?
        .into_iter()
        .filter(|s| {
            let mut is_tiflash = false;
            let mut is_write_role = false;
            for l in s.get_labels().iter() {
                if l.key.to_lowercase() == "engine" && l.value.to_lowercase() == "tiflash" {
                    is_tiflash = true;
                }
                // exclude the tiflash write node
                if l.key.to_lowercase() == "engine_role" && l.value.to_lowercase() == "write" {
                    is_write_role = true;
                }
            }
            is_tiflash && !is_write_role
        })
        .collect())
}

#[cfg(test)]
mod test {
    use std::cmp::{
        Ordering,
        Ordering::{Equal, Greater, Less},
    };

    use kvproto::{metapb, metapb::BucketStats};

    use super::*;
    use crate::merge_bucket_stats;

    #[test]
    fn test_merge_bucket_stats() {
        #[allow(clippy::type_complexity)]
        let cases: &[((Vec<&[u8]>, _), (Vec<&[u8]>, _), _)] = &[
            (
                (vec![b"k1", b"k3", b"k5", b"k7", b"k9"], vec![1, 1, 1, 1]),
                (vec![b"k1", b"k3", b"k5", b"k7", b"k9"], vec![1, 1, 1, 1]),
                vec![2, 2, 2, 2],
            ),
            (
                (vec![b"k1", b"k3", b"k5", b"k7", b"k9"], vec![1, 1, 1, 1]),
                (vec![b"k0", b"k6", b"k8"], vec![1, 1]),
                vec![2, 1, 2, 1],
            ),
            (
                (vec![b"k0", b"k6", b"k8"], vec![1, 1]),
                (
                    vec![b"k1", b"k3", b"k5", b"k7", b"k9", b"ka"],
                    vec![1, 1, 1, 1, 1],
                ),
                vec![4, 2],
            ),
            (
                (vec![b"k4", b"k6", b"kb"], vec![1, 1]),
                (
                    vec![b"k1", b"k3", b"k5", b"k7", b"k9", b"ka"],
                    vec![1, 1, 1, 1, 1],
                ),
                vec![3, 3],
            ),
            (
                (vec![b"k3", b"k5", b"k7"], vec![1, 1]),
                (vec![b"k4", b"k5"], vec![1]),
                vec![2, 1],
            ),
            (
                (vec![b"", b""], vec![1]),
                (vec![b"", b""], vec![1]),
                vec![2],
            ),
            (
                (vec![b"", b"k1", b""], vec![1, 1]),
                (vec![b"", b"k2", b""], vec![1, 1]),
                vec![2, 2],
            ),
            (
                (vec![b"", b""], vec![1]),
                (vec![b"", b"k1", b""], vec![1, 1]),
                vec![3],
            ),
            (
                (vec![b"", b"k1", b""], vec![1, 1]),
                (vec![b"", b""], vec![1]),
                vec![2, 1],
            ),
            (
                (vec![b"", b"k1", b""], vec![1, 1]),
                (vec![b"k2", b"k3"], vec![1]),
                vec![1, 2],
            ),
            (
                (vec![b"", b"k3", b""], vec![1, 1]),
                (vec![b"k1", b"k2"], vec![1]),
                vec![2, 1],
            ),
            (
                (vec![b"", b"k3"], vec![1]),
                (vec![b"k1", b""], vec![1]),
                vec![2],
            ),
            (
                (vec![b"", b"k3"], vec![1]),
                (vec![b"k4", b""], vec![1]),
                vec![1],
            ),
        ];
        for (current, incoming, expected) in cases {
            let cur_keys = &current.0;
            let incoming_keys = &incoming.0;
            let mut cur_stats = BucketStats::default();
            cur_stats.set_read_qps(current.1.to_vec());
            let mut incoming_stats = BucketStats::default();
            incoming_stats.set_read_qps(incoming.1.to_vec());
            merge_bucket_stats(cur_keys, &mut cur_stats, incoming_keys, &incoming_stats);
            assert_eq!(cur_stats.get_read_qps(), expected);
        }
    }

    #[test]
    fn test_find_bucket_index() {
        let keys = vec![
            b"k1".to_vec(),
            b"k3".to_vec(),
            b"k5".to_vec(),
            b"k7".to_vec(),
        ];
        assert_eq!(find_bucket_index(b"k1", &keys), Some(0));
        assert_eq!(find_bucket_index(b"k5", &keys), Some(2));
        assert_eq!(find_bucket_index(b"k2", &keys), Some(0));
        assert_eq!(find_bucket_index(b"k6", &keys), Some(2));
        assert_eq!(find_bucket_index(b"k7", &keys), None);
        assert_eq!(find_bucket_index(b"k0", &keys), None);
        assert_eq!(find_bucket_index(b"k8", &keys), None);
        let keys = vec![
            b"".to_vec(),
            b"k1".to_vec(),
            b"k3".to_vec(),
            b"k5".to_vec(),
            b"k7".to_vec(),
            b"".to_vec(),
        ];
        assert_eq!(find_bucket_index(b"k0", &keys), Some(0));
        assert_eq!(find_bucket_index(b"k7", &keys), Some(4));
        assert_eq!(find_bucket_index(b"k8", &keys), Some(4));
    }

    #[test]
    fn test_compare_region_end_key() {
        let cases: Vec<(&[u8], &[u8], Ordering)> = vec![
            (b"", b"", Equal),
            (b"", b"k1", Greater),
            (b"k1", b"", Less),
            (b"k1", b"k1", Equal),
            (b"k1", b"k2", Less),
            (b"k2", b"k1", Greater),
        ];

        for (lhs, rhs, expected) in cases {
            assert_eq!(compare_region_end_key(lhs, rhs), expected);
        }
    }

    #[test]
    fn test_convert_key_encoding() {
        let cases = vec![
            // key, key_is_encoded, expect_encoding, expected.
            ("", false, true, ""),
            ("xkey00001", false, false, "xkey00001"),
            (
                "xkey0000\\3771\\000\\000\\000\\000\\000\\000\\000\\370",
                true,
                true,
                "xkey0000\\3771\\000\\000\\000\\000\\000\\000\\000\\370",
            ),
            (
                "xkey00001",
                false,
                true,
                "xkey0000\\3771\\000\\000\\000\\000\\000\\000\\000\\370",
            ),
        ];
        for (key, key_is_encoded, expected_encoding, expected) in cases {
            let key = tikv_util::unescape(key);
            let expected = tikv_util::unescape(expected);

            assert_eq!(
                &expected,
                convert_key_encoding(&key, key_is_encoded, expected_encoding)
                    .unwrap()
                    .as_ref()
            );
            assert_eq!(
                &key,
                convert_key_encoding(&expected, expected_encoding, key_is_encoded)
                    .unwrap()
                    .as_ref()
            );
        }

        let illegal_key =
            tikv_util::unescape("xkey0000\\3771\\000\\000\\000\\000\\000\\000\\000\\360");
        convert_key_encoding(&illegal_key, true, false).unwrap_err();
    }

    #[test]
    fn test_check_regions_boundary() {
        let mk_key = |i: i32| -> Vec<u8> {
            if i == i32::MIN || i == i32::MAX {
                vec![]
            } else {
                format!("{:04}", i).into_bytes()
            }
        };
        let mk_region = |start: i32, end: i32| -> metapb::Region {
            let mut region = metapb::Region::default();
            region.set_start_key(mk_key(start));
            region.set_end_key(mk_key(end));
            region
        };
        let mk_regions = |regions: Vec<(i32, i32)>| -> Vec<metapb::Region> {
            regions
                .into_iter()
                .map(|(start, end)| mk_region(start, end))
                .collect()
        };

        let cases: Vec<(
            i32, // start_key
            i32, // end_key
            Vec<metapb::Region>,
            bool, // expect success
        )> = vec![
            (i32::MIN, i32::MAX, vec![], false),
            (
                i32::MIN,
                i32::MAX,
                mk_regions(vec![(i32::MIN, i32::MAX)]),
                true,
            ),
            (
                i32::MIN,
                i32::MAX,
                mk_regions(vec![(i32::MIN, 1), (1, 2), (2, 10), (10, i32::MAX)]),
                true,
            ),
            (
                i32::MIN,
                i32::MAX,
                mk_regions(vec![(1, 2), (2, 10), (10, i32::MAX)]),
                false,
            ),
            (
                i32::MIN,
                i32::MAX,
                mk_regions(vec![(i32::MIN, 1), (1, 2), (2, 10)]),
                false,
            ),
            (
                i32::MIN,
                i32::MAX,
                mk_regions(vec![(i32::MIN, 1), (1, 2), (10, i32::MAX)]),
                false,
            ),
            (i32::MIN, 2, vec![], false),
            (i32::MIN, 2, mk_regions(vec![(i32::MIN, 2)]), true),
            (2, 2, vec![], true),
            (2, 10, mk_regions(vec![(2, 5), (6, 10)]), false),
            (2, 10, mk_regions(vec![(2, 5), (5, 6), (6, 10)]), true),
            (
                2,
                10,
                mk_regions(vec![(i32::MIN, 5), (5, 6), (6, i32::MAX)]),
                true,
            ),
            (2, 10, mk_regions(vec![(i32::MIN, i32::MAX)]), true),
            (10, i32::MAX, vec![], false),
            (10, i32::MAX, mk_regions(vec![(10, i32::MAX)]), true),
            (10, i32::MAX, mk_regions(vec![(10, 20)]), false),
        ];

        for (idx, (start, end, regions, expect_success)) in cases.into_iter().enumerate() {
            let result = check_regions_boundary(&mk_key(start), &mk_key(end), true, &regions);
            assert_eq!(result.is_ok(), expect_success, "case {}", idx);
        }
    }
}
