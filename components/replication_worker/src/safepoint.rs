// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

//! ServiceSafepointManager manages service safepoints for changefeeds.
//!
//! Ref: https://github.com/pingcap/tiflow/blob/release-8.5/pkg/txnutil/gc/doc.go

use std::{fmt, sync::Arc, time::Duration};

use collections::{HashMap, HashMapEntry};
use pd_client::PdClient;
use security::HttpClient;
use tikv_util::{
    box_err, box_try, box_try_join, debug, error, info, warn,
    worker::{Runnable, RunnableWithTimer, Scheduler, Worker},
};
use txn_types::TimeStamp;

use crate::{
    Error, KeyspaceStates, ReplicationWorkerConfig, Result, SafepointConfig,
    metrics::{REP_KEYSPACE_SERVICE_SAFEPOINT, REP_SAFEPOINT_EVENTS_COUNTER},
    ticdc_util::{ReplicationTaskItem, ReplicationTaskList},
    util::read_from_ticdc,
};

const GC_WORKER_SERVICE_SAFEPOINT_ID: &str = "gc_worker";

pub(crate) struct ServiceSafepointManager {
    config: SafepointConfig,
    merged_store_id: u64,

    pd: Arc<dyn PdClient>,
    runtime: tokio::runtime::Handle,

    worker: Worker,
    scheduler: Scheduler<ServiceSafepointTask>,
}

impl ServiceSafepointManager {
    pub(crate) fn new(
        merged_store_id: u64,
        pd: Arc<dyn PdClient>,
        rep_config: &ReplicationWorkerConfig,
        runtime: tokio::runtime::Handle,
        keyspaces: HashMap<u32 /* keyspace_id */, KeyspaceChangefeeds>,
    ) -> Result<Self> {
        info!("ServiceSafepointManager start";
            "store" => merged_store_id, "keyspaces" => ?keyspaces);

        let http_client = box_try!(pd.get_security_mgr().http_client(hyper::Client::builder()));
        let config = rep_config.safepoint.clone();
        let runner = ServiceSafepointRunner {
            config: config.clone(),
            merged_store_id,
            pd: pd.clone(),
            runtime: runtime.clone(),
            http_client,
            keyspaces,
        };

        let worker = Worker::new("ServiceSafepointWorker");
        let scheduler = worker.start_with_timer("ServiceSafepointRunner", runner);
        Ok(Self {
            config,
            merged_store_id,
            pd,
            runtime,
            worker,
            scheduler,
        })
    }

    pub(crate) fn shutdown(self) {
        self.scheduler.stop();
        self.worker.stop();
    }

    pub(crate) fn ensure_changefeed_start_ts_safety(
        &mut self,
        keyspace_id: u32,
        feed_id: &str,
        start_ts: u64,
    ) -> Result<()> {
        let keyspace_str = keyspace_id.to_string();
        let service_id = self.get_ensure_start_ts_service_id(feed_id);
        match self
            .runtime
            .block_on(self.pd.update_keyspace_service_safe_point(
                keyspace_id,
                service_id,
                start_ts.into(),
                self.config.create_changefeed_gc_ttl.0,
            )) {
            Ok(new_safepoint) => {
                info!("ServiceSafepointManager: ensure_changefeed_start_ts_safety ok";
                    "keyspace" => keyspace_id, "feed" => feed_id,
                    "start_ts" => start_ts, "new_safepoint" => new_safepoint);
                REP_SAFEPOINT_EVENTS_COUNTER
                    .with_label_values(&["ok", &keyspace_str, "ensure_start_ts_safepoint"])
                    .inc();
                Ok(())
            }
            Err(pd_client::Error::UnsafeServiceGcSafePoint {
                requested,
                current_minimal,
            }) => {
                debug_assert_eq!(requested.into_inner(), start_ts);
                warn!("ServiceSafepointManager: start_ts before safepoint";
                    "keyspace" => keyspace_id, "feed" => feed_id,
                    "start_ts" => start_ts, "current" => current_minimal);
                REP_SAFEPOINT_EVENTS_COUNTER
                    .with_label_values(&["warn", &keyspace_str, "start_ts_before_safepoint"])
                    .inc();
                Err(Error::StartTsBeforeSafepoint {
                    start_ts,
                    gc_safe_point: current_minimal.into_inner(),
                })
            }
            Err(err) => {
                error!("ServiceSafepointManager: ensure_changefeed_start_ts_safety failed: {:?}", err;
                    "keyspace" => keyspace_id, "feed" => feed_id, "start_ts" => start_ts);
                REP_SAFEPOINT_EVENTS_COUNTER
                    .with_label_values(&["error", &keyspace_str, "ensure_start_ts_safepoint"])
                    .inc();
                Err(err.into())
            }
        }
    }

    pub(crate) fn add_keyspace(
        &self,
        keyspace_id: u32,
        cdc_addr: String,
        rep_pd_cli: Arc<dyn PdClient>,
    ) {
        let task = ServiceSafepointTask::AddKeyspace {
            keyspace_id,
            cdc_addr,
            rep_pd_cli,
        };
        if let Err(err) = self.scheduler.schedule(task) {
            warn!("ServiceSafepointManager: add_keyspace schedule failed: {:?}", err;
                "keyspace" => keyspace_id);
        }
    }

    pub(crate) fn remove_keyspace(&self, keyspace_id: u32) {
        let task = ServiceSafepointTask::RemoveKeyspace { keyspace_id };
        if let Err(err) = self.scheduler.schedule(task) {
            warn!("ServiceSafepointManager: remove_keyspace schedule failed: {:?}", err;
                "keyspace" => keyspace_id);
        }
    }

    pub(crate) fn add_changefeed(&self, keyspace_id: u32, feed_id: String) {
        let task = ServiceSafepointTask::AddChangefeed {
            keyspace_id,
            feed_id,
        };
        if let Err(err) = self.scheduler.schedule(task) {
            warn!("ServiceSafepointManager: add_changefeed schedule failed: {:?}", err;
                "keyspace" => keyspace_id);
        }
    }

    pub(crate) fn remove_changefeed(&self, keyspace_id: u32, feed_id: String) {
        let task = ServiceSafepointTask::RemoveChangefeed {
            keyspace_id,
            feed_id,
        };
        if let Err(err) = self.scheduler.schedule(task) {
            warn!("ServiceSafepointManager: remove_changefeed schedule failed: {:?}", err;
                "keyspace" => keyspace_id);
        }
    }

    fn get_ensure_start_ts_service_id(&self, changefeed_id: &str) -> String {
        format!(
            "rep-worker-{}-creating-{}",
            self.merged_store_id, changefeed_id
        )
    }
}

#[derive(Debug, Clone)]
enum ChangefeedSyncState {
    NotSynced,
    Synced,
    Expired,
}

impl ChangefeedSyncState {
    fn need_sync(&self) -> bool {
        matches!(
            self,
            ChangefeedSyncState::NotSynced | ChangefeedSyncState::Synced
        )
    }
}

#[derive(Clone)]
struct KeyspaceContext {
    keyspace_id: u32,
    cdc_addr: String,
    rep_pd_cli: Arc<dyn PdClient>,

    last_rep_gc_safepoint: Option<u64>,
}

pub(crate) struct KeyspaceChangefeeds {
    ctx: KeyspaceContext,

    /// Used for tracking keyspaces & changefeeds. Consider changefeeds from
    /// TiCDC as source of truth, but use `feeds` to reduce unnecessary
    /// requests.
    feeds: HashMap<String /* changefeed_id */, ChangefeedSyncState>,
}

impl fmt::Debug for KeyspaceChangefeeds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyspaceChangefeeds")
            .field("keyspace_id", &self.ctx.keyspace_id)
            .field("cdc_addr", &self.ctx.cdc_addr)
            .field("feeds", &self.feeds)
            .finish()
    }
}

impl KeyspaceChangefeeds {
    pub(crate) fn new(
        keyspace_id: u32,
        states: &KeyspaceStates,
        rep_pd_cli: Arc<dyn PdClient>,
    ) -> Self {
        KeyspaceChangefeeds {
            ctx: KeyspaceContext {
                keyspace_id,
                cdc_addr: states.cdc_addr.clone(),
                rep_pd_cli,
                last_rep_gc_safepoint: None,
            },
            feeds: states
                .feeds
                .keys()
                .map(|feed_id| (feed_id.clone(), ChangefeedSyncState::NotSynced))
                .collect(),
        }
    }

    fn need_sync(&self) -> bool {
        self.feeds.values().any(|state| state.need_sync())
    }

    fn set_feed_state(&mut self, feed_id: &str, new_state: ChangefeedSyncState) {
        let previous = self.feeds.insert(feed_id.to_string(), new_state.clone());
        if previous.is_none() {
            warn!("ServiceSafepointManager: set_feed_state: changefeed not found";
                "feed" => feed_id, "new_state" => ?new_state, "keyspace" => self.ctx.keyspace_id);
            debug_assert!(false);
        };
    }

    fn add_feed(&mut self, feed_id: String) {
        let previous = self.feeds.insert(feed_id, ChangefeedSyncState::NotSynced);
        debug_assert!(previous.is_none());
    }

    fn remove_feed(&mut self, feed_id: &str) -> bool {
        let previous = self.feeds.remove(feed_id);
        debug_assert!(previous.is_some());
        previous.is_some()
    }
}

struct ServiceSafepointRunner {
    config: SafepointConfig,

    merged_store_id: u64,
    pd: Arc<dyn PdClient>,
    runtime: tokio::runtime::Handle,
    http_client: HttpClient,

    keyspaces: HashMap<u32 /* keyspace_id */, KeyspaceChangefeeds>,
}

impl ServiceSafepointRunner {
    #[inline]
    fn mut_keyspace(&mut self, keyspace_id: u32) -> Option<&mut KeyspaceChangefeeds> {
        self.keyspaces.get_mut(&keyspace_id)
    }

    fn add_changefeed(&mut self, keyspace_id: u32, feed_id: String) {
        let Some(ks) = self.mut_keyspace(keyspace_id) else {
            return;
        };
        ks.add_feed(feed_id);
    }

    fn remove_changefeed(&mut self, keyspace_id: u32, feed_id: &str) {
        let Some(ks) = self.mut_keyspace(keyspace_id) else {
            return;
        };
        let existed = ks.remove_feed(feed_id);
        if existed && !ks.need_sync() {
            self.remove_keyspace_safepoint(keyspace_id);
        }
    }

    fn remove_keyspace(&mut self, keyspace_id: u32) {
        let Some(ks) = self.keyspaces.remove(&keyspace_id) else {
            debug_assert!(false);
            return;
        };
        if ks.need_sync() {
            // Happens only when the changefeeds are not consistent with replication worker.
            // Remove the safepoint anyway for safety.
            warn!("ServiceSafepointManager: remove_keyspace: still has active changefeeds";
                "keyspace" => keyspace_id);
            debug_assert!(false, "feeds: {:?}", ks.feeds);
            self.remove_keyspace_safepoint(keyspace_id);
        }
    }

    fn remove_keyspace_safepoint(&self, keyspace_id: u32) {
        self.runtime.block_on(Self::remove_keyspace_safepoint_async(
            self.pd.as_ref(),
            keyspace_id,
            self.get_gc_service_id(),
        ));
    }

    async fn remove_keyspace_safepoint_async(
        pd: &dyn PdClient,
        keyspace_id: u32,
        gc_service_id: String,
    ) {
        let keyspace_str = keyspace_id.to_string();
        match pd
            .remove_keyspace_service_safe_point(keyspace_id, gc_service_id)
            .await
        {
            Ok(new_safepoint) => {
                info!("ServiceSafepointManager: remove_keyspace_safepoint ok";
                    "keyspace" => keyspace_id, "new_safepoint" => new_safepoint);
                REP_SAFEPOINT_EVENTS_COUNTER
                    .with_label_values(&["ok", &keyspace_str, "remove_keyspace_safepoint"])
                    .inc();
            }
            Err(err) => {
                // TODO: retry on error.
                warn!("ServiceSafepointManager: remove_keyspace_safepoint failed: {:?}", err; "keyspace" => keyspace_id);
                REP_SAFEPOINT_EVENTS_COUNTER
                    .with_label_values(&["warn", &keyspace_str, "remove_keyspace_safepoint"])
                    .inc();
            }
        }
        let _ = REP_KEYSPACE_SERVICE_SAFEPOINT.remove_label_values(&[&keyspace_str, "safepoint"]);
        let _ = REP_KEYSPACE_SERVICE_SAFEPOINT
            .remove_label_values(&[&keyspace_str, "min_checkpoint_ts"]);
    }

    async fn update_keyspace_safepoint(
        pd: &dyn PdClient,
        keyspace_id: u32,
        gc_service_id: String,
        min_checkpoint_ts: u64,
        gc_ttl: Duration,
    ) {
        let keyspace_str = keyspace_id.to_string();
        match pd
            .update_keyspace_service_safe_point(
                keyspace_id,
                gc_service_id,
                min_checkpoint_ts.into(),
                gc_ttl,
            )
            .await
        {
            Ok(new_safepoint) => {
                info!("ServiceSafepointManager: update keyspace safepoint ok";
                    "keyspace" => keyspace_id, "min_checkpoint_ts" => min_checkpoint_ts, "new" => new_safepoint);
                REP_SAFEPOINT_EVENTS_COUNTER
                    .with_label_values(&["ok", &keyspace_str, "update_keyspace_safepoint"])
                    .inc();
                REP_KEYSPACE_SERVICE_SAFEPOINT
                    .with_label_values(&[&keyspace_str, "safepoint"])
                    .set(new_safepoint as i64);
                REP_KEYSPACE_SERVICE_SAFEPOINT
                    .with_label_values(&[&keyspace_str, "min_checkpoint_ts"])
                    .set(min_checkpoint_ts as i64);
            }
            Err(pd_client::Error::UnsafeServiceGcSafePoint {
                requested,
                current_minimal,
            }) => {
                // TODO: Stop the replication ?
                debug_assert_eq!(requested.into_inner(), min_checkpoint_ts);
                error!("ServiceSafepointManager: unsafe service safepoint";
                    "keyspace" => keyspace_id, "min_checkpoint_ts" => min_checkpoint_ts, "current" => current_minimal);
                REP_SAFEPOINT_EVENTS_COUNTER
                    .with_label_values(&["fatal", &keyspace_str, "unsafe_safepoint"])
                    .inc();
            }
            Err(err) => {
                warn!("ServiceSafepointManager: update_keyspace_safepoint failed: {:?}", err;
                    "keyspace" => keyspace_id, "min_checkpoint_ts" => min_checkpoint_ts);
                REP_SAFEPOINT_EVENTS_COUNTER
                    .with_label_values(&["warn", &keyspace_str, "update_keyspace_safepoint"])
                    .inc();
            }
        }
    }

    fn sync_ticdc(&mut self) {
        let runtime = self.runtime.clone();
        match runtime.block_on(self.sync_all_changefeeds_from_ticdc()) {
            Ok(()) => {
                debug!("ServiceSafepointManager: sync ticdc ok");
                REP_SAFEPOINT_EVENTS_COUNTER
                    .with_label_values(&["ok", "", "sync_ticdc"])
                    .inc();
            }
            Err(err) => {
                warn!("ServiceSafepointManager: sync ticdc failed: {:?}", err);
                REP_SAFEPOINT_EVENTS_COUNTER
                    .with_label_values(&["error", "", "sync_ticdc"])
                    .inc();
            }
        }
    }

    async fn sync_all_changefeeds_from_ticdc(&mut self) -> Result<()> {
        let pd = self.pd.clone();
        let http_client = self.http_client.clone();
        let gc_service_id = self.get_gc_service_id();
        let gc_ttl = self.config.gc_ttl.0;
        let timeout = self.config.sync_ticdc_timeout.0;

        let mut join_set = tokio::task::JoinSet::new();
        // TODO: filter keyspaces by `ks.need_sync()`.
        let ks_ctxs = self.keyspaces.values().map(|ks| ks.ctx.clone());
        for ks_ctx in ks_ctxs {
            let keyspace_id = ks_ctx.keyspace_id;
            let pd = pd.clone();
            let http_client = http_client.clone();
            let gc_service_id = gc_service_id.clone();
            let sub_task = async move {
                let sync_res = Self::sync_changefeeds_from_ticdc(
                    pd.as_ref(),
                    ks_ctx,
                    gc_service_id,
                    gc_ttl,
                    &http_client,
                    timeout,
                )
                .await;
                (keyspace_id, sync_res)
            };
            join_set.spawn(sub_task);
        }

        while let Some(res) = join_set.join_next().await {
            let (keyspace_id, sync_res) = box_try_join!(res);
            match sync_res {
                Ok((updated_ctx, synced_feeds, expired_feeds)) => {
                    let Some(ks) = self.mut_keyspace(keyspace_id) else {
                        continue;
                    };
                    ks.ctx.last_rep_gc_safepoint = updated_ctx.last_rep_gc_safepoint;
                    let maybe_all_expired = synced_feeds.is_empty() && !expired_feeds.is_empty();
                    for feed in synced_feeds {
                        ks.set_feed_state(&feed.id, ChangefeedSyncState::Synced);
                    }
                    for feed in expired_feeds {
                        ks.set_feed_state(&feed.id, ChangefeedSyncState::Expired);
                    }
                    if maybe_all_expired && !ks.need_sync() {
                        Self::remove_keyspace_safepoint_async(
                            pd.as_ref(),
                            keyspace_id,
                            gc_service_id.clone(),
                        )
                        .await;
                    }
                }
                Err(err) => {
                    warn!("ServiceSafepointManager: sync_changefeeds_from_ticdc failed: {:?}", err; "keyspace" => keyspace_id);
                }
            }
        }
        Ok(())
    }

    async fn get_changefeeds_from_ticdc(
        pd: &dyn PdClient,
        keyspace_id: u32,
        cdc_addr: &str,
        http_client: &HttpClient,
        timeout: Duration,
    ) -> Result<ReplicationTaskList> {
        let sec_mgr = pd.get_security_mgr();
        let uri = box_try!(sec_mgr.build_uri(format!("{}/api/v2/changefeeds", cdc_addr)));
        let resp = read_from_ticdc(
            "get_changefeeds_from_ticdc",
            http_client,
            uri,
            timeout,
            Duration::from_millis(200),
            |_| false,
        )
        .await?;
        let changefeed_list =
            serde_json::from_slice::<ReplicationTaskList>(&resp).map_err(|e| -> Error {
                let err_msg = format!("failed to parse changefeeds response: {:?}", e);
                debug_assert!(false, "{}", err_msg);
                box_err!("{}", err_msg)
            })?;
        info!("ServiceSafepointManager: changefeed list: {:?}", changefeed_list; "keyspace" => keyspace_id);
        debug_assert_eq!(changefeed_list.items.len(), changefeed_list.total);
        Ok(changefeed_list)
    }

    async fn sync_changefeeds_from_ticdc(
        pd: &dyn PdClient,
        mut ctx: KeyspaceContext,
        gc_service_id: String,
        gc_ttl: Duration,
        http_client: &HttpClient,
        timeout: Duration,
    ) -> Result<(
        KeyspaceContext,
        Vec<ReplicationTaskItem>, // synced_feeds
        Vec<ReplicationTaskItem>, // expired_feeds
    )> {
        let keyspace_str = ctx.keyspace_id.to_string();
        match Self::get_changefeeds_from_ticdc(
            pd,
            ctx.keyspace_id,
            &ctx.cdc_addr,
            http_client,
            timeout,
        )
        .await
        {
            Ok(feeds_list) => {
                let now = TimeStamp::now();
                let (synced_feeds, expired_feeds): (Vec<_>, Vec<_>) = feeds_list
                    .items
                    .into_iter()
                    .partition(|x| now.duration_since(TimeStamp::new(x.checkpoint_tso)) <= gc_ttl);

                let min_checkpoint_ts_opt = synced_feeds.iter().map(|x| x.checkpoint_tso).min();
                if let Some(min_checkpoint_ts) = min_checkpoint_ts_opt {
                    info!("ServiceSafepointManager: sync_changefeeds";
                        "min_checkpoint_ts" => min_checkpoint_ts, "feeds" => ?synced_feeds, "keyspace" => ctx.keyspace_id);
                    // Update the keyspace even if safepoint not changed, to extend the TTL.
                    Self::update_keyspace_safepoint(
                        pd,
                        ctx.keyspace_id,
                        gc_service_id,
                        min_checkpoint_ts,
                        gc_ttl,
                    )
                    .await;
                }

                if !expired_feeds.is_empty() {
                    warn!("ServiceSafepointManager: expired changefeeds: {:?}", expired_feeds; "keyspace" => ctx.keyspace_id);
                    REP_SAFEPOINT_EVENTS_COUNTER
                        .with_label_values(&["warn", &keyspace_str, "changefeed_expired"])
                        .inc_by(expired_feeds.len() as u64);

                    // Set the rep_pd GC safepoint to make TiCDC consider the GC is blocked by
                    // expired changefeeds, then set the changefeeds failed, and cannot resume.
                    // See `isTiCDCBlockGC` in TiCDC code.
                    let max_expired_checkpoint_ts = expired_feeds
                        .iter()
                        .map(|x| x.checkpoint_tso)
                        .max()
                        .unwrap();
                    if ctx
                        .last_rep_gc_safepoint
                        .is_none_or(|sp| sp < max_expired_checkpoint_ts)
                    {
                        let current_gc_safepoint_opt = Self::update_rep_pd_gc_safepoint(
                            ctx.keyspace_id,
                            ctx.rep_pd_cli.as_ref(),
                            max_expired_checkpoint_ts,
                        )
                        .await;
                        if let Some(current) = current_gc_safepoint_opt {
                            ctx.last_rep_gc_safepoint = Some(current);
                        }
                    }
                }
                Ok((ctx, synced_feeds, expired_feeds))
            }
            Err(err) => {
                warn!("ServiceSafepointManager: get_changefeeds_from_ticdc failed: {:?}", err; "keyspace" => ctx.keyspace_id);
                REP_SAFEPOINT_EVENTS_COUNTER
                    .with_label_values(&["warn", &keyspace_str, "get_changefeeds_from_ticdc"])
                    .inc();
                Err(err)
            }
        }
    }

    // TiDB use the `update_service_safe_pint` API to set the GC safepoint, with
    // service name "gc_worker" and TTL MaxInt64.
    // See https://github.com/pingcap/tidb/blob/v8.5.3/pkg/store/gcworker/gc_worker.go#L719.
    async fn update_rep_pd_gc_safepoint(
        keyspace_id: u32,
        rep_pd_cli: &dyn PdClient,
        safepoint: u64,
    ) -> Option<u64 /* current_gc_safepoint */> {
        let keyspace_str = keyspace_id.to_string();
        match rep_pd_cli
            .update_service_safe_point(
                GC_WORKER_SERVICE_SAFEPOINT_ID.into(),
                safepoint.into(),
                Duration::from_secs(i64::MAX as u64),
            )
            .await
        {
            Ok(()) => {
                info!("ServiceSafepointManager: update_rep_pd_gc_safepoint ok";
                    "safepoint" => safepoint, "keyspace" => keyspace_id);
                REP_SAFEPOINT_EVENTS_COUNTER
                    .with_label_values(&["ok", &keyspace_str, "update_rep_gc_safepoint"])
                    .inc();
                Some(safepoint)
            }
            Err(pd_client::Error::UnsafeServiceGcSafePoint {
                requested,
                current_minimal,
            }) => {
                // Will happen when replication worker restart.
                warn!("ServiceSafepointManager: update_rep_pd_gc_safepoint stale gc safepoint";
                    "requested" => requested, "current" => current_minimal, "keyspace" => keyspace_id);
                REP_SAFEPOINT_EVENTS_COUNTER
                    .with_label_values(&["warn", &keyspace_str, "stale_rep_gc_safepoint"])
                    .inc();
                Some(current_minimal.into_inner())
            }
            Err(err) => {
                warn!("ServiceSafepointManager: update_rep_pd_gc_safepoint failed: {:?}", err;
                    "keyspace" => keyspace_id);
                REP_SAFEPOINT_EVENTS_COUNTER
                    .with_label_values(&["warn", &keyspace_str, "update_rep_gc_safepoint"])
                    .inc();
                None
            }
        }
    }

    fn get_gc_service_id(&self) -> String {
        format!("rep-worker-{}", self.merged_store_id)
    }
}

pub(crate) enum ServiceSafepointTask {
    AddKeyspace {
        keyspace_id: u32,
        cdc_addr: String,
        rep_pd_cli: Arc<dyn PdClient>,
    },
    RemoveKeyspace {
        keyspace_id: u32,
    },
    AddChangefeed {
        keyspace_id: u32,
        feed_id: String,
    },
    RemoveChangefeed {
        keyspace_id: u32,
        feed_id: String,
    },
}

impl fmt::Display for ServiceSafepointTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceSafepointTask::AddKeyspace { .. } => write!(f, "AddKeyspace"),
            ServiceSafepointTask::RemoveKeyspace { .. } => write!(f, "RemoveKeyspace"),
            ServiceSafepointTask::AddChangefeed { .. } => write!(f, "AddChangefeed"),
            ServiceSafepointTask::RemoveChangefeed { .. } => {
                write!(f, "RemoveChangefeed")
            }
        }
    }
}

impl Runnable for ServiceSafepointRunner {
    type Task = ServiceSafepointTask;

    fn run(&mut self, task: Self::Task) {
        match task {
            ServiceSafepointTask::AddKeyspace {
                keyspace_id,
                cdc_addr,
                rep_pd_cli,
            } => {
                match self.keyspaces.entry(keyspace_id) {
                    HashMapEntry::Vacant(e) => {
                        e.insert(KeyspaceChangefeeds {
                            ctx: KeyspaceContext {
                                keyspace_id,
                                cdc_addr,
                                rep_pd_cli,
                                last_rep_gc_safepoint: None,
                            },
                            feeds: HashMap::default(),
                        });
                    }
                    HashMapEntry::Occupied(mut e) => {
                        warn!(
                            "ServiceSafepointManager: add keyspace: keyspace exists: {}",
                            keyspace_id
                        );
                        debug_assert!(false);
                        // Overwrite ctx for safety.
                        e.get_mut().ctx.cdc_addr = cdc_addr;
                        e.get_mut().ctx.rep_pd_cli = rep_pd_cli;
                    }
                }
            }
            ServiceSafepointTask::RemoveKeyspace { keyspace_id } => {
                self.remove_keyspace(keyspace_id);
            }
            ServiceSafepointTask::AddChangefeed {
                keyspace_id,
                feed_id,
            } => {
                self.add_changefeed(keyspace_id, feed_id);
            }
            ServiceSafepointTask::RemoveChangefeed {
                keyspace_id,
                feed_id,
            } => {
                self.remove_changefeed(keyspace_id, &feed_id);
            }
        }
    }
}

impl RunnableWithTimer for ServiceSafepointRunner {
    fn on_timeout(&mut self) {
        self.sync_ticdc();
    }

    fn get_interval(&self) -> Duration {
        self.config.sync_safepoint_interval.0
    }
}
