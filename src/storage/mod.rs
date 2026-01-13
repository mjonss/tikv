// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]

//! This module contains TiKV's transaction layer. It lowers high-level,
//! transactional commands to low-level (raw key-value) interactions with
//! persistent storage.
//!
//! This module is further split into layers: [`txn`](txn) lowers transactional
//! commands to key-value operations on an MVCC abstraction. [`mvcc`](mvcc) is
//! our MVCC implementation. [`kv`](kv) is an abstraction layer over persistent
//! storage.
//!
//! Other responsibilities of this module are managing latches (see
//! [`latch`](txn::latch)), deadlock and wait handling (see
//! [`lock_manager`](lock_manager)), sche duling command execution (see
//! [`txn::scheduler`](txn::scheduler)), and handling commands from the raw and
//! versioned APIs (in the [`Storage`](Storage) struct).
//!
//! For more information about TiKV's transactions, see the [sig-txn docs](https://github.com/tikv/sig-transaction/tree/master/doc).
//!
//! Some important types are:
//!
//! * the [`Engine`](kv::Engine) trait and related traits, which abstracts over
//!   underlying storage,
//! * the [`MvccTxn`](mvcc::txn::MvccTxn) struct, which is the primary object in
//!   the MVCC implementation,
//! * the commands in the [`commands`](txn::commands) module, which are how each
//!   command is implemented,
//! * the [`Storage`](Storage) struct, which is the primary entry point for this
//!   module.
//!
//! Related code:
//!
//! * the [`kv`](crate::server::service::kv) module, which is the interface for
//!   TiKV's APIs,
//! * the [`lock_manager](crate::server::lock_manager), which takes part in lock
//!   and deadlock management,
//! * [`gc_worker`](crate::server::gc_worker), which drives garbage collection
//!   of old values,
//! * the [`txn_types](::txn_types) crate, some important types for this
//!   module's interface,
//! * the [`kvproto`](::kvproto) crate, which defines TiKV's protobuf API and
//!   includes some documentation of the commands implemented here,
//! * the [`test_storage`](::test_storage) crate, integration tests for this
//!   module,
//! * the [`engine_traits`](::engine_traits) crate, more detail of the engine
//!   abstraction.

pub mod config;
pub mod config_manager;
pub mod errors;
pub mod kv;
pub mod lock_manager;
pub(crate) mod metrics;
pub use metrics::SCHED_WRITE_FLOW_GAUGE;
pub mod mvcc;
pub mod txn;

mod read_pool;
mod types;

use std::{
    borrow::Cow,
    iter,
    marker::PhantomData,
    mem,
    sync::{
        Arc,
        atomic::{self, AtomicBool, AtomicU64, Ordering},
    },
};

use api_version::{ApiV1, ApiV2, KeyMode, KvFormat};
use collections::HashMap;
use concurrency_manager::ConcurrencyManager;
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE, CfName, DATA_CFS};
use futures::prelude::*;
use kvproto::{
    kvrpcpb::{ApiVersion, CommandPri, Context, GetRequest, IsolationLevel, KeyRange, LockInfo},
    pdpb::QueryKind,
};
use pd_client::FeatureGate;
use raftstore::{
    RegionInfoAccessor,
    store::{ReadStats, TxnExt, WriteStats, util::build_key_range},
};
use rand::prelude::*;
use resource_metering::{FutureExt, ResourceTagFactory};
use tikv_kv::{OnAppliedCb, SnapshotExt};
use tikv_util::{
    future::try_poll,
    quota_limiter::QuotaLimiter,
    time::{Instant, ThreadReadId, duration_to_ms, duration_to_sec},
};
use tracker::{
    TrackedFuture, TrackerToken, clear_tls_tracker_token, set_tls_tracker_token, with_tls_tracker,
};
use txn_types::{Key, KvPair, Lock, TimeStamp, TsSet, Value};

pub use self::{
    errors::{Error, ErrorHeaderKind, ErrorInner, get_error_kind_from_header, get_tag_from_header},
    kv::{
        CfStatistics, Cursor, CursorBuilder, Engine, FlowStatistics, FlowStatsReporter, Iterator,
        RocksEngine, ScanMode, Snapshot, StageLatencyStats, Statistics, TestEngineBuilder,
    },
    read_pool::{build_read_pool, build_read_pool_for_test},
    txn::{CloudStore, Latches, Lock as LatchLock, ProcessResult, Scanner, Store},
    types::{
        PessimisticLockKeyResult, PessimisticLockResults, PrewriteResult, SecondaryLocksStatus,
        StorageCallback, TxnStatus,
    },
};
use self::{kv::SnapContext, test_util::latest_feature_gate};
use crate::{
    read_pool::{ReadPool, ReadPoolHandle},
    server::lock_manager::waiter_manager,
    storage::{
        config::Config,
        kv::{Modify, WriteData, with_tls_engine},
        lock_manager::{LockManager, MockLockManager},
        metrics::{CommandKind, *},
        txn::{
            Command, commands::TypedCommand, flow_controller::FlowController,
            scheduler::Scheduler as TxnScheduler,
        },
        types::StorageCallbackType,
    },
};

pub type Result<T> = std::result::Result<T, Error>;
pub type Callback<T> = Box<dyn FnOnce(Result<T>) + Send>;

/// [`Storage`](Storage) implements transactional KV APIs and raw KV APIs on a
/// given [`Engine`]. An [`Engine`] provides low level KV functionality.
/// [`Engine`] has multiple implementations. When a TiKV server is running, a
/// [`RaftKv`](crate::server::raftkv::RaftKv) will be the underlying [`Engine`]
/// of [`Storage`]. The other two types of engines are for test purpose.
///
/// [`Storage`] is reference counted and cloning [`Storage`] will just increase
/// the reference counter. Storage resources (i.e. threads, engine) will be
/// released when all references are dropped.
///
/// Notice that read and write methods may not be performed over full data in
/// most cases, i.e. when underlying engine is
/// [`RaftKv`](crate::server::raftkv::RaftKv), which limits data access in the
/// range of a single region according to specified `ctx` parameter. However,
/// [`unsafe_destroy_range`](crate::server::gc_worker::GcTask::
/// UnsafeDestroyRange) is the only exception. It's always performed on the
/// whole TiKV.
///
/// Operations of [`Storage`](Storage) can be divided into two types: MVCC
/// operations and raw operations. MVCC operations uses MVCC keys, which usually
/// consist of several physical keys in different CFs. In default CF and write
/// CF, the key will be memcomparable-encoded and append the timestamp to it, so
/// that multiple versions can be saved at the same time. Raw operations use raw
/// keys, which are saved directly to the engine without memcomparable- encoding
/// and appending timestamp.
pub struct Storage<E: Engine, L: LockManager, F: KvFormat> {
    // TODO: Too many Arcs, would be slow when clone.
    engine: E,

    sched: TxnScheduler<E, L>,

    /// The thread pool used to run most read operations.
    read_pool: ReadPoolHandle,

    concurrency_manager: ConcurrencyManager,

    /// How many strong references. Thread pool and workers will be stopped
    /// once there are no more references.
    // TODO: This should be implemented in thread pool and worker.
    refs: Arc<atomic::AtomicUsize>,

    // Fields below are storage configurations.
    max_key_size: usize,

    resource_tag_factory: ResourceTagFactory,

    api_version: ApiVersion, // TODO: remove this. Use `Api` instead.

    quota_limiter: Arc<QuotaLimiter>,

    _phantom: PhantomData<F>,
}

/// Storage for Api V1
/// To be convenience for test cases unrelated to RawKV.
pub type StorageApiV1<E, L> = Storage<E, L, ApiV1>;

impl<E: Engine, L: LockManager, F: KvFormat> Clone for Storage<E, L, F> {
    #[inline]
    fn clone(&self) -> Self {
        let refs = self.refs.fetch_add(1, atomic::Ordering::SeqCst);

        trace!(
            "Storage referenced"; "original_ref" => refs
        );

        Self {
            engine: self.engine.clone(),
            sched: self.sched.clone(),
            read_pool: self.read_pool.clone(),
            refs: self.refs.clone(),
            max_key_size: self.max_key_size,
            concurrency_manager: self.concurrency_manager.clone(),
            api_version: self.api_version,
            resource_tag_factory: self.resource_tag_factory.clone(),
            quota_limiter: Arc::clone(&self.quota_limiter),
            _phantom: PhantomData,
        }
    }
}

impl<E: Engine, L: LockManager, F: KvFormat> Drop for Storage<E, L, F> {
    #[inline]
    fn drop(&mut self) {
        let refs = self.refs.fetch_sub(1, atomic::Ordering::SeqCst);

        trace!(
            "Storage de-referenced"; "original_ref" => refs
        );

        if refs != 1 {
            return;
        }

        info!("Storage stopped.");
    }
}

macro_rules! check_key_size {
    ($key_iter:expr, $max_key_size:expr, $callback:ident) => {
        for k in $key_iter {
            let key_size = k.len();
            if key_size > $max_key_size {
                $callback(Err(Error::from(ErrorInner::KeyTooLarge {
                    size: key_size,
                    limit: $max_key_size,
                })));
                return Ok(());
            }
        }
    };
}

impl<E: Engine, L: LockManager, F: KvFormat> Storage<E, L, F> {
    /// Create a `Storage` from given engine.
    pub fn from_engine<R: FlowStatsReporter>(
        engine: E,
        config: &Config,
        read_pool: ReadPoolHandle,
        lock_mgr: L,
        concurrency_manager: ConcurrencyManager,
        dynamic_switches: DynamicConfigs,
        flow_controller: Arc<FlowController>,
        reporter: R,
        resource_tag_factory: ResourceTagFactory,
        quota_limiter: Arc<QuotaLimiter>,
        feature_gate: FeatureGate,
        region_info_accessor: Option<RegionInfoAccessor>,
    ) -> Result<Self> {
        assert_eq!(config.api_version(), F::TAG, "Api version not match");

        let sched = TxnScheduler::new(
            engine.clone(),
            lock_mgr,
            concurrency_manager.clone(),
            config,
            dynamic_switches,
            flow_controller,
            reporter,
            resource_tag_factory.clone(),
            Arc::clone(&quota_limiter),
            feature_gate,
            read_pool.clone(),
            region_info_accessor,
        );

        info!("Storage started.");

        Ok(Storage {
            engine,
            sched,
            read_pool,
            concurrency_manager,
            refs: Arc::new(atomic::AtomicUsize::new(1)),
            max_key_size: config.max_key_size,
            api_version: config.api_version(),
            resource_tag_factory,
            quota_limiter,
            _phantom: PhantomData,
        })
    }

    /// Get the underlying `Engine` of the `Storage`.
    pub fn get_engine(&self) -> E {
        self.engine.clone()
    }

    pub fn get_scheduler(&self) -> TxnScheduler<E, L> {
        self.sched.clone()
    }

    pub fn get_concurrency_manager(&self) -> ConcurrencyManager {
        self.concurrency_manager.clone()
    }

    pub fn dump_wait_for_entries(&self, cb: waiter_manager::Callback) {
        self.sched.dump_wait_for_entries(cb);
    }

    /// Get a snapshot of `engine`.
    fn snapshot(
        engine: &mut E,
        ctx: SnapContext<'_>,
    ) -> impl std::future::Future<Output = Result<E::Snap>> {
        kv::snapshot(engine, ctx)
            .map_err(txn::Error::from)
            .map_err(Error::from)
    }

    #[cfg(test)]
    pub fn get_snapshot(&mut self) -> E::Snap {
        self.engine.snapshot(Default::default()).unwrap()
    }

    pub fn get_readpool_queue_per_worker(&self) -> usize {
        self.read_pool.get_queue_size_per_worker()
    }

    pub fn get_normal_pool_size(&self) -> usize {
        self.read_pool.get_normal_pool_size()
    }

    #[inline]
    fn with_tls_engine<R>(f: impl FnOnce(&mut E) -> R) -> R {
        // Safety: the read pools ensure that a TLS engine exists.
        unsafe { with_tls_engine(f) }
    }

    /// Check if key range is valid
    ///
    /// - If `reverse` is true, `end_key` is less than `start_key`. `end_key` is
    ///   the lower bound.
    /// - If `reverse` is false, `end_key` is greater than `start_key`.
    ///   `end_key` is the upper bound.
    #[cfg(test)]
    fn check_key_ranges(ranges: &[KeyRange], reverse: bool) -> bool {
        let ranges_len = ranges.len();
        for i in 0..ranges_len {
            let start_key = ranges[i].get_start_key();
            let mut end_key = ranges[i].get_end_key();
            if end_key.is_empty() && i + 1 != ranges_len {
                end_key = ranges[i + 1].get_start_key();
            }
            if !end_key.is_empty()
                && (!reverse && start_key >= end_key || reverse && start_key <= end_key)
            {
                return false;
            }
        }
        true
    }

    /// Check whether a raw kv command or not.
    #[inline]
    fn is_raw_command(cmd: CommandKind) -> bool {
        matches!(
            cmd,
            CommandKind::raw_batch_get_command
                | CommandKind::raw_get
                | CommandKind::raw_batch_get
                | CommandKind::raw_scan
                | CommandKind::raw_batch_scan
                | CommandKind::raw_put
                | CommandKind::raw_batch_put
                | CommandKind::raw_delete
                | CommandKind::raw_delete_range
                | CommandKind::raw_batch_delete
                | CommandKind::raw_get_key_ttl
                | CommandKind::raw_compare_and_swap
                | CommandKind::raw_atomic_store
                | CommandKind::raw_checksum
        )
    }

    /// Check whether a trancsation kv command or not.
    #[inline]
    fn is_txn_command(cmd: CommandKind) -> bool {
        !Self::is_raw_command(cmd)
    }

    /// Check api version.
    ///
    /// When config.api_version = V1: accept request of V1 only.
    /// When config.api_version = V2: accept the following:
    ///   * Request of V1 from TiDB, for compatibility.
    ///   * Request of V2 with legal prefix. See the following for detail:
    ///     * rfc: https://github.com/tikv/rfcs/blob/master/text/0069-api-v2.md.
    ///     * proto: https://github.com/pingcap/kvproto/blob/master/proto/kvrpcpb.proto,
    ///       enum APIVersion.
    // TODO: refactor to use `Api` parameter.
    fn check_api_version(
        storage_api_version: ApiVersion,
        req_api_version: ApiVersion,
        cmd: CommandKind,
        keys: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> Result<()> {
        match (storage_api_version, req_api_version) {
            (ApiVersion::V1, ApiVersion::V1) => {}
            (ApiVersion::V1ttl, ApiVersion::V1) if Self::is_raw_command(cmd) => {
                // storage api_version = V1ttl, allow RawKV request only.
            }
            (ApiVersion::V2, ApiVersion::V1) if Self::is_txn_command(cmd) => {
                // For compatibility, accept TiDB request only.
                for key in keys {
                    if ApiV2::parse_key_mode(key.as_ref()) != KeyMode::Tidb {
                        return Err(ErrorInner::invalid_key_mode(
                            cmd,
                            storage_api_version,
                            key.as_ref(),
                        )
                        .into());
                    }
                }
            }
            (ApiVersion::V2, ApiVersion::V2) if Self::is_raw_command(cmd) => {
                for key in keys {
                    if ApiV2::parse_key_mode(key.as_ref()) != KeyMode::Raw {
                        return Err(ErrorInner::invalid_key_mode(
                            cmd,
                            storage_api_version,
                            key.as_ref(),
                        )
                        .into());
                    }
                }
            }
            (ApiVersion::V2, ApiVersion::V2) if Self::is_txn_command(cmd) => {
                for key in keys {
                    if ApiV2::parse_key_mode(key.as_ref()) != KeyMode::Txn {
                        return Err(ErrorInner::invalid_key_mode(
                            cmd,
                            storage_api_version,
                            key.as_ref(),
                        )
                        .into());
                    }
                }
            }
            _ => {
                return Err(Error::from(ErrorInner::ApiVersionNotMatched {
                    cmd,
                    storage_api_version,
                    req_api_version,
                }));
            }
        }
        Ok(())
    }

    // TODO: refactor to use `Api` parameter.
    fn check_api_version_ranges(
        storage_api_version: ApiVersion,
        req_api_version: ApiVersion,
        cmd: CommandKind,
        ranges: impl IntoIterator<Item = (Option<impl AsRef<[u8]>>, Option<impl AsRef<[u8]>>)>,
    ) -> Result<()> {
        match (storage_api_version, req_api_version) {
            (ApiVersion::V1, ApiVersion::V1) => {}
            (ApiVersion::V1ttl, ApiVersion::V1) if Self::is_raw_command(cmd) => {
                // storage api_version = V1ttl, allow RawKV request only.
            }
            (ApiVersion::V2, ApiVersion::V1) if Self::is_txn_command(cmd) => {
                // For compatibility, accept TiDB request only.
                for range in ranges {
                    let range = (
                        range.0.as_ref().map(AsRef::as_ref),
                        range.1.as_ref().map(AsRef::as_ref),
                    );
                    if ApiV2::parse_range_mode(range) != KeyMode::Tidb {
                        return Err(ErrorInner::invalid_key_range_mode(
                            cmd,
                            storage_api_version,
                            range,
                        )
                        .into());
                    }
                }
            }
            (ApiVersion::V2, ApiVersion::V2) if Self::is_raw_command(cmd) => {
                for range in ranges {
                    let range = (
                        range.0.as_ref().map(AsRef::as_ref),
                        range.1.as_ref().map(AsRef::as_ref),
                    );
                    if ApiV2::parse_range_mode(range) != KeyMode::Raw {
                        return Err(ErrorInner::invalid_key_range_mode(
                            cmd,
                            storage_api_version,
                            range,
                        )
                        .into());
                    }
                }
            }
            (ApiVersion::V2, ApiVersion::V2) if Self::is_txn_command(cmd) => {
                for range in ranges {
                    let range = (
                        range.0.as_ref().map(AsRef::as_ref),
                        range.1.as_ref().map(AsRef::as_ref),
                    );
                    if ApiV2::parse_range_mode(range) != KeyMode::Txn {
                        return Err(ErrorInner::invalid_key_range_mode(
                            cmd,
                            storage_api_version,
                            range,
                        )
                        .into());
                    }
                }
            }
            _ => {
                return Err(Error::from(ErrorInner::ApiVersionNotMatched {
                    cmd,
                    storage_api_version,
                    req_api_version,
                }));
            }
        }
        Ok(())
    }

    /// Get value of the given key from a snapshot.
    ///
    /// Only writes that are committed before `start_ts` are visible.
    pub fn get(
        &self,
        mut ctx: Context,
        key: Key,
        start_ts: TimeStamp,
    ) -> impl Future<Output = Result<(Option<Value>, KvGetStatistics)>> {
        let stage_begin_ts = Instant::now();
        const CMD: CommandKind = CommandKind::get;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let concurrency_manager = self.concurrency_manager.clone();
        let api_version = self.api_version;

        let quota_limiter = self.quota_limiter.clone();
        let mut sample = quota_limiter.new_sample(true);

        tls_collect_query(
            ctx.get_region_id(),
            ctx.get_peer(),
            key.as_encoded(),
            key.as_encoded(),
            false,
            QueryKind::Get,
        );
        self.maybe_flush_read_stats();

        KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
        SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
            .get(priority_tag)
            .inc();
        let mut engine = self.engine.clone();
        let read_pool = self.read_pool.clone();
        let task_id = thread_rng().next_u64();
        async move {
            Self::check_api_version(api_version, ctx.api_version, CMD, [key.as_encoded()])?;

            let command_duration = Instant::now();

            // The bypass_locks and access_locks set will be checked at most once.
            // `TsSet::vec` is more efficient here.
            let bypass_locks = TsSet::vec_from_u64s(ctx.take_resolved_locks());

            let snap_ctx = prepare_snap_ctx(
                &ctx,
                iter::once(&key),
                start_ts,
                &bypass_locks,
                &concurrency_manager,
                CMD,
            )?;
            let snapshot = Self::snapshot(&mut engine, snap_ctx).await?;
            let is_sync = snapshot.is_sync();
            let task = async move {
                let begin_instant = Instant::now();
                let stage_snap_recv_ts = begin_instant;
                let buckets = snapshot.ext().get_buckets();
                let mut statistics = Statistics::default();
                let result = {
                    let snap_store = CloudStore::new(
                        snapshot,
                        start_ts.into_inner(),
                        bypass_locks,
                        !ctx.get_not_fill_cache(),
                    );
                    let res = if snap_store.is_sync() {
                        let _guard = sample.observe_cpu();
                        snap_store.get(&key, &mut statistics)
                    } else {
                        let (cpu_time, res) = sample
                            .observe_cpu_async(snap_store.get_async(&key, &mut statistics))
                            .await;
                        sample.add_cpu_time(cpu_time);
                        res
                    };
                    // map storage::txn::Error -> storage::Error
                    res.map_err(Error::from).inspect(|_| {
                        KV_COMMAND_KEYREAD_HISTOGRAM_STATIC.get(CMD).observe(1_f64);
                    })
                };
                metrics::tls_collect_scan_details(CMD, &statistics);
                metrics::tls_collect_read_flow(
                    ctx.get_region_id(),
                    Some(key.as_encoded()),
                    Some(key.as_encoded()),
                    &statistics,
                    buckets.as_ref(),
                );
                let now = Instant::now();
                SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                    .get(CMD)
                    .observe(duration_to_sec(
                        now.saturating_duration_since(begin_instant),
                    ));
                SCHED_HISTOGRAM_VEC_STATIC.get(CMD).observe(duration_to_sec(
                    now.saturating_duration_since(command_duration),
                ));

                let read_bytes = key.len()
                    + result
                        .as_ref()
                        .unwrap_or(&None)
                        .as_ref()
                        .map_or(0, |v| v.len());
                sample.add_read_bytes(read_bytes);
                let quota_delay = quota_limiter.consume_sample(sample, true).await;
                if !quota_delay.is_zero() {
                    TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC
                        .get(CMD)
                        .inc_by(quota_delay.as_micros() as u64);
                }

                let stage_finished_ts = Instant::now();
                let snapshot_wait_time =
                    stage_snap_recv_ts.saturating_duration_since(stage_begin_ts);
                let wait_wall_time = stage_snap_recv_ts.saturating_duration_since(stage_begin_ts);
                let process_wall_time =
                    stage_finished_ts.saturating_duration_since(stage_snap_recv_ts);
                let latency_stats = StageLatencyStats {
                    schedule_wait_time_ms: 0,
                    snapshot_wait_time_ms: duration_to_ms(snapshot_wait_time),
                    wait_wall_time_ms: duration_to_ms(wait_wall_time),
                    process_wall_time_ms: duration_to_ms(process_wall_time),
                };
                Ok((
                    result?,
                    KvGetStatistics {
                        stats: statistics,
                        latency_stats,
                    },
                ))
            };
            if is_sync {
                task.await
            } else {
                read_pool
                    .spawn_handle(task, priority, task_id)
                    .map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                    .await?
            }
        }
    }

    fn maybe_flush_read_stats(&self) {
        if let Some(read_stats) = tls_maybe_take_read_stats() {
            let _ = self.read_pool.spawn(
                async move {
                    tls_merge_read_stats(read_stats);
                },
                CommandPri::Normal,
                thread_rng().next_u64(),
            );
        }
    }

    /// Get values of a set of keys with separate context from a snapshot,
    /// return a list of `Result`s.
    ///
    /// Only writes that are committed before their respective `start_ts` are
    /// visible.
    pub fn batch_get_command<P: 'static + ResponseBatchConsumer<(Option<Vec<u8>>, Statistics)>>(
        &self,
        requests: Vec<GetRequest>,
        ids: Vec<u64>,
        trackers: Vec<TrackerToken>,
        consumer: P,
        begin_instant: Instant,
    ) -> impl Future<Output = Result<()>> {
        const CMD: CommandKind = CommandKind::batch_get_command;
        // all requests in a batch have the same region, epoch, term, replica_read
        let priority = requests[0].get_context().get_priority();
        let concurrency_manager = self.concurrency_manager.clone();
        let api_version = self.api_version;

        // The resource tags of these batched requests are not the same, and it is quite
        // expensive to distinguish them, so we can find random one of them as a
        // representative.
        let rand_index = rand::thread_rng().gen_range(0, requests.len());
        let rand_ctx = requests[rand_index].get_context();
        let rand_key = requests[rand_index].get_key().to_vec();
        let resource_tag = self
            .resource_tag_factory
            .new_tag_with_key_ranges(rand_ctx, vec![(rand_key.clone(), rand_key)]);
        // Unset the TLS tracker because the future below does not belong to any
        // specific request
        clear_tls_tracker_token();
        let res = self.read_pool.spawn_handle(
            async move {
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                    .get(CMD)
                    .observe(requests.len() as f64);
                let command_duration = Instant::now();
                let read_id = Some(ThreadReadId::new());
                let mut statistics = Statistics::default();
                let mut req_snaps = vec![];

                for ((mut req, id), tracker) in requests.into_iter().zip(ids).zip(trackers) {
                    set_tls_tracker_token(tracker);
                    let mut ctx = req.take_context();
                    let source = ctx.take_request_source();
                    let region_id = ctx.get_region_id();
                    let peer = ctx.get_peer();

                    let key = Key::from_raw(req.get_key());
                    tls_collect_query(
                        region_id,
                        peer,
                        key.as_encoded(),
                        key.as_encoded(),
                        false,
                        QueryKind::Get,
                    );

                    Self::check_api_version(api_version, ctx.api_version, CMD, [key.as_encoded()])?;

                    let start_ts = req.get_version().into();
                    let isolation_level = ctx.get_isolation_level();
                    let fill_cache = !ctx.get_not_fill_cache();
                    let bypass_locks = TsSet::vec_from_u64s(ctx.take_resolved_locks());
                    let access_locks = TsSet::vec_from_u64s(ctx.take_committed_locks());
                    let region_id = ctx.get_region_id();

                    let snap_ctx = match prepare_snap_ctx(
                        &ctx,
                        iter::once(&key),
                        start_ts,
                        &bypass_locks,
                        &concurrency_manager,
                        CMD,
                    ) {
                        Ok(mut snap_ctx) => {
                            snap_ctx.read_id = if ctx.get_stale_read() {
                                None
                            } else {
                                read_id.clone()
                            };
                            snap_ctx
                        }
                        Err(e) => {
                            consumer.consume(id, Err(e), begin_instant, source);
                            continue;
                        }
                    };

                    let snap = Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx));
                    req_snaps.push((
                        TrackedFuture::new(snap),
                        key,
                        start_ts,
                        isolation_level,
                        fill_cache,
                        bypass_locks,
                        access_locks,
                        region_id,
                        id,
                        source,
                        tracker,
                    ));
                }
                for req_snap in req_snaps {
                    let (
                        snap,
                        key,
                        start_ts,
                        _isolation_level,
                        fill_cache,
                        bypass_locks,
                        _access_locks,
                        _region_id,
                        id,
                        source,
                        tracker,
                    ) = req_snap;
                    let snap_res = snap.await;
                    set_tls_tracker_token(tracker);
                    match snap_res {
                        Ok(snapshot) => {
                            if snapshot.get_kvengine_snap().is_some() {
                                let cloud_store = CloudStore::new(
                                    snapshot,
                                    start_ts.into_inner(),
                                    bypass_locks,
                                    fill_cache,
                                );
                                let mut stat = Statistics::default();
                                let v = if cloud_store.is_sync() {
                                    cloud_store.get(&key, &mut stat)
                                } else {
                                    cloud_store.get_async(&key, &mut stat).await
                                };
                                statistics.add(&stat);
                                consumer.consume(
                                    id,
                                    v.map_err(|e| Error::from(e)).map(|v| (v, stat)),
                                    begin_instant,
                                    source,
                                );
                            } else {
                                unimplemented!()
                            }
                        }
                        Err(e) => {
                            consumer.consume(id, Err(e), begin_instant, source);
                        }
                    }
                }
                metrics::tls_collect_scan_details(CMD, &statistics);
                SCHED_HISTOGRAM_VEC_STATIC
                    .get(CMD)
                    .observe(command_duration.saturating_elapsed_secs());

                Ok(())
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );
        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    /// Get values of a set of keys in a batch from the snapshot.
    ///
    /// Only writes that are committed before `start_ts` are visible.
    pub fn batch_get(
        &self,
        mut ctx: Context,
        keys: Vec<Key>,
        start_ts: TimeStamp,
    ) -> impl Future<Output = Result<(Vec<Result<KvPair>>, KvGetStatistics)>> {
        let stage_begin_ts = Instant::now();
        const CMD: CommandKind = CommandKind::batch_get;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let key_ranges = keys
            .iter()
            .map(|k| (k.as_encoded().to_vec(), k.as_encoded().to_vec()))
            .collect();
        let resource_tag = self
            .resource_tag_factory
            .new_tag_with_key_ranges(&ctx, key_ranges);
        let concurrency_manager = self.concurrency_manager.clone();
        let api_version = self.api_version;
        let quota_limiter = self.quota_limiter.clone();
        let mut sample = quota_limiter.new_sample(true);
        let res = self.read_pool.spawn_handle(
            async move {
                let stage_scheduled_ts = Instant::now();
                let mut key_ranges = vec![];
                for key in &keys {
                    key_ranges.push(build_key_range(key.as_encoded(), key.as_encoded(), false));
                }
                tls_collect_query_batch(
                    ctx.get_region_id(),
                    ctx.get_peer(),
                    key_ranges,
                    QueryKind::Get,
                );

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                Self::check_api_version(
                    api_version,
                    ctx.api_version,
                    CMD,
                    keys.iter().map(Key::as_encoded),
                )?;

                let command_duration = Instant::now();

                let bypass_locks = TsSet::from_u64s(ctx.take_resolved_locks());

                let snap_ctx = prepare_snap_ctx(
                    &ctx,
                    &keys,
                    start_ts,
                    &bypass_locks,
                    &concurrency_manager,
                    CMD,
                )?;
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                {
                    let begin_instant = Instant::now();

                    let stage_snap_recv_ts = begin_instant;
                    let mut statistics = Vec::with_capacity(keys.len());
                    let buckets = snapshot.ext().get_buckets();
                    let (result, stats) = {
                        let snap_store = CloudStore::new(
                            snapshot,
                            start_ts.into_inner(),
                            bypass_locks,
                            !ctx.get_not_fill_cache(),
                        );
                        let mut stats = Statistics::default();
                        let res = if snap_store.is_sync() {
                            let _guard = sample.observe_cpu();
                            snap_store.batch_get(&keys, &mut statistics)
                        } else {
                            let (cpu_time, res) = sample
                                .observe_cpu_async(
                                    snap_store.batch_get_async(&keys, &mut statistics),
                                )
                                .await;
                            sample.add_cpu_time(cpu_time);
                            res
                        };
                        let result = res.map_err(Error::from).map(|v| {
                            let kv_pairs: Vec<_> = v
                                .into_iter()
                                .zip(keys)
                                .enumerate()
                                .filter(|&(i, (ref v, ref k))| {
                                    metrics::tls_collect_read_flow(
                                        ctx.get_region_id(),
                                        Some(k.as_encoded()),
                                        Some(k.as_encoded()),
                                        &statistics[i],
                                        buckets.as_ref(),
                                    );
                                    stats.add(&statistics[i]);
                                    !(v.is_ok() && v.as_ref().unwrap().is_none())
                                })
                                .map(|(_, (v, k))| match v {
                                    Ok(Some(x)) => Ok((k.into_raw().unwrap(), x)),
                                    Err(e) => Err(Error::from(e)),
                                    _ => unreachable!(),
                                })
                                .collect();
                            KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                                .get(CMD)
                                .observe(kv_pairs.len() as f64);
                            kv_pairs
                        });
                        (result, stats)
                    };
                    metrics::tls_collect_scan_details(CMD, &stats);
                    let now = Instant::now();
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(duration_to_sec(
                            now.saturating_duration_since(begin_instant),
                        ));
                    SCHED_HISTOGRAM_VEC_STATIC.get(CMD).observe(duration_to_sec(
                        now.saturating_duration_since(command_duration),
                    ));

                    let read_bytes = stats.cf_statistics(CF_DEFAULT).flow_stats.read_bytes
                        + stats.cf_statistics(CF_LOCK).flow_stats.read_bytes
                        + stats.cf_statistics(CF_WRITE).flow_stats.read_bytes;
                    sample.add_read_bytes(read_bytes);
                    let quota_delay = quota_limiter.consume_sample(sample, true).await;
                    if !quota_delay.is_zero() {
                        TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC
                            .get(CMD)
                            .inc_by(quota_delay.as_micros() as u64);
                    }

                    let stage_finished_ts = Instant::now();
                    let schedule_wait_time =
                        stage_scheduled_ts.saturating_duration_since(stage_begin_ts);
                    let snapshot_wait_time =
                        stage_snap_recv_ts.saturating_duration_since(stage_scheduled_ts);
                    let wait_wall_time =
                        stage_snap_recv_ts.saturating_duration_since(stage_begin_ts);
                    let process_wall_time =
                        stage_finished_ts.saturating_duration_since(stage_snap_recv_ts);
                    with_tls_tracker(|tracker| {
                        tracker.metrics.read_pool_schedule_wait_nanos =
                            schedule_wait_time.as_nanos() as u64;
                    });
                    let latency_stats = StageLatencyStats {
                        schedule_wait_time_ms: duration_to_ms(schedule_wait_time),
                        snapshot_wait_time_ms: duration_to_ms(snapshot_wait_time),
                        wait_wall_time_ms: duration_to_ms(wait_wall_time),
                        process_wall_time_ms: duration_to_ms(process_wall_time),
                    };
                    Ok((
                        result?,
                        KvGetStatistics {
                            stats,
                            latency_stats,
                        },
                    ))
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    /// Scan keys in [`start_key`, `end_key`) up to `limit` keys from the
    /// snapshot. If `reverse_scan` is true, it scans [`end_key`,
    /// `start_key`) in descending order. If `end_key` is `None`, it means
    /// the upper bound or the lower bound if reverse scan is unbounded.
    ///
    /// Only writes committed before `start_ts` are visible.
    pub fn scan(
        &self,
        mut ctx: Context,
        start_key: Key,
        end_key: Option<Key>,
        limit: usize,
        sample_step: usize,
        start_ts: TimeStamp,
        key_only: bool,
        reverse_scan: bool,
    ) -> impl Future<Output = Result<Vec<Result<KvPair>>>> {
        const CMD: CommandKind = CommandKind::scan;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self.resource_tag_factory.new_tag_with_key_ranges(
            &ctx,
            vec![(
                start_key.as_encoded().to_vec(),
                match &end_key {
                    Some(k) => k.as_encoded().to_vec(),
                    None => vec![],
                },
            )],
        );
        let concurrency_manager = self.concurrency_manager.clone();
        let api_version = self.api_version;

        let res = self.read_pool.spawn_handle(
            async move {
                {
                    let end_key = match &end_key {
                        Some(k) => k.as_encoded().as_slice(),
                        None => &[],
                    };
                    tls_collect_query(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        start_key.as_encoded(),
                        end_key,
                        reverse_scan,
                        QueryKind::Scan,
                    );
                }
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                Self::check_api_version_ranges(
                    api_version,
                    ctx.api_version,
                    CMD,
                    [(
                        Some(start_key.as_encoded()),
                        end_key.as_ref().map(Key::as_encoded),
                    )],
                )?;

                let (mut start_key, mut end_key) = (Some(start_key), end_key);
                if reverse_scan {
                    std::mem::swap(&mut start_key, &mut end_key);
                }
                let command_duration = Instant::now();

                let bypass_locks = TsSet::from_u64s(ctx.take_resolved_locks());

                // Update max_ts and check the in-memory lock table before getting the snapshot
                if !ctx.get_stale_read() {
                    concurrency_manager.update_max_ts(start_ts);
                }
                if need_check_locks(ctx.get_isolation_level()) {
                    let begin_instant = Instant::now();
                    concurrency_manager
                        .read_range_check(start_key.as_ref(), end_key.as_ref(), |key, lock| {
                            Lock::check_ts_conflict(
                                Cow::Borrowed(lock),
                                key,
                                start_ts,
                                &bypass_locks,
                                ctx.get_isolation_level(),
                            )
                        })
                        .map_err(|e| {
                            CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                                .get(CMD)
                                .locked
                                .observe(begin_instant.saturating_elapsed().as_secs_f64());
                            txn::Error::from_mvcc(e)
                        })?;
                    CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                        .get(CMD)
                        .unlocked
                        .observe(begin_instant.saturating_elapsed().as_secs_f64());
                }

                let mut snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    start_ts: Some(start_ts),
                    ..Default::default()
                };
                let mut key_range = KeyRange::default();
                if let Some(start_key) = &start_key {
                    key_range.set_start_key(start_key.as_encoded().to_vec());
                }
                if let Some(end_key) = &end_key {
                    key_range.set_end_key(end_key.as_encoded().to_vec());
                }
                if need_check_locks_in_replica_read(&ctx) {
                    snap_ctx.key_ranges = vec![key_range.clone()];
                }

                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                {
                    let begin_instant = Instant::now();
                    let buckets = snapshot.ext().get_buckets();

                    let mut snap_store = CloudStore::new(
                        snapshot,
                        start_ts.into_inner(),
                        bypass_locks,
                        !ctx.get_not_fill_cache(),
                    );
                    let (mut scanner, res) = if snap_store.is_sync() {
                        let mut scanner = snap_store.scanner(
                            reverse_scan,
                            key_only,
                            false,
                            start_key,
                            end_key,
                        )?;
                        let res = scanner.scan(limit, sample_step);
                        (scanner, res)
                    } else {
                        let mut scanner = snap_store
                            .scanner_async(reverse_scan, key_only, false, start_key, end_key)
                            .await?;
                        let res = scanner.scan_async(limit, sample_step).await;
                        (scanner, res)
                    };

                    let statistics = scanner.take_statistics();
                    metrics::tls_collect_scan_details(CMD, &statistics);
                    metrics::tls_collect_read_flow(
                        ctx.get_region_id(),
                        Some(key_range.get_start_key()),
                        Some(key_range.get_end_key()),
                        &statistics,
                        buckets.as_ref(),
                    );
                    let now = Instant::now();
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(duration_to_sec(
                            now.saturating_duration_since(begin_instant),
                        ));
                    SCHED_HISTOGRAM_VEC_STATIC.get(CMD).observe(duration_to_sec(
                        now.saturating_duration_since(command_duration),
                    ));

                    res.map_err(Error::from).map(|results| {
                        KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                            .get(CMD)
                            .observe(results.len() as f64);
                        results
                            .into_iter()
                            .map(|x| x.map_err(Error::from))
                            .collect()
                    })
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    pub fn scan_lock(
        &self,
        mut ctx: Context,
        max_ts: TimeStamp,
        start_key: Option<Key>,
        end_key: Option<Key>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<LockInfo>>> {
        const CMD: CommandKind = CommandKind::scan_lock;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self.resource_tag_factory.new_tag_with_key_ranges(
            &ctx,
            vec![(
                match &start_key {
                    Some(k) => k.as_encoded().to_vec(),
                    None => vec![],
                },
                match &end_key {
                    Some(k) => k.as_encoded().to_vec(),
                    None => vec![],
                },
            )],
        );
        let concurrency_manager = self.concurrency_manager.clone();
        // Do not allow replica read for scan_lock.
        ctx.set_replica_read(false);

        let res = self.read_pool.spawn_handle(
            async move {
                if let Some(start_key) = &start_key {
                    let end_key = match &end_key {
                        Some(k) => k.as_encoded().as_slice(),
                        None => &[],
                    };
                    tls_collect_query(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        start_key.as_encoded(),
                        end_key,
                        false,
                        QueryKind::Scan,
                    );
                }

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                // Do not check_api_version in scan_lock, to be compatible with TiDB gc-worker,
                // which resolves locks on regions, and boundary of regions will be out of range
                // of TiDB keys.

                let command_duration = Instant::now();

                concurrency_manager.update_max_ts(max_ts);
                let begin_instant = Instant::now();
                // TODO: Though it's very unlikely to find a conflicting memory lock here, it's
                // not a good idea to return an error to the client, making the GC fail. A
                // better approach is to wait for these locks to be unlocked.
                concurrency_manager.read_range_check(
                    start_key.as_ref(),
                    end_key.as_ref(),
                    |key, lock| {
                        // `Lock::check_ts_conflict` can't be used here, because LockType::Lock
                        // can't be ignored in this case.
                        if lock.ts <= max_ts {
                            CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                                .get(CMD)
                                .locked
                                .observe(begin_instant.saturating_elapsed().as_secs_f64());
                            Err(txn::Error::from_mvcc(mvcc::ErrorInner::KeyIsLocked(
                                lock.clone().into_lock_info(key.to_raw()?),
                            )))
                        } else {
                            Ok(())
                        }
                    },
                )?;
                CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                    .get(CMD)
                    .unlocked
                    .observe(begin_instant.saturating_elapsed().as_secs_f64());

                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };

                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                {
                    let begin_instant = Instant::now();
                    let mut statistics = Statistics::default();
                    let buckets = snapshot.ext().get_buckets();
                    let mut reader = mvcc::CloudReader::new(
                        snapshot.get_kvengine_snap().unwrap().clone(),
                        !ctx.get_not_fill_cache(),
                    );
                    let result = reader
                        .scan_locks(
                            start_key.as_ref(),
                            end_key.as_ref(),
                            |lock| lock.ts <= max_ts,
                            limit,
                        )
                        .map_err(txn::Error::from);
                    statistics.add(&reader.statistics);
                    let (kv_pairs, _) = result?;
                    let mut locks = Vec::with_capacity(kv_pairs.len());
                    for (key, lock) in kv_pairs {
                        let lock_info =
                            lock.into_lock_info(key.into_raw().map_err(txn::Error::from)?);
                        locks.push(lock_info);
                    }

                    metrics::tls_collect_scan_details(CMD, &statistics);
                    metrics::tls_collect_read_flow(
                        ctx.get_region_id(),
                        start_key.as_ref().map(|key| key.as_encoded().as_slice()),
                        end_key.as_ref().map(|key| key.as_encoded().as_slice()),
                        &statistics,
                        buckets.as_ref(),
                    );
                    let now = Instant::now();
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(duration_to_sec(
                            now.saturating_duration_since(begin_instant),
                        ));
                    SCHED_HISTOGRAM_VEC_STATIC.get(CMD).observe(duration_to_sec(
                        now.saturating_duration_since(command_duration),
                    ));

                    Ok(locks)
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );
        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    // The entry point of the storage scheduler. Not only transaction commands need
    // to access keys serially.
    pub fn sched_txn_command<T: StorageCallbackType>(
        &self,
        cmd: TypedCommand<T>,
        callback: Callback<T>,
    ) -> Result<()> {
        use crate::storage::txn::commands::{
            AcquirePessimisticLock, AcquirePessimisticLockResumed, Prewrite, PrewritePessimistic,
        };

        let cmd: Command = cmd.into();
        if recovery::check_request_rejected(cmd.ctx().keyspace_id) {
            callback(Err(box_err!("rejected in recovery mode")));
            return Ok(());
        }

        match &cmd {
            Command::Prewrite(Prewrite { mutations, .. }) => {
                let keys = mutations.iter().map(|m| m.key().as_encoded());
                Self::check_api_version(
                    self.api_version,
                    cmd.ctx().api_version,
                    CommandKind::prewrite,
                    keys.clone(),
                )?;
                check_key_size!(keys, self.max_key_size, callback);
            }
            Command::PrewritePessimistic(PrewritePessimistic { mutations, .. }) => {
                let keys = mutations.iter().map(|(m, _)| m.key().as_encoded());
                Self::check_api_version(
                    self.api_version,
                    cmd.ctx().api_version,
                    CommandKind::prewrite,
                    keys.clone(),
                )?;
                check_key_size!(keys, self.max_key_size, callback);
            }
            Command::AcquirePessimisticLock(AcquirePessimisticLock { keys, .. }) => {
                let keys = keys.iter().map(|k| k.0.as_encoded());
                Self::check_api_version(
                    self.api_version,
                    cmd.ctx().api_version,
                    CommandKind::prewrite,
                    keys.clone(),
                )?;
                check_key_size!(keys, self.max_key_size, callback);
            }
            Command::AcquirePessimisticLockResumed(AcquirePessimisticLockResumed {
                items, ..
            }) => {
                let keys = items.iter().map(|item| item.key.as_encoded());
                Self::check_api_version(
                    self.api_version,
                    cmd.ctx().api_version,
                    CommandKind::acquire_pessimistic_lock_resumed,
                    keys.clone(),
                )?;
                check_key_size!(keys, self.max_key_size, callback);
            }
            _ => {}
        }
        with_tls_tracker(|tracker| {
            tracker.req_info.start_ts = cmd.ts().into_inner();
            tracker.req_info.request_type = cmd.request_type();
        });

        fail_point!("storage_drop_message", |_| Ok(()));
        cmd.incr_cmd_metric();
        self.sched.run_cmd(cmd, T::callback(callback));

        Ok(())
    }

    /// Delete all keys in the range [`start_key`, `end_key`).
    ///
    /// All keys in the range will be deleted permanently regardless of their
    /// timestamps. This means that deleted keys will not be retrievable by
    /// specifying an older timestamp. If `notify_only` is set, the data will
    /// not be immediately deleted, but the operation will still be replicated
    /// via Raft. This is used to notify that the data will be deleted by
    /// [`unsafe_destroy_range`](crate::server::gc_worker::GcTask::
    /// UnsafeDestroyRange) soon.
    pub fn delete_range(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Key,
        notify_only: bool,
        callback: Callback<()>,
    ) -> Result<()> {
        Self::check_api_version_ranges(
            self.api_version,
            ctx.api_version,
            CommandKind::delete_range,
            [(Some(start_key.as_encoded()), Some(end_key.as_encoded()))],
        )?;

        let mut modifies = Vec::with_capacity(DATA_CFS.len());
        for cf in DATA_CFS {
            modifies.push(Modify::DeleteRange(
                cf,
                start_key.clone(),
                end_key.clone(),
                notify_only,
            ));
        }

        let mut batch = WriteData::from_modifies(modifies);
        batch.set_allowed_on_disk_almost_full();
        let res = kv::write(
            &self.engine,
            &ctx,
            batch,
            Some(Box::new(|res| {
                callback(mem::replace(res, Ok(())).map_err(Error::from))
            })),
        );
        // TODO: perhaps change delete_range API to return future.
        if let Some(Some(Err(e))) = try_poll(res) {
            return Err(Error::from(e));
        }
        KV_COMMAND_COUNTER_VEC_STATIC.delete_range.inc();
        Ok(())
    }
}

pub struct DynamicConfigs {
    pub pipelined_pessimistic_lock: Arc<AtomicBool>,
    pub in_memory_pessimistic_lock: Arc<AtomicBool>,
    pub wake_up_delay_duration_ms: Arc<AtomicU64>,
}

fn get_priority_tag(priority: CommandPri) -> CommandPriority {
    match priority {
        CommandPri::Low => CommandPriority::low,
        CommandPri::Normal => CommandPriority::normal,
        CommandPri::High => CommandPriority::high,
    }
}

fn prepare_snap_ctx<'a>(
    pb_ctx: &'a Context,
    keys: impl IntoIterator<Item = &'a Key> + Clone,
    start_ts: TimeStamp,
    bypass_locks: &'a TsSet,
    concurrency_manager: &ConcurrencyManager,
    cmd: CommandKind,
) -> Result<SnapContext<'a>> {
    // Update max_ts and check the in-memory lock table before getting the snapshot
    if !pb_ctx.get_stale_read() {
        concurrency_manager.update_max_ts(start_ts);
    }
    fail_point!("before-storage-check-memory-locks");
    let isolation_level = pb_ctx.get_isolation_level();
    if need_check_locks(isolation_level) {
        let begin_instant = Instant::now();
        for key in keys.clone() {
            concurrency_manager
                .read_key_check(key, |lock| {
                    // No need to check access_locks because they are committed which means they
                    // can't be in memory lock table.
                    Lock::check_ts_conflict(
                        Cow::Borrowed(lock),
                        key,
                        start_ts,
                        bypass_locks,
                        isolation_level,
                    )
                })
                .map_err(|e| {
                    CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                        .get(cmd)
                        .locked
                        .observe(begin_instant.saturating_elapsed().as_secs_f64());
                    txn::Error::from_mvcc(e)
                })?;
        }
        CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
            .get(cmd)
            .unlocked
            .observe(begin_instant.saturating_elapsed().as_secs_f64());
    }

    let mut snap_ctx = SnapContext {
        pb_ctx,
        start_ts: Some(start_ts),
        ..Default::default()
    };
    if need_check_locks_in_replica_read(pb_ctx) {
        snap_ctx.key_ranges = keys
            .into_iter()
            .map(|k| point_key_range(k.clone()))
            .collect();
    }
    Ok(snap_ctx)
}

pub fn need_check_locks_in_replica_read(ctx: &Context) -> bool {
    ctx.get_replica_read() && ctx.get_isolation_level() == IsolationLevel::Si
}

// checks whether the current isolation level needs to check related locks.
pub fn need_check_locks(iso_level: IsolationLevel) -> bool {
    matches!(iso_level, IsolationLevel::Si | IsolationLevel::RcCheckTs)
}

pub fn point_key_range(key: Key) -> KeyRange {
    let mut end_key = key.as_encoded().to_vec();
    end_key.push(0);
    let end_key = Key::from_encoded(end_key);
    let mut key_range = KeyRange::default();
    key_range.set_start_key(key.into_encoded());
    key_range.set_end_key(end_key.into_encoded());
    key_range
}

/// A builder to build a temporary `Storage<E>`.
///
/// Only used for test purpose.
#[must_use]
pub struct TestStorageBuilder<E: Engine, L: LockManager, F: KvFormat> {
    engine: E,
    config: Config,
    pipelined_pessimistic_lock: Arc<AtomicBool>,
    in_memory_pessimistic_lock: Arc<AtomicBool>,
    wake_up_delay_duration_ms: Arc<AtomicU64>,
    lock_mgr: L,
    resource_tag_factory: ResourceTagFactory,
    _phantom: PhantomData<F>,
}

/// TestStorageBuilder for Api V1
/// To be convenience for test cases unrelated to RawKV.
pub type TestStorageBuilderApiV1<E, L> = TestStorageBuilder<E, L, ApiV1>;

impl<F: KvFormat> TestStorageBuilder<RocksEngine, MockLockManager, F> {
    /// Build `Storage<RocksEngine>`.
    pub fn new(lock_mgr: MockLockManager) -> Self {
        let engine = TestEngineBuilder::new()
            .api_version(F::TAG)
            .build()
            .unwrap();

        Self::from_engine_and_lock_mgr(engine, lock_mgr)
    }
}

/// An `Engine` with `TxnExt`. It is used for test purpose.
#[derive(Clone)]
pub struct TxnTestEngine<E: Engine> {
    engine: E,
    txn_ext: Arc<TxnExt>,
}

impl<E: Engine> Engine for TxnTestEngine<E> {
    type Snap = TxnTestSnapshot<E::Snap>;
    type Local = E::Local;

    fn kv_engine(&self) -> Option<Self::Local> {
        self.engine.kv_engine()
    }

    fn modify_on_kv_engine(
        &self,
        region_modifies: HashMap<u64, Vec<Modify>>,
    ) -> tikv_kv::Result<()> {
        self.engine.modify_on_kv_engine(region_modifies)
    }

    type SnapshotRes = impl Future<Output = tikv_kv::Result<Self::Snap>> + Send;
    fn async_snapshot(&mut self, ctx: SnapContext<'_>) -> Self::SnapshotRes {
        let txn_ext = self.txn_ext.clone();
        let f = self.engine.async_snapshot(ctx);
        async move {
            let snapshot = f.await?;
            Ok(TxnTestSnapshot { snapshot, txn_ext })
        }
    }

    type WriteRes = E::WriteRes;
    fn async_write(
        &self,
        ctx: &Context,
        batch: WriteData,
        subscribed: u8,
        on_applied: Option<OnAppliedCb>,
    ) -> Self::WriteRes {
        self.engine.async_write(ctx, batch, subscribed, on_applied)
    }
}

#[derive(Clone)]
pub struct TxnTestSnapshot<S: Snapshot> {
    snapshot: S,
    txn_ext: Arc<TxnExt>,
}

impl<S: Snapshot> Snapshot for TxnTestSnapshot<S> {
    type Iter = S::Iter;
    type Ext<'a>
        = TxnTestSnapshotExt<'a>
    where
        S: 'a;

    fn get(&self, key: &Key) -> tikv_kv::Result<Option<Value>> {
        self.snapshot.get(key)
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> tikv_kv::Result<Option<Value>> {
        self.snapshot.get_cf(cf, key)
    }

    fn get_cf_opt(
        &self,
        opts: engine_traits::ReadOptions,
        cf: CfName,
        key: &Key,
    ) -> tikv_kv::Result<Option<Value>> {
        self.snapshot.get_cf_opt(opts, cf, key)
    }

    fn iter(
        &self,
        cf: CfName,
        iter_opt: engine_traits::IterOptions,
    ) -> tikv_kv::Result<Self::Iter> {
        self.snapshot.iter(cf, iter_opt)
    }

    fn ext(&self) -> Self::Ext<'_> {
        TxnTestSnapshotExt(&self.txn_ext)
    }
}

pub struct TxnTestSnapshotExt<'a>(&'a Arc<TxnExt>);

impl SnapshotExt for TxnTestSnapshotExt<'_> {
    fn get_txn_ext(&self) -> Option<&Arc<TxnExt>> {
        Some(self.0)
    }
}

#[derive(Clone)]
struct DummyReporter;

impl FlowStatsReporter for DummyReporter {
    fn report_read_stats(&self, _read_stats: ReadStats) {}
    fn report_write_stats(&self, _write_stats: WriteStats) {}
}

impl<E: Engine, L: LockManager, F: KvFormat> TestStorageBuilder<E, L, F> {
    pub fn from_engine_and_lock_mgr(engine: E, lock_mgr: L) -> Self {
        let mut config = Config::default();
        config.set_api_version(F::TAG);
        Self {
            engine,
            config,
            pipelined_pessimistic_lock: Arc::new(AtomicBool::new(false)),
            in_memory_pessimistic_lock: Arc::new(AtomicBool::new(false)),
            // Make it very large to avoid tests being affected by the delayed-waking-up behavior.
            wake_up_delay_duration_ms: Arc::new(AtomicU64::new(100000)),
            lock_mgr,
            resource_tag_factory: ResourceTagFactory::new_for_test(),
            _phantom: PhantomData,
        }
    }

    /// Customize the config of the `Storage`.
    ///
    /// By default, `Config::default()` will be used.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    pub fn pipelined_pessimistic_lock(self, enabled: bool) -> Self {
        self.pipelined_pessimistic_lock
            .store(enabled, atomic::Ordering::Relaxed);
        self
    }

    pub fn async_apply_prewrite(mut self, enabled: bool) -> Self {
        self.config.enable_async_apply_prewrite = enabled;
        self
    }

    pub fn in_memory_pessimistic_lock(self, enabled: bool) -> Self {
        self.in_memory_pessimistic_lock
            .store(enabled, atomic::Ordering::Relaxed);
        self
    }

    pub fn wake_up_delay_duration(self, duration_ms: u64) -> Self {
        self.wake_up_delay_duration_ms
            .store(duration_ms, Ordering::Relaxed);
        self
    }

    pub fn set_api_version(mut self, api_version: ApiVersion) -> Self {
        self.config.set_api_version(api_version);
        self
    }

    pub fn set_resource_tag_factory(mut self, resource_tag_factory: ResourceTagFactory) -> Self {
        self.resource_tag_factory = resource_tag_factory;
        self
    }

    /// Build a `Storage<E>`.
    pub fn build(self) -> Result<Storage<E, L, F>> {
        let read_pool = build_read_pool_for_test(
            &crate::config::StorageReadPoolConfig::default_for_test(),
            self.engine.clone(),
        );

        Storage::from_engine(
            self.engine,
            &self.config,
            ReadPool::from(read_pool).handle(),
            self.lock_mgr,
            ConcurrencyManager::new(1.into()),
            DynamicConfigs {
                pipelined_pessimistic_lock: self.pipelined_pessimistic_lock,
                in_memory_pessimistic_lock: self.in_memory_pessimistic_lock,
                wake_up_delay_duration_ms: self.wake_up_delay_duration_ms,
            },
            Arc::new(FlowController::NoLimit),
            DummyReporter,
            self.resource_tag_factory,
            Arc::new(QuotaLimiter::default()),
            latest_feature_gate(),
            None,
        )
    }

    pub fn build_for_txn(self, txn_ext: Arc<TxnExt>) -> Result<Storage<TxnTestEngine<E>, L, F>> {
        let engine = TxnTestEngine {
            engine: self.engine,
            txn_ext,
        };
        let read_pool = build_read_pool_for_test(
            &crate::config::StorageReadPoolConfig::default_for_test(),
            engine.clone(),
        );

        Storage::from_engine(
            engine,
            &self.config,
            ReadPool::from(read_pool).handle(),
            self.lock_mgr,
            ConcurrencyManager::new(1.into()),
            DynamicConfigs {
                pipelined_pessimistic_lock: self.pipelined_pessimistic_lock,
                in_memory_pessimistic_lock: self.in_memory_pessimistic_lock,
                wake_up_delay_duration_ms: self.wake_up_delay_duration_ms,
            },
            Arc::new(FlowController::NoLimit),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            latest_feature_gate(),
            None,
        )
    }
}

pub trait ResponseBatchConsumer<ConsumeResponse: Sized>: Send {
    fn consume(
        &self,
        id: u64,
        res: Result<ConsumeResponse>,
        begin: Instant,
        request_source: String,
    );
}

pub mod test_util {
    use std::{
        fmt::Debug,
        sync::{
            Mutex,
            mpsc::{Sender, channel},
        },
    };

    use futures_executor::block_on;
    use kvproto::kvrpcpb::Op;

    use super::*;
    use crate::storage::{
        lock_manager::WaitTimeout,
        txn::commands,
        types::{PessimisticLockKeyResult, PessimisticLockResults},
    };

    pub fn expect_none(x: Option<Value>) {
        assert_eq!(x, None);
    }

    pub fn expect_value(v: Vec<u8>, x: Option<Value>) {
        assert_eq!(x.unwrap(), v);
    }

    pub fn expect_multi_values(v: Vec<Option<KvPair>>, x: Vec<Result<KvPair>>) {
        let x: Vec<Option<KvPair>> = x.into_iter().map(Result::ok).collect();
        assert_eq!(x, v);
    }

    pub fn expect_error<T, F>(err_matcher: F, x: Result<T>)
    where
        F: FnOnce(Error) + Send + 'static,
    {
        match x {
            Err(e) => err_matcher(e),
            _ => panic!("expect result to be an error"),
        }
    }

    pub fn expect_ok_callback<T: Debug>(done: Sender<i32>, id: i32) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            x.unwrap();
            done.send(id).unwrap();
        })
    }

    pub fn expect_fail_callback<T, F>(done: Sender<i32>, id: i32, err_matcher: F) -> Callback<T>
    where
        F: FnOnce(Error) + Send + 'static,
    {
        Box::new(move |x: Result<T>| {
            expect_error(err_matcher, x);
            done.send(id).unwrap();
        })
    }

    pub fn expect_too_busy_callback<T>(done: Sender<i32>, id: i32) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            expect_error(
                |err| match err {
                    Error(box ErrorInner::SchedTooBusy) => {}
                    e => panic!("unexpected error chain: {:?}, expect too busy", e),
                },
                x,
            );
            done.send(id).unwrap();
        })
    }

    pub fn expect_value_callback<T: PartialEq + Debug + Send + 'static>(
        done: Sender<i32>,
        id: i32,
        value: T,
    ) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            assert_eq!(x.unwrap(), value);
            done.send(id).unwrap();
        })
    }

    pub fn expect_value_with_checker_callback<T: 'static>(
        done: Sender<i32>,
        id: i32,
        check: impl FnOnce(T) + Send + 'static,
    ) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            check(x.unwrap());
            done.send(id).unwrap();
        })
    }

    pub fn expect_pessimistic_lock_res_callback(
        done: Sender<i32>,
        pessimistic_lock_res: PessimisticLockResults,
    ) -> Callback<Result<PessimisticLockResults>> {
        fn key_res_matches_ignoring_error_content(
            lhs: &PessimisticLockKeyResult,
            rhs: &PessimisticLockKeyResult,
        ) -> bool {
            match (lhs, rhs) {
                (PessimisticLockKeyResult::Empty, PessimisticLockKeyResult::Empty) => true,
                (PessimisticLockKeyResult::Value(l), PessimisticLockKeyResult::Value(r)) => l == r,
                (
                    PessimisticLockKeyResult::Existence(l),
                    PessimisticLockKeyResult::Existence(r),
                ) => l == r,
                (
                    PessimisticLockKeyResult::LockedWithConflict {
                        value: value1,
                        conflict_ts: ts1,
                    },
                    PessimisticLockKeyResult::LockedWithConflict {
                        value: value2,
                        conflict_ts: ts2,
                    },
                ) => value1 == value2 && ts1 == ts2,
                (PessimisticLockKeyResult::Waiting, PessimisticLockKeyResult::Waiting) => true,
                (PessimisticLockKeyResult::Failed(_), PessimisticLockKeyResult::Failed(_)) => false,
                _ => false,
            }
        }

        Box::new(move |res: Result<Result<PessimisticLockResults>>| {
            let res = res.unwrap().unwrap();
            assert_eq!(
                res.0.len(),
                pessimistic_lock_res.0.len(),
                "pessimistic lock result length not match, expected: {:?}, got: {:?}",
                pessimistic_lock_res,
                res
            );
            for (expected, got) in pessimistic_lock_res.0.iter().zip(res.0.iter()) {
                assert!(
                    key_res_matches_ignoring_error_content(expected, got),
                    "pessimistic lock result not match, expected: {:?}, got: {:?}",
                    pessimistic_lock_res,
                    res
                );
            }
            done.send(0).unwrap();
        })
    }

    pub fn expect_secondary_locks_status_callback(
        done: Sender<i32>,
        secondary_locks_status: SecondaryLocksStatus,
    ) -> Callback<SecondaryLocksStatus> {
        Box::new(move |res: Result<SecondaryLocksStatus>| {
            assert_eq!(res.unwrap(), secondary_locks_status);
            done.send(0).unwrap();
        })
    }

    type PessimisticLockCommand = TypedCommand<Result<PessimisticLockResults>>;

    impl PessimisticLockCommand {
        pub fn allow_lock_with_conflict(mut self, v: bool) -> Self {
            if let Command::AcquirePessimisticLock(commands::AcquirePessimisticLock {
                allow_lock_with_conflict,
                ..
            }) = &mut self.cmd
            {
                *allow_lock_with_conflict = v;
            } else {
                panic!(
                    "expects AcquirePessimisticLock command, got: {:?}",
                    self.cmd
                );
            }
            self
        }

        pub fn lock_wait_timeout(mut self, timeout: Option<WaitTimeout>) -> Self {
            if let Command::AcquirePessimisticLock(commands::AcquirePessimisticLock {
                wait_timeout,
                ..
            }) = &mut self.cmd
            {
                *wait_timeout = timeout;
            } else {
                panic!(
                    "expects AcquirePessimisticLock command, got: {:?}",
                    self.cmd
                );
            }
            self
        }
    }

    pub fn new_acquire_pessimistic_lock_command(
        keys: Vec<(Key, bool)>,
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        return_values: bool,
        check_existence: bool,
    ) -> PessimisticLockCommand {
        new_acquire_pessimistic_lock_command_with_pk(
            keys,
            None,
            start_ts,
            for_update_ts,
            return_values,
            check_existence,
        )
    }

    pub fn new_acquire_pessimistic_lock_command_with_pk(
        keys: Vec<(Key, bool)>,
        pk: Option<&[u8]>,
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        return_values: bool,
        check_existence: bool,
    ) -> PessimisticLockCommand {
        let primary = pk
            .map(|k| k.to_vec())
            .unwrap_or_else(|| keys[0].0.clone().to_raw().unwrap());
        let for_update_ts: TimeStamp = for_update_ts.into();
        commands::AcquirePessimisticLock::new(
            keys,
            primary,
            start_ts.into(),
            3000,
            false,
            for_update_ts,
            Some(WaitTimeout::Default),
            return_values,
            for_update_ts.next(),
            check_existence,
            false,
            false,
            Context::default(),
        )
    }

    pub fn delete_pessimistic_lock<E: Engine, L: LockManager, F: KvFormat>(
        storage: &Storage<E, L, F>,
        key: Key,
        start_ts: u64,
        for_update_ts: u64,
    ) {
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::PessimisticRollback::new(
                    vec![key],
                    start_ts.into(),
                    for_update_ts.into(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    pub struct GetResult {
        id: u64,
        res: Result<Option<Vec<u8>>>,
    }

    #[derive(Clone)]
    pub struct GetConsumer {
        pub data: Arc<Mutex<Vec<GetResult>>>,
    }

    impl GetConsumer {
        pub fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(vec![])),
            }
        }

        pub fn take_data(self) -> Vec<Result<Option<Vec<u8>>>> {
            let mut data = self.data.lock().unwrap();
            let mut results = std::mem::take(&mut *data);
            results.sort_by_key(|k| k.id);
            results.into_iter().map(|v| v.res).collect()
        }
    }

    impl Default for GetConsumer {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ResponseBatchConsumer<(Option<Vec<u8>>, Statistics)> for GetConsumer {
        fn consume(
            &self,
            id: u64,
            res: Result<(Option<Vec<u8>>, Statistics)>,
            _: Instant,
            _source: String,
        ) {
            self.data.lock().unwrap().push(GetResult {
                id,
                res: res.map(|(v, ..)| v),
            });
        }
    }

    impl ResponseBatchConsumer<Option<Vec<u8>>> for GetConsumer {
        fn consume(&self, id: u64, res: Result<Option<Vec<u8>>>, _: Instant, _source: String) {
            self.data.lock().unwrap().push(GetResult { id, res });
        }
    }

    pub fn latest_feature_gate() -> FeatureGate {
        let feature_gate = FeatureGate::default();
        feature_gate.set_version(env!("CARGO_PKG_VERSION")).unwrap();
        feature_gate
    }

    pub fn must_have_locks<E: Engine, L: LockManager, F: KvFormat>(
        storage: &Storage<E, L, F>,
        ts: u64,
        start_key: &[u8],
        end_key: &[u8],
        expected_locks: &[(
            // key
            &[u8],
            Op,
            // start_ts
            u64,
            // for_update_ts
            u64,
        )],
    ) {
        let locks = block_on(storage.scan_lock(
            Context::default(),
            ts.into(),
            Some(Key::from_raw(start_key)),
            Some(Key::from_raw(end_key)),
            100,
        ))
        .unwrap();
        assert_eq!(
            locks.len(),
            expected_locks.len(),
            "lock count not match, expected: {:?}; got: {:?}",
            expected_locks,
            locks
        );
        for (lock_info, (expected_key, expected_op, expected_start_ts, expected_for_update_ts)) in
            locks.into_iter().zip(expected_locks.iter())
        {
            assert_eq!(lock_info.get_key(), *expected_key);
            assert_eq!(lock_info.get_lock_type(), *expected_op);
            assert_eq!(lock_info.get_lock_version(), *expected_start_ts);
            assert_eq!(lock_info.get_lock_for_update_ts(), *expected_for_update_ts);
        }
    }
}

/// All statistics related to KvGet/KvBatchGet.
#[derive(Debug, Default, Clone)]
pub struct KvGetStatistics {
    pub stats: Statistics,
    pub latency_stats: StageLatencyStats,
}

#[cfg(test)]
mod tests {
    use std::{
        iter::Iterator,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc::{Sender, channel},
        },
        time::Duration,
    };

    use api_version::ApiV2;
    use collections::HashMap;
    use engine_traits::{CF_LOCK, CF_RAFT, CF_WRITE};
    use error_code::ErrorCodeExt;
    use errors::extract_key_error;
    use futures::executor::block_on;
    use kvproto::{
        kvrpcpb::{AssertionLevel, CommandPri, Op, PrewriteRequestPessimisticAction::*},
        metapb::RegionEpoch,
    };
    use parking_lot::Mutex;
    use tikv_util::config::ReadableSize;
    use tracker::INVALID_TRACKER_TOKEN;
    use txn_types::{Mutation, PessimisticLock, WriteType};

    use super::{
        mvcc::tests::{must_unlocked, must_written},
        test_util::*,
        *,
    };
    use crate::{
        config::TitanDbConfig,
        storage::{
            config::BlockCacheConfig,
            kv::{
                Error as KvError, ErrorInner as EngineErrorInner, ExpectedWrite, MockEngineBuilder,
            },
            lock_manager::{
                CancellationCallback, DiagnosticContext, KeyLockWaitInfo, LockDigest,
                LockWaitToken, UpdateWaitForEvent, WaitTimeout,
            },
            mvcc::LockType,
            txn::{
                Error as TxnError, ErrorInner as TxnErrorInner, commands,
                commands::{AcquirePessimisticLock, Prewrite},
                tests::must_rollback,
            },
            types::{PessimisticLockKeyResult, PessimisticLockResults},
        },
    };

    #[test]
    fn test_prewrite_blocks_read() {
        use kvproto::kvrpcpb::ExtraOp;
        let mut storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();

        // We have to do the prewrite manually so that the mem locks don't get released.
        let snapshot = storage.engine.snapshot(Default::default()).unwrap();
        let mutations = vec![Mutation::make_put(Key::from_raw(b"x"), b"z".to_vec())];
        let mut cmd = commands::Prewrite::with_defaults(mutations, vec![1, 2, 3], 10.into());
        if let Command::Prewrite(p) = &mut cmd.cmd {
            p.secondary_keys = Some(vec![]);
        }
        let wr = cmd
            .cmd
            .process_write(
                snapshot,
                commands::WriteContext {
                    lock_mgr: &MockLockManager::new(),
                    concurrency_manager: storage.concurrency_manager.clone(),
                    extra_op: ExtraOp::Noop,
                    statistics: &mut Statistics::default(),
                    async_apply_prewrite: false,
                },
            )
            .unwrap();
        assert_eq!(wr.lock_guards.len(), 1);

        let result = block_on(storage.get(Context::default(), Key::from_raw(b"x"), 100.into()));
        assert!(matches!(
            result,
            Err(Error(box ErrorInner::Txn(txn::Error(box txn::ErrorInner::Mvcc(mvcc::Error(
                box mvcc::ErrorInner::KeyIsLocked { .. },
            ))))))
        ));
    }

    #[test]
    fn test_get_put() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"x"), b"100".to_vec())],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::KeyIsLocked { .. },
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 101.into())),
        );
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"x")],
                    100.into(),
                    101.into(),
                    false,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx, 3),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 101.into()))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_cf_error() {
        // New engine lacks normal column families.
        let engine = TestEngineBuilder::new()
            .cfs([CF_DEFAULT, "foo"])
            .build()
            .unwrap();
        let storage =
            TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
                .build()
                .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"aa".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"bb".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"cc".to_vec()),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_fail_callback(tx, 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::Kv(KvError(box EngineErrorInner::Request(..))),
                    ))))) => {}
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::Kv(KvError(box EngineErrorInner::Request(..))),
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 1.into())),
        );
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::Kv(KvError(box EngineErrorInner::Request(..))),
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"x"),
                None,
                1000,
                0,
                1.into(),
                false,
                false,
            )),
        );
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::Kv(KvError(box EngineErrorInner::Request(..))),
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            block_on(storage.batch_get(
                Context::default(),
                vec![Key::from_raw(b"c"), Key::from_raw(b"d")],
                1.into(),
            )),
        );
        let consumer = GetConsumer::new();
        block_on(storage.batch_get_command(
            vec![create_get_request(b"c", 1), create_get_request(b"d", 1)],
            vec![1, 2],
            vec![INVALID_TRACKER_TOKEN; 2],
            consumer.clone(),
            Instant::now(),
        ))
        .unwrap();
        let data = consumer.take_data();
        for v in data {
            expect_error(
                |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::Kv(KvError(box EngineErrorInner::Request(..))),
                    ))))) => {}
                    e => panic!("unexpected error chain: {:?}", e),
                },
                v,
            );
        }
    }

    #[test]
    fn test_scan() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"aa".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"bb".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"cc".to_vec()),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        // Forward
        expect_multi_values(
            vec![None, None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1000,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward
        expect_multi_values(
            vec![None, None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1000,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with bound
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                Some(Key::from_raw(b"c")),
                1000,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with bound
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                Some(Key::from_raw(b"b")),
                1000,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with limit
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                2,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with limit
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                2,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );

        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![
                        Key::from_raw(b"a"),
                        Key::from_raw(b"b"),
                        Key::from_raw(b"c"),
                    ],
                    1.into(),
                    2.into(),
                    false,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        // Forward
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
                Some((b"c".to_vec(), b"cc".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1000,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
                Some((b"a".to_vec(), b"aa".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1000,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with sample step
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"c".to_vec(), b"cc".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1000,
                2,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with sample step
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"a".to_vec(), b"aa".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1000,
                2,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with sample step and limit
        expect_multi_values(
            vec![Some((b"a".to_vec(), b"aa".to_vec()))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1,
                2,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with sample step and limit
        expect_multi_values(
            vec![Some((b"c".to_vec(), b"cc".to_vec()))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1,
                2,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with bound
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                Some(Key::from_raw(b"c")),
                1000,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with bound
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                Some(Key::from_raw(b"b")),
                1000,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );

        // Forward with limit
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                2,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with limit
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                2,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
    }

    #[test]
    fn test_scan_with_key_only() {
        let db_config = crate::config::DbConfig {
            titan: TitanDbConfig {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let engine = {
            let path = "".to_owned();
            let cfg_rocksdb = db_config;
            let cache = BlockCacheConfig::default().build_shared_cache();
            let cfs_opts = vec![
                (
                    CF_DEFAULT,
                    cfg_rocksdb
                        .defaultcf
                        .build_opt(&cache, None, ApiVersion::V1),
                ),
                (CF_LOCK, cfg_rocksdb.lockcf.build_opt(&cache)),
                (CF_WRITE, cfg_rocksdb.writecf.build_opt(&cache, None)),
                (CF_RAFT, cfg_rocksdb.raftcf.build_opt(&cache)),
            ];
            RocksEngine::new(
                &path,
                None,
                cfs_opts,
                cache.is_some(),
                None, // io_rate_limiter
            )
        }
        .unwrap();
        let storage =
            TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
                .build()
                .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"aa".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"bb".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"cc".to_vec()),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        // Forward
        expect_multi_values(
            vec![None, None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1000,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward
        expect_multi_values(
            vec![None, None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1000,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );
        // Forward with bound
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                Some(Key::from_raw(b"c")),
                1000,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward with bound
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                Some(Key::from_raw(b"b")),
                1000,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );
        // Forward with limit
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                2,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward with limit
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                2,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );

        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![
                        Key::from_raw(b"a"),
                        Key::from_raw(b"b"),
                        Key::from_raw(b"c"),
                    ],
                    1.into(),
                    2.into(),
                    false,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        // Forward
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), vec![])),
                Some((b"b".to_vec(), vec![])),
                Some((b"c".to_vec(), vec![])),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1000,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), vec![])),
                Some((b"b".to_vec(), vec![])),
                Some((b"a".to_vec(), vec![])),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1000,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );
        // Forward with bound
        expect_multi_values(
            vec![Some((b"a".to_vec(), vec![])), Some((b"b".to_vec(), vec![]))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                Some(Key::from_raw(b"c")),
                1000,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward with bound
        expect_multi_values(
            vec![Some((b"c".to_vec(), vec![])), Some((b"b".to_vec(), vec![]))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                Some(Key::from_raw(b"b")),
                1000,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );

        // Forward with limit
        expect_multi_values(
            vec![Some((b"a".to_vec(), vec![])), Some((b"b".to_vec(), vec![]))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                2,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward with limit
        expect_multi_values(
            vec![Some((b"c".to_vec(), vec![])), Some((b"b".to_vec(), vec![]))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                2,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );
    }

    #[test]
    fn test_batch_get() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"aa".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"bb".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"cc".to_vec()),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_multi_values(
            vec![None],
            block_on(storage.batch_get(
                Context::default(),
                vec![Key::from_raw(b"c"), Key::from_raw(b"d")],
                2.into(),
            ))
            .unwrap()
            .0,
        );
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![
                        Key::from_raw(b"a"),
                        Key::from_raw(b"b"),
                        Key::from_raw(b"c"),
                    ],
                    1.into(),
                    2.into(),
                    false,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            block_on(storage.batch_get(
                Context::default(),
                vec![
                    Key::from_raw(b"c"),
                    Key::from_raw(b"x"),
                    Key::from_raw(b"a"),
                    Key::from_raw(b"b"),
                ],
                5.into(),
            ))
            .unwrap()
            .0,
        );
    }

    fn create_get_request(key: &[u8], start_ts: u64) -> GetRequest {
        let mut req = GetRequest::default();
        req.set_key(key.to_owned());
        req.set_version(start_ts);
        req
    }

    #[test]
    fn test_batch_get_command() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"aa".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"bb".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"cc".to_vec()),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        let consumer = GetConsumer::new();
        block_on(storage.batch_get_command(
            vec![create_get_request(b"c", 2), create_get_request(b"d", 2)],
            vec![1, 2],
            vec![INVALID_TRACKER_TOKEN; 2],
            consumer.clone(),
            Instant::now(),
        ))
        .unwrap();
        let mut x = consumer.take_data();
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::KeyIsLocked(..),
                ))))) => {}
                e => panic!("unexpected error chain: {:?}", e),
            },
            x.remove(0),
        );
        assert_eq!(x.remove(0).unwrap(), None);
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![
                        Key::from_raw(b"a"),
                        Key::from_raw(b"b"),
                        Key::from_raw(b"c"),
                    ],
                    1.into(),
                    2.into(),
                    false,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let consumer = GetConsumer::new();
        block_on(storage.batch_get_command(
            vec![
                create_get_request(b"c", 5),
                create_get_request(b"x", 5),
                create_get_request(b"a", 5),
                create_get_request(b"b", 5),
            ],
            vec![1, 2, 3, 4],
            vec![INVALID_TRACKER_TOKEN; 4],
            consumer.clone(),
            Instant::now(),
        ))
        .unwrap();

        let x: Vec<Option<Vec<u8>>> = consumer
            .take_data()
            .into_iter()
            .map(|x| x.unwrap())
            .collect();
        assert_eq!(
            x,
            vec![
                Some(b"cc".to_vec()),
                None,
                Some(b"aa".to_vec()),
                Some(b"bb".to_vec())
            ]
        );
    }

    #[test]
    fn test_txn() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"x"), b"100".to_vec())],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"y"), b"101".to_vec())],
                    b"y".to_vec(),
                    101.into(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"x")],
                    100.into(),
                    110.into(),
                    false,
                    false,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 2, TxnStatus::committed(110.into())),
            )
            .unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"y")],
                    101.into(),
                    111.into(),
                    false,
                    false,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 3, TxnStatus::committed(111.into())),
            )
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 120.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"101".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"y"), 120.into()))
                .unwrap()
                .0,
        );
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"x"), b"105".to_vec())],
                    b"x".to_vec(),
                    105.into(),
                ),
                expect_fail_callback(tx, 6, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::WriteConflict { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_sched_too_busy() {
        let config = Config {
            scheduler_pending_write_threshold: ReadableSize(1),
            ..Default::default()
        };
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .config(config)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        storage
            .sched_txn_command::<()>(
                commands::Pause::new(vec![Key::from_raw(b"x")], 1000, Context::default()),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"y"), b"101".to_vec())],
                    b"y".to_vec(),
                    101.into(),
                ),
                expect_too_busy_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"z"), b"102".to_vec())],
                    b"y".to_vec(),
                    102.into(),
                ),
                expect_ok_callback(tx, 3),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_cleanup() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"x"), b"100".to_vec())],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Cleanup::new(
                    Key::from_raw(b"x"),
                    100.into(),
                    TimeStamp::zero(),
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        assert_eq!(cm.max_ts(), 100.into());
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 105.into()))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_cleanup_check_ttl() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();

        let ts = TimeStamp::compose;
        storage
            .sched_txn_command(
                commands::Prewrite::with_lock_ttl(
                    vec![Mutation::make_put(Key::from_raw(b"x"), b"110".to_vec())],
                    b"x".to_vec(),
                    ts(110, 0),
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Cleanup::new(
                    Key::from_raw(b"x"),
                    ts(110, 0),
                    ts(120, 0),
                    Context::default(),
                ),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::KeyIsLocked(info),
                    ))))) => assert_eq!(info.get_lock_ttl(), 100),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Cleanup::new(
                    Key::from_raw(b"x"),
                    ts(110, 0),
                    ts(220, 0),
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), ts(230, 0)))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_high_priority_get_put() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_none(
            block_on(storage.get(ctx, Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        storage
            .sched_txn_command(
                commands::Prewrite::with_context(
                    vec![Mutation::make_put(Key::from_raw(b"x"), b"100".to_vec())],
                    b"x".to_vec(),
                    100.into(),
                    ctx,
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"x")],
                    100.into(),
                    101.into(),
                    false,
                    false,
                    ctx,
                ),
                expect_ok_callback(tx, 2),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_none(
            block_on(storage.get(ctx, Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(ctx, Key::from_raw(b"x"), 101.into()))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_high_priority_no_block() {
        let config = Config {
            scheduler_worker_pool_size: 1,
            ..Default::default()
        };
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .config(config)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"x"), b"100".to_vec())],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"x")],
                    100.into(),
                    101.into(),
                    false,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Pause::new(vec![Key::from_raw(b"y")], 1000, Context::default()),
                expect_ok_callback(tx, 3),
            )
            .unwrap();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(ctx, Key::from_raw(b"x"), 101.into()))
                .unwrap()
                .0,
        );
        // Command Get with high priority not block by command Pause.
        assert_eq!(rx.recv().unwrap(), 3);
    }

    #[test]
    fn test_delete_range() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        // Write x and y.
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"x"), b"100".to_vec()),
                        Mutation::make_put(Key::from_raw(b"y"), b"100".to_vec()),
                        Mutation::make_put(Key::from_raw(b"z"), b"100".to_vec()),
                    ],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![
                        Key::from_raw(b"x"),
                        Key::from_raw(b"y"),
                        Key::from_raw(b"z"),
                    ],
                    100.into(),
                    101.into(),
                    false,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 101.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"y"), 101.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"z"), 101.into()))
                .unwrap()
                .0,
        );

        // Delete range [x, z)
        storage
            .delete_range(
                Context::default(),
                Key::from_raw(b"x"),
                Key::from_raw(b"z"),
                false,
                expect_ok_callback(tx.clone(), 5),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 101.into()))
                .unwrap()
                .0,
        );
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"y"), 101.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"z"), 101.into()))
                .unwrap()
                .0,
        );

        storage
            .delete_range(
                Context::default(),
                Key::from_raw(b""),
                Key::from_raw(&[255]),
                false,
                expect_ok_callback(tx, 9),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"z"), 101.into()))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_check_key_ranges() {
        fn make_ranges(ranges: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<KeyRange> {
            ranges
                .into_iter()
                .map(|(s, e)| {
                    let mut range = KeyRange::default();
                    range.set_start_key(s);
                    if !e.is_empty() {
                        range.set_end_key(e);
                    }
                    range
                })
                .collect()
        }

        let ranges = make_ranges(vec![
            (b"a".to_vec(), b"a3".to_vec()),
            (b"b".to_vec(), b"b3".to_vec()),
            (b"c".to_vec(), b"c3".to_vec()),
        ]);
        // TODO: refactor to use `Api` parameter.
        assert_eq!(
            <StorageApiV1<RocksEngine, MockLockManager>>::check_key_ranges(&ranges, false,),
            true
        );

        let ranges = make_ranges(vec![
            (b"a".to_vec(), vec![]),
            (b"b".to_vec(), vec![]),
            (b"c".to_vec(), vec![]),
        ]);
        assert_eq!(
            <StorageApiV1<RocksEngine, MockLockManager>>::check_key_ranges(&ranges, false,),
            true
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), b"a".to_vec()),
            (b"b3".to_vec(), b"b".to_vec()),
            (b"c3".to_vec(), b"c".to_vec()),
        ]);
        assert_eq!(
            <StorageApiV1<RocksEngine, MockLockManager>>::check_key_ranges(&ranges, false,),
            false
        );

        // if end_key is omitted, the next start_key is used instead. so, false is
        // returned.
        let ranges = make_ranges(vec![
            (b"c".to_vec(), vec![]),
            (b"b".to_vec(), vec![]),
            (b"a".to_vec(), vec![]),
        ]);
        assert_eq!(
            <StorageApiV1<RocksEngine, MockLockManager>>::check_key_ranges(&ranges, false,),
            false
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), b"a".to_vec()),
            (b"b3".to_vec(), b"b".to_vec()),
            (b"c3".to_vec(), b"c".to_vec()),
        ]);
        assert_eq!(
            <StorageApiV1<RocksEngine, MockLockManager>>::check_key_ranges(&ranges, true,),
            true
        );

        let ranges = make_ranges(vec![
            (b"c3".to_vec(), vec![]),
            (b"b3".to_vec(), vec![]),
            (b"a3".to_vec(), vec![]),
        ]);
        assert_eq!(
            <StorageApiV1<RocksEngine, MockLockManager>>::check_key_ranges(&ranges, true,),
            true
        );

        let ranges = make_ranges(vec![
            (b"a".to_vec(), b"a3".to_vec()),
            (b"b".to_vec(), b"b3".to_vec()),
            (b"c".to_vec(), b"c3".to_vec()),
        ]);
        assert_eq!(
            <StorageApiV1<RocksEngine, MockLockManager>>::check_key_ranges(&ranges, true,),
            false
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), vec![]),
            (b"b3".to_vec(), vec![]),
            (b"c3".to_vec(), vec![]),
        ]);
        assert_eq!(
            <StorageApiV1<RocksEngine, MockLockManager>>::check_key_ranges(&ranges, true,),
            false
        );
    }

    #[test]
    fn test_scan_lock() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"x"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"y"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"z"), b"foo".to_vec()),
                    ],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"foo".to_vec()),
                    ],
                    b"c".to_vec(),
                    101.into(),
                    123,
                    false,
                    3,
                    TimeStamp::default(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    vec![],
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let (lock_a, lock_b, lock_c, lock_x, lock_y, lock_z) = (
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"c".to_vec());
                lock.set_lock_version(101);
                lock.set_key(b"a".to_vec());
                lock.set_lock_ttl(123);
                lock.set_txn_size(3);
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"c".to_vec());
                lock.set_lock_version(101);
                lock.set_key(b"b".to_vec());
                lock.set_lock_ttl(123);
                lock.set_txn_size(3);
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"c".to_vec());
                lock.set_lock_version(101);
                lock.set_key(b"c".to_vec());
                lock.set_lock_ttl(123);
                lock.set_txn_size(3);
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"x".to_vec());
                lock.set_lock_version(100);
                lock.set_key(b"x".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"x".to_vec());
                lock.set_lock_version(100);
                lock.set_key(b"y".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"x".to_vec());
                lock.set_lock_version(100);
                lock.set_key(b"z".to_vec());
                lock
            },
        );

        let cm = storage.concurrency_manager.clone();

        let res =
            block_on(storage.scan_lock(Context::default(), 99.into(), None, None, 10)).unwrap();
        assert_eq!(res, vec![]);
        assert_eq!(cm.max_ts(), 99.into());

        let res =
            block_on(storage.scan_lock(Context::default(), 100.into(), None, None, 10)).unwrap();
        assert_eq!(res, vec![lock_x.clone(), lock_y.clone(), lock_z.clone()]);
        assert_eq!(cm.max_ts(), 100.into());

        let res = block_on(storage.scan_lock(
            Context::default(),
            100.into(),
            Some(Key::from_raw(b"a")),
            None,
            10,
        ))
        .unwrap();
        assert_eq!(res, vec![lock_x.clone(), lock_y.clone(), lock_z.clone()]);

        let res = block_on(storage.scan_lock(
            Context::default(),
            100.into(),
            Some(Key::from_raw(b"y")),
            None,
            10,
        ))
        .unwrap();
        assert_eq!(res, vec![lock_y.clone(), lock_z.clone()]);

        let res =
            block_on(storage.scan_lock(Context::default(), 101.into(), None, None, 10)).unwrap();
        assert_eq!(
            res,
            vec![
                lock_a.clone(),
                lock_b.clone(),
                lock_c.clone(),
                lock_x.clone(),
                lock_y.clone(),
                lock_z.clone(),
            ]
        );
        assert_eq!(cm.max_ts(), 101.into());

        let res =
            block_on(storage.scan_lock(Context::default(), 101.into(), None, None, 4)).unwrap();
        assert_eq!(
            res,
            vec![lock_a, lock_b.clone(), lock_c.clone(), lock_x.clone()]
        );

        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            None,
            4,
        ))
        .unwrap();
        assert_eq!(
            res,
            vec![
                lock_b.clone(),
                lock_c.clone(),
                lock_x.clone(),
                lock_y.clone(),
            ]
        );

        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            None,
            0,
        ))
        .unwrap();
        assert_eq!(
            res,
            vec![
                lock_b.clone(),
                lock_c.clone(),
                lock_x.clone(),
                lock_y.clone(),
                lock_z
            ]
        );

        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"c")),
            0,
        ))
        .unwrap();
        assert_eq!(res, vec![lock_b.clone()]);

        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"z")),
            4,
        ))
        .unwrap();
        assert_eq!(
            res,
            vec![
                lock_b.clone(),
                lock_c.clone(),
                lock_x.clone(),
                lock_y.clone()
            ]
        );

        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"z")),
            3,
        ))
        .unwrap();
        assert_eq!(res, vec![lock_b.clone(), lock_c.clone(), lock_x.clone()]);

        let mem_lock = |k: &[u8], ts: u64, lock_type| {
            let key = Key::from_raw(k);
            let guard = block_on(cm.lock_key(&key));
            guard.with_lock(|lock| {
                *lock = Some(txn_types::Lock::new(
                    lock_type,
                    k.to_vec(),
                    ts.into(),
                    100,
                    None,
                    0.into(),
                    1,
                    20.into(),
                ));
            });
            guard
        };

        let guard = mem_lock(b"z", 80, LockType::Put);
        block_on(storage.scan_lock(Context::default(), 101.into(), None, None, 1)).unwrap_err();

        let guard2 = mem_lock(b"a", 80, LockType::Put);
        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"z")),
            0,
        ))
        .unwrap();
        assert_eq!(
            res,
            vec![
                lock_b.clone(),
                lock_c.clone(),
                lock_x.clone(),
                lock_y.clone()
            ]
        );
        drop(guard);
        drop(guard2);

        // LockType::Lock can't be ignored by scan_lock
        let guard = mem_lock(b"c", 80, LockType::Lock);
        block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"z")),
            1,
        ))
        .unwrap_err();
        drop(guard);

        let guard = mem_lock(b"c", 102, LockType::Put);
        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"z")),
            0,
        ))
        .unwrap();
        assert_eq!(res, vec![lock_b, lock_c, lock_x, lock_y]);
        drop(guard);
    }

    #[test]
    fn test_resolve_lock() {
        test_resolve_lock_impl::<ApiV1>();
        test_resolve_lock_impl::<ApiV2>();
    }

    fn test_resolve_lock_impl<F: KvFormat>() {
        use crate::storage::txn::RESOLVE_LOCK_BATCH_SIZE;

        let storage = TestStorageBuilder::<_, _, F>::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();

        // These locks (transaction ts=99) are not going to be resolved.
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"ta"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"tb"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"tc"), b"foo".to_vec()),
                    ],
                    b"tc".to_vec(),
                    99.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let (lock_a, lock_b, lock_c) = (
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"tc".to_vec());
                lock.set_lock_version(99);
                lock.set_key(b"ta".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"tc".to_vec());
                lock.set_lock_version(99);
                lock.set_key(b"tb".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"tc".to_vec());
                lock.set_lock_version(99);
                lock.set_key(b"tc".to_vec());
                lock
            },
        );

        // We should be able to resolve all locks for transaction ts=100 when there are
        // this many locks.
        let scanned_locks_coll = vec![
            1,
            RESOLVE_LOCK_BATCH_SIZE,
            RESOLVE_LOCK_BATCH_SIZE - 1,
            RESOLVE_LOCK_BATCH_SIZE + 1,
            RESOLVE_LOCK_BATCH_SIZE * 2,
            RESOLVE_LOCK_BATCH_SIZE * 2 - 1,
            RESOLVE_LOCK_BATCH_SIZE * 2 + 1,
        ];

        let is_rollback_coll = vec![
            false, // commit
            true,  // rollback
        ];
        let mut ts = 100.into();

        for scanned_locks in scanned_locks_coll {
            for is_rollback in &is_rollback_coll {
                let mut mutations = vec![];
                for i in 0..scanned_locks {
                    mutations.push(Mutation::make_put(
                        Key::from_raw(format!("tx{:08}", i).as_bytes()),
                        b"foo".to_vec(),
                    ));
                }

                storage
                    .sched_txn_command(
                        commands::Prewrite::with_defaults(mutations, b"tx".to_vec(), ts),
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                let mut txn_status = HashMap::default();
                txn_status.insert(
                    ts,
                    if *is_rollback {
                        TimeStamp::zero() // rollback
                    } else {
                        (ts.into_inner() + 5).into() // commit, commit_ts = start_ts + 5
                    },
                );
                storage
                    .sched_txn_command(
                        commands::ResolveLockReadPhase::new(txn_status, None, Context::default()),
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                // All locks should be resolved except for a, b and c.
                let res =
                    block_on(storage.scan_lock(Context::default(), ts, None, None, 0)).unwrap();
                assert_eq!(res, vec![lock_a.clone(), lock_b.clone(), lock_c.clone()]);

                ts = (ts.into_inner() + 10).into();
            }
        }
    }

    #[test]
    fn test_resolve_lock_lite() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();

        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"foo".to_vec()),
                    ],
                    b"c".to_vec(),
                    99.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Rollback key 'b' and key 'c' and left key 'a' still locked.
        let resolve_keys = vec![Key::from_raw(b"b"), Key::from_raw(b"c")];
        storage
            .sched_txn_command(
                commands::ResolveLockLite::new(
                    99.into(),
                    TimeStamp::zero(),
                    resolve_keys,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Check lock for key 'a'.
        let lock_a = {
            let mut lock = LockInfo::default();
            lock.set_primary_lock(b"c".to_vec());
            lock.set_lock_version(99);
            lock.set_key(b"a".to_vec());
            lock
        };
        let res =
            block_on(storage.scan_lock(Context::default(), 99.into(), None, None, 0)).unwrap();
        assert_eq!(res, vec![lock_a]);

        // Resolve lock for key 'a'.
        storage
            .sched_txn_command(
                commands::ResolveLockLite::new(
                    99.into(),
                    TimeStamp::zero(),
                    vec![Key::from_raw(b"a")],
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"foo".to_vec()),
                    ],
                    b"c".to_vec(),
                    101.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Commit key 'b' and key 'c' and left key 'a' still locked.
        let resolve_keys = vec![Key::from_raw(b"b"), Key::from_raw(b"c")];
        storage
            .sched_txn_command(
                commands::ResolveLockLite::new(
                    101.into(),
                    102.into(),
                    resolve_keys,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Check lock for key 'a'.
        let lock_a = {
            let mut lock = LockInfo::default();
            lock.set_primary_lock(b"c".to_vec());
            lock.set_lock_version(101);
            lock.set_key(b"a".to_vec());
            lock
        };
        let res =
            block_on(storage.scan_lock(Context::default(), 101.into(), None, None, 0)).unwrap();
        assert_eq!(res, vec![lock_a]);
    }

    #[test]
    fn test_txn_heart_beat() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();

        let k = Key::from_raw(b"k");
        let v = b"v".to_vec();

        let uncommitted = TxnStatus::uncommitted;

        // No lock.
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(k.clone(), 10.into(), 100, false, Context::default()),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::TxnNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Prewrite::with_lock_ttl(
                    vec![Mutation::make_put(k.clone(), v.clone())],
                    b"k".to_vec(),
                    10.into(),
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let lock_with_ttl = |ttl| {
            txn_types::Lock::new(
                LockType::Put,
                b"k".to_vec(),
                10.into(),
                ttl,
                Some(v.clone()),
                0.into(),
                0,
                0.into(),
            )
        };

        // `advise_ttl` = 90, which is less than current ttl 100. The lock's ttl will
        // remains 100.
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(k.clone(), 10.into(), 90, false, Context::default()),
                expect_value_callback(tx.clone(), 0, uncommitted(lock_with_ttl(100), false)),
            )
            .unwrap();
        rx.recv().unwrap();

        // `advise_ttl` = 110, which is greater than current ttl. The lock's ttl will be
        // updated to 110.
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(k.clone(), 10.into(), 110, false, Context::default()),
                expect_value_callback(tx.clone(), 0, uncommitted(lock_with_ttl(110), false)),
            )
            .unwrap();
        rx.recv().unwrap();

        // Lock not match. Nothing happens except throwing an error.
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(k, 11.into(), 150, false, Context::default()),
                expect_fail_callback(tx, 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::TxnNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_check_txn_status() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        let (tx, rx) = channel();

        let k = Key::from_raw(b"k");
        let v = b"b".to_vec();

        let ts = TimeStamp::compose;
        use TxnStatus::*;
        let uncommitted = TxnStatus::uncommitted;
        let committed = TxnStatus::committed;

        // No lock and no commit info. Gets an error.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(9, 0),
                    ts(9, 1),
                    ts(9, 1),
                    false,
                    false,
                    false,
                    false,
                    true,
                    Context::default(),
                ),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::TxnNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        assert_eq!(cm.max_ts(), ts(9, 1));

        // No lock and no commit info. If specified rollback_if_not_exist, the key will
        // be rolled back.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(9, 0),
                    ts(9, 1),
                    ts(9, 1),
                    true,
                    false,
                    false,
                    false,
                    true,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, LockNotExist),
            )
            .unwrap();
        rx.recv().unwrap();

        // A rollback will be written, so an later-arriving prewrite will fail.
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(k.clone(), v.clone())],
                    k.as_encoded().to_vec(),
                    ts(9, 0),
                ),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::WriteConflict { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![Mutation::make_put(k.clone(), v.clone())],
                    b"k".to_vec(),
                    ts(10, 0),
                    100,
                    false,
                    3,
                    ts(10, 1),
                    TimeStamp::default(),
                    Some(vec![b"k1".to_vec(), b"k2".to_vec()]),
                    false,
                    AssertionLevel::Off,
                    vec![],
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // If lock exists and not expired, returns the lock's information.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(10, 0),
                    0.into(),
                    0.into(),
                    true,
                    false,
                    false,
                    false,
                    true,
                    Context::default(),
                ),
                expect_value_callback(
                    tx.clone(),
                    0,
                    uncommitted(
                        txn_types::Lock::new(
                            LockType::Put,
                            b"k".to_vec(),
                            ts(10, 0),
                            100,
                            Some(v.clone()),
                            0.into(),
                            3,
                            ts(10, 1),
                        )
                        .use_async_commit(vec![b"k1".to_vec(), b"k2".to_vec()]),
                        false,
                    ),
                ),
            )
            .unwrap();
        rx.recv().unwrap();

        // TODO: Check the lock's min_commit_ts field.

        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![k.clone()],
                    ts(10, 0),
                    ts(20, 0),
                    true,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // If the transaction is committed, returns the commit_ts.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(10, 0),
                    ts(12, 0),
                    ts(15, 0),
                    true,
                    false,
                    false,
                    false,
                    true,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, committed(ts(20, 0))),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Prewrite::with_lock_ttl(
                    vec![Mutation::make_put(k.clone(), v)],
                    k.to_raw().unwrap(),
                    ts(25, 0),
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // If the lock has expired, cleanup it.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(25, 0),
                    ts(126, 0),
                    ts(127, 0),
                    true,
                    false,
                    false,
                    false,
                    true,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, TtlExpire),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![k],
                    ts(25, 0),
                    ts(28, 0),
                    false,
                    false,
                    Context::default(),
                ),
                expect_fail_callback(tx, 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::TxnLockNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_check_secondary_locks() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        let (tx, rx) = channel();

        let k1 = Key::from_raw(b"k1");
        let k2 = Key::from_raw(b"k2");

        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![
                        Mutation::make_lock(k1.clone()),
                        Mutation::make_lock(k2.clone()),
                    ],
                    b"k".to_vec(),
                    10.into(),
                    100,
                    false,
                    2,
                    TimeStamp::zero(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    vec![],
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // All locks exist

        let mut lock1 = LockInfo::default();
        lock1.set_primary_lock(b"k".to_vec());
        lock1.set_lock_version(10);
        lock1.set_key(b"k1".to_vec());
        lock1.set_txn_size(2);
        lock1.set_lock_ttl(100);
        lock1.set_lock_type(Op::Lock);
        let mut lock2 = lock1.clone();
        lock2.set_key(b"k2".to_vec());

        storage
            .sched_txn_command(
                commands::CheckSecondaryLocks::new(
                    vec![k1.clone(), k2.clone()],
                    10.into(),
                    Context::default(),
                ),
                expect_secondary_locks_status_callback(
                    tx.clone(),
                    SecondaryLocksStatus::Locked(vec![lock1, lock2]),
                ),
            )
            .unwrap();
        rx.recv().unwrap();

        // One of the locks are committed

        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![k1.clone()],
                    10.into(),
                    20.into(),
                    false,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::CheckSecondaryLocks::new(vec![k1, k2], 10.into(), Context::default()),
                expect_secondary_locks_status_callback(
                    tx.clone(),
                    SecondaryLocksStatus::Committed(20.into()),
                ),
            )
            .unwrap();
        rx.recv().unwrap();

        assert_eq!(cm.max_ts(), 10.into());

        // Some of the locks do not exist
        let k3 = Key::from_raw(b"k3");
        let k4 = Key::from_raw(b"k4");

        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![Mutation::make_lock(k3.clone())],
                    b"k".to_vec(),
                    30.into(),
                    100,
                    false,
                    2,
                    TimeStamp::zero(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    vec![],
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::CheckSecondaryLocks::new(vec![k3, k4], 10.into(), Context::default()),
                expect_secondary_locks_status_callback(tx, SecondaryLocksStatus::RolledBack),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    fn test_pessimistic_lock_impl(pipelined_pessimistic_lock: bool) {
        let lock_mgr = MockLockManager::new();
        let storage = TestStorageBuilderApiV1::new(lock_mgr.clone())
            .pipelined_pessimistic_lock(pipelined_pessimistic_lock)
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        let (tx, rx) = channel();
        let (key, val) = (Key::from_raw(b"key"), b"val".to_vec());
        let (key2, val2) = (Key::from_raw(b"key2"), b"val2".to_vec());

        let results_values = |res: Vec<Option<Value>>| {
            PessimisticLockResults(
                res.into_iter()
                    .map(|v| PessimisticLockKeyResult::Value(v))
                    .collect::<Vec<_>>(),
            )
        };
        let results_existence = |res: Vec<bool>| {
            PessimisticLockResults(
                res.into_iter()
                    .map(|v| PessimisticLockKeyResult::Existence(v))
                    .collect::<Vec<_>>(),
            )
        };
        let results_empty =
            |len| PessimisticLockResults(vec![PessimisticLockKeyResult::Empty; len]);

        // Key not exist
        for &(return_values, check_existence) in
            &[(false, false), (false, true), (true, false), (true, true)]
        {
            let pessimistic_lock_res = if return_values {
                results_values(vec![None])
            } else if check_existence {
                results_existence(vec![false])
            } else {
                results_empty(1)
            };

            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(key.clone(), false)],
                        10,
                        10,
                        return_values,
                        check_existence,
                    ),
                    expect_pessimistic_lock_res_callback(tx.clone(), pessimistic_lock_res.clone()),
                )
                .unwrap();
            rx.recv().unwrap();

            if return_values || check_existence {
                assert_eq!(cm.max_ts(), 10.into());
            }

            // Duplicated command
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(key.clone(), false)],
                        10,
                        10,
                        return_values,
                        check_existence,
                    ),
                    expect_pessimistic_lock_res_callback(tx.clone(), pessimistic_lock_res.clone()),
                )
                .unwrap();
            rx.recv().unwrap();

            delete_pessimistic_lock(&storage, key.clone(), 10, 10);
        }

        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![(key.clone(), false)],
                    10,
                    10,
                    false,
                    false,
                ),
                expect_pessimistic_lock_res_callback(tx.clone(), results_empty(1)),
            )
            .unwrap();
        rx.recv().unwrap();

        // KeyIsLocked
        for &(return_values, check_existence) in
            &[(false, false), (false, true), (true, false), (true, true)]
        {
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(key.clone(), false)],
                        20,
                        20,
                        return_values,
                        check_existence,
                    ),
                    expect_fail_callback(tx.clone(), 0, |e| match e {
                        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                            mvcc::Error(box mvcc::ErrorInner::KeyIsLocked(_)),
                        )))) => (),
                        e => panic!("unexpected error chain: {:?}", e),
                    }),
                )
                .unwrap();
            // The request enters lock waiting state.
            rx.recv_timeout(Duration::from_millis(100)).unwrap_err();
            lock_mgr.simulate_timeout_all();
            // The lock-waiting request is cancelled.
            rx.recv().unwrap();
        }

        // Always update max_ts when trying to read.
        assert_eq!(cm.max_ts(), 20.into());

        // Put key and key2.
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![
                        (
                            Mutation::make_put(key.clone(), val.clone()),
                            DoPessimisticCheck,
                        ),
                        (
                            Mutation::make_put(key2.clone(), val2.clone()),
                            SkipPessimisticCheck,
                        ),
                    ],
                    key.to_raw().unwrap(),
                    10.into(),
                    3000,
                    10.into(),
                    1,
                    TimeStamp::zero(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![key.clone(), key2.clone()],
                    10.into(),
                    20.into(),
                    false,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // WriteConflict
        for &(return_values, check_existence) in
            &[(false, false), (false, true), (true, false), (true, true)]
        {
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(key.clone(), false)],
                        15,
                        15,
                        return_values,
                        check_existence,
                    ),
                    expect_fail_callback(tx.clone(), 0, |e| match e {
                        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                            mvcc::Error(box mvcc::ErrorInner::WriteConflict { .. }),
                        )))) => (),
                        e => panic!("unexpected error chain: {:?}", e),
                    }),
                )
                .unwrap();
            rx.recv().unwrap();
        }

        assert_eq!(cm.max_ts(), 20.into());

        // Return multiple values
        for &(return_values, check_existence) in
            &[(false, false), (false, true), (true, false), (true, true)]
        {
            let pessimistic_lock_res = if return_values {
                results_values(vec![Some(val.clone()), Some(val2.clone()), None])
            } else if check_existence {
                results_existence(vec![true, true, false])
            } else {
                results_empty(3)
            };
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![
                            (key.clone(), false),
                            (key2.clone(), false),
                            (Key::from_raw(b"key3"), false),
                        ],
                        30,
                        30,
                        return_values,
                        check_existence,
                    ),
                    expect_pessimistic_lock_res_callback(tx.clone(), pessimistic_lock_res),
                )
                .unwrap();
            rx.recv().unwrap();

            if return_values || check_existence {
                assert_eq!(cm.max_ts(), 30.into());
            }

            delete_pessimistic_lock(&storage, key.clone(), 30, 30);
        }
    }

    #[test]
    fn test_pessimistic_lock() {
        test_pessimistic_lock_impl(false);
        test_pessimistic_lock_impl(true);
    }

    fn test_pessimistic_lock_resumable_impl(
        pipelined_pessimistic_lock: bool,
        in_memory_lock: bool,
    ) {
        type Res = PessimisticLockKeyResult;
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .pipelined_pessimistic_lock(pipelined_pessimistic_lock)
            .in_memory_pessimistic_lock(in_memory_lock)
            .build()
            .unwrap();
        let (tx, rx) = channel();

        let results_empty =
            |len| PessimisticLockResults(vec![PessimisticLockKeyResult::Empty; len]);

        for case_num in 0..4 {
            let key = |i| vec![b'k', case_num, i];
            // Put key "k1".
            storage
                .sched_txn_command(
                    commands::Prewrite::new(
                        vec![Mutation::make_put(Key::from_raw(&key(1)), b"v1".to_vec())],
                        key(1),
                        10.into(),
                        3000,
                        false,
                        1,
                        TimeStamp::zero(),
                        TimeStamp::default(),
                        None,
                        false,
                        AssertionLevel::Off,
                        vec![],
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
            storage
                .sched_txn_command(
                    commands::Commit::new(
                        vec![Key::from_raw(&key(1))],
                        10.into(),
                        20.into(),
                        false,
                        false,
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            // Put key "k2".
            storage
                .sched_txn_command(
                    commands::Prewrite::new(
                        vec![Mutation::make_put(Key::from_raw(&key(2)), b"v2".to_vec())],
                        key(2),
                        30.into(),
                        3000,
                        false,
                        1,
                        TimeStamp::zero(),
                        TimeStamp::default(),
                        None,
                        false,
                        AssertionLevel::Off,
                        vec![],
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
            storage
                .sched_txn_command(
                    commands::Commit::new(
                        vec![Key::from_raw(&key(2))],
                        30.into(),
                        40.into(),
                        false,
                        false,
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            // Lock "k3", and we will pessimistic-rollback it.
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(Key::from_raw(&key(3)), false)],
                        20,
                        20,
                        false,
                        false,
                    ),
                    expect_pessimistic_lock_res_callback(tx.clone(), results_empty(1)),
                )
                .unwrap();
            rx.recv().unwrap();

            // Prewrite "k4", and we will commit it
            storage
                .sched_txn_command(
                    commands::Prewrite::new(
                        vec![Mutation::make_put(Key::from_raw(&key(4)), b"v4".to_vec())],
                        key(4),
                        30.into(),
                        3000,
                        false,
                        1,
                        TimeStamp::zero(),
                        TimeStamp::default(),
                        None,
                        false,
                        AssertionLevel::Off,
                        vec![],
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            // Prewrite "k5", and we will roll it back
            storage
                .sched_txn_command(
                    commands::Prewrite::new(
                        vec![Mutation::make_put(Key::from_raw(&key(5)), b"v5".to_vec())],
                        key(5),
                        30.into(),
                        3000,
                        false,
                        1,
                        TimeStamp::zero(),
                        TimeStamp::default(),
                        None,
                        false,
                        AssertionLevel::Off,
                        vec![],
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            // Prewrite "k6", and it won't cause conflict after committing.
            storage
                .sched_txn_command(
                    commands::Prewrite::new(
                        vec![Mutation::make_put(Key::from_raw(&key(6)), b"v6".to_vec())],
                        key(6),
                        10.into(),
                        3000,
                        false,
                        1,
                        TimeStamp::zero(),
                        TimeStamp::default(),
                        None,
                        false,
                        AssertionLevel::Off,
                        vec![],
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
        }

        for &(case_num, return_values, check_existence) in &[
            (0, false, false),
            (1, false, true),
            (2, true, false),
            (3, true, true),
        ] {
            let key = |i| vec![b'k', case_num, i];
            let expected_results = if return_values {
                vec![
                    Res::Value(Some(b"v1".to_vec())),
                    Res::LockedWithConflict {
                        value: Some(b"v2".to_vec()),
                        conflict_ts: 40.into(),
                    },
                    Res::Value(None),
                    Res::LockedWithConflict {
                        value: Some(b"v4".to_vec()),
                        conflict_ts: 40.into(),
                    },
                    Res::LockedWithConflict {
                        value: None,
                        conflict_ts: 30.into(),
                    },
                    Res::Value(Some(b"v6".to_vec())),
                ]
            } else if check_existence {
                vec![
                    Res::Existence(true),
                    Res::LockedWithConflict {
                        value: Some(b"v2".to_vec()),
                        conflict_ts: 40.into(),
                    },
                    Res::Existence(false),
                    Res::LockedWithConflict {
                        value: Some(b"v4".to_vec()),
                        conflict_ts: 40.into(),
                    },
                    Res::LockedWithConflict {
                        value: None,
                        conflict_ts: 30.into(),
                    },
                    Res::Existence(true),
                ]
            } else {
                vec![
                    Res::Empty,
                    Res::LockedWithConflict {
                        value: Some(b"v2".to_vec()),
                        conflict_ts: 40.into(),
                    },
                    Res::Empty,
                    Res::LockedWithConflict {
                        value: Some(b"v4".to_vec()),
                        conflict_ts: 40.into(),
                    },
                    Res::LockedWithConflict {
                        value: None,
                        conflict_ts: 30.into(),
                    },
                    Res::Empty,
                ]
            };

            // k1 & k2
            for (i, k) in &[(0, key(1)), (1, key(2))] {
                let i = *i;
                storage
                    .sched_txn_command(
                        new_acquire_pessimistic_lock_command(
                            vec![(Key::from_raw(k), false)],
                            25,
                            25,
                            return_values,
                            check_existence,
                        )
                        .allow_lock_with_conflict(true),
                        expect_pessimistic_lock_res_callback(
                            tx.clone(),
                            PessimisticLockResults(vec![expected_results[i].clone()]),
                        ),
                    )
                    .unwrap();
                rx.recv().unwrap();
            }

            // k3
            // Report KeyIsLocked if no wait
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(Key::from_raw(&key(3)), false)],
                        25,
                        25,
                        return_values,
                        check_existence,
                    )
                    .allow_lock_with_conflict(true)
                    .lock_wait_timeout(None),
                    expect_value_with_checker_callback(
                        tx.clone(),
                        0,
                        |res: Result<PessimisticLockResults>| {
                            let e = res.unwrap().0[0].unwrap_err();
                            match e.inner() {
                                ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                                    mvcc::Error(box mvcc::ErrorInner::KeyIsLocked(..)),
                                ))) => (),
                                e => panic!("unexpected error chain: {:?}", e),
                            }
                        },
                    ),
                )
                .unwrap();
            rx.recv().unwrap();

            // Lock wait
            let (tx1, rx1) = channel();
            // k3
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(Key::from_raw(&key(3)), false)],
                        25,
                        25,
                        return_values,
                        check_existence,
                    )
                    .allow_lock_with_conflict(true)
                    .lock_wait_timeout(Some(WaitTimeout::Default)),
                    expect_pessimistic_lock_res_callback(
                        tx1.clone(),
                        PessimisticLockResults(vec![expected_results[2].clone()]),
                    ),
                )
                .unwrap();
            rx1.recv_timeout(Duration::from_millis(100)).unwrap_err();

            delete_pessimistic_lock(&storage, Key::from_raw(&key(3)), 20, 20);
            rx1.recv().unwrap();

            // k4
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(Key::from_raw(&key(4)), false)],
                        25,
                        25,
                        return_values,
                        check_existence,
                    )
                    .allow_lock_with_conflict(true)
                    .lock_wait_timeout(Some(WaitTimeout::Default)),
                    expect_pessimistic_lock_res_callback(
                        tx1.clone(),
                        PessimisticLockResults(vec![expected_results[3].clone()]),
                    ),
                )
                .unwrap();
            rx1.recv_timeout(Duration::from_millis(100)).unwrap_err();
            storage
                .sched_txn_command(
                    commands::Commit::new(
                        vec![Key::from_raw(&key(4))],
                        30.into(),
                        40.into(),
                        false,
                        false,
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
            rx1.recv().unwrap();

            // k5
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(Key::from_raw(&key(5)), false)],
                        25,
                        25,
                        return_values,
                        check_existence,
                    )
                    .allow_lock_with_conflict(true)
                    .lock_wait_timeout(Some(WaitTimeout::Default)),
                    expect_pessimistic_lock_res_callback(
                        tx1.clone(),
                        PessimisticLockResults(vec![expected_results[4].clone()]),
                    ),
                )
                .unwrap();
            rx1.recv_timeout(Duration::from_millis(100)).unwrap_err();
            storage
                .sched_txn_command(
                    commands::Rollback::new(
                        vec![Key::from_raw(&key(5))],
                        30.into(),
                        false,
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
            rx1.recv().unwrap();

            // k6
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(Key::from_raw(&key(6)), false)],
                        25,
                        25,
                        return_values,
                        check_existence,
                    )
                    .allow_lock_with_conflict(true)
                    .lock_wait_timeout(Some(WaitTimeout::Default)),
                    expect_pessimistic_lock_res_callback(
                        tx1.clone(),
                        PessimisticLockResults(vec![expected_results[5].clone()]),
                    ),
                )
                .unwrap();
            rx1.recv_timeout(Duration::from_millis(100)).unwrap_err();
            storage
                .sched_txn_command(
                    commands::Commit::new(
                        vec![Key::from_raw(&key(6))],
                        10.into(),
                        20.into(),
                        false,
                        false,
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
            rx1.recv().unwrap();

            must_have_locks(
                &storage,
                50,
                &key(0),
                &key(10),
                &[
                    (&key(1), Op::PessimisticLock, 25, 25),
                    (&key(2), Op::PessimisticLock, 25, 40),
                    (&key(3), Op::PessimisticLock, 25, 25),
                    (&key(4), Op::PessimisticLock, 25, 40),
                    (&key(5), Op::PessimisticLock, 25, 30),
                    (&key(6), Op::PessimisticLock, 25, 25),
                ],
            );

            // Test idempotency
            for i in 0..6usize {
                storage
                    .sched_txn_command(
                        new_acquire_pessimistic_lock_command(
                            vec![(Key::from_raw(&key(i as u8 + 1)), false)],
                            25,
                            25,
                            return_values,
                            check_existence,
                        )
                        .allow_lock_with_conflict(true)
                        .lock_wait_timeout(Some(WaitTimeout::Default)),
                        expect_pessimistic_lock_res_callback(
                            tx1.clone(),
                            PessimisticLockResults(vec![expected_results[i].clone()]),
                        ),
                    )
                    .unwrap();
                rx1.recv().unwrap();
            }
        }

        // Check the channel is clear to avoid misusing in the above test code.
        tx.send(100).unwrap();
        assert_eq!(rx.recv().unwrap(), 100);

        // Test request queueing.
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![(Key::from_raw(b"k21"), false)],
                    10,
                    10,
                    false,
                    false,
                )
                .allow_lock_with_conflict(true)
                .lock_wait_timeout(Some(WaitTimeout::Default)),
                expect_pessimistic_lock_res_callback(tx, results_empty(1)),
            )
            .unwrap();
        rx.recv().unwrap();

        let channels: Vec<_> = (0..4).map(|_| channel()).collect();
        let start_ts = &[20, 50, 30, 40];
        for i in 0..4 {
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(Key::from_raw(b"k21"), false)],
                        start_ts[i],
                        start_ts[i],
                        false,
                        false,
                    )
                    .allow_lock_with_conflict(true)
                    .lock_wait_timeout(Some(WaitTimeout::Default)),
                    expect_pessimistic_lock_res_callback(channels[i].0.clone(), results_empty(1)),
                )
                .unwrap();
            channels[i]
                .1
                .recv_timeout(Duration::from_millis(100))
                .unwrap_err();
        }

        delete_pessimistic_lock(&storage, Key::from_raw(b"k21"), 10, 10);
        channels[0].1.recv().unwrap();
        channels[2]
            .1
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        delete_pessimistic_lock(&storage, Key::from_raw(b"k21"), 20, 20);
        channels[2].1.recv().unwrap();
        channels[3]
            .1
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        delete_pessimistic_lock(&storage, Key::from_raw(b"k21"), 30, 30);
        channels[3].1.recv().unwrap();
        channels[1]
            .1
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        delete_pessimistic_lock(&storage, Key::from_raw(b"k21"), 40, 40);
        channels[1].1.recv().unwrap();
    }

    #[test]
    fn test_pessimistic_lock_resumable() {
        for &pipelined_pessimistic_lock in &[false, true] {
            for &in_memory_lock in &[false, true] {
                test_pessimistic_lock_resumable_impl(pipelined_pessimistic_lock, in_memory_lock);
            }
        }
    }

    #[allow(clippy::large_enum_variant)]
    pub enum Msg {
        WaitFor {
            token: LockWaitToken,
            region_id: u64,
            region_epoch: RegionEpoch,
            term: u64,
            start_ts: TimeStamp,
            wait_info: KeyLockWaitInfo,
            is_first_lock: bool,
            timeout: Option<WaitTimeout>,
            cancel_callback: CancellationCallback,
            diag_ctx: DiagnosticContext,
        },
        RemoveLockWait {
            token: LockWaitToken,
        },
    }

    // `ProxyLockMgr` sends all msgs it received to `Sender`.
    // It's used to check whether we send right messages to lock manager.
    #[derive(Clone)]
    pub struct ProxyLockMgr {
        tx: Arc<Mutex<Sender<Msg>>>,
        has_waiter: Arc<AtomicBool>,
    }

    impl ProxyLockMgr {
        pub fn new(tx: Sender<Msg>) -> Self {
            Self {
                tx: Arc::new(Mutex::new(tx)),
                has_waiter: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    impl LockManager for ProxyLockMgr {
        fn allocate_token(&self) -> LockWaitToken {
            LockWaitToken(Some(1))
        }

        fn wait_for(
            &self,
            token: LockWaitToken,
            region_id: u64,
            region_epoch: RegionEpoch,
            term: u64,
            start_ts: TimeStamp,
            wait_info: KeyLockWaitInfo,
            is_first_lock: bool,
            timeout: Option<WaitTimeout>,
            cancel_callback: CancellationCallback,
            diag_ctx: DiagnosticContext,
        ) {
            self.tx
                .lock()
                .send(Msg::WaitFor {
                    token,
                    region_id,
                    region_epoch,
                    term,
                    start_ts,
                    wait_info,
                    is_first_lock,
                    timeout,
                    cancel_callback,
                    diag_ctx,
                })
                .unwrap();
        }

        fn update_wait_for(&self, _updated_items: Vec<UpdateWaitForEvent>) {}

        fn remove_lock_wait(&self, token: LockWaitToken) {
            self.tx.lock().send(Msg::RemoveLockWait { token }).unwrap();
        }

        fn has_waiter(&self) -> bool {
            self.has_waiter.load(Ordering::Relaxed)
        }

        fn dump_wait_for_entries(&self, _cb: waiter_manager::Callback) {
            unimplemented!()
        }
    }

    // Test whether `Storage` sends right wait-for-lock msgs to `LockManager`.
    #[test]
    fn validate_wait_for_lock_msg() {
        let (msg_tx, msg_rx) = channel();
        let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(
            TestEngineBuilder::new().build().unwrap(),
            ProxyLockMgr::new(msg_tx),
        )
        .build()
        .unwrap();

        let (k, v) = (b"k".to_vec(), b"v".to_vec());
        let (tx, rx) = channel();
        // Write lock-k.
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(&k), v)],
                    k.clone(),
                    10.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        // No wait for msg
        assert!(msg_rx.try_recv().is_err());

        // Meet lock-k.
        storage
            .sched_txn_command(
                commands::AcquirePessimisticLock::new(
                    vec![(Key::from_raw(b"foo"), false), (Key::from_raw(&k), false)],
                    k.clone(),
                    20.into(),
                    3000,
                    true,
                    20.into(),
                    Some(WaitTimeout::Millis(100)),
                    false,
                    21.into(),
                    false,
                    false,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        // The transaction should be waiting for lock released so cb won't be called.
        rx.recv_timeout(Duration::from_millis(500)).unwrap_err();

        let msg = msg_rx.try_recv().unwrap();
        // Check msg validation.
        match msg {
            Msg::WaitFor {
                token,
                region_id,
                region_epoch,
                term,
                start_ts,
                wait_info,
                is_first_lock,
                timeout,
                cancel_callback,
                diag_ctx,
            } => {
                let _ = (
                    token,
                    region_id,
                    region_epoch,
                    term,
                    cancel_callback,
                    diag_ctx,
                );
                assert_eq!(start_ts, TimeStamp::new(20));
                assert_eq!(
                    wait_info.lock_digest,
                    LockDigest {
                        ts: 10.into(),
                        hash: Key::from_raw(&k).gen_hash(),
                    }
                );
                assert_eq!(is_first_lock, true);
                assert_eq!(timeout, Some(WaitTimeout::Millis(100)));
            }

            Msg::RemoveLockWait { token } => {
                let _ = token;
                panic!("unexpected msg");
            }
        }
    }

    // Test whether `Storage` correctly wakes up lock-waiting requests
    #[test]
    fn test_wake_up() {
        struct BlockedLockRequestHandle {
            remaining: usize,
            rx: std::sync::mpsc::Receiver<i32>,
        }

        impl BlockedLockRequestHandle {
            fn assert_blocked(&mut self) {
                while self.remaining > 0 {
                    match self.rx.recv_timeout(Duration::from_millis(50)) {
                        Ok(_) => self.remaining -= 1,
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return,
                        Err(e) => panic!("unexpected error: {:?}", e),
                    }
                }
                panic!("pessimistic lock requests expected to be blocked finished unexpectedly")
            }

            fn assert_woken_up(mut self) {
                while self.remaining > 0 {
                    match self.rx.recv_timeout(Duration::from_millis(200)) {
                        Ok(_) => self.remaining -= 1,
                        Err(e) => panic!("unexpected error: {:?}", e),
                    }
                }
            }
        }

        let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(
            TestEngineBuilder::new().build().unwrap(),
            MockLockManager::new(),
        )
        .build()
        .unwrap();

        let lock_blocked = |keys: &[Key],
                            lock_ts: u64,
                            expected_conflicting_start_ts: u64,
                            expected_conflicting_commit_ts: u64| {
            let (tx, rx) = channel();
            for k in keys {
                storage
                    .sched_txn_command(
                        commands::AcquirePessimisticLock::new(
                            vec![(k.clone(), false)],
                            k.to_raw().unwrap(),
                            lock_ts.into(),
                            3000,
                            false,
                            lock_ts.into(),
                            Some(WaitTimeout::Millis(5000)),
                            false,
                            (lock_ts + 1).into(),
                            false,
                            false,
                            false,
                            Context::default(),
                        ),
                        expect_fail_callback(tx.clone(), 6, move |e| match e {
                            Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                                mvcc::Error(box mvcc::ErrorInner::WriteConflict {
                                    conflict_start_ts,
                                    conflict_commit_ts,
                                    ..
                                }),
                            )))) => {
                                assert_eq!(conflict_start_ts, expected_conflicting_start_ts.into());
                                assert_eq!(
                                    conflict_commit_ts,
                                    expected_conflicting_commit_ts.into()
                                );
                            }
                            e => panic!("unexpected error chain: {:?}", e),
                        }),
                    )
                    .unwrap();
            }
            let mut h = BlockedLockRequestHandle {
                remaining: keys.len(),
                rx,
            };
            h.assert_blocked();
            h
        };

        let (tx, rx) = channel();
        let prewrite_locks = |keys: &[Key], ts: TimeStamp| {
            storage
                .sched_txn_command(
                    commands::Prewrite::with_defaults(
                        keys.iter()
                            .map(|k| Mutation::make_put(k.clone(), b"v".to_vec()))
                            .collect(),
                        keys[0].to_raw().unwrap(),
                        ts,
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
        };
        let acquire_pessimistic_locks = |keys: &[Key], ts: TimeStamp| {
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        keys.iter().map(|k| (k.clone(), false)).collect(),
                        ts,
                        ts,
                        false,
                        false,
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
        };

        let keys = vec![
            Key::from_raw(b"a"),
            Key::from_raw(b"b"),
            Key::from_raw(b"c"),
        ];

        // Commit
        prewrite_locks(&keys, 10.into());
        let h = lock_blocked(&keys, 15, 10, 20);
        storage
            .sched_txn_command(
                commands::Commit::new(
                    keys.clone(),
                    10.into(),
                    20.into(),
                    false,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        h.assert_woken_up();

        // Cleanup
        for pessimistic in &[false, true] {
            let mut ts = TimeStamp::new(30);
            if *pessimistic {
                ts.incr();
                acquire_pessimistic_locks(&keys[..1], ts);
            } else {
                prewrite_locks(&keys[..1], ts);
            }
            let h = lock_blocked(&keys[..1], 35, ts.into_inner(), 0);
            storage
                .sched_txn_command(
                    commands::Cleanup::new(
                        keys[0].clone(),
                        ts,
                        TimeStamp::max(),
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            h.assert_woken_up();
        }

        // Rollback
        for pessimistic in &[false, true] {
            let mut ts = TimeStamp::new(40);
            if *pessimistic {
                ts.incr();
                acquire_pessimistic_locks(&keys, ts);
            } else {
                prewrite_locks(&keys, ts);
            }
            let h = lock_blocked(&keys, 45, ts.into_inner(), 0);
            storage
                .sched_txn_command(
                    commands::Rollback::new(keys.clone(), ts, false, Context::default()),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            h.assert_woken_up();
        }

        // PessimisticRollback
        acquire_pessimistic_locks(&keys, 50.into());
        let h = lock_blocked(&keys, 55, 50, 0);
        storage
            .sched_txn_command(
                commands::PessimisticRollback::new(
                    keys.clone(),
                    50.into(),
                    50.into(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        h.assert_woken_up();

        // ResolveLockLite
        for commit in &[false, true] {
            let mut start_ts = TimeStamp::new(60);
            let commit_ts = if *commit {
                start_ts.incr();
                start_ts.next()
            } else {
                TimeStamp::zero()
            };
            prewrite_locks(&keys, start_ts);
            let h = lock_blocked(&keys, 65, start_ts.into_inner(), commit_ts.into_inner());
            storage
                .sched_txn_command(
                    commands::ResolveLockLite::new(
                        start_ts,
                        commit_ts,
                        keys.clone(),
                        false,
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            h.assert_woken_up();
        }

        // ResolveLock
        let mut txn_status = HashMap::default();
        acquire_pessimistic_locks(&keys, 70.into());
        // Rollback start_ts=70
        txn_status.insert(TimeStamp::new(70), TimeStamp::zero());
        let committed_keys = vec![
            Key::from_raw(b"d"),
            Key::from_raw(b"e"),
            Key::from_raw(b"f"),
        ];
        prewrite_locks(&committed_keys, 75.into());
        txn_status.insert(TimeStamp::new(75), TimeStamp::new(76));
        let h_rolled_back = lock_blocked(&keys, 76, 70, 0);
        let h_committed = lock_blocked(&committed_keys, 76, 75, 76);
        storage
            .sched_txn_command(
                commands::ResolveLockReadPhase::new(txn_status, None, Context::default()),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        h_rolled_back.assert_woken_up();
        h_committed.assert_woken_up();

        // CheckTxnStatus
        let key = Key::from_raw(b"k");
        let start_ts = TimeStamp::compose(100, 0);
        storage
            .sched_txn_command(
                commands::Prewrite::with_lock_ttl(
                    vec![Mutation::make_put(key.clone(), b"v".to_vec())],
                    key.to_raw().unwrap(),
                    start_ts,
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let mut h = lock_blocked(&[key.clone()], 105, start_ts.into_inner(), 0);

        // Not expire
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    key.clone(),
                    start_ts,
                    TimeStamp::compose(110, 0),
                    TimeStamp::compose(150, 0),
                    false,
                    false,
                    false,
                    false,
                    true,
                    Context::default(),
                ),
                expect_value_callback(
                    tx.clone(),
                    0,
                    TxnStatus::uncommitted(
                        txn_types::Lock::new(
                            LockType::Put,
                            b"k".to_vec(),
                            start_ts,
                            100,
                            Some(b"v".to_vec()),
                            0.into(),
                            0,
                            0.into(),
                        ),
                        false,
                    ),
                ),
            )
            .unwrap();
        rx.recv().unwrap();
        // Not woken up
        h.assert_blocked();

        // Expired
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    key,
                    start_ts,
                    TimeStamp::compose(110, 0),
                    TimeStamp::compose(201, 0),
                    false,
                    false,
                    false,
                    false,
                    true,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, TxnStatus::TtlExpire),
            )
            .unwrap();
        rx.recv().unwrap();
        h.assert_woken_up();
    }

    #[test]
    fn test_check_memory_locks() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let cm = storage.get_concurrency_manager();
        let key = Key::from_raw(b"key");
        let guard = block_on(cm.lock_key(&key));
        guard.with_lock(|lock| {
            *lock = Some(txn_types::Lock::new(
                LockType::Put,
                b"key".to_vec(),
                10.into(),
                100,
                Some(vec![]),
                0.into(),
                1,
                20.into(),
            ));
        });

        let mut ctx = Context::default();
        ctx.set_isolation_level(IsolationLevel::Si);

        // Test get
        let key_error = extract_key_error(
            &block_on(storage.get(ctx.clone(), Key::from_raw(b"key"), 100.into())).unwrap_err(),
        );
        assert_eq!(key_error.get_locked().get_key(), b"key");
        // Ignore memory locks in resolved or committed locks.
        ctx.set_resolved_locks(vec![10]);
        block_on(storage.get(ctx.clone(), Key::from_raw(b"key"), 100.into())).unwrap();
        ctx.take_resolved_locks();

        // Test batch_get
        let batch_get = |ctx| {
            block_on(storage.batch_get(
                ctx,
                vec![Key::from_raw(b"a"), Key::from_raw(b"key")],
                100.into(),
            ))
        };
        let key_error = extract_key_error(&batch_get(ctx.clone()).unwrap_err());
        assert_eq!(key_error.get_locked().get_key(), b"key");
        // Ignore memory locks in resolved locks.
        ctx.set_resolved_locks(vec![10]);
        batch_get(ctx.clone()).unwrap();
        ctx.take_resolved_locks();

        // Test scan
        let scan = |ctx, start_key, end_key, reverse| {
            block_on(storage.scan(ctx, start_key, end_key, 10, 0, 100.into(), false, reverse))
        };
        let key_error =
            extract_key_error(&scan(ctx.clone(), Key::from_raw(b"a"), None, false).unwrap_err());
        assert_eq!(key_error.get_locked().get_key(), b"key");
        ctx.set_resolved_locks(vec![10]);
        scan(ctx.clone(), Key::from_raw(b"a"), None, false).unwrap();
        ctx.take_resolved_locks();
        let key_error =
            extract_key_error(&scan(ctx.clone(), Key::from_raw(b"\xff"), None, true).unwrap_err());
        assert_eq!(key_error.get_locked().get_key(), b"key");
        ctx.set_resolved_locks(vec![10]);
        scan(ctx.clone(), Key::from_raw(b"\xff"), None, false).unwrap();
        ctx.take_resolved_locks();
        // Ignore memory locks in resolved or committed locks.

        // Test batch_get_command
        let mut req1 = GetRequest::default();
        req1.set_context(ctx.clone());
        req1.set_key(b"a".to_vec());
        req1.set_version(50);
        let mut req2 = GetRequest::default();
        req2.set_context(ctx);
        req2.set_key(b"key".to_vec());
        req2.set_version(100);
        let batch_get_command = |req2| {
            let consumer = GetConsumer::new();
            block_on(storage.batch_get_command(
                vec![req1.clone(), req2],
                vec![1, 2],
                vec![INVALID_TRACKER_TOKEN; 2],
                consumer.clone(),
                Instant::now(),
            ))
            .unwrap();
            consumer.take_data()
        };
        let res = batch_get_command(req2.clone());
        res[0].as_ref().unwrap();
        let key_error = extract_key_error(res[1].as_ref().unwrap_err());
        assert_eq!(key_error.get_locked().get_key(), b"key");
        // Ignore memory locks in resolved or committed locks.
        req2.mut_context().set_resolved_locks(vec![10]);
        let res = batch_get_command(req2.clone());
        res[0].as_ref().unwrap();
        res[1].as_ref().unwrap();
        req2.mut_context().take_resolved_locks();
    }

    #[test]
    fn test_read_access_locks() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();

        let (k1, v1) = (b"k1".to_vec(), b"v1".to_vec());
        let (k2, v2) = (b"k2".to_vec(), b"v2".to_vec());
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(&k1), v1.clone()),
                        Mutation::make_put(Key::from_raw(&k2), v2.clone()),
                    ],
                    k1.clone(),
                    100.into(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let mut ctx = Context::default();
        ctx.set_isolation_level(IsolationLevel::Si);
        ctx.set_committed_locks(vec![100]);
        // get
        assert_eq!(
            block_on(storage.get(ctx.clone(), Key::from_raw(&k1), 110.into()))
                .unwrap()
                .0,
            Some(v1.clone())
        );
        // batch get
        let res = block_on(storage.batch_get(
            ctx.clone(),
            vec![Key::from_raw(&k1), Key::from_raw(&k2)],
            110.into(),
        ))
        .unwrap()
        .0;
        if res[0].as_ref().unwrap().0 == k1 {
            assert_eq!(&res[0].as_ref().unwrap().1, &v1);
            assert_eq!(&res[1].as_ref().unwrap().1, &v2);
        } else {
            assert_eq!(&res[0].as_ref().unwrap().1, &v2);
            assert_eq!(&res[1].as_ref().unwrap().1, &v1);
        }
        // batch get commands
        let mut req = GetRequest::default();
        req.set_context(ctx.clone());
        req.set_key(k1.clone());
        req.set_version(110);
        let consumer = GetConsumer::new();
        block_on(storage.batch_get_command(
            vec![req],
            vec![1],
            vec![INVALID_TRACKER_TOKEN],
            consumer.clone(),
            Instant::now(),
        ))
        .unwrap();
        let res = consumer.take_data();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].as_ref().unwrap(), &Some(v1.clone()));
        // scan
        for desc in &[false, true] {
            let mut values = vec![
                Some((k1.clone(), v1.clone())),
                Some((k2.clone(), v2.clone())),
            ];
            let mut key = Key::from_raw(b"\x00");
            if *desc {
                key = Key::from_raw(b"\xff");
                values.reverse();
            }
            expect_multi_values(
                values,
                block_on(storage.scan(ctx.clone(), key, None, 1000, 0, 110.into(), false, *desc))
                    .unwrap(),
            );
        }
    }

    #[test]
    fn test_async_commit_prewrite() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        cm.update_max_ts(10.into());

        // Optimistic prewrite
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"v".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"v".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"v".to_vec()),
                    ],
                    b"c".to_vec(),
                    100.into(),
                    1000,
                    false,
                    3,
                    TimeStamp::default(),
                    TimeStamp::default(),
                    Some(vec![b"a".to_vec(), b"b".to_vec()]),
                    false,
                    AssertionLevel::Off,
                    vec![],
                    Context::default(),
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        let res = rx.recv().unwrap().unwrap();
        assert_eq!(res.min_commit_ts, 101.into());

        // Pessimistic prewrite
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![(Key::from_raw(b"d"), false), (Key::from_raw(b"e"), false)],
                    200,
                    300,
                    false,
                    false,
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        cm.update_max_ts(1000.into());

        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![
                        (
                            Mutation::make_put(Key::from_raw(b"d"), b"v".to_vec()),
                            DoPessimisticCheck,
                        ),
                        (
                            Mutation::make_put(Key::from_raw(b"e"), b"v".to_vec()),
                            DoPessimisticCheck,
                        ),
                    ],
                    b"d".to_vec(),
                    200.into(),
                    1000,
                    400.into(),
                    2,
                    401.into(),
                    TimeStamp::default(),
                    Some(vec![b"e".to_vec()]),
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        let res = rx.recv().unwrap().unwrap();
        assert_eq!(res.min_commit_ts, 1001.into());
    }

    // This is one of the series of tests to test overlapped timestamps.
    // Overlapped ts means there is a rollback record and a commit record with the
    // same ts. In this test we check that if rollback happens before commit, then
    // they should not have overlapped ts, which is an expected property.
    #[test]
    fn test_overlapped_ts_rollback_before_prewrite() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(
            engine.clone(),
            MockLockManager::new(),
        )
        .build()
        .unwrap();

        let (k1, v1) = (b"key1", b"v1");
        let (k2, v2) = (b"key2", b"v2");
        let key1 = Key::from_raw(k1);
        let key2 = Key::from_raw(k2);
        let value1 = v1.to_vec();
        let value2 = v2.to_vec();

        let (tx, rx) = channel();

        // T1 acquires lock on k1, start_ts = 1, for_update_ts = 3
        storage
            .sched_txn_command(
                commands::AcquirePessimisticLock::new(
                    vec![(key1.clone(), false)],
                    k1.to_vec(),
                    1.into(),
                    0,
                    true,
                    3.into(),
                    None,
                    false,
                    0.into(),
                    false,
                    false,
                    false,
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // T2 acquires lock on k2, start_ts = 10, for_update_ts = 15
        storage
            .sched_txn_command(
                commands::AcquirePessimisticLock::new(
                    vec![(key2.clone(), false)],
                    k2.to_vec(),
                    10.into(),
                    0,
                    true,
                    15.into(),
                    None,
                    false,
                    0.into(),
                    false,
                    false,
                    false,
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // T2 pessimistically prewrites, start_ts = 10, lock ttl = 0
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![(
                        Mutation::make_put(key2.clone(), value2.clone()),
                        DoPessimisticCheck,
                    )],
                    k2.to_vec(),
                    10.into(),
                    0,
                    15.into(),
                    1,
                    0.into(),
                    100.into(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // T3 checks T2, which rolls back key2 and pushes max_ts to 10
        // use a large timestamp to make the lock expire so key2 will be rolled back.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    key2.clone(),
                    10.into(),
                    ((1 << 18) + 8).into(),
                    ((1 << 18) + 8).into(),
                    true,
                    false,
                    false,
                    false,
                    true,
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        must_unlocked(&mut engine, k2);
        must_written(&mut engine, k2, 10, 10, WriteType::Rollback);

        // T1 prewrites, start_ts = 1, for_update_ts = 3
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![
                        (Mutation::make_put(key1.clone(), value1), DoPessimisticCheck),
                        (
                            Mutation::make_put(key2.clone(), value2),
                            SkipPessimisticCheck,
                        ),
                    ],
                    k1.to_vec(),
                    1.into(),
                    0,
                    3.into(),
                    2,
                    0.into(),
                    (1 << 19).into(),
                    Some(vec![k2.to_vec()]),
                    false,
                    AssertionLevel::Off,
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // T1.commit_ts must be pushed to be larger than T2.start_ts (if we resolve T1)
        storage
            .sched_txn_command(
                commands::CheckSecondaryLocks::new(vec![key1, key2], 1.into(), Default::default()),
                Box::new(move |res| {
                    let pr = res.unwrap();
                    match pr {
                        SecondaryLocksStatus::Locked(l) => {
                            let min_commit_ts = l
                                .iter()
                                .map(|lock_info| lock_info.min_commit_ts)
                                .max()
                                .unwrap();
                            tx.send(min_commit_ts as i32).unwrap();
                        }
                        _ => unreachable!(),
                    }
                }),
            )
            .unwrap();
        assert!(rx.recv().unwrap() > 10);
    }
    // this test shows that the scheduler take `response_policy` in `WriteResult`
    // serious, ie. call the callback at expected stage when writing to the
    // engine
    #[test]
    fn test_scheduler_response_policy() {
        struct Case<T: 'static + StorageCallbackType + Send> {
            expected_writes: Vec<ExpectedWrite>,

            command: TypedCommand<T>,
            pipelined_pessimistic_lock: bool,
        }

        impl<T: 'static + StorageCallbackType + Send> Case<T> {
            fn run(self) {
                let mut builder =
                    MockEngineBuilder::from_rocks_engine(TestEngineBuilder::new().build().unwrap());
                for expected_write in self.expected_writes {
                    builder = builder.add_expected_write(expected_write)
                }
                let engine = builder.build();
                let mut builder = TestStorageBuilderApiV1::from_engine_and_lock_mgr(
                    engine,
                    MockLockManager::new(),
                );
                builder.config.enable_async_apply_prewrite = true;
                if self.pipelined_pessimistic_lock {
                    builder
                        .pipelined_pessimistic_lock
                        .store(true, Ordering::Relaxed);
                }
                let storage = builder.build().unwrap();
                let (tx, rx) = channel();
                storage
                    .sched_txn_command(
                        self.command,
                        Box::new(move |res| {
                            tx.send(res).unwrap();
                        }),
                    )
                    .unwrap();
                rx.recv().unwrap().unwrap();
            }
        }

        let keys = [b"k1", b"k2"];
        let values = [b"v1", b"v2"];
        let mutations = vec![
            Mutation::make_put(Key::from_raw(keys[0]), keys[0].to_vec()),
            Mutation::make_put(Key::from_raw(keys[1]), values[1].to_vec()),
        ];

        let on_applied_case = Case {
            // this case's command return ResponsePolicy::OnApplied
            // tested by `test_response_stage` in command::prewrite
            expected_writes: vec![
                ExpectedWrite::new()
                    .expect_no_committed_cb()
                    .expect_no_proposed_cb(),
                ExpectedWrite::new()
                    .expect_no_committed_cb()
                    .expect_no_proposed_cb(),
            ],

            command: Prewrite::new(
                mutations.clone(),
                keys[0].to_vec(),
                TimeStamp::new(10),
                0,
                false,
                1,
                TimeStamp::default(),
                TimeStamp::default(),
                None,
                false,
                AssertionLevel::Off,
                vec![],
                Context::default(),
            ),
            pipelined_pessimistic_lock: false,
        };
        let on_commited_case = Case {
            // this case's command return ResponsePolicy::OnCommitted
            // tested by `test_response_stage` in command::prewrite
            expected_writes: vec![
                ExpectedWrite::new().expect_committed_cb(),
                ExpectedWrite::new().expect_committed_cb(),
            ],

            command: Prewrite::new(
                mutations,
                keys[0].to_vec(),
                TimeStamp::new(10),
                0,
                false,
                1,
                TimeStamp::default(),
                TimeStamp::default(),
                Some(vec![]),
                false,
                AssertionLevel::Off,
                vec![],
                Context::default(),
            ),
            pipelined_pessimistic_lock: false,
        };
        let on_proposed_case = Case {
            // this case's command return ResponsePolicy::OnProposed
            // untested, but all AcquirePessimisticLock should return ResponsePolicy::OnProposed now
            // and the scheduler expected to take OnProposed serious when
            // enable pipelined pessimistic lock
            expected_writes: vec![
                ExpectedWrite::new().expect_proposed_cb(),
                ExpectedWrite::new().expect_proposed_cb(),
            ],

            command: AcquirePessimisticLock::new(
                keys.iter().map(|&it| (Key::from_raw(it), true)).collect(),
                keys[0].to_vec(),
                TimeStamp::new(10),
                0,
                false,
                TimeStamp::new(11),
                None,
                false,
                TimeStamp::new(12),
                false,
                false,
                false,
                Context::default(),
            ),
            pipelined_pessimistic_lock: true,
        };
        let on_proposed_fallback_case = Case {
            // this case's command return ResponsePolicy::OnProposed
            // but when pipelined pessimistic lock is off,
            // the scheduler should fallback to use OnApplied
            expected_writes: vec![
                ExpectedWrite::new().expect_no_proposed_cb(),
                ExpectedWrite::new().expect_no_proposed_cb(),
            ],

            command: AcquirePessimisticLock::new(
                keys.iter().map(|&it| (Key::from_raw(it), true)).collect(),
                keys[0].to_vec(),
                TimeStamp::new(10),
                0,
                false,
                TimeStamp::new(11),
                None,
                false,
                TimeStamp::new(12),
                false,
                false,
                false,
                Context::default(),
            ),
            pipelined_pessimistic_lock: false,
        };

        on_applied_case.run();
        on_commited_case.run();
        on_proposed_case.run();
        on_proposed_fallback_case.run();
    }

    #[test]
    fn test_resolve_commit_pessimistic_locks() {
        let mut storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();

        // Pessimistically lock k1, k2, k3, k4, after the pessimistic retry k2 is no
        // longer needed and the pessimistic lock on k2 is left.
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![
                        (Key::from_raw(b"k1"), false),
                        (Key::from_raw(b"k2"), false),
                        (Key::from_raw(b"k3"), false),
                        (Key::from_raw(b"k4"), false),
                        (Key::from_raw(b"k5"), false),
                        (Key::from_raw(b"k6"), false),
                    ],
                    10,
                    10,
                    false,
                    false,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Prewrite keys except the k2.
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::with_defaults(
                    vec![
                        (
                            Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec()),
                            DoPessimisticCheck,
                        ),
                        (
                            Mutation::make_put(Key::from_raw(b"k3"), b"v2".to_vec()),
                            DoPessimisticCheck,
                        ),
                        (
                            Mutation::make_put(Key::from_raw(b"k4"), b"v4".to_vec()),
                            DoPessimisticCheck,
                        ),
                        (
                            Mutation::make_put(Key::from_raw(b"k5"), b"v5".to_vec()),
                            DoPessimisticCheck,
                        ),
                        (
                            Mutation::make_put(Key::from_raw(b"k6"), b"v6".to_vec()),
                            DoPessimisticCheck,
                        ),
                    ],
                    b"k1".to_vec(),
                    10.into(),
                    10.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Commit the primary key.
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"k1")],
                    10.into(),
                    20.into(),
                    false,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Pessimistically rollback the k2 lock.
        // Non lite lock resolve on k1 and k2, there should no errors as lock on k2 is
        // pessimistic type.
        must_rollback(&mut storage.engine, b"k2", 10, false);
        let mut temp_map = HashMap::default();
        temp_map.insert(10.into(), 20.into());
        storage
            .sched_txn_command(
                commands::ResolveLock::new(
                    temp_map.clone(),
                    None,
                    vec![
                        (
                            Key::from_raw(b"k1"),
                            mvcc::Lock::new(
                                mvcc::LockType::Put,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                Some(b"v1".to_vec()),
                                10.into(),
                                0,
                                11.into(),
                            ),
                        ),
                        (
                            Key::from_raw(b"k2"),
                            mvcc::Lock::new(
                                mvcc::LockType::Pessimistic,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                None,
                                10.into(),
                                0,
                                11.into(),
                            ),
                        ),
                    ],
                    Default::default(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Non lite lock resolve on k3 and k4, there should be no errors.
        storage
            .sched_txn_command(
                commands::ResolveLock::new(
                    temp_map.clone(),
                    None,
                    vec![
                        (
                            Key::from_raw(b"k3"),
                            mvcc::Lock::new(
                                mvcc::LockType::Put,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                Some(b"v3".to_vec()),
                                10.into(),
                                0,
                                11.into(),
                            ),
                        ),
                        (
                            Key::from_raw(b"k4"),
                            mvcc::Lock::new(
                                mvcc::LockType::Put,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                Some(b"v4".to_vec()),
                                10.into(),
                                0,
                                11.into(),
                            ),
                        ),
                    ],
                    Default::default(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Unlock the k6 first.
        // Non lite lock resolve on k5 and k6, error should be reported.
        must_rollback(&mut storage.engine, b"k6", 10, true);
        storage
            .sched_txn_command(
                commands::ResolveLock::new(
                    temp_map,
                    None,
                    vec![
                        (
                            Key::from_raw(b"k5"),
                            mvcc::Lock::new(
                                mvcc::LockType::Put,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                Some(b"v5".to_vec()),
                                10.into(),
                                0,
                                11.into(),
                            ),
                        ),
                        (
                            Key::from_raw(b"k6"),
                            mvcc::Lock::new(
                                mvcc::LockType::Put,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                Some(b"v6".to_vec()),
                                10.into(),
                                0,
                                11.into(),
                            ),
                        ),
                    ],
                    Default::default(),
                    Context::default(),
                ),
                expect_fail_callback(tx, 6, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::TxnLockNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    // Test check_api_version.
    // See the following for detail:
    //   * rfc: https://github.com/tikv/rfcs/blob/master/text/0069-api-v2.md.
    //   * proto: https://github.com/pingcap/kvproto/blob/master/proto/kvrpcpb.proto,
    //     enum APIVersion.
    #[test]
    fn test_check_api_version() {
        use error_code::storage::*;

        const TIDB_KEY_CASE: &[u8] = b"t_a";
        const TXN_KEY_CASE: &[u8] = b"x\0a";
        const RAW_KEY_CASE: &[u8] = b"r\0a";

        let test_data = vec![
            // storage api_version = V1, for backward compatible.
            (
                ApiVersion::V1,                    // storage api_version
                ApiVersion::V1,                    // request api_version
                CommandKind::get,                  // command kind
                vec![TIDB_KEY_CASE, RAW_KEY_CASE], // keys
                None,                              // expected error code
            ),
            (
                ApiVersion::V1,
                ApiVersion::V1,
                CommandKind::raw_get,
                vec![RAW_KEY_CASE, TXN_KEY_CASE],
                None,
            ),
            // storage api_version = V1ttl, allow RawKV request only.
            (
                ApiVersion::V1ttl,
                ApiVersion::V1,
                CommandKind::raw_get,
                vec![RAW_KEY_CASE],
                None,
            ),
            (
                ApiVersion::V1ttl,
                ApiVersion::V1,
                CommandKind::get,
                vec![TIDB_KEY_CASE],
                Some(API_VERSION_NOT_MATCHED),
            ),
            // storage api_version = V1, reject V2 request.
            (
                ApiVersion::V1,
                ApiVersion::V2,
                CommandKind::get,
                vec![TIDB_KEY_CASE],
                Some(API_VERSION_NOT_MATCHED),
            ),
            // storage api_version = V2.
            // backward compatible for TiDB request, and TiDB request only.
            (
                ApiVersion::V2,
                ApiVersion::V1,
                CommandKind::get,
                vec![TIDB_KEY_CASE, TIDB_KEY_CASE],
                None,
            ),
            (
                ApiVersion::V2,
                ApiVersion::V1,
                CommandKind::raw_get,
                vec![TIDB_KEY_CASE, TIDB_KEY_CASE],
                Some(API_VERSION_NOT_MATCHED),
            ),
            (
                ApiVersion::V2,
                ApiVersion::V1,
                CommandKind::get,
                vec![TIDB_KEY_CASE, TXN_KEY_CASE],
                Some(INVALID_KEY_MODE),
            ),
            (
                ApiVersion::V2,
                ApiVersion::V1,
                CommandKind::get,
                vec![RAW_KEY_CASE],
                Some(INVALID_KEY_MODE),
            ),
            // V2 api validation.
            (
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::get,
                vec![TXN_KEY_CASE],
                None,
            ),
            (
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::raw_get,
                vec![RAW_KEY_CASE, RAW_KEY_CASE],
                None,
            ),
            (
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::get,
                vec![RAW_KEY_CASE, TXN_KEY_CASE],
                Some(INVALID_KEY_MODE),
            ),
            (
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::raw_get,
                vec![RAW_KEY_CASE, TXN_KEY_CASE],
                Some(INVALID_KEY_MODE),
            ),
            (
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::get,
                vec![TIDB_KEY_CASE],
                Some(INVALID_KEY_MODE),
            ),
        ];

        for (i, (storage_api_version, req_api_version, cmd, keys, err)) in
            test_data.into_iter().enumerate()
        {
            // TODO: refactor to use `Api` parameter.
            let res = StorageApiV1::<RocksEngine, MockLockManager>::check_api_version(
                storage_api_version,
                req_api_version,
                cmd,
                keys,
            );
            if let Some(err) = err {
                assert!(res.is_err(), "case {}", i);
                assert_eq!(res.unwrap_err().error_code(), err, "case {}", i);
            } else {
                assert!(res.is_ok(), "case {} {:?}", i, res);
            }
        }
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn test_check_api_version_ranges() {
        use error_code::storage::*;

        const TIDB_KEY_CASE: &[(Option<&[u8]>, Option<&[u8]>)] = &[
            (Some(b"t_a"), Some(b"t_z")),
            (Some(b"t"), Some(b"u")),
            (Some(b"m"), Some(b"n")),
            (Some(b"m_a"), Some(b"m_z")),
        ];
        const TXN_KEY_CASE: &[(Option<&[u8]>, Option<&[u8]>)] =
            &[(Some(b"x\0a"), Some(b"x\0z")), (Some(b"x"), Some(b"y"))];
        const RAW_KEY_CASE: &[(Option<&[u8]>, Option<&[u8]>)] =
            &[(Some(b"r\0a"), Some(b"r\0z")), (Some(b"r"), Some(b"s"))];
        // The cases that should fail in API V2
        const TIDB_KEY_CASE_APIV2_ERR: &[(Option<&[u8]>, Option<&[u8]>)] = &[
            (Some(b"t_a"), Some(b"ua")),
            (Some(b"t"), None),
            (None, Some(b"t_z")),
            (Some(b"m_a"), Some(b"na")),
            (Some(b"m"), None),
            (None, Some(b"m_z")),
        ];
        const TXN_KEY_CASE_APIV2_ERR: &[(Option<&[u8]>, Option<&[u8]>)] = &[
            (Some(b"x\0a"), Some(b"ya")),
            (Some(b"x"), None),
            (None, Some(b"x\0z")),
        ];
        const RAW_KEY_CASE_APIV2_ERR: &[(Option<&[u8]>, Option<&[u8]>)] = &[
            (Some(b"r\0a"), Some(b"sa")),
            (Some(b"r"), None),
            (None, Some(b"r\0z")),
        ];

        let test_case = |storage_api_version,
                         req_api_version,
                         cmd,
                         range: &[(Option<&[u8]>, Option<&[u8]>)],
                         err| {
            // TODO: refactor to use `Api` parameter.
            let res = StorageApiV1::<RocksEngine, MockLockManager>::check_api_version_ranges(
                storage_api_version,
                req_api_version,
                cmd,
                range.iter().cloned(),
            );
            if let Some(err) = err {
                assert!(res.is_err());
                assert_eq!(res.unwrap_err().error_code(), err);
            } else {
                res.unwrap();
            }
        };

        // storage api_version = V1, for backward compatible.
        test_case(
            ApiVersion::V1,    // storage api_version
            ApiVersion::V1,    // request api_version
            CommandKind::scan, // command kind
            TIDB_KEY_CASE,     // ranges
            None,              // expected error code
        );
        test_case(
            ApiVersion::V1,
            ApiVersion::V1,
            CommandKind::raw_scan,
            TIDB_KEY_CASE,
            None,
        );
        test_case(
            ApiVersion::V1,
            ApiVersion::V1,
            CommandKind::raw_scan,
            TIDB_KEY_CASE_APIV2_ERR,
            None,
        );
        // storage api_version = V1ttl, allow RawKV request only.
        test_case(
            ApiVersion::V1ttl,
            ApiVersion::V1,
            CommandKind::raw_scan,
            RAW_KEY_CASE,
            None,
        );
        test_case(
            ApiVersion::V1ttl,
            ApiVersion::V1,
            CommandKind::raw_scan,
            RAW_KEY_CASE_APIV2_ERR,
            None,
        );
        test_case(
            ApiVersion::V1ttl,
            ApiVersion::V1,
            CommandKind::scan,
            TIDB_KEY_CASE,
            Some(API_VERSION_NOT_MATCHED),
        );
        // storage api_version = V1, reject V2 request.
        test_case(
            ApiVersion::V1,
            ApiVersion::V2,
            CommandKind::scan,
            TIDB_KEY_CASE,
            Some(API_VERSION_NOT_MATCHED),
        );
        // storage api_version = V2.
        // backward compatible for TiDB request, and TiDB request only.
        test_case(
            ApiVersion::V2,
            ApiVersion::V1,
            CommandKind::scan,
            TIDB_KEY_CASE,
            None,
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V1,
            CommandKind::raw_scan,
            TIDB_KEY_CASE,
            Some(API_VERSION_NOT_MATCHED),
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V1,
            CommandKind::scan,
            TXN_KEY_CASE,
            Some(INVALID_KEY_MODE),
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V1,
            CommandKind::scan,
            RAW_KEY_CASE,
            Some(INVALID_KEY_MODE),
        );
        // V2 api validation.
        test_case(
            ApiVersion::V2,
            ApiVersion::V2,
            CommandKind::scan,
            TXN_KEY_CASE,
            None,
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V2,
            CommandKind::raw_scan,
            RAW_KEY_CASE,
            None,
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V2,
            CommandKind::scan,
            RAW_KEY_CASE,
            Some(INVALID_KEY_MODE),
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V2,
            CommandKind::raw_scan,
            TXN_KEY_CASE,
            Some(INVALID_KEY_MODE),
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V2,
            CommandKind::scan,
            TIDB_KEY_CASE,
            Some(INVALID_KEY_MODE),
        );

        for range in TIDB_KEY_CASE_APIV2_ERR {
            test_case(
                ApiVersion::V2,
                ApiVersion::V1,
                CommandKind::scan,
                &[*range],
                Some(INVALID_KEY_MODE),
            );
        }
        for range in TXN_KEY_CASE_APIV2_ERR {
            test_case(
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::scan,
                &[*range],
                Some(INVALID_KEY_MODE),
            );
        }
        for range in RAW_KEY_CASE_APIV2_ERR {
            test_case(
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::raw_scan,
                &[*range],
                Some(INVALID_KEY_MODE),
            );
        }
    }

    #[test]
    fn test_write_in_memory_pessimistic_locks() {
        let txn_ext = Arc::new(TxnExt::default());
        let lock_mgr = MockLockManager::new();
        let storage = TestStorageBuilderApiV1::new(lock_mgr.clone())
            .pipelined_pessimistic_lock(true)
            .in_memory_pessimistic_lock(true)
            .build_for_txn(txn_ext.clone())
            .unwrap();
        let (tx, rx) = channel();

        let k1 = Key::from_raw(b"k1");
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![(k1.clone(), false)],
                    10,
                    10,
                    false,
                    false,
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        {
            let pessimistic_locks = txn_ext.pessimistic_locks.read();
            let lock = pessimistic_locks.get(&k1).unwrap();
            assert_eq!(
                lock,
                &(
                    PessimisticLock {
                        primary: Box::new(*b"k1"),
                        start_ts: 10.into(),
                        ttl: 3000,
                        for_update_ts: 10.into(),
                        min_commit_ts: 11.into(),
                        last_change_ts: TimeStamp::zero(),
                        versions_to_last_change: 1,
                    },
                    false
                )
            );
        }

        let (tx, rx) = channel();
        // The written in-memory pessimistic lock should be visible, so the new lock
        // request should fail.
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![(k1.clone(), false)],
                    20,
                    20,
                    false,
                    false,
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        // The request enters lock waiting state.
        rx.recv_timeout(Duration::from_millis(100)).unwrap_err();
        lock_mgr.simulate_timeout_all();
        // The lock-waiting request is cancelled.
        rx.recv().unwrap().unwrap_err();

        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![(
                        Mutation::make_put(k1.clone(), b"v".to_vec()),
                        DoPessimisticCheck,
                    )],
                    b"k1".to_vec(),
                    10.into(),
                    3000,
                    10.into(),
                    1,
                    20.into(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        rx.recv().unwrap().unwrap();
        // After prewrite, the memory lock should be removed.
        {
            let pessimistic_locks = txn_ext.pessimistic_locks.read();
            assert!(pessimistic_locks.get(&k1).is_none());
        }
    }

    #[test]
    fn test_disable_in_memory_pessimistic_locks() {
        let txn_ext = Arc::new(TxnExt::default());
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .pipelined_pessimistic_lock(true)
            .in_memory_pessimistic_lock(false)
            .build_for_txn(txn_ext.clone())
            .unwrap();
        let (tx, rx) = channel();

        let k1 = Key::from_raw(b"k1");
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![(k1.clone(), false)],
                    10,
                    10,
                    false,
                    false,
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();
        // When disabling in-memory pessimistic lock, the lock map should remain
        // unchanged.
        assert!(txn_ext.pessimistic_locks.read().is_empty());

        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![(Mutation::make_put(k1, b"v".to_vec()), DoPessimisticCheck)],
                    b"k1".to_vec(),
                    10.into(),
                    3000,
                    10.into(),
                    1,
                    20.into(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        // Prewrite still succeeds
        rx.recv().unwrap().unwrap();
    }
}
