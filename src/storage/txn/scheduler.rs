// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath
//! Scheduler which schedules the execution of `storage::Command`s.
//!
//! There is one scheduler for each store. It receives commands from clients,
//! executes them against the MVCC layer storage engine.
//!
//! Logically, the data organization hierarchy from bottom to top is row ->
//! region -> store -> database. But each region is replicated onto N stores for
//! reliability, the replicas form a Raft group, one of which acts as the
//! leader. When the client read or write a row, the command is sent to the
//! scheduler which is on the region leader's store.
//!
//! Scheduler runs in a single-thread event loop, but command executions are
//! delegated to a pool of worker thread.
//!
//! Scheduler keeps track of all the running commands and uses latches to ensure
//! serialized access to the overlapping rows involved in concurrent commands.
//! But note that scheduler only ensures serialized access to the overlapping
//! rows at command level, but a transaction may consist of multiple commands,
//! therefore conflicts may happen at transaction level. Transaction semantics
//! is ensured by the transaction protocol implemented in the client library,
//! which is transparent to the scheduler.

use std::{
    future::Future,
    marker::PhantomData,
    mem,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use collections::HashMap;
use concurrency_manager::{ConcurrencyManager, KeyHandleGuard, TrackedBackupTs};
use crossbeam::utils::CachePadded;
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use futures::{StreamExt, compat::Future01CompatExt};
use kvproto::{
    kvrpcpb::{self, CommandPri, Context, DiskFullOpt, ExtraOp},
    pdpb::QueryKind,
};
use parking_lot::{Mutex, MutexGuard, RwLockWriteGuard};
use pd_client::{Feature, FeatureGate};
use raftstore::{
    RegionInfoAccessor,
    coprocessor::RegionInfoProvider,
    store::{FlowStatsReporter, TxnExt},
};
use resource_metering::{FutureExt, ResourceTagFactory};
use smallvec::{SmallVec, smallvec};
use tikv_kv::{Modify, Snapshot, SnapshotExt, WriteData, WriteEvent};
use tikv_util::{
    deadline::Deadline,
    quota_limiter::QuotaLimiter,
    sys::thread::ThreadBuildWrapper,
    time::{Instant, duration_to_sec},
    timer::GLOBAL_TIMER_HANDLE,
};
use tracker::{TrackerToken, get_tls_tracker_token, set_tls_tracker_token};

use crate::{
    command_process_read, command_process_write,
    read_pool::ReadPoolHandle,
    server::lock_manager::waiter_manager,
    storage::{
        DynamicConfigs, Error as StorageError, ErrorInner as StorageErrorInner,
        PessimisticLockKeyResult, PessimisticLockResults,
        config::Config,
        errors::SharedError,
        get_priority_tag,
        kv::{
            self, Engine, Result as EngineResult, SnapContext, Statistics, destroy_tls_engine,
            set_tls_engine, with_tls_engine,
        },
        lock_manager::{
            self, DiagnosticContext, LockManager, LockWaitToken,
            lock_wait_context::{LockWaitContext, PessimisticLockKeyCallback},
            lock_waiting_queue::{DelayedNotifyAllFuture, LockWaitEntry, LockWaitQueues},
        },
        metrics::*,
        mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, ReleasedLock},
        txn::{
            Error, ProcessResult,
            commands::{
                self, Command, ReleasedLocks, ResponsePolicy, WriteContext, WriteResult,
                WriteResultLockInfo, txn_file, txn_file::TxnFileCommand,
            },
            flow_controller::{FlowControlHelper, FlowController},
            latch::Lock,
            region_latch::{GlobalLatches, WakeupTask},
            sched_pool::{tls_collect_query, tls_collect_scan_details},
        },
        types::StorageCallback,
    },
};

const TASKS_SLOTS_NUM: usize = 1 << 12; // 4096 slots.

// The default limit is set to be very large. Then, requests without
// `max_exectuion_duration` will not be aborted unexpectedly.
pub const DEFAULT_EXECUTION_DURATION_LIMIT: Duration = Duration::from_secs(24 * 60 * 60);

const IN_MEMORY_PESSIMISTIC_LOCK: Feature = Feature::require(6, 0, 0);
pub const LAST_CHANGE_TS: Feature = Feature::require(6, 5, 0);

type SVec<T> = SmallVec<[T; 4]>;

/// Task is a running command.
pub(super) struct Task {
    pub(super) cid: u64,
    pub(super) tracker: TrackerToken,
    pub(super) cmd: Command,
    pub(super) extra_op: ExtraOp,
}

impl Task {
    /// Creates a task for a running command.
    pub(super) fn new(cid: u64, tracker: TrackerToken, cmd: Command) -> Task {
        Task {
            cid,
            tracker,
            cmd,
            extra_op: ExtraOp::Noop,
        }
    }
}

struct CmdTimer {
    tag: CommandKind,
    begin: Instant,
}

impl Drop for CmdTimer {
    fn drop(&mut self) {
        SCHED_HISTOGRAM_VEC_STATIC
            .get(self.tag)
            .observe(self.begin.saturating_elapsed_secs());
    }
}

// It stores context of a task.
struct TaskContext {
    task: Option<Task>,

    lock: Lock,
    cb: Option<SchedulerTaskCallback>,
    pr: Option<ProcessResult>,
    woken_up_resumable_lock_requests: SVec<Box<LockWaitEntry>>,
    // The one who sets `owned` from false to true is allowed to take
    // `cb` and `pr` safely.
    owned: AtomicBool,
    write_bytes: usize,
    tag: CommandKind,
    // How long it waits on latches.
    // latch_timer: Option<Instant>,
    latch_timer: Instant,
    // Total duration of a command.
    _cmd_timer: CmdTimer,
}

impl TaskContext {
    fn new(
        mut task: Task,
        cb: SchedulerTaskCallback,
        prepared_latches: Option<Lock>,
    ) -> TaskContext {
        let tag = task.cmd.tag();
        let lock = prepared_latches.unwrap_or_else(|| task.cmd.gen_lock());
        task.cmd.ctx_mut().set_keyspace_id(lock.keyspace_id); // keyspace_id may be fixed in `Lock::new`.

        // The initial locks should be either all acquired or all not acquired.
        assert!(lock.owned_count == 0 || lock.owned_count == lock.required_hashes.len());
        // Write command should acquire write lock.
        if !task.cmd.readonly() && !lock.is_write_lock() {
            panic!("write lock is expected for command {}", task.cmd);
        }
        let write_bytes = if lock.is_write_lock() {
            task.cmd.write_bytes()
        } else {
            0
        };

        TaskContext {
            task: Some(task),
            lock,
            cb: Some(cb),
            pr: None,
            woken_up_resumable_lock_requests: smallvec![],
            owned: AtomicBool::new(false),
            write_bytes,
            tag,
            latch_timer: Instant::now(),
            _cmd_timer: CmdTimer {
                tag,
                begin: Instant::now(),
            },
        }
    }

    fn on_schedule(&mut self) {
        SCHED_LATCH_HISTOGRAM_VEC
            .get(self.tag)
            .observe(self.latch_timer.saturating_elapsed_secs());
    }

    // Try to own this TaskContext by setting `owned` from false to true.
    // Returns whether it succeeds to own the TaskContext.
    fn try_own(&self) -> bool {
        self.owned
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }
}

pub enum SchedulerTaskCallback {
    NormalRequestCallback(StorageCallback),
    LockKeyCallbacks(Vec<PessimisticLockKeyCallback>),
}

impl SchedulerTaskCallback {
    fn execute(self, pr: ProcessResult) {
        match self {
            Self::NormalRequestCallback(cb) => cb.execute(pr),
            Self::LockKeyCallbacks(cbs) => match pr {
                ProcessResult::Failed { err }
                | ProcessResult::PessimisticLockRes { res: Err(err) } => {
                    let err = SharedError::from(err);
                    for cb in cbs {
                        cb(Err(err.clone()), false);
                    }
                }
                ProcessResult::PessimisticLockRes { res: Ok(v) } => {
                    assert_eq!(v.0.len(), cbs.len());
                    for (res, cb) in v.0.into_iter().zip(cbs) {
                        cb(Ok(res), false)
                    }
                }
                _ => unreachable!(),
            },
        }
    }

    fn unwrap_normal_request_callback(self) -> StorageCallback {
        match self {
            Self::NormalRequestCallback(cb) => cb,
            _ => panic!(""),
        }
    }
}

struct SchedulerInner<L: LockManager> {
    // slot_id -> { cid -> `TaskContext` } in the slot.
    task_slots: Vec<CachePadded<Mutex<HashMap<u64, TaskContext>>>>,

    // write concurrency control
    latches: GlobalLatches,

    engine: Option<kvengine::Engine>,

    sched_pending_write_threshold: usize,

    // worker pool
    worker_pool: ReadPoolHandle,

    // background pool, introduced for deadline check tasks
    background_pool: Arc<tikv_util::worker_pool::WorkerPool>,

    // used to control write flow
    running_write_bytes: CachePadded<AtomicUsize>,

    flow_controller: Arc<FlowController>,

    lock_mgr: L,

    concurrency_manager: ConcurrencyManager,

    pipelined_pessimistic_lock: Arc<AtomicBool>,

    in_memory_pessimistic_lock: Arc<AtomicBool>,

    enable_async_apply_prewrite: bool,

    pessimistic_lock_wake_up_delay_duration_ms: Arc<AtomicU64>,

    resource_tag_factory: ResourceTagFactory,

    lock_wait_queues: LockWaitQueues<L>,

    quota_limiter: Arc<QuotaLimiter>,
    feature_gate: FeatureGate,

    region_info_accessor: Option<RegionInfoAccessor>,
}

#[inline]
fn id_index(cid: u64) -> usize {
    cid as usize % TASKS_SLOTS_NUM
}

lazy_static::lazy_static! {
    static ref CMD_ID_ALLOC: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
}

impl<L: LockManager> SchedulerInner<L> {
    /// Generates the next command ID.
    #[inline]
    fn gen_id(&self) -> u64 {
        let id = CMD_ID_ALLOC.fetch_add(1, Ordering::Relaxed);
        id + 1
    }

    #[inline]
    fn get_task_slot(&self, cid: u64) -> MutexGuard<'_, HashMap<u64, TaskContext>> {
        self.task_slots[id_index(cid)].lock()
    }

    fn new_task_context(
        &self,
        task: Task,
        callback: SchedulerTaskCallback,
        prepared_latches: Option<Lock>,
    ) -> TaskContext {
        let tctx = TaskContext::new(task, callback, prepared_latches);
        let running_write_bytes = self
            .running_write_bytes
            .fetch_add(tctx.write_bytes, Ordering::AcqRel) as i64;
        SCHED_WRITING_BYTES_GAUGE.set(running_write_bytes + tctx.write_bytes as i64);
        SCHED_CONTEX_GAUGE.inc();
        tctx
    }

    fn dequeue_task_context(&self, cid: u64) -> TaskContext {
        let tctx = self.get_task_slot(cid).remove(&cid).unwrap();

        let running_write_bytes = self
            .running_write_bytes
            .fetch_sub(tctx.write_bytes, Ordering::AcqRel) as i64;
        SCHED_WRITING_BYTES_GAUGE.set(running_write_bytes - tctx.write_bytes as i64);
        SCHED_CONTEX_GAUGE.dec();

        tctx
    }

    /// Try to own the corresponding task context and take the callback.
    ///
    /// If the task is been processing, it should be owned.
    /// If it has been finished, then it is not in the slot.
    /// In both cases, cb should be None. Otherwise, cb should be some.
    fn try_own_and_take_cb(&self, cid: u64) -> Option<SchedulerTaskCallback> {
        self.get_task_slot(cid)
            .get_mut(&cid)
            .and_then(|tctx| if tctx.try_own() { tctx.cb.take() } else { None })
    }

    fn take_task_cb(&self, cid: u64) -> Option<SchedulerTaskCallback> {
        self.get_task_slot(cid)
            .get_mut(&cid)
            .map(|tctx| tctx.cb.take())
            .unwrap_or(None)
    }

    fn store_lock_changes(
        &self,
        cid: u64,
        woken_up_resumable_lock_requests: SVec<Box<LockWaitEntry>>,
    ) {
        self.get_task_slot(cid)
            .get_mut(&cid)
            .map(move |tctx| {
                assert!(tctx.woken_up_resumable_lock_requests.is_empty());
                tctx.woken_up_resumable_lock_requests = woken_up_resumable_lock_requests;
            })
            .unwrap();
    }

    fn too_busy(&self, region_id: u64) -> bool {
        fail_point!("txn_scheduler_busy", |_| true);
        self.running_write_bytes.load(Ordering::Acquire) >= self.sched_pending_write_threshold
            || self.flow_controller.should_drop(region_id)
    }

    /// Tries to acquire all the required latches for a command when waken up by
    /// another finished command.
    ///
    /// Returns a deadline error if the deadline is exceeded. Returns the `Task`
    /// if all latches are acquired, returns `None` otherwise.
    fn acquire_lock_on_wakeup(
        &self,
        cid: u64,
    ) -> Result<Option<Task>, (u64 /* region_id */, StorageError)> {
        let mut task_slot = self.get_task_slot(cid);
        let tctx = task_slot.get_mut(&cid).unwrap_or_else(|| {
            panic!("cid {} not in task slot", cid);
        });
        if self.latches.acquire(&mut tctx.lock, cid) {
            // Check deadline early.
            // TODO: implement a fast path to release without acquiring the latch.
            if let Err(e) = tctx.task.as_ref().unwrap().cmd.deadline().check() {
                SCHED_STAGE_COUNTER_VEC
                    .get(tctx.tag)
                    .wakeup_deadline_exceeded
                    .inc();
                debug!("acquire_lock_on_wakeup: deadline exceeded"; "cid" => cid, "lock" => ?tctx.lock);
                return Err((tctx.lock.region_id, e.into()));
            }
            tctx.on_schedule();
            return Ok(tctx.task.take());
        }
        Ok(None)
    }

    fn dump_wait_for_entries(&self, cb: waiter_manager::Callback) {
        self.lock_mgr.dump_wait_for_entries(cb);
    }

    /// Spawn a task in the background pool with monitoring
    fn spawn_background<F>(&self, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.background_pool.spawn(async move {
            SCHED_BACKGROUND_POOL_RUNNING_TASKS_GAUGE.inc();
            f.await;
            SCHED_BACKGROUND_POOL_RUNNING_TASKS_GAUGE.dec();
        });
    }
}

/// Scheduler which schedules the execution of `storage::Command`s.
#[derive(Clone)]
pub struct Scheduler<E: Engine, L: LockManager> {
    inner: Arc<SchedulerInner<L>>,
    // The engine can be fetched from the thread local storage of scheduler threads.
    // So, we don't store the engine here.
    _engine: PhantomData<E>,
}

unsafe impl<E: Engine, L: LockManager> Send for Scheduler<E, L> {}

impl<E: Engine, L: LockManager> Scheduler<E, L> {
    /// Creates a scheduler.
    pub(in crate::storage) fn new<R: FlowStatsReporter>(
        engine: E,
        lock_mgr: L,
        concurrency_manager: ConcurrencyManager,
        config: &Config,
        dynamic_configs: DynamicConfigs,
        flow_controller: Arc<FlowController>,
        _reporter: R,
        resource_tag_factory: ResourceTagFactory,
        quota_limiter: Arc<QuotaLimiter>,
        feature_gate: FeatureGate,
        read_pool_handle: ReadPoolHandle,
        region_info_accessor: Option<RegionInfoAccessor>,
    ) -> Self {
        let t = Instant::now_coarse();
        let mut task_slots = Vec::with_capacity(TASKS_SLOTS_NUM);
        for _ in 0..TASKS_SLOTS_NUM {
            task_slots.push(Mutex::new(Default::default()).into());
        }

        let lock_wait_queues = LockWaitQueues::new(lock_mgr.clone());
        let background_pool = {
            let engine_for_background = Arc::new(std::sync::Mutex::new(engine.clone()));
            let props = tikv_util::thread_group::current_properties();
            tokio::runtime::Builder::new_multi_thread()
                .thread_name_fn(move || {
                    static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                    let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                    format!("sched-background-{}", id)
                })
                .worker_threads(config.scheduler_background_worker_pool_size)
                .after_start_wrapper(move || {
                    let engine = engine_for_background.lock().unwrap().clone();
                    set_tls_engine(engine);
                    tikv_util::thread_group::set_properties(props.clone());
                })
                .before_stop_wrapper(|| unsafe { destroy_tls_engine::<E>() })
                .enable_all()
                .build()
                .unwrap()
        };

        let inner = Arc::new(SchedulerInner {
            task_slots,
            latches: GlobalLatches::new(config.scheduler_concurrency),
            engine: engine.get_kvengine(),
            running_write_bytes: AtomicUsize::new(0).into(),
            sched_pending_write_threshold: config.scheduler_pending_write_threshold.0 as usize,
            worker_pool: read_pool_handle,
            background_pool: Arc::new(background_pool.into()),
            lock_mgr,
            concurrency_manager,
            pipelined_pessimistic_lock: dynamic_configs.pipelined_pessimistic_lock,
            in_memory_pessimistic_lock: dynamic_configs.in_memory_pessimistic_lock,
            enable_async_apply_prewrite: config.enable_async_apply_prewrite,
            pessimistic_lock_wake_up_delay_duration_ms: dynamic_configs.wake_up_delay_duration_ms,
            flow_controller,
            resource_tag_factory,
            lock_wait_queues,
            quota_limiter,
            feature_gate,
            region_info_accessor,
        });

        slow_log!(
            t.saturating_elapsed(),
            "initialized the transaction scheduler"
        );
        Scheduler {
            inner,
            _engine: PhantomData,
        }
    }

    pub fn dump_wait_for_entries(&self, cb: waiter_manager::Callback) {
        self.inner.dump_wait_for_entries(cb);
    }

    pub(in crate::storage) fn run_cmd(&self, cmd: Command, callback: StorageCallback) {
        let region_id = cmd.ctx().get_region_id();
        // write flow control
        if cmd.need_flow_control() && self.inner.too_busy(region_id) {
            SCHED_TOO_BUSY_COUNTER_VEC.get(cmd.tag()).inc();
            callback.execute(ProcessResult::Failed {
                err: StorageError::from(StorageErrorInner::SchedTooBusy),
            });
            return;
        }

        // Workaround for the issue that the pending command is keep increasing, when
        // there is no leader in raft group.
        if let Err(err) = self.check_leader(region_id) {
            callback.execute(ProcessResult::Failed {
                err: StorageError::from(StorageErrorInner::Kv(err)),
            });
            return;
        }

        self.schedule_command(
            None,
            cmd,
            SchedulerTaskCallback::NormalRequestCallback(callback),
            None,
        );
    }

    fn check_leader(&self, region_id: u64) -> kv::Result<()> {
        if let Some(region_info_accessor) = self.inner.region_info_accessor.as_ref() {
            let region = region_info_accessor.find_region_by_id(region_id);
            if let Some(region) = region {
                let is_leader = fail::eval("check_leader_peer_is_leader", |t| {
                    t.and_then(|s: String| s.parse::<bool>().ok())
                })
                .flatten()
                .unwrap_or(region.is_leader());
                if is_leader {
                    return Ok(());
                }
                let mut not_leader_err = kvproto::errorpb::Error::default();
                not_leader_err.mut_not_leader().set_region_id(region_id);
                if let Some(leader) = region.leader_peer() {
                    not_leader_err.mut_not_leader().set_leader(leader.clone());
                }
                return Err(kv::ErrorInner::Request(not_leader_err).into());
            }
            let mut not_found_err = kvproto::errorpb::Error::default();
            not_found_err
                .mut_region_not_found()
                .set_region_id(region_id);
            return Err(kv::ErrorInner::Request(not_found_err).into());
        }
        Ok(())
    }

    /// Releases all the latches held by a command.
    fn release_latches(
        &self,
        lock: Lock,
        cid: u64,
        keep_latches_for_next_cmd: Option<(u64, &Lock)>,
    ) {
        let wakeup_list = self
            .inner
            .latches
            .release(&lock, cid, keep_latches_for_next_cmd);
        for w in wakeup_list {
            if w.in_waiting_list {
                self.try_to_wake_up(w.cid);
            } else {
                self.fast_fail_wake_up(w);
            }
        }
    }

    fn schedule_command(
        &self,
        specified_cid: Option<u64>,
        cmd: Command,
        callback: SchedulerTaskCallback,
        prepared_latches: Option<Lock>,
    ) {
        let cid = specified_cid.unwrap_or_else(|| self.inner.gen_id());
        let tracker = get_tls_tracker_token();
        debug!("received new command"; "cid" => cid, "cmd" => ?cmd, "tracker" => ?tracker);
        #[cfg(feature = "debug-trace-txn-tasks")]
        crate::storage::txn::debug::trace_txn_task(cid, &cmd);

        let tag = cmd.tag();
        let priority_tag = get_priority_tag(cmd.priority());
        SCHED_STAGE_COUNTER_VEC.get(tag).new.inc();
        SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
            .get(priority_tag)
            .inc();

        let region_id = cmd.ctx().get_region_id();
        tikv_util::set_current_region(region_id);
        if cmd.can_build_txn_file() {
            self.build_and_schedule_txn_file_cmd(tag, cid, cmd, callback, prepared_latches);
            return;
        }
        let ts = cmd.ts().into_inner();
        let mut task_slot = self.inner.get_task_slot(cid);
        let tctx = task_slot.entry(cid).or_insert_with(|| {
            self.inner
                .new_task_context(Task::new(cid, tracker, cmd), callback, prepared_latches)
        });
        tctx.lock.set_region_id_start_ts(region_id, ts);

        if self.inner.latches.acquire(&mut tctx.lock, cid) {
            fail_point!("txn_scheduler_acquire_success");
            tctx.on_schedule();
            let task = tctx.task.take().unwrap();
            drop(task_slot);
            self.execute(task);
            return;
        }
        let task = tctx.task.as_ref().unwrap();
        let deadline = task.cmd.deadline();
        let cmd_ctx = task.cmd.ctx().clone();
        self.fail_fast_or_check_deadline(cid, tag, cmd_ctx, deadline);
        fail_point!("txn_scheduler_acquire_fail");
    }

    fn fail_fast_or_check_deadline(
        &self,
        cid: u64,
        tag: CommandKind,
        cmd_ctx: Context,
        deadline: Deadline,
    ) {
        let sched = self.clone();
        // Spawn the task in the background pool
        // There is no cancellation for deadline check tasks, they can pile up
        // even if the tasks already finished successfully. See
        // cloud-storage-engine#2516 for more details
        self.inner.spawn_background(async move {
            match unsafe {
                with_tls_engine(|engine: &mut E| engine.precheck_write_with_ctx(&cmd_ctx))
            } {
                // Precheck failed, try to return err early.
                Err(e) => {
                    let cb = sched.inner.try_own_and_take_cb(cid);
                    // The task is not processing or finished currently. It's safe
                    // to response early here. In the future, the task will be waked up
                    // and it will finished with DeadlineExceeded error.
                    // As the cb is taken here, it will not be executed anymore.
                    if let Some(cb) = cb {
                        let pr = ProcessResult::Failed {
                            err: StorageError::from(e),
                        };
                        Self::early_response(
                            cid,
                            cb,
                            pr,
                            tag,
                            CommandStageKind::precheck_write_err,
                        );
                    }
                }
                Ok(()) => {
                    SCHED_STAGE_COUNTER_VEC.get(tag).precheck_write_ok.inc();
                    // Check deadline in background.
                    GLOBAL_TIMER_HANDLE
                        .delay(deadline.to_std_instant())
                        .compat()
                        .await
                        .unwrap();
                    let cb = sched.inner.try_own_and_take_cb(cid);
                    if let Some(cb) = cb {
                        cb.execute(ProcessResult::Failed {
                            err: StorageErrorInner::DeadlineExceeded.into(),
                        })
                    }
                }
            }
        });
    }

    /// Tries to acquire all the necessary latches. If all the necessary latches
    /// are acquired, the method initiates a get snapshot operation for further
    /// processing.
    fn try_to_wake_up(&self, cid: u64) {
        match self.inner.acquire_lock_on_wakeup(cid) {
            Ok(Some(task)) => {
                fail_point!("txn_scheduler_try_to_wake_up");
                self.execute(task);
            }
            Ok(None) => {}
            Err((region_id, err)) => {
                // Spawn the finish task to the pool to avoid stack overflow
                // when many queuing tasks fail successively.
                let this = self.clone();
                // Force spawn, otherwise we may directly invoke `finish_with_err`.
                self.force_spawn(
                    async move {
                        tikv_util::set_current_region(region_id);
                        this.finish_with_err(cid, err);
                    },
                    cid,
                );
            }
        }
    }

    /// Wake up the task NOT in waiting list with fast fail.
    fn fast_fail_wake_up(&self, task: WakeupTask) {
        debug_assert!(!task.in_waiting_list);
        let cid = task.cid;
        let (tag, deadline) = match task.deadline {
            Some(deadline) => (CommandKind::unknown, deadline),
            None => {
                let task_slot = self.inner.get_task_slot(cid);
                let tctx = task_slot.get(&cid).unwrap_or_else(|| {
                    panic!("cid {} is not in the slot", cid);
                });
                (tctx.tag, tctx.task.as_ref().unwrap().cmd.deadline())
            }
        };

        if let Err(e) = deadline.check() {
            SCHED_STAGE_COUNTER_VEC
                .get(tag)
                .wakeup_deadline_exceeded
                .inc();
            debug!("fast_wake_up: deadline exceeded"; "cid" => cid);
            self.finish_with_err_without_release_latch(cid, e);
            return;
        }

        self.try_to_wake_up(cid);
    }

    fn schedule_awakened_pessimistic_locks(
        &self,
        specified_cid: Option<u64>,
        prepared_latches: Option<Lock>,
        mut awakened_entries: SVec<Box<LockWaitEntry>>,
    ) {
        let key_callbacks: Vec<_> = awakened_entries
            .iter_mut()
            .map(|i| i.key_cb.take().unwrap().into_inner())
            .collect();

        let cmd = commands::AcquirePessimisticLockResumed::from_lock_wait_entries(awakened_entries);

        // TODO: Make flow control take effect on this thing.
        self.schedule_command(
            specified_cid,
            cmd.into(),
            SchedulerTaskCallback::LockKeyCallbacks(key_callbacks),
            prepared_latches,
        );
    }

    // pub for test
    pub fn get_sched_pool(&self) -> &ReadPoolHandle {
        &self.inner.worker_pool
    }

    fn try_spawn<F>(&self, f: F, priority: CommandPri, cid: u64, tag: CommandKind)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if let Err(err) = self.get_sched_pool().spawn(f, priority, cid) {
            debug!("try spawn task failed"; "cid" => cid, "err" => ?err);
            SCHED_TOO_BUSY_COUNTER_VEC.get(tag).inc();
            self.finish_with_err(cid, StorageError::from(StorageErrorInner::SchedTooBusy));
        }
    }

    pub fn force_spawn<F>(&self, f: F, cid: u64)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // Spawn high priority task must succeed.
        self.get_sched_pool()
            .spawn(f, CommandPri::High, cid)
            .unwrap()
    }

    /// Executes the task in the sched pool.
    fn execute(&self, mut task: Task) {
        set_tls_tracker_token(task.tracker);
        let sched = self.clone();
        let pri = task.cmd.priority();
        let cid = task.cid;
        let tag = task.cmd.tag();
        self.try_spawn(
            async move {
                fail_point!("scheduler_start_execute");
                if sched.check_task_deadline_exceeded(&task) {
                    return;
                }

                SCHED_STAGE_COUNTER_VEC.get(tag).snapshot.inc();

                if !task.cmd.readonly() {
                    // TiDB may incorrectly set `replica_read` to true when retrying even for
                    // write commands. So we fix the flags here.
                    // See https://github.com/tidbcloud/cloud-storage-engine/issues/1211.
                    let ctx = task.cmd.ctx_mut();
                    ctx.set_replica_read(false);
                    ctx.set_stale_read(false);
                }

                let snap_ctx = SnapContext {
                    pb_ctx: task.cmd.ctx(),
                    ..Default::default()
                };
                // The program is currently in scheduler worker threads.
                // Safety: `self.inner.worker_pool` should ensure that a TLS engine exists.
                match unsafe { with_tls_engine(|engine: &mut E| kv::snapshot(engine, snap_ctx)) }
                    .await
                {
                    Ok(snapshot) => {
                        SCHED_STAGE_COUNTER_VEC.get(tag).snapshot_ok.inc();
                        let term = snapshot.ext().get_term();
                        let extra_op = snapshot.ext().get_txn_extra_op();
                        if !sched
                            .inner
                            .get_task_slot(task.cid)
                            .get(&task.cid)
                            .unwrap()
                            .try_own()
                        {
                            sched.finish_with_err(task.cid, StorageErrorInner::DeadlineExceeded);
                            return;
                        }

                        if let Some(term) = term {
                            task.cmd.ctx_mut().set_term(term.get());
                        }
                        task.extra_op = extra_op;

                        debug!(
                            "process cmd with snapshot";
                            "cid" => task.cid, "term" => ?term, "extra_op" => ?extra_op,
                            "trakcer" => ?task.tracker
                        );
                        sched.process(snapshot, task).await;
                    }
                    Err(err) => {
                        SCHED_STAGE_COUNTER_VEC.get(tag).snapshot_err.inc();

                        debug!("get snapshot failed"; "cid" => task.cid, "err" => ?err);
                        sched.finish_with_err(task.cid, Error::from(err));
                    }
                }
            },
            pri,
            cid,
            tag,
        );
    }

    /// Calls the callback with an error.
    fn finish_with_err<ER>(&self, cid: u64, err: ER)
    where
        StorageError: From<ER>,
    {
        self.finish_with_err_opt(cid, err, true);
    }

    fn finish_with_err_without_release_latch<ER>(&self, cid: u64, err: ER)
    where
        StorageError: From<ER>,
    {
        self.finish_with_err_opt(cid, err, false);
    }

    fn finish_with_err_opt<ER>(&self, cid: u64, err: ER, release_latch: bool)
    where
        StorageError: From<ER>,
    {
        let err = StorageError::from(err);
        debug!("write command finished with error"; "cid" => cid, "err" => ?err);
        #[cfg(feature = "debug-trace-txn-tasks")]
        crate::storage::txn::debug::trace_txn_task_error(cid, &err);

        let tctx = self.inner.dequeue_task_context(cid);

        SCHED_STAGE_COUNTER_VEC.get(tctx.tag).error.inc();

        let pr = ProcessResult::Failed { err };
        if let Some(cb) = tctx.cb {
            cb.execute(pr);
        }

        if !tctx.woken_up_resumable_lock_requests.is_empty() {
            self.put_back_lock_wait_entries(tctx.woken_up_resumable_lock_requests);
        }

        if release_latch {
            self.release_latches(tctx.lock, cid, None);
        }
    }

    /// Event handler for the success of read.
    ///
    /// If a next command is present, continues to execute; otherwise, delivers
    /// the result to the callback.
    fn on_read_finished(&self, cid: u64, pr: ProcessResult, tag: CommandKind) {
        SCHED_STAGE_COUNTER_VEC.get(tag).read_finish.inc();

        debug!("read command finished"; "cid" => cid);
        let tctx = self.inner.dequeue_task_context(cid);
        if let ProcessResult::NextCommand { cmd } = pr {
            SCHED_STAGE_COUNTER_VEC.get(tag).next_cmd.inc();
            self.schedule_command(None, cmd, tctx.cb.unwrap(), None);
        } else {
            tctx.cb.unwrap().execute(pr);
        }

        self.release_latches(tctx.lock, cid, None);
    }

    /// Event handler for the success of write.
    fn on_write_finished(
        &self,
        cid: u64,
        pr: Option<ProcessResult>,
        result: EngineResult<()>,
        lock_guards: Vec<KeyHandleGuard>,
        pipelined: bool,
        async_apply_prewrite: bool,
        tag: CommandKind,
    ) {
        // TODO: Does async apply prewrite worth a special metric here?
        if pipelined {
            SCHED_STAGE_COUNTER_VEC
                .get(tag)
                .pipelined_write_finish
                .inc();
        } else if async_apply_prewrite {
            SCHED_STAGE_COUNTER_VEC
                .get(tag)
                .async_apply_prewrite_finish
                .inc();
        } else {
            SCHED_STAGE_COUNTER_VEC.get(tag).write_finish.inc();
        }

        debug!("write command finished";
            "cid" => cid, "pipelined" => pipelined, "async_apply_prewrite" => async_apply_prewrite);
        drop(lock_guards);
        let tctx = self.inner.dequeue_task_context(cid);

        let mut do_wake_up = !tctx.woken_up_resumable_lock_requests.is_empty();
        // If pipelined pessimistic lock or async apply prewrite takes effect, it's not
        // guaranteed that the proposed or committed callback is surely invoked, which
        // takes and invokes `tctx.cb(tctx.pr)`.
        if let Some(cb) = tctx.cb {
            let pr = match result {
                Ok(()) => pr.or(tctx.pr).unwrap(),
                Err(e) => {
                    if !Self::is_undetermined_error(&e) {
                        do_wake_up = false;
                    } else {
                        warn!(
                            "undetermined error: {:?} cid={}, tag={}, process result={:?}",
                            e, cid, tag, &pr
                        );
                    }
                    ProcessResult::Failed {
                        err: StorageError::from(e),
                    }
                }
            };
            if let ProcessResult::NextCommand { cmd } = pr {
                SCHED_STAGE_COUNTER_VEC.get(tag).next_cmd.inc();
                self.schedule_command(None, cmd, cb, None);
            } else {
                cb.execute(pr);
            }
        } else {
            assert!(pipelined || async_apply_prewrite);
        }

        // TODO: Update lock wait relationships after acquiring some locks.

        if do_wake_up {
            let keyspace_id = tctx.lock.keyspace_id;
            let woken_up_resumable_lock_requests = tctx.woken_up_resumable_lock_requests;
            let next_cid = self.inner.gen_id();
            let mut next_latches = Self::gen_latches_for_lock_wait_entries(
                keyspace_id,
                woken_up_resumable_lock_requests.iter(),
            );

            self.release_latches(tctx.lock, cid, Some((next_cid, &next_latches)));

            next_latches.force_assume_acquired();
            self.schedule_awakened_pessimistic_locks(
                Some(next_cid),
                Some(next_latches),
                woken_up_resumable_lock_requests,
            );
        } else {
            if !tctx.woken_up_resumable_lock_requests.is_empty() {
                self.put_back_lock_wait_entries(tctx.woken_up_resumable_lock_requests);
            }
            self.release_latches(tctx.lock, cid, None);
        }
    }

    fn gen_latches_for_lock_wait_entries<'a>(
        keyspace_id: u32,
        entries: impl IntoIterator<Item = &'a Box<LockWaitEntry>>,
    ) -> Lock {
        Lock::new(
            keyspace_id,
            entries.into_iter().map(|entry| &entry.key),
            None,
        )
    }

    /// Event handler for the request of waiting for lock
    fn on_wait_for_lock(
        &self,
        ctx: &Context,
        cid: u64,
        lock_info: WriteResultLockInfo,
        tracker: TrackerToken,
    ) {
        let key = lock_info.key.clone();
        let lock_digest = lock_info.lock_digest;
        let start_ts = lock_info.parameters.start_ts;
        let is_first_lock = lock_info.parameters.is_first_lock;
        let wait_timeout = lock_info.parameters.wait_timeout;

        let diag_ctx = DiagnosticContext {
            key: lock_info.key.to_raw().unwrap(),
            resource_group_tag: ctx.get_resource_group_tag().into(),
            tracker,
        };
        let wait_token = self.inner.lock_mgr.allocate_token();

        let (lock_req_ctx, lock_wait_entry, lock_info_pb) =
            self.make_lock_waiting(cid, wait_token, lock_info);

        // The entry must be pushed to the lock waiting queue before sending to
        // `lock_mgr`. When the request is canceled in anywhere outside the lock
        // waiting queue (including `lock_mgr`), it first tries to remove the
        // entry from the lock waiting queue. If the entry doesn't exist
        // in the queue, it will be regarded as already popped out from the queue and
        // therefore will woken up, thus the canceling operation will be
        // skipped. So pushing the entry to the queue must be done before any
        // possible cancellation.
        self.inner
            .lock_wait_queues
            .push_lock_wait(lock_wait_entry, lock_info_pb.clone());

        let wait_info = lock_manager::KeyLockWaitInfo {
            key,
            lock_digest,
            lock_info: lock_info_pb,
        };
        self.inner.lock_mgr.wait_for(
            wait_token,
            ctx.get_region_id(),
            ctx.get_region_epoch().clone(),
            ctx.get_term(),
            start_ts,
            wait_info,
            is_first_lock,
            wait_timeout,
            lock_req_ctx.get_callback_for_cancellation(),
            diag_ctx,
        );
    }

    fn on_release_locks(&self, released_locks: ReleasedLocks) -> SVec<Box<LockWaitEntry>> {
        // This function is always called when holding the latch of the involved keys.
        // So if we found the lock waiting queues are empty, there's no chance
        // that other threads/commands adds new lock-wait entries to the keys
        // concurrently. Therefore it's safe to skip waking up when we found the
        // lock waiting queues are empty.
        if self.inner.lock_wait_queues.is_empty() {
            return smallvec![];
        }

        let mut legacy_wake_up_list = SVec::new();
        let mut delay_wake_up_futures = SVec::new();
        let mut resumable_wake_up_list = SVec::new();
        let wake_up_delay_duration_ms = self
            .inner
            .pessimistic_lock_wake_up_delay_duration_ms
            .load(Ordering::Relaxed);

        released_locks.into_iter().for_each(|released_lock| {
            let (lock_wait_entry, delay_wake_up_future) =
                match self.inner.lock_wait_queues.pop_for_waking_up(
                    &released_lock.key,
                    released_lock.start_ts,
                    released_lock.commit_ts,
                    wake_up_delay_duration_ms,
                ) {
                    Some(e) => e,
                    None => return,
                };

            if lock_wait_entry.parameters.allow_lock_with_conflict {
                resumable_wake_up_list.push(lock_wait_entry);
            } else {
                legacy_wake_up_list.push((lock_wait_entry, released_lock));
            }
            if let Some(f) = delay_wake_up_future {
                delay_wake_up_futures.push(f);
            }
        });

        if !legacy_wake_up_list.is_empty() || !delay_wake_up_futures.is_empty() {
            self.wake_up_legacy_pessimistic_locks(legacy_wake_up_list, delay_wake_up_futures);
        }

        resumable_wake_up_list
    }

    fn wake_up_legacy_pessimistic_locks(
        &self,
        legacy_wake_up_list: impl IntoIterator<Item = (Box<LockWaitEntry>, ReleasedLock)>
        + Send
        + 'static,
        delayed_wake_up_futures: impl IntoIterator<Item = DelayedNotifyAllFuture> + Send + 'static,
    ) {
        let self1 = self.clone();
        // Force spawn for internal tasks.
        self.force_spawn(
            async move {
                for (lock_info, released_lock) in legacy_wake_up_list {
                    let cb = lock_info.key_cb.unwrap().into_inner();
                    let e = StorageError::from(Error::from(MvccError::from(
                        MvccErrorInner::WriteConflict {
                            start_ts: lock_info.parameters.start_ts,
                            conflict_start_ts: released_lock.start_ts,
                            conflict_commit_ts: released_lock.commit_ts,
                            key: released_lock.key.into_raw().unwrap(),
                            primary: lock_info.parameters.primary,
                            reason: kvrpcpb::WriteConflictReason::PessimisticRetry,
                        },
                    )));
                    cb(Err(e.into()), false);
                }

                for f in delayed_wake_up_futures {
                    let self2 = self1.clone();
                    self1.force_spawn(
                        async move {
                            let res = f.await;
                            if let Some(resumable_lock_wait_entry) = res {
                                self2.schedule_awakened_pessimistic_locks(
                                    None,
                                    None,
                                    smallvec![resumable_lock_wait_entry],
                                );
                            }
                        },
                        0,
                    );
                }
            },
            0,
        );
    }

    fn is_undetermined_error(e: &tikv_kv::Error) -> bool {
        // TODO: If there's some cases that `engine.async_write` returns error but it's
        // still possible that the data is successfully written, return true.
        matches!(&*(e.0), tikv_kv::ErrorInner::Undetermined(_))
    }

    fn early_response(
        cid: u64,
        cb: SchedulerTaskCallback,
        pr: ProcessResult,
        tag: CommandKind,
        stage: CommandStageKind,
    ) {
        debug!("early return response"; "cid" => cid);
        SCHED_STAGE_COUNTER_VEC.get(tag).get(stage).inc();
        cb.execute(pr);
        // It won't release locks here until write finished.
    }

    /// Process the task in the current thread.
    async fn process(self, snapshot: E::Snap, task: Task) {
        if self.check_task_deadline_exceeded(&task) {
            return;
        }

        let resource_tag = self.inner.resource_tag_factory.new_tag(task.cmd.ctx());
        async {
            let tag = task.cmd.tag();
            fail_point!("scheduler_async_snapshot_finish");
            SCHED_STAGE_COUNTER_VEC.get(tag).process.inc();

            let timer = Instant::now();

            let region_id = task.cmd.ctx().get_region_id();
            let ts = task.cmd.ts();
            let mut statistics = Statistics::default();
            match &task.cmd {
                Command::Prewrite(_) | Command::PrewritePessimistic(_) => {
                    tls_collect_query(region_id, QueryKind::Prewrite);
                }
                Command::AcquirePessimisticLock(_) => {
                    tls_collect_query(region_id, QueryKind::AcquirePessimisticLock);
                }
                Command::Commit(_) => {
                    tls_collect_query(region_id, QueryKind::Commit);
                }
                Command::Rollback(_) | Command::PessimisticRollback(_) => {
                    tls_collect_query(region_id, QueryKind::Rollback);
                }
                _ => {}
            }

            fail_point!("scheduler_process");
            if task.cmd.readonly() {
                self.process_read(snapshot, task, &mut statistics).await;
            } else {
                self.process_write(snapshot, task, &mut statistics).await;
            };
            tls_collect_scan_details(tag.get_str(), &statistics);
            let elapsed = timer.saturating_elapsed();
            slow_log!(
                elapsed,
                "[region {}] scheduler handle command: {}, ts: {}",
                region_id,
                tag,
                ts
            );
        }
        .in_resource_metering_tag(resource_tag)
        .await;
    }

    /// Processes a read command within a worker thread, then posts
    /// `ReadFinished` message back to the `Scheduler`.
    async fn process_read(self, snapshot: E::Snap, task: Task, statistics: &mut Statistics) {
        fail_point!("txn_before_process_read");
        debug!("process read cmd in worker pool"; "cid" => task.cid);

        if let Some(snap_access) = snapshot.get_kvengine_snap() {
            tikv_util::set_current_region(snap_access.get_id());
        }
        let tag = task.cmd.tag();

        let begin_instant = Instant::now();
        let cmd = task.cmd;
        let pr = command_process_read!(cmd, snapshot, statistics)
            .unwrap_or_else(|e| ProcessResult::Failed { err: e.into() });
        SCHED_PROCESSING_READ_HISTOGRAM_STATIC
            .get(tag)
            .observe(begin_instant.saturating_elapsed_secs());
        self.on_read_finished(task.cid, pr, tag);
    }

    /// Processes a write command within a worker thread, then posts either a
    /// `WriteFinished` message if successful or a `FinishedWithErr` message
    /// back to the `Scheduler`.
    async fn process_write(self, snapshot: E::Snap, task: Task, statistics: &mut Statistics) {
        fail_point!("txn_before_process_write");
        let snap = snapshot.get_kvengine_snap();
        let region_id = snap.as_ref().map(|snap| snap.get_id()).unwrap_or_default();
        tikv_util::set_current_region(region_id);
        let write_bytes = task.cmd.write_bytes();
        let tag = task.cmd.tag();
        let cid = task.cid;
        let tracker = task.tracker;
        let scheduler = self.clone();
        let quota_limiter = self.inner.quota_limiter.clone();
        let mut sample = quota_limiter.new_sample(true);
        let pessimistic_lock_mode = self.pessimistic_lock_mode();
        let pipelined =
            task.cmd.can_be_pipelined() && pessimistic_lock_mode == PessimisticLockMode::Pipelined;
        let txn_ext = snapshot.ext().get_txn_ext().cloned();
        let concurrency_manager = self.inner.concurrency_manager.clone();
        let region_limiter = snap.as_ref().map(|snap| snap.get_limiter().clone());

        let deadline = task.cmd.deadline();
        let write_result = {
            let context = WriteContext {
                lock_mgr: &self.inner.lock_mgr,
                concurrency_manager,
                extra_op: task.extra_op,
                statistics,
                async_apply_prewrite: self.inner.enable_async_apply_prewrite,
            };
            let begin_instant = Instant::now();
            let res = command_process_write!(task.cmd, snapshot, context, sample)
                .map_err(StorageError::from);
            SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                .get(tag)
                .observe(begin_instant.saturating_elapsed_secs());
            res
        };

        if write_result.is_ok() {
            // TODO: write bytes can be a bit inaccurate due to error requests or in-memory
            // pessimistic locks.
            sample.add_write_bytes(write_bytes);
        }
        let read_bytes = statistics.cf_statistics(CF_DEFAULT).flow_stats.read_bytes
            + statistics.cf_statistics(CF_LOCK).flow_stats.read_bytes
            + statistics.cf_statistics(CF_WRITE).flow_stats.read_bytes;
        sample.add_read_bytes(read_bytes);
        let quota_delay = quota_limiter.consume_sample(sample, true).await;
        if !quota_delay.is_zero() {
            TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC
                .get(tag)
                .inc_by(quota_delay.as_micros() as u64);
        }

        let WriteResult {
            ctx,
            mut to_be_write,
            rows,
            pr,
            lock_info,
            released_locks,
            lock_guards,
            response_policy,
        } = match deadline
            .check()
            .map_err(StorageError::from)
            .and(write_result)
        {
            // Write prepare failure typically means conflicting transactions are detected. Delivers
            // the error to the callback, and releases the latches.
            Err(err) => {
                SCHED_STAGE_COUNTER_VEC.get(tag).prepare_write_err.inc();
                debug!("write command failed"; "cid" => cid, "err" => ?err);
                scheduler.finish_with_err(cid, err);
                return;
            }
            // Initiates an async write operation on the storage engine, there'll be a
            // `WriteFinished` message when it finishes.
            Ok(res) => res,
        };
        let region_id = ctx.get_region_id();
        SCHED_STAGE_COUNTER_VEC.get(tag).write.inc();

        let mut pr = Some(pr);

        if !lock_info.is_empty() {
            if tag == CommandKind::acquire_pessimistic_lock {
                assert_eq!(lock_info.len(), 1);
                let lock_info = lock_info.into_iter().next().unwrap();

                // Only handle lock waiting if `wait_timeout` is set. Otherwise it indicates
                // that it's a lock-no-wait request and we need to report error
                // immediately.
                if lock_info.parameters.wait_timeout.is_some() {
                    assert_eq!(to_be_write.size(), 0);
                    pr = Some(ProcessResult::Res);

                    scheduler.on_wait_for_lock(&ctx, cid, lock_info, tracker);
                } else {
                    // For requests with `allow_lock_with_conflict`, key errors are set key-wise.
                    // TODO: It's better to return this error from
                    // `commands::AcquirePessimisticLocks::process_write`.
                    if lock_info.parameters.allow_lock_with_conflict {
                        pr = Some(ProcessResult::PessimisticLockRes {
                            res: Err(StorageError::from(Error::from(MvccError::from(
                                MvccErrorInner::KeyIsLocked(lock_info.lock_info_pb),
                            )))),
                        });
                    }
                }
            } else if tag == CommandKind::acquire_pessimistic_lock_resumed {
                // Some requests meets lock again after waiting and resuming.
                scheduler.on_wait_for_lock_after_resuming(cid, pr.as_mut().unwrap(), lock_info);
            } else {
                // WriteResult returning lock info is only expected to exist for pessimistic
                // lock requests.
                unreachable!();
            }
        }

        let woken_up_resumable_entries = if !released_locks.is_empty() {
            scheduler.on_release_locks(released_locks)
        } else {
            smallvec![]
        };

        if !woken_up_resumable_entries.is_empty() {
            scheduler
                .inner
                .store_lock_changes(cid, woken_up_resumable_entries);
        }

        if to_be_write.modifies.is_empty() && to_be_write.txn_file.is_none() {
            scheduler.on_write_finished(cid, pr, Ok(()), lock_guards, false, false, tag);
            return;
        }

        if (tag == CommandKind::acquire_pessimistic_lock
            || tag == CommandKind::acquire_pessimistic_lock_resumed)
            && pessimistic_lock_mode == PessimisticLockMode::InMemory
            && self.try_write_in_memory_pessimistic_locks(
                txn_ext.as_deref(),
                &mut to_be_write,
                &ctx,
            )
        {
            // Safety: `self.sched_pool` ensures a TLS engine exists.
            unsafe {
                with_tls_engine(|engine: &mut E| {
                    // We skip writing the raftstore, but to improve CDC old value hit rate,
                    // we should send the old values to the CDC scheduler.
                    engine.schedule_txn_extra(to_be_write.extra);
                })
            }
            scheduler.on_write_finished(cid, pr, Ok(()), lock_guards, false, false, tag);
            return;
        }

        let mut is_async_apply_prewrite = false;
        let write_size = to_be_write.size();
        if ctx.get_disk_full_opt() == DiskFullOpt::AllowedOnAlmostFull {
            to_be_write.disk_full_opt = DiskFullOpt::AllowedOnAlmostFull
        }
        to_be_write.deadline = Some(deadline);

        let sched = scheduler.clone();

        let mut subscribed = WriteEvent::BASIC_EVENT;
        match response_policy {
            ResponsePolicy::OnCommitted => {
                subscribed |= WriteEvent::EVENT_COMMITTED;
                is_async_apply_prewrite = true;
            }
            ResponsePolicy::OnProposed if pipelined => subscribed |= WriteEvent::EVENT_PROPOSED,
            _ => (),
        }

        let flow_control = FlowControlHelper::new(
            region_id,
            write_size,
            self.inner.flow_controller.clone(),
            region_limiter,
        );
        if flow_control.enabled() {
            if flow_control.is_unlimited() {
                // no need to delay if unthrottled, just call consume to record write flow
                let _ = flow_control.consume();
            } else {
                let start = Instant::now_coarse();

                // Note: cloud storage engine remove the `control_mutex`.
                let consume_delay = flow_control.consume();

                let deadline_delay = deadline.inner().saturating_duration_since(start);
                let deadline_exceeded = consume_delay >= deadline_delay;
                if deadline_exceeded {
                    flow_control.unconsume();
                }

                // Always sleep even if it must exceed deadline.
                let delay = consume_delay.min(deadline_delay);
                let after_delay = if delay > Duration::ZERO {
                    GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + delay)
                        .compat()
                        .await
                        .unwrap();
                    let after_delay = Instant::now_coarse();
                    SCHED_THROTTLE_TIME.observe(duration_to_sec(
                        after_delay.saturating_duration_since(start),
                    ));
                    after_delay
                } else {
                    start
                };

                if deadline_exceeded || after_delay >= deadline.inner() {
                    scheduler.finish_with_err(cid, StorageErrorInner::DeadlineExceeded);
                    if !deadline_exceeded {
                        flow_control.unconsume();
                    }
                    return;
                }
            }
        }

        let (version, term) = (ctx.get_region_epoch().get_version(), ctx.get_term());
        // Mutations on the lock CF should overwrite the memory locks.
        // We only set a deleted flag here, and the lock will be finally removed when it
        // finishes applying. See the comments in `PeerPessimisticLocks` for how this
        // flag is used.
        let txn_ext2 = txn_ext.clone();
        let mut pessimistic_locks_guard = txn_ext2
            .as_ref()
            .map(|txn_ext| txn_ext.pessimistic_locks.write());
        let removed_pessimistic_locks = match pessimistic_locks_guard.as_mut() {
            Some(locks)
                // If there is a leader or region change, removing the locks is unnecessary.
                if locks.term == term && locks.version == version && !locks.is_empty() =>
            {
                to_be_write
                    .modifies
                    .iter()
                    .filter_map(|write| match write {
                        Modify::Put(cf, key, ..) | Modify::Delete(cf, key) if *cf == CF_LOCK => {
                            locks.get_mut(key).map(|(_, deleted)| {
                                *deleted = true;
                                key.to_owned()
                            })
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            }
            _ => vec![],
        };
        // Keep the read lock guard of the pessimistic lock table until the request is
        // sent to the raftstore.
        //
        // If some in-memory pessimistic locks need to be proposed, we will propose
        // another TransferLeader command. Then, we can guarentee even if the proposed
        // locks don't include the locks deleted here, the response message of the
        // transfer leader command must be later than this write command because this
        // write command has been sent to the raftstore. Then, we don't need to worry
        // this request will fail due to the voluntary leader transfer.
        let downgraded_guard = pessimistic_locks_guard.and_then(|guard| {
            (!removed_pessimistic_locks.is_empty()).then(|| RwLockWriteGuard::downgrade(guard))
        });
        let backup_ts_checked: Option<TrackedBackupTs> = to_be_write.backup_ts_checked.take();
        let on_applied = Box::new(move |res: &mut kv::Result<()>| {
            drop(backup_ts_checked);
            if res.is_ok() && !removed_pessimistic_locks.is_empty() {
                // Removing pessimistic locks when it succeeds to apply. This should be done in
                // the apply thread, to make sure it happens before other admin commands are
                // executed.
                if let Some(mut pessimistic_locks) = txn_ext
                    .as_ref()
                    .map(|txn_ext| txn_ext.pessimistic_locks.write())
                {
                    // If epoch version or term does not match, region or leader change has
                    // happened, so we needn't remove the key.
                    if pessimistic_locks.term == term && pessimistic_locks.version == version {
                        for key in removed_pessimistic_locks {
                            pessimistic_locks.remove(&key);
                        }
                    }
                }
            }
        });

        let mut res = unsafe {
            with_tls_engine(|e: &mut E| {
                tikv_util::set_current_region(region_id);
                e.async_write(&ctx, to_be_write, subscribed, Some(on_applied))
            })
        };
        drop(downgraded_guard);

        while let Some(ev) = res.next().await {
            match ev {
                WriteEvent::Committed => {
                    let early_return = (|| {
                        fail_point!("before_async_apply_prewrite_finish", |_| false);
                        true
                    })();
                    if WriteEvent::subscribed_committed(subscribed) && early_return {
                        // Currently, the only case that response is returned after finishing
                        // commit is async applying prewrites for async commit transactions.
                        let cb = scheduler.inner.take_task_cb(cid);
                        Self::early_response(
                            cid,
                            cb.unwrap(),
                            pr.take().unwrap(),
                            tag,
                            CommandStageKind::async_apply_prewrite,
                        );
                    }
                }
                WriteEvent::Proposed => {
                    let early_return = (|| {
                        fail_point!("before_pipelined_write_finish", |_| false);
                        true
                    })();
                    if WriteEvent::subscribed_proposed(subscribed) && early_return {
                        // The normal write process is respond to clients and release
                        // latches after async write finished. If pipelined pessimistic
                        // locking is enabled, the process becomes parallel and there are
                        // two msgs for one command:
                        //   1. Msg::PipelinedWrite: respond to clients
                        //   2. Msg::WriteFinished: deque context and release latches
                        // Currently, the only case that response is returned after finishing
                        // proposed phase is pipelined pessimistic lock.
                        // TODO: Unify the code structure of pipelined pessimistic lock and
                        // async apply prewrite.
                        let cb = scheduler.inner.take_task_cb(cid);
                        Self::early_response(
                            cid,
                            cb.unwrap(),
                            pr.take().unwrap(),
                            tag,
                            CommandStageKind::pipelined_write,
                        );
                    }
                }
                WriteEvent::Finished(res) => {
                    fail_point!("scheduler_async_write_finish");
                    let ok = res.is_ok();

                    sched.on_write_finished(
                        cid,
                        pr,
                        res,
                        lock_guards,
                        pipelined,
                        is_async_apply_prewrite,
                        tag,
                    );
                    KV_COMMAND_KEYWRITE_HISTOGRAM_VEC
                        .get(tag)
                        .observe(rows as f64);

                    if !ok {
                        // Only consume the quota when write succeeds, otherwise failed write
                        // requests may exhaust the quota and other write requests would be in long
                        // delay.
                        if flow_control.enabled() {
                            flow_control.unconsume();
                        }
                    }
                    return;
                }
            }
        }
        // If it's not finished while the channel is closed, it means the write
        // is undeterministic. in this case, we don't know whether the
        // request is finished or not, so we should not release latch as
        // it may break correctness.
    }

    /// Returns whether it succeeds to write pessimistic locks to the in-memory
    /// lock table.
    fn try_write_in_memory_pessimistic_locks(
        &self,
        txn_ext: Option<&TxnExt>,
        to_be_write: &mut WriteData,
        context: &Context,
    ) -> bool {
        let txn_ext = match txn_ext {
            Some(txn_ext) => txn_ext,
            None => return false,
        };
        let mut pessimistic_locks = txn_ext.pessimistic_locks.write();
        // When not writable, it only means we cannot write locks to the in-memory lock
        // table, but it is still possible for the region to propose request.
        // When term or epoch version has changed, the request must fail. To be simple,
        // here we just let the request fallback to propose and let raftstore generate
        // an appropriate error.
        if !pessimistic_locks.is_writable()
            || pessimistic_locks.term != context.get_term()
            || pessimistic_locks.version != context.get_region_epoch().get_version()
        {
            return false;
        }
        match pessimistic_locks.insert(mem::take(&mut to_be_write.modifies)) {
            Ok(()) => {
                IN_MEMORY_PESSIMISTIC_LOCKING_COUNTER_STATIC.success.inc();
                true
            }
            Err(modifies) => {
                IN_MEMORY_PESSIMISTIC_LOCKING_COUNTER_STATIC.full.inc();
                to_be_write.modifies = modifies;
                false
            }
        }
    }

    /// If the task has expired, return `true` and call the callback of
    /// the task with a `DeadlineExceeded` error.
    #[inline]
    fn check_task_deadline_exceeded(&self, task: &Task) -> bool {
        if let Err(e) = task.cmd.deadline().check() {
            self.finish_with_err(task.cid, e);
            true
        } else {
            false
        }
    }

    fn pessimistic_lock_mode(&self) -> PessimisticLockMode {
        let pipelined = self
            .inner
            .pipelined_pessimistic_lock
            .load(Ordering::Relaxed);
        let in_memory = self
            .inner
            .in_memory_pessimistic_lock
            .load(Ordering::Relaxed)
            && self
                .inner
                .feature_gate
                .can_enable(IN_MEMORY_PESSIMISTIC_LOCK);
        if pipelined && in_memory {
            PessimisticLockMode::InMemory
        } else if pipelined {
            PessimisticLockMode::Pipelined
        } else {
            PessimisticLockMode::Sync
        }
    }

    fn make_lock_waiting(
        &self,
        cid: u64,
        lock_wait_token: LockWaitToken,
        lock_info: WriteResultLockInfo,
    ) -> (LockWaitContext<L>, Box<LockWaitEntry>, kvrpcpb::LockInfo) {
        let mut slot = self.inner.get_task_slot(cid);
        let task_ctx = slot.get_mut(&cid).unwrap();
        let cb = task_ctx.cb.take().unwrap();

        let ctx = LockWaitContext::new(
            lock_info.key.clone(),
            self.inner.lock_wait_queues.clone(),
            lock_wait_token,
            cb.unwrap_normal_request_callback(),
            lock_info.parameters.allow_lock_with_conflict,
        );
        let first_batch_cb = ctx.get_callback_for_first_write_batch();
        task_ctx.cb = Some(SchedulerTaskCallback::NormalRequestCallback(first_batch_cb));
        drop(slot);

        assert!(lock_info.req_states.is_none());

        let lock_wait_entry = Box::new(LockWaitEntry {
            key: lock_info.key,
            lock_hash: lock_info.lock_digest.hash,
            parameters: lock_info.parameters,
            should_not_exist: lock_info.should_not_exist,
            lock_wait_token,
            req_states: ctx.get_shared_states().clone(),
            legacy_wake_up_index: None,
            key_cb: Some(ctx.get_callback_for_blocked_key().into()),
        });

        (ctx, lock_wait_entry, lock_info.lock_info_pb)
    }

    fn make_lock_waiting_after_resuming(
        &self,
        lock_info: WriteResultLockInfo,
        cb: PessimisticLockKeyCallback,
    ) -> Box<LockWaitEntry> {
        Box::new(LockWaitEntry {
            key: lock_info.key,
            lock_hash: lock_info.lock_digest.hash,
            parameters: lock_info.parameters,
            should_not_exist: lock_info.should_not_exist,
            lock_wait_token: lock_info.lock_wait_token,
            // This must be called after an execution fo AcquirePessimisticLockResumed, in which
            // case there must be a valid req_state.
            req_states: lock_info.req_states.unwrap(),
            legacy_wake_up_index: None,
            key_cb: Some(cb.into()),
        })
    }

    fn on_wait_for_lock_after_resuming(
        &self,
        cid: u64,
        pr: &mut ProcessResult,
        lock_info: Vec<WriteResultLockInfo>,
    ) {
        if lock_info.is_empty() {
            return;
        }

        // TODO: Update lock wait relationship.

        let results = match pr {
            ProcessResult::PessimisticLockRes {
                res: Ok(PessimisticLockResults(res)),
            } => res,
            _ => unreachable!(),
        };

        let mut slot = self.inner.get_task_slot(cid);
        let task_ctx = slot.get_mut(&cid).unwrap();
        let cbs = match task_ctx.cb {
            Some(SchedulerTaskCallback::LockKeyCallbacks(ref mut v)) => v,
            _ => unreachable!(),
        };
        assert_eq!(results.len(), cbs.len());

        let finished_len = results.len() - lock_info.len();

        let original_results = std::mem::replace(results, Vec::with_capacity(finished_len));
        let original_cbs = std::mem::replace(cbs, Vec::with_capacity(finished_len));
        let mut lock_wait_entries = SmallVec::<[_; 10]>::with_capacity(lock_info.len());
        let mut lock_info_it = lock_info.into_iter();

        for (result, cb) in original_results.into_iter().zip(original_cbs) {
            if let PessimisticLockKeyResult::Waiting = &result {
                let lock_info = lock_info_it.next().unwrap();
                let lock_info_pb = lock_info.lock_info_pb.clone();
                let entry = self.make_lock_waiting_after_resuming(lock_info, cb);
                lock_wait_entries.push((entry, lock_info_pb));
            } else {
                results.push(result);
                cbs.push(cb);
            }
        }

        assert!(lock_info_it.next().is_none());
        assert_eq!(results.len(), cbs.len());

        // Release the mutex in the latch slot.
        drop(slot);

        // Add to the lock waiting queue.
        // TODO: the request may be canceled from lock manager at this time. If so, it
        // should not be added to the queue.
        for (entry, lock_info_pb) in lock_wait_entries {
            self.inner
                .lock_wait_queues
                .push_lock_wait(entry, lock_info_pb);
        }
    }

    fn put_back_lock_wait_entries(&self, entries: impl IntoIterator<Item = Box<LockWaitEntry>>) {
        for entry in entries.into_iter() {
            // TODO: Do not pass `default` as the lock info. Here we need another method
            // `put_back_lock_wait`, which doesn't require updating lock info and
            // additionally checks if the lock wait entry is already canceled.
            self.inner
                .lock_wait_queues
                .push_lock_wait(entry, Default::default());
        }
    }

    // Build commands with `can_build_txn_file() == true` to TxnFileCommand, and
    // re-schedule it.
    fn build_and_schedule_txn_file_cmd(
        &self,
        tag: CommandKind,
        cid: u64,
        cmd: Command,
        callback: SchedulerTaskCallback,
        prepared_latches: Option<Lock>,
    ) {
        let sched = self.clone();
        let manager = self.inner.engine.as_ref().unwrap().get_txn_chunk_manager();
        let worker_pool = manager.worker_pool().clone();
        let mut start = Instant::now();
        // Force spawn as we just re-enqueue the tasks. Besides, as the task has not
        // been inserted to task slot, `finish_with_err` will fail.
        sched.clone().force_spawn(
            async move {
                let region_id = cmd.ctx().get_region_id();
                tikv_util::set_current_region(region_id);

                let snap_ctx = SnapContext {
                    pb_ctx: cmd.ctx(),
                    ..Default::default()
                };
                match unsafe { with_tls_engine(|engine: &mut E| kv::snapshot(engine, snap_ctx)) }
                    .await
                {
                    Ok(snapshot) => {
                        SCHED_STAGE_COUNTER_VEC.get(tag).snapshot_ok.inc();
                        let start_ts = cmd.ts();
                        let chunks_id = txn_file::command_chunks_to_load(&cmd);
                        if !chunks_id.is_empty() && !manager.all_chunks_exists(chunks_id) {
                            let snap = snapshot.get_kvengine_snap().unwrap();
                            let encryption_key = snap.get_encryption_key();
                            let manager = manager.clone();
                            let chunks_id = chunks_id.to_vec();
                            let prepare_res = worker_pool
                                .spawn_blocking(move || -> kvengine::Result<()> {
                                    tikv_util::set_current_region(region_id);

                                    let wait_dur = start.saturating_elapsed();
                                    start = Instant::now();
                                    manager
                                        .prepare_txn_chunks(chunks_id.clone(), encryption_key)?;
                                    let prepare_dur = start.saturating_elapsed();
                                    SCHED_TXN_FILE_HISTOGRAM_VEC_STATIC
                                        .get(tag)
                                        .prepare
                                        .observe(prepare_dur.as_secs_f64());
                                    info!("prepare txn file chunks";
                                        "region_id" => region_id,
                                        "cid" => cid,
                                        "chunks_id" => ?chunks_id,
                                        "chunks_count" => chunks_id.len(),
                                        "wait_dur" => ?wait_dur,
                                        "prepare_dur" => ?prepare_dur,
                                    );
                                    Ok(())
                                })
                                .await;
                            tikv_util::set_current_region(region_id);
                            if let Err(err) = prepare_res.unwrap() {
                                error!("txn file: prepare failed";
                                    "start_ts" => start_ts,
                                    "cid" => cid,
                                    "err" => ?err);
                                Self::finish_txn_file_cmd_with_err(cid, tag, callback, err);
                                return;
                            }
                        }

                        let txn_file_cmd = match TxnFileCommand::try_from(cmd, snapshot, manager) {
                            Ok(txn_file_cmd) => txn_file_cmd,
                            Err(err) => {
                                error!("txn file: build cmd failed";
                                    "start_ts" => start_ts,
                                    "cid" => cid,
                                    "err" => ?err);
                                Self::finish_txn_file_cmd_with_err(cid, tag, callback, err);
                                return;
                            }
                        };
                        txn_file_cmd.incr_cmd_metric();
                        sched.schedule_command(Some(cid), txn_file_cmd, callback, prepared_latches);
                    }
                    Err(err) => {
                        SCHED_STAGE_COUNTER_VEC.get(tag).snapshot_err.inc();
                        debug!("txn file: get snapshot failed"; "cid" => cid, "cmd" => ?cmd, "err" => ?err);
                        Self::finish_txn_file_cmd_with_err(cid, tag, callback, err);
                    }
                }
            },
            cid,
        );
    }

    fn finish_txn_file_cmd_with_err<ER>(
        _cid: u64,
        tag: CommandKind,
        callback: SchedulerTaskCallback,
        err: ER,
    ) where
        StorageError: From<ER>,
    {
        let err = StorageError::from(err);

        #[cfg(feature = "debug-trace-txn-tasks")]
        crate::storage::txn::debug::trace_txn_task_error(_cid, &err);

        SCHED_STAGE_COUNTER_VEC.get(tag).error.inc();
        callback.execute(ProcessResult::Failed { err });
    }
}

#[derive(Debug, PartialEq)]
enum PessimisticLockMode {
    // Return success only if the pessimistic lock is persisted.
    Sync,
    // Return success after the pessimistic lock is proposed successfully.
    Pipelined,
    // Try to store pessimistic locks only in the memory.
    InMemory,
}

#[cfg(test)]
mod tests {
    use std::thread;

    use futures_executor::block_on;
    use kvproto::kvrpcpb::{BatchRollbackRequest, CheckTxnStatusRequest, Context};
    use raftstore::store::{ReadStats, WriteStats};
    use tikv_util::{config::ReadableSize, future::paired_future_callback};
    use txn_types::{Key, TimeStamp};

    use super::*;
    use crate::{
        config::UnifiedReadPoolConfig,
        read_pool::build_tokio_pool,
        storage::{
            RocksEngine, TestEngineBuilder, TxnStatus,
            kv::{Error as KvError, ErrorInner as KvErrorInner},
            lock_manager::{MockLockManager, WaitTimeout},
            mvcc::{self, Mutation},
            test_util::latest_feature_gate,
            txn::{commands, commands::TypedCommand, flow_controller::FlowController, latch::*},
        },
    };

    #[derive(Clone)]
    struct DummyReporter;

    impl FlowStatsReporter for DummyReporter {
        fn report_read_stats(&self, _read_stats: ReadStats) {}
        fn report_write_stats(&self, _write_stats: WriteStats) {}
    }

    // TODO(cosven): use this in the following test cases to reduce duplicate code.
    fn new_test_scheduler() -> (Scheduler<RocksEngine, MockLockManager>, RocksEngine) {
        let engine = TestEngineBuilder::new().build().unwrap();
        let config = Config {
            scheduler_concurrency: 1024,
            scheduler_worker_pool_size: 1,
            scheduler_pending_write_threshold: ReadableSize(100 * 1024 * 1024),
            enable_async_apply_prewrite: false,
            ..Default::default()
        };
        (
            Scheduler::new(
                engine.clone(),
                MockLockManager::new(),
                ConcurrencyManager::new(1.into()),
                &config,
                DynamicConfigs {
                    pipelined_pessimistic_lock: Arc::new(AtomicBool::new(true)),
                    in_memory_pessimistic_lock: Arc::new(AtomicBool::new(false)),
                    wake_up_delay_duration_ms: Arc::new(AtomicU64::new(0)),
                },
                Arc::new(FlowController::NoLimit),
                DummyReporter,
                ResourceTagFactory::new_for_test(),
                Arc::new(QuotaLimiter::default()),
                latest_feature_gate(),
                new_read_pool_handle(engine.clone()),
                None,
            ),
            engine,
        )
    }

    fn new_read_pool_handle<E: Engine>(engine: E) -> ReadPoolHandle {
        let read_pool_cfg = UnifiedReadPoolConfig::default();
        build_tokio_pool(&read_pool_cfg, DummyReporter, engine.clone()).handle()
    }

    #[test]
    fn test_command_latches() {
        let mut temp_map = HashMap::default();
        temp_map.insert(10.into(), 20.into());
        let readonly_cmds: Vec<Command> = vec![
            commands::ResolveLockReadPhase::new(temp_map.clone(), None, Context::default()).into(),
            commands::MvccByKey::new(Key::from_raw(b"k"), Context::default()).into(),
            commands::MvccByStartTs::new(25.into(), Context::default()).into(),
        ];
        let write_cmds: Vec<Command> = vec![
            commands::Prewrite::with_defaults(
                vec![Mutation::make_put(Key::from_raw(b"k"), b"v".to_vec())],
                b"k".to_vec(),
                10.into(),
            )
            .into(),
            commands::AcquirePessimisticLock::new(
                vec![(Key::from_raw(b"k"), false)],
                b"k".to_vec(),
                10.into(),
                0,
                false,
                TimeStamp::default(),
                Some(WaitTimeout::Default),
                false,
                TimeStamp::default(),
                false,
                false,
                false,
                Context::default(),
            )
            .into(),
            commands::Commit::new(
                vec![Key::from_raw(b"k")],
                10.into(),
                20.into(),
                false,
                false,
                Context::default(),
            )
            .into(),
            commands::Cleanup::new(
                Key::from_raw(b"k"),
                10.into(),
                20.into(),
                Context::default(),
            )
            .into(),
            commands::Rollback::new(
                vec![Key::from_raw(b"k")],
                10.into(),
                false,
                Context::default(),
            )
            .into(),
            commands::PessimisticRollback::new(
                vec![Key::from_raw(b"k")],
                10.into(),
                20.into(),
                None,
                Context::default(),
            )
            .into(),
            commands::ResolveLock::new(
                temp_map,
                None,
                vec![(
                    Key::from_raw(b"k"),
                    mvcc::Lock::new(
                        mvcc::LockType::Put,
                        b"k".to_vec(),
                        10.into(),
                        20,
                        None,
                        TimeStamp::zero(),
                        0,
                        TimeStamp::zero(),
                    ),
                )],
                Default::default(),
                Context::default(),
            )
            .into(),
            commands::ResolveLockLite::new(
                10.into(),
                TimeStamp::zero(),
                vec![Key::from_raw(b"k")],
                false,
                Context::default(),
            )
            .into(),
            commands::TxnHeartBeat::new(
                Key::from_raw(b"k"),
                10.into(),
                100,
                false,
                Context::default(),
            )
            .into(),
        ];

        let latches = Latches::new(1024);
        let write_locks: Vec<Lock> = write_cmds
            .into_iter()
            .enumerate()
            .map(|(id, cmd)| {
                let mut lock = cmd.gen_lock();
                assert_eq!(latches.acquire(&mut lock, id as u64), id == 0);
                lock
            })
            .collect();

        for (id, cmd) in readonly_cmds.iter().enumerate() {
            let mut lock = cmd.gen_lock();
            assert!(latches.acquire(&mut lock, id as u64));
        }

        // acquire/release locks one by one.
        let max_id = write_locks.len() as u64 - 1;
        for (id, mut lock) in write_locks.into_iter().enumerate() {
            let id = id as u64;
            if id != 0 {
                assert!(latches.acquire(&mut lock, id));
            }
            let unlocked = latches.release(&lock, id, None);
            if id == max_id {
                assert!(unlocked.is_empty());
            } else {
                assert_eq!(unlocked, vec![id + 1]);
            }
        }
    }

    #[test]
    fn test_acquire_latch_deadline() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let config = Config {
            scheduler_concurrency: 1024,
            scheduler_worker_pool_size: 1,
            scheduler_pending_write_threshold: ReadableSize(100 * 1024 * 1024),
            enable_async_apply_prewrite: false,
            ..Default::default()
        };
        let read_pool = new_read_pool_handle(engine.clone());
        let scheduler = Scheduler::new(
            engine,
            MockLockManager::new(),
            ConcurrencyManager::new(1.into()),
            &config,
            DynamicConfigs {
                pipelined_pessimistic_lock: Arc::new(AtomicBool::new(true)),
                in_memory_pessimistic_lock: Arc::new(AtomicBool::new(false)),
                wake_up_delay_duration_ms: Arc::new(AtomicU64::new(0)),
            },
            Arc::new(FlowController::NoLimit),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            latest_feature_gate(),
            read_pool,
            None,
        );

        let mut lock = Lock::new(0, &[Key::from_raw(b"b")], None);
        let cid = scheduler.inner.gen_id();
        assert!(scheduler.inner.latches.acquire(&mut lock, cid));

        let mut req = BatchRollbackRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());

        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));

        // The task waits for 200ms until it acquires the latch, but the execution
        // time limit is 100ms. Before the latch is released, it should return
        // DeadlineExceeded error.
        thread::sleep(Duration::from_millis(200));
        assert!(matches!(
            block_on(f).unwrap(),
            Err(StorageError(box StorageErrorInner::DeadlineExceeded))
        ));
        scheduler.release_latches(lock, cid, None);

        // A new request should not be blocked.
        let mut req = BatchRollbackRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());
        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        block_on(f).unwrap().unwrap();
    }

    /// When all latches are acquired, the command should be executed directly.
    /// When any latch is not acquired, the command should be prechecked.
    #[test]
    fn test_schedule_command_with_fail_fast_mode() {
        let (scheduler, engine) = new_test_scheduler();

        // req can acquire all latches, so it should be executed directly.
        let mut req = BatchRollbackRequest::default();
        req.mut_context().max_execution_duration_ms = 10000;
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());
        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        // It must be executed (and succeed).
        block_on(f).unwrap().unwrap();

        // Acquire the latch, so that next command(req2) can't require all latches.
        let mut lock = Lock::new(0, &[Key::from_raw(b"d")], None);
        let cid = scheduler.inner.gen_id();
        assert!(scheduler.inner.latches.acquire(&mut lock, cid));

        engine.trigger_not_leader();

        // req2 can't acquire all latches, req2 will be prechecked.
        let mut req2 = BatchRollbackRequest::default();
        req2.mut_context().max_execution_duration_ms = 10000;
        req2.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"d".to_vec()].into());
        let cmd2: TypedCommand<()> = req2.into();
        let (cb2, f2) = paired_future_callback();
        scheduler.run_cmd(cmd2.cmd, StorageCallback::Boolean(cb2));

        // Precheck should return NotLeader error.
        assert!(matches!(
            block_on(f2).unwrap(),
            Err(StorageError(box StorageErrorInner::Kv(KvError(
                box KvErrorInner::Request(ref e),
            )))) if e.has_not_leader(),
        ));
        // The task context should be owned, and it's cb should be taken.
        let cid2 = cid + 1; // Hack: get the cid of req2.
        let mut task_slot = scheduler.inner.get_task_slot(cid2);
        let tctx = task_slot.get_mut(&cid2).unwrap();
        assert!(!tctx.try_own());
        assert!(tctx.cb.is_none());
    }

    #[test]
    fn test_pool_available_deadline() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let config = Config {
            scheduler_concurrency: 1024,
            scheduler_worker_pool_size: 1,
            scheduler_pending_write_threshold: ReadableSize(100 * 1024 * 1024),
            enable_async_apply_prewrite: false,
            ..Default::default()
        };
        let read_pool = new_read_pool_handle(engine.clone());
        let scheduler = Scheduler::new(
            engine,
            MockLockManager::new(),
            ConcurrencyManager::new(1.into()),
            &config,
            DynamicConfigs {
                pipelined_pessimistic_lock: Arc::new(AtomicBool::new(true)),
                in_memory_pessimistic_lock: Arc::new(AtomicBool::new(false)),
                wake_up_delay_duration_ms: Arc::new(AtomicU64::new(0)),
            },
            Arc::new(FlowController::NoLimit),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            latest_feature_gate(),
            read_pool,
            None,
        );

        // Spawn a task that sleeps for 500ms to occupy the pool. The next request
        // cannot run within 500ms.
        scheduler
            .inner
            .worker_pool
            .spawn(
                async { thread::sleep(Duration::from_millis(500)) },
                CommandPri::Normal,
                1,
            )
            .unwrap();

        let mut req = BatchRollbackRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());

        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));

        // But the max execution duration is 100ms, so the deadline is exceeded.
        assert!(matches!(
            block_on(f).unwrap(),
            Err(StorageError(box StorageErrorInner::DeadlineExceeded))
        ));

        // A new request should not be blocked.
        let mut req = BatchRollbackRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());
        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        block_on(f).unwrap().unwrap();
    }

    #[test]
    fn test_flow_control_trottle_deadline() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let config = Config {
            scheduler_concurrency: 1024,
            scheduler_worker_pool_size: 1,
            scheduler_pending_write_threshold: ReadableSize(100 * 1024 * 1024),
            enable_async_apply_prewrite: false,
            ..Default::default()
        };
        let read_pool = new_read_pool_handle(engine.clone());
        let scheduler = Scheduler::new(
            engine,
            MockLockManager::new(),
            ConcurrencyManager::new(1.into()),
            &config,
            DynamicConfigs {
                pipelined_pessimistic_lock: Arc::new(AtomicBool::new(true)),
                in_memory_pessimistic_lock: Arc::new(AtomicBool::new(false)),
                wake_up_delay_duration_ms: Arc::new(AtomicU64::new(0)),
            },
            Arc::new(FlowController::NoLimit),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            latest_feature_gate(),
            read_pool,
            None,
        );

        let mut req = CheckTxnStatusRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_primary_key(b"a".to_vec());
        req.set_lock_ts(10);
        req.set_rollback_if_not_exist(true);

        let cmd: TypedCommand<TxnStatus> = req.into();
        let (cb, f) = paired_future_callback();

        scheduler.inner.flow_controller.enable(true);
        scheduler.inner.flow_controller.set_speed_limit(0, 1.0);
        scheduler.run_cmd(cmd.cmd, StorageCallback::TxnStatus(cb));
        // The task waits for 200ms until it locks the control_mutex, but the execution
        // time limit is 100ms. Before the mutex is locked, it should return
        // DeadlineExceeded error.
        thread::sleep(Duration::from_millis(200));
        assert!(matches!(
            block_on(f).unwrap(),
            Err(StorageError(box StorageErrorInner::DeadlineExceeded))
        ));
        // should unconsume if the request fails
        assert_eq!(scheduler.inner.flow_controller.total_bytes_consumed(0), 0);

        // A new request should not be blocked without flow control.
        scheduler
            .inner
            .flow_controller
            .set_speed_limit(0, f64::INFINITY);
        let mut req = CheckTxnStatusRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_primary_key(b"a".to_vec());
        req.set_lock_ts(10);
        req.set_rollback_if_not_exist(true);

        let cmd: TypedCommand<TxnStatus> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::TxnStatus(cb));
        block_on(f).unwrap().unwrap();
    }

    #[test]
    fn test_accumulate_many_expired_commands() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let config = Config {
            scheduler_concurrency: 1024,
            scheduler_worker_pool_size: 1,
            scheduler_pending_write_threshold: ReadableSize(100 * 1024 * 1024),
            enable_async_apply_prewrite: false,
            ..Default::default()
        };
        let read_pool = new_read_pool_handle(engine.clone());
        let scheduler = Scheduler::new(
            engine,
            MockLockManager::new(),
            ConcurrencyManager::new(1.into()),
            &config,
            DynamicConfigs {
                pipelined_pessimistic_lock: Arc::new(AtomicBool::new(true)),
                in_memory_pessimistic_lock: Arc::new(AtomicBool::new(false)),
                wake_up_delay_duration_ms: Arc::new(AtomicU64::new(0)),
            },
            Arc::new(FlowController::NoLimit),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            latest_feature_gate(),
            read_pool,
            None,
        );

        let mut lock = Lock::new(0, &[Key::from_raw(b"b")], None);
        let cid = scheduler.inner.gen_id();
        assert!(scheduler.inner.latches.acquire(&mut lock, cid));

        // Push lots of requests in the queue.
        for _ in 0..65536 {
            let mut req = BatchRollbackRequest::default();
            req.mut_context().max_execution_duration_ms = 100;
            req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());

            let cmd: TypedCommand<()> = req.into();
            let (cb, _) = paired_future_callback();
            scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        }

        // The task waits for 200ms until it acquires the latch, but the execution
        // time limit is 100ms.
        thread::sleep(Duration::from_millis(200));

        // When releasing the lock, the queuing tasks should be all waken up without
        // stack overflow.
        scheduler.release_latches(lock, cid, None);

        // A new request should not be blocked.
        let mut req = BatchRollbackRequest::default();
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());
        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        block_on(f).unwrap().unwrap();
    }

    #[test]
    fn test_pessimistic_lock_mode() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let config = Config {
            scheduler_concurrency: 1024,
            scheduler_worker_pool_size: 1,
            scheduler_pending_write_threshold: ReadableSize(100 * 1024 * 1024),
            enable_async_apply_prewrite: false,
            ..Default::default()
        };
        let feature_gate = FeatureGate::default();
        feature_gate.set_version("6.0.0").unwrap();

        let read_pool = new_read_pool_handle(engine.clone());
        let scheduler = Scheduler::new(
            engine,
            MockLockManager::new(),
            ConcurrencyManager::new(1.into()),
            &config,
            DynamicConfigs {
                pipelined_pessimistic_lock: Arc::new(AtomicBool::new(false)),
                in_memory_pessimistic_lock: Arc::new(AtomicBool::new(false)),
                wake_up_delay_duration_ms: Arc::new(AtomicU64::new(0)),
            },
            Arc::new(FlowController::NoLimit),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            feature_gate.clone(),
            read_pool,
            None,
        );
        // Use sync mode if pipelined_pessimistic_lock is false.
        assert_eq!(scheduler.pessimistic_lock_mode(), PessimisticLockMode::Sync);
        // Use sync mode even when in_memory is true.
        scheduler
            .inner
            .in_memory_pessimistic_lock
            .store(true, Ordering::SeqCst);
        assert_eq!(scheduler.pessimistic_lock_mode(), PessimisticLockMode::Sync);
        // Mode is InMemory when both pipelined and in_memory is true.
        scheduler
            .inner
            .pipelined_pessimistic_lock
            .store(true, Ordering::SeqCst);
        assert_eq!(
            scheduler.pessimistic_lock_mode(),
            PessimisticLockMode::InMemory
        );
        // Test the feature gate. The feature should not work under 6.0.0.
        unsafe { feature_gate.reset_version("5.4.0").unwrap() };
        assert_eq!(
            scheduler.pessimistic_lock_mode(),
            PessimisticLockMode::Pipelined
        );
        feature_gate.set_version("6.0.0").unwrap();
        // Mode is Pipelined when only pipelined is true.
        scheduler
            .inner
            .in_memory_pessimistic_lock
            .store(false, Ordering::SeqCst);
        assert_eq!(
            scheduler.pessimistic_lock_mode(),
            PessimisticLockMode::Pipelined
        );
    }
}
