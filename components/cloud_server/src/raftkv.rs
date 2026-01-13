// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Cow,
    cell::UnsafeCell,
    fmt::{self, Debug, Display, Formatter},
    io::Error as IoError,
    mem,
    num::NonZeroU64,
    ops::Deref,
    pin::Pin,
    result,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    task::Poll,
    time::Duration,
};

use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use futures::{Future, Stream, StreamExt, task::AtomicWaker};
use kvproto::{
    errorpb,
    kvrpcpb::{Context, IsolationLevel},
    raft_cmdpb::{
        CmdType, CustomRequest, RaftCmdRequest, RaftCmdResponse, RaftRequestHeader, Request,
        Response,
    },
};
use raft::{
    StateRole,
    eraftpb::{self, MessageType},
};
use raftstore::coprocessor::{
    Coprocessor, CoprocessorHost, ReadIndexObserver, dispatcher::BoxReadIndexObserver,
};
use rfstore::{
    Error as RaftServerError, RaftStoreRouter, store,
    store::{
        Callback as StoreCallback, CustomBuilder, LocalReader, ReadIndexContext, ReadResponse,
        RegionSnapshot, WriteResponse, rlog,
    },
};
use thiserror::Error;
use tikv::{
    server::metrics::*,
    storage::{
        self,
        kv::{
            self, Engine, Error as KvError, ErrorInner as KvErrorInner, Modify, SnapContext,
            WriteData,
        },
    },
};
use tikv_kv::{ExtraRegionOverride, OnAppliedCb, WriteEvent};
use tikv_util::{
    callback::must_call, codec::number::NumberEncoder, future::paired_must_called_future_callback,
    time::Instant,
};
use txn_types::{Key, Lock, LockType, ReqType, WriteBatchFlags, WriteRef, WriteType};

#[derive(Debug, Error)]
pub enum Error {
    #[error("{}", .0.get_message())]
    RequestFailed(Box<errorpb::Error>),

    #[error("{0}")]
    Io(#[from] IoError),

    #[error("{0}")]
    Server(#[from] RaftServerError),

    #[error("{0}")]
    InvalidResponse(String),

    #[error("{0}")]
    InvalidRequest(String),

    #[error("{0}")]
    Undetermined(String),

    #[error("timeout after {0:?}")]
    Timeout(Duration),
}

fn get_status_kind_from_engine_error(e: &kv::Error) -> RequestStatusKind {
    match *e {
        KvError(box KvErrorInner::Request(ref header)) => {
            RequestStatusKind::from(storage::get_error_kind_from_header(header))
        }
        KvError(box KvErrorInner::KeyIsLocked(_)) => {
            RequestStatusKind::err_leader_memory_lock_check
        }
        KvError(box KvErrorInner::Timeout(_)) => RequestStatusKind::err_timeout,
        KvError(box KvErrorInner::EmptyRequest) => RequestStatusKind::err_empty_request,
        KvError(box KvErrorInner::Undetermined(_)) => RequestStatusKind::err_undetermind,
        KvError(box KvErrorInner::Other(_)) => RequestStatusKind::err_other,
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<Error> for kv::Error {
    fn from(e: Error) -> kv::Error {
        match e {
            Error::RequestFailed(e) => KvError::from(KvErrorInner::Request(*e)),
            Error::Undetermined(e) => KvError::from(KvErrorInner::Undetermined(e)),
            Error::Server(e) => e.into(),
            e => box_err!(e),
        }
    }
}

#[inline]
pub fn new_request_header(
    ctx: &Context,
    extra_snap_override: Option<&ExtraRegionOverride>,
) -> RaftRequestHeader {
    let mut header = RaftRequestHeader::default();
    match extra_snap_override {
        Some(&ExtraRegionOverride {
            region_id,
            ref region_epoch,
            ref peer,
            check_term,
        }) => {
            header.set_region_id(region_id);
            header.set_peer(peer.clone());
            header.set_region_epoch(region_epoch.clone());
            if let Some(term) = check_term {
                header.set_term(term);
            }
        }
        _ => {
            header.set_region_id(ctx.get_region_id());
            header.set_peer(ctx.get_peer().clone());
            header.set_region_epoch(ctx.get_region_epoch().clone());
            if ctx.get_term() != 0 {
                header.set_term(ctx.get_term());
            }
        }
    }
    header.set_sync_log(ctx.get_sync_log());
    header.set_replica_read(ctx.get_replica_read());
    header
}

pub fn drop_snapshot_callback<T>() -> kv::Result<T> {
    let bt = backtrace::Backtrace::new();
    warn!("async snapshot callback is dropped"; "backtrace" => ?bt);
    let mut err = errorpb::Error::default();
    err.set_message("async snapshot callback is dropped".to_string());
    Err(kv::Error::from(kv::ErrorInner::Request(err)))
}

struct WriteResCore {
    ev: AtomicU8,
    result: UnsafeCell<Option<kv::Result<()>>>,
    wake: AtomicWaker,
}

struct WriteResSub {
    notified_ev: u8,
    core: Arc<WriteResCore>,
}

unsafe impl Send for WriteResSub {}

impl Stream for WriteResSub {
    type Item = WriteEvent;

    #[inline]
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut s = self.as_mut();
        let mut cur_ev = s.core.ev.load(Ordering::Acquire);
        if cur_ev == s.notified_ev {
            s.core.wake.register(cx.waker());
            cur_ev = s.core.ev.load(Ordering::Acquire);
            if cur_ev == s.notified_ev {
                return Poll::Pending;
            }
        }
        s.notified_ev = cur_ev;
        match cur_ev {
            WriteEvent::EVENT_PROPOSED => Poll::Ready(Some(WriteEvent::Proposed)),
            WriteEvent::EVENT_COMMITTED => Poll::Ready(Some(WriteEvent::Committed)),
            u8::MAX => {
                let result = unsafe { (*s.core.result.get()).take().unwrap() };
                Poll::Ready(Some(WriteEvent::Finished(result)))
            }
            e => panic!("unexpected event {}", e),
        }
    }
}

#[derive(Clone)]
struct WriteResFeed {
    core: Arc<WriteResCore>,
}

unsafe impl Send for WriteResFeed {}

impl WriteResFeed {
    fn pair() -> (Self, WriteResSub) {
        let core = Arc::new(WriteResCore {
            ev: AtomicU8::new(0),
            result: UnsafeCell::new(None),
            wake: AtomicWaker::new(),
        });
        (
            Self { core: core.clone() },
            WriteResSub {
                notified_ev: 0,
                core,
            },
        )
    }

    fn notify_proposed(&self) {
        self.core
            .ev
            .store(WriteEvent::EVENT_PROPOSED, Ordering::Release);
        self.core.wake.wake();
    }

    fn notify_committed(&self) {
        self.core
            .ev
            .store(WriteEvent::EVENT_COMMITTED, Ordering::Release);
        self.core.wake.wake();
    }

    fn notify(&self, result: kv::Result<()>) {
        unsafe {
            (*self.core.result.get()) = Some(result);
        }
        self.core.ev.store(u8::MAX, Ordering::Release);
        self.core.wake.wake();
    }
}

/// `RaftKv` is a storage engine base on `RaftStore`.
#[derive(Clone)]
pub struct RaftKv {
    reader: Arc<LocalReader>,
}

impl Deref for RaftKv {
    type Target = LocalReader;

    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

pub enum CmdRes {
    Resp(Vec<Response>),
    Snap(RegionSnapshot),
}

fn check_raft_cmd_response(resp: &mut RaftCmdResponse) -> Result<()> {
    if resp.get_header().has_error() {
        let mut err = resp.take_header().take_error();
        if err.get_message() == ASYNC_WRITE_CALLBACK_DROPPED_ERR_MSG {
            return Err(Error::Undetermined(err.take_message()));
        }
        return Err(Error::RequestFailed(Box::from(err)));
    }

    Ok(())
}

fn on_write_result(mut write_resp: WriteResponse) -> Result<CmdRes> {
    check_raft_cmd_response(&mut write_resp.response)?;
    let resps = write_resp.response.take_responses();
    Ok(CmdRes::Resp(resps.into()))
}

fn on_read_result(mut read_resp: ReadResponse) -> Result<CmdRes> {
    check_raft_cmd_response(&mut read_resp.response)?;
    let resps = read_resp.response.take_responses();
    if let Some(mut snapshot) = read_resp.snapshot {
        snapshot.term = NonZeroU64::new(read_resp.response.get_header().get_current_term());
        snapshot.txn_extra_op = read_resp.txn_extra_op;
        Ok(CmdRes::Snap(snapshot))
    } else {
        Ok(CmdRes::Resp(resps.into()))
    }
}

impl RaftKv {
    /// Create a RaftKv using specified configuration.
    pub fn new(reader: LocalReader) -> RaftKv {
        RaftKv {
            reader: Arc::new(reader),
        }
    }

    fn new_request_header(&self, ctx: &Context) -> RaftRequestHeader {
        let mut header = RaftRequestHeader::default();
        header.set_region_id(ctx.get_region_id());
        header.set_peer(ctx.get_peer().clone());
        header.set_region_epoch(ctx.get_region_epoch().clone());
        if ctx.get_term() != 0 {
            header.set_term(ctx.get_term());
        }
        header.set_sync_log(ctx.get_sync_log());
        header.set_replica_read(ctx.get_replica_read());
        header
    }
}

fn invalid_resp_type(exp: CmdType, act: CmdType) -> Error {
    Error::InvalidResponse(format!(
        "cmd type not match, want {:?}, got {:?}!",
        exp, act
    ))
}

pub const ASYNC_WRITE_CALLBACK_DROPPED_ERR_MSG: &str = "async write on_applied callback is dropped";

pub fn async_write_callback_dropped_err() -> errorpb::Error {
    let mut err = errorpb::Error::default();
    err.set_message(ASYNC_WRITE_CALLBACK_DROPPED_ERR_MSG.to_string());
    err
}

pub fn drop_on_applied_callback() -> WriteResponse {
    let bt = backtrace::Backtrace::new();
    error!("async write on_applied callback is dropped"; "backtrace" => ?bt);
    let mut write_resp = WriteResponse {
        response: Default::default(),
        key_errors: Default::default(),
    };
    write_resp
        .response
        .mut_header()
        .set_error(async_write_callback_dropped_err());
    write_resp
}

impl Display for RaftKv {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl Debug for RaftKv {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

#[allow(dead_code)]
impl Engine for RaftKv {
    type Snap = RegionSnapshot;
    type Local = kvengine::Engine;

    fn kv_engine(&self) -> Option<kvengine::Engine> {
        Some(self.kv_engine.clone())
    }

    fn modify_on_kv_engine(&self, _modifies: HashMap<u64, Vec<Modify>>) -> kv::Result<()> {
        panic!();
    }

    type WriteRes = impl Stream<Item = WriteEvent> + Send + Unpin;
    fn async_write(
        &self,
        ctx: &Context,
        mut batch: WriteData,
        subscribed: u8,
        on_applied: Option<OnAppliedCb>,
    ) -> Self::WriteRes {
        let mut res = (|| {
            fail_point!("raftkv_async_write");
            if batch.modifies.is_empty() && batch.txn_file.is_none() {
                return Err(KvError::from(KvErrorInner::EmptyRequest));
            }
            Ok(())
        })();

        ASYNC_REQUESTS_COUNTER_VEC.write.all.inc();
        let begin_instant = Instant::now_coarse();

        if res.is_ok() {
            // If rid is some, only the specified region reports error.
            // If rid is None, all regions report error.
            res = (|| {
                fail_point!("raftkv_early_error_report", |rid| {
                    let region_id = ctx.get_region_id();
                    rid.and_then(|rid| {
                        let rid: u64 = rid.parse().unwrap();
                        if rid == region_id { None } else { Some(()) }
                    })
                    .ok_or_else(|| RaftServerError::RegionNotFound(region_id, None).into())
                });
                Ok(())
            })();
        }

        let req = modifies_to_requests(ctx, &mut batch);
        let txn_extra = batch.extra;
        let mut header = self.new_request_header(ctx);
        if txn_extra.one_pc {
            header.set_flags(WriteBatchFlags::ONE_PC.bits());
        }

        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_custom_request(req);

        self.schedule_txn_extra(txn_extra);

        let (tx, rx) = WriteResFeed::pair();
        if res.is_ok() {
            let proposed_cb = if !WriteEvent::subscribed_proposed(subscribed) {
                None
            } else {
                let tx = tx.clone();
                Some(Box::new(move || tx.notify_proposed()) as store::ExtCallback)
            };
            let committed_cb = if !WriteEvent::subscribed_committed(subscribed) {
                None
            } else {
                let tx = tx.clone();
                Some(Box::new(move || tx.notify_committed()) as store::ExtCallback)
            };
            let applied_tx = tx.clone();
            let applied_cb = must_call(
                Box::new(move |resp: WriteResponse| {
                    fail_point!("applied_cb_return_undetermined_err", |_| {
                        applied_tx.notify(Err(kv::Error::from(Error::Undetermined(
                            ASYNC_WRITE_CALLBACK_DROPPED_ERR_MSG.to_string(),
                        ))));
                    });
                    let mut res = match on_write_result(resp) {
                        Ok(CmdRes::Resp(_)) => {
                            fail_point!("raftkv_async_write_finish");
                            Ok(())
                        }
                        Ok(CmdRes::Snap(_)) => {
                            Err(box_err!("unexpect snapshot, should mutate instead."))
                        }
                        Err(e) => Err(kv::Error::from(e)),
                    };
                    if let Some(cb) = on_applied {
                        cb(&mut res);
                    }
                    applied_tx.notify(res);
                }),
                drop_on_applied_callback,
            );

            let cb = StoreCallback::write_ext(applied_cb, proposed_cb, committed_cb);

            // TODO: do we need to support deadline?
            self.router.send_command(cmd, cb);
        } else {
            // Note that `on_applied` is not called in this case. We send a message to the
            // channel here to notify the caller that the writing ended, like
            // how the `applied_cb` does.
            tx.notify(res);
        }
        rx.inspect(move |ev| {
            let WriteEvent::Finished(res) = ev else {
                return;
            };
            match res {
                Ok(()) => {
                    ASYNC_REQUESTS_COUNTER_VEC.write.success.inc();
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .write
                        .observe(begin_instant.saturating_elapsed_secs());
                }
                Err(e) => {
                    let status_kind = get_status_kind_from_engine_error(e);
                    ASYNC_REQUESTS_COUNTER_VEC.write.get(status_kind).inc();
                }
            }
        })
    }

    type SnapshotRes = impl Future<Output = kv::Result<Self::Snap>> + Send;
    fn async_snapshot(&mut self, mut ctx: SnapContext<'_>) -> Self::SnapshotRes {
        let res: kv::Result<()> = (|| {
            fail_point!("raftkv_async_snapshot_err", |_| {
                Err(box_err!("injected error for async_snapshot"))
            });
            Ok(())
        })();

        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        if !ctx.key_ranges.is_empty() && ctx.start_ts.is_some_and(|ts| !ts.is_zero()) {
            req.mut_read_index()
                .set_start_ts(ctx.start_ts.as_ref().unwrap().into_inner());
            req.mut_read_index()
                .set_key_ranges(mem::take(&mut ctx.key_ranges).into());
        }
        ASYNC_REQUESTS_COUNTER_VEC.snapshot.all.inc();
        let begin_instant = Instant::now_coarse();
        let (cb, f) = paired_must_called_future_callback(drop_snapshot_callback);

        let mut header = new_request_header(ctx.pb_ctx, ctx.extra_region_override.as_ref());
        let mut flags = 0;
        if ctx.pb_ctx.get_stale_read() && ctx.start_ts.is_none_or(|ts| !ts.is_zero()) {
            let mut data = [0u8; 8];
            (&mut data[..])
                .encode_u64(ctx.start_ts.unwrap_or_default().into_inner())
                .unwrap();
            flags |= WriteBatchFlags::STALE_READ.bits();
            header.set_flag_data(data.into());
        }
        header.set_flags(flags);

        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(vec![req].into());
        if res.is_ok() {
            self.read(
                ctx.read_id,
                cmd,
                StoreCallback::Read(Box::new(move |resp| {
                    cb(on_read_result(resp).map_err(Error::into));
                })),
            );
        }
        async move {
            let res = match res {
                Ok(()) => f
                    .await
                    .map_err(|_| tikv_kv::ErrorInner::Other(box_err!("canceled")))?,
                Err(e) => Err(e),
            };
            match res {
                Ok(CmdRes::Resp(mut r)) => {
                    let e = if r
                        .first()
                        .map(|resp| resp.get_read_index().has_locked())
                        .unwrap_or(false)
                    {
                        let locked = r[0].take_read_index().take_locked();
                        KvError::from(KvErrorInner::KeyIsLocked(locked))
                    } else {
                        invalid_resp_type(CmdType::Snap, r[0].get_cmd_type()).into()
                    };
                    Err(e)
                }
                Ok(CmdRes::Snap(s)) => {
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .snapshot
                        .observe(begin_instant.saturating_elapsed_secs());
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.success.inc();
                    Ok(s)
                }
                Err(e) => {
                    let status_kind = get_status_kind_from_engine_error(&e);
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.get(status_kind).inc();
                    Err(e)
                }
            }
        }
    }

    fn get_kvengine(&self) -> Option<kvengine::Engine> {
        self.kv_engine()
    }
}

#[derive(Clone)]
pub struct ReplicaReadLockChecker {
    concurrency_manager: ConcurrencyManager,
}

impl ReplicaReadLockChecker {
    pub fn new(concurrency_manager: ConcurrencyManager) -> Self {
        ReplicaReadLockChecker {
            concurrency_manager,
        }
    }

    pub fn register(self, host: &mut CoprocessorHost) {
        host.registry
            .register_read_index_observer(1, BoxReadIndexObserver::new(self));
    }
}

impl Coprocessor for ReplicaReadLockChecker {}

impl ReadIndexObserver for ReplicaReadLockChecker {
    fn on_step(&self, msg: &mut eraftpb::Message, role: StateRole) {
        // Only check and return result if the current peer is a leader.
        // If it's not a leader, the read index request will be redirected to the leader
        // later.
        if msg.get_msg_type() != MessageType::MsgReadIndex || role != StateRole::Leader {
            return;
        }
        assert_eq!(msg.get_entries().len(), 1);
        let mut rctx = ReadIndexContext::parse(msg.get_entries()[0].get_data()).unwrap();
        if let Some(mut request) = rctx.request.take() {
            let begin_instant = Instant::now();

            let start_ts = request.get_start_ts().into();
            self.concurrency_manager.update_max_ts(start_ts);
            for range in request.mut_key_ranges().iter_mut() {
                let key_bound = |key: Vec<u8>| {
                    if key.is_empty() {
                        None
                    } else {
                        Some(txn_types::Key::from_encoded(key))
                    }
                };
                let start_key = key_bound(range.take_start_key());
                let end_key = key_bound(range.take_end_key());
                let res = self.concurrency_manager.read_range_check(
                    start_key.as_ref(),
                    end_key.as_ref(),
                    |key, lock| {
                        txn_types::Lock::check_ts_conflict(
                            Cow::Borrowed(lock),
                            key,
                            start_ts,
                            &Default::default(),
                            IsolationLevel::Si,
                        )
                    },
                );
                if let Err(txn_types::Error(box txn_types::ErrorInner::KeyIsLocked(lock))) = res {
                    rctx.locked = Some(lock);
                    REPLICA_READ_LOCK_CHECK_HISTOGRAM_VEC_STATIC
                        .locked
                        .observe(begin_instant.saturating_elapsed().as_secs_f64());
                } else {
                    REPLICA_READ_LOCK_CHECK_HISTOGRAM_VEC_STATIC
                        .unlocked
                        .observe(begin_instant.saturating_elapsed().as_secs_f64());
                }
            }
            msg.mut_entries()[0].set_data(rctx.to_bytes().into());
        }
    }
}

/// Reconstruct transaction operations from separated modifications.
///
/// pessimistic_lock
/// modify::put lock(pessimistic)
///
/// prewrite
/// optional modify::put cf_default
/// | modify::put cf_lock
///
/// 1pc
/// optional modify::put cf_default
/// optional modify::del cf_lock
/// | modify::put cf_write:put
/// | modify::put cf_write:del
///
/// commit
/// modify:put cf_write + modify::del cf_lock
///
/// rollback
/// modify:del cf_lock
/// | modify::put cf_write::put overlapped + modify:del cf_lock +
///   optional modify::del cf_write(collapse rollback)
/// | modify::put cf_write::rollback +
///   optional modify::del cf_write(collapse rollback)
/// | modify::put cf_write::rollback + modify:del cf_lock +
///   optional modify::del cf_write(collapse rollback)
/// | modify::put cf_lock(has rollback_ts) + modify:put cf_write:rollback +
///   optional modify::del cf_write(collapse rollback)
///
/// pessimistic_rollback
/// modify::del cf_lock(pessimistic)
///
/// check_txn_status
/// | pessimistic_lock(push min_commit_ts)
/// | prewrite(push min_commit_ts)
/// | pessimistic_rollback
/// | rollback
///
/// CheckSecondaryLocks
/// rollback
///
/// ResolveLock
/// commit and rollback
///
/// Heartbeat
/// modify::put cf_lock(optimistc or pessimistic)
pub fn modifies_to_requests(_ctx: &Context, data: &mut WriteData) -> CustomRequest {
    let builder = &mut rlog::CustomBuilder::new();
    let modifies = std::mem::take(&mut data.modifies);
    if let Some(txn_file) = data.txn_file.take() {
        builder.set_txn_file(&txn_file.txn_file_ref);
        return builder.build();
    }
    if data.extra.one_pc {
        build_one_pc(builder, modifies);
    } else {
        match data.extra.req_type {
            ReqType::PessimisticLock => build_pessimistic_lock(builder, modifies),
            ReqType::Prewrite => build_prewrite(builder, modifies),
            ReqType::Commit => build_commit(builder, modifies),
            ReqType::PessimisticRollback => build_pessimistic_rollback(builder, modifies),
            ReqType::CheckTxnStatus => build_check_txn_status(builder, modifies),
            ReqType::CheckSecondaryLocks | ReqType::Rollback => build_rollback(builder, modifies),
            ReqType::ResolveLock => build_resolve_lock(builder, modifies),
            ReqType::Heartbeat => build_heartbeat(builder, modifies),
            ReqType::Noop => unreachable!("modifies: {:?}", modifies),
        }
    }
    builder.build()
}

fn build_pessimistic_lock(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    builder.set_type(rlog::CustomRaftLogType::PessimisticLock);
    for m in modifies {
        match m {
            Modify::PessimisticLock(key, lock) => {
                let raw_key = key.into_raw().unwrap();
                let val = lock.into_lock().to_bytes();
                builder.append_lock(&raw_key, &val);
            }
            Modify::Put(CF_LOCK, key, val) => {
                let raw_key = key.into_raw().unwrap();
                builder.append_lock(&raw_key, &val);
            }
            _ => unreachable!("unexpected modify: {:?}", m),
        }
    }
}

fn build_prewrite(builder: &mut CustomBuilder, mut modifies: Vec<Modify>) {
    builder.set_type(rlog::CustomRaftLogType::Prewrite);

    let mut default_vals = modifies
        .iter_mut()
        .filter_map(|m| {
            if let Modify::Put(CF_DEFAULT, k, v) = m {
                Some((k.to_raw().unwrap(), mem::take(v)))
            } else {
                None
            }
        })
        .collect::<HashMap<_, _>>();

    for m in modifies {
        match m {
            Modify::Put(CF_LOCK, key, mut val) => {
                let raw_key = key.into_raw().unwrap();
                if let Some(default_val) = default_vals.remove(&raw_key) {
                    let mut lock = Lock::parse(&val).unwrap();
                    lock.short_value = Some(default_val);
                    val = lock.to_bytes();
                }
                builder.append_lock(&raw_key, &val);
            }
            Modify::Put(CF_DEFAULT, ..) => {}
            _ => unreachable!("unexpected modify {:?}", m),
        }
    }
}

fn build_commit(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    builder.set_type(rlog::CustomRaftLogType::Commit);
    for m in modifies {
        match m {
            Modify::Put(CF_WRITE, key, _) => {
                let commit_ts = key.decode_ts().unwrap();
                let raw_key = key.into_raw().unwrap();
                builder.append_commit(&raw_key, commit_ts.into_inner());
                // value will be fetched during applying.
            }
            Modify::Delete(CF_LOCK, _) => {
                // lock will be deleted during applying.
            }
            _ => unreachable!("unexpected modify: {:?}", m),
        }
    }
}

fn build_one_pc(builder: &mut CustomBuilder, mut modifies: Vec<Modify>) {
    builder.set_type(rlog::CustomRaftLogType::OnePc);

    let deleted_lock = modifies
        .iter_mut()
        .filter_map(|m| {
            if let Modify::Delete(CF_LOCK, k) = m {
                Some(mem::replace(k, Key::from_encoded(vec![])))
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();
    let mut default_vals = modifies
        .iter_mut()
        .filter_map(|m| {
            if let Modify::Put(CF_DEFAULT, k, v) = m {
                Some((k.to_raw().unwrap(), mem::take(v)))
            } else {
                None
            }
        })
        .collect::<HashMap<_, _>>();

    for m in modifies {
        match m {
            Modify::Put(CF_WRITE, key, val) => {
                let commit_ts = key.decode_ts().unwrap().into_inner();
                let key = key.truncate_ts().unwrap();
                let del_lock = deleted_lock.contains(&key);
                let raw_key = key.into_raw().unwrap();
                let write = WriteRef::parse(&val).unwrap();
                let mut value = vec![];
                if write.write_type == WriteType::Put {
                    value = default_vals
                        .remove(&raw_key)
                        .unwrap_or_else(|| write.short_value.unwrap().to_vec());
                }
                let is_extra = write.write_type == WriteType::Lock;
                let start_ts = write.start_ts.into_inner();
                builder.append_one_pc(&raw_key, &value, is_extra, del_lock, start_ts, commit_ts);
            }
            Modify::Delete(CF_LOCK, _) | Modify::Put(CF_DEFAULT, ..) => {}
            _ => unreachable!("unexpected modify: {:?}", m),
        }
    }
}

fn build_pessimistic_rollback(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    builder.set_type(rlog::CustomRaftLogType::PessimisticRollback);
    for m in modifies {
        match m {
            Modify::Delete(CF_LOCK, key) => {
                let raw_key = key.into_raw().unwrap();
                builder.append_del_lock(&raw_key);
            }
            _ => unreachable!("unexpected modify: {:?}", m),
        }
    }
}

fn build_rollback(builder: &mut CustomBuilder, mut modifies: Vec<Modify>) {
    builder.set_type(rlog::CustomRaftLogType::Rollback);

    let mut deleted_locks = modifies
        .iter_mut()
        .filter_map(|m| {
            if let Modify::Delete(CF_LOCK, k) = m {
                Some(mem::replace(k, Key::from_encoded(vec![])))
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    for m in modifies {
        match m {
            Modify::Put(CF_WRITE, key, _) => {
                let start_ts = key.decode_ts().unwrap().into_inner();
                let key = key.truncate_ts().unwrap();
                let del_lock = deleted_locks.remove(&key);
                let raw_key = key.into_raw().unwrap();
                builder.append_rollback(&raw_key, start_ts, del_lock);
            }
            Modify::Delete(CF_LOCK, _) => {
                // Lock deletion is carried on putting write.
            }
            Modify::Put(CF_LOCK, ..) => {
                // The EXTRA_CF ensures no overlapped rollback and commit
                // record, so we ignore them.
            }
            Modify::Delete(CF_WRITE, _) => {
                // No need to collapse rollback write due to the EXTRA_CF.
            }
            _ => unreachable!("unexpected modify: {:?}", m),
        }
    }
    // Non-pessimistic lock in a pessimistic transaction doesn't check write and
    // rollback record during prewrite, so rollback record and lock may both
    // exist. In this case, we delete the lock only.
    for delete_only in deleted_locks {
        let raw_key = delete_only.into_raw().unwrap();
        builder.append_rollback(&raw_key, 0, true);
    }
}

fn build_check_txn_status(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    let tp = match &modifies[0] {
        Modify::Put(CF_LOCK, _, v) => match Lock::parse(v).unwrap().lock_type {
            LockType::Pessimistic => rlog::CustomRaftLogType::PessimisticLock,
            _ => rlog::CustomRaftLogType::Prewrite,
        },
        Modify::Delete(CF_LOCK, _) => {
            if modifies.len() == 1 {
                rlog::CustomRaftLogType::PessimisticRollback
            } else {
                rlog::CustomRaftLogType::Rollback
            }
        }
        Modify::Put(CF_WRITE, ..) | Modify::Delete(CF_WRITE, ..) => {
            rlog::CustomRaftLogType::Rollback
        }
        _ => unreachable!("unexpected modifies: {:?}", modifies),
    };
    match tp {
        rlog::CustomRaftLogType::PessimisticLock => build_pessimistic_lock(builder, modifies),
        rlog::CustomRaftLogType::Prewrite => build_prewrite(builder, modifies),
        rlog::CustomRaftLogType::PessimisticRollback => {
            build_pessimistic_rollback(builder, modifies)
        }
        rlog::CustomRaftLogType::Rollback => build_rollback(builder, modifies),
        _ => unreachable!(),
    };
}

fn build_resolve_lock(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    // Group modifies by key.
    let mut grouped = HashMap::<Vec<_>, Vec<_>>::default();
    for m in modifies {
        let k = m.key().to_raw().unwrap();
        grouped.entry(k).or_default().push(m);
    }
    for (_, modifies) in grouped {
        if modifies.len() == 2 {
            if let Modify::Put(CF_WRITE, _, v) = &modifies[0] {
                let write = WriteRef::parse(v).unwrap();
                if write.write_type != WriteType::Rollback {
                    assert!(matches!(&modifies[1], Modify::Delete(CF_LOCK, _)));
                    builder.append_type(rlog::CustomRaftLogType::Commit);
                    build_commit(builder, modifies);
                    continue;
                }
            }
        }
        builder.append_type(rlog::CustomRaftLogType::Rollback);
        build_rollback(builder, modifies);
    }
    builder.set_type(rlog::CustomRaftLogType::ResolveLock);
}

fn build_heartbeat(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    let tp = match &modifies[0] {
        Modify::Put(CF_LOCK, _, v) => match Lock::parse(v).unwrap().lock_type {
            LockType::Pessimistic => rlog::CustomRaftLogType::PessimisticLock,
            _ => rlog::CustomRaftLogType::Prewrite,
        },
        _ => unreachable!("unexpected modifies: {:?}", modifies),
    };
    match tp {
        rlog::CustomRaftLogType::PessimisticLock => build_pessimistic_lock(builder, modifies),
        rlog::CustomRaftLogType::Prewrite => build_prewrite(builder, modifies),
        _ => unreachable!(),
    };
}

#[cfg(test)]
mod tests {
    use std::{
        iter::FromIterator,
        sync::{Mutex, mpsc},
    };

    use kvproto::kvrpcpb::PrewriteRequestPessimisticAction::*;
    use tikv_kv::RocksEngine;
    use txn_types::SHORT_VALUE_MAX_LEN;
    use uuid::Uuid;

    use super::*;

    // This test ensures `ReplicaReadLockChecker` won't change UUID context of read
    // index.
    #[test]
    fn test_replica_read_lock_checker_for_single_uuid() {
        let cm = ConcurrencyManager::new(1.into());
        let checker = ReplicaReadLockChecker::new(cm);
        let mut m = eraftpb::Message::default();
        m.set_msg_type(MessageType::MsgReadIndex);
        let uuid = Uuid::new_v4();
        let mut e = eraftpb::Entry::default();
        e.set_data(uuid.as_bytes().to_vec().into());
        m.mut_entries().push(e);

        checker.on_step(&mut m, raft::StateRole::Leader);
        assert_eq!(m.get_entries()[0].get_data(), uuid.as_bytes());
    }

    // It saves the write data of the last request for testing custom raft log.
    #[derive(Clone)]
    struct MockEngine {
        base: RocksEngine,
        last_write_data: Arc<Mutex<Option<WriteData>>>,
    }

    impl MockEngine {
        fn new(base: RocksEngine) -> Self {
            Self {
                base,
                last_write_data: Arc::default(),
            }
        }

        fn take_last_write_data(&self) -> Option<WriteData> {
            self.last_write_data.lock().unwrap().take()
        }

        fn set_write_data(&self, data: &WriteData) {
            let clone = WriteData::new(data.modifies.clone(), data.extra.clone(), None);
            *self.last_write_data.lock().unwrap() = Some(clone);
        }
    }

    impl Engine for MockEngine {
        type Snap = <RocksEngine as Engine>::Snap;
        type Local = <RocksEngine as Engine>::Local;

        fn kv_engine(&self) -> Option<Self::Local> {
            self.base.kv_engine()
        }

        fn modify_on_kv_engine(&self, modifies: HashMap<u64, Vec<Modify>>) -> kv::Result<()> {
            self.base.modify_on_kv_engine(modifies)
        }

        type SnapshotRes = <RocksEngine as Engine>::SnapshotRes;
        fn async_snapshot(&mut self, ctx: SnapContext<'_>) -> Self::SnapshotRes {
            self.base.async_snapshot(ctx)
        }

        type WriteRes = <RocksEngine as Engine>::WriteRes;
        fn async_write(
            &self,
            ctx: &Context,
            batch: WriteData,
            subscribed: u8,
            on_applied: Option<OnAppliedCb>,
        ) -> Self::WriteRes {
            self.set_write_data(&batch);
            self.base.async_write(ctx, batch, subscribed, on_applied)
        }
    }

    #[test]
    fn test_custom_raft_log() {
        use kvproto::kvrpcpb::AssertionLevel;
        use tikv::storage::{
            TestStorageBuilderApiV1, kv::TestEngineBuilder, lock_manager::MockLockManager,
            test_util::expect_ok_callback, txn::commands,
        };
        use txn_types::Mutation;

        let engine = MockEngine::new(TestEngineBuilder::new().build().unwrap());
        let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(
            engine.clone(),
            MockLockManager::new(),
        )
        .build()
        .unwrap();
        let (tx, rx) = mpsc::channel();
        let get_modify_value = |modify: &_| match modify {
            Modify::Put(_, _, v) => v.clone(),
            Modify::PessimisticLock(_, lock) => lock.to_lock().to_bytes(),
            _ => unreachable!("unexpected modify: {:?}", modify),
        };

        let prewrite =
            |mutations, primary: &[_], ts: u64, secondaries, one_pc, max_commit_ts: u64| {
                storage
                    .sched_txn_command(
                        commands::Prewrite::new(
                            mutations,
                            primary.to_vec(),
                            ts.into(),
                            3000,
                            false,
                            0,
                            (ts + 1).into(),
                            max_commit_ts.into(),
                            secondaries,
                            one_pc,
                            AssertionLevel::Off,
                            vec![],
                            Context::default(),
                        ),
                        expect_ok_callback(tx.clone(), 1),
                    )
                    .unwrap();
                rx.recv().unwrap();
                engine.take_last_write_data().unwrap()
            };
        let pessimistic_prewrite =
            |mutations, primary: &[_], ts: u64, one_pc, max_commit_ts: u64| {
                storage
                    .sched_txn_command(
                        commands::PrewritePessimistic::new(
                            mutations,
                            primary.to_vec(),
                            ts.into(),
                            3000,
                            ts.into(),
                            0,
                            (ts + 1).into(),
                            max_commit_ts.into(),
                            None,
                            one_pc,
                            AssertionLevel::Off,
                            Context::default(),
                        ),
                        expect_ok_callback(tx.clone(), 1),
                    )
                    .unwrap();
                rx.recv().unwrap();
                engine.take_last_write_data().unwrap()
            };
        let pessimistic_lock = |keys: Vec<_>, primary: &[_], ts: u64| {
            storage
                .sched_txn_command(
                    commands::AcquirePessimisticLock::new(
                        keys.into_iter()
                            .map(|k| (Key::from_raw(k), false))
                            .collect(),
                        primary.to_vec(),
                        ts.into(),
                        3000,
                        false,
                        ts.into(),
                        None,
                        false,
                        (ts + 1).into(),
                        Default::default(),
                        false,
                        false,
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 1),
                )
                .unwrap();
            rx.recv().unwrap();
            engine.take_last_write_data().unwrap()
        };

        // Prewrite
        let mut data = prewrite(
            vec![
                Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec()),
                Mutation::make_put(Key::from_raw(b"k2"), b"v".repeat(SHORT_VALUE_MAX_LEN + 1)),
            ],
            b"k1",
            10,
            None,
            false,
            0,
        );
        let modifies = data.modifies.clone();
        assert_eq!(data.extra.req_type, ReqType::Prewrite);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::CustomRaftLogType::Prewrite);
        custom_log.iterate_lock(|k, v| match k {
            b"k1" => {
                assert_eq!(k, modifies[0].key().to_raw().unwrap());
                assert_eq!(v, &get_modify_value(&modifies[0]));
            }
            b"k2" => {
                assert_eq!(k, modifies[2].key().to_raw().unwrap());
                let default_val = get_modify_value(&modifies[1]);
                let mut lock = Lock::parse(&get_modify_value(&modifies[2])).unwrap();
                lock.short_value = Some(default_val);
                assert_eq!(v, lock.to_bytes());
            }
            _ => unreachable!(),
        });

        // Commit
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"k1"), Key::from_raw(b"k2")],
                    10.into(),
                    20.into(),
                    false,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut data = engine.take_last_write_data().unwrap();
        assert_eq!(data.extra.req_type, ReqType::Commit);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::CustomRaftLogType::Commit);
        let mut keys = [b"k1", b"k2"].iter();
        custom_log.iterate_commit(|k, ts| {
            assert_eq!(k, keys.next().unwrap().as_slice());
            assert_eq!(ts, 20);
        });
        assert!(keys.next().is_none());

        // 1PC
        let mut data = prewrite(
            vec![
                Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec()),
                Mutation::make_put(Key::from_raw(b"k2"), b"v".repeat(SHORT_VALUE_MAX_LEN + 1)),
            ],
            b"k1",
            30,
            None,
            true,
            40,
        );
        assert!(data.extra.one_pc);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::CustomRaftLogType::OnePc);
        let mut cnt = 0;
        custom_log.iterate_one_pc(|k, v, is_extra, del_lock, start_ts, commit_ts| {
            match k {
                b"k1" => assert_eq!(v, b"v1"),
                b"k2" => assert_eq!(v, &b"v".repeat(SHORT_VALUE_MAX_LEN + 1)),
                _ => unreachable!(),
            }
            assert!(!is_extra);
            assert!(!del_lock);
            assert_eq!(start_ts, 30);
            assert_eq!(commit_ts, 31);
            cnt += 1;
        });
        assert_eq!(cnt, 2);

        // PessimisticLock
        let mut data = pessimistic_lock(vec![b"k1"], b"k1", 40);
        let modifies = data.modifies.clone();
        assert_eq!(data.extra.req_type, ReqType::PessimisticLock);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(
            custom_log.get_type(),
            rlog::CustomRaftLogType::PessimisticLock
        );
        let mut cnt = 0;
        custom_log.iterate_lock(|k, v| {
            assert_eq!(k, &modifies[0].key().to_raw().unwrap());
            assert_eq!(v, &get_modify_value(&modifies[0]));
            cnt += 1;
        });
        assert_eq!(cnt, 1);

        // Pessimistic 1PC
        let mut data = pessimistic_prewrite(
            vec![
                (
                    Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec()),
                    DoPessimisticCheck,
                ),
                (
                    Mutation::make_lock(Key::from_raw(b"k2")),
                    SkipPessimisticCheck,
                ),
            ],
            b"k1",
            40,
            true,
            50,
        );
        assert!(data.extra.one_pc);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::CustomRaftLogType::OnePc);
        let mut cnt = 0;
        custom_log.iterate_one_pc(|k, v, is_extra, del_lock, start_ts, commit_ts| {
            match k {
                b"k1" => {
                    assert_eq!(v, b"v1");
                    assert!(!is_extra);
                    assert!(del_lock);
                }
                b"k2" => {
                    assert!(v.is_empty());
                    assert!(is_extra);
                    assert!(!del_lock);
                }
                _ => unreachable!("unexpected key: {:?}", k),
            };
            assert_eq!(start_ts, 40);
            assert_eq!(commit_ts, 41);
            cnt += 1;
        });
        assert_eq!(cnt, 2);

        // ResolveLock
        prewrite(
            vec![
                Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec()),
                Mutation::make_put(Key::from_raw(b"k2"), b"v2".to_vec()),
            ],
            b"k1",
            50,
            None,
            false,
            0,
        );
        prewrite(
            vec![
                Mutation::make_put(Key::from_raw(b"k3"), b"v3".to_vec()),
                Mutation::make_put(Key::from_raw(b"k4"), b"v4".to_vec()),
            ],
            b"k3",
            60,
            None,
            false,
            0,
        );
        let txn_status = HashMap::from_iter(
            [(50.into(), 51.into()), (60.into(), 0.into())]
                .iter()
                .cloned(),
        );
        storage
            .sched_txn_command(
                commands::ResolveLockReadPhase::new(txn_status, None, Context::default()),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut data = engine.take_last_write_data().unwrap();
        assert_eq!(data.extra.req_type, ReqType::ResolveLock);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::CustomRaftLogType::ResolveLock);
        let mut expected = HashMap::from_iter(vec![
            (
                b"k1".as_slice(),
                (rlog::CustomRaftLogType::Commit, b"k1".as_slice(), 51, true),
            ),
            (
                b"k2".as_slice(),
                (rlog::CustomRaftLogType::Commit, b"k2".as_slice(), 51, true),
            ),
            (
                b"k3".as_slice(),
                (
                    rlog::CustomRaftLogType::Rollback,
                    b"k3".as_slice(),
                    60,
                    true,
                ),
            ),
            (
                b"k4".as_slice(),
                (
                    rlog::CustomRaftLogType::Rollback,
                    b"k4".as_slice(),
                    60,
                    true,
                ),
            ),
        ]);
        custom_log.iterate_resolve_lock(|tp, k, ts, del_lock| {
            assert_eq!(expected.remove(k).unwrap(), (tp, k, ts, del_lock));
        });

        // 1PC fallback
        let mut data = prewrite(
            vec![Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec())],
            b"k1",
            70,
            None,
            true,
            1,
        );
        assert_eq!(data.extra.req_type, ReqType::Prewrite);
        let modifies = data.modifies.clone();
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::CustomRaftLogType::Prewrite);
        let mut cnt = 0;
        custom_log.iterate_lock(|k, v| {
            assert_eq!(k, modifies[0].key().to_raw().unwrap());
            assert_eq!(v, &get_modify_value(&modifies[0]));
            cnt += 1;
        });
        assert_eq!(cnt, 1);
        // Rollback
        storage
            .sched_txn_command(
                commands::Rollback::new(
                    vec![Key::from_raw(b"k1"), Key::from_raw(b"k2")],
                    70.into(),
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut data = engine.take_last_write_data().unwrap();
        assert_eq!(data.extra.req_type, ReqType::Rollback);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::CustomRaftLogType::Rollback);
        let mut cnt = 0;
        custom_log.iterate_rollback(|k, ts, del_lock| {
            match k {
                b"k1" => assert!(del_lock),
                b"k2" => assert!(!del_lock),
                _ => unreachable!(),
            };
            assert_eq!(ts, 70);
            cnt += 1
        });
        assert_eq!(cnt, 2);

        // PessimisticRollback
        pessimistic_lock(vec![b"k1"], b"k1", 80);
        storage
            .sched_txn_command(
                commands::PessimisticRollback::new(
                    vec![Key::from_raw(b"k1")],
                    80.into(),
                    80.into(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut data = engine.take_last_write_data().unwrap();
        assert_eq!(data.extra.req_type, ReqType::PessimisticRollback);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(
            custom_log.get_type(),
            rlog::CustomRaftLogType::PessimisticRollback
        );
        let mut cnt = 0;
        custom_log.iterate_del_lock(|k| {
            assert_eq!(k, b"k1");
            cnt += 1
        });
        assert_eq!(cnt, 1);

        // Heartbeat
        pessimistic_lock(vec![b"k1"], b"k1", 90);
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(
                    Key::from_raw(b"k1"),
                    90.into(),
                    10000,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut data = engine.take_last_write_data().unwrap();
        assert_eq!(data.extra.req_type, ReqType::Heartbeat);
        let modifies = data.modifies.clone();
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(
            custom_log.get_type(),
            rlog::CustomRaftLogType::PessimisticLock
        );
        let mut cnt = 0;
        custom_log.iterate_lock(|k, v| {
            assert_eq!(k, &modifies[0].key().to_raw().unwrap());
            assert_eq!(v, &get_modify_value(&modifies[0]));
            cnt += 1
        });
        assert_eq!(cnt, 1);
        pessimistic_prewrite(
            vec![(
                Mutation::make_delete(Key::from_raw(b"k1")),
                DoPessimisticCheck,
            )],
            b"k1",
            90,
            false,
            0,
        );
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(
                    Key::from_raw(b"k1"),
                    90.into(),
                    20000,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut data = engine.take_last_write_data().unwrap();
        assert_eq!(data.extra.req_type, ReqType::Heartbeat);
        let modifies = data.modifies.clone();
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::CustomRaftLogType::Prewrite);
        let mut cnt = 0;
        custom_log.iterate_lock(|k, v| {
            assert_eq!(k, &modifies[0].key().to_raw().unwrap());
            assert_eq!(v, &get_modify_value(&modifies[0]));
            cnt += 1
        });
        assert_eq!(cnt, 1);

        // CheckTxnStatus
        let check_txn_status =
            |key, lock_ts: u64, caller_start_ts: u64, resolving_pessimistic_lock| {
                storage
                    .sched_txn_command(
                        commands::CheckTxnStatus::new(
                            Key::from_raw(key),
                            lock_ts.into(),
                            caller_start_ts.into(),
                            caller_start_ts.into(),
                            true,
                            true,
                            resolving_pessimistic_lock,
                            false,
                            true,
                            Context::default(),
                        ),
                        expect_ok_callback(tx.clone(), 1),
                    )
                    .unwrap();
                rx.recv().unwrap();
                engine.take_last_write_data().unwrap()
            };
        // Push lock's min_commit_ts
        let mut data = check_txn_status(b"k1", 90, 95, false);
        assert_eq!(data.extra.req_type, ReqType::CheckTxnStatus);
        let modifies = data.modifies.clone();
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::CustomRaftLogType::Prewrite);
        let mut cnt = 0;
        custom_log.iterate_lock(|k, v| {
            assert_eq!(k, &modifies[0].key().to_raw().unwrap());
            assert_eq!(v, &get_modify_value(&modifies[0]));
            cnt += 1
        });
        assert_eq!(cnt, 1);

        // Rollback lock
        let mut data = check_txn_status(b"k1", 90, u64::MAX, false);
        assert_eq!(data.extra.req_type, ReqType::CheckTxnStatus);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::CustomRaftLogType::Rollback);
        let mut cnt = 0;
        custom_log.iterate_rollback(|k, ts, del_lock| {
            assert_eq!(k, b"k1");
            assert!(del_lock);
            assert_eq!(ts, 90);
            cnt += 1
        });
        assert_eq!(cnt, 1);

        // Rollback a missing lock and there is a lock of another async-commit
        // transaction.
        prewrite(
            vec![Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec())],
            b"k1",
            95,
            Some(vec![b"k2".to_vec()]),
            false,
            100,
        );
        let mut data = check_txn_status(b"k1", 96, 97, false);
        assert_eq!(data.extra.req_type, ReqType::CheckTxnStatus);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::CustomRaftLogType::Rollback);
        let mut cnt = 0;
        custom_log.iterate_rollback(|k, ts, del_lock| {
            assert_eq!(k, b"k1");
            assert!(!del_lock);
            assert_eq!(ts, 96);
            cnt += 1
        });
        assert_eq!(cnt, 1);
        check_txn_status(b"k1", 95, u64::MAX, false);

        pessimistic_lock(vec![b"k1"], b"k1", 100);
        // Push pessimistic lock's min_commit_ts
        let mut data = check_txn_status(b"k1", 100, 110, false);
        assert_eq!(data.extra.req_type, ReqType::CheckTxnStatus);
        let modifies = data.modifies.clone();
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(
            custom_log.get_type(),
            rlog::CustomRaftLogType::PessimisticLock
        );
        let mut cnt = 0;
        custom_log.iterate_lock(|k, v| {
            assert_eq!(k, &modifies[0].key().to_raw().unwrap());
            assert_eq!(v, &get_modify_value(&modifies[0]));
            cnt += 1
        });
        assert_eq!(cnt, 1);
        // Rollback pessimistic lock
        let mut data = check_txn_status(b"k1", 100, u64::MAX, true);
        assert_eq!(data.extra.req_type, ReqType::CheckTxnStatus);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(
            custom_log.get_type(),
            rlog::CustomRaftLogType::PessimisticRollback
        );
        let mut cnt = 0;
        custom_log.iterate_del_lock(|k| {
            assert_eq!(k, b"k1");
            cnt += 1
        });
        assert_eq!(cnt, 1);

        // CheckSecondaryLocks
        storage
            .sched_txn_command(
                commands::CheckSecondaryLocks::new(
                    vec![Key::from_raw(b"k1")],
                    110.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut data = engine.take_last_write_data().unwrap();
        assert_eq!(data.extra.req_type, ReqType::CheckSecondaryLocks);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::CustomRaftLogType::Rollback);
        let mut cnt = 0;
        custom_log.iterate_rollback(|k, ts, del_lock| {
            assert_eq!(k, b"k1");
            assert_eq!(ts, 110);
            assert!(!del_lock);
            cnt += 1
        });
        assert_eq!(cnt, 1);
    }
}
