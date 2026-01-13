// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::min,
    collections::{HashMap, HashSet, hash_map::Entry},
    fmt::Debug,
    mem,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
    time::Duration,
};

use crossbeam::channel::RecvTimeoutError;
use kvproto::{errorpb, raft_cmdpb::RaftCmdResponse};
use raft_proto::eraftpb::MessageType;
use raftstore::store::{
    metrics::{
        STORE_WRITE_RAFTDB_DURATION_HISTOGRAM, STORE_WRITE_SEND_DURATION_HISTOGRAM,
        STORE_WRITE_TRIGGER_SIZE_HISTOGRAM,
    },
    util,
};
use rfengine::WriteBatch;
use tikv_util::{
    debug, error, info,
    mpsc::{Receiver, Sender},
    sys::thread::StdThreadBuildWrapper,
    time::{Instant, duration_to_sec},
    warn,
};

use super::*;
use crate::{RaftRouter, store::metrics::IDLE_PEER_COUNT};

#[derive(Clone)]
pub(crate) struct PeerStates {
    pub(crate) applier: Arc<Mutex<Applier>>,
    pub(crate) peer_fsm: Arc<Mutex<PeerFsm>>,
}

impl PeerStates {
    pub(crate) fn new(applier: Applier, peer_fsm: PeerFsm) -> Self {
        Self {
            applier: Arc::new(Mutex::new(applier)),
            peer_fsm: Arc::new(Mutex::new(peer_fsm)),
        }
    }
}

#[allow(clippy::vec_box)]
pub struct PeerInbox {
    pub(crate) peer: PeerStates,
    pub(crate) msgs: Vec<Box<PeerMsg>>,
}

impl Debug for PeerInbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerInbox")
            .field("messages", &self.msgs.len())
            .finish()
    }
}

impl PeerInbox {
    pub(crate) fn new(peer_state: PeerStates) -> Self {
        PeerInbox {
            peer: peer_state,
            msgs: vec![],
        }
    }

    pub(crate) fn process(
        &mut self,
        ctx: &mut RaftContext,
        apply_senders: &[Sender<Option<ApplyBatch>>],
        statistics: Option<&mut [InboxPeerStat]>,
    ) {
        if self.msgs.is_empty() {
            return;
        }
        let mut peer_fsm = self.peer.peer_fsm.lock().unwrap();
        if peer_fsm.stopped {
            return;
        }
        let msg_len = self.msgs.len();
        let region_id = peer_fsm.region_id();
        let start = tikv_util::time::Instant::now_coarse();
        tikv_util::set_current_region_thread_local(region_id);
        let msg_type = self.msgs[0].type_code();
        PeerMsgHandler::new(&mut peer_fsm, ctx).handle_msgs(&mut self.msgs);
        let tick_flag = peer_fsm.ticker.tick_flag;
        peer_fsm.ticker.tick_flag = 0;
        let handle_ready_start = tikv_util::time::Instant::now_coarse();
        let handle_msgs_duration = handle_ready_start.saturating_duration_since(start);
        peer_fsm.peer.handle_raft_ready(ctx, None);
        let handle_ready_duration = handle_ready_start.saturating_elapsed();
        if !ctx.apply_msgs.msgs.is_empty() {
            peer_fsm.may_change_apply_worker();
            let peer_batch = ApplyBatch {
                msgs: mem::take(&mut ctx.apply_msgs.msgs),
                applier: self.peer.applier.clone(),
                applying_cnt: peer_fsm.applying_cnt.clone(),
                send_time: tikv_util::time::Instant::now(),
            };
            peer_batch
                .applying_cnt
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = apply_senders[peer_fsm.apply_worker_idx].send(Some(peer_batch));
        }
        if statistics.is_none() {
            return;
        }
        // We are in main raft worker thread, so we can check the idle time.
        if ctx.worker_type == WorkerType::Main
            && start.saturating_duration_since(peer_fsm.peer.last_active_time)
                > ctx.cfg.peer_idle_duration.0
            && !peer_fsm.peer.pending_remove
        {
            let store = peer_fsm.peer.get_store();
            // The heartbeat message generate persist ready, if we send it to idle, we will
            // receive PersistReady message soon, then it will wakeup instantly, so we check
            // persist ready to avoid this.
            let has_persist_ready = ctx
                .persist_readies
                .last()
                .map(|p| p.region_id == region_id)
                .unwrap_or_default();
            if !has_persist_ready
                && store.is_initialized()
                && store.snap_state == SnapState::Relax
                && store.applied_index() == store.last_index()
            {
                // The peer is idle for a long time, we can send it to idle worker.
                ctx.pending_idles.push(region_id);
            }
        }
        let statistics = statistics.unwrap();
        // Filter out the elapsed time longer than recorded and replace the minimum one
        // if any.
        let elapsed = start.saturating_elapsed();
        // If this peer handle cost is too short, skip the statistics to avoid iterate.
        if elapsed < Duration::from_millis(10) {
            return;
        }
        if let Some(min_index) = statistics
            .iter()
            .enumerate()
            .filter(|(_, &stat)| stat.elapsed < elapsed)
            .min_by_key(|&(_, &val)| val.elapsed)
            .map(|(i, _)| i)
        {
            let stat = &mut statistics[min_index];
            stat.region_id = region_id;
            stat.elapsed = elapsed;
            stat.handle_msgs = handle_msgs_duration;
            stat.handle_ready = handle_ready_duration;
            stat.tick_flag = tick_flag;
            stat.msg_cnt = msg_len;
            stat.msg_type = msg_type;
        }
    }
}

pub(crate) struct Inboxes {
    inboxes: HashMap<u64, PeerInbox>,
}

impl Inboxes {
    fn new() -> Self {
        Inboxes {
            inboxes: HashMap::new(),
        }
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub(crate) struct InboxPeerStat {
    pub(crate) region_id: u64,
    pub(crate) elapsed: Duration,
    pub(crate) handle_msgs: Duration,
    pub(crate) handle_ready: Duration,
    pub(crate) msg_cnt: usize,
    pub(crate) msg_type: u8,
    pub(crate) tick_flag: u64,
}

pub(crate) struct RaftWorker {
    ctx: StoreContext,
    receiver: Receiver<(u64, Box<PeerMsg>)>,
    router: RaftRouter,
    apply_senders: Vec<Sender<Option<ApplyBatch>>>,
    io_sender: Sender<Option<IoTask>>,
    last_tick: Instant,
    tick_seg_idx: usize,
    tick_millis: u64,
    store_fsm: StoreFsm,
    batch_msg_count: usize,
    aux_task_senders: Vec<Sender<Vec<PeerInbox>>>,
    aux_res_receivers: Vec<Receiver<()>>,
    sent_aux_task: Vec<bool>,
    aux_handles: Vec<std::thread::JoinHandle<()>>,
    // The cpu usage percent for sum of raft worker thread and aux threads.
    // It is updated by background threads and checked in raft thread to update active aux
    // worker count.
    cpu_util: Arc<AtomicUsize>,
    active_aux_count: usize,
    idle_handle: Option<std::thread::JoinHandle<()>>,
}

const MAX_BATCH_COUNT: usize = 1024;
const PEER_INBOX_STATISTIC_COUNT: usize = 5;

impl RaftWorker {
    pub(crate) fn new(
        ctx: StoreContext,
        receiver: Receiver<(u64, Box<PeerMsg>)>,
        router: RaftRouter,
        io_sender: Sender<Option<IoTask>>,
        store_fsm: StoreFsm,
        cpu_util: Arc<AtomicUsize>,
    ) -> (Self, Vec<Receiver<Option<ApplyBatch>>>) {
        let all_apply_pool_size = ctx.cfg.apply_pool_size + ctx.cfg.apply_follower_pool_size;
        let mut apply_senders = Vec::with_capacity(all_apply_pool_size);
        let mut apply_receivers = Vec::with_capacity(all_apply_pool_size);
        for _ in 0..all_apply_pool_size {
            let (sender, receiver) = tikv_util::mpsc::unbounded();
            apply_senders.push(sender);
            apply_receivers.push(receiver);
        }
        let tick_millis = ctx.cfg.raft_base_tick_interval.as_millis();
        (
            Self {
                ctx,
                receiver,
                router,
                apply_senders,
                io_sender,
                last_tick: Instant::now(),
                tick_seg_idx: 0,
                tick_millis,
                store_fsm,
                batch_msg_count: 0,
                aux_task_senders: vec![],
                aux_res_receivers: vec![],
                sent_aux_task: vec![],
                aux_handles: vec![],
                cpu_util,
                active_aux_count: 0,
                idle_handle: None,
            },
            apply_receivers,
        )
    }

    pub(crate) fn run(&mut self) {
        let mut inboxes: Inboxes = Inboxes::new();
        let mut inbox_peer_stats = vec![InboxPeerStat::default(); PEER_INBOX_STATISTIC_COUNT];
        let store_id = self.ctx.store_id();
        for i in 0..self.ctx.cfg.aux_worker_count {
            let (mut aux_worker, aux_result_rx) = RaftAuxWorker::new(
                self.ctx.global.clone(),
                self.apply_senders.clone(),
                self.io_sender.clone(),
            );
            let aux_task_tx = aux_worker.task_tx.clone();
            self.aux_task_senders.push(aux_task_tx);
            self.aux_res_receivers.push(aux_result_rx);
            self.sent_aux_task.push(false);
            let props = tikv_util::thread_group::current_properties();
            let aux_handle = std::thread::Builder::new()
                .name(format!("raftstore_{}", i + 1))
                .spawn_wrapper(move || {
                    tikv_util::thread_group::set_properties(props);
                    aux_worker.run()
                })
                .unwrap();
            self.aux_handles.push(aux_handle);
        }
        let idle_ctx = RaftContext::new(self.ctx.global.clone(), WorkerType::Idle);
        let (idle_sender, idle_receiver) = tikv_util::mpsc::unbounded();
        let mut idle_worker =
            RaftIdleWorker::new(idle_ctx, idle_receiver, self.apply_senders.clone());
        let props = tikv_util::thread_group::current_properties();
        let idle_handle = std::thread::Builder::new()
            .name("raftstore_idle".to_string())
            .spawn_wrapper(move || {
                tikv_util::thread_group::set_properties(props);
                idle_worker.run()
            })
            .unwrap();
        self.ctx.idle_sender = Some(idle_sender);
        self.idle_handle = Some(idle_handle);
        loop {
            self.handle_store_msg();
            if self.store_fsm.stopped {
                self.stop();
                return;
            }

            let loop_start = match self.receive_msgs(&mut inboxes) {
                Ok(start_time) => start_time,
                Err(_) => return,
            };
            // If store msg is empty, we can sync aux_worker after receive latest messages.
            self.sync_aux_worker();
            let mut inboxes_vec: Vec<PeerInbox> =
                inboxes.inboxes.drain().map(|(_, inbox)| inbox).collect();
            self.try_send_aux_task(&mut inboxes_vec);
            inboxes_vec.into_iter().for_each(|mut inbox| {
                inbox.process(
                    &mut self.ctx,
                    &self.apply_senders,
                    Some(&mut inbox_peer_stats),
                );
            });
            for idle_region in mem::take(&mut self.ctx.pending_idles) {
                let peer_state = self.ctx.get_peer(idle_region);
                let idle = PeerInbox::new(peer_state);
                let idle_sender = self.ctx.idle_sender.as_ref().unwrap();
                idle_sender
                    .send((idle_region, Box::new(PeerMsg::Idle(idle))))
                    .unwrap();
                let store_id = self.ctx.store_id();
                info!("{}:{}: region is idle", store_id, idle_region);
                self.ctx.idle_regions.insert(idle_region);
            }
            let process_inbox_duration = loop_start.saturating_elapsed();
            if process_inbox_duration > SLOW_LOG_DURATION {
                inbox_peer_stats.sort_by(|a, b| b.elapsed.cmp(&a.elapsed));
                warn!(
                    "store_id: {} raft worker batch loop takes too long, process_inbox_elapsed: {:?}, top_{} peers: {:?}",
                    store_id, process_inbox_duration, PEER_INBOX_STATISTIC_COUNT, inbox_peer_stats
                );
            }
            inbox_peer_stats.fill(InboxPeerStat::default());
            if self.ctx.global.trans.need_flush() {
                self.ctx.global.trans.flush();
            }
            persist_states(&mut self.ctx, &self.io_sender);
            batch_end(&mut self.ctx, loop_start.saturating_elapsed());
            self.update_active_aux_count();
        }
    }

    fn try_send_aux_task(&mut self, inboxes: &mut Vec<PeerInbox>) {
        if !self.ctx.raft_ctx.raft_wb.is_empty() {
            // The raft_wb is written by handling store messages, do not use aux worker to
            // avoid race.
            return;
        }
        if self.active_aux_count == 0 {
            return;
        }
        if inboxes.len() <= 1 {
            // The aux worker is helpful only when the main raft worker is busy.
            // When there only one inbox, redirect the task to aux worker doesn't
            // have any benefit but increase the latency.
            return;
        }
        let mut aux_inboxes = vec![];
        let mut aux_msg_count = 0;
        let mut aux_worker_idx = 0;
        let target_aux_msg_count = self.batch_msg_count / (self.active_aux_count + 1);
        while let Some(inbox) = inboxes.pop() {
            aux_msg_count += inbox.msgs.len();
            aux_inboxes.push(inbox);
            if aux_msg_count >= target_aux_msg_count {
                self.aux_task_senders[aux_worker_idx]
                    .send(mem::take(&mut aux_inboxes))
                    .unwrap();
                self.sent_aux_task[aux_worker_idx] = true;
                aux_msg_count = 0;
                aux_worker_idx += 1;
                if aux_worker_idx == self.active_aux_count {
                    break;
                }
            }
        }
        if !aux_inboxes.is_empty() {
            self.aux_task_senders[aux_worker_idx]
                .send(mem::take(&mut aux_inboxes))
                .unwrap();
            self.sent_aux_task[aux_worker_idx] = true;
        }
    }

    fn sync_aux_worker(&mut self) {
        for i in 0..self.ctx.cfg.aux_worker_count {
            if self.sent_aux_task[i] {
                self.aux_res_receivers[i].recv().unwrap();
                self.sent_aux_task[i] = false;
            }
        }
    }

    pub(crate) fn stop(&mut self) {
        self.apply_senders.iter().for_each(|sender| {
            let _ = sender.send(None);
        });
        let _ = self.io_sender.send(None);
        for peer_map in &self.ctx.peers {
            for peer in peer_map.values() {
                let mut peer_fsm = peer.peer_fsm.lock().unwrap();
                peer_fsm.peer.pending_reads.clear_all(None);
            }
        }
        for aux_task_tx in self.aux_task_senders.drain(..) {
            let _ = aux_task_tx.send(vec![]);
        }
        for handle in self.aux_handles.drain(..) {
            let _ = handle.join();
        }
        self.ctx.idle_sender.take();
        if let Some(handle) = self.idle_handle.take() {
            let _ = handle.join();
        }
    }

    fn handle_store_msg(&mut self) {
        while let Ok(msg) = self.store_fsm.receiver.try_recv() {
            self.sync_aux_worker();
            let mut store_handler = StoreMsgHandler::new(&mut self.store_fsm, &mut self.ctx);
            if let Some(apply_region) = store_handler.handle_msg(msg) {
                let peer = self.ctx.get_peer(apply_region);
                let mut peer_fsm = peer.peer_fsm.lock().unwrap();
                let applier = peer.applier.clone();
                self.maybe_send_apply(&applier, &mut peer_fsm);
            }
        }
        if self.store_fsm.last_tick.saturating_elapsed().as_millis() as u64
            > self.store_fsm.tick_millis
        {
            let mut store_handler = StoreMsgHandler::new(&mut self.store_fsm, &mut self.ctx);
            store_handler.handle_msg(StoreMsg::Tick);
            self.store_fsm.last_tick = Instant::now();
        }
    }

    /// return true means channel is disconnected, return outer loop.
    fn receive_msgs(
        &mut self,
        inboxes: &mut Inboxes,
    ) -> std::result::Result<tikv_util::time::Instant, RecvTimeoutError> {
        self.batch_msg_count = 0;
        let res = self.receiver.recv_timeout(Duration::from_millis(10));
        let receive_time = tikv_util::time::Instant::now();
        match res {
            Ok((region_id, msg)) => {
                let mut batch_size = msg.size();
                self.append_msg(inboxes, region_id, msg);
                while let Ok((region_id, msg)) = self.receiver.try_recv() {
                    batch_size += msg.size();
                    self.append_msg(inboxes, region_id, msg);
                    if self.batch_msg_count > MAX_BATCH_COUNT
                        || batch_size > self.ctx.cfg.raft_worker_max_batch_size.0 as usize
                    {
                        break;
                    }
                }
            }
            Err(RecvTimeoutError::Disconnected) => return Err(RecvTimeoutError::Disconnected),
            Err(RecvTimeoutError::Timeout) => {}
        }
        let tick_elapsed_millis = self.last_tick.saturating_elapsed().as_millis() as u64;
        let next_tick_seg_idx = min(
            (tick_elapsed_millis * PEER_SEGMENTS as u64 / self.tick_millis) as usize,
            PEER_SEGMENTS,
        );
        let mut tick_cnt = 0;
        for seg_idx in self.tick_seg_idx..next_tick_seg_idx {
            let peer_map = &self.ctx.peers[seg_idx];
            for (region_id, peer) in peer_map.iter() {
                if self.ctx.idle_regions.contains(region_id) {
                    continue;
                }
                match inboxes.inboxes.entry(*region_id) {
                    Entry::Occupied(mut entry) => {
                        entry.get_mut().msgs.push(Box::new(PeerMsg::Tick));
                        tick_cnt += 1;
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(PeerInbox {
                            peer: peer.clone(),
                            msgs: vec![Box::new(PeerMsg::Tick)],
                        });
                        tick_cnt += 1;
                    }
                }
            }
        }
        self.batch_msg_count += tick_cnt;
        if next_tick_seg_idx == PEER_SEGMENTS {
            self.last_tick = Instant::now();
            self.tick_seg_idx = 0;
        } else {
            self.tick_seg_idx = next_tick_seg_idx;
        }
        Ok(receive_time)
    }

    fn append_msg(&mut self, inboxes: &mut Inboxes, region_id: u64, msg: Box<PeerMsg>) {
        if let PeerMsg::WakeUp(idle_peer) = *msg {
            self.ctx.idle_regions.remove(&region_id);
            self.batch_msg_count += idle_peer.msgs.len();
            inboxes.inboxes.insert(region_id, idle_peer);
            return;
        }
        if self.ctx.idle_regions.contains(&region_id) {
            let idle_sender = self.ctx.idle_sender.as_ref().unwrap();
            idle_sender.send((region_id, msg)).unwrap();
            return;
        }
        if let Some(inbox) = inboxes.inboxes.get_mut(&region_id) {
            inbox.msgs.push(msg);
            self.batch_msg_count += 1;
            return;
        }
        if let Some(peer) = self.ctx.try_get_peer(region_id) {
            inboxes.inboxes.insert(
                region_id,
                PeerInbox {
                    peer,
                    msgs: vec![msg],
                },
            );
            self.batch_msg_count += 1;
            return;
        }
        match *msg {
            PeerMsg::RaftMessage(msg) => {
                self.router.send_store(StoreMsg::RaftMessage(msg));
            }
            PeerMsg::RaftCommand(cmd) => {
                let mut resp = RaftCmdResponse::default();
                let mut err = errorpb::Error::default();
                err.set_message(format!("region {} is missing", region_id));
                err.mut_region_not_found().set_region_id(region_id);
                resp.mut_header().set_error(err);
                cmd.callback.invoke_with_response(resp);
            }
            PeerMsg::Tick => {}
            PeerMsg::Start => {}
            PeerMsg::ApplyResult(_) => {}
            PeerMsg::CasualMessage(_) => {}
            PeerMsg::SignificantMsg(_) => {}
            PeerMsg::GenerateEngineChangeSet(..) => {}
            PeerMsg::ApplySnapshotResult(_) => {}
            PeerMsg::PrepareChangeSetResult(..) => {}
            PeerMsg::Persisted(_) => {}
            PeerMsg::PrepareCommitMergeResult(..) => {}
            PeerMsg::PrepareTxnFileResult { .. } => {}
            PeerMsg::Idle(_) => {}
            PeerMsg::WakeUp(_) => {}
            PeerMsg::StoreMsgForWakeUp(_) => unreachable!("included in wakeup"),
        }
    }

    fn maybe_send_apply(&mut self, applier: &Arc<Mutex<Applier>>, peer_fsm: &mut PeerFsm) {
        if !self.ctx.apply_msgs.msgs.is_empty() {
            peer_fsm.may_change_apply_worker();
            let peer_batch = ApplyBatch {
                msgs: mem::take(&mut self.ctx.apply_msgs.msgs),
                applier: applier.clone(),
                applying_cnt: peer_fsm.applying_cnt.clone(),
                send_time: tikv_util::time::Instant::now(),
            };
            peer_batch
                .applying_cnt
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.apply_senders[peer_fsm.apply_worker_idx]
                .send(Some(peer_batch))
                .unwrap();
        }
    }

    fn update_active_aux_count(&mut self) {
        let cfg = &self.ctx.cfg;
        if cfg.aux_worker_count == 0 {
            return;
        }
        let cpu_usage = self.cpu_util.load(Relaxed);
        let origin_active_aux_count = self.active_aux_count;
        if cpu_usage < cfg.main_worker_max_util {
            self.active_aux_count = 0;
        } else {
            let expect_aux_usage = cpu_usage - cfg.main_worker_max_util;
            self.active_aux_count = expect_aux_usage
                .div_ceil(cfg.aux_worker_max_util)
                .min(cfg.aux_worker_count);
        }
        if self.active_aux_count != origin_active_aux_count {
            debug!(
                "{}: update use aux worker count to {} on usage {}",
                self.ctx.store_id(),
                self.active_aux_count,
                cpu_usage
            );
        }
    }
}

pub(crate) struct RaftAuxWorker {
    ctx: RaftContext,
    task_tx: Sender<Vec<PeerInbox>>,
    task_rx: Receiver<Vec<PeerInbox>>,
    result_tx: Sender<()>,
    apply_senders: Vec<Sender<Option<ApplyBatch>>>,
    io_sender: Sender<Option<IoTask>>,
}

impl RaftAuxWorker {
    fn new(
        ctx: GlobalContext,
        apply_senders: Vec<Sender<Option<ApplyBatch>>>,
        io_sender: Sender<Option<IoTask>>,
    ) -> (Self, Receiver<()>) {
        let ctx = RaftContext::new(ctx, WorkerType::Aux);
        let (task_tx, task_rx) = tikv_util::mpsc::bounded(1);
        let (result_tx, result_rx) = tikv_util::mpsc::bounded(1);
        (
            Self {
                ctx,
                task_tx,
                task_rx,
                result_tx,
                apply_senders,
                io_sender,
            },
            result_rx,
        )
    }

    fn run(&mut self) {
        while let Ok(inboxes) = self.task_rx.recv() {
            if inboxes.is_empty() {
                return;
            }
            let loop_start = tikv_util::time::Instant::now();
            for mut inbox in inboxes {
                inbox.process(&mut self.ctx, &self.apply_senders, None);
            }
            if self.ctx.global.trans.need_flush() {
                self.ctx.global.trans.flush();
            }
            persist_states(&mut self.ctx, &self.io_sender);
            self.result_tx.send(()).unwrap();
            batch_end(&mut self.ctx, loop_start.saturating_elapsed());
        }
    }
}

pub(crate) struct ApplyWorker {
    ctx: ApplyContext,
    receiver: Receiver<Option<ApplyBatch>>,
}

impl ApplyWorker {
    pub(crate) fn new(
        engine: kvengine::Engine,
        router: RaftRouter,
        receiver: Receiver<Option<ApplyBatch>>,
    ) -> Self {
        let ctx = ApplyContext::new(engine, Some(router));
        Self { ctx, receiver }
    }

    pub(crate) fn run(&mut self) {
        let mut loop_cnt = 0u64;
        while let Ok(Some(mut batch)) = self.receiver.recv() {
            let timer = tikv_util::time::Instant::now();
            self.ctx.apply_wait.observe(duration_to_sec(
                timer.saturating_duration_since(batch.send_time),
            ));
            let mut applier = batch.applier.lock().unwrap();
            tikv_util::set_current_region_thread_local(applier.region_id());
            for msg in batch.msgs.drain(..) {
                applier.handle_msg(&mut self.ctx, msg);
            }
            batch
                .applying_cnt
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            loop_cnt += 1;
            if loop_cnt % 128 == 0 {
                self.ctx.apply_wait.flush();
                self.ctx.apply_time.flush();
            }
        }
    }
}

fn persist_states(ctx: &mut RaftContext, io_sender: &Sender<Option<IoTask>>) {
    if ctx.persist_readies.is_empty() && ctx.raft_wb.is_empty() {
        return;
    }
    let raft_wb = mem::take(&mut ctx.raft_wb);
    let remove_dependents = mem::take(&mut ctx.remove_dependents);
    ctx.global.engines.raft.apply(&raft_wb);
    let readies = mem::take(&mut ctx.persist_readies);
    let io_task = IoTask {
        raft_wb,
        readies,
        remove_dependents,
    };
    io_sender.send(Some(io_task)).unwrap();
}

fn batch_end(ctx: &mut RaftContext, batch_duration: Duration) {
    if batch_duration > Duration::from_millis(50) {
        info!("raft worker batch loop takes {:?}", batch_duration);
    }
    ctx.raft_metrics
        .store_time
        .observe(duration_to_sec(batch_duration));
    ctx.raft_metrics.maybe_flush();
    ctx.current_time = None;
    ctx.global.destroying.clear();
}

pub(crate) struct IoWorker {
    engine: rfengine::RfEngine,
    receiver: Receiver<Option<IoTask>>,
    router: RaftRouter,
    trans: Box<dyn Transport>,
    wb: WriteBatch,
    max_batch_size: usize,
    // min write duration is used to avoid too frequent write to raft db to avoid write
    // amplification.
    min_write_duration: Duration,
}

impl IoWorker {
    pub(crate) fn new(
        engine: rfengine::RfEngine,
        router: RaftRouter,
        trans: Box<dyn Transport>,
        max_batch_size: usize,
        min_write_duration: Duration,
    ) -> (Self, Sender<Option<IoTask>>) {
        let (sender, receiver) = tikv_util::mpsc::bounded(1024);
        (
            Self {
                engine,
                receiver,
                router,
                trans,
                wb: Default::default(),
                max_batch_size,
                min_write_duration,
            },
            sender,
        )
    }

    pub(crate) fn run(&mut self) {
        while let Ok(Some(task)) = self.receiver.recv() {
            let len = self.receiver.len();
            let mut tasks = Vec::with_capacity(len + 1);

            let mut total_estimated_size = task.raft_wb.estimated_size();
            tasks.push(task);
            for _ in 0..len {
                if total_estimated_size >= self.max_batch_size {
                    break;
                }

                let task = self.receiver.recv().unwrap();
                if task.is_none() {
                    return;
                }
                let task = task.unwrap();
                total_estimated_size += task.raft_wb.estimated_size();
                tasks.push(task);
            }
            for task in &mut tasks {
                if !task.raft_wb.is_empty() {
                    let wb = mem::take(&mut task.raft_wb);
                    self.wb.merge_write_batch(wb);
                }
            }
            self.handle_tasks(tasks);
        }
    }

    fn handle_tasks(&mut self, tasks: Vec<IoTask>) {
        let timer = tikv_util::time::Instant::now();
        let mut after_write = timer;
        if !self.wb.is_empty() {
            let wb = mem::take(&mut self.wb);
            let write_size = self.engine.persist(wb).unwrap();
            after_write = tikv_util::time::Instant::now();
            let write_raft_db_dur = after_write.saturating_duration_since(timer);
            if write_raft_db_dur > Duration::from_millis(40) {
                info!(
                    "io worker write raft db takes too long {:?}， size {}",
                    write_raft_db_dur, write_size
                );
            }
            STORE_WRITE_RAFTDB_DURATION_HISTOGRAM.observe(duration_to_sec(write_raft_db_dur));
            STORE_WRITE_TRIGGER_SIZE_HISTOGRAM.observe(write_size as f64);
        }
        for task in tasks {
            for mut ready in task.readies {
                let raft_messages = mem::take(&mut ready.raft_messages);
                for msg in raft_messages {
                    debug!(
                        "follower send raft message";
                        "region_id" => msg.region_id,
                        "message_type" => %util::MsgType(&msg),
                        "from_peer_id" => msg.get_from_peer().get_id(),
                        "to_peer_id" => msg.get_to_peer().get_id(),
                    );
                    if let Err(err) = self.trans.send(msg) {
                        error!("failed to send persist raft message {:?}", err);
                    }
                }
                let region_id = ready.region_id;
                self.router.send(region_id, PeerMsg::Persisted(ready));
            }

            for (parent_id, dependent_id) in task.remove_dependents {
                remove_dependent(&self.engine, &self.router, parent_id, dependent_id);
            }
        }
        if self.trans.need_flush() {
            self.trans.flush();
        }
        let end_time = tikv_util::time::Instant::now();
        let handle_time = end_time.saturating_duration_since(timer);
        if self.min_write_duration > handle_time {
            std::thread::sleep(self.min_write_duration - handle_time);
        }
        let send_time = duration_to_sec(end_time.saturating_duration_since(after_write));
        STORE_WRITE_SEND_DURATION_HISTOGRAM.observe(send_time);
    }
}

struct SegmentTicker {
    segments: Vec<HashSet<u64>>,
    tick_interval_ms: u64,
    last_tick: Instant,
    tick_segment_idx: usize,
}

impl SegmentTicker {
    fn new(tick_interval_ms: u64, segment_count: usize) -> Self {
        Self {
            segments: vec![HashSet::default(); segment_count],
            tick_interval_ms,
            last_tick: Instant::now(),
            tick_segment_idx: 0,
        }
    }

    fn tick(&mut self) -> &[HashSet<u64>] {
        let segment_count = self.segments.len();
        let tick_elapsed_millis = self.last_tick.saturating_elapsed().as_millis() as u64;
        let current_tick_seg_idx = self.tick_segment_idx;
        let next_tick_seg_idx = min(
            (tick_elapsed_millis * segment_count as u64 / self.tick_interval_ms) as usize,
            segment_count,
        );
        if next_tick_seg_idx == segment_count {
            self.last_tick = Instant::now();
            self.tick_segment_idx = 0;
        } else {
            self.tick_segment_idx = next_tick_seg_idx;
        }
        &self.segments[current_tick_seg_idx..next_tick_seg_idx]
    }

    fn insert(&mut self, region_id: u64) {
        let seg_idx = self.region_segment_idx(region_id);
        self.segments[seg_idx].insert(region_id);
    }

    fn remove(&mut self, region_id: u64) {
        let seg_idx = self.region_segment_idx(region_id);
        self.segments[seg_idx].remove(&region_id);
    }

    fn region_segment_idx(&self, region_id: u64) -> usize {
        crc32c::crc32c(&region_id.to_le_bytes()) as usize % self.segments.len()
    }
}

const IDLE_SEGMENTS: usize = 128;

pub(crate) struct RaftIdleWorker {
    ctx: RaftContext,
    rx: Receiver<(u64, Box<PeerMsg>)>,
    apply_senders: Vec<Sender<Option<ApplyBatch>>>,
    idle_peers: HashMap<u64, PeerInbox>,
    ticker: SegmentTicker,
}

impl RaftIdleWorker {
    fn new(
        ctx: RaftContext,
        rx: Receiver<(u64, Box<PeerMsg>)>,
        apply_senders: Vec<Sender<Option<ApplyBatch>>>,
    ) -> Self {
        let mut idle_tick_interval = ctx.cfg.raft_base_tick_interval.as_millis();
        if ctx.cfg.idle_worker_tick_slow {
            // Slow down the idle ticker by double the base tick interval to reduce the
            // heartbeat overhead.
            idle_tick_interval *= 2;
        }
        let ticker = SegmentTicker::new(idle_tick_interval, IDLE_SEGMENTS);
        RaftIdleWorker {
            ctx,
            rx,
            apply_senders,
            idle_peers: HashMap::default(),
            ticker,
        }
    }

    fn need_wake_up(idle_peer: &PeerInbox) -> Option<String> {
        idle_peer
            .msgs
            .iter()
            .find(|msg| !Self::is_heartbeat(msg))
            .map(|msg| msg.type_str().to_string())
    }

    fn is_heartbeat(msg: &PeerMsg) -> bool {
        match msg {
            PeerMsg::RaftMessage(raft_msg) => matches!(
                raft_msg.get_message().get_msg_type(),
                MessageType::MsgHeartbeat | MessageType::MsgHeartbeatResponse
            ),
            _ => {
                debug!("not heartbeat msg {:?}", msg);
                false
            }
        }
    }

    fn run(&mut self) {
        let mut update_regions = HashSet::new();
        loop {
            let loop_start = match self.receive_msgs(&mut update_regions) {
                Ok(start_time) => start_time,
                Err(_) => return,
            };
            for region_id in &update_regions {
                let idle_peer = self.idle_peers.get(region_id).unwrap();
                if let Some(msg_type) = Self::need_wake_up(idle_peer) {
                    let store_id = self.ctx.store_id();
                    let idle_peer = self.idle_peers.remove(region_id).unwrap();
                    self.ticker.remove(*region_id);
                    let mut peer_fsm = idle_peer.peer.peer_fsm.lock().unwrap();
                    peer_fsm.peer.last_active_time = tikv_util::time::Instant::now_coarse();
                    drop(peer_fsm);
                    info!("{}:{}: wake up by {}", store_id, region_id, msg_type);
                    self.ctx
                        .global
                        .router
                        .send(*region_id, PeerMsg::WakeUp(idle_peer));
                }
            }
            IDLE_PEER_COUNT.set(self.idle_peers.len() as i64);
            for segment in self.ticker.tick() {
                for region_id in segment {
                    if let Some(idle_peer) = self.idle_peers.get_mut(region_id) {
                        idle_peer.msgs.push(Box::new(PeerMsg::Tick));
                        idle_peer.process(&mut self.ctx, &self.apply_senders, None);
                        update_regions.remove(region_id);
                    }
                }
            }
            for region_id in update_regions.drain() {
                if let Some(idle_peer) = self.idle_peers.get_mut(&region_id) {
                    idle_peer.process(&mut self.ctx, &self.apply_senders, None);
                }
            }
            if !self.ctx.raft_wb.is_empty() {
                // It is possible the idle peers did raft log gc, we should persist it.
                let wb = mem::take(&mut self.ctx.raft_wb);
                self.ctx.global.engines.raft.write(wb).unwrap();
            }
            let process_duration = loop_start.saturating_elapsed();
            if self.ctx.global.trans_idle.need_flush() {
                self.ctx.global.trans_idle.flush();
            }
            let loop_duration = loop_start.saturating_elapsed();
            if loop_duration > Duration::from_millis(100) {
                info!(
                    "raft idle worker batch loop takes {:?}, process {:?}",
                    loop_duration, process_duration
                );
            }
        }
    }

    fn receive_msgs(
        &mut self,
        update_regions: &mut HashSet<u64>,
    ) -> Result<tikv_util::time::Instant, RecvTimeoutError> {
        let res = self.rx.recv_timeout(Duration::from_millis(10));
        let receive_time = tikv_util::time::Instant::now();
        let mut messages = vec![];
        match res {
            Ok((region_id, msg)) => {
                messages.push((region_id, msg));
            }
            Err(RecvTimeoutError::Disconnected) => return Err(RecvTimeoutError::Disconnected),
            Err(RecvTimeoutError::Timeout) => {}
        }
        let msg_len = self.rx.len();
        for _ in 0..msg_len {
            let (region_id, msg) = self.rx.recv()?;
            messages.push((region_id, msg));
        }
        for (region_id, message) in messages {
            if let Some(idle_peer) = self.idle_peers.get_mut(&region_id) {
                idle_peer.msgs.push(message);
                update_regions.insert(region_id);
            } else if let PeerMsg::Idle(idle_peer) = *message {
                self.idle_peers.insert(region_id, idle_peer);
                self.ticker.insert(region_id);
                IDLE_PEER_COUNT.set(self.idle_peers.len() as i64);
            } else {
                // The peer has been waked up, resend the message to main thread.
                match *message {
                    PeerMsg::StoreMsgForWakeUp(store_msg) => {
                        self.ctx.global.router.send_store(store_msg);
                    }
                    msg => {
                        self.ctx.global.router.send(region_id, msg);
                    }
                }
            }
        }
        Ok(receive_time)
    }
}
