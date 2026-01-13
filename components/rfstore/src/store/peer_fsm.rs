// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    collections::VecDeque,
    mem,
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Buf;
use error_code::ErrorCodeExt;
use fail::fail_point;
use kvengine::{
    CheckMergeResult, DEL_PREFIXES_KEY, IdVer, LARGE_NUM_COLUMNAR_TABLES_IN_SHARD,
    MANUAL_MAJOR_COMPACTION, MANUAL_MAJOR_COMPACTION_DISABLE, MANUAL_MAJOR_COMPACTION_ENABLE,
    MANUAL_MAJOR_COMPACTION_ENABLE_COLUMNAR, Shard, TERM_KEY, ia::ia_auto_file::report_transitions,
    table::schema_file::SchemaFile, table_id::is_table_boundary_key,
};
use kvproto::{
    import_sstpb::SwitchMode,
    metapb::{self, Region, RegionEpoch},
    pdpb::CheckPolicy,
    raft_cmdpb::{
        AdminCmdType, AdminRequest, CmdType, RaftCmdRequest, RaftCmdResponse, RaftRequestHeader,
        Request, StatusCmdType, StatusResponse,
    },
    raft_serverpb::{ExtraMessageType, PeerState, RaftMessage},
};
use protobuf::Message;
use raft::{self, GetEntriesContext, Storage, eraftpb::MessageType};
use raft_proto::eraftpb;
use raftstore::store::util;
use rand::{Rng, thread_rng};
use tikv_util::{
    box_err,
    codec::bytes::{decode_bytes, encode_bytes, encode_bytes_maybe_empty},
    debug, error, info,
    store::{find_peer, is_learner, region_on_same_stores},
    time::duration_to_sec,
    trace, warn,
};
use txn_types::{Key, WriteBatchFlags};

use super::{RequestInspector, SchemaTask, WorkerType};
use crate::{
    DiscardReason, Error, MERGE_REGION_WITH_TXN_FILE_LOCKS_ERR_MSG, RaftStoreRouter, Result,
    store::{
        ApplyMetrics, ApplyMsg, CasualMessage, Config, CustomBuilder, Engines, MsgApplyResult,
        PEER_TICK_CHECK_LONG, PEER_TICK_MAINTENANCE, PEER_TICK_PD_HEARTBEAT, PEER_TICK_RAFT,
        PEER_TICK_RAFT_LOG_GC, PEER_TICK_SPLIT_CHECK, PdTask, PeerMsg, PersistReady,
        RaftApplyState, RaftCommand, RaftContext, SignificantMsg, SnapState, StoreMeta, StoreMsg,
        Ticker, TrimOverBoundParameter,
        cmd_resp::{bind_term, message_error, new_error, new_with_key_error},
        ingest::convert_sst,
        load_last_peer_state,
        msg::Callback,
        notify_req_region_removed,
        peer::{Peer, StaleState},
        schema::{schema_file_is_matched_with_meta, shard_is_matched_with_meta},
        util as _util, write_engine_meta, write_engine_meta_diff,
    },
};

/// Limits the maximum number of regions returned by error.
///
/// Another choice is using coprocessor batch limit, but 10 should be a good fit
/// in most case.
const _MAX_REGIONS_IN_ERROR: usize = 10;

pub struct PeerFsm {
    pub(crate) peer: Peer,
    pub(crate) stopped: bool,
    // leader_apply_worker_idx and follower_apply_worker_idx are initialized randomly,
    // and apply_worker_idx can use the corresponding worker index based on the role.
    pub(crate) apply_worker_idx: usize,
    leader_apply_worker_idx: usize,
    // If follower_apply_worker_idx is non-zero,
    // the follower's apply batch is sent to follower apply worker,
    // otherwise follower and leader share the apply worker.
    follower_apply_worker_idx: usize,
    // applying_cnt is increased by raft worker and decreased by apply worker.
    // When we need to change the worker idx, we need to make sure the applying_cnt is zero.
    pub(crate) applying_cnt: Arc<AtomicU64>,
    pub(crate) ticker: Ticker,
}

impl PeerFsm {
    // If we create the peer actively, like bootstrap/split/merge region, we should
    // use this function to create the peer. The region must contain the peer info
    // for this store.
    pub fn create(
        store_id: u64,
        cfg: &Config,
        engines: Engines,
        region: &metapb::Region,
    ) -> Result<PeerFsm> {
        let meta_peer = match find_peer(region, store_id) {
            None => {
                return Err(box_err!(
                    "find no peer for store {} in region {:?}",
                    store_id,
                    region
                ));
            }
            Some(peer) => peer.clone(),
        };
        let peer = Peer::new(store_id, cfg, engines, region, meta_peer)?;
        info!(
            "create peer";
            "region" => peer.tag(),
            "peer_id" => peer.peer_id(),
        );
        let apply_worker_idx = thread_rng().gen_range(0..cfg.apply_pool_size);
        let leader_apply_worker_idx = apply_worker_idx;
        let follower_apply_worker_idx = if cfg.apply_follower_pool_size > 0 {
            cfg.apply_pool_size + thread_rng().gen_range(0..cfg.apply_follower_pool_size)
        } else {
            0
        };
        Ok(PeerFsm {
            peer,
            stopped: false,
            apply_worker_idx,
            leader_apply_worker_idx,
            follower_apply_worker_idx,
            applying_cnt: Arc::new(AtomicU64::new(0)),
            ticker: Ticker::new(cfg),
        })
    }

    // The peer can be created from another node with raft membership changes, and
    // we only know the region_id and peer_id when creating this replicated
    // peer, the region info will be retrieved later after restored snapshot.
    pub fn replicate(
        store_id: u64,
        cfg: &Config,
        engines: Engines,
        region_id: u64,
        region_epoch: metapb::RegionEpoch,
        peer: metapb::Peer,
    ) -> Result<PeerFsm> {
        // We will remove tombstone key when apply snapshot

        let mut region = metapb::Region::default();
        region.set_id(region_id);
        // Peer state key contains version, so that we have to set it to prevent
        // destroyed uninitialized peer from being created by raft msg again.
        region.set_region_epoch(region_epoch);
        let peer = Peer::new(store_id, cfg, engines, &region, peer)?;
        info!(
            "replicate peer";
            "region" => peer.tag(),
            "peer_id" => peer.peer_id(),
        );
        let apply_worker_idx = thread_rng().gen_range(0..cfg.apply_pool_size);
        let leader_apply_worker_idx = apply_worker_idx;
        let follower_apply_worker_idx = if cfg.apply_follower_pool_size > 0 {
            cfg.apply_pool_size + thread_rng().gen_range(0..cfg.apply_follower_pool_size)
        } else {
            0
        };
        Ok(PeerFsm {
            peer,
            stopped: false,
            ticker: Ticker::new(cfg),
            apply_worker_idx,
            leader_apply_worker_idx,
            follower_apply_worker_idx,
            applying_cnt: Arc::new(AtomicU64::new(0)),
        })
    }

    #[inline]
    pub fn may_change_apply_worker(&mut self) {
        if self.follower_apply_worker_idx == 0 {
            return;
        }
        if self.applying_cnt.load(Ordering::SeqCst) > 0 {
            return;
        }
        self.apply_worker_idx = if self.peer.is_leader() {
            self.leader_apply_worker_idx
        } else {
            self.follower_apply_worker_idx
        };
    }

    #[inline]
    pub fn region_id(&self) -> u64 {
        self.peer.region().get_id()
    }

    #[inline]
    pub(crate) fn get_peer(&self) -> &Peer {
        &self.peer
    }

    #[inline]
    pub fn peer_id(&self) -> u64 {
        self.peer.peer_id()
    }

    #[inline]
    pub fn stop(&mut self) {
        self.stopped = true;
    }
}

pub(crate) struct PeerMsgHandler<'a> {
    fsm: &'a mut PeerFsm,
    pub(crate) ctx: &'a mut RaftContext,
}

impl Deref for PeerMsgHandler<'_> {
    type Target = PeerFsm;

    fn deref(&self) -> &Self::Target {
        self.fsm
    }
}

impl DerefMut for PeerMsgHandler<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.fsm
    }
}

impl<'a> PeerMsgHandler<'a> {
    pub(crate) fn new(fsm: &'a mut PeerFsm, ctx: &'a mut RaftContext) -> PeerMsgHandler<'a> {
        PeerMsgHandler { fsm, ctx }
    }

    #[allow(clippy::vec_box)]
    pub fn handle_msgs(&mut self, msgs: &mut Vec<Box<PeerMsg>>) {
        for m in msgs.drain(..) {
            match *m {
                PeerMsg::RaftMessage(msg) => {
                    let msg_type = msg.get_message().get_msg_type();
                    let from_store = msg.get_from_peer().get_store_id();
                    if let Err(e) = self.on_raft_message(msg) {
                        error!(%e;
                            "handle raft message err";
                            "tag" => self.peer.tag(),
                            "peer_id" => self.fsm.peer_id(),
                            "type" => ?msg_type,
                            "from_store" => from_store,
                        );
                    }
                }
                PeerMsg::RaftCommand(cmd) => {
                    self.ctx
                        .raft_metrics
                        .propose_wait_time
                        .observe(duration_to_sec(cmd.send_time.saturating_elapsed()));
                    self.propose_raft_command(cmd.request, cmd.callback, None);
                }
                PeerMsg::Tick => self.on_tick(),
                PeerMsg::ApplyResult(res) => {
                    self.on_apply_result(res);
                }
                PeerMsg::SignificantMsg(msg) => self.on_significant_msg(msg),
                PeerMsg::CasualMessage(msg) => self.on_casual_msg(msg),
                PeerMsg::Start => self.start(),
                PeerMsg::GenerateEngineChangeSet(cs, cb) => {
                    self.on_generate_engine_change_set(cs, cb)
                }
                PeerMsg::ApplySnapshotResult(cs) => {
                    self.on_apply_snapshot_result(cs);
                }
                PeerMsg::Persisted(ready) => {
                    self.on_persisted(ready);
                }
                PeerMsg::PrepareChangeSetResult(res, peer_id) => {
                    self.on_prepared_change_set(res, peer_id);
                }
                PeerMsg::PrepareCommitMergeResult(res, commit_index) => {
                    self.on_prepared_commit_merge(res, commit_index);
                }
                PeerMsg::PrepareTxnFileResult {
                    entry_index,
                    peer_id,
                } => {
                    self.on_prepared_txn_file(entry_index, peer_id);
                }
                PeerMsg::WakeUp(_) => {
                    unreachable!("wakeup idle peer should be handled in raft worker");
                }
                PeerMsg::Idle(_) => {
                    unreachable!("idle peer should be handled in raft idle worker");
                }
                PeerMsg::StoreMsgForWakeUp(store_msg) => {
                    self.ctx.global.router.send_store(store_msg);
                }
            }
        }
    }

    fn on_casual_msg(&mut self, msg: CasualMessage) {
        match msg {
            CasualMessage::SplitRegion {
                region_epoch,
                split_keys,
                callback,
                source,
            } => {
                self.on_prepare_split_region(region_epoch, split_keys, callback, &source);
            }
            CasualMessage::HalfSplitRegion {
                region_epoch,
                policy,
                source,
            } => {
                self.on_schedule_half_split_region(region_epoch, policy, source);
            }
            CasualMessage::DeletePrefix {
                region_version,
                prefix,
                callback,
            } => self.on_delete_prefix(region_version, prefix, callback),
            CasualMessage::IngestFiles { cs, callback } => self.on_ingest_files(cs, callback),
            CasualMessage::RestoreShard { cs, callback } => self.on_restore_shard(cs, callback),
            CasualMessage::TriggerTrimOverBound(parameter) => {
                self.trigger_trim_over_bound(parameter.target_shard.unwrap().ver, parameter)
            }
            CasualMessage::MajorCompact {
                major_compact,
                columnar,
                callback,
            } => self.on_manual_major_compact(major_compact, columnar, callback),
            CasualMessage::UpdateSchemaFile(schema_file) => self.on_update_schema_file(schema_file),
            CasualMessage::CheckLeader {
                shard_ver,
                callback,
            } => self.on_check_leader(shard_ver, callback),
            CasualMessage::ClearColumnar { restore_version } => {
                self.on_clear_columnar(restore_version)
            }
            CasualMessage::TriggerRefreshShardStates => self.on_trigger_refresh_shard_states(),
            CasualMessage::ForceSwitchMemTable {
                current_size,
                callback,
            } => self.on_force_switch_mem_table(current_size, callback),
        }
    }

    fn on_tick(&mut self) {
        if self.fsm.stopped {
            return;
        }
        trace!(
            "tick";
            "peer_id" => self.fsm.peer_id(),
            "tag" => self.peer.tag(),
        );
        self.ticker.tick_clock();
        if self.ticker.is_on_tick(PEER_TICK_RAFT) {
            self.on_raft_base_tick();
        }
        if self.ticker.is_on_tick(PEER_TICK_PD_HEARTBEAT) {
            self.on_pd_heartbeat_tick();
        }
        if self.ticker.is_on_tick(PEER_TICK_SPLIT_CHECK) {
            self.on_split_region_check_tick();
        }
        if self.ticker.is_on_tick(PEER_TICK_MAINTENANCE) {
            self.on_maintenance_tick();
        }
        if self.ticker.is_on_tick(PEER_TICK_RAFT_LOG_GC) {
            self.on_raft_log_gc_tick();
        }
        if self.ticker.is_on_tick(PEER_TICK_CHECK_LONG) {
            self.on_check_long_tick();
        }
    }

    fn start(&mut self) {
        self.ticker.schedule(PEER_TICK_RAFT);
        self.ticker.schedule(PEER_TICK_PD_HEARTBEAT);
        self.ticker.schedule(PEER_TICK_SPLIT_CHECK);
        self.ticker.schedule(PEER_TICK_MAINTENANCE);
        self.ticker.schedule(PEER_TICK_RAFT_LOG_GC);
        self.ticker.schedule(PEER_TICK_CHECK_LONG);
    }

    fn on_significant_msg(&mut self, msg: SignificantMsg) {
        match msg {
            SignificantMsg::StoreUnreachable { store_id } => {
                if let Some(peer_id) = find_peer(self.region(), store_id).map(|p| p.get_id()) {
                    if self.fsm.peer.is_leader() {
                        self.fsm.peer.raft_group.report_unreachable(peer_id);
                    }
                }
            }
        }
    }

    #[inline]
    fn region_id(&self) -> u64 {
        self.fsm.peer.region().get_id()
    }

    #[inline]
    fn region(&self) -> &Region {
        self.fsm.peer.region()
    }

    #[inline]
    fn store_id(&self) -> u64 {
        self.fsm.peer.peer.get_store_id()
    }

    fn on_raft_base_tick(&mut self) {
        if self.peer.pending_remove {
            self.peer.mut_store().flush_cache_metrics();
            return;
        }
        self.ticker.schedule(PEER_TICK_RAFT);
        let peer = &mut self.fsm.peer;
        // When having pending snapshot, if election timeout is met, it can't pass
        // the pending conf change check because first index has been updated to
        // a value that is larger than last index.
        if peer.is_applying_snapshot() || peer.has_pending_snapshot() {
            // need to check if snapshot is applied.
            return;
        }
        if peer.pending_merge_state.is_some() && peer.get_store().initial_flushed() {
            self.ctx
                .global
                .router
                .send_store(StoreMsg::CheckMerge(peer.region_id));
        }
        let snapshot_not_ready_peers = &peer.get_store().snapshot_not_ready_peers;
        if !snapshot_not_ready_peers.borrow().is_empty() {
            let peers = snapshot_not_ready_peers.take();
            if peer.is_leader() {
                let last_index = peer.get_store().last_index();
                for peer_id in peers {
                    if let Some(pr) = peer.raft_group.raft.mut_prs().get_mut(peer_id) {
                        // Increase these peers' next_idx manually to append entries.
                        // pr.next_idx should never equal to pr.matched, otherwise the follower
                        // will keep ignoring the snapshot, never be able to catch up.
                        pr.next_idx = cmp::max(last_index, pr.matched + 1);
                    }
                }
            }
        }
        let raft_election_timeout_ticks = self.ctx.cfg.raft_election_timeout_ticks;
        peer.retry_pending_reads(raft_election_timeout_ticks);
        peer.raft_group.tick();
        if self.ctx.cfg.idle_worker_tick_slow
            && self.ctx.worker_type == WorkerType::Idle
            && !peer.is_leader()
        {
            // We increase the tick interval by 2 for Idle worker but want the election
            // timeout to be the same. So we tick the follower raft group again.
            peer.raft_group.tick();
        }
        peer.mut_store().flush_cache_metrics();
        if peer.need_campaign {
            let _ = peer.raft_group.campaign();
            peer.need_campaign = false;
        }
        peer.check_snapshot_ack_timeout();
    }

    fn on_apply_result(&mut self, res: MsgApplyResult) {
        if res.peer_id != self.peer.peer_id() {
            warn!(
                "{} mismatch apply result peer_id {}",
                self.peer.tag(),
                res.peer_id
            );
            return;
        }
        if self.peer.is_applying_snapshot() {
            debug!(
                "{} applying snapshot, skip handle apply results {:?}",
                self.peer.tag(),
                res.results
            );
            return;
        }
        fail_point!("on_apply_res", |_| {});
        if !self.fsm.peer.pending_apply_results.is_empty() {
            // Apply results should be handled in order but there is a pending one, so
            // delay to handle it.
            self.fsm.peer.pending_apply_results.push(res);
            return;
        }
        if !res.results.is_empty() {
            self.fsm.peer.pending_apply_results.push(res);
            // Some metadata change need to be handled by store FSM, so send the result to
            // it and store FSM will handle all pending results of the peer.
            let region_id = self.region_id();
            let peer_id = self.peer_id();
            self.ctx
                .global
                .router
                .send_store(StoreMsg::ApplyResult { region_id, peer_id });
            return;
        }
        debug!(
            "async apply finish";
            "tag" => self.peer.tag(),
            "peer_id" => self.fsm.peer_id(),
            "res" => ?res,
        );
        self.ctx
            .global
            .engine_total_bytes_written
            .fetch_add(res.metrics.written_bytes, Ordering::Relaxed);
        self.ctx
            .global
            .engine_total_keys_written
            .fetch_add(res.metrics.written_keys, Ordering::Relaxed);
        self.fsm.peer.post_apply(self.ctx, &res);
        if self.fsm.stopped {}
    }

    fn on_transfer_leader_msg(&mut self, msg: &eraftpb::Message) {
        // log_term is set by original leader, represents the term last log is written
        // in, which should be equal to the original leader's term.
        if msg.get_log_term() != self.fsm.peer.term() {
            return;
        }
        if self.fsm.peer.is_leader() {
            let from = match self.fsm.peer.get_peer_from_cache(msg.get_from()) {
                Some(p) => p,
                None => return,
            };
            match self
                .fsm
                .peer
                .ready_to_transfer_leader(self.ctx, msg.get_index(), &from)
            {
                Some(reason) => {
                    info!(
                        "reject to transfer leader";
                        "tag" => self.peer.tag(),
                        "peer_id" => self.fsm.peer_id(),
                        "to" => ?from,
                        "reason" => reason,
                        "index" => msg.get_index(),
                        "last_index" => self.fsm.peer.get_store().last_index(),
                    );
                }
                None => {
                    self.fsm.peer.transfer_leader(&from);
                }
            }
        } else {
            self.fsm.peer.execute_transfer_leader(self.ctx, msg);
        }
    }

    fn on_raft_message(&mut self, mut msg: RaftMessage) -> Result<()> {
        let msg_debug = MsgDebug(&msg);
        debug!(
            "handle raft message";
            "tag" => self.peer.tag(),
            "peer_id" => self.fsm.peer_id(),
            "message" => %msg_debug,
        );
        if !matches!(
            msg.get_message().get_msg_type(),
            MessageType::MsgHeartbeat | MessageType::MsgHeartbeatResponse
        ) {
            self.peer.last_active_time = tikv_util::time::Instant::now_coarse();
        }

        if !self.validate_raft_msg(&msg) {
            return Ok(());
        }
        if self.fsm.peer.pending_remove || self.fsm.stopped || self.peer.is_applying_snapshot() {
            return Ok(());
        }

        if msg.get_is_tombstone() {
            // we receive a message tells us to remove ourself.
            self.handle_gc_peer_msg(&msg);
            return Ok(());
        }

        if msg.has_merge_target() {
            fail_point!("on_has_merge_target", |_| Ok(()));
            self.on_stale_merge(
                msg.get_merge_target().get_id(),
                msg.get_from_peer().get_id(),
            );
            return Ok(());
        }

        if self.check_msg(&msg) {
            return Ok(());
        }

        if msg.has_extra_msg() {
            self.on_extra_message(msg);
            return Ok(());
        }

        let is_snapshot = msg.get_message().has_snapshot();
        let regions_to_destroy = self.check_snapshot(&msg)?;

        let from_peer_id = msg.get_from_peer().get_id();
        self.fsm.peer.insert_peer_cache(msg.take_from_peer());

        let result = if msg.get_message().get_msg_type() == MessageType::MsgTransferLeader {
            self.on_transfer_leader_msg(msg.get_message());
            Ok(())
        } else {
            // This can be a message that sent when it's still a follower. Nevertheless,
            // it's meaningless to continue to handle the request as callbacks are cleared.
            if msg.get_message().get_msg_type() == MessageType::MsgReadIndex
                && self.fsm.peer.is_leader()
                && (msg.get_message().get_from() == raft::INVALID_ID
                    || msg.get_message().get_from() == self.fsm.peer_id())
            {
                self.ctx.raft_metrics.message_dropped.stale_msg.inc();
                return Ok(());
            }
            self.fsm.peer.step(self.ctx, msg.take_message())
        };

        if is_snapshot && self.fsm.peer.has_pending_snapshot() {
            self.destroy_regions_for_snapshot(regions_to_destroy);
        }
        if self.fsm.peer.any_new_peer_catch_up(from_peer_id) {
            self.fsm.peer.heartbeat_pd(self.ctx);
        }
        result
    }

    fn on_extra_message(&mut self, mut msg: RaftMessage) {
        match msg.get_extra_msg().get_type() {
            ExtraMessageType::MsgWantRollbackMerge => {
                self.fsm.peer.maybe_add_want_rollback_merge_peer(
                    msg.get_from_peer().get_id(),
                    msg.get_extra_msg(),
                );
            }
            ExtraMessageType::MsgCheckStalePeerResponse => {
                self.fsm.peer.on_check_stale_peer_response(
                    msg.get_region_epoch().get_conf_ver(),
                    msg.mut_extra_msg().take_check_peers().into(),
                );
            }
            _ => {} // TODO
        }
    }

    // return false means the message is invalid, and can be ignored.
    fn validate_raft_msg(&mut self, msg: &RaftMessage) -> bool {
        let region_id = msg.get_region_id();
        let to = msg.get_to_peer();

        if to.get_store_id() != self.store_id() {
            warn!(
                "store not match, ignore it";
                "tag" => self.peer.tag(),
                "region_id" => region_id,
                "to_store_id" => to.get_store_id(),
                "my_store_id" => self.store_id(),
            );
            self.ctx
                .raft_metrics
                .message_dropped
                .mismatch_store_id
                .inc();
            return false;
        }

        if !msg.has_region_epoch() {
            error!(
                "missing epoch in raft message, ignore it";
                "tag" => self.peer.tag(),
                "region_id" => region_id,
            );
            self.ctx
                .raft_metrics
                .message_dropped
                .mismatch_region_epoch
                .inc();
            return false;
        }

        true
    }

    /// Checks if the message is sent to the correct peer.
    ///
    /// Returns true means that the message can be dropped silently.
    fn check_msg(&mut self, msg: &RaftMessage) -> bool {
        let from_epoch = msg.get_region_epoch();
        let from_store_id = msg.get_from_peer().get_store_id();

        // Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
        // - 1 removes 2, 2 may still send MsgAppendResponse to 1.
        //  We should ignore this stale message and let 2 remove itself after
        //  applying the ConfChange log.
        // - 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
        //  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc
        // itself.
        // - 2 is isolated but can communicate with 3. 1 removes 3.
        //  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
        // - 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove
        //   3.
        //  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
        // - 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
        //  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
        //  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
        //  rejoin the raft group again.
        // - 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4
        //   removes 2.
        //  unlike case e, 2 will be stale forever.
        // TODO: for case f, if 2 is stale for a long time, 2 will communicate with pd
        // and pd will tell 2 is stale, so 2 can remove itself.
        if util::is_epoch_stale(from_epoch, self.fsm.peer.region().get_region_epoch())
            && self.peer.is_initialized()
            && find_peer(self.fsm.peer.region(), from_store_id).is_none()
        {
            self.ctx
                .handle_stale_msg(msg, self.fsm.peer.region().get_region_epoch().clone(), None);
            return true;
        }
        if !self.peer.is_initialized() {
            match msg.get_message().get_msg_type() {
                MessageType::MsgRequestVote | MessageType::MsgRequestPreVote => {
                    // An uninitialized peer may be replaced by split later, if we vote for one peer
                    // and later replaced by split, we may vote for another peer on the same term,
                    // break the raft protocol.
                    return true;
                }
                _ => {}
            }
        }

        let target = msg.get_to_peer();
        match target.get_id().cmp(&self.fsm.peer.peer_id()) {
            cmp::Ordering::Less => {
                info!(
                    "target peer id is smaller, msg maybe stale";
                    "tag" => self.peer.tag(),
                    "peer_id" => self.fsm.peer_id(),
                    "target_peer" => ?target,
                );
                self.ctx.raft_metrics.message_dropped.stale_msg.inc();
                true
            }
            cmp::Ordering::Greater => {
                self.maybe_destroy();
                true
            }
            cmp::Ordering::Equal => false,
        }
    }

    fn handle_gc_peer_msg(&mut self, msg: &RaftMessage) {
        let from_epoch = msg.get_region_epoch();
        if !util::is_epoch_stale(self.fsm.peer.region().get_region_epoch(), from_epoch) {
            return;
        }

        if self.fsm.peer.peer != *msg.get_to_peer() {
            info!(
                "receive stale gc message, ignore.";
                "tag" => self.peer.tag(),
                "peer_id" => self.fsm.peer_id(),
            );
            return;
        }
        // TODO: ask pd to guarantee we are stale now.
        info!(
            "receives gc message, trying to remove";
            "tag" => self.peer.tag(),
            "peer_id" => self.fsm.peer_id(),
            "to_peer" => ?msg.get_to_peer(),
        );
        self.maybe_destroy();
    }

    fn on_stale_merge(&mut self, target_region_id: u64, from_peer_id: u64) {
        if self.fsm.peer.pending_remove {
            return;
        }
        info!(
            "successful merge can't be continued, try to gc stale peer";
            "tag" => self.peer.tag(),
            "from_peer_id" => from_peer_id,
            "to_peer_id" => self.fsm.peer_id(),
            "target_region_id" => target_region_id,
            "merge_state" => ?self.fsm.peer.pending_merge_state,
        );
        // Because of the checking before proposing `PrepareMerge`, which is
        // no `CompactLog` proposal between the smallest commit index and the latest
        // index. If the merge succeed, all source peers are impossible in apply
        // snapshot state and must be initialized.
        // So `maybe_destroy` must succeed here.
        self.maybe_destroy();
    }

    // Returns `Vec<(u64, bool)>` indicated (source_region_id, merge_to_this_peer)
    // if the `msg` doesn't contain a snapshot or this snapshot doesn't conflict
    // with any other snapshots or regions. Otherwise a `SnapKey` is returned.
    fn check_snapshot(&mut self, msg: &RaftMessage) -> Result<Vec<(u64, bool)>> {
        if !msg.get_message().has_snapshot() {
            return Ok(vec![]);
        }
        // TODO(x)
        Ok(vec![])
    }

    fn destroy_regions_for_snapshot(&mut self, regions_to_destroy: Vec<(u64, bool)>) {
        if regions_to_destroy.is_empty() {}
        // TODO(x)
    }

    /// Check if a request is valid if it has valid prepare_merge/commit_merge
    /// proposal.
    fn check_merge_proposal(
        &mut self,
        msg: &RaftCmdRequest,
        store_meta: Option<&mut StoreMeta>,
    ) -> Result<()> {
        if !msg.has_admin_request() {
            return Ok(());
        }
        let admin_req = msg.get_admin_request();
        let region = self.fsm.peer.get_preprocessed_region();
        if admin_req.has_prepare_merge() {
            let store_meta = store_meta.unwrap();
            // Just for simplicity, do not start region merge while in joint state
            if self.fsm.peer.in_joint_state() {
                return Err(box_err!(
                    "{} region in joint state, can not propose merge command, command: {:?}",
                    self.fsm.peer.tag(),
                    msg.get_admin_request()
                ));
            }
            let target_region = admin_req.get_prepare_merge().get_target();
            {
                match store_meta.region_map.get(target_region.get_id()) {
                    Some(r) => {
                        if r != target_region {
                            return Err(box_err!(
                                "target region not matched, skip proposing: {:?} != {:?}",
                                r,
                                target_region
                            ));
                        }
                    }
                    None => {
                        return Err(box_err!(
                            "target region {} doesn't exist.",
                            target_region.get_id()
                        ));
                    }
                }
            }
            if !util::is_sibling_regions(target_region, region) {
                return Err(box_err!(
                    "{:?} and {:?} are not sibling, skip proposing.",
                    target_region,
                    region
                ));
            }
            if !region_on_same_stores(target_region, region) {
                return Err(box_err!(
                    "peers doesn't match {:?} != {:?}, reject merge",
                    region.get_peers(),
                    target_region.get_peers()
                ));
            }

            let id = self.region_id();
            let version = msg.get_header().get_region_epoch().get_version();
            let target_id = target_region.get_id();
            let target_version = target_region.get_region_epoch().get_version();
            let kv = &self.ctx.global.engines.kv;
            let check_result = kv.check_merge(id, version, target_id, target_version)?;

            info!("check_merge_proposal, {:?}", check_result);
            let CheckMergeResult {
                source_overbound,
                target_overbound,
                source_require_empty,
                target_require_empty,
                inconsistent_encryption_key,
                inconsistent_storage_class_spec: inconsistent_storage_class,
            } = check_result;

            // If the two regions belongs to different keyspaces,
            // `inconsistent_encryption_key` will always be false.
            if inconsistent_encryption_key {
                return Err(kvengine::Error::CheckMerge(format!(
                    "shards have inconsistent encryption key, source:{:?}, target:{:?}",
                    IdVer::new(id, version),
                    IdVer::new(target_id, target_version)
                ))
                .into());
            }

            if inconsistent_storage_class {
                return Err(kvengine::Error::CheckMerge(format!(
                    "shards have inconsistent storage classes, source:{:?}, target:{:?}",
                    IdVer::new(id, version),
                    IdVer::new(target_id, target_version)
                ))
                .into());
            }

            if source_overbound || target_overbound {
                let parameter = TrimOverBoundParameter {
                    source_shard: if source_overbound {
                        Some(IdVer::new(id, version))
                    } else {
                        None
                    },
                    target_shard: if target_overbound {
                        Some(IdVer::new(target_id, target_version))
                    } else {
                        None
                    },
                };
                self.trigger_trim_over_bound(version, parameter);
                return Err(kvengine::Error::CheckMerge(format!(
                    "shards have over bound data, source:{source_overbound}, target:{target_overbound}"
                ))
                .into());
            }

            // Shards are required to be empty when they are from different keyspaces.
            // As SSTs have no prefix, merge SSTs from different keyspaces will violate
            // correctness.
            // Also note that the empty status of shard is not reliable, so on commit merge,
            // the data will be cleared in this scene.
            // The checking here is just for more safety.
            if source_require_empty || target_require_empty {
                return Err(kvengine::Error::CheckMerge(format!(
                    "shards from different keyspaces require to be empty, source:{:?}: {:?}, target:{:?}: {:?}",
                    IdVer::new(id, version),
                    region,
                    IdVer::new(target_id, target_version),
                    target_region,
                ))
                .into());
            }

            if let Some(source_meta) = self.fsm.get_peer().get_store().shard_meta.as_ref() {
                // NOTE: source shard meta maybe stale, so we should also check unconverted_l0s
                // after propose.
                if !source_meta.unconverted_l0s.is_empty() {
                    return Err(box_err!(
                        "failed to merge source region with {} unconverted l0s",
                        source_meta.unconverted_l0s.len()
                    ));
                }
            }
        } else if admin_req.has_commit_merge() {
            let source_region = msg.get_admin_request().get_commit_merge().get_source();
            if !util::is_sibling_regions(source_region, region) {
                return Err(box_err!(
                    "{:?} and {:?} should be sibling",
                    source_region,
                    region
                ));
            }
            if !region_on_same_stores(source_region, region) {
                return Err(box_err!(
                    "peers not matched: {:?} {:?}",
                    source_region,
                    region
                ));
            }
        }

        Ok(())
    }

    fn pre_propose_raft_command(
        &mut self,
        msg: &RaftCmdRequest,
    ) -> Result<Option<RaftCmdResponse>> {
        // Check store_id, make sure that the msg is dispatched to the right place.
        if let Err(e) = _util::check_store_id(msg, self.store_id()) {
            self.ctx
                .raft_metrics
                .invalid_proposal
                .mismatch_store_id
                .inc();
            return Err(e);
        }
        if msg.has_status_request() {
            // For status commands, we handle it here directly.
            let resp = self.execute_status_command(msg)?;
            return Ok(Some(resp));
        }

        // Check whether the store has the right peer to handle the request.
        let region_id = self.region_id();
        let leader_id = self.fsm.peer.leader_id();
        let request = msg.get_requests();

        // ReadIndex can be processed on the replicas.
        let is_read_index_request =
            request.len() == 1 && request[0].get_cmd_type() == CmdType::ReadIndex;
        let read_only = !msg.get_requests().is_empty()
            && msg.get_requests().iter().all(|r| {
                matches!(
                    r.get_cmd_type(),
                    CmdType::Get | CmdType::Snap | CmdType::ReadIndex
                )
            });
        let allow_replica_read = read_only && msg.get_header().get_replica_read();
        let flags = WriteBatchFlags::from_bits_check(msg.get_header().get_flags());
        let allow_stale_read = read_only && flags.contains(WriteBatchFlags::STALE_READ);
        if !self.fsm.peer.is_leader()
            && !is_read_index_request
            && !allow_replica_read
            && !allow_stale_read
        {
            self.ctx.raft_metrics.invalid_proposal.not_leader.inc();
            let leader = self.fsm.peer.get_peer_from_cache(leader_id);
            return Err(Error::NotLeader(region_id, leader));
        }
        // peer_id must be the same as peer's.
        if let Err(e) = _util::check_peer_id(msg, self.fsm.peer.peer_id()) {
            self.ctx
                .raft_metrics
                .invalid_proposal
                .mismatch_peer_id
                .inc();
            return Err(e);
        }
        // check whether the peer is initialized.
        if !self.fsm.peer.is_initialized() {
            self.ctx
                .raft_metrics
                .invalid_proposal
                .region_not_initialized
                .inc();
            return Err(Error::RegionNotInitialized(region_id));
        }
        // If the peer is applying snapshot, it may drop some sending messages, that
        // could make clients wait for response until timeout.
        if self.fsm.peer.is_applying_snapshot() {
            self.ctx
                .raft_metrics
                .invalid_proposal
                .is_applying_snapshot
                .inc();
            // TODO: replace to a more suitable error.
            return Err(Error::Other(box_err!(
                "{} peer is applying snapshot",
                self.fsm.peer.tag()
            )));
        }
        // Check whether the term is stale.
        if let Err(e) = _util::check_term(msg, self.fsm.peer.term()) {
            self.ctx.raft_metrics.invalid_proposal.stale_command.inc();
            return Err(e);
        }

        match _util::check_region_epoch(msg, self.fsm.peer.region(), true) {
            Err(Error::EpochNotMatch(m, new_regions)) => {
                // Attach the region which might be split from the current region. But it
                // doesn't matter if the region is not split from the current
                // region. If the region meta received by the TiKV driver is
                // newer than the meta cached in the driver, the meta is
                // updated.
                // TODO(x) add sibling region.
                self.ctx.raft_metrics.invalid_proposal.epoch_not_match.inc();
                Err(Error::EpochNotMatch(m, new_regions))
            }
            Err(e) => Err(e),
            Ok(()) => Ok(None),
        }
    }

    pub(crate) fn propose_raft_command(
        &mut self,
        msg: RaftCmdRequest,
        cb: Callback,
        store_meta: Option<&mut StoreMeta>,
    ) {
        if self.fsm.peer.pending_remove {
            notify_req_region_removed(self.region_id(), cb);
            return;
        }
        self.peer.last_active_time = tikv_util::time::Instant::now_coarse();
        match self.pre_propose_raft_command(&msg) {
            Ok(Some(resp)) => {
                cb.invoke_with_response(resp);
                return;
            }
            Err(e) => {
                debug!(
                    "failed to propose";
                    "tag" => self.peer.tag(),
                    "peer_id" => self.fsm.peer_id(),
                    "message" => ?msg,
                    "err" => %e,
                );
                cb.invoke_with_response(new_error(e));
                return;
            }
            _ => (),
        }

        if let Err(e) = self.check_merge_proposal(&msg, store_meta) {
            warn!(
                "failed to propose merge";
                "tag" => self.peer.tag(),
                "peer_id" => self.fsm.peer_id(),
                "message" => ?msg,
                "err" => %e,
            );
            cb.invoke_with_response(new_error(e));
            return;
        }
        if !msg.get_requests().is_empty() && msg.get_requests()[0].has_ingest_sst() {
            self.propose_ingest_sst(msg, cb);
            return;
        }

        // Note:
        // The peer that is being checked is a leader. It might step down to be a
        // follower later. It doesn't matter whether the peer is a leader or
        // not. If it's not a leader, the proposing command log entry can't be
        // committed.

        let mut resp = RaftCmdResponse::default();
        let term = self.fsm.peer.term();
        bind_term(&mut resp, term);
        self.fsm.peer.propose(self.ctx, cb, msg, resp);

        // TODO: add timeout, if the command is not applied after timeout,
        // we will call the callback with timeout error.
    }

    fn propose_ingest_sst(&mut self, msg: RaftCmdRequest, cb: Callback) {
        // This is a ingest sst request, we need to redirect to worker thread and
        // convert it to cloud engine format.
        let importer = self.ctx.global.importer.clone();
        let router = self.ctx.global.router.clone();
        let kv = self.ctx.global.engines.kv.clone();
        let shard_meta = self.peer.get_store().shard_meta.as_ref().unwrap().clone();
        std::thread::spawn(move || {
            tikv_util::set_current_region(shard_meta.id);
            match convert_sst(kv, importer, &msg, shard_meta) {
                Ok(cs) => {
                    // Make ingest command.
                    let mut cmd = RaftCmdRequest::default();
                    cmd.set_header(msg.get_header().clone());
                    let mut custom_builder = CustomBuilder::new();
                    custom_builder.set_change_set(&cs);
                    cmd.set_custom_request(custom_builder.build());
                    router.send_command(cmd, cb);
                }
                Err(e) => {
                    cb.invoke_with_response(new_error(e));
                }
            }
        });
    }

    fn on_schedule_half_split_region(
        &mut self,
        region_epoch: RegionEpoch,
        policy: CheckPolicy,
        source: &str,
    ) {
        info!(
            "on half split";
            "tag" => self.peer.tag(),
            "peer_id" => self.fsm.peer_id(),
            "policy" => ?policy,
            "source" => source,
        );
        if !self.fsm.peer.is_leader() {
            // region on this store is no longer leader, skipped.
            warn!(
                "not leader, skip";
                "tag" => self.peer.tag(),
                "peer_id" => self.fsm.peer_id(),
            );
            return;
        }

        let region = self.fsm.peer.region();
        if util::is_epoch_stale(&region_epoch, region.get_region_epoch()) {
            warn!(
                "receive a stale halfsplit message";
                "tag" => self.peer.tag(),
                "peer_id" => self.fsm.peer_id(),
            );
            return;
        }
        let split_key = self
            .fsm
            .peer
            .buckets
            .as_mut()
            .map(|buckets| {
                let length = buckets.meta.keys.len();
                if length > 2 {
                    buckets.meta.keys[length / 2].clone()
                } else {
                    Vec::new()
                }
            })
            .unwrap_or_default();
        if !split_key.is_empty() {
            info!(
                "schedule ask split";
                "tag" => self.peer.tag(),
                "peer_id" => self.fsm.peer_id(),
                "split_key" => log_wrappers::hex_encode(split_key.clone()),
            );
            // The split key comes from the bucket key, which is the encoded key.
            self.schedule_ask_split(vec![split_key]);
        } else {
            warn!(
                "no need to schedule, split key not found";
                "tag" => self.peer.tag(),
                "peer_id" => self.fsm.peer_id(),
            );
        }
    }

    fn on_split_region_check_tick(&mut self) -> bool /* should_split */ {
        self.ticker.schedule(PEER_TICK_SPLIT_CHECK);
        if let Some(shard) = self.ctx.global.engines.kv.get_shard(self.region_id()) {
            let estimated_size = shard.get_estimated_size();
            let estimated_entries = shard.get_estimated_entries();
            let estimated_kv_size = shard.get_estimated_kv_size();
            let estimated_columnar_size = shard.get_estimated_columnar_size();
            let estimated_columnar_kv_size = shard.get_estimated_columnar_kv_size();
            // use 1 for empty size as 0 is for unknown size in PD.
            self.peer.peer_stat.approximate_size = cmp::max(estimated_size, 1);
            self.peer.peer_stat.approximate_keys = estimated_entries;
            self.peer.peer_stat.approximate_kv_size = estimated_kv_size;
            self.peer.peer_stat.approximate_columnar_size = estimated_columnar_size;
            self.peer.peer_stat.approximate_columnar_kv_size = estimated_columnar_kv_size;

            self.adjust_peer_stat_for_storage_class(shard.as_ref());

            if !shard.get_initial_flushed() {
                return false;
            }

            let is_leader = self.fsm.peer.is_leader();
            self.check_schema(&shard, is_leader);

            if !is_leader {
                return false;
            }
            // When Lightning or BR is importing data to TiKV, their ingest-request may fail
            // because of region-epoch not matched. So we hope TiKV do not check
            // region size and split region during importing.
            if self.ctx.global.importer.get_mode() == SwitchMode::Import {
                return false;
            }
            if self.ctx.cfg.region_split_keys < 10 {
                // For some tests to be effective, we need to split tiny regions.
                // But tiny region only has mem-table, get_suggest_split_key will return None.
                // So we handle it specially.
                self.split_by_iterate(shard);
                return true;
            }
            if self.fsm.peer.last_bucket_update_meta_sequence != shard.get_meta_sequence() {
                // In case the region is under heavy write, we need to update the bucket more
                // frequently than pd heartbeat.
                self.fsm.peer.update_buckets(self.ctx);
            }
            let mut region_max_size = self.ctx.cfg.region_split_size.0 * 3 / 2;
            let mut region_max_entries = self.ctx.cfg.region_split_keys * 3 / 2;
            if let Some(keyspace_config) = self
                .ctx
                .global
                .engines
                .kv
                .get_keyspace_config(shard.keyspace_id)
            {
                region_max_size =
                    (region_max_size as f64 * keyspace_config.split_size_factor) as u64;
                region_max_entries =
                    (region_max_entries as f64 * keyspace_config.split_size_factor) as u64;
            }
            raftstore::coprocessor::metrics::REGION_SIZE_HISTOGRAM.observe(estimated_size as f64);
            raftstore::coprocessor::metrics::REGION_KEYS_HISTOGRAM
                .observe(estimated_entries as f64);

            let table_count = shard.get_columnar_table_ids().len();
            let max_tables_threshold = fail::eval("max_columnar_tables_threshold", |t| {
                t.and_then(|s: String| s.parse::<usize>().ok())
            })
            .flatten()
            .unwrap_or(LARGE_NUM_COLUMNAR_TABLES_IN_SHARD * 3);
            if estimated_size >= region_max_size
                || estimated_entries >= region_max_entries
                || table_count > max_tables_threshold
            {
                if let Some(k) = shard.get_suggest_split_key(self.ctx.cfg.region_bucket_size.0) {
                    info!(
                        "region {} split, estimated size {}, estimated entries {}, table count {}, max_size {}",
                        self.peer.tag(),
                        estimated_size,
                        estimated_entries,
                        table_count,
                        region_max_size,
                    );
                    self.schedule_ask_split(vec![Key::from_raw(k.chunk()).into_encoded()]);
                    return true;
                }
            }
        }
        false
    }

    fn adjust_peer_stat_for_storage_class(&mut self, shard: &Shard) {
        if shard.get_storage_class_spec().can_be_ia() {
            let region_split_size = self.ctx.cfg.region_split_size.0;
            let ia_kv_size_discount = self.ctx.cfg.ia_kv_size_discount;
            let peer_stat = &mut self.peer.peer_stat;

            // Adjust region size to prevent merge when the region is on the boundary of
            // table.
            if is_table_boundary_key(shard.inner_start())
                || is_table_boundary_key(shard.inner_end())
            {
                peer_stat.approximate_size =
                    cmp::max(peer_stat.approximate_size, region_split_size);
            }

            peer_stat.approximate_kv_size = Self::calc_kv_size_for_storage_class(
                peer_stat.approximate_kv_size,
                shard.get_estimated_ia_kv_size(),
                ia_kv_size_discount,
            );
        }
    }

    fn calc_kv_size_for_storage_class(
        estimated_kv_size: u64,
        estimated_ia_kv_size: u64,
        ia_kv_size_discount: f64,
    ) -> u64 {
        estimated_kv_size.saturating_sub(estimated_ia_kv_size)
            + (estimated_ia_kv_size as f64 * ia_kv_size_discount) as u64
    }

    fn schedule_ask_split(&mut self, split_keys: Vec<Vec<u8>>) {
        let task = PdTask::AskBatchSplit {
            region: self.region().clone(),
            split_keys,
            peer: self.peer.peer.clone(),
            right_derive: true,
            callback: Callback::None,
        };
        self.ctx.global.pd_scheduler.schedule(task).unwrap();
    }

    // TODO: support async (or split by block keys ?)
    fn split_by_iterate(&mut self, shard: Arc<Shard>) {
        let split_keys = self.ctx.cfg.region_split_keys;
        let split_max_keys = split_keys * 3 / 2;
        let snap = shard.new_snap_access();
        let mut iter = snap.new_iterator(0, false, false, None, true);
        iter.rewind();
        let mut i = 0;
        let mut keys = vec![];
        while iter.valid() {
            if i > 0 && i % split_keys == 0 {
                keys.push(Key::from_raw(iter.key()).into_encoded());
            }
            i += 1;
            iter.next();
        }
        if i <= split_max_keys {
            return;
        }
        info!("region {} split by iterate", self.peer.tag());
        self.schedule_ask_split(keys);
    }

    // For idle regions, we need tick to trigger switch mem-table and shrink
    // lock_cache to reduce memory consumption. Only the applier knows how large
    // the mem-table is and when the mem-table has been switched, so we don't
    // need to check anything, just send a message.
    fn on_maintenance_tick(&mut self) {
        self.ticker.schedule(PEER_TICK_MAINTENANCE);
        // We also send the message to follower to let follower do periodic maintenance
        // task.
        let region_id = self.region_id();
        self.ctx
            .apply_msgs
            .msgs
            .push(ApplyMsg::Maintenance { region_id });
        self.check_gc_tombstones();
    }

    fn on_prepare_split_region(
        &mut self,
        region_epoch: metapb::RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        cb: Callback,
        source: &str,
    ) {
        if let Err(e) = self.validate_split_region(&region_epoch, &split_keys) {
            info!(
                "prepare split error";
                "tag" => self.peer.tag(),
                "peer_id" => self.fsm.peer_id(),
                "split_keys" => %util::KeysInfoFormatter(split_keys.iter()),
                "source" => source,
                "error" => ?e,
            );
            let (resp, key_errs) = new_with_key_error(e);
            cb.invoke_with_response_ext(resp, key_errs);
            return;
        }
        info!(
            "on split";
            "tag" => self.peer.tag(),
            "peer_id" => self.fsm.peer_id(),
            "split_keys" => %util::KeysInfoFormatter(split_keys.iter()),
            "source" => source,
        );
        let task = PdTask::AskBatchSplit {
            region: self.region().clone(),
            split_keys,
            peer: self.peer.peer.clone(),
            right_derive: true,
            callback: cb,
        };
        self.ctx.global.pd_scheduler.schedule(task).unwrap();
    }

    fn validate_split_region(
        &mut self,
        epoch: &metapb::RegionEpoch,
        split_keys: &[Vec<u8>],
    ) -> Result<()> {
        let tag = self.peer.tag();
        if split_keys.is_empty() {
            error!(
                "no split key is specified.";
                "tag" => tag,
                "peer_id" => self.fsm.peer_id(),
            );
            return Err(box_err!("{} no split key is specified.", tag));
        }
        if split_keys.len() > self.ctx.cfg.split_region_max_keys {
            error!(
                "number of split keys exceeds limit";
                "tag" => tag,
            );
            return Err(box_err!("{} number of split keys exceeds limit", tag));
        }
        for key in split_keys {
            if key.is_empty() {
                error!(
                    "split key should not be empty!!!";
                    "tag" => tag,
                    "peer_id" => self.fsm.peer_id(),
                );
                return Err(box_err!("{} split key should not be empty", tag));
            }
            if let Err(err) = decode_bytes(&mut key.as_slice(), false) {
                error!(
                    "split key decode error";
                    "tag" => tag,
                    "peer_id" => self.fsm.peer_id(),
                    "err" => ?err,
                );
                return Err(box_err!("{} split key decode failed", tag));
            }
        }
        if !self.fsm.peer.is_leader() {
            // region on this store is no longer leader, skipped.
            info!(
                "not leader, skip.";
                "tag" => tag,
                "peer_id" => self.fsm.peer_id(),
            );
            return Err(Error::NotLeader(
                self.region_id(),
                self.fsm.peer.get_peer_from_cache(self.fsm.peer.leader_id()),
            ));
        }

        let region = self.fsm.peer.region();
        let latest_epoch = region.get_region_epoch();

        // This is a little difference for `check_region_epoch` in region split case.
        // Here we just need to check `version` because `conf_ver` will be update
        // to the latest value of the peer, and then send to PD.
        if latest_epoch.get_version() != epoch.get_version() {
            info!(
                "epoch changed, retry later";
                "tag" => tag,
                "peer_id" => self.fsm.peer_id(),
                "prev_epoch" => ?region.get_region_epoch(),
                "epoch" => ?epoch,
            );
            return Err(Error::EpochNotMatch(
                format!(
                    "{} epoch changed {:?} != {:?}, retry later",
                    tag, latest_epoch, epoch
                ),
                vec![region.to_owned()],
            ));
        }
        if !self.peer.get_store().initial_flushed() {
            return Err(Error::Transport(DiscardReason::Full));
        }

        if let Some(shard_meta) = self.peer.get_store().shard_meta.as_ref() {
            if shard_meta.has_txn_file_locks() {
                info!("{} prepare split: txn file locks exist, retry later", tag;
                    "txn_file_locks" => %shard_meta.txn_file_locks(),
                );
                let raw_key =
                    decode_bytes(&mut split_keys.first().unwrap().as_slice(), false).unwrap();
                let key_errs = shard_meta
                    .txn_file_locks()
                    .get_key_errors(&raw_key)
                    .map_err(|err| {
                        error!("{} validate_split_region: get txn file key errors failed", tag;
                        "txn_file_locks" => ?shard_meta.txn_file_locks(),
                        "err" => ?err);
                        Error::Other(box_err!("get txn file key errors failed"))
                    })?;
                return Err(Error::KeyErrors(key_errs));
            }
        }

        Ok(())
    }

    fn on_delete_prefix(&mut self, region_version: u64, prefix: Vec<u8>, callback: Callback) {
        if !self.peer.is_leader() {
            // As delete prefix requests are sent to all stores, not leader error can be
            // ignored.
            callback.invoke_with_response(RaftCmdResponse::default());
            return;
        }
        let tag = self.peer.tag();
        let id_ver = tag.id_ver;
        if region_version != id_ver.ver() {
            warn!("{} on_delete_prefix: version not match", tag);
            callback.invoke_with_response(new_error(Error::EpochNotMatch(
                "on_delete_prefix: version not match".to_owned(),
                vec![self.fsm.peer.region().clone()],
            )));
            return;
        }
        let mut cmd = self.new_raft_cmd_request();
        let mut cs = kvengine::new_change_set(id_ver.id(), id_ver.ver());
        cs.set_property_key(DEL_PREFIXES_KEY.to_string());
        cs.set_property_value(prefix);
        cs.set_property_merge(true);
        let mut custom_builder = CustomBuilder::new();
        custom_builder.set_change_set(&cs);
        cmd.set_custom_request(custom_builder.build());
        self.propose_raft_command(cmd, callback, None);
    }

    fn on_manual_major_compact(&mut self, major_compact: bool, columnar: bool, callback: Callback) {
        if !self.peer.is_leader() {
            callback.invoke_with_response(RaftCmdResponse::default());
            return;
        }
        let id_ver = self.peer.tag().id_ver;
        let mut cmd = self.new_raft_cmd_request();
        let mut cs = kvengine::new_change_set(id_ver.id(), id_ver.ver());
        cs.set_property_key(MANUAL_MAJOR_COMPACTION.to_string());
        if major_compact {
            if columnar {
                cs.set_property_value(MANUAL_MAJOR_COMPACTION_ENABLE_COLUMNAR.to_vec());
            } else {
                cs.set_property_value(MANUAL_MAJOR_COMPACTION_ENABLE.to_vec());
            }
        } else {
            cs.set_property_value(MANUAL_MAJOR_COMPACTION_DISABLE.to_vec());
        }
        let mut custom_builder = CustomBuilder::new();
        custom_builder.set_change_set(&cs);
        cmd.set_custom_request(custom_builder.build());
        self.propose_raft_command(cmd, callback, None);
    }

    fn on_force_switch_mem_table(&mut self, current_size: u64, callback: Callback) {
        if !self.peer.is_leader() {
            callback.invoke_with_response(RaftCmdResponse::default());
            return;
        }
        // Propose a command with TYPE_SWITCH_MEM_TABLE
        let mut req = self.new_raft_cmd_request();
        let mut builder = CustomBuilder::new();
        builder.set_switch_mem_table(current_size);
        req.set_custom_request(builder.build());
        self.propose_raft_command(req, callback, None);
    }

    fn on_update_schema_file(&mut self, schema_file: SchemaFile) {
        if !self.peer.is_leader() {
            return;
        }
        let shard_meta = self.peer.get_store().shard_meta.as_ref().unwrap();
        let overlap = schema_file.overlap(
            &shard_meta.range.outer_start,
            &shard_meta.range.outer_end,
            shard_meta.range.keyspace_id,
        );
        let tag = self.peer.tag();
        let mut change_set = kvengine::new_change_set(shard_meta.id, shard_meta.ver);
        let update_schema_meta = change_set.mut_update_schema_meta();
        update_schema_meta.set_file_id(schema_file.get_file_id());
        update_schema_meta.set_version(schema_file.get_version());
        if shard_meta.schema.restore_ver() != schema_file.get_restore_version() {
            info!(
                "{} skip stale schema file id {}, restore_version not match, expected {}, actual {}",
                tag,
                schema_file.get_file_id(),
                shard_meta.schema.restore_ver(),
                schema_file.get_restore_version()
            );
            return;
        }
        update_schema_meta.set_restore_version(schema_file.get_restore_version());
        info!(
            "{} on_update_schema_file {:?}, file_ver: {}",
            tag,
            shard_meta.schema,
            schema_file.get_version()
        );
        let cur_ver = shard_meta.schema.file_ver();
        let new_ver = schema_file.get_version();
        let valid = shard_meta.schema.is_valid();
        // If the schema file is valid, we should skip if the new_ver is not newer.
        // In `clear_columnar` case, the schema file is invalid and we need to update
        // the schema file with the same version.
        let should_skip = if valid {
            cur_ver >= new_ver
        } else {
            cur_ver > new_ver
        };
        if should_skip {
            info!(
                "{} skip stale schema file (cur_ver={}, new_ver={}, valid={})",
                tag, cur_ver, new_ver, valid
            );
            return;
        }
        if !overlap {
            if shard_meta.schema.is_valid() {
                info!("{} update schema file, remove non overlap", tag);
                update_schema_meta.set_file_id(0);
            } else {
                return;
            }
        }
        let kv = self.ctx.global.engines.kv.clone();
        // Ensure the shard has been initial flushed. If not initial flushed, reject the
        // proposal. Update schema file will be proposed again by schema manager.
        if let Some(shard) = kv.get_shard(shard_meta.id) {
            if !shard.get_initial_flushed() {
                warn!(
                    "{} shard not initial flushed, reject propose update schema file {}",
                    tag,
                    schema_file.get_file_id()
                );
                return;
            }
        }
        info!(
            "{} propose update schema file {:?}",
            tag, update_schema_meta
        );
        self.propose_change_set(change_set, Callback::None);
    }

    fn check_schema(&mut self, shard: &Arc<Shard>, is_leader: bool) {
        let tag = self.peer.tag();
        let shard_meta = self.peer.get_store().shard_meta.as_ref().unwrap();
        let schema_meta = &shard_meta.schema;
        let schema_file = shard.get_schema_file();
        let schema_file_id = schema_file.as_ref().map(|x| x.get_file_id());
        debug!("{} check schema", tag;
            "schema_file_meta" => ?schema_meta,
            "meta.spec" => ?shard_meta.get_storage_class_spec(),
            "shard.spec" => ?shard.get_storage_class_spec(),
            "checked_schema_ver" => shard.get_checked_schema_ver(),
            "schema_file" => ?schema_file_id,
        );

        if !schema_file_is_matched_with_meta(schema_file.as_ref(), schema_meta) {
            // Wait for schema file to be updated.
            debug!("{} check schema: skip, schema file is stale", tag;
                "schema_meta" => ?schema_meta, "schema_file" => ?schema_file_id);
            return;
        }

        if is_leader && !shard_is_matched_with_meta(shard.as_ref(), shard_meta) {
            let task = SchemaTask::StorageClass {
                region: self.region().clone(),
                schema_meta: schema_meta.clone(),
            };
            if let Err(e) = self.ctx.global.schema_scheduler.schedule(task) {
                info!("{} check schema: schedule update sc task failed", tag; "err" => ?e, "schema_meta" => ?schema_meta);
            }
        } else if shard.should_try_storage_class_transition() {
            let tables_transited_to_ia = shard.try_transit_storage_class();
            shard.set_last_transit_storage_class_instant_to_now();

            if tables_transited_to_ia > 0 {
                shard.refresh_estimated_size_and_entries();
                report_transitions(tables_transited_to_ia);
                info!("{} check schema: transit storage class", tag;
                    "tables_transited_to_ia" => tables_transited_to_ia);
            }
        } else {
            debug!("{} check schema: skip, shard is up-to-date", tag);
        };
    }

    fn on_check_leader(&mut self, shard_ver: u64, callback: Callback) {
        let mut resp = RaftCmdResponse::default();
        // If the peer is not leader or not applied to current term, return not_leader
        // error. The client should sleep and retry.
        let is_leader = fail::eval("check_leader_peer_is_leader", |t| {
            t.and_then(|s: String| s.parse::<bool>().ok())
        })
        .flatten()
        .unwrap_or(self.peer.is_leader());
        if !is_leader || !self.peer.has_applied_to_current_term() {
            let header = resp.mut_header();
            let error = header.mut_error();
            let not_leader = error.mut_not_leader();
            not_leader.set_region_id(self.region_id());
            let leader_id = self.peer.leader_id();
            if leader_id != 0 {
                if let Some(p) = self.region().get_peers().iter().find(|p| p.id == leader_id) {
                    not_leader.set_leader(p.clone());
                }
            }
        } else if shard_ver != 0 && self.region().get_region_epoch().version != shard_ver {
            let header = resp.mut_header();
            let error = header.mut_error();
            let epoch_not_match = error.mut_epoch_not_match();
            epoch_not_match
                .mut_current_regions()
                .push(self.region().clone());
        }
        callback.invoke_with_response(resp);
    }

    fn on_clear_columnar(&mut self, restore_version: Option<u64>) {
        if !self.peer.is_leader() {
            return;
        }
        let shard_meta = self.peer.get_store().shard_meta.as_ref().unwrap();
        if !shard_meta.schema.has_value() {
            return;
        }
        let mut change_set = kvengine::new_change_set(shard_meta.id, shard_meta.ver);
        let clear_columnar = change_set.mut_clear_columnar_with_restore_version();
        if let Some(restore_version) = restore_version {
            clear_columnar.set_restore_version(restore_version);
        } else {
            clear_columnar.set_restore_version(shard_meta.schema.restore_ver());
        }
        info!("{} propose clear_columnar", self.peer.tag());
        self.propose_change_set(change_set, Callback::None);
    }

    fn on_ingest_files(&mut self, cs: kvenginepb::ChangeSet, callback: Callback) {
        if !self.peer.is_leader() {
            let mut resp = RaftCmdResponse::default();
            let header = resp.mut_header();
            let error = header.mut_error();
            let not_leader = error.mut_not_leader();
            not_leader.set_region_id(self.region_id());
            let leader_id = self.peer.leader_id();
            if leader_id != 0 {
                if let Some(p) = self.region().get_peers().iter().find(|p| p.id == leader_id) {
                    not_leader.set_leader(p.clone());
                }
            }
            callback.invoke_with_response(resp);
            return;
        }
        if !self.get_peer().get_store().initial_flushed() {
            let mut resp = RaftCmdResponse::default();
            let not_initialized = resp.mut_header().mut_error().mut_region_not_initialized();
            not_initialized.set_region_id(self.region_id());
            callback.invoke_with_response(resp);
            return;
        }
        if cs.get_shard_ver() != self.region().get_region_epoch().get_version() {
            let mut resp = RaftCmdResponse::default();
            let regions = resp
                .mut_header()
                .mut_error()
                .mut_epoch_not_match()
                .mut_current_regions();
            regions.push(self.region().clone());
            callback.invoke_with_response(resp);
            return;
        }

        // Note: overlap is checked in preprocess stage.

        let mut cmd = self.new_raft_cmd_request();
        let mut custom_builder = CustomBuilder::new();
        custom_builder.set_change_set(&cs);
        cmd.set_custom_request(custom_builder.build());
        self.propose_raft_command(cmd, callback, None);
    }

    fn trigger_trim_over_bound(&mut self, shard_ver: u64, parameter: TrimOverBoundParameter) {
        if !self.peer.is_leader() {
            return;
        }
        let tag = self.peer.tag();
        let id_ver = tag.id_ver;
        if shard_ver != id_ver.ver() {
            warn!("{} trigger_trim_over_bound version not match", tag);
            return;
        }
        let mut cmd = self.new_raft_cmd_request();
        let mut custom_builder = CustomBuilder::new();
        custom_builder.set_trigger_trim_over_bound(&parameter);
        cmd.set_custom_request(custom_builder.build());

        let cb = Callback::write(Box::new(move |resp| {
            if resp.response.get_header().has_error() {
                let err_msg = resp.response.get_header().get_error().get_message();
                warn!("{} failed to trigger trim_over_bound: {:?}", tag, err_msg);
            } else {
                info!(
                    "{} trigger trim_over_bound successfully: {:?}",
                    tag, parameter
                );
            }
        }));
        self.propose_raft_command(cmd, cb, None);
    }

    fn on_trigger_refresh_shard_states(&mut self) {
        if !self.peer.is_leader() {
            return;
        }
        self.ctx
            .apply_msgs
            .msgs
            .push(ApplyMsg::TriggerRefreshShardStates);
    }

    fn on_pd_heartbeat_tick(&mut self) {
        self.ticker.schedule(PEER_TICK_PD_HEARTBEAT);
        self.fsm.peer.check_peers();
        self.update_max_lag_metrics();

        if !self.fsm.peer.is_leader() {
            return;
        }
        self.fsm.peer.heartbeat_pd(self.ctx);
    }

    fn on_generate_engine_change_set(&mut self, cs: kvenginepb::ChangeSet, cb: Callback) {
        let tag = self.peer.tag();
        info!("generate meta change event {:?}", &cs; "region" => tag);
        self.propose_change_set(cs, cb);
    }

    fn propose_change_set(&mut self, cs: kvenginepb::ChangeSet, cb: Callback) {
        let tag = self.peer.tag();
        let mut req = self.new_raft_cmd_request();
        let mut builder = CustomBuilder::new();
        builder.set_change_set(&cs);
        let custom_req = builder.build();
        req.set_custom_request(custom_req);
        let router = self.ctx.global.router.clone();
        let kv = self.ctx.global.engines.kv.clone();
        let propose_cb = Callback::write(Box::new(move |resp| {
            if resp.response.get_header().has_error() {
                let err_msg = resp.response.get_header().get_error().get_message();
                warn!("{} failed to propose engine change set", tag; "err" => ?err_msg, "cs" => ?cs);
                if err_msg.contains("raft: proposal dropped") {
                    // Proposal may dropped due to leader transfer in progress.
                    router.send_store(StoreMsg::GenerateEngineChangeSet(cs, cb));
                } else {
                    // Reset the state of flush worker or compactor.
                    kv.meta_committed(&cs, true);
                    cb.invoke_with_response(resp.response);
                }
            } else {
                info!("{} proposed meta change event", tag);
                cb.invoke_with_response(resp.response);
            }
        }));
        self.propose_raft_command(req, propose_cb, None);
    }

    fn new_raft_cmd_request(&self) -> RaftCmdRequest {
        let mut req = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(self.region_id());
        header.set_peer(self.peer.peer.clone());
        header.set_region_epoch(self.region().get_region_epoch().clone());
        header.set_term(self.peer.term());
        req.set_header(header);
        req
    }

    fn on_apply_snapshot_result(&mut self, change: kvenginepb::ChangeSet) {
        let tag = self.peer.tag();
        debug_assert!(change.has_snapshot(), "{}", tag);
        assert_eq!(
            change.shard_ver,
            self.peer
                .get_preprocessed_region()
                .get_region_epoch()
                .get_version(),
            "{} snapshot change set version not match, region {:?}, is_applying_snapshot {}, cs {:?}",
            tag,
            self.peer.get_preprocessed_region(),
            self.peer.get_store().is_applying_snapshot(),
            change
        );

        if self.peer.mut_store().is_applying_snapshot() {
            self.peer.mut_store().snap_state = SnapState::Relax;
            // After applying a snapshot, merge is rollbacked implicitly.
            self.peer.clear_merge_in_mem_data();
            let apply_state = RaftApplyState::from_snapshot(change.get_snapshot());
            let apply_result = MsgApplyResult {
                peer_id: self.peer.peer_id(),
                results: VecDeque::new(),
                apply_state,
                metrics: ApplyMetrics::default(),
                bucket_stat: None,
            };
            info!(
                "{} apply snapshot finished, snapshot index {}",
                tag, apply_state.applied_index
            );
            self.fsm.peer.post_apply(self.ctx, &apply_result);
            let on_apply_snapshot_msgs =
                std::mem::take(&mut self.peer.mut_store().on_apply_snapshot_msgs);
            for msg in on_apply_snapshot_msgs {
                if let Err(err) = self.ctx.global.trans.send(msg) {
                    error!("failed to send on apply snapshot msg {:?}", err);
                }
            }
        }
    }

    fn on_persisted(&mut self, ready: PersistReady) {
        if ready.peer_id != self.fsm.peer_id() {
            error!(
                "peer id not match";
                "tag" => self.peer.tag(),
                "peer_id" => self.fsm.peer_id(),
                "persisted_peer_id" => ready.peer_id,
                "persisted_number" => ready.ready_number,
            );
            return;
        }
        self.fsm.peer.on_persist_ready(self.ctx, ready.ready_number);
    }

    pub(crate) fn on_prepared_change_set(
        &mut self,
        res: kvengine::Result<kvengine::ChangeSet>,
        peer_id: u64,
    ) {
        if res.is_err() {
            // TODO(x): properly handle this error.
            panic!(
                "{} failed to prepare change set {:?}",
                self.peer.tag(),
                res.unwrap_err()
            );
        }
        // If peer_msg is from a stale peer, ignore it. prepare_change_set in kvengine
        // will be return empty change set if old peer has be destroyed.
        if peer_id != self.peer_id() {
            warn!(
                "{} peer id not match {} != {}, skip on_prepared_change_set",
                self.peer.tag(),
                peer_id,
                self.peer_id()
            );
            return;
        }
        self.ctx
            .apply_msgs
            .msgs
            .push(ApplyMsg::ApplyChangeSet(res.unwrap()));
    }

    pub(crate) fn on_prepared_commit_merge(
        &mut self,
        res: kvengine::Result<kvengine::ChangeSet>,
        commit_index: u64,
    ) {
        if res.is_err() {
            // TODO(x): properly handle this error.
            panic!(
                "{} failed to prepare change set {:?}",
                self.peer.tag(),
                res.unwrap_err()
            );
        }
        self.ctx.apply_msgs.msgs.push(ApplyMsg::ResumeCommitMerge {
            source: res.unwrap(),
            commit_index,
        });
    }

    pub(crate) fn on_prepared_txn_file(&mut self, entry_index: u64, peer_id: u64) {
        // Ignore peer_msg from a stale peer.
        if peer_id != self.peer_id() {
            warn!(
                "{} peer id not match {} != {}, skip on_prepared_txn_file, entry_index {}",
                self.peer.tag(),
                peer_id,
                self.peer_id(),
                entry_index,
            );
            return;
        }
        self.ctx
            .apply_msgs
            .msgs
            .push(ApplyMsg::ResumeTxnFile(entry_index));
    }

    pub(crate) fn update_max_lag_metrics(&mut self) {
        if !self.fsm.peer.is_leader() {
            return;
        }
        let last_idx = self.peer.get_store().last_index();
        let replicated_idx = self
            .peer
            .raft_group
            .raft
            .prs()
            .iter()
            .map(|(_, pr)| pr.matched)
            .min()
            .unwrap_or_default();
        // When an election happened or a new peer is added, replicated_idx can be 0.
        if replicated_idx > 0 {
            assert!(
                last_idx >= replicated_idx,
                "expect last index {} >= replicated index {}",
                last_idx,
                replicated_idx
            );
            raftstore::store::metrics::REGION_MAX_LOG_LAG
                .observe((last_idx - replicated_idx) as f64);
        }
    }

    fn on_raft_log_gc_tick(&mut self) {
        self.ticker.schedule(PEER_TICK_RAFT_LOG_GC);
        let region_id = self.region_id();
        let engines = &self.ctx.global.engines;
        if !self.peer.is_initialized()
            || self.peer.is_applying_snapshot()
            || engines.raft.has_dependents(region_id)
            || self.peer.pending_merge_state.is_some()
        {
            return;
        }

        let last_idx = self.peer.get_store().last_index();
        let replicated_idx = if self.peer.is_leader() {
            self.peer
                .raft_group
                .raft
                .prs()
                .iter()
                .filter_map(|(peer_id, pr)| {
                    // Don't keep raft logs for down peer. It may be too long(default 10mins).
                    (!self.peer.down_peer_ids.contains(peer_id)).then_some(pr.matched)
                })
                .min()
                .unwrap_or(last_idx)
        } else {
            // Followers can't know the progress of other replicas, so they truncate logs as
            // many as possible, but it may result in snapshot transport when
            // becomes leader. One solution is that leader propagates truncated
            // index to followers.
            last_idx
        };
        let peer_id = self.peer_id();
        let size_limit_index = engines.raft.index_to_truncate_to_size(
            peer_id,
            self.ctx.cfg.raft_log_gc_size_limit.unwrap().0 as usize,
        );

        // If raft logs occupies too much memory, truncates them regardless of lagged
        // replicas to avoid OOM.
        let mut to_truncate_idx = cmp::max(size_limit_index, replicated_idx);
        // Shouldn't truncate unapplied logs.
        let applied_idx = self.peer.get_store().applied_index();
        to_truncate_idx = cmp::min(to_truncate_idx, applied_idx);

        // Shouldn't truncate unpersisted logs.
        let mut persisted_log_idx = self.peer.get_store().data_persisted_log_index().unwrap();
        if persisted_log_idx < to_truncate_idx && applied_idx == last_idx {
            // It's possible that the shard mem-table is empty but the data sequence is
            // smaller. All the entries between persisted_log_idx and last_idx are no kv
            // entries, there is no chance to trigger flush, the data sequence fall behind.
            let Some(shard) = self.ctx.global.engines.kv.get_shard(self.region_id()) else {
                return;
            };
            if shard.data_all_persisted() {
                // All raft logs are applied and mem-table is empty, we can safely truncate to
                // 'to_truncate_idx'. But we don't want to pay the cost of
                // rewriting shard meta on each leader change. So we tolerate
                // config.raft_log_gc_no_kv_count to reduce the frequency of shard meta
                // rewriting.
                let truncate_no_kv_logs =
                    persisted_log_idx + self.ctx.cfg.raft_log_gc_no_kv_count < to_truncate_idx;
                // change set raft logs are less frequent, we can always truncate it.
                let truncate_change_set_logs = shard.get_write_sequence() > persisted_log_idx;
                if truncate_no_kv_logs || truncate_change_set_logs {
                    // Advance the data sequence to to_truncate_idx.
                    let store = self.peer.mut_store();
                    let term = store.term(to_truncate_idx).unwrap();
                    // workaround for borrow checker.
                    let mut shard_meta = store.shard_meta.take().unwrap();
                    shard_meta.data_sequence = to_truncate_idx;
                    shard_meta.set_property(TERM_KEY, &term.to_le_bytes());
                    if self.ctx.cfg.enable_kv_engine_meta_diff {
                        write_engine_meta_diff(
                            &self.ctx.global.engines.raft,
                            &mut self.ctx.raft_wb,
                            peer_id,
                            &shard_meta,
                            None,
                            self.ctx.cfg.kv_engine_meta_diff_rewrite_percent,
                        );
                    } else {
                        write_engine_meta(&mut self.ctx.raft_wb, peer_id, &shard_meta);
                    }
                    self.peer.mut_store().shard_meta = Some(shard_meta);
                    persisted_log_idx = to_truncate_idx;
                    info!(
                        "{} shard meta advanced data sequence to {}",
                        self.peer.tag(),
                        to_truncate_idx,
                    );
                }
            }
        }
        to_truncate_idx = cmp::min(to_truncate_idx, persisted_log_idx);

        // Check if we need to handle pending_truncate.
        if let Some((_, idx)) = self.peer.pending_truncate {
            if idx <= applied_idx {
                to_truncate_idx = idx;
                self.peer.pending_truncate = None;
            }
        }

        let truncated_idx = self.peer.get_store().truncated_index();
        if to_truncate_idx > truncated_idx {
            let to_truncate_term = self.peer.get_store().term(to_truncate_idx).unwrap();
            // mem::take and mem::replace is a workaround for borrow checker.
            let mut raft_wb = std::mem::take(&mut self.ctx.raft_wb);
            self.peer.mut_store().truncate_raft_log(
                &mut raft_wb,
                to_truncate_idx,
                to_truncate_term,
            );
            let _ = std::mem::replace(&mut self.ctx.raft_wb, raft_wb);
            info!(
                "{} truncate raft logs to {}, term {}",
                self.peer.tag(),
                to_truncate_idx,
                to_truncate_term,
            );
        }
    }

    fn on_check_peer_stale_state(&mut self) {
        if self.fsm.peer.is_applying_snapshot() || self.fsm.peer.has_pending_snapshot() {
            return;
        }

        // If this peer detects the leader is missing for a long long time,
        // it should consider itself as a stale peer which is removed from
        // the original cluster.
        // This most likely happens in the following scenario:
        // At first, there are three peer A, B, C in the cluster, and A is leader.
        // Peer B gets down. And then A adds D, E, F into the cluster.
        // Peer D becomes leader of the new cluster, and then removes peer A, B, C.
        // After all these peer in and out, now the cluster has peer D, E, F.
        // If peer B goes up at this moment, it still thinks it is one of the cluster
        // and has peers A, C. However, it could not reach A, C since they are removed
        // from the cluster or probably destroyed.
        // Meantime, D, E, F would not reach B, since it's not in the cluster anymore.
        // In this case, peer B would notice that the leader is missing for a long time,
        // and it would check with pd to confirm whether it's still a member of the
        // cluster. If not, it destroys itself as a stale peer which is removed
        // out already.
        let state = self.fsm.peer.check_stale_state(&self.ctx.cfg);
        fail_point!("peer_check_stale_state", state != StaleState::Valid, |_| {});
        match state {
            StaleState::Valid => (),
            StaleState::LeaderMissing => {
                warn!(
                    "leader missing longer than abnormal_leader_missing_duration";
                    "tag" => self.peer.tag(),
                    "peer_id" => self.fsm.peer_id(),
                    "expect" => %self.ctx.cfg.abnormal_leader_missing_duration,
                );
                self.ctx
                    .raft_metrics
                    .leader_missing
                    .lock()
                    .unwrap()
                    .insert(self.region_id());
            }
            StaleState::ToValidate => {
                // for peer B in case 1 above
                warn!(
                    "leader missing longer than max_leader_missing_duration. \
                     To check with pd and other peers whether it's still valid";
                    "tag" => self.peer.tag(),
                    "peer_id" => self.fsm.peer_id(),
                    "expect" => %self.ctx.cfg.max_leader_missing_duration,
                );

                self.fsm.peer.bcast_check_stale_peer_message(self.ctx);

                let task = PdTask::ValidatePeer {
                    peer: self.fsm.peer.peer.clone(),
                    region: self.fsm.peer.region().clone(),
                };
                if let Err(e) = self.ctx.global.pd_scheduler.schedule(task) {
                    error!(
                        "failed to notify pd";
                        "tag" => self.peer.tag(),
                        "peer_id" => self.fsm.peer_id(),
                        "err" => %e,
                    )
                }
            }
        }
    }

    fn on_check_long_tick(&mut self) {
        if self.fsm.peer.pending_remove {
            return;
        }
        self.ticker.schedule(PEER_TICK_CHECK_LONG);
        self.on_check_peer_stale_state();
        self.on_check_failed_compaction()
    }

    fn on_check_failed_compaction(&mut self) {
        if !self.peer.is_leader() {
            return;
        }
        let kv = &self.ctx.global.engines.kv;
        let Some(shard) = kv.get_shard(self.region_id()) else {
            return;
        };
        if shard.has_failed_compaction() {
            info!("{} re-trigger failed compaction", self.peer.tag());
            kv.trigger_compact(shard.id_ver());
        }
    }

    /// Check if merge target region is staler than the local one in kv engine.
    /// It should be called when target region is not in region map in memory.
    /// If everything is ok, the answer should always be true because PD should
    /// ensure all target peers exist. So if not, error log will be printed
    /// and return false.
    fn is_merge_target_region_stale(&self, target_region: &metapb::Region) -> Result<bool> {
        let target_peer_id = find_peer(target_region, self.ctx.store_id())
            .unwrap()
            .get_id();
        if let Some(target_state) =
            load_last_peer_state(&self.ctx.global.engines.raft, target_peer_id)
        {
            let state_epoch = target_state.get_region().get_region_epoch();
            if util::is_epoch_stale(target_region.get_region_epoch(), state_epoch) {
                return Ok(true);
            }
            // The local target region epoch is staler than target region's.
            // In the case where the peer is destroyed by receiving gc msg rather than
            // applying conf change, the epoch may staler but it's legal, so
            // check peer id to assure that.
            if let Some(local_target_peer_id) =
                find_peer(target_state.get_region(), self.ctx.store_id()).map(|r| r.get_id())
            {
                match local_target_peer_id.cmp(&target_peer_id) {
                    cmp::Ordering::Equal => {
                        if target_state.get_state() == PeerState::Tombstone {
                            // The local target peer has already been destroyed.
                            return Ok(true);
                        }
                        error!(
                            "the local target peer state is not tombstone in kv engine";
                            "target_peer_id" => target_peer_id,
                            "target_peer_state" => ?target_state.get_state(),
                            "target_region" => ?target_region,
                            "tag" => self.peer.tag(),
                            "peer_id" => self.fsm.peer_id(),
                        );
                    }
                    cmp::Ordering::Greater => {
                        if state_epoch.get_version() == 0 && state_epoch.get_conf_ver() == 0 {
                            // There is a new peer and it's destroyed without being initialised.
                            return Ok(true);
                        }
                        // The local target peer id is greater than the one in target region, but
                        // its epoch is staler than target_region's. That is
                        // contradictory.
                        panic!("{} local target peer id {} is greater than the one in target region {}, but its epoch is staler, local target region {:?},
                                    target region {:?}", self.fsm.peer.tag(), local_target_peer_id, target_peer_id, target_state.get_region(), target_region);
                    }
                    cmp::Ordering::Less => {
                        error!(
                            "the local target peer id in kv engine is less than the one in target region";
                            "local_target_peer_id" => local_target_peer_id,
                            "target_peer_id" => target_peer_id,
                            "target_region" => ?target_region,
                            "tag" => self.peer.tag(),
                            "peer_id" => self.fsm.peer_id(),
                        );
                    }
                }
            } else {
                // Can't get local target peer id probably because this target peer is removed
                // by applying conf change
                error!(
                    "the local target peer does not exist in target region state";
                    "target_region" => ?target_region,
                    "local_target" => ?target_state.get_region(),
                    "tag" => self.peer.tag(),
                    "peer_id" => self.fsm.peer_id(),
                );
            }
        } else {
            error!(
                "failed to load target peer's RegionLocalState from kv engine";
                "target_peer_id" => target_peer_id,
                "target_region" => ?target_region,
                "tag" => self.peer.tag(),
                "peer_id" => self.fsm.peer_id(),
            );
        }
        Ok(false)
    }

    fn validate_merge_peer(
        &self,
        target_region: &metapb::Region,
        store_meta: &mut StoreMeta,
    ) -> Result<bool> {
        let target_region_id = target_region.get_id();
        let exist_region = { store_meta.region_map.get(target_region_id).cloned() };
        if let Some(r) = exist_region {
            let exist_epoch = r.get_region_epoch();
            let expect_epoch = target_region.get_region_epoch();
            // exist_epoch > expect_epoch
            if util::is_epoch_stale(expect_epoch, exist_epoch) {
                return Err(box_err!(
                    "target region changed {:?} -> {:?}",
                    target_region,
                    r
                ));
            }
            // exist_epoch < expect_epoch
            if util::is_epoch_stale(exist_epoch, expect_epoch) {
                info!(
                    "target region still not catch up, skip.";
                    "tag" => self.peer.tag(),
                    "peer_id" => self.fsm.peer_id(),
                    "target_region" => ?target_region,
                    "exist_region" => ?r,
                );
                return Ok(false);
            }
            return Ok(true);
        }

        // All of the target peers must exist before merging which is guaranteed by PD.
        // Now the target peer is not in region map.
        match self.is_merge_target_region_stale(target_region) {
            Err(e) => {
                error!(%e;
                    "failed to load region state, ignore";
                    "tag" => self.peer.tag(),
                    "peer_id" => self.fsm.peer_id(),
                    "target_region_id" => target_region_id,
                );
                Ok(false)
            }
            Ok(true) => Err(box_err!("region {} is destroyed", target_region_id)),
            Ok(false) => {
                error!(
                    "something is wrong, maybe PD do not ensure all target peers exist before merging"
                );
                Ok(false)
            }
        }
    }

    fn schedule_merge(&mut self, store_meta: &mut StoreMeta) -> Result<()> {
        fail_point!("on_schedule_merge_error", |_| {
            // Return error to trigger rollback merge.
            Err(box_err!("on_schedule_merge_error failpoint"))
        });
        let (request, target_id) = {
            let state = self.fsm.peer.pending_merge_state.as_ref().unwrap();
            let expect_region = state.get_target();

            // NOTE: Should NOT check the validation of merge with target region.
            // The state of target region would not be consistent between peers, and lead to
            // the merge be rollback & commit at the same time.
            // See https://github.com/tidbcloud/cloud-storage-engine/issues/2503.
            if let Some(source_meta) = self.fsm.peer.get_store().shard_meta.as_ref() {
                if source_meta.has_txn_file_locks() {
                    let tag = self.peer.tag();
                    info!("{} fail to schedule merge: source region has txn file locks", tag;
                        "target" => ?expect_region,
                        "txn_file_locks" => %source_meta.txn_file_locks(),
                    );
                    return Err(box_err!(
                        "{}: {}",
                        tag,
                        MERGE_REGION_WITH_TXN_FILE_LOCKS_ERR_MSG
                    ));
                }

                // If source region has unconverted l0s, wait for retry instead of returning
                // error. These l0s may be compacted during merge. This avoids inconsistent
                // rollback/commit decisions across peers.
                if !source_meta.unconverted_l0s.is_empty() {
                    let tag = self.peer.tag();
                    info!("{} fail to schedule merge: source region has unconverted l0s, retry", tag;
                        "target" => ?expect_region,
                        "unconverted_l0s" => ?source_meta.unconverted_l0s,
                    );
                    return Ok(());
                }
            }

            if !self.validate_merge_peer(expect_region, store_meta)? {
                // Wait till next round.
                return Ok(());
            }
            let target_id = expect_region.get_id();
            let sibling_region = expect_region;

            let sibling_peer = find_peer(sibling_region, self.store_id()).unwrap();
            let mut request = new_admin_request(sibling_region.get_id(), sibling_peer.clone());
            request
                .mut_header()
                .set_region_epoch(sibling_region.get_region_epoch().clone());
            let mut admin = AdminRequest::default();
            admin.set_cmd_type(AdminCmdType::CommitMerge);
            admin
                .mut_commit_merge()
                .set_source(self.fsm.peer.get_preprocessed_region().clone());
            admin.mut_commit_merge().set_commit(state.get_commit());
            // Fetch the prepare_merge entry for learner.
            let entries = self
                .peer
                .get_store()
                .entries(
                    state.commit,
                    state.commit + 1,
                    None,
                    GetEntriesContext::empty(false),
                )
                .unwrap();
            admin.mut_commit_merge().set_entries(entries.into());
            let source_meta = self
                .fsm
                .peer
                .get_store()
                .shard_meta
                .as_ref()
                .unwrap()
                .to_change_set();
            admin
                .mut_commit_merge()
                .set_source_meta(source_meta.write_to_bytes().unwrap());
            request.set_admin_request(admin);
            (request, target_id)
        };
        // Please note that, here assumes that the unit of network isolation is store
        // rather than peer. So a quorum stores of source region should also be
        // the quorum stores of target region. Otherwise we need to enable
        // proposal forwarding.
        self.ctx.global.router.send(
            target_id,
            PeerMsg::RaftCommand(RaftCommand::new(request, Callback::None)),
        );
        Ok(())
    }

    fn rollback_merge(&mut self) {
        let req = {
            let state = self.fsm.peer.pending_merge_state.as_ref().unwrap();
            let mut request =
                new_admin_request(self.fsm.peer.region().get_id(), self.fsm.peer.peer.clone());
            request
                .mut_header()
                .set_region_epoch(self.fsm.peer.region().get_region_epoch().clone());
            let mut admin = AdminRequest::default();
            admin.set_cmd_type(AdminCmdType::RollbackMerge);
            admin.mut_rollback_merge().set_commit(state.get_commit());
            request.set_admin_request(admin);
            request
        };
        self.propose_raft_command(req, Callback::None, None);
    }

    pub(crate) fn on_check_merge(&mut self, store_meta: &mut StoreMeta) {
        if self.fsm.stopped
            || self.fsm.peer.pending_remove
            || self.fsm.peer.pending_merge_state.is_none()
        {
            return;
        }
        fail_point!(
            "on_check_merge_not_1001",
            self.fsm.peer_id() != 1001,
            |_| {}
        );
        if let Err(e) = self.schedule_merge(store_meta) {
            if self.fsm.peer.is_leader() {
                self.fsm
                    .peer
                    .add_want_rollback_merge_peer(self.fsm.peer_id());
                if self
                    .fsm
                    .peer
                    .raft_group
                    .raft
                    .prs()
                    .has_quorum(&self.fsm.peer.want_rollback_merge_peers)
                {
                    info!(
                        "failed to schedule merge, rollback";
                        "tag" => self.peer.tag(),
                        "peer_id" => self.fsm.peer_id(),
                        "err" => %e,
                        "error_code" => %e.error_code(),
                    );
                    self.rollback_merge();
                }
            } else if !is_learner(&self.fsm.peer.peer) {
                info!(
                    "want to rollback merge";
                    "tag" => self.peer.tag(),
                    "peer_id" => self.fsm.peer_id(),
                    "leader_id" => self.fsm.peer.leader_id(),
                    "err" => %e,
                    "error_code" => %e.error_code(),
                );
                if self.fsm.peer.leader_id() != raft::INVALID_ID {
                    self.fsm.peer.send_want_rollback_merge(
                        self.fsm
                            .peer
                            .pending_merge_state
                            .as_ref()
                            .unwrap()
                            .get_commit(),
                        self.ctx,
                    );
                }
            }
        }
    }

    fn on_restore_shard(&mut self, mut cs: kvenginepb::ChangeSet, callback: Callback) {
        let tag = self.peer.tag();
        let region = self.peer.get_preprocessed_region();

        // Check request.
        if !cs.has_restore_shard() {
            callback.invoke_with_response(message_error("invalid changeset"));
            return;
        }
        // The start key of first region is b"", but `encode_bytes(b""") ==
        // 0000000000000000F7`.
        let encoded_start_key = encode_bytes_maybe_empty(cs.get_restore_shard().get_outer_start());
        let encoded_end_key = encode_bytes(cs.get_restore_shard().get_outer_end());
        if encoded_start_key != region.get_start_key() || encoded_end_key != region.get_end_key() {
            let err_msg = format!(
                "invalid snapshot range: [{:?},{:?}), expect: [{:?},{:?})",
                encoded_start_key,
                encoded_end_key,
                region.get_start_key(),
                region.get_end_key()
            );
            callback.invoke_with_response(message_error(err_msg));
            return;
        }

        // Check peer.
        if !self.peer.is_leader() {
            callback.invoke_with_response(new_error(Error::NotLeader(
                self.peer.region_id,
                self.fsm.peer.get_peer_from_cache(self.fsm.peer.leader_id()),
            )));
            return;
        }
        if cs.shard_ver != region.get_region_epoch().get_version() {
            callback.invoke_with_response(new_error(Error::EpochNotMatch(
                "restore_shard".to_owned(),
                vec![region.clone()],
            )));
            return;
        }

        let shard = match self.ctx.global.engines.kv.get_shard(self.region_id()) {
            // initial flushed is necessary ?
            Some(shard) if !shard.get_initial_flushed() => {
                let err_msg = format!("{tag} not initial flushed, try again");
                callback.invoke_with_response(message_error(err_msg));
                return;
            }
            None => {
                let err_msg = format!("{tag} shard not found");
                error!("{}", err_msg);
                callback.invoke_with_response(message_error(err_msg));
                return;
            }
            Some(shard) => shard,
        };

        debug!("{} on_restore_shard, original changeset: {:?}", tag, cs);

        // Adjust base version.
        let snap = cs.mut_restore_shard();
        let write_seq = shard.get_write_sequence();
        let old_mem_tbl_version = shard.get_base_version() + write_seq;
        let new_mem_tbl_version =
            cmp::max(snap.base_version + snap.data_sequence, old_mem_tbl_version);
        snap.set_base_version(new_mem_tbl_version - write_seq);
        snap.set_data_sequence(write_seq); // not necessary but just keep fields consistency.
        info!("{} on_restore_shard, adjusted changeset: {:?}", tag, cs);

        // Set `learner_skip_idx` to skip "restore_shard" replicating to learner.
        // NOTE: Next `MsgAppend` use the `last_index` other than `next_proposal_index`.
        let new_idx = self.fsm.peer.raft_group.raft.raft_log.last_index();
        let old_idx = mem::replace(&mut self.fsm.peer.learner_skip_idx, new_idx);
        info!(
            "{} on_restore_shard, set peer.learner_skip_idx {}, old idx {}",
            tag, new_idx, old_idx
        );

        let mut cmd = self.new_raft_cmd_request();
        let mut custom_builder = CustomBuilder::new();
        custom_builder.set_change_set(&cs);
        cmd.set_custom_request(custom_builder.build());
        self.propose_raft_command(cmd, callback, None);
    }

    fn check_gc_tombstones(&self) {
        if !self.peer.is_leader() {
            return;
        }
        let kv = &self.ctx.global.engines.kv;
        if let Some(shard) = kv.get_shard(self.region_id()) {
            let safe_ts = kv.get_keyspace_gc_safepoint_v2(shard.keyspace_id);
            if shard.check_need_gc_tombstones(safe_ts, kv.opts.gc_lock_extra_cf) {
                kv.trigger_compact(shard.id_ver());
            }
        }
    }

    fn maybe_destroy(&mut self) {
        if self.fsm.peer.maybe_destroy(self.ctx) {
            // Destroy the apply fsm first, wait for the reply msg from apply fsm
            self.ctx.apply_msgs.msgs.push(ApplyMsg::UnsafeDestroy {
                region_id: self.region_id(),
            });
        } else {
            self.ctx
                .raft_metrics
                .message_dropped
                .region_tombstone_peer
                .inc();
        }
    }
}

pub fn new_read_index_request(
    region_id: u64,
    region_epoch: RegionEpoch,
    peer: metapb::Peer,
) -> RaftCmdRequest {
    let mut request = RaftCmdRequest::default();
    request.mut_header().set_region_id(region_id);
    request.mut_header().set_region_epoch(region_epoch);
    request.mut_header().set_peer(peer);
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::ReadIndex);
    request
}

pub fn new_admin_request(region_id: u64, peer: metapb::Peer) -> RaftCmdRequest {
    let mut request = RaftCmdRequest::default();
    request.mut_header().set_region_id(region_id);
    request.mut_header().set_peer(peer);
    request
}

/// For status command.
impl PeerMsgHandler<'_> {
    // Handle status commands here, separate the logic, maybe we can move it
    // to another file later.
    // Unlike other commands (write or admin), status commands only show current
    // store status, so no need to handle it in raft group.
    fn execute_status_command(&mut self, request: &RaftCmdRequest) -> Result<RaftCmdResponse> {
        let cmd_type = request.get_status_request().get_cmd_type();

        let mut response = match cmd_type {
            StatusCmdType::RegionLeader => self.execute_region_leader(),
            StatusCmdType::RegionDetail => self.execute_region_detail(request),
            StatusCmdType::InvalidStatus => {
                Err(box_err!("{} invalid status command!", self.fsm.peer.tag()))
            }
        }?;
        response.set_cmd_type(cmd_type);

        let mut resp = RaftCmdResponse::default();
        resp.set_status_response(response);
        // Bind peer current term here.
        bind_term(&mut resp, self.fsm.peer.term());
        Ok(resp)
    }

    fn execute_region_leader(&mut self) -> Result<StatusResponse> {
        let mut resp = StatusResponse::default();
        if let Some(leader) = self.fsm.peer.get_peer_from_cache(self.fsm.peer.leader_id()) {
            resp.mut_region_leader().set_leader(leader);
        }

        Ok(resp)
    }

    fn execute_region_detail(&mut self, request: &RaftCmdRequest) -> Result<StatusResponse> {
        if !self.fsm.peer.get_store().is_initialized() {
            let region_id = request.get_header().get_region_id();
            return Err(Error::RegionNotInitialized(region_id));
        }
        let mut resp = StatusResponse::default();
        resp.mut_region_detail()
            .set_region(self.fsm.peer.region().clone());
        if let Some(leader) = self.fsm.peer.get_peer_from_cache(self.fsm.peer.leader_id()) {
            resp.mut_region_detail().set_leader(leader);
        }

        Ok(resp)
    }
}

pub struct MsgDebug<'a>(pub &'a RaftMessage);

impl std::fmt::Display for MsgDebug<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = self.0.get_message();
        write!(
            f,
            "{:?}, to: {}, from: {}, term: {}, log_term: {}, index: {}, commit: {}, reject: {}, num_entries: {}",
            msg.msg_type,
            msg.to,
            msg.from,
            msg.term,
            msg.log_term,
            msg.index,
            msg.commit,
            msg.reject,
            msg.get_entries().len(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::PeerMsgHandler;

    #[test]
    fn test_calc_kv_size_for_storage_class() {
        let ia_kv_size_discount = 0.5;

        let cases = vec![
            (0, 0, 0),
            (100, 0, 100),
            (100, 50, 75),
            (100, 100, 50),
            (100, 200, 100),
        ];
        for (estimated_kv_size, estimated_ia_kv_size, expect) in cases {
            assert_eq!(
                PeerMsgHandler::calc_kv_size_for_storage_class(
                    estimated_kv_size,
                    estimated_ia_kv_size,
                    ia_kv_size_discount
                ),
                expect,
            );
        }
    }
}
