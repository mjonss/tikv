// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    cmp,
    collections::VecDeque,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use api_version::ApiV2;
use bitflags::bitflags;
use bytes::{Buf, BufMut};
use cloud_encryption::EncryptionKey;
use collections::{HashMap, HashSet};
use error_code::ErrorCodeExt;
use fail::fail_point;
use kvengine::{
    ENCRYPTION_KEY, FilePrepareType, LARGE_NUM_COLUMNAR_TABLES_IN_SHARD, STORAGE_CLASS_KEY,
    ShardMeta, get_shard_property,
    metrics::ENGINE_DUPLICATED_CHANGE_SET_COUNTER,
    set_shard_property,
    util::{PropertiesHelper, estimated_entries_by_table_count},
};
use kvproto::{
    disk_usage::DiskUsage,
    kvrpcpb::ExtraOp as TxnExtraOp,
    metapb::{PeerRole, Region},
    pdpb::PeerStats,
    raft_cmdpb::{
        AdminCmdType, AdminResponse, ChangePeerRequest, CmdType, CustomRequest, RaftCmdRequest,
        RaftCmdResponse, TransferLeaderRequest, TransferLeaderResponse,
    },
    raft_serverpb::{ExtraMessage, ExtraMessageType, MergeState, PeerState, RaftMessage},
    *,
};
use pd_client::{BucketMeta, BucketStat, new_bucket_write_stats, simple_merge_bucket_write_stats};
use protobuf::Message;
use raft::{
    self, Changer, INVALID_ID, INVALID_INDEX, LightReady, ProgressState, ProgressTracker, RawNode,
    Ready, SnapshotStatus, StateRole, Storage,
};
use raft_proto::{
    eraftpb::{ConfChangeType, Entry, MessageType},
    *,
};
use raftstore::{
    coprocessor,
    coprocessor::{RegionChangeEvent, RegionChangeReason, RoleChange},
    store::{
        TxnExt,
        local_metrics::*,
        metrics::*,
        util::{
            AdminCmdEpochState, ChangePeerI, ConfChangeKind, Lease, LeaseState,
            admin_cmd_epoch_lookup, is_epoch_stale, is_initial_msg,
        },
    },
};
use rfengine::KV_ENGINE_META_KEY;
use tikv_util::{
    Either, box_err,
    codec::bytes::decode_bytes,
    debug, error, info,
    time::{Instant, duration_to_sec, monotonic_raw_now},
    warn,
    worker::Scheduler,
};
use time::Timespec;
use txn_types::Key;
use uuid::Uuid;

use super::*;
use crate::{RaftRouter, errors::*};

const SHRINK_CACHE_CAPACITY: usize = 64;
const MAX_COMMITTED_SIZE_PER_READY: u64 = 16 * 1024 * 1024;
pub(crate) const SPLIT_FLAG_ENCRYPTION_KEYS: u64 = 0x01;
pub(crate) const PENDING_CONF_CHANGE_ERR_MSG: &str = "pending conf change";

pub(crate) const SLOW_LOG_DURATION: Duration = Duration::from_millis(30);
pub(crate) const WAIT_SNAPSHOT_ACK_TICKS: u32 = 60;

/// The returned states of the peer after checking whether it is stale
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum StaleState {
    Valid,
    ToValidate,
    LeaderMissing,
}

pub(crate) fn notify_stale_req(term: u64, cb: Callback, reason: &str) {
    info!("notify stale req reason: {}", reason);
    let resp = cmd_resp::err_resp(Error::StaleCommand, term);
    cb.invoke_with_response(resp);
}

pub(crate) fn notify_req_region_removed(region_id: u64, cb: Callback) {
    let region_not_found = Error::RegionNotFound(region_id, None);
    let resp = cmd_resp::new_error(region_not_found);
    cb.invoke_with_response(resp);
}

#[derive(Default)]
pub(crate) struct ProposalQueue {
    queue: VecDeque<Proposal>,
}

impl ProposalQueue {
    fn find_propose_time(&self, term: u64, index: u64) -> Option<Timespec> {
        self.queue
            .binary_search_by_key(&(term, index), |p: &Proposal| (p.term, p.index))
            .ok()
            .and_then(|i| self.queue[i].propose_time)
    }

    // Find proposal in front or at the given term and index
    fn pop(&mut self, term: u64, index: u64) -> Option<Proposal> {
        self.queue.pop_front().and_then(|p| {
            // Comparing the term first then the index, because the term is
            // increasing among all log entries and the index is increasing
            // inside a given term
            if (p.term, p.index) > (term, index) {
                self.queue.push_front(p);
                return None;
            }
            Some(p)
        })
    }

    /// Find proposal at the given term and index and notify stale proposals
    /// in front that term and index
    fn find_proposal(
        &mut self,
        tag: PeerTag,
        term: u64,
        index: u64,
        current_term: u64,
        preprocess_errors: &mut PreprocessErrors, // `&mut` due to Error is not clonable.
    ) -> Option<Proposal> {
        while let Some(p) = self.pop(term, index) {
            if p.term == term {
                if p.index == index {
                    return if p.cb.is_none() {
                        None
                    } else {
                        Self::handle_preprocess_errors(p, preprocess_errors)
                    };
                } else {
                    panic!(
                        "{} unexpected callback at term {}, found index {}, expected {}",
                        tag, term, p.index, index
                    );
                }
            } else {
                notify_stale_req(current_term, p.cb, "old term");
            }
        }
        None
    }

    fn push(&mut self, p: Proposal) {
        if let Some(f) = self.queue.back() {
            // The term must be increasing among all log entries and the index
            // must be increasing inside a given term
            assert!((p.term, p.index) > (f.term, f.index));
        }
        self.queue.push_back(p);
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn gc(&mut self) {
        if self.queue.capacity() > SHRINK_CACHE_CAPACITY && self.queue.len() < SHRINK_CACHE_CAPACITY
        {
            self.queue.shrink_to_fit();
        }
    }

    fn handle_preprocess_errors(
        p: Proposal,
        preprocess_errors: &mut PreprocessErrors,
    ) -> Option<Proposal> {
        if let Some(err) = preprocess_errors.take_error(p.term, p.index) {
            let (resp, key_errs) = cmd_resp::new_with_key_error(err);
            p.cb.invoke_with_response_ext(resp, key_errs);
            None
        } else {
            Some(p)
        }
    }
}

bitflags! {
    // TODO: maybe declare it as protobuf struct is better.
    /// A bitmap contains some useful flags when dealing with `eraftpb::Entry`.
    pub struct ProposalContext: u8 {
        const SYNC_LOG       = 0b0000_0001;
        const SPLIT          = 0b0000_0010;
        const PREPARE_MERGE  = 0b0000_0100;
        const PRE_PROCESS    = 0b0000_1000;
        const ENCRYPTED      = 0b0001_0000;
    }
}

impl ProposalContext {
    /// Converts itself to a vector.
    pub fn to_vec(self) -> Vec<u8> {
        if self.is_empty() {
            return vec![];
        }
        let ctx = self.bits();
        vec![ctx]
    }

    /// Initializes a `ProposalContext` from a byte slice.
    pub fn from_bytes(ctx: &[u8]) -> ProposalContext {
        if ctx.is_empty() {
            ProposalContext::empty()
        } else if ctx.len() == 1 {
            ProposalContext::from_bits_truncate(ctx[0])
        } else {
            panic!("invalid ProposalContext {:?}", ctx);
        }
    }
}

/// `ConsistencyState` is used for consistency check.
pub struct ConsistencyState {
    pub last_check_time: Instant,
    // (computed_result_or_to_be_verified, index, hash)
    pub index: u64,
    pub context: Vec<u8>,
    pub hash: Vec<u8>,
}

/// Statistics about raft peer.
#[derive(Default, Clone)]
pub struct PeerStat {
    pub written_bytes: u64,
    pub written_keys: u64,
    pub approximate_size: u64,
    pub approximate_keys: u64,
    pub approximate_kv_size: u64,
    pub approximate_columnar_size: u64,
    pub approximate_columnar_kv_size: u64,
}

pub struct ProposedAdminCmd {
    cmd_type: AdminCmdType,
    epoch_state: AdminCmdEpochState,
    index: u64,
    cbs: Vec<Callback>,
}

impl ProposedAdminCmd {
    fn new(
        cmd_type: AdminCmdType,
        epoch_state: AdminCmdEpochState,
        index: u64,
    ) -> ProposedAdminCmd {
        ProposedAdminCmd {
            cmd_type,
            epoch_state,
            index,
            cbs: Vec::new(),
        }
    }
}

#[derive(Default)]
struct CmdEpochChecker {
    // Although it's a deque, because of the characteristics of the settings from
    // `admin_cmd_epoch_lookup`, the max size of admin cmd is 2, i.e. split/merge and change
    // peer.
    proposed_admin_cmd: VecDeque<ProposedAdminCmd>,
    term: u64,
}

impl CmdEpochChecker {
    fn maybe_update_term(&mut self, term: u64) {
        assert!(term >= self.term);
        if term > self.term {
            self.term = term;
            for cmd in self.proposed_admin_cmd.drain(..) {
                for cb in cmd.cbs {
                    notify_stale_req(term, cb, "old term");
                }
            }
        }
    }

    /// Check if the proposal can be proposed on the basis of its epoch and
    /// previous proposed admin cmds.
    ///
    /// Returns None if passing the epoch check, otherwise returns a index which
    /// is the last admin cmd index conflicted with this proposal.
    pub fn propose_check_epoch(&mut self, req: &RaftCmdRequest, term: u64) -> Option<u64> {
        self.maybe_update_term(term);
        let (check_ver, check_conf_ver) = if !req.has_admin_request() {
            (NORMAL_REQ_CHECK_VER, NORMAL_REQ_CHECK_CONF_VER)
        } else {
            let cmd_type = req.get_admin_request().get_cmd_type();
            let epoch_state = admin_cmd_epoch_lookup(cmd_type);
            (epoch_state.check_ver, epoch_state.check_conf_ver)
        };
        self.last_conflict_index(check_ver, check_conf_ver)
    }

    pub fn post_propose(&mut self, cmd_type: AdminCmdType, index: u64, term: u64) {
        self.maybe_update_term(term);
        let epoch_state = admin_cmd_epoch_lookup(cmd_type);
        assert!(
            self.last_conflict_index(epoch_state.check_ver, epoch_state.check_conf_ver)
                .is_none()
        );

        if epoch_state.change_conf_ver || epoch_state.change_ver {
            if let Some(cmd) = self.proposed_admin_cmd.back() {
                assert!(cmd.index < index);
            }
            self.proposed_admin_cmd
                .push_back(ProposedAdminCmd::new(cmd_type, epoch_state, index));
        }
    }

    fn last_conflict_index(&self, check_ver: bool, check_conf_ver: bool) -> Option<u64> {
        self.proposed_admin_cmd
            .iter()
            .rev()
            .find(|cmd| {
                (check_ver && cmd.epoch_state.change_ver)
                    || (check_conf_ver && cmd.epoch_state.change_conf_ver)
            })
            .map(|cmd| cmd.index)
    }

    /// Returns the last proposed admin cmd index.
    ///
    /// Note that the cmd of this type must change epoch otherwise it can not be
    /// recorded to `proposed_admin_cmd`.
    #[allow(unused)]
    pub fn last_cmd_index(&mut self, cmd_type: AdminCmdType) -> Option<u64> {
        self.proposed_admin_cmd
            .iter()
            .rev()
            .find(|cmd| cmd.cmd_type == cmd_type)
            .map(|cmd| cmd.index)
    }

    pub fn advance_apply(&mut self, index: u64, term: u64, region: &metapb::Region) {
        self.maybe_update_term(term);
        while !self.proposed_admin_cmd.is_empty() {
            let cmd = self.proposed_admin_cmd.front_mut().unwrap();
            if cmd.index <= index {
                for cb in cmd.cbs.drain(..) {
                    let mut resp = cmd_resp::new_error(Error::EpochNotMatch(
                        format!(
                            "current epoch of region {} is {:?}",
                            region.get_id(),
                            region.get_region_epoch(),
                        ),
                        vec![region.to_owned()],
                    ));
                    cmd_resp::bind_term(&mut resp, term);
                    cb.invoke_with_response(resp);
                }
            } else {
                break;
            }
            self.proposed_admin_cmd.pop_front();
        }
    }

    pub fn attach_to_conflict_cmd(&mut self, index: u64, cb: Callback) {
        if let Some(cmd) = self
            .proposed_admin_cmd
            .iter_mut()
            .rev()
            .find(|cmd| cmd.index == index)
        {
            cmd.cbs.push(cb);
        } else {
            panic!(
                "index {} can not found in proposed_admin_cmd, callback {:?}",
                index, cb
            );
        }
    }
}

impl Drop for CmdEpochChecker {
    fn drop(&mut self) {
        if tikv_util::thread_group::is_shutdown(true) {
            for mut state in self.proposed_admin_cmd.drain(..) {
                state.cbs.clear();
            }
        } else {
            for state in self.proposed_admin_cmd.drain(..) {
                for cb in state.cbs {
                    notify_stale_req(self.term, cb, "drop");
                }
            }
        }
    }
}

#[derive(Default, Debug)]
pub struct DiskFullPeers {
    majority: bool,
    // Indicates whether a peer can help to establish a quorum.
    peers: HashMap<u64, (DiskUsage, bool)>,
}

impl DiskFullPeers {
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
    pub fn majority(&self) -> bool {
        self.majority
    }
    pub fn has(&self, peer_id: u64) -> bool {
        !self.peers.is_empty() && self.peers.contains_key(&peer_id)
    }
    pub fn get(&self, peer_id: u64) -> Option<DiskUsage> {
        self.peers.get(&peer_id).map(|x| x.0)
    }
}

pub(crate) struct Peer {
    /// The ID of the Region which this Peer belongs to.
    pub(crate) region_id: u64,
    pub(crate) keyspace_id: u32,

    /// The Peer meta information.
    pub(crate) peer: metapb::Peer,
    /// The Raft state machine of this Peer.
    pub(crate) raft_group: raft::RawNode<PeerStorage>,
    /// The cache of meta information for Region's other Peers.
    peer_cache: RefCell<HashMap<u64, metapb::Peer>>,
    /// Record the last instant of each peer's heartbeat response.
    pub(crate) peer_heartbeats: HashMap<u64, Instant>,

    pub(crate) proposals: ProposalQueue,
    pub(crate) pending_reads: ReadIndexQueue,

    pub(crate) pending_apply_results: Vec<MsgApplyResult>,

    /// Index of last scheduled committed raft log.
    pub(crate) last_applying_idx: u64,
    /// The index of the latest urgent proposal index.
    pub(crate) last_urgent_proposal_idx: u64,
    /// The index of the latest committed split command.
    pub(crate) last_committed_split_idx: u64,
    /// The index of last sent snapshot
    last_sent_snapshot_idx: u64,
    /// Track snapshots we have sent but not yet acknowledged by the follower.
    /// peer_id -> (snapshot_index, sent_at).
    pending_snapshot_acks: HashMap<u64, (u64, Instant)>,
    /// Timeout duration to wait for snapshot ack from follower.
    wait_snapshot_ack_timeout: Duration,

    /// preprocessed_index is used to avoid duplicated preprocess execution.
    pub(crate) preprocessed_index: u64,

    /// Indexes and term to trigger TiFlash acquiring latest snapshot after
    /// "restore_shard".
    /// learner_skip_idx: the raft log which is skipped replicating to learner.
    /// It is set when proposing "restore_shard".
    pub learner_skip_idx: u64,
    /// pending_truncate: the raft log pending to be truncated after
    /// applied. It is set after "restore_shard" is committed.
    /// Note that index of `pending_truncate` is not necessarily equal to
    /// `learner_skip_idx`. E.g. when there is another "restore_shard"
    /// request in a short time.
    pub pending_truncate: Option<(u64 /* term */, u64 /* index */)>,

    pub(crate) pending_remove: bool,

    /// True if the peer is being destroyed, but waiting for dependents empty.
    pub(crate) delay_destroy: bool,
    /// True if the delay_destroy is caused by successful commit merge.
    pub(crate) delay_destroy_merged_target: Option<Region>,

    /// Record the instants of peers being added into the configuration.
    /// Remove them after they are not pending any more.
    pub peers_start_pending_time: Vec<(u64, Instant)>,

    /// A inaccurate cache about which peer is marked as down.
    pub(crate) down_peer_ids: Vec<u64>,

    pub(crate) need_campaign: bool,

    /// The id of the leader since last report region leader change event.
    reported_leader_id: u64,
    leader_missing_time: Option<Instant>,
    leader_lease: Lease,

    pub(crate) peer_stat: PeerStat,

    /// Transaction extensions related to this peer.
    pub txn_ext: Arc<TxnExt>,

    /// Check whether this proposal can be proposed based on its epoch.
    cmd_epoch_checker: CmdEpochChecker,

    /// lead_transferee if the peer is in a leadership transferring.
    pub lead_transferee: u64,

    /// Record the shard meta sequence at the time of last buckets splitting. If
    /// the meta sequence changed, buckets will be refreshed.
    pub(crate) last_bucket_update_meta_sequence: u64,

    pub(crate) buckets: Option<BucketStat>,
    pub(crate) bucket_version: u64,

    pub(crate) pending_merge_state: Option<MergeState>,
    /// The rollback merge proposal can be proposed only when the number
    /// of peers is greater than the majority of all peers.
    /// There are more details in the annotation above
    /// `test_node_merge_write_data_to_source_region_after_merging`
    /// The peers who want to rollback merge.
    pub want_rollback_merge_peers: HashSet<u64>,

    /// The known newest conf version and its corresponding peer list
    /// Send to these peers to check whether itself is stale.
    pub check_stale_conf_ver: u64,
    pub check_stale_peers: Vec<metapb::Peer>,
    pub(crate) encryption_key: Option<EncryptionKey>,
    pub(crate) encryption_buf: Vec<u8>,
    pub(crate) last_active_time: tikv_util::time::Instant,
}

impl Peer {
    pub fn new(
        store_id: u64,
        cfg: &Config,
        engines: Engines,
        region: &metapb::Region,
        peer: metapb::Peer,
    ) -> Result<Peer> {
        if peer.get_id() == raft::INVALID_ID {
            return Err(box_err!("invalid peer id"));
        }
        let ps = PeerStorage::new(engines, region.clone(), peer.get_id(), store_id)?;
        let first = ps.first_index();
        let truncated = ps.truncated_index();
        let truncated_term = ps.truncated_term();
        let last = ps.last_index();
        let last_term = ps.last_term();
        let commit = ps.commit_index();

        let applied_index = ps.applied_index();
        info!(
            "{} new peer storage first:{} truncated:{} trunc_term:{} last:{}, last_term:{}, applied: {}, commit: {}",
            ps.tag(),
            first,
            truncated,
            truncated_term,
            last,
            last_term,
            applied_index,
            commit,
        );

        let raft_cfg = raft::Config {
            id: peer.get_id(),
            election_tick: cfg.raft_election_timeout_ticks,
            heartbeat_tick: cfg.raft_heartbeat_ticks,
            min_election_tick: cfg.raft_min_election_timeout_ticks,
            max_election_tick: cfg.raft_max_election_timeout_ticks,
            max_size_per_msg: cfg.raft_max_size_per_msg.0,
            max_inflight_msgs: cfg.raft_max_inflight_msgs,
            applied: applied_index,
            check_quorum: true,
            skip_bcast_commit: true,
            pre_vote: cfg.prevote,
            max_committed_size_per_ready: MAX_COMMITTED_SIZE_PER_READY,
            ..Default::default()
        };

        let region_tag = format!("[{}:{}:]", store_id, region.clone().get_id());
        let logger = slog_global::get_global().new(slog::o!("region" => region_tag));
        let encryption_key = ps.get_encryption_key();
        let raft_group = RawNode::new(&raft_cfg, ps, &logger)?;
        let keyspace_id =
            ApiV2::get_u32_keyspace_id_by_key(region.get_start_key()).unwrap_or_default();
        let wait_snapshot_ack_timeout = cfg.raft_base_tick_interval.0 * WAIT_SNAPSHOT_ACK_TICKS;
        let mut peer = Peer {
            peer,
            region_id: region.get_id(),
            keyspace_id,
            raft_group,
            proposals: ProposalQueue::default(),
            pending_reads: Default::default(),
            pending_apply_results: Default::default(),
            peer_cache: RefCell::new(HashMap::default()),
            peer_heartbeats: HashMap::default(),
            peers_start_pending_time: vec![],
            down_peer_ids: vec![],
            pending_remove: false,
            delay_destroy: false,
            delay_destroy_merged_target: None,
            reported_leader_id: 0,
            leader_missing_time: Some(Instant::now()),
            last_applying_idx: applied_index,
            last_urgent_proposal_idx: u64::MAX,
            last_committed_split_idx: 0,
            last_sent_snapshot_idx: 0,
            pending_snapshot_acks: HashMap::default(),
            wait_snapshot_ack_timeout,
            preprocessed_index: 0,
            learner_skip_idx: 0,
            pending_truncate: None,
            leader_lease: Lease::new(
                cfg.raft_store_max_leader_lease(),
                cfg.renew_leader_lease_advance_duration(),
            ),
            peer_stat: PeerStat::default(),
            txn_ext: Arc::new(TxnExt::default()),
            cmd_epoch_checker: Default::default(),
            need_campaign: false,
            lead_transferee: raft::INVALID_ID,
            last_bucket_update_meta_sequence: 0,
            buckets: None,
            bucket_version: 0,
            pending_merge_state: None,
            want_rollback_merge_peers: Default::default(),
            check_stale_conf_ver: 0,
            check_stale_peers: vec![],
            encryption_key,
            encryption_buf: vec![],
            last_active_time: tikv_util::time::Instant::now_coarse(),
        };
        // If this region has only one peer and I am the one, campaign directly.
        if region.get_peers().len() == 1 && region.get_peers()[0].get_store_id() == store_id {
            peer.raft_group.campaign()?;
        }

        Ok(peer)
    }

    pub(crate) fn tag(&self) -> PeerTag {
        self.get_store().tag()
    }

    pub(crate) fn term(&self) -> u64 {
        self.raft_group.raft.term
    }

    pub fn maybe_add_want_rollback_merge_peer(&mut self, peer_id: u64, extra_msg: &ExtraMessage) {
        if !self.is_leader() {
            return;
        }
        if let Some(ref state) = self.pending_merge_state {
            if state.get_commit() == extra_msg.get_index() {
                self.add_want_rollback_merge_peer(peer_id);
            }
        }
    }

    pub fn add_want_rollback_merge_peer(&mut self, peer_id: u64) {
        assert!(self.pending_merge_state.is_some());
        self.want_rollback_merge_peers.insert(peer_id);
    }

    pub(crate) fn next_proposal_index(&self) -> u64 {
        self.raft_group.raft.raft_log.last_index() + 1
    }

    pub(crate) fn maybe_destroy(&mut self, ctx: &RaftContext) -> bool {
        if self.pending_remove {
            info!(
                "is being destroyed, skip";
                "tag" => self.tag(),
                "peer_id" => self.peer.get_id(),
            );
            return false;
        }
        if self.is_applying_snapshot() {
            info!(
                "stale peer is applying snapshot, will destroy next time";
                "tag" => self.tag(),
                "peer_id" => self.peer.get_id(),
            );
            return false;
        }
        if !self.get_store().region_match_preprocessed() {
            // If we don't wait for region applied to latest epoch, the split may failed to
            // execute, So the dependent may failed to clean up, the destroy
            // would delay forever.
            info!(
                "region has not applied to preprocessed epoch, wait for apply";
                "tag" => self.tag(),
                "peer_id" => self.peer.get_id(),
            );
            return false;
        }
        if !self.dependents_is_empty_or_only_self(ctx) {
            info!(
                "region has dependent, wait for destroy";
                "tag" => self.tag(),
                "peer_id" => self.peer.get_id(),
            );
            return false;
        }
        self.pending_remove = true;
        true
    }

    pub(crate) fn destroy(
        &mut self,
        raft_wb: &mut rfengine::WriteBatch,
        merged_target: Option<Region>,
    ) -> Result<()> {
        let t = Instant::now();

        let mut region = self.get_preprocessed_region().clone();
        info!(
            "begin to destroy";
            "tag" => self.tag(),
            "peer_id" => self.peer.get_id(),
        );
        raft_wb.clear_peer(self.peer_id());
        self.mut_store().clear_meta(raft_wb, true);

        // StoreMsgHandler::check_msg use both epoch and region peer list to check
        // whether a message is targing a staled peer. But for an uninitialized
        // peer, peer list is empty, so a removed peer will be created again.
        // Saving current peer into the peer list of region will fix this
        // problem.
        if !self.get_store().is_initialized() {
            region.mut_peers().push(self.peer.clone());
        }
        let merge_state = if self.pending_merge_state.is_some() {
            self.pending_merge_state.clone()
        } else {
            merged_target.map(|r| {
                let mut merge_state = MergeState::default();
                merge_state.set_target(r);
                merge_state
            })
        };
        write_peer_state(
            raft_wb,
            self.peer_id(),
            &region,
            PeerState::Tombstone,
            merge_state,
        );

        self.pending_reads.clear_all(Some(self.region_id));

        for Proposal { cb, .. } in self.proposals.queue.drain(..) {
            notify_req_region_removed(self.region_id, cb);
        }

        info!(
            "peer destroy itself";
            "tag" => self.tag(),
            "peer_id" => self.peer.get_id(),
            "takes" => ?t.saturating_elapsed(),
        );

        Ok(())
    }

    #[inline]
    pub fn is_initialized(&self) -> bool {
        self.get_store().is_initialized()
    }

    #[inline]
    pub fn region(&self) -> &metapb::Region {
        self.get_store().region()
    }

    /// Set the region of a peer.
    ///
    /// This will update the region of the peer, caller must ensure the region
    /// has been preserved in a durable device.
    pub fn set_region(
        &mut self,
        host: &coprocessor::CoprocessorHost,
        region: metapb::Region,
        reason: RegionChangeReason,
    ) {
        if self.region().get_region_epoch().get_version() < region.get_region_epoch().get_version()
        {
            // Epoch version changed, disable read on the localreader for this region.
            self.leader_lease.expire_remote_lease();
        }
        if is_epoch_stale(
            self.get_preprocessed_region().get_region_epoch(),
            region.get_region_epoch(),
        ) {
            self.mut_store().preprocessed_region = None;
        }
        self.mut_store().set_region(region);
        self.keyspace_id =
            ApiV2::get_u32_keyspace_id_by_key(self.region().get_start_key()).unwrap_or_default();

        if !self.pending_remove {
            host.on_region_changed(
                self.region(),
                coprocessor::RegionChangeEvent::Update(reason),
                self.get_role(),
            );
        }
    }

    #[inline]
    pub fn peer_id(&self) -> u64 {
        self.peer.get_id()
    }

    #[inline]
    pub fn leader_id(&self) -> u64 {
        self.raft_group.raft.leader_id
    }

    #[inline]
    pub fn is_leader(&self) -> bool {
        self.raft_group.raft.state == raft::StateRole::Leader
    }

    #[inline]
    pub fn get_role(&self) -> raft::StateRole {
        self.raft_group.raft.state
    }

    #[inline]
    pub fn get_store(&self) -> &PeerStorage {
        self.raft_group.store()
    }

    #[inline]
    pub fn mut_store(&mut self) -> &mut PeerStorage {
        self.raft_group.mut_store()
    }

    #[inline]
    pub fn is_applying_snapshot(&self) -> bool {
        self.get_store().is_applying_snapshot()
    }

    /// Returns `true` if the raft group has replicated a snapshot but not
    /// committed it yet.
    #[inline]
    pub fn has_pending_snapshot(&self) -> bool {
        self.get_pending_snapshot().is_some()
    }

    pub fn dependents_is_empty_or_only_self(&self, ctx: &RaftContext) -> bool {
        let mut empty_or_only_self = true;
        ctx.global
            .engines
            .raft
            .with_dependents(self.region_id, |dependents| {
                empty_or_only_self = dependents.is_empty()
                    || (dependents.len() == 1 && dependents.contains(&self.region_id))
            });
        empty_or_only_self
    }

    #[inline]
    pub fn get_pending_snapshot(&self) -> Option<&eraftpb::Snapshot> {
        self.raft_group.snap()
    }

    #[allow(unused)]
    fn add_ready_metric(&self, ready: &Ready, metrics: &mut RaftMetrics) {
        metrics.ready.message.inc_by(ready.messages().len() as u64);
        metrics
            .ready
            .commit
            .inc_by(ready.committed_entries().len() as u64);
        metrics.ready.append.inc_by(ready.entries().len() as u64);

        if !ready.snapshot().is_empty() {
            metrics.ready.snapshot.inc();
        }
    }

    #[allow(unused)]
    fn add_light_ready_metric(&self, light_ready: &LightReady, metrics: &mut RaftMetrics) {
        metrics
            .ready
            .message
            .inc_by(light_ready.messages().len() as u64);
        metrics
            .ready
            .commit
            .inc_by(light_ready.committed_entries().len() as u64);
    }

    #[allow(unused)]
    #[inline]
    pub fn in_joint_state(&self) -> bool {
        self.region().get_peers().iter().any(|p| {
            p.get_role() == PeerRole::IncomingVoter || p.get_role() == PeerRole::DemotingVoter
        })
    }

    #[inline]
    pub fn send_raft_messages(&mut self, ctx: &mut RaftContext, msgs: Vec<RaftMessage>) {
        let start = tikv_util::time::Instant::now_coarse();
        let trans = if ctx.worker_type == WorkerType::Idle {
            &mut ctx.global.trans_idle
        } else {
            &mut ctx.global.trans
        };
        for msg in msgs {
            let msg_type = msg.get_message().get_msg_type();
            if msg_type == MessageType::MsgSnapshot {
                let snap_index = msg.get_message().get_snapshot().get_metadata().get_index();
                if snap_index > self.last_sent_snapshot_idx {
                    self.last_sent_snapshot_idx = snap_index;
                }
                let to_peer_id = msg.get_to_peer().get_id();
                self.pending_snapshot_acks
                    .insert(to_peer_id, (snap_index, Instant::now_coarse()));
                let pr = self.raft_group.raft.prs().get(to_peer_id).unwrap();
                let store = self.get_store();
                let truncated_idx = store.truncated_index();
                let truncated_term = store.truncated_term();
                let tag = store.tag();
                info!(
                    "{} send snapshot idx {} to_peer {}, truncated_idx {}, truncated term {}, progress {:?}",
                    tag, snap_index, to_peer_id, truncated_idx, truncated_term, pr,
                )
            }
            if msg_type == MessageType::MsgTimeoutNow && self.is_leader() {
                // After a leader transfer procedure is triggered, the lease for
                // the old leader may be expired earlier than usual, since a new leader
                // may be elected and the old leader doesn't step down due to
                // network partition from the new leader.
                // For lease safety during leader transfer, transit `leader_lease`
                // to suspect.
                self.leader_lease.suspect(monotonic_raw_now());
            }
            let to_peer_id = msg.get_to_peer().get_id();
            let to_store_id = msg.get_to_peer().get_store_id();

            // Skip replicating to learner. See `learner_skip_idx`.
            if self.learner_skip_idx > 0
                && msg_type == MessageType::MsgAppend
                && msg.get_message().get_index() <= self.learner_skip_idx
                && msg.get_to_peer().get_role() == PeerRole::Learner
            {
                info!(
                    "SKIP send raft msg to learner";
                    "tag" => self.tag(),
                    "peer_id" => self.peer.get_id(),
                    "msg_type" => ?msg_type,
                    "msg_size" => msg.get_message().compute_size(),
                    "to" => to_peer_id,
                    "learner_skip_idx" => self.learner_skip_idx,
                    "msg_index" => msg.get_message().get_index(),
                );
                continue;
            }

            debug!(
                "send raft msg";
                "tag" => self.tag(),
                "peer_id" => self.peer.get_id(),
                "msg_type" => ?msg_type,
                "msg_size" => msg.get_message().compute_size(),
                "to" => to_peer_id,
                "disk_usage" => ?msg.get_disk_usage(),
                "msg_index" => msg.get_message().get_index(),
            );

            if let Err(e) = trans.send(msg) {
                // We use metrics to observe failure on production.
                debug!(
                    "failed to send msg to other peer";
                    "tag" => self.tag(),
                    "peer_id" => self.peer.get_id(),
                    "target_peer_id" => to_peer_id,
                    "target_store_id" => to_store_id,
                    "err" => ?e,
                    "error_code" => %e.error_code(),
                );
                // unreachable store
                self.raft_group.report_unreachable(to_peer_id);
                if msg_type == eraftpb::MessageType::MsgSnapshot {
                    self.pending_snapshot_acks.remove(&to_peer_id);
                    self.raft_group
                        .report_snapshot(to_peer_id, SnapshotStatus::Failure);
                }
                ctx.raft_metrics.send_message.add(msg_type, false);
            } else {
                ctx.raft_metrics.send_message.add(msg_type, true);
            }
        }
        let duration = start.saturating_elapsed();
        if duration > SLOW_LOG_DURATION {
            warn!(
                "{} send raft messages takes too long {:?}",
                self.tag(),
                duration,
            );
        }
    }

    fn build_raft_message(&mut self, msg: eraftpb::Message) -> Option<RaftMessage> {
        let mut send_msg = self.prepare_raft_message();

        let to_peer = match self.get_peer_from_cache(msg.get_to()) {
            Some(p) => p,
            None => {
                warn!(
                    "failed to look up recipient peer";
                    "tag" => self.tag(),
                    "peer_id" => self.peer.get_id(),
                    "to_peer" => msg.get_to(),
                );
                return None;
            }
        };

        send_msg.set_to_peer(to_peer);

        // There could be two cases:
        // - Target peer already exists but has not established communication with
        //   leader yet
        // - Target peer is added newly due to member change or region split, but it's
        //   not created yet
        // For both cases the region start key and end key are attached in RequestVote
        // and Heartbeat message for the store of that peer to check whether to create a
        // new peer when receiving these messages, or just to wait for a pending region
        // split to perform later.
        if self.get_store().is_initialized() && is_initial_msg(&msg) {
            let region = self.region();
            send_msg.set_start_key(region.get_start_key().to_vec());
            send_msg.set_end_key(region.get_end_key().to_vec());
        }

        send_msg.set_message(msg);

        Some(send_msg)
    }

    #[inline]
    pub fn build_raft_messages(
        &mut self,
        _ctx: &RaftContext,
        msgs: Vec<eraftpb::Message>,
    ) -> Vec<RaftMessage> {
        let mut raft_msgs = Vec::with_capacity(msgs.len());
        for msg in msgs {
            if let Some(m) = self.build_raft_message(msg) {
                raft_msgs.push(m);
            }
        }
        raft_msgs
    }

    /// Steps the raft message.
    pub fn step(&mut self, ctx: &mut RaftContext, mut m: eraftpb::Message) -> Result<()> {
        fail_point!(
            "step_message_3_1",
            self.peer.get_store_id() == 3 && self.region_id == 1,
            |_| Ok(())
        );
        self.maybe_report_snapshot(&m);
        if self.is_leader() && m.get_from() != raft::INVALID_ID {
            self.peer_heartbeats.insert(m.get_from(), Instant::now());
            // As the leader we know we are not missing.
            self.leader_missing_time.take();
        } else if m.get_from() == self.leader_id() {
            // As another role know we're not missing.
            self.leader_missing_time.take();
        }
        let msg_type = m.get_msg_type();
        if msg_type == MessageType::MsgReadIndex {
            fail_point!("on_step_read_index_msg");
            ctx.global
                .coprocessor_host
                .on_step_read_index(&mut m, self.get_role());
            // Must use the commit index of `PeerStorage` instead of the commit index
            // in raft-rs which may be greater than the former one.
            // For more details, see the annotations above `on_leader_commit_idx_changed`.
            let index = self.get_store().commit_index();
            // Check if the log term of this index is equal to current term, if so,
            // this index can be used to reply the read index request if the leader holds
            // the lease. Please also take a look at raft-rs.
            if self.get_store().term(index).unwrap() == self.term() {
                let state = self.inspect_lease();
                if let LeaseState::Valid = state {
                    // If current peer has valid lease, then we could handle the
                    // request directly, rather than send a heartbeat to check quorum.
                    let mut resp = eraftpb::Message::default();
                    resp.set_msg_type(MessageType::MsgReadIndexResp);
                    resp.term = self.term();
                    resp.to = m.from;

                    resp.index = index;
                    resp.set_entries(m.take_entries());

                    self.raft_group.raft.msgs.push(resp);
                    return Ok(());
                }
            }
        }
        self.raft_group.step(m)?;
        Ok(())
    }

    fn maybe_report_snapshot(&mut self, m: &eraftpb::Message) {
        if self.pending_snapshot_acks.is_empty()
            || !self.is_leader()
            || m.get_msg_type() != MessageType::MsgAppendResponse
        {
            return;
        }
        let from = m.get_from();
        let Some(&(snap_index, _)) = self.pending_snapshot_acks.get(&from) else {
            return;
        };
        if (m.get_term(), m.get_index()) < (self.term(), snap_index) {
            // stale message
            return;
        }
        let status = if m.get_reject() {
            SnapshotStatus::Failure
        } else {
            SnapshotStatus::Finish
        };
        self.pending_snapshot_acks.remove(&from);
        self.raft_group.report_snapshot(from, status);
        info!("{} snapshot {:?} from peer {}", self.tag(), status, from);
    }

    /// Report snapshot failure if follower never acknowledges within timeout.
    pub(crate) fn check_snapshot_ack_timeout(&mut self) {
        if self.pending_snapshot_acks.is_empty() {
            return;
        }
        let now = Instant::now_coarse();
        let expired_peers: Vec<u64> = self
            .pending_snapshot_acks
            .iter()
            .filter_map(|(&peer_id, &(_, sent_at))| {
                (now.saturating_duration_since(sent_at) > self.wait_snapshot_ack_timeout)
                    .then_some(peer_id)
            })
            .collect();
        for peer_id in expired_peers {
            self.pending_snapshot_acks.remove(&peer_id);
            self.raft_group
                .report_snapshot(peer_id, SnapshotStatus::Failure);
            info!("{} snapshot timeout for peer {}", self.tag(), peer_id);
        }
    }

    /// Checks and updates `peer_heartbeats` for the peer.
    pub fn check_peers(&mut self) {
        if !self.is_leader() {
            self.peer_heartbeats.clear();
            self.peers_start_pending_time.clear();
            return;
        }

        if self.peer_heartbeats.len() == self.region().get_peers().len() {
            return;
        }

        // Insert heartbeats in case that some peers never response heartbeats.
        let region = self.raft_group.store().region();
        for peer in region.get_peers() {
            self.peer_heartbeats
                .entry(peer.get_id())
                .or_insert_with(Instant::now);
        }
    }

    /// Collects all down peers.
    pub fn collect_down_peers(&mut self, max_duration: Duration) -> Vec<PeerStats> {
        let mut down_peers = Vec::new();
        let mut down_peer_ids = Vec::new();
        for p in self.region().get_peers() {
            if p.get_id() == self.peer.get_id() {
                continue;
            }
            if let Some(instant) = self.peer_heartbeats.get(&p.get_id()) {
                if instant.saturating_elapsed() >= max_duration {
                    let mut stats = PeerStats::new();
                    stats.set_peer(p.clone());
                    stats.set_down_seconds(instant.saturating_elapsed().as_secs());
                    down_peers.push(stats);
                    down_peer_ids.push(p.get_id());
                }
            }
        }
        self.down_peer_ids = down_peer_ids;
        down_peers
    }

    /// Collects all pending peers and update `peers_start_pending_time`.
    pub fn collect_pending_peers(&mut self) -> Vec<metapb::Peer> {
        let mut pending_peers = Vec::with_capacity(self.region().get_peers().len());
        let status = self.raft_group.status();
        let truncated_idx = self.get_store().truncated_index();

        if status.progress.is_none() {
            return pending_peers;
        }

        // TODO: add metrics
        let progresses = status.progress.unwrap().iter();
        for (&id, progress) in progresses {
            if id == self.peer.get_id() {
                continue;
            }
            // The `matched` is 0 only in these two cases:
            // 1. Current leader hasn't communicated with this peer.
            // 2. This peer does not exist yet(maybe it is created but not initialized)
            //
            // The correctness of region merge depends on the fact that all target peers
            // must exist during merging. (PD rely on `pending_peers` to check
            // whether all target peers exist)
            //
            // So if the `matched` is 0, it must be a pending peer.
            // It can be ensured because `truncated_index` must be greater than
            // `RAFT_INIT_LOG_INDEX`(5).
            if progress.matched < truncated_idx {
                if let Some(p) = self.get_peer_from_cache(id) {
                    pending_peers.push(p);
                    if !self
                        .peers_start_pending_time
                        .iter()
                        .any(|&(pid, _)| pid == id)
                    {
                        let now = Instant::now();
                        self.peers_start_pending_time.push((id, now));
                        debug!(
                            "peer start pending";
                            "tag" => self.tag(),
                            "peer_id" => self.peer.get_id(),
                            "time" => ?now,
                        );
                    }
                } else {
                    error!(
                        "failed to get peer from cache";
                        "tag" => self.tag(),
                        "peer_id" => self.peer.get_id(),
                        "get_peer_id" => id,
                    );
                }
            }
        }
        pending_peers
    }

    /// Returns `true` if any peer recover from connectivity problem.
    ///
    /// A peer can become pending or down if it has not responded for a
    /// long time. If it becomes normal again, PD need to be notified.
    pub fn any_new_peer_catch_up(&mut self, peer_id: u64) -> bool {
        if self.peers_start_pending_time.is_empty() && self.down_peer_ids.is_empty() {
            return false;
        }
        if !self.is_leader() {
            self.down_peer_ids = vec![];
            self.peers_start_pending_time = vec![];
            return false;
        }
        for i in 0..self.peers_start_pending_time.len() {
            if self.peers_start_pending_time[i].0 != peer_id {
                continue;
            }
            let truncated_idx = self.raft_group.store().truncated_index();
            if let Some(progress) = self.raft_group.raft.prs().get(peer_id) {
                if progress.matched >= truncated_idx {
                    let (_, pending_after) = self.peers_start_pending_time.swap_remove(i);
                    let elapsed = duration_to_sec(pending_after.saturating_elapsed());
                    RAFT_PEER_PENDING_DURATION.observe(elapsed);
                    debug!(
                        "peer has caught up logs";
                        "region_id" => self.region_id,
                        "peer_id" => self.peer.get_id(),
                        "takes" => elapsed,
                    );
                    return true;
                }
            }
        }
        if self.down_peer_ids.contains(&peer_id) {
            return true;
        }
        false
    }

    pub fn check_stale_state(&mut self, cfg: &Config) -> StaleState {
        if self.is_leader() {
            // Leaders always have valid state.
            //
            // We update the leader_missing_time in the `fn step`. However one peer region
            // does not send any raft messages, so we have to check and update it before
            // reporting stale states.
            self.leader_missing_time = None;
            return StaleState::Valid;
        }
        let naive_peer = !self.is_initialized() || !self.raft_group.raft.promotable();
        // Updates the `leader_missing_time` according to the current state.
        //
        // If we are checking this it means we suspect the leader might be missing.
        // Mark down the time when we are called, so we can check later if it's been
        // longer than it should be.
        match self.leader_missing_time {
            None => {
                self.leader_missing_time = Instant::now().into();
                StaleState::Valid
            }
            Some(instant) if instant.saturating_elapsed() >= cfg.max_leader_missing_duration.0 => {
                // Resets the `leader_missing_time` to avoid sending the same tasks to
                // PD worker continuously during the leader missing timeout.
                self.leader_missing_time = Instant::now().into();
                StaleState::ToValidate
            }
            Some(instant)
                if instant.saturating_elapsed() >= cfg.abnormal_leader_missing_duration.0
                    && !naive_peer =>
            {
                // A peer is considered as in the leader missing state
                // if it's initialized but is isolated from its leader or
                // something bad happens that the raft group can not elect a leader.
                StaleState::LeaderMissing
            }
            _ => StaleState::Valid,
        }
    }

    pub fn send_want_rollback_merge(&self, premerge_commit: u64, ctx: &mut RaftContext) {
        let to_peer = match self.get_peer_from_cache(self.leader_id()) {
            Some(p) => p,
            None => {
                warn!(
                    "failed to look up recipient peer";
                    "tag" => self.tag(),
                    "peer_id" => self.peer.get_id(),
                    "to_peer" => self.leader_id(),
                );
                return;
            }
        };
        let mut extra_msg = ExtraMessage::default();
        extra_msg.set_type(ExtraMessageType::MsgWantRollbackMerge);
        extra_msg.set_index(premerge_commit);
        self.send_extra_message(extra_msg, &mut ctx.global.trans, &to_peer);
    }

    pub fn require_updating_max_ts(&self, pd_scheduler: &Scheduler<PdTask>) {
        let epoch = self.region().get_region_epoch();
        let term_low_bits = self.term() & ((1 << 32) - 1); // 32 bits
        let version_lot_bits = epoch.get_version() & ((1 << 31) - 1); // 31 bits
        let initial_status = (term_low_bits << 32) | (version_lot_bits << 1);
        self.txn_ext
            .max_ts_sync_status
            .store(initial_status, Ordering::SeqCst);
        info!(
            "require updating max ts";
            "tag" => self.tag(),
            "initial_status" => initial_status,
        );
        if let Err(e) = pd_scheduler.schedule(PdTask::UpdateMaxTimestamp {
            region_id: self.region_id,
            initial_status,
            txn_ext: self.txn_ext.clone(),
        }) {
            error!(
                "failed to update max ts";
                "err" => ?e,
            );
        }
    }

    pub fn notify_role_changed(&self, pd_scheduler: &Scheduler<PdTask>, role: StateRole) {
        let keyspace_id =
            api_version::ApiV2::get_u32_keyspace_id_by_key(self.region().get_start_key());
        if let Err(e) = pd_scheduler.schedule(PdTask::RoleChanged {
            region_id: self.region_id,
            keyspace_id,
            role,
        }) {
            error!(
                "failed to notify pd runner that peer's role has changed";
                "err" => ?e,
            );
        }
    }

    pub(crate) fn on_role_changed(&mut self, ctx: &mut RaftContext, ready: &Ready) {
        let start = tikv_util::time::Instant::now_coarse();
        // Update leader lease when the Raft state changes.
        if let Some(ss) = ready.ss() {
            match ss.raft_state {
                StateRole::Leader => {
                    // The local read can only be performed after a new leader has applied
                    // the first empty entry on its term. After that the lease expiring time
                    // should be updated to
                    //   send_to_quorum_ts + max_lease
                    // as the comments in `Lease` explain.
                    // It is recommended to update the lease expiring time right after
                    // this peer becomes leader because it's more convenient to do it here and
                    // it has no impact on the correctness.
                    let progress_term = ReadProgress::term(self.term());
                    self.maybe_renew_leader_lease(monotonic_raw_now(), ctx, Some(progress_term));
                    debug!(
                        "becomes leader with lease";
                        "tag" => self.tag(),
                        "peer_id" => self.peer.get_id(),
                        "lease" => ?self.leader_lease,
                    );
                    // If the predecessor reads index during transferring leader and receives
                    // quorum's heartbeat response after that, it may wait for applying to
                    // current term to apply the read. So broadcast eagerly to avoid unexpected
                    // latency.
                    //
                    // TODO: Maybe the predecessor should just drop all the read requests directly?
                    // All the requests need to be redirected in the end anyway and executing
                    // prewrites or commits will be just a waste.
                    self.last_urgent_proposal_idx = self.raft_group.raft.raft_log.last_index();
                    self.raft_group.skip_bcast_commit(false);
                    self.last_sent_snapshot_idx = self.raft_group.raft.raft_log.last_index();

                    // A more recent read may happen on the old leader. So max ts should
                    // be updated after a peer becomes leader.
                    self.require_updating_max_ts(&ctx.global.pd_scheduler);
                    self.heartbeat_pd(ctx);
                }
                StateRole::Follower => {
                    self.leader_lease.expire();
                }
                _ => {}
            }
            if ss.raft_state != StateRole::Leader {
                self.pending_snapshot_acks.clear();
            }

            self.notify_role_changed(&ctx.global.pd_scheduler, ss.raft_state);
            // TODO: it may possible that only the `leader_id` change and the role
            // didn't change
            ctx.global.coprocessor_host.on_role_change(
                self.region(),
                RoleChange {
                    state: ss.raft_state,
                    leader_id: ss.leader_id,
                    prev_lead_transferee: self.lead_transferee,
                    vote: self.raft_group.raft.vote,
                },
            );
            self.cmd_epoch_checker.maybe_update_term(self.term());
            self.get_store()
                .snapshot_not_ready_peers
                .borrow_mut()
                .clear();
        }
        self.lead_transferee = self.raft_group.raft.lead_transferee.unwrap_or_default();
        let duration = start.saturating_elapsed();
        if duration > SLOW_LOG_DURATION {
            warn!(
                "{} on_role_changed takes too long {:?}",
                self.tag(),
                duration,
            );
        }
    }

    pub fn insert_peer_cache(&mut self, peer: metapb::Peer) {
        self.peer_cache.borrow_mut().insert(peer.get_id(), peer);
    }

    pub fn remove_peer_from_cache(&mut self, peer_id: u64) {
        self.peer_cache.borrow_mut().remove(&peer_id);
    }

    pub fn get_peer_from_cache(&self, peer_id: u64) -> Option<metapb::Peer> {
        if peer_id == 0 {
            return None;
        }
        fail_point!("stale_peer_cache_2", peer_id == 2, |_| None);
        if let Some(peer) = self.peer_cache.borrow().get(&peer_id) {
            return Some(peer.clone());
        }

        // Try to find in region, if found, set in cache.
        for peer in self.region().get_peers() {
            if peer.get_id() == peer_id {
                self.peer_cache.borrow_mut().insert(peer_id, peer.clone());
                return Some(peer.clone());
            }
        }

        None
    }

    pub fn update_buckets(&mut self, ctx: &RaftContext) {
        if !ctx.cfg.enable_region_bucket {
            return;
        }
        if !self.has_applied_to_current_term() {
            return;
        }
        if let Some(shard) = ctx.global.engines.kv.get_shard(self.region_id) {
            // Update the buckets whenever the shard meta sequence change, so the bucket
            // keys would be more accurate than size diff.
            let meta_sequence = shard.get_meta_sequence();
            if meta_sequence == self.last_bucket_update_meta_sequence {
                debug!("{} update_buckets:skip", self.tag();
                    "last_bucket_update_meta_sequence" => self.last_bucket_update_meta_sequence);
                return;
            }
            self.last_bucket_update_meta_sequence = meta_sequence;
            let origin_bucket_size = ctx.cfg.region_bucket_size.0;
            let mut bucket_size = origin_bucket_size;
            if shard.has_vector_index() {
                // increase the bucket size to better serve vector index.
                bucket_size *= 2;
            }
            let estimated_size = shard.get_estimated_size();
            let expected_bucket_count = estimated_size.div_ceil(bucket_size);
            let mut bucket_keys = vec![self.region().get_start_key().to_vec()];
            if let Some(keys) =
                shard.get_evenly_split_keys(expected_bucket_count as usize, origin_bucket_size)
            {
                bucket_keys.extend(
                    keys.into_iter()
                        .map(|k| Key::from_raw(k.chunk()).into_encoded()),
                );
            }
            if shard.ver > self.region().get_region_epoch().version {
                // The shard's version may be greater than the region's version, then the
                // returned bucket_keys may exceed the region's range. So we need to exclude the
                // out of range keys.
                bucket_keys.retain(|k| check_key_in_region(k, self.region()).is_ok());
                bucket_keys.dedup();
            }
            bucket_keys.push(self.region().get_end_key().to_vec());
            if let Some(old_buckets) = self.buckets.as_ref() {
                if old_buckets.meta.keys == bucket_keys {
                    // Skip update if the keys are same.
                    info!("{} skip update buckets for same keys", self.tag());
                    return;
                }
            }
            let mut bucket_meta = BucketMeta::new(self.region(), bucket_keys, bucket_size);
            bucket_meta.version = self.bucket_version;
            bucket_meta.incr_version(self.term());
            info!(
                "{} update buckets version {}, keys {}, estimated_size {}",
                self.tag(),
                bucket_meta.version,
                bucket_meta.keys.len(),
                estimated_size
            );
            let stats = new_bucket_write_stats(&bucket_meta);
            let bucket_meta = Arc::new(bucket_meta);
            let bucket_stat = BucketStat::new(bucket_meta.clone(), stats);
            self.bucket_version = bucket_stat.meta.version;
            self.buckets = Some(bucket_stat);
            let readers = ctx.global.readers.pin();
            readers.update(self.region_id, |old| {
                let mut new_reader = old.clone_for_update();
                new_reader.update(ReadProgress::RegionBuckets(
                    self.buckets.as_ref().unwrap().meta.clone(),
                ));
                Arc::new(new_reader)
            });
            ctx.global.coprocessor_host.on_region_changed(
                self.region(),
                RegionChangeEvent::UpdateBuckets(bucket_meta),
                self.get_role(),
            );
        }
    }

    pub fn heartbeat_pd(&mut self, ctx: &RaftContext) {
        self.update_buckets(ctx);
        let bucket_stat = self.buckets.as_mut().map(|buckets| {
            let stats =
                std::mem::replace(&mut buckets.stats, new_bucket_write_stats(&buckets.meta));
            BucketStat {
                meta: buckets.meta.clone(),
                stats,
                create_time: buckets.create_time,
            }
        });
        // If the number of columnar tables in the shard is greater than
        // MAX_COLUMNAR_TABLES_IN_SHARD, set the approximate keys to (table_count *
        // 100_000) to avoid pd scheduling merge region.
        // NOTE: as a side effect, if the table count is greater than
        // MAX_COLUMNAR_TABLES_IN_SHARD, the approximate keys is inaccurate.
        let approximate_keys = self
            .raft_group
            .store()
            .shard_meta
            .as_ref()
            .map(|shard_meta| {
                if shard_meta.columnar_table_ids.len() > LARGE_NUM_COLUMNAR_TABLES_IN_SHARD {
                    estimated_entries_by_table_count(shard_meta.columnar_table_ids.len())
                } else {
                    self.peer_stat.approximate_keys
                }
            })
            .unwrap_or(self.peer_stat.approximate_keys);
        let task = PdTask::Heartbeat(HeartbeatTask {
            term: self.term(),
            region: self.region().clone(),
            peer: self.peer.clone(),
            down_peers: self.collect_down_peers(ctx.cfg.max_peer_down_duration.0),
            pending_peers: self.collect_pending_peers(),
            written_bytes: self.peer_stat.written_bytes,
            written_keys: self.peer_stat.written_keys,
            approximate_size: self.peer_stat.approximate_size,
            approximate_keys,
            approximate_kv_size: self.peer_stat.approximate_kv_size,
            approximate_columnar_size: self.peer_stat.approximate_columnar_size,
            approximate_columnar_kv_size: self.peer_stat.approximate_columnar_kv_size,
            replication_status: None,
            bucket_stat,
        });
        if let Err(e) = ctx.global.pd_scheduler.schedule(task) {
            error!(
                "failed to notify pd";
                "tag" => self.tag(),
                "peer_id" => self.peer.get_id(),
                "err" => ?e,
            );
            return;
        }
        fail_point!("schedule_check_split");
    }

    fn prepare_raft_message(&self) -> RaftMessage {
        let mut send_msg = RaftMessage::default();
        send_msg.set_region_id(self.region_id);
        // set current epoch
        send_msg.set_region_epoch(self.region().get_region_epoch().clone());
        send_msg.set_from_peer(self.peer.clone());
        send_msg
    }

    pub fn send_extra_message(
        &self,
        msg: ExtraMessage,
        trans: &mut Box<dyn Transport>,
        to: &metapb::Peer,
    ) {
        let mut send_msg = self.prepare_raft_message();
        let ty = msg.get_type();
        debug!("send extra msg";
            "tag" => self.tag(),
            "peer_id" => self.peer.get_id(),
            "msg_type" => ?ty,
            "to" => to.get_id()
        );
        send_msg.set_extra_msg(msg);
        send_msg.set_to_peer(to.clone());
        if let Err(e) = trans.send(send_msg) {
            error!(?e;
                "failed to send extra message";
                "type" => ?ty,
                "tag" => self.tag(),
                "peer_id" => self.peer.get_id(),
                "target" => ?to,
            );
        }
    }

    pub(crate) fn handle_raft_ready(
        &mut self,
        ctx: &mut RaftContext,
        store_meta: Option<&mut StoreMeta>,
    ) {
        if self.pending_remove {
            return;
        }
        if self.get_store().is_applying_snapshot() {
            // If we continue to handle all the messages, it may cause too many messages
            // because leader will send all the remaining messages to this
            // follower, which can lead to full message queue under high load.
            return;
        }
        let handle_start = tikv_util::time::Instant::now_coarse();
        if !self.raft_group.has_ready() {
            return;
        }
        if self.has_pending_snapshot() {
            if !self.dependents_is_empty_or_only_self(ctx) {
                // We can't apply snapshot until dependents are empty, because applying snapshot
                // will clear meta and truncate raft logs which makes other peers fail
                // to recover. If ourself is the last dependent, we can apply snapshot.
                debug!(
                    "not ready to handle pending snapshot, skip";
                    "tag" => self.tag(),
                    "peer_id" => self.peer.get_id(),
                );
                return;
            }
            if store_meta.is_none() {
                ctx.global
                    .router
                    .send_store(StoreMsg::SnapshotReady(self.region_id));
                return;
            }
        }
        if self.reported_leader_id != self.raft_group.raft.leader_id {
            self.reported_leader_id = self.raft_group.raft.leader_id;
            ctx.global.coprocessor_host.on_region_changed(
                self.region(),
                RegionChangeEvent::UpdateLeader(self.reported_leader_id),
                self.get_role(),
            );
        }
        let mut ready = self.raft_group.ready();
        let ready_stats = ReadyStats::new(&ready);
        self.on_role_changed(ctx, &ready);
        self.add_ready_metric(&ready, &mut ctx.raft_metrics);

        // TODO(x) on leader commit index change.

        if ready_stats.messages > 0 {
            assert!(self.is_leader());
            let raft_msgs = self.build_raft_messages(ctx, ready.take_messages());
            self.send_raft_messages(ctx, raft_msgs);
        }

        self.apply_reads(ctx, &ready);

        let new_role = ready.ss().map(|ss| ss.raft_state);
        self.handle_raft_committed_entries(ctx, ready.take_committed_entries(), new_role);
        let last_preprocessed_index = self.last_applying_idx;
        if let Some(snap_res) =
            self.mut_store()
                .handle_raft_ready(ctx, &mut ready, last_preprocessed_index)
        {
            self.last_applying_idx = snap_res.snap_last_index;
            self.mut_store().preprocessed_region = None;
            self.encryption_key = self.get_store().get_encryption_key();
            // The peer may change from learner to voter after snapshot persisted.
            let peer = self
                .region()
                .get_peers()
                .iter()
                .find(|p| p.get_id() == self.peer.get_id())
                .unwrap()
                .clone();
            if peer != self.peer {
                info!(
                    "meta changed after restored snapshot";
                    "tag" => self.tag(),
                    "peer_id" => self.peer.get_id(),
                    "before" => ?self.peer,
                    "after" => ?peer,
                );
                self.peer = peer;
            };
            if !self.update_store_meta_for_snap(snap_res, store_meta.unwrap()) {
                return;
            }
        }
        if let Some(ss) = ready.ss() {
            if ss.raft_state == raft::StateRole::Leader {
                self.heartbeat_pd(ctx)
            }
        }
        let mut persist_messages = self.build_raft_messages(ctx, ready.take_persisted_messages());
        if !ready.snapshot().is_empty() {
            self.mut_store().on_apply_snapshot_msgs = std::mem::take(&mut persist_messages);
        }
        if ctx.worker_type == WorkerType::Idle {
            // avoid persist ready message to wake up idle peer.
            self.send_raft_messages(ctx, persist_messages);
            let ready_number = ready.number();
            self.raft_group.advance_append_async(ready);
            self.on_persist_ready(ctx, ready_number);
            return;
        }
        ctx.persist_readies.push(PersistReady {
            region_id: self.region_id,
            ready_number: ready.number(),
            peer_id: self.peer_id(),
            _commit_idx: self.get_store().commit_index(),
            raft_messages: persist_messages,
        });
        self.raft_group.advance_append_async(ready);
        let handle_end = tikv_util::time::Instant::now_coarse();
        let handle_duration = handle_end.saturating_duration_since(handle_start);
        if handle_duration > SLOW_LOG_DURATION {
            warn!(
                "{} raft ready takes too long, ready:{:?}, handle_takes:{:?}",
                self.tag(),
                ready_stats,
                handle_duration,
            );
        }
    }

    pub(crate) fn on_persist_ready(&mut self, ctx: &mut RaftContext, ready_number: u64) {
        // If peer is set `pending_remove`, no need to update persist index.
        if !self.pending_remove {
            self.raft_group.on_persist_ready(ready_number);
        }

        let store = self.mut_store();
        let is_snapshot_ready = store
            .restored_snapshot
            .as_ref()
            .map(|(_, number)| *number == ready_number)
            .unwrap_or_default();
        if is_snapshot_ready {
            let change_set = store.restored_snapshot.take().unwrap().0;
            let reg = MsgRegistration::new(self);
            ctx.apply_msgs.msgs.push(ApplyMsg::Registration(reg));
            ctx.apply_msgs.msgs.push(ApplyMsg::PrepareChangeSet {
                cs: change_set,
                encryption_key: self.encryption_key.clone(),
                reload_snap: None,
                prepare_type: FilePrepareType::Local, // reset by snapshot
            });
        }
    }

    pub(crate) fn handle_raft_committed_entries(
        &mut self,
        ctx: &mut RaftContext,
        committed_entries: Vec<Entry>,
        new_role: Option<raft::StateRole>,
    ) {
        if committed_entries.is_empty() && new_role.is_none() {
            return;
        }
        fail_point!(
            "before_leader_handle_committed_entries",
            self.is_leader(),
            |_| ()
        );
        let tag = self.tag();

        assert!(
            !self.is_applying_snapshot(),
            "{} is applying snapshot when it is ready to handle committed entries",
            tag
        );
        let start = tikv_util::time::Instant::now_coarse();
        // Leader needs to update lease.
        let mut lease_to_be_updated = self.is_leader();
        for entry in committed_entries.iter().rev() {
            // raft meta is very small, can be ignored.
            if lease_to_be_updated {
                let propose_time = self
                    .proposals
                    .find_propose_time(entry.get_term(), entry.get_index());
                if let Some(propose_time) = propose_time {
                    // We must renew current_time because this value may be created a long time ago.
                    // If we do not renew it, this time may be smaller than propose_time of a
                    // command, which was proposed in another thread while this
                    // thread receives its AppendEntriesResponse and is ready to
                    // calculate its commit-log-duration.
                    ctx.current_time.replace(monotonic_raw_now());
                    ctx.raft_metrics.commit_log.observe(duration_to_sec(
                        (ctx.current_time.unwrap() - propose_time).to_std().unwrap(),
                    ));
                    self.maybe_renew_leader_lease(propose_time, ctx, None);
                    lease_to_be_updated = false;
                }
            }
        }
        let mut preprocess_ctx = PreprocessContext::from_raft_context(ctx);
        let mut preprocess_ref = PreprocessRef::from_peer(self);
        let mut preprocess_errors = PreprocessErrors::default();
        for entry in &committed_entries {
            if let Some(err) = preprocess_ref.preprocess_committed_entry(&mut preprocess_ctx, entry)
            {
                preprocess_errors.insert_error(entry.term, entry.index, err);
            }
        }
        self.build_apply_msg(ctx, committed_entries, new_role, preprocess_errors);
        let duration = start.saturating_elapsed();
        if duration > SLOW_LOG_DURATION {
            warn!(
                "{} handle committed entries takes too long {:?}",
                tag, duration
            );
        }
    }

    fn build_apply_msg(
        &mut self,
        ctx: &mut RaftContext,
        committed_entries: Vec<Entry>,
        new_role: Option<raft::StateRole>,
        mut preprocess_errors: PreprocessErrors,
    ) {
        if let Some(last_entry) = committed_entries.last() {
            self.last_applying_idx = last_entry.get_index();
            if self.last_applying_idx >= self.last_urgent_proposal_idx {
                // Urgent requests are flushed, make it lazy again.
                self.raft_group.skip_bcast_commit(true);
                self.last_urgent_proposal_idx = u64::MAX;
            }
        }
        let tag = self.tag();
        let cbs = if !self.proposals.is_empty() {
            let current_term = self.term();
            let cbs = committed_entries
                .iter()
                .filter_map(|e| {
                    self.proposals.find_proposal(
                        tag,
                        e.get_term(),
                        e.get_index(),
                        current_term,
                        &mut preprocess_errors,
                    )
                })
                .map(|mut p| {
                    if p.must_pass_epoch_check {
                        // In this case the apply can be guaranteed to be successful. Invoke the
                        // on_committed callback if necessary.
                        p.cb.invoke_committed();
                    }
                    p
                })
                .collect();
            self.proposals.gc();
            cbs
        } else {
            vec![]
        };
        let apply_msg = ApplyMsg::Apply(MsgApply {
            term: self.term(),
            entries: committed_entries,
            new_role,
            cbs,
            bucket_meta: self.buckets.as_ref().map(|b| b.meta.clone()),
        });
        ctx.apply_msgs.msgs.push(apply_msg);
        fail_point!("after_send_to_apply_1003", self.peer_id() == 1003, |_| {});
    }
}

// wrap in `Option` to reduce memory usage, as in most time there should be no
// error.
#[derive(Default)]
pub struct PreprocessErrors(Option<HashMap<(u64 /* term */, u64 /* index */), Error>>);

impl PreprocessErrors {
    pub fn insert_error(&mut self, term: u64, index: u64, err: Error) {
        if self.0.is_none() {
            self.0 = Some(HashMap::default());
        }
        self.0.as_mut().unwrap().insert((term, index), err);
    }

    pub fn take_error(&mut self, term: u64, index: u64) -> Option<Error> {
        self.0.as_mut().and_then(|m| m.remove(&(term, index)))
    }
}

// TODO: move to individual file.
impl PreprocessRef<'_> {
    /// `preprocess_committed_entry` would process entries from applied index
    /// (of kvengine, during restore from snapshot or backup). So it should
    /// be able to properly handle entries before `ShardMeta.seq`.
    ///
    /// Return whether there is error during preprocessing committed entry which
    /// should be informed caller by callback.
    pub fn preprocess_committed_entry(
        &mut self,
        ctx: &mut PreprocessContext<'_>,
        entry: &Entry,
    ) -> Option<Error> {
        debug_assert!(entry.index > 0, "{} invalid entry: {:?}", self.tag(), entry);
        let mut preprocess_err = None;
        if *self.preprocessed_index > 0 && entry.index <= *self.preprocessed_index {
            return None;
        }
        *self.preprocessed_index = entry.index;
        if let Some(cmd) = get_preprocess_cmd(entry) {
            if cmd.has_custom_request() {
                let custom_req = cmd.get_custom_request();
                if is_engine_meta_log(custom_req.get_data()) {
                    if let Err(e) = self.preprocess_change_set(ctx, entry, custom_req) {
                        preprocess_err = Some(e);
                    }
                } else if is_txn_file_ref(custom_req.get_data()) {
                    if let Err(e) = self.preprocess_txn_file_ref(ctx, entry, custom_req) {
                        preprocess_err = Some(e);
                    }
                }
            } else {
                if let Err(err) = check_region_epoch(&cmd, self.get_preprocessed_region(), false) {
                    warn!("{} preprocess pending admin failed {:?}", self.tag(), err);
                    return None;
                }
                let admin = cmd.get_admin_request();
                if admin.has_splits() {
                    if let Err(e) = self.preprocess_pending_splits(ctx, entry, &cmd) {
                        preprocess_err = Some(e);
                    }
                } else if admin.has_prepare_merge() {
                    self.preprocess_prepare_merge(ctx, entry, &cmd);
                } else if admin.has_rollback_merge() {
                    self.preprocess_rollback_merge(ctx, entry, &cmd);
                } else if admin.has_commit_merge() {
                    self.preprocess_commit_merge(ctx, entry, &cmd);
                }
            }
        } else if entry.entry_type != eraftpb::EntryType::EntryNormal {
            self.preprocess_conf_change(ctx, entry);
        }
        preprocess_err
    }

    pub(crate) fn preprocess_change_set(
        &mut self,
        ctx: &mut PreprocessContext<'_>,
        entry: &Entry,
        custom_req: &CustomRequest,
    ) -> Result<()> {
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        let mut cs = custom_log.get_change_set().unwrap();
        cs.set_sequence(entry.get_index());
        let peer_id = self.peer_id();
        let region_id = self.region_id();
        let tag = self.tag();
        let opt_parent_id = self.parent_id();
        let shard_meta = self.shard_meta();
        let mut rejected = false;
        let mut err: Option<Error> = None;
        if shard_meta.ver != cs.get_shard_ver() {
            rejected = true;
            err = Some(Error::EpochNotMatch(
                format!(
                    "preprocess change set: current version of region {} is {}",
                    region_id, shard_meta.ver
                ),
                vec![self.region.clone()],
            ));
            warn!(
                "shard meta not match";
                "region" => tag,
                "shard_meta_ver" => shard_meta.ver,
                "version" => cs.get_shard_ver(),
            );
        } else if shard_meta.is_duplicated_change_set(&mut cs) {
            if self.is_leader {
                ENGINE_DUPLICATED_CHANGE_SET_COUNTER.inc();
            }
            rejected = true;
            warn!(
                "shard meta is duplicated change set";
                "region" => tag,
                "seq" => cs.get_sequence(),
            );
        } else if cs.has_ingest_files()
            && kvengine::is_legacy_ingest(cs.get_ingest_files()).is_none()
        {
            // It is not necessary to check overlap for legacy ingest.
            // See `Ingest::convert_sst` & `ShardMeta::get_ingest_level`.
            if let Some((existed_file_id, ingest_file_id)) =
                shard_meta.check_overlap_for_load_data(cs.get_ingest_files())
            {
                warn!(
                    "shard meta is overlapped ingest files";
                    "region" => tag,
                    "existed_file_id" => existed_file_id,
                    "ingest_file_id" => ingest_file_id,
                );
                return Err(Error::IngestOverlap {
                    region_id,
                    existed_file_id,
                    ingest_file_id,
                });
            }
        }
        if let Some(kv) = ctx.kv.as_ref() {
            // `ctx.kv` is `None` only in restore. In which we don't need `meta_committed`.
            kv.meta_committed(&cs, rejected);
        }
        if rejected {
            if let Some(err) = err {
                return Err(err);
            }
            return Ok(());
        }
        if cs.has_restore_shard() {
            self.preprocess_restore_shard(ctx, entry, &mut cs);
        }
        let shard_meta = self.mut_shard_meta();
        shard_meta.apply_change_set(&cs);
        info!(
            "shard meta apply change set {:?}", &cs;
            "region" => tag,
            "max_ts" => shard_meta.max_ts,
        );
        // During the cs process of updating the storage class property, we instantly
        // reload all the existing files `reload_snap`.
        // After cs of updating the storage class property, the new files of other cs
        // will be directly loaded according to the target storage class
        // `shard_use_ia` and applied normally.
        let reload_snap =
            if !cs.get_property_key().is_empty() && cs.get_property_key() == STORAGE_CLASS_KEY {
                shard_meta.collect_files_for_storage_class(&cs)
            } else {
                None
            };
        let prepare_type = FilePrepareType::from_shard_meta(shard_meta);
        if ctx.cfg.enable_kv_engine_meta_diff {
            write_engine_meta_diff(
                ctx.raft,
                ctx.raft_wb,
                peer_id,
                shard_meta,
                Some(&cs),
                ctx.cfg.kv_engine_meta_diff_rewrite_percent,
            );
        } else {
            write_engine_meta(ctx.raft_wb, peer_id, shard_meta);
        }
        if cs.has_initial_flush() || cs.has_snapshot() || cs.has_restore_shard() {
            if let Some(parent_id) = opt_parent_id {
                ctx.add_remove_dependent(parent_id, self.region_id());
            }
        }
        ctx.apply_msgs.msgs.push(ApplyMsg::PrepareChangeSet {
            cs,
            encryption_key: self.encryption_key.clone(),
            reload_snap,
            prepare_type,
        });
        Ok(())
    }

    fn preprocess_restore_shard(
        &mut self,
        ctx: &mut PreprocessContext<'_>,
        entry: &Entry,
        cs: &mut kvenginepb::ChangeSet,
    ) {
        debug_assert!(cs.has_restore_shard());

        let tag = self.tag();
        let peer_id = self.peer_id();

        // Increase region version to discard stale change sets.
        // Otherwise, change sets generated before but apply after restore shard will
        // result in undefined behavior.
        let mut new_region = self.get_preprocessed_region().clone();
        let region_version = new_region.get_region_epoch().get_version();
        debug_assert_eq!(cs.shard_ver, region_version);
        new_region
            .mut_region_epoch()
            .set_version(region_version + 1);
        info!(
            "{} preprocess_restore_shard: new_region: {:?}",
            tag, new_region
        );
        write_peer_state(ctx.raft_wb, peer_id, &new_region, PeerState::Normal, None);
        *self.preprocessed_region = Some(new_region);

        // Set raft term & index.
        let snap = cs.mut_restore_shard();
        snap.set_data_sequence(entry.index);
        let props = snap.mut_properties();
        props.mut_keys().push(TERM_KEY.to_string());
        props.mut_values().push(entry.term.to_le_bytes().to_vec());

        // Peer's encryption_key should be updated when branching with encryption
        // enabled.
        if let Some(en) = ctx.kv {
            let master_key = en.get_master_key();
            let encryption_key = get_shard_property(ENCRYPTION_KEY, snap.get_properties())
                .map(|v| master_key.decrypt_encryption_key(&v).unwrap());
            *self.encryption_key = encryption_key;
        }

        // Set pending truncate raft log to trigger TiFlash acquiring latest snapshot.
        let old = self.pending_truncate.replace((entry.term, entry.index));
        info!(
            "{} set pending truncate raft log for restore_shard, term {} index {} old {:?}",
            tag, entry.term, entry.index, old
        );
        // We use the last_index to set learner_skip_idx, but the last_index may not be
        // committed and later truncated, so we need to check and update it here.
        if *self.learner_skip_idx > entry.index - 1 {
            *self.learner_skip_idx = entry.index - 1;
        }
    }

    pub(crate) fn preprocess_pending_splits(
        &mut self,
        ctx: &mut PreprocessContext<'_>,
        entry: &Entry,
        req: &RaftCmdRequest,
    ) -> Result<()> {
        let tag = self.tag();
        if let Err(err) = check_region_epoch(req, self.get_preprocessed_region(), false) {
            warn!("{} preprocess pending split failed {:?}", tag, err);
            return Err(err);
        }
        let shard_meta = self.shard_meta();
        if shard_meta.seq >= entry.index {
            // Duplicated split should happen only in restore snapshot. In this scene, the
            // split must be denied by txn file locks. So skip preprocess & apply.
            info!(
                "{} preprocess_pending_splits: skip duplicated req", tag;
                "meta_seq" => shard_meta.seq,
                "entry.index" => entry.index,
            );
            return Ok(());
        }
        if shard_meta.has_txn_file_locks() {
            warn!("{} preprocess_pending_splits denied, shard has txn file locks", tag;
                "txn_file_locks" => %self.shard_meta().txn_file_locks(),
            );
            let mut first_split_key = req
                .get_admin_request()
                .get_splits()
                .get_requests()
                .first()
                .map(|r| r.get_split_key())
                .unwrap_or_default();
            if first_split_key.is_empty() {
                return Err(box_err!("missing split key"));
            }
            let raw_split_key = decode_bytes(&mut first_split_key, false).map_err(|err| {
                Error::Other(box_err!(
                    "failed to decode split key: {:?}, req {:?}",
                    err,
                    req
                ))
            })?;
            let key_errs = shard_meta
                .txn_file_locks()
                .get_key_errors(&raw_split_key)
                .map_err(|err| {
                    error!("{} preprocess_pending_splits: get txn file key errors failed", tag;
                        "txn_file_locks" => ?shard_meta.txn_file_locks(),
                        "err" => ?err);
                    Error::Other(box_err!("get txn file key errors failed"))
                })?;
            return Err(Error::KeyErrors(key_errs));
        }

        let regions = split_gen_new_region_metas(
            self.store_id(),
            self.get_preprocessed_region(),
            req.get_admin_request().get_splits(),
        )
        .unwrap();
        *self.last_committed_split_idx = entry.index;
        let region_id = self.region_id();
        let shard_meta = self.mut_shard_meta();
        let properties_helper = PropertiesHelper::new_from_shard_meta(shard_meta);
        let split = build_split_pb(
            region_id,
            &regions,
            entry.term,
            req.get_header(),
            shard_meta.get_property(ENCRYPTION_KEY),
            &properties_helper,
        );
        info!("{} preprocess_pending_splits, split: {:?}", tag, split);
        let new_metas = shard_meta.apply_split(&split, entry.index, RAFT_INIT_LOG_INDEX);
        let mut cs = shard_meta.to_change_set();
        cs.set_split(split);
        cs.set_sequence(entry.index);
        ctx.apply_msgs.msgs.push(ApplyMsg::PendingSplit(cs));
        for (new_meta, new_region) in new_metas.iter().zip(regions.iter()) {
            let new_peer_id = get_peer_id_by_store_id(new_region, self.peer.store_id).unwrap();
            if new_meta.id == self.region_id() {
                self.update_meta_on_version_change(ctx, new_meta, new_region, None);
            } else {
                // The peer has been created or destroyed.
                if load_last_peer_state(ctx.raft, new_peer_id).is_some()
                    || ctx.destroying.contains(&new_region.id)
                    // The new region may restore snapshot in the same batch, so we should check
                    // raft_wb too.
                    || ctx
                        .raft_wb
                        .get_state(new_peer_id, KV_ENGINE_META_KEY)
                        .is_some()
                {
                    info!(
                        "{} avoid write region meta for region {:?}",
                        self.tag(),
                        new_region,
                    );
                    continue;
                }
                write_peer_state(
                    ctx.raft_wb,
                    new_peer_id,
                    new_region,
                    PeerState::Normal,
                    None,
                );
                write_engine_meta(ctx.raft_wb, new_peer_id, new_meta);
                let region_version = new_region.get_region_epoch().get_version();
                let keyspace_id = ApiV2::get_u32_keyspace_id_by_key(new_region.get_start_key())
                    .unwrap_or_default();
                write_initial_raft_state(
                    ctx.raft_wb,
                    new_peer_id,
                    new_region.get_id(),
                    region_version,
                    keyspace_id,
                );
            }
            ctx.raft
                .add_dependent(self.region_id(), new_region.get_id());
        }
        Ok(())
    }

    pub(crate) fn update_meta_on_version_change(
        &mut self,
        ctx: &mut PreprocessContext<'_>,
        new_meta: &ShardMeta,
        new_region: &Region,
        merge_state: Option<MergeState>,
    ) {
        let peer_state = if merge_state.is_some() {
            PeerState::Merging
        } else {
            PeerState::Normal
        };
        write_peer_state(
            ctx.raft_wb,
            self.peer_id(),
            new_region,
            peer_state,
            merge_state,
        );
        // NOTE: We write the whole meta here in case of corner case.
        write_engine_meta(ctx.raft_wb, self.peer_id(), new_meta);
        // The raft state key changed when region version change, we need to set it
        // here. We handle committed entries before update peer storage's raft
        // state, so the peer storage's raft state may not be update to date, we
        // set the hard state of raft. This is the final raft state of the old
        // version.
        self.raft_state.set_hard_state(&self.raft_hard_state);
        self.raft_state.last_preprocessed_index = new_meta.seq;
        self.write_raft_state(ctx);
        *self.shard_meta = Some(new_meta.clone());
        // PeerStore use the shard meta's version to persist raft state, since the
        // shard_meta has updated, we also need to set the new version's raft state.
        self.write_raft_state(ctx);
        *self.preprocessed_region = Some(new_region.clone());
        // Update peer's encryption_key when split a whole keyspace with encryption
        // enabled.
        if let Some(en) = ctx.kv {
            let master_key = en.get_master_key();
            let encryption_key = new_meta
                .get_property(ENCRYPTION_KEY)
                .map(|v| master_key.decrypt_encryption_key(&v).unwrap());
            *self.encryption_key = encryption_key;
        }
    }

    pub(crate) fn preprocess_prepare_merge(
        &mut self,
        ctx: &mut PreprocessContext<'_>,
        entry: &Entry,
        req: &RaftCmdRequest,
    ) {
        let tag = self.tag();
        let shard_meta = self.shard_meta();
        if shard_meta.seq >= entry.index {
            // Duplicated prepare merge should happen only in restore snapshot. In this
            // scene, the prepare merge must be denied by txn file locks. So skip preprocess
            // & apply.
            info!(
                "{} preprocess_prepare_merge: skip duplicated req", tag;
                "meta_seq" => shard_meta.seq,
                "entry.index" => entry.index,
            );
            return;
        }
        if shard_meta.has_txn_file_locks() {
            // Error response is returned by `exec_admin_cmd`, so do not return error here.
            warn!("{} preprocess_prepare_merge denied, shard has txn file locks", tag;
                "txn_file_locks" => ?self.shard_meta().txn_file_locks());
            return;
        }
        if !shard_meta.unconverted_l0s.is_empty() {
            // Error response is returned by `exec_admin_cmd`, so do not return error here.
            warn!(
                "{} preprocess_prepare_merge denied, unconverted l0s : {:?}",
                tag, shard_meta.unconverted_l0s
            );
            // Send the prepare merge message to applier to make it wait for flush.
            // Then unconverted l0 is added and the kvengine can correctly deny the prepare
            // merge.
            ctx.apply_msgs.msgs.push(ApplyMsg::PrepareMerge);
            return;
        }

        let prepare_merge = req.get_admin_request().get_prepare_merge();
        let mut region = self.get_preprocessed_region().clone();
        let region_version = region.get_region_epoch().get_version() + 1;
        region.mut_region_epoch().set_version(region_version);
        // In theory conf version should not be increased when executing prepare_merge.
        // However, we don't want to do conf change after prepare_merge is committed.
        // This can also be done by iterating all proposal to find if prepare_merge is
        // proposed before proposing conf change, but it make things complicated.
        // Another way is make conf change also check region version, but this is not
        // backward compatible.
        // Change conf version only when there are multiple peers, to meet the
        // requirement of merged engine which has no conf change.
        if region.get_peers().len() > 1 {
            let conf_version = region.get_region_epoch().get_conf_ver() + 1;
            region.mut_region_epoch().set_conf_ver(conf_version);
        }
        let mut merge_state = MergeState::default();
        merge_state.set_target(prepare_merge.get_target().to_owned());
        merge_state.set_commit(entry.index);
        merge_state.set_min_index(prepare_merge.get_min_index());
        let parent_meta = self.shard_meta().clone();
        let mut new_meta = parent_meta;
        new_meta.prepare_merge(entry.index);
        new_meta.set_property(TERM_KEY, &entry.term.to_le_bytes());
        self.update_meta_on_version_change(ctx, &new_meta, &region, Some(merge_state.clone()));
        *self.pending_merge_state = Some(merge_state);
        ctx.apply_msgs.msgs.push(ApplyMsg::PrepareMerge);
    }

    pub(crate) fn preprocess_rollback_merge(
        &mut self,
        ctx: &mut PreprocessContext<'_>,
        entry: &Entry,
        req: &RaftCmdRequest,
    ) {
        let commit = req.get_admin_request().get_rollback_merge().get_commit();
        if self.pending_merge_state.is_none() {
            panic!("{} pending merge state is none", self.tag());
        }
        let pending_commit = self.pending_merge_state.as_ref().unwrap().get_commit();
        if commit != 0 && pending_commit != commit {
            panic!(
                "{} rollbacks a wrong merge: {} != {}",
                self.tag(),
                pending_commit,
                commit
            );
        }
        self.clear_merge_in_mem_data();

        let mut region = self.get_preprocessed_region().clone();
        let version = region.get_region_epoch().get_version();
        // Update version to avoid duplicated rollback requests.
        region.mut_region_epoch().set_version(version + 1);
        let mut new_meta = self.shard_meta().clone();

        // Get the applied sequence of last initial flush, which was applied to meta
        // before check merge.
        // The sequence is used to pause apply queue before the initial flush is applied
        // to kvengine.
        let initial_flush_seq = new_meta.seq;

        new_meta.rollback_merge(entry.index);
        self.update_meta_on_version_change(ctx, &new_meta, &region, None);
        ctx.apply_msgs
            .msgs
            .push(ApplyMsg::PrepareRollbackMerge(initial_flush_seq));
    }

    pub(crate) fn preprocess_commit_merge(
        &mut self,
        ctx: &mut PreprocessContext<'_>,
        entry: &Entry,
        req: &RaftCmdRequest,
    ) {
        let commit_merge = req.get_admin_request().get_commit_merge();
        let source_region = commit_merge.get_source();
        let source_peer_id = get_peer_id_by_store_id(source_region, self.peer.store_id).unwrap();
        let mut new_meta = self.shard_meta().clone();
        let mut source = kvenginepb::ChangeSet::new();
        let source_meta_bytes = commit_merge.get_source_meta();
        source.merge_from_bytes(source_meta_bytes).unwrap();
        let source_meta = ShardMeta::new(self.peer.store_id, &source);
        new_meta.commit_merge(&source_meta, entry.index);
        new_meta.set_property(TERM_KEY, &entry.term.to_le_bytes());
        let merged_region = new_merged_region(source_region, self.get_preprocessed_region());
        self.update_meta_on_version_change(ctx, &new_meta, &merged_region, None);

        // set target to tombstone in the same batch, so the commit merge and destroy
        // source would be atomic.
        let mut merge_state = MergeState::new();
        merge_state.set_commit(commit_merge.commit);
        merge_state.set_target(self.region.clone());
        write_peer_state(
            ctx.raft_wb,
            source_peer_id,
            source_region,
            PeerState::Tombstone,
            Some(merge_state),
        );

        // Get storage class after `new_meta.commit_merge`. As storage class would be
        // changed in there.
        let source_sc_spec = source_meta.get_storage_class_spec();
        let target_sc_spec = new_meta.get_storage_class_spec();
        if source_sc_spec != target_sc_spec {
            set_shard_property(
                STORAGE_CLASS_KEY,
                source.mut_snapshot().mut_properties(),
                target_sc_spec.marshal(),
            );
            warn!("{} preprocess commit merge: storage class mismatch", self.tag();
                "source_spec" => ?source_sc_spec, "target_spec" => ?target_sc_spec, "source" => ?source);
        }

        let apply_msg = ApplyMsg::PrepareCommitMerge {
            source,
            commit_index: entry.index,
        };
        ctx.apply_msgs.msgs.push(apply_msg);
    }

    pub(crate) fn preprocess_conf_change(
        &mut self,
        ctx: &mut PreprocessContext<'_>,
        entry: &Entry,
    ) {
        let (cmd, _) = parse_conf_change_cmd(entry, self.tag());
        if let Err(err) = check_region_epoch(&cmd, self.get_preprocessed_region(), false) {
            warn!("preprocess pending conf change failed {:?}", err);
            return;
        }
        let changes = match cmd.get_admin_request().cmd_type {
            AdminCmdType::ChangePeer => {
                vec![cmd.get_admin_request().get_change_peer().clone()]
            }
            AdminCmdType::ChangePeerV2 => cmd
                .get_admin_request()
                .get_change_peer_v2()
                .get_changes()
                .to_vec(),
            _ => unreachable!(),
        };
        if let Ok(region) = region_apply_conf_change(
            self.get_preprocessed_region(),
            &changes,
            self.peer_id(),
            self.tag(),
        ) {
            if region_has_peer(&region, self.peer_id()) {
                write_peer_state(
                    ctx.raft_wb,
                    self.peer_id(),
                    &region,
                    PeerState::Normal,
                    None,
                );
            } else {
                // It's a remove self conf change, it will be updated in destroy
                // method with tombstone state.
            }
            *self.preprocessed_region = Some(region);
        }
    }

    pub(crate) fn preprocess_txn_file_ref(
        &mut self,
        ctx: &mut PreprocessContext<'_>,
        entry: &Entry,
        custom_req: &CustomRequest,
    ) -> Result<()> {
        let custom_data = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        let txn_file_ref = custom_data.get_txn_file_ref()?;

        let shard_meta = self.shard_meta();
        if shard_meta.ver != txn_file_ref.shard_ver {
            warn!(
                "{} preprocess txn file ref: shard meta version not match", self.tag();
                "shard_meta.ver" => shard_meta.ver,
                "req.shard_ver" => txn_file_ref.shard_ver,
                "log_index" => entry.index,
            );
            return Err(Error::EpochNotMatch(
                "preprocess txn file ref: version not match".to_owned(),
                vec![self.region.clone()],
            ));
        }

        self.mut_shard_meta()
            .merge_txn_file_ref(&txn_file_ref, entry.index);

        if ctx.kv.is_none() {
            // kv is none in restore, we don't need to load txn file.
            return Ok(());
        }
        let kv = ctx.kv.unwrap();
        let chunk_manager = kv.get_txn_chunk_manager();
        if chunk_manager.all_chunks_exists(txn_file_ref.get_chunk_ids()) {
            return Ok(());
        }
        ctx.apply_msgs.msgs.push(ApplyMsg::PrepareTxnFile {
            txn_file_ref,
            commit_index: entry.index,
            encryption_key: self.encryption_key.clone(),
        });
        Ok(())
    }
}

impl Peer {
    fn truncate_pending_raft_log(&mut self, ctx: &mut RaftContext, applied_index: u64) {
        if self.pending_truncate.is_none() {
            return;
        }

        let (term, idx) = self.pending_truncate.unwrap();
        if ctx.global.engines.raft.has_dependents(self.region_id) {
            info!(
                "{} has dependents, skip truncate pending raft log, term {} index {}",
                self.tag(),
                term,
                idx
            );
            return;
        }

        if self.get_store().truncated_index() < idx && idx <= applied_index {
            self.mut_store()
                .truncate_raft_log(&mut ctx.raft_wb, idx, term);
            self.pending_truncate = None;
            info!(
                "{} truncate pending raft log, term {} index {}",
                self.tag(),
                term,
                idx
            );
        }
    }

    pub(crate) fn get_preprocessed_region(&self) -> &Region {
        self.get_store().get_preprocessed_region()
    }

    pub(crate) fn post_apply(&mut self, ctx: &mut RaftContext, apply_result: &MsgApplyResult) {
        if self.get_store().is_applying_snapshot() {
            info!(
                "{} is applying snapshot, ignore outdated apply result.",
                self.tag()
            );
            return;
        }
        let apply_state = apply_result.apply_state;
        let applied_index = apply_state.applied_index;
        let applied_index_term = apply_state.applied_index_term;
        self.raft_group.advance_apply_to(applied_index);

        self.truncate_pending_raft_log(ctx, applied_index);

        self.cmd_epoch_checker.advance_apply(
            applied_index,
            self.term(),
            self.raft_group.store().region(),
        );

        let progress_to_be_updated = self.mut_store().applied_index_term() != applied_index_term;
        self.mut_store().set_applied_state(apply_state);
        self.peer_stat.written_keys += apply_result.metrics.written_keys;
        self.peer_stat.written_bytes += apply_result.metrics.written_bytes;
        if let (Some(delta), Some(buckets)) =
            (apply_result.bucket_stat.as_ref(), self.buckets.as_mut())
        {
            simple_merge_bucket_write_stats(buckets, delta);
        }
        if !self.is_leader() {
            // TODO(x) post_pending_read_index_on_replica
        } else if self.ready_to_handle_read() {
            while let Some(mut read) = self.pending_reads.pop_front() {
                self.response_read(&mut read, ctx, false);
            }
        }
        self.pending_reads.gc();

        // Only leaders need to update applied_index_term.
        if progress_to_be_updated && self.is_leader() {
            if applied_index_term == self.term() {
                ctx.global
                    .coprocessor_host
                    .on_applied_current_term(StateRole::Leader, self.region());
            }
            if !self.pending_remove {
                let readers = ctx.global.readers.pin();
                readers.update(self.region_id, move |old| {
                    let mut new_reader = old.clone_for_update();
                    let progress = ReadProgress::applied_index_term(applied_index_term);
                    new_reader.update(progress);
                    Arc::new(new_reader)
                });
            }
        }
    }

    fn response_read(
        &self,
        read: &mut ReadIndexRequest,
        ctx: &mut RaftContext,
        replica_read: bool,
    ) {
        debug!(
            "handle reads with a read index";
            "request_id" => ?read.id,
            "tag" => self.tag(),
            "peer_id" => self.peer.get_id(),
        );
        RAFT_READ_INDEX_PENDING_COUNT.sub(read.cmds().len() as i64);
        for (req, cb, mut read_index) in read.take_cmds().drain(..) {
            // leader reports key is locked
            if let Some(locked) = read.locked.take() {
                let mut response = raft_cmdpb::Response::default();
                response.mut_read_index().set_locked(*locked);
                let mut cmd_resp = RaftCmdResponse::default();
                cmd_resp.mut_responses().push(response);
                cb.invoke_read(ReadResponse {
                    response: cmd_resp,
                    snapshot: None,
                    txn_extra_op: TxnExtraOp::Noop,
                });
                continue;
            }
            if !replica_read {
                match (read_index, read.read_index) {
                    (Some(local_responsed_index), Some(batch_index)) => {
                        // `read_index` could be less than `read.read_index` because the former is
                        // filled with `committed index` when proposed, and the latter is filled
                        // after a read-index procedure finished.
                        read_index = Some(cmp::max(local_responsed_index, batch_index));
                    }
                    (None, _) => {
                        // Actually, the read_index is none if and only if it's the first one in
                        // read.cmds. Starting from the second, all the following ones' read_index
                        // is not none.
                        read_index = read.read_index;
                    }
                    _ => {}
                }
                cb.invoke_read(self.handle_read(ctx, req, true, read_index));
                continue;
            }
            if req.get_header().get_replica_read() {
                // We should check epoch since the range could be changed.
                cb.invoke_read(self.handle_read(ctx, req, true, read.read_index));
            } else {
                // The request could be proposed when the peer was leader.
                // TODO: figure out that it's necessary to notify stale or not.
                let term = self.term();
                notify_stale_req(term, cb, "replica read index on leader");
            }
        }
    }

    /// Responses to the ready read index request on the replica, the replica is
    /// not a leader.
    fn post_pending_read_index_on_replica(&mut self, ctx: &mut RaftContext) {
        while let Some(mut read) = self.pending_reads.pop_front() {
            // The response of this read index request is lost, but we need it for
            // the memory lock checking result. Resend the request.
            if let Some(read_index) = read.addition_request.take() {
                assert_eq!(read.cmds().len(), 1);
                let (mut req, cb, _) = read.take_cmds().pop().unwrap();
                assert_eq!(req.requests.len(), 1);
                req.requests[0].set_read_index(*read_index);
                let read_cmd = RaftCommand::new(req, cb);
                info!(
                    "re-propose read index request because the response is lost";
                    "tag" => self.tag(),
                    "peer_id" => self.peer.get_id(),
                );
                RAFT_READ_INDEX_PENDING_COUNT.sub(1);
                self.send_read_command(ctx, read_cmd);
                continue;
            }

            assert!(read.read_index.is_some());
            let is_read_index_request = read.cmds().len() == 1
                && read.cmds()[0].0.get_requests().len() == 1
                && read.cmds()[0].0.get_requests()[0].get_cmd_type() == CmdType::ReadIndex;

            if is_read_index_request {
                self.response_read(&mut read, ctx, false);
            } else if self.ready_to_handle_unsafe_replica_read(read.read_index.unwrap()) {
                self.response_read(&mut read, ctx, true);
            } else {
                // TODO: `ReadIndex` requests could be blocked.
                self.pending_reads.push_front(read);
                break;
            }
        }
    }

    fn send_read_command(&self, ctx: &mut RaftContext, read_cmd: RaftCommand) {
        ctx.global
            .router
            .send(self.region_id, PeerMsg::RaftCommand(read_cmd));
    }

    fn apply_reads(&mut self, ctx: &mut RaftContext, ready: &Ready) {
        let read_start = tikv_util::time::Instant::now_coarse();
        let mut propose_time = None;
        let states = ready.read_states().iter().map(|state| {
            let read_index_ctx = ReadIndexContext::parse(state.request_ctx.as_slice()).unwrap();
            (read_index_ctx.id, read_index_ctx.locked, state.index)
        });
        // The follower may lost `ReadIndexResp`, so the pending_reads does not
        // guarantee the orders are consistent with read_states. `advance` will
        // update the `read_index` of read request that before this successful
        // `ready`.
        if !self.is_leader() {
            // NOTE: there could still be some pending reads proposed by the peer when it
            // was leader. They will be cleared in
            // `clear_uncommitted_on_role_change` later in the function.
            self.pending_reads.advance_replica_reads(states);
            self.post_pending_read_index_on_replica(ctx);
        } else {
            self.pending_reads.advance_leader_reads(self.tag(), states);
            propose_time = self.pending_reads.last_ready().map(|r| r.propose_time);
            if self.ready_to_handle_read() {
                while let Some(mut read) = self.pending_reads.pop_front() {
                    self.response_read(&mut read, ctx, false);
                }
            }
        }

        // Note that only after handle read_states can we identify what requests are
        // actually stale.
        if ready.ss().is_some() {
            let term = self.term();
            // all uncommitted reads will be dropped silently in raft.
            self.pending_reads.clear_uncommitted_on_role_change(term);
        }
        if let Some(propose_time) = propose_time {
            // `propose_time` is a placeholder, here cares about `Suspect` only,
            // and if it is in `Suspect` phase, the actual timestamp is useless.
            if self.leader_lease.inspect(Some(propose_time)) == LeaseState::Suspect {
                return;
            }
            self.maybe_renew_leader_lease(propose_time, ctx, None);
        }
        let read_duration = read_start.saturating_elapsed();
        if read_duration > SLOW_LOG_DURATION {
            warn!(
                "{} apply_reads takes too long {:?}",
                self.tag(),
                read_duration
            );
        }
    }

    #[inline]
    fn ready_to_handle_read(&self) -> bool {
        // TODO: It may cause read index to wait a long time.

        // There may be some values that are not applied by this leader yet but the old
        // leader, if applied_index_term isn't equal to current term.
        self.get_store().applied_index_term() == self.term()
            // There may be stale read if the old leader splits really slow,
            // the new region may already elected a new leader while
            // the old leader still think it owns the split range.
            && !self.is_splitting()
            && !self.is_merging()
    }

    fn ready_to_handle_unsafe_replica_read(&self, read_index: u64) -> bool {
        // Wait until the follower applies all values before the read. There is still a
        // problem if the leader applies fewer values than the follower, the follower
        // read could get a newer value, and after that, the leader may read a
        // stale value, which violates linearizability.
        self.get_store().applied_index() >= read_index
            // If it is in pending merge state(i.e. applied PrepareMerge), the data may be stale.
            // TODO: Add a test to cover this case
            // TODO(x) && self.pending_merge_state.is_none()
            // a peer which is applying snapshot will clean up its data and ingest a snapshot file,
            // during between the two operations a replica read could read empty data.
            && !self.is_applying_snapshot()
    }

    #[inline]
    fn is_splitting(&self) -> bool {
        self.last_committed_split_idx > self.get_store().applied_index()
    }

    #[inline]
    fn is_merging(&self) -> bool {
        self.pending_merge_state.is_some()
    }

    /// Try to renew leader lease.
    fn maybe_renew_leader_lease(
        &mut self,
        ts: Timespec,
        ctx: &mut RaftContext,
        progress: Option<ReadProgress>,
    ) {
        // A nonleader peer should never has leader lease.
        let read_progress = if !self.is_leader() {
            None
        } else if self.is_splitting() {
            // A splitting leader should not renew its lease.
            // Because we split regions asynchronous, the leader may read stale results
            // if splitting runs slow on the leader.
            debug!(
                "prevents renew lease while splitting";
                "tag" => self.tag(),
                "peer_id" => self.peer.get_id(),
            );
            None
        } else if self.is_merging() {
            // A merging leader should not renew its lease.
            // Because we merge regions asynchronous, the leader may read stale results
            // if commit merge runs slow on sibling peers.
            debug!(
                "prevents renew lease while merging";
                "tag" => self.tag(),
                "peer_id" => self.peer.get_id(),
            );
            None
        } else {
            self.leader_lease.renew(ts);
            let term = self.term();
            self.leader_lease
                .maybe_new_remote_lease(term)
                .map(ReadProgress::leader_lease)
        };
        if (progress.is_some() || read_progress.is_some()) && !self.pending_remove {
            let readers = ctx.global.readers.pin();
            let mut new_reader = readers
                .get(&self.region_id)
                .cloned()
                .unwrap()
                .clone_for_update();
            if let Some(progress) = progress {
                new_reader.update(progress);
            }
            if let Some(read_progress) = read_progress {
                new_reader.update(read_progress);
            }
            let arc_new_reader = Arc::new(new_reader);
            readers.update(self.region_id, |_| arc_new_reader.clone());
        }
    }

    pub(crate) fn update_store_meta_for_snap(
        &mut self,
        restore_snap_result: RestoreSnapResult,
        meta: &mut StoreMeta,
    ) -> bool {
        let region = restore_snap_result.region;
        info!(
            "snapshot is restored";
            "tag" => self.tag(),
            "peer_id" => self.peer_id(),
            "region" => ?region,
        );
        // TODO(x) update commit group
        meta.set_region(region, self, RegionChangeReason::RestoreSnapshot);
        true
    }

    /// Propose a request.
    ///
    /// Return true means the request has been proposed successfully.
    pub fn propose(
        &mut self,
        ctx: &mut RaftContext,
        mut cb: Callback,
        req: RaftCmdRequest,
        mut err_resp: RaftCmdResponse,
    ) -> bool {
        if self.pending_remove {
            return false;
        }

        ctx.raft_metrics.propose.all.inc();

        let req_admin_cmd_type = if !req.has_admin_request() {
            None
        } else {
            Some(req.get_admin_request().get_cmd_type())
        };
        let is_urgent = is_request_urgent(&req);

        let policy = self.inspect(&req);
        let res = match policy {
            RequestPolicy::ReadLocal | RequestPolicy::StaleRead => {
                self.read_local(ctx, req, cb);
                return false;
            }
            RequestPolicy::ReadIndex => {
                return self.read_index(ctx, req, err_resp, cb);
            }
            RequestPolicy::ProposeNormal => {
                // TODO: check disk usage.
                self.propose_normal(ctx, req)
            }
            RequestPolicy::ProposeTransferLeader => {
                return self.propose_transfer_leader(ctx, req, cb);
            }
            RequestPolicy::ProposeConfChange => self.propose_conf_change(ctx, &req),
        };

        match res {
            Err(e) => {
                cmd_resp::bind_error(&mut err_resp, e);
                cb.invoke_with_response(err_resp);
                false
            }
            Ok(Either::Right(idx)) => {
                if !cb.is_none() {
                    self.cmd_epoch_checker.attach_to_conflict_cmd(idx, cb);
                }
                false
            }
            Ok(Either::Left(idx)) => {
                let has_applied_to_current_term = self.has_applied_to_current_term();
                if has_applied_to_current_term {
                    // After this peer has applied to current term and passed above checking
                    // including `cmd_epoch_checker`, we can safely guarantee
                    // that this proposal will be committed if there is no abnormal leader transfer
                    // in the near future. Thus proposed callback can be called.
                    cb.invoke_proposed();
                }
                if is_urgent {
                    self.last_urgent_proposal_idx = idx;
                    // Eager flush to make urgent proposal be applied on all nodes as soon as
                    // possible.
                    self.raft_group.skip_bcast_commit(false);
                }
                let p = Proposal {
                    is_conf_change: req_admin_cmd_type == Some(AdminCmdType::ChangePeer)
                        || req_admin_cmd_type == Some(AdminCmdType::ChangePeerV2),
                    index: idx,
                    term: self.term(),
                    cb,
                    propose_time: None,
                    must_pass_epoch_check: has_applied_to_current_term,
                };
                if let Some(cmd_type) = req_admin_cmd_type {
                    self.cmd_epoch_checker
                        .post_propose(cmd_type, idx, self.term());
                }
                self.push_proposal(ctx, p);
                true
            }
        }
    }

    fn push_proposal(&mut self, ctx: &mut RaftContext, mut p: Proposal) {
        // Try to renew leader lease on every consistent read/write request.
        if ctx.current_time.is_none() {
            ctx.current_time = Some(monotonic_raw_now());
        }
        p.propose_time = ctx.current_time;

        self.proposals.push(p);
    }

    // TODO: set higher election priority of voter/incoming voter than demoting
    // voter
    /// Validate the `ConfChange` requests and check whether it's safe to
    /// propose these conf change requests.
    /// It's safe iff at least the quorum of the Raft group is still healthy
    /// right after all conf change is applied.
    /// If 'allow_remove_leader' is false then the peer to be removed should
    /// not be the leader.
    fn check_conf_change(
        &mut self,
        ctx: &mut RaftContext,
        change_peers: &[ChangePeerRequest],
        cc: &impl ConfChangeI,
    ) -> Result<()> {
        // Check whether current joint state can handle this request
        let mut after_progress = self.check_joint_state(cc)?;
        let current_progress = self.raft_group.status().progress.unwrap().clone();
        let kind = ConfChangeKind::confchange_kind(change_peers.len());

        if kind == ConfChangeKind::LeaveJoint {
            if self.peer.get_role() == PeerRole::DemotingVoter {
                return Err(box_err!(
                    "{} ignore leave joint command that demoting leader",
                    self.tag()
                ));
            }
            // Leaving joint state, skip check
            return Ok(());
        }

        // Check whether this request is valid
        let mut check_dup = HashSet::default();
        let mut only_learner_change = true;
        let current_voter = current_progress.conf().voters().ids();
        for cp in change_peers.iter() {
            let (change_type, peer) = (cp.get_change_type(), cp.get_peer());
            match (change_type, peer.get_role()) {
                (ConfChangeType::RemoveNode, PeerRole::Voter) if kind != ConfChangeKind::Simple => {
                    return Err(box_err!(
                        "{} invalid conf change request: {:?}, can not remove voter directly",
                        self.tag(),
                        cp
                    ));
                }
                (ConfChangeType::RemoveNode, _)
                | (ConfChangeType::AddNode, PeerRole::Voter)
                | (ConfChangeType::AddLearnerNode, PeerRole::Learner) => {}
                _ => {
                    return Err(box_err!(
                        "{} invalid conf change request: {:?}",
                        self.tag(),
                        cp
                    ));
                }
            }

            if !check_dup.insert(peer.get_id()) {
                return Err(box_err!(
                    "{} invalid conf change request, have multiple commands for the same peer {}",
                    self.tag(),
                    peer.get_id()
                ));
            }

            if peer.get_id() == self.peer_id()
                && (change_type == ConfChangeType::RemoveNode
                // In Joint confchange, the leader is allowed to be DemotingVoter
                || (kind == ConfChangeKind::Simple
                && change_type == ConfChangeType::AddLearnerNode))
                && !ctx.cfg.allow_remove_leader
            {
                return Err(box_err!(
                    "{} ignore remove leader or demote leader",
                    self.tag()
                ));
            }

            if current_voter.contains(peer.get_id()) || change_type == ConfChangeType::AddNode {
                only_learner_change = false;
            }
        }

        // Multiple changes that only effect learner will not product `IncommingVoter`
        // or `DemotingVoter` after apply, but raftstore layer and PD rely on
        // these roles to detect joint state
        if kind != ConfChangeKind::Simple && only_learner_change {
            return Err(box_err!(
                "{} invalid conf change request, multiple changes that only effect learner",
                self.tag()
            ));
        }

        let promoted_commit_index = after_progress.maximal_committed_index().0;
        if current_progress.is_singleton() // It's always safe if there is only one node in the cluster.
            || promoted_commit_index >= self.get_store().truncated_index()
        {
            return Ok(());
        }

        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["conf_change", "reject_unsafe"])
            .inc();

        Err(box_err!(
            "{} unsafe to perform conf change {:?}, before: {:?}, after: {:?}, truncated index {}, promoted commit index {}",
            self.tag(),
            change_peers,
            current_progress.conf().to_conf_state(),
            after_progress.conf().to_conf_state(),
            self.get_store().truncated_index(),
            promoted_commit_index
        ))
    }

    /// Check if current joint state can handle this confchange
    fn check_joint_state(&mut self, cc: &impl ConfChangeI) -> Result<ProgressTracker> {
        let cc = &cc.as_v2();
        let mut prs = self.raft_group.status().progress.unwrap().clone();
        let mut changer = Changer::new(&prs);
        let (cfg, changes) = if cc.leave_joint() {
            changer.leave_joint()?
        } else if let Some(auto_leave) = cc.enter_joint() {
            changer.enter_joint(auto_leave, &cc.changes)?
        } else {
            changer.simple(&cc.changes)?
        };
        prs.apply_conf(cfg, changes, self.raft_group.raft.raft_log.last_index());
        Ok(prs)
    }

    pub(crate) fn transfer_leader(&mut self, peer: &metapb::Peer) {
        info!(
            "transfer leader";
            "tag" => self.tag(),
            "peer_id" => self.peer.get_id(),
            "peer" => ?peer,
        );

        self.raft_group.transfer_leader(peer.get_id());
    }

    fn pre_transfer_leader(&mut self, peer: &metapb::Peer) -> bool {
        // Checks if safe to transfer leader.
        if self.raft_group.raft.has_pending_conf() {
            let pending_conf_index = self.raft_group.raft.pending_conf_index;
            let applied_index = self.get_store().applied_index();
            info!(
                "reject transfer leader due to pending conf change";
                "tag" => self.tag(),
                "peer_id" => self.peer.get_id(),
                "peer" => ?peer,
                "pending_conf_index" => pending_conf_index,
                "applied_index" => applied_index,
            );
            return false;
        }

        // Broadcast heartbeat to make sure followers commit the entries immediately.
        // It's only necessary to ping the target peer, but ping all for simplicity.
        self.raft_group.ping();
        let mut msg = eraftpb::Message::new();
        msg.set_to(peer.get_id());
        msg.set_msg_type(eraftpb::MessageType::MsgTransferLeader);
        msg.set_from(self.peer_id());
        // log term here represents the term of last log. For leader, the term of last
        // log is always its current term. Not just set term because raft library
        // forbids setting it for MsgTransferLeader messages.
        msg.set_log_term(self.term());
        self.raft_group.raft.msgs.push(msg);
        true
    }

    pub(crate) fn ready_to_transfer_leader(
        &self,
        ctx: &mut RaftContext,
        mut index: u64,
        peer: &metapb::Peer,
    ) -> Option<&'static str> {
        let peer_id = peer.get_id();
        let status = self.raft_group.status();
        let progress = status.progress.unwrap();

        if !progress.conf().voters().contains(peer_id) {
            return Some("non voter");
        }

        for (id, pr) in progress.iter() {
            if pr.state == ProgressState::Snapshot {
                return Some("pending snapshot");
            }
            if *id == peer_id && index == 0 {
                // index will be zero if it's sent from an instance without
                // pre-transfer-leader feature. Set it to matched to make it
                // possible to transfer leader to an older version. It may be
                // useful during rolling restart.
                index = pr.matched;
            }
        }

        if self.raft_group.raft.has_pending_conf()
            || self.raft_group.raft.pending_conf_index > index
        {
            return Some("pending conf change");
        }

        let last_index = self.get_store().last_index();
        if last_index >= index + ctx.cfg.leader_transfer_max_log_lag {
            return Some("log gap");
        }
        None
    }

    fn read_local(&mut self, ctx: &mut RaftContext, req: RaftCmdRequest, cb: Callback) {
        debug!("read local");
        cb.invoke_read(self.handle_read(ctx, req, false, Some(self.get_store().commit_index())))
    }

    fn pre_read_index(&self) -> Result<()> {
        fail_point!(
            "before_propose_readindex",
            |s| if s.is_none_or(|s| s.parse().unwrap_or(true)) {
                Ok(())
            } else {
                Err(box_err!(
                    "{} can not read due to injected failure",
                    self.tag()
                ))
            }
        );

        // See more in ready_to_handle_read().
        if self.is_splitting() {
            return Err(Error::ReadIndexNotReady {
                reason: "can not read index due to split",
                region_id: self.region_id,
            });
        }
        if self.is_merging() {
            return Err(Error::ReadIndexNotReady {
                reason: "can not read index due to merge",
                region_id: self.region_id,
            });
        }
        Ok(())
    }

    /// `ReadIndex` requests could be lost in network, so on followers commands
    /// could queue in `pending_reads` forever. Sending a new `ReadIndex`
    /// periodically can resolve this.
    pub fn retry_pending_reads(&mut self, raft_election_timeout_ticks: usize) {
        if self.is_leader()
            || !self
                .pending_reads
                .check_needs_retry(raft_election_timeout_ticks)
            || self.pre_read_index().is_err()
        {
            return;
        }

        let read = self.pending_reads.back_mut().unwrap();
        debug_assert!(read.read_index.is_none());
        self.raft_group
            .read_index(ReadIndexContext::fields_to_bytes(
                read.id,
                read.addition_request.as_deref(),
                None,
            ));
    }

    // Returns a boolean to indicate whether the `read` is proposed or not.
    // For these cases it won't be proposed:
    // 1. The region is in merging or splitting;
    // 2. The message is stale and dropped by the Raft group internally;
    // 3. There is already a read request proposed in the current lease;
    fn read_index(
        &mut self,
        ctx: &mut RaftContext,
        mut req: RaftCmdRequest,
        mut err_resp: RaftCmdResponse,
        cb: Callback,
    ) -> bool {
        if let Err(e) = self.pre_read_index() {
            debug!(
                "prevents unsafe read index";
                "tag" => self.tag(),
                "peer_id" => self.peer.get_id(),
                "err" => ?e,
            );
            ctx.raft_metrics.propose.unsafe_read_index.inc();
            cmd_resp::bind_error(&mut err_resp, e);
            cb.invoke_with_response(err_resp);
            return false;
        }

        let now = monotonic_raw_now();
        if self.is_leader() {
            match self.inspect_lease() {
                // Here combine the new read request with the previous one even if the lease expired
                // is ok because in this case, the previous read index must be sent
                // out with a valid lease instead of a suspect lease. So there must
                // no pending transfer-leader proposals before or after the previous
                // read index, and the lease can be renewed when get
                // heartbeat responses.
                LeaseState::Valid | LeaseState::Expired => {
                    // Must use the commit index of `PeerStorage` instead of the commit index
                    // in raft-rs which may be greater than the former one.
                    // For more details, see the annotations above `on_leader_commit_idx_changed`.
                    let commit_index = self.get_store().commit_index();
                    if let Some(read) = self.pending_reads.back_mut() {
                        let max_lease = ctx.cfg.raft_store_max_leader_lease();
                        let is_read_index_request = req
                            .get_requests()
                            .first()
                            .map(|req| req.has_read_index())
                            .unwrap_or_default();
                        // A read index request or a read with addition request always needs the
                        // response of checking memory lock for async
                        // commit, so we cannot apply the optimization here
                        if !is_read_index_request
                            && read.addition_request.is_none()
                            && read.propose_time + max_lease > now
                        {
                            // A read request proposed in the current lease is found; combine the
                            // new read request to that previous one, so
                            // that no proposing needed.
                            read.push_command(req, cb, commit_index);
                            return false;
                        }
                    }
                }
                // If the current lease is suspect, new read requests can't be appended into
                // `pending_reads` because if the leader is transferred, the latest read could
                // be dirty.
                _ => {}
            }
        }

        // When a replica cannot detect any leader, `MsgReadIndex` will be dropped,
        // which would cause a long time waiting for a read response. Then we
        // should return an error directly in this situation.
        if !self.is_leader() && self.leader_id() == INVALID_ID {
            ctx.raft_metrics.invalid_proposal.read_index_no_leader.inc();
            cmd_resp::bind_error(&mut err_resp, Error::NotLeader(self.region_id, None));
            cb.invoke_with_response(err_resp);
            return false;
        }

        // Should we call pre_propose here?
        let last_pending_read_count = self.raft_group.raft.pending_read_count();
        let last_ready_read_count = self.raft_group.raft.ready_read_count();

        ctx.raft_metrics.propose.read_index.inc();

        let id = Uuid::new_v4();
        let request = req
            .mut_requests()
            .get_mut(0)
            .filter(|req| req.has_read_index())
            .map(|req| req.take_read_index());
        self.raft_group
            .read_index(ReadIndexContext::fields_to_bytes(
                id,
                request.as_ref(),
                None,
            ));

        let pending_read_count = self.raft_group.raft.pending_read_count();
        let ready_read_count = self.raft_group.raft.ready_read_count();

        if pending_read_count == last_pending_read_count
            && ready_read_count == last_ready_read_count
            && self.is_leader()
        {
            // The message gets dropped silently, can't be handled anymore.
            let reason = format!("{} not applied to current term", self.tag());
            notify_stale_req(self.term(), cb, &reason);
            return false;
        }

        let mut read = ReadIndexRequest::with_command(id, req, cb, now);
        read.addition_request = request.map(Box::new);
        self.pending_reads.push_back(read, self.is_leader());

        debug!(
            "request to get a read index";
            "request_id" => ?id,
            "tag" => self.tag(),
            "peer_id" => self.peer.get_id(),
            "is_leader" => self.is_leader(),
        );

        // TimeoutNow has been sent out, so we need to propose explicitly to
        // update leader lease.
        if self.leader_lease.inspect(Some(now)) == LeaseState::Suspect {
            let req = RaftCmdRequest::default();
            if let Ok(Either::Left(index)) = self.propose_normal(ctx, req) {
                let p = Proposal {
                    is_conf_change: false,
                    index,
                    term: self.term(),
                    cb: Callback::None,
                    propose_time: Some(now),
                    must_pass_epoch_check: false,
                };
                self.push_proposal(ctx, p);
            }
        }

        true
    }

    /// Returns (minimal matched, minimal committed_index)
    ///
    /// For now, it is only used in merge.
    pub fn get_min_progress(&self) -> Result<(u64, u64)> {
        let (mut min_m, mut min_c) = (None, None);
        if let Some(progress) = self.raft_group.status().progress {
            for (id, pr) in progress.iter() {
                // Reject merge if there is any pending request snapshot,
                // because a target region may merge a source region which is in
                // an invalid state.
                if pr.state == ProgressState::Snapshot
                    || pr.pending_request_snapshot != INVALID_INDEX
                {
                    return Err(box_err!(
                        "there is a pending snapshot peer {} [{:?}], skip merge",
                        id,
                        pr
                    ));
                }
                if min_m.unwrap_or(u64::MAX) > pr.matched {
                    min_m = Some(pr.matched);
                }
                if min_c.unwrap_or(u64::MAX) > pr.committed_index {
                    min_c = Some(pr.committed_index);
                }
            }
        }
        let (mut min_m, min_c) = (min_m.unwrap_or(0), min_c.unwrap_or(0));
        if min_m < min_c {
            warn!(
                "min_matched < min_committed, raft progress is inaccurate";
                "tag" => self.tag(),
                "peer_id" => self.peer.get_id(),
                "min_matched" => min_m,
                "min_committed" => min_c,
            );
            // Reset `min_matched` to `min_committed`, since the raft log at `min_committed`
            // is known to be committed in all peers, all of the peers should
            // also have replicated it
            min_m = min_c;
        }
        Ok((min_m, min_c))
    }

    fn pre_propose_prepare_merge(&self, req: &mut RaftCmdRequest) -> Result<()> {
        let last_index = self.raft_group.raft.raft_log.last_index();
        let (min_matched, min_committed) = self.get_min_progress()?;
        if last_index != min_matched {
            return Err(box_err!(
                "log gap too large, skip merge: matched: {}, committed: {}, last index: {}, last_snapshot: {}",
                min_matched,
                min_committed,
                last_index,
                self.last_sent_snapshot_idx
            ));
        }
        req.mut_admin_request()
            .mut_prepare_merge()
            .set_min_index(min_matched + 1);
        Ok(())
    }

    fn pre_propose(
        &self,
        raft_ctx: &mut RaftContext,
        req: &mut RaftCmdRequest,
    ) -> Result<ProposalContext> {
        raft_ctx
            .global
            .coprocessor_host
            .pre_propose(self.region(), req)?;
        let mut ctx = ProposalContext::empty();
        if req.has_custom_request() {
            let data = req.get_custom_request().get_data();
            if rlog::is_engine_meta_log(data)
                || rlog::is_trigger_trim_over_bound(data)
                || rlog::is_txn_file_ref(data)
            {
                ctx.insert(ProposalContext::PRE_PROCESS);
            } else if self.encryption_key.is_some() {
                // only encrypt entry with user data.
                ctx.insert(ProposalContext::ENCRYPTED);
            }
        } else if req.has_admin_request() {
            match req.get_admin_request().get_cmd_type() {
                AdminCmdType::Split | AdminCmdType::BatchSplit => {
                    if self.raft_group.raft.pending_conf_index > self.get_store().applied_index() {
                        let pending_conf_index = self.raft_group.raft.pending_conf_index;
                        let applied_index = self.get_store().applied_index();
                        info!("there is a pending conf change, try later";
                            "region" => self.tag(),
                            "pending_conf_index" => pending_conf_index,
                            "applied_index" => applied_index,
                        );
                        return Err(box_err!(PENDING_CONF_CHANGE_ERR_MSG));
                    }
                    ctx.insert(ProposalContext::PRE_PROCESS);
                }
                AdminCmdType::PrepareMerge => {
                    self.pre_propose_prepare_merge(req)?;
                    ctx.insert(ProposalContext::PRE_PROCESS);
                }
                AdminCmdType::CommitMerge | AdminCmdType::RollbackMerge => {
                    ctx.insert(ProposalContext::PRE_PROCESS);
                }
                _ => {}
            }
        }
        Ok(ctx)
    }

    /// Propose normal request to raft
    ///
    /// Returns Ok(Either::Left(index)) means the proposal is proposed
    /// successfully and is located on `index` position.
    /// Ok(Either::Right(index)) means the proposal is rejected by
    /// `CmdEpochChecker` and the `index` is the position of
    /// the last conflict admin cmd.
    fn propose_normal(
        &mut self,
        raft_ctx: &mut RaftContext,
        mut req: RaftCmdRequest,
    ) -> Result<Either<u64, u64>> {
        if self.pending_merge_state.is_some() {
            if req.has_admin_request() {
                if req.get_admin_request().get_cmd_type() != AdminCmdType::RollbackMerge {
                    return Err(Error::ProposalInMergingMode(self.region_id));
                }
            } else if req.has_custom_request() {
                let custom_log = CustomRaftLog::new_from_data(req.get_custom_request().get_data());
                if custom_log.get_type() != CustomRaftLogType::EngineMeta {
                    return Err(Error::ProposalInMergingMode(self.region_id));
                }
                let cs = custom_log.get_change_set().unwrap();
                let has_pending_convert_l0 = if cs.has_columnar_compaction() {
                    let row_l0s = cs
                        .get_columnar_compaction()
                        .get_row_l0s()
                        .iter()
                        .collect::<HashSet<_>>();
                    let unconverted_l0s = self
                        .get_store()
                        .shard_meta
                        .as_ref()
                        .map(|shard_meta| shard_meta.unconverted_l0s.iter().collect::<HashSet<_>>())
                        .unwrap_or_default();
                    !row_l0s.is_empty() && row_l0s.eq(&unconverted_l0s)
                } else {
                    false
                };
                if !cs.has_initial_flush() && !has_pending_convert_l0 {
                    return Err(Error::ProposalInMergingMode(self.region_id));
                }
            }
        }

        if self.has_applied_to_current_term() {
            // Only when applied index's term is equal to current leader's term, the
            // information in epoch checker is up to date and can be used to
            // check epoch.
            if let Some(index) = self
                .cmd_epoch_checker
                .propose_check_epoch(&req, self.term())
            {
                return Ok(Either::Right(index));
            }
        } else if req.has_admin_request() {
            // The admin request is rejected because it may need to update epoch checker
            // which introduces an uncertainty and may breaks the correctness of
            // epoch checker.
            // Note: clients in test cases rely on the message to retry.
            return Err(box_err!(
                "{} peer has not applied to current term, applied_term {}, current_term {}",
                self.tag(),
                self.get_store().applied_index_term(),
                self.term()
            ));
        }

        // TODO: validate request for unexpected changes.
        let ctx = match self.pre_propose(raft_ctx, &mut req) {
            Ok(ctx) => ctx,
            Err(e) => {
                warn!(
                    "skip proposal";
                    "tag" => self.tag(),
                    "peer_id" => self.peer.get_id(),
                    "err" => ?e,
                    "error_code" => %e.error_code(),
                );
                return Err(e);
            }
        };
        let propose_index = self.next_proposal_index();
        let data = if ctx.contains(ProposalContext::ENCRYPTED) {
            let encryption_key = self.encryption_key.as_ref().unwrap();
            let buf = &mut self.encryption_buf;
            buf.truncate(0);
            req.write_to_vec(buf)?;
            let mut data =
                Vec::with_capacity(4 + buf.len() + encryption_key.encryption_block_size());
            data.put_u32(encryption_key.current_ver);
            encryption_key.encrypt(buf, self.region_id, propose_index as u32, &mut data);
            data
        } else {
            req.write_to_bytes()?
        };

        if data.len() as u64 > raft_ctx.cfg.raft_entry_max_size.0 {
            error!(
                "entry is too large";
                "tag" => self.tag(),
                "peer_id" => self.peer.get_id(),
                "size" => data.len(),
            );
            return Err(Error::RaftEntryTooLarge {
                region_id: self.region_id,
                entry_size: data.len() as u64,
            });
        }
        self.raft_group.propose(ctx.to_vec(), data)?;
        if self.next_proposal_index() == propose_index {
            // The message is dropped silently, this usually due to leader absence
            // or transferring leader. Both cases can be considered as NotLeader error.
            return Err(Error::NotLeader(self.region_id, None));
        }
        Ok(Either::Left(propose_index))
    }

    pub(crate) fn execute_transfer_leader(
        &mut self,
        _ctx: &mut RaftContext,
        msg: &eraftpb::Message,
    ) {
        let pending_snapshot =
            self.get_store().is_applying_snapshot() || self.has_pending_snapshot();
        if pending_snapshot || msg.get_from() != self.leader_id() {
            info!(
                "reject transferring leader";
                "tag" => self.tag(),
                "peer_id" => self.peer.get_id(),
                "from" => msg.get_from(),
                "pending_snapshot" => pending_snapshot,
            );
            return;
        }

        let mut msg = eraftpb::Message::new();
        msg.set_from(self.peer_id());
        msg.set_to(self.leader_id());
        msg.set_msg_type(eraftpb::MessageType::MsgTransferLeader);
        msg.set_index(self.get_store().applied_index());
        msg.set_log_term(self.term());
        self.raft_group.raft.msgs.push(msg);
    }

    /// Return true to if the transfer leader request is accepted.
    ///
    /// When transferring leadership begins, leader sends a pre-transfer
    /// to target follower first to ensures it's ready to become leader.
    /// After that the real transfer leader process begin.
    ///
    /// 1. pre_transfer_leader on leader: Leader will send a MsgTransferLeader
    ///    to follower.
    /// 2. execute_transfer_leader on follower: If follower passes all necessary
    ///    checks, it will reply an ACK with type MsgTransferLeader and its
    ///    promised persistent index.
    /// 3. execute_transfer_leader on leader: Leader checks if it's appropriate
    ///    to transfer leadership. If it does, it calls raft transfer_leader API
    ///    to do the remaining work.
    ///
    /// See also: tikv/rfcs#37.
    fn propose_transfer_leader(
        &mut self,
        ctx: &mut RaftContext,
        req: RaftCmdRequest,
        cb: Callback,
    ) -> bool {
        ctx.raft_metrics.propose.transfer_leader.inc();

        let transfer_leader = get_transfer_leader_cmd(&req).unwrap();
        let peer = transfer_leader.get_peer();

        let transferred = self.pre_transfer_leader(peer);

        // transfer leader command doesn't need to replicate log and apply, so we
        // return immediately. Note that this command may fail, we can view it just as
        // an advice
        cb.invoke_with_response(make_transfer_leader_response());

        transferred
    }

    // Fails in such cases:
    // 1. A pending conf change has not been applied yet;
    // 2. Removing the leader is not allowed in the configuration;
    // 3. The conf change makes the raft group not healthy;
    // 4. The conf change is dropped by raft group internally.
    /// Returns Ok(Either::Left(index)) means the proposal is proposed
    /// successfully and is located on `index` position.
    /// Ok(Either::Right(index)) means the proposal is rejected by
    /// `CmdEpochChecker` and the `index` is the position of
    /// the last conflict admin cmd.
    fn propose_conf_change(
        &mut self,
        ctx: &mut RaftContext,
        req: &RaftCmdRequest,
    ) -> Result<Either<u64, u64>> {
        if self.pending_merge_state.is_some() {
            return Err(Error::ProposalInMergingMode(self.region_id));
        }

        if self.raft_group.raft.has_pending_conf() {
            let pending_conf_index = self.raft_group.raft.pending_conf_index;
            let applied_index = self.raft_group.raft.raft_log.applied;
            info!(
                "there is a pending conf change, try later";
                "tag" => self.tag(),
                "peer_id" => self.peer.get_id(),
                "pending_conf_index" => pending_conf_index,
                "applied_index" => applied_index,
            );
            return Err(box_err!(
                "{} there is a pending conf change, try later",
                self.tag()
            ));
        }
        // Actually, according to the implementation of conf change in raft-rs, this
        // check must be passed if the previous check that `pending_conf_index`
        // should be less than or equal to `self.get_store().applied_index()` is
        // passed.
        let store = self.get_store();
        if store.applied_index_term() != self.term() {
            return Err(box_err!(
                "{} peer has not applied to current term, applied_term {}, current_term {}",
                self.tag(),
                store.applied_index_term(),
                self.term()
            ));
        }
        if !store.initial_flushed() {
            return Err(box_err!("{} peer has not initial flushed", self.tag()));
        }
        if let Some(index) = self.cmd_epoch_checker.propose_check_epoch(req, self.term()) {
            return Ok(Either::Right(index));
        }

        let data = req.write_to_bytes()?;
        let admin = req.get_admin_request();
        let res = if admin.has_change_peer() {
            self.propose_conf_change_internal(ctx, admin.get_change_peer(), data)
        } else if admin.has_change_peer_v2() {
            self.propose_conf_change_internal(ctx, admin.get_change_peer_v2(), data)
        } else {
            unreachable!()
        };
        if let Err(ref e) = res {
            warn!("failed to propose confchange"; "error" => ?e);
        }
        res.map(Either::Left)
    }

    // Fails in such cases:
    // 1. A pending conf change has not been applied yet;
    // 2. Removing the leader is not allowed in the configuration;
    // 3. The conf change makes the raft group not healthy;
    // 4. The conf change is dropped by raft group internally.
    fn propose_conf_change_internal(
        &mut self,
        ctx: &mut RaftContext,
        change_peer: impl ChangePeerI,
        data: Vec<u8>,
    ) -> Result<u64> {
        let cc = change_peer.to_confchange(data);
        let changes = change_peer.get_change_peers();

        self.check_conf_change(ctx, changes.as_ref(), &cc)?;

        ctx.raft_metrics.propose.conf_change.inc();
        info!(
            "propose conf change peer";
            "tag" => self.tag(),
            "peer_id" => self.peer.get_id(),
            "changes" => ?changes.as_ref(),
            "kind" => ?ConfChangeKind::confchange_kind(changes.as_ref().len()),
        );

        let propose_index = self.next_proposal_index();
        self.raft_group
            .propose_conf_change(ProposalContext::SYNC_LOG.to_vec(), cc)?;
        if self.next_proposal_index() == propose_index {
            // The message is dropped silently, this usually due to leader absence
            // or transferring leader. Both cases can be considered as NotLeader error.
            return Err(Error::NotLeader(self.region_id, None));
        }

        Ok(propose_index)
    }

    fn handle_read(
        &self,
        ctx: &mut RaftContext,
        req: RaftCmdRequest,
        check_epoch: bool,
        read_index: Option<u64>,
    ) -> ReadResponse {
        let region = self.region().clone();
        if check_epoch {
            if let Err(e) = check_region_epoch(&req, &region, true) {
                debug!("epoch not match"; "tag" => self.tag(), "err" => ?e);
                let mut response = cmd_resp::new_error(e);
                cmd_resp::bind_term(&mut response, self.term());
                return ReadResponse {
                    response,
                    snapshot: None,
                    txn_extra_op: TxnExtraOp::Noop,
                };
            }
        }
        let mut resp = ctx.execute(&req, &Arc::new(region), read_index, None);
        if let Some(snap) = resp.snapshot.as_mut() {
            snap.txn_ext = Some(self.txn_ext.clone());
            snap.bucket_meta = self.buckets.as_ref().map(|b| b.meta.clone());
        }
        cmd_resp::bind_term(&mut resp.response, self.term());
        resp
    }

    /// Pings if followers are still connected.
    ///
    /// Leader needs to know exact progress of followers, and
    /// followers just need to know whether leader is still alive.
    pub fn ping(&mut self) {
        if self.is_leader() {
            self.raft_group.ping();
        }
    }

    // Keep consistent with PreprocessRef::clear_merge_in_mem_data.
    pub(crate) fn clear_merge_in_mem_data(&mut self) {
        if self.pending_merge_state.is_some() {
            self.pending_merge_state = None;
            self.want_rollback_merge_peers.clear();
        }
    }

    pub fn bcast_check_stale_peer_message(&mut self, ctx: &mut RaftContext) {
        if self.check_stale_conf_ver < self.region().get_region_epoch().get_conf_ver() {
            self.check_stale_conf_ver = self.region().get_region_epoch().get_conf_ver();
            self.check_stale_peers = self.region().get_peers().to_vec();
        }
        for peer in &self.check_stale_peers {
            if peer.get_id() == self.peer_id() {
                continue;
            }
            let mut extra_msg = ExtraMessage::default();
            extra_msg.set_type(ExtraMessageType::MsgCheckStalePeer);
            self.send_extra_message(extra_msg, &mut ctx.global.trans, peer);
        }
    }

    pub fn on_check_stale_peer_response(
        &mut self,
        check_conf_ver: u64,
        check_peers: Vec<metapb::Peer>,
    ) {
        if self.check_stale_conf_ver < check_conf_ver {
            self.check_stale_conf_ver = check_conf_ver;
            self.check_stale_peers = check_peers;
        }
    }
}

/// `RequestPolicy` decides how we handle a request.
#[derive(Clone, PartialEq, Debug)]
pub enum RequestPolicy {
    // Handle the read request directly without dispatch.
    ReadLocal,
    StaleRead,
    // Handle the read request via raft's SafeReadIndex mechanism.
    ReadIndex,
    ProposeNormal,
    ProposeTransferLeader,
    ProposeConfChange,
}

pub trait RequestInspector {
    /// Has the current term been applied?
    fn has_applied_to_current_term(&mut self) -> bool;
    /// Inspects its lease.
    fn inspect_lease(&mut self) -> LeaseState;

    /// Inspect a request, return a policy that tells us how to
    /// handle the request.
    fn inspect(&mut self, req: &RaftCmdRequest) -> RequestPolicy {
        if req.has_admin_request() {
            if apply::is_conf_change_cmd(req) {
                return RequestPolicy::ProposeConfChange;
            }
            if get_transfer_leader_cmd(req).is_some() {
                return RequestPolicy::ProposeTransferLeader;
            }
            return RequestPolicy::ProposeNormal;
        }
        if req.has_custom_request() {
            return RequestPolicy::ProposeNormal;
        }
        if req.get_header().get_read_quorum() {
            return RequestPolicy::ReadIndex;
        }

        // If applied index's term is differ from current raft's term, leader transfer
        // must happened, if read locally, we may read old value.
        if !self.has_applied_to_current_term() {
            return RequestPolicy::ReadIndex;
        }

        // Local read should be performed, if and only if leader is in lease.
        // None for now.
        match self.inspect_lease() {
            LeaseState::Valid => RequestPolicy::ReadLocal,
            LeaseState::Expired | LeaseState::Suspect => {
                // Perform a consistent read to Raft quorum and try to renew the leader lease.
                RequestPolicy::ReadIndex
            }
        }
    }
}

impl RequestInspector for Peer {
    fn has_applied_to_current_term(&mut self) -> bool {
        self.get_store().applied_index_term() == self.term()
    }

    fn inspect_lease(&mut self) -> LeaseState {
        if !self.raft_group.raft.in_lease() {
            return LeaseState::Suspect;
        }
        // None means now.
        let state = self.leader_lease.inspect(None);
        if LeaseState::Expired == state {
            debug!(
                "leader lease is expired";
                "tag" => self.tag(),
                "peer_id" => self.peer.get_id(),
                "lease" => ?self.leader_lease,
            );
            // The lease is expired, call `expire` explicitly.
            self.leader_lease.expire();
        }
        state
    }
}

impl ReadExecutor for RaftContext {
    fn get_snapshot(&self, region_id: u64, region_ver: u64) -> Result<RegionSnapshot> {
        if let Some(snap) = self.global.engines.kv.get_snap_access(region_id) {
            if snap.get_version() == region_ver {
                return Ok(RegionSnapshot::from_snapshot(
                    snap,
                    self.global.engines.kv.get_value_cache(),
                ));
            }
        }
        Err(Error::StaleCommand)
    }
}

fn get_transfer_leader_cmd(msg: &RaftCmdRequest) -> Option<&TransferLeaderRequest> {
    if !msg.has_admin_request() {
        return None;
    }
    let req = msg.get_admin_request();
    if !req.has_transfer_leader() {
        return None;
    }

    Some(req.get_transfer_leader())
}

/// We enable follower lazy commit to get a better performance.
/// But it may not be appropriate for some requests. This function
/// checks whether the request should be committed on all followers
/// as soon as possible.
fn is_request_urgent(req: &RaftCmdRequest) -> bool {
    if !req.has_admin_request() {
        return false;
    }

    matches!(
        req.get_admin_request().get_cmd_type(),
        AdminCmdType::Split
            | AdminCmdType::BatchSplit
            | AdminCmdType::ChangePeer
            | AdminCmdType::ChangePeerV2
            | AdminCmdType::ComputeHash
            | AdminCmdType::VerifyHash
            | AdminCmdType::PrepareMerge
            | AdminCmdType::CommitMerge
            | AdminCmdType::RollbackMerge
    )
}

fn make_transfer_leader_response() -> RaftCmdResponse {
    let mut response = AdminResponse::default();
    response.set_cmd_type(AdminCmdType::TransferLeader);
    response.set_transfer_leader(TransferLeaderResponse::default());
    let mut resp = RaftCmdResponse::default();
    resp.set_admin_response(response);
    resp
}

pub fn get_preprocess_cmd(entry: &eraftpb::Entry) -> Option<RaftCmdRequest> {
    if entry.entry_type != eraftpb::EntryType::EntryNormal {
        return None;
    }
    let pc = ProposalContext::from_bytes(&entry.context);
    if !pc.contains(ProposalContext::PRE_PROCESS) {
        return None;
    }
    let mut cmd = RaftCmdRequest::default();
    cmd.merge_from_bytes(&entry.data).unwrap();
    Some(cmd)
}

#[derive(Clone, Copy, Debug)]
pub struct ReadyStats {
    pub messages: usize,
    pub committed_entries: usize,
    pub entries: usize,
    pub read_states: usize,
    pub has_ss: bool,
    pub has_hs: bool,
    pub has_snapshot: bool,
}

impl ReadyStats {
    pub fn new(ready: &Ready) -> Self {
        let messages = ready.messages().len();
        let committed_entries = ready.committed_entries().len();
        let entries = ready.entries().len();
        let read_states = ready.read_states().len();
        let has_ss = ready.ss().is_some();
        let has_hs = ready.hs().is_some();
        let has_snapshot = !ready.snapshot().is_empty();
        ReadyStats {
            committed_entries,
            messages,
            entries,
            read_states,
            has_ss,
            has_hs,
            has_snapshot,
        }
    }
}

pub(crate) fn new_merged_region(source: &Region, target: &Region) -> Region {
    let mut merged = target.clone();
    // Use a max value so that pd can ensure overlapped region has a priority.
    let version = cmp::max(
        source.get_region_epoch().get_version(),
        target.get_region_epoch().get_version(),
    ) + 1;
    merged.mut_region_epoch().set_version(version);
    if keys::enc_end_key(&merged) == keys::enc_start_key(source) {
        merged.set_end_key(source.get_end_key().to_vec());
    } else {
        merged.set_start_key(source.get_start_key().to_vec());
    }
    merged
}

// TODO: move to individual file.
/// `PreprocessRef` is a reference of `Peer`, including only necessary fields
/// and methods for preprocess.
///
/// The main purpose of `PreprocessRef` is to separate the preprocess logic
/// from the `Peer`, and reuse for remote recover procedures (e.g. restore
/// keyspace).
pub struct PreprocessRef<'a> {
    pub region: &'a mut metapb::Region,
    pub preprocessed_region: &'a mut Option<metapb::Region>,
    pub peer: &'a mut metapb::Peer,

    pub shard_meta: &'a mut Option<ShardMeta>,
    pub raft_state: &'a mut RaftState,
    // `raft_hard_state` is not reference as we don't have `raft::Raft` during restore.
    // Note: `raft_hard_state` should be updated before handle every batch of Raft entries.
    pub raft_hard_state: eraftpb::HardState,
    pub is_leader: bool,

    pub preprocessed_index: &'a mut u64,

    pub last_committed_split_idx: &'a mut u64,
    pub pending_truncate: &'a mut Option<(u64 /* term */, u64 /* index */)>,
    pub pending_merge_state: &'a mut Option<MergeState>,
    pub want_rollback_merge_peers: &'a mut HashSet<u64>,
    pub learner_skip_idx: &'a mut u64,

    pub encryption_key: &'a mut Option<EncryptionKey>,
}

impl<'a> PreprocessRef<'a> {
    pub(crate) fn from_peer(peer: &'a mut Peer) -> PreprocessRef<'a> {
        let raft_hard_state = peer.raft_group.raft.hard_state();
        let is_leader = peer.is_leader();

        let (
            pb_peer,
            last_committed_split_idx,
            pending_truncate,
            pending_merge_state,
            want_rollback_merge_peers,
            preprocessed_index,
            learner_skip_idx,
            encryption_key,
        ) = (
            &mut peer.peer,
            &mut peer.last_committed_split_idx,
            &mut peer.pending_truncate,
            &mut peer.pending_merge_state,
            &mut peer.want_rollback_merge_peers,
            &mut peer.preprocessed_index,
            &mut peer.learner_skip_idx,
            &mut peer.encryption_key,
        );

        let store = peer.raft_group.mut_store();
        let (region, preprocessed_region, shard_meta, raft_state) = (
            &mut store.region,
            &mut store.preprocessed_region,
            &mut store.shard_meta,
            &mut store.raft_state,
        );

        Self {
            region,
            preprocessed_region,
            peer: pb_peer,
            shard_meta,
            raft_state,
            raft_hard_state,
            is_leader,
            preprocessed_index,
            last_committed_split_idx,
            pending_truncate,
            pending_merge_state,
            want_rollback_merge_peers,
            learner_skip_idx,
            encryption_key,
        }
    }

    fn get_preprocessed_region(&self) -> &metapb::Region {
        self.preprocessed_region.as_ref().unwrap_or(self.region)
    }

    fn store_id(&self) -> u64 {
        self.peer.store_id
    }

    fn region_id(&self) -> u64 {
        self.region.get_id()
    }

    fn peer_id(&self) -> u64 {
        self.peer.get_id()
    }

    pub fn tag(&self) -> PeerTag {
        PeerTag::new(self.store_id(), RegionIdVer::from_region(self.region))
    }

    fn parent_id(&self) -> Option<u64> {
        self.shard_meta
            .as_ref()
            .and_then(|m| m.parent.as_ref())
            .map(|p| p.id)
    }

    fn shard_meta(&self) -> &ShardMeta {
        self.shard_meta.as_ref().unwrap()
    }

    fn mut_shard_meta(&mut self) -> &mut ShardMeta {
        self.shard_meta.as_mut().unwrap()
    }

    pub fn write_raft_state(&mut self, ctx: &mut PreprocessContext<'_>) {
        debug!("{} write raft state {:?}", self.tag(), self.raft_state);
        write_raft_state(
            ctx.raft_wb,
            self.peer_id(),
            self.shard_meta(),
            self.raft_state,
        );
    }

    // Keep consistent with Peer::clear_merge_in_mem_data.
    fn clear_merge_in_mem_data(&mut self) {
        *self.pending_merge_state = None;
        self.want_rollback_merge_peers.clear();
    }
}

pub struct PreprocessContext<'a> {
    pub store_id: u64,
    pub raft: &'a rfengine::RfEngine,
    pub raft_wb: &'a mut rfengine::WriteBatch,
    pub remove_dependents: &'a mut Vec<(u64 /* parent_id */, u64 /* dependent_id */)>,
    pub apply_msgs: &'a mut ApplyMsgs,
    pub cfg: &'a Config,
    pub destroying: &'a mut std::collections::HashSet<u64>,

    // Following fields are `None` for restore scenario.
    pub kv: Option<&'a kvengine::Engine>,
    pub router: Option<&'a RaftRouter>,
}

impl<'a> PreprocessContext<'a> {
    pub(crate) fn from_raft_context(raft_ctx: &'a mut RaftContext) -> PreprocessContext<'a> {
        PreprocessContext {
            store_id: raft_ctx.store_id(),
            raft: &raft_ctx.global.engines.raft,
            raft_wb: &mut raft_ctx.raft_wb,
            remove_dependents: &mut raft_ctx.remove_dependents,
            apply_msgs: &mut raft_ctx.apply_msgs,
            cfg: &raft_ctx.cfg,
            destroying: &mut raft_ctx.global.destroying,
            kv: Some(&raft_ctx.global.engines.kv),
            router: Some(&raft_ctx.global.router),
        }
    }

    // Region(dependent_id) depends on Region(parent_id).
    // Remove dependent will be done after preprocess is persisted.
    // See https://github.com/tidbcloud/cloud-storage-engine/issues/1540.
    pub fn add_remove_dependent(&mut self, parent_id: u64, dependent_id: u64) {
        self.remove_dependents.push((parent_id, dependent_id));
    }

    pub fn build_apply_msg_for_replication(&mut self, entries: Vec<Entry>) {
        let msg = ApplyMsg::Apply(MsgApply::new_for_replication(entries));
        self.apply_msgs.msgs.push(msg);
    }

    pub fn handle_apply_msgs_for_replication(
        &mut self,
        applier: &mut Applier,
        apply_ctx: &mut ApplyContext,
    ) {
        for msg in self.apply_msgs.msgs.drain(..) {
            applier.handle_msg(apply_ctx, msg);
        }
    }

    pub fn build_prepared_msg_for_replication(&mut self, peer_msg: Box<PeerMsg>) {
        match *peer_msg {
            PeerMsg::PrepareChangeSetResult(res, _) => self
                .apply_msgs
                .msgs
                .push(ApplyMsg::ApplyChangeSet(res.unwrap())),
            PeerMsg::PrepareCommitMergeResult(res, commit_index) => {
                self.apply_msgs.msgs.push(ApplyMsg::ResumeCommitMerge {
                    source: res.unwrap(),
                    commit_index,
                })
            }
            PeerMsg::PrepareTxnFileResult { entry_index, .. } => self
                .apply_msgs
                .msgs
                .push(ApplyMsg::ResumeTxnFile(entry_index)),
            _ => {}
        }
    }
}
