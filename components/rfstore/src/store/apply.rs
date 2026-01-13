// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::{RefCell, RefMut},
    cmp::min,
    collections::{HashMap, VecDeque, vec_deque},
    fmt::{self, Debug, Formatter},
    mem,
    ops::RangeBounds,
    sync::{Arc, Mutex, atomic::AtomicU64},
    time::Duration,
    vec::Drain,
};

use api_version::{ApiV2, api_v2::is_whole_keyspace_range};
use bytes::{Buf, Bytes};
use cloud_encryption::EncryptionKey;
use fail::fail_point;
use kvengine::{
    ChangeSet, ENCRYPTION_KEY, EXTRA_CF, Engine, FilePrepareType, LOCK_CF, PrepareOpts, SnapAccess,
    TRIM_OVER_BOUND, TRIM_OVER_BOUND_ENABLE, TXN_FILE_REF, UserMeta, WriteBatch,
    encode_extra_txn_status_key, get_shard_property, mvcc, table::InnerKey, util::PropertiesHelper,
};
use kvenginepb::{TxnFileRef, TxnFileRefs};
use kvproto::{
    metapb,
    metapb::{PeerRole, Region},
    raft_cmdpb::{
        AdminCmdType, AdminRequest, AdminResponse, BatchSplitRequest, BatchSplitResponse,
        ChangePeerRequest, RaftCmdRequest, RaftCmdResponse, RaftRequestHeader, RaftResponseHeader,
    },
};
use log_wrappers::Value;
use pd_client::{BucketStat, new_bucket_write_stats};
use prometheus::local::LocalHistogram;
use protobuf::{Message, RepeatedField};
use raft::{
    StateRole,
    eraftpb::{ConfChange, ConfChangeType, ConfChangeV2, EntryType},
};
use raft_proto::eraftpb;
use raftstore::store::{
    fsm::metrics::*,
    metrics::*,
    util,
    util::{ChangePeerI, ConfChangeKind},
};
use rand::Rng;
use tikv_util::{
    box_err, debug, error, info,
    store::{find_peer, find_peer_mut, remove_peer},
    time::{Instant, duration_to_sec},
    warn,
};
use time::Timespec;
use txn_types::LockType;

use super::*;
use crate::{
    RaftRouter, RaftStoreRouter,
    errors::*,
    store::{
        cmd_resp::{bind_term, err_resp},
        metrics::{LOCK_CACHE_CAPCITY, LOCK_CACHE_LEN, STORE_PROPOSE_SWITCH_MEM_TABLE_COUNTER},
    },
};

pub(crate) struct PendingCmd {
    pub(crate) index: u64,
    pub(crate) term: u64,
    pub(crate) cb: Callback,
}

impl PendingCmd {
    pub(crate) fn new(index: u64, term: u64, cb: Callback) -> Self {
        Self { index, term, cb }
    }
}

#[derive(Default)]
pub(crate) struct PendingCmdQueue {
    pub(crate) normals: VecDeque<PendingCmd>,
    pub(crate) conf_change: Option<PendingCmd>,
}

impl PendingCmdQueue {
    pub(crate) fn pop_normal(&mut self, index: u64, term: u64) -> Option<PendingCmd> {
        self.normals.pop_front().and_then(|cmd| {
            if (cmd.term, cmd.index) > (term, index) {
                self.normals.push_front(cmd);
                return None;
            }
            Some(cmd)
        })
    }

    pub(crate) fn append_normal(&mut self, cmd: PendingCmd) {
        self.normals.push_back(cmd)
    }

    pub(crate) fn take_conf_change(&mut self) -> Option<PendingCmd> {
        // conf change will not be affected when changing between follower and leader,
        // so there is no need to check term.
        self.conf_change.take()
    }

    // TODO: seems we don't need to separate conf change from normal entries.
    pub(crate) fn set_conf_change(&mut self, cmd: PendingCmd) {
        self.conf_change = Some(cmd)
    }
}

#[derive(Default, Debug)]
pub struct ChangePeer {
    pub index: u64,
    // The proposed ConfChangeV2 or (legacy) ConfChange
    // ConfChange (if it is) will convert to ConfChangeV2
    pub conf_change: ConfChangeV2,
    // The change peer requests come along with ConfChangeV2
    // or (legacy) ConfChange, for ConfChange, it only contains
    // one element
    pub changes: Vec<ChangePeerRequest>,
    pub region: Region,
}

pub struct Range {
    pub cf: String,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

impl Debug for Range {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ cf: {:?}, start_key: {:?}, end_key: {:?} }}",
            self.cf,
            log_wrappers::Value::key(&self.start_key),
            log_wrappers::Value::key(&self.end_key)
        )
    }
}

#[allow(unused)]
impl Range {
    fn new(cf: String, start_key: Vec<u8>, end_key: Vec<u8>) -> Range {
        Range {
            cf,
            start_key,
            end_key,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NewSplitPeer {
    pub peer_id: u64,
    // `None` => success,
    // `Some(s)` => fail due to `s`.
    pub result: Option<String>,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ExecResult {
    ChangePeer(ChangePeer),
    SplitRegion { regions: Vec<Region> },
    DeleteRange { ranges: Vec<Range> },
    UnsafeDestroy,
    PrepareMerge { region: Region },
    CommitMerge { region: Region, source: Region },
    RollbackMerge { region: Region, commit: u64 },
    RestoreShard { cs: kvenginepb::ChangeSet },
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum ApplyResult {
    None,
    /// Additional result that needs to be sent back to raftstore.
    Res(ExecResult),
}

#[derive(Debug)]
pub(crate) struct Proposal {
    pub(crate) is_conf_change: bool,
    pub(crate) index: u64,
    pub(crate) term: u64,
    pub(crate) cb: Callback,

    /// `propose_time` is set to the last time when a peer starts to renew
    /// lease.
    pub propose_time: Option<Timespec>,
    pub must_pass_epoch_check: bool,
}

#[derive(Default)]
pub struct ApplyMsgs {
    pub(crate) msgs: Vec<ApplyMsg>,
}

impl ApplyMsgs {
    pub fn get_last_change_set(&self) -> Option<kvenginepb::ChangeSet> {
        let last = self.msgs.last()?;
        match last {
            ApplyMsg::PendingSplit(cs) => Some(cs.clone()),
            ApplyMsg::PrepareCommitMerge { source, .. } => Some(source.clone()),
            ApplyMsg::PrepareChangeSet { cs, .. } => Some(cs.clone()),
            _ => None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.msgs.is_empty()
    }

    pub fn len(&self) -> usize {
        self.msgs.len()
    }

    pub fn clear(&mut self) {
        self.msgs.clear();
    }
}

pub(crate) struct ApplyBatch {
    pub(crate) applier: Arc<Mutex<Applier>>,
    pub(crate) msgs: Vec<ApplyMsg>,
    pub(crate) applying_cnt: Arc<AtomicU64>,
    pub(crate) send_time: Instant,
}

/// The Applier of a Region which is responsible for handling committed
/// raft log entries of a Region.
///
/// `Apply` is a term of Raft, which means executing the actual commands.
/// In Raft, once some log entries are committed, for every peer of the Raft
/// group will apply the logs one by one. For write commands, it does write or
/// delete to local engine; for admin commands, it does some meta change of the
/// Raft group.
///
/// The raft worker receives all the apply tasks of different Regions
/// located at this store, and it will get the corresponding applier to
/// handle the apply task to make the code logic more clear.
#[derive(Default)]
pub struct Applier {
    pub(crate) peer: metapb::Peer,
    pub(crate) term: u64,
    pub(crate) region: metapb::Region,

    /// If the applier should be stopped from polling.
    /// A applier can be stopped in conf change, merge or requested by destroy
    /// message.
    pub(crate) stopped: bool,
    /// Set to true when removing itself because of
    /// `ConfChangeType::RemoveNode`, and then any following committed logs
    /// in same Ready should be applied failed.
    pub(crate) pending_remove: bool,

    /// The commands waiting to be committed and applied
    pub(crate) pending_cmds: PendingCmdQueue,

    pub(crate) apply_state: RaftApplyState,

    pub(crate) lock_cache: HashMap<Vec<u8>, Vec<u8>>,
    last_lock_cache_len: i64,
    last_lock_cache_cap: i64,

    pub(crate) snap: Option<SnapAccess>,

    pub(crate) metrics: ApplyMetrics,

    pub(crate) pending_split: HashMap<u64, kvenginepb::ChangeSet>,

    pub(crate) commit_merge_source_tables: HashMap<u64, ChangeSet>,

    pub(crate) paused_apply_queue: PausedApplyQueue,

    // The ChangeSetType is used to decide whether to pause the apply task for split/merge.
    pub(crate) scheduled_change_sets: VecDeque<(u64, ChangeSetType)>,

    pub(crate) prepared_change_sets: HashMap<u64, kvengine::ChangeSet>,

    pub(crate) role: raft::StateRole,

    mem_table_state: Option<MemTableState>,

    last_property_term: u64,

    buckets: Option<BucketStat>,

    encryption_key: Option<EncryptionKey>,
    decryption_buf: Vec<u8>,

    /// Shard is waiting to be active/inactive until the raft logs of previous
    /// term are all been preprocessed, so that all change sets proposed by
    /// the previous leader are scheduled.
    ///
    /// Note that the `role` of applier is NOT pending.
    shard_pending_active: Option<(
        u64,  // current term
        bool, // is_active
    )>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum ChangeSetType {
    Normal,
    Snapshot,
    TrimOverBound,
    InitialFlush,
    DestroyRange,
    Compaction { shard_ver: u64 },
}

impl ChangeSetType {
    fn from_change_set(cs: &kvenginepb::ChangeSet) -> Self {
        if cs.has_snapshot() {
            ChangeSetType::Snapshot
        } else if cs.has_trim_over_bound() {
            ChangeSetType::TrimOverBound
        } else if cs.has_initial_flush() {
            ChangeSetType::InitialFlush
        } else if cs.has_destroy_range() {
            ChangeSetType::DestroyRange
        } else if cs.has_compaction() || cs.has_major_compaction() || cs.has_columnar_compaction() {
            ChangeSetType::Compaction {
                shard_ver: cs.shard_ver,
            }
        } else {
            ChangeSetType::Normal
        }
    }

    fn should_pause_for_split(&self) -> bool {
        matches!(
            self,
            ChangeSetType::InitialFlush | ChangeSetType::DestroyRange
        )
    }
}

impl Applier {
    pub(crate) fn new_from_peer(peer: &PeerFsm) -> Self {
        let reg = MsgRegistration::new(&peer.peer);
        Self::new_from_reg(reg)
    }

    pub fn new_from_reg(reg: MsgRegistration) -> Self {
        Self {
            peer: reg.peer,
            term: reg.term,
            region: reg.region,
            apply_state: reg.apply_state,
            encryption_key: reg.encryption_key,
            ..Default::default()
        }
    }

    pub fn is_paused(&self) -> bool {
        !self.paused_apply_queue.paused_sequences.is_empty()
    }

    pub fn get_apply_state(&self) -> RaftApplyState {
        self.apply_state
    }

    fn tag(&self) -> PeerTag {
        let id_ver = RegionIdVer::new(self.region.id, self.region.get_region_epoch().version);
        PeerTag::new(self.peer.store_id, id_ver)
    }

    pub(crate) fn new_for_recover(
        store_id: u64,
        region: metapb::Region,
        snap: SnapAccess,
        apply_state: RaftApplyState,
    ) -> Self {
        let peer = find_peer(&region, store_id)
            .unwrap_or_else(|| {
                // When recover a shard in restore, it's possible that the peer is just removed
                // from the region, to avoid panic, we change the peer to the first
                // one. This change is safe because in recover applier, peer is only
                // used for logging.
                let tag = PeerTag::new(store_id, RegionIdVer::from_region(&region));
                warn!(
                    "{} peer is not in region {:?}, change to the first one",
                    tag, region
                );
                region.get_peers().first().unwrap()
            })
            .clone();
        let encryption_key = snap.get_encryption_key();
        Self {
            peer,
            term: RAFT_INIT_LOG_TERM,
            region,
            apply_state,
            snap: Some(snap),
            encryption_key,
            ..Default::default()
        }
    }

    pub fn new_for_replication(
        region: metapb::Region,
        encryption_key: Option<EncryptionKey>,
        apply_state: RaftApplyState,
    ) -> Self {
        let peer = region.peers.first().cloned().unwrap();
        Self {
            peer,
            term: apply_state.applied_index_term,
            region,
            apply_state,
            snap: None,
            encryption_key,
            ..Default::default()
        }
    }

    fn clear_caches(&mut self) {
        self.snap.take();
        self.lock_cache = HashMap::new();
        self.mem_table_state.take();
    }

    fn update_lock_cache_metrics(&mut self) {
        let current_len = self.lock_cache.len() as i64;
        let len_diff = current_len - self.last_lock_cache_len;
        self.last_lock_cache_len = current_len;
        if len_diff != 0 {
            LOCK_CACHE_LEN.add(len_diff);
        }
        let current_cap = self.lock_cache.capacity() as i64;
        let cap_diff = current_cap - self.last_lock_cache_cap;
        self.last_lock_cache_cap = current_cap;
        if cap_diff != 0 {
            LOCK_CACHE_CAPCITY.add(cap_diff);
        }
    }

    pub(crate) fn get_peer(&self) -> &metapb::Peer {
        &self.peer
    }

    pub(crate) fn id(&self) -> u64 {
        self.get_peer().get_id()
    }

    pub fn region_id(&self) -> u64 {
        self.region.get_id()
    }

    pub fn region_version(&self) -> u64 {
        self.region.get_region_epoch().version
    }

    pub(crate) fn commit_lock(
        &mut self,
        kv: &kvengine::Engine,
        wb: &mut kvengine::WriteBatch,
        key: &[u8],
        commit_ts: u64,
        log_index: u64,
    ) {
        let lock_val = self.get_lock_for_commit(kv, key, commit_ts, log_index);
        if lock_val.is_empty() {
            return;
        }
        let lock = txn_types::Lock::parse(&lock_val).unwrap_or_else(|x| {
            panic!(
                "failed to parse lock value {:?}, local_val {:?}",
                x, &lock_val
            );
        });
        let start_ts = lock.ts.into_inner();
        let user_meta = &UserMeta::new(start_ts, commit_ts).to_array()[..];
        match lock.lock_type {
            LockType::Lock | LockType::Pessimistic => {
                let op_lock_key = mvcc::encode_extra_txn_status_key(key, start_ts);
                wb.put(
                    mvcc::EXTRA_CF,
                    op_lock_key.chunk(),
                    &[0],
                    0,
                    user_meta,
                    commit_ts,
                )
            }
            LockType::Put => {
                let val = lock.short_value.unwrap();
                wb.put(mvcc::WRITE_CF, key, &val, 0, user_meta, commit_ts);
            }
            LockType::Delete => {
                wb.put(mvcc::WRITE_CF, key, &[], 0, user_meta, commit_ts);
            }
        }
        wb.delete(mvcc::LOCK_CF, key, 0);
    }

    pub(crate) fn get_lock_for_commit(
        &mut self,
        kv: &kvengine::Engine,
        key: &[u8],
        commit_ts: u64,
        log_index: u64,
    ) -> Vec<u8> {
        if let Some((_, v)) = self.lock_cache.remove_entry(key) {
            return v;
        }
        let region_id = self.region.get_id();
        if self.snap.is_none() {
            self.snap = Some(kv.get_snap_access(region_id).unwrap());
        }
        let mut snap = self.snap.clone().unwrap();
        let item = snap.get(mvcc::LOCK_CF, key, u64::MAX);
        if item.value_len() > 0 {
            return item.get_value().to_vec();
        }
        // Maybe snap is stale, try to get snap access again.
        snap = kv.get_snap_access(region_id).unwrap();
        self.snap = Some(snap.clone());
        // If the lock is txn file, the commit will be executed in exec_txn_file_ref.
        // So we don't need to read the txn files here.
        let item = snap.get_non_txn_file_lock(key);
        if item.value_len() > 0 {
            return item.get_value().to_vec();
        }
        // TODO: investigate why there is duplicated commit.
        if snap.has_unloaded_tables() {
            // Currently only WRITE_CF data would be unloaded.
            // TODO: save which data is unloaded for more general.
            kv.load_unloaded_tables(snap.get_id(), snap.get_version(), false).unwrap_or_else(|err| {
                panic!(
                    "{} failed to load unloaded tables, snap_write_sequence: {}, snap_files: {:?}, log_index:{}, err:{:?}",
                    snap.get_tag(),
                    snap.get_write_sequence(),
                    snap.get_all_sst_files(),
                    log_index,
                    err,
                );
            });
            snap = kv.get_snap_access(region_id).unwrap();
            self.snap = Some(snap.clone());
        }
        let item = snap.get(mvcc::WRITE_CF, key, u64::MAX);
        let old_commit_ts = if item.user_meta_len() == 0 {
            0
        } else {
            UserMeta::from_slice(item.user_meta()).commit_ts
        };
        if old_commit_ts != commit_ts {
            panic!(
                "{} failed to get lock for key {:?}, snap_write_sequence: {}, snap_files: {:?}, log_index:{}",
                snap.get_tag(),
                key,
                snap.get_write_sequence(),
                snap.get_all_sst_files(),
                log_index,
            );
        }
        warn!("duplicated commit for key {:?}, {}", key, snap.get_tag());
        vec![]
    }

    pub(crate) fn rollback(
        &mut self,
        wb: &mut kvengine::WriteBatch,
        key: &[u8],
        start_ts: u64,
        delete_lock: bool,
    ) {
        if start_ts == 0 {
            // start_ts == 0 means delete_lock only.
            assert!(delete_lock);
        } else {
            let rollback_key = mvcc::encode_extra_txn_status_key(key, start_ts);
            let user_meta = &mvcc::UserMeta::new(start_ts, 0).to_array()[..];
            wb.put(
                mvcc::EXTRA_CF,
                rollback_key.chunk(),
                &[0],
                0,
                user_meta,
                start_ts,
            );
        }
        if delete_lock {
            wb.delete(mvcc::LOCK_CF, key, 0);
            self.lock_cache.remove(key);
        }
    }

    pub(crate) fn exec_txn_file_ref(
        &mut self,
        engine: &Engine,
        wb: &mut kvengine::WriteBatch,
        txn_file_ref: TxnFileRef,
    ) {
        debug!("{} apply txn file ref {:?}", self.tag(), txn_file_ref);
        if !txn_file_ref.user_meta.is_empty() {
            let txn_file_um = UserMeta::from_slice(&txn_file_ref.user_meta);
            let snap = engine
                .get_shard(self.region_id())
                .unwrap()
                .new_snap_access();
            let start_ts = txn_file_ref.start_ts;
            let lock_txn_file_opt = snap.get_lock_txn_file(start_ts);
            if lock_txn_file_opt.is_none() {
                error!("{} lock txn file not found, maybe duplicated", self.tag());
                // TODO: verify it is duplicated
                return;
            }
            let lock_txn_file = lock_txn_file_opt.unwrap();
            let lock = txn_types::Lock::parse(lock_txn_file.get_lock_val_prefix()).unwrap();
            let primary_key = lock.primary.as_slice();
            let inner_primary_key = InnerKey::from_outer_key(primary_key);
            let is_primary = lock_txn_file.lower_bound() <= inner_primary_key
                && inner_primary_key < lock_txn_file.upper_bound();
            if is_primary {
                if txn_file_um.is_rollback() {
                    let rollback_key = encode_extra_txn_status_key(primary_key, start_ts);
                    let user_meta = UserMeta::new(start_ts, 0).to_array();
                    wb.put(
                        EXTRA_CF,
                        rollback_key.chunk(),
                        &[0],
                        0,
                        &user_meta,
                        start_ts,
                    );
                } else {
                    let item = snap.get(LOCK_CF, primary_key, 0);
                    let primary_lock =
                        txn_types::Lock::parse(item.get_value()).unwrap_or_else(|err| {
                            panic!(
                                "{} apply txn file ref: parse lock error: {:?}, value {}, primary_key {}, start_ts {}, lock_txn_file {:?}",
                                self.tag(),
                                err,
                                &Value::value(item.get_value()),
                                &Value::key(primary_key),
                                start_ts,
                                lock_txn_file,
                            );
                        });
                    if primary_lock.lock_type == LockType::Lock {
                        let op_lock_key = encode_extra_txn_status_key(primary_key, start_ts);
                        let user_meta = UserMeta::new(start_ts, txn_file_um.commit_ts).to_array();
                        wb.put(
                            EXTRA_CF,
                            op_lock_key.chunk(),
                            &[0],
                            0,
                            &user_meta,
                            txn_file_um.commit_ts,
                        );
                    }
                }
            }
        }
        let mut txn_file_refs = TxnFileRefs::new();
        txn_file_refs.mut_txn_file_refs().push(txn_file_ref);
        wb.set_property(TXN_FILE_REF, &txn_file_refs.write_to_bytes().unwrap());
    }

    fn exec_admin_cmd(
        &mut self,
        ctx: &mut ApplyContext,
        req: &RaftCmdRequest,
    ) -> Result<(RaftCmdResponse, ApplyResult)> {
        let request = req.get_admin_request();
        let cmd_type = request.get_cmd_type();
        if cmd_type != AdminCmdType::CompactLog {
            info!(
                "execute admin command";
                "tag" => self.tag(),
                "peer_id" => self.id(),
                "term" => ctx.exec_log_term,
                "index" => ctx.exec_log_index,
                "command" => ?request,
            );
        }

        let (mut response, exec_result) = match cmd_type {
            AdminCmdType::ChangePeer => self.exec_change_peer(ctx, request),
            AdminCmdType::ChangePeerV2 => self.exec_change_peer_v2(ctx, request),
            AdminCmdType::BatchSplit => self.exec_split(ctx, request),
            AdminCmdType::TransferLeader => Err(box_err!("transfer leader won't exec")),
            AdminCmdType::PrepareMerge => self.exec_prepare_merge(ctx),
            AdminCmdType::RollbackMerge => self.exec_rollback_merge(ctx, req.get_admin_request()),
            AdminCmdType::CommitMerge => self.exec_commit_merge(ctx, req.get_admin_request()),
            // TODO: is it backward compatible to add new cmd_type?
            _ => Err(box_err!("unsupported admin command type")),
        }?;
        response.set_cmd_type(cmd_type);
        if let Some(observer) = &mut ctx.observer {
            let region_id = req.get_header().get_region_id();
            let region_version = req.get_header().get_region_epoch().get_version();
            observer.on_apply_admin(region_id, region_version, ctx.exec_log_index, request);
        }

        let mut resp = RaftCmdResponse::default();
        if !req.get_header().get_uuid().is_empty() {
            let uuid = req.get_header().get_uuid().to_vec();
            resp.mut_header().set_uuid(uuid);
        }
        resp.set_admin_response(response);
        Ok((resp, exec_result))
    }

    fn record_write_stat(&mut self, k: &[u8], v: &[u8]) {
        self.metrics.written_keys += 1;
        self.metrics.written_bytes += (k.len() + v.len()) as u64;
        if let Some(s) = self.buckets.as_mut() {
            s.write_key(k, v.len() as u64);
        }
    }

    pub(crate) fn exec_custom_log(
        &mut self,
        ctx: &mut ApplyContext,
        cl: &CustomRaftLog<'_>,
        cs: Option<kvenginepb::ChangeSet>,
    ) -> Result<(RaftCmdResponse, ApplyResult)> {
        let mut observer = ctx.observer.take();
        let mut wb_ref = ctx.get_engine_wb(self.region.get_id());
        let wb = &mut wb_ref;
        let engine = &ctx.engine;
        let log_index = ctx.exec_log_index;
        wb.set_sequence(log_index);
        if ctx.exec_log_term != self.last_property_term {
            wb.set_property(TERM_KEY, &ctx.exec_log_term.to_le_bytes());
            self.last_property_term = ctx.exec_log_term;
        }
        let timer = Instant::now();
        match cl.get_type() {
            CustomRaftLogType::Prewrite => cl.iterate_lock(|k, v| {
                wb.put(mvcc::LOCK_CF, k, v, 0, &[], 0);
                self.record_write_stat(k, v);
                self.lock_cache.insert(k.to_vec(), v.to_vec());
            }),
            CustomRaftLogType::PessimisticLock => cl.iterate_lock(|k, v| {
                wb.put(mvcc::LOCK_CF, k, v, 0, &[], 0);
            }),
            CustomRaftLogType::Commit => cl.iterate_commit(|k, commit_ts| {
                self.commit_lock(engine, wb, k, commit_ts, log_index);
            }),
            CustomRaftLogType::OnePc => {
                cl.iterate_one_pc(|k, v, is_extra, del_lock, start_ts, commit_ts| {
                    self.record_write_stat(k, v);
                    let user_meta = mvcc::UserMeta::new(start_ts, commit_ts).to_array();
                    if is_extra {
                        let op_lock_key = mvcc::encode_extra_txn_status_key(k, start_ts);
                        wb.put(mvcc::EXTRA_CF, &op_lock_key, &[0], 0, &user_meta, commit_ts);
                    } else {
                        wb.put(mvcc::WRITE_CF, k, v, 0, &user_meta, commit_ts);
                    }
                    if del_lock {
                        wb.delete(mvcc::LOCK_CF, k, 0);
                    }
                })
            }
            CustomRaftLogType::Rollback => cl.iterate_rollback(|k, start_ts, del_lock| {
                self.rollback(wb, k, start_ts, del_lock);
            }),
            CustomRaftLogType::PessimisticRollback => {
                cl.iterate_del_lock(|k| {
                    wb.delete(mvcc::LOCK_CF, k, 0);
                });
            }
            CustomRaftLogType::EngineMeta => {
                let cs = cs.unwrap_or_else(|| cl.get_change_set().unwrap());
                // Checking kind of change set is duplicated, but it ensures the consistency of
                // online and recovery process.
                // Besides, some change sets (e.g. restore shard) do not have the same version
                // with `self.region`.
                if is_change_set_affect_mem_table(&cs) {
                    let current_ver = self.region.get_region_epoch().version;
                    if cs.shard_ver == current_ver {
                        if is_property_change_set(&cs) {
                            wb.set_property(cs.get_property_key(), cs.get_property_value());
                        }
                    } else {
                        warn!("{} exec custom log: shard version not match", self.tag();
                            "cs.ver" => cs.shard_ver, "log_index" => log_index);
                    }
                }
            }
            CustomRaftLogType::ResolveLock => {
                cl.iterate_resolve_lock(|tp, k, ts, del_lock| match tp {
                    CustomRaftLogType::Commit => self.commit_lock(engine, wb, k, ts, log_index),
                    CustomRaftLogType::Rollback => self.rollback(wb, k, ts, del_lock),
                    _ => unreachable!("unexpected custom log type: {:?}", tp),
                })
            }
            CustomRaftLogType::SwitchMemTable => {
                let switch_mem_table_size = cl.get_switch_mem_table();
                if self.mut_mem_table_state(engine).mem_table_size >= switch_mem_table_size {
                    wb.set_switch_mem_table();
                }
            }
            CustomRaftLogType::TriggerTrimOverBound => {
                let parameter = cl.get_trigger_trim_over_bound();
                self.trigger_trim_over_bound(parameter, wb, engine, ctx.router.as_ref());
            }
            CustomRaftLogType::TxnFileRef => {
                let txn_file_ref = cl.get_txn_file_ref().unwrap();
                self.exec_txn_file_ref(engine, wb, txn_file_ref);
            }
        }
        let writable_mem_tbl_state = ctx.engine.write(wb, cl.get_raw());
        if let Some(observer) = &mut observer {
            observer.on_apply(self.region_id(), self.region_version(), log_index, wb);
        }
        drop(wb_ref);
        ctx.observer = observer;
        let mem_states = self.mut_mem_table_state(engine);
        if writable_mem_tbl_state.is_none() {
            mem_states.set_switch_time(timer);
        }
        let (mem_table_size, unpersisted_props_size) = writable_mem_tbl_state.unwrap_or_default();
        mem_states.update(mem_table_size, unpersisted_props_size);
        self.maybe_propose_switch_mem_table(ctx, timer);
        ctx.apply_time.observe(timer.saturating_elapsed_secs());
        // self.metrics.written_bytes += wb.estimated_size() as u64;
        // self.metrics.written_keys += wb.num_entries() as u64;
        let mut resp = RaftCmdResponse::default();
        let header = RaftResponseHeader::default();
        resp.set_header(header);
        Ok((resp, ApplyResult::None))
    }

    /// Applies raft command.
    ///
    /// An apply operation can fail in the following situations:
    ///   1. it encounters an error that will occur on all stores, it can
    ///      continue applying next entry safely, like epoch not match for
    ///      example;
    ///   2. it encounters an error that may not occur on all stores, in this
    ///      case we should try to apply the entry again or panic. Considering
    ///      that this usually due to disk operation fail, which is rare, so
    ///      just panic is ok.
    fn apply_raft_log(
        &mut self,
        ctx: &mut ApplyContext,
        req: &RaftCmdRequest,
    ) -> (RaftCmdResponse, ApplyResult) {
        if let Err(err) = check_region_epoch(req, &self.region, true) {
            let mut check_in_region_worker = false;
            if let Some(custom) = rlog::get_custom_log(req) {
                if custom.get_type() == rlog::CustomRaftLogType::EngineMeta {
                    check_in_region_worker = true;
                }
            }
            if !check_in_region_worker {
                return (err_resp(err, ctx.exec_log_term), ApplyResult::None);
            }
        }
        if req.has_admin_request() {
            return match self.exec_admin_cmd(ctx, req) {
                Ok((resp, result)) => (resp, result),
                Err(e) => (err_resp(e, ctx.exec_log_term), ApplyResult::None),
            };
        }
        let custom = rlog::get_custom_log(req).unwrap();
        match self.exec_custom_log(ctx, &custom, None) {
            Ok((resp, result)) => (resp, result),
            Err(e) => (err_resp(e, ctx.exec_log_term), ApplyResult::None),
        }
    }

    fn handle_apply_result(
        &mut self,
        ctx: &mut ApplyContext,
        mut resp: RaftCmdResponse,
        result: &ApplyResult,
        is_conf_change: bool,
    ) {
        self.apply_state.applied_index = ctx.exec_log_index;
        self.apply_state.applied_index_term = ctx.exec_log_term;
        if let ApplyResult::Res(exec_result) = result {
            match exec_result {
                ExecResult::ChangePeer(cp) => {
                    self.region = cp.region.clone();
                    if let Some(new_peer) =
                        cp.region.get_peers().iter().find(|p| p.id == self.peer.id)
                    {
                        self.peer = new_peer.clone();
                    } else {
                        self.pending_remove = true;
                    }
                }
                ExecResult::SplitRegion { regions } => {
                    self.region = regions.last().unwrap().clone();
                }
                ExecResult::DeleteRange { .. } => {}
                ExecResult::UnsafeDestroy => {}
                ExecResult::PrepareMerge { region } => {
                    self.region = region.clone();
                }
                ExecResult::CommitMerge { region, .. } => {
                    self.region = region.clone();
                }
                ExecResult::RollbackMerge { region, .. } => {
                    self.region = region.clone();
                }
                ExecResult::RestoreShard { .. } => unreachable!(),
            }
        }
        // TODO: if we have exec_result, maybe we should return this callback too. Outer
        // store will call it after handing exec result.
        bind_term(&mut resp, ctx.exec_log_term);
        if let Some(cmd_cb) =
            self.find_callback(ctx.exec_log_index, ctx.exec_log_term, is_conf_change)
        {
            cmd_cb.invoke_with_response(resp);
        }
    }

    fn find_callback(&mut self, index: u64, term: u64, is_conf_change: bool) -> Option<Callback> {
        if is_conf_change {
            if let Some(cmd) = self.pending_cmds.take_conf_change() {
                if cmd.index == index && cmd.term == term {
                    return Some(cmd.cb);
                }
                notify_stale_req(term, cmd.cb, "conf change term not match");
            }
            return None;
        }
        while let Some(head) = self.pending_cmds.pop_normal(index, term) {
            if head.term == term {
                if head.index == index {
                    return Some(head.cb);
                } else {
                    panic!(
                        "{} unexpected callback at term {}, found index {}, expected {}",
                        self.tag(),
                        term,
                        head.index,
                        index
                    );
                }
            } else {
                // Because of the lack of original RaftCmdRequest, we skip calling
                // coprocessor here.
                notify_stale_req(term, head.cb, "term not match");
            }
        }
        None
    }

    pub(crate) fn parse_cmd(&mut self, entry: &eraftpb::Entry) -> RaftCmdRequest {
        parse_raft_cmd(
            self.tag(),
            entry,
            self.encryption_key.as_ref(),
            &mut self.decryption_buf,
        )
    }

    fn handle_raft_entry_normal(
        &mut self,
        ctx: &mut ApplyContext,
        entry: &eraftpb::Entry,
    ) -> ApplyResult {
        // fail_point!(
        // "yield_apply_first_region",
        // self.region.get_start_key().is_empty() &&
        // !self.region.get_end_key().is_empty(), |_| ApplyResult::Yield
        // );

        let index = entry.get_index();
        let term = entry.get_term();

        if !entry.get_data().is_empty() {
            let cmd = self.parse_cmd(entry);
            assert!(index > 0);
            // if pending remove, apply should be aborted already.
            assert!(!self.pending_remove);
            let (resp, result) = self.apply_raft_log(ctx, &cmd);
            self.handle_apply_result(ctx, resp, &result, false);
            return result;
        }
        self.apply_state.applied_index = index;
        self.apply_state.applied_index_term = term;
        assert!(term > 0);
        // when a peer become leader, it will send an empty entry.
        // apparently, all the callbacks whose term is less than entry's term are stale.
        while let Some(cmd) = self.pending_cmds.pop_normal(u64::MAX, term - 1) {
            cmd.cb
                .invoke_with_response(err_resp(Error::StaleCommand, term));
        }
        ApplyResult::None
    }

    pub(crate) fn exec_change_peer(
        &mut self,
        ctx: &mut ApplyContext,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult)> {
        let star_time = Instant::now();
        assert!(request.has_change_peer());
        let request = request.get_change_peer();
        let change_type = request.get_change_type();

        fail_point!(
            "apply_on_conf_change_1_3_1",
            (self.id() == 1 || self.id() == 3) && self.region_id() == 1,
            |_| panic!("should not use return")
        );
        fail_point!(
            "apply_on_conf_change_3_1",
            self.id() == 3 && self.region_id() == 1,
            |_| panic!("should not use return")
        );
        fail_point!(
            "apply_on_conf_change_all_1",
            self.region_id() == 1,
            |_| panic!("should not use return")
        );
        info!(
            "exec ConfChange";
            "tag" => self.tag(),
            "peer_id" => self.id(),
            "type" => util::conf_change_type_str(change_type),
            "epoch" => ?self.region.get_region_epoch(),
        );

        let changes = vec![request.clone()];
        let region = region_apply_conf_change(&self.region, &changes, self.id(), self.tag())?;

        let mut resp = AdminResponse::default();
        resp.mut_change_peer().set_region(region.clone());

        RF_PEER_ADMIN_CMD_HISTOGRAM
            .change_peer
            .observe(duration_to_sec(star_time.saturating_elapsed()));
        Ok((
            resp,
            ApplyResult::Res(ExecResult::ChangePeer(ChangePeer {
                index: ctx.exec_log_index,
                conf_change: Default::default(),
                changes,
                region,
            })),
        ))
    }

    fn exec_change_peer_v2(
        &mut self,
        ctx: &mut ApplyContext,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult)> {
        assert!(request.has_change_peer_v2());
        let star_time = Instant::now();
        let changes = request.get_change_peer_v2().get_change_peers().to_vec();

        info!(
            "exec ConfChangeV2";
            "tag" => self.tag(),
            "peer_id" => self.id(),
            "kind" => ?ConfChangeKind::confchange_kind(changes.len()),
            "epoch" => ?self.region.get_region_epoch(),
        );

        let region =
            region_apply_conf_change(&self.region, changes.as_slice(), self.id(), self.tag())?;

        let mut resp = AdminResponse::default();
        resp.mut_change_peer().set_region(region.clone());
        RF_PEER_ADMIN_CMD_HISTOGRAM
            .change_peer_v2
            .observe(duration_to_sec(star_time.saturating_elapsed()));
        Ok((
            resp,
            ApplyResult::Res(ExecResult::ChangePeer(ChangePeer {
                index: ctx.exec_log_index,
                conf_change: Default::default(),
                changes,
                region,
            })),
        ))
    }

    fn handle_raft_entry_conf_change(
        &mut self,
        ctx: &mut ApplyContext,
        entry: &eraftpb::Entry,
    ) -> ApplyResult {
        // Although conf change can't yield in normal case, it is convenient to
        // simulate yield before applying a conf change log.
        // fail_point!("yield_apply_conf_change_3", self.id() == 3, |_| {
        // ApplyResult::Yield
        // });
        let index = entry.get_index();
        let (cmd, conf_change) = parse_conf_change_cmd(entry, self.tag());
        let (resp, result) = self.apply_raft_log(ctx, &cmd);
        self.handle_apply_result(ctx, resp, &result, true);
        match result {
            ApplyResult::None => {
                // If failed, tell Raft that the `ConfChange` was aborted.
                ApplyResult::Res(ExecResult::ChangePeer(Default::default()))
            }
            ApplyResult::Res(mut res) => {
                if let ExecResult::ChangePeer(ref mut cp) = res {
                    cp.conf_change = conf_change;
                } else {
                    panic!(
                        "{} unexpected result {:?} for conf change {:?} at {}",
                        self.tag(),
                        res,
                        conf_change,
                        index
                    );
                }
                ApplyResult::Res(res)
            }
        }
    }

    fn exec_split(
        &mut self,
        ctx: &mut ApplyContext,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult)> {
        let star_time = Instant::now();
        // Write the engine before run finish split, or we will get shard not match
        // error.
        let cs = self.pending_split.remove(&ctx.exec_log_index);
        if cs.is_none() {
            return Err(box_err!("split conflict with conf change"));
        }
        let cs = cs.unwrap();
        // Applier `encryption_key` should be updated after split to a whole keyspace
        // with encryption enabled.
        let encryption_key = if let Some(properties) = cs
            .get_split()
            .get_new_shards()
            .iter()
            .find(|s| s.get_shard_id() == self.region_id())
        {
            let master_key = ctx.engine.get_master_key();
            get_shard_property(ENCRYPTION_KEY, properties)
                .map(|v| master_key.decrypt_encryption_key(&v).unwrap())
        } else {
            None
        };
        let mut resp = AdminResponse::default();
        if let Err(err) = ctx.engine.split(cs, RAFT_INIT_LOG_INDEX) {
            // This must be a follower that fall behind, we need to pause the apply and wait
            // for split files to finish in the background worker.
            panic!(
                "region {} failed to execute split operation, error {:?}",
                self.tag(),
                err
            );
        }
        self.encryption_key = encryption_key;

        self.snap.take(); // snapshot is outdated.
        // clear the cache here or the locks doesn't belong to the new range would never
        // have chance to delete.
        self.lock_cache = HashMap::new();
        let mut splits = BatchSplitResponse::default();
        let regions =
            split_gen_new_region_metas(self.peer.store_id, &self.region, request.get_splits())
                .unwrap();
        splits.set_regions(RepeatedField::from(regions.clone()));
        resp.set_splits(splits);
        let result = ApplyResult::Res(ExecResult::SplitRegion { regions });
        RF_PEER_ADMIN_CMD_HISTOGRAM
            .batch_split
            .observe(duration_to_sec(star_time.saturating_elapsed()));
        Ok((resp, result))
    }

    fn exec_prepare_merge(
        &mut self,
        ctx: &mut ApplyContext,
    ) -> Result<(AdminResponse, ApplyResult)> {
        let star_time = Instant::now();
        ctx.engine.prepare_merge(
            self.region_id(),
            self.region.get_region_epoch().get_version(),
            ctx.exec_log_index,
        )?;
        let mut region = self.region.clone();
        let epoch = region.mut_region_epoch();
        epoch.version += 1;
        // Change conf version only when there are multiple peers, to meet the
        // requirement of merged engine which has no conf change.
        if self.region.get_peers().len() > 1 {
            epoch.conf_ver += 1;
        }
        RF_PEER_ADMIN_CMD_HISTOGRAM
            .prepare_merge
            .observe(duration_to_sec(star_time.saturating_elapsed()));
        Ok((
            AdminResponse::default(),
            ApplyResult::Res(ExecResult::PrepareMerge { region }),
        ))
    }

    fn exec_rollback_merge(
        &mut self,
        ctx: &mut ApplyContext,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult)> {
        fail_point!("on_follower_exec_rollback_merge", !self.is_leader(), |_| {
            unimplemented!()
        });
        let star_time = Instant::now();
        ctx.engine.rollback_merge(
            self.region_id(),
            self.region.get_region_epoch().version,
            ctx.exec_log_index,
        );
        let mut region = self.region.clone();
        let epoch = region.mut_region_epoch();
        epoch.version += 1;
        let commit = request.get_rollback_merge().commit;
        RF_PEER_ADMIN_CMD_HISTOGRAM
            .rollback_merge
            .observe(duration_to_sec(star_time.saturating_elapsed()));
        Ok((
            AdminResponse::default(),
            ApplyResult::Res(ExecResult::RollbackMerge { region, commit }),
        ))
    }

    fn exec_commit_merge(
        &mut self,
        ctx: &mut ApplyContext,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult)> {
        let star_time = Instant::now();
        let source_id = request.get_commit_merge().get_source().get_id();
        let source_tables = self
            .commit_merge_source_tables
            .remove(&source_id)
            .unwrap_or_else(|| panic!("{} source_id not exists {}", self.tag(), source_id));
        let region_ver = self.region.get_region_epoch().get_version();
        ctx.engine.commit_merge(
            self.region_id(),
            region_ver,
            &source_tables,
            ctx.exec_log_index,
        )?;
        let source = request.get_commit_merge().get_source().clone();
        let region = new_merged_region(&source, &self.region);
        RF_PEER_ADMIN_CMD_HISTOGRAM
            .commit_merge
            .observe(duration_to_sec(star_time.saturating_elapsed()));
        Ok((
            AdminResponse::default(),
            ApplyResult::Res(ExecResult::CommitMerge { region, source }),
        ))
    }

    /// Handles proposals, and appends the commands to the apply delegate.
    fn append_proposal(&mut self, props_drainer: Drain<'_, Proposal>) {
        let propose_num = props_drainer.len();
        if self.stopped {
            for p in props_drainer {
                notify_stale_req(p.term, p.cb, "stopped");
            }
            return;
        }
        for p in props_drainer {
            let cmd = PendingCmd::new(p.index, p.term, p.cb);
            if p.is_conf_change {
                if let Some(cmd) = self.pending_cmds.take_conf_change() {
                    // if it loses leadership before conf change is replicated, there may be
                    // a stale pending conf change before next conf change is applied. If it
                    // becomes leader again with the stale pending conf change, will enter
                    // this block, so we notify leadership may have been changed.
                    notify_stale_req(self.term, cmd.cb, "pending conf change");
                }
                self.pending_cmds.set_conf_change(cmd);
            } else {
                self.pending_cmds.append_normal(cmd);
            }
        }
        // TODO: observe it in batch.
        APPLY_PROPOSAL.observe(propose_num as f64);
    }

    /// Handles all the committed_entries, namely, applies the committed
    /// entries.
    fn handle_raft_committed_entries(
        &mut self,
        ctx: &mut ApplyContext,
        committed_entries_drainer: Drain<'_, eraftpb::Entry>,
    ) {
        if committed_entries_drainer.len() == 0 {
            return;
        }

        // If we send multiple ConfChange commands, only first one will be proposed
        // correctly, others will be saved as a normal entry with no data, so we
        // must re-propose these commands again.
        let mut results = VecDeque::<ExecResult>::new();
        for entry in committed_entries_drainer {
            if self.pending_remove {
                // This peer is about to be destroyed, skip everything.
                break;
            }
            let expected_index = self.apply_state.applied_index + 1;
            if expected_index != entry.get_index() {
                panic!(
                    "{} expect index {}, but got {}",
                    self.tag(),
                    expected_index,
                    entry.get_index()
                );
            }
            ctx.exec_log_index = entry.index;
            ctx.exec_log_term = entry.term;
            let result = match entry.get_entry_type() {
                eraftpb::EntryType::EntryNormal => self.handle_raft_entry_normal(ctx, &entry),
                eraftpb::EntryType::EntryConfChange | eraftpb::EntryType::EntryConfChangeV2 => {
                    self.handle_raft_entry_conf_change(ctx, &entry)
                }
            };
            match result {
                ApplyResult::None => {}
                ApplyResult::Res(res) => {
                    results.push_back(res);
                }
            }
        }
        ctx.finish_for(self, results);
    }

    fn on_role_changed(&mut self, new_role: StateRole) {
        self.role = new_role;
        self.shard_pending_active = Some((self.term, self.is_leader()));
    }

    fn set_shard_active(&self, ctx: &ApplyContext, active: bool) {
        debug_assert_eq!(self.is_leader(), active);
        let region_id = self.region.get_id();
        if active {
            if let Some((seq, shard_ver)) = self.last_scheduled_compaction() {
                let id_ver = kvengine::IdVer::new(region_id, shard_ver);
                ctx.engine.pause_compaction(id_ver, seq);
            }
        }
        ctx.engine.set_shard_active(region_id, active);
    }

    fn is_leader(&self) -> bool {
        self.role == StateRole::Leader
    }

    fn handle_apply(&mut self, ctx: &mut ApplyContext, apply: MsgApply) {
        if (apply.entries.is_empty() && apply.new_role.is_none())
            || self.pending_remove
            || self.stopped
        {
            return;
        }

        if self.paused_apply_queue.must_not_paused() {
            self.process_apply_msg(ctx, apply);
        } else if let Some(split_apply) = self.paused_apply_queue.push(apply) {
            self.process_apply_msg(ctx, split_apply);
        }
    }

    fn process_apply_msg(&mut self, ctx: &mut ApplyContext, mut apply: MsgApply) {
        self.metrics = ApplyMetrics::default();
        if let Some(meta) = apply.bucket_meta.clone() {
            let bucket_stats = new_bucket_write_stats(&meta);
            self.buckets = Some(BucketStat::new(meta, bucket_stats));
        }
        self.term = apply.term;
        self.append_proposal(apply.cbs.drain(..));
        self.handle_raft_committed_entries(ctx, apply.entries.drain(..));
        self.snap.take();
        if let Some(state) = apply.new_role {
            self.on_role_changed(state);
        }
        if let Some((pending_term, is_active)) = self.shard_pending_active {
            if self.apply_state.applied_index_term >= pending_term {
                self.set_shard_active(ctx, is_active);
                self.shard_pending_active = None;
            }
        }
        if self.pending_remove {
            self.destroy();
        }
    }

    fn resume_handle_apply(&mut self, ctx: &mut ApplyContext) {
        while let Some(apply) = self.paused_apply_queue.take_next() {
            self.process_apply_msg(ctx, apply);
        }
    }

    fn handle_apply_change_set(&mut self, ctx: &mut ApplyContext, cs: ChangeSet) {
        let change_set_tp = ChangeSetType::from_change_set(&cs.change_set);
        if !self
            .scheduled_change_sets
            .iter()
            .any(|&(seq, cs_tp)| seq == cs.sequence && cs_tp == change_set_tp)
        {
            info!(
                "{} discard outdated change set {:?}",
                self.tag(),
                &cs.change_set
            );
            return;
        }
        self.prepared_change_sets.insert(cs.sequence, cs);
        self.apply_prepared_change_set(ctx);
    }

    fn apply_prepared_change_set(&mut self, ctx: &mut ApplyContext) {
        let mut exec_results = VecDeque::<ExecResult>::new();
        while let Some(cs) = self.take_prepared_change_set() {
            let seq = cs.sequence;
            let label = change_set_label(&cs.change_set);
            let cs_pb = cs.change_set.clone();
            let result = if cs.has_snapshot() {
                // The lock may already committed in the snapshot.
                // clear_caches to avoid memory leak.
                self.clear_caches();
                self.apply_state = RaftApplyState::from_snapshot(cs.get_snapshot());
                ctx.engine.ingest(cs, false)
            } else {
                ctx.engine.apply_change_set(&cs)
            };

            match result {
                Ok(()) => self.handle_apply_change_set_result(ctx, cs_pb, &mut exec_results),
                Err(err) => warn!(
                    "{} failed to apply change set to kvengine, err {:?}, change set {:?}",
                    self.tag(),
                    err,
                    cs_pb
                ),
            }

            if self
                .paused_apply_queue
                .try_unpause(seq, &format!("{} {}", self.tag(), label))
            {
                if !exec_results.is_empty() {
                    ctx.finish_for(self, mem::take(&mut exec_results));
                }
                self.resume_handle_apply(ctx);
            }
        }
        if !exec_results.is_empty() {
            ctx.finish_for(self, exec_results);
        }
    }

    fn handle_apply_change_set_result(
        &mut self,
        ctx: &mut ApplyContext,
        cs: kvenginepb::ChangeSet,
        exec_results: &mut VecDeque<ExecResult>,
    ) {
        if cs.has_snapshot() {
            if !exec_results.is_empty() {
                ctx.finish_for(self, std::mem::take(exec_results));
            }

            let router = ctx.router.as_ref().unwrap();
            router.send(self.region_id(), PeerMsg::ApplySnapshotResult(cs));
        } else if cs.has_restore_shard() {
            debug_assert_eq!(self.region.get_region_epoch().get_version(), cs.shard_ver);
            // Applier encryption_key may be updated after branching with encryption
            // enabled.
            let snap = cs.get_restore_shard();
            let master_key = ctx.engine.get_master_key();
            let encryption_key = get_shard_property(ENCRYPTION_KEY, snap.get_properties())
                .map(|v| master_key.decrypt_encryption_key(&v).unwrap());
            self.encryption_key = encryption_key;

            self.region.mut_region_epoch().set_version(cs.shard_ver + 1);
            exec_results.push_back(ExecResult::RestoreShard { cs });
        }
    }

    fn take_prepared_change_set(&mut self) -> Option<ChangeSet> {
        if let Some(&(sequence, _)) = self.scheduled_change_sets.front() {
            if let Some(paused_seq) = self.paused_apply_queue.paused_sequence() {
                if sequence > paused_seq {
                    debug!(
                        "{} prepared change set is hold by paused, sequence {}, paused_seq {}",
                        self.tag(),
                        sequence,
                        paused_seq,
                    );
                    return None;
                }
            }

            if let Some(cs) = self.prepared_change_sets.remove(&sequence) {
                self.scheduled_change_sets.pop_front();
                return Some(cs);
            }
        }
        None
    }

    fn clear_all_commands_as_stale(&mut self) {
        for mut apply in self.paused_apply_queue.drain(..) {
            for proposal in apply.cbs.drain(..) {
                notify_stale_req(self.term, proposal.cb, "reregistration");
            }
        }
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_stale_req(self.term, cmd.cb, "reregistration");
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_stale_req(self.term, cmd.cb, "reregistration");
        }
    }

    /// Handles peer registration. When a peer is created, it will register an
    /// apply delegate.
    fn handle_registration(&mut self, reg: MsgRegistration) {
        info!(
            "re-register to applier";
            "tag" => self.tag(),
            "peer_id" => self.get_peer().get_id(),
            "term" => reg.term,
            "reg_region_id" => reg.region.get_id(),
        );
        assert_eq!(self.get_peer().get_id(), reg.peer.id);
        assert!(self.apply_state.applied_index <= reg.apply_state.applied_index);
        self.term = reg.term;
        self.clear_all_commands_as_stale();
        *self = Applier::new_from_reg(reg);
    }

    pub(crate) fn destroy(&mut self) {
        let peer_id = self.get_peer().get_id();
        fail_point!("before_peer_destroy_1003", peer_id == 1003, |_| {});
        info!(
            "remove applier";
            "tag" => self.tag(),
            "peer_id" => peer_id,
        );
        self.stopped = true;
        self.lock_cache = HashMap::new();
        for mut apply in self.paused_apply_queue.drain(..) {
            for proposal in apply.cbs.drain(..) {
                notify_req_region_removed(self.region.get_id(), proposal.cb);
            }
        }
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_req_region_removed(self.region.get_id(), cmd.cb);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_req_region_removed(self.region.get_id(), cmd.cb);
        }
    }

    fn handle_unsafe_destroy(&mut self, ctx: &mut ApplyContext, region_id: u64) {
        assert_eq!(region_id, self.region.get_id());
        if !self.stopped {
            let mut results = VecDeque::new();
            results.push_back(ExecResult::UnsafeDestroy);
            self.destroy();
            ctx.finish_for(self, results);
        }
    }

    fn handle_prepare_change_set(
        &mut self,
        ctx: &mut ApplyContext,
        cs: kvenginepb::ChangeSet,
        encryption_key: Option<EncryptionKey>,
        reload_snap: Option<kvenginepb::Snapshot>, /* The snap contains the current files that
                                                    * need to be reloaded. */
        prepare_type: FilePrepareType,
    ) {
        if cs.has_ingest_files() {
            self.paused_apply_queue
                .pause(cs.sequence, &format!("{} ingest", self.tag()));
        } else if cs.has_restore_shard() {
            self.handle_prepare_restore_shard(&cs);
        }
        let cs_tp = ChangeSetType::from_change_set(&cs);
        self.scheduled_change_sets.push_back((cs.sequence, cs_tp));
        let engine = ctx.engine.clone();
        let router = ctx.router.clone().unwrap();
        let is_leader = self.is_leader();
        let peer_id = self.id();
        std::thread::spawn(move || {
            let id = cs.shard_id;
            tikv_util::set_current_region(id);
            let res = engine.prepare_change_set(
                cs,
                PrepareOpts {
                    use_direct_io: !is_leader,
                    prepare_type,
                    reload_snap,
                    encryption_key,
                    ..Default::default()
                },
            );
            router.send(id, PeerMsg::PrepareChangeSetResult(res, peer_id));
        });
    }

    fn mut_mem_table_state(&mut self, engine: &Engine) -> &mut MemTableState {
        let region_id = self.region_id();
        self.mem_table_state.get_or_insert_with(|| {
            let shard = engine.get_shard(region_id).unwrap();
            let max_mem_table_size = engine.opts.max_mem_table_size;
            let (size, unpersisted_props_size) = shard.get_writable_mem_table_state();
            MemTableState::new(size, unpersisted_props_size, max_mem_table_size)
        })
    }

    fn maybe_propose_switch_mem_table(&mut self, ctx: &mut ApplyContext, now: Instant) {
        if !self.is_leader() {
            return;
        }
        let tag = self.tag();
        let mem_state = self.mut_mem_table_state(&ctx.engine);
        if mem_state.need_switch(&tag, now) {
            let mut custom_builder = CustomBuilder::new();
            // Only set `mem_table_size` for backward compatibility.
            custom_builder.set_switch_mem_table(mem_state.mem_table_size);
            let mut req = self.new_raft_cmd_request();
            req.set_custom_request(custom_builder.build());
            ctx.router
                .as_ref()
                .unwrap()
                .send_command(req, Callback::None);
            self.mut_mem_table_state(&ctx.engine).proposed_time = Some(now);

            STORE_PROPOSE_SWITCH_MEM_TABLE_COUNTER.inc();
        }
    }

    fn handle_maintenance(&mut self, ctx: &mut ApplyContext, region_id: u64) {
        assert_eq!(self.region_id(), region_id);
        self.lock_cache.shrink_to_fit();
        self.maybe_propose_switch_mem_table(ctx, Instant::now());
    }

    fn new_raft_cmd_request(&self) -> RaftCmdRequest {
        let mut req = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(self.region_id());
        header.set_peer(self.peer.clone());
        header.set_region_epoch(self.region.get_region_epoch().clone());
        header.set_term(self.term);
        req.set_header(header);
        req
    }

    fn handle_prepare_commit_merge(
        &mut self,
        ctx: &mut ApplyContext,
        source: kvenginepb::ChangeSet,
        commit_index: u64,
    ) {
        self.maybe_pause_for_merge();
        let is_leader = self.is_leader();
        let engine = ctx.engine.clone();

        self.paused_apply_queue
            .pause(commit_index, &format!("{} commit merge", self.tag()));

        let region_id = self.region_id();
        let router = ctx.router.as_ref().unwrap().clone();
        let encryption_key = self.encryption_key.clone();
        std::thread::spawn(move || {
            tikv_util::set_current_region(source.shard_id);
            let res = engine.prepare_change_set(
                source,
                PrepareOpts {
                    use_direct_io: !is_leader,
                    prepare_type: FilePrepareType::Local, // reset by snapshot
                    encryption_key,
                    ..Default::default()
                },
            );
            router.send(
                region_id,
                PeerMsg::PrepareCommitMergeResult(res, commit_index),
            );
        });
    }

    // When region split, the leader must have already initial flushed, but the
    // follower may not. To avoid the follower diverge too much from the leader,
    // we need to make sure the follower already initial flushed before apply
    // split.
    // TODO: Confirm that destroy range can be skipped or not.
    fn maybe_pause_for_split(&mut self) {
        for &(seq, cs_tp) in &self.scheduled_change_sets {
            if cs_tp.should_pause_for_split() {
                self.paused_apply_queue
                    .pause(seq, &format!("{} {:?}", self.tag(), cs_tp));
            }
        }
        self.paused_apply_queue.sort_and_dedup_paused_seqs();
    }

    // For prepare & commit merge, we must wait for all change sets as any of them
    // may remove over bound data.
    fn maybe_pause_for_merge(&mut self) {
        if let Some(&(seq, cs_tp)) = &self.scheduled_change_sets.back() {
            self.paused_apply_queue
                .pause(seq, &format!("{} {:?}", self.tag(), cs_tp));
            self.paused_apply_queue.sort_and_dedup_paused_seqs();
        }
    }

    fn last_scheduled_compaction(&self) -> Option<(u64 /* seq */, u64 /* shard_ver */)> {
        self.scheduled_change_sets
            .iter()
            .rev()
            .find_map(|&(seq, cs_tp)| match cs_tp {
                ChangeSetType::Compaction { shard_ver } => Some((seq, shard_ver)),
                _ => None,
            })
    }

    fn handle_resume_commit_merge(
        &mut self,
        ctx: &mut ApplyContext,
        source: ChangeSet,
        commit_index: u64,
    ) {
        // Resume commit merge would be stale after restore snapshot.
        if self.apply_state.applied_index >= commit_index {
            info!(
                "{} ignore stale resume commit merge, applied_index {}, commit_index {}",
                self.tag(),
                self.apply_state.applied_index,
                commit_index
            );
            return;
        }

        self.commit_merge_source_tables
            .insert(source.shard_id, source);

        if !self
            .paused_apply_queue
            .pick_unpause(commit_index, &format!("{} commit merge", self.tag()))
        {
            panic!(
                "{} commit merge unpause seq {} not found",
                self.tag(),
                commit_index
            )
        }
        self.resume_handle_apply(ctx);
        self.apply_prepared_change_set(ctx);
    }

    fn handle_prepare_rollback_merge(&mut self, ctx: &mut ApplyContext, initial_flush_seq: u64) {
        let shard = ctx.engine.get_shard(self.region_id()).unwrap();
        if shard.get_meta_sequence() < initial_flush_seq {
            // Wait for initial flush before apply rollback merge.
            self.paused_apply_queue
                .pause(initial_flush_seq, &format!("{} rollback merge", self.tag()));
        }
    }

    fn handle_prepare_txn_file(
        &mut self,
        ctx: &mut ApplyContext,
        txn_file_ref: TxnFileRef,
        entry_index: u64,
        encryption_key: Option<EncryptionKey>,
    ) {
        let tag = self.tag();
        self.paused_apply_queue
            .pause(entry_index, &format!("{} txn file", tag));
        let txn_chunk_manager = ctx.engine.get_txn_chunk_manager();
        let router = ctx.router.clone().unwrap();
        let region_id = self.region_id();
        let peer_id = self.id();
        let worker_pool = txn_chunk_manager.worker_pool().clone();
        let start = Instant::now();
        worker_pool.spawn_blocking(move || {
            tikv_util::set_current_region(region_id);
            PREPARE_TASK_WAIT_TIME_HISTOGRAM
                .with_label_values(&["txn"])
                .observe(duration_to_sec(start.saturating_elapsed()));
            if let Err(err) =
                txn_chunk_manager.prepare_txn_chunks(txn_file_ref.chunk_ids.clone(), encryption_key)
            {
                // We can't handle the error here, just panic.
                panic!(
                    "{} failed to prepare txn chunks, err {:?}, txn_file_ref {:?}, entry_index {}",
                    tag, err, txn_file_ref, entry_index
                );
            }
            router.send(
                region_id,
                PeerMsg::PrepareTxnFileResult {
                    entry_index,
                    peer_id,
                },
            );
        });
    }

    fn handle_resume_txn_file(&mut self, ctx: &mut ApplyContext, commit_index: u64) {
        // Resume txn file would be stale after restore snapshot.
        if self.apply_state.applied_index >= commit_index {
            info!(
                "{} ignore stale resume txn file, applied_index {}, commit_index {}",
                self.tag(),
                self.apply_state.applied_index,
                commit_index
            );
            return;
        }

        if !self
            .paused_apply_queue
            .pick_unpause(commit_index, &format!("{} txn file", self.tag()))
        {
            panic!(
                "{} txn file unpause seq {} not found",
                self.tag(),
                commit_index
            )
        }
        self.resume_handle_apply(ctx);
        self.apply_prepared_change_set(ctx);
    }

    pub(crate) fn handle_msg(&mut self, ctx: &mut ApplyContext, msg: ApplyMsg) {
        if self.stopped {
            info!("{} skip apply msg {:?}", self.tag(), msg);
            return;
        }

        match msg {
            ApplyMsg::Apply(apply) => {
                self.handle_apply(ctx, apply);
            }
            ApplyMsg::Registration(reg) => {
                self.handle_registration(reg);
            }
            ApplyMsg::UnsafeDestroy { region_id } => {
                self.handle_unsafe_destroy(ctx, region_id);
            }
            ApplyMsg::PendingSplit(pending_split) => {
                self.pending_split
                    .insert(pending_split.sequence, pending_split);
                self.maybe_pause_for_split();
            }
            ApplyMsg::PrepareChangeSet {
                cs,
                encryption_key,
                reload_snap,
                prepare_type,
            } => {
                self.handle_prepare_change_set(ctx, cs, encryption_key, reload_snap, prepare_type);
            }
            ApplyMsg::ApplyChangeSet(cs) => {
                self.handle_apply_change_set(ctx, cs);
            }
            ApplyMsg::Maintenance { region_id } => {
                self.handle_maintenance(ctx, region_id);
            }
            ApplyMsg::PrepareMerge => {
                self.maybe_pause_for_merge();
            }
            ApplyMsg::PrepareCommitMerge {
                source,
                commit_index,
            } => {
                self.handle_prepare_commit_merge(ctx, source, commit_index);
            }
            ApplyMsg::ResumeCommitMerge {
                source,
                commit_index,
            } => {
                self.handle_resume_commit_merge(ctx, source, commit_index);
            }
            ApplyMsg::PrepareRollbackMerge(initial_flush_seq) => {
                self.handle_prepare_rollback_merge(ctx, initial_flush_seq);
            }
            ApplyMsg::PrepareTxnFile {
                txn_file_ref,
                commit_index,
                encryption_key,
            } => {
                self.handle_prepare_txn_file(ctx, txn_file_ref, commit_index, encryption_key);
            }
            ApplyMsg::ResumeTxnFile(commit_index) => {
                self.handle_resume_txn_file(ctx, commit_index);
            }
            ApplyMsg::TriggerRefreshShardStates => {
                self.trigger_refresh_shard_states(ctx);
            }
        }
        self.update_lock_cache_metrics();
    }

    fn trigger_trim_over_bound(
        &self,
        parameter: TrimOverBoundParameter,
        wb: &mut kvengine::WriteBatch,
        engine: &Engine,
        router: Option<&RaftRouter>,
    ) {
        info!(
            "{} apply trigger_trim_over_bound: {:?}",
            self.tag(),
            parameter
        );

        let shard_id = self.region.get_id();
        if parameter.is_for_shard(shard_id) {
            let shard = engine.get_shard(shard_id).unwrap();
            if !shard.get_trim_over_bound() && shard.has_over_bound_data() {
                wb.set_property(TRIM_OVER_BOUND, TRIM_OVER_BOUND_ENABLE);
            }
        }

        match (parameter.target_shard, router) {
            (Some(target), Some(router)) if target.id != shard_id => {
                if let Some(target_shard) = engine.get_shard(target.id) {
                    if !target_shard.get_trim_over_bound() && target_shard.has_over_bound_data() {
                        info!(
                            "{} send trigger_trim_over_bound to {:?}",
                            self.tag(),
                            target
                        );
                        router.send(
                            target.id,
                            PeerMsg::CasualMessage(CasualMessage::TriggerTrimOverBound(parameter)),
                        );
                    }
                };
            }
            _ => {}
        }
    }

    fn trigger_refresh_shard_states(&self, ctx: &mut ApplyContext) {
        info!("{} apply trigger_refresh_shard_states", self.tag());
        let engine = &ctx.engine;
        let Some(shard) = engine.get_shard(self.region_id()) else {
            return;
        };
        engine.refresh_shard_states(&shard);
    }

    /// Sequence of applying:
    ///
    /// 1. Preprocess restore shard
    /// 2. Prepare restore shard, pause applying of custom logs (until seq of
    ///    restore shard)
    /// 3. Apply custom logs before seq of restore shard
    /// 4. Apply restore shard, resume applying of custom logs
    /// 5. Apply custom logs after seq of restore shard
    ///
    /// Without the pause, `apply_restore_shard` would clear the mem-table on
    /// different apply index with other peer, and lead to data inconsistency.
    fn handle_prepare_restore_shard(&mut self, cs: &kvenginepb::ChangeSet) {
        // Invalidate caches, otherwise they would be inconsistent with restored data.
        self.clear_caches();

        // Will resume after `apply_restore_shard`. See `apply_prepared_change_set`.
        self.paused_apply_queue
            .pause(cs.sequence, &format!("{} restore_shard", self.tag()));
    }
}

pub(crate) fn is_property_change_set(cs: &kvenginepb::ChangeSet) -> bool {
    !cs.get_property_key().is_empty()
        && !cs.has_destroy_range()
        && !cs.has_truncate_ts()
        && !cs.has_trim_over_bound()
        && !cs.has_update_schema_meta()
}

// Used in recover. These change sets have side effect of switch mem-table.
pub(crate) fn is_change_set_affect_mem_table(cs: &kvenginepb::ChangeSet) -> bool {
    is_property_change_set(cs)
}

struct MemTableState {
    mem_table_size: u64,
    unpersisted_props_size: usize,
    max_mem_table_size: u64,
    init_time: Instant,
    last_switch_time: Option<Instant>,
    proposed_time: Option<Instant>,
}

const BYTES_MB: u64 = 1024 * 1024;

// 128MB mem-table flush at 10 seconds.
// 32MB mem-table flush at 40 seconds.
// 4MB mem-table flush at 320 seconds.
// 1MB mem-table flush at 1280 seconds.
// 256KB mem-table flush at 5120 seconds.
// 64KB mem-table flush at 20480 seconds.
const STANDARD_MEMORY_SIZE_DURATION: u64 = 128 * BYTES_MB * 10;

const MAX_SWITCH_SECONDS: u64 = 24 * 60 * 60;
const MAX_JITTER_SECONDS: u64 = 4096;
const PROPOSE_SWITCH_TIMEOUT: Duration = Duration::from_secs(10);

impl MemTableState {
    fn new(mem_table_size: u64, unpersisted_props_size: usize, max_mem_table_size: u64) -> Self {
        Self {
            mem_table_size,
            unpersisted_props_size,
            max_mem_table_size,
            init_time: Instant::now(),
            last_switch_time: None,
            proposed_time: None,
        }
    }

    fn update(&mut self, mem_table_size: u64, unpersisted_props_size: usize) {
        self.mem_table_size = mem_table_size;
        self.unpersisted_props_size = unpersisted_props_size;
    }

    fn need_switch(&self, tag: &PeerTag, now: Instant) -> bool {
        let total_size = self.mem_table_size + self.unpersisted_props_size as u64;
        if total_size == 0 {
            return false;
        }
        if let Some(proposed_time) = self.proposed_time {
            if now.saturating_duration_since(proposed_time) < PROPOSE_SWITCH_TIMEOUT {
                return false;
            }
            // The proposal maybe failed for some reason, propose again.
            warn!("{} propose switch mem-table expired, propose again", tag);
        }
        if cfg!(debug_assertions) && self.max_mem_table_size < BYTES_MB {
            // running in test mode, use probability algorithm to switch for better
            // coverage.
            let size_ratio = total_size as f64 / self.max_mem_table_size as f64;
            return size_ratio > 0.75 && rand::thread_rng().gen_bool(0.1);
        }
        // Avoid too many mem-tables flush at the same time.
        let jitter_seconds = total_size % MAX_JITTER_SECONDS;
        // We don't need to propose on hard limit, it is handled by the engine.
        let duration_secs_to_switch = min(
            STANDARD_MEMORY_SIZE_DURATION / total_size,
            MAX_SWITCH_SECONDS + jitter_seconds,
        );
        self.get_duration_since_last_switch(now).as_secs() > duration_secs_to_switch
    }

    fn get_duration_since_last_switch(&self, now: Instant) -> Duration {
        self.last_switch_time
            .map(|time| now.saturating_duration_since(time))
            .unwrap_or_else(|| now.saturating_duration_since(self.init_time))
    }

    fn set_switch_time(&mut self, t: Instant) {
        self.last_switch_time = Some(t);
        self.proposed_time = None;
    }
}

pub fn get_peer_idx_by_store_id(region: &metapb::Region, store_id: u64) -> usize {
    let mut peer_idx = region.peers.len();
    for (i, peer) in region.peers.iter().enumerate() {
        if peer.store_id == store_id {
            peer_idx = i;
            break;
        }
    }
    peer_idx
}

pub fn get_peer_id_by_store_id(region: &metapb::Region, store_id: u64) -> Option<u64> {
    region
        .peers
        .iter()
        .find(|x| x.store_id == store_id)
        .map(|x| x.id)
}

pub fn get_peer_idx_by_peer_id(region: &metapb::Region, peer_id: u64) -> usize {
    let mut peer_idx = region.peers.len();
    for (i, peer) in region.peers.iter().enumerate() {
        if peer.get_id() == peer_id {
            peer_idx = i;
            break;
        }
    }
    peer_idx
}

pub fn is_conf_change_cmd(msg: &RaftCmdRequest) -> bool {
    if !msg.has_admin_request() {
        return false;
    }
    let req = msg.get_admin_request();
    req.has_change_peer() || req.has_change_peer_v2()
}

pub(crate) fn split_gen_new_region_metas(
    store_id: u64,
    old_region: &metapb::Region,
    splits: &BatchSplitRequest,
) -> Result<Vec<metapb::Region>> {
    let requests = splits.get_requests();
    if requests.is_empty() {
        return Err(box_err!("missing split key"));
    }
    let new_region_cnt = requests.len();
    let tag = PeerTag::new(store_id, RegionIdVer::from_region(old_region));

    let mut keys = Vec::with_capacity(new_region_cnt + 1);
    keys.push(old_region.start_key.clone());
    for request in requests {
        let split_key = &request.split_key;
        if split_key.is_empty() {
            return Err(box_err!("missing split key"));
        }
        if split_key <= keys.last().unwrap() {
            return Err(box_err!(
                "invalid split requests {:?}, old region {} start_key {:?}, end_key {:?}",
                splits,
                tag,
                old_region.start_key.clone(),
                old_region.end_key.clone()
            ));
        }
        if request.new_peer_ids.len() != old_region.peers.len() {
            return Err(box_err!(
                "invalid new id peer count need {} but got {}",
                old_region.peers.len(),
                request.new_peer_ids.len()
            ));
        }
        keys.push(split_key.clone());
    }
    let end_key = &old_region.end_key;
    if !end_key.is_empty() && end_key <= keys.last().unwrap() {
        return Err(box_err!("invalid split requests {:?}", splits));
    }
    keys.push(end_key.clone());

    let mut derived = old_region.clone();
    let mut new_regions = Vec::with_capacity(new_region_cnt + 1);
    let old_version = old_region.get_region_epoch().get_version();
    info!("split region"; "region" => tag);
    derived
        .mut_region_epoch()
        .set_version(old_version + new_region_cnt as u64);

    // Note that the split requests only contain ids for new regions, so we need
    // to handle new regions and old region separately.
    for (i, req) in requests.iter().enumerate() {
        let mut new_region = metapb::Region::new();
        new_region.set_id(req.new_region_id);
        new_region.set_region_epoch(derived.get_region_epoch().clone());
        new_region.set_start_key(keys[i].clone());
        new_region.set_end_key(keys[i + 1].clone());
        for j in 0..derived.peers.len() {
            let mut new_peer = metapb::Peer::new();
            new_peer.set_id(req.new_peer_ids[j]);
            new_peer.set_store_id(derived.peers[j].get_store_id());
            new_peer.set_role(derived.peers[j].get_role());
            new_region.mut_peers().push(new_peer);
        }
        new_regions.push(new_region);
    }
    derived.start_key = keys[keys.len() - 2].clone();
    new_regions.push(derived);
    Ok(new_regions)
}

pub(crate) fn build_split_pb(
    region_id: u64,
    new_regions: &Vec<metapb::Region>,
    term: u64,
    header: &RaftRequestHeader,
    parent_encryption_key: Option<Bytes>,
    properties_helper: &PropertiesHelper,
) -> kvenginepb::Split {
    let mut split = kvenginepb::Split::new();
    let mut keyspace_encryption_keys = HashMap::new();
    let flag_data = header.get_flag_data();
    if !flag_data.is_empty() {
        keyspace_encryption_keys = decode_split_flag_encryption_keys(flag_data);
    }
    for new_region in new_regions {
        let raw_start = raw_start_key(new_region);
        let raw_end = raw_end_key(new_region);

        let mut props = kvenginepb::Properties::new();
        props.set_shard_id(new_region.get_id());
        props.mut_keys().push(TERM_KEY.to_string());
        if new_region.get_id() == region_id {
            props.mut_values().push(term.to_le_bytes().to_vec());
        } else {
            props
                .mut_values()
                .push(RAFT_INIT_LOG_TERM.to_le_bytes().to_vec());
        }
        if let Some(encryption_key) = parent_encryption_key.as_ref() {
            props.mut_keys().push(ENCRYPTION_KEY.to_string());
            props.mut_values().push(encryption_key.to_vec());
        } else if is_whole_keyspace_range(&raw_start, &raw_end)
            && !keyspace_encryption_keys.is_empty()
        {
            let keyspace_id = ApiV2::get_u32_keyspace_id_by_key(&raw_start).unwrap_or_default();
            if let Some(encryption_key) = keyspace_encryption_keys.remove(&keyspace_id) {
                props.mut_keys().push(ENCRYPTION_KEY.to_string());
                props.mut_values().push(encryption_key);
            }
        }
        properties_helper.split_to_properties(&raw_start, &raw_end, &mut props);

        split.mut_new_shards().push(props);
    }
    for new_region in &new_regions[1..] {
        split.mut_keys().push(raw_start_key(new_region));
    }
    split
}

pub(crate) fn parse_conf_change_cmd(
    entry: &eraftpb::Entry,
    tag: PeerTag,
) -> (RaftCmdRequest, ConfChangeV2) {
    let (index, _) = (entry.get_index(), entry.get_term());
    let conf_change: ConfChangeV2 = match entry.get_entry_type() {
        EntryType::EntryConfChange => {
            let conf_change: ConfChange = parse_data_at(entry.get_data(), index, tag);
            use raft_proto::ConfChangeI;
            conf_change.into_v2()
        }
        EntryType::EntryConfChangeV2 => parse_data_at(entry.get_data(), index, tag),
        _ => unreachable!(),
    };
    let cmd = parse_data_at(conf_change.get_context(), index, tag);
    (cmd, conf_change)
}

pub(crate) fn region_apply_conf_change(
    old_region: &Region,
    changes: &[ChangePeerRequest],
    peer_id: u64,
    tag: PeerTag,
) -> Result<Region> {
    let kind = ConfChangeKind::confchange_kind(changes.len());
    let mut region = old_region.clone();
    if kind == ConfChangeKind::LeaveJoint {
        let mut change_num = 0;
        for peer in region.mut_peers().iter_mut() {
            match peer.get_role() {
                PeerRole::IncomingVoter => peer.set_role(PeerRole::Voter),
                PeerRole::DemotingVoter => peer.set_role(PeerRole::Learner),
                _ => continue,
            }
            change_num += 1;
        }
        if change_num == 0 {
            panic!(
                "{} can't leave a non-joint config, region: {:?}",
                tag, old_region
            );
        }
        let conf_ver = region.get_region_epoch().get_conf_ver() + change_num;
        region.mut_region_epoch().set_conf_ver(conf_ver);
        return Ok(region);
    }
    for cp in changes.iter() {
        let (change_type, peer) = (cp.get_change_type(), cp.get_peer());
        let store_id = peer.get_store_id();

        if let Some(exist_peer) = find_peer(&region, store_id) {
            let r = exist_peer.get_role();
            if r == PeerRole::IncomingVoter || r == PeerRole::DemotingVoter {
                panic!(
                    "{} can't apply confchange because configuration is still in joint state, confchange: {:?}, region: {:?}",
                    tag, cp, old_region
                );
            }
        }
        match (find_peer_mut(&mut region, store_id), change_type) {
            (None, ConfChangeType::AddNode) => {
                let mut peer = peer.clone();
                match kind {
                    ConfChangeKind::Simple => peer.set_role(PeerRole::Voter),
                    ConfChangeKind::EnterJoint => peer.set_role(PeerRole::IncomingVoter),
                    _ => unreachable!(),
                }
                region.mut_peers().push(peer);
            }
            (None, ConfChangeType::AddLearnerNode) => {
                let mut peer = peer.clone();
                peer.set_role(PeerRole::Learner);
                region.mut_peers().push(peer);
            }
            (None, ConfChangeType::RemoveNode) => {
                error!(
                    "remove missing peer";
                    "tag" => tag,
                    "peer_id" => peer_id,
                    "peer" => ?peer,
                    "region" => ?old_region,
                );
                return Err(box_err!(
                    "remove missing peer {:?} from region {:?}",
                    peer,
                    old_region
                ));
            }
            // Add node
            (Some(exist_peer), ConfChangeType::AddNode)
            | (Some(exist_peer), ConfChangeType::AddLearnerNode) => {
                let (role, exist_id, incoming_id) =
                    (exist_peer.get_role(), exist_peer.get_id(), peer.get_id());

                if exist_id != incoming_id // Add peer with different id to the same store
                    // The peer is already the requested role
                    || (role, change_type) == (PeerRole::Voter, ConfChangeType::AddNode)
                    || (role, change_type) == (PeerRole::Learner, ConfChangeType::AddLearnerNode)
                {
                    error!(
                        "can't add duplicated peer";
                        "tag" => tag,
                        "peer_id" => peer_id,
                        "peer" => ?peer,
                        "exist peer" => ?exist_peer,
                        "confchnage type" => ?change_type,
                        "region" => ?old_region
                    );
                    return Err(box_err!(
                        "can't add duplicated peer {:?} to region {:?}, duplicated with exist peer {:?}",
                        peer,
                        old_region,
                        exist_peer
                    ));
                }
                match (role, change_type) {
                    (PeerRole::Voter, ConfChangeType::AddLearnerNode) => match kind {
                        ConfChangeKind::Simple => exist_peer.set_role(PeerRole::Learner),
                        ConfChangeKind::EnterJoint => exist_peer.set_role(PeerRole::DemotingVoter),
                        _ => unreachable!(),
                    },
                    (PeerRole::Learner, ConfChangeType::AddNode) => match kind {
                        ConfChangeKind::Simple => exist_peer.set_role(PeerRole::Voter),
                        ConfChangeKind::EnterJoint => exist_peer.set_role(PeerRole::IncomingVoter),
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                }
            }
            // Remove node
            (Some(exist_peer), ConfChangeType::RemoveNode) => {
                if kind == ConfChangeKind::EnterJoint && exist_peer.get_role() == PeerRole::Voter {
                    error!(
                        "can't remove voter directly";
                        "tag" => tag,
                        "peer_id" => peer_id,
                        "peer" => ?peer,
                        "region" => ?old_region
                    );
                    return Err(box_err!(
                        "can not remove voter {:?} directly from region {:?}",
                        peer,
                        old_region
                    ));
                }
                match remove_peer(&mut region, store_id) {
                    Some(p) => {
                        if &p != peer {
                            error!(
                                "ignore remove unmatched peer";
                                "tag" => tag,
                                "peer_id" => peer_id,
                                "expect_peer" => ?peer,
                                "get_peeer" => ?p
                            );
                            return Err(box_err!(
                                "remove unmatched peer: expect: {:?}, get {:?}, ignore",
                                peer,
                                p
                            ));
                        }
                    }
                    None => unreachable!(),
                }
            }
        }
        // confchange_cmd_metric::inc_success(change_type);
    }
    let conf_ver = region.get_region_epoch().get_conf_ver() + changes.len() as u64;
    region.mut_region_epoch().set_conf_ver(conf_ver);
    Ok(region)
}

pub use kvengine::shard::TERM_KEY;

pub trait ApplyObserver: Send {
    fn on_apply(&mut self, region_id: u64, region_version: u64, log_index: u64, wb: &WriteBatch);

    fn on_apply_admin(
        &mut self,
        region_id: u64,
        region_version: u64,
        log_index: u64,
        admin: &AdminRequest,
    );

    fn flush(&mut self);

    fn flush_region(&mut self, region_id: u64);
}

pub struct ApplyContext {
    pub(crate) engine: kvengine::Engine,
    pub(crate) router: Option<RaftRouter>, // None in recover mode.
    pub(crate) exec_log_index: u64,
    pub(crate) exec_log_term: u64,
    // NOTE: `wb` must be `reset` before use.
    // Use `RefCell` to work around the borrow check.
    wb: RefCell<WriteBatch>,
    pub(crate) apply_wait: LocalHistogram,
    pub(crate) apply_time: LocalHistogram,
    pub(crate) observer: Option<Box<dyn ApplyObserver>>,
}

impl ApplyContext {
    pub fn new(engine: kvengine::Engine, router: Option<RaftRouter>) -> Self {
        Self {
            engine,
            router,
            exec_log_index: Default::default(),
            exec_log_term: Default::default(),
            wb: RefCell::new(WriteBatch::default()),
            apply_wait: APPLY_TASK_WAIT_TIME_HISTOGRAM.local(),
            apply_time: APPLY_TIME_HISTOGRAM.local(),
            observer: None,
        }
    }

    pub fn set_apply_observer(&mut self, observer: Box<dyn ApplyObserver>) {
        self.observer = Some(observer);
    }

    pub fn take_apply_observer(&mut self) -> Option<Box<dyn ApplyObserver>> {
        self.observer.take()
    }

    pub fn flush_observer(&mut self) {
        if let Some(observer) = &mut self.observer {
            observer.flush();
        }
    }

    pub fn flush_observer_region(&mut self, region_id: u64) {
        if let Some(observer) = &mut self.observer {
            observer.flush_region(region_id);
        }
    }

    pub(crate) fn get_engine_wb(&self, region_id: u64) -> RefMut<'_, WriteBatch> {
        let mut wb = self.wb.borrow_mut();
        wb.reset(region_id);
        wb
    }

    pub(crate) fn finish_for(&self, applier: &mut Applier, results: VecDeque<ExecResult>) {
        if let Some(router) = &self.router {
            let apply_res = MsgApplyResult {
                peer_id: applier.id(),
                results,
                apply_state: applier.apply_state,
                metrics: applier.metrics.clone(),
                bucket_stat: applier.buckets.take().map(Box::new),
            };
            let region_id = applier.region.get_id();
            let msg = Box::new(PeerMsg::ApplyResult(apply_res));
            if let Err(err) = router.peer_sender.send((region_id, msg)) {
                warn!("send apply result error {:?}", err);
            }
        }
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct ApplyMetrics {
    pub approximate_size: u64,
    pub written_bytes: u64,
    pub written_keys: u64,
    pub lock_cf_written_bytes: u64,
}

#[derive(Default)]
pub(crate) struct PausedApplyQueue {
    queue: VecDeque<MsgApply>,
    paused_sequences: Vec<u64>,
}

impl PausedApplyQueue {
    /// Note that if `must_not_paused` is false, there would be part of entries
    /// can be applied.
    pub fn must_not_paused(&self) -> bool {
        let not_paused = self.paused_sequences.is_empty();
        if not_paused {
            // If queue is not empty, the entries in it would have no chance to apply.
            // It should be caused by missing to call `resume_handle_apply` after unpause.
            debug_assert!(
                self.queue.is_empty(),
                "PausedApplyQueue must be empty when not paused: {:?}",
                self.queue
            );
        }
        not_paused
    }

    pub fn paused_sequence(&self) -> Option<u64> {
        self.paused_sequences.first().cloned()
    }

    pub fn pause(&mut self, seq: u64, label: &str) {
        info!("{} pause apply at {}", label, seq);
        self.paused_sequences.push(seq);
    }

    pub fn unpause(&mut self, label: &str) -> Option<u64> {
        if self.paused_sequences.is_empty() {
            return None;
        }
        let paused_seq = self.paused_sequences.remove(0);
        info!("{} unpause apply at {:?}", label, paused_seq);
        Some(paused_seq)
    }

    /// Unpause by picking the expected seq in queue, and return true if found.
    /// Otherwise return false.
    pub fn pick_unpause(&mut self, expected_seq: u64, label: &str) -> bool {
        // `paused_sequences` must be small, so it's ok to use `contain` & `retain`.
        let found = self.paused_sequences.contains(&expected_seq);
        if found {
            self.paused_sequences.retain(|&seq| seq != expected_seq);
            info!("{} unpause apply at {}", label, expected_seq);
        }
        found
    }

    /// Return true when the seq is un-paused.
    /// Otherwise false.
    pub fn try_unpause(&mut self, seq: u64, label: &str) -> bool {
        if self.paused_sequence() == Some(seq) {
            self.unpause(label);
            true
        } else {
            false
        }
    }

    /// Push would return part of msg that is not paused.
    #[inline]
    pub fn push(&mut self, msg: MsgApply) -> Option<MsgApply> {
        self.queue.push_back(msg);
        self.take_next()
    }

    pub fn take_next(&mut self) -> Option<MsgApply> {
        if self.queue.is_empty() {
            return None;
        }
        if let Some(&seq) = self.paused_sequences.first() {
            let front = self.queue.front_mut().unwrap();
            // Treat message with empty entries as not paused.
            // Otherwise, this message would block following available entries.
            if front
                .last_raft_index()
                .is_none_or(|idx| !Self::index_should_pause(idx, seq))
            {
                self.queue.pop_front()
            } else if Self::index_should_pause(front.first_raft_index().unwrap(), seq) {
                None
            } else {
                front.split_off(seq)
            }
        } else {
            self.queue.pop_front()
        }
    }

    #[inline]
    fn index_should_pause(raft_idx: u64, pause_seq: u64) -> bool {
        raft_idx >= pause_seq
    }

    /// Drain all elements in the queue regardless of the pause state.
    /// Used on destroy only.
    pub fn drain<R>(&mut self, range: R) -> vec_deque::Drain<'_, MsgApply>
    where
        R: RangeBounds<usize>,
    {
        self.queue.drain(range)
    }

    pub fn sort_and_dedup_paused_seqs(&mut self) {
        self.paused_sequences.sort();
        self.paused_sequences.dedup();
    }
}

// Only for ChangeSets will pause/unpause apply queue.
fn change_set_label(cs: &kvenginepb::ChangeSet) -> &'static str {
    if cs.has_initial_flush() {
        "initial_flush"
    } else if cs.has_ingest_files() {
        "ingest_files"
    } else if cs.has_restore_shard() {
        "restore_shard"
    } else {
        "other"
    }
}

#[cfg(test)]
mod tests {
    use raft_proto::eraftpb::Entry;

    use super::*;

    #[test]
    fn test_mem_table_state() {
        // Test that
        #[derive(Clone, Copy, Debug)]
        struct Case {
            size_kb: u64,
            propose_time: Option<u64>,
            switch_time: Option<u64>,
            check_time: u64,
            check_result: bool,
        }
        impl Case {
            fn new(size_kb: u64) -> Self {
                Case {
                    size_kb,
                    propose_time: None,
                    switch_time: None,
                    check_time: 0,
                    check_result: false,
                }
            }

            fn propose_at(&self, secs: u64) -> Self {
                let mut c = *self;
                c.propose_time = Some(secs);
                c
            }

            fn switch_at(&self, secs: u64) -> Self {
                let mut c = *self;
                c.switch_time = Some(secs);
                c
            }

            fn check_at(&self, secs: u64) -> Self {
                let mut c = *self;
                c.check_time = secs;
                c
            }

            fn result(&self, b: bool) -> Self {
                let mut c = *self;
                c.check_result = b;
                c
            }
        }

        let cases = vec![
            // check mem size bound
            Case::new(0).result(false),
            // check propose
            Case::new(129 * 1024)
                .propose_at(0)
                .check_at(3)
                .result(false),
            Case::new(129 * 1024)
                .propose_at(0)
                .check_at(11)
                .result(true),
            // check switched
            Case::new(32 * 1024).check_at(39).result(false),
            Case::new(32 * 1024).check_at(41).result(true),
            Case::new(8 * 1024)
                .switch_at(100)
                .check_at(259)
                .result(false),
            Case::new(8 * 1024)
                .switch_at(100)
                .check_at(261)
                .result(true),
            Case::new(4 * 1024).check_at(319).result(false),
            Case::new(4 * 1024).check_at(321).result(true),
            Case::new(2 * 1024).check_at(639).result(false),
            Case::new(2 * 1024).check_at(641).result(true),
            Case::new(1024).check_at(1279).result(false),
            Case::new(1024).check_at(1281).result(true),
            Case::new(256).check_at(5119).result(false),
            Case::new(256).check_at(5121).result(true),
            Case::new(64).check_at(20479).result(false),
            Case::new(64).check_at(20481).result(true),
            Case::new(1).check_at(23 * 60 * 60).result(false),
            Case::new(1).check_at(25 * 60 * 60).result(true),
        ];
        for case in cases {
            let mut states = MemTableState::new(case.size_kb * 1024, 0usize, BYTES_MB);
            states.proposed_time = case
                .propose_time
                .map(|secs| Instant::now() + Duration::from_secs(secs));
            states.last_switch_time = case
                .switch_time
                .map(|secs| Instant::now() + Duration::from_secs(secs));
            let check_time = Instant::now() + Duration::from_secs(case.check_time);
            assert_eq!(
                states.need_switch(&PeerTag::default(), check_time),
                case.check_result,
                "{:?}",
                case
            );
        }
    }

    #[test]
    fn test_paused_apply_queue() {
        fn new_msg(entry_indexes: &[u64], cb_indexes: &[u64]) -> MsgApply {
            let entries = entry_indexes
                .iter()
                .map(|&index| Entry {
                    index,
                    ..Default::default()
                })
                .collect();
            let cbs = cb_indexes
                .iter()
                .map(|&index| Proposal {
                    is_conf_change: false,
                    index,
                    term: 0,
                    cb: Callback::None,
                    propose_time: None,
                    must_pass_epoch_check: false,
                })
                .collect();
            MsgApply {
                term: 0,
                entries,
                new_role: None,
                cbs,
                bucket_meta: None,
            }
        }

        fn clone_msg(msg: &MsgApply) -> MsgApply {
            new_msg(
                msg.entries
                    .iter()
                    .map(|e| e.index)
                    .collect::<Vec<_>>()
                    .as_slice(),
                msg.cbs
                    .iter()
                    .map(|cb| cb.index)
                    .collect::<Vec<_>>()
                    .as_slice(),
            )
        }

        #[must_use]
        fn msg_is_equal(msg1: &MsgApply, msg2: &MsgApply) -> bool {
            msg1.entries == msg2.entries
                && msg1.cbs.len() == msg2.cbs.len()
                && msg1
                    .cbs
                    .iter()
                    .zip(msg2.cbs.iter())
                    .all(|(cb1, cb2)| cb1.index == cb2.index)
        }

        let mut queue = PausedApplyQueue::default();
        assert!(queue.must_not_paused());

        let msg0 = new_msg(&[0], &[0]);
        let msg1 = new_msg(&[1, 2, 3], &[1, 2, 3]);
        let msg2 = new_msg(&[4, 5, 6], &[4, 5, 6]);
        let msg3 = new_msg(&[7, 8, 9], &[7, 8, 9]);
        let msg4 = new_msg(&[10, 11, 12], &[10, 11, 12]);
        let msg5_empty = new_msg(&[], &[]);
        let msg6 = new_msg(&[13, 14, 15], &[13, 14, 15]);
        let msg7 = new_msg(&[16, 17, 18], &[16, 17, 18]);

        // Test push
        assert!(msg_is_equal(&queue.push(clone_msg(&msg0)).unwrap(), &msg0));
        assert_eq!(queue.paused_sequence(), None);
        queue.pause(1, "");
        assert_eq!(queue.paused_sequence(), Some(1));

        assert!(queue.push(clone_msg(&msg1)).is_none());
        assert!(queue.pick_unpause(1, ""));
        queue.pause(4, "");
        assert!(msg_is_equal(&queue.push(clone_msg(&msg2)).unwrap(), &msg1));
        assert!(queue.push(clone_msg(&msg3)).is_none());

        // Test take_next
        queue.pause(7, "");
        assert!(!queue.try_unpause(3, ""));
        assert!(queue.pick_unpause(4, ""));
        assert!(msg_is_equal(&queue.take_next().unwrap(), &msg2));
        assert!(queue.take_next().is_none());
        assert!(queue.try_unpause(7, ""));
        assert!(!queue.pick_unpause(7, ""));
        assert!(msg_is_equal(&queue.take_next().unwrap(), &msg3));
        assert!(queue.must_not_paused());

        // Test split
        queue.pause(11, "");
        assert!(msg_is_equal(
            &queue.push(clone_msg(&msg4)).unwrap(),
            &new_msg(&[10], &[10])
        ));
        assert!(queue.take_next().is_none());
        assert!(queue.push(clone_msg(&msg5_empty)).is_none());
        assert!(queue.push(clone_msg(&msg6)).is_none());

        assert!(queue.pick_unpause(11, ""));
        queue.pause(13, "");
        assert!(msg_is_equal(
            &queue.push(clone_msg(&msg7)).unwrap(),
            &new_msg(&[11, 12], &[11, 12])
        ));

        // Test empty entries
        assert!(msg_is_equal(&queue.take_next().unwrap(), &msg5_empty));

        // Test drain
        let mut drained_entries = vec![];
        for msg in queue.drain(..) {
            drained_entries.extend(msg.entries.iter().map(|e| e.get_index()));
        }
        assert_eq!(drained_entries, (13..=18).collect::<Vec<_>>());
    }

    #[test]
    fn test_paused_apply_queue_pick_unpause() {
        let mut queue = PausedApplyQueue::default();
        assert!(queue.must_not_paused());
        assert!(!queue.pick_unpause(1, ""));

        queue.pause(1, "");
        assert!(queue.pick_unpause(1, ""));
        assert!(!queue.pick_unpause(1, ""));
        assert!(queue.must_not_paused());

        queue.pause(2, "");
        queue.pause(4, "");
        queue.pause(6, "");
        assert!(!queue.pick_unpause(1, ""));
        assert!(!queue.pick_unpause(5, ""));
        assert!(!queue.pick_unpause(7, ""));
        assert!(queue.pick_unpause(4, ""));
        assert!(!queue.pick_unpause(4, ""));
        assert!(queue.pick_unpause(6, ""));
        assert!(!queue.pick_unpause(6, ""));
        assert!(queue.pick_unpause(2, ""));
        assert!(!queue.pick_unpause(2, ""));
        assert!(queue.must_not_paused());
    }
}
