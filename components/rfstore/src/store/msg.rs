// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{borrow::Cow, collections::VecDeque, fmt, fmt::Formatter, sync::Arc};

use cloud_encryption::EncryptionKey;
use kvengine::{FilePrepareType, table::schema_file::SchemaFile};
use kvenginepb::TxnFileRef;
use kvproto::{
    kvrpcpb::ExtraOp as TxnExtraOp,
    metapb, pdpb,
    pdpb::SyncRegionResponse,
    raft_cmdpb::{RaftCmdRequest, RaftCmdResponse},
    raft_serverpb as rspb,
    raft_serverpb::RaftMessage,
};
use pd_client::{BucketMeta, BucketStat};
use raft_proto::eraftpb;
use raftstore::store::util::KeysInfoFormatter;
use tikv_util::time::Instant;

use super::{Peer, PeerInbox, RaftApplyState};
use crate::store::{
    ApplyMetrics, ExecResult, Proposal, RegionIdVer, RegionSnapshot, TrimOverBoundParameter,
};

#[derive(Debug)]
pub enum PeerMsg {
    RaftMessage(rspb::RaftMessage),
    RaftCommand(RaftCommand),
    Tick,
    Start,
    ApplyResult(MsgApplyResult),
    CasualMessage(CasualMessage),
    /// Message that can't be lost but rarely created. If they are lost, real
    /// bad things happen like some peers will be considered dead in the
    /// group.
    SignificantMsg(SignificantMsg),
    GenerateEngineChangeSet(kvenginepb::ChangeSet, Callback),
    ApplySnapshotResult(kvenginepb::ChangeSet),
    PrepareChangeSetResult(
        kvengine::Result<kvengine::ChangeSet>,
        u64, // peer_id
    ),
    PrepareCommitMergeResult(
        kvengine::Result<kvengine::ChangeSet>,
        u64, // commit index
    ),
    PrepareTxnFileResult {
        entry_index: u64,
        peer_id: u64,
    },
    Persisted(PersistReady),
    Idle(PeerInbox),
    WakeUp(PeerInbox),
    StoreMsgForWakeUp(StoreMsg),
}

impl PeerMsg {
    pub(crate) fn size(&self) -> usize {
        match self {
            PeerMsg::RaftMessage(msg) => {
                let entries = msg.get_message().get_entries();
                let mut size = 0;
                for entry in entries {
                    size += entry.data.len();
                }
                size
            }
            PeerMsg::RaftCommand(cmd) => {
                if cmd.request.has_custom_request() {
                    return cmd.request.get_custom_request().data.len();
                }
                0
            }
            _ => 0,
        }
    }

    pub(crate) fn type_str(&self) -> &str {
        match self {
            PeerMsg::RaftMessage(_) => "RaftMessage",
            PeerMsg::RaftCommand(_) => "RaftCommand",
            PeerMsg::Tick => "Tick",
            PeerMsg::Start => "Start",
            PeerMsg::ApplyResult(_) => "ApplyResult",
            PeerMsg::CasualMessage(_) => "CasualMessage",
            PeerMsg::SignificantMsg(_) => "SignificantMsg",
            PeerMsg::GenerateEngineChangeSet(..) => "GenerateEngineChangeSet",
            PeerMsg::ApplySnapshotResult(_) => "ApplySnapshotResult",
            PeerMsg::PrepareChangeSetResult(..) => "PrepareChangeSetResult",
            PeerMsg::PrepareCommitMergeResult(..) => "PrepareCommitMergeResult",
            PeerMsg::PrepareTxnFileResult { .. } => "PrepareTxnFileResult",
            PeerMsg::Persisted(_) => "Persisted",
            PeerMsg::Idle(_) => "Idle",
            PeerMsg::WakeUp(_) => "WakeUp",
            PeerMsg::StoreMsgForWakeUp(_) => "StoreMsgForWakeUp",
        }
    }

    pub(crate) fn type_code(&self) -> u8 {
        match self {
            PeerMsg::RaftMessage(_) => 1,
            PeerMsg::RaftCommand(_) => 2,
            PeerMsg::Tick => 3,
            PeerMsg::Start => 4,
            PeerMsg::ApplyResult(_) => 5,
            PeerMsg::CasualMessage(_) => 6,
            PeerMsg::SignificantMsg(_) => 7,
            PeerMsg::GenerateEngineChangeSet(..) => 8,
            PeerMsg::ApplySnapshotResult(_) => 9,
            PeerMsg::PrepareChangeSetResult(..) => 10,
            PeerMsg::PrepareCommitMergeResult(..) => 11,
            PeerMsg::PrepareTxnFileResult { .. } => 12,
            PeerMsg::Persisted(_) => 13,
            PeerMsg::Idle(_) => 14,
            PeerMsg::WakeUp(_) => 15,
            PeerMsg::StoreMsgForWakeUp(_) => 16,
        }
    }
}

#[derive(Debug)]
pub(crate) enum ApplyMsg {
    Apply(MsgApply),
    Registration(MsgRegistration),
    PendingSplit(kvenginepb::ChangeSet),
    ApplyChangeSet(kvengine::ChangeSet),
    PrepareChangeSet {
        cs: kvenginepb::ChangeSet,
        encryption_key: Option<EncryptionKey>,
        reload_snap: Option<kvenginepb::Snapshot>, /* The snap contains the current files that
                                                    * need to be reloaded. */
        prepare_type: FilePrepareType,
    },
    PrepareMerge,
    PrepareCommitMerge {
        source: kvenginepb::ChangeSet,
        commit_index: u64,
    },
    ResumeCommitMerge {
        source: kvengine::ChangeSet,
        commit_index: u64,
    },
    PrepareRollbackMerge(u64 /* initial flush sequence */),
    UnsafeDestroy {
        region_id: u64,
    },
    Maintenance {
        region_id: u64,
    },
    PrepareTxnFile {
        txn_file_ref: TxnFileRef,
        commit_index: u64,
        encryption_key: Option<EncryptionKey>,
    },
    ResumeTxnFile(u64 /* commit index */),
    TriggerRefreshShardStates,
}

pub enum StoreMsg {
    Tick,
    Start {
        store: metapb::Store,
    },
    StoreUnreachable {
        store_id: u64,
    },
    GenerateEngineChangeSet(kvenginepb::ChangeSet, Callback),
    RaftMessage(kvproto::raft_serverpb::RaftMessage),
    SnapshotReady(u64),
    GetRegionsInRange {
        start: Vec<u8>,
        end: Vec<u8>,
        callback: Box<dyn FnOnce(Vec<RegionIdVer>) + Send>,
    },
    GetRegionById {
        region_id: u64,
        callback: Box<dyn FnOnce(Option<metapb::Region>) + Send>,
    },
    SyncRegion {
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
        reverse: bool,
        callback: Box<dyn FnOnce(SyncRegionResponse) + Send>,
    },
    SyncRegionById {
        region_id: u64,
        callback: Box<dyn FnOnce(SyncRegionResponse) + Send>,
    },
    ApplyResult {
        region_id: u64,
        peer_id: u64,
    },
    DependentsEmpty(u64 /* region id */),
    PrepareMerge {
        region_id: u64,
        req: RaftCmdRequest,
    },
    CheckMerge(u64),
    Stop,
}

impl fmt::Debug for StoreMsg {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // dummy implementation for debug peer message.
        write!(f, "StoreMsg")
    }
}

#[derive(Debug)]
pub struct PersistReady {
    pub(crate) region_id: u64,
    pub(crate) peer_id: u64,
    pub(crate) ready_number: u64,
    pub(crate) _commit_idx: u64,
    pub(crate) raft_messages: Vec<RaftMessage>,
}

/// IOTask contains I/O tasks which need to be persisted to raft db.
pub(crate) struct IoTask {
    pub(crate) readies: Vec<PersistReady>,
    pub(crate) raft_wb: rfengine::WriteBatch,
    pub(crate) remove_dependents: Vec<(u64 /* parent_id */, u64 /* dependent_id */)>,
}

#[derive(Debug)]
pub struct RaftCommand {
    pub send_time: Instant,
    pub request: RaftCmdRequest,
    pub callback: Callback,
}

impl RaftCommand {
    pub fn new(request: RaftCmdRequest, callback: Callback) -> Self {
        Self {
            request,
            callback,
            send_time: Instant::now(),
        }
    }
}

#[derive(Debug)]
pub struct MsgApply {
    pub(crate) term: u64,
    pub(crate) entries: Vec<eraftpb::Entry>,
    pub(crate) new_role: Option<raft::StateRole>,
    pub(crate) cbs: Vec<Proposal>,
    pub(crate) bucket_meta: Option<Arc<BucketMeta>>,
}

impl MsgApply {
    /// Split off [-, raft-index). The rest [raft-index, +) is left in `self`.
    pub fn split_off(&mut self, raft_idx: u64) -> Option<MsgApply> {
        let pos = self
            .entries
            .iter()
            .position(|e| e.get_index() >= raft_idx)
            .unwrap_or(self.entries.len());
        if pos == 0 {
            return None;
        }
        let mut split_entries = self.entries.split_off(pos);
        std::mem::swap(&mut split_entries, &mut self.entries);

        let cbs_pos = self
            .cbs
            .iter()
            .position(|cb| cb.index >= raft_idx)
            .unwrap_or(self.cbs.len());
        let mut split_cbs = self.cbs.split_off(cbs_pos);
        std::mem::swap(&mut split_cbs, &mut self.cbs);

        Some(MsgApply {
            term: self.term,
            entries: split_entries,
            new_role: self.new_role,
            cbs: split_cbs,
            bucket_meta: self.bucket_meta.clone(),
        })
    }

    /// Return `None` when there is no entry.
    #[inline]
    pub fn first_raft_index(&self) -> Option<u64> {
        self.entries.first().map(|e| e.get_index())
    }

    /// Return `None` when there is no entry.
    #[inline]
    pub fn last_raft_index(&self) -> Option<u64> {
        self.entries.last().map(|e| e.get_index())
    }

    pub fn new_for_replication(entries: Vec<eraftpb::Entry>) -> Self {
        Self {
            term: 1,
            entries,
            new_role: None,
            cbs: vec![],
            bucket_meta: None,
        }
    }
}

#[derive(Debug)]
pub struct MsgApplyResult {
    pub(crate) peer_id: u64,
    pub(crate) results: VecDeque<ExecResult>,
    pub(crate) apply_state: RaftApplyState,
    pub(crate) metrics: ApplyMetrics,
    pub(crate) bucket_stat: Option<Box<BucketStat>>,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct MsgRegistration {
    pub(crate) peer: metapb::Peer,
    pub(crate) term: u64,
    pub(crate) apply_state: RaftApplyState,
    pub(crate) region: metapb::Region,
    #[derivative(Debug = "ignore")]
    pub(crate) encryption_key: Option<EncryptionKey>,
}

impl MsgRegistration {
    pub(crate) fn new(peer: &Peer) -> Self {
        Self {
            peer: peer.peer.clone(),
            term: peer.term(),
            apply_state: peer.get_store().apply_state(),
            region: peer.get_store().region().clone(),
            encryption_key: peer.encryption_key.clone(),
        }
    }
}

#[derive(Debug)]
pub struct ReadResponse {
    pub response: RaftCmdResponse,
    pub snapshot: Option<RegionSnapshot>,
    pub txn_extra_op: TxnExtraOp,
}

#[derive(Debug)]
pub struct WriteResponse {
    pub response: RaftCmdResponse,

    // Used for split_region only.
    pub key_errors: Option<Vec<kvproto::kvrpcpb::KeyError>>,
}

pub type ReadCallback = Box<dyn FnOnce(ReadResponse) + Send>;
pub type WriteCallback = Box<dyn FnOnce(WriteResponse) + Send>;
pub type ExtCallback = Box<dyn FnOnce() + Send>;

/// Variants of callbacks for `Msg`.
///  - `Read`: a callback for read only requests including `StatusRequest`,
///    `GetRequest` and `SnapRequest`
///  - `Write`: a callback for write only requests including `AdminRequest`
///    `PutRequest`, `DeleteRequest` and `DeleteRangeRequest`.
pub enum Callback {
    /// No callback.
    None,
    /// Read callback.
    Read(ReadCallback),
    /// Write callback.
    Write {
        cb: WriteCallback,
        /// `proposed_cb` is called after a request is proposed to the raft
        /// group successfully. It's used to notify the caller to move
        /// on early because it's very likely the request
        /// will be applied to the raftstore.
        proposed_cb: Option<ExtCallback>,
        /// `committed_cb` is called after a request is committed and before
        /// it's being applied, and it's guaranteed that the request
        /// will be successfully applied soon.
        committed_cb: Option<ExtCallback>,
    },
}

impl Callback {
    pub fn write(cb: WriteCallback) -> Self {
        Self::write_ext(cb, None, None)
    }

    pub fn write_ext(
        cb: WriteCallback,
        proposed_cb: Option<ExtCallback>,
        committed_cb: Option<ExtCallback>,
    ) -> Self {
        Callback::Write {
            cb,
            proposed_cb,
            committed_cb,
        }
    }

    pub fn invoke_with_response(self, resp: RaftCmdResponse) {
        self.invoke_with_response_ext(resp, None);
    }

    pub fn invoke_with_response_ext(
        self,
        resp: RaftCmdResponse,
        key_errs: Option<Vec<kvproto::kvrpcpb::KeyError>>,
    ) {
        match self {
            Callback::None => (),
            Callback::Read(read) => {
                let resp = ReadResponse {
                    response: resp,
                    snapshot: None,
                    txn_extra_op: TxnExtraOp::Noop,
                };
                read(resp);
            }
            Callback::Write { cb, .. } => {
                let resp = WriteResponse {
                    response: resp,
                    key_errors: key_errs,
                };
                cb(resp);
            }
        }
    }

    pub fn has_proposed_cb(&mut self) -> bool {
        if let Callback::Write { proposed_cb, .. } = self {
            proposed_cb.is_some()
        } else {
            false
        }
    }

    pub fn invoke_proposed(&mut self) {
        if let Callback::Write { proposed_cb, .. } = self {
            if let Some(cb) = proposed_cb.take() {
                cb()
            }
        }
    }

    pub fn invoke_committed(&mut self) {
        if let Callback::Write { committed_cb, .. } = self {
            if let Some(cb) = committed_cb.take() {
                cb()
            }
        }
    }

    pub fn invoke_read(self, args: ReadResponse) {
        match self {
            Callback::Read(read) => read(args),
            other => panic!("expect Callback::Read(..), got {:?}", other),
        }
    }

    pub fn is_none(&self) -> bool {
        matches!(self, Callback::None)
    }
}

impl fmt::Debug for Callback {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Callback::None => write!(fmt, "Callback::None"),
            Callback::Read(_) => write!(fmt, "Callback::Read(..)"),
            Callback::Write { .. } => write!(fmt, "Callback::Write(..)"),
        }
    }
}

/// Some significant messages sent to raftstore. Raftstore will dispatch these
/// messages to Raft groups to update some important internal status.
#[derive(Debug)]
pub enum SignificantMsg {
    StoreUnreachable { store_id: u64 },
}

/// Message that will be sent to a peer.
///
/// These messages are not significant and can be dropped occasionally.
pub enum CasualMessage {
    /// Split the target region into several partitions.
    SplitRegion {
        region_epoch: metapb::RegionEpoch,
        // It's an encoded key.
        // TODO: support meta key.
        split_keys: Vec<Vec<u8>>,
        callback: Callback,
        source: Cow<'static, str>,
    },

    /// Half split the target region.
    HalfSplitRegion {
        region_epoch: metapb::RegionEpoch,
        policy: pdpb::CheckPolicy,
        source: &'static str,
    },
    DeletePrefix {
        region_version: u64,
        prefix: Vec<u8>,
        callback: Callback,
    },
    /// IngestFiles from load_data worker.
    IngestFiles {
        cs: kvenginepb::ChangeSet,
        callback: Callback,
    },
    /// Restore from backup data.
    RestoreShard {
        cs: kvenginepb::ChangeSet,
        callback: Callback,
    },
    TriggerTrimOverBound(TrimOverBoundParameter),
    MajorCompact {
        major_compact: bool,
        columnar: bool,
        callback: Callback,
    },
    UpdateSchemaFile(SchemaFile),
    CheckLeader {
        shard_ver: u64,
        callback: Callback,
    },
    ClearColumnar {
        restore_version: Option<u64>,
    },
    TriggerRefreshShardStates,
    ForceSwitchMemTable {
        current_size: u64,
        callback: Callback,
    },
}

impl fmt::Debug for CasualMessage {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CasualMessage::SplitRegion {
                ref split_keys,
                source,
                ..
            } => write!(
                fmt,
                "Split region with {} from {}",
                KeysInfoFormatter(split_keys.iter()),
                source,
            ),
            CasualMessage::HalfSplitRegion { source, .. } => {
                write!(fmt, "Half Split from {}", source)
            }
            CasualMessage::DeletePrefix { prefix, .. } => {
                write!(fmt, "delete prefix {:?}", prefix)
            }
            CasualMessage::IngestFiles { cs, .. } => {
                write!(fmt, "ingest files {:?}", cs)
            }
            CasualMessage::RestoreShard { cs, .. } => {
                write!(fmt, "restore shard from {:?}", cs)
            }
            CasualMessage::TriggerTrimOverBound(param) => {
                write!(fmt, "trigger trim over bound {:?}", param)
            }
            CasualMessage::MajorCompact {
                major_compact,
                columnar,
                ..
            } => {
                write!(
                    fmt,
                    "major compact {:?}, columnar: {}",
                    major_compact, columnar
                )
            }
            CasualMessage::UpdateSchemaFile(schema_file) => {
                write!(
                    fmt,
                    "update schema file id:{}, ver:{}",
                    schema_file.get_file_id(),
                    schema_file.get_version()
                )
            }
            CasualMessage::CheckLeader { shard_ver, .. } => {
                write!(fmt, "check leader with version {}", shard_ver)
            }
            CasualMessage::ClearColumnar { restore_version } => {
                write!(
                    fmt,
                    "clear columnar, restore_version: {:?}",
                    restore_version
                )
            }
            CasualMessage::TriggerRefreshShardStates => {
                write!(fmt, "trigger refresh shard states")
            }
            CasualMessage::ForceSwitchMemTable { .. } => {
                write!(fmt, "manual flush")
            }
        }
    }
}
