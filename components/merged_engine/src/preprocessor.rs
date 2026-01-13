// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::Buf;
use cloud_encryption::{EncryptionKey, MasterKey};
use kvengine::{ENCRYPTION_KEY, GLOBAL_SHARD_END_KEY, ShardMeta};
use kvproto::{metapb, raft_serverpb::MergeState};
use raft_proto::eraftpb::HardState;
use rfstore::store::{PreprocessRef, RAFT_INIT_LOG_TERM, state::RaftState};
use tikv_util::{codec::bytes::encode_bytes, info};

pub(crate) struct Preprocessor {
    preprocessed_index: u64,
    region: metapb::Region,
    preprocessed_region: Option<metapb::Region>,
    peer: metapb::Peer,
    shard_meta: Option<ShardMeta>,
    last_committed_split_idx: u64,
    pending_truncate: Option<(u64 /* term */, u64 /* index */)>,
    raft_hard_state: HardState,
    raft_state: RaftState,
    pending_merge_state: Option<MergeState>,
    want_rollback_merge_peers: collections::HashSet<u64>,
    learner_skip_idx: u64,
    encryption_key: Option<EncryptionKey>,
}

impl Preprocessor {
    pub(crate) fn new(
        raft: &rfengine::RfEngine,
        store_id: u64,
        region_id: u64,
        master_key: &MasterKey,
    ) -> Option<Self> {
        let shard_meta =
            rfstore::store::load_engine_meta(raft, store_id, region_id).or_else(|| {
                info!(
                    "{}:{} new_preprocessor: no engine meta for region",
                    store_id, region_id
                );
                None
            })?;
        let encryption_key = shard_meta
            .get_property(ENCRYPTION_KEY)
            .map(|v| master_key.decrypt_encryption_key(&v).unwrap());
        let mut region = metapb::Region::default();
        region.set_id(shard_meta.id);
        let epoch = region.mut_region_epoch();
        epoch.set_version(shard_meta.ver);
        epoch.set_conf_ver(1);
        let mut peer = metapb::Peer::default();
        peer.set_id(shard_meta.id);
        peer.set_store_id(store_id);
        region.set_peers(vec![peer.clone()].into());
        region.set_start_key(encode_bytes(&shard_meta.range.outer_start));
        if shard_meta.range.outer_end.chunk() != GLOBAL_SHARD_END_KEY {
            region.set_end_key(encode_bytes(&shard_meta.range.outer_end));
        }
        let raft_index = shard_meta.seq;
        let mut raft_state = RaftState::default();
        let mut raft_hard_state = HardState::default();
        raft_hard_state.set_vote(shard_meta.id);
        raft_hard_state.set_term(RAFT_INIT_LOG_TERM);
        raft_state.set_last_preprocessed_index(raft_index);
        raft_state.set_hard_state(&raft_hard_state);

        let mut region_state = raft
            .load_region_state(shard_meta.id, shard_meta.ver)
            .unwrap_or_else(|| {
                let states = raft.get_peer_all_states(shard_meta.id, false);
                panic!(
                    "{} failed to load region state, states: {:?}",
                    shard_meta.tag(),
                    states
                );
            });
        let merge_state = region_state
            .has_merge_state()
            .then(|| region_state.take_merge_state());

        Some(Self {
            preprocessed_index: raft_index,
            region,
            preprocessed_region: None,
            peer,
            shard_meta: Some(shard_meta),
            last_committed_split_idx: raft_index,
            pending_truncate: None,
            raft_hard_state,
            raft_state,
            pending_merge_state: merge_state,
            want_rollback_merge_peers: collections::HashSet::default(),
            learner_skip_idx: raft_index,
            encryption_key,
        })
    }

    pub(crate) fn as_ref(&mut self) -> PreprocessRef<'_> {
        PreprocessRef {
            region: &mut self.region,
            preprocessed_index: &mut self.preprocessed_index,
            preprocessed_region: &mut self.preprocessed_region,
            peer: &mut self.peer,
            shard_meta: &mut self.shard_meta,
            last_committed_split_idx: &mut self.last_committed_split_idx,
            pending_truncate: &mut self.pending_truncate,
            raft_hard_state: self.raft_hard_state.clone(),
            raft_state: &mut self.raft_state,
            is_leader: false,
            pending_merge_state: &mut self.pending_merge_state,
            want_rollback_merge_peers: &mut self.want_rollback_merge_peers,
            learner_skip_idx: &mut self.learner_skip_idx,
            encryption_key: &mut self.encryption_key,
        }
    }

    pub(crate) fn sync_region(&mut self) {
        if let Some(new_region) = self.preprocessed_region.take() {
            self.region = new_region;
        }
    }

    pub(crate) fn mut_shard_meta(&mut self) -> Option<&mut ShardMeta> {
        self.shard_meta.as_mut()
    }

    pub(crate) fn pending_merge_state(&self) -> Option<&MergeState> {
        self.pending_merge_state.as_ref()
    }
}
