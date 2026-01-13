// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, iter::FromIterator, sync::Arc};

use api_version::ApiV2;
use bytes::{Buf, Bytes};
use cloud_encryption::EncryptionKey;
use collections::HashSet;
use kvengine::{
    Engine, FilePrepareType, PrepareOpts, Shard, ShardMeta, collect_snap_lock_txn_file_refs,
    table::{
        SnapVersion,
        tiny_meta::{MetaPackReader, MetaPackScheduler},
    },
};
use kvenginepb::ChangeSet;
use kvproto::{
    metapb,
    raft_cmdpb::{CustomRequest, RaftCmdRequest},
    raft_serverpb::{self, RegionLocalState},
};
use protobuf::Message;
use raft_proto::eraftpb;
use raftstore::store::metrics::BLACKLIST_REGION_GAUGE;
use rfengine::{
    TRUNCATE_ALL_INDEX, WriteBatch, load_store_ident, raft_state_key, region_state_key,
};
use slog_global::info;
use tikv_util::{debug, warn};

use crate::store::{
    Applier, ApplyContext, CustomRaftLog, PeerTag, RaftApplyState, RaftState, RegionIdVer,
    TERM_KEY, is_change_set_affect_mem_table, is_property_change_set, load_raft_engine_meta,
    load_raft_truncated_state, load_region_state, rlog,
};

#[derive(Clone)]
pub struct RecoverHandler {
    rf_engine: rfengine::RfEngine,
    store_id: u64,
    region_peer_map: Arc<HashMap<u64, u64>>,
    black_list: Option<BlackList>,
    contained_region_ids: Option<HashSet<u64>>,
    files_in_blacklist: Vec<u64>,
    merged_engine: bool,

    meta_pack_scheduler: Option<MetaPackScheduler>,
    meta_pack_reader: Option<MetaPackReader>,
}

pub const BLACK_LIST_FILE: &str = "black_list_file";

#[derive(Clone)]
pub struct BlackList {
    keyspace_ids: HashSet<u32>,
    table_ids: HashSet<(u32, i64)>,
    region_ids: HashSet<u64>,
}

impl BlackList {
    pub fn new(
        mut keyspace_ids: Vec<u32>,
        mut table_ids: Vec<(u32, i64)>,
        mut region_ids: Vec<u64>,
    ) -> Self {
        BLACKLIST_REGION_GAUGE.add(region_ids.len() as i64);
        Self {
            keyspace_ids: HashSet::from_iter(keyspace_ids.drain(..)),
            table_ids: HashSet::from_iter(table_ids.drain(..)),
            region_ids: HashSet::from_iter(region_ids.drain(..)),
        }
    }

    pub(crate) fn check_blocked(&mut self, region_id: u64, start: &[u8]) -> bool {
        if let Some(keyspace_id) = ApiV2::get_u32_keyspace_id_by_key(start) {
            if self.keyspace_ids.contains(&keyspace_id) {
                BLACKLIST_REGION_GAUGE.inc();
                self.region_ids.insert(region_id);
            }
        }
        if let Some((keyspace_id, table_id)) = ApiV2::get_keyspace_table_id(start) {
            if self.is_table_blocked(keyspace_id, table_id) {
                BLACKLIST_REGION_GAUGE.inc();
                self.region_ids.insert(region_id);
            }
        }
        self.region_ids.contains(&region_id)
    }

    pub(crate) fn is_region_blocked(&self, region_id: u64) -> bool {
        self.region_ids.contains(&region_id)
    }

    pub(crate) fn is_keyspace_blocked(&self, keyspace_id: u32) -> bool {
        self.keyspace_ids.contains(&keyspace_id)
    }

    pub(crate) fn is_table_blocked(&self, keyspace_id: u32, table_id: i64) -> bool {
        self.table_ids.contains(&(keyspace_id, table_id))
    }

    pub fn add_regions(&mut self, region_ids: Vec<u64>) {
        BLACKLIST_REGION_GAUGE.add(region_ids.len() as i64);
        self.region_ids.extend(region_ids);
    }

    pub fn add_tables(&mut self, table_ids: Vec<(u32, i64)>) {
        self.table_ids.extend(table_ids);
    }

    pub fn add_keyspaces(&mut self, keyspace_ids: Vec<u32>) {
        self.keyspace_ids.extend(keyspace_ids);
    }
}

impl RecoverHandler {
    pub fn new(rf_engine: rfengine::RfEngine) -> Self {
        let store_id = match load_store_ident(&rf_engine) {
            Some(ident) => ident.store_id,
            None => 0,
        };
        let region_peer_map = Arc::new(rf_engine.get_region_peer_map());
        Self {
            rf_engine,
            store_id,
            region_peer_map,
            black_list: None,
            contained_region_ids: None,
            files_in_blacklist: vec![],
            merged_engine: false,
            meta_pack_scheduler: None,
            meta_pack_reader: None,
        }
    }

    pub fn set_contained_region_ids(&mut self, mut region_ids: Vec<u64>) {
        self.contained_region_ids = Some(HashSet::from_iter(region_ids.drain(..)))
    }

    pub fn set_merged_engine(&mut self, merged_engine: bool) {
        self.merged_engine = merged_engine;
    }

    pub fn set_meta_pack(
        &mut self,
        scheduler: Option<MetaPackScheduler>,
        reader: Option<MetaPackReader>,
    ) {
        self.meta_pack_scheduler = scheduler;
        self.meta_pack_reader = reader;
    }

    pub fn set_black_list(&mut self, black_list: BlackList) {
        self.black_list = Some(black_list)
    }

    pub fn take_black_list(&mut self) -> Option<BlackList> {
        self.black_list.take()
    }

    fn load_region_meta(&self, shard_id: u64, shard_ver: u64) -> (metapb::Region, u64) {
        let peer_id = self.get_peer_id(shard_id);
        let tag = PeerTag::new(self.store_id, RegionIdVer::new(shard_id, shard_ver));

        let mut region_state = self
            .rf_engine
            .load_region_state(peer_id, shard_ver)
            .unwrap_or_else(|| {
                panic!(
                    "{} failed to get region state, state key {:?}, state keys {:?}",
                    tag,
                    region_state_key(shard_ver),
                    self.get_state_keys(peer_id)
                );
            });
        let region = region_state.take_region();

        let raft_state_key = raft_state_key(shard_ver);
        let raft_state_val = self
            .rf_engine
            .get_state(peer_id, &raft_state_key)
            .unwrap_or_else(|| {
                panic!(
                    "{} failed to get raft state, state keys {:?}",
                    tag,
                    self.get_state_keys(peer_id)
                );
            });
        let mut raft_state = RaftState::default();
        raft_state.unmarshal(raft_state_val.as_ref());
        (region, raft_state.last_preprocessed_index)
    }

    fn get_state_keys(&self, peer_id: u64) -> Vec<Bytes> {
        let mut state_keys = vec![];
        self.rf_engine.iterate_peer_states(peer_id, false, |k, _| {
            state_keys.push(k.clone());
            true
        });
        state_keys
    }

    fn execute_admin_request(
        applier: &mut Applier,
        ctx: &mut ApplyContext,
        req: RaftCmdRequest,
    ) -> kvengine::Result<()> {
        let admin_req = req.get_admin_request();
        if admin_req.has_change_peer() {
            applier
                .exec_change_peer(ctx, admin_req)
                .map_err(|x| kvengine::Error::ErrOpen(format!("{}", x)))?;
        }
        Ok(())
    }

    fn get_peer_id(&self, shard_id: u64) -> u64 {
        if self.merged_engine {
            shard_id
        } else {
            *self.region_peer_map.get(&shard_id).unwrap()
        }
    }

    pub fn recover_with_apply_ctx(
        &self,
        ctx: &mut ApplyContext,
        shard: &Arc<Shard>,
        meta: &ShardMeta,
        is_parent: bool,
    ) -> kvengine::Result<()> {
        let tag = shard.tag();
        let applied_index = shard.get_write_sequence();
        let applied_index_term = shard.get_property(TERM_KEY).unwrap().get_u64_le();
        let apply_state = RaftApplyState::new(applied_index, applied_index_term);
        let (region_meta, preprocessed_index) = self.load_region_meta(shard.id, shard.ver);
        let low_idx = applied_index + 1;
        let high_idx = preprocessed_index + 1;
        info!("{} recover: [{}, {})", tag, low_idx, high_idx,);
        let mut entries = Vec::with_capacity((high_idx.saturating_sub(low_idx)) as usize);
        let peer_id = self.get_peer_id(shard.id);
        self.rf_engine
            .fetch_raft_entries_to(peer_id, low_idx, high_idx, None, &mut entries)
            .map_err(|e| {
                let stats = self.rf_engine.get_peer_stats(peer_id);
                let truncated_state = load_raft_truncated_state(&self.rf_engine, peer_id);
                let err_msg = format!(
                    "{} entries unavailable err: {:?}, stats {:?}, truncated_state: {:?}, low: {}, high {}",
                    tag,
                    e,
                    stats,
                    truncated_state,
                    low_idx,
                    high_idx
                );
                kvengine::Error::ErrOpen(err_msg)
            })?;

        let snap = shard.new_snap_access();
        let encryption_key = snap.get_encryption_key();
        let mut applier = Applier::new_for_recover(self.store_id, region_meta, snap, apply_state);

        for e in &entries {
            if e.data.is_empty() || e.entry_type != eraftpb::EntryType::EntryNormal {
                continue;
            }
            ctx.exec_log_index = e.get_index();
            ctx.exec_log_term = e.get_term();
            let req = applier.parse_cmd(e);
            if req.get_header().get_region_epoch().version != shard.ver {
                continue;
            }
            if req.has_admin_request() {
                let admin = req.get_admin_request();
                let is_split_or_prepare_merge = admin.has_splits() || admin.has_prepare_merge();
                if is_split_or_prepare_merge && shard.has_txn_file_locks() {
                    info!("{} recover: split/prepare_merge is ignored due to txn file locks", tag; "log_index" => ctx.exec_log_index);
                } else if is_split_or_prepare_merge || admin.has_commit_merge() {
                    // We are recovering an parent shard, we need to switch the mem-table for
                    // children to copy.
                    ctx.engine.switch_mem_table(
                        shard,
                        SnapVersion::new(meta.base_version, ctx.exec_log_index),
                        true,
                        ctx.exec_log_index,
                    );
                    // It is the last command for a parent shard, we should return here.
                    return Ok(());
                } else {
                    Self::execute_admin_request(&mut applier, ctx, req)?;
                }
            } else if let Some(custom) = rlog::get_custom_log(&req) {
                if rlog::is_txn_file_ref(custom.data) {
                    let txn_file_ref = custom.get_txn_file_ref().unwrap();
                    prepare_txn_file_ref(&ctx.engine, &txn_file_ref, encryption_key.clone())?;
                }
                let cs = if rlog::is_engine_meta_log(custom.data) && !is_parent {
                    // parent table may failed to apply change set, because write cf sst is mising.
                    Some(recover_change_set(
                        ctx,
                        e,
                        meta,
                        encryption_key.as_ref(),
                        &custom,
                        self.meta_pack_reader.as_ref(),
                    )?)
                } else {
                    None
                };
                if cs.as_ref().is_none_or(is_change_set_affect_mem_table) {
                    if let Err(err) = applier.exec_custom_log(ctx, &custom, cs) {
                        // Only duplicated pre-split may fail, we can ignore this error.
                        warn!("{} failed to execute custom log {:?}", tag, err);
                    }
                }
            }
            applier.apply_state.applied_index = ctx.exec_log_index;
            applier.apply_state.applied_index_term = ctx.exec_log_term;
        }
        if let Some(last_entry) = entries.last() {
            shard.update_write_sequence_on_recover(last_entry.index);
        }
        Ok(())
    }
}

impl kvengine::RecoverHandler for RecoverHandler {
    fn recover(
        &self,
        engine: &Engine,
        shard: &Arc<Shard>,
        meta: &ShardMeta,
        is_parent: bool,
    ) -> kvengine::Result<()> {
        let mut ctx = ApplyContext::new(engine.clone(), None);
        self.recover_with_apply_ctx(&mut ctx, shard, meta, is_parent)
    }

    fn meta_pack_scheduler(&self) -> Option<&MetaPackScheduler> {
        self.meta_pack_scheduler.as_ref()
    }

    fn meta_pack_reader(&self) -> Option<&MetaPackReader> {
        self.meta_pack_reader.as_ref()
    }
}

fn prepare_txn_file_ref(
    kv: &Engine,
    txn_file_ref: &kvenginepb::TxnFileRef,
    encryption_key: Option<EncryptionKey>,
) -> kvengine::Result<()> {
    let manager = kv.get_txn_chunk_manager();
    manager.prepare_txn_chunks(txn_file_ref.chunk_ids.clone(), encryption_key)
}

fn recover_change_set(
    ctx: &mut ApplyContext,
    e: &eraftpb::Entry,
    meta: &ShardMeta,
    encryption_key: Option<&EncryptionKey>,
    custom: &CustomRaftLog<'_>,
    meta_pack_reader: Option<&MetaPackReader>,
) -> kvengine::Result<ChangeSet> {
    let mut cs = custom.get_change_set().unwrap();
    // Change sets that only set property are applied synchronously by
    // exec_custom_log, other change sets are applied asynchronously.
    if !is_property_change_set(&cs) {
        cs.sequence = e.get_index();
        if meta.ver == cs.get_shard_ver() && !meta.is_duplicated_change_set(&mut cs) {
            // We don't have a background region worker now, should do it
            // synchronously.
            let cs1 = ctx.engine.prepare_change_set(
                std::mem::take(&mut cs),
                PrepareOpts {
                    prepare_type: FilePrepareType::from_shard_meta(meta),
                    encryption_key: encryption_key.cloned(),
                    meta_pack_reader,
                    ..Default::default()
                },
            )?;
            ctx.engine.apply_change_set(&cs1)?;
            cs = cs1.into_inner();
        }
    }
    Ok(cs)
}

struct PeerToDestroy {
    region_local_state: RegionLocalState,
    region_id: u64,
    peer_id: u64,
    shard_ver: u64,
    parent_id: Option<u64>,
}

impl kvengine::MetaIterator for RecoverHandler {
    fn iterate<F>(&mut self, mut f: F) -> kvengine::Result<()>
    where
        F: FnMut(ChangeSet),
    {
        let store_id = self.engine_id();
        let mut wb = WriteBatch::new();
        let region_to_peers = self.rf_engine.get_region_peer_map();

        let mut peers_to_destroy = vec![];
        let mut region_ids_to_destroy = HashSet::default();
        let mut destroy_peer = |region_local_state, region_id, peer_id, shard_ver, parent_id| {
            peers_to_destroy.push(PeerToDestroy {
                region_local_state,
                region_id,
                peer_id,
                shard_ver,
                parent_id,
            });
            region_ids_to_destroy.insert(region_id);
        };

        for (region_id, peer_id) in region_to_peers {
            tikv_util::set_current_region(region_id);
            if let Some(cs) = load_raft_engine_meta(&self.rf_engine, peer_id) {
                assert_eq!(region_id, cs.shard_id);
                let region_local_state = load_region_state(&self.rf_engine, peer_id, cs.shard_ver)
                    .unwrap_or_else(|| {
                        panic!(
                            "{}:{}:{} failed to get region state, state key {:?}",
                            store_id,
                            region_id,
                            cs.shard_ver,
                            region_state_key(cs.shard_ver),
                        );
                    });

                let parent_id = cs.has_parent().then(|| {
                    let parent = cs.get_parent();
                    self.rf_engine.add_dependent(parent.shard_id, region_id);
                    parent.shard_id
                });

                // Check if the store exists in the region peers. The peer may be already
                // removed in region local state, but not destroyed yet. It's safe to destroy
                // it.
                if !region_local_state
                    .get_region()
                    .get_peers()
                    .iter()
                    .any(|p| p.get_store_id() == store_id)
                {
                    warn!(
                        "store {} not found in region peers {:?}, set it to tombstone",
                        store_id,
                        region_local_state.get_region()
                    );
                    destroy_peer(
                        region_local_state,
                        region_id,
                        peer_id,
                        cs.shard_ver,
                        parent_id,
                    );
                    continue;
                }
                if let Some(contained_region_ids) = self.contained_region_ids.as_mut() {
                    if !contained_region_ids.contains(&region_id) {
                        warn!("region {} removed by pd", region_id);
                        destroy_peer(
                            region_local_state,
                            region_id,
                            peer_id,
                            cs.shard_ver,
                            parent_id,
                        );
                        continue;
                    }
                }
                if let Some(black_list) = self.black_list.as_mut() {
                    let snap = cs.get_snapshot();
                    if black_list.check_blocked(cs.shard_id, snap.get_outer_start()) {
                        warn!("region {} blocked by black list", cs.shard_id);
                        // Collect all file ids for blacklisted region.
                        snap.get_l0_creates().iter().for_each(|f| {
                            self.files_in_blacklist.push(f.get_id());
                        });
                        snap.get_table_creates().iter().for_each(|f| {
                            self.files_in_blacklist.push(f.get_id());
                        });
                        snap.get_blob_creates().iter().for_each(|f| {
                            self.files_in_blacklist.push(f.get_id());
                        });
                        snap.get_columnar_creates().iter().for_each(|f| {
                            self.files_in_blacklist.push(f.get_id());
                        });
                        if snap.get_schema_meta().get_file_id() > 0 {
                            self.files_in_blacklist
                                .push(snap.get_schema_meta().get_file_id());
                        }
                        snap.get_vector_indexes().iter().for_each(|vec_idx| {
                            for file in vec_idx.get_files() {
                                self.files_in_blacklist.push(file.get_id());
                            }
                        });
                        collect_snap_lock_txn_file_refs(snap)
                            .into_iter()
                            .flat_map(|r| r.chunk_ids)
                            .for_each(|chunk_id| {
                                self.files_in_blacklist.push(chunk_id);
                            });
                        continue;
                    }
                }
                f(cs);
            }
        }

        for mut peer in peers_to_destroy {
            let mut has_dependent = false;
            self.rf_engine
                .with_dependents(peer.region_id, |dependents| {
                    has_dependent = dependents
                        .iter()
                        .any(|dependent_id| !region_ids_to_destroy.contains(dependent_id));
                });
            if has_dependent {
                continue;
            }
            let keyspace_id = ApiV2::get_u32_keyspace_id_by_key(
                peer.region_local_state.get_region().get_start_key(),
            )
            .unwrap_or_default();
            self.rf_engine
                .iterate_peer_states(peer.peer_id, false, |k, _| {
                    wb.set_state(peer.peer_id, peer.region_id, keyspace_id, k, &[]);
                    true
                });
            peer.region_local_state.state = raft_serverpb::PeerState::Tombstone;
            let region_state_val = peer.region_local_state.write_to_bytes().unwrap();
            let region_state_key = region_state_key(peer.shard_ver);
            wb.set_state(
                peer.peer_id,
                peer.region_id,
                keyspace_id,
                &region_state_key,
                &region_state_val,
            );
            wb.truncate_raft_log(
                peer.peer_id,
                peer.region_id,
                keyspace_id,
                TRUNCATE_ALL_INDEX,
            );
            if let Some(&parent_id) = peer.parent_id.as_ref() {
                self.rf_engine.remove_dependent(parent_id, peer.region_id);
            }
            info!(
                "{}:{}:{} destroy stale/removed peer",
                store_id, peer.region_id, peer.shard_ver;
                "peer_id" => peer.peer_id,
                "parent_id" => peer.parent_id,
            );
        }

        if !wb.is_empty() {
            self.rf_engine.write(wb).unwrap();
        }
        Ok(())
    }

    fn take_files_in_blacklist(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.files_in_blacklist)
    }

    fn engine_id(&self) -> u64 {
        self.store_id
    }
}

pub fn apply_custom_log_in_recover(
    engine: &Engine,
    store_id: u64,
    shard: &Shard,
    region_meta: metapb::Region,
    custom_req: CustomRequest,
) -> crate::errors::Result<()> {
    let custom = CustomRaftLog::new_from_data(custom_req.get_data());
    let applied_index = shard.get_write_sequence();
    let mut ctx = ApplyContext::new(engine.clone(), None);
    let applied_index_term = shard.get_property(TERM_KEY).unwrap().get_u64_le();
    let apply_state = RaftApplyState::new(applied_index, applied_index_term);

    let snap = shard.new_snap_access();
    let mut applier = Applier::new_for_recover(store_id, region_meta, snap, apply_state);
    ctx.exec_log_index = applied_index + 1;
    ctx.exec_log_term = applied_index_term;
    debug!("{} apply_custom_log_in_recover", shard.tag(); "log_index" => ctx.exec_log_index);
    let _ = applier.exec_custom_log(&mut ctx, &custom, None)?;
    Ok(())
}
