// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::{Buf, BufMut};
use kvproto::{
    metapb,
    raft_serverpb::{PeerState, RegionLocalState, StoreIdent},
};
use protobuf::{Message, RepeatedField};
use rfengine::{
    KV_ENGINE_META_DIFF_KEY, KV_ENGINE_META_KEY, KV_ENGINE_META_SNAP_DIFF_KEY,
    PREPARE_BOOTSTRAP_KEY, STORE_IDENT_KEY, TRUNCATE_ALL_INDEX, raft_state_key, region_state_key,
};
use tikv_util::{box_err, store::new_peer};

use super::peer_storage::{INIT_EPOCH_CONF_VER, INIT_EPOCH_VER, write_initial_raft_state};
use crate::{
    Result,
    store::{
        EMPTY_KEY, Engines, RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM, TERM_KEY, write_peer_state,
    },
};

pub fn initial_region(store_id: u64, region_id: u64, peer_id: u64) -> metapb::Region {
    let mut region = metapb::Region::default();
    region.set_id(region_id);
    region.set_start_key(EMPTY_KEY.to_vec());
    region.set_end_key(EMPTY_KEY.to_vec());
    region.mut_region_epoch().set_version(INIT_EPOCH_VER);
    region.mut_region_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);
    region.mut_peers().push(new_peer(store_id, peer_id));
    region
}

// Bootstrap the store, the DB for this store must be empty and has no data.
//
// FIXME: ER typaram should just be impl KvEngine, but RaftEngine doesn't
// support the `is_range_empty` query yet.
pub fn bootstrap_store(engines: &Engines, cluster_id: u64, store_id: u64) -> Result<()> {
    if engines.kv.size() > 1 {
        return Err(box_err!("kv store is not empty and has already had data."));
    }
    if !engines.raft.is_empty() {
        return Err(box_err!(
            "raft store is not empty and has already had data."
        ));
    }
    let mut ident = StoreIdent::default();

    ident.set_cluster_id(cluster_id);
    ident.set_store_id(store_id);
    let bin = ident.write_to_bytes().unwrap();
    let mut wb = rfengine::WriteBatch::new();
    wb.set_state(0, 0, 0, STORE_IDENT_KEY, bin.as_slice());
    engines.raft.write(wb)?;
    Ok(())
}

pub fn load_store_ident(engines: &Engines) -> Option<StoreIdent> {
    match engines.raft.get_state(0, STORE_IDENT_KEY) {
        None => None,
        Some(bin) => {
            let mut ident = StoreIdent::default();
            ident.merge_from_bytes(&bin).unwrap();
            Some(ident)
        }
    }
}

/// The first phase of bootstrap cluster
///
/// Write the first region meta and prepare state.
pub fn prepare_bootstrap_cluster(engines: &Engines, region: &metapb::Region) -> Result<()> {
    let peer_id = region.get_peers().first().unwrap().id;
    let mut state = RegionLocalState::default();
    state.set_region(region.clone());
    let mut raft_wb = rfengine::WriteBatch::new();
    let val = state.write_to_bytes().unwrap();
    raft_wb.set_state(0, 0, 0, PREPARE_BOOTSTRAP_KEY, val.as_slice());
    write_peer_state(&mut raft_wb, peer_id, region, PeerState::Normal, None);
    let region_version = region.get_region_epoch().get_version();
    write_initial_raft_state(&mut raft_wb, peer_id, region.get_id(), region_version, 0);
    let change_set = initial_change_set(region.get_id(), region_version);
    let cs_bin = change_set.write_to_bytes().unwrap();
    raft_wb.set_state(peer_id, region.get_id(), 0, KV_ENGINE_META_KEY, &cs_bin);
    engines.raft.write(raft_wb)?;
    engines.kv.ingest(change_set, true)?;
    Ok(())
}

pub fn initial_change_set(region_id: u64, shard_ver: u64) -> kvengine::ChangeSet {
    let mut change_set = kvenginepb::ChangeSet::default();
    change_set.set_shard_id(region_id);
    change_set.set_shard_ver(shard_ver);
    change_set.set_sequence(RAFT_INIT_LOG_INDEX);
    let mut snap = kvenginepb::Snapshot::default();
    snap.set_outer_end(kvengine::GLOBAL_SHARD_END_KEY.to_vec());
    snap.set_data_sequence(RAFT_INIT_LOG_INDEX);
    let props = new_initial_properties(region_id);
    snap.set_properties(props);
    change_set.set_snapshot(snap);
    kvengine::ChangeSet::new(change_set)
}

fn new_initial_properties(shard_id: u64) -> kvenginepb::Properties {
    let mut props = kvenginepb::Properties::default();
    props.set_shard_id(shard_id);
    props.set_keys(RepeatedField::from_slice(&[TERM_KEY.to_string()]));
    let mut val = Vec::with_capacity(8);
    val.put_u64_le(RAFT_INIT_LOG_TERM);
    props.set_values(RepeatedField::from_slice(&[val]));
    props
}

// Clear first region meta and prepare key.
pub fn clear_prepare_bootstrap_cluster(engines: &Engines, region: &metapb::Region) -> Result<()> {
    let region_id = region.get_id();
    let region_ver = region.get_region_epoch().get_version();
    let peer_id = region.get_peers().first().unwrap().id;
    let mut wb = rfengine::WriteBatch::new();
    wb.set_state(0, 0, 0, PREPARE_BOOTSTRAP_KEY, &[]);
    let state_key = region_state_key(region_ver);
    wb.set_state(peer_id, region_id, 0, state_key.chunk(), &[]);
    let raft_state_key = raft_state_key(region_ver);
    wb.set_state(peer_id, region_id, 0, raft_state_key.chunk(), &[]);
    wb.set_state(peer_id, region_id, 0, KV_ENGINE_META_KEY, &[]);
    wb.set_state(peer_id, region_id, 0, KV_ENGINE_META_DIFF_KEY, &[]);
    wb.set_state(peer_id, region_id, 0, KV_ENGINE_META_SNAP_DIFF_KEY, &[]);
    wb.truncate_raft_log(peer_id, region_id, 0, TRUNCATE_ALL_INDEX);
    engines.raft.write(wb)?;
    engines.kv.remove_shard(region_id);
    Ok(())
}

pub fn clear_prepare_bootstrap_state(engines: &Engines) -> Result<()> {
    let mut wb = rfengine::WriteBatch::new();
    wb.set_state(0, 0, 0, PREPARE_BOOTSTRAP_KEY, &[]);
    engines.raft.write(wb)?;
    Ok(())
}
