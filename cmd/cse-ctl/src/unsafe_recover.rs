// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use core::panic;
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    str::FromStr,
};

use api_version::ApiV2;
use bytes::{Buf, BufMut, BytesMut};
use clap::Args;
use kvproto::{
    metapb,
    metapb::PeerRole,
    raft_serverpb::{PeerState, RegionLocalState, StoreIdent},
};
use protobuf::Message;
use rfengine::{
    REGION_META_KEY_BYTE, RfEngine, STORE_IDENT_KEY, WriteBatch, raft_state_key, region_state_key,
};
use rfstore::store::{
    RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM, TERM_KEY, load_raft_engine_meta,
    peer_storage::{collect_prefix_regions, load_region_state},
    write_engine_meta_bytes,
};
use tikv_util::{
    codec::{bytes::decode_bytes, number::NumberEncoder},
    info,
};
#[derive(Args)]
pub struct UnsafeRecoverArgs {
    /// The main path of the raft engine.
    #[clap(long)]
    pub path: PathBuf,

    /// The sync wal dir of the raft engine.
    /// This must be set because `wal_sync_dir` is enabled in
    /// TiKV.
    #[clap(long)]
    pub wal_sync_dir: String,

    /// Skip the check of `wal_sync_dir`.
    /// This is useful when the `wal_sync_dir` is not set in TiKV.
    #[clap(long)]
    pub skip_wal_sync_dir: bool,

    /// Filter the region id to operate.
    #[clap(long)]
    pub region: Option<u64>,

    /// Filter the keyspace id regions to operate.
    #[clap(long)]
    pub keyspace: Option<u32>,

    /// Filter the table id regions to operate.
    #[clap(long)]
    pub table: Option<u64>,

    /// Destroy the target regions on the store.
    #[clap(long)]
    pub destroy: bool,

    /// create empty regions on the store.
    #[clap(long)]
    pub create_empty: Option<String>,

    /// Update the regions by removing the peers on the stores.
    /// Multiple stores are separated by ",".
    #[clap(long)]
    pub remove_stores: Option<String>,

    /// Commit the change.
    #[clap(long)]
    pub commit: bool,

    /// Operate on all regions.
    #[clap(long)]
    pub all: bool,
}

pub(crate) fn execute_unsafe_recover(args: UnsafeRecoverArgs) {
    let mut cfg = rfengine::RfEngineConfig::default();
    cfg.lightweight_backup = false;
    cfg.wal_sync_dir = if !args.skip_wal_sync_dir {
        // validate `wal_sync_dir` and wal files.
        if !wal_sync_dir_validate(&PathBuf::from(&args.wal_sync_dir)) {
            panic!("wal_sync_dir {} not exists", &args.wal_sync_dir);
        }
        args.wal_sync_dir
    } else {
        String::new()
    };
    let rf = rfengine::RfEngine::open(&args.path, &cfg, None, None).unwrap();
    if let Some(create_empty) = args.create_empty {
        create_empty_regions(&rf, create_empty, args.commit);
        return;
    }
    let target_regions = if let Some(region_id) = args.region {
        let region_to_peers = rf.get_region_peer_map();
        let &peer_id = region_to_peers.get(&region_id).unwrap();
        let cs = load_raft_engine_meta(&rf, peer_id).unwrap();
        let keyspace_id = ApiV2::get_u32_keyspace_id_by_key(cs.get_snapshot().get_outer_start())
            .unwrap_or_default();
        vec![(peer_id, region_id, cs.shard_ver, keyspace_id)]
    } else if let Some(keyspace_id) = args.keyspace {
        let mut keyspace = keyspace_id.to_be_bytes();
        keyspace[0] = b'x';
        let mut prefix = keyspace.to_vec();
        if let Some(table_id) = args.table {
            prefix.put_u8(b't');
            prefix.encode_i64(table_id as i64).unwrap();
        }
        collect_prefix_regions(&rf, &prefix).unwrap()
    } else if let Some(table_id) = args.table {
        let mut prefix = vec![b't'];
        prefix.encode_i64(table_id as i64).unwrap();
        collect_prefix_regions(&rf, &prefix).unwrap()
    } else if args.all {
        collect_prefix_regions(&rf, &[]).unwrap()
    } else {
        panic!("no filter specified");
    };
    let mut wb = WriteBatch::new();
    if let Some(failed_stores_str) = args.remove_stores {
        let failed_stores: HashSet<u64> = parse_stores(&failed_stores_str);
        for (peer_id, region_id, region_version, keyspace_id) in target_regions {
            let mut region_local_state = load_region_state(&rf, peer_id, region_version).unwrap();
            let region = region_local_state.mut_region();
            let old_region = region.clone();
            region.mut_region_epoch().conf_ver += 1;
            let mut new_peers = region.get_peers().to_vec();
            new_peers.retain(|peer| !failed_stores.contains(&peer.store_id));
            region.set_peers(new_peers.into());
            let region_state_val = region_local_state.write_to_bytes().unwrap();
            let region_state_key = region_state_key(region_version);
            wb.set_state(
                peer_id,
                region_id,
                keyspace_id,
                &region_state_key,
                &region_state_val,
            );
            info!(
                "update region from {:?} to {:?}",
                old_region,
                region_local_state.get_region(),
            );
        }
    } else if args.destroy {
        for (peer_id, region_id, region_version, keyspace_id) in target_regions {
            rf.iterate_peer_states(peer_id, false, |k, _| {
                wb.set_state(peer_id, region_id, keyspace_id, k, &[]);
                true
            });
            let mut region_local_state = load_region_state(&rf, peer_id, region_version).unwrap();
            region_local_state.state = PeerState::Tombstone;
            let region_state_val = region_local_state.write_to_bytes().unwrap();
            let region_state_key = region_state_key(region_version);
            wb.set_state(
                peer_id,
                region_id,
                keyspace_id,
                &region_state_key,
                &region_state_val,
            );
            wb.truncate_raft_log(peer_id, region_id, keyspace_id, u64::MAX);
            info!("destroy region {:?}", region_local_state);
        }
    } else {
        for (peer_id, _, region_version, _) in target_regions {
            let region_local_state = load_region_state(&rf, peer_id, region_version).unwrap();
            info!("region: {:?}", region_local_state.get_region());
        }
    }
    if args.commit {
        info!("commit changes");
        rf.write(wb).unwrap();
        info!("done")
    }
}

// Validate if the wal_sync_dir and wal files exists.
fn wal_sync_dir_validate(wal_sync_dir: &Path) -> bool {
    if wal_sync_dir.to_str().unwrap().is_empty() {
        return false;
    }
    let wal_files = vec![
        wal_sync_dir.join("0.wal"),
        wal_sync_dir.join("1.wal"),
        wal_sync_dir.join("2.wal"),
        wal_sync_dir.join("3.wal"),
    ];
    for wal_file in wal_files {
        if !wal_file.is_file() {
            return false;
        }
    }
    true
}

fn parse_stores(stores_str: &str) -> HashSet<u64> {
    stores_str
        .split(',')
        .map(|x| u64::from_str(x).unwrap())
        .collect()
}

fn create_empty_regions(rf: &RfEngine, empty_region_file: String, commit: bool) {
    let mut store_ident = StoreIdent::new();
    let data = rf.get_state(0, STORE_IDENT_KEY).unwrap_or_default();
    store_ident.merge_from_bytes(data.chunk()).unwrap();
    let region_map = rf.get_region_peer_map();
    let mut wb = WriteBatch::new();
    let data = std::fs::read(empty_region_file.as_str()).unwrap();
    let empty_regions: Vec<EmptyRegion> = serde_json::from_slice(&data).unwrap();
    for empty_region in &empty_regions {
        let peer_id = empty_region.peer_id;
        let region_id = empty_region.region_id;
        if rf
            .get_last_state_with_prefix(peer_id, &[REGION_META_KEY_BYTE])
            .is_some()
        {
            panic!("peer conflict peer_id: {}", peer_id);
        }
        if let Some(&old_peer_id) = region_map.get(&region_id) {
            panic!("region conflict {}, on peer {}", region_id, old_peer_id);
        }
        // reference rfstore::RaftState::marshal
        let raft_state_key = raft_state_key(empty_region.epoch_ver);
        let mut raft_state_val = BytesMut::with_capacity(40);
        raft_state_val.put_u64_le(RAFT_INIT_LOG_TERM);
        raft_state_val.put_u64_le(0);
        raft_state_val.put_u64_le(RAFT_INIT_LOG_INDEX);
        raft_state_val.put_u64_le(RAFT_INIT_LOG_INDEX);
        raft_state_val.put_u64_le(RAFT_INIT_LOG_INDEX);
        wb.set_state(
            peer_id,
            region_id,
            empty_region.keyspace_id,
            raft_state_key.chunk(),
            raft_state_val.chunk(),
        );
        let region_state_key = region_state_key(empty_region.epoch_ver);
        let region = empty_region.to_region(store_ident.store_id);
        info!("create region {:?}", &region);
        let mut region_local_state = RegionLocalState::new();
        region_local_state.set_region(region);
        let region_state_val = region_local_state.write_to_bytes().unwrap();
        wb.set_state(
            peer_id,
            region_id,
            empty_region.keyspace_id,
            region_state_key.chunk(),
            &region_state_val,
        );
        let engine_meta = empty_region.to_engine_meta();
        info!("create engine meta {:?}", &engine_meta);
        let engine_meta_data = engine_meta.write_to_bytes().unwrap();
        write_engine_meta_bytes(
            &mut wb,
            peer_id,
            region_id,
            empty_region.keyspace_id,
            &engine_meta_data,
        );
    }
    if commit {
        rf.write(wb).unwrap();
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
struct EmptyRegion {
    pub region_id: u64,
    pub store_id: u64,
    pub peer_id: u64,
    pub keyspace_id: u32,
    pub start_key: String,
    pub end_key: String,
    pub epoch_ver: u64,
    pub epoch_conf_ver: u64,
}

impl EmptyRegion {
    fn to_region(&self, store_id: u64) -> metapb::Region {
        let mut region = metapb::Region::new();
        region.set_id(self.region_id);
        let epoch = region.mut_region_epoch();
        epoch.set_version(self.epoch_ver);
        epoch.set_conf_ver(self.epoch_conf_ver);
        region.set_start_key(Self::encoded_key(self.start_key.as_bytes()));
        region.set_end_key(Self::encoded_key(self.end_key.as_bytes()));
        let mut peer = metapb::Peer::new();
        peer.set_id(self.peer_id);
        peer.set_store_id(store_id);
        peer.set_role(PeerRole::Voter);
        region.mut_peers().push(peer);
        region
    }

    fn to_engine_meta(&self) -> kvenginepb::ChangeSet {
        let mut cs = kvenginepb::ChangeSet::new();
        cs.set_shard_id(self.region_id);
        cs.set_shard_ver(self.epoch_ver);
        cs.set_sequence(RAFT_INIT_LOG_INDEX);
        let snap = cs.mut_snapshot();
        snap.set_outer_start(Self::raw_key(self.start_key.as_bytes()));
        snap.set_outer_end(Self::raw_key(self.end_key.as_bytes()));
        snap.set_data_sequence(RAFT_INIT_LOG_INDEX);
        let props = snap.mut_properties();
        props.set_shard_id(self.region_id);
        props.mut_keys().push(TERM_KEY.to_string());
        props
            .mut_values()
            .push(RAFT_INIT_LOG_TERM.to_le_bytes().to_vec());
        cs
    }

    fn encoded_key(str_key: &[u8]) -> Vec<u8> {
        hex::decode(str_key).unwrap()
    }

    fn raw_key(str_key: &[u8]) -> Vec<u8> {
        let encoded_key = Self::encoded_key(str_key);
        let mut slice = encoded_key.as_slice();
        decode_bytes(&mut slice, false).unwrap()
    }
}
