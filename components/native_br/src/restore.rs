// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use api_version::ApiV2;
use cloud_server::TikvServer;
use etcd_client::{Compare, CompareOp, Txn, TxnOp};
use kvengine::dfs::{DFSConfig, Dfs, new_dfs_from_config};
use kvproto::raft_serverpb::StoreIdent;
use pd_client::PdClient;
use protobuf::Message;
use rfengine::{RfEngine, STORE_IDENT_KEY, load_store_ident, region_state_key};
use rfenginepb::{ClusterBackupMeta, StoreBackupMeta};
use rfstore::store::{load_raft_engine_meta, load_region_state};
use security::{GetSecurityManager, SecurityConfig};
use tikv::config::TikvConfig;
use tikv_util::{
    box_try,
    config::{ReadableDuration, ReadableSize, ensure_dir_exist},
    debug, info, warn,
};

use crate::{
    backup::backup_file_full_path,
    common::{
        ReplayWalLogsContext, check_store_id_exists, collect_snapshot_meta_rlog_files,
        generate_etcd_connect_opt, get_latest_backup_meta, replay_wal_logs_from_backup,
    },
    error::{Error, Result},
    restore_keyspace::RESTORE_RFENGINE_CONCURRENCY,
};

const PD_ROOT_PATH: &str = "/pd";
const PD_CLUSTER_ID_PATH: &str = "/pd/cluster_id";
const MAX_TXN_OPTS: usize = 128; // Default configuration in etcd server.

struct MockPdClient {}

impl PdClient for MockPdClient {}

impl GetSecurityManager for MockPdClient {}

// Generate new store id by the ordered index of the store.
//
// Even we use the alloc_id in latest cluster backup meta as the base, it still
// has conflict possibility if new store added to the cluster after the backup.
// If conflict happens, specify the `delta` to avoid conflict. `alloc_id_base =
// alloc_id + delta`.
fn get_new_store_id(alloc_id_base: u64, stores: &[StoreBackupMeta], store_id: u64) -> u64 {
    let mut store_ids = stores
        .iter()
        .map(|store| store.store_id)
        .collect::<Vec<_>>();
    store_ids.sort_unstable();
    alloc_id_base + store_ids.into_iter().position(|id| id == store_id).unwrap() as u64 + 1
}

pub fn restore_tikv(
    config: &RestoreConfig,
    name: String,
    store_id: u64,
    new_store_id_delta: u64,
    path: &str,
) {
    let tag = format!("restore_tikv:{store_id}");
    let mut dfs_conf = config.dfs.clone();
    dfs_conf.read_only = true; // Set read only for safety.
    let dfs = new_dfs_from_config(dfs_conf);
    let cluster_backup = get_cluster_backup_meta(dfs.as_ref(), name);
    let dfs_clone = dfs.clone();
    // Get the alloc_id in the cluster latest backup meta.
    let latest_cluster_backup = dfs_clone
        .get_runtime()
        .block_on(get_latest_backup_meta(
            dfs_clone.as_ref(),
            cluster_backup.cluster_id,
        ))
        .unwrap_or_else(|_| cluster_backup.clone());
    let alloc_id = latest_cluster_backup.alloc_id + new_store_id_delta;

    // Pre-check all new store ids to avoid conflicts.
    cluster_backup.get_stores().iter().for_each(|store| {
        let new_store_id = get_new_store_id(alloc_id, cluster_backup.get_stores(), store.store_id);
        info!(
            "check new store id {} for store id {}",
            new_store_id, store.store_id
        );
        match check_store_id_exists(dfs_clone.as_ref(), new_store_id) {
            Ok(false) => {}
            Ok(true) => {
                panic!("new store id {} already exists", new_store_id);
            }
            Err(e) => {
                panic!("check store id failed: {}", e);
            }
        }
    });
    assert!(cluster_backup.is_lightweight);
    if store_id > 0 {
        let tikv_conf = generate_store_config(path, config.wal_target_size);
        let truncate_ts = cluster_backup.backup_ts;
        info!(
            "start replay wal for store {} with truncate_ts {}",
            store_id, truncate_ts
        );
        setup_raft_engine(
            &tag,
            store_id,
            alloc_id,
            &cluster_backup,
            &tikv_conf,
            dfs,
            config.lower_memory,
        )
        .unwrap();

        if new_store_id_delta > 0 {
            info!(
                "!!!ATTENTION!!! new_store_id_delta {}, please also specify this in restore pd.",
                new_store_id_delta
            );
        }
    }
}

fn put_store_ident(rf: &RfEngine, ident: &StoreIdent) {
    let val = ident.write_to_bytes().unwrap();
    let mut wb = rfengine::WriteBatch::new();
    wb.set_state(0, 0, 0, STORE_IDENT_KEY, val.as_slice());
    rf.write(wb).unwrap();
}

// Update all peers' store id in local region state to `new_store_id_base +
// old_store_id`. We should guarantee to use the same `new_store_id_base` for
// all full restores in other nodes.
fn update_local_region_state_store_id(
    rf: &RfEngine,
    cluster_backup: &ClusterBackupMeta,
    alloc_id: u64,
) {
    let mut wb = rfengine::WriteBatch::new();
    let region_to_peers = rf.get_region_peer_map();
    for (region_id, peer_id) in region_to_peers {
        debug!("region: {} peer: {}", region_id, peer_id);
        if let Some(cs) = load_raft_engine_meta(rf, peer_id) {
            let mut region_local_state = load_region_state(rf, peer_id, cs.shard_ver).unwrap();
            let peers = region_local_state.mut_region().mut_peers();

            // Collect all learner peers if exists to remove TiFlash replica.
            let to_remove_pos = peers
                .iter()
                .enumerate()
                .filter_map(|(idx, peer)| {
                    if peer.get_role() == kvproto::metapb::PeerRole::Learner {
                        Some(idx)
                    } else {
                        None
                    }
                })
                .collect::<Vec<usize>>();
            // Remove the peer in reverse order to avoid invalid pos index.
            for pos in to_remove_pos.into_iter().rev() {
                peers.remove(pos);
            }

            for peer in peers.iter_mut() {
                let new_store_id =
                    get_new_store_id(alloc_id, cluster_backup.get_stores(), peer.get_store_id());
                debug!(
                    "update peer {} store id from {} to {}",
                    peer.get_id(),
                    peer.get_store_id(),
                    new_store_id
                );
                peer.set_store_id(new_store_id);
            }
            let keyspace_id =
                ApiV2::get_u32_keyspace_id_by_key(region_local_state.get_region().get_start_key())
                    .unwrap_or_default();
            let region_state_key = region_state_key(cs.shard_ver);
            let region_state_val = region_local_state.write_to_bytes().unwrap();
            wb.set_state(
                peer_id,
                cs.shard_id,
                keyspace_id,
                region_state_key.as_ref(),
                region_state_val.as_slice(),
            );
        } else {
            warn!(
                "region: {} peer: {} has no raft local state",
                region_id, peer_id
            );
        }
    }
    if !wb.is_empty() {
        rf.write(wb).unwrap();
    }
}

fn setup_raft_engine_new_store_id(
    rf: &RfEngine,
    cluster_backup: &ClusterBackupMeta,
    store_id: u64,
    alloc_id: u64,
) {
    // Update all peers' store id in local region state.
    update_local_region_state_store_id(rf, cluster_backup, alloc_id);

    // Update store id in raft engine.
    if let Some(mut store_ident) = load_store_ident(rf) {
        let stores = cluster_backup.get_stores();
        let new_store_id = get_new_store_id(alloc_id, stores, store_id);
        info!(
            "replace store id {} with new store id {}",
            store_ident.get_store_id(),
            new_store_id
        );
        store_ident.set_store_id(new_store_id);
        put_store_ident(rf, &store_ident);
    };
}

fn generate_store_config(path: &str, wal_target_size: ReadableSize) -> TikvConfig {
    ensure_dir_exist(path).unwrap();

    let mut config = TikvConfig::default();
    config.raft_store.raftdb_path = path.to_string();
    config.rfengine.lightweight_backup = false;
    config.rfengine.target_file_size = wal_target_size;
    config.rfengine.cli_mode = true;
    config
}

fn setup_raft_engine(
    tag: &str,
    store_id: u64,
    alloc_id: u64,
    cluster_backup: &ClusterBackupMeta,
    conf: &TikvConfig,
    dfs: Arc<dyn Dfs>,
    lower_memory: bool,
) -> Result<()> {
    let rlog_files = collect_snapshot_meta_rlog_files(
        dfs.clone(),
        &dfs.get_prefix(),
        cluster_backup,
        store_id,
        None,
    )?;
    rfengine::lightweight_restore(
        store_id,
        None,
        Path::new(&conf.raft_store.raftdb_path),
        rlog_files.snap_epoch,
        rlog_files.snap_meta,
        rlog_files.snap_rlog,
        conf.rfengine.epoch_rotate_len,
    )
    .map_err(|x| Error::RfEngine(x))?;
    let snap_epoch = rlog_files.snap_epoch;

    let rf_engine = TikvServer::init_raft_engine(conf, None)?;
    rf_engine.set_engine_id(store_id);
    let cache_dir = if lower_memory {
        let dir = Path::new(&conf.storage.data_dir).join("cache");
        box_try!(fs::create_dir_all(&dir));
        Some(dir)
    } else {
        None
    };
    let ctx = ReplayWalLogsContext {
        pd_client: Arc::new(MockPdClient {}),
        dfs,
        store_id,
        cluster_backup,
        rf_engine: &rf_engine,
        complete_wal_chunks: true,
        full_restore: true,
        fetch_wal_timeout: Duration::from_secs(1), /* NOTE: Retry is unnecessary for full
                                                    * restoration. */
        cache_dir,
        wal_chunks_cache: None,
        from_archive: false,
    };
    replay_wal_logs_from_backup(tag, &ctx, snap_epoch)?;
    setup_raft_engine_new_store_id(&rf_engine, cluster_backup, store_id, alloc_id);

    Ok(())
}

pub fn restore_pd(config: RestoreConfig, name: String) {
    let dfs_conf = config.dfs.clone();
    let dfs = new_dfs_from_config(dfs_conf);
    let cluster_backup = get_cluster_backup_meta(dfs.as_ref(), name);
    // Get the alloc_id in the cluster latest backup meta.
    let latest_cluster_backup = dfs
        .get_runtime()
        .block_on(get_latest_backup_meta(
            dfs.as_ref(),
            cluster_backup.cluster_id,
        ))
        .unwrap_or_else(|_| cluster_backup.clone());
    if latest_cluster_backup.keyspace_meta.is_empty() {
        panic!("No keyspace meta in backup");
    }
    let new_alloc_id = latest_cluster_backup.alloc_id
        + cluster_backup.get_stores().len() as u64
        + config.new_store_id_delta
        + 1;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(restore_pd_keyspace_meta(
        &config,
        &cluster_backup,
        new_alloc_id,
    ));
}

pub fn get_cluster_backup_meta(dfs: &dyn Dfs, name: String) -> ClusterBackupMeta {
    let runtime = dfs.get_runtime();
    runtime.block_on(get_cluster_backup_meta_async(dfs, name))
}

pub async fn get_cluster_backup_meta_async(dfs: &dyn Dfs, name: String) -> ClusterBackupMeta {
    let backup_key = backup_file_full_path(dfs.get_prefix(), name.clone(), None);
    let data = dfs
        .get_object(backup_key, name, engine_traits::GetObjectOptions::default())
        .await
        .unwrap();
    let mut cluster_backup = ClusterBackupMeta::new();
    cluster_backup.merge_from_bytes(&data).unwrap();
    info!(
        "get_cluster_backup_meta: cluster_id {}, alloc_id {}, backup_ts {}, safe_ts {}, store cnt {}",
        cluster_backup.cluster_id,
        cluster_backup.alloc_id,
        cluster_backup.backup_ts,
        cluster_backup.safe_ts,
        cluster_backup.stores.len()
    );
    cluster_backup
}

// Mainly ref `recoverFromNewPDCluster` in `pd-recover`.
async fn restore_pd_keyspace_meta(
    config: &RestoreConfig,
    meta: &ClusterBackupMeta,
    new_alloc_id: u64,
) {
    let option = generate_etcd_connect_opt(&config.security).unwrap();
    let mut etcd_client = etcd_client::Client::connect(&config.pd.endpoints, Some(option))
        .await
        .unwrap();
    let mut txn_opts = Vec::with_capacity(4);
    let root_path = format!("{}/{}", PD_ROOT_PATH, meta.cluster_id);
    // recover cluster_id
    txn_opts.push(TxnOp::put(
        PD_CLUSTER_ID_PATH.as_bytes().to_vec(),
        meta.cluster_id.to_be_bytes().to_vec(),
        None,
    ));
    // recover alloc id
    let alloc_id_path = format!("{}/{}", root_path, "alloc_id");
    txn_opts.push(TxnOp::put(
        alloc_id_path.as_bytes().to_vec(),
        new_alloc_id.to_be_bytes().to_vec(),
        None,
    ));
    // recover meta of cluster
    let cluster_raft_path = format!("{}/{}", root_path, "raft");
    let cluster_meta = kvproto::metapb::Cluster {
        id: meta.cluster_id,
        ..Default::default()
    };
    txn_opts.push(TxnOp::put(
        cluster_raft_path.as_bytes().to_vec(),
        cluster_meta.write_to_bytes().unwrap(),
        None,
    ));
    // set raft bootstrap time
    let raft_bootstrap_time_path = format!(
        "{}/{}/{}",
        cluster_raft_path, "status", "raft_bootstrap_time"
    );
    let cur_nano = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    txn_opts.push(TxnOp::put(
        raft_bootstrap_time_path.as_bytes().to_vec(),
        cur_nano.to_be_bytes().to_vec(),
        None,
    ));
    let resp = etcd_client
        .txn(
            Txn::new()
                .when(
                    &[Compare::create_revision(
                        cluster_raft_path,
                        CompareOp::Equal,
                        0,
                    )][..],
                )
                .and_then(txn_opts),
        )
        .await
        .unwrap();
    if !resp.succeeded() {
        panic!(
            "Failed to restore pd keyspace meta, please JUST start new pd-server(s) without tikv nodes."
        );
    }
    // recover key space meta
    let meta_cnt = meta.keyspace_meta.len();
    let mut txn_opts = Vec::with_capacity(std::cmp::min(meta_cnt, MAX_TXN_OPTS));
    let mut idx = 0;
    for (key, value) in &meta.keyspace_meta {
        txn_opts.push(TxnOp::put(key.to_owned(), value.to_owned(), None));
        idx += 1;
        if txn_opts.len() == MAX_TXN_OPTS || idx == meta_cnt {
            // There is no batch put interface now, use txn to do batch.
            let batch_cnt = txn_opts.len();
            let resp = etcd_client
                .txn(Txn::new().and_then(txn_opts))
                .await
                .unwrap();
            if !resp.succeeded() {
                panic!(
                    "Fail to restore pd meta, cur idx {} batch {} total {}",
                    idx - batch_cnt,
                    batch_cnt,
                    meta_cnt
                );
            }
            txn_opts = Vec::with_capacity(std::cmp::min(meta_cnt - idx, MAX_TXN_OPTS));
        }
    }
    info!(
        "Restore PD {} meta data(revision: {}) of cluster {} successfully",
        meta_cnt, meta.meta_revision, meta.cluster_id
    );
}

const DEFAULT_WAL_TARGET_SIZE: ReadableSize = ReadableSize::mb(512);
pub const DEFAULT_TIMEOUT_WAIT_FLUSH: ReadableDuration = ReadableDuration::minutes(10);
pub const DEFAULT_TIMEOUT_RESTORE_SNAPSHOT: ReadableDuration = ReadableDuration::minutes(10);
pub const DEFAULT_RESTORE_SNAPSHOT_CONCURRENCY: usize = 16;
pub const DEFAULT_TIMEOUT_FETCH_WAL: ReadableDuration = ReadableDuration::minutes(10);
pub const DEFAULT_TIMEOUT_SPLIT_REGIONS: ReadableDuration = ReadableDuration::secs(30);
pub const DEFAULT_TIMEOUT_PD_CONTROL: ReadableDuration = ReadableDuration::secs(10);
pub const DEFAULT_RESTORE_MAX_RETRY: usize = 30;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct RestoreConfig {
    pub pd: pd_client::Config,
    pub security: SecurityConfig,
    pub dfs: DFSConfig,
    pub wal_target_size: ReadableSize,
    pub new_store_id_delta: u64,

    /// The timeout for waiting the flush of mem-tables.
    pub timeout_wait_flush: ReadableDuration,
    /// The timeout for the requests of restoring snapshots to TiKV servers.
    pub timeout_restore_snapshot: ReadableDuration,
    /// The concurrency number of restoring snapshots.
    pub restore_snapshot_concurrency: usize,
    /// The maximum number of retries for the process from split regions to
    /// restore snapshots.
    pub max_retry: usize,
    /// Tolerance error count during setup raft engine for lightweight
    /// restoration.
    pub tolerate_err: usize,
    /// Whether to strictly tolerate the specified errors (e.g.
    /// RfengineHttpError) ONLY.
    ///
    /// By default, all kinds of errors can be tolerated.
    pub strict_tolerate: bool,
    /// The timeout for retrying fetch wal chunk from store.
    pub timeout_fetch_wal: ReadableDuration,
    /// The timeout for split regions of target keyspace.
    pub timeout_split_regions: ReadableDuration,
    /// The timeout for PD control.
    pub timeout_pd_control: ReadableDuration,
    /// Concurrency on number of TiKV stores when perform restoration.
    pub store_concurrency: usize,
    /// Coarse split regions when the target region cover more than the factor *
    /// number of regions in backup.
    pub coarse_split_regions_factor: usize,

    /// Whether to use lower memory (by cache remote WAL chunks in local disks).
    pub lower_memory: bool,

    /// Whether to cache compressed or decompressed WAL chunks in object cache.
    ///
    /// Note: this only takes effect when compression type of rlogs is lz4.
    pub cache_for_decompressed_wal_chunks: bool,
}

impl Default for RestoreConfig {
    fn default() -> Self {
        Self {
            pd: Default::default(),
            security: Default::default(),
            dfs: Default::default(),
            wal_target_size: DEFAULT_WAL_TARGET_SIZE,
            new_store_id_delta: 0,
            timeout_wait_flush: DEFAULT_TIMEOUT_WAIT_FLUSH,
            timeout_restore_snapshot: DEFAULT_TIMEOUT_RESTORE_SNAPSHOT,
            restore_snapshot_concurrency: DEFAULT_RESTORE_SNAPSHOT_CONCURRENCY,
            timeout_fetch_wal: DEFAULT_TIMEOUT_FETCH_WAL,
            timeout_split_regions: DEFAULT_TIMEOUT_SPLIT_REGIONS,
            timeout_pd_control: DEFAULT_TIMEOUT_PD_CONTROL,
            max_retry: DEFAULT_RESTORE_MAX_RETRY,
            tolerate_err: 0,
            strict_tolerate: false,
            store_concurrency: RESTORE_RFENGINE_CONCURRENCY,
            coarse_split_regions_factor: 64, // It's about 32 GiB when region size is 500 MiB.
            lower_memory: false,
            cache_for_decompressed_wal_chunks: false,
        }
    }
}

#[cfg(feature = "testexport")]
impl RestoreConfig {
    pub fn default_for_test() -> Self {
        Self {
            tolerate_err: 1,
            lower_memory: Self::use_lower_memory(),
            cache_for_decompressed_wal_chunks: Self::cache_for_decompressed_wal_chunks(),
            ..Default::default()
        }
    }

    pub fn use_lower_memory() -> bool {
        use rand::Rng;
        if rand::thread_rng().gen_bool(0.5) {
            info!("lower memory enabled");
            true
        } else {
            false
        }
    }

    pub fn cache_for_decompressed_wal_chunks() -> bool {
        use rand::Rng;
        if rand::thread_rng().gen_bool(0.5) {
            info!("cache_for_decompressed_wal_chunks enabled");
            true
        } else {
            false
        }
    }
}
