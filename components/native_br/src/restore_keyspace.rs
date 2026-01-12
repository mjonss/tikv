// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Cow,
    cmp,
    collections::hash_map::Entry,
    default::Default,
    fmt, fs, mem, ops,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
    thread,
    time::Duration,
};

use api_version::{api_v2::KEYSPACE_PREFIX_LEN, ApiV2};
use bytes::Bytes;
use cloud_encryption::{EncryptionKey, MasterKey};
use cloud_server::{RestoreShardResponse, TikvServer};
use collections::{HashMap, HashMapExt, HashSet};
pub use engine_traits::ObjectCache;
use engine_traits::ObjectCacheWithHook;
use file_system::{IoRateLimitMode, IoRateLimiter};
use http::{header, Request};
use hyper::Body;
use itertools::Itertools;
use kvengine::{
    dfs::{self, Dfs, FileType},
    ia::util::IaConfig,
    limiter::StoreLimiter,
    table::{BoundedDataSet, DataBound, InnerKey},
    IdAllocator, IdVer, LoadTableFilterFn, PrepareOpts, ShardMeta, ShardRange, ShardStats,
    ShardTag, ENCRYPTION_KEY, GLOBAL_SHARD_END_KEY,
};
use kvenginepb as pb;
use kvproto::{metapb, metapb::PeerRole, raft_serverpb::MergeState};
use pb::VectorIndex;
use pd_client::{pd_control::PdControl, PdClient};
use protobuf::Message;
use raft::eraftpb;
use rfengine::{convert_to_decompressed_wal_chunk, RfEngine};
use rfenginepb::ClusterBackupMeta;
use rfstore::store::{
    load_raft_engine_meta, parse_raft_cmd, rlog, state::RaftState, ApplyMsgs, PdIdAllocator,
    PeerTag, PreprocessContext, RegionIdVer, StoreMsg,
};
use security::SecurityConfig;
use slog_global::{debug, error, info, warn};
use tempdir::TempDir;
use tikv::{config::TikvConfig, storage::mvcc::Key};
use tikv_util::{
    backoff::ExponentialBackoff, box_err, box_try, http::CONTENT_TYPE_PROTOBUF,
    merge_range::MergeRanges, mpsc, retry::try_wait_result_async, time::Instant, HandyRwLock,
};
use tokio::{runtime::Runtime, sync::Semaphore};

use crate::{
    archive::{
        get_cluster_backup_file_and_meta, get_incremental_backup_with_name, get_not_found_files,
        ArchiveReader, StoreMeta,
    },
    common::{
        collect_snapshot_meta_rlog_files, load_peer_raft_state, now, replay_wal_logs,
        retain_sst_files, send_request_to_store, RawRegion, RegionMetaGetter, ReplayWalLogsContext,
        StorePeer, TableFile,
    },
    error::{
        Error,
        Error::{MetaNotFound, RetryLimitExceeded},
        Result,
    },
    limiter::{SnapshotSize, ThroughputLimiter},
    lock::LockResolver,
    metrics::{
        NATIVE_BR_RESTORED_DATA_SIZE, NATIVE_BR_RESTORED_KV_SIZE,
        NATIVE_BR_RESTORE_PENDING_DATA_SIZE,
    },
    restore::RestoreConfig,
    rfengine_cache::RfEngineCache,
    step,
    tiflash::remove_tiflash_replica_of_keyspace,
};

const WORKING_PATH_PREFIX: &str = "keyspace-restore";
const ZSTD_COMPRESSION_LEVEL: &str = "5"; // The same as ZSTD_COMPRESSION_LEVEL_FOR_REMOTE.

// TODO: get replicas from keyspace config to handle 5 replicas in the future.
const REPLICAS: usize = 3; // Number of replicas for each region.

const RESOLVE_LOCKS_BATCH_SIZE: usize = 1024;

const SPLIT_REGIONS_BATCH_SIZE: usize = 256;
const SPLIT_REGIONS_TIMEOUT_PER_KEY: Duration = Duration::from_millis(100);

pub const RESTORE_RFENGINE_CONCURRENCY: usize = 3;

#[derive(Clone, Debug, Default)]
pub struct RestoredKeyspace {
    pub keyspace_id: u32,
    pub target_keyspace_id: u32,
    pub ts: u64,
    pub restore_bytes: u64,
    pub tolerated_err: usize,
}

#[derive(Clone, Debug, Default)]
pub struct RestoredSnapshots {
    pub count: u64,
    pub restore_bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub struct RestoredShard {
    pub shard_id: u64,
    pub restore_bytes: u64,
}

/// RestoreStep is the steps of restore process.
///
/// Some steps are performed in retry loop, and will be reported multiple times.
///
/// Must keep the order of enums the same as they happen, then reporters can
/// avoid the progress jumping back and forth.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum RestoreStep {
    Pending,
    Init,
    InstantBackup,
    RemoveTiFlashReplicas,
    LoadBackupMeta,
    ExtractBackupShards,
    ResolveLocks,
    FlushShards,
    TruncateTs,
    SplitRegions,
    AlignRegions,
    RestoreSnapshotsToServers,
    RestoreSnapshotsFinished,
    RetainSstFiles,
    Finalize,
}

pub trait ReportRestoreStepTrait: Send + Sync {
    fn report_step(&self, step: RestoreStep);

    fn report_pending_restore_data_size(&self, _data_size: i64) {}
}

pub fn restore_keyspace_with_cfg(
    config: RestoreConfig,
    keyspace_name: &str,
    target_keyspace_name: &str,
    backup_name: &str,
    working_path: Option<PathBuf>,
    dfs: Arc<dyn Dfs>,
    pd_client: Arc<dyn PdClient>,
    runtime: &Runtime,
    truncate_ts: Option<u64>,
    reporter: Arc<dyn ReportRestoreStepTrait>,
    object_cache: Option<ObjectCache>,
    limiter: Option<Arc<ThroughputLimiter>>,
    rfengine_cache: Option<RfEngineCache>,
) -> Result<RestoredKeyspace> {
    let mut pd_control = PdControl::new(config.pd.clone(), pd_client.get_security_mgr())?;
    pd_control.set_timeout(config.timeout_pd_control.0);
    let keyspace_id = {
        let keyspace = runtime.block_on(pd_control.get_keyspace_by_name(keyspace_name))?;
        assert_eq!(keyspace_name, keyspace.name);
        keyspace.id
    };
    step!("Keyspace name {keyspace_name}'s id is {keyspace_id}");

    let target_keyspace_id = if keyspace_name != target_keyspace_name {
        let target_id = {
            let target_keyspace =
                runtime.block_on(pd_control.get_keyspace_by_name(target_keyspace_name))?;
            assert_eq!(target_keyspace_name, target_keyspace.name);
            target_keyspace.id
        };
        step!("Target keyspace name {target_keyspace_name}'s id is {target_id}");
        target_id
    } else {
        reporter.report_step(RestoreStep::RemoveTiFlashReplicas);
        if let Err(e) = runtime.block_on(remove_tiflash_replica_of_keyspace(
            keyspace_id,
            &pd_control,
            pd_client.clone(),
        )) {
            error!("Keyspace {keyspace_id} remove tiflash replica err {:?}", e);
            return Err(e);
        }
        step!("Removed keyspace {keyspace_name} tiflash replica");
        keyspace_id
    };

    restore_keyspace(
        keyspace_id,
        target_keyspace_id,
        backup_name,
        working_path,
        dfs,
        config,
        pd_client,
        runtime,
        truncate_ts,
        reporter,
        object_cache,
        limiter,
        rfengine_cache,
    )
}

pub fn restore_keyspace(
    keyspace_id: u32,
    target_keyspace_id: u32,
    backup_name: &str,
    working_path: Option<PathBuf>,
    dfs: Arc<dyn Dfs>,
    config: RestoreConfig,
    pd_client: Arc<dyn PdClient>,
    runtime: &Runtime,
    truncate_ts: Option<u64>,
    reporter: Arc<dyn ReportRestoreStepTrait>,
    object_cache: Option<ObjectCache>,
    limiter: Option<Arc<ThroughputLimiter>>,
    rfengine_cache: Option<RfEngineCache>,
) -> Result<RestoredKeyspace> {
    let keyspace_tag = make_keyspace_tag(keyspace_id, target_keyspace_id);

    let inplace_restore = target_keyspace_id == keyspace_id;
    if !inplace_restore
        && (ApiV2::is_default_keyspace(keyspace_id)
            || ApiV2::is_default_keyspace(target_keyspace_id))
    {
        error!(
            "{} restore other keyspace from/to default keyspace",
            keyspace_tag
        );
        return Err(Error::RestoreWithDefaultKeyspace);
    }

    let (working_path, _guard) = match working_path {
        Some(path) => (path, None),
        None => {
            let temp_dir = box_try!(TempDir::new(WORKING_PATH_PREFIX));
            let path = temp_dir.path().to_path_buf();
            (path, Some(temp_dir))
        }
    };

    let (keyspace_start, keyspace_end) = ApiV2::get_keyspace_range_by_id(keyspace_id);
    let (target_keyspace_start, target_keyspace_end) =
        ApiV2::get_keyspace_range_by_id(target_keyspace_id);

    reporter.report_step(RestoreStep::LoadBackupMeta);
    let (cluster_backup, archive_reader) =
        match get_cluster_backup_file_and_meta(dfs.as_ref(), backup_name.to_owned()) {
            Ok((_, backup_meta)) => (backup_meta, None),
            Err(dfs::Error::NoSuchKey(err)) => {
                warn!(
                    "The cluster backup meta {} is not found: {:?}, try the archive reader",
                    backup_name.to_owned(),
                    err
                );
                let backup_file =
                    get_incremental_backup_with_name(dfs.get_prefix(), backup_name.to_owned());
                let backup_date = backup_file.created_at().date_naive();
                let archive_reader = ArchiveReader::new(dfs.clone(), &backup_date)?;
                let backup_meta = archive_reader
                    .read_meta_file()
                    .map_err(|_| MetaNotFound(backup_file.id()))?;
                (backup_meta, Some(archive_reader))
            }
            Err(err) => {
                error!("get cluster backup meta {} failed: {:?}", backup_name, err);
                return Err(Error::DfsError(err));
            }
        };
    let truncate_ts = if archive_reader.is_none() {
        truncate_ts
    } else {
        Some(cluster_backup.backup_ts)
    };
    step!(
        "Start restore keyspace {} from backup <{}>, lightweight:{} range:[{},{}), target_range:[{},{}), backup ts:{}, safe ts:{} truncate ts:{:?}",
        keyspace_tag,
        backup_name,
        cluster_backup.is_lightweight,
        log_wrappers::hex_encode_upper(keyspace_start),
        log_wrappers::hex_encode_upper(keyspace_end),
        log_wrappers::hex_encode_upper(&target_keyspace_start),
        log_wrappers::hex_encode_upper(&target_keyspace_end),
        cluster_backup.backup_ts,
        cluster_backup.safe_ts,
        truncate_ts,
    );
    info!("{} restore config: {:?}", keyspace_tag, config);
    debug!(
        "Keyspace {} get cluster backup meta: {:?}",
        keyspace_tag, cluster_backup
    );

    reporter.report_step(RestoreStep::ExtractBackupShards);
    if truncate_ts.is_some() {
        check_backup_meta_ts(&cluster_backup, truncate_ts.unwrap())?;
    }
    let truncate_ts = truncate_ts.unwrap_or(cluster_backup.backup_ts);
    let mut cluster = BackupCluster::new(
        &cluster_backup,
        working_path,
        pd_client.clone(),
        dfs.clone(),
        config.clone(),
        keyspace_id,
        target_keyspace_id,
        truncate_ts,
        false,
        archive_reader,
        false,
        object_cache,
        rfengine_cache,
    )?;
    step!(
        "Keyspace {} restore {} shards from backup",
        keyspace_tag,
        cluster.shards_count()
    );

    // Resolve locks
    // In order to avoid data inconsistency, it is necessary to commit all locks of
    // secondary keys of a committed transaction that are in the backup.
    // If this is not done, uncommitted secondary keys may be dropped when the
    // committed record of primary key is compacted after restoration.
    // This is likely to happen because the timestamp of restored data is generally
    // smaller than the GC safepoint at that time.
    reporter.report_step(RestoreStep::ResolveLocks);
    let lock_resolver = LockResolver::new(
        &keyspace_tag,
        cluster.get_kvengine(),
        truncate_ts,
        RESOLVE_LOCKS_BATCH_SIZE,
        cluster.get_shard_meta_getter(),
    );
    let resolved_locks = runtime.block_on(lock_resolver.resolve_locks())?;
    cluster.add_shards_need_flush(&resolved_locks.resolved_shards);
    step!(
        "Keyspace {keyspace_tag} resolve {} locks / {} lock_txn_files of shards {:?}",
        resolved_locks.total_normal_locks_cnt,
        resolved_locks.total_lock_txn_files_cnt,
        resolved_locks.resolved_shards
    );

    // Trigger initial flush & flush mem-table to S3
    // NOTE: `cluster.shards` is NOT available before `flush_shards` finished.
    reporter.report_step(RestoreStep::FlushShards);
    let flush_cnt = cluster.flush_shards(config.timeout_wait_flush.0)?;
    step!("Keyspace {keyspace_tag} flush {flush_cnt} shards");

    reporter.report_step(RestoreStep::TruncateTs);
    let truncate_ts_cnt = runtime.block_on(cluster.truncate_ts())?;
    step!(
        "Keyspace {} truncate {} shards to ts {}",
        keyspace_tag,
        truncate_ts_cnt,
        cluster.truncate_ts
    );

    // Pre-split & scatter target keyspace regions.
    reporter.report_step(RestoreStep::SplitRegions);
    cluster.pre_split_and_scatter_regions(&config, runtime)?;

    // Restore snapshot.
    let mut success_ranges = MergeRanges::default();
    let mut bo = Backoff::new(ExponentialBackoff::new(
        Duration::from_secs(1),
        Duration::from_secs(10),
        config.max_retry,
    ));
    let mut restore_bytes = 0;
    // NOTE: must call `bo::on_error` before every "continue".
    loop {
        let mut target_regions = match runtime.block_on(get_target_regions(
            pd_client.as_ref() as &dyn PdClient,
            &target_keyspace_start,
            &target_keyspace_end,
        )) {
            Ok(regions) => regions,
            Err(err) => {
                warn!("get target regions failed: {:?}, retry", err);
                bo.on_error(err)?;
                continue;
            }
        };

        let target_regions_total_cnt = target_regions.len();
        if !success_ranges.is_empty() {
            target_regions.retain(|r| !success_ranges.covered(&r.raw_start, &r.raw_end));
        }
        step!(
            "Keyspace {} target regions: {} ({} in total)",
            keyspace_tag,
            target_regions.len(),
            target_regions_total_cnt
        );

        // Align target regions.
        reporter.report_step(RestoreStep::AlignRegions);
        let aligned_regions = cluster.align_target_regions(target_regions);
        let trimmed_shards_cnt =
            runtime.block_on(cluster.trim_over_bound_shards(&aligned_regions))?;
        step!(
            "Keyspace {} align {} backup shards to {} target regions and trim {} over bound shards",
            keyspace_tag,
            cluster.shards_count(),
            aligned_regions.len(),
            trimmed_shards_cnt
        );

        // Gather sstables for target regions.
        let (target_shards, sstables_cnt) = cluster.gather_all_tables(aligned_regions)?;
        step!(
            "Keyspace {} gather {} SSTables for {} target regions",
            keyspace_tag,
            sstables_cnt,
            target_shards.len(),
        );

        reporter.report_step(RestoreStep::RestoreSnapshotsToServers);
        let snapshots = cluster.generate_snapshots(target_shards);
        let snapshots_count = snapshots.len();
        let ret = restore_snapshots(
            &keyspace_tag,
            runtime,
            pd_client.clone(),
            &reporter,
            snapshots,
            &mut success_ranges,
            &config,
            &mut bo,
            &limiter,
        )?;
        reporter.report_step(RestoreStep::RestoreSnapshotsFinished);
        restore_bytes += ret.restore_bytes;
        step!(
            "Keyspace {keyspace_tag} restore {snapshots_count} regions, result: {:?}",
            ret,
        );

        if success_ranges.covered(&target_keyspace_start, &target_keyspace_end) {
            break;
        }
        bo.on_error(Error::RestoreSnapshot)?;
        step!(
            "Keyspace {keyspace_tag} restore retry {}",
            bo.current_attempts()
        );
    }

    reporter.report_step(RestoreStep::RetainSstFiles);
    let files = cluster.get_all_shard_files(None, None);
    match retain_sst_files(files, dfs.clone()) {
        Err(e) => {
            return Err(box_err!(
                "Keyspace {} fail to retain restored sst files in s3, {:?}",
                keyspace_tag,
                e
            ));
        }
        Ok(file_cnt) => {
            step!("Keyspace {keyspace_tag} retain {file_cnt} files successfully");
        }
    }

    let restore_ret = RestoredKeyspace {
        keyspace_id,
        target_keyspace_id,
        ts: truncate_ts,
        restore_bytes,
        tolerated_err: cluster.tolerated_err(),
    };
    Ok(restore_ret)
}

fn check_backup_meta_ts(meta: &ClusterBackupMeta, truncate_ts: u64) -> Result<()> {
    if truncate_ts < meta.safe_ts || truncate_ts > meta.backup_ts {
        return Err(Error::PitrTsError(
            truncate_ts,
            meta.safe_ts,
            meta.backup_ts,
        ));
    }
    Ok(())
}

#[derive(Default, Clone)]
pub struct BackupShard {
    pub region_id: u64,
    pub store_id: u64,
    pub peer_id: u64,
    pub need_flush: bool,
    pub meta: ShardMeta,
    pub(crate) raw_meta: Option<pb::ChangeSet>, // used for kv engine recovery only.
    raft_state: RaftState,
}

impl fmt::Debug for BackupShard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackupShard")
            .field("tag", &format!("{}", self.tag()))
            .field("region_id", &self.region_id)
            .field("store_id", &self.store_id)
            .field("peer_id", &self.peer_id)
            .field("need_flush", &self.need_flush)
            .field("meta", &self.meta.to_change_set())
            .field("raw_meta", &self.raw_meta)
            .field("raft_state", &self.raft_state)
            .finish()
    }
}

#[cfg(test)]
impl PartialEq for BackupShard {
    fn eq(&self, other: &Self) -> bool {
        self.region_id == other.region_id
            && self.store_id == other.store_id
            && self.peer_id == other.peer_id
            && self.need_flush == other.need_flush
            && self.meta.to_change_set() == other.meta.to_change_set()
            && self.raw_meta == other.raw_meta
            && self.raft_state == other.raft_state
    }
}

impl BackupShard {
    pub fn tag(&self) -> ShardTag {
        ShardTag::new(self.store_id, IdVer::new(self.region_id, self.ver()))
    }

    pub fn ver(&self) -> u64 {
        self.meta.ver
    }

    pub fn id_ver(&self) -> IdVer {
        IdVer::new(self.region_id, self.ver())
    }

    pub fn start(&self) -> &[u8] {
        &self.meta.range.outer_start
    }

    pub fn end(&self) -> &[u8] {
        &self.meta.range.outer_end
    }

    pub fn inner_start(&self) -> InnerKey<'_> {
        self.meta.range.inner_start()
    }

    pub fn inner_end(&self) -> InnerKey<'_> {
        self.meta.range.inner_end()
    }

    pub fn inner_key_off(&self) -> usize {
        self.meta.inner_key_off
    }

    pub fn table_version(&self) -> u64 {
        self.meta.base_version + self.meta.data_sequence
    }

    pub fn reset_columnar(&mut self) {
        self.meta.clear_columnar_related_meta();
    }
}

impl BoundedDataSet for BackupShard {
    fn data_bound(&self) -> DataBound<'_> {
        self.meta.range.data_bound()
    }
}

#[derive(Debug, PartialEq)]
struct AlignedRegion {
    target_region: RawRegion,
    backup_shards_id: Vec<u64>,
}

pub struct BackupCluster {
    tag: String,
    path: PathBuf,
    pd_client: Arc<dyn PdClient>,
    dfs: Arc<dyn Dfs>,
    security_conf: SecurityConfig,
    master_key: MasterKey,
    keyspace_id: u32,
    target_keyspace_id: u32,
    keyspace_start: Vec<u8>,
    keyspace_end: Vec<u8>,
    truncate_ts: u64,
    archive_reader: Option<ArchiveReader>,

    // store_id -> rf_engine.
    raft_engines: HashMap<u64, RfEngine>,
    // store_id -> store_config.
    store_configs: HashMap<u64, TikvConfig>,
    // must be `Some` after `new()`.
    kv_engine: Option<kvengine::Engine>,

    // region_id -> shard.
    shards: HashMap<u64, BackupShard>,
    // region_id -> raw_meta, used for kv_engine recover.
    raw_metas: HashMap<u64, pb::ChangeSet>,
    // store_id -> Vec<shard_id>.
    store_shards: HashMap<u64, Vec<u64>>,
    // shard_id -> StorePeer{store_id, peer_id}. Used to resolve locks.
    shards_store_map: Option<Arc<HashMap<u64 /* shard_id */, StorePeer>>>,
    // shard_id sorted by start_key.
    sorted_shards: Vec<u64>,
    // Set<shard_id>.
    shards_need_flush: HashSet<u64>,
    // Set<shard_id>.
    shards_need_truncate: HashSet<u64>,
    // Set<shard_id>.
    shards_need_reset_columnar: HashSet<u64>,

    meta_applier: Option<Arc<MetaApplier>>,

    meta_sender: Option<mpsc::Sender<StoreMsg>>,

    tolerated_err: usize,

    // Wrap by `Option` to detect use before filled.
    txn_chunk_ids_in_wal: Option<Vec<u64>>,

    // `false` for restoration.
    // `true` for "check_table" to read data from backup directly.
    load_all_tables: bool,

    id_allocator: Arc<dyn IdAllocator>,
    // NOTE: New members need to check if need to clear in reset_keyspace.
}

impl Drop for BackupCluster {
    fn drop(&mut self) {
        self.stop_engines();
    }
}

impl BackupCluster {
    pub fn reset_keyspace(&mut self, keyspace_id: u32, target_keyspace_id: u32) -> Result<()> {
        let (keyspace_start, keyspace_end) = ApiV2::get_keyspace_range_by_id(keyspace_id);
        self.tag = make_keyspace_tag(keyspace_id, target_keyspace_id);
        self.keyspace_id = keyspace_id;
        self.target_keyspace_id = target_keyspace_id;
        self.keyspace_start = keyspace_start;
        self.keyspace_end = keyspace_end;

        let kv_engine = self.kv_engine.take().unwrap();
        kv_engine.close();
        drop(kv_engine);

        if let Some(sender) = self.meta_sender.take() {
            sender.send(StoreMsg::Stop).unwrap();
        }
        self.meta_applier.take();
        self.sorted_shards.clear();
        self.raw_metas.clear();
        self.store_shards.clear();
        self.shards_store_map = None;
        self.shards.clear();
        self.shards_need_flush.clear();
        self.shards_need_truncate.clear();
        self.shards_need_reset_columnar.clear();
        self.tolerated_err = 0;
        self.txn_chunk_ids_in_wal = None;
        self.load_shards()?;
        self.collect_txn_chunks_in_wal()?;

        self.check_all_shard_files()?;

        for store_id in self.get_all_stores_id() {
            self.setup_kv_engine(store_id)?;
        }
        Ok(())
    }

    pub fn new(
        cluster_meta: &ClusterBackupMeta,
        path: PathBuf,
        pd_client: Arc<dyn PdClient>,
        dfs: Arc<dyn Dfs>,
        restore_conf: RestoreConfig,
        keyspace_id: u32,
        target_keyspace_id: u32,
        truncate_ts: u64,
        archiving: bool,
        archive_reader: Option<ArchiveReader>,
        load_all_tables: bool, // `true` for "check_table" ONLY.
        object_cache: Option<ObjectCache>,
        rfengine_cache: Option<RfEngineCache>,
    ) -> Result<BackupCluster> {
        let (keyspace_start, keyspace_end) = if archiving {
            (Vec::default(), GLOBAL_SHARD_END_KEY.to_vec())
        } else {
            ApiV2::get_keyspace_range_by_id(keyspace_id)
        };
        let security_conf = restore_conf.security.clone();
        let master_key = dfs.get_runtime().block_on(security_conf.new_master_key());
        let tag = make_keyspace_tag(keyspace_id, target_keyspace_id);
        let mut cluster = Self {
            tag: tag.clone(),
            path,
            pd_client: pd_client.clone(),
            dfs,
            security_conf,
            master_key,
            keyspace_id,
            target_keyspace_id,
            keyspace_start,
            keyspace_end,
            truncate_ts,
            archive_reader,
            shards: Default::default(),
            raw_metas: Default::default(),
            raft_engines: Default::default(),
            store_configs: Default::default(),
            kv_engine: Default::default(),
            store_shards: Default::default(),
            shards_store_map: Default::default(),
            sorted_shards: Default::default(),
            shards_need_flush: Default::default(),
            shards_need_truncate: Default::default(),
            shards_need_reset_columnar: Default::default(),
            meta_applier: None,
            meta_sender: None,
            tolerated_err: 0,
            txn_chunk_ids_in_wal: None,
            load_all_tables,
            id_allocator: Arc::new(PdIdAllocator::new(pd_client)),
        };

        let mut store_configs = HashMap::with_capacity(cluster_meta.stores.len());

        // If the cluster backup already has tolerate error, we can't tolerate more
        // errors in restoration.
        let mut tolerate_err = if cluster_meta.get_tolerated_err() > 0 {
            0
        } else {
            restore_conf.tolerate_err
        };

        let is_error_can_tolerate = |err: &Error| -> bool {
            if matches!(
                err,
                Error::RfengineHttpRequestError(_)
                    | Error::PdError(pd_client::Error::StoreTombstone(_))
            ) {
                true
            } else if !restore_conf.strict_tolerate {
                crate::metrics::NATIVE_BR_RESTORE_ERROR
                    .with_label_values(&["setup_raft_engine"])
                    .inc();
                true
            } else {
                false
            }
        };
        let mut raft_engines: HashMap<u64, RfEngine> = Default::default();
        let mut errors = vec![];
        let mut cluster_tolerated_err = 0;
        let mut recv_restore_rfengine =
            |result_rx: &mpsc::Receiver<Result<(u64, TikvConfig, RfEngine)>>| {
                // We can tolerate one store failure for lightweight restoration during fetch
                // latest wal chunk from store.
                match result_rx.recv().unwrap() {
                    Err(err) => errors.push(err),
                    Ok((store_id, store_config, rf_engine)) => {
                        store_configs.insert(store_id, store_config);
                        raft_engines.insert(store_id, rf_engine);
                        info!(
                            "Keyspace {} setup raft engine for store {} done",
                            cluster.tag(),
                            store_id
                        );
                    }
                }
            };
        let (result_tx, result_rx) = tikv_util::mpsc::bounded(cluster_meta.stores.len());
        let mut msg_count = 0;

        for store in &cluster_meta.stores {
            let store_id = store.get_store_id();
            if !store.keyspace_size.is_empty() && store.keyspace_size.get(&keyspace_id).is_none() {
                info!(
                    "Keyspace {} skip store {} which has no data",
                    cluster.tag(),
                    store_id
                );
                continue;
            }
            let tag = format!("{tag}:{store_id}");
            let store_config = cluster.generate_store_config(store_id);

            let tx = result_tx.clone();
            let cluster_backup_meta = cluster_meta.clone();
            let pd_client = cluster.pd_client.clone();
            let dfs = cluster.dfs.clone();
            let keyspace_tag = cluster.tag().to_string();
            let archive_store_meta = if let Some(archive_reader) = &cluster.archive_reader {
                let store_meta = archive_reader.get_store_wal_rlog_meta(store_id)?;
                Some((archive_reader.get_start_date().to_string(), store_meta))
            } else {
                None
            };
            let restore_conf_cp = restore_conf.clone();
            let object_cache = object_cache.clone();
            let rfengine_cache = rfengine_cache.clone();
            std::thread::spawn(move || {
                let res = BackupCluster::setup_raft_engine(
                    &tag,
                    store_id,
                    keyspace_id,
                    &cluster_backup_meta,
                    &store_config,
                    pd_client,
                    dfs,
                    archiving,
                    archive_store_meta,
                    &restore_conf_cp,
                    object_cache,
                    rfengine_cache,
                    truncate_ts,
                );
                if let Err(err) = &res {
                    warn!(
                        "Keyspace {} setup raft engine for store {} failed: {:?}",
                        keyspace_tag, store_id, err,
                    );
                }
                let _ = tx.send(res.map(|rf_engine| (store_id, store_config, rf_engine)));
            });

            if msg_count < restore_conf.store_concurrency {
                msg_count += 1;
            } else {
                // Do NOT return on error to avoid thread leaky.
                recv_restore_rfengine(&result_rx);
            }
        }
        for _ in 0..msg_count {
            recv_restore_rfengine(&result_rx);
        }

        for err in errors {
            if is_error_can_tolerate(&err) {
                if tolerate_err == 0 {
                    return Err(err);
                }
                warn!(
                    "Keyspace {} setup raft engine failed, tolerate it: {:?}",
                    cluster.tag(),
                    err
                );
                tolerate_err -= 1;
                cluster_tolerated_err += 1;
            } else {
                return Err(err);
            }
        }

        cluster.raft_engines = raft_engines;
        cluster.store_configs = store_configs;
        cluster.tolerated_err = cluster_tolerated_err;
        cluster.load_shards()?;
        cluster.collect_txn_chunks_in_wal()?;

        if archiving {
            return Ok(cluster);
        }
        cluster.check_all_shard_files()?;

        for store_id in cluster.get_all_stores_id() {
            cluster.setup_kv_engine(store_id)?;
        }
        Ok(cluster)
    }

    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub(crate) fn setup_raft_engine_for_lightweight(
        tag: &str,
        store_id: u64,
        keyspace_ids: Vec<u32>,
        cluster_backup: &ClusterBackupMeta,
        conf: &TikvConfig,
        pd_client: Arc<dyn PdClient>,
        dfs: Arc<dyn Dfs>,
        archiving: bool,
        archive_store_meta: Option<(String, StoreMeta)>, // archive date, archive store meta
        restore_conf: &RestoreConfig,
        object_cache: Option<ObjectCache>,
    ) -> Result<RfEngine> {
        let object_cache_no_hook: Option<ObjectCacheWithHook> = object_cache.map(Into::into);
        let rlog_files = if let Some((date, store_meta)) = &archive_store_meta {
            ArchiveReader::read_store_rlog_files(dfs.as_ref(), date, store_meta)?
        } else {
            collect_snapshot_meta_rlog_files(
                dfs.clone(),
                &dfs.get_prefix(),
                cluster_backup,
                store_id,
                object_cache_no_hook.as_ref(),
            )?
        };
        rfengine::lightweight_restore(
            store_id,
            (!archiving).then_some(keyspace_ids),
            Path::new(&conf.raft_store.raftdb_path),
            rlog_files.snap_epoch,
            rlog_files.snap_meta,
            rlog_files.snap_rlog,
            conf.rfengine.epoch_rotate_len,
        )
        .map_err(|x| Error::RfEngine(x))?;

        let rf_engine = TikvServer::init_raft_engine(conf, None)?;

        // When archiving, the dfs should have complete wal chunks.
        let complete_wal_chunks = archiving;
        let cache_dir = if restore_conf.lower_memory {
            let dir = Path::new(&conf.storage.data_dir).join("cache");
            box_try!(fs::create_dir_all(&dir));
            Some(dir)
        } else {
            None
        };
        let wal_chunks_cache = if restore_conf.cache_for_decompressed_wal_chunks {
            object_cache_no_hook.map(|cache| {
                let hook =
                    |chunk| convert_to_decompressed_wal_chunk(chunk).map_err(|e| e.to_string());
                cache.with_hook(Arc::new(hook))
            })
        } else {
            object_cache_no_hook
        };
        let ctx = ReplayWalLogsContext {
            pd_client,
            dfs,
            store_id,
            cluster_backup,
            rf_engine: &rf_engine,
            complete_wal_chunks,
            full_restore: false,
            fetch_wal_timeout: restore_conf.timeout_fetch_wal.0,
            cache_dir,
            wal_chunks_cache,
            from_archive: archive_store_meta.is_some(),
        };
        replay_wal_logs(tag, ctx, archive_store_meta, rlog_files.snap_epoch)?;
        Ok(rf_engine)
    }

    pub fn setup_raft_engine(
        tag: &str,
        store_id: u64,
        keyspace_id: u32,
        cluster_backup: &ClusterBackupMeta,
        conf: &TikvConfig,
        pd_client: Arc<dyn PdClient>,
        dfs: Arc<dyn Dfs>,
        archiving: bool,
        archive_store_meta: Option<(String, StoreMeta)>, // archive date, archive store meta
        restore_conf: &RestoreConfig,
        object_cache: Option<ObjectCache>,
        rfengine_cache: Option<RfEngineCache>,
        truncate_ts: u64,
    ) -> Result<RfEngine> {
        if let (Some(cache), None) = (&rfengine_cache, &archive_store_meta) {
            let raft_db_path = Path::new(&conf.raft_store.raftdb_path);
            if let Some(cached_engine) =
                cache.get_for_keyspace(store_id, keyspace_id, truncate_ts, raft_db_path)
            {
                info!(
                    "Keyspace {} use cached raft engine for store {}",
                    tag, store_id
                );
                return Ok(cached_engine);
            }
        }
        if !cluster_backup.is_lightweight {
            return Err(Error::BackupError(
                "only support lightweight restore".to_string(),
            ));
        }
        Self::setup_raft_engine_for_lightweight(
            tag,
            store_id,
            vec![keyspace_id],
            cluster_backup,
            conf,
            pd_client,
            dfs,
            archiving,
            archive_store_meta,
            restore_conf,
            object_cache,
        )
    }

    // Take all raw metas to release memory.
    fn take_raw_metas(&mut self, store_id: u64) -> HashMap<u64, pb::ChangeSet> {
        let shards = self.store_shards.get(&store_id).unwrap().to_owned();
        let mut raw_metas = HashMap::with_capacity(shards.len());
        for shard_id in shards {
            raw_metas.insert(shard_id, self.raw_metas.remove(&shard_id).unwrap());
        }
        raw_metas
    }

    fn get_shards_need_flush_and_truncate(&self) -> Vec<u64> {
        self.shards_need_flush
            .union(&self.shards_need_truncate)
            .cloned()
            .collect()
    }

    fn setup_kv_engine(&mut self, store_id: u64) -> Result<()> {
        let conf = self.store_configs.get(&store_id).unwrap();
        let rf_engine = self.raft_engines.get(&store_id).unwrap();
        let recoverer = rfstore::store::RecoverHandler::new(rf_engine.clone());

        if self.kv_engine.is_none() {
            let io_rate_limiter =
                Arc::new(IoRateLimiter::new(IoRateLimitMode::WriteOnly, true, true));
            io_rate_limiter
                .set_io_rate_limit(conf.storage.io_rate_limit.max_bytes_per_sec.0 as usize);
            // TODO: enable flow control to avoid OOM during recovery.
            let store_limiter = Arc::new(StoreLimiter::dummy());

            let mut meta_iter = MetaIterator::new(store_id, Vec::new(), HashMap::default());
            let (kv_engine, sender, receiver) = TikvServer::init_kv_engine(
                self.pd_client.clone(),
                conf,
                self.dfs.clone(),
                io_rate_limiter,
                store_limiter,
                &mut meta_iter,
                recoverer.clone(),
                true,
                self.master_key.clone(),
                self.pd_client.get_security_mgr(),
            )?;

            let encryption_key = self.get_keyspace_exported_encryption_key().map(|exported| {
                kv_engine
                    .get_master_key()
                    .decrypt_encryption_key(&exported)
                    .unwrap()
            });

            // Move shards to meta_applier to apply meta change in `flush_mem_table`.
            // Move back after flush finished.
            let mut shards = mem::take(&mut self.shards);
            for shard in shards.values_mut() {
                if self.shards_need_reset_columnar.contains(&shard.region_id) {
                    info!(
                        "Keyspace {} shard {} reset columnar",
                        self.keyspace_id, shard.region_id
                    );
                    shard.reset_columnar();
                }
            }
            let meta_applier = Arc::new(MetaApplier::new(
                self.keyspace_id,
                kv_engine.clone(),
                encryption_key,
                shards,
                receiver,
            ));
            self.meta_applier = Some(meta_applier.clone());
            self.meta_sender = Some(sender);
            thread::spawn(move || {
                meta_applier.run();
            });

            self.kv_engine = Some(kv_engine);
        }

        // Load L0 & LOCK_CF of all shards for scanning locks.
        // TODO: L0 & LOCK_CF with `max_ts` < `safe_ts` can be skipped.
        // (Currently there is no `max_ts` for LOCK_CF of L0)
        let store_shards = self.store_shards.get(&store_id).unwrap().to_owned();
        let raw_metas = self.take_raw_metas(store_id);
        let kv_engine = self.kv_engine.as_mut().unwrap();
        if !store_shards.is_empty() {
            let mut meta_iter =
                MetaIterator::new(kv_engine.get_engine_id(), store_shards, raw_metas);
            let (mut metas, _) = kvengine::EngineCore::read_meta(&mut meta_iter)?;
            info!(
                "Keyspace {} kv_engine load {} shards in restore keyspace",
                self.keyspace_id,
                metas.len()
            );

            // Clear columnar related meta in metas if the shard need reset columnar.
            for meta in metas.values_mut() {
                if self.shards_need_reset_columnar.contains(&meta.id) {
                    info!(
                        "Keyspace {} shard {} clear columnar related meta",
                        self.keyspace_id, meta.id
                    );
                    meta.clear_columnar_related_meta();
                }
            }

            let table_filter: Option<LoadTableFilterFn> = if self.load_all_tables {
                None
            } else {
                // Load the following tables from DFS:
                // 1. Tables of shards need truncate, to get the `max_ts`.
                // 2. Tables has locks (L0 or LOCK_CF) to replay commit requests.
                let shard_need_truncate = self.shards_need_truncate.clone();
                let table_filter = move |shard_id: u64, tb: &kvengine::FileMeta| -> bool {
                    shard_need_truncate.contains(&shard_id) || tb.has_locks()
                };
                Some(Arc::new(table_filter))
            };
            tikv_util::init_task_local_sync(|| {
                kv_engine.load_shards(metas, recoverer, table_filter)
            })?;
        }
        Ok(())
    }

    fn stop_engines(&mut self) {
        let raft_engines = mem::take(&mut self.raft_engines);
        for rf in raft_engines.into_values() {
            rf.stop_worker(true);
            drop(rf);
        }
        if let Some(kv_engine) = self.kv_engine.take() {
            // `self.kv_engine` will be None when `BackupCluster` fail to initialize.
            kv_engine.close();
            drop(kv_engine);
        }
        // Receiver cannot get err msg even close_sender, send Stop msg instead.
        if let Some(sender) = &self.meta_sender {
            sender.send(StoreMsg::Stop).unwrap();
        }
    }

    fn generate_store_config(&self, store_id: u64) -> TikvConfig {
        let (store_path, rf_engine_path) = (
            self.path.join(store_id.to_string()),
            self.path.join(store_id.to_string()).join("raft"),
        );

        let mut config = TikvConfig::default();
        config.storage.data_dir = store_path.to_str().unwrap().to_string();
        config.raft_store.raftdb_path = rf_engine_path.to_str().unwrap().to_string();
        config.dfs.zstd_compression_level = ZSTD_COMPRESSION_LEVEL.to_string();
        config.rocksdb.max_background_jobs = 2;
        config.rocksdb.max_sub_compactions = 1;
        config.security = self.security_conf.clone();

        config.rfengine.lightweight_backup = false;
        config.kvengine.ia = IaConfig::disabled(); // TODO: Use IA ?

        TikvServer::init_config(config).get_current()
    }

    fn collect_keyspace_shards(
        tag: &str,
        store_id: u64,
        rf: &RfEngine,
        keyspace_start: &[u8],
        keyspace_end: &[u8],
    ) -> Result<Vec<BackupShard>> {
        let region_peers = rf.get_region_peer_map();
        let mut prefix_shards = vec![];
        let mut no_meta_peers = vec![];
        for (region_id, peer_id) in region_peers {
            if region_id == 0 {
                continue;
            }
            let meta = match load_raft_engine_meta(rf, peer_id) {
                Some(meta) => meta,
                None => {
                    debug!(
                        "region {} peer {} has no rf_engine meta",
                        region_id, peer_id
                    );
                    if no_meta_peers.len() < 5 {
                        no_meta_peers.push((region_id, peer_id));
                    }
                    continue;
                }
            };
            assert_eq!(region_id, meta.shard_id);

            let snap = meta.get_snapshot();
            if keyspace_start <= snap.get_outer_start() && snap.get_outer_end() <= keyspace_end {
                let shard = Self::create_backup_shard(rf, store_id, region_id, peer_id, meta)?;
                debug!("{} collect_keyspace_shards", shard.tag(); "inner_key_off" => shard.inner_key_off());
                prefix_shards.push(shard);
            }
        }
        if !no_meta_peers.is_empty() {
            warn!(
                "{} collect_keyspace_shards: peers has no meta: {:?}",
                tag, no_meta_peers
            );
        }
        Ok(prefix_shards)
    }

    fn collect_full_shards(store_id: u64, rf: &RfEngine) -> Result<Vec<BackupShard>> {
        let region_peers = rf.get_region_peer_map();
        let mut prefix_shards = vec![];
        for (region_id, peer_id) in region_peers {
            if region_id == 0 {
                continue;
            }
            let meta = match load_raft_engine_meta(rf, peer_id) {
                Some(meta) => meta,
                None => {
                    warn!(
                        "region {} peer {} has no rf_engine meta",
                        region_id, peer_id
                    );
                    continue;
                }
            };
            assert_eq!(region_id, meta.shard_id);

            let shard = Self::create_backup_shard(rf, store_id, region_id, peer_id, meta)?;
            prefix_shards.push(shard);
        }
        Ok(prefix_shards)
    }

    pub fn is_full_range(&self) -> bool {
        self.keyspace_start.is_empty() && self.keyspace_end.eq(GLOBAL_SHARD_END_KEY)
    }

    fn create_backup_shard(
        rf: &RfEngine,
        store_id: u64,
        region_id: u64,
        peer_id: u64,
        mut meta: kvenginepb::ChangeSet,
    ) -> Result<BackupShard> {
        // Fix backups before pull/1732 in which the `data_sequence` in meta is not
        // correct.
        if meta.has_parent() {
            meta.mut_snapshot().data_sequence = meta.sequence;
        }

        let snap = meta.get_snapshot();
        let raft_last_index = rf.get_last_index(peer_id);
        let need_flush_mem_table = raft_last_index.map_or(false, |last_idx| {
            assert!(last_idx >= snap.get_data_sequence());
            last_idx > snap.get_data_sequence()
        });
        let need_initial_flush = meta.has_parent();

        let raft_state = load_peer_raft_state(rf, peer_id, meta.shard_ver).ok_or_else(|| -> Error {
            let states = rf.get_peer_all_states(peer_id, false);
            error!(
                "failed to load peer raft state, store_id: {}, region_id: {}, peer_id: {}, state: {:?}",
                store_id, region_id, peer_id, states
            );

            box_err!("failed to load peer raft state, store_id: {}, region_id: {} peer_id: {}",
                store_id, region_id, peer_id)
        })?;

        let shard = BackupShard {
            region_id,
            store_id,
            peer_id,
            need_flush: need_flush_mem_table || need_initial_flush,
            meta: ShardMeta::new(rf.get_engine_id(), &meta),
            raw_meta: Some(meta),
            raft_state,
        };
        debug!(
            "create_backup_shard: raft_last_index {:?}, {:?}",
            raft_last_index, shard
        );
        Ok(shard)
    }

    fn load_shards(&mut self) -> Result<()> {
        let tag = self.tag();
        // Will need to retry when `get_leader_shards_and_preprocess` produce new shards
        // after preprocess.
        let mut leader_shards = HashMap::default();
        let mut retry = 0_usize;
        let is_full_range = self.is_full_range();
        loop {
            let mut all_shards = HashMap::default();
            for (&store_id, rf_engine) in &self.raft_engines {
                let store_shards = if is_full_range {
                    Self::collect_full_shards(store_id, rf_engine)?
                } else {
                    Self::collect_keyspace_shards(
                        tag,
                        store_id,
                        rf_engine,
                        &self.keyspace_start,
                        &self.keyspace_end,
                    )?
                };
                all_shards.insert(store_id, store_shards);
            }

            // Check whether backup is empty (for this keyspace).
            // Empty backup should be invalid or before the creation of this
            // keyspace, so the restore is not allowed.
            if all_shards.iter().all(|(_, shards)| shards.is_empty()) {
                return Err(Error::BackupEmptyForKeyspace(self.keyspace_id));
            }

            match self.get_leader_shards_and_preprocess(all_shards)? {
                (_, true) => {
                    if retry > 20 {
                        // Should never happen. Just a safe guard to avoid infinite loop.
                        return Err(box_err!(
                            "get_leader_shards_and_preprocess retry too many times"
                        ));
                    }
                    retry += 1;
                    continue;
                }
                (shards, false) => {
                    let _ = mem::replace(&mut leader_shards, shards);
                    break;
                }
            }
        }

        if is_full_range {
            // Reserve all the leader shards for archiving.
            let mut shards: HashMap<u64, BackupShard> = HashMap::default();
            let sorted_shards = leader_shards
                .values()
                .sorted_by(|a, b| a.start().cmp(b.start()))
                .map(|x| x.region_id)
                .collect();
            shards.extend(leader_shards.into_iter().map(|(k, v)| (k.id, v)));
            self.shards = shards;
            self.sorted_shards = sorted_shards;
        } else {
            // The `find_intact_shards` in the following methods can only handle the shards
            // of a single keyspace.
            let (intact_shards, sorted_shards_id) =
                Self::handle_overlapping_shards(self.tag(), leader_shards)?;
            self.shards = intact_shards;
            self.sorted_shards = sorted_shards_id;
        }
        info!("{}: shards cnt {}", self.tag(), self.shards.len());
        debug!(
            "Keyspace {} BackupCluster.load_shards: {:?}",
            self.tag(),
            self.shards
        );
        debug!(
            "Keyspace {} BackupCluster.sorted_shards: {:?}",
            self.tag(),
            self.sorted_shards
        );

        for (&shard_id, shard) in self.shards.iter_mut() {
            self.store_shards
                .entry(shard.store_id)
                .and_modify(|ids| ids.push(shard_id))
                .or_insert_with(|| vec![shard_id]);

            // Take `raw_meta`s out from `shards`, as `shards` will be moved into
            // `MetaApplier` for flush shards.
            self.raw_metas
                .insert(shard_id, shard.raw_meta.take().unwrap());
        }

        self.shards_store_map = Some(Arc::new(
            self.shards
                .iter()
                .map(|(&shard_id, shard)| {
                    (
                        shard_id,
                        StorePeer {
                            store_id: shard.store_id,
                            peer_id: shard.peer_id,
                        },
                    )
                })
                .collect(),
        ));
        self.shards_need_flush.extend(
            self.shards
                .iter()
                .filter(|(_, shard)| shard.need_flush)
                .map(|(&shard_id, _)| shard_id),
        );
        self.shards_need_truncate.extend(
            self.shards
                .iter()
                .filter(|(_, shard)| shard.meta.max_ts >= self.truncate_ts)
                .map(|(&shard_id, _)| shard_id),
        );

        self.verify_shards()
    }

    // Should be called after load_shard & collect_txn_chunks_in_wal.
    pub fn get_all_shard_files(
        &self,
        skip_shards: Option<HashSet<u64>>,
        skip_keyspace_ids: Option<HashSet<u32>>,
    ) -> Vec<TableFile> {
        let mut files = vec![];
        for shard in self.shards.values() {
            if let Some(skip_shards) = &skip_shards {
                if skip_shards.contains(&shard.region_id) {
                    info!(
                        "skip shard {} files {}",
                        shard.tag(),
                        shard.meta.all_files().len()
                    );
                    continue;
                }
            }
            if let Some(skip_keyspace_ids) = &skip_keyspace_ids {
                if let Some(keyspace_id) = ApiV2::get_u32_keyspace_id_by_key(shard.start()) {
                    if skip_keyspace_ids.contains(&keyspace_id) {
                        let keyspace_end_key = ApiV2::get_keyspace_end_by_id(keyspace_id);
                        if shard.end() <= keyspace_end_key.as_slice() {
                            info!(
                                "skip keyspace {} shard {} files {}",
                                keyspace_id,
                                shard.tag(),
                                shard.meta.all_files().len()
                            );
                            continue;
                        }
                    }
                }
            }
            files.extend(shard.meta.all_files().iter().map(|(&id, fm)| TableFile {
                id,
                ftype: fm.file_type,
                shard_id: shard.region_id,
            }));
            files.extend(
                shard
                    .meta
                    .all_txn_chunk_ids()
                    .into_iter()
                    .map(|id| TableFile {
                        id,
                        ftype: FileType::TxnChunk,
                        shard_id: shard.region_id,
                    }),
            );
            if shard.meta.schema.is_valid() {
                files.push(TableFile {
                    id: shard.meta.schema.file_id(),
                    ftype: FileType::Schema,
                    shard_id: shard.region_id,
                });
            }
            for vec_idx in &shard.meta.vector_indexes {
                for idx_file in vec_idx.get_files() {
                    files.push(TableFile {
                        id: idx_file.get_id(),
                        ftype: FileType::VectorIndex,
                        shard_id: shard.region_id,
                    });
                }
            }
            // TODO: Add more file types here. e.g. ftspacked/ftsdedicated.
        }
        files.extend(
            self.txn_chunk_ids_in_wal
                .as_ref()
                .unwrap()
                .iter()
                .map(|&id| TableFile {
                    id,
                    ftype: FileType::TxnChunk,
                    shard_id: 0,
                }),
        );
        files
    }

    #[inline]
    fn is_file_type_need_archive(ftype: FileType) -> bool {
        // TODO: Add more file types here. e.g. ftspacked/ftsdedicated.
        ftype != FileType::Columnar && ftype != FileType::VectorIndex
    }

    // Should be called after load_shard and before setup_kv_engine.
    pub fn check_all_shard_files(&mut self) -> Result<()> {
        let files = self.get_all_shard_files(None, None);
        let mut shards_need_reset_columnar = HashSet::default();
        if let Some(archive_reader) = &self.archive_reader {
            let mut not_found_files = get_not_found_files(self.dfs.clone(), files)?;
            shards_need_reset_columnar = not_found_files
                .iter()
                .filter(|f| !Self::is_file_type_need_archive(f.ftype))
                .map(|f| f.shard_id)
                .collect::<HashSet<_>>();
            not_found_files.retain(|f| !shards_need_reset_columnar.contains(&f.shard_id));
            archive_reader.restore_files(not_found_files)?;
        }
        step!(
            "Keyspace {} need reset columnar shards: {:?}",
            self.tag(),
            shards_need_reset_columnar
        );
        self.shards_need_reset_columnar = shards_need_reset_columnar;
        Ok(())
    }

    fn get_leader_shards_and_preprocess(
        &self,
        all_shards: HashMap<u64, Vec<BackupShard>>,
    ) -> Result<(
        HashMap<IdVer, BackupShard>, // leader_shards
        bool,                        // has_new_peer
    )> {
        let shards_cnt = all_shards.values().map(|x| x.len()).sum::<usize>() / REPLICAS;
        let mut shard_peers: HashMap<IdVer, Vec<BackupShard>> = HashMap::with_capacity(shards_cnt);

        for shard in all_shards.into_values().flatten() {
            shard_peers.entry(shard.id_ver()).or_default().push(shard);
        }
        let mut leader_shards = HashMap::with_capacity(shard_peers.len());
        for (&id_ver, peers) in shard_peers.iter_mut() {
            // Since we enabled raft pre-vote, the correct leader must have the max term and
            // then max last index.
            // Also sort by store_id (descending) to get peer with smallest store id as
            // leader, to make the result determined and improve efficiency for
            // retry.
            peers.sort_by(|a, b| {
                let term_last_a = (
                    a.raft_state.get_term(),
                    a.raft_state.get_last_index(),
                    !a.store_id,
                );
                let term_last_b = (
                    b.raft_state.get_term(),
                    b.raft_state.get_last_index(),
                    !b.store_id,
                );
                term_last_a.cmp(&term_last_b)
            });
            let mut leader = peers.pop().unwrap();
            if leader.raft_state.get_commit() < leader.raft_state.get_last_index() {
                // If we tolerate error, the actual leader maybe missing, it's possible that the
                // most up-to-date follower's last_index is already committed.
                // It's possible that the original cluster eventually not committed to
                // the last index, and the last index contains commit primary key timestamp
                // lower than backup ts. But the probability is very low and the
                // data is still consistent.
                let mut hard_state = leader.raft_state.get_hard_state();
                hard_state.set_commit(leader.raft_state.get_last_index());
                leader.raft_state.set_hard_state(&hard_state);
            }
            leader_shards.insert(id_ver, leader);
        }

        let mut has_new_peer = false;
        let mut ver_changed = vec![];
        for (&id_ver, shard) in leader_shards.iter_mut() {
            // Preprocess if needed.
            if shard.raft_state.get_commit() > shard.raft_state.get_last_preprocessed_index() {
                info!(
                    "Keyspace {} shard {} need preprocess, commit {}, last_preprocessed_index {}",
                    self.tag(),
                    shard.region_id,
                    shard.raft_state.get_commit(),
                    shard.raft_state.get_last_preprocessed_index()
                );
                let rf_engine = self.raft_engines.get(&shard.store_id).unwrap();
                if self.preprocess_shard(rf_engine, shard)? {
                    has_new_peer = true;
                }

                if shard.ver() != id_ver.ver {
                    ver_changed.push(id_ver);
                }
            }
        }

        for id_ver in ver_changed {
            let shard = leader_shards.remove(&id_ver).unwrap();
            leader_shards.insert(shard.id_ver(), shard);
        }

        Ok((leader_shards, has_new_peer))
    }

    /// Handle overlapping shards.
    /// Shards overlap will happen when some followers had not finished split or
    /// merge.
    fn handle_overlapping_shards(
        tag: &str,
        mut leader_shards: HashMap<IdVer, BackupShard>,
    ) -> Result<(
        HashMap<u64, BackupShard>, // intact_shards
        Vec<u64>,                  // Vec<shard_id> sorted by BackupShard.start()
    )> {
        let mut shards_map: HashMap<InnerKey<'_>, Vec<&BackupShard>> = HashMap::default();
        for shard in leader_shards.values() {
            shards_map
                .entry(shard.inner_start())
                .or_default()
                .push(shard);
        }
        // Increasingly sort as `find_intact_shards` will try the last shard first.
        for shards in shards_map.values_mut() {
            shards.sort_by(|a, b| a.ver().cmp(&b.ver()));
        }
        let Some(intact_shards) = Self::find_intact_shards(&shards_map) else {
            error!("{}: shards not intact for keyspace", tag; "shards" => ?shards_map);
            return Err(box_err!("shards not intact for keyspace"));
        };
        info!("{}: intact shards: {:?}", tag, intact_shards); // TODO: debug log.
        let mut shards: HashMap<u64, BackupShard> = HashMap::default();
        let mut sorted_shards: Vec<u64> = vec![];
        for id_ver in intact_shards {
            let shard = leader_shards.remove(&id_ver).unwrap();
            shards.insert(id_ver.id, shard);
            sorted_shards.push(id_ver.id);
        }

        Ok((shards, sorted_shards))
    }

    // Consider as a DAG, where the start/end keys are the nodes, and the shards are
    // the edges.
    fn find_intact_shards(
        shards_map: &HashMap<InnerKey<'_>, Vec<&BackupShard>>,
    ) -> Option<Vec<IdVer>> {
        let shards = shards_map.get(&InnerKey::default())?;

        let mut stack: Vec<&BackupShard> = Vec::with_capacity(shards_map.len());
        stack.extend(shards);
        let mut visited: HashMap<InnerKey<'_>, &BackupShard> = HashMap::default();

        while let Some(curr) = stack.pop() {
            let next_key = curr.inner_end();
            if next_key.deref() == GLOBAL_SHARD_END_KEY {
                let mut edges = vec![];
                let mut curr = curr;
                loop {
                    edges.push(curr.id_ver());
                    let prev_key = curr.inner_start();
                    if prev_key.is_empty() {
                        break;
                    } else {
                        curr = *visited.get(&prev_key).unwrap()
                    }
                }

                edges.reverse();
                return Some(edges);
            }

            if let Entry::Vacant(e) = visited.entry(next_key) {
                if let Some(shards) = shards_map.get(&next_key) {
                    stack.extend(shards);
                    e.insert(curr);
                    continue;
                }
            }
        }
        None
    }

    fn preprocess_shard(
        &self,
        rf_engine: &RfEngine,
        old_shard: &mut BackupShard,
    ) -> Result<bool /* has_new_peer */> {
        let mut preprocessor = PeerPreprocessor::new(rf_engine, old_shard);

        // Get raft logs.
        let low_idx = preprocessor.preprocessed_index + 1;
        let high_idx = old_shard.raft_state.get_commit() + 1;
        info!(
            "Keyspace {} shard {} preprocess Raft log [{}, {}), raft_state {:?}",
            self.tag(),
            old_shard.tag(),
            low_idx,
            high_idx,
            preprocessor.raft_state,
        );

        let mut entries = Vec::with_capacity((high_idx.saturating_sub(low_idx)) as usize);
        let peer_id = old_shard.peer_id;
        rf_engine
            .fetch_raft_entries_to(peer_id, low_idx, high_idx, None, &mut entries)
            .map_err(|e| -> Error {
                let stats = rf_engine.get_peer_stats(peer_id);
                let truncated_state = rfstore::store::load_raft_truncated_state(rf_engine, peer_id);
                box_err!(
                    "{} entries unavailable err: {:?}, stats {:?}, truncated_state: {:?}, low: {}, high {}",
                    old_shard.tag(),
                    e,
                    stats,
                    truncated_state,
                    low_idx,
                    high_idx
                )
            })?;

        // Prepare context.
        let mut raft_wb = rfengine::WriteBatch::new();
        let mut remove_dependents = Vec::new();
        let mut apply_msgs = ApplyMsgs::default();
        let raft_cfg = rfstore::store::Config::default();
        let mut destroying = std::collections::HashSet::default();
        let mut ctx = PreprocessContext {
            store_id: old_shard.store_id,
            kv: None,
            raft: rf_engine,
            raft_wb: &mut raft_wb,
            remove_dependents: &mut remove_dependents,
            apply_msgs: &mut apply_msgs,
            cfg: &raft_cfg,
            router: None,
            destroying: &mut destroying,
        };

        // Preprocess
        let mut preprocess_ref = preprocessor.as_ref();
        for entry in &entries {
            preprocess_ref.preprocess_committed_entry(&mut ctx, entry);
        }

        // Apply to rf_engine
        preprocess_ref
            .raft_state
            .set_last_preprocessed_index(*preprocess_ref.preprocessed_index);
        preprocess_ref.write_raft_state(&mut ctx); // update `last_preprocessed_index` to rf_engine.
        rf_engine.apply(ctx.raft_wb); // Do not need persist.

        // Get new shard.
        // TODO: In-place update `old_shard`, other than get a new one from rf_engine.
        let meta = load_raft_engine_meta(rf_engine, peer_id).unwrap();
        let new_shard = Self::create_backup_shard(
            rf_engine,
            old_shard.store_id,
            old_shard.region_id,
            peer_id,
            meta,
        )?;
        info!(
            "Keyspace {} preprocessed shard {}, old {:?}, new {:?}",
            self.tag(),
            old_shard.tag(),
            old_shard,
            new_shard
        );

        // Verify.
        let old_commit = old_shard.raft_state.get_commit();
        let new_commit = new_shard.raft_state.get_commit();
        let new_last_preprocessed_index = new_shard.raft_state.get_last_preprocessed_index();
        assert!(
            old_commit == new_commit && new_commit == new_last_preprocessed_index,
            "Keyspace {} shard {} preprocess: commit_index(old:{}, new:{})/last_preprocessed_index({}) not match",
            self.tag(),
            old_shard.tag(),
            old_commit,
            new_commit,
            new_last_preprocessed_index,
        );

        // When new shard has smaller range, there must be new peer due to split.
        // Raise error to retry from `load_shards` for simplicity, as it is a very rare
        // case.
        // Do not need to care about region merge, and `handle_overlapping_shards` will
        // handle the overlapping caused by merge.
        let has_new_peer =
            old_shard.start() < new_shard.start() || new_shard.end() < old_shard.end();
        if has_new_peer {
            info!(
                "{} split detected, retry load_shards, {} old_shard {:?}, new_shard {:?}",
                self.tag,
                old_shard.tag(),
                old_shard,
                new_shard
            );
        }

        let _ = mem::replace(old_shard, new_shard);
        Ok(has_new_peer)
    }

    // Not necessary as `find_intact_shards` already check the shards.
    // TODO: remove when `find_intact_shards` is stable.
    fn verify_shards(&self) -> Result<()> {
        debug_assert!(!self.sorted_shards.is_empty());
        if self.is_full_range() {
            self.verify_full_shards();
            return Ok(());
        }

        let first_shard = self.get_sorted_shard(0);
        let last_shard = self.get_sorted_shard(self.sorted_shards.len() - 1);
        if first_shard.start() != self.keyspace_start {
            return Err(box_err!(
                "start key of first region not match: {:?}",
                first_shard
            ));
        }
        if last_shard.end() != self.keyspace_end {
            return Err(box_err!(
                "end key of last region not match: {:?}",
                last_shard
            ));
        }

        for shards_id in self.sorted_shards.windows(2) {
            let left = self.get_shard(shards_id[0]).unwrap();
            let right = self.get_shard(shards_id[1]).unwrap();
            if left.end() != right.start() {
                return Err(box_err!(
                    "region boundary not match: {:?}, {:?}",
                    left,
                    right
                ));
            }
        }

        Ok(())
    }

    fn verify_full_shards(&self) {
        let first_shard = self.get_sorted_shard(0);
        let last_shard = self.get_sorted_shard(self.sorted_shards.len() - 1);
        if first_shard.start() != self.keyspace_start {
            warn!("start key of first region not match: {:?}", first_shard);
        }
        if last_shard.end() != self.keyspace_end {
            warn!("end key of last region not match: {:?}", last_shard);
        }

        for shards_id in self.sorted_shards.windows(2) {
            let left = self.get_shard(shards_id[0]).unwrap();
            let right = self.get_shard(shards_id[1]).unwrap();
            if left.end() != right.start() {
                warn!("region boundary not match: {:?}, {:?}", left, right);
            }
        }
    }

    fn get_sorted_shard(&self, sorted_idx: usize) -> &BackupShard {
        self.get_shard(self.sorted_shards[sorted_idx]).unwrap()
    }

    pub fn get_shard(&self, shard_id: u64) -> Option<&BackupShard> {
        self.shards.get(&shard_id)
    }

    pub fn get_shard_mut(&mut self, shard_id: u64) -> Option<&mut BackupShard> {
        self.shards.get_mut(&shard_id)
    }

    pub fn shards_count(&self) -> usize {
        self.sorted_shards.len()
    }

    pub fn get_shard_metas_before_flush(&self) -> Vec<ShardMeta> {
        let mut shards = Vec::with_capacity(self.sorted_shards.len());
        let guard = self.meta_applier.as_ref().unwrap().shards.read().unwrap();
        let backup_shards = guard.as_ref().unwrap();
        for &id in &self.sorted_shards {
            let backup_shard = backup_shards.get(&id).unwrap();
            shards.push(backup_shard.meta.clone());
        }
        shards
    }

    pub fn get_kvengine(&self) -> kvengine::Engine {
        self.kv_engine.as_ref().unwrap().clone()
    }

    pub fn get_rfengine(&self, store_id: u64) -> Option<RfEngine> {
        self.raft_engines.get(&store_id).cloned()
    }

    pub fn get_shard_meta_getter(&self) -> RegionMetaGetter {
        RegionMetaGetter {
            shard_store_map: self.shards_store_map.as_ref().unwrap().clone(),
            raft_engines: Arc::new(self.raft_engines.clone()),
        }
    }

    // The returned id of stores are sorted.
    // This will help to reuse downloaded table files when re-running the
    // restoration process.
    // See `BackupCluster::setup_kv_engine`.
    fn get_all_stores_id(&self) -> Vec<u64> {
        let mut stores_id: Vec<_> = self.store_shards.keys().copied().collect();
        stores_id.sort();
        stores_id
    }

    pub fn add_shards_need_flush(&mut self, shard_ids: &[u64]) {
        self.shards_need_flush.extend(shard_ids);
    }

    pub fn flush_shards(
        &mut self,
        timeout: Duration,
    ) -> Result<usize /* number of flushed shards */> {
        let kv_engine = self.kv_engine.as_ref().unwrap();
        // TODO: run in parallel.
        for &shard_id in &self.shards_need_flush {
            let engine_shard = kv_engine
                .get_shard(shard_id)
                .unwrap_or_else(|| panic!("shard not found: {}", shard_id));
            kv_engine.flush_shard_for_restore(&engine_shard)?;
        }

        self.check_flushed(timeout)?;

        let errors = self.meta_applier.as_ref().unwrap().take_errors();
        if !errors.is_empty() {
            return Err(box_err!(
                "{} meta_applier meet errors: {:?}",
                self.tag,
                errors
            ));
        }

        // Take back from meta_applier.
        let mut shards = self.meta_applier.as_ref().unwrap().take_shards().unwrap();
        self.shards = mem::take(&mut shards);

        Ok(self.shards_need_flush.len())
    }

    fn check_flushed(&self, timeout: Duration) -> Result<()> {
        let is_flushed = |stats: &ShardStats| {
            stats.flushed && stats.mem_table_size == 0 && stats.mem_table_count == 1
        };

        let kv_engine = self.kv_engine.as_ref().unwrap();
        let begin = Instant::now_coarse();
        while begin.saturating_elapsed() < timeout {
            let done = kv_engine
                .get_all_shard_stats()
                .into_iter()
                .all(|stats| is_flushed(&stats));
            if done {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(500));
        }

        let stats: Vec<_> = kv_engine
            .get_all_shard_stats()
            .into_iter()
            .filter(|stats| !is_flushed(stats))
            .collect();
        error!(
            "Keyspace {} wait_for_mem_table_flush timeout, stats: {:?}",
            self.keyspace_id, stats
        );
        Err(box_err!("wait_for_mem_table_flush timeout"))
    }

    async fn truncate_ts(&mut self) -> Result<usize> {
        let mut truncate_cnt = 0_usize;
        for shard_id in self.get_shards_need_flush_and_truncate() {
            truncate_cnt += self.truncate_shard_ts(shard_id).await? as usize;
        }
        Ok(truncate_cnt)
    }

    // NOTE: `sorted_backup_shards_id` & `sorted_target_regions` must be sorted by
    // start_key.
    fn align_target_regions_impl(
        sorted_backup_shards_id: &[u64],
        backup_shards: &HashMap<u64, BackupShard>,
        sorted_target_regions: Vec<RawRegion>,
    ) -> Vec<AlignedRegion> {
        let mut aligned_regions = Vec::with_capacity(sorted_target_regions.len());
        let mut idx = 0;

        // NOTE: use inner_key to check overlapping, target_regions may belongs to
        // a new keyspace.
        let key_in_shard = |key: &[u8], shard: &BackupShard| {
            let inner_key = InnerKey::from_outer_key(key);
            shard.data_bound().overlap_key(inner_key)
        };

        let get_backup_shard =
            |idx: usize| backup_shards.get(&sorted_backup_shards_id[idx]).unwrap();

        for target_region in sorted_target_regions {
            // Check overlapping with previous backup_region.
            if idx > 0 && key_in_shard(target_region.get_start_key(), get_backup_shard(idx - 1)) {
                idx -= 1;
            }
            let target_inner_begin = InnerKey::from_outer_key(target_region.get_start_key());
            let target_inner_end = InnerKey::from_outer_end_key(target_region.get_end_key());
            let mut aligned_shards_id = vec![];
            while idx < sorted_backup_shards_id.len()
                && get_backup_shard(idx).inner_start() < target_inner_end
            {
                // When retrying, some of regions may be filtered out.
                // In this scenario, we need to advance the cursor of `backup` to the first
                // region actually overlapping with target.
                //
                // An example: without this `if`, the shard `a` will be mistakenly added to
                // `backup_shards_id` when processing the target region `b`.
                //
                // Backup Shards  |-------|--a--|----|--...
                // Target Regions |-----|----|     |----b---|
                //                            ^^^^^ filtered out by preivous restore.
                if get_backup_shard(idx).inner_end() > target_inner_begin {
                    aligned_shards_id.push(sorted_backup_shards_id[idx]);
                }
                idx += 1;
            }

            aligned_regions.push(AlignedRegion {
                target_region,
                backup_shards_id: aligned_shards_id,
            });
        }

        aligned_regions
    }

    fn align_target_regions(&self, sorted_target_regions: Vec<RawRegion>) -> Vec<AlignedRegion> {
        Self::align_target_regions_impl(&self.sorted_shards, &self.shards, sorted_target_regions)
    }

    fn pre_split_and_scatter_regions(
        &self,
        config: &RestoreConfig,
        runtime: &Runtime,
    ) -> Result<()> {
        let (target_keyspace_start, target_keyspace_end) =
            ApiV2::get_keyspace_range_by_id(self.target_keyspace_id);
        let target_regions = runtime.block_on(get_target_regions_with_retry(
            self.pd_client.as_ref(),
            &target_keyspace_start,
            &target_keyspace_end,
            config.timeout_restore_snapshot.0,
        ))?;
        let aligned_regions = self.align_target_regions(target_regions);

        let mut coarse_split_keys =
            Vec::with_capacity(self.sorted_shards.len() / config.coarse_split_regions_factor);
        let mut split_keys = Vec::with_capacity(self.sorted_shards.len() - 1);
        for r in aligned_regions {
            for (idx, &shard_id) in r.backup_shards_id.iter().enumerate().skip(1) {
                let shard = self.get_shard(shard_id).unwrap();
                let split_key =
                    [target_keyspace_start.clone(), shard.inner_start().to_vec()].concat();

                if idx % config.coarse_split_regions_factor == 0 {
                    coarse_split_keys.push(split_key);
                } else {
                    split_keys.push(split_key);
                }
            }
        }

        self.split_regions_for_keys(
            coarse_split_keys,
            split_keys,
            config.timeout_split_regions.0,
            runtime,
        )
    }

    fn split_regions_for_keys(
        &self,
        coarse_split_keys: Vec<Vec<u8>>,
        split_keys: Vec<Vec<u8>>,
        timeout: Duration,
        runtime: &Runtime,
    ) -> Result<()> {
        if !coarse_split_keys.is_empty() {
            debug!("{} coarse split regions", self.tag(); "keys" => ?coarse_split_keys);
            self.split_regions_with_retry(coarse_split_keys, true, timeout, runtime)?;
        }
        if !split_keys.is_empty() {
            debug!("{} split regions", self.tag(); "keys" => ?split_keys);
            self.split_regions_with_retry(split_keys, false, timeout, runtime)?;
        }
        Ok(())
    }

    fn split_regions_with_retry(
        &self,
        raw_keys: Vec<Vec<u8>>,
        scatter: bool,
        timeout: Duration,
        runtime: &Runtime,
    ) -> Result<()> {
        for batch in raw_keys.chunks(SPLIT_REGIONS_BATCH_SIZE) {
            let encoded_keys = batch
                .iter()
                .map(|key| Key::from_raw(key).into_encoded())
                .collect::<Vec<_>>();
            let timeout = cmp::max(
                SPLIT_REGIONS_TIMEOUT_PER_KEY * encoded_keys.len() as u32,
                timeout,
            );
            step!(
                "Keyspace {} split regions for {} keys",
                self.tag(),
                encoded_keys.len()
            );
            let regions = runtime
                .block_on(
                    self.pd_client
                        .split_regions_with_retry(encoded_keys, timeout),
                )
                .map_err(|e| Error::SpitRegionsError(e))?;
            if scatter {
                step!("Keyspace {} scatter {} regions", self.tag(), regions.len());
                if let Err(err) = self.pd_client.scatter_regions_by_id(regions) {
                    warn!("{} scatter regions failed: {:?}", self.tag(), err);
                }
            }
        }

        Ok(())
    }

    fn update_schema_file_restore_version(&self, schema_file_id: u64) -> Result<u64> {
        let runtime = self.dfs.get_runtime();
        let schema_file = runtime.block_on(
            self.kv_engine
                .as_ref()
                .unwrap()
                .load_schema_file(schema_file_id),
        )?;
        info!(
            "Keyspace {} set schema file {schema_file_id} restore version to {}",
            self.tag(),
            self.truncate_ts
        );
        let new_schema_file = schema_file
            .set_restore_version_and_keyspace_id(self.truncate_ts, self.target_keyspace_id);
        let new_schema_file_id = *self.id_allocator.alloc_id(1)?.first().unwrap();
        let schema_data = new_schema_file.to_bytes();
        let opts = dfs::Options::default().with_type(dfs::FileType::Schema);
        runtime.block_on(
            self.dfs
                .create(new_schema_file_id, Bytes::from(schema_data), opts),
        )?;
        info!(
            "Keyspace {} update schema file {schema_file_id} restore version to {} and keyspace_id to {}, new file id {new_schema_file_id}",
            self.tag(),
            self.truncate_ts,
            self.target_keyspace_id,
        );
        Ok(new_schema_file_id)
    }

    fn gather_all_tables(
        &self,
        aligned_regions: Vec<AlignedRegion>,
    ) -> Result<(Vec<ShardMeta>, usize /* number of sstables */)> {
        let mut sstables_cnt = 0;
        let mut target_shards = Vec::with_capacity(aligned_regions.len());
        let mut new_schema_file_id = None;
        for region in aligned_regions {
            let shards = region
                .backup_shards_id
                .iter()
                .map(|&shard_id| self.get_shard(shard_id).unwrap())
                .collect::<Vec<_>>();

            let mut meta = ShardMeta::default();
            meta.id = region.target_region.id;
            meta.ver = region.target_region.epoch.version;
            meta.range = ShardRange::new(
                region.target_region.get_start_key(),
                region.target_region.get_end_key(),
            );
            meta.inner_key_off = self.target_inner_key_off();

            let shards_change_inner_key_off = shards
                .iter()
                .filter_map(|shard| {
                    (shard.inner_key_off() != meta.inner_key_off).then_some(shard.region_id)
                })
                .collect::<Vec<_>>();
            if !shards_change_inner_key_off.is_empty() {
                info!(
                    "{} shards change inner key offset after restore", self.tag();
                    "shards" => ?shards_change_inner_key_off, "target_inner_key_off" => meta.inner_key_off
                );
            }

            if let Some(encryption_key) = self.get_keyspace_exported_encryption_key() {
                meta.set_property(ENCRYPTION_KEY, encryption_key.as_slice());
            }
            let mut properties_helper =
                kvengine::util::PropertiesHelper::new_from_shard_meta(&meta);
            properties_helper.set_rewrite_range_prefix(!self.is_inplace_restore());
            let mut vector_indexes = HashMap::default();
            for shard in shards {
                let mut all_l0_ssts: HashSet<u64> = HashSet::default();
                for (&file_id, file_meta) in shard.meta.all_files() {
                    if meta.overlap_bound(file_meta.data_bound()) {
                        meta.add_file(file_id, file_meta.clone());
                        sstables_cnt += 1;
                        if file_meta.get_level() == 0 && file_meta.is_sst_file() {
                            all_l0_ssts.insert(file_id);
                        }
                    }
                }
                for vec_idx in shard.meta.vector_indexes.iter() {
                    for file in vec_idx.files.iter() {
                        vector_indexes
                            .entry((vec_idx.table_id, vec_idx.col_id, vec_idx.index_id))
                            .or_insert((HashMap::default(), vec_idx.get_snap_version()))
                            .0
                            .insert(file.get_id(), file.clone());
                    }
                }
                meta.columnar_l2_snap_version = cmp::max(
                    meta.columnar_l2_snap_version,
                    shard.meta.columnar_l2_snap_version,
                );
                // Only add the filtered l0s from ShardMeta.unconverted_l0s
                // to meta.unconverted_l0s
                meta.unconverted_l0s.extend(
                    shard
                        .meta
                        .unconverted_l0s
                        .iter()
                        .filter(|id| all_l0_ssts.contains(id)),
                );
                // Use `base_version` as table version, and `data_sequence` is 0.
                // And they will be adjusted at server side in `restore_shard` procedure.
                meta.base_version = cmp::max(meta.base_version, shard.table_version());
                properties_helper.merge_shard_meta(&shard.meta);
                if shard.meta.schema.is_valid() {
                    if new_schema_file_id.is_none() {
                        // Get the schema file and update schema_restore_version. Then put the new
                        // schema file to dfs.
                        new_schema_file_id = Some(
                            self.update_schema_file_restore_version(shard.meta.schema.file_id())?,
                        );
                    }
                    meta.schema.update_by_restore(
                        new_schema_file_id.unwrap(),
                        shard.meta.schema.file_ver(),
                        self.truncate_ts,
                    );
                    // Update columnar_snap_version to initial value to guarantee the new flushed
                    // l0s can be added to unconverted_l0s in target shard.
                    meta.columnar_table_ids = shard.meta.columnar_table_ids.clone();
                } else {
                    meta.schema.update_by_restore(0, 0, self.truncate_ts);
                }
            }
            meta.vector_indexes = vector_indexes
                .into_iter()
                .map(|((table_id, col_id, index_id), (files, snap_version))| {
                    let mut vector_index = VectorIndex::default();
                    vector_index.set_table_id(table_id);
                    vector_index.set_col_id(col_id);
                    vector_index.set_index_id(index_id);
                    vector_index.set_files(files.into_values().collect());
                    vector_index.set_snap_version(snap_version);
                    vector_index
                })
                .collect();

            properties_helper.build_to_shard_meta(&mut meta);
            target_shards.push(meta);
        }
        Ok((target_shards, sstables_cnt))
    }

    async fn truncate_shard_ts(&mut self, shard_id: u64) -> Result<bool /* has_truncate_ts */> {
        let shard_meta = &self.get_shard(shard_id).unwrap().meta;
        let kvengine = self.kv_engine.as_ref().unwrap();
        let shard = kvengine
            .get_shard_with_ver(shard_meta.id, shard_meta.ver)
            .expect("Could not find shard with meta");
        let keyspace_id = self.keyspace_id;
        let mut res_cs = kvengine
            .truncate_with_ts(&shard, self.truncate_ts)
            .await?
            .unwrap();
        if res_cs.has_truncate_ts() && !ShardMeta::is_empty_table_change(res_cs.get_truncate_ts()) {
            let shard = self.get_shard_mut(shard_id).unwrap();
            res_cs.set_sequence(shard.meta.seq + 1);
            debug!(
                "Keyspace {} before truncate ts: {:?}, table change: {:?}",
                keyspace_id,
                shard,
                res_cs.get_truncate_ts()
            );
            shard.meta.apply_change_set(&res_cs);
            debug!(
                "Keyspace {} after apply_truncate_ts: {:?}",
                keyspace_id, shard
            );
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn trim_over_bound(&mut self, shard_id: u64) -> Result<bool /* has_trim_over_bound */> {
        let shard = self.get_shard(shard_id).unwrap();
        let kvengine = self.kv_engine.as_ref().unwrap();

        let mut res_cs = kvengine.trim_over_bound_by_meta(&shard.meta).await?;
        let keyspace_id = self.keyspace_id;
        if res_cs.has_trim_over_bound() {
            let shard = self.get_shard_mut(shard_id).unwrap();
            res_cs.set_sequence(shard.meta.seq + 1);
            debug!(
                "Keyspace {} before trim_over_bound: {:?}, table change: {:?}",
                keyspace_id,
                shard,
                res_cs.get_trim_over_bound()
            );
            shard.meta.apply_change_set(&res_cs);
            debug!(
                "Keyspace {} after apply_trim_over_bound: {:?}",
                keyspace_id, shard
            );
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn trim_over_bound_shards(
        &mut self,
        aligned_regions: &[AlignedRegion],
    ) -> Result<usize /* number of trimmed shards */> {
        let mut trim_shards_cnt = 0_usize;
        let mut unique_shards = HashSet::default();
        for region in aligned_regions
            .iter()
            .filter(|x| x.backup_shards_id.len() > 1)
        {
            for &shard_id in &region.backup_shards_id {
                if unique_shards.contains(&shard_id) {
                    continue;
                }
                unique_shards.insert(shard_id);

                // TODO: Run in parallel.
                if self.trim_over_bound(shard_id).await? {
                    trim_shards_cnt += 1;
                }
            }
        }

        Ok(trim_shards_cnt)
    }

    pub fn generate_snapshots(&mut self, target_shards: Vec<ShardMeta>) -> Vec<pb::ChangeSet> {
        target_shards
            .into_iter()
            .map(|meta| {
                let mut cs = meta.to_change_set();
                let snap = cs.take_snapshot();
                cs.set_restore_shard(snap);
                info!("{} generate_snapshot cs: {:?}", self.tag, cs);
                cs
            })
            .collect()
    }

    fn target_inner_key_off(&self) -> usize {
        if ApiV2::is_default_keyspace(self.target_keyspace_id) {
            0
        } else {
            KEYSPACE_PREFIX_LEN
        }
    }

    fn is_inplace_restore(&self) -> bool {
        self.keyspace_id == self.target_keyspace_id
    }

    fn get_keyspace_exported_encryption_key(&self) -> Option<Vec<u8>> {
        self.get_sorted_shard(0)
            .meta
            .get_property(ENCRYPTION_KEY)
            .map(|x| x.to_vec())
    }

    fn tolerated_err(&self) -> usize {
        self.tolerated_err
    }

    // Should be called after `load_shards`.
    fn collect_txn_chunks_in_wal(&mut self) -> Result<()> {
        let mut total_chunk_ids = Vec::new();

        let master_key = self
            .dfs
            .get_runtime()
            .block_on(self.security_conf.new_master_key());
        let mut decryption_buf = Vec::new();

        for store_id in self.get_all_stores_id() {
            let rf_engine = self.raft_engines.get(&store_id).unwrap();
            for &shard_id in self.store_shards.get(&store_id).unwrap() {
                let shard = self.get_shard(shard_id).unwrap();

                let meta = self.raw_metas.get(&shard_id).unwrap();
                let snap = meta.get_snapshot();

                let encryption_key =
                    kvengine::get_shard_property(ENCRYPTION_KEY, snap.get_properties()).map(
                        |exported_key| master_key.decrypt_encryption_key(&exported_key).unwrap(),
                    );

                let tag = ShardTag::new(store_id, IdVer::from_change_set(meta));
                let peer_tag =
                    PeerTag::new(store_id, RegionIdVer::new(meta.shard_id, meta.shard_ver));

                let applied_index = snap.get_data_sequence();
                let commit_index = shard.raft_state.get_commit();
                let low_idx = applied_index + 1;
                let high_idx = commit_index + 1;
                debug_assert!(
                    low_idx <= high_idx,
                    "invalid entry index, low_idx {}, high_idx {}",
                    low_idx,
                    high_idx
                );
                if low_idx >= high_idx {
                    continue;
                }

                let mut entries = Vec::with_capacity((high_idx.saturating_sub(low_idx)) as usize);
                rf_engine
                    .fetch_raft_entries_to(shard.peer_id, low_idx, high_idx, None, &mut entries)
                    .map_err(|e| -> Error {
                        box_err!(
                            "{} entries unavailable err: {:?}, low: {}, high {}",
                            tag,
                            e,
                            low_idx,
                            high_idx
                        )
                    })?;

                let mut chunk_ids = Vec::new();
                for e in entries {
                    if e.data.is_empty() || e.entry_type != eraftpb::EntryType::EntryNormal {
                        continue;
                    }

                    let req =
                        parse_raft_cmd(peer_tag, &e, encryption_key.as_ref(), &mut decryption_buf);
                    if req.get_header().get_region_epoch().version != meta.shard_ver {
                        continue;
                    }

                    if let Some(custom) = rlog::get_custom_log(&req) {
                        if custom.is_txn_file_ref() {
                            let mut txn_file_ref = box_try!(custom.get_txn_file_ref());
                            chunk_ids.extend(txn_file_ref.take_chunk_ids());
                        }
                    }
                }

                if !chunk_ids.is_empty() {
                    info!("{} collect txn chunks in wal [{}, {})", tag, low_idx, high_idx;
                        "chunk_ids" => ?chunk_ids);
                    total_chunk_ids.extend(chunk_ids);
                }
            }
        }

        total_chunk_ids.sort();
        total_chunk_ids.dedup();
        self.txn_chunk_ids_in_wal = Some(total_chunk_ids);
        Ok(())
    }
}

struct MetaApplier {
    keyspace_id: u32,
    engine: kvengine::Engine,
    encryption_key: Option<EncryptionKey>,
    shards: RwLock<Option<HashMap<u64, BackupShard>>>,
    store_rx: mpsc::Receiver<StoreMsg>,
    errors: Mutex<Vec<Error>>,
}

impl Drop for MetaApplier {
    fn drop(&mut self) {
        info!(
            "Meta applier is dropped, engine {}",
            self.engine.get_engine_id()
        );
    }
}

impl MetaApplier {
    fn new(
        keyspace_id: u32,
        engine: kvengine::Engine,
        encryption_key: Option<EncryptionKey>,
        shards: HashMap<u64, BackupShard>,
        store_rx: mpsc::Receiver<StoreMsg>,
    ) -> Self {
        Self {
            keyspace_id,
            engine,
            encryption_key,
            shards: RwLock::new(Some(shards)),
            store_rx,
            errors: Default::default(),
        }
    }

    pub fn take_shards(&self) -> Option<HashMap<u64, BackupShard>> {
        self.shards.wl().take()
    }

    pub fn take_errors(&self) -> Vec<Error> {
        let mut errors = self.errors.lock().unwrap();
        mem::take(&mut *errors)
    }

    fn run(&self) {
        'outer: loop {
            let msg = match self.store_rx.recv() {
                Ok(msg) => msg,
                Err(err) => {
                    error!(
                        "Keyspace {} applier recv task error: {:?}",
                        self.keyspace_id, err
                    );
                    return;
                }
            };
            match msg {
                StoreMsg::GenerateEngineChangeSet(mut cs, _) => {
                    let tag =
                        ShardTag::new(self.engine.get_engine_id(), IdVer::from_change_set(&cs));
                    let engine_shard = self.engine.get_shard(cs.shard_id).unwrap();
                    let seq = cmp::max(
                        engine_shard.get_write_sequence(),
                        engine_shard.get_meta_sequence(),
                    ) + 1;
                    cs.set_sequence(seq);
                    debug!(
                        "Keyspace {} shard {} apply change set: {:?}",
                        self.keyspace_id, tag, cs
                    );

                    {
                        let mut shards = self.shards.wl();
                        if let Some(shards) = shards.as_mut() {
                            let shard = shards.get_mut(&cs.shard_id).unwrap();
                            shard.meta.apply_change_set(&cs);
                        } else {
                            if cs.has_compaction() {
                                debug!(
                                    "Keyspace {} MetaApplier: ignore changeset: {:?}",
                                    self.keyspace_id, cs
                                );
                                self.engine.meta_committed(&cs, true);
                                continue 'outer;
                            }
                            panic!(
                                "MetaApplier: changeset is lost due to shards map had be taken: {:?}",
                                cs
                            );
                        }
                    }

                    self.engine.meta_committed(&cs, false);
                    match self
                        .engine
                        .prepare_change_set(
                            cs,
                            PrepareOpts {
                                encryption_key: self.encryption_key.clone(),
                                ..Default::default()
                            },
                        )
                        .and_then(|cs| self.engine.apply_change_set(&cs))
                    {
                        Ok(()) => debug!(
                            "Keyspace {} shard {} MetaApplier apply change set successfully",
                            self.keyspace_id, tag
                        ),
                        Err(err) => {
                            let msg = format!(
                                "Keyspace {} shard {} MetaApplier apply change set failed: {:?}",
                                self.keyspace_id, tag, err
                            );
                            error!("{}", msg);
                            self.errors
                                .lock()
                                .unwrap()
                                .push(Error::Other(box_err!("{}", msg)));
                        }
                    }
                }
                StoreMsg::Stop => {
                    info!(
                        "Engine {} restore keyspace {} meta applier receive stop msg and stop now",
                        self.engine.get_engine_id(),
                        self.keyspace_id
                    );
                    break 'outer;
                }
                _ => {
                    error!("unexpected msg");
                }
            }
        }
    }
}

struct MetaIterator {
    store_id: u64,
    shards: Vec<u64>,
    raw_metas: HashMap<u64, pb::ChangeSet>,
}

impl MetaIterator {
    pub fn new(store_id: u64, shards: Vec<u64>, raw_metas: HashMap<u64, pb::ChangeSet>) -> Self {
        Self {
            store_id,
            shards,
            raw_metas,
        }
    }
}

impl kvengine::MetaIterator for MetaIterator {
    fn iterate<F>(&mut self, mut f: F) -> kvengine::Result<()>
    where
        F: FnMut(kvenginepb::ChangeSet),
    {
        for shard_id in &self.shards {
            f(self.raw_metas.get(shard_id).unwrap().to_owned());
        }
        Ok(())
    }

    fn engine_id(&self) -> u64 {
        self.store_id
    }
}

async fn get_target_regions(
    pd_client: &dyn PdClient,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<Vec<RawRegion>> {
    let encoded_start_key = Key::from_raw(start_key).into_encoded();
    let encoded_end_key = Key::from_raw(end_key).into_encoded();

    let mut regions = vec![];
    let mut next_key = encoded_start_key.clone();
    while next_key < encoded_end_key {
        let region = pd_client.get_region_async(&next_key).await?;
        next_key = region.get_end_key().to_vec();
        regions.push(region.into());
    }
    debug!("target_regions: {:?}", regions);

    verify_regions_boundary(start_key, end_key, &regions)?;
    Ok(regions)
}

async fn get_target_regions_with_retry(
    pd_client: &dyn PdClient,
    start_key: &[u8],
    end_key: &[u8],
    timeout: Duration,
) -> Result<Vec<RawRegion>> {
    try_wait_result_async(
        || Box::pin(get_target_regions(pd_client, start_key, end_key)),
        timeout,
        || Duration::from_millis(500),
    )
    .await
}

fn verify_regions_boundary(start_key: &[u8], end_key: &[u8], regions: &[RawRegion]) -> Result<()> {
    if regions.is_empty() {
        return Err(box_err!("no region"));
    }

    let first_region = regions.first().unwrap();
    let last_region = regions.last().unwrap();
    if first_region.get_start_key() != start_key {
        return Err(box_err!(
            "unexpected start key of first region: {:?}, start_key: {:?}",
            first_region,
            start_key
        ));
    } else if last_region.get_end_key() != end_key {
        return Err(box_err!(
            "unexpected end key of last region: {:?}, end_key: {:?}",
            last_region,
            end_key
        ));
    }

    for region in regions.windows(2) {
        if region[0].get_end_key() != region[1].get_start_key() {
            return Err(box_err!(
                "region boundary not match: {:?}, {:?}",
                region[0],
                region[1]
            ));
        }
    }

    Ok(())
}

fn restore_snapshots(
    tag: &str,
    runtime: &Runtime,
    pd_client: Arc<dyn PdClient>,
    reporter: &Arc<dyn ReportRestoreStepTrait>,
    snapshots: Vec<pb::ChangeSet>,
    success_ranges: &mut MergeRanges,
    config: &RestoreConfig,
    bo: &mut Backoff,
    limiter: &Option<Arc<ThroughputLimiter>>,
) -> Result<RestoredSnapshots> {
    let (snapshots_size, need_calibrate) = estimate_snapshot_size(&snapshots, limiter, REPLICAS);
    debug_assert_eq!(snapshots.len(), snapshots_size.len());

    let security_mgr = pd_client.get_security_mgr();
    let client = security_mgr.http_client(hyper::Client::builder())?;
    let timeout = config.timeout_restore_snapshot.0;
    let semaphore = Arc::new(Semaphore::new(config.restore_snapshot_concurrency.max(1)));

    let mut handles = Vec::with_capacity(snapshots.len());
    for (snap, snapshot_size) in snapshots.into_iter().zip(snapshots_size.into_iter()) {
        let pd_client = pd_client.clone();
        let client = client.clone();
        let reporter = reporter.clone();
        let shard_id = snap.shard_id;
        let start = snap.get_restore_shard().get_outer_start().to_vec();
        let end = snap.get_restore_shard().get_outer_end().to_vec();
        let limiter = limiter.clone();
        let semaphore = semaphore.clone();
        let task = async move {
            let _permit = box_try!(semaphore.acquire().await);
            request_restore_snapshot(
                pd_client,
                client,
                reporter,
                snap,
                timeout,
                limiter,
                snapshot_size,
                need_calibrate,
            )
            .await
        };
        handles.push((shard_id, runtime.spawn(task), start, end));
    }

    let is_error_retryable = |err: &Error| {
        matches!(
            err,
            Error::RegionVerNotMatch { .. }
                | Error::RegionNotFoundOrNoLeader(_)
                | Error::PdError(_)
        )
    };

    let mut disk_full_regions = vec![];
    let mut disk_full_stores: HashSet<u64> = HashSet::default();
    let mut restored = RestoredSnapshots::default();
    for (shard_id, h, start, end) in handles {
        match runtime.block_on(h).unwrap() {
            Ok(resp) => {
                success_ranges.insert(start, end);
                restored.count += 1;
                restored.restore_bytes += resp.restore_bytes;
            }
            Err(Error::StoreDiskFull(stores_id)) => {
                disk_full_regions.push(shard_id);
                disk_full_stores.extend(stores_id);
            }
            Err(e) if is_error_retryable(&e) => {
                info!("{} request_restore_snapshot error: {:?}, retry", tag, e);
            }
            Err(e) => {
                error!("{} request_restore_snapshot error: {:?}", tag, e);
                return Err(e);
            }
        }
    }

    if !disk_full_regions.is_empty() {
        let stores = Vec::from_iter(disk_full_stores);
        warn!("{} request_restore_snapshot error: disk full", tag;
            "regions" => ?disk_full_regions, "stores" => ?stores);
        bo.on_error(Error::StoreDiskFull(stores))?;

        if let Err(err) = pd_client.scatter_regions_by_id(disk_full_regions.clone()) {
            warn!("{} scatter regions failed", tag;
                "regions" => ?disk_full_regions, "err" => ?err);
        }
    }

    Ok(restored)
}

async fn request_restore_snapshot(
    pd_client: Arc<dyn PdClient>,
    client: security::HttpClient,
    reporter: Arc<dyn ReportRestoreStepTrait>,
    cs: pb::ChangeSet,
    timeout: Duration,
    limiter: Option<Arc<ThroughputLimiter>>,
    snapshot_size: SnapshotSize,
    restore_size_need_calibrate: bool,
) -> Result<RestoreShardResponse> {
    let security_mgr = pd_client.get_security_mgr();
    let post_data = Cow::from(cs.write_to_bytes().unwrap());
    let mut last_err = None;
    let mut retry_cnt = 0;
    let start_time = Instant::now();
    'retry: while start_time.saturating_elapsed() < timeout {
        retry_cnt += 1;
        let (region, leader) = match pd_client.get_region_leader_by_id(cs.shard_id).await? {
            Some((region, leader)) => (region, leader),
            None => {
                let e = Err(Error::RegionNotFoundOrNoLeader(cs.shard_id));
                warn!("{}:{}: {:?}", cs.shard_id, cs.shard_ver, e);
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue 'retry;
            }
        };

        let region_ver = region.get_region_epoch().get_version();
        let shard_ver = cs.get_shard_ver();
        if region_ver != shard_ver {
            return Err(Error::RegionVerNotMatch {
                expected: shard_ver,
                actual: region_ver,
            });
        }

        let store = pd_client.get_store_async(leader.get_store_id()).await?;
        let tag = ShardTag::new(store.get_id(), IdVer::new(cs.shard_id, cs.shard_ver));

        let replicas = region
            .get_peers()
            .iter()
            .filter(|p| p.role == PeerRole::Voter)
            .count();
        let mut estimated_restore_size = snapshot_size.estimated_size * replicas as u64;
        if let Some(limiter) = limiter.as_ref() {
            if restore_size_need_calibrate && !snapshot_size.files.is_empty() {
                match limiter
                    .calibrate_restore_size_from_tikv(&snapshot_size, &region)
                    .await
                {
                    Ok(calibrated_restore_size) => {
                        debug!(
                            "{} request_restore_snapshot: calibrated restore size: {} bytes",
                            tag, calibrated_restore_size
                        );
                        estimated_restore_size = calibrated_restore_size;
                    }
                    Err(err) => {
                        warn!(
                            "{} request_restore_snapshot: calibrate restore size error: {:?}",
                            tag, err
                        );
                        debug_assert!(false);
                    }
                }
            }

            if estimated_restore_size > 0 {
                reporter.report_pending_restore_data_size(estimated_restore_size as i64);
                NATIVE_BR_RESTORE_PENDING_DATA_SIZE.add(estimated_restore_size as i64);
                limiter.consume_restore_size(estimated_restore_size).await;
            }
        } else {
            reporter.report_pending_restore_data_size(estimated_restore_size as i64);
            NATIVE_BR_RESTORE_PENDING_DATA_SIZE.add(estimated_restore_size as i64);
        }
        tikv_util::defer!({
            reporter.report_pending_restore_data_size(-(estimated_restore_size as i64));
            NATIVE_BR_RESTORE_PENDING_DATA_SIZE.sub(estimated_restore_size as i64);
        });

        let uri = security_mgr.build_uri(format!("{}/restore-shard", &store.status_address))?;
        let req = Request::post(uri)
            .header(header::ACCEPT, CONTENT_TYPE_PROTOBUF)
            .body(Body::from(post_data.clone()))
            .unwrap();
        match send_request_to_store(req, &store, &client, timeout / 2).await {
            Ok((_, resp)) => {
                let resp: RestoreShardResponse = serde_json::from_slice(&resp).unwrap();
                debug!("{} request_restore_snapshot succeed", tag);
                NATIVE_BR_RESTORED_DATA_SIZE.inc_by(estimated_restore_size);
                NATIVE_BR_RESTORED_KV_SIZE.inc_by(resp.restore_bytes);
                return Ok(resp);
            }
            Err(Error::HttpPbError(status, mut err)) => {
                warn!("{} request_restore_snapshot failed: {:?}", tag, err);
                let sleep_dur = if err.has_disk_full() {
                    return Err(Error::StoreDiskFull(err.take_disk_full().take_store_id()));
                } else if err.has_epoch_not_match() {
                    return Err(Error::RegionVerNotMatch {
                        expected: shard_ver,
                        actual: err
                            .get_epoch_not_match()
                            .get_current_regions()
                            .first()
                            .map_or(0, |r| r.get_region_epoch().get_version()),
                    });
                } else if err.has_not_leader() && err.get_not_leader().has_leader() {
                    Duration::from_millis(50)
                } else {
                    Duration::from_millis(500)
                };
                if let Some(limiter) = limiter.as_ref()
                    && estimated_restore_size > 0
                {
                    limiter.unconsume(estimated_restore_size);
                }
                tokio::time::sleep(sleep_dur).await;
                last_err = Some(Err(Error::HttpPbError(status, err)));
                continue 'retry;
            }
            Err(e) => {
                let err_msg = format!(
                    "{} request_restore_snapshot #{retry_cnt} error: {:?}, region: {:?}, leader: {:?}",
                    tag, e, region, leader
                );
                warn!("{}", err_msg);
                last_err = Some(Err(box_err!(err_msg)));
                if let Some(limiter) = limiter.as_ref()
                    && estimated_restore_size > 0
                {
                    limiter.unconsume(estimated_restore_size);
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue 'retry;
            }
        }
    }
    last_err.expect("there must be error")
}

fn estimate_snapshot_size(
    snapshots: &[pb::ChangeSet],
    limiter: &Option<Arc<ThroughputLimiter>>,
    replicas: usize,
) -> (Vec<SnapshotSize>, bool /* need_calibrate */) {
    let mut total_snapshot_size = 0;
    let snapshots_size = snapshots
        .iter()
        .map(|cs| {
            let snapshot_size = ThroughputLimiter::estimate_snapshot_size_locally(cs);
            total_snapshot_size += snapshot_size.estimated_size;
            snapshot_size
        })
        .collect();
    let need_calibrate = limiter
        .as_ref()
        .is_some_and(|x| total_snapshot_size * replicas as u64 >= x.calibrate_threshold());

    (snapshots_size, need_calibrate)
}

fn make_keyspace_tag(source_keyspace_id: u32, target_keyspace_id: u32) -> String {
    format!("{}->{}", source_keyspace_id, target_keyspace_id)
}

struct PeerPreprocessor {
    preprocessed_index: u64,
    region: metapb::Region,
    preprocessed_region: Option<metapb::Region>,
    peer: metapb::Peer,
    shard_meta: Option<ShardMeta>,
    last_committed_split_idx: u64,
    pending_truncate: Option<(u64 /* term */, u64 /* index */)>,
    raft_hard_state: eraftpb::HardState,
    raft_state: RaftState,
    pending_merge_state: Option<MergeState>,
    want_rollback_merge_peers: HashSet<u64>,
    learner_skip_idx: u64,
    encryption_key: Option<EncryptionKey>,
}

impl PeerPreprocessor {
    fn new(rf_engine: &RfEngine, shard: &mut BackupShard) -> Self {
        let peer = metapb::Peer {
            id: shard.peer_id,
            store_id: shard.store_id,
            role: PeerRole::Voter,
            ..Default::default()
        };
        let mut region_state = rf_engine
            .load_region_state(shard.peer_id, shard.ver())
            .unwrap_or_else(|| {
                panic!("failed to load region state for peer {}", shard.peer_id);
            });
        let region = region_state.take_region();
        let merge_state = region_state
            .has_merge_state()
            .then(|| region_state.take_merge_state());

        // Set `preprocessed_index` to the `data_sequence` to replay from last flush and
        // restore memory-based fields of `ShardMeta`.
        // See https://github.com/tidbcloud/cloud-storage-engine/issues/1680.
        //
        // Also note that when the shard is not yet initial flushed, in theory we should
        // preprocess from parent. But the only memory field `txn_file_locks` so far is
        // used for ignore split/merge, which will not happen when the shard is not
        // initial flushed. So currently we do not preprocess from parent for
        // simplicity.
        // See https://github.com/tidbcloud/cloud-storage-engine/issues/1862.
        let preprocessed_index = shard.meta.data_sequence;

        Self {
            preprocessed_index,
            region,
            preprocessed_region: None,
            peer,
            shard_meta: Some(shard.meta.clone()), // TODO: mem::take()
            last_committed_split_idx: 0,
            pending_truncate: None,
            raft_hard_state: shard.raft_state.get_hard_state(),
            raft_state: shard.raft_state,
            pending_merge_state: merge_state,
            want_rollback_merge_peers: HashSet::default(),
            learner_skip_idx: 0,
            encryption_key: None,
        }
    }

    fn as_ref(&mut self) -> rfstore::store::peer::PreprocessRef<'_> {
        rfstore::store::peer::PreprocessRef {
            preprocessed_index: &mut self.preprocessed_index,
            region: &mut self.region,
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
}

struct Backoff {
    inner: ExponentialBackoff,
}

impl ops::Deref for Backoff {
    type Target = ExponentialBackoff;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ops::DerefMut for Backoff {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Backoff {
    fn new(bo: ExponentialBackoff) -> Self {
        Self { inner: bo }
    }

    fn on_error(&mut self, err: Error) -> Result<usize /* current_attempts */> {
        match self.next_delay() {
            Ok(delay) => {
                thread::sleep(delay);
                Ok(self.current_attempts())
            }
            Err(_) => Err(RetryLimitExceeded(Box::new(err))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use api_version::api_v2::KEYSPACE_PREFIX_LEN;

    use super::*;

    #[test]
    fn test_handle_overlapping_shards() {
        const KEYSPACE_ID: u32 = 42;
        let make_backup_shard = |tuple: (
            u64,  // shard_id
            u64,  // ver
            &str, // start, "00" for start of keyspace
            &str, // end, "99" for end of keyspace
        )|
         -> BackupShard {
            let mut shard = BackupShard::default();
            shard.region_id = tuple.0;
            shard.meta.ver = tuple.1;
            let outer_start = if tuple.2 == "00" {
                ApiV2::get_keyspace_prefix_by_id(KEYSPACE_ID)
            } else {
                [
                    ApiV2::get_keyspace_prefix_by_id(KEYSPACE_ID),
                    tuple.2.as_bytes().to_vec(),
                ]
                .concat()
            };
            let outer_end = if tuple.3 == "99" {
                ApiV2::get_keyspace_prefix_by_id(KEYSPACE_ID + 1)
            } else {
                [
                    ApiV2::get_keyspace_prefix_by_id(KEYSPACE_ID),
                    tuple.3.as_bytes().to_vec(),
                ]
                .concat()
            };
            shard.meta.range = ShardRange::new(&outer_start, &outer_end);
            shard.meta.inner_key_off = 4;
            shard
        };

        let add_leader_shard =
            |leader_shards: &mut HashMap<IdVer, BackupShard>,
             _store_id: u64,
             shards: Vec<(u64, u64, &str, &str)>| {
                for shard_tuple in shards {
                    let shard = make_backup_shard(shard_tuple);
                    leader_shards.insert(shard.id_ver(), shard);
                }
            };

        let cases = vec![
            (
                vec![(1, 100, "00", "99")], // Vec<(shard_id, ver, start, end)>
                vec![(1, 100, "00", "99")],
                vec![(1, 100, "00", "99")],
                // expected: Option<Vec<(shard_id, ver, start,end)>>, None means error
                Some(vec![(1, 100, "00", "99")]),
            ),
            (
                vec![(1, 100, "00", "01"), (2, 200, "01", "99")],
                vec![(1, 100, "00", "01"), (2, 200, "01", "99")],
                vec![(1, 100, "00", "01"), (2, 200, "01", "99")],
                Some(vec![(1, 100, "00", "01"), (2, 200, "01", "99")]),
            ),
            (
                vec![(1, 100, "00", "01"), (2, 200, "01", "99")],
                vec![(1, 201, "00", "99")], // merge from shard 1 & 2
                vec![(1, 100, "00", "01"), (2, 200, "01", "99")],
                Some(vec![(1, 201, "00", "99")]),
            ),
            (
                vec![(1, 100, "00", "01"), (2, 200, "01", "99")],
                vec![(2, 201, "00", "99")], // merge from shard 1 & 2
                vec![(1, 100, "00", "01"), (2, 200, "01", "99")],
                Some(vec![(2, 201, "00", "99")]),
            ),
            (
                vec![(1, 101, "00", "01"), (2, 101, "01", "99")], // split from shard 1
                vec![(1, 100, "00", "99")],
                vec![(1, 100, "00", "99")],
                Some(vec![(1, 101, "00", "01"), (2, 101, "01", "99")]),
            ),
            (
                vec![(2, 100, "01", "02")],
                vec![(1, 100, "00", "01")],
                vec![(3, 100, "02", "99")],
                Some(vec![
                    (1, 100, "00", "01"),
                    (2, 100, "01", "02"),
                    (3, 100, "02", "99"),
                ]),
            ),
            (
                vec![(1, 100, "00", "99")],
                vec![(1, 100, "00", "99")],
                vec![(1, 101, "01", "99")],
                Some(vec![(1, 100, "00", "99")]),
            ),
            (
                vec![(1, 100, "00", "99")],
                vec![(1, 100, "00", "99")],
                vec![(1, 101, "00", "01")],
                Some(vec![(1, 100, "00", "99")]),
            ),
            (
                vec![(1, 102, "00", "01"), (2, 102, "01", "02")],
                vec![(1, 101, "00", "01")], // "01" is visited.
                vec![(1, 100, "00", "99")],
                Some(vec![(1, 100, "00", "99")]),
            ),
            (
                vec![(1, 100, "00", "01"), (2, 100, "01", "99")],
                vec![(2, 101, "01", "02"), (3, 101, "02", "03")],
                vec![(2, 101, "01", "02"), (3, 102, "02", "04")],
                Some(vec![(1, 100, "00", "01"), (2, 100, "01", "99")]),
            ),
            #[cfg_attr(rustfmt, rustfmt_skip)]
            (
                vec![(1, 100, "00", "01"), (2, 100, "01", "03"), (3, 100, "03", "04"), (4, 100, "04", "06"), (7, 102, "06", "07"), (8, 102, "07", "99")],
                vec![(1, 100, "00", "01"), (2, 101, "01", "02"), (3, 101, "02", "04"), (4, 100, "04", "05"), (5, 100, "05", "99")],
                vec![(1, 100, "00", "01"), (2, 100, "01", "03"), (3, 100, "03", "04"), (5, 101, "04", "06"), (6, 101, "06", "07")],
                Some(vec![
                    (1, 100, "00", "01"), (2, 101, "01", "02"), (3, 101, "02", "04"), (5, 101, "04", "06"), (7, 102, "06", "07"), (8, 102, "07", "99"),
                ]),
            ),
            (
                vec![(1, 100, "00", "01")],
                vec![(2, 100, "00", "02")],
                vec![(1, 100, "00", "01")],
                None,
            ),
            (
                vec![(1, 100, "01", "02")],
                vec![(1, 100, "01", "02")],
                vec![(1, 100, "01", "02")],
                None,
            ),
            (
                vec![(1, 100, "01", "99")],
                vec![(1, 100, "01", "99")],
                vec![(1, 100, "01", "99")],
                None,
            ),
        ];

        for (case_idx, (store0, store1, store2, expected)) in cases.into_iter().enumerate() {
            let mut leader_shards = HashMap::default();
            add_leader_shard(&mut leader_shards, 0, store0);
            add_leader_shard(&mut leader_shards, 1, store1);
            add_leader_shard(&mut leader_shards, 2, store2);

            let res = BackupCluster::handle_overlapping_shards("", leader_shards);
            if let Some(expected) = expected {
                let expected_sorted_shards: Vec<u64> = expected.iter().map(|x| x.0).collect();
                let expected_leader_shards: HashMap<u64, BackupShard> =
                    HashMap::from_iter(expected.into_iter().map(|x| (x.0, make_backup_shard(x))));

                let (leader_shards, sorted_shards) = res.unwrap_or_else(|err| {
                    panic!("case: {}: {:?}", case_idx, err);
                });
                assert_eq!(
                    BTreeMap::from_iter(leader_shards.into_iter()),
                    BTreeMap::from_iter(expected_leader_shards.into_iter()),
                    "case: {}",
                    case_idx
                );
                assert_eq!(sorted_shards, expected_sorted_shards, "case: {}", case_idx);
            } else {
                assert!(res.is_err(), "case: {}", case_idx);
            }
        }
    }

    #[test]
    fn test_align_target_regions() {
        test_align_target_regions_helper(false);
        test_align_target_regions_helper(true);
    }

    fn test_align_target_regions_helper(inner_key_off_enabled: bool) {
        let inner_key_off = if inner_key_off_enabled {
            KEYSPACE_PREFIX_LEN
        } else {
            0
        };
        let key_prefix = b"x000";
        let cases: Vec<(Vec<&[u8]>, Vec<&[u8]>, Vec<Vec<u64>>)> = vec![
            (
                vec![b"0", b"1"], // backup_regions_boundary_keys, the index is the region id.
                vec![b"0", b"1"], // target_regions_boundary_keys
                vec![vec![0]],    // align_regions_id
            ),
            (
                vec![b"0", b"05", b"1"],
                vec![b"0", b"05", b"1"],
                vec![vec![0], vec![1]],
            ),
            (
                vec![b"1", b"13", b"16", b"2"],
                vec![b"1", b"15", b"2"],
                vec![vec![0, 1], vec![1, 2]],
            ),
            (
                vec![b"2", b"25", b"3"],
                vec![b"2", b"23", b"26", b"3"],
                vec![vec![0], vec![0, 1], vec![1]],
            ),
            #[cfg_attr(rustfmt, rustfmt_skip)]
            (
            vec![ b"3",    b"32",            b"34",  b"36", b"37", b"38", b"39", b"4"],
            vec![ b"3",    b"32",   b"33",   b"34",  b"36",                      b"4"],
            vec![vec![0], vec![1], vec![1], vec![2],         vec![3,4,5,6]],
            ),
            #[cfg_attr(rustfmt, rustfmt_skip)]
            (
                vec![b"2", b"25", b"41",               b"51", b"55"],
                vec!                    [b"42", b"5"],
                vec![vec![2]],
            ),
            #[cfg_attr(rustfmt, rustfmt_skip)]
            (
                vec![b"1", b"11", b"2", b"25",       b"41", b"5"],
                vec![                          b"4",        b"5"],
                vec![vec![3, 4]],
            ),
            #[cfg_attr(rustfmt, rustfmt_skip)]
            (
                vec![b"1", b"11", b"2", b"25", b"4", b"41", b"5"],
                vec![                          b"4",        b"5"],
                vec![vec![4, 5]],
            ),
        ];

        let make_regions = |keys: Vec<&[u8]>| -> Vec<RawRegion> {
            let mut regions = Vec::with_capacity(keys.len() - 1);
            for (idx, w) in keys.as_slice().windows(2).enumerate() {
                let (mut raw_start, mut raw_end) = (key_prefix.to_vec(), key_prefix.to_vec());
                raw_start.extend_from_slice(w[0]);
                raw_end.extend_from_slice(w[1]);
                regions.push(RawRegion {
                    id: idx as u64,
                    raw_start,
                    raw_end,
                    ..Default::default()
                });
            }

            regions
        };

        let make_backup_shards = |regions: Vec<RawRegion>| {
            let mut shards = HashMap::with_capacity(regions.len());
            let mut shards_id = Vec::with_capacity(regions.len());
            for r in regions {
                let mut shard = BackupShard::default();
                shard.region_id = r.id;
                shard.meta.range = ShardRange::new(r.get_start_key(), r.get_end_key());
                shard.meta.inner_key_off = inner_key_off;

                shards_id.push(shard.region_id);
                shards.insert(shard.region_id, shard);
            }

            (shards, shards_id)
        };

        for (case_idx, (backup_shards_key, target_regions_key, expected_regions_id)) in
            cases.into_iter().enumerate()
        {
            let backup_regions = make_regions(backup_shards_key);
            let (backup_shards, backup_shards_id) = make_backup_shards(backup_regions);

            let target_regions = make_regions(target_regions_key);

            let mut expected = Vec::with_capacity(target_regions.len());
            for (i, target_region) in target_regions.clone().into_iter().enumerate() {
                expected.push(AlignedRegion {
                    target_region,
                    backup_shards_id: expected_regions_id[i].clone(),
                });
            }

            let aligned_regions = BackupCluster::align_target_regions_impl(
                &backup_shards_id,
                &backup_shards,
                target_regions,
            );
            assert_eq!(aligned_regions, expected, "case: {}", case_idx);
        }
    }
}
