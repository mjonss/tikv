// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(any(test, feature = "testexport"))]
use std::sync::atomic::AtomicBool;
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
    fmt::{Display, Formatter},
    fs,
    fs::{File, OpenOptions, create_dir_all},
    mem,
    ops::{Deref, DerefMut},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
};

use api_version::ApiV2;
use bytes::{Buf, Bytes};
use file_system::{IoRateLimitMode, IoRateLimiter, open_direct_file};
use kvengine::dfs::Dfs;
use kvproto::raft_serverpb::{RegionLocalState, StoreIdent};
use protobuf::Message;
use raft_proto::eraftpb;
use rfenginepb::{ChangeSet, KeySpaceBackupMeta, StoreBackupMeta, StoreRaftLogBackupMeta};
use tikv_util::{
    error,
    errors::Context as _,
    info,
    mpsc::{SendError, Sender},
    panic_mark_dfs_worker_file_exists,
    time::Instant,
    warn,
};

use crate::{
    config::Config,
    log_batch::{RaftLogBlock, RaftLogs},
    manifest::{Manifest, manifest_path, persist_change_set},
    metrics::*,
    peers::RaftPeers,
    service_worker::{ServiceTask, ServiceWorker, WalProgress},
    write_batch::{PeerBatch, WriteBatch},
    *,
};

pub const MIN_EPOCH_ROTATE_LEN: usize = 4;
pub const MAX_EPOCH_ROTATE_LEN: usize = 32;

pub const TRUNCATE_ALL_INDEX: u64 = u64::MAX;
pub const MAX_EPOCH_BACKWARD: u32 = 64;
pub const MIN_RLOG_FILE_SIZE: u64 = 16 * 1024 * 1024; // 16MB

/// `RfEngine` is a persistent storage engine for multi-raft logs.
#[derive(Clone)]
pub struct RfEngine {
    core: Arc<RfEngineCore>,
}

impl Deref for RfEngine {
    type Target = RfEngineCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl RfEngine {
    // NOTE: Pass `Some(dfs)` for usage of TiKV servers ONLY. Otherwise, it would
    // corrupt the WAL chunks in DFS.
    pub fn open(
        dir: &Path,
        cfg: &Config,
        data_dir: Option<&Path>, // for check panic mark file exists
        dfs: Option<Arc<dyn Dfs>>,
    ) -> Result<Self> {
        let core = RfEngineCore::open(dir, cfg, data_dir, dfs)?;
        Ok(Self {
            core: Arc::new(core),
        })
    }
}

pub struct RfEngineCore {
    pub dir: PathBuf,

    pub wal_sync_dir: Option<PathBuf>,

    pub(crate) writer: Mutex<WalWriterExt>,

    pub(crate) peers: RaftPeers,

    pub(crate) dependants: papaya::HashMap<u64, RwLock<HashSet<u64>>>,

    pub(crate) task_sender: Sender<ServiceTask>,

    pub(crate) service_worker_handle: Mutex<Option<JoinHandle<()>>>,

    pub(crate) engine_id: Arc<AtomicU64>,

    pub(crate) lightweight: bool,

    pub(crate) dfs_worker_healthy: Healthy,

    pub(crate) epoch_rotate_len: usize,

    // Initializes current epoch id during loading engine and update it after wal rotation of sync
    // writer.
    pub(crate) current_epoch_id: Arc<AtomicU32>,

    pub(crate) compacted_epoch: Arc<AtomicU32>,

    _lock: fslock::LockFile, // hold lock to avoid release

    #[cfg(any(test, feature = "testexport"))]
    pub(crate) compact_force_stop: Arc<AtomicBool>,
}

impl Deref for RfEngineCore {
    type Target = RaftPeers;
    fn deref(&self) -> &Self::Target {
        &self.peers
    }
}

impl RfEngineCore {
    fn open(
        dir: &Path,
        cfg: &Config,
        data_dir: Option<&Path>,
        dfs: Option<Arc<dyn Dfs>>,
    ) -> Result<Self> {
        if !cfg!(feature = "testexport")
            && (cfg.rlog_file_size.0 < MIN_RLOG_FILE_SIZE || cfg.rlog_file_size.0 > u32::MAX as u64)
        {
            return Err(Error::Other(
                "invalid config: rlog_file_size must be between [16MB, 4GB]".to_owned(),
            ));
        }
        let wal_sync_dir = (!cfg.wal_sync_dir.is_empty()).then(|| PathBuf::from(&cfg.wal_sync_dir));
        let wal_secondary_dir =
            (!cfg.wal_secondary_dir.is_empty()).then(|| PathBuf::from(&cfg.wal_secondary_dir));
        init_wal_files(
            dir,
            wal_sync_dir.as_ref(),
            wal_secondary_dir.as_ref(),
            cfg.epoch_rotate_len,
        )?;

        // Lock the rfengine directory to prevent concurrent opening.
        let lock_path = dir.join("LOCK");
        let mut lock = fslock::LockFile::open(&lock_path)?;
        if !lock.try_lock()? {
            panic!("rfengine lock failed, maybe already used by another process");
        }

        let engine_id = Arc::new(AtomicU64::new(0));
        let manifest = Manifest::open(dir, engine_id.clone())?;
        let (service_tx, service_rx) = tikv_util::mpsc::unbounded();
        let compacted_epoch = Arc::new(AtomicU32::new(manifest.epoch_id));
        let wal_dir = wal_sync_dir.as_deref().unwrap_or(dir);
        let writer_type = if cfg.cli_mode {
            WriterType::CliMode
        } else {
            WriterType::Sync
        };
        let writer = WalWriter::new(wal_dir, cfg, compacted_epoch.clone(), writer_type);
        let writer_ext = if cfg.wal_secondary_dir.is_empty() {
            WalWriterExt::SingleWriter(writer)
        } else {
            let wal_secondary_dir = PathBuf::from(&cfg.wal_secondary_dir);
            let secondary_writer = WalWriter::new(
                &wal_secondary_dir,
                cfg,
                compacted_epoch.clone(),
                writer_type,
            );
            WalWriterExt::DoubleWriter(DoubleWriter::new(
                writer,
                secondary_writer,
                manifest.epoch_id,
                cfg.wal_double_write_unhealthy_size.0 as usize,
            )?)
        };
        let dfs_worker_healthy = dfs_worker::Healthy::default();
        #[cfg(any(test, feature = "testexport"))]
        let compact_force_stop = Arc::new(AtomicBool::new(false));
        let mut en = Self {
            dir: dir.to_owned(),
            wal_sync_dir,
            peers: Default::default(),
            dependants: Default::default(),
            writer: Mutex::new(writer_ext),
            task_sender: service_tx,
            service_worker_handle: Mutex::new(None),
            engine_id,
            lightweight: cfg.lightweight_backup,
            dfs_worker_healthy: dfs_worker_healthy.clone(),
            epoch_rotate_len: cfg.epoch_rotate_len,
            current_epoch_id: Arc::new(AtomicU32::new(0)),
            compacted_epoch: compacted_epoch.clone(),
            _lock: lock,
            #[cfg(any(test, feature = "testexport"))]
            compact_force_stop: compact_force_stop.clone(),
        };
        let async_epoch_offset = en.load(&manifest)?;
        if cfg.disable_compaction {
            return Ok(en);
        }
        {
            let async_wal_writer = if let Some((async_epoch_id, async_offset)) = async_epoch_offset
            {
                debug_assert!(en.is_async_wal_enabled());
                let mut async_wal_writer =
                    WalWriter::new(dir, cfg, compacted_epoch.clone(), WriterType::Async);
                async_wal_writer.open_file(async_epoch_id, async_offset)?;
                Some(async_wal_writer)
            } else {
                None
            };

            let lightweight_backup_args: Option<(LightweightBackupConfig, Arc<dyn Dfs>)> = if cfg
                .lightweight_backup
                && dfs.is_some()
            {
                if data_dir.is_some() && panic_mark_dfs_worker_file_exists(data_dir.unwrap()) {
                    // If panic_mark_dfs_worker_file exists, skip init dfs worker thread.
                    en.dfs_worker_healthy.set_unhealthy();
                    error!(
                        "lightweight backup is enabled, but panic_mark_dfs_worker_file exists, skip init dfs worker thread"
                    );
                    None
                } else {
                    let cfg = LightweightBackupConfig::new(
                        dir.to_owned(),
                        cfg.wal_chunk_target_file_size.0 as usize,
                        CompressionType::Lz4Compression,
                        CompressionType::Lz4Compression,
                        cfg.rlog_cache_capacity.0 as usize,
                        cfg.rlog_cache_size_threshold.0 as usize,
                        cfg.dfs_worker_memory_limit.as_memory_size() as usize,
                    );
                    Some((cfg, dfs.unwrap()))
                }
            } else {
                None
            };
            let compact_rate_limiter = if cfg.enable_compact_rate_limiter {
                let rate_limiter =
                    Arc::new(IoRateLimiter::new(IoRateLimitMode::WriteOnly, true, false));
                rate_limiter.set_io_rate_limit(cfg.compact_bytes_per_sec.0 as usize);
                Some(rate_limiter)
            } else {
                None
            };
            let mut service_worker = ServiceWorker::new(
                dir.to_owned(),
                cfg,
                async_wal_writer,
                service_rx,
                manifest,
                compacted_epoch.clone(),
                lightweight_backup_args,
                dfs_worker_healthy,
                compact_rate_limiter,
                #[cfg(any(test, feature = "testexport"))]
                compact_force_stop,
            );
            let join_handle = thread::spawn(move || service_worker.run());
            let mut guard = en.service_worker_handle.lock().unwrap();
            *guard = Some(join_handle);
        }

        Ok(en)
    }

    pub(crate) fn wal_dir(&self) -> &Path {
        self.wal_sync_dir.as_ref().unwrap_or(&self.dir)
    }

    /// Applies and persists the write batch.
    pub fn write(&self, wb: WriteBatch) -> Result<usize> {
        self.apply(&wb);
        self.persist(wb)
    }

    /// Applies the write batch to memory without persisting it to WAL.
    pub fn apply(&self, wb: &WriteBatch) {
        let truncated_logs = self.peers.apply(wb);
        if !truncated_logs.is_empty() {
            self.try_send_task(ServiceTask::Truncates(truncated_logs));
        }
    }

    /// Persists the write batch to WAL. It can be used in another thread to
    /// implement async I/O, i.e., call `apply` in the main thread and call
    /// `persist` in the I/O thread.
    /// When the epoch is rotated, the return size would be 4096 which is not
    /// accurate but ok to be used as metrics.
    pub fn persist(&self, wb: WriteBatch) -> Result<usize> {
        let timer = Instant::now();
        let wb = Arc::new(wb.into_vector());
        let mut writer = self.writer.lock().unwrap();
        let old_file_off = writer.get_file_off();
        let (epoch_id, file_off, rotated) = writer.write_batch(wb.clone())?;
        if rotated {
            self.current_epoch_id.store(epoch_id, Ordering::SeqCst);
            self.try_send_task(ServiceTask::Rotate {
                epoch_id: epoch_id - 1,
            });
        }
        if self.is_async_wal_enabled() {
            self.try_send_task(ServiceTask::Write { wb });
        }
        ENGINE_PERSIST_DURATION_HISTOGRAM.observe(timer.saturating_elapsed_secs());
        if rotated {
            return Ok(4096);
        }
        Ok(file_off.saturating_sub(old_file_off) as usize)
    }

    pub fn stop_worker(&self, force: bool) {
        #[cfg(any(test, feature = "testexport"))]
        if force {
            self.compact_force_stop.store(true, Ordering::SeqCst);
        }
        let mut handle = self.service_worker_handle.lock().unwrap();
        if let Some(h) = handle.take() {
            self.try_send_task(ServiceTask::Close { force });
            h.join().unwrap();
        }
    }

    pub fn close_writer(&self) {
        let mut _double_writer = None;
        {
            let mut writer = self.writer.lock().unwrap();
            match writer.deref_mut() {
                WalWriterExt::SingleWriter(_) => {}
                WalWriterExt::DoubleWriter(w) => {
                    _double_writer = Some(mem::take(w));
                }
            }
        }
    }

    /// After split and before the new region is initially flushed, the old
    /// region's raft log can not be truncated, otherwise, it would not be
    /// able to recover the new region. So we can call `add_dependent` after
    /// split to protect the raft log. After the new region is initially
    /// flushed or re-ingested or destroyed, call `remove_dependent` to
    /// resume truncating the raft log.
    pub fn add_dependent(&self, region_id: u64, dependent_id: u64) {
        let dependants = self.dependants.pin();
        let hs_ref = dependants.get_or_insert_with(region_id, || RwLock::new(HashSet::new()));
        let mut hs = hs_ref.write().unwrap();
        let newly_inserted = hs.insert(dependent_id);
        if !newly_inserted {
            return;
        }
        let len = hs.len();
        drop(hs);
        let tag = PeerTag::new(self.get_engine_id(), region_id);
        info!(
            "{} add dependent {}, dependents_len {}",
            tag, dependent_id, len
        );
    }

    pub fn remove_dependent(&self, region_id: u64, dependent_id: u64) -> usize {
        let dependants = self.dependants.pin();
        dependants
            .get(&region_id)
            .map(|hs| {
                let len = {
                    let mut hs = hs.write().unwrap();
                    hs.remove(&dependent_id);
                    hs.len()
                };
                let tag = PeerTag::new(self.get_engine_id(), region_id);
                info!(
                    "{} remove dependent {}, dependents_len {}",
                    tag, dependent_id, len
                );
                len
            })
            .unwrap_or_default()
    }

    pub fn with_dependents(&self, region_id: u64, f: impl FnOnce(&HashSet<u64>)) {
        let dependants = self.dependants.pin();
        if let Some(hs) = dependants.get(&region_id) {
            f(&hs.read().unwrap());
        }
    }

    pub fn has_dependents(&self, region_id: u64) -> bool {
        let dependants = self.dependants.pin();
        dependants
            .get(&region_id)
            .is_some_and(|hs| !hs.read().unwrap().is_empty())
    }

    pub fn pending_compaction_wals(&self) -> u8 {
        (self.current_epoch_id.load(Ordering::SeqCst)
            - 1
            - self.compacted_epoch.load(Ordering::SeqCst)) as u8
    }

    /// Dumps the state of the engine.
    pub fn get_engine_stats(&self) -> EngineStats {
        let mut total_mem_size = 0;
        let mut total_mem_entries = 0;
        let peers = self.peers.peers.pin();
        let mut peers_stats = peers
            .iter()
            .map(|(_, data)| {
                let peer_stats = data.read().unwrap().get_stats();
                total_mem_size += peer_stats.size;
                total_mem_entries += peer_stats.num_logs;
                peer_stats
            })
            .collect::<Vec<PeerStats>>();
        peers_stats.sort_by(|a, b| (b.size).cmp(&a.size));
        peers_stats.truncate(10);

        let mut disk_size = 0;
        let mut num_files = 0;
        if let Ok(read_dir) = self.dir.read_dir() {
            for e in read_dir.flatten() {
                if let Ok(m) = e.metadata() {
                    num_files += 1;
                    disk_size += m.size();
                }
            }
        }
        let pending_compaction_wals = self.pending_compaction_wals();
        ENGINE_PENDING_COMPACTION_WALS_GAUGE.set(pending_compaction_wals as i64);
        EngineStats {
            total_mem_size,
            total_mem_entries,
            disk_size,
            num_files,
            pending_compaction_wals,
            top_10_size_peers: peers_stats,
        }
    }

    pub fn set_engine_id(&self, engine_id: u64) {
        self.engine_id.store(engine_id, Ordering::Release)
    }

    pub fn get_engine_id(&self) -> u64 {
        self.engine_id.load(Ordering::Acquire)
    }

    // Upload latest wal chunk to object storage
    pub fn upload_wal_chunk(&self) {
        self.try_send_task(ServiceTask::Upload);
    }

    pub fn dump_wal_chunk(
        &self,
        epoch_id: u32,
        start_off: u64,
        end_off: u64,
        callback: Box<dyn FnOnce(Result<(Bytes, bool /* partial content */)>) + Send>,
    ) {
        self.try_send_task(ServiceTask::Dump {
            epoch_id,
            start_off,
            end_off,
            callback,
        });
    }

    pub fn get_wal_progress(&self, callback: Box<dyn FnOnce(Result<WalProgress>) + Send>) {
        self.try_send_task(ServiceTask::GetProgress { callback });
    }

    pub fn backup(&self, mut task: BackupTask) {
        if !self.is_async_wal_enabled() {
            // Note: when async wal is enabled, `file_off` is acquired from
            // `async_wal_writer` in worker.
            let writer = self.writer.lock().unwrap();
            task.file_off = writer.get_file_off();
        }
        self.try_send_task(ServiceTask::Backup(task));
    }

    pub fn get_epoch_offset(&self) -> (u32, u64) {
        let writer = self.writer.lock().unwrap();
        (writer.get_epoch_id(), writer.get_file_off())
    }

    pub(crate) fn is_async_wal_enabled(&self) -> bool {
        self.wal_sync_dir.is_some()
    }

    pub fn is_lightweight_backup_enabled(&self) -> bool {
        self.lightweight
    }

    pub fn is_dfs_worker_healthy(&self) -> bool {
        self.dfs_worker_healthy.is_healthy()
    }

    pub(crate) fn try_send_task(&self, task: ServiceTask) {
        if let Err(SendError(task)) = self.task_sender.send(task) {
            warn!("send service task failed: {:?}", task);
            let err_msg = "service worker is closed".to_string();
            match task {
                ServiceTask::Dump { callback, .. } => callback(Err(Error::Other(err_msg))),
                ServiceTask::GetProgress { callback } => callback(Err(Error::Other(err_msg))),
                ServiceTask::Backup(task) => (task.callback)(Err(Error::Backup(err_msg))),
                _ => {}
            }
        }
    }

    pub fn clone_for_keyspace(
        &self,
        keyspace_id: u32,
        target_dir: &Path,
        cfg: &Config,
    ) -> Result<RfEngine> {
        let mut new_engine = Self::open(target_dir, cfg, None, None)?;
        new_engine.peers = self.peers.clone_keyspace(keyspace_id);
        Ok(RfEngine {
            core: Arc::new(new_engine),
        })
    }
}

pub(crate) fn get_delayed_to_epoch_id(manifest: &ChangeSet) -> u32 {
    manifest.get_epoch_id() + manifest.get_delayed_epoches()
}

fn decompress_snap_rlog_file(compression_type: u32, content: &[u8]) -> Result<Cow<'_, [u8]>> {
    match CompressionType::from(compression_type) {
        CompressionType::Lz4Compression => {
            let decompressed = decompress_lz4(content).map_err(Error::from)?;
            Ok(Cow::Owned(decompressed))
        }
        CompressionType::NoCompression => Ok(Cow::Borrowed(content)),
    }
}

fn restore_all_raft_logs_with_snap_rlog_file(
    keyspace_ids: Option<&[u32]>,
    store_meta: &StoreBackupMeta,
    dir: &Path,
    rlog_data: &Bytes,
) -> Result<()> {
    let mut raft_meta = StoreRaftLogBackupMeta::default();
    let size = rlog_data.len();
    debug_assert!(size as u64 > store_meta.raft_meta_start_off);

    raft_meta
        .merge_from_bytes(&rlog_data.chunk()[store_meta.raft_meta_start_off as usize..size])
        .unwrap();
    let compression_type = raft_meta.get_header().get_compression_type();

    let handle_keyspace = |keyspace_meta: &KeySpaceBackupMeta| -> Result<()> {
        for file in keyspace_meta.get_files() {
            let path = raft_log_file_name(dir, file.peer_id, file.first_index, file.last_index);
            let content = decompress_snap_rlog_file(
                compression_type,
                &rlog_data.chunk()[file.start_off as usize..file.end_off as usize],
            )?;
            fs::write(&path, content).with_ctx(|| format!("write rlog {}", path.display()))?;
        }
        Ok(())
    };

    if let Some(keyspace_ids) = keyspace_ids {
        for keyspace_id in keyspace_ids {
            if let Some(keyspace_meta) = raft_meta.raft_logs.get(keyspace_id) {
                handle_keyspace(keyspace_meta)?;
            }
        }
    } else {
        for (_, keyspace_meta) in raft_meta.raft_logs {
            handle_keyspace(&keyspace_meta)?;
        }
    }
    Ok(())
}

pub fn find_latest_snapshot(
    dfs: Arc<dyn Dfs>,
    prefix: &str,
    store_id: u64,
    epoch_id: u32,
) -> Result<Option<String>> {
    let start_epoch = if epoch_id > MAX_EPOCH_BACKWARD {
        epoch_id - MAX_EPOCH_BACKWARD
    } else {
        1
    };

    // Find the latest snapshot smaller than cluster_backup epoch.
    let snapshot = match dfs.list_objects(
        &snapshot_rlog_key_suffix(start_epoch - 1),
        Some(&snapshot_rlog_key_prefix(store_id)),
        Some(MAX_EPOCH_BACKWARD),
    ) {
        Ok((objects, _)) => {
            if objects.is_empty() {
                None
            } else {
                objects.into_iter().rev().find(|obj| {
                    let snap_delayed_to_epoch =
                        parse_delayed_to_epoch_from_snapshot_key(Some(obj.key.deref())).unwrap();
                    snap_delayed_to_epoch < epoch_id
                })
            }
        }
        Err(err) => {
            error!("failed to list snapshot full backup files: {}", err);
            None
        }
    };
    Ok(snapshot.map(|snap| {
        snap.key
            .strip_prefix(&format!("{}/", prefix))
            .unwrap()
            .to_owned()
    }))
}

// For lightweight restore, find a latest snapshot full backup before
// `cluster_backup.backup_ts` and replay all WAL chunk files from snapshot epoch
// to epoch of the backup.
pub fn lightweight_restore(
    store_id: u64,
    keyspace_ids: Option<Vec<u32>>,
    dir: &Path,
    snap_epoch: u32,
    snap_meta: Bytes,
    snap_rlog: Bytes,
    epoch_rotate_len: usize,
) -> Result<u32> {
    let start_time = Instant::now_coarse();

    init_wal_files(dir, None, None, epoch_rotate_len)?;

    let mut snap_store_meta = StoreBackupMeta::default();
    if snap_epoch > 0 {
        snap_store_meta.merge_from_bytes(snap_meta.chunk()).unwrap();
        assert_eq!(snap_epoch, snap_store_meta.get_manifest().epoch_id);
        restore_all_raft_logs_with_snap_rlog_file(
            keyspace_ids.as_deref(),
            &snap_store_meta,
            dir,
            &snap_rlog,
        )?;
    }
    let dur_restore_rlogs = start_time.saturating_elapsed();

    info!("{} manifest file path: {:?}", store_id, manifest_path(dir); "keyspace" => ?keyspace_ids);
    let manifest_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(manifest_path(dir))
        .ctx("open manifest")?;
    if let Some(keyspace_ids) = keyspace_ids {
        let before = snap_store_meta.get_manifest().peers.len();
        filter_manifest_peers_for_keyspace(snap_store_meta.mut_manifest(), &keyspace_ids);
        let after = snap_store_meta.get_manifest().peers.len();
        info!("{} filter manifest peers: {} -> {}", store_id, before, after; "keyspace" => ?keyspace_ids);
    }
    persist_change_set(&manifest_file, 0, snap_store_meta.get_manifest())
        .ctx("persist change set")?;
    let dur_persist_manifest = start_time.saturating_elapsed() - dur_restore_rlogs;

    info!("{} restore rfengine", store_id;
        "restore_rlogs" => ?dur_restore_rlogs, "persist_manifest" => ?dur_persist_manifest);
    Ok(snap_store_meta.get_manifest().epoch_id)
}

fn filter_manifest_peers_for_keyspace(cs: &mut rfenginepb::ChangeSet, keyspace_ids: &[u32]) {
    let keyspace_ids_set = keyspace_ids.iter().cloned().collect::<HashSet<u32>>();
    let peers = cs.take_peers();
    peers
        .into_iter()
        .filter(|peer| {
            peer.region_id == 0
                || peer.peer_id == 0
                || get_keyspace_id_from_peer(peer).is_some_and(|x| keyspace_ids_set.contains(&x))
        })
        .for_each(|peer| cs.mut_peers().push(peer));
}

pub(crate) fn init_wal_files(
    dir: &Path,
    wal_sync_dir: Option<&PathBuf>,
    wal_secondary_dir: Option<&PathBuf>,
    epoch_rotate_len: usize,
) -> Result<()> {
    if !dir.exists() {
        create_dir_all(dir)?;
    }
    open_wal_files(dir, epoch_rotate_len)?;
    if let Some(wal_secondary_dir) = wal_secondary_dir {
        if !wal_secondary_dir.exists() {
            create_dir_all(wal_secondary_dir)?;
        }
    }
    if wal_sync_dir.is_none() {
        return Ok(());
    }
    let wal_sync_dir = wal_sync_dir.unwrap();
    if !wal_sync_dir.exists() {
        create_dir_all(wal_sync_dir)?;
    }
    // upgrade_mark file is used to make the upgrade procedure idempotent.
    // In case the upgrade process is interrupted, we can resume it later.
    let upgrade_mark_file = upgrade_mark_file_path(dir);
    if !upgrade_mark_file.exists() {
        if all_wal_files_exists(wal_sync_dir.as_path(), epoch_rotate_len) {
            return Ok(()); // already upgraded.
        }
        File::create(upgrade_mark_file.as_path())?;
    }
    copy_wal_files(dir, wal_sync_dir.as_path(), epoch_rotate_len)?;
    fs::remove_file(upgrade_mark_file.as_path())?;
    file_system::sync_dir(dir)?;
    Ok(())
}

fn upgrade_mark_file_path(dir: &Path) -> PathBuf {
    dir.join("upgrade_mark")
}

fn open_wal_files(dir: &Path, epoch_rotate_len: usize) -> Result<()> {
    // create epoch_rotate_len wal files and always reuse them, so we never need to
    // sync dir on writer thread.
    for i in 0..epoch_rotate_len {
        let file_path = wal_file_path(dir, i);
        let _ = open_direct_file(&file_path, true)?;
    }
    file_system::sync_dir(dir)?;
    Ok(())
}

pub(crate) fn wal_file_path(dir: &Path, idx: usize) -> PathBuf {
    dir.join(format!("{}.wal", idx))
}

fn all_wal_files_exists(dir: &Path, epoch_rotate_len: usize) -> bool {
    for i in 0..epoch_rotate_len {
        if !wal_file_path(dir, i).exists() {
            return false;
        }
    }
    true
}

fn copy_wal_files(dir: &Path, wal_sync_dir: &Path, epoch_rotate_len: usize) -> Result<()> {
    for i in 0..epoch_rotate_len {
        let src_file_path = wal_file_path(dir, i);
        let dst_file_path = wal_file_path(wal_sync_dir, i);
        fs::copy(src_file_path, dst_file_path)?;
    }
    file_system::sync_dir(wal_sync_dir)?;
    Ok(())
}

pub fn load_store_ident(rf: &RfEngine) -> Option<StoreIdent> {
    let val = rf.get_state(0, STORE_IDENT_KEY);
    val.as_ref()?;
    let mut ident = StoreIdent::new();
    ident.merge_from_bytes(val.unwrap().chunk()).unwrap();
    Some(ident)
}

pub fn save_store_ident(rf: &RfEngine, store_ident: &StoreIdent) {
    let val = store_ident.write_to_bytes().unwrap();
    let mut wb = WriteBatch::new();
    wb.set_state(0, 0, 0, STORE_IDENT_KEY, &val);
    rf.write(wb).unwrap();
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PeerMeta {
    pub(crate) region_id: u64,
    pub(crate) truncated_idx: u64,
    pub(crate) keyspace_id: u32,
    pub(crate) has_region_meta: bool,
    pub(crate) states: BTreeMap<Bytes, Bytes>,
    pub(crate) states_encoded_len: usize,
}

const ENTRY_BASE_LEN: usize = 2 /* key len */ + 4 /* value len */;

impl PeerMeta {
    pub(crate) fn new(region_id: u64, keyspace_id: u32) -> Self {
        Self {
            region_id,
            keyspace_id,
            ..Default::default()
        }
    }

    pub fn set_state(&mut self, key: &[u8], val: &[u8]) {
        self.set_state_bytes(Bytes::copy_from_slice(key), Bytes::copy_from_slice(val))
    }

    pub fn set_state_bytes(&mut self, key: Bytes, val: Bytes) {
        if key[0] == REGION_META_KEY_BYTE && !val.is_empty() {
            self.has_region_meta = true;
        }
        let key_len = key.len();
        self.states_encoded_len += key_len + val.len() + ENTRY_BASE_LEN;
        let old = self.states.insert(key, val);
        if let Some(old) = old {
            self.states_encoded_len -= key_len + old.len() + ENTRY_BASE_LEN;
        }
    }

    pub fn remove_state(&mut self, key: &[u8]) {
        if let Some(old) = self.states.remove(key) {
            self.states_encoded_len -= key.len() + old.len() + ENTRY_BASE_LEN;
            if self.has_region_meta && key[0] == REGION_META_KEY_BYTE {
                self.has_region_meta = self.get_latest_state(REGION_META_KEY_PREFIX).is_some()
            }
        }
    }

    pub fn get_state(&self, key: &[u8]) -> Option<&[u8]> {
        self.states.get(key).map(|v| v.chunk())
    }

    pub fn get_state_bytes(&self, key: &[u8]) -> Option<Bytes> {
        self.states.get(key).cloned()
    }

    pub fn get_latest_state(&self, key_prefix: &[u8]) -> Option<&[u8]> {
        self.states
            .iter()
            .rev()
            .find(|(k, _)| k.starts_with(key_prefix))
            .map(|(_, v)| v.chunk())
    }

    pub fn get_latest_peer_state(&self) -> Option<RegionLocalState> {
        let bin = self.get_latest_state(REGION_META_KEY_PREFIX)?;
        let mut region_local_state = RegionLocalState::new();
        region_local_state.merge_from_bytes(bin).unwrap();
        Some(region_local_state)
    }

    pub fn get_keyspace_id(&self) -> Option<u32> {
        if self.keyspace_id > 0 {
            return Some(self.keyspace_id);
        }
        if !self.has_region_meta {
            return None;
        }
        self.get_latest_peer_state().map(|local_state| {
            ApiV2::get_u32_keyspace_id_by_key(local_state.get_region().get_start_key())
                .unwrap_or_default()
        })
    }

    pub(crate) fn merge(&mut self, other: &PeerMeta, keep_empty: bool) {
        assert_eq!(self.region_id, other.region_id);
        if self.truncated_idx < other.truncated_idx {
            self.truncated_idx = other.truncated_idx;
        }
        if let Some(keyspace_id) = other.get_keyspace_id() {
            self.keyspace_id = keyspace_id;
        }
        for (key, val) in &other.states {
            if keep_empty || !val.is_empty() {
                self.set_state_bytes(key.clone(), val.clone());
            } else {
                self.remove_state(key)
            };
        }
    }
}

/// `PeerData` contains region data and state in memory.
#[derive(Clone, Default)]
pub(crate) struct PeerData {
    pub(crate) peer_id: u64,
    pub(crate) meta: PeerMeta,
    pub(crate) raft_logs: RaftLogs,
}

impl Deref for PeerData {
    type Target = PeerMeta;

    fn deref(&self) -> &Self::Target {
        &self.meta
    }
}

impl DerefMut for PeerData {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.meta
    }
}

impl PeerData {
    pub(crate) fn new(peer_id: u64, region_id: u64, keyspace_id: u32) -> Self {
        Self {
            peer_id,
            meta: PeerMeta::new(region_id, keyspace_id),
            ..Default::default()
        }
    }

    pub(crate) fn get(&self, index: u64) -> Option<eraftpb::Entry> {
        self.raft_logs.get(index)
    }

    pub(crate) fn term(&self, index: u64) -> Option<u64> {
        self.raft_logs.get(index).map(|e| e.term)
    }

    pub(crate) fn get_state(&self, key: &[u8]) -> Option<&Bytes> {
        self.states.get(key)
    }

    pub(crate) fn apply(&mut self, batch: &PeerBatch) -> Vec<RaftLogBlock> {
        debug_assert_eq!(self.peer_id, batch.peer_id);
        let mut truncated_blocks = vec![];
        for op in &batch.raft_logs {
            let truncated = self.raft_logs.append(self.peer_id, op.clone());
            if !truncated.is_empty() {
                truncated_blocks.extend(truncated);
            }
        }
        let truncated_index = batch.truncated_idx;
        if self.truncated_idx < truncated_index {
            self.truncated_idx = truncated_index;
            truncated_blocks.extend(self.raft_logs.truncate(truncated_index));
        }
        if self.truncated_idx == TRUNCATE_ALL_INDEX
            && truncated_index > 0
            && truncated_index != TRUNCATE_ALL_INDEX
        {
            warn!(
                "region: {} peer:{} restore truncate all index to index {}",
                self.region_id, self.peer_id, truncated_index,
            );
            self.truncated_idx = truncated_index;
        }
        self.meta.merge(&batch.meta, false);
        truncated_blocks
    }

    pub(crate) fn get_stats(&self) -> PeerStats {
        let size = self.raft_logs.size();
        let first_idx = self.raft_logs.first_index();
        let last_idx = self.raft_logs.last_index();
        let num_logs = if last_idx != 0 {
            (last_idx - first_idx + 1) as usize
        } else {
            0
        };
        PeerStats {
            peer_id: self.peer_id,
            region_id: self.meta.region_id,
            keyspace_id: self.meta.keyspace_id,
            size,
            num_logs,
            num_states: self.meta.states.len(),
            first_idx,
            last_idx,
            truncated_idx: self.meta.truncated_idx,
        }
    }
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct EngineStats {
    pub total_mem_size: usize,
    pub total_mem_entries: usize,
    pub num_files: usize,
    pub disk_size: u64,
    pub pending_compaction_wals: u8,
    pub top_10_size_peers: Vec<PeerStats>,
}

#[derive(Default, Serialize, Deserialize, Debug, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct PeerStats {
    pub peer_id: u64,
    pub region_id: u64,
    pub keyspace_id: u32,
    pub size: usize,
    pub num_logs: usize,
    pub num_states: usize,
    pub first_idx: u64,
    pub last_idx: u64,
    pub truncated_idx: u64,
}

pub struct PeerTag {
    pub engine_id: u64,
    pub region_id: u64,
}

impl PeerTag {
    pub fn new(engine_id: u64, region_id: u64) -> Self {
        Self {
            engine_id,
            region_id,
        }
    }
}

impl Display for PeerTag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.engine_id, self.region_id)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        assert_matches::assert_matches, collections::HashMap, fs::OpenOptions,
        os::unix::prelude::FileExt, time::Duration,
    };

    use engine_traits::Error as TraitError;
    use eraftpb::EntryType;
    use protobuf::Message;

    use super::*;
    use crate::{
        log_batch::RaftLogOp,
        test_util::{
            init_logger, make_log_data, make_region_state, make_state_kv, new_raft_entry, try_wait,
        },
    };

    #[test]
    fn test_rfengine() {
        init_logger();
        let tmp_dir = tempfile::tempdir().unwrap();
        let cfg = Config::new(128 * 1024_usize);
        let engine = RfEngine::open(tmp_dir.path(), &cfg, None, None).unwrap();
        let mut wb = WriteBatch::new();
        for peer_id in 1..=10_u64 {
            let (key, val) = make_state_kv(3, 1);
            let region_id = peer_id + 1;
            wb.set_state(peer_id, region_id, 0, key.chunk(), val.chunk());
        }
        engine.write(wb).unwrap();

        let mut truncated_regions = vec![];
        for idx in 1..=1050_u64 {
            let mut wb = WriteBatch::new();
            for peer_id in 1..=10_u64 {
                if peer_id == 1 && (idx > 100 && idx < 900) {
                    continue;
                }
                let region_id = peer_id + 1;
                let keyspace_id = 1;
                wb.append_raft_log(peer_id, region_id, keyspace_id, &make_log_data(idx, 128));
                let (key, val) = make_state_kv(1, idx);
                wb.set_state(peer_id, region_id, keyspace_id, key.chunk(), val.chunk());
                if idx % 100 == 0 && peer_id != 1 {
                    truncated_regions.push((peer_id, region_id, keyspace_id, idx - 100));
                    wb.truncate_raft_log(peer_id, region_id, keyspace_id, idx - 100);
                }
            }
            engine.write(wb).unwrap();
        }
        assert_eq!(engine.peers.peers.len(), 10);
        let wal_cnt = engine
            .dir
            .read_dir()
            .unwrap()
            .filter(|p| {
                p.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|e| e == "wal")
            })
            .count();
        assert_eq!(wal_cnt, 4);

        let mut old_entries_map = HashMap::new();
        let peers = engine.peers.peers.pin();
        for (peer_id, peer_ref) in peers.iter() {
            let peer_data = peer_ref.read().unwrap();
            assert_eq!(*peer_id, peer_data.peer_id);
            old_entries_map.insert(peer_data.peer_id, peer_data.clone());
        }
        drop(peers);
        assert_eq!(old_entries_map.len(), 10);
        engine.stop_worker(true);

        for _ in 0..2 {
            let engine = RfEngine::open(tmp_dir.path(), &cfg, None, None).unwrap();
            let mut wb = WriteBatch::new();
            for &(peer_id, region_id, keyspace_id, truncated_idx) in truncated_regions.iter() {
                wb.truncate_raft_log(peer_id, region_id, keyspace_id, truncated_idx);
            }
            engine.apply(&wb);
            engine.iterate_all_states(false, |peer_id, _, key, _| {
                let old_region_data = old_entries_map.get(&peer_id).unwrap();
                assert!(old_region_data.get_state(key).is_some());
                true
            });
            assert_eq!(engine.peers.peers.len(), 10);
            let peers = engine.peers.peers.pin();
            for (peer_id, new_data_ref) in peers.iter() {
                let new_data = new_data_ref.read().unwrap();
                let old_data = old_entries_map.get(peer_id).unwrap();
                assert_eq!(
                    old_data.raft_logs.first_index(),
                    new_data.raft_logs.first_index()
                );
                assert_eq!(
                    old_data.raft_logs.last_index(),
                    new_data.raft_logs.last_index()
                );
                for i in new_data.raft_logs.first_index()..=new_data.raft_logs.last_index() {
                    let entry = new_data.raft_logs.get(i).unwrap();
                    assert_eq!(
                        old_data.get(entry.index).unwrap().data.chunk(),
                        entry.data.chunk()
                    );
                }
            }
        }
    }

    #[test]
    fn test_region_data() {
        init_logger();
        let mut region_data = PeerData::new(1, 2, 1);

        let mut region_batch = PeerBatch::new(1, 2, 1);
        for i in 1..=5 {
            region_batch.append_raft_log(RaftLogOp::new(&new_raft_entry(
                EntryType::EntryNormal,
                i,
                i,
                b"data",
                0,
            )));
        }
        assert!(region_data.apply(&region_batch).is_empty());
        for i in 1..=5 {
            assert_eq!(
                region_data.get(i).unwrap(),
                region_batch.raft_logs[(i - 1) as usize].to_entry()
            );
            assert_eq!(region_data.term(i).unwrap(), i);
        }
        assert!(region_data.get(6).is_none());
        let region_stats = region_data.get_stats();
        assert_eq!(
            region_stats,
            PeerStats {
                peer_id: 1,
                region_id: 2,
                keyspace_id: 1,
                size: 20,
                num_logs: 5,
                num_states: 0,
                first_idx: 1,
                last_idx: 5,
                truncated_idx: 0,
            }
        );

        region_batch = PeerBatch::new(1, 2, 1);
        region_batch.truncate(5);
        region_batch.set_state(b"k1", b"v1");
        region_batch.set_state(b"k2", b"v2");
        let truncated = region_data.apply(&region_batch);
        assert_eq!(truncated.len(), 1);
        assert_eq!(truncated[0].first_index(), 1);
        assert_eq!(truncated[0].last_index(), 5);
        for i in 1..=5 {
            assert!(region_data.get(i).is_none());
        }
        assert_eq!(region_data.get_state(b"k1"), Some(&b"v1".to_vec().into()));
        assert_eq!(region_data.get_state(b"k2"), Some(&b"v2".to_vec().into()));
        let region_stats = region_data.get_stats();
        assert_eq!(
            region_stats,
            PeerStats {
                peer_id: 1,
                region_id: 2,
                keyspace_id: 1,
                size: 0,
                num_logs: 0,
                num_states: 2,
                first_idx: 0,
                last_idx: 0,
                truncated_idx: 5,
            }
        );

        region_batch = PeerBatch::new(1, 2, 1);
        region_batch.truncate(5);
        region_batch.set_state(b"k1", b"");
        assert!(region_data.apply(&region_batch).is_empty());
        assert!(region_data.get_state(b"k1").is_none());
        assert_eq!(region_data.get_state(b"k2"), Some(&b"v2".to_vec().into()));

        region_batch = PeerBatch::new(1, 2, 1);
        region_batch.truncate(100);
        assert!(region_data.apply(&region_batch).is_empty());
        assert_eq!(region_data.truncated_idx, 100);
    }

    #[test]
    fn test_rfengine_basic() {
        init_logger();
        const STATE_PREFIX: u8 = b'p';

        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::new(128 * 1024);
        let engine = RfEngine::open(dir.path(), &cfg, None, None).unwrap();

        // Write 10 logs and states to 2 region.
        let mut data_map = HashMap::new();
        let mut wb = WriteBatch::new();
        for peer_id in 1..=2 {
            let region_id = peer_id + 1;
            let keyspace_id = 1;
            for i in 1..=10 {
                let entry = new_raft_entry(EntryType::EntryNormal, peer_id, i, b"data", 0);
                let (state_key, state_val) = (&[STATE_PREFIX, i as u8], &[i as u8]);
                wb.append_raft_log(peer_id, region_id, keyspace_id, &entry);
                wb.set_state(peer_id, region_id, keyspace_id, state_key, state_val);

                let (entries, states) = data_map
                    .entry(peer_id)
                    .or_insert_with(|| (vec![], BTreeMap::new()));
                entries.push(entry);
                states.insert(state_key.to_vec(), state_val.to_vec());
            }
        }
        engine.write(wb).unwrap();

        assert_eq!(engine.get_term(1, 1), Some(1));
        assert_eq!(engine.get_term(1, 11), None);
        assert_eq!(engine.get_term(3, 1), None);
        assert_eq!(engine.get_last_index(1), Some(10));
        assert_eq!(engine.get_last_index(3), None);

        // Test `get_entry` and `get_state`.
        for (&peer_id, (entries, states)) in &data_map {
            entries.iter().for_each(|entry| {
                assert_eq!(entry, &engine.get_raft_entry(peer_id, entry.index).unwrap(),);
            });
            states
                .iter()
                .for_each(|(key, val)| assert_eq!(val, &engine.get_state(peer_id, key).unwrap()));
        }
        assert!(engine.get_raft_entry(1, 11).is_none());
        assert!(engine.get_raft_entry(3, 1).is_none());
        assert!(engine.get_state(1, b"k").is_none());

        // Test `fetch_entries_to`.
        let mut buf = vec![];
        for peer_id in 1..=2 {
            for low in 1..=10 {
                for high in low + 1..=11 {
                    assert_eq!(
                        engine
                            .fetch_raft_entries_to(peer_id, low, high, None, &mut buf)
                            .unwrap(),
                        (high - low) as usize
                    );
                    assert_eq!(
                        data_map.get(&peer_id).unwrap().0[(low - 1) as usize..(high - 1) as usize],
                        buf
                    );
                    buf.clear();
                }
            }
        }
        // Test `fetch_entries_to` should push logs to the buf.
        let peer1_entries = &data_map.get(&1).unwrap().0;
        for i in 1..=10 {
            assert_eq!(
                engine
                    .fetch_raft_entries_to(1, i, i + 1, None, &mut buf)
                    .unwrap(),
                1
            );
            assert_eq!(buf, peer1_entries[..i as usize]);
        }
        assert_matches!(
            engine.fetch_raft_entries_to(1, 11, 12, None, &mut buf),
            Err(TraitError::EntriesUnavailable),
        );
        // Test `fetch_entries_to` limits size.
        let mut max_size = 0;
        for (i, entry) in peer1_entries.iter().enumerate() {
            buf.clear();
            max_size += entry.compute_size();
            assert_eq!(
                engine
                    .fetch_raft_entries_to(1, 1, 11, Some(max_size as usize), &mut buf)
                    .unwrap(),
                i + 1
            );
            assert_eq!(buf, peer1_entries[..=i]);
        }

        // Test fetch empty logs.
        buf.clear();
        assert_eq!(
            engine
                .fetch_raft_entries_to(1, 1, 1, None, &mut buf)
                .unwrap(),
            0
        );
        assert!(buf.is_empty());

        // Test `get_last_state_with_prefix`
        assert_eq!(
            engine
                .get_last_state_with_prefix(1, &[STATE_PREFIX])
                .unwrap(),
            [10_u8].as_slice()
        );
        assert!(
            engine
                .get_last_state_with_prefix(1, &[STATE_PREFIX + 1])
                .is_none()
        );

        // Test `iterate_region_states`
        for desc in [false, true] {
            let mut expect_index = if desc { 10 } else { 1 };
            engine.iterate_peer_states(1, desc, |k, v| {
                assert_eq!(k.chunk(), &[STATE_PREFIX, expect_index]);
                assert_eq!(v.chunk(), &[expect_index]);
                if desc {
                    expect_index -= 1;
                } else {
                    expect_index += 1;
                }
                true
            });
            assert_eq!(expect_index, if desc { 0 } else { 11 });
        }

        // Test `iterate_all_states`
        for desc in [false, true] {
            let mut count = 0;
            engine.iterate_all_states(desc, |id, _, k, v| {
                assert_eq!(v, data_map.get(&id).unwrap().1.get(k).unwrap());
                count += 1;
                true
            });
            assert_eq!(count, 20);
        }
        // Test `iterate_all_states` breaks.
        let mut count = 0;
        engine.iterate_all_states(false, |_, _, _, _| {
            count += 1;
            false
        });
        assert_eq!(count, 2);

        // Test `add_dependent` and `remove_dependent`.
        engine.add_dependent(1, 1);
        let dependants = engine.dependants.pin();
        assert!(dependants.get(&1).unwrap().read().unwrap().contains(&1));
        engine.remove_dependent(1, 1);
        assert!(!dependants.get(&1).unwrap().read().unwrap().contains(&1));
    }

    #[test]
    fn test_rfengine_wal() {
        init_logger();
        let tmp_dir = tempfile::tempdir().unwrap();
        let wal_size = 128 * 1024_usize;
        let dir_path = tmp_dir.path();
        let cfg = Config::new(wal_size);
        let engine = RfEngine::open(dir_path, &cfg, None, None).unwrap();
        let mut wb = WriteBatch::new();
        for peer_id in 1..=10_u64 {
            let (key, val) = make_state_kv(3, 1);
            let region_id = peer_id + 1;
            wb.set_state(peer_id, region_id, 0, key.chunk(), val.chunk());
        }
        engine.write(wb).unwrap();
        for idx in 1..=1050_u64 {
            let mut wb = WriteBatch::new();
            for peer_id in 1..=10_u64 {
                let region_id = peer_id + 1;
                let keyspace_id = 1;
                wb.append_raft_log(peer_id, region_id, keyspace_id, &make_log_data(idx, 128));
                let (key, val) = make_state_kv(1, idx);
                wb.set_state(peer_id, region_id, keyspace_id, key.chunk(), val.chunk());
            }
            engine.write(wb).unwrap();
        }
        assert_eq!(engine.peers.peers.len(), 10);
        engine.stop_worker(true);
        for _ in 0..2 {
            let engine = RfEngine::open(dir_path, &cfg, None, None).unwrap();
            assert_eq!(engine.peers.peers.len(), 10);
            engine.stop_worker(true);
        }
        let compacted_epoch = engine.compacted_epoch.load(Ordering::Relaxed);
        let current_epoch = {
            let writer = engine.writer.lock().unwrap();
            writer.get_epoch_id()
        };
        {
            let mut it =
                WalIterator::new(dir_path, current_epoch + 1, cfg.epoch_rotate_len).unwrap();
            let Error::Corruption {
                msg: _,
                epoch_id: _,
                offset,
                data: _,
            } = it.check_wal_header().unwrap_err()
            else {
                panic!("expected corruption error");
            };
            // header epoch mismatch error offset should be 0
            assert_eq!(offset, 0);
        }
        for ep in compacted_epoch + 1..=current_epoch {
            let filename = wal_file_name(dir_path, ep, cfg.epoch_rotate_len);
            let mut it = WalIterator::new(dir_path, ep, cfg.epoch_rotate_len).unwrap();
            it.check_wal_header().unwrap();
            let mut offsets = vec![it.offset];
            loop {
                match it.read_batch() {
                    Err(err) => {
                        if let Error::Eof = err {
                            break;
                        }
                        panic!("{:?}", err);
                    }
                    Ok(_data) => offsets.push(it.offset),
                }
            }
            offsets.pop().unwrap();
            for (idx, offset) in offsets.iter().enumerate() {
                if idx == 0 || idx == offsets.len() / 2 || idx == offsets.len() - 1 {
                    for pos in &[0, 4, 8, 12] {
                        let fd = OpenOptions::new()
                            .read(true)
                            .write(true)
                            .open(filename.as_path())
                            .unwrap();
                        let mut buf = [0u8; 4096];
                        fd.read_exact_at(&mut buf, *offset).unwrap();
                        if buf[*pos] == 255 {
                            continue;
                        }
                        buf[*pos] += 1;
                        fd.write_all_at(buf.as_ref(), *offset).unwrap();
                        fd.sync_data().unwrap();
                        let open_engine = RfEngine::open(dir_path, &cfg, None, None);
                        // RfEngine can auto recover from corruption for the last epoch wal
                        // corruption.
                        assert!(if ep == current_epoch {
                            open_engine.is_ok()
                        } else {
                            open_engine.is_err()
                        });
                        buf[*pos] -= 1;
                        fd.write_all_at(buf.as_ref(), *offset).unwrap();
                        fd.sync_data().unwrap();
                    }
                }
            }
        }
    }

    #[test]
    fn test_truncate_all_logs() {
        init_logger();
        let tmp_dir = tempfile::tempdir().unwrap();
        let wal_size = 4096 * 10;
        let cfg = Config::new(wal_size);
        let engine = RfEngine::open(tmp_dir.path(), &cfg, None, None).unwrap();
        {
            let mut wb = WriteBatch::new();
            let (key, val) = make_region_state(10, 42);
            wb.set_state(1, 2, 1, &key, &val);
            engine.write(wb).unwrap();
        }
        for i in 1..=50 {
            let mut wb = WriteBatch::new();
            wb.append_raft_log(1, 2, 1, &make_log_data(i, 128));
            engine.write(wb).unwrap();
        }

        // Truncate all index.
        let mut wb = WriteBatch::new();
        wb.truncate_raft_log(1, 2, 1, TRUNCATE_ALL_INDEX);
        engine.write(wb).unwrap();

        // Write more batch to trigger WAL compaction.
        {
            let mut wb = WriteBatch::new();
            let (key, val) = make_region_state(11, 43);
            wb.set_state(2, 3, 0, &key, &val);
            engine.write(wb).unwrap();
        }
        for i in 1..=10 {
            let mut wb = WriteBatch::new();
            wb.append_raft_log(2, 3, 0, &make_log_data(i, wal_size));
            engine.write(wb).unwrap();
        }

        // Check no file of peer 1 left.
        wait_for_rlogs_truncated(&engine, 1, 10);
    }

    #[test]
    fn test_init_wal_files() {
        init_logger();
        let tmp_dir = tempfile::tempdir().unwrap();
        let epoch_rotate_len = 4;
        init_wal_files(tmp_dir.path(), None, None, epoch_rotate_len).unwrap();
        let check_file_exists = |path: &Path| {
            for idx in 0..epoch_rotate_len {
                assert!(wal_file_path(path, idx).exists());
            }
        };
        check_file_exists(tmp_dir.path());

        let file_contents: Vec<String> = (0..epoch_rotate_len)
            .map(|i| format!("wal {}", i))
            .collect();
        let write_files = |path: &Path| {
            for idx in 0..epoch_rotate_len {
                let wal_file_path = wal_file_path(path, idx);
                fs::write(wal_file_path.as_path(), file_contents[idx].as_bytes()).unwrap();
            }
        };
        write_files(tmp_dir.path());
        File::create(manifest_path(tmp_dir.path())).unwrap();

        // upgrade to use wal_sync_dir
        let wal_sync_dir = tmp_dir.path().join("wal_sync_dir");
        init_wal_files(tmp_dir.path(), Some(&wal_sync_dir), None, epoch_rotate_len).unwrap();
        assert!(!upgrade_mark_file_path(tmp_dir.path()).exists());
        let check_files = || {
            for idx in 0..epoch_rotate_len {
                let async_wal_file_path = wal_file_path(tmp_dir.path(), idx);
                assert!(async_wal_file_path.exists());
                let sync_wal_file_path = wal_file_path(&wal_sync_dir, idx);
                assert!(sync_wal_file_path.exists());
                let data = fs::read_to_string(sync_wal_file_path.as_path()).unwrap();
                assert_eq!(data, file_contents[idx]);
            }
        };
        check_files();

        // simulate upgrade interrupted.
        write_files(tmp_dir.path());
        fs::remove_file(wal_file_path(wal_sync_dir.as_path(), 3)).unwrap();
        File::create(upgrade_mark_file_path(tmp_dir.path())).unwrap();

        // init_wal_files again should recover from the interrupted upgrade.
        init_wal_files(tmp_dir.path(), Some(&wal_sync_dir), None, epoch_rotate_len).unwrap();
        check_files();

        // wal_secondary_dir is created no matter if wal_sync_dir is provided.
        let wal_secondary_dir = tmp_dir.path().join("wal_secondary_dir");
        init_wal_files(
            tmp_dir.path(),
            Some(&wal_sync_dir),
            Some(&wal_secondary_dir),
            epoch_rotate_len,
        )
        .unwrap();
        assert!(wal_secondary_dir.exists());
        fs::remove_dir(wal_secondary_dir.as_path()).unwrap();
        init_wal_files(
            tmp_dir.path(),
            None,
            Some(&wal_secondary_dir),
            epoch_rotate_len,
        )
        .unwrap();
        assert!(wal_secondary_dir.exists());
    }

    #[rstest::rstest]
    #[case(false)]
    #[case::wal_sync(true)]
    fn test_rfengine_compact_force_restart(#[case] with_wal_sync: bool) {
        init_logger();
        let tmp_dir = tempfile::tempdir().unwrap();
        let wal_sync_dir = tmp_dir.path().join("wal_sync");
        let mut cfg = Config::new(16 * 1024);
        if with_wal_sync {
            cfg.wal_sync_dir = wal_sync_dir.to_str().unwrap().to_owned();
        }
        let engine = RfEngine::open(tmp_dir.path(), &cfg, None, None).unwrap();

        let peer_id = 1;
        let region_id = 2;
        let keyspace_id = 1;
        let (key1, val1) = make_state_kv(b'a', 1);
        let key1_vec = key1.clone().freeze().to_vec();
        let val1_vec = val1.clone().freeze().to_vec();
        let mut wb = WriteBatch::new();
        wb.set_state(peer_id, region_id, keyspace_id, key1.chunk(), val1.chunk());
        engine.write(wb).unwrap();

        // Force stop before WAL rotate for stable interruption.
        engine.compact_force_stop.store(true, Ordering::SeqCst);

        let mut log_index = 1;
        let entry_size = (cfg.target_file_size.0 as usize / 2).max(1024);
        let start_epoch = engine.writer.lock().unwrap().get_epoch_id();
        while engine.writer.lock().unwrap().get_epoch_id() == start_epoch {
            let mut wb = WriteBatch::new();
            wb.append_raft_log(
                peer_id,
                region_id,
                keyspace_id,
                &make_log_data(log_index, entry_size),
            );
            engine.write(wb).unwrap();
            log_index += 1;
        }

        let (key2, val2) = make_state_kv(b'b', 2);
        let key2_vec = key2.clone().freeze().to_vec();
        let val2_vec = val2.clone().freeze().to_vec();
        let mut wb = WriteBatch::new();
        wb.set_state(peer_id, region_id, keyspace_id, key2.chunk(), val2.chunk());
        engine.write(wb).unwrap();

        engine.stop_worker(true);
        drop(engine);

        let engine = RfEngine::open(tmp_dir.path(), &cfg, None, None).unwrap();
        let epoch_after_restart = engine.writer.lock().unwrap().get_epoch_id();
        assert_eq!(epoch_after_restart, start_epoch + 1);

        while engine.writer.lock().unwrap().get_epoch_id() == epoch_after_restart {
            let mut wb = WriteBatch::new();
            wb.append_raft_log(
                peer_id,
                region_id,
                keyspace_id,
                &make_log_data(log_index, entry_size),
            );
            engine.write(wb).unwrap();
            log_index += 1;
        }

        let (key3, val3) = make_state_kv(b'c', 3);
        let key3_vec = key3.clone().freeze().to_vec();
        let val3_vec = val3.clone().freeze().to_vec();
        let mut wb = WriteBatch::new();
        wb.set_state(peer_id, region_id, keyspace_id, key3.chunk(), val3.chunk());
        engine.write(wb).unwrap();

        assert!(
            try_wait(
                || engine.compacted_epoch.load(Ordering::SeqCst) >= epoch_after_restart,
                10
            ),
            "compact epoch {} not finished",
            epoch_after_restart
        );

        engine.stop_worker(false);
        drop(engine);

        let engine = RfEngine::open(tmp_dir.path(), &cfg, None, None).unwrap();
        assert_eq!(
            val1_vec.as_slice(),
            engine.get_state(peer_id, &key1_vec).unwrap().as_ref()
        );
        assert_eq!(
            val2_vec.as_slice(),
            engine.get_state(peer_id, &key2_vec).unwrap().as_ref()
        );
        assert_eq!(
            val3_vec.as_slice(),
            engine.get_state(peer_id, &key3_vec).unwrap().as_ref()
        );
        engine.stop_worker(true);
    }

    fn wait_for_rlogs_truncated(en: &RfEngine, peer_id: u64, seconds: usize) {
        let mut ok = false;
        let peer_id_str = format!("{:016x}", peer_id);

        let start_time = Instant::now_coarse();
        let timeout = Duration::from_secs(seconds as u64);
        while start_time.saturating_elapsed() < timeout {
            let read_dir = en.dir.read_dir().unwrap();
            let found = read_dir.into_iter().any(|entry| {
                let filename = entry.unwrap().file_name();
                let filename = filename.to_string_lossy();
                let parts: Vec<_> = filename.as_ref().split('_').collect();
                parts.len() == 3 && parts[0] == peer_id_str
            });
            if !found {
                ok = true;
                break;
            }
            thread::sleep(Duration::from_secs(1));
        }

        assert!(ok);
    }
}
