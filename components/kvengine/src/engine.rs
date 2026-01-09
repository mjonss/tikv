// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    env, fmt,
    fmt::{Debug, Display, Formatter},
    iter::{FromIterator, Iterator},
    ops::Deref,
    path::PathBuf,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    thread,
    time::Duration,
};

use bytes::BufMut;
use cloud_encryption::MasterKey;
use collections::HashSet;
use crossbeam::channel::RecvTimeoutError;
use dashmap::{DashMap, DashSet};
use file_system::IoRateLimiter;
use fslock;
use security::SecurityManager;
use slog_global::info;
use tikv_util::{box_err, mpsc, sys::thread::StdThreadBuildWrapper, time::Instant, HandyRwLock};
use txn_chunk_manager::with_pool_size;

use crate::{
    apply::ChangeSet,
    config::PerKeyspaceConfig,
    context::{IaCtx, PrepareType},
    ia::manager::IaManager,
    limiter::{DfsLoadLimiter, StoreLimiter},
    meta::ShardMeta,
    metrics::{ENGINE_FREE_MEM_BYTES_HISTOGRAM, PREPARE_COUNTER_VEC},
    table::{
        columnar::ColumnarMetaCache,
        file::FdCache,
        memtable::{CfTable, CfTableCore},
        schema_file::SchemaFile,
        sstable::{BlockCache, MAGIC_NUMBER},
        tiny_meta::{MetaPackReader, MetaPackScheduler},
        BoundedDataSet, DataBound, InnerKey, SnapVersion, ZSTD_COMPRESSION,
    },
    txn_chunk_manager::{TxnChunkManager, TxnChunkManagerConfig},
    util::{new_blob_create_pb, new_l0_create_pb, new_table_create_pb},
    value_cache::ValueCache,
    *,
};

#[derive(Clone)]
pub struct Engine {
    pub core: Arc<EngineCore>,
    pub(crate) meta_change_listener: Box<dyn MetaChangeListener>,
}

impl Deref for Engine {
    type Target = EngineCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl Debug for Engine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // TODO: complete debug info.
        let cnt = self.shards.len();
        let str = format!("num_shards: {}", cnt);
        f.write_str(&str)
    }
}

const FILE_LOCK_SLOTS: usize = 1024;

/// LoadTableFilterFn is used to filter tables which are not necessary to load
/// from DFS to local disks during restoration.
pub type LoadTableFilterFn =
    Arc<dyn Fn(u64 /* shard_id */, &FileMeta) -> bool + Send + Sync + 'static>;

impl Engine {
    pub fn open(
        fs: Arc<dyn dfs::Dfs>,
        opts: Arc<Options>,
        config: KvEngineConfig,
        meta_iter: &mut impl MetaIterator,
        recoverer: impl RecoverHandler + 'static,
        id_allocator: Arc<dyn IdAllocator>,
        meta_change_listener: Box<dyn MetaChangeListener>,
        rate_limiter: Arc<IoRateLimiter>,
        store_limiter: Arc<StoreLimiter>,
        ks_gc_sp_map: Option<Arc<DashMap<u32, u64>>>,
        master_key: MasterKey,
        security_mgr: Arc<SecurityManager>,
        set_loaded: bool,
    ) -> Result<Engine> {
        info!("open KVEngine");
        let lock_path = opts.local_dirs[0].join("LOCK");
        let mut x = fslock::LockFile::open(&lock_path).ctx("engine.open.open_lockfile")?;
        x.lock().ctx("engine.open.lock")?;

        let mut max_capacity = opts.max_block_cache_size as usize;
        if max_capacity < 512 * opts.table_builder_options.block_size {
            max_capacity = 512 * opts.table_builder_options.block_size;
        }
        let cache = BlockCache::new(
            config.block_cache_type,
            max_capacity as u64,
            opts.table_builder_options.block_size,
        );
        let fd_cache = FdCache::new(config.fd_cache_capacity);
        let meta_fd_cache = FdCache::new(config.fd_cache_capacity);
        let value_cache = if !config.value_cache_capacity.is_zero() {
            Some(ValueCache::new(
                config.value_cache_capacity.as_memory_size(),
                config.value_cache_double_check,
            ))
        } else {
            None
        };
        let columnar_meta_cache =
            ColumnarMetaCache::new(config.columnar_meta_cache_capacity.as_memory_size());
        let (flush_tx, flush_rx) = mpsc::unbounded();
        let (compact_tx, compact_rx) = mpsc::unbounded();
        let (free_tx, free_rx) = mpsc::unbounded();
        let compression_lvl = opts.table_builder_options.compression_lvl;
        let checksum_type = config.checksum_type;
        let allow_fallback_local = opts.allow_fallback_local;
        let file_locks = (0..FILE_LOCK_SLOTS).map(|_| Mutex::new(())).collect();
        let per_keyspace_configs = Arc::new(config.get_per_keyspace_configs());
        let txn_chunk_mgr = TxnChunkManager::new(
            opts.local_dirs.iter().map(|dir| dir.join("txn")).collect(),
            fs.clone(),
            cache.clone(),
            Some(fd_cache.clone()),
            with_pool_size(opts.txn_file_worker_pool_size),
            TxnChunkManagerConfig::default(),
        );
        let ia_ctx = create_ia_ctx(opts.clone(), fs.clone(), meta_fd_cache.clone());
        let (metas, files_in_blacklist) =
            tikv_util::init_task_local_sync(|| EngineCore::read_meta(meta_iter))?;
        let dfs_load_limiter = DfsLoadLimiter::new(&config);
        let core = EngineCore {
            engine_id: AtomicU64::new(meta_iter.engine_id()),
            shards: papaya::HashMap::new(),
            keyspace_shards: DashMap::new(),
            opts: opts.clone(),
            per_keyspace_configs,
            flush_tx,
            compact_tx,
            fs: fs.clone(),
            cache,
            fd_cache,
            value_cache,
            columnar_meta_cache: columnar_meta_cache.clone(),
            comp_client: CompactionClient::new(
                fs.clone(),
                opts.remote_compactor_addr.clone(),
                compression_lvl,
                checksum_type,
                allow_fallback_local,
                id_allocator.clone(),
                master_key.clone(),
                security_mgr,
                opts.local_dirs.clone(),
                opts.for_restore,
                columnar_meta_cache,
            ),
            id_allocator,
            managed_safe_ts: AtomicU64::new(0),
            rate_limiter,
            store_limiter,
            free_tx,
            loaded: AtomicBool::new(false),
            file_locks,
            files_in_blacklist: Arc::new(files_in_blacklist),
            shutting_down: AtomicBool::new(false),
            ks_safepoint_v2: ks_gc_sp_map,
            master_key,
            txn_chunk_mgr,
            ia_ctx,
            schema_files: Arc::new(DashMap::new()),
            dfs_load_limiter,
            available_space_bytes: AtomicU64::new(0),
            worker_handles: Default::default(),
            meta_pack_scheduler: recoverer.meta_pack_scheduler().cloned(),
        };
        let en = Engine {
            core: Arc::new(core),
            meta_change_listener,
        };

        info!("engine load {} shards", metas.len());
        tikv_util::init_task_local_sync(|| en.load_shards(metas, recoverer, None))?;
        if set_loaded {
            en.set_loaded();
        }
        let flush_en = en.clone();
        en.add_worker_handle(
            thread::Builder::new()
                .name("flush".to_string())
                .spawn_wrapper(move || {
                    flush_en.run_flush_worker(flush_rx);
                })
                .unwrap(),
        );
        if !opts.for_restore {
            // Disable compaction for restore, as some tables are not loaded from DFS.
            let compact_en = en.clone();
            en.add_worker_handle(
                thread::Builder::new()
                    .name("compaction".to_string())
                    .spawn_wrapper(move || {
                        compact_en.run_compaction(compact_rx);
                    })
                    .unwrap(),
            );
        } else {
            warn!("compaction is disabled");
        }
        en.add_worker_handle(
            thread::Builder::new()
                .name("free_mem".to_string())
                .spawn_wrapper(move || {
                    free_mem(free_rx);
                })
                .unwrap(),
        );
        Ok(en)
    }

    // This method is also used by native-br for cluster restore.
    pub fn load_shards(
        &self,
        metas: HashMap<u64, ShardMeta>,
        recoverer: impl RecoverHandler + 'static,
        load_table_filter: Option<LoadTableFilterFn>,
    ) -> Result<()> {
        if metas.is_empty() {
            return Ok(());
        };

        let start_time = Instant::now_coarse();
        info!("load_shards: start load parents");
        let mut parents = HashMap::new();
        for meta in metas.values() {
            if let Some(parent) = &meta.parent {
                let id_ver = IdVer::new(parent.id, parent.ver);
                if !parents.contains_key(&id_ver) {
                    info!("load parent of {}", meta.tag());
                    tikv_util::set_current_region(id_ver.id);
                    let parent_shard = Arc::new(self.load_parent_shard(
                        parent,
                        load_table_filter.clone(),
                        recoverer.meta_pack_reader(),
                    )?);

                    // Ingest the parent shard before recovery, as recoverer depends on the shard
                    // existing in kvengine.
                    let normal_shard = self.insert_shard(parent_shard.clone());
                    recoverer.recover(self, &parent_shard, parent, true)?;
                    parents.insert(IdVer::new(parent.id, parent.ver), parent_shard);
                    // Do not keep the parent in the engine as we only use the parent's mem-table
                    // for children.
                    if let Some(normal_shard) = normal_shard {
                        self.insert_shard(normal_shard);
                    } else {
                        self.remove_shard(parent.id);
                    }
                }
            }
        }

        let concurrency = usize::from_str(&env::var("RECOVERY_CONCURRENCY").unwrap_or_default())
            .unwrap_or_else(|_| std::cmp::min(num_cpus::get() * 8, 64));
        let load_shards_time = Instant::now_coarse();
        info!("load_shards: start"; "shards" => metas.len(), "concurrency" => concurrency);
        let (token_tx, token_rx) = tikv_util::mpsc::bounded(concurrency);
        for _ in 0..concurrency {
            token_tx.send(true).unwrap();
        }
        for meta in metas.values() {
            tikv_util::set_current_region(meta.id);
            let meta = meta.clone();
            let engine = self.clone();
            let recoverer = recoverer.clone();
            let token_tx = token_tx.clone();
            token_rx.recv().unwrap();
            let parent_shard = meta.parent.as_ref().map(|parent_meta| {
                parents
                    .get(&IdVer::new(parent_meta.id, parent_meta.ver))
                    .cloned()
                    .unwrap()
            });
            let load_table_filter = load_table_filter.clone();
            std::thread::spawn(move || {
                tikv_util::set_current_region(meta.id);
                let shard = engine
                    .load_and_ingest_shard(&meta, load_table_filter, recoverer.meta_pack_reader())
                    .unwrap();
                if let Some(parent) = parent_shard {
                    shard.add_parent_data(parent)
                }
                recoverer.recover(&engine, &shard, &meta, false).unwrap();
                token_tx.send(true).unwrap();
            });
        }
        for _ in 0..concurrency {
            token_rx.recv().unwrap();
        }

        let end_time = Instant::now_coarse();
        let load_parents_dur = load_shards_time.saturating_duration_since(start_time);
        let total_dur = end_time.saturating_duration_since(start_time);
        PREPARE_COUNTER_VEC
            .load_parent_shards
            .inc_by(parents.len() as u64);
        PREPARE_COUNTER_VEC.load_shards.inc_by(metas.len() as u64);
        info!("load_shards: finished";
            "shards" => metas.len(),
            "parents" => parents.len(),
            "takes" => ?total_dur,
            "load_parents" => ?load_parents_dur,
        );
        Ok(())
    }

    pub fn set_loaded(&self) {
        self.loaded.store(true, Ordering::Relaxed);
    }

    // This method is used for merged_engine to prepare remote files on adding
    // keyspace.
    pub fn prepare_shards(&self, metas: &collections::HashMap<u64, ShardMeta>) -> Result<()> {
        let concurrency = usize::from_str(&env::var("RECOVERY_CONCURRENCY").unwrap_or_default())
            .unwrap_or_else(|_| std::cmp::min(num_cpus::get() * 4, 64));
        info!("prepare concurrency {}", concurrency);
        let (result_tx, result_rx) = tikv_util::mpsc::bounded(concurrency);
        let runtime = self.fs.get_runtime();
        let mut msg_count = 0;

        let mut prepare_meta = |meta: &ShardMeta| -> Result<()> {
            let engine = self.clone();
            let result_tx = result_tx.clone();
            let cs = meta.to_change_set();
            let prepare_type = FilePrepareType::from_shard_meta(meta);
            let task = move || -> Result<()> {
                tikv_util::set_current_region(cs.shard_id);
                // Not using tiny meta as it's designed for start up by now.
                // And it's unlikely that tiny meta existed in this scenario.
                engine.prepare_change_set(
                    cs,
                    PrepareOpts {
                        prepare_type,
                        encryption_key: None, // Get from snapshot.
                        ..Default::default()
                    },
                )?;
                Ok(())
            };
            runtime.spawn_blocking(move || {
                let _ = result_tx.send(tikv_util::init_task_local_sync(task));
            });
            if msg_count < concurrency {
                msg_count += 1;
            } else {
                result_rx.recv().unwrap()?;
            }
            Ok(())
        };

        let mut parents = collections::HashSet::default();
        for meta in metas.values() {
            if let Some(parent) = &meta.parent {
                let id_ver = IdVer::new(parent.id, parent.ver);
                if !parents.contains(&id_ver) {
                    prepare_meta(parent.as_ref())?;
                    parents.insert(id_ver);
                }
            }

            prepare_meta(meta)?;
        }
        for _ in 0..msg_count {
            result_rx.recv().unwrap()?;
        }
        Ok(())
    }

    /// Should close engine explicitly if engine is not useful anymore but
    /// process is still running.
    pub fn close(&self) {
        info!(
            "Close kvengine {} and stop the background worker, core ref count {}",
            self.get_engine_id(),
            Arc::strong_count(&self.core)
        );
        self.shutting_down.store(true, Ordering::Release);
        if !self.opts.for_restore {
            self.compact_tx.send(CompactMsg::Stop).unwrap();
        }
        self.flush_tx.send(FlushMsg::Stop).unwrap();
        self.free_tx.send(FreeMemMsg::Stop).unwrap();

        self.join_workers();
    }

    pub fn notify_memtables_size(&self, size: u64) {
        let tag = ShardTag::new(self.get_engine_id(), IdVer::default());
        self.store_limiter.update_usage(&tag, size);
    }
}

pub struct EngineCore {
    pub(crate) engine_id: AtomicU64,
    pub(crate) shards: papaya::HashMap<u64, Arc<Shard>>,
    pub(crate) keyspace_shards:
        DashMap<u32 /* keyspace id */, Arc<DashSet<u64 /* shard_id */>>>,
    pub opts: Arc<Options>,
    pub per_keyspace_configs: Arc<HashMap<u32, PerKeyspaceConfig>>,
    pub(crate) flush_tx: mpsc::Sender<FlushMsg>,
    pub(crate) compact_tx: mpsc::Sender<CompactMsg>,
    pub(crate) fs: Arc<dyn dfs::Dfs>,
    pub(crate) cache: BlockCache,
    pub(crate) fd_cache: FdCache,
    pub(crate) value_cache: Option<ValueCache>,
    pub(crate) columnar_meta_cache: ColumnarMetaCache,
    pub comp_client: CompactionClient,
    pub(crate) id_allocator: Arc<dyn IdAllocator>,
    pub(crate) managed_safe_ts: AtomicU64,
    pub(crate) rate_limiter: Arc<IoRateLimiter>,
    pub(crate) store_limiter: Arc<StoreLimiter>,
    pub(crate) free_tx: mpsc::Sender<FreeMemMsg>,
    pub(crate) loaded: AtomicBool,
    pub(crate) file_locks: Vec<Mutex<()>>,
    pub(crate) shutting_down: AtomicBool,
    pub(crate) ks_safepoint_v2: Option<Arc<DashMap<u32, u64>>>,
    pub(crate) master_key: MasterKey,
    pub(crate) txn_chunk_mgr: TxnChunkManager,
    pub(crate) ia_ctx: IaCtx,
    pub(crate) files_in_blacklist: Arc<HashSet<u64>>,
    pub(crate) schema_files: Arc<DashMap<u64, SchemaFile>>,
    pub(crate) dfs_load_limiter: DfsLoadLimiter,
    pub(crate) meta_pack_scheduler: Option<MetaPackScheduler>,
    available_space_bytes: AtomicU64, // Set during store heartbeat.
    worker_handles: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl Drop for EngineCore {
    fn drop(&mut self) {
        info!("Drop kvengine {:?} core ", self.get_engine_id());
    }
}

impl EngineCore {
    pub fn set_engine_id(&self, engine_id: u64) {
        self.engine_id.store(engine_id, Ordering::Release);
    }

    pub fn get_engine_id(&self) -> u64 {
        self.engine_id.load(Ordering::Acquire)
    }

    // This method is also used by native-br for cluster restore.
    pub fn read_meta(
        meta_iter: &mut impl MetaIterator,
    ) -> Result<(HashMap<u64, ShardMeta>, HashSet<u64>)> {
        let mut metas = HashMap::new();
        let mut files_in_blacklist = HashSet::default();
        let engine_id = meta_iter.engine_id();
        meta_iter.iterate(|cs| {
            let meta = ShardMeta::new(engine_id, &cs);
            metas.insert(meta.id, meta);
        })?;

        meta_iter
            .take_files_in_blacklist()
            .into_iter()
            .for_each(|id| {
                files_in_blacklist.insert(id);
            });
        Ok((metas, files_in_blacklist))
    }

    /// Load shard from meta and ingest to the engine.
    fn load_and_ingest_shard(
        &self,
        meta: &ShardMeta,
        table_filter: Option<LoadTableFilterFn>,
        meta_pack_reader: Option<&MetaPackReader>,
    ) -> Result<Arc<Shard>> {
        if let Some(shard) = self.get_shard(meta.id) {
            let write_seq = shard.get_write_sequence();
            if shard.ver == meta.ver && write_seq >= meta.data_sequence {
                info!("{} load and ingest shard: skipped", meta.tag();
                    "write_seq" => write_seq, "data_seq" => meta.data_sequence);
                return Ok(shard);
            }
        }
        info!("load and ingest shard {}", meta.tag());
        // Encryption key is not necessary for change set of snapshot.
        let change_set = self.prepare_change_set(
            meta.to_change_set(),
            PrepareOpts {
                prepare_type: FilePrepareType::from_shard_meta(meta),
                table_filter,
                encryption_key: None, // Get from snapshot.
                meta_pack_reader,
                ..Default::default()
            },
        )?;
        self.ingest(change_set, false)?;
        let shard = self.get_shard(meta.id);
        Ok(shard.unwrap())
    }

    /// Load parent shard for recovery.
    /// The shard is not ingested to the engine.
    fn load_parent_shard(
        &self,
        meta: &ShardMeta,
        table_filter: Option<LoadTableFilterFn>,
        meta_pack_reader: Option<&MetaPackReader>,
    ) -> Result<Shard> {
        info!("load parent shard {}", meta.tag());
        // Encryption key is not necessary for change set of snapshot.
        let change_set = self.prepare_change_set(
            meta.to_change_set(),
            PrepareOpts {
                prepare_type: FilePrepareType::from_shard_meta(meta),
                table_filter,
                encryption_key: None, // Get from snapshot.
                meta_pack_reader,
                ..Default::default()
            },
        )?;
        let shard = self.new_shard_from_change_set(change_set);
        shard.refresh_states();
        Ok(shard)
    }

    fn new_shard_from_change_set(&self, cs: ChangeSet) -> Shard {
        let engine_id = self.engine_id.load(Ordering::Acquire);
        let shard = Shard::new_for_ingest(engine_id, &cs, self.opts.clone(), &self.master_key);
        let data = shard.get_data();
        info!(
            "ingest shard {} mem_table_snap_version {}, inner_key_off {}, change {:?}",
            shard.tag(),
            shard.load_mem_table_snap_version(),
            data.inner_key_off,
            &cs,
        );
        let mut builder = ShardDataBuilder::new(data);
        let prepare_type = if self.opts.ignore_columnar_table_load {
            PrepareType::SstOnly
        } else {
            PrepareType::All
        };
        create_snapshot_tables(
            &mut builder,
            cs.get_snapshot(),
            &cs,
            self.opts.for_restore,
            prepare_type,
        );
        // schema_file_id may be 0 after the columnar removed.
        let schema_file = (cs.get_snapshot().has_schema_meta()
            && cs.get_snapshot().get_schema_meta().get_file_id() != 0)
            .then(|| {
                let schema_file_id = cs.get_snapshot().get_schema_meta().get_file_id();
                let runtime = self.fs.get_runtime();
                runtime
                    .block_on(self.load_schema_file(schema_file_id))
                    .unwrap()
            });
        builder.set_schema(
            cs.get_schema_version(),
            cs.get_restore_version(),
            schema_file,
        );
        builder.set_unloaded_tbls(cs.unloaded_tables);
        shard.set_data(builder.build());
        shard
    }

    pub fn ingest(&self, cs: ChangeSet, active: bool) -> Result<()> {
        let shard = Arc::new(self.new_shard_from_change_set(cs));
        shard.set_active(active);
        self.insert_keyspace_shard(shard.keyspace_id, shard.id);
        let shards = self.shards.pin();
        shards.update_or_insert(shard.id, |old| {
            let mut old_mem_tbls = old.get_data().mem_tbls.clone();
            let old_total_seq = old.get_write_sequence() + old.get_meta_sequence();
            let new_total_seq = shard.get_write_sequence() + shard.get_meta_sequence();
            // It's possible that the new version shard has the same write_sequence and meta
            // sequence, so we need to compare shard version first.
            if shard.ver > old.ver || new_total_seq > old_total_seq {
                for mem_tbl in old_mem_tbls.drain(..) {
                    self.send_free_mem_msg(FreeMemMsg::FreeMem(mem_tbl));
                }
                shard.clone()
            } else {
                info!(
                        "ingest shard {} found old shard {} already exists with higher or equal sequence, skip insert",
                        shard.tag(), old.tag();
                        "new_write_seq" => shard.get_write_sequence(),
                        "new_meta_seq" => shard.get_meta_sequence(),
                        "old_write_seq" => old.get_write_sequence(),
                        "old_meta_seq" => old.get_meta_sequence(),
                    );
                old.clone()
            }
        }, shard.clone());
        self.refresh_shard_states(&shard);
        Ok(())
    }

    pub fn get_snap_access(&self, id: u64) -> Option<SnapAccess> {
        self.get_shard(id).map(|shard| shard.new_snap_access())
    }

    pub fn get_shard(&self, id: u64) -> Option<Arc<Shard>> {
        let shards = self.shards.pin();
        shards.get(&id).cloned()
    }

    /// Iterate over shards.
    pub fn shards(&self) -> impl Iterator<Item = Arc<Shard>> + '_ {
        let ids = self.get_all_shard_id_vers();
        ids.into_iter().filter_map(move |iv| self.get_shard(iv.id))
    }

    pub fn get_shard_with_ver(&self, shard_id: u64, shard_ver: u64) -> Result<Arc<Shard>> {
        let shard = self.get_shard(shard_id).ok_or(Error::ShardNotFound)?;
        if shard.ver != shard_ver {
            warn!(
                "shard {} version not match, request {}",
                shard.tag(),
                shard_ver,
            );
            return Err(Error::ShardNotMatch);
        }
        Ok(shard)
    }

    pub fn remove_shard(&self, shard_id: u64) -> bool {
        let shards = self.shards.pin();
        let x = shards.remove(&shard_id).cloned();
        drop(shards);
        if let Some(shard) = x {
            let keyspace_id = shard.keyspace_id;
            let shards_entry = self
                .keyspace_shards
                .get(&keyspace_id)
                .map(|r| r.value().clone());
            if let Some(shards) = shards_entry {
                shards.remove(&shard_id);
            }
            return true;
        }
        false
    }

    pub fn remove_keyspace_shards(&self, keyspace_id: u32) -> Option<Vec<u64>> {
        let (_, shards) = self.keyspace_shards.remove(&keyspace_id)?;
        let shard_ids: Vec<u64> = shards.iter().map(|r| *r).collect();

        let shards = self.shards.pin();
        for shard_id in &shard_ids {
            shards.remove(shard_id);
        }
        Some(shard_ids)
    }

    pub fn insert_shard(&self, shard: Arc<Shard>) -> Option<Arc<Shard>> {
        let shard_id = shard.id;
        let keyspace_id = shard.keyspace_id;
        self.insert_keyspace_shard(keyspace_id, shard_id);
        let shards = self.shards.pin();
        shards.insert(shard_id, shard).cloned()
    }

    pub fn insert_shard_and_refresh(&self, shard: Arc<Shard>) {
        self.insert_shard(shard.clone());
        // Refresh after insert. Otherwise, compaction may fail to get the new shard.
        self.refresh_shard_states(&shard);
    }

    // `insert_keyspace_shard` is used by `insert_shard` and using entry to insert
    // shard in `split` and `ingest`.
    pub fn insert_keyspace_shard(&self, keyspace_id: u32, shard_id: u64) {
        if let Some(shards) = self.get_keyspace_shards(keyspace_id) {
            shards.insert(shard_id);
            return;
        }
        let shards_entry = self
            .keyspace_shards
            .entry(keyspace_id)
            .or_insert_with(|| Arc::new(DashSet::default()))
            .clone();
        shards_entry.insert(shard_id);
    }

    pub fn get_keyspace_shards(&self, keyspace_id: u32) -> Option<Arc<DashSet<u64>>> {
        self.keyspace_shards
            .get(&keyspace_id)
            .map(|r| r.value().clone())
    }

    pub fn get_files_in_blacklist(&self) -> Arc<HashSet<u64>> {
        self.files_in_blacklist.clone()
    }

    pub fn size(&self) -> u64 {
        let shards = self.shards.pin();
        shards
            .iter()
            .map(|(_, v)| v.estimated_size.load(Ordering::Relaxed) + 1)
            .reduce(|x, y| x + y)
            .unwrap_or(0)
    }

    pub(crate) fn trigger_flush(&self, shard: &Shard) -> Result<()> {
        if !shard.is_active() {
            return Ok(());
        }
        if !shard.get_initial_flushed() {
            return self.trigger_initial_flush(shard);
        }
        let data = shard.get_data();
        let mut mem_tbls = data.mem_tbls.clone();
        let mut tasks = Vec::with_capacity(mem_tbls.len() - 1);
        while let Some(mem_tbl) = mem_tbls.pop() {
            // writable mem-table's version is 0.
            if mem_tbl.get_snap_version().is_not_zero() {
                tasks.push(FlushTask::new_normal(shard, mem_tbl));
            }
        }
        if !tasks.is_empty() {
            // Send all flush tasks in one message to avoid interleaving with clear.
            // See https://github.com/tidbcloud/cloud-storage-engine/issues/1764.
            self.send_flush_msg(FlushMsg::Tasks(tasks));
        }
        Ok(())
    }

    fn trigger_initial_flush(&self, shard: &Shard) -> Result<()> {
        let mut mem_tbls = vec![];
        let data = shard.get_data();
        if !data.unloaded_tbls.is_empty() {
            let msg = format!(
                "{} trigger_initial_flush: shard has unloaded tables",
                shard.tag()
            );
            error!("{}", msg; "unloaded_tables" => ?data.unloaded_tbls.keys());
            return Err(box_err!("{}", msg));
        }
        // A newly split shard's meta_sequence is an in-mem state until initial flush.
        let data_sequence = shard.get_meta_sequence();
        let base_version = shard.get_base_version();
        let snap_version = SnapVersion::new(base_version, data_sequence);
        let mut max_ts = shard.get_sst_max_ts();
        let mut properties: Option<kvenginepb::Properties> = None;
        for mem_tbl in &data.mem_tbls.as_slice()[1..] {
            debug!(
                "trigger initial flush check mem table snap version {}, size {}",
                mem_tbl.get_snap_version(),
                mem_tbl.size(),
            );
            if mem_tbl.get_snap_version() <= snap_version {
                if properties.is_none() {
                    // To persist properties of first mem-table.
                    properties = Some(mem_tbl.get_properties().unwrap_or_default());
                    info!("{} trigger_initial_flush with properties", shard.tag();
                        "props" => ?properties,
                        "data_seq" => data_sequence,
                        "base_ver" => base_version);
                }
                if mem_tbl.has_data_in_bound(shard.data_bound()) {
                    max_ts = std::cmp::max(max_ts, mem_tbl.data_max_ts());
                    mem_tbls.push(mem_tbl.clone());
                }
            }
        }
        let columnar_table_ids = data.columnar_table_ids.clone();
        self.send_flush_msg(FlushMsg::Tasks(vec![FlushTask::new_initial(
            shard,
            InitialFlush {
                mem_tbls,
                base_version,
                data_sequence,
                shard_data: data,
                columnar_table_ids,
                max_ts,
                properties,
            },
        )]));
        Ok(())
    }

    pub fn build_ingest_files(
        &self,
        shard_id: u64,
        shard_ver: u64,
        mut iter: Box<dyn table::Iterator>,
        ingest_id: Vec<u8>,
        meta: ShardMeta,
    ) -> Result<kvenginepb::ChangeSet> {
        let shard = self.get_shard_with_ver(shard_id, shard_ver)?;
        let snap_version = shard.load_mem_table_snap_version();
        let mut cs = new_change_set(shard_id, shard_ver);
        let ingest_files = cs.mut_ingest_files();
        ingest_files
            .mut_properties()
            .mut_keys()
            .push(INGEST_ID_KEY.to_string());
        ingest_files.mut_properties().mut_values().push(ingest_id);
        let opts = dfs::Options::default().with_shard(shard_id, shard_ver);
        let (tx, rx) = tikv_util::mpsc::unbounded();
        let mut tbl_cnt = 0;
        let block_size = self.opts.table_builder_options.block_size;
        let max_table_size = self.opts.table_builder_options.max_table_size;
        let zstd_compression_lvl = self.opts.table_builder_options.compression_lvl;
        let checksum_type = self.comp_client.checksum_type;
        let mut builder = table::sstable::Builder::new(
            0,
            block_size,
            ZSTD_COMPRESSION,
            zstd_compression_lvl,
            checksum_type,
            shard.encryption_key.clone(),
        );
        let mut fids = vec![];

        for (id, smallest, biggest, table_meta_off) in meta.get_blob_files() {
            let blob_create =
                new_blob_create_pb(id, smallest.to_vec(), biggest.to_vec(), table_meta_off);
            warn!("ingest blob file: {}, {:?}, {:?}", id, smallest, biggest);
            ingest_files.mut_blob_creates().push(blob_create);
        }

        iter.rewind();
        while iter.valid() {
            if fids.is_empty() {
                fids = self.id_allocator.alloc_id(10).unwrap();
            }
            let id = fids.pop().unwrap();
            builder.reset(id);
            while iter.valid() {
                builder.add(iter.key(), &iter.value(), None);
                iter.next();
                if builder.estimated_size() > max_table_size || !iter.valid() {
                    info!("builder estimated_size {}", builder.estimated_size());
                    let mut buf = Vec::with_capacity(builder.estimated_size());
                    let res = builder.finish(0, &mut buf);
                    let level = meta.get_ingest_level(DataBound::new(
                        InnerKey::from_inner_buf(&res.smallest),
                        InnerKey::from_inner_buf(&res.biggest),
                        true,
                    ));
                    if level == 0 {
                        let mut offsets = vec![buf.len() as u32; NUM_CFS];
                        offsets[0] = 0;
                        for offset in offsets {
                            buf.put_u32_le(offset);
                        }
                        buf.put_u64_le(snap_version.into_inner());
                        buf.put_u32_le(NUM_CFS as u32);
                        buf.put_u32_le(MAGIC_NUMBER);
                        let l0_create =
                            new_l0_create_pb(id, res.smallest, res.biggest, buf.len() as u32);
                        ingest_files.mut_l0_creates().push(l0_create);
                    } else {
                        let tbl_create = new_table_create_pb(
                            id,
                            level,
                            0,
                            res.smallest,
                            res.biggest,
                            res.meta_offset,
                        );
                        ingest_files.mut_table_creates().push(tbl_create);
                    }
                    tbl_cnt += 1;
                    let fs = self.fs.clone();
                    let atx = tx.clone();
                    self.fs.get_runtime().spawn(async move {
                        if let Err(err) = fs.create(id, buf.into(), opts).await {
                            atx.send(Err(err)).unwrap();
                        } else {
                            atx.send(Ok(())).unwrap();
                        }
                    });
                    break;
                }
            }
        }
        let mut errs = vec![];
        for _ in 0..tbl_cnt {
            if let Err(err) = rx.recv().unwrap() {
                errs.push(err)
            }
        }
        if !errs.is_empty() {
            return Err(errs.pop().unwrap().into());
        }
        Ok(cs)
    }

    // get_all_shard_id_vers collects all the id and vers of the engine.
    // To prevent the shard change during the iteration, we iterate twice and make
    // sure there is no change during the iteration.
    // Use this method first, then get each shard by id to reduce lock contention.
    pub fn get_all_shard_id_vers(&self) -> Vec<IdVer> {
        loop {
            let id_vers = self.collect_shard_id_vers();
            let id_vers_set = HashSet::<_>::from_iter(id_vers.iter());
            let recheck = self.collect_shard_id_vers();
            if recheck.len() == id_vers_set.len()
                && recheck.iter().all(|id_ver| id_vers_set.contains(id_ver))
            {
                return id_vers;
            }
        }
    }

    // Use `get_all_shard_id_vers` first.
    pub fn collect_shard_id_vers(&self) -> Vec<IdVer> {
        let shards = self.shards.pin();
        shards
            .iter()
            .map(|(_, x)| IdVer::new(x.id, x.ver))
            .collect()
    }

    // get shards has compaction priority, return Vec<(shard_id, keyspace_id)>.
    pub fn get_pending_compaction_shards(&self) -> Vec<(u64, u32)> {
        let id_vers = self.get_all_shard_id_vers();
        id_vers
            .iter()
            .filter_map(|id_ver| {
                let shard = self.get_shard(id_ver.id)?;
                let priority = shard.compaction_priority.rl();
                if priority.is_none() {
                    return None;
                }
                Some((id_ver.id, shard.keyspace_id))
            })
            .collect()
    }

    pub fn unblock_keyspace_compaction(&self) {
        self.send_compact_msg(CompactMsg::UnblockKeyspace);
    }

    // meta_committed should be called when a change set is committed in the raft
    // group.
    // Return `None` when the shard is not found.
    pub fn meta_committed(&self, cs: &kvenginepb::ChangeSet, rejected: bool) -> Option<()> {
        if cs.has_flush() || cs.has_initial_flush() {
            if rejected {
                self.send_flush_msg(FlushMsg::Clear(cs.shard_id));
                let shard = self.get_shard(cs.shard_id)?;
                if let Err(err) = self.trigger_flush(&shard) {
                    warn!("{} trigger_flush error: {:?}", shard.tag(), err);
                }
            } else {
                let snap_version = change_set_snap_version(cs);
                let id_ver = IdVer::new(cs.shard_id, cs.shard_ver);
                self.send_flush_msg(FlushMsg::Committed((id_ver, snap_version)));
            }
        }
        if rejected
            && (cs.has_compaction()
                || cs.has_destroy_range()
                || cs.has_truncate_ts()
                || cs.has_trim_over_bound()
                || cs.has_major_compaction()
                || cs.has_columnar_compaction())
        {
            // This compaction may be conflicted with initial flush, so we have to trigger
            // next compaction if needed.
            let shard = self.get_shard(cs.shard_id)?;
            store_bool(&shard.compacting, false);
            // Notify the compaction runner otherwise the shard can't be compacted any more.
            self.send_compact_msg(CompactMsg::Applied(IdVer::new(cs.shard_id, cs.shard_ver)));
            self.refresh_shard_states(&shard);
        }
        Some(())
    }

    pub fn set_shard_active(&self, shard_id: u64, active: bool) {
        if let Some(shard) = self.get_shard(shard_id) {
            info!("shard {} set active {}", shard.tag(), active);
            shard.set_active(active);

            // Shard will become active from a previous active state, without an inactive
            // state in between. So always clear the flags.
            store_bool(&shard.compacting, false);
            self.send_flush_msg(FlushMsg::Clear(shard_id));
            self.send_compact_msg(CompactMsg::Clear(IdVer::new(shard.id, shard.ver)));

            if active {
                self.refresh_shard_states(&shard);
                if let Err(err) = self.trigger_flush(&shard) {
                    warn!("{} trigger_flush error: {:?}", shard.tag(), err);
                }
            }
        }
    }

    pub fn refresh_shard_states(&self, shard: &Shard) {
        shard.refresh_states();

        fail::fail_point!("before_engine_trigger_compact", |_| ());
        if shard.ready_to_compact() && shard.get_compaction_priority().is_some() {
            self.trigger_compact(shard.id_ver());
        }
    }

    pub fn trigger_compact(&self, id_ver: IdVer) {
        self.send_compact_msg(CompactMsg::Compact(id_ver));
    }

    #[inline]
    pub fn pause_compaction(&self, id_ver: IdVer, seq: u64) {
        self.send_compact_msg(CompactMsg::Pause { id_ver, seq });
    }

    pub fn get_cache_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    // lock_file is used to prevent race between local file gc and
    // prepare_change_set.
    pub fn lock_file(&self, file_id: u64) -> MutexGuard<'_, ()> {
        let idx = file_id as usize % FILE_LOCK_SLOTS;
        self.file_locks[idx].lock().unwrap()
    }

    pub(crate) fn send_compact_msg(&self, msg: CompactMsg) {
        if self.opts.for_restore {
            return;
        }
        if let Err(e) = self.compact_tx.send(msg) {
            assert!(self.shutting_down.load(Ordering::Acquire));
            info!(
                "Engine {} is shutting down, cannot send compact msg {:?}",
                self.get_engine_id(),
                e
            );
        }
    }

    pub(crate) fn send_flush_msg(&self, msg: FlushMsg) {
        if let Err(e) = self.flush_tx.send(msg) {
            assert!(self.shutting_down.load(Ordering::Acquire));
            info!(
                "Engine {} is shutting down, cannot send flush msg {:?}",
                self.get_engine_id(),
                e
            );
        }
    }

    pub(crate) fn send_free_mem_msg(&self, msg: FreeMemMsg) {
        if let Err(e) = self.free_tx.send(msg) {
            assert!(self.shutting_down.load(Ordering::Acquire));
            info!(
                "Engine {} is shutting down, cannot send free mem msg {:?}",
                self.get_engine_id(),
                e
            );
        }
    }

    pub fn get_master_key(&self) -> MasterKey {
        self.master_key.clone()
    }

    pub fn is_ia_enabled(&self) -> bool {
        self.ia_ctx.is_enabled()
    }

    pub fn ia_ctx(&self) -> &IaCtx {
        &self.ia_ctx
    }

    pub fn get_txn_chunk_manager(&self) -> TxnChunkManager {
        self.txn_chunk_mgr.clone()
    }

    pub fn get_keyspace_config(&self, keyspace_id: u32) -> Option<&PerKeyspaceConfig> {
        self.per_keyspace_configs.get(&keyspace_id)
    }

    pub fn available_space(&self) -> u64 {
        self.available_space_bytes.load(Ordering::Relaxed)
    }

    pub fn is_low_space(&self) -> bool {
        fail::fail_point!("engine_is_low_space", |_| true);

        self.available_space() < self.opts.low_space_threshold
    }

    pub fn set_available_space(&self, bytes: u64) {
        self.available_space_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn remove_fd_cache(&self, file_id: u64) {
        self.fd_cache.remove(file_id);
    }

    pub fn remove_schema_file(&self, file_id: u64) {
        self.schema_files.remove(&file_id);
    }

    pub fn get_value_cache(&self) -> Option<&ValueCache> {
        self.value_cache.as_ref()
    }

    fn add_worker_handle(&self, handle: thread::JoinHandle<()>) {
        self.worker_handles.lock().unwrap().push(handle);
    }

    fn join_workers(&self) {
        let handles = self
            .worker_handles
            .lock()
            .unwrap()
            .drain(..)
            .collect::<Vec<_>>();
        for handle in handles {
            if let Err(e) = handle.join() {
                warn!("Failed to join worker thread: {:?}", e);
            }
        }
    }
}

#[derive(Copy, Clone, Default)]
pub struct ShardTag {
    pub engine_id: u64,
    pub id_ver: IdVer,
}

impl ShardTag {
    pub fn new(engine_id: u64, id_ver: IdVer) -> Self {
        Self { engine_id, id_ver }
    }

    pub fn from_comp_req(req: &CompactionRequest) -> Self {
        Self {
            engine_id: req.engine_id,
            id_ver: IdVer::new(req.shard_id, req.shard_ver),
        }
    }

    // Get `engine_id` from first peer when it is None.
    pub fn from_region(engine_id: Option<u64>, region: &kvproto::metapb::Region) -> Self {
        let engine_id = engine_id.unwrap_or_else(|| {
            region
                .get_peers()
                .first()
                .map(|p| p.store_id)
                .unwrap_or_default()
        });
        Self::new(
            engine_id,
            IdVer::new(region.id, region.get_region_epoch().version),
        )
    }

    pub fn with_region_version(mut self, region_ver: u64) -> Self {
        self.id_ver.ver = region_ver;
        self
    }
}

impl Display for ShardTag {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.engine_id, self.id_ver.id, self.id_ver.ver
        )
    }
}

impl Debug for ShardTag {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdVer {
    pub id: u64,
    pub ver: u64,
}

impl fmt::Display for IdVer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.id, self.ver)
    }
}

impl IdVer {
    pub fn new(id: u64, ver: u64) -> Self {
        Self { id, ver }
    }

    pub fn from_change_set(cs: &kvenginepb::ChangeSet) -> Self {
        Self::new(cs.shard_id, cs.shard_ver)
    }
}

pub fn new_sst_filename(file_id: u64) -> PathBuf {
    PathBuf::from(format!("{:016x}.sst", file_id))
}

pub fn new_tmp_filename(file_id: u64, tmp_id: u64) -> PathBuf {
    PathBuf::from(format!("{:016x}.{}.tmp", file_id, tmp_id))
}

pub fn new_blob_filename(file_id: u64) -> PathBuf {
    PathBuf::from(format!("{:016x}.blob", file_id))
}

pub fn new_schema_filename(file_id: u64) -> PathBuf {
    PathBuf::from(format!("{:016x}.schema", file_id))
}

pub fn new_columnar_filename(file_id: u64) -> PathBuf {
    PathBuf::from(format!("{:016x}.col", file_id))
}

pub fn new_vector_index_filename(file_id: u64) -> PathBuf {
    PathBuf::from(format!("{:016x}.vec", file_id))
}

const FREE_MEM_RECV_TIMEOUT: Duration = Duration::from_secs(5);

// pub for testing.
pub enum FreeMemMsg {
    /// Free CfTable
    FreeMem(CfTable),
    /// Stop the free mem background worker
    Stop,
}

// pub for testing.
pub fn free_mem(free_rx: mpsc::Receiver<FreeMemMsg>) {
    let mut tables: collections::HashMap<*const CfTableCore, CfTable> = HashMap::default();

    fn free_table(tbl: &CfTable) {
        for txn_file in tbl.get_cf(WRITE_CF).get_txn_files() {
            txn_file.expire_ttl_cache();
        }

        ENGINE_FREE_MEM_BYTES_HISTOGRAM.observe(tbl.skip_list_size() as f64);
    }
    fn check_and_free_mem(tables: &mut collections::HashMap<*const CfTableCore, CfTable>) {
        tables.retain(|_, tbl| {
            if Arc::strong_count(&tbl.core) == 1 {
                free_table(tbl);
                return false;
            }
            true
        });
    }

    loop {
        let msg = if tables.is_empty() {
            // No tables to free, block until a message is received.
            match free_rx.recv() {
                Ok(msg) => msg,
                Err(_) => {
                    info!("Engine free mem worker channel disconnected, stopping");
                    return;
                }
            }
        } else {
            // There are tables to check, use timeout.
            match free_rx.recv_timeout(FREE_MEM_RECV_TIMEOUT) {
                Ok(msg) => msg,
                Err(RecvTimeoutError::Timeout) => {
                    check_and_free_mem(&mut tables);
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    info!("Engine free mem worker channel disconnected, stopping");
                    return;
                }
            }
        };

        match msg {
            FreeMemMsg::FreeMem(tbl) => {
                if Arc::strong_count(&tbl.core) == 1 {
                    free_table(&tbl);
                } else {
                    check_and_free_mem(&mut tables);
                    let ptr = Arc::as_ptr(&tbl.core);
                    tables.insert(ptr, tbl);
                }
            }
            FreeMemMsg::Stop => {
                drop(tables);
                info!("Engine free mem worker receive stop msg and stop now");
                return;
            }
        }
    }
}

fn create_ia_ctx(opts: Arc<Options>, fs: Arc<dyn dfs::Dfs>, meta_fd_cache: FdCache) -> IaCtx {
    if !opts.ia.mem_cap.is_zero() && !opts.ia.disk_cap.is_zero() {
        let segment_paths = opts
            .local_dirs
            .iter()
            .map(|p| p.join("ia/segment"))
            .collect();
        for segment_path in &segment_paths {
            std::fs::create_dir_all(segment_path).unwrap();
        }
        let meta_paths = opts
            .local_dirs
            .iter()
            .map(|p| p.join("ia/meta"))
            .collect::<Vec<_>>();
        for meta_path in &meta_paths {
            std::fs::create_dir_all(meta_path).unwrap();
        }
        let opts = opts.ia.to_manager_options(segment_paths).unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("ia")
            .build()
            .unwrap();
        let ia_mgr = IaManager::new(opts, fs, Some(meta_fd_cache), runtime.into()).unwrap();
        IaCtx::Enabled(ia_mgr, Arc::new(meta_paths))
    } else {
        IaCtx::Disabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_free_mem() {
        let (free_tx, free_rx) = mpsc::unbounded();
        let handle = std::thread::spawn(move || {
            free_mem(free_rx);
        });

        let tbl1 = CfTable::new();
        free_tx.send(FreeMemMsg::FreeMem(tbl1)).unwrap();

        // Delay drop.
        let tbl2 = CfTable::new();
        free_tx.send(FreeMemMsg::FreeMem(tbl2.clone())).unwrap();

        // A table with 2 Arc in free mem thread can not be released, until one of them
        // exceeds lifetime.
        let tbl3 = CfTable::new();
        let tbl3_clone = tbl3.clone();
        assert_eq!(Arc::as_ptr(&tbl3.core), Arc::as_ptr(&tbl3_clone.core));
        free_tx.send(FreeMemMsg::FreeMem(tbl3_clone)).unwrap();
        free_tx.send(FreeMemMsg::FreeMem(tbl3)).unwrap();

        std::thread::sleep(Duration::from_secs(1));
        drop(tbl2);

        let start_time = Instant::now_coarse();
        loop {
            let free_cnt = ENGINE_FREE_MEM_BYTES_HISTOGRAM.get_sample_count();
            if free_cnt == 3 {
                break;
            }

            if start_time.saturating_elapsed() > Duration::from_secs(10) {
                panic!("timeout: free counter: {}", free_cnt);
            }
            std::thread::sleep(Duration::from_millis(200));
        }

        free_tx.send(FreeMemMsg::Stop).unwrap();
        handle.join().unwrap();
    }
}
