// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    collections::{HashMap, HashSet},
    fmt,
    iter::Iterator,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering::*, *},
        Arc, RwLock,
    },
    time::Duration,
};

use api_version::{api_v2::KEYSPACE_PREFIX_LEN, ApiV2};
use bytes::{Buf, BufMut, Bytes};
use cloud_encryption::{EncryptionKey, MasterKey};
use kvenginepb::{self as pb, TxnFileRef};
use quick_cache::sync::Cache;
use rand::Rng;
use schema::schema::StorageClassSpec;
use slog_global::*;
use tikv_util::{box_err, box_try, time::Instant};
use txn_types::TimeStamp;

use crate::{
    context::{IaCtx, MetaFileCacheWeighter, PrepareType, SnapCtx},
    ia::{
        ia_auto_file::TransitResult, ia_file::IaFile, manager::IaManager, types::FileSegmentIdent,
    },
    limiter::RegionLimiter,
    metrics::ENGINE_COLUMNAR_TOO_MANY_UNCONVERTED_L0S,
    table::{
        self,
        blobtable::blobtable::BlobTable,
        columnar::{ColumnarLevel, ColumnarLevels, VectorIndexDef},
        file::InMemFile,
        fts::FtsLevels,
        get_local_dir,
        memtable::{self, CfTable},
        schema_file::SchemaFile,
        search,
        sstable::{L0Table, SsTable},
        tiny_meta::TypedTinyMeta,
        vector_index::{VectorIndex, VectorIndexes},
        BoundedDataSet, DataBound, InnerKey, OwnedInnerKey, SnapVersion, TxnFile,
    },
    util::{evenly_distribute, TxnFileRefPropertyHelper},
    *,
};

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum MajorCompactionType {
    Disable,
    Enable,
    EnableColumnar,
}

impl From<&[u8]> for MajorCompactionType {
    fn from(s: &[u8]) -> Self {
        match s {
            MANUAL_MAJOR_COMPACTION_ENABLE_COLUMNAR => MajorCompactionType::EnableColumnar,
            MANUAL_MAJOR_COMPACTION_ENABLE => MajorCompactionType::Enable,
            _ => MajorCompactionType::Disable,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ShardPendingOperations {
    pub(crate) del_prefixes: Arc<DeletePrefixes>,
    pub(crate) truncate_ts: Option<u64>,
    pub(crate) trim_over_bound: bool,
    pub(crate) manual_major_compaction: MajorCompactionType,
    pub(crate) storage_class_spec: StorageClassSpec,
    pub(crate) gc_lock_files: HashMap<u64, SnapVersion>,
}

impl ShardPendingOperations {
    fn new(keyspace_id: u32) -> Self {
        Self {
            del_prefixes: Arc::new(DeletePrefixes::new_with_keyspace_id(keyspace_id)),
            truncate_ts: None,
            trim_over_bound: false,
            manual_major_compaction: MajorCompactionType::Disable,
            storage_class_spec: StorageClassSpec::default(),
            gc_lock_files: HashMap::new(),
        }
    }
}

pub struct Shard {
    pub engine_id: u64,
    pub id: u64,
    pub ver: u64,
    pub range: ShardRange,
    pub parent_id: u64,
    pub(crate) data: RwLock<ShardData>,
    pub(crate) opt: Arc<Options>,

    // If the shard is not active, flush mem table and do compaction will ignore this shard.
    pub(crate) active: AtomicBool,

    pub(crate) properties: Properties,
    pub(crate) pending_ops: RwLock<ShardPendingOperations>,
    pub(crate) compacting: AtomicBool, // for statistics only
    pub(crate) initial_flushed: AtomicBool,

    pub(crate) base_version: AtomicU64,
    // Size of all SSTables + all blobs referred (note, not the blob table size).
    pub(crate) estimated_size: AtomicU64,
    pub(crate) estimated_entries: AtomicU64,
    pub(crate) max_ts: AtomicU64,
    pub(crate) estimated_kv_size: AtomicU64,
    pub(crate) estimated_ia_kv_size: AtomicU64,
    pub(crate) estimated_columnar_size: AtomicU64,
    pub(crate) estimated_columnar_kv_size: AtomicU64,

    pub(crate) sst_max_ts: AtomicU64, // the max_ts of sst files (mem-tables excluded)

    // Used for tombstone gc. Stats are collected from WRITE_CF & level 2+ only.
    pub(crate) lv2plus_max_ts: AtomicU64, // the max_ts of sst files
    pub(crate) lv2plus_tombs: AtomicU64,  // number of tombstone entries
    pub(crate) lv2plus_entries_write_cf: AtomicU64, // number of entries in WRITE_CF

    // Used for gc lock cf files.
    pub(crate) lv1plus_min_lock_file_max_ts: AtomicU64, // the min max_ts of all lock cf sst files.

    // Used for gc extra cf files.
    pub(crate) lv1_min_max_extra_ts: AtomicU64, // the min max_ts of all extra cf sst files.

    // meta_seq is the raft log index of the applied change set.
    // Because change set are applied in the worker thread, the value is usually smaller
    // than write_sequence.
    pub(crate) meta_seq: AtomicU64,

    // write_sequence is the raft log index of the applied write batch.
    pub(crate) write_sequence: AtomicU64,

    pub(crate) compaction_priority: RwLock<Option<CompactionPriority>>,

    pub(crate) encryption_key: Option<EncryptionKey>,

    // outdated_schema_ver is used to record the last schema outdated error in convert L0 to
    // columnar if the schema is not updated, we skip retrying the convert.
    pub(crate) outdated_schema_ver: AtomicI64,

    pub(crate) checked_schema_ver: AtomicI64,
    pub(crate) last_transit_storage_class_instant_sec: AtomicI64,
}

// Note: when add new property, consider whether to add it to following process:
// * `PropertiesHelper`
// * `is_property_need_initial_flush`
// * `is_property_need_flush`
// * `is_property_change_set`
// * `PROPERTIES_COPY_FROM_PARENT_IN_RECOVERY`

// Following properties are maintained by Shard, and should be flushed to
// `ShardMeta` for persistence.
pub const TERM_KEY: &str = "term";
pub const INGEST_ID_KEY: &str = "_ingest_id";
pub const TRIM_OVER_BOUND: &str = "_trim_over_bound";
pub const MANUAL_MAJOR_COMPACTION: &str = "_manual_major_compaction";
pub const TXN_FILE_REF: &str = "_txn_file_ref";

// Following properties are maintained by ShardMeta, and should be skipped
// during flush.
pub const DEL_PREFIXES_KEY: &str = "_del_prefixes";
pub const ENCRYPTION_KEY: &str = "_encryption";
pub const STORAGE_CLASS_KEY: &str = "_storage_class";

// Note: TERM_KEY should not be flushed during initial flush, to keep
// `ShardMeta.data_sequence` consistent with `TERM_KEY`.
#[inline]
pub fn is_property_need_initial_flush(key: &str) -> bool {
    matches!(
        key,
        INGEST_ID_KEY | TRIM_OVER_BOUND | MANUAL_MAJOR_COMPACTION | TXN_FILE_REF
    )
}

#[inline]
pub fn is_property_need_flush(key: &str) -> bool {
    matches!(key, TERM_KEY) || is_property_need_initial_flush(key)
}

// The properties must persist before advance data sequence.
// See `ShardData::all_persisted` and `PeerMsgHandler::on_raft_log_gc_tick`.
// Some properties in `is_property_need_flush` are not considered as "must
// persist" for performance:
// - TERM_KEY: Specially handle in `PeerMsgHandler::on_raft_log_gc_tick`.
// - INGEST_ID_KEY: deprecated for legacy import.
// - TRIM_OVER_BOUND: safe to ignore.
// - MANUAL_MAJOR_COMPACTION: safe to ignore.
#[inline]
pub fn is_property_must_persist(key: &str) -> bool {
    matches!(key, TXN_FILE_REF)
}

// Properties will change in parent shard during recovery.
// See `Shard::add_parent_data`.
const PROPERTIES_COPY_FROM_PARENT_IN_RECOVERY: &[&str] = &[DEL_PREFIXES_KEY, TXN_FILE_REF];

pub const TRIM_OVER_BOUND_ENABLE: &[u8] = &[1];
pub const TRIM_OVER_BOUND_DISABLE: &[u8] = b"";

pub const MANUAL_MAJOR_COMPACTION_ENABLE: &[u8] = &[1];
pub const MANUAL_MAJOR_COMPACTION_ENABLE_COLUMNAR: &[u8] = &[2];
pub const MANUAL_MAJOR_COMPACTION_DISABLE: &[u8] = b"";

pub(crate) const INITIAL_UPDATE_COUNTER: u64 = 0;

const MAX_COL_L0_FILE_COUNTS: usize = 32;
const MAX_COL_L1_FILE_COUNTS: usize = 64;
const MAX_UNCONVERTED_L0_FILE_COUNTS: usize = 32;

impl Deref for Shard {
    type Target = ShardRange;

    fn deref(&self) -> &Self::Target {
        &self.range
    }
}

impl Shard {
    pub fn new(
        engine_id: u64,
        props: &pb::Properties,
        ver: u64,
        range: ShardRange,
        inner_key_off: usize,
        opt: Arc<Options>,
        master_key: &MasterKey,
    ) -> Self {
        let encryption_key = get_shard_property(ENCRYPTION_KEY, props)
            .map(|v| master_key.decrypt_encryption_key(&v).unwrap());
        let limiter = RegionLimiter::new(
            opt.flow_control.region_memtable_limiter_options(),
            opt.flow_control.region_l0table_limiter_options(),
        );
        let now = Instant::now_coarse();
        let shard = Self {
            engine_id,
            id: props.shard_id,
            ver,
            range: range.clone(),
            parent_id: 0,
            pending_ops: RwLock::new(ShardPendingOperations::new(range.keyspace_id)),
            data: RwLock::new(ShardData::new_empty(range, inner_key_off, limiter)),
            opt,
            active: Default::default(),
            properties: Properties::new().apply_pb(props),
            compacting: Default::default(),
            initial_flushed: Default::default(),
            base_version: Default::default(),
            estimated_size: Default::default(),
            estimated_entries: Default::default(),
            max_ts: Default::default(),
            estimated_kv_size: Default::default(),
            estimated_ia_kv_size: Default::default(),
            estimated_columnar_size: Default::default(),
            estimated_columnar_kv_size: Default::default(),
            sst_max_ts: Default::default(),
            lv2plus_max_ts: Default::default(),
            lv2plus_tombs: Default::default(),
            lv2plus_entries_write_cf: Default::default(),
            lv1plus_min_lock_file_max_ts: Default::default(),
            lv1_min_max_extra_ts: Default::default(),
            meta_seq: Default::default(),
            write_sequence: Default::default(),
            compaction_priority: RwLock::new(None),
            encryption_key,
            outdated_schema_ver: Default::default(),
            checked_schema_ver: Default::default(),
            last_transit_storage_class_instant_sec: AtomicI64::new(now.second()),
        };
        {
            let mut pending_ops = shard.pending_ops.write().unwrap();
            if let Some(val) = get_shard_property(DEL_PREFIXES_KEY, props) {
                let mut del_prefixes = DeletePrefixes::unmarshal(&val, shard.keyspace_id);
                del_prefixes.schedule_at = shard.gen_rand_schedule_del_range_time();
                pending_ops.del_prefixes = Arc::new(del_prefixes);
            }
            if let Some(val) = get_shard_property(TRIM_OVER_BOUND, props) {
                if !val.is_empty() {
                    pending_ops.trim_over_bound = true;
                }
            }
            if let Some(val) = get_shard_property(MANUAL_MAJOR_COMPACTION, props) {
                if !val.is_empty() {
                    pending_ops.manual_major_compaction = MajorCompactionType::from(val.as_slice());
                }
            }
            if let Some(val) = get_shard_property(STORAGE_CLASS_KEY, props) {
                pending_ops.storage_class_spec = StorageClassSpec::unmarshal(Some(&val));
            }
        }
        shard
    }

    pub fn new_for_ingest(
        engine_id: u64,
        cs: &pb::ChangeSet,
        opt: Arc<Options>,
        master_key: &MasterKey,
    ) -> Self {
        let snap = cs.get_snapshot();
        let range = ShardRange::from_snap(snap);
        let mut shard = Self::new(
            engine_id,
            snap.get_properties(),
            cs.shard_ver,
            range,
            snap.inner_key_off as usize,
            opt,
            master_key,
        );
        if !cs.has_parent() {
            store_bool(&shard.initial_flushed, true);
        } else {
            shard.parent_id = cs.get_parent().shard_id;
        }
        shard.base_version.store(snap.base_version, Release);
        shard.meta_seq.store(cs.sequence, Release);
        shard.write_sequence.store(snap.data_sequence, Release);
        shard
    }

    fn collect_ids_from_snapshot(
        snap: &pb::Snapshot,
        prepare_type: PrepareType,
    ) -> HashMap<u64, FileMeta> {
        let prepare_sst = matches!(prepare_type, PrepareType::SstOnly | PrepareType::All);
        let prepare_columnar = matches!(prepare_type, PrepareType::ColumnarOnly | PrepareType::All);

        let mut ids = HashMap::new();
        let unconverted_l0s: HashSet<u64> = snap.get_unconverted_l0s().iter().copied().collect();
        for l0 in snap.get_l0_creates() {
            // If type is prepare columnar only, unconverted l0s should also be prepared.
            if prepare_columnar && !prepare_sst && !unconverted_l0s.contains(&l0.id) {
                continue;
            }
            ids.insert(l0.id, FileMeta::from_l0_table(l0));
        }
        if prepare_sst {
            for ln in snap.get_table_creates() {
                ids.insert(ln.id, FileMeta::from_table(ln));
            }
            for blob in snap.get_blob_creates() {
                ids.insert(blob.id, FileMeta::from_blob_table(blob));
            }
        }
        if prepare_columnar {
            for columnar in snap.get_columnar_creates() {
                ids.insert(columnar.id, FileMeta::from_columnar_table(columnar));
            }
            // `file_id` in shard meta will set to `0` when the columnar replica was
            // removed. We also need to check if the schema file is valid.
            if snap.has_schema_meta() && snap.get_schema_meta().get_file_id() > 0 {
                ids.insert(
                    snap.get_schema_meta().get_file_id(),
                    FileMeta::from_schema_meta(),
                );
            }
            for vec_index in snap.get_vector_indexes() {
                for f in vec_index.files.iter() {
                    ids.insert(f.id, FileMeta::from_vector_index_file(f));
                }
            }
        }
        ids
    }

    pub async fn from_change_set(
        tag: &str,
        ctx: &SnapCtx,
        change_set: pb::ChangeSet,
        mut mem_tbls: Vec<CfTable>,
        write_cf_only: bool,
    ) -> Result<Self> {
        let mut cs = ChangeSet::new(change_set);
        let mut ids = HashMap::new();
        let mut lock_txn_file_refs: Vec<TxnFileRef> = vec![];
        if mem_tbls.is_empty() {
            mem_tbls.push(CfTable::new());
        }
        let encryption_key = if cs.has_snapshot() {
            let snap = cs.get_snapshot();
            ids = Self::collect_ids_from_snapshot(snap, ctx.prepare_type);
            if !write_cf_only {
                lock_txn_file_refs = collect_snap_lock_txn_file_refs(snap);
            }
            box_try!(
                get_shard_property(ENCRYPTION_KEY, snap.get_properties())
                    .map(|v| ctx.master_key.decrypt_encryption_key(&v))
                    .transpose()
            )
        } else {
            None
        };
        if let IaCtx::Enabled(ia_mgr, _) = &ctx.ia_ctx {
            let mut added_files = HashSet::new();
            // For cached files, we prepare with the meta data directly to reduce latency.
            for (id, fm) in &ids {
                let file = if fm.can_use_ia() {
                    ctx.meta_file_cache.get(id)
                } else if fm.is_l0_sst_with_size() {
                    // Cache the whole file as a segment.
                    let ident = FileSegmentIdent::new(*id, 0, fm.l0_size as u64);
                    ia_mgr.is_segment_cached(ident).then_some(
                        ia_mgr
                            .get_segment_handle(ident, fm.file_type, Some(ctx.dfs.deref()))
                            .await?
                            .into_inner(),
                    )
                } else {
                    None
                };
                if let Some(file) = file {
                    cs.add_file(
                        *id,
                        file,
                        fm,
                        ctx.block_cache.clone(),
                        ctx.vector_index_cache.clone(),
                        ctx.columnar_file_cache.clone(),
                        encryption_key.clone(),
                        ctx.columnar_meta_cache.clone(),
                        TypedTinyMeta::None,
                        None,
                    )?;
                    added_files.insert(*id);
                }
            }
            ids.retain(|id, _| !added_files.contains(id));
        }
        let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = ctx.dfs.get_runtime();
        let opts = dfs::Options::default().with_shard(cs.shard_id, cs.shard_ver);
        let mut msg_count = 0;
        for (&id, fm) in &ids {
            if fm.is_schema_file() && ctx.schema_files.is_some() {
                if let Some(schema_file) = ctx.schema_files.as_ref().unwrap().get(&id) {
                    cs.set_schema_file(Some(schema_file.clone()));
                    continue;
                }
            }
            if write_cf_only && fm.get_cf() != WRITE_CF as i32 && fm.get_level() != 0 {
                continue;
            }
            let fs = ctx.dfs.clone();
            let tx = result_tx.clone();
            let fm = fm.clone();
            let tag = tag.to_string();
            let ia_ctx = if fm.can_use_ia() || fm.is_l0_sst_with_size() {
                ctx.ia_ctx.clone()
            } else {
                IaCtx::Disabled
            };
            let meta_file_cache = ctx.meta_file_cache.clone();
            runtime.spawn(async move {
                let res =
                    Self::prepare_file(id, &fm, fs.as_ref(), &opts, &ia_ctx, meta_file_cache).await;
                if let Err(err) = tx.send(res.map(|file| (id, fm, file))) {
                    error!("failed to send result"; "tag" => tag, "file_id" => id, "err" => %err);
                }
            });
            msg_count += 1;
        }
        let mut errors: Vec<Error> = vec![];
        for _ in 0..msg_count {
            match result_rx.recv().await.unwrap() {
                Ok((id, fm, file)) => {
                    cs.add_file(
                        id,
                        file,
                        &fm,
                        ctx.block_cache.clone(),
                        ctx.vector_index_cache.clone(),
                        ctx.columnar_file_cache.clone(),
                        encryption_key.clone(),
                        ctx.columnar_meta_cache.clone(),
                        TypedTinyMeta::None,
                        None,
                    )?;
                    if fm.is_schema_file() {
                        if let Some(schema_files) = ctx.schema_files.as_ref() {
                            schema_files.insert(id, cs.get_schema_file().unwrap());
                        }
                    }
                }
                Err(err) => {
                    error!("prefetch failed {:?}", &err);
                    errors.push(err);
                }
            }
        }
        if !errors.is_empty() {
            return Err(box_err!("errors is not empty: {:?}", errors));
        }

        // Load lock_txn_files:
        if !lock_txn_file_refs.is_empty() {
            let worker_pool = ctx.txn_chunk_manager.worker_pool().clone();
            let shard_id = cs.shard_id;
            let shard_ver = cs.shard_ver;
            let txn_chunk_manager = ctx.txn_chunk_manager.clone();
            cs.lock_txn_files = box_try!(
                worker_pool
                    .spawn_blocking(move || {
                        txn_chunk_manager.load_txn_files_from_refs(
                            shard_id,
                            shard_ver,
                            &lock_txn_file_refs,
                            encryption_key,
                        )
                    })
                    .await
                    .unwrap()
            );
        }

        let mut opts = Options::default();
        opts.read_columnar = ctx.read_columnar;

        let mut shard = Shard::new_for_ingest(0, &cs, Arc::new(opts), &ctx.master_key);
        let mut builder = ShardDataBuilder::new(shard.get_data());
        builder.set_mem_tbls(mem_tbls);
        create_snapshot_tables(
            &mut builder,
            cs.get_snapshot(),
            &cs,
            write_cf_only,
            ctx.prepare_type,
        );
        builder.set_schema(
            cs.get_schema_version(),
            cs.get_restore_version(),
            cs.get_schema_file(),
        );
        shard.id = cs.shard_id;
        shard.set_data(builder.build());
        Ok(shard)
    }

    async fn prepare_file(
        id: u64,
        fm: &FileMeta,
        fs: &dyn dfs::Dfs,
        opts: &dfs::Options,
        ia_ctx: &IaCtx,
        meta_file_cache: Arc<Cache<u64, Arc<dyn table::file::File>, MetaFileCacheWeighter>>,
    ) -> Result<Arc<dyn table::file::File>> {
        match ia_ctx {
            IaCtx::Disabled => fs
                .read_file(id, opts.with_type(fm.file_type))
                .await
                .map(|data| Arc::new(InMemFile::new(id, data)) as _)
                .map_err(|err| err.into()),
            IaCtx::Enabled(ia_mgr, data_dirs) => {
                if fm.can_use_ia() {
                    let file = meta_file_cache
                        .get_or_insert_async(&id, async move {
                            let data_dir = get_local_dir(data_dirs, id);
                            let data = IaFile::prepare_table_meta(
                                id,
                                fm.file_type,
                                fm.table_meta_off as u64,
                                data_dir.as_path(),
                                opts,
                                ia_mgr,
                                Some(fs),
                            )
                            .await?;
                            Self::prepare_ia_file_with_meta_data(id, fm, data, ia_mgr)
                        })
                        .await?;
                    Ok(file)
                } else if fm.is_l0_sst_with_size() {
                    // Cache the whole file as a segment.
                    let ident = FileSegmentIdent::new(id, 0, fm.l0_size as u64);
                    ia_mgr
                        .get_segment_handle(ident, fm.file_type, Some(fs))
                        .await
                        .map(|handle| handle.into_inner())
                        .map_err(|err| err.into())
                } else {
                    unreachable!()
                }
            }
        }
    }

    fn prepare_ia_file_with_meta_data(
        id: u64,
        fm: &FileMeta,
        data: Bytes,
        ia_mgr: &IaManager,
    ) -> Result<Arc<dyn table::file::File>> {
        debug_assert!(fm.can_use_ia(), "{}: {:?}", id, fm);
        let table_meta_file = Arc::new(InMemFile::new(id, data));
        let file = IaFile::open(id, fm, table_meta_file, ia_mgr.clone())?;
        Ok(Arc::new(file))
    }

    pub fn get_cf_total_size(&self, cf: usize) -> u64 {
        let mut total_size = 0;
        let data = self.get_data();
        for cf_tbl in &data.mem_tbls {
            let tbl = cf_tbl.get_cf(cf);
            total_size += tbl.size() as u64;
        }
        let shard_cf = data.get_cf(cf);
        for lh in shard_cf.levels.iter() {
            for tbl in lh.tables.iter() {
                total_size += tbl.kv_size;
            }
        }
        total_size
    }

    pub fn id_ver(&self) -> IdVer {
        IdVer::new(self.id, self.ver)
    }

    pub(crate) fn set_active(&self, active: bool) {
        self.active.store(active, Release);
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Acquire)
    }

    pub(crate) fn refresh_states(&self) {
        self.refresh_estimated_size_and_entries();
        self.refresh_compaction_priority();
    }

    pub fn refresh_estimated_size_and_entries(&self) {
        let data = self.get_data();
        let sc_spec = self.get_storage_class_spec();

        let mut lv_stats = data.get_l0_stats();
        data.for_each_level(|cf, l| {
            lv_stats.add(&data.get_level_stats(cf, l, &sc_spec), cf);
            false
        });

        data.for_each_columnar_level(|cl| {
            lv_stats.add(&data.get_columnar_level_stats(cl.level), WRITE_CF);
            false
        });

        store_u64(
            &self.estimated_size,
            lv_stats.data_size + lv_stats.blob_size,
        );
        store_u64(&self.estimated_entries, lv_stats.entries);
        store_u64(&self.sst_max_ts, lv_stats.max_ts);
        store_u64(
            &self.max_ts,
            std::cmp::max(lv_stats.max_ts, data.get_mem_table_max_ts()),
        );
        store_u64(&self.estimated_kv_size, lv_stats.kv_size);
        store_u64(&self.estimated_ia_kv_size, lv_stats.ia_kv_size);
        store_u64(&self.estimated_columnar_size, lv_stats.columnar_size);
        store_u64(&self.estimated_columnar_kv_size, lv_stats.columnar_kv_size);

        store_u64(&self.lv2plus_max_ts, lv_stats.lv2plus_max_ts);
        store_u64(&self.lv2plus_tombs, lv_stats.lv2plus_tombs);
        store_u64(
            &self.lv2plus_entries_write_cf,
            lv_stats.lv2plus_entries_write_cf,
        );
        store_u64(
            &self.lv1plus_min_lock_file_max_ts,
            lv_stats.lv1plus_min_max_lock_ts,
        );
        store_u64(&self.lv1_min_max_extra_ts, lv_stats.lv1_min_max_extra_ts);

        data.refresh_for_limiter(&self.tag());
    }

    pub(crate) fn gen_rand_schedule_del_range_time(&self) -> u64 {
        let now = time::precise_time_ns();
        let delay = rand::thread_rng().gen_range(0..self.opt.max_del_range_delay.as_nanos() as u64);
        now + delay
    }

    pub(crate) fn merge_del_prefix(&self, val: &[u8]) {
        info!("{} Shard::merge_del_prefix: {:?}", self.tag(), val);
        let mut pending_ops = self.pending_ops.write().unwrap();
        let mut del_prefixes = (*pending_ops.del_prefixes).merge_prefix(val);
        del_prefixes.schedule_at = self.gen_rand_schedule_del_range_time();
        pending_ops.del_prefixes = Arc::new(del_prefixes);
    }

    pub(crate) fn set_del_prefixes(&self, val: &[u8]) {
        info!("{} Shard::set_del_prefixes: {:?}", self.tag(), val);
        let mut pending_ops = self.pending_ops.write().unwrap();
        if val.is_empty() {
            pending_ops.del_prefixes =
                Arc::new(DeletePrefixes::new_with_keyspace_id(self.keyspace_id));
            return;
        }
        let mut del_prefixes = DeletePrefixes::unmarshal(val, self.keyspace_id);
        del_prefixes.schedule_at = self.gen_rand_schedule_del_range_time();
        pending_ops.del_prefixes = Arc::new(del_prefixes);
    }

    pub(crate) fn set_trim_over_bound(&self, val: &[u8]) -> bool {
        let mut pending_ops = self.pending_ops.write().unwrap();
        if !val.is_empty() {
            pending_ops.trim_over_bound = true;
            return true;
        }
        false
    }

    pub(crate) fn set_manual_major_compaction(&self, val: &[u8]) -> bool {
        let mut pending_ops = self.pending_ops.write().unwrap();
        pending_ops.manual_major_compaction = MajorCompactionType::from(val);
        !val.is_empty()
    }

    /// Get suggest key for region split.
    pub fn get_suggest_split_key(&self, bucket_size: u64) -> Option<Bytes> {
        let candidate_keys = self.get_candidate_inner_keys(bucket_size);
        if candidate_keys.is_empty() {
            return None;
        }
        let split_idx = candidate_keys.len() * 2 / 3;
        Some(self.to_outer_key(candidate_keys[split_idx].as_ref()))
    }

    fn to_outer_key(&self, inner_key: InnerKey<'_>) -> Bytes {
        [self.keyspace_prefix(), inner_key.deref()].concat().into()
    }

    fn get_candidate_inner_keys(&self, bucket_size: u64) -> Vec<OwnedInnerKey> {
        let data = self.get_data();
        let mut ln_tables = 0;
        data.for_each_level(|_, lvl| {
            ln_tables += lvl.tables.len();
            false
        });
        let num_tables = data.l0_tbls.len() + ln_tables;
        let mut candidate_inner_keys = Vec::with_capacity(num_tables);
        for cf in WRITE_CF..=LOCK_CF {
            let shard_cf = data.get_cf(cf);
            for lvl in &shard_cf.levels {
                for tbl in lvl.tables.iter() {
                    if tbl.smallest() > data.inner_start() {
                        // table smallest must be less than inner_end or it will not be included.
                        candidate_inner_keys.push(OwnedInnerKey::new(tbl.clone_smallest()));
                    }
                }
            }
        }
        let l0_size: u64 = data.l0_tbls.iter().map(|l0| l0.size()).sum();
        if l0_size > bucket_size {
            let max_table_size = self.opt.table_builder_options.max_table_size;
            let block_size = self.opt.table_builder_options.block_size;
            let step = max_table_size / block_size;
            for l0 in data.l0_tbls.iter() {
                for cf in WRITE_CF..=LOCK_CF {
                    if let Some(tbl) = l0.get_cf(cf) {
                        if tbl.size() < max_table_size as u64 {
                            // ignore small l0 table.
                            continue;
                        }
                        let idx = tbl.load_index();
                        if idx.num_blocks() <= step {
                            continue;
                        }
                        for i in (step..idx.num_blocks()).step_by(step) {
                            let sample_key = idx.clone_block_key(i);
                            if sample_key.as_ref() > data.inner_start()
                                && sample_key.as_ref() < data.inner_end()
                            {
                                candidate_inner_keys.push(sample_key);
                            }
                        }
                    }
                }
            }
        }
        candidate_inner_keys.sort_by(|a, b| a.as_ref().cmp(&b.as_ref()));
        candidate_inner_keys
    }

    pub fn get_evenly_split_keys(&self, count: usize, bucket_size: u64) -> Option<Vec<Bytes>> {
        if count <= 1 {
            return None;
        }
        let candidate_inner_keys = self.get_candidate_inner_keys(bucket_size);
        if candidate_inner_keys.len() < 2 {
            return None;
        }
        // Split at table boundaries.
        let steps = evenly_distribute(candidate_inner_keys.len(), count);
        let mut split_keys = Vec::with_capacity(count - 1);
        let mut key_idx = steps[0];
        for step in steps.into_iter().skip(1) {
            let split_key = self.to_outer_key(candidate_inner_keys[key_idx].as_ref());
            split_keys.push(split_key);
            key_idx += step;
        }
        split_keys.dedup();
        Some(split_keys)
    }

    pub fn get_property(&self, key: &str) -> Option<Bytes> {
        self.properties.get(key)
    }

    pub fn set_property(&self, key: &str, val: &[u8]) {
        // TODO: remove property if available.
        self.properties.set(key, val);
        // sync shard pending ops when set shard property.
        match key {
            DEL_PREFIXES_KEY => {
                self.set_del_prefixes(val);
            }
            TRIM_OVER_BOUND => {
                let mut pending_ops = self.pending_ops.write().unwrap();
                pending_ops.trim_over_bound = !val.is_empty();
            }
            MANUAL_MAJOR_COMPACTION => {
                if val.is_empty() {
                    self.properties.remove(MANUAL_MAJOR_COMPACTION);
                }
                let mut pending_ops = self.pending_ops.write().unwrap();
                pending_ops.manual_major_compaction = MajorCompactionType::from(val);
            }
            STORAGE_CLASS_KEY => {
                let mut pending_ops = self.pending_ops.write().unwrap();
                pending_ops.storage_class_spec = StorageClassSpec::unmarshal(Some(val));
            }
            _ => {}
        }
    }

    pub fn set_property_opt(&self, key: &str, val: Option<&[u8]>) {
        if let Some(val) = val {
            self.set_property(key, val);
        }
    }

    pub fn del_property(&self, key: &str) {
        self.properties.remove(key);
    }

    pub(crate) fn load_mem_table_snap_version(&self) -> SnapVersion {
        SnapVersion::new(self.get_base_version(), self.get_write_sequence())
    }

    pub fn get_all_files(&self) -> Vec<u64> {
        let data = self.get_data();
        let mut files = data.get_all_sst_files();
        files.extend(data.get_txn_chunks());
        files.extend(data.get_all_col_files());
        files.extend(data.get_all_vec_idx_files());
        files.extend(self.get_schema_file().map(|f| f.get_file_id()));
        files
    }

    pub fn get_all_sst_files(&self) -> Vec<u64> {
        let data = self.get_data();
        data.get_all_sst_files()
    }

    pub fn get_local_sst_files(
        &self,
    ) -> (
        Vec<u64>, // local_files
        Vec<u64>, // ia_files
    ) {
        let data = self.get_data();
        data.get_local_sst_files(true)
    }

    pub fn get_txn_chunks(&self) -> Vec<u64> {
        let data = self.get_data();
        data.get_txn_chunks()
    }

    pub fn get_all_col_files(&self) -> Vec<u64> {
        let data = self.get_data();
        data.get_all_col_files()
    }

    pub fn get_all_vec_idx_files(&self) -> Vec<u64> {
        let data = self.get_data();
        data.get_all_vec_idx_files()
    }

    #[inline]
    pub fn has_txn_file_locks(&self) -> bool {
        self.get_data().has_txn_file_locks()
    }

    pub(crate) fn split_mem_tables(&self, parent_mem_tbls: &[CfTable]) -> Vec<CfTable> {
        let mut new_mem_tbls = vec![CfTable::new()];
        for old_mem_tbl in parent_mem_tbls {
            if old_mem_tbl.is_force_switch() || old_mem_tbl.has_data_in_bound(self.data_bound()) {
                new_mem_tbls.push(old_mem_tbl.new_split());
            }
        }
        new_mem_tbls
    }

    pub fn get_base_version(&self) -> u64 {
        self.base_version.load(Ordering::Acquire)
    }

    // For test only.
    #[cfg(test)]
    pub fn set_base_version(&self, ver: u64) {
        self.base_version.store(ver, Ordering::Release);
    }

    pub fn get_write_sequence(&self) -> u64 {
        self.write_sequence.load(Ordering::Acquire)
    }

    pub fn update_write_sequence_on_recover(&self, seq: u64) {
        self.write_sequence.store(seq, Ordering::Release);
    }

    pub fn get_persisted_snap_version(&self) -> SnapVersion {
        self.get_data().persisted_version
    }

    pub fn get_columnar_snap_version(&self) -> SnapVersion {
        self.get_data().get_columnar_snap_version()
    }

    pub fn get_columnar_l2_snap_version(&self) -> SnapVersion {
        self.get_data().col_levels.l2_snap_version
    }

    pub fn get_columnar_table_ids(&self) -> Vec<i64> {
        self.get_data().columnar_table_ids.clone()
    }

    pub fn get_meta_sequence(&self) -> u64 {
        self.meta_seq.load(Ordering::Acquire)
    }

    pub fn get_estimated_size(&self) -> u64 {
        self.estimated_size.load(Ordering::Relaxed)
    }

    pub fn get_estimated_entries(&self) -> u64 {
        self.estimated_entries.load(Ordering::Relaxed)
    }

    // Note: `get_max_ts` would be not up-to-date.
    // Invoke `Shard::refresh_states` to refresh it.
    pub fn get_max_ts(&self) -> u64 {
        cmp::max(
            self.max_ts.load(Ordering::Relaxed),
            self.get_data().get_mem_table_max_ts(),
        )
    }

    pub fn get_sst_max_ts(&self) -> u64 {
        self.sst_max_ts.load(Ordering::Relaxed)
    }

    pub fn get_estimated_kv_size(&self) -> u64 {
        self.estimated_kv_size.load(Ordering::Relaxed)
    }

    pub fn get_estimated_ia_kv_size(&self) -> u64 {
        self.estimated_ia_kv_size.load(Ordering::Relaxed)
    }

    pub fn get_estimated_columnar_size(&self) -> u64 {
        self.estimated_columnar_size.load(Ordering::Relaxed)
    }

    pub fn get_estimated_columnar_kv_size(&self) -> u64 {
        self.estimated_columnar_kv_size.load(Ordering::Relaxed)
    }

    pub fn get_initial_flushed(&self) -> bool {
        self.initial_flushed.load(Acquire)
    }

    pub(crate) fn ready_to_destroy_range(
        del_prefixes: &DeletePrefixes,
        shard_data: &ShardData,
    ) -> bool {
        !del_prefixes.is_empty() && del_prefixes.after_scheduled_time()
        // No memtable contains data covered by deleted prefixes.
        && !shard_data.mem_tbls.iter().any(|mem_tbl| {
            del_prefixes
                .inner_delete_bounds()
                .any(|bound| mem_tbl.has_data_in_bound(bound))
        })
    }

    fn ready_to_truncate_ts(truncate_ts: &Option<u64>, shard_data: &ShardData) -> bool {
        truncate_ts.is_some()
        // No memtable contains data with version > truncate_ts.
        && !shard_data.mem_tbls.iter().any(|mem_tbl| {
            mem_tbl.data_max_ts() > truncate_ts.unwrap()
        })
    }

    fn ready_to_trim_over_bound(trim_over_bound: bool, shard_data: &ShardData) -> bool {
        trim_over_bound && !shard_data.has_mem_over_bound_data()
    }

    fn ready_to_manual_major_compaction(comp: MajorCompactionType, shard_data: &ShardData) -> bool {
        match comp {
            MajorCompactionType::Disable => false,
            MajorCompactionType::Enable => !shard_data.has_unconverted_l0s(),
            MajorCompactionType::EnableColumnar => {
                shard_data
                    .schema_file
                    .as_ref()
                    .map(|f| !f.tables_with_columnar().is_empty())
                    .unwrap_or(false)
                    && !shard_data.columnar_table_ids.is_empty()
                    && !shard_data.get_all_columnar_files().is_empty()
            }
        }
    }

    fn refresh_compaction_priority(&self) {
        let data = self.get_data();
        let pending_ops = self.pending_ops.read().unwrap();

        if Self::ready_to_destroy_range(&pending_ops.del_prefixes, &data) {
            *self.compaction_priority.write().unwrap() = Some(CompactionPriority::DestroyRange);
            return;
        } else if Self::ready_to_truncate_ts(&pending_ops.truncate_ts, &data) {
            *self.compaction_priority.write().unwrap() = Some(CompactionPriority::TruncateTs);
            return;
        } else if Self::ready_to_trim_over_bound(pending_ops.trim_over_bound, &data) {
            *self.compaction_priority.write().unwrap() = Some(CompactionPriority::TrimOverBound);
            return;
        } else if Self::ready_to_manual_major_compaction(pending_ops.manual_major_compaction, &data)
        {
            // Set major compaction priority to 2.0 to make it less likely to be picked up
            // when there are other shards waiting to be compacted.
            let is_columnar =
                pending_ops.manual_major_compaction == MajorCompactionType::EnableColumnar;
            *self.compaction_priority.write().unwrap() = if !is_columnar {
                Some(CompactionPriority::Major {
                    score: 2.0,
                    is_manual: true,
                })
            } else {
                Some(CompactionPriority::ColumnarMajor {
                    table_ids_to_add: vec![],
                    table_ids_to_clear: vec![],
                    is_manual: true,
                    schema_version: data.schema_file.as_ref().unwrap().get_version(),
                })
            };
            return;
        }
        if !data.blob_tbl_map.is_empty() {
            let blob_table_utilization = {
                let mut in_use_blob_size = 0;
                let mut total_blob_size = 0;
                for l0 in &data.l0_tbls {
                    in_use_blob_size += l0.total_blob_size();
                }
                for cf in &data.cfs {
                    for lh in &cf.levels {
                        for sst in &(*lh.tables) {
                            in_use_blob_size += sst.total_blob_size();
                        }
                    }
                }

                for blob_table in data.blob_tbl_map.values() {
                    total_blob_size += blob_table.total_blob_size();
                }
                if total_blob_size == 0 {
                    1.0
                } else {
                    in_use_blob_size as f64 / total_blob_size as f64
                }
            };
            if blob_table_utilization < self.opt.blob_table_gc_ratio && !data.has_unconverted_l0s()
            {
                let mut lock = self.compaction_priority.write().unwrap();
                *lock = Some(CompactionPriority::Major {
                    score: f64::MAX,
                    is_manual: false,
                });
                return;
            }
        }

        if let Some(priority) = self.get_col_or_vec_idx_compaction_high_priority(&data) {
            let mut lock = self.compaction_priority.write().unwrap();
            *lock = Some(priority);
            return;
        }

        let mut score = 0.0;
        let mut cf_with_highest_score = -1;
        let mut level_with_highest_score = 0;
        if data.l0_tbls.len() > 2 {
            let size_score = data.get_l0_total_size() as f64 / self.opt.base_size as f64;
            let num_tbl_score = data.l0_tbls.len() as f64 / 4.0;
            score = size_score * 0.6 + num_tbl_score * 0.4;
        }
        let shard_bound = data.data_bound();
        for l0 in &data.l0_tbls {
            if !shard_bound.contains_bound(l0.data_bound()) {
                // set highest priority for newly split L0.
                score += 2.0;
                let mut lock = self.compaction_priority.write().unwrap();
                *lock = Some(CompactionPriority::L0 { score });
                return;
            }
        }
        for cf in 0..NUM_CFS {
            let scf = data.get_cf(cf);
            for lh in &scf.levels[..scf.levels.len() - 1] {
                // Use `StorageClassSpec::default()`.
                // The `sc_spec` just affect `ia_kv_size` which is not used here.
                let lvl_stats = data.get_level_stats(cf, lh, &StorageClassSpec::default());
                let lvl_score = lvl_stats.data_size as f64
                    / ((self.opt.base_size as f64) * 10f64.powf((lh.level - 1) as f64));
                if score < lvl_score {
                    score = lvl_score;
                    level_with_highest_score = lh.level;
                    cf_with_highest_score = cf as isize;
                }
            }
        }
        let priority = if score > 1.0 {
            let max_pri = if level_with_highest_score == 0 {
                CompactionPriority::L0 { score }
            } else {
                CompactionPriority::L1Plus {
                    cf: cf_with_highest_score,
                    level: level_with_highest_score,
                    score,
                }
            };
            Some(max_pri)
        } else {
            // NOTE: the columnar compaction priority should be lower than sst compaction
            // to avoid sst compaction being blocked by columnar major compaction.
            self.get_columnar_compaction_priority(&data)
                .or_else(|| self.get_vector_index_priority(&data))
        };

        // Trigger L0 compaction for test purpose.
        let priority = priority.or_else(|| {
            let handle_priority_none = || -> Option<CompactionPriority> {
                if !data.l0_tbls.is_empty() {
                    fail::fail_point!("refresh_compaction_priority_for_l0", |_| {
                        Some(CompactionPriority::L0 { score: 2.0 })
                    });
                }
                None
            };
            handle_priority_none()
        });

        let mut lock = self.compaction_priority.write().unwrap();
        *lock = priority;
    }

    fn get_col_or_vec_idx_compaction_high_priority(
        &self,
        data: &ShardData,
    ) -> Option<CompactionPriority> {
        if self.opt.ignore_columnar_table_load {
            return None;
        }

        if data.schema_file.is_none() && !data.columnar_table_ids.is_empty() {
            return Some(CompactionPriority::ColumnarClear);
        }

        // The vector index is removed from schema file, remove it.
        let Some(schema_file) = data.schema_file.as_ref() else {
            return None;
        };
        for vec_idx in data.vector_indexes.get_all() {
            if let Some(schema) = schema_file.get_table(vec_idx.table_id) {
                if !schema
                    .vector_indexes
                    .iter()
                    .any(|idx| idx.index_id == vec_idx.index_id && idx.col_id == vec_idx.col_id)
                {
                    return Some(CompactionPriority::UpdateVectorIndex {
                        score: 2.0,
                        table_id: vec_idx.table_id,
                        index_id: vec_idx.index_id,
                        col_id: vec_idx.col_id,
                        rebuild: true,
                    });
                }
            }
        }

        // ColumnarMajor compaction must be done before converting L0 to columnar.
        if !data.col_levels.unconverted_l0s.is_empty() {
            debug_assert!(!data.columnar_table_ids.is_empty());
            // Schema is outdated, wait for update or clear columnar if there are too many
            // unconverted L0.
            if self.get_outdated_schema_ver() == schema_file.get_version() {
                if data.has_too_many_unconverted_l0s() {
                    ENGINE_COLUMNAR_TOO_MANY_UNCONVERTED_L0S.inc();
                    warn!(
                        "{} schema is outdated and has {} unconverted L0 files, clear schema file and columnar files.",
                        self.tag(),
                        data.col_levels.unconverted_l0s.len()
                    );
                    return Some(CompactionPriority::ColumnarClear);
                } else {
                    // Do nothing, give it a chance to update schema file.
                    return None;
                }
            }
            fail::fail_point!("kvengine_l0_to_columnar", |_| None);
            return Some(CompactionPriority::L0ToColumnar);
        }

        None
    }

    fn get_table_ids_need_columnar_major_compaction(
        &self,
        data: &ShardData,
    ) -> (
        Vec<i64>, // table ids that need to add columnar
        Vec<i64>, // table ids that need to clear columnar
    ) {
        if data.schema_file.is_none() {
            // Return empty table ids means no table need to add or clear columnar.
            return (vec![], vec![]);
        }
        // Collect table ids that exists only in shard or schema file. If table id only
        // exists in schema file, it means the table need to add columnar; If table id
        // only exists in shard, it means the table need to clear columnar.
        let tables_in_shard = data
            .columnar_table_ids
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let tables_in_schema = data
            .schema_file
            .as_ref()
            .map(|schema| {
                schema
                    .overlap_columnar_tables(data.range.inner_start(), data.range.inner_end())
                    .into_iter()
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();
        let mut tables_to_add: Vec<i64> = tables_in_schema
            .difference(&tables_in_shard)
            .cloned()
            .collect();
        let mut tables_to_clear: Vec<i64> = tables_in_shard
            .difference(&tables_in_schema)
            .cloned()
            .collect();
        tables_to_add.sort();
        tables_to_clear.sort();
        (tables_to_add, tables_to_clear)
    }

    fn get_columnar_compaction_priority(&self, data: &ShardData) -> Option<CompactionPriority> {
        if self.opt.ignore_columnar_table_load {
            return None;
        }

        let (table_ids_to_add, table_ids_to_clear) =
            self.get_table_ids_need_columnar_major_compaction(data);
        if self.opt.build_columnar()
            && (!table_ids_to_add.is_empty() || !table_ids_to_clear.is_empty())
        {
            return Some(CompactionPriority::ColumnarMajor {
                table_ids_to_add,
                table_ids_to_clear,
                is_manual: false,
                schema_version: data.schema_file.as_ref().unwrap().get_version(),
            });
        }

        // TODO: use dependent base_size for columnar
        let col_l0_score =
            data.get_columnar_level_stats(0).columnar_size as f64 / self.opt.base_size as f64;
        let col_l1_score = data.get_columnar_level_stats(1).columnar_size as f64
            / (10 * self.opt.base_size) as f64;
        if data.schema_file.is_some()
            && (data.get_col_table_counts(0) > MAX_COL_L0_FILE_COUNTS
                || (col_l0_score >= col_l1_score && col_l0_score > 1.0))
        {
            Some(CompactionPriority::ColumnarL0 {
                score: col_l0_score,
            })
        } else if data.schema_file.is_some()
            && (data.get_col_table_counts(1) > MAX_COL_L1_FILE_COUNTS
                || (col_l0_score < col_l1_score && col_l1_score > 1.0))
        {
            if let Some(priority) = Self::maybe_override_by_vector_index(data) {
                Some(priority)
            } else {
                Some(CompactionPriority::ColumnarL1 {
                    score: col_l1_score,
                })
            }
        } else {
            None
        }
    }

    // To prevent L1 columnar compaction invalidate existing vector index, we
    // replace the L1 columnar compaction with update vector index.
    fn maybe_override_by_vector_index(data: &ShardData) -> Option<CompactionPriority> {
        if !data.vector_indexes.is_empty() {
            let first_l1_columnar = data.col_levels.levels[1].files.first().unwrap();
            let l2_snap_after_compaction = first_l1_columnar.get_snap_version().unwrap();
            for vec_idx in data.vector_indexes.get_all() {
                if vec_idx.need_rebuild() {
                    return Some(CompactionPriority::UpdateVectorIndex {
                        score: 2.0,
                        table_id: vec_idx.table_id,
                        index_id: vec_idx.index_id,
                        col_id: vec_idx.col_id,
                        rebuild: true,
                    });
                }
                if vec_idx.snap_version() < l2_snap_after_compaction {
                    return Some(CompactionPriority::UpdateVectorIndex {
                        score: 2.0,
                        table_id: vec_idx.table_id,
                        index_id: vec_idx.index_id,
                        col_id: vec_idx.col_id,
                        rebuild: false,
                    });
                }
            }
        }
        None
    }

    fn get_vector_index_priority(&self, data: &ShardData) -> Option<CompactionPriority> {
        if data.columnar_table_ids.is_empty() {
            return None;
        }
        let schema_file = data.schema_file.as_ref()?;
        let schemas = schema_file.get_vector_index_schemas();
        let mut priority = None;
        let mut max_score = 1.0;
        for schema in &schemas {
            for vec_idx in &schema.vector_indexes {
                let (score, rebuild) = self.vector_index_score(data, schema.table_id, vec_idx);
                if score > max_score {
                    max_score = score;
                    priority = Some(CompactionPriority::UpdateVectorIndex {
                        score,
                        table_id: schema.table_id,
                        index_id: vec_idx.index_id,
                        col_id: vec_idx.col_id,
                        rebuild,
                    });
                }
            }
        }
        priority
    }

    fn vector_index_score(
        &self,
        data: &ShardData,
        table_id: i64,
        vec_idx: &VectorIndexDef,
    ) -> (f64, bool) {
        // Trigger rebuild vector index file to v2 format.
        // TODO: remove this after v2 format is transitioned.
        if let Some(vec_idx) = data
            .vector_indexes
            .get(table_id, vec_idx.index_id, vec_idx.col_id)
        {
            if vec_idx.files.iter().any(|file| file.is_legacy_format()) {
                return (2.0, true);
            }
        }

        let snap_version = if let Some(vec_idx) =
            data.vector_indexes
                .get(table_id, vec_idx.index_id, vec_idx.col_id)
        {
            if vec_idx.files_count() >= self.opt.vector_index_build_options.rebuild_file_count {
                return (1.1, true);
            }
            vec_idx.snap_version()
        } else {
            SnapVersion::zero()
        };
        if !self.need_build_vector_index(data, table_id, vec_idx, snap_version) {
            return (0.0, false);
        }
        (1.1, snap_version.is_zero())
    }

    fn need_build_vector_index(
        &self,
        data: &ShardData,
        table_id: i64,
        vec_idx: &VectorIndexDef,
        snap_version: SnapVersion,
    ) -> bool {
        let mut total_row_count = 0usize;
        for col_level in &data.col_levels.levels {
            if col_level.level == 2 && snap_version.is_not_zero() {
                // The level 2 columnar files are already included in vector index.
                continue;
            }
            for file in &col_level.files {
                if !file.has_table(table_id) {
                    continue;
                }
                if snap_version.is_not_zero()
                    && file.get_snap_version().unwrap_or_default() <= snap_version
                {
                    continue;
                }
                let tbl_meta = file.get_table(table_id);
                let (_, row_count) = tbl_meta.handle_column.pack_offsets.end_offset();
                total_row_count += row_count as usize;
            }
        }

        // Note: Remember to update `collect_columnar_index_stats` when strategy here
        // is changed, because we provide a field to notify user how many rows
        // WILL build vector index.

        let total_size = total_row_count * vec_idx.dimension * 4;
        // We don't need to build vector index for small number of vectors.
        total_size >= self.opt.vector_index_build_options.delta_size
    }

    pub(crate) fn get_compaction_priority(&self) -> Option<CompactionPriority> {
        self.compaction_priority.read().unwrap().clone()
    }

    pub(crate) fn is_compacting(&self) -> bool {
        load_bool(&self.compacting)
    }

    /// Check whether to trigger compaction for tombstones.
    ///
    /// Due to high costs of major compaction, the current strategies are
    /// relatively conservative.
    pub fn check_need_gc_tombstones(&self, safe_ts: u64, gc_lock_extra: bool) -> bool {
        if self.is_compacting()
            || self.get_compaction_priority().is_some()
            || self.pending_ops.read().unwrap().manual_major_compaction
                != MajorCompactionType::Disable
        {
            return false;
        }
        if gc_lock_extra && self.check_need_gc_extra_file(safe_ts) {
            return true;
        }
        if gc_lock_extra && self.check_need_gc_lock_file(safe_ts) {
            return true;
        }
        let lv2plus_max_ts = self.lv2plus_max_ts.load(Ordering::Relaxed);
        if lv2plus_max_ts > safe_ts {
            return false;
        }

        let tombs = self.lv2plus_tombs.load(Ordering::Relaxed);
        if tombs < self.opt.compaction_tombs_count {
            return false;
        }

        let write_entries = self.lv2plus_entries_write_cf.load(Ordering::Relaxed);
        if write_entries < tombs {
            // To handle the corner case that entries & tombs are not consistent.
            return false;
        }

        let tombs_ratio = tombs as f64 / write_entries as f64;
        if tombs_ratio >= self.opt.compaction_tombs_ratio && !self.get_data().has_unconverted_l0s()
        {
            let mut priority = self.compaction_priority.write().unwrap();
            if priority.is_some() {
                return false;
            }

            info!("{} trigger major compaction for tombstones", self.tag();
                "tombs" => tombs, "write_entries" => write_entries, "ratio" => tombs_ratio,
                "max_ts" => lv2plus_max_ts, "safe_ts" => safe_ts);
            *priority = Some(CompactionPriority::Major {
                score: tombs_ratio,
                is_manual: false,
            });
            return true;
        }

        false
    }

    fn check_need_gc_lock_file(&self, safe_ts: u64) -> bool {
        let lv1plus_max_lock_ts = self.lv1plus_min_lock_file_max_ts.load(Ordering::Relaxed);
        if lv1plus_max_lock_ts == 0 || lv1plus_max_lock_ts > safe_ts {
            return false;
        }
        let mut priority = self.compaction_priority.write().unwrap();
        if priority.is_some() {
            return false;
        }
        *priority = Some(CompactionPriority::GcLockFile);
        info!("{} trigger gc lock file", self.tag(); "safe_ts" => safe_ts);
        true
    }

    fn check_need_gc_extra_file(&self, safe_ts: u64) -> bool {
        let lv1_max_extra_ts = self.lv1_min_max_extra_ts.load(Ordering::Relaxed);
        if lv1_max_extra_ts == 0 || lv1_max_extra_ts > safe_ts {
            return false;
        }
        let mut priority = self.compaction_priority.write().unwrap();
        if priority.is_some() {
            return false;
        }
        *priority = Some(CompactionPriority::GcExtraFile);
        info!("{} trigger gc extra file", self.tag(); "safe_ts" => safe_ts);
        true
    }

    pub fn has_failed_compaction(&self) -> bool {
        if self.is_compacting() {
            return false;
        }
        let priority = self.compaction_priority.read().unwrap();
        priority.is_some()
    }

    pub(crate) fn get_data(&self) -> ShardData {
        self.data.read().unwrap().clone()
    }

    pub(crate) fn set_data(&self, data: ShardData) {
        self.set_data_opt(data, true);
    }

    pub(crate) fn set_data_opt(&self, data: ShardData, check_update_counter: bool) {
        let mut guard = self.data.write().unwrap();
        if check_update_counter {
            debug_assert_eq!(
                guard.update_counter + 1,
                data.update_counter,
                "{} set_data: update counter mismatch: old: {:?}, new: {:?}",
                self.tag(),
                guard,
                data
            );
            if guard.update_counter + 1 != data.update_counter {
                warn!("{} set_data: update counter mismatch", self.tag(); "old" => ?guard, "new" => ?data);
            }
        }
        *guard = data
    }

    pub fn new_snap_access(&self) -> SnapAccess {
        SnapAccess::new(self)
    }

    pub fn get_writable_mem_table_state(
        &self,
    ) -> (
        u64,   // mem_table_size
        usize, // unpersisted_props_size
    ) {
        let guard = self.data.read().unwrap();
        let mem_tbl = &guard.mem_tbls[0];
        (mem_tbl.size(), mem_tbl.unpersisted_props_size())
    }

    pub fn get_writable_mem_table(&self) -> memtable::CfTable {
        let guard = self.data.read().unwrap();
        return guard.get_writable_mem_table().clone();
    }

    pub fn get_mem_table_max_version(&self) -> SnapVersion {
        let guard = self.data.read().unwrap();
        guard
            .mem_tbls
            .iter()
            .map(|t| t.get_snap_version())
            .max()
            .unwrap_or_default()
    }

    pub fn has_over_bound_data(&self) -> bool {
        self.get_data().has_over_bound_data()
    }

    pub fn get_trim_over_bound(&self) -> bool {
        self.pending_ops.read().unwrap().trim_over_bound
    }

    pub fn get_del_prefixes(&self) -> Arc<DeletePrefixes> {
        self.pending_ops.read().unwrap().del_prefixes.clone()
    }

    pub fn get_truncate_ts(&self) -> Option<u64> {
        self.pending_ops.read().unwrap().truncate_ts
    }

    pub fn get_storage_class_spec(&self) -> StorageClassSpec {
        self.pending_ops.read().unwrap().storage_class_spec.clone()
    }

    #[cfg(feature = "testexport")]
    pub fn storage_class_spec_equals(&self, spec: &StorageClassSpec) -> bool {
        &self.pending_ops.read().unwrap().storage_class_spec == spec
    }

    pub fn data_all_persisted(&self) -> bool {
        self.data.read().unwrap().all_persisted()
    }

    pub fn tag(&self) -> ShardTag {
        ShardTag::new(self.engine_id, IdVer::new(self.id, self.ver))
    }

    pub fn get_checked_schema_ver(&self) -> i64 {
        self.checked_schema_ver.load(Acquire)
    }

    pub fn set_checked_schema_ver(&self, schema_ver: i64) {
        self.checked_schema_ver.store(schema_ver, Ordering::Release);
    }

    pub fn get_schema_file(&self) -> Option<SchemaFile> {
        self.data.read().unwrap().schema_file.clone()
    }

    pub fn has_columnar_table(&self, table_id: i64) -> bool {
        let data = self.get_data();
        data.schema_file.is_some() && data.columnar_table_ids.contains(&table_id)
    }

    pub fn vector_index_ready(&self, table_id: i64, index_id: i64) -> bool {
        let data = self.get_data();
        let Some(schema_file) = data.schema_file.as_ref() else {
            return false;
        };
        let Some(schema) = schema_file.get_table(table_id) else {
            return false;
        };
        let vec_idx_def = schema
            .vector_indexes
            .iter()
            .find(|idx| idx.index_id == index_id);
        let Some(vec_idx_def) = vec_idx_def else {
            return false;
        };
        let vec_idx = data
            .vector_indexes
            .get(table_id, index_id, vec_idx_def.col_id);
        if vec_idx.is_some() {
            return true;
        }
        // If the table already applied the columnar major compaction, but no
        // columnar files contains the table or the columnar data is too small to
        // build vector index, treat it as ready.
        if data.columnar_table_ids.contains(&table_id)
            && !self.need_build_vector_index(&data, table_id, vec_idx_def, SnapVersion::zero())
        {
            return true;
        }
        false
    }

    pub fn has_unconverted_l0s(&self) -> bool {
        self.get_data().has_unconverted_l0s()
    }

    pub fn has_vector_index(&self) -> bool {
        !self.get_data().vector_indexes.is_empty()
    }

    pub fn get_vector_index(
        &self,
        table_id: i64,
        index_id: i64,
        col_id: i64,
    ) -> Option<VectorIndex> {
        let data = self.get_data();
        data.vector_indexes.get(table_id, index_id, col_id).cloned()
    }

    pub fn get_encryption_key(&self) -> Option<EncryptionKey> {
        self.encryption_key.clone()
    }

    pub fn is_encrypted(&self) -> bool {
        self.encryption_key.is_some()
    }

    // When recover is called multiple times, the write_sequence may be smaller than
    // the meta.data_sequence, so we need to update it before recover to avoid
    // entries unavailable error.
    pub fn sync_data_sequence(&self, meta: &ShardMeta) {
        if self.get_meta_sequence() == meta.seq && self.get_write_sequence() < meta.data_sequence {
            self.write_sequence
                .store(meta.data_sequence, Ordering::Release);
        }
    }

    pub(crate) fn ready_to_compact(&self) -> bool {
        self.is_active() && self.get_initial_flushed()
    }

    pub fn add_parent_data(&self, parent: Arc<Shard>) {
        let shard_data = self.get_data();
        let parent_data = parent.get_data();

        let mem_tbls = self.split_mem_tables(&parent_data.mem_tbls);
        let mem_tbl_vers: Vec<SnapVersion> =
            mem_tbls.iter().map(|tbl| tbl.get_snap_version()).collect();

        for prop_key in PROPERTIES_COPY_FROM_PARENT_IN_RECOVERY {
            if let Some(val) = parent.get_property(prop_key) {
                self.set_property(prop_key, &val);
            }
        }

        let lock_txn_files = parent_data.lock_txn_files.clone();

        info!("{} add parent data", self.tag();
            "mem-tables" => ?mem_tbl_vers,
            "lock_txn_files" => ?lock_txn_files.iter().map(|x| x.start_ts()).collect::<Vec<_>>(),
            "properties" => ?self.properties.multi_get(PROPERTIES_COPY_FROM_PARENT_IN_RECOVERY));
        let mut builder = ShardDataBuilder::new(shard_data);
        builder.set_mem_tbls(mem_tbls);
        builder.set_lock_txn_files(lock_txn_files);
        self.set_data(builder.build());
    }

    pub(crate) fn keyspace_prefix(&self) -> &[u8] {
        if self.keyspace_id > 0 {
            return &self.outer_start[0..KEYSPACE_PREFIX_LEN];
        }
        &[]
    }

    /// Whether the shard is empty.
    ///
    /// NOTE: use with caution.
    /// The empty status of shard is not reliable, as the applying to kvengine
    /// would be late.
    pub(crate) fn is_empty(&self) -> bool {
        let data = self.get_data();
        if data.mem_tbls.iter().any(|mem_tbl| !mem_tbl.is_empty()) {
            return false;
        }
        self.get_estimated_size() == 0
    }

    pub(crate) fn clear_finished_txn_file_refs(&self, version: SnapVersion) {
        if let Some(val) = self.get_property(TXN_FILE_REF) {
            let mut prop = TxnFileRefPropertyHelper::from_property(Some(val)).unwrap();
            prop.clear_finished_txn_file_lock(version);
            self.set_property(TXN_FILE_REF, &prop.marshall());
            debug!("{} clear finished txn file ref", self.tag(); "prop" => ?prop, "version" => version);
        }
    }

    pub(crate) fn get_outdated_schema_ver(&self) -> i64 {
        self.outdated_schema_ver.load(Ordering::Acquire)
    }

    pub(crate) fn set_outdated_schema_ver(&self, ver: i64) {
        self.outdated_schema_ver.store(ver, Ordering::Release);
    }

    pub fn set_last_transit_storage_class_instant_to_now(&self) {
        let now = Instant::now_coarse();
        self.last_transit_storage_class_instant_sec
            .fetch_max(now.second(), Ordering::Relaxed);
    }

    fn last_transit_storage_class_elapsed(&self) -> Duration {
        Instant::from_timespec_second_coarse(
            self.last_transit_storage_class_instant_sec
                .load(Ordering::Relaxed),
        )
        .saturating_elapsed()
    }

    pub fn should_try_storage_class_transition(&self) -> bool {
        self.get_storage_class_spec().can_transit_to_other_sc()
            && self.last_transit_storage_class_elapsed() > self.opt.ia.auto_ia_check_interval.0
    }

    pub fn try_transit_storage_class(&self) -> usize /* tables_transited_to_ia */ {
        let spec = self.get_storage_class_spec();
        if !spec.can_transit_to_other_sc() {
            return 0;
        }

        let tag = self.tag();
        let data = self.get_data();
        let current_ts = TimeStamp::now().into_inner();

        let mut transited_to_ia = 0;
        for level in &data.cfs[WRITE_CF].levels {
            for t in level.tables.as_ref() {
                let Some(auto_file) = t.try_get_auto_ia_file() else {
                    // Must be updating storage class spec.
                    info!("{} try_transit_sc: skip, not auto IA file", tag);
                    return transited_to_ia;
                };

                match auto_file.try_transit(current_ts) {
                    TransitResult::NoChange => {}
                    TransitResult::TransitedToIa => transited_to_ia += 1,
                }
            }
        }
        transited_to_ia
    }
}

impl BoundedDataSet for Shard {
    fn data_bound(&self) -> DataBound<'_> {
        self.range.data_bound()
    }
}

pub(crate) struct ShardDataBuilder {
    old: ShardData,
    range: Option<ShardRange>,
    inner_key_off: Option<usize>,
    mem_tbls: Option<Vec<CfTable>>,
    l0_tbls: Option<Vec<L0Table>>,
    cfs: Option<[ShardCf; 3]>,
    unloaded_tbls: Option<HashMap<u64, FileMeta>>,
    limiter: Option<RegionLimiter>,
    blob_tbls: Option<Arc<HashMap<u64, BlobTable>>>,
    lock_txn_files: Option<Vec<TxnFile>>,
    schema: Option<(
        i64, // schema_version
        u64, // restore_version
        Option<SchemaFile>,
    )>,
    columnar_levels: Option<ColumnarLevels>,
    vector_indexes: Option<VectorIndexes>,
    columnar_table_ids: Option<Vec<i64>>,
    fts_levels: Option<FtsLevels>,
    persisted_version: Option<SnapVersion>,
}

impl ShardDataBuilder {
    pub(crate) fn new(old: ShardData) -> Self {
        Self {
            old,
            range: None,
            inner_key_off: None,
            mem_tbls: None,
            l0_tbls: None,
            cfs: None,
            unloaded_tbls: None,
            limiter: None,
            blob_tbls: None,
            lock_txn_files: None,
            schema: None,
            columnar_levels: None,
            vector_indexes: None,
            columnar_table_ids: None,
            fts_levels: None,
            persisted_version: None,
        }
    }

    pub(crate) fn set_range(&mut self, range: ShardRange) {
        self.range = Some(range);
    }

    pub(crate) fn set_inner_key_off(&mut self, inner_key_off: usize) {
        self.inner_key_off = Some(inner_key_off);
    }

    pub(crate) fn set_mem_tbls(&mut self, mem_tbls: Vec<CfTable>) {
        self.mem_tbls = Some(mem_tbls);
    }

    pub(crate) fn set_l0_tbls(&mut self, l0_tbls: Vec<L0Table>) {
        self.l0_tbls = Some(l0_tbls);
    }

    pub(crate) fn set_cfs(&mut self, cfs: [ShardCf; 3]) {
        self.cfs = Some(cfs);
    }

    pub(crate) fn set_unloaded_tbls(&mut self, unloaded_tbls: HashMap<u64, FileMeta>) {
        self.unloaded_tbls = Some(unloaded_tbls);
    }

    pub(crate) fn with_new_limiter(&mut self) {
        self.limiter = Some(RegionLimiter::new_from(&self.old.limiter))
    }

    pub(crate) fn set_blob_tbls(&mut self, blob_tbls: HashMap<u64, BlobTable>) {
        self.blob_tbls = Some(Arc::new(blob_tbls));
    }

    pub(crate) fn set_lock_txn_files(&mut self, lock_txn_files: Vec<TxnFile>) {
        self.lock_txn_files = Some(lock_txn_files);
    }

    pub(crate) fn set_schema(
        &mut self,
        schema_version: i64,
        restore_version: u64,
        schema_file: Option<SchemaFile>,
    ) {
        debug_assert!(
            schema_file
                .as_ref()
                .map_or(true, |x| x.get_version() == schema_version)
        );
        self.schema = Some((schema_version, restore_version, schema_file));
    }

    #[inline]
    pub(crate) fn clear_schema(&mut self, restore_version: u64) {
        self.schema = Some((0, restore_version, None));
    }

    pub(crate) fn set_columnar_levels(&mut self, columnar_levels: ColumnarLevels) {
        self.columnar_levels = Some(columnar_levels);
    }

    pub(crate) fn set_vector_indexes(&mut self, vector_indexes: VectorIndexes) {
        self.vector_indexes = Some(vector_indexes);
    }

    pub(crate) fn set_columnar_table_ids(&mut self, columnar_table_ids: Vec<i64>) {
        self.columnar_table_ids = Some(columnar_table_ids);
    }

    pub(crate) fn set_persisted_version(&mut self, persisted_version: SnapVersion) {
        self.persisted_version = Some(persisted_version);
    }

    pub(crate) fn build(mut self) -> ShardData {
        let (schema_version, restore_version, schema_file) =
            self.schema.take().unwrap_or_else(|| {
                (
                    self.old.schema_version,
                    self.old.restore_version,
                    self.old.schema_file.clone(),
                )
            });
        let col_levels = self
            .columnar_levels
            .take()
            .unwrap_or_else(|| self.old.col_levels.clone());
        let mut vector_indexes = self
            .vector_indexes
            .take()
            .unwrap_or_else(|| self.old.vector_indexes.clone());
        // Collect the extra columnar files to read for vector index.
        // Note: there may have columnar files not transformed to vector index with
        // smaller version than l2_snap_version after region merge.
        for vec_idx in vector_indexes.get_mut_all() {
            vec_idx.update_extra_columnar_files(&col_levels);
        }
        ShardData::new(
            self.range.take().unwrap_or_else(|| self.old.range.clone()),
            self.inner_key_off.take().unwrap_or(self.old.inner_key_off),
            self.mem_tbls
                .take()
                .unwrap_or_else(|| self.old.mem_tbls.clone()),
            self.l0_tbls
                .take()
                .unwrap_or_else(|| self.old.l0_tbls.clone()),
            self.blob_tbls
                .take()
                .unwrap_or_else(|| self.old.blob_tbl_map.clone()),
            self.cfs.take().unwrap_or_else(|| self.old.cfs.clone()),
            self.unloaded_tbls
                .take()
                .unwrap_or_else(|| self.old.unloaded_tbls.clone()),
            self.lock_txn_files
                .take()
                .unwrap_or_else(|| self.old.lock_txn_files.clone()),
            self.limiter
                .take()
                .unwrap_or_else(|| self.old.limiter.clone()),
            self.old.update_counter + 1,
            schema_version,
            restore_version,
            schema_file,
            col_levels,
            vector_indexes,
            self.columnar_table_ids
                .take()
                .unwrap_or_else(|| self.old.columnar_table_ids.clone()),
            self.fts_levels
                .take()
                .unwrap_or_else(|| (*self.old.fts_levels).clone()),
            self.persisted_version
                .take()
                .unwrap_or(self.old.persisted_version),
        )
    }
}

#[derive(Clone)]
pub(crate) struct ShardData {
    pub(crate) core: Arc<ShardDataCore>,
}

impl fmt::Debug for ShardData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardData")
            .field("range", &self.range)
            .field("inner_key_off", &self.inner_key_off)
            .field("sst_files", &self.get_all_sst_files())
            .field("txn_chunks", &self.get_txn_chunks())
            .field("columnar_files", &self.get_all_columnar_files())
            .field("columnar_table_ids", &self.columnar_table_ids)
            .field("mem_table_max_ts", &self.get_mem_table_max_ts())
            .field("mem_table_size", &self.get_mem_table_size())
            .field("l0_total_size", &self.get_l0_total_size())
            .field("unloaded_tbls", &self.unloaded_tbls)
            .field("update_counter", &self.update_counter)
            .field("schema_version", &self.schema_version)
            .field("restore_version", &self.restore_version)
            .field("schema_file", &self.schema_file_id())
            .field("columnar_table_ids", &self.columnar_table_ids)
            .finish()
    }
}

impl Deref for ShardData {
    type Target = ShardDataCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl BoundedDataSet for ShardData {
    fn data_bound(&self) -> DataBound<'_> {
        self.range.data_bound()
    }
}

impl ShardData {
    pub(crate) fn new_empty(
        shard_range: ShardRange,
        inner_key_offset: usize,
        limiter: RegionLimiter,
    ) -> Self {
        Self::new(
            shard_range,
            inner_key_offset,
            vec![CfTable::new()],
            vec![],
            Arc::new(HashMap::new()),
            [ShardCf::new(0), ShardCf::new(1), ShardCf::new(2)],
            HashMap::new(),
            vec![],
            limiter,
            INITIAL_UPDATE_COUNTER,
            0,
            0,
            None,
            ColumnarLevels::new(),
            VectorIndexes::default(),
            vec![],
            FtsLevels::default(),
            SnapVersion::zero(),
        )
    }

    pub(crate) fn new(
        range: ShardRange,
        inner_key_off: usize,
        mem_tbls: Vec<memtable::CfTable>,
        l0_tbls: Vec<L0Table>,
        blob_tbl_map: Arc<HashMap<u64, BlobTable>>,
        cfs: [ShardCf; 3],
        unloaded_tbls: HashMap<u64, FileMeta>,
        lock_txn_files: Vec<TxnFile>,
        limiter: RegionLimiter,
        update_counter: u64,
        schema_version: i64,
        restore_version: u64,
        schema_file: Option<SchemaFile>,
        col_levels: ColumnarLevels,
        vector_indexes: VectorIndexes,
        columnar_table_ids: Vec<i64>,
        fts_levels: FtsLevels,
        persisted_version: SnapVersion,
    ) -> Self {
        assert!(!mem_tbls.is_empty());

        Self {
            core: Arc::new(ShardDataCore {
                range,
                inner_key_off,
                mem_tbls,
                lock_txn_files,
                l0_tbls,
                blob_tbl_map,
                cfs,
                unloaded_tbls,
                limiter,
                update_counter,
                schema_version,
                restore_version,
                schema_file,
                col_levels,
                vector_indexes,
                columnar_table_ids,
                fts_levels: Arc::new(fts_levels),
                persisted_version,
            }),
        }
    }

    #[inline]
    pub fn can_use_ia(cf: usize, level: usize) -> bool {
        cf == WRITE_CF && level > 0
    }
}

pub(crate) struct ShardDataCore {
    pub(crate) range: ShardRange,
    pub(crate) inner_key_off: usize,
    pub(crate) mem_tbls: Vec<memtable::CfTable>,
    pub(crate) lock_txn_files: Vec<TxnFile>,
    pub(crate) l0_tbls: Vec<L0Table>,
    pub(crate) blob_tbl_map: Arc<HashMap<u64, BlobTable>>,
    pub(crate) cfs: [ShardCf; 3],
    /// Tables that are not loaded from DFS yet.
    pub(crate) unloaded_tbls: HashMap<u64, FileMeta>,
    pub limiter: RegionLimiter,
    pub update_counter: u64,
    /// `schema_version` is used to indicate the schema version even if the
    /// `schema_file` is None.
    pub(crate) schema_version: i64,
    /// `restore_version` is used to indicate the restore version of the shard
    /// even if the `schema_file` is None.
    pub(crate) restore_version: u64,
    pub(crate) schema_file: Option<SchemaFile>,
    pub(crate) col_levels: ColumnarLevels,
    pub(crate) vector_indexes: VectorIndexes,
    // columnar_table_ids is the list of columnar table ids that have been columnar major
    // compacted.
    pub(crate) columnar_table_ids: Vec<i64>,
    pub(crate) fts_levels: Arc<FtsLevels>,
    pub(crate) persisted_version: SnapVersion,
}

impl Deref for ShardDataCore {
    type Target = ShardRange;

    fn deref(&self) -> &Self::Target {
        &self.range
    }
}

impl ShardDataCore {
    pub fn get_writable_mem_table(&self) -> &memtable::CfTable {
        self.mem_tbls.first().unwrap()
    }

    pub(crate) fn get_cf(&self, cf: usize) -> &ShardCf {
        &self.cfs[cf]
    }

    pub(crate) fn get_all_sst_files(&self) -> Vec<u64> {
        let (files, ia_files) = self.get_local_sst_files(false);
        debug_assert!(ia_files.is_empty());
        files
    }

    // Set `distinguish_ia` to true when need to distinguish local & ia files.
    // Note: `local_files` & `ia_files` may have duplicated ids.
    pub(crate) fn get_local_sst_files(
        &self,
        distinguish_ia: bool,
    ) -> (
        Vec<u64>, // local_files
        Vec<u64>, // ia_files
    ) {
        let mut local_files = Vec::new();
        let mut ia_files = Vec::new();

        for l0 in &self.l0_tbls {
            local_files.push(l0.id());
        }
        for blob_tbl_id in self.blob_tbl_map.keys() {
            local_files.push(*blob_tbl_id);
        }

        let shard_is_sync = self.is_sync();
        self.for_each_level(|cf, lh| {
            let cf_is_sync = shard_is_sync || !ShardData::can_use_ia(cf, lh.level);
            for tbl in lh.tables.iter() {
                debug_assert_eq!(tbl.is_sync(), cf_is_sync, "{}", tbl.id());
                if !distinguish_ia || tbl.is_sync() {
                    local_files.push(tbl.id())
                } else {
                    ia_files.push(tbl.id());
                    if !tbl.is_storage_class_ia() {
                        // Async tables with storage class not IA, must have local file.
                        local_files.push(tbl.id());
                    }
                }
            }
            false
        });
        local_files.sort_unstable();
        ia_files.sort_unstable();
        (local_files, ia_files)
    }

    pub(crate) fn get_txn_chunks(&self) -> Vec<u64> {
        let mut files = Vec::new();
        for txn_file in &self.lock_txn_files {
            files.extend_from_slice(&txn_file.chunk_ids());
        }
        for mem_tbl in &self.mem_tbls {
            let skl = mem_tbl.get_cf(WRITE_CF);
            for txn_file in skl.get_txn_files() {
                files.extend_from_slice(&txn_file.chunk_ids());
            }
        }
        files.sort_unstable();
        files
    }

    pub(crate) fn get_all_col_files(&self) -> Vec<u64> {
        let mut col_ids = vec![];
        self.for_each_columnar_level(|cl| {
            for tbl in cl.files.iter() {
                col_ids.push(tbl.id());
            }
            false
        });
        col_ids
    }

    pub(crate) fn get_all_vec_idx_files(&self) -> Vec<u64> {
        let mut vec_idx_file_ids = vec![];
        for vec_idx in self.vector_indexes.get_all() {
            for file in &vec_idx.files {
                vec_idx_file_ids.push(file.file_id())
            }
        }
        vec_idx_file_ids
    }

    #[inline]
    pub(crate) fn has_txn_file_locks(&self) -> bool {
        !self.lock_txn_files.is_empty()
    }

    #[inline]
    pub(crate) fn get_lock_txn_files(&self) -> &[TxnFile] {
        &self.lock_txn_files
    }

    pub(crate) fn for_each_level<F>(&self, mut f: F)
    where
        F: FnMut(usize /* cf */, &LevelHandler) -> bool, // stopped
    {
        for cf in 0..NUM_CFS {
            let scf = self.get_cf(cf);
            for lh in scf.levels.as_slice() {
                if f(cf, lh) {
                    return;
                }
            }
        }
    }

    pub(crate) fn for_each_columnar_level<F>(&self, mut f: F)
    where
        F: FnMut(&ColumnarLevel) -> bool, // stopped
    {
        for cl in self.col_levels.levels.iter() {
            if f(cl) {
                return;
            }
        }
    }

    pub(crate) fn get_col_table_counts(&self, level: usize) -> usize {
        assert!(level < 3);
        self.col_levels.levels[level].files.len()
    }

    pub(crate) fn get_all_columnar_files(&self) -> Vec<u64> {
        let mut files = Vec::new();
        for cl in self.col_levels.levels.iter() {
            for f in cl.files.iter() {
                files.push(f.id());
            }
        }
        files.sort_unstable();
        files
    }

    pub(crate) fn get_columnar_snap_version(&self) -> SnapVersion {
        let mut max_version = SnapVersion::zero();
        for cl in self.col_levels.levels.iter() {
            if cl.level < 2 {
                for f in cl.files.iter() {
                    max_version = cmp::max(
                        max_version,
                        f.get_snap_version().unwrap_or(SnapVersion::zero()),
                    );
                }
            } else {
                max_version = cmp::max(max_version, self.col_levels.l2_snap_version);
            }
        }
        max_version
    }

    // Return (max_ts).
    pub(crate) fn get_mem_table_max_ts(&self) -> u64 {
        let mut max_ts = 0;
        self.mem_tbls.iter().for_each(|t| {
            max_ts = cmp::max(max_ts, t.data_max_ts());
        });
        max_ts
    }

    fn get_mem_table_size(&self) -> u64 {
        self.mem_tbls.iter().map(|t| t.size()).sum()
    }

    pub(crate) fn get_l0_total_size(&self) -> u64 {
        self.get_l0_stats().data_size
    }

    pub(crate) fn get_l0_stats(&self) -> LevelStatsLite {
        let mut stats = LevelStatsLite::default();
        self.l0_tbls.iter().for_each(|l0| {
            if self.data_bound().contains_bound(l0.data_bound()) {
                stats.data_size += l0.size();
                stats.entries += l0.entries();
                stats.kv_size += l0.kv_size();
            } else {
                stats.data_size += l0.size() / 2;
                stats.entries += l0.entries() / 2;
                stats.kv_size += l0.kv_size() / 2;
            }
            stats.max_ts = cmp::max(stats.max_ts, l0.max_ts());
        });
        stats
    }

    pub(crate) fn get_level_stats(
        &self,
        cf: usize,
        level: &LevelHandler,
        sc_spec: &StorageClassSpec,
    ) -> LevelStatsLite {
        let mut stats = LevelStatsLite::default();
        level.tables.iter().enumerate().for_each(|(i, tbl)| {
            if self.is_over_bound_table(level, i, tbl) {
                stats.data_size += tbl.size() / 2;
                stats.blob_size += tbl.in_use_total_blob_size / 2;
                stats.entries += tbl.entries as u64 / 2;
                if cf == WRITE_CF {
                    stats.kv_size += tbl.kv_size / 2;
                    if level.level >= 2 {
                        stats.lv2plus_max_ts = cmp::max(stats.lv2plus_max_ts, tbl.max_ts);
                        stats.lv2plus_tombs += tbl.tombs as u64 / 2;
                        stats.lv2plus_entries_write_cf += tbl.entries as u64 / 2;
                    }
                }
            } else {
                stats.data_size += tbl.size();
                stats.blob_size += tbl.in_use_total_blob_size;
                stats.entries += tbl.entries as u64;
                if cf == WRITE_CF {
                    stats.kv_size += tbl.kv_size;
                    if level.level >= 2 {
                        stats.lv2plus_max_ts = cmp::max(stats.lv2plus_max_ts, tbl.max_ts);
                        stats.lv2plus_tombs += tbl.tombs as u64;
                        stats.lv2plus_entries_write_cf += tbl.entries as u64;
                    }
                } else if cf == LOCK_CF {
                    let max_lock_ts = tbl.get_max_lock_ts();
                    if stats.lv1plus_min_max_lock_ts == 0
                        || stats.lv1plus_min_max_lock_ts > max_lock_ts
                    {
                        stats.lv1plus_min_max_lock_ts = max_lock_ts;
                    }
                } else if cf == EXTRA_CF {
                    #[allow(clippy::collapsible_if)]
                    if stats.lv1_min_max_extra_ts == 0 || stats.lv1_min_max_extra_ts > tbl.max_ts {
                        stats.lv1_min_max_extra_ts = tbl.max_ts;
                    }
                }
            }
            stats.max_ts = cmp::max(stats.max_ts, tbl.max_ts);
        });
        if cf == WRITE_CF {
            stats.ia_kv_size = self.calc_storage_class_stats_for_level(level, sc_spec, &stats);
        }
        stats
    }

    fn calc_storage_class_stats_for_level(
        &self,
        level: &LevelHandler,
        sc_spec: &StorageClassSpec,
        stats: &LevelStatsLite,
    ) -> u64 {
        if sc_spec.must_be_ia() {
            stats.kv_size
        } else if sc_spec.can_be_ia() {
            let mut ia_kv_size = 0;
            level.tables.iter().enumerate().for_each(|(i, tbl)| {
                if tbl.is_storage_class_ia() {
                    if self.is_over_bound_table(level, i, tbl) {
                        ia_kv_size += tbl.kv_size / 2;
                    } else {
                        ia_kv_size += tbl.kv_size;
                    }
                }
            });
            ia_kv_size
        } else {
            0
        }
    }

    pub(crate) fn get_columnar_level_stats(&self, level: usize) -> LevelStatsLite {
        let level = &self.col_levels.levels[level];
        let mut stats = LevelStatsLite::default();
        level.files.iter().for_each(|tbl| {
            stats.columnar_size += tbl.size();
            stats.columnar_kv_size += tbl.get_estimated_kv_size() as u64;
        });
        stats
    }

    fn is_over_bound_table(&self, level: &LevelHandler, i: usize, tbl: &SsTable) -> bool {
        let is_bound = i == 0 || i == level.tables.len() - 1;
        is_bound && !self.data_bound().contains_bound(tbl.data_bound())
    }

    pub fn all_persisted(&self) -> bool {
        self.mem_tbls.len() == 1
            && self.mem_tbls[0].size() == 0
            && self.mem_tbls[0].unpersisted_props_size() == 0
    }

    pub(crate) fn has_mem_over_bound_data(&self) -> bool {
        for mem_tbl in &self.mem_tbls {
            for cf in 0..NUM_CFS {
                let skl = mem_tbl.get_cf(cf);
                if skl.has_over_bound_data(self.inner_start(), self.inner_end()) {
                    return true;
                }
            }
        }
        false
    }

    pub(crate) fn has_file_over_bound_data(&self) -> bool {
        let shard_bound = self.data_bound();
        for l0 in &self.l0_tbls {
            if !shard_bound.contains_bound(l0.data_bound()) {
                return true;
            }
        }
        for cf in 0..NUM_CFS {
            let scf = &self.cfs[cf];
            for level in &scf.levels {
                if let Some(level_bound) = level.get_data_bound() {
                    if !shard_bound.contains_bound(level_bound) {
                        return true;
                    }
                }
            }
        }
        for cl in &self.col_levels.levels {
            for col_file in &cl.files {
                if !shard_bound.contains_bound(col_file.data_bound()) {
                    return true;
                }
            }
        }
        false
    }

    pub fn has_over_bound_data(&self) -> bool {
        self.has_mem_over_bound_data() || self.has_file_over_bound_data()
    }

    pub fn refresh_for_limiter(&self, tag: &ShardTag) {
        let mem_table_size = self.get_mem_table_size();
        let l0_table_size = self.get_l0_total_size();
        self.limiter
            .update_usage(tag, mem_table_size, l0_table_size);
    }

    pub fn get_unconverted_l0s(&self) -> Vec<u64> {
        self.col_levels
            .unconverted_l0s
            .iter()
            .map(|l0| l0.id())
            .collect()
    }

    pub fn has_unconverted_l0s(&self) -> bool {
        !self.col_levels.unconverted_l0s.is_empty()
    }

    pub(crate) fn has_too_many_unconverted_l0s(&self) -> bool {
        self.col_levels.unconverted_l0s.len() > MAX_UNCONVERTED_L0_FILE_COUNTS
    }

    pub(crate) fn is_sync(&self) -> bool {
        self.cfs[WRITE_CF].is_sync()
    }

    pub fn schema_file_id(&self) -> Option<u64> {
        self.schema_file.as_ref().map(|x| x.get_file_id())
    }

    pub fn get_columnar_table_ids_in_schema(&self) -> Vec<i64> {
        if self.schema_file.is_none() {
            return vec![];
        }
        let mut table_ids: Vec<i64> = self
            .columnar_table_ids
            .iter()
            .filter(|&&id| {
                self.schema_file
                    .as_ref()
                    .unwrap()
                    .contains_columnar_table(id)
            })
            .copied()
            .collect();
        table_ids.sort();
        table_ids
    }
}

pub fn store_u64(ptr: &AtomicU64, val: u64) {
    ptr.store(val, Release);
}

pub fn load_u64(ptr: &AtomicU64) -> u64 {
    ptr.load(Acquire)
}

pub fn store_bool(ptr: &AtomicBool, val: bool) {
    ptr.store(val, Release)
}

pub fn load_bool(ptr: &AtomicBool) -> bool {
    ptr.load(Acquire)
}

pub(crate) struct ShardCfBuilder {
    levels: Vec<LevelHandlerBuilder>,
}

impl ShardCfBuilder {
    pub(crate) fn new(cf: usize) -> Self {
        Self {
            levels: vec![LevelHandlerBuilder::new(); CF_LEVELS[cf]],
        }
    }

    pub(crate) fn build(&mut self) -> ShardCf {
        let mut levels = Vec::with_capacity(self.levels.len());
        for i in 0..self.levels.len() {
            levels.push(self.levels[i].build(i + 1))
        }
        ShardCf { levels }
    }

    pub(crate) fn add_table(&mut self, tbl: SsTable, level: usize) {
        self.levels[level - 1].add_table(tbl)
    }
}

#[derive(Clone)]
struct LevelHandlerBuilder {
    tables: Option<Vec<SsTable>>,
}

impl LevelHandlerBuilder {
    fn new() -> Self {
        Self {
            tables: Some(vec![]),
        }
    }

    fn build(&mut self, level: usize) -> LevelHandler {
        let mut tables = self.tables.take().unwrap();
        tables.sort_by(|a, b| a.smallest().cmp(&b.smallest()));
        LevelHandler::new(level, tables)
    }

    fn add_table(&mut self, tbl: SsTable) {
        if self.tables.is_none() {
            self.tables = Some(vec![])
        }
        self.tables.as_mut().unwrap().push(tbl)
    }
}

#[derive(Clone)]
pub(crate) struct ShardCf {
    pub(crate) levels: Vec<LevelHandler>,
}

impl ShardCf {
    pub(crate) fn new(cf: usize) -> Self {
        let mut levels = vec![];
        for j in 1..=CF_LEVELS[cf] {
            levels.push(LevelHandler::new(j, vec![]));
        }
        Self { levels }
    }

    pub(crate) fn get_level(&self, level: usize) -> &LevelHandler {
        &self.levels[level - 1]
    }

    pub(crate) fn set_level(&mut self, level_handler: LevelHandler) {
        let level_idx = level_handler.level - 1;
        self.levels[level_idx] = level_handler
    }

    pub(crate) fn size(&self) -> u64 {
        self.levels
            .iter()
            .map(|lh| lh.tables.iter().map(|tbl| tbl.size()).sum::<u64>())
            .sum()
    }

    pub(crate) fn is_sync(&self) -> bool {
        for l in &self.levels {
            if !l.tables.is_empty() {
                return l.tables[0].is_sync();
            }
        }
        true
    }
}

#[derive(Default, Clone)]
pub struct LevelHandler {
    pub(crate) tables: Arc<Vec<SsTable>>,
    pub(crate) level: usize,
    pub(crate) max_ts: u64,
}

impl LevelHandler {
    pub fn new(level: usize, tables: Vec<SsTable>) -> Self {
        let mut max_ts = 0;
        for tbl in &tables {
            if max_ts < tbl.max_ts {
                max_ts = tbl.max_ts;
            }
        }
        Self {
            tables: Arc::new(tables),
            level,
            max_ts,
        }
    }

    #[maybe_async::both]
    pub async fn get(
        &self,
        key: InnerKey<'_>,
        version: u64,
        key_hash: u64,
        out_val_owner: &mut Vec<u8>,
    ) -> table::Value {
        match self.get_table(key) {
            Some(t) => {
                self.get_in_table(key, version, key_hash, t, out_val_owner)
                    .await
            }
            None => table::Value::new(),
        }
    }

    #[maybe_async::both]
    async fn get_in_table(
        &self,
        key: InnerKey<'_>,
        version: u64,
        key_hash: u64,
        tbl: &SsTable,
        out_val_owner: &mut Vec<u8>,
    ) -> table::Value {
        tbl.get(key, version, key_hash, out_val_owner, self.level)
            .await
    }

    // Note: the `key` may be on the left outside the returned sstable.
    pub(crate) fn get_table(&self, key: InnerKey<'_>) -> Option<&SsTable> {
        let idx = search(self.tables.len(), |i| self.tables[i].biggest() >= key);
        if idx >= self.tables.len() {
            return None;
        }
        Some(&self.tables[idx])
    }

    pub(crate) fn get_table_by_id(&self, id: u64) -> Option<SsTable> {
        for tbl in self.tables.as_slice() {
            if tbl.id() == id {
                return Some(tbl.clone());
            }
        }
        None
    }

    pub(crate) fn check_order(&self, cf: usize, tag: ShardTag) {
        let tables = &self.tables;
        if tables.len() <= 1 {
            return;
        }
        for i in 0..(tables.len() - 1) {
            let ti = &tables[i];
            let tj = &tables[i + 1];
            if ti.smallest() > ti.biggest()
                || ti.smallest() >= tj.smallest()
                || ti.biggest() >= tj.biggest()
            {
                panic!(
                    "[{}] check table fail table ti {} smallest {:?}, biggest {:?}, tj {} smallest {:?}, biggest {:?}, cf: {}, level: {}",
                    tag,
                    ti.id(),
                    ti.smallest(),
                    ti.biggest(),
                    tj.id(),
                    tj.smallest(),
                    tj.biggest(),
                    cf,
                    self.level,
                );
            }
        }
    }

    #[maybe_async::both]
    pub(crate) async fn get_newer(
        &self,
        key: InnerKey<'_>,
        version: u64,
        key_hash: u64,
        out_val_owner: &mut Vec<u8>,
    ) -> table::Value {
        if self.max_ts <= version {
            return table::Value::new();
        }
        if let Some(tbl) = self.get_table(key) {
            return tbl
                .get_newer(key, version, key_hash, out_val_owner, self.level)
                .await;
        }
        table::Value::new()
    }

    pub(crate) fn get_data_bound(&self) -> Option<DataBound<'_>> {
        if self.tables.is_empty() {
            return None;
        }
        let first = self.tables.first().unwrap();
        let last = self.tables.last().unwrap();
        Some(DataBound::new(first.smallest(), last.biggest(), true))
    }

    pub(crate) fn range_blocks_size(&self, ranges: &[(Bytes, Bytes)]) -> usize {
        let mut blocks_size = 0;
        let mut prev_table_id = 0;
        let mut prev_block_right = 0;
        for ran in ranges {
            let start_key = InnerKey::from_outer_key(&ran.0);
            let end_key = InnerKey::from_outer_end_key(&ran.1);
            let bound = DataBound::new(start_key, end_key, false);
            let (left, right) = bound.get_overlap_data_sets(&self.tables);
            if left == right {
                continue;
            }
            let overlap_tables = &self.tables[left..right];
            let first_table = overlap_tables.first().unwrap();
            let first_idx = first_table.load_index();
            let first_avg_block_size = first_table.kv_size as usize / first_idx.num_blocks();
            let first_block_left = first_idx.seek_block(start_key).saturating_sub(1);
            let first_block_right = if overlap_tables.len() == 1 {
                first_idx.seek_block_bigger_or_equal(end_key)
            } else {
                first_idx.num_blocks()
            };
            blocks_size += (first_block_right - first_block_left) * first_avg_block_size;
            if first_table.id() == prev_table_id && first_block_left < prev_block_right {
                // do not count duplicated block.
                blocks_size -= first_avg_block_size;
            }
            if overlap_tables.len() == 1 {
                prev_table_id = first_table.id();
                prev_block_right = first_block_right;
                continue;
            }
            if overlap_tables.len() > 2 {
                let middle_tables = &overlap_tables[1..overlap_tables.len() - 1];
                for middle_table in middle_tables {
                    blocks_size += middle_table.kv_size as usize;
                }
            }
            let last_table = overlap_tables.last().unwrap();
            let last_idx = last_table.load_index();
            let last_block_right = last_idx.seek_block_bigger_or_equal(end_key);
            let last_avg_block_size = last_table.kv_size as usize / last_idx.num_blocks();
            blocks_size += last_block_right * last_avg_block_size;
            prev_table_id = last_table.id();
            prev_block_right = last_block_right;
        }
        blocks_size
    }
}

#[derive(Default, Clone, Debug)]
pub struct Properties {
    m: Arc<RwLock<HashMap<String, Bytes>>>,
}

impl Properties {
    pub fn new() -> Self {
        Self {
            m: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn set(&self, key: &str, val: &[u8]) {
        let mut m = self.m.write().unwrap();
        m.insert(key.to_string(), Bytes::copy_from_slice(val));
    }

    pub fn set_bytes(&self, key: &str, val: Bytes) {
        let mut m = self.m.write().unwrap();
        m.insert(key.to_string(), val);
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let m = self.m.read().unwrap();
        let bin = m.get(key)?;
        Some(bin.clone())
    }

    pub fn multi_get(&self, keys: &[&str]) -> Vec<(String, Bytes)> {
        let m = self.m.read().unwrap();
        m.iter()
            .filter(|(k, _)| keys.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn remove(&self, key: &str) -> Option<Bytes> {
        let mut m = self.m.write().unwrap();
        m.remove(key)
    }

    pub fn to_pb(&self, shard_id: u64) -> kvenginepb::Properties {
        let mut props = kvenginepb::Properties::new();
        props.shard_id = shard_id;
        let m = self.m.read().unwrap();
        m.iter().for_each(|(k, v)| {
            props.keys.push(k.clone());
            props.values.push(v.to_vec());
        });
        props
    }

    pub fn apply_pb(self, props: &kvenginepb::Properties) -> Self {
        let keys = props.get_keys();
        let vals = props.get_values();
        for i in 0..keys.len() {
            let key = &keys[i];
            let val = &vals[i];
            self.set(key, val.as_slice());
        }
        self
    }

    // Complement properties from `props` if it's currently not set in self.
    pub fn complement_merge(&mut self, props: Self) {
        let other_m = props.m.read().unwrap();
        // copy to vector to avoid potential deadlock.
        let entries: Vec<(String, Bytes)> = other_m
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        drop(other_m);
        let mut self_m = self.m.write().unwrap();
        for (k, v) in entries {
            self_m.entry(k).or_insert(v);
        }
    }
}

impl PartialEq for Properties {
    fn eq(&self, other: &Self) -> bool {
        let self_m = self.m.read().unwrap();
        let other_m = other.m.read().unwrap();
        if self_m.len() != other_m.len() {
            return false;
        }
        self_m
            .iter()
            .all(|(k, v)| other_m.get(k).is_some_and(|other_v| v == other_v))
    }
}

pub fn get_shard_property(key: &str, props: &kvenginepb::Properties) -> Option<Vec<u8>> {
    debug_assert_eq!(props.get_keys().len(), props.get_values().len());
    let keys = props.get_keys();
    for i in 0..keys.len() {
        if key == keys[i] {
            return Some(props.get_values()[i].clone());
        }
    }
    None
}

pub fn set_shard_property(key: &str, props: &mut kvenginepb::Properties, val: Vec<u8>) {
    debug_assert_eq!(props.get_keys().len(), props.get_values().len());
    let keys = props.get_keys();
    for i in 0..keys.len() {
        if key == keys[i] {
            props.values[i] = val;
            return;
        }
    }
    props.keys.push(key.to_string());
    props.values.push(val);
}

/// Returns the ingest id if it is a legacy ingest request from the TiDB BR
/// tool.
#[inline]
pub fn is_legacy_ingest(ingest_files: &kvenginepb::IngestFiles) -> Option<Vec<u8>> {
    get_shard_property(INGEST_ID_KEY, ingest_files.get_properties())
}

pub fn get_splitting_start_end<'a: 'b, 'b>(
    start: &'a [u8],
    end: &'a [u8],
    split_keys: &'b [Vec<u8>],
    i: usize,
) -> (&'b [u8], &'b [u8]) {
    let start_key = if i != 0 {
        split_keys[i - 1].as_slice()
    } else {
        start as &'b [u8]
    };
    let end_key = if i == split_keys.len() {
        end
    } else {
        split_keys[i].as_slice()
    };
    (start_key, end_key)
}

#[derive(Clone)]
pub struct DeletePrefixes {
    pub(crate) prefixes: Vec<Vec<u8>>,
    // prefixes_nexts are prefix-next keys of prefixes, i.e., [prefixes[i], prefixes_nexts[i]) is
    // the range that should be destroyed.
    prefixes_nexts: Vec<Vec<u8>>,
    pub(crate) schedule_at: u64,
    pub(crate) keyspace_prefix_len: usize,
}

impl std::fmt::Debug for DeletePrefixes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeletePrefixes")
            .field(
                "prefixes",
                &self
                    .prefixes
                    .iter()
                    .map(|p| log_wrappers::Value::key(p))
                    .collect::<Vec<_>>(),
            )
            .field(
                "prefixes_nexts",
                &self
                    .prefixes_nexts
                    .iter()
                    .map(|p| log_wrappers::Value::key(p))
                    .collect::<Vec<_>>(),
            )
            .field("schedule_at", &self.schedule_at)
            .field("keyspace_prefix_len", &self.keyspace_prefix_len)
            .finish()
    }
}

impl DeletePrefixes {
    pub fn new_with_keyspace_id(keyspace_id: u32) -> Self {
        Self {
            prefixes: vec![],
            prefixes_nexts: vec![],
            schedule_at: 0,
            keyspace_prefix_len: Self::keyspace_id_to_keyspace_prefix_len(keyspace_id),
        }
    }

    fn keyspace_id_to_keyspace_prefix_len(keyspace_id: u32) -> usize {
        if keyspace_id > 0 {
            KEYSPACE_PREFIX_LEN
        } else {
            0
        }
    }

    fn gen_prefixes_nexts(prefixes: &[Vec<u8>]) -> Vec<Vec<u8>> {
        prefixes
            .iter()
            .map(|p| {
                let mut p_n = p.to_vec();
                tidb_query_common::util::convert_to_prefix_next(&mut p_n);
                p_n
            })
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.prefixes.is_empty()
    }

    pub fn after_scheduled_time(&self) -> bool {
        time::precise_time_ns() > self.schedule_at
    }

    #[must_use]
    pub fn merge_prefix(&self, prefix: &[u8]) -> Self {
        assert!(prefix.len() >= self.keyspace_prefix_len);

        let mut new_prefixes = vec![];
        for old_prefix in &self.prefixes {
            if prefix.starts_with(old_prefix) {
                // old prefix covers new prefix, do not need to change.
                return self.clone();
            }
            if old_prefix.starts_with(prefix) {
                // new prefix covers old prefix. old prefix can be dropped.
                continue;
            }
            new_prefixes.push(old_prefix.clone());
        }
        new_prefixes.push(prefix.to_vec());
        new_prefixes.sort_unstable();
        let new_prefixes_nexts = DeletePrefixes::gen_prefixes_nexts(&new_prefixes);
        Self {
            prefixes: new_prefixes,
            prefixes_nexts: new_prefixes_nexts,
            schedule_at: 0,
            keyspace_prefix_len: self.keyspace_prefix_len,
        }
    }

    pub fn merge_prefix_in_place(&mut self, prefix: &[u8]) {
        let merged = self.merge_prefix(prefix);
        *self = merged;
    }

    /// Merges multiple prefixes & self into a new one.
    ///
    /// `sorted_prefixes` must be sorted.
    ///
    /// If `sorted_prefixes` contains a prefix that is covered by another prefix
    /// in it, the covered one will not be dropped.
    ///
    /// `schedule_at` & `inner_key_off` are copied from self.
    #[must_use]
    fn merge_prefixes<'a, S: AsRef<[u8]> + 'a>(
        &self,
        sorted_prefixes: impl Iterator<Item = S>,
    ) -> Self {
        let mut new_prefixes = Vec::with_capacity(self.prefixes.len());

        let mut first_iter = self.prefixes.iter();
        let mut first_opt = first_iter.next();
        let mut second_iter = sorted_prefixes;
        let mut second_opt = second_iter.next();

        while let (Some(first), Some(second)) = (first_opt, second_opt.as_ref()) {
            let second = second.as_ref();
            if first.as_slice().starts_with(second) {
                // `second` covers `first`
                first_opt = first_iter.next();
            } else if second.starts_with(first) {
                // `first` covers `second`
                second_opt = second_iter.next();
            } else {
                // `first` and `second` are not covered by each other
                if first.as_slice() < second {
                    new_prefixes.push(first.clone());
                    first_opt = first_iter.next();
                } else {
                    new_prefixes.push(second.to_vec());
                    second_opt = second_iter.next();
                }
            }
        }

        while let Some(first) = first_opt {
            new_prefixes.push(first.clone());
            first_opt = first_iter.next();
        }
        while let Some(second) = second_opt {
            new_prefixes.push(second.as_ref().to_vec());
            second_opt = second_iter.next();
        }

        let new_prefixes_nexts = DeletePrefixes::gen_prefixes_nexts(&new_prefixes);
        Self {
            prefixes: new_prefixes,
            prefixes_nexts: new_prefixes_nexts,
            schedule_at: self.schedule_at,
            keyspace_prefix_len: self.keyspace_prefix_len,
        }
    }

    /// Merge another `DeletePrefixes` into self.
    ///
    /// `schedule_at` & `inner_key_off` are unchanged.
    pub fn merge(&mut self, other: &Self) {
        let merged = self.merge_prefixes(other.prefixes.iter().map(|p| p.as_slice()));
        *self = merged;
    }

    /// Removes all prefixes in the `other` one.
    pub fn split(&self, other: &DeletePrefixes) -> Self {
        let mut new = self.clone();
        new.prefixes.retain(|p| !other.prefixes.contains(p));
        new.prefixes_nexts = DeletePrefixes::gen_prefixes_nexts(&new.prefixes);
        new
    }

    pub fn marshal(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for prefix in &self.prefixes {
            buf.put_u16_le(prefix.len() as u16);
            buf.extend_from_slice(prefix);
        }
        buf
    }

    pub fn unmarshal(mut data: &[u8], keyspace_id: u32) -> Self {
        let mut prefixes = vec![];
        while !data.is_empty() {
            let len = data.get_u16_le() as usize;
            prefixes.push(data[..len].to_vec());
            data.advance(len);
        }
        let prefixes_nexts = DeletePrefixes::gen_prefixes_nexts(&prefixes);
        Self {
            prefixes,
            prefixes_nexts,
            schedule_at: 0,
            keyspace_prefix_len: Self::keyspace_id_to_keyspace_prefix_len(keyspace_id),
        }
    }

    pub fn build_split(&self, start: &[u8], end: &[u8]) -> Self {
        let prefixes: Vec<_> = self
            .prefixes
            .iter()
            .filter(|prefix| {
                (start <= prefix.as_slice() && prefix.as_slice() < end) || start.starts_with(prefix)
            })
            .cloned()
            .collect();
        let prefixes_nexts = DeletePrefixes::gen_prefixes_nexts(&prefixes);
        let scheduled_at = self.schedule_at;
        Self {
            prefixes,
            prefixes_nexts,
            schedule_at: scheduled_at,
            keyspace_prefix_len: self.keyspace_prefix_len,
        }
    }

    pub(crate) fn cover_full_keyspace(&self, keyspace_prefix: &[u8]) -> bool {
        let keyspace_prefix_len = self.keyspace_prefix_len;
        self.prefixes.iter().any(|p| {
            if keyspace_prefix_len >= p.len() {
                return true;
            }
            keyspace_prefix.starts_with(p.as_slice())
        })
    }

    pub(crate) fn cover_prefix(&self, inner_prefix: InnerKey<'_>) -> bool {
        let keyspace_prefix_len = self.keyspace_prefix_len;
        self.prefixes.iter().any(|p| {
            if keyspace_prefix_len >= p.len() {
                return true;
            }
            let inner_p = InnerKey::from_outer_key(p);
            inner_prefix.starts_with(inner_p.deref())
        })
    }

    pub(crate) fn cover_range(&self, inner_start: InnerKey<'_>, inner_end: InnerKey<'_>) -> bool {
        let keyspace_prefix_len = self.keyspace_prefix_len;
        self.prefixes.iter().any(|p| {
            if keyspace_prefix_len >= p.len() {
                return true;
            }
            let inner_p = InnerKey::from_outer_key(p);
            inner_start.starts_with(inner_p.deref()) && inner_end.starts_with(inner_p.deref())
        })
    }

    pub(crate) fn inner_delete_bounds(&self) -> impl Iterator<Item = DataBound<'_>> {
        let keyspace_prefix_len = self.keyspace_prefix_len;
        self.prefixes
            .iter()
            .zip(self.prefixes_nexts.iter())
            .map(move |(p, p_n)| {
                if keyspace_prefix_len >= p.len() {
                    DataBound::new(
                        InnerKey::from_inner_buf(&[]),
                        InnerKey::from_inner_buf(GLOBAL_SHARD_END_KEY),
                        false,
                    )
                } else {
                    DataBound::new(
                        InnerKey::from_outer_key(p),
                        InnerKey::from_outer_end_key(p_n),
                        false,
                    )
                }
            })
    }

    /// Get ranges that are not covered by the delete prefixes.
    pub fn get_complementary_ranges(
        &self,
        outer_start: &[u8],
        outer_end: &[u8],
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut ranges = Vec::with_capacity(self.prefixes.len() + 1);
        let mut start = outer_start.to_vec();
        for (prefix, prefix_next) in self.prefixes.iter().zip(self.prefixes_nexts.iter()) {
            if prefix_next.as_slice() <= outer_start {
                continue;
            }
            if prefix.as_slice() >= outer_end {
                break;
            }
            // `start` is possible to be equal to `prefix` here.
            // e.q. prefixes: [00001, 00002] will generate iterations (00001, 00002),
            // (00002, 00003).
            if &start < prefix {
                ranges.push((start.clone(), prefix.clone()));
            }
            start = prefix_next.clone();
        }
        if start.as_slice() < outer_end {
            ranges.push((start, outer_end.to_vec()));
        }
        ranges
    }

    pub fn rewrite_range_prefix(&mut self, prefix: &[u8]) {
        assert_eq!(
            prefix.len(),
            self.keyspace_prefix_len,
            "unexpected prefix.len()"
        );

        // If the prefixes is empty or the prefixes no need to update, return.
        if self.prefixes.is_empty() || self.prefixes.first().unwrap().starts_with(prefix) {
            return;
        }
        let offset = self.keyspace_prefix_len;
        self.prefixes.iter_mut().for_each(|p| {
            p[0..offset].copy_from_slice(prefix);
        });
        self.prefixes_nexts.iter_mut().for_each(|p| {
            p[0..offset].copy_from_slice(prefix);
        });
    }
}

pub fn merge_del_prefixes_if_needed(
    first: Option<Bytes>,
    second: Option<Bytes>,
    keyspace_id: u32,
) -> Option<Vec<u8>> {
    if first.is_none() && second.is_none() {
        return None;
    }

    let mut first_prefixes = first
        .map(|b| DeletePrefixes::unmarshal(b.chunk(), keyspace_id))
        .unwrap_or_else(|| DeletePrefixes::new_with_keyspace_id(keyspace_id));
    let second_prefixes = second
        .map(|b| DeletePrefixes::unmarshal(b.chunk(), keyspace_id))
        .unwrap_or_else(|| DeletePrefixes::new_with_keyspace_id(keyspace_id));
    for prefix in &second_prefixes.prefixes {
        first_prefixes = first_prefixes.merge_prefix(prefix);
    }
    Some(first_prefixes.marshal())
}

#[derive(Clone, Default, PartialEq)]
pub struct ShardRange {
    pub outer_start: Bytes,
    pub outer_end: Bytes,
    pub keyspace_id: u32,
}

impl BoundedDataSet for ShardRange {
    fn data_bound(&self) -> DataBound<'_> {
        DataBound::new(self.inner_start(), self.inner_end(), false)
    }
}

impl ShardRange {
    pub fn new(outer_start: &[u8], outer_end: &[u8]) -> Self {
        let keyspace_id = ApiV2::get_u32_keyspace_id_by_key(outer_start).unwrap_or_default();
        Self {
            outer_start: Bytes::from(outer_start.to_vec()),
            outer_end: Bytes::from(outer_end.to_vec()),
            keyspace_id,
        }
    }

    pub(crate) fn from_snap(snap: &kvenginepb::Snapshot) -> Self {
        Self::new(snap.get_outer_start(), snap.get_outer_end())
    }

    pub fn inner_start(&self) -> InnerKey<'_> {
        InnerKey::from_outer_key(&self.outer_start)
    }

    pub fn inner_end(&self) -> InnerKey<'_> {
        InnerKey::from_outer_end_key(&self.outer_end)
    }

    pub(crate) fn keyspace_prefix(&self) -> &[u8] {
        &self.outer_start[..self.keyspace_prefix_len()]
    }

    pub(crate) fn keyspace_prefix_len(&self) -> usize {
        if self.keyspace_id > 0 {
            KEYSPACE_PREFIX_LEN
        } else {
            0
        }
    }

    pub fn to_outer_key(&self, inner_key: InnerKey<'_>) -> Vec<u8> {
        [self.keyspace_prefix(), inner_key.deref()].concat()
    }
}

impl fmt::Debug for ShardRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardRange")
            .field(
                "outer_start",
                &log_wrappers::hex_encode_upper(&self.outer_start),
            )
            .field(
                "outer_end",
                &log_wrappers::hex_encode_upper(&self.outer_end),
            )
            .field("keyspace_id", &self.keyspace_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_del_prefixes() {
        for keyspace_id in [0, 1] {
            let assert_prefix_invariant = |del_prefix: &DeletePrefixes| {
                assert_eq!(del_prefix.prefixes.len(), del_prefix.prefixes_nexts.len());
                assert_eq!(
                    del_prefix.prefixes.len(),
                    del_prefix.inner_delete_bounds().count()
                );
                assert!(del_prefix.inner_delete_bounds().all(|bound| {
                    let mut p_c = bound.lower_bound.to_vec();
                    tidb_query_common::util::convert_to_prefix_next(&mut p_c);
                    p_c == bound.upper_bound.deref()
                }));
            };

            let mut del_prefix = DeletePrefixes::new_with_keyspace_id(keyspace_id);
            assert_prefix_invariant(&del_prefix);
            assert!(del_prefix.is_empty());
            del_prefix = del_prefix.merge_prefix("0000101".as_bytes());
            assert!(!del_prefix.is_empty());
            assert_eq!(del_prefix.prefixes.len(), 1);
            assert_prefix_invariant(&del_prefix);
            for prefix in ["00001010", "0000101"] {
                let inner_prefix = InnerKey::from_outer_key(prefix.as_bytes());
                assert!(del_prefix.cover_prefix(inner_prefix));
            }
            for prefix in ["000010", "0000103"] {
                let inner_prefix = InnerKey::from_outer_key(prefix.as_bytes());
                assert!(!del_prefix.cover_prefix(inner_prefix));
            }

            del_prefix = del_prefix.merge_prefix("0000105".as_bytes());
            assert_prefix_invariant(&del_prefix);
            let bin = del_prefix.marshal();
            del_prefix = DeletePrefixes::unmarshal(&bin, keyspace_id);
            assert_prefix_invariant(&del_prefix);
            assert_eq!(del_prefix.prefixes.len(), 2);
            for prefix in ["00001010", "0000101", "00001050", "0000105"] {
                let inner_prefix = InnerKey::from_outer_key(prefix.as_bytes());
                assert!(del_prefix.cover_prefix(inner_prefix));
            }
            for prefix in ["000010", "0000103"] {
                let inner_prefix = InnerKey::from_outer_key(prefix.as_bytes());
                assert!(!del_prefix.cover_prefix(inner_prefix));
            }

            del_prefix = del_prefix.merge_prefix("000010".as_bytes());
            assert_prefix_invariant(&del_prefix);
            assert_eq!(del_prefix.prefixes.len(), 1);
            for prefix in ["000010", "0000101", "0000103", "0000104"] {
                let inner_prefix = InnerKey::from_outer_key(prefix.as_bytes());
                assert!(del_prefix.cover_prefix(inner_prefix));
            }
            for prefix in ["00001", "000011"] {
                let inner_prefix = InnerKey::from_outer_key(prefix.as_bytes());
                assert!(!del_prefix.cover_prefix(inner_prefix));
            }

            del_prefix = DeletePrefixes::new_with_keyspace_id(keyspace_id);
            del_prefix = del_prefix.merge_prefix("0000101".as_bytes());
            del_prefix = del_prefix.merge_prefix("0000102".as_bytes());
            assert_prefix_invariant(&del_prefix);
            for (start, end) in [("0000101", "00001011"), ("0000102", "00001022")] {
                let inner_start = InnerKey::from_outer_key(start.as_bytes());
                let inner_end = InnerKey::from_outer_key(end.as_bytes());
                assert!(del_prefix.cover_range(inner_start, inner_end));
            }
            for (start, end) in [
                ("000099", "0000100"),
                ("0000101", "0000102"),
                ("0000102", "0000103"),
            ] {
                let inner_start = InnerKey::from_outer_key(start.as_bytes());
                let inner_end = InnerKey::from_outer_key(end.as_bytes());
                assert!(!del_prefix.cover_range(inner_start, inner_end));
            }

            del_prefix = DeletePrefixes::new_with_keyspace_id(keyspace_id);
            del_prefix = del_prefix.merge_prefix("0000101".as_bytes());
            del_prefix = del_prefix.merge_prefix("00001033".as_bytes());
            del_prefix = del_prefix.merge_prefix("00001055".as_bytes());
            del_prefix = del_prefix.merge_prefix("0000107".as_bytes());
            assert_prefix_invariant(&del_prefix);

            let split_del_range =
                del_prefix.build_split("00001033".as_bytes(), "00001055".as_bytes());
            assert_prefix_invariant(&split_del_range);
            assert_eq!(split_del_range.prefixes.len(), 1);
            assert_eq!(&split_del_range.prefixes[0], "00001033".as_bytes());

            let split_del_range =
                del_prefix.build_split("00001034".as_bytes(), "00001055".as_bytes());
            assert_prefix_invariant(&split_del_range);
            assert_eq!(split_del_range.prefixes.len(), 0);

            let split_del_range =
                del_prefix.build_split("000010334".as_bytes(), "000010555".as_bytes());
            assert_prefix_invariant(&split_del_range);
            assert_eq!(split_del_range.prefixes.len(), 2);
            assert_eq!(&split_del_range.prefixes[0], "00001033".as_bytes());
            assert_eq!(&split_del_range.prefixes[1], "00001055".as_bytes());

            del_prefix = DeletePrefixes::new_with_keyspace_id(keyspace_id)
                .merge_prefix("0000100".as_bytes())
                .merge_prefix("0000200".as_bytes())
                .merge_prefix("0000300".as_bytes());
            assert_prefix_invariant(&del_prefix);
            del_prefix = del_prefix.split(
                &DeletePrefixes::new_with_keyspace_id(keyspace_id)
                    .merge_prefix("0000100".as_bytes()),
            );
            assert_prefix_invariant(&del_prefix);
            assert_eq!(del_prefix.prefixes.len(), 2);
            assert!(!del_prefix.cover_prefix(InnerKey::from_outer_key("0000100".as_bytes(),)));
            assert!(del_prefix.cover_prefix(InnerKey::from_outer_key("0000200".as_bytes(),)));
            assert!(del_prefix.cover_prefix(InnerKey::from_outer_key("0000300".as_bytes(),)));
            del_prefix = del_prefix.split(
                &DeletePrefixes::new_with_keyspace_id(keyspace_id)
                    .merge_prefix("0000200".as_bytes())
                    .merge_prefix("0000300".as_bytes()),
            );
            assert_prefix_invariant(&del_prefix);
            assert_eq!(del_prefix.prefixes.len(), 0);
            assert!(!del_prefix.cover_prefix(InnerKey::from_outer_key("0000100".as_bytes(),)));
            assert!(!del_prefix.cover_prefix(InnerKey::from_outer_key("0000200".as_bytes(),)));
            assert!(!del_prefix.cover_prefix(InnerKey::from_outer_key("0000300".as_bytes(),)));
        }
    }

    #[test]
    fn test_del_prefixes_merge_prefix() {
        for keyspace_id in [0, 1] {
            let mut first = DeletePrefixes::new_with_keyspace_id(keyspace_id);
            first = first.merge_prefix("0000101".as_bytes());
            first = first.merge_prefix("00001033".as_bytes());
            first = first.merge_prefix("00001055".as_bytes());
            first = first.merge_prefix("0000107".as_bytes());

            let mut second = DeletePrefixes::new_with_keyspace_id(keyspace_id);
            second = second.merge_prefix("0000101".as_bytes());
            second = second.merge_prefix("00001022".as_bytes());
            second = second.merge_prefix("00001044".as_bytes());
            second = second.merge_prefix("00001066".as_bytes());
            second = second.merge_prefix("0000107".as_bytes());

            let merged = merge_del_prefixes_if_needed(
                Some(Bytes::from(first.marshal())),
                Some(Bytes::from(second.marshal())),
                keyspace_id,
            )
            .unwrap();
            let merged_del_prefixes = DeletePrefixes::unmarshal(&merged, keyspace_id);
            assert_eq!(
                merged_del_prefixes.prefixes,
                vec![
                    "0000101".as_bytes(),
                    "00001022".as_bytes(),
                    "00001033".as_bytes(),
                    "00001044".as_bytes(),
                    "00001055".as_bytes(),
                    "00001066".as_bytes(),
                    "0000107".as_bytes()
                ]
            );

            let rev_merged = merge_del_prefixes_if_needed(
                Some(Bytes::from(second.marshal())),
                Some(Bytes::from(first.marshal())),
                keyspace_id,
            );
            let rev_merged_del_prefixes =
                DeletePrefixes::unmarshal(&rev_merged.unwrap(), keyspace_id);
            assert_eq!(
                merged_del_prefixes.prefixes,
                rev_merged_del_prefixes.prefixes,
            );
        }
    }

    #[test]
    fn test_del_prefixes_merge_prefixes() {
        let cases = vec![
            (
                vec![], // sorted_prefixes
                vec![], // expected prefixes
            ),
            (vec!["00005000"], vec!["00005000"]),
            (vec![], vec!["00005000"]),
            (
                vec!["00005000", "00005001", "00006"],
                vec!["00005000", "00005001", "00006"],
            ),
            (
                vec!["00004000", "0000500", "00007"],
                vec!["00004000", "0000500", "00006", "00007"],
            ),
            (vec!["0000", "0001"], vec!["0000", "0001"]),
            (vec!["00001"], vec!["0000", "0001"]),
        ];

        let mut del_prefix = DeletePrefixes::new_with_keyspace_id(1);
        for (idx, (sorted_prefixes, expected_prefixes)) in cases.into_iter().enumerate() {
            del_prefix.schedule_at = idx as u64;
            del_prefix = del_prefix.merge_prefixes(sorted_prefixes.iter().map(|p| p.as_bytes()));
            let expected_prefixes = expected_prefixes
                .iter()
                .map(|p| p.as_bytes().to_vec())
                .collect::<Vec<_>>();
            assert_eq!(del_prefix.prefixes, expected_prefixes, "case {}", idx);
            assert_eq!(del_prefix.schedule_at, idx as u64);
            assert_eq!(del_prefix.keyspace_prefix_len, 4);
        }
    }

    #[test]
    fn test_del_prefixes_complementary_ranges() {
        let mut del_prefix = DeletePrefixes::new_with_keyspace_id(1);
        del_prefix = del_prefix.merge_prefix("0000-1".as_bytes());
        del_prefix = del_prefix.merge_prefix("0000-2".as_bytes());
        del_prefix = del_prefix.merge_prefix("0000-33".as_bytes());
        del_prefix = del_prefix.merge_prefix("0000-5".as_bytes());

        let cases: Vec<(
            (&str, &str),      // outer_range
            Vec<(&str, &str)>, // expected
        )> = vec![
            (("0000-0", "0000-1"), vec![("0000-0", "0000-1")]),
            (("0000-0", "0000-2"), vec![("0000-0", "0000-1")]),
            (("0000-0", "0000-3"), vec![("0000-0", "0000-1")]),
            (
                ("0000-0", "0000-33"),
                vec![("0000-0", "0000-1"), ("0000-3", "0000-33")],
            ),
            (
                ("0000-0", "0000-333"),
                vec![("0000-0", "0000-1"), ("0000-3", "0000-33")],
            ),
            (
                ("0000-0", "0000-4"),
                vec![
                    ("0000-0", "0000-1"),
                    ("0000-3", "0000-33"),
                    ("0000-34", "0000-4"),
                ],
            ),
            (
                ("0000-0", "0000-9"),
                vec![
                    ("0000-0", "0000-1"),
                    ("0000-3", "0000-33"),
                    ("0000-34", "0000-5"),
                    ("0000-6", "0000-9"),
                ],
            ),
        ];

        for (i, (outer_range, expected)) in cases.into_iter().enumerate() {
            let ranges = del_prefix
                .get_complementary_ranges(outer_range.0.as_bytes(), outer_range.1.as_bytes());
            let expected = expected
                .into_iter()
                .map(|(start, end)| (start.as_bytes().to_vec(), end.as_bytes().to_vec()))
                .collect::<Vec<_>>();
            assert_eq!(ranges, expected, "case {}: {:?}", i, outer_range);
        }
    }

    #[test]
    fn test_range_keyspace_id() {
        let mut range = ShardRange::new(&[b'x', 0, 0, 1], &[b'x', 0, 0, 2]);
        assert_eq!(range.keyspace_id, 1);
        range = ShardRange::new(&[], GLOBAL_SHARD_END_KEY);
        assert_eq!(range.keyspace_id, 0);
        range = ShardRange::new(&[b't', 0, 0, 0], &[]);
        assert_eq!(range.keyspace_id, 0);
    }

    #[test]
    fn test_rewrite_range_prefix() {
        let mut del_prefix = DeletePrefixes::new_with_keyspace_id(1);
        del_prefix = del_prefix.merge_prefix("0000-01".as_bytes());
        del_prefix = del_prefix.merge_prefix("0000-10".as_bytes());
        del_prefix = del_prefix.merge_prefix("0000-21".as_bytes());
        assert!(del_prefix.prefixes.contains(&"0000-01".as_bytes().to_vec()));
        assert!(del_prefix.prefixes.contains(&"0000-10".as_bytes().to_vec()));
        assert!(del_prefix.prefixes.contains(&"0000-21".as_bytes().to_vec()));

        del_prefix.rewrite_range_prefix("1111".as_bytes());
        assert!(del_prefix.prefixes.contains(&"1111-01".as_bytes().to_vec()));
        assert!(del_prefix.prefixes.contains(&"1111-10".as_bytes().to_vec()));
        assert!(del_prefix.prefixes.contains(&"1111-21".as_bytes().to_vec()));
    }

    #[test]
    fn test_properties() {
        let cases: Vec<(
            &'static str, // property_key
            bool,         // need_initial_flush
            bool,         // need_flush
        )> = vec![(TERM_KEY, false, true), (TXN_FILE_REF, true, true)];

        for (property_key, need_initial_flush, need_flush) in cases {
            assert_eq!(
                is_property_need_initial_flush(property_key),
                need_initial_flush
            );
            assert_eq!(is_property_need_flush(property_key), need_flush);
        }
    }
}
