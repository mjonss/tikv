// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Formatter},
    iter::Iterator,
    ops::Deref,
    sync::Arc,
};

use bytes::Buf;
use cloud_encryption::EncryptionKey;
use kvenginepb as pb;
use schema::schema::StorageClassSpec;

use crate::{
    context::PrepareType,
    dfs::FileType,
    meta::is_move_down,
    table::{
        BoundedDataSet, SnapVersion, TxnFile,
        blobtable::blobtable::BlobTable,
        columnar::{ColumnarFile, ColumnarFileCache, ColumnarLevels, ColumnarMetaCache},
        file::File,
        schema_file::SchemaFile,
        sstable::{BlockCache, L0Table, NewSsTableCtx, SsTable},
        tiny_meta::{MetaPackScheduler, SstTinyMeta, TypedTinyMeta},
        vector_index::{VectorIndexCache, VectorIndexFile, VectorIndexes},
    },
    table_id::get_table_id_from_ingest_files,
    *,
};

pub struct ChangeSet {
    pub change_set: kvenginepb::ChangeSet,
    pub l0_tables: HashMap<u64, L0Table>,
    pub ln_tables: HashMap<u64, SsTable>,
    pub blob_tables: HashMap<u64, BlobTable>,
    /// Tables that are not loaded from DFS.
    pub unloaded_tables: HashMap<u64, FileMeta>,
    pub lock_txn_files: Vec<TxnFile>,
    pub schema_file: Option<SchemaFile>,
    pub col_files: HashMap<u64, ColumnarFile>,
    pub vec_index_files: HashMap<u64, VectorIndexFile>,
}

impl Deref for ChangeSet {
    type Target = kvenginepb::ChangeSet;

    fn deref(&self) -> &Self::Target {
        &self.change_set
    }
}

impl Debug for ChangeSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("ChangeSet");
        de.field("change_set", &self.change_set);
        if !self.l0_tables.is_empty() {
            de.field("l0_tables", &self.l0_tables.keys());
        }
        if !self.ln_tables.is_empty() {
            de.field("ln_tables", &self.ln_tables.keys());
        }
        if !self.blob_tables.is_empty() {
            de.field("blob_tables", &self.blob_tables.keys());
        }
        if !self.unloaded_tables.is_empty() {
            de.field("unloaded_tables", &self.unloaded_tables.keys());
        }
        if !self.lock_txn_files.is_empty() {
            de.field(
                "lock_txn_files",
                &self.lock_txn_files.iter().map(|x| x.id()),
            );
        }
        if let Some(schema_file) = &self.schema_file {
            de.field("schema_file", &schema_file.get_file_id());
        }
        if !self.col_files.is_empty() {
            de.field("col_files", &self.col_files.keys());
        }
        if !self.vec_index_files.is_empty() {
            de.field("vec_index_files", &self.vec_index_files.keys());
        }
        de.finish()
    }
}

impl ChangeSet {
    pub fn new(change_set: kvenginepb::ChangeSet) -> Self {
        Self {
            change_set,
            l0_tables: HashMap::new(),
            ln_tables: HashMap::new(),
            blob_tables: HashMap::new(),
            unloaded_tables: HashMap::new(),
            lock_txn_files: vec![],
            schema_file: None,
            col_files: HashMap::new(),
            vec_index_files: HashMap::new(),
        }
    }

    pub fn add_file(
        &mut self,
        id: u64,
        file: Arc<dyn File>,
        meta: &FileMeta,
        cache: BlockCache,
        vector_index_cache: Option<VectorIndexCache>, // For vector index file.
        columnar_file_cache: Option<ColumnarFileCache>,
        encryption_key: Option<EncryptionKey>,
        columnar_meta_cache: ColumnarMetaCache,
        tiny_meta: TypedTinyMeta,
        meta_pack_scheduler: Option<&MetaPackScheduler>,
    ) -> Result<()> {
        match meta.file_type {
            FileType::Sst => {
                if meta.level == 0 {
                    let l0_table = L0Table::new(file, cache, false, encryption_key)?;
                    self.l0_tables.insert(id, l0_table.unwrap());
                } else {
                    let mut ctx = NewSsTableCtx {
                        tiny_meta: tiny_meta.into_sst(),
                        ..Default::default()
                    };
                    let ln_table = SsTable::new_with_ctx(file, cache, encryption_key, &mut ctx)?;

                    try_pack_sstable_meta(&ln_table, meta_pack_scheduler, ctx);
                    self.ln_tables.insert(id, ln_table);
                }
            }
            FileType::Blob => {
                let blob_table = BlobTable::new(file)?;
                self.blob_tables.insert(id, blob_table);
            }
            FileType::Columnar => {
                self.col_files.insert(
                    id,
                    ColumnarFile::open(file, columnar_file_cache, columnar_meta_cache.clone())?,
                );
            }
            FileType::Schema => {
                self.schema_file = Some(SchemaFile::open(file)?);
            }
            FileType::VectorIndex => {
                let file = VectorIndexFile::new(file, meta.table_meta_off, vector_index_cache)?;
                self.vec_index_files.insert(id, file);
            }
            file_type => unreachable!("unexpected file type {:?}", file_type),
        }
        Ok(())
    }

    pub fn set_schema_file(&mut self, schema_file: Option<SchemaFile>) {
        self.schema_file = schema_file
    }

    #[inline]
    pub fn get_schema_file(&self) -> Option<SchemaFile> {
        self.schema_file.clone()
    }

    pub fn get_schema_file_id(&self) -> Option<u64> {
        self.schema_file.as_ref().map(|x| x.get_file_id())
    }

    pub fn get_schema_version(&self) -> i64 {
        if let Some(schema_file) = &self.schema_file {
            schema_file.get_version()
        } else if self.has_update_schema_meta() {
            self.get_update_schema_meta().get_version()
        } else if let Some(snap) = kvenginepb::get_any_snap_from_changeset(&self.change_set) {
            if snap.has_schema_meta() {
                snap.get_schema_meta().get_version()
            } else {
                0
            }
        } else {
            debug_assert!(false, "changeset has no schema: {:?}", self);
            0
        }
    }

    pub fn get_restore_version(&self) -> u64 {
        if let Some(schema_file) = &self.schema_file {
            schema_file.get_restore_version()
        } else if self.has_update_schema_meta() {
            self.get_update_schema_meta().get_restore_version()
        } else if let Some(snap) = kvenginepb::get_any_snap_from_changeset(&self.change_set) {
            if snap.has_schema_meta() {
                snap.get_schema_meta().get_restore_version()
            } else {
                0
            }
        } else {
            debug_assert!(false, "changeset has no schema: {:?}", self);
            0
        }
    }

    pub fn into_inner(self) -> kvenginepb::ChangeSet {
        self.change_set
    }
}

// `not_all_tables_loaded` means that some tables in `snap` are not loaded to
// `tables`. Should only happen in "ignore lock" or restoration (`for_restore`).
// Note: keep consistency with `estimate_tables_size_from_snapshot`.
pub(crate) fn create_snapshot_tables(
    builder: &mut ShardDataBuilder,
    snap: &kvenginepb::Snapshot,
    tables: &ChangeSet,
    not_all_tables_loaded: bool,
    prepare_type: PrepareType,
) {
    let prepare_sst = matches!(prepare_type, PrepareType::SstOnly | PrepareType::All);
    let prepare_columnar = matches!(prepare_type, PrepareType::ColumnarOnly | PrepareType::All);
    // Note: Some tables in `snap` will not exist in `tables` if it's not necessary
    // to load from DFS.
    // Should only happen in restoration.
    let blob_creates = snap.get_blob_creates();
    let mut blob_tbl_map = HashMap::new();
    let mut scf_builders = vec![];
    let mut l0_tbls = vec![];
    let mut scfs = [ShardCf::new(0), ShardCf::new(1), ShardCf::new(2)];

    let unconverted_l0s: HashSet<u64> = snap.get_unconverted_l0s().iter().copied().collect();
    for l0_create in snap.get_l0_creates() {
        // If type is prepare columnar only, unconverted l0s should also be prepared.
        if prepare_columnar && !prepare_sst && !unconverted_l0s.contains(&l0_create.id) {
            continue;
        }
        if let Some(l0_tbl) = tables.l0_tables.get(&l0_create.id) {
            l0_tbls.push(l0_tbl.clone());
        } else {
            assert!(
                not_all_tables_loaded,
                "l0_create: {:?}, tables: {:?}",
                l0_create, tables,
            );
        }
    }
    l0_tbls.sort_by(|a, b| b.snap_version().cmp(&a.snap_version()));

    if prepare_sst {
        for blob_create in blob_creates {
            if let Some(blob_tbl) = tables.blob_tables.get(&blob_create.id) {
                blob_tbl_map.insert(blob_create.id, blob_tbl.clone());
            } else {
                assert!(
                    not_all_tables_loaded,
                    "blob_create: {:?}, tables: {:?}",
                    blob_create, tables,
                );
            }
        }

        for cf in 0..NUM_CFS {
            let scf = ShardCfBuilder::new(cf);
            scf_builders.push(scf);
        }
        for table_create in snap.get_table_creates() {
            if let Some(tbl) = tables.ln_tables.get(&table_create.id) {
                let scf = &mut scf_builders.as_mut_slice()[table_create.cf as usize];
                scf.add_table(tbl.clone(), table_create.level as usize);
            } else {
                assert!(
                    not_all_tables_loaded,
                    "table_create: {:?}, tables: {:?}",
                    table_create, tables,
                );
            }
        }
        for cf in 0..NUM_CFS {
            let scf = &mut scf_builders.as_mut_slice()[cf];
            scfs[cf] = scf.build();
        }
    }
    let mut col_levels = ColumnarLevels::new();
    let mut vector_indexes = VectorIndexes::default();
    if prepare_columnar {
        for col_create in snap.get_columnar_creates() {
            if let Some(col_file) = tables.col_files.get(&col_create.id) {
                col_levels.add_file(col_create.level as usize, col_file.clone());
            } else {
                assert!(
                    not_all_tables_loaded,
                    "columnar_create: {:?}, tables: {:?}",
                    col_create, tables,
                );
            }
        }
        for &l0_id in snap.get_unconverted_l0s() {
            if let Some(unconverted_l0) = l0_tbls.iter().find(|l0| l0.id() == l0_id) {
                col_levels.unconverted_l0s.push(unconverted_l0.clone());
            } else {
                assert!(
                    not_all_tables_loaded,
                    "unconverted_l0: {:?}, tables: {:?}",
                    l0_id, tables
                );
            }
        }
        col_levels.l2_snap_version = snap.columnar_l2_snap_version.into();
        col_levels.sort();
        for vec_idx_pb in snap.get_vector_indexes() {
            for vec_idx_file_pb in vec_idx_pb.get_files() {
                let vec_idx_file = tables
                    .vec_index_files
                    .get(&vec_idx_file_pb.get_id())
                    .unwrap()
                    .clone();
                vector_indexes.add_index_file(vec_idx_file);
            }
            vector_indexes.update_snap_version(
                vec_idx_pb.get_table_id(),
                vec_idx_pb.get_index_id(),
                vec_idx_pb.get_col_id(),
                vec_idx_pb.get_snap_version().into(),
            );
        }
        vector_indexes.sort();
        builder.set_columnar_table_ids(snap.get_columnar_table_ids().to_vec());
    }
    builder.set_l0_tbls(l0_tbls);
    builder.set_blob_tbls(blob_tbl_map);
    builder.set_cfs(scfs);
    builder.set_lock_txn_files(tables.lock_txn_files.clone());
    builder.set_columnar_levels(col_levels);
    builder.set_vector_indexes(vector_indexes);
    builder.set_persisted_version(SnapVersion::new(snap.base_version, snap.data_sequence));
}

// Note: keep consistency with `create_snapshot_tables`.
pub(crate) fn estimate_tables_size_from_snapshot(
    snap: &kvenginepb::Snapshot,
    prepare_type: PrepareType,
) -> usize {
    let mut tables_size: usize = 0;

    let prepare_sst = matches!(prepare_type, PrepareType::SstOnly | PrepareType::All);
    let prepare_columnar = matches!(prepare_type, PrepareType::ColumnarOnly | PrepareType::All);

    let unconverted_l0s: HashSet<u64> = snap.get_unconverted_l0s().iter().copied().collect();
    for l0_create in snap.get_l0_creates() {
        if prepare_columnar && !prepare_sst && !unconverted_l0s.contains(&l0_create.id) {
            continue;
        }
        tables_size += l0_create.size as usize;
    }
    if prepare_sst {
        // TODO: handle blob.
        tables_size += snap
            .get_table_creates()
            .iter()
            .map(|t| t.meta_offset as usize)
            .sum::<usize>();
    }
    if prepare_columnar {
        tables_size += snap
            .get_columnar_creates()
            .iter()
            .map(|t| t.meta_offset as usize)
            .sum::<usize>();
        tables_size += snap
            .get_vector_indexes()
            .iter()
            .flat_map(|vec_idx| vec_idx.get_files().iter().map(|f| f.meta_offset as usize))
            .sum::<usize>();
    }
    tables_size
}

impl EngineCore {
    pub fn apply_change_set(&self, cs: &ChangeSet) -> Result<()> {
        let shard = self.get_shard(cs.shard_id);
        if shard.is_none() {
            return Err(Error::ShardNotFound);
        }
        let shard = shard.unwrap();
        info!(
            "{} kvengine apply change set sequence: {}",
            shard.tag(),
            cs.sequence
        );
        if shard.ver != cs.shard_ver {
            warn!(
                "{} kvengine::apply_change_set: shard not match, shard {}, cs {}, {:?}",
                shard.tag(),
                shard.ver,
                cs.shard_ver,
                cs
            );
            return Err(Error::ShardNotMatch);
        }
        let seq = load_u64(&shard.meta_seq);
        if seq >= cs.sequence {
            warn!(
                "{} skip duplicated shard seq:{}, change seq:{}",
                shard.tag(),
                seq,
                cs.sequence
            );
            return Ok(());
        }
        if cs.has_flush() {
            self.apply_flush(&shard, cs);
        } else if cs.has_compaction()
            || cs.has_destroy_range()
            || cs.has_truncate_ts()
            || cs.has_trim_over_bound()
            || cs.has_major_compaction()
            || cs.has_columnar_compaction()
            || cs.has_update_vector_index()
        {
            if cs.has_compaction() {
                self.apply_compaction(&shard, cs);
            } else if cs.has_destroy_range() {
                self.apply_destroy_range(&shard, cs);
            } else if cs.has_truncate_ts() {
                self.apply_truncate_ts(&shard, cs);
            } else if cs.has_trim_over_bound() {
                self.apply_trim_over_bound(&shard, cs);
            } else if cs.has_major_compaction() {
                self.apply_major_compaction(&shard, cs);
            } else if cs.has_columnar_compaction() {
                self.apply_columnar_compaction(&shard, cs);
            } else if cs.has_update_vector_index() {
                self.apply_update_vector_index(&shard, cs);
            }
            store_bool(&shard.compacting, false);
            self.send_compact_msg(CompactMsg::Applied(IdVer::new(shard.id, shard.ver)));
        } else if cs.has_initial_flush() {
            self.apply_initial_flush(&shard, cs);
        } else if cs.has_ingest_files() {
            self.apply_ingest_files(&shard, cs)?;
        } else if cs.has_restore_shard() {
            self.apply_restore_shard(&shard, cs)?;
        } else if cs.has_update_schema_meta() {
            self.apply_update_schema_meta(&shard, cs);
        } else if cs.get_clear_columnar() || cs.has_clear_columnar_with_restore_version() {
            self.apply_clear_columnar(&shard, cs);
        } else if cs.get_property_key() == STORAGE_CLASS_KEY {
            self.apply_update_storage_class(&shard, cs);
        }
        debug!("{} finished applying change set: {:?}", shard.tag(), cs);

        // Get shard again as version may be changed after change set applied.
        // Note that the shard may have been destroyed (e.g. by merge, in rfstore
        // thread) at this point.
        if let Some(shard) = self.get_shard(cs.shard_id) {
            self.refresh_shard_states(&shard);
            // Since shard maybe replaced, we update the meta_seq here to avoid the
            // the case that the old shard's meta_sequence mismatch the old shard data.
            store_u64(&shard.meta_seq, cs.sequence);
        }

        Ok(())
    }

    fn apply_flush(&self, shard: &Shard, cs: &ChangeSet) {
        let flush = cs.get_flush();
        let old_data = shard.get_data();
        let mut new_mem_tbls = old_data.mem_tbls.clone();
        let flush_version = flush.get_version().into();
        if flush.has_l0_create() || !flush.get_l0_creates().is_empty() {
            let mut l0s = vec![];
            if flush.has_l0_create() {
                let l0_id = flush.get_l0_create().get_id();
                let l0_tbl = cs.l0_tables.get(&l0_id).unwrap().clone();
                l0s.push(l0_tbl);
            }
            for l0_create in flush.get_l0_creates() {
                let l0_tbl = cs.l0_tables.get(&l0_create.get_id()).unwrap().clone();
                l0s.push(l0_tbl);
            }
            let l0_snap_version = l0s.first().unwrap().snap_version();
            let last = new_mem_tbls.pop().unwrap();
            let last_version = last.get_snap_version();
            if last_version != l0_snap_version {
                #[cfg(feature = "debug-trace-mem-table")]
                {
                    debug::dump_mem_table_actions(shard.id);
                    debug::dump_raft_logs(shard.id);
                }

                panic!(
                    "{} mem table last version {}, size {} not match L0 version {}, shard meta seq {}, flush seq {}",
                    shard.tag(),
                    last_version,
                    last.size(),
                    l0_snap_version,
                    shard.get_meta_sequence(),
                    cs.sequence,
                );
            }
            let mut new_l0_tbls = Vec::with_capacity(old_data.l0_tbls.len() + l0s.len());
            new_l0_tbls.extend_from_slice(l0s.as_slice());
            new_l0_tbls.extend_from_slice(old_data.l0_tbls.as_slice());
            let mut col_levels = old_data.col_levels.clone();
            if old_data.schema_file.is_some()
                && !old_data.columnar_table_ids.is_empty()
                && !self.opts.ignore_columnar_table_load
            {
                col_levels.unconverted_l0s.extend_from_slice(l0s.as_slice());
            }
            let mut builder = ShardDataBuilder::new(old_data);
            builder.set_mem_tbls(new_mem_tbls);
            builder.set_l0_tbls(new_l0_tbls);
            builder.set_columnar_levels(col_levels);
            builder.set_persisted_version(flush_version);
            shard.set_data(builder.build());
            self.send_free_mem_msg(FreeMemMsg::FreeMem(last));
        } else {
            // If there is no L0Create, it means the mem-table is empty during flush.
            // It's possible that the mem-table is not empty but doesn't have any data.
            // So we need to use has_data_in_range to check.
            let last = new_mem_tbls.last().unwrap();
            if new_mem_tbls.len() > 1 && !last.has_data_in_bound(shard.data_bound()) {
                let last = new_mem_tbls.pop().unwrap();
                let mut builder = ShardDataBuilder::new(old_data);
                builder.set_mem_tbls(new_mem_tbls);
                builder.set_persisted_version(flush_version);
                shard.set_data(builder.build());
                self.send_free_mem_msg(FreeMemMsg::FreeMem(last));
            }
        }
        shard.clear_finished_txn_file_refs(flush.version.into());
    }

    fn apply_initial_flush(&self, shard: &Shard, cs: &ChangeSet) {
        let initial_flush = cs.get_initial_flush();
        let data = shard.get_data();
        let mut mem_tbls = data.mem_tbls.clone();

        let mut builder = ShardDataBuilder::new(data.clone());
        let prepare_type = if self.opts.ignore_columnar_table_load {
            PrepareType::SstOnly
        } else {
            PrepareType::All
        };
        create_snapshot_tables(
            &mut builder,
            initial_flush,
            cs,
            self.opts.for_restore,
            prepare_type,
        );
        // `lock_txn_files` is ignored because it does not depend on initial flush to
        // keep consistency between peers, as only target region has txn file locks.
        builder.set_lock_txn_files(data.lock_txn_files.clone());
        let mut max_flushed_mem_tbl_version = SnapVersion::zero();
        let new_snap_version =
            SnapVersion::new(initial_flush.base_version, initial_flush.data_sequence);
        builder.set_persisted_version(new_snap_version);
        mem_tbls.retain(|x| {
            let mem_tbl_snap_version = x.get_snap_version();
            let flushed =
                mem_tbl_snap_version.is_not_zero() && mem_tbl_snap_version <= new_snap_version;
            if flushed {
                if max_flushed_mem_tbl_version < mem_tbl_snap_version {
                    max_flushed_mem_tbl_version = mem_tbl_snap_version;
                }
                self.send_free_mem_msg(FreeMemMsg::FreeMem(x.clone()));
            }
            !flushed
        });
        builder.set_mem_tbls(mem_tbls);
        // Set schema file and col_snap_version is no needed in normal case. We set it
        // here to ensure the schema file and col_snap_version consistency between
        // peers in some corner cases. e.g. the parent shard has inconsistency schema
        // file or col_snap_version.
        builder.set_schema(
            cs.get_schema_version(),
            cs.get_restore_version(),
            cs.get_schema_file(),
        );
        let new_data = builder.build();
        info!("{} apply_initial_flush", shard.tag(); "seq" => cs.sequence);
        shard.set_data(new_data);
        shard.clear_finished_txn_file_refs(max_flushed_mem_tbl_version);

        store_bool(&shard.initial_flushed, true);
        // Switched memtables can't be flushed until initial flush finished, so we
        // trigger it actively.
        if let Err(err) = self.trigger_flush(shard) {
            warn!("{} trigger_flush error: {:?}", shard.tag(), err);
        }
    }

    fn apply_compaction(&self, shard: &Shard, cs: &ChangeSet) {
        let comp = cs.get_compaction();
        let mut del_files = HashMap::new();
        if comp.conflicted {
            if is_move_down(comp) {
                return;
            }
            for create in comp.get_table_creates() {
                let cover = shard.contains_bound(create.data_bound());
                del_files.insert(create.id, cover);
            }
            self.remove_dfs_files(shard, del_files);
            return;
        }
        let data = shard.get_data();
        let mut new_cfs = data.cfs.clone();
        let mut new_l0s = data.l0_tbls.clone();
        let mut new_blob_tbl_map = data.blob_tbl_map.as_ref().clone();
        if comp.level == 0 {
            let is_move_down = is_move_down(comp);
            new_l0s.retain(|x| {
                let is_deleted = comp.get_top_deletes().contains(&x.id());
                if is_deleted && !is_move_down {
                    del_files.insert(x.id(), shard.contains_bound(x.data_bound()));
                }
                !is_deleted
            });
            new_blob_tbl_map.extend(cs.blob_tables.clone());
            for cf in 0..NUM_CFS {
                let new_l1 = self.new_level(shard, cs, &data, cf, &mut del_files, true);
                new_cfs[cf].set_level(new_l1);
            }
        } else {
            let cf = comp.cf as usize;
            let new_top_level = self.new_level(shard, cs, &data, cf, &mut del_files, false);
            new_cfs[cf].set_level(new_top_level);
            if CF_LEVELS[cf] > comp.level as usize {
                let new_bottom_level = self.new_level(shard, cs, &data, cf, &mut del_files, true);
                new_cfs[cf].set_level(new_bottom_level);
            }
            // For move down operation, the TableCreates may contains TopDeletes, we don't
            // want to delete them.
            for create in comp.get_table_creates() {
                del_files.remove(&create.id);
            }
        }
        let mut builder = ShardDataBuilder::new(data);
        builder.set_l0_tbls(new_l0s);
        builder.set_blob_tbls(new_blob_tbl_map);
        builder.set_cfs(new_cfs);
        shard.set_data(builder.build());
        self.remove_dfs_files(shard, del_files);
    }

    fn apply_major_compaction(&self, shard: &Shard, cs: &ChangeSet) {
        let comp = cs.get_major_compaction();
        let mut del_file_is_subrange = HashMap::new();
        if comp.conflicted {
            for sst_create in comp.get_sstable_change().get_table_creates() {
                let is_subrange = shard.contains_bound(sst_create.data_bound());
                del_file_is_subrange.insert(sst_create.get_id(), is_subrange);
            }
            for blob_tbl_create in comp.get_new_blob_tables() {
                let is_subrange = shard.contains_bound(blob_tbl_create.data_bound());
                del_file_is_subrange.insert(blob_tbl_create.get_id(), is_subrange);
            }
            self.remove_dfs_files(shard, del_file_is_subrange);
            return;
        }
        let mut to_be_deleted = HashSet::new();
        let data = shard.get_data();

        for del in comp.get_sstable_change().get_table_deletes() {
            to_be_deleted.insert(del.get_id());
        }
        for del in comp.get_old_blob_tables() {
            to_be_deleted.insert(*del);
        }
        let mut new_blob_tbl_map = data.blob_tbl_map.as_ref().clone();
        let mut new_l0s = data.l0_tbls.clone();
        let mut new_cfs = data.cfs.clone();
        new_l0s.retain(|x| {
            let del = to_be_deleted.contains(&x.id());
            if del {
                del_file_is_subrange.insert(x.id(), shard.contains_bound(x.data_bound()));
            }
            !del
        });
        new_blob_tbl_map.retain(|id, v| {
            let del = to_be_deleted.contains(id);
            if del {
                del_file_is_subrange.insert(*id, shard.contains_bound(v.data_bound()));
            }
            !del
        });

        for new_blob_table in comp.get_new_blob_tables() {
            new_blob_tbl_map.insert(
                new_blob_table.get_id(),
                cs.blob_tables
                    .get(&new_blob_table.get_id())
                    .unwrap()
                    .clone(),
            );
        }
        for cf in 0..NUM_CFS {
            for level in 1..=CF_LEVELS[cf] {
                let mut tables = new_cfs[cf].get_level(level).tables.as_ref().clone();
                tables.retain(|x| {
                    let del = to_be_deleted.contains(&x.id());
                    if del {
                        del_file_is_subrange.insert(x.id(), shard.contains_bound(x.data_bound()));
                    }
                    !del
                });
                if level == CF_LEVELS[cf] {
                    for new_sstable in comp.get_sstable_change().get_table_creates() {
                        if new_sstable.get_cf() as usize == cf {
                            assert_eq!(new_sstable.get_level() as usize, CF_LEVELS[cf]);
                            tables.push(cs.ln_tables.get(&new_sstable.get_id()).unwrap().clone());
                        }
                    }
                }
                tables.sort_by(|a, b| a.smallest().cmp(&b.smallest()));
                let lh = LevelHandler::new(level, tables);
                new_cfs[cf].set_level(lh);
            }
        }
        let mut builder = ShardDataBuilder::new(data);
        builder.set_l0_tbls(new_l0s);
        builder.set_blob_tbls(new_blob_tbl_map);
        builder.set_cfs(new_cfs);
        shard.set_data(builder.build());
        self.remove_dfs_files(shard, del_file_is_subrange);
        shard.set_property(MANUAL_MAJOR_COMPACTION, MANUAL_MAJOR_COMPACTION_DISABLE);
    }

    fn get_tables_from_table_change(
        &self,
        buidler: &mut ShardDataBuilder,
        data: &ShardData,
        cs: &ChangeSet,
        tc: &pb::TableChange,
        del_files: &mut HashMap<u64, bool>,
    ) {
        let mut new_l0s = data.l0_tbls.clone();
        let mut new_cfs = data.cfs.clone();
        // Group files by cf and level.
        let mut grouped = HashMap::new();
        for deleted in tc.get_table_deletes() {
            grouped
                .entry((deleted.get_cf(), deleted.get_level() as usize))
                .or_insert_with(|| (Vec::new(), Vec::new()))
                .0
                .push(deleted.get_id());
        }
        for created in tc.get_table_creates() {
            grouped
                .entry((created.get_cf(), created.get_level() as usize))
                .or_insert_with(|| (Vec::new(), Vec::new()))
                .1
                .push(created.get_id());
        }

        for ((cf, level), (deletes, creates)) in grouped {
            if level == 0 {
                new_l0s.retain(|l0| {
                    let is_deleted = deletes.contains(&l0.id());
                    if is_deleted {
                        del_files.insert(l0.id(), data.contains_bound(l0.data_bound()));
                    }
                    !is_deleted
                });
                new_l0s.extend(
                    creates
                        .clone()
                        .into_iter()
                        .map(|id| cs.l0_tables.get(&id).unwrap().clone()),
                );
                new_l0s.sort_by(|a, b| b.snap_version().cmp(&a.snap_version()));
            } else {
                let old_level = new_cfs[cf as usize].get_level(level);
                let mut new_level_tables = old_level.tables.as_ref().clone();
                new_level_tables.retain(|t| {
                    let is_deleted = deletes.contains(&t.id());
                    if is_deleted {
                        del_files.insert(t.id(), data.contains_bound(t.data_bound()));
                    }
                    !is_deleted
                });
                new_level_tables.extend(
                    creates
                        .into_iter()
                        .map(|id| cs.ln_tables.get(&id).unwrap().clone()),
                );
                new_level_tables.sort_by(|a, b| a.smallest().cmp(&b.smallest()));
                let new_level = LevelHandler::new(level, new_level_tables);
                new_cfs[cf as usize].set_level(new_level);
            }
        }
        buidler.set_l0_tbls(new_l0s);
        buidler.set_cfs(new_cfs);

        let mut new_col_levels = data.col_levels.clone();
        if !tc.get_file_ids_map().is_empty() {
            // Update unconverted_l0s
            let unconverted_l0_ids: Vec<u64> = new_col_levels
                .unconverted_l0s
                .iter()
                .map(|l0| l0.id())
                .collect();
            let mut iter = tc.get_file_ids_map().iter();
            while let (Some(delete_id), Some(create_id)) = (iter.next(), iter.next()) {
                // Only add the corresponding created file if the deleted file is in the
                // `unconverted_l0s`.
                if unconverted_l0_ids.contains(delete_id) {
                    new_col_levels.unconverted_l0s.push(
                        cs.l0_tables
                            .get(create_id)
                            .unwrap_or_else(|| {
                                panic!("create id {} not prepared, cs: {:?}", create_id, cs)
                            })
                            .clone(),
                    );
                }
            }
        }
        for deleted in tc.get_table_deletes() {
            new_col_levels
                .unconverted_l0s
                .retain(|l0| l0.id() != deleted.get_id());
        }
        let mut columnar_deletes = HashSet::new();
        for deleted in tc.get_columnar_deletes() {
            columnar_deletes.insert(deleted.get_id());
        }
        for col_level in &mut new_col_levels.levels {
            col_level
                .files
                .retain(|c| !columnar_deletes.contains(&c.get_file().id()));
        }
        for created in tc.get_columnar_creates() {
            let col_file = cs.col_files.get(&created.get_id()).unwrap().clone();
            new_col_levels.add_file(created.get_level() as usize, col_file);
        }
        new_col_levels.sort();
        buidler.set_columnar_levels(new_col_levels);
    }

    fn apply_destroy_range(&self, shard: &Shard, cs: &ChangeSet) {
        assert!(cs.has_destroy_range());
        let data = shard.get_data();
        let tc = cs.get_destroy_range();
        let mut del_files = HashMap::new();
        let mut builder = ShardDataBuilder::new(data.clone());
        self.get_tables_from_table_change(&mut builder, &data, cs, tc, &mut del_files);
        assert_eq!(cs.get_property_key(), DEL_PREFIXES_KEY);
        let done = DeletePrefixes::unmarshal(cs.get_property_value(), shard.keyspace_id);
        shard.set_data(builder.build());
        let del_prefixes = shard.get_del_prefixes();
        let new_del_prefixes = del_prefixes.split(&done);
        shard.set_property(DEL_PREFIXES_KEY, &new_del_prefixes.marshal());
        self.remove_dfs_files(shard, del_files);
    }

    fn apply_truncate_ts(&self, shard: &Shard, cs: &ChangeSet) {
        debug!("apply truncate ts in engine, shard {:?}", shard.tag());
        assert!(cs.has_truncate_ts());
        let data = shard.get_data();
        let tc = cs.get_truncate_ts();
        let mut del_files = HashMap::new();
        let mut builder = ShardDataBuilder::new(data.clone());
        self.get_tables_from_table_change(&mut builder, &data, cs, tc, &mut del_files);
        shard.set_data(builder.build());
        self.remove_dfs_files(shard, del_files);
    }

    fn apply_trim_over_bound(&self, shard: &Shard, cs: &ChangeSet) {
        debug!("{} apply changeset.trim_over_bound in engine", shard.tag());
        assert!(cs.has_trim_over_bound());
        let data = shard.get_data();
        let tc = cs.get_trim_over_bound();
        let mut del_files = HashMap::new();
        let mut builder = ShardDataBuilder::new(data.clone());
        self.get_tables_from_table_change(&mut builder, &data, cs, tc, &mut del_files);
        shard.set_data(builder.build());
        shard.set_property(TRIM_OVER_BOUND, TRIM_OVER_BOUND_DISABLE);
        self.remove_dfs_files(shard, del_files);
    }

    fn new_level(
        &self,
        shard: &Shard,
        cs: &ChangeSet,
        data: &ShardData,
        cf: usize,
        del_files: &mut HashMap<u64, bool>,
        is_bottom: bool,
    ) -> LevelHandler {
        let old_scf = data.get_cf(cf);
        let comp = cs.get_compaction();
        let level = if is_bottom {
            comp.get_level() as usize + 1
        } else {
            comp.get_level() as usize
        };
        let deletes = if is_bottom {
            comp.get_bottom_deletes()
        } else {
            comp.get_top_deletes()
        };
        let mut new_level_tables = old_scf.get_level(level).tables.as_ref().clone();
        new_level_tables.retain(|x| {
            let is_deleted = deletes.contains(&x.id());
            if is_deleted {
                del_files.insert(x.id(), shard.contains_bound(x.data_bound()));
            }
            !is_deleted
        });
        if is_bottom {
            for new_tbl_create in comp.get_table_creates() {
                if new_tbl_create.cf as usize == cf {
                    let new_tbl = if is_move_down(comp) {
                        if comp.level == 0 {
                            if let Some(tbl) = cs.ln_tables.get(&new_tbl_create.get_id()) {
                                // reload sst for ia
                                tbl.clone()
                            } else {
                                let old_l0 = data
                                    .l0_tbls
                                    .iter()
                                    .find(|l0| l0.id() == new_tbl_create.id)
                                    .unwrap();
                                old_l0.get_cf(WRITE_CF).clone().unwrap()
                            }
                        } else {
                            let old_top_level = old_scf.get_level(level - 1);
                            old_top_level.get_table_by_id(new_tbl_create.id).unwrap()
                        }
                    } else {
                        cs.ln_tables.get(&new_tbl_create.get_id()).unwrap().clone()
                    };
                    new_level_tables.push(new_tbl);
                }
            }
            new_level_tables.sort_by(|a, b| a.smallest().cmp(&b.smallest()));
        }
        let new_level = LevelHandler::new(level, new_level_tables);
        new_level.check_order(cf, shard.tag());
        new_level
    }

    fn remove_dfs_files(&self, shard: &Shard, del_files: HashMap<u64, bool>) {
        if !shard.is_active() {
            return;
        }
        let shard_use_ia = shard.get_storage_class_spec().can_be_ia();
        for (id, _cover) in del_files {
            self.set_local_file_mtime(id, shard_use_ia);
        }
    }

    // On remove local file, we need to retain the file for a while as SnapAccess
    // hold the file may reopen it, update the mtime so local file gc worker
    // will delay the remove.
    fn set_local_file_mtime(&self, file_id: u64, shard_use_ia: bool) {
        let path = self.local_sst_file_path(file_id);
        if !path.exists() && shard_use_ia {
            // Ia file may not exists.
            return;
        }
        if let Err(err) = filetime::set_file_mtime(path, filetime::FileTime::now()) {
            error!("failed to set local file mtime {} {:?}", file_id, err);
        }
    }

    fn _local_file_len(&self, file_id: u64) -> Option<u64> {
        let local_file_path = self.local_sst_file_path(file_id);
        match std::fs::metadata(local_file_path) {
            Ok(metadata) => Some(metadata.len()),
            Err(err) => {
                error!("failed to get local file len {:?}", err);
                None
            }
        }
    }

    fn apply_ingest_files(&self, shard: &Shard, cs: &ChangeSet) -> Result<()> {
        let ingest_files = cs.get_ingest_files();
        if let (Some(ingest_id), Some(old_ingest_id)) = (
            get_shard_property(INGEST_ID_KEY, ingest_files.get_properties()),
            shard.get_property(INGEST_ID_KEY),
        ) {
            if old_ingest_id.chunk() == ingest_id.as_slice() {
                // skip duplicated ingest files.
                return Ok(());
            }
        }
        let old_data = shard.get_data();
        let mut new_blob_tbl_map = old_data.blob_tbl_map.as_ref().clone();
        for blob_table_create in ingest_files.get_blob_creates() {
            let id = blob_table_create.get_id();
            new_blob_tbl_map.insert(id, cs.blob_tables.get(&id).unwrap().clone());
        }
        let mut new_l0s = old_data.l0_tbls.clone();
        for l0_create in ingest_files.get_l0_creates() {
            let l0_table = cs.l0_tables.get(&l0_create.get_id()).unwrap().clone();
            new_l0s.push(l0_table);
        }
        new_l0s.sort_unstable_by(|a, b| b.snap_version().cmp(&a.snap_version()));
        let mut scf_builder = ShardCfBuilder::new(0);
        for level in &old_data.cfs[0].levels {
            for old_tbl in level.tables.as_ref() {
                scf_builder.add_table(old_tbl.clone(), level.level);
            }
        }
        for tbl_create in ingest_files.get_table_creates() {
            let table = cs.ln_tables.get(&tbl_create.get_id()).unwrap().clone();
            scf_builder.add_table(table, tbl_create.level as usize);
        }
        let new_cf = scf_builder.build();
        let mut new_cfs = old_data.cfs.clone();
        let mut columnar_table_ids = old_data.columnar_table_ids.clone();
        if let Some(table_id) = get_table_id_from_ingest_files(ingest_files) {
            columnar_table_ids.retain(|id| *id != table_id);
        }
        new_cfs[0] = new_cf;
        let mut builder = ShardDataBuilder::new(old_data);
        builder.set_l0_tbls(new_l0s);
        builder.set_blob_tbls(new_blob_tbl_map);
        builder.set_cfs(new_cfs);
        builder.set_columnar_table_ids(columnar_table_ids);
        shard.set_data(builder.build());
        Ok(())
    }

    fn apply_restore_shard(&self, old_shard: &Shard, cs: &ChangeSet) -> Result<()> {
        debug_assert!(cs.has_restore_shard());
        debug_assert!(cs.get_restore_version() > 0);
        // TODO: skip duplicated.

        let snap = cs.get_restore_shard();
        assert_eq!(old_shard.outer_start, snap.outer_start);
        assert_eq!(old_shard.outer_end, snap.outer_end);

        self.send_flush_msg(FlushMsg::Clear(old_shard.id));
        self.send_compact_msg(CompactMsg::Clear(IdVer::new(old_shard.id, old_shard.ver)));

        let range = ShardRange::from_snap(snap);
        // Increase shard version to make change sets generated before restore shard
        // stale.
        let new_shard = Shard::new(
            self.get_engine_id(),
            snap.get_properties(),
            cs.shard_ver + 1,
            range,
            snap.inner_key_off as usize,
            old_shard.opt.clone(),
            &self.master_key,
        );
        let snap_data = new_shard.get_data();
        let mut builder = ShardDataBuilder::new(snap_data);
        let prepare_type = if self.opts.ignore_columnar_table_load {
            PrepareType::SstOnly
        } else {
            PrepareType::All
        };
        create_snapshot_tables(&mut builder, snap, cs, self.opts.for_restore, prepare_type);
        builder.set_schema(
            cs.get_schema_version(),
            cs.get_restore_version(),
            cs.get_schema_file(),
        );
        builder.set_persisted_version(SnapVersion::new(snap.base_version, cs.sequence));
        let new_data = builder.build();
        let new_inner_key_off = new_data.inner_key_off;
        new_shard.set_data(new_data);
        new_shard.set_active(old_shard.is_active());

        store_u64(&new_shard.base_version, snap.base_version);
        store_u64(&new_shard.meta_seq, cs.sequence);
        store_u64(&new_shard.write_sequence, cs.sequence);
        debug_assert!(!cs.has_parent());
        store_bool(&new_shard.initial_flushed, true);

        let old_data = old_shard.get_data();
        let old_inner_key_off = old_data.inner_key_off;
        let mut old_mem_tbls = old_data.mem_tbls.clone();
        for mem_tbl in old_mem_tbls.drain(..) {
            self.send_free_mem_msg(FreeMemMsg::FreeMem(mem_tbl));
        }

        info!(
            "{} restore shard: mem_table_snap_version {}, change {:?}",
            new_shard.tag(),
            new_shard.load_mem_table_snap_version(),
            &cs,
        );
        if old_inner_key_off != new_inner_key_off {
            info!(
                "{} restore shard: inner key off {} -> {}",
                new_shard.tag(),
                old_inner_key_off,
                new_inner_key_off
            );
        }
        self.insert_shard_and_refresh(Arc::new(new_shard));

        Ok(())
    }

    fn apply_update_schema_meta(&self, shard: &Shard, cs: &ChangeSet) {
        info!("{} apply update schema meta", shard.tag(); "schema_file" => ?cs.get_schema_file_id());
        let old_data = shard.get_data();
        let mut builder = ShardDataBuilder::new(old_data);
        builder.set_schema(
            cs.get_schema_version(),
            cs.get_restore_version(),
            cs.get_schema_file(),
        );
        shard.set_data(builder.build());
    }

    fn apply_update_storage_class(&self, shard: &Shard, cs: &ChangeSet) {
        assert_eq!(cs.get_property_key(), STORAGE_CLASS_KEY);
        info!("{} apply update storage class", shard.tag(); "cs" => ?cs);
        if !cs.ln_tables.is_empty() {
            let old_data = shard.get_data();
            let mut scf_builder = ShardCfBuilder::new(WRITE_CF);
            for level in &old_data.cfs[WRITE_CF].levels {
                for old_tbl in level.tables.as_ref() {
                    let new_tbl = cs.ln_tables.get(&old_tbl.id()).unwrap().clone();
                    scf_builder.add_table(new_tbl, level.level);
                }
            }
            let new_cf = scf_builder.build();
            let mut new_cfs = old_data.cfs.clone();
            new_cfs[WRITE_CF] = new_cf;
            let mut builder = ShardDataBuilder::new(old_data);
            builder.set_cfs(new_cfs);
            shard.set_data(builder.build());
        }
        let sc_spec = StorageClassSpec::unmarshal(Some(cs.get_property_value()));
        // The unspecified storage class only reloads files, but does not set the
        // storage class property.
        if !sc_spec.is_specified() {
            info!(
                "{} del storage class property, reload files {}",
                shard.tag(),
                cs.ln_tables.len(),
            );
            shard.del_property(STORAGE_CLASS_KEY);
            let mut pending_ops = shard.pending_ops.write().unwrap();
            pending_ops.storage_class_spec = StorageClassSpec::default();
        } else {
            shard.set_property(STORAGE_CLASS_KEY, cs.get_property_value());
            info!(
                "{} set storage class spec to {:?}, reload files {}",
                shard.tag(),
                sc_spec,
                cs.ln_tables.len(),
            );
        }
    }

    fn apply_clear_columnar(&self, shard: &Shard, cs: &ChangeSet) {
        let restore_version = if cs.has_clear_columnar_with_restore_version() {
            cs.get_clear_columnar_with_restore_version()
                .get_restore_version()
        } else {
            shard.get_data().restore_version
        };
        info!(
            "{} shard apply clear_columnar, restore_version: {:?}",
            shard.tag(),
            restore_version
        );
        let old_data = shard.get_data();
        let mut builder = ShardDataBuilder::new(old_data);
        builder.clear_schema(restore_version);
        builder.set_columnar_levels(ColumnarLevels::new());
        builder.set_vector_indexes(VectorIndexes::default());
        builder.set_columnar_table_ids(vec![]);
        shard.set_data(builder.build());
    }

    fn apply_columnar_compaction(&self, shard: &Shard, cs: &ChangeSet) {
        if self.opts.ignore_columnar_table_load {
            return;
        }
        let col_comp = cs.get_columnar_compaction();
        let is_manual_major_compaction = col_comp.get_is_manual_major_compaction();
        let col_change = col_comp.get_columnar_change();
        let old_data = shard.get_data();
        let mut new_col_levels = old_data.col_levels.clone();
        if col_comp.target_level == 2 {
            new_col_levels.l2_snap_version = col_comp.get_snap_version().into();
        }
        let deletes: HashSet<u64> = col_change
            .get_columnar_deletes()
            .iter()
            .map(|del| del.get_id())
            .collect();
        new_col_levels.retain(|c| !deletes.contains(&c.get_file().id()));
        for create in col_change.get_columnar_creates() {
            let col_file = cs.col_files.get(&create.get_id()).unwrap().clone();
            new_col_levels.add_file(create.get_level() as usize, col_file);
        }
        new_col_levels.sort();
        new_col_levels
            .unconverted_l0s
            .retain(|l0| !col_comp.row_l0s.contains(&l0.id()));
        let mut columnar_table_ids = old_data.columnar_table_ids.clone();
        if columnar_table_ids.is_empty() {
            // If the columnar_table_ids is empty, this must be the first columnar major
            // compaction or manual columnar major compaction.
            let new_flushed_l0_tbls: Vec<L0Table> = old_data
                .l0_tbls
                .iter()
                .filter(|tbl| !col_comp.get_row_l0s().contains(&tbl.id()))
                .cloned()
                .collect();
            new_col_levels.unconverted_l0s.extend(new_flushed_l0_tbls);
        }
        let mut clear_schema = false;
        if col_comp.get_snap_version() == 0 {
            new_col_levels.unconverted_l0s.clear();
            columnar_table_ids.clear();

            // Clear the schema to re-sync.
            clear_schema = old_data.has_too_many_unconverted_l0s();
            if clear_schema {
                warn!("{} apply columnar compaction: too many unconverted l0s, clear schema", shard.tag();
                    "schema_file" => ?old_data.schema_file_id(), "schema_version" => old_data.schema_version);
            }
        } else {
            columnar_table_ids.extend_from_slice(col_comp.get_columnar_table_ids());
            columnar_table_ids.sort_unstable();
            columnar_table_ids.dedup();
            columnar_table_ids
                .retain(|id| !col_comp.get_columnar_table_ids_to_clear().contains(id));
        };
        // If columnar_table_ids is empty, this must be a major compaction to clear the
        // last table. Make sure all unconverted l0s are cleared.
        if columnar_table_ids.is_empty() {
            new_col_levels.unconverted_l0s.clear();
        }
        let mut vector_indexes = old_data.vector_indexes.clone();
        vector_indexes.retain(|vec_idx| columnar_table_ids.contains(&vec_idx.table_id));
        let old_restore_version = old_data.restore_version;
        let mut builder = ShardDataBuilder::new(old_data);
        if clear_schema {
            builder.clear_schema(old_restore_version);
        }
        builder.set_columnar_levels(new_col_levels);
        builder.set_columnar_table_ids(columnar_table_ids);
        builder.set_vector_indexes(vector_indexes);
        shard.set_data(builder.build());
        if is_manual_major_compaction {
            shard.set_property(MANUAL_MAJOR_COMPACTION, MANUAL_MAJOR_COMPACTION_DISABLE);
        }
    }

    fn apply_update_vector_index(&self, shard: &Shard, cs: &ChangeSet) {
        if self.opts.ignore_columnar_table_load {
            return;
        }
        let update_vector_index = cs.get_update_vector_index();
        let mut vector_indexes = shard.get_data().vector_indexes.clone();
        for added in &update_vector_index.added {
            let vec_idx_file = cs.vec_index_files.get(&added.id).unwrap().clone();
            vector_indexes.add_index_file(vec_idx_file);
        }
        vector_indexes.remove_index_file(
            update_vector_index.table_id,
            update_vector_index.index_id,
            update_vector_index.col_id,
            &update_vector_index.removed,
        );
        vector_indexes.update_snap_version(
            update_vector_index.table_id,
            update_vector_index.index_id,
            update_vector_index.col_id,
            update_vector_index.snap_version.into(),
        );
        let mut builder = ShardDataBuilder::new(shard.get_data());
        builder.set_vector_indexes(vector_indexes);
        shard.set_data(builder.build());
    }
}

fn try_pack_sstable_meta(
    sstable: &SsTable,
    meta_pack_scheduler: Option<&MetaPackScheduler>,
    ctx: NewSsTableCtx,
) {
    let Some((meta_pack_scheduler, (footer, props_data))) =
        meta_pack_scheduler.zip(ctx.footer_and_properties_data)
    else {
        return;
    };

    let tiny_meta = SstTinyMeta::from_sstable(sstable, footer, &props_data);
    meta_pack_scheduler.try_pack(tiny_meta.into());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_tables_size_from_snapshot() {
        let mut snap = pb::Snapshot::default();

        snap.mut_l0_creates().push(pb::L0Create {
            id: 1,
            size: 10,
            ..Default::default()
        });
        snap.mut_l0_creates().push(pb::L0Create {
            id: 2,
            size: 20,
            ..Default::default()
        });

        snap.mut_unconverted_l0s().push(2);

        snap.mut_table_creates().push(pb::TableCreate {
            meta_offset: 100,
            ..Default::default()
        });
        snap.mut_table_creates().push(pb::TableCreate {
            meta_offset: 200,
            ..Default::default()
        });

        snap.mut_columnar_creates().push(pb::ColumnarCreate {
            meta_offset: 1000,
            ..Default::default()
        });

        let mut vec_idx = pb::VectorIndex::default();
        vec_idx.mut_files().push(pb::VectorIndexFile {
            meta_offset: 10000,
            ..Default::default()
        });
        let mut vec_idx1 = pb::VectorIndex::default();
        vec_idx1.mut_files().push(pb::VectorIndexFile {
            meta_offset: 20000,
            ..Default::default()
        });
        vec_idx1.mut_files().push(pb::VectorIndexFile {
            meta_offset: 30000,
            ..Default::default()
        });
        snap.mut_vector_indexes().push(vec_idx);
        snap.mut_vector_indexes().push(vec_idx1);

        assert_eq!(
            estimate_tables_size_from_snapshot(&snap, PrepareType::SstOnly),
            10 + 20 + 100 + 200
        );
        assert_eq!(
            estimate_tables_size_from_snapshot(&snap, PrepareType::ColumnarOnly),
            20 + 1000 + 10000 + 20000 + 30000
        );
        assert_eq!(
            estimate_tables_size_from_snapshot(&snap, PrepareType::All),
            10 + 20 + 100 + 200 + 1000 + 10000 + 20000 + 30000
        );
    }
}
