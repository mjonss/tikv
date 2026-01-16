// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    fs,
    io::{Read, Seek, SeekFrom},
    iter::Iterator,
    ops::Deref,
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
};

use bytes::{Buf, Bytes};
use cloud_encryption::EncryptionKey;
use dashmap::mapref::entry::Entry;
use file_system::IoType;
use kvenginepb::{TxnFileRef, TxnFileRefs};
use protobuf::Message;
use schema::schema::StorageClassSpec;
use tikv_util::{mpsc::Receiver, time::Instant};
use txn_types::TimeStamp;

use crate::{
    EngineCore,
    apply::ChangeSet,
    context::IaCtx,
    dfs::FileType,
    error::IoContext,
    ia::{
        ia_auto_file::IaAutoFile,
        ia_file::{IaFile, table_meta_file_local_path},
    },
    limiter::{DfsLoadLimiter, DfsLoadLimiterPermit},
    meta::FileMeta,
    metrics::{ENGINE_LEVEL_WRITE_VEC, PREPARE_COUNTER_VEC},
    table::{
        BoundedDataSet,
        file::{FdCache, File, InMemFile, LocalFile},
        fts::FtsCache,
        get_local_dir,
        schema_file::SchemaFile,
        sstable::{PROP_KEY_MAX_TS, SsTable, SsTableCore, SsTableProperty},
        tiny_meta::{MetaPackReader, TypedTinyMeta},
        vector_index::VectorIndexFile,
    },
    *,
};

#[derive(Default)]
pub struct PrepareOpts<'a> {
    pub use_direct_io: bool,
    pub prepare_type: FilePrepareType,
    // The snap contains the current files that need to be reloaded.
    pub reload_snap: Option<kvenginepb::Snapshot>,
    pub table_filter: Option<LoadTableFilterFn>,
    // encryption_key will be ignored if cs is snapshot or restore_shard.
    pub encryption_key: Option<EncryptionKey>,
    pub meta_pack_reader: Option<&'a MetaPackReader>,
}

impl EngineCore {
    pub fn prepare_change_set(
        &self,
        cs: kvenginepb::ChangeSet,
        opts: PrepareOpts<'_>,
    ) -> Result<ChangeSet> {
        let PrepareOpts {
            use_direct_io,
            prepare_type,
            reload_snap,
            table_filter,
            encryption_key,
            meta_pack_reader,
        } = opts;

        let mut ids: HashMap<u64, FileMeta> = HashMap::new();
        let mut lock_txn_file_refs: Vec<TxnFileRef> = vec![];
        let mut cs = ChangeSet::new(cs);
        let mut snap = None;
        let mut schema_meta = None;
        if cs.has_flush() {
            let flush = cs.get_flush();
            if flush.has_l0_create() {
                ids.insert(
                    flush.get_l0_create().id,
                    FileMeta::from_l0_table(flush.get_l0_create()),
                );
            }
            for l0 in flush.get_l0_creates() {
                ids.insert(l0.get_id(), FileMeta::from_l0_table(l0));
            }
        }
        if cs.has_compaction() {
            let comp = cs.get_compaction();
            if !is_move_down(comp) {
                for tbl in &comp.table_creates {
                    ids.insert(tbl.id, FileMeta::from_table(tbl));
                }
                for bt in comp.get_blob_tables() {
                    ids.insert(bt.get_id(), FileMeta::from_blob_table(bt));
                }
            } else if comp.level == 0 && (prepare_type.not_local() || self.opts.ia.force_ia) {
                for tbl in &comp.table_creates {
                    ids.insert(tbl.id, FileMeta::from_table(tbl));
                }
            }
        }
        if cs.has_destroy_range() {
            for t in cs.get_destroy_range().get_table_creates() {
                ids.insert(t.id, FileMeta::from_table(t));
            }
            for col in cs.get_destroy_range().get_columnar_creates() {
                ids.insert(col.id, FileMeta::from_columnar_table(col));
            }
        }
        if cs.has_truncate_ts() {
            for t in cs.get_truncate_ts().get_table_creates() {
                ids.insert(t.id, FileMeta::from_table(t));
            }
            for col in cs.get_truncate_ts().get_columnar_creates() {
                ids.insert(col.id, FileMeta::from_columnar_table(col));
            }
        }
        if cs.has_trim_over_bound() {
            for t in cs.get_trim_over_bound().get_table_creates() {
                ids.insert(t.id, FileMeta::from_table(t));
            }
            for col in cs.get_trim_over_bound().get_columnar_creates() {
                ids.insert(col.id, FileMeta::from_columnar_table(col));
            }
        }
        if cs.has_snapshot() {
            snap = Some(cs.get_snapshot());
        }
        if cs.has_initial_flush() {
            let snap = cs.get_initial_flush();
            self.collect_snap_ids(snap, &mut ids);
            // Load schema file when prepare initial_flush
            if snap.has_schema_meta() {
                schema_meta = Some(snap.get_schema_meta());
            }
        }
        if cs.has_restore_shard() {
            snap = Some(cs.get_restore_shard());
        }
        if cs.has_ingest_files() {
            let ingest_files = cs.get_ingest_files();
            for l0 in ingest_files.get_l0_creates() {
                ids.insert(l0.id, FileMeta::from_l0_table(l0));
            }
            for tbl in ingest_files.get_table_creates() {
                ids.insert(tbl.id, FileMeta::from_table(tbl));
            }
            for blob in ingest_files.get_blob_creates() {
                ids.insert(blob.id, FileMeta::from_blob_table(blob));
            }
        }
        if cs.has_major_compaction() {
            let major_comp = cs.get_major_compaction();
            for tbl in major_comp.get_sstable_change().get_table_creates() {
                ids.insert(tbl.get_id(), FileMeta::from_table(tbl));
            }
            for blob in major_comp.get_new_blob_tables() {
                ids.insert(blob.get_id(), FileMeta::from_blob_table(blob));
            }
        }
        if cs.has_update_schema_meta() {
            schema_meta = Some(cs.get_update_schema_meta());
        }
        if let Some(reload_snap) = &reload_snap {
            self.collect_snap_ids(reload_snap, &mut ids);
        }
        if cs.has_columnar_compaction() {
            let columnar_comp = cs.get_columnar_compaction();
            for tbl in columnar_comp.get_columnar_change().get_columnar_creates() {
                ids.insert(tbl.get_id(), FileMeta::from_columnar_table(tbl));
            }
        }
        if cs.has_update_vector_index() {
            let update_vec_idx = cs.get_update_vector_index();
            for vec_idx_file in update_vec_idx.get_added() {
                ids.insert(
                    vec_idx_file.get_id(),
                    FileMeta::from_vector_index_file(vec_idx_file),
                );
            }
        }
        for file in cs.get_fts_update().get_l0_add_files() {
            ids.insert(file.get_id(), FileMeta::from_fts_l0_file(file));
        }
        for file in cs.get_fts_update().get_l1_add_files() {
            ids.insert(file.get_id(), FileMeta::from_fts_l1_file(file));
        }
        for file in cs.get_fts_update().get_l2_add_files() {
            ids.insert(file.get_id(), FileMeta::from_fts_l2_file(file));
        }
        let mut encryption_key = encryption_key;
        let mut prepare_type = prepare_type;
        if let Some(snap) = snap {
            self.collect_snap_ids(snap, &mut ids);
            lock_txn_file_refs.extend(collect_snap_lock_txn_file_refs(snap));
            encryption_key = get_shard_property(ENCRYPTION_KEY, snap.get_properties())
                .map(|v| self.master_key.decrypt_encryption_key(&v).unwrap());
            prepare_type = FilePrepareType::from_snapshot(snap);
            if snap.has_schema_meta() {
                schema_meta = Some(snap.get_schema_meta());
            }
        }
        let schema_file = if let Some(schema_meta) = schema_meta {
            let schema_file_id = schema_meta.get_file_id();
            if schema_file_id == 0 {
                // Set schema_file to None if schema file id is 0, no need to mark tombstone.
                None
            } else {
                Some(
                    self.fs
                        .get_runtime()
                        .block_on(self.load_schema_file(schema_file_id))?,
                )
            }
        } else {
            None
        };

        let tag = ShardTag::new(self.get_engine_id(), IdVer::from_change_set(&cs));
        if let Some(table_filter) = table_filter {
            info!(
                "{} is preparing change set (before table filter)", tag;
                "ids" => ?ids.keys(),
            );
            cs.unloaded_tables = ids
                .extract_if(|_, tb| !table_filter(cs.shard_id, tb)) // !table_filter: table will not load
                .collect();
        }

        if self.opts.ignore_columnar_table_load {
            ids.retain(|_, meta| {
                meta.file_type != FileType::Columnar
                    && meta.file_type != FileType::VectorIndex
                    && meta.file_type != FileType::FtsPackedFile
                    && meta.file_type != FileType::FtsDedicatedFile
            });
        }

        info!(
            "{} is preparing change set, loading file by ids", tag;
            "ids" => ?ids.keys(),
            "encryption_key" => ?encryption_key,
            "prepare_type" => ?prepare_type,
        );
        self.load_tables_by_ids(
            tag,
            cs.shard_id,
            cs.shard_ver,
            &ids,
            &mut cs,
            use_direct_io,
            prepare_type,
            encryption_key.clone(),
            meta_pack_reader,
        )?;

        if !lock_txn_file_refs.is_empty() {
            cs.lock_txn_files = self.txn_chunk_mgr.load_txn_files_from_refs(
                cs.shard_id,
                cs.shard_ver,
                &lock_txn_file_refs,
                encryption_key.clone(),
            )?;
            info!(
                "{} is preparing change set, load lock txn files", tag;
                "lock_txn_files" => ?cs.lock_txn_files,
                "encryption_key" => ?encryption_key,
            );
        }
        cs.schema_file = schema_file;
        Ok(cs)
    }

    fn collect_snap_ids(&self, snap: &kvenginepb::Snapshot, ids: &mut HashMap<u64, FileMeta>) {
        for blob in snap.get_blob_creates() {
            ids.insert(blob.id, FileMeta::from_blob_table(blob));
        }
        for l0 in snap.get_l0_creates() {
            ids.insert(l0.id, FileMeta::from_l0_table(l0));
        }
        for ln in snap.get_table_creates() {
            ids.insert(ln.id, FileMeta::from_table(ln));
        }
        for col in snap.get_columnar_creates() {
            ids.insert(col.id, FileMeta::from_columnar_table(col));
        }
        for vec_idx in snap.get_vector_indexes() {
            for vec_idx_file in vec_idx.get_files() {
                ids.insert(
                    vec_idx_file.id,
                    FileMeta::from_vector_index_file(vec_idx_file),
                );
            }
        }
        for file in snap.get_fts_l0_files() {
            ids.insert(file.id, FileMeta::from_fts_l0_file(file));
        }
        for file in snap.get_fts_l1_files() {
            ids.insert(file.id, FileMeta::from_fts_l1_file(file));
        }
        for file in snap.get_fts_l2_files() {
            ids.insert(file.id, FileMeta::from_fts_l2_file(file));
        }
    }

    fn load_tables_by_ids(
        &self,
        tag: ShardTag,
        shard_id: u64,
        shard_ver: u64,
        ids: &HashMap<u64, FileMeta>,
        cs: &mut ChangeSet,
        use_direct_io: bool,
        shard_prepare_type: FilePrepareType,
        encryption_key: Option<EncryptionKey>,
        meta_pack_reader: Option<&MetaPackReader>,
    ) -> Result<()> {
        let start_time = Instant::now_coarse();
        let (result_tx, result_rx) = tikv_util::mpsc::bounded(ids.len());
        let runtime = self.fs.get_runtime().handle();
        let opts = dfs::Options::default().with_shard(shard_id, shard_ver);
        let current_ts: Option<u64> = matches!(shard_prepare_type, FilePrepareType::AutoIa(_))
            .then(|| TimeStamp::now().into_inner());
        let available_permits = self.dfs_load_limiter.available_permits();
        let mut join_set = tokio::task::JoinSet::new();
        let mut msg_count = 0;
        for (&id, fm) in ids {
            let fs = self.fs.clone();
            let prepare_type = if self.ia_ctx.is_enabled() {
                shard_prepare_type.convert_for_prepare_table(fm, self.opts.ia.force_ia)
            } else {
                FilePrepareType::Local
            };
            let tiny_meta = meta_pack_reader
                .as_ref()
                .and_then(|reader| reader.get(id))
                .map(|tm| tm.try_convert_to(fm.file_type))
                .inspect(|_| {
                    PREPARE_COUNTER_VEC.tiny_meta_hit.inc();
                })
                .unwrap_or_default();

            // Try open local file first.
            let mut table_meta_file = None;
            let file: Option<Arc<dyn File>> = match &prepare_type {
                FilePrepareType::Local => {
                    // TODO: support tiny meta.
                    self.open_local_file(id, fm.file_type, None).ok().map(|f| {
                        PREPARE_COUNTER_VEC.local_files.inc();
                        Arc::new(f) as _
                    })
                }
                FilePrepareType::Ia => self
                    .try_open_local_ia_file(tag, id, fm, use_direct_io, &tiny_meta)?
                    .map(|f| {
                        PREPARE_COUNTER_VEC.local_ia_files.inc();
                        Arc::new(f) as _
                    }),
                FilePrepareType::AutoIa(spec) => {
                    let (ia_auto_f, table_meta_f) = self.try_open_local_auto_ia_file(
                        tag,
                        id,
                        fm,
                        use_direct_io,
                        current_ts.unwrap(),
                        spec,
                        &TypedTinyMeta::None, // TODO: support tiny meta.
                    )?;
                    table_meta_file = table_meta_f;
                    ia_auto_f.map(|f| {
                        PREPARE_COUNTER_VEC.local_auto_ia_files.inc();
                        Arc::new(f) as _
                    })
                }
            };
            if let Some(file) = file {
                cs.add_file(
                    id,
                    file,
                    fm,
                    self.cache.clone(),
                    None,
                    None,
                    FtsCache::disabled(),
                    encryption_key.clone(),
                    self.columnar_meta_cache.clone(),
                    tiny_meta,
                    self.meta_pack_scheduler.as_ref(),
                )?;
                continue;
            }

            // Load from remote.
            let tx = result_tx.clone();
            let fm = fm.clone();
            join_set.spawn_on(
                Self::load_file_from_remote(
                    self.dfs_load_limiter.clone(),
                    fs,
                    id,
                    fm,
                    prepare_type,
                    opts,
                    tag,
                    table_meta_file,
                    current_ts,
                    tx,
                ),
                runtime,
            );
            if msg_count < self.opts.dfs_load_concurrency_per_request {
                msg_count += 1;
            } else {
                self.recv_file_data(cs, use_direct_io, &result_rx, encryption_key.clone())?;
            }
        }
        for _ in 0..msg_count {
            self.recv_file_data(cs, use_direct_io, &result_rx, encryption_key.clone())?;
        }
        PREPARE_COUNTER_VEC.total_files.inc_by(ids.len() as u64);
        info!("{} load tables by ids", tag; "ids" => ids.len(),
            "takes" => ?start_time.saturating_elapsed(), "available_permits" => available_permits);
        Ok(())
    }

    async fn load_file_from_remote(
        dfs_load_limiter: DfsLoadLimiter,
        fs: Arc<dyn dfs::Dfs>,
        id: u64,
        fm: FileMeta,
        prepare_type: FilePrepareType,
        opts: dfs::Options,
        tag: ShardTag,
        table_meta_file: Option<Arc<dyn File>>,
        current_ts: Option<u64>,
        tx: tikv_util::mpsc::Sender<Result<LoadRemoteResult>>,
    ) {
        let task = async {
            let permit = dfs_load_limiter.acquire_permit().await;
            let res = match prepare_type {
                FilePrepareType::Local => {
                    Self::load_remote_table(id, fm.file_type, opts, fs.as_ref())
                        .await
                        .map(|local_data| PreparedFileResult::Local { local_data })
                }
                FilePrepareType::Ia => {
                    Self::load_remote_table_meta(tag, id, &fm, opts, fs.as_ref())
                        .await
                        .map(|table_meta_data| PreparedFileResult::Ia { table_meta_data })
                }
                FilePrepareType::AutoIa(spec) => {
                    Self::load_remote_auto_ia_file(
                        tag,
                        id,
                        &fm,
                        opts,
                        fs.as_ref(),
                        table_meta_file,
                        current_ts.unwrap(),
                        spec,
                    )
                    .await
                }
            };
            (res, permit)
        };

        let (res, permit) = task.await;
        let _ = tx.send(res.map(|prepared| LoadRemoteResult {
            id,
            fm,
            prepared,
            permit,
        }));
    }

    fn recv_file_data(
        &self,
        cs: &mut ChangeSet,
        use_direct_io: bool,
        result_rx: &Receiver<Result<LoadRemoteResult>>,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<()> {
        let LoadRemoteResult {
            id,
            fm,
            prepared,
            permit,
        } = result_rx.recv().unwrap()?;
        let file: Arc<dyn File> = match prepared {
            PreparedFileResult::Local { local_data } => self
                .save_and_open_table(id, &fm, local_data, permit, use_direct_io)
                .map(|f| {
                    PREPARE_COUNTER_VEC.remote_files.inc();
                    Arc::new(f) as _
                })?,
            PreparedFileResult::Ia { table_meta_data } => self
                .save_and_open_ia_file(id, &fm, table_meta_data, Some(permit), use_direct_io)
                .map(|f| {
                    PREPARE_COUNTER_VEC.remote_ia_files.inc();
                    Arc::new(f) as _
                })?,
            prepared @ PreparedFileResult::AutoIa { .. } => self
                .save_and_open_auto_ia_file(id, &fm, prepared, permit, use_direct_io)
                .map(|f| {
                    PREPARE_COUNTER_VEC.remote_auto_ia_files.inc();
                    Arc::new(f) as _
                })?,
        };
        cs.add_file(
            file.id(),
            file,
            &fm,
            self.cache.clone(),
            None,
            None,
            FtsCache::disabled(),
            encryption_key,
            self.columnar_meta_cache.clone(),
            TypedTinyMeta::None,
            self.meta_pack_scheduler.as_ref(),
        )?;
        Ok(())
    }

    pub fn load_unloaded_tables(
        &self,
        shard_id: u64,
        shard_ver: u64,
        use_direct_io: bool,
    ) -> Result<()> {
        let shard = self.get_shard(shard_id).unwrap();
        let tag = shard.tag();
        let mut cs = ChangeSet::new(kvenginepb::ChangeSet::default());
        let data = shard.get_data();
        let mut builder = ShardDataBuilder::new(data.clone());

        let load_tables = data
            .unloaded_tbls
            .iter()
            .filter(|&(_, tbl)| shard.overlap_bound(tbl.data_bound()))
            .map(|(id, tbl)| (*id, tbl.clone()))
            .collect();
        info!("{} load_unloaded_tables: {:?}", tag, load_tables);

        self.load_tables_by_ids(
            tag,
            shard_id,
            shard_ver,
            &load_tables,
            &mut cs,
            use_direct_io,
            FilePrepareType::Local,
            shard.encryption_key.clone(),
            None,
        )?;

        // level 0
        let mut new_l0s = data.l0_tbls.clone();
        // blob
        let mut new_blob_tbl_map = data.blob_tbl_map.as_ref().clone();
        // columnar
        let mut new_columnar_levels = data.col_levels.clone();
        // level n
        let mut scf_builders = vec![];
        for cf in 0..NUM_CFS {
            let scf = ShardCfBuilder::new(cf);
            scf_builders.push(scf);
        }
        for (cf, shard_cf) in data.cfs.iter().enumerate() {
            for lh in &shard_cf.levels {
                for sst in &(*lh.tables) {
                    let scf = &mut scf_builders.as_mut_slice()[cf];
                    scf.add_table(sst.clone(), lh.level);
                }
            }
        }
        // vector index
        let mut new_vec_indexes = data.vector_indexes.clone();
        // fts
        let mut new_fts_levels = data.fts_levels.deref().clone();
        let mut new_fts_l0_files = vec![];
        let mut new_fts_l1_files = vec![];
        for (id, tbl) in load_tables.into_iter() {
            match tbl.file_type {
                FileType::Sst => {
                    if tbl.level == 0 {
                        let l0 = cs.l0_tables.get(&id).unwrap().clone();
                        new_l0s.push(l0);
                    } else {
                        let sst = cs.ln_tables.get(&id).unwrap().clone();
                        let scf = &mut scf_builders.as_mut_slice()[tbl.get_cf() as usize];
                        scf.add_table(sst, tbl.get_level() as usize);
                    }
                }
                FileType::Columnar => {
                    let col_file = cs.col_files.get(&id).unwrap().clone();
                    new_columnar_levels.add_file(tbl.level as usize, col_file);
                }
                FileType::Blob => {
                    let blob_file = cs.blob_tables.get(&id).unwrap().clone();
                    new_blob_tbl_map.insert(id, blob_file);
                }
                FileType::TxnChunk | FileType::Schema => unreachable!("not supported"),
                FileType::VectorIndex => {
                    let vec_idx_file = cs.vec_index_files.get(&id).unwrap().clone();
                    new_vec_indexes.add_index_file(vec_idx_file);
                }
                FileType::FtsPackedFile => {
                    if let Some(l0_file) = cs.fts_l0_files.get(&id) {
                        new_fts_l0_files.push(l0_file.clone());
                    } else if let Some(l1_file) = cs.fts_l1_files.get(&id) {
                        new_fts_l1_files.push(l1_file.clone());
                    } else {
                        panic!("unexpected FTS file id {}", id);
                    }
                }
                FileType::FtsDedicatedFile => {
                    let l2_file = cs.fts_l2_files.get(&id).unwrap().clone();
                    new_fts_levels.insert_l2_file(l2_file);
                }
            }
        }
        if !new_fts_l0_files.is_empty() {
            new_fts_levels.mut_l0(move |l0| l0.extend(new_fts_l0_files));
        }
        if !new_fts_l1_files.is_empty() {
            new_fts_levels.mut_l1(move |l1| l1.extend(new_fts_l1_files));
        }
        new_l0s.sort_by(|a, b| b.snap_version().cmp(&a.snap_version()));
        let mut scfs = [ShardCf::new(0), ShardCf::new(1), ShardCf::new(2)];
        for cf in 0..NUM_CFS {
            let scf = &mut scf_builders.as_mut_slice()[cf];
            scfs[cf] = scf.build();
        }
        new_columnar_levels.sort();
        new_vec_indexes.sort();

        builder.set_l0_tbls(new_l0s);
        builder.set_blob_tbls(new_blob_tbl_map);
        builder.set_cfs(scfs);
        builder.set_unloaded_tbls(HashMap::new());
        builder.set_columnar_levels(new_columnar_levels);
        builder.set_vector_indexes(new_vec_indexes);
        builder.set_fts_levels(new_fts_levels);
        shard.set_data(builder.build());
        Ok(())
    }

    fn write_local_file(
        &self,
        id: u64,
        data: Bytes,
        use_direct_io: bool,
        file_type: FileType,
    ) -> Result<()> {
        let local_file_name = self.local_file_path(id, file_type);
        self.write_local_file_with_file_path(id, data, use_direct_io, &local_file_name)
    }

    fn write_local_file_with_file_path(
        &self,
        id: u64,
        data: Bytes,
        use_direct_io: bool,
        file_path: &Path,
    ) -> Result<()> {
        let start_time = Instant::now_coarse();
        let tmp_file_name = Self::tmp_file_path(file_path);
        if use_direct_io {
            let mut writer =
                file_system::DirectWriter::new(self.rate_limiter.clone(), IoType::Compaction);
            writer
                .write_to_file(data.chunk(), &tmp_file_name)
                .table_ctx(id, "write_local_file.direct_write_tmp")?;
        } else {
            let mut file = std::fs::File::create(&tmp_file_name)
                .table_ctx(id, "write_local_file.create_tmp")?;
            let synced_data = file_system::write_all_with_rate_limiter(
                &mut file,
                data.chunk(),
                &self.rate_limiter,
                256 * 1024,
            )
            .table_ctx(id, "write_local_file.write_tmp")?;
            if synced_data < data.len() {
                file.sync_data()
                    .table_ctx(id, "write_local_file.sync_tmp")?;
            }
        }
        let rename_time = Instant::now_coarse();
        std::fs::rename(tmp_file_name, file_path).table_ctx(id, "write_local_file.rename")?;
        let end_time = Instant::now_coarse();
        let takes_msg = format!(
            "{:?}(w:{:?},r:{:?})", // total(write,sync,rename)
            end_time.saturating_duration_since(start_time),
            rename_time.saturating_duration_since(start_time),
            end_time.saturating_duration_since(rename_time)
        );
        info!(
            "{}: write local file {}", self.get_engine_id(), id;
            "size" => data.len(),
            "takes" => takes_msg,
            "path" => ?file_path.file_name().unwrap_or_default(),
        );
        Ok(())
    }

    pub fn write_local_file_if_not_exists(
        &self,
        id: u64,
        data: Bytes,
        file_type: FileType,
    ) -> Result<()> {
        let file_path = self.local_file_path(id, file_type);
        let _guard = self.lock_file(id);
        if !file_path.exists() {
            self.write_local_file(id, data, false, file_type)?;
        } else {
            let meta = file_path.metadata().table_ctx(id, "metadata")?;
            if meta.size() != data.len() as u64 {
                return Err(table::Error::InvalidFileSize.into());
            }
        }
        Ok(())
    }

    pub fn read_local_file(
        &self,
        id: u64,
        file_type: FileType,
        start_off: u64,
        end_off: Option<u64>,
        check_exists_only: bool,
    ) -> Result<Bytes> {
        let _guard = self.lock_file(id);
        let path = self.local_file_path(id, file_type);
        if check_exists_only {
            let _ = fs::metadata(path).table_ctx(id, "read_local_file.metadata")?;
            return Ok(Bytes::new());
        }
        let mut f = fs::File::open(path).table_ctx(id, "read_local_file.open")?;
        if let Some(end_off) = end_off {
            let mut buf = vec![0; (end_off - start_off) as usize];
            f.read_exact_at(&mut buf, start_off).dfs_ctx(id, "read")?;
            Ok(buf.into())
        } else {
            if start_off > 0 {
                f.seek(SeekFrom::Start(start_off))
                    .table_ctx(id, "read_local_file.seek")?;
            }
            let mut data = vec![];
            f.read_to_end(&mut data).table_ctx(id, "read_local_file")?;
            Ok(data.into())
        }
    }

    pub(crate) fn open_local_file(
        &self,
        id: u64,
        file_type: FileType,
        file_size: Option<u64>,
    ) -> Result<LocalFile> {
        let path = self.local_file_path(id, file_type);
        self.open_local_file_with_file_path(id, Some(self.fd_cache.clone()), path, file_size)
    }

    fn open_local_file_with_file_path(
        &self,
        id: u64,
        fd_cache: Option<FdCache>,
        file_path: PathBuf,
        file_size_opt: Option<u64>,
    ) -> Result<LocalFile> {
        let _guard = self.lock_file(id);
        Ok(LocalFile::open_ext(
            id,
            file_path,
            file_size_opt,
            fd_cache,
            self.loaded.load(Relaxed),
        )?)
    }

    fn try_open_local_ia_file(
        &self,
        tag: ShardTag,
        id: u64,
        fm: &FileMeta,
        use_direct_io: bool,
        tiny_meta: &TypedTinyMeta,
    ) -> Result<Option<IaFile>> {
        let IaCtx::Enabled(ia_mgr, data_dirs) = self.ia_ctx.clone() else {
            return Ok(None);
        };
        let data_dir = get_local_dir(&data_dirs, id);
        let meta_file_path = table_meta_file_local_path(id, fm.file_type, data_dir.deref());
        let mut table_meta_file = self.open_local_file_with_file_path(
            id,
            ia_mgr.get_meta_fd_cache(),
            meta_file_path.clone(),
            tiny_meta.meta_size(),
        );
        if table_meta_file.is_err()
            && let Ok(local_file) = self.open_local_file(id, fm.file_type, tiny_meta.file_size())
        {
            let table_meta_off = fm.table_meta_off as u64;
            // Read table meta from local sst file.
            if let Ok(data) = local_file
                .read(
                    table_meta_off,
                    local_file.size() as usize - table_meta_off as usize,
                )
                .map_err(Into::into)
                .and_then(|data| validate_table_meta_off(tag, id, fm, data))
            {
                let data_len = data.len() as u64;
                self.write_local_file_with_file_path(id, data, use_direct_io, &meta_file_path)?;
                table_meta_file = self.open_local_file_with_file_path(
                    id,
                    ia_mgr.get_meta_fd_cache(),
                    meta_file_path,
                    Some(data_len),
                );
            };
        }
        if let Ok(table_meta_file) = table_meta_file {
            if let Ok(ia_file) =
                IaFile::open_with_tiny_meta(id, fm, Arc::new(table_meta_file), ia_mgr, tiny_meta)
            {
                return Ok(Some(ia_file));
            }
        }
        Ok(None)
    }

    async fn load_remote_table(
        id: u64,
        ftype: FileType,
        opts: dfs::Options,
        fs: &dyn dfs::Dfs,
    ) -> Result<Bytes> {
        fs.read_file(id, opts.with_type(ftype))
            .await
            .map_err(Into::into)
    }

    fn save_and_open_table(
        &self,
        id: u64,
        fm: &FileMeta,
        data: Bytes,
        permit: DfsLoadLimiterPermit,
        use_direct_io: bool,
    ) -> Result<LocalFile> {
        let data_len = data.len() as u64;
        self.write_local_file(id, data, use_direct_io, fm.file_type)?;
        drop(permit);

        ENGINE_LEVEL_WRITE_VEC
            .with_label_values(&[&fm.get_level().to_string()])
            .inc_by(data_len);

        let file = self.open_local_file(id, fm.file_type, Some(data_len))?;
        Ok(file)
    }

    async fn load_remote_table_meta(
        tag: ShardTag,
        id: u64,
        fm: &FileMeta,
        opts: dfs::Options,
        fs: &dyn dfs::Dfs,
    ) -> Result<Bytes> {
        let opts = opts
            .with_type(fm.file_type)
            .with_start_off(fm.table_meta_off as u64);
        fs.read_file(id, opts)
            .await
            .map_err(Into::into)
            .and_then(|data| validate_table_meta_off(tag, id, fm, data))
    }

    fn save_and_open_ia_file(
        &self,
        id: u64,
        fm: &FileMeta,
        table_meta_data: Bytes,
        permit: Option<DfsLoadLimiterPermit>,
        use_direct_io: bool,
    ) -> Result<IaFile> {
        let IaCtx::Enabled(ia_mgr, data_dirs) = &self.ia_ctx else {
            unreachable!("ia_ctx should be enabled");
        };

        let data_len = table_meta_data.len() as u64;
        let data_dir = get_local_dir(data_dirs, id);
        let meta_file_path = table_meta_file_local_path(id, fm.file_type, data_dir.deref());
        self.write_local_file_with_file_path(id, table_meta_data, use_direct_io, &meta_file_path)?;
        drop(permit);

        ENGINE_LEVEL_WRITE_VEC
            .with_label_values(&[&fm.get_level().to_string()])
            .inc_by(data_len);

        let meta_file = self.open_local_file_with_file_path(
            id,
            ia_mgr.get_meta_fd_cache(),
            meta_file_path,
            Some(data_len),
        )?;
        let table_meta_file = Arc::new(meta_file);
        Ok(IaFile::open(id, fm, table_meta_file, ia_mgr.clone())?)
    }

    fn try_open_local_auto_ia_file(
        &self,
        tag: ShardTag,
        id: u64,
        fm: &FileMeta,
        use_direct_io: bool,
        current_ts: u64,
        spec: &StorageClassSpec,
        tiny_meta: &TypedTinyMeta,
    ) -> Result<(
        Option<IaAutoFile>,
        Option<Arc<dyn File>>, // table_meta_file
    )> {
        debug_assert_eq!(fm.file_type, FileType::Sst);
        let ia_file = self.try_open_local_ia_file(tag, id, fm, use_direct_io, tiny_meta)?;
        let Some(ia_file) = ia_file else {
            return Ok((None, None));
        };

        let SsTableProperty::MaxTs(max_ts) = SsTableCore::extract_property_from_table_meta_file(
            ia_file.table_meta_file().as_ref(),
            PROP_KEY_MAX_TS,
        )?;
        let elapsed = TimeStamp::new(current_ts).duration_since(max_ts.into());
        let target_sc = spec.target_storage_class(elapsed);
        Ok(if target_sc.is_ia() {
            let auto_ia_file = IaAutoFile::new(ia_file, None, max_ts, spec.clone());
            (Some(auto_ia_file), None)
        } else if let Ok(local_file) = self.open_local_file(id, fm.file_type, tiny_meta.file_size())
        {
            let auto_ia_file =
                IaAutoFile::new(ia_file, Some(Arc::new(local_file)), max_ts, spec.clone());
            (Some(auto_ia_file), None)
        } else {
            (None, Some(ia_file.table_meta_file().clone()))
        })
    }

    async fn load_remote_auto_ia_file(
        tag: ShardTag,
        id: u64,
        fm: &FileMeta,
        opts: dfs::Options,
        fs: &dyn dfs::Dfs,
        table_meta_file: Option<Arc<dyn File>>,
        current_ts: u64,
        spec: StorageClassSpec,
    ) -> Result<PreparedFileResult> {
        debug_assert_eq!(fm.file_type, FileType::Sst);

        let table_meta_file = if let Some(table_meta_file) = table_meta_file {
            table_meta_file
        } else {
            let table_meta = Self::load_remote_table_meta(tag, id, fm, opts, fs).await?;
            Arc::new(InMemFile::new(id, table_meta.clone()))
        };

        let SsTableProperty::MaxTs(max_ts) = SsTableCore::extract_property_from_table_meta_file(
            table_meta_file.as_ref(),
            PROP_KEY_MAX_TS,
        )?;
        let elapsed = TimeStamp::new(current_ts).duration_since(max_ts.into());
        let target_sc = spec.target_storage_class(elapsed);
        let table_data = if target_sc.is_ia() {
            None
        } else {
            Some(Self::load_remote_table(id, fm.file_type, opts, fs).await?)
        };
        Ok(PreparedFileResult::AutoIa {
            table_meta_file,
            local_data: table_data,
            max_ts,
            spec,
        })
    }

    fn save_and_open_auto_ia_file(
        &self,
        id: u64,
        fm: &FileMeta,
        prepared: PreparedFileResult,
        permit: DfsLoadLimiterPermit,
        use_direct_io: bool,
    ) -> Result<IaAutoFile> {
        let PreparedFileResult::AutoIa {
            table_meta_file,
            local_data,
            max_ts,
            spec,
        } = prepared
        else {
            unreachable!();
        };
        let ia_file = if table_meta_file.path().is_none() {
            let table_meta_data = table_meta_file.read_all()?;
            self.save_and_open_ia_file(id, fm, table_meta_data, None, use_direct_io)?
        } else {
            let IaCtx::Enabled(ia_mgr, _) = &self.ia_ctx else {
                unreachable!("ia_ctx should be enabled");
            };
            IaFile::open(id, fm, table_meta_file, ia_mgr.clone())?
        };

        let local_file: Option<Arc<dyn File>> = if let Some(local_data) = local_data {
            let local_file = self.save_and_open_table(id, fm, local_data, permit, use_direct_io)?;
            Some(Arc::new(local_file) as _)
        } else {
            drop(permit);
            None
        };

        Ok(IaAutoFile::new(ia_file, local_file, max_ts, spec))
    }

    pub async fn load_schema_file(&self, id: u64) -> Result<SchemaFile> {
        if let Some(schema_file) = self.schema_files.get(&id) {
            return Ok(schema_file.clone());
        }
        let local_schema_file_path = self.local_schema_file_path(id);
        let res = fs::read(local_schema_file_path.as_path());
        if res.is_ok() {
            let in_mem_file = InMemFile::new(id, res.unwrap().into());
            let schema_file = SchemaFile::open(Arc::new(in_mem_file))?;
            return match self.schema_files.entry(id) {
                Entry::Occupied(e) => Ok(e.get().clone()),
                Entry::Vacant(e) => {
                    e.insert(schema_file.clone());
                    Ok(schema_file)
                }
            };
        }
        let opts = dfs::Options::default().with_type(FileType::Schema);
        let data = self.fs.read_file(id, opts).await?;
        let in_mem_file = InMemFile::new(id, data.clone());
        let schema_file = SchemaFile::open(Arc::new(in_mem_file))?;
        match self.schema_files.entry(id) {
            Entry::Occupied(e) => return Ok(e.get().clone()),
            Entry::Vacant(e) => {
                e.insert(schema_file.clone());
            }
        }
        // Only the first one will write the file to disk.
        let tmp_file_name = Self::tmp_file_path(&local_schema_file_path);
        fs::write(&tmp_file_name, data.chunk()).table_ctx(id, "load_schema_file.write_tmp")?;
        fs::rename(&tmp_file_name, local_schema_file_path)
            .table_ctx(id, "load_schema_file.rename")?;
        Ok(schema_file)
    }

    pub(crate) fn local_file_path(&self, file_id: u64, file_type: FileType) -> PathBuf {
        match file_type {
            FileType::Sst => self.local_sst_file_path(file_id),
            FileType::Blob => self.local_blob_file_path(file_id),
            FileType::Schema => self.local_schema_file_path(file_id),
            FileType::Columnar => self.local_columnar_file_path(file_id),
            FileType::TxnChunk => panic!("TxnChunk files are managed by TxnChunkManager"),
            FileType::VectorIndex => self.local_vector_index_file_path(file_id),
            FileType::FtsPackedFile => self.local_fts_packed_file_path(file_id),
            FileType::FtsDedicatedFile => self.local_fts_dedicated_file_path(file_id),
        }
    }

    pub(crate) fn local_sst_file_path(&self, file_id: u64) -> PathBuf {
        get_local_dir(&self.opts.local_dirs, file_id).join(new_sst_filename(file_id))
    }

    pub(crate) fn local_blob_file_path(&self, file_id: u64) -> PathBuf {
        get_local_dir(&self.opts.local_dirs, file_id).join(new_blob_filename(file_id))
    }

    pub(crate) fn local_schema_file_path(&self, file_id: u64) -> PathBuf {
        get_local_dir(&self.opts.local_dirs, file_id).join(new_schema_filename(file_id))
    }

    pub(crate) fn local_columnar_file_path(&self, file_id: u64) -> PathBuf {
        get_local_dir(&self.opts.local_dirs, file_id).join(new_columnar_filename(file_id))
    }

    pub(crate) fn local_vector_index_file_path(&self, file_id: u64) -> PathBuf {
        get_local_dir(&self.opts.local_dirs, file_id).join(new_vector_index_filename(file_id))
    }

    pub(crate) fn local_fts_packed_file_path(&self, file_id: u64) -> PathBuf {
        get_local_dir(&self.opts.local_dirs, file_id).join(new_fts_packed_filename(file_id))
    }

    pub(crate) fn local_fts_dedicated_file_path(&self, file_id: u64) -> PathBuf {
        get_local_dir(&self.opts.local_dirs, file_id).join(new_fts_dedicated_filename(file_id))
    }

    fn tmp_file_path(file_path: &Path) -> PathBuf {
        lazy_static::lazy_static! {
            static ref TMP_FILE_ID: AtomicU64 = AtomicU64::default();
        }
        let tmp_id = TMP_FILE_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        file_path.with_extension(format!("{tmp_id}.tmp"))
    }
}

pub fn collect_snap_lock_txn_file_refs(snap: &kvenginepb::Snapshot) -> Vec<TxnFileRef> {
    let props = snap.get_properties();
    debug_assert_eq!(props.keys.len(), props.values.len());
    for (key, val) in props.get_keys().iter().zip(props.get_values().iter()) {
        if key == TXN_FILE_REF {
            let mut txn_file_refs = TxnFileRefs::new();
            txn_file_refs.merge_from_bytes(val).unwrap();
            return txn_file_refs
                .take_txn_file_refs()
                .into_iter()
                .filter(|r| !r.lock_val_prefix.is_empty())
                .collect::<Vec<_>>();
        }
    }
    vec![]
}

// For safety. Table meta offset should have been fixed. See
// `ShardMeta::fix_table_meta_offset`.
fn validate_table_meta_off(tag: ShardTag, id: u64, fm: &FileMeta, data: Bytes) -> Result<Bytes> {
    debug_assert!(fm.can_use_ia(), "{}: {}: {:?}", tag, id, fm);
    if fm.table_meta_off > 0 {
        return Ok(data);
    }

    warn!("{} validate_table_meta_off: table meta offset is 0", tag; "id" => id, "fm" => ?fm);
    match fm.file_type {
        FileType::Sst => SsTable::get_meta_data(&data).map_err(|err| {
            error!("{} validate_table_meta_off: get meta data failed", tag;
                "err" => ?err, "id" => id, "fm" => ?fm);
            Error::TableError(err)
        }),
        FileType::Columnar => {
            // Columnar must have meta offset.
            debug_assert!(false);
            Ok(data)
        }
        FileType::VectorIndex => VectorIndexFile::get_meta_data(&data).map_err(|err| {
            error!("{} validate_table_meta_off: vector index must have meta offset", tag;
                "id" => id, "fm" => ?fm);
            Error::TableError(err)
        }),
        _ => Ok(data),
    }
}

#[derive(Debug, Clone, Default)]
pub enum FilePrepareType {
    #[default]
    Local,
    Ia,
    AutoIa(StorageClassSpec),
}

impl FilePrepareType {
    fn from_storage_class_spec(sc_spec: StorageClassSpec) -> Self {
        if sc_spec.must_be_ia() {
            Self::Ia
        } else if sc_spec.can_be_ia() {
            Self::AutoIa(sc_spec)
        } else {
            Self::Local
        }
    }

    pub fn from_shard_meta(meta: &ShardMeta) -> Self {
        Self::from_storage_class_spec(meta.get_storage_class_spec())
    }

    pub fn from_snapshot(snap: &kvenginepb::Snapshot) -> Self {
        Self::from_storage_class_spec(StorageClassSpec::unmarshal(
            get_shard_property(STORAGE_CLASS_KEY, snap.get_properties()).as_deref(),
        ))
    }

    pub fn not_local(&self) -> bool {
        !matches!(self, Self::Local)
    }

    pub fn convert_for_prepare_table(&self, fm: &FileMeta, force_ia: bool) -> Self {
        match fm.file_type {
            FileType::Sst if fm.can_use_ia() => {
                if force_ia {
                    Self::Ia
                } else {
                    self.clone()
                }
            }
            FileType::Columnar => Self::Ia,
            FileType::VectorIndex => Self::Ia,
            FileType::FtsPackedFile => Self::Ia,
            FileType::FtsDedicatedFile => Self::Ia,
            _ => Self::Local,
        }
    }
}

enum PreparedFileResult {
    Local {
        local_data: Bytes,
    },
    Ia {
        table_meta_data: Bytes,
    },
    AutoIa {
        table_meta_file: Arc<dyn File>,
        local_data: Option<Bytes>,
        max_ts: u64,
        spec: StorageClassSpec,
    },
}

struct LoadRemoteResult {
    id: u64,
    fm: FileMeta,
    prepared: PreparedFileResult,
    permit: DfsLoadLimiterPermit,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_tmp_file_path() {
        for p in ["/var/lib/tikv/db/100.sst", "/var/lib/tikv/db/100"] {
            let file_path = PathBuf::from(p);
            let tmp_file_path = EngineCore::tmp_file_path(&file_path);
            let path_str = tmp_file_path.to_str().unwrap();

            let segs = path_str.split('.').collect::<Vec<_>>();
            assert_eq!(segs.len(), 3);
            assert_eq!(segs[0], "/var/lib/tikv/db/100");
            u64::from_str(segs[1]).unwrap();
            assert_eq!(segs[2], "tmp");
        }
    }
}
