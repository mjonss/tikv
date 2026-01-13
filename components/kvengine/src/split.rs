// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::{max, min},
    collections::HashMap,
    iter::Iterator,
    sync::{
        Arc,
        atomic::{Ordering, Ordering::Release},
    },
};

use api_version::{
    ApiV2, KeyMode, KvFormat,
    api_v2::{KEYSPACE_PREFIX_LEN, is_whole_keyspace_range},
};
use bytes::{Buf, Bytes};
use collections::HashSet;
use kvenginepb as pb;
use schema::schema::StorageClassSpec;
use slog_global::info;

use crate::{
    table::{
        BoundedDataSet, DataBound, InnerKey, SnapVersion, columnar::ColumnarLevels,
        vector_index::VectorIndexes,
    },
    table_id::{get_table_id_from_data_bound, keys_belong_to_same_table, merge_columnar_table_ids},
    *,
};

#[derive(Debug)]
pub struct CheckMergeResult {
    pub source_overbound: bool,
    pub target_overbound: bool,

    /// source/target region is required to be empty, but it's not.
    pub source_require_empty: bool,
    pub target_require_empty: bool,

    // source and target shards belong to same keyspace but with different encryption key.
    pub inconsistent_encryption_key: bool,
    // source and target shards belong to same keyspace but with different storage class spec.
    pub inconsistent_storage_class_spec: bool,
}

impl Engine {
    pub fn split(&self, mut cs: pb::ChangeSet, initial_seq: u64) -> Result<()> {
        let split = cs.take_split();
        let sequence = cs.get_sequence();

        let old_shard = self.get_shard_with_ver(cs.shard_id, cs.shard_ver)?;
        self.prepare_update_shard_version(&old_shard, sequence, true);

        let mut new_shards = vec![];
        let new_shard_props = split.get_new_shards();
        let new_ver = old_shard.ver + new_shard_props.len() as u64 - 1;
        let old_data = old_shard.get_data();
        assert!(
            !old_data.has_txn_file_locks(),
            "{} shard with txn files are not allowed to split, lock_txn_files {:?}",
            old_shard.tag(),
            old_data.lock_txn_files
        );
        let old_del_prefixes = old_shard.pending_ops.read().unwrap().del_prefixes.clone();
        let sc_spec = old_shard.get_property(STORAGE_CLASS_KEY);
        for i in 0..=split.keys.len() {
            let (start_key, end_key) = get_splitting_start_end(
                old_shard.outer_start.chunk(),
                old_shard.outer_end.chunk(),
                split.get_keys(),
                i,
            );
            let (range, inner_key_off) = if is_whole_keyspace_range(start_key, end_key) {
                (ShardRange::new(start_key, end_key), KEYSPACE_PREFIX_LEN)
            } else {
                debug!(
                    "engine split";
                    "shard_id" => old_shard.id,
                    "start_key" => format!("{:?}", start_key),
                    "end_key" => format!("{:?}", end_key),
                    "inner_key_off" => old_data.inner_key_off
                );
                (ShardRange::new(start_key, end_key), old_data.inner_key_off)
            };
            // Note: properties of new shards are processed in `build_split_pb`.
            let mut new_shard = Shard::new(
                self.get_engine_id(),
                &new_shard_props[i],
                new_ver,
                range,
                inner_key_off,
                self.opts.clone(),
                &self.master_key,
            );
            new_shard.parent_id = old_shard.id;
            if new_shard.id == old_shard.id {
                new_shard.set_active(old_shard.is_active());
                store_u64(&new_shard.base_version, old_shard.get_base_version());
                store_u64(&new_shard.meta_seq, sequence);
                store_u64(&new_shard.write_sequence, sequence);
            } else {
                store_u64(
                    &new_shard.base_version,
                    old_shard.get_base_version() + sequence,
                );
                store_u64(&new_shard.meta_seq, initial_seq);
                store_u64(&new_shard.write_sequence, initial_seq);
            }
            if !old_del_prefixes.is_empty() {
                // We need to use the old shard's DEL_PREFIXES_KEY to overwrite the new shard's
                // DEL_PREFIXES_KEY. because the destroy_range compaction may have not
                // applied to the shard.
                let new_del_prefixes = old_del_prefixes.build_split(start_key, end_key);
                if !new_del_prefixes.is_empty() {
                    new_shard.set_property(DEL_PREFIXES_KEY, &new_del_prefixes.marshal());
                }
            }
            new_shard.set_property_opt(STORAGE_CLASS_KEY, sc_spec.as_deref());
            new_shards.push(Arc::new(new_shard));
        }
        let unconverted_l0s: HashSet<u64> = old_data
            .col_levels
            .unconverted_l0s
            .iter()
            .map(|l0| l0.id())
            .collect();
        for new_shard in &new_shards {
            if is_whole_keyspace_range(
                new_shard.range.outer_start.chunk(),
                new_shard.range.outer_end.chunk(),
            ) {
                // The newly split keyspace should be empty.
                continue;
            }
            let (min_table_id, max_table_id) = get_table_id_from_data_bound(new_shard.data_bound());
            let columnar_table_ids: Vec<_> = old_data
                .columnar_table_ids
                .iter()
                .filter(|&&table_id| table_id >= min_table_id && table_id <= max_table_id)
                .copied()
                .collect();
            let new_mem_tbls = new_shard.split_mem_tables(&old_data.mem_tbls);
            let mut new_l0s = vec![];
            let mut new_unconverted_l0s = vec![];
            for l0 in &old_data.l0_tbls {
                if new_shard.range.data_bound().overlap_bound(l0.data_bound()) {
                    new_l0s.push(l0.clone());
                    if unconverted_l0s.contains(&l0.id()) && !columnar_table_ids.is_empty() {
                        new_unconverted_l0s.push(l0.clone());
                    }
                }
            }
            let mut new_blob_tbl_map = HashMap::new();
            for blob_tbl in old_data.blob_tbl_map.values() {
                if new_shard.overlap_bound(blob_tbl.data_bound()) {
                    new_blob_tbl_map.insert(blob_tbl.id(), blob_tbl.clone());
                }
            }
            let mut new_cfs = [ShardCf::new(0), ShardCf::new(1), ShardCf::new(2)];
            for cf in 0..NUM_CFS {
                let old_scf = old_data.get_cf(cf);
                for lh in &old_scf.levels {
                    let mut new_level_tbls = vec![];
                    for tbl in lh.tables.as_slice() {
                        if new_shard.overlap_bound(tbl.data_bound()) {
                            new_level_tbls.push(tbl.clone());
                        }
                    }
                    let new_level = LevelHandler::new(lh.level, new_level_tbls);
                    new_cfs[cf].set_level(new_level);
                }
            }
            let schema_version = old_data.schema_version;
            let restore_version = old_data.restore_version;
            let schema_file = old_data.schema_file.clone();
            let mut new_col_levels = ColumnarLevels::new();
            new_col_levels.unconverted_l0s = new_unconverted_l0s;
            for col_level in &old_data.col_levels.levels {
                let new_col_level = &mut new_col_levels.levels[col_level.level];
                for col_file in &col_level.files {
                    if new_shard.overlap_bound(col_file.data_bound())
                        && !columnar_table_ids.is_empty()
                    {
                        new_col_level.files.push(col_file.clone());
                    }
                }
            }
            new_col_levels.l2_snap_version = old_data.col_levels.l2_snap_version;

            let mut new_vec_indexes = VectorIndexes::default();
            for vec_index in old_data.vector_indexes.get_all() {
                if !columnar_table_ids.contains(&vec_index.table_id) {
                    continue;
                }
                for file in &vec_index.files {
                    if new_shard.overlap_bound(file.data_bound()) {
                        new_vec_indexes.add_index_file(file.clone());
                    }
                }
                new_vec_indexes.update_snap_version(
                    vec_index.table_id,
                    vec_index.index_id,
                    vec_index.col_id,
                    vec_index.snap_version,
                );
            }
            let mut builder = ShardDataBuilder::new(new_shard.get_data());
            builder.set_mem_tbls(new_mem_tbls);
            builder.set_l0_tbls(new_l0s);
            builder.set_blob_tbls(new_blob_tbl_map);
            builder.set_cfs(new_cfs);
            builder.set_unloaded_tbls(old_data.unloaded_tbls.clone());
            builder.with_new_limiter();
            builder.set_schema(schema_version, restore_version, schema_file);
            builder.set_columnar_levels(new_col_levels);
            builder.set_vector_indexes(new_vec_indexes);
            builder.set_columnar_table_ids(columnar_table_ids);
            builder.set_persisted_version(old_data.persisted_version);
            new_shard.set_data(builder.build());
        }
        for shard in new_shards.drain(..) {
            let id = shard.id;
            if id != old_shard.id {
                self.insert_keyspace_shard(shard.keyspace_id, id);
                let shards = self.shards.pin();
                // If the shard already exists, it must be created by ingest, and it maybe
                // newer than this one, we avoid insert it.
                let _ = shards.try_insert(id, shard.clone());
            } else {
                let shards = self.shards.pin();
                shards.insert(id, shard.clone());
            }
            self.refresh_shard_states(&shard);
            let all_files = shard.get_all_files();
            let all_col_files = shard.get_all_col_files();
            info!(
                "split new shard {}, start {:x}, end {:x}, all files {:?} (columnar {:?})",
                shard.tag(),
                shard.outer_start,
                shard.outer_end,
                all_files,
                all_col_files,
            );
        }
        Ok(())
    }

    // `force_switch_mem_table` must be set `true` when & only when the shard need
    // initial flush, i.e. during split/prepare-merge/commit-merge.
    // See https://github.com/tidbcloud/cloud-storage-engine/issues/1557.
    pub(crate) fn prepare_update_shard_version(
        &self,
        shard: &Shard,
        sequence: u64,
        force_switch_mem_table: bool,
    ) {
        shard.write_sequence.store(sequence, Release);
        let version = shard.load_mem_table_snap_version();
        // Switch the old shard mem-table, so the first mem-table is always empty.
        // ignore the read-only mem-table to be flushed. let the new shard handle it.
        self.switch_mem_table(shard, version, force_switch_mem_table, sequence);
        self.send_flush_msg(FlushMsg::Clear(shard.id));
        self.send_compact_msg(CompactMsg::Clear(IdVer::new(shard.id, shard.ver)));
    }

    pub fn check_merge(
        &self,
        source_id: u64,
        source_ver: u64,
        target_id: u64,
        target_ver: u64,
    ) -> Result<CheckMergeResult> {
        let source_shard = self.get_shard_with_ver(source_id, source_ver)?;
        if !source_shard.get_initial_flushed() {
            return Err(Error::CheckMerge("source not initial flushed".to_string()));
        }
        if source_shard.has_txn_file_locks() {
            return Err(Error::CheckMerge("source has txn file locks".to_string()));
        }
        let source_sc_spec =
            StorageClassSpec::unmarshal(source_shard.get_property(STORAGE_CLASS_KEY).as_deref());

        let target_shard = self.get_shard_with_ver(target_id, target_ver)?;
        if !target_shard.get_initial_flushed() {
            return Err(Error::CheckMerge("target not initial flushed".to_string()));
        }
        let target_sc_spec =
            StorageClassSpec::unmarshal(target_shard.get_property(STORAGE_CLASS_KEY).as_deref());

        let belongs_to_same_keyspace = ApiV2::is_belongs_to_same_keyspace(
            &source_shard.outer_start,
            &target_shard.outer_start,
        );
        // Check if the source and target shards belong to the same keyspace but have
        // different encryption key property. This situation might occur during
        // keyspace restoration. In such cases, merging should be avoided.
        // If the source and target shards are from different keyspaces, the encryption
        // key property will be removed in `commit_merge`.
        let inconsistent_encryption_key = belongs_to_same_keyspace
            && source_shard
                .encryption_key
                .as_ref()
                .map(|k| k.cipher_text.clone())
                != target_shard
                    .encryption_key
                    .as_ref()
                    .map(|k| k.cipher_text.clone());

        let inconsistent_sc_spec = if belongs_to_same_keyspace {
            if !source_sc_spec.is_specified() && !target_sc_spec.is_specified() {
                false
            } else {
                source_sc_spec != target_sc_spec
                    || !keys_belong_to_same_table(
                        source_shard.inner_start(),
                        target_shard.inner_start(),
                    )
            }
        } else {
            source_sc_spec.is_specified() || target_sc_spec.is_specified()
        };

        let (clear_source, clear_target) =
            need_clear_region_data_on_merge(&source_shard.outer_start, &target_shard.outer_start);
        let shard_is_empty = |shard: &Shard| -> bool {
            shard.is_empty()
                || shard.get_del_prefixes().cover_full_keyspace(
                    ApiV2::get_keyspace_prefix(&shard.outer_start).unwrap_or_default(),
                )
        };
        let source_require_empty = clear_source && !shard_is_empty(source_shard.as_ref());
        let target_require_empty = clear_target && !shard_is_empty(target_shard.as_ref());

        Ok(CheckMergeResult {
            source_overbound: source_shard.has_over_bound_data(),
            target_overbound: target_shard.has_over_bound_data(),
            source_require_empty,
            target_require_empty,
            inconsistent_encryption_key,
            inconsistent_storage_class_spec: inconsistent_sc_spec,
        })
    }

    pub fn prepare_merge(&self, shard_id: u64, shard_ver: u64, sequence: u64) -> Result<()> {
        let old_shard = self.get_shard_with_ver(shard_id, shard_ver).unwrap();
        let old_data = old_shard.get_data();

        if old_data.has_txn_file_locks() {
            warn!("{} prepare_merge denied, shard has txn file locks", old_shard.tag();
                "txn_file_locks" => ?old_data.lock_txn_files);
            return Err(Error::Other(
                MERGE_REGION_WITH_TXN_FILE_LOCKS_ERR_MSG.into(),
            ));
        }

        if !old_data.col_levels.unconverted_l0s.is_empty() {
            warn!(
                "{} prepare_merge denied, unconverted l0s : {:?}",
                old_shard.tag(),
                old_data
                    .col_levels
                    .unconverted_l0s
                    .iter()
                    .map(|l0| l0.id())
                    .collect::<Vec<_>>()
            );
            return Err(Error::Other(
                MERGE_REGION_WITH_UNCONVERTED_L0S_ERR_MSG.into(),
            ));
        }

        self.prepare_update_shard_version(&old_shard, sequence, true);
        let mut new_shard = self.new_shard_version(&old_shard, sequence);
        // source shard may have non-empty mem-table, we need to flush them before
        // commit merge. The initial_flushed of the new shard is false, set the
        // parent for later initial flush.
        new_shard.parent_id = old_shard.id;
        info!("{} shard prepared merge", new_shard.tag());
        self.insert_shard(Arc::new(new_shard));
        Ok(())
    }

    pub fn rollback_merge(&self, shard_id: u64, shard_ver: u64, sequence: u64) {
        let old_shard = self.get_shard_with_ver(shard_id, shard_ver).unwrap();
        self.prepare_update_shard_version(&old_shard, sequence, false);
        let new_shard = self.new_shard_version(&old_shard, sequence);
        // There is no write during merging state, so we can directly set
        // initial_flushed to true.
        new_shard.initial_flushed.store(true, Ordering::Release);
        info!("{} shard rollback merge", new_shard.tag());
        self.insert_shard_and_refresh(Arc::new(new_shard));
    }

    pub fn commit_merge(
        &self,
        shard_id: u64,
        shard_ver: u64,
        source: &ChangeSet,
        sequence: u64,
    ) -> Result<()> {
        let old_shard = self.get_shard_with_ver(shard_id, shard_ver)?;
        let inner_key_off = old_shard.get_data().inner_key_off;
        self.prepare_update_shard_version(&old_shard, sequence, true);
        let source_snap = source.get_snapshot();

        let belongs_to_same_keyspace =
            ApiV2::is_belongs_to_same_keyspace(&source_snap.outer_start, &old_shard.outer_start);
        if !belongs_to_same_keyspace {
            old_shard.del_property(ENCRYPTION_KEY);
        }

        let (clear_source, clear_target) =
            need_clear_region_data_on_merge(&source_snap.outer_start, &old_shard.outer_start);

        if clear_target {
            info!(
                "{} clear data of target shard on merge, target: {:?}",
                old_shard.tag(),
                old_shard.range,
            );
            old_shard.set_property(DEL_PREFIXES_KEY, &[]);
            let source_sc = get_shard_property(STORAGE_CLASS_KEY, source_snap.get_properties());
            old_shard.set_property(STORAGE_CLASS_KEY, source_sc.as_deref().unwrap_or_default());
            old_shard.set_data_opt(
                ShardData::new_empty(
                    old_shard.range.clone(),
                    inner_key_off,
                    old_shard.get_data().limiter.clone(),
                ),
                false,
            );
        }
        let mut new_shard = self.new_shard_version(&old_shard, sequence);

        // TODO: Do we need to merge pending operations here?
        new_shard.range.outer_start = min(
            old_shard.outer_start.clone(),
            source_snap.outer_start.clone().into(),
        );
        new_shard.range.outer_end = max(
            old_shard.outer_end.clone(),
            source_snap.outer_end.clone().into(),
        );
        new_shard.ver = max(shard_ver, source.shard_ver) + 1;
        // make sure the new mem-table version is greater than source.
        let source_mem_tbl_version = source_snap.base_version + source.sequence;
        let target_mem_tbl_version = old_shard.get_base_version() + sequence;
        store_u64(
            &new_shard.base_version,
            max(source_mem_tbl_version, target_mem_tbl_version) - sequence,
        );

        let data = if !clear_source {
            // merge source DEL_PREFIXES_KEY to new shard
            let source_del_prefixes =
                get_shard_property(DEL_PREFIXES_KEY, source_snap.get_properties())
                    .map(|v| Bytes::from(v));
            let old_del_prefixes = old_shard.get_property(DEL_PREFIXES_KEY);
            if let Some(new_del_prefixes) = merge_del_prefixes_if_needed(
                source_del_prefixes,
                old_del_prefixes,
                new_shard.keyspace_id,
            ) {
                new_shard.set_property(DEL_PREFIXES_KEY, &new_del_prefixes);
            }

            let source_sc_spec = StorageClassSpec::unmarshal(
                get_shard_property(STORAGE_CLASS_KEY, source_snap.get_properties()).as_deref(),
            );
            let target_sc_spec = old_shard.get_storage_class_spec();
            if source_sc_spec != target_sc_spec {
                warn!("{} commit merge: storage class mismatch", old_shard.tag();
                    "source" => ?source, "target" => ?target_sc_spec);

                // Happen when source & target shards come from different
                // version (during upgrade).
                // TODO: uncomment the following assert after upgrade.
                // debug_assert!(false);
            }

            // merge shard data
            let old_data = old_shard.get_data();
            let source_snap_version = SnapVersion::new(
                source_snap.get_base_version(),
                source_snap.get_data_sequence(),
            );
            let new_snap_version = max(old_data.persisted_version, source_snap_version);
            let mem_tbls = old_data.mem_tbls.clone();
            let mut blob_tbl_map = old_data.blob_tbl_map.as_ref().clone();
            for v in source.blob_tables.values() {
                blob_tbl_map.insert(v.id(), v.clone());
            }
            let mut l0_tbls = old_data.l0_tbls.clone();
            for l0 in source.l0_tables.values() {
                l0_tbls.push(l0.clone())
            }
            l0_tbls.sort_by(|a, b| b.snap_version().cmp(&a.snap_version()));
            let mut new_cf_builders = [
                ShardCfBuilder::new(0),
                ShardCfBuilder::new(1),
                ShardCfBuilder::new(2),
            ];
            for cf in 0..NUM_CFS {
                let old_scf = old_data.get_cf(cf);
                for level in 1..=CF_LEVELS[cf] {
                    let old_level = old_scf.get_level(level);
                    let cf_builder = &mut new_cf_builders[cf];
                    for tbl in old_level.tables.as_slice() {
                        cf_builder.add_table(tbl.clone(), level);
                    }
                }
            }
            for tbl_create in source_snap.get_table_creates() {
                let tbl = source.ln_tables.get(&tbl_create.id).unwrap().clone();
                let cf_builder = &mut new_cf_builders[tbl_create.cf as usize];
                cf_builder.add_table(tbl, tbl_create.level as usize);
            }
            let new_cfs = [
                new_cf_builders[0].build(),
                new_cf_builders[1].build(),
                new_cf_builders[2].build(),
            ];
            let mut unloaded_tbls = source.unloaded_tables.clone();
            for (&id, tbl) in old_data.unloaded_tbls.iter() {
                unloaded_tbls.insert(id, tbl.clone());
            }
            let lock_txn_files = old_data.lock_txn_files.clone();
            assert!(
                source.lock_txn_files.is_empty(),
                "{} source shard with txn file locks are not allowed to be merged, source {:?}",
                old_shard.tag(),
                source
            );
            let mut columnar_levels = old_data.col_levels.clone();
            for columnar_create in source_snap.get_columnar_creates() {
                let col = source.col_files.get(&columnar_create.id).unwrap().clone();
                columnar_levels.add_file(columnar_create.level as usize, col);
            }
            columnar_levels.l2_snap_version = max(
                old_data.col_levels.l2_snap_version,
                source_snap.columnar_l2_snap_version.into(),
            );
            columnar_levels.sort();
            let mut vector_indexes = old_data.vector_indexes.clone();
            for source_vec_index in source_snap.get_vector_indexes() {
                for source_vec_idx_file_id in source_vec_index.files.iter().map(|f| f.id) {
                    let vec_idx_file = source
                        .vec_index_files
                        .get(&source_vec_idx_file_id)
                        .unwrap()
                        .clone();
                    vector_indexes.add_index_file(vec_idx_file);
                }
                let old_snap_version = vector_indexes
                    .get(
                        source_vec_index.table_id,
                        source_vec_index.index_id,
                        source_vec_index.col_id,
                    )
                    .map(|v| v.snap_version)
                    .unwrap_or(SnapVersion::zero());
                vector_indexes.update_snap_version(
                    source_vec_index.table_id,
                    source_vec_index.index_id,
                    source_vec_index.col_id,
                    std::cmp::max(old_snap_version, source_vec_index.snap_version.into()),
                );
            }
            let mut schema_version = old_data.schema_version;
            let mut restore_version = old_data.restore_version;
            let mut schema_file = old_data.schema_file.clone();
            // If the target shard has no schema file and the source has schema file, we
            // should merge the schema file to target.
            if schema_file.is_none() && source.schema_file.is_some() {
                schema_version = source.schema_file.as_ref().unwrap().get_version();
                restore_version = source.schema_file.as_ref().unwrap().get_restore_version();
                schema_file = source.schema_file.clone();
            }
            let source_data_bound = DataBound::new(
                InnerKey::from_outer_key(source_snap.get_outer_start()),
                InnerKey::from_outer_key(source_snap.get_outer_end()),
                false,
            );
            let columnar_table_ids = merge_columnar_table_ids(
                &source_snap.columnar_table_ids,
                &old_data.columnar_table_ids,
                source_data_bound,
                old_data.data_bound(),
            );
            retain_columnar_and_vector(
                &columnar_table_ids,
                &mut columnar_levels,
                &mut vector_indexes,
            );
            let mut builder = ShardDataBuilder::new(old_data);
            builder.set_range(new_shard.range.clone());
            if clear_target {
                // `inner_key_off` will be different when merge regions of different keyspaces.
                builder.set_inner_key_off(source_snap.inner_key_off as usize);
            }
            builder.set_mem_tbls(mem_tbls);
            builder.set_l0_tbls(l0_tbls);
            builder.set_blob_tbls(blob_tbl_map);
            builder.set_cfs(new_cfs);
            builder.set_unloaded_tbls(unloaded_tbls);
            builder.set_lock_txn_files(lock_txn_files);
            builder.set_columnar_levels(columnar_levels);
            builder.set_schema(schema_version, restore_version, schema_file);
            builder.set_vector_indexes(vector_indexes);
            builder.set_columnar_table_ids(columnar_table_ids);
            builder.set_persisted_version(new_snap_version);
            builder.build()
        } else {
            info!(
                "{} clear data of source shard on merge, source start: {}, end: {}",
                old_shard.tag(),
                log_wrappers::hex_encode_upper(&source_snap.outer_start),
                &log_wrappers::hex_encode_upper(&source_snap.outer_end),
            );

            let old_data = old_shard.get_data();
            let mut builder = ShardDataBuilder::new(old_data);
            builder.set_range(new_shard.range.clone());
            builder.build()
        };
        new_shard.set_data(data);
        debug_assert_eq!(new_shard.range, new_shard.get_data().range);

        new_shard.parent_id = shard_id;
        let all_files = new_shard.get_all_files();
        let all_col_files = new_shard.get_all_col_files();
        info!(
            "merged new shard {}, start {:x}, end {:x}, all files {:?} (columnar {:?})",
            new_shard.tag(),
            new_shard.outer_start,
            new_shard.outer_end,
            all_files,
            all_col_files,
        );
        self.insert_shard_and_refresh(Arc::new(new_shard));
        Ok(())
    }

    pub(crate) fn new_shard_version(&self, old_shard: &Shard, sequence: u64) -> Shard {
        let engine_id = self.get_engine_id();
        let inner_key_off = old_shard.get_data().inner_key_off;
        let new_shard = Shard::new(
            engine_id,
            &old_shard.properties.to_pb(old_shard.id),
            old_shard.ver + 1,
            old_shard.range.clone(),
            inner_key_off,
            old_shard.opt.clone(),
            &self.master_key,
        );
        new_shard.set_data_opt(old_shard.get_data(), false);
        new_shard.set_active(old_shard.is_active());
        store_u64(&new_shard.base_version, old_shard.get_base_version());
        store_u64(&new_shard.meta_seq, sequence);
        store_u64(&new_shard.write_sequence, sequence);
        store_u64(&new_shard.estimated_size, old_shard.get_estimated_size());
        store_u64(
            &new_shard.estimated_entries,
            old_shard.get_estimated_entries(),
        );
        store_u64(&new_shard.max_ts, old_shard.get_max_ts());
        store_u64(
            &new_shard.estimated_kv_size,
            old_shard.get_estimated_kv_size(),
        );
        store_u64(
            &new_shard.estimated_ia_kv_size,
            old_shard.get_estimated_ia_kv_size(),
        );
        new_shard
    }
}

fn retain_columnar_and_vector(
    columnar_table_ids: &[i64],
    col_levels: &mut ColumnarLevels,
    vector_indexes: &mut VectorIndexes,
) {
    col_levels.retain(|col| {
        let data_bound = col.data_bound();
        let (min_table_id, max_table_id) = get_table_id_from_data_bound(data_bound);
        columnar_table_ids
            .iter()
            .any(|&id| id >= min_table_id && id <= max_table_id)
    });
    if columnar_table_ids.is_empty() {
        col_levels.unconverted_l0s.clear();
    }
    vector_indexes.retain(|idx| columnar_table_ids.contains(&idx.table_id));
}

pub fn get_split_shard_index(split_keys: &[Vec<u8>], key: &[u8]) -> usize {
    for i in 0..split_keys.len() {
        if key < split_keys[i].as_slice() {
            return i;
        }
    }
    split_keys.len()
}

/// Whether to clear the data of source and/or target regions on merge.
///
/// Merging regions of different keyspaces only happens when the keyspace(s)
/// has been deleted. The validity of which was ensured by PD.
///
/// In this condition, we must clear the data of region in keyspace.
///
/// Otherwise, as SSTs in keyspace do not have prefix (when enable key
/// offset), the merged SSTs will violate data correctness.
pub fn need_clear_region_data_on_merge(
    source_outer_start: &[u8],
    target_outer_start: &[u8],
) -> (bool /* clear_source */, bool /* clear_target */) {
    let is_same_keyspace_merge =
        ApiV2::is_belongs_to_same_keyspace(source_outer_start, target_outer_start);
    let clear_region = |key: &[u8]| -> bool {
        let key_mode = ApiV2::parse_key_mode(key);
        let in_keyspace = key_mode == KeyMode::Txn || key_mode == KeyMode::Raw;
        !is_same_keyspace_merge && in_keyspace
    };
    (
        clear_region(source_outer_start),
        clear_region(target_outer_start),
    )
}

#[cfg(test)]
mod tests {
    use std::iter::Iterator;

    use super::*;

    #[test]
    fn test_need_clear_region_data_on_merge() {
        let cases: Vec<(&[u8], &[u8], (bool, bool))> = vec![
            // Txn
            (
                b"x0000",       // source_outer_start
                b"x0001",       // target_outer_start
                (false, false), // (clear_source, clear_target)
            ),
            (b"x0000", b"x0010", (true, true)),
            // Raw
            (b"r0000", b"r0001", (false, false)),
            (b"r0000", b"r0010", (false, false)),
            // Txn & Raw
            (b"x0000", b"r0000", (true, false)),
            // Txn & TiDB/Unknown
            (b"x0000", b"t", (true, false)),
            (b"t", b"x0000", (false, true)),
            (b"x0000", b"m", (true, false)),
            (b"m", b"x0000", (false, true)),
            (b"x0000", b"", (true, false)),
            (b"", b"x0000", (false, true)),
            // TiDB/Unknown
            (b"t", b"m", (false, false)),
            (b"m", b"", (false, false)),
            (b"", b"t", (false, false)),
        ];

        for (idx, (source_outer_start, target_outer_start, expected)) in
            cases.into_iter().enumerate()
        {
            let res = need_clear_region_data_on_merge(source_outer_start, target_outer_start);
            assert_eq!(res, expected, "case {}", idx);
        }
    }
}
