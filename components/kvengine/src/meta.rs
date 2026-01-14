// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    fmt,
    iter::Iterator,
};

use api_version::{
    ApiV2,
    api_v2::{KEYSPACE_PREFIX_LEN, is_whole_keyspace_range},
};
use bytes::{Buf, Bytes};
use kvenginepb as pb;
use kvenginepb::{
    SchemaMeta, TxnFileRef, VectorIndex, fts::TableIndexId, get_any_snap_from_changeset,
};
use protobuf::Message;
use schema::schema::StorageClassSpec;
use slog_global::*;
use util::TxnFileRefExt as _;

use super::*;
use crate::{
    dfs::FileType,
    table::{BoundedDataSet, DataBound, InnerKey, SnapVersion},
    table_id::{
        get_table_id_from_data_bound, get_table_id_from_ingest_files, merge_columnar_table_ids,
    },
    util::{TxnFileLocks, TxnFileRefPropertyHelper, is_matched_vector_index, is_same_vector_index},
};

#[derive(Default, Clone, Debug)]
pub struct ShardMeta {
    pub engine_id: u64,
    pub id: u64,
    pub ver: u64,
    pub range: ShardRange,
    pub inner_key_off: usize,

    /// `seq` is the raft log index of the applied change set.
    ///
    /// Should be updated and ONLY be updated when LSM is changed.
    pub seq: u64,
    pub(crate) files: HashMap<u64, FileMeta>,

    pub(crate) properties: Properties,
    pub base_version: u64,

    /// `data_sequence` is the raft log index of data included in the latest L0
    /// file.
    ///
    /// Note: if meta is not initial flushed, the data persisted log index
    /// should be got from parent. Use `ShardMeta::data_persisted_log_index`.
    pub data_sequence: u64,

    /// `max_ts` is the max ts in all sst files.
    pub max_ts: u64,
    pub parent: Option<Box<ShardMeta>>,
    pub schema: SchemaFileMeta,
    pub columnar_table_ids: Vec<i64>,
    pub unconverted_l0s: Vec<u64>,
    pub vector_indexes: Vec<kvenginepb::VectorIndex>,
    pub columnar_l2_snap_version: SnapVersion,

    // The followings are FTS related fields ============
    pub fts_l0_snap_version: SnapVersion,
    /// Currently tracked FTS indexes. We use this list to determine whether
    /// it is incremental update or index add.
    pub fts_indexes: HashSet<(i64 /* table_id */, i64 /* index_id */)>,
    /// FTS Level 0 files (order is not guaranteed).
    pub fts_l0_files: Vec<kvenginepb::FtsPackedFileInfo>,
    /// FTS Level 1 packed partitions (order is not guaranteed).
    pub fts_l1_files: Vec<kvenginepb::FtsPackedFileInfo>,
    /// FTS Level 2 dedicated partitions grouped by logical partition.
    pub fts_l2_files: Vec<kvenginepb::FtsDedFileInfo>,
    /// Columnar L0 ids that still need incremental FTS processing after merge.
    pub fts_pending_l0_ids: Vec<u64>,

    /// The following are memory-based field(s).
    ///
    /// Recover process:
    ///
    /// * On restart: recover from shard after kvengine is recovered. See
    ///   `ShardMeta::recover_txn_file_locks_from_kv`.
    ///
    /// * On restore from snapshot: replay raft logs from snapshot index
    ///   (instead of meta.seq). See `PeerStorage::restore_snapshot`.
    ///
    /// * On restore from backup: replay raft logs from applied index (instead
    ///   of preprocessed index). See `BackupCluster::preprocess_shard`.
    pub(crate) txn_file_locks: TxnFileLocks,
}

impl fmt::Display for ShardMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardMeta")
            .field("tag", &self.tag())
            .field("range", &self.range)
            .field("inner_key_off", &self.inner_key_off)
            .field("seq", &self.seq)
            .field("files", &self.files.len())
            .field("properties", &self.properties)
            .field("base_ver", &self.base_version)
            .field("data_seq", &self.data_sequence)
            .field("max_ts", &self.max_ts)
            .field("parent", &self.parent.is_some())
            .field("schema", &self.schema)
            .field("columnar_table_ids", &self.columnar_table_ids.len())
            .field("unconverted_l0s", &self.unconverted_l0s.len())
            .field("vector_indexes", &self.vector_indexes.len())
            .field("columnar_l2_snap_version", &self.columnar_l2_snap_version)
            .field("txn_file_locks", &self.txn_file_locks.len())
            .finish()
    }
}

impl ShardMeta {
    pub fn new(engine_id: u64, cs: &pb::ChangeSet) -> Self {
        assert!(cs.has_snapshot() || cs.has_initial_flush() || cs.has_restore_shard());
        let snap = get_any_snap_from_changeset(cs).unwrap();
        let properties = Properties::new().apply_pb(snap.get_properties());

        // To fix the issue that `MANUAL_MAJOR_COMPACTION` is not removed after apply
        // major compaction.
        // TODO: remove after next upgrade.
        properties.remove(MANUAL_MAJOR_COMPACTION);

        let txn_file_locks = TxnFileRefPropertyHelper::from_property(properties.get(TXN_FILE_REF))
            .unwrap()
            .into_txn_file_locks(snap.data_sequence);

        let mut meta = Self {
            engine_id,
            id: cs.shard_id,
            ver: cs.shard_ver,
            range: ShardRange::from_snap(snap),
            inner_key_off: snap.inner_key_off as usize,
            seq: cs.sequence,
            properties,
            base_version: snap.base_version,
            data_sequence: snap.data_sequence,
            max_ts: snap.max_ts,
            columnar_table_ids: snap.columnar_table_ids.clone(),
            unconverted_l0s: snap.unconverted_l0s.clone(),
            columnar_l2_snap_version: snap.columnar_l2_snap_version.into(),
            txn_file_locks,
            ..Default::default()
        };
        for l0 in snap.get_l0_creates() {
            meta.add_file(l0.id, FileMeta::from_l0_table(l0));
        }
        for blob in snap.get_blob_creates() {
            meta.add_file(blob.id, FileMeta::from_blob_table(blob));
        }
        for tbl in snap.get_table_creates() {
            meta.add_file(tbl.id, FileMeta::from_table(tbl));
        }
        for col in snap.get_columnar_creates() {
            meta.add_file(col.get_id(), FileMeta::from_columnar_table(col));
        }
        if cs.has_parent() {
            let parent_meta = Box::new(Self::new(engine_id, cs.get_parent()));
            meta.parent = Some(parent_meta);
        }
        if snap.has_schema_meta() {
            meta.schema = SchemaFileMeta::from_snapshot(snap);
        }
        for vec_idx in snap.get_vector_indexes() {
            meta.vector_indexes.push(vec_idx.clone());
            for vec_idx_file in vec_idx.get_files() {
                meta.add_file(
                    vec_idx_file.id,
                    FileMeta::from_vector_index_file(vec_idx_file),
                );
            }
        }
        for tbl_idx_id in snap.get_fts_indexes() {
            meta.fts_indexes
                .insert((tbl_idx_id.table_id, tbl_idx_id.index_id));
        }
        for fts_l0_file in snap.get_fts_l0_files() {
            meta.fts_l0_files.push(fts_l0_file.clone());
        }
        for fts_l1_file in snap.get_fts_l1_files() {
            meta.fts_l1_files.push(fts_l1_file.clone());
        }
        for fts_l2_file in snap.get_fts_l2_files() {
            meta.fts_l2_files.push(fts_l2_file.clone());
        }
        meta.fts_pending_l0_ids = snap.get_fts_pending_l0_ids().to_vec();
        meta.fts_l0_snap_version = SnapVersion::from(snap.get_fts_l0_snap_version());
        meta
    }

    pub fn recover_txn_file_locks_from_kv(&mut self, kv: &Engine) {
        match kv.get_shard_with_ver(self.id, self.ver) {
            Ok(shard) => {
                self.recover_txn_file_locks_from_shard(&shard);
            }
            Err(err) => {
                warn!("{} recover_txn_file_locks_from_kv: failed to get shard", self.tag(); "err" => ?err);
            }
        }
    }

    pub fn fix_table_meta_offset(&mut self, kv: &Engine) {
        let tag = self.tag();
        let mut to_fix_files: HashMap<&u64, &mut FileMeta> = self
            .files
            .iter_mut()
            .filter(|(_, fm)| {
                // Columnar files must have `table_meta_off` > 0.
                fm.is_sst_file() && fm.can_use_ia() && fm.table_meta_off == 0
            })
            .collect();
        if to_fix_files.is_empty() {
            return;
        }

        info!("{} fix_table_meta_offset", tag; "files" => ?to_fix_files);
        match kv.get_shard_with_ver(self.id, self.ver) {
            Ok(shard) => {
                let data = shard.get_data();
                data.for_each_level(|_cf, lh| {
                    for t in lh.tables.iter() {
                        if let Some(fm) = to_fix_files.remove(&t.id()) {
                            fm.table_meta_off = t.meta_offset();
                        }
                    }
                    to_fix_files.is_empty()
                });
                if !to_fix_files.is_empty() {
                    warn!("{} fix_table_meta_offset: failed to fix table meta offset", tag;
                        "files" => ?to_fix_files);
                }
            }
            Err(err) => {
                warn!("{} fix_table_meta_offset: failed to get shard", tag;
                    "err" => ?err, "files" => ?to_fix_files);
            }
        }
    }

    pub fn tag(&self) -> ShardTag {
        ShardTag::new(self.engine_id, IdVer::new(self.id, self.ver))
    }

    #[inline]
    pub fn initial_flushed(&self) -> bool {
        self.parent.is_none()
    }

    pub fn new_split(
        id: u64,
        ver: u64,
        start: &[u8],
        end: &[u8],
        props: &pb::Properties,
        parent: Box<ShardMeta>,
    ) -> Self {
        let (range, inner_key_off) = if is_whole_keyspace_range(start, end) {
            (ShardRange::new(start, end), KEYSPACE_PREFIX_LEN)
        } else {
            debug!(
                "engine new_split";
                "shard_id" => id,
                "start_key" => format!("{:?}", start),
                "end_key" => format!("{:?}", end),
                "inner_key_off" => parent.inner_key_off
            );
            (ShardRange::new(start, end), parent.inner_key_off)
        };
        Self {
            id,
            ver,
            range,
            inner_key_off,
            parent: Some(parent),
            properties: Properties::new().apply_pb(props),
            ..Default::default()
        }
    }

    fn move_down_file(&mut self, id: u64, cf: i32, level: u32, meta_offset: u32) {
        let tag = self.tag();
        let fm = self
            .files
            .get_mut(&id)
            .unwrap_or_else(|| panic!("{} move_down_file, file {} not found", tag, id));
        assert_eq!(
            fm.get_level() + 1,
            level,
            "{} fm.level {} level {} id: {}",
            tag,
            fm.get_level(),
            level,
            id
        );
        if fm.cf == -1 {
            fm.cf = cf as i8;
        } else {
            assert_eq!(fm.cf, cf as i8);
        }
        fm.level = level as u8;
        fm.l0_size = 0;
        fm.table_meta_off = meta_offset;
    }

    pub fn add_file(&mut self, id: u64, file_meta: FileMeta) {
        self.files.insert(id, file_meta);
    }

    fn delete_file(&mut self, id: u64, level: u32, ft: FileType) {
        if self.has_file_at_level(id, level, ft) {
            self.files.remove(&id);
        }
    }

    fn has_file_at_level(&self, id: u64, level: u32, ft: FileType) -> bool {
        self.file_level(id, ft) == Some(level)
    }

    fn file_level(&self, id: u64, ft: FileType) -> Option<u32> {
        self.files
            .get(&id)
            .filter(|fm| fm.file_type == ft)
            .map(|fm| fm.get_level())
    }

    pub fn get_property(&self, key: &str) -> Option<Bytes> {
        self.properties.get(key)
    }

    pub fn set_property(&mut self, key: &str, value: &[u8]) {
        self.properties.set(key, value);
    }

    pub fn set_property_opt(&mut self, key: &str, value: Option<&[u8]>) {
        if let Some(value) = value {
            self.set_property(key, value);
        }
    }

    pub fn set_property_bytes(&mut self, key: &str, value: Bytes) {
        self.properties.set_bytes(key, value);
    }

    fn set_property_from_other(&mut self, key: &str, other: &ShardMeta) {
        if let Some(value) = other.get_property(key) {
            self.set_property_bytes(key, value);
        } else {
            self.del_property(key);
        }
    }

    pub fn del_property(&mut self, key: &str) -> Option<Bytes> {
        self.properties.remove(key)
    }

    pub fn apply_change_set(&mut self, cs: &pb::ChangeSet) {
        // Keep this assert. As we will invoke this method directly in some scene.
        debug_assert!(cs.sequence > 0, "{}: invalid cs: {:?}", self.tag(), cs);
        self.seq = cs.sequence;
        if cs.has_initial_flush() {
            self.apply_initial_flush(cs);
            return;
        }
        if cs.has_flush() {
            self.apply_flush(cs);
            return;
        }
        if cs.has_compaction() {
            self.apply_compaction(cs.get_compaction());
            return;
        }
        if cs.has_destroy_range() {
            self.apply_destroy_range(cs);
            return;
        }
        if cs.has_truncate_ts() {
            self.apply_truncate_ts(cs);
            return;
        }
        if cs.has_trim_over_bound() {
            self.apply_trim_over_bound(cs);
            return;
        }
        if cs.has_ingest_files() {
            self.apply_ingest_files(cs.get_ingest_files());
            return;
        }
        if cs.has_restore_shard() {
            self.apply_restore_shard(cs);
            return;
        }
        if cs.has_major_compaction() {
            self.apply_major_compaction(cs);
            return;
        }
        if cs.has_update_schema_meta() {
            self.schema.update_from_schema_meta(cs);
            return;
        }
        if cs.has_columnar_compaction() {
            self.apply_columnar_compaction(cs.get_columnar_compaction());
            return;
        }
        if cs.has_update_vector_index() {
            self.apply_update_vector_index(cs.get_update_vector_index());
            return;
        }
        if cs.has_fts_update() {
            self.apply_fts_update(cs.get_fts_update());
            return;
        }
        if cs.get_clear_columnar() || cs.has_clear_columnar_with_restore_version() {
            self.clear_columnar(cs);
            return;
        }
        // NOTE: Used for shard meta persist by diff.
        if cs.has_snapshot_diff() {
            self.apply_snapshot_diff(cs);
            return;
        }
        if !cs.get_property_key().is_empty() {
            if cs.get_property_merge() {
                // Now only DEL_PREFIXES_KEY is mergeable.
                assert_eq!(cs.get_property_key(), DEL_PREFIXES_KEY);
                let prefix = cs.get_property_value();
                self.properties.set(
                    DEL_PREFIXES_KEY,
                    &self
                        .properties
                        .get(DEL_PREFIXES_KEY)
                        .map(|b| DeletePrefixes::unmarshal(b.chunk(), self.range.keyspace_id))
                        .unwrap_or_else(|| {
                            DeletePrefixes::new_with_keyspace_id(self.range.keyspace_id)
                        })
                        .merge_prefix(prefix)
                        .marshal(),
                );
            } else if cs.get_property_key() == STORAGE_CLASS_KEY {
                self.apply_update_storage_class(cs);
            } else {
                self.properties
                    .set(cs.get_property_key(), cs.get_property_value());
            }
            return;
        }
        panic!("unexpected change set {:?}", cs)
    }

    fn is_duplicated_compaction(&self, comp: &mut pb::Compaction) -> bool {
        for i in 0..comp.get_top_deletes().len() {
            let id = comp.get_top_deletes()[i];
            if self.is_compaction_file_deleted(id, comp.level, comp) {
                return true;
            }
        }
        for i in 0..comp.get_bottom_deletes().len() {
            let id = comp.get_bottom_deletes()[i];
            if self.is_compaction_file_deleted(id, comp.level + 1, comp) {
                return true;
            }
        }
        false
    }

    pub fn is_duplicated_major_compaction(&self, comp: &mut pb::MajorCompaction) -> bool {
        let ssts_already_deleted =
            comp.get_sstable_change()
                .get_table_deletes()
                .iter()
                .any(|sst_delete| {
                    !self.has_file_at_level(
                        sst_delete.get_id(),
                        sst_delete.get_level(),
                        FileType::Sst,
                    )
                });
        let blobs_already_deleted = comp
            .get_old_blob_tables()
            .iter()
            .any(|blob_tbl_delete| !self.has_file_at_level(*blob_tbl_delete, 0, FileType::Blob));
        if ssts_already_deleted || blobs_already_deleted {
            info!("{} skip duplicated major compaction {:?}", self.tag(), comp);
            // set compaction to be conflicted, so that newly created files will be GCed.
            comp.conflicted = true;
            return true;
        }
        false
    }

    pub fn is_duplicated_columnar_compaction(&self, comp: &pb::ColumnarCompaction) -> bool {
        match comp.get_target_level() {
            0 => {
                // L0 to columnar compaction.
                let unconverted_l0s = self.unconverted_l0s.iter().collect::<HashSet<_>>();
                if comp
                    .get_row_l0s()
                    .iter()
                    .any(|l0| !unconverted_l0s.contains(l0))
                {
                    return true;
                }
            }
            1 | 2 => {
                // L0/L1 or major columnar compaction.
                let col_already_deleted = comp
                    .get_columnar_change()
                    .get_columnar_deletes()
                    .iter()
                    .any(|delete| {
                        !self.has_file_at_level(
                            delete.get_id(),
                            delete.get_level(),
                            FileType::Columnar,
                        )
                    });
                if col_already_deleted {
                    return true;
                }
            }
            _ => {}
        }
        false
    }

    pub fn is_duplicated_change_set(&self, cs: &mut pb::ChangeSet) -> bool {
        // Keep this assert. As we will invoke this method directly in some scene.
        debug_assert!(cs.sequence > 0, "{}: invalid cs: {:?}", self.tag(), cs);
        if self.seq >= cs.sequence {
            info!(
                "{} skip duplicated change {:?}, meta_seq {}",
                self.tag(),
                cs,
                self.seq
            );
            return true;
        }
        if cs.has_initial_flush() && self.initial_flushed() {
            info!(
                "{} skip duplicated initial flush, already flushed",
                self.tag()
            );
            return true;
        }
        if cs.has_flush() {
            let flush = cs.get_flush();
            if flush.get_version() <= self.data_version() {
                info!(
                    "{} skip duplicated flush {:?} for old version",
                    self.tag(),
                    cs
                );
                return true;
            }
        }
        if cs.has_compaction() {
            let comp = cs.mut_compaction();
            if self.is_duplicated_compaction(comp) {
                return true;
            }
        }
        if cs.has_major_compaction() {
            let comp = cs.mut_major_compaction();
            if self.is_duplicated_major_compaction(comp) {
                return true;
            }
        }
        if cs.has_columnar_compaction()
            && self.is_duplicated_columnar_compaction(cs.get_columnar_compaction())
        {
            info!(
                "{} skip duplicated columnar compaction {:?}",
                self.tag(),
                cs
            );
            return true;
        }
        if cs.has_destroy_range() && self.is_duplicated_table_change(cs.get_destroy_range()) {
            info!("{} skip duplicated destroy range {:?}", self.tag(), cs);
            return true;
        }
        if cs.has_truncate_ts() && self.is_duplicated_table_change(cs.get_truncate_ts()) {
            info!("{} skip duplicated truncate ts {:?}", self.tag(), cs);
            return true;
        }
        if cs.has_trim_over_bound() && self.is_duplicated_table_change(cs.get_trim_over_bound()) {
            info!("{} skip duplicated trim_over_bound {:?}", self.tag(), cs);
            return true;
        }
        if cs.has_ingest_files() {
            let ingest_files = cs.mut_ingest_files();
            if let Some(ingest_id) = is_legacy_ingest(ingest_files) {
                // For legacy BR.
                if let Some(old_ingest_id) = self.get_property(INGEST_ID_KEY) {
                    if ingest_id.eq(&old_ingest_id) {
                        info!(
                            "{} skip duplicated ingest files, ingest_id:{:?}",
                            self.tag(),
                            ingest_id,
                        );
                        return true;
                    }
                }
            } else {
                // For load data.
                let is_empty = self.dedup_ingest_files_of_load_data(ingest_files);
                if is_empty {
                    info!("{} skip duplicated ingest files", self.tag(),);
                    return true;
                }
            }
        }
        if cs.has_restore_shard() {
            // TODO: skip duplicated
        }
        if cs.has_update_schema_meta() {
            let schema_version = cs.get_update_schema_meta().get_version();
            let cur_ver = self.schema.file_ver();
            let valid = self.schema.is_valid();
            let is_duplicated = if valid {
                cur_ver >= schema_version
            } else {
                cur_ver > schema_version
            };
            if is_duplicated {
                info!(
                    "{} skip duplicated update schema meta version {}, cur_ver: {}, valid: {}",
                    self.tag(),
                    schema_version,
                    cur_ver,
                    valid
                );
                return true;
            }
        }
        if cs.has_fts_update() {
            let update = cs.get_fts_update();
            if self.is_duplicated_fts_update(update) {
                info!(
                    "{} skip duplicated fts update {:?}, fts_l0_snap_version: {:?}",
                    self.tag(),
                    update,
                    self.fts_l0_snap_version
                );
                return true;
            }
        }
        if cs.get_property_key() == STORAGE_CLASS_KEY {
            let sc_spec = StorageClassSpec::unmarshal(Some(&cs.property_value));
            return self.get_storage_class_spec() == sc_spec;
        }
        false
    }

    fn is_duplicated_fts_update(&self, update: &pb::FtsUpdate) -> bool {
        if update.get_set_snap_version() > 0
            && update.get_set_snap_version() < self.fts_l0_snap_version.into_inner()
        {
            return true;
        }
        if update
            .get_l0_remove_files()
            .iter()
            .any(|id| !self.fts_l0_files.iter().any(|file| file.get_id() == *id))
            || update
                .get_l1_remove_files()
                .iter()
                .any(|id| !self.fts_l1_files.iter().any(|file| file.get_id() == *id))
            || update
                .get_l2_remove_files()
                .iter()
                .any(|id| !self.fts_l2_files.iter().any(|file| file.get_id() == *id))
        {
            return true;
        }
        false
    }

    fn is_compaction_file_deleted(&self, id: u64, level: u32, comp: &mut pb::Compaction) -> bool {
        if !self.has_file_at_level(id, level, FileType::Sst) {
            info!(
                "{} skip duplicated compaction file {} at level {}, already deleted.",
                self.tag(),
                id,
                level,
            );
            comp.conflicted = true;
            return true;
        }
        false
    }

    // We assume that the shard is empty before load data.
    // Duplication happens when client side retry, or the shard is merged from two
    // shards and one of which has not been ingested.
    fn dedup_ingest_files_of_load_data(&self, ingest_files: &mut pb::IngestFiles) -> bool /* is_empty */
    {
        let l0_files = ingest_files
            .take_l0_creates()
            .into_iter()
            .filter(|file| !self.all_files().contains_key(&file.id))
            .collect::<Vec<_>>();
        ingest_files.set_l0_creates(l0_files.into());

        let ln_files = ingest_files
            .take_table_creates()
            .into_iter()
            .filter(|file| !self.all_files().contains_key(&file.id))
            .collect::<Vec<_>>()
            .into();
        ingest_files.set_table_creates(ln_files);

        let blob_files = ingest_files
            .take_blob_creates()
            .into_iter()
            .filter(|file| !self.all_files().contains_key(&file.id))
            .collect::<Vec<_>>()
            .into();
        ingest_files.set_blob_creates(blob_files);

        ingest_files.get_l0_creates().is_empty()
            && ingest_files.get_table_creates().is_empty()
            && ingest_files.get_blob_creates().is_empty()
    }

    pub fn check_overlap_for_load_data(
        &self,
        ingest_files: &pb::IngestFiles,
    ) -> Option<(
        u64, // existed file id
        u64, // ingest file id
    )> {
        let ingest_tables = ingest_files.get_table_creates();
        // Ingest tables should be sorted, but we still check here for safety.
        let is_sorted = ingest_tables.is_sorted_by(|x, y| x.lower_bound() <= y.lower_bound());

        // Ingest files of load data will be in bottom level of WRITE_CF only.
        for (&existed_file_id, existed_file) in self.files.iter().filter(|(_, f)| {
            f.get_cf() == WRITE_CF as i32 && f.get_level() == WRITE_CF_BOTTOM_LEVEL
        }) {
            if let Some(i) = existed_file
                .data_bound()
                .find_overlap(ingest_tables, is_sorted)
            {
                return Some((existed_file_id, ingest_tables[i].id));
            }
        }
        None
    }

    fn apply_flush(&mut self, cs: &pb::ChangeSet) {
        let flush = cs.get_flush();
        self.apply_properties(flush.get_properties());
        let mut new_l0s = vec![];
        if flush.has_l0_create() {
            let l0 = flush.get_l0_create();
            self.add_file(l0.id, FileMeta::from_l0_table(l0));
            new_l0s.push(l0.id);
        }
        for l0 in flush.get_l0_creates() {
            self.add_file(l0.id, FileMeta::from_l0_table(l0));
            new_l0s.push(l0.id);
        }
        if self.schema.is_valid() && !self.columnar_table_ids.is_empty() {
            self.unconverted_l0s.extend_from_slice(&new_l0s);
        }
        let new_data_seq = flush.get_version() - self.base_version;
        if self.data_sequence < new_data_seq {
            debug!(
                "{} apply flush update data sequence from {} to {}, flush ver {} base {}",
                self.tag(),
                self.data_sequence,
                new_data_seq,
                flush.get_version(),
                self.base_version
            );
            self.data_sequence = new_data_seq;
        }
        self.max_ts = std::cmp::max(self.max_ts, flush.max_ts);
    }

    pub fn apply_initial_flush(&mut self, cs: &pb::ChangeSet) {
        let old_props = self.properties.clone();
        let mut new_meta = Self::new(self.engine_id, cs);
        new_meta.range = self.range.clone();
        new_meta.properties.complement_merge(old_props);
        new_meta.txn_file_locks = self.txn_file_locks.clone();
        // self.data_sequence may be advanced on raft log gc tick.
        new_meta.data_sequence = std::cmp::max(new_meta.data_sequence, self.data_sequence);
        new_meta.max_ts = std::cmp::max(new_meta.max_ts, self.max_ts);

        info!("{} apply_initial_flush", self.tag();
            "prop" => ?new_meta.properties,
            "data_seq" => new_meta.data_sequence,
            "columnar_table_ids" => ?new_meta.columnar_table_ids,
            "max_ts" => new_meta.max_ts);
        *self = new_meta;
    }

    fn apply_compaction(&mut self, comp: &pb::Compaction) {
        if is_move_down(comp) {
            for tbl in comp.get_table_creates() {
                self.move_down_file(tbl.id, tbl.cf, tbl.level, tbl.meta_offset);
            }
            return;
        }
        for id in comp.get_top_deletes() {
            self.delete_file(*id, comp.level, FileType::Sst);
        }
        for id in comp.get_bottom_deletes() {
            self.delete_file(*id, comp.level + 1, FileType::Sst);
        }
        for tbl in comp.get_table_creates() {
            self.add_file(tbl.get_id(), FileMeta::from_table(tbl));
        }
        for blob_table in comp.get_blob_tables() {
            self.add_file(blob_table.get_id(), FileMeta::from_blob_table(blob_table));
        }
    }

    fn apply_major_compaction(&mut self, cs: &pb::ChangeSet) {
        let comp = cs.get_major_compaction();
        for delete in comp.get_sstable_change().get_table_deletes() {
            self.delete_file(delete.get_id(), delete.get_level(), FileType::Sst);
        }
        for create in comp.get_sstable_change().get_table_creates() {
            self.add_file(create.id, FileMeta::from_table(create));
        }
        for delete in comp.get_old_blob_tables() {
            self.delete_file(*delete, 0, FileType::Blob);
        }
        for create in comp.get_new_blob_tables() {
            self.add_file(create.get_id(), FileMeta::from_blob_table(create));
        }
        self.del_property(MANUAL_MAJOR_COMPACTION);
    }

    fn is_duplicated_table_change(&self, tc: &pb::TableChange) -> bool {
        tc.get_table_deletes().iter().any(|deleted| {
            !self.has_file_at_level(deleted.get_id(), deleted.get_level(), FileType::Sst)
        }) || tc.get_columnar_deletes().iter().any(|deleted| {
            !self.has_file_at_level(deleted.get_id(), deleted.get_level(), FileType::Columnar)
        })
    }

    pub fn is_empty_table_change(tc: &pb::TableChange) -> bool {
        tc.get_table_deletes().is_empty()
            && tc.get_table_creates().is_empty()
            && tc.get_columnar_deletes().is_empty()
            && tc.get_columnar_creates().is_empty()
    }

    fn apply_table_change(&mut self, tc: &pb::TableChange) {
        for deleted in tc.get_table_deletes() {
            self.delete_file(deleted.get_id(), deleted.get_level(), FileType::Sst);
        }
        for created in tc.get_table_creates() {
            self.add_file(created.get_id(), FileMeta::from_table(created));
        }
        for deleted in tc.get_columnar_deletes() {
            self.delete_file(deleted.get_id(), deleted.get_level(), FileType::Columnar);
        }
        for created in tc.get_columnar_creates() {
            self.add_file(created.get_id(), FileMeta::from_columnar_table(created));
        }
    }

    fn apply_table_change_to_unconverted_l0s(&mut self, tc: &pb::TableChange) {
        if self.unconverted_l0s.is_empty() {
            return;
        }
        let mut unconverted_l0s: HashSet<u64> = self.unconverted_l0s.iter().cloned().collect();
        // `file_ids_map` is sst table file ids that each deleted file id followed by a
        // created one. If there is no created file for a deleted file, it will not be
        // contained in the `file_ids_map`.
        let mut iter = tc.get_file_ids_map().iter();
        while let (Some(delete_id), Some(create_id)) = (iter.next(), iter.next()) {
            // Only add the corresponding created file if the deleted file is in the
            // `unconverted_l0s`.
            if unconverted_l0s.contains(delete_id) {
                unconverted_l0s.insert(*create_id);
            }
        }
        for deleted in tc.get_table_deletes() {
            unconverted_l0s.remove(&deleted.get_id());
        }

        self.unconverted_l0s = unconverted_l0s.into_iter().collect();
    }

    fn apply_destroy_range(&mut self, cs: &pb::ChangeSet) {
        assert!(cs.has_destroy_range());
        self.apply_table_change(cs.get_destroy_range());
        self.apply_table_change_to_unconverted_l0s(cs.get_destroy_range());
        if cs.has_fts_update() {
            self.apply_fts_update(cs.get_fts_update());
        }
        // ChangeSet of DestroyRange contains the corresponding delete-prefixes which
        // should be cleaned up.
        assert_eq!(cs.get_property_key(), DEL_PREFIXES_KEY);
        if let Some(data) = self.properties.get(DEL_PREFIXES_KEY) {
            let old = DeletePrefixes::unmarshal(data.chunk(), self.range.keyspace_id);
            let done = DeletePrefixes::unmarshal(cs.get_property_value(), self.range.keyspace_id);
            let new = old.split(&done);
            self.properties.set(DEL_PREFIXES_KEY, &new.marshal());
            info!(
                "Destroy range in meta changed from {:?} to {:?} (split done: {:?})",
                old, new, done
            );
        }
    }

    fn apply_truncate_ts(&mut self, cs: &pb::ChangeSet) {
        debug!("apply truncate ts in meta {:?}", self.id);
        assert!(cs.has_truncate_ts());
        self.apply_table_change(cs.get_truncate_ts());
        // During keyspace restoration, truncate_ts will be triggered. We also need
        // manipulate the unconverted_l0s.
        self.apply_table_change_to_unconverted_l0s(cs.get_truncate_ts());
        if cs.has_fts_update() {
            self.apply_fts_update(cs.get_fts_update());
        }
    }

    fn apply_trim_over_bound(&mut self, cs: &pb::ChangeSet) {
        debug!("apply changeset.trim_over_bound in meta {:?}", self.id);
        assert!(cs.has_trim_over_bound());
        self.apply_table_change(cs.get_trim_over_bound());
        self.apply_table_change_to_unconverted_l0s(cs.get_trim_over_bound());
        if cs.has_fts_update() {
            self.apply_fts_update(cs.get_fts_update());
        }
        self.set_property(TRIM_OVER_BOUND, TRIM_OVER_BOUND_DISABLE);
    }

    fn apply_ingest_files(&mut self, ingest_files: &pb::IngestFiles) {
        self.max_ts = std::cmp::max(self.max_ts, ingest_files.max_ts);
        self.apply_properties(ingest_files.get_properties());
        for tbl in ingest_files.get_table_creates() {
            self.add_file(tbl.id, FileMeta::from_table(tbl));
        }
        for l0_tbl in ingest_files.get_l0_creates() {
            self.add_file(l0_tbl.id, FileMeta::from_l0_table(l0_tbl));
        }
        for blob_tbl in ingest_files.get_blob_creates() {
            self.add_file(blob_tbl.id, FileMeta::from_blob_table(blob_tbl));
        }
        if let Some(table_id) = get_table_id_from_ingest_files(ingest_files) {
            self.columnar_table_ids.retain(|id| *id != table_id);
        }
    }

    fn apply_properties(&mut self, props: &pb::Properties) {
        for i in 0..props.get_keys().len() {
            let key = &props.get_keys()[i];
            let val = &props.get_values()[i];
            self.properties.set(key, val.as_slice());
        }
    }

    fn apply_restore_shard(&mut self, cs: &pb::ChangeSet) {
        assert!(cs.has_restore_shard());
        assert_eq!(self.range.outer_start, cs.get_restore_shard().outer_start);
        assert_eq!(self.range.outer_end, cs.get_restore_shard().outer_end);
        info!(
            "{} apply_restore_shard in meta", self.tag();
            "current ver" => self.ver,
            "seq" => self.seq,
            "base_ver" => self.base_version,
            "data_seq" => self.data_sequence,
            "inner_key_off" => self.inner_key_off,
        );

        // Increase shard version to make change sets generated before restore shard
        // stale.
        let mut new_meta = Self::new(self.engine_id, cs);
        new_meta.data_sequence = cs.sequence;
        new_meta.ver = cs.shard_ver + 1;
        *self = new_meta;
        info!(
            "{} apply_restore_shard in meta", self.tag();
            "new ver" => self.ver,
            "seq" => self.seq,
            "base_ver" => self.base_version,
            "data_seq" => self.data_sequence,
            "properties" => ?self.properties.to_pb(self.id),
            "inner_key_off" => self.inner_key_off,
        );
    }

    pub fn apply_split(
        &self,
        split: &kvenginepb::Split,
        sequence: u64,
        initial_seq: u64,
    ) -> Vec<ShardMeta> {
        let old = self;
        let old_storage_class = old.get_property(STORAGE_CLASS_KEY);
        let new_shards_len = split.get_new_shards().len();
        let mut new_shards = Vec::with_capacity(new_shards_len);
        let new_ver = old.ver + new_shards_len as u64 - 1;

        let mut parent = old.clone();
        // We only need lock CF files to recover the mem-table.
        // remove other files to reduce memory usage and rfengine manifest file size.
        parent.files.retain(|_, fm| {
            (fm.file_type == FileType::Sst) && (fm.level == 0 || fm.cf == LOCK_CF as i8)
        });
        parent.vector_indexes.clear();

        for i in 0..new_shards_len {
            let (start_key, end_key) = get_splitting_start_end(
                &old.range.outer_start,
                &old.range.outer_end,
                split.get_keys(),
                i,
            );
            let new_shard = &split.get_new_shards()[i];
            let id = new_shard.get_shard_id();
            let mut meta = ShardMeta::new_split(
                id,
                new_ver,
                start_key,
                end_key,
                new_shard,
                Box::new(parent.clone()),
            );
            meta.engine_id = self.engine_id;
            if id == old.id {
                meta.base_version = old.base_version;
                meta.data_sequence = sequence;
                meta.seq = sequence;
            } else {
                debug!(
                    "new base for {}, {} {}",
                    meta.tag(),
                    old.base_version,
                    sequence
                );
                meta.base_version = old.base_version + sequence;
                meta.data_sequence = initial_seq;
                meta.seq = initial_seq;
            }
            if is_whole_keyspace_range(&meta.range.outer_start, &meta.range.outer_end) {
                new_shards.push(meta);
                continue;
            }
            let (min_table_id, max_table_id) = get_table_id_from_data_bound(meta.data_bound());
            let columnar_table_ids: Vec<_> = old
                .columnar_table_ids
                .iter()
                .filter(|&&table_id| table_id >= min_table_id && table_id <= max_table_id)
                .copied()
                .collect();
            meta.columnar_table_ids = columnar_table_ids;
            meta.columnar_l2_snap_version = old.columnar_l2_snap_version;
            // Although `max_ts` will be updated in initial flush again, still set here to
            // avoid issue in unexpected corner case.
            meta.max_ts = self.max_ts;
            meta.schema = self.schema.clone();
            meta.set_property_opt(STORAGE_CLASS_KEY, old_storage_class.as_deref());
            new_shards.push(meta);
        }
        for new_shard in &mut new_shards {
            if is_whole_keyspace_range(&new_shard.range.outer_start, &new_shard.range.outer_end) {
                continue;
            }
            let new_shard_bound = new_shard.range.data_bound();
            for (fid, fm) in &old.files {
                if new_shard_bound.overlap_bound(fm.data_bound()) {
                    if fm.get_level() == 0
                        && new_shard.schema.is_valid()
                        && old.unconverted_l0s.contains(fid)
                        && !new_shard.columnar_table_ids.is_empty()
                    {
                        new_shard.unconverted_l0s.push(*fid);
                    }
                    if fm.is_columnar_file() && new_shard.columnar_table_ids.is_empty() {
                        continue;
                    }
                    new_shard.files.insert(*fid, fm.clone());
                }
            }
            for vec_idx in &old.vector_indexes {
                if !new_shard.columnar_table_ids.contains(&vec_idx.table_id) {
                    continue;
                }
                let mut files = vec![];
                for vec_file in vec_idx.get_files() {
                    if new_shard_bound.overlap_bound(vec_file.data_bound()) {
                        files.push(vec_file.clone());
                    }
                }
                if !files.is_empty() {
                    let mut new_vec_idx = vec_idx.clone();
                    new_vec_idx.set_files(files.into());
                    new_shard.vector_indexes.push(new_vec_idx);
                }
            }
        }
        new_shards
    }

    pub fn apply_columnar_compaction(&mut self, comp: &pb::ColumnarCompaction) {
        if comp.target_level == 2 {
            self.columnar_l2_snap_version = comp.snap_version.into();
        }

        let col_change = comp.get_columnar_change();
        for col_create in col_change.get_columnar_creates() {
            self.files.insert(
                col_create.get_id(),
                FileMeta::from_columnar_table(col_create),
            );
        }
        for col_delete in col_change.get_columnar_deletes() {
            self.files.remove(&col_delete.get_id());
        }
        self.unconverted_l0s
            .retain(|unconverted_l0| !comp.row_l0s.contains(unconverted_l0));
        if self.columnar_table_ids.is_empty() {
            let new_flushed_l0s: Vec<u64> = self
                .files
                .iter()
                .filter(|(id, fm)| {
                    fm.get_level() == 0 && fm.is_sst_file() && !comp.row_l0s.contains(id)
                })
                .map(|(id, _)| *id)
                .collect();
            self.unconverted_l0s.extend(new_flushed_l0s);
        }
        if comp.snap_version == 0 {
            self.columnar_table_ids.clear();
            self.unconverted_l0s.clear();
            self.vector_indexes.clear();
            self.clear_fts();
            self.schema.clear(self.schema.restore_ver());
        } else {
            self.columnar_table_ids
                .extend_from_slice(comp.get_columnar_table_ids());
            self.columnar_table_ids.sort_unstable();
            self.columnar_table_ids.dedup();
            self.columnar_table_ids
                .retain(|id| !comp.get_columnar_table_ids_to_clear().contains(id));
            let columnar_table_ids = self.columnar_table_ids.clone();
            self.vector_indexes
                .retain(|vec_idx| columnar_table_ids.contains(&vec_idx.table_id));
            // If columnar_table_ids is empty, this must be a major compaction to clear the
            // last table. Make sure all unconverted l0s are cleared.
            if columnar_table_ids.is_empty() {
                self.unconverted_l0s.clear();
            }

            self.retain_fts(&columnar_table_ids);
        }
        if comp.get_is_manual_major_compaction() {
            self.del_property(MANUAL_MAJOR_COMPACTION);
        }
    }

    pub fn apply_update_vector_index(&mut self, update_vec_idx: &pb::UpdateVectorIndex) {
        if let Some(old_idx) = self
            .vector_indexes
            .iter_mut()
            .find(|vec_idx| is_matched_vector_index(vec_idx, update_vec_idx))
        {
            let mut old_files = old_idx.take_files().into_vec();
            old_files.retain(|f| !update_vec_idx.removed.contains(&f.id));
            old_files.extend_from_slice(update_vec_idx.get_added());
            old_idx.set_files(old_files.into());
            old_idx.set_snap_version(update_vec_idx.snap_version);
            return;
        }
        let mut vec_idx = kvenginepb::VectorIndex::new();
        vec_idx.table_id = update_vec_idx.table_id;
        vec_idx.index_id = update_vec_idx.index_id;
        vec_idx.col_id = update_vec_idx.col_id;
        vec_idx.set_snap_version(update_vec_idx.snap_version);
        for file in update_vec_idx.get_added() {
            vec_idx.mut_files().push(file.clone());
        }
        self.vector_indexes.push(vec_idx)
    }

    /// Applies a combined FTS update to the current ShardMeta.
    pub fn apply_fts_update(&mut self, update: &pb::FtsUpdate) {
        if update.get_clear_all() {
            self.fts_indexes.clear();
            self.fts_l0_files.clear();
            self.fts_l1_files.clear();
            self.fts_l2_files.clear();
            self.fts_l0_snap_version = SnapVersion::zero();
            self.fts_pending_l0_ids.clear();
            return;
        }
        for tbl_idx_id in update.get_remove_tracked_indexes() {
            self.fts_indexes
                .remove(&(tbl_idx_id.table_id, tbl_idx_id.index_id));
        }
        for tbl_idx_id in update.get_add_tracked_indexes() {
            self.fts_indexes
                .insert((tbl_idx_id.table_id, tbl_idx_id.index_id));
        }
        if self.fts_indexes.is_empty() {
            self.fts_pending_l0_ids.clear();
        }
        for fts_l0_file in update.get_l0_add_files() {
            self.fts_l0_files.push(fts_l0_file.clone());
        }
        for file in update.get_l1_add_files() {
            self.fts_l1_files.push(file.clone());
        }
        for file in update.get_l2_add_files() {
            self.fts_l2_files.push(file.clone());
        }
        if !update.get_l0_remove_files().is_empty() {
            let remove: HashSet<u64> = update.get_l0_remove_files().iter().copied().collect();
            self.fts_l0_files
                .retain(|file| !remove.contains(&file.get_id()));
        }
        if !update.get_l1_remove_files().is_empty() {
            let remove: HashSet<u64> = update.get_l1_remove_files().iter().copied().collect();
            self.fts_l1_files
                .retain(|file| !remove.contains(&file.get_id()));
        }
        if !update.get_l2_remove_files().is_empty() {
            let remove: HashSet<u64> = update.get_l2_remove_files().iter().copied().collect();
            self.fts_l2_files
                .retain(|file| !remove.contains(&file.get_id()));
        }
        if update.get_set_snap_version() > 0 {
            self.fts_l0_snap_version = SnapVersion::from(update.get_set_snap_version());
        }
        if !update.get_pending_columnar_l0_remove_files().is_empty()
            && !self.fts_pending_l0_ids.is_empty()
        {
            self.fts_pending_l0_ids
                .retain(|id| !update.get_pending_columnar_l0_remove_files().contains(id));
        }
    }

    pub fn apply_snapshot_diff(&mut self, cs: &pb::ChangeSet) {
        self.ver = cs.get_shard_ver();
        let snap = cs.get_snapshot_diff();
        if snap.has_properties() {
            let properties = std::mem::take(&mut self.properties);
            self.properties = properties.apply_pb(snap.get_properties());
        }
        if snap.get_data_sequence() > 0 {
            self.data_sequence = snap.get_data_sequence();
        }
        if snap.has_schema_meta() {
            let schema_meta = snap.get_schema_meta();
            self.schema.schema_file_id = schema_meta.get_file_id();
            self.schema.schema_file_ver = schema_meta.get_version();
            self.schema.schema_restore_ver = schema_meta.get_restore_version();
        }
    }

    pub fn clear_columnar(&mut self, cs: &pb::ChangeSet) {
        let restore_version = if cs.has_clear_columnar_with_restore_version() {
            cs.get_clear_columnar_with_restore_version()
                .get_restore_version()
        } else {
            self.schema.restore_ver()
        };
        info!(
            "{} shard_meta apply clear_columnar, restore_version: {:?}",
            self.tag(),
            restore_version
        );
        self.schema.clear(restore_version);
        self.columnar_table_ids.clear();
        self.columnar_l2_snap_version = SnapVersion::zero();
        self.unconverted_l0s.clear();
        self.vector_indexes.clear();
        self.clear_fts();
        self.files.retain(|_, fm| {
            fm.file_type != FileType::Columnar
                && fm.file_type != FileType::Schema
                && fm.file_type != FileType::VectorIndex
                && fm.file_type != FileType::FtsPackedFile
                && fm.file_type != FileType::FtsDedicatedFile
        });
    }

    pub fn new_snapshot_diff_pb(&self) -> pb::ChangeSet {
        let mut cs = new_change_set(self.id, self.ver);
        cs.set_sequence(self.seq);
        let snap = cs.mut_snapshot_diff();
        let props = snap.mut_properties();
        props.shard_id = self.id;
        if let Some(term) = self.get_property(TERM_KEY) {
            props.keys.push(TERM_KEY.to_string());
            props.values.push(term.to_vec());
        }
        snap.set_data_sequence(self.data_sequence);
        if self.schema.has_value() {
            let schema_meta = snap.mut_schema_meta();
            schema_meta.set_file_id(self.schema.file_id());
            schema_meta.set_version(self.schema.file_ver());
            schema_meta.set_restore_version(self.schema.restore_ver());
            schema_meta.set_keyspace_id(self.range.keyspace_id);
        }
        cs
    }

    fn new_snapshop_pb(&self) -> pb::Snapshot {
        let mut snap = pb::Snapshot::new();
        snap.set_outer_start(self.range.outer_start.to_vec());
        snap.set_outer_end(self.range.outer_end.to_vec());
        snap.set_inner_key_off(self.inner_key_off as u32);
        snap.set_properties(self.properties.to_pb(self.id));
        snap.set_base_version(self.base_version);
        snap.set_data_sequence(self.data_sequence);
        snap.set_max_ts(self.max_ts);
        snap.set_columnar_table_ids(self.columnar_table_ids.clone());
        snap.set_columnar_l2_snap_version(self.columnar_l2_snap_version.into_inner());
        self.schema.to_snapshot(&mut snap);
        snap.set_unconverted_l0s(self.unconverted_l0s.clone());
        snap.set_vector_indexes(self.vector_indexes.clone().into());
        snap.set_fts_indexes(
            self.fts_indexes
                .iter()
                .map(|&(t, i)| {
                    let mut tbl_idx_id = TableIndexId::new();
                    tbl_idx_id.set_table_id(t);
                    tbl_idx_id.set_index_id(i);
                    tbl_idx_id
                })
                .collect(),
        );
        snap.set_fts_l0_snap_version(self.fts_l0_snap_version.into_inner());
        snap.set_fts_l0_files(self.fts_l0_files.clone().into());
        snap.set_fts_l1_files(self.fts_l1_files.clone().into());
        snap.set_fts_l2_files(self.fts_l2_files.clone().into());
        if !self.fts_indexes.is_empty() {
            snap.set_fts_pending_l0_ids(self.collect_fts_pending_l0_ids(&[]));
        }
        snap
    }

    fn collect_fts_pending_l0_ids(&self, source_pending_l0_ids: &[u64]) -> Vec<u64> {
        let mut fts_pending_l0_ids: HashSet<u64> = self
            .fts_pending_l0_ids
            .iter()
            .filter(|id| self.files.contains_key(id))
            .copied()
            .collect();
        fts_pending_l0_ids.extend(
            self.files
                .iter()
                .filter(|(_, fm)| {
                    fm.file_type == FileType::Columnar
                        && fm.get_level() == 0
                        && fm.get_snap_version() > self.fts_l0_snap_version
                })
                .map(|(id, _)| *id),
        );
        fts_pending_l0_ids.extend(source_pending_l0_ids.iter().copied());
        fts_pending_l0_ids.into_iter().collect()
    }

    pub fn to_change_set(&self) -> pb::ChangeSet {
        let mut cs = new_change_set(self.id, self.ver);
        cs.set_sequence(self.seq);
        let mut snap = self.new_snapshop_pb();
        for (&k, v) in self.files.iter() {
            match v.file_type {
                FileType::Sst => {
                    if v.get_level() == 0 {
                        snap.mut_l0_creates().push(v.to_l0_create(k));
                    } else {
                        snap.mut_table_creates().push(v.to_table_create(k));
                    }
                }
                FileType::TxnChunk | FileType::Schema => unreachable!("not loaded to file meta"),
                FileType::Columnar => {
                    snap.mut_columnar_creates().push(v.to_columnar_create(k));
                }
                FileType::Blob => {
                    snap.mut_blob_creates().push(v.to_blob_create(k));
                }
                FileType::VectorIndex => {} // already handled in vector_indexes.
                FileType::FtsPackedFile => {} // handled separately.
                FileType::FtsDedicatedFile => {} // handled separately.
            }
        }
        cs.set_snapshot(snap);
        if let Some(parent) = &self.parent {
            cs.set_parent(parent.to_change_set());
        }
        cs
    }

    pub fn marshal(&self) -> Vec<u8> {
        let cs = self.to_change_set();
        cs.write_to_bytes().unwrap()
    }

    pub fn all_file_keys(&self) -> Vec<u64> {
        let mut ids = Vec::with_capacity(self.files.len());
        for k in self.files.keys() {
            ids.push(*k);
        }
        ids
    }

    pub fn all_files(&self) -> &HashMap<u64, FileMeta> {
        &self.files
    }

    pub fn all_txn_chunk_ids(&self) -> Vec<u64> {
        TxnFileRefPropertyHelper::from_property(self.properties.get(TXN_FILE_REF))
            .unwrap()
            .get_all_chunk_ids()
    }

    pub(crate) fn get_blob_files(&self) -> Vec<(u64, Vec<u8>, Vec<u8>, u32)> {
        let mut blob_files = vec![];
        for (id, file) in &self.files {
            if file.file_type == FileType::Blob {
                blob_files.push((
                    *id,
                    file.smallest.to_vec(),
                    file.biggest.to_vec(),
                    file.table_meta_off,
                ));
            }
        }
        blob_files
    }

    // the ingest level never skip to lower level if upper level file exists, this
    // may not be optimal but prevent compaction generate conflicting files.
    // It find the top most existing file's level as ingest level, if there is
    // overlap with existing files, it will use the one level upper.
    pub(crate) fn get_ingest_level(&self, table_bound: DataBound<'_>) -> u32 {
        // find the top most level as ingest level.
        let mut ingest_level = self
            .files
            .values()
            .filter(|f| f.file_type == FileType::Sst)
            .map(|f| f.level)
            .min()
            .unwrap_or(3);
        if ingest_level == 0 {
            return 0;
        }
        let overlap = self
            .files
            .values()
            .filter(|f| f.level == ingest_level && f.cf == 0)
            .any(|file| table_bound.overlap_bound(file.data_bound()));
        if overlap {
            ingest_level -= 1;
        }
        ingest_level as u32
    }

    pub(crate) fn data_version(&self) -> u64 {
        self.base_version + self.data_sequence
    }

    pub fn data_persisted_log_index(&self) -> u64 {
        match &self.parent {
            Some(parent) if parent.id == self.id => parent.data_sequence,
            _ => self.data_sequence,
        }
    }

    pub fn prepare_merge(&mut self, sequence: u64) {
        let parent = self.clone();
        self.ver += 1;
        self.seq = sequence;
        self.data_sequence = sequence;
        self.parent = Some(Box::new(parent));
    }

    pub fn rollback_merge(&mut self, sequence: u64) {
        self.ver += 1;
        self.seq = sequence;
    }

    pub fn commit_merge(&mut self, source: &ShardMeta, sequence: u64) {
        // If the regions are not belong to the same keyspace, reset the encryption_key
        // property.
        let belongs_to_same_keyspace =
            ApiV2::is_belongs_to_same_keyspace(&source.range.outer_start, &self.range.outer_start);
        if !belongs_to_same_keyspace {
            self.del_property(ENCRYPTION_KEY);
        }

        let (clear_source, clear_target) =
            need_clear_region_data_on_merge(&source.range.outer_start, &self.range.outer_start);

        // Include the max_ts and source files in the parent for future initial flush.
        self.max_ts = std::cmp::max(self.max_ts, source.max_ts);
        let mut parent = self.clone();

        if clear_target {
            info!(
                "{} clear data of target region on merge, target: {:?}",
                self.tag(),
                self.range,
            );

            // clear all files if exists
            parent.files.clear();
            parent.vector_indexes.clear();
            self.files.clear();
            self.vector_indexes.clear();
            // remove DEL_PREFIXES_KEY property if exists
            self.del_property(DEL_PREFIXES_KEY);
            self.del_property(STORAGE_CLASS_KEY);
        }
        if !clear_source {
            for (&id, source_file) in &source.files {
                parent.files.insert(id, source_file.clone());
            }
            for (&id, source_file) in &source.files {
                self.files.insert(id, source_file.clone());
            }
            for vec_idx in &source.vector_indexes {
                self.merge_vector_index(vec_idx);
            }

            if clear_target {
                // `inner_key_off` will be different when merge regions of different keyspaces.
                parent.inner_key_off = source.inner_key_off;
                self.inner_key_off = source.inner_key_off;

                parent.set_property_from_other(STORAGE_CLASS_KEY, source);
                self.set_property_from_other(STORAGE_CLASS_KEY, source);
            }

            // merge DEL_PREFIXES_KEY from source if needed
            let source_del_prefixes = source.get_property(DEL_PREFIXES_KEY);
            let parent_del_prefixes = self.get_property(DEL_PREFIXES_KEY);
            if let Some(new_del_prefixes) = merge_del_prefixes_if_needed(
                source_del_prefixes,
                parent_del_prefixes,
                self.range.keyspace_id,
            ) {
                self.set_property(DEL_PREFIXES_KEY, &new_del_prefixes);
            }
            self.schema.merge_from(&self.tag(), &source.schema);
        } else {
            info!(
                "{} clear data of source region on merge, source: {:?}",
                self.tag(),
                source.range,
            );
        }

        if self.range.outer_end == source.range.outer_start {
            self.range.outer_end = source.range.outer_end.clone();
        } else {
            self.range.outer_start = source.range.outer_start.clone();
        }
        self.ver = max(self.ver, source.ver) + 1;
        let source_mem_tbl_version = source.base_version + source.seq;
        let target_mem_tbl_version = self.base_version + sequence;
        self.base_version = max(source_mem_tbl_version, target_mem_tbl_version) - sequence;
        self.data_sequence = sequence;
        self.columnar_table_ids = merge_columnar_table_ids(
            &source.columnar_table_ids,
            &self.columnar_table_ids,
            source.data_bound(),
            self.data_bound(),
        );
        self.columnar_l2_snap_version = max(
            self.columnar_l2_snap_version,
            source.columnar_l2_snap_version,
        );
        // Remove columnar and vector if related table is not in the merged
        // columnar_table_ids.
        self.retain_columnar_and_vector();
        self.parent = Some(Box::new(parent));
        self.seq = sequence;
    }

    fn retain_columnar_and_vector(&mut self) {
        let columnar_table_ids = self.columnar_table_ids.clone();
        self.files.retain(|_, file| {
            if !file.is_columnar_file() && !file.is_vector_index_file() {
                return true;
            }
            let data_bound = file.data_bound();
            let (min_table_id, max_table_id) = get_table_id_from_data_bound(data_bound);
            columnar_table_ids
                .iter()
                .any(|&id| id >= min_table_id && id <= max_table_id)
        });
        // Source or target shard may has pending columnar major compaction, after merge
        // the columnar_table_ids will not contains the table. If columnar_table_ids is
        // empty, we should also clear the unconverted_l0s.
        if columnar_table_ids.is_empty() {
            self.unconverted_l0s.clear();
        }
        self.vector_indexes
            .retain(|vec| columnar_table_ids.contains(&vec.table_id));
    }

    fn merge_vector_index(&mut self, vec_idx: &VectorIndex) {
        if let Some(old_idx) = self
            .vector_indexes
            .iter_mut()
            .find(|v| is_same_vector_index(v, vec_idx))
        {
            let old_file_ids: HashSet<u64> = old_idx.files.iter().map(|f| f.id).collect();
            for file in vec_idx.files.iter() {
                if !old_file_ids.contains(&file.id) {
                    old_idx.files.push(file.clone());
                }
            }
        } else {
            self.vector_indexes.push(vec_idx.clone());
        }
    }

    fn clear_fts(&mut self) {
        self.fts_indexes.clear();
        self.fts_l0_files.clear();
        self.fts_l1_files.clear();
        self.fts_l2_files.clear();
        self.fts_l0_snap_version = SnapVersion::zero();
        self.fts_pending_l0_ids.clear();
    }

    fn retain_fts(&mut self, columnar_table_ids: &[i64]) {
        self.fts_indexes
            .retain(|(table_id, _)| columnar_table_ids.contains(table_id));
        self.fts_l0_files.retain(|fts_l0_file| {
            let data_bound = DataBound::new(
                InnerKey::from_inner_buf(&fts_l0_file.smallest),
                InnerKey::from_inner_buf(&fts_l0_file.biggest),
                true,
            );
            let (min_table_id, max_table_id) = get_table_id_from_data_bound(data_bound);
            columnar_table_ids
                .iter()
                .any(|&id| id >= min_table_id && id <= max_table_id)
        });
        self.fts_l1_files.retain(|fts_l1_file| {
            let data_bound = DataBound::new(
                InnerKey::from_inner_buf(&fts_l1_file.smallest),
                InnerKey::from_inner_buf(&fts_l1_file.biggest),
                true,
            );
            let (min_table_id, max_table_id) = get_table_id_from_data_bound(data_bound);
            columnar_table_ids
                .iter()
                .any(|&id| id >= min_table_id && id <= max_table_id)
        });
        self.fts_l2_files.retain(|fts_l2_file| {
            let data_bound = DataBound::new(
                InnerKey::from_inner_buf(&fts_l2_file.smallest),
                InnerKey::from_inner_buf(&fts_l2_file.biggest),
                true,
            );
            let (min_table_id, max_table_id) = get_table_id_from_data_bound(data_bound);
            columnar_table_ids
                .iter()
                .any(|&id| id >= min_table_id && id <= max_table_id)
        });
        let files = &self.files;
        self.fts_pending_l0_ids.retain(|pending_id| {
            files.get(pending_id).is_some_and(|fm| {
                let (min_table_id, max_table_id) = get_table_id_from_data_bound(fm.data_bound());
                columnar_table_ids
                    .iter()
                    .any(|&id| id >= min_table_id && id <= max_table_id)
            })
        });
    }

    pub fn merge_txn_file_ref(&mut self, wb_ref: &TxnFileRef, log_index: u64) {
        let tag = self.tag();
        let current_seq = self.txn_file_locks.seq();

        debug_assert!(
            current_seq < log_index,
            "{} duplicated merge txn file ref, seq {}, log_index {}, wb_ref {:?}",
            tag,
            current_seq,
            log_index,
            wb_ref
        );

        if wb_ref.is_locked() {
            self.txn_file_locks
                .insert(log_index, wb_ref.start_ts, wb_ref.get_lock_val_prefix());
        } else {
            let existed = self.txn_file_locks.remove(log_index, wb_ref.start_ts);
            if !existed {
                warn!(
                    "{} unexpected txn not existed, wb_ref {:?}, log_index {}, last_seq {}, current locks {:?}",
                    tag, wb_ref, log_index, current_seq, self.txn_file_locks
                );
            }
        }
        debug!("{} ShardMeta merge txn file ref", tag;
            "wb_ref" => ?wb_ref,
            "log_index" => log_index,
            "locks" => ?self.txn_file_locks);
    }

    pub fn has_txn_file_locks(&self) -> bool {
        !self.txn_file_locks.is_empty()
    }

    pub fn txn_file_locks(&self) -> &TxnFileLocks {
        &self.txn_file_locks
    }

    pub fn recover_txn_file_locks_from_shard(&mut self, shard: &Shard) {
        self.txn_file_locks = TxnFileLocks::from_lock_txn_files(
            shard.get_write_sequence(),
            shard.get_data().get_lock_txn_files(),
        );
        debug!("{} recover txn file locks from shard", self.tag(); "locks" => ?self.txn_file_locks);
    }

    pub fn collect_files_for_storage_class(
        &mut self,
        cs: &kvenginepb::ChangeSet,
    ) -> Option<pb::Snapshot> {
        debug_assert_eq!(cs.get_property_key(), STORAGE_CLASS_KEY);
        let mut snap = pb::Snapshot::new();
        for (&k, v) in self.files.iter() {
            if v.file_type == FileType::Sst && v.get_level() > 0 && v.get_cf() as usize == WRITE_CF
            {
                snap.mut_table_creates().push(v.to_table_create(k));
            }
        }
        if snap.table_creates.is_empty() {
            return None;
        }
        Some(snap)
    }

    pub fn get_storage_class_spec(&self) -> StorageClassSpec {
        StorageClassSpec::unmarshal(self.get_property(STORAGE_CLASS_KEY).as_deref())
    }

    fn apply_update_storage_class(&self, cs: &pb::ChangeSet) {
        debug_assert_eq!(cs.get_property_key(), STORAGE_CLASS_KEY);
        let sc_spec = StorageClassSpec::unmarshal(Some(cs.get_property_value()));
        if sc_spec.is_specified() {
            debug!("{} set storage class spec", self.tag(); "spec" => ?sc_spec);
            self.properties
                .set(cs.get_property_key(), cs.get_property_value());
        } else {
            debug!("{} clear storage class spec", self.tag());
            self.properties.remove(cs.get_property_key());
        }
    }

    // Used in native_br.
    pub fn clear_columnar_related_meta(&mut self) {
        self.files.retain(|_, file| {
            file.file_type != FileType::Columnar && file.file_type != FileType::VectorIndex
        });
        self.columnar_table_ids.clear();
        self.columnar_l2_snap_version = SnapVersion::zero();
        self.unconverted_l0s.clear();
        self.vector_indexes.clear();
        if let Some(parent) = &mut self.parent {
            parent.clear_columnar_related_meta();
        }
    }
}

impl BoundedDataSet for ShardMeta {
    fn data_bound(&self) -> DataBound<'_> {
        self.range.data_bound()
    }
}

#[derive(Clone, Debug)]
pub struct FileMeta {
    pub cf: i8,
    pub level: u8,
    pub file_type: FileType,
    pub smallest: Bytes,
    pub biggest: Bytes,

    // Available ONLY for SST level 0.
    pub l0_size: u32,

    // Available ONLY for SST (level 1+) & Columnar & VectorIndex & Blob.
    pub table_meta_off: u32,

    // Available ONLY for Columnar.
    pub snap_version: Option<SnapVersion>,
}

impl FileMeta {
    fn new(
        cf: i32,
        level: u32,
        file_type: FileType,
        smallest: &[u8],
        biggest: &[u8],
        l0_size: u32,
        table_meta_off: u32,
        snap_version: Option<SnapVersion>,
    ) -> Self {
        Self {
            cf: cf as i8,
            level: level as u8,
            file_type,
            smallest: Bytes::copy_from_slice(smallest),
            biggest: Bytes::copy_from_slice(biggest),
            l0_size,
            table_meta_off,
            snap_version,
        }
    }

    pub fn get_level(&self) -> u32 {
        self.level as u32
    }

    pub fn get_cf(&self) -> i32 {
        self.cf as i32
    }

    pub fn has_locks(&self) -> bool {
        self.level == 0 || self.cf == LOCK_CF as i8
    }

    pub fn is_schema_file(&self) -> bool {
        self.file_type == FileType::Schema
    }

    pub fn is_sst_file(&self) -> bool {
        self.file_type == FileType::Sst
    }

    pub fn is_columnar_file(&self) -> bool {
        self.file_type == FileType::Columnar
    }

    pub fn is_vector_index_file(&self) -> bool {
        self.file_type == FileType::VectorIndex
    }

    pub fn is_fts_file(&self) -> bool {
        self.file_type == FileType::FtsPackedFile || self.file_type == FileType::FtsDedicatedFile
    }

    pub fn can_use_ia(&self) -> bool {
        match self.file_type {
            FileType::Sst if (self.cf as usize == WRITE_CF && self.level > 0) => true,
            FileType::Columnar => true,
            FileType::VectorIndex => true,
            FileType::FtsPackedFile => true,
            FileType::FtsDedicatedFile => true,
            FileType::Blob => true,
            _ => false,
        }
    }

    pub fn get_snap_version(&self) -> SnapVersion {
        self.snap_version.unwrap_or(SnapVersion::zero())
    }

    pub fn is_l0_sst_with_size(&self) -> bool {
        self.file_type == FileType::Sst && self.level == 0 && self.l0_size > 0
    }

    pub fn from_l0_table(table: &kvenginepb::L0Create) -> Self {
        Self::new(
            -1,
            0,
            FileType::Sst,
            table.get_smallest(),
            table.get_biggest(),
            table.size,
            0,
            None,
        )
    }

    pub fn from_table(table: &kvenginepb::TableCreate) -> Self {
        Self::new(
            table.cf,
            table.level,
            FileType::Sst,
            table.get_smallest(),
            table.get_biggest(),
            0,
            table.meta_offset,
            None,
        )
    }

    pub fn from_columnar_table(table: &kvenginepb::ColumnarCreate) -> Self {
        Self::new(
            0,
            table.level,
            FileType::Columnar,
            table.get_smallest(),
            table.get_biggest(),
            0,
            table.meta_offset,
            Some(SnapVersion::from(table.snap_version)),
        )
    }

    pub fn from_blob_table(table: &kvenginepb::BlobCreate) -> Self {
        Self::new(
            -1,
            0,
            FileType::Blob,
            table.get_smallest(),
            table.get_biggest(),
            0,
            table.get_meta_offset(),
            None,
        )
    }

    pub fn from_vector_index_file(vec_idx_file: &kvenginepb::VectorIndexFile) -> Self {
        Self::new(
            0,
            0,
            FileType::VectorIndex,
            vec_idx_file.get_smallest(),
            vec_idx_file.get_biggest(),
            0,
            vec_idx_file.get_meta_offset(),
            None,
        )
    }

    pub fn from_fts_l0_file(fts_file: &kvenginepb::FtsPackedFileInfo) -> Self {
        Self::new(
            0,
            0,
            FileType::FtsPackedFile,
            fts_file.get_smallest(),
            fts_file.get_biggest(),
            0,
            fts_file.get_meta_offset(),
            None,
        )
    }

    pub fn from_fts_l1_file(fts_file: &kvenginepb::FtsPackedFileInfo) -> Self {
        Self::new(
            0,
            1,
            FileType::FtsPackedFile,
            fts_file.get_smallest(),
            fts_file.get_biggest(),
            0,
            fts_file.get_meta_offset(),
            None,
        )
    }

    pub fn from_fts_l2_file(fts_file: &kvenginepb::FtsDedFileInfo) -> Self {
        Self::new(
            0,
            2,
            FileType::FtsDedicatedFile,
            fts_file.get_smallest(),
            fts_file.get_biggest(),
            0,
            fts_file.get_meta_offset(),
            None,
        )
    }

    pub fn from_schema_meta() -> Self {
        Self::new(0, 0, FileType::Schema, &[], &[], 0, 0, None)
    }

    pub fn to_l0_create(&self, id: u64) -> kvenginepb::L0Create {
        let mut l0_create = kvenginepb::L0Create::new();
        l0_create.set_id(id);
        l0_create.set_smallest(self.smallest.to_vec());
        l0_create.set_biggest(self.biggest.to_vec());
        l0_create.set_size(self.l0_size);
        l0_create
    }

    pub fn to_table_create(&self, id: u64) -> kvenginepb::TableCreate {
        let mut table_create = kvenginepb::TableCreate::new();
        table_create.set_id(id);
        table_create.set_cf(self.cf as i32);
        table_create.set_level(self.get_level());
        table_create.set_smallest(self.smallest.to_vec());
        table_create.set_biggest(self.biggest.to_vec());
        table_create.set_meta_offset(self.table_meta_off);
        table_create
    }

    pub fn to_columnar_create(&self, id: u64) -> kvenginepb::ColumnarCreate {
        let mut columnar_create = kvenginepb::ColumnarCreate::new();
        columnar_create.set_id(id);
        columnar_create.set_level(self.get_level());
        columnar_create.set_smallest(self.smallest.to_vec());
        columnar_create.set_biggest(self.biggest.to_vec());
        columnar_create.set_meta_offset(self.table_meta_off);
        columnar_create.set_snap_version(self.snap_version.map(|v| v.into_inner()).unwrap_or(0));
        columnar_create
    }

    pub fn to_blob_create(&self, id: u64) -> kvenginepb::BlobCreate {
        let mut blob_create = kvenginepb::BlobCreate::new();
        blob_create.set_id(id);
        blob_create.set_smallest(self.smallest.to_vec());
        blob_create.set_biggest(self.biggest.to_vec());
        blob_create
    }
}

impl BoundedDataSet for FileMeta {
    fn data_bound(&self) -> DataBound<'_> {
        DataBound::new(
            InnerKey::from_inner_buf(&self.smallest),
            InnerKey::from_inner_buf(&self.biggest),
            true,
        )
    }
}

impl Default for FileMeta {
    fn default() -> Self {
        Self::new(0, 0, FileType::Sst, b"", b"", 0, 0, None)
    }
}

pub fn is_move_down(comp: &pb::Compaction) -> bool {
    !comp.top_deletes.is_empty()
        && comp.top_deletes.len() == comp.table_creates.len()
        && comp.top_deletes[0] == comp.table_creates[0].id
}

pub fn new_change_set(shard_id: u64, shard_ver: u64) -> pb::ChangeSet {
    let mut cs = pb::ChangeSet::new();
    cs.set_shard_id(shard_id);
    cs.set_shard_ver(shard_ver);
    cs
}

pub const GLOBAL_SHARD_END_KEY: &[u8] = &[255, 255, 255, 255, 255, 255, 255, 255];

#[derive(Default, Clone, Debug)]
pub struct SchemaFileMeta {
    schema_file_id: u64,
    schema_file_ver: i64,
    schema_restore_ver: u64,
}

impl SchemaFileMeta {
    pub fn is_valid(&self) -> bool {
        self.schema_file_id > 0
    }

    #[inline]
    pub fn file_id(&self) -> u64 {
        self.schema_file_id
    }

    #[inline]
    pub fn file_ver(&self) -> i64 {
        self.schema_file_ver
    }

    #[inline]
    pub fn restore_ver(&self) -> u64 {
        self.schema_restore_ver
    }

    #[inline]
    pub fn has_value(&self) -> bool {
        self.schema_file_id > 0 || self.schema_file_ver > 0 || self.schema_restore_ver > 0
    }

    pub fn from_snapshot(snap: &pb::Snapshot) -> Self {
        debug_assert!(snap.has_schema_meta());
        let sm = snap.get_schema_meta();
        Self {
            schema_file_id: sm.get_file_id(),
            schema_file_ver: sm.get_version(),
            schema_restore_ver: sm.get_restore_version(),
        }
    }

    pub fn update_from_schema_meta(&mut self, cs: &pb::ChangeSet) {
        debug_assert!(cs.has_update_schema_meta());
        let sm = cs.get_update_schema_meta();
        self.schema_file_id = sm.get_file_id();
        self.schema_file_ver = sm.get_version();
        debug_assert_eq!(self.schema_restore_ver, sm.get_restore_version());
    }

    pub fn update_by_restore(
        &mut self,
        new_file_id: u64,
        schema_file_ver: i64,
        schema_restore_ver: u64,
    ) {
        self.schema_file_id = new_file_id;
        self.schema_file_ver = schema_file_ver;
        self.schema_restore_ver = schema_restore_ver;
    }

    pub fn clear(&mut self, reset_restore_version: u64) {
        self.schema_file_id = 0;
        self.schema_restore_ver = reset_restore_version;
    }

    pub fn to_snapshot(&self, snap: &mut pb::Snapshot) {
        if self.is_valid() || self.schema_restore_ver > 0 {
            let mut sm = SchemaMeta::new();
            sm.set_file_id(self.schema_file_id);
            sm.set_version(self.schema_file_ver);
            sm.set_restore_version(self.schema_restore_ver);
            snap.set_schema_meta(sm);
        }
    }

    pub fn merge_from(&mut self, tag: &ShardTag, source: &SchemaFileMeta) {
        if source.schema_restore_ver == self.schema_restore_ver
            && source.schema_file_ver >= self.schema_file_ver
        {
            self.schema_file_id = source.schema_file_id;
            self.schema_file_ver = source.schema_file_ver;
        } else if source.schema_restore_ver != self.schema_restore_ver
            && source.schema_file_id > self.schema_file_id
        {
            // Happens during restore and only one of the source/target has been restored.
            // The restored schema should have larger schema file id.
            info!("{} merge schema file meta with different restore ver", tag;
                "current" => ?self, "source" => ?source);
            *self = source.clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::iter::{FromIterator, Iterator};

    use super::*;

    #[test]
    fn test_fts_update_remove_tracked_indexes() {
        let mut meta = ShardMeta::default();
        meta.fts_indexes.insert((10, 1));
        meta.fts_indexes.insert((10, 2));
        meta.fts_pending_l0_ids = vec![100, 200];

        let mut update = pb::FtsUpdate::new();
        let mut tbl_idx_id = TableIndexId::new();
        tbl_idx_id.set_table_id(10);
        tbl_idx_id.set_index_id(1);
        update.mut_remove_tracked_indexes().push(tbl_idx_id);

        meta.apply_fts_update(&update);

        assert!(!meta.fts_indexes.contains(&(10, 1)));
        assert!(meta.fts_indexes.contains(&(10, 2)));
        assert_eq!(
            meta.fts_pending_l0_ids,
            vec![100, 200],
            "pending ids should be kept if there are remaining tracked indexes"
        );

        let mut update = pb::FtsUpdate::new();
        let mut tbl_idx_id = TableIndexId::new();
        tbl_idx_id.set_table_id(10);
        tbl_idx_id.set_index_id(2);
        update.mut_remove_tracked_indexes().push(tbl_idx_id);

        meta.apply_fts_update(&update);
        assert!(meta.fts_indexes.is_empty());
        assert!(
            meta.fts_pending_l0_ids.is_empty(),
            "pending ids should be cleared when no indexes are tracked"
        );
    }

    #[test]
    fn test_ingest_level() {
        for level in 0..=3 {
            let mut cs = new_change_set(1, 1);
            let snap = cs.mut_snapshot();
            snap.set_outer_end(GLOBAL_SHARD_END_KEY.to_vec());
            snap.set_max_ts(100);
            let mut id = 0;
            let mut make_table = |level: u32, smallest: &str, biggest: &str| {
                id += 1;
                let mut tbl = pb::TableCreate::new();
                tbl.id = id;
                tbl.level = level;
                tbl.smallest = smallest.as_bytes().to_vec();
                tbl.biggest = biggest.as_bytes().to_vec();
                tbl
            };
            let tables = snap.mut_table_creates();
            tables.push(make_table(level, "1000", "2000"));
            let meta = ShardMeta::new(1, &cs);
            assert_eq!(meta.max_ts, 100);
            let assert_get_ingest_level = |smallest: &str, biggest: &str, level| {
                assert_eq!(
                    meta.get_ingest_level(DataBound::new(
                        InnerKey::from_inner_buf(smallest.as_bytes()),
                        InnerKey::from_inner_buf(biggest.as_bytes()),
                        true
                    )),
                    level
                );
            };
            let overlap_level = level.saturating_sub(1);
            assert_get_ingest_level("0000", "0999", level);
            assert_get_ingest_level("1000", "1500", overlap_level);
            assert_get_ingest_level("1000", "2000", overlap_level);
            assert_get_ingest_level("1500", "2000", overlap_level);
            assert_get_ingest_level("1500", "2999", overlap_level);
            assert_get_ingest_level("2000", "2999", overlap_level);
            assert_get_ingest_level("2500", "2999", level);
        }
    }

    // See issue #572
    // https://github.com/tidbcloud/cloud-storage-engine/issues/572
    #[test]
    fn test_delete_file_with_level() {
        let files = vec![
            // L0:
            (1, FileMeta::new(0, 0, FileType::Sst, b"", b"", 0, 0, None)),
            // L1:
            (
                101,
                FileMeta::new(0, 1, FileType::Sst, b"", b"", 0, 0, None),
            ),
            (
                102,
                FileMeta::new(0, 1, FileType::Sst, b"", b"", 0, 0, None),
            ),
            // L2:
            (
                201,
                FileMeta::new(0, 2, FileType::Sst, b"", b"", 0, 0, None),
            ),
            (
                202,
                FileMeta::new(0, 2, FileType::Sst, b"", b"", 0, 0, None),
            ),
        ];

        // comp_level, top_deletes, bottom_deletes, is_duplicated, result_files
        struct Case(u32, &'static [u64], &'static [u64], bool, &'static [u64]);
        let cases: Vec<Case> = vec![
            // delete top
            Case(0, &[1], &[], false, &[101, 102, 201, 202]),
            Case(1, &[101], &[], false, &[1, 102, 201, 202]),
            // delete bottom
            Case(0, &[], &[102], false, &[1, 101, 201, 202]),
            Case(1, &[], &[201, 202], false, &[1, 101, 102]),
            // delete both top & bottom
            Case(1, &[102], &[202], false, &[1, 101, 201]),
            // top deleted
            Case(0, &[888], &[], true, &[1, 101, 102, 201, 202]),
            Case(1, &[888], &[], true, &[1, 101, 102, 201, 202]),
            // bottom deleted
            Case(1, &[], &[888], true, &[1, 101, 102, 201, 202]),
            // bottom level not match
            Case(1, &[], &[101], true, &[1, 101, 102, 201, 202]),
            // top level not match
            Case(1, &[201], &[202], true, &[1, 101, 102, 201]),
        ];

        for (idx, Case(comp_level, top_deletes, bottom_deletes, is_duplicated, result_files)) in
            cases.into_iter().enumerate()
        {
            let meta = ShardMeta {
                files: HashMap::from_iter(files.clone().into_iter()),
                ..Default::default()
            };

            // Test compaction.
            {
                let mut meta = meta.clone();
                let mut comp = pb::Compaction::default();
                comp.set_level(comp_level);
                comp.mut_top_deletes().extend_from_slice(top_deletes);
                comp.mut_bottom_deletes().extend_from_slice(bottom_deletes);
                comp.mut_table_creates().push(pb::TableCreate {
                    id: 100000,
                    level: comp_level + 1,
                    ..Default::default()
                });

                assert_eq!(
                    meta.is_duplicated_compaction(&mut comp),
                    is_duplicated,
                    "case {}",
                    idx
                );

                meta.apply_compaction(&comp);
                assert_eq!(result_files.len() + 1, meta.files.len());
                for id in result_files {
                    assert!(meta.files.contains_key(id), "case {} file id {}", idx, id);
                }
            }

            // Test other table changes.
            {
                let mut table_change = pb::TableChange::default();
                let table_deletes = table_change.mut_table_deletes();
                for &id in top_deletes {
                    let deleted = pb::TableDelete {
                        id,
                        level: comp_level,
                        ..Default::default()
                    };
                    table_deletes.push(deleted);
                }
                for &id in bottom_deletes {
                    let deleted = pb::TableDelete {
                        id,
                        level: comp_level + 1,
                        ..Default::default()
                    };
                    table_deletes.push(deleted);
                }

                let changesets = vec![
                    {
                        // destroy_range
                        let mut cs = pb::ChangeSet::default();
                        cs.set_destroy_range(table_change.clone());
                        cs.set_property_key(DEL_PREFIXES_KEY.to_string());
                        cs
                    },
                    {
                        // truncate_ts
                        let mut cs = pb::ChangeSet::default();
                        cs.set_truncate_ts(table_change.clone());
                        cs
                    },
                    {
                        // trim_over_bound
                        let mut cs = pb::ChangeSet::default();
                        cs.set_trim_over_bound(table_change.clone());
                        cs
                    },
                ];

                let mut seq = 5; // RAFT_INIT_LOG_INDEX
                for mut cs in changesets {
                    cs.set_sequence(seq);
                    seq += 1;

                    let mut meta = meta.clone();
                    assert_eq!(
                        meta.is_duplicated_change_set(&mut cs),
                        is_duplicated,
                        "case {} cs {:?}",
                        idx,
                        cs,
                    );

                    meta.apply_change_set(&cs);
                    assert_eq!(result_files.len(), meta.files.len());
                    for id in result_files {
                        assert!(
                            meta.files.contains_key(id),
                            "case {} cs {:?} file id {}",
                            idx,
                            cs,
                            id
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_table_overlap() {
        let (range, inner_key_off) = (
            ShardRange::new(&[b'x', 2, 3, 4, 10], &[b'x', 2, 3, 4, 20]),
            4,
        );
        let meta = ShardMeta {
            range,
            inner_key_off,
            ..Default::default()
        };

        let cases: Vec<(u8, u8, bool, bool)> = vec![
            // smallest, biggest, overlap, contains
            (3, 5, false, false),
            (3, 10, true, false),
            (3, 15, true, false),
            (3, 20, true, false),
            (3, 25, true, false),
            (10, 15, true, true),
            (10, 20, true, false),
            (10, 25, true, false),
            (13, 15, true, true),
            (13, 20, true, false),
            (13, 25, true, false),
            (20, 25, false, false),
            (23, 25, false, false),
        ];

        for (idx, (smallest, biggest, overlap, contains_bound)) in cases.into_iter().enumerate() {
            let smallest_buf = [smallest];
            let smallest = InnerKey::from_inner_buf(&smallest_buf);
            let biggest_buf = [biggest];
            let biggest = InnerKey::from_inner_buf(&biggest_buf);
            let table_bound = DataBound::new(smallest, biggest, true);
            let shard_bound = meta.range.data_bound();
            assert_eq!(
                shard_bound.overlap_bound(table_bound),
                overlap,
                "case {}",
                idx
            );
            assert_eq!(
                shard_bound.contains_bound(table_bound),
                contains_bound,
                "case {}",
                idx
            );
        }
    }

    #[test]
    fn test_dedup_ingest_files() {
        let make_ingest_files = |l0: &[u64], ln: &[u64], blob: &[u64]| -> pb::IngestFiles {
            let mut ingest_files = pb::IngestFiles::default();
            for &id in l0 {
                ingest_files.mut_l0_creates().push(kvenginepb::L0Create {
                    id,
                    ..Default::default()
                });
            }
            for &id in ln {
                ingest_files
                    .mut_table_creates()
                    .push(kvenginepb::TableCreate {
                        id,
                        ..Default::default()
                    });
            }
            for &id in blob {
                ingest_files
                    .mut_blob_creates()
                    .push(kvenginepb::BlobCreate {
                        id,
                        ..Default::default()
                    });
            }
            ingest_files
        };

        {
            let meta = ShardMeta::default();
            let mut ingest_files = make_ingest_files(&[1], &[2], &[3]);
            let expected = ingest_files.clone();

            let is_empty = meta.dedup_ingest_files_of_load_data(&mut ingest_files);
            assert!(!is_empty);
            assert_eq!(ingest_files, expected);
        }

        {
            let files = (1..=7).map(|id| (id, FileMeta::default()));
            let meta = ShardMeta {
                files: HashMap::from_iter(files),
                ..Default::default()
            };
            let mut ingest_files = make_ingest_files(&[1, 2], &[3, 4], &[5, 6, 7]);
            let is_empty = meta.dedup_ingest_files_of_load_data(&mut ingest_files);
            assert!(is_empty);
            assert_eq!(ingest_files, pb::IngestFiles::default());
        }

        {
            let files = (1..=7).map(|id| (id, FileMeta::default()));
            let meta = ShardMeta {
                files: HashMap::from_iter(files),
                ..Default::default()
            };
            let mut ingest_files =
                make_ingest_files(&[1, 2, 10, 11], &[3, 4, 12], &[5, 6, 7, 13, 14]);
            let is_empty = meta.dedup_ingest_files_of_load_data(&mut ingest_files);
            assert!(!is_empty);
            let expected = make_ingest_files(&[10, 11], &[12], &[13, 14]);
            assert_eq!(ingest_files, expected);
        }
    }

    #[test]
    fn test_check_overlap_for_load_data() {
        let make_smallest_biggest = |t: &(usize, usize)| {
            (
                format!("{:04}", t.0).into_bytes(),
                format!("{:04}", t.1).into_bytes(),
            )
        };
        let make_ingest_files = |ln: &[(usize, usize)]| -> pb::IngestFiles {
            let mut ingest_files = pb::IngestFiles::default();
            for (id, t) in ln.iter().enumerate() {
                let (smallest, biggest) = make_smallest_biggest(t);
                ingest_files
                    .mut_table_creates()
                    .push(kvenginepb::TableCreate {
                        id: id as u64,
                        smallest,
                        biggest,
                        ..Default::default()
                    });
            }
            ingest_files
        };
        let make_meta = |files: &[(usize, usize)]| -> ShardMeta {
            let mut meta = ShardMeta::default();
            for (id, t) in files.iter().enumerate() {
                let (smallest, biggest) = make_smallest_biggest(t);
                meta.files.insert(
                    id as u64,
                    FileMeta::new(
                        WRITE_CF as i32,
                        3,
                        FileType::Sst,
                        &smallest,
                        &biggest,
                        0,
                        0,
                        None,
                    ),
                );
            }
            meta
        };

        let ingest_files = make_ingest_files(&[(1, 1), (2, 5), (10, 20)]);
        let cases = vec![
            (
                vec![(0, 0)], // existed files
                None,         // expected
            ),
            (vec![(0, 0), (1, 2)], Some((1, 0))),
            (vec![(0, 0), (6, 6), (8, 11)], Some((2, 2))),
            (vec![(0, 0), (6, 6), (7, 7), (30, 40)], None),
        ];

        for (idx, (existed_files, expected)) in cases.into_iter().enumerate() {
            let meta = make_meta(&existed_files);
            let overlap = meta.check_overlap_for_load_data(&ingest_files);
            assert_eq!(overlap, expected, "case {}", idx);
        }
    }

    #[test]
    fn test_merge_vector_index() {
        let make_vector_index_file =
            |id: u64, smallest: &[u8], biggest: &[u8]| -> kvenginepb::VectorIndexFile {
                let mut file = kvenginepb::VectorIndexFile::new();
                file.set_id(id);
                file.set_smallest(smallest.to_vec());
                file.set_biggest(biggest.to_vec());
                file.set_meta_offset(100);
                file
            };

        let make_vector_index = |table_id: i64,
                                 index_id: i64,
                                 col_id: i64,
                                 files: Vec<kvenginepb::VectorIndexFile>|
         -> VectorIndex {
            let mut vec_idx = VectorIndex::new();
            vec_idx.set_table_id(table_id);
            vec_idx.set_index_id(index_id);
            vec_idx.set_col_id(col_id);
            vec_idx.set_files(files.into());
            vec_idx
        };

        // Test case 1: Merge files into existing vector index
        {
            let mut meta = ShardMeta::default();

            // Add initial vector index with files [1, 2]
            let initial_files = vec![
                make_vector_index_file(1, b"key1", b"key2"),
                make_vector_index_file(2, b"key3", b"key4"),
            ];
            let initial_vec_idx = make_vector_index(100, 1, 10, initial_files);
            meta.vector_indexes.push(initial_vec_idx);

            // Merge new files [2, 3] (file 2 already exists, file 3 is new)
            let new_files = vec![
                make_vector_index_file(2, b"key3", b"key4"), // duplicate
                make_vector_index_file(3, b"key5", b"key6"), // new
            ];
            let new_vec_idx = make_vector_index(100, 1, 10, new_files);

            meta.merge_vector_index(&new_vec_idx);

            // Verify: should have 1 vector index with files [1, 2, 3]
            assert_eq!(meta.vector_indexes.len(), 1);
            let merged_idx = &meta.vector_indexes[0];
            assert_eq!(merged_idx.table_id, 100);
            assert_eq!(merged_idx.index_id, 1);
            assert_eq!(merged_idx.col_id, 10);
            assert_eq!(merged_idx.files.len(), 3);

            let file_ids: std::collections::HashSet<u64> =
                merged_idx.files.iter().map(|f| f.id).collect();
            assert!(file_ids.contains(&1));
            assert!(file_ids.contains(&2));
            assert!(file_ids.contains(&3));
        }

        // Test case 2: Add new vector index (no matching existing index)
        {
            let mut meta = ShardMeta::default();

            // Add initial vector index
            let initial_files = vec![make_vector_index_file(1, b"key1", b"key2")];
            let initial_vec_idx = make_vector_index(100, 1, 10, initial_files);
            meta.vector_indexes.push(initial_vec_idx);

            // Add new vector index with different table_id
            let new_files = vec![make_vector_index_file(4, b"key7", b"key8")];
            let new_vec_idx = make_vector_index(200, 1, 10, new_files);

            meta.merge_vector_index(&new_vec_idx);

            // Verify: should have 2 vector indexes
            assert_eq!(meta.vector_indexes.len(), 2);

            // Find the new index
            let new_idx = meta
                .vector_indexes
                .iter()
                .find(|idx| idx.table_id == 200)
                .unwrap();
            assert_eq!(new_idx.index_id, 1);
            assert_eq!(new_idx.col_id, 10);
            assert_eq!(new_idx.files.len(), 1);
            assert_eq!(new_idx.files[0].id, 4);
        }

        // Test case 3: Add new vector index with different index_id
        {
            let mut meta = ShardMeta::default();

            // Add initial vector index
            let initial_files = vec![make_vector_index_file(1, b"key1", b"key2")];
            let initial_vec_idx = make_vector_index(100, 1, 10, initial_files);
            meta.vector_indexes.push(initial_vec_idx);

            // Add new vector index with different index_id
            let new_files = vec![make_vector_index_file(5, b"key9", b"key10")];
            let new_vec_idx = make_vector_index(100, 2, 10, new_files);

            meta.merge_vector_index(&new_vec_idx);

            // Verify: should have 2 vector indexes
            assert_eq!(meta.vector_indexes.len(), 2);

            // Find the new index
            let new_idx = meta
                .vector_indexes
                .iter()
                .find(|idx| idx.index_id == 2)
                .unwrap();
            assert_eq!(new_idx.table_id, 100);
            assert_eq!(new_idx.col_id, 10);
            assert_eq!(new_idx.files.len(), 1);
            assert_eq!(new_idx.files[0].id, 5);
        }

        // Test case 4: Add new vector index with different col_id
        {
            let mut meta = ShardMeta::default();

            // Add initial vector index
            let initial_files = vec![make_vector_index_file(1, b"key1", b"key2")];
            let initial_vec_idx = make_vector_index(100, 1, 10, initial_files);
            meta.vector_indexes.push(initial_vec_idx);

            // Add new vector index with different col_id
            let new_files = vec![make_vector_index_file(6, b"key11", b"key12")];
            let new_vec_idx = make_vector_index(100, 1, 20, new_files);

            meta.merge_vector_index(&new_vec_idx);

            // Verify: should have 2 vector indexes
            assert_eq!(meta.vector_indexes.len(), 2);

            // Find the new index
            let new_idx = meta
                .vector_indexes
                .iter()
                .find(|idx| idx.col_id == 20)
                .unwrap();
            assert_eq!(new_idx.table_id, 100);
            assert_eq!(new_idx.index_id, 1);
            assert_eq!(new_idx.files.len(), 1);
            assert_eq!(new_idx.files[0].id, 6);
        }
    }
}
