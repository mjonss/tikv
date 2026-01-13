// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, collections::HashSet, convert::TryFrom};

use api_version;
use bytes::Bytes;
use codec::{buffer::BufferWriter, number::NumberEncoder};
use itertools::Itertools;
use schema::schema::{
    STORAGE_CLASS_SPEC_STR_AUTO, STORAGE_CLASS_TIER_IA, StorageClass, StorageClassSpec,
};
use serde::Deserialize;
use tikv_util::set_current_region;

use crate::{
    COLUMNAR_LEVELS, EXTRA_CF, IdVer, LOCK_CF, LevelHandler, MajorCompactionType, NUM_CFS,
    WRITE_CF,
    metrics::{
        ENGINE_OPEN_FILES, ENGINE_REGION_HUGE_L0_TABLE_BYTES_HISTOGRAM,
        ENGINE_REGION_HUGE_MEM_TABLE_BYTES_HISTOGRAM,
    },
    table::{BoundedDataSet, DataBound, InnerKey},
};

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct EngineStats {
    pub num_shards: usize,
    pub num_initial_flushed_shard: usize,
    pub num_active_shards: usize,
    pub num_compacting_shards: usize,
    pub num_pending_compaction_shards: usize,
    pub num_has_del_prefixes_shards: usize,
    pub num_inner_key_shards: usize,
    pub ready_destroy_range_shards: Vec<IdVer>,
    pub mem_tables_count: usize,
    pub mem_tables_size: u64,
    pub l0_tables_count: usize,
    pub l0_tables_size: u64,
    pub blob_tables_count: usize,
    pub in_use_blob_size: u64,
    pub total_blob_size: u64,
    pub partial_l0_count: usize,
    pub partial_blob_count: usize,
    pub partial_ln_count: usize,
    pub cfs_num_files: Vec<usize>,
    pub cf_total_sizes: Vec<u64>,
    pub level_num_files: Vec<usize>,
    pub level_total_sizes: Vec<u64>,
    pub index_size: u64,
    pub in_mem_index_size: u64,
    pub filter_size: u64,
    pub in_mem_filter_size: u64,
    pub open_files: i64,
    pub max_ts: u64, // Use to check whether PiTR has completed.
    pub entries: usize,
    pub old_entries: usize,
    pub tombs: usize,
    pub kv_size: u64,
    pub txn_file_locks: usize,
    pub columnar_levels: Vec<ColumnarLevelStats>,
    pub vector_indexes: VectorIndexStats,
    pub tops: TopStatCollection,
    pub ia: StorageClassStats,
}

impl EngineStats {
    pub fn new() -> Self {
        let mut stats = EngineStats::default();
        stats.cfs_num_files = vec![0; 3];
        stats.cf_total_sizes = vec![0; 3];
        stats.level_num_files = vec![0; 3];
        stats.level_total_sizes = vec![0; 3];
        stats
    }
}

#[derive(Default, Debug, Deserialize, Serialize)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ColumnarStatusResp {
    // Schema file already installed region count.
    pub ready: u64,
    // Vector index ready region count.
    pub vector_index_ready: u64,
    // Total region count.
    pub total: u64,
}

impl super::Engine {
    pub fn get_all_shard_stats(&self) -> Vec<ShardStats> {
        self.get_all_shard_stats_ext(None)
    }

    pub fn get_all_shard_stats_ext(&self, skip_shards: Option<&HashSet<IdVer>>) -> Vec<ShardStats> {
        self.get_all_shard_id_vers()
            .into_iter()
            .filter_map(|id_ver| {
                if skip_shards.is_some_and(|skip_shards| skip_shards.contains(&id_ver)) {
                    return None;
                }
                self.get_shard(id_ver.id).map(|shard| shard.get_stats())
            })
            .collect()
    }

    pub fn get_all_active_shard_stats_lite(&self) -> Vec<ShardStatsLite> {
        self.get_all_shard_id_vers()
            .into_iter()
            .filter_map(|id_ver| {
                self.get_shard(id_ver.id).and_then(|shard| {
                    if shard.is_active() {
                        Some(shard.get_stats().into())
                    } else {
                        None
                    }
                })
            })
            .collect()
    }

    pub fn get_shard_stat(&self, region_id: u64) -> ShardStats {
        self.get_shard_stat_opt(region_id).unwrap_or_default()
    }

    pub fn get_shard_stat_opt(&self, region_id: u64) -> Option<ShardStats> {
        self.get_shard(region_id).map(|shard| shard.get_stats())
    }

    pub fn get_engine_stats(&self, shard_stats: Vec<ShardStats>) -> EngineStats {
        let mut engine_stats = EngineStats::new();
        engine_stats.num_shards = shard_stats.len();
        engine_stats.open_files = self.fd_cache.size() as i64;
        engine_stats.columnar_levels = vec![ColumnarLevelStats::default(); COLUMNAR_LEVELS];
        for shard in &shard_stats {
            if shard.active {
                engine_stats.num_active_shards += 1;
            }
            if shard.compacting {
                engine_stats.num_compacting_shards += 1;
            } else if shard.compaction_score > 1.0 {
                engine_stats.num_pending_compaction_shards += 1;
            }
            if shard.flushed {
                engine_stats.num_initial_flushed_shard += 1;
            }
            if shard.has_del_prefixes {
                engine_stats.num_has_del_prefixes_shards += 1;
            }
            if shard.ready_to_destroy_range {
                engine_stats
                    .ready_destroy_range_shards
                    .push(IdVer::new(shard.id, shard.ver));
            }
            if shard.inner_key_off > 0 {
                engine_stats.num_inner_key_shards += 1;
            }
            engine_stats.mem_tables_count += shard.mem_table_count;
            engine_stats.mem_tables_size += shard.mem_table_size;
            engine_stats.l0_tables_count += shard.l0_table_count;
            engine_stats.l0_tables_size += shard.l0_table_size;
            engine_stats.blob_tables_count += shard.blob_table_count;
            engine_stats.in_use_blob_size += shard.in_use_blob_size;
            engine_stats.total_blob_size += shard.total_blob_size;
            engine_stats.partial_l0_count += shard.partial_l0s;
            engine_stats.partial_blob_count += shard.shared_blob_tables;
            engine_stats.partial_ln_count += shard.partial_tbls;
            engine_stats.index_size += shard.index_size;
            engine_stats.in_mem_index_size += shard.in_mem_index_size;
            engine_stats.filter_size += shard.filter_size;
            engine_stats.in_mem_filter_size += shard.in_mem_filter_size;
            engine_stats.max_ts = cmp::max(engine_stats.max_ts, shard.max_ts);
            engine_stats.entries += shard.entries;
            engine_stats.old_entries += shard.old_entries;
            engine_stats.tombs += shard.tombs;
            engine_stats.kv_size += shard.kv_size;
            let mut shard_file_count = shard.l0_table_count;
            for cf in 0..NUM_CFS {
                let shard_cf_stat = &shard.cfs[cf];
                for (i, level_stat) in shard_cf_stat.levels.iter().enumerate() {
                    engine_stats.level_num_files[i] += level_stat.num_tables;
                    engine_stats.cfs_num_files[cf] += level_stat.num_tables;
                    engine_stats.level_total_sizes[i] += level_stat.data_size;
                    engine_stats.cf_total_sizes[cf] += level_stat.data_size;
                    shard_file_count += level_stat.num_tables;
                }
            }
            engine_stats.txn_file_locks += shard.txn_file_locks;
            for (i, col_level) in shard.columnar_levels.iter().enumerate() {
                let engine_col_level = &mut engine_stats.columnar_levels[i];
                shard_file_count += col_level.num_files;
                engine_col_level.num_files += col_level.num_files;
                engine_col_level.data_size += col_level.data_size;
                engine_col_level.data_kv_size += col_level.data_kv_size;
            }
            engine_stats.vector_indexes.data_size += shard.vector_indexes.data_size;
            engine_stats.vector_indexes.num_files += shard.vector_indexes.num_files;
            shard_file_count += shard.vector_indexes.num_files;
            engine_stats.tops.collect(shard, shard_file_count);
            engine_stats.ia.merge(&shard.ia);
        }
        ENGINE_OPEN_FILES.set(engine_stats.open_files);
        engine_stats.tops.sort();
        engine_stats
    }

    pub fn update_region_huge_table_bytes_metrics(
        shard_stats: &Vec<ShardStats>,
        max_mem_table_size: u64,
    ) {
        if max_mem_table_size == 0 {
            return;
        }
        let region_huge_mem_table_bytes = ENGINE_REGION_HUGE_MEM_TABLE_BYTES_HISTOGRAM.local();
        let region_huge_l0_table_bytes = ENGINE_REGION_HUGE_L0_TABLE_BYTES_HISTOGRAM.local();
        for shard in shard_stats {
            if shard.mem_table_size > max_mem_table_size * 2 {
                region_huge_mem_table_bytes.observe(shard.mem_table_size as f64);
            }
            if shard.l0_table_size > max_mem_table_size * 2 {
                region_huge_l0_table_bytes.observe(shard.l0_table_size as f64);
            }
        }
        region_huge_mem_table_bytes.flush();
        region_huge_l0_table_bytes.flush();
    }

    fn append_table_record_prefix(prefix: &mut Vec<u8>, table_id: i64) {
        prefix.write_bytes(b"t").unwrap();
        prefix.write_i64(table_id).unwrap();
        prefix.write_bytes(b"_r").unwrap();
    }

    pub fn collect_columnar_status(
        &self,
        keyspace_id: u32,
        table_id: i64,
        index_id: Option<i64>, /* used for vector index, to be removed (use
                                * collect_columnar_index_stats instead) */
        shard_ids: Vec<u64>,
    ) -> ColumnarStatusResp {
        let mut ready = 0;
        let mut vector_index_ready = 0;
        let mut total = 0;
        let prefix = api_version::ApiV2::get_txn_keyspace_prefix(keyspace_id);
        let mut table_lower_key = prefix.clone();
        Self::append_table_record_prefix(&mut table_lower_key, table_id);
        let table_upper_key = keys::next_key(&table_lower_key);
        let table_bound = DataBound::new(
            InnerKey::from_outer_key(&table_lower_key),
            InnerKey::from_outer_end_key(&table_upper_key),
            false,
        );
        shard_ids.iter().for_each(|shard_id| {
            if let Some(shard) = self.get_shard(*shard_id) {
                if shard.keyspace_id == keyspace_id && shard.overlap_bound(table_bound) {
                    if shard.has_columnar_table(table_id) {
                        ready += 1;
                    }
                    if let Some(index_id) = index_id {
                        if shard.vector_index_ready(table_id, index_id) {
                            vector_index_ready += 1;
                        }
                    }
                    total += 1;
                }
            }
        });
        ColumnarStatusResp {
            ready,
            vector_index_ready,
            total,
        }
    }

    /// Returns the aggregated stats about columnar indexes for all shards.
    pub fn collect_columnar_index_stats(
        &self,
        keyspace_id: u32,
    ) -> crate::Result<Vec<super::ColumnarIndexStats>> {
        let mut res_map = super::ColumnarIndexStatsByTableIndex::default();
        if let Some(shard_ids) = self.get_keyspace_shards(keyspace_id) {
            for shard_id in shard_ids.iter() {
                if let Some(shard) = self.get_shard(*shard_id) {
                    if shard.is_active() {
                        set_current_region(*shard_id);
                        res_map.merge_from(shard.collect_columnar_index_stats()?);
                    }
                }
            }
        }
        // Just take out map values as a vector and return the vector.
        // We cannot return the map directly because map key is a tuple and cannot be
        // serialized.
        let mut res_vec: Vec<super::ColumnarIndexStats> = res_map.0.into_values().collect();
        res_vec.sort_by(|a, b| {
            a.table_id
                .cmp(&b.table_id)
                .then(a.index_id.cmp(&b.index_id))
        });
        Ok(res_vec)
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TopStatCollection {
    pub mem_size: TopStat,
    pub l0_size: TopStat,
    pub lock_cf_size: TopStat,
    pub extra_cf_size: TopStat,
    pub total_size: TopStat,
    pub columnar_size: TopStat,
    pub vector_size: TopStat,
    pub entries: TopStat,
    pub files_count: TopStat,
    pub columnar_tables_count: TopStat,
}

impl TopStatCollection {
    fn collect(&mut self, shard_stats: &ShardStats, files_count: usize) {
        let id = shard_stats.id;
        self.mem_size.add(id, shard_stats.mem_table_size);
        self.l0_size.add(id, shard_stats.l0_table_size);
        let lock_cf_size: u64 = shard_stats.cfs[LOCK_CF]
            .levels
            .iter()
            .map(|l| l.data_size)
            .sum();
        self.lock_cf_size.add(id, lock_cf_size);
        let extra_cf_size: u64 = shard_stats.cfs[EXTRA_CF]
            .levels
            .iter()
            .map(|l| l.data_size)
            .sum();
        self.extra_cf_size.add(id, extra_cf_size);
        self.total_size.add(id, shard_stats.total_size);
        let columnar_size = shard_stats
            .columnar_levels
            .iter()
            .map(|l| l.data_size)
            .sum();
        self.columnar_size.add(id, columnar_size);
        self.vector_size
            .add(id, shard_stats.vector_indexes.data_size);
        self.entries.add(id, shard_stats.entries as u64);
        self.files_count.add(id, files_count as u64);
        self.columnar_tables_count
            .add(id, shard_stats.columnar_tables as u64);
    }

    fn sort(&mut self) {
        self.mem_size.sort();
        self.l0_size.sort();
        self.lock_cf_size.sort();
        self.extra_cf_size.sort();
        self.total_size.sort();
        self.vector_size.sort();
        self.entries.sort();
        self.files_count.sort();
        self.columnar_tables_count.sort();
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TopStat {
    pub shards: Vec<ShardTopValue>,
    pub max_value: u64,
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ShardTopValue {
    id: u64,
    value: u64,
}

impl ShardTopValue {
    fn new(id: u64, value: u64) -> Self {
        Self { id, value }
    }
}

impl TopStat {
    fn add(&mut self, shard_id: u64, value: u64) {
        if value == 0 {
            return;
        }
        if self.shards.len() < 5 {
            self.shards.push(ShardTopValue::new(shard_id, value));
            if value > self.max_value {
                self.max_value = value;
            }
        } else if value > self.max_value {
            let pos = self.shards.iter().map(|v| v.value).position_min().unwrap();
            self.shards[pos] = ShardTopValue::new(shard_id, value);
            self.max_value = value;
        }
    }

    fn sort(&mut self) {
        self.shards.sort_by(|a, b| b.value.cmp(&a.value));
    }
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ShardStats {
    pub id: u64,
    pub ver: u64,
    pub keyspace: u32,
    #[serde(serialize_with = "serialize_bytes_as_hex")]
    pub start: Bytes,
    #[serde(serialize_with = "serialize_bytes_as_hex")]
    pub end: Bytes,
    pub inner_key_off: usize,
    pub active: bool,
    pub compacting: bool,
    pub flushed: bool,
    pub mem_table_count: usize,
    pub mem_table_size: u64,
    pub mem_table_unpersisted_props_size: usize,
    pub l0_table_count: usize,
    pub blob_table_count: usize,
    pub in_use_blob_size: u64,
    pub total_blob_size: u64,

    pub l0_table_size: u64,
    pub l0_cf_table_size: [u64; NUM_CFS],
    pub cfs: Vec<CfStats>,

    pub index_size: u64,
    pub in_mem_index_size: u64,
    pub filter_size: u64,
    pub in_mem_filter_size: u64,
    pub max_ts: u64,
    pub entries: usize,
    pub old_entries: usize,
    pub tombs: usize,
    /// Total kv size including all storage classes.
    pub kv_size: u64,
    pub base_version: u64,
    pub meta_sequence: u64,
    pub write_sequence: u64,
    // Total size of all SST files and blobs referenced by the shard (not blob table file size).
    pub total_size: u64,
    pub partial_l0s: usize,
    pub shared_blob_tables: usize,
    pub partial_tbls: usize,
    pub compaction_cf: isize,
    pub compaction_level: isize,
    pub compaction_score: f64,
    pub has_over_bound_data: bool,
    pub has_del_prefixes: bool,
    pub ready_to_destroy_range: bool,
    pub trim_over_bound: bool,
    pub manual_major_compaction: bool,
    #[serde(
        serialize_with = "serialize_storage_class_spec",
        deserialize_with = "deserialize_storage_class_spec"
    )]
    pub storage_class_spec: StorageClassSpec,
    pub ia: StorageClassStats,
    pub is_sync: bool,
    pub checked_schema_version: i64,
    // Txn File Stats
    pub txn_file_locks: usize,
    // Columnar Stats
    pub schema_version: i64,
    pub schema_restore_version: u64,
    pub columnar_tables: usize,
    pub unconverted_l0_count: usize,
    pub columnar_levels: Vec<ColumnarLevelStats>,
    pub vector_indexes: VectorIndexStats,
    pub encryption: bool,
}

impl ShardStats {
    #[inline]
    pub fn mem_table_is_empty(&self) -> bool {
        self.mem_table_size == 0 && self.mem_table_count == 1
    }

    pub fn for_each_cf_level(&self, mut f: impl FnMut((usize, &LevelStats))) {
        for (cf, cf_stats) in self.cfs.iter().enumerate() {
            cf_stats.levels.iter().for_each(|lv| {
                f((cf, lv));
            });
        }
    }
}

#[cfg(any(test, feature = "testexport"))]
impl ShardStats {
    pub fn is_major_compacted(&self) -> bool {
        let bottom_most_level = self.cfs[WRITE_CF].levels.last().unwrap();
        self.mem_table_size == 0
            && self.mem_table_count == 1
            && self.l0_table_count == 0
            && bottom_most_level.level == crate::CF_LEVELS[WRITE_CF]
            && bottom_most_level.num_tables != 0
    }

    pub fn write_cf_level_n_is_empty(&self) -> bool {
        self.cfs[WRITE_CF]
            .levels
            .iter()
            .all(|lv| lv.num_tables == 0)
    }
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ShardStatsLite {
    pub id: u64,
    pub ver: u64,
    pub start: Bytes,
    pub end: Bytes,
    pub inner_key_off: usize,
    // Total size of all SST files and blobs referenced by the shard (not blob table file size).
    pub total_size: u64,
    pub schema_version: i64,
    pub schema_restore_version: u64,
    pub write_sequence: u64,
    #[serde(
        serialize_with = "serialize_storage_class_spec",
        deserialize_with = "deserialize_storage_class_spec"
    )]
    pub storage_class_spec: StorageClassSpec,
    pub columnar_tables: usize,
}

impl From<ShardStats> for ShardStatsLite {
    fn from(s: ShardStats) -> Self {
        Self {
            id: s.id,
            ver: s.ver,
            start: s.start,
            end: s.end,
            inner_key_off: s.inner_key_off,
            total_size: s.total_size,
            schema_version: s.schema_version,
            schema_restore_version: s.schema_restore_version,
            write_sequence: s.write_sequence,
            storage_class_spec: s.storage_class_spec,
            columnar_tables: s.columnar_tables,
        }
    }
}

impl ShardStatsLite {
    pub fn with_schema(&self) -> bool {
        self.storage_class_spec.is_specified() || self.columnar_tables > 0
    }
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CfStats {
    pub levels: Vec<LevelStats>,
}

#[cfg(test)]
impl CfStats {
    pub(crate) fn data_size(&self) -> u64 {
        self.levels.iter().map(|l| l.data_size).sum()
    }
}

#[derive(Default, Serialize, Deserialize, Debug, Clone)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ColumnarLevelStats {
    pub num_files: usize,
    pub data_size: u64,
    pub data_kv_size: u64,
}

#[derive(Default, Serialize, Deserialize, Debug, Clone)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct VectorIndexStats {
    pub num_files: usize,
    pub data_size: u64,
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct LevelStatsLite {
    pub data_size: u64,
    pub blob_size: u64,
    pub entries: u64,
    // The followings are for WRITE_CF only.
    pub kv_size: u64, // Total kv size including all storage classes.
    pub ia_kv_size: u64,
    // The followings are for WRITE_CF & level 2+ only.
    pub lv2plus_max_ts: u64,
    pub lv2plus_tombs: u64,
    pub lv2plus_entries_write_cf: u64,
    // The followings are for LOCK_CF & level 1+ only.
    pub lv1plus_min_max_lock_ts: u64,
    // The following are for EXTRA_CF & level 1 only.
    pub lv1_min_max_extra_ts: u64,
    // The following is for WRITE_CF & EXTRA_CF
    pub max_ts: u64,
    // The following is for Columnar.
    pub columnar_size: u64,
    pub columnar_kv_size: u64, // Total kv size of columnar tables.
}

impl LevelStatsLite {
    pub fn add(&mut self, other: &LevelStatsLite, cf: usize) {
        self.data_size += other.data_size;
        self.blob_size += other.blob_size;
        self.entries += other.entries;
        if cf == WRITE_CF {
            self.kv_size += other.kv_size;
            self.ia_kv_size += other.ia_kv_size;
            self.lv2plus_max_ts = cmp::max(self.lv2plus_max_ts, other.lv2plus_max_ts);
            self.lv2plus_tombs += other.lv2plus_tombs;
            self.lv2plus_entries_write_cf += other.lv2plus_entries_write_cf;
            self.columnar_size += other.columnar_size;
            self.columnar_kv_size += other.columnar_kv_size;
        } else if cf == LOCK_CF {
            #[allow(clippy::collapsible_if)]
            if other.lv1plus_min_max_lock_ts > 0 {
                if self.lv1plus_min_max_lock_ts == 0
                    || self.lv1plus_min_max_lock_ts > other.lv1plus_min_max_lock_ts
                {
                    self.lv1plus_min_max_lock_ts = other.lv1plus_min_max_lock_ts;
                }
            }
        } else if cf == EXTRA_CF {
            #[allow(clippy::collapsible_if)]
            if other.lv1_min_max_extra_ts > 0 {
                if self.lv1_min_max_extra_ts == 0
                    || self.lv1_min_max_extra_ts > other.lv1_min_max_extra_ts
                {
                    self.lv1_min_max_extra_ts = other.lv1_min_max_extra_ts;
                }
            }
        }
        self.max_ts = max_ts_by_cf(self.max_ts, cf, other.max_ts);
    }
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct LevelStats {
    pub level: usize,
    pub num_tables: usize,
    pub data_size: u64,
    pub index_size: u64,
    pub in_mem_index_size: u64,
    pub filter_size: u64,
    pub in_mem_filter_size: u64,
    pub max_ts: u64,
    pub entries: usize,
    pub old_entries: usize,
    pub tombs: usize,
    pub kv_size: u64,
    pub in_use_blob_size: u64,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct StorageClassStats {
    /// Number of shards using the specified storage class.
    pub num_shards: usize,
    /// Number of tables (e.g. SsTables, not TiDB tables).
    pub num_tables: usize,
    /// Total size of the tables.
    pub data_size: u64,
    /// Total kv size (before discount).
    pub kv_size: u64,
}

impl StorageClassStats {
    pub fn merge(&mut self, other: &Self) {
        self.num_shards += other.num_shards;
        self.num_tables += other.num_tables;
        self.data_size += other.data_size;
        self.kv_size += other.kv_size;
    }
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ShardTruncateTsStats {
    pub id: u64,
    pub ver: u64,
    pub cur_max_ts: u64,
    pub truncate_ts: u64,
}

impl super::Shard {
    pub fn get_stats(&self) -> ShardStats {
        let mut total_size = 0;
        let mut tbl_index_size = 0;
        let mut in_mem_tbl_index_size = 0;
        let mut tbl_filter_size = 0;
        let mut in_mem_tbl_filter_size = 0;
        let mut max_ts = 0;
        let mut entries = 0;
        let mut old_entries = 0;
        let mut tombs = 0; // for WRITE_CF only
        let mut kv_size = 0; // for WRITE_CF only
        let data = self.get_data();
        let mem_table_count = data.mem_tbls.len();
        let mut mem_table_size = 0;
        let mut mem_table_unpersisted_props_size = 0;
        for mem_tbl in data.mem_tbls.as_slice() {
            mem_table_size += mem_tbl.size();
            mem_table_unpersisted_props_size = mem_tbl.unpersisted_props_size();
            max_ts = cmp::max(max_ts, mem_tbl.data_max_ts());
        }
        total_size += mem_table_size;
        let mut partial_l0s = 0;
        let mut shared_blob_tables = 0;
        let l0_table_count = data.l0_tbls.len();
        let mut l0_table_size = 0;
        let mut l0_cf_table_size = [0; NUM_CFS];
        let mut total_blob_size = 0;
        let mut in_use_blob_size = 0;
        let blob_table_count = data.blob_tbl_map.len();
        // FIXME: Calculate the total size of blob files.
        let mut blob_table_size = 0;
        let mut ia_stats = StorageClassStats::default();
        let pending_ops = self.pending_ops.read().unwrap().clone();
        let shard_bound = self.data_bound();
        for v in data.blob_tbl_map.values() {
            if shard_bound.contains_bound(v.data_bound()) {
                blob_table_size += v.size();
            } else {
                blob_table_size += v.size() / 2;
                shared_blob_tables += 1;
            }
        }
        total_blob_size += blob_table_size;
        for l0_tbl in data.l0_tbls.as_slice() {
            if shard_bound.contains_bound(l0_tbl.data_bound()) {
                l0_table_size += l0_tbl.size();
            } else {
                // TODO: estimate size by number of blocks in table.
                l0_table_size += l0_tbl.size() / 2;
                partial_l0s += 1;
            }
            for cf in 0..NUM_CFS {
                if let Some(cf_tbl) = l0_tbl.get_cf(cf) {
                    max_ts = max_ts_by_cf(max_ts, cf, cf_tbl.max_ts);
                    if cf == WRITE_CF {
                        cf_tbl.expire_cache(0);
                    }

                    if shard_bound.contains_bound(cf_tbl.data_bound()) {
                        tbl_index_size += cf_tbl.index_size();
                        tbl_filter_size += cf_tbl.filter_size();
                        in_mem_tbl_filter_size += cf_tbl.in_mem_filter_size();
                        entries += cf_tbl.entries as usize;
                        old_entries += cf_tbl.old_entries as usize;
                        if cf == WRITE_CF {
                            tombs += cf_tbl.tombs as usize;
                            kv_size += cf_tbl.kv_size;
                        }
                        in_use_blob_size += cf_tbl.total_blob_size();
                        l0_cf_table_size[cf] += cf_tbl.size();
                    } else {
                        tbl_index_size += cf_tbl.index_size() / 2;
                        tbl_filter_size += cf_tbl.filter_size() / 2;
                        in_mem_tbl_filter_size += cf_tbl.in_mem_filter_size() / 2;
                        entries += cf_tbl.entries as usize / 2;
                        old_entries += cf_tbl.old_entries as usize / 2;
                        if cf == WRITE_CF {
                            tombs += cf_tbl.tombs as usize / 2;
                            kv_size += cf_tbl.kv_size / 2;
                        }
                        in_use_blob_size += cf_tbl.total_blob_size() / 2;
                        l0_cf_table_size[cf] += cf_tbl.size() / 2;
                    }
                }
            }
        }
        total_size += l0_table_size;
        let mut partial_tbls = 0;
        let mut cfs = vec![];
        for cf in 0..NUM_CFS {
            let scf = data.get_cf(cf);
            let mut cf_stat = CfStats { levels: vec![] };
            for l in scf.levels.as_slice() {
                let mut level_stats = LevelStats::default();
                level_stats.level = l.level;
                level_stats.num_tables = l.tables.len();
                for t in l.tables.as_slice() {
                    t.expire_cache(l.level);
                    if shard_bound.contains_bound(t.data_bound()) {
                        level_stats.data_size += t.size();
                        level_stats.index_size += t.index_size();
                        level_stats.in_mem_index_size += t.in_mem_index_size();
                        level_stats.filter_size += t.filter_size();
                        level_stats.in_mem_filter_size += t.in_mem_filter_size();
                        level_stats.entries += t.entries as usize;
                        level_stats.old_entries += t.old_entries as usize;
                        if cf == WRITE_CF {
                            level_stats.tombs += t.tombs as usize;
                            level_stats.kv_size += t.kv_size;
                        }
                        level_stats.in_use_blob_size += t.total_blob_size();
                    } else {
                        level_stats.data_size += t.size() / 2;
                        level_stats.index_size += t.index_size() / 2;
                        level_stats.in_mem_index_size += t.in_mem_index_size() / 2;
                        level_stats.filter_size += t.filter_size() / 2;
                        level_stats.in_mem_filter_size += t.in_mem_filter_size() / 2;
                        level_stats.entries += t.entries as usize / 2;
                        level_stats.old_entries += t.old_entries as usize / 2;
                        if cf == WRITE_CF {
                            level_stats.tombs += t.tombs as usize / 2;
                            level_stats.kv_size += t.kv_size / 2;
                        }
                        level_stats.in_use_blob_size += t.total_blob_size() / 2;
                        partial_tbls += 1;
                    }
                    level_stats.max_ts = max_ts_by_cf(level_stats.max_ts, cf, t.max_ts);
                }
                total_size += level_stats.data_size;
                tbl_index_size += level_stats.index_size;
                in_mem_tbl_index_size += level_stats.in_mem_index_size;
                tbl_filter_size += level_stats.filter_size;
                in_mem_tbl_filter_size += level_stats.in_mem_filter_size;
                max_ts = cmp::max(max_ts, level_stats.max_ts);
                entries += level_stats.entries;
                old_entries += level_stats.old_entries;
                tombs += level_stats.tombs;
                kv_size += level_stats.kv_size;
                in_use_blob_size += level_stats.in_use_blob_size;
                if cf == WRITE_CF {
                    let level_ia_stats = self.calc_storage_class_stats_for_level(
                        &pending_ops.storage_class_spec,
                        l,
                        shard_bound,
                        &level_stats,
                    );
                    ia_stats.merge(&level_ia_stats);
                }
                cf_stat.levels.push(level_stats);
            }
            cfs.push(cf_stat);
        }
        total_size += in_use_blob_size;
        let priority = self.compaction_priority.read().unwrap().clone();
        let compaction_cf = priority.as_ref().map_or(0, |x| x.cf());
        let compaction_level = priority.as_ref().map_or(0, |x| x.level());
        let compaction_score = priority.as_ref().map_or(0f64, |x| x.score());
        let txn_file_locks = data.lock_txn_files.len();
        let schema_version = data.schema_version;
        let schema_restore_version = data.restore_version;
        let columnar_tables = data.columnar_table_ids.len();
        let unconverted_l0_count = data.col_levels.unconverted_l0s.len();
        let mut columnar_levels = vec![ColumnarLevelStats::default(); COLUMNAR_LEVELS];
        for (i, l) in data.col_levels.levels.iter().enumerate() {
            columnar_levels[i].num_files = l.files.len();
            columnar_levels[i].data_size = l.files.iter().map(|c| c.get_file().size()).sum();
            columnar_levels[i].data_kv_size = l
                .files
                .iter()
                .map(|c| c.get_estimated_kv_size() as u64)
                .sum();
        }
        let mut vector_indexes = VectorIndexStats::default();
        for vi in data.vector_indexes.get_all() {
            vector_indexes.num_files += vi.files.len();
            for f in &vi.files {
                vector_indexes.data_size += f.file_size();
            }
        }
        let sc_spec = pending_ops.storage_class_spec;
        ia_stats.num_shards = sc_spec.is_specified() as usize;
        ShardStats {
            id: self.id,
            ver: self.ver,
            keyspace: self.keyspace_id,
            start: self.outer_start.clone(),
            end: self.outer_end.clone(),
            inner_key_off: data.inner_key_off,
            active: self.is_active(),
            compacting: self.is_compacting(),
            flushed: self.get_initial_flushed(),
            mem_table_count,
            mem_table_size,
            mem_table_unpersisted_props_size,
            l0_table_count,
            blob_table_count,
            l0_table_size,
            l0_cf_table_size,
            total_blob_size,
            in_use_blob_size,
            cfs,
            base_version: self.get_base_version(),
            meta_sequence: self.get_meta_sequence(),
            write_sequence: self.get_write_sequence(),
            total_size,
            index_size: tbl_index_size,
            in_mem_index_size: in_mem_tbl_index_size,
            filter_size: tbl_filter_size,
            in_mem_filter_size: in_mem_tbl_filter_size,
            max_ts,
            entries,
            old_entries,
            tombs,
            kv_size,
            partial_l0s,
            shared_blob_tables,
            partial_tbls,
            compaction_cf,
            compaction_level,
            compaction_score,
            has_over_bound_data: data.has_over_bound_data(),
            has_del_prefixes: !pending_ops.del_prefixes.is_empty(),
            ready_to_destroy_range: Self::ready_to_destroy_range(&pending_ops.del_prefixes, &data),
            trim_over_bound: pending_ops.trim_over_bound,
            manual_major_compaction: pending_ops.manual_major_compaction
                != MajorCompactionType::Disable,
            storage_class_spec: sc_spec,
            ia: ia_stats,
            is_sync: data.is_sync(),
            checked_schema_version: self.get_checked_schema_ver(),
            txn_file_locks,
            schema_version,
            schema_restore_version,
            columnar_tables,
            unconverted_l0_count,
            columnar_levels,
            vector_indexes,
            encryption: self.is_encrypted(),
        }
    }

    fn calc_storage_class_stats_for_level(
        &self,
        sc_spec: &StorageClassSpec,
        level: &LevelHandler,
        shard_bound: DataBound<'_>,
        level_stats: &LevelStats,
    ) -> StorageClassStats {
        if sc_spec.must_be_ia() {
            StorageClassStats {
                num_tables: level_stats.num_tables,
                data_size: level_stats.data_size,
                kv_size: level_stats.kv_size,
                ..Default::default()
            }
        } else if sc_spec.can_be_ia() {
            let mut ia_stats = StorageClassStats::default();
            for t in level.tables.as_slice() {
                if t.is_storage_class_ia() {
                    ia_stats.num_tables += 1;
                    if shard_bound.contains_bound(t.data_bound()) {
                        ia_stats.data_size += t.size();
                        ia_stats.kv_size += t.kv_size;
                    } else {
                        ia_stats.data_size += t.size() / 2;
                        ia_stats.kv_size += t.kv_size / 2;
                    }
                }
            }
            ia_stats
        } else {
            StorageClassStats::default()
        }
    }
}

#[inline]
pub fn max_ts_by_cf(max_ts: u64, cf: usize, cf_max_ts: u64) -> u64 {
    // Ignore LOCK_CF, as `ts` in LOCK_CF is not a TSO.
    if (cf == WRITE_CF || cf == EXTRA_CF) && max_ts < cf_max_ts {
        cf_max_ts
    } else {
        max_ts
    }
}

pub fn serialize_bytes_as_hex<S>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let hex_str = log_wrappers::hex_encode_upper(bytes);
    serializer.serialize_str(&hex_str)
}

fn serialize_storage_class_spec<S>(
    spec: &StorageClassSpec,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if spec.must_be_ia() {
        serializer.serialize_str(STORAGE_CLASS_TIER_IA)
    } else if spec.can_be_ia() {
        serializer.serialize_str(STORAGE_CLASS_SPEC_STR_AUTO)
    } else {
        serializer.serialize_str("")
    }
}

fn deserialize_storage_class_spec<'de, D>(deserializer: D) -> Result<StorageClassSpec, D::Error>
where
    D: serde::Deserializer<'de>,
    D::Error: serde::de::Error,
{
    let spec_str: String = String::deserialize(deserializer)?;
    Ok(match spec_str.as_str() {
        STORAGE_CLASS_SPEC_STR_AUTO => {
            // Only used for schema manager to check schema (`ShardStats::with_schema`).
            // So just use "IA" to represent "Auto IA".
            StorageClass::Ia.into()
        }
        s => StorageClass::try_from(s).unwrap_or_default().into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LOCK_CF;

    #[test]
    fn test_max_ts_by_cf() {
        let cases = vec![
            (100, WRITE_CF, 200, 200),
            (100, LOCK_CF, 200, 100),
            (100, EXTRA_CF, 200, 200),
            (200, WRITE_CF, 100, 200),
            (200, LOCK_CF, 100, 200),
            (200, EXTRA_CF, 100, 200),
        ];
        for (max_ts, cf, cf_max_ts, expected) in cases {
            assert_eq!(max_ts_by_cf(max_ts, cf, cf_max_ts), expected,);
        }
    }

    #[test]
    fn test_level_stats_lite() {
        let mut stats1 = LevelStatsLite {
            data_size: 10000,
            blob_size: 20000,
            kv_size: 8000,
            columnar_kv_size: 1000,
            ia_kv_size: 800,
            entries: 2000,
            lv2plus_max_ts: 50,
            lv2plus_tombs: 1000,
            lv2plus_entries_write_cf: 1500,
            lv1plus_min_max_lock_ts: 80,
            lv1_min_max_extra_ts: 30,
            max_ts: 100,
            columnar_size: 30000,
        };
        let mut stats2 = stats1.clone();

        stats2.max_ts = 110;
        stats2.lv2plus_max_ts = 60;
        stats1.add(&stats2, WRITE_CF);
        assert_eq!(
            stats1,
            LevelStatsLite {
                data_size: 20000,
                blob_size: 40000,
                kv_size: 16000,
                columnar_kv_size: 2000,
                ia_kv_size: 1600,
                entries: 4000,
                lv2plus_max_ts: 60,
                lv2plus_tombs: 2000,
                lv2plus_entries_write_cf: 3000,
                lv1plus_min_max_lock_ts: 80,
                lv1_min_max_extra_ts: 30,
                max_ts: 110,
                columnar_size: 60000,
            }
        );

        stats2.max_ts = 120;
        stats2.lv2plus_max_ts = 70;
        stats2.lv1plus_min_max_lock_ts = 60;
        stats1.add(&stats2, LOCK_CF);
        assert_eq!(
            stats1,
            LevelStatsLite {
                data_size: 30000,
                blob_size: 60000,
                kv_size: 16000,         // unchanged
                columnar_kv_size: 2000, // unchanged
                ia_kv_size: 1600,       // unchanged
                entries: 6000,
                lv2plus_max_ts: 60,
                lv2plus_tombs: 2000,            // unchanged
                lv2plus_entries_write_cf: 3000, // unchanged
                lv1plus_min_max_lock_ts: 60,    // unchanged
                lv1_min_max_extra_ts: 30,       // unchanged
                max_ts: 110,                    // unchanged
                columnar_size: 60000,
            }
        );

        stats2.max_ts = 130;
        stats1.add(&stats2, EXTRA_CF);
        assert_eq!(
            stats1,
            LevelStatsLite {
                data_size: 40000,
                blob_size: 80000,
                kv_size: 16000,         // unchanged
                columnar_kv_size: 2000, // unchanged
                ia_kv_size: 1600,       // unchanged
                entries: 8000,
                lv2plus_max_ts: 60,             // unchanged
                lv2plus_tombs: 2000,            // unchanged
                lv2plus_entries_write_cf: 3000, // unchanged
                lv1plus_min_max_lock_ts: 60,    // unchanged
                lv1_min_max_extra_ts: 30,       // unchanged
                max_ts: 130,
                columnar_size: 60000,
            }
        );

        let mut stats3 = LevelStatsLite::default();
        stats3.columnar_size = 10000;
        stats1.add(&stats3, WRITE_CF);
        assert_eq!(
            stats1,
            LevelStatsLite {
                data_size: 40000,               // unchanged
                blob_size: 80000,               // unchanged
                kv_size: 16000,                 // unchanged
                columnar_kv_size: 2000,         // unchanged
                ia_kv_size: 1600,               // unchanged
                entries: 8000,                  // unchanged
                lv2plus_max_ts: 60,             // unchanged
                lv2plus_tombs: 2000,            // unchanged
                lv2plus_entries_write_cf: 3000, // unchanged
                lv1plus_min_max_lock_ts: 60,    // unchanged
                lv1_min_max_extra_ts: 30,       // unchanged
                max_ts: 130,                    // unchanged
                columnar_size: 70000,
            }
        );
    }
}
