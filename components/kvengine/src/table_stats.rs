// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use crate::table::{SnapVersion, columnar::VectorIndexDef};

/// Statistics about the columnar index coverage for a specific table and index
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ColumnarIndexStats {
    pub table_id: i64,
    pub index_id: i64,
    pub index_kind: String,

    /// Total number of rows in columnar files.
    /// Note: memtable and l0 sst key-values that are not turned into columnar
    /// format are not included.
    pub total_columnar_rows: usize,

    /// Total number of rows in columnar files which are not covered by index.
    pub unindexed_columnar_rows: usize,

    /// Total number of rows in columnar files which are covered by index.
    pub indexed_columnar_rows: usize,

    /// Number of unindexed columnar rows that will be indexed. Not all
    /// unindexed rows will be indexed, specifically, if the total size of
    /// unindexed rows is below the threshold for vector index building,
    /// these rows will not be indexed. See `Shard::need_build_vector_index`
    /// for more details.
    pub pending_columnar_rows: usize,
}

impl ColumnarIndexStats {
    pub fn merge_from(&mut self, other: ColumnarIndexStats) {
        self.total_columnar_rows += other.total_columnar_rows;
        self.unindexed_columnar_rows += other.unindexed_columnar_rows;
        self.indexed_columnar_rows += other.indexed_columnar_rows;
        self.pending_columnar_rows += other.pending_columnar_rows;
    }
}

#[derive(Debug, Default)]
pub struct ColumnarIndexStatsByTableIndex(
    pub HashMap<(/* table_id */ i64, /* index_id */ i64), ColumnarIndexStats>,
);

impl ColumnarIndexStatsByTableIndex {
    pub fn merge_from(&mut self, other: ColumnarIndexStatsByTableIndex) {
        for (key, stats) in other.0 {
            let entry = self.0.entry(key);
            entry
                .or_insert_with(|| ColumnarIndexStats {
                    table_id: stats.table_id,
                    index_id: stats.index_id,
                    index_kind: stats.index_kind.clone(),
                    total_columnar_rows: 0,
                    unindexed_columnar_rows: 0,
                    indexed_columnar_rows: 0,
                    pending_columnar_rows: 0,
                })
                .merge_from(stats);
        }
    }
}

impl super::Shard {
    /// Returns the stats about all columnar indexes for this shard.
    /// It may not return all columnar indexes in the Table schema if the schema
    /// is not fully synced.
    /// On the other hand, even if an index or table is deleted in TiDB, it may
    /// still be here.
    pub fn collect_columnar_index_stats(&self) -> crate::Result<ColumnarIndexStatsByTableIndex> {
        let mut result = ColumnarIndexStatsByTableIndex(HashMap::new());

        let data = self.get_data();
        let schema_file = data.schema_file.as_ref();
        let Some(schema_file) = schema_file else {
            return Ok(result);
        };

        // TODO: Update to support other indexes
        let mut table_indexes =
            HashMap::</* table_id */ i64, HashMap</* index_id */ i64, &VectorIndexDef>>::new();
        for (table_id, schema) in schema_file.iter_tables() {
            let entry = table_indexes.entry(*table_id).or_default();
            for idx in schema.vector_indexes.iter() {
                result.0.insert(
                    (*table_id, idx.index_id),
                    ColumnarIndexStats {
                        table_id: *table_id,
                        index_id: idx.index_id,
                        index_kind: "Vector".to_string(), /* Must be Vector or FullText to
                                                           * aligned with the ecosystem */
                        total_columnar_rows: 0,
                        unindexed_columnar_rows: 0,
                        indexed_columnar_rows: 0,
                        pending_columnar_rows: 0,
                    },
                );
                entry.insert(idx.index_id, idx);
            }
        }

        // snap_versions comes from existing vector indexes in the shard.
        // If a vector index is not built yet, it will not be in this map.
        let mut snap_versions =
            HashMap::<(/* table_id */ i64, /* index_id */ i64), SnapVersion>::new();
        for vec_idx in data.vector_indexes.iter() {
            snap_versions.insert((vec_idx.table_id, vec_idx.index_id), vec_idx.snap_version());
        }

        for col_level in &data.col_levels.levels {
            for file in &col_level.files {
                for &table_id in file.iter_tables() {
                    if !table_indexes.contains_key(&table_id) {
                        continue;
                    }
                    let table_meta = file.try_get_table(table_id)?;
                    let row_count = table_meta.handle_column.rows();
                    for &index_id in table_indexes[&table_id].keys() {
                        // For each index in the table, total_columnar_rows is the same.
                        let result_entry = result.0.get_mut(&(table_id, index_id)).unwrap();
                        result_entry.total_columnar_rows += row_count;

                        let snap_version = snap_versions
                            .get(&(table_id, index_id))
                            .copied()
                            .unwrap_or(SnapVersion::zero());
                        let mut is_indexed = false;
                        if snap_version.is_not_zero()
                            && (col_level.level == 2
                                || file.get_snap_version().unwrap_or_default() <= snap_version)
                        {
                            is_indexed = true;
                        }
                        if is_indexed {
                            result_entry.indexed_columnar_rows += row_count;
                        } else {
                            result_entry.unindexed_columnar_rows += row_count;
                        }
                    }
                }
            }
        }

        // Vector index specific:
        // Update pending_columnar_rows if unindexed_columnar_rows is above the
        // threshold for vector index building.
        for stats in result.0.values_mut() {
            if stats.unindexed_columnar_rows == 0 {
                continue;
            }
            let idx = table_indexes[&stats.table_id][&stats.index_id];
            let total_pending_size = stats.unindexed_columnar_rows * idx.dimension * 4;
            if total_pending_size >= self.opt.vector_index_build_options.delta_size {
                stats.pending_columnar_rows = stats.unindexed_columnar_rows;
            }
        }

        Ok(result)
    }
}
