// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use cloud_encryption::EncryptionKey;
use futures::{StreamExt, TryStreamExt, stream};
use tikv_util::sys::SysQuota;

use crate::table::{
    self, Error, Result,
    blobtable::blobtable::BlobTable,
    columnar::{Block, ColumnarReader},
    fts::{
        CommonPk, FtsDeltaCache, FtsIndexReader, IntPk, PkType,
        delta_cache::{FtsDeltaBuildSpec, FtsDeltaSource},
        level::FtsLevels,
    },
    schema_file::Schema,
};

const TEXT_COLUMN_IDX: usize = 0;
const SCORE_COLUMN_IDX: usize = 1;

/// Columnar reader that serves FTS index results. It runs the FTS index search
/// once, materializes handle/version (and optional score) into an in-memory
/// block, and then responds to `seek`/`read` from that cached block without
/// performing further I/O.
///
/// It requires output_schema = (NullableText, NullableScore). Other schema is
/// not valid. NullableText will be always filled with NULL. NullableScore will
/// be always filled with the score from FTS index results (which will never be
/// NULL).
pub struct FtsIndexColumnarReader {
    output_schema: Schema,
    query: clara_fts::Query,
    table_id: i64,
    read_ts: u64,
    fts_levels: Arc<FtsLevels>,
    delta_sources: Vec<FtsDeltaSource>,
    delta_build_specs: Vec<FtsDeltaBuildSpec>,
    fts_delta_cache: FtsDeltaCache,
    blob_tables: Arc<std::collections::HashMap<u64, BlobTable>>,
    encryption_key: Option<EncryptionKey>,
    start_handle: Option<Bytes>, // Inclusive
    end_handle: Option<Bytes>,   // Exclusive

    // Cached results
    loaded: bool,
    block: Block,
    idx: usize,
}

impl FtsIndexColumnarReader {
    pub fn new(
        output_schema: Schema,
        query: clara_fts::Query,
        table_id: i64,
        read_ts: u64,
        fts_levels: Arc<FtsLevels>,
        delta_sources: Vec<FtsDeltaSource>,
        delta_build_specs: Vec<FtsDeltaBuildSpec>,
        fts_delta_cache: FtsDeltaCache,
        blob_tables: Arc<std::collections::HashMap<u64, BlobTable>>,
        encryption_key: Option<EncryptionKey>,
        // Handle bounds for FTS index search.
        //
        // NOTE: int handles are encoded in *TiDB row-key* format (i64 memcomparable,
        // i.e. `IntPk::encode`), NOT the columnar in-block format (i64 little-endian).
        // Common handles are raw bytes.
        start_handle: Option<Bytes>, // Inclusive
        end_handle: Option<Bytes>,   // Exclusive
    ) -> Result<Self> {
        {
            if output_schema.columns.len() != 2 {
                return Err(Error::Other(format!(
                    "FtsIndexColumnarReader only supports 2 columns, got {}",
                    output_schema.columns.len()
                )));
            }
            super::check_fts_text_col(&output_schema.columns[TEXT_COLUMN_IDX], &query, true)
                .map_err(|e| Error::Other(e.to_string()))?;
            super::check_score_col(&output_schema.columns[SCORE_COLUMN_IDX], true)
                .map_err(|e| Error::Other(e.to_string()))?;
        }
        Ok(Self {
            block: Block::new(&output_schema),
            output_schema,
            query,
            table_id,
            read_ts,
            fts_levels,
            delta_sources,
            delta_build_specs,
            fts_delta_cache,
            blob_tables,
            encryption_key,
            start_handle,
            end_handle,
            loaded: false,
            idx: 0,
        })
    }

    async fn ensure_loaded(&mut self) -> Result<()> {
        if self.loaded {
            return Ok(());
        }

        // Lazily build delta indexes for immutable unindexed sources so the
        // first query pays the build cost only once (singleflight).
        if !self.delta_build_specs.is_empty() {
            let parser_type: String = self
                .output_schema
                .fulltext_indexes
                .iter()
                .find(|idx| idx.index_id == self.query.info().get_index_id())
                .map(|idx| idx.parser_type.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| self.query.info().get_query_tokenizer())
                .to_string();
            let schema = self.output_schema.clone();
            let blob_tables = Arc::clone(&self.blob_tables);
            let encryption_key = self.encryption_key.clone();

            let cache = self.fts_delta_cache.clone();
            let specs = std::mem::take(&mut self.delta_build_specs);
            let concurrency = (SysQuota::cpu_cores_quota() as usize).clamp(1, 8);
            let mut built: Vec<FtsDeltaSource> = stream::iter(specs)
                .map(|spec| {
                    let schema = schema.clone();
                    let parser_type = parser_type.clone();
                    let blob_tables = Arc::clone(&blob_tables);
                    let encryption_key = encryption_key.clone();
                    let cache = cache.clone();
                    async move {
                        let seq = spec.input.snap_version().into_inner();
                        let entry = cache
                            .get_or_build(spec, schema, &parser_type, blob_tables, encryption_key)
                            .await
                            .map_err(|e| Error::Other(e.to_string()))?;
                        Ok::<Option<FtsDeltaSource>, Error>(
                            entry.map(|entry| FtsDeltaSource { entry, seq }),
                        )
                    }
                })
                .buffer_unordered(concurrency)
                .try_filter_map(|x| async move { Ok(x) })
                .try_collect()
                .await?;
            self.delta_sources.append(&mut built);
        }

        let mut hits = if self.output_schema.is_common_handle() {
            FtsIndexReader::<CommonPk>::new(
                self.fts_levels.clone(),
                self.table_id,
                self.read_ts,
                self.start_handle.clone(),
                self.end_handle.clone(),
                self.query.clone(),
            )
            .search(&self.delta_sources)
            .await
            .map_err(|e| Error::Other(e.to_string()))?
        } else {
            FtsIndexReader::<IntPk>::new(
                self.fts_levels.clone(),
                self.table_id,
                self.read_ts,
                self.start_handle.clone(),
                self.end_handle.clone(),
                self.query.clone(),
            )
            .search(&self.delta_sources)
            .await
            .map_err(|e| Error::Other(e.to_string()))?
        };

        self.block.reset();

        if !hits.is_empty() {
            // Both common and int handles are encoded in memory-comparable order
            hits.sort_unstable_by(|a, b| a.pk.as_ref().cmp(b.pk.as_ref()));

            if self.output_schema.is_common_handle() {
                for hit in hits.iter() {
                    self.block.handles.push_value(&hit.pk);
                    self.block.versions.push_version(hit.version, false);
                    self.block.columns[TEXT_COLUMN_IDX].push_null();
                    let score_bytes = (hit.score as f64).to_le_bytes();
                    self.block.columns[SCORE_COLUMN_IDX].push_value(&score_bytes);
                }
            } else {
                for hit in hits.iter() {
                    let pk_int: i64 =
                        IntPk::decode(&hit.pk).map_err(|e| Error::Other(e.to_string()))?;
                    self.block.handles.push_value(&pk_int.to_le_bytes());
                    self.block.versions.push_version(hit.version, false);
                    self.block.columns[TEXT_COLUMN_IDX].push_null();
                    let score_bytes = (hit.score as f64).to_le_bytes();
                    self.block.columns[SCORE_COLUMN_IDX].push_value(&score_bytes);
                }
            }
        }

        self.idx = 0;
        self.loaded = true;
        Ok(())
    }
}

#[async_trait]
impl ColumnarReader for FtsIndexColumnarReader {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    async fn seek(&mut self, handle: &[u8]) -> table::Result<()> {
        self.ensure_loaded().await?;
        if handle.is_empty() {
            self.idx = 0;
            return Ok(());
        }
        self.idx = if self.output_schema.is_common_handle() {
            crate::table::search(self.block.length(), |mid| {
                self.block.handles.get_not_null_value(mid) >= handle
            })
        } else {
            let int_handle = (&handle[..]).get_i64_le();
            crate::table::search(self.block.length(), |mid| {
                self.block.handles.get_int_handle_value(mid) >= int_handle
            })
        };
        Ok(())
    }

    async fn read(&mut self, block: &mut Block, limit: usize) -> table::Result<usize> {
        self.ensure_loaded().await?;
        if self.idx >= self.block.length() {
            return Ok(0);
        }
        let read_end = std::cmp::min(self.idx + limit, self.block.length());
        block.append(&self.block, self.idx, read_end);
        let read_rows = read_end - self.idx;
        self.idx = read_end;
        Ok(read_rows)
    }

    fn reset(&mut self) -> table::Result<()> {
        self.idx = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use clara_fts::test_util::{PlainFtsQueryInfo, make_scored_query};
    use tidb_query_datatype::FieldTypeTp;

    use super::FtsIndexColumnarReader;
    use crate::table::{
        columnar::{Block, ColumnarReader},
        fts::{
            FtsDeltaCache, IntPk,
            level::FtsLevels,
            reader::FtsIndexReader,
            test_util::{SchemaBuilder, new_block, new_packed},
        },
    };

    const TABLE_ID: i64 = 808;
    const INDEX_ID: i64 = 9;
    const TEXT_COL_ID: i64 = 133;

    #[tokio::test]
    async fn index_columnar_fills_columns() {
        let mut levels = FtsLevels::default();
        levels.track_index(TABLE_ID, INDEX_ID);
        let file = new_packed(1, 5)
            .lp(TABLE_ID, INDEX_ID, |d| {
                d(5, 15, false, "alpha");
            })
            .finish_as_file();
        levels.mut_l0(|files| files.push(file));
        let levels = Arc::new(levels);

        let query = make_scored_query(&PlainFtsQueryInfo {
            query: "alpha".into(),
            column_id: TEXT_COL_ID,
            index_id: INDEX_ID,
            ..Default::default()
        });
        let schema = SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column(TEXT_COL_ID, FieldTypeTp::String)
            .fts_score_column(true)
            .schema();

        let hits =
            FtsIndexReader::<IntPk>::new(levels.clone(), TABLE_ID, 20, None, None, query.clone())
                .search(&[])
                .await
                .unwrap();
        assert_eq!(hits.len(), 1);

        let mut reader = FtsIndexColumnarReader::new(
            schema.clone(),
            query,
            TABLE_ID,
            20,
            levels,
            vec![],
            vec![],
            FtsDeltaCache::disabled(),
            Arc::new(std::collections::HashMap::new()),
            None,
            None,
            None,
        )
        .unwrap();
        let mut block = Block::new(&schema);
        let rows = reader.read(&mut block, 8).await.unwrap();
        assert_eq!(rows, 1);

        let expected = new_block::<IntPk>(&schema, |row| {
            row(5, 15, false, |datum| {
                datum.null();
                datum(hits[0].score as f64);
            });
        });
        assert!(block.eq(&expected));
    }
}
