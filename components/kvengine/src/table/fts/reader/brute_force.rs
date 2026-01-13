// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use clara_fts::BruteScoredSearcher;

use super::VIRTUAL_SCORE_COLUMN_ID;
use crate::table::{
    columnar::{Block, ColumnBuffer, ColumnarFilterReader},
    fts::util::{build_stringify_fn, empty_stringify_fn, StringifyFn},
    schema_file::{Schema, SchemaBuf},
    Error, Result,
};

pub struct FtsBruteForceReader<R> {
    output_schema: Schema,
    // `fts_col_idx` is used to find the fts_col in the schema.
    // it may be `None` if schema does not contain fts_col.
    fts_col_idx: Option<usize>,
    // `inner_reader` is the reader that actually reads fts_col,
    // it always reads the fts_col column and then score_col is calculated based on the result.
    inner_reader: R,
    // `string_fn` is used to convert the fts_col data to string.
    string_fn: StringifyFn,
    // `brute_searcher` is used to calculate the score for each row.
    brute_searcher: BruteScoredSearcher,
    query: clara_fts::Query,
    // `score_results` is used to store the score results for each row.
    score_results: Vec<clara_fts::ScoredResult>,
    // Temporary buffers reused between reads to avoid extra allocations.
    score_buffer: Vec<f32>,
    matched_buffer: Vec<bool>,
}

impl FtsBruteForceReader<()> {
    /// Generates a "inner" schema for reading physical data for FTS.
    ///
    /// - In FtsQueryTypeWithScore, last column is score_col, no need to read
    ///   physically, thus it will be removed from inner schema.
    /// - In both FtsQueryTypeWithScore and FtsQueryTypeWithNoScore, fts column
    ///   may be not included in the TableScan schema (if user query does not
    ///   actually need its value). However as we are doing FTS without an index
    ///   we still need the data of fts column. So fts will be added in inner
    ///   schema.
    ///
    /// IMPORTANT: schema and query must be first validated before calling this
    /// function.
    pub fn build_inner_schema(schema: &Schema, query: &clara_fts::Query) -> Result<Schema> {
        let text_col = &query.info().get_columns()[0];
        let has_text_col = schema
            .columns
            .iter()
            .any(|c| c.get_column_id() == text_col.get_column_id());

        let mut new_inner = (*schema.inner).clone();
        if query.info().get_query_type() == tipb::FtsQueryType::FtsQueryTypeWithScore {
            // In FtsQueryTypeWithScore, the last column is score_col, need to remove
            debug_assert_eq!(
                new_inner.columns.last().unwrap().get_column_id(),
                VIRTUAL_SCORE_COLUMN_ID
            );
            new_inner.columns.pop();
        }
        if !has_text_col {
            new_inner.columns.push(text_col.clone());
        }

        let buf = SchemaBuf {
            inner: Arc::new(new_inner),
            partitions: schema.partitions.clone(),
            table_id: schema.table_id,
            sc_spec: schema.sc_spec.clone(),
            is_sub_partition: schema.is_sub_partition,
        };
        Ok(Schema::new(buf))
    }
}

impl<R: ColumnarFilterReader> FtsBruteForceReader<R> {
    /// IMPORTANT: schema and query must be first validated before calling this
    /// function.
    pub fn new(inner_reader: R, schema: &Schema, query: &clara_fts::Query) -> Result<Self> {
        let inner_schema = inner_reader.get_schema();
        if inner_schema.columns.is_empty() {
            return Err(Error::Other(
                "FtsBruteForceReader inner schema must have at least 1 column".to_string(),
            ));
        }
        let fts_col_id = query.info().get_columns()[0].get_column_id();
        let fts_col_idx = schema
            .columns
            .iter()
            .position(|c| c.get_column_id() == fts_col_id);
        let fts_col_info = if let Some(idx) = fts_col_idx {
            &schema.columns[idx]
        } else {
            let col_info = inner_schema.columns.last().unwrap();
            if col_info.get_column_id() != fts_col_id {
                return Err(Error::Other(
                    "FtsBruteForceReader cannot find fts column in inner schema".to_string(),
                ));
            }
            col_info
        };

        let string_fn = build_stringify_fn(fts_col_info).unwrap_or_else(|_| empty_stringify_fn());
        let brute_searcher =
            BruteScoredSearcher::new(query).map_err(|e| Error::Other(e.to_string()))?;
        Ok(Self {
            output_schema: schema.clone(),
            fts_col_idx,
            inner_reader,
            string_fn,
            brute_searcher,
            query: query.clone(),
            score_results: Vec::new(),
            score_buffer: Vec::new(),
            matched_buffer: Vec::new(),
        })
    }

    /// `read_with_score` handles the case where fts_type is
    /// `FtsQueryTypeWithScore`.
    async fn read_with_score(
        &mut self,
        block: &mut Block,
        limit: usize,
    ) -> crate::table::Result<(usize /* read_row */, bool /* drained */)> {
        let mut score_col = block.columns.pop().unwrap();

        // If fts_col_idx is None, it means that there is no fts column in the schema,
        // so need to add a column in block to read the original data. When adding the
        // fts column, it will be added to the last column.
        if self.fts_col_idx.is_none() {
            let fts_col = ColumnBuffer::new_from_col_info(
                self.inner_reader.get_schema().columns.last().unwrap(),
            );
            block.columns.push(fts_col);
        }
        let (rows, drained) = self.inner_reader.try_read_block(block, limit).await?;

        let fts_col = if let Some(idx) = self.fts_col_idx {
            &block.columns[idx]
        } else {
            &block.columns[block.columns.len() - 1]
        };

        self.brute_searcher.clear();
        for row in 0..rows {
            if let Some(fts_data) = fts_col.get_value(row) {
                match (self.string_fn)(fts_data) {
                    Ok(fts_text) if !fts_text.is_empty() => {
                        self.brute_searcher.add_document(fts_text.as_ref())
                    }
                    _ => self.brute_searcher.add_null(),
                }
            } else {
                self.brute_searcher.add_null();
            }
        }

        self.score_results.clear();
        self.brute_searcher
            .search(&mut self.score_results)
            .map_err(|e| Error::Other(e.to_string()))?;

        self.score_buffer.clear();
        self.score_buffer.resize(rows, 0.0);
        for result in &self.score_results {
            self.score_buffer[result.doc_id as usize] = result.score;
        }
        for row in 0..rows {
            let score_f64 = self.score_buffer[row] as f64;
            score_col.push_value(&score_f64.to_le_bytes());
        }

        // When fts_col_idx is None, the last column of block.columns is the fts_col
        // which we added, and needs to be removed.
        if self.fts_col_idx.is_none() {
            block.columns.pop().unwrap();
        }
        block.columns.push(score_col);

        // Keep only rows that have matching scores
        block.retain_rows(|row_idx| self.score_buffer[row_idx] > 0.0);

        Ok((block.length(), drained))
    }

    /// `read_no_score` handles the case where fts_type is
    /// `FtsQueryTypeNoScore`.
    /// We need to match the read fts_col column and
    /// return only the matching rows.
    async fn read_no_score(
        &mut self,
        block: &mut Block,
        limit: usize,
    ) -> crate::table::Result<(usize /* read_row */, bool /* drained */)> {
        if self.fts_col_idx.is_none() {
            let fts_col_src = ColumnBuffer::new_from_col_info(
                self.inner_reader.get_schema().columns.last().unwrap(),
            );
            block.columns.push(fts_col_src);
        }
        let (rows, drained) = self.inner_reader.try_read_block(block, limit).await?;

        let fts_col = if let Some(idx) = self.fts_col_idx {
            &block.columns[idx]
        } else {
            &block.columns[block.columns.len() - 1]
        };

        self.brute_searcher.clear();
        for row in 0..rows {
            if let Some(fts_data) = fts_col.get_value(row) {
                match (self.string_fn)(fts_data) {
                    Ok(fts_text) if !fts_text.is_empty() => {
                        self.brute_searcher.add_document(fts_text.as_ref())
                    }
                    _ => self.brute_searcher.add_null(),
                }
            } else {
                self.brute_searcher.add_null();
            }
        }

        self.score_results.clear();
        self.brute_searcher
            .search(&mut self.score_results)
            .map_err(|e| Error::Other(e.to_string()))?;

        self.matched_buffer.clear();
        self.matched_buffer.resize(rows, false);
        for result in &self.score_results {
            self.matched_buffer[result.doc_id as usize] = true;
        }

        if self.fts_col_idx.is_none() {
            block.columns.pop().unwrap();
        }

        // Keep only rows that have matching scores
        let matched = &self.matched_buffer;
        block.retain_rows(|row_idx| matched[row_idx]);

        Ok((block.length(), drained))
    }
}

#[async_trait]
impl<R: ColumnarFilterReader> ColumnarFilterReader for FtsBruteForceReader<R> {
    #[inline]
    async fn set_handle_range(
        &mut self,
        start_handle: &[u8],
        end_handle: &[u8],
    ) -> crate::table::Result<()> {
        self.inner_reader
            .set_handle_range(start_handle, end_handle)
            .await
    }

    #[inline]
    async fn set_int_handle_range(
        &mut self,
        start_handle: i64,
        end_handle: Option<i64>,
    ) -> crate::table::Result<()> {
        self.inner_reader
            .set_int_handle_range(start_handle, end_handle)
            .await
    }

    // get schema of inner_reader
    #[inline]
    fn get_schema(&self) -> &Schema {
        &self.output_schema
    }

    #[inline]
    fn reset(&mut self) {
        self.brute_searcher.clear();
        self.score_results.clear();
        self.score_buffer.clear();
        self.matched_buffer.clear();
        self.inner_reader.reset();
    }

    #[inline]
    async fn prefetch_ia_remote_segments(
        &mut self,
        tag: &str,
        ia_mgr: &crate::ia::manager::IaManager,
        keyspace_id: u32,
        timeout: std::time::Duration,
    ) -> crate::table::Result<Option<f64>> {
        self.inner_reader
            .prefetch_ia_remote_segments(tag, ia_mgr, keyspace_id, timeout)
            .await
    }

    #[inline]
    async fn try_read_block(
        &mut self,
        block: &mut Block,
        limit: usize,
    ) -> crate::table::Result<(usize /* read_row */, bool /* drained */)> {
        if self.query.info().get_query_type() == tipb::FtsQueryType::FtsQueryTypeWithScore {
            self.read_with_score(block, limit).await
        } else {
            self.read_no_score(block, limit).await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use bytes::Buf;
    use clara_fts::test_util::{make_scored_query, make_unscored_query, PlainFtsQueryInfo};
    use tidb_query_datatype::FieldTypeTp;

    use super::*;
    use crate::table::{
        columnar::{
            ColumnarFile, ColumnarMergeReader, ColumnarMvccReader, ColumnarReader,
            ColumnarTableReader,
        },
        fts::{
            iter::{CommonPk, IntPk},
            test_util::{new_block, new_columnar, SchemaBuilder},
        },
        schema_file::Schema,
    };

    static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(10_000);

    fn mvcc_reader_from_file(
        file: ColumnarFile,
        schema: &Schema,
        read_ts: u64,
    ) -> ColumnarMvccReader {
        let table_reader = ColumnarTableReader::new(&file, schema.clone(), None, None);
        let readers: Vec<Box<dyn ColumnarReader>> = vec![Box::new(table_reader)];
        let merge_reader = ColumnarMergeReader::new(schema.clone(), readers);
        ColumnarMvccReader::new(Box::new(merge_reader), schema, read_ts)
    }

    #[tokio::test]
    async fn read_with_score_returns_scored_rows() {
        let table_id = 100;
        let table_schema = SchemaBuilder::<IntPk>::new(table_id)
            .column(1, FieldTypeTp::LongLong)
            .column(7, FieldTypeTp::String)
            .schema();
        let query_schema = SchemaBuilder::<IntPk>::new(table_id)
            .column(1, FieldTypeTp::LongLong)
            .column(7, FieldTypeTp::String)
            .fts_score_column(false)
            .schema();
        let query = make_scored_query(&PlainFtsQueryInfo {
            query: "rust".to_string(),
            column_id: 7,
            index_id: 1,
            ..Default::default()
        });
        let inner_schema = FtsBruteForceReader::build_inner_schema(&query_schema, &query).unwrap();
        let file_id = NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let columnar_file = new_columnar(file_id, 50)
            .table::<IntPk>(&table_schema, |row| {
                row(1, 10, false, |datum| {
                    datum(10);
                    datum("rust search");
                });
                row(2, 10, false, |datum| {
                    datum(20);
                    datum("plain text");
                });
                row(3, 10, false, |datum| {
                    datum(30);
                    datum("rust core");
                });
                row(4, 10, false, |datum| {
                    datum(40);
                    datum.null();
                });
                row(5, 10, false, |datum| {
                    datum(50);
                    datum("rust compute");
                });
            })
            .finish_as_file();
        let mvcc_reader = mvcc_reader_from_file(columnar_file, &inner_schema, 50);
        let mut reader = FtsBruteForceReader::new(mvcc_reader, &query_schema, &query).unwrap();
        reader.set_unbounded_handle_range().await.unwrap();

        let block = reader.read_all().await;
        assert_eq!(block.length(), 3);
        let expected = new_block::<IntPk>(&query_schema, |row| {
            row(1, 10, false, |datum| {
                datum(10);
                datum("rust search");
                datum(0.0);
            });
            row(3, 10, false, |datum| {
                datum(30);
                datum("rust core");
                datum(0.0);
            });
            row(5, 10, false, |datum| {
                datum(50);
                datum("rust compute");
                datum(0.0);
            });
        });
        assert!(block.cols_eq(&expected, true, &[1, 7]));

        let score_col = block.columns.last().unwrap();
        assert_eq!(score_col.col_id() as i64, VIRTUAL_SCORE_COLUMN_ID);
        for idx in 0..score_col.length() {
            assert!(score_col.get_value(idx).unwrap().get_f64_le() > 0.0);
        }
    }

    #[tokio::test]
    async fn read_no_score_filters_rows_without_score_column() {
        let table_id = 200;
        let table_schema = SchemaBuilder::<IntPk>::new(table_id)
            .column(1, FieldTypeTp::LongLong)
            .column(7, FieldTypeTp::String)
            .schema();
        let query_schema = SchemaBuilder::<IntPk>::new(table_id)
            .column(1, FieldTypeTp::LongLong)
            .column(7, FieldTypeTp::String)
            .schema();
        let query = make_unscored_query(&PlainFtsQueryInfo {
            query: "rust".to_string(),
            column_id: 7,
            index_id: 1,
            ..Default::default()
        });
        let inner_schema = FtsBruteForceReader::build_inner_schema(&query_schema, &query).unwrap();
        let file_id = NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let columnar_file = new_columnar(file_id, 50)
            .table::<IntPk>(&table_schema, |row| {
                row(1, 10, false, |datum| {
                    datum(10);
                    datum("rust search");
                });
                row(2, 10, false, |datum| {
                    datum(20);
                    datum("plain text");
                });
                row(3, 10, false, |datum| {
                    datum(30);
                    datum("rust core");
                });
                row(4, 10, false, |datum| {
                    datum(40);
                    datum.null();
                });
                row(5, 10, false, |datum| {
                    datum(50);
                    datum("rust compute");
                });
            })
            .finish_as_file();
        let mvcc_reader = mvcc_reader_from_file(columnar_file, &inner_schema, 50);
        let mut reader = FtsBruteForceReader::new(mvcc_reader, &query_schema, &query).unwrap();
        reader.set_unbounded_handle_range().await.unwrap();

        let block = reader.read_all().await;
        assert_eq!(block.length(), 3);
        let expected = new_block::<IntPk>(&query_schema, |row| {
            row(1, 10, false, |datum| {
                datum(10);
                datum("rust search");
            });
            row(3, 10, false, |datum| {
                datum(30);
                datum("rust core");
            });
            row(5, 10, false, |datum| {
                datum(50);
                datum("rust compute");
            });
        });
        assert!(block.eq(&expected));
    }

    #[tokio::test]
    async fn read_no_score_filters_when_query_schema_lacks_all_columns() {
        let table_id = 250;
        let table_schema = SchemaBuilder::<IntPk>::new(table_id)
            .column(1, FieldTypeTp::LongLong)
            .column(7, FieldTypeTp::String)
            .schema();
        let query_schema = SchemaBuilder::<IntPk>::new(table_id).schema();
        let query = make_unscored_query(&PlainFtsQueryInfo {
            query: "rust".to_string(),
            column_id: 7,
            index_id: 1,
            ..Default::default()
        });
        let inner_schema = FtsBruteForceReader::build_inner_schema(&query_schema, &query).unwrap();
        let file_id = NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let columnar_file = new_columnar(file_id, 50)
            .table::<IntPk>(&table_schema, |row| {
                row(1, 10, false, |datum| {
                    datum(10);
                    datum("rust search");
                });
                row(2, 10, false, |datum| {
                    datum(20);
                    datum("plain text");
                });
                row(3, 10, false, |datum| {
                    datum(30);
                    datum("rust core");
                });
            })
            .finish_as_file();
        let mvcc_reader = mvcc_reader_from_file(columnar_file, &inner_schema, 50);
        let mut reader = FtsBruteForceReader::new(mvcc_reader, &query_schema, &query).unwrap();
        reader.set_unbounded_handle_range().await.unwrap();

        let block = reader.read_all().await;
        assert_eq!(block.length(), 2);
        let expected = new_block::<IntPk>(&query_schema, |row| {
            row(1, 10, false, |_| {});
            row(3, 10, false, |_| {});
        });
        assert!(block.eq(&expected));
    }

    #[tokio::test]
    async fn read_no_score_filters_when_query_schema_lacks_text_column() {
        let table_id = 250;
        let table_schema = SchemaBuilder::<IntPk>::new(table_id)
            .column(1, FieldTypeTp::LongLong)
            .column(7, FieldTypeTp::String)
            .schema();
        let query_schema = SchemaBuilder::<IntPk>::new(table_id)
            .column(1, FieldTypeTp::LongLong)
            .schema();
        let query = make_unscored_query(&PlainFtsQueryInfo {
            query: "rust".to_string(),
            column_id: 7,
            index_id: 1,
            ..Default::default()
        });
        let inner_schema = FtsBruteForceReader::build_inner_schema(&query_schema, &query).unwrap();
        let file_id = NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let columnar_file = new_columnar(file_id, 50)
            .table::<IntPk>(&table_schema, |row| {
                row(1, 10, false, |datum| {
                    datum(10);
                    datum("rust search");
                });
                row(2, 10, false, |datum| {
                    datum(20);
                    datum("plain text");
                });
                row(3, 10, false, |datum| {
                    datum(30);
                    datum("rust core");
                });
            })
            .finish_as_file();
        let mvcc_reader = mvcc_reader_from_file(columnar_file, &inner_schema, 50);
        let mut reader = FtsBruteForceReader::new(mvcc_reader, &query_schema, &query).unwrap();
        reader.set_unbounded_handle_range().await.unwrap();

        let block = reader.read_all().await;
        assert_eq!(block.length(), 2);
        let expected = new_block::<IntPk>(&query_schema, |row| {
            row(1, 10, false, |datum| {
                datum(10);
            });
            row(3, 10, false, |datum| {
                datum(30);
            });
        });
        assert!(block.eq(&expected));
    }

    #[tokio::test]
    async fn read_with_score_filters_when_query_schema_lacks_text_column() {
        let table_id = 275;
        let table_schema = SchemaBuilder::<IntPk>::new(table_id)
            .column(1, FieldTypeTp::LongLong)
            .column(7, FieldTypeTp::String)
            .schema();
        let query_schema = SchemaBuilder::<IntPk>::new(table_id)
            .column(1, FieldTypeTp::LongLong)
            .fts_score_column(false)
            .schema();
        let query = make_scored_query(&PlainFtsQueryInfo {
            query: "rust".to_string(),
            column_id: 7,
            index_id: 1,
            ..Default::default()
        });
        let inner_schema = FtsBruteForceReader::build_inner_schema(&query_schema, &query).unwrap();
        let file_id = NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let columnar_file = new_columnar(file_id, 50)
            .table::<IntPk>(&table_schema, |row| {
                row(1, 10, false, |datum| {
                    datum(10);
                    datum("rust search");
                });
                row(2, 10, false, |datum| {
                    datum(20);
                    datum("plain text");
                });
                row(3, 10, false, |datum| {
                    datum(30);
                    datum("rust core");
                });
            })
            .finish_as_file();
        let mvcc_reader = mvcc_reader_from_file(columnar_file, &inner_schema, 50);
        let mut reader = FtsBruteForceReader::new(mvcc_reader, &query_schema, &query).unwrap();
        reader.set_unbounded_handle_range().await.unwrap();

        let block = reader.read_all().await;
        assert_eq!(block.length(), 2);
        let expected = new_block::<IntPk>(&query_schema, |row| {
            row(1, 10, false, |datum| {
                datum(10);
                datum(0.0);
            });
            row(3, 10, false, |datum| {
                datum(30);
                datum(0.0);
            });
        });
        assert!(block.cols_eq(&expected, true, &[1]));

        assert_eq!(block.columns.len(), 2);
        assert_eq!(block.columns[1].col_id() as i64, VIRTUAL_SCORE_COLUMN_ID);
    }

    #[tokio::test]
    async fn scored_query_all_filtered_returns_empty_block() {
        let table_id = 300;
        let table_schema = SchemaBuilder::<IntPk>::new(table_id)
            .column(1, FieldTypeTp::LongLong)
            .column(7, FieldTypeTp::String)
            .schema();
        let query_schema = SchemaBuilder::<IntPk>::new(table_id)
            .column(1, FieldTypeTp::LongLong)
            .column(7, FieldTypeTp::String)
            .fts_score_column(false)
            .schema();
        let query = make_scored_query(&PlainFtsQueryInfo {
            query: "nomatch".to_string(),
            column_id: 7,
            index_id: 1,
            ..Default::default()
        });
        let inner_schema = FtsBruteForceReader::build_inner_schema(&query_schema, &query).unwrap();
        let file_id = NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let columnar_file = new_columnar(file_id, 50)
            .table::<IntPk>(&table_schema, |row| {
                row(1, 10, false, |datum| {
                    datum(10);
                    datum("rust search");
                });
                row(2, 10, false, |datum| {
                    datum(20);
                    datum("plain text");
                });
                row(3, 10, false, |datum| {
                    datum(30);
                    datum("rust core");
                });
                row(4, 10, false, |datum| {
                    datum(40);
                    datum.null();
                });
                row(5, 10, false, |datum| {
                    datum(50);
                    datum("rust compute");
                });
            })
            .finish_as_file();
        let mvcc_reader = mvcc_reader_from_file(columnar_file, &inner_schema, 50);
        let mut reader = FtsBruteForceReader::new(mvcc_reader, &query_schema, &query).unwrap();
        reader.set_unbounded_handle_range().await.unwrap();

        let block = reader.read_all().await;
        assert!(block.eq(&Block::new(&query_schema)));
    }

    #[tokio::test]
    async fn handle_range_limits_int_handles() {
        let table_id = 400;
        let table_schema = SchemaBuilder::<IntPk>::new(table_id)
            .column(1, FieldTypeTp::LongLong)
            .column(7, FieldTypeTp::String)
            .schema();
        let query_schema = SchemaBuilder::<IntPk>::new(table_id)
            .column(1, FieldTypeTp::LongLong)
            .column(7, FieldTypeTp::String)
            .schema();
        let query = make_unscored_query(&PlainFtsQueryInfo {
            query: "rust".to_string(),
            column_id: 7,
            index_id: 1,
            ..Default::default()
        });
        let inner_schema = FtsBruteForceReader::build_inner_schema(&query_schema, &query).unwrap();
        let file_id = NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let columnar_file = new_columnar(file_id, 50)
            .table::<IntPk>(&table_schema, |row| {
                row(1, 10, false, |datum| {
                    datum(10);
                    datum("rust search");
                });
                row(2, 10, false, |datum| {
                    datum(20);
                    datum("plain text");
                });
                row(3, 10, false, |datum| {
                    datum(30);
                    datum("rust core");
                });
                row(4, 10, false, |datum| {
                    datum(40);
                    datum.null();
                });
                row(5, 10, false, |datum| {
                    datum(50);
                    datum("rust compute");
                });
            })
            .finish_as_file();
        let mvcc_reader = mvcc_reader_from_file(columnar_file, &inner_schema, 50);
        let mut reader = FtsBruteForceReader::new(mvcc_reader, &query_schema, &query).unwrap();
        reader.set_int_handle_range(2, Some(5)).await.unwrap();

        let block = reader.read_all().await;
        assert_eq!(block.length(), 1);
        let expected = new_block::<IntPk>(&query_schema, |row| {
            row(3, 10, false, |datum| {
                datum(30);
                datum("rust core");
            });
        });
        assert!(block.eq(&expected));
    }

    #[tokio::test]
    async fn handle_range_limits_common_handles() {
        let table_id = 500;
        let table_schema = SchemaBuilder::<CommonPk>::new(table_id)
            .column(1, FieldTypeTp::LongLong)
            .column(7, FieldTypeTp::String)
            .schema();
        let query_schema = SchemaBuilder::<CommonPk>::new(table_id)
            .column(1, FieldTypeTp::LongLong)
            .column(7, FieldTypeTp::String)
            .schema();
        let query = make_unscored_query(&PlainFtsQueryInfo {
            query: "rust".to_string(),
            column_id: 7,
            index_id: 1,
            ..Default::default()
        });
        let inner_schema = FtsBruteForceReader::build_inner_schema(&query_schema, &query).unwrap();
        let file_id = NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let columnar_file = new_columnar(file_id, 50)
            .table::<CommonPk>(&table_schema, |row| {
                row(b"00000001", 10, false, |datum| {
                    datum(11);
                    datum("rust alpha");
                });
                row(b"00000002", 10, false, |datum| {
                    datum(12);
                    datum("delta");
                });
                row(b"00000003", 10, false, |datum| {
                    datum(13);
                    datum("rust mid");
                });
                row(b"00000004", 10, false, |datum| {
                    datum(14);
                    datum.null();
                });
                row(b"00000005", 10, false, |datum| {
                    datum(15);
                    datum("rust omega");
                });
            })
            .finish_as_file();
        let mvcc_reader = mvcc_reader_from_file(columnar_file, &inner_schema, 50);
        let mut reader = FtsBruteForceReader::new(mvcc_reader, &query_schema, &query).unwrap();
        reader
            .set_handle_range(b"00000002", b"00000005")
            .await
            .unwrap();

        let block = reader.read_all().await;
        assert_eq!(block.length(), 1);
        let expected = new_block::<CommonPk>(&query_schema, |row| {
            row(b"00000003", 10, false, |datum| {
                datum(13);
                datum("rust mid");
            });
        });
        assert!(block.eq(&expected));
    }
}
