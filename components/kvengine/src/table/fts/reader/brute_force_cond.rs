// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::Ordering, collections::BinaryHeap};

use async_trait::async_trait;
use bytes::Buf;
use clara_fts::{BruteScoredSearcher, Query};
use tidb_query_datatype::{FieldTypeFlag, FieldTypeTp};
use tipb::ColumnInfo;

use crate::table::{
    Error, Result,
    columnar::{Block, ColumnarFilterReader},
    fts::util::{StringifyFn, build_stringify_fn, empty_stringify_fn},
    schema_file::{Schema, SchemaBuf},
};

const TEXT_COLUMN_IDX: usize = 0;
const SCORE_COLUMN_IDX: usize = 1;

/// We will only perform a TopK when the requested TopK is less than this limit.
/// If TopK is larger than this limit, we will not do any TopK, considering that
/// the overhead of maintaining the TopK heap may outweigh the benefits.
/// Maintaining a TopK heap requires us to transform column blocks into row-wise
/// entries, buffer all entries in the memory and finally transform them back
/// to column blocks. So it is not worth doing TopK for large K.
const TOPK_BUFFER_LIMIT: u32 = 50_000;

/// A conditional brute force reader that only performs brute force FTS on rows
/// whose score column is NULL using the text column. Rows with NULL or zero
/// score are filtered out.
///
/// This reader has the same output schema as the inner reader's schema. It
/// requires inner reader to have schema = (NullableText, NullableScore). Other
/// schema is not valid. This reader will always fill in the score column as not
/// NULL value (as NULL or zero score rows are filtered out).
pub struct FtsBruteForceCondReader<R> {
    inner: R,
    top_k: Option<u32>,
    query: Query,
    string_fn: StringifyFn,
    brute_searcher: BruteScoredSearcher,
    /// A reusable buffer to store brute search results.
    score_results: Vec<clara_fts::ScoredResult>,
    /// A reusable buffer to mark which rows to keep.
    keep_rows: Vec<bool>,

    // =================================================
    // Fields below are for TopK support:
    // When TopK < TOPK_BUFFER_LIMIT is requested, we buffer all qualified
    // rows in a min-heap and only propagate the best TopK rows out.
    // =================================================
    /// A min-heap that tracks the best top_k rows when buffering is needed.
    topk_heap: BinaryHeap<HeapRow>,
    /// Materialized TopK rows sorted by descending score once inner reader is
    /// drained.
    topk_results: Vec<HeapRow>,
    /// Current offset into `topk_results` when streaming them out.
    topk_emit_pos: usize,
    /// Whether the inner reader has been fully drained for the current range.
    topk_ready: bool,
}

impl FtsBruteForceCondReader<()> {
    /// Generates a "inner" schema for reading physical data required by this
    /// reader: (NullableText, NullableScore).
    ///
    /// IMPORTANT: query_schema and query must be first validated before calling
    /// this function.
    pub fn build_inner_schema(query_schema: &Schema, query: &Query) -> Schema {
        let opt_text_col = {
            let mut text_col = query.info().get_columns()[0].clone();
            let mut text_flag = FieldTypeFlag::from_bits_truncate(text_col.get_flag() as u32);
            text_flag.remove(FieldTypeFlag::NOT_NULL);
            text_col.set_flag(text_flag.bits() as i32);
            text_col
        };
        let opt_score_col = {
            if query.info().get_query_type() == tipb::FtsQueryType::FtsQueryTypeWithScore {
                let col = query_schema.columns.last().unwrap();
                debug_assert_eq!(col.get_column_id(), super::VIRTUAL_SCORE_COLUMN_ID);
                debug_assert_eq!(col.get_tp(), FieldTypeTp::Float as i32);
            }
            let mut col = ColumnInfo::new();
            col.set_column_id(super::VIRTUAL_SCORE_COLUMN_ID);
            col.set_tp(FieldTypeTp::Float as i32);
            col
        };
        Schema::new(SchemaBuf::new(
            query_schema.table_id,
            query_schema.handle_column.clone(),
            query_schema.version_column.clone(),
            vec![opt_text_col, opt_score_col],
            query_schema.pk_col_ids.clone(),
            query_schema.max_col_id,
            query_schema.vector_indexes.clone(), // As a query schema it should be null
            query_schema.fulltext_indexes.clone(), // As a query schema it should be null
            query_schema.sc_spec.clone(),
            query_schema.partitions.clone(),
        ))
    }
}

impl<R: ColumnarFilterReader> FtsBruteForceCondReader<R> {
    pub fn new(inner: R, query: &Query) -> Result<Self> {
        {
            let schema = inner.get_schema();
            if schema.columns.len() != 2 {
                return Err(Error::Other(format!(
                    "FtsBruteForceCondReader only supports 2 columns, got {}",
                    schema.columns.len()
                )));
            }
            super::check_fts_text_col(&schema.columns[TEXT_COLUMN_IDX], query, true)
                .map_err(|e| Error::Other(e.to_string()))?;
            super::check_score_col(&schema.columns[SCORE_COLUMN_IDX], true)
                .map_err(|e| Error::Other(e.to_string()))?;
        }
        let string_fn = build_stringify_fn(&inner.get_schema().columns[TEXT_COLUMN_IDX])
            .unwrap_or_else(|_| empty_stringify_fn());
        let brute_searcher =
            BruteScoredSearcher::new(query).map_err(|e| Error::Other(e.to_string()))?;
        let top_k = super::effective_top_k(query).map(|k| k as u32);
        Ok(Self {
            inner,
            top_k,
            query: query.clone(),
            string_fn,
            brute_searcher,
            score_results: Vec::new(),
            keep_rows: Vec::new(),

            topk_heap: BinaryHeap::new(),
            topk_results: Vec::new(),
            topk_emit_pos: 0,
            topk_ready: false,
        })
    }

    /// Given a block with possibly NULL scores, fill in the scores using
    /// brute force search on text column, and filter out rows with NULL or zero
    /// scores.
    fn transform_block(&mut self, block: &mut Block) -> Result<usize> {
        let rows = block.length();
        if rows == 0 {
            return Ok(0);
        }

        // Fast path: when global BM25 stats are prepared (from indexed sources),
        // score unindexed rows using the same weights so their scores are on the
        // same scale as indexed hits.
        if self.query.info().get_query_type() == tipb::FtsQueryType::FtsQueryTypeWithScore
            && self.query.prepared_bm25().is_some()
        {
            return self.transform_block_bm25_prepared(block);
        }

        self.transform_block_fallback(block)
    }

    fn transform_block_bm25_prepared(&mut self, block: &mut Block) -> Result<usize> {
        let rows = block.length();
        debug_assert!(rows > 0);

        let mut has_unindexed = false;
        {
            let score_col = &block.columns[SCORE_COLUMN_IDX];
            let text_col = &block.columns[TEXT_COLUMN_IDX];

            self.score_results.clear();
            for row in 0..rows {
                if !score_col.is_null(row) {
                    continue;
                }
                has_unindexed = true;
                let Some(fts_data) = text_col.get_value(row) else {
                    continue;
                };
                let Ok(fts_text) = (self.string_fn)(fts_data) else {
                    continue;
                };
                if fts_text.is_empty() {
                    continue;
                }
                let score = self.brute_searcher.score_document(fts_text.as_ref());
                if score > 0.0 {
                    self.score_results.push(clara_fts::ScoredResult {
                        doc_id: row as u32,
                        score,
                    });
                }
            }
        }
        if !has_unindexed {
            return Ok(block.length());
        }

        {
            let score_col = &mut block.columns[SCORE_COLUMN_IDX];

            if let Some(top_k) = self.top_k
                && self.score_results.len() > top_k as usize
            {
                self.score_results
                    .select_nth_unstable_by(top_k as usize, |a, b| {
                        b.score.partial_cmp(&a.score).unwrap()
                    });
                self.score_results.truncate(top_k as usize);
            }

            for result in &self.score_results {
                let row = result.doc_id as usize;
                // Update score column in place.
                score_col.nulls[row] = 0;
                let score_bytes = (result.score as f64).to_le_bytes();
                score_col
                    .mut_not_null_value(row)
                    .copy_from_slice(&score_bytes);
            }

            self.keep_rows.clear();
            self.keep_rows.resize(rows, true);
            for row in 0..rows {
                if score_col.is_null(row) {
                    self.keep_rows[row] = false;
                    continue;
                }
                let score = score_col.get_not_null_value(row).get_f64_le();
                if score <= 0.0 {
                    self.keep_rows[row] = false;
                }
            }
        }

        block.retain_rows(|row| self.keep_rows[row]);
        Ok(block.length())
    }

    fn transform_block_fallback(&mut self, block: &mut Block) -> Result<usize> {
        let rows = block.length();
        debug_assert!(rows > 0);
        let mut has_unindexed = false;
        {
            let score_col = &block.columns[SCORE_COLUMN_IDX];
            let text_col = &block.columns[TEXT_COLUMN_IDX];

            self.brute_searcher.clear();
            for row in 0..rows {
                if !score_col.is_null(row) {
                    self.brute_searcher.add_null(); // This makes DocId aligned with row index.
                    continue;
                }
                has_unindexed = true;
                if let Some(fts_data) = text_col.get_value(row) {
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
        }
        if !has_unindexed {
            return Ok(block.length());
        }
        {
            let score_col = &mut block.columns[SCORE_COLUMN_IDX];
            self.score_results.clear();
            self.brute_searcher
                .search(&mut self.score_results)
                .map_err(|e| Error::Other(e.to_string()))?;

            if let Some(top_k) = self.top_k
                && self.score_results.len() > top_k as usize
            {
                self.score_results
                    .select_nth_unstable_by(top_k as usize, |a, b| {
                        b.score.partial_cmp(&a.score).unwrap()
                    });
                self.score_results.truncate(top_k as usize);
            }

            for result in &self.score_results {
                let doc_id = result.doc_id as usize;
                // Update score column in place.
                score_col.nulls[doc_id] = 0;
                let score_bytes = (result.score as f64).to_le_bytes();
                score_col
                    .mut_not_null_value(doc_id)
                    .copy_from_slice(&score_bytes);
            }

            self.keep_rows.clear();
            self.keep_rows.resize(rows, true);
            for row in 0..rows {
                if score_col.is_null(row) {
                    self.keep_rows[row] = false;
                    continue;
                }
                let score = score_col.get_not_null_value(row).get_f64_le();
                if score <= 0.0 {
                    self.keep_rows[row] = false;
                }
            }
        }

        block.retain_rows(|row| self.keep_rows[row]);
        Ok(block.length())
    }

    fn reset_topk_state(&mut self) {
        self.topk_heap.clear();
        self.topk_results.clear();
        self.topk_emit_pos = 0;
        self.topk_ready = false;
    }

    #[inline]
    fn active_topk_limit(&self) -> Option<usize> {
        match self.top_k {
            Some(k) if k > 0 && k < TOPK_BUFFER_LIMIT => Some(k as usize),
            _ => None,
        }
    }

    fn feed_block_to_topk(&mut self, block: &Block, topk_limit: usize) {
        if topk_limit == 0 {
            return;
        }
        let score_col = &block.columns[SCORE_COLUMN_IDX];
        for row in 0..block.length() {
            if score_col.is_null(row) {
                continue;
            }
            let score = score_col.get_not_null_value(row).get_f64_le();
            if !score.is_finite() {
                continue;
            }

            if self.topk_heap.len() < topk_limit {
                let mut entry = HeapRow::default();
                entry.capture_from_block(block, row, score);
                self.topk_heap.push(entry);
            } else if let Some(mut weakest) = self.topk_heap.peek_mut() {
                // We only do column->row convert when we are sure the row can enter the TopK
                // heap.
                if score.total_cmp(&weakest.score) == Ordering::Greater {
                    weakest.capture_from_block(block, row, score);
                }
            }
        }
    }
}

#[async_trait]
impl<R: ColumnarFilterReader> ColumnarFilterReader for FtsBruteForceCondReader<R> {
    #[inline]
    async fn set_handle_range(&mut self, start_handle: &[u8], end_handle: &[u8]) -> Result<()> {
        self.reset_topk_state();
        self.inner.set_handle_range(start_handle, end_handle).await
    }

    #[inline]
    async fn set_int_handle_range(
        &mut self,
        start_handle: i64,
        end_handle: Option<i64>,
    ) -> Result<()> {
        self.reset_topk_state();
        self.inner
            .set_int_handle_range(start_handle, end_handle)
            .await
    }

    #[inline]
    fn get_schema(&self) -> &Schema {
        self.inner.get_schema()
    }

    #[inline]
    fn reset(&mut self) {
        self.reset_topk_state();
        self.inner.reset();
    }

    #[inline]
    async fn prefetch_ia_remote_segments(
        &mut self,
        tag: &str,
        ia_mgr: &crate::ia::manager::IaManager,
        keyspace_id: u32,
        timeout: std::time::Duration,
    ) -> crate::table::Result<Option<f64>> {
        self.inner
            .prefetch_ia_remote_segments(tag, ia_mgr, keyspace_id, timeout)
            .await
    }

    #[inline]
    async fn try_read_block(&mut self, block: &mut Block, limit: usize) -> Result<(usize, bool)> {
        // TODO: Currently there are double TopK when unindexed data are very few
        // (indexed part already satisfy TopK).
        if let Some(topk_limit) = self.active_topk_limit() {
            if !self.topk_ready {
                let read_limit = limit.max(1);
                loop {
                    let (read_rows, drained) = self.inner.try_read_block(block, read_limit).await?;
                    if read_rows > 0 {
                        let kept = self.transform_block(block)?;
                        if kept > 0 {
                            self.feed_block_to_topk(block, topk_limit);
                        }
                    }
                    if drained {
                        break;
                    }
                }
                self.topk_results.clear();
                self.topk_results.extend(self.topk_heap.drain());
                self.topk_emit_pos = 0;
                self.topk_ready = true;
                block.reset();
            }

            if self.topk_emit_pos >= self.topk_results.len() {
                return Ok((0, true));
            }

            block.reset();
            let remaining = self.topk_results.len() - self.topk_emit_pos;
            let emit = remaining.min(limit);
            for idx in 0..emit {
                self.topk_results[self.topk_emit_pos + idx].write_to_block(block);
            }
            self.topk_emit_pos += emit;
            let drained = self.topk_emit_pos >= self.topk_results.len();
            return Ok((emit, drained));
        }

        let (read_rows, drained) = self.inner.try_read_block(block, limit).await?;
        if read_rows == 0 {
            return Ok((0, drained));
        }
        let kept = self.transform_block(block)?;
        Ok((kept, drained))
    }
}

#[derive(Default)]
struct HeapRow {
    handle: Vec<u8>,
    version: u64,
    is_deleted: bool,
    text: Vec<u8>,
    text_is_null: bool,
    score: f64,
}

impl HeapRow {
    fn capture_from_block(&mut self, block: &Block, row_idx: usize, score: f64) {
        self.score = score;

        self.handle.clear();
        self.handle
            .extend_from_slice(block.handles.get_not_null_value(row_idx));

        self.version = block.versions.get_version(row_idx);
        self.is_deleted = block.versions.is_null(row_idx);

        let text_col = &block.columns[TEXT_COLUMN_IDX];
        if let Some(text) = text_col.get_value(row_idx) {
            self.text.clear();
            self.text.extend_from_slice(text);
            self.text_is_null = false;
        } else {
            self.text.clear();
            self.text_is_null = true;
        }
    }

    fn write_to_block(&self, block: &mut Block) {
        block.handles.push_value(&self.handle);
        block.versions.push_version(self.version, self.is_deleted);

        let text_col = &mut block.columns[TEXT_COLUMN_IDX];
        if self.text_is_null {
            text_col.push_null();
        } else {
            text_col.push_value(&self.text);
        }

        let score_col = &mut block.columns[SCORE_COLUMN_IDX];
        score_col.push_value(&self.score.to_le_bytes());
    }
}

impl PartialEq for HeapRow {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.handle == other.handle
    }
}

impl Eq for HeapRow {}

impl PartialOrd for HeapRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapRow {
    fn cmp(&self, other: &Self) -> Ordering {
        match other.score.total_cmp(&self.score) {
            Ordering::Equal => other.handle.cmp(&self.handle),
            ord => ord,
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Buf;
    use clara_fts::{
        index_for_test,
        test_util::{PlainFtsQueryInfo, make_scored_query, make_unscored_query},
    };
    use tidb_query_datatype::FieldTypeTp;

    use super::{FtsBruteForceCondReader, SCORE_COLUMN_IDX};
    use crate::table::{
        Error,
        columnar::{Block, ColumnarFilterReader, MockColumnarFilterReader},
        fts::{
            IntPk,
            reader::VIRTUAL_SCORE_COLUMN_ID,
            test_util::{SchemaBuilder, new_block},
        },
        schema_file::Schema,
    };

    const TABLE_ID: i64 = 100;
    const TEXT_COL_ID: i64 = 7;

    fn build_schema() -> Schema {
        SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column(TEXT_COL_ID, FieldTypeTp::String)
            .fts_score_column(true)
            .schema()
    }

    #[tokio::test]
    async fn fills_scores_and_filters_non_positive_rows() {
        let schema = build_schema();
        let block = new_block::<IntPk>(&schema, |row| {
            row(1, 10, false, |datum| {
                datum.null();
                datum(0.9);
            });
            row(2, 10, false, |datum| {
                datum.null();
                datum(0.0);
            });
            row(3, 9, false, |datum| {
                datum("rust search");
                datum.null();
            });
            row(4, 9, false, |datum| {
                datum("plain text");
                datum.null();
            });
            row(5, 8, false, |datum| {
                datum.null();
                datum.null();
            });
        });
        let mut reader = FtsBruteForceCondReader::new(
            MockColumnarFilterReader::new(schema.clone(), block),
            &make_scored_query(&PlainFtsQueryInfo {
                query: "rust".into(),
                column_id: TEXT_COL_ID,
                index_id: 1,
                ..Default::default()
            }),
        )
        .unwrap();

        let mut output = reader.read_all().await;
        assert_eq!(output.length(), 2);

        output.sort();
        let expected = new_block::<IntPk>(reader.get_schema(), |row| {
            row(1, 10, false, |datum| {
                datum.null();
                datum(0.9);
            });
            row(3, 9, false, |datum| {
                datum("rust search");
                datum(0.0);
            });
        });
        assert!(output.cols_eq(&expected, true, &[TEXT_COL_ID]));

        assert_eq!(
            reader.get_schema().columns[SCORE_COLUMN_IDX].get_column_id(),
            VIRTUAL_SCORE_COLUMN_ID
        );
        let score_col = &output.columns[SCORE_COLUMN_IDX];
        assert_eq!(score_col.get_not_null_value(0).get_f64_le(), 0.9);
        assert!(score_col.get_not_null_value(1).get_f64_le() > 0.0);
    }

    #[tokio::test]
    async fn fills_scores_for_unscored_queries() {
        let schema = build_schema();
        let block = new_block::<IntPk>(&schema, |row| {
            row(1, 10, false, |datum| {
                datum("rust match");
                datum.null();
            });
            row(2, 10, false, |datum| {
                datum("unrelated topic");
                datum.null();
            });
        });
        let mut reader = FtsBruteForceCondReader::new(
            MockColumnarFilterReader::new(schema.clone(), block),
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "rust".into(),
                column_id: TEXT_COL_ID,
                index_id: 1,
                ..Default::default()
            }),
        )
        .unwrap();

        let output = reader.read_all().await;
        assert_eq!(output.length(), 1);
        let expected = new_block::<IntPk>(reader.get_schema(), |row| {
            row(1, 10, false, |datum| {
                datum("rust match");
                datum(0.0);
            });
        });
        assert!(output.cols_eq(&expected, true, &[TEXT_COL_ID]));

        assert_eq!(
            reader.get_schema().columns[SCORE_COLUMN_IDX].get_column_id(),
            VIRTUAL_SCORE_COLUMN_ID
        );
        let score_col = &output.columns[SCORE_COLUMN_IDX];
        assert!(!score_col.is_null(0));
        assert!(score_col.get_not_null_value(0).get_f64_le() > 0.0);
    }

    #[tokio::test]
    async fn returns_zero_when_all_rows_filtered_out() {
        let schema = build_schema();
        let block = new_block::<IntPk>(&schema, |row| {
            row(1, 5, false, |datum| {
                datum("python only");
                datum.null();
            });
            row(2, 5, false, |datum| {
                datum.null();
                datum(0.0);
            });
        });
        let mut reader = FtsBruteForceCondReader::new(
            MockColumnarFilterReader::new(schema.clone(), block),
            &make_scored_query(&PlainFtsQueryInfo {
                query: "rust".into(),
                column_id: TEXT_COL_ID,
                index_id: 1,
                ..Default::default()
            }),
        )
        .unwrap();

        let output = reader.read_all().await;
        assert_eq!(output.length(), 0);
        let expected = Block::new(reader.get_schema());
        assert!(output.eq(&expected));
    }

    #[tokio::test]
    async fn mixed_prefilled_hits_and_misses() {
        let schema = build_schema();
        let block = new_block::<IntPk>(&schema, |row| {
            row(1, 11, false, |datum| {
                datum("already_scored");
                datum(0.5);
            });
            row(2, 11, false, |datum| {
                datum("bad score");
                datum(-0.2);
            });
            row(3, 11, false, |datum| {
                datum("rust journey");
                datum.null();
            });
            row(4, 11, false, |datum| {
                datum("python world");
                datum.null();
            });
            row(5, 11, false, |datum| {
                datum("rust ecosystem");
                datum.null();
            });
            row(6, 11, false, |datum| {
                datum("nothing relevant");
                datum.null();
            });
        });
        let mut reader = FtsBruteForceCondReader::new(
            MockColumnarFilterReader::new(schema.clone(), block),
            &make_scored_query(&PlainFtsQueryInfo {
                query: "rust".into(),
                column_id: TEXT_COL_ID,
                index_id: 1,
                ..Default::default()
            }),
        )
        .unwrap();

        let mut output = reader.read_all().await;
        assert_eq!(output.length(), 3);
        output.sort();
        let expected = new_block::<IntPk>(reader.get_schema(), |row| {
            row(1, 11, false, |datum| {
                datum("already_scored");
                datum(0.5);
            });
            row(3, 11, false, |datum| {
                datum("rust journey");
                datum(0.0);
            });
            row(5, 11, false, |datum| {
                datum("rust ecosystem");
                datum(0.0);
            });
        });
        assert!(output.cols_eq(&expected, true, &[TEXT_COL_ID]));

        let score_col = &output.columns[SCORE_COLUMN_IDX];
        assert_eq!(score_col.get_not_null_value(0).get_f64_le(), 0.5);
        assert!(score_col.get_not_null_value(1).get_f64_le() > 0.0);
        assert!(score_col.get_not_null_value(2).get_f64_le() > 0.0);
    }

    #[test]
    fn rejects_non_nullable_score_column() {
        let schema = SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column(TEXT_COL_ID, FieldTypeTp::String)
            .fts_score_column(false)
            .schema();
        let reader = MockColumnarFilterReader::new(schema.clone(), Block::new(&schema));
        let err = match FtsBruteForceCondReader::new(
            reader,
            &make_scored_query(&PlainFtsQueryInfo {
                query: "rust".into(),
                column_id: TEXT_COL_ID,
                index_id: 1,
                ..Default::default()
            }),
        ) {
            Ok(_) => panic!("expected schema validation error"),
            Err(err) => err,
        };
        match err {
            Error::Other(msg) => {
                assert!(
                    msg.contains("score column nullable=false"),
                    "unexpected error message: {}",
                    msg
                );
            }
            other => panic!("expected Error::Other, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn with_score_topk_limits_results() {
        let schema = build_schema();
        let block = new_block::<IntPk>(&schema, |row| {
            row(1, 20, false, |datum| {
                datum("rust alpha");
                datum(0.3);
            });
            row(2, 20, false, |datum| {
                datum("rust beta");
                datum(0.9);
            });
            row(3, 20, false, |datum| {
                datum("rust gamma");
                datum(0.8);
            });
            row(4, 20, false, |datum| {
                datum("rust delta");
                datum(1.5);
            });
            row(5, 20, false, |datum| {
                datum("rust epsilon");
                datum(0.6);
            });
        });
        let mut reader = FtsBruteForceCondReader::new(
            MockColumnarFilterReader::new(schema.clone(), block),
            &make_scored_query(&PlainFtsQueryInfo {
                query: "rust".into(),
                column_id: TEXT_COL_ID,
                index_id: 1,
                top_k: 3,
                ..Default::default()
            }),
        )
        .unwrap();

        let mut output = reader.read_all().await;
        assert_eq!(output.length(), 3);

        output.sort();
        let expected = new_block::<IntPk>(reader.get_schema(), |row| {
            row(2, 20, false, |datum| {
                datum("rust beta");
                datum(0.9);
            });
            row(3, 20, false, |datum| {
                datum("rust gamma");
                datum(0.8);
            });
            row(4, 20, false, |datum| {
                datum("rust delta");
                datum(1.5);
            });
        });
        assert!(output.eq(&expected));
    }

    #[tokio::test]
    async fn zero_top_k_means_unlimited() {
        let schema = build_schema();
        let block = new_block::<IntPk>(&schema, |row| {
            row(1, 20, false, |datum| {
                datum("rust alpha");
                datum(0.3);
            });
            row(2, 20, false, |datum| {
                datum("rust beta");
                datum(0.9);
            });
            row(3, 20, false, |datum| {
                datum("rust gamma");
                datum(0.8);
            });
        });
        let mut reader = FtsBruteForceCondReader::new(
            MockColumnarFilterReader::new(schema.clone(), block),
            &make_scored_query(&PlainFtsQueryInfo {
                query: "rust".into(),
                column_id: TEXT_COL_ID,
                index_id: 1,
                top_k: 0,
                ..Default::default()
            }),
        )
        .unwrap();

        let output = reader.read_all().await;
        assert_eq!(output.length(), 3);
    }

    #[tokio::test]
    async fn topk_respects_limit_across_calls() {
        let schema = build_schema();
        let block = new_block::<IntPk>(&schema, |row| {
            row(1, 20, false, |datum| {
                datum("rust alpha");
                datum(0.3);
            });
            row(2, 20, false, |datum| {
                datum("rust beta");
                datum(0.9);
            });
            row(3, 20, false, |datum| {
                datum("rust gamma");
                datum(0.8);
            });
            row(4, 20, false, |datum| {
                datum("rust delta");
                datum(1.5);
            });
            row(5, 20, false, |datum| {
                datum("rust epsilon");
                datum(0.6);
            });
        });
        let mut reader = FtsBruteForceCondReader::new(
            MockColumnarFilterReader::new(schema.clone(), block),
            &make_scored_query(&PlainFtsQueryInfo {
                query: "rust".into(),
                column_id: TEXT_COL_ID,
                index_id: 1,
                top_k: 3,
                ..Default::default()
            }),
        )
        .unwrap();

        let mut output = reader.read_all().await;
        assert_eq!(output.length(), 3);
        output.sort();
        let expected = new_block::<IntPk>(reader.get_schema(), |row| {
            row(2, 20, false, |datum| {
                datum("rust beta");
                datum(0.9);
            });
            row(3, 20, false, |datum| {
                datum("rust gamma");
                datum(0.8);
            });
            row(4, 20, false, |datum| {
                datum("rust delta");
                datum(1.5);
            });
        });
        assert!(output.eq(&expected));
    }

    /// A simple `ColumnarFilterReader` mock that returns multiple prebuilt
    /// blocks in sequence.
    struct MultiBlockMockReader {
        schema: Schema,
        blocks: Vec<Block>,
        pos: usize,
    }

    impl MultiBlockMockReader {
        fn new(schema: Schema, blocks: Vec<Block>) -> Self {
            Self {
                schema,
                blocks,
                pos: 0,
            }
        }
    }

    #[async_trait::async_trait]
    impl ColumnarFilterReader for MultiBlockMockReader {
        async fn set_handle_range(
            &mut self,
            _start_handle: &[u8],
            _end_handle: &[u8],
        ) -> crate::table::Result<()> {
            Ok(())
        }

        async fn set_int_handle_range(
            &mut self,
            _start_handle: i64,
            _end_handle: Option<i64>,
        ) -> crate::table::Result<()> {
            Ok(())
        }

        fn get_schema(&self) -> &Schema {
            &self.schema
        }

        fn reset(&mut self) {
            self.pos = 0;
        }

        async fn prefetch_ia_remote_segments(
            &mut self,
            _tag: &str,
            _ia_mgr: &crate::ia::manager::IaManager,
            _keyspace_id: u32,
            _timeout: std::time::Duration,
        ) -> crate::table::Result<Option<f64>> {
            Ok(None)
        }

        async fn try_read_block(
            &mut self,
            block: &mut Block,
            _limit: usize,
        ) -> crate::table::Result<(usize, bool)> {
            block.reset();
            if self.pos >= self.blocks.len() {
                return Ok((0, true));
            }
            let data = &self.blocks[self.pos];
            self.pos += 1;
            let rows = data.length();
            block.append(data, 0, rows);
            Ok((rows, self.pos >= self.blocks.len()))
        }
    }

    #[tokio::test]
    async fn prepared_scores_are_block_invariant() {
        let schema = build_schema();

        let query = make_scored_query(&PlainFtsQueryInfo {
            query: "rust".into(),
            column_id: TEXT_COL_ID,
            index_id: 1,
            ..Default::default()
        });

        // Prepare BM25 stats from an arbitrary non-empty stats corpus.
        let idx = index_for_test(&["rust stats", "other"] as &[&str])
            .unwrap()
            .finalize()
            .unwrap();
        let stats_reader = clara_fts::IndexReader::from_tantivy_index(idx).unwrap();
        let mut stats = clara_fts::Bm25Stats::empty(&query);
        stats_reader
            .accumulate_bm25_stats(&query, &mut stats)
            .unwrap();
        assert!(query.prepare_bm25_once(stats).unwrap());

        let block_all = new_block::<IntPk>(&schema, |row| {
            row(1, 10, false, |datum| {
                datum("rust rust");
                datum.null();
            });
            row(2, 10, false, |datum| {
                datum("rust rust rust");
                datum.null();
            });
            row(3, 10, false, |datum| {
                datum("rust");
                datum.null();
            });
            row(4, 10, false, |datum| {
                datum("python");
                datum.null();
            });
        });

        let block_1 = new_block::<IntPk>(&schema, |row| {
            row(1, 10, false, |datum| {
                datum("rust rust");
                datum.null();
            });
            row(2, 10, false, |datum| {
                datum("rust rust rust");
                datum.null();
            });
        });
        let block_2 = new_block::<IntPk>(&schema, |row| {
            row(3, 10, false, |datum| {
                datum("rust");
                datum.null();
            });
            row(4, 10, false, |datum| {
                datum("python");
                datum.null();
            });
        });

        let mut reader_all = FtsBruteForceCondReader::new(
            MockColumnarFilterReader::new(schema.clone(), block_all),
            &query,
        )
        .unwrap();
        let mut reader_split = FtsBruteForceCondReader::new(
            MultiBlockMockReader::new(schema.clone(), vec![block_1, block_2]),
            &query,
        )
        .unwrap();

        let mut out_all = reader_all.read_all().await;
        let mut out_split = reader_split.read_all().await;
        out_all.sort();
        out_split.sort();

        assert_eq!(out_all.length(), out_split.length());
        assert_eq!(out_all.length(), 3, "python should be filtered out");

        let score_all = &out_all.columns[SCORE_COLUMN_IDX];
        let score_split = &out_split.columns[SCORE_COLUMN_IDX];
        for row in 0..out_all.length() {
            let handle_all = out_all.handles.get_int_handle_value(row);
            let handle_split = out_split.handles.get_int_handle_value(row);
            assert_eq!(handle_all, handle_split);

            let s1 = score_all.get_not_null_value(row).get_f64_le();
            let s2 = score_split.get_not_null_value(row).get_f64_le();
            assert!(
                (s1 - s2).abs() < 1e-6,
                "handle={} score mismatch: {} vs {}",
                handle_all,
                s1,
                s2
            );
            assert!(s1.is_finite() && s1 > 0.0);
        }
    }
}
