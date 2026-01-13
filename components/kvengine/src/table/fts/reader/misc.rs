// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use anyhow::bail;
use async_trait::async_trait;
use clara_fts::Query;
use tidb_query_datatype::{FieldTypeAccessor, FieldTypeFlag, FieldTypeTp};
use tipb::ColumnInfo;

use crate::table::{
    Error, Result,
    columnar::{Block, ColumnarFilterReader},
    schema_file::{Schema, SchemaBuf},
};

/// Column id reserved for the virtual score column appended when
/// `FtsQueryTypeWithScore` is requested. The column is expected to be the
/// last entry in the output schema with type `Float` and `NOT NULL`.
pub const VIRTUAL_SCORE_COLUMN_ID: i64 = -2050;

/// Wraps a `tipb::FtsQueryInfo` into a `clara_fts::Query`, performing basic
/// validation on query type, index id and the first column definition so that
/// later FTS code can assume these invariants hold.
pub fn wrap_fts_pb(info: tipb::FtsQueryInfo) -> anyhow::Result<Query> {
    match info.get_query_type() {
        tipb::FtsQueryType::FtsQueryTypeWithScore | tipb::FtsQueryType::FtsQueryTypeNoScore => {
            // Pass
        }
        _ => {
            bail!(
                "unsupported fts_query.query_type {:?}",
                info.get_query_type()
            );
        }
    }
    if info.get_columns().is_empty() {
        bail!("unexpected empty fts_query_info.columns");
    }
    if !info.has_index_id() {
        bail!("missing fts_query.index_id");
    }
    let fts_col = &info.get_columns()[0];
    if !fts_col.has_column_id() {
        bail!("missing fts_query.column.column_id");
    }
    if !fts_col.as_accessor().is_string_like() {
        bail!(
            "fts_query.column.tp must be string, but got {}",
            fts_col.get_tp()
        );
    }
    Query::new(info)
}

/// Ensures the provided logical `Schema` matches requirements implied by the
/// FTS query:
/// - For `FtsQueryTypeNoScore`, the schema must NOT contain the virtual score
///   column.
/// - For `FtsQueryTypeWithScore`, the last column must be the virtual score
///   column with type `Float` and `NOT NULL`.
pub fn validate_schema(schema: &Schema, query: &Query) -> anyhow::Result<()> {
    #[cfg(debug_assertions)]
    if query.info().get_query_type() == tipb::FtsQueryType::FtsQueryTypeNoScore {
        // FtsQueryTypeNoScore should not contain score column in schema
        if schema
            .columns
            .iter()
            .any(|c| c.get_column_id() == VIRTUAL_SCORE_COLUMN_ID)
        {
            bail!(
                "unexpected score col (id={}) at pos {} for FtsQueryTypeNoScore",
                VIRTUAL_SCORE_COLUMN_ID,
                schema
                    .columns
                    .iter()
                    .position(|c| c.get_column_id() == VIRTUAL_SCORE_COLUMN_ID)
                    .unwrap()
            );
        }
    }

    if query.info().get_query_type() == tipb::FtsQueryType::FtsQueryTypeWithScore {
        // FtsQueryTypeWithScore should contain score column in schema
        if schema.columns.is_empty() {
            bail!("unexpected empty schema for FtsQueryTypeWithScore");
        }
        let last_col = schema.columns.last().unwrap();
        check_score_col(last_col, false)?;
    }

    Ok(())
}

/// Checks whether the given column is a valid score column.
pub fn check_score_col(col: &ColumnInfo, require_nullable: bool) -> anyhow::Result<()> {
    if col.get_column_id() != VIRTUAL_SCORE_COLUMN_ID {
        bail!(
            "column id={} is not the virtual score column id={}",
            col.get_column_id(),
            VIRTUAL_SCORE_COLUMN_ID
        );
    }
    if col.get_tp() != FieldTypeTp::Float as i32 {
        bail!("score column must be float column, got {:?}", col.get_tp());
    }
    if col.as_accessor().flag().contains(FieldTypeFlag::NOT_NULL) == require_nullable {
        bail!(
            "score column nullable={} does not match required={}",
            !col.as_accessor().flag().contains(FieldTypeFlag::NOT_NULL),
            require_nullable
        );
    }
    Ok(())
}

/// Checks whether the given column is a valid FTS text column.
pub fn check_fts_text_col(
    col: &ColumnInfo,
    query: &Query,
    require_nullable: bool,
) -> anyhow::Result<()> {
    if col.get_column_id() != query.info().get_columns()[0].get_column_id() {
        bail!(
            "fts text column id={} does not match FTS query text column id={}",
            col.get_column_id(),
            query.info().get_columns()[0].get_column_id()
        );
    }
    if !col.as_accessor().is_string_like() {
        bail!("fts text column must be string column, got {:?}", col.tp());
    }
    if col.as_accessor().flag().contains(FieldTypeFlag::NOT_NULL) == require_nullable {
        bail!(
            "fts text column nullable={} does not match required={}",
            !col.as_accessor().flag().contains(FieldTypeFlag::NOT_NULL),
            require_nullable
        );
    }
    Ok(())
}

/// A wrapper reader that drops nullable property of the virtual score column.
///
/// If the inner reader contains a nullable virtual score column (id =
/// `VIRTUAL_SCORE_COLUMN_ID`), this reader will:
/// - Ensure all NULL scores are treated as 0.0.
/// - Mark the score column as NOT NULL in output schema.
/// - Clear null markers in the score column.
///
/// If there is no nullable score column in the inner schema, this reader is
/// transparent.
pub struct FtsDropScoreNullableReader<R: ColumnarFilterReader> {
    inner: R,
    output_schema: Schema,
    /// Index into `Block::columns` for the score column when it is nullable in
    /// the inner schema. `None` means transparent.
    score_col_idx: Option<usize>,
}

impl<R: ColumnarFilterReader> FtsDropScoreNullableReader<R> {
    pub fn new(inner: R) -> Result<Self> {
        let inner_schema = inner.get_schema();
        // Locate score column and determine if it is nullable.
        let mut score_schema_idx: Option<usize> = None;
        for (schema_idx, col) in inner_schema.columns.iter().enumerate() {
            if col.get_column_id() == VIRTUAL_SCORE_COLUMN_ID {
                score_schema_idx = Some(schema_idx);
                break;
            }
        }

        // Build output schema. Default to inner schema.
        let mut output_schema = inner_schema.clone();
        let mut score_col_idx = None;
        if let Some(schema_idx) = score_schema_idx {
            let score_col = &inner_schema.columns[schema_idx];
            // Only transform when the score column is nullable.
            if !score_col
                .as_accessor()
                .flag()
                .contains(FieldTypeFlag::NOT_NULL)
            {
                // Validate score column definition.
                super::check_score_col(score_col, true).map_err(|e| Error::Other(e.to_string()))?;

                let mut new_score_col = score_col.clone();
                let mut flag = FieldTypeFlag::from_bits_truncate(new_score_col.get_flag() as u32);
                flag.insert(FieldTypeFlag::NOT_NULL);
                new_score_col.set_flag(flag.bits() as i32);

                let mut buf: SchemaBuf = output_schema.to_schema_buf();
                let mut inner_buf = (*buf.inner).clone();
                inner_buf.columns[schema_idx] = new_score_col;
                buf.inner = Arc::new(inner_buf);
                output_schema = Schema::new(buf);
                // `Block::columns` follows `schema.columns` order.
                score_col_idx = Some(schema_idx);
            }
        }

        Ok(Self {
            inner,
            output_schema,
            score_col_idx,
        })
    }
}

#[async_trait]
impl<R: ColumnarFilterReader> ColumnarFilterReader for FtsDropScoreNullableReader<R> {
    #[inline]
    async fn set_handle_range(
        &mut self,
        start_handle: &[u8],
        end_handle: &[u8],
    ) -> crate::table::Result<()> {
        self.inner.set_handle_range(start_handle, end_handle).await
    }

    #[inline]
    async fn set_int_handle_range(
        &mut self,
        start_handle: i64,
        end_handle: Option<i64>,
    ) -> crate::table::Result<()> {
        self.inner
            .set_int_handle_range(start_handle, end_handle)
            .await
    }

    #[inline]
    fn get_schema(&self) -> &Schema {
        &self.output_schema
    }

    #[inline]
    fn reset(&mut self) {
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

    async fn try_read_block(
        &mut self,
        block: &mut Block,
        limit: usize,
    ) -> crate::table::Result<(usize, bool)> {
        // If we need to drop nullable, temporarily mark score column nullable so
        // inner reader can write NULLs into the buffer.
        if let Some(idx) = self.score_col_idx {
            block.columns[idx].nullable = true;
        }

        let (read_rows, drained) = self.inner.try_read_block(block, limit).await?;

        if let Some(idx) = self.score_col_idx {
            let score_col = &mut block.columns[idx];
            // If no rows read, just restore nullable state.
            for i in 0..read_rows {
                if score_col.nulls.get(i) == Some(&1u8) {
                    // Ensure NULL scores become 0.0.
                    score_col.nulls[i] = 0;
                    score_col
                        .mut_not_null_value(i)
                        .copy_from_slice(&0f64.to_le_bytes());
                }
            }
            score_col.nullable = false;
            score_col.nulls.clear();
        }

        Ok((read_rows, drained))
    }
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::{FieldTypeAccessor, FieldTypeFlag, FieldTypeTp};

    use super::FtsDropScoreNullableReader;
    use crate::table::{
        columnar::{Block, ColumnarFilterReader, MockColumnarFilterReader},
        fts::{
            IntPk,
            reader::VIRTUAL_SCORE_COLUMN_ID,
            test_util::{SchemaBuilder, new_block},
        },
    };

    const TABLE_ID: i64 = 7;
    const TEXT_COL_ID: i64 = 100;

    #[tokio::test]
    async fn drops_nullable_score_and_fills_zero() {
        let schema = SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column(TEXT_COL_ID, FieldTypeTp::VarString)
            .fts_score_column(true)
            .schema();
        let block = new_block::<IntPk>(&schema, |row| {
            row(1, 10, false, |datum| {
                datum("a");
                datum(0.5);
            });
            row(2, 10, false, |datum| {
                datum("b");
                datum.null();
            });
        });
        let mut reader =
            FtsDropScoreNullableReader::new(MockColumnarFilterReader::new(schema.clone(), block))
                .unwrap();

        assert_eq!(reader.get_schema().columns.len(), 2);
        assert_eq!(
            reader.get_schema().columns[1].get_column_id(),
            VIRTUAL_SCORE_COLUMN_ID
        );
        assert!(
            reader.get_schema().columns[1]
                .flag()
                .contains(FieldTypeFlag::NOT_NULL)
        );

        let mut out = Block::new(reader.get_schema());
        let read = reader.read_block(&mut out, 32).await.unwrap();
        assert_eq!(read, 2);

        let expected = new_block::<IntPk>(reader.get_schema(), |row| {
            row(1, 10, false, |datum| {
                datum("a");
                datum(0.5);
            });
            row(2, 10, false, |datum| {
                datum("b");
                datum(0.0);
            });
        });
        assert!(out.eq(&expected));
    }

    #[tokio::test]
    async fn transparent_when_score_not_nullable() {
        let schema = SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column(TEXT_COL_ID, FieldTypeTp::VarString)
            .fts_score_column(false)
            .schema();
        let block = new_block::<IntPk>(&schema, |row| {
            row(1, 10, false, |datum| {
                datum("a");
                datum(0.7);
            });
            row(2, 10, false, |datum| {
                datum("b");
                datum(0.2);
            });
        });
        let mut reader =
            FtsDropScoreNullableReader::new(MockColumnarFilterReader::new(schema.clone(), block))
                .unwrap();

        assert_eq!(reader.get_schema().columns.len(), 2);
        assert_eq!(
            reader.get_schema().columns[1].get_column_id(),
            VIRTUAL_SCORE_COLUMN_ID
        );
        assert!(
            reader.get_schema().columns[1]
                .flag()
                .contains(FieldTypeFlag::NOT_NULL)
        );

        let mut out = Block::new(reader.get_schema());
        let read = reader.read_block(&mut out, 32).await.unwrap();
        assert_eq!(read, 2);
        let expected = new_block::<IntPk>(reader.get_schema(), |row| {
            row(1, 10, false, |datum| {
                datum("a");
                datum(0.7);
            });
            row(2, 10, false, |datum| {
                datum("b");
                datum(0.2);
            });
        });
        assert!(out.eq(&expected));
    }

    #[tokio::test]
    async fn transparent_when_no_score_column() {
        let schema = SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column(TEXT_COL_ID, FieldTypeTp::VarString)
            .schema();
        let block = new_block::<IntPk>(&schema, |row| {
            row(1, 10, false, |datum| {
                datum("a");
            });
            row(2, 10, false, |datum| {
                datum.null();
            });
        });
        let mut reader =
            FtsDropScoreNullableReader::new(MockColumnarFilterReader::new(schema.clone(), block))
                .unwrap();

        assert_eq!(reader.get_schema().columns.len(), 1);
        assert_eq!(reader.get_schema().columns[0].get_column_id(), TEXT_COL_ID);

        let mut out = Block::new(reader.get_schema());
        let read = reader.read_block(&mut out, 32).await.unwrap();
        assert_eq!(read, 2);
        let expected = new_block::<IntPk>(reader.get_schema(), |row| {
            row(1, 10, false, |datum| {
                datum("a");
            });
            row(2, 10, false, |datum| {
                datum.null();
            });
        });
        assert!(out.eq(&expected));
    }
}
