// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use bytes::Buf;
use cloud_encryption::EncryptionKey;
use log_wrappers::Value as LogValue;
use tidb_query_datatype::{
    FieldTypeAccessor, FieldTypeFlag, FieldTypeTp,
    codec::{
        Datum, datum,
        datum::{
            BYTES_FLAG, COMPACT_BYTES_FLAG, DECIMAL_FLAG, DURATION_FLAG, FLOAT_FLAG, INT_FLAG,
            JSON_FLAG, NIL_FLAG, UINT_FLAG, VAR_INT_FLAG, VAR_UINT_FLAG, VECTOR_FLOAT32_FLAG,
            decode,
        },
        mysql::{DecimalDecoder, JsonEncoder, VectorFloat32Encoder},
        row::v2::{CODEC_VERSION, RowSlice, decode_v2_i64, decode_v2_u64},
        table::{append_common_handle_row_key, append_row_key},
    },
};
use tikv_util::codec::{
    BytesSlice,
    bytes::{decode_bytes, decode_compact_bytes},
    number::{decode_f64, decode_i64, decode_u64, decode_var_i64, decode_var_u64},
};

use super::VIRTUAL_SCORE_COLUMN_ID;
use crate::{
    next_version_async,
    table::{
        self, Error, InnerKey, Iterator as TableIterator, Result,
        blobtable::blobtable::BlobTable,
        columnar::{
            Block, ColumnBuffer, ColumnarFilterReader, decode_decimal_as_int, parse_default_val,
        },
        schema_file::Schema,
    },
};

/// A join reader that fills missing / NULL columns from row-based iterators.
///
/// It outputs blocks following `output_schema`. For non-score columns:
/// - If the column exists in inner reader, keep inner values unless they are
///   NULL (then fill from row).
/// - If missing in inner reader, always fill from row.
///
/// Score column is special: if output schema contains the virtual score column,
/// inner reader must also contain it and it will be transparently moved from
/// inner output.
pub struct FtsJoinReader<R: ColumnarFilterReader> {
    inner: R,
    iters: Vec<Box<dyn TableIterator>>,
    output_schema: Schema,
    inner_block: Block,

    blob_tbls: Arc<HashMap<u64, BlobTable>>,
    encryption_key: Option<EncryptionKey>,
    decryption_buf: Vec<u8>,

    is_int_handle: bool,
    default_vals: Vec<Option<Vec<u8>>>,

    /// Per-output column (Block::columns index) mapping to inner Block::columns
    /// index.
    out_to_inner: Vec<Option<usize>>,

    out_score_idx: Option<usize>,
    inner_score_idx: Option<usize>,
    has_missing_cols: bool,

    // Reused per-block flags to mark columns that were fully copied from inner.
    prefilled: Vec<bool>,

    row_key_buf: Vec<u8>,
    row_value_buf: Vec<u8>,
}

enum RowLookup {
    Found,
    Deleted,
    NotFound,
}

impl<R: ColumnarFilterReader> FtsJoinReader<R> {
    pub fn new(
        inner: R,
        iters: Vec<Box<dyn TableIterator>>,
        output_schema: Schema,
        blob_tbls: Arc<HashMap<u64, BlobTable>>,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Self> {
        let inner_schema = inner.get_schema();
        if inner_schema.table_id != output_schema.table_id {
            return Err(Error::Other(format!(
                "FtsJoinReader schema mismatch: inner table_id={} output table_id={}",
                inner_schema.table_id, output_schema.table_id
            )));
        }
        if inner_schema.is_common_handle() != output_schema.is_common_handle() {
            return Err(Error::Other(format!(
                "FtsJoinReader schema mismatch: inner is_common_handle={} output is_common_handle={}",
                inner_schema.is_common_handle(),
                output_schema.is_common_handle()
            )));
        }
        let is_int_handle = !output_schema.is_common_handle();

        // Map inner column id -> inner Block::columns index.
        let mut inner_map: HashMap<i64, usize> = HashMap::with_capacity(inner_schema.columns.len());
        for (inner_idx, col) in inner_schema.columns.iter().enumerate() {
            inner_map.insert(col.get_column_id(), inner_idx);
        }

        let mut out_to_inner = Vec::new();
        let mut default_vals = Vec::new();
        let mut out_score_idx = None;
        let mut inner_score_idx = None;
        let mut has_missing_cols = false;

        for (out_idx, col) in output_schema.columns.iter().enumerate() {
            default_vals.push(parse_default_val(col));
            let col_id = col.get_column_id();
            if col_id == VIRTUAL_SCORE_COLUMN_ID {
                out_score_idx = Some(out_idx);
                if let Some(&inner_idx) = inner_map.get(&col_id) {
                    let inner_col = &inner_schema.columns[inner_idx];
                    let out_nullable = !col.flag().contains(FieldTypeFlag::NOT_NULL);
                    let inner_nullable = !inner_col.flag().contains(FieldTypeFlag::NOT_NULL);
                    if inner_col.get_tp() != col.get_tp() || inner_nullable != out_nullable {
                        return Err(Error::Other(format!(
                            "FtsJoinReader score column mismatch inner tp/nullable (inner tp={}, nullable={}) vs output (tp={}, nullable={})",
                            inner_col.get_tp(),
                            inner_nullable,
                            col.get_tp(),
                            out_nullable,
                        )));
                    }
                    inner_score_idx = Some(inner_idx);
                    out_to_inner.push(Some(inner_idx));
                } else {
                    return Err(Error::Other(
                        "FtsJoinReader requires score column in inner reader".to_string(),
                    ));
                }
            } else if let Some(&inner_idx) = inner_map.get(&col_id) {
                let inner_col = &inner_schema.columns[inner_idx];
                // Defensive: ensure raw bytes from inner can be reused as-is.
                if inner_col.get_tp() != col.get_tp() {
                    return Err(Error::Other(format!(
                        "FtsJoinReader column mismatch (col_id={}): inner tp={} vs output tp={}",
                        col_id,
                        inner_col.get_tp(),
                        col.get_tp(),
                    )));
                }
                out_to_inner.push(Some(inner_idx));
            } else {
                out_to_inner.push(None);
                // Missing PK columns can be filled from the common handle without reading row
                // value.
                if !col.flag().contains(FieldTypeFlag::PRIMARY_KEY) || is_int_handle {
                    has_missing_cols = true;
                }
            }
        }

        debug_assert_eq!(out_to_inner.len(), default_vals.len());
        let out_cols = out_to_inner.len();

        Ok(Self {
            inner_block: Block::new(inner_schema),
            inner,
            iters,
            output_schema,
            blob_tbls,
            encryption_key,
            decryption_buf: Vec::new(),
            is_int_handle,
            default_vals,
            out_to_inner,
            out_score_idx,
            inner_score_idx,
            has_missing_cols,
            prefilled: vec![false; out_cols],
            row_key_buf: Vec::new(),
            row_value_buf: Vec::new(),
        })
    }

    #[inline]
    async fn seek_iter_to_version(iter: &mut dyn TableIterator, version: u64) -> bool {
        if version >= iter.value().version {
            return true;
        }
        while next_version_async!(iter) {
            if version >= iter.value().version {
                return true;
            }
        }
        false
    }

    async fn load_row_value(&mut self, handle: &[u8], version: u64) -> Result<RowLookup> {
        self.row_key_buf.clear();
        self.row_value_buf.clear();
        if self.is_int_handle {
            // Handle values in blocks are little-endian i64, while row keys encode i64
            // handles in memcomparable format.
            let int_handle = (&handle[..]).get_i64_le();
            append_row_key(
                &mut self.row_key_buf,
                self.output_schema.table_id,
                int_handle,
            )
            .map_err(|e| Error::Other(e.to_string()))?;
        } else {
            append_common_handle_row_key(
                &mut self.row_key_buf,
                self.output_schema.table_id,
                handle,
            )
            .map_err(|e| Error::Other(e.to_string()))?;
        }

        let seek_key = InnerKey::from_inner_buf(&self.row_key_buf);
        for iter in self.iters.iter_mut() {
            iter.seek_async(seek_key).await;
            if !iter.valid() {
                continue;
            }
            if iter.key() != seek_key {
                continue;
            }
            let ok = Self::seek_iter_to_version(iter.as_mut(), version).await;
            if !ok || !iter.valid() || iter.key() != seek_key {
                continue;
            }
            let val = iter.value();
            if val.version != version {
                continue;
            }
            if val.is_deleted() {
                return Ok(RowLookup::Deleted);
            }
            if val.is_blob_ref() {
                let blob_ref = val.get_blob_ref();
                let blob_tbl = self.blob_tbls.get(&blob_ref.fid).ok_or_else(|| {
                    Error::Other(format!(
                        "FtsJoinReader blob table not found (fid={})",
                        blob_ref.fid
                    ))
                })?;
                self.row_value_buf = blob_tbl
                    .get(
                        &blob_ref,
                        &mut self.decryption_buf,
                        self.encryption_key.clone(),
                    )
                    .map_err(|e| Error::Other(e.to_string()))?;
            } else {
                self.row_value_buf.extend_from_slice(val.get_value());
            }
            return Ok(RowLookup::Found);
        }
        Ok(RowLookup::NotFound)
    }

    fn push_from_default_or_null(&self, col_buf: &mut ColumnBuffer, out_idx: usize) {
        if let Some(default_val) = &self.default_vals[out_idx] {
            col_buf.push_value(default_val);
        } else if col_buf.nullable {
            col_buf.push_null();
        } else {
            col_buf.push_zero();
        }
    }

    fn push_pk_from_common_handle(
        &self,
        col_buf: &mut ColumnBuffer,
        col_info: &tipb::ColumnInfo,
        common_handle: Option<&[u8]>,
    ) -> table::Result<bool> {
        if self.is_int_handle || !col_info.flag().contains(FieldTypeFlag::PRIMARY_KEY) {
            return Ok(false);
        }
        let mut common_handle = match common_handle {
            Some(h) => h,
            None => return Ok(false),
        };
        for &pk_col_id in &self.output_schema.pk_col_ids {
            let (datum_bytes, remain) = datum::split_datum(common_handle, false).unwrap();
            if pk_col_id == col_info.get_column_id() {
                Self::push_col_buf_with_common_handle_datum(col_buf, col_info, datum_bytes)
                    .map_err(|e| table::Error::Other(e.to_string()))?;
                return Ok(true);
            }
            common_handle = remain;
        }
        Ok(false)
    }

    fn push_from_row_v1(
        &self,
        col_buf: &mut ColumnBuffer,
        col_info: &tipb::ColumnInfo,
        common_handle: Option<&[u8]>,
        datums_map: &HashMap<i64, Datum>,
        out_idx: usize,
    ) -> table::Result<()> {
        let col_id = col_info.get_column_id();
        if let Some(d) = datums_map.get(&col_id) {
            Self::push_col_buf_with_row_v1_datum(col_buf, col_info, d);
            return Ok(());
        }
        if self.push_pk_from_common_handle(col_buf, col_info, common_handle)? {
            return Ok(());
        }
        self.push_from_default_or_null(col_buf, out_idx);
        Ok(())
    }

    fn push_from_row_v2(
        &self,
        col_buf: &mut ColumnBuffer,
        col_info: &tipb::ColumnInfo,
        common_handle: Option<&[u8]>,
        row_slice: &RowSlice<'_>,
        values: &[u8],
        out_idx: usize,
    ) -> table::Result<()> {
        let col_id = col_info.get_column_id();
        if row_slice.search_in_null_ids(col_id) {
            col_buf.push_null();
            return Ok(());
        }
        if let Some((start, end)) = row_slice.search_in_non_null_ids(col_id).unwrap() {
            let col_val = &values[start..end];
            Self::push_col_buf_with_field_type(col_buf, col_info, col_val)
                .map_err(|e| table::Error::Other(e.to_string()))?;
            return Ok(());
        }
        if self.push_pk_from_common_handle(col_buf, col_info, common_handle)? {
            return Ok(());
        }
        self.push_from_default_or_null(col_buf, out_idx);
        Ok(())
    }

    fn push_col_buf_with_field_type(
        col_buf: &mut ColumnBuffer,
        col_info: &tipb::ColumnInfo,
        col_val: &[u8],
    ) -> tidb_query_datatype::codec::Result<()> {
        let ft = FieldTypeTp::from_i32(col_info.get_tp()).ok_or(
            tidb_query_datatype::codec::Error::InvalidDataType("invalid field type".to_string()),
        )?;
        match ft {
            FieldTypeTp::Tiny
            | FieldTypeTp::Short
            | FieldTypeTp::Int24
            | FieldTypeTp::Long
            | FieldTypeTp::LongLong => {
                if col_info.flag().contains(FieldTypeFlag::UNSIGNED) {
                    let v = decode_v2_u64(col_val).unwrap();
                    col_buf.push_value(&v.to_le_bytes());
                } else {
                    let v = decode_v2_i64(col_val)?;
                    col_buf.push_value(&v.to_le_bytes());
                }
            }
            FieldTypeTp::Date
            | FieldTypeTp::DateTime
            | FieldTypeTp::Timestamp
            | FieldTypeTp::Enum
            | FieldTypeTp::Bit
            | FieldTypeTp::Set => {
                let v = decode_v2_u64(col_val)?;
                col_buf.push_value(&v.to_le_bytes());
            }
            FieldTypeTp::Year | FieldTypeTp::Duration => {
                let v = decode_v2_i64(col_val)?;
                col_buf.push_value(&v.to_le_bytes());
            }
            FieldTypeTp::Float | FieldTypeTp::Double => {
                let mut val = col_val;
                let v = decode_f64(&mut val)?;
                col_buf.push_value(&v.to_le_bytes());
            }
            FieldTypeTp::Null => {
                col_buf.push_null();
            }
            FieldTypeTp::NewDecimal => {
                let mut val = col_val;
                let decimal = val.read_decimal().unwrap();
                col_buf.push_value(&decode_decimal_as_int(col_info, &decimal));
            }
            FieldTypeTp::Unspecified
            | FieldTypeTp::NewDate
            | FieldTypeTp::VarChar
            | FieldTypeTp::Json
            | FieldTypeTp::TinyBlob
            | FieldTypeTp::MediumBlob
            | FieldTypeTp::LongBlob
            | FieldTypeTp::Blob
            | FieldTypeTp::VarString
            | FieldTypeTp::String
            | FieldTypeTp::Geometry
            | FieldTypeTp::TiDbVectorFloat32 => {
                col_buf.push_value(col_val);
            }
        }
        Ok(())
    }

    fn push_col_buf_with_row_v1_datum(
        col_buf: &mut ColumnBuffer,
        col_info: &tipb::ColumnInfo,
        datum: &Datum,
    ) {
        match datum {
            Datum::I64(v) => {
                col_buf.push_value(&v.to_le_bytes());
            }
            Datum::Dur(ref d) => {
                col_buf.push_value(&d.to_nanos().to_le_bytes());
            }
            Datum::U64(v) => {
                col_buf.push_value(&v.to_le_bytes());
            }
            Datum::Bytes(ref bs) => {
                col_buf.push_value(bs);
            }
            Datum::Null => {
                col_buf.push_null();
            }
            Datum::F64(v) => {
                col_buf.push_value(&v.to_le_bytes());
            }
            Datum::Dec(ref d) => {
                col_buf.push_value(&decode_decimal_as_int(col_info, d));
            }
            Datum::VectorFloat32(ref v) => {
                let mut buf = vec![];
                buf.write_vector_float32(v.as_ref()).unwrap();
                col_buf.push_value(&buf);
            }
            Datum::Json(ref j) => {
                let mut buf = vec![];
                buf.write_json(j.as_ref()).unwrap();
                col_buf.push_value(&buf);
            }
            _ => {
                panic!("unsupported datum type: {:?}", datum);
            }
        }
    }

    fn push_col_buf_with_common_handle_datum(
        col_buf: &mut ColumnBuffer,
        col_info: &tipb::ColumnInfo,
        mut datum: &[u8],
    ) -> tidb_query_datatype::codec::Result<()> {
        let flag = datum.get_u8();
        match flag {
            INT_FLAG | DURATION_FLAG => {
                let v = decode_i64(&mut datum)?;
                col_buf.push_value(&v.to_le_bytes());
            }
            UINT_FLAG => {
                let v = decode_u64(&mut datum)?;
                col_buf.push_value(&v.to_le_bytes());
            }
            BYTES_FLAG => {
                let v = decode_bytes(&mut datum, false)?;
                col_buf.push_value(&v);
            }
            COMPACT_BYTES_FLAG => {
                let v = decode_compact_bytes(&mut datum)?;
                col_buf.push_value(&v);
            }
            NIL_FLAG => {
                col_buf.push_null();
            }
            FLOAT_FLAG => {
                let v = decode_f64(&mut datum)?;
                col_buf.push_value(&v.to_le_bytes());
            }
            VAR_INT_FLAG => {
                let v = decode_var_i64(&mut datum)?;
                col_buf.push_value(&v.to_le_bytes());
            }
            VAR_UINT_FLAG => {
                let v = decode_var_u64(&mut datum)?;
                col_buf.push_value(&v.to_le_bytes());
            }
            DECIMAL_FLAG => {
                let decimal = datum.read_decimal().unwrap();
                col_buf.push_value(&decode_decimal_as_int(col_info, &decimal));
            }
            JSON_FLAG | VECTOR_FLOAT32_FLAG => {
                return Err(tidb_query_datatype::codec::Error::InvalidDataType(format!(
                    "invalid flag {} in common handle",
                    flag
                )));
            }
            _ => {
                return Err(tidb_query_datatype::codec::Error::InvalidDataType(format!(
                    "unknown flag {} in common handle",
                    flag
                )));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<R: ColumnarFilterReader> ColumnarFilterReader for FtsJoinReader<R> {
    async fn set_handle_range(
        &mut self,
        start_handle: &[u8],
        end_handle: &[u8],
    ) -> crate::table::Result<()> {
        self.inner.set_handle_range(start_handle, end_handle).await
    }

    async fn set_int_handle_range(
        &mut self,
        start_handle: i64,
        end_handle: Option<i64>,
    ) -> crate::table::Result<()> {
        self.inner
            .set_int_handle_range(start_handle, end_handle)
            .await
    }

    fn get_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn reset(&mut self) {
        self.inner.reset();
        self.inner_block.reset();
        self.decryption_buf.clear();
        self.row_key_buf.clear();
        self.row_value_buf.clear();
    }

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
        self.inner_block.reset();
        let (read_rows, drained) = self
            .inner
            .try_read_block(&mut self.inner_block, limit)
            .await?;
        block.reset();
        if read_rows == 0 {
            return Ok((0, drained));
        }

        // Move handles and versions directly.
        std::mem::swap(&mut block.handles, &mut self.inner_block.handles);
        std::mem::swap(&mut block.versions, &mut self.inner_block.versions);

        // Move score column directly if needed.
        if let (Some(out_idx), Some(inner_idx)) = (self.out_score_idx, self.inner_score_idx) {
            std::mem::swap(
                &mut block.columns[out_idx],
                &mut self.inner_block.columns[inner_idx],
            );
        }

        let has_deleted_rows = block.versions.is_nullable()
            && block.versions.nulls[..read_rows].iter().any(|&b| b == 1);

        // Fast path: pre-fill columns that can be copied wholesale from inner (no NULLs
        // and not common-handle PK columns).
        debug_assert_eq!(self.prefilled.len(), block.columns.len());
        for v in self.prefilled.iter_mut() {
            *v = false;
        }
        if !has_deleted_rows {
            for out_idx in 0..block.columns.len() {
                if Some(out_idx) == self.out_score_idx {
                    continue;
                }
                let col_info = &self.output_schema.columns[out_idx];
                if col_info.flag().contains(FieldTypeFlag::PRIMARY_KEY) && !self.is_int_handle {
                    continue;
                }
                let Some(inner_idx) = self.out_to_inner[out_idx] else {
                    continue;
                };
                if Some(inner_idx) == self.inner_score_idx {
                    continue;
                }
                let inner_col = &self.inner_block.columns[inner_idx];
                if inner_col.is_nullable() && inner_col.nulls[..read_rows].iter().any(|&b| b == 1) {
                    continue;
                }
                block.columns[out_idx].append(inner_col, 0, read_rows);
                self.prefilled[out_idx] = true;
            }
        }

        // Fill other columns row by row.
        for row in 0..read_rows {
            let handle = block.handles.get_not_null_value(row);
            let version = block.versions.get_version(row);

            let mut is_deleted = block.versions.is_nullable() && block.versions.is_null(row);
            if is_deleted {
                for out_idx in 0..block.columns.len() {
                    if Some(out_idx) == self.out_score_idx {
                        continue;
                    }
                    if self.prefilled[out_idx] {
                        continue;
                    }
                    let col_buf = &mut block.columns[out_idx];
                    if col_buf.nullable {
                        col_buf.push_null();
                    } else {
                        col_buf.push_zero();
                    }
                }
                continue;
            }

            // Determine if we need to read row data.
            let mut need_row = self.has_missing_cols;
            if !need_row {
                for (out_idx, inner_idx_opt) in self.out_to_inner.iter().enumerate() {
                    if Some(out_idx) == self.out_score_idx {
                        continue;
                    }
                    let col_info = &self.output_schema.columns[out_idx];
                    let pk_from_handle =
                        col_info.flag().contains(FieldTypeFlag::PRIMARY_KEY) && !self.is_int_handle;
                    if let Some(inner_idx) = inner_idx_opt {
                        let inner_col = &self.inner_block.columns[*inner_idx];
                        if inner_col.is_nullable() && inner_col.is_null(row) && !pk_from_handle {
                            need_row = true;
                            break;
                        }
                    } else if !pk_from_handle {
                        need_row = true;
                        break;
                    }
                }
            }

            let mut row_lookup = RowLookup::NotFound;
            if need_row {
                row_lookup = self.load_row_value(handle, version).await?;
                if matches!(row_lookup, RowLookup::Deleted) && block.versions.is_nullable() {
                    block.versions.nulls[row] = 1;
                    is_deleted = true;
                }
                if !matches!(row_lookup, RowLookup::Found) {
                    let reason = if matches!(row_lookup, RowLookup::Deleted) {
                        "deleted"
                    } else {
                        "not_found"
                    };
                    if self.is_int_handle {
                        warn!(
                            "FtsJoinReader cannot load row for join";
                            "table_id" => self.output_schema.table_id,
                            "handle" => (&handle[..]).get_i64_le(),
                            "version" => version,
                            "reason" => reason,
                            "row_key" => LogValue::key(&self.row_key_buf),
                        );
                    } else {
                        warn!(
                            "FtsJoinReader cannot load row for join";
                            "table_id" => self.output_schema.table_id,
                            "handle" => LogValue::key(handle),
                            "version" => version,
                            "reason" => reason,
                            "row_key" => LogValue::key(&self.row_key_buf),
                        );
                    }
                }
            }
            if is_deleted {
                for out_idx in 0..block.columns.len() {
                    if Some(out_idx) == self.out_score_idx {
                        continue;
                    }
                    if self.prefilled[out_idx] {
                        continue;
                    }
                    let col_buf = &mut block.columns[out_idx];
                    if col_buf.nullable {
                        col_buf.push_null();
                    } else {
                        col_buf.push_zero();
                    }
                }
                continue;
            }

            let has_row = matches!(row_lookup, RowLookup::Found);

            let mut datums_map_opt: Option<HashMap<i64, Datum>> = None;
            let mut row_slice_opt: Option<RowSlice<'_>> = None;

            if has_row && !self.row_value_buf.is_empty() {
                if self.row_value_buf[0] == CODEC_VERSION {
                    let row_slice = RowSlice::from_bytes(&self.row_value_buf).map_err(|e| {
                        table::Error::Other(format!(
                            "FtsJoinReader decode row v2 failed (table_id={} handle={} version={} row_key={} value_len={}): {}",
                            self.output_schema.table_id,
                            if self.is_int_handle {
                                (&handle[..]).get_i64_le().to_string()
                            } else {
                                format!("{}", LogValue::key(handle))
                            },
                            version,
                            LogValue::key(&self.row_key_buf),
                            self.row_value_buf.len(),
                            e
                        ))
                    })?;
                    row_slice_opt = Some(row_slice);
                } else {
                    let mut data: BytesSlice<'_> = &self.row_value_buf;
                    let datums =
                        decode(&mut data).map_err(|e| table::Error::Other(e.to_string()))?;
                    let mut datums_map: HashMap<i64, Datum> =
                        HashMap::with_capacity(datums.len() / 2);
                    let mut iter = datums.into_iter();
                    while let (Some(col_id), Some(val)) = (iter.next(), iter.next()) {
                        if let Ok(Some(id)) = col_id.as_int() {
                            datums_map.insert(id, val);
                        } else {
                            return Err(table::Error::Other(format!(
                                "FtsJoinReader decode row v1 got invalid col id (table_id={} handle={} version={} row_key={}): {:?}",
                                self.output_schema.table_id,
                                if self.is_int_handle {
                                    (&handle[..]).get_i64_le().to_string()
                                } else {
                                    format!("{}", LogValue::key(handle))
                                },
                                version,
                                LogValue::key(&self.row_key_buf),
                                col_id,
                            )));
                        }
                    }
                    datums_map_opt = Some(datums_map);
                }
            }

            let common_handle = if self.is_int_handle {
                None
            } else {
                Some(block.handles.get_not_null_value(row))
            };

            for out_idx in 0..block.columns.len() {
                if Some(out_idx) == self.out_score_idx {
                    continue;
                }
                if self.prefilled[out_idx] {
                    continue;
                }
                let col_info = &self.output_schema.columns[out_idx];
                let col_buf = &mut block.columns[out_idx];

                if let Some(inner_idx) = self.out_to_inner[out_idx] {
                    if Some(inner_idx) == self.inner_score_idx {
                        continue;
                    }
                    let inner_col = &self.inner_block.columns[inner_idx];
                    if !inner_col.is_nullable() || !inner_col.is_null(row) {
                        col_buf.push_value(inner_col.get_not_null_value(row));
                        continue;
                    }
                }

                if !need_row || !has_row || self.row_value_buf.is_empty() {
                    // Common-handle PK columns are always derivable from the handle, even when we
                    // don't have (or don't need) row value.
                    if self.push_pk_from_common_handle(col_buf, col_info, common_handle)? {
                        continue;
                    }
                    self.push_from_default_or_null(col_buf, out_idx);
                    continue;
                }

                if let Some(ref row_slice) = row_slice_opt {
                    let values = row_slice.values();
                    self.push_from_row_v2(
                        col_buf,
                        col_info,
                        common_handle,
                        row_slice,
                        values,
                        out_idx,
                    )?;
                } else {
                    self.push_from_row_v1(
                        col_buf,
                        col_info,
                        common_handle,
                        datums_map_opt.as_ref().unwrap(),
                        out_idx,
                    )?;
                }
            }
        }

        Ok((read_rows, drained))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use bytes::Bytes;
    use tidb_query_datatype::{
        FieldTypeTp,
        codec::{datum, datum::Datum},
        expr::EvalContext,
    };

    use super::FtsJoinReader;
    use crate::table::{
        InnerKey, Iterator as TableIterator, NO_COMPRESSION,
        blobtable::{blobtable::BlobTable, builder::BlobTableBuilder},
        columnar::{Block, ColumnarFilterReader, MockColumnarFilterReader},
        file::InMemFile,
        fts::{
            CommonPk, IntPk,
            test_util::{SchemaBuilder, encode_row_value_v1, new_block, new_write_batch},
        },
        table::{BIT_BLOB_REF, Value},
    };

    const TABLE_ID: i64 = 42;

    struct CountingRowIter {
        inner: Box<dyn TableIterator>,
        seek_count: Arc<AtomicUsize>,
    }

    impl CountingRowIter {
        fn new(inner: Box<dyn TableIterator>, seek_count: Arc<AtomicUsize>) -> Self {
            Self { inner, seek_count }
        }
    }

    impl TableIterator for CountingRowIter {
        fn next(&mut self) {
            self.inner.next();
        }

        fn is_next_sync(&self) -> bool {
            self.inner.is_next_sync()
        }

        fn next_version(&mut self) -> bool {
            self.inner.next_version()
        }

        fn is_next_version_sync(&self) -> bool {
            self.inner.is_next_version_sync()
        }

        fn rewind(&mut self) {
            self.inner.rewind();
        }

        fn seek(&mut self, key: InnerKey<'_>) {
            self.seek_count.fetch_add(1, Ordering::Relaxed);
            self.inner.seek(key);
        }

        fn is_cf_sync(&self, cf: usize) -> bool {
            self.inner.is_cf_sync(cf)
        }

        fn key(&self) -> InnerKey<'_> {
            self.inner.key()
        }

        fn value(&self) -> Value {
            self.inner.value()
        }

        fn valid(&self) -> bool {
            self.inner.valid()
        }
    }

    fn encode_default_i64(v: i64) -> Vec<u8> {
        use tidb_query_datatype::codec::datum_codec::DatumFlagAndPayloadEncoder;
        let mut buf = vec![];
        buf.write_datum_i64(v).unwrap();
        buf
    }

    #[tokio::test]
    async fn joins_missing_cols_and_fills_null_text_from_row_v1() {
        let inner_schema = SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column(17, FieldTypeTp::VarString)
            .fts_score_column(false)
            .schema();
        let inner_block = new_block::<IntPk>(&inner_schema, |row| {
            row(1, 10, false, |datum| {
                datum.null();
                datum(0.8); // Score
            });
            row(2, 10, false, |datum| {
                datum("inner2");
                datum(0.5); // Score
            });
        });
        let iter = new_write_batch()
            .table::<IntPk>(
                &SchemaBuilder::<IntPk>::new(TABLE_ID)
                    .column(5, FieldTypeTp::LongLong)
                    .column(17, FieldTypeTp::VarString)
                    .column(23, FieldTypeTp::LongLong)
                    .schema(),
                |row| {
                    row.v1(1, 10, |datum| {
                        datum(7);
                        datum("row1");
                        datum(9);
                    });
                    row.v1(2, 10, |datum| {
                        datum(8);
                        datum("row2");
                        datum(10);
                    });
                },
            )
            .finish();

        let inner = MockColumnarFilterReader::new(inner_schema.clone(), inner_block);
        let mut reader = FtsJoinReader::new(
            inner,
            vec![iter],
            SchemaBuilder::<IntPk>::new(TABLE_ID)
                .column(5, FieldTypeTp::LongLong)
                .column(17, FieldTypeTp::VarString)
                .column(23, FieldTypeTp::LongLong)
                .fts_score_column(false)
                .schema(),
            Arc::new(HashMap::new()),
            None,
        )
        .unwrap();

        let mut out = Block::new(reader.get_schema());
        let (read, drained) = reader.try_read_block(&mut out, 32).await.unwrap();
        assert!(drained);
        assert_eq!(read, 2);

        let expected = new_block::<IntPk>(reader.get_schema(), |row| {
            // Foo and Bar filled from row, Text from row only for row1.
            row(1, 10, false, |datum| {
                datum(7);
                datum("row1");
                datum(9);
                datum(0.8);
            });
            row(2, 10, false, |datum| {
                datum(8);
                datum("inner2");
                datum(10);
                datum(0.5);
            });
        });
        assert!(out.eq(&expected));
    }

    #[tokio::test]
    async fn joins_from_row_v2() {
        let inner_schema = SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column(17, FieldTypeTp::VarString)
            .fts_score_column(false)
            .schema();
        let inner_block = new_block::<IntPk>(&inner_schema, |row| {
            row(1, 10, false, |datum| {
                datum.null();
                datum(0.8);
            });
            row(2, 10, false, |datum| {
                datum("inner2");
                datum(0.5);
            });
        });
        let iter = new_write_batch()
            .table::<IntPk>(
                &SchemaBuilder::<IntPk>::new(TABLE_ID)
                    .column(5, FieldTypeTp::LongLong)
                    .column(17, FieldTypeTp::VarString)
                    .column(23, FieldTypeTp::LongLong)
                    .schema(),
                |row| {
                    row.v2(1, 10, |datum| {
                        datum(7);
                        datum("row1");
                        datum(9);
                    });
                    row.v2(2, 10, |datum| {
                        datum(8);
                        datum("row2");
                        datum(10);
                    });
                },
            )
            .finish();

        let inner = MockColumnarFilterReader::new(inner_schema.clone(), inner_block);
        let mut reader = FtsJoinReader::new(
            inner,
            vec![iter],
            SchemaBuilder::<IntPk>::new(TABLE_ID)
                .column(5, FieldTypeTp::LongLong)
                .column(17, FieldTypeTp::VarString)
                .column(23, FieldTypeTp::LongLong)
                .schema(),
            Arc::new(HashMap::new()),
            None,
        )
        .unwrap();

        let mut out = Block::new(reader.get_schema());
        let (read, drained) = reader.try_read_block(&mut out, 32).await.unwrap();
        assert!(drained);
        assert_eq!(read, 2);

        let expected = new_block::<IntPk>(reader.get_schema(), |row| {
            row(1, 10, false, |datum| {
                datum(7);
                datum("row1");
                datum(9);
            });
            row(2, 10, false, |datum| {
                datum(8);
                datum("inner2");
                datum(10);
            });
        });
        assert!(out.eq(&expected));
    }

    #[tokio::test]
    async fn does_not_read_row_when_all_output_columns_present() {
        let schema = SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column(17, FieldTypeTp::VarString)
            .fts_score_column(false)
            .schema();
        let inner_block = new_block::<IntPk>(&schema, |row| {
            row(1, 10, false, |datum| {
                datum("inner1");
                datum(0.8); // Score
            });
            row(2, 10, false, |datum| {
                datum("inner2");
                datum(0.5); // Score
            });
        });

        let row_schema = SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column(17, FieldTypeTp::VarString)
            .schema();
        let iter = new_write_batch()
            .table::<IntPk>(&row_schema, |row| {
                row.v2(1, 10, |datum| {
                    datum("row1");
                });
                row.v2(2, 10, |datum| {
                    datum("row2");
                });
            })
            .finish();

        let seek_count = Arc::new(AtomicUsize::new(0));
        let iter = Box::new(CountingRowIter::new(iter, Arc::clone(&seek_count)));

        let inner = MockColumnarFilterReader::new(schema.clone(), inner_block);
        let mut reader =
            FtsJoinReader::new(inner, vec![iter], schema, Arc::new(HashMap::new()), None).unwrap();

        let mut out = Block::new(reader.get_schema());
        let (read, drained) = reader.try_read_block(&mut out, 32).await.unwrap();
        assert!(drained);
        assert_eq!(read, 2);
        assert_eq!(
            seek_count.load(Ordering::Relaxed),
            0,
            "expected join to not seek row iterator"
        );

        let expected = new_block::<IntPk>(reader.get_schema(), |row| {
            row(1, 10, false, |datum| {
                datum("inner1");
                datum(0.8);
            });
            row(2, 10, false, |datum| {
                datum("inner2");
                datum(0.5);
            });
        });
        assert!(out.eq(&expected));
    }

    #[tokio::test]
    async fn score_only_is_transparent() {
        let inner_schema = SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column(17, FieldTypeTp::VarString)
            .fts_score_column(false)
            .schema();
        let inner_block = new_block::<IntPk>(&inner_schema, |row| {
            row(1, 10, false, |datum| {
                datum.null();
                datum(0.8);
            });
            row(2, 10, false, |datum| {
                datum("inner2");
                datum(0.5);
            });
        });

        let inner = MockColumnarFilterReader::new(inner_schema.clone(), inner_block);
        let mut reader = FtsJoinReader::new(
            inner,
            vec![],
            SchemaBuilder::<IntPk>::new(TABLE_ID)
                .fts_score_column(false)
                .schema(),
            Arc::new(HashMap::new()),
            None,
        )
        .unwrap();

        let mut out = Block::new(reader.get_schema());
        let (read, _) = reader.try_read_block(&mut out, 32).await.unwrap();
        assert_eq!(read, 2);
        let expected = new_block::<IntPk>(reader.get_schema(), |row| {
            row(1, 10, false, |datum| {
                datum(0.8);
            });
            row(2, 10, false, |datum| {
                datum(0.5);
            });
        });
        assert!(out.eq(&expected));
    }

    #[tokio::test]
    async fn text_and_score_only_reads_row_conditionally() {
        let inner_schema = SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column(17, FieldTypeTp::VarString)
            .fts_score_column(false)
            .schema();
        let inner_block = new_block::<IntPk>(&inner_schema, |row| {
            row(1, 10, false, |datum| {
                datum.null();
                datum(0.8);
            });
            row(2, 10, false, |datum| {
                datum("inner2");
                datum(0.5);
            });
        });

        let inner = MockColumnarFilterReader::new(inner_schema.clone(), inner_block);
        let seek_count = Arc::new(AtomicUsize::new(0));
        let iter = new_write_batch()
            .table::<IntPk>(
                &SchemaBuilder::<IntPk>::new(TABLE_ID)
                    .column(5, FieldTypeTp::LongLong)
                    .column(17, FieldTypeTp::VarString)
                    .column(23, FieldTypeTp::LongLong)
                    .schema(),
                |row| {
                    row.v1(1, 10, |datum| {
                        datum(7);
                        datum("row1");
                        datum(9);
                    });
                    row.v1(2, 10, |datum| {
                        datum(8);
                        datum("row2");
                        datum(10);
                    });
                },
            )
            .finish();
        let counting_iter = Box::new(CountingRowIter::new(iter, seek_count.clone())) as _;

        let mut reader = FtsJoinReader::new(
            inner,
            vec![counting_iter],
            SchemaBuilder::<IntPk>::new(TABLE_ID)
                .column(17, FieldTypeTp::VarString)
                .fts_score_column(false)
                .schema(),
            Arc::new(HashMap::new()),
            None,
        )
        .unwrap();

        let mut out = Block::new(reader.get_schema());
        let (read, drained) = reader.try_read_block(&mut out, 32).await.unwrap();
        assert!(drained);
        assert_eq!(read, 2);

        let expected = new_block::<IntPk>(reader.get_schema(), |row| {
            row(1, 10, false, |datum| {
                datum("row1");
                datum(0.8);
            });
            row(2, 10, false, |datum| {
                datum("inner2");
                datum(0.5);
            });
        });
        assert!(out.eq(&expected));

        assert_eq!(seek_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn common_handle_pk_filled_from_handle_when_row_missing() {
        const PK1_COL_ID: i64 = 10;
        const PK2_COL_ID: i64 = 11;
        const VAL_COL_ID: i64 = 12;

        let inner_schema = SchemaBuilder::<CommonPk>::new(TABLE_ID)
            .pk_col_ids(vec![PK1_COL_ID, PK2_COL_ID])
            .schema();

        // Build a composite common handle.
        let mut ctx = EvalContext::default();
        let handle_bytes =
            datum::encode_key(&mut ctx, &[Datum::I64(7), Datum::Bytes(b"pk2".to_vec())]).unwrap();

        let mut inner_block = Block::new(&inner_schema);
        inner_block.handles.push_value(&handle_bytes);
        inner_block.versions.push_version(10, false);

        let inner = MockColumnarFilterReader::new(inner_schema.clone(), inner_block);
        let mut reader = FtsJoinReader::new(
            inner,
            vec![], // no row iterator => row missing
            SchemaBuilder::<CommonPk>::new(TABLE_ID)
                .pk_column(PK1_COL_ID, FieldTypeTp::LongLong)
                .pk_column(PK2_COL_ID, FieldTypeTp::VarString)
                .column(VAL_COL_ID, FieldTypeTp::LongLong)
                .schema(),
            Arc::new(HashMap::new()),
            None,
        )
        .unwrap();

        let mut out = Block::new(reader.get_schema());
        let (read, drained) = reader.try_read_block(&mut out, 4).await.unwrap();
        assert!(drained);
        assert_eq!(read, 1);

        let expected = new_block::<CommonPk>(reader.get_schema(), |row| {
            row(&handle_bytes, 10, false, |datum| {
                datum(7);
                datum("pk2");
                datum.null();
            });
        });
        assert!(out.eq(&expected));
        // assert_eq!(read_i64(out.columns[0].get_not_null_value(0)), 7);
        // assert_eq!(out.columns[1].get_not_null_value(0), b"pk2");
        // assert!(out.columns[2].is_null(0));
    }

    #[tokio::test]
    async fn fills_default_value_when_column_absent_in_row_v1() {
        let inner_schema = SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column(17, FieldTypeTp::VarString)
            .schema();
        let inner_block = new_block::<IntPk>(&inner_schema, |row| {
            row(1, 10, false, |datum| {
                datum.null();
            });
        });

        let row_schema = SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column_with_default(5, FieldTypeTp::LongLong, encode_default_i64(47))
            .column(17, FieldTypeTp::VarString)
            .schema();
        let out_schema = row_schema.clone();

        // Row v1 contains only text.
        let iter = new_write_batch()
            .table::<IntPk>(&row_schema, |row| {
                row.v1(1, 10, |datum| {
                    datum.absent();
                    datum("row_text");
                });
            })
            .finish();

        let inner = MockColumnarFilterReader::new(inner_schema.clone(), inner_block);
        let mut reader = FtsJoinReader::new(
            inner,
            vec![iter],
            out_schema,
            Arc::new(HashMap::new()),
            None,
        )
        .unwrap();

        let mut out = Block::new(reader.get_schema());
        let (read, drained) = reader.try_read_block(&mut out, 4).await.unwrap();
        assert!(drained);
        assert_eq!(read, 1);

        let expected = new_block::<IntPk>(reader.get_schema(), |row| {
            row(1, 10, false, |datum| {
                datum(47);
                datum("row_text");
            });
        });
        assert!(out.eq(&expected));
    }

    #[tokio::test]
    async fn fills_default_value_when_column_absent_in_row_v2() {
        let row_schema = SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column_with_default(5, FieldTypeTp::LongLong, encode_default_i64(41))
            .column(17, FieldTypeTp::VarString)
            .schema();
        let out_schema = row_schema.clone();

        let inner_schema = SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column(17, FieldTypeTp::VarString)
            .schema();
        let inner_block = new_block::<IntPk>(&inner_schema, |row| {
            row(1, 10, false, |datum| {
                datum.null();
            });
        });

        // Row v2 contains only text.
        let iter = new_write_batch()
            .table::<IntPk>(&row_schema, |row| {
                row.v2(1, 10, |datum| {
                    datum.absent();
                    datum("row_text");
                });
            })
            .finish();

        let inner = MockColumnarFilterReader::new(inner_schema.clone(), inner_block);
        let mut reader = FtsJoinReader::new(
            inner,
            vec![iter],
            out_schema,
            Arc::new(HashMap::new()),
            None,
        )
        .unwrap();

        let mut out = Block::new(reader.get_schema());
        let (read, drained) = reader.try_read_block(&mut out, 4).await.unwrap();
        assert!(drained);
        assert_eq!(read, 1);

        let expected = new_block::<IntPk>(reader.get_schema(), |row| {
            row(1, 10, false, |datum| {
                datum(41);
                datum("row_text");
            });
        });
        assert!(out.eq(&expected));
    }

    #[tokio::test]
    async fn reads_row_value_from_blob_table() {
        // Use the same schemas as joins_missing_cols_and_fills_null_text_from_row_v1,
        // but store row values in a blob table.
        let inner_schema = SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column(17, FieldTypeTp::VarString)
            .fts_score_column(false)
            .schema();
        let inner_block = new_block::<IntPk>(&inner_schema, |row| {
            row(1, 10, false, |datum| {
                datum.null();
                datum(0.8);
            });
            row(2, 10, false, |datum| {
                datum("inner2");
                datum(0.5);
            });
        });

        // Build blob table with two row values.
        let blob_fid = 100;
        let mut blob_builder = BlobTableBuilder::new(blob_fid, NO_COMPRESSION, 0, 0, 1024, None);

        let mut blob_refs = Vec::new();
        let row_schema = SchemaBuilder::<IntPk>::new(TABLE_ID)
            .column(5, FieldTypeTp::LongLong)
            .column(17, FieldTypeTp::VarString)
            .column(23, FieldTypeTp::LongLong)
            .schema();
        for (handle, foo, text, bar) in [(1i64, 7i64, "row1", 9i64), (2i64, 8i64, "row2", 10i64)] {
            let row_key = tidb_query_datatype::codec::table::encode_row_key(TABLE_ID, handle);
            let row_value = encode_row_value_v1(&row_schema, |datum| {
                datum(foo);
                datum(text);
                datum(bar);
            });
            let value_buf = Value::encode_buf(0, &[], 10, &row_value);
            let val = Value::decode(value_buf.as_slice());
            let blob_ref = blob_builder.add(InnerKey::from_inner_buf(&row_key), &val);

            let mut blob_ref_bytes =
                vec![0u8; std::mem::size_of::<crate::table::blobtable::BlobRef>()];
            blob_ref.serialize(&mut blob_ref_bytes);
            blob_refs.push((handle, blob_ref_bytes));
        }

        let blob_bytes: Bytes = blob_builder.finish();
        let blob_file = InMemFile::new(blob_fid, blob_bytes);
        let blob_tbl = BlobTable::new(Arc::new(blob_file)).unwrap();
        let blob_tbls = Arc::new(HashMap::from([(blob_fid, blob_tbl)]));

        let inner = MockColumnarFilterReader::new(inner_schema.clone(), inner_block);
        let iter = new_write_batch()
            .table::<IntPk>(&row_schema, |row| {
                for (handle, blob_ref_bytes) in blob_refs {
                    row.raw(handle, 10, BIT_BLOB_REF, blob_ref_bytes);
                }
            })
            .finish();
        let mut reader = FtsJoinReader::new(
            inner,
            vec![iter],
            SchemaBuilder::<IntPk>::new(TABLE_ID)
                .column(5, FieldTypeTp::LongLong)
                .column(17, FieldTypeTp::VarString)
                .column(23, FieldTypeTp::LongLong)
                .fts_score_column(false)
                .schema(),
            blob_tbls,
            None,
        )
        .unwrap();

        let mut out = Block::new(reader.get_schema());
        let (read, drained) = reader.try_read_block(&mut out, 32).await.unwrap();
        assert!(drained);
        assert_eq!(read, 2);

        let expected = new_block::<IntPk>(reader.get_schema(), |row| {
            row(1, 10, false, |datum| {
                datum(7);
                datum("row1");
                datum(9);
                datum(0.8);
            });
            row(2, 10, false, |datum| {
                datum(8);
                datum("inner2");
                datum(10);
                datum(0.5);
            });
        });
        assert!(out.eq(&expected));
    }
}
