// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Cow,
    convert::{TryFrom, TryInto},
    sync::Arc,
};

use anyhow::anyhow;
use api_version::{api_v2::KEYSPACE_PREFIX_LEN, ApiV2, KeyMode, KvFormat};
use bytes::{buf::Buf, Bytes};
use kvengine::{
    read::Iterator,
    table::{
        columnar::{filter::TableScanCtx, Block, ColumnarFilterReader, HANDLE_COL_ID},
        fts,
    },
    LOCK_CF,
};
use kvproto::{coprocessor::KeyRange, kvrpcpb::IsolationLevel};
use tidb_query_common::{
    error::{ErrorInner, EvaluateError, StorageError},
    storage::IntervalRange,
    util::convert_to_prefix_next,
    Result,
};
use tidb_query_datatype::{
    codec::{
        batch::{LazyBatchColumn, LazyBatchColumnVec},
        data_type::{ChunkedVec, Enum, Real, VectorValue},
        mysql::{DecimalDecoder, Duration, JsonDecoder, Set, Time, VectorFloat32Decoder},
        table,
        table::{encode_common_handle_row_key, encode_row_key, PREFIX_LEN},
    },
    expr::EvalContext,
    EvalType, FieldTypeTp,
};
use tikv_util::{buffer_vec::BufferVec, info};
use tipb::{AnnQueryInfo, FtsQueryInfo, TableScan};
use txn_types::{Key, Lock, TsSet};

pub struct ColumnarScanner {
    // The current scan position.
    reader: Box<dyn ColumnarFilterReader>,
    keyspace_id: u32,
    output_offsets: Vec<i32>,
    eval_types: Vec<EvalType>,
    block: Block,
    start_range: Vec<u8>,
    working_end_handle: Option<Vec<u8>>,
    // TODO: support backward scan
    _scan_backward_in_range: bool,
    handle_range: Option<(Vec<u8>, Option<Vec<u8>>)>,
}

impl ColumnarScanner {
    pub fn new(
        reader: Box<dyn ColumnarFilterReader>,
        output_offsets: Vec<i32>,
        keyspace_id: u32,
        start_range: Vec<u8>,
        handle_range: (Vec<u8>, Option<Vec<u8>>),
    ) -> Self {
        let schema = reader.get_schema();
        let mut eval_types = vec![];
        for &offset in &output_offsets {
            let col_tp = if offset == -1 {
                schema.handle_column.get_tp()
            } else {
                schema.columns[offset as usize].get_tp()
            };
            let tp = FieldTypeTp::from_i32(col_tp).unwrap();
            let eval_type = EvalType::try_from(tp).unwrap();
            eval_types.push(eval_type);
        }
        let block = Block::new(schema);
        Self {
            reader,
            keyspace_id,
            output_offsets,
            eval_types,
            block,
            start_range,
            working_end_handle: None,
            _scan_backward_in_range: false,
            handle_range: Some(handle_range),
        }
    }
}

impl ColumnarScanner {
    pub fn take_scanned_range(&mut self) -> IntervalRange {
        let mut range = IntervalRange::default();
        range.lower_inclusive = self.start_range.clone();
        let schema = self.reader.get_schema();
        let keyspace_prefix = api_version::ApiV2::get_txn_keyspace_prefix(self.keyspace_id);
        let table_id = schema.table_id;
        let mut upper = if schema.is_common_handle() {
            let end = encode_common_handle_row_key(
                table_id,
                self.working_end_handle.as_ref().unwrap_or(&vec![]),
            );
            [keyspace_prefix.as_slice(), end.as_slice()].concat()
        } else {
            let end_handle = if self.working_end_handle.is_none() {
                i64::MIN
            } else {
                i64::from_le_bytes(
                    self.working_end_handle.as_ref().unwrap()[..8]
                        .try_into()
                        .unwrap(),
                )
            };
            let end = encode_row_key(table_id, end_handle);
            [keyspace_prefix.as_slice(), end.as_slice()].concat()
        };
        convert_to_prefix_next(&mut upper);
        range.upper_exclusive = upper;
        range
    }

    pub async fn scan(&mut self, scan_rows: usize) -> (LazyBatchColumnVec, Result<bool>) {
        if let Some((start_handle, end_handle)) = self.handle_range.take() {
            if self.reader.get_schema().is_common_handle() {
                self.reader
                    .set_handle_range(&start_handle, &end_handle.unwrap())
                    .await
                    .unwrap();
            } else {
                let end_handle = end_handle.as_ref().map(|h| h.as_slice().get_i64_le());
                self.reader
                    .set_int_handle_range(start_handle.as_slice().get_i64_le(), end_handle)
                    .await
                    .unwrap();
            }
        }
        let mut column_vec =
            new_lazy_batch_columns(&self.output_offsets, &self.eval_types, scan_rows);
        let read_size = self
            .reader
            .read_block(&mut self.block, scan_rows)
            .await
            .unwrap();
        if read_size > 0 {
            let schema = self.reader.get_schema();
            let mut eval_ctx = EvalContext::default();
            let col_buf = self.block.get_handle_buf();
            let end = col_buf.get_value(read_size - 1);
            self.working_end_handle = end.map(|v| v.to_vec());
            for (i, &output_off) in self.output_offsets.iter().enumerate() {
                let (column_buf, column_info) = if output_off < 0 {
                    (self.block.get_handle_buf(), &schema.handle_column)
                } else {
                    (
                        &self.block.get_columns()[output_off as usize],
                        &schema.columns[output_off as usize],
                    )
                };
                match column_vec[i].mut_decoded() {
                    VectorValue::Int(cv) => {
                        for row_idx in 0..read_size {
                            cv.push(
                                column_buf
                                    .get_value(row_idx)
                                    .map(|mut v| v.get_u64_le() as i64),
                            );
                        }
                    }
                    VectorValue::Real(cv) => {
                        for row_idx in 0..read_size {
                            let real_val = column_buf
                                .get_value(row_idx)
                                .map(|mut v| Real::new(v.get_f64_le()));
                            if let Some(Ok(real)) = real_val {
                                cv.push_data(real);
                            } else {
                                cv.push_null();
                            }
                        }
                    }
                    VectorValue::Decimal(cv) => {
                        for row_idx in 0..read_size {
                            cv.push(
                                column_buf
                                    .get_value(row_idx)
                                    .map(|mut v| v.read_decimal().unwrap()),
                            );
                        }
                    }
                    VectorValue::Bytes(cv) => {
                        for row_idx in 0..read_size {
                            cv.push_ref(column_buf.get_value(row_idx));
                        }
                    }
                    VectorValue::DateTime(cv) => {
                        let fsp = column_info.get_decimal() as i8;
                        let time_type = FieldTypeTp::from_i32(column_info.get_tp())
                            .unwrap()
                            .try_into()
                            .unwrap();
                        for row_idx in 0..read_size {
                            cv.push(column_buf.get_value(row_idx).map(|mut v| {
                                let u64_val = v.get_u64_le();
                                Time::from_packed_u64(&mut eval_ctx, u64_val, time_type, fsp)
                                    .unwrap()
                            }));
                        }
                    }
                    VectorValue::Duration(cv) => {
                        let fsp = column_info.get_decimal() as i8;
                        for row_idx in 0..read_size {
                            cv.push(column_buf.get_value(row_idx).map(|mut v| {
                                let i64_val = v.get_i64_le();
                                Duration::from_nanos(i64_val, fsp).unwrap()
                            }));
                        }
                    }
                    VectorValue::Json(cv) => {
                        for row_idx in 0..read_size {
                            cv.push(
                                column_buf
                                    .get_value(row_idx)
                                    .map(|mut v| v.read_json().unwrap()),
                            );
                        }
                    }
                    VectorValue::Enum(cv) => {
                        let elems = self.reader.get_schema().columns[i].get_elems();
                        for row_idx in 0..read_size {
                            cv.push(column_buf.get_value(row_idx).map(|mut v| {
                                let value = v.get_u64_le();
                                let name = Enum::get_value_name(value, elems);
                                Enum::new(name.to_vec(), value)
                            }));
                        }
                    }
                    VectorValue::Set(cv) => {
                        let data_buf = Arc::new(BufferVec::new());
                        for row_idx in 0..read_size {
                            cv.push(column_buf.get_value(row_idx).map(|mut v| {
                                let value = v.get_u64_le();
                                Set::new(data_buf.clone(), value)
                            }));
                        }
                    }
                    VectorValue::VectorFloat32(v) => {
                        for row_idx in 0..read_size {
                            v.push(
                                column_buf
                                    .get_value(row_idx)
                                    .map(|mut v| v.read_vector_float32().unwrap()),
                            );
                        }
                    }
                }
            }
        }
        // Update scanned keys end.
        (LazyBatchColumnVec::from(column_vec), Ok(read_size == 0))
    }
}

fn new_lazy_batch_columns(
    output_offsets: &[i32],
    eval_types: &[EvalType],
    scan_rows: usize,
) -> Vec<LazyBatchColumn> {
    let mut columns: Vec<LazyBatchColumn> = Vec::with_capacity(output_offsets.len());
    for &eval_type in eval_types {
        let vec_val = LazyBatchColumn::decoded_with_capacity_and_tp(scan_rows, eval_type);
        columns.push(vec_val);
    }
    columns
}

fn get_output_offsets(table_scan: &TableScan) -> Vec<i32> {
    let mut output_offsets = vec![];
    let mut col_offset = 0;
    for col in table_scan.get_columns() {
        if col.get_column_id() == HANDLE_COL_ID as i64 || col.get_pk_handle() {
            output_offsets.push(-1);
        } else {
            output_offsets.push(col_offset);
            col_offset += 1;
        }
    }
    output_offsets
}

// Due to the discarding of column_id in the vector index, the value assigned to
// ann_query_info may be different in different versions of tidb. For
// compatibility, use this function to get column_id.
// For field changes, see: https://github.com/pingcap/tipb/pull/358
fn get_ann_vec_col_id(ann_query: &AnnQueryInfo) -> Result<i64> {
    // Check `has_deprecated_column_id` first, because `column` is marked as
    // not null in TiDB side so that it will be always populated with some value.
    if ann_query.has_deprecated_column_id() {
        Ok(ann_query.get_deprecated_column_id())
    } else if ann_query.has_column() {
        Ok(ann_query.get_column().get_column_id())
    } else {
        Err(StorageError(anyhow!("unexpected empty vector column id in ANNQueryInfo")).into())
    }
}

fn build_columnar_scanner_internal(
    snap: Option<&kvengine::SnapAccess>,
    key_ranges: &[KeyRange],
    table_scan: &TableScan,
    start_ts: u64,
    ann_query: Option<&AnnQueryInfo>,
    fts_query: Option<&FtsQueryInfo>,
) -> Result<Option<ColumnarScanner>> {
    let snap = match snap {
        Some(s) => s,
        None => return Ok(None),
    };
    if key_ranges.len() != 1 {
        return Ok(None);
    }
    let key_range = &key_ranges[0];
    if ApiV2::parse_key_mode(&key_range.start) != KeyMode::Txn {
        return Ok(None);
    }
    let table_id = table_scan.get_table_id();
    let schema = match snap.new_schema_from_columns(table_id, table_scan.get_columns()) {
        Some(s) => s,
        None => return Ok(None),
    };

    // Decimal encoding in columnar is for TiFlash only. It is not supported TiKV
    // yet.
    if table_scan
        .get_columns()
        .iter()
        .any(|col_info| col_info.get_tp() == FieldTypeTp::NewDecimal.to_u8().unwrap() as i32)
    {
        info!("build_columnar_scanner, decimal is not supported yet");
        return Ok(None);
    }

    let keyspace_id = ApiV2::get_u32_keyspace_id_by_key(&key_range.start).unwrap();
    let start_table_key = &key_range.start[KEYSPACE_PREFIX_LEN..];
    let end_table_key = &key_range.end[KEYSPACE_PREFIX_LEN..];
    let (start_handle, end_handle) = if schema.is_common_handle() {
        let start_handle = table::decode_common_handle(start_table_key)?;
        let end_handle = table::decode_common_handle(end_table_key)?;
        (start_handle.to_vec(), Some(end_handle.to_vec()))
    } else {
        let start_handle = table::decode_int_handle(start_table_key).unwrap_or(i64::MIN);
        let end_handle = {
            let h = table::decode_int_handle(end_table_key).unwrap_or(i64::MAX);
            if h == i64::MAX && end_table_key.len() > PREFIX_LEN + 8 {
                // If the end handle is i64::MAX appended a 0, then we need to include the
                // i64::MAX value, use None to represent there is no upper bound.
                None
            } else {
                Some(h.to_le_bytes().to_vec())
            }
        };
        (start_handle.to_le_bytes().to_vec(), end_handle)
    };
    // Check locks before create columnar reader. Or the locks may be committed
    // after the reader created. It will result to data loss.
    let mut lock_iter = snap.new_iterator(LOCK_CF, false, false, None, true);
    lock_iter.set_range(
        Bytes::copy_from_slice(key_range.get_start()),
        Bytes::copy_from_slice(key_range.get_end()),
    );
    check_locks(&mut lock_iter, start_ts)?;

    if let Some(fts_query) = fts_query {
        // `SnapAccessCore::new_fts_reader` blocks on a small async stub (to cooperate
        // with `maybe_async`), so we must have a Tokio runtime handle here. If
        // there is no runtime in the current context, fall back to row-based
        // scan by returning `None`.
        let runtime = match tokio::runtime::Handle::try_current() {
            Ok(rt) => rt,
            Err(_) => return Ok(None),
        };
        let query = fts::wrap_fts_pb(fts_query.clone())
            .map_err(|e| ErrorInner::Evaluate(EvaluateError::Other(e.to_string())))?;
        fts::validate_schema(&schema, &query)
            .map_err(|e| ErrorInner::Evaluate(EvaluateError::Other(e.to_string())))?;
        // For `SnapAccessCore::new_fts_reader` bounds:
        // - int handles are taken from the *row key* bytes (memcomparable i64)
        // - common handles are taken from the *row key* bytes (raw handle)
        let fts_start_handle = start_table_key
            .get(PREFIX_LEN..)
            .filter(|pk| !pk.is_empty())
            .map(Bytes::copy_from_slice);
        let fts_end_handle = end_table_key
            .get(PREFIX_LEN..)
            .filter(|pk| !pk.is_empty())
            .map(Bytes::copy_from_slice);
        let reader = snap.new_fts_reader(
            &runtime,
            table_id,
            query,
            schema.clone(),
            start_ts,
            fts_start_handle,
            fts_end_handle,
        )?;
        return Ok(Some(ColumnarScanner::new(
            reader,
            get_output_offsets(table_scan),
            keyspace_id,
            key_range.start.clone(),
            (start_handle, end_handle),
        )));
    }

    if ann_query.is_none() {
        // Not a vector search at all
        let mut executor = tipb::Executor::default();
        executor.set_tbl_scan(table_scan.clone());
        let scan_ctx = TableScanCtx::new(executor, vec![]);

        return Ok(snap
            .new_columnar_mvcc_reader(
                table_id,
                table_scan.get_columns(),
                Some(&scan_ctx),
                start_ts,
                None,
            )?
            .map(|r| {
                ColumnarScanner::new(
                    Box::new(r),
                    get_output_offsets(table_scan),
                    keyspace_id,
                    key_range.start.clone(),
                    (start_handle, end_handle),
                )
            }));
    }

    // Below is for vector search.
    let ann_query = Arc::new(ann_query.unwrap().clone());
    let index_id = ann_query.get_index_id();
    let col_id = get_ann_vec_col_id(&ann_query)?;
    let target = ann_query.get_ref_vec_f32().read_vector_float32()?;

    // First try to use vector index reader.
    if let Some(r) = snap.new_vector_index_reader(
        table_id,
        index_id,
        col_id,
        target.as_ref().data(),
        ann_query.get_top_k() as usize,
        schema.clone(),
        start_ts,
        Some(&start_handle),
        end_handle.as_deref(),
        Arc::clone(&ann_query),
    )? {
        return Ok(Some(ColumnarScanner::new(
            Box::new(r),
            get_output_offsets(table_scan),
            keyspace_id,
            key_range.start.clone(),
            (start_handle, end_handle),
        )));
    }

    // If vector index reader is not available, use columnar MVCC reader.
    match snap.new_columnar_mvcc_reader(
        table_id,
        table_scan.get_columns(),
        None,
        start_ts,
        Some(Arc::clone(&ann_query)),
    )? {
        Some(r) => Ok(Some(ColumnarScanner::new(
            Box::new(r),
            get_output_offsets(table_scan),
            keyspace_id,
            key_range.start.clone(),
            (start_handle, end_handle),
        ))),
        None => Ok(None),
    }
}

pub fn build_columnar_scanner(
    snap: Option<&kvengine::SnapAccess>,
    key_ranges: &[KeyRange],
    table_scan: &TableScan,
    start_ts: u64,
) -> Result<Option<ColumnarScanner>> {
    // For compatibility: deprecated_ann_query may be assigned from old TiDB
    // version.
    let ann_query = if table_scan.has_deprecated_ann_query() {
        Some(table_scan.get_deprecated_ann_query())
    } else {
        table_scan
            .get_used_columnar_indexes()
            .iter()
            .find(|idx| idx.has_ann_query_info())
            .map(|idx| idx.get_ann_query_info())
    };

    let is_distance_proj = ann_query
        .map(|q| q.get_enable_distance_proj())
        .unwrap_or(false);

    let fts_query = table_scan
        .get_used_columnar_indexes()
        .iter()
        .find(|idx| idx.has_fts_query_info())
        .map(|idx| idx.get_fts_query_info());

    let scanner = build_columnar_scanner_internal(
        snap, key_ranges, table_scan, start_ts, ann_query, fts_query,
    )?;

    // under `distance proj`, an error will be returned if build columnar reader
    // fails, since reading distance does not yet support normal read by rows.
    if scanner.is_none() && is_distance_proj {
        return Err(StorageError(anyhow!(
            "failed to build columnar scanner with distance projection"
        ))
        .into());
    }

    Ok(scanner)
}

fn check_locks(lock_iter: &mut Iterator, read_ts: u64) -> Result<()> {
    while lock_iter.valid() {
        let raw_key = lock_iter.key();
        let key = Key::from_raw(raw_key);
        let val = lock_iter.val();
        let lock = Lock::parse(val)
            .map_err(|e| ErrorInner::Evaluate(EvaluateError::Other(e.to_string())))?;
        Lock::check_ts_conflict(
            Cow::Borrowed(&lock),
            &key,
            read_ts.into(),
            &TsSet::default(),
            IsolationLevel::Si,
        )
        .map_err(|e| match *e.0 {
            txn_types::ErrorInner::KeyIsLocked(info) => {
                ErrorInner::Evaluate(EvaluateError::KeyIsLocked(info))
            }
            _ => ErrorInner::Evaluate(EvaluateError::Other(e.to_string())),
        })?;
        lock_iter.next();
    }
    Ok(())
}
