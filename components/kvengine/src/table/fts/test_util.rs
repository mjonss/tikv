// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{io::Cursor, sync::Arc};

use bytes::Bytes;
use clara_fts::index_for_test;
use kvenginepb::fts::FullTextIndexDef;
use tidb_query_datatype::{
    codec::{
        data_type::ScalarValue,
        datum,
        datum::Datum,
        row::v2::encoder_for_test::{Column, RowEncoder},
        table::{encode_common_handle_row_key, encode_row_key},
    },
    expr::EvalContext,
    FieldTypeAccessor, FieldTypeFlag, FieldTypeTp,
};
use tipb::ColumnInfo;

use crate::{
    table::{
        columnar::{
            Block, ColumnarFile, ColumnarFileBuilder, ColumnarMetaCache, ColumnarTableBuildOptions,
            ColumnarTableBuilder,
        },
        file::{File, InMemFile},
        fts::{
            dedicated_file::{DedicatedFileBuilder, DedicatedFileBuilderOptions, EDedicatedFile},
            iter::{CommonPk, IntPk, PkType},
            packed_file::{PackedFile, PackedFileBuilder, PackedFileBuilderOptions},
            FtsCache,
        },
        memtable::{CfTable, WriteBatch},
        schema_file::{build_schema_file, Schema, SchemaBuf, SchemaFile},
        sstable::{BlockCache, L0Builder, L0Table},
        ChecksumType, InnerKey, SnapVersion, Value,
    },
    Iterator, WRITE_CF,
};

/// Helper utility to build a table [`Schema`] for tests.
///
/// `Pk` determines whether the handle column is int-handle (`IntPk`) or
/// common-handle (`CommonPk`).
///
/// Usage:
/// ```
/// let schema = SchemaBuilder::<IntPk>::new(table_id)
///     .column(1, FieldTypeTp::LongLong)
///     .column(2, FieldTypeTp::String)
///     .schema();
/// ```
pub struct SchemaBuilder<Pk: PkType> {
    table_id: i64,
    columns: Vec<ColumnInfo>,
    pk_col_ids: Vec<i64>,
    fts_idx: Vec<FullTextIndexDef>,
    _marker: std::marker::PhantomData<Pk>,
}

impl<Pk: PkType> SchemaBuilder<Pk> {
    pub fn new(table_id: i64) -> Self {
        Self {
            table_id,
            columns: Vec::new(),
            pk_col_ids: Vec::new(),
            fts_idx: Vec::new(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn column(mut self, column_id: i64, tp: FieldTypeTp) -> Self {
        let mut col = ColumnInfo::new();
        col.set_column_id(column_id);
        col.set_tp(tp as i32);
        self.columns.push(col);
        self
    }

    pub fn column_not_null(mut self, column_id: i64, tp: FieldTypeTp) -> Self {
        let mut col = ColumnInfo::new();
        col.set_column_id(column_id);
        col.set_tp(tp as i32);
        col.set_flag(FieldTypeFlag::NOT_NULL.bits() as i32);
        self.columns.push(col);
        self
    }

    pub fn pk_column(mut self, column_id: i64, tp: FieldTypeTp) -> Self {
        let mut col = ColumnInfo::new();
        col.set_column_id(column_id);
        col.set_tp(tp as i32);
        col.set_flag(FieldTypeFlag::PRIMARY_KEY.bits() as i32);
        self.pk_col_ids.push(column_id);
        self.columns.push(col);
        self
    }

    pub fn column_with_default(
        mut self,
        column_id: i64,
        tp: FieldTypeTp,
        default_val: Vec<u8>,
    ) -> Self {
        let mut col = ColumnInfo::new();
        col.set_column_id(column_id);
        col.set_tp(tp as i32);
        col.set_default_val(default_val);
        self.columns.push(col);
        self
    }

    pub fn pk_col_ids(mut self, pk_col_ids: Vec<i64>) -> Self {
        self.pk_col_ids = pk_col_ids;
        self
    }

    pub fn fts_index(mut self, def: FullTextIndexDef) -> Self {
        self.fts_idx.push(def);
        self
    }

    pub fn schema(self) -> Schema {
        let handle_column = if Pk::IS_INT {
            crate::table::columnar::new_int_handle_column_info()
        } else {
            crate::table::columnar::new_common_handle_column_info()
        };
        let version_column = crate::table::columnar::new_version_column_info();

        Schema::new(SchemaBuf::new(
            self.table_id,
            handle_column,
            version_column,
            self.columns,
            self.pk_col_ids,
            0,
            vec![],
            self.fts_idx,
            Default::default(),
            None, // partitions
        ))
    }
}

/// Helper to build a SchemaFile from a list of referenced schemas for tests.
pub fn new_schema_file(schemas: &[&Schema]) -> SchemaFile {
    let keyspace_id = 1u32;
    let schema_version = 10i64;
    let schema_vec: Vec<Schema> = schemas.iter().map(|s| (*s).clone()).collect();
    let data = build_schema_file(keyspace_id, schema_version, schema_vec, 0);
    let raw = Arc::new(InMemFile::new(9999, Bytes::from(data)));
    SchemaFile::open(raw).unwrap()
}

/// Build an in-memory row `Iterator` (memtable `WriteBatch`) for tests.
///
/// Use [`TestWriteTableDataAppender::v1`] / [`TestWriteTableDataAppender::v2`]
/// to encode row values.
///
/// Usage:
/// ```
/// let iter = new_write_batch()
///     .table::<IntPk>(&schema, |row| {
///         row.v1(1, 10, |datum| {
///             datum(7);
///             datum("row1");
///         });
///     })
///     .finish();
/// ```
pub fn new_write_batch() -> TestWriteBatchBuilder {
    TestWriteBatchBuilder {
        entries: Vec::new(),
    }
}

pub struct TestWriteBatchBuilder {
    entries: Vec<TestWriteEntry>,
}

struct TestWriteEntry {
    key: Vec<u8>,
    meta: u8,
    version: u64,
    value: Vec<u8>,
}

impl TestWriteBatchBuilder {
    pub fn table<Pk: PkType>(
        mut self,
        schema: &Schema,
        rows: impl FnOnce(&mut TestWriteTableDataAppender<Pk>),
    ) -> Self {
        let mut builder = TestWriteTableDataAppender::<Pk> {
            schema: schema.clone(),
            entries: Vec::new(),
            deleted_entries: Vec::new(),
            _marker: std::marker::PhantomData,
        };
        rows(&mut builder);
        self.entries.extend(builder.entries);
        self.entries.extend(builder.deleted_entries);
        self
    }

    pub fn apply_to_cf(mut self, cf_tbl: &CfTable, snap: Option<&crate::SnapAccess>) {
        // For the same key, versions must be inserted from old to new so that
        // SkipList can keep the full version chain.
        self.entries.sort_by(|a, b| match a.key.cmp(&b.key) {
            std::cmp::Ordering::Equal => a.version.cmp(&b.version),
            other => other,
        });

        let mut wb = WriteBatch::new();
        for e in self.entries {
            wb.put(
                InnerKey::from_inner_buf(&e.key),
                e.meta,
                &[],
                e.version,
                &e.value,
            );
        }
        cf_tbl.get_cf(WRITE_CF).put_batch(&mut wb, snap, WRITE_CF);
    }

    pub fn finish_as_cf(self) -> CfTable {
        let cf_tbl = CfTable::new();
        self.apply_to_cf(&cf_tbl, None);
        cf_tbl
    }

    pub fn finish_as_sst_l0(mut self, file_id: u64) -> L0Table {
        // Ensure keys are sorted and versions for the same key are ordered from
        // newest to oldest so the SST builder can encode version chains.
        self.entries.sort_by(|a, b| match a.key.cmp(&b.key) {
            std::cmp::Ordering::Equal => b.version.cmp(&a.version),
            other => other,
        });

        assert!(
            !self.entries.is_empty(),
            "finish_as_sst_l0() requires at least one entry"
        );

        let mut builder = L0Builder::new(
            file_id,
            4 << 10,
            SnapVersion::zero(),
            ChecksumType::Crc32,
            None,
        );
        for e in &self.entries {
            let val = Value::new_with_meta_version(e.meta, e.version, 0, e.value.as_slice());
            builder.add(WRITE_CF, InnerKey::from_inner_buf(&e.key), &val, None);
        }
        let (_, data) = builder.finish();
        let file = Arc::new(InMemFile::new(file_id, data));
        L0Table::new(file, BlockCache::None, false, None)
            .unwrap()
            .unwrap()
    }

    pub fn finish(self) -> Box<dyn Iterator> {
        let cf_tbl = self.finish_as_cf();
        cf_tbl.get_cf(WRITE_CF).new_iterator(false)
    }
}

pub struct TestWriteTableDataAppender<Pk: PkType> {
    schema: Schema,
    entries: Vec<TestWriteEntry>,
    deleted_entries: Vec<TestWriteEntry>,
    _marker: std::marker::PhantomData<Pk>,
}

impl<Pk: PkType> TestWriteTableDataAppender<Pk> {
    pub fn v1(
        &mut self,
        pk: Pk::T<'_>,
        version: u64,
        fill: impl FnOnce(&mut TestWriteRowAppender),
    ) {
        let key = encode_pk_row_key::<Pk>(&self.schema, pk);
        let value = encode_row_value_v1(&self.schema, fill);
        self.entries.push(TestWriteEntry {
            key,
            meta: 0,
            version,
            value,
        });
    }

    pub fn v2(
        &mut self,
        pk: Pk::T<'_>,
        version: u64,
        fill: impl FnOnce(&mut TestWriteRowAppender),
    ) {
        let key = encode_pk_row_key::<Pk>(&self.schema, pk);
        let value = encode_row_value_v2(&self.schema, fill);
        self.entries.push(TestWriteEntry {
            key,
            meta: 0,
            version,
            value,
        });
    }

    pub fn delete(&mut self, pk: Pk::T<'_>, version: u64) {
        // NOTE: For deletes that should shadow data in older tables, apply the
        // resulting write batch with a `SnapAccess` (via `apply_to_cf`) so the
        // memtable keeps the tombstone. `finish_as_cf()` uses `None` and may
        // drop such deletes.
        let key = encode_pk_row_key::<Pk>(&self.schema, pk);
        self.deleted_entries.push(TestWriteEntry {
            key,
            meta: crate::table::BIT_DELETE,
            version,
            value: Vec::new(),
        });
    }

    pub fn raw(&mut self, pk: Pk::T<'_>, version: u64, meta: u8, value: impl Into<Vec<u8>>) {
        let key = encode_pk_row_key::<Pk>(&self.schema, pk);
        self.entries.push(TestWriteEntry {
            key,
            meta,
            version,
            value: value.into(),
        });
    }
}

fn encode_pk_row_key<Pk: PkType>(schema: &Schema, pk: Pk::T<'_>) -> Vec<u8> {
    if Pk::IS_INT {
        debug_assert_eq!(
            std::any::TypeId::of::<Pk::T<'_>>(),
            std::any::TypeId::of::<i64>()
        );
        let pk_val = unsafe { *(&pk as *const Pk::T<'_> as *const i64) };
        encode_row_key(schema.table_id, pk_val)
    } else {
        debug_assert_eq!(
            std::any::TypeId::of::<Pk::T<'_>>(),
            std::any::TypeId::of::<&[u8]>()
        );
        let pk_val = unsafe { *(&pk as *const Pk::T<'_> as *const &[u8]) };
        encode_common_handle_row_key(schema.table_id, pk_val)
    }
}

pub enum TestRowValue {
    Absent,
    Null,
    I64(i64),
    U64(u64),
    F64(f64),
    Bytes(Vec<u8>),
}

pub struct TestWriteRowAppender {
    schema: Schema,
    values: Vec<TestRowValue>,
}

impl<T: TestRowDatum> std::ops::FnOnce<(T,)> for TestWriteRowAppender {
    type Output = ();

    extern "rust-call" fn call_once(mut self, args: (T,)) -> Self::Output {
        self.datum(args.0);
    }
}

impl<T: TestRowDatum> std::ops::FnMut<(T,)> for TestWriteRowAppender {
    extern "rust-call" fn call_mut(&mut self, args: (T,)) -> Self::Output {
        self.datum(args.0);
    }
}

impl TestWriteRowAppender {
    pub fn datum(&mut self, d: impl TestRowDatum) {
        let col_pos = self.values.len();
        if col_pos >= self.schema.columns.len() {
            panic!(
                "too many column values supplied: expected {} according to schema",
                self.schema.columns.len()
            );
        }
        let col_info = &self.schema.columns[col_pos];
        let tp = col_info.as_accessor().tp();
        let is_unsigned = col_info
            .as_accessor()
            .flag()
            .contains(FieldTypeFlag::UNSIGNED);
        self.values.push(d.materialize(tp, is_unsigned));
    }

    pub fn null(&mut self) {
        let col_pos = self.values.len();
        if col_pos >= self.schema.columns.len() {
            panic!(
                "too many column values supplied: expected {} according to schema",
                self.schema.columns.len()
            );
        }
        self.values.push(TestRowValue::Null);
    }

    pub fn absent(&mut self) {
        let col_pos = self.values.len();
        if col_pos >= self.schema.columns.len() {
            panic!(
                "too many column values supplied: expected {} according to schema",
                self.schema.columns.len()
            );
        }
        self.values.push(TestRowValue::Absent);
    }
}

pub trait TestRowDatum {
    fn materialize(&self, field_type: FieldTypeTp, is_unsigned: bool) -> TestRowValue;
}

impl TestRowDatum for i64 {
    fn materialize(&self, field_type: FieldTypeTp, is_unsigned: bool) -> TestRowValue {
        match field_type {
            FieldTypeTp::Tiny
            | FieldTypeTp::Short
            | FieldTypeTp::Int24
            | FieldTypeTp::Long
            | FieldTypeTp::LongLong
            | FieldTypeTp::Duration
            | FieldTypeTp::Year => {
                if is_unsigned {
                    if *self < 0 {
                        panic!("negative value for unsigned column");
                    }
                    TestRowValue::U64(*self as u64)
                } else {
                    TestRowValue::I64(*self)
                }
            }
            _ => panic!("unsupported field type {:?} for i64 datum", field_type),
        }
    }
}

impl TestRowDatum for i32 {
    fn materialize(&self, field_type: FieldTypeTp, is_unsigned: bool) -> TestRowValue {
        let v = *self as i64;
        TestRowDatum::materialize(&v, field_type, is_unsigned)
    }
}

impl TestRowDatum for u64 {
    fn materialize(&self, field_type: FieldTypeTp, _is_unsigned: bool) -> TestRowValue {
        match field_type {
            FieldTypeTp::Tiny
            | FieldTypeTp::Short
            | FieldTypeTp::Int24
            | FieldTypeTp::Long
            | FieldTypeTp::LongLong
            | FieldTypeTp::Duration
            | FieldTypeTp::Year => TestRowValue::U64(*self),
            _ => panic!("unsupported field type {:?} for u64 datum", field_type),
        }
    }
}

impl TestRowDatum for f64 {
    fn materialize(&self, field_type: FieldTypeTp, _is_unsigned: bool) -> TestRowValue {
        match field_type {
            FieldTypeTp::Float | FieldTypeTp::Double => TestRowValue::F64(*self),
            _ => panic!("unsupported field type {:?} for f64 datum", field_type),
        }
    }
}

impl TestRowDatum for String {
    fn materialize(&self, field_type: FieldTypeTp, _is_unsigned: bool) -> TestRowValue {
        match field_type {
            FieldTypeTp::VarChar
            | FieldTypeTp::VarString
            | FieldTypeTp::String
            | FieldTypeTp::TinyBlob
            | FieldTypeTp::Blob
            | FieldTypeTp::MediumBlob
            | FieldTypeTp::LongBlob => TestRowValue::Bytes(self.as_bytes().to_vec()),
            _ => panic!("unsupported field type {:?} for string datum", field_type),
        }
    }
}

impl<'a> TestRowDatum for &'a str {
    fn materialize(&self, field_type: FieldTypeTp, _is_unsigned: bool) -> TestRowValue {
        match field_type {
            FieldTypeTp::VarChar
            | FieldTypeTp::VarString
            | FieldTypeTp::String
            | FieldTypeTp::TinyBlob
            | FieldTypeTp::Blob
            | FieldTypeTp::MediumBlob
            | FieldTypeTp::LongBlob => TestRowValue::Bytes(self.as_bytes().to_vec()),
            _ => panic!("unsupported field type {:?} for &str datum", field_type),
        }
    }
}

impl TestRowDatum for Vec<u8> {
    fn materialize(&self, field_type: FieldTypeTp, _is_unsigned: bool) -> TestRowValue {
        match field_type {
            FieldTypeTp::VarChar
            | FieldTypeTp::VarString
            | FieldTypeTp::String
            | FieldTypeTp::TinyBlob
            | FieldTypeTp::Blob
            | FieldTypeTp::MediumBlob
            | FieldTypeTp::LongBlob => TestRowValue::Bytes(self.clone()),
            _ => panic!("unsupported field type {:?} for bytes datum", field_type),
        }
    }
}

pub fn encode_row_value_v1(
    schema: &Schema,
    fill: impl FnOnce(&mut TestWriteRowAppender),
) -> Vec<u8> {
    let mut row_builder = TestWriteRowAppender {
        schema: schema.clone(),
        values: Vec::new(),
    };
    fill(&mut row_builder);
    if row_builder.values.len() != schema.columns.len() {
        panic!(
            "expected {} column values according to schema, got {}",
            schema.columns.len(),
            row_builder.values.len()
        );
    }

    let mut datums = Vec::new();
    for (col_info, v) in schema.columns.iter().zip(row_builder.values.iter()) {
        if matches!(v, TestRowValue::Absent) {
            continue;
        }
        datums.push(Datum::I64(col_info.get_column_id()));
        match v {
            TestRowValue::Absent => unreachable!(),
            TestRowValue::Null => datums.push(Datum::Null),
            TestRowValue::I64(x) => datums.push(Datum::I64(*x)),
            TestRowValue::U64(x) => datums.push(Datum::U64(*x)),
            TestRowValue::F64(x) => datums.push(Datum::F64(*x)),
            TestRowValue::Bytes(bs) => datums.push(Datum::Bytes(bs.clone())),
        }
    }

    let mut ctx = EvalContext::default();
    datum::encode_value(&mut ctx, &datums).unwrap()
}

pub fn encode_row_value_v2(
    schema: &Schema,
    fill: impl FnOnce(&mut TestWriteRowAppender),
) -> Vec<u8> {
    let mut row_builder = TestWriteRowAppender {
        schema: schema.clone(),
        values: Vec::new(),
    };
    fill(&mut row_builder);
    if row_builder.values.len() != schema.columns.len() {
        panic!(
            "expected {} column values according to schema, got {}",
            schema.columns.len(),
            row_builder.values.len()
        );
    }

    fn scalar_none_for_tp(tp: FieldTypeTp) -> ScalarValue {
        match tp {
            FieldTypeTp::Float | FieldTypeTp::Double => ScalarValue::from(Option::<f64>::None),
            FieldTypeTp::VarChar
            | FieldTypeTp::VarString
            | FieldTypeTp::String
            | FieldTypeTp::TinyBlob
            | FieldTypeTp::Blob
            | FieldTypeTp::MediumBlob
            | FieldTypeTp::LongBlob => ScalarValue::from(Option::<Vec<u8>>::None),
            _ => ScalarValue::from(Option::<i64>::None),
        }
    }

    let mut cols = Vec::new();
    for (col_info, v) in schema.columns.iter().zip(row_builder.values.iter()) {
        let col_id = col_info.get_column_id();
        let tp = col_info.as_accessor().tp();
        let is_unsigned = col_info
            .as_accessor()
            .flag()
            .contains(FieldTypeFlag::UNSIGNED);
        let mut col = match v {
            TestRowValue::Absent => continue,
            TestRowValue::Null => Column::new(col_id, scalar_none_for_tp(tp)).with_tp(tp),
            TestRowValue::I64(x) => Column::new(col_id, *x).with_tp(tp),
            TestRowValue::U64(x) => {
                if *x > i64::MAX as u64 {
                    panic!("u64 value too large for v2 test encoder");
                }
                Column::new(col_id, *x as i64).with_tp(tp).with_unsigned()
            }
            TestRowValue::F64(x) => Column::new(col_id, *x).with_tp(tp),
            TestRowValue::Bytes(bs) => Column::new(col_id, bs.clone()).with_tp(tp),
        };
        if is_unsigned && !col.is_unsigned() {
            col = col.with_unsigned();
        }
        cols.push(col);
    }

    let mut ctx = EvalContext::default();
    let mut buf = vec![];
    buf.write_row(&mut ctx, cols).unwrap();
    buf
}

pub struct Doc<Pk: PkType> {
    pub pk: Pk::T<'static>,
    pub version: u64,
    pub deleted: bool,
    pub text: String,
}

impl<Pk: PkType> Clone for Doc<Pk> {
    fn clone(&self) -> Self {
        Self {
            pk: self.pk,
            version: self.version,
            deleted: self.deleted,
            text: self.text.clone(),
        }
    }
}

pub fn doc(pk: i64, version: u64, deleted: bool, text: impl Into<String>) -> Doc<IntPk> {
    Doc {
        pk,
        version,
        deleted,
        text: text.into(),
    }
}

pub fn doc_common(
    pk: &'static [u8],
    version: u64,
    deleted: bool,
    text: impl Into<String>,
) -> Doc<CommonPk> {
    Doc {
        pk,
        version,
        deleted,
        text: text.into(),
    }
}

struct Lp {
    table_id: i64,
    index_id: i64,
    docs: DocSet,
}

enum DocSet {
    Int(Vec<Doc<IntPk>>),
    Common(Vec<Doc<CommonPk>>),
}

impl DocSet {
    fn is_int(&self) -> bool {
        matches!(self, DocSet::Int(_))
    }

    fn texts(&self) -> Vec<&str> {
        match self {
            DocSet::Int(docs) => docs.iter().map(|doc| doc.text.as_str()).collect(),
            DocSet::Common(docs) => docs.iter().map(|doc| doc.text.as_str()).collect(),
        }
    }
}

impl From<Vec<Doc<IntPk>>> for DocSet {
    fn from(docs: Vec<Doc<IntPk>>) -> Self {
        DocSet::Int(docs)
    }
}

impl From<Vec<Doc<CommonPk>>> for DocSet {
    fn from(docs: Vec<Doc<CommonPk>>) -> Self {
        DocSet::Common(docs)
    }
}

/// Build an in-memory FTS [`PackedFile`] (L0/L1) for tests.
///
/// Chain multiple `.lp(...)` / `.lp_common(...)` calls to add more logical
/// partitions.
///
/// Usage:
/// ```
/// let file = new_packed(10, 0)
///     .lp(10, 1, |d| {
///         d(10, 100, false, "doc10");
///         d(20, 200, false, "doc20");
///     })
///     .finish_as_file();
/// ```
///
/// Use [`TestPackedFileBuilder::lp_common`] to build a file with common-handle
/// PKs.
pub fn new_packed(file_id: u64, snap_version: u64) -> TestPackedFileBuilder {
    TestPackedFileBuilder {
        file_id,
        snap_version,
        lps: Vec::new(),
    }
}

pub struct TestPackedFileBuilder {
    file_id: u64,
    snap_version: u64,
    lps: Vec<Lp>,
}

impl TestPackedFileBuilder {
    #[allow(private_bounds)]
    pub fn lp_with_docs(mut self, table_id: i64, index_id: i64, docs: impl Into<DocSet>) -> Self {
        self.push_lp(table_id, index_id, docs.into());
        self
    }

    pub fn lp<S>(
        mut self,
        table_id: i64,
        index_id: i64,
        fill: impl FnOnce(&mut dyn FnMut(i64, u64, bool, S)),
    ) -> Self
    where
        S: Into<String>,
    {
        let mut docs = Vec::new();
        {
            let mut push = |pk, version, deleted, text| {
                docs.push(doc(pk, version, deleted, text));
            };
            fill(&mut push);
        }
        self.push_lp(table_id, index_id, DocSet::Int(docs));
        self
    }

    pub fn lp_common<S>(
        mut self,
        table_id: i64,
        index_id: i64,
        fill: impl FnOnce(&mut dyn FnMut(&'static str, u64, bool, S)),
    ) -> Self
    where
        S: Into<String>,
    {
        let mut docs = Vec::new();
        {
            let mut push = |pk: &'static str, version, deleted, text| {
                docs.push(doc_common(pk.as_bytes(), version, deleted, text));
            };
            fill(&mut push);
        }
        self.push_lp(table_id, index_id, DocSet::Common(docs));
        self
    }

    fn push_lp(&mut self, table_id: i64, index_id: i64, docs: DocSet) {
        self.lps.push(Lp {
            table_id,
            index_id,
            docs,
        });
    }

    fn into_bytes(self) -> (Bytes, u64) {
        let TestPackedFileBuilder {
            file_id,
            snap_version,
            lps,
        } = self;
        let mut buffer = Vec::new();
        {
            let mut builder = PackedFileBuilder::new(
                Cursor::new(&mut buffer),
                PackedFileBuilderOptions::default(),
            );
            for lp in &lps {
                let key = crate::table::fts::compact::lp_key(lp.table_id, lp.index_id);
                builder
                    .start_lp(lp.table_id, lp.index_id, lp.docs.is_int(), &key)
                    .unwrap();
                match &lp.docs {
                    DocSet::Int(docs) => {
                        for doc in docs {
                            builder
                                .add_pk_int(doc.pk, doc.version, doc.deleted)
                                .unwrap();
                        }
                    }
                    DocSet::Common(docs) => {
                        for doc in docs {
                            builder
                                .add_pk_common(doc.pk, doc.version, doc.deleted)
                                .unwrap();
                        }
                    }
                }
                let texts = lp.docs.texts();
                let dir = index_for_test(&texts).unwrap().finalize_as_dir().unwrap();
                builder.finish_lp(&dir).unwrap();
            }
            builder.finish(snap_version.into()).unwrap();
        }
        (Bytes::from(buffer), file_id)
    }

    pub fn finish_as_bytes(self) -> Bytes {
        self.into_bytes().0
    }

    pub fn finish_as_file(self) -> PackedFile {
        let (bytes, file_id) = self.into_bytes();
        PackedFile::new(
            Arc::new(InMemFile::new(file_id, bytes)),
            FtsCache::disabled(),
        )
        .unwrap()
    }
}

/// Build an in-memory FTS [`EDedicatedFile`] (L2) for tests.
///
/// Dedicated files only support a single LP in tests.
///
/// Usage:
/// ```
/// let file = new_ded(1)
///     .lp(100, 0, |d| {
///         d(1, 100, false, "doc1");
///         d(2, 90, false, "doc2");
///     })
///     .finish_as_file();
/// ```
pub fn new_ded(file_id: u64) -> TestDedicatedFileBuilder {
    TestDedicatedFileBuilder { file_id, lp: None }
}

pub struct TestDedicatedFileBuilder {
    file_id: u64,
    lp: Option<Lp>,
}

impl TestDedicatedFileBuilder {
    #[allow(private_bounds)]
    pub fn lp_with_docs(mut self, table_id: i64, index_id: i64, docs: impl Into<DocSet>) -> Self {
        self.set_lp(table_id, index_id, docs.into());
        self
    }

    pub fn lp<S>(
        mut self,
        table_id: i64,
        index_id: i64,
        fill: impl FnOnce(&mut dyn FnMut(i64, u64, bool, S)),
    ) -> Self
    where
        S: Into<String>,
    {
        let mut docs = Vec::new();
        {
            let mut push = |pk, version, deleted, text| {
                docs.push(doc(pk, version, deleted, text));
            };
            fill(&mut push);
        }
        self.set_lp(table_id, index_id, DocSet::Int(docs));
        self
    }

    fn set_lp(&mut self, table_id: i64, index_id: i64, docs: DocSet) {
        if self.lp.is_some() {
            panic!("Dedicated files only support a single LP in tests");
        }
        self.lp = Some(Lp {
            table_id,
            index_id,
            docs,
        });
    }

    fn into_bytes(self) -> (Bytes, u64) {
        let TestDedicatedFileBuilder { file_id, lp } = self;
        let lp = lp.expect("Dedicated file must contain exactly one LP");
        let mut buffer = Vec::new();

        fn inner<Pk: PkType>(buffer: &mut Vec<u8>, lp: &Lp, docs: &Vec<Doc<Pk>>) {
            let mut builder: DedicatedFileBuilder<_, Pk> = DedicatedFileBuilder::new(
                Cursor::new(buffer),
                DedicatedFileBuilderOptions::default(),
                lp.table_id,
                lp.index_id,
                &crate::table::fts::compact::lp_key(lp.table_id, lp.index_id),
            )
            .unwrap();
            for doc in docs {
                builder.add_pk(doc.pk, doc.version, doc.deleted).unwrap();
            }
            let texts = lp.docs.texts();
            let dir = index_for_test(&texts).unwrap().finalize_as_dir().unwrap();
            builder.finish(&dir).unwrap();
        }

        match &lp.docs {
            DocSet::Int(docs) => {
                inner::<IntPk>(&mut buffer, &lp, docs);
            }
            DocSet::Common(docs) => {
                inner::<CommonPk>(&mut buffer, &lp, docs);
            }
        }
        (Bytes::from(buffer), file_id)
    }

    pub fn finish_as_bytes(self) -> Bytes {
        self.into_bytes().0
    }

    pub fn finish_as_file(self) -> EDedicatedFile {
        let (bytes, file_id) = self.into_bytes();
        EDedicatedFile::new(
            Arc::new(InMemFile::new(file_id, bytes)),
            FtsCache::disabled(),
        )
        .unwrap()
    }
}

/// Build an in-memory [`ColumnarFile`] for tests.
///
/// Chain multiple `.table(...)` calls to add more tables.
///
/// Usage:
/// ```
/// let file = new_columnar(1, 0)
///     .table::<IntPk>(&schema, |row| {
///         row(0, 1000, false, |datum| {
///             datum("abc first entry");
///         });
///     })
///     .finish_as_file();
/// ```
pub fn new_columnar(file_id: u64, snap_version: u64) -> TestColumnarFileBuilder {
    TestColumnarFileBuilder {
        file_id,
        snap_version,
        tables: Vec::new(),
    }
}

/// Build an in-memory columnar `Block` for tests following the schema.
///
/// Usage:
/// ```
/// let block = new_block::<IntPk>(&schema, |row| {
///     row(1, 10, false, |datum| {
///         datum(10);
///         datum("foo");
///     });
/// });
/// ```
///
/// For common PK:
///
/// ```
/// let block = new_block::<CommonPk>(&schema, |row| {
///     row("key1", 10, false, |datum| { ... });
///     row(vec, 20, false, |datum| { ... });
/// });
/// ```
pub fn new_block<Pk: PkType>(
    schema: &Schema,
    rows: impl FnOnce(&mut TestColumnarTableDataAppender<Pk>),
) -> Block {
    let mut builder = TestColumnarTableDataAppender::<Pk> {
        schema: schema.clone(),
        rows: Vec::new(),
        _marker: std::marker::PhantomData,
    };
    rows(&mut builder);
    build_block_from_rows(schema, &builder.rows)
}

pub struct TestColumnarFileBuilder {
    file_id: u64,
    snap_version: u64,
    tables: Vec<TestColumnarTable>,
}

struct TestColumnarTable {
    schema: Schema,
    rows: Vec<TestColumnarRowCommon>,
}

struct TestColumnarRowCommon {
    pk: Vec<u8>,
    version: u64,
    deleted: bool,
    values: Vec<ColumnarValue>,
}

struct ColumnarValue {
    data: Vec<u8>,
    is_null: bool,
}

impl TestColumnarFileBuilder {
    pub fn table<Pk: PkType>(
        mut self,
        schema: &Schema,
        rows: impl FnOnce(&mut TestColumnarTableDataAppender<Pk>),
    ) -> Self {
        let mut builder = TestColumnarTableDataAppender::<Pk> {
            schema: schema.clone(),
            rows: Vec::new(),
            _marker: std::marker::PhantomData,
        };
        rows(&mut builder);
        self.tables.push(TestColumnarTable {
            schema: schema.clone(),
            rows: builder.rows,
        });
        self
    }

    fn into_bytes(self) -> (Bytes, u64) {
        let TestColumnarFileBuilder {
            file_id,
            snap_version,
            tables,
        } = self;
        let mut file_builder = ColumnarFileBuilder::new(file_id, Some(snap_version.into()), None);
        for table in tables {
            let block = build_block_from_rows(&table.schema, &table.rows);
            if block.length() == 0 {
                continue;
            }
            let mut table_builder = ColumnarTableBuilder::new(
                table.schema.clone(),
                ColumnarTableBuildOptions::default(),
                None,
                file_id,
                0,
            );
            let mut offset = 0;
            while offset < block.length() {
                offset = table_builder.append_block(&block, offset);
            }
            file_builder.add_table(table_builder);
        }
        let (file_data, _) = file_builder.build();
        (Bytes::from(file_data), file_id)
    }

    pub fn finish_as_bytes(self) -> Bytes {
        self.into_bytes().0
    }

    pub fn finish_as_file(self) -> ColumnarFile {
        let (bytes, file_id) = self.into_bytes();
        let file: Arc<dyn File> = Arc::new(InMemFile::new(file_id, bytes));
        ColumnarFile::open(file, None, ColumnarMetaCache::default()).unwrap()
    }
}

fn build_block_from_rows(schema: &Schema, rows: &[TestColumnarRowCommon]) -> Block {
    let mut block = Block::new(schema);
    for row in rows {
        block.handles.push_value(&row.pk);
        block.versions.push_version(row.version, row.deleted);
        for (column, value) in block.columns.iter_mut().zip(row.values.iter()) {
            if value.is_null {
                column.push_null();
            } else {
                column.push_value(&value.data);
            }
        }
    }
    block
}

pub struct TestColumnarTableDataAppender<Pk: PkType> {
    schema: Schema,
    rows: Vec<TestColumnarRowCommon>,
    _marker: std::marker::PhantomData<Pk>,
}

impl<'a, Pk: PkType, F: FnOnce(&mut TestColumnarTableRowAppender)>
    std::ops::FnOnce<(Pk::T<'a>, u64, bool, F)> for TestColumnarTableDataAppender<Pk>
{
    type Output = ();

    extern "rust-call" fn call_once(mut self, args: (Pk::T<'a>, u64, bool, F)) -> Self::Output {
        let (pk, version, deleted, fill) = args;
        self.fn_call(pk, version, deleted, fill);
    }
}

impl<'a, Pk: PkType, F: FnOnce(&mut TestColumnarTableRowAppender)>
    std::ops::FnMut<(Pk::T<'a>, u64, bool, F)> for TestColumnarTableDataAppender<Pk>
{
    extern "rust-call" fn call_mut(&mut self, args: (Pk::T<'a>, u64, bool, F)) -> Self::Output {
        let (pk, version, deleted, fill) = args;
        self.fn_call(pk, version, deleted, fill);
    }
}

impl<Pk: PkType> TestColumnarTableDataAppender<Pk> {
    fn fn_call<'a>(
        &mut self,
        pk: Pk::T<'a>,
        version: u64,
        deleted: bool,
        datums: impl FnOnce(&mut TestColumnarTableRowAppender),
    ) {
        let mut row_builder = TestColumnarTableRowAppender {
            schema: self.schema.clone(),
            values: Vec::new(),
        };
        datums(&mut row_builder);
        if row_builder.values.len() != self.schema.columns.len() {
            panic!(
                "expected {} column values according to schema, got {}",
                self.schema.columns.len(),
                row_builder.values.len()
            );
        }
        let pk_bytes = if std::any::TypeId::of::<Pk>() == std::any::TypeId::of::<IntPk>() {
            debug_assert_eq!(
                std::any::TypeId::of::<Pk::T<'_>>(),
                std::any::TypeId::of::<i64>()
            );
            let pk_val = unsafe { *(&pk as *const Pk::T<'a> as *const i64) };
            pk_val.to_le_bytes().to_vec()
        } else if std::any::TypeId::of::<Pk>() == std::any::TypeId::of::<CommonPk>() {
            debug_assert_eq!(
                std::any::TypeId::of::<Pk::T<'_>>(),
                std::any::TypeId::of::<&'_ [u8]>()
            );
            let pk_val = unsafe { *(&pk as *const Pk::T<'a> as *const &'a [u8]) };
            pk_val.to_vec()
        } else {
            panic!("Unsupported Pk type");
        };

        self.rows.push(TestColumnarRowCommon {
            pk: pk_bytes,
            version,
            deleted,
            values: row_builder.values,
        });
    }
}

pub struct TestColumnarTableRowAppender {
    schema: Schema,
    values: Vec<ColumnarValue>,
}

impl<T: TestColumnarDatum> std::ops::FnOnce<(T,)> for TestColumnarTableRowAppender {
    type Output = ();

    extern "rust-call" fn call_once(mut self, args: (T,)) -> Self::Output {
        self.fn_call(args.0);
    }
}

impl<T: TestColumnarDatum> std::ops::FnMut<(T,)> for TestColumnarTableRowAppender {
    extern "rust-call" fn call_mut(&mut self, args: (T,)) -> Self::Output {
        self.fn_call(args.0);
    }
}

impl TestColumnarTableRowAppender {
    fn fn_call(&mut self, d: impl TestColumnarDatum) {
        let col_pos = self.values.len();
        if col_pos >= self.schema.columns.len() {
            panic!(
                "too many column values supplied: expected {} according to schema",
                self.schema.columns.len()
            );
        }
        let field_type = self.schema.columns[col_pos].as_accessor().tp();
        let is_unsigned = self.schema.columns[col_pos]
            .as_accessor()
            .flag()
            .contains(FieldTypeFlag::UNSIGNED);
        match d.materialize(field_type, is_unsigned) {
            Some(data) => self.values.push(ColumnarValue {
                data,
                is_null: false,
            }),
            None => self.values.push(ColumnarValue {
                data: Vec::new(),
                is_null: true,
            }),
        }
    }

    pub fn null(&mut self) {
        let col_pos = self.values.len();
        if col_pos >= self.schema.columns.len() {
            panic!(
                "too many column values supplied: expected {} according to schema",
                self.schema.columns.len()
            );
        }
        self.values.push(ColumnarValue {
            data: Vec::new(),
            is_null: true,
        });
    }
}

pub trait TestColumnarDatum {
    fn materialize(&self, field_type: FieldTypeTp, is_unsigned: bool) -> Option<Vec<u8>>;
}

impl TestColumnarDatum for i64 {
    fn materialize(&self, field_type: FieldTypeTp, is_unsigned: bool) -> Option<Vec<u8>> {
        let bytes = match field_type {
            FieldTypeTp::Tiny
            | FieldTypeTp::Short
            | FieldTypeTp::Int24
            | FieldTypeTp::Long
            | FieldTypeTp::LongLong
            | FieldTypeTp::Duration
            | FieldTypeTp::Year => {
                if is_unsigned {
                    if *self < 0 {
                        panic!("negative value for unsigned column");
                    }
                    (*self as u64).to_le_bytes().to_vec()
                } else {
                    self.to_le_bytes().to_vec()
                }
            }
            FieldTypeTp::Timestamp
            | FieldTypeTp::DateTime
            | FieldTypeTp::Date
            | FieldTypeTp::NewDate
            | FieldTypeTp::Bit
            | FieldTypeTp::Enum => {
                if *self < 0 {
                    panic!("negative value for unsigned-compatible column");
                }
                (*self as u64).to_le_bytes().to_vec()
            }
            _ => panic!("unsupported field type {:?} for i64 datum", field_type),
        };
        Some(bytes)
    }
}

impl TestColumnarDatum for i32 {
    fn materialize(&self, field_type: FieldTypeTp, is_unsigned: bool) -> Option<Vec<u8>> {
        let v = *self as i64;
        TestColumnarDatum::materialize(&v, field_type, is_unsigned)
    }
}

impl TestColumnarDatum for u64 {
    fn materialize(&self, field_type: FieldTypeTp, _is_unsigned: bool) -> Option<Vec<u8>> {
        let bytes = match field_type {
            FieldTypeTp::Tiny
            | FieldTypeTp::Short
            | FieldTypeTp::Int24
            | FieldTypeTp::Long
            | FieldTypeTp::LongLong
            | FieldTypeTp::Timestamp
            | FieldTypeTp::DateTime
            | FieldTypeTp::Date
            | FieldTypeTp::NewDate
            | FieldTypeTp::Bit
            | FieldTypeTp::Enum
            | FieldTypeTp::Duration
            | FieldTypeTp::Year => self.to_le_bytes().to_vec(),
            _ => panic!("unsupported field type {:?} for u64 datum", field_type),
        };
        Some(bytes)
    }
}

impl TestColumnarDatum for f64 {
    fn materialize(&self, field_type: FieldTypeTp, _is_unsigned: bool) -> Option<Vec<u8>> {
        let bytes = match field_type {
            FieldTypeTp::Float | FieldTypeTp::Double => self.to_le_bytes().to_vec(),
            _ => panic!("unsupported field type {:?} for f64 datum", field_type),
        };
        Some(bytes)
    }
}

impl TestColumnarDatum for String {
    fn materialize(&self, field_type: FieldTypeTp, _is_unsigned: bool) -> Option<Vec<u8>> {
        let bytes = match field_type {
            FieldTypeTp::VarChar
            | FieldTypeTp::VarString
            | FieldTypeTp::String
            | FieldTypeTp::TinyBlob
            | FieldTypeTp::Blob
            | FieldTypeTp::MediumBlob
            | FieldTypeTp::LongBlob => self.as_bytes().to_vec(),
            _ => panic!("unsupported field type {:?} for string datum", field_type),
        };
        Some(bytes)
    }
}

impl<'a> TestColumnarDatum for &'a str {
    fn materialize(&self, field_type: FieldTypeTp, _is_unsigned: bool) -> Option<Vec<u8>> {
        let bytes = match field_type {
            FieldTypeTp::VarChar
            | FieldTypeTp::VarString
            | FieldTypeTp::String
            | FieldTypeTp::TinyBlob
            | FieldTypeTp::Blob
            | FieldTypeTp::MediumBlob
            | FieldTypeTp::LongBlob => self.as_bytes().to_vec(),
            _ => panic!("unsupported field type {:?} for &str datum", field_type),
        };
        Some(bytes)
    }
}

impl<'a> TestColumnarDatum for &'a [u8] {
    fn materialize(&self, field_type: FieldTypeTp, _is_unsigned: bool) -> Option<Vec<u8>> {
        let bytes = match field_type {
            FieldTypeTp::VarChar
            | FieldTypeTp::VarString
            | FieldTypeTp::String
            | FieldTypeTp::TinyBlob
            | FieldTypeTp::Blob
            | FieldTypeTp::MediumBlob
            | FieldTypeTp::LongBlob => (*self).to_vec(),
            _ => panic!("unsupported field type {:?} for &[u8] datum", field_type),
        };
        Some(bytes)
    }
}
