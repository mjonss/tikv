// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, btree_map::Range},
    ops::Deref,
    sync::Arc,
};

use api_version::api_v2::KEYSPACE_PREFIX_LEN;
use bytes::{Buf, BufMut};
use collections::HashSet;
use kvenginepb::{SchemaMeta, fts::FullTextIndexDef};
use protobuf::Message;
use schema::schema::StorageClassSpec;
use tidb_query_datatype::codec::table::{
    INDEX_PREFIX_SEP, RECORD_PREFIX_SEP, TABLE_PREFIX, TABLE_PREFIX_KEY_LEN, decode_table_id,
};
use tikv_util::Either;
use tipb::ColumnInfo;

use crate::{
    table::{
        self, ChecksumType, DataBound, InnerKey, NO_COMPRESSION, OwnedInnerKey,
        columnar::{VectorIndexDef, get_fixed_size, new_version_column_info},
        file::File,
    },
    table_id::{encode_table_prefix_key, get_table_id_from_data_bound},
};

pub const SCHEMA_FILE_MAGIC: u32 = 0x5353484D;
pub const SCHEMA_FILE_FORMAT_VER: u16 = 1;

#[derive(Clone)]
pub struct SchemaFile {
    core: Arc<SchemaFileCore>,
}

pub(crate) struct SchemaFileCore {
    file_id: u64,
    keyspace_id: u32,
    version: i64,
    tables: BTreeMap<i64, Schema>,
    restore_version: u64,
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct SchemaFileFooter {
    pub checksum: u32,
    pub checksum_type: u8,
    pub compression_type: u8,
    pub format_version: u16,
    pub magic: u32,
}

impl SchemaFileFooter {
    pub(crate) fn new() -> Self {
        SchemaFileFooter {
            checksum: 0,
            checksum_type: ChecksumType::Crc32.value(),
            compression_type: NO_COMPRESSION,
            format_version: SCHEMA_FILE_FORMAT_VER,
            magic: SCHEMA_FILE_MAGIC,
        }
    }

    pub(crate) fn parse(mut buf: &[u8]) -> Self {
        SchemaFileFooter {
            checksum: buf.get_u32_le(),
            checksum_type: buf.get_u8(),
            compression_type: buf.get_u8(),
            format_version: buf.get_u16_le(),
            magic: buf.get_u32_le(),
        }
    }

    pub(crate) fn write_to(&self, data: &mut Vec<u8>) {
        data.put_u32_le(self.checksum);
        data.put_u8(self.checksum_type);
        data.put_u8(self.compression_type);
        data.put_u16_le(self.format_version);
        data.put_u32_le(self.magic);
    }

    pub const fn footer_size() -> usize {
        std::mem::size_of::<Self>()
    }
}

impl SchemaFile {
    pub fn open(file: Arc<dyn File>) -> table::Result<Self> {
        let file_id = file.id();
        let file_data = file.read(0, file.size() as usize)?;
        let footer_size = SchemaFileFooter::footer_size();
        if file_data.len() < footer_size {
            return Err(table::Error::InvalidFileSize);
        }
        let footer_offset = file_data.len() - footer_size;
        let footer = SchemaFileFooter::parse(&file_data[footer_offset..]);
        let mut data = &file_data[..footer_offset];
        if footer.magic != SCHEMA_FILE_MAGIC {
            return Err(table::Error::InvalidMagicNumber);
        }
        assert_eq!(footer.format_version, SCHEMA_FILE_FORMAT_VER);
        assert_eq!(footer.compression_type, NO_COMPRESSION);
        let checksum_type = ChecksumType::from(footer.checksum_type);
        let got_checksum = checksum_type.checksum(data);
        if got_checksum != footer.checksum {
            return Err(table::Error::InvalidChecksum(format!(
                "expect{}, got:{}",
                footer.checksum, got_checksum
            )));
        }
        let keyspace_id = data.get_u32_le();
        let schema_version = data.get_i64_le();
        let restore_version = data.get_u64_le();
        let mut tables = BTreeMap::default();
        while !data.is_empty() {
            let schema_pb_len = data.get_u32_le() as usize;
            let mut schema_pb = kvenginepb::Schema::new();
            schema_pb.merge_from_bytes(&data[..schema_pb_len]).unwrap();
            data.advance(schema_pb_len);
            let table_id = schema_pb.get_table_id();
            let sc_spec = StorageClassSpec::from_schema_pb(&schema_pb);
            let partitions = if !schema_pb.partitions.is_empty() {
                let mut partition_sc_spec = Vec::with_capacity(schema_pb.partitions.len());
                for partition in &schema_pb.partitions {
                    partition_sc_spec.push((
                        partition.get_id(),
                        StorageClassSpec::from_partition_pb(partition),
                    ));
                }
                Some(partition_sc_spec)
            } else {
                None
            };

            let mut builder = SchemaBufBuilder::new(table_id);
            builder.storage_class_spec(sc_spec);
            builder.partitions(partitions.clone());

            if !schema_pb.columns.is_empty() {
                let mut columns = Vec::with_capacity(schema_pb.columns.len());
                for col_data in &schema_pb.columns {
                    let mut column_info = tipb::ColumnInfo::new();
                    column_info.merge_from_bytes(col_data).unwrap();
                    columns.push(column_info);
                }
                let handle_column = columns.pop().unwrap();
                let vector_indexes = schema_pb
                    .vector_indexes
                    .iter()
                    .map(|vec_idx| {
                        let vec_col = columns
                            .iter()
                            .find(|c| c.get_column_id() == vec_idx.col_id)
                            .unwrap();
                        VectorIndexDef::from_pb(vec_col.get_column_len() as usize, vec_idx)
                    })
                    .collect();

                let fulltext_indexes = schema_pb.fulltext_indexes.iter().cloned().collect();

                builder.columns(
                    handle_column,
                    new_version_column_info(),
                    columns,
                    schema_pb.pk_col_ids,
                    schema_pb.max_col_id,
                    vector_indexes,
                    fulltext_indexes,
                );
            }

            let schema_buf = builder.build();

            // Add partitions to tables.
            if let Some(partitions) = partitions {
                for (partition_id, sc) in partitions {
                    let partition_schema: Schema =
                        schema_buf.to_partition_schema(partition_id, sc).into();
                    tables.insert(partition_id, partition_schema);
                }
            }
            tables.insert(table_id, Schema::new(schema_buf));
        }
        let core = SchemaFileCore {
            file_id,
            keyspace_id,
            version: schema_version,
            restore_version,
            tables,
        };
        Ok(SchemaFile {
            core: Arc::new(core),
        })
    }

    pub fn get_table(&self, table_id: i64) -> Option<&Schema> {
        self.core.tables.get(&table_id)
    }

    pub fn contains_columnar_table(&self, table_id: i64) -> bool {
        self.get_table(table_id)
            .is_some_and(|schema| schema.with_columnar())
    }

    pub fn iter_tables(&self) -> impl Iterator<Item = (/* table_id */ &i64, &Schema)> {
        self.core.tables.iter()
    }

    pub fn get_vector_index_schemas(&self) -> Vec<Schema> {
        let mut schemas = vec![];
        for schema in self.core.tables.values() {
            if !schema.vector_indexes.is_empty() {
                schemas.push(schema.clone())
            }
        }
        schemas
    }

    pub fn get_keyspace_id(&self) -> u32 {
        self.core.keyspace_id
    }

    pub fn get_version(&self) -> i64 {
        self.core.version
    }

    pub fn get_restore_version(&self) -> u64 {
        self.core.restore_version
    }

    pub fn set_restore_version_and_keyspace_id(
        self,
        restore_version: u64,
        keyspace_id: u32,
    ) -> Self {
        Self {
            core: Arc::new(SchemaFileCore {
                file_id: self.core.file_id,
                keyspace_id,
                version: self.core.version,
                restore_version,
                tables: self.core.tables.clone(),
            }),
        }
    }

    pub fn get_file_id(&self) -> u64 {
        self.core.file_id
    }

    pub fn is_tombstone(&self) -> bool {
        self.core.file_id == 0
    }

    // overlap checks if the Shard contains any rows in any of the schema tables.
    pub fn overlap(&self, mut start_key: &[u8], mut end_key: &[u8], keyspace_id: u32) -> bool {
        if self.core.tables.is_empty() {
            return false;
        }
        if keyspace_id != self.get_keyspace_id() {
            return false;
        }
        if keyspace_id > 0 {
            start_key.advance(KEYSPACE_PREFIX_LEN);
            end_key.advance(KEYSPACE_PREFIX_LEN);
        }
        if !end_key.is_empty() && end_key < TABLE_PREFIX {
            // meta data region is not overlapped.
            return false;
        }
        let start_table_id = decode_table_id(start_key).unwrap_or(0);
        let mut end_table_id = decode_table_id(end_key).unwrap_or(i64::MAX);
        let end_lt_index = end_key.len() >= TABLE_PREFIX_KEY_LEN
            && &end_key[TABLE_PREFIX_KEY_LEN..] < INDEX_PREFIX_SEP;
        for (&table_id, schema) in self.core.tables.iter() {
            if schema.get_storage_class_spec().is_specified() {
                if start_table_id == end_table_id && table_id == start_table_id {
                    return true;
                }
                if start_table_id < end_table_id
                    && start_table_id <= table_id
                    && (end_lt_index && table_id < end_table_id
                        || !end_lt_index && table_id <= end_table_id)
                {
                    return true;
                }
            }
        }
        if start_table_id == end_table_id && start_table_id > 0 && start_table_id < i64::MAX {
            // The shard only contains a single table's index is not overlapped.
            if start_key[TABLE_PREFIX_KEY_LEN..].starts_with(INDEX_PREFIX_SEP)
                && end_key[TABLE_PREFIX_KEY_LEN..].starts_with(INDEX_PREFIX_SEP)
            {
                return false;
            }
        }
        if end_key.len() >= TABLE_PREFIX_KEY_LEN
            && &end_key[TABLE_PREFIX_KEY_LEN..] <= RECORD_PREFIX_SEP
        {
            // When end key is smaller than or equal to the table first record key, it
            // doesn't overlap the table.
            end_table_id -= 1;
        }
        self.range_tables(start_table_id, end_table_id)
            .next()
            .is_some()
    }

    // Check if the schema contains any tables in the range of [start_table_id,
    // end_table_id]
    pub fn overlap_columnar_table_ids(&self, start_table_id: i64, end_table_id: i64) -> bool {
        if self.core.tables.is_empty() || start_table_id > end_table_id {
            return false;
        }
        self.range_tables(start_table_id, end_table_id)
            .any(|(_, schema)| schema.with_columnar())
    }

    pub fn overlap_columnar_tables(
        &self,
        smallest: InnerKey<'_>,
        biggest: InnerKey<'_>,
    ) -> Vec<i64> {
        let data_bound = DataBound::new(smallest, biggest, true);
        let (start_table_id, end_table_id) = get_table_id_from_data_bound(data_bound);
        self.range_tables(start_table_id, end_table_id)
            .filter_map(|(&id, schema)| {
                if schema.with_columnar() {
                    Some(id)
                } else {
                    None
                }
            })
            .collect()
    }

    fn range_tables(&self, start_table_id: i64, end_table_id: i64) -> Range<'_, i64, Schema> {
        if start_table_id > end_table_id {
            return Range::default();
        }
        self.core.tables.range(start_table_id..=end_table_id)
    }

    /// Return:
    /// - Left: The id of the table with specified storage class and fully
    ///   covers the range.
    /// - Right: Overlapped prefix keys of tables which requires exclusive
    ///   region.
    pub fn overlap_storage_class_tables(
        &self,
        range: DataBound<'_>,
    ) -> Either<i64 /* table_id */, Vec<OwnedInnerKey> /* overlapped_keys */> {
        let mut overlapped_keys = Vec::new();
        for (&table_id, schema) in self.core.tables.iter() {
            if schema.is_partitioned_table() {
                // Storage class is only set to partitions for partitioned tables.
                continue;
            }

            let sc_spec = schema.get_storage_class_spec();
            if !sc_spec.is_specified() {
                continue;
            }

            let table_start_key = encode_table_prefix_key(table_id);
            let table_end_key = encode_table_prefix_key(table_id + 1);
            let table_bound =
                DataBound::new(table_start_key.as_ref(), table_end_key.as_ref(), false);
            if table_bound.contains_bound(range) {
                return Either::Left(table_id);
            }

            if sc_spec.require_exclusive_region() {
                if range.exclusive_overlap_key(table_start_key.as_ref()) {
                    overlapped_keys.push(table_start_key);
                }
                if range.exclusive_overlap_key(table_end_key.as_ref()) {
                    overlapped_keys.push(table_end_key);
                }
            }
        }

        overlapped_keys.sort();
        overlapped_keys.dedup();
        Either::Right(overlapped_keys)
    }

    pub fn contains(&self, others: &[Schema]) -> bool {
        for schema in others {
            if self.get_table(schema.table_id).is_none()
                || !self.get_table(schema.table_id).unwrap().eq(schema)
            {
                return false;
            }
        }
        true
    }

    pub fn has_overlap_ids(&self, others: &[i64]) -> bool {
        for tbl_id in others {
            if self.core.tables.contains_key(tbl_id) {
                return true;
            }
        }
        false
    }

    // Export all table schemas except the sub partition tables.
    pub fn export_schemas(&self) -> BTreeMap<i64, Schema> {
        self.core
            .tables
            .iter()
            .filter_map(|(id, schema)| {
                if schema.is_sub_partition() {
                    None
                } else {
                    Some((*id, schema.clone()))
                }
            })
            .collect()
    }

    // Export all table ids except the parent partition tables.
    pub fn export_backend_table_ids(&self) -> Vec<i64> {
        self.core
            .tables
            .iter()
            .filter_map(|(&id, schema)| {
                if schema.is_partitioned_table() {
                    None
                } else {
                    Some(id)
                }
            })
            .collect::<Vec<_>>()
    }

    pub fn tables_with_columnar(&self) -> Vec<i64> {
        self.core
            .tables
            .iter()
            .filter_map(|(&table_id, schema)| schema.with_columnar().then_some(table_id))
            .collect()
    }

    // Note: This function return table id of regular & partitioned tables, but not
    // partition id of sub partitions.
    pub fn tables_with_storage_class(&self) -> HashSet<i64> {
        self.core
            .tables
            .iter()
            .filter_map(|(&table_id, schema)| {
                let with_sc_spec = match schema.partitions.as_ref() {
                    Some(partitions) => {
                        partitions.iter().any(|(_, sc_spec)| sc_spec.is_specified())
                    }
                    None => {
                        !schema.is_sub_partition() && schema.get_storage_class_spec().is_specified()
                    }
                };
                with_sc_spec.then_some(table_id)
            })
            .collect()
    }

    pub fn schema_count(&self) -> usize {
        self.core.tables.len()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        build_schema_file(
            self.get_keyspace_id(),
            self.get_version(),
            self.core.tables.values().cloned().collect(),
            self.get_restore_version(),
        )
    }

    pub fn to_schema_meta(&self) -> SchemaMeta {
        let mut schema_meta = SchemaMeta::new();
        schema_meta.set_file_id(self.get_file_id());
        schema_meta.set_version(self.get_version());
        schema_meta.set_keyspace_id(self.get_keyspace_id());
        schema_meta
    }
}

pub fn build_schema_file(
    keyspace_id: u32,
    schema_version: i64,
    tables: Vec<Schema>,
    restore_version: u64,
) -> Vec<u8> {
    let mut data = Vec::new();
    data.put_u32_le(keyspace_id);
    data.put_i64_le(schema_version);
    data.put_u64_le(restore_version);
    for schema in &tables {
        let mut schema_pb = kvenginepb::Schema::default();
        schema
            .get_storage_class_spec()
            .apply_to_schema_pb(&mut schema_pb);
        schema_pb.table_id = schema.table_id;
        schema_pb.max_col_id = schema.max_col_id;
        if schema.with_columnar() {
            let mut columns = Vec::with_capacity(schema.columns.len() + 1);
            columns.extend_from_slice(&schema.columns);
            columns.push(schema.handle_column.clone());
            for col in &columns {
                schema_pb.mut_columns().push(col.write_to_bytes().unwrap());
            }
            schema_pb.set_pk_col_ids(schema.pk_col_ids.clone());
            for vec_idx in &schema.vector_indexes {
                schema_pb.mut_vector_indexes().push(vec_idx.to_pb());
            }
            for fts_idx in &schema.fulltext_indexes {
                schema_pb.mut_fulltext_indexes().push(fts_idx.clone());
            }
        }
        if let Some(partitions) = &schema.partitions {
            for (id, sc_spec) in partitions {
                let mut partition_pb = kvenginepb::Partition::default();
                sc_spec.apply_to_partition_pb(&mut partition_pb);
                partition_pb.set_id(*id);
                schema_pb.mut_partitions().push(partition_pb);
            }
        }
        let schema_pb_data = schema_pb.write_to_bytes().unwrap();
        data.put_u32_le(schema_pb_data.len() as u32);
        data.extend_from_slice(&schema_pb_data);
    }
    let mut footer = SchemaFileFooter::new();
    let checksum_type = ChecksumType::Crc32;
    footer.checksum = checksum_type.checksum(&data);
    footer.write_to(&mut data);
    data
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct Schema {
    core: Arc<SchemaBuf>,
}

impl Deref for Schema {
    type Target = SchemaBuf;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl From<SchemaBuf> for Schema {
    fn from(buf: SchemaBuf) -> Self {
        Self::new(buf)
    }
}

impl Schema {
    pub fn new(buf: SchemaBuf) -> Self {
        Self {
            core: Arc::new(buf),
        }
    }

    pub fn is_common_handle(&self) -> bool {
        get_fixed_size(&self.handle_column) == 0
    }

    pub fn with_columnar(&self) -> bool {
        self.handle_column.has_column_id()
            || !self.columns.is_empty()
            || !self.pk_col_ids.is_empty()
            || !self.vector_indexes.is_empty()
    }

    pub fn find_column_by_id(&self, id: i64) -> Option<&ColumnInfo> {
        if let Some(c) = self.columns.iter().find(|c| c.get_column_id() == id) {
            return Some(c);
        }
        if self.handle_column.get_column_id() == id {
            return Some(&self.handle_column);
        }
        None
    }

    pub fn to_schema_buf(&self) -> SchemaBuf {
        self.deref().clone()
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct SchemaBuf {
    pub inner: Arc<SchemaBufInner>,
    pub partitions: Option<Vec<(i64, StorageClassSpec)>>,
    pub table_id: i64,
    pub sc_spec: StorageClassSpec,
    pub is_sub_partition: bool,
}

impl Deref for SchemaBuf {
    type Target = SchemaBufInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl SchemaBuf {
    pub fn new(
        table_id: i64,
        handle_column: ColumnInfo,
        version_column: ColumnInfo,
        columns: Vec<ColumnInfo>,
        pk_col_ids: Vec<i64>,
        max_col_id: i64,
        vector_indexes: Vec<VectorIndexDef>,
        fulltext_indexes: Vec<FullTextIndexDef>,
        sc_spec: StorageClassSpec,
        partitions: Option<Vec<(i64, StorageClassSpec)>>,
    ) -> Self {
        Self {
            table_id,
            inner: Arc::new(SchemaBufInner {
                handle_column,
                version_column,
                columns,
                pk_col_ids,
                max_col_id,
                vector_indexes,
                fulltext_indexes,
            }),
            partitions,
            sc_spec,
            is_sub_partition: false,
        }
    }

    pub fn retain_columns<F>(&self, f: F) -> Self
    where
        F: FnMut(&ColumnInfo) -> bool,
    {
        let mut columns = self.inner.columns.clone();
        columns.retain(f);
        SchemaBuf::new(
            self.table_id,
            self.handle_column.clone(),
            self.version_column.clone(),
            columns,
            self.pk_col_ids.clone(),
            self.max_col_id,
            self.vector_indexes.clone(),
            self.fulltext_indexes.clone(),
            self.sc_spec.clone(),
            self.partitions.clone(),
        )
    }

    pub fn to_partition_schema(&self, partition_id: i64, sc_spec: StorageClassSpec) -> SchemaBuf {
        SchemaBuf {
            table_id: partition_id,
            inner: self.inner.clone(),
            partitions: None,
            sc_spec,
            is_sub_partition: true,
        }
    }

    pub fn to_partition_schemas(&self) -> Vec<Schema> {
        self.partitions
            .as_ref()
            .map(|btree| {
                btree
                    .iter()
                    .map(|(id, sc_spec)| self.to_partition_schema(*id, sc_spec.clone()).into())
                    .collect()
            })
            .unwrap_or_default()
    }

    #[inline]
    pub fn is_sub_partition(&self) -> bool {
        self.is_sub_partition
    }

    #[cfg(any(test, feature = "testexport"))]
    #[inline]
    pub fn set_storage_class(&mut self, sc: schema::schema::StorageClass) {
        self.set_storage_class_spec(sc.into());
    }

    #[inline]
    pub fn set_storage_class_spec(&mut self, sc_spec: StorageClassSpec) {
        self.sc_spec = sc_spec;
    }

    #[inline]
    pub fn get_storage_class_spec(&self) -> &StorageClassSpec {
        &self.sc_spec
    }

    #[inline]
    pub fn is_partitioned_table(&self) -> bool {
        self.partitions.is_some()
    }

    pub fn remove_pk_col_from_columns(&mut self) {
        let handle_col_id = self.handle_column.get_column_id();
        let mut columns = self.columns.clone();
        columns.retain(|c| c.get_column_id() != handle_col_id);
        self.inner = Arc::new(SchemaBufInner {
            handle_column: self.handle_column.clone(),
            version_column: self.version_column.clone(),
            columns,
            pk_col_ids: self.pk_col_ids.clone(),
            max_col_id: self.max_col_id,
            vector_indexes: self.vector_indexes.clone(),
            fulltext_indexes: self.fulltext_indexes.clone(),
        });
    }
}

#[derive(Default)]
pub struct SchemaBufBuilder {
    table_id: i64,
    handle_column: Option<ColumnInfo>,
    version_column: Option<ColumnInfo>,
    columns: Vec<ColumnInfo>,
    pk_col_ids: Vec<i64>,
    max_col_id: i64,
    vector_indexes: Vec<VectorIndexDef>,
    fulltext_indexes: Vec<FullTextIndexDef>,
    sc_spec: StorageClassSpec,
    partitions: Option<Vec<(i64, StorageClassSpec)>>,
}

impl SchemaBufBuilder {
    pub fn new(table_id: i64) -> Self {
        Self {
            table_id,
            ..Default::default()
        }
    }

    pub fn storage_class_spec(&mut self, sc_spec: StorageClassSpec) -> &mut Self {
        self.sc_spec = sc_spec;
        self
    }

    pub fn partitions(&mut self, partitions: Option<Vec<(i64, StorageClassSpec)>>) -> &mut Self {
        self.partitions = partitions;
        self
    }

    pub fn columns(
        &mut self,
        handle_column: ColumnInfo,
        version_column: ColumnInfo,
        columns: Vec<ColumnInfo>,
        pk_col_ids: Vec<i64>,
        max_col_id: i64,
        vector_indexes: Vec<VectorIndexDef>,
        fulltext_indexes: Vec<FullTextIndexDef>,
    ) -> &mut Self {
        // For compatiable, if max_col_id is not set, try to calculate using the columns
        // id.
        // Note: calculate by columns id is not accurate. e.g. if the column max id is
        // dropped, the calculated max_col_id is smaller than the real max_col_id.
        self.max_col_id = if max_col_id > 0 {
            debug!(
                "table_id: {}, max_col_id from pb: {}",
                self.table_id, max_col_id
            );
            max_col_id
        } else {
            debug!(
                "table_id: {}, max_col_id from columns: {}",
                self.table_id,
                columns.iter().map(|c| c.get_column_id()).max().unwrap_or(0)
            );
            columns.iter().map(|c| c.get_column_id()).max().unwrap_or(0)
        };
        self.handle_column = Some(handle_column);
        self.version_column = Some(version_column);
        self.columns = columns;
        self.pk_col_ids = pk_col_ids;
        self.vector_indexes = vector_indexes;
        self.fulltext_indexes = fulltext_indexes;
        self
    }

    pub fn build(self) -> SchemaBuf {
        SchemaBuf::new(
            self.table_id,
            self.handle_column.unwrap_or_default(),
            self.version_column.unwrap_or_default(),
            self.columns,
            self.pk_col_ids,
            self.max_col_id,
            self.vector_indexes,
            self.fulltext_indexes,
            self.sc_spec,
            self.partitions,
        )
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct SchemaBufInner {
    pub handle_column: ColumnInfo,
    pub version_column: ColumnInfo,
    pub columns: Vec<ColumnInfo>,
    pub pk_col_ids: Vec<i64>,
    pub vector_indexes: Vec<VectorIndexDef>,
    pub fulltext_indexes: Vec<FullTextIndexDef>,
    pub max_col_id: i64,
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use api_version::{ApiV2, api_v2::TIDB_META_KEY_PREFIX};
    use bytes::Bytes;
    use schema::schema::{StorageClass, StorageClassSpec};
    use tidb_query_datatype::{
        Collation::Utf8Mb4GeneralCi, FieldTypeTp, codec::table::RECORD_PREFIX_SEP,
    };
    use tikv_util::codec::number::NumberEncoder;

    use super::*;
    use crate::table::{
        columnar::{new_common_handle_column_info, new_int_handle_column_info},
        file::InMemFile,
        schema_file::SchemaBuf,
    };

    fn new_column_info(id: i64, is_int: bool) -> tipb::ColumnInfo {
        let mut column_info = tipb::ColumnInfo::new();
        column_info.set_column_id(id);
        if is_int {
            column_info.set_tp(FieldTypeTp::LongLong as i32);
        } else {
            column_info.set_tp(FieldTypeTp::VarChar as i32);
            column_info.set_collation(Utf8Mb4GeneralCi as i32);
        }
        column_info
    }

    #[test]
    fn test_schema_file_with_columnar() {
        let keyspace_id = 1;
        let schema_version = 1234i64;
        let schema_1 = Schema::new(SchemaBuf::new(
            10,
            new_common_handle_column_info(),
            new_version_column_info(),
            vec![new_column_info(3, true), new_column_info(4, false)],
            vec![],
            4,
            vec![],
            vec![],
            StorageClassSpec::default(),
            None,
        ));
        let schema_2 = Schema::new(SchemaBuf::new(
            20,
            new_int_handle_column_info(),
            new_version_column_info(),
            vec![new_column_info(3, false), new_column_info(4, true)],
            vec![],
            4,
            vec![],
            vec![],
            StorageClassSpec::default(),
            None,
        ));
        let schemas = vec![schema_1, schema_2];
        let data = build_schema_file(keyspace_id, schema_version, schemas.clone(), 0);
        let file = Arc::new(InMemFile::new(100, data.into()));
        let schema_file = SchemaFile::open(file).unwrap();
        assert_eq!(&schemas[0], schema_file.get_table(10).unwrap());
        assert_eq!(&schemas[1], schema_file.get_table(20).unwrap());
        assert_eq!(schema_version, schema_file.get_version());

        for case in vec![
            OverlapCase {
                start_key: ApiV2::get_txn_keyspace_prefix(keyspace_id),
                end_key: encode_meta_key(keyspace_id, b"def"),
                overlap: false,
            },
            OverlapCase {
                start_key: ApiV2::get_txn_keyspace_prefix(keyspace_id),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 20, false, b""),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 21, true, b""),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                overlap: false,
            },
            OverlapCase {
                start_key: encode_meta_key(keyspace_id, b"abc"),
                end_key: encode_meta_key(keyspace_id, b"def"),
                overlap: false,
            },
            OverlapCase {
                start_key: encode_meta_key(keyspace_id, b"abc"),
                end_key: encode_table_key(keyspace_id, 13, true, b""),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 1, true, b""),
                end_key: encode_table_key(keyspace_id, 10, false, b""),
                overlap: false,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 1, true, b""),
                end_key: encode_table_key(keyspace_id, 10, true, b""),
                overlap: false,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 1, true, b""),
                end_key: encode_table_key(keyspace_id, 10, true, b"0"),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 10, true, b""),
                end_key: encode_table_key(keyspace_id, 13, false, b""),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 10, false, b"012"),
                end_key: encode_table_key(keyspace_id, 10, false, b"123"),
                overlap: false,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 10, false, b"012"),
                end_key: encode_table_key(keyspace_id, 10, true, b"123"),
                overlap: true,
            },
        ] {
            assert_eq!(
                schema_file.overlap(&case.start_key, &case.end_key, keyspace_id),
                case.overlap,
                "{:?}",
                case
            );
        }
    }

    #[test]
    fn test_schema_file_with_storage_class() {
        let keyspace_id = 1;
        let schema_version = 1234i64;
        let mut schema_buf_1 = SchemaBuf::default();
        schema_buf_1.table_id = 10;
        schema_buf_1.set_storage_class(StorageClass::Ia);
        let schema_1 = Schema::new(schema_buf_1);
        let mut schema_buf_2 = SchemaBuf::default();
        schema_buf_2.table_id = 20;
        schema_buf_2.set_storage_class(StorageClass::Standard);
        let schema_2 = Schema::new(schema_buf_2);
        let schemas = vec![schema_1, schema_2];
        let data = build_schema_file(keyspace_id, schema_version, schemas.clone(), 0);
        let file = Arc::new(InMemFile::new(100, data.into()));
        let schema_file = SchemaFile::open(file).unwrap();
        assert_eq!(&schemas[0], schema_file.get_table(10).unwrap());
        assert_eq!(&schemas[1], schema_file.get_table(20).unwrap());
        assert_eq!(schema_version, schema_file.get_version());

        for case in vec![
            OverlapCase {
                start_key: ApiV2::get_txn_keyspace_prefix(keyspace_id),
                end_key: encode_meta_key(keyspace_id, b"def"),
                overlap: false,
            },
            OverlapCase {
                start_key: ApiV2::get_txn_keyspace_prefix(keyspace_id),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 19, true, b""),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 21, false, b""),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                overlap: false,
            },
            OverlapCase {
                start_key: encode_meta_key(keyspace_id, b"abc"),
                end_key: encode_meta_key(keyspace_id, b"def"),
                overlap: false,
            },
            OverlapCase {
                start_key: encode_meta_key(keyspace_id, b"abc"),
                end_key: encode_table_key(keyspace_id, 13, true, b""),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 1, true, b""),
                end_key: encode_table_key(keyspace_id, 9, true, b""),
                overlap: false,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 1, true, b""),
                end_key: encode_table_key(keyspace_id, 10, false, b""),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 10, true, b""),
                end_key: encode_table_key(keyspace_id, 13, false, b""),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 10, false, b"012"),
                end_key: encode_table_key(keyspace_id, 10, false, b"123"),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 10, false, b"012"),
                end_key: encode_table_key(keyspace_id, 10, true, b"123"),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 10, true, b"012"),
                end_key: encode_table_key(keyspace_id, 10, true, b"123"),
                overlap: true,
            },
        ] {
            assert_eq!(
                schema_file.overlap(&case.start_key, &case.end_key, keyspace_id),
                case.overlap,
                "{:?}",
                case
            );
        }
    }

    #[test]
    fn test_schema_file_with_columnar_and_storage_class() {
        let keyspace_id = 1;
        let schema_version = 1234i64;
        let schema_1 = Schema::new(SchemaBuf::new(
            10,
            new_common_handle_column_info(),
            new_version_column_info(),
            vec![new_column_info(3, true), new_column_info(4, false)],
            vec![],
            4,
            vec![],
            vec![],
            StorageClassSpec::default(),
            None,
        ));
        let mut schema_buf_2 = SchemaBuf::new(
            20,
            new_int_handle_column_info(),
            new_version_column_info(),
            vec![new_column_info(3, false), new_column_info(4, true)],
            vec![],
            4,
            vec![],
            vec![],
            StorageClassSpec::default(),
            None,
        );
        schema_buf_2.set_storage_class(StorageClass::Standard);
        let schema_2 = Schema::new(schema_buf_2);
        let mut schema_buf_3 = SchemaBuf::default();
        schema_buf_3.table_id = 30;
        schema_buf_3.set_storage_class(StorageClass::Ia);
        let schema_3 = Schema::new(schema_buf_3);
        let schemas = vec![schema_1, schema_2, schema_3];
        let data = build_schema_file(keyspace_id, schema_version, schemas.clone(), 0);
        let file = Arc::new(InMemFile::new(100, data.into()));
        let schema_file = SchemaFile::open(file).unwrap();
        assert_eq!(&schemas[0], schema_file.get_table(10).unwrap());
        assert_eq!(&schemas[1], schema_file.get_table(20).unwrap());
        assert_eq!(&schemas[2], schema_file.get_table(30).unwrap());
        assert_eq!(schema_version, schema_file.get_version());

        for case in vec![
            OverlapCase {
                start_key: ApiV2::get_txn_keyspace_prefix(keyspace_id),
                end_key: encode_meta_key(keyspace_id, b"def"),
                overlap: false,
            },
            OverlapCase {
                start_key: ApiV2::get_txn_keyspace_prefix(keyspace_id),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 29, true, b""),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 31, false, b""),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                overlap: false,
            },
            OverlapCase {
                start_key: encode_meta_key(keyspace_id, b"abc"),
                end_key: encode_meta_key(keyspace_id, b"def"),
                overlap: false,
            },
            OverlapCase {
                start_key: encode_meta_key(keyspace_id, b"abc"),
                end_key: encode_table_key(keyspace_id, 23, true, b""),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 11, false, b""),
                end_key: encode_table_key(keyspace_id, 19, true, b""),
                overlap: false,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 11, true, b""),
                end_key: encode_table_key(keyspace_id, 20, false, b""),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 20, true, b""),
                end_key: encode_table_key(keyspace_id, 23, false, b""),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 19, true, b"012"),
                end_key: encode_table_key(keyspace_id, 19, true, b"123"),
                overlap: false,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 20, false, b"012"),
                end_key: encode_table_key(keyspace_id, 20, false, b"123"),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 20, false, b"012"),
                end_key: encode_table_key(keyspace_id, 20, true, b"123"),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 20, true, b"012"),
                end_key: encode_table_key(keyspace_id, 20, true, b"123"),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 21, false, b"012"),
                end_key: encode_table_key(keyspace_id, 21, false, b"123"),
                overlap: false,
            },
        ] {
            assert_eq!(
                schema_file.overlap(&case.start_key, &case.end_key, keyspace_id),
                case.overlap,
                "{:?}",
                case
            );
        }
    }

    #[test]
    fn test_contains_schema() {
        let keyspace_id = 1;
        let schema_version = 1234i64;
        let schema_1 = Schema::new(SchemaBuf::new(
            10,
            new_common_handle_column_info(),
            new_version_column_info(),
            vec![new_column_info(3, true), new_column_info(4, false)],
            vec![],
            4,
            vec![],
            vec![],
            StorageClassSpec::default(),
            None,
        ));
        let schema_2 = Schema::new(SchemaBuf::new(
            20,
            new_int_handle_column_info(),
            new_version_column_info(),
            vec![new_column_info(3, false), new_column_info(4, true)],
            vec![],
            4,
            vec![],
            vec![],
            StorageClassSpec::default(),
            None,
        ));
        let schema_3 = Schema::new(SchemaBuf::new(
            30,
            new_int_handle_column_info(),
            new_version_column_info(),
            vec![new_column_info(3, false), new_column_info(4, true)],
            vec![],
            4,
            vec![],
            vec![],
            StorageClassSpec::default(),
            None,
        ));
        let schemas = vec![schema_1.clone(), schema_2.clone()];
        let data = build_schema_file(keyspace_id, schema_version, schemas.clone(), 0);
        let file = Arc::new(InMemFile::new(100, data.into()));
        let schema_file = SchemaFile::open(file).unwrap();
        assert!(schema_file.contains(&schemas));
        let reverse_schemas = vec![schema_2.clone(), schema_1.clone()];
        assert!(schema_file.contains(&reverse_schemas));
        let single_schema = vec![schema_1.clone()];
        assert!(schema_file.contains(&single_schema));
        let more_schema = vec![schema_1, schema_2, schema_3];
        assert!(!schema_file.contains(&more_schema));
    }

    #[test]
    fn test_schema_file_with_partition_tables() {
        let keyspace_id = 1;
        let schema_version = 1234i64;
        let schema_1 = Schema::new(SchemaBuf::new(
            10,
            new_common_handle_column_info(),
            new_version_column_info(),
            vec![new_column_info(3, true), new_column_info(4, false)],
            vec![],
            4,
            vec![],
            vec![],
            StorageClassSpec::default(),
            None,
        ));
        let partitions = vec![
            (21, StorageClassSpec::default()),
            (22, StorageClassSpec::default()),
            (23, StorageClassSpec::default()),
        ];
        let schema_2 = Schema::new(SchemaBuf::new(
            20,
            new_int_handle_column_info(),
            new_version_column_info(),
            vec![new_column_info(3, false), new_column_info(4, true)],
            vec![],
            4,
            vec![],
            vec![],
            StorageClassSpec::default(),
            Some(partitions),
        ));
        let schemas = vec![schema_1, schema_2];
        let data = build_schema_file(keyspace_id, schema_version, schemas.clone(), 0);
        let file = Arc::new(InMemFile::new(100, data.into()));
        let schema_file = SchemaFile::open(file).unwrap();
        assert_eq!(&schemas[0], schema_file.get_table(10).unwrap());
        assert_eq!(&schemas[1], schema_file.get_table(20).unwrap());
        assert_eq!(
            schemas[1].columns,
            schema_file.get_table(21).unwrap().columns
        );
        assert_eq!(
            schemas[1].columns,
            schema_file.get_table(22).unwrap().columns
        );
        assert_eq!(
            schemas[1].columns,
            schema_file.get_table(23).unwrap().columns
        );
        assert_eq!(schema_version, schema_file.get_version());
        for case in vec![
            OverlapCase {
                start_key: ApiV2::get_txn_keyspace_prefix(keyspace_id),
                end_key: encode_meta_key(keyspace_id, b"def"),
                overlap: false,
            },
            OverlapCase {
                start_key: ApiV2::get_txn_keyspace_prefix(keyspace_id),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 20, false, b""),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 21, true, b""),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 23, true, b""),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 24, true, b""),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                overlap: false,
            },
            OverlapCase {
                start_key: encode_meta_key(keyspace_id, b"abc"),
                end_key: encode_meta_key(keyspace_id, b"def"),
                overlap: false,
            },
            OverlapCase {
                start_key: encode_meta_key(keyspace_id, b"abc"),
                end_key: encode_table_key(keyspace_id, 13, true, b""),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 1, true, b""),
                end_key: encode_table_key(keyspace_id, 10, false, b""),
                overlap: false,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 1, true, b""),
                end_key: encode_table_key(keyspace_id, 10, true, b""),
                overlap: false,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 1, true, b""),
                end_key: encode_table_key(keyspace_id, 10, true, b"0"),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 10, true, b""),
                end_key: encode_table_key(keyspace_id, 13, false, b""),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 10, false, b"012"),
                end_key: encode_table_key(keyspace_id, 10, false, b"123"),
                overlap: false,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 10, false, b"012"),
                end_key: encode_table_key(keyspace_id, 10, true, b"123"),
                overlap: true,
            },
            OverlapCase {
                start_key: encode_table_key(keyspace_id, 10, false, b"012"),
                end_key: encode_table_key(keyspace_id, 11, false, b"123"),
                overlap: true,
            },
        ] {
            assert_eq!(
                schema_file.overlap(&case.start_key, &case.end_key, keyspace_id),
                case.overlap,
                "{:?}",
                case
            );
        }
    }

    #[test]
    fn test_schema_file_overlap_storage_class() {
        let keyspace_id = 1;
        let schema_version = 1234i64;

        let schema_1 = Schema::new(SchemaBuf::new(
            10,
            new_common_handle_column_info(),
            new_version_column_info(),
            vec![new_column_info(3, true), new_column_info(4, false)],
            vec![],
            4,
            vec![],
            vec![],
            StorageClass::Ia.into(),
            None,
        ));

        let partitions = vec![
            (21, StorageClassSpec::default()),
            (22, StorageClass::Ia.into()),
            (23, StorageClassSpec::default()),
        ];
        let schema_2 = Schema::new(SchemaBuf::new(
            20,
            new_int_handle_column_info(),
            new_version_column_info(),
            vec![new_column_info(3, false), new_column_info(4, true)],
            vec![],
            4,
            vec![],
            vec![],
            StorageClassSpec::default(),
            Some(partitions),
        ));

        let schemas = vec![schema_1, schema_2];
        let data = build_schema_file(keyspace_id, schema_version, schemas.clone(), 0);
        let file = Arc::new(InMemFile::new(100, data.into()));
        let schema_file = SchemaFile::open(file).unwrap();

        let sc_tables = schema_file.tables_with_storage_class();
        assert_eq!(sc_tables, HashSet::from_iter([10, 20]));

        for case in vec![
            ScOverlapCase {
                start_key: ApiV2::get_keyspace_prefix_by_id(keyspace_id),
                end_key: encode_meta_key(keyspace_id, b"def"),
                expect: Either::Right(vec![]),
            },
            ScOverlapCase {
                start_key: ApiV2::get_keyspace_prefix_by_id(keyspace_id),
                end_key: ApiV2::get_keyspace_prefix_by_id(keyspace_id + 1),
                expect: Either::Right(vec![
                    encode_table_prefix(keyspace_id, 10),
                    encode_table_prefix(keyspace_id, 11),
                    encode_table_prefix(keyspace_id, 22),
                    encode_table_prefix(keyspace_id, 23),
                ]),
            },
            ScOverlapCase {
                start_key: encode_table_key(keyspace_id, 20, false, b""),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                expect: Either::Right(vec![
                    encode_table_prefix(keyspace_id, 22),
                    encode_table_prefix(keyspace_id, 23),
                ]),
            },
            ScOverlapCase {
                start_key: encode_table_key(keyspace_id, 21, true, b""),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                expect: Either::Right(vec![
                    encode_table_prefix(keyspace_id, 22),
                    encode_table_prefix(keyspace_id, 23),
                ]),
            },
            ScOverlapCase {
                start_key: encode_table_key(keyspace_id, 23, true, b""),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                expect: Either::Right(vec![]),
            },
            ScOverlapCase {
                start_key: encode_table_key(keyspace_id, 24, true, b""),
                end_key: ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
                expect: Either::Right(vec![]),
            },
            ScOverlapCase {
                start_key: encode_meta_key(keyspace_id, b"abc"),
                end_key: encode_meta_key(keyspace_id, b"def"),
                expect: Either::Right(vec![]),
            },
            ScOverlapCase {
                start_key: encode_meta_key(keyspace_id, b"abc"),
                end_key: encode_table_key(keyspace_id, 13, true, b""),
                expect: Either::Right(vec![
                    encode_table_prefix(keyspace_id, 10),
                    encode_table_prefix(keyspace_id, 11),
                ]),
            },
            ScOverlapCase {
                start_key: encode_table_key(keyspace_id, 1, true, b""),
                end_key: encode_table_key(keyspace_id, 10, false, b""),
                expect: Either::Right(vec![encode_table_prefix(keyspace_id, 10)]),
            },
            ScOverlapCase {
                start_key: encode_table_key(keyspace_id, 1, true, b""),
                end_key: encode_table_key(keyspace_id, 10, true, b""),
                expect: Either::Right(vec![encode_table_prefix(keyspace_id, 10)]),
            },
            ScOverlapCase {
                start_key: encode_table_key(keyspace_id, 1, true, b""),
                end_key: encode_table_key(keyspace_id, 10, true, b"0"),
                expect: Either::Right(vec![encode_table_prefix(keyspace_id, 10)]),
            },
            ScOverlapCase {
                start_key: encode_table_key(keyspace_id, 10, true, b""),
                end_key: encode_table_key(keyspace_id, 13, false, b""),
                expect: Either::Right(vec![encode_table_prefix(keyspace_id, 11)]),
            },
            ScOverlapCase {
                start_key: encode_table_key(keyspace_id, 10, false, b"012"),
                end_key: encode_table_key(keyspace_id, 10, false, b"123"),
                expect: Either::Left(10),
            },
            ScOverlapCase {
                start_key: encode_table_key(keyspace_id, 10, false, b"012"),
                end_key: encode_table_key(keyspace_id, 10, true, b"123"),
                expect: Either::Left(10),
            },
            ScOverlapCase {
                start_key: encode_table_key(keyspace_id, 21, false, b"012"),
                end_key: encode_table_key(keyspace_id, 21, true, b"123"),
                expect: Either::Right(vec![]),
            },
            ScOverlapCase {
                start_key: encode_table_key(keyspace_id, 22, false, b"012"),
                end_key: encode_table_key(keyspace_id, 22, true, b"123"),
                expect: Either::Left(22),
            },
        ] {
            let range = DataBound::new(
                InnerKey::from_outer_key(&case.start_key),
                InnerKey::from_outer_end_key(&case.end_key),
                false,
            );
            let expect: Either<i64, Vec<OwnedInnerKey>> = match case.expect {
                Either::Left(table_id) => Either::Left(table_id),
                Either::Right(ref keys) => Either::Right(
                    keys.iter()
                        .map(|key| OwnedInnerKey::new(Bytes::copy_from_slice(key)))
                        .collect(),
                ),
            };
            assert_eq!(
                schema_file.overlap_storage_class_tables(range),
                expect,
                "{:?}",
                case
            );
        }
    }

    #[derive(Debug)]
    struct OverlapCase {
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        overlap: bool,
    }

    #[derive(Debug)]
    struct ScOverlapCase {
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        expect: Either<i64, Vec<Vec<u8>>>,
    }

    fn encode_table_key(keyspace_id: u32, table_id: i64, is_row: bool, suffix: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(KEYSPACE_PREFIX_LEN + TABLE_PREFIX_KEY_LEN);
        key.put(ApiV2::get_txn_keyspace_prefix(keyspace_id).as_slice());
        key.put(TABLE_PREFIX);
        key.encode_i64(table_id).unwrap();
        if is_row {
            key.put(RECORD_PREFIX_SEP);
        } else {
            key.put(INDEX_PREFIX_SEP);
        }
        key.put(suffix);
        key
    }

    fn encode_table_prefix(keyspace_id: u32, table_id: i64) -> Vec<u8> {
        let mut key = Vec::with_capacity(KEYSPACE_PREFIX_LEN + TABLE_PREFIX_KEY_LEN);
        key.put(ApiV2::get_txn_keyspace_prefix(keyspace_id).as_slice());
        key.put(TABLE_PREFIX);
        key.encode_i64(table_id).unwrap();
        key
    }

    fn encode_meta_key(keyspace_id: u32, suffix: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(KEYSPACE_PREFIX_LEN + TABLE_PREFIX_KEY_LEN);
        key.put(ApiV2::get_txn_keyspace_prefix(keyspace_id).as_slice());
        key.push(TIDB_META_KEY_PREFIX);
        key.put(suffix);
        key
    }
}
