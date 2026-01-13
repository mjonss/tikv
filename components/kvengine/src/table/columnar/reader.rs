// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::{Ordering, min},
    collections::HashMap,
    mem,
    ops::Deref,
    sync::Arc,
    time::Duration,
};

use aligned_vec::{AVec, avec};
use api_version::ApiV2;
use arrow_buffer::i256;
use async_trait::async_trait;
use bytes::Buf;
use cloud_encryption::EncryptionKey;
use futures::future::try_join_all;
use tidb_query_datatype::{
    FieldTypeFlag, FieldTypeTp,
    codec::{
        Datum, datum,
        datum::{
            BYTES_FLAG, COMPACT_BYTES_FLAG, DECIMAL_FLAG, DURATION_FLAG, FLOAT_FLAG, INT_FLAG,
            JSON_FLAG, NIL_FLAG, UINT_FLAG, VAR_INT_FLAG, VAR_UINT_FLAG, VECTOR_FLOAT32_FLAG,
            decode,
        },
        mysql::{Decimal, DecimalDecoder, JsonEncoder, VectorFloat32Encoder},
        row::v2::{CODEC_VERSION, RowSlice, decode_v2_i64, decode_v2_u64},
        table::{
            PREFIX_LEN, decode_common_handle, decode_int_handle, encode_common_handle_row_key,
            encode_row_key,
        },
    },
};
use tikv_util::{
    codec::{
        BytesSlice,
        bytes::{decode_bytes, decode_compact_bytes},
        number::{decode_f64, decode_i64, decode_u64, decode_var_i64, decode_var_u64},
    },
    deadline::Deadline,
};
use tipb::ColumnInfo;

use super::filter::{FilterOpResult, FilterOpResults, FilterOperator};
use crate::{
    WRITE_CF,
    dfs::FileType,
    ia::{manager::IaManager, types::FileSegmentIdent},
    table::{
        self, InnerKey,
        blobtable::blobtable::BlobTable,
        columnar::{
            columnar::{
                Block, ColumnBuffer, ColumnMeta, ColumnarFile, TableMeta, decompress_pack,
                get_fixed_size,
            },
            get_primary_key,
        },
        file::File,
        schema_file::Schema,
        search,
    },
};

pub const GLOBAL_COMMON_HANDLE_END: &[u8] = &[255];
const PACK_CACHE_THRESHOLD: usize = 256 * 1024; // 256KB

#[async_trait]
pub trait ColumnarReader: Send {
    fn schema(&self) -> &Schema;
    async fn seek(&mut self, handle: &[u8]) -> crate::table::Result<()>;
    async fn read(&mut self, block: &mut Block, limit: usize) -> crate::table::Result<usize>;
    fn reset(&mut self) -> crate::table::Result<()> {
        Ok(())
    }
    fn get_remote_segments(&self) -> crate::table::Result<(Vec<FileSegmentIdent>, usize)> {
        Ok((vec![], 0))
    }
}

pub(crate) struct ColumnarTableReader {
    table_meta: Arc<TableMeta>,
    schema: Schema,
    handle_reader: ColumnarColumnReader,
    version_reader: ColumnarColumnReader,
    columns_readers: Vec<ColumnarColumnReader>,
}

#[allow(dead_code)]
impl ColumnarTableReader {
    pub fn new(
        columnar_file: &ColumnarFile,
        schema: Schema,
        filter_op: Option<FilterOperator>,
        encryption_key: Option<EncryptionKey>,
    ) -> ColumnarTableReader {
        let table_meta = columnar_file.get_table(schema.table_id);
        debug_assert_eq!(table_meta.table_id, schema.table_id);
        let packs_filter = filter_op
            .as_ref()
            .map(|op| Arc::new(op.rough_check(&table_meta)));
        let file = columnar_file.get_file();
        let encryption_ver = columnar_file.get_encryption_ver();
        let handle_reader = ColumnarColumnReader::new(
            file.clone(),
            table_meta.handle_column.clone(),
            false,
            encryption_key.clone(),
            encryption_ver,
            packs_filter.clone(),
        );
        let version_reader = ColumnarColumnReader::new(
            file.clone(),
            table_meta.version_column.clone(),
            false,
            encryption_key.clone(),
            encryption_ver,
            packs_filter.clone(),
        );
        let columns_readers = schema
            .columns
            .iter()
            .map(|col| {
                let col_id = col.get_column_id() as i32;
                let packs_filter = if get_fixed_size(col) == 0 {
                    None
                } else {
                    packs_filter.clone()
                };
                if let Some(col_meta) = table_meta.columns.get(&col_id) {
                    ColumnarColumnReader::new(
                        file.clone(),
                        col_meta.clone(),
                        false,
                        encryption_key.clone(),
                        encryption_ver,
                        packs_filter,
                    )
                } else {
                    let col_meta = ColumnMeta::new(col.clone(), false);
                    ColumnarColumnReader::new(
                        file.clone(),
                        Arc::new(col_meta),
                        true,
                        encryption_key.clone(),
                        encryption_ver,
                        packs_filter,
                    )
                }
            })
            .collect();
        ColumnarTableReader {
            table_meta,
            schema,
            handle_reader,
            version_reader,
            columns_readers,
        }
    }

    pub fn get_file(&self) -> Arc<dyn File> {
        self.handle_reader.pack_loader.file.clone()
    }
}

#[async_trait]
impl ColumnarReader for ColumnarTableReader {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    async fn seek(&mut self, handle: &[u8]) -> crate::table::Result<()> {
        let mut pack_idx = self.table_meta.handle_index.search_pack_idx(handle);
        while self.handle_reader.load_pack(pack_idx).await? {
            if pack_idx >= self.handle_reader.num_packs() {
                break;
            }
            // If the pack is skipped, try load next pack. Keep the row position align with
            // other column readers.
            pack_idx += 1;
        }
        let handle_buffer = &self.handle_reader.pack_buffer;
        let row_idx_in_pack = if handle.is_empty() {
            0
        } else if self.handle_reader.col_meta.fixed_size > 0 {
            let int_handle = (&handle[..]).get_i64_le();
            search(handle_buffer.length(), |i| {
                handle_buffer.get_int_handle_value(i) >= int_handle
            })
        } else {
            search(handle_buffer.length(), |i| {
                handle_buffer.get_not_null_value(i) >= handle
            })
        };
        self.handle_reader.row_idx_in_pack = row_idx_in_pack;
        let row_idx = self.handle_reader.pack_row_start + row_idx_in_pack;
        self.version_reader.set_row_idx(row_idx).await?;
        for col_reader in &mut self.columns_readers {
            col_reader.set_row_idx(row_idx).await?;
        }
        Ok(())
    }

    async fn read(&mut self, block: &mut Block, limit: usize) -> crate::table::Result<usize> {
        // Read version column first to check if the pack is marked as skipped. If this
        // pack is marked as skipped, we should set_row_idx to other column readers,
        // including handle column if it is common handle.
        let mut total_read_row = 0;
        while total_read_row < limit {
            if !self.version_reader.valid() {
                break;
            }
            // If the pack is skipped, the read will return ASAP, in this case we should
            // continue to read if total_read_row is less than limit.
            let (read_row, is_skipped, row_idx) =
                self.version_reader.read(&mut block.versions, limit).await?;
            if is_skipped {
                let (handle_read_row, handle_is_skipped, ..) = self
                    .handle_reader
                    .read(&mut block.handles, read_row)
                    .await?;
                assert_eq!(
                    read_row,
                    handle_read_row,
                    "read_row: {}, handle_read_row: {}, version_pack_idx: {}, pack_idx: {}, version_num_packs: {}, num_packs: {}, handle_is_skipped: {}",
                    read_row,
                    handle_read_row,
                    self.version_reader.pack_idx(),
                    self.handle_reader.pack_idx(),
                    self.version_reader.num_packs(),
                    self.handle_reader.num_packs(),
                    handle_is_skipped
                );
                self.handle_reader.set_row_idx(row_idx).await?;
                for (i, col) in self.columns_readers.iter_mut().enumerate() {
                    let (col_read_row, col_is_skipped, ..) =
                        col.read(&mut block.columns[i], read_row).await?;
                    assert_eq!(
                        read_row,
                        col_read_row,
                        "read_row: {}, col_read_row: {}, col_pack_idx: {}, pack_idx: {}, col_num_packs: {}, num_packs: {}, col_is_skipped: {}",
                        read_row,
                        col_read_row,
                        col.pack_idx(),
                        self.version_reader.pack_idx(),
                        col.num_packs(),
                        self.version_reader.num_packs(),
                        col_is_skipped
                    );
                    col.set_row_idx(row_idx).await?;
                }
                total_read_row += read_row;
            } else {
                let (handle_read_row, handle_is_skipped, _) = self
                    .handle_reader
                    .read(&mut block.handles, read_row)
                    .await?;
                assert_eq!(
                    read_row,
                    handle_read_row,
                    "read_row: {}, handle_read_row: {}, version_pack_idx: {}, pack_idx: {}, version_num_packs: {}, num_packs: {}, handle_is_skipped: {}",
                    read_row,
                    handle_read_row,
                    self.version_reader.pack_idx(),
                    self.handle_reader.pack_idx(),
                    self.version_reader.num_packs(),
                    self.handle_reader.num_packs(),
                    handle_is_skipped
                );
                assert!(!handle_is_skipped);
                for (i, col) in self.columns_readers.iter_mut().enumerate() {
                    let (col_read_row, col_is_skipped, _) =
                        col.read(&mut block.columns[i], read_row).await?;
                    assert_eq!(
                        read_row,
                        col_read_row,
                        "read_row: {}, col_read_row: {}, col_pack_idx: {}, pack_idx: {}, col_num_packs: {}, num_packs: {}, col_is_skipped: {}",
                        read_row,
                        col_read_row,
                        col.pack_idx(),
                        self.version_reader.pack_idx(),
                        col.num_packs(),
                        self.version_reader.num_packs(),
                        col_is_skipped
                    );
                    assert!(!col_is_skipped);
                }
                total_read_row += read_row;
                return Ok(total_read_row);
            }
        }
        Ok(total_read_row)
    }

    fn get_remote_segments(&self) -> crate::table::Result<(Vec<FileSegmentIdent>, usize)> {
        let file = self.get_file();
        let mut ranges = vec![];
        // handle segment
        let (start_off, _) = self.handle_reader.col_meta.pack_offsets.get(0);
        ranges.push((start_off as u64, start_off as u64 + 1));
        // version segment
        let (start_off, _) = self.version_reader.col_meta.pack_offsets.get(0);
        ranges.push((start_off as u64, start_off as u64 + 1));
        // column segments
        for col in &self.columns_readers {
            let (start_off, _) = col.col_meta.pack_offsets.get(0);
            ranges.push((start_off as u64, start_off as u64 + 1));
        }
        file.get_remote_segments(&ranges)
    }
}

pub(crate) struct ColumnarColumnReader {
    pack_loader: PackLoader,
    col_meta: Arc<ColumnMeta>,
    pack_buffer: ColumnBuffer,
    pack_idx: usize,
    pack_row_start: usize,
    pack_row_end: usize,
    row_idx_in_pack: usize,
    is_default_val: bool,
    default_val: Option<Vec<u8>>,
    decryption_buf: Vec<u8>,
    packs_filter: Option<Arc<FilterOpResults>>,
    tag: String,
}

impl ColumnarColumnReader {
    pub(crate) fn new(
        file: Arc<dyn File>,
        col_meta: Arc<ColumnMeta>,
        is_default_val: bool,
        encryption_key: Option<EncryptionKey>,
        encryption_ver: u32,
        packs_filter: Option<Arc<FilterOpResults>>,
    ) -> ColumnarColumnReader {
        let pack_loader = PackLoader::new(file, col_meta.clone(), encryption_key, encryption_ver);
        let pack_buffer = ColumnBuffer::new_from_col_info(&col_meta.col_info);
        let default_val = if is_default_val {
            parse_default_val(&col_meta.col_info)
        } else {
            None
        };
        let tag = format!(
            "{}_{}",
            pack_loader.file.id(),
            col_meta.col_info.get_column_id(),
        );
        ColumnarColumnReader {
            pack_loader,
            col_meta,
            pack_buffer,
            pack_idx: 0,
            pack_row_start: 0,
            pack_row_end: 0,
            row_idx_in_pack: 0,
            is_default_val,
            default_val,
            decryption_buf: vec![],
            packs_filter,
            tag,
        }
    }

    pub(crate) async fn set_row_idx(&mut self, row_idx: usize) -> crate::table::Result<()> {
        if self.is_default_val {
            self.row_idx_in_pack = row_idx;
            return Ok(());
        }
        if self.pack_row_start <= row_idx && row_idx < self.pack_row_end {
            self.row_idx_in_pack = row_idx - self.pack_row_start;
            return Ok(());
        }
        let from_pack_idx = if self.pack_row_start < row_idx {
            self.pack_idx + 1
        } else {
            1
        };
        let pack_idx = self
            .col_meta
            .pack_offsets
            .search_pack_idx(from_pack_idx, row_idx as u32);
        self.load_pack(pack_idx).await?;
        if row_idx >= self.pack_row_start {
            self.row_idx_in_pack = row_idx - self.pack_row_start;
        }
        Ok(())
    }

    pub(crate) fn valid(&self) -> bool {
        self.pack_idx < self.col_meta.pack_offsets.num_packs()
    }

    pub(crate) fn pack_idx(&self) -> usize {
        self.pack_idx
    }

    pub(crate) fn num_packs(&self) -> usize {
        self.col_meta.pack_offsets.num_packs()
    }

    pub(crate) fn tag(&self) -> &str {
        &self.tag
    }

    pub(crate) async fn read(
        &mut self,
        to: &mut ColumnBuffer,
        limit: usize,
    ) -> crate::table::Result<(
        usize, // read_row
        bool,  // is_skipped
        usize, // row_idx
    )> {
        if self.is_default_val {
            let read_row = self.read_default(to, limit)?;
            return Ok((read_row, false, 0));
        }
        let mut read_row = 0;
        let num_packs = self.col_meta.pack_offsets.num_packs();
        while read_row < limit {
            let remain = self.pack_buffer.length() - self.row_idx_in_pack;
            if remain == 0 {
                let next_pack_idx = self.pack_idx + 1;
                if next_pack_idx >= num_packs {
                    break;
                }
                let is_skipped = self.load_pack(next_pack_idx).await?;
                self.row_idx_in_pack = 0;
                if is_skipped {
                    return Ok((read_row, true, self.pack_row_end));
                }
                continue;
            }
            let batch_size = min(remain, limit - read_row);
            to.append(
                &self.pack_buffer,
                self.row_idx_in_pack,
                self.row_idx_in_pack + batch_size,
            );
            self.row_idx_in_pack += batch_size;
            read_row += batch_size;
        }
        Ok((read_row, false, 0))
    }

    fn read_default(&mut self, to: &mut ColumnBuffer, limit: usize) -> crate::table::Result<usize> {
        if let Some(val) = &self.default_val {
            for _ in 0..limit {
                to.push_value(val);
            }
        } else {
            for _ in 0..limit {
                to.push_null();
            }
        }
        Ok(limit)
    }

    async fn load_pack(&mut self, pack_idx: usize) -> crate::table::Result<bool /* is_skipped */> {
        if pack_idx == self.pack_idx && self.pack_buffer.length() > 0 {
            return Ok(false);
        }
        let num_packs = self.col_meta.pack_offsets.num_packs();
        if pack_idx >= num_packs {
            self.pack_idx = num_packs;
            self.pack_buffer.reset();
            let (_, row_end_off) = self.col_meta.pack_offsets.end_offset();
            self.pack_row_start = row_end_off as usize;
            self.pack_row_end = row_end_off as usize;
            self.row_idx_in_pack = 0;
            return Ok(false);
        }
        let ((pack_start, pack_row_start), (pack_end, pack_row_end)) =
            self.col_meta.get_pack_offset(pack_idx);
        if let Some(packs_filter) = &self.packs_filter {
            // If the pack is not support filter, result is Unknown, treat it as Some.
            if packs_filter[pack_idx] == FilterOpResult::None {
                debug!("{}: skip load_pack: {}", self.tag(), pack_idx);
                self.pack_idx = pack_idx;
                self.pack_buffer.reset();
                self.pack_row_start = pack_row_end as usize;
                self.pack_row_end = pack_row_end as usize;
                self.row_idx_in_pack = 0;
                return Ok(true);
            }
        }
        self.pack_loader
            .load_pack(
                &mut self.pack_buffer,
                pack_start,
                pack_end,
                &mut self.decryption_buf,
            )
            .await?;
        debug!("{}: load_pack: {}", self.tag(), pack_idx);
        self.pack_idx = pack_idx;
        self.pack_row_start = pack_row_start as usize;
        self.pack_row_end = pack_row_end as usize;
        Ok(false)
    }
}

struct PackLoader {
    file: Arc<dyn File>,
    col_meta: Arc<ColumnMeta>,
    segment_ident: FileSegmentIdent,
    segment_content: Vec<u8>,
    segment_content_offset: u32,
    compressed_buf: Vec<u8>,
    uncompressed_buf: AVec<u8>,
    encryption_key: Option<EncryptionKey>,
    encryption_ver: u32,
}

impl PackLoader {
    pub fn new(
        file: Arc<dyn File>,
        col_meta: Arc<ColumnMeta>,
        encryption_key: Option<EncryptionKey>,
        encryption_ver: u32,
    ) -> PackLoader {
        PackLoader {
            file,
            col_meta,
            segment_ident: FileSegmentIdent::new(0, 0, 0),
            segment_content: vec![],
            segment_content_offset: 0,
            encryption_key,
            encryption_ver,
            compressed_buf: vec![],
            uncompressed_buf: avec![],
        }
    }

    async fn read_from_segment_cache(
        &mut self,
        buf: Option<&mut [u8]>, // None means read to compressed_buf
        pack_offset: u32,
    ) -> crate::table::Result<()> {
        let segment_ident = self.file.get_segment_ident(pack_offset as u64)?;
        let data_len = if let Some(buf) = &buf {
            buf.len()
        } else {
            self.compressed_buf.len()
        };

        // For large packs (>256KB), don't use cache, read directly
        if data_len > PACK_CACHE_THRESHOLD {
            // Read directly from file for large packs
            if let Some(buf) = buf {
                self.file.read_at_async(buf, pack_offset as u64).await?;
            } else {
                self.compressed_buf.resize(data_len, 0);
                self.file
                    .read_at_async(&mut self.compressed_buf, pack_offset as u64)
                    .await?;
            }
            return Ok(());
        }

        // For smaller packs, check if it's in our current cache
        let is_in_current_segment = segment_ident == self.segment_ident
            && pack_offset >= self.segment_content_offset
            && (pack_offset as usize + data_len)
                <= (self.segment_content_offset as usize + self.segment_content.len());

        if !is_in_current_segment {
            let (pack_end_offset, _) = self.col_meta.pack_offsets.end_offset();
            let read_size = std::cmp::min(
                PACK_CACHE_THRESHOLD,
                (pack_end_offset - pack_offset) as usize,
            );
            self.segment_content.resize(read_size, 0);
            self.file
                .read_at_async(&mut self.segment_content, pack_offset as u64)
                .await?;
            self.segment_content_offset = pack_offset;
            self.segment_ident = segment_ident;
        }

        let start_off = pack_offset - self.segment_content_offset;
        // Copy content from segment_content to buf
        if let Some(buf) = buf {
            buf.copy_from_slice(
                &self.segment_content[start_off as usize..start_off as usize + data_len],
            );
        } else {
            self.compressed_buf.copy_from_slice(
                &self.segment_content[start_off as usize..start_off as usize + data_len],
            );
        }

        Ok(())
    }

    pub async fn load_pack(
        &mut self,
        col_buf: &mut ColumnBuffer,
        pack_offset: u32,
        pack_end_offset: u32,
        decryption_buf: &mut Vec<u8>,
    ) -> crate::table::Result<()> {
        let length = (pack_end_offset - pack_offset) as usize;
        if let Some(encryption_key) = self.encryption_key.clone() {
            decryption_buf.resize(length, 0);
            if self.file.is_sync() {
                // InMemFile should always be sync.
                self.file.read_at(decryption_buf, pack_offset as u64)?;
            } else {
                // Try to read from segment cache, if not found, load it.
                self.read_from_segment_cache(Some(decryption_buf), pack_offset)
                    .await?;
            }
            self.compressed_buf.clear();
            encryption_key.decrypt(
                decryption_buf,
                self.file.id(),
                pack_offset,
                self.encryption_ver,
                &mut self.compressed_buf,
            );
        } else {
            self.compressed_buf.resize(length, 0);
            if self.file.is_sync() {
                // InMemFile should always be sync.
                self.file
                    .read_at(&mut self.compressed_buf, pack_offset as u64)?;
            } else {
                // Try to read from segment cache, if not found, load it.
                self.read_from_segment_cache(None, pack_offset).await?;
            }
        }
        decompress_pack(&self.compressed_buf, &mut self.uncompressed_buf);
        col_buf.parse(&self.uncompressed_buf);
        Ok(())
    }
}

#[async_trait]
pub trait ColumnarFilterReader: Send {
    async fn set_handle_range(
        &mut self,
        start_handle: &[u8],
        end_handle: &[u8],
    ) -> crate::table::Result<()>;
    async fn set_int_handle_range(
        &mut self,
        start_handle: i64,
        end_handle: Option<i64>,
    ) -> crate::table::Result<()>;
    fn get_schema(&self) -> &Schema;

    fn reset(&mut self) {}

    async fn prefetch_ia_remote_segments(
        &mut self,
        _tag: &str,
        _ia_mgr: &IaManager,
        _keyspace_id: u32,
        _timeout: Duration,
    ) -> crate::table::Result<Option<f64>> {
        Ok(None)
    }

    // Try read block, return the number of rows read and whether the reader is
    // drained.
    async fn try_read_block(
        &mut self,
        block: &mut Block,
        limit: usize,
    ) -> crate::table::Result<(usize /* read_row */, bool /* drained */)>;

    // Return size may be less than limit, and it doesn't mean the reader is drained
    // if the return size above 0.
    async fn read_block(&mut self, block: &mut Block, limit: usize) -> crate::table::Result<usize> {
        loop {
            let (read_row, drained) = self.try_read_block(block, limit).await?;
            if read_row > 0 {
                return Ok(read_row);
            }
            if drained {
                return Ok(0);
            }
            // If all rows are filtered, we should try to read next block.
            continue;
        }
    }

    #[cfg(any(test, feature = "testexport"))]
    async fn read_all(&mut self) -> Block {
        let schema = self.get_schema().clone();
        let mut out = Block::new(&schema);
        let mut buf = Block::new(&schema);
        loop {
            buf.reset();
            let read = self.read_block(&mut buf, 1024).await.unwrap();
            if read == 0 {
                break;
            }
            out.append(&buf, 0, read);
        }
        out
    }

    async fn set_unbounded_handle_range(&mut self) -> crate::table::Result<()> {
        if self.get_schema().is_common_handle() {
            self.set_handle_range(&[], GLOBAL_COMMON_HANDLE_END).await?;
        } else {
            self.set_int_handle_range(i64::MIN, None).await?;
        }
        Ok(())
    }
}

struct ColumnarFilter {
    ranges: Vec<(usize, usize)>,
    in_range: bool,
    range_start: usize,
    filter_block: Block,
}

impl ColumnarFilter {
    fn new(schema: &Schema) -> Self {
        Self {
            ranges: vec![],
            in_range: false,
            range_start: 0,
            filter_block: Block::new(schema),
        }
    }

    fn finish_range(&mut self, i: usize) {
        if self.in_range {
            self.ranges.push((self.range_start, i));
            self.in_range = false;
        }
    }

    fn start_range(&mut self, i: usize) {
        if !self.in_range {
            self.range_start = i;
            self.in_range = true;
        }
    }

    fn clear(&mut self) {
        self.ranges.clear();
        self.in_range = false;
    }

    fn do_filter(&mut self, read_row: usize, block: &mut Block) -> usize {
        if self.ranges.len() == 1 {
            let (start, end) = self.ranges[0];
            if start == 0 {
                return if end == read_row {
                    // All rows are valid, no need to filter.
                    read_row
                } else {
                    block.truncate(end);
                    end
                };
            }
        }
        self.filter_block.reset();
        let mut filtered_rows = 0;
        for &(start, end) in &self.ranges {
            self.filter_block.handles.append(&block.handles, start, end);
            self.filter_block
                .versions
                .append(&block.versions, start, end);
            for (i, col) in self.filter_block.columns.iter_mut().enumerate() {
                col.append(&block.columns[i], start, end);
            }
            filtered_rows += end - start;
        }
        mem::swap(block, &mut self.filter_block);
        filtered_rows
    }
}

pub struct ColumnarMvccReader {
    src: Box<dyn ColumnarReader>,
    read_ts: u64,
    filter: ColumnarFilter,
    end_handle: Vec<u8>,
    end_int_handle: Option<i64>,
    prev_int_handle: Option<i64>,
    prev_common_handle: Vec<u8>,
}

impl ColumnarMvccReader {
    pub fn new(src: Box<dyn ColumnarReader>, schema: &Schema, read_ts: u64) -> ColumnarMvccReader {
        ColumnarMvccReader {
            src,
            filter: ColumnarFilter::new(schema),
            read_ts,
            end_handle: vec![],
            end_int_handle: None,
            prev_int_handle: None,
            prev_common_handle: vec![],
        }
    }
}

#[async_trait]
impl ColumnarFilterReader for ColumnarMvccReader {
    async fn set_handle_range(
        &mut self,
        start_handle: &[u8],
        end_handle: &[u8],
    ) -> crate::table::Result<()> {
        self.end_handle = end_handle.to_vec();
        self.src.seek(start_handle).await?;
        self.filter.clear();
        Ok(())
    }

    async fn set_int_handle_range(
        &mut self,
        start_handle: i64,
        end_handle: Option<i64>,
    ) -> crate::table::Result<()> {
        self.end_int_handle = end_handle;
        self.src.seek(&start_handle.to_le_bytes()).await?;
        self.filter.clear();
        Ok(())
    }

    fn get_schema(&self) -> &Schema {
        self.src.schema()
    }

    fn reset(&mut self) {
        self.src.reset().unwrap();
    }

    async fn prefetch_ia_remote_segments(
        &mut self,
        tag: &str,
        ia_mgr: &IaManager,
        keyspace_id: u32,
        timeout: Duration,
    ) -> crate::table::Result<Option<f64>> {
        let (idents, total_segments) = self.src.get_remote_segments()?;
        if idents.is_empty() {
            // No need to prefetch, return 1.0 cache hit.
            return Ok(Some(1.0));
        }

        let _enter = ia_mgr.enter_runtime();
        let mut tasks = vec![];
        let deadline = Deadline::from_now(timeout);
        let prefetch_segments = idents.len();
        for ident in idents {
            let mgr = ia_mgr.clone();
            let task = async move {
                mgr.prefetch_segment(ident, FileType::Columnar, keyspace_id, deadline, None)
                    .await
            };
            tasks.push(tokio::spawn(task));
        }

        match tokio::time::timeout_at(deadline.to_tokio_instant(), try_join_all(tasks)).await {
            Ok(res) => {
                let res = res.map_err(|err| -> crate::table::Error {
                    crate::table::Error::Other(format!(
                        "{} prefetch segment failed: {:?}",
                        tag, err
                    ))
                })?;
                res.into_iter().collect::<crate::table::Result<()>>()?;
                let cache_hit = (total_segments - prefetch_segments) as f64 / total_segments as f64;
                info!("{} prefetch ia remote segments", tag;
                    "prefetch_segments" => prefetch_segments, "cache_hit" => cache_hit);
                Ok(Some(cache_hit))
            }
            Err(_) => Err(crate::table::Error::DeadlineExceeded(format!(
                "{} prefetch ia remote segments timeout",
                tag
            ))),
        }
    }

    async fn try_read_block(
        &mut self,
        block: &mut Block,
        limit: usize,
    ) -> crate::table::Result<(usize /* read_row */, bool /* drained */)> {
        self.filter.clear();
        block.reset();
        let read_row = self.src.read(block, limit).await?;
        if read_row == 0 {
            // Return 0 rows means the reader is drained.
            return Ok((0, true));
        }
        if block.handles.fixed_size > 0 {
            for i in 0..block.handles.length() {
                let handle = block.handles.get_int_handle_value(i);
                let version = block.versions.get_version(i);
                if version > self.read_ts
                    || (self.prev_int_handle.is_some() && handle == self.prev_int_handle.unwrap())
                {
                    self.filter.finish_range(i);
                    continue;
                }
                if block.versions.is_null(i) {
                    self.prev_int_handle = Some(handle);
                    self.filter.finish_range(i);
                    continue;
                }
                if self.end_int_handle.is_some() && handle >= self.end_int_handle.unwrap() {
                    self.filter.finish_range(i);
                    break;
                }
                self.filter.start_range(i);
                self.prev_int_handle = Some(handle);
            }
        } else {
            let length = block.handles.length();
            let last_handle = block.handles.get_not_null_value(length - 1);
            let check_handle = last_handle >= self.end_handle.as_slice();
            for i in 0..block.handles.length() {
                let handle = block.handles.get_not_null_value(i);
                let version = block.versions.get_version(i);
                if version > self.read_ts || handle == self.prev_common_handle {
                    self.filter.finish_range(i);
                    continue;
                }
                if block.versions.is_null(i) {
                    self.prev_common_handle = handle.to_vec();
                    self.filter.finish_range(i);
                    continue;
                }
                if check_handle && handle >= self.end_handle.as_slice() {
                    self.filter.finish_range(i);
                    break;
                }
                self.filter.start_range(i);
                self.prev_common_handle = handle.to_vec();
            }
        }
        self.filter.finish_range(read_row);
        let filtered_rows = self.filter.do_filter(read_row, block);
        Ok((filtered_rows, false))
    }
}

pub struct ColumnarCompactReader {
    src: Box<dyn ColumnarReader>,
    level: u32,
    safe_ts: u64,
    filter: ColumnarFilter,
    end_handle: Vec<u8>,
    end_int_handle: Option<i64>,
    prev_int_handle: Option<i64>,
    prev_common_handle: Vec<u8>,
}

impl ColumnarCompactReader {
    pub fn new(
        src: Box<dyn ColumnarReader>,
        level: u32,
        schema: &Schema,
        safe_ts: u64,
    ) -> ColumnarCompactReader {
        ColumnarCompactReader {
            src,
            level,
            filter: ColumnarFilter::new(schema),
            safe_ts,
            end_handle: vec![],
            end_int_handle: None,
            prev_int_handle: None,
            prev_common_handle: vec![],
        }
    }
}

#[async_trait]
impl ColumnarFilterReader for ColumnarCompactReader {
    async fn set_handle_range(
        &mut self,
        start_handle: &[u8],
        end_handle: &[u8],
    ) -> crate::table::Result<()> {
        self.end_handle = end_handle.to_vec();
        self.src.seek(start_handle).await?;
        self.filter.clear();
        Ok(())
    }

    async fn set_int_handle_range(
        &mut self,
        start_handle: i64,
        end_handle: Option<i64>,
    ) -> crate::table::Result<()> {
        self.end_int_handle = end_handle;
        self.src.seek(&start_handle.to_le_bytes()).await?;
        self.filter.clear();
        Ok(())
    }

    fn get_schema(&self) -> &Schema {
        self.src.schema()
    }

    fn reset(&mut self) {
        self.end_handle.clear();
        self.end_int_handle = None;
        self.prev_int_handle = None;
        self.prev_common_handle.clear();
        self.filter.clear();
        self.src.reset().unwrap();
    }

    async fn try_read_block(
        &mut self,
        block: &mut Block,
        limit: usize,
    ) -> crate::table::Result<(usize /* read_row */, bool /* drained */)> {
        self.filter.clear();
        block.reset();
        let read_row = self.src.read(block, limit).await?;
        if read_row == 0 {
            return Ok((0, true));
        }
        if block.handles.fixed_size > 0 {
            for i in 0..block.handles.length() {
                let handle = block.handles.get_int_handle_value(i);
                let version = block.versions.get_version(i);
                if self.prev_int_handle.is_some()
                    && handle == self.prev_int_handle.unwrap()
                    && version < self.safe_ts
                {
                    self.filter.finish_range(i);
                    continue;
                }
                if self.level == 2 && version < self.safe_ts && block.versions.is_null(i) {
                    self.prev_int_handle = Some(handle);
                    self.filter.finish_range(i);
                    continue;
                }
                if self.end_int_handle.is_some() && handle >= self.end_int_handle.unwrap() {
                    self.filter.finish_range(i);
                    break;
                }
                self.filter.start_range(i);
                self.prev_int_handle = Some(handle);
            }
        } else {
            let length = block.handles.length();
            let last_handle = block.handles.get_not_null_value(length - 1);
            let check_handle = last_handle >= self.end_handle.as_slice();
            for i in 0..block.handles.length() {
                let handle = block.handles.get_not_null_value(i);
                let version = block.versions.get_version(i);
                if handle == self.prev_common_handle && version < self.safe_ts {
                    self.filter.finish_range(i);
                    continue;
                }
                if self.level == 2 && version < self.safe_ts && block.versions.is_null(i) {
                    self.prev_common_handle = handle.to_vec();
                    self.filter.finish_range(i);
                    continue;
                }
                if check_handle && handle >= self.end_handle.as_slice() {
                    self.filter.finish_range(i);
                    break;
                }
                self.filter.start_range(i);
                self.prev_common_handle = handle.to_vec();
            }
        }
        self.filter.finish_range(read_row);
        Ok((self.filter.do_filter(read_row, block), false))
    }
}

pub struct ColumnarTruncateTsReader {
    src: Box<dyn ColumnarReader>,
    truncate_ts: u64,
    filter: ColumnarFilter,
    end_handle: Vec<u8>,
    end_int_handle: Option<i64>,
}

impl ColumnarTruncateTsReader {
    pub fn new(
        src: Box<dyn ColumnarReader>,
        schema: &Schema,
        truncate_ts: u64,
    ) -> ColumnarTruncateTsReader {
        ColumnarTruncateTsReader {
            src,
            filter: ColumnarFilter::new(schema),
            truncate_ts,
            end_handle: vec![],
            end_int_handle: None,
        }
    }
}

#[async_trait]
impl ColumnarFilterReader for ColumnarTruncateTsReader {
    async fn set_handle_range(
        &mut self,
        start_handle: &[u8],
        end_handle: &[u8],
    ) -> crate::table::Result<()> {
        self.end_handle = end_handle.to_vec();
        self.src.seek(start_handle).await?;
        self.filter.clear();
        Ok(())
    }

    async fn set_int_handle_range(
        &mut self,
        start_handle: i64,
        end_handle: Option<i64>,
    ) -> crate::table::Result<()> {
        self.end_int_handle = end_handle;
        self.src.seek(&start_handle.to_le_bytes()).await?;
        self.filter.clear();
        Ok(())
    }

    fn get_schema(&self) -> &Schema {
        self.src.schema()
    }

    fn reset(&mut self) {
        self.end_handle.clear();
        self.end_int_handle = None;
        self.filter.clear();
        self.src.reset().unwrap();
    }

    async fn try_read_block(
        &mut self,
        block: &mut Block,
        limit: usize,
    ) -> crate::table::Result<(usize /* read_row */, bool /* drained */)> {
        self.filter.clear();
        block.reset();
        let read_row = self.src.read(block, limit).await?;
        if read_row == 0 {
            return Ok((0, true));
        }
        if block.handles.fixed_size > 0 {
            for i in 0..block.handles.length() {
                let handle = block.handles.get_int_handle_value(i);
                let version = block.versions.get_version(i);
                if version > self.truncate_ts {
                    self.filter.finish_range(i);
                    continue;
                }
                if self.end_int_handle.is_some() && handle >= self.end_int_handle.unwrap() {
                    self.filter.finish_range(i);
                    break;
                }
                self.filter.start_range(i);
            }
        } else {
            let length = block.handles.length();
            let last_handle = block.handles.get_not_null_value(length - 1);
            let check_handle = last_handle >= self.end_handle.as_slice();
            for i in 0..block.handles.length() {
                let handle = block.handles.get_not_null_value(i);
                let version = block.versions.get_version(i);
                if version > self.truncate_ts {
                    self.filter.finish_range(i);
                    continue;
                }
                if check_handle && handle >= self.end_handle.as_slice() {
                    self.filter.finish_range(i);
                    break;
                }
                self.filter.start_range(i);
            }
        }
        self.filter.finish_range(read_row);
        Ok((self.filter.do_filter(read_row, block), false))
    }
}

pub(crate) struct ColumnarMergeReader {
    schema: Schema,
    #[allow(clippy::vec_box)]
    heap: Vec<Box<ColumnarReaderBuffer>>,
    // Save the invalid reader buffer to recover them when reset.
    #[allow(clippy::vec_box)]
    origin_heap: Vec<Box<ColumnarReaderBuffer>>,
    is_int_handle: bool,
    first_batch_end_row_idx: usize,
}

pub(crate) struct ColumnarReaderBuffer {
    reader: Box<dyn ColumnarReader>,
    block: Block,
    row_idx: usize,
}

impl ColumnarReaderBuffer {
    pub async fn seek(&mut self, handle: &[u8]) -> crate::table::Result<()> {
        self.reader.seek(handle).await?;
        self.block.reset();
        self.reader.read(&mut self.block, 1).await?;
        self.row_idx = 0;
        Ok(())
    }

    pub fn handle(&self) -> &[u8] {
        self.block.handles.get_not_null_value(self.row_idx)
    }

    pub fn int_handle(&self) -> i64 {
        self.block.handles.get_int_handle_value(self.row_idx)
    }

    pub fn inner_reader(&self) -> &dyn ColumnarReader {
        self.reader.as_ref()
    }

    pub fn reset(&mut self) {
        self.row_idx = 0;
        self.block.reset();
        self.reader.reset().unwrap();
    }

    pub fn version(&self) -> u64 {
        self.block.versions.get_version(self.row_idx)
    }

    pub fn valid(&self) -> bool {
        self.row_idx < self.block.handles.length()
    }

    pub async fn read_block(&mut self) -> crate::table::Result<()> {
        self.block.reset();
        self.reader.read(&mut self.block, 1024).await?;
        self.row_idx = 0;
        Ok(())
    }
}

#[allow(dead_code)]
impl ColumnarMergeReader {
    pub fn new(schema: Schema, readers: Vec<Box<dyn ColumnarReader>>) -> ColumnarMergeReader {
        let heap: Vec<Box<ColumnarReaderBuffer>> = readers
            .into_iter()
            .map(|reader| {
                Box::new(ColumnarReaderBuffer {
                    reader,
                    block: Block::new(&schema),
                    row_idx: 0,
                })
            })
            .collect();
        let is_int_handle = !schema.is_common_handle();
        ColumnarMergeReader {
            schema,
            heap,
            origin_heap: vec![],
            is_int_handle,
            first_batch_end_row_idx: 0,
        }
    }

    fn init_heap(&mut self) {
        for i in (0..self.heap.len() / 2).rev() {
            self.down(i);
        }
        if !self.heap.is_empty() {
            self.update_first_block_end_row_idx();
        }
    }

    fn down(&mut self, i0: usize) -> bool {
        let n = self.heap.len();
        let mut i = i0;
        loop {
            let left = 2 * i + 1;
            if left >= n {
                break;
            }
            let right = left + 1;
            let j = if right < n && self.less(right, left) {
                right
            } else {
                left
            };
            if !self.less(j, i) {
                break;
            }
            self.heap.swap(i, j);
            i = j;
        }
        i > i0
    }

    fn less(&mut self, a: usize, b: usize) -> bool {
        if self.is_int_handle {
            let a_handle = self.heap[a].int_handle();
            let b_handle = self.heap[b].int_handle();
            if a_handle != b_handle {
                a_handle < b_handle
            } else {
                self.heap[a].version() > self.heap[b].version()
            }
        } else {
            match self.heap[a].handle().cmp(self.heap[b].handle()) {
                Ordering::Less => true,
                Ordering::Equal => self.heap[a].version() > self.heap[b].version(),
                Ordering::Greater => false,
            }
        }
    }

    fn update_first_block_end_row_idx(&mut self) {
        let first = &self.heap[0];
        if self.heap.len() == 1 {
            self.first_batch_end_row_idx = first.block.handles.length();
            return;
        }
        let mut second_handle = if self.heap.len() >= 3 {
            if first.block.handles.fixed_size > 0 {
                if self.heap[1].int_handle() < self.heap[2].int_handle() {
                    self.heap[1].handle()
                } else {
                    self.heap[2].handle()
                }
            } else if self.heap[1].handle() < self.heap[2].handle() {
                self.heap[1].handle()
            } else {
                self.heap[2].handle()
            }
        } else {
            self.heap[1].handle()
        };
        if first.block.handles.fixed_size > 0 {
            let int_second_handle = second_handle.get_i64_le();
            for i in first.row_idx + 1..first.block.handles.length() {
                let handle = first.block.handles.get_int_handle_value(i);
                if handle >= int_second_handle {
                    self.first_batch_end_row_idx = i;
                    return;
                }
            }
        } else {
            for i in first.row_idx + 1..first.block.handles.length() {
                if first.block.handles.get_not_null_value(i) >= second_handle {
                    self.first_batch_end_row_idx = i;
                    return;
                }
            }
        }
        self.first_batch_end_row_idx = first.block.handles.length();
    }
}

#[async_trait]
impl ColumnarReader for ColumnarMergeReader {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn reset(&mut self) -> crate::table::Result<()> {
        // Recover all readers from origin_heap and push them into heap.
        while let Some(reader) = self.origin_heap.pop() {
            self.heap.push(reader);
        }
        self.heap.iter_mut().for_each(|r| r.reset());
        Ok(())
    }

    async fn seek(&mut self, handle: &[u8]) -> crate::table::Result<()> {
        for reader in &mut self.heap {
            reader.seek(handle).await?;
        }
        // Move invalid readers to origin_heap, keep valid readers in heap.
        let (invalid, valid) = self.heap.drain(..).partition(|r| !r.valid());
        self.origin_heap = invalid;
        self.heap = valid;
        if !self.heap.is_empty() {
            self.init_heap();
        }
        Ok(())
    }

    async fn read(&mut self, block: &mut Block, limit: usize) -> crate::table::Result<usize> {
        let mut read_row = 0;
        while read_row < limit {
            if self.heap.is_empty() {
                return Ok(read_row);
            }
            let first = &mut self.heap[0];
            let remain = min(
                limit - read_row,
                self.first_batch_end_row_idx - first.row_idx,
            );

            block.append(&first.block, first.row_idx, first.row_idx + remain);
            first.row_idx += remain;
            read_row += remain;
            if first.row_idx == first.block.handles.length() {
                first.read_block().await?;
                if !first.valid() {
                    let removed = self.heap.swap_remove(0);
                    self.origin_heap.push(removed);
                    if self.heap.is_empty() {
                        return Ok(read_row);
                    }
                }
                self.down(0);
                self.update_first_block_end_row_idx();
            } else if first.row_idx == self.first_batch_end_row_idx {
                self.down(0);
                self.update_first_block_end_row_idx();
            }
        }
        Ok(read_row)
    }

    fn get_remote_segments(&self) -> crate::table::Result<(Vec<FileSegmentIdent>, usize)> {
        let mut col_idents = vec![];
        let mut seg_count = 0;
        for reader in &self.heap {
            let (idents, count) = reader.inner_reader().get_remote_segments()?;
            col_idents.extend(idents);
            seg_count += count;
        }
        Ok((col_idents, seg_count))
    }
}

pub(crate) fn parse_default_val(col_info: &ColumnInfo) -> Option<Vec<u8>> {
    let mut default_val = col_info.get_default_val();
    if default_val.is_empty() {
        return None;
    }
    let flag = default_val.get_u8();
    let val = match flag {
        INT_FLAG | DURATION_FLAG => decode_i64(&mut default_val).unwrap().to_le_bytes().to_vec(),
        UINT_FLAG => decode_u64(&mut default_val).unwrap().to_le_bytes().to_vec(),
        BYTES_FLAG => decode_bytes(&mut default_val, false).unwrap(),
        COMPACT_BYTES_FLAG => decode_compact_bytes(&mut default_val).unwrap(),
        NIL_FLAG => {
            return None;
        }
        FLOAT_FLAG => decode_f64(&mut default_val).unwrap().to_le_bytes().to_vec(),
        JSON_FLAG => default_val.to_vec(),
        VAR_INT_FLAG => decode_var_i64(&mut default_val)
            .unwrap()
            .to_le_bytes()
            .to_vec(),
        VAR_UINT_FLAG => decode_var_u64(&mut default_val)
            .unwrap()
            .to_le_bytes()
            .to_vec(),
        DECIMAL_FLAG => {
            warn!("unimplemented decimal default");
            return None;
        }
        VECTOR_FLOAT32_FLAG => {
            warn!("unimplemented vector default");
            return None;
        }
        f => {
            warn!("parse default val get unknown flag {}", f);
            return None;
        }
    };
    Some(val)
}

pub struct ColumnarRowTableReader {
    schema: Schema,
    iter: Box<dyn table::Iterator>,
    blob_tbls: Option<Arc<HashMap<u64, BlobTable>>>,
    prefix: Vec<u8>,
    default_vals: Vec<Option<Vec<u8>>>,
    is_int_handle: bool,
    check_schema: bool,
    max_col_id: i32,
    encryption_key: Option<EncryptionKey>,
    decryption_buf: Vec<u8>,
}

impl ColumnarRowTableReader {
    pub fn new(
        schema: Schema,
        iter: Box<dyn table::Iterator>,
        blob_tbls: Option<Arc<HashMap<u64, BlobTable>>>,
        check_schema: bool,
        encryption_key: Option<EncryptionKey>,
    ) -> ColumnarRowTableReader {
        let mut prefix = encode_row_key(schema.table_id, 0);
        prefix.truncate(PREFIX_LEN);
        let is_int_handle = get_fixed_size(&schema.handle_column) > 0;
        let default_vals = schema
            .columns
            .iter()
            .map(|col| parse_default_val(col))
            .collect();
        let max_col_id = schema.max_col_id as i32;

        ColumnarRowTableReader {
            schema,
            iter,
            blob_tbls,
            prefix,
            default_vals,
            is_int_handle,
            check_schema,
            max_col_id,
            encryption_key,
            decryption_buf: vec![],
        }
    }

    fn decode_row_columns(&self, block: &mut Block, row_value: &[u8]) -> table::Result<()> {
        if row_value[0] == CODEC_VERSION {
            self.decode_row_columns_v2(block, row_value)
        } else {
            self.decode_row_columns_v1(block, row_value)
        }
    }

    fn decode_row_columns_v1(&self, block: &mut Block, row_value: &[u8]) -> table::Result<()> {
        let mut data: BytesSlice<'_> = row_value;
        let datums = decode(&mut data).map_err(|e| table::Error::Other(e.to_string()))?;
        let mut datums_map = HashMap::with_capacity(datums.len() / 2);
        let mut datums_iter = datums.into_iter();
        while let (Some(col_id), Some(data)) = (datums_iter.next(), datums_iter.next()) {
            if let Ok(Some(col_id)) = col_id.as_int() {
                datums_map.insert(col_id, data);
            } else {
                return Err(table::Error::Other("invalid col id".to_string()));
            }
        }

        for (offset, col_info) in self.schema.columns.iter().enumerate() {
            let col_id = col_info.get_column_id();
            let col_buf = &mut block.columns[offset];

            if datums_map.contains_key(&col_id) {
                Self::push_col_buf_with_row_v1_datum(
                    col_buf,
                    col_info,
                    datums_map.get(&col_id).unwrap(),
                );
            } else if !self.is_int_handle && get_primary_key(col_info) {
                // get value from common handle
                let mut common_handle =
                    block.handles.get_not_null_value(block.handles.length() - 1);
                for &pk_col_id in &self.schema.pk_col_ids {
                    let (datum, remain) = datum::split_datum(common_handle, false).unwrap();
                    if pk_col_id == col_id {
                        if let Err(e) =
                            Self::push_col_buf_with_common_handle_datum(col_buf, col_info, datum)
                        {
                            error!(
                                "decode_row_columns_v1 pk: {:?}, col_info col_id: {}, col_buf col_id: {}, tp: {}, offset: {}, val_len: {}, schema: {:?}",
                                e,
                                col_id,
                                col_buf.col_id,
                                col_info.get_tp(),
                                offset,
                                datum.len(),
                                self.schema
                            );
                            return Err(table::Error::Other(e.to_string()));
                        }
                        break;
                    }
                    common_handle = remain;
                }
            } else if let Some(default_val) = &self.default_vals[offset] {
                col_buf.push_value(default_val);
            } else {
                col_buf.push_null();
            }
        }

        Ok(())
    }

    fn decode_row_columns_v2(&self, block: &mut Block, row_value: &[u8]) -> table::Result<()> {
        let row_slice = RowSlice::from_bytes(row_value).unwrap();
        if self.check_schema {
            let row_max_col_id = row_slice.max_col_id();
            if self.max_col_id < row_max_col_id {
                let err_info = format!("tbl:{} col:{}", self.schema.table_id, row_max_col_id);
                return Err(table::Error::SchemaOutOfDate(err_info));
            }
        }

        let values = row_slice.values();
        for (offset, col_info) in self.schema.columns.iter().enumerate() {
            let col_id = col_info.get_column_id();
            let col_buf = &mut block.columns[offset];
            assert_eq!(col_buf.col_id as i64, col_id);
            if row_slice.search_in_null_ids(col_id) {
                col_buf.push_null();
                continue;
            }
            if let Some((start, end)) = row_slice.search_in_non_null_ids(col_id).unwrap() {
                let col_val = &values[start..end];
                if let Err(e) = Self::push_col_buf_with_field_type(col_buf, col_info, col_val) {
                    error!(
                        "decode_row_columns_v2: {:?}, col_info col_id: {}, col_buf col_id: {}, tp: {}, offset: {}, val_len: {}, schema: {:?}",
                        e,
                        col_id,
                        col_buf.col_id,
                        col_info.get_tp(),
                        offset,
                        col_val.len(),
                        self.schema
                    );
                    return Err(table::Error::Other(e.to_string()));
                }
            } else if !self.is_int_handle && get_primary_key(col_info) {
                // get value from common handle
                let mut common_handle =
                    block.handles.get_not_null_value(block.handles.length() - 1);
                for &pk_col_id in &self.schema.pk_col_ids {
                    let (datum, remain) = datum::split_datum(common_handle, false).unwrap();
                    if pk_col_id == col_id {
                        if let Err(e) =
                            Self::push_col_buf_with_common_handle_datum(col_buf, col_info, datum)
                        {
                            error!(
                                "decode_row_columns_v2 pk: {:?}, col_info col_id: {}, col_buf col_id: {}, tp: {}, offset: {}, val_len: {}, schema: {:?}",
                                e,
                                col_id,
                                col_buf.col_id,
                                col_info.get_tp(),
                                offset,
                                datum.len(),
                                self.schema
                            );
                            return Err(table::Error::Other(e.to_string()));
                        }
                        break;
                    }
                    common_handle = remain;
                }
            } else if let Some(default_val) = &self.default_vals[offset] {
                col_buf.push_value(default_val);
            } else {
                col_buf.push_null();
            }
        }
        Ok(())
    }

    fn push_col_buf_with_field_type(
        col_buf: &mut ColumnBuffer,
        col_info: &ColumnInfo,
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
                if is_unsigned(col_info) {
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
                // Marshal decimal in TiFlash compatible format.
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
        col_info: &ColumnInfo,
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
        col_info: &ColumnInfo,
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
            NIL_FLAG => {
                col_buf.push_null();
            }
            FLOAT_FLAG => {
                let v = decode_f64(&mut datum)?;
                col_buf.push_value(&v.to_le_bytes());
            }
            DECIMAL_FLAG => {
                // let v = datum.read_decimal().unwrap();
                // let mut buf = vec![];
                // let prec = col_info.get_column_len() as u8;
                // let frac = col_info.get_decimal() as u8;
                // buf.write_decimal(&v, prec, frac).unwrap();
                // col_buf.push_value(&buf);
                //
                // Marshal decimal to TiFlash compatible format.
                let decimal = datum.read_decimal().unwrap();
                col_buf.push_value(&decode_decimal_as_int(col_info, &decimal));
            }
            JSON_FLAG | VECTOR_FLOAT32_FLAG | VAR_UINT_FLAG | VAR_INT_FLAG | COMPACT_BYTES_FLAG => {
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

pub fn is_unsigned(col_info: &ColumnInfo) -> bool {
    FieldTypeFlag::from_bits_truncate(col_info.get_flag() as u32).contains(FieldTypeFlag::UNSIGNED)
}

#[async_trait]
impl ColumnarReader for ColumnarRowTableReader {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    async fn seek(&mut self, handle: &[u8]) -> table::Result<()> {
        let row_key = if self.is_int_handle && !handle.is_empty() {
            encode_row_key(self.schema.table_id, (&handle[..]).get_i64_le())
        } else {
            encode_common_handle_row_key(self.schema.table_id, handle)
        };
        if self.iter.is_cf_sync(WRITE_CF) {
            self.iter.seek(InnerKey::from_inner_buf(&row_key));
        } else {
            self.iter
                .seek_async(InnerKey::from_inner_buf(&row_key))
                .await;
        }
        Ok(())
    }

    async fn read(&mut self, block: &mut Block, limit: usize) -> table::Result<usize> {
        let mut read_rows = 0;
        while self.iter.valid() && read_rows < limit {
            let key = self.iter.key();
            if !key.starts_with(&self.prefix) {
                break;
            }
            if self.is_int_handle {
                let int_handle = decode_int_handle(key.deref()).unwrap();
                block.handles.push_value(&int_handle.to_le_bytes());
            } else {
                let common_handle = decode_common_handle(key.deref()).unwrap();
                block.handles.push_value(common_handle);
            }
            let value = self.iter.value();
            let version = value.version;
            let mut handle_row_value = |reader: &mut Self, row_value: &[u8]| -> table::Result<()> {
                let is_deleted = row_value.is_empty();
                block.versions.push_version(version, is_deleted);
                if is_deleted {
                    for col in &mut block.columns {
                        if col.nullable {
                            col.push_null();
                        } else {
                            col.push_zero();
                        }
                    }
                } else {
                    reader.decode_row_columns(block, row_value)?;
                }
                Ok(())
            };
            if value.is_blob_ref() {
                let blob_ref = value.get_blob_ref();
                let blob_tbl = self.blob_tbls.as_ref().unwrap().get(&blob_ref.fid).unwrap();
                let row_value = blob_tbl
                    .get(
                        &blob_ref,
                        &mut self.decryption_buf,
                        self.encryption_key.clone(),
                    )
                    .unwrap();
                handle_row_value(self, &row_value)?
            } else {
                let row_value = value.get_value();
                handle_row_value(self, row_value)?
            };
            read_rows += 1;
            if self.iter.is_cf_sync(WRITE_CF) {
                self.iter.next_all_version();
            } else {
                self.iter.next_all_version_async().await;
            }
        }
        Ok(read_rows)
    }
}

pub struct ColumnarConcatReader {
    schema: Schema,
    filter_op: Option<FilterOperator>,
    files: Vec<ColumnarFile>,
    reader: Option<ColumnarTableReader>,
    idx: usize,
    encryption_key: Option<EncryptionKey>,
}

impl ColumnarConcatReader {
    pub fn new(
        files: &[ColumnarFile],
        schema: Schema,
        filter_op: Option<FilterOperator>,
        encryption_key: Option<EncryptionKey>,
    ) -> ColumnarConcatReader {
        let mut reader_files = vec![];
        for file in files {
            if file.has_table(schema.table_id) {
                reader_files.push(file.clone());
            }
        }
        ColumnarConcatReader {
            schema,
            filter_op,
            files: reader_files,
            reader: None,
            idx: 0,
            encryption_key,
        }
    }
}

#[async_trait]
impl ColumnarReader for ColumnarConcatReader {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    async fn seek(&mut self, handle: &[u8]) -> table::Result<()> {
        if self.files.is_empty() {
            return Ok(());
        }
        let mut row_key = if get_fixed_size(&self.schema.handle_column) > 0 {
            encode_row_key(self.schema.table_id, (&handle[..]).get_i64_le())
        } else {
            encode_common_handle_row_key(self.schema.table_id, handle)
        };
        if let Some(prefix) = ApiV2::get_keyspace_prefix(self.files[0].get_smallest().deref()) {
            let mut buf = Vec::with_capacity(prefix.len() + row_key.len());
            buf.extend_from_slice(prefix);
            buf.extend_from_slice(&row_key);
            row_key = buf;
        }
        for (i, file) in self.files.iter().enumerate() {
            if file.get_biggest().deref() < row_key.as_slice() {
                continue;
            }
            let mut reader = ColumnarTableReader::new(
                file,
                self.schema.clone(),
                self.filter_op.clone(),
                self.encryption_key.clone(),
            );
            reader.seek(handle).await?;
            self.reader = Some(reader);
            self.idx = i;
            return Ok(());
        }
        self.idx = self.files.len();
        Ok(())
    }

    async fn read(&mut self, block: &mut Block, limit: usize) -> table::Result<usize> {
        let mut read_rows = 0;
        while self.idx < self.files.len() && read_rows < limit {
            let reader = self.reader.as_mut().unwrap();
            let cnt = reader.read(block, limit - read_rows).await?;
            if cnt == 0 {
                self.idx += 1;
                if self.idx < self.files.len() {
                    let file = &self.files[self.idx];
                    let mut reader = ColumnarTableReader::new(
                        file,
                        self.schema.clone(),
                        self.filter_op.clone(),
                        self.encryption_key.clone(),
                    );
                    reader.seek(&[]).await?;
                    self.reader = Some(reader);
                }
                continue;
            }
            read_rows += cnt;
        }
        Ok(read_rows)
    }

    fn get_remote_segments(&self) -> crate::table::Result<(Vec<FileSegmentIdent>, usize)> {
        let mut seg_idents = vec![];
        let mut seg_count = 0;
        for file in &self.files {
            let reader = ColumnarTableReader::new(
                file,
                self.schema.clone(),
                self.filter_op.clone(),
                self.encryption_key.clone(),
            );
            let (idents, count) = reader.get_remote_segments()?;
            seg_idents.extend(idents);
            seg_count += count;
        }
        Ok((seg_idents, seg_count))
    }
}

pub fn decode_decimal_as_int(col_info: &ColumnInfo, decimal: &Decimal) -> Vec<u8> {
    let mut buf = vec![];
    let mut decimal_str = decimal.to_string();
    decimal_str.retain(|c| c != '.');
    let prec = col_info.get_column_len();
    if prec <= 9 {
        let val = decimal_str.parse::<i32>().unwrap_or_default();
        buf.extend_from_slice(&val.to_le_bytes());
    } else if prec <= 18 {
        let val = decimal_str.parse::<i64>().unwrap_or_default();
        buf.extend_from_slice(&val.to_le_bytes());
    } else if prec <= 38 {
        let val = decimal_str.parse::<i128>().unwrap_or_default();
        buf.extend_from_slice(&val.to_le_bytes());
    } else if prec <= 65 {
        let val = decimal_str.parse::<i256>().unwrap_or_default();
        buf.extend_from_slice(&val.to_le_bytes());
    } else {
        panic!("unsupported precision: {}", prec);
    }
    buf
}

#[cfg(test)]
pub use tests::MockColumnarFilterReader;

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::executor::block_on;
    use proptest::{arbitrary::any, proptest};
    use rand::Rng;
    use rstest::rstest;
    use schema::schema::StorageClassSpec;
    use test_util::init_log_for_test;
    use tidb_query_datatype::{
        Collation::Utf8Mb4GeneralCi,
        FieldTypeTp,
        codec::row::v2::encoder_for_test::{Column, RowEncoder},
        expr::EvalContext,
    };

    use super::*;
    use crate::{
        UserMeta, WRITE_CF,
        table::{
            columnar::{
                ColumnarMetaCache,
                builder::{
                    ColumnarFileBuilder, ColumnarTableBuildOptions, ColumnarTableBuilder,
                    new_common_handle_column_info, new_int_handle_column_info,
                    new_version_column_info,
                },
                columnar::ColumnarFile,
                reader::{ColumnarMvccReader, ColumnarReader, ColumnarTableReader},
            },
            file::{File, InMemFile},
            memtable::{CfTable, WriteBatch},
            schema_file::{SchemaBuf, SchemaBufBuilder},
        },
    };

    /// A simple `ColumnarFilterReader` implementation for tests that returns a
    /// prebuilt block once.
    pub struct MockColumnarFilterReader {
        schema: Schema,
        block: Option<Block>,
    }

    impl MockColumnarFilterReader {
        pub fn new(schema: Schema, block: Block) -> Self {
            Self {
                schema,
                block: Some(block),
            }
        }
    }

    #[async_trait]
    impl ColumnarFilterReader for MockColumnarFilterReader {
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

        async fn try_read_block(
            &mut self,
            block: &mut Block,
            _limit: usize,
        ) -> crate::table::Result<(usize, bool)> {
            block.reset();
            if let Some(data) = self.block.take() {
                let rows = data.length();
                block.append(&data, 0, rows);
                Ok((rows, true))
            } else {
                Ok((0, true))
            }
        }
    }

    #[derive(Default, Clone)]
    pub struct RefRow {
        handle: Vec<u8>,
        is_common_handle: bool,
        version: u64,
        txn_id: u64,
        is_deleted: bool,
        c0: Option<i64>,
        c1: Option<Vec<u8>>,
    }

    impl RefRow {
        pub fn in_bound(&self, mut start: &[u8], mut end: &[u8]) -> bool {
            if self.is_common_handle {
                self.handle.as_slice() >= start && self.handle.as_slice() < end
            } else {
                let handle = self.handle.as_slice().get_i64_le();
                handle >= start.get_i64_le() && handle < end.get_i64_le()
            }
        }

        pub fn cmp(&self, other: &Self) -> Ordering {
            let order = if self.is_common_handle {
                self.handle.cmp(&other.handle)
            } else {
                self.handle
                    .as_slice()
                    .get_i64_le()
                    .cmp(&other.handle.as_slice().get_i64_le())
            };
            if order.is_eq() {
                other.version.cmp(&self.version)
            } else {
                order
            }
        }
    }

    fn new_column_info(col_id: i64) -> ColumnInfo {
        let mut col_info = ColumnInfo::new();
        col_info.set_column_id(col_id);
        col_info
    }

    pub fn new_schema(table_id: i64, common_handle: bool) -> Schema {
        new_schema_with_nullable(table_id, common_handle, true)
    }

    pub fn new_schema_with_nullable(table_id: i64, common_handle: bool, nullable: bool) -> Schema {
        let handle_column = if common_handle {
            new_common_handle_column_info()
        } else {
            new_int_handle_column_info()
        };
        let version_column = new_version_column_info();
        let mut col_1 = new_column_info(1);
        col_1.set_tp(FieldTypeTp::LongLong as i32);
        let mut col_2 = new_column_info(2);
        col_2.set_tp(FieldTypeTp::VarChar as i32);
        col_2.set_collation(Utf8Mb4GeneralCi as i32);
        if !nullable {
            col_2.set_flag(FieldTypeFlag::NOT_NULL.bits() as i32);
        }
        let columns = vec![col_1, col_2];
        Schema::new(SchemaBuf::new(
            table_id,
            handle_column,
            version_column,
            columns,
            vec![],
            0,
            vec![],
            vec![],
            StorageClassSpec::default(),
            None,
        ))
    }

    pub fn build_table(
        file_id: u64,
        schema: &Schema,
        start: i32,
        end: i32,
        version: u64,
    ) -> (Arc<dyn File>, Vec<RefRow>) {
        let (tbl, mut refs) = build_table_with_encryption(
            file_id,
            &[schema.clone()],
            start,
            end,
            version,
            false,
            None,
        );
        (tbl, refs.pop().unwrap())
    }

    pub fn build_table_with_all_rows_deleted(
        file_id: u64,
        schema: &Schema,
        start: i32,
        end: i32,
        version: u64,
    ) -> (Arc<dyn File>, Vec<RefRow>) {
        let (tbl, mut refs) = build_table_with_encryption(
            file_id,
            &[schema.clone()],
            start,
            end,
            version,
            true,
            None,
        );
        (tbl, refs.pop().unwrap())
    }

    pub fn build_mixed_table(
        file_id: u64,
        schemas: &[Schema],
        start: i32,
        end: i32,
        version: u64,
    ) -> (Arc<dyn File>, Vec<Vec<RefRow>>) {
        build_table_with_encryption(file_id, schemas, start, end, version, false, None)
    }

    fn new_test_encryption_key() -> EncryptionKey {
        EncryptionKey::new(b"cipher".to_vec(), b"plain".to_vec(), 0)
    }

    pub fn build_table_with_encryption(
        file_id: u64,
        schemas: &[Schema],
        start: i32,
        end: i32,
        version: u64,
        all_deleted: bool, // generate all deleted rows
        encryption_key: Option<EncryptionKey>,
    ) -> (Arc<dyn File>, Vec<Vec<RefRow>>) {
        let mut rng = rand::thread_rng();
        let mut file_builder = ColumnarFileBuilder::new(
            file_id,
            Some(version.into()), // use version as snap_version for test
            encryption_key.clone(),
        );
        let cf_tbl = CfTable::new();
        let mut tables_ref_rows = vec![];
        for schema in schemas {
            let mut ref_rows = vec![];
            let is_common_handle = get_fixed_size(&schema.handle_column) == 0;
            for i in start..end {
                let mut ref_row = new_ref_row(i, version, is_common_handle);
                if all_deleted {
                    ref_row.is_deleted = true;
                }
                ref_rows.push(ref_row);
                if rng.gen_ratio(1, 8) {
                    let mut ref_row = new_ref_row(i, version - 1, is_common_handle);
                    if all_deleted {
                        ref_row.is_deleted = true;
                    }
                    ref_rows.push(ref_row);
                }
                if rng.gen_ratio(1, 16) {
                    let mut ref_row = new_ref_row(i, version - 2, is_common_handle);
                    if all_deleted {
                        ref_row.is_deleted = true;
                    }
                    ref_rows.push(ref_row);
                }
            }
            let mut eval_ctx = EvalContext::default();
            let mut wb = WriteBatch::new();
            for ref_row in ref_rows.iter().rev() {
                let row_key = if is_common_handle {
                    encode_common_handle_row_key(schema.table_id, ref_row.handle.as_slice())
                } else {
                    encode_row_key(schema.table_id, ref_row.handle.as_slice().get_i64_le())
                };
                let inner_key = InnerKey::from_inner_buf(&row_key);
                let mut row_val = vec![];
                let cols = vec![
                    Column::new(1, ref_row.c0),
                    Column::new(2, ref_row.c1.clone()),
                ];
                if !ref_row.is_deleted {
                    row_val.write_row(&mut eval_ctx, cols).unwrap();
                }
                let user_meta = UserMeta::new(ref_row.txn_id, ref_row.version).to_array();
                wb.put(inner_key, 0, &user_meta, ref_row.version, &row_val);
            }
            cf_tbl.get_cf(WRITE_CF).put_batch(&mut wb, None, WRITE_CF);
            tables_ref_rows.push(ref_rows);
        }
        for (i, schema) in schemas.iter().enumerate() {
            let iter = cf_tbl.get_cf(WRITE_CF).new_iterator(false);
            let mut row_tbl_reader = ColumnarRowTableReader::new(
                schema.clone(),
                iter,
                None,
                false,
                encryption_key.clone(),
            );
            let mut block = Block::new(schema);
            let mut opts = ColumnarTableBuildOptions::default();
            opts.pack_max_row_count = 8;
            opts.pack_max_size = 256;
            let mut table_builder =
                ColumnarTableBuilder::new(schema.clone(), opts, encryption_key.clone(), file_id, 0);
            block_on(row_tbl_reader.seek(&tables_ref_rows[i][0].handle)).unwrap();
            let mut append_rows = 0;
            let mut block_off = 0;
            while append_rows < tables_ref_rows[i].len() {
                if block_off == block.length() {
                    block_off = 0;
                    block.reset();
                    let limit = rng.gen_range(2..10);
                    let read = block_on(row_tbl_reader.read(&mut block, limit)).unwrap();
                    if read == 0 {
                        break;
                    }
                }
                let block_off_before = block_off;
                block_off = table_builder.append_block(&block, block_off);
                append_rows += block_off - block_off_before;
            }
            file_builder.add_table(table_builder);
        }

        let (file_data, _) = file_builder.build();
        (
            Arc::new(InMemFile::new(file_id, file_data.into())),
            tables_ref_rows,
        )
    }

    fn new_ref_row(i: i32, version: u64, is_common_handle: bool) -> RefRow {
        let mut ref_row = RefRow::default();
        ref_row.is_common_handle = is_common_handle;
        if is_common_handle {
            ref_row.handle = i_to_common_handle(i);
        } else {
            ref_row.handle = (i as i64).to_le_bytes().to_vec();
        }
        ref_row.version = version;
        ref_row.txn_id = version - 1;
        let mut rng = rand::thread_rng();
        let is_delete = rng.gen_ratio(1, 10);
        ref_row.is_deleted = is_delete;
        let is_null = is_delete || rng.gen_ratio(1, 5);
        if !is_null {
            ref_row.c0 = Some(i as i64);
            ref_row.c1 = Some(i_to_string_col(i, version));
        }
        ref_row
    }

    fn i_to_string_col(i: i32, version: u64) -> Vec<u8> {
        let repeat = 1 + i % 16;
        format!("abc_{}_{}", i, version)
            .repeat(repeat as usize)
            .into_bytes()
    }

    pub fn i_to_common_handle(i: i32) -> Vec<u8> {
        format!("{:08x}", i).into_bytes()
    }

    #[test]
    fn test_columnar_builder() {
        init_log_for_test();
        for common_handle in [true, false] {
            let columnar_meta_cache = ColumnarMetaCache::default();
            let schema = new_schema(1, common_handle);
            let (file, ref_rows) = build_table(1, &schema, 100, 150, 100);
            let columnar_file =
                ColumnarFile::open(file, None, columnar_meta_cache.clone()).unwrap();
            let mut reader = ColumnarTableReader::new(&columnar_file, schema.clone(), None, None);
            block_on(reader.seek(&0u64.to_le_bytes())).unwrap();
            let mut block = Block::new(&schema);
            block_on(reader.read(&mut block, 100)).unwrap();
            verify_with_ref_rows(&block, &ref_rows);
        }
    }

    fn new_mvcc_reader(
        schema: &Schema,
        files: &[Arc<dyn File>],
        read_ts: u64,
        encryption_key: Option<EncryptionKey>,
    ) -> ColumnarMvccReader {
        let mut readers: Vec<Box<dyn ColumnarReader>> = vec![];
        let columnar_meta_cache = ColumnarMetaCache::default();
        for file in files {
            let columnar_file =
                ColumnarFile::open(file.clone(), None, columnar_meta_cache.clone()).unwrap();
            let reader = ColumnarTableReader::new(
                &columnar_file,
                schema.clone(),
                None,
                encryption_key.clone(),
            );
            readers.push(Box::new(reader));
        }
        let merge_reader = ColumnarMergeReader::new(schema.clone(), readers);
        ColumnarMvccReader::new(Box::new(merge_reader), schema, read_ts)
    }

    fn new_compact_reader(
        schema: &Schema,
        level: u32,
        files: &[Arc<dyn File>],
        safe_ts: u64,
    ) -> ColumnarCompactReader {
        let columnar_meta_cache = ColumnarMetaCache::default();
        let mut readers: Vec<Box<dyn ColumnarReader>> = vec![];
        for file in files {
            let columnar_file =
                ColumnarFile::open(file.clone(), None, columnar_meta_cache.clone()).unwrap();
            if !columnar_file.has_table(schema.table_id) {
                continue;
            }
            let reader = ColumnarTableReader::new(&columnar_file, schema.clone(), None, None);
            readers.push(Box::new(reader));
        }
        let merge_reader = ColumnarMergeReader::new(schema.clone(), readers);
        ColumnarCompactReader::new(Box::new(merge_reader), level, schema, safe_ts)
    }

    pub fn verify_with_ref_rows(block: &Block, ref_rows: &[RefRow]) {
        for i in 0..ref_rows.len() {
            let ref_row = &ref_rows[i];
            assert_eq!(ref_row.handle, block.handles.get_not_null_value(i), "{}", i);
            assert_eq!(ref_row.version, block.versions.get_version(i));
            assert_eq!(ref_row.is_deleted, block.versions.is_null(i));
            assert_eq!(ref_row.c0.is_none(), block.columns[0].is_null(i));
            assert_eq!(ref_row.c1.is_none(), block.columns[1].is_null(i));
            if let Some(c0) = ref_row.c0 {
                assert_eq!(c0, block.columns[0].get_value(i).unwrap().get_i64_le());
            }
            if let Some(c1) = &ref_row.c1 {
                assert_eq!(c1.as_slice(), block.columns[1].get_value(i).unwrap());
            }
        }
    }

    pub fn merge_refs(
        refs: Vec<Vec<RefRow>>,
        level: u32,
        read_ts: Option<u64>,
        safe_ts: Option<u64>,
        bound: Option<(Vec<u8>, Vec<u8>)>,
    ) -> Vec<RefRow> {
        let mut merged = vec![];
        for ref_rows in refs {
            merged.extend_from_slice(&ref_rows);
        }
        if let Some(read_ts) = read_ts {
            merged.retain(|r| r.version <= read_ts);
        }
        if let Some((start, end)) = bound {
            merged.retain(|r| r.in_bound(&start, &end));
        }
        merged.sort_by(|a, b| a.cmp(b));
        if read_ts.is_some() {
            merged.dedup_by(|a, b| a.handle == b.handle);
            merged.retain(|r| !r.is_deleted);
        }
        if safe_ts.is_some() {
            let mut new_merged = vec![];
            let mut prev_handle = vec![];
            for r in merged.iter_mut() {
                let handle = r.handle.clone();
                let version = r.version;
                if prev_handle == handle && version < safe_ts.unwrap() {
                    continue;
                }
                if level == 2 && version < safe_ts.unwrap() && r.is_deleted {
                    prev_handle = handle;
                    continue;
                }
                prev_handle = handle;
                new_merged.push(r.clone());
            }
            merged = new_merged;
        }
        merged
    }

    #[rstest]
    #[case::enable_encryption(true)]
    #[case::disable_encryption(false)]
    fn test_reader(#[case] enable_encryption: bool) {
        init_log_for_test();
        let encryption_key = if enable_encryption {
            Some(new_test_encryption_key())
        } else {
            None
        };
        let mut options = vec![];
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            options.push(rng.gen_bool(0.5))
        }
        for common_handle in options {
            let schema = new_schema(1, common_handle);
            let (file_1, mut ref_1) = build_table_with_encryption(
                1,
                &[schema.clone()],
                100,
                150,
                100,
                false,
                encryption_key.clone(),
            );
            let (file_2, mut ref_2) = build_table_with_encryption(
                2,
                &[schema.clone()],
                140,
                190,
                110,
                false,
                encryption_key.clone(),
            );
            let (file_3, mut ref_3) = build_table_with_encryption(
                3,
                &[schema.clone()],
                185,
                240,
                120,
                false,
                encryption_key.clone(),
            );
            let files = [file_1, file_2, file_3];
            let ref_rows = vec![
                ref_1.pop().unwrap(),
                ref_2.pop().unwrap(),
                ref_3.pop().unwrap(),
            ];
            let mut block = Block::new(&schema);
            let mut rng = rand::thread_rng();
            for read_ts in [90, 100, 110, 120] {
                let start_handle = rng.gen_range(90i64..170i64);
                let end_handle = start_handle + rng.gen_range(1i64..200i64);
                let mut mvcc_reader =
                    new_mvcc_reader(&schema, &files, read_ts, encryption_key.clone());
                if common_handle {
                    let common_start_handle = i_to_common_handle(start_handle as i32);
                    let common_end_handle = i_to_common_handle(end_handle as i32);
                    block_on(
                        mvcc_reader.set_handle_range(&common_start_handle, &common_end_handle),
                    )
                    .unwrap();
                } else {
                    block_on(mvcc_reader.set_int_handle_range(start_handle, Some(end_handle)))
                        .unwrap();
                }
                block_on(mvcc_reader.read_block(&mut block, 500)).unwrap();
                let range_bound = if common_handle {
                    (
                        i_to_common_handle(start_handle as i32),
                        i_to_common_handle(end_handle as i32),
                    )
                } else {
                    (
                        start_handle.to_le_bytes().to_vec(),
                        end_handle.to_le_bytes().to_vec(),
                    )
                };
                let merged_refs =
                    merge_refs(ref_rows.clone(), 0, Some(read_ts), None, Some(range_bound));
                verify_with_ref_rows(&block, &merged_refs);
            }
        }
    }

    proptest! {
        #[test]
        fn test_concat_reader(
            common_handle in any::<bool>(),
        ) {
            init_log_for_test();
            let schema = new_schema(1, common_handle);
            let columnar_meta_cache = ColumnarMetaCache::default();
            let (file_1, ref_1) = build_table(1, &schema, 100, 150, 100);
            let (file_2, ref_2) = build_table(2, &schema, 160, 190, 100);
            let (file_3, ref_3) = build_table(3, &schema, 191, 240, 100);
            let files = [file_1, file_2, file_3];
            let ref_rows = vec![ref_1, ref_2, ref_3];
            let col_files: Vec<ColumnarFile> = files.iter().map(|f| ColumnarFile::open(f.clone(), None, columnar_meta_cache.clone()).unwrap()).collect();
            for _ in 0..50 {
                let mut rng = rand::thread_rng();
                let start_handle = rng.gen_range(90i64..170i64);
                let end_handle = start_handle + rng.gen_range(1i64..200i64);
                let mut block = Block::new(&schema);
                let concat_reader = ColumnarConcatReader::new(&col_files, schema.clone(), None, None);
                let mut mvcc_reader = ColumnarMvccReader::new(Box::new(concat_reader), &schema, 100);
                if common_handle {
                    let common_start_handle = i_to_common_handle(start_handle as i32);
                    let common_end_handle = i_to_common_handle(end_handle as i32);
                    block_on(mvcc_reader
                        .set_handle_range(&common_start_handle, &common_end_handle))
                        .unwrap();
                } else {
                    block_on(mvcc_reader
                        .set_int_handle_range(start_handle, Some(end_handle)))
                        .unwrap();
                }
                block_on(mvcc_reader.read_block(&mut block, 500)).unwrap();
                let range_bound = if common_handle {
                    (
                        i_to_common_handle(start_handle as i32),
                        i_to_common_handle(end_handle as i32),
                    )
                } else {
                    (
                        start_handle.to_le_bytes().to_vec(),
                        end_handle.to_le_bytes().to_vec(),
                    )
                };
                let merged_refs = merge_refs(ref_rows.clone(), 0, Some(100), None, Some(range_bound));
                verify_with_ref_rows(&block, &merged_refs);
            }
        }
    }

    proptest! {
        #[test]
        fn test_compact_reader(
            common_handle in any::<bool>(),
        ) {
            init_log_for_test();
            let schema = new_schema(1, common_handle);
            let (file_1, ref_1) = build_table(1, &schema, 100, 200, 100);
            let (file_2, ref_2) = build_table(2, &schema, 150, 250, 200);
            let (file_3, ref_3) = build_table(3, &schema, 200, 300, 300);
            let files = vec![file_1, file_2, file_3];
            let ref_rows = vec![ref_1, ref_2, ref_3];
            for _ in 0..50 {
                let mut rng = rand::thread_rng();
                let start_handle = rng.gen_range(90i64..170i64);
                let end_handle = start_handle + rng.gen_range(1i64..200i64);
                let mut block = Block::new(&schema);
                let mut compact_reader = new_compact_reader(&schema, 2, &files, 250);
                if common_handle {
                    let common_start_handle = i_to_common_handle(start_handle as i32);
                    let common_end_handle = i_to_common_handle(end_handle as i32);
                    block_on(compact_reader
                        .set_handle_range(&common_start_handle, &common_end_handle))
                        .unwrap();
                } else {
                    block_on(compact_reader
                        .set_int_handle_range(start_handle, Some(end_handle)))
                        .unwrap();
                }
                block_on(compact_reader.read_block(&mut block, 1000)).unwrap();
                let range_bound = if common_handle {
                    (
                        i_to_common_handle(start_handle as i32),
                        i_to_common_handle(end_handle as i32),
                    )
                } else {
                    (
                        start_handle.to_le_bytes().to_vec(),
                        end_handle.to_le_bytes().to_vec(),
                    )
                };
                let merged_refs = merge_refs(ref_rows.clone(), 2, None, Some(250), Some(range_bound));
                verify_with_ref_rows(&block, &merged_refs);
            }
        }
    }

    #[test]
    fn test_reader_with_multi_tables() {
        init_log_for_test();
        let schema_1 = new_schema(1, false);
        let schema_2 = new_schema(2, false);
        let schemas = vec![schema_1.clone(), schema_2.clone()];
        let (file_1, ref_rows) = build_mixed_table(1, &schemas, 0, 100, 100);

        for (i, schema) in schemas.iter().enumerate() {
            let mut block = Block::new(schema);
            let ref_row = ref_rows[i].clone();
            let mut mvcc_reader = new_mvcc_reader(schema, &[file_1.clone()], 110, None);
            block_on(mvcc_reader.set_unbounded_handle_range()).unwrap();
            block_on(mvcc_reader.read_block(&mut block, 500)).unwrap();
            let merged_refs = merge_refs(vec![ref_row], 0, Some(110), None, None);
            verify_with_ref_rows(&block, &merged_refs);
        }
    }

    #[test]
    fn test_read_column_default_value() {
        init_log_for_test();
        let schema = new_schema(1, false);
        let (file, ref_row) = build_table(1, &schema, 0, 100, 100);

        let mut schema_buf_builder = SchemaBufBuilder::new(1);
        let mut columns = schema.columns.clone();
        let mut column_info = ColumnInfo::new();
        column_info.set_tp(FieldTypeTp::Int24 as i32);
        let default_val = [vec![UINT_FLAG], 666_u64.to_be_bytes().to_vec()].concat();
        column_info.set_default_val(default_val);
        column_info.set_column_id(100);
        columns.push(column_info);
        schema_buf_builder.columns(
            schema.handle_column.clone(),
            schema.version_column.clone(),
            columns,
            schema.pk_col_ids.clone(),
            100,
            schema.vector_indexes.clone(),
            schema.fulltext_indexes.clone(),
        );
        let new_schema = Schema::new(schema_buf_builder.build());
        // Add a new column with default value to read.
        let mut mvcc_reader = new_mvcc_reader(&new_schema, &[file], 110, None);
        block_on(mvcc_reader.set_unbounded_handle_range()).unwrap();
        let mut block = Block::new(&new_schema);
        block_on(mvcc_reader.read_block(&mut block, 500)).unwrap();
        let merged_refs = merge_refs(vec![ref_row], 0, Some(110), None, None);
        verify_with_ref_rows(&block, &merged_refs);
        let column = block.get_column(100);
        assert_eq!(column.length(), block.handles.length());
        for i in 0..block.handles.length() {
            assert_eq!(column.get_value(i).unwrap(), 666_u64.to_le_bytes());
        }
    }

    #[test]
    fn test_filter_reader_drained() {
        init_log_for_test();
        let schema = new_schema(1, false);
        let (file_1, ref_1) = build_table(1, &schema, 100, 200, 100);
        let (file_2, ref_2) = build_table(2, &schema, 150, 250, 200);
        let (file_3, ref_3) = build_table(3, &schema, 200, 300, 300);
        let (file_4, ref_4) = build_table_with_all_rows_deleted(4, &schema, 50, 150, 90);
        let files = vec![file_1, file_2, file_3, file_4];
        let ref_rows = vec![ref_1, ref_2, ref_3, ref_4];
        let mut block = Block::new(&schema);
        let mut compact_reader = new_compact_reader(&schema, 2, &files, 250);
        block_on(compact_reader.set_unbounded_handle_range()).unwrap();
        loop {
            let mut read_block = Block::new(&schema);
            let rows = block_on(compact_reader.read_block(&mut read_block, 10)).unwrap();
            if rows == 0 {
                break;
            }
            block.append(&read_block, 0, rows);
        }
        let merged_refs = merge_refs(ref_rows.clone(), 2, None, Some(250), None);
        verify_with_ref_rows(&block, &merged_refs);
    }
}
