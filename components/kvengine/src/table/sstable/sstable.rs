// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering,
    fmt,
    future::Future,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, Bytes, BytesMut};
use cloud_encryption::EncryptionKey;
use kvenginepb::TableCreate;
use schema::schema::StorageClass;
use tikv_util::sys::SysQuota;
use xorf::{BinaryFuse8, Filter};

use super::{builder::*, iterator::TableIterator};
use crate::{
    IoContext, WRITE_CF,
    ia::{ia_auto_file::IaAutoFile, ia_file::IaFile, types::FileSegmentIdent},
    next_version, next_version_async,
    table::{
        file::{File, TtlCache},
        table::{Iterator, Result},
        tiny_meta::SstTinyMeta,
        *,
    },
};

// higher level ttl is longer than lower level.
const IDX_TTL_LEVELS: [u64; 4] = [60 * 8, 60 * 4, 60 * 2, 60];
const FILTER_TTL_LEVELS: [u64; 4] = [60 * 2, 60, 30, 15];
const SMALL_VALUE_SIZE: u64 = 96;

#[derive(Clone)]
pub struct SsTable {
    core: Arc<SsTableCore>,
}

impl Deref for SsTable {
    type Target = SsTableCore;
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl fmt::Debug for SsTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SsTable")
            .field("id", &self.id())
            .field("size", &self.size())
            .field("entries", &self.entries)
            .field("old_entries", &self.old_entries)
            .field("tombs", &self.tombs)
            .field("max_ts", &self.max_ts)
            .field("kv_size", &self.kv_size)
            .field("snap_version", &self.snap_version)
            .field("is_sync", &self.is_sync())
            .finish()
    }
}

impl SsTable {
    pub fn new(
        file: Arc<dyn File>,
        cache: BlockCache,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Self> {
        let mut ctx = NewSsTableCtx::default();
        Self::new_with_ctx(file, cache, encryption_key, &mut ctx)
    }

    pub fn new_with_ctx(
        file: Arc<dyn File>,
        cache: BlockCache,
        encryption_key: Option<EncryptionKey>,
        ctx: &mut NewSsTableCtx,
    ) -> Result<Self> {
        let size = file.size();
        let core = SsTableCore::new(file, 0, size, cache, encryption_key, ctx)?;
        Ok(Self {
            core: Arc::new(core),
        })
    }

    // L0 does not use IA files.
    pub fn new_l0_cf(
        file: Arc<dyn File>,
        start: u64,
        end: u64,
        cache: BlockCache,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Self> {
        let mut ctx = NewSsTableCtx::default();
        let core = SsTableCore::new(file, start, end, cache, encryption_key, &mut ctx)?;
        Ok(Self {
            core: Arc::new(core),
        })
    }

    pub fn new_iterator(&self, reversed: bool, fill_cache: bool) -> Box<TableIterator> {
        let it = TableIterator::new(self.clone(), reversed, fill_cache);
        Box::new(it)
    }

    // Get the value with the given key and version.
    // It is caller's responsibility to maintain the lifetime of the returned value.
    // Value will be filled in out_val_owner , while the returned value is a parsed
    // slice of it.
    #[maybe_async::both]
    pub async fn get(
        &self,
        key: InnerKey<'_>,
        version: u64,
        key_hash: u64,
        out_val_owner: &mut Vec<u8>,
        level: usize,
    ) -> table::Value {
        // For small value on level 3, load the filter is not cost-effective.
        // TODO: avoid build filter on level 3 small value table.
        let small_value = self.kv_size < self.entries as u64 * SMALL_VALUE_SIZE;
        let skip_filter = small_value && level == 3;
        if self.filter_size() > 0 && !skip_filter {
            let filter = self.get_filter();
            if !filter.contains(&key_hash) {
                return table::Value::new();
            }
        }
        let mut it = self.new_iterator(false, true);
        it.seek(key).await;
        if !it.valid() || key != it.key() {
            return table::Value::new();
        }
        while it.value().version > version {
            if !next_version!(it).await {
                return table::Value::new();
            }
        }
        // Reconstruct the value to avoid the lifetime issue.
        let val = it.value();
        out_val_owner.resize(val.encoded_size(), 0);
        val.encode(out_val_owner.as_mut_slice());
        Value::decode(out_val_owner.as_slice())
    }

    #[maybe_async::both]
    async fn check_overlap_by_seek(&self, data_bound: DataBound<'_>) -> bool {
        let mut it = self.new_iterator(false, true);
        it.seek(data_bound.lower_bound).await;
        if !it.valid() {
            return it.error().is_some();
        }
        !data_bound.less_than_key(it.key())
    }

    #[maybe_async::both]
    pub async fn has_overlap(&self, data_bound: DataBound<'_>) -> bool {
        if !self.data_bound().overlap_bound(data_bound) {
            return false;
        }
        self.check_overlap_by_seek(data_bound).await
    }

    /// Used when seek on async tables is expensive and strictly overlapped is
    /// not necessary.
    pub fn has_overlap_loose(&self, data_bound: DataBound<'_>) -> bool {
        if !self.data_bound().overlap_bound(data_bound) {
            return false;
        }
        !self.is_sync() || self.check_overlap_by_seek(data_bound)
    }

    #[maybe_async::both]
    pub async fn has_any_overlap<'a>(
        &self,
        bounds: impl std::iter::Iterator<Item = DataBound<'a>>,
    ) -> bool {
        for bound in bounds {
            if self.has_overlap(bound).await {
                return true;
            }
        }
        false
    }

    #[maybe_async::both]
    pub async fn get_newer(
        &self,
        key: InnerKey<'_>,
        version: u64,
        key_hash: u64,
        out_val_owner: &mut Vec<u8>,
        level: usize,
    ) -> table::Value {
        if self.max_ts <= version {
            return table::Value::new();
        }
        let val = self
            .get(key, u64::MAX, key_hash, out_val_owner, level)
            .await;
        if val.version > version {
            return val;
        }
        table::Value::new()
    }

    pub(crate) fn clone_smallest(&self) -> Bytes {
        self.smallest_buf.clone()
    }

    pub(crate) fn clone_biggest(&self) -> Bytes {
        self.biggest_buf.clone()
    }

    pub const fn footer_size() -> usize {
        FOOTER_SIZE
    }

    #[inline]
    pub fn is_sync(&self) -> bool {
        self.file.is_sync()
    }

    #[inline]
    pub fn is_cf_sync(&self, cf: usize) -> bool {
        cf != WRITE_CF || self.is_sync()
    }

    pub fn get_remote_segments(
        &self,
        data_bound: DataBound<'_>,
    ) -> Result<(Vec<FileSegmentIdent>, usize /* total_segments */)> {
        if self.file.is_sync() {
            return Ok((vec![], 0));
        }

        let idx = self.load_index();
        let (first_block, exclusive_last_block) = idx.seek_overlap_blocks(data_bound);
        let start_off = idx.get_block_addr(first_block).curr_off as u64;
        let end_off = if exclusive_last_block < idx.num_blocks() {
            idx.get_block_addr(exclusive_last_block).curr_off as u64
        } else {
            self.data_end_offset() as u64
        };

        self.file.get_remote_segments(&[(start_off, end_off)])
    }

    pub fn get_meta_data(file_data: &[u8]) -> Result<Bytes> {
        let data_len = file_data.len();
        let footer_size = SsTable::footer_size();
        if data_len < footer_size {
            error!("get_meta_data: invalid fie size"; "data_len" => data_len);
            return Err(Error::InvalidFileSize);
        }
        let footer_data = &file_data[data_len - footer_size..];

        let mut footer = Footer::default();
        footer.unmarshal(footer_data);

        let meta_off = footer.meta_offset() as usize;
        if data_len < meta_off {
            error!("get_meta_data: invalid meta offset"; "data_len" => data_len, "footer" => ?footer);
            return Err(Error::InvalidFileSize);
        }
        Ok(Bytes::copy_from_slice(&file_data[meta_off..]))
    }
}

impl BoundedDataSet for SsTable {
    fn data_bound(&self) -> DataBound<'_> {
        DataBound::new(self.smallest(), self.biggest(), true)
    }
}

pub struct SsTableCore {
    file: Arc<dyn File>,
    cache: BlockCache,
    filter: TtlCache<BinaryFuse8>,
    start_off: u64,
    end_off: u64,
    footer: Footer,
    smallest_buf: Bytes,
    biggest_buf: Bytes,
    pub max_ts: u64,
    pub max_lock_ts: u64,
    pub entries: u32,
    pub old_entries: u32,
    pub tombs: u32,
    pub kv_size: u64,
    pub in_use_total_blob_size: u64,
    idx: TtlCache<Index>,
    old_idx: TtlCache<Index>,
    encryption_key: Option<EncryptionKey>,
    encryption_ver: u32,
    pub snap_version: SnapVersion,
}

pub enum SsTableProperty {
    MaxTs(u64),
}

impl SsTableCore {
    pub fn new(
        file: Arc<dyn File>,
        start_off: u64,
        end_off: u64,
        cache: BlockCache,
        encryption_key: Option<EncryptionKey>,
        ctx: &mut NewSsTableCtx,
    ) -> Result<Self> {
        let size = end_off - start_off;

        let (footer, props_data) = match ctx.take_footer_and_properties() {
            Some(x) => x,
            None => {
                let (footer, props_data) = Self::read_footer_and_properties_from_file(
                    file.as_ref(),
                    start_off,
                    end_off,
                    encryption_key.as_ref(),
                )?;
                if end_off == file.size() {
                    // Handle normal SST (not L0) only by now.
                    ctx.put_footer_and_properties(footer, props_data.clone());
                }
                (footer, props_data)
            }
        };

        let mut prop_slice = &props_data.chunk()[4..]; // Trim checksum.
        let mut smallest_buf = Bytes::new();
        let mut biggest_buf = Bytes::new();
        let mut max_ts = 0;
        let mut max_lock_ts = 0;
        let mut entries = 0;
        let mut old_entries = 0;
        let mut tombs = 0;
        let mut kv_size = None;
        let mut in_use_total_blob_size = 0u64;
        let mut encryption_ver = 0;
        let mut snap_version = SnapVersion::default();
        while !prop_slice.is_empty() {
            let (key, val, remain) = parse_prop_data(prop_slice);
            prop_slice = remain;
            if key == PROP_KEY_SMALLEST {
                smallest_buf = Bytes::copy_from_slice(val);
            } else if key == PROP_KEY_BIGGEST {
                biggest_buf = Bytes::copy_from_slice(val);
            } else if key == PROP_KEY_MAX_TS {
                max_ts = LittleEndian::read_u64(val);
            } else if key == PROP_KEY_MAX_LOCK_TS {
                max_lock_ts = LittleEndian::read_u64(val);
            } else if key == PROP_KEY_ENTRIES {
                entries = LittleEndian::read_u32(val);
            } else if key == PROP_KEY_OLD_ENTRIES {
                old_entries = LittleEndian::read_u32(val);
            } else if key == PROP_KEY_TOMBS {
                tombs = LittleEndian::read_u32(val);
            } else if key == PROP_KEY_KV_SIZE {
                kv_size = Some(LittleEndian::read_u64(val));
            } else if key == PROP_KEY_IN_USE_TOTAL_BLOB_SIZE {
                in_use_total_blob_size = LittleEndian::read_u64(val);
            } else if key == PROP_KEY_ENCRYPTION_VER {
                encryption_ver = LittleEndian::read_u32(val);
            } else if key == PROP_KEY_SNAP_VERSION {
                snap_version = LittleEndian::read_u64(val).into();
            }
        }
        let core = Self {
            file,
            cache,
            filter: TtlCache::default(),
            start_off,
            end_off,
            footer,
            smallest_buf,
            biggest_buf,
            max_ts,
            max_lock_ts,
            entries,
            old_entries,
            tombs,
            kv_size: kv_size.unwrap_or(size),
            in_use_total_blob_size,
            idx: TtlCache::default(),
            old_idx: TtlCache::default(),
            encryption_ver,
            encryption_key,
            snap_version,
        };
        Ok(core)
    }

    fn read_footer_and_properties_from_file(
        file: &dyn File,
        start_off: u64,
        end_off: u64,
        encryption_key: Option<&EncryptionKey>,
    ) -> Result<(Footer, Bytes)> {
        let size = end_off - start_off;

        let mut footer = Footer::default();
        let footer_data = if end_off == file.size() {
            file.read_footer(FOOTER_SIZE)?
        } else {
            if size < FOOTER_SIZE as u64 {
                return Err(table::Error::InvalidFileSize);
            }
            // It's a L0 sst, and must be sync.
            file.read(end_off - FOOTER_SIZE as u64, FOOTER_SIZE)?
        };
        footer.unmarshal(footer_data.chunk());
        if footer.magic != MAGIC_NUMBER && footer.magic != MAGIC_NUMBER_SPLIT_L0 {
            return Err(table::Error::InvalidMagicNumber);
        }
        let props_data = file.read_table_meta(
            start_off + footer.properties_offset as u64,
            footer.properties_len(size as usize),
        )?;
        let prop_slice = props_data.chunk();
        validate_checksum_with_fix(prop_slice, &footer, file, encryption_key)?;

        Ok((footer, props_data))
    }

    // Support max_ts only.
    // TODO: Support other properties.
    pub fn extract_property_from_table_meta_file(
        table_meta_file: &dyn File,
        prop_key: &[u8],
    ) -> Result<SsTableProperty> {
        let mut footer = Footer::default();
        let footer_data = table_meta_file.read_footer(FOOTER_SIZE)?;
        footer.unmarshal(footer_data.chunk());
        if !footer.is_match() {
            return Err(table::Error::InvalidMagicNumber);
        }

        let meta_size = table_meta_file.size();
        let table_meta_off = footer.meta_offset() as u64;
        let table_size = table_meta_off + meta_size;

        let props_data = table_meta_file.read_table_meta(
            footer.properties_offset as u64 - table_meta_off,
            footer.properties_len(table_size as usize),
        )?;
        let mut prop_slice = props_data.chunk();
        validate_checksum_with_fix(prop_slice, &footer, table_meta_file, None)?;
        prop_slice = &prop_slice[4..];
        while !prop_slice.is_empty() {
            let (key, val, remain) = parse_prop_data(prop_slice);
            prop_slice = remain;
            if key == prop_key {
                let v = match key {
                    PROP_KEY_MAX_TS => SsTableProperty::MaxTs(LittleEndian::read_u64(val)),
                    _ => {
                        return Err(table::Error::Other(format!(
                            "property not supported: {:?}",
                            key
                        )));
                    }
                };
                return Ok(v);
            }
        }
        Err(table::Error::Other(format!(
            "property not found: {:?}",
            prop_key
        )))
    }

    pub fn footer(&self) -> Footer {
        self.footer
    }

    pub fn init_index(&self, offset: u32, length: usize) -> Result<Index> {
        let idx_data = self
            .file
            .read_table_meta(self.start_off + offset as u64, length)?;
        self.validate_checksum_with_fix(idx_data.chunk())?;
        Index::new(idx_data)
    }

    pub fn load_index(&self) -> Arc<Index> {
        self.idx
            .get(|| self.init_index(self.footer.index_offset, self.footer.index_len()))
            .expect("load index")
    }

    pub fn load_old_index(&self) -> Arc<Index> {
        self.old_idx
            .get(|| self.init_index(self.footer.old_index_offset, self.footer.old_index_len()))
            .expect("load old index")
    }

    pub fn expire_cache(&self, level: usize) {
        self.filter.expire(FILTER_TTL_LEVELS[level]);
        self.idx.expire(IDX_TTL_LEVELS[level]);
        self.old_idx.expire(IDX_TTL_LEVELS[level]);
    }

    #[maybe_async::both]
    pub async fn load_block(
        &self,
        idx: &Index,
        pos: usize,
        buf: &mut Vec<u8>,
        decryption_buf: &mut Vec<u8>,
        fill_cache: bool,
    ) -> Result<Bytes> {
        let addr = idx.get_block_addr(pos);
        let length = if pos + 1 < idx.num_blocks() {
            let next_addr = idx.get_block_addr(pos + 1);
            (next_addr.curr_off - addr.curr_off) as usize
        } else {
            self.start_off as usize + self.footer.data_len() - addr.curr_off as usize
        };
        self.load_block_by_addr_len(addr, length, buf, decryption_buf, fill_cache)
            .await
    }

    fn load_block_by_addr_len(
        &self,
        addr: BlockAddress,
        length: usize,
        buf: &mut Vec<u8>,
        decryption_buf: &mut Vec<u8>,
        fill_cache: bool,
    ) -> Result<Bytes> {
        let cache_key = BlockCacheKey::new(addr.origin_fid, addr.origin_off);
        self.cache.try_get_with_ext(
            cache_key,
            || self.read_block_from_file(addr, length, buf, decryption_buf),
            fill_cache,
        )
    }

    async fn load_block_by_addr_len_async(
        &self,
        addr: BlockAddress,
        length: usize,
        buf: &mut Vec<u8>,
        decryption_buf: &mut Vec<u8>,
        fill_cache: bool,
    ) -> Result<Bytes> {
        let cache_key = BlockCacheKey::new(addr.origin_fid, addr.origin_off);
        self.cache
            .try_get_with_ext_async(
                cache_key,
                self.read_block_from_file_async(addr, length, buf, decryption_buf),
                fill_cache,
            )
            .await
    }

    #[maybe_async::both]
    async fn read_block_from_file(
        &self,
        addr: BlockAddress,
        length: usize,
        buf: &mut Vec<u8>,
        decryption_buf: &mut Vec<u8>,
    ) -> Result<Bytes> {
        let compression_type = self.footer.compression_type;
        if compression_type == NO_COMPRESSION {
            let mut raw_block = self.file.read(addr.curr_off as u64, length).await?;
            if let Some(encryption_key) = &self.encryption_key {
                let mut block = Vec::with_capacity(length + encryption_key.encryption_block_size());
                encryption_key.decrypt(
                    raw_block.chunk(),
                    addr.origin_fid,
                    addr.curr_off,
                    self.encryption_ver,
                    &mut block,
                );
                raw_block = Bytes::from(block)
            }
            self.validate_checksum_with_fix(raw_block.chunk())?;
            return Ok(raw_block.slice(4..));
        }
        buf.resize(length, 0);
        if let Some(encryption_key) = &self.encryption_key {
            decryption_buf.resize(length, 0);
            self.file
                .read_at(decryption_buf, addr.curr_off as u64)
                .await?;
            buf.truncate(0);
            encryption_key.decrypt(
                decryption_buf,
                addr.origin_fid,
                addr.curr_off,
                self.encryption_ver,
                buf,
            );
        } else {
            self.file.read_at(buf, addr.curr_off as u64).await?;
        }
        self.validate_checksum_with_fix(buf)?;
        let content = &buf[4..];
        match compression_type {
            LZ4_COMPRESSION => {
                let block = lz4::block::decompress(content, None)
                    .table_ctx(self.file.id(), "sst.read_block.lz4_decompress")?;
                Ok(Bytes::from(block))
            }
            ZSTD_COMPRESSION => {
                let capacity = unsafe {
                    zstd_sys::ZSTD_getFrameContentSize(
                        content.as_ptr() as *const libc::c_void,
                        content.len(),
                    ) as usize
                };
                let mut block = Vec::<u8>::with_capacity(capacity);
                unsafe {
                    let result = zstd_sys::ZSTD_decompress(
                        block.as_mut_ptr() as *mut libc::c_void,
                        capacity,
                        content.as_ptr() as *const libc::c_void,
                        content.len(),
                    );
                    assert_eq!(zstd_sys::ZSTD_isError(result), 0u32);
                    block.set_len(capacity);
                }
                Ok(Bytes::from(block))
            }
            _ => panic!("unknown compression type {}", compression_type),
        }
    }

    #[maybe_async::both]
    pub async fn load_old_block(
        &self,
        old_idx: &Index,
        pos: usize,
        buf: &mut Vec<u8>,
        decryption_buf: &mut Vec<u8>,
        fill_cache: bool,
    ) -> Result<Bytes> {
        let addr = old_idx.get_block_addr(pos);
        let length = if pos + 1 < old_idx.num_blocks() {
            let next_addr = old_idx.get_block_addr(pos + 1);
            (next_addr.curr_off - addr.curr_off) as usize
        } else {
            self.start_off as usize + self.footer.index_offset as usize - addr.curr_off as usize
        };
        self.load_block_by_addr_len(addr, length, buf, decryption_buf, fill_cache)
            .await
    }

    fn get_filter(&self) -> Arc<BinaryFuse8> {
        self.filter
            .get(|| {
                let filter_data = self.read_filter_data_from_file()?;
                let filter = self.decode_filter(&filter_data)?;
                Ok::<_, Error>(filter)
            })
            .expect("load filter")
    }

    fn read_filter_data_from_file(&self) -> Result<Bytes> {
        let mut data = self
            .file
            .read_table_meta(self.filter_offset() as u64, self.filter_size() as usize)?;
        self.validate_checksum_with_fix(data.chunk())?;
        data.get_u32_le();
        assert_eq!(data.get_u32_le(), AUX_INDEX_BINARY_FUSE8);
        let len = data.get_u32_le();
        assert_eq!(len as usize, data.len());
        Ok(data)
    }

    fn decode_filter(&self, data: &Bytes) -> Result<BinaryFuse8> {
        BinaryFuse8::try_from_bytes(data).map_err(|e| Error::Other(e.to_string()))
    }

    pub fn id(&self) -> u64 {
        self.file.id()
    }

    pub fn file(&self) -> &Arc<dyn File> {
        &self.file
    }

    pub fn try_get_ia_file(&self) -> Option<Arc<IaFile>> {
        self.file.clone().as_any().downcast::<IaFile>().ok()
    }

    pub fn try_get_auto_ia_file(&self) -> Option<Arc<IaAutoFile>> {
        self.file.clone().as_any().downcast::<IaAutoFile>().ok()
    }

    #[inline]
    pub(crate) fn is_storage_class_ia(&self) -> bool {
        self.file.storage_class() == StorageClass::Ia
    }

    /// The size on disk in bytes.
    /// Should be equal to `self.file.size()` for L1+, and less than or equal to
    /// for L0.
    pub fn size(&self) -> u64 {
        self.end_off - self.start_off
    }

    pub fn index_size(&self) -> u64 {
        (self.footer.index_len() + self.footer.old_index_len()) as u64
    }

    pub fn in_mem_index_size(&self) -> u64 {
        let idx_in_mem = if self.idx.is_loaded() {
            self.footer.index_len()
        } else {
            0
        };
        let old_idx_in_mem = if self.old_idx.is_loaded() {
            self.footer.old_index_len()
        } else {
            0
        };
        (idx_in_mem + old_idx_in_mem) as u64
    }

    fn data_end_offset(&self) -> u32 {
        self.start_off as u32 + self.footer.data_len() as u32
    }

    /// The offset of first table meta (index).
    #[inline]
    pub fn meta_offset(&self) -> u32 {
        self.start_off as u32 + self.footer.meta_offset()
    }

    pub fn index_offset(&self) -> u32 {
        self.start_off as u32 + self.footer.index_offset
    }

    fn filter_offset(&self) -> u32 {
        self.start_off as u32 + self.footer.aux_index_offset
    }

    pub fn filter_size(&self) -> u64 {
        self.footer.aux_index_len() as u64
    }

    pub fn in_mem_filter_size(&self) -> u64 {
        if self.filter.is_loaded() {
            self.footer.aux_index_len() as u64
        } else {
            0
        }
    }

    pub fn smallest(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(self.smallest_buf.chunk())
    }

    pub fn biggest(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(self.biggest_buf.chunk())
    }

    pub fn compression_type(&self) -> u8 {
        self.footer.compression_type
    }

    pub fn total_blob_size(&self) -> u64 {
        self.in_use_total_blob_size
    }

    pub fn encryption_ver(&self) -> u32 {
        self.encryption_ver
    }

    pub fn get_max_lock_ts(&self) -> u64 {
        // For legacy file, the max_lock_ts is zero, we use file id instead.
        // TODO: use tbl.max_lock_ts directly after all old lock files are GCed.
        if self.max_lock_ts > 0 {
            self.max_lock_ts
        } else {
            self.file.id()
        }
    }

    #[inline]
    fn validate_checksum_with_fix(&self, data: &[u8]) -> Result<()> {
        validate_checksum_with_fix(
            data,
            &self.footer,
            self.file.as_ref(),
            self.encryption_key.as_ref(),
        )
    }

    pub fn to_table_create(&self, cf: usize, lvl: usize) -> TableCreate {
        let mut table_create = TableCreate::new();
        table_create.set_id(self.id());
        table_create.set_smallest(self.smallest().to_vec());
        table_create.set_biggest(self.biggest().to_vec());
        table_create.set_cf(cf as i32);
        table_create.set_level(lvl as u32);
        table_create.set_meta_offset(self.meta_offset());
        table_create
    }
}

// Used in sstable and blobtable.
#[derive(Clone)]
pub struct Index {
    common_prefix: Bytes,
    block_key_offs: Bytes,
    block_addrs: Bytes,
    block_keys: Bytes,
}

impl Index {
    pub(crate) fn new(data: Bytes) -> Result<Self> {
        Self::new_with_opts(data, BLOCK_ADDR_SIZE)
    }

    pub(crate) fn new_for_blob(data: Bytes) -> Result<Self> {
        Self::new_with_opts(data, 4)
    }

    fn new_with_opts(mut data: Bytes, block_addr_size: usize) -> Result<Self> {
        let _checksum = data.get_u32_le();
        assert_eq!(data.get_u32_le(), INDEX_FORMAT_V1);
        let num_blocks = data.get_u32_le() as usize;
        let block_key_offs = data.slice(..num_blocks * 4);
        data.advance(block_key_offs.len());
        let block_addrs = data.slice(..num_blocks * block_addr_size);
        data.advance(block_addrs.len());
        let common_prefix_len = data.get_u16_le() as usize;
        let common_prefix = data.slice(..common_prefix_len);
        data.advance(common_prefix.len());
        let block_key_len = data.get_u32_le() as usize;
        let block_keys = data.slice(..block_key_len);
        Ok(Self {
            common_prefix,
            block_key_offs,
            block_addrs,
            block_keys,
        })
    }

    fn get_common_prefix(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(self.common_prefix.chunk())
    }

    pub(crate) fn get_block_addr(&self, pos: usize) -> BlockAddress {
        let off = pos * BLOCK_ADDR_SIZE;
        BlockAddress::from_slice(&self.block_addrs[off..off + BLOCK_ADDR_SIZE])
    }

    pub(crate) fn get_block_addr_for_blob(&self, pos: usize) -> u32 {
        let off = pos * 4;
        LittleEndian::read_u32(&self.block_addrs[off..off + 4])
    }

    pub fn num_blocks(&self) -> usize {
        self.block_key_offs.len() / 4
    }

    /// Returns the block index of the first block whose key > `key`.
    pub fn seek_block(&self, key: InnerKey<'_>) -> usize {
        let common_prefix = self.get_common_prefix();
        if key.len() <= common_prefix.len() {
            if key <= common_prefix {
                return 0;
            }
            return self.num_blocks();
        }
        let cmp = key.slice(0, common_prefix.len()).cmp(&common_prefix);
        match cmp {
            Ordering::Less => 0,
            Ordering::Equal => {
                let diff_key = &key[common_prefix.len()..];
                search(self.num_blocks(), |i| self.block_diff_key(i) > diff_key)
            }
            Ordering::Greater => self.num_blocks(),
        }
    }

    /// Returns the block index of the first block whose key >= `key`.
    pub fn seek_block_bigger_or_equal(&self, key: InnerKey<'_>) -> usize {
        let common_prefix = self.get_common_prefix();
        if key.len() <= common_prefix.len() {
            if key <= common_prefix {
                return 0;
            }
            return self.num_blocks();
        }
        let cmp = key[..common_prefix.len()].cmp(&common_prefix);
        match cmp {
            Ordering::Less => 0,
            Ordering::Equal => {
                let diff_key = &key[common_prefix.len()..];
                search(self.num_blocks(), |i| self.block_diff_key(i) >= diff_key)
            }
            Ordering::Greater => self.num_blocks(),
        }
    }

    pub fn seek_overlap_blocks(
        &self,
        data_bound: DataBound<'_>,
    ) -> (
        usize, // first_block_idx
        usize, // exclusive_last_block_idx
    ) {
        let first_idx = self.seek_block(data_bound.lower_bound).saturating_sub(1);
        let last_exclusive_idx = if data_bound.upper_inclusive {
            self.seek_block(data_bound.upper_bound)
        } else {
            self.seek_block_bigger_or_equal(data_bound.upper_bound)
        };
        (first_idx, last_exclusive_idx)
    }

    fn block_diff_key(&self, i: usize) -> &[u8] {
        let off = self.get_block_key_off(i);
        let end_off = if i + 1 < self.num_blocks() {
            self.get_block_key_off(i + 1)
        } else {
            self.block_keys.len()
        };
        &self.block_keys[off..end_off]
    }

    fn get_block_key_off(&self, i: usize) -> usize {
        (&self.block_key_offs[i * 4..]).get_u32_le() as usize
    }

    pub(crate) fn clone_block_key(&self, i: usize) -> OwnedInnerKey {
        let diff_key = self.block_diff_key(i);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(self.common_prefix.chunk());
        buf.extend_from_slice(diff_key);
        OwnedInnerKey::new(buf.freeze())
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug)]
pub struct BlockCacheKey {
    origin_id: u64,
    origin_off: u32,
}

impl BlockCacheKey {
    pub fn new(origin_id: u64, origin_off: u32) -> Self {
        Self {
            origin_id,
            origin_off,
        }
    }
}

const BLOCK_CACHE_KEY_SIZE: usize = std::mem::size_of::<BlockCacheKey>();
const BLOCK_CACHE_SHARDS_PER_CORE: usize = 64;

#[inline]
fn block_weight(_k: &BlockCacheKey, v: &Bytes) -> usize {
    BLOCK_CACHE_KEY_SIZE + v.len()
}

#[repr(u8)]
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Copy, Default)]
#[serde(rename_all = "kebab-case")]
pub enum BlockCacheType {
    #[default]
    Quick = 1,
    None = 2,
}

#[derive(Clone)]
pub enum BlockCache {
    Quick(Arc<quick_cache::sync::Cache<BlockCacheKey, Bytes, BlockWeighter>>),
    None,
}

impl BlockCache {
    pub fn new(tp: BlockCacheType, max_capacity: u64, block_size: usize) -> Self {
        if max_capacity == 0 {
            return BlockCache::None;
        }
        let block_cache_shards =
            (SysQuota::cpu_cores_quota() as usize).max(1) * BLOCK_CACHE_SHARDS_PER_CORE;
        match tp {
            BlockCacheType::Quick => {
                let opts = quick_cache::OptionsBuilder::new()
                    .shards(block_cache_shards)
                    .weight_capacity(max_capacity)
                    .estimated_items_capacity(
                        max_capacity as usize / (BLOCK_CACHE_KEY_SIZE + block_size),
                    )
                    .build()
                    .unwrap();
                let cache = quick_cache::sync::Cache::with_options(
                    opts,
                    BlockWeighter,
                    quick_cache::DefaultHashBuilder::default(),
                    quick_cache::sync::DefaultLifecycle::default(),
                );
                Self::Quick(Arc::new(cache))
            }
            BlockCacheType::None => Self::None,
        }
    }

    pub fn get(&self, key: &BlockCacheKey) -> Option<Bytes> {
        match self {
            Self::Quick(cache) => cache.get(key),
            Self::None => None,
        }
    }

    pub fn try_get_with(
        &self,
        key: BlockCacheKey,
        init: impl FnOnce() -> Result<Bytes>,
    ) -> Result<Bytes> {
        let init = || {
            self.report_cache_miss();
            init()
        };
        match self {
            Self::Quick(cache) => cache.get_or_insert_with(&key, init),
            Self::None => init(),
        }
    }

    pub fn try_get_with_ext(
        &self,
        key: BlockCacheKey,
        init: impl FnOnce() -> Result<Bytes>,
        fill_cache: bool,
    ) -> Result<Bytes> {
        if fill_cache {
            self.try_get_with(key, init)
        } else if let Some(block) = self.get(&key) {
            Ok(block)
        } else {
            self.report_cache_miss();
            init()
        }
    }

    pub async fn try_get_with_async(
        &self,
        key: BlockCacheKey,
        init: impl Future<Output = Result<Bytes>>,
    ) -> Result<Bytes> {
        let init = async {
            self.report_cache_miss();
            init.await
        };
        match self {
            Self::None => init.await,
            Self::Quick(cache) => cache.get_or_insert_async(&key, init).await,
        }
    }

    pub async fn try_get_with_ext_async(
        &self,
        key: BlockCacheKey,
        init: impl Future<Output = Result<Bytes>>,
        fill_cache: bool,
    ) -> Result<Bytes> {
        if fill_cache {
            self.try_get_with_async(key, init).await
        } else if let Some(block) = self.get(&key) {
            Ok(block)
        } else {
            self.report_cache_miss();
            init.await
        }
    }

    pub fn weighted_size(&self) -> u64 {
        match self {
            Self::Quick(cache) => cache.weight(),
            Self::None => 0,
        }
    }

    fn report_cache_miss(&self) {
        match self {
            Self::Quick(_) => crate::metrics::ENGINE_CACHE_MISS.inc_by(1),
            Self::None => {}
        }
    }
}

#[derive(Clone)]
pub struct BlockWeighter;

impl quick_cache::Weighter<BlockCacheKey, Bytes> for BlockWeighter {
    fn weight(&self, key: &BlockCacheKey, val: &Bytes) -> u64 {
        block_weight(key, val) as u64
    }
}

pub(crate) fn validate_checksum(file_id: u64, data: &[u8], footer: &Footer) -> Result<()> {
    if data.len() < 4 {
        return Err(table::Error::InvalidChecksum(String::from(
            "data is too short",
        )));
    }
    let checksum_type: ChecksumType = footer.checksum_type.into();
    let checksum = LittleEndian::read_u32(data);
    let content = &data[4..];
    let got_checksum = checksum_type.checksum(content);
    if checksum != got_checksum {
        error!("checksum mismatch";
            "file" => file_id,
            "expect" => checksum,
            "got" => got_checksum,
            "footer" => ?footer,
        );
        return Err(table::Error::InvalidChecksum(format!(
            "{:?} checksum mismatch expect {} got {}",
            checksum_type, checksum, got_checksum
        )));
    }
    Ok(())
}

#[inline]
pub(crate) fn validate_checksum_with_fix(
    data: &[u8],
    footer: &Footer,
    file: &dyn File,
    encryption_key: Option<&EncryptionKey>,
) -> Result<()> {
    match validate_checksum(file.id(), data, footer) {
        Ok(()) => Ok(()),
        Err(err) => {
            if let Some(file_path) = file.path() {
                warn!(
                    "file {} path {:?} failed to validate checksum, encryption_key {:?}, try to remove it from local disk",
                    file.id(),
                    file.path(),
                    encryption_key
                );
                // TODO: add `remove` to `File` to remove `IaFile`.
                // Just remove the sst file in local and download from dfs during next restart.
                std::fs::remove_file(file_path)
                    .table_ctx(file.id(), "sst.validate_checksum.remove_file")?;
            }
            Err(err)
        }
    }
}

const FILE_SUFFIX: &str = ".sst";

pub fn parse_file_id(path: &Path) -> Result<u64> {
    let name = path.file_name().unwrap().to_str().unwrap();
    if !name.ends_with(FILE_SUFFIX) {
        return Err(table::Error::InvalidFileName);
    }
    let digit_part = &name[..name.len() - FILE_SUFFIX.len()];
    if let Ok(id) = u64::from_str_radix(digit_part, 16) {
        return Ok(id);
    }
    Err(table::Error::InvalidFileName)
}

fn parse_prop_data(mut prop_data: &[u8]) -> (&[u8], &[u8], &[u8]) {
    let key_len = LittleEndian::read_u16(prop_data) as usize;
    prop_data = &prop_data[2..];
    let key = &prop_data[..key_len];
    prop_data = &prop_data[key_len..];
    let val_len = LittleEndian::read_u32(prop_data) as usize;
    prop_data = &prop_data[4..];
    let val = &prop_data[..val_len];
    let remained = &prop_data[val_len..];
    (key, val, remained)
}

pub fn id_to_filename(id: u64) -> String {
    format!("{:016x}.sst", id)
}

pub fn new_filename(id: u64, dir: &Path) -> PathBuf {
    dir.join(id_to_filename(id))
}

#[derive(Default)]
pub struct NewSsTableCtx {
    // Input
    pub(crate) tiny_meta: Option<SstTinyMeta>,

    // Output
    pub(crate) footer_and_properties_data: Option<(Footer, Bytes)>,
}

impl NewSsTableCtx {
    pub fn take_footer_and_properties(&mut self) -> Option<(Footer, Bytes)> {
        self.tiny_meta
            .take()
            .and_then(|x| x.get_footer_and_properties().ok())
    }

    pub fn put_footer_and_properties(&mut self, footer: Footer, props_data: Bytes) {
        self.footer_and_properties_data = Some((footer, props_data));
    }
}

#[cfg(test)]
pub(crate) mod test_util {
    use std::sync::{Arc, atomic::Ordering};

    use rand::Rng;

    use crate::table::{
        ChecksumType, InnerKey, NO_COMPRESSION, Value, file,
        sstable::{BlockCache, BlockCacheType, Builder, SsTable},
    };

    pub(crate) static TEST_ID_ALLOC: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(1);

    pub(crate) fn get_test_value(n: usize) -> String {
        format!("{}", n)
    }

    pub(crate) fn generate_key_values(prefix: &str, n: usize) -> Vec<(String, String)> {
        assert!(n <= 10000);
        let mut results = Vec::with_capacity(n);
        for i in 0..n {
            let k = get_test_key(prefix, i);
            let v = get_test_value(i);
            results.push((k, v));
        }
        results
    }

    #[maybe_async::both]
    pub(crate) async fn build_test_table_with_kvs(kvs: &Vec<(String, String)>) -> SsTable {
        let sst_fid = TEST_ID_ALLOC.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        let mut sst_builder = new_table_builder_for_test(sst_fid);
        let meta = 0u8;

        for (k, v) in kvs {
            let value_buf = Value::encode_buf(meta, &[0], 0, v.as_bytes());
            let value = &mut Value::decode(value_buf.as_slice());
            sst_builder.add(InnerKey::from_inner_buf(k.as_bytes()), value, None);
        }

        let mut buf = Vec::with_capacity(sst_builder.estimated_size());
        sst_builder.finish(0, &mut buf);
        let sst_file = file::InMemFile::new(sst_fid, buf.into()).await;

        SsTable::new(Arc::new(sst_file), new_test_cache(), None).unwrap()
    }

    pub(crate) fn new_table_builder_for_test(sst_fid: u64) -> Builder {
        Builder::new(
            sst_fid,
            4096,
            NO_COMPRESSION,
            0,
            ChecksumType::default(),
            None,
        )
    }

    #[maybe_async::both]
    pub(crate) async fn build_test_table_with_prefix(
        prefix: &str,
        n: usize,
    ) -> (SsTable, Vec<(String, String)>) {
        let kvs = generate_key_values(prefix, n);
        (build_test_table_with_kvs(&kvs).await, kvs)
    }

    pub(crate) fn new_test_cache() -> BlockCache {
        BlockCache::new(BlockCacheType::Quick, 1024, 16)
    }

    pub(crate) fn get_test_key(prefix: &str, i: usize) -> String {
        format!("{}{:04}", prefix, i)
    }

    #[maybe_async::both]
    pub(crate) async fn create_sst_table(
        prefix: &str,
        n: usize,
    ) -> (SsTable, Vec<(String, String)>) {
        let kvs = generate_key_values(prefix, n);
        (build_test_table_with_kvs(&kvs).await, kvs)
    }

    // `kvs` must be sorted by key.
    #[maybe_async::both]
    pub(crate) async fn create_multi_version_sst(kvs: &[(String, String)]) -> (SsTable, usize) {
        let sst_fid = TEST_ID_ALLOC.fetch_add(1, Ordering::Relaxed) + 1;
        let mut sst_builder = new_table_builder_for_test(sst_fid);
        let mut all_cnt = kvs.len();
        let meta = 0u8;
        for (k, v) in kvs {
            let val_buf = Value::encode_buf(meta, &[0], 9, v.as_bytes());
            sst_builder.add(
                InnerKey::from_inner_buf(k.as_bytes()),
                &Value::decode(val_buf.as_slice()),
                None,
            );
            let mut r = rand::thread_rng();
            for i in (1..=8).rev() {
                // A lower probability than 1/8 to generate more entries with no old version.
                if r.gen_ratio(1, 10) {
                    let val_str = format!("{}_{}", v, i);
                    let val_buf = Value::encode_buf(meta, &[0], i, val_str.as_bytes());
                    sst_builder.add(
                        InnerKey::from_inner_buf(k.as_bytes()),
                        &Value::decode(val_buf.as_slice()),
                        None,
                    );
                    all_cnt += 1;
                }
            }
        }
        let mut sst_buf = Vec::with_capacity(sst_builder.estimated_size());
        sst_builder.finish(0, &mut sst_buf);
        let sst_file = Arc::new(file::InMemFile::new(sst_fid, sst_buf.into()).await);
        (
            SsTable::new(sst_file, new_test_cache(), None).unwrap(),
            all_cnt,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::iter::Iterator as StdIterator;

    use bytes::BytesMut;
    use futures::executor::block_on;
    use rand::Rng;
    use rstest::rstest;

    use super::{test_util::*, *};
    use crate::{Iterator, next, next_async};

    #[maybe_async::test]
    async fn test_table_iterator() {
        for n in 99..=101 {
            let (t, _) = create_sst_table("key", n).await;
            let mut it = t.new_iterator(false, true);
            let mut count = 0;
            it.rewind().await;
            while it.valid() {
                let k = it.key();
                assert_eq!(k.deref(), get_test_key("key", count).as_bytes());
                let v = it.value();
                assert_eq!(v.get_value(), get_test_value(count).as_bytes());
                count += 1;
                next!(it).await;
            }
        }
    }

    #[maybe_async::test]
    async fn test_point_get() {
        let (t, _) = create_sst_table("key", 8000).await;
        for i in 0..8000 {
            let k = get_test_key("key", i);
            let k_h = farmhash::fingerprint64(k.as_bytes());
            let mut owned_v = vec![];
            let v = t
                .get(
                    InnerKey::from_inner_buf(k.as_bytes()),
                    u64::MAX,
                    k_h,
                    &mut owned_v,
                    1,
                )
                .await;
            assert!(!v.is_empty())
        }
        for i in 8000..10000 {
            let k = get_test_key("key", i);
            let k_h = farmhash::fingerprint64(k.as_bytes());
            let mut owned_v = vec![];
            let v = t
                .get(
                    InnerKey::from_inner_buf(k.as_bytes()),
                    u64::MAX,
                    k_h,
                    &mut owned_v,
                    1,
                )
                .await;
            assert!(v.is_empty())
        }
    }

    #[maybe_async::test]
    async fn test_seek_to_first() {
        let nums = &[99, 100, 101, 199, 200, 250, 9999, 10000];
        for n in nums {
            let (t, _) = create_sst_table("key", *n).await;
            let mut it = t.new_iterator(false, true);
            it.rewind().await;
            assert!(it.valid());
            let v = it.value();
            assert_eq!(v.get_value(), get_test_value(0).as_bytes());
            assert_eq!(v.user_meta(), &[0u8]);
        }
    }

    struct TestData {
        input: &'static str,
        valid: bool,
        output: &'static str,
    }
    impl TestData {
        fn new(input: &'static str, valid: bool, output: &'static str) -> Self {
            Self {
                input,
                valid,
                output,
            }
        }
    }

    #[maybe_async::test]
    async fn test_seek_to_last() {
        let nums = vec![99, 100, 101, 199, 200, 250, 9999, 10000];
        for n in nums {
            let (t, _) = create_sst_table("key", n).await;
            let mut it = t.new_iterator(true, true);
            it.rewind().await;
            assert!(it.valid());
            let v = it.value();
            assert_eq!(v.get_value(), get_test_value(n - 1).as_bytes());
            assert!(!v.is_blob_ref());
            assert_eq!(v.user_meta(), &[0u8]);
            next!(it).await;
            assert!(it.valid());
            let v = it.value();
            assert_eq!(v.get_value(), get_test_value(n - 2).as_bytes());
            assert!(!v.is_blob_ref());
            assert_eq!(v.user_meta(), &[0u8]);
        }
    }

    #[maybe_async::test]
    async fn test_seek_basic() {
        let test_datas: Vec<TestData> = vec![
            TestData::new("abc", true, "k0000"),
            TestData::new("k0100", true, "k0100"),
            TestData::new("k0100b", true, "k0101"),
            TestData::new("k1234", true, "k1234"),
            TestData::new("k1234b", true, "k1235"),
            TestData::new("k9999", true, "k9999"),
            TestData::new("z", false, ""),
        ];
        let (t, _) = create_sst_table("k", 10000).await;
        let mut it = t.new_iterator(false, true);
        for td in test_datas {
            it.seek(InnerKey::from_inner_buf(td.input.as_bytes())).await;
            if !td.valid {
                assert!(!it.valid());
                continue;
            }
            assert!(it.valid());
            assert_eq!(it.key().deref(), td.output.as_bytes());
        }
    }

    #[maybe_async::test]
    async fn test_seek_for_prev() {
        let test_datas: Vec<TestData> = vec![
            TestData::new("abc", false, ""),
            TestData::new("k0100", true, "k0100"),
            TestData::new("k0100b", true, "k0100"),
            TestData::new("k1234", true, "k1234"),
            TestData::new("k1234b", true, "k1234"),
            TestData::new("k9999", true, "k9999"),
            TestData::new("z", true, "k9999"),
        ];
        let (t, _) = create_sst_table("k", 10000).await;
        let mut it = t.new_iterator(true, true);
        for td in test_datas {
            it.seek(InnerKey::from_inner_buf(td.input.as_bytes())).await;
            if !td.valid {
                assert!(!it.valid());
                continue;
            }
            assert!(it.valid());
            assert_eq!(it.key().deref(), td.output.as_bytes());
        }
    }

    #[maybe_async::test]
    async fn test_iterate_from_start() {
        let nums = vec![99, 100, 101, 199, 200, 250, 9999, 10000];
        for n in nums {
            let (t, _) = create_sst_table("key", n).await;
            let mut it = t.new_iterator(false, true);
            let mut count = 0;
            it.rewind().await;
            assert!(it.valid());
            while it.valid() {
                let k = it.key();
                assert_eq!(k.deref(), get_test_key("key", count).as_bytes());
                let v = it.value();
                assert_eq!(v.get_value(), get_test_value(count).as_bytes());
                assert!(!v.is_blob_ref());
                count += 1;
                next!(it).await
            }
        }
    }

    #[maybe_async::test]
    async fn test_iterate_from_end() {
        let nums = vec![99, 100, 101, 199, 200, 250, 9999, 10000];
        for n in nums {
            let (t, _) = create_sst_table("key", n).await;
            let mut it = t.new_iterator(true, true);
            it.seek(InnerKey::from_inner_buf("zzzzzz".as_bytes())).await; // Seek to end, an invalid element.
            assert!(it.valid());
            it.rewind().await;
            for i in (0..n).rev() {
                assert!(it.valid());
                let v = it.value();
                assert_eq!(v.get_value(), get_test_value(i).as_bytes());
                assert!(!v.is_blob_ref());
                next!(it).await;
            }
            next!(it).await;
            assert!(!it.valid());
        }
    }

    #[maybe_async::test]
    async fn test_table() {
        let (t, _) = create_sst_table("key", 10000).await;
        let mut it = t.new_iterator(false, true);
        let mut kid = 1010_usize;
        let seek = get_test_key("key", kid);
        it.seek(InnerKey::from_inner_buf(seek.as_bytes())).await;
        while it.valid() {
            assert_eq!(it.key().deref(), get_test_key("key", kid).as_bytes());
            kid += 1;
            next!(it).await;
        }
        assert_eq!(kid, 10000);

        it.seek(InnerKey::from_inner_buf(
            get_test_key("key", 99999).as_bytes(),
        ))
        .await;
        assert!(!it.valid());

        it.seek(InnerKey::from_inner_buf(get_test_key("kex", 0).as_bytes()))
            .await;
        assert!(it.valid());
        assert_eq!(it.key().deref(), get_test_key("key", 0).as_bytes());
    }

    #[maybe_async::test]
    async fn test_iterate_back_and_forth() {
        let (t, _) = create_sst_table("key", 10000).await;
        let seek = get_test_key("key", 1010);
        let mut it = t.new_iterator(false, true);
        it.seek(InnerKey::from_inner_buf(seek.as_bytes())).await;
        assert!(it.valid());
        assert_eq!(it.key().deref(), seek.as_bytes());

        it.set_reversed(true);
        next!(it).await;
        next!(it).await;
        assert!(it.valid());
        assert_eq!(it.key().deref(), get_test_key("key", 1008).as_bytes());

        it.set_reversed(false);
        next!(it).await;
        next!(it).await;
        assert_eq!(it.valid(), true);
        assert_eq!(it.key().deref(), get_test_key("key", 1010).as_bytes());

        it.seek(InnerKey::from_inner_buf(
            get_test_key("key", 2000).as_bytes(),
        ))
        .await;
        assert_eq!(it.valid(), true);
        assert_eq!(it.key().deref(), get_test_key("key", 2000).as_bytes());

        it.set_reversed(true);
        next!(it).await;
        assert_eq!(it.valid(), true);
        assert_eq!(it.key().deref(), get_test_key("key", 1999).as_bytes());

        it.set_reversed(false);
        it.rewind().await;
        assert_eq!(it.key().deref(), get_test_key("key", 0).as_bytes());
    }

    #[maybe_async::test]
    async fn test_iterate_multi_version() {
        let num = 4000;
        let kvs = generate_key_values("key", num);
        let (t, all_cnt) = create_multi_version_sst(&kvs).await;
        let mut it = t.new_iterator(false, true);
        let mut it_cnt = 0;
        let mut last_key = BytesMut::new();
        it.rewind().await;
        while it.valid() {
            if !last_key.is_empty() {
                assert!(last_key < it.key().deref());
            }
            last_key.truncate(0);
            last_key.extend_from_slice(it.key().deref());
            it_cnt += 1;
            while next_version!(it).await {
                it_cnt += 1;
            }
            next!(it).await;
        }
        assert_eq!(it_cnt, all_cnt);
        let mut r = rand::thread_rng();
        for _ in 0..1000 {
            let k = get_test_key("key", r.gen_range(0..num));
            let ver = 5 + r.gen_range(0..5) as u64;
            let k_h = farmhash::fingerprint64(k.as_bytes());
            let mut owned_v = vec![];
            let val = t
                .get(
                    InnerKey::from_inner_buf(k.as_bytes()),
                    ver,
                    k_h,
                    &mut owned_v,
                    1,
                )
                .await;
            if !val.is_empty() {
                assert!(val.version <= ver);
            }
        }
        let mut rev_it = t.new_iterator(true, true);
        last_key.truncate(0);
        rev_it.rewind().await;
        while rev_it.valid() {
            if !last_key.is_empty() {
                assert!(last_key > rev_it.key().deref());
            }
            last_key.truncate(0);
            last_key.extend_from_slice(rev_it.key().deref());
            next!(rev_it).await;
        }
        for _ in 0..1000 {
            let k = get_test_key("key", r.gen_range(0..num));
            // reverse iterator never seek to the same key with smaller version.
            rev_it.seek(InnerKey::from_inner_buf(k.as_bytes())).await;
            if !rev_it.valid() {
                continue;
            }
            assert_eq!(rev_it.value().version, 9);
            assert!(rev_it.key().deref() <= k.as_bytes());
        }
    }

    #[maybe_async::test]
    async fn test_uni_iterator() {
        let (t, _) = create_sst_table("key", 10000).await;
        {
            let mut it = t.new_iterator(false, true);
            let mut cnt = 0;
            it.rewind().await;
            while it.valid() {
                let v = it.value();
                assert_eq!(v.get_value(), get_test_value(cnt).as_bytes());
                assert!(!v.is_blob_ref());
                cnt += 1;
                next!(it).await;
            }
            assert_eq!(cnt, 10000);
        }
        {
            let mut it = t.new_iterator(true, true);
            let mut cnt = 0;
            it.rewind().await;
            while it.valid() {
                let v = it.value();
                assert_eq!(v.get_value(), get_test_value(10000 - 1 - cnt).as_bytes());
                assert!(!v.is_blob_ref());
                cnt += 1;
                next!(it).await;
            }
        }
    }

    // For:
    // https://github.com/tidbcloud/cloud-storage-engine/issues/1957
    // https://github.com/tidbcloud/cloud-storage-engine/issues/2062
    #[rstest]
    #[case::n1(1)]
    #[case::n10(10)]
    fn test_reset_old_block_iter(#[case] n: usize) {
        test_reset_old_block_iter_impl(n);
        block_on(test_reset_old_block_iter_impl_async(n));
    }

    #[maybe_async::both]
    async fn test_reset_old_block_iter_impl(n: usize) {
        let kvs = generate_key_values("key", n);
        let (t, _) = create_multi_version_sst(&kvs).await;
        let mut it = t.new_iterator(false, true);

        for (k, _) in kvs {
            it.seek(InnerKey::from_inner_buf(k.as_bytes())).await;
            assert!(it.valid());
            next_version!(it).await;
            next_version!(it).await; // Reach the second version in old block.

            it.seek(InnerKey::from_inner_buf(k.as_bytes())).await;
            assert!(it.valid());
            // If old block iterator is not reset, it will point to the second version and
            // panic for version not match.
            next_version!(it).await;
        }
    }

    #[tokio::test]
    async fn test_is_next_version_sync_on_not_valid() {
        // No old version.
        {
            let (t, _) = create_sst_table_async("key", 10).await;
            let mut it = t.new_iterator(false, true);
            it.rewind_async().await;
            assert!(it.is_next_version_sync());
            assert!(!next_version_async!(it));
        }

        // No more version.
        {
            let kvs = generate_key_values("key", 10);
            let (t, _) = create_multi_version_sst_async(&kvs).await;
            let mut it = t.new_iterator(false, true);
            it.rewind_async().await;
            while next_version_async!(it) {}
            assert!(it.is_next_version_sync());
            assert!(!next_version_async!(it));
        }
    }

    #[bench]
    fn bench_decode_filter(b: &mut test::Bencher) {
        let (t, _) = create_sst_table("key", 10000);
        let data = t.read_filter_data_from_file().unwrap();
        b.iter(|| {
            test::black_box(t.decode_filter(&data).unwrap());
        });
    }

    #[maybe_async::test]
    async fn test_has_overlap() {
        // k0000 ~ k0009
        let (t, _) = create_sst_table("k", 10).await;

        // lower_bound, upper_bound, upper_inclusive, expected
        let cases: Vec<(&'static str, &'static str, bool, bool)> = vec![
            ("k0", "k00", false, false),
            ("k000", "k0000", false, false),
            ("k000", "k0000", true, true),
            ("k0000", "k00000", false, true),
            ("k00000", "k00001", false, false), // inner no overlap.
            ("k0001", "k00010", false, true),
            ("k00010", "k0002", false, false), // inner no overlap.
            ("k00010", "k0002", true, true),
            ("k0000", "k0009", false, true),
            ("k00090", "k00091", true, false),
        ];

        for (lower, upper, upper_inclusive, expected) in cases {
            let lower_bound = InnerKey::from_inner_buf(lower.as_bytes());
            let upper_bound = InnerKey::from_inner_buf(upper.as_bytes());
            let data_bound = DataBound {
                lower_bound,
                upper_bound,
                upper_inclusive,
            };
            let has_overlap = t.has_overlap(data_bound).await;
            assert_eq!(
                has_overlap, expected,
                "case: {} {} {} {}",
                lower, upper, upper_inclusive, expected
            );
        }
    }

    #[tokio::test]
    async fn test_has_overlap_loose() {
        // k0000 ~ k0009
        let (t_sync, _) = create_sst_table("k", 10);
        let (t_async, _) = create_sst_table_async("k", 10).await;

        // lower_bound, upper_bound, upper_inclusive, sync_expected, async_expected
        let cases: Vec<(&'static str, &'static str, bool, bool, bool)> = vec![
            ("k0", "k00", false, false, false),
            ("k000", "k0000", false, false, false),
            ("k000", "k0000", true, true, true),
            ("k0000", "k00000", false, true, true),
            ("k00000", "k00001", false, false, true), // inner no overlap.
            ("k0001", "k00010", false, true, true),
            ("k00010", "k0002", false, false, true), // inner no overlap.
            ("k00010", "k0002", true, true, true),
            ("k0000", "k0009", false, true, true),
            ("k00090", "k00091", true, false, false),
        ];

        for (lower, upper, upper_inclusive, sync_expected, async_expected) in cases {
            let lower_bound = InnerKey::from_inner_buf(lower.as_bytes());
            let upper_bound = InnerKey::from_inner_buf(upper.as_bytes());
            let data_bound = DataBound {
                lower_bound,
                upper_bound,
                upper_inclusive,
            };

            let has_overlap = t_sync.has_overlap_loose(data_bound);
            assert_eq!(
                has_overlap, sync_expected,
                "sync case: {} {} {} {}",
                lower, upper, upper_inclusive, sync_expected
            );

            let has_overlap = t_async.has_overlap_loose(data_bound);
            assert_eq!(
                has_overlap, async_expected,
                "async case: {} {} {} {}",
                lower, upper, upper_inclusive, async_expected
            );
        }
    }

    #[test]
    fn test_get_meta_data() {
        let mut sst_builder = new_table_builder_for_test(100);
        for (k, v) in generate_key_values("k", 10) {
            let value_buf = Value::encode_buf(0u8, &[0], 0, v.as_bytes());
            let value = &mut Value::decode(value_buf.as_slice());
            sst_builder.add(InnerKey::from_inner_buf(k.as_bytes()), value, None);
        }

        let mut buf = Vec::with_capacity(sst_builder.estimated_size());
        sst_builder.finish(0, &mut buf);
        let buf = Bytes::from(buf);
        let sst_file = file::InMemFile::new(100, buf.clone());
        let sst = SsTable::new(Arc::new(sst_file), new_test_cache(), None).unwrap();

        let meta_data = SsTable::get_meta_data(&buf).unwrap();
        assert_eq!(meta_data.as_ref(), &buf[sst.meta_offset() as usize..]);

        SsTable::get_meta_data(&[]).unwrap_err();
        SsTable::get_meta_data(&[0xff; SsTable::footer_size()]).unwrap_err();
    }
}
