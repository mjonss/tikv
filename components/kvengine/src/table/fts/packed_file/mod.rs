// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::Debug,
    sync::{Arc, RwLock},
};

use anyhow::{Result, anyhow, bail};
use bytes::{Buf, Bytes};
use hexhex::hex;
use kvenginepb::{FtsPackedFileInfo, fts as ftspb};
use protobuf::Message;
use xorf::{BinaryFuse8, Filter};

use super::iter::{CommonPk, IntPk, PkType};
use crate::{
    codecutil::{BytesExt, next_aligned_offset},
    table::{
        BoundedDataSet, ChecksumType, DataBound, InnerKey,
        file::{File, FileMmapGuard, TtlCache},
        fts::{CacheKeyRef, FtsCache, FtsCacheValue, dedicated_file::BytesDirRO},
    },
};

mod builder;
mod iter;

#[cfg(any(test, feature = "testexport"))]
#[cfg_attr(feature = "testexport", allow(unused))]
mod test;

pub use builder::*;
pub use iter::*;

/// Magic number for FTS PackedFile format
pub const FTS_PACKED_FILE_MAGIC: u32 = 0xFE75F11E;

/// Format version for FTS PackedFile
pub const FTS_PACKED_FILE_FORMAT_V1: u8 = 0x1;

/// Size of the serialized footer in bytes
pub const FTS_PACKED_FILE_FOOTER_SIZE: usize = 32;

/// Footer has a fixed size and is always located at the end of the file.
/// Footer size cannot be changed.
/// Format:
/// - 1 byte: format version
/// - 1 byte: checksum type
/// - 6 bytes: reserved
/// - 4 bytes: checksum footer
///   > Calculated as the checksum of the whole footer (with checksum
///   > footer set to 0)
/// - 4 bytes: checksum (remaining meta)
/// - 4 bytes: index block offset
/// - 4 bytes: lp (LogicalPartition) filter block offset
/// - 4 bytes: property block offset
/// - 4 bytes: magic number
#[repr(C)]
#[derive(Default, Clone, Copy, Debug)]
pub struct PackedFileFooter {
    pub format: u8,
    pub checksum_type: ChecksumType,
    pub checksum_other_meta: u32,
    pub index_block_offset: u32,
    pub lp_filter_block_offset: u32,
    pub prop_offset: u32,
    pub magic: u32,
}

impl PackedFileFooter {
    /// Get the size of the footer
    pub const fn footer_size() -> usize {
        FTS_PACKED_FILE_FOOTER_SIZE
    }

    /// Create a new footer with default values
    pub fn new() -> Self {
        Self {
            format: FTS_PACKED_FILE_FORMAT_V1,
            checksum_type: ChecksumType::None,
            checksum_other_meta: 0,
            index_block_offset: 0,
            lp_filter_block_offset: 0,
            prop_offset: 0,
            magic: FTS_PACKED_FILE_MAGIC,
        }
    }

    /// Unmarshal footer from bytes
    pub fn unmarshal(mut data: &[u8]) -> Result<Self> {
        if data.len() != FTS_PACKED_FILE_FOOTER_SIZE {
            bail!(
                "Invalid footer size, expected {:#x}, got {:#x}",
                FTS_PACKED_FILE_FOOTER_SIZE,
                data.len()
            );
        }
        let mut copied_data = [0u8; FTS_PACKED_FILE_FOOTER_SIZE];
        copied_data.copy_from_slice(data);

        let format = data.get_u8();
        let checksum_type_value = data.get_u8();
        let checksum_type = ChecksumType::from(checksum_type_value);
        data.advance(6);
        let checksum_footer = data.get_u32_le();
        let checksum_other_meta = data.get_u32_le();
        let index_block_offset = data.get_u32_le();
        let lp_filter_block_offset = data.get_u32_le();
        let prop_offset = data.get_u32_le();
        let magic = data.get_u32_le();
        if magic != FTS_PACKED_FILE_MAGIC {
            bail!(
                "Invalid magic number, expected {:#x}, got {:#x}",
                FTS_PACKED_FILE_MAGIC,
                magic
            );
        }
        if format != FTS_PACKED_FILE_FORMAT_V1 {
            bail!(
                "Unsupported format version, expected {:#x}, got {:#x}",
                FTS_PACKED_FILE_FORMAT_V1,
                format
            );
        }

        {
            // Verify footer's checksum
            copied_data[8..12].fill(0);
            let checksum_footer_actual = checksum_type.checksum(&copied_data);
            if checksum_footer != checksum_footer_actual {
                copied_data[8..12].copy_from_slice(&checksum_footer.to_le_bytes());
                bail!(
                    "FtsPackedFile footer checksum mismatch, actual {:#08x}, expect {:#08x}, footer data: {}, ChecksumType={:?}",
                    checksum_footer_actual,
                    checksum_footer,
                    hex(&copied_data),
                    checksum_type,
                );
            }
        }

        Ok(Self {
            format,
            checksum_type,
            checksum_other_meta,
            index_block_offset,
            lp_filter_block_offset,
            prop_offset,
            magic,
        })
    }

    /// Marshal footer to bytes
    pub fn marshal<W: std::io::Write>(&self, mut w: W) -> Result<usize> {
        use bytes::BufMut;

        let mut footer = [0u8; FTS_PACKED_FILE_FOOTER_SIZE];
        let mut data = &mut footer[..];
        data.put_u8(self.format);
        data.put_u8(self.checksum_type.value());
        data.put_slice(&[0; 6]); // Reserved bytes
        data.put_slice(&[0; 4]); // Placeholder for checksum footer
        data.put_u32_le(self.checksum_other_meta);
        data.put_u32_le(self.index_block_offset);
        data.put_u32_le(self.lp_filter_block_offset);
        data.put_u32_le(self.prop_offset);
        data.put_u32_le(self.magic);

        let checksum_footer = self.checksum_type.checksum(&footer);
        footer[8..12].copy_from_slice(&checksum_footer.to_le_bytes());

        w.write_all(&footer)?;

        Ok(FTS_PACKED_FILE_FOOTER_SIZE)
    }

    /// Returns the starting offset of the metadata blocks in the PackedFile
    /// indicated by this footer.
    ///
    /// As Index Block is the first metadata block, its offset is returned.
    pub fn metadata_offset(&self) -> u64 {
        self.index_block_offset as u64
    }
}

/// A FtsPackedFile in memory. Only minimal metadata is kept in memory.
/// The main data is accessed through mmap and acceleration data is cached via
/// TtlCache.
///
/// FtsPackedFile layout:
/// - Data blocks:     Each data block contains several LP entries
///   > Data block start offset is always 8-byte aligned
/// - Index block:     For quickly seeking a data block
/// - LP filter block: For checking whether LPKey must not exist
/// - Property block:  Some additional properties (in Protobuf)
/// - Footer:          Metadata
#[derive(Clone)]
pub struct PackedFile(Arc<PackedFileCore>);

/// Core implementation of PackedFile
struct PackedFileCore {
    file: Arc<dyn File>,
    cache: FtsCache,

    index_block: TtlCache<ftspb::PackedFileIndexBlock>,
    lp_filter: TtlCache<BinaryFuse8>,
    props: ftspb::PackedFilePropBlock,
    footer: PackedFileFooter,
    data_block_checked: RwLock<Vec<bool>>,
}

impl PackedFile {
    /// Create a new PackedFile from a file handle.
    pub fn new(file: Arc<dyn File>, cache: FtsCache) -> Result<PackedFile> {
        let footer = PackedFile::load_footer(file.as_ref())?;
        let props = PackedFile::load_props(file.as_ref(), &footer)?;
        Ok(Self(Arc::new(PackedFileCore {
            file,
            cache,
            index_block: TtlCache::default(),
            lp_filter: TtlCache::default(),
            props,
            footer,
            data_block_checked: RwLock::new(vec![]),
        })))
    }

    /// Calculate segment offsets for an FTS packed file based on data blocks.
    /// This function reads the index block and calculates segment boundaries
    /// based on the standard segment size. It is primarily used when preparing
    /// IA metadata.
    pub fn generate_ia_segment_boundaries(file: &dyn File, segment_size: u64) -> Result<Vec<u64>> {
        let footer = Self::load_footer(file)?;
        let index_block = Self::load_index_block(file, &footer)?;

        let mut segment_offsets = vec![0]; // Always start with offset 0
        let mut current_segment_start = 0u64;

        // Iterate through data block offsets and create segments when accumulated size
        // reaches the standard IA segment size
        for &block_offset in &index_block.data_block_offsets {
            let block_offset = block_offset as u64;
            // If this block would make the current segment exceed the segment size,
            // start a new segment
            if block_offset >= current_segment_start + segment_size {
                segment_offsets.push(block_offset);
                current_segment_start = block_offset;
            }
        }

        // Make sure the last segment boundary is the last data block offset to seal
        // a complete segment
        if let Some(&last_block_offset) = index_block.data_block_offsets.last() {
            let last_block_offset = last_block_offset as u64;
            if segment_offsets.last().copied().unwrap_or(0) < last_block_offset {
                segment_offsets.push(last_block_offset);
            }
        }

        Ok(segment_offsets)
    }

    /// Load the footer from the packed file.
    fn load_footer(file: &dyn File) -> Result<PackedFileFooter> {
        let footer_data = file.read_footer(FTS_PACKED_FILE_FOOTER_SIZE)?;
        let footer = PackedFileFooter::unmarshal(footer_data.as_ref())?;
        Self::verify_meta_checksum(file, &footer)?;
        Self::validate_footer_offsets(file, &footer)?;
        Ok(footer)
    }

    /// Load the properties block from the packed file.
    fn load_props(
        file: &dyn File,
        footer: &PackedFileFooter,
    ) -> Result<ftspb::PackedFilePropBlock> {
        let prop_offset = footer.prop_offset as u64;
        let prop_len = file
            .size()
            .checked_sub(FTS_PACKED_FILE_FOOTER_SIZE as u64 + prop_offset)
            .ok_or_else(|| {
                anyhow!(
                    "Property block offset {} is out of bounds for file size {}",
                    prop_offset,
                    file.size()
                )
            })? as usize;
        let mut prop = ftspb::PackedFilePropBlock::new();
        if prop_len > 0 {
            let data = file.read_table_meta(prop_offset, prop_len)?;
            prop.merge_from_bytes(&data)?;
        }
        Ok(prop)
    }

    /// Verify the metadata checksum for the packed file.
    fn verify_meta_checksum(file: &dyn File, footer: &PackedFileFooter) -> Result<()> {
        let offset = footer.metadata_offset();
        let file_size = file.size();
        if file_size < offset + FTS_PACKED_FILE_FOOTER_SIZE as u64 {
            bail!(
                "PackedFile size {} smaller than metadata offset {}",
                file_size,
                offset
            );
        }
        let meta_size = (file_size - offset) as usize;
        let size = meta_size
            .checked_sub(FTS_PACKED_FILE_FOOTER_SIZE)
            .ok_or_else(|| anyhow!("metadata section too small for footer"))?;
        let rest_meta_data = file.read_table_meta(offset, size)?;
        let checksum_actual = footer.checksum_type.checksum(&rest_meta_data);
        if checksum_actual != footer.checksum_other_meta {
            bail!(
                "FtsPackedFile meta checksum mismatch, actual {:#08x}, expect {:#08x}, ChecksumType={:?}",
                checksum_actual,
                footer.checksum_other_meta,
                footer.checksum_type
            );
        }
        Ok(())
    }

    /// Load the index block from the packed file.
    fn load_index_block(
        file: &dyn File,
        footer: &PackedFileFooter,
    ) -> Result<ftspb::PackedFileIndexBlock> {
        let offset = footer.index_block_offset as u64;
        let length = footer
            .lp_filter_block_offset
            .checked_sub(footer.index_block_offset)
            .ok_or_else(|| {
                anyhow!(
                    "Index block broken: lp_filter_block_offset {:#x} smaller than index_block_offset {:#x}",
                    footer.lp_filter_block_offset,
                    footer.index_block_offset
                )
            })?;
        let data = file.read_table_meta(offset, length as usize)?;

        let mut index_block = ftspb::PackedFileIndexBlock::new();
        index_block.merge_from_bytes(&data)?;

        if index_block.data_block_offsets.is_empty() {
            bail!("Index block broken: data_block_offsets must not be empty");
        }
        if index_block.data_block_start_keys.len() + 1 != index_block.data_block_offsets.len() {
            bail!(
                "Index block broken: data_block_start_keys.len={} but data_block_offsets.len={}",
                index_block.data_block_start_keys.len(),
                index_block.data_block_offsets.len()
            );
        }

        Ok(index_block)
    }

    /// Load the LP filter block from the packed file.
    fn load_lp_filter(file: &dyn File, footer: &PackedFileFooter) -> Result<BinaryFuse8> {
        let offset = footer.lp_filter_block_offset as u64;
        let length = footer
            .prop_offset
            .checked_sub(footer.lp_filter_block_offset)
            .ok_or_else(|| {
                anyhow!(
                    "LP filter block broken: prop_offset {:#x} smaller than lp_filter_block_offset {:#x}",
                    footer.prop_offset,
                    footer.lp_filter_block_offset
                )
            })?;
        let data = file.read_table_meta(offset, length as usize)?;

        let filter =
            BinaryFuse8::try_from_bytes(&data).map_err(|e| anyhow!("Bad LPKey filter: {}", e))?;
        Ok(filter)
    }

    fn validate_footer_offsets(file: &dyn File, footer: &PackedFileFooter) -> Result<()> {
        let file_size = file.size();
        if file_size < FTS_PACKED_FILE_FOOTER_SIZE as u64 {
            bail!(
                "PackedFile size {} smaller than footer size {}",
                file_size,
                FTS_PACKED_FILE_FOOTER_SIZE
            );
        }
        let footer_start = file_size - FTS_PACKED_FILE_FOOTER_SIZE as u64;

        let index_off = footer.index_block_offset as u64;
        let filter_off = footer.lp_filter_block_offset as u64;
        let prop_off = footer.prop_offset as u64;

        if !(index_off <= filter_off && filter_off <= prop_off) {
            bail!(
                "PackedFile footer offsets not monotonic: index_block_offset={:#x}, lp_filter_block_offset={:#x}, prop_offset={:#x}",
                index_off,
                filter_off,
                prop_off
            );
        }
        if prop_off > footer_start {
            bail!(
                "PackedFile footer offsets out of bounds: prop_offset={:#x} > footer_start={:#x}",
                prop_off,
                footer_start
            );
        }

        Ok(())
    }
}

impl BoundedDataSet for PackedFile {
    fn data_bound(&self) -> DataBound<'_> {
        DataBound::new(
            InnerKey::from_inner_buf(self.props().get_smallest_key()),
            InnerKey::from_inner_buf(self.props().get_biggest_key()),
            true,
        )
    }
}

impl Debug for PackedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PackedFile")
            .field("file_id", &self.0.file.id())
            .field("props", &self.0.props)
            .finish()
    }
}

impl PackedFile {
    /// Get the file ID of the underlying file
    #[inline]
    pub fn id(&self) -> u64 {
        self.0.file.id()
    }

    /// Access the underlying file handle.
    #[inline]
    pub fn file(&self) -> &Arc<dyn File> {
        &self.0.file
    }

    /// Get or load the index block.
    /// Index block can be used to quickly find which data block contains the
    /// given logical partition key.
    #[inline]
    fn get_index_block(&self) -> Result<Arc<ftspb::PackedFileIndexBlock>> {
        self.0
            .index_block
            .get(|| PackedFile::load_index_block(self.0.file.as_ref(), &self.0.footer))
    }

    /// Get or load the LogicalPartition filter.
    /// LogicalPartition filter can be used to quickly check whether this packed
    /// file contains a logical partition key.
    #[inline]
    fn get_lp_filter(&self) -> Result<Arc<BinaryFuse8>> {
        self.0
            .lp_filter
            .get(|| PackedFile::load_lp_filter(self.0.file.as_ref(), &self.0.footer))
    }

    /// Returns the property block.
    #[inline]
    pub fn props(&self) -> &ftspb::PackedFilePropBlock {
        &self.0.props
    }

    /// Builds a protobuf info message for a packed file based on the provided
    /// metadata.
    pub fn info(
        file_id: u64,
        meta_offset: u32,
        mut props: ftspb::PackedFilePropBlock,
    ) -> FtsPackedFileInfo {
        kvenginepb::FtsPackedFileInfo {
            id: file_id,
            meta_offset,
            smallest: props.take_smallest_key(),
            biggest: props.take_biggest_key(),
            snap_version: props.get_snap_version(),

            // We use explicit fields to avoid missing new fields in the future.
            unknown_fields: Default::default(),
            cached_size: Default::default(),
        }
    }

    /// Builds a protobuf info message describing this packed file.
    pub fn build_info(&self) -> FtsPackedFileInfo {
        Self::info(
            self.id(),
            self.0.footer.metadata_offset() as u32,
            self.0.props.clone(),
        )
    }

    /// Return the number of data blocks.
    #[inline]
    pub fn n_data_blocks(&self) -> Result<usize> {
        Ok(self.get_index_block()?.data_block_start_keys.len())
    }

    /// Check if this file contains data for the specified table.
    /// Uses the precomputed table_ids from property block for O(log n) lookup.
    #[inline]
    pub fn has_table(&self, table_id: i64) -> bool {
        // Use binary search on sorted table_ids for efficient lookup
        self.props().table_ids.binary_search(&table_id).is_ok()
    }

    /// Gets a data block at the given index (with caching).
    async fn cached_data_block_at(
        &self,
        data_block_idx: usize,
    ) -> Result<Arc<PackedFileDataBlockAccessor>> {
        self.0
            .cache
            .get_async(
                CacheKeyRef::PackedDBlock {
                    file_id: self.id(),
                    block_idx: data_block_idx as u64,
                },
                async { self.load_data_block_at(data_block_idx).await },
            )
            .await
    }

    /// Loads a data block with checksum verification.
    /// The loaded block is mmap'd and checksummed on first access.
    async fn load_data_block_at(
        &self,
        data_block_idx: usize,
    ) -> Result<FtsCacheValue<PackedFileDataBlockAccessor>> {
        let index_block = self.get_index_block()?;

        let offset = index_block.data_block_offsets[data_block_idx];
        let len = index_block.data_block_offsets[data_block_idx + 1] - offset;
        let (data, guard) = self.0.file.mmap_range(offset as u64, len as usize).await?;

        let data_block = Arc::new(PackedFileDataBlockAccessor::new(
            self.id(),
            self.0.cache.clone(),
            data,
            guard,
        )?);

        // Verify checksum if not already checked
        let need_checksum = {
            let data_block_checked = self.0.data_block_checked.read().unwrap();
            data_block_checked.is_empty() || !data_block_checked[data_block_idx]
        };
        if need_checksum {
            let checksum_type = self.0.footer.checksum_type;
            data_block.verify_checksum(checksum_type)?;
            {
                let mut data_block_checked = self.0.data_block_checked.write().unwrap();
                if data_block_checked.is_empty() {
                    data_block_checked.resize(index_block.data_block_offsets.len(), false);
                }
                data_block_checked[data_block_idx] = true;
            }
        }

        let source_guard = data_block.source_guard.clone();
        Ok(FtsCacheValue::with_mmap_guard(data_block, source_guard))
    }

    /// Get a logical partition for future access.
    /// Note: This function will download the data block if it is not in IA.
    pub async fn cached_get_lp(&self, lp_key: &[u8]) -> Result<Option<Arc<EPackedFileLp>>> {
        self.0
            .cache
            .get_async_opt(
                CacheKeyRef::PackedLp {
                    file_id: self.id(),
                    lp_key,
                },
                async { self.get_lp(lp_key).await },
            )
            .await
    }

    async fn get_lp(&self, lp_key: &[u8]) -> Result<Option<FtsCacheValue<EPackedFileLp>>> {
        let lp_filter = self.get_lp_filter()?;
        let key_hash = farmhash::fingerprint64(lp_key);
        if !lp_filter.contains(&key_hash) {
            return Ok(None);
        }

        // Navigate the data block idx from index block.
        let index_block = self.get_index_block()?;
        let data_block_idx = self.find_data_block_index(&index_block, lp_key);
        let data_block_idx = match data_block_idx {
            Some(idx) => idx,
            None => return Ok(None),
        };

        let data_block = self.cached_data_block_at(data_block_idx).await?;
        let Some(lp) = data_block.find_lp(lp_key)? else {
            return Ok(None);
        };
        Ok(Some(FtsCacheValue::with_mmap_guard(
            Arc::new(lp),
            data_block.source_guard.clone(),
        )))
    }

    /// Find the data block index that may contain the given logical partition
    /// key. It returns None when data block must not exist (there is no
    /// data block, or the key is smaller than the key of the first data
    /// block).
    ///
    /// If a data block is returned, you should further check whether the data
    /// block contains the given logical partition key by yourself.
    #[inline]
    fn find_data_block_index(
        &self,
        index_block: &ftspb::PackedFileIndexBlock,
        lp_key: &[u8],
    ) -> Option<usize> {
        let start_keys = index_block.get_data_block_start_keys();
        let pos = start_keys.partition_point(|key| key.as_slice() <= lp_key);
        if pos == 0 { None } else { Some(pos - 1) }
    }
}

/// A zero-copy accessor for reading a data block in PackedFile.
/// Creating the accessor is designed to be very cheap.
///
/// DataBlock layout:
/// ...    (Align to 8 bytes)
/// ...    Entries
///        > Each entry is a logical partition
///        > Each entry is aligned to 8 bytes
///        > Entries are ordered by LPKey
/// ...    (Align to 8 bytes)
/// [u32]  Entry Offsets (n=Entries Count+1)
/// u32    (Optional) Padding (only when Entries Count is even)
/// u32    Entries Count
/// u32    Checksum (start from 0 byte including paddings)
pub(in crate::table::fts) struct PackedFileDataBlockAccessor {
    file_id: u64,
    cache: FtsCache,

    data_block: Bytes,
    source_guard: FileMmapGuard,

    /// It contains n_entries+1 elements, where the last element is the end
    /// offset.
    ///
    /// 'static because comes from `data_block`.
    /// MUST NOT expose to outside.
    unsafe_offsets: &'static [u32],
    n_entries: u32,
    checksum: u32,
}

impl PackedFileDataBlockAccessor {
    fn new(
        file_id: u64,
        cache: FtsCache,
        mut data_block: Bytes,
        source_guard: FileMmapGuard,
    ) -> Result<Self> {
        let data_block_clone = data_block.clone();
        let checksum = data_block.try_get_last_u32_le()?;
        let n_entries = data_block.try_get_last_u32_le()?;
        // Data block metadata contains a u32 padding before `n_entries` when
        // n_entries is even, so the data block ends at an 8-byte aligned offset.
        if n_entries % 2 == 0 {
            let padding = data_block.try_get_last_u32_le()?;
            if padding != 0 {
                bail!(
                    "Broken data block, expected 0 padding before n_entries, got {}",
                    padding
                );
            }
        }
        let offsets_data = data_block.try_get_last((n_entries + 1) as usize * 4)?;
        let offsets = offsets_data.try_as_slice::<u32>()?;
        Ok(PackedFileDataBlockAccessor {
            file_id,
            cache,
            data_block: data_block_clone,
            source_guard,

            unsafe_offsets: unsafe { crate::util::extend_lifetime(offsets) },
            n_entries,
            checksum,
        })
    }

    fn verify_checksum(&self, checksum_type: ChecksumType) -> Result<()> {
        let content = &self.data_block.as_ref()[..self.data_block.len() - 4];
        let checksum_actual = checksum_type.checksum(content);
        if self.checksum != checksum_actual {
            bail!(
                "FtsPackedFile data block checksum mismatch, actual {:#08x}, expect {:#08x}, ChecksumType={:?}",
                checksum_actual,
                self.checksum,
                checksum_type
            );
        }
        Ok(())
    }

    fn entry_data_at(&self, i: usize) -> Result<Bytes> {
        let offset = self.unsafe_offsets[i] as usize;
        let end_offset = self.unsafe_offsets[i + 1] as usize;
        let entry_data = self.data_block.try_slice(offset..end_offset)?;
        entry_data.check_aligned::<u64>()?;
        Ok(entry_data)
    }

    /// Extracts the LPKey from the entry data without constructing
    /// PackedFileLp.
    fn fast_extract_lp_key(entry_data: &Bytes) -> Result<Bytes> {
        let mut entry_data = entry_data.clone();
        let key_len = entry_data.try_get_u16_le()?;
        if key_len == 0 {
            bail!("Broken entry data, unexpected empty key length");
        }
        entry_data.try_get_first(key_len as usize)
    }

    /// Gets the logical partition at the given index within this data block.
    #[inline]
    fn lp_at(&self, i: usize) -> Result<EPackedFileLp> {
        let entry_data = self.entry_data_at(i)?;
        EPackedFileLp::new(
            self.file_id,
            self.cache.clone(),
            entry_data,
            self.source_guard.clone(),
        )
    }

    fn find_lp(&self, lp_key: &[u8]) -> Result<Option<EPackedFileLp>> {
        let n_entries = self.n_entries as usize;
        let pos = crate::table::try_search(n_entries, |i| {
            let entry_data = self.entry_data_at(i)?;
            Self::fast_extract_lp_key(&entry_data).map(|key| key.as_ref() >= lp_key)
        })?;
        if pos == n_entries {
            return Ok(None);
        }
        let entry_data = self.entry_data_at(pos)?;
        let lp_key_at_pos = Self::fast_extract_lp_key(&entry_data)?;
        if lp_key_at_pos.as_ref() != lp_key {
            return Ok(None);
        }
        Ok(Some(self.lp_at(pos)?))
    }
}

/// Reading from a logical partition in PackedFile.
/// This struct is supposed to be cached.
///
/// Entry data layout:
/// u16    LogicalPartitionKey Length
/// ...    LogicalPartitionKey Bytes
/// u32    Props Length
/// ...    Props Protobuf (PackedFileLpProps)
///        > Contains: n_pk, is_int_handle, table_id
/// ...    (Align to 8 bytes)
/// [u64]  Versions (For the same PK, they are ordered from largest to smallest)
/// [u8]   Delete Marks
/// ...    (Align to 4 bytes)
/// [u32]  Common Handle Offsets (only for Common Handle), n=n_pk+1
/// ...    (Align to 8 bytes)
/// ...    Primary Key Data Bytes (ordered by PK in ascending order)
/// u32    Primary Key Filter Length
/// ...    Primary Key Filter Bytes
/// u32    Tantivy Data Length
/// ...    (Align to 8 bytes)
/// ...    Tantivy Data Bytes
/// ...    (Align to 8 bytes)
///
/// For example, we may have such data (unordered):
/// (PK, Version)
/// (7,  2)
/// (1,  1)
/// (1,  3)
/// (3,  100)
///
/// They will be stored in PackedFileLp in this way:
/// PK:      [1, 1, 3,   7] (ascending order)
/// Version: [3, 1, 100, 2] (newer version comes first for the same PK)
#[derive(Clone)]
pub enum EPackedFileLp {
    Int(PackedFileLp<IntPk>),
    Common(PackedFileLp<CommonPk>),
}

pub struct PackedFileLp<Pk: PkType>(Arc<PackedFileLpCore<Pk>>);

impl<Pk: PkType> Clone for PackedFileLp<Pk> {
    fn clone(&self) -> Self {
        PackedFileLp(self.0.clone())
    }
}

struct PackedFileLpCore<Pk: PkType> {
    file_id: u64,
    cache: FtsCache,

    entry_data: Bytes, /* Contains the whole entry data. It must be kept so that the rest
                        * fields' lifetime is valid */

    #[allow(unused)]
    source_guard: FileMmapGuard,

    lp_key: Bytes,

    props: ftspb::PackedFileLpProps,

    /// 'static because comes from `entry_data`.
    /// MUST NOT expose to outside.
    unsafe_versions: &'static [u64],
    /// 'static because comes from `entry_data`.
    /// MUST NOT expose to outside.
    unsafe_delete_marks: &'static [u8],

    /// Only exists when pk is common PK (is_pk_int == false).
    /// It contains n_pk+1 elements, where the last element is the end offset.
    /// 'static because comes from `entry_data`.
    /// MUST NOT expose to outside.
    unsafe_pk_common_offsets: &'static [u32],

    /// for Int PK, size=n_pk*8, must be aligned by 8 bytes
    /// for Common PK, size varies.
    pk_data: Bytes,

    pk_filter: BinaryFuse8, // The underlying storage is still entry_data, sharing same lifecycle.

    tantivy_index_data: Bytes,

    _mark: std::marker::PhantomData<Pk>,
}

impl EPackedFileLp {
    /// Returns the PackedFileLp as IntPk type.
    #[inline]
    pub fn as_int_lp(&self) -> Result<&PackedFileLp<IntPk>> {
        match self {
            EPackedFileLp::Int(lp) => Ok(lp),
            _ => Err(anyhow!("Unexpected LP type, expect Int")),
        }
    }

    /// Returns the PackedFileLp as CommonPk type.
    #[inline]
    pub fn as_common_lp(&self) -> Result<&PackedFileLp<CommonPk>> {
        match self {
            EPackedFileLp::Common(lp) => Ok(lp),
            _ => Err(anyhow!("Unexpected LP type, expect Common")),
        }
    }

    /// Returns the PackedFileLp as the specified Pk type.
    #[inline]
    pub fn as_pk<Pk: PkType>(&self) -> Result<&PackedFileLp<Pk>> {
        // This `if` will be eliminated during compilation because TypeId::of()
        // is const.
        if std::any::TypeId::of::<Pk>() == std::any::TypeId::of::<IntPk>() {
            self.as_int_lp().map(|lp| unsafe {
                std::mem::transmute::<&PackedFileLp<IntPk>, &PackedFileLp<Pk>>(lp)
            })
        } else if std::any::TypeId::of::<Pk>() == std::any::TypeId::of::<CommonPk>() {
            self.as_common_lp().map(|lp| unsafe {
                std::mem::transmute::<&PackedFileLp<CommonPk>, &PackedFileLp<Pk>>(lp)
            })
        } else {
            panic!("Unsupported Pk type");
        }
    }

    /// Returns the Logical Partition Key.
    ///
    /// Returns `&Bytes` which can be:
    /// - Used as `&[u8]` via automatic Deref coercion
    /// - Cloned efficiently with `clone()` (just increments Arc refcount)
    #[inline]
    pub fn lp_key(&self) -> &Bytes {
        match self {
            EPackedFileLp::Int(lp) => lp.lp_key(),
            EPackedFileLp::Common(lp) => lp.lp_key(),
        }
    }

    /// Returns the properties.
    #[inline]
    pub fn props(&self) -> &ftspb::PackedFileLpProps {
        match self {
            EPackedFileLp::Int(lp) => lp.props(),
            EPackedFileLp::Common(lp) => lp.props(),
        }
    }

    /// Build a Tantivy reader backed by this LP.
    #[inline]
    pub fn read_tantivy_index(&self) -> Result<Arc<clara_fts::IndexReader>> {
        match self {
            EPackedFileLp::Int(f) => f.cached_read_index(),
            EPackedFileLp::Common(f) => f.cached_read_index(),
        }
    }

    /// Returns the serialized size of this LogicalPartition entry in bytes.
    #[inline]
    pub fn serialized_size(&self) -> usize {
        match self {
            EPackedFileLp::Int(lp) => lp.serialized_size(),
            EPackedFileLp::Common(lp) => lp.serialized_size(),
        }
    }

    /// Parses entry bytes and builds an [`EPackedFileLp`] backed by them.
    fn new(
        file_id: u64,
        cache: FtsCache,

        mut entry_data: Bytes,
        source_guard: FileMmapGuard,
    ) -> Result<EPackedFileLp> {
        entry_data.check_aligned::<u64>()?;
        let entry_data_full = entry_data.clone();

        let mut current_offset = 0usize;

        // Read LogicalPartitionKey Length and skip key bytes
        let lp_key_len = entry_data.try_get_u16_le()? as usize;
        current_offset += 2;
        let lp_key = entry_data.try_get_first(lp_key_len)?;
        current_offset += lp_key_len;

        // Read Props Length and Props Protobuf
        let props_len = entry_data.try_get_u32_le()? as usize;
        current_offset += 4;
        let props_data = entry_data.try_get_first(props_len)?;
        current_offset += props_len;

        let mut props = ftspb::PackedFileLpProps::new();
        props.merge_from_bytes(&props_data)?;

        let n_pk = props.get_n_pk();
        let is_pk_int = props.get_is_int_handle();

        // Align to 8 bytes
        let next_offset = next_aligned_offset(current_offset, 8);
        if next_offset > current_offset {
            entry_data.advance(next_offset - current_offset);
            current_offset = next_offset;
        }

        // Read Versions (n_pk * 8 bytes)
        let versions_data = entry_data.try_get_first((n_pk as usize) * 8)?;
        let versions = versions_data.try_as_slice::<u64>()?;
        current_offset += versions_data.len();

        // Read Delete Marks (n_pk bytes)
        let delete_marks_data = entry_data.try_get_first(n_pk as usize)?;
        let delete_marks = delete_marks_data.try_as_slice::<u8>()?;
        current_offset += delete_marks_data.len();

        // Align to 4 bytes
        let next_offset = next_aligned_offset(current_offset, 4);
        if next_offset > current_offset {
            entry_data.advance(next_offset - current_offset);
            current_offset = next_offset;
        }

        // Read Common Handle Offsets (only for Common Handle)
        let offsets_data = if !is_pk_int {
            entry_data.try_get_first(((n_pk + 1) as usize) * 4)?
        } else {
            Bytes::new() // No offsets for Int handle
        };
        current_offset += offsets_data.len();
        let pk_common_offsets = if !is_pk_int {
            offsets_data.try_as_slice::<u32>()?
        } else {
            // Empty Bytes will not have a proper alignment. So for empty Bytes (pk_int) we
            // create an empty slice directly.
            &[]
        };

        // Align to 8 bytes for Primary Key Data
        let next_offset = next_aligned_offset(current_offset, 8);
        if next_offset > current_offset {
            entry_data.advance(next_offset - current_offset);
            current_offset = next_offset;
        }

        // Read Primary Key Data
        let pk_data_size = if is_pk_int {
            // For Int handle: n_pk * 8 bytes
            (n_pk as usize) * 8
        } else {
            // For Common handle
            pk_common_offsets.last().copied().unwrap() as usize
        };

        let pk_data = entry_data.try_get_first(pk_data_size)?;
        current_offset += pk_data_size;

        // Read Primary Key Filter Length and Filter Bytes
        let pk_filter_len = entry_data.try_get_u32_le()? as usize;
        let pk_filter_data = entry_data.try_get_first(pk_filter_len)?;
        current_offset += 4 + pk_filter_len;

        let pk_filter = BinaryFuse8::try_from_bytes(&pk_filter_data)
            .map_err(|e| anyhow!("Bad PK filter: {}", e))?;

        // Read Tantivy Index Data Length and Index Data Bytes
        let tantivy_len = entry_data.try_get_u32_le()? as usize;
        current_offset += 4;
        let next_offset = next_aligned_offset(current_offset, 8);
        if next_offset > current_offset {
            entry_data.advance(next_offset - current_offset);
            current_offset = next_offset;
        }
        let tantivy_index_data = entry_data.try_get_first(tantivy_len)?;

        // Make lint happy
        _ = current_offset;

        if is_pk_int {
            Ok(EPackedFileLp::Int(PackedFileLp::<IntPk>(Arc::new(
                PackedFileLpCore {
                    entry_data: entry_data_full, /* Keep a full entry data to make
                                                  * serialized_size work */
                    source_guard,
                    file_id,
                    cache,
                    lp_key,
                    props,
                    unsafe_versions: unsafe { crate::util::extend_lifetime(versions) },
                    unsafe_delete_marks: unsafe { crate::util::extend_lifetime(delete_marks) },
                    unsafe_pk_common_offsets: unsafe {
                        crate::util::extend_lifetime(pk_common_offsets)
                    },
                    pk_data,
                    pk_filter,
                    tantivy_index_data,

                    _mark: Default::default(),
                },
            ))))
        } else {
            Ok(EPackedFileLp::Common(PackedFileLp::<CommonPk>(Arc::new(
                PackedFileLpCore {
                    entry_data: entry_data_full, /* Keep a full entry data to make
                                                  * serialized_size work */
                    source_guard,
                    file_id,
                    cache,
                    lp_key,
                    props,
                    unsafe_versions: unsafe { crate::util::extend_lifetime(versions) },
                    unsafe_delete_marks: unsafe { crate::util::extend_lifetime(delete_marks) },
                    unsafe_pk_common_offsets: unsafe {
                        crate::util::extend_lifetime(pk_common_offsets)
                    },
                    pk_data,
                    pk_filter,
                    tantivy_index_data,

                    _mark: Default::default(),
                },
            ))))
        }
    }
}

impl<Pk: PkType> PackedFileLp<Pk> {
    /// Returns the Logical Partition Key of this LogicalPartition.
    #[inline]
    pub fn lp_key(&self) -> &Bytes {
        &self.0.lp_key
    }

    /// Returns the properties of this LogicalPartition.
    #[inline]
    pub fn props(&self) -> &ftspb::PackedFileLpProps {
        &self.0.props
    }

    /// Returns the serialized size of this LogicalPartition entry in bytes.
    #[inline]
    pub fn serialized_size(&self) -> usize {
        self.0.entry_data.len()
    }

    /// Returns the version at the given primary key index.
    #[inline]
    pub fn version_at(&self, doc_id: usize) -> u64 {
        // This is safe, because we copy the reference into a value
        self.0.unsafe_versions[doc_id]
    }

    /// Returns the delete mark at the given primary key index.
    #[inline]
    pub fn is_deleted_at(&self, doc_id: usize) -> u8 {
        // This is safe, because we copy the reference into a value
        self.0.unsafe_delete_marks[doc_id]
    }

    /// Returns the encoded primary key bytes at the given index.
    #[inline]
    pub fn encoded_pk_at(&self, doc_id: usize) -> Bytes {
        // This `if` will be eliminated during compilation because TypeId::of()
        // is const.
        if std::any::TypeId::of::<Pk>() == std::any::TypeId::of::<IntPk>() {
            let start_offset = doc_id * 8; // Each encoded i64 is exactly 8 bytes
            let end_offset = start_offset + 8;
            self.0.pk_data.slice(start_offset..end_offset)
        } else if std::any::TypeId::of::<Pk>() == std::any::TypeId::of::<CommonPk>() {
            let offsets = self.0.unsafe_pk_common_offsets;
            let start_offset = offsets[doc_id] as usize;
            let end_offset = offsets[doc_id + 1] as usize;
            self.0.pk_data.slice(start_offset..end_offset)
        } else {
            panic!("Unsupported Pk type");
        }
    }

    /// Build a Tantivy reader backed by this LP.
    #[inline]
    pub fn cached_read_index(&self) -> Result<Arc<clara_fts::IndexReader>> {
        self.0.cache.get_with(
            CacheKeyRef::PackedIndex {
                file_id: self.0.file_id,
                lp_key: self.lp_key().as_ref(),
            },
            || {
                let bytes_dir = self.tantivy_directory();
                let reader = Arc::new(clara_fts::IndexReader::new(bytes_dir)?);
                Ok(FtsCacheValue::with_mmap_guard(
                    reader,
                    self.0.source_guard.clone(),
                ))
            },
        )
    }

    fn tantivy_directory(&self) -> BytesDirRO {
        let layout = self.0.props.get_tantivy_layout();
        let slice = |range: &ftspb::OffsetSize| -> Bytes {
            if range.size == 0 {
                return Bytes::new();
            }
            self.0
                .tantivy_index_data
                .slice(range.offset as usize..(range.offset + range.size) as usize)
        };
        BytesDirRO {
            meta_json: slice(layout.get_meta()),
            managed_json: slice(layout.get_managed()),
            term: slice(layout.get_term()),
            idx: slice(layout.get_idx()),
            pos: slice(layout.get_pos()),
            store: slice(layout.get_store()),
            fast: slice(layout.get_fast()),
            fieldnorm: slice(layout.get_fieldnorm()),
        }
    }
}
