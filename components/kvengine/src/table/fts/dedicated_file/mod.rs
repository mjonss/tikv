// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::Debug,
    sync::{Arc, RwLock},
};

use anyhow::{Result, anyhow, bail};
use bytes::{Buf, Bytes};
use hexhex::hex;
use kvenginepb::{FtsDedFileInfo, fts as ftspb};
use protobuf::Message;
use xorf::BinaryFuse8;

use super::iter::{CommonPk, IntPk, PkType};
use crate::{
    codecutil::BytesExt,
    table::{
        BoundedDataSet, ChecksumType, DataBound, InnerKey,
        file::{File, FileMmapGuard, TtlCache},
        fts::{CacheKeyRef, FtsCache, FtsCacheValue},
    },
};

mod builder;
mod bytes_directory;
pub use bytes_directory::BytesDirRO;
mod iter;

#[cfg(any(test, feature = "testexport"))]
#[cfg_attr(feature = "testexport", allow(unused))]
pub mod test;

pub use builder::*;
pub use iter::*;

/// Magic number for FTS DedicatedFile format
pub const FTS_DEDICATED_FILE_MAGIC: u32 = 0xFEE74B2F;

/// Format version for FTS DedicatedFile
pub const FTS_DEDICATED_FILE_FORMAT_V1: u8 = 0x1;

/// Size of serialized footer in bytes
pub const FTS_DEDICATED_FILE_FOOTER_SIZE: usize = 52;

// TODO List:
// 1. Currently, DedicatedFile mmaps the tantivy index data in .xxx files, which
//    ultimately results in the entire index data being downloaded. This
//    requires more refined handling of on-demand download requirements.
// 2. Add integrity check for DedicatedFile's data block.

/// Footer has a fixed size and is always located at the end of the file.
/// The footer size cannot be changed.
/// Format:
/// - 1 byte: format version
/// - 1 byte: checksum type
/// - 6 bytes: reserved
/// - 4 bytes: checksum of footer
/// - 4 bytes: checksum of remaining meta blocks
/// - 8 bytes: data block offset (Tantivy data)
/// - 8 bytes: pk filter block offset
/// - 8 bytes: handle index block offset
/// - 8 bytes: property block offset
/// - 4 bytes: magic number
#[repr(C)]
#[derive(Default, Clone, Copy, Debug)]
pub struct DedicatedFileFooter {
    pub format: u8,
    pub checksum_type: ChecksumType,
    pub checksum_other_meta: u32,
    pub data_block_offset: u64,
    pub pk_filter_block_offset: u64,
    pub iblock_offset: u64,
    pub prop_offset: u64,
    pub magic: u32,
}

impl DedicatedFileFooter {
    /// Get the size of the footer
    pub const fn footer_size() -> usize {
        FTS_DEDICATED_FILE_FOOTER_SIZE
    }

    /// Create a new footer with default values
    pub fn new() -> Self {
        Self {
            format: FTS_DEDICATED_FILE_FORMAT_V1,
            checksum_type: ChecksumType::None,
            checksum_other_meta: 0,
            data_block_offset: 0,
            pk_filter_block_offset: 0,
            iblock_offset: 0,
            prop_offset: 0,
            magic: FTS_DEDICATED_FILE_MAGIC,
        }
    }

    /// Marshal footer to bytes
    pub fn marshal<W: std::io::Write>(&self, mut w: W) -> Result<usize> {
        use bytes::BufMut;

        let mut footer = [0u8; FTS_DEDICATED_FILE_FOOTER_SIZE];
        let mut data = &mut footer[..];

        // Write fields according to the format specification
        data.put_u8(self.format); // 1 byte: format version
        data.put_u8(self.checksum_type.value()); // 1 byte: checksum type
        data.put_slice(&[0; 6]); // 6 bytes: reserved
        data.put_slice(&[0; 4]); // 4 bytes: placeholder for checksum of footer
        data.put_u32_le(self.checksum_other_meta); // 4 bytes: checksum of remaining meta blocks
        data.put_u64_le(self.data_block_offset); // 8 bytes: data block offset (Tantivy data)
        data.put_u64_le(self.pk_filter_block_offset); // 8 bytes: pk filter block offset
        data.put_u64_le(self.iblock_offset); // 8 bytes: handle index block offset
        data.put_u64_le(self.prop_offset); // 8 bytes: property block offset
        data.put_u32_le(self.magic); // 4 bytes: magic number

        // Calculate and write the footer checksum
        let checksum_footer = self.checksum_type.checksum(&footer);
        footer[8..12].copy_from_slice(&checksum_footer.to_le_bytes());

        w.write_all(&footer)?;

        Ok(FTS_DEDICATED_FILE_FOOTER_SIZE)
    }

    /// Unmarshal footer from bytes
    pub fn unmarshal(mut data: &[u8]) -> Result<Self> {
        if data.len() != FTS_DEDICATED_FILE_FOOTER_SIZE {
            bail!(
                "Invalid footer size, expected {:#x}, got {:#x}",
                FTS_DEDICATED_FILE_FOOTER_SIZE,
                data.len()
            );
        }
        let mut copied_data = [0u8; FTS_DEDICATED_FILE_FOOTER_SIZE];
        copied_data.copy_from_slice(data);

        // Read fields according to the format specification
        let format = data.get_u8(); // 1 byte: format version
        let checksum_type_value = data.get_u8(); // 1 byte: checksum type
        let checksum_type = ChecksumType::from(checksum_type_value);
        data.advance(6); // 6 bytes: reserved
        let checksum_footer = data.get_u32_le(); // 4 bytes: checksum of footer
        let checksum_other_meta = data.get_u32_le(); // 4 bytes: checksum of remaining meta blocks
        let data_block_offset = data.get_u64_le(); // 8 bytes: data block offset (Tantivy data)
        let pk_filter_block_offset = data.get_u64_le(); // 8 bytes: pk filter block offset
        let iblock_offset = data.get_u64_le(); // 8 bytes: handle index block offset
        let prop_offset = data.get_u64_le(); // 8 bytes: property block offset
        let magic = data.get_u32_le(); // 4 bytes: magic number

        // Validate magic number
        if magic != FTS_DEDICATED_FILE_MAGIC {
            bail!(
                "Invalid magic number, expected {:#x}, got {:#x}",
                FTS_DEDICATED_FILE_MAGIC,
                magic
            );
        }

        // Validate format version
        if format != FTS_DEDICATED_FILE_FORMAT_V1 {
            bail!(
                "Unsupported format version, expected {:#x}, got {:#x}",
                FTS_DEDICATED_FILE_FORMAT_V1,
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
                    "FtsDedicatedFile footer checksum mismatch, actual {:#08x}, expect {:#08x}, footer data: {}, ChecksumType={:?}",
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
            data_block_offset,
            pk_filter_block_offset,
            iblock_offset,
            prop_offset,
            magic,
        })
    }

    /// Returns the starting offset of the metadata blocks in the DedicatedFile
    /// indicated by this footer.
    ///
    /// As Handle Index Block is the first metadata block, its offset is
    /// returned.
    pub fn metadata_offset(&self) -> u64 {
        self.iblock_offset
    }
}

/// Enum wrapper for DedicatedFile that determines PK type at runtime.
#[derive(Debug)]
pub enum EDedicatedFile {
    Int(DedicatedFile<IntPk>),
    Common(DedicatedFile<CommonPk>),
}

impl Clone for EDedicatedFile {
    fn clone(&self) -> Self {
        match self {
            Self::Int(file) => Self::Int(file.clone()),
            Self::Common(file) => Self::Common(file.clone()),
        }
    }
}

/// An in-memory FtsDedicatedFile. Only minimal metadata is kept in memory.
/// Main data is accessed through mmap, accelerated data is cached through
/// TtlCache.
pub struct DedicatedFile<Pk>(Arc<DedicatedFileCore<Pk>>);

impl<Pk> Clone for DedicatedFile<Pk> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// Core implementation of DedicatedFile
struct DedicatedFileCore<Pk> {
    file: Arc<dyn File>,
    cache: FtsCache,

    iblock: TtlCache<ftspb::DedFileIBlock>,
    pk_filter: TtlCache<BinaryFuse8>,
    props: ftspb::DedFilePropBlock,
    footer: DedicatedFileFooter,
    hblock_checked: RwLock<Vec<bool>>, // AtomicBool would be better

    // Tantivy index cache - Caches initialized tantivy Index.
    // tantivy_index_cache: OnceCell<tantivy::Index>,
    _marker: std::marker::PhantomData<Pk>,
}

// Static helper methods on DedicatedFile<()>
impl DedicatedFile<()> {
    /// Load the footer from the packed file.
    fn load_footer(file: &dyn File) -> Result<DedicatedFileFooter> {
        let footer_data = file.read_footer(FTS_DEDICATED_FILE_FOOTER_SIZE)?;
        let footer = DedicatedFileFooter::unmarshal(footer_data.as_ref())?;
        Self::validate_footer_offsets(file, &footer)?;
        Self::verify_meta_checksum(file, &footer)?;
        Ok(footer)
    }

    /// Load the properties block from the dedicated file.
    /// Property block is the last metadata block, located between prop_offset
    /// and footer. According to DedicatedFileBuilder write order: Handle
    /// Index -> PK Filter -> Property -> Footer
    fn load_props(
        file: &dyn File,
        footer: &DedicatedFileFooter,
    ) -> Result<ftspb::DedFilePropBlock> {
        let prop_offset = footer.prop_offset;
        let file_size = file.size();
        let footer_start = file_size - FTS_DEDICATED_FILE_FOOTER_SIZE as u64;

        // Verify that Property Block offset is valid
        if prop_offset >= footer_start {
            bail!(
                "Invalid property block offset {:#x}, footer starts at {:#x}",
                prop_offset,
                footer_start
            );
        }

        // Property Block is located between prop_offset and footer start
        let prop_len = (footer_start - prop_offset) as usize;
        if prop_len == 0 {
            bail!("Property block has zero size, this should not happen");
        }

        let data = file.read_table_meta(prop_offset, prop_len)?;
        let mut prop = ftspb::DedFilePropBlock::new();
        prop.merge_from_bytes(&data)
            .map_err(|e| anyhow!("Failed to parse property block: {}", e))?;

        // Verify that required fields are present
        if prop.get_lp_key().is_empty() {
            bail!("Property block is missing lp_key field");
        }

        Ok(prop)
    }

    /// Verify the metadata checksum for the dedicated file.
    /// The checksum covers metadata blocks in order: handle index, pk filter,
    /// and property blocks. This matches the order used in
    /// DedicatedFileBuilder::finish().
    fn verify_meta_checksum(file: &dyn File, footer: &DedicatedFileFooter) -> Result<()> {
        let mut meta_checksum = 0u32;

        let file_size = file.size();
        let footer_start = file_size
            .checked_sub(FTS_DEDICATED_FILE_FOOTER_SIZE as u64)
            .ok_or_else(|| {
                anyhow!(
                    "DedicatedFile size {} smaller than footer size {}",
                    file_size,
                    FTS_DEDICATED_FILE_FOOTER_SIZE
                )
            })?;
        let meta_start = footer.metadata_offset();
        let meta_len = footer_start.checked_sub(meta_start).ok_or_else(|| {
            anyhow!(
                "DedicatedFile metadata offset {:#x} out of bounds for footer start {:#x}",
                meta_start,
                footer_start
            )
        })?;
        let meta_data = file.read_table_meta(meta_start, meta_len as usize)?;
        meta_checksum = footer.checksum_type.append(meta_checksum, &meta_data);

        // Verify accumulated checksum
        if meta_checksum != footer.checksum_other_meta {
            bail!(
                "FtsDedicatedFile meta checksum mismatch, actual {:#08x}, expect {:#08x}, ChecksumType={:?}",
                meta_checksum,
                footer.checksum_other_meta,
                footer.checksum_type
            );
        }
        Ok(())
    }

    fn validate_footer_offsets(file: &dyn File, footer: &DedicatedFileFooter) -> Result<()> {
        let file_size = file.size();
        let footer_start = file_size
            .checked_sub(FTS_DEDICATED_FILE_FOOTER_SIZE as u64)
            .ok_or_else(|| {
                anyhow!(
                    "DedicatedFile size {} smaller than footer size {}",
                    file_size,
                    FTS_DEDICATED_FILE_FOOTER_SIZE
                )
            })?;

        let data_off = footer.data_block_offset;
        let iblock_off = footer.iblock_offset;
        let filter_off = footer.pk_filter_block_offset;
        let prop_off = footer.prop_offset;

        if !(data_off <= iblock_off && iblock_off <= filter_off && filter_off <= prop_off) {
            bail!(
                "DedicatedFile footer offsets not monotonic: data_block_offset={:#x}, iblock_offset={:#x}, pk_filter_block_offset={:#x}, prop_offset={:#x}",
                data_off,
                iblock_off,
                filter_off,
                prop_off
            );
        }
        if prop_off > footer_start {
            bail!(
                "DedicatedFile footer offsets out of bounds: prop_offset={:#x} > footer_start={:#x}",
                prop_off,
                footer_start
            );
        }

        Ok(())
    }

    /// Builds a protobuf info message for a dedicated file based on metadata.
    pub fn info(
        file_id: u64,
        meta_offset: u32,
        mut props: ftspb::DedFilePropBlock,
    ) -> FtsDedFileInfo {
        FtsDedFileInfo {
            id: file_id,
            meta_offset,
            smallest: props.take_smallest_key(),
            biggest: props.take_biggest_key(),

            // We use explicit fields to avoid missing new fields in the future.
            unknown_fields: Default::default(),
            cached_size: Default::default(),
        }
    }

    /// Load the handle index block from the dedicated file.
    fn load_iblock(file: &dyn File, footer: &DedicatedFileFooter) -> Result<ftspb::DedFileIBlock> {
        let offset = footer.iblock_offset;
        let length = footer
            .pk_filter_block_offset
            .checked_sub(footer.iblock_offset)
            .ok_or_else(|| {
                anyhow!(
                    "iblock broken: pk_filter_block_offset {:#x} smaller than iblock_offset {:#x}",
                    footer.pk_filter_block_offset,
                    footer.iblock_offset
                )
            })?;
        let data = file.read_table_meta(offset, length as usize)?;

        let mut iblock = ftspb::DedFileIBlock::new();
        iblock.merge_from_bytes(&data)?;

        if iblock.hblock_start_key.is_empty() || iblock.hblock_offsets.is_empty() {
            bail!("iblock broken: hblock_start_key and hblock_offsets must not be empty");
        }
        if iblock.hblock_start_key.len() + 1 != iblock.hblock_offsets.len() {
            bail!(
                "iblock broken: hblock_start_key.len={} but hblock_offsets.len={}, expected N+1 relationship",
                iblock.hblock_start_key.len(),
                iblock.hblock_offsets.len()
            );
        }
        if iblock.hblock_start_docid.is_empty() {
            bail!("iblock broken: hblock_start_docid must not be empty");
        }
        if iblock.hblock_start_docid.len() != iblock.hblock_offsets.len() {
            bail!(
                "iblock broken: hblock_start_docid.len={} but hblock_offsets.len={}, expected identical length",
                iblock.hblock_start_docid.len(),
                iblock.hblock_offsets.len()
            );
        }
        if iblock.hblock_start_docid.first() != Some(&0) {
            bail!(
                "iblock broken: first hblock_start_docid must be 0, got {}",
                iblock
                    .hblock_start_docid
                    .first()
                    .copied()
                    .unwrap_or_default()
            );
        }
        let rank_stride = iblock.get_hblock_handle_rank_stride();
        if rank_stride == 0 {
            bail!("iblock broken: handle rank stride must be greater than 0");
        }
        let ranks = iblock.get_hblock_handle_rank();
        if ranks.len() < 2 {
            bail!("iblock broken: handle rank must have at least 2 entries");
        }
        let last = ranks.last().copied().unwrap() as usize;
        if last != iblock.hblock_start_key.len() {
            bail!(
                "iblock broken: handle rank sentinel {} must equal handle block count {}",
                last,
                iblock.hblock_start_key.len()
            );
        }

        Ok(iblock)
    }

    /// Load the PK filter block from the dedicated file.
    fn load_pk_filter(file: &dyn File, footer: &DedicatedFileFooter) -> Result<BinaryFuse8> {
        let offset = footer.pk_filter_block_offset;
        let length = footer
            .prop_offset
            .checked_sub(footer.pk_filter_block_offset)
            .ok_or_else(|| {
                anyhow!(
                    "pk filter block broken: prop_offset {:#x} smaller than pk_filter_block_offset {:#x}",
                    footer.prop_offset,
                    footer.pk_filter_block_offset
                )
            })?;
        let data = file.read_table_meta(offset, length as usize)?;

        let filter =
            BinaryFuse8::try_from_bytes(&data).map_err(|e| anyhow!("Bad pk filter: {}", e))?;
        Ok(filter)
    }

    /// Create a new DedicatedFile from a remote file.
    pub fn new(file: Arc<dyn File>, cache: FtsCache) -> Result<EDedicatedFile> {
        EDedicatedFile::new(file, cache)
    }

    /// Calculate IA segment offsets for an FTS dedicated file based on handle
    /// blocks and data block. This function reads the handle index block and
    /// calculates segment boundaries based on the standard IA segment size.
    ///
    /// DedicatedFile layout: Handle Blocks -> Data Block -> Metadata Blocks ->
    /// Footer We need to create segments that respect both handle block
    /// boundaries and the large data block.
    pub fn generate_ia_segment_boundaries(file: &dyn File, segment_size: u64) -> Result<Vec<u64>> {
        let footer = DedicatedFile::load_footer(file)?;
        let iblock = DedicatedFile::load_iblock(file, &footer)?;

        let mut segment_offsets = vec![0]; // Always start with offset 0
        let mut current_segment_start = 0u64;

        // 1. First, create segments based on handle block boundaries
        // Handle blocks are typically small and numerous, so we group them into
        // segments
        for &block_offset in &iblock.hblock_offsets {
            // If this handle block would make the current segment exceed the segment size,
            // start a new segment
            if block_offset >= current_segment_start + segment_size {
                segment_offsets.push(block_offset);
                current_segment_start = block_offset;
            }
        }

        // 2. Handle the data block (Tantivy data) which is typically very large
        // The data block starts after all handle blocks
        let data_block_start = footer.data_block_offset;
        if data_block_start >= current_segment_start + segment_size {
            segment_offsets.push(data_block_start);
            current_segment_start = data_block_start;
        }

        // 3. Create segment boundaries based on individual Tantivy files within the
        //    data_block
        // Load props to get Tantivy file information
        let props = DedicatedFile::load_props(file, &footer)?;
        let layout = props.get_tantivy_layout();

        // Collect boundaries of all Tantivy files
        let mut tantivy_file_boundaries = Vec::new();
        let file_ranges = [
            layout.get_meta(),
            layout.get_managed(),
            layout.get_term(),
            layout.get_idx(),
            layout.get_pos(),
            layout.get_store(),
            layout.get_fast(),
            layout.get_fieldnorm(),
        ];

        for file_range in file_ranges {
            if file_range.get_size() > 0 {
                let file_start = data_block_start + file_range.get_offset();
                tantivy_file_boundaries.push(file_start);
            }
        }

        // Sort by file start position and remove duplicates.
        tantivy_file_boundaries.sort_unstable();
        tantivy_file_boundaries.dedup();

        // Create segment boundaries for each Tantivy file.
        for &file_start in &tantivy_file_boundaries {
            // If the file start is far enough from the current segment start, create a new
            // segment.
            if file_start >= current_segment_start + segment_size {
                segment_offsets.push(file_start);
                current_segment_start = file_start;
            }
        }

        segment_offsets.push(footer.iblock_offset);

        Ok(segment_offsets)
    }
}

impl<Pk: PkType> Debug for DedicatedFile<Pk> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DedicatedFile")
            .field("file_id", &self.0.file.id())
            .field("props", &self.0.props)
            .finish()
    }
}

impl<Pk: PkType> BoundedDataSet for DedicatedFile<Pk> {
    fn data_bound(&self) -> DataBound<'_> {
        DataBound::new(
            InnerKey::from_inner_buf(self.props().get_smallest_key()),
            InnerKey::from_inner_buf(self.props().get_biggest_key()),
            true,
        )
    }
}

impl BoundedDataSet for EDedicatedFile {
    fn data_bound(&self) -> DataBound<'_> {
        match self {
            EDedicatedFile::Int(file) => file.data_bound(),
            EDedicatedFile::Common(file) => file.data_bound(),
        }
    }
}

impl EDedicatedFile {
    pub fn new(file: Arc<dyn File>, cache: FtsCache) -> Result<Self> {
        let footer = DedicatedFile::load_footer(file.as_ref())?;
        let props = DedicatedFile::load_props(file.as_ref(), &footer)?;

        let is_int_handle = props.get_is_int_handle();

        if is_int_handle {
            Ok(EDedicatedFile::Int(DedicatedFile(Arc::new(
                DedicatedFileCore {
                    file,
                    cache,

                    iblock: TtlCache::default(),
                    pk_filter: TtlCache::default(),
                    props,
                    footer,
                    hblock_checked: RwLock::new(vec![]),

                    _marker: std::marker::PhantomData,
                },
            ))))
        } else {
            Ok(EDedicatedFile::Common(DedicatedFile(Arc::new(
                DedicatedFileCore {
                    file,
                    cache,

                    iblock: TtlCache::default(),
                    pk_filter: TtlCache::default(),
                    props,
                    footer,
                    hblock_checked: RwLock::new(vec![]),

                    _marker: std::marker::PhantomData,
                },
            ))))
        }
    }

    /// Build a Tantivy reader backed by this dedicated file.
    #[inline]
    pub async fn cached_read_index(&self) -> Result<Arc<clara_fts::IndexReader>> {
        match self {
            EDedicatedFile::Int(f) => f.cached_read_index().await,
            EDedicatedFile::Common(f) => f.cached_read_index().await,
        }
    }

    /// Returns the file ID.
    #[inline]
    pub fn id(&self) -> u64 {
        match self {
            EDedicatedFile::Int(file) => file.id(),
            EDedicatedFile::Common(file) => file.id(),
        }
    }

    /// Returns whether the handle is int.
    #[inline]
    pub fn is_int_handle(&self) -> bool {
        matches!(self, EDedicatedFile::Int(_))
    }

    /// Access the underlying file.
    #[inline]
    pub fn file(&self) -> &Arc<dyn File> {
        match self {
            EDedicatedFile::Int(file) => file.file(),
            EDedicatedFile::Common(file) => file.file(),
        }
    }

    #[inline]
    pub fn as_int(&self) -> Result<&DedicatedFile<IntPk>> {
        match self {
            EDedicatedFile::Int(file) => Ok(file),
            _ => Err(anyhow!("Unexpected LP type, expect Int")),
        }
    }

    #[inline]
    pub fn as_common(&self) -> Result<&DedicatedFile<CommonPk>> {
        match self {
            EDedicatedFile::Common(file) => Ok(file),
            _ => Err(anyhow!("Unexpected LP type, expect Common")),
        }
    }

    /// Returns this dedicated file as the specified `Pk` type.
    #[inline]
    pub fn as_pk<Pk: PkType>(&self) -> Result<&DedicatedFile<Pk>> {
        // This `if` will be eliminated during compilation because TypeId::of()
        // is const.
        if std::any::TypeId::of::<Pk>() == std::any::TypeId::of::<IntPk>() {
            self.as_int().map(|lp| unsafe {
                std::mem::transmute::<&DedicatedFile<IntPk>, &DedicatedFile<Pk>>(lp)
            })
        } else if std::any::TypeId::of::<Pk>() == std::any::TypeId::of::<CommonPk>() {
            self.as_common().map(|lp| unsafe {
                std::mem::transmute::<&DedicatedFile<CommonPk>, &DedicatedFile<Pk>>(lp)
            })
        } else {
            Err(anyhow!("Unsupported Pk type"))
        }
    }

    /// Returns the property block of this dedicated file.
    #[inline]
    pub fn props(&self) -> &ftspb::DedFilePropBlock {
        match self {
            EDedicatedFile::Int(file) => file.props(),
            EDedicatedFile::Common(file) => file.props(),
        }
    }

    /// Builds a protobuf info message describing this dedicated file.
    pub fn build_info(&self) -> FtsDedFileInfo {
        match self {
            EDedicatedFile::Int(file) => file.build_info(),
            EDedicatedFile::Common(file) => file.build_info(),
        }
    }
}

impl<Pk: PkType> DedicatedFile<Pk> {
    /// Builds a protobuf info message describing this dedicated file.
    pub fn build_info(&self) -> FtsDedFileInfo {
        DedicatedFile::info(
            self.id(),
            self.0.footer.metadata_offset() as u32,
            self.0.props.clone(),
        )
    }

    /// Returns the file ID of the underlying IA file.
    #[inline]
    pub fn id(&self) -> u64 {
        self.0.file.id()
    }

    /// Access the underlying file.
    #[inline]
    pub fn file(&self) -> &Arc<dyn File> {
        &self.0.file
    }

    /// Returns whether the primary key is int.
    #[inline]
    pub fn is_pk_int(&self) -> bool {
        Pk::IS_INT
    }

    /// Get or load Handle index block.
    #[inline]
    fn get_iblock(&self) -> Result<Arc<ftspb::DedFileIBlock>> {
        // ... TtlCache get-or-create implementation ...
        self.0
            .iblock
            .get(|| DedicatedFile::load_iblock(self.0.file.as_ref(), &self.0.footer))
    }

    /// Get or load primary key filter.
    #[inline]
    fn get_pk_filter(&self) -> Result<Arc<BinaryFuse8>> {
        // ... TtlCache get-or-create implementation ...
        self.0
            .pk_filter
            .get(|| DedicatedFile::load_pk_filter(self.0.file.as_ref(), &self.0.footer))
    }

    /// Get footer.
    #[inline]
    pub fn footer(&self) -> &DedicatedFileFooter {
        &self.0.footer
    }

    /// Get property block.
    #[inline]
    pub fn props(&self) -> &ftspb::DedFilePropBlock {
        &self.0.props
    }

    /// Build a Tantivy reader backed by this dedicated file.
    #[inline]
    pub async fn cached_read_index(&self) -> Result<Arc<clara_fts::IndexReader>> {
        self.0
            .cache
            .get_async(CacheKeyRef::DedicatedIndex { file_id: self.id() }, async {
                let (bytes_dir, mmap_guards) = self.load_tantivy_directory().await?;
                let reader = Arc::new(clara_fts::IndexReader::new(bytes_dir)?);
                Ok(FtsCacheValue::with_mmap_guards(reader, mmap_guards))
            })
            .await
    }

    async fn load_tantivy_directory(&self) -> Result<(BytesDirRO, Vec<FileMmapGuard>)> {
        let props = self.props();
        let footer = self.footer();
        let data_off = footer.data_block_offset;
        let layout = props.get_tantivy_layout();

        let load = |off: u64, size: u64| async move {
            if size == 0 {
                return Ok::<_, anyhow::Error>((Bytes::new(), Option::<FileMmapGuard>::None));
            }
            let (b, guard) = self
                .file()
                .mmap_range(data_off + off, size as usize)
                .await?;
            Ok::<_, anyhow::Error>((b, Some(guard)))
        };

        let (
            (meta_json, meta_guard),
            (managed_json, managed_guard),
            (term, term_guard),
            (idx, idx_guard),
            (pos, pos_guard),
            (store, store_guard),
            (fast, fast_guard),
            (fieldnorm, fieldnorm_guard),
        ) = tokio::try_join!(
            load(layout.get_meta().offset, layout.get_meta().size),
            load(layout.get_managed().offset, layout.get_managed().size),
            load(layout.get_term().offset, layout.get_term().size),
            load(layout.get_idx().offset, layout.get_idx().size),
            load(layout.get_pos().offset, layout.get_pos().size),
            load(layout.get_store().offset, layout.get_store().size),
            load(layout.get_fast().offset, layout.get_fast().size),
            load(layout.get_fieldnorm().offset, layout.get_fieldnorm().size),
        )?;

        let mut mmap_guards = Vec::new();
        for guard in [
            meta_guard,
            managed_guard,
            term_guard,
            idx_guard,
            pos_guard,
            store_guard,
            fast_guard,
            fieldnorm_guard,
        ] {
            mmap_guards.extend(guard);
        }

        Ok((
            BytesDirRO {
                meta_json,
                managed_json,
                term,
                idx,
                pos,
                store,
                fast,
                fieldnorm,
            },
            mmap_guards,
        ))
    }

    /// Load handle block from disk with checksum verification
    async fn load_hblock_accessor(
        &self,
        hblock_idx: usize,
    ) -> Result<(HBlockAccessor<Pk>, FileMmapGuard)> {
        // Get handle index block
        let iblock = self.get_iblock()?;

        // Get Handle Block data
        if hblock_idx + 1 >= iblock.hblock_offsets.len() {
            bail!("Handle block index {} out of bounds", hblock_idx);
        }
        let block_offset = iblock.hblock_offsets[hblock_idx];

        // Calculate block end offset using N+1 offset array
        let block_end_offset = iblock.hblock_offsets[hblock_idx + 1];
        let block_len = block_end_offset - block_offset;

        let (data, guard) = self
            .0
            .file
            .mmap_range(block_offset, block_len as usize)
            .await?;

        // Use HBlockAccessor to search within the block
        // The Pk type parameter is already known from DedicatedFile<Pk>
        let block_accessor = HBlockAccessor::<Pk>::new(data)?;

        // Verify checksum if not already verified
        let need_checksum = {
            let checked_hblock = self.0.hblock_checked.read().unwrap();
            checked_hblock.is_empty() || !checked_hblock.get(hblock_idx).unwrap_or(&false)
        };

        if need_checksum {
            let checksum_type = self.0.footer.checksum_type;
            block_accessor.verify_checksum(checksum_type)?;

            // Mark this handle block as verified
            {
                let mut hblock_checked = self.0.hblock_checked.write().unwrap();
                if hblock_checked.is_empty() {
                    hblock_checked.resize(iblock.hblock_offsets.len(), false);
                }
                if hblock_idx < hblock_checked.len() {
                    hblock_checked[hblock_idx] = true;
                }
            }
        }

        Ok((block_accessor, guard))
    }

    /// Find the index of Handle Block that contains the given doc id,
    fn find_hblock_offset_docid(&self, doc_id: usize) -> Result<(usize, usize)> {
        let iblock = self.get_iblock()?;
        let docids = iblock.get_hblock_start_docid();
        let ranks = iblock.get_hblock_handle_rank();
        let stride = iblock.get_hblock_handle_rank_stride() as usize;

        let total = docids.last().copied().unwrap_or(0) as usize;
        if doc_id >= total {
            bail!("Doc id {} out of range (total {})", doc_id, total);
        }

        let rank_idx = doc_id / stride;
        let lo = ranks[rank_idx] as usize;
        let hi = ranks[rank_idx + 1] as usize;

        let block_idx = match hi.cmp(&lo) {
            std::cmp::Ordering::Less => {
                // Best-effort: this should not happen for a well-formed rank array.
                lo
            }
            std::cmp::Ordering::Equal => lo,
            std::cmp::Ordering::Greater if hi == lo + 1 => {
                // `hi` is the block that contains doc_id at the next rank boundary
                // (doc_id = (rank_idx+1) * stride), but the boundary between `lo`
                // and `hi` blocks may still fall *within* this stride window.
                //
                // When that happens, doc_id may belong to `hi` even though
                // `lo = rank[rank_idx]`. If we always return `lo`, it can lead to
                // out-of-bounds local doc IDs.
                let hi_start = docids[hi] as usize;
                if doc_id >= hi_start { hi } else { lo }
            }
            std::cmp::Ordering::Greater => {
                // Slow path
                let relative = docids[lo..=hi]
                    .partition_point(|v| (*v as usize) <= doc_id)
                    .saturating_sub(1);
                lo + relative
            }
        };
        let local_idx = doc_id - docids[block_idx] as usize;
        Ok((block_idx, local_idx))
    }

    /// Find the index of Handle Block that might contain the given primary key.
    fn find_hblock_offset(&self, pk_encoded: &[u8]) -> Result<Option<usize>> {
        let index_block = self.get_iblock()?;
        let start_keys = index_block.get_hblock_start_key();
        let pos = start_keys.partition_point(|key| key.as_slice() <= pk_encoded);
        if pos == 0 {
            Ok(None)
        } else {
            Ok(Some(pos - 1))
        }
    }

    async fn cached_hblock_at(&self, block_idx: usize) -> Result<Arc<HBlockAccessor<Pk>>> {
        self.0
            .cache
            .get_async(
                CacheKeyRef::DedicatedHBlock {
                    file_id: self.0.file.id(),
                    block_idx: block_idx as u64,
                },
                async {
                    let (accessor, guard) = self.load_hblock_accessor(block_idx).await?;
                    Ok(FtsCacheValue::with_mmap_guard(Arc::new(accessor), guard))
                },
            )
            .await
    }
}

/// A zero-copy accessor for reading Handle Block.
#[derive(Clone)]
pub(in crate::table::fts) struct HBlockAccessor<Pk: PkType> {
    data: Bytes,
    n_pk: u32,

    /// 'static because comes from `data`.
    /// MUST NOT expose to outside.
    unsafe_versions: &'static [u64],
    /// 'static because comes from `data`.
    /// MUST NOT expose to outside.
    unsafe_delete_marks: &'static [u8],
    /// 'static because comes from `data`.
    /// MUST NOT expose to outside.
    unsafe_pk_common_offsets: &'static [u64],

    pk_data: Bytes,

    _marker: std::marker::PhantomData<Pk>,
}

impl<Pk: PkType> HBlockAccessor<Pk> {
    /// Parse Handle Block binary layout
    /// Handle Block structure:
    /// u32    Number of Primary Keys
    /// ...    Align to 8 bytes
    /// [u64]  Versions (mmap mapped)
    /// [u8]   Delete Mark (mmap mapped)
    /// ...    Align to 8 bytes
    /// [u64]  Common Handle Offsets (only exists for Common Handle, mmap
    /// mapped, has n+1 items) ...    Align to 8 bytes
    /// ...    Primary Key Data Bytes (mmap mapped)
    /// u32    Checksum (checksum of all above fields)
    /// ...    Align to 8 bytes
    fn new(mut data: Bytes) -> Result<Self> {
        use crate::codecutil::next_aligned_offset;

        let original_data = data.clone();

        // 1. Read PK count (u32)
        let n_pk = data.get_u32_le();
        if n_pk == 0 {
            bail!("Handle block cannot have zero PKs");
        }

        // 2. Skip padding to align to 8 bytes
        let mut current_offset = 4; // u32 for n_pk
        let next_offset = next_aligned_offset(current_offset, 8);
        if next_offset > current_offset {
            data.advance(next_offset - current_offset);
            current_offset = next_offset;
        }

        // 3. Read versions array [u64] (mmap mapped)
        let versions_size = (n_pk as usize) * 8;
        let versions_data = data.try_get_first(versions_size)?;
        let versions = versions_data.try_as_slice::<u64>()?;
        current_offset += versions_size;

        // 4. Read delete marks array [u8] (mmap mapped)
        let delete_marks_size = n_pk as usize;
        let delete_marks_data = data.try_get_first(delete_marks_size)?;
        let delete_marks = delete_marks_data.as_ref();
        current_offset += delete_marks_size;

        // 5. Align to 8 bytes
        let next_offset = next_aligned_offset(current_offset, 8);
        if next_offset > current_offset {
            data.advance(next_offset - current_offset);
            current_offset = next_offset;
        }

        // 6. Handle Common Handle Offsets [u64] (only exists for Common Handle, mmap
        //    mapped, has n+1 items)
        let (pk_common_offsets, pk_data_size) = if !Pk::IS_INT {
            let offsets_size = ((n_pk + 1) as usize) * 8; // n_pk + 1 u64s
            let offsets_data = data.try_get_first(offsets_size)?;
            let offsets = offsets_data.try_as_slice::<u64>()?;
            current_offset += offsets_size;

            // 7. Align to 8 bytes
            let next_offset = next_aligned_offset(current_offset, 8);
            if next_offset > current_offset {
                data.advance(next_offset - current_offset);
                current_offset = next_offset;
                let _ = current_offset;
            }

            let pk_data_size = offsets[n_pk as usize] as usize;

            let offsets_static = unsafe { crate::util::extend_lifetime(offsets) };
            (offsets_static, pk_data_size)
        } else {
            // For integer PK, no offset array, directly calculate data size
            (&[] as &[u64], (n_pk as usize) * 8) // Integer PK: u64 * n_pk
        };

        // 8. Read Primary Key Data Bytes (mmap mapped)
        let pk_data = data.try_get_first(pk_data_size)?;
        let versions_static = unsafe { crate::util::extend_lifetime(versions) };
        let delete_marks_static = unsafe { crate::util::extend_lifetime(delete_marks) };

        Ok(Self {
            data: original_data,
            n_pk,
            unsafe_versions: versions_static,
            unsafe_delete_marks: delete_marks_static,
            unsafe_pk_common_offsets: pk_common_offsets,
            pk_data,

            _marker: std::marker::PhantomData,
        })
    }

    /// Returns the version at the given local primary key index.
    #[inline]
    pub fn version_at(&self, local_i: usize) -> u64 {
        // This is safe, because we copy the reference into a value
        self.unsafe_versions[local_i]
    }

    /// Returns the delete mark at the given local primary key index.
    #[inline]
    pub fn is_deleted_at(&self, local_i: usize) -> u8 {
        // This is safe, because we copy the reference into a value
        self.unsafe_delete_marks[local_i]
    }

    /// Returns the encoded primary key bytes at the given index.
    #[inline]
    pub fn encoded_pk_at(&self, local_i: usize) -> Bytes {
        // This `if` will be eliminated during compilation because TypeId::of()
        // is const.
        if std::any::TypeId::of::<Pk>() == std::any::TypeId::of::<IntPk>() {
            let start_offset = local_i * 8; // Each encoded i64 is exactly 8 bytes
            let end_offset = start_offset + 8;
            self.pk_data.slice(start_offset..end_offset)
        } else if std::any::TypeId::of::<Pk>() == std::any::TypeId::of::<CommonPk>() {
            let start_offset = self.unsafe_pk_common_offsets[local_i] as usize;
            let end_offset = self.unsafe_pk_common_offsets[local_i + 1] as usize;
            self.pk_data.slice(start_offset..end_offset)
        } else {
            panic!("Unsupported Pk type");
        }
    }

    /// Verify the checksum of this Handle Block
    fn verify_checksum(&self, checksum_type: ChecksumType) -> Result<()> {
        let mut data_clone = self.data.clone();

        // The block is aligned to 8 bytes. The layout is:
        // [content][checksum: 4 bytes][padding: 4 bytes]
        // We read the last 8 bytes to get the checksum_bytes.
        let checksum_bytes = data_clone.try_get_last(8)?;

        // The checksum_bytes contains the checksum in its first 4 bytes.
        let mut checksum_reader = &checksum_bytes[..4];
        let checksum = checksum_reader.get_u32_le();

        let calculated_checksum = checksum_type.checksum(&data_clone);

        if checksum != calculated_checksum {
            bail!(
                "Handle block checksum mismatch: calculated {:#08x}, stored {:#08x}",
                calculated_checksum,
                checksum
            );
        }

        Ok(())
    }
}
