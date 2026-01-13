// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashSet, convert::TryFrom, io::Write};

use anyhow::{Result, anyhow, bail};
use bytes::{BufMut, Bytes};
use hexhex::hex;
use kvenginepb::fts as ftspb;
use protobuf::Message;
use tidb_query_datatype::codec::table::{append_common_handle_row_key, append_row_key};
use xorf::BinaryFuse8;

use super::DedicatedFileFooter;
use crate::{
    codecutil::next_aligned_offset,
    table::{
        ChecksumType,
        fts::iter::{CommonPk, IntPk, PkType},
    },
};

// warning:
// bytemuck::cast_slice converts the data based on the platform's default
// big-endian and small-endian order. DedicatedFile is read in
// little-endian order, so we need to ensure that the system defaults to
// little-endian storage.
#[cfg(not(target_endian = "little"))]
compile_error!("DedicatedFile builder can't be compiled on a big-endian platform.");

/// Options for building a `DedicatedFile`.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct DedicatedFileBuilderOptions {
    /// Target size for Handle Blocks. Blocks may be larger if a single primary
    /// key's data causes the block to exceed this size. Default is 8 MiB.
    pub handle_block_size: usize,
    /// Checksum algorithm to use.
    pub checksum_type: ChecksumType,
    /// Number of handles grouped into a single rank entry for fast doc-id
    /// lookup. Must be greater than 0.
    pub handle_rank_stride: u32,
}

impl Default for DedicatedFileBuilderOptions {
    fn default() -> Self {
        Self {
            handle_block_size: 8 * 1024 * 1024, // 8 MiB
            checksum_type: ChecksumType::Crc32c,
            handle_rank_stride: 10_000,
        }
    }
}

/// Internal state for building a single Handle Block.
/// This struct organizes state and can be reused via the `clear` method.
#[derive(Default)]
struct CurrentHandleBlock {
    // First PK in the block, used for writing to the index block
    start_key: Vec<u8>,
    // Current size of data written to the block
    current_size: usize,

    // Flattened buffers for efficient PK data storage
    pk_data: Vec<u8>, // i64 or common PKs in their memory-comparable form
    // Contains n_pk + 1 elements. For an empty Handle Block, it always contains one element 0.
    // Each time a new PK is added, its end offset is appended.
    pk_common_offsets: Vec<u64>,

    // Other metadata for each PK
    versions: Vec<u64>,
    delete_marks: Vec<u8>,
    pk_count: u32,

    // State for checking order (encoded form for both IntPk and CommonPk)
    last_pk_encoded: Vec<u8>,
    last_version: u64,

    // Buffer for serializing the current Handle Block.
    serialized_buf: Vec<u8>,
}

impl CurrentHandleBlock {
    /// Reset state for a new Handle Block.
    fn clear(&mut self) {
        self.start_key.clear();
        self.current_size = 0;
        self.pk_data.clear();
        self.pk_common_offsets.clear();
        self.versions.clear();
        self.delete_marks.clear();
        self.pk_count = 0;
        self.last_pk_encoded.clear();
        self.last_version = 0;
        self.serialized_buf.clear();
    }
}

trait WrittenSize {
    /// Returns the size of data written so far.
    fn written_size(&self) -> usize;
}

impl WrittenSize for Vec<u8> {
    fn written_size(&self) -> usize {
        self.len()
    }
}

/// Builder for creating FTS DedicatedFile.
///
/// Users are responsible for providing data in the correct order:
/// 1. Primary keys (PK) must be added in ascending order.
/// 2. For a given PK, versions must be added in descending order.
///
/// The builder checks this order and returns an error if violated.
/// It buffers at most one Handle Block's worth of data to minimize memory
/// usage.
pub struct DedicatedFileBuilder<W: Write, Pk: PkType> {
    writer: W,
    options: DedicatedFileBuilderOptions,
    table_id: i64,

    // Global file state
    offset: u64,
    footer: DedicatedFileFooter,
    iblock: ftspb::DedFileIBlock,
    pk_hashes: HashSet<u64>, // For building the final PK filter (deduplicated)
    props: ftspb::DedFilePropBlock,

    // For tracking smallest and biggest row keys across all handles
    smallest_key: Vec<u8>,
    biggest_key: Vec<u8>,

    // The current row key for calculating smallest/biggest row keys.
    current_row_key: Vec<u8>,

    // State of the currently building Handle Block
    this_handle_block: CurrentHandleBlock,

    _marker: std::marker::PhantomData<Pk>,
}

/// Summary describing the DedicatedFile produced by a builder.
#[derive(Clone, Debug)]
pub struct DedicatedFileBuildSummary {
    /// Total bytes written to the underlying writer (including footer).
    pub written_bytes: u64,
    /// Offset of the metadata section within the file.
    pub meta_offset: u32,
    /// Property block generated for the dedicated file.
    pub props: ftspb::DedFilePropBlock,
}

impl<W: Write, Pk: PkType> DedicatedFileBuilder<W, Pk> {
    /// Create a new `DedicatedFileBuilder`.
    pub fn new(
        writer: W,
        options: DedicatedFileBuilderOptions,
        table_id: i64,
        index_id: i64,
        lp_key: &[u8],
    ) -> Result<Self> {
        // Check if LP key is empty as soon as it's provided
        if lp_key.is_empty() {
            bail!("Logical partition key cannot be empty");
        }
        if options.handle_rank_stride == 0 {
            bail!("handle_rank_stride must be greater than 0");
        }

        let mut footer = DedicatedFileFooter::new();
        footer.checksum_type = options.checksum_type;

        let mut props = ftspb::DedFilePropBlock::new();
        props.set_is_int_handle(Pk::IS_INT);
        props.set_lp_key(lp_key.to_vec());
        props.set_table_id(table_id);
        props.set_index_id(index_id);

        let mut iblock = ftspb::DedFileIBlock::new();
        iblock.hblock_start_docid.push(0);

        Ok(Self {
            writer,
            options,
            table_id,
            offset: 0,
            footer,
            iblock,
            pk_hashes: HashSet::new(),
            props,
            smallest_key: Vec::new(),
            biggest_key: Vec::new(),
            current_row_key: Vec::new(),
            this_handle_block: CurrentHandleBlock::default(),
            _marker: std::marker::PhantomData,
        })
    }

    fn update_row_key_bounds(
        row_key: &[u8],
        smallest_key: &mut Vec<u8>,
        biggest_key: &mut Vec<u8>,
    ) {
        if smallest_key.is_empty() || row_key < smallest_key.as_slice() {
            smallest_key.clear();
            smallest_key.extend_from_slice(row_key);
        }
        if biggest_key.is_empty() || row_key > biggest_key.as_slice() {
            biggest_key.clear();
            biggest_key.extend_from_slice(row_key);
        }
    }

    /// Internal method to add an encoded PK with version and delete mark.
    /// This is the common implementation for both IntPk and CommonPk.
    fn add_pk_encoded_(&mut self, pk_encoded: &[u8], version: u64, is_delete: bool) -> Result<()> {
        if pk_encoded.is_empty() {
            bail!("PK cannot be empty.");
        }

        // A handle block can only be flushed after all versions of its last PK
        // have been added. We only know a PK is complete when we observe a new PK
        // is about to be written.
        //
        // Therefore, flush the current handle block *before* writing a new PK,
        // so all versions of the new PK stay in the same block.
        if self.this_handle_block.pk_count > 0 {
            let last_pk_encoded = self.this_handle_block.last_pk_encoded.as_slice();

            // Check ordering constraints.
            if pk_encoded < last_pk_encoded {
                bail!(
                    "PKs must be added in ascending order. Previous: {}, Current: {}",
                    hex(&last_pk_encoded),
                    hex(pk_encoded)
                );
            }
            if pk_encoded == last_pk_encoded && version >= self.this_handle_block.last_version {
                bail!("Versions for the same PK must be added in descending order.");
            }

            let is_new_pk = pk_encoded != last_pk_encoded;
            if is_new_pk && self.this_handle_block.current_size >= self.options.handle_block_size {
                self.finish_handle_block()?;
            }
        }

        // First PK in the (possibly new) block, record it as start_key.
        if self.this_handle_block.pk_count == 0 {
            self.this_handle_block
                .start_key
                .extend_from_slice(pk_encoded);
        }

        let block = &mut self.this_handle_block;

        // Append PK data to the block
        if !Pk::IS_INT {
            block
                .pk_common_offsets
                .push(u64::try_from(block.pk_data.len())?);
            block.current_size += 8;
        }
        block.pk_data.extend_from_slice(pk_encoded);
        block.current_size += pk_encoded.len();

        // Store version and delete_mark
        block.versions.push(version);
        block.delete_marks.push(is_delete as u8);
        block.current_size += 8 + 1;

        block.pk_count = block
            .pk_count
            .checked_add(1)
            .ok_or_else(|| anyhow!("Too many PKs in a single Handle Block"))?;
        self.props.pk_total = self
            .props
            .pk_total
            .checked_add(1)
            .ok_or_else(|| anyhow!("Too many PK versions in DedicatedFile"))?;

        let is_new_pk = block.pk_count == 1 || pk_encoded != block.last_pk_encoded.as_slice();
        if is_new_pk && !is_delete {
            self.props.pk_live_unique = self
                .props
                .pk_live_unique
                .checked_add(1)
                .ok_or_else(|| anyhow!("Too many live PKs in DedicatedFile"))?;
        }

        self.pk_hashes.insert(farmhash::fingerprint64(pk_encoded));

        // Update the state for the next call's order check.
        block.last_pk_encoded.clear();
        block.last_pk_encoded.extend_from_slice(pk_encoded);
        block.last_version = version;

        Ok(())
    }

    fn add_pk_int(&mut self, pk: i64, version: u64, is_deleted: bool) -> Result<()> {
        let pk_encoded = IntPk::encode(pk);
        self.add_pk_encoded_(&pk_encoded, version, is_deleted)?;
        self.current_row_key.clear();
        append_row_key(&mut self.current_row_key, self.table_id, pk)?;
        Self::update_row_key_bounds(
            &self.current_row_key,
            &mut self.smallest_key,
            &mut self.biggest_key,
        );
        Ok(())
    }

    fn add_pk_common(&mut self, pk: &[u8], version: u64, is_deleted: bool) -> Result<()> {
        if pk.is_empty() {
            bail!("Common PK cannot be empty.");
        }
        self.add_pk_encoded_(pk, version, is_deleted)?;
        self.current_row_key.clear();
        append_common_handle_row_key(&mut self.current_row_key, self.table_id, pk)?;
        Self::update_row_key_bounds(
            &self.current_row_key,
            &mut self.smallest_key,
            &mut self.biggest_key,
        );
        Ok(())
    }

    #[inline]
    pub fn add_pk(&mut self, pk: Pk::T<'_>, version: u64, is_deleted: bool) -> Result<()> {
        // This `if` will be eliminated during compilation because TypeId::of()
        // is const.
        if std::any::TypeId::of::<Pk>() == std::any::TypeId::of::<IntPk>() {
            debug_assert_eq!(
                std::any::TypeId::of::<Pk::T<'_>>(),
                std::any::TypeId::of::<i64>()
            );
            let pk_val = unsafe { *(&pk as *const Pk::T<'_> as *const i64) };
            self.add_pk_int(pk_val, version, is_deleted)
        } else if std::any::TypeId::of::<Pk>() == std::any::TypeId::of::<CommonPk>() {
            debug_assert_eq!(
                std::any::TypeId::of::<Pk::T<'_>>(),
                std::any::TypeId::of::<&[u8]>()
            );
            let pk_val = unsafe { *(&pk as *const Pk::T<'_> as *const &[u8]) };
            self.add_pk_common(pk_val, version, is_deleted)
        } else {
            panic!("Unsupported Pk type");
        }
    }

    /// Serialize and write the current Handle Block.
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
    fn finish_handle_block(&mut self) -> Result<()> {
        if self.this_handle_block.pk_count == 0 {
            return Ok(()); // No data, no need to write
        }

        let block = &mut self.this_handle_block;
        let buf = &mut block.serialized_buf;
        buf.clear();

        // 1. PK count (u32)
        buf.put_u32_le(block.pk_count);

        // 2. Align to 8 bytes
        Self::write_align_padding(buf, 8);

        // 3. Versions array [u64] (mmap mapped)
        buf.put_slice(bytemuck::cast_slice(&block.versions));

        // 4. Delete marks array [u8] (mmap mapped)
        buf.put_slice(&block.delete_marks);

        // 5. Align to 8 bytes
        Self::write_align_padding(buf, 8);

        // 6. Common PK offsets array [u64] (only exists for Common Handle, mmap mapped,
        //    has n+1 items)
        if !Pk::IS_INT {
            block
                .pk_common_offsets
                .push(u64::try_from(block.pk_data.len())?);
            buf.put_slice(bytemuck::cast_slice(&block.pk_common_offsets));
            // 7. Align to 8 bytes
            Self::write_align_padding(buf, 8);
        }

        // 8. PK data bytes (mmap mapped)
        buf.put_slice(&block.pk_data);
        Self::write_align_padding(buf, 8);

        // 9. Checksum (u32) - checksum of all above fields
        let checksum = self.options.checksum_type.checksum(buf);
        buf.put_u32_le(checksum);

        // 10. Align to 8 bytes
        Self::write_align_padding(buf, 8);

        // --- Write Handle Block to writer ---
        self.iblock.hblock_start_key.push(block.start_key.clone());
        self.iblock.hblock_offsets.push(self.offset);

        self.writer.write_all(buf)?;
        self.offset += buf.len() as u64;

        // Clear current block state for the next block
        block.clear();

        let pk_total = u32::try_from(self.props.pk_total).map_err(|_| {
            anyhow!(
                "Too many PKs in DedicatedFile (pk total {} exceeds u32::MAX)",
                self.props.pk_total
            )
        })?;
        self.iblock.hblock_start_docid.push(pk_total);

        Ok(())
    }

    fn build_handle_rank(&mut self) -> Result<()> {
        let stride = self.options.handle_rank_stride;
        self.iblock.set_hblock_handle_rank_stride(stride);
        self.iblock.clear_hblock_handle_rank();
        let total_pk = usize::try_from(self.props.pk_total).map_err(|_| {
            anyhow!(
                "pk_total {} exceeds usize::MAX while building handle rank",
                self.props.pk_total
            )
        })?;
        if total_pk == 0 {
            self.iblock.hblock_handle_rank.push(0);
            return Ok(());
        }

        let stride_usize = stride as usize;
        let docids = self.iblock.hblock_start_docid.clone();
        let block_count = self.iblock.hblock_start_key.len();
        debug_assert_eq!(docids.len(), block_count + 1);

        let mut doc_idx = 0usize;
        {
            let rank = self.iblock.mut_hblock_handle_rank();
            while doc_idx < total_pk {
                let block_idx = docids
                    .partition_point(|&v| (v as usize) <= doc_idx)
                    .saturating_sub(1)
                    .min(docids.len().saturating_sub(2));
                rank.push(block_idx as u32);
                doc_idx = doc_idx.saturating_add(stride_usize);
            }
            rank.push(block_count as u32);
        }
        Ok(())
    }

    /// Complete the build process, writing all remaining buffered data,
    /// metadata blocks, and footer. Returns the build summary.
    pub fn finish<D: tantivy::Directory + Clone>(
        mut self,
        tantivy_dir: &clara_fts::TrackedDirectory<D>,
    ) -> Result<DedicatedFileBuildSummary> {
        // Check if there's at least one PK
        if self.pk_hashes.is_empty() {
            bail!("At least one primary key is required to build DedicatedFile");
        }

        // Flush the last Handle Block (if it has data)
        self.finish_handle_block()?;

        // Add the final offset to ensure N+1 length.
        self.iblock.hblock_offsets.push(self.offset);
        self.build_handle_rank()?;

        // --- Write Data Block (containing all Tantivy files) ---
        let data_block_start_offset = self.offset;

        // Set data block offset in footer
        self.footer.data_block_offset = data_block_start_offset;

        let mut write_sub_file = |b: Bytes| -> Result<ftspb::OffsetSize> {
            if b.is_empty() {
                return Ok(ftspb::OffsetSize::new());
            }

            let mut range = ftspb::OffsetSize::new();
            range.set_offset(self.offset - data_block_start_offset);
            range.set_size(b.len() as u64);

            self.writer.write_all(&b)?;
            self.offset += b.len() as u64;

            let padding = [0u8; 8];
            let pad_len = next_aligned_offset(self.offset as usize, 8) - (self.offset as usize);
            self.writer.write_all(&padding[..pad_len])?;
            self.offset += pad_len as u64;

            Ok(range)
        };

        let dir = super::BytesDirRO::from_directory(tantivy_dir)?;
        let mut layout = ftspb::TantivyIndexLayout::new();
        layout.set_meta(write_sub_file(dir.meta_json)?);
        layout.set_managed(write_sub_file(dir.managed_json)?);
        layout.set_term(write_sub_file(dir.term)?);
        layout.set_idx(write_sub_file(dir.idx)?);
        layout.set_pos(write_sub_file(dir.pos)?);
        layout.set_store(write_sub_file(dir.store)?);
        layout.set_fast(write_sub_file(dir.fast)?);
        layout.set_fieldnorm(write_sub_file(dir.fieldnorm)?);
        self.props.set_tantivy_layout(layout);

        // --- Write metadata blocks ---
        let mut meta_checksum = 0;

        // Write Handle Index Block
        self.footer.iblock_offset = self.offset;
        let index_data = self.iblock.write_to_bytes()?;
        self.writer.write_all(&index_data)?;
        self.offset += index_data.len() as u64;
        meta_checksum = self
            .options
            .checksum_type
            .append(meta_checksum, &index_data);

        // Write PK Filter Block
        self.footer.pk_filter_block_offset = self.offset;
        let pk_hashes_vec: Vec<u64> = self.pk_hashes.iter().copied().collect();
        let pk_filter = BinaryFuse8::try_from(&pk_hashes_vec)
            .map_err(|e| anyhow!("Failed to build PK filter: {}", e))?;
        let filter_data = pk_filter.to_vec();
        self.writer.write_all(&filter_data)?;
        self.offset += filter_data.len() as u64;
        meta_checksum = self
            .options
            .checksum_type
            .append(meta_checksum, &filter_data);

        // Write Property Block
        // Set smallest and biggest row keys in properties (like vector index)
        if !self.smallest_key.is_empty() {
            self.props
                .set_smallest_key(std::mem::take(&mut self.smallest_key));
        }
        if !self.biggest_key.is_empty() {
            self.props
                .set_biggest_key(std::mem::take(&mut self.biggest_key));
        }

        self.footer.prop_offset = self.offset;
        let props_data = self.props.write_to_bytes()?;
        self.writer.write_all(&props_data)?;
        self.offset += props_data.len() as u64;
        meta_checksum = self
            .options
            .checksum_type
            .append(meta_checksum, &props_data);

        // --- Write Footer ---
        self.footer.checksum_other_meta = meta_checksum;
        let n = self.footer.marshal(&mut self.writer)?;
        self.offset += n as u64;

        Ok(DedicatedFileBuildSummary {
            written_bytes: self.offset,
            meta_offset: self.footer.metadata_offset() as u32,
            props: std::mem::take(&mut self.props),
        })
    }

    /// Append zero padding to buffer to meet alignment requirements.
    fn write_align_padding<B: BufMut + WrittenSize>(buf: &mut B, align: usize) {
        let written = buf.written_size();
        let next_offset = next_aligned_offset(written, align);
        if next_offset > written {
            buf.put_bytes(0, next_offset - written);
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;
    use crate::table::fts::dedicated_file::test::dummy_tantivy_dir;

    /// Create a test DedicatedFileBuilder for IntPk
    fn create_int_builder() -> DedicatedFileBuilder<Vec<u8>, IntPk> {
        let lp_key = "test_lp";
        let writer = Vec::new();
        let options = DedicatedFileBuilderOptions {
            handle_block_size: 1024 * 1024, // 1 MiB
            ..Default::default()
        };
        DedicatedFileBuilder::new(writer, options, 1, 0, lp_key.as_bytes()).unwrap()
    }

    /// Create a test DedicatedFileBuilder for CommonPk
    fn create_common_builder() -> DedicatedFileBuilder<Vec<u8>, CommonPk> {
        let lp_key = "test_lp";
        let writer = Vec::new();
        let options = DedicatedFileBuilderOptions {
            handle_block_size: 1024 * 1024, // 1 MiB
            ..Default::default()
        };
        DedicatedFileBuilder::new(writer, options, 1, 0, lp_key.as_bytes()).unwrap()
    }

    // ==================== Error handling tests ====================

    #[test]
    fn test_builder_same_int_handle_in_one_handle_block() -> Result<()> {
        let lp_key = "test_lp";
        let writer = Vec::new();
        let options = DedicatedFileBuilderOptions {
            handle_block_size: 1024, // 1 KB
            ..Default::default()
        };
        let mut builder: DedicatedFileBuilder<_, IntPk> =
            DedicatedFileBuilder::new(writer, options, 1, 0, lp_key.as_bytes())?;

        for i in 0..1000 {
            builder.add_pk(100, 2000 - i as u64, false)?;
        }

        builder.finish_handle_block()?;

        // same handle should be in one handle block.
        assert_eq!(builder.iblock.hblock_start_key.len(), 1);
        assert_eq!(builder.iblock.hblock_start_docid.len(), 2);
        assert_eq!(builder.iblock.hblock_start_docid[0], 0);
        assert_eq!(
            builder.iblock.hblock_start_docid[1],
            u32::try_from(builder.props.pk_total).unwrap()
        );

        Ok(())
    }

    #[test]
    fn test_builder_same_common_handle_in_one_handle_block() -> Result<()> {
        let lp_key = "test_lp";
        let writer = Vec::new();
        let options = DedicatedFileBuilderOptions {
            handle_block_size: 1024, // 1 KB
            ..Default::default()
        };
        let mut builder: DedicatedFileBuilder<_, CommonPk> =
            DedicatedFileBuilder::new(writer, options, 1, 0, lp_key.as_bytes())?;

        for i in 0..1000 {
            builder.add_pk("common_bytes".as_bytes(), 2000 - i as u64, false)?;
        }

        builder.finish_handle_block()?;

        // same handle should be in one handle block.
        assert_eq!(builder.iblock.hblock_start_key.len(), 1);
        assert_eq!(builder.iblock.hblock_start_docid.len(), 2);
        assert_eq!(builder.iblock.hblock_start_docid[0], 0);
        assert_eq!(
            builder.iblock.hblock_start_docid[1],
            u32::try_from(builder.props.pk_total).unwrap()
        );

        Ok(())
    }

    #[test]
    fn test_builder_error_empty_common_pk() -> Result<()> {
        let mut builder = create_common_builder();

        // Trying to add empty common PK should fail
        let result = builder.add_pk(b"", 100, false);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Common PK cannot be empty")
        );
        Ok(())
    }

    #[test]
    fn test_builder_add_int_pk() -> Result<()> {
        let mut builder = create_int_builder();

        builder.add_pk(-100, 100, false)?;
        builder.add_pk(100, 100, false)?;
        Ok(())
    }

    #[test]
    fn test_builder_error_pk_order_int() -> Result<()> {
        let mut builder = create_int_builder();

        // Add first PK
        builder.add_pk(200, 100, false).unwrap();

        // Trying to add smaller PK should fail
        let result = builder.add_pk(100, 99, false);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("PKs must be added in ascending order")
        );
        Ok(())
    }

    #[test]
    fn test_builder_error_pk_order_common() -> Result<()> {
        let mut builder = create_common_builder();

        // Add first PK (larger in byte order)
        builder.add_pk(b"zebra", 100, false).unwrap();

        // Trying to add smaller PK in byte order should fail
        let result = builder.add_pk(b"apple", 99, false);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("PKs must be added in ascending order")
        );
        Ok(())
    }

    #[test]
    fn test_builder_error_version_order() -> Result<()> {
        let mut builder = create_int_builder();

        // Add first version
        builder.add_pk(100, 200, false).unwrap();

        // Trying to add higher version for same PK should fail
        let result = builder.add_pk(100, 300, false);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Versions for the same PK must be added in descending order")
        );
        Ok(())
    }

    #[test]
    fn test_builder_error_finish_without_pks() -> Result<()> {
        let builder = create_int_builder();

        // Trying to finish build without adding any PKs should fail
        let result = builder.finish(&clara_fts::TrackedDirectory::wrap(
            tantivy::directory::RamDirectory::default(),
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("At least one primary key is required to build DedicatedFile")
        );

        Ok(())
    }

    #[test]
    fn test_builder_error_empty_lp_key() {
        let lp_key = b""; // Empty LP key
        let writer = Vec::new();
        let options = DedicatedFileBuilderOptions {
            handle_block_size: 1024 * 1024,
            ..Default::default()
        };
        let res: Result<DedicatedFileBuilder<_, IntPk>> =
            DedicatedFileBuilder::new(writer, options, 1, 0, lp_key);
        assert!(res.is_err());

        if let Err(err) = res {
            assert!(
                err.to_string()
                    .contains("Logical partition key cannot be empty")
            );
        }
    }

    #[test]
    fn test_dedicated_file_pk_stats() -> Result<()> {
        let lp_key = b"lp";
        let writer = Vec::new();
        let options = DedicatedFileBuilderOptions::default();
        let mut builder: DedicatedFileBuilder<_, IntPk> =
            DedicatedFileBuilder::new(writer, options, 1, 0, lp_key)?;

        builder.add_pk(1, 10, false)?;
        builder.add_pk(1, 9, true)?;
        builder.add_pk(2, 8, true)?;
        builder.add_pk(3, 7, false)?;

        let summary = builder.finish(&dummy_tantivy_dir())?;
        assert_eq!(summary.props.pk_total, 4);
        assert_eq!(summary.props.pk_live_unique, 2);
        Ok(())
    }
}
