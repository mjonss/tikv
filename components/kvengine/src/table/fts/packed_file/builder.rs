// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{convert::TryFrom, io::Write};

use anyhow::{Result, anyhow, bail};
use bytes::{BufMut, Bytes};
use clara_fts::TrackedDirectory;
use collections::HashSet;
use hexhex::hex;
use kvenginepb::fts as ftspb;
use protobuf::Message;
use tidb_query_datatype::codec::table::{append_common_handle_row_key, append_row_key};
use xorf::BinaryFuse8;

use super::PackedFileFooter;
use crate::{
    codecutil::next_aligned_offset,
    table::{
        ChecksumType, SnapVersion,
        fts::{CommonPk, IntPk, PkType, dedicated_file::BytesDirRO},
    },
};

#[cfg(not(target_endian = "little"))]
compile_error!("PackedFile builder can't be compiled on a big-endian platform.");

/// Options for building a `PackedFile`.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct PackedFileBuilderOptions {
    /// The target size of data blocks. A block can be larger if a single
    /// logical partition exceeds this size. Defaults to 64 KiB.
    pub block_size: usize,
    /// The checksum algorithm to use.
    pub checksum_type: ChecksumType,
}

impl Default for PackedFileBuilderOptions {
    fn default() -> Self {
        Self {
            block_size: 64 * 1024, // 64 KiB
            checksum_type: ChecksumType::Crc32c,
        }
    }
}

/// Holds the internal state for building a single logical partition.
/// This struct is designed to group state and be reusable via `reset`.
#[derive(Default)]
struct CurrentLp {
    is_touched: bool,
    is_int_pk: bool,
    lp_key: Vec<u8>,
    table_id: i64,
    index_id: i64,

    // Data for building the PK filter
    // XORF does not support duplicate hashes, so we need to ensure uniqueness.
    unique_pk_hashes: HashSet<u64>,

    // PK data stored in efficient flat buffers. i64 PKs in memory-comparable form.
    pk_data: Vec<u8>,
    // n_pk + 1 elements. For a blank LP it always has one element, which is always 0.
    // For each new PK, the end offset is added.
    pk_common_offsets: Vec<u32>,

    // Other per-PK metadata
    versions: Vec<u64>,
    delete_marks: Vec<u8>,
    pk_count: u32,
    live_unique_count: u64,

    // State for order checking (encoded form for both IntPk and CommonPk)
    last_pk_encoded: Vec<u8>,
    last_version: u64,

    // Serialized data for the current LP.
    serialized_buf: Vec<u8>,

    // A buf used when building the PK filter
    pk_hashes_buf: Vec<u64>,
}

impl CurrentLp {
    /// Resets the state for a new logical partition.
    fn clear(&mut self) {
        self.is_touched = false;
        self.is_int_pk = false;
        self.lp_key.clear();
        self.table_id = 0;
        self.index_id = 0;
        self.unique_pk_hashes.clear();
        self.pk_data.clear();
        self.pk_common_offsets.clear();
        self.versions.clear();
        self.delete_marks.clear();
        self.pk_count = 0;
        self.live_unique_count = 0;
        self.last_pk_encoded.clear();
        self.last_version = 0;
        self.serialized_buf.clear();
    }
}

#[derive(Default)]
struct CurrentDataBlock {
    offset: u64,
    checksum: u32,

    // n_entries + 1 elements. For a blank data block it always have one element,
    // which is the offset of the first entry (0).
    // For each new entry, the end offset is added.
    entry_offsets: Vec<u32>,

    /// A buffer to be reused for serializing data block metadata.
    meta_ser_buf: Vec<u8>,
}

impl CurrentDataBlock {
    /// Resets the state for a new data block.
    fn clear(&mut self) {
        self.offset = 0;
        self.checksum = 0;
        self.entry_offsets.clear();
        self.meta_ser_buf.clear();
    }
}

trait WrittenSize {
    /// Returns the size of the data that has been written so far.
    fn written_size(&self) -> usize;
}

impl WrittenSize for Vec<u8> {
    fn written_size(&self) -> usize {
        self.len()
    }
}

/// A builder for creating FTS PackedFiles.
///
/// The user is responsible for feeding data in the correct order:
/// 1. Logical partitions must be added in ascending order of their keys.
/// 2. Within an LP, primary keys must be added in ascending order.
/// 3. For a given PK, versions must be added in descending order.
///
/// The builder checks for this order and returns an error if it's violated.
/// It buffers at most one LP's data to minimize memory usage.
pub struct PackedFileBuilder<W: Write> {
    writer: W,
    options: PackedFileBuilderOptions,

    // Global file state
    offset: u64,
    footer: PackedFileFooter,
    index_block: ftspb::PackedFileIndexBlock,
    lp_key_hashes: Vec<u64>, // For building the final LP filter
    props: ftspb::PackedFilePropBlock,

    // For tracking all table IDs in this file (deduplicated)
    table_ids: HashSet<i64>,

    // For tracking smallest and biggest row keys across all tables/LPs
    smallest_key: Vec<u8>,
    biggest_key: Vec<u8>,
    smallest_table_index: Option<(i64, i64)>,
    largest_table_index: Option<(i64, i64)>,

    // For checking order of LP keys
    last_lp_key: Vec<u8>,

    // The current row key for calculating smallest/biggest row keys for the file.
    current_row_key: Vec<u8>,

    // State for the current data block being built
    this_data_block: CurrentDataBlock,

    // State for the current logical partition being built
    this_lp: CurrentLp,
}

/// Summary of the packed file written by [`PackedFileBuilder::finish`].
#[derive(Debug)]
pub struct PackedFileBuildSummary {
    /// Total bytes written to the underlying writer (including footer).
    pub written_bytes: u64,
    /// Offset of the metadata section (index block) within the file.
    pub meta_offset: u32,
    /// Property block generated for the packed file.
    pub props: ftspb::PackedFilePropBlock,
}

impl<W: Write> PackedFileBuilder<W> {
    /// Creates a new `PackedFileBuilder`.
    pub fn new(writer: W, options: PackedFileBuilderOptions) -> Self {
        let mut footer = PackedFileFooter::new();
        footer.checksum_type = options.checksum_type;

        Self {
            writer,
            options,
            offset: 0,
            footer,
            index_block: ftspb::PackedFileIndexBlock::new(),
            lp_key_hashes: Vec::new(),
            props: ftspb::PackedFilePropBlock::new(),
            table_ids: HashSet::default(),
            smallest_key: Vec::new(),
            biggest_key: Vec::new(),
            smallest_table_index: None,
            largest_table_index: None,
            last_lp_key: Vec::new(),
            current_row_key: Vec::new(),
            this_data_block: CurrentDataBlock::default(),
            this_lp: CurrentLp::default(),
        }
    }

    /// Starts building a new logical partition with the given key.
    /// This will automatically finalize the previous LP if one was in progress.
    pub fn start_lp(
        &mut self,
        table_id: i64,
        index_id: i64,
        is_int_pk: bool,
        lp_key: &[u8],
    ) -> Result<()> {
        if self.this_lp.is_touched {
            bail!("Must call finish_lp() before starting a new one.");
        }
        if lp_key.is_empty() {
            bail!("Logical partition key cannot be empty.");
        }
        if !self.last_lp_key.is_empty() && lp_key <= self.last_lp_key.as_slice() {
            bail!(
                "LP keys must be added in ascending order. Last: {}, new: {}",
                hex(&self.last_lp_key),
                hex(lp_key)
            );
        }

        self.this_lp.is_touched = true;
        self.this_lp.is_int_pk = is_int_pk;
        self.this_lp.lp_key.extend_from_slice(lp_key);
        self.this_lp.table_id = table_id;
        self.this_lp.index_id = index_id;
        self.this_lp.pk_common_offsets.push(0); // The first offset is always 0.

        // Track this table_id for efficient has_table() lookups
        self.table_ids.insert(table_id);

        Ok(())
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

    /// Internal helper to add an encoded PK with common logic.
    /// This is called by both add_pk_int and add_pk_common after their
    /// type-specific checks.
    fn add_pk_encoded_(&mut self, pk_encoded: &[u8], version: u64, is_delete: bool) -> Result<()> {
        // Check ordering constraints using encoded form (BEFORE any state
        // modifications)
        if self.this_lp.pk_count > 0 {
            let last_pk_encoded = self.this_lp.last_pk_encoded.as_slice();
            if pk_encoded < last_pk_encoded {
                bail!("PKs must be in ascending order.");
            }
            if pk_encoded == last_pk_encoded && version >= self.this_lp.last_version {
                bail!("Versions for the same PK must be in descending order.");
            }
        }

        let is_new_pk =
            self.this_lp.pk_count == 0 || pk_encoded != self.this_lp.last_pk_encoded.as_slice();
        if is_new_pk && !is_delete {
            self.this_lp.live_unique_count = self
                .this_lp
                .live_unique_count
                .checked_add(1)
                .ok_or_else(|| anyhow!("Too many live PKs in a single LP"))?;
        }

        // Update props
        self.props.total_keys_bytes += pk_encoded.len() as u32;
        self.props.pk_total += 1;

        // Append PK data to the appropriate buffer
        self.this_lp.pk_data.extend_from_slice(pk_encoded);
        if !self.this_lp.is_int_pk {
            self.this_lp
                .pk_common_offsets
                .push(u32::try_from(self.this_lp.pk_data.len())?);
        }

        // Common metadata
        self.this_lp.versions.push(version);
        self.this_lp.delete_marks.push(is_delete as u8);
        if is_new_pk {
            self.this_lp
                .unique_pk_hashes
                .insert(farmhash::fingerprint64(pk_encoded));
        }

        self.this_lp.pk_count = self
            .this_lp
            .pk_count
            .checked_add(1)
            .ok_or_else(|| anyhow!("Too many PKs in a single LP"))?;

        // Update last PK tracking (encoded form)
        self.this_lp.last_pk_encoded.clear();
        self.this_lp.last_pk_encoded.extend_from_slice(pk_encoded);
        self.this_lp.last_version = version;

        Ok(())
    }

    #[inline]
    pub fn add_pk<Pk: PkType>(
        &mut self,
        pk: Pk::T<'_>,
        version: u64,
        is_deleted: bool,
    ) -> Result<()> {
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

    /// Adds a primary key to the current logical partition (for int PK).
    pub fn add_pk_int(&mut self, pk: i64, version: u64, is_deleted: bool) -> Result<()> {
        if !self.this_lp.is_touched {
            bail!("Must call start_lp() before adding PKs.");
        }
        if !self.this_lp.is_int_pk {
            bail!("Expected int PK, but current LP is set for common PKs.");
        }

        let pk_encoded = crate::table::fts::iter::IntPk::encode(pk);
        self.add_pk_encoded_(&pk_encoded, version, is_deleted)?;

        self.current_row_key.clear();
        append_row_key(&mut self.current_row_key, self.this_lp.table_id, pk)?;
        Self::update_row_key_bounds(
            &self.current_row_key,
            &mut self.smallest_key,
            &mut self.biggest_key,
        );

        Ok(())
    }

    /// Adds a primary key to the current logical partition (for common PK).
    pub fn add_pk_common(&mut self, pk: &[u8], version: u64, is_deleted: bool) -> Result<()> {
        if !self.this_lp.is_touched {
            bail!("Must call start_lp() before adding PKs.");
        }
        if self.this_lp.is_int_pk {
            bail!("Expected common PK, but current LP is set for int PKs.");
        }
        if pk.is_empty() {
            bail!("Common PK cannot be empty.");
        }

        self.add_pk_encoded_(pk, version, is_deleted)?;

        self.current_row_key.clear();
        append_common_handle_row_key(&mut self.current_row_key, self.this_lp.table_id, pk)?;
        Self::update_row_key_bounds(
            &self.current_row_key,
            &mut self.smallest_key,
            &mut self.biggest_key,
        );

        Ok(())
    }

    /// Serializes all data for the current LP into `this_lp.serialized_buf`.
    fn serialize_lp_entry(
        &mut self,
        tantivy_layout: &ftspb::TantivyIndexLayout,
        tantivy_data: &[u8],
    ) -> Result<()> {
        if !self.this_lp.is_touched {
            bail!("Must call start_lp() before serializing LP.");
        }

        let lp = &mut self.this_lp;
        let buf = &mut lp.serialized_buf;
        buf.clear();

        if lp.lp_key.is_empty() {
            bail!("LP key cannot be empty.");
        }
        if lp.pk_count == 0 {
            bail!("LP must have at least one PK.");
        }

        // --- LP Key ---
        buf.put_u16_le(u16::try_from(lp.lp_key.len())?);
        buf.put_slice(&lp.lp_key);

        // --- Props Protobuf ---
        let mut props = ftspb::PackedFileLpProps::new();
        props.set_n_pk(lp.pk_count);
        props.set_is_int_handle(lp.is_int_pk);
        props.set_table_id(lp.table_id);
        props.set_index_id(lp.index_id);
        props.set_tantivy_layout(tantivy_layout.clone());
        let props_data = props.write_to_bytes()?;
        buf.put_u32_le(u32::try_from(props_data.len())?);
        buf.put_slice(&props_data);
        Self::write_align_padding(buf, 8);

        // --- Versions and Delete Marks ---
        buf.put_slice(bytemuck::cast_slice(&lp.versions));
        buf.put_slice(&lp.delete_marks);
        Self::write_align_padding(buf, 4);

        // --- Common PK Offsets (if applicable) ---
        if !lp.is_int_pk {
            buf.put_slice(bytemuck::cast_slice(&lp.pk_common_offsets));
        }
        Self::write_align_padding(buf, 8);

        // --- PK Data ---
        buf.put_slice(&lp.pk_data);

        // --- PK Filter ---
        // XORF requires a list of hash in the form of [hash1, hash2, ...] instead
        // of a HashSet, so we need to collect the hashes into a Vec.
        lp.pk_hashes_buf.clear();
        lp.pk_hashes_buf.reserve(lp.unique_pk_hashes.len());
        for hash in &lp.unique_pk_hashes {
            lp.pk_hashes_buf.push(*hash);
        }
        let pk_filter = match BinaryFuse8::try_from(&lp.pk_hashes_buf) {
            Ok(filter) => filter,
            Err(e) => bail!(
                "Failed to build PK filter for LP {}: {}",
                hex(&lp.lp_key),
                e
            ),
        };
        let filter_data = pk_filter.to_vec();
        buf.put_u32_le(u32::try_from(filter_data.len())?);
        buf.put_slice(&filter_data);

        // --- Tantivy Index Data ---
        buf.put_u32_le(u32::try_from(tantivy_data.len())?);
        Self::write_align_padding(buf, 8);
        buf.put_slice(tantivy_data);

        // --- End padding ---
        Self::write_align_padding(buf, 8);

        Ok(())
    }

    /// Serializes the current LP data and adds it to the current data block
    /// buffer.
    pub fn finish_lp<D: tantivy::Directory + Clone>(
        &mut self,
        tantivy_dir: &TrackedDirectory<D>,
    ) -> Result<()> {
        if !self.this_lp.is_touched {
            bail!("No active LP to finish.");
        }

        let dir = BytesDirRO::from_directory(tantivy_dir)?;
        let (tantivy_data, tantivy_layout) = serialize_tantivy_dir(dir)?;

        {
            // Props
            self.props.total_index_bytes += u32::try_from(tantivy_data.len())?;

            let lp_key = self.this_lp.lp_key.as_slice();
            if self.props.smallest_lp_key.is_empty() {
                self.props.smallest_lp_key.extend_from_slice(lp_key);
            } else if lp_key < self.props.smallest_lp_key.as_slice() {
                self.props.smallest_lp_key.clear();
                self.props.smallest_lp_key.extend_from_slice(lp_key);
            }
            if self.props.largest_lp_key.is_empty() {
                self.props.largest_lp_key.extend_from_slice(lp_key);
            } else if lp_key > self.props.largest_lp_key.as_slice() {
                self.props.largest_lp_key.clear();
                self.props.largest_lp_key.extend_from_slice(lp_key);
            }
            self.props.total_lps += 1;
            self.props.pk_live_unique = self
                .props
                .pk_live_unique
                .checked_add(self.this_lp.live_unique_count)
                .ok_or_else(|| anyhow!("Too many live PKs in PackedFile"))?;

            self.update_table_index_bounds(self.this_lp.table_id, self.this_lp.index_id);
        }

        self.serialize_lp_entry(&tantivy_layout, &tantivy_data)?;
        let entry_len = self.this_lp.serialized_buf.len();

        // Each data block records the start key of the first LP in it.
        // So we can only start a new data block after the first LP is added.
        if self.this_data_block.entry_offsets.is_empty() {
            self.start_data_block(self.this_lp.lp_key.to_vec())?;
        }

        // At this moment we expect the file offset and entry length are already
        // aligned to 8 bytes.
        // The padding after entry should be added in serialize_lp_entry.
        if self.offset % 8 != 0 {
            bail!(
                "Runtime error: Unexpected file offset alignment: offset={}",
                self.offset
            );
        }
        if entry_len % 8 != 0 {
            bail!(
                "Runtime error: Unexpected LP entry length alignment: len={}",
                entry_len
            );
        }

        self.writer.write_all(&self.this_lp.serialized_buf)?;
        self.offset += entry_len as u64;
        self.this_data_block.offset += entry_len as u64;
        self.this_data_block.checksum = self
            .options
            .checksum_type
            .append(self.this_data_block.checksum, &self.this_lp.serialized_buf);
        self.this_data_block
            .entry_offsets
            .push(u32::try_from(self.this_data_block.offset)?);

        // Add LP key hash for the file-level filter.
        self.lp_key_hashes
            .push(farmhash::fingerprint64(&self.this_lp.lp_key));

        // Update the last seen LP key for order checking.
        self.last_lp_key.clear();
        self.last_lp_key.extend_from_slice(&self.this_lp.lp_key);

        // Clear the current LP state for the next one.
        self.this_lp.clear();

        // Flush the data block if it's large enough.
        if self.this_data_block.offset >= self.options.block_size as u64 {
            self.finish_data_block()?;
        }

        Ok(())
    }

    /// Starts a new data block.
    /// It will also record this data block in the index block.
    fn start_data_block(&mut self, lp_key: Vec<u8>) -> Result<()> {
        let block_offset = self.offset;
        if block_offset % 8 != 0 {
            bail!(
                "Runtime error: Unexpected data block start alignment: offset={}",
                block_offset
            );
        }
        self.this_data_block
            .entry_offsets
            .push(u32::try_from(self.this_data_block.offset)?);

        self.index_block
            .data_block_offsets
            .push(u32::try_from(block_offset)?);
        self.index_block.data_block_start_keys.push(lp_key);

        Ok(())
    }

    /// Finish this data block. It will write data block "footer" to the
    /// underlying writer.
    fn finish_data_block(&mut self) -> Result<()> {
        if self.this_data_block.entry_offsets.is_empty() {
            // This should not happen, because we should always call
            // `try_start_data_block()` before adding any entries.
            bail!("Runtime error: Unexpected empty data block.");
        }
        if self.offset % 8 != 0 {
            // This should not happen, because each entry has been padded to 8 bytes
            // at the end.
            bail!(
                "Runtime error: Unexpected file offset alignment: offset={}",
                self.offset
            );
        }

        // We first serialize metadata into a buffer, then write it to the writer.
        let buf = &mut self.this_data_block.meta_ser_buf;
        buf.clear();

        // --- Entry Offsets ---
        buf.put_slice(bytemuck::cast_slice(&self.this_data_block.entry_offsets));

        // --- Metadata Padding & Entry Count ---
        // There are n+1 elements in entry_offsets
        let n_entries = self.this_data_block.entry_offsets.len() - 1;
        let n_entries = u32::try_from(n_entries)?;

        // --- Metadata Padding ---
        // PackedFile data blocks are segmented and mmap'd by IA. The alignment of
        // returned bytes depends on the segment start offset. To make `u64` casts
        // safe (e.g. versions array), each data block must start at an 8-byte
        // aligned offset, thus each data block must end at an 8-byte aligned
        // offset.
        //
        // Entries are always padded to 8 bytes, so we only need to ensure the
        // data block metadata has a length aligned to 8. The metadata length is:
        //   (n_entries + 1) * sizeof(u32) + sizeof(u32) + sizeof(u32)
        // which is 4 mod 8 when n_entries is even. So we insert an extra u32=0
        // padding only for even n_entries.
        //
        // Note: The padding is written before `n_entries`, so the reader can
        // always decode (checksum, n_entries) from the end and conditionally
        // skip the padding.
        if n_entries % 2 == 0 {
            buf.put_u32_le(0);
        }
        buf.put_u32_le(n_entries);

        // --- Checksum ---
        // Checksum should include everything before the checksum field, including the
        // metadata.
        self.this_data_block.checksum = self
            .options
            .checksum_type
            .append(self.this_data_block.checksum, buf);
        buf.put_u32_le(self.this_data_block.checksum);

        // --- Write the data block metadata to the writer ---
        self.writer.write_all(buf)?;
        self.offset += buf.len() as u64;
        if self.offset % 8 != 0 {
            bail!(
                "Runtime error: Unexpected file offset alignment after finishing data block: offset={}",
                self.offset
            );
        }

        // Clear the current data block.
        self.this_data_block.clear();

        Ok(())
    }

    fn update_table_index_bounds(&mut self, table_id: i64, index_id: i64) {
        let pair = (table_id, index_id);
        match self.smallest_table_index {
            None => self.smallest_table_index = Some(pair),
            Some(current) => {
                if pair < current {
                    self.smallest_table_index = Some(pair);
                }
            }
        }
        match self.largest_table_index {
            None => self.largest_table_index = Some(pair),
            Some(current) => {
                if pair > current {
                    self.largest_table_index = Some(pair);
                }
            }
        }
    }

    /// Returns the current size of the file being built.
    /// If there is a LP being built, its size is not included.
    pub fn written_size(&self) -> u64 {
        self.offset
    }

    /// Finalizes the build process, writing all remaining buffered data,
    /// metadata blocks, and the footer to the writer. Returns the total
    /// size of the file and the final property.
    pub fn finish(mut self, snap_version: SnapVersion) -> Result<PackedFileBuildSummary> {
        if self.this_lp.is_touched {
            bail!("Must call finish_lp() before finishing the file.");
        }
        if !self.this_data_block.entry_offsets.is_empty() {
            // Finish the last data block if it has data.
            // This is allowed, because a data block is only finalized
            // when it exceeds the block size.
            self.finish_data_block()?;
        }

        // Index block additionally contains the end offset of the last data block.
        self.index_block
            .data_block_offsets
            .push(u32::try_from(self.offset)?);

        // Checksum for index+filter+props.
        let mut meta_checksum = 0;

        // --- Write Index Block ---
        self.footer.index_block_offset = u32::try_from(self.offset)?;
        let index_data = self.index_block.write_to_bytes()?;
        self.writer.write_all(&index_data)?;
        self.offset += index_data.len() as u64;
        meta_checksum = self
            .options
            .checksum_type
            .append(meta_checksum, &index_data);

        // --- Write LP Filter Block ---
        self.footer.lp_filter_block_offset = u32::try_from(self.offset)?;
        // XORF filter construction expects unique keys. LP keys are unique, but
        // their hashes might collide in extremely rare cases, so we deduplicate
        // hashes to avoid construction failures.
        self.lp_key_hashes.sort_unstable();
        self.lp_key_hashes.dedup();
        let lp_filter = BinaryFuse8::try_from(&self.lp_key_hashes)
            .map_err(|e| anyhow!("Failed to build LP filter: {}", e))?;
        let filter_data = lp_filter.to_vec();
        self.writer.write_all(&filter_data)?;
        self.offset += filter_data.len() as u64;
        meta_checksum = self
            .options
            .checksum_type
            .append(meta_checksum, &filter_data);

        // --- Write Property Block ---
        // Set smallest and biggest row keys in properties (like vector index)
        if !self.smallest_key.is_empty() {
            self.props
                .set_smallest_key(std::mem::take(&mut self.smallest_key));
        }
        if !self.biggest_key.is_empty() {
            self.props
                .set_biggest_key(std::mem::take(&mut self.biggest_key));
        }

        // Set table_ids for efficient has_table() lookups (sorted, deduplicated)
        if !self.table_ids.is_empty() {
            let mut table_ids: Vec<i64> = self.table_ids.into_iter().collect();
            table_ids.sort_unstable();
            self.props.set_table_ids(table_ids);
        }
        if let Some((table_id, index_id)) = self.smallest_table_index {
            let mut bound = ftspb::TableIndexId::new();
            bound.set_table_id(table_id);
            bound.set_index_id(index_id);
            self.props.set_smallest_table_index(bound);
        }
        if let Some((table_id, index_id)) = self.largest_table_index {
            let mut bound = ftspb::TableIndexId::new();
            bound.set_table_id(table_id);
            bound.set_index_id(index_id);
            self.props.set_largest_table_index(bound);
        }
        self.props.set_snap_version(snap_version.into_inner());
        self.footer.prop_offset = u32::try_from(self.offset)?;
        let props_data = self.props.write_to_bytes()?;
        if !props_data.is_empty() {
            self.writer.write_all(&props_data)?;
            self.offset += props_data.len() as u64;
            meta_checksum = self
                .options
                .checksum_type
                .append(meta_checksum, &props_data);
        }

        // --- Write Footer ---
        self.footer.checksum_other_meta = meta_checksum;
        let n = self.footer.marshal(&mut self.writer)?;
        self.offset += n as u64;

        Ok(PackedFileBuildSummary {
            written_bytes: self.offset,
            meta_offset: self.footer.metadata_offset() as u32,
            props: std::mem::take(&mut self.props),
        })
    }

    /// Appends zero-padding to a buffer to meet an alignment requirement.
    fn write_align_padding<B: BufMut + WrittenSize>(buf: &mut B, align: usize) {
        let written = buf.written_size();
        let next_offset = next_aligned_offset(written, align);
        if next_offset > written {
            buf.put_bytes(0, next_offset - written);
        }
    }
}

fn serialize_tantivy_dir(dir: BytesDirRO) -> Result<(Vec<u8>, ftspb::TantivyIndexLayout)> {
    fn append(buf: &mut Vec<u8>, data: &Bytes) -> Result<ftspb::OffsetSize> {
        let mut range = ftspb::OffsetSize::new();
        if data.is_empty() {
            return Ok(range);
        }
        range.set_offset(buf.len() as u64);
        range.set_size(data.len() as u64);
        buf.extend_from_slice(data);
        let pad_len = next_aligned_offset(buf.len(), 8) - buf.len();
        buf.resize(buf.len() + pad_len, 0);
        Ok(range)
    }

    let mut layout = ftspb::TantivyIndexLayout::new();
    let mut buffer = Vec::new();
    layout.set_meta(append(&mut buffer, &dir.meta_json)?);
    layout.set_managed(append(&mut buffer, &dir.managed_json)?);
    layout.set_term(append(&mut buffer, &dir.term)?);
    layout.set_idx(append(&mut buffer, &dir.idx)?);
    layout.set_pos(append(&mut buffer, &dir.pos)?);
    layout.set_store(append(&mut buffer, &dir.store)?);
    layout.set_fast(append(&mut buffer, &dir.fast)?);
    layout.set_fieldnorm(append(&mut buffer, &dir.fieldnorm)?);

    Ok((buffer, layout))
}

#[cfg(test)]
mod tests {
    // Additional comprehensive tests for builder and roundtrip functionality
    use std::io::Cursor;

    use super::*;
    use crate::table::fts::dedicated_file::test::dummy_tantivy_dir;

    #[test]
    fn test_builder_error_empty_lp_key() {
        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions::default(),
        );

        let result = builder.start_lp(1, 0, true, b"");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Logical partition key cannot be empty")
        );
    }

    #[test]
    fn test_builder_error_lp_key_order() {
        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions::default(),
        );

        builder.start_lp(1, 0, true, b"lp2").unwrap();
        builder.add_pk_int(1, 100, false).unwrap();
        builder.finish_lp(&dummy_tantivy_dir()).unwrap();

        // Try to add LP with smaller key
        let result = builder.start_lp(1, 0, true, b"lp1");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("LP keys must be added in ascending order")
        );
    }

    #[test]
    fn test_builder_error_pk_without_lp() {
        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions::default(),
        );

        let result = builder.add_pk_int(1, 100, false);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Must call start_lp")
        );
    }

    #[test]
    fn test_builder_error_pk_type_mismatch() {
        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions::default(),
        );

        builder.start_lp(1, 0, true, b"lp1").unwrap(); // int PK

        let result = builder.add_pk_common(b"pk1", 100, false);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Expected common PK")
        );
    }

    #[test]
    fn test_builder_error_pk_order() {
        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions::default(),
        );

        builder.start_lp(1, 0, true, b"lp1").unwrap();
        builder.add_pk_int(2, 100, false).unwrap();

        // Try to add smaller PK
        let result = builder.add_pk_int(1, 99, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ascending order"));
    }

    #[test]
    fn test_builder_error_version_order() {
        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions::default(),
        );

        builder.start_lp(1, 0, true, b"lp1").unwrap();
        builder.add_pk_int(1, 100, false).unwrap();

        // Try to add larger version for same PK
        let result = builder.add_pk_int(1, 101, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("descending order"));
    }

    #[test]
    fn test_builder_error_empty_common_pk() {
        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions::default(),
        );

        builder.start_lp(1, 0, false, b"lp1").unwrap(); // common PK

        let result = builder.add_pk_common(b"", 100, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_builder_error_finish_without_lp() {
        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions::default(),
        );

        let result = builder.finish_lp(&dummy_tantivy_dir());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No active LP"));
    }

    #[test]
    fn test_builder_error_finish_file_with_active_lp() {
        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions::default(),
        );

        builder.start_lp(1, 0, true, b"lp1").unwrap();
        builder.add_pk_int(1, 100, false).unwrap();
        // Don't finish LP

        let result = builder.finish(SnapVersion::zero());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Must call finish_lp")
        );
    }

    #[test]
    fn test_builder_error_start_lp_without_finishing() {
        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions::default(),
        );

        builder.start_lp(1, 0, true, b"lp1").unwrap();
        builder.add_pk_int(1, 100, false).unwrap();

        // Try to start another LP without finishing the first
        let result = builder.start_lp(1, 0, true, b"lp2");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Must call finish_lp")
        );
    }

    #[test]
    fn test_builder_error_finish_lp_with_no_pks() {
        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions::default(),
        );

        builder.start_lp(1, 0, true, b"lp1").unwrap();
        // No PKs added
        let result = builder.finish_lp(&dummy_tantivy_dir());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("LP must have at least one PK")
        );
    }

    #[test]
    fn test_packed_file_pk_stats() {
        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions::default(),
        );

        builder.start_lp(42, 0, true, b"lp-a").unwrap();
        builder.add_pk_int(1, 10, false).unwrap();
        builder.add_pk_int(1, 9, true).unwrap();
        builder.add_pk_int(2, 8, true).unwrap();
        builder.add_pk_int(3, 7, false).unwrap();
        builder.finish_lp(&dummy_tantivy_dir()).unwrap();

        let summary = builder.finish(SnapVersion::zero()).unwrap();
        assert_eq!(summary.props.pk_total, 4);
        assert_eq!(summary.props.pk_live_unique, 2);
    }
}
