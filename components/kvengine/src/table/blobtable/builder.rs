// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{mem, ops::Deref};

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut, Bytes};
use cloud_encryption::EncryptionKey;
use serde::{Deserialize, Serialize};

use super::BlobRef;
use crate::table::{
    ChecksumType, InnerKey, LZ4_COMPRESSION, NO_COMPRESSION, Value, ZSTD_COMPRESSION,
    sstable::{PROP_KEY_ENCRYPTION_VER, key_diff_idx},
};

pub type ValueLength = u32; // Max value length is 4GB
pub type BlobOffset = u32; // Max blob file size is 4GB
pub type Checksum = u32;

pub const BLOB_FORMAT_V1: u16 = 1;
pub const BLOB_MAGIC_NUMBER: u32 = 0xdeadbeef;
pub const BLOB_INDEX_FORMAT_V1: u32 = 1;
pub const CRC32C: u8 = 1;
pub const PROP_KEY_BIGGEST: &[u8] = b"biggest_key";
pub const PROP_KEY_SMALLEST: &[u8] = b"smallest_key";

// Blob file format:
//
// +---------------------------------+
// |          blob record 1          |
// +---------------------------------+
// |          blob record 2          |
// +---------------------------------+
// |             ...                 |
// +---------------------------------+
// |          blob record n          |
// +---------------------------------+
// |           blob index            |
// +---------------------------------+
// |         blob properties         |
// +---------------------------------+
// |           blob footer           |
// +---------------------------------+
//
// Blob record format:
//
// +---------------------------------+
// |          checksum: u32          |
// +---------------------------------+
// |        value length: u32        |
// +---------------------------------+
// |          value: bytes           |
// +---------------------------------+

#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct BlobFooter {
    pub total_blob_size: u32,
    pub index_offset: u32,
    pub properties_offset: u32,
    pub min_blob_size: u32,
    pub magic: u32,
    pub blob_format_version: u16,
    pub compression_type: u8,
    pub checksum_type: u8,
}

pub const BLOB_TABLE_FOOTER_SIZE: usize = mem::size_of::<BlobFooter>();
pub const BLOB_ENTRY_META_SIZE: usize = mem::size_of::<Checksum>() + mem::size_of::<ValueLength>();
pub const BLOB_ENTRY_LENGTH_OFFSET: usize = mem::size_of::<Checksum>();
pub const BLOB_ENTRY_VALUE_OFFSET: usize = BLOB_ENTRY_META_SIZE;

impl BlobFooter {
    pub fn properties_len(&self, table_size: usize) -> usize {
        table_size - BLOB_TABLE_FOOTER_SIZE - self.properties_offset as usize
    }

    pub fn index_len(&self) -> usize {
        self.properties_offset as usize - self.index_offset as usize
    }

    pub fn unmarshal(&mut self, mut data: &[u8]) {
        self.total_blob_size = data.get_u32_le();
        self.index_offset = data.get_u32_le();
        self.properties_offset = data.get_u32_le();
        self.min_blob_size = data.get_u32_le();
        self.magic = data.get_u32_le();
        self.blob_format_version = data.get_u16_le();
        self.compression_type = data.get_u8();
        self.checksum_type = data.get_u8();
    }

    pub fn marshal(&self, buf: &mut Vec<u8>) {
        buf.put_u32_le(self.total_blob_size);
        buf.put_u32_le(self.index_offset);
        buf.put_u32_le(self.properties_offset);
        buf.put_u32_le(self.min_blob_size);
        buf.put_u32_le(self.magic);
        buf.put_u16_le(self.blob_format_version);
        buf.put_u8(self.compression_type);
        buf.put_u8(self.checksum_type);
    }

    pub fn is_match(&self) -> bool {
        self.magic == BLOB_MAGIC_NUMBER && self.blob_format_version == BLOB_FORMAT_V1
    }
}

#[derive(Default, Clone)]
struct BlobTableIndexEntry {
    pub block_offset: u32,
    pub key: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct BlobTableBuildOptions {
    pub compression_type: u8,
    pub min_blob_size: u32,
    pub max_blob_table_size: usize,
    // Do not bother creating a blob table if the target blob table size is not reached.
    pub target_blob_table_size: usize,
    pub block_size: u32,
}

impl Default for BlobTableBuildOptions {
    fn default() -> Self {
        Self {
            compression_type: LZ4_COMPRESSION,
            min_blob_size: 4096,
            max_blob_table_size: 64 * 1024 * 1024,
            target_blob_table_size: 2 * 1024 * 1024,
            block_size: 32 * 1024,
        }
    }
}

#[derive(Default)]
pub struct BlobTableBuilder {
    fid: u64,
    buf: Vec<u8>,
    block_indexes: Vec<BlobTableIndexEntry>,
    current_block_size: u32,
    block_size: u32,
    checksum_tp: u8,
    compression_tp: u8,
    compression_lvl: i32,
    min_blob_size: u32,
    total_blob_size: u64,
    smallest_key: Vec<u8>,
    biggest_key: Vec<u8>,
    encryption_key: Option<EncryptionKey>,
    compressed_buf: Vec<u8>,
    table_meta_off: u32,
}

impl BlobTableBuilder {
    pub fn new(
        fid: u64,
        compression_tp: u8,
        compression_lvl: i32,
        min_blob_size: u32,
        block_size: u32,
        encryption_key: Option<EncryptionKey>,
    ) -> Self {
        Self {
            fid,
            buf: vec![],
            block_indexes: vec![],
            current_block_size: 0,
            block_size,
            // TODO(xxx): Enabling checksum is recommended in the future, as it is a crucial data
            // integrity feature that must be adopted by the storage format.
            // The likely reason it is currently disabled is the lack of blob cache, which could
            // cause performance degradation when checksum is enabled.
            checksum_tp: 0,
            compression_tp,
            compression_lvl,
            min_blob_size,
            total_blob_size: 0,
            smallest_key: vec![],
            biggest_key: vec![],
            encryption_key,
            compressed_buf: vec![],
            table_meta_off: 0,
        }
    }

    pub fn new_with_checksum_type(
        fid: u64,
        compression_tp: u8,
        compression_lvl: i32,
        min_blob_size: u32,
        block_size: u32,
        encryption_key: Option<EncryptionKey>,
        checksum_type: ChecksumType,
    ) -> Self {
        Self {
            fid,
            buf: vec![],
            block_indexes: vec![],
            current_block_size: 0,
            checksum_tp: checksum_type as u8,
            compression_tp,
            compression_lvl,
            min_blob_size,
            total_blob_size: 0,
            smallest_key: vec![],
            biggest_key: vec![],
            block_size,
            encryption_key,
            compressed_buf: vec![],
            table_meta_off: 0,
        }
    }

    pub fn reset(&mut self, fid: u64) {
        self.fid = fid;
        self.buf.clear();
        self.block_indexes.clear();
        self.current_block_size = 0;
        self.total_blob_size = 0;
        self.smallest_key.clear();
        self.biggest_key.clear();
        self.smallest_key.clear();
        self.compressed_buf.clear();
        self.table_meta_off = 0;
    }

    pub fn add_blob(
        &mut self,
        inner_key: InnerKey<'_>,
        blob: &[u8],
        already_compressed: Option<ValueLength>,
        need_encrypt: bool,
    ) -> BlobRef {
        let key = inner_key.deref();
        assert!(blob.len() <= ValueLength::MAX as usize);
        assert!(self.total_blob_size as usize + blob.len() <= BlobOffset::MAX as usize);
        if self.smallest_key.is_empty() || self.smallest_key.as_slice() > key {
            self.smallest_key.clear();
            self.smallest_key.extend_from_slice(key);
        }
        if self.biggest_key.is_empty() || self.biggest_key.as_slice() < key {
            self.biggest_key.clear();
            self.biggest_key.extend_from_slice(key);
        }
        self.total_blob_size += blob.len() as u64;
        self.current_block_size += blob.len() as u32;
        if self.buf.is_empty() || self.current_block_size >= self.block_size {
            self.block_indexes.push(BlobTableIndexEntry {
                block_offset: self.buf.len() as u32,
                key: key.to_vec(),
            });
            self.current_block_size = 0;
        }

        let begin_off = self.buf.len();
        self.buf.resize(self.buf.len() + BLOB_ENTRY_META_SIZE, 0);
        let mut original_len = blob.len() as ValueLength;

        let compressed_blob = if let Some(len) = already_compressed {
            original_len = len;
            blob
        } else {
            match self.compression_tp {
                NO_COMPRESSION => blob,
                LZ4_COMPRESSION => {
                    self.compressed_buf.clear();
                    let _ = Self::compress_lz4(blob, &mut self.compressed_buf);
                    &self.compressed_buf
                }
                ZSTD_COMPRESSION => {
                    self.compressed_buf.clear();
                    let _ =
                        Self::compress_zstd(blob, self.compression_lvl, &mut self.compressed_buf);
                    &self.compressed_buf
                }
                _ => panic!("unexpected compression type {}", self.compression_tp),
            }
        };
        if let Some(encryption_key) = &self.encryption_key {
            if need_encrypt {
                encryption_key.encrypt(compressed_blob, self.fid, begin_off as u32, &mut self.buf);
            } else {
                self.buf.extend_from_slice(compressed_blob);
            }
        } else {
            self.buf.extend_from_slice(compressed_blob);
        }
        let data_len = self.buf.len() - begin_off - BLOB_ENTRY_VALUE_OFFSET;
        let checksum = ChecksumType::from(self.checksum_tp)
            .checksum(&self.buf[(begin_off + BLOB_ENTRY_VALUE_OFFSET)..]);
        let slice = self.buf.as_mut_slice();
        LittleEndian::write_u32(&mut slice[begin_off..], checksum); // put checksum at the reserved place.
        LittleEndian::write_u32(
            &mut slice[begin_off + BLOB_ENTRY_LENGTH_OFFSET..],
            data_len as ValueLength,
        ); // put compressed length at the reserved place.
        BlobRef::new(
            self.fid,
            begin_off as BlobOffset,
            data_len as u32,
            original_len,
        )
    }

    pub fn add(&mut self, key: InnerKey<'_>, value: &Value) -> BlobRef {
        self.add_blob(key, value.get_value(), None, true)
    }

    fn compress_lz4(uncompressed: &[u8], compressed_buf: &mut Vec<u8>) -> usize {
        unsafe {
            let uncompressed_len = uncompressed.len() as i32;
            let compress_bound = lz4::liblz4::LZ4_compressBound(uncompressed_len);
            let original_len = compressed_buf.len();
            compressed_buf.resize(original_len + compress_bound as usize, 0);
            let dst = &mut compressed_buf[original_len..];
            let size = lz4::liblz4::LZ4_compress_default(
                uncompressed.as_ptr() as *const libc::c_char,
                dst.as_mut_ptr() as *mut libc::c_char,
                uncompressed_len,
                compress_bound,
            ) as usize;
            compressed_buf.set_len(original_len + size);
            size
        }
    }

    fn compress_zstd(
        uncompressed: &[u8],
        compression_lvl: i32,
        compressed_buf: &mut Vec<u8>,
    ) -> usize {
        unsafe {
            let uncompressed_len = uncompressed.len();
            let compress_bound = zstd_sys::ZSTD_compressBound(uncompressed_len);
            let original_len = compressed_buf.len();
            compressed_buf.resize(original_len + compress_bound, 0);
            let dst = &mut compressed_buf[original_len..];
            let size = zstd_sys::ZSTD_compress(
                dst.as_mut_ptr() as *mut libc::c_void,
                compress_bound,
                uncompressed.as_ptr() as *const libc::c_void,
                uncompressed_len,
                compression_lvl as libc::c_int,
            );
            compressed_buf.set_len(original_len + size);
            size
        }
    }

    fn add_property(buf: &mut Vec<u8>, key: &[u8], val: &[u8]) {
        buf.put_u16_le(key.len() as u16);
        buf.put_slice(key);
        buf.put_u32_le(val.len() as u32);
        buf.put_slice(val);
    }

    fn set_table_meta_off(&mut self, table_meta_off: u32) {
        self.table_meta_off = table_meta_off;
    }

    // Smallest and biggest key are used to indicate the key range of the blob file.
    pub fn finish(&mut self) -> Bytes {
        let index_offset = self.buf.len();
        self.set_table_meta_off(index_offset as u32);

        Self::build_index_static(
            &self.block_indexes,
            &mut self.buf,
            ChecksumType::from(self.checksum_tp),
        );

        let properties_offset = self.buf.len();
        BlobTableBuilder::add_property(&mut self.buf, PROP_KEY_SMALLEST, &self.smallest_key);
        BlobTableBuilder::add_property(&mut self.buf, PROP_KEY_BIGGEST, &self.biggest_key);
        if let Some(encryption_key) = &self.encryption_key {
            BlobTableBuilder::add_property(
                &mut self.buf,
                PROP_KEY_ENCRYPTION_VER,
                &encryption_key.current_ver.to_le_bytes(),
            )
        }

        let mut footer = BlobFooter::default();
        footer.index_offset = index_offset as u32;
        footer.properties_offset = properties_offset as u32;
        footer.total_blob_size = self.total_blob_size as u32;
        footer.compression_type = self.compression_tp;
        footer.checksum_type = self.checksum_tp;
        footer.blob_format_version = BLOB_FORMAT_V1;
        footer.min_blob_size = self.min_blob_size;
        footer.magic = BLOB_MAGIC_NUMBER;
        footer.marshal(&mut self.buf);
        mem::take(&mut self.buf).into()
    }

    pub fn meta_offset(&self) -> usize {
        self.table_meta_off as usize
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn get_fid(&self) -> u64 {
        self.fid
    }

    pub fn smallest_biggest_key(&self) -> (&[u8], &[u8]) {
        (&self.smallest_key, &self.biggest_key)
    }

    pub fn total_blob_size(&self) -> u64 {
        self.total_blob_size
    }

    fn build_index_static(
        block_indexes: &[BlobTableIndexEntry],
        buf: &mut Vec<u8>,
        checksum_type: ChecksumType,
    ) {
        let num_blocks = block_indexes.len();
        if num_blocks == 0 {
            return;
        }

        let first_key = &block_indexes[0].key;
        let last_key = &block_indexes[num_blocks - 1].key;
        let common_prefix_len = key_diff_idx(first_key, last_key);
        let start_pos = buf.len();
        buf.put_u32_le(0);
        buf.put_u32_le(BLOB_INDEX_FORMAT_V1);
        buf.put_u32_le(num_blocks as u32);

        let mut key_offset = 0u32;
        for index in block_indexes {
            buf.put_u32_le(key_offset);
            key_offset += (index.key.len() - common_prefix_len) as u32;
        }

        for index in block_indexes {
            buf.put_u32_le(index.block_offset);
        }

        buf.put_u16_le(common_prefix_len as u16);
        if common_prefix_len > 0 {
            buf.extend_from_slice(&first_key[..common_prefix_len]);
        }

        let compressed_keys_len = block_indexes
            .iter()
            .map(|idx| idx.key.len() - common_prefix_len)
            .sum::<usize>();
        buf.put_u32_le(compressed_keys_len as u32);

        for index in block_indexes {
            buf.extend_from_slice(&index.key[common_prefix_len..]);
        }

        let checksum = checksum_type.checksum(&buf[start_pos + 4..]);
        LittleEndian::write_u32(&mut buf[start_pos..], checksum);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{Value, blobtable::blobtable::BlobTable};

    #[test]
    fn test_blob_index() {
        test_util::init_log_for_test();
        let mut builder = BlobTableBuilder::new(1, NO_COMPRESSION, 0, 0, 512, None);

        const NUM_KEYS: usize = 10000;
        let mut keys = Vec::with_capacity(NUM_KEYS);
        let mut rng_state = 12345u64; // Simple LCG for reproducible randomness

        // Generate random keys with various patterns
        for i in 0..NUM_KEYS {
            // Simple LCG: a = 1664525, c = 1013904223, m = 2^32
            rng_state = rng_state.wrapping_mul(1664525).wrapping_add(1013904223);

            let key = if i < 8 {
                // Include some edge cases
                match i {
                    0 => "a".to_string(),             // Single character
                    1 => "z".repeat(100),             // Very long key
                    2 => "duplicate_key".to_string(), // Will be duplicated later
                    3 => "duplicate_key".to_string(), // Duplicate
                    4 => "prefix_common_suffix".to_string(),
                    5 => "prefix_common_different".to_string(),
                    6 => "zzz_last_alphabetically".to_string(),
                    _ => "aaa_first_alphabetically".to_string(),
                }
            } else {
                // Random keys with various patterns
                let pattern = rng_state % 4;
                match pattern {
                    0 => format!("user_{:08x}", rng_state),
                    1 => format!("data/file/{:06}", rng_state % 100000),
                    2 => format!("key_{:020}", rng_state),
                    _ => format!("random_{:x}_{:x}", rng_state, i),
                }
            };
            keys.push(key);
        }

        // Sort keys to ensure proper index behavior
        keys.sort();
        keys.dedup();

        // Add all keys to the builder
        for (i, key) in keys.iter().enumerate() {
            let value_data = format!("value_data_for_key_{}_index_{}", key, i);
            let encoded = Value::encode_buf(0, &[0], 0, value_data.as_bytes());
            let value = Value::decode(encoded.as_slice());
            builder.add(InnerKey::from_inner_buf(key.as_bytes()), &value);
        }

        let table_data = builder.finish();

        // Verify the format can be parsed
        let table = BlobTable::from_bytes(table_data).unwrap();
        assert_eq!(table.version(), BLOB_FORMAT_V1);
        assert!(table.index().num_blocks() > 0);
        assert!(table.index().num_blocks() <= keys.len());

        // Test key lookup for all keys
        for (expected_max_idx, key) in keys.iter().enumerate() {
            let block_idx = table
                .index()
                .seek_block(InnerKey::from_inner_buf(key.as_bytes()))
                .saturating_sub(1);
            assert!(
                block_idx <= expected_max_idx,
                "Block index {} should be <= expected max {} for key: '{}'",
                block_idx,
                expected_max_idx,
                key
            );
        }

        // Test lookup for keys not in the table
        let non_existent_keys = [
            "before_all_keys_000",
            "middle_non_existent_key",
            "zzz_after_all_keys_999",
        ];

        for key in &non_existent_keys {
            let block_idx = table
                .index()
                .seek_block(InnerKey::from_inner_buf(key.as_bytes()))
                .saturating_sub(1);
            assert!(
                block_idx < table.index().num_blocks(),
                "Block index should be valid even for non-existent key: '{}', block_idx: {}, num_blocks: {}",
                key,
                block_idx,
                table.index().num_blocks(),
            );
        }

        // Test boundary conditions
        if !keys.is_empty() {
            // Test first 2 keys
            for i in 0..=1 {
                let first_key = &keys[i];
                let block_idx = table
                    .index()
                    .seek_block(InnerKey::from_inner_buf(first_key.as_bytes()))
                    .saturating_sub(1);
                assert_eq!(block_idx, 0, "First {} key should map to block 0", i);
            }

            // Test last 2 keys
            for i in 2..=3 {
                let last_key = &keys[keys.len() - i];
                let block_idx = table
                    .index()
                    .seek_block(InnerKey::from_inner_buf(last_key.as_bytes()))
                    .saturating_sub(1);
                assert!(
                    block_idx < table.index().num_blocks(),
                    "Last key should map to a valid block, block_idx: {}, num_blocks: {}, i: {}",
                    block_idx,
                    table.index().num_blocks(),
                    i
                );
            }
        }

        info!(
            "Successfully tested blob index with {} unique keys across {} blocks",
            keys.len(),
            table.index().num_blocks()
        );
    }

    #[test]
    fn test_blob_table_builder_getters_and_reset() {
        // Create builder with specific properties
        let mut builder = BlobTableBuilder::new(
            42,               // file id
            ZSTD_COMPRESSION, // compression type
            3,                // compression level
            1024,             // min blob size
            10 * 1024,        // segment size
            None,             // encryption key
        );

        // Add test data
        let test_data = vec![b'x'; 2048];
        let encoded = Value::encode_buf(0, &[0], 0, &test_data);
        let value = Value::decode(encoded.as_slice());
        let small_key = InnerKey::from_inner_buf(b"aaa_key");
        let big_key = InnerKey::from_inner_buf(b"zzz_key");

        builder.add(big_key, &value);
        builder.add(small_key, &value);

        // Test getters
        assert_eq!(
            builder.get_fid(),
            42,
            "get_fid() should return correct file id"
        );
        assert_eq!(
            builder.total_blob_size(),
            test_data.len() as u64 * 2,
            "total_blob_size() should reflect sum of all blob sizes"
        );
        assert!(
            !builder.is_empty(),
            "is_empty() should return false after adding data"
        );

        // Test boundary keys
        let (smallest, biggest) = builder.smallest_biggest_key();
        assert_eq!(
            smallest,
            small_key.deref(),
            "smallest_key should be lexicographically smallest"
        );
        assert_eq!(
            biggest,
            big_key.deref(),
            "biggest_key should be lexicographically biggest"
        );

        // Test reset
        builder.reset(43);

        // Verify state after reset
        assert_eq!(
            builder.get_fid(),
            43,
            "get_fid() should return new file id after reset"
        );
        assert_eq!(
            builder.total_blob_size(),
            0,
            "total_blob_size() should be 0 after reset"
        );
        assert!(
            builder.is_empty(),
            "is_empty() should return true after reset"
        );

        let (smallest, biggest) = builder.smallest_biggest_key();
        assert!(
            smallest.is_empty(),
            "smallest_key should be empty after reset"
        );
        assert!(
            biggest.is_empty(),
            "biggest_key should be empty after reset"
        );
    }
}
