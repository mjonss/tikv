// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
use std::{cmp, collections::HashMap, sync::Arc};

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, Bytes};
use cloud_encryption::EncryptionKey;
use kvenginepb::BlobCreate;

use super::{BlobRef, builder::*};
use crate::{
    error::IoContext,
    ia::types::FileSegmentIdent,
    table::{
        BoundedDataSet, ChecksumType, DataBound, Error, InnerKey, LZ4_COMPRESSION, NO_COMPRESSION,
        Result, ZSTD_COMPRESSION,
        file::File,
        sstable::{Index, PROP_KEY_ENCRYPTION_VER},
    },
};

fn verify_blob_checksum(checksum_type: u8, meta_slice: &[u8], data_slice: &[u8]) -> Result<()> {
    if checksum_type == ChecksumType::None as u8 {
        return Ok(());
    }

    let checksum = LittleEndian::read_u32(meta_slice);
    let got_checksum = ChecksumType::from(checksum_type).checksum(data_slice);
    if checksum != got_checksum {
        Err(Error::InvalidChecksum("blob checksum mismatch".to_owned()))
    } else {
        Ok(())
    }
}

#[derive(Clone)]
pub struct BlobTable {
    file: Option<Arc<dyn File>>,
    preloaded_data: Option<Bytes>,
    index: Index,
    footer: BlobFooter,
    smallest_key: Bytes,
    biggest_key: Bytes,
    pub(crate) encryption_ver: u32,
}

impl BlobTable {
    pub fn new(file: Arc<dyn File>) -> Result<Self> {
        let mut footer = BlobFooter::default();
        let size = file.size();
        let footer_data = file.read_footer(Self::footer_size())?;
        footer.unmarshal(&footer_data);
        let index_data = file.read(footer.index_offset as u64, footer.index_len())?;
        let index = Index::new_for_blob(index_data)?;
        let props_data = file.read(
            footer.properties_offset as u64,
            footer.properties_len(size as usize),
        )?;
        let mut prop_slice = props_data.chunk();
        let mut smallest_key = Bytes::new();
        let mut biggest_key = Bytes::new();
        let mut encryption_ver = 0;
        while !prop_slice.is_empty() {
            let (key, val, remain) = parse_prop_data(prop_slice);
            prop_slice = remain;
            if key == PROP_KEY_SMALLEST {
                smallest_key = Bytes::copy_from_slice(val);
            } else if key == PROP_KEY_BIGGEST {
                biggest_key = Bytes::copy_from_slice(val);
            } else if key == PROP_KEY_ENCRYPTION_VER {
                encryption_ver = LittleEndian::read_u32(val);
            }
        }
        Ok(Self {
            file: Some(file),
            preloaded_data: None,
            index,
            footer,
            smallest_key,
            biggest_key,
            encryption_ver,
        })
    }

    pub fn from_bytes(bytes: Bytes) -> Result<Self> {
        let mut footer = BlobFooter::default();
        let size = bytes.len();
        if size < BLOB_TABLE_FOOTER_SIZE {
            return Err(Error::InvalidFileSize);
        }
        let footer_data = &bytes[size - BLOB_TABLE_FOOTER_SIZE..size];
        footer.unmarshal(footer_data);

        let index_data = bytes
            .slice(footer.index_offset as usize..footer.index_offset as usize + footer.index_len());
        let index = Index::new_for_blob(index_data)?;

        let mut props_data = &bytes[footer.properties_offset as usize
            ..footer.properties_offset as usize + footer.properties_len(size)];
        let mut smallest_key = Bytes::new();
        let mut biggest_key = Bytes::new();
        let mut encryption_ver = 0;
        while !props_data.is_empty() {
            let (key, val, remain) = parse_prop_data(props_data);
            props_data = remain;
            if key == PROP_KEY_SMALLEST {
                smallest_key = Bytes::copy_from_slice(val);
            } else if key == PROP_KEY_BIGGEST {
                biggest_key = Bytes::copy_from_slice(val);
            } else if key == PROP_KEY_ENCRYPTION_VER {
                encryption_ver = LittleEndian::read_u32(val);
            }
        }
        Ok(Self {
            file: None,
            preloaded_data: Some(bytes),
            index,
            footer,
            smallest_key,
            biggest_key,
            encryption_ver,
        })
    }

    pub fn get(
        &self,
        blob_ref: &BlobRef,
        decryption_buf: &mut Vec<u8>,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Vec<u8>> {
        let data = self
            .file
            .as_ref()
            .unwrap_or_else(|| panic!("file is not set"))
            .read(
                blob_ref.offset as u64,
                blob_ref.len as usize + BLOB_ENTRY_META_SIZE,
            )?;
        let meta_slice = &data.chunk()[..BLOB_ENTRY_META_SIZE];
        let mut data_slice = &data.chunk()[BLOB_ENTRY_META_SIZE..];
        verify_blob_checksum(self.footer.checksum_type, meta_slice, data_slice)?;

        if let Some(encryption_key) = &encryption_key {
            decryption_buf.clear();
            encryption_key.decrypt(
                data_slice,
                self.id(),
                blob_ref.offset,
                self.encryption_ver,
                decryption_buf,
            );
            data_slice = decryption_buf;
        }
        if self.footer.compression_type != NO_COMPRESSION {
            let mut decompressed = Vec::with_capacity(blob_ref.original_len as usize);
            self.decompress(
                meta_slice,
                data_slice,
                blob_ref.len,
                blob_ref.original_len,
                &mut decompressed,
            )?;
            Ok(decompressed)
        } else {
            Ok(data_slice.to_vec())
        }
    }

    // If data is compressed and the caller want it to be decpressed, the buf will
    // be used to store decompressed data, and the returned slice is a reference
    // to the decompressed data. Otherwise, the returned slice is a reference to
    // the original data.
    pub fn get_from_preloaded<'a>(
        &'a self,
        blob_ref: &BlobRef,
        need_decompress: bool,
        need_decrypt: bool,
        buf: &'a mut Vec<u8>,
        decryption_buf: &'a mut Vec<u8>,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<&'a [u8]> {
        let data = &self.preloaded_data.as_ref().unwrap()[blob_ref.offset as usize
            ..blob_ref.offset as usize + BLOB_ENTRY_VALUE_OFFSET + blob_ref.len as usize];
        let meta_slice = &data[..BLOB_ENTRY_META_SIZE];
        let mut data_slice = &data[BLOB_ENTRY_META_SIZE..];
        verify_blob_checksum(self.footer.checksum_type, meta_slice, data_slice)?;

        if let Some(encryption_key) = &encryption_key {
            if need_decrypt {
                decryption_buf.clear();
                encryption_key.decrypt(
                    data_slice,
                    // Must use blob_ref.fid instead of self.id(), since self.file is not
                    // initialized by BlobTable::from_bytes() at preload.
                    blob_ref.fid,
                    blob_ref.offset,
                    self.encryption_ver,
                    decryption_buf,
                );
                data_slice = decryption_buf;
            }
        }
        if need_decompress
            && self.decompress(
                meta_slice,
                data_slice,
                blob_ref.len,
                blob_ref.original_len,
                buf,
            )?
        {
            Ok(buf.as_slice())
        } else {
            Ok(data_slice)
        }
    }

    pub fn decompress(
        &self,
        meta_slice: &[u8],
        compressed_data: &[u8],
        size: u32,
        original_len: u32,
        decompressed_buf: &mut Vec<u8>,
    ) -> Result<bool> {
        let compressed_len = LittleEndian::read_u32(&meta_slice[BLOB_ENTRY_LENGTH_OFFSET..]);
        assert_eq!(compressed_len, size);
        match self.footer.compression_type {
            NO_COMPRESSION => Ok(false), // in place decoding
            LZ4_COMPRESSION => {
                decompressed_buf.resize(original_len as usize, 0);
                lz4::block::decompress_to_buffer(
                    compressed_data,
                    Some(original_len as i32),
                    decompressed_buf,
                )
                .table_ctx(0, "blob.lz4_decompress")?;
                Ok(true)
            }
            ZSTD_COMPRESSION => unsafe {
                decompressed_buf.resize(original_len as usize, 0);
                let result = zstd_sys::ZSTD_decompress(
                    decompressed_buf.as_mut_ptr() as *mut libc::c_void,
                    original_len as usize,
                    compressed_data.as_ptr() as *const libc::c_void,
                    compressed_data.len(),
                );
                assert_eq!(zstd_sys::ZSTD_isError(result), 0u32);
                Ok(true)
            },
            _ => panic!("unknown compression type {}", self.footer.compression_type),
        }
    }

    pub fn id(&self) -> u64 {
        self.file
            .as_ref()
            .unwrap_or_else(|| panic!("file is not set"))
            .id()
    }

    pub fn version(&self) -> u16 {
        self.footer.blob_format_version
    }

    pub fn smallest_key(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(self.smallest_key.chunk())
    }

    pub fn biggest_key(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(self.biggest_key.chunk())
    }

    pub fn size(&self) -> u64 {
        self.file
            .as_ref()
            .unwrap_or_else(|| panic!("file is not set"))
            .size()
    }
    pub fn smallest_biggest_key(&self) -> (InnerKey<'_>, InnerKey<'_>) {
        (self.smallest_key(), self.biggest_key())
    }

    pub fn total_blob_size(&self) -> u32 {
        self.footer.total_blob_size
    }

    pub fn compression_tp(&self) -> u8 {
        self.footer.compression_type
    }

    pub fn min_blob_size(&self) -> u32 {
        self.footer.min_blob_size
    }

    pub const fn footer_size() -> usize {
        BLOB_TABLE_FOOTER_SIZE
    }

    pub fn meta_offset(&self) -> u32 {
        self.footer.index_offset
    }

    pub fn index(&self) -> &Index {
        &self.index
    }

    pub fn to_blob_create(&self) -> BlobCreate {
        let mut blob_create = BlobCreate::new();
        blob_create.set_id(self.id());
        blob_create.set_smallest(self.smallest_key().to_vec());
        blob_create.set_biggest(self.biggest_key().to_vec());
        blob_create.set_meta_offset(self.footer.index_offset);
        blob_create
    }

    pub fn is_sync(&self) -> bool {
        if let Some(file) = &self.file {
            file.is_sync()
        } else {
            true
        }
    }

    pub fn get_remote_segments(
        &self,
        data_bound: DataBound<'_>,
    ) -> Result<(Vec<FileSegmentIdent>, usize /* total_segments */)> {
        if self.is_sync() {
            return Ok((vec![], 0));
        }
        let Some(file) = &self.file else {
            return Ok((vec![], 0));
        };

        let (first_block, exclusive_last_block) = self.index.seek_overlap_blocks(data_bound);
        let start_off = self.index.get_block_addr(first_block).curr_off as u64;
        let end_off = if exclusive_last_block < self.index.num_blocks() {
            self.index.get_block_addr(exclusive_last_block).curr_off as u64
        } else {
            self.meta_offset() as u64
        };

        file.get_remote_segments(&[(start_off, end_off)])
    }
}

impl BoundedDataSet for BlobTable {
    fn data_bound(&self) -> DataBound<'_> {
        DataBound::new(self.smallest_key(), self.biggest_key(), true)
    }
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

pub struct BlobPrefetcher {
    tables: Arc<HashMap<u64, BlobTable>>,
    tbl_buffers: HashMap<u64, (u32, Vec<u8>)>,
    prefetch_size: usize,
    decompressed_buffer: Vec<u8>,
    decryption_buffer: Vec<u8>,
    encryption_key: Option<EncryptionKey>,
}

impl BlobPrefetcher {
    pub fn new(
        tables: Arc<HashMap<u64, BlobTable>>,
        prefetch_size: usize,
        encryption_key: Option<EncryptionKey>,
    ) -> Self {
        Self {
            tables,
            tbl_buffers: Default::default(),
            prefetch_size,
            decompressed_buffer: vec![],
            decryption_buffer: vec![],
            encryption_key,
        }
    }

    pub fn get(&mut self, blob_ref: &BlobRef) -> Result<&[u8]> {
        let blob_table = self
            .tables
            .get(&blob_ref.fid)
            .ok_or_else(|| Error::Other(format!("blob table not found, fid: {}", blob_ref.fid)))?;
        let data_size = blob_ref.len as usize + BLOB_ENTRY_META_SIZE;
        let (buffer_offset, buffer) = self
            .tbl_buffers
            .entry(blob_ref.fid)
            .or_insert_with(|| (blob_ref.offset, vec![]));
        if !(blob_ref.offset >= *buffer_offset
            && blob_ref.offset + data_size as u32 <= *buffer_offset + buffer.len() as u32)
        {
            let file = blob_table.file.as_ref().unwrap_or_else(|| {
                panic!(
                    "blob table file not set, blob table id: {}",
                    blob_table.id()
                )
            });
            let len = cmp::min(
                cmp::max(self.prefetch_size, data_size),
                file.size() as usize - blob_ref.offset as usize,
            );
            if len != buffer.len() {
                buffer.resize(len, 0);
            }
            file.read_at(buffer, blob_ref.offset as u64)?;
            *buffer_offset = blob_ref.offset;
        }
        let start_off = (blob_ref.offset - *buffer_offset) as usize;
        let data = &buffer[start_off..start_off + BLOB_ENTRY_VALUE_OFFSET + blob_ref.len as usize];
        let meta_slice = &data[..BLOB_ENTRY_META_SIZE];
        let mut data_slice = &data[BLOB_ENTRY_META_SIZE..];
        verify_blob_checksum(blob_table.footer.checksum_type, meta_slice, data_slice)?;

        if let Some(encryption_key) = &self.encryption_key {
            let encryption_ver = blob_table.encryption_ver;
            self.decryption_buffer.clear();
            encryption_key.decrypt(
                data_slice,
                blob_table.id(),
                blob_ref.offset,
                encryption_ver,
                &mut self.decryption_buffer,
            );
            data_slice = &self.decryption_buffer;
        }
        if blob_table.decompress(
            meta_slice,
            data_slice,
            blob_ref.len,
            blob_ref.original_len,
            &mut self.decompressed_buffer,
        )? {
            return Ok(&self.decompressed_buffer);
        }
        Ok(data_slice)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use cloud_encryption::EncryptionKey;
    use rand::{Rng, distributions::Alphanumeric, rngs::ThreadRng};
    use rstest::rstest;
    use test_util::init_log_for_test;

    use super::*;
    use crate::table::{
        Error, InnerKey, LZ4_COMPRESSION, NO_COMPRESSION, Value, ZSTD_COMPRESSION,
        blobtable::{
            BlobRef,
            builder::{BLOB_ENTRY_META_SIZE, BlobTableBuilder},
        },
        file::InMemFile,
    };

    const BLOB_BLOCK_SIZE: u32 = 32 * 1024;

    fn get_blob_text(max_len: usize, rng: &mut ThreadRng) -> String {
        let len = rng.gen_range(1..max_len);
        rng.sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    struct TestData {
        blob: String,
        blob_ref: BlobRef,
    }

    #[rstest]
    #[case::enable_encryption(true)]
    #[case::disable_encryption(false)]
    fn test_basic(#[case] enable_encryption: bool) {
        init_log_for_test();
        let mut rng = rand::thread_rng();
        let encryption_key = if enable_encryption {
            Some(new_test_encryption_key())
        } else {
            None
        };
        let mut builder = BlobTableBuilder::new(
            1,
            NO_COMPRESSION,
            0,
            0,
            BLOB_BLOCK_SIZE,
            encryption_key.clone(),
        );
        let mut test_data = Vec::new();
        let meta: u8 = 0;

        for i in 0..128 {
            let key = format!("key_{:03}", i);
            let blob = get_blob_text(64, &mut rng);
            let encoded = Value::encode_buf(meta, &[0], 0, blob.as_bytes());
            let value = Value::decode(encoded.as_slice());
            // In this test assume that all values are converted to blob refs.
            let blob_ref = builder.add(InnerKey::from_inner_buf(key.as_bytes()), &value);
            test_data.push(TestData { blob, blob_ref });
        }

        let file = InMemFile::new(1, builder.finish());
        let table = BlobTable::new(Arc::new(file)).unwrap();
        let mut decryption_buf = vec![];
        for td in test_data {
            let blob = table
                .get(&td.blob_ref, &mut decryption_buf, encryption_key.clone())
                .unwrap();
            assert_eq!(td.blob.as_bytes(), blob);
        }

        assert_eq!(table.smallest_key, format!("key_{:03}", 0));
        assert_eq!(table.biggest_key, format!("key_{:03}", 127));
    }

    fn new_test_encryption_key() -> EncryptionKey {
        EncryptionKey::new(b"cipher".to_vec(), b"plain".to_vec(), 0)
    }

    /// Test preloaded functionality with different compression and
    /// encryption settings
    #[rstest]
    #[case::enable_encryption_no_compression(true, NO_COMPRESSION)]
    #[case::enable_encryption_compression(true, ZSTD_COMPRESSION)]
    #[case::disable_encryption_no_compression(false, NO_COMPRESSION)]
    #[case::disable_encryption_compression(false, ZSTD_COMPRESSION)]
    fn test_preloaded(#[case] enable_encryption: bool, #[case] compression_type: u8) {
        test_util::init_log_for_test();
        let encryption_key = if enable_encryption {
            Some(new_test_encryption_key())
        } else {
            None
        };

        let mut builder = BlobTableBuilder::new(
            1,
            compression_type,
            0,
            0,
            BLOB_BLOCK_SIZE,
            encryption_key.clone(),
        );

        // Add test data
        let test_data = b"test_data".to_vec();
        let encoded = Value::encode_buf(0, &[0], 0, &test_data);
        let value = Value::decode(encoded.as_slice());
        let blob_ref = builder.add(InnerKey::from_inner_buf(b"test_key"), &value);

        // Create a preloaded table from the blob data
        let table_data = builder.finish();
        let table = BlobTable::from_bytes(table_data).unwrap();

        // Buffers needed for decompression and decryption
        let mut decompress_buf = vec![];
        let mut decrypt_buf = vec![];

        // Test retrieval using get_from_preloaded
        let result = table.get_from_preloaded(
            &blob_ref,
            true,
            true,
            &mut decompress_buf,
            &mut decrypt_buf,
            encryption_key.clone(),
        );

        // Verify the result
        assert!(result.is_ok(), "Data retrieval should succeed");
        let retrieved_value = result.unwrap();
        assert_eq!(
            retrieved_value,
            test_data.as_slice(),
            "Retrieved data should match original"
        );

        // Verify that buffer reuse works by using the same buffers again
        let result2 = table.get_from_preloaded(
            &blob_ref,
            true,
            true,
            &mut decompress_buf,
            &mut decrypt_buf,
            encryption_key,
        );
        assert!(result2.is_ok(), "Second retrieval should succeed");
        assert_eq!(
            result2.unwrap(),
            test_data.as_slice(),
            "Second retrieval should match original"
        );
    }

    #[rstest]
    #[case::enable_encryption(true)]
    #[case::disable_encryption(false)]
    fn test_prefetcher(#[case] enable_encryption: bool) {
        init_log_for_test();
        let encryption_key = if enable_encryption {
            Some(new_test_encryption_key())
        } else {
            None
        };
        let mut builder = BlobTableBuilder::new(
            1,
            NO_COMPRESSION,
            0,
            0,
            BLOB_BLOCK_SIZE,
            encryption_key.clone(),
        );
        let mut offsets = Vec::new();
        for i in 0..100 {
            let key_str = format!("key_{:03}", i);
            let val_str = format!("val_{:03}", i);
            let val_buf = Value::encode_buf(b'A', &[0], 0, val_str.as_bytes());
            let blob_ref = builder.add(
                InnerKey::from_inner_buf(key_str.as_bytes()),
                &Value::decode(val_buf.as_slice()),
            );
            offsets.push(blob_ref);
        }
        let file = InMemFile::new(1, builder.finish());
        let table = BlobTable::new(Arc::new(file)).unwrap();
        let blob_tables: HashMap<u64, BlobTable> = [(1, table.clone())].into();
        let mut prefetcher = BlobPrefetcher::new(Arc::new(blob_tables), 1000, encryption_key);
        for i in 0..100 {
            let expected_val = format!("val_{:03}", i);
            let val = prefetcher.get(&offsets[i]).unwrap();
            assert_eq!(val, expected_val.as_bytes());
        }
    }

    /// Test compression with different types and levels
    #[test]
    fn test_blob_compression_effectiveness() {
        // Create highly compressible data (repeating pattern)
        let test_data = vec![b'a'; 10000];
        let encoded = Value::encode_buf(0, &[0], 0, &test_data);
        let value = Value::decode(encoded.as_slice());

        // Test different compression configurations
        let configs = [
            (NO_COMPRESSION, 0, "no compression"),
            (LZ4_COMPRESSION, 1, "LZ4 low level"),
            (LZ4_COMPRESSION, 9, "LZ4 high level"),
            (ZSTD_COMPRESSION, 1, "ZSTD low level"),
            (ZSTD_COMPRESSION, 9, "ZSTD high level"),
        ];

        // Store compression results for comparison
        let mut results = Vec::new();

        for &(compression_type, compression_level, desc) in &configs {
            let mut builder = BlobTableBuilder::new(
                1,
                compression_type,
                compression_level,
                0,
                BLOB_BLOCK_SIZE,
                None,
            );
            let blob_ref = builder.add(InnerKey::from_inner_buf(b"test_key"), &value);

            let file = Arc::new(InMemFile::new(1, builder.finish()));
            let table = BlobTable::new(file).unwrap();
            let mut decryption_buf = vec![];

            let decompressed = table.get(&blob_ref, &mut decryption_buf, None).unwrap();
            assert_eq!(decompressed, test_data, "Data corrupted with {}", desc);

            // Record compressed size for comparison
            results.push((compression_type, compression_level, blob_ref.len, desc));
        }

        // Find uncompressed size as baseline
        let uncompressed_size = results
            .iter()
            .find(|&&(tp, ..)| tp == NO_COMPRESSION)
            .map(|&(_, _, size, _)| size)
            .unwrap();

        // Verify compression effectiveness
        for &(tp, level, size, desc) in &results {
            if tp == NO_COMPRESSION {
                continue;
            }

            // All compression should be better than no compression
            assert!(
                size < uncompressed_size,
                "{} (size: {}) should be smaller than uncompressed (size: {})",
                desc,
                size,
                uncompressed_size
            );

            // High compression levels should be better than or equal to low levels
            if level == 9 {
                let low_level_size = results
                    .iter()
                    .find(|&&(t, l, ..)| t == tp && l == 1)
                    .map(|&(_, _, s, _)| s)
                    .unwrap();

                assert!(
                    size <= low_level_size,
                    "High level {} (size: {}) should be smaller than or equal to low level (size: {})",
                    desc,
                    size,
                    low_level_size
                );
            }
        }

        // Compare different algorithms (optional, as one might not always be better)
        let lz4_high = results
            .iter()
            .find(|&&(tp, level, ..)| tp == LZ4_COMPRESSION && level == 9)
            .map(|&(_, _, size, _)| size)
            .unwrap();

        let zstd_high = results
            .iter()
            .find(|&&(tp, level, ..)| tp == ZSTD_COMPRESSION && level == 9)
            .map(|&(_, _, size, _)| size)
            .unwrap();

        // Note that zstd does not always better, as it depends on data
        // characteristics. But in this test workload, it works better.
        assert!(zstd_high < lz4_high);
    }

    /// Test data integrity with corrupted blobs using different access methods
    #[rstest]
    #[case::enable_encryption_no_compression(true, NO_COMPRESSION)]
    #[case::enable_encryption_compression(true, ZSTD_COMPRESSION)]
    #[case::disable_encryption_no_compression(false, NO_COMPRESSION)]
    #[case::disable_encryption_compression(false, ZSTD_COMPRESSION)]
    fn test_blob_data_integrity(#[case] enable_encryption: bool, #[case] compression_type: u8) {
        let encryption_key = if enable_encryption {
            Some(new_test_encryption_key())
        } else {
            None
        };

        // Setup common test data
        let mut builder = BlobTableBuilder::new_with_checksum_type(
            1,
            compression_type,
            0,
            0,
            BLOB_BLOCK_SIZE,
            encryption_key.clone(),
            ChecksumType::Crc32,
        );

        let test_data = b"test_data".to_vec();
        let encoded = Value::encode_buf(0, &[0], 0, &test_data);
        let value = Value::decode(encoded.as_slice());
        let blob_ref = builder.add(InnerKey::from_inner_buf(b"test_key"), &value);

        // Corrupt the data
        let mut file_data = builder.finish().to_vec();
        let corrupt_pos = blob_ref.offset as usize + BLOB_ENTRY_META_SIZE;
        file_data[corrupt_pos] ^= 0xFF; // Flip some bits

        // Test Method 1: Standard Access
        {
            let file = Arc::new(InMemFile::new(1, Bytes::from(file_data.clone())));
            let table = BlobTable::new(file).unwrap();
            let mut decryption_buf = vec![];

            let result = table.get(&blob_ref, &mut decryption_buf, None);
            assert!(
                result.is_err(),
                "Standard access: Should detect corrupted data"
            );
            assert!(matches!(result, Err(Error::InvalidChecksum(_))));
        }

        // Test Method 2: Preloaded Access
        {
            let table = BlobTable::from_bytes(Bytes::from(file_data.clone())).unwrap();
            let mut decompress_buf = vec![];
            let mut decrypt_buf = vec![];

            let result = table.get_from_preloaded(
                &blob_ref,
                compression_type != NO_COMPRESSION,
                enable_encryption,
                &mut decompress_buf,
                &mut decrypt_buf,
                encryption_key.clone(),
            );
            assert!(
                result.is_err(),
                "Preloaded access: Should detect corrupted data"
            );
            assert!(matches!(result, Err(Error::InvalidChecksum(_))));
        }

        // Test Method 3: Prefetched Access
        {
            let file = Arc::new(InMemFile::new(1, Bytes::from(file_data.clone())));
            let table = BlobTable::new(file).unwrap();
            let blob_tables: HashMap<u64, BlobTable> = [(1, table)].into();
            let mut prefetcher = BlobPrefetcher::new(Arc::new(blob_tables), 1000, encryption_key);

            let result = prefetcher.get(&blob_ref);
            assert!(
                result.is_err(),
                "Prefetched access: Should detect corrupted data"
            );
            assert!(matches!(result, Err(Error::InvalidChecksum(_))));
        }
    }

    /// Test boundary keys (smallest_key and biggest_key methods)
    #[test]
    fn test_blob_table_boundary_keys() {
        // Create a blob table with multiple entries in specific order
        let mut builder = BlobTableBuilder::new(1, 0, 0, 0, BLOB_BLOCK_SIZE, None);

        // Adding keys in non-sorted order to test the boundary keys
        let small_key = InnerKey::from_inner_buf(b"aaa_small_key");
        let medium_key = InnerKey::from_inner_buf(b"mmm_medium_key");
        let large_key = InnerKey::from_inner_buf(b"zzz_large_key");

        // Add test data for each key
        let test_data = b"test_data".to_vec();
        let encoded = Value::encode_buf(0, &[0], 0, &test_data);
        let value = Value::decode(encoded.as_slice());

        // Add entries in mixed order
        builder.add(medium_key, &value);
        builder.add(large_key, &value);
        builder.add(small_key, &value);

        // Create the blob table
        let file_data = builder.finish();
        let file = Arc::new(InMemFile::new(1, file_data.clone()));
        let table = BlobTable::new(file).unwrap();

        // Test smallest_key and biggest_key methods
        assert_eq!(
            table.smallest_key(),
            small_key,
            "smallest_key should return the lexicographically smallest key"
        );
        assert_eq!(
            table.biggest_key(),
            large_key,
            "biggest_key should return the lexicographically largest key"
        );
    }

    #[test]
    fn test_blob_table_getters() {
        // Create a blob table with specific properties
        let mut builder = BlobTableBuilder::new_with_checksum_type(
            42,               // specific id
            ZSTD_COMPRESSION, // specific compression type
            3,                // compression level
            1024,             // min blob size
            BLOB_BLOCK_SIZE,  // block size
            None,
            ChecksumType::Crc32,
        );

        // Add some test data to ensure non-zero total_blob_size
        let test_data = vec![b'x'; 2048]; // 2KB data
        let encoded = Value::encode_buf(0, &[0], 0, &test_data);
        let value = Value::decode(encoded.as_slice());
        builder.add(InnerKey::from_inner_buf(b"test_key"), &value);

        let file = Arc::new(InMemFile::new(42, builder.finish()));
        let table = BlobTable::new(file).unwrap();

        // Test all getter methods
        assert_eq!(table.id(), 42, "id() should return correct file id");
        assert_eq!(
            table.version(),
            BLOB_FORMAT_V1,
            "version() should return correct version"
        );
        assert!(table.size() > 0, "size() should return non-zero value");
        assert!(
            table.total_blob_size() > 0,
            "total_blob_size() should return non-zero value"
        );
        assert_eq!(
            table.compression_tp(),
            ZSTD_COMPRESSION,
            "compression_tp() should return correct type"
        );
        assert_eq!(
            table.min_blob_size(),
            1024,
            "min_blob_size() should return correct value"
        );
        assert_eq!(
            BlobTable::footer_size(),
            BLOB_TABLE_FOOTER_SIZE,
            "footer_size() should return correct size"
        );

        // Test smallest_biggest_key() returns same as individual getters
        let (small, big) = table.smallest_biggest_key();
        assert_eq!(
            small,
            table.smallest_key(),
            "smallest_key from tuple should match direct getter"
        );
        assert_eq!(
            big,
            table.biggest_key(),
            "biggest_key from tuple should match direct getter"
        );
    }
}
