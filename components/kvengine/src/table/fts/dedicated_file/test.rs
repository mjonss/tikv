// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    convert::TryFrom,
    io::Cursor,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::Result;
use bytes::Bytes;
use tantivy::Directory;
use tikv_util::codec::number::NumberEncoder;

use super::*;
use crate::table::{
    ChecksumType,
    file::{File, InMemFile},
    fts::{
        FtsCache,
        iter::{CommonPk, IntPk, PkReader},
    },
};

#[cfg(any(test, feature = "testexport"))]
#[cfg_attr(feature = "testexport", allow(unused))]
impl DedicatedFile<()> {
    pub fn from_buffer(buf: &[u8]) -> Result<EDedicatedFile> {
        static NEXT_TEST_FILE_ID: AtomicU64 = AtomicU64::new(1);
        let file_id = NEXT_TEST_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let file: Arc<dyn File> = Arc::new(InMemFile::new(file_id, Bytes::copy_from_slice(buf)));
        DedicatedFile::new(file, FtsCache::disabled())
    }
}

pub fn dummy_tantivy_dir() -> clara_fts::TrackedDirectory<tantivy::directory::RamDirectory> {
    let dir = clara_fts::TrackedDirectory::wrap(tantivy::directory::RamDirectory::default());
    let files = [
        ("meta.json", b"{}" as &[u8]),
        (".managed.json", b"{}"),
        ("seg_1.term", b"term"),
        ("seg_1.idx", b"idx"),
        ("seg_1.pos", b"pos"),
        ("seg_1.store", b"store"),
        ("seg_1.fast", b"fast"),
        ("seg_1.fieldnorm", b"fieldnorm"),
    ];
    for (name, data) in files {
        dir.atomic_write(Path::new(name), data).unwrap();
    }
    dir
}

#[tokio::test]
// Insert multiple handles so that DedicatedFile has multiple handle blocks,
// verify the count of handle_index_block_offset.
async fn test_finish_handle_block() -> Result<()> {
    let mut buffer = Vec::new();
    let mut options = builder::DedicatedFileBuilderOptions::default();
    options.handle_block_size = 1024; // set to 1 KB size
    let mut builder: builder::DedicatedFileBuilder<_, IntPk> =
        builder::DedicatedFileBuilder::new(Cursor::new(&mut buffer), options, 1, 0, b"test_lp")?;

    // Add enough data to trigger handle block flushing
    // Each integer PK requires approximately 8 + 8 + 1 = 17 bytes (PK + version +
    // delete_mark) Plus other overhead, we add 100,000 PKs to exceed the
    // 1MB limit, ensuring multiple handle blocks are created
    for i in 0..1000 {
        builder.add_pk(i, 1000, false)?;
    }

    let _summary = builder.finish(&dummy_tantivy_dir())?;

    let dedicated_file = DedicatedFile::from_buffer(&buffer)?;

    let dedicated_file = dedicated_file.as_int().unwrap();

    let footer = dedicated_file.footer();
    assert_eq!(footer.magic, FTS_DEDICATED_FILE_MAGIC);
    assert_eq!(footer.format, FTS_DEDICATED_FILE_FORMAT_V1);

    // Verify other metadata block offsets
    assert!(footer.pk_filter_block_offset > footer.iblock_offset);
    assert!(footer.prop_offset > footer.pk_filter_block_offset);

    // Verify properties can be loaded
    let props = dedicated_file.props();
    assert!(props.get_is_int_handle());
    assert_eq!(props.get_lp_key(), b"test_lp");

    // Core test: verify creation of multiple handle blocks
    // Load handle index block to check the count of hblock_offsets
    let iblock = dedicated_file.get_iblock()?;

    // Verify the hblock_offsets array
    let hblock_offsets = &iblock.hblock_offsets;
    let hblock_docids = &iblock.hblock_start_docid;

    // Since we added 100,000 PKs, each approximately 17 bytes, plus overhead,
    // the total data size is approximately 1.7MB. Therefore, multiple handle blocks
    // should be created (each block is limited to 1MB). At least 2 handle
    // blocks are expected.
    assert!(
        hblock_offsets.len() >= 2,
        "Expected at least 2 handle block offsets (multiple blocks), got {}",
        hblock_offsets.len()
    );

    // Verify that the offset is increasing
    for i in 1..hblock_offsets.len() {
        assert!(
            hblock_offsets[i] > hblock_offsets[i - 1],
            "Handle block offsets should be increasing: offset[{}]={} should be > offset[{}]={}",
            i,
            hblock_offsets[i],
            i - 1,
            hblock_offsets[i - 1]
        );
    }

    assert_eq!(
        hblock_docids.len(),
        hblock_offsets.len(),
        "Doc ID array must align with hblock offsets",
    );
    assert_eq!(hblock_docids.first().copied(), Some(0));
    assert_eq!(u64::from(*hblock_docids.last().unwrap()), props.pk_total);
    for i in 1..hblock_docids.len() {
        assert!(
            hblock_docids[i] > hblock_docids[i - 1],
            "Doc IDs must be strictly increasing: docid[{}]={} should be > docid[{}]={}",
            i,
            hblock_docids[i],
            i - 1,
            hblock_docids[i - 1]
        );
    }

    // Verify that the first offset is 0 (the start of the data block)
    assert_eq!(
        hblock_offsets[0], 0,
        "First handle block offset should be 0"
    );

    // Verify that the last offset should equal the start of the data block
    // (because the N+1 offset marks the end of the last handle block)
    let last_offset = hblock_offsets[hblock_offsets.len() - 1];
    assert_eq!(
        last_offset, footer.data_block_offset,
        "Last handle block offset {} should equal data_block_offset {}",
        last_offset, footer.data_block_offset
    );

    // Verify that the size between each handle block is reasonable
    for i in 1..hblock_offsets.len() - 1 {
        let block_size = hblock_offsets[i] - hblock_offsets[i - 1];
        // Each handle block should be close to or exceed the 1MB limit
        assert!(
            block_size >= 1024,
            "Handle block {} size {} should be close to the 1MB limit",
            i - 1,
            block_size
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_handle_rank() -> Result<()> {
    let mut buffer = Vec::new();
    let mut options = builder::DedicatedFileBuilderOptions::default();
    options.handle_block_size = 1;
    options.handle_rank_stride = 1;
    let mut builder: builder::DedicatedFileBuilder<_, IntPk> =
        builder::DedicatedFileBuilder::new(Cursor::new(&mut buffer), options, 1, 0, b"ranked_lp")?;

    for i in 0..1000 {
        builder.add_pk(i * 5, 100, false)?;
    }

    let _summary = builder.finish(&dummy_tantivy_dir())?;
    let dedicated_file = DedicatedFile::from_buffer(&buffer)?;
    let dedicated_file = dedicated_file.as_int().unwrap();
    let iblock = dedicated_file.get_iblock()?;

    let stride = iblock.get_hblock_handle_rank_stride() as usize;
    assert_eq!(stride, 1);
    let ranks = iblock.get_hblock_handle_rank();
    let total = dedicated_file.props().get_pk_total() as usize;
    let expected = total.div_ceil(stride) + 1;
    assert_eq!(ranks.len(), expected);
    assert_eq!(
        ranks.last().copied(),
        Some(iblock.hblock_start_key.len() as u32)
    );

    for doc_id in 0..1000 {
        let (pk_bytes, version, is_deleted) =
            dedicated_file.async_at(doc_id, u64::MAX).await?.unwrap();
        assert_eq!(version, 100);
        assert_eq!(is_deleted, 0u8);
        let decoded_pk = IntPk::decode(pk_bytes.as_ref())?;
        assert_eq!(decoded_pk, (doc_id * 5) as i64);
    }

    Ok(())
}

#[tokio::test]
async fn test_handle_rank_stride_boundary_within_bucket() -> Result<()> {
    let mut buffer = Vec::new();
    let mut options = builder::DedicatedFileBuilderOptions::default();
    options.handle_rank_stride = 10;
    // For Int handle, each PK contributes 8(pk)+8(ver)+1(del)=17 bytes to
    // current_size. 11 * 17 = 187 < 200, 12 * 17 = 204 >= 200, so the first
    // handle block will flush right after writing doc_id=11, and doc_id=12..
    // will be in the next block.
    options.handle_block_size = 200;
    let mut builder: builder::DedicatedFileBuilder<_, IntPk> = builder::DedicatedFileBuilder::new(
        Cursor::new(&mut buffer),
        options,
        1,
        0,
        b"rank_stride_boundary",
    )?;

    // total_pk must be > (rank_idx+1)*stride so that `hi` is not the sentinel.
    for i in 0..25 {
        builder.add_pk(i, 100, false)?;
    }

    let _summary = builder.finish(&dummy_tantivy_dir())?;
    let dedicated_file = DedicatedFile::from_buffer(&buffer)?;
    let dedicated_file = dedicated_file.as_int().unwrap();

    // doc_id=15 falls into rank bucket [10, 20), while the handle block boundary
    // happens at doc_id=12. This used to select the wrong handle block and could
    // panic by slicing past pk_data.
    let (pk_bytes, version, is_deleted) = dedicated_file.async_at(15, u64::MAX).await?.unwrap();
    assert_eq!(version, 100);
    assert_eq!(is_deleted, 0u8);
    let decoded_pk = IntPk::decode(pk_bytes.as_ref())?;
    assert_eq!(decoded_pk, 15);

    Ok(())
}

#[tokio::test]
async fn test_docid_accessors() -> Result<()> {
    let mut buffer = Vec::new();
    let mut options = builder::DedicatedFileBuilderOptions::default();
    options.handle_block_size = 32;
    let mut builder: builder::DedicatedFileBuilder<_, IntPk> =
        builder::DedicatedFileBuilder::new(Cursor::new(&mut buffer), options, 1, 0, b"docid_lp")?;

    for i in 0..6 {
        builder.add_pk(i, 100 - i as u64, i % 2 == 1)?;
    }

    let _summary = builder.finish(&dummy_tantivy_dir())?;

    let dedicated_file = DedicatedFile::from_buffer(&buffer)?;
    let dedicated_file = dedicated_file.as_int().unwrap();

    for doc_id in 0..6 {
        let got = dedicated_file.async_at(doc_id, u64::MAX).await?;

        if doc_id % 2 == 1 {
            assert!(got.is_none());
            continue;
        }

        let (pk_bytes, version, deleted) = got.unwrap();
        let decoded_pk = IntPk::decode(pk_bytes.as_ref())?;
        assert_eq!(decoded_pk, doc_id as i64);
        assert_eq!(version, 100 - doc_id as u64);
        assert_eq!(deleted, 0u8);
    }

    Ok(())
}

#[tokio::test]
async fn test_handle_block_flush_does_not_split_pk_versions() -> Result<()> {
    let mut buffer = Vec::new();
    let mut options = builder::DedicatedFileBuilderOptions::default();
    options.handle_block_size = 50;
    let mut builder: builder::DedicatedFileBuilder<_, IntPk> =
        builder::DedicatedFileBuilder::new(Cursor::new(&mut buffer), options, 1, 0, b"lp")?;

    builder.add_pk(1, 100, false)?;
    builder.add_pk(2, 100, false)?;
    builder.add_pk(3, 100, false)?;
    builder.add_pk(3, 99, false)?;
    builder.add_pk(4, 100, false)?;

    let _summary = builder.finish(&dummy_tantivy_dir())?;

    let dedicated_file = DedicatedFile::from_buffer(&buffer)?;
    let dedicated_file = dedicated_file.as_int().unwrap();
    let iblock = dedicated_file.get_iblock()?;

    // 4 triggers a new handle block flush, so the second block should start at
    // pk=4.
    assert_eq!(iblock.hblock_start_key.len(), 2);
    assert_eq!(IntPk::decode(iblock.hblock_start_key[0].as_slice())?, 1);
    assert_eq!(IntPk::decode(iblock.hblock_start_key[1].as_slice())?, 4);

    // doc_id=3 is (pk=3, version=99), which should be shadowed by doc_id=2 (pk=3,
    // version=100).
    assert!(dedicated_file.async_at(3, u64::MAX).await?.is_none());
    assert!(
        dedicated_file
            .has_newer_version(&IntPk::encode(3), 99, 100)
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_int_handle_newer_version() -> Result<()> {
    let mut buffer = Vec::new();
    let mut options = builder::DedicatedFileBuilderOptions::default();
    options.handle_block_size = 1024; // set to 1 KB size
    let mut builder: builder::DedicatedFileBuilder<_, IntPk> =
        builder::DedicatedFileBuilder::new(Cursor::new(&mut buffer), options, 1, 0, b"test_lp")?;

    for i in 0..20 {
        builder.add_pk(100, 50 - i as u64, false)?;
    }

    let _summary = builder.finish(&dummy_tantivy_dir())?;

    let dedicated_file = DedicatedFile::from_buffer(&buffer)?;

    let dedicated_file = dedicated_file.as_int().unwrap();

    let result = dedicated_file
        .has_newer_version(&IntPk::encode(100), 40, 50)
        .await?;
    assert!(result);

    Ok(())
}

#[tokio::test]
async fn test_common_handle_newer_version() -> Result<()> {
    let mut buffer = Vec::new();
    let mut options = builder::DedicatedFileBuilderOptions::default();
    options.handle_block_size = 1024; // set to 1 KB size
    let mut builder: builder::DedicatedFileBuilder<_, CommonPk> =
        builder::DedicatedFileBuilder::new(Cursor::new(&mut buffer), options, 1, 0, b"test_lp")?;

    let handle = "handle_111";
    for i in 0..20 {
        builder.add_pk(handle.as_bytes(), 50 - i as u64, false)?; // version=100, delete_mark=false
    }

    let _summary = builder.finish(&dummy_tantivy_dir())?;

    let dedicated_file = DedicatedFile::from_buffer(&buffer)?;

    let dedicated_file = dedicated_file.as_common().unwrap();

    let result = dedicated_file
        .has_newer_version(handle.as_bytes(), 40, 50)
        .await?;
    assert!(result);

    Ok(())
}

#[tokio::test]
async fn test_multi_block_int_handle_search_with_negative_positive() -> Result<()> {
    let mut buffer = Vec::new();
    let options = builder::DedicatedFileBuilderOptions::default();
    let mut builder: builder::DedicatedFileBuilder<_, IntPk> = builder::DedicatedFileBuilder::new(
        Cursor::new(&mut buffer),
        options,
        1,
        0,
        b"test_lp_mixed",
    )?;

    // Add mixed negative and positive integer PKs in ascending order
    // Range: -75000 to +74999 (total 150,000 PKs)
    // This ensures we have both negative and positive values
    for i in -75i64..75i64 {
        builder.add_pk(i, 100, false)?; // Mark every 1000th PK as deleted
    }

    let _summary = builder.finish(&dummy_tantivy_dir())?;

    let dedicated_file = DedicatedFile::from_buffer(&buffer)?;

    let dedicated_file = dedicated_file.as_int().unwrap();

    // Test cases with both negative and positive handles
    let test_cases = vec![
        (-50i64, "large negative"),
        (-1i64, "small negative"),
        (0i64, "zero"),
        (1i64, "small positive"),
        (50i64, "large positive"),
    ];

    for (handle_value, _description) in test_cases {
        let result = dedicated_file
            .has_newer_version(&IntPk::encode(handle_value), 40, 100)
            .await?;
        assert!(result);
    }

    // Test boundary cases
    let boundary_cases = vec![(-75i64, "minimum value"), (74i64, "maximum value")];

    for (handle_value, _description) in boundary_cases {
        let result = dedicated_file
            .has_newer_version(&IntPk::encode(handle_value), 40, 100)
            .await?;
        assert!(result);
    }

    // Test out-of-range cases
    let out_of_range_cases = vec![(-1000i64, "below minimum"), (1000i64, "above maximum")];

    for (handle_value, _description) in out_of_range_cases {
        let result = dedicated_file
            .has_newer_version(&IntPk::encode(handle_value), 40, 100)
            .await?;
        assert!(!result);
    }

    Ok(())
}

#[tokio::test]
async fn test_handle_block_checksum_corruption() -> Result<()> {
    // This test verifies checksum mechanism and data integrity checks
    // Build a valid DedicatedFile with checksum enabled
    let mut buffer = Vec::new();
    let options = builder::DedicatedFileBuilderOptions::default();
    let mut builder: builder::DedicatedFileBuilder<_, IntPk> =
        builder::DedicatedFileBuilder::new(&mut buffer, options, 1, 0, b"test_lp_checksum")?;

    // Add some test data
    for i in 0..1000 {
        builder.add_pk(i, 10000 - i as u64, i % 50 == 0)?;
    }

    let _summary = builder.finish(&dummy_tantivy_dir())?;

    // Test Case 1: Handle Block Corruption
    {
        let mut corrupted_buffer = buffer.clone();

        // Corrupt a byte in the handle block area (near the beginning)
        corrupted_buffer[20] ^= 0xFF;

        // DedicatedFile::new() should succeed because metadata and footer are intact
        let dedicated_file = DedicatedFile::from_buffer(&corrupted_buffer)?;

        let dedicated_file = dedicated_file.as_int().unwrap();

        // Accessing the corrupted handle block should fail
        let err = dedicated_file
            .has_newer_version(&IntPk::encode(0), 50, 200)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("Handle block checksum mismatch"));
    }

    Ok(())
}

#[tokio::test]
async fn test_footer_marshal_unmarshal_with_checksum() {
    let mut f = DedicatedFileFooter::new();
    f.checksum_type = ChecksumType::Crc32c;
    f.checksum_other_meta = 0xA1B2_C3D4;
    f.data_block_offset = 0x10_20_30_40;
    f.pk_filter_block_offset = 0x20_30_40_50;
    f.iblock_offset = 0x30_40_50_60;
    f.prop_offset = 0x40_50_60_70;

    let mut buf = Vec::new();
    f.marshal(&mut buf).unwrap();

    // Roundtrip test - unmarshal and verify all fields match
    let f2 = DedicatedFileFooter::unmarshal(&buf).unwrap();
    assert_eq!(f2.format, f.format);
    assert_eq!(f2.checksum_type.value(), f.checksum_type.value());
    assert_eq!(f2.checksum_other_meta, f.checksum_other_meta);
    assert_eq!(f2.data_block_offset, f.data_block_offset);
    assert_eq!(f2.pk_filter_block_offset, f.pk_filter_block_offset);
    assert_eq!(f2.iblock_offset, f.iblock_offset);
    assert_eq!(f2.prop_offset, f.prop_offset);
    assert_eq!(f2.magic, f.magic);

    // Modify checksum field should cause checksum mismatch
    let mut buf2 = buf.clone();
    buf2[8] ^= 0xFF; // Flip a bit in checksum field
    let result = DedicatedFileFooter::unmarshal(&buf2);
    assert!(
        result.is_err(),
        "Expected checksum mismatch when checksum field is corrupted"
    );
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("FtsDedicatedFile footer checksum mismatch"),
        "Error should mention checksum mismatch"
    );
}

#[tokio::test]
async fn test_last_int_handle_block_access() -> Result<()> {
    // This test specifically targets the bug where accessing the last handle block
    // would cause an out-of-bounds error due to incorrect N vs N+1 offset array
    // handling

    let mut buffer = Vec::new();
    let mut options = builder::DedicatedFileBuilderOptions::default();
    options.handle_block_size = 1024; // set to 1 KB size
    let mut builder: builder::DedicatedFileBuilder<_, IntPk> =
        builder::DedicatedFileBuilder::new(Cursor::new(&mut buffer), options, 1, 0, b"test_lp")?;

    // Add enough data to create multiple handle blocks
    // With small block size, this should create several blocks
    for i in 0..1000 {
        builder.add_pk(i, 200, false)?;
    }

    let _summary = builder.finish(&dummy_tantivy_dir())?;

    // Create IaFile from buffer using the same pattern as other tests
    let dedicated_file = DedicatedFile::from_buffer(&buffer)?;

    let dedicated_file = dedicated_file.as_int().unwrap();

    // Test PKs that should definitely be in the last block
    // We need to test PKs that are >= last_block_start_pk
    let test_cases = vec![
        990i64, // First PK in last block
        995i64, // Second PK in last block (if exists)
        999i64, // Highest PK we added (should be in last block)
    ];

    let handle_index = dedicated_file.get_iblock()?;

    for pk in test_cases {
        let mut key = Vec::with_capacity(64);
        key.encode_i64(pk).unwrap();
        let Some(idx) = dedicated_file.find_hblock_offset(key.as_slice())? else {
            bail!("Should find handle block containing PK {:?}", pk);
        };
        assert_eq!(idx, handle_index.hblock_start_key.len() - 1);

        // This call should not panic or cause out-of-bounds errors
        let result = dedicated_file
            .has_newer_version(&IntPk::encode(pk), 50, 200)
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap());

        let result = dedicated_file
            .has_newer_version(&IntPk::encode(pk), 300, 200)
            .await;
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }
    Ok(())
}

#[tokio::test]
async fn test_last_common_handle_block_access() -> Result<()> {
    // This test specifically targets the bug where accessing the last handle block
    // would cause an out-of-bounds error due to incorrect N vs N+1 offset array
    // handling This version tests common handles (string PKs) instead of
    // integer PKs

    let mut buffer = Vec::new();
    let options = builder::DedicatedFileBuilderOptions::default();
    let mut builder: builder::DedicatedFileBuilder<_, CommonPk> =
        builder::DedicatedFileBuilder::new(
            &mut buffer, // Use &mut buffer instead of Cursor
            options,
            1,
            0,
            b"test_lp",
        )?;

    // Add enough data to create multiple handle blocks
    // Use string PKs with consistent format for proper lexicographic ordering
    // Use fewer PKs but still enough to create multiple blocks
    for i in 0..10000 {
        let pk_str = format!("handle_{:08}", i); // e.g., "handle_00000000", "handle_00000001"
        builder.add_pk(pk_str.as_bytes(), 200, false)?;
    }

    let _summary = builder.finish(&dummy_tantivy_dir())?;

    // Create IaFile from buffer using the same pattern as other tests
    let dedicated_file = DedicatedFile::from_buffer(&buffer)?;

    let dedicated_file = dedicated_file.as_common().unwrap();

    // Test PKs that should definitely be in the last block
    // For common handles, we need to be more careful about which PKs are in the
    // last block Let's test PKs that are very likely to be in the last few
    // blocks
    let test_cases = vec![
        b"handle_00009997".to_vec(), // Near the end
        b"handle_00009998".to_vec(), // Near the end
        b"handle_00009999".to_vec(), // Highest PK we added (should be in last block)
    ];

    let handle_index = dedicated_file.get_iblock()?;

    for pk_bytes in test_cases {
        let Some(idx) = dedicated_file.find_hblock_offset(pk_bytes.as_slice())? else {
            bail!("Should find handle block containing PK {:?}", pk_bytes);
        };
        assert_eq!(idx, handle_index.hblock_start_key.len() - 1);

        // This call should not panic or cause out-of-bounds errors
        let result = dedicated_file.has_newer_version(&pk_bytes, 50, 200).await;
        assert!(result.is_ok());
        assert!(result.unwrap());

        let result = dedicated_file.has_newer_version(&pk_bytes, 300, 200).await;
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }
    Ok(())
}

#[test]
fn test_footer_offsets_non_monotonic_should_error() -> Result<()> {
    let mut buffer = Vec::new();
    let mut builder: builder::DedicatedFileBuilder<_, IntPk> = builder::DedicatedFileBuilder::new(
        Cursor::new(&mut buffer),
        builder::DedicatedFileBuilderOptions::default(),
        1,
        0,
        b"test_lp",
    )?;
    builder.add_pk(1, 10, false)?;
    let _summary = builder.finish(&dummy_tantivy_dir())?;

    let footer_start = buffer.len() - FTS_DEDICATED_FILE_FOOTER_SIZE;
    let footer = DedicatedFileFooter::unmarshal(&buffer[footer_start..])?;
    assert!(footer.iblock_offset > 0);

    let mut bad_footer = footer;
    bad_footer.pk_filter_block_offset = footer.iblock_offset - 1;

    let mut bad_footer_bytes = Vec::new();
    bad_footer.marshal(&mut bad_footer_bytes)?;
    buffer[footer_start..].copy_from_slice(&bad_footer_bytes);

    let err = DedicatedFile::from_buffer(&buffer).unwrap_err();
    assert!(
        err.to_string()
            .contains("DedicatedFile footer offsets not monotonic")
    );
    Ok(())
}

#[test]
fn test_footer_offsets_out_of_bounds_should_error() -> Result<()> {
    let mut buffer = Vec::new();
    let mut builder: builder::DedicatedFileBuilder<_, IntPk> = builder::DedicatedFileBuilder::new(
        Cursor::new(&mut buffer),
        builder::DedicatedFileBuilderOptions::default(),
        1,
        0,
        b"test_lp",
    )?;
    builder.add_pk(1, 10, false)?;
    let _summary = builder.finish(&dummy_tantivy_dir())?;

    let footer_start = buffer.len() - FTS_DEDICATED_FILE_FOOTER_SIZE;
    let footer = DedicatedFileFooter::unmarshal(&buffer[footer_start..])?;

    let mut bad_footer = footer;
    let footer_start_off = u64::try_from(footer_start)?;
    bad_footer.prop_offset = footer_start_off + 1;

    let mut bad_footer_bytes = Vec::new();
    bad_footer.marshal(&mut bad_footer_bytes)?;
    buffer[footer_start..].copy_from_slice(&bad_footer_bytes);

    let err = DedicatedFile::from_buffer(&buffer).unwrap_err();
    assert!(
        err.to_string()
            .contains("DedicatedFile footer offsets out of bounds")
    );
    Ok(())
}

#[test]
fn test_footer_metadata_offset_out_of_bounds_should_error() -> Result<()> {
    let mut buffer = Vec::new();
    let mut builder: builder::DedicatedFileBuilder<_, IntPk> = builder::DedicatedFileBuilder::new(
        Cursor::new(&mut buffer),
        builder::DedicatedFileBuilderOptions::default(),
        1,
        0,
        b"test_lp",
    )?;
    builder.add_pk(1, 10, false)?;
    let _summary = builder.finish(&dummy_tantivy_dir())?;

    let footer_start = buffer.len() - FTS_DEDICATED_FILE_FOOTER_SIZE;
    let footer = DedicatedFileFooter::unmarshal(&buffer[footer_start..])?;

    let mut bad_footer = footer;
    let footer_start_off = u64::try_from(footer_start)?;
    bad_footer.iblock_offset = footer_start_off + 1;

    let mut bad_footer_bytes = Vec::new();
    bad_footer.marshal(&mut bad_footer_bytes)?;
    buffer[footer_start..].copy_from_slice(&bad_footer_bytes);

    let err = DedicatedFile::from_buffer(&buffer).unwrap_err();
    assert!(
        err.to_string()
            .contains("DedicatedFile footer offsets not monotonic")
    );
    Ok(())
}
