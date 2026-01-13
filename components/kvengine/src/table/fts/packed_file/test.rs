// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    convert::TryFrom,
    io::Cursor,
    sync::atomic::{AtomicU64, Ordering},
};

use aligned_vec::{AVec, ConstAlign};
use bytes::Bytes;
use schema::schema::StorageClass;

use super::*;
use crate::table::{
    SnapVersion,
    file::{File, FileMmapGuard, InMemFile, LocalFile, MmapData},
    fts::{
        dedicated_file::test::dummy_tantivy_dir,
        iter::{IntPk, PkReader},
    },
};

#[cfg(any(test, feature = "testexport"))]
#[cfg_attr(feature = "testexport", allow(unused))]
impl PackedFile {
    pub fn from_buffer(buf: &[u8]) -> Result<PackedFile> {
        static NEXT_TEST_FILE_ID: AtomicU64 = AtomicU64::new(1);
        let file_id = NEXT_TEST_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let bytes = Bytes::copy_from_slice(buf);
        let file = Arc::new(InMemFile::new(file_id, bytes));
        PackedFile::new(file, FtsCache::disabled())
    }
}

#[derive(Clone)]
struct SegmentedMmapFile {
    id: u64,
    data: Bytes,
    // Must contain N+1 monotonically increasing offsets, where the last one is
    // the end offset of the last segment.
    segment_offsets: Arc<Vec<u64>>,
}

impl SegmentedMmapFile {
    fn new(id: u64, data: Bytes, segment_offsets: Vec<u64>) -> Self {
        Self {
            id,
            data,
            segment_offsets: Arc::new(segment_offsets),
        }
    }
}

#[async_trait::async_trait]
impl File for SegmentedMmapFile {
    fn id(&self) -> u64 {
        self.id
    }

    fn size(&self) -> u64 {
        self.data.len() as u64
    }

    fn read(&self, off: u64, length: usize) -> crate::table::Result<Bytes> {
        if off
            .checked_add(length as u64)
            .is_none_or(|end| end > self.size())
        {
            return Err(crate::table::Error::InvalidFileSize);
        }
        Ok(self.data.slice(off as usize..off as usize + length))
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> crate::table::Result<()> {
        let data = self.read(offset, buf.len())?;
        buf.copy_from_slice(&data);
        Ok(())
    }

    fn mmap(&self) -> crate::table::Result<MmapData> {
        Ok(MmapData::InMem(self.data.clone()))
    }

    fn mmap2(&self) -> crate::table::Result<Bytes> {
        Ok(self.data.clone())
    }

    async fn mmap_range(
        &self,
        offset: u64,
        length: usize,
    ) -> crate::table::Result<(Bytes, FileMmapGuard)> {
        if offset
            .checked_add(length as u64)
            .is_none_or(|end| end > self.size())
        {
            return Err(crate::table::Error::InvalidFileSize);
        }
        if length == 0 {
            return Ok((Bytes::new(), FileMmapGuard::None));
        }

        // Mimic IA: mmap from a per-segment file, where the in-memory address
        // alignment depends on the offset within the segment (not the absolute
        // file offset).
        let end = offset + length as u64;
        let segment_len = self.segment_offsets.len();
        if segment_len < 2 {
            return Err(crate::table::Error::InvalidFileSize);
        }
        let pos = crate::table::search(segment_len, |i| offset < self.segment_offsets[i]) - 1;
        if self.segment_offsets[pos + 1] < end {
            return Err(crate::table::Error::Other(format!(
                "read more than one segment: start {}, end {}",
                offset, end
            )));
        }
        let seg_start = self.segment_offsets[pos];
        let seg_end = self.segment_offsets[pos + 1];

        let segment_view = self.data.slice(seg_start as usize..seg_end as usize);
        let segment_data = segment_view.as_ref();
        let mut aligned = AVec::<u8, ConstAlign<8>>::with_capacity(8, segment_data.len());
        aligned.extend_from_slice(segment_data);
        let segment_bytes = Bytes::from_owner(aligned);

        let offset_in_segment = (offset - seg_start) as usize;
        Ok((
            segment_bytes.slice(offset_in_segment..offset_in_segment + length),
            FileMmapGuard::None,
        ))
    }

    fn storage_class(&self) -> StorageClass {
        StorageClass::Unspecified
    }

    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

#[tokio::test]
async fn from_local_file() -> Result<()> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let file_path = tmp_dir.path().join("fts_local.bin");
    let file = std::fs::File::create(&file_path)?;
    let mut builder = PackedFileBuilder::new(
        file,
        PackedFileBuilderOptions {
            block_size: 1024,
            checksum_type: ChecksumType::Crc32c,
        },
    );

    builder.start_lp(15, 0, true, b"lp_local")?;
    builder.add_pk_int(1, 10, false)?;
    builder.add_pk_int(2, 9, true)?;
    let tantivy_layout = dummy_tantivy_dir();
    builder.finish_lp(&tantivy_layout)?;
    builder.finish(SnapVersion::zero())?;

    let file = LocalFile::open(2024, file_path.clone())?;
    let packed = PackedFile::new(Arc::new(file), FtsCache::disabled())?;

    let lp = packed.cached_get_lp(b"lp_local").await?.unwrap();
    let lp = lp.as_int_lp().unwrap();
    assert_eq!(lp.props().get_table_id(), 15);
    assert_eq!(lp.props().get_n_pk(), 2);
    assert!(lp.has_newer_version(&IntPk::encode(1), 5, 20).await?);
    assert!(lp.has_newer_version(&IntPk::encode(2), 1, 20).await?);
    assert!(!lp.has_newer_version(&IntPk::encode(2), 9, 20).await?);

    assert!(packed.cached_get_lp(b"missing").await?.is_none());
    Ok(())
}

#[test]
fn footer_marshal_unmarshal_with_checksum() {
    let mut f = PackedFileFooter::new();
    f.checksum_type = ChecksumType::Crc32c;
    f.checksum_other_meta = 0xA1B2_C3D4;
    f.index_block_offset = 0x10_20_30_40;
    f.lp_filter_block_offset = 0x20_30_40_50;
    f.prop_offset = 0x30_40_50_60;

    let mut buf = Vec::new();
    f.marshal(&mut buf).unwrap();

    // Roundtrip
    let f2 = PackedFileFooter::unmarshal(&buf).unwrap();
    assert_eq!(f2.format, f.format);
    assert_eq!(f2.checksum_type.value(), f.checksum_type.value());
    assert_eq!(f2.checksum_other_meta, f.checksum_other_meta);
    assert_eq!(f2.index_block_offset, f.index_block_offset);
    assert_eq!(f2.lp_filter_block_offset, f.lp_filter_block_offset);
    assert_eq!(f2.prop_offset, f.prop_offset);
    assert_eq!(f2.magic, f.magic);

    // Modify some byte should cause checksum mismatch
    let mut buf2 = buf.clone();
    buf2[8] ^= 0xFF; // Flip a bit in checksum field
    PackedFileFooter::unmarshal(&buf2).unwrap_err();

    // Modify some data field should also cause checksum mismatch
    let mut buf2 = buf.clone();
    buf2[15] ^= 0xFF;
    PackedFileFooter::unmarshal(&buf2).unwrap_err();
}

#[test]
fn footer_unmarshal_invalid_size() {
    // Too short
    PackedFileFooter::unmarshal(&[]).unwrap_err();
    // Too long
    let mut v = vec![0u8; FTS_PACKED_FILE_FOOTER_SIZE + 1];
    PackedFileFooter::unmarshal(&v).unwrap_err();
    // Off by one short
    v.truncate(FTS_PACKED_FILE_FOOTER_SIZE - 1);
    PackedFileFooter::unmarshal(&v).unwrap_err();
}

#[test]
fn footer_unmarshal_invalid_magic() {
    let mut f = PackedFileFooter::new();
    f.checksum_type = ChecksumType::Crc32c; // any type
    let mut buf = Vec::new();
    f.marshal(&mut buf).unwrap();
    // Corrupt magic
    buf[28..32].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
    PackedFileFooter::unmarshal(&buf).unwrap_err();
}

#[tokio::test]
async fn test_int_pk() -> Result<()> {
    let mut buffer = Vec::new();
    let mut builder = PackedFileBuilder::new(
        Cursor::new(&mut buffer),
        PackedFileBuilderOptions::default(),
    );

    // Create test data with int PKs
    // First LP: lp1
    builder.start_lp(1, 0, true, b"lp1")?;
    builder.add_pk_int(1, 100, false)?;
    builder.add_pk_int(2, 99, false)?;
    builder.add_pk_int(3, 98, true)?;
    let tantivy_layout = dummy_tantivy_dir();
    builder.finish_lp(&tantivy_layout)?;

    // Second LP: lp2
    builder.start_lp(1, 1, true, b"lp2")?;
    // Include an overlapping PK (1) that also exists in lp1 to ensure
    // LPs are independent even if PK bytes are the same.
    builder.add_pk_int(1, 60, false)?;
    builder.add_pk_int(10, 50, false)?;
    builder.add_pk_int(20, 49, false)?;
    let tantivy_layout = dummy_tantivy_dir();
    builder.finish_lp(&tantivy_layout)?;

    builder.finish(SnapVersion::zero())?;

    let packed_file = PackedFile::from_buffer(&buffer)?;

    let lp1 = packed_file.cached_get_lp(b"lp1").await?.unwrap();
    let lp1 = lp1.as_int_lp().unwrap();
    assert_eq!(lp1.lp_key().as_ref(), b"lp1");
    assert_eq!(lp1.props().get_table_id(), 1);
    assert_eq!(lp1.props().get_n_pk(), 3);
    assert_eq!(lp1.props().get_is_int_handle(), true);
    assert!(lp1.has_newer_version(&IntPk::encode(1), 50, 150).await?);
    assert!(!lp1.has_newer_version(&IntPk::encode(1), 100, 150).await?);
    assert!(lp1.has_newer_version(&IntPk::encode(3), 10, 150).await?);
    assert!(!lp1.has_newer_version(&IntPk::encode(3), 98, 150).await?);

    let expected_lp1 = [
        (0usize, 1i64, 100u64, 0u8),
        (1, 2, 99, 0u8),
        (2, 3, 98, 1u8),
    ];
    for (doc_id, pk, version, deleted) in expected_lp1 {
        let got = lp1.async_at(doc_id, u64::MAX).await?;
        if deleted != 0 {
            assert!(got.is_none());
            continue;
        }

        let (pk_bytes, ver, del) = got.unwrap();
        assert_eq!(IntPk::decode(pk_bytes.as_ref())?, pk);
        assert_eq!(ver, version);
        assert_eq!(del, 0u8);
    }

    let lp2 = packed_file.cached_get_lp(b"lp2").await?.unwrap();
    let lp2 = lp2.as_int_lp().unwrap();
    assert_eq!(lp2.lp_key().as_ref(), b"lp2");
    assert_eq!(lp2.props().get_table_id(), 1);
    assert_eq!(lp2.props().get_n_pk(), 3);
    assert!(lp2.has_newer_version(&IntPk::encode(1), 50, 150).await?);
    assert!(!lp2.has_newer_version(&IntPk::encode(1), 60, 150).await?);
    assert!(lp2.has_newer_version(&IntPk::encode(10), 10, 150).await?);
    assert!(!lp2.has_newer_version(&IntPk::encode(10), 50, 150).await?);

    let missing = packed_file.cached_get_lp(b"lp_missing").await?;
    assert!(missing.is_none());
    assert_eq!(packed_file.n_data_blocks()?, 1);
    Ok(())
}

#[tokio::test]
async fn test_common_pk() -> Result<()> {
    let mut buffer = Vec::new();
    let mut builder = PackedFileBuilder::new(
        Cursor::new(&mut buffer),
        PackedFileBuilderOptions::default(),
    );

    // Create test data with common PKs
    builder.start_lp(1, 0, false, b"lp_common1")?;
    builder.add_pk_common(b"pk_a", 200, false)?;
    builder.add_pk_common(b"pk_b", 199, false)?;
    builder.add_pk_common(b"pk_c", 198, true)?;
    let tantivy_layout = dummy_tantivy_dir();
    builder.finish_lp(&tantivy_layout)?;

    builder.finish(SnapVersion::zero())?;

    let packed_file = PackedFile::from_buffer(&buffer)?;

    let lp = packed_file.cached_get_lp(b"lp_common1").await?.unwrap();
    let lp = lp.as_common_lp().unwrap();
    assert_eq!(lp.props().get_table_id(), 1);
    assert_eq!(lp.props().get_n_pk(), 3);
    assert_eq!(lp.props().get_is_int_handle(), false);
    assert!(lp.has_newer_version(b"pk_a", 150, 250).await?);
    assert!(!lp.has_newer_version(b"pk_a", 200, 250).await?);
    assert!(lp.has_newer_version(b"pk_b", 150, 250).await?);
    assert!(lp.has_newer_version(b"pk_c", 150, 300).await?);
    assert!(!lp.has_newer_version(b"pk_c", 198, 300).await?);
    let expected_common = [
        (0usize, b"pk_a".as_slice(), 200u64, 0u8),
        (1, b"pk_b".as_slice(), 199, 0u8),
        (2, b"pk_c".as_slice(), 198, 1u8),
    ];
    for (doc_id, pk, version, deleted) in expected_common {
        let got = lp.async_at(doc_id, u64::MAX).await?;
        if deleted != 0 {
            assert!(got.is_none());
            continue;
        }

        let (pk_bytes, ver, del) = got.unwrap();
        assert_eq!(pk_bytes.as_ref(), pk);
        assert_eq!(ver, version);
        assert_eq!(del, 0u8);
    }
    assert_eq!(packed_file.n_data_blocks()?, 1);
    Ok(())
}

#[tokio::test]
async fn test_mixed_int_and_common_pk() -> Result<()> {
    // Build a packed file containing exactly two LPs: one int-PK LP and one
    // common-PK LP.
    let mut buffer = Vec::new();
    let mut builder = PackedFileBuilder::new(
        Cursor::new(&mut buffer),
        PackedFileBuilderOptions {
            block_size: 1024,
            checksum_type: ChecksumType::Crc32c,
        },
    );

    // Int PK LP: lp_int (will be added after lp_comm to respect ordering)
    // Common PK LP: lp_comm comes first lexicographically ("lp_comm" < "lp_int")
    builder.start_lp(1, 0, false, b"lp_comm")?;
    builder.add_pk_common(b"a_key", 500, false)?;
    builder.add_pk_common(b"a_key", 450, false)?; // older same key
    builder.add_pk_common(b"b_key", 400, true)?;
    builder.add_pk_common(b"c_key", 350, false)?;
    let dir = dummy_tantivy_dir();
    builder.finish_lp(&dir)?;

    builder.start_lp(1, 0, true, b"lp_int")?;
    builder.add_pk_int(10, 300, false)?; // Newest
    builder.add_pk_int(10, 250, false)?; // Older version same PK
    builder.add_pk_int(11, 200, true)?; // A delete mark PK
    let dir = dummy_tantivy_dir();
    builder.finish_lp(&dir)?;

    builder.finish(SnapVersion::zero())?;

    let packed_file = PackedFile::from_buffer(&buffer)?;

    let lp_int = packed_file.cached_get_lp(b"lp_int").await?.unwrap();
    let lp_int = lp_int.as_int_lp().unwrap();
    let lp_comm = packed_file.cached_get_lp(b"lp_comm").await?.unwrap();
    let lp_comm = lp_comm.as_common_lp().unwrap();
    assert_eq!(lp_int.lp_key().as_ref(), b"lp_int");
    assert_eq!(lp_comm.lp_key().as_ref(), b"lp_comm");
    assert_eq!(lp_int.props().get_n_pk(), 3); // 10x2 + 11
    assert_eq!(lp_comm.props().get_n_pk(), 4); // a_key x2 + b_key + c_key

    // Int PK checks
    // pk10 has versions 300 & 250
    assert!(
        lp_int
            .has_newer_version(&IntPk::encode(10), 200, 400)
            .await?
    ); // 300,250 >200
    assert!(
        lp_int
            .has_newer_version(&IntPk::encode(10), 250, 400)
            .await?
    ); // 300 >250
    assert!(
        !lp_int
            .has_newer_version(&IntPk::encode(10), 300, 400)
            .await?
    ); // none >300
    assert!(
        lp_int
            .has_newer_version(&IntPk::encode(10), 200, 299)
            .await?
    ); // 250 fits <=299 >200
    assert!(
        !lp_int
            .has_newer_version(&IntPk::encode(10), 200, 249)
            .await?
    ); // neither 300 nor 250 <=249
    // pk11 only one version 200
    assert!(
        lp_int
            .has_newer_version(&IntPk::encode(11), 150, 400)
            .await?
    );
    assert!(
        !lp_int
            .has_newer_version(&IntPk::encode(11), 200, 400)
            .await?
    );

    // Common PK checks
    assert!(lp_comm.has_newer_version(b"a_key", 100, 600).await?); // 500,450
    assert!(lp_comm.has_newer_version(b"a_key", 450, 600).await?); // 500
    assert!(!lp_comm.has_newer_version(b"a_key", 500, 600).await?);
    assert!(lp_comm.has_newer_version(b"b_key", 300, 600).await?); // 400
    assert!(!lp_comm.has_newer_version(b"b_key", 400, 600).await?);
    assert!(lp_comm.has_newer_version(b"c_key", 300, 600).await?); // 350
    assert!(!lp_comm.has_newer_version(b"c_key", 350, 600).await?);

    // Cross-LP negative: keys should not appear in other LP
    let none = packed_file.cached_get_lp(b"nonexistent_lp").await?;
    assert!(none.is_none());
    assert_eq!(packed_file.n_data_blocks()?, 1);
    Ok(())
}

#[tokio::test]
async fn test_builder_multiple_data_blocks() -> Result<()> {
    // Force extremely small block size so each LP will reside in its own data
    // block.
    let options = PackedFileBuilderOptions {
        block_size: 1,
        checksum_type: ChecksumType::Crc32c,
    };

    let mut buffer = Vec::new();
    let mut builder = PackedFileBuilder::new(Cursor::new(&mut buffer), options);

    // LP keys must be in ascending order: lp_a < lp_b < lp_c
    // LP 1: lp_a (int PKs)
    builder.start_lp(1, 0, true, b"lp_a")?;
    builder.add_pk_int(1, 300, false)?; // Single version
    builder.add_pk_int(2, 250, true)?; // Delete mark
    let dir = dummy_tantivy_dir();
    builder.finish_lp(&dir)?;

    // LP 2: lp_b
    builder.start_lp(1, 1, true, b"lp_b")?;
    builder.add_pk_int(10, 200, false)?;
    builder.add_pk_int(20, 190, false)?;
    builder.add_pk_int(30, 180, true)?;
    let dir = dummy_tantivy_dir();
    builder.finish_lp(&dir)?;

    // LP 3: lp_c with duplicate PK versions
    builder.start_lp(1, 2, true, b"lp_c")?;
    builder.add_pk_int(100, 160, false)?; // Newest
    builder.add_pk_int(100, 150, false)?; // Older version same PK
    builder.add_pk_int(110, 140, true)?;
    let dir = dummy_tantivy_dir();
    builder.finish_lp(&dir)?;

    builder.finish(SnapVersion::zero())?;

    let packed_file = PackedFile::from_buffer(&buffer)?;

    // lp_a validations
    let lp_a = packed_file.cached_get_lp(b"lp_a").await?.unwrap();
    let lp_a = lp_a.as_int_lp().unwrap();
    assert_eq!(lp_a.props().get_n_pk(), 2);
    assert!(lp_a.has_newer_version(&IntPk::encode(1), 100, 400).await?);
    assert!(!lp_a.has_newer_version(&IntPk::encode(1), 300, 400).await?);
    assert!(lp_a.has_newer_version(&IntPk::encode(2), 0, 400).await?); // delete-mark version still counts
    assert!(!lp_a.has_newer_version(&IntPk::encode(2), 250, 400).await?);

    // lp_b validations
    let lp_b = packed_file.cached_get_lp(b"lp_b").await?.unwrap();
    let lp_b = lp_b.as_int_lp().unwrap();
    assert_eq!(lp_b.props().get_n_pk(), 3);
    assert!(lp_b.has_newer_version(&IntPk::encode(10), 150, 400).await?);
    assert!(!lp_b.has_newer_version(&IntPk::encode(10), 200, 400).await?);
    assert!(lp_b.has_newer_version(&IntPk::encode(30), 100, 400).await?);
    assert!(!lp_b.has_newer_version(&IntPk::encode(30), 180, 400).await?);

    // lp_c validations
    let lp_c = packed_file.cached_get_lp(b"lp_c").await?.unwrap();
    let lp_c = lp_c.as_int_lp().unwrap();
    assert_eq!(lp_c.props().get_n_pk(), 3);
    assert!(
        lp_c.has_newer_version(&IntPk::encode(100), 120, 400)
            .await?
    ); // 160,150 >120
    assert!(
        lp_c.has_newer_version(&IntPk::encode(100), 150, 400)
            .await?
    ); // 160 >150
    assert!(
        !lp_c
            .has_newer_version(&IntPk::encode(100), 160, 400)
            .await?
    ); // none >160
    assert!(
        lp_c.has_newer_version(&IntPk::encode(110), 100, 400)
            .await?
    ); // 140
    assert!(
        !lp_c
            .has_newer_version(&IntPk::encode(110), 140, 400)
            .await?
    );

    // Negative cases
    let none = packed_file.cached_get_lp(b"lp_d").await?;
    assert!(none.is_none());
    assert_eq!(packed_file.n_data_blocks()?, 3);
    Ok(())
}

#[tokio::test]
async fn test_empty_file() -> Result<()> {
    let options = PackedFileBuilderOptions::default();

    let mut buffer = Vec::new();
    let builder = PackedFileBuilder::new(Cursor::new(&mut buffer), options);
    builder.finish(SnapVersion::zero())?;

    let packed_file = PackedFile::from_buffer(&buffer)?;
    assert!(packed_file.cached_get_lp(b"anything").await?.is_none());
    assert_eq!(packed_file.n_data_blocks()?, 0);
    Ok(())
}

#[tokio::test]
async fn test_empty_lp_not_allowed() -> Result<()> {
    let options = PackedFileBuilderOptions::default();

    let mut buffer = Vec::new();
    let mut builder = PackedFileBuilder::new(Cursor::new(&mut buffer), options);
    builder.start_lp(1, 0, true, b"lp1")?;
    assert!(builder.finish_lp(&dummy_tantivy_dir()).is_err());

    Ok(())
}

#[tokio::test]
async fn test_roundtrip_multiple_lps_with_tantivy_data() -> Result<()> {
    let options = PackedFileBuilderOptions {
        block_size: 512, // Smaller block size to test multiple blocks
        checksum_type: ChecksumType::Crc32c,
    };

    let mut buffer = Vec::new();
    let mut builder = PackedFileBuilder::new(Cursor::new(&mut buffer), options);

    // Create multiple LPs with different tantivy data
    builder.start_lp(1, 0, true, b"lp1")?;
    builder.add_pk_int(1, 100, false)?;
    builder.add_pk_int(2, 99, false)?;
    let dir = dummy_tantivy_dir();
    builder.finish_lp(&dir)?;

    builder.start_lp(1, 1, false, b"lp2")?;
    builder.add_pk_common(b"key_a", 200, false)?;
    builder.add_pk_common(b"key_b", 199, true)?;
    let dir = dummy_tantivy_dir();
    builder.finish_lp(&dir)?;

    builder.start_lp(1, 2, true, b"lp3")?;
    builder.add_pk_int(10, 300, false)?;
    builder.add_pk_int(20, 299, false)?;
    builder.add_pk_int(30, 298, true)?;
    let dir = dummy_tantivy_dir();
    builder.finish_lp(&dir)?;

    builder.finish(SnapVersion::zero())?;

    let packed_file = PackedFile::from_buffer(&buffer)?;

    let lp1 = packed_file.cached_get_lp(b"lp1").await?.unwrap();
    let lp1 = lp1.as_int_lp().unwrap();
    assert_eq!(lp1.lp_key().as_ref(), b"lp1");
    assert_eq!(lp1.props().get_n_pk(), 2);
    let lp2 = packed_file.cached_get_lp(b"lp2").await?.unwrap();
    let lp2 = lp2.as_common_lp().unwrap();
    assert_eq!(lp2.lp_key().as_ref(), b"lp2");
    assert_eq!(lp2.props().get_n_pk(), 2);
    let lp3 = packed_file.cached_get_lp(b"lp3").await?.unwrap();
    let lp3 = lp3.as_int_lp().unwrap();
    assert_eq!(lp3.lp_key().as_ref(), b"lp3");
    assert_eq!(lp3.props().get_n_pk(), 3);
    assert!(lp1.has_newer_version(&IntPk::encode(1), 50, 150).await?);
    assert!(!lp1.has_newer_version(&IntPk::encode(1), 100, 150).await?);
    assert!(lp2.has_newer_version(b"key_a", 150, 250).await?);
    assert!(!lp2.has_newer_version(b"key_a", 200, 250).await?);
    assert!(lp3.has_newer_version(&IntPk::encode(30), 250, 350).await?);
    assert!(!lp3.has_newer_version(&IntPk::encode(30), 298, 350).await?);
    let lp_none = packed_file.cached_get_lp(b"lp_nonexistent").await?;
    let n_blocks = packed_file.n_data_blocks()?;
    assert!(n_blocks >= 1);
    assert!(lp_none.is_none());
    Ok(())
}

#[tokio::test]
async fn test_checksum_data_block_corruption() -> Result<()> {
    // Build a simple file with checksum enabled.
    let options = PackedFileBuilderOptions {
        block_size: 1024,
        checksum_type: ChecksumType::Crc32c,
    };
    let mut buffer = Vec::new();
    let mut builder = PackedFileBuilder::new(Cursor::new(&mut buffer), options);

    builder.start_lp(1, 0, true, b"lp1")?;
    builder.add_pk_int(1, 100, false)?;
    builder.finish_lp(&dummy_tantivy_dir())?;
    builder.finish(SnapVersion::zero())?;

    // Corrupt a byte in the data block
    buffer[5] ^= 0xFF;

    // Open and read should fail with data block checksum mismatch.
    let packed_file = PackedFile::from_buffer(&buffer)?;

    let err = packed_file
        .cached_get_lp(b"lp1")
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("data block checksum mismatch"));

    Ok(())
}

#[tokio::test]
async fn test_get_lp_edge_cases() -> Result<()> {
    let mut buffer = Vec::new();
    let mut builder = PackedFileBuilder::new(
        Cursor::new(&mut buffer),
        PackedFileBuilderOptions::default(),
    );

    builder.start_lp(1, 1, true, b"lp_b")?;
    builder.add_pk_int(1, 100, false)?;
    builder.finish_lp(&dummy_tantivy_dir())?;

    builder.start_lp(1, 3, true, b"lp_d")?;
    builder.add_pk_int(2, 200, false)?;
    builder.finish_lp(&dummy_tantivy_dir())?;

    builder.finish(SnapVersion::zero())?;

    let packed_file = PackedFile::from_buffer(&buffer)?;

    // Key before the first LP
    let lp_a = packed_file.cached_get_lp(b"lp_a").await?;
    assert!(lp_a.is_none());

    // Key that should exist
    let lp_b = packed_file.cached_get_lp(b"lp_b").await?;
    assert!(lp_b.is_some());

    // Key between two existing LPs
    let lp_c = packed_file.cached_get_lp(b"lp_c").await?;
    assert!(lp_c.is_none());

    // Key that should exist
    let lp_d = packed_file.cached_get_lp(b"lp_d").await?;
    assert!(lp_d.is_some());

    // Key after the last LP
    let lp_e = packed_file.cached_get_lp(b"lp_e").await?;
    assert!(lp_e.is_none());

    Ok(())
}

#[tokio::test]
async fn test_smallest_and_largest_row_keys() -> Result<()> {
    use tidb_query_datatype::codec::table::encode_row_key;

    use crate::table::fts::compact::lp_key;

    let mut buffer = Vec::new();
    let mut builder = PackedFileBuilder::new(
        Cursor::new(&mut buffer),
        PackedFileBuilderOptions::default(),
    );

    // Add multiple logical partitions with different table_ids and PKs
    // Table 100, Index 1, PK range: 5-10
    let lp1 = lp_key(100, 1);
    builder.start_lp(100, 1, true, &lp1)?;
    builder.add_pk_int(5, 100, false)?;
    builder.add_pk_int(10, 101, false)?;
    let dir = dummy_tantivy_dir();
    builder.finish_lp(&dir)?;

    // Table 200, Index 2, PK range: 1-3 (should be smallest overall)
    let lp2 = lp_key(200, 2);
    builder.start_lp(200, 2, true, &lp2)?;
    builder.add_pk_int(1, 200, false)?;
    builder.add_pk_int(3, 201, false)?;
    let dir = dummy_tantivy_dir();
    builder.finish_lp(&dir)?;

    // Table 300, Index 3, PK range: 15-20 (should be biggest overall)
    let lp3 = lp_key(300, 3);
    builder.start_lp(300, 3, true, &lp3)?;
    builder.add_pk_int(15, 300, false)?;
    builder.add_pk_int(20, 301, false)?;
    let dir = dummy_tantivy_dir();
    builder.finish_lp(&dir)?;

    let _file_size = builder.finish(SnapVersion::zero())?;

    // Read back the file and check properties
    let packed_file = PackedFile::from_buffer(&buffer)?;
    let props = packed_file.props();

    // Check that smallest and largest row keys are correctly set
    // Generate all row keys to verify correct tracking
    let key_100_5 = encode_row_key(100, 5);
    let key_300_20 = encode_row_key(300, 20);

    // In lexicographic byte order:
    // - Smallest should be encode_row_key(100, 5) (first table, smallest PK in that
    //   table)
    // - Largest should be encode_row_key(300, 20) (last table, largest PK in that
    //   table)
    assert_eq!(props.get_smallest_key(), &key_100_5);
    assert_eq!(props.get_biggest_key(), &key_300_20);

    // Verify table_id can be read from each LP
    let lp1 = packed_file.cached_get_lp(&lp1).await?.unwrap();
    assert_eq!(lp1.as_int_lp().unwrap().props().get_table_id(), 100);

    let lp2 = packed_file.cached_get_lp(&lp2).await?.unwrap();
    assert_eq!(lp2.as_int_lp().unwrap().props().get_table_id(), 200);

    let lp3 = packed_file.cached_get_lp(&lp3).await?.unwrap();
    assert_eq!(lp3.as_int_lp().unwrap().props().get_table_id(), 300);

    Ok(())
}

#[tokio::test]
async fn test_data_block_offsets_aligned_and_segmented_mmap_roundtrip() -> Result<()> {
    // Measure the serialized entry length of a single LP so we can choose a
    // block size that deterministically flushes after exactly 2 entries.
    let entry_len = {
        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions {
                block_size: 1024 * 1024,
                checksum_type: ChecksumType::Crc32c,
            },
        );
        builder.start_lp(1, 0, true, b"lp_a")?;
        builder.add_pk_int(1, 10, false)?;
        builder.add_pk_int(2, 9, false)?;
        builder.finish_lp(&dummy_tantivy_dir())?;
        builder.written_size() as usize
    };
    assert!(entry_len > 0);

    // Ensure: entry_len < block_size <= 2 * entry_len, so the first data block
    // contains exactly 2 entries and the second data block contains 1 entry.
    let block_size = entry_len * 2;
    let options = PackedFileBuilderOptions {
        block_size,
        checksum_type: ChecksumType::Crc32c,
    };

    let mut buffer = Vec::new();
    let mut builder = PackedFileBuilder::new(Cursor::new(&mut buffer), options);

    for lp_key in [b"lp_a".as_slice(), b"lp_b".as_slice(), b"lp_c".as_slice()] {
        builder.start_lp(1, 0, true, lp_key)?;
        builder.add_pk_int(1, 10, false)?;
        builder.add_pk_int(2, 9, false)?;
        builder.finish_lp(&dummy_tantivy_dir())?;
    }
    builder.finish(SnapVersion::zero())?;

    let packed_file = PackedFile::from_buffer(&buffer)?;
    assert_eq!(packed_file.n_data_blocks()?, 2);

    let index_block = packed_file.get_index_block()?;
    let offsets = index_block.get_data_block_offsets();
    assert_eq!(offsets.len(), 3);
    assert_eq!(offsets[0], 0);
    assert_eq!(
        index_block.get_data_block_start_keys()[0].as_slice(),
        b"lp_a"
    );
    assert_eq!(
        index_block.get_data_block_start_keys()[1].as_slice(),
        b"lp_c"
    );
    assert_eq!(
        offsets[1] % 8,
        0,
        "expected data block offsets to always be 8-byte aligned"
    );

    // No leading padding in a data block. The first entry starts at offset=0.
    let data_block_1 = packed_file.cached_data_block_at(1).await?;
    assert_eq!(data_block_1.unsafe_offsets[0], 0);

    // "Segment shift" simulation: put each data block in its own mmap segment,
    // so the returned bytes do not share the same base address as the file.
    let segment_offsets = index_block
        .get_data_block_offsets()
        .iter()
        .map(|&x| x as u64)
        .collect::<Vec<_>>();
    for &off in &segment_offsets {
        assert_eq!(off % 8, 0, "segment boundary must be 8-byte aligned");
    }
    let file_id = 1;
    let segmented_file = Arc::new(SegmentedMmapFile::new(
        file_id,
        Bytes::copy_from_slice(&buffer),
        segment_offsets,
    ));
    let packed_file = PackedFile::new(segmented_file, FtsCache::disabled())?;

    // Roundtrip: all LPs should be readable, including the one in the second
    // data block.
    for lp_key in [b"lp_a".as_slice(), b"lp_b".as_slice(), b"lp_c".as_slice()] {
        let lp = packed_file.cached_get_lp(lp_key).await?.unwrap();
        let lp = lp.as_int_lp().unwrap();
        assert_eq!(lp.lp_key().as_ref(), lp_key);
        assert_eq!(lp.props().get_n_pk(), 2);
        assert!(lp.has_newer_version(&IntPk::encode(1), 0, 20).await?);
        assert!(lp.has_newer_version(&IntPk::encode(2), 0, 20).await?);
    }
    Ok(())
}

#[test]
fn test_footer_offsets_non_monotonic_should_error() -> Result<()> {
    let mut buffer = Vec::new();
    let mut builder = PackedFileBuilder::new(
        Cursor::new(&mut buffer),
        PackedFileBuilderOptions {
            block_size: 1024,
            checksum_type: ChecksumType::Crc32c,
        },
    );
    builder.start_lp(1, 0, true, b"lp")?;
    builder.add_pk_int(1, 10, false)?;
    builder.finish_lp(&dummy_tantivy_dir())?;
    builder.finish(SnapVersion::zero())?;

    let footer_start = buffer.len() - FTS_PACKED_FILE_FOOTER_SIZE;
    let footer = PackedFileFooter::unmarshal(&buffer[footer_start..])?;
    assert!(footer.index_block_offset > 0);

    let mut bad_footer = footer;
    bad_footer.lp_filter_block_offset = footer.index_block_offset - 1;

    let mut bad_footer_bytes = Vec::new();
    bad_footer.marshal(&mut bad_footer_bytes)?;
    buffer[footer_start..].copy_from_slice(&bad_footer_bytes);

    let err = PackedFile::from_buffer(&buffer).unwrap_err();
    assert!(
        err.to_string()
            .contains("PackedFile footer offsets not monotonic")
    );
    Ok(())
}

#[test]
fn test_footer_offsets_out_of_bounds_should_error() -> Result<()> {
    let mut buffer = Vec::new();
    let mut builder = PackedFileBuilder::new(
        Cursor::new(&mut buffer),
        PackedFileBuilderOptions {
            block_size: 1024,
            checksum_type: ChecksumType::Crc32c,
        },
    );
    builder.start_lp(1, 0, true, b"lp")?;
    builder.add_pk_int(1, 10, false)?;
    builder.finish_lp(&dummy_tantivy_dir())?;
    builder.finish(SnapVersion::zero())?;

    let footer_start = buffer.len() - FTS_PACKED_FILE_FOOTER_SIZE;
    let footer = PackedFileFooter::unmarshal(&buffer[footer_start..])?;

    let mut bad_footer = footer;
    let footer_start_off = u32::try_from(footer_start)?;
    bad_footer.prop_offset = footer_start_off + 1;

    let mut bad_footer_bytes = Vec::new();
    bad_footer.marshal(&mut bad_footer_bytes)?;
    buffer[footer_start..].copy_from_slice(&bad_footer_bytes);

    let err = PackedFile::from_buffer(&buffer).unwrap_err();
    assert!(
        err.to_string()
            .contains("PackedFile footer offsets out of bounds")
    );
    Ok(())
}
