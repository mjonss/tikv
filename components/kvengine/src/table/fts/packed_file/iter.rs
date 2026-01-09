// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use xorf::Filter;

use super::{EPackedFileLp, PackedFile, PackedFileDataBlockAccessor, PackedFileLp};
use crate::table::fts::iter::*;

// ============= PK Iterators =============

impl<Pk: PkType> PkReader<Pk> for PackedFileLp<Pk> {
    type Iterator = PackedFileLpPkIterator<Pk>;

    #[inline]
    async fn has_newer_version(
        &self,
        pk_encoded: &[u8],
        version: u64,
        max_version: u64,
    ) -> Result<bool> {
        let key_hash = farmhash::fingerprint64(pk_encoded);
        if !self.0.pk_filter.contains(&key_hash) {
            return Ok(false);
        }
        has_newer_version(
            pk_encoded,
            version,
            max_version,
            self.0.props.get_n_pk() as usize,
            |i| Ok(self.version_at(i)),
            |i| Ok(self.encoded_pk_at(i)),
        )
    }

    #[inline]
    fn pk_iter(&self) -> Result<Self::Iterator> {
        Ok(PackedFileLpPkIterator::<Pk> {
            lp: self.clone(),
            next_doc_id: 0,
        })
    }

    async fn async_at(&self, doc_id: usize, read_ts: u64) -> Result<Option<(Bytes, u64, u8)>> {
        let n_pk = self.0.props.get_n_pk() as usize;
        if doc_id >= n_pk {
            bail!(
                "packed_file_lp.async_at OOB: file_id={}, lp_key={:?}, doc_id={}, n_pk={}",
                self.0.file_id,
                self.lp_key(),
                doc_id,
                n_pk
            );
        }

        let pk = self.encoded_pk_at(doc_id);
        let version = self.version_at(doc_id);
        let is_deleted = self.is_deleted_at(doc_id);

        if version > read_ts || is_deleted != 0 {
            return Ok(None);
        }

        // Versions for the same PK are ordered from newest to oldest, and they
        // are stored contiguously, so we only need to check `doc_id - 1`.
        if doc_id > 0 {
            let p_i = doc_id - 1;
            let p_pk = self.encoded_pk_at(p_i);
            if pk == p_pk && self.version_at(p_i) <= read_ts {
                return Ok(None);
            }
        }

        Ok(Some((pk, version, is_deleted)))
    }
}

/// Iterator implementation for PackedFileLp that returns sorted primary keys,
/// using a function parameter for maximum efficiency
pub struct PackedFileLpPkIterator<Pk: PkType> {
    lp: PackedFileLp<Pk>,
    next_doc_id: DocId,
}

impl<Pk: PkType> OrderedPkIterator for PackedFileLpPkIterator<Pk> {
    async fn next(&mut self) -> Result<Option<(DocId, Bytes, u64, u8)>> {
        let doc_id = self.next_doc_id;
        if doc_id >= self.lp.props().get_n_pk() {
            return Ok(None);
        }
        let idx = doc_id as usize;
        let version = self.lp.version_at(idx);
        let is_deleted = self.lp.is_deleted_at(idx);
        let pk_bytes = self.lp.encoded_pk_at(idx);
        self.next_doc_id = doc_id
            .checked_add(1)
            .ok_or_else(|| anyhow!("doc_id overflow"))?;
        Ok(Some((doc_id, pk_bytes, version, is_deleted)))
    }
}

// ============= LP Iterators =============

/// Iterator for iterating over all logical partitions in a PackedFile.
/// Logical partitions are yielded in sorted order by LP key.
pub struct PackedFileLpIterator {
    packed_file: PackedFile,
    n_data_blocks: usize,
    current_data_block_idx: usize,
    current_entry_idx: usize,
    current_data_block: Option<Arc<PackedFileDataBlockAccessor>>,
}

impl PackedFileLpIterator {
    /// Returns the next logical partition, or None if iteration is complete.
    pub async fn next_lp(&mut self) -> Result<Option<EPackedFileLp>> {
        loop {
            // If no current data block loaded, try to load the next one
            if self.current_data_block.is_none() {
                // Check if we've exhausted all data blocks
                if self.current_data_block_idx >= self.n_data_blocks {
                    return Ok(None);
                }
                let data_block = self
                    .packed_file
                    .cached_data_block_at(self.current_data_block_idx)
                    .await?;
                self.current_data_block = Some(data_block);
                self.current_entry_idx = 0;
            }

            let data_block = self.current_data_block.as_ref().unwrap();
            // Check if we have exhausted all entries (lps) in the current data block
            if self.current_entry_idx < data_block.n_entries as usize {
                let lp = data_block.lp_at(self.current_entry_idx)?;
                self.current_entry_idx += 1;
                return Ok(Some(lp));
            }

            // Current data block exhausted, move to next
            self.current_data_block = None;
            self.current_data_block_idx += 1;
        }
    }
}

impl PackedFile {
    /// Creates an iterator over all logical partitions in this PackedFile.
    /// Logical partitions are returned in sorted order by LP key.
    pub fn lp_iter(&self) -> Result<PackedFileLpIterator> {
        Ok(PackedFileLpIterator {
            packed_file: self.clone(),
            n_data_blocks: self.n_data_blocks()?,
            current_data_block_idx: 0,
            current_entry_idx: 0,
            current_data_block: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use anyhow::Result;
    use codec::number::NumberCodec;

    use super::*;
    use crate::table::{
        fts::{dedicated_file::test::dummy_tantivy_dir, packed_file::*},
        SnapVersion,
    };

    #[tokio::test]
    async fn test_int_pk_iterator() -> Result<()> {
        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions::default(),
        );

        // Create test data with int PKs in sorted order
        builder.start_lp(1, 0, true, b"lp_test")?;
        builder.add_pk_int(1, 100, false)?; // pk=1, version=100, not deleted
        builder.add_pk_int(1, 50, true)?; // pk=1, version=50, deleted (older version)
        builder.add_pk_int(3, 200, false)?; // pk=3, version=200, not deleted
        builder.add_pk_int(5, 300, true)?; // pk=5, version=300, deleted
        let tantivy_layout = dummy_tantivy_dir();
        builder.finish_lp(&tantivy_layout)?;

        builder.finish(SnapVersion::zero())?;

        let packed_file = PackedFile::from_buffer(&buffer)?;
        let lp = packed_file.cached_get_lp(b"lp_test").await?.unwrap();

        // Test the iterator - extract IntPk variant
        let lp = lp.as_int_lp().unwrap();
        let mut iter = lp.pk_iter()?;

        // First entry: pk=1, version=100, not deleted
        let (doc_id, pk_bytes, version, is_deleted) = iter.next().await.unwrap().unwrap();
        assert_eq!(doc_id, 0);
        assert_eq!(pk_bytes.len(), 8); // Encoded i64 is 8 bytes
        assert_eq!(version, 100);
        assert_eq!(is_deleted, 0);

        // Decode the PK bytes to verify
        let decoded_pk = NumberCodec::decode_i64(pk_bytes.as_ref());
        assert_eq!(decoded_pk, 1);

        // Second entry: pk=1, version=50, deleted
        let (doc_id, pk_bytes, version, is_deleted) = iter.next().await.unwrap().unwrap();
        assert_eq!(doc_id, 1);
        assert_eq!(pk_bytes.len(), 8);
        assert_eq!(version, 50);
        assert_eq!(is_deleted, 1);
        let decoded_pk = NumberCodec::decode_i64(pk_bytes.as_ref());
        assert_eq!(decoded_pk, 1);

        // Third entry: pk=3, version=200, not deleted
        let (doc_id, pk_bytes, version, is_deleted) = iter.next().await.unwrap().unwrap();
        assert_eq!(doc_id, 2);
        assert_eq!(pk_bytes.len(), 8);
        assert_eq!(version, 200);
        assert_eq!(is_deleted, 0);
        let decoded_pk = NumberCodec::decode_i64(pk_bytes.as_ref());
        assert_eq!(decoded_pk, 3);

        // Fourth entry: pk=5, version=300, deleted
        let (doc_id, pk_bytes, version, is_deleted) = iter.next().await.unwrap().unwrap();
        assert_eq!(doc_id, 3);
        assert_eq!(pk_bytes.len(), 8);
        assert_eq!(version, 300);
        assert_eq!(is_deleted, 1);
        let decoded_pk = NumberCodec::decode_i64(pk_bytes.as_ref());
        assert_eq!(decoded_pk, 5);

        // Iterator should be exhausted
        assert!(iter.next().await.unwrap().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_common_pk_iterator() -> Result<()> {
        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions::default(),
        );

        // Create test data with common PKs in sorted order
        builder.start_lp(1, 0, false, b"lp_common")?;
        builder.add_pk_common(b"key_a", 100, false)?; // pk="key_a", version=100, not deleted
        builder.add_pk_common(b"key_a", 50, true)?; // pk="key_a", version=50, deleted (older version)
        builder.add_pk_common(b"key_b", 200, false)?; // pk="key_b", version=200, not deleted
        builder.add_pk_common(b"key_c", 300, true)?; // pk="key_c", version=300, deleted
        let tantivy_layout = dummy_tantivy_dir();
        builder.finish_lp(&tantivy_layout)?;

        builder.finish(SnapVersion::zero())?;

        let packed_file = PackedFile::from_buffer(&buffer)?;
        let lp = packed_file.cached_get_lp(b"lp_common").await?.unwrap();

        // Test the iterator - extract CommonPk variant
        let lp = lp.as_common_lp().unwrap();
        let mut iter = lp.pk_iter()?;

        // First entry: pk="key_a", version=100, not deleted
        let (doc_id, pk_bytes, version, is_deleted) = iter.next().await.unwrap().unwrap();
        assert_eq!(doc_id, 0);
        assert_eq!(pk_bytes.as_ref(), b"key_a");
        assert_eq!(version, 100);
        assert_eq!(is_deleted, 0);

        // Second entry: pk="key_a", version=50, deleted
        let (doc_id, pk_bytes, version, is_deleted) = iter.next().await.unwrap().unwrap();
        assert_eq!(doc_id, 1);
        assert_eq!(pk_bytes.as_ref(), b"key_a");
        assert_eq!(version, 50);
        assert_eq!(is_deleted, 1);

        // Third entry: pk="key_b", version=200, not deleted
        let (doc_id, pk_bytes, version, is_deleted) = iter.next().await.unwrap().unwrap();
        assert_eq!(doc_id, 2);
        assert_eq!(pk_bytes.as_ref(), b"key_b");
        assert_eq!(version, 200);
        assert_eq!(is_deleted, 0);

        // Fourth entry: pk="key_c", version=300, deleted
        let (doc_id, pk_bytes, version, is_deleted) = iter.next().await.unwrap().unwrap();
        assert_eq!(doc_id, 3);
        assert_eq!(pk_bytes.as_ref(), b"key_c");
        assert_eq!(version, 300);
        assert_eq!(is_deleted, 1);

        // Iterator should be exhausted
        assert!(iter.next().await.unwrap().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_lp_iterator() -> Result<()> {
        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions::default(),
        );

        // Create LP with single PK to avoid "must have at least one PK" error
        builder.start_lp(1, 0, true, b"lp_single")?;
        builder.add_pk_int(42, 100, false)?;
        let tantivy_layout = dummy_tantivy_dir();
        builder.finish_lp(&tantivy_layout)?;

        builder.finish(SnapVersion::zero())?;

        let packed_file = PackedFile::from_buffer(&buffer)?;
        let lp = packed_file.cached_get_lp(b"lp_single").await?.unwrap();

        // Test the iterator - should have one entry
        let lp = lp.as_int_lp().unwrap();
        let mut iter = lp.pk_iter()?;

        let (doc_id, pk_bytes, version, is_deleted) = iter.next().await.unwrap().unwrap();
        assert_eq!(doc_id, 0);
        assert_eq!(pk_bytes.len(), 8);
        assert_eq!(version, 100);
        assert_eq!(is_deleted, 0);

        // Iterator should be exhausted
        assert!(iter.next().await.unwrap().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_lp_iterator_multiple_blocks() -> Result<()> {
        use crate::table::fts::compact::lp_key;

        // Force small block size to create multiple data blocks
        let options = PackedFileBuilderOptions {
            block_size: 1,
            checksum_type: ChecksumType::Crc32c,
        };

        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(Cursor::new(&mut buffer), options);

        // Create 3 LPs in sorted order
        let lp1 = lp_key(1, 1);
        builder.start_lp(1, 1, true, &lp1)?;
        builder.add_pk_int(10, 100, false)?;
        let dir = dummy_tantivy_dir();
        builder.finish_lp(&dir)?;

        let lp2 = lp_key(1, 2);
        builder.start_lp(1, 2, true, &lp2)?;
        builder.add_pk_int(20, 200, false)?;
        let dir = dummy_tantivy_dir();
        builder.finish_lp(&dir)?;

        let lp3 = lp_key(2, 1);
        builder.start_lp(2, 1, true, &lp3)?;
        builder.add_pk_int(30, 300, false)?;
        let dir = dummy_tantivy_dir();
        builder.finish_lp(&dir)?;

        builder.finish(SnapVersion::zero())?;

        let packed_file = PackedFile::from_buffer(&buffer)?;
        let mut iter = packed_file.lp_iter()?;

        // First LP
        let lp = iter.next_lp().await?.unwrap();
        let lp = lp.as_int_lp().unwrap();
        assert_eq!(lp.lp_key().as_ref(), &lp1);
        assert_eq!(lp.props().get_n_pk(), 1);

        // Second LP
        let lp = iter.next_lp().await?.unwrap();
        let lp = lp.as_int_lp().unwrap();
        assert_eq!(lp.lp_key().as_ref(), &lp2);
        assert_eq!(lp.props().get_n_pk(), 1);

        // Third LP
        let lp = iter.next_lp().await?.unwrap();
        let lp = lp.as_int_lp().unwrap();
        assert_eq!(lp.lp_key().as_ref(), &lp3);
        assert_eq!(lp.props().get_n_pk(), 1);

        // No more LPs
        assert!(iter.next_lp().await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_lp_iterator_empty_file() -> Result<()> {
        let mut buffer = Vec::new();
        let builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions::default(),
        );
        builder.finish(SnapVersion::zero())?;

        let packed_file = PackedFile::from_buffer(&buffer)?;
        let mut iter = packed_file.lp_iter()?;

        // Should return None immediately for empty file
        assert!(iter.next_lp().await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_lp_iterator_single_lp() -> Result<()> {
        use crate::table::fts::compact::lp_key;

        let mut buffer = Vec::new();
        let mut builder = PackedFileBuilder::new(
            Cursor::new(&mut buffer),
            PackedFileBuilderOptions::default(),
        );

        let lp1 = lp_key(5, 10);
        builder.start_lp(5, 0, false, &lp1)?;
        builder.add_pk_common(b"key_a", 100, false)?;
        builder.add_pk_common(b"key_b", 99, true)?;
        let dir = dummy_tantivy_dir();
        builder.finish_lp(&dir)?;

        builder.finish(SnapVersion::zero())?;

        let packed_file = PackedFile::from_buffer(&buffer)?;
        let mut iter = packed_file.lp_iter()?;

        // First LP (CommonPk variant)
        let lp = iter.next_lp().await?.unwrap();
        let lp = lp.as_common_lp().unwrap();
        assert_eq!(lp.lp_key().as_ref(), &lp1);
        assert_eq!(lp.props().get_n_pk(), 2);

        // No more LPs
        assert!(iter.next_lp().await?.is_none());

        Ok(())
    }
}
