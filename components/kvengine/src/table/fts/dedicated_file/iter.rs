// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use bytes::Bytes;
use xorf::Filter;

use super::{DedicatedFile, HBlockAccessor};
use crate::table::fts::iter::*;

impl<Pk: PkType> PkReader<Pk> for DedicatedFile<Pk> {
    type Iterator = DedicatedFilePkIterator<Pk>;

    #[inline]
    fn pk_iter(&self) -> Result<Self::Iterator> {
        DedicatedFilePkIterator::<Pk>::new(self.clone())
    }

    async fn has_newer_version(
        &self,
        pk_encoded: &[u8],
        version: u64,
        max_version: u64,
    ) -> Result<bool> {
        // 1. First check file-level PK filter
        let pk_filter = self.get_pk_filter()?;
        let key_hash = farmhash::fingerprint64(pk_encoded);
        if !pk_filter.contains(&key_hash) {
            return Ok(false);
        }

        // 2. Find Handle Block that might contain this PK in Handle index block
        let block_idx = match self.find_hblock_offset(pk_encoded)? {
            Some(idx) => idx,
            None => return Ok(false),
        };

        // 3. Get Handle Block data and verify
        let accessor = self.cached_hblock_at(block_idx).await?;
        has_newer_version(
            pk_encoded,
            version,
            max_version,
            accessor.n_pk as usize,
            |i| Ok(accessor.version_at(i)),
            |i| Ok(accessor.encoded_pk_at(i)),
        )
    }

    async fn async_at(&self, doc_id: usize, read_ts: u64) -> Result<Option<(Bytes, u64, u8)>> {
        let (block_idx, local_doc_id) = self.find_hblock_offset_docid(doc_id)?;
        let accessor = self.cached_hblock_at(block_idx).await?;

        if local_doc_id >= accessor.n_pk as usize {
            bail!(
                "dedicated_file.async_at OOB: file_id={}, doc_id={}, block_idx={}, local_doc_id={}, block_n_pk={}",
                self.id(),
                doc_id,
                block_idx,
                local_doc_id,
                accessor.n_pk
            );
        }

        let pk = accessor.encoded_pk_at(local_doc_id);
        let version = accessor.version_at(local_doc_id);
        let is_deleted = accessor.is_deleted_at(local_doc_id);

        if version > read_ts || is_deleted != 0 {
            return Ok(None);
        }

        // Handle block invariant (see DedicatedFileBuilder):
        // - PKs are added in ascending order.
        // - Versions of the same PK are added in descending order.
        // - A handle block is flushed only after all versions of the current PK have
        //   been added.
        //
        // Therefore, if there is a newer visible version for this PK, it must be
        // at `local_doc_id - 1` within the same handle block.
        if local_doc_id > 0 {
            let p_i = local_doc_id - 1;
            let p_pk = accessor.encoded_pk_at(p_i);
            if pk == p_pk && accessor.version_at(p_i) <= read_ts {
                return Ok(None);
            }
        }

        Ok(Some((pk, version, is_deleted)))
    }
}

pub struct DedicatedFilePkIterator<Pk: PkType> {
    file: DedicatedFile<Pk>,
    current_hblock: Option<Arc<HBlockAccessor<Pk>>>,
    next_hblock: usize, // Index of current handle block
    next_pk: usize,     // Index within current block
    next_doc_id: DocId,
    total_hblocks: usize,
    _marker: std::marker::PhantomData<Pk>,
}

impl<Pk: PkType> DedicatedFilePkIterator<Pk> {
    #[inline]
    pub fn new(ded_file: DedicatedFile<Pk>) -> Result<Self> {
        let hblock_index = ded_file.get_iblock()?;
        let total_hblocks = hblock_index.hblock_start_key.len();

        Ok(Self {
            file: ded_file,
            current_hblock: None,
            next_hblock: 0,
            next_pk: 0,
            next_doc_id: 0,
            total_hblocks,
            _marker: std::marker::PhantomData,
        })
    }
}

impl<Pk: PkType> OrderedPkIterator for DedicatedFilePkIterator<Pk> {
    async fn next(&mut self) -> Result<Option<(DocId, Bytes, u64, u8)>> {
        loop {
            // Check if we need to load a new block
            let need_new_hblock = match &self.current_hblock {
                None => true,
                Some(hblock) => self.next_pk >= hblock.n_pk as usize,
            };

            if need_new_hblock {
                // Check if we have more blocks to load
                if self.next_hblock >= self.total_hblocks {
                    return Ok(None);
                }

                // Load next handle block
                let block_accessor = self.file.cached_hblock_at(self.next_hblock).await?;

                self.current_hblock = Some(block_accessor);
                self.next_hblock += 1;
                self.next_pk = 0;
                // Continue to try getting item from new block
            } else {
                // Get next item from current block
                let handle_block = self.current_hblock.as_ref().unwrap();
                let version = handle_block.version_at(self.next_pk);
                let is_deleted = handle_block.is_deleted_at(self.next_pk);
                let pk_bytes = handle_block.encoded_pk_at(self.next_pk);
                let doc_id = self.next_doc_id;

                self.next_pk += 1;
                self.next_doc_id = doc_id
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("doc_id overflow"))?;
                return Ok(Some((doc_id, pk_bytes, version, is_deleted)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use codec::number::NumberCodec;

    use super::*;
    use crate::table::fts::{
        DedicatedFileBuilder, DedicatedFileBuilderOptions, EDedicatedFile,
        dedicated_file::test::dummy_tantivy_dir,
    };

    #[tokio::test]
    async fn test_int_pk_iterator() -> Result<()> {
        // Create test data with int PKs in sorted order
        let mut buffer = Vec::new();
        let mut builder: DedicatedFileBuilder<_, IntPk> = DedicatedFileBuilder::new(
            &mut buffer,
            DedicatedFileBuilderOptions::default(),
            1,
            0,
            b"lp_test",
        )?;

        // Add int PKs with multiple versions
        builder.add_pk(1, 100, false)?; // pk=1, version=100, not deleted
        builder.add_pk(1, 50, true)?; // pk=1, version=50, deleted (older version)
        builder.add_pk(3, 200, false)?; // pk=3, version=200, not deleted
        builder.add_pk(5, 300, true)?; // pk=5, version=300, deleted

        let _summary = builder.finish(&dummy_tantivy_dir())?;

        let dedicated_file = DedicatedFile::from_buffer(&buffer)?;

        // Test the iterator - extract IntPk variant
        let dedicated_file = match dedicated_file {
            EDedicatedFile::Int(f) => f,
            _ => panic!("Expected IntPk variant"),
        };
        let mut iter = dedicated_file.pk_iter()?;

        // First entry: pk=1, version=100, not deleted
        let (doc_id, pk_bytes, version, is_deleted) = iter.next().await?.unwrap();
        assert_eq!(doc_id, 0);
        assert_eq!(pk_bytes.len(), 8); // Encoded i64 is 8 bytes
        assert_eq!(version, 100);
        assert_eq!(is_deleted, 0);

        // Decode the PK bytes to verify
        let decoded_pk = NumberCodec::decode_i64(pk_bytes.as_ref());
        assert_eq!(decoded_pk, 1);

        // Second entry: pk=1, version=50, deleted
        let (doc_id, pk_bytes, version, is_deleted) = iter.next().await?.unwrap();
        assert_eq!(doc_id, 1);
        assert_eq!(pk_bytes.len(), 8);
        assert_eq!(version, 50);
        assert_eq!(is_deleted, 1);
        let decoded_pk = NumberCodec::decode_i64(pk_bytes.as_ref());
        assert_eq!(decoded_pk, 1);

        // Third entry: pk=3, version=200, not deleted
        let (doc_id, pk_bytes, version, is_deleted) = iter.next().await?.unwrap();
        assert_eq!(doc_id, 2);
        assert_eq!(pk_bytes.len(), 8);
        assert_eq!(version, 200);
        assert_eq!(is_deleted, 0);
        let decoded_pk = NumberCodec::decode_i64(pk_bytes.as_ref());
        assert_eq!(decoded_pk, 3);

        // Fourth entry: pk=5, version=300, deleted
        let (doc_id, pk_bytes, version, is_deleted) = iter.next().await?.unwrap();
        assert_eq!(doc_id, 3);
        assert_eq!(pk_bytes.len(), 8);
        assert_eq!(version, 300);
        assert_eq!(is_deleted, 1);
        let decoded_pk = NumberCodec::decode_i64(pk_bytes.as_ref());
        assert_eq!(decoded_pk, 5);

        // Iterator should be exhausted
        assert!(iter.next().await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_common_pk_iterator() -> Result<()> {
        // Create test data with common PKs in sorted order
        let mut buffer = Vec::new();
        let mut builder: DedicatedFileBuilder<_, CommonPk> = DedicatedFileBuilder::new(
            &mut buffer,
            DedicatedFileBuilderOptions::default(),
            1,
            0,
            b"lp_common",
        )?;

        // Add common PKs with multiple versions
        builder.add_pk(b"key_a", 100, false)?; // pk="key_a", version=100, not deleted
        builder.add_pk(b"key_a", 50, true)?; // pk="key_a", version=50, deleted (older version)
        builder.add_pk(b"key_b", 200, false)?; // pk="key_b", version=200, not deleted
        builder.add_pk(b"key_c", 300, true)?; // pk="key_c", version=300, deleted

        let _summary = builder.finish(&dummy_tantivy_dir())?;

        let dedicated_file = DedicatedFile::from_buffer(&buffer)?;

        // Test the iterator - extract CommonPk variant
        let dedicated_file = match dedicated_file {
            EDedicatedFile::Common(f) => f,
            _ => panic!("Expected CommonPk variant"),
        };
        let mut iter = dedicated_file.pk_iter()?;

        // First entry: pk="key_a", version=100, not deleted
        let (doc_id, pk_bytes, version, is_deleted) = iter.next().await?.unwrap();
        assert_eq!(doc_id, 0);
        assert_eq!(pk_bytes.as_ref(), b"key_a");
        assert_eq!(version, 100);
        assert_eq!(is_deleted, 0);

        // Second entry: pk="key_a", version=50, deleted
        let (doc_id, pk_bytes, version, is_deleted) = iter.next().await?.unwrap();
        assert_eq!(doc_id, 1);
        assert_eq!(pk_bytes.as_ref(), b"key_a");
        assert_eq!(version, 50);
        assert_eq!(is_deleted, 1);

        // Third entry: pk="key_b", version=200, not deleted
        let (doc_id, pk_bytes, version, is_deleted) = iter.next().await?.unwrap();
        assert_eq!(doc_id, 2);
        assert_eq!(pk_bytes.as_ref(), b"key_b");
        assert_eq!(version, 200);
        assert_eq!(is_deleted, 0);

        // Fourth entry: pk="key_c", version=300, deleted
        let (doc_id, pk_bytes, version, is_deleted) = iter.next().await?.unwrap();
        assert_eq!(doc_id, 3);
        assert_eq!(pk_bytes.as_ref(), b"key_c");
        assert_eq!(version, 300);
        assert_eq!(is_deleted, 1);

        // Iterator should be exhausted
        assert!(iter.next().await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_multi_handle_block_iterator() -> Result<()> {
        // Create test data that will span multiple handle blocks
        let mut buffer = Vec::new();
        let mut options = DedicatedFileBuilderOptions::default();
        options.handle_block_size = 1; // Very small block size to force multiple blocks

        let mut builder: DedicatedFileBuilder<_, IntPk> =
            DedicatedFileBuilder::new(&mut buffer, options, 1, 0, b"lp_multi_block")?;

        // Add multiple PKs that will be split across blocks
        for i in 1..10 {
            builder.add_pk(i * 10, (i * 100) as u64, false)?; // pk=10,20,30..., version=100,200,300...
            builder.add_pk(i * 10, (i * 50) as u64, true)?; // pk=10,20,30..., version=50,100,150... (older versions)
        }

        let _summary = builder.finish(&dummy_tantivy_dir())?;

        let dedicated_file = DedicatedFile::from_buffer(&buffer)?;

        // Test the iterator - extract IntPk variant
        let dedicated_file = match dedicated_file {
            EDedicatedFile::Int(f) => f,
            _ => panic!("Expected IntPk variant"),
        };
        let mut iter = dedicated_file.pk_iter()?;
        let mut pk_count = 0;
        let mut last_decoded_pk = 0i64;

        while let Some((doc_id, pk_bytes, version, _is_deleted)) = iter.next().await? {
            assert_eq!(doc_id, pk_count);
            // Decode the PK bytes
            let decoded_pk = NumberCodec::decode_i64(pk_bytes.as_ref());

            // Verify PKs are in ascending order
            assert!(decoded_pk >= last_decoded_pk);

            // If same PK, verify versions are in descending order
            if decoded_pk == last_decoded_pk {
                // This should be the older version
                assert!(version < (decoded_pk / 10 * 100) as u64);
            }

            last_decoded_pk = decoded_pk;
            pk_count += 1;
        }

        // Verify we got all entries (9 PKs * 2 versions each = 18 total)
        assert_eq!(pk_count, 18);

        Ok(())
    }
}
