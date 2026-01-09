// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use anyhow::Result;

use super::merge_fts_l2_files;
use crate::table::{
    file::{File, FileMmapGuard, InMemFile, MmapData},
    fts::{
        dedicated_file::{DedicatedFile, DedicatedFileBuilderOptions, EDedicatedFile},
        iter::{IntPk, PkReader, PkType},
        lp_key,
        test_util::new_ded,
        FtsCache,
    },
    table,
};

#[tokio::test]
async fn merge_l2_dedicated_files_int_pk() -> Result<()> {
    let file1 = new_ded(1)
        .lp(100, 0, |d| {
            d(1, 100, false, "doc1");
            d(2, 90, false, "doc2");
        })
        .finish_as_file();

    let file2 = new_ded(2)
        .lp(100, 0, |d| {
            d(1, 110, false, "doc3");
            d(3, 80, true, "doc4");
        })
        .finish_as_file();

    let files = vec![file1, file2];

    let output = merge_fts_l2_files(&files, 0, DedicatedFileBuilderOptions::default())
        .await?
        .expect("should produce merged file");
    assert_eq!(output.summary.props.get_table_id(), 100);
    assert_eq!(output.summary.props.get_pk_total(), 4);
    assert_eq!(output.summary.props.get_lp_key(), lp_key(100, 0));
    Ok(())
}

#[derive(Clone)]
struct DelayFile {
    inner: InMemFile,
    delay: Duration,
}

impl DelayFile {
    fn new(id: u64, data: bytes::Bytes, delay: Duration) -> Self {
        Self {
            inner: InMemFile::new(id, data),
            delay,
        }
    }
}

#[async_trait::async_trait]
impl File for DelayFile {
    fn id(&self) -> u64 {
        self.inner.id()
    }

    fn size(&self) -> u64 {
        self.inner.size()
    }

    fn is_sync(&self) -> bool {
        self.inner.is_sync()
    }

    fn read(&self, off: u64, length: usize) -> table::Result<bytes::Bytes> {
        self.inner.read(off, length)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> table::Result<()> {
        self.inner.read_at(buf, offset)
    }

    fn read_table_meta(&self, off: u64, length: usize) -> table::Result<bytes::Bytes> {
        self.inner.read_table_meta(off, length)
    }

    fn mmap(&self) -> table::Result<MmapData> {
        self.inner.mmap()
    }

    fn mmap2(&self) -> table::Result<bytes::Bytes> {
        self.inner.mmap2()
    }

    async fn mmap_range(
        &self,
        offset: u64,
        length: usize,
    ) -> table::Result<(bytes::Bytes, FileMmapGuard)> {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        self.inner.mmap_range(offset, length).await
    }

    fn storage_class(&self) -> schema::schema::StorageClass {
        self.inner.storage_class()
    }

    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

fn search_doc_ids(reader: &clara_fts::IndexReader, term: &str) -> Result<Vec<u32>> {
    let mut results = Vec::new();
    reader.search_no_score(term, &clara_fts::BitmapFilter::all_match(), &mut results)?;
    Ok(results)
}

#[tokio::test]
async fn merge_l2_preserves_file_order_for_tantivy_segments() -> Result<()> {
    // Build 2 dedicated files with disjoint PK ranges so that the expected
    // merged doc order is unambiguous: PK 1,2,3,4.
    let bytes_slow = new_ded(1)
        .lp(100, 0, |d| {
            d(1, 100, false, "slow1");
            d(2, 90, false, "slow2");
        })
        .finish_as_bytes();
    let bytes_fast = new_ded(2)
        .lp(100, 0, |d| {
            d(3, 80, false, "fast3");
            d(4, 70, false, "fast4");
        })
        .finish_as_bytes();

    // Make the first file load slower, forcing `cached_read_index()` futures to
    // complete out-of-order. The merge implementation must still keep the
    // Tantivy segment order aligned with `files` order.
    let file_slow = EDedicatedFile::new(
        Arc::new(DelayFile::new(1, bytes_slow, Duration::from_millis(50))),
        FtsCache::disabled(),
    )?;
    let file_fast = EDedicatedFile::new(
        Arc::new(DelayFile::new(2, bytes_fast, Duration::from_millis(0))),
        FtsCache::disabled(),
    )?;
    let files = vec![file_slow, file_fast];

    let output = merge_fts_l2_files(&files, 0, DedicatedFileBuilderOptions::default())
        .await?
        .expect("should produce merged file");

    let merged = DedicatedFile::from_buffer(output.data.as_ref())?;
    let merged_int = merged.as_int().unwrap();
    let reader = merged.cached_read_index().await?;

    // Term from the "fast" file should map to doc_id=2 -> pk=3.
    let doc_ids = search_doc_ids(&reader, "fast3")?;
    assert_eq!(doc_ids, vec![2]);
    let (pk_bytes, ..) = merged_int
        .async_at(doc_ids[0] as usize, u64::MAX)
        .await?
        .expect("doc should be visible");
    assert_eq!(IntPk::decode(pk_bytes.as_ref())?, 3);

    // Term from the "slow" file should map to doc_id=0 -> pk=1.
    let doc_ids = search_doc_ids(&reader, "slow1")?;
    assert_eq!(doc_ids, vec![0]);
    let (pk_bytes, ..) = merged_int
        .async_at(doc_ids[0] as usize, u64::MAX)
        .await?
        .expect("doc should be visible");
    assert_eq!(IntPk::decode(pk_bytes.as_ref())?, 1);

    Ok(())
}
