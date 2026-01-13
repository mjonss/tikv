// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, io::Cursor, mem::size_of, sync::Arc};

use anyhow::{Context, Result};
use bytes::Bytes;
use clara_fts::{IndexReader as ClaraIndexReader, TantivyIndexWriter};
use quick_cache::{Weighter, sync::Cache};
use tikv_util::sys::SysQuota;

use super::util::{build_stringify_fn, empty_stringify_fn};
use crate::table::{
    ChecksumType, SnapVersion,
    blobtable::blobtable::BlobTable,
    columnar::{Block, ColumnarReader, ColumnarRowTableReader, ColumnarTableReader},
    file::InMemFile,
    fts::{
        FtsCache, PackedFile, compact,
        packed_file::{EPackedFileLp, PackedFileBuilder, PackedFileBuilderOptions},
    },
    memtable::CfTable,
    schema_file::Schema,
    sstable::L0Table,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FtsDeltaKind {
    /// A sealed (immutable) memtable.
    /// FIXME: Currently in read node we can only get one virtual memtable from
    /// the snapshot, so this kind is currently unused.
    Memtable,
    /// A row-format L0 (SST L0) that is not converted to columnar yet.
    RowL0,
    /// A columnar L0 file that is not covered by the official FTS index yet.
    ColumnarL0,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct CacheKey {
    kind: FtsDeltaKind,
    source_id: u64,
    table_id: i64,
    index_id: i64,
}

#[derive(Clone)]
pub enum FtsDeltaInput {
    /// FIXME: Currently in read node we can only get one virtual memtable from
    /// the snapshot, so this kind is currently unused.
    Memtable {
        mem: CfTable,
        snap_version: SnapVersion,
    },
    RowL0 {
        l0: L0Table,
        snap_version: SnapVersion,
    },
    ColumnarL0 {
        file: crate::table::columnar::ColumnarFile,
        snap_version: SnapVersion,
    },
}

impl FtsDeltaInput {
    pub fn snap_version(&self) -> SnapVersion {
        match self {
            Self::Memtable { snap_version, .. }
            | Self::RowL0 { snap_version, .. }
            | Self::ColumnarL0 { snap_version, .. } => *snap_version,
        }
    }
}

#[derive(Clone)]
pub struct FtsDeltaBuildSpec {
    pub kind: FtsDeltaKind,
    pub source_id: u64,
    pub table_id: i64,
    pub index_id: i64,
    pub input: FtsDeltaInput,
}

impl FtsDeltaBuildSpec {
    #[inline]
    fn cache_key(&self) -> CacheKey {
        CacheKey {
            kind: self.kind,
            source_id: self.source_id,
            table_id: self.table_id,
            index_id: self.index_id,
        }
    }
}

/// One cached delta index entry.
///
/// The entry is aligned to the on-disk packed file layout so that we can reuse
/// the existing MVCC/has_newer_version logic implemented on [`EPackedFileLp`].
#[derive(Clone)]
pub struct FtsDeltaCacheEntry {
    lp: Arc<EPackedFileLp>,
    index_reader: Arc<ClaraIndexReader>,
}

impl FtsDeltaCacheEntry {
    #[inline]
    pub fn lp(&self) -> &EPackedFileLp {
        &self.lp
    }

    #[inline]
    pub fn index_reader(&self) -> &Arc<ClaraIndexReader> {
        &self.index_reader
    }

    #[inline]
    fn estimated_size(&self) -> u64 {
        self.lp.serialized_size() as u64 + size_of::<Self>() as u64
    }
}

#[derive(Clone)]
struct CacheWeighter;

impl Weighter<CacheKey, Arc<FtsDeltaCacheEntry>> for CacheWeighter {
    fn weight(&self, _key: &CacheKey, value: &Arc<FtsDeltaCacheEntry>) -> u64 {
        size_of::<CacheKey>() as u64 + value.estimated_size()
    }
}

type DeltaCacheInner = Cache<CacheKey, Arc<FtsDeltaCacheEntry>, CacheWeighter>;

#[derive(Clone)]
pub struct FtsDeltaSource {
    pub entry: Arc<FtsDeltaCacheEntry>,
    /// Ordering key used by the MVCC shadowing algorithm. Higher means newer.
    pub seq: u64,
}

/// A cache for per-source, per-(table_id,index_id) in-memory FTS indexes for
/// the "unindexed" part of FTS queries.
///
/// This cache is designed for immutable sources only (sealed memtables, row
/// L0 and untracked columnar L0). Writable memtables are intentionally
/// excluded.
#[derive(Clone)]
pub struct FtsDeltaCache {
    inner: Option<Arc<DeltaCacheInner>>,
}

impl FtsDeltaCache {
    /// Creates a cache with the given total weight capacity in bytes.
    ///
    /// A capacity of 0 disables the cache.
    pub fn new(capacity_bytes: u64) -> Self {
        if capacity_bytes == 0 {
            return Self::disabled();
        }

        let cache_shards = (SysQuota::cpu_cores_quota() as usize).max(1) * 8;
        // A very rough heuristic; entries can vary wildly but we only need a
        // reasonable starting point for allocation.
        let estimated_items_capacity = (capacity_bytes as usize / (4 << 20)).max(64);

        let opts = quick_cache::OptionsBuilder::new()
            .shards(cache_shards)
            .weight_capacity(capacity_bytes)
            .estimated_items_capacity(estimated_items_capacity)
            .build()
            .unwrap();
        let cache = Cache::with_options(
            opts,
            CacheWeighter,
            quick_cache::DefaultHashBuilder::default(),
            quick_cache::sync::DefaultLifecycle::default(),
        );

        Self {
            inner: Some(Arc::new(cache)),
        }
    }

    /// Creates a disabled cache instance that never stores any entry.
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Returns a cached delta index entry if present.
    #[inline]
    pub fn get(
        &self,
        kind: FtsDeltaKind,
        source_id: u64,
        table_id: i64,
        index_id: i64,
    ) -> Option<Arc<FtsDeltaCacheEntry>> {
        let key = CacheKey {
            kind,
            source_id,
            table_id,
            index_id,
        };
        self.inner.as_ref().and_then(|cache| cache.get(&key))
    }

    /// Invalidates all cached entries for a source (across all table/index
    /// combinations).
    ///
    /// Currently unused, because Read Node always grabs a snapshot, it does not
    /// know what is really invalidated. But it is provided for future use
    /// cases.
    pub fn invalidate_memtable(&self, memtable_id: u64) {
        self.invalidate_source(FtsDeltaKind::Memtable, memtable_id);
    }

    /// Currently unused, because Read Node always grabs a snapshot, it does not
    /// know what is really invalidated. But it is provided for future use
    /// cases.
    pub fn invalidate_row_l0(&self, l0_id: u64) {
        self.invalidate_source(FtsDeltaKind::RowL0, l0_id);
    }

    /// Currently unused, because Read Node always grabs a snapshot, it does not
    /// know what is really invalidated. But it is provided for future use
    /// cases.
    pub fn invalidate_columnar_l0(&self, file_id: u64) {
        self.invalidate_source(FtsDeltaKind::ColumnarL0, file_id);
    }

    fn invalidate_source(&self, kind: FtsDeltaKind, source_id: u64) {
        let Some(cache) = &self.inner else {
            return;
        };
        cache.retain(|key, _| key.kind != kind || key.source_id != source_id);
    }

    /// Builds (or retrieves) a delta index entry for the given spec.
    ///
    /// This is a lazy, singleflight operation: concurrent callers for the same
    /// key will share the build.
    ///
    /// Returns `Ok(None)` when the cache is disabled or the source has no rows
    /// for this table.
    ///
    /// Returns `Err` when building the delta index fails.
    pub async fn get_or_build(
        &self,
        spec: FtsDeltaBuildSpec,
        schema: Schema,
        parser_type: &str,
        blob_tables: Arc<HashMap<u64, BlobTable>>,
        encryption_key: Option<cloud_encryption::EncryptionKey>,
    ) -> Result<Option<Arc<FtsDeltaCacheEntry>>> {
        let Some(cache) = self.inner.as_ref() else {
            // FIXME: We should possibly still build the index even when the cache is
            // disabled.
            return Ok(None);
        };
        let key = spec.cache_key();
        match cache.get_value_or_guard_async(&key).await {
            Ok(entry) => Ok(Some(entry)),
            Err(guard) => Ok(build_delta_index(
                &spec.input,
                key,
                schema,
                parser_type,
                blob_tables,
                encryption_key,
            )
            .await
            .with_context(|| {
                format!(
                    "delta index build failed (kind={:?}, source_id={}, table_id={}, index_id={})",
                    key.kind, key.source_id, key.table_id, key.index_id
                )
            })?
            .map(|built| {
                let entry = Arc::new(built);
                let _ = guard.insert(Arc::clone(&entry));
                entry
            })),
        }
    }
}

async fn build_delta_index(
    input: &FtsDeltaInput,
    key: CacheKey,
    schema: Schema,
    parser_type: &str,
    blob_tables: Arc<HashMap<u64, BlobTable>>,
    encryption_key: Option<cloud_encryption::EncryptionKey>,
) -> Result<Option<FtsDeltaCacheEntry>> {
    let mut buffer = Vec::new();
    let mut builder = PackedFileBuilder::new(
        Cursor::new(&mut buffer),
        PackedFileBuilderOptions {
            checksum_type: ChecksumType::None,
            ..Default::default()
        },
    );

    let lp_key = compact::lp_key(key.table_id, key.index_id);
    let is_int_handle = !schema.is_common_handle();
    builder.start_lp(key.table_id, key.index_id, is_int_handle, &lp_key)?;

    let stringify = build_stringify_fn(&schema.columns[0]).unwrap_or_else(|_| empty_stringify_fn());
    let mut index_writer = TantivyIndexWriter::new_in_memory(parser_type)?;

    let mut reader: Box<dyn ColumnarReader> = match input {
        FtsDeltaInput::Memtable { mem, .. } => {
            // FIXME: Currently in read node we can only get one virtual
            // memtable from the snapshot, so this kind is
            // currently unused.
            let iter = mem.get_cf(crate::WRITE_CF).new_iterator(false);
            Box::new(ColumnarRowTableReader::new(
                schema.clone(),
                iter,
                Some(blob_tables),
                false,
                encryption_key,
            ))
        }
        FtsDeltaInput::RowL0 { l0, .. } => {
            let Some(l0_write) = l0.get_cf(crate::WRITE_CF) else {
                return Ok(None);
            };
            let iter = l0_write.new_iterator(false, true);
            Box::new(ColumnarRowTableReader::new(
                schema.clone(),
                iter,
                Some(blob_tables),
                false,
                encryption_key,
            ))
        }
        FtsDeltaInput::ColumnarL0 { file, .. } => Box::new(ColumnarTableReader::new(
            file,
            schema.clone(),
            None,
            encryption_key,
        )),
    };

    reader.seek(&[]).await?;

    let mut any_rows = false;
    let mut block = Block::new(&schema);

    loop {
        block.reset();
        let rows = reader.read(&mut block, 1024).await?;
        if rows == 0 {
            break;
        }
        any_rows = true;

        for row in 0..rows {
            // Tantivy docs align with doc_id in the packed LP.
            let text_col = &block.columns[0];
            let Some(raw) = text_col.get_value(row) else {
                index_writer.add_null()?;
                continue;
            };
            let text = match stringify(raw) {
                Ok(text) if !text.is_empty() => text,
                _ => {
                    index_writer.add_null()?;
                    continue;
                }
            };
            index_writer.add_document(text.as_ref())?;
        }

        if is_int_handle {
            for row in 0..rows {
                let version = block.versions.get_version(row);
                let is_deleted = block.versions.is_null(row);
                let pk = block.handles.get_int_handle_value(row);
                builder.add_pk_int(pk, version, is_deleted)?;
            }
        } else {
            for row in 0..rows {
                let version = block.versions.get_version(row);
                let is_deleted = block.versions.is_null(row);
                let pk = block.handles.get_not_null_value(row);
                builder.add_pk_common(pk, version, is_deleted)?;
            }
        }
    }

    if !any_rows {
        return Ok(None);
    }

    let dir = index_writer.finalize_as_dir()?;
    builder.finish_lp(&dir)?;
    builder.finish(input.snap_version())?;

    let bytes = Bytes::from(buffer);
    let file_id = key.source_id;
    let packed = PackedFile::new(
        Arc::new(InMemFile::new(file_id, bytes)),
        FtsCache::disabled(),
    )?;
    let lp = packed
        .cached_get_lp(&lp_key)
        .await?
        .expect("delta packed file must contain exactly one lp");
    let index_reader = lp.read_tantivy_index()?;
    Ok(Some(FtsDeltaCacheEntry { lp, index_reader }))
}
