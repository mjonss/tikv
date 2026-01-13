// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use cloud_encryption::EncryptionKey;

use super::FtsIndexColumnarReader;
use crate::table::{
    blobtable::blobtable::BlobTable,
    columnar::{ColumnarReader, ColumnarRowTableReader, ColumnarTableReader},
    fts::{FtsDeltaBuildSpec, FtsDeltaCache, FtsDeltaKind, FtsDeltaSource, level::FtsLevels},
    memtable::CfTable,
    schema_file::Schema,
    sstable::L0Table,
};

/// Builds the reader list for the delta (unindexed) portion of the FTS query.
///
/// The returned reader list always puts the FTS index reader first.
pub fn build_fts_delta_readers(
    fts_delta_cache: &FtsDeltaCache,
    _tag: &crate::ShardTag,
    table_id: i64,
    query: &clara_fts::Query,
    read_row_schema: Schema,
    read_ts: u64,
    fts_levels: Arc<FtsLevels>,
    mem_tbls: &[CfTable],
    unconverted_l0s: &[L0Table],
    columnar_l0_files: &[crate::table::columnar::ColumnarFile],
    blob_tables: Arc<HashMap<u64, BlobTable>>,
    encryption_key: Option<EncryptionKey>,
    start_handle: Option<Bytes>,
    end_handle: Option<Bytes>,
) -> crate::Result<Vec<Box<dyn ColumnarReader>>> {
    let cache = fts_delta_cache;
    let index_id = query.info().get_index_id();

    let mut delta_sources: Vec<FtsDeltaSource> = Vec::new();
    let mut delta_build_specs: Vec<FtsDeltaBuildSpec> = Vec::new();
    let mut fallback_readers: Vec<Box<dyn ColumnarReader>> = Vec::new();

    // For memtable currently we always use brute-force fallback readers.
    // Because in read node we can only get one virtual memtable from the snapshot
    // currently.
    for mem in mem_tbls {
        let skl = mem.get_cf(crate::WRITE_CF);
        if skl.is_empty() {
            continue;
        }
        // Writable memtable, or cache disabled: brute-force fallback.
        let iter = skl.new_iterator(false);
        fallback_readers.push(Box::new(ColumnarRowTableReader::new(
            read_row_schema.clone(),
            iter,
            Some(Arc::clone(&blob_tables)),
            false,
            encryption_key.clone(),
        )));
    }

    // Unconverted row L0s.
    for l0 in unconverted_l0s {
        let Some(l0_write) = l0.get_cf(crate::WRITE_CF) else {
            continue;
        };

        if !cache.is_enabled() {
            // Cache disabled: brute-force fallback.
            let iter = l0_write.new_iterator(false, true);
            fallback_readers.push(Box::new(ColumnarRowTableReader::new(
                read_row_schema.clone(),
                iter,
                Some(Arc::clone(&blob_tables)),
                false,
                encryption_key.clone(),
            )));
            continue;
        }

        // Cache enabled: must use delta index.
        if let Some(entry) = cache.get(FtsDeltaKind::RowL0, l0.id(), table_id, index_id) {
            delta_sources.push(FtsDeltaSource {
                entry,
                seq: l0.snap_version().into_inner(),
            });
            continue;
        }
        delta_build_specs.push(FtsDeltaBuildSpec {
            kind: FtsDeltaKind::RowL0,
            source_id: l0.id(),
            table_id,
            index_id,
            input: crate::table::fts::delta_cache::FtsDeltaInput::RowL0 {
                l0: l0.clone(),
                snap_version: l0.snap_version(),
            },
        });
    }

    // Untracked columnar L0 files.
    for col_file in columnar_l0_files {
        if !col_file.has_table(read_row_schema.table_id) {
            continue;
        }
        let snap_version = col_file.get_snap_version().unwrap_or_default();
        if fts_levels.is_columnar_l0_tracked(col_file.id(), snap_version) {
            continue;
        }

        if !cache.is_enabled() {
            // Cache disabled: brute-force fallback.
            fallback_readers.push(Box::new(ColumnarTableReader::new(
                col_file,
                read_row_schema.clone(),
                None,
                encryption_key.clone(),
            )));
            continue;
        }

        // Cache enabled: must use delta index.
        if let Some(entry) = cache.get(FtsDeltaKind::ColumnarL0, col_file.id(), table_id, index_id)
        {
            delta_sources.push(FtsDeltaSource {
                entry,
                seq: snap_version.into_inner(),
            });
            continue;
        }
        delta_build_specs.push(FtsDeltaBuildSpec {
            kind: FtsDeltaKind::ColumnarL0,
            source_id: col_file.id(),
            table_id,
            index_id,
            input: crate::table::fts::delta_cache::FtsDeltaInput::ColumnarL0 {
                file: col_file.clone(),
                snap_version,
            },
        });
    }

    let index_reader = FtsIndexColumnarReader::new(
        read_row_schema,
        query.clone(),
        table_id,
        read_ts,
        fts_levels,
        delta_sources,
        delta_build_specs,
        fts_delta_cache.clone(),
        blob_tables,
        encryption_key,
        start_handle,
        end_handle,
    )?;

    let mut readers: Vec<Box<dyn ColumnarReader>> = Vec::with_capacity(1 + fallback_readers.len());
    readers.push(Box::new(index_reader));
    readers.extend(fallback_readers);
    Ok(readers)
}
