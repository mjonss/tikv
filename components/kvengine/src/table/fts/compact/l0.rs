// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    io::Cursor,
};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use clara_fts::{TantivyIndexWriter, TrackedDirectory};
use cloud_encryption::EncryptionKey;
use tantivy::directory::RamDirectory;

use super::super::util::{build_stringify_fn, empty_stringify_fn};
use crate::table::{
    SnapVersion,
    columnar::{
        Block, ColumnBuffer, ColumnarFile, ColumnarMergeReader, ColumnarReader, ColumnarTableReader,
    },
    fts::packed_file::{PackedFileBuildSummary, PackedFileBuilder, PackedFileBuilderOptions},
    schema_file::{Schema, SchemaFile},
};

/// Parameters for the Columnar to FTS L0 conversion function
pub struct ColumnarToFtsL0Opts<'a> {
    /// The columnar files to convert (can be from multiple levels).
    pub columnar_files: &'a [ColumnarFile],

    /// The schema file containing FullTextIndex definitions
    pub schema_file: SchemaFile,

    /// List of (table_id, index_id) pairs to convert - only these specific
    /// indexes will be processed, enabling proper separation between
    /// incremental updates (FTSCreateL0) and new index addition (FTSAddIndex)
    pub index_pairs: &'a [(i64, i64)],

    /// Encryption key (if any)
    pub encryption_key: Option<EncryptionKey>,

    /// The snap version associated with the resulting packed file.
    pub snap_version: SnapVersion,

    /// Hard cap for a single generated FTS L0 file. When exceeded, a new L0
    /// file will be started after finishing the current logical partition.
    /// If set to 0, no splitting will be performed.
    pub l0_file_max_size: u64,
}

#[derive(Debug)]
pub struct FtsL0Output {
    pub data: Bytes,
    pub summary: PackedFileBuildSummary,
}

/// Converts columnar files to FTS L0 file based on the FullTextIndex
/// definitions in the SchemaFile.
///
/// This function:
/// 1. Reads the columnar files using ColumnarMergeReader
/// 2. Extracts text data from columns defined in FullTextIndex
/// 3. Builds FTS indexes for each text column
/// 4. Creates a new FTS L0 file with the indexed data
/// 5. Returns the serialized FTS L0 file data
pub async fn columnar_to_fts_l0(opts: ColumnarToFtsL0Opts<'_>) -> Result<Vec<FtsL0Output>> {
    if opts.index_pairs.is_empty() || opts.columnar_files.is_empty() {
        return Ok(Vec::new());
    }

    // Collect only the specified fulltext indexes from the schema file for the
    // given (table_id, index_id) pairs. This ensures only tracked indexes are
    // processed for incremental updates.
    let mut requested_indexes_by_table: HashMap<i64, HashSet<i64>> = HashMap::new();
    for &(table_id, index_id) in opts.index_pairs {
        requested_indexes_by_table
            .entry(table_id)
            .or_default()
            .insert(index_id);
    }

    let mut fts_indexes_by_table: HashMap<i64, Vec<&kvenginepb::fts::FullTextIndexDef>> =
        HashMap::new();
    for (table_id, requested_index_ids) in requested_indexes_by_table {
        let schema = match opts.schema_file.get_table(table_id) {
            Some(schema) => schema,
            None => continue, // Skip tables not in the schema file
        };

        let filtered_indexes: Vec<&kvenginepb::fts::FullTextIndexDef> = schema
            .fulltext_indexes
            .iter()
            .filter(|idx| requested_index_ids.contains(&idx.index_id))
            .collect();

        if !filtered_indexes.is_empty() {
            fts_indexes_by_table.insert(table_id, filtered_indexes);
        }
    }

    if fts_indexes_by_table.is_empty() {
        return Ok(Vec::new());
    }

    // IMPORTANT:
    // Make sure to iterate by Table IDs in order, so that the logical partitions
    // are generated in order.
    let mut ordered_table_ids: Vec<i64> = fts_indexes_by_table.keys().cloned().collect();
    ordered_table_ids.sort_unstable();

    // Create a PackedFile builder for the FTS L0 files.
    // We may split into multiple output files at LP boundaries.
    let mut outputs = Vec::new();
    let mut target_buffer = Vec::new();
    let mut builder = PackedFileBuilder::new(
        Cursor::new(&mut target_buffer),
        PackedFileBuilderOptions::default(),
    );

    // Reused in each iteration.
    let mut needed_col_ids = HashSet::new();
    let mut col_id_to_pos = HashMap::with_capacity(16);
    let mut ordered_indexes = Vec::new();

    let mut has_index_to_build = false;

    // Process each table and build FTS indexes
    for table_id in ordered_table_ids {
        // Try to figure out what columns are really needed, and ignore other columns.
        // This creates an optimized schema that only includes the handle, versions, and
        // text columns that are referenced by fulltext indexes.

        let index_def = fts_indexes_by_table.get(&table_id).unwrap().as_slice();
        let schema: Schema = {
            let full_schema = opts.schema_file.get_table(table_id).unwrap();
            needed_col_ids.clear();
            for idx in index_def {
                needed_col_ids.insert(idx.col_id);
            }
            full_schema
                .retain_columns(|col| needed_col_ids.contains(&col.get_column_id()))
                .into()
        };

        // Also attach the position of the referenced column for each index
        // to avoid lookups.

        col_id_to_pos.clear();
        for (idx, col) in schema.columns.iter().enumerate() {
            col_id_to_pos.insert(col.get_column_id(), idx);
        }
        ordered_indexes.clear();
        for idx in index_def {
            let col_pos = col_id_to_pos.get(&idx.col_id).cloned();
            // It is possible that schema's column and schema's fts index def is not
            // matching. In that case, the index will not be built.
            if let Some(col_pos) = col_pos {
                ordered_indexes.push(FullTextIndexDefWithPos {
                    inner: (*idx).clone(),
                    resolved_col_pos: col_pos,
                });
            }
        }

        if ordered_indexes.is_empty() {
            continue;
        }

        ordered_indexes.sort_unstable_by(|a, b| a.inner.index_id.cmp(&b.inner.index_id));
        has_index_to_build = true;

        let Some((index_dirs, buf_handles, buf_versions, is_common_handle)) =
            build_fts_dirs_for_table(
                opts.columnar_files,
                schema,
                table_id,
                ordered_indexes.as_slice(),
                opts.encryption_key.clone(),
            )
            .await
            .with_context(|| {
                let index_ids: Vec<i64> = ordered_indexes
                    .iter()
                    .map(|idx| idx.inner.index_id)
                    .collect();
                format!(
                    "Failed to process table {} with indexes {:?} for FTS indexing",
                    table_id, index_ids
                )
            })?
        else {
            continue;
        };

        for (indexer_i, dir) in index_dirs.into_iter().enumerate() {
            let index_id = ordered_indexes[indexer_i].inner.index_id;
            let lp_key = super::lp_key(table_id, index_id);
            builder.start_lp(table_id, index_id, !is_common_handle, &lp_key)?;

            if is_common_handle {
                for row_index in 0..buf_handles.length() {
                    builder.add_pk_common(
                        buf_handles.get_not_null_value(row_index),
                        buf_versions.get_version(row_index),
                        buf_versions.is_null(row_index),
                    )?;
                }
            } else {
                for row_index in 0..buf_handles.length() {
                    builder.add_pk_int(
                        buf_handles.get_int_handle_value(row_index),
                        buf_versions.get_version(row_index),
                        buf_versions.is_null(row_index),
                    )?;
                }
            }

            builder.finish_lp(&dir)?;

            if opts.l0_file_max_size > 0 && builder.written_size() > opts.l0_file_max_size {
                let summary = builder
                    .finish(opts.snap_version)
                    .context("Failed to finish building FTS L0 file")?;
                if summary.props.pk_total > 0 {
                    validate_non_empty_summary(&summary)?;
                    let data = Bytes::from(std::mem::take(&mut target_buffer));
                    outputs.push(FtsL0Output { data, summary });
                }
                builder = PackedFileBuilder::new(
                    Cursor::new(&mut target_buffer),
                    PackedFileBuilderOptions::default(),
                );
            }
        }
    }

    if !has_index_to_build {
        return Ok(Vec::new());
    }

    // Finish the last file.
    let summary = builder
        .finish(opts.snap_version)
        .context("Failed to finish building FTS L0 file")?;
    if summary.props.pk_total > 0 {
        validate_non_empty_summary(&summary)?;
        outputs.push(FtsL0Output {
            data: Bytes::from(target_buffer),
            summary,
        });
    }

    Ok(outputs)
}

/// Processes a table's data to extract text content from columns with fulltext
/// indexes and adds the data to the FTS index.
async fn build_fts_dirs_for_table(
    columnar_files: &[ColumnarFile],
    schema: Schema,
    table_id: i64,
    ordered_indexes: &[FullTextIndexDefWithPos], // The pos can be used to lookup column in schema
    encryption_key: Option<EncryptionKey>,
) -> Result<
    Option<(
        Vec<TrackedDirectory<RamDirectory>>,
        ColumnBuffer,
        ColumnBuffer,
        bool,
    )>,
> {
    // Create individual ColumnarTableReader for each columnar file
    // This is necessary because columnar L0 files can overlap (unlike L2 files)
    let mut readers: Vec<Box<dyn ColumnarReader>> = Vec::new();
    for columnar_file in columnar_files {
        if !columnar_file.has_table(table_id) {
            continue;
        }
        let reader = ColumnarTableReader::new(
            columnar_file,
            schema.clone(),
            None, // no filter operation
            encryption_key.clone(),
        );
        readers.push(Box::new(reader));
    }

    if readers.is_empty() {
        // No files contain this table
        return Ok(None);
    }

    let mut merge_reader = ColumnarMergeReader::new(schema.clone(), readers);
    merge_reader.seek(&[]).await?;

    // One Tantivy indexer for each index
    let mut indexers = ordered_indexes
        .iter()
        .map(|index_def| TantivyIndexWriter::new_in_memory(&index_def.inner.parser_type))
        .collect::<Result<Vec<_>>>()?;

    // Build stringify functions once per indexed column.
    // Callers may choose to treat unsupported collations as non-searchable.
    let stringifiers = ordered_indexes
        .iter()
        .map(|index| {
            let col_info = &schema.columns[index.resolved_col_pos];
            build_stringify_fn(col_info).unwrap_or_else(|_| empty_stringify_fn())
        })
        .collect::<Vec<_>>();

    let mut is_data_indexed = false;

    // We buffer all handles and versions, because we can only write target
    // L0 by logical partitions.
    let mut buf_handles = ColumnBuffer::new_from_col_info(&schema.handle_column);
    let mut buf_versions = ColumnBuffer::new_from_col_info(&schema.version_column);

    const BATCH_SIZE: usize = 1024;
    let mut block = Block::new(&schema);
    loop {
        block.reset();
        let rows_read = merge_reader
            .read(&mut block, BATCH_SIZE)
            .await
            .context("Failed to read data from columnar table")?;

        // No more data to read
        if rows_read == 0 {
            break;
        }

        is_data_indexed = true;

        // We will deal with PK and versions later
        {
            buf_handles.append(&block.handles, 0, rows_read);
            buf_versions.append(&block.versions, 0, rows_read);
        }

        for row_idx in 0..rows_read {
            // IMPORTANT: We utilized the feature that returned rows are ordered
            // by (handle, !version).
            // This becomes the doc_id of the Tantivy index.
            for (index_i, index) in ordered_indexes.iter().enumerate() {
                let col_pos = index.resolved_col_pos;
                let Some(value_ref) = block.columns[col_pos].get_value(row_idx) else {
                    indexers[index_i].add_null()?;
                    continue;
                };
                let text = match (stringifiers[index_i])(value_ref) {
                    Ok(text) if !text.is_empty() => text,
                    _ => {
                        // Keep doc_id aligned with row index.
                        indexers[index_i].add_null()?;
                        continue;
                    }
                };
                indexers[index_i].add_document(text.as_ref())?;
            }
        }
    }

    if !is_data_indexed {
        // No data to index, skip this table (and all its indexes)
        return Ok(None);
    }

    let mut index_dirs = Vec::with_capacity(indexers.len());
    for indexer in indexers.into_iter() {
        index_dirs.push(indexer.finalize_as_dir()?);
    }

    Ok(Some((
        index_dirs,
        buf_handles,
        buf_versions,
        schema.is_common_handle(),
    )))
}

fn validate_non_empty_summary(summary: &PackedFileBuildSummary) -> Result<()> {
    if summary.props.smallest_key.is_empty() || summary.props.biggest_key.is_empty() {
        bail!("Unexpected empty smallest/largest row key in the props");
    }
    if summary.props.smallest_lp_key.is_empty() || summary.props.largest_lp_key.is_empty() {
        bail!("Unexpected empty smallest/largest LP key in the props");
    }
    Ok(())
}

/// A wrapper for FullTextIndexDef that includes the resolved column position.
///
/// This struct is used during FTS index creation to avoid repeatedly looking up
/// the position of a column in the schema. It contains the original
/// FullTextIndexDef and adds the resolved_col_pos field for efficient access.
#[derive(Debug, Clone)]
struct FullTextIndexDefWithPos {
    /// The original FullTextIndexDef
    pub inner: kvenginepb::fts::FullTextIndexDef,
    /// The resolved position of the column in the schema
    pub resolved_col_pos: usize,
}
