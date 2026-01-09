// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    convert::TryFrom,
    io::{Cursor, Write},
};

use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use tantivy::common::BitSet;
use tidb_query_datatype::codec::table::{append_common_handle_row_key, append_row_key};

use super::merge_utils::rewrite_single_index;
use crate::table::fts::{
    dedicated_file::{
        DedicatedFile, DedicatedFileBuildSummary, DedicatedFileBuilder,
        DedicatedFileBuilderOptions, EDedicatedFile,
    },
    iter::{CommonPk, IntPk, OrderedPkIterator, PkType},
    packed_file::{
        EPackedFileLp, PackedFile, PackedFileBuildSummary, PackedFileBuilder,
        PackedFileBuilderOptions, PackedFileLp,
    },
    PkReader,
};

/// Key range used by trim overbound. Start is inclusive, end is exclusive when
/// provided.
#[derive(Clone, Debug, Default)]
pub struct KeyRange {
    pub start: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
}

impl KeyRange {
    fn contains(&self, key: &[u8]) -> bool {
        if let Some(start) = &self.start {
            if key < start.as_slice() {
                return false;
            }
        }
        if let Some(end) = &self.end {
            if key >= end.as_slice() {
                return false;
            }
        }
        true
    }

    fn validate(&self) -> Result<()> {
        if let (Some(start), Some(end)) = (&self.start, &self.end) {
            if start >= end {
                bail!("invalid key range: start must be smaller than end");
            }
        }
        Ok(())
    }
}

/// Specification for in-place compaction.
#[derive(Clone, Debug)]
pub enum InplaceSpec {
    TrimOverbound(KeyRange),
    TruncateTs { truncate_ts: u64 },
    DestroyRange { prefixes: Vec<Vec<u8>> },
}

/// Result of rewriting a file in-place.
pub enum InplaceResult<S> {
    NoChange,
    Rewritten { data: Bytes, summary: S },
    Removed,
}

pub type PackedInplaceResult = InplaceResult<PackedFileBuildSummary>;
pub type DedicatedInplaceResult = InplaceResult<DedicatedFileBuildSummary>;

/// Rewrite a packed file by trim_overbound/truncate_ts/destroy_range.
pub async fn rewrite_packed_file(
    source: &PackedFile,
    spec: &InplaceSpec,
    options: &PackedFileBuilderOptions,
) -> Result<PackedInplaceResult> {
    validate_range_if_needed(spec)?;
    let mut lp_iter = source.lp_iter()?;
    let mut buffer = Vec::new();
    let mut builder = PackedFileBuilder::new(Cursor::new(&mut buffer), *options);
    let mut wrote_lp = 0usize;
    let mut changed = false;
    let mut saw_input = false;

    while let Some(lp) = lp_iter.next_lp().await? {
        saw_input = true;
        match rewrite_packed_lp(&mut builder, &lp, spec).await? {
            PackedLpRewrite::Removed { total_docs } => {
                if total_docs > 0 {
                    changed = true;
                }
            }
            PackedLpRewrite::Rewritten {
                changed: lp_changed,
            } => {
                wrote_lp += 1;
                if lp_changed {
                    changed = true;
                }
            }
        }
    }

    if wrote_lp == 0 {
        if changed {
            return Ok(InplaceResult::Removed);
        }
        if saw_input {
            return Ok(InplaceResult::NoChange);
        }
        return Ok(InplaceResult::NoChange);
    }

    if !changed {
        return Ok(InplaceResult::NoChange);
    }

    let summary = builder.finish(source.props().snap_version.into())?;
    Ok(InplaceResult::Rewritten {
        data: Bytes::from(buffer),
        summary,
    })
}

enum PackedLpRewrite {
    Removed { total_docs: u32 },
    Rewritten { changed: bool },
}

async fn rewrite_packed_lp<W: Write>(
    builder: &mut PackedFileBuilder<W>,
    lp: &EPackedFileLp,
    spec: &InplaceSpec,
) -> Result<PackedLpRewrite> {
    async fn inner<Pk: PkType, W: Write>(
        builder: &mut PackedFileBuilder<W>,
        lp: &PackedFileLp<Pk>,
        spec: &InplaceSpec,
    ) -> Result<PackedLpRewrite> {
        let total_docs = lp.props().get_n_pk();
        let mut iter = lp.pk_iter()?;
        let mut started = false;
        let mut alive = BitSet::with_max_value_and_full(total_docs);
        let mut removed_any = false;

        let table_id = lp.props().get_table_id();
        let index_id = lp.props().get_index_id();
        let lp_key = lp.lp_key().as_ref();

        while let Some((doc_id, pk_encoded, version, is_deleted)) = iter.next().await? {
            if should_keep_entry::<Pk>(spec, table_id, pk_encoded.as_ref(), version, is_deleted)? {
                if !started {
                    builder.start_lp(table_id, index_id, Pk::IS_INT, lp_key)?;
                    started = true;
                }
                let pk = Pk::decode(pk_encoded.as_ref())?;
                builder.add_pk::<Pk>(pk, version, is_deleted != 0)?;
            } else {
                alive.remove(doc_id);
                removed_any = true;
            }
        }

        if total_docs == 0 || alive.len() == 0 {
            return Ok(PackedLpRewrite::Removed { total_docs });
        }

        let index = lp.cached_read_index()?.tantivy_index().clone();
        if !removed_any {
            let dir = rewrite_single_index(index, None)?;
            builder.finish_lp(&dir)?;
            return Ok(PackedLpRewrite::Rewritten { changed: false });
        }

        let mut buffer = Vec::new();
        alive.serialize(&mut buffer)?;
        let dir = rewrite_single_index(index, Some(buffer))?;
        builder.finish_lp(&dir)?;
        Ok(PackedLpRewrite::Rewritten { changed: true })
    }

    match lp {
        EPackedFileLp::Int(lp) => inner::<IntPk, _>(builder, lp, spec).await,
        EPackedFileLp::Common(lp) => inner::<CommonPk, _>(builder, lp, spec).await,
    }
}

/// Rewrite a dedicated file via trim_overbound/truncate_ts/destroy_range.
pub async fn rewrite_dedicated_file(
    file: &EDedicatedFile,
    spec: &InplaceSpec,
    options: &DedicatedFileBuilderOptions,
) -> Result<DedicatedInplaceResult> {
    validate_range_if_needed(spec)?;

    async fn inner<Pk: PkType>(
        file: &DedicatedFile<Pk>,
        spec: &InplaceSpec,
        options: &DedicatedFileBuilderOptions,
    ) -> Result<DedicatedInplaceResult> {
        let table_id = file.props().get_table_id();
        let index_id = file.props().get_index_id();
        let lp_key = file.props().get_lp_key();
        let mut iter = file.pk_iter()?;
        let total_docs_u64 = file.props().get_pk_total();
        let mut alive = BitSet::with_max_value_and_full(
            u32::try_from(total_docs_u64)
                .map_err(|_| anyhow!("pk_total too large: {}", total_docs_u64))?,
        );
        let mut removed_any = false;
        let mut buffer = Vec::new();
        let mut builder: Option<DedicatedFileBuilder<Cursor<&mut Vec<u8>>, Pk>> = None;

        while let Some((doc_id, pk_encoded, version, is_deleted)) = iter.next().await? {
            if should_keep_entry::<Pk>(spec, table_id, pk_encoded.as_ref(), version, is_deleted)? {
                if builder.is_none() {
                    builder = Some(DedicatedFileBuilder::new(
                        Cursor::new(&mut buffer),
                        *options,
                        table_id,
                        index_id,
                        lp_key,
                    )?);
                }
                let pk = Pk::decode(pk_encoded.as_ref())?;
                builder
                    .as_mut()
                    .unwrap()
                    .add_pk(pk, version, is_deleted != 0)?;
            } else {
                alive.remove(doc_id);
                removed_any = true;
            }
        }

        if alive.max_value() == 0 || alive.len() == 0 {
            return Ok(InplaceResult::Removed);
        }

        if !removed_any {
            return Ok(InplaceResult::NoChange);
        }

        let builder = builder.expect("builder must exist when entries are kept");
        let mut buffer_alive = Vec::new();
        alive.serialize(&mut buffer_alive)?;
        let index = file.cached_read_index().await?.tantivy_index().clone();
        let dir = rewrite_single_index(index, Some(buffer_alive))?;
        let summary = builder.finish(&dir)?;
        Ok(InplaceResult::Rewritten {
            data: Bytes::from(buffer),
            summary,
        })
    }

    match file {
        EDedicatedFile::Int(file) => inner(file, spec, options).await,
        EDedicatedFile::Common(file) => inner(file, spec, options).await,
    }
}

fn should_keep_entry<Pk: PkType>(
    spec: &InplaceSpec,
    table_id: i64,
    pk_encoded: &[u8],
    version: u64,
    _is_deleted: u8,
) -> Result<bool> {
    match spec {
        InplaceSpec::TrimOverbound(range) => {
            let row_key = build_row_key::<Pk>(table_id, pk_encoded)?;
            Ok(range.contains(&row_key))
        }
        InplaceSpec::TruncateTs { truncate_ts } => Ok(version <= *truncate_ts),
        InplaceSpec::DestroyRange { prefixes } => {
            let row_key = build_row_key::<Pk>(table_id, pk_encoded)?;
            Ok(!prefixes.iter().any(|p| row_key.starts_with(p)))
        }
    }
}

fn build_row_key<Pk: PkType>(table_id: i64, pk_encoded: &[u8]) -> Result<Vec<u8>> {
    let mut key = Vec::new();
    if Pk::IS_INT {
        let handle = IntPk::decode(pk_encoded)?;
        append_row_key(&mut key, table_id, handle)
            .map_err(|e| anyhow!("failed to encode row key: {e}"))?;
    } else {
        append_common_handle_row_key(&mut key, table_id, pk_encoded)
            .map_err(|e| anyhow!("failed to encode row key: {e}"))?;
    }
    Ok(key)
}

fn validate_range_if_needed(spec: &InplaceSpec) -> Result<()> {
    if let InplaceSpec::TrimOverbound(range) = spec {
        range.validate()?;
    }
    Ok(())
}
