// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashSet},
    io::Cursor,
};

use anyhow::{Result, bail};
use bytes::Bytes;

use super::merge_utils::{MergeMapping, compute_merge_mapping, merge_tantivy_indexes};
use crate::table::{
    SnapVersion,
    fts::{
        IntPk,
        dedicated_file::{
            DedicatedFileBuildSummary, DedicatedFileBuilder, DedicatedFileBuilderOptions,
        },
        iter::{CommonPk, PkReader, PkType},
        packed_file::{
            EPackedFileLp, PackedFile, PackedFileBuildSummary, PackedFileBuilder,
            PackedFileBuilderOptions,
        },
    },
};

/// Options for merging FTS PackedFiles.
pub struct PackedFileMergeOpt {
    /// Maximum size for a single output PackedFile.
    /// When an output file exceeds this size, a new file will be started
    /// (after completing the current logical partition).
    pub max_pack_file_size: u64,

    /// The minimum size for a L2 logical partition.
    /// When a logical partition's total size exceeds this threshold, it will be
    /// promoted to L2 during the merge.
    pub min_l2_lp_size: u64,

    /// Options for building output PackedFiles.
    /// Controls compression, block size, and other file-level settings.
    pub pack_opt: PackedFileBuilderOptions,

    /// Options for building DedicatedFiles when emitting L2 outputs.
    pub ded_opt: DedicatedFileBuilderOptions,
}

/// Merges FTS L0 and L1 PackedFiles into new L1/L2 outputs.
pub async fn merge_fts_l0_l1(
    source_files: &[PackedFile], // L0 and L1 PackedFiles
    options: PackedFileMergeOpt,
    safe_ts: u64, // For removing tombstones
    existing_l2_lp_keys: &HashSet<Vec<u8>>,
    tracked_indexes: &HashSet<(i64, i64)>,
) -> Result<MergeOutputs> {
    if source_files.is_empty() {
        return Ok(MergeOutputs::default());
    }

    struct HeapItem {
        lp: EPackedFileLp,
        src_idx: usize, // Which PackedFile does this LP come from
    }
    impl PartialEq for HeapItem {
        fn eq(&self, other: &Self) -> bool {
            self.lp.lp_key() == other.lp.lp_key()
        }
    }
    impl Eq for HeapItem {}
    impl PartialOrd for HeapItem {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for HeapItem {
        fn cmp(&self, other: &Self) -> Ordering {
            self.lp.lp_key().cmp(other.lp.lp_key())
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum LpTargetLevel {
        L1,
        L2,
    }

    let PackedFileMergeOpt {
        max_pack_file_size,
        min_l2_lp_size,
        pack_opt,
        ded_opt,
    } = options;

    // Initialize LP iterators for all source files
    let mut lp_sources = Vec::with_capacity(source_files.len());
    for file in source_files {
        lp_sources.push(file.lp_iter()?);
    }

    // Initialize LP-level min-heap
    let mut lp_heap: BinaryHeap<Reverse<HeapItem>> = BinaryHeap::new();
    for (src_idx, source) in &mut lp_sources.iter_mut().enumerate() {
        if let Some(lp) = source.next_lp().await? {
            lp_heap.push(Reverse(HeapItem { lp, src_idx }));
        }
    }

    let mut outputs = MergeOutputs::default();

    let mut l1_buffer = Vec::new();
    let mut l1_builder = PackedFileBuilder::new(Cursor::new(&mut l1_buffer), pack_opt);
    let mut lps_to_merge = Vec::with_capacity(8);

    // Main merge loop
    while let Some(Reverse(item)) = lp_heap.peek() {
        // NOTE: `lp.lp_key()` may be backed by mmapped Bytes (IA segment). It is
        // fine to keep a zero-copy `Bytes` clone within this function for
        // grouping/comparison, but never let it escape (e.g. put into outputs /
        // caches) without copying; otherwise it can pin the source mmap (and
        // thus inode/blocks) even after the source file is removed.
        let this_lp_key = item.lp.lp_key().clone();

        // Collect all LPs with the same key
        lps_to_merge.clear();
        while let Some(Reverse(item)) = lp_heap.peek() {
            if item.lp.lp_key().as_ref() != this_lp_key.as_ref() {
                break;
            }
            let Reverse(item) = lp_heap.pop().unwrap();
            lps_to_merge.push(item.lp);

            // Advance this source
            if let Some(next_lp) = lp_sources[item.src_idx].next_lp().await? {
                lp_heap.push(Reverse(HeapItem {
                    lp: next_lp,
                    src_idx: item.src_idx,
                }));
            }
        }

        if lps_to_merge.is_empty() {
            continue;
        }

        check_lps_can_merge(&lps_to_merge)?;
        let first_lp = &lps_to_merge[0];
        let index_pair = (
            first_lp.props().get_table_id(),
            first_lp.props().get_index_id(),
        );
        if !tracked_indexes.contains(&index_pair) {
            // This logical partition no longer belongs to a tracked index;
            // skip writing it so its data gets dropped.
            continue;
        }

        let lp_total_size: usize = lps_to_merge.iter().map(|lp| lp.serialized_size()).sum();
        let already_in_l2 = existing_l2_lp_keys.contains(this_lp_key.as_ref());
        let promote_by_size = lp_total_size as u64 >= min_l2_lp_size;
        let target_level = if already_in_l2 || promote_by_size {
            LpTargetLevel::L2
        } else {
            LpTargetLevel::L1
        };

        let lp_n_docs: Vec<u32> = lps_to_merge
            .iter()
            .map(|lp| lp.props().get_n_pk())
            .collect();
        let merge_mapping = match &lps_to_merge[0] {
            EPackedFileLp::Int(_) => {
                let mut iters = Vec::with_capacity(lps_to_merge.len());
                for lp in &lps_to_merge {
                    iters.push(lp.as_int_lp()?.pk_iter()?);
                }
                compute_merge_mapping(iters, &lp_n_docs, safe_ts).await?
            }
            EPackedFileLp::Common(_) => {
                let mut iters = Vec::with_capacity(lps_to_merge.len());
                for lp in &lps_to_merge {
                    iters.push(lp.as_common_lp()?.pk_iter()?);
                }
                compute_merge_mapping(iters, &lp_n_docs, safe_ts).await?
            }
        };

        if merge_mapping.doc_mapping.is_empty() {
            // No live documents after safe_ts cleaning; skip this LP
            continue;
        }

        match target_level {
            LpTargetLevel::L1 => {
                write_lp_to_packed(&mut l1_builder, &lps_to_merge, &merge_mapping)?;

                // Check if we should start a new file
                // We check after finishing the LP to ensure we split at LP boundaries
                if l1_builder.written_size() > max_pack_file_size {
                    // L1 does not have snap_version.
                    let summary = l1_builder.finish(SnapVersion::zero())?;
                    check_new_packed(&summary)?;
                    let data = Bytes::from(std::mem::take(&mut l1_buffer));
                    outputs.l1_files.push(L1FileOutput { data, summary });
                    l1_builder = PackedFileBuilder::new(Cursor::new(&mut l1_buffer), pack_opt);
                }
            }
            LpTargetLevel::L2 => {
                let (data, summary) =
                    write_lp_to_dedicated(&lps_to_merge, &merge_mapping, ded_opt)?;
                outputs.l2_files.push(L2FileOutput {
                    lp_key: this_lp_key.as_ref().to_vec(),
                    data,
                    summary,
                });
            }
        }
    }

    {
        // L1 does not have snap_version.
        let summary = l1_builder.finish(SnapVersion::zero())?;
        if summary.props.pk_total > 0 {
            check_new_packed(&summary)?;
            let data = Bytes::from(l1_buffer);
            outputs.l1_files.push(L1FileOutput { data, summary });
        }
    }

    Ok(outputs)
}

/// Outputs produced by merging L0/L1 files.
#[derive(Default, Debug)]
pub struct MergeOutputs {
    pub l1_files: Vec<L1FileOutput>,
    pub l2_files: Vec<L2FileOutput>,
}

/// Represents a newly built L1 PackedFile.
#[derive(Debug)]
pub struct L1FileOutput {
    pub data: Bytes,
    pub summary: PackedFileBuildSummary,
}

/// Represents a newly built L2 DedicatedFile for a specific logical partition.
#[derive(Debug)]
pub struct L2FileOutput {
    pub lp_key: Vec<u8>,
    pub data: Bytes,
    pub summary: DedicatedFileBuildSummary,
}

/// Validates that all LPs have consistent properties so that they can be
/// merged.
fn check_lps_can_merge(lps: &[EPackedFileLp]) -> Result<()> {
    if lps.is_empty() {
        bail!("No LPs to merge");
    }
    let first_lp = &lps[0];
    for (idx, lp) in lps.iter().enumerate().skip(1) {
        if lp.props().get_is_int_handle() != first_lp.props().get_is_int_handle() {
            bail!(
                "Expected is_int_handle={} for all LPs, but found is_int_handle={} at index {} (lp_key: {}, table_id: {})",
                first_lp.props().get_is_int_handle(),
                lp.props().get_is_int_handle(),
                idx,
                hexhex::hex(lp.lp_key().as_ref()),
                lp.props().get_table_id()
            );
        }
        if lp.props().get_table_id() != first_lp.props().get_table_id() {
            bail!(
                "Expected table_id={} for all LPs, but found table_id={} at index {} (lp_key: {})",
                first_lp.props().get_table_id(),
                lp.props().get_table_id(),
                idx,
                hexhex::hex(lp.lp_key().as_ref())
            );
        }
        if lp.lp_key() != first_lp.lp_key() {
            bail!(
                "Expected lp_key={} for all LPs, but found lp_key={} at index {} (table_id: {})",
                hexhex::hex(first_lp.lp_key().as_ref()),
                hexhex::hex(lp.lp_key().as_ref()),
                idx,
                lp.props().get_table_id()
            );
        }
    }
    Ok(())
}

/// Validates a PackedFile is valid to be included in the level.
fn check_new_packed(summary: &PackedFileBuildSummary) -> Result<()> {
    if summary.props.pk_total == 0 {
        bail!("Unexpected empty PackedFile generated during merge");
    }
    if summary.props.smallest_key.is_empty() || summary.props.biggest_key.is_empty() {
        bail!("Unexpected empty smallest/largest row key in the props");
    }
    if summary.props.smallest_lp_key.is_empty() || summary.props.largest_lp_key.is_empty() {
        bail!("Unexpected empty smallest/largest LP key in the props");
    }
    Ok(())
}

/// Merge multiple LPs into a PackedFile.
fn write_lp_to_packed<W: std::io::Write>(
    builder: &mut PackedFileBuilder<W>,
    lps: &[EPackedFileLp],
    merge_mapping: &MergeMapping,
) -> Result<()> {
    let first_lp = &lps[0];
    builder.start_lp(
        first_lp.props().get_table_id(),
        first_lp.props().get_index_id(),
        first_lp.props().get_is_int_handle(),
        first_lp.lp_key(),
    )?;

    #[inline]
    fn inner<W: std::io::Write, Pk: PkType>(
        builder: &mut PackedFileBuilder<W>,
        lps: &[EPackedFileLp],
        merge_mapping: &MergeMapping,
    ) -> Result<()> {
        for mapping in &merge_mapping.doc_mapping {
            let lp = lps[mapping.segment_ord as usize].as_pk::<Pk>()?;
            let pk_encoded = lp.encoded_pk_at(mapping.doc_id as usize);
            let version = lp.version_at(mapping.doc_id as usize);
            let is_deleted = lp.is_deleted_at(mapping.doc_id as usize);
            let pk = Pk::decode(pk_encoded.as_ref())?;
            builder.add_pk::<Pk>(pk, version, is_deleted != 0)?;
        }
        Ok(())
    }

    match first_lp {
        EPackedFileLp::Int(_) => inner::<_, IntPk>(builder, lps, merge_mapping)?,
        EPackedFileLp::Common(_) => inner::<_, CommonPk>(builder, lps, merge_mapping)?,
    }

    let indexes = lps
        .iter()
        .map(|lp| Ok(lp.read_tantivy_index()?.tantivy_index().clone()))
        .collect::<Result<Vec<_>>>()?;
    let dir = merge_tantivy_indexes(&indexes, merge_mapping)?;
    builder.finish_lp(&dir)?;
    Ok(())
}

/// Merge multiple LPs into a new DedicatedFile.
fn write_lp_to_dedicated(
    lps: &[EPackedFileLp],
    merge_mapping: &MergeMapping,
    opt: DedicatedFileBuilderOptions,
) -> Result<(Bytes, DedicatedFileBuildSummary)> {
    if lps[0].props().get_is_int_handle() {
        return inner::<IntPk>(lps, merge_mapping, opt);
    } else {
        return inner::<CommonPk>(lps, merge_mapping, opt);
    }

    #[inline]
    fn inner<Pk: PkType>(
        lps: &[EPackedFileLp],
        merge_mapping: &MergeMapping,
        opt: DedicatedFileBuilderOptions,
    ) -> Result<(Bytes, DedicatedFileBuildSummary)> {
        let first_lp = &lps[0];
        let lp_key = first_lp.lp_key();
        let table_id = first_lp.props().get_table_id();
        let index_id = first_lp.props().get_index_id();
        let mut buffer = Vec::new();

        let mut builder: DedicatedFileBuilder<_, Pk> = DedicatedFileBuilder::new(
            Cursor::new(&mut buffer),
            opt,
            table_id,
            index_id,
            lp_key.as_ref(),
        )?;
        for mapping in &merge_mapping.doc_mapping {
            let lp = lps[mapping.segment_ord as usize].as_pk::<Pk>()?;
            let pk_encoded = lp.encoded_pk_at(mapping.doc_id as usize);
            let version = lp.version_at(mapping.doc_id as usize);
            let is_deleted = lp.is_deleted_at(mapping.doc_id as usize);
            let pk = Pk::decode(pk_encoded.as_ref())?;
            builder.add_pk(pk, version, is_deleted != 0)?;
        }
        let indexes = lps
            .iter()
            .map(|lp| Ok(lp.read_tantivy_index()?.tantivy_index().clone()))
            .collect::<Result<Vec<_>>>()?;
        let dir = merge_tantivy_indexes(&indexes, merge_mapping)?;
        let summary = builder.finish(&dir)?;
        Ok((Bytes::from(buffer), summary))
    }
}
