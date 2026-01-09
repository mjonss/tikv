// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
};

use anyhow::{bail, Result};
use bytes::Bytes;
use clara_fts::TrackedDirectory;
use tantivy::{
    directory::{DirectoryClone, OwnedBytes, RamDirectory},
    fastfield::AliveBitSet,
};

use crate::table::fts::iter::OrderedPkIterator;

/// Mapping describing how documents from source Tantivy segments should be
/// rewritten into the target index.
pub struct MergeMapping {
    pub doc_mapping: Vec<tantivy::DocAddress>,
    pub src_alive_bitsets: Vec<Option<Vec<u8>>>,
}

/// Compute the merge mapping by determining the order of PKs across
/// all input iterators. Returns a list of mappings indicating which iterator
/// and which PK index each merged PK comes from.
pub async fn compute_merge_mapping<Iter>(
    mut lp_iters: Vec<Iter>,
    lp_n_docs: &[u32],
    safe_ts: u64,
) -> Result<MergeMapping>
where
    Iter: OrderedPkIterator,
{
    /// Item for PK-level min-heap, ordered by PK asc, version desc.
    struct PkHeapItem {
        pk_encoded: Bytes,
        version: u64,
        is_deleted: bool,
        addr: tantivy::DocAddress,
    }
    impl PartialEq for PkHeapItem {
        fn eq(&self, other: &Self) -> bool {
            self.pk_encoded == other.pk_encoded && self.version == other.version
        }
    }
    impl Eq for PkHeapItem {}
    impl PartialOrd for PkHeapItem {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for PkHeapItem {
        fn cmp(&self, other: &Self) -> Ordering {
            // Order by PK ascending, version descending
            self.pk_encoded
                .cmp(&other.pk_encoded)
                .then_with(|| other.version.cmp(&self.version))
        }
    }

    let lp_len = lp_iters.len();
    if lp_n_docs.len() != lp_len {
        bail!(
            "Mismatch between iterators len ({}) and segment metadata len ({})",
            lp_len,
            lp_n_docs.len()
        );
    }

    if lp_iters.is_empty() {
        return Ok(MergeMapping {
            doc_mapping: Vec::new(),
            src_alive_bitsets: Vec::new(),
        });
    }

    // safe_ts cleaning rule:
    // For the first version < safe_ts, it should be kept if not deleted.
    // For the remaining versions < safe_ts, they should be pruned.
    // If the first version < safe_ts is deleted, all should be pruned.
    enum TailState {
        NeedLowerVersion,
        PruneRemaining,
    }

    let mut current_pk: Option<Bytes> = None;
    let mut tail_state = TailState::NeedLowerVersion;

    let mut doc_mapping = Vec::new();
    let mut alive_bitsets: Vec<Option<tantivy::common::BitSet>> = vec![None; lp_len];
    let mut heap: BinaryHeap<Reverse<PkHeapItem>> = BinaryHeap::new();

    // Initialize heap with first item from each iterator
    for (ord, iter) in lp_iters.iter_mut().enumerate() {
        if let Some((doc_id, pk_encoded, version, is_deleted)) = iter.next().await? {
            heap.push(Reverse(PkHeapItem {
                pk_encoded,
                version,
                is_deleted: is_deleted != 0,
                addr: tantivy::DocAddress {
                    segment_ord: ord as u32,
                    doc_id,
                },
            }));
        }
    }

    // K-way merge using BinaryHeap
    while let Some(item) = heap.pop() {
        let PkHeapItem {
            pk_encoded,
            version,
            is_deleted,
            addr,
        } = item.0;

        if current_pk
            .as_ref()
            .map_or(true, |pk| pk.as_ref() != pk_encoded.as_ref())
        {
            current_pk = Some(pk_encoded.clone());
            tail_state = TailState::NeedLowerVersion;
        }

        let seg_idx = addr.segment_ord as usize;
        let max_doc = lp_n_docs[seg_idx];
        if addr.doc_id >= max_doc {
            bail!(
                "Doc mapping references invalid doc {} >= max_doc {} for segment {}",
                addr.doc_id,
                max_doc,
                seg_idx
            );
        }

        let mut keep_doc = true;
        if version < safe_ts {
            match tail_state {
                TailState::NeedLowerVersion => {
                    tail_state = TailState::PruneRemaining;
                    keep_doc = !is_deleted;
                }
                TailState::PruneRemaining => {
                    keep_doc = false;
                }
            }
        }

        if keep_doc {
            doc_mapping.push(addr);
        } else {
            let bitset = alive_bitsets[seg_idx]
                .get_or_insert_with(|| tantivy::common::BitSet::with_max_value_and_full(max_doc));
            bitset.remove(addr.doc_id);
        }

        if let Some((next_doc_id, next_pk_encoded, next_version, is_deleted)) =
            lp_iters[addr.segment_ord as usize].next().await?
        {
            let next_item = PkHeapItem {
                pk_encoded: next_pk_encoded,
                version: next_version,
                is_deleted: is_deleted != 0,
                addr: tantivy::DocAddress {
                    segment_ord: addr.segment_ord,
                    doc_id: next_doc_id,
                },
            };
            heap.push(Reverse(next_item));
        }
    }

    let mut alive_bitset_bytes = Vec::with_capacity(lp_len);
    for idx in 0..lp_len {
        if let Some(bitset) = alive_bitsets[idx].take() {
            let mut buffer = Vec::new();
            bitset.serialize(&mut buffer)?;
            alive_bitset_bytes.push(Some(buffer));
        } else {
            alive_bitset_bytes.push(None);
        }
    }

    Ok(MergeMapping {
        doc_mapping,
        src_alive_bitsets: alive_bitset_bytes,
    })
}

/// Merge one or more Tantivy indexes into a single in-memory directory using
/// the provided mapping and optional alive bitsets.
pub fn merge_tantivy_indexes(
    indexes: &[tantivy::Index],
    merge_mapping: &MergeMapping,
) -> Result<TrackedDirectory<RamDirectory>> {
    if indexes.is_empty() {
        bail!("No Tantivy indexes to merge");
    }

    for (ord, index) in indexes.iter().enumerate().skip(1) {
        if index.schema() != indexes[0].schema() {
            bail!(
                "Found mismatched Tantivy index schemas during merge at #{}",
                ord
            );
        }
        if index.settings() != indexes[0].settings() {
            bail!(
                "Found mismatched Tantivy index settings during merge at #{}",
                ord
            );
        }
    }

    let all_segments = indexes
        .iter()
        .enumerate()
        .map(|(ord, index)| {
            let segments = index.searchable_segments()?;
            if segments.len() != 1 {
                bail!(
                    "Expected exactly one segment for index #{}, got {}",
                    ord,
                    segments.len()
                );
            }
            Ok(segments.into_iter().next().unwrap())
        })
        .collect::<Result<Vec<_>>>()?;

    let segment_count = all_segments.len();
    if merge_mapping.src_alive_bitsets.len() != segment_count {
        bail!(
            "Merge mapping segment metadata mismatch: expected {}, got {} alive sets",
            segment_count,
            merge_mapping.src_alive_bitsets.len()
        );
    }

    let mut src_alive_sets = Vec::with_capacity(segment_count);
    for (idx, (bytes_opt, seg_meta)) in merge_mapping
        .src_alive_bitsets
        .iter()
        .zip(all_segments.iter())
        .enumerate()
    {
        if let Some(bytes) = bytes_opt {
            let abs = AliveBitSet::open(OwnedBytes::new(bytes.clone()));
            let expected = seg_meta.meta().max_doc();
            // Alive bitsets must exactly match the source segment's max_doc.
            // A mismatch creates a corrupted merged index (doc ids shift or
            // vanish), so we reject early.
            if abs.bitset().max_value() != expected {
                bail!(
                    "AliveBitSet max_doc mismatch for segment {}: expected {}, got {}",
                    idx,
                    expected,
                    abs.bitset().max_value()
                );
            }
            src_alive_sets.push(Some(abs));
        } else {
            src_alive_sets.push(None);
        }
    }

    let doc_id_mapping = tantivy::indexer::doc_id_mapping::SegmentDocIdMapping::new(
        merge_mapping.doc_mapping.clone(),
        tantivy::indexer::doc_id_mapping::MappingType::Shuffled,
        vec![None; segment_count],
    );

    // TODO: Use mmap directory to avoid OOM?
    let dir = TrackedDirectory::wrap(RamDirectory::default());

    let target_schema = indexes[0].schema();
    let target_settings = indexes[0].settings().clone();

    let merged_index = tantivy::Index::create(
        dir.box_clone(),
        target_schema.clone(),
        target_settings.clone(),
    )?;
    let merged_segment = merged_index.new_segment();
    let merger = tantivy::indexer::IndexMerger::open_with_custom_alive_set(
        merged_index.schema(),
        merged_index.settings().clone(),
        &all_segments,
        src_alive_sets,
    )?;
    let segment_serializer =
        tantivy::indexer::SegmentSerializer::for_segment(merged_segment.clone(), true)?;
    let num_docs = merger.write_with_custom_mapping(segment_serializer, doc_id_mapping)?;

    let segment_meta = merged_index.new_segment_meta(merged_segment.id(), num_docs);
    let index_meta = tantivy::IndexMeta {
        index_settings: target_settings,
        segments: vec![segment_meta],
        schema: target_schema,
        opstamp: 0u64,
        payload: None,
    };

    save_metas(&index_meta, merged_index.directory())?;
    drop(merged_index);
    Ok(dir)
}

/// Rewrite a single-segment Tantivy index, optionally applying an alive bitset
/// to drop documents. This is faster than using a custom doc mapping.
pub fn rewrite_single_index(
    index: tantivy::Index,
    alive_bitset_bytes: Option<Vec<u8>>,
) -> Result<TrackedDirectory<RamDirectory>> {
    let segments = index.searchable_segments()?;
    if segments.len() != 1 {
        bail!("Expected exactly one segment, got {}", segments.len());
    }
    let segment = segments.into_iter().next().unwrap();
    let alive_opt = if let Some(bytes) = alive_bitset_bytes {
        let abs = AliveBitSet::open(OwnedBytes::new(bytes));
        let expected = segment.meta().max_doc();
        if abs.bitset().max_value() != expected {
            bail!(
                "AliveBitSet max_doc mismatch for segment 0: expected {}, got {}",
                expected,
                abs.bitset().max_value()
            );
        }
        vec![Some(abs)]
    } else {
        vec![None]
    };

    // TODO: Use mmap directory to avoid OOM?
    let dir = TrackedDirectory::wrap(RamDirectory::default());
    let merged_index =
        tantivy::Index::create(dir.box_clone(), index.schema(), index.settings().clone())?;
    let merged_segment = merged_index.new_segment();
    let merger = tantivy::indexer::IndexMerger::open_with_custom_alive_set(
        merged_index.schema(),
        merged_index.settings().clone(),
        &[segment],
        alive_opt,
    )?;
    let segment_serializer =
        tantivy::indexer::SegmentSerializer::for_segment(merged_segment.clone(), true)?;
    let num_docs = merger.write(segment_serializer)?;
    let segment_meta = merged_index.new_segment_meta(merged_segment.id(), num_docs);
    let index_meta = tantivy::IndexMeta {
        index_settings: merged_index.settings().clone(),
        segments: vec![segment_meta],
        schema: merged_index.schema(),
        opstamp: 0u64,
        payload: None,
    };

    save_metas(&index_meta, merged_index.directory())?;
    drop(merged_index);
    Ok(dir)
}

/// Ref:
/// https://github.com/quickwit-oss/tantivy/blob/60225bdd459dd3fd68cd5b1f70abb77ef8f92d71/src/indexer/segment_updater.rs#L38C1-L53C2
fn save_metas(metas: &tantivy::IndexMeta, directory: &dyn tantivy::Directory) -> Result<()> {
    use std::io::Write;
    let mut buffer = serde_json::to_vec_pretty(metas)?;
    writeln!(&mut buffer)?;
    directory.sync_directory()?;
    directory.atomic_write(std::path::Path::new("meta.json"), &buffer[..])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tantivy::{common::BitSet, DocAddress, DocSet};

    use super::*;

    fn build_index(docs: &[&str]) -> Result<tantivy::Index> {
        clara_fts::index_for_test(docs)?.finalize()
    }

    fn alive_bitset(max_doc: u32, keep: &[u32]) -> Option<Vec<u8>> {
        if keep.len() as u32 == max_doc {
            return None;
        }
        let mut keep_flags = vec![false; max_doc as usize];
        for &doc in keep {
            keep_flags[doc as usize] = true;
        }
        let mut bitset = BitSet::with_max_value_and_full(max_doc);
        for (doc_id, &is_kept) in keep_flags.iter().enumerate() {
            if !is_kept {
                bitset.remove(doc_id as u32);
            }
        }
        let mut buf = Vec::new();
        bitset.serialize(&mut buf).unwrap();
        Some(buf)
    }

    fn doc_texts(index: &tantivy::Index) -> Result<Vec<String>> {
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let field_body = index.schema().get_field("body")?;
        let segment_readers = searcher.segment_readers();
        if segment_readers.len() != 1 {
            bail!("expected 1 segment, got {}", segment_readers.len());
        }

        // We intentionally do NOT rely on Tantivy "stored fields".
        // Production FTS indexes only need postings / doc ids, not the original text.
        //
        // Test convention here: each doc is a single unique token (e.g. "a0"),
        // so we can reconstruct "doc texts" by inverting term -> postings(doc_id).
        let inv = segment_readers[0].inverted_index(field_body)?;
        let mut out: Vec<Option<String>> = vec![None; searcher.num_docs() as usize];
        let mut term_stream = inv.terms().stream()?;
        while term_stream.advance() {
            let term_bytes = term_stream.key();
            let term_str = String::from_utf8_lossy(term_bytes).to_string();
            let term = tantivy::Term::from_field_bytes(field_body, term_bytes);
            let Some(mut postings) =
                inv.read_postings(&term, tantivy::schema::IndexRecordOption::Basic)?
            else {
                continue;
            };
            while postings.doc() != tantivy::TERMINATED {
                let doc_id = postings.doc() as usize;
                match &out[doc_id] {
                    None => out[doc_id] = Some(term_str.clone()),
                    Some(prev) => bail!(
                        "doc {} unexpectedly matched multiple terms: {:?} and {:?}",
                        doc_id,
                        prev,
                        term_str
                    ),
                }
                postings.advance();
            }
        }

        out.into_iter()
            .enumerate()
            .map(|(doc_id, v)| v.ok_or_else(|| anyhow::anyhow!("missing term for doc {}", doc_id)))
            .collect()
    }

    #[test]
    fn merge_single_index_with_deletions() -> Result<()> {
        let index = build_index(&["a", "b", "c"])?;
        let mapping = MergeMapping {
            doc_mapping: vec![
                DocAddress::new(0, 0), //
                DocAddress::new(0, 2),
            ],
            src_alive_bitsets: vec![alive_bitset(3, &[0, 2])],
        };
        let dir = merge_tantivy_indexes(&[index], &mapping)?;
        let merged_index = tantivy::Index::open(dir)?;
        assert_eq!(doc_texts(&merged_index)?, vec!["a", "c"]);
        Ok(())
    }

    #[test]
    fn merge_multiple_indexes_with_deletions() -> Result<()> {
        let index1 = build_index(&["a", "b"])?;
        let index2 = build_index(&["c", "d"])?;
        let mapping = MergeMapping {
            doc_mapping: vec![
                DocAddress::new(0, 1), //
                DocAddress::new(1, 1),
            ],
            src_alive_bitsets: vec![alive_bitset(2, &[1]), alive_bitset(2, &[1])],
        };
        let dir = merge_tantivy_indexes(&[index1, index2], &mapping)?;
        let merged_index = tantivy::Index::open(dir)?;
        assert_eq!(doc_texts(&merged_index)?, vec!["b", "d"]);
        Ok(())
    }

    #[test]
    fn merge_alive_bitset_len_mismatch_too_large_rejected() -> Result<()> {
        let index = build_index(&["a", "b"])?;
        let max_doc = index
            .searchable_segments()?
            .first()
            .unwrap()
            .meta()
            .max_doc();
        assert_eq!(max_doc, 2);
        let mapping = MergeMapping {
            doc_mapping: vec![DocAddress::new(0, 0)],
            // Bitset claims larger max_doc than segment.
            src_alive_bitsets: vec![alive_bitset(max_doc + 1, &[0])],
        };
        merge_tantivy_indexes(&[index], &mapping).unwrap_err();
        Ok(())
    }

    #[test]
    fn merge_alive_bitset_len_mismatch_too_small_rejected() -> Result<()> {
        let index = build_index(&["a", "b", "c"])?;
        let max_doc = index
            .searchable_segments()?
            .first()
            .unwrap()
            .meta()
            .max_doc();
        assert_eq!(max_doc, 3);
        let alive = alive_bitset(max_doc - 1, &[0]).expect("alive bitset must exist");
        let abs = AliveBitSet::open(OwnedBytes::new(alive.clone()));
        assert_ne!(abs.bitset().max_value(), max_doc);
        let mapping = MergeMapping {
            doc_mapping: vec![DocAddress::new(0, 0)],
            // Bitset claims smaller max_doc than segment.
            src_alive_bitsets: vec![Some(alive)],
        };
        merge_tantivy_indexes(&[index], &mapping).unwrap_err();
        Ok(())
    }

    #[test]
    fn merge_complex_interleaved_partial_and_removed_sources() -> Result<()> {
        // A kept fully, B kept partially (only doc1), C fully removed.
        let index_a = build_index(&["a0", "a1"])?;
        let index_b = build_index(&["b0", "b1"])?;
        let index_c = build_index(&["c0"])?;
        let mapping = MergeMapping {
            doc_mapping: vec![
                DocAddress::new(0, 0), // a0
                DocAddress::new(1, 1), // b1
                DocAddress::new(0, 1), // a1
            ],
            src_alive_bitsets: vec![
                None,                  // A preserved
                alive_bitset(2, &[1]), // B keep only doc1
                alive_bitset(1, &[]),  // C remove all
            ],
        };
        let dir = merge_tantivy_indexes(&[index_a, index_b, index_c], &mapping)?;
        let merged_index = tantivy::Index::open(dir)?;
        assert_eq!(doc_texts(&merged_index)?, vec!["a0", "b1", "a1"]);
        Ok(())
    }

    #[test]
    fn merge_all_docs_removed() -> Result<()> {
        let index = build_index(&["x", "y"])?;
        let mapping = MergeMapping {
            doc_mapping: vec![],
            src_alive_bitsets: vec![alive_bitset(2, &[])],
        };
        let dir = merge_tantivy_indexes(&[index], &mapping)?;
        let merged_index = tantivy::Index::open(dir)?;
        let reader = merged_index.reader()?;
        assert_eq!(reader.searcher().num_docs(), 0);
        Ok(())
    }

    #[test]
    fn merge_all_docs_preserved_single_index() -> Result<()> {
        let index = build_index(&["p", "q"])?;
        let mapping = MergeMapping {
            doc_mapping: vec![DocAddress::new(0, 0), DocAddress::new(0, 1)],
            src_alive_bitsets: vec![None],
        };
        let dir = merge_tantivy_indexes(&[index], &mapping)?;
        let merged_index = tantivy::Index::open(dir)?;
        assert_eq!(doc_texts(&merged_index)?, vec!["p", "q"]);
        Ok(())
    }

    #[test]
    fn merge_all_docs_preserved_interleaved_multiple_indexes() -> Result<()> {
        let index1 = build_index(&["a", "b"])?;
        let index2 = build_index(&["c", "d"])?;
        let mapping = MergeMapping {
            doc_mapping: vec![
                DocAddress::new(0, 0), // a
                DocAddress::new(1, 0), // c
                DocAddress::new(0, 1), // b
                DocAddress::new(1, 1), // d
            ],
            src_alive_bitsets: vec![None, None],
        };
        let dir = merge_tantivy_indexes(&[index1, index2], &mapping)?;
        let merged_index = tantivy::Index::open(dir)?;
        assert_eq!(doc_texts(&merged_index)?, vec!["a", "c", "b", "d"]);
        Ok(())
    }

    #[test]
    fn merge_multi_segment_index_rejected() -> Result<()> {
        let mut schema_builder = tantivy::schema::Schema::builder();
        let field_body = schema_builder.add_text_field("body", tantivy::schema::TEXT);
        let schema = schema_builder.build();
        let index = tantivy::Index::create_in_ram(schema);
        let mut writer = index.writer(50_000_000)?;
        writer.set_merge_policy(Box::new(tantivy::indexer::NoMergePolicy));
        writer.add_document(tantivy::doc!(field_body => "a"))?;
        writer.commit()?;
        writer.add_document(tantivy::doc!(field_body => "b"))?;
        writer.commit()?;

        // merge_tantivy_indexes currently assumes each Tantivy index contains exactly
        // one segment. A multi-segment index must be rejected to avoid silently
        // dropping data.
        let mapping = MergeMapping {
            doc_mapping: Vec::new(),
            src_alive_bitsets: vec![None],
        };
        merge_tantivy_indexes(&[index], &mapping).unwrap_err();
        Ok(())
    }

    #[test]
    fn rewrite_single_index_drops_docs_with_alive() -> Result<()> {
        let index = build_index(&["keep0", "drop1", "keep2"])?;
        let alive = alive_bitset(3, &[0, 2]).expect("alive");
        let dir = rewrite_single_index(index, Some(alive))?;
        let merged_index = tantivy::Index::open(dir)?;
        assert_eq!(doc_texts(&merged_index)?, vec!["keep0", "keep2"]);
        Ok(())
    }

    #[test]
    fn rewrite_single_index_no_filter_passthrough() -> Result<()> {
        let index = build_index(&["x", "y"])?;
        let dir = rewrite_single_index(index, None)?;
        let merged_index = tantivy::Index::open(dir)?;
        assert_eq!(doc_texts(&merged_index)?, vec!["x", "y"]);
        Ok(())
    }

    #[test]
    fn rewrite_single_index_all_docs_removed() -> Result<()> {
        let index = build_index(&["gone0", "gone1"])?;
        let alive = alive_bitset(2, &[]).expect("alive");
        let dir = rewrite_single_index(index, Some(alive))?;
        let merged_index = tantivy::Index::open(dir)?;
        let reader = merged_index.reader()?;
        assert_eq!(reader.searcher().num_docs(), 0);
        Ok(())
    }

    #[test]
    fn rewrite_single_index_alive_bitset_mismatch_rejected() -> Result<()> {
        let index = build_index(&["bad0", "bad1"])?;
        // Alive bitset claims 3 docs but segment has 2.
        let alive = alive_bitset(3, &[0]).expect("alive");
        rewrite_single_index(index, Some(alive)).unwrap_err();
        Ok(())
    }
}
