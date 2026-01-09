// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{convert::TryFrom, io::Cursor};

use anyhow::{anyhow, bail, Result};
use bytes::Bytes;

use super::merge_utils::{compute_merge_mapping, merge_tantivy_indexes};
use crate::table::fts::{
    dedicated_file::{
        DedicatedFile, DedicatedFileBuilder, DedicatedFileBuilderOptions, EDedicatedFile,
    },
    iter::{CommonPk, OrderedPkIterator, PkReader, PkType},
    FtsBuildOptions, IntPk,
};

async fn advance_iter_to_doc<I: OrderedPkIterator>(
    iter: &mut I,
    target_doc: u32,
) -> Result<(Bytes, u64, u8)> {
    while let Some((doc_id, pk_bytes, version, is_deleted)) = iter.next().await? {
        if doc_id == target_doc {
            return Ok((pk_bytes, version, is_deleted));
        }
        if doc_id > target_doc {
            break;
        }
    }
    Err(anyhow!(
        "failed to reach document {} while merging dedicated files",
        target_doc
    ))
}

async fn merge_fts_l2_files_<Pk: PkType>(
    files: &[&DedicatedFile<Pk>],
    lp_n_docs: &[u32],
    tantivy_indexes: &[tantivy::Index],
    safe_ts: u64,
    builder_opt: DedicatedFileBuilderOptions,
    table_id: i64,
    lp_key: &[u8],
    index_id: i64,
) -> Result<Option<super::L2FileOutput>> {
    let mut mapping_iters = Vec::with_capacity(files.len());
    for file in files {
        mapping_iters.push(file.pk_iter()?);
    }
    let merge_mapping = compute_merge_mapping(mapping_iters, lp_n_docs, safe_ts).await?;
    if merge_mapping.doc_mapping.is_empty() {
        return Ok(None);
    }

    let mut data_iters = Vec::with_capacity(files.len());
    for file in files {
        data_iters.push(file.pk_iter()?);
    }

    let mut buffer = Vec::new();
    let summary = {
        let mut builder: DedicatedFileBuilder<_, Pk> = DedicatedFileBuilder::new(
            Cursor::new(&mut buffer),
            builder_opt,
            table_id,
            index_id,
            lp_key,
        )?;

        for mapping in &merge_mapping.doc_mapping {
            let seg_idx = mapping.segment_ord as usize;
            let (pk_bytes, version, is_deleted) =
                advance_iter_to_doc(&mut data_iters[seg_idx], mapping.doc_id).await?;
            let pk = Pk::decode(pk_bytes.as_ref())?;
            builder.add_pk(pk, version, is_deleted != 0)?;
        }

        let dir = merge_tantivy_indexes(tantivy_indexes, &merge_mapping)?;
        builder.finish(&dir)?
    };
    let data = Bytes::from(buffer);

    Ok(Some(super::L2FileOutput {
        lp_key: lp_key.to_vec(),
        data,
        summary,
    }))
}

/// Merge multiple L2 dedicated files of the same logical partition.
pub async fn merge_fts_l2_files(
    files: &[EDedicatedFile],
    safe_ts: u64,
    builder_opt: DedicatedFileBuilderOptions,
) -> Result<Option<super::L2FileOutput>> {
    if files.is_empty() {
        return Ok(None);
    }

    let props0 = files[0].props();
    for file in files {
        let props = file.props();
        if props.get_table_id() != props0.get_table_id() {
            bail!(
                "All dedicated files must belong to the same table (expect {}, got {})",
                props0.get_table_id(),
                props.get_table_id()
            );
        }
        if props.get_lp_key() != props0.get_lp_key() {
            bail!("All dedicated files must have the same logical partition key");
        }
        if props.get_is_int_handle() != props0.get_is_int_handle() {
            bail!("Mixed PK types are not supported in L2 compaction");
        }
    }

    let mut lp_n_docs = Vec::with_capacity(files.len());
    for file in files {
        let props = file.props();
        let pk_total = props.get_pk_total();
        let docs = u32::try_from(pk_total).map_err(|_| {
            anyhow!(
                "pk_total {} exceeds u32::MAX for dedicated file {}",
                pk_total,
                file.id()
            )
        })?;
        lp_n_docs.push(docs);
    }

    // Tantivy segments are addressed by `segment_ord` in `merge_mapping`, which
    // is derived from `files` iterator order. Preserve this order when loading
    // Tantivy indexes, even if individual `cached_read_index()` futures
    // complete out-of-order.
    let tantivy_indexes = futures::future::try_join_all(files.iter().map(|file| async move {
        Ok::<_, anyhow::Error>(file.cached_read_index().await?.tantivy_index().clone())
    }))
    .await?;

    if props0.get_is_int_handle() {
        let files: Vec<&DedicatedFile<IntPk>> = files
            .iter()
            .map(|file| file.as_int())
            .collect::<Result<_>>()?;
        merge_fts_l2_files_::<IntPk>(
            &files,
            &lp_n_docs,
            &tantivy_indexes,
            safe_ts,
            builder_opt,
            props0.get_table_id(),
            props0.get_lp_key(),
            props0.get_index_id(),
        )
        .await
    } else {
        let files: Vec<&DedicatedFile<CommonPk>> = files
            .iter()
            .map(|file| file.as_common())
            .collect::<Result<_>>()?;
        merge_fts_l2_files_::<CommonPk>(
            &files,
            &lp_n_docs,
            &tantivy_indexes,
            safe_ts,
            builder_opt,
            props0.get_table_id(),
            props0.get_lp_key(),
            props0.get_index_id(),
        )
        .await
    }
}

/// Metadata required for choosing an intra-L2 merge set.
#[derive(Clone, Debug, PartialEq)]
pub struct LogMergeCandidate {
    pub id: u64,
    pub pk_total: u64,
    pub size: u64,
}

/// Pick a set of L2 files (within the same LP) to merge, following a
/// simplified Tantivy LogMergePolicy style.
pub fn pick_l2_merge_files(
    files: &[LogMergeCandidate],
    opts: &FtsBuildOptions,
) -> Option<Vec<u64>> {
    if files.len() < opts.l2_min_merge_files {
        return None;
    }

    let mut files: Vec<_> = files
        .iter()
        .filter_map(|c| {
            if c.pk_total > opts.l2_max_docs_before_merge {
                return None;
            }
            if c.size > opts.l2_max_output_file_size.0 {
                return None;
            }
            let norm_docs = std::cmp::max(c.pk_total, opts.l2_min_layer_docs);
            Some((c, (norm_docs as f64).log2()))
        })
        .collect();

    if files.len() < opts.l2_min_merge_files {
        return None;
    }

    #[derive(Debug)]
    struct Plan {
        file_ids: Vec<u64>,
        merge_len: usize,
        total_size: u64,
    }

    // Sort descending by docs, tie by older id for determinism.
    files.sort_by(|a, b| {
        b.0.pk_total
            .cmp(&a.0.pk_total)
            .then_with(|| a.0.id.cmp(&b.0.id))
    });

    // Build levels dynamically: start a new level when log size drops by more than
    // l2_level_log_size.
    let mut levels = Vec::new();
    let mut current_max_log = f64::MAX;
    for f in files {
        let log = f.1;
        if levels.is_empty() || log < current_max_log - opts.l2_level_log_size {
            current_max_log = log;
            levels.push(Vec::new());
        }
        levels.last_mut().unwrap().push(f);
    }

    let mut best: Option<Plan> = None;

    for level in levels.into_iter() {
        if level.len() < opts.l2_min_merge_files {
            continue;
        }

        let mut level_by_size = level;
        level_by_size.sort_by(|a, b| match a.0.size.cmp(&b.0.size) {
            std::cmp::Ordering::Equal => a.0.id.cmp(&b.0.id),
            other => other,
        });

        let mut total_size = 0u64;
        let mut file_ids = Vec::new();
        let output_limit = opts.l2_max_output_file_size.0;

        for f in &level_by_size {
            if output_limit > 0 && total_size + f.0.size > output_limit {
                break;
            }
            total_size += f.0.size;
            file_ids.push(f.0.id);
        }

        if file_ids.len() < opts.l2_min_merge_files {
            continue;
        }

        let merge_len = file_ids.len();
        let candidate = Plan {
            file_ids,
            merge_len,
            total_size,
        };

        let replace = match &best {
            None => true,
            Some(prev) => {
                candidate.merge_len > prev.merge_len
                    || (candidate.merge_len == prev.merge_len
                        && candidate.total_size > prev.total_size)
            }
        };
        if replace {
            best = Some(candidate);
        }
    }

    best.map(|cand| cand.file_ids)
}

#[cfg(test)]
mod tests {
    use super::{pick_l2_merge_files, LogMergeCandidate};
    use crate::table::fts::FtsBuildOptions;

    fn ids_vec(ids: &[u64]) -> Vec<u64> {
        ids.to_vec()
    }

    #[test]
    fn merge_small_segments_bucketed_together() {
        let opts = FtsBuildOptions {
            l2_min_merge_files: 3,
            ..Default::default()
        };
        let files = vec![
            LogMergeCandidate {
                id: 1,
                pk_total: 1_000,
                size: 10,
            },
            LogMergeCandidate {
                id: 2,
                pk_total: 2_000,
                size: 20,
            },
            LogMergeCandidate {
                id: 3,
                pk_total: 3_000,
                size: 30,
            },
        ];

        let picked = pick_l2_merge_files(&files, &opts).unwrap();
        assert_eq!(ids_vec(&picked), ids_vec(&[1, 2, 3]));
    }

    #[test]
    fn prefer_larger_level_when_merge_len_ties() {
        let opts = FtsBuildOptions {
            l2_min_merge_files: 3,
            ..Default::default()
        };
        // Level 0 (larger docs, more segments)
        let mut files = vec![
            LogMergeCandidate {
                id: 10,
                pk_total: 1_000_000,
                size: 100,
            },
            LogMergeCandidate {
                id: 11,
                pk_total: 900_000,
                size: 90,
            },
            LogMergeCandidate {
                id: 12,
                pk_total: 800_000,
                size: 80,
            },
            LogMergeCandidate {
                id: 13,
                pk_total: 750_000,
                size: 70,
            },
        ];
        // Level 1 (smaller docs, fewer segments) should not win because merge_len
        // smaller
        files.extend(vec![
            LogMergeCandidate {
                id: 20,
                pk_total: 50_000,
                size: 30,
            },
            LogMergeCandidate {
                id: 21,
                pk_total: 40_000,
                size: 25,
            },
            LogMergeCandidate {
                id: 22,
                pk_total: 30_000,
                size: 20,
            },
        ]);

        let picked = pick_l2_merge_files(&files, &opts).unwrap();
        // Should pick every file in the larger level because there is no merge cap.
        assert_eq!(ids_vec(&picked), ids_vec(&[13, 12, 11, 10]));
    }

    #[test]
    fn respects_output_size_cap_by_shrinking_width() {
        let opts = FtsBuildOptions {
            l2_min_merge_files: 2,
            l2_max_output_file_size: tikv_util::config::ReadableSize(50),
            ..Default::default()
        };

        let files = vec![
            LogMergeCandidate {
                id: 1,
                pk_total: 100_000,
                size: 30,
            },
            LogMergeCandidate {
                id: 2,
                pk_total: 90_000,
                size: 25,
            },
            LogMergeCandidate {
                id: 3,
                pk_total: 80_000,
                size: 10,
            },
        ];

        // Sum of smallest 3 is 65 > 50, but dropping one makes 40.
        let picked = pick_l2_merge_files(&files, &opts).unwrap();
        assert_eq!(ids_vec(&picked), ids_vec(&[3, 2]));
    }

    #[test]
    fn merges_full_level_when_no_size_cap() {
        let opts = FtsBuildOptions {
            l2_min_merge_files: 2,
            ..Default::default()
        };
        let files = vec![
            LogMergeCandidate {
                id: 1,
                pk_total: 50_000,
                size: 10,
            },
            LogMergeCandidate {
                id: 2,
                pk_total: 45_000,
                size: 15,
            },
            LogMergeCandidate {
                id: 3,
                pk_total: 40_000,
                size: 20,
            },
            LogMergeCandidate {
                id: 4,
                pk_total: 35_000,
                size: 25,
            },
        ];

        let picked = pick_l2_merge_files(&files, &opts).unwrap();
        assert_eq!(ids_vec(&picked), ids_vec(&[1, 2, 3, 4]));
    }
}
