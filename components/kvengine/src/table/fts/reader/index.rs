// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::Reverse, marker::PhantomData, sync::Arc};

use anyhow::Result;
use bytes::Bytes;
use clara_fts::{IndexReader as ClaraIndexReader, Query};
use futures::stream::{FuturesUnordered, TryStreamExt};
use tidb_query_datatype::codec::table::{
    ID_LEN, PREFIX_LEN, RECORD_PREFIX_SEP, TABLE_PREFIX, TABLE_PREFIX_LEN,
};

use crate::table::fts::{
    compact::lp_key,
    dedicated_file::{DedicatedFile, EDedicatedFile},
    delta_cache::FtsDeltaSource,
    iter::{PkReader, PkType},
    level::FtsLevels,
    packed_file::{PackedFile, PackedFileLp},
};

pub struct FtsIndexReader<Pk: PkType> {
    levels: Arc<FtsLevels>,
    table_id: i64,
    read_ts: u64,

    start_pk: Option<Bytes>, // Inclusive
    end_pk: Option<Bytes>,   // Exclusive
    query: Query,

    lp_key: [u8; 16],

    _marker: PhantomData<Pk>,
}

impl<Pk: PkType> FtsIndexReader<Pk> {
    pub fn new(
        levels: Arc<FtsLevels>,
        table_id: i64,
        read_ts: u64,
        start_pk: Option<Bytes>,
        end_pk: Option<Bytes>,
        query: Query,
    ) -> Self {
        Self {
            lp_key: lp_key(table_id, query.info().get_index_id()),
            levels,
            table_id,
            read_ts,
            start_pk,
            end_pk,
            query,

            _marker: PhantomData,
        }
    }

    /// Searches both the official FTS levels and additional in-memory delta
    /// sources.
    ///
    /// Delta sources must be immutable sources that implement the same MVCC
    /// conflict resolution semantics as packed/dedicated files (via
    /// `has_newer_version` on their packed LP metadata).
    pub async fn search(&self, delta_sources: &[FtsDeltaSource]) -> Result<Vec<Hit>> {
        let mut sources = self.collect_stable_sources().await?;
        for delta in delta_sources {
            let lp = delta.entry.lp().as_pk::<Pk>()?.clone();
            sources.push(SearchSource {
                reader: Arc::clone(delta.entry.index_reader()),
                lp: SourceLp::Packed(lp),
                level: 0,
                seq: delta.seq,
            });
        }
        if sources.is_empty() {
            return Ok(Vec::new());
        }
        sources.sort_unstable_by_key(|s| (s.level, Reverse(s.seq)));

        // Prepare global BM25 statistics so that scores from different Tantivy
        // indexes are on the same scale and can be compared directly.
        if self.query.info().get_query_type() == tipb::FtsQueryType::FtsQueryTypeWithScore
            && self.query.prepared_bm25().is_none()
            && !self.query.query_tokens().is_empty()
        {
            let mut stats = clara_fts::Bm25Stats::empty(&self.query);
            for src in &sources {
                src.reader.accumulate_bm25_stats(&self.query, &mut stats)?;
            }
            // Best-effort: ignore if another concurrent path already prepared it.
            let _ = self.query.prepare_bm25_once(stats)?;
        }

        self.collect_hits(&sources).await
    }

    /// Collect all Logical Partitions to be read.
    /// Results are ordered by (level, !seq).
    async fn collect_stable_sources(&self) -> Result<Vec<SearchSource<Pk>>> {
        let lp_key = self.lp_key.as_slice();
        let table_id = self.table_id;
        let index_id = self.query.info().get_index_id();

        let l0 = self
            .levels
            .l0()
            .iter()
            .filter(|file| self.is_packed_file_relevant(file))
            .cloned()
            .map(Source::L0);
        let l1 = self
            .find_covering_l1_files()
            .iter()
            .filter(|file| self.is_packed_file_relevant(file))
            .cloned()
            .map(Source::L1);
        let l2 = self
            .levels
            .l2()
            .get(lp_key)
            .into_iter()
            .flat_map(move |files| {
                files
                    .iter()
                    .filter(move |file| {
                        if file.props().get_table_id() != table_id
                            || file.props().get_index_id() != index_id
                        {
                            return false;
                        }
                        // If the filter range does not overlap with the file's key range, it means
                        // we can skip this file totally.
                        if let Some(start) = self.start_pk.as_deref() {
                            if let Some((f_table_id, f_pk)) =
                                decode_row_key(file.props().get_biggest_key())
                            {
                                if (table_id, start) > (f_table_id, f_pk) {
                                    return false;
                                }
                            }
                        }
                        if let Some(end) = self.end_pk.as_deref() {
                            if let Some((f_table_id, f_pk)) =
                                decode_row_key(file.props().get_smallest_key())
                            {
                                if (table_id, end) <= (f_table_id, f_pk) {
                                    return false;
                                }
                            }
                        }
                        true
                    })
                    .cloned()
                    .map(Source::L2)
            });
        let futures: FuturesUnordered<_> = l0
            .chain(l1)
            .chain(l2)
            .map(|task| Self::collect_source(task, lp_key))
            .collect();
        let sources: Vec<_> = futures
            .try_filter_map(|maybe_source| async move { Ok(maybe_source) })
            .try_collect()
            .await?;
        Ok(sources)
    }

    async fn collect_source(task: Source, lp_key: &[u8]) -> Result<Option<SearchSource<Pk>>> {
        match task {
            Source::L0(file) => {
                let Some(lp) = Self::load_packed_lp(&file, lp_key).await? else {
                    return Ok(None);
                };
                Ok(Some(SearchSource {
                    reader: lp.cached_read_index()?,
                    lp: SourceLp::Packed(lp),
                    level: 0,
                    seq: file.props().get_snap_version(),
                }))
            }
            Source::L1(file) => {
                let Some(lp) = Self::load_packed_lp(&file, lp_key).await? else {
                    return Ok(None);
                };
                Ok(Some(SearchSource {
                    reader: lp.cached_read_index()?,
                    lp: SourceLp::Packed(lp),
                    level: 1,
                    seq: file.id(),
                }))
            }
            Source::L2(file) => {
                let handle = SourceLp::Dedicated(file.as_pk::<Pk>()?.clone());
                Ok(Some(SearchSource {
                    reader: file.cached_read_index().await?,
                    lp: handle,
                    level: 2,
                    seq: file.id(),
                }))
            }
        }
    }

    async fn load_packed_lp(file: &PackedFile, lp_key: &[u8]) -> Result<Option<PackedFileLp<Pk>>> {
        let Some(raw_lp) = file.cached_get_lp(lp_key).await? else {
            return Ok(None);
        };
        Ok(Some(raw_lp.as_pk::<Pk>()?.clone()))
    }

    fn is_packed_file_relevant(&self, file: &PackedFile) -> bool {
        if !file.has_table(self.table_id)
            || !lp_covers_key(
                file.props().get_smallest_lp_key(),
                file.props().get_largest_lp_key(),
                &self.lp_key,
            )
        {
            return false;
        }
        // If the filter range does not overlap with the file's key range, it means
        // we can skip this file totally.
        if let Some(start) = self.start_pk.as_deref() {
            if let Some((f_table_id, f_pk)) = decode_row_key(file.props().get_biggest_key()) {
                if (self.table_id, start) > (f_table_id, f_pk) {
                    return false;
                }
            }
        }
        if let Some(end) = self.end_pk.as_deref() {
            if let Some((f_table_id, f_pk)) = decode_row_key(file.props().get_smallest_key()) {
                if (self.table_id, end) <= (f_table_id, f_pk) {
                    return false;
                }
            }
        }
        true
    }

    fn find_covering_l1_files(&self) -> &[PackedFile] {
        let files = self.levels.l1();
        if files.is_empty() {
            return &[];
        }
        let lp_key = self.lp_key.as_slice();
        let lower = files.partition_point(|f| f.props().get_largest_lp_key() < lp_key);
        let upper = files.partition_point(|f| f.props().get_smallest_lp_key() <= lp_key);
        if lower >= upper {
            &[]
        } else {
            &files[lower..upper]
        }
    }

    /// Collects Top K (or all, if TopK not specified) hits from all sources.
    /// The returned hits are not sorted.
    async fn collect_hits(&self, sources: &[SearchSource<Pk>]) -> Result<Vec<Hit>> {
        let top_k = super::effective_top_k(&self.query);
        let (shrink_threshold, shrink_to) = if let Some(top_k) = top_k {
            // We perform cross-source visibility check (`has_newer_versions`) after
            // collecting hits. For small K, shrinking to K*2 too early may
            // significantly reduce recall because many top-scoring hits can be
            // filtered out by the visibility check.
            //
            // Keep a soft limit with enough redundancy before the visibility check.
            const MIN_SOFT_LIMIT: usize = 1000;
            let shrink_to = std::cmp::max(MIN_SOFT_LIMIT, top_k.saturating_mul(2));
            let shrink_threshold = shrink_to.saturating_mul(2);
            (shrink_threshold, shrink_to)
        } else {
            (usize::MAX, usize::MAX)
        };

        fn shrink_top_k(hits: &mut Vec<Hit>, k: usize) {
            if k == 0 || hits.len() <= k {
                return;
            }
            let pivot = k - 1;
            hits.select_nth_unstable_by(pivot, |a, b| {
                // Higher score first => compare b vs a
                b.score.total_cmp(&a.score)
            });
            hits.truncate(k);
        }

        let mut hits = Vec::new();
        for (idx, src) in sources.iter().enumerate() {
            let results_iter = src.reader.search_new(&self.query)?;
            hits.reserve(results_iter.size_hint().0);
            for (doc_id, score) in results_iter {
                // Some fast visibility checks are performed for all possible results before
                // TopK.
                let Some((pk, version, _is_deleted)) =
                    src.lp.async_at(doc_id as usize, self.read_ts).await?
                else {
                    continue;
                };
                if let Some(start) = &self.start_pk {
                    if pk.as_ref() < start.as_ref() {
                        continue;
                    }
                }
                if let Some(end) = &self.end_pk {
                    if pk.as_ref() >= end.as_ref() {
                        continue;
                    }
                }
                hits.push(Hit {
                    source_idx: idx,
                    doc_id,
                    score,
                    pk,
                    version,
                });
            }
            // This still keeps overall O(N) time complexity while reducing peak memory.
            if hits.len() > shrink_threshold {
                shrink_top_k(&mut hits, shrink_to);
            }
        }

        // Cap the candidate set before the full visibility check to bound the number of
        // `has_newer_versions` calls.
        if hits.len() > shrink_to {
            shrink_top_k(&mut hits, shrink_to);
        }

        // Now perform a full visibility check for the candidates.
        // We do this in-place to avoid extra allocation.
        {
            let len = hits.len();
            let mut write_idx = 0;
            let mut i = 0;
            while i < len {
                let hit = &hits[i];
                let found =
                    has_newer_versions(sources, hit.source_idx, &hit.pk, hit.version, self.read_ts)
                        .await?;
                if !found {
                    if i != write_idx {
                        hits.swap(i, write_idx);
                    }
                    write_idx += 1;
                }
                i += 1;
            }
            hits.truncate(write_idx);
        }

        // Finally keep only TopK if needed.
        if let Some(top_k) = top_k {
            shrink_top_k(&mut hits, top_k);
        }

        Ok(hits)
    }
}

fn lp_covers_key(smallest: &[u8], largest: &[u8], key: &[u8]) -> bool {
    (smallest.is_empty() || smallest <= key) && (largest.is_empty() || key <= largest)
}

/// Decodes table ID and handle part from a full row key
/// (props.smallest_key/biggest_key).
#[inline]
fn decode_row_key(key: &[u8]) -> Option<(i64, &[u8])> {
    use codec::number::NumberDecoder;
    if key.len() < PREFIX_LEN {
        return None;
    }
    if &key[..TABLE_PREFIX_LEN] != TABLE_PREFIX {
        return None;
    }
    if &key[TABLE_PREFIX_LEN + ID_LEN..PREFIX_LEN] != RECORD_PREFIX_SEP {
        return None;
    }
    let table_id = (&key[TABLE_PREFIX_LEN..TABLE_PREFIX_LEN + ID_LEN])
        .read_i64()
        .unwrap(); // We have checked length above
    Some((table_id, &key[PREFIX_LEN..]))
}

/// Represents one searchable file plus metadata needed to resolve conflicts.
struct SearchSource<Pk: PkType> {
    reader: Arc<ClaraIndexReader>,
    lp: SourceLp<Pk>,

    level: u8,
    seq: u64,
}

enum Source {
    L0(PackedFile),
    L1(PackedFile),
    L2(EDedicatedFile),
}

/// Checks if there is a newer version for `(pk, ver)` in any *newer* file.
///
/// Newer files are defined as:
/// - For L0 file: any L0 file with strictly greater snap_version.
/// - For L1 file: all L0 files.
/// - For L2 file: all L0 files and any L2 file with strictly greater file_id.
///
/// The trick is that `sources` are sorted by (level, !seq), so we can
/// easily find which files are newer than the current one by just looking
/// at all files < src_idx for all levels. This is even true for L2 files,
/// because when something is found in L2 we suppose there will be no such LP in
/// L1, so checking all files < src_idx will correctly cover all L0 files and
/// all L2 files with greater file_id.
#[inline]
async fn has_newer_versions<Pk: PkType>(
    sources: &[SearchSource<Pk>],
    src_idx: usize,
    pk: &[u8],
    ver: u64,
    read_ts: u64,
) -> Result<bool> {
    for i in 0..src_idx {
        if sources[i].lp.has_newer_version(pk, ver, read_ts).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub struct Hit {
    source_idx: usize,

    pub doc_id: u32,
    pub score: f32,
    pub pk: Bytes,
    pub version: u64,
}

enum SourceLp<Pk: PkType> {
    Packed(PackedFileLp<Pk>),
    Dedicated(DedicatedFile<Pk>),
}

impl<Pk: PkType> SourceLp<Pk> {
    #[inline]
    async fn async_at(&self, doc_id: usize, read_ts: u64) -> Result<Option<(Bytes, u64, u8)>> {
        match self {
            Self::Packed(lp) => lp.async_at(doc_id, read_ts).await,
            Self::Dedicated(f) => f.async_at(doc_id, read_ts).await,
        }
    }

    #[inline]
    async fn has_newer_version(&self, pk: &[u8], ver: u64, max_ver: u64) -> Result<bool> {
        match self {
            Self::Packed(lp) => lp.has_newer_version(pk, ver, max_ver).await,
            Self::Dedicated(f) => f.has_newer_version(pk, ver, max_ver).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use anyhow::Result;
    use clara_fts::{
        index_for_test,
        test_util::{make_scored_query, make_unscored_query, PlainFtsQueryInfo},
    };

    use super::*;
    use crate::table::fts::{
        test_util::{new_ded, new_packed},
        IntPk,
    };

    const TABLE_ID: i64 = 1;
    const INDEX_ID: i64 = 2;

    fn build_levels() -> FtsLevels {
        let mut levels = FtsLevels::default();
        levels.track_index(TABLE_ID, INDEX_ID);
        levels
    }

    fn assert_hits(hits: &[Hit], expected: &[(i64, u64)]) {
        let mut actual: Vec<_> = hits
            .iter()
            .map(|hit| (IntPk::decode(hit.pk.as_ref()).unwrap(), hit.version))
            .collect();
        actual.sort_unstable();
        let mut expect = expected.to_vec();
        expect.sort_unstable();
        assert_eq!(actual, expect);
    }

    #[tokio::test]
    async fn l0_filters_versions_and_bounds() {
        let l0 = new_packed(11, 100)
            .lp(TABLE_ID, INDEX_ID, |d| {
                d(5, 7, false, "alpha");
                d(10, 9, false, "alpha");
                d(10, 4, false, "alpha");
                d(12, 20, false, "alpha");
                d(15, 8, false, "noise"); // Unrelated term should not appear in results
                d(20, 5, true, "alpha");
                d(30, 6, false, "alpha");
                d(40, 2, false, "alpha");
            })
            .finish_as_file();
        let mut levels = build_levels();
        levels.mut_l0(|files| files.push(l0));

        let mut reader = FtsIndexReader::<IntPk>::new(
            Arc::new(levels),
            TABLE_ID,
            10,
            None,
            None,
            make_scored_query(&PlainFtsQueryInfo {
                query: "alpha".into(),
                top_k: 10,
                index_id: INDEX_ID,
                ..Default::default()
            }),
        );

        let sources = reader.collect_stable_sources().await.unwrap();
        assert_eq!(sources.len(), 1);

        reader.start_pk = Some(Bytes::copy_from_slice(&IntPk::encode(10)));
        reader.end_pk = Some(Bytes::copy_from_slice(&IntPk::encode(35)));

        let hits = reader.collect_hits(&sources).await.unwrap();
        assert_eq!(hits.len(), 2);
        assert_hits(&hits, &[(10, 9), (30, 6)]);
    }

    #[tokio::test]
    async fn l0_and_l2_visibility_respects_read_ts() {
        let l0 = new_packed(21, 500)
            .lp(TABLE_ID, INDEX_ID, |d| {
                d(1, 50, false, "kappa");
                d(2, 40, false, "kappa");
                d(4, 10, false, "lambda"); // Non-matching doc
            })
            .finish_as_file();
        let l2 = new_ded(201)
            .lp(TABLE_ID, INDEX_ID, |d| {
                d(1, 30, false, "kappa");
                d(3, 35, false, "kappa");
                d(5, 15, false, "lambda"); // Non-matching doc
            })
            .finish_as_file();
        let mut levels = build_levels();
        levels.mut_l0(|files| files.push(l0));
        levels.insert_l2_file(l2);
        let levels = Arc::new(levels);

        let query = make_unscored_query(&PlainFtsQueryInfo {
            query: "kappa".into(),
            index_id: INDEX_ID,
            ..Default::default()
        });
        let fresh_reader =
            FtsIndexReader::<IntPk>::new(levels.clone(), TABLE_ID, 100, None, None, query.clone());
        let fresh_hits = fresh_reader.search(&[]).await.unwrap();
        assert_hits(&fresh_hits, &[(1, 50), (2, 40), (3, 35)]);

        let stale_reader = FtsIndexReader::<IntPk>::new(levels, TABLE_ID, 45, None, None, query);
        let stale_hits = stale_reader.search(&[]).await.unwrap();
        assert_hits(&stale_hits, &[(1, 30), (2, 40), (3, 35)]);
    }

    #[tokio::test]
    async fn l0_and_l1_deduplicate_overlaps() {
        let l0 = new_packed(31, 600)
            .lp(TABLE_ID, INDEX_ID, |d| {
                d(10, 8, false, "theta");
                d(20, 6, false, "theta");
                d(30, 4, false, "iota"); // Non-matching doc
            })
            .finish_as_file();
        let l1_active = new_packed(32, 0)
            .lp(TABLE_ID, INDEX_ID, |d| {
                d(10, 6, false, "theta");
                d(11, 5, false, "theta");
                d(25, 3, false, "theta");
                d(35, 2, false, "iota"); // Non-matching doc
            })
            .finish_as_file();
        let l1_irrelevant = new_packed(33, 0)
            .lp(TABLE_ID, INDEX_ID + 1, |d| {
                d(50, 2, false, "theta");
            })
            .finish_as_file();

        let mut levels = build_levels();
        levels.mut_l0(|files| files.push(l0));
        levels.mut_l1(|files| {
            files.push(l1_irrelevant);
            files.push(l1_active);
        });
        let reader = FtsIndexReader::<IntPk>::new(
            Arc::new(levels),
            TABLE_ID,
            20,
            None,
            None,
            make_scored_query(&PlainFtsQueryInfo {
                query: "theta".into(),
                top_k: 10,
                index_id: INDEX_ID,
                ..Default::default()
            }),
        );
        let hits = reader.search(&[]).await.unwrap();

        assert_hits(&hits, &[(10, 8), (11, 5), (20, 6), (25, 3)]);
        assert!(
            !hits
                .iter()
                .any(|hit| IntPk::decode(hit.pk.as_ref()).unwrap() == 10 && hit.version == 6),
            "older L1 version should be filtered out when L0 has a newer row"
        );
    }

    #[tokio::test]
    async fn with_score_top_k_limits_results() {
        let l0 = new_packed(41, 700)
            .lp(TABLE_ID, INDEX_ID, |d| {
                d(1, 10, false, "zeta");
                d(2, 10, false, "zeta");
                d(3, 10, false, "zeta");
                d(4, 10, false, "zeta");
                d(5, 10, false, "zeta");
                d(100, 5, false, "upsilon"); // Non-matching doc
            })
            .finish_as_file();
        let mut levels = build_levels();
        levels.mut_l0(|files| files.push(l0));
        let reader = FtsIndexReader::<IntPk>::new(
            Arc::new(levels),
            TABLE_ID,
            20,
            None,
            None,
            make_scored_query(&PlainFtsQueryInfo {
                query: "zeta".into(),
                top_k: 1,
                index_id: INDEX_ID,
                ..Default::default()
            }),
        );
        let hits = reader.search(&[]).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].score.is_finite());
    }

    #[tokio::test]
    async fn top_k_soft_limit_keeps_candidates_after_visibility_filter() {
        // Regression test: When TopK is small, shrinking to `TopK*2` before visibility
        // filtering can drop all results if the top hits are shadowed by newer
        // versions.
        let l0_new = new_packed(71, 1_000)
            .lp(TABLE_ID, INDEX_ID, |d| {
                d(1, 6, false, "noise");
                d(2, 6, false, "noise");
                d(4, 6, false, "noise");
                d(5, 6, false, "noise");
            })
            .finish_as_file();
        let l0_old = new_packed(72, 900)
            .lp(TABLE_ID, INDEX_ID, |d| {
                d(
                    1,
                    5,
                    false,
                    "alpha alpha alpha alpha alpha alpha alpha alpha alpha alpha",
                );
                d(
                    2,
                    5,
                    false,
                    "alpha alpha alpha alpha alpha alpha alpha alpha alpha",
                );
                d(3, 5, false, "alpha");
                d(4, 5, false, "alpha alpha");
                d(5, 5, false, "alpha alpha alpha");
            })
            .finish_as_file();

        let mut levels = build_levels();
        levels.mut_l0(|files| {
            files.push(l0_old);
            files.push(l0_new);
        });

        let reader = FtsIndexReader::<IntPk>::new(
            Arc::new(levels),
            TABLE_ID,
            10,
            None,
            None,
            make_scored_query(&PlainFtsQueryInfo {
                query: "alpha".into(),
                top_k: 1,
                index_id: INDEX_ID,
                ..Default::default()
            }),
        );
        let hits = reader.search(&[]).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(IntPk::decode(hits[0].pk.as_ref()).unwrap(), 3);
        assert_eq!(hits[0].version, 5);
        assert!(hits[0].score.is_finite());
    }

    #[tokio::test]
    async fn zero_top_k() {
        let l0 = new_packed(51, 800)
            .lp(TABLE_ID, INDEX_ID, |d| {
                d(1, 5, false, "eta");
                d(2, 4, false, "omega"); // Non-matching doc
            })
            .finish_as_file();
        let mut levels = build_levels();
        levels.mut_l0(|files| files.push(l0));
        let reader = FtsIndexReader::<IntPk>::new(
            Arc::new(levels),
            TABLE_ID,
            10,
            None,
            None,
            make_scored_query(&PlainFtsQueryInfo {
                query: "eta".into(),
                top_k: 0,
                index_id: INDEX_ID,
                ..Default::default()
            }),
        );
        let hits = reader.search(&[]).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(IntPk::decode(hits[0].pk.as_ref()).unwrap(), 1);
        assert_eq!(hits[0].version, 5);
        assert!(hits[0].score.is_finite());
    }

    #[test]
    fn global_bm25_scores_match_combined_index() -> Result<()> {
        // Two independent indexes should produce the same scores as a single combined
        // index when using prepared global BM25 statistics.
        let docs_a = ["alpha beta beta", "alpha alpha", "gamma"];
        let docs_b = ["alpha beta", "beta", "delta alpha"];
        let docs_all: Vec<&str> = docs_a.iter().chain(docs_b.iter()).copied().collect();

        let idx_a = index_for_test(&docs_a)?.finalize()?;
        let idx_b = index_for_test(&docs_b)?.finalize()?;
        let idx_all = index_for_test(&docs_all)?.finalize()?;

        let reader_a = clara_fts::IndexReader::from_tantivy_index(idx_a)?;
        let reader_b = clara_fts::IndexReader::from_tantivy_index(idx_b)?;
        let reader_all = clara_fts::IndexReader::from_tantivy_index(idx_all)?;

        let plain_info = PlainFtsQueryInfo {
            query: "alpha beta beta".to_string(),
            tokenizer: "STANDARD_V1".to_string(),
            ..Default::default()
        };

        // Prepare global BM25 stats from (A + B).
        let query_prepared = make_scored_query(&plain_info);
        let mut stats = clara_fts::Bm25Stats::empty(&query_prepared);
        reader_a.accumulate_bm25_stats(&query_prepared, &mut stats)?;
        reader_b.accumulate_bm25_stats(&query_prepared, &mut stats)?;
        assert!(query_prepared.prepare_bm25_once(stats)?);

        let mut results_a = Vec::new();
        reader_a.search(&query_prepared, &mut results_a)?;
        let mut results_b = Vec::new();
        reader_b.search(&query_prepared, &mut results_b)?;

        // Combined index uses its own local BM25 statistics (which are already global).
        let query_combined = make_scored_query(&plain_info);
        let mut results_all = Vec::new();
        reader_all.search(&query_combined, &mut results_all)?;

        let mut score_all: HashMap<u32, f32> = HashMap::with_capacity(results_all.len());
        for r in results_all {
            score_all.insert(r.doc_id, r.score);
        }

        assert_eq!(score_all.len(), results_a.len() + results_b.len());
        for r in results_a {
            let expected = score_all.get(&r.doc_id).unwrap();
            assert!(
                (r.score - expected).abs() < 0.001,
                "doc {} score mismatch: split={} combined={}",
                r.doc_id,
                r.score,
                expected
            );
        }

        let offset = docs_a.len() as u32;
        for r in results_b {
            let combined_doc_id = r.doc_id + offset;
            let expected = score_all.get(&combined_doc_id).unwrap();
            assert!(
                (r.score - expected).abs() < 0.001,
                "doc {} (combined {}) score mismatch: split={} combined={}",
                r.doc_id,
                combined_doc_id,
                r.score,
                expected
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn prepares_global_bm25_for_with_score_query() {
        let l0 = new_packed(61, 900)
            .lp(TABLE_ID, INDEX_ID, |d| {
                d(1, 10, false, "alpha");
                d(2, 9, false, "alpha");
            })
            .finish_as_file();
        let mut levels = build_levels();
        levels.mut_l0(|files| files.push(l0));
        let levels = Arc::new(levels);

        let query = make_scored_query(&PlainFtsQueryInfo {
            query: "alpha".into(),
            index_id: INDEX_ID,
            ..Default::default()
        });
        let reader = FtsIndexReader::<IntPk>::new(levels, TABLE_ID, 20, None, None, query.clone());
        let _ = reader.search(&[]).await.unwrap();
        assert!(
            query.prepared_bm25().is_some(),
            "FtsIndexReader should prepare global BM25 stats for WithScore queries"
        );
    }
}
