// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::{bail, Result};
use tantivy::{
    query::{Bm25StatisticsProvider, Bm25Weight},
    schema::Field,
    TantivyError, Term,
};

use crate::{Query, FIELD_BODY};

/// Accumulates BM25 statistics across multiple Tantivy indexes for a query.
///
/// This is typically used to build `PreparedBm25` and store it into `Query` so
/// that scores from different sources are comparable on the same scale.
///
/// The document count follows Tantivy's BM25 convention: `max_doc` (including
/// empty-placeholder documents), not `num_docs`.
///
/// The `doc_freqs_by_token_idx` vector is aligned to `Query::query_tokens()`'s
/// `token_index`.
#[non_exhaustive]
pub struct Bm25Stats {
    /// Total number of documents (`max_doc` in Tantivy BM25 convention).
    pub total_docs: u64,
    /// Total number of tokens in `FIELD_BODY` across all documents.
    pub total_tokens: u64,
    /// Document frequencies aligned to `Query::query_tokens()`'s `token_index`.
    pub doc_freqs_by_token_idx: Vec<u64>,
}

impl Bm25Stats {
    /// Create an empty BM25 statistics instance.
    pub fn empty(empty: &Query) -> Self {
        Self {
            total_docs: 0,
            total_tokens: 0,
            doc_freqs_by_token_idx: vec![0u64; empty.query_tokens().len()],
        }
    }
}

/// Prepared BM25 statistics and per-term weights for a specific query.
///
/// This is computed by aggregating BM25 statistics across a chosen "stats
/// corpus" (typically multiple Tantivy indexes) so that scores produced by
/// different sources are directly comparable.
///
/// The vectors are aligned to `Query::query_tokens()`'s `token_index`.
pub struct PreparedBm25Stats {
    total_docs: u64,
    total_tokens: u64,
    doc_freqs_by_token_idx: Vec<u64>,
    weights_by_token_idx: Vec<Bm25Weight>,
    /// Token indices sorted by their term order.
    ///
    /// This enables `O(log n)` `doc_freq(term)` lookup via binary search while
    /// keeping `doc_freqs_by_token_idx` aligned to `Query::query_tokens()`'s
    /// `token_index`.
    sorted_token_idx_by_term: Vec<usize>,
}

impl PreparedBm25Stats {
    pub(super) fn new(
        query_terms_by_token_idx: &[Term],
        total_docs: u64,
        total_tokens: u64,
        mut doc_freqs_by_token_idx: Vec<u64>,
    ) -> Result<Self> {
        if doc_freqs_by_token_idx.len() != query_terms_by_token_idx.len() {
            bail!(
                "doc_freqs_by_token_idx length {} does not match query terms length {}",
                doc_freqs_by_token_idx.len(),
                query_terms_by_token_idx.len()
            );
        }

        // Tantivy's BM25 assumes `total_docs > 0` and `avgdl > 0`. In practice,
        // we can observe edge cases like an empty index (0 docs) or a corpus
        // where `FIELD_BODY` has no tokens at all (`total_tokens == 0`). These
        // cases are normal and should not fail BM25 preparation.
        //
        // We sanitize the totals to keep the BM25 math numerically stable, and
        // then compute weights based on the sanitized values.
        let bm25_total_docs = total_docs.max(1);
        let bm25_total_tokens = if total_tokens == 0 {
            bm25_total_docs
        } else {
            total_tokens
        };

        for df in &mut doc_freqs_by_token_idx {
            if *df > bm25_total_docs {
                *df = bm25_total_docs;
            }
        }

        let avgdl = bm25_total_tokens as f32 / bm25_total_docs as f32;
        let mut weights_by_token_idx = Vec::with_capacity(doc_freqs_by_token_idx.len());
        for df in doc_freqs_by_token_idx.iter() {
            weights_by_token_idx.push(Bm25Weight::for_one_term_without_explain(
                *df,
                bm25_total_docs,
                avgdl,
            ));
        }

        let mut sorted_token_idx_by_term: Vec<usize> =
            (0..query_terms_by_token_idx.len()).collect();
        sorted_token_idx_by_term.sort_unstable_by(|&a, &b| {
            query_terms_by_token_idx[a].cmp(&query_terms_by_token_idx[b])
        });

        Ok(Self {
            total_docs: bm25_total_docs,
            total_tokens: bm25_total_tokens,
            doc_freqs_by_token_idx,
            weights_by_token_idx,
            sorted_token_idx_by_term,
        })
    }

    /// Precomputed BM25 weights aligned to `Query::query_tokens()` indices.
    #[inline]
    pub fn weights_by_token_idx(&self) -> &[Bm25Weight] {
        &self.weights_by_token_idx
    }
}

impl Bm25StatisticsProvider for crate::Query {
    fn total_num_tokens(&self, field: Field) -> tantivy::Result<u64> {
        if field != FIELD_BODY {
            return Err(TantivyError::InvalidArgument(
                "clara_fts only supports BM25 on FIELD_BODY".to_string(),
            ));
        }
        let prepared = self.prepared_bm25().ok_or_else(|| {
            TantivyError::InvalidArgument("BM25 statistics are not prepared".to_string())
        })?;
        Ok(prepared.total_tokens)
    }

    fn total_num_docs(&self) -> tantivy::Result<u64> {
        let prepared = self.prepared_bm25().ok_or_else(|| {
            TantivyError::InvalidArgument("BM25 statistics are not prepared".to_string())
        })?;
        Ok(prepared.total_docs)
    }

    fn doc_freq(&self, term: &Term) -> tantivy::Result<u64> {
        let prepared = self.prepared_bm25().ok_or_else(|| {
            TantivyError::InvalidArgument("BM25 statistics are not prepared".to_string())
        })?;
        if term.field() != FIELD_BODY {
            return Ok(0);
        }
        let query_terms = self.query_terms_by_token_idx();
        match prepared
            .sorted_token_idx_by_term
            .binary_search_by(|&token_idx| query_terms[token_idx].cmp(term))
        {
            Ok(pos) => {
                let token_idx = prepared.sorted_token_idx_by_term[pos];
                Ok(prepared.doc_freqs_by_token_idx[token_idx])
            }
            Err(_) => Ok(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::test_util::{make_scored_query, PlainFtsQueryInfo};

    #[test]
    fn prepare_bm25_succeeds_with_empty_corpus() -> Result<()> {
        let query = make_scored_query(&PlainFtsQueryInfo {
            query: "rust".to_string(),
            ..Default::default()
        });
        assert!(!query.query_tokens().is_empty());

        let stats = Bm25Stats::empty(&query);
        assert!(query.prepare_bm25_once(stats)?);

        let prepared = query.prepared_bm25().unwrap();
        assert_eq!(
            prepared.weights_by_token_idx().len(),
            query.query_tokens().len()
        );

        let total_docs = Bm25StatisticsProvider::total_num_docs(&query)?;
        assert!(total_docs >= 1);
        let total_tokens = Bm25StatisticsProvider::total_num_tokens(&query, FIELD_BODY)?;
        assert!(total_tokens >= total_docs);

        let mut brute = crate::BruteScoredSearcher::new(&query)?;
        let score = brute.score_document("rust rust");
        assert!(score.is_finite());
        assert!(score > 0.0);
        Ok(())
    }

    #[test]
    fn prepare_bm25_succeeds_with_empty_query() -> Result<()> {
        let query = make_scored_query(&PlainFtsQueryInfo {
            query: String::new(),
            ..Default::default()
        });
        assert!(query.query_tokens().is_empty());

        let stats = Bm25Stats::empty(&query);
        assert!(query.prepare_bm25_once(stats)?);

        let total_docs = Bm25StatisticsProvider::total_num_docs(&query)?;
        assert!(total_docs >= 1);
        let total_tokens = Bm25StatisticsProvider::total_num_tokens(&query, FIELD_BODY)?;
        assert!(total_tokens >= total_docs);

        let term = Term::from_field_text(FIELD_BODY, "rust");
        assert_eq!(Bm25StatisticsProvider::doc_freq(&query, &term)?, 0);

        let mut brute = crate::BruteScoredSearcher::new(&query)?;
        assert_eq!(brute.score_document("rust rust"), 0.0);
        Ok(())
    }
}
