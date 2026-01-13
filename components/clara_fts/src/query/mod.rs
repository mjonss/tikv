// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, OnceLock},
};

use anyhow::{anyhow, Result};
use tantivy::{
    query::{BooleanQuery, Occur, Query as TantivyQuery, TermQuery},
    tokenizer::TextAnalyzer,
    Term,
};

mod bm25;
pub use bm25::{Bm25Stats, PreparedBm25Stats};

/// Represent a Full Text Query. It is the parsed form of a query from the user.
#[derive(Clone)]
pub struct Query(Arc<QueryCore>);

struct QueryCore {
    info: tipb::FtsQueryInfo,

    /// For indexed searcher.
    tantivy_query: Box<dyn TantivyQuery + 'static>,

    /// For BruteScoredSearcher:
    /// The tokenizer used to tokenize the query text. Currently it is also used
    /// to tokenize the document text.
    query_tokenizer: TextAnalyzer,

    /// For BruteScoredSearcher:
    /// Pre-tokenized query terms with their indices (deduplicated order of
    /// appearance).
    query_tokens: HashMap<String, /* token_index */ usize>,

    /// Query terms aligned to `query_tokens`'s `token_index`.
    ///
    /// This is used for BM25 global statistics accumulation without repeatedly
    /// constructing Tantivy `Term`s on hot paths.
    query_terms_by_token_idx: Vec<Term>,

    /// For BruteScoredSearcher:
    /// Occurrence count per token, aligned with `query_tokens` order.
    /// Tantivy is calculating BM25 by summing token scores and if there are
    /// duplicate tokens in the query, they will be counted multiple times.
    /// We provide the same score as Tantivy. The number of occurs for each
    /// token in the query: `[Token0, Token1, ...]` if there are duplicate
    /// token in one query they will be counted here.
    token_occurs: Vec<u32>,

    /// Prepared global BM25 statistics for this query (if available).
    ///
    /// When present, indexed search will use this as Tantivy's
    /// `Bm25StatisticsProvider` so that scores from different indexes are on
    /// the same scale.
    prepared_bm25: OnceLock<PreparedBm25Stats>,
}

impl Query {
    pub fn new(info: tipb::FtsQueryInfo) -> Result<Self> {
        let mut tokenizer = crate::TOKENIZERS
            .get(info.get_query_tokenizer())
            .ok_or_else(|| anyhow!("unsupported tokenizer {}", info.get_query_tokenizer()))?;

        let mut subqueries = Vec::new();
        let mut query_tokens = HashMap::new();
        let mut query_terms_by_token_idx = Vec::new();
        let mut token_occurs = Vec::new();
        let mut token_idx = 0usize;
        {
            let mut token_stream = tokenizer.token_stream(info.get_query_text());
            while token_stream.advance() {
                let token = token_stream.token_mut();
                let text = std::mem::take(&mut token.text);

                let term = tantivy::Term::from_field_text(crate::FIELD_BODY, &text);
                let term_query =
                    TermQuery::new(term, tantivy::schema::IndexRecordOption::WithFreqs);
                let boxed: Box<dyn TantivyQuery + 'static> = Box::new(term_query);
                subqueries.push((Occur::Should, boxed));

                let entry = query_tokens.entry(text);
                match entry {
                    Entry::Occupied(e) => {
                        token_occurs[*e.get()] += 1;
                    }
                    Entry::Vacant(e) => {
                        debug_assert_eq!(
                            query_terms_by_token_idx.len(),
                            token_idx,
                            "token_index must be contiguous starting from 0"
                        );
                        query_terms_by_token_idx.push(tantivy::Term::from_field_text(
                            crate::FIELD_BODY,
                            e.key().as_str(),
                        ));
                        e.insert(token_idx);
                        token_idx += 1;
                        token_occurs.push(1);
                    }
                }
            }
        }
        debug_assert_eq!(
            query_terms_by_token_idx.len(),
            query_tokens.len(),
            "query_terms_by_token_idx must align with query_tokens"
        );

        Ok(Query(Arc::new(QueryCore {
            info,
            tantivy_query: Box::new(BooleanQuery::new(subqueries)),
            query_tokenizer: tokenizer,
            query_tokens,
            query_terms_by_token_idx,
            token_occurs,
            prepared_bm25: OnceLock::new(),
        })))
    }

    #[inline]
    pub fn tokenizer(&self) -> &TextAnalyzer {
        &self.0.query_tokenizer
    }

    #[inline]
    pub fn info(&self) -> &tipb::FtsQueryInfo {
        &self.0.info
    }

    /// Returns the deduplicated query tokens with their indices.
    #[inline]
    pub fn query_tokens(&self) -> &HashMap<String, usize> {
        &self.0.query_tokens
    }

    /// Returns query terms aligned to `query_tokens`'s `token_index`.
    ///
    /// This is primarily used to avoid repeatedly constructing Tantivy `Term`s
    /// on hot paths (e.g. BM25 statistics accumulation across indexes).
    #[inline]
    pub fn query_terms_by_token_idx(&self) -> &[Term] {
        self.0.query_terms_by_token_idx.as_ref()
    }

    /// Returns occurrence counts per token aligned to `query_tokens` ordering.
    #[inline]
    pub fn token_occurs(&self) -> &[u32] {
        &self.0.token_occurs
    }

    /// Returns the tantivy query object.
    #[inline]
    pub fn tantivy_query(&self) -> &(dyn TantivyQuery + 'static) {
        &self.0.tantivy_query
    }

    /// Returns the prepared BM25 statistics if available.
    #[inline]
    pub fn prepared_bm25(&self) -> Option<&PreparedBm25Stats> {
        self.0.prepared_bm25.get()
    }

    /// Sets prepared BM25 statistics for this query if it has not been set.
    ///
    /// This is used to make scores produced by different sources directly
    /// comparable by forcing them to use the same global `(N, df, avgdl)`
    /// statistics.
    ///
    /// Returns `true` if the prepared statistics were stored by this call.
    pub fn prepare_bm25_once(&self, stats: Bm25Stats) -> Result<bool> {
        if self.prepared_bm25().is_some() {
            return Ok(false);
        }
        if stats.doc_freqs_by_token_idx.len() != self.query_tokens().len() {
            return Err(anyhow!(
                "doc_freqs_by_token_idx length {} does not match query_tokens length {}",
                stats.doc_freqs_by_token_idx.len(),
                self.query_tokens().len()
            ));
        }

        let prepared = PreparedBm25Stats::new(
            self.query_terms_by_token_idx(),
            stats.total_docs,
            stats.total_tokens,
            stats.doc_freqs_by_token_idx,
        )?;
        Ok(self.0.prepared_bm25.set(prepared).is_ok())
    }
}

impl std::convert::TryFrom<tipb::FtsQueryInfo> for Query {
    type Error = anyhow::Error;

    #[inline]
    fn try_from(info: tipb::FtsQueryInfo) -> Result<Self> {
        Self::new(info)
    }
}

pub mod test_util {

    use super::Query;

    pub const DEFAULT_FTS_TOP_K: u32 = 10;
    pub const DEFAULT_FTS_COLUMN_ID: i64 = 130;
    pub const DEFAULT_FTS_INDEX_ID: i64 = 42;
    pub const DEFAULT_FTS_TOKENIZER: &str = "STANDARD_V1";

    pub struct PlainFtsQueryInfo {
        pub query: String,
        pub top_k: u32,
        pub column_id: i64,
        pub index_id: i64,
        pub tokenizer: String,
    }

    impl Default for PlainFtsQueryInfo {
        fn default() -> Self {
            Self {
                query: String::new(),
                top_k: DEFAULT_FTS_TOP_K,
                column_id: DEFAULT_FTS_COLUMN_ID,
                index_id: DEFAULT_FTS_INDEX_ID,
                tokenizer: DEFAULT_FTS_TOKENIZER.to_string(),
            }
        }
    }

    impl PlainFtsQueryInfo {
        pub fn as_query_info(&self) -> tipb::FtsQueryInfo {
            let mut info = tipb::FtsQueryInfo::new();
            info.set_query_text(self.query.clone());
            info.set_top_k(self.top_k);
            info.set_index_id(self.index_id);
            info.set_query_tokenizer(self.tokenizer.clone());

            // Create a ColumnInfo for the column_id
            let mut column_info = tipb::ColumnInfo::new();
            column_info.set_column_id(self.column_id);
            column_info.set_tp(0xfe); // FieldTypeTp::String
            info.mut_columns().push(column_info);

            info
        }
    }

    /// Creates a new query for searching with scoring.
    pub fn make_scored_query(plain_info: &PlainFtsQueryInfo) -> Query {
        let mut info = plain_info.as_query_info();
        info.set_query_type(tipb::FtsQueryType::FtsQueryTypeWithScore);
        Query::new(info).unwrap()
    }

    /// Creates a new query for searching without scoring.
    pub fn make_unscored_query(plain_info: &PlainFtsQueryInfo) -> Query {
        let mut info = plain_info.as_query_info();
        info.set_query_type(tipb::FtsQueryType::FtsQueryTypeNoScore);
        Query::new(info).unwrap()
    }
}
