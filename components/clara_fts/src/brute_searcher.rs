// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::Result;
use tantivy::tokenizer::TextAnalyzer;

/// Search for a specified query within a dataset without any index.
/// It is faster than building an index over this dataset on the fly and then
/// search the index. However it will be magnitudes slower than a pre-built
/// index when the dataset does not change.
///
/// This searcher use the same BM25 algorithm as Tantivy, aims to produce
/// exactly the same score for the same query and source text.
pub struct BruteScoredSearcher {
    /// Used for both source text and query text.
    /// TODO: Support using different tokenizers for source text and query text.
    text_tokenizer: TextAnalyzer,

    /// Whether the caller needs scores (true) or only matching doc ids (false).
    need_score: bool,

    query: crate::Query,

    // Fields below are for BM25 statistics.
    /// Number of tokens each row: `[Row0, Row1, ...]`
    /// Here we do not store the actual tokens number, but store "fieldnorm id"
    /// (which maps u32 length to u8 id). This is not for saving memory, but
    /// to produce the same score result as Tantivy.
    per_row_n_tokens: Vec<u8>,
    /// Although per-row token number is recorded as fieldnorm id, the total
    /// tokens are not treated in this way. This is to produce the same
    /// score result as Tantivy.
    total_tokens: u32,
    /// Number of hits for each token for each row: `[Row0Token0, Row0Token1,
    /// ... Row1Token0, Row1Token1, ...]`
    per_row_per_token_hits: Vec<u32>,
    /// Number of hit docs for each token: `[Token0, Token1, ...]`
    per_token_doc_hits: Vec<u32>,

    /// A local buffer reused in each `add_document()`.
    buf_per_token_hits: Vec<u32>,
}

impl BruteScoredSearcher {
    /// Creates a brute-force searcher based on a parsed FTS query.
    ///
    /// The query parsing work is shared with the indexed path (see
    /// `query/mod.rs` and `IndexReader`), so callers don't need to re-tokenize
    /// or reinterpret query strings differently for brute-force search.
    pub fn new(query: &crate::Query) -> Result<Self> {
        Ok(Self {
            text_tokenizer: query.tokenizer().clone(),
            need_score: query.info().get_query_type() == tipb::FtsQueryType::FtsQueryTypeWithScore,
            query: query.clone(),
            per_row_n_tokens: Vec::new(),
            total_tokens: 0,
            per_row_per_token_hits: Vec::new(),
            per_token_doc_hits: vec![0; query.query_tokens().len()],
            buf_per_token_hits: vec![0; query.query_tokens().len()],
        })
    }

    #[inline]
    pub fn total_num_docs(&self) -> usize {
        self.per_row_n_tokens.len()
    }

    /// Reserves capacity for at least `additional` more documents (includes
    /// null) to be inserted.
    pub fn reserve(&mut self, additional: usize) {
        self.per_row_n_tokens.reserve(additional);
        self.per_row_per_token_hits
            .reserve(additional * self.query.query_tokens().len());
    }

    pub fn clear(&mut self) {
        self.per_row_n_tokens.clear();
        self.total_tokens = 0;
        self.per_row_per_token_hits.clear();
        self.per_token_doc_hits.fill(0);
    }

    pub fn add_document(&mut self, body: &str) {
        // We don't actually store the tokenlized data, only store metadata,
        // which is enough.

        self.buf_per_token_hits.fill(0);

        let mut this_doc_tokens = 0u32;
        let mut src_tokens = self.text_tokenizer.token_stream(body);
        while src_tokens.advance() {
            let token = src_tokens.token();
            this_doc_tokens += 1;
            let token_idx_ = self.query.query_tokens().get(&token.text);
            if let Some(token_idx) = token_idx_ {
                // Token occurs in the query.
                self.buf_per_token_hits[*token_idx] += 1;
            }
        }

        self.per_row_n_tokens
            .push(crate::tantivy_compat::fieldnorm_code::fieldnorm_to_id(
                this_doc_tokens,
            ));
        self.total_tokens += this_doc_tokens;
        self.per_row_per_token_hits.extend(&self.buf_per_token_hits);
        for idx in 0..self.buf_per_token_hits.len() {
            if self.buf_per_token_hits[idx] > 0 {
                // Even if a token is hit multiple times we only count it once.
                self.per_token_doc_hits[idx] += 1;
            }
        }
    }

    /// Scores a single document using globally prepared BM25 weights.
    ///
    /// This is used by readers that need to score unindexed rows while keeping
    /// scores comparable to indexed hits.
    ///
    /// Returns 0.0 when the document is empty or does not match any query term.
    pub fn score_document(&mut self, body: &str) -> f32 {
        debug_assert!(
            self.need_score,
            "score_document should only be used for WithScore queries"
        );
        debug_assert!(
            self.query.prepared_bm25().is_some(),
            "score_document requires prepared BM25 statistics"
        );
        let prepared = self.query.prepared_bm25().unwrap();
        debug_assert_eq!(
            prepared.weights_by_token_idx().len(),
            self.query.query_tokens().len(),
            "PreparedBm25 must be aligned to query_tokens"
        );

        self.buf_per_token_hits.fill(0);

        let mut dl = 0u32;
        let mut token_stream = self.text_tokenizer.token_stream(body);
        while token_stream.advance() {
            let token = token_stream.token();
            dl += 1;
            if let Some(&idx) = self.query.query_tokens().get(token.text.as_str()) {
                self.buf_per_token_hits[idx] += 1;
            }
        }
        if dl == 0 {
            return 0.0;
        }

        let fieldnorm_id = crate::fieldnorm_to_id(dl);
        let weights = prepared.weights_by_token_idx();
        let token_occurs = self.query.token_occurs();

        let mut score = 0.0f32;
        for idx in 0..weights.len() {
            let tf = self.buf_per_token_hits[idx];
            if tf == 0 {
                continue;
            }
            score += weights[idx].score(fieldnorm_id, tf) * (token_occurs[idx] as f32);
        }
        score
    }

    pub fn add_null(&mut self) {
        self.per_row_n_tokens.push(0);
        self.per_row_per_token_hits.resize(
            self.per_row_per_token_hits.len() + self.query.query_tokens().len(),
            0,
        );
    }

    /// The result is always sorted by RowID (but may miss some rows if row is
    /// null or not hit).
    ///
    /// This function contains heavy computation and the result won't change if
    /// there is no new doc.
    #[inline]
    pub fn search(&self, results: &mut Vec<super::ScoredResult>) -> Result<()> {
        if self.need_score {
            self.search_scored(results)
        } else {
            self.search_no_scored(results)
        }
    }

    fn search_scored(&self, results: &mut Vec<super::ScoredResult>) -> Result<()> {
        results.clear();

        let total_num_docs = self.per_row_n_tokens.len();
        let uniq_tokens = self.query.query_tokens().len();

        if total_num_docs == 0 || uniq_tokens == 0 {
            return Ok(());
        }

        let avg_fieldnorm = self.total_tokens as f32 / total_num_docs as f32;
        let mut weights_per_token = Vec::with_capacity(uniq_tokens);
        for i in 0..uniq_tokens {
            let term_doc_freq = self.per_token_doc_hits[i];
            let weight = tantivy::query::Bm25Weight::for_one_term(
                term_doc_freq as u64,
                total_num_docs as u64,
                avg_fieldnorm,
            );
            weights_per_token.push(weight);
        }

        let token_occurs = self.query.token_occurs();

        results.reserve(total_num_docs);
        for doc_id in 0..total_num_docs {
            let doc_id_mul_tokens = doc_id * uniq_tokens;
            let mut score = 0.0;
            for token_idx in 0..uniq_tokens {
                let term_freq = self.per_row_per_token_hits[doc_id_mul_tokens + token_idx];
                if term_freq == 0 {
                    continue;
                }
                let token_score = weights_per_token[token_idx].score(
                    self.per_row_n_tokens[doc_id], //
                    term_freq,
                );
                score += token_score * (token_occurs[token_idx] as f32);
            }
            if score > 0.0 {
                results.push(super::ScoredResult {
                    doc_id: doc_id as u32,
                    score,
                });
            }
        }

        Ok(())
    }

    fn search_no_scored(&self, results: &mut Vec<super::ScoredResult>) -> Result<()> {
        results.clear();

        let total_num_docs = self.per_row_n_tokens.len();
        let uniq_tokens = self.query.query_tokens().len();

        if total_num_docs == 0 || uniq_tokens == 0 {
            return Ok(());
        }

        results.reserve(total_num_docs);
        for doc_id in 0..total_num_docs {
            let doc_id_mul_tokens = doc_id * uniq_tokens;
            for token_idx in 0..uniq_tokens {
                if self.per_row_per_token_hits[doc_id_mul_tokens + token_idx] != 0 {
                    results.push(super::ScoredResult {
                        doc_id: doc_id as u32,
                        score: 1.0,
                    });
                    break;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::query::test_util::{make_scored_query, make_unscored_query, PlainFtsQueryInfo};

    fn is_index_match(src: &str, query: &str) -> Result<bool> {
        let idx = crate::index_for_test(&[src])?.finalize()?;
        let index_reader = crate::IndexReader::from_tantivy_index(idx)?;
        let mut results = Vec::new();
        index_reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: query.to_string(),
                tokenizer: "STANDARD_V1".to_string(),
                ..Default::default()
            }),
            &mut results,
        )?;
        Ok(!results.is_empty())
    }

    fn assert_match_eq(src: &str, query: &str) -> Result<()> {
        let index_match_result = is_index_match(src, query)?;
        let query_obj = make_unscored_query(&PlainFtsQueryInfo {
            query: query.to_string(),
            tokenizer: "STANDARD_V1".to_string(),
            ..Default::default()
        });
        let mut searcher = BruteScoredSearcher::new(&query_obj)?;
        searcher.add_document(src);
        let mut results = Vec::new();
        searcher.search(&mut results)?;
        let noindex_match_result = !results.is_empty();
        assert_eq!(
            index_match_result, noindex_match_result,
            "Search `{}` in `{}` got different result: found_by_index={}, found_by_noindex={}",
            query, src, index_match_result, noindex_match_result
        );
        Ok(())
    }

    #[test]
    fn test_search_same_as_index() -> Result<()> {
        // This test verifies that the BruteScoredSearcher returns the same result
        // as using the index.

        let src = "Being too popular can be such a hassle";
        let queries = vec![
            "popular",
            "people",
            "peoples",
            "can",
            "being",
            "beING",
            "CAN",
            "furina",
            "eing",
            "foo bar",
            "foo bar be",
            "ass",
            "ass a",
            "   ass    a   ",
            "elssah",
        ];
        for query in queries {
            assert_match_eq(src, query)?;
        }
        Ok(())
    }

    const SCORE_SAMPLE_DOCS: &[&str] = &[
        // 0
        "machine learning machine learning machine learning",
        // 1
        "machine learning algorithms. Machine learning models. Machine learning optimization.",
        // 2
        "Machine learning (Machine learning (Machine learning (Machine learning (Machine learning (is used in AI. Machine learning (Machine learning (Machine learning (Machine learning (Machine learning (is used in AI. Machine learning (Machine learning (Machine learning (Machine learning (Machine learning (is used in AI. Machine learning ends.",
        // 3
        "Understanding machine learning: basics of learning from machines. Machine learning requires data.",
        // 4
        "machine learning",
        // 5
        "This 50-word document briefly mentions machine learning once. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. ",
        // 6
        "Learning machine learning machine. Learning machine applications.",
        // 7
        "Space Tourism: Risks and Opportunities - Private companies like SpaceX and Blue Origin are making suborbital flights accessible, but safety protocols remain a critical concern.",
        // 8
        "Sustainable Fashion Trends - Eco-friendly materials like mushroom leather and recycled polyester are reshaping the fashion industry’s environmental impact.",
        // 9
        "AI-powered tools are transforming medical imaging analysis. Deep learning algorithms now detect early-stage tumors with 95% accuracy.", /* TODO: Add lines with > 100 words */
    ];

    fn assert_scores_eq(query: &str) -> Result<()> {
        let idx = crate::index_for_test(SCORE_SAMPLE_DOCS)?.finalize()?;
        let index_reader = crate::IndexReader::from_tantivy_index(idx)?;
        let mut results_index = Vec::new();
        let query_obj = make_scored_query(&PlainFtsQueryInfo {
            query: query.to_string(),
            tokenizer: "STANDARD_V1".to_string(),
            ..Default::default()
        });
        index_reader.search(&query_obj, &mut results_index)?;

        let mut searcher = BruteScoredSearcher::new(&query_obj)?;
        for doc in SCORE_SAMPLE_DOCS {
            searcher.add_document(doc);
        }
        let mut results_noindex = Vec::new();
        searcher.search(&mut results_noindex)?;

        assert_eq!(
            results_index.len(),
            results_noindex.len(),
            "Length mismatch: index={:?}, noindex={:?}",
            results_index,
            results_noindex
        );
        for (index_result, noindex_result) in results_index.iter().zip(results_noindex.iter()) {
            assert_eq!(
                index_result.doc_id, noindex_result.doc_id,
                "Docid mismatch: index={:?}, noindex={:?}",
                results_index, results_noindex
            );
            assert!(
                (index_result.score - noindex_result.score).abs() < 0.001,
                "Score mismatch: index={:?}, noindex={:?}",
                results_index,
                results_noindex
            );
        }
        Ok(())
    }

    #[test]
    fn test_search_score_same_as_index() -> Result<()> {
        // This test verifies that the BruteScoredSearcher returns the same result
        // as using the index.
        let queries = vec![
            "machine learn",
            "machine learning",
            "machine learning machine learning",
            "machine learning machine",
            "machine learning learning",
            "machine learning algorithms",
            "machine learning optimization",
            "machine learning is used in AI",
            "machine learning ends",
            "machine learning requires data",
            "learning machine",
            "learning machine applications",
            "space tourism",
            "tantivy bm25",
        ];
        for query in queries {
            assert_scores_eq(query)?;
        }
        Ok(())
    }

    #[test]
    fn test_score_document_same_as_index_with_prepared_stats() -> Result<()> {
        let idx = crate::index_for_test(SCORE_SAMPLE_DOCS)?.finalize()?;
        let index_reader = crate::IndexReader::from_tantivy_index(idx)?;

        let query_obj = make_scored_query(&PlainFtsQueryInfo {
            query: "machine learning machine learning".to_string(),
            tokenizer: "STANDARD_V1".to_string(),
            ..Default::default()
        });

        let mut results_index = Vec::new();
        index_reader.search(&query_obj, &mut results_index)?;
        let index_score = results_index
            .iter()
            .find(|r| r.doc_id == 0)
            .map(|r| r.score)
            .expect("doc 0 should match query");
        assert!(index_score.is_finite());
        assert!(index_score > 0.0);

        let mut stats = crate::Bm25Stats::empty(&query_obj);
        index_reader.accumulate_bm25_stats(&query_obj, &mut stats)?;
        assert!(query_obj.prepare_bm25_once(stats)?);

        let mut brute = BruteScoredSearcher::new(&query_obj)?;
        let brute_score = brute.score_document(SCORE_SAMPLE_DOCS[0]);
        assert!(brute_score.is_finite());
        assert!(
            (brute_score - index_score).abs() < 0.001,
            "Score mismatch: index={} brute={}",
            index_score,
            brute_score
        );
        Ok(())
    }
}

#[cfg(test)]
mod benches {
    use std::{fs, hint::black_box, io};

    use super::*;
    use crate::query::test_util::{make_scored_query, PlainFtsQueryInfo};

    fn prepare_bench_data() -> Vec<String> {
        use io::BufRead;
        // > The working directory of every benchmark is set to the
        // > root directory of the package the benchmark belongs to.
        let p = std::path::Path::new("../testdata/Arts_Crafts_and_Sewing.jsonl.gz");
        if !p.exists() {
            panic!("Test data file does not exist, see testdata/README.md for details");
        }
        let gz_file = flate2::read::GzDecoder::new(fs::File::open(p).unwrap());
        let buf_gz_file = io::BufReader::new(gz_file);
        let lines = buf_gz_file.lines();
        let mut data = Vec::new();
        for line in lines.map_while(Result::ok) {
            let v: serde_json::Value = serde_json::from_str(&line).unwrap();
            data.push(v["text"].as_str().unwrap().to_string());
            if data.len() >= 100 {
                break;
            }
        }

        data
    }

    #[bench]
    #[ignore]
    fn bench_scored_brute_search(b: &mut test::Bencher) {
        let data = prepare_bench_data();
        let query_obj = make_scored_query(&PlainFtsQueryInfo {
            query: "sewing machine".to_string(),
            tokenizer: "STANDARD_V1".to_string(),
            ..Default::default()
        });

        let mut results = Vec::new();
        b.iter(|| {
            let mut s = BruteScoredSearcher::new(&query_obj).unwrap();
            for d in &data {
                s.add_document(d);
            }
            s.search(&mut results).unwrap();
            let matches = results.len();
            black_box(matches);
            assert_eq!(4, matches);
        });
    }

    #[bench]
    #[ignore]
    fn bench_scored_tantivy_searcher_prebuilt(b: &mut test::Bencher) {
        let data = prepare_bench_data();
        let data_str = data.iter().map(|s| s.as_str()).collect::<Vec<_>>();

        let idx = crate::index_for_test(&data_str)
            .unwrap()
            .finalize()
            .unwrap();
        let idx_reader = crate::IndexReader::from_tantivy_index(idx).unwrap();

        let mut results = Vec::new();
        let query_obj = make_scored_query(&PlainFtsQueryInfo {
            query: "sewing machine".to_string(),
            tokenizer: "STANDARD_V1".to_string(),
            ..Default::default()
        });

        b.iter(|| {
            idx_reader.search(&query_obj, &mut results).unwrap();
            let matches = results.len();
            black_box(matches);
            assert_eq!(4, matches);
        });
    }

    #[bench]
    #[ignore]
    fn bench_scored_tantivy_searcher_on_demand(b: &mut test::Bencher) {
        let data = prepare_bench_data();
        let data_str = data.iter().map(|s| s.as_str()).collect::<Vec<_>>();

        let mut results = Vec::new();
        let query_obj = make_scored_query(&PlainFtsQueryInfo {
            query: "sewing machine".to_string(),
            tokenizer: "STANDARD_V1".to_string(),
            ..Default::default()
        });

        b.iter(|| {
            let idx = crate::index_for_test(&data_str)
                .unwrap()
                .finalize()
                .unwrap();
            let idx_reader = crate::IndexReader::from_tantivy_index(idx).unwrap();
            idx_reader.search(&query_obj, &mut results).unwrap();
            let matches = results.len();
            black_box(matches);
            assert_eq!(4, matches);
        });
    }
}
