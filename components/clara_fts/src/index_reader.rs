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

use anyhow::{bail, Result};
use tantivy::query::{Bm25StatisticsProvider, ConstScorer};

/// IndexReader reads index file for full text searching.
pub struct IndexReader {
    index: tantivy::Index,
    index_reader: tantivy::IndexReader,
}

impl Clone for IndexReader {
    fn clone(&self) -> Self {
        Self {
            index: self.index.clone(),
            index_reader: self.index_reader.clone(),
        }
    }
}

pub struct ResultIter {
    docset: Box<dyn tantivy::query::Scorer>,
}

impl Iterator for ResultIter {
    type Item = (u32, f32);

    fn next(&mut self) -> Option<Self::Item> {
        let docid = self.docset.doc();
        if docid == tantivy::TERMINATED {
            return None;
        }
        let score = self.docset.score();
        self.docset.advance();
        Some((docid, score))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.docset.size_hint() as usize;
        (size, Some(size))
    }
}

impl IndexReader {
    /// Create an IndexReader from any Tantivy Directory.
    /// Tokenizers are wired automatically to match the writer side.
    pub fn new(directory: impl tantivy::Directory + Clone) -> Result<Self> {
        let mut index = tantivy::Index::open(directory)?;
        index.set_tokenizers(crate::tokenizer::TOKENIZERS.clone());
        Self::from_tantivy_index(index)
    }

    pub fn from_tantivy_index(index: tantivy::Index) -> Result<Self> {
        let index_reader = index.reader()?;
        let schema = index.schema();
        {
            let field_body = schema.get_field("body")?;
            if field_body.field_id() != crate::FIELD_BODY.field_id() {
                bail!("Unexpected field id for body");
            }
        }
        Ok(Self {
            index,
            index_reader,
        })
    }

    /// Returns the underlying Tantivy index object.
    #[inline]
    pub fn tantivy_index(&self) -> &tantivy::Index {
        &self.index
    }

    /// Returns the underlying Tantivy index reader object.
    #[inline]
    pub fn tantivy_reader(&self) -> &tantivy::IndexReader {
        &self.index_reader
    }

    /// Returns the number of documents in the index.
    #[inline]
    pub fn n_docs(&self) -> usize {
        self.index_reader.searcher().num_docs() as usize
    }

    /// Adds this index's BM25 statistics to `stats`.
    ///
    /// The document count follows Tantivy's BM25 convention: `max_doc`
    /// (including empty-placeholder documents), not `num_docs`.
    ///
    /// `stats.doc_freqs_by_token_idx` must be aligned to
    /// `Query::query_tokens()`'s `token_index`.
    pub fn accumulate_bm25_stats(
        &self,
        query: &crate::Query,
        stats: &mut crate::Bm25Stats,
    ) -> Result<()> {
        if stats.doc_freqs_by_token_idx.len() != query.query_tokens().len() {
            bail!(
                "bm25 stats buffer length {} does not match query_tokens length {}",
                stats.doc_freqs_by_token_idx.len(),
                query.query_tokens().len()
            );
        }
        let searcher = self.index_reader.searcher();
        stats.total_docs += Bm25StatisticsProvider::total_num_docs(&searcher)?;
        stats.total_tokens +=
            Bm25StatisticsProvider::total_num_tokens(&searcher, crate::FIELD_BODY)?;

        let doc_freqs_by_token_idx = &mut stats.doc_freqs_by_token_idx;
        for (idx, term) in query.query_terms_by_token_idx().iter().enumerate() {
            doc_freqs_by_token_idx[idx] += searcher.doc_freq(term)?;
        }
        Ok(())
    }

    /// Searches the index for documents matching the query.
    /// If the document is not matching at all, it will not be returned.
    /// The returned documents do not have a pre-defined order.
    #[inline]
    pub fn search(&self, query: &crate::Query, results: &mut Vec<ScoredResult>) -> Result<()> {
        let iter = self.search_new(query)?;
        results.clear();
        results.reserve(iter.size_hint().0);
        for (doc_id, score) in iter {
            results.push(ScoredResult { doc_id, score });
        }
        Ok(())
    }

    /// Searches the index for documents matching the query.
    /// If the document is not matching at all, it will not be returned.
    /// The returned documents do not have a pre-defined order.
    #[inline]
    pub fn search_new(&self, query: &crate::Query) -> Result<ResultIter> {
        match query.info().get_query_type() {
            tipb::FtsQueryType::FtsQueryTypeNoScore => self.search_no_score(query),
            tipb::FtsQueryType::FtsQueryTypeWithScore => self.search_scored(query),
            _ => bail!("Unsupported query type {:?}", query.info().get_query_type()),
        }
    }

    /// Searches the index for documents matching the query without scoring.
    /// If the document is not matching at all, it will not be returned.
    /// The returned documents do not have a pre-defined order.
    ///
    /// The returned score field is always 1.0.
    fn search_no_score(&self, query: &crate::Query) -> Result<ResultIter> {
        let searcher = self.index_reader.searcher();
        let segment_readers = searcher.segment_readers();
        if segment_readers.len() != 1 {
            bail!("Expected 1 segment, got {}", segment_readers.len());
        }

        let weight =
            query
                .tantivy_query()
                .weight(tantivy::query::EnableScoring::disabled_from_searcher(
                    &searcher,
                ))?;

        // Note: We use a different impl compare as the simple one to allow early stop:
        //     weight.for_each_no_score(&segment_readers[0], &mut |docs| {
        // results.extend(docs); })?;
        //
        // TODO: Support max docs (=10000 by default) like ElasticSearch
        let docset = weight.scorer(&segment_readers[0], 1.0)?;
        Ok(ResultIter {
            docset: Box::new(ConstScorer::new(docset, 1.0)),
        })
    }

    /// Searches the index for documents matching the query and returns a score.
    /// If the document is not matching at all, it will not be returned.
    /// The returned documents do not have a pre-defined order.
    ///
    /// TODO: This function should be improved to not return scores, but return
    /// score primitives so that the caller could join multiple scores.
    fn search_scored(&self, query: &crate::Query) -> Result<ResultIter> {
        let searcher = self.index_reader.searcher();
        let segment_readers = searcher.segment_readers();
        if segment_readers.len() != 1 {
            bail!("Expected 1 segment, got {}", segment_readers.len());
        }

        let enable_scoring = match query.prepared_bm25() {
            Some(_) => {
                tantivy::query::EnableScoring::enabled_from_statistics_provider(query, &searcher)
            }
            None => tantivy::query::EnableScoring::enabled_from_searcher(&searcher),
        };

        let weight = query.tantivy_query().weight(enable_scoring)?;

        let docset = weight.scorer(&segment_readers[0], 1.0)?;
        Ok(ResultIter { docset })
    }
}

#[derive(Debug)]
pub struct BitmapFilter<'a> {
    pub match_all: bool,
    pub match_partial: &'a [u8],
}

#[derive(Debug, PartialEq)]
pub struct ScoredResult {
    pub doc_id: u32,
    pub score: f32,
}

impl BitmapFilter<'_> {
    pub fn all_match() -> Self {
        Self {
            match_all: true,
            match_partial: &[],
        }
    }

    pub fn partial_match<'a>(filter: &'a [u8]) -> BitmapFilter<'a> {
        BitmapFilter {
            match_all: false,
            match_partial: filter,
        }
    }
}

#[cfg(test)]
mod tests {

    use ordered_float::OrderedFloat;
    use paste::paste;

    use super::*;
    use crate::query::test_util::{make_scored_query, make_unscored_query, PlainFtsQueryInfo};

    fn assert_doc_ids_eq(results: &[ScoredResult], expected_doc_ids: &[u32]) {
        let doc_ids: Vec<u32> = results.iter().map(|r| r.doc_id).collect();
        assert_eq!(doc_ids, expected_doc_ids);
    }

    macro_rules! write_read_test {
        ($test_name:ident, $get_writer:ident, $finalize:ident, $body:expr) => {
            paste! {
                #[test]
                fn [< $test_name _in_memory >]() -> Result<()> {
                    // In-memory test
                    let $get_writer = || crate::TantivyIndexWriter::new_in_memory("STANDARD_V1");
                    let $finalize = |w: crate::TantivyIndexWriter<crate::TrackedDirectory<tantivy::directory::RamDirectory>>| {
                        let idx = w.finalize()?;
                        IndexReader::from_tantivy_index(idx)
                    };
                    $body;
                    Ok(())
                }
            }
        };
    }

    write_read_test!(test_indexer_basic, get_writer, finalize, {
        let mut w = get_writer()?;
        w.add_document("Being too popular can be such a hassle")?;
        w.add_document("Who knew the people would adore me so much?")?;
        w.add_document("The world is but a stage.")?;
        w.add_document("Why cry, when you can laugh instead?")?;

        // Just test some basic behaviors to ensure that we can read and search the
        // index. There are standalone search tests covering case sensitivity,
        // normalization, tokenization, etc.
        let reader = finalize(w)?;
        let mut results = Vec::new();
        reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "popular".to_string(),
                tokenizer: "STANDARD_V1".to_string(),
                ..Default::default()
            }),
            &mut results,
        )?;
        assert_doc_ids_eq(&results, &[0]);
        reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "people".to_string(),
                ..Default::default()
            }),
            &mut results,
        )?;
        assert_doc_ids_eq(&results, &[1]);
        reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "can".to_string(),
                ..Default::default()
            }),
            &mut results,
        )?;
        assert_doc_ids_eq(&results, &[0, 3]);
        reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "furina".to_string(),
                ..Default::default()
            }),
            &mut results,
        )?;
        assert!(results.is_empty());

        assert_eq!(reader.n_docs(), 4);
    });

    write_read_test!(test_null, get_writer, finalize, {
        let mut w = get_writer()?;
        w.add_document("Being too popular can be such a hassle")?; // id=0
        w.add_null()?;
        w.add_document("Who knew the people would adore me so much?")?; // id=2
        w.add_document("The world is but a stage.")?; // id=3
        w.add_null()?;
        w.add_null()?;
        w.add_document("Why cry, when you can laugh instead?")?; // id=6

        // Just test some basic behaviors to ensure that we can read and search the
        // index. There are standalone search tests covering case sensitivity,
        // normalization, tokenization, etc.
        let reader = finalize(w)?;
        let mut results = Vec::new();
        reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "popular".to_string(),
                tokenizer: "STANDARD_V1".to_string(),
                ..Default::default()
            }),
            &mut results,
        )?;
        assert_doc_ids_eq(&results, &[0]);
        reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "people".to_string(),
                ..Default::default()
            }),
            &mut results,
        )?;
        assert_doc_ids_eq(&results, &[2]);
        reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "can".to_string(),
                ..Default::default()
            }),
            &mut results,
        )?;
        assert_doc_ids_eq(&results, &[0, 6]);
        reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "furina".to_string(),
                ..Default::default()
            }),
            &mut results,
        )?;
        assert!(results.is_empty());
        assert_eq!(reader.n_docs(), 7);
    });

    write_read_test!(test_indexer_empty, get_writer, finalize, {
        let w = get_writer()?;
        let reader = finalize(w)?;
        let mut results = Vec::new();
        reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "popular".to_string(),
                tokenizer: "STANDARD_V1".to_string(),
                ..Default::default()
            }),
            &mut results,
        )?;
        assert!(results.is_empty());
    });

    write_read_test!(test_indexer_search_empty, get_writer, finalize, {
        let mut w = get_writer()?;
        w.add_document("Being too popular can be such a hassle")?; // id=0
        w.add_null()?;
        w.add_document("Who knew the people would adore me so much?")?; // id=2
        w.add_document("The world is but a stage.")?; // id=3
        w.add_null()?;
        w.add_null()?;
        w.add_document("Why cry, when you can laugh instead?")?; // id=6

        // Empty query should not match any document.
        let reader = finalize(w)?;
        let mut results = Vec::new();
        reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "".to_string(),
                ..Default::default()
            }),
            &mut results,
        )?;
        assert!(results.is_empty());

        reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "  ".to_string(),
                ..Default::default()
            }),
            &mut results,
        )?;
        assert!(results.is_empty());

        reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "foobar".to_string(),
                ..Default::default()
            }),
            &mut results,
        )?;
        assert!(results.is_empty());
    });

    write_read_test!(test_filter_partial_match, get_writer, finalize, {
        let mut w = get_writer()?;
        w.add_document("Being too popular can be such a hassle")?;
        w.add_document("Who knew the people would adore me so much?")?;
        w.add_document("The world is but a stage.")?;
        w.add_document("Why cry, when you can laugh instead?")?;

        let reader = finalize(w)?;
        let mut results = Vec::new();

        reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "can".to_string(),
                ..Default::default()
            }),
            &mut results,
        )?;
        assert_doc_ids_eq(&results, &[0, 3]);
    });

    write_read_test!(test_filter_partial_match_with_null, get_writer, finalize, {
        let mut w = get_writer()?;
        w.add_null()?;
        w.add_null()?;
        w.add_document("Being too popular can be such a hassle")?; //id=2
        w.add_null()?;
        w.add_null()?;
        w.add_document("Who knew the people would adore me so much?")?; //id=5
        w.add_document("The world is but a stage.")?; //id=6
        w.add_document("Why cry, when you can laugh instead?")?; //id=7

        let reader = finalize(w)?;

        let mut results = Vec::new();
        reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "can".to_string(),
                ..Default::default()
            }),
            &mut results,
        )?;
        assert_doc_ids_eq(&results, &[2, 7]);
    });

    write_read_test!(test_filter_none_match, get_writer, finalize, {
        let mut w = get_writer()?;
        w.add_document("Being too popular can be such a hassle")?;
        w.add_document("Who knew the people would adore me so much?")?;
        w.add_document("The world is but a stage.")?;
        w.add_document("Why cry, when you can laugh instead?")?;

        let reader = finalize(w)?;
        let mut results = Vec::new();

        reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "can".to_string(),
                ..Default::default()
            }),
            &mut results,
        )?;
        assert_doc_ids_eq(&results, &[0, 3]);
    });

    write_read_test!(test_filter_all_match, get_writer, finalize, {
        let mut w = get_writer()?;
        w.add_document("Being too popular can be such a hassle")?;
        w.add_document("Who knew the people would adore me so much?")?;
        w.add_document("The world is but a stage.")?;
        w.add_document("Why cry, when you can laugh instead?")?;

        let reader = finalize(w)?;
        let mut results = Vec::new();

        reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "can".to_string(),
                ..Default::default()
            }),
            &mut results,
        )?;
        assert_doc_ids_eq(&results, &[0, 3]);
    });

    write_read_test!(test_filter_partial_match_length, get_writer, finalize, {
        let mut w = get_writer()?;
        w.add_document("Being too popular can be such a hassle")?;

        let reader = finalize(w)?;
        let mut results = Vec::new();

        let r = reader.search(
            &make_unscored_query(&PlainFtsQueryInfo {
                query: "can".to_string(),
                ..Default::default()
            }),
            &mut results,
        );
        assert!(r.is_ok());
        assert_doc_ids_eq(&results, &[0]);
    });

    write_read_test!(
        test_filter_partial_match_length_with_null,
        get_writer,
        finalize,
        {
            let mut w = get_writer()?;
            w.add_null()?;
            w.add_document("Being too popular can be such a hassle")?;

            // There are 2 documents including the null.

            let reader = finalize(w)?;
            let mut results = Vec::new();

            let r = reader.search(
                &make_unscored_query(&PlainFtsQueryInfo {
                    query: "can".to_string(),
                    ..Default::default()
                }),
                &mut results,
            );
            assert!(r.is_ok());
            assert_doc_ids_eq(&results, &[1]);
        }
    );

    write_read_test!(test_indexer_abandon, get_writer, finalize, {
        let mut w = get_writer()?;
        w.add_document("Being too popular can be such a hassle")?;
        // w.finalize() is not called. Should be fine.
        drop(w);

        _ = finalize;
    });

    #[test]
    fn test_scored_search_uses_prepared_global_bm25_stats() -> Result<()> {
        // Two independent indexes should produce the same scores as a single combined
        // index when using prepared global BM25 statistics.
        let docs_a: [&str; 3] = ["alpha beta beta", "alpha alpha", "gamma"];
        let docs_b: [&str; 7] = [
            "alpha beta",
            "beta",
            "delta alpha",
            "epsilon",
            "alpha beta beta beta",
            "beta beta",
            "zeta",
        ];
        let docs_all: Vec<&str> = docs_a.iter().chain(docs_b.iter()).copied().collect();

        let idx_a = crate::index_for_test(&docs_a)?.finalize()?;
        let idx_b = crate::index_for_test(&docs_b)?.finalize()?;
        let idx_all = crate::index_for_test(&docs_all)?.finalize()?;

        let reader_a = IndexReader::from_tantivy_index(idx_a)?;
        let reader_b = IndexReader::from_tantivy_index(idx_b)?;
        let reader_all = IndexReader::from_tantivy_index(idx_all)?;

        let plain_info = PlainFtsQueryInfo {
            query: "alpha beta beta".to_string(),
            tokenizer: "STANDARD_V1".to_string(),
            ..Default::default()
        };

        // Combined index uses its own local BM25 statistics (which are already global).
        let query_combined = make_scored_query(&plain_info);
        let mut results_all = Vec::new();
        reader_all.search(&query_combined, &mut results_all)?;
        results_all.sort_by_key(|r| r.doc_id);

        // Prepare global BM25 stats from (A + B).
        let query_prepared = make_scored_query(&plain_info);
        let mut stats = crate::Bm25Stats::empty(&query_prepared);
        reader_a.accumulate_bm25_stats(&query_prepared, &mut stats)?;
        reader_b.accumulate_bm25_stats(&query_prepared, &mut stats)?;
        assert!(query_prepared.prepare_bm25_once(stats)?);

        let mut results_split = Vec::new();

        let mut results_a = Vec::new();
        reader_a.search(&query_prepared, &mut results_a)?;
        results_split.extend(results_a);

        let mut results_b = Vec::new();
        reader_b.search(&query_prepared, &mut results_b)?;
        let offset = docs_a.len() as u32;
        results_split.extend(results_b.into_iter().map(|r| ScoredResult {
            doc_id: r.doc_id + offset,
            score: r.score,
        }));

        results_split.sort_by_key(|r| r.doc_id);

        assert_eq!(
            results_split.len(),
            results_all.len(),
            "Length mismatch: split={:?}, all={:?}",
            results_split,
            results_all
        );

        for (split, combined) in results_split.iter().zip(results_all.iter()) {
            assert_eq!(
                split.doc_id, combined.doc_id,
                "Docid mismatch: split={:?}, all={:?}",
                results_split, results_all
            );
            assert!(
                (split.score - combined.score).abs() < 0.001,
                "Doc {} score mismatch: split={} combined={}",
                split.doc_id,
                split.score,
                combined.score
            );
        }
        Ok(())
    }

    const TOPK_SAMPLES: &[&str] = &[
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
        "AI-powered tools are transforming medical imaging analysis. Deep learning algorithms now detect early-stage tumors with 95% accuracy.",
    ];

    write_read_test!(test_topk, get_writer, finalize, {
        let mut w = get_writer()?;
        for sample in TOPK_SAMPLES {
            w.add_document(sample)?;
        }
        let reader = finalize(w)?;
        let mut r = Vec::new();

        reader.search(
            &make_scored_query(&PlainFtsQueryInfo {
                query: "machine learning".to_string(),
                ..Default::default()
            }),
            &mut r,
        )?;
        r.sort_by_key(|result| OrderedFloat(-result.score));
        let rows = r.iter().map(|result| result.doc_id).collect::<Vec<_>>();
        assert_eq!(rows, vec![2, 0, 6, 1, 3, 4, 5, 9]);
    });

    write_read_test!(test_topk_with_filter, get_writer, finalize, {
        let mut w = get_writer()?;
        for sample in TOPK_SAMPLES {
            w.add_document(sample)?;
        }
        let reader = finalize(w)?;
        let mut r = Vec::new();

        reader.search(
            &make_scored_query(&PlainFtsQueryInfo {
                query: "machine learning".to_string(),
                ..Default::default()
            }),
            &mut r,
        )?;
        r.sort_by_key(|result| OrderedFloat(-result.score));
        let rows = r.iter().map(|result| result.doc_id).collect::<Vec<_>>();
        assert_eq!(rows, vec![2, 0, 6, 1, 3, 4, 5, 9]);
    });

    #[test]
    #[rustfmt::skip]
    fn test_tokenizer() -> Result<()> {
        let mut w = crate::TantivyIndexWriter::new_in_memory("MULTILINGUAL_V1")?;
        w.add_document(/* 0 */ "· 中办国办印发《逐步把永久基本农田建成高标准农田实施方案》")?;
        w.add_document(/* 1 */ "· 现象级创新，中国后劲如何？（读者点题·共同关注）")?;
        w.add_document(/* 2 */ "· 陕西新能源汽车产业争创新优势")?;
        w.add_document(/* 3 */ "· 让城市排水治理更智能（“两重”建设扎实推进）")?;
        w.add_document(/* 4 */ "· 图片报道")?;
        w.add_document(/* 5 */ "· 去年我国港口吞吐量稳居世界第一")?;
        w.add_document(/* 6 */ "· 今年前两月社会物流总额同比增5.3%")?;
        w.add_document(/* 7 */ "· 共同探索互利共赢的全球科技合作新模式")?;
        w.add_document(/* 8 */ "· 双向奔赴，让投资更好惠及两国人民（钟声）")?;
        w.add_document(/* 9 */ "· 中国—东盟合作树立“全球南方”联合自强重要范本（国际论坛）")?;
        w.add_document(/* 10 */ "· “携手打造一个更加繁荣的亚洲命运共同体”（高端访谈）")?;
        w.add_document(/* 11 */ "· 中方救援队成功救出幸存者")?;
        w.add_document(/* 12 */ "· 加强文明对话 促进理解信任（聚焦博鳌亚洲论坛2025年年会）")?;

        let reader = IndexReader::from_tantivy_index(w.finalize()?)?;

        let mut results = Vec::new();
        reader.search(&make_unscored_query(&PlainFtsQueryInfo {
            query: "中国".to_string(),
            tokenizer: "MULTILINGUAL_V1".to_string(),
            ..Default::default()
        }), &mut results)?;
        assert_doc_ids_eq(&results, &[1, 9]);
        reader.search(&make_unscored_query(&PlainFtsQueryInfo {
            query: "年".to_string(),
            tokenizer: "MULTILINGUAL_V1".to_string(),
            ..Default::default()
        }), &mut results)?;
        assert_doc_ids_eq(&results, &[12]);
        reader.search(&make_unscored_query(&PlainFtsQueryInfo {
            query: "今年".to_string(),
            tokenizer: "MULTILINGUAL_V1".to_string(),
            ..Default::default()
        }), &mut results)?;
        assert_doc_ids_eq(&results, &[6]);
        reader.search(&make_unscored_query(&PlainFtsQueryInfo {
            query: "报道图片".to_string(),
            tokenizer: "MULTILINGUAL_V1".to_string(),
            ..Default::default()
        }), &mut results)?;
        assert_doc_ids_eq(&results, &[4]);
        reader.search(&make_unscored_query(&PlainFtsQueryInfo {
            query: "中国年".to_string(),
            tokenizer: "MULTILINGUAL_V1".to_string(),
            ..Default::default()
        }), &mut results)?;
        assert_doc_ids_eq(&results, &[1, 9, 12]);

        Ok(())
    }
}
