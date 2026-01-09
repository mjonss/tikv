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

use std::path::Path;

use anyhow::{anyhow, bail, Result};

/// IndexReader reads index file for full text searching.
pub struct IndexReader {
    index: tantivy::Index,
    index_reader: tantivy::IndexReader,
    query_tokenizer: tantivy::tokenizer::TextAnalyzer,

    field_body: tantivy::schema::Field,
}

impl IndexReader {
    pub fn new(directory: impl tantivy::Directory + Clone) -> Result<Self> {
        let mut index = tantivy::Index::open(directory)?;
        index.set_tokenizers(crate::tokenizer::TOKENIZERS.clone());
        Self::from_tantivy_index(index)
    }

    pub fn from_tantivy_index(index: tantivy::Index) -> Result<Self> {
        let index_reader = index.reader()?;
        let schema = index.schema();
        let field_body = schema.get_field("body")?;

        let tokenizer = {
            let name = {
                let field_body_type = schema.get_field_entry(field_body).field_type();
                if let tantivy::schema::FieldType::Str(body_option) = field_body_type {
                    let options = body_option
                        .get_indexing_options()
                        .ok_or_else(|| anyhow!("Unexpected field body not indexed"))?;
                    options.tokenizer()
                } else {
                    bail!("Unexpected field type for body");
                }
            };
            crate::tokenizer::TOKENIZERS
                .get(name)
                .ok_or_else(|| anyhow!("Tokenizer not found: {}", name))?
        };

        Ok(Self {
            index,
            index_reader,
            query_tokenizer: tokenizer,
            field_body,
        })
    }

    /// Returns the underlying Tantivy index object.
    #[inline]
    pub fn tantivy_index(&self) -> &tantivy::Index {
        &self.index
    }

    /// Returns the number of documents in the index.
    #[inline]
    pub fn n_docs(&self) -> usize {
        self.index_reader.searcher().num_docs() as usize
    }

    /// Creates a new `IndexReader` from index file.
    /// Parameters use simple primitive types for FFI compatibility.
    pub fn new_mmap(index_file_path: &str) -> Result<Self> {
        let directory = crate::MergedFileAsDirectory::from_mmap_file(Path::new(index_file_path))?;
        Self::new(directory)
    }

    /// Creates a new `IndexReader` from a memory buffer which holds the index.
    /// Parameters use simple primitive types for FFI compatibility.
    pub fn new_memory(index_buffer: Vec<u8>) -> Result<Self> {
        let directory = crate::MergedFileAsDirectory::from_buffer(index_buffer)?;
        Self::new(directory)
    }

    /// Build a "Should" boolean query for the given query string.
    /// Unlike query parser, for texts like "我失败了" (after segmentation
    /// 我/失败/了), it's semantic is "我" OR "失败" OR "了", while in the
    /// Tantivy's query parser it will be "我" AND "失败" AND "了", which is not
    /// what we want.
    ///
    /// // TODO: Extract into a standalone function so that the query can be
    /// reused for different segments.
    fn build_query(&self, query: &str) -> Result<Box<dyn tantivy::query::Query + 'static>> {
        use tantivy::query::*;
        let mut tokenizer = self.query_tokenizer.clone();
        let mut token_stream = tokenizer.token_stream(query);
        let mut subqueries = vec![];
        token_stream.process(&mut |token| {
            let term = tantivy::Term::from_field_text(self.field_body, &token.text);
            let term_query = TermQuery::new(term, tantivy::schema::IndexRecordOption::WithFreqs);
            let boxed: Box<dyn Query + 'static> = Box::new(term_query);
            subqueries.push((Occur::Should, boxed));
        });
        let query = BooleanQuery::new(subqueries);
        Ok(Box::new(query))
    }

    /// Searches the index for documents matching the query without scoring.
    /// If the document is not matching at all, it will not be returned.
    /// The returned documents do not have a pre-defined order.
    ///
    /// Parameters use simple primitive types for FFI compatibility.
    ///
    /// # Arguments
    ///
    /// * `filter_bitmap` - Filter each result.
    pub fn search_no_score(
        &self,
        query: &str,
        filter: &BitmapFilter<'_>,
        results: &mut Vec<u32>,
    ) -> Result<()> {
        results.clear();

        let searcher = self.index_reader.searcher();
        let segment_readers = searcher.segment_readers();
        if segment_readers.len() != 1 {
            bail!("Expected 1 segment, got {}", segment_readers.len());
        }

        if !filter.match_all {
            let docs_in_segment = segment_readers[0].max_doc();
            if (docs_in_segment as usize) != filter.match_partial.len() {
                bail!(
                    "Invalid bitmap filter, expected length {}, got {}",
                    docs_in_segment,
                    filter.match_partial.len()
                );
            }
        }

        let query = self.build_query(query)?;
        let weight = query.weight(tantivy::query::EnableScoring::disabled_from_searcher(
            &searcher,
        ))?;

        // Note: We use a different impl compare as the simple one to allow early stop:
        //     weight.for_each_no_score(&segment_readers[0], &mut |docs| {
        // results.extend(docs); })?; TODO: Support max docs (=10000 by default)
        // like ElasticSearch
        let mut docset = weight.scorer(&segment_readers[0], 1.0)?;
        results.reserve(docset.size_hint() as usize);

        if filter.match_all {
            // Fast path, no filtering is applied
            let mut buffer = [0u32; tantivy::COLLECT_BLOCK_BUFFER_LEN];
            loop {
                let filled_n = docset.fill_buffer(&mut buffer);
                results.extend_from_slice(&buffer[..filled_n]);
                if filled_n < tantivy::COLLECT_BLOCK_BUFFER_LEN {
                    break;
                }
            }
        } else {
            // Slow path: filter rows one by one
            loop {
                let docid = docset.doc();
                if docid == tantivy::TERMINATED {
                    break;
                }
                if filter.match_partial[docid as usize] > 0 {
                    results.push(docid);
                }
                docset.advance();
            }
        }

        Ok(())
    }

    /// Searches the index for documents matching the query and returns a score.
    /// If the document is not matching at all, it will not be returned.
    /// The returned documents do not have a pre-defined order.
    ///
    /// Parameters use simple primitive types for FFI compatibility.
    ///
    /// TODO: This function should be improved to not return scores, but return
    /// score primitives so that the caller could join multiple scores.
    ///
    /// # Arguments
    ///
    /// * `filter_bitmap` - Filter each result.
    pub fn search_scored(
        &self,
        query: &str,
        filter: &BitmapFilter<'_>,
        results: &mut Vec<ScoredResult>,
    ) -> Result<()> {
        results.clear();

        let searcher = self.index_reader.searcher();
        let segment_readers = searcher.segment_readers();
        if segment_readers.len() != 1 {
            bail!("Expected 1 segment, got {}", segment_readers.len());
        }

        if !filter.match_all {
            let docs_in_segment = segment_readers[0].max_doc();
            if (docs_in_segment as usize) != filter.match_partial.len() {
                bail!(
                    "Invalid bitmap filter, expected length {}, got {}",
                    docs_in_segment,
                    filter.match_partial.len()
                );
            }
        }

        let query = self.build_query(query)?;
        let weight = query.weight(tantivy::query::EnableScoring::enabled_from_searcher(
            &searcher,
        ))?;

        let mut docset = weight.scorer(&segment_readers[0], 1.0)?;
        results.reserve(docset.size_hint() as usize);

        if filter.match_all {
            loop {
                let docid = docset.doc();
                if docid == tantivy::TERMINATED {
                    break;
                }
                results.push(ScoredResult {
                    doc_id: docid,
                    score: docset.score(),
                });
                docset.advance();
            }
        } else {
            loop {
                let docid = docset.doc();
                if docid == tantivy::TERMINATED {
                    break;
                }
                if filter.match_partial[docid as usize] > 0 {
                    results.push(ScoredResult {
                        doc_id: docid,
                        score: docset.score(),
                    });
                }
                docset.advance();
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct BitmapFilter<'a> {
    pub match_all: bool,
    pub match_partial: &'a [u8],
}

#[derive(Debug)]
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
    use crate::{IndexWriterInMemory, IndexWriterOnDisk};

    macro_rules! write_read_test {
        ($test_name:ident, $get_writer:ident, $finalize:ident, $body:expr) => {
            paste! {
                #[test]
                fn [< $test_name _on_disk >]() -> Result<()> {
                    // On-disk test
                    let dir_guard = tempfile::tempdir()?;
                    let $get_writer = || {
                        IndexWriterOnDisk::new(
                            "STANDARD_V1",
                            dir_guard.path().join("test.index").to_str().unwrap(),
                        )
                    };
                    let $finalize = |mut w: IndexWriterOnDisk| {
                        w.finalize()?;
                        IndexReader::new_mmap(
                            dir_guard.path().join("test.index").to_str().unwrap(),
                        )
                    };
                    $body;
                    Ok(())
                }

                #[test]
                fn [< $test_name _in_memory >]() -> Result<()> {
                    // In-memory test
                    let $get_writer = || IndexWriterInMemory::new("STANDARD_V1");
                    let $finalize = |mut w: IndexWriterInMemory| {
                        let buffer = w.finalize()?;
                        IndexReader::new_memory(buffer)
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
        reader.search_no_score("popular", &BitmapFilter::all_match(), &mut results)?;
        assert_eq!(results, vec![0]);
        reader.search_no_score("people", &BitmapFilter::all_match(), &mut results)?;
        assert_eq!(results, vec![1]);
        reader.search_no_score("can", &BitmapFilter::all_match(), &mut results)?;
        assert_eq!(results, vec![0, 3]);
        reader.search_no_score("furina", &BitmapFilter::all_match(), &mut results)?;
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
        reader.search_no_score("popular", &BitmapFilter::all_match(), &mut results)?;
        assert_eq!(results, vec![0]);
        reader.search_no_score("people", &BitmapFilter::all_match(), &mut results)?;
        assert_eq!(results, vec![2]);
        reader.search_no_score("can", &BitmapFilter::all_match(), &mut results)?;
        assert_eq!(results, vec![0, 6]);
        reader.search_no_score("furina", &BitmapFilter::all_match(), &mut results)?;
        assert!(results.is_empty());

        assert_eq!(reader.n_docs(), 7);
    });

    write_read_test!(test_indexer_empty, get_writer, finalize, {
        let w = get_writer()?;
        let reader = finalize(w)?;
        let mut results = Vec::new();
        reader.search_no_score("popular", &BitmapFilter::all_match(), &mut results)?;
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
        reader.search_no_score("", &BitmapFilter::all_match(), &mut results)?;
        assert!(results.is_empty());

        reader.search_no_score("  ", &BitmapFilter::all_match(), &mut results)?;
        assert!(results.is_empty());

        reader.search_no_score("foobar", &BitmapFilter::all_match(), &mut results)?;
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

        reader.search_no_score(
            "can",
            &BitmapFilter::partial_match(&[0, 1, 0, 1]),
            &mut results,
        )?;
        assert_eq!(results, vec![3]);
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
        reader.search_no_score(
            "can",
            &BitmapFilter::partial_match(&[1, 1, 0, 0, 0, 0, 0, 0]),
            &mut results,
        )?;
        assert!(results.is_empty());

        reader.search_no_score(
            "can",
            &BitmapFilter::partial_match(&[1, 1, 1, 0, 0, 0, 0, 0]),
            &mut results,
        )?;
        assert_eq!(results, vec![2]);

        reader.search_no_score(
            "can",
            &BitmapFilter::partial_match(&[1, 1, 1, 0, 1, 1, 1, 0]),
            &mut results,
        )?;
        assert_eq!(results, vec![2]);

        reader.search_no_score(
            "can",
            &BitmapFilter::partial_match(&[1, 1, 1, 0, 0, 0, 0, 1]),
            &mut results,
        )?;
        assert_eq!(results, vec![2, 7]);
    });

    write_read_test!(test_filter_none_match, get_writer, finalize, {
        let mut w = get_writer()?;
        w.add_document("Being too popular can be such a hassle")?;
        w.add_document("Who knew the people would adore me so much?")?;
        w.add_document("The world is but a stage.")?;
        w.add_document("Why cry, when you can laugh instead?")?;

        let reader = finalize(w)?;
        let mut results = Vec::new();

        reader.search_no_score(
            "can",
            &BitmapFilter::partial_match(&[0, 0, 0, 0]),
            &mut results,
        )?;
        assert!(results.is_empty());
    });

    write_read_test!(test_filter_all_match, get_writer, finalize, {
        let mut w = get_writer()?;
        w.add_document("Being too popular can be such a hassle")?;
        w.add_document("Who knew the people would adore me so much?")?;
        w.add_document("The world is but a stage.")?;
        w.add_document("Why cry, when you can laugh instead?")?;

        let reader = finalize(w)?;
        let mut results = Vec::new();

        reader.search_no_score("can", &BitmapFilter::all_match(), &mut results)?;
        assert_eq!(results, vec![0, 3]);
    });

    write_read_test!(test_filter_partial_match_length, get_writer, finalize, {
        let mut w = get_writer()?;
        w.add_document("Being too popular can be such a hassle")?;

        let reader = finalize(w)?;
        let mut results = Vec::new();

        let r = reader.search_no_score("can", &BitmapFilter::partial_match(&[]), &mut results);
        assert!(r.is_err());

        let r = reader.search_no_score("can", &BitmapFilter::partial_match(&[0]), &mut results);
        assert!(r.is_ok());

        let r = reader.search_no_score("can", &BitmapFilter::partial_match(&[0, 0]), &mut results);
        assert!(r.is_err());
    });

    write_read_test!(
        test_filter_partial_match_length_with_null,
        get_writer,
        finalize,
        {
            let mut w = get_writer()?;
            w.add_null()?;
            w.add_document("Being too popular can be such a hassle")?;

            // There are 2 documents including the null, so the filter length should be 2.

            let reader = finalize(w)?;
            let mut results = Vec::new();

            let r = reader.search_no_score("can", &BitmapFilter::partial_match(&[]), &mut results);
            assert!(r.is_err());

            let r = reader.search_no_score("can", &BitmapFilter::partial_match(&[0]), &mut results);
            assert!(r.is_err());

            let r =
                reader.search_no_score("can", &BitmapFilter::partial_match(&[0, 0]), &mut results);
            assert!(r.is_ok());

            let r = reader.search_no_score(
                "can",
                &BitmapFilter::partial_match(&[0, 0, 0]),
                &mut results,
            );
            assert!(r.is_err());
        }
    );

    write_read_test!(test_indexer_abandon, get_writer, finalize, {
        let mut w = get_writer()?;
        w.add_document("Being too popular can be such a hassle")?;
        // w.finalize() is not called. Should be fine.
        drop(w);

        _ = finalize;
    });

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

        reader.search_scored("machine learning", &BitmapFilter::all_match(), &mut r)?;
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

        reader.search_scored(
            "machine learning",
            &BitmapFilter::partial_match(&[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]),
            &mut r,
        )?;
        r.sort_by_key(|result| OrderedFloat(-result.score));
        let rows = r.iter().map(|result| result.doc_id).collect::<Vec<_>>();
        assert_eq!(rows, vec![2, 0, 6, 1, 3, 4, 5, 9]);

        reader.search_scored(
            "machine learning",
            &BitmapFilter::partial_match(&[0, 0, 0, 1, 1, 1, 1, 1, 1, 1]),
            &mut r,
        )?;
        r.sort_by_key(|result| OrderedFloat(-result.score));
        let rows = r.iter().map(|result| result.doc_id).collect::<Vec<_>>();
        assert_eq!(rows, vec![6, 3, 4, 5, 9]);

        reader.search_scored(
            "machine learning",
            &BitmapFilter::partial_match(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            &mut r,
        )?;
        r.sort_by_key(|result| OrderedFloat(-result.score));
        let rows = r.iter().map(|result| result.doc_id).collect::<Vec<_>>();
        assert!(rows.is_empty());
    });

    #[test]
    fn test_on_disk_abandon_cleanup() -> Result<()> {
        let dir_guard = tempfile::tempdir()?;
        let mut w = IndexWriterOnDisk::new(
            "STANDARD_V1",
            dir_guard.path().join("test.index").to_str().unwrap(),
        )?;
        w.add_document("Being too popular can be such a hassle")?;
        assert!(dir_guard.path().read_dir()?.count() > 0);

        // w.finalize() is not called. Immediate files should be cleaned up.
        drop(w);
        assert_eq!(0, dir_guard.path().read_dir()?.count());

        Ok(())
    }

    #[test]
    fn test_on_disk_overwrite() -> Result<()> {
        let dir_guard = tempfile::tempdir()?;
        let mut w = IndexWriterOnDisk::new(
            "STANDARD_V1",
            dir_guard.path().join("test.index").to_str().unwrap(),
        )?;
        w.add_document("Being too popular can be such a hassle")?;
        w.finalize()?;

        // There should be only one test.index file in the directory.
        assert_eq!(1, dir_guard.path().read_dir()?.count());

        // Overwrite the index in serial is allowed. The later one takes effect.
        let mut w = IndexWriterOnDisk::new(
            "STANDARD_V1",
            dir_guard.path().join("test.index").to_str().unwrap(),
        )?;
        w.add_document("Who knew the people would adore me so much?")?;
        w.finalize()?;
        assert_eq!(1, dir_guard.path().read_dir()?.count());

        let reader = IndexReader::new_mmap(dir_guard.path().join("test.index").to_str().unwrap())?;
        let mut results = Vec::new();
        reader.search_no_score("popular", &BitmapFilter::all_match(), &mut results)?;
        assert!(results.is_empty());
        reader.search_no_score("people", &BitmapFilter::all_match(), &mut results)?;
        assert_eq!(results, vec![0]);

        Ok(())
    }

    #[test]
    fn test_on_disk_open_invalid() -> Result<()> {
        let dir_guard = tempfile::tempdir()?;

        // Open a non-index file should fail.
        let reader = IndexReader::new_mmap(dir_guard.path().join("test.index2").to_str().unwrap());
        assert!(reader.is_err());

        Ok(())
    }

    #[test]
    #[rustfmt::skip]
    fn test_tokenizer() -> Result<()> {
        let mut w = IndexWriterInMemory::new("MULTILINGUAL_V1")?;
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

        let reader = IndexReader::new_memory(w.finalize()?)?;

        let mut results = Vec::new();
        reader.search_no_score("中国", &BitmapFilter::all_match(), &mut results)?;
        assert_eq!(results, vec![1, 9]);
        reader.search_no_score("年", &BitmapFilter::all_match(), &mut results)?;
        assert_eq!(results, vec![12]);
        reader.search_no_score("今年", &BitmapFilter::all_match(), &mut results)?;
        assert_eq!(results, vec![6]);
        reader.search_no_score("报道图片", &BitmapFilter::all_match(), &mut results)?;
        assert_eq!(results, vec![4]);
        reader.search_no_score("中国年", &BitmapFilter::all_match(), &mut results)?;
        assert_eq!(results, vec![1, 9, 12]);

        Ok(())
    }
}
