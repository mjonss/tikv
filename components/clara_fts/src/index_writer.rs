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

use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{anyhow, bail, Context, Result};
use tantivy::{
    directory::{Directory, MmapDirectory, RamDirectory},
    schema::Schema,
};

use crate::{MergedFileFromDirectory, TrackedDirectory};

static INDEX_IMMEDIATE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct TantivyIndexWriter<D: Directory> {
    index_writer: tantivy::SingleSegmentIndexWriter,
    dir: D,
}

impl<D: Directory> TantivyIndexWriter<D> {
    pub fn build_schema(tokenizer_name: &str) -> Result<Schema> {
        let mut schema_builder = Schema::builder();
        let field_body = schema_builder.add_text_field(
            "body",
            // Intentionally not stored: callers only need doc ids / scores.
            tantivy::schema::TextOptions::default().set_indexing_options(
                tantivy::schema::TextFieldIndexing::default()
                    .set_tokenizer(tokenizer_name)
                    .set_fieldnorms(true)
                    .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions),
            ),
        );
        if field_body.field_id() != crate::FIELD_BODY.field_id() {
            bail!("Unexpected field id for body");
        }
        Ok(schema_builder.build())
    }

    fn new(tokenizer_name: &str, dir: D) -> Result<Self> {
        let schema = Self::build_schema(tokenizer_name)?;
        let index_writer = tantivy::IndexBuilder::new()
            .tokenizers(crate::tokenizer::TOKENIZERS.clone())
            .schema(schema)
            .single_segment_index_writer(dir.box_clone(), 32_000_000_000)?;
        Ok(Self { index_writer, dir })
    }

    pub fn add_document(&mut self, body: &str) -> Result<()> {
        self.index_writer
            .add_document(tantivy::doc!(crate::FIELD_BODY => body))?;
        Ok(())
    }

    pub fn add_null(&mut self) -> Result<()> {
        self.index_writer.add_document(tantivy::doc!())?;
        Ok(())
    }

    pub fn finalize(self) -> Result<tantivy::Index> {
        Ok(self.index_writer.finalize()?)
    }

    pub fn finalize_as_dir(self) -> Result<D> {
        self.index_writer.finalize()?;
        Ok(self.dir)
    }
}

impl TantivyIndexWriter<TrackedDirectory<RamDirectory>> {
    pub fn new_in_memory(tokenizer_name: &str) -> Result<Self> {
        let dir = TrackedDirectory::wrap(RamDirectory::default());
        TantivyIndexWriter::new(tokenizer_name, dir)
    }
}

/// Helper function to build a Tantivy index in memory for testing purposes.
/// The index will contain the provided documents as the body field.
/// Empty documents will be represented as null documents.
pub fn index_for_test(docs: &[&str]) -> Result<TantivyIndexWriter<TrackedDirectory<RamDirectory>>> {
    let mut writer = TantivyIndexWriter::new_in_memory("STANDARD_V1")?;
    if docs.is_empty() {
        writer.add_null()?;
    } else {
        for doc in docs {
            if doc.is_empty() {
                writer.add_null()?;
            } else {
                writer.add_document(doc)?;
            }
        }
    }
    Ok(writer)
}

pub struct IndexWriterOnDisk {
    index_path: PathBuf,
    index_immediate_path: PathBuf,
    merging_directory: Option<crate::MergedFileFromMmapDirectory>,
    internal_writer: Option<TantivyIndexWriter<TrackedDirectory<MmapDirectory>>>,
}

impl IndexWriterOnDisk {
    /// Creates a new `IndexWriterOnDisk` for on-disk index.
    /// If the index file already exists, it will be overwritten.
    /// Immediate index files will be all stored on disk before merging into a
    /// single file.
    pub fn new(tokenizer_name: &str, index_path: &str) -> Result<Self> {
        let immediate_path = PathBuf::from(format!(
            "{}-immediate-{}-{}-{}",
            index_path,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis(),
            INDEX_IMMEDIATE_COUNTER.fetch_add(1, Ordering::SeqCst),
            std::iter::repeat_with(fastrand::alphanumeric)
                .take(10)
                .collect::<String>()
        ));
        if immediate_path.exists() {
            // This shall not happen.
            bail!(
                "Failed to create immediate directory {}: already exists",
                immediate_path.display()
            );
        }

        let merging_dir = crate::MergedFileFromMmapDirectory::new(&immediate_path)?;
        let dir = merging_dir.tracked_directory().clone();
        Ok(Self {
            index_path: PathBuf::from(index_path),
            index_immediate_path: immediate_path,
            merging_directory: Some(merging_dir),
            internal_writer: Some(TantivyIndexWriter::new(tokenizer_name, dir)?),
        })
    }

    /// Adds a document to the index.
    /// The first document will have ID = 0, the second will have ID = 1, and so
    /// on. Parameters use simple primitive types for FFI compatibility.
    pub fn add_document(&mut self, body: &str) -> Result<()> {
        self.internal_writer
            .as_mut()
            .ok_or_else(|| anyhow!("IndexWriterOnDisk is already finalized"))?
            .add_document(body)
    }

    /// Adds a null document to the index, which will also occupy an ID.
    pub fn add_null(&mut self) -> Result<()> {
        self.internal_writer
            .as_mut()
            .ok_or_else(|| anyhow!("IndexWriterOnDisk is already finalized"))?
            .add_null()
    }

    /// Finalizes the index. If this function is not called before drop, the
    /// index will be discarded and index file will not be actually created.
    pub fn finalize(&mut self) -> Result<()> {
        if self.internal_writer.is_none() || self.merging_directory.is_none() {
            bail!("IndexWriterOnDisk is already finalized");
        }

        self.internal_writer.take().unwrap().finalize()?;

        let file = fs::File::create(&self.index_path)?;
        self.merging_directory
            .take()
            .unwrap()
            .merge_files(file)
            .with_context(|| {
                format!(
                    "Failed to merge immediate index {} to index file {}",
                    self.index_immediate_path.display(),
                    self.index_path.display()
                )
            })?;

        Ok(())
    }
}

impl Drop for IndexWriterOnDisk {
    fn drop(&mut self) {
        drop(self.internal_writer.take());
        drop(self.merging_directory.take());
    }
}

pub struct IndexWriterInMemory {
    merging_directory: Option<crate::MergedFileFromRamDirectory>,
    internal_writer: Option<TantivyIndexWriter<TrackedDirectory<RamDirectory>>>,
}

impl IndexWriterInMemory {
    /// Creates a new `IndexWriterInMemory` for in-memory index. The index will
    /// be stored in memory and will not be persisted to disk.
    pub fn new(tokenizer_name: &str) -> Result<Self> {
        let merging_dir = crate::MergedFileFromRamDirectory::new();
        let dir = merging_dir.tracked_directory().clone();
        Ok(Self {
            merging_directory: Some(merging_dir),
            internal_writer: Some(TantivyIndexWriter::new(tokenizer_name, dir)?),
        })
    }

    /// Adds a document to the index.
    /// The first document will have ID = 0, the second will have ID = 1, and so
    /// on. Parameters use simple primitive types for FFI compatibility.
    pub fn add_document(&mut self, body: &str) -> Result<()> {
        if self.internal_writer.is_none() {
            bail!("IndexWriterInMemory is already finalized");
        }
        self.internal_writer.as_mut().unwrap().add_document(body)
    }

    /// Adds a null document to the index, which will also occupy an ID.
    pub fn add_null(&mut self) -> Result<()> {
        if self.internal_writer.is_none() {
            bail!("IndexWriterInMemory is already finalized");
        }
        self.internal_writer.as_mut().unwrap().add_null()
    }

    /// Finalizes the index. If this function is not called before drop, the
    /// index will be discarded and index file will not be actually created.
    pub fn finalize(&mut self) -> Result<Vec<u8>> {
        if self.internal_writer.is_none() || self.merging_directory.is_none() {
            bail!("IndexWriterInMemory is already finalized");
        }

        self.internal_writer.take().unwrap().finalize()?;

        let buffer = self
            .merging_directory
            .take()
            .unwrap()
            .merge_files_to_buffer()?;

        Ok(buffer)
    }
}

impl Drop for IndexWriterInMemory {
    fn drop(&mut self) {
        drop(self.internal_writer.take());
        drop(self.merging_directory.take());
    }
}

// Unit tests are placed in index_reader.rs
