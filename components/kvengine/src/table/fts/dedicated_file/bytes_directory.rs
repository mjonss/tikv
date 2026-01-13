// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io,
    ops::{Deref, Range},
    path::Path,
    sync::Arc,
};

use anyhow::{Result, bail};
use bytes::Bytes;
use tantivy::directory::{
    Directory, DirectoryLock, FileHandle, FileSlice, Lock, OwnedBytes, WatchCallback, WatchHandle,
    WritePtr,
    error::{DeleteError, LockError, OpenReadError, OpenWriteError},
};

// TODO: Move into clara_fts crate.

/// A read-only Tantivy directory backed by Bytes.
#[derive(Debug, Clone, Default)]
#[allow(clippy::upper_case_acronyms)]
pub struct BytesDirRO {
    pub meta_json: Bytes,
    pub managed_json: Bytes,
    pub term: Bytes,
    pub idx: Bytes,
    pub pos: Bytes,
    pub store: Bytes,
    pub fast: Bytes,
    pub fieldnorm: Bytes,
}

impl BytesDirRO {
    /// Gets the corresponding memory slice for a given file path.
    fn get_file_data(&self, path: &Path) -> Option<&Bytes> {
        let file_name = path.file_name()?.to_str()?;

        let bytes = match file_name {
            s if s.ends_with(".term") => &self.term,
            s if s.ends_with(".idx") => &self.idx,
            s if s.ends_with(".pos") => &self.pos,
            s if s.ends_with(".store") => &self.store,
            s if s.ends_with(".fast") => &self.fast,
            s if s.ends_with(".fieldnorm") => &self.fieldnorm,
            "meta.json" => &self.meta_json,
            ".managed.json" => &self.managed_json,
            _ => return None,
        };

        if bytes.is_empty() { None } else { Some(bytes) }
    }

    pub fn from_directory<D: tantivy::Directory + Clone>(
        dir: &clara_fts::TrackedDirectory<D>,
    ) -> Result<Self> {
        let mut r = BytesDirRO::default();
        let files_snapshot = dir.all_files();
        for entry in &files_snapshot {
            let path = Path::new(&entry);
            let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            if file_name == "meta.json" {
                if !r.meta_json.is_empty() {
                    bail!("duplicate meta.json file");
                }
                r.meta_json = Bytes::from(dir.atomic_read(path)?);
            } else if file_name == ".managed.json" {
                if !r.managed_json.is_empty() {
                    bail!("duplicate .managed.json file");
                }
                r.managed_json = Bytes::from(dir.atomic_read(path)?);
            } else if file_name.ends_with(".term") {
                if !r.term.is_empty() {
                    bail!("duplicate .term file {:?}", entry);
                }
                r.term = Bytes::from(dir.atomic_read(path)?);
            } else if file_name.ends_with(".idx") {
                if !r.idx.is_empty() {
                    bail!("duplicate .idx file {:?}", entry);
                }
                r.idx = Bytes::from(dir.atomic_read(path)?);
            } else if file_name.ends_with(".pos") {
                if !r.pos.is_empty() {
                    bail!("duplicate .pos file {:?}", entry);
                }
                r.pos = Bytes::from(dir.atomic_read(path)?);
            } else if file_name.ends_with(".store") {
                if !r.store.is_empty() {
                    bail!("duplicate .store file {:?}", entry);
                }
                r.store = Bytes::from(dir.atomic_read(path)?);
            } else if file_name.ends_with(".fast") {
                if !r.fast.is_empty() {
                    bail!("duplicate .fast file {:?}", entry);
                }
                r.fast = Bytes::from(dir.atomic_read(path)?);
            } else if file_name.ends_with(".fieldnorm") {
                if !r.fieldnorm.is_empty() {
                    bail!("duplicate .fieldnorm file {:?}", entry);
                }
                r.fieldnorm = Bytes::from(dir.atomic_read(path)?);
            }
        }

        if r.meta_json.is_empty()
            || r.managed_json.is_empty()
            || r.term.is_empty()
            || r.idx.is_empty()
        {
            bail!(
                "missing tantivy files in tracked directory: {:?}",
                files_snapshot
            );
        }

        Ok(r)
    }
}

#[derive(Clone)]
struct BytesStableDeref(Bytes);

impl Deref for BytesStableDeref {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe impl stable_deref_trait::StableDeref for BytesStableDeref {}

#[derive(Debug)]
#[allow(clippy::upper_case_acronyms)]
struct BytesDirROFileHandle {
    data: Bytes,
}

impl std::ops::Deref for BytesDirROFileHandle {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl FileHandle for BytesDirROFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> Result<OwnedBytes, io::Error> {
        if range.end > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Read beyond file end",
            ));
        }
        let sliced_bytes = self.data.slice(range);
        let holder = BytesStableDeref(sliced_bytes);
        Ok(OwnedBytes::new(holder))
    }
}

struct DummyLock;

impl Drop for DummyLock {
    fn drop(&mut self) {
        // Does nothing.
    }
}

impl Directory for BytesDirRO {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let data = self
            .get_file_data(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?
            .clone();
        Ok(Arc::new(BytesDirROFileHandle { data }))
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        let handle = self.get_file_handle(path)?;
        Ok(FileSlice::new(handle))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        Err(DeleteError::FileDoesNotExist(path.to_path_buf()))
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.get_file_data(path).is_some())
    }

    fn open_write(&self, _path: &Path) -> Result<WritePtr, OpenWriteError> {
        Err(OpenWriteError::FileAlreadyExists(_path.to_path_buf()))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let data = self
            .get_file_data(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        Ok(data.to_vec())
    }

    fn atomic_write(&self, path: &Path, _data: &[u8]) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("Cannot write to BytesDirRO for {}", path.display()),
        ))
    }

    fn watch(&self, _watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    fn acquire_lock(&self, _lock: &Lock) -> Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(DummyLock)))
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use bytes::Bytes;
    use clara_fts::TrackedDirectory;
    use tantivy::{
        HasLen,
        directory::{Directory, WatchCallback, error::OpenReadError},
    };

    use super::*;

    #[test]
    fn test_from_directory_with_nested_tantivy_paths() {
        // Build a minimal tantivy index in a tracked RAM directory where files live
        // under segment subdirectories (e.g., seg_1/xxx.term). This mirrors the
        // real layout that previously failed to load.
        let dir = tantivy::directory::RamDirectory::default();
        let tracked = TrackedDirectory::wrap(dir.clone());

        let schema = {
            let mut builder = tantivy::schema::SchemaBuilder::new();
            builder.add_text_field("body", tantivy::schema::TEXT);
            builder.build()
        };

        let settings = tantivy::IndexSettings::default();
        let index = tantivy::Index::create(tracked.clone(), schema.clone(), settings).unwrap();
        // Tantivy enforces a 15MB minimum arena. Use the minimum to avoid
        // InvalidArgument.
        let mut writer = index.writer(15_000_000).unwrap();
        writer
            .add_document(tantivy::doc!(schema.get_field("body").unwrap() => "hello world"))
            .unwrap();
        writer.commit().unwrap();
        index.load_metas().unwrap();

        let bytes_dir = BytesDirRO::from_directory(&tracked).expect("should load tantivy files");

        // All core files must be populated; previously they were empty because
        // PathBuf::ends_with on a multi-component path never matched.
        assert!(!bytes_dir.term.is_empty());
        assert!(!bytes_dir.idx.is_empty());
        assert!(!bytes_dir.pos.is_empty());
        assert!(!bytes_dir.store.is_empty());

        // Ensure we can reopen an Index from the collected bytes to verify integrity.
        let reopened = bytes_dir.clone();
        let index = tantivy::Index::open(reopened).unwrap();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(searcher.num_docs(), 1);
    }

    /// Helper function to create test data for different file types
    fn create_test_data() -> BytesDirRO {
        BytesDirRO {
            meta_json: Bytes::from("meta.json content"),
            managed_json: Bytes::from(".managed.json content"),
            term: Bytes::from("segment.term content"),
            idx: Bytes::from("segment.idx content"),
            pos: Bytes::from("segment.pos content"),
            store: Bytes::from("segment.store content"),
            fast: Bytes::from("segment.fast content"),
            fieldnorm: Bytes::from("segment.fieldnorm content"),
        }
    }

    #[test]
    fn test_exists_method() {
        let directory = create_test_data();

        // Test existing files
        assert!(directory.exists(Path::new("meta.json")).unwrap());
        assert!(directory.exists(Path::new(".managed.json")).unwrap());
        assert!(directory.exists(Path::new("segment.term")).unwrap());
        assert!(directory.exists(Path::new("segment.idx")).unwrap());
        assert!(directory.exists(Path::new("segment.pos")).unwrap());
        assert!(directory.exists(Path::new("segment.store")).unwrap());
        assert!(directory.exists(Path::new("segment.fast")).unwrap());
        assert!(directory.exists(Path::new("segment.fieldnorm")).unwrap());

        // Test non-existing files
        assert!(!directory.exists(Path::new("nonexistent.file")).unwrap());
        assert!(!directory.exists(Path::new("segment.unknown")).unwrap());
    }

    #[test]
    fn test_exists_with_hash_prefixes() {
        // Create test data with hash prefixes
        let directory = BytesDirRO {
            meta_json: Bytes::new(),
            managed_json: Bytes::new(),
            term: Bytes::from("term content"),
            idx: Bytes::from("idx content"),
            pos: Bytes::from("pos content"),
            store: Bytes::from("store content"),
            fast: Bytes::from("fast content"),
            fieldnorm: Bytes::from("fieldnorm content"),
        };

        // Test files with hash prefixes (Tantivy-generated filenames)
        assert!(directory.exists(Path::new("abc123.term")).unwrap());
        assert!(directory.exists(Path::new("def456.idx")).unwrap());
        assert!(directory.exists(Path::new("ghi789.pos")).unwrap());
        assert!(directory.exists(Path::new("jkl012.store")).unwrap());
        assert!(directory.exists(Path::new("mno345.fast")).unwrap());
        assert!(directory.exists(Path::new("pqr678.fieldnorm")).unwrap());
    }

    #[test]
    fn test_atomic_read() {
        let directory = create_test_data();

        // Test reading existing files
        let meta_content = directory.atomic_read(Path::new("meta.json")).unwrap();
        assert_eq!(meta_content, b"meta.json content");

        let managed_content = directory.atomic_read(Path::new(".managed.json")).unwrap();
        assert_eq!(managed_content, b".managed.json content");

        let term_content = directory.atomic_read(Path::new("segment.term")).unwrap();
        assert_eq!(term_content, b"segment.term content");

        // Test reading with hash prefixes
        let idx_content = directory.atomic_read(Path::new("abc123.idx")).unwrap();
        assert_eq!(idx_content, b"segment.idx content");
    }

    #[test]
    fn test_atomic_read_nonexistent_file() {
        let directory = create_test_data();

        // Test reading non-existent file
        let result = directory.atomic_read(Path::new("nonexistent.file"));
        assert!(matches!(result, Err(OpenReadError::FileDoesNotExist(_))));
    }

    #[test]
    fn test_get_file_handle() {
        let directory = create_test_data();

        // Test getting file handle for existing file
        let handle = directory.get_file_handle(Path::new("meta.json")).unwrap();
        assert_eq!(handle.len(), b"meta.json content".len());

        // Test getting file handle for non-existent file
        let result = directory.get_file_handle(Path::new("nonexistent.file"));
        assert!(matches!(result, Err(OpenReadError::FileDoesNotExist(_))));
    }

    #[test]
    fn test_open_read() {
        let directory = create_test_data();

        // Test opening existing file for reading
        let file_slice = directory.open_read(Path::new("segment.term")).unwrap();
        assert_eq!(file_slice.len(), b"segment.term content".len());

        // Test opening non-existent file
        let result = directory.open_read(Path::new("nonexistent.file"));
        assert!(matches!(result, Err(OpenReadError::FileDoesNotExist(_))));
    }

    #[test]
    fn test_read_only_operations() {
        let directory = create_test_data();

        // Test that write operations are not supported
        let write_result = directory.open_write(Path::new("test.file"));
        assert!(write_result.is_err());

        let atomic_write_result = directory.atomic_write(Path::new("test.file"), b"data");
        assert!(atomic_write_result.is_err());

        // Test that delete operations are not supported
        let delete_result = directory.delete(Path::new("meta.json"));
        assert!(delete_result.is_err());
    }

    #[test]
    fn test_file_handle_read_bytes() {
        let directory = create_test_data();

        let handle = directory
            .get_file_handle(Path::new("segment.term"))
            .unwrap();
        let content = b"segment.term content";

        // Test reading full content
        let full_read = handle.read_bytes(0..content.len()).unwrap();
        assert_eq!(full_read.as_slice(), content);

        // Test reading partial content
        let partial_read = handle.read_bytes(0..7).unwrap();
        assert_eq!(partial_read.as_slice(), b"segment");

        // Test reading beyond file end
        let beyond_read = handle.read_bytes(0..content.len() + 10);
        beyond_read.unwrap_err();
    }

    #[test]
    fn test_directory_sync_and_watch() {
        let directory = create_test_data();

        // Test sync_directory (should always succeed for read-only directory)
        directory.sync_directory().unwrap();

        // Test watch (should return empty handle for immutable directory)
        let watch_callback = WatchCallback::new(|| {});
        let watch_handle = directory.watch(watch_callback).unwrap();
        // The watch handle should be valid but empty
        drop(watch_handle);
    }

    // Note: acquire_lock test is skipped as it requires complex Lock construction
    // The functionality is tested indirectly through the Directory trait
    // implementation

    #[test]
    fn test_bounds_checking() {
        // Test with valid files only
        let directory = BytesDirRO {
            meta_json: Bytes::from("sma"),
            managed_json: Bytes::new(),
            term: Bytes::new(),
            idx: Bytes::new(),
            pos: Bytes::new(),
            store: Bytes::new(),
            fast: Bytes::new(),
            fieldnorm: Bytes::new(),
        };

        // meta.json should be present
        assert!(directory.exists(Path::new("meta.json")).unwrap());

        // segment.term should not be present (empty Bytes)
        assert!(!directory.exists(Path::new("segment.term")).unwrap());
    }

    #[test]
    fn test_zero_size_files() {
        // Test with empty and normal files
        let directory = BytesDirRO {
            meta_json: Bytes::new(),
            managed_json: Bytes::new(),
            term: Bytes::from("content"),
            idx: Bytes::new(),
            pos: Bytes::new(),
            store: Bytes::new(),
            fast: Bytes::new(),
            fieldnorm: Bytes::new(),
        };

        // A file with empty Bytes should not exist (get_file_bytes returns None)
        assert!(!directory.exists(Path::new("meta.json")).unwrap());

        // Normal file should be present
        assert!(directory.exists(Path::new("segment.term")).unwrap());
    }

    #[test]
    fn test_unknown_file_types() {
        // Test with known and unknown file types
        let directory = create_test_data();

        // Unknown file type should NOT be found by exists() (returns false)
        assert!(!directory.exists(Path::new("unknown.file")).unwrap());
    }

    #[test]
    fn test_clone_directory() {
        let directory = create_test_data();

        // Clone the directory
        let cloned_directory = directory.clone();

        // Both directories should have the same files
        assert!(directory.exists(Path::new("meta.json")).unwrap());
        assert!(cloned_directory.exists(Path::new("meta.json")).unwrap());

        // Both should be able to read the same content
        let original_content = directory.atomic_read(Path::new("meta.json")).unwrap();
        let cloned_content = cloned_directory
            .atomic_read(Path::new("meta.json"))
            .unwrap();
        assert_eq!(original_content, cloned_content);
    }

    #[test]
    fn test_invalid_path_handling() {
        let directory = create_test_data();

        // Test with invalid path (no filename)
        let result = directory.atomic_read(Path::new(""));
        assert!(matches!(result, Err(OpenReadError::FileDoesNotExist(_))));

        // Test with path that has no file extension
        let result = directory.atomic_read(Path::new("noextension"));
        assert!(matches!(result, Err(OpenReadError::FileDoesNotExist(_))));
    }
}
