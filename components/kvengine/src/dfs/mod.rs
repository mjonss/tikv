// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod azure;
mod config;
mod metrics;
mod remote_cached;
mod s3;

use std::{
    any::Any,
    convert::TryFrom,
    fmt::{Debug, Display, Formatter},
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    ops::Deref,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    result,
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};

use async_trait::async_trait;
pub use azure::*;
use bytes::Bytes;
pub use config::{
    AzureConfig as DFSAzureConfig, Config as DFSConfig, ConnOptions as DFSConnOptions,
};
use engine_traits::{GetObjectOptions, ListObjectContent, ObjectCacheWithHook, ObjectStorage};
use farmhash::fingerprint64;
use file_system;
use metrics::*;
use regex::Regex;
pub use remote_cached::*;
pub use s3::*;
use thiserror::Error;
use tikv_util::time::Instant;
use tokio::runtime::Runtime;

use crate::{
    IoContext,
    table::{
        TxnChunk,
        blobtable::blobtable::BlobTable,
        columnar::ColumnarFileFooter,
        fts::{DedicatedFileFooter, PackedFileFooter},
        schema_file::SchemaFileFooter,
        sstable::{L0Table, SsTable},
    },
};

// DFS represents a distributed file system.
#[async_trait]
pub trait Dfs: Any + Sync + Send {
    /// read_file reads the whole file to memory.
    /// It can be used by remote compaction server that doesn't have local disk.
    async fn read_file(&self, file_id: u64, opts: Options) -> Result<Bytes>;

    /// Create creates a new File.
    /// The shard_id and shard_ver can be used determine where to write the
    /// file.
    async fn create(&self, file_id: u64, data: Bytes, opts: Options) -> Result<()>;

    /// remove removes the file from the DFS.
    /// `file_len` is used to choose proper storage class for cost
    /// efficiency, in bytes.
    /// `None` if file_len is unknown when invoke this method.
    async fn remove(&self, file_id: u64, file_len: Option<u64>, opts: Options);

    /// Remove the file from DFS permanently.
    async fn permanently_remove(&self, file_id: u64, opts: Options) -> Result<()>;

    /// get_runtime gets the tokio runtime for the DFS.
    fn get_runtime(&self) -> &tokio::runtime::Runtime;

    /// prefix returns the prefix used by the DFS.
    fn get_prefix(&self) -> String;

    /// Lists objects in the DFS.
    ///
    /// Returns a tuple containing:
    /// - Vec<ListObjectContent>: List of objects
    /// - bool: has_more (deprecated, use next_start_after)
    /// - Option<String>: next_start_after for pagination
    async fn list(
        &self,
        _start_after: &str,
        _prefix: Option<&str>,
        _max_keys: Option<u32>,
    ) -> Result<(Vec<ListObjectContent>, bool, Option<String>)> {
        Err(Error::Other("list is unsupported".to_string()))
    }

    async fn get_object(
        &self,
        _key: String,
        _file_name: String,
        _opts: GetObjectOptions,
    ) -> Result<Bytes> {
        Err(Error::Other("get_object is unsupported".to_string()))
    }

    async fn get_object_with_cache(
        &self,
        key: String,
        file_name: String,
        opts: GetObjectOptions,
        cache_with_hook: Option<&ObjectCacheWithHook>,
    ) -> crate::dfs::Result<Bytes> {
        if let Some(cache_with_hook) = cache_with_hook {
            let ObjectCacheWithHook { cache, hook } = cache_with_hook;
            let with = async {
                let data = self.get_object(key.clone(), file_name, opts).await?;
                hook.invoke(data).map_err(Error::Hook)
            };
            return cache.get_or_insert_async(&key, with).await;
        }
        self.get_object(key, file_name, opts).await
    }

    async fn get_object_to_path(
        &self,
        _key: String,
        _file_name: String,
        _opts: GetObjectOptions,
        _path: &Path,
    ) -> Result<u64> {
        Err(Error::Other(
            "get_object_to_path is unsupported".to_string(),
        ))
    }

    async fn put_object(&self, _key: String, _data: Bytes, _file_name: String) -> Result<()> {
        Err(Error::Other("put_object is unsupported".to_string()))
    }

    async fn put_object_with_storage_class(
        &self,
        key: String,
        data: Bytes,
        file_name: String,
        _storage_class: StorageClass,
    ) -> Result<()> {
        self.put_object(key, data, file_name).await
    }

    async fn exist(&self, _key: String, _file_name: String) -> Result<bool> {
        Err(Error::Other("exist is unsupported".to_string()))
    }

    async fn retain_file(&self, _file_key: &str) -> Result<()> {
        Err(Error::Other("retain_file is unsupported".to_string()))
    }

    async fn delete_object(&self, _key: String, _file_name: String) -> Result<()> {
        Err(Error::Other("delete_object is unsupported".to_string()))
    }

    async fn object_size(&self, _key: String, _file_name: String) -> Result<u64> {
        Err(Error::Other("object_size is unsupported".to_string()))
    }

    async fn list_folders(&self, _prefix: &str, _delimiter: Option<&str>) -> Result<Vec<String>> {
        Err(Error::Other("list_folders is unsupported".to_string()))
    }

    async fn is_removed(&self, _file_key: &str) -> Result<bool> {
        Err(Error::Other("is_removed is unsupported".to_string()))
    }

    /// Synchronously lists objects in the DFS.
    ///
    /// Returns a tuple containing:
    /// - Vec<ListObjectContent>: List of objects
    /// - Option<String>: next_start_after for pagination
    fn list_objects(
        &self,
        _start_after: &str,
        _prefix: Option<&str>,
        _max_keys: Option<u32>,
    ) -> std::result::Result<(Vec<ListObjectContent>, Option<String>), String> {
        Err("list_objects is unsupported".to_string())
    }

    fn put_objects(&self, _objects: Vec<(String, Bytes)>) -> std::result::Result<(), String> {
        Err("put_objects is unsupported".to_string())
    }

    fn file_key(&self, file_id: u64, file_type: FileType) -> String {
        let idx = (fingerprint64(file_id.to_le_bytes().as_slice())) as u8;
        let prefix = self.get_prefix();
        match file_type {
            FileType::Sst => {
                format!("{}/{:02x}/{:016x}.sst", prefix, idx, file_id)
            }
            FileType::Blob => {
                format!("{}/blob/{:02x}/{:016x}.blob", prefix, idx, file_id)
            }
            FileType::TxnChunk => {
                format!("{}/txn/{:02x}/{:016x}.txn", prefix, idx, file_id)
            }
            FileType::Schema => {
                format!("{}/schema/{:02x}/{:016x}.schema", prefix, idx, file_id)
            }
            FileType::Columnar => {
                format!("{}/col/{:02x}/{:016x}.col", prefix, idx, file_id)
            }
            FileType::VectorIndex => {
                format!("{}/vec/{:02x}/{:016x}.vec", prefix, idx, file_id)
            }
            FileType::FtsPackedFile => {
                format!("{}/fts/{:02x}/{:016x}.ftspack", prefix, idx, file_id)
            }
            FileType::FtsDedicatedFile => {
                format!("{}/fts/{:02x}/{:016x}.ftsded", prefix, idx, file_id)
            }
        }
    }
}

pub fn new_dfs_from_config(conf: DFSConfig) -> Arc<dyn Dfs> {
    match conf.backend.to_ascii_lowercase().as_str() {
        "azure" => Arc::new(AzureFs::new_from_config(conf)),
        _ => Arc::new(S3Fs::new_from_config(conf)),
    }
}

pub fn new_object_storage_from_config(conf: DFSConfig) -> Box<dyn ObjectStorage> {
    match conf.backend.to_ascii_lowercase().as_str() {
        "azure" => Box::new(AzureFs::new_from_config(conf)),
        _ => Box::new(S3Fs::new_from_config(conf)),
    }
}

const REMOVE_DELAY: Duration = Duration::from_secs(90);

pub struct InMemFs {
    files: dashmap::DashMap<u64, Bytes>,
    pending_remove: dashmap::DashMap<u64, Instant>,
    runtime: tokio::runtime::Runtime,
}

impl Default for InMemFs {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemFs {
    pub fn new() -> Self {
        Self {
            files: Default::default(),
            pending_remove: Default::default(),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap(),
        }
    }
}

#[async_trait]
impl Dfs for InMemFs {
    async fn read_file(&self, file_id: u64, opts: Options) -> Result<Bytes> {
        if let Some(file) = self.files.get(&file_id).as_deref() {
            if let Some(end_off) = opts.end_off {
                return Ok(file.slice(opts.start_off as usize..end_off as usize));
            } else {
                return Ok(file.slice(opts.start_off as usize..));
            }
        }
        Err(Error::NotExists(file_id))
    }

    async fn create(&self, file_id: u64, data: Bytes, _opts: Options) -> Result<()> {
        self.files.insert(file_id, data);
        Ok(())
    }

    async fn remove(&self, file_id: u64, _file_len: Option<u64>, _opts: Options) {
        if self.pending_remove.contains_key(&file_id) {
            return;
        }
        let now = Instant::now_coarse();
        self.pending_remove.insert(file_id, now);
        self.pending_remove.retain(|id, &mut remove_time| {
            if now.saturating_duration_since(remove_time) > REMOVE_DELAY {
                self.files.remove(id);
                false
            } else {
                true
            }
        });
    }

    async fn permanently_remove(&self, file_id: u64, _opts: Options) -> Result<()> {
        self.files.remove(&file_id);
        Ok(())
    }

    fn get_runtime(&self) -> &Runtime {
        &self.runtime
    }

    fn get_prefix(&self) -> String {
        "in_mem_fs".to_string()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum FileType {
    Sst = 0,
    TxnChunk = 1,
    Schema = 2,
    Columnar = 3,
    Blob = 4,
    VectorIndex = 5,
    FtsPackedFile = 6,
    FtsDedicatedFile = 7,
}

impl FileType {
    pub fn from_u8(t: u8) -> Option<FileType> {
        match t {
            0 => Some(FileType::Sst),
            1 => Some(FileType::TxnChunk),
            2 => Some(FileType::Schema),
            3 => Some(FileType::Columnar),
            4 => Some(FileType::Blob),
            5 => Some(FileType::VectorIndex),
            6 => Some(FileType::FtsPackedFile),
            7 => Some(FileType::FtsDedicatedFile),
            _ => None,
        }
    }

    pub fn suffix(&self) -> &'static str {
        match self {
            FileType::Sst => "sst",
            FileType::TxnChunk => "txn",
            FileType::Schema => "schema",
            FileType::Columnar => "col",
            FileType::Blob => "blob",
            FileType::VectorIndex => "vec",
            FileType::FtsPackedFile => "ftspack",
            FileType::FtsDedicatedFile => "ftsded",
        }
    }

    pub fn footer_size(&self) -> usize {
        match self {
            FileType::Sst => std::cmp::max(SsTable::footer_size(), L0Table::footer_size()),
            FileType::TxnChunk => TxnChunk::footer_size(),
            FileType::Schema => SchemaFileFooter::footer_size(),
            FileType::Columnar => ColumnarFileFooter::compute_size(),
            FileType::Blob => BlobTable::footer_size(),
            FileType::VectorIndex => unimplemented!(), // TODO
            FileType::FtsPackedFile => PackedFileFooter::footer_size(),
            FileType::FtsDedicatedFile => DedicatedFileFooter::footer_size(),
        }
    }
}

impl Display for FileType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.suffix())
    }
}

impl TryFrom<&str> for FileType {
    type Error = String;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            "sst" => FileType::Sst,
            "txn" => FileType::TxnChunk,
            "schema" => FileType::Schema,
            "col" => FileType::Columnar,
            "blob" => FileType::Blob,
            "vec" => FileType::VectorIndex,
            "ftspack" => FileType::FtsPackedFile,
            "ftsded" => FileType::FtsDedicatedFile,
            _ => {
                return Err(format!("invalid suffix: {value}"));
            }
        })
    }
}

#[derive(Clone)]
pub struct LocalFs {
    core: Arc<LocalFsCore>,
}

impl LocalFs {
    pub fn new(dir: &Path) -> Self {
        let core = Arc::new(LocalFsCore::new(dir));
        Self { core }
    }
    pub fn local_sst_file_path(&self, file_id: u64) -> PathBuf {
        self.dir.join(self.sst_filename(file_id))
    }
    pub fn local_blob_file_path(&self, file_id: u64) -> PathBuf {
        self.dir.join(self.blob_filename(file_id))
    }
    pub fn local_columnar_file_path(&self, file_id: u64) -> PathBuf {
        self.dir.join(self.columnar_filename(file_id))
    }
    pub fn local_vector_index_file_path(&self, file_id: u64) -> PathBuf {
        self.dir.join(self.vector_index_filename(file_id))
    }
    pub fn local_ftspack_file_path(&self, file_id: u64) -> PathBuf {
        self.dir.join(self.ftspack_filename(file_id))
    }
    pub fn local_ftsdedicated_file_path(&self, file_id: u64) -> PathBuf {
        self.dir.join(self.ftsdedicated_filename(file_id))
    }
    pub fn sst_filename(&self, file_id: u64) -> PathBuf {
        PathBuf::from(format!("{:016x}.sst", file_id))
    }
    pub fn blob_filename(&self, file_id: u64) -> PathBuf {
        PathBuf::from(format!("{:016x}.blob", file_id))
    }
    pub fn columnar_filename(&self, file_id: u64) -> PathBuf {
        PathBuf::from(format!("{:016x}.col", file_id))
    }
    pub fn vector_index_filename(&self, file_id: u64) -> PathBuf {
        PathBuf::from(format!("{:016x}.vec", file_id))
    }
    pub fn ftsdedicated_filename(&self, file_id: u64) -> PathBuf {
        PathBuf::from(format!("{:016x}.ftsded", file_id))
    }
    pub fn ftspack_filename(&self, file_id: u64) -> PathBuf {
        PathBuf::from(format!("{:016x}.ftspack", file_id))
    }
    pub fn tmp_file_path(&self, file_id: u64) -> PathBuf {
        let tmp_id = self
            .tmp_file_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.dir.join(self.new_tmp_filename(file_id, tmp_id))
    }
    pub fn new_tmp_filename(&self, file_id: u64, tmp_id: u64) -> PathBuf {
        PathBuf::from(format!("{:016x}.{}.tmp", file_id, tmp_id))
    }
    pub fn local_txn_chunk_path(&self, id: u64) -> PathBuf {
        self.dir.join("txn").join(format!("{:016x}.txn", id))
    }
    pub fn local_schema_file_path(&self, id: u64) -> PathBuf {
        self.dir.join("schema").join(format!("{:016x}.schema", id))
    }
    pub fn local_file_path(&self, file_id: u64, file_type: FileType) -> PathBuf {
        match file_type {
            FileType::Sst => self.local_sst_file_path(file_id),
            FileType::TxnChunk => self.local_txn_chunk_path(file_id),
            FileType::Schema => self.local_schema_file_path(file_id),
            FileType::Columnar => self.local_columnar_file_path(file_id),
            FileType::Blob => self.local_blob_file_path(file_id),
            FileType::VectorIndex => self.local_vector_index_file_path(file_id),
            FileType::FtsPackedFile => self.local_ftspack_file_path(file_id),
            FileType::FtsDedicatedFile => self.local_ftsdedicated_file_path(file_id),
        }
    }
}

impl Deref for LocalFs {
    type Target = LocalFsCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

pub struct LocalFsCore {
    dir: PathBuf,
    tmp_file_id: AtomicU64,
    runtime: tokio::runtime::Runtime,
}

impl LocalFsCore {
    pub fn new(dir: &Path) -> Self {
        if !dir.exists() {
            std::fs::create_dir_all(dir).unwrap();
        }
        if !dir.is_dir() {
            panic!("path {:?} is not dir", dir);
        }
        Self {
            dir: dir.to_owned(),
            tmp_file_id: AtomicU64::new(0),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(8)
                .enable_all()
                .build()
                .unwrap(),
        }
    }
}

#[async_trait]
impl Dfs for LocalFs {
    async fn read_file(&self, file_id: u64, opts: Options) -> Result<Bytes> {
        let local_file_name = self.local_file_path(file_id, opts.file_type);
        let mut fd = std::fs::File::open(local_file_name).dfs_ctx(file_id, "open")?;
        let buf = if let Some(end_off) = opts.end_off {
            let mut buf = vec![0; (end_off - opts.start_off) as usize];
            fd.read_exact_at(&mut buf, opts.start_off)
                .dfs_ctx(file_id, "read")?;
            buf
        } else {
            if opts.start_off > 0 {
                fd.seek(SeekFrom::Start(opts.start_off))
                    .dfs_ctx(file_id, "seek")?;
            }
            let mut reader = BufReader::new(fd);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).dfs_ctx(file_id, "read")?;
            buf
        };
        KVENGINE_DFS_THROUGHPUT_VEC
            .with_label_values(&["read"])
            .inc_by(buf.len() as u64);
        Ok(Bytes::from(buf))
    }

    async fn create(&self, file_id: u64, data: Bytes, opts: Options) -> Result<()> {
        let local_file_name = self.local_file_path(file_id, opts.file_type);
        let tmp_file_name = self.tmp_file_path(file_id);
        let mut file = std::fs::File::create(&tmp_file_name)?;
        let mut start_off = 0;
        let write_batch_size = 256 * 1024;
        while start_off < data.len() {
            let end_off = std::cmp::min(start_off + write_batch_size, data.len());
            file.write_all(&data[start_off..end_off])?;
            file.sync_data()?;
            start_off = end_off;
        }
        std::fs::rename(&tmp_file_name, local_file_name)?;
        file_system::sync_dir(&self.dir)?;
        KVENGINE_DFS_THROUGHPUT_VEC
            .with_label_values(&["write"])
            .inc_by(data.len() as u64);
        Ok(())
    }

    async fn remove(&self, file_id: u64, _file_len: Option<u64>, opts: Options) {
        let local_file_path = self.local_file_path(file_id, opts.file_type);
        if let Err(err) = std::fs::remove_file(local_file_path) {
            error!("failed to remove local file {:?}", err);
        }
    }

    async fn permanently_remove(&self, file_id: u64, opts: Options) -> Result<()> {
        self.remove(file_id, None, opts).await;
        Ok(())
    }

    fn get_runtime(&self) -> &Runtime {
        &self.runtime
    }

    fn get_prefix(&self) -> String {
        "local".to_string()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Options {
    pub file_type: FileType,
    pub shard_id: u64,
    pub shard_ver: u64,
    pub start_off: u64,
    pub end_off: Option<u64>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            file_type: FileType::Sst,
            shard_id: 0,
            shard_ver: 0,
            start_off: 0,
            end_off: None,
        }
    }
}

impl Options {
    #[must_use]
    pub fn with_shard(mut self, shard_id: u64, shard_ver: u64) -> Self {
        self.shard_id = shard_id;
        self.shard_ver = shard_ver;
        self
    }

    #[must_use]
    pub fn with_type(mut self, file_type: FileType) -> Self {
        self.file_type = file_type;
        self
    }

    #[must_use]
    pub fn with_start_off(mut self, start_off: u64) -> Self {
        self.start_off = start_off;
        self
    }

    #[must_use]
    pub fn with_end_off(mut self, end_off: u64) -> Self {
        self.end_off = Some(end_off);
        self
    }
}

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Error, Clone)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(String),
    #[error("File {0} not exists")]
    NotExists(u64),
    #[error("Txn Chunk {0} not exists")]
    TxnChunkNotExists(u64),
    #[error("S3 error {0}")]
    S3(String),
    #[error("Hook error {0}")]
    Hook(String),
    #[error("Other error {0}")]
    Other(String),
    #[error("The specified key {0} does not exist.")]
    NoSuchKey(String),
    #[error("Hyper error {0}")]
    Hyper(String),
    #[error("The DFS is read only")]
    ReadOnly,
}

impl From<io::Error> for Error {
    #[inline]
    fn from(e: io::Error) -> Error {
        Error::Io(e.to_string())
    }
}

impl From<hyper::Error> for Error {
    #[inline]
    fn from(e: hyper::Error) -> Error {
        Error::Hyper(e.to_string())
    }
}

impl<E: Debug> From<rusoto_core::RusotoError<E>> for Error {
    fn from(err: rusoto_core::RusotoError<E>) -> Self {
        Error::S3(format!("{:?}", err))
    }
}

pub trait ReservableWriter: std::io::Write {
    fn reserve_capacity(&mut self, additional: u64);
}

impl ReservableWriter for Vec<u8> {
    fn reserve_capacity(&mut self, additional: u64) {
        self.reserve(additional as usize);
    }
}

impl ReservableWriter for bytes::buf::Writer<bytes::BytesMut> {
    fn reserve_capacity(&mut self, additional: u64) {
        self.get_mut().reserve(additional as usize);
    }
}

impl ReservableWriter for std::fs::File {
    fn reserve_capacity(&mut self, _additional: u64) {}
}

impl<W: std::io::Write> ReservableWriter for std::io::BufWriter<W> {
    fn reserve_capacity(&mut self, _additional: u64) {}
}

// parse the sst file's suffix with format {idx}/{file_id}.sst
pub fn parse_sst_file_suffix(key: &str) -> String {
    let end_idx = key.len();
    let start_idx = end_idx - 4 - 16 - 1 - 2;
    let suffix = &key[start_idx..end_idx];
    suffix.to_string()
}

// Try to parse the sst file id from file key.
// Note: do NOT use in performance critical path as regex is used.
pub fn try_parse_all_file_id(key: &str) -> Option<(u64, FileType)> {
    try_parse_sst_file_id(key).or_else(|| try_parse_other_file_id(key))
}

// Expected file key format: "/{prefix}/{idx}/{file_id}.sst".
pub fn try_parse_sst_file_id(key: &str) -> Option<(u64, FileType)> {
    if !key.ends_with(".sst") {
        return None;
    }

    lazy_static::lazy_static! {
        static ref RE: Regex = Regex::new(r"/[0-9a-f]{2}/([0-9a-f]{16})\.sst$").unwrap();
    }
    let caps = RE.captures(key)?;
    Some((u64::from_str_radix(&caps[1], 16).unwrap(), FileType::Sst))
}

// Expected file key format:
// "/{prefix}/{file_type}/{idx}/{file_id}.{file_type}".
pub fn try_parse_other_file_id(key: &str) -> Option<(u64, FileType)> {
    lazy_static::lazy_static! {
        static ref RE: Regex = Regex::new(r"/(?<subdir>[a-z]+)/[0-9a-f]{2}/(?<fileid>[0-9a-f]{16})\.(?<filetype>[a-z]+)$").unwrap();
    }
    let caps = RE.captures(key)?;

    if caps["filetype"] != caps["subdir"] {
        return None;
    }
    let file_type = FileType::try_from(&caps["filetype"]).ok()?;
    let file_id = u64::from_str_radix(&caps["fileid"], 16).ok()?;
    Some((file_id, file_type))
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::MetadataExt;

    use super::*;
    use crate::dfs::LocalFs;

    #[test]
    fn test_local_fs() {
        ::test_util::init_log_for_test();

        let local_dir = tempfile::tempdir().unwrap();
        let file_data = "abcdefgh".to_string().into_bytes();
        let localfs = LocalFs::new(local_dir.path());
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let file_id = 321u64;
        let fs = localfs.clone();
        let file_data_clone = file_data.clone();
        let f = async move {
            match fs
                .create(
                    file_id,
                    bytes::Bytes::from(file_data_clone),
                    Options::default(),
                )
                .await
            {
                Ok(_) => {
                    tx.send(true).unwrap();
                    println!("create ok");
                }
                Err(err) => {
                    tx.send(false).unwrap();
                    println!("create error {:?}", err)
                }
            }
        };
        localfs.runtime.spawn(f);
        assert!(rx.recv().unwrap());
        let fs = localfs.clone();
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let f = async move {
            let opts = Options::default();
            match fs.read_file(file_id, opts).await {
                Ok(data) => {
                    assert_eq!(&data, &file_data);
                    tx.send(true).unwrap();
                    println!("prefetch ok");
                }
                Err(err) => {
                    tx.send(false).unwrap();
                    println!("prefetch failed {:?}", err)
                }
            }
        };
        localfs.runtime.spawn(f);
        assert!(rx.recv().unwrap());
        let local_file = localfs.local_sst_file_path(file_id);
        let fd = std::fs::File::open(&local_file).unwrap();
        let meta = fd.metadata().unwrap();
        assert_eq!(meta.size(), 8u64);
        let fs = localfs.clone();
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let f = async move {
            fs.remove(file_id, None, Options::default()).await;
            tx.send(true).unwrap();
        };
        localfs.runtime.spawn(f);
        assert!(rx.recv().unwrap());
        std::fs::File::open(&local_file).unwrap_err();
    }

    #[test]
    fn test_parse_file_id() {
        use rand::random;

        use crate::dfs::test_util::new_test_s3fs;

        let s3fs = new_test_s3fs(b"abcdefgh");

        let file_key = s3fs.file_key(random(), FileType::Sst);
        assert_eq!(
            format!("{}/{}", "prefix", parse_sst_file_suffix(&file_key)),
            file_key
        );

        for file_id in [0, 42, 0x1_0000_0000, 0xffff_ffff_ffff_ffff] {
            let file_key = s3fs.file_key(file_id, FileType::Sst);
            assert_eq!(
                try_parse_sst_file_id(&file_key),
                Some((file_id, FileType::Sst))
            );
            assert_eq!(
                try_parse_all_file_id(&file_key),
                Some((file_id, FileType::Sst))
            );
        }

        for file_type in [
            FileType::Blob,
            FileType::TxnChunk,
            FileType::Schema,
            FileType::Columnar,
            FileType::VectorIndex,
        ] {
            for file_key in [
                "".to_string(),
                "cse/0000000000000001/e00000001/0000000000800000_00000000008e9000.wal".to_string(),
                s3fs.file_key(42, file_type),
            ] {
                assert_eq!(try_parse_sst_file_id(&file_key), None);
            }

            for file_id in [0, 42, 0x1_0000_0000, 0xffff_ffff_ffff_ffff] {
                let file_key = s3fs.file_key(file_id, file_type);
                assert_eq!(
                    try_parse_other_file_id(&file_key),
                    Some((file_id, file_type))
                );
                assert_eq!(try_parse_all_file_id(&file_key), Some((file_id, file_type)));
            }
        }

        for file_key in [
            "".to_string(),
            "cse/0000000000000001/e00000001/0000000000800000_00000000008e9000.wal".to_string(),
            s3fs.file_key(42, FileType::Sst),
        ] {
            assert_eq!(try_parse_other_file_id(&file_key), None);
        }
    }
}
