// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    fs,
    io::Write,
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use dashmap::DashMap;
use quick_cache::sync::{Cache, GuardResult};
use tikv_util::{
    config::{AbsoluteOrPercentSize, ReadableDuration},
    sys::SysQuota,
};

use crate::{
    IoContext,
    ia::{
        manager::{IaManagerOptions, QueueOptions},
        types::SegmentHandle,
    },
    table::{Error, Result, file::LocalFile, get_local_dir},
};

pub const TEMPORARY_FILE_SUFFIX: &str = "tmp";

/// LocalStore is used to provide uniform access to both local disk and memory.
#[async_trait]
pub trait LocalStore: Send + Sync {
    /// Return the path of store. Will be `None` when it's in memory.
    fn main_paths(&self) -> &[PathBuf];

    fn init(&self) -> Result<()>;

    /// Return the existed keys & suffixes in the store from last startup.
    fn scan(&self) -> Result<HashMap<String /* suffix */, Vec<String> /* keys */>>;

    fn save(&self, file_id: u64, key: &str, data: Bytes) -> Result<()>;

    /// Read exactly `buf.len()` bytes into `buf` starting at `offset`.
    fn read_at(&self, file_id: u64, key: &str, buf: &mut [u8], offset: u64) -> Result<Option<()>>;

    fn read(&self, file_id: u64, key: &str, start_off: u64, end_off: u64) -> Result<Option<Bytes>>;

    fn remove(&self, file_id: u64, key: &str) -> Result<Option<()>>;

    /// Get a handle to hold the underlying data from being removed.
    fn handle(&self, file_id: u64, key: &str) -> Result<Option<SegmentHandle>>;
}

pub fn new_local_store(paths: Vec<PathBuf>, fd_cache_capacity: usize) -> Arc<dyn LocalStore> {
    if !paths.is_empty() {
        Arc::new(LocalFileStore::new(paths, fd_cache_capacity)) as _
    } else {
        Arc::new(LocalMemoryStore::default()) as _
    }
}

macro_rules! try_fs {
    ($e:expr) => {{
        match $e {
            Ok(f) => Ok(f),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(err) => Err(err),
        }
    }};
}

macro_rules! try_open {
    ($path:expr) => {{ try_fs!(std::fs::File::open($path)) }};
}

#[macro_export]
macro_rules! try_some {
    ($expr:expr) => {{
        match $expr {
            Some(v) => v,
            None => return Ok(None),
        }
    }};
}

pub struct LocalFileStore {
    dirs: Vec<PathBuf>,
    fd_cache: Arc<Cache<String, Arc<std::fs::File>>>,
}

impl LocalFileStore {
    pub fn new(dirs: Vec<PathBuf>, fd_cache_capacity: usize) -> Self {
        let fd_cache = Arc::new(Cache::new(fd_cache_capacity));
        Self { dirs, fd_cache }
    }

    fn open_file(&self, file_id: u64, key: &str) -> Result<Option<Arc<std::fs::File>>> {
        let dir = get_local_dir(&self.dirs, file_id);
        let f = match self.fd_cache.get_value_or_guard(key, None) {
            GuardResult::Value(file) => file,
            GuardResult::Guard(placeholder) => {
                let path = dir.join(key);
                debug!("FileDataStore.open_file cache miss"; "file_id" => file_id, "key" => key, "path" => ?path);
                let f = try_open!(path).table_ctx(file_id, format!("open.{key}"))?;
                let arc_f = Arc::new(f);
                let _ = placeholder.insert(arc_f.clone());
                arc_f
            }
            GuardResult::Timeout => unreachable!(),
        };
        Ok(Some(f))
    }
}

#[async_trait]
impl LocalStore for LocalFileStore {
    fn main_paths(&self) -> &[PathBuf] {
        &self.dirs
    }

    fn init(&self) -> Result<()> {
        for dir in &self.dirs {
            fs::create_dir_all(dir).table_ctx(0, format!("create_dir.{:?}", dir))?;
        }
        Ok(())
    }

    fn scan(&self) -> Result<HashMap<String /* suffix */, Vec<String> /* keys */>> {
        let mut map = HashMap::new();
        for dir in &self.dirs {
            if !dir.is_dir() {
                return Err(Error::Io(format!("not a directory: {:?}", dir)));
            }
            let entries = fs::read_dir(dir).table_ctx(0, format!("read_dir.{:?}", dir))?;
            for entry in entries {
                let entry = entry.table_ctx(0, "entry")?;
                let path = entry.path();
                if path.is_file() {
                    let key = path.file_name().unwrap().to_string_lossy().into_owned();
                    let suffix = path
                        .extension()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .into_owned();
                    map.entry(suffix).or_insert_with(Vec::new).push(key);
                }
            }
        }
        Ok(map)
    }

    fn save(&self, file_id: u64, key: &str, data: Bytes) -> Result<()> {
        lazy_static::lazy_static! {
            static ref TMP_ID: AtomicU64 = AtomicU64::new(0);
        }

        let tmp_filename = format!(
            "{}.{}.{}",
            key,
            TMP_ID.fetch_add(1, Relaxed),
            TEMPORARY_FILE_SUFFIX
        );
        let dir = get_local_dir(&self.dirs, file_id);
        let tmp_path = dir.join(tmp_filename);
        let mut f = fs::File::create(&tmp_path).table_ctx(file_id, format!("create_tmp.{key}"))?;

        f.write_all(&data)
            .table_ctx(file_id, format!("write_tmp.{key}"))?;
        f.sync_data()
            .table_ctx(file_id, format!("sync_tmp.{key}"))?;

        let path = dir.join(key);
        fs::rename(&tmp_path, &path).table_ctx(file_id, format!("rename.{key}"))?;

        debug!("FileDataStore.save"; "file_id" => file_id, "key" => key, "path" => ?path);
        Ok(())
    }

    fn read_at(&self, file_id: u64, key: &str, buf: &mut [u8], offset: u64) -> Result<Option<()>> {
        let f = try_some!(self.open_file(file_id, key)?);
        f.read_exact_at(buf, offset)
            .table_ctx(file_id, format!("read_exact.{key}"))?;
        Ok(Some(()))
    }

    fn read(&self, file_id: u64, key: &str, start_off: u64, end_off: u64) -> Result<Option<Bytes>> {
        let mut buf = vec![0; (end_off - start_off) as usize];
        try_some!(self.read_at(file_id, key, &mut buf, start_off)?);
        Ok(Some(Bytes::from(buf)))
    }

    fn remove(&self, file_id: u64, key: &str) -> Result<Option<()>> {
        let dir = get_local_dir(&self.dirs, file_id);
        let path = dir.join(key);
        self.fd_cache.remove(key);
        try_fs!(std::fs::remove_file(path)).table_ctx(file_id, format!("remove.{key}"))?;
        Ok(Some(()))
    }

    fn handle(&self, file_id: u64, key: &str) -> Result<Option<SegmentHandle>> {
        let f = try_some!(self.open_file(file_id, key)?);
        let dir = get_local_dir(&self.dirs, file_id);
        let path = dir.join(key);
        let local_file = LocalFile::from_file(file_id, path, f)?;
        Ok(Some(local_file.into()))
    }
}

#[derive(Default)]
pub(crate) struct LocalMemoryStore {
    m: DashMap<String, Bytes>,
}

impl LocalMemoryStore {
    fn get(&self, key: &str) -> Option<Bytes> {
        self.m.get(key).map(|x| x.value().clone())
    }

    fn get_with_check(&self, key: &str, end_off: u64) -> Result<Option<Bytes>> {
        let data = try_some!(self.get(key));
        if end_off > data.len() as u64 {
            return Err(Error::Io(format!(
                "read out of range, key: {}, end_off: {}, data length: {}",
                key,
                end_off,
                data.len()
            )));
        }
        Ok(Some(data))
    }
}

#[async_trait]
impl LocalStore for LocalMemoryStore {
    fn main_paths(&self) -> &[PathBuf] {
        &[]
    }

    fn init(&self) -> Result<()> {
        Ok(())
    }

    fn scan(&self) -> Result<HashMap<String /* suffix */, Vec<String> /* keys */>> {
        Ok(HashMap::new())
    }

    fn save(&self, _file_id: u64, key: &str, data: Bytes) -> Result<()> {
        self.m.insert(key.to_owned(), data);
        Ok(())
    }

    fn read_at(&self, _file_id: u64, key: &str, buf: &mut [u8], offset: u64) -> Result<Option<()>> {
        let end_off = offset + buf.len() as u64;
        let data = try_some!(self.get_with_check(key, end_off)?);
        buf.copy_from_slice(&data[offset as usize..end_off as usize]);
        Ok(Some(()))
    }

    fn read(
        &self,
        _file_id: u64,
        key: &str,
        start_off: u64,
        end_off: u64,
    ) -> Result<Option<Bytes>> {
        let data = try_some!(self.get_with_check(key, end_off)?);
        Ok(Some(Bytes::copy_from_slice(
            data.slice(start_off as usize..end_off as usize).chunk(),
        )))
    }

    fn remove(&self, _file_id: u64, key: &str) -> Result<Option<()>> {
        Ok(self.m.remove(key).map(|_| ()))
    }

    fn handle(&self, file_id: u64, key: &str) -> Result<Option<SegmentHandle>> {
        let data = try_some!(self.get(key));
        Ok(Some(SegmentHandle::from_bytes(file_id, data)))
    }
}

const IA_SEGMENT_SIZE_DEF: i64 = 1 << 20; // 1MiB
const IA_FREQ_UPDATE_INTERVAL_DEF: Duration = Duration::from_secs(60);
const IA_DFS_CONCURRENCY_DEF: usize = 512;
const IA_DFS_KEYSPACE_CONCURRENCY_DEF: usize = 192;
const IA_FD_CACHE_CAPACITY_DEF: usize = 102400; // 100k
const IA_TABLE_META_MTIME_INTERVAL_DEF: Duration = Duration::from_secs(3600); // 1 hour
const IA_DYNAMIC_CACHE_CAPACITY_DEF: bool = false;
const IA_CACHE_CAP_TO_TOTAL_DATA_SIZE_RATIO_DEF: f64 = 0.2; // 20%

const MAIN_QUEUE_CAPACITY_FACTOR: i64 = 10; // Main queue is 10x larger than small queue.

const SYNC_READ_TIMEOUT: Duration = Duration::from_secs(3);

const AUTO_IA_CHECK_INTERVAL_DEF: Duration = Duration::from_secs(60 * 60); // 1 hour

pub enum IaCapacity {
    Manual {
        small_queue: QueueOptions,
        main_queue: QueueOptions,
    },
    MemoryCap(AbsoluteOrPercentSize),
    /// Small queue in memory & main queue in disk.
    MemoryAndDiskCap(
        AbsoluteOrPercentSize, // mem_cap
        Vec<PathBuf>,          // local_dirs
        AbsoluteOrPercentSize, // disk_cap
    ),
}

impl Default for IaCapacity {
    fn default() -> Self {
        IaCapacity::MemoryCap(AbsoluteOrPercentSize::Percent(20.0))
    }
}

impl IaCapacity {
    fn build_options(self, options: &mut IaManagerOptions) -> Result<()> {
        match self {
            IaCapacity::Manual {
                small_queue,
                main_queue,
            } => {
                options.small_queue = small_queue;
                options.main_queue = main_queue;
            }
            IaCapacity::MemoryCap(cap) => {
                let mem_cap = cap.as_memory_size() as i64;
                options.small_queue.paths = vec![];
                options.small_queue.cap = mem_cap / MAIN_QUEUE_CAPACITY_FACTOR;
                options.main_queue.paths = vec![];
                options.main_queue.cap = mem_cap - options.small_queue.cap;
            }
            IaCapacity::MemoryAndDiskCap(mem_cap, local_dirs, disk_cap) => {
                let mem_cap = mem_cap.as_memory_size();
                options.small_queue.paths = vec![];
                options.small_queue.cap = mem_cap as i64;
                for local_dir in &local_dirs {
                    if !local_dir.exists() {
                        fs::create_dir_all(local_dir).map_err(|err| {
                            Error::Io(format!("create dir {:?} failed: {err:?}", local_dir))
                        })?;
                    }
                }
                let disk_cap = disk_cap
                    .as_disks_size(&local_dirs)
                    .map_err(|err| Error::Io(format!("get disks capacity failed: {err:?}")))?;
                options.main_queue.paths = local_dirs;
                options.main_queue.cap = disk_cap as i64;
            }
        }
        Ok(())
    }

    #[cfg(any(test, feature = "testexport"))]
    pub fn set_parent_dir(&mut self, parent: PathBuf) {
        match self {
            IaCapacity::Manual {
                small_queue,
                main_queue,
            } => {
                for path in &mut small_queue.paths {
                    *path = parent.join(&path);
                }
                for path in &mut main_queue.paths {
                    *path = parent.join(&path);
                }
            }
            IaCapacity::MemoryAndDiskCap(_, dirs, _) => {
                for dir in dirs {
                    *dir = parent.join(&dir);
                }
            }
            IaCapacity::MemoryCap(..) => {}
        }
    }
}

#[derive(Default)]
pub struct IaManagerOptionsBuilder {
    capacity: Option<IaCapacity>,
    segment_size: Option<i64>,
    freq_update_interval: Option<Duration>,
    dfs_concurrency: Option<usize>,
    dfs_keyspace_concurrency: Option<usize>,
    fd_cache_capacity: Option<usize>,
    table_meta_mtime_interval: Option<Duration>,
    dynamic_capacity: bool,
    cache_cap_to_total_data_size_ratio: Option<f64>,

    disable_sync_read: bool,
    sync_read_concurrency: Option<usize>,
    sync_read_timeout: Option<Duration>,
}

impl IaManagerOptionsBuilder {
    pub fn capacity(mut self, capacity: IaCapacity) -> Self {
        self.capacity = Some(capacity);
        self
    }

    pub fn segment_size(mut self, size: i64) -> Self {
        self.segment_size = Some(size);
        self
    }

    pub fn freq_update_interval(mut self, interval: Duration) -> Self {
        self.freq_update_interval = Some(interval);
        self
    }

    pub fn dfs_concurrency(mut self, concurrency: usize, keyspace_concurrency: usize) -> Self {
        self.dfs_concurrency = Some(concurrency);
        self.dfs_keyspace_concurrency = Some(keyspace_concurrency);
        self
    }

    pub fn fd_cache_capacity(mut self, capacity: usize) -> Self {
        self.fd_cache_capacity = Some(capacity);
        self
    }

    pub fn table_meta_mtime_interval(mut self, interval: Duration) -> Self {
        self.table_meta_mtime_interval = Some(interval);
        self
    }

    pub fn dynamic_capacity(mut self, enable: bool) -> Self {
        self.dynamic_capacity = enable;
        self
    }

    pub fn cache_cap_to_total_data_size_ratio(mut self, ratio: f64) -> Self {
        self.cache_cap_to_total_data_size_ratio = Some(ratio);
        self
    }

    pub fn sync_read(mut self, disable: bool, concurrency: usize, timeout: Duration) -> Self {
        self.disable_sync_read = disable;
        self.sync_read_concurrency = Some(concurrency);
        self.sync_read_timeout = Some(timeout);
        self
    }

    #[cfg(any(test, feature = "testexport"))]
    pub fn disable_sync_read(mut self) -> Self {
        self.disable_sync_read = true;
        self
    }

    pub fn build(mut self) -> Result<IaManagerOptions> {
        let segment_size = self.segment_size.unwrap_or(IA_SEGMENT_SIZE_DEF);
        let freq_update_interval = self
            .freq_update_interval
            .unwrap_or(IA_FREQ_UPDATE_INTERVAL_DEF);
        let dfs_concurrency = self.dfs_concurrency.unwrap_or(IA_DFS_CONCURRENCY_DEF);
        let dfs_keyspace_concurrency = self
            .dfs_keyspace_concurrency
            .unwrap_or(IA_DFS_KEYSPACE_CONCURRENCY_DEF);
        let fd_cache_capacity = self.fd_cache_capacity.unwrap_or(IA_FD_CACHE_CAPACITY_DEF);
        let table_meta_mtime_interval = self
            .table_meta_mtime_interval
            .unwrap_or(IA_TABLE_META_MTIME_INTERVAL_DEF);
        let cache_cap_to_total_data_size_ratio = self
            .cache_cap_to_total_data_size_ratio
            .unwrap_or(IA_CACHE_CAP_TO_TOTAL_DATA_SIZE_RATIO_DEF);

        let mut options = IaManagerOptions {
            small_queue: QueueOptions::default(),
            main_queue: QueueOptions::default(),
            segment_size,
            freq_update_interval,
            dfs_concurrency,
            dfs_keyspace_concurrency,
            fd_cache_capacity,
            table_meta_mtime_interval,
            dynamic_capacity: self.dynamic_capacity,
            cache_cap_to_total_data_size_ratio,
            disable_sync_read: self.disable_sync_read,
            sync_read_concurrency: self
                .sync_read_concurrency
                .unwrap_or_else(|| SysQuota::cpu_cores_quota() as usize / 2),
            sync_read_timeout: self.sync_read_timeout.unwrap_or(SYNC_READ_TIMEOUT),
        };
        let cap = self.capacity.take().unwrap_or_default();
        cap.build_options(&mut options)?;
        Ok(options)
    }
}

/// The config of IA (Infrequent Access) which use memory for small queue and
/// disk for main queue.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct IaConfig {
    pub mem_cap: AbsoluteOrPercentSize,
    pub disk_cap: AbsoluteOrPercentSize,
    pub segment_size: i64,
    pub freq_update_interval: ReadableDuration,
    pub dfs_concurrency: usize,
    pub dfs_keyspace_concurrency: usize,
    pub fd_cache_capacity: usize,
    pub table_meta_mtime_interval: ReadableDuration,
    pub dynamic_capacity: bool,
    pub cache_cap_to_total_data_size_ratio: f64,

    pub disable_sync_read: bool,
    pub sync_read_timeout: ReadableDuration,
    pub force_ia: bool,
    pub auto_ia_check_interval: ReadableDuration,
}

impl Default for IaConfig {
    fn default() -> Self {
        Self {
            mem_cap: Default::default(),
            disk_cap: Default::default(),
            segment_size: IA_SEGMENT_SIZE_DEF,
            freq_update_interval: ReadableDuration(IA_FREQ_UPDATE_INTERVAL_DEF),
            dfs_concurrency: IA_DFS_CONCURRENCY_DEF,
            dfs_keyspace_concurrency: IA_DFS_KEYSPACE_CONCURRENCY_DEF,
            fd_cache_capacity: IA_FD_CACHE_CAPACITY_DEF,
            table_meta_mtime_interval: ReadableDuration(IA_TABLE_META_MTIME_INTERVAL_DEF),
            dynamic_capacity: IA_DYNAMIC_CACHE_CAPACITY_DEF,
            cache_cap_to_total_data_size_ratio: IA_CACHE_CAP_TO_TOTAL_DATA_SIZE_RATIO_DEF,
            disable_sync_read: false,
            sync_read_timeout: ReadableDuration(SYNC_READ_TIMEOUT),
            force_ia: false,
            auto_ia_check_interval: ReadableDuration(AUTO_IA_CHECK_INTERVAL_DEF),
        }
    }
}

impl IaConfig {
    pub fn to_manager_options(&self, data_paths: Vec<PathBuf>) -> Result<IaManagerOptions> {
        let cap = IaCapacity::MemoryAndDiskCap(self.mem_cap, data_paths, self.disk_cap);
        let sync_read_concurrency = if self.disable_sync_read {
            0
        } else {
            SysQuota::cpu_cores_quota() as usize / 2
        };
        IaManagerOptionsBuilder::default()
            .capacity(cap)
            .segment_size(self.segment_size)
            .freq_update_interval(self.freq_update_interval.0)
            .dfs_concurrency(self.dfs_concurrency, self.dfs_keyspace_concurrency)
            .fd_cache_capacity(self.fd_cache_capacity)
            .table_meta_mtime_interval(self.table_meta_mtime_interval.0)
            .dynamic_capacity(self.dynamic_capacity)
            .cache_cap_to_total_data_size_ratio(self.cache_cap_to_total_data_size_ratio)
            .sync_read(
                self.disable_sync_read,
                sync_read_concurrency,
                self.sync_read_timeout.0,
            )
            .build()
    }

    pub fn disabled() -> Self {
        Self {
            mem_cap: 0.into(),
            disk_cap: 0.into(),
            ..Default::default()
        }
    }
}

#[cfg(any(test, feature = "testexport"))]
pub mod test_util {
    #[cfg(feature = "debug-trace-ia-segments")]
    use crate::ia::debug::SegmentAction;
    use crate::ia::{
        queue::{QueueItem, QueueItemPos},
        types::{FileSegmentData, FileSegmentIdent},
    };

    #[cfg(feature = "debug-trace-ia-segments")]
    fn dump_segment_action_history(ident: FileSegmentIdent) -> Vec<SegmentAction> {
        crate::ia::debug::dump_segment_action_history(ident)
    }

    #[cfg(not(feature = "debug-trace-ia-segments"))]
    fn dump_segment_action_history(_ident: FileSegmentIdent) -> &'static str {
        "(disabled)"
    }

    const SEGMENT_POS_MISMATCH_THRESHOLD: usize = 3;

    pub fn verify_local_segments(
        segments: &[(FileSegmentIdent, FileSegmentData, Option<QueueItem>)],
        small_cap: i64,
        main_cap: i64,
        expected_total_size: Option<u64>,
    ) {
        let mut total_size = 0;
        let mut small_cached_size = 0;
        let mut main_cached_size = 0;
        let mut mismatch_segments = vec![];
        for (ident, segment, queue_item) in segments {
            let history = dump_segment_action_history(*ident);
            debug!("verify_local_segments"; "ident" => ?ident, "segment" => ?segment,
                "queue_item" => ?queue_item, "history" => ?history);

            total_size += ident.size();
            match &segment {
                FileSegmentData::InMem(_) => {
                    small_cached_size += ident.size();
                }
                FileSegmentData::InStore => {
                    main_cached_size += ident.size();
                }
            }

            match queue_item {
                None => {
                    panic!(
                        "pos mismatch, not in queue: ident: {:?}, segment: {:?}, history: {:?}",
                        ident, segment, history
                    );
                }
                Some(queue_item) => match (&queue_item.pos, segment) {
                    (QueueItemPos::Small, FileSegmentData::InMem(_))
                    | (QueueItemPos::Main, FileSegmentData::InStore) => {}
                    (QueueItemPos::Small, FileSegmentData::InStore) => {
                        // Tolerate this case.
                        // Happen when the movement to main store is delayed to execute after
                        // another evict and insert to memory for the same segment.
                        // See https://github.com/tidbcloud/cloud-storage-engine/issues/1993#issuecomment-2567583092.
                        warn!("pos mismatch, queue: {:?}, store: {:?}", queue_item.pos, segment;
                                "ident" =>?ident, "segment" => ?segment, "queue_item" => ?queue_item, "history" => ?history);
                        mismatch_segments.push(ident);
                    }
                    (QueueItemPos::Main, FileSegmentData::InMem(_)) => {
                        panic!(
                            "pos mismatch, ident: {:?}, queue_item: {:?}, segment: {:?}, history: {:?}",
                            ident, queue_item, segment, history
                        );
                    }
                },
            }
        }

        assert!(
            mismatch_segments.len() <= SEGMENT_POS_MISMATCH_THRESHOLD,
            "pos mismatch: {:?}, segments: {:?}, threshold: {}",
            mismatch_segments,
            segments,
            SEGMENT_POS_MISMATCH_THRESHOLD
        );

        if let Some(expected_total_size) = expected_total_size {
            assert!(total_size <= expected_total_size);
        }

        info!("cached size";
            "small_cached_size" => small_cached_size,
            "small_cap" => small_cap,
            "main_cached_size" => main_cached_size,
            "main_cap" => main_cap,
            "total_size" => total_size,
        );
        assert!(
            small_cached_size as i64 <= small_cap,
            "total_size: {}, small_cached_size: {}, small_cap: {}, segments: {:?}",
            total_size,
            small_cached_size,
            small_cap,
            segments
        );
        let mismatch_size = mismatch_segments
            .iter()
            .map(|ident| ident.size())
            .sum::<u64>();
        assert!(
            main_cached_size as i64 <= main_cap + mismatch_size as i64,
            "total_size: {}, main_cached_size: {}, main_cap: {}, segments: {:?}",
            total_size,
            main_cached_size,
            main_cap,
            segments
        );
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_ia_capacity() {
        let mut options = IaManagerOptions::default();

        let ia_cap = IaCapacity::MemoryCap(AbsoluteOrPercentSize::Percent(20.0));
        ia_cap.build_options(&mut options).unwrap();
        println!("options for MemoryCap(20%): {:?}", options);

        let temp_dir = TempDir::new().unwrap();
        let ia_cap = IaCapacity::MemoryAndDiskCap(
            AbsoluteOrPercentSize::Percent(10.0),
            vec![temp_dir.path().to_path_buf()],
            AbsoluteOrPercentSize::Percent(10.0),
        );
        ia_cap.build_options(&mut options).unwrap();
        println!("options for MemoryAndDiskCap(10%+10%): {:?}", options);
    }
}
