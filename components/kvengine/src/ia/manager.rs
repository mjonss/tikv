// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::{Read, Write},
    ops,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use bytes::{Buf, BufMut, Bytes};
use dashmap::DashMap;
use tikv_util::{
    codec::number::{I64_SIZE, U8_SIZE},
    deadline::Deadline,
    time::Instant,
    worker_pool::{WorkerPool, WorkerPoolHandle},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    dfs,
    dfs::{Dfs, FileType},
    ia::{
        queue::S3FifoHandle,
        types::{
            FILE_SEGMENT_DATA_IN_MEMORY, FileSegmentData, FileSegmentIdent, FileSegmentPosition,
            GuardMap, LocalSegmentMap, SegmentHandle, TableMetaInfo,
        },
        util::{LocalStore, new_local_store},
    },
    metrics::{
        ENGINE_IA_MAIN_QUEUE_CAPACITY, ENGINE_IA_READ_SEGMENT_CACHE_MISS,
        ENGINE_IA_READ_SEGMENT_DURATION_HISTOGRAM, ENGINE_IA_SMALL_QUEUE_CAPACITY,
    },
    table::{Error, Result, file::FdCache},
    try_some,
};

const MANIFEST_PERSIST_INTERVAL: Duration = Duration::from_secs(60);

const SMALL_QUEUE_MIN_SEGMENT_COUNT: i64 = 64;
const MAIN_QUEUE_MIN_SEGMENT_COUNT: i64 = 256;

/// `buf` can be empty, which means to request the specified range of data but
/// do not actually read it. Used to prefetch segments from remote.
pub(crate) struct ReadAt<'a> {
    buf: &'a mut [u8],
    offset: u64,

    /// Get handle to keep underlying data available, even the segment is
    /// evicted.
    need_segment_handle: bool,
    segment_handle: Option<SegmentHandle>,
}

impl<'a> ReadAt<'a> {
    pub(crate) fn new(buf: &'a mut [u8], offset: u64, need_segment_handle: bool) -> Self {
        Self {
            buf,
            offset,
            need_segment_handle,
            segment_handle: None,
        }
    }

    pub(crate) fn start_off(&self) -> u64 {
        self.offset
    }

    pub(crate) fn end_off(&self) -> u64 {
        self.offset + self.buf.len() as u64
    }

    fn read_from_segment_bytes(&mut self, ident: FileSegmentIdent, seg_data: &Bytes) {
        let (start_off, end_off) = (self.start_off(), self.end_off());
        debug_assert!(
            start_off <= end_off && ident.start_off <= start_off && end_off <= ident.end_off
        );
        if self.need_segment_handle {
            let handle = SegmentHandle::from_bytes(ident.file_id, seg_data.clone());
            self.set_handle(handle);
        }
        if !self.buf.is_empty() {
            let seg_slice = seg_data.slice(
                (start_off - ident.start_off) as usize..(end_off - ident.start_off) as usize,
            );
            self.buf.copy_from_slice(&seg_slice);
        }
    }

    fn set_handle(&mut self, handle: SegmentHandle) {
        let prev = self.segment_handle.replace(handle);
        debug_assert!(prev.is_none());
    }
}

/// Note that the capacity is not strictly limited for performance. So some
/// additional buffer (maybe 10%) should be reserved.
#[derive(Default, Clone, Debug)]
pub struct QueueOptions {
    /// It means in memory when `path` is `None`.
    pub paths: Vec<PathBuf>,
    pub cap: i64,
}

#[derive(Default, Clone, Debug)]
pub struct IaManagerOptions {
    pub small_queue: QueueOptions,
    pub main_queue: QueueOptions,
    pub segment_size: i64,

    /// The minimum interval to update "freq" counter in queue.
    ///
    /// Used to handle the scene that a single request touch multiple slice of a
    /// segment and increase the freq unexpectedly.
    pub freq_update_interval: Duration,

    pub dfs_concurrency: usize,
    pub dfs_keyspace_concurrency: usize,

    // The capacity of the file descriptor cache.
    pub fd_cache_capacity: usize,

    pub table_meta_mtime_interval: Duration,

    /// Whether to enable dynamic capacity adjustment.
    pub dynamic_capacity: bool,

    /// The ratio of cache capacity to total data size.
    ///
    /// Used to dynamic adjust the cache capacity when total data size of IA
    /// changed.
    pub cache_cap_to_total_data_size_ratio: f64,

    pub disable_sync_read: bool,
    pub sync_read_concurrency: usize,
    pub sync_read_timeout: Duration,
}

impl IaManagerOptions {
    pub fn total_capacity(&self) -> i64 {
        self.small_queue.cap + self.main_queue.cap
    }

    #[inline]
    pub fn small_queue_min_cap(&self) -> i64 {
        if self.dynamic_capacity {
            (self.segment_size * SMALL_QUEUE_MIN_SEGMENT_COUNT).min(self.small_queue.cap)
        } else {
            self.small_queue.cap
        }
    }

    #[inline]
    pub fn main_queue_min_cap(&self) -> i64 {
        if self.dynamic_capacity {
            (self.segment_size * MAIN_QUEUE_MIN_SEGMENT_COUNT).min(self.main_queue.cap)
        } else {
            self.main_queue.cap
        }
    }
}

#[derive(Clone)]
pub struct IaManager {
    core: Arc<IaManagerCore>,
}

impl ops::Deref for IaManager {
    type Target = IaManagerCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl IaManager {
    pub fn new(
        opts: IaManagerOptions,
        fs: Arc<dyn dfs::Dfs>,
        meta_fd_cache: Option<FdCache>,
        runtime: WorkerPool,
    ) -> Result<Self> {
        assert!(
            opts.small_queue.paths.is_empty(),
            "small queue must be in memory"
        );

        info!("create IA manager"; "opts" => ?opts);
        let main_store = new_local_store(opts.main_queue.paths.clone(), opts.fd_cache_capacity);
        let segments = Arc::new(LocalSegmentMap::default());
        let segment_data_ctx = SegmentDataContext {
            segments: segments.clone(),
            main_store: main_store.clone(),
        };

        let small_queue_cap = Arc::new(AtomicI64::new(opts.small_queue_min_cap()));
        let main_queue_cap = Arc::new(AtomicI64::new(opts.main_queue_min_cap()));
        let fifo = S3FifoHandle::new(
            small_queue_cap.clone(),
            main_queue_cap.clone(),
            opts.segment_size,
            opts.freq_update_interval,
            runtime.handle(),
            segment_data_ctx,
        );

        let dfs_concurrency = Arc::new(Semaphore::new(opts.dfs_concurrency));

        let core = Arc::new(IaManagerCore {
            opts,
            fs,
            runtime,
            main_store,
            meta_fd_cache,
            loading_segments: Default::default(),
            segments,
            table_metas: Default::default(),
            small_queue_cap,
            main_queue_cap,
            fifo,
            dfs_concurrency,
            dfs_keyspace_concurrency: Default::default(),
            last_persist_manifest_time: AtomicI64::new(Instant::now_coarse().second()),
            sync_read_counter: Arc::new(()),
        });

        let mgr = Self { core };
        mgr.init()?;
        Ok(mgr)
    }
}

pub struct IaManagerCore {
    opts: IaManagerOptions,
    fs: Arc<dyn dfs::Dfs>,
    runtime: WorkerPool,
    main_store: Arc<dyn LocalStore>,
    meta_fd_cache: Option<FdCache>,

    loading_segments: GuardMap<FileSegmentIdent, ()>,
    segments: Arc<LocalSegmentMap>,

    table_metas: Arc<DashMap<u64 /* file_id */, TableMetaInfo>>,

    small_queue_cap: Arc<AtomicI64>,
    main_queue_cap: Arc<AtomicI64>,
    fifo: S3FifoHandle,

    dfs_concurrency: Arc<Semaphore>,
    dfs_keyspace_concurrency: Arc<DashMap<u32 /* keyspace_id */, Arc<Semaphore>>>,

    last_persist_manifest_time: AtomicI64,

    sync_read_counter: Arc<()>,
}

impl IaManagerCore {
    pub fn enter_runtime(&self) -> tokio::runtime::EnterGuard<'_> {
        self.runtime.enter()
    }

    pub(crate) fn runtime_handle(&self) -> WorkerPoolHandle {
        self.runtime.handle()
    }

    pub fn get_dfs(&self) -> &dyn Dfs {
        self.fs.deref()
    }

    pub fn get_meta_fd_cache(&self) -> Option<FdCache> {
        self.meta_fd_cache.clone()
    }

    pub(crate) fn segment_size(&self) -> i64 {
        self.opts.segment_size
    }

    fn init(&self) -> Result<()> {
        if let Some(path) = self.main_store.main_paths().first() {
            match Manifest::read_from_path(path) {
                Ok(Some(manifest)) => {
                    self.init_from_manifest(&manifest);
                }
                Ok(None) => {}
                Err(err) => warn!("open manifest failed: {:?}", err),
            }
        }

        self.main_store.init()?;
        let mut entries = self.main_store.scan()?;
        if let Some(segments) = entries.remove("seg") {
            self.init_segments(segments)?;
        }

        ENGINE_IA_MAIN_QUEUE_CAPACITY.set(self.main_queue_cap.load(Ordering::Relaxed));
        ENGINE_IA_SMALL_QUEUE_CAPACITY.set(self.small_queue_cap.load(Ordering::Relaxed));

        Ok(())
    }

    fn init_from_manifest(&self, manifest: &Manifest) {
        info!("init from manifest: {:?}", manifest);
        self.adjust_queue_cap(manifest.main_queue_cap);
    }

    // TODO: take snapshot for queue & restore from it.
    fn init_segments(&self, keys: Vec<String>) -> Result<()> {
        for k in keys {
            if let Some(ident) = FileSegmentIdent::parse_local_filename(&k) {
                self.segments
                    .set_segment_data(ident, FileSegmentData::InStore);
                self.fifo.read(ident, true)?;
            }
        }
        Ok(())
    }

    /// Offset in `read_at` are absolute offsets of the file.
    pub(crate) async fn read_segment(
        &self,
        ident: FileSegmentIdent,
        ftype: FileType,
        keyspace_id: Option<u32>,
        deadline: Option<Deadline>,
        read_at: &mut ReadAt<'_>,
        dfs: Option<&dyn Dfs>,
    ) -> Result<()> {
        let (start_off, end_off) = (read_at.start_off(), read_at.end_off());
        if start_off > end_off || start_off < ident.start_off || ident.end_off < end_off {
            debug_assert!(
                false,
                "invalid range, segment {:?}, range: {}-{}",
                ident, start_off, end_off
            );
            return Err(Error::IaMgr(format!(
                "invalid range, ident: {:?}, range: {}-{}",
                ident, start_off, end_off
            )));
        }

        let start_time = Instant::now_coarse();
        debug!("read segment"; "ident" => %ident, "start_off" => start_off, "end_off" => end_off);
        let on_finish = |cache_hit: bool| {
            let elapsed = start_time.saturating_elapsed();
            ENGINE_IA_READ_SEGMENT_DURATION_HISTOGRAM.observe(elapsed.as_secs_f64());
            if !cache_hit {
                ENGINE_IA_READ_SEGMENT_CACHE_MISS.inc();
            }
            debug!("read segment finished"; "ident" => %ident, "elapsed" => ?elapsed, "cache_hit" => cache_hit);
        };
        if let Some(()) = self.read_segment_from_cache(ident, read_at)? {
            on_finish(true);
            return Ok(());
        }

        let data = {
            let _loading_guard = self.loading_segments.get_locked(ident).await;

            // Check cache again. Another thread may have filled the cache.
            if let Some(()) = self.read_segment_from_cache(ident, read_at)? {
                on_finish(true);
                // Not necessary to remove ident from `loading_segments`, as there must be
                // another concurrent request read the segment from remote.
                return Ok(());
            }

            let data = self
                .read_segment_from_remote(ident, ftype, keyspace_id, deadline, dfs)
                .await?;
            self.segments
                .set_segment_data(ident, FileSegmentData::InMem(data.clone()));

            self.loading_segments.remove(&ident);

            data
        };

        self.fifo.read(ident, false)?;

        read_at.read_from_segment_bytes(ident, &data);
        on_finish(false);
        Ok(())
    }

    async fn read_segment_from_remote(
        &self,
        ident: FileSegmentIdent,
        ftype: FileType,
        keyspace_id: Option<u32>,
        deadline: Option<Deadline>,
        dfs: Option<&dyn Dfs>,
    ) -> Result<Bytes> {
        let _permit = self.acquire_concurrency_permit(keyspace_id).await;
        if deadline.is_some_and(|d| d.check().is_err()) {
            return Err(Error::DeadlineExceeded(format!(
                "acquire concurrency permit timeout: {ident}"
            )));
        }
        let dfs = dfs.unwrap_or_else(|| self.fs.deref());

        let opts = dfs::Options::default()
            .with_type(ftype)
            .with_start_off(ident.start_off)
            .with_end_off(ident.end_off);
        let _enter = dfs.get_runtime().enter();
        Ok(dfs.read_file(ident.file_id, opts).await?)
    }

    fn read_segment_from_cache(
        &self,
        ident: FileSegmentIdent,
        read_at: &mut ReadAt<'_>,
    ) -> Result<Option<()>> {
        let segment_data = try_some!(self.segments.get_segment(ident));
        let res = match &segment_data {
            FileSegmentData::InMem(data) => {
                read_at.read_from_segment_bytes(ident, data);
                Some(())
            }
            FileSegmentData::InStore => self.read_segment_from_local_store(ident, read_at)?,
        };

        if res.is_some() {
            self.fifo.read(ident, false)?;
        }
        Ok(res)
    }

    // Note: when `read_at.buf` is empty, the existence of segment is not checked.
    fn read_segment_from_local_store(
        &self,
        ident: FileSegmentIdent,
        read_at: &mut ReadAt<'_>,
    ) -> Result<Option<()>> {
        debug!("read segment from local"; "ident" => %ident);
        let local_filename = ident.local_filename();
        if read_at.need_segment_handle {
            match self.main_store.handle(ident.file_id, &local_filename)? {
                Some(handle) => read_at.set_handle(handle),
                None => return Ok(None),
            }
        }
        if read_at.buf.is_empty() {
            return Ok(Some(()));
        }
        self.main_store.read_at(
            ident.file_id,
            &local_filename,
            read_at.buf,
            read_at.offset - ident.start_off,
        )
    }

    // Note: It's possible that the segment is of `SegmentData::InStore` but
    // actually not existed. In such condition this method will still return
    // true.
    pub fn is_segment_cached(&self, ident: FileSegmentIdent) -> bool {
        self.segments.get_segment(ident).is_some()
    }

    pub fn segment_cached_position(&self, ident: FileSegmentIdent) -> FileSegmentPosition {
        match self.segments.get_segment(ident) {
            Some(FileSegmentData::InMem(_)) => FileSegmentPosition::InMem,
            Some(FileSegmentData::InStore) => FileSegmentPosition::InStore,
            None => FileSegmentPosition::NotExist,
        }
    }

    pub async fn prefetch_segment(
        &self,
        ident: FileSegmentIdent,
        ftype: FileType,
        keyspace_id: u32,
        deadline: Deadline,
        dfs: Option<&dyn Dfs>,
    ) -> Result<()> {
        let mut buf: [u8; 0] = [];
        let mut read_at = ReadAt::new(buf.as_mut_slice(), ident.start_off, false);
        self.read_segment(
            ident,
            ftype,
            Some(keyspace_id),
            Some(deadline),
            &mut read_at,
            dfs,
        )
        .await
    }

    pub async fn get_segment_handle(
        &self,
        ident: FileSegmentIdent,
        ftype: FileType,
        dfs: Option<&dyn Dfs>,
    ) -> Result<SegmentHandle> {
        let mut buf: [u8; 0] = [];
        let mut read_at = ReadAt::new(buf.as_mut_slice(), ident.start_off, true);
        self.read_segment(ident, ftype, None, None, &mut read_at, dfs)
            .await?;
        let handle = read_at.segment_handle.unwrap();
        debug!("get segment handle"; "ident" => ?ident, "ftype" => ?ftype, "handle" => ?handle);
        Ok(handle)
    }

    pub async fn acquire_concurrency_permit(
        &self,
        keyspace_id: Option<u32>,
    ) -> (Option<OwnedSemaphorePermit>, OwnedSemaphorePermit) {
        let keyspace_permit = match keyspace_id {
            Some(keyspace_id) => {
                let sem = self
                    .dfs_keyspace_concurrency
                    .entry(keyspace_id)
                    .or_insert_with(|| Arc::new(Semaphore::new(self.opts.dfs_keyspace_concurrency)))
                    .clone();
                Some(sem.acquire_owned().await.unwrap())
            }
            None => None,
        };
        let global_permit = self.dfs_concurrency.clone().acquire_owned().await.unwrap();
        (keyspace_permit, global_permit)
    }

    pub fn cache_hit_rate(&self) -> f64 {
        let total = ENGINE_IA_READ_SEGMENT_DURATION_HISTOGRAM.get_sample_count();
        let miss = ENGINE_IA_READ_SEGMENT_CACHE_MISS.get();
        if total > 0 {
            (total - miss) as f64 / total as f64
        } else {
            0.0
        }
    }

    pub fn contains_segment(&self, ident: FileSegmentIdent) -> bool {
        self.segments.contains(ident)
    }

    pub fn main_store_paths(&self) -> &[PathBuf] {
        self.main_store.main_paths()
    }

    pub fn access_table_meta(&self, file_id: u64) -> bool /* should_set_mtime */ {
        match self.table_metas.entry(file_id) {
            dashmap::mapref::entry::Entry::Vacant(e) => {
                e.insert(TableMetaInfo::default());
                // Return false because:
                // The cop worker on spot instances will restart frequently for a short while.
                // If return true here, we will set mtime more than expected.
                // On the other hand, as the lifetime of table meta are much longer than
                // interval (24h vs. 1h by default), miss one or two times of
                // set mtime will not impact too much.
                false
            }
            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                let meta = e.get_mut();
                let now = Instant::now_coarse();
                let last_set_mtime =
                    Instant::from_timespec_second_coarse(meta.last_set_mtime_instant_sec);
                if now.saturating_duration_since(last_set_mtime)
                    >= self.opts.table_meta_mtime_interval
                {
                    meta.last_set_mtime_instant_sec = now.second();
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn remove_table_meta(&self, file_id: u64) {
        self.table_metas.remove(&file_id);
        if let Some(cache) = self.meta_fd_cache.as_ref() {
            cache.remove(file_id)
        }
    }

    pub fn notify_total_data_size(&self, total_data_size: u64) {
        let main_queue_cap =
            (total_data_size as f64 * self.opts.cache_cap_to_total_data_size_ratio) as i64;
        self.adjust_queue_cap(main_queue_cap);
        self.persist_manifest();
    }

    fn adjust_queue_cap(&self, main_queue_cap: i64) {
        if !self.opts.dynamic_capacity {
            return;
        }

        let main_cap =
            main_queue_cap.clamp(self.opts.main_queue_min_cap(), self.opts.main_queue.cap);

        // Should use f64. Otherwise, overflow may happen.
        let small_cap = (main_cap as f64 / self.opts.main_queue.cap as f64
            * self.opts.small_queue.cap as f64) as i64;
        let small_cap = small_cap.max(self.opts.small_queue_min_cap());

        let old_main_cap = self.main_queue_cap.swap(main_cap, Ordering::Relaxed);
        let old_small_cap = self.small_queue_cap.swap(small_cap, Ordering::Relaxed);

        if old_main_cap != main_cap || old_small_cap != small_cap {
            ENGINE_IA_MAIN_QUEUE_CAPACITY.set(main_cap);
            ENGINE_IA_SMALL_QUEUE_CAPACITY.set(small_cap);
            info!(
                "adjust queue cap: small: {}->{}, main: {}->{}",
                old_small_cap, small_cap, old_main_cap, main_cap
            );
        }
    }

    fn persist_manifest(&self) {
        let Some(path) = self.main_store.main_paths().first() else {
            return;
        };

        let last_time_secs = self.last_persist_manifest_time.load(Ordering::Relaxed);
        if Instant::from_timespec_second_coarse(last_time_secs).saturating_elapsed()
            < Self::manifest_persist_interval()
        {
            return;
        }
        if self
            .last_persist_manifest_time
            .compare_exchange_weak(
                last_time_secs,
                Instant::now_coarse().second(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_err()
        {
            return;
        }

        let main_queue_cap = self.main_queue_cap.load(Ordering::Relaxed);
        let manifest = Manifest { main_queue_cap };
        if let Err(err) = manifest.persist_to_path(path) {
            warn!("persist manifest failed"; "err" => ?err);
        }
    }

    pub(crate) fn acquire_sync_read(&self) -> Result<(Arc<()>, Duration)> {
        if self.opts.disable_sync_read {
            return Err(Error::IaMgr("sync read disabled".to_string()));
        }
        let guard = self.sync_read_counter.clone();
        if Arc::strong_count(&guard) > self.opts.sync_read_concurrency + 1 {
            Err(Error::IaMgr("sync read limit exceeded".to_string()))
        } else {
            Ok((guard, self.opts.sync_read_timeout))
        }
    }

    #[inline]
    fn manifest_persist_interval() -> Duration {
        if cfg!(debug_assertion) {
            return Duration::from_secs(3);
        }
        MANIFEST_PERSIST_INTERVAL
    }
}

#[cfg(any(test, feature = "testexport"))]
impl IaManager {
    // Flush all tasks and wait for completion.
    pub async fn flush_tasks(&self, timeout: Duration) -> Result<()> {
        let start_time = Instant::now_coarse();
        while start_time.saturating_elapsed() < timeout {
            let pending_tasks = self.fifo.flush_tasks().await?;
            if pending_tasks == 0 {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(Error::IaMgr("wait for pending tasks timeout".to_string()))
    }

    pub async fn get_local_segments(
        &self,
    ) -> Vec<(
        FileSegmentIdent,
        crate::ia::types::FileSegmentData,
        Option<crate::ia::queue::QueueItem>,
    )> {
        let local_segments = self.segments.get_all();
        let mut segments = Vec::with_capacity(local_segments.len());
        // TODO: find segments in queue but not in local store.
        for (ident, segment) in local_segments {
            let queue_item = self.fifo.get_item(ident).await.unwrap();
            segments.push((ident, segment, queue_item));
        }
        segments
    }
}

#[derive(Clone)]
pub(crate) struct SegmentDataContext {
    segments: Arc<LocalSegmentMap>,
    main_store: Arc<dyn LocalStore>,
}

impl SegmentDataContext {
    #[inline]
    pub(crate) fn set_segment_data_from_mem_to_store(
        &self,
        ident: FileSegmentIdent,
    ) -> std::result::Result<(), Option<FileSegmentData>> {
        self.compare_and_set_segment_data(
            ident,
            &FILE_SEGMENT_DATA_IN_MEMORY,
            Some(FileSegmentData::InStore),
        )
    }

    fn is_pos_match(m: &FileSegmentData, n: &FileSegmentData) -> bool {
        matches!(
            (m, n),
            (FileSegmentData::InMem(_), FileSegmentData::InMem(_))
                | (FileSegmentData::InStore, FileSegmentData::InStore)
        )
    }

    fn compare_and_set_segment_data(
        &self,
        ident: FileSegmentIdent,
        expected: &FileSegmentData,
        segment_data: Option<FileSegmentData>,
    ) -> std::result::Result<(), Option<FileSegmentData>> {
        self.segments
            .compare_and_set(ident, expected, segment_data, Self::is_pos_match)
    }

    #[inline]
    pub(crate) fn get_segment_data(&self, ident: FileSegmentIdent) -> Option<FileSegmentData> {
        self.segments.get_segment(ident)
    }

    #[inline]
    pub(crate) fn remove_segment_data(&self, ident: FileSegmentIdent) -> Option<FileSegmentData> {
        self.segments.remove(ident)
    }

    #[inline]
    pub(crate) fn remove_from_main_store(&self, ident: FileSegmentIdent) -> Result<Option<()>> {
        self.main_store
            .remove(ident.file_id, &ident.local_filename())
    }

    #[inline]
    pub(crate) fn save_to_main_store(&self, ident: FileSegmentIdent, bytes: Bytes) -> Result<()> {
        self.main_store
            .save(ident.file_id, &ident.local_filename(), bytes)
    }

    #[cfg(test)]
    pub(crate) fn new_for_test() -> Self {
        Self {
            main_store: Arc::new(crate::ia::util::LocalMemoryStore::default()),
            segments: Arc::new(LocalSegmentMap::default()),
        }
    }
}

const MANIFEST_FORMAT_VER: u8 = 1;
const MANIFEST_FILE_NAME: &str = "manifest";

#[derive(Debug, PartialEq)]
struct Manifest {
    main_queue_cap: i64,
    // TODO: main queue snapshot
}

impl Manifest {
    fn marshal(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(U8_SIZE /* ver */ + I64_SIZE /* main_queue_cap */);
        buf.put_u8(MANIFEST_FORMAT_VER);
        buf.put_i64_le(self.main_queue_cap);
        buf
    }

    fn unmarshal(mut data: &[u8]) -> Result<Self> {
        if data.len() < U8_SIZE + I64_SIZE {
            return Err(Error::IaMgr("manifest data too small".to_string()));
        }

        let ver = data.get_u8();
        if ver != MANIFEST_FORMAT_VER {
            return Err(Error::IaMgr(format!("manifest version not match: {ver}")));
        }

        let main_queue_cap = data.get_i64_le();
        Ok(Self { main_queue_cap })
    }

    fn persist_to_path(&self, path: &Path) -> std::io::Result<()> {
        let mut f = tempfile::Builder::new()
            .prefix(MANIFEST_FILE_NAME)
            .tempfile_in(path)?;
        f.write_all(&self.marshal())?;
        f.flush()?;
        f.persist(path.join(MANIFEST_FILE_NAME))?;
        Ok(())
    }

    fn read_from_path(path: &Path) -> Result<Option<Self>> {
        let mut f = match std::fs::File::open(path.join(MANIFEST_FILE_NAME)) {
            Ok(f) => f,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(err) => return Err(Error::IaMgr(format!("open manifest error {err:?}"))),
        };
        let mut data = vec![];
        f.read_to_end(&mut data)
            .map_err(|e| Error::IaMgr(format!("read manifest error {e:?}")))?;
        Ok(Some(Self::unmarshal(&data)?))
    }
}

#[cfg(any(test, feature = "testexport"))]
#[cfg_attr(feature = "testexport", allow(unused))]
pub mod tests {
    use super::*;

    #[test]
    fn test_manifest() {
        let dir = tempfile::tempdir().unwrap();

        assert!(Manifest::read_from_path(dir.path()).unwrap().is_none());

        let manifest = Manifest {
            main_queue_cap: 1024,
        };
        manifest
            .persist_to_path(dir.path())
            .expect("persist manifest");

        let manifest1 = Manifest::read_from_path(dir.path()).unwrap().unwrap();
        assert_eq!(manifest1, manifest);
    }
}
