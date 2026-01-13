// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    ops::Deref,
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
};

use aligned_vec::{AVec, ConstAlign};
use bytes::Bytes;
use memmap2::Mmap;
use quick_cache::sync::GuardResult;
use schema::schema::StorageClass;

use crate::{
    error::IoContext,
    ia::{ia_file::IaMmapSource, types::FileSegmentIdent},
    table::table,
};

/// A guard that describes whether the underlying entity of a mmapped `Bytes`
/// slice is still the expected one.
///
/// For non-IA files this is always [`FileMmapGuard::None`].
///
/// For IA files, the returned `Bytes` may pin in-memory data or local segment
/// files (via mmap). If you keep the returned `Bytes` (or any slices derived
/// from them) for a long time (e.g. storing into a cache, or building a
/// Tantivy directory/index reader and then caching it), you **must** keep the
/// corresponding [`FileMmapGuard`] alongside and proactively drop the cached
/// value once [`FileMmapGuard::is_valid`] becomes `false`. Otherwise IA may
/// unlink/evict segment files while the mmap is still alive, and the disk
/// space (inode/blocks) will not be reclaimed until the last `Bytes` reference
/// is dropped.
#[derive(Clone)]
pub enum FileMmapGuard {
    None,
    Ia(IaMmapSource),
    #[cfg(test)]
    Test(Arc<std::sync::atomic::AtomicBool>),
}

impl FileMmapGuard {
    /// Returns whether the mmapped bytes are still backed by the expected
    /// underlying entity.
    ///
    /// Note: even when this returns `false`, previously returned `Bytes` are
    /// still readable. Callers should drop them as soon as possible to release
    /// pinned memory or disk space (inode/blocks).
    #[inline]
    pub fn is_valid(&self) -> bool {
        match self {
            FileMmapGuard::None => true,
            FileMmapGuard::Ia(source) => source.is_valid(),
            #[cfg(test)]
            FileMmapGuard::Test(valid) => valid.load(Relaxed),
        }
    }
}

#[async_trait::async_trait]
pub trait File: std::any::Any + Sync + Send {
    // id returns the id of the file.
    fn id(&self) -> u64;

    /// `size` returns the size of the file.
    fn size(&self) -> u64;

    /// `path` returns the file path.
    fn path(&self) -> Option<PathBuf> {
        None
    }

    /// `is_sync` indicates that whether the whole file is local and can always
    /// be read by sync methods.
    fn is_sync(&self) -> bool {
        true
    }

    /// `read` reads the data at given offset.
    fn read(&self, off: u64, length: usize) -> table::Result<Bytes>;

    /// `read_at` reads the data to the buffer.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> table::Result<()>;

    fn read_all(&self) -> table::Result<Bytes> {
        self.read(0, self.size() as usize)
    }

    /// `read_table_meta` read meta (e.g, index, filter) of tables.
    ///
    /// `read_table_meta` has the same result of `read`. But some implementation
    /// can have better performance (e.g. `IaFile`).
    fn read_table_meta(&self, off: u64, length: usize) -> table::Result<Bytes> {
        self.read(off, length)
    }

    fn read_footer(&self, footer_length: usize) -> table::Result<Bytes> {
        let size = self.size();
        let Some(off) = size.checked_sub(footer_length as u64) else {
            error!("invalid file size"; "file_id" => self.id(), "size" => size, "footer_length" => footer_length);
            return Err(table::Error::InvalidFileSize);
        };
        self.read_table_meta(off, footer_length)
    }

    /// `read_async` is async version of `read`.
    async fn read_async(&self, off: u64, length: usize) -> table::Result<Bytes> {
        self.read(off, length)
    }

    /// `read_at_async` is async version of `read_at`.
    async fn read_at_async(&self, buf: &mut [u8], offset: u64) -> table::Result<()> {
        self.read_at(buf, offset)
    }

    /// Deprecated. Recommend to use mmap2 instead.
    fn mmap(&self) -> table::Result<MmapData>;

    /// Create a memory-map of the whole file.
    /// The memory-map is valid even if the file is dropped, closed or removed.
    fn mmap2(&self) -> table::Result<Bytes>;

    /// TODO: Remove. We should provide this method on `IaFile` only.
    async fn mmap_async(&self) -> table::Result<MmapData> {
        unimplemented!()
    }

    /// Mmaps a range of this file, returning both the `Bytes` and a guard.
    ///
    /// Callers that keep the returned `Bytes` for a long time must keep the
    /// returned [`FileMmapGuard`] alongside and use it to invalidate/drop the
    /// cached value when needed.
    async fn mmap_range(&self, offset: u64, length: usize)
    -> table::Result<(Bytes, FileMmapGuard)>;

    /// `get_remote_segments` returns the remote segments of the file. Used for
    /// prefetching. Available for IA files only.
    fn get_remote_segments(
        &self,
        _ranges: &[(u64 /* start_off */, u64 /* end_off */)],
    ) -> table::Result<(Vec<FileSegmentIdent>, usize /* total_segments */)> {
        Ok((vec![], 0))
    }

    /// `get_segment_ident` returns the segment ident of the file. Used for
    /// getting the segment ident of the file. Available for IA files only.
    fn get_segment_ident(&self, _offset: u64) -> table::Result<FileSegmentIdent> {
        unimplemented!()
    }

    /// Note: the result is NOT strongly consistent.
    fn storage_class(&self) -> StorageClass;

    /// Cast to `Any`. Used for downcast.
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync>;

    /// `mem_size` returns the size of the file in memory. Used for cache
    /// weight calculation.
    fn mem_size(&self) -> u64 {
        0
    }
}

pub enum MmapData {
    /// Data backed by a memory-mapped local file.
    ///
    /// Note: this variant is **not** considered "in-memory" for cache
    /// invalidation and cache weight calculation. The OS pages the data in/out
    /// on demand.
    Local(Bytes),
    /// Data backed by an in-memory buffer.
    InMem(Bytes),
    AlignedInMem(AVec<u8, ConstAlign<8>>),
}

impl Default for MmapData {
    fn default() -> Self {
        MmapData::InMem(Bytes::new())
    }
}

impl Deref for MmapData {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            MmapData::Local(data) => data.deref(),
            MmapData::InMem(data) => data.deref(),
            MmapData::AlignedInMem(data) => data.deref(),
        }
    }
}

impl MmapData {
    pub fn to_aligned(self) -> MmapData {
        match self {
            MmapData::InMem(data) if data.as_ptr() as usize % 8 != 0 => {
                let mut avec = AVec::<u8, ConstAlign<8>>::with_capacity(8, data.len());
                avec.extend_from_slice(&data);
                MmapData::AlignedInMem(avec)
            }
            data => data,
        }
    }
}

pub struct LocalFile {
    id: u64,
    size: u64,
    path: PathBuf,
    mmap_bytes: Mutex<Option<Bytes>>,
    fd_cache: FdCache,
}

impl LocalFile {
    pub fn open(id: u64, path: PathBuf) -> table::Result<LocalFile> {
        Self::open_ext(id, path, None, None, false)
    }

    pub fn open_ext(
        id: u64,
        path: PathBuf,
        size_opt: Option<u64>,
        fd_cache_opt: Option<FdCache>,
        set_mtime: bool,
    ) -> table::Result<LocalFile> {
        let fd_cache = match fd_cache_opt {
            None => {
                let fd =
                    std::fs::File::open(path.as_path()).table_ctx(id, "local.open.open_file")?;
                let fd_cache = FdCache::new(2);
                fd_cache.insert(id, Arc::new(fd));
                fd_cache
            }
            Some(fd_cache) => fd_cache,
        };
        if set_mtime {
            filetime::set_file_mtime(path.as_path(), filetime::FileTime::now())
                .table_ctx(id, "local.open.set_file_mtime")?;
        }
        let size = match size_opt {
            Some(s) => s,
            None => std::fs::metadata(path.as_path())
                .table_ctx(id, "local.open.metadata")?
                .size(),
        };
        let local_file = LocalFile {
            id,
            size,
            path,
            mmap_bytes: Mutex::new(None),
            fd_cache,
        };
        Ok(local_file)
    }

    pub fn from_file(id: u64, path: PathBuf, file: Arc<std::fs::File>) -> table::Result<LocalFile> {
        // The file may have been unlinked after it was opened (e.g. due to cache
        // eviction), while the file descriptor is still valid. Use fd metadata
        // instead of path metadata to avoid spurious ENOENT.
        let meta = file.metadata().table_ctx(id, "local.from_file.metadata")?;
        let fd_cache = FdCache::new(2);
        fd_cache.insert(id, file);
        let local_file = LocalFile {
            id,
            size: meta.size(),
            path,
            mmap_bytes: Mutex::new(None),
            fd_cache,
        };
        Ok(local_file)
    }

    fn get_file(&self) -> table::Result<Arc<std::fs::File>> {
        self.fd_cache.get(self.id, self.path.as_path())
    }

    fn mmap_cached(&self) -> table::Result<Bytes> {
        let mut guard = self.mmap_bytes.lock().unwrap();
        if let Some(bytes) = guard.as_ref() {
            return Ok(bytes.clone());
        }
        let fd = self.get_file()?;
        let mmap = unsafe { Mmap::map(&fd).table_ctx(self.id(), "local.mmap")? };
        let bytes = Bytes::from_owner(mmap);
        *guard = Some(bytes.clone());
        Ok(bytes)
    }
}

#[async_trait::async_trait]
impl File for LocalFile {
    fn id(&self) -> u64 {
        self.id
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn path(&self) -> Option<PathBuf> {
        Some(self.path.clone())
    }

    fn read(&self, off: u64, length: usize) -> table::Result<Bytes> {
        let mut buf = vec![0; length];
        let fd = self.get_file()?;
        fd.read_exact_at(&mut buf, off)
            .table_ctx(self.id(), "local.read")?;
        Ok(Bytes::from(buf))
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> table::Result<()> {
        let fd = self.get_file()?;
        fd.read_exact_at(buf, offset)
            .table_ctx(self.id(), "local.read_at")?;
        Ok(())
    }

    fn mmap(&self) -> table::Result<MmapData> {
        let bytes = self.mmap_cached()?;
        Ok(MmapData::Local(bytes))
    }

    fn mmap2(&self) -> table::Result<Bytes> {
        self.mmap_cached()
    }

    async fn mmap_range(
        &self,
        offset: u64,
        length: usize,
    ) -> table::Result<(Bytes, FileMmapGuard)> {
        if offset
            .checked_add(length as u64)
            .is_none_or(|end| end > self.size)
        {
            return Err(table::Error::InvalidFileSize);
        }

        if length == 0 {
            return Ok((Bytes::new(), FileMmapGuard::None));
        }

        let bytes = self.mmap_cached()?;
        let slice = bytes.slice(offset as usize..offset as usize + length);

        Ok((slice, FileMmapGuard::None))
    }

    fn storage_class(&self) -> StorageClass {
        StorageClass::Unspecified
    }

    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

#[derive(Clone)]
pub struct InMemFile {
    pub id: u64,
    data: Bytes,
    pub size: u64,
    is_sync: bool,
}

impl fmt::Debug for InMemFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemFile")
            .field("id", &self.id)
            .field("size", &self.size)
            .finish()
    }
}

impl InMemFile {
    pub fn new(id: u64, data: Bytes) -> Self {
        let size = data.len() as u64;
        Self {
            id,
            data,
            size,
            is_sync: true,
        }
    }

    #[cfg(any(test, feature = "testexport"))]
    pub async fn new_async(id: u64, data: Bytes) -> Self {
        let size = data.len() as u64;
        Self {
            id,
            data,
            size,
            is_sync: false,
        }
    }

    fn read_inner(&self, off: u64, length: usize) -> table::Result<Bytes> {
        let off_usize = off as usize;
        Ok(self.data.slice(off_usize..off_usize + length))
    }

    fn read_at_inner(&self, buf: &mut [u8], offset: u64) -> table::Result<()> {
        let off_usize = offset as usize;
        let length = buf.len();
        buf.copy_from_slice(&self.data[off_usize..off_usize + length]);
        Ok(())
    }
}

#[async_trait::async_trait]
impl File for InMemFile {
    fn id(&self) -> u64 {
        self.id
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn is_sync(&self) -> bool {
        self.is_sync
    }

    #[inline]
    fn read(&self, off: u64, length: usize) -> table::Result<Bytes> {
        debug_assert!(self.is_sync);
        self.read_inner(off, length)
    }

    #[inline]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> table::Result<()> {
        debug_assert!(self.is_sync);
        self.read_at_inner(buf, offset)
    }

    fn read_table_meta(&self, off: u64, length: usize) -> table::Result<Bytes> {
        // Skip the `is_sync` checking.
        self.read_inner(off, length)
    }

    #[cfg(test)]
    #[inline]
    async fn read_async(&self, off: u64, length: usize) -> table::Result<Bytes> {
        debug_assert!(!self.is_sync);
        self.read_inner(off, length)
    }

    #[cfg(test)]
    #[inline]
    async fn read_at_async(&self, buf: &mut [u8], offset: u64) -> table::Result<()> {
        debug_assert!(!self.is_sync);
        self.read_at_inner(buf, offset)
    }

    fn mmap(&self) -> table::Result<MmapData> {
        Ok(MmapData::InMem(self.data.clone()))
    }

    fn mmap2(&self) -> table::Result<Bytes> {
        Ok(self.data.clone())
    }

    async fn mmap_range(
        &self,
        offset: u64,
        length: usize,
    ) -> table::Result<(Bytes, FileMmapGuard)> {
        if offset
            .checked_add(length as u64)
            .is_none_or(|end| end > self.size)
        {
            return Err(table::Error::InvalidFileSize);
        }
        let data = if length == 0 {
            Bytes::new()
        } else {
            let off_usize = offset as usize;
            self.data.slice(off_usize..off_usize + length)
        };
        Ok((data, FileMmapGuard::None))
    }

    fn storage_class(&self) -> StorageClass {
        StorageClass::Unspecified
    }

    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

pub struct TtlCache<T> {
    access_ns: AtomicU64,
    data: Mutex<Option<Arc<T>>>,
}

impl<T> Default for TtlCache<T> {
    fn default() -> Self {
        Self {
            access_ns: AtomicU64::new(0),
            data: Default::default(),
        }
    }
}

impl<T> TtlCache<T> {
    pub fn new(t: Arc<T>) -> Self {
        let now_ns = time::precise_time_ns();
        Self {
            access_ns: AtomicU64::new(now_ns),
            data: Mutex::new(Some(t)),
        }
    }

    pub fn get<E>(
        &self,
        init: impl FnOnce() -> std::result::Result<T, E>,
    ) -> std::result::Result<Arc<T>, E> {
        let now_ns = time::precise_time_ns();
        self.access_ns.store(now_ns, Relaxed);
        let mut guard = self.data.lock().unwrap();
        if guard.is_none() {
            let data = init()?;
            *guard = Some(Arc::new(data));
        }
        Ok(guard.as_ref().unwrap().clone())
    }

    pub fn expire(&self, dur_secs: u64) {
        let access_ns = self.access_ns.load(Relaxed);
        let now_ns = time::precise_time_ns();
        let dur_nanos = dur_secs * 1_000_000_000;
        if access_ns > 0 && now_ns.saturating_sub(access_ns) > dur_nanos {
            if let Ok(mut data) = self.data.try_lock() {
                data.take();
                self.access_ns.store(0, Relaxed);
            }
        }
    }

    pub fn is_loaded(&self) -> bool {
        self.access_ns.load(Relaxed) > 0
    }
}

#[derive(Clone)]
pub struct FdCache {
    cache: Arc<quick_cache::sync::Cache<u64, Arc<std::fs::File>>>,
}

impl FdCache {
    pub fn new(capacity: usize) -> Self {
        let cache = quick_cache::sync::Cache::new(capacity);
        Self {
            cache: Arc::new(cache),
        }
    }

    pub fn get(&self, file_id: u64, path: &Path) -> table::Result<Arc<std::fs::File>> {
        match self.cache.get_value_or_guard(&file_id, None) {
            GuardResult::Value(v) => Ok(v.clone()),
            GuardResult::Guard(holder) => {
                let file = Arc::new(std::fs::File::open(path).table_ctx(file_id, "FdCache::get")?);
                let _ = holder.insert(file.clone());
                Ok(file)
            }
            GuardResult::Timeout => unreachable!(),
        }
    }

    pub fn insert(&self, file_id: u64, file: Arc<std::fs::File>) {
        self.cache.insert(file_id, file);
    }

    pub fn remove(&self, file_id: u64) {
        self.cache.remove(&file_id);
    }

    pub fn size(&self) -> usize {
        self.cache.len()
    }
}

#[cfg(test)]
mod tests {
    use std::{intrinsics::black_box, ops::Deref, time::Duration};

    use aligned_vec::{AVec, ConstAlign};
    use bytes::Bytes;
    use rand::Rng;

    use crate::table::file::{MmapData, TtlCache};

    #[test]
    fn test_mmap_data_to_aligned() {
        // Test case 1: InMem data that is already 8-byte aligned
        // Create an 8-byte aligned vector
        let mut base_bytes: Bytes;
        // Guarantee the base_bytes is 8-byte aligned
        loop {
            let rand_len = rand::thread_rng().gen_range(16..=128);
            let mut base_vec = Vec::with_capacity(rand_len);
            for i in 0..rand_len {
                base_vec.push(i as u8);
            }
            base_bytes = Bytes::from(base_vec);
            if base_bytes.as_ptr() as usize % 8 == 0 {
                break;
            }
        }
        let aligned_bytes = base_bytes.slice(0..8);

        assert_eq!(aligned_bytes.as_ptr() as usize % 8, 0);

        let mmap_data = MmapData::InMem(aligned_bytes);
        let result = mmap_data.to_aligned();

        // Should return the original InMem data since it's already aligned
        match result {
            MmapData::InMem(data) => {
                assert_eq!(data.len(), 8);
                assert_eq!(&data[..], &[0, 1, 2, 3, 4, 5, 6, 7]);
            }
            _ => panic!("Expected InMem variant for aligned data"),
        }

        // Test case 2: InMem data that is NOT 8-byte aligned
        // Create a non-aligned byte array by using a slice that starts at an odd
        // position

        let unaligned_bytes = base_bytes.slice(1..9); // Start from index 1 to make it likely unaligned
        assert!(unaligned_bytes.as_ptr() as usize % 8 != 0);

        // Verify the data is not 8-byte aligned (this might not always be true, but
        // we'll test the behavior)
        let mmap_data = MmapData::InMem(unaligned_bytes.clone());
        let result = mmap_data.to_aligned();

        // If it was unaligned, it should now be AlignedInMem; if it was already
        // aligned, it stays InMem
        match result {
            MmapData::AlignedInMem(data) => {
                assert_eq!(data.len(), 8);
                assert_eq!(&data[..], &[1, 2, 3, 4, 5, 6, 7, 8]);
                // Verify the new data is 8-byte aligned
                assert_eq!(data.as_ptr() as usize % 8, 0);
            }
            _ => panic!("Expected AlignedInMem variant"),
        }

        // Test case 3: AlignedInMem data should pass through unchanged
        let mut avec = AVec::<u8, ConstAlign<8>>::with_capacity(8, 8);
        avec.extend_from_slice(&[10, 20, 30, 40, 50, 60, 70, 80]);
        let mmap_data = MmapData::AlignedInMem(avec);
        let result = mmap_data.to_aligned();

        match result {
            MmapData::AlignedInMem(data) => {
                assert_eq!(data.len(), 8);
                assert_eq!(&data[..], &[10, 20, 30, 40, 50, 60, 70, 80]);
                assert_eq!(data.as_ptr() as usize % 8, 0);
            }
            _ => panic!("Expected AlignedInMem variant to pass through unchanged"),
        }
    }

    #[test]
    fn test_ttl_cache() {
        let cache: TtlCache<Vec<u8>> = TtlCache::default();
        assert!(!cache.is_loaded());
        let data = cache.get(|| Ok::<_, ()>(vec![1, 2, 3, 4])).unwrap();
        assert_eq!(data.deref(), &[1, 2, 3, 4]);
        assert!(cache.is_loaded());
        cache.expire(1);
        assert!(cache.is_loaded());
        std::thread::sleep(Duration::from_millis(1500));
        cache.expire(1);
        assert!(!cache.is_loaded());
    }

    #[bench]
    fn bench_ttl_cache(b: &mut test::Bencher) {
        let cache: TtlCache<u64> = TtlCache::default();
        b.iter(|| {
            for _ in 0..1000 {
                black_box(cache.get(|| Ok::<_, ()>(1)).unwrap());
            }
        });
    }
}
