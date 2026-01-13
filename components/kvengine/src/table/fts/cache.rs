// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    any::{Any, TypeId},
    borrow::Borrow,
    future::Future,
    hash::{Hash, Hasher},
    mem::size_of,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use anyhow::Result;
use clara_fts::IndexReader as ClaraIndexReader;
use quick_cache::{Equivalent, Weighter, sync::Cache};
use tikv_util::sys::SysQuota;

use crate::table::{
    file::FileMmapGuard,
    fts::{EPackedFileLp, HBlockAccessor, IntPk, PackedFileDataBlockAccessor},
};

#[derive(Clone, PartialEq, Eq)]
enum CacheKey {
    DedicatedHBlock { file_id: u64, block_idx: u64 },
    DedicatedIndex { file_id: u64 },
    PackedLp { file_id: u64, lp_key: Vec<u8> },
    PackedDBlock { file_id: u64, block_idx: u64 },
    PackedIndex { file_id: u64, lp_key: Vec<u8> },
}

impl CacheKey {
    fn as_ref(&self) -> CacheKeyRef<'_> {
        match self {
            CacheKey::DedicatedHBlock { file_id, block_idx } => CacheKeyRef::DedicatedHBlock {
                file_id: *file_id,
                block_idx: *block_idx,
            },
            CacheKey::DedicatedIndex { file_id } => {
                CacheKeyRef::DedicatedIndex { file_id: *file_id }
            }
            CacheKey::PackedLp { file_id, lp_key } => CacheKeyRef::PackedLp {
                file_id: *file_id,
                lp_key: lp_key.as_slice(),
            },
            CacheKey::PackedDBlock { file_id, block_idx } => CacheKeyRef::PackedDBlock {
                file_id: *file_id,
                block_idx: *block_idx,
            },
            CacheKey::PackedIndex { file_id, lp_key } => CacheKeyRef::PackedIndex {
                file_id: *file_id,
                lp_key: lp_key.as_slice(),
            },
        }
    }
}

#[derive(Hash, PartialEq, Eq, Clone, Copy)]
pub enum CacheKeyRef<'a> {
    DedicatedHBlock { file_id: u64, block_idx: u64 },
    DedicatedIndex { file_id: u64 },
    PackedLp { file_id: u64, lp_key: &'a [u8] },
    PackedDBlock { file_id: u64, block_idx: u64 },
    PackedIndex { file_id: u64, lp_key: &'a [u8] },
}

impl CacheKeyRef<'_> {
    fn to_owned(&self) -> CacheKey {
        match self {
            CacheKeyRef::DedicatedHBlock { file_id, block_idx } => CacheKey::DedicatedHBlock {
                file_id: *file_id,
                block_idx: *block_idx,
            },
            CacheKeyRef::DedicatedIndex { file_id } => {
                CacheKey::DedicatedIndex { file_id: *file_id }
            }
            CacheKeyRef::PackedLp { file_id, lp_key } => CacheKey::PackedLp {
                file_id: *file_id,
                lp_key: lp_key.to_vec(),
            },
            CacheKeyRef::PackedDBlock { file_id, block_idx } => CacheKey::PackedDBlock {
                file_id: *file_id,
                block_idx: *block_idx,
            },
            CacheKeyRef::PackedIndex { file_id, lp_key } => CacheKey::PackedIndex {
                file_id: *file_id,
                lp_key: lp_key.to_vec(),
            },
        }
    }
}

impl CacheKey {
    #[inline]
    fn key_weight(&self) -> u64 {
        const BASE: u64 = size_of::<CacheKey>() as u64;
        match self {
            Self::PackedIndex { lp_key, .. } => BASE + lp_key.len() as u64,
            Self::PackedLp { lp_key, .. } => BASE + lp_key.len() as u64,
            _ => BASE,
        }
    }

    #[inline]
    fn value_weight(&self) -> u64 {
        match self {
            Self::DedicatedHBlock { .. } => size_of::<HBlockAccessor<IntPk>>() as u64,
            Self::DedicatedIndex { .. } => size_of::<ClaraIndexReader>() as u64,
            Self::PackedLp { .. } => size_of::<EPackedFileLp>() as u64,
            Self::PackedDBlock { .. } => size_of::<PackedFileDataBlockAccessor>() as u64,
            Self::PackedIndex { .. } => size_of::<ClaraIndexReader>() as u64,
        }
    }

    #[inline]
    fn total_weight(&self) -> u64 {
        self.key_weight() + self.value_weight()
    }
}

#[derive(Clone)]
pub struct FtsCache(Option<Arc<FtsCacheCore>>);

/// Configuration for [`FtsCache`].
#[derive(Clone, Copy, Debug)]
pub struct FtsCacheConfig {
    /// How often the cache scans and removes invalid entries.
    ///
    /// Setting this to `Duration::ZERO` disables the background cleanup task.
    pub cleanup_interval: Duration,

    /// Maximum capacity of the cache in bytes.
    pub max_capacity_bytes: u64,
}

impl Default for FtsCacheConfig {
    fn default() -> Self {
        Self {
            cleanup_interval: Duration::from_secs(10),
            max_capacity_bytes: 256 * 1024 * 1024,
        }
    }
}

/// A value to be inserted into [`FtsCache`].
///
/// It couples the cached value with a set of [`FileMmapGuard`]s so that the
/// cache can proactively drop entries once the underlying IA segments are moved
/// or evicted (even though the mmapped bytes remain readable).
pub struct FtsCacheValue<T> {
    pub value: Arc<T>,
    pub mmap_guards: Vec<FileMmapGuard>,
}

impl<T> FtsCacheValue<T> {
    #[inline]
    pub fn new(value: Arc<T>) -> Self {
        Self {
            value,
            mmap_guards: Vec::new(),
        }
    }

    #[inline]
    pub fn with_mmap_guard(value: Arc<T>, guard: FileMmapGuard) -> Self {
        Self {
            value,
            mmap_guards: vec![guard],
        }
    }

    #[inline]
    pub fn with_mmap_guards(value: Arc<T>, mmap_guards: Vec<FileMmapGuard>) -> Self {
        Self { value, mmap_guards }
    }
}

#[derive(Clone)]
struct CacheEntry {
    value: Arc<dyn Any + Send + Sync>,
    mmap_guards: Arc<[FileMmapGuard]>,
}

impl CacheEntry {
    #[inline]
    fn is_valid(&self) -> bool {
        self.mmap_guards.iter().all(FileMmapGuard::is_valid)
    }
}

struct FtsCacheCore {
    cache: Arc<Cache<TypedKey, CacheEntry, CacheWeighter>>,
    cleanup_handle: RwLock<Option<tokio::task::JoinHandle<()>>>,
    handle: tokio::runtime::Handle,
    config: FtsCacheConfig,
}

/// Drop the cleanup task when the cache core is dropped.
impl Drop for FtsCacheCore {
    fn drop(&mut self) {
        if let Ok(guard) = self.cleanup_handle.try_read() {
            if let Some(handle) = guard.as_ref() {
                handle.abort();
            }
        }
    }
}

impl Default for FtsCache {
    #[inline]
    fn default() -> Self {
        Self(None)
    }
}

impl FtsCache {
    pub fn new(config: FtsCacheConfig, handle: tokio::runtime::Handle) -> Self {
        if config.max_capacity_bytes == 0 {
            return Self::disabled();
        }

        let cache_shards = (SysQuota::cpu_cores_quota() as usize).max(1) * 8;
        let estimated_items_capacity = (config.max_capacity_bytes as usize
            / (size_of::<TypedKey>() + size_of::<CacheEntry>()).max(1))
        .max(1);

        let opts = quick_cache::OptionsBuilder::new()
            .shards(cache_shards)
            .weight_capacity(config.max_capacity_bytes)
            .estimated_items_capacity(estimated_items_capacity)
            .build()
            .unwrap();
        let cache = Cache::with_options(
            opts,
            CacheWeighter,
            quick_cache::DefaultHashBuilder::default(),
            quick_cache::sync::DefaultLifecycle::default(),
        );
        let core = Arc::new(FtsCacheCore {
            cache: Arc::new(cache),
            cleanup_handle: RwLock::new(None),
            handle,
            config,
        });
        let cache = Self(Some(core));
        cache.start_cleanup_task();
        cache
    }

    pub fn disabled() -> Self {
        Self(None)
    }

    #[inline]
    pub fn get<T>(&self, key: CacheKeyRef<'_>) -> Option<Arc<T>>
    where
        T: 'static + Send + Sync,
    {
        let cache_key = TypedKeyRef(TypeId::of::<T>(), key);
        match self.0.as_ref() {
            Some(core) => core.cache.get::<dyn LendTypedKey>(&cache_key).map(|entry| {
                if !entry.is_valid() {
                    let _ = core.cache.remove::<dyn LendTypedKey>(&cache_key);
                }
                Self::arc_downcast(entry.value)
            }),
            None => None,
        }
    }

    #[inline]
    pub fn get_with<T>(
        &self,
        key: CacheKeyRef<'_>,
        init: impl FnOnce() -> Result<FtsCacheValue<T>>,
    ) -> Result<Arc<T>>
    where
        T: 'static + Send + Sync,
    {
        let cache_key = TypedKeyRef(TypeId::of::<T>(), key);
        match self.0.as_ref() {
            Some(core) => {
                let entry = core
                    .cache
                    .get_or_insert_with::<dyn LendTypedKey, _>(&cache_key, || {
                        init().map(Self::cache_entry_from_value)
                    })?;
                if !entry.is_valid() {
                    let _ = core.cache.remove::<dyn LendTypedKey>(&cache_key);
                }
                Ok(Self::arc_downcast(entry.value))
            }
            None => Ok(init()?.value),
        }
    }

    #[inline]
    pub async fn get_async<T>(
        &self,
        key: CacheKeyRef<'_>,
        init: impl Future<Output = Result<FtsCacheValue<T>>>,
    ) -> Result<Arc<T>>
    where
        T: 'static + Send + Sync,
    {
        let cache_key = TypedKeyRef(TypeId::of::<T>(), key);
        match self.0.as_ref() {
            Some(core) => {
                let entry = core
                    .cache
                    .get_or_insert_async::<dyn LendTypedKey, _>(&cache_key, async move {
                        init.await.map(Self::cache_entry_from_value)
                    })
                    .await?;
                if !entry.is_valid() {
                    let _ = core.cache.remove::<dyn LendTypedKey>(&cache_key);
                }
                Ok(Self::arc_downcast(entry.value))
            }
            None => Ok(init.await?.value),
        }
    }

    /// Unlike `get_async`, returning None in `init` will not cache any value.
    #[inline]
    pub async fn get_async_opt<T>(
        &self,
        key: CacheKeyRef<'_>,
        init: impl Future<Output = Result<Option<FtsCacheValue<T>>>>,
    ) -> Result<Option<Arc<T>>>
    where
        T: 'static + Send + Sync,
    {
        let cache_key = TypedKeyRef(TypeId::of::<T>(), key);
        match self.0.as_ref() {
            Some(core) => match core
                .cache
                .get_value_or_guard_async::<dyn LendTypedKey>(&cache_key)
                .await
            {
                Ok(entry) => {
                    if !entry.is_valid() {
                        let _ = core.cache.remove::<dyn LendTypedKey>(&cache_key);
                    }
                    Ok(Some(Self::arc_downcast(entry.value)))
                }
                Err(g) => {
                    let v = init.await?;
                    match v {
                        None => Ok(None),
                        Some(value) => {
                            let typed_v = value.value.clone();
                            let entry = Self::cache_entry_from_value(value);
                            let _ = g.insert(entry);
                            Ok(Some(typed_v))
                        }
                    }
                }
            },
            None => Ok(init.await?.map(|v| v.value)),
        }
    }

    #[inline]
    fn arc_downcast<T>(value: Arc<dyn Any + Send + Sync>) -> Arc<T>
    where
        T: 'static + Send + Sync,
    {
        value.downcast::<T>().unwrap()
    }

    #[inline]
    fn arc_erase<T>(value: Arc<T>) -> Arc<dyn Any + Send + Sync>
    where
        T: 'static + Send + Sync,
    {
        value as Arc<dyn Any + Send + Sync>
    }

    #[inline]
    fn cache_entry_from_value<T>(value: FtsCacheValue<T>) -> CacheEntry
    where
        T: 'static + Send + Sync,
    {
        CacheEntry {
            value: Self::arc_erase(value.value),
            mmap_guards: value.mmap_guards.into(),
        }
    }

    fn start_cleanup_task(&self) {
        let Some(core) = self.0.as_ref() else {
            return;
        };
        if core.config.cleanup_interval == Duration::ZERO {
            return;
        }

        let cache = core.cache.clone();
        let interval = core.config.cleanup_interval;
        let handle = core.handle.spawn(async move {
            let mut interval = tokio::time::interval(interval);
            loop {
                interval.tick().await;
                Self::cleanup_invalid_entries(&cache);
            }
        });

        if let Ok(mut guard) = core.cleanup_handle.try_write() {
            *guard = Some(handle);
        }
    }

    fn cleanup_invalid_entries(cache: &Cache<TypedKey, CacheEntry, CacheWeighter>) {
        let start_time = Instant::now();
        let mut expired_count = 0usize;

        for (key, entry) in cache.iter() {
            if !entry.is_valid() {
                let _ = cache.remove(&key);
                expired_count += 1;
            }
        }

        if expired_count > 0 {
            info!(
                "fts cache cleanup completed";
                "expired_count" => expired_count,
                "duration_ms" => start_time.elapsed().as_millis(),
                "cache_len" => cache.len(),
                "cache_weight" => cache.weight(),
            );
        }
    }
}

#[derive(Clone)]
struct CacheWeighter;

impl Weighter<TypedKey, CacheEntry> for CacheWeighter {
    #[inline]
    fn weight(&self, key: &TypedKey, val: &CacheEntry) -> u64 {
        // We intentionally avoid counting the size of mmapped bytes as cache
        // weight, since the OS can reclaim the page cache under memory pressure.
        // Keep a small per-entry weight to prevent unbounded growth.
        let guards_weight =
            (val.mmap_guards.len() as u64).saturating_mul(size_of::<FileMmapGuard>() as u64);
        key.1.total_weight().saturating_add(guards_weight)
    }
}

/// The actual cache key, which carries the type id to be type safe.
#[derive(Clone, PartialEq, Eq)]
struct TypedKey(TypeId, CacheKey);

/// The actual cache key to query with. No need to own the key data.
#[derive(Hash, PartialEq, Eq, Clone, Copy)]
struct TypedKeyRef<'a>(TypeId, CacheKeyRef<'a>);

// Below are using tricks from https://quinedot.github.io/rust-learning/dyn-trait-borrow.html

trait LendTypedKey: Send + Sync {
    fn lend(&self) -> TypedKeyRef<'_>;
}

impl Hash for TypedKey {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        let r = TypedKeyRef(self.0, self.1.as_ref());
        r.hash(state);
    }
}

impl ToOwned for dyn LendTypedKey + '_ {
    type Owned = TypedKey;
    #[inline]
    fn to_owned(&self) -> TypedKey {
        let lended = self.lend();
        TypedKey(lended.0, lended.1.to_owned())
    }
}

impl LendTypedKey for TypedKey {
    #[inline]
    fn lend(&self) -> TypedKeyRef<'_> {
        TypedKeyRef(self.0, self.1.as_ref())
    }
}

impl LendTypedKey for TypedKeyRef<'_> {
    #[inline]
    fn lend(&self) -> TypedKeyRef<'_> {
        TypedKeyRef(self.0, self.1)
    }
}

impl<'a> Borrow<dyn LendTypedKey + 'a> for TypedKey {
    #[inline]
    fn borrow(&self) -> &(dyn LendTypedKey + 'a) {
        self
    }
}

impl<'a, 'b: 'a> Borrow<dyn LendTypedKey + 'a> for TypedKeyRef<'b> {
    #[inline]
    fn borrow(&self) -> &(dyn LendTypedKey + 'a) {
        self
    }
}

impl Hash for dyn LendTypedKey + '_ {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.lend().hash(state);
    }
}

impl Equivalent<TypedKey> for dyn LendTypedKey + '_ {
    #[inline]
    fn equivalent(&self, key: &TypedKey) -> bool {
        let lended = self.lend();
        lended.0 == key.0 && lended.1 == key.1.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use anyhow::Result;

    use super::*;

    #[derive(Debug)]
    struct Foo(u32);

    #[derive(Debug)]
    struct Bar;

    #[tokio::test]
    async fn cache_entries_are_type_isolated() -> Result<()> {
        let cache = FtsCache::new(FtsCacheConfig::default(), tokio::runtime::Handle::current());
        let key = CacheKeyRef::DedicatedIndex { file_id: 7 };
        let inits = AtomicUsize::new(0);

        let cached = cache.get_with::<Foo>(key, || {
            inits.fetch_add(1, Ordering::SeqCst);
            Ok(FtsCacheValue::new(Arc::new(Foo(42))))
        })?;
        assert_eq!(inits.load(Ordering::SeqCst), 1);

        let cached_again = cache.get::<Foo>(key).expect("entry should exist");
        assert!(Arc::ptr_eq(&cached, &cached_again));

        // Different value type with the same logical key should result in a miss.
        assert!(cache.get::<Bar>(key).is_none());

        // Re-fetching shouldn't invoke the initializer again.
        let cached_third = cache.get_with::<Foo>(key, || panic!("should not reinitialize"))?;
        assert!(Arc::ptr_eq(&cached, &cached_third));
        assert_eq!(inits.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn borrowed_slice_keys_can_lookup() -> Result<()> {
        let cache = FtsCache::new(FtsCacheConfig::default(), tokio::runtime::Handle::current());
        let owned_key = b"lp-key".to_vec();
        let first_key = CacheKeyRef::PackedIndex {
            file_id: 10,
            lp_key: owned_key.as_slice(),
        };

        let cached =
            cache.get_with::<Foo>(first_key, || Ok(FtsCacheValue::new(Arc::new(Foo(7)))))?;
        drop(owned_key);

        // Lookup using a different borrowed slice that matches in content.
        let lookup_key = CacheKeyRef::PackedIndex {
            file_id: 10,
            lp_key: b"lp-key",
        };
        let cached_again = cache.get::<Foo>(lookup_key).expect("entry should exist");
        assert!(Arc::ptr_eq(&cached, &cached_again));
        Ok(())
    }

    #[tokio::test]
    async fn disabled_cache_bypasses_storage() -> Result<()> {
        let cache = FtsCache::disabled();
        let key = CacheKeyRef::DedicatedHBlock {
            file_id: 3,
            block_idx: 5,
        };
        let inits = AtomicUsize::new(0);

        for _ in 0..2 {
            let _ = cache.get_with::<Foo>(key, || {
                inits.fetch_add(1, Ordering::SeqCst);
                Ok(FtsCacheValue::new(Arc::new(Foo(1))))
            })?;
        }

        assert_eq!(inits.load(Ordering::SeqCst), 2);
        assert!(cache.get::<Foo>(key).is_none());
        Ok(())
    }

    #[tokio::test]
    async fn async_initializer_runs_once() -> Result<()> {
        let cache = FtsCache::new(FtsCacheConfig::default(), tokio::runtime::Handle::current());
        let key = CacheKeyRef::PackedDBlock {
            file_id: 55,
            block_idx: 2,
        };
        let inits = Arc::new(AtomicUsize::new(0));

        let first = cache
            .get_async::<Foo>(key, {
                let inits = Arc::clone(&inits);
                async move {
                    inits.fetch_add(1, Ordering::SeqCst);
                    Ok(FtsCacheValue::new(Arc::new(Foo(8))))
                }
            })
            .await?;

        let second = cache
            .get_async::<Foo>(key, {
                let inits = Arc::clone(&inits);
                async move {
                    inits.fetch_add(1, Ordering::SeqCst);
                    Ok(FtsCacheValue::new(Arc::new(Foo(9))))
                }
            })
            .await?;

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(inits.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn async_optional_insertion() -> Result<()> {
        let cache = FtsCache::new(FtsCacheConfig::default(), tokio::runtime::Handle::current());
        let key_some = CacheKeyRef::PackedIndex {
            file_id: 88,
            lp_key: b"some",
        };
        let key_none = CacheKeyRef::PackedIndex {
            file_id: 88,
            lp_key: b"none",
        };
        let inits = Arc::new(AtomicUsize::new(0));

        // Insert an entry with Some(..) and make sure subsequent calls reuse it.
        let first = cache
            .get_async_opt::<Foo>(key_some, {
                let inits = Arc::clone(&inits);
                async move {
                    inits.fetch_add(1, Ordering::SeqCst);
                    Ok(Some(FtsCacheValue::new(Arc::new(Foo(11)))))
                }
            })
            .await?
            .expect("should insert value");

        let second = cache
            .get_async_opt::<Foo>(key_some, async {
                panic!("init must not run for cached value");
            })
            .await?
            .expect("cached value should be returned");
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(inits.load(Ordering::SeqCst), 1);

        // Returning None should skip insertion entirely.
        let none_result = cache
            .get_async_opt::<Foo>(key_none, {
                let inits = Arc::clone(&inits);
                async move {
                    inits.fetch_add(1, Ordering::SeqCst);
                    Ok(None)
                }
            })
            .await?;
        assert!(none_result.is_none());
        assert!(cache.get::<Foo>(key_none).is_none());

        // A later attempt that returns Some should now insert successfully.
        let inserted = cache
            .get_async_opt::<Foo>(key_none, {
                let inits = Arc::clone(&inits);
                async move {
                    inits.fetch_add(1, Ordering::SeqCst);
                    Ok(Some(FtsCacheValue::new(Arc::new(Foo(12)))))
                }
            })
            .await?
            .expect("value should be inserted now");
        assert_eq!(inserted.0, 12);
        assert_eq!(inits.load(Ordering::SeqCst), 3);
        Ok(())
    }

    #[tokio::test]
    async fn invalid_mmap_guard_evicts_on_access() -> Result<()> {
        let guard_valid = Arc::new(AtomicBool::new(true));
        let cache = FtsCache::new(
            FtsCacheConfig {
                cleanup_interval: Duration::ZERO,
                max_capacity_bytes: 1024 * 1024,
            },
            tokio::runtime::Handle::current(),
        );
        let key = CacheKeyRef::DedicatedIndex { file_id: 42 };

        let first = cache.get_with::<Foo>(key, || {
            Ok(FtsCacheValue::with_mmap_guard(
                Arc::new(Foo(123)),
                FileMmapGuard::Test(guard_valid.clone()),
            ))
        })?;
        assert_eq!(cache.0.as_deref().unwrap().cache.len(), 1);

        guard_valid.store(false, Ordering::SeqCst);

        // The stale entry can still be returned this time, but it must be
        // removed from the cache so it won't be held for long.
        let stale = cache.get::<Foo>(key).expect("entry should exist");
        assert!(Arc::ptr_eq(&first, &stale));
        assert_eq!(cache.0.as_deref().unwrap().cache.len(), 0);
        assert!(cache.get::<Foo>(key).is_none());
        Ok(())
    }

    #[tokio::test]
    async fn invalid_mmap_guard_evicts_in_background() -> Result<()> {
        let guard_valid = Arc::new(AtomicBool::new(true));
        let cache = FtsCache::new(
            FtsCacheConfig {
                cleanup_interval: Duration::from_millis(10),
                max_capacity_bytes: 1024 * 1024,
            },
            tokio::runtime::Handle::current(),
        );
        let key = CacheKeyRef::DedicatedIndex { file_id: 43 };

        let _ = cache.get_with::<Foo>(key, || {
            Ok(FtsCacheValue::with_mmap_guard(
                Arc::new(Foo(123)),
                FileMmapGuard::Test(guard_valid.clone()),
            ))
        })?;
        assert_eq!(cache.0.as_deref().unwrap().cache.len(), 1);

        guard_valid.store(false, Ordering::SeqCst);

        tokio::time::timeout(Duration::from_secs(1), async {
            while !cache.0.as_deref().unwrap().cache.is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("background cleanup should remove invalid entries");

        // Ensure the eviction is not caused by access-time invalidation.
        assert!(cache.get::<Foo>(key).is_none());
        Ok(())
    }
}
