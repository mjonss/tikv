// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ops::Deref,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use quick_cache::{
    DefaultHashBuilder,
    sync::{Cache, DefaultLifecycle},
};
use tikv_util::config::AbsoluteOrPercentSize;

use crate::{
    ia::{
        manager::IaManager,
        types::{FileSegmentIdent, FileSegmentPosition},
    },
    metrics::{ENGINE_VECTOR_INDEX_CACHE_HIT, ENGINE_VECTOR_INDEX_CACHE_MISS},
    table::vector_index::IndexData,
};

#[derive(Clone)]
pub(crate) struct CachedIndexData {
    pub(crate) index_data: Arc<IndexData>,
    pub(crate) segment_ident: FileSegmentIdent,
}

#[derive(Clone)]
pub struct IndexDataWeighter;

impl quick_cache::Weighter<u64, CachedIndexData> for IndexDataWeighter {
    fn weight(&self, _key: &u64, val: &CachedIndexData) -> u64 {
        val.index_data.index_size() as u64 + 1
    }
}

#[derive(Clone)]
pub struct VectorIndexCache {
    core: Arc<VectorIndexCacheCore>,
}

impl Deref for VectorIndexCache {
    type Target = VectorIndexCacheCore;
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

#[derive(Clone)]
pub struct VectorIndexCacheCore {
    cache: Arc<Cache<u64, CachedIndexData, IndexDataWeighter, DefaultHashBuilder>>,
    cleanup_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    handle: tokio::runtime::Handle,
    stats: Arc<VectorIndexCacheStats>,
    config: VectorIndexConfig,
    ia_mgr: IaManager,
}

impl VectorIndexCache {
    pub fn new(
        config: VectorIndexConfig,
        handle: tokio::runtime::Handle,
        ia_mgr: IaManager,
    ) -> Self {
        let cache_cap = config.cache_cap.as_memory_size();
        info!(
            "create vector index cache using config: {:?}, calcualted cache_cap: {}",
            config, cache_cap
        );
        let opts = quick_cache::OptionsBuilder::new()
            .weight_capacity(cache_cap)
            .estimated_items_capacity(config.cache_size)
            .build()
            .unwrap();

        let cache = Cache::with_options(
            opts,
            IndexDataWeighter,
            DefaultHashBuilder::default(),
            DefaultLifecycle::default(),
        );

        let cache_instance = Self {
            core: Arc::new(VectorIndexCacheCore {
                cache: Arc::new(cache),
                cleanup_handle: Arc::new(RwLock::new(None)),
                handle,
                stats: Arc::new(VectorIndexCacheStats {
                    expired: AtomicU64::new(0),
                    cache_hit: AtomicU64::new(0),
                    cache_miss: AtomicU64::new(0),
                }),
                config,
                ia_mgr,
            }),
        };

        cache_instance.start_cleanup_task();
        cache_instance
    }

    pub fn get(&self, file_id: u64) -> Option<Arc<IndexData>> {
        if let Some(cached) = self.cache.get(&file_id) {
            debug!("vector index cache hit"; "file_id" => file_id);
            self.stats.cache_hit.fetch_add(1, Ordering::Relaxed);
            ENGINE_VECTOR_INDEX_CACHE_HIT.inc();
            // If the segment is invalid, we need to remove it from the cache. But we can
            // return it from cache this time.
            if !Self::is_file_valid(cached.segment_ident, &cached, &self.ia_mgr) {
                self.cache.remove(&file_id);
                self.stats.expired.fetch_add(1, Ordering::Relaxed);
            }
            return Some(cached.index_data.clone());
        }
        debug!("vector index cache miss"; "file_id" => file_id);
        self.stats.cache_miss.fetch_add(1, Ordering::Relaxed);
        ENGINE_VECTOR_INDEX_CACHE_MISS.inc();
        None
    }

    pub fn insert(
        &self,
        file_id: u64,
        index_data: Arc<IndexData>,
        segment_ident: FileSegmentIdent,
    ) {
        let cached = CachedIndexData {
            index_data,
            segment_ident,
        };

        self.cache.insert(file_id, cached);
    }

    pub fn stats(&self) -> Arc<VectorIndexCacheStats> {
        self.stats.clone()
    }

    fn start_cleanup_task(&self) {
        let cache = self.cache.clone();
        let stats = self.stats.clone();
        let ia_mgr = self.ia_mgr.clone();
        let interval = self.config.cache_cleanup_interval;
        let handle = self.handle.spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval));
            loop {
                interval.tick().await;
                Self::cleanup_invalid_entries(&cache, &ia_mgr, &stats);
            }
        });

        if let Ok(mut guard) = self.cleanup_handle.try_write() {
            *guard = Some(handle);
        }
    }

    fn cleanup_invalid_entries(
        cache: &Cache<u64, CachedIndexData, IndexDataWeighter>,
        ia_mgr: &IaManager,
        stats: &VectorIndexCacheStats,
    ) {
        let start_time = Instant::now();
        let mut expired_count = 0;

        for (file_id, cached) in cache.iter() {
            let should_remove = !Self::is_file_valid(cached.segment_ident, &cached, ia_mgr);
            if should_remove {
                cache.remove(&file_id);
                stats.expired.fetch_add(1, Ordering::Relaxed);
                expired_count += 1;
            }
        }

        let cleanup_duration = start_time.elapsed();
        if expired_count > 0 {
            info!(
                "vector index cache cleanup completed";
                "expired_count" => expired_count,
                "duration_ms" => cleanup_duration.as_millis(),
                "cache_len" => cache.len(),
                "cache_weight" => cache.weight()
            );
        }
    }

    fn is_file_valid(
        segment_ident: FileSegmentIdent,
        cached: &CachedIndexData,
        ia_mgr: &IaManager,
    ) -> bool {
        match ia_mgr.segment_cached_position(segment_ident) {
            FileSegmentPosition::InMem => true,
            FileSegmentPosition::InStore => {
                // If the segment is in store but cached is in mem, we can evict it.
                if cached.index_data.is_in_mem() {
                    return false;
                }
                true
            }
            FileSegmentPosition::NotExist => false,
        }
    }

    pub async fn stop_cleanup_task(&self) {
        if let Ok(mut guard) = self.cleanup_handle.write() {
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }
    }
}

/// Drop the cleanup task when the cache core is dropped.
impl Drop for VectorIndexCacheCore {
    fn drop(&mut self) {
        if let Ok(guard) = self.cleanup_handle.try_read() {
            if let Some(handle) = guard.as_ref() {
                handle.abort();
            }
        }
    }
}

pub struct VectorIndexCacheStats {
    pub expired: AtomicU64,
    pub cache_hit: AtomicU64,
    pub cache_miss: AtomicU64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct VectorIndexConfig {
    pub cache_size: usize,
    pub cache_cap: AbsoluteOrPercentSize,
    pub cache_cleanup_interval: u64,
}

impl Default for VectorIndexConfig {
    fn default() -> Self {
        Self {
            cache_size: 4096,
            cache_cap: AbsoluteOrPercentSize::Percent(10.0),
            cache_cleanup_interval: 10,
        }
    }
}
