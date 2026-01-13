// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tikv_util::sys::SysQuota;

use crate::table::{Result, columnar::TableMeta};

const TABLE_META_CACHE_SHARDS_PER_CORE: usize = 16;
const ESTIMATED_TABLE_META_SIZE: u64 = 1024; // 1KB

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ColumnarMetaCacheKey {
    pub file_id: u64,
    pub table_id: i64,
}

#[derive(Clone)]
pub struct ColumnarMetaWeighter;

impl quick_cache::Weighter<ColumnarMetaCacheKey, Arc<TableMeta>> for ColumnarMetaWeighter {
    fn weight(&self, _: &ColumnarMetaCacheKey, val: &Arc<TableMeta>) -> u64 {
        8 + 8 + val.size
    }
}

#[derive(Clone)]
pub struct ColumnarMetaCache {
    cache: Arc<
        quick_cache::sync::Cache<
            ColumnarMetaCacheKey,
            Arc<TableMeta>,
            ColumnarMetaWeighter,
            quick_cache::DefaultHashBuilder,
        >,
    >,
}

impl Default for ColumnarMetaCache {
    fn default() -> Self {
        let mem_limit = SysQuota::memory_limit_in_bytes();
        Self::new(mem_limit / 500)
    }
}

impl ColumnarMetaCache {
    pub fn new(max_capacity: u64) -> Self {
        let columnar_meta_cache_shards =
            (SysQuota::cpu_cores_quota() as usize).max(1) * TABLE_META_CACHE_SHARDS_PER_CORE;
        let opts = quick_cache::OptionsBuilder::new()
            .shards(columnar_meta_cache_shards)
            .weight_capacity(max_capacity)
            .estimated_items_capacity(max_capacity as usize / ESTIMATED_TABLE_META_SIZE as usize)
            .build()
            .unwrap();
        let cache = quick_cache::sync::Cache::with_options(
            opts,
            ColumnarMetaWeighter,
            quick_cache::DefaultHashBuilder::default(),
            quick_cache::sync::DefaultLifecycle::default(),
        );
        Self {
            cache: Arc::new(cache),
        }
    }

    pub(crate) fn try_get_with(
        &self,
        file_id: u64,
        table_id: i64,
        init: impl FnOnce() -> Result<Arc<TableMeta>>,
    ) -> Result<Arc<TableMeta>> {
        let key = ColumnarMetaCacheKey { file_id, table_id };
        let init = || init();
        self.cache.get_or_insert_with(&key, init)
    }
}
