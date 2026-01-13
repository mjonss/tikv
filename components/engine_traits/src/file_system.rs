// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{result::Result as StdResult, sync::Arc};

use bytes::Bytes;
use file_system::{IoOp, IoRateLimiter, get_io_rate_limiter, get_io_type};
use serde::{Deserialize, Serialize};

use crate::Result;

pub trait FileSystemInspector: Sync + Send {
    fn read(&self, len: usize) -> Result<usize>;
    fn write(&self, len: usize) -> Result<usize>;
}

pub struct EngineFileSystemInspector {
    limiter: Option<Arc<IoRateLimiter>>,
}

impl EngineFileSystemInspector {
    #[allow(dead_code)]
    pub fn new() -> Self {
        EngineFileSystemInspector {
            limiter: get_io_rate_limiter(),
        }
    }

    pub fn from_limiter(limiter: Option<Arc<IoRateLimiter>>) -> Self {
        EngineFileSystemInspector { limiter }
    }
}

impl Default for EngineFileSystemInspector {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSystemInspector for EngineFileSystemInspector {
    fn read(&self, len: usize) -> Result<usize> {
        if let Some(limiter) = &self.limiter {
            let io_type = get_io_type();
            Ok(limiter.request(io_type, IoOp::Read, len))
        } else {
            Ok(len)
        }
    }

    fn write(&self, len: usize) -> Result<usize> {
        if let Some(limiter) = &self.limiter {
            let io_type = get_io_type();
            Ok(limiter.request(io_type, IoOp::Write, len))
        } else {
            Ok(len)
        }
    }
}

/// (`start_off`, `end_off`):
/// * (None, None): read total object.
/// * (Some(s), Some(e)): read range [s, e).
/// * (Some(s), None): read from `s` to end of object.
/// * (None, Some(n)): read last `n` bytes.
#[derive(Debug, Default, Clone, Copy)]
pub struct GetObjectOptions {
    pub start_off: Option<u64>,
    pub end_off: Option<u64>,
}

impl GetObjectOptions {
    pub fn is_full_range(&self) -> bool {
        self.start_off.unwrap_or_default() == 0 && self.end_off.is_none()
    }

    pub fn range_string(&self) -> String {
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
        // For example:
        // The first 10 bytes: Range: bytes=0-9
        // The last 10 bytes: Range: bytes=-10
        if self.start_off.is_none() && self.end_off.is_some() {
            format!("-{}", self.end_off.unwrap())
        } else {
            format!(
                "{}-{}",
                self.start_off.map_or(String::new(), |s| format!("{}", s)),
                self.end_off.map_or(String::new(), |e| format!("{}", e - 1))
            )
        }
    }
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "PascalCase")]
pub struct ListObjectContent {
    pub key: String,
    pub last_modified: String,
    pub storage_class: String,
    pub size: u64, // in bytes.
}

pub trait ObjectStorage: Sync + Send {
    fn put_objects(&self, objects: Vec<(String, Bytes)>) -> StdResult<(), String>;

    /// Note: The order of returned objects may not be the same as `keys`.
    fn get_objects(
        &self,
        keys: Vec<(String, GetObjectOptions)>,
        cache: Option<&ObjectCacheWithHook>,
    ) -> StdResult<Vec<(String, Bytes)>, String>;

    fn list_objects(
        &self,
        start_after: &str, // The key to start after when listing objects (exclusive).
        prefix: Option<&str>,
        max_keys: Option<u32>,
    ) -> StdResult<(Vec<ListObjectContent>, Option<String>), String>;
}

/// The hook will be invoked when the object insert to object cache.
pub type CacheInsertHookFn = Arc<dyn Fn(Bytes) -> StdResult<Bytes, String> + Sync + Send>;

#[derive(Clone)]
pub enum CacheInsertHook {
    None,
    Hook(CacheInsertHookFn),
}

impl CacheInsertHook {
    pub fn invoke(&self, bytes: Bytes) -> StdResult<Bytes, String> {
        match self {
            Self::None => Ok(bytes),
            Self::Hook(f) => f(bytes),
        }
    }
}

#[derive(Clone)]
pub struct ObjectCacheWithHook {
    pub cache: ObjectCache,
    pub hook: CacheInsertHook,
}

impl ObjectCacheWithHook {
    #[must_use]
    pub fn with_hook(self, hook_fn: CacheInsertHookFn) -> Self {
        Self {
            cache: self.cache,
            hook: CacheInsertHook::Hook(hook_fn),
        }
    }

    pub fn has_hook(&self) -> bool {
        !matches!(self.hook, CacheInsertHook::None)
    }
}

impl From<ObjectCache> for ObjectCacheWithHook {
    fn from(cache: ObjectCache) -> Self {
        Self {
            cache,
            hook: CacheInsertHook::None,
        }
    }
}

#[derive(Clone)]
pub struct ObjectWeighter {}

impl quick_cache::Weighter<String, Bytes> for ObjectWeighter {
    fn weight(&self, key: &String, val: &Bytes) -> u64 {
        key.len() as u64 + val.len() as u64
    }
}

#[derive(Clone)]
pub struct ObjectCache {
    cache: Arc<
        quick_cache::sync::Cache<String, Bytes, ObjectWeighter, quick_cache::DefaultHashBuilder>,
    >,
}

impl std::ops::Deref for ObjectCache {
    type Target = quick_cache::sync::Cache<String, Bytes, ObjectWeighter>;

    fn deref(&self) -> &Self::Target {
        &self.cache
    }
}

impl ObjectCache {
    pub fn new(max_capacity: u64, avg_object_size: u64) -> Self {
        let opts = quick_cache::OptionsBuilder::new()
            .weight_capacity(max_capacity)
            .estimated_items_capacity(max_capacity as usize / avg_object_size as usize)
            .build()
            .unwrap();
        let cache = quick_cache::sync::Cache::with_options(
            opts,
            ObjectWeighter {},
            quick_cache::DefaultHashBuilder::default(),
            quick_cache::sync::DefaultLifecycle::default(),
        );
        Self {
            cache: Arc::new(cache),
        }
    }
}
