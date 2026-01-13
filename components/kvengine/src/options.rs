// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use dyn_clone::DynClone;

use self::config::{
    DEFAULT_HARD_REGION_MEM_USAGE_LIMIT_MB, DEFAULT_MAX_REGION_SPEED_LIMIT_MB_PER_SEC,
    DEFAULT_MIN_REGION_SPEED_LIMIT_MB_PER_SEC, DEFAULT_SOFT_REGION_MEM_USAGE_LIMIT_MB,
};
use crate::{
    config::{
        DEFAULT_BASE_SIZE, DEFAULT_COMPACTION_REQUEST_VERSION, DEFAULT_COMPACTION_TOMBS_COUNT,
        DEFAULT_COMPACTION_TOMBS_RATIO,
    },
    ia::util::IaConfig,
    table::{
        blobtable, columnar, sstable,
        tiny_meta::{MetaPackReader, MetaPackScheduler},
        vector_index::VectorIndexBuildOptions,
    },
    *,
};

// Options are params for creating Engine object.
//
// This package provides DefaultOptions which contains options that should
// work for most applications. Consider using that as a starting point before
// customizing it for your own needs.
pub struct Options {
    pub local_dirs: Vec<PathBuf>,

    /// Base_size is th maximum L1 size before trigger a compaction.
    /// The L2 size is 10x of the base size, L3 size is 100x of the base size.
    pub base_size: u64,

    pub max_block_cache_size: i64,

    /// Number of compaction workers to run concurrently.
    pub num_compactors: usize,

    pub table_builder_options: sstable::TableBuilderOptions,

    pub blob_table_build_options: blobtable::builder::BlobTableBuildOptions,

    pub columnar_build_options: columnar::ColumnarTableBuildOptions,

    pub vector_index_build_options: VectorIndexBuildOptions,

    pub remote_compactor_addr: String,

    pub recovery_concurrency: usize,

    pub preparation_concurrency: usize,

    /// Concurrency per request (e.g. prepare change set) for loading dfs files.
    pub dfs_load_concurrency_per_request: usize,

    pub max_mem_table_size: u64,

    pub allow_fallback_local: bool,

    pub blob_table_gc_ratio: f64,

    pub blob_prefetch_size: usize,

    pub max_del_range_delay: Duration,

    /// Indicate kvengine is used for restore or not.
    pub for_restore: bool,

    /// Start try to get gc safe point from gc v2 cache.
    pub enable_safe_point_v2: bool,

    /// Block keyspace gc safe point back to gc v1.
    pub disable_safe_point_fallback_v1: bool,

    pub compaction_request_version: u32,
    /// The ratio threshold of tombstone entries to trigger compaction.
    pub compaction_tombs_ratio: f64,
    /// The number threshold of tombstone entries to trigger compaction.
    pub compaction_tombs_count: u64,

    pub flow_control: FlowControlOptions,

    pub txn_file_worker_pool_size: usize,

    /// Enable IA by setting `ia.mem_cap > 0 && ia.disk_cap > 0`.
    pub ia: IaConfig,

    /// Ignore columnar table load and ingest when start kvengine. This is used
    /// for clear columnar replica in all shards when encounter critical issue.
    pub ignore_columnar_table_load: bool,
    /// Enable building columnar table.
    build_columnar: AtomicBool,
    /// Enable columnar table read.
    pub read_columnar: bool,
    /// Enable gc lock & extra cf.
    pub gc_lock_extra_cf: bool,

    /// Threshold of low available space. Reject some requests when available
    /// space is lower than this.
    pub low_space_threshold: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            local_dirs: vec![PathBuf::from("/tmp")],
            base_size: DEFAULT_BASE_SIZE,
            max_block_cache_size: 0,
            num_compactors: 3,
            table_builder_options: Default::default(),
            blob_table_build_options: Default::default(),
            columnar_build_options: Default::default(),
            vector_index_build_options: Default::default(),
            remote_compactor_addr: Default::default(),
            recovery_concurrency: Default::default(),
            preparation_concurrency: Default::default(),
            dfs_load_concurrency_per_request: 16,
            max_mem_table_size: 96 << 20,
            allow_fallback_local: true,
            blob_table_gc_ratio: 0.5,
            blob_prefetch_size: 256 * 1024,
            max_del_range_delay: Duration::from_secs(3600),
            for_restore: false,
            enable_safe_point_v2: false,
            disable_safe_point_fallback_v1: false,
            compaction_request_version: DEFAULT_COMPACTION_REQUEST_VERSION,
            compaction_tombs_ratio: DEFAULT_COMPACTION_TOMBS_RATIO,
            compaction_tombs_count: DEFAULT_COMPACTION_TOMBS_COUNT,
            flow_control: Default::default(),
            txn_file_worker_pool_size: 16,
            ia: Default::default(),
            ignore_columnar_table_load: false,
            build_columnar: AtomicBool::new(false),
            read_columnar: false,
            gc_lock_extra_cf: true,
            low_space_threshold: 0,
        }
    }
}

impl Options {
    pub fn build_columnar(&self) -> bool {
        self.build_columnar.load(Ordering::Relaxed)
    }

    pub fn set_build_columnar(&self, switch: bool) -> bool /* previous */ {
        self.build_columnar.swap(switch, Ordering::Relaxed)
    }
}

#[derive(Default, Clone, Copy)]
pub struct CfConfig {
    pub managed: bool,
    pub max_levels: usize,
}

impl CfConfig {
    pub fn new(managed: bool, max_levels: usize) -> Self {
        Self {
            managed,
            max_levels,
        }
    }
}

#[derive(Clone)]
pub struct FlowControlOptions {
    pub enable: bool,
    pub soft_region_mem_limit: u64,
    pub hard_region_mem_limit: u64,
    pub soft_region_l0table_size_limit: u64,
    pub hard_region_l0table_size_limit: u64,
    pub max_region_speed_limit: u64,
    pub min_region_speed_limit: u64,
}

impl Default for FlowControlOptions {
    fn default() -> Self {
        Self {
            enable: false,
            soft_region_mem_limit: DEFAULT_SOFT_REGION_MEM_USAGE_LIMIT_MB << 20,
            hard_region_mem_limit: DEFAULT_HARD_REGION_MEM_USAGE_LIMIT_MB << 20,
            soft_region_l0table_size_limit: DEFAULT_SOFT_REGION_MEM_USAGE_LIMIT_MB << 20,
            hard_region_l0table_size_limit: DEFAULT_HARD_REGION_MEM_USAGE_LIMIT_MB << 20,
            max_region_speed_limit: DEFAULT_MAX_REGION_SPEED_LIMIT_MB_PER_SEC << 20,
            min_region_speed_limit: DEFAULT_MIN_REGION_SPEED_LIMIT_MB_PER_SEC << 20,
        }
    }
}

impl FlowControlOptions {
    pub fn region_memtable_limiter_options(&self) -> limiter::LimiterOptions {
        limiter::LimiterOptions {
            enable: self.enable,
            soft_limit: self.soft_region_mem_limit,
            hard_limit: self.hard_region_mem_limit,
            max_speed_limit: self.max_region_speed_limit,
            min_speed_limit: self.min_region_speed_limit,
        }
    }

    pub fn region_l0table_limiter_options(&self) -> limiter::LimiterOptions {
        limiter::LimiterOptions {
            enable: self.enable,
            soft_limit: self.soft_region_l0table_size_limit,
            hard_limit: self.hard_region_l0table_size_limit,
            max_speed_limit: self.max_region_speed_limit,
            min_speed_limit: self.min_region_speed_limit,
        }
    }
}

#[async_trait]
pub trait IdAllocator: Sync + Send {
    // alloc_id returns the last id, and last_id - count is valid.
    fn alloc_id(&self, count: usize) -> Result<Vec<u64>>;

    async fn alloc_id_async(&self, count: usize) -> Result<Vec<u64>>;
}

pub trait RecoverHandler: Clone + Send {
    // Recovers from the shard's state to the state that is stored in the toState
    // property. So the Engine has a chance to execute pre-split command.
    // If toState is nil, the implementation should recovers to the latest state.
    fn recover(
        &self,
        engine: &Engine,
        shard: &Arc<Shard>,
        info: &ShardMeta,
        is_parent: bool,
    ) -> Result<()>;

    fn meta_pack_scheduler(&self) -> Option<&MetaPackScheduler> {
        None
    }

    fn meta_pack_reader(&self) -> Option<&MetaPackReader> {
        None
    }
}

pub trait MetaIterator {
    fn iterate<F>(&mut self, f: F) -> Result<()>
    where
        F: FnMut(kvenginepb::ChangeSet);

    fn take_files_in_blacklist(&mut self) -> Vec<u64> {
        vec![]
    }
    fn engine_id(&self) -> u64;
}

pub trait MetaChangeListener: DynClone + Sync + Send {
    fn on_change_set(&self, cs: kvenginepb::ChangeSet);
}

dyn_clone::clone_trait_object!(MetaChangeListener);
