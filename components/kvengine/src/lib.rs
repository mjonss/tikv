// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(pointer_is_aligned)]
#![feature(hash_extract_if)]
#![feature(is_sorted)]
#![feature(core_intrinsics)]
#![feature(extract_if)]
#![feature(assert_matches)]
#![feature(cell_update)]
#![feature(let_chains)]
#![feature(async_fn_track_caller)]
#![feature(debug_closure_helpers)]
#![feature(unboxed_closures)]
#![feature(fn_traits)]
#![allow(clippy::diverging_sub_expression)]
#![allow(internal_features)]
#![cfg_attr(test, feature(test))]
#[cfg(test)]
extern crate test;

pub mod apply;
pub mod codecutil;
pub mod compaction;
mod concat_iterator;
pub mod config;
pub use config::{
    Config as KvEngineConfig, PerKeyspaceConfig as KvEnginePerKeyspaceConfig,
    MEM_TABLE_MAX_SIZE as KV_ENGINE_MEM_TABLE_MAX_SIZE,
};
pub mod context;
pub mod dfs;
pub mod engine;
mod error;
pub mod flush;
pub mod ia;
pub mod limiter;
pub mod meta;
pub mod metrics;
pub mod mvcc;
pub mod options;
pub mod prepare;
pub mod read;
pub mod shard;
pub mod split;
pub mod stats;
pub mod table;
pub mod table_id;
pub mod table_stats;
pub mod txn_chunk_manager;
pub mod util;
pub mod write;

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog_global;

#[allow(unused_extern_crates)]
extern crate tikv_alloc;

#[cfg(test)]
mod tests;

#[cfg(feature = "debug-trace-mem-table")]
pub mod debug;
mod value_cache;

pub use apply::*;
pub use compaction::*;
use concat_iterator::ConcatIterator;
#[cfg(test)]
pub use dfs::Tagging;
pub use engine::*;
pub use error::*;
use flush::*;
pub use meta::*;
pub use mvcc::*;
pub use options::*;
pub use prepare::*;
pub use read::*;
pub use shard::*;
pub use split::*;
pub use stats::*;
pub use table::table::Iterator;
pub use table_stats::*;
pub use value_cache::*;
pub use write::*;

pub const NUM_CFS: usize = 3;
pub const CF_LEVELS: [usize; NUM_CFS] = [3, 2, 1];
const CF_MANAGED: [bool; NUM_CFS] = [true, false, true];

pub const COLUMNAR_LEVELS: usize = 3;

pub const WRITE_CF_BOTTOM_LEVEL: u32 = 3;

pub const LARGE_NUM_COLUMNAR_TABLES_IN_SHARD: usize = 256;
