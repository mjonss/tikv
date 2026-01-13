// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Implementation of engine_traits for RocksDB
//!
//! This is a work-in-progress attempt to abstract all the features needed by
//! TiKV to persist its data.
//!
//! The module structure here mirrors that in engine_traits where possible.
//!
//! Because there are so many similarly named types across the TiKV codebase,
//! and so much "import renaming", this crate consistently explicitly names type
//! that implement a trait as `RocksTraitname`, to avoid the need for import
//! renaming and make it obvious what type any particular module is working
//! with.
//!
//! Please read the engine_trait crate docs before hacking.

#![cfg_attr(test, feature(test))]

#[allow(unused_extern_crates)]
extern crate tikv_alloc;

mod cf_names;

mod cf_options;
pub use crate::cf_options::*;

mod db_options;
pub use crate::db_options::*;
mod db_vector;
pub use crate::db_vector::*;
mod engine;
pub use crate::engine::*;
mod import;
pub use crate::import::*;
mod logger;
pub use crate::logger::*;
mod misc;
mod snapshot;
pub use crate::snapshot::*;
mod sst;
pub use crate::sst::*;
mod status;
pub use crate::status::*;
mod write_batch;
pub use crate::write_batch::*;
mod perf_context_metrics;
pub use perf_context_metrics::APPLY_PERF_CONTEXT_TIME_HISTOGRAM_STATIC;

mod engine_iterator;
pub use crate::engine_iterator::*;

pub mod options;
pub mod util;

pub mod rocks_metrics;
pub use rocks_metrics::*;

pub mod rocks_metrics_defs;
pub use rocks_metrics_defs::*;

pub mod config;
pub use config::*;

pub mod encryption;

pub mod file_system;

pub use rocksdb::{
    PerfContext, PerfFlag, PerfFlags, PerfLevel, Statistics as RocksStatistics, set_perf_flags,
    set_perf_level,
};

pub mod flow_control_factors;

pub mod raw;

pub fn get_env(
    key_manager: Option<std::sync::Arc<::encryption::DataKeyManager>>,
    limiter: Option<std::sync::Arc<::file_system::IoRateLimiter>>,
) -> engine_traits::Result<std::sync::Arc<raw::Env>> {
    let env = encryption::get_env(None /* base_env */, key_manager)?;
    file_system::get_env(Some(env), limiter)
}
