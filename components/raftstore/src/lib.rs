// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg_attr(test, feature(test))]
#![feature(cell_update)]
#![feature(min_specialization)]
#![feature(box_patterns)]
#![feature(let_chains)]
#![feature(impl_trait_in_assoc_type)]
#![recursion_limit = "256"]

pub mod coprocessor;
pub mod errors;
pub mod router;
pub mod store;
pub use self::{
    coprocessor::{RegionInfo, RegionInfoAccessor, SeekRegionCallback},
    errors::{DiscardReason, Error, Result},
};

// `bytes::Bytes` is generated for `bytes` in protobuf.
pub fn bytes_capacity(b: &bytes::Bytes) -> usize {
    // NOTE: For deserialized raft messages, `len` equals capacity.
    // This is used to report memory usage to metrics.
    b.len()
}
