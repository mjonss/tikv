// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(let_chains)]
#![cfg_attr(test, feature(test))]
// Bytes as map key
#![allow(clippy::mutable_key_type)]
#![feature(assert_matches)]

#[cfg(test)]
extern crate test;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;
#[macro_use]
extern crate serde_derive;

mod config;

pub use config::Config as RfEngineConfig;

pub mod compact_worker;
pub mod dfs_worker;
pub mod engine;
pub mod iterator;
pub mod load;
mod log_batch;
pub mod manifest;
mod metrics;
#[cfg(feature = "testexport")]
pub use metrics::{
    RFENGINE_DFS_WORKER_BECOME_UNHEALTHY_COUNTER, RFENGINE_DFS_WORKER_HEALTHY_GAUGE,
};
pub mod peers;
pub mod service_worker;
pub mod utils;
mod write_batch;
pub mod writer;

use std::num::ParseIntError;

pub use compact_worker::*;
pub use dfs_worker::*;
pub use engine::*;
use iterator::*;
pub use log_batch::RaftLogOp;
use metrics::*;
use thiserror::Error as ThisError;
use tikv_util::errors::IoError;
pub use utils::*;
pub use write_batch::WriteBatch;
pub use writer::*;

pub type Result<T> = std::result::Result<T, Error>;

pub const RFENGINE_DFS_WORKER_UNHEALTHY_ERR_MSG: &str = "DFS worker unhealthy";

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] IoError),
    #[error("EOF")]
    Eof,
    #[error("parse error")]
    ParseError,
    #[error("Open error: {0}")]
    Open(String),
    #[error("Corruption: {msg}, epoch_id {epoch_id}, offset {offset}")]
    Corruption {
        msg: String,
        epoch_id: u32,
        offset: u64,
        data: Vec<u8>,
    },
    #[error("WAL Epoch {epoch_id} is overwritten")]
    WalEpochOverwritten { epoch_id: u32 },
    #[error("Snapshot is oversize: {0}")]
    SnapshotOversize(u64),
    #[error("Memory limit exceed, request {request}, available {available}")]
    MemoryLimitExceed { request: usize, available: i64 },
    // Should contain `RFENGINE_DFS_WORKER_UNHEALTHY_ERR_MSG`.
    #[error("DFS worker unhealthy, store_id {store_id}, epoch_id {epoch_id}")]
    DfsWorkerUnhealthy { store_id: u64, epoch_id: u32 },
    #[error("Backup error: {0}")]
    Backup(String),
    #[error("Async writer disabled")]
    AsyncWriterDisabled,
    #[error("max batch size exceeded")]
    MaxBatchSizeExceeded,
    #[error("compact worker force stopped")]
    CompactWorkerForceStop,
    #[error("Other error: {0}")]
    Other(String),
    #[error("dfs error: {0}")]
    Dfs(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Error::Eof;
        }
        Error::Io(IoError::new(e, "".to_string()))
    }
}

impl From<ParseIntError> for Error {
    fn from(_: ParseIntError) -> Self {
        Error::ParseError
    }
}

impl From<String> for Error {
    fn from(msg: String) -> Self {
        Error::Other(msg)
    }
}
