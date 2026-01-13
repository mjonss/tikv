// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(box_patterns)]
#![feature(assert_matches)]

mod channel;
mod delegate;
mod errors;
pub mod metrics;
mod service;

pub use channel::{CdcEvent, Drain, MemoryQuota, Sink, channel, recv_timeout};
pub use errors::{Error, Result};
pub use service::{Conn, ConnId};
