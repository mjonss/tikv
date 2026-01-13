// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

mod raft_extension;

// #[PerformanceCriticalPath]
use std::{
    fmt::{self, Debug, Display, Formatter},
    io::Error as IoError,
    result,
    sync::Arc,
    time::Duration,
};

use collections::HashMap;
use engine_rocks::RocksEngine;
use futures::{Future, Stream};
use futures_util::stream::empty;
use kvproto::{errorpb, kvrpcpb::Context, raft_cmdpb::Response};
pub use raft_extension::RaftRouterWrap;
use raftstore::{
    RegionInfoAccessor, errors::Error as RaftServerError, router::RaftStoreRouter,
    store::RegionSnapshot,
};
use thiserror::Error;
use tikv_kv::{Modify, OnAppliedCb, WriteEvent};
use txn_types::{TxnExtra, TxnExtraScheduler};

use crate::storage::{
    kv,
    kv::{Engine, Error as KvError, ErrorInner as KvErrorInner, SnapContext, WriteData},
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("{}", .0.get_message())]
    RequestFailed(errorpb::Error),

    #[error("{0}")]
    Io(#[from] IoError),

    #[error("{0}")]
    Server(#[from] RaftServerError),

    #[error("{0}")]
    InvalidResponse(String),

    #[error("{0}")]
    InvalidRequest(String),

    #[error("timeout after {0:?}")]
    Timeout(Duration),
}

pub type Result<T> = result::Result<T, Error>;

impl From<Error> for kv::Error {
    fn from(e: Error) -> kv::Error {
        match e {
            Error::RequestFailed(e) => KvError::from(KvErrorInner::Request(e)),
            Error::Server(e) => e.into(),
            e => box_err!(e),
        }
    }
}

pub enum CmdRes {
    Resp(Vec<Response>),
    Snap(RegionSnapshot),
}

/// `RaftKv` is a storage engine base on `RaftStore`.
#[derive(Clone)]
pub struct RaftKv<S>
where
    S: RaftStoreRouter + 'static,
{
    router: RaftRouterWrap<S>,
    engine: RocksEngine,
    txn_extra_scheduler: Option<Arc<dyn TxnExtraScheduler>>,
}

impl<S> RaftKv<S>
where
    S: RaftStoreRouter + 'static,
{
    /// Create a RaftKv using specified configuration.
    pub fn new(router: S, engine: RocksEngine, _: RegionInfoAccessor) -> RaftKv<S> {
        RaftKv {
            router: RaftRouterWrap::new(router),
            engine,
            txn_extra_scheduler: None,
        }
    }

    pub fn set_txn_extra_scheduler(&mut self, txn_extra_scheduler: Arc<dyn TxnExtraScheduler>) {
        self.txn_extra_scheduler = Some(txn_extra_scheduler);
    }
}

impl<S> Display for RaftKv<S>
where
    S: RaftStoreRouter + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<S> Debug for RaftKv<S>
where
    S: RaftStoreRouter + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<S> Engine for RaftKv<S>
where
    S: RaftStoreRouter + 'static,
{
    type Snap = RegionSnapshot;
    type Local = RocksEngine;

    fn kv_engine(&self) -> Option<Self::Local> {
        Some(self.engine.clone())
    }

    type RaftExtension = RaftRouterWrap<S>;
    #[inline]
    fn raft_extension(&self) -> &Self::RaftExtension {
        &self.router
    }

    fn modify_on_kv_engine(&self, _region_modifies: HashMap<u64, Vec<Modify>>) -> kv::Result<()> {
        unimplemented!()
    }

    fn precheck_write_with_ctx(&self, _: &Context) -> kv::Result<()> {
        unimplemented!()
    }

    type WriteRes = impl Stream<Item = WriteEvent> + Send + Unpin;
    fn async_write(
        &self,
        _ctx: &Context,
        _batch: WriteData,
        _subscribed: u8,
        _on_applied: Option<OnAppliedCb>,
    ) -> Self::WriteRes {
        empty()
    }

    type SnapshotRes = impl Future<Output = kv::Result<Self::Snap>> + Send;
    fn async_snapshot(&mut self, _ctx: SnapContext<'_>) -> Self::SnapshotRes {
        async move { unimplemented!() }
    }

    fn schedule_txn_extra(&self, txn_extra: TxnExtra) {
        if let Some(tx) = self.txn_extra_scheduler.as_ref() {
            if !txn_extra.is_empty() {
                tx.schedule(txn_extra);
            }
        }
    }
}
