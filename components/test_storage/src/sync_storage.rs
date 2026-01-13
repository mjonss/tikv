// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use api_version::{ApiV1, KvFormat};
use collections::HashMap;
use futures::executor::block_on;
use kvproto::{
    kvrpcpb::{Context, GetRequest, LockInfo},
    metapb,
};
use tikv::storage::{
    Engine, KvGetStatistics, PrewriteResult, Result, Storage, TestEngineBuilder, TxnStatus,
    config::Config, kv::RocksEngine, lock_manager::MockLockManager, test_util::GetConsumer,
    txn::commands,
};
use tikv_util::time::Instant;
use tracker::INVALID_TRACKER_TOKEN;
use txn_types::{Key, KvPair, Mutation, TimeStamp, Value};

/// A builder to build a `SyncTestStorage`.
///
/// Only used for test purpose.
pub struct SyncTestStorageBuilder<E: Engine, F: KvFormat> {
    _engine: E,
    config: Option<Config>,
    _phantom: PhantomData<F>,
}

/// SyncTestStorageBuilder for Api V1
/// To be convenience for test cases unrelated to RawKV.
pub type SyncTestStorageBuilderApiV1<E> = SyncTestStorageBuilder<E, ApiV1>;

impl<F: KvFormat> SyncTestStorageBuilder<RocksEngine, F> {
    pub fn new() -> Self {
        Self {
            _engine: TestEngineBuilder::new()
                .api_version(F::TAG)
                .build()
                .unwrap(),
            config: None,
            _phantom: PhantomData,
        }
    }
}

impl Default for SyncTestStorageBuilder<RocksEngine, ApiV1> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Engine, F: KvFormat> SyncTestStorageBuilder<E, F> {
    pub fn from_engine(engine: E) -> Self {
        Self {
            _engine: engine,
            config: None,
            _phantom: PhantomData,
        }
    }

    #[must_use]
    pub fn config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    pub fn build(self, _store_id: u64) -> Result<SyncTestStorage<E, F>> {
        unimplemented!()
    }
}

/// A `Storage` like structure with sync API.
///
/// Only used for test purpose.
#[derive(Clone)]
pub struct SyncTestStorage<E: Engine, F: KvFormat> {
    store: Storage<E, MockLockManager, F>,
}

/// SyncTestStorage for Api V1
/// To be convenience for test cases unrelated to RawKV.
pub type SyncTestStorageApiV1<E> = SyncTestStorage<E, ApiV1>;

impl<E: Engine, F: KvFormat> SyncTestStorage<E, F> {
    pub fn get_storage(&self) -> Storage<E, MockLockManager, F> {
        self.store.clone()
    }

    pub fn get_engine(&self) -> E {
        self.store.get_engine()
    }

    pub fn get(
        &self,
        ctx: Context,
        key: &Key,
        start_ts: impl Into<TimeStamp>,
    ) -> Result<(Option<Value>, KvGetStatistics)> {
        block_on(self.store.get(ctx, key.to_owned(), start_ts.into()))
    }

    #[allow(dead_code)]
    pub fn batch_get(
        &self,
        ctx: Context,
        keys: &[Key],
        start_ts: impl Into<TimeStamp>,
    ) -> Result<(Vec<Result<KvPair>>, KvGetStatistics)> {
        block_on(self.store.batch_get(ctx, keys.to_owned(), start_ts.into()))
    }

    #[allow(clippy::type_complexity)]
    pub fn batch_get_command(
        &self,
        ctx: Context,
        keys: &[&[u8]],
        start_ts: u64,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let mut ids = vec![];
        let requests: Vec<GetRequest> = keys
            .iter()
            .copied()
            .map(|key| {
                let mut req = GetRequest::default();
                req.set_context(ctx.clone());
                req.set_key(key.to_owned());
                req.set_version(start_ts);
                ids.push(ids.len() as u64);
                req
            })
            .collect();
        let trackers = keys.iter().map(|_| INVALID_TRACKER_TOKEN).collect();
        let p = GetConsumer::new();
        block_on(
            self.store
                .batch_get_command(requests, ids, trackers, p.clone(), Instant::now()),
        )?;
        let mut values = vec![];
        for value in p.take_data().into_iter() {
            values.push(value?);
        }
        Ok(values)
    }

    pub fn scan(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Option<Key>,
        limit: usize,
        key_only: bool,
        start_ts: impl Into<TimeStamp>,
    ) -> Result<Vec<Result<KvPair>>> {
        block_on(self.store.scan(
            ctx,
            start_key,
            end_key,
            limit,
            0,
            start_ts.into(),
            key_only,
            false,
        ))
    }

    pub fn reverse_scan(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Option<Key>,
        limit: usize,
        key_only: bool,
        start_ts: impl Into<TimeStamp>,
    ) -> Result<Vec<Result<KvPair>>> {
        block_on(self.store.scan(
            ctx,
            start_key,
            end_key,
            limit,
            0,
            start_ts.into(),
            key_only,
            true,
        ))
    }

    pub fn prewrite(
        &self,
        ctx: Context,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: impl Into<TimeStamp>,
    ) -> Result<PrewriteResult> {
        wait_op!(|cb| self.store.sched_txn_command(
            commands::Prewrite::with_context(mutations, primary, start_ts.into(), ctx),
            cb,
        ))
        .unwrap()
    }

    pub fn commit(
        &self,
        ctx: Context,
        keys: Vec<Key>,
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) -> Result<TxnStatus> {
        wait_op!(|cb| self.store.sched_txn_command(
            commands::Commit::new(keys, start_ts.into(), commit_ts.into(), false, false, ctx),
            cb,
        ))
        .unwrap()
    }

    pub fn cleanup(
        &self,
        ctx: Context,
        key: Key,
        start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        wait_op!(|cb| self.store.sched_txn_command(
            commands::Cleanup::new(key, start_ts.into(), current_ts.into(), ctx),
            cb,
        ))
        .unwrap()
    }

    pub fn rollback(
        &self,
        ctx: Context,
        keys: Vec<Key>,
        start_ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        wait_op!(|cb| self.store.sched_txn_command(
            commands::Rollback::new(keys, start_ts.into(), false, ctx),
            cb
        ))
        .unwrap()
    }

    pub fn scan_locks(
        &self,
        ctx: Context,
        max_ts: impl Into<TimeStamp>,
        start_key: Option<Key>,
        end_key: Option<Key>,
        limit: usize,
    ) -> Result<Vec<LockInfo>> {
        block_on(
            self.store
                .scan_lock(ctx, max_ts.into(), start_key, end_key, limit),
        )
    }

    pub fn resolve_lock(
        &self,
        ctx: Context,
        start_ts: impl Into<TimeStamp>,
        commit_ts: Option<impl Into<TimeStamp>>,
    ) -> Result<()> {
        let mut txn_status = HashMap::default();
        txn_status.insert(
            start_ts.into(),
            commit_ts.map(Into::into).unwrap_or_else(TimeStamp::zero),
        );
        wait_op!(|cb| self.store.sched_txn_command(
            commands::ResolveLockReadPhase::new(txn_status, None, ctx),
            cb,
        ))
        .unwrap()
    }

    pub fn resolve_lock_batch(
        &self,
        ctx: Context,
        txns: Vec<(TimeStamp, TimeStamp)>,
    ) -> Result<()> {
        let txn_status: HashMap<TimeStamp, TimeStamp> = txns.into_iter().collect();
        wait_op!(|cb| self.store.sched_txn_command(
            commands::ResolveLockReadPhase::new(txn_status, None, ctx),
            cb,
        ))
        .unwrap()
    }

    pub fn gc(
        &self,
        _region: metapb::Region,
        _: Context,
        _safe_point: impl Into<TimeStamp>,
    ) -> Result<()> {
        unimplemented!()
    }

    pub fn delete_range(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Key,
        notify_only: bool,
    ) -> Result<()> {
        wait_op!(|cb| self
            .store
            .delete_range(ctx, start_key, end_key, notify_only, cb))
        .unwrap()
    }
}
