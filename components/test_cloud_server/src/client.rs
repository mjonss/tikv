// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Cow,
    cmp,
    collections::{BTreeMap, HashMap, HashSet},
    ops::{
        Bound::{Excluded, Included, Unbounded},
        Deref, DerefMut, Range,
    },
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread::{self, sleep},
    time::Duration,
};

use api_version::{
    ApiV2, KeyMode, KvFormat,
    api_v2::{self, TXN_KEY_PREFIX},
};
use bstr::ByteSlice;
use bytes::Bytes;
use futures::executor::block_on;
use grpcio::Channel;
use kvengine::ShardTag;
use kvproto::{
    coprocessor as coppb, errorpb, kvrpcpb,
    kvrpcpb::{
        CommitRequest, Context, GetRequest, IsolationLevel, Op, PrewriteRequest, SplitRegionRequest,
    },
    metapb,
    metapb::Peer,
    tikvpb::TikvClient,
};
use log_wrappers::Value;
use pd_client::{
    PdClient, check_regions_boundary,
    util::{RegionLike, compare_region_end_key},
};
use protobuf::ProtobufEnum;
use rfstore::store::RegionIdVer;
use test_pd_client;
use tikv::storage::mvcc::TimeStamp;
use tikv_client::{
    CheckLevel, IntoOwnedRange, TransactionOptions, proto::kvrpcpb::Mutation as KvMutation,
};
use tikv_util::{
    box_err,
    codec::bytes::{decode_bytes, encode_bytes},
    debug, error, info,
    time::Instant,
    warn,
};

use crate::{
    must_wait_result, try_wait, try_wait_result,
    txn::{
        lock_resolver::{LockResolver, ResolveLocksOptions},
        txn_file::{TxnFileChunk, TxnFileHelper},
    },
    util::{Mutation, RawRegion},
};

const MAX_WAIT_LOCK_DURATION: Duration = Duration::from_millis(500);
const SCAN_REGIONS_MAX_BATCH_SIZE: usize = 1024;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Other error {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error("PD error {0}")]
    Pd(#[from] pd_client::Error),
    #[error("PD control error {0}")]
    PdCtl(#[from] pd_client::pd_control::Error),
    #[error("Write conflict {0:?}")]
    WriteConflict(kvrpcpb::WriteConflict),
    #[error("Already exist {0:?}")]
    AlreadyExist(kvrpcpb::AlreadyExist),
    #[error("Transaction not found {0:?}")]
    TxnNotFound(kvrpcpb::TxnNotFound),
    #[error(transparent)]
    Grpc(#[from] grpcio::Error),
    #[error("TiKV Client error {0}")]
    TikvClient(#[from] tikv_client::Error),
    #[error("Region error {0:?}")]
    RegionError(errorpb::Error),
    #[error("Key error {0:?}")]
    KeyError(kvrpcpb::KeyError),
    #[error("Key errors {0:?}")]
    KeyErrors(Vec<kvrpcpb::KeyError>),
    #[error("Build RPC context failure region:{0:?}")]
    RpcContext(u64),
    #[error("Security client error {0}")]
    SecurityClientError(#[from] security::HttpClientError),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Default, Clone, Debug)]
pub struct RefStore(HashMap<Vec<u8>, Option<Vec<u8>>>); // `None` means the key has been deleted.

impl RefStore {
    pub fn put_kv(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.0.insert(key, Some(value));
    }

    pub fn del_kv(&mut self, key: Vec<u8>) {
        self.0.insert(key, None);
    }

    pub fn ingest(&mut self, other: RefStore) {
        for (k, v) in other.0 {
            self.0.insert(k, v);
        }
    }

    pub fn destroy_range(&mut self, start: &[u8], end: &[u8]) {
        for (k, v) in self.0.iter_mut() {
            if k.as_slice() >= start && k.as_slice() < end {
                *v = None;
            }
        }
    }

    pub fn rewrite_keyspace_prefix(&mut self, target_keyspace_id: u32) {
        let old_ref_store = std::mem::take(&mut self.0);
        let target_prefix = ApiV2::get_txn_keyspace_prefix(target_keyspace_id);
        let inner = old_ref_store
            .into_iter()
            .map(|(mut k, v)| {
                assert_eq!(ApiV2::parse_key_mode(&k), api_version::KeyMode::Txn);
                k[0..api_v2::KEYSPACE_PREFIX_LEN].copy_from_slice(&target_prefix);
                (k, v)
            })
            .collect::<HashMap<_, _>>();
        self.0 = inner;
    }
}

impl Deref for RefStore {
    type Target = HashMap<Vec<u8>, Option<Vec<u8>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RefStore {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Clone)]
pub struct ClusterClientOptions {
    pub with_lock_resolver: bool,
    pub txn_file_max_chunk_size: Option<usize>,
    pub req_timeout: Duration,
}

impl Default for ClusterClientOptions {
    fn default() -> Self {
        Self {
            with_lock_resolver: true,
            txn_file_max_chunk_size: None,
            req_timeout: Duration::from_secs(30),
        }
    }
}

pub struct ClusterClient {
    pub(crate) opts: ClusterClientOptions,
    pub(crate) pd_client: Arc<dyn test_pd_client::PdClientExt>,
    pub(crate) channels: HashMap<u64, Channel>,
    /// region_raw_end_key -> region_id
    pub(crate) region_ranges: BTreeMap<Vec<u8>, RegionIdVer>,
    /// region_id -> region
    pub(crate) regions: HashMap<u64, RawRegion>,
    pub(crate) ref_store: Arc<Mutex<RefStore>>,
    pub(crate) max_ts: AtomicU64,
    pub(crate) async_commit: bool,

    // `lock_resolver` embed a `ClusterClient` to reuse methods to communicate with tikv-server.
    // So we need the `Option<Box>` to resolve circular dependency.
    // TODO: separate methods of RPCs (kv_xxx) from ClusterClient.
    pub(crate) lock_resolver: Option<Box<LockResolver>>,

    pub(crate) txn_file_helper: Option<Arc<TxnFileHelper>>,
}

// Named as `RequestPeerRole` to avoid conflict with `metapb::PeerRole`.
pub enum RequestPeerRole {
    Leader,
    Learner,
}

pub struct RequestOptions {
    pub peer_role: RequestPeerRole,
    pub resolve_lock: bool,
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self {
            peer_role: RequestPeerRole::Leader,
            resolve_lock: true,
        }
    }
}

impl RequestOptions {
    pub fn learner() -> Self {
        Self {
            peer_role: RequestPeerRole::Learner,
            ..Default::default()
        }
    }

    pub fn no_resolve_lock() -> Self {
        Self {
            resolve_lock: false,
            ..Default::default()
        }
    }

    pub fn replica_read(&self) -> bool {
        !matches!(self.peer_role, RequestPeerRole::Leader)
    }
}

#[derive(Clone)]
pub enum CommitAction {
    /// Sync commit all keys.
    SyncCommit,
    /// Sync commit the primary key, then async commit the secondary keys.
    /// When `delay == Duration::MAX`, the secondary keys will not be committed.
    AsyncCommitSecondaryKeys(Duration /* delay */),
    /// No key is committed. The transaction should be rolled back.
    NoCommit,
    /// Async commit all keys.
    AsyncCommit(Duration /* delay */),
}

// Note: the index keys should be global unique to make ref store deterministic.
pub type GenIndexFn = Box<dyn Fn(u64 /* start_ts */, &[Mutation]) -> Vec<(Bytes, Bytes)>>;

pub struct MutateOptions {
    pub start_ts: Option<TimeStamp>,
    pub commit_action: CommitAction,
    pub write_method: TxnWriteMethod,
    pub gen_index: Option<GenIndexFn>,
    pub no_verify: bool,
}

impl Default for MutateOptions {
    fn default() -> Self {
        Self {
            start_ts: None,
            commit_action: CommitAction::SyncCommit,
            write_method: TxnWriteMethod::Normal,
            gen_index: None,
            no_verify: false,
        }
    }
}

impl MutateOptions {
    pub fn start_ts(start_ts: TimeStamp) -> Self {
        Self {
            start_ts: Some(start_ts),
            ..Default::default()
        }
    }
}

impl Clone for ClusterClient {
    fn clone(&self) -> Self {
        // Do not copy region cache to reduce memory usage.
        let make_clone = || -> Self {
            Self {
                opts: self.opts.clone(),
                pd_client: self.pd_client.clone(),
                channels: self.channels.clone(),
                region_ranges: Default::default(),
                regions: Default::default(),
                ref_store: self.ref_store.clone(),
                max_ts: Default::default(),
                async_commit: self.async_commit,
                lock_resolver: None,
                txn_file_helper: self.txn_file_helper.clone(),
            }
        };
        let mut cloned = make_clone();
        let lock_resolver = if self.lock_resolver.is_some() {
            Some(LockResolver::new(make_clone()))
        } else {
            None
        };
        cloned.lock_resolver = lock_resolver.map(|x| Box::new(x));
        cloned
    }
}

#[derive(Copy, Clone)]
pub struct PrewriteExt {
    pub resolve_lock: bool,
    pub lock_ttl: Duration,
    pub for_update_ts: TimeStamp,
}

impl Default for PrewriteExt {
    fn default() -> Self {
        Self {
            resolve_lock: true,
            lock_ttl: Duration::from_secs(3),
            for_update_ts: TimeStamp::zero(),
        }
    }
}

impl PrewriteExt {
    pub fn no_resolve_lock() -> Self {
        Self {
            resolve_lock: false,
            ..Default::default()
        }
    }

    pub fn with_for_update_ts(for_update_ts: TimeStamp) -> Self {
        Self {
            for_update_ts,
            ..Default::default()
        }
    }
}

impl ClusterClient {
    pub fn pd_client(&self) -> &Arc<dyn test_pd_client::PdClientExt> {
        &self.pd_client
    }

    pub fn get_ts(&self) -> TimeStamp {
        must_wait_result(
            || {
                block_on(self.pd_client.get_tso()).map_err(|e| {
                    warn!("get_ts failed: {:?}", e);
                    e
                })
            },
            10,
            || "get_ts failed".to_string(),
        )
    }

    pub fn del_table_rows_commit(&mut self, start_ts: TimeStamp, mutations: &[Mutation]) {
        let commit_ts = self.get_ts();
        self.kv_commit(
            TxnMutations::from_normal(mutations.to_vec()),
            start_ts,
            commit_ts,
            false,
        )
        .expect("kv_commit");
        self.del_kv_in_ref_store(mutations.to_vec());
        self.set_max_ts(commit_ts.into_inner());
    }

    pub fn del_kv<F>(&mut self, rng: Range<usize>, gen_key: F)
    where
        F: Fn(usize) -> Vec<u8>,
    {
        self.try_del_kv(rng, gen_key, MutateOptions::default())
            .unwrap();
    }

    pub fn try_del_kv<F>(
        &mut self,
        rng: Range<usize>,
        gen_key: F,
        options: MutateOptions,
    ) -> Result<TimeStamp /* commit_ts */>
    where
        F: Fn(usize) -> Vec<u8>,
    {
        let mut mutations = vec![];
        for i in rng {
            let mut m = Mutation::default();
            m.set_op(Op::Del);
            m.set_key(gen_key(i));
            mutations.push(m)
        }
        let txn_muts = block_on(TxnMutations::build(
            mutations.clone(),
            options.write_method,
            self.txn_file_helper.clone(),
        ))?;
        let mut txn = self.begin_transaction(options.start_ts);
        let secondaries = txn.async_commit.then(|| txn_muts.secondaries());
        self.kv_prewrite_ext(
            txn_muts.primary(),
            secondaries.as_ref(),
            txn_muts.clone(),
            &mut txn,
            PrewriteExt::default(),
        )?;

        let commit_ts = match options.commit_action {
            CommitAction::NoCommit => {
                return Ok(0.into());
            }
            commit_action => {
                self.kv_commit_ext(txn_muts, txn.start_ts, txn.get_commit_ts(), commit_action)?
            }
        };

        self.del_kv_in_ref_store(mutations);
        self.set_max_ts(commit_ts.into_inner());
        Ok(commit_ts)
    }

    fn del_kv_in_ref_store(&mut self, mutations: Vec<Mutation>) {
        let mut ref_store = self.ref_store.lock().unwrap();
        for mut m in mutations {
            ref_store.del_kv(m.take_key());
        }
    }

    pub fn put_commit(&mut self, start_ts: TimeStamp, mutations: &[Mutation]) -> Result<TimeStamp> {
        let put_time = Instant::now();
        assert!(!self.async_commit);
        let req_commit_ts = self.get_ts();
        let first = mutations.first().unwrap().clone();
        let txn_muts = TxnMutations::from_normal(mutations.to_vec());
        let commit_ts = self
            .kv_commit(txn_muts, start_ts, req_commit_ts, false)
            .expect("kv_commit");
        self.verify_key_value(
            first.get_key(),
            Some(first.get_value()),
            commit_ts.into_inner(),
            put_time,
            &RequestOptions::default(),
        )?;
        self.put_kv_in_ref_store(mutations.to_vec());
        self.set_max_ts(commit_ts.into_inner());
        Ok(commit_ts)
    }

    // Return commit_ts.
    pub fn put_kv<R, F, G>(&mut self, rng: R, gen_key: F, gen_val: G) -> TimeStamp
    where
        R: IntoIterator<Item = usize>,
        F: Fn(usize) -> Vec<u8>,
        G: Fn(usize) -> Vec<u8>,
    {
        self.try_put_kv(rng, gen_key, gen_val, MutateOptions::default())
            .unwrap()
    }

    pub fn try_put_kv<R, F, G>(
        &mut self,
        rng: R,
        gen_key: F,
        gen_val: G,
        mut options: MutateOptions,
    ) -> Result<TimeStamp /* commit_ts */>
    where
        R: IntoIterator<Item = usize>,
        F: Fn(usize) -> Vec<u8>,
        G: Fn(usize) -> Vec<u8>,
    {
        let start_ts = *options.start_ts.get_or_insert_with(|| self.get_ts());

        let mut mutations = vec![];
        for i in rng {
            let mut m = Mutation::default();
            m.set_op(Op::Put);
            m.set_key(gen_key(i));
            m.set_value(gen_val(i));
            mutations.push(m)
        }
        if let Some(gen_index) = options.gen_index.as_ref() {
            let index_kv = gen_index(start_ts.into_inner(), &mutations);
            for (key, value) in index_kv {
                mutations.push(Mutation {
                    op: Op::Put,
                    key,
                    value,
                    ..Default::default()
                });
            }

            mutations.sort_by(|a, b| a.key.cmp(&b.key));
        }

        let first = mutations.first().unwrap().clone();
        let no_commit = matches!(options.commit_action, CommitAction::NoCommit);
        let no_verify = options.no_verify;
        let put_time = Instant::now();
        let commit_ts = self.prewrite_and_commit(mutations, options, PrewriteExt::default())?;

        if !no_commit && !no_verify {
            self.verify_key_value(
                first.get_key(),
                Some(first.get_value()),
                commit_ts.into_inner(),
                put_time,
                &RequestOptions::default(),
            )?;
        }
        Ok(commit_ts)
    }

    pub fn prewrite_and_commit(
        &mut self,
        mutations: Vec<Mutation>,
        options: MutateOptions,
        ext: PrewriteExt,
    ) -> Result<TimeStamp /* commit_ts */> {
        let start_ts = options.start_ts.unwrap_or_else(|| self.get_ts());

        let txn_muts = block_on(TxnMutations::build(
            mutations.clone(),
            options.write_method,
            self.txn_file_helper.clone(),
        ))?;
        let mut txn = self.begin_transaction(Some(start_ts));
        let secondaries = txn.async_commit.then(|| txn_muts.secondaries());

        fail::fail_point!("client::before_kv_prewrite");

        self.kv_prewrite_ext(
            txn_muts.primary(),
            secondaries.as_ref(),
            txn_muts.clone(),
            &mut txn,
            ext,
        )?;

        fail::fail_point!("client::before_kv_commit");

        let commit_ts = match options.commit_action {
            CommitAction::NoCommit => {
                return Ok(0.into());
            }
            commit_action => {
                self.kv_commit_ext(txn_muts, txn.start_ts, txn.get_commit_ts(), commit_action)?
            }
        };

        self.put_kv_in_ref_store(mutations);
        self.set_max_ts(commit_ts.into_inner());
        Ok(commit_ts)
    }

    pub fn put_kv_in_ref_store(&mut self, mutations: Vec<Mutation>) {
        let mut ref_store = self.ref_store.lock().unwrap();
        for mut m in mutations {
            ref_store.put_kv(m.take_key(), m.take_value());
        }
    }

    fn set_max_ts(&mut self, max_ts: u64) {
        self.max_ts.fetch_max(max_ts, Ordering::Relaxed);
    }

    pub fn max_ts(&mut self) -> u64 {
        self.max_ts.load(Ordering::Relaxed)
    }

    pub fn kv_mutate(&mut self, muts: Vec<Mutation>) -> Result<TimeStamp /* commit_ts */> {
        assert!(!muts.is_empty());
        let txn_muts = TxnMutations::from_normal(muts);
        let mut txn = self.begin_transaction(None);
        let secondaries = txn.async_commit.then(|| txn_muts.secondaries());
        self.kv_prewrite_ext(
            txn_muts.primary(),
            secondaries.as_ref(),
            txn_muts.clone(),
            &mut txn,
            PrewriteExt::default(),
        )?;

        let commit_ts = self.kv_commit(
            txn_muts,
            txn.start_ts,
            txn.get_commit_ts(),
            txn.async_commit,
        )?;
        self.set_max_ts(commit_ts.into_inner());
        Ok(commit_ts)
    }

    pub fn begin_transaction(&self, start_ts: Option<TimeStamp>) -> Transaction {
        let start_ts = start_ts.unwrap_or_else(|| self.get_ts());
        Transaction {
            start_ts,
            min_commit_ts: 0.into(),
            async_commit: self.async_commit,
            pd_client: self.pd_client.clone(),
        }
    }

    pub fn kv_prewrite_ext(
        &mut self,
        pk: Bytes,
        secondaries: Option<&Vec<Bytes>>,
        muts: TxnMutations,
        txn: &mut Transaction,
        ext: PrewriteExt,
    ) -> Result<()> {
        // Don't use muts.primary() for pk & muts.secondaries() for secondary keys, as
        // `kv_prewrite` will recursively be invoked in `kv_prewrite_single_region`.
        let groups = muts.group_by_regions(self, PrimaryFilter::All).unwrap();
        for (id_ver, group_muts) in groups {
            self.kv_prewrite_single_region(id_ver, pk.clone(), secondaries, group_muts, txn, ext)?;
        }
        Ok(())
    }

    pub fn kv_prewrite(
        &mut self,
        pk: Bytes,
        secondaries: Option<&Vec<Bytes>>,
        muts: TxnMutations,
        ts: TimeStamp,
    ) -> Result<TimeStamp /* min_commit_ts */> {
        let mut txn = self.begin_transaction(Some(ts));
        self.kv_prewrite_ext(pk, secondaries, muts, &mut txn, PrewriteExt::default())?;
        Ok(txn.min_commit_ts)
    }

    pub fn kv_prewrite_single_region(
        &mut self,
        id_ver: RegionIdVer,
        pk: Bytes,
        secondary_keys: Option<&Vec<Bytes>>,
        muts: TxnMutations,
        txn: &mut Transaction,
        ext: PrewriteExt,
    ) -> Result<()> {
        let region_id = id_ver.id();
        let mut errors: Vec<(ShardTag, Error)> = vec![];
        let start_time = Instant::now();
        while start_time.saturating_elapsed() < self.opts.req_timeout {
            let ctx = self
                .new_rpc_ctx(region_id, &pk)
                .filter(|x| x.get_region_epoch().get_version() == id_ver.ver());
            if ctx.is_none() {
                return self.kv_prewrite_ext(pk, secondary_keys, muts, txn, ext);
            }
            let ctx = ctx.unwrap();
            let tag = Self::tag_from_ctx(&ctx);
            let store_id = ctx.get_peer().get_store_id();
            let kv_client = self.get_kv_client(store_id);
            let mut prewrite_req = PrewriteRequest::default();
            prewrite_req.set_context(ctx);
            muts.set_prewrite_req(&mut prewrite_req);
            prewrite_req.primary_lock = pk.to_vec();
            prewrite_req.start_version = txn.start_ts.into_inner();
            prewrite_req.lock_ttl = ext.lock_ttl.as_millis() as u64;
            prewrite_req.min_commit_ts = prewrite_req.start_version + 1;
            prewrite_req.use_async_commit = txn.async_commit;
            prewrite_req.assertion_level = kvrpcpb::AssertionLevel::Strict;
            if ext.for_update_ts.into_inner() > 0 {
                prewrite_req.set_for_update_ts(ext.for_update_ts.into_inner());
                prewrite_req.set_pessimistic_actions(vec![
                    kvrpcpb::PrewriteRequestPessimisticAction::DoPessimisticCheck;
                    prewrite_req.mutations.len()
                ]);
            }
            if let Some(secondary_keys) = secondary_keys {
                if muts.primary() == pk {
                    prewrite_req.set_secondaries(
                        secondary_keys
                            .iter()
                            .map(|k| k.to_vec())
                            .collect::<Vec<_>>()
                            .into(),
                    );
                }
            }
            debug!("{} prewrite {:?}", tag, prewrite_req);
            let result = kv_client.kv_prewrite(&prewrite_req);
            if let Err(err) = result {
                errors.push((tag, err.into()));
                sleep(Duration::from_millis(100));
                self.update_cache_by_id(region_id, None);
                continue;
            }
            let mut resp = result.unwrap();
            if resp.has_region_error() {
                let region_err = resp.take_region_error();
                if self.handle_retryable_error(&tag, &region_err) {
                    errors.push((tag, Error::RegionError(region_err)));
                    continue;
                }
                if self.handle_region_epoch_not_match_or_not_found(&region_err) {
                    return self.kv_prewrite_ext(pk, secondary_keys, muts, txn, ext);
                }
                error!("{} unexpected error {:?}", tag, region_err);
                return Err(Error::RegionError(region_err));
            }
            let key_errors = resp.take_errors();
            if !key_errors.is_empty() {
                info!("{} prewrite: encounters key_errors: {:?}", tag, key_errors);
                if !ext.resolve_lock {
                    return Err(Error::KeyErrors(key_errors.into_vec()));
                }

                let key_errors = key_errors.into_vec();
                errors.push((tag, Error::KeyErrors(key_errors.clone())));
                self.handle_key_errors(&tag, prewrite_req.start_version, false, key_errors)?;
                continue;
            }

            if txn.async_commit {
                if resp.min_commit_ts == 0 {
                    info!("{} fallback to 2pc", tag);
                    txn.async_commit = false;
                    txn.min_commit_ts = 0.into();
                } else {
                    let prev_min_commit_ts = txn.min_commit_ts;
                    txn.min_commit_ts = txn.min_commit_ts.max(resp.min_commit_ts.into());
                    debug!("{} async commit resp", tag; "resp" => ?resp, "start_ts" => txn.start_ts,
                        "min_commit_ts" => txn.min_commit_ts, "prev_min_commit_ts" => prev_min_commit_ts);
                }
            }
            return Ok(());
        }
        error!("{} prewrite failed {:?}", region_id, errors);
        Err(errors.pop().unwrap().1)
    }

    pub fn kv_prewrite_single_region_without_retry(
        &mut self,
        pk: Bytes,
        secondary_keys: Option<&Vec<Bytes>>,
        muts: TxnMutations,
        ts: TimeStamp,
    ) -> Result<kvrpcpb::PrewriteResponse> {
        let groups = muts.group_by_regions(self, PrimaryFilter::All)?;
        if groups.is_empty() {
            return Err(Error::Other(box_err!("No region found for prewrite")));
        }
        let region_id = groups[0].0;
        let ctx = self
            .new_rpc_ctx(region_id.id(), &pk)
            .filter(|x| x.get_region_epoch().get_version() == region_id.ver());
        if ctx.is_none() {
            return Err(Error::RpcContext(region_id.id()));
        }
        let ctx = ctx.unwrap();
        let store_id = ctx.get_peer().get_store_id();
        let kv_client = self.get_kv_client(store_id);
        let mut prewrite_req = PrewriteRequest::default();
        prewrite_req.set_context(ctx);
        muts.set_prewrite_req(&mut prewrite_req);
        prewrite_req.primary_lock = pk.to_vec();
        prewrite_req.start_version = ts.into_inner();
        prewrite_req.lock_ttl = 3000;
        prewrite_req.min_commit_ts = prewrite_req.start_version + 1;
        prewrite_req.use_async_commit = self.async_commit;
        if let Some(secondary_keys) = secondary_keys {
            if muts.primary() == pk {
                prewrite_req.set_secondaries(
                    secondary_keys
                        .iter()
                        .map(|k| k.to_vec())
                        .collect::<Vec<_>>()
                        .into(),
                );
            }
        }
        let prewrite_resp = kv_client.kv_prewrite(&prewrite_req)?;
        Ok(prewrite_resp)
    }

    // Return the actual commit_ts, which would be larger than commit_ts in request.
    pub fn kv_commit(
        &mut self,
        muts: TxnMutations,
        start_ts: TimeStamp,
        mut commit_ts: TimeStamp,
        use_async_commit: bool,
    ) -> Result<TimeStamp> {
        // fail_point!("kv_commmit");
        // println!("{:?}", fail::list());

        let groups = muts.group_by_regions(self, PrimaryFilter::All).unwrap();
        for (id_ver, group_muts) in groups {
            commit_ts = self
                .kv_commit_single_region(id_ver, group_muts, start_ts, commit_ts, use_async_commit)?
                .max(commit_ts);
        }
        Ok(commit_ts)
    }

    /// Primary key is the first item of `keys`.
    pub fn kv_commit_ext(
        &mut self,
        muts: TxnMutations,
        start_ts: TimeStamp,
        mut commit_ts: TimeStamp,
        commit_action: CommitAction,
    ) -> Result<TimeStamp> {
        match commit_action {
            CommitAction::SyncCommit => Ok(self.kv_commit(muts, start_ts, commit_ts, false)?),
            CommitAction::AsyncCommitSecondaryKeys(delay) => {
                let mut primary_groups = muts
                    .group_by_regions(self, PrimaryFilter::PrimaryOnly)
                    .unwrap();
                assert_eq!(primary_groups.len(), 1);
                let (id_ver, group_muts) = primary_groups.swap_remove(0);
                commit_ts =
                    self.kv_commit_single_region(id_ver, group_muts, start_ts, commit_ts, false)?;

                if delay < Duration::MAX {
                    let mut client = self.clone();
                    thread::spawn(move || {
                        thread::sleep(delay);
                        let secondary_groups = muts
                            .group_by_regions(&mut client, PrimaryFilter::Secondaries)
                            .unwrap();
                        for (id_ver, group_muts) in secondary_groups {
                            if let Err(err) = client.kv_commit_single_region(
                                id_ver, group_muts, start_ts, commit_ts, false,
                            ) {
                                warn!("secondaries commit failed: {:?}", err; "start_ts" => ?start_ts);
                            }
                        }
                    });
                }
                Ok(commit_ts)
            }
            CommitAction::NoCommit => Ok(0.into()),
            CommitAction::AsyncCommit(delay) => {
                assert!(self.async_commit);
                let mut client = self.clone();
                thread::spawn(move || {
                    thread::sleep(delay);
                    if let Err(err) = client.kv_commit(muts, start_ts, commit_ts, true) {
                        warn!("async commit failed: {:?}", err; "start_ts" => ?start_ts);
                    }
                });
                Ok(commit_ts)
            }
        }
    }

    pub fn kv_commit_single_region(
        &mut self,
        id_ver: RegionIdVer,
        muts: TxnMutations,
        start_ts: TimeStamp,
        mut commit_ts: TimeStamp,
        use_async_commit: bool,
    ) -> Result<TimeStamp> {
        let region_id = id_ver.id();
        let mut errors: Vec<(ShardTag, Error)> = vec![];
        let start_time = Instant::now();
        while start_time.saturating_elapsed() < self.opts.req_timeout {
            let ctx = self
                .new_rpc_ctx(region_id, &muts.primary())
                .filter(|x| x.get_region_epoch().get_version() == id_ver.ver());
            if ctx.is_none() {
                return self.kv_commit(muts, start_ts, commit_ts, use_async_commit);
            }
            let ctx = ctx.unwrap();
            let tag = Self::tag_from_ctx(&ctx);
            let store_id = ctx.get_peer().get_store_id();
            let kv_client = self.get_kv_client(store_id);
            let mut commit_req = CommitRequest::default();
            commit_req.set_context(ctx);
            commit_req.start_version = start_ts.into_inner();
            muts.set_commit_req(&mut commit_req);
            commit_req.commit_version = commit_ts.into_inner();
            commit_req.use_async_commit = use_async_commit;
            let result = kv_client.kv_commit(&commit_req);
            if let Err(err) = result {
                errors.push((tag, err.into()));
                sleep(Duration::from_millis(100));
                self.update_cache_by_id(region_id, None);
                continue;
            }
            let mut commit_resp = result.unwrap();
            if commit_resp.has_region_error() {
                let region_err = commit_resp.take_region_error();
                if self.handle_retryable_error(&tag, &region_err) {
                    errors.push((tag, Error::RegionError(region_err)));
                    continue;
                }
                if self.handle_region_epoch_not_match_or_not_found(&region_err) {
                    return self.kv_commit(muts, start_ts, commit_ts, use_async_commit);
                }
                error!("{} unexpected error {:?}", tag, region_err);
                return Err(Error::RegionError(region_err));
            }
            if commit_resp.has_error() {
                let key_err = commit_resp.take_error();
                if key_err.has_commit_ts_expired() {
                    errors.push((tag, Error::KeyError(key_err)));
                    commit_ts = self.get_ts();
                    info!("{} commit_ts expired, retry {:?}", tag, commit_ts);
                    continue;
                }
                error!("{} commit failed with key error {:?}", tag, key_err);
                return Err(Error::KeyError(key_err));
            }
            commit_ts = TimeStamp::from(commit_resp.commit_version);
            return Ok(commit_ts);
        }
        error!("{} commit failed {:?}", region_id, errors);
        Err(errors.pop().unwrap().1)
    }

    pub fn kv_rollback(&mut self, muts: TxnMutations, start_ts: TimeStamp) -> Result<()> {
        let groups = muts.group_by_regions(self, PrimaryFilter::All).unwrap();
        for (id_ver, group_muts) in groups {
            self.kv_rollback_single_region(id_ver, group_muts, start_ts)?;
        }
        Ok(())
    }

    pub fn kv_rollback_single_region(
        &mut self,
        id_ver: RegionIdVer,
        muts: TxnMutations,
        start_ts: TimeStamp,
    ) -> Result<()> {
        let region_id = id_ver.id();
        let mut errors: Vec<(ShardTag, Error)> = vec![];
        let start_time = Instant::now();
        while start_time.saturating_elapsed() < self.opts.req_timeout {
            let ctx = self
                .new_rpc_ctx(region_id, &muts.primary())
                .filter(|x| x.get_region_epoch().get_version() == id_ver.ver());
            if ctx.is_none() {
                return self.kv_rollback(muts, start_ts);
            }
            let ctx = ctx.unwrap();
            let tag = Self::tag_from_ctx(&ctx);
            let store_id = ctx.get_peer().get_store_id();
            let kv_client = self.get_kv_client(store_id);
            let mut rollback_req = kvrpcpb::BatchRollbackRequest::default();
            rollback_req.set_context(ctx);
            rollback_req.start_version = start_ts.into_inner();
            muts.set_rollback_req(&mut rollback_req);
            let result = kv_client.kv_batch_rollback(&rollback_req);
            if let Err(err) = result {
                errors.push((tag, err.into()));
                sleep(Duration::from_millis(100));
                self.update_cache_by_id(region_id, None);
                continue;
            }
            let mut rollback_resp = result.unwrap();
            if rollback_resp.has_region_error() {
                let region_err = rollback_resp.take_region_error();
                if self.handle_retryable_error(&tag, &region_err) {
                    errors.push((tag, Error::RegionError(region_err)));
                    continue;
                }
                if self.handle_region_epoch_not_match_or_not_found(&region_err) {
                    return self.kv_rollback(muts, start_ts);
                }
                error!("{} unexpected error {:?}", tag, region_err);
                return Err(Error::RegionError(region_err));
            }
            if rollback_resp.has_error() {
                let key_err = rollback_resp.take_error();
                error!("{} rollback failed with key error {:?}", tag, key_err);
                return Err(Error::KeyError(key_err));
            }
            return Ok(());
        }
        error!("{} rollback failed {:?}", region_id, errors);
        Err(errors.pop().unwrap().1)
    }

    // TODO: eliminate duplicated codes for handling network & region errors.
    pub fn kv_check_txn_status(
        &mut self,
        primary_key: &[u8],
        lock_ts: TimeStamp,
        caller_start_ts: TimeStamp,
        current_ts: TimeStamp,
        rollback_if_not_exist: bool,
        force_sync_commit: bool,
        resolving_pessimistic_lock: bool,
        is_txn_file: bool,
    ) -> Result<kvrpcpb::CheckTxnStatusResponse> {
        let stat_time = Instant::now();
        let mut last_err: Option<Error> = None;
        let mut tag = ShardTag::default();
        while stat_time.saturating_elapsed() < self.opts.req_timeout {
            let region_id = self.get_region_id(primary_key);
            let ctx = self.new_rpc_ctx(region_id, primary_key).unwrap();
            tag = Self::tag_from_ctx(&ctx);
            let client = self.get_kv_client(ctx.get_peer().get_store_id());
            let mut req = kvrpcpb::CheckTxnStatusRequest::default();
            req.set_context(ctx);
            req.set_primary_key(primary_key.to_vec());
            req.set_lock_ts(lock_ts.into_inner());
            req.set_caller_start_ts(caller_start_ts.into_inner());
            req.set_current_ts(current_ts.into_inner());
            req.set_rollback_if_not_exist(rollback_if_not_exist);
            req.set_force_sync_commit(force_sync_commit);
            req.set_resolving_pessimistic_lock(resolving_pessimistic_lock);
            req.set_is_txn_file(is_txn_file);
            req.set_verify_is_primary(true);
            let result = client.kv_check_txn_status(&req);
            if result.is_err() {
                last_err = Some(box_err!(
                    "{} kv_check_txn_status error: {:?}",
                    tag,
                    result.unwrap_err()
                ));
                warn!("{:?}", last_err);
                sleep(Duration::from_millis(100));
                self.update_cache_by_id(region_id, None);
                continue;
            }

            let resp = result.unwrap();
            if resp.has_region_error() {
                let region_err = resp.get_region_error();
                last_err = Some(box_err!(
                    "{} kv_check_txn_status: region_err {:?}",
                    tag,
                    region_err
                ));
                warn!("{:?}", last_err);
                if self.handle_retryable_error(&tag, region_err) {
                    continue;
                }
                if self.handle_region_epoch_not_match_or_not_found(region_err) {
                    continue;
                }
                panic!("{:?}", last_err);
            }

            return Ok(resp);
        }
        panic!("{} kv_check_txn_status failed {:?}", tag, last_err.unwrap());
    }

    // TODO: eliminate duplicated codes for handling network & region errors.
    // TODO: support lite: resolve single lock when number of keys is small.
    pub fn kv_resolve_lock(
        &mut self,
        start_version: u64,
        commit_version: Option<u64>,
        key: Vec<u8>,
        is_txn_file: bool,
        clean_regions: &mut HashSet<RegionIdVer>,
    ) -> Result<()> {
        let stat_time = Instant::now();
        let mut last_err: Option<Error> = None;
        let mut tag = ShardTag::default();

        let mut req = kvrpcpb::ResolveLockRequest::default();
        req.set_start_version(start_version);
        if let Some(commit_version) = commit_version {
            req.set_commit_version(commit_version);
        }
        req.set_keys(vec![key.clone()].into());
        req.set_is_txn_file(is_txn_file);

        while stat_time.saturating_elapsed() < self.opts.req_timeout {
            let region = self.get_region_by_key(&key);
            if clean_regions.contains(&region.id_ver()) {
                return Ok(());
            }

            let ctx = self.new_rpc_ctx(region.id, &key).unwrap();
            tag = Self::tag_from_ctx(&ctx);
            let client = self.get_kv_client(ctx.get_peer().get_store_id());
            req.set_context(ctx);
            debug!("{} kv_resolve_lock", tag; "req" => ?req);
            let result = client.kv_resolve_lock(&req);
            if result.is_err() {
                last_err = Some(box_err!(
                    "{} kv_resolve_lock error: {:?}",
                    tag,
                    result.unwrap_err()
                ));
                warn!("{:?}", last_err);
                sleep(Duration::from_millis(100));
                self.update_cache_by_id(region.id, None);
                continue;
            }

            let mut resp = result.unwrap();
            if resp.has_region_error() {
                let region_err = resp.take_region_error();
                last_err = Some(box_err!(
                    "{} kv_resolve_lock: region_err {:?}",
                    tag,
                    region_err
                ));
                warn!("{:?}", last_err);
                if self.handle_retryable_error(&tag, &region_err) {
                    continue;
                }
                if self.handle_region_epoch_not_match_or_not_found(&region_err) {
                    continue;
                }
                error!(
                    "{} kv_resolve_lock failed, start_version {}, error {:?}",
                    tag, start_version, region_err
                );
                return Err(Error::RegionError(region_err));
            }

            if resp.has_error() {
                let key_err = resp.take_error();
                error!(
                    "{} kv_resolve_lock failed, start_version {}, error {:?}",
                    tag, start_version, key_err
                );
                return Err(Error::KeyError(key_err));
            }

            clean_regions.insert(region.id_ver());
            return Ok(());
        }
        let last_err = last_err.unwrap();
        error!("{} kv_resolve_lock failed {:?}", tag, last_err);
        Err(last_err)
    }

    pub fn kv_resolve_lock_for_mutations(
        &mut self,
        start_version: u64,
        commit_version: Option<u64>,
        muts: Vec<Mutation>,
        is_txn_file: bool,
    ) -> Result<()> {
        let mut clean_regions = HashSet::new();
        for mut m in muts {
            self.kv_resolve_lock(
                start_version,
                commit_version,
                m.take_key(),
                is_txn_file,
                &mut clean_regions,
            )?;
        }
        Ok(())
    }

    fn group_mutations_by_region(
        &mut self,
        mut mutations: Vec<Mutation>,
    ) -> HashMap<RegionIdVer, Vec<Mutation>> {
        let mut groups: HashMap<RegionIdVer, Vec<Mutation>> = HashMap::new();
        for m in mutations.drain(..) {
            let region = self.get_region_by_key(m.get_key());
            groups.entry(region.id_ver()).or_default().push(m);
        }
        groups
    }

    pub fn get_region_id(&mut self, key: &[u8]) -> u64 {
        let region = self.get_region_by_key(key);
        region.id
    }

    pub fn get_peer_id(&mut self, key: &[u8], store_id: u64) -> u64 {
        let region = self.get_region_by_key(key);
        region
            .peers
            .iter()
            .find(|x| x.store_id == store_id)
            .unwrap()
            .id
    }

    pub fn get_region_by_key(&mut self, key: &[u8]) -> RawRegion {
        if let Some(region) = self.get_region_from_cache(key) {
            return region;
        }
        let region: RawRegion = self
            .pd_client
            .get_region(&encode_bytes(key))
            .unwrap()
            .into();
        self.update_cache_by_id(region.id, Some(region.clone()));
        region
    }

    // Note: `scan_regions` do not use region cache.
    pub fn scan_regions(&mut self, raw_start: &[u8], raw_end: &[u8]) -> Result<Vec<RawRegion>> {
        if !raw_end.is_empty() && raw_start >= raw_end {
            return Ok(vec![]);
        }

        let mut raw_regions: Vec<RawRegion> = vec![];
        let mut encoded_start = encode_bytes(raw_start);
        let encoded_end = encode_bytes(raw_end);
        loop {
            let batch = try_wait_result(
                || -> Result<Vec<kvproto::pdpb::Region>> {
                    let batch = block_on(self.pd_client.scan_regions(
                        encoded_start.clone(),
                        encoded_end.clone(),
                        SCAN_REGIONS_MAX_BATCH_SIZE,
                    ))?;
                    debug!(
                        "scan_regions: {:?}, start {}, end {}",
                        batch,
                        &Value::key(&encoded_start),
                        &Value::key(&encoded_end)
                    );
                    let last = batch
                        .last()
                        .ok_or_else(|| Error::Other(box_err!("empty batch")))?;
                    check_regions_boundary(&encoded_start, last.end_key(), true, &batch)?;
                    Ok(batch)
                },
                10,
            )?;

            // Handle overlapping with previous batch.
            if !raw_regions.is_empty() {
                let raw_first_key = decode_bytes(&mut batch.first().unwrap().start_key(), false)
                    .map_err(|err| (err, batch.first()))
                    .expect("decode_bytes");
                if raw_regions.last().unwrap().end_key() != raw_first_key {
                    let idx = raw_regions
                        .iter()
                        .rev()
                        .position(|r| r.start_key() <= raw_first_key.as_slice())
                        .unwrap();
                    encoded_start = encode_bytes(raw_regions[idx].start_key());
                    raw_regions.truncate(idx);
                    continue;
                }
            }

            encoded_start = batch.last().unwrap().end_key().to_vec();
            for mut region in batch {
                let mut raw_region: RawRegion = region.take_region().into();
                raw_region.update_leader(region.get_leader());
                self.update_cache_by_id(raw_region.id, Some(raw_region.clone()));
                raw_regions.push(raw_region);
            }
            if compare_region_end_key(&encoded_start, &encoded_end).is_ge() {
                break;
            }
        }

        check_regions_boundary(raw_start, raw_end, false, &raw_regions).unwrap_or_else(|err| {
            panic!(
                "check_regions_boundary, err {:?}, regions {:?}",
                err, raw_regions
            )
        });
        Ok(raw_regions)
    }

    pub fn clear_region_cache(&mut self) {
        self.region_ranges.clear();
        self.regions.clear();
    }

    fn invalidate_region_cache(&mut self, region_id: u64) {
        if let Some(region) = self.regions.remove(&region_id) {
            self.region_ranges.remove(&region.raw_end);
        }
    }

    fn update_cache_by_id(&mut self, region_id: u64, opt_region: Option<RawRegion>) {
        let region: RawRegion = if let Some(region) = opt_region {
            region
        } else if let Some((region, leader)) =
            block_on(self.pd_client.get_region_leader_by_id(region_id)).unwrap()
        {
            let mut region = RawRegion::from(region);
            region.update_leader(&leader);
            region
        } else {
            self.invalidate_region_cache(region_id);
            return;
        };

        if let Some(old_region) = self.regions.get(&region_id) {
            if old_region.equal(&region) {
                return;
            }
        }

        for (raw_end, id) in
            self.get_regions_in_range_from_cache(&region.raw_start, &region.raw_end)
        {
            self.region_ranges.remove(&raw_end);
            self.regions.remove(&id);
        }
        self.region_ranges
            .insert(region.raw_end.clone(), region.id_ver());
        self.regions.insert(region.id, region);
    }

    fn get_region_from_cache(&self, key: &[u8]) -> Option<RawRegion> {
        if let Some((_, &id_ver)) = self
            .region_ranges
            .range((Excluded(key.to_vec()), Unbounded))
            .next()
        {
            if let Some(region) = self.regions.get(&id_ver.id()) {
                if region.id_ver() == id_ver && region.raw_start.as_slice() <= key {
                    return Some(region.clone());
                }
            }
        }
        None
    }

    fn get_regions_in_range_from_cache(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, u64)> {
        self.region_ranges
            .range((Excluded(start.to_vec()), Included(end.to_vec())))
            .map(|(raw_end, &id_ver)| (raw_end.clone(), id_ver.id()))
            .collect()
    }

    fn handle_retryable_error(&mut self, tag: &ShardTag, region_err: &errorpb::Error) -> bool {
        let region_id = tag.id_ver.id;
        if region_err.has_not_leader() {
            let region = self.regions.get_mut(&region_id).unwrap();
            if region_err.get_not_leader().has_leader() {
                let leader = region_err.get_not_leader().get_leader();
                if region.update_leader(leader) {
                    sleep(Duration::from_millis(100));
                    return true;
                }
            }
            sleep(Duration::from_millis(100));
            self.update_cache_by_id(region_id, None);
            return true;
        }
        if region_err.has_epoch_not_match() {
            // Handle the scene that PD has newer region version than current TiKV leader by
            // invalidate cache & retry later.
            if let Some(region) = region_err
                .get_epoch_not_match()
                .get_current_regions()
                .iter()
                .find(|r| r.id == region_id)
            {
                if region.get_region_epoch().get_version() < tag.id_ver.ver {
                    warn!("{} tikv has older region version", tag; "current_region" => ?region, "region_err" => ?region_err);
                    sleep(Duration::from_millis(100));
                    self.update_cache_by_id(region_id, None);
                    return true;
                }
            }
            return false;
        }
        if region_err.has_proposal_in_merging_mode() {
            sleep(Duration::from_millis(100));
            return true;
        }
        if region_err.has_stale_command() {
            sleep(Duration::from_millis(100));
            self.update_cache_by_id(region_id, None);
            return true;
        }
        if region_err.has_server_is_busy() {
            sleep(Duration::from_millis(100));
            return true;
        }
        if region_err.has_read_index_not_ready() {
            sleep(Duration::from_millis(100));
            return true;
        }
        if region_err.get_message().contains("mismatch peer") {
            sleep(Duration::from_millis(100));
            self.update_cache_by_id(region_id, None);
            return true;
        }
        if region_err.get_message().contains("proposal dropped") {
            sleep(Duration::from_millis(100));
            return true;
        }
        if region_err
            .get_message()
            .contains("peer has not applied to current term")
        {
            // Occurs when split region.
            // See `rfstore::peer::Peer::propose_normal`.
            sleep(Duration::from_millis(100));
            return true;
        }
        false
    }

    fn handle_region_epoch_not_match_or_not_found(&mut self, region_err: &errorpb::Error) -> bool {
        if region_err.has_epoch_not_match() {
            let not_match = region_err.get_epoch_not_match();
            for region in not_match.get_current_regions() {
                self.update_cache_by_id(region.id, Some(region.clone().into()));
            }
            return true;
        }
        if region_err.has_region_not_found() {
            let region_id = region_err.get_region_not_found().get_region_id();
            self.invalidate_region_cache(region_id);
            return true;
        }
        false
    }

    fn lock_resolver(&mut self) -> &mut LockResolver {
        self.lock_resolver.as_mut().unwrap()
    }

    // Note: only support optimistic transactions by now.
    // TODO: support pessimistic transactions.
    fn handle_key_errors(
        &mut self,
        tag: &ShardTag,
        start_ts: u64,
        is_pessimistic: bool,
        key_errors: Vec<kvrpcpb::KeyError>,
    ) -> Result<()> {
        debug_assert!(!is_pessimistic);

        let mut locks: Vec<kvrpcpb::LockInfo> = Vec::with_capacity(key_errors.len());
        for key_err in key_errors {
            let lock = self.handle_single_key_error(tag, start_ts, is_pessimistic, key_err)?;
            locks.push(lock);
        }
        let opts = ResolveLocksOptions {
            caller_start_ts: start_ts,
            locks,
        };
        let res = self.lock_resolver().resolve_locks(opts)?;
        if res.until_expire_ms() > 0 {
            info!(
                "{} txn {} resolve_locks: sleep {}ms for locks",
                tag,
                start_ts,
                res.until_expire_ms()
            );
            // Sleep no more than MAX_WAIT_LOCK_DURATION to speed up tests.
            sleep(Duration::from_millis(res.until_expire_ms()).min(MAX_WAIT_LOCK_DURATION));
        } else {
            info!(
                "{} txn {} resolve_locks: all locks are resolved",
                tag, start_ts
            );
        }
        Ok(())
    }

    fn handle_single_key_error(
        &self,
        tag: &ShardTag,
        start_ts: u64,
        is_pessimistic: bool,
        mut key_err: kvrpcpb::KeyError,
    ) -> Result<kvrpcpb::LockInfo> {
        if key_err.has_locked() {
            let lock = key_err.take_locked();
            debug!(
                "{} encounters lock, start_ts {}, lock {:?}",
                tag, start_ts, lock
            );

            // If an optimistic transaction encounters a lock with larger ts, this
            // transaction will certainly fail due to a WriteConflict error.
            // So we can construct and return an error here early.
            // Pessimistic transactions don't need such an optimization. If this key needs a
            // pessimistic lock, TiKV will return a PessimisticLockNotFound
            // error directly if it encounters a different lock. Otherwise,
            // TiKV returns lock.lock_ttl = 0, and we still need to resolve the lock.
            if lock.lock_version > start_ts && !is_pessimistic {
                let write_conflict = kvrpcpb::WriteConflict {
                    start_ts,
                    conflict_ts: lock.lock_version,
                    key: lock.key,
                    conflict_commit_ts: 0,
                    reason: kvrpcpb::WriteConflictReason::Optimistic,
                    ..Default::default()
                };
                return Err(Error::WriteConflict(write_conflict));
            }

            return Ok(lock);
        }

        if key_err.has_conflict() {
            Err(Error::WriteConflict(key_err.take_conflict()))
        } else if key_err.has_already_exist() {
            Err(Error::AlreadyExist(key_err.take_already_exist()))
        } else {
            Err(Error::KeyError(key_err))
        }
    }

    pub fn get_kv_client(&self, store_id: u64) -> TikvClient {
        TikvClient::new(self.get_client_channel(store_id))
    }

    pub fn get_client_channel(&self, store_id: u64) -> Channel {
        self.channels.get(&store_id).unwrap().clone()
    }

    pub fn get_stores(&self) -> Vec<u64> {
        self.channels.keys().copied().collect()
    }

    pub fn new_rpc_ctx(&mut self, region_id: u64, key: &[u8]) -> Option<Context> {
        self.new_rpc_ctx_opt(region_id, key, &RequestOptions::default())
    }

    fn tag_from_ctx(ctx: &Context) -> kvengine::ShardTag {
        kvengine::ShardTag {
            engine_id: ctx.get_peer().get_store_id(),
            id_ver: kvengine::IdVer::new(ctx.get_region_id(), ctx.get_region_epoch().get_version()),
        }
    }

    fn get_peer_for_request(
        &self,
        region_id: u64,
        options: &RequestOptions,
    ) -> Option<(&RawRegion, &Peer)> {
        self.regions
            .get(&region_id)
            .and_then(|region| match options.peer_role {
                RequestPeerRole::Leader => Some((region, region.get_leader())),
                RequestPeerRole::Learner => {
                    let learner = region
                        .peers
                        .iter()
                        .find(|peer| peer.role == metapb::PeerRole::Learner);
                    if learner.is_none() {
                        warn!("{} has no learner, region: {:?}", region_id, region);
                    }
                    learner.map(|learner| (region, learner))
                }
            })
    }

    pub fn new_rpc_ctx_opt(
        &mut self,
        region_id: u64,
        key: &[u8],
        options: &RequestOptions,
    ) -> Option<Context> {
        if self.get_peer_for_request(region_id, options).is_none() {
            let ok = try_wait(
                || {
                    self.update_cache_by_id(region_id, None);
                    self.get_peer_for_request(region_id, options).is_some()
                },
                3,
            );
            if !ok {
                return None;
            }
        }
        let (region, peer) = self.get_peer_for_request(region_id, options).unwrap();
        let mut ctx = Context::new();
        ctx.set_api_version(api_version_of_key(key));
        ctx.set_region_id(region_id);
        ctx.set_region_epoch(region.epoch.clone());
        ctx.set_peer(peer.clone());
        ctx.set_replica_read(options.replica_read());
        Some(ctx)
    }

    pub fn split(&mut self, key: &[u8]) {
        self.try_split(key, 1).expect("ClusterClient::split");
    }

    pub fn try_split(&mut self, key: &[u8], timeout_secs: usize) -> Result<()> {
        let start = Instant::now_coarse();
        let timeout = Duration::from_secs(timeout_secs as u64);
        while start.saturating_elapsed() < timeout {
            let region_id = self.get_region_id(key);
            let ctx = self.new_rpc_ctx(region_id, key).unwrap();
            let tag = Self::tag_from_ctx(&ctx);
            let client = self.get_kv_client(ctx.get_peer().get_store_id());
            let mut split_req = SplitRegionRequest::default();
            split_req.set_context(ctx);
            split_req.set_split_key(key.to_vec());
            let mut resp = client.split_region(&split_req)?;
            if !resp.get_errors().is_empty() {
                info!(
                    "{} try_split: encounters key errors: {:?}",
                    tag,
                    resp.get_errors()
                );
                return Err(Error::KeyErrors(resp.take_errors().into()));
            }
            if resp.has_region_error() {
                let region_err = resp.get_region_error();
                if self.handle_retryable_error(&tag, region_err) {
                    sleep(Duration::from_millis(100));
                    continue;
                }
                if self.handle_region_epoch_not_match_or_not_found(region_err) {
                    sleep(Duration::from_millis(100));
                    continue;
                }
                return Err(box_err!(
                    "failed to split key {:?} error {:?}",
                    key,
                    region_err
                ));
            }
            for region in resp.take_regions().into_iter() {
                self.update_cache_by_id(region.get_id(), Some(region.into()));
            }
            return Ok(());
        }
        Err(box_err!("failed to split key {:?}", key))
    }

    pub fn try_split_by_pd(&self, key: &[u8], timeout_secs: usize) -> Result<()> {
        let encoded_key = encode_bytes(key);
        let region = self.pd_client.get_region(&encoded_key)?;
        if region.start_key == encoded_key || region.end_key == encoded_key {
            return Ok(());
        }
        Ok(self.pd_client.must_split_region_opt(
            region,
            kvproto::pdpb::CheckPolicy::Usekey,
            vec![encoded_key],
            Duration::from_secs(timeout_secs as u64),
        )?)
    }

    pub fn split_keyspace(&mut self, keyspace_id: u32) {
        let mut keys = vec![];
        let (start, end) = ApiV2::get_keyspace_range_by_id(keyspace_id);
        if !start.is_empty() {
            keys.push(encode_bytes(&start));
        }
        keys.push(encode_bytes(&end));
        block_on(
            self.pd_client
                .split_regions_with_retry(keys, Duration::from_secs(60)),
        )
        .unwrap();
    }

    pub fn split_keyspaces(&mut self, keyspace_ids: impl IntoIterator<Item = u32>) {
        let mut keys = vec![];
        for keyspace_id in keyspace_ids {
            let (start, end) = ApiV2::get_keyspace_range_by_id(keyspace_id);
            if !start.is_empty() {
                keys.push(encode_bytes(&start));
            }
            keys.push(encode_bytes(&end));
        }
        keys.sort();
        keys.dedup();
        let timeout_secs = cmp::max(keys.len() * 3, 60);
        block_on(
            self.pd_client
                .split_regions_with_retry(keys, Duration::from_secs(timeout_secs as u64)),
        )
        .unwrap();
    }

    pub fn merge(&mut self, source_key: &[u8], target_key: &[u8]) {
        let source_region = self
            .pd_client
            .get_region(&encode_bytes(source_key))
            .unwrap();
        let target_region = self
            .pd_client
            .get_region(&encode_bytes(target_key))
            .unwrap();
        assert_ne!(source_region.id, target_region.id);
        self.pd_client
            .merge_region(source_region.id, target_region.id);
    }

    /// Try to merge.
    /// Return true: merge request sent.
    /// Return false: `source_key` & `target_key` had been merged.
    pub fn try_merge(&mut self, source_key: &[u8], target_key: &[u8]) -> bool /* sent */ {
        let source_region = self
            .pd_client
            .get_region(&encode_bytes(source_key))
            .unwrap();
        let target_region = self
            .pd_client
            .get_region(&encode_bytes(target_key))
            .unwrap();
        if source_region.id == target_region.id {
            return false;
        }
        self.pd_client
            .try_merge_region(source_region.id, target_region.id);
        true
    }

    /// Try to merge and wait for regions merged.
    /// Return true: merged.
    /// Return false: timeout.
    #[must_use]
    pub fn try_merge_and_wait(
        &mut self,
        source_key: &[u8],
        target_key: &[u8],
        timeout_secs: usize,
    ) -> bool /* merged or not */ {
        try_wait(
            || {
                let sent = self.try_merge(source_key, target_key);
                !sent // `sent` == true only when not merged.
            },
            timeout_secs,
        )
    }

    pub fn try_merge_adjacent_region(
        &mut self,
        source_key: &[u8],
        boundary_prefix: Option<&[u8]>,
        timeout: Duration,
    ) -> Result<()> {
        let source_region = self.pd_client.get_region(source_key)?;
        let raw_end_key = rfstore::store::raw_end_key(&source_region);
        if let Some(prefix) = boundary_prefix {
            if !raw_end_key.starts_with(prefix) {
                return Err(box_err!(
                    "adjacent region out of boundary prefix, raw_end_key: {:?}",
                    raw_end_key
                ));
            }
        }

        let target_region = self.pd_client.get_region(source_region.get_end_key())?;
        assert_ne!(source_region.id, target_region.id);
        self.pd_client
            .merge_region(source_region.id, target_region.id);

        let start = Instant::now();
        loop {
            if block_on(self.pd_client.get_region_by_id(source_region.id))
                .unwrap()
                .is_none()
            {
                break;
            }
            if start.saturating_elapsed() >= timeout {
                return Err(box_err!("region {:?} is still not merged.", source_region));
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        Ok(())
    }

    pub fn must_get_key(&mut self, key: &[u8], put_time: Instant) -> (Vec<u8>, Context) {
        self.must_get_key_version(key, u64::MAX, put_time)
    }

    pub fn must_get_key_version(
        &mut self,
        key: &[u8],
        version: u64,
        put_time: Instant,
    ) -> (Vec<u8>, Context) {
        let (value, ctx) = self
            .get_key_version_opt(key, version, put_time, &RequestOptions::default())
            .unwrap();
        let value = value.unwrap_or_else(|| {
            panic!(
                "{} key {} not found",
                Self::tag_from_ctx(&ctx),
                log_wrappers::hex_encode_upper(key),
            )
        });
        (value, ctx)
    }

    pub fn get_memtable_snapshot(
        &mut self,
        req_ctx: Option<kvproto::kvrpcpb::Context>,
        start_ts: u64,
        ranges: Vec<coppb::KeyRange>,
    ) -> Result<(Vec<u8>, Vec<u8>, Context)> {
        assert!(ranges.len() == 1);
        let start_time = Instant::now();
        let mut region_id = 0;
        let mut store_id_errors = vec![];

        let mut req = coppb::DelegateRequest::default();

        // All ranges should be in the same region.
        for range in ranges {
            let id = self.get_region_id(range.get_start());
            if region_id != 0 && id != region_id {
                return Err(box_err!("Key ranges not in the same region".to_owned()));
            }
            region_id = id;
            req.mut_ranges().push(range);
        }

        if let Some(req_ctx) = req_ctx {
            req.set_context(req_ctx);
        }
        req.set_start_ts(start_ts);
        req.mut_context().set_isolation_level(IsolationLevel::Si);

        while start_time.saturating_elapsed() < self.opts.req_timeout {
            let ctx = self.new_rpc_ctx(region_id, req.get_ranges()[0].get_start());

            if ctx.is_none() {
                continue;
            }

            let ctx = ctx.unwrap();
            let tag = Self::tag_from_ctx(&ctx);
            let store_id = ctx.get_peer().get_store_id();
            let client = self.get_kv_client(store_id);

            req.set_context(ctx.clone());

            let result = client.delegate_coprocessor(&req);

            if result.is_err() {
                store_id_errors.push((store_id, format!("{:?}", result.unwrap_err())));
                sleep(Duration::from_millis(100));
                continue;
            }

            let mut resp = result.unwrap();

            if resp.has_locked() {
                sleep(Duration::from_millis(100));
                continue;
            }

            let other_err = resp.get_other_error();

            if !other_err.is_empty() {
                panic!("unexpected error {:?}", other_err);
            } else if resp.has_region_error() {
                let region_err = resp.get_region_error();

                store_id_errors.push((store_id, format!("{:?}", region_err)));

                if self.handle_retryable_error(&tag, region_err) {
                    continue;
                }

                if self.handle_region_epoch_not_match_or_not_found(region_err) {
                    continue;
                }
                panic!("unexpected error {:?}", region_err);
            }

            return Ok((resp.take_mem_table_data(), resp.take_snapshot(), ctx));
        }

        panic!(
            "region {} failed to get memtable snapshot: {:?}",
            region_id, store_id_errors
        );
    }

    pub fn get_key_version(&mut self, key: &[u8], version: u64) -> Result<Option<Vec<u8>>> {
        let (value, _) =
            self.get_key_version_opt(key, version, Instant::now(), &RequestOptions::default())?;
        Ok(value)
    }

    pub fn get_key_version_opt(
        &mut self,
        key: &[u8],
        version: u64,
        put_time: Instant,
        options: &RequestOptions,
    ) -> Result<(Option<Vec<u8>>, Context)> {
        let start_time = Instant::now();
        let mut tag = ShardTag::default();
        let mut store_id_errors = vec![];
        while start_time.saturating_elapsed() < self.opts.req_timeout {
            let region_id = self.get_region_id(key);
            let ctx = self.new_rpc_ctx_opt(region_id, key, options);
            if ctx.is_none() {
                continue;
            }
            let ctx = ctx.unwrap();
            tag = Self::tag_from_ctx(&ctx);
            let store_id = ctx.get_peer().get_store_id();
            let client = self.get_kv_client(store_id);
            let mut get_req = GetRequest::default();
            get_req.set_context(ctx.clone());
            get_req.set_key(key.to_vec());
            get_req.set_version(version);
            let result = client.kv_get(&get_req);
            if result.is_err() {
                store_id_errors.push((store_id, format!("{:?}", result.unwrap_err())));
                sleep(Duration::from_millis(100));
                self.update_cache_by_id(region_id, None);
                continue;
            }
            let mut resp = result.unwrap();
            if resp.has_region_error() {
                let region_err = resp.get_region_error();
                store_id_errors.push((store_id, format!("{:?}", region_err)));
                if self.handle_retryable_error(&tag, region_err) {
                    continue;
                }
                if self.handle_region_epoch_not_match_or_not_found(region_err) {
                    continue;
                }

                if region_err
                    .get_message()
                    .contains("peer is applying snapshot")
                {
                    continue;
                }
                return Err(box_err!("{} unexpected error {:?}", tag, region_err));
            }
            if resp.has_error() {
                let key_err = resp.take_error();
                info!(
                    "{} get_key_version_opt: encounters key_error: {:?}",
                    tag, key_err
                );
                if !options.resolve_lock {
                    return Err(Error::KeyError(key_err));
                }
                self.handle_key_errors(&tag, version, false, vec![key_err])
                    .expect("handle_key_errors");
                continue;
            }
            if resp.get_not_found() {
                return Ok((None, ctx));
            }
            return Ok((Some(resp.take_value()), ctx));
        }
        Err(box_err!(
            "{} failed to get key {}, errors {:?}, put elapsed {:?}",
            tag,
            log_wrappers::hex_encode_upper(key),
            store_id_errors,
            put_time.saturating_elapsed()
        ))
    }

    pub fn verify_data_with_ref_store(&mut self) -> (usize /* existed */, usize /* deleted */) {
        let ref_store = self.ref_store.lock().unwrap().clone();
        self.verify_data_with_given_ref_store(&ref_store, None, &RequestOptions::default())
            .expect("verify_data_with_ref_store")
    }

    // Note: return verified number of existed entries only, to be uniform with
    // `ClusterTxnClient::verify_data_by_scan`.
    pub fn verify_data_with_given_ref_store(
        &mut self,
        ref_store: &RefStore,
        range: Option<(&[u8], &[u8])>,
        options: &RequestOptions,
    ) -> Result<(usize, usize)> {
        let mut existed_cnt = 0;
        let mut deleted_cnt = 0;
        let start_time = Instant::now();
        for (k, v) in ref_store.iter() {
            if let Some(range) = range {
                if k.as_slice() < range.0 || k.as_slice() >= range.1 {
                    continue;
                }
            }
            self.verify_key_value(k, v.as_ref(), u64::MAX, start_time, options)?;
            if v.is_some() {
                existed_cnt += 1;
            } else {
                deleted_cnt += 1;
            }
        }
        info!(
            "verify_data_with_given_ref_store: verified keys: {}/{} (existed/deleted), takes {:?}",
            existed_cnt,
            deleted_cnt,
            start_time.saturating_elapsed()
        );
        Ok((existed_cnt, deleted_cnt))
    }

    pub fn verify_key_value<T: AsRef<[u8]> + ?Sized>(
        &mut self,
        key: &[u8],
        expect_val: Option<&T>,
        version: u64,
        put_time: Instant,
        options: &RequestOptions,
    ) -> Result<()> {
        let (val, ctx) = self.get_key_version_opt(key, version, put_time, options)?;
        let val = val.as_deref();
        let expect_val = expect_val.map(|v| v.as_ref());
        if val != expect_val {
            return Err(box_err!(
                "{} val not equal for key {}, ver {}, db: {:?}, ref store {:?}",
                Self::tag_from_ctx(&ctx),
                log_wrappers::hex_encode_upper(key),
                version,
                val.map(|v| (v.len(), tikv_util::escape(v))),
                expect_val.map(|v| (v.len(), tikv_util::escape(v)))
            ));
        }
        Ok(())
    }

    pub fn ref_store(&self) -> Arc<Mutex<RefStore>> {
        self.ref_store.clone()
    }

    pub fn replace_ref_store(&mut self, ref_store: Arc<Mutex<RefStore>>) -> Arc<Mutex<RefStore>> {
        std::mem::replace(&mut self.ref_store, ref_store)
    }

    pub fn ref_store_contains_key(&self, key: &[u8]) -> bool {
        self.ref_store.lock().unwrap().contains_key(key)
    }

    pub fn dump_ref_store(&mut self) -> RefStore {
        self.ref_store.lock().unwrap().clone()
    }

    pub fn ingest_ref_store(&mut self, mut ref_store: RefStore) {
        *self.ref_store.lock().unwrap() = std::mem::take(&mut ref_store);
    }

    pub fn set_async_commit(&mut self) {
        self.async_commit = true;
    }

    pub fn set_req_timeout(&mut self, timeout: Duration) {
        self.opts.req_timeout = timeout;
    }

    pub fn txn_file_helper(&self) -> Option<Arc<TxnFileHelper>> {
        self.txn_file_helper.clone()
    }

    pub fn kv_get_mvcc_by_key(&mut self, key: &[u8]) -> Result<Option<kvrpcpb::MvccInfo>> {
        let start_time = Instant::now();
        let mut tag = ShardTag::default();
        let mut store_id_errors = vec![];

        while start_time.saturating_elapsed() < self.opts.req_timeout {
            let region_id = self.get_region_id(key);
            let ctx = self.new_rpc_ctx_opt(region_id, key, &RequestOptions::default());
            if ctx.is_none() {
                continue;
            }

            let ctx = ctx.unwrap();
            tag = Self::tag_from_ctx(&ctx);
            let store_id = ctx.get_peer().get_store_id();
            let client = self.get_kv_client(store_id);

            let mut req = kvrpcpb::MvccGetByKeyRequest::default();
            req.set_context(ctx.clone());
            req.set_key(key.to_vec());

            let result = client.mvcc_get_by_key(&req);
            if result.is_err() {
                store_id_errors.push((store_id, format!("{:?}", result.unwrap_err())));
                sleep(Duration::from_millis(100));
                self.update_cache_by_id(region_id, None);
                continue;
            }

            let mut resp = result.unwrap();
            if resp.has_region_error() {
                let region_err = resp.get_region_error();
                store_id_errors.push((store_id, format!("{:?}", region_err)));
                if self.handle_retryable_error(&tag, region_err) {
                    continue;
                }
                if self.handle_region_epoch_not_match_or_not_found(region_err) {
                    continue;
                }

                if region_err
                    .get_message()
                    .contains("peer is applying snapshot")
                {
                    continue;
                }
                return Err(box_err!("{} unexpected error {:?}", tag, region_err));
            }

            if !resp.has_info() {
                return Ok(None);
            }

            return Ok(Some(resp.take_info()));
        }

        Err(box_err!(
            "{} failed to get mvcc info for key {}, errors {:?}",
            tag,
            log_wrappers::hex_encode_upper(key),
            store_id_errors
        ))
    }

    // TODO: flatten the error
    pub fn kv_pessimistic_lock(
        &mut self,
        primary_key: Bytes,
        keys: Vec<Bytes>,
        start_ts: TimeStamp,
        lock_ttl: u64,
        for_update_ts: TimeStamp,
        wait_timeout: Option<i64>,
    ) -> Result<(
        Vec<kvrpcpb::PessimisticLockKeyResult>,
        Vec<kvrpcpb::KeyError>,
    )> {
        if keys.is_empty() {
            return Err(Error::Other(box_err!(
                "Empty mutations for pessimistic lock"
            )));
        }

        let mutations = keys
            .iter()
            .map(|k| Mutation {
                op: Op::PessimisticLock,
                key: k.clone(),
                value: vec![].into(),
                ..Default::default()
            })
            .collect();

        let grouped_mutations = self.group_mutations_by_region(mutations);

        let mut all_results: Vec<kvrpcpb::PessimisticLockKeyResult> = Vec::new();
        let mut all_key_errors: Vec<kvrpcpb::KeyError> = Vec::new();

        for (id_ver, muts_in_region) in grouped_mutations {
            match self.kv_pessimistic_lock_single_region(
                id_ver,
                muts_in_region,
                &primary_key,
                start_ts,
                lock_ttl,
                for_update_ts,
                wait_timeout,
            ) {
                Ok((results, key_errors)) => {
                    all_results.extend(results);
                    all_key_errors.extend(key_errors);
                }
                Err(e) => {
                    error!(
                        "Pessimistic lock for region {:?} failed with error: {:?}",
                        id_ver, e
                    );
                    return Err(e);
                }
            }
        }

        Ok((all_results, all_key_errors))
    }

    fn kv_pessimistic_lock_single_region(
        &mut self,
        id_ver: RegionIdVer,
        mutations_in_region: Vec<Mutation>,
        primary_key: &Bytes,
        start_ts: TimeStamp,
        lock_ttl: u64,
        for_update_ts: TimeStamp,
        wait_timeout: Option<i64>,
    ) -> Result<(
        Vec<kvrpcpb::PessimisticLockKeyResult>,
        Vec<kvrpcpb::KeyError>,
    )> {
        let region_id = id_ver.id();
        let mut last_err: Option<Error> = None;
        let start_time = Instant::now();
        let timeout = Duration::from_secs(15);

        let representative_key = if mutations_in_region.is_empty() {
            return Err(Error::Other(box_err!(
                "Empty mutations for single region pessimistic lock"
            )));
        } else {
            mutations_in_region[0].get_key()
        };

        while start_time.saturating_elapsed() < timeout {
            let ctx = self
                .new_rpc_ctx(region_id, representative_key)
                .filter(|x| x.get_region_epoch().get_version() == id_ver.ver());

            if ctx.is_none() {
                return Err(Error::RpcContext(region_id));
            }
            let ctx = ctx.unwrap();
            let tag = Self::tag_from_ctx(&ctx);
            let store_id = ctx.get_peer().get_store_id();
            let kv_client = self.get_kv_client(store_id);

            let mut req = kvrpcpb::PessimisticLockRequest::default();
            req.set_context(ctx.clone());
            req.set_mutations(mutations_in_region.iter().map(|m| m.into()).collect());
            req.set_primary_lock(primary_key.to_vec());
            req.set_start_version(start_ts.into_inner());
            req.set_lock_ttl(lock_ttl);
            req.set_for_update_ts(for_update_ts.into_inner());
            if let Some(wt) = wait_timeout {
                req.set_wait_timeout(wt);
            }
            // req.set_return_values(return_values);
            // req.set_min_commit_ts(min_commit_ts.into_inner());
            req.set_wake_up_mode(kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeNormal);

            debug!("{} pessimistic_lock {:?}", tag, req);
            let result = kv_client.kv_pessimistic_lock(&req);

            match result {
                Ok(mut resp) => {
                    if resp.has_region_error() {
                        let region_err = resp.take_region_error();
                        if self.handle_retryable_error(&tag, &region_err) {
                            last_err = Some(Error::RegionError(region_err));
                            continue;
                        }
                        if self.handle_region_epoch_not_match_or_not_found(&region_err) {
                            let keys = mutations_in_region
                                .into_iter()
                                .map(|m| m.get_key().to_vec().into())
                                .collect();
                            return self.kv_pessimistic_lock(
                                primary_key.clone(),
                                keys,
                                start_ts,
                                lock_ttl,
                                for_update_ts,
                                wait_timeout,
                            );
                        }
                        return Err(Error::RegionError(region_err));
                    }
                    return Ok((resp.get_results().to_vec(), resp.get_errors().to_vec()));
                }
                Err(err) => {
                    error!("{} pessimistic_lock grpc error: {:?}", tag, err);
                    last_err = Some(err.into());
                    self.update_cache_by_id(region_id, None);
                    sleep(Duration::from_millis(100));
                }
            }
        }
        error!(
            "{} pessimistic_lock timed out or failed: {:?}",
            region_id, last_err
        );
        Err(last_err.unwrap_or_else(|| Error::Other(box_err!("pessimistic_lock timeout"))))
    }
}

const MIN_TXN_KEY: &[u8] = &[TXN_KEY_PREFIX];
const MAX_TXN_KEY: &[u8] = &[TXN_KEY_PREFIX, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];

#[derive(Clone, Default)]
pub struct ApiV2NoPrefixCodec {}

impl tikv_client::codec::Codec for ApiV2NoPrefixCodec {
    fn encode_request<R: tikv_client::request::KvRequest>(&self, req: &mut R) {
        req.set_api_version(tikv_client::proto::kvrpcpb::ApiVersion::V2);
    }
}

type TxnClient = tikv_client::TransactionClient<ApiV2NoPrefixCodec>;

/// ClusterTxnClient provides transaction operations.
#[derive(Clone)]
pub struct ClusterTxnClient {
    pub inner: TxnClient,

    // Used to get extra info for debug.
    pd_client: Arc<dyn PdClient>,
    cluster_client: ClusterClient,
}

impl Deref for ClusterTxnClient {
    type Target = TxnClient;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ClusterTxnClient {
    pub fn new(
        inner: TxnClient,
        pd_client: Arc<dyn PdClient>,
        cluster_client: ClusterClient,
    ) -> Self {
        Self {
            inner,
            pd_client,
            cluster_client,
        }
    }

    /// Verify data by scanning the given range.
    pub async fn verify_data_by_scan(
        &mut self,
        ref_store: &RefStore,
        range: Option<(&[u8], &[u8])>,
    ) -> Result<usize> {
        fn dump_txn_tasks() {
            #[cfg(feature = "debug-trace-txn-tasks")]
            tikv::storage::txn::debug::dump_txn_tasks();
        }

        let start_time = Instant::now();
        let mut ref_cnt = 0;
        for (k, v) in ref_store.iter() {
            if let Some(range) = range {
                if k.as_slice() < range.0 || k.as_slice() >= range.1 {
                    continue;
                }
            }
            if v.is_some() {
                ref_cnt += 1;
            }
        }
        let limit = ref_cnt + 1; // +1 to check if there are more keys
        let kv_pairs = self
            .kv_scan(range, limit, Duration::from_secs(60))
            .await
            .unwrap()
            .collect::<Vec<_>>();

        let mut db_cnt = 0;
        for kv in &kv_pairs {
            db_cnt += 1;
            let key: &[u8] = kv.key().into();
            let ref_val = ref_store.get(key);

            // Not found in ref store.
            if ref_val.is_none() || ref_val.as_ref().unwrap().is_none() {
                let err: Error = box_err!(
                    "verify_data_by_scan: not found in ref store, db: {} -> {}, ref_val {:?}",
                    log_wrappers::hex_encode_upper(key),
                    tikv_util::escape(kv.value()),
                    ref_val
                );
                dump_txn_tasks();
                self.log_verify_error(key, None, u64::MAX, &err);
                return Err(err);
            }

            // Value not match.
            let ref_val = ref_val.as_ref().unwrap().as_ref().unwrap();
            if ref_val != kv.value() {
                let err: Error = box_err!(
                    "verify_data_by_scan: value not match for key {}, db: {}(len:{}), ref store: {}(len:{})",
                    log_wrappers::hex_encode_upper(key),
                    tikv_util::escape(kv.value()),
                    kv.value().len(),
                    tikv_util::escape(ref_val),
                    ref_val.len()
                );
                dump_txn_tasks();
                self.log_verify_error(key, Some(ref_val), u64::MAX, &err);
                return Err(err);
            }
        }

        // Not found in db.
        if ref_cnt != db_cnt {
            assert!(ref_cnt > db_cnt, "ref_cnt: {}, db_cnt: {}", ref_cnt, db_cnt);
            let mut diff_cnt = 0;
            for (key, ref_val) in ref_store.iter() {
                if ref_val.is_none() {
                    continue;
                }
                if let Some(range) = range {
                    if key.as_slice() < range.0 || key.as_slice() >= range.1 {
                        continue;
                    }
                }

                let res = kv_pairs.binary_search_by(|kv| {
                    let k: &[u8] = kv.key().into();
                    k.cmp(key)
                });
                if res.is_err() {
                    let err: Error = box_err!(
                        "verify_data_by_scan: not found in db, ref store: {} -> {:?}",
                        log_wrappers::hex_encode_upper(key),
                        tikv_util::escape(ref_val.as_ref().unwrap())
                    );
                    self.log_verify_error(
                        key,
                        ref_val.as_ref().map(|x| x.as_slice()),
                        u64::MAX,
                        &err,
                    );

                    diff_cnt += 1;
                    if diff_cnt >= ref_cnt - db_cnt {
                        break;
                    }
                }
            }
            dump_txn_tasks();
            return Err(box_err!(
                "verify_data_by_scan: entries count not match, db: {}, ref store: {}",
                db_cnt,
                ref_cnt
            ));
        }

        info!(
            "verify_data_by_scan: verified entries {}, takes {:?}",
            ref_cnt,
            start_time.saturating_elapsed()
        );
        Ok(ref_cnt)
    }

    pub async fn kv_scan(
        &self,
        range: Option<(&[u8], &[u8])>,
        limit: usize,
        timeout: Duration,
    ) -> tikv_client::Result<impl Iterator<Item = tikv_client::KvPair>> {
        let start_ts = self.inner.current_timestamp().await?;
        let scan_range = if let Some(range) = range {
            (range.0, range.1)
        } else {
            (MIN_TXN_KEY, MAX_TXN_KEY)
        };
        let tag = self.tag_from_key("kv_scan", scan_range.0);

        let mut snapshot = self
            .inner
            .snapshot(start_ts, TransactionOptions::new_pessimistic());

        let start_time = Instant::now();
        let mut last_error: Option<tikv_client::Error> = None;
        while start_time.saturating_elapsed() < timeout {
            match tokio::time::timeout(
                timeout,
                snapshot.scan(scan_range.into_owned(), limit as u32),
            )
            .await
            {
                Ok(Ok(kvs)) => return Ok(kvs),
                Ok(Err(err)) if Self::is_kv_error_retryable(&tag, &err) => {
                    self.log_kv_error(&tag, &err);
                    last_error = Some(err);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                Ok(Err(err)) => {
                    self.log_kv_error(&tag, &err);
                    return Err(err);
                }
                Err(elapsed) => {
                    let err_msg = format!("{} kv_scan timeout: {:?}", tag, elapsed);
                    error!("{}", err_msg);
                    return Err(tikv_client::Error::OtherError(err_msg));
                }
            }
        }
        Err(last_error.unwrap())
    }

    async fn kv_mutate_inner(
        &self,
        txn: &mut tikv_client::Transaction<ApiV2NoPrefixCodec>,
        muts: Vec<KvMutation>,
        started_commit: &mut bool,
    ) -> std::result::Result<(), tikv_client::Error> {
        if !*started_commit {
            txn.batch_mutate(muts).await?;
        }

        *started_commit = true;
        let _ = txn.commit().await?;
        Ok(())
    }

    pub async fn kv_mutate(&self, muts: Vec<Mutation>, timeout: Duration) -> Result<()> {
        assert!(!muts.is_empty());
        let tag = self.tag_from_key("kv_mutate", muts[0].get_key());
        let muts: Vec<KvMutation> = muts
            .into_iter()
            .map(|m| KvMutation {
                op: m.op.value(),
                key: m.key.to_vec(),
                value: m.value.to_vec(),
                ..Default::default()
            })
            .collect();

        // Must use the same transaction during retries.
        // Otherwise the later transaction in retries would be blocked by the locks of a
        // previous one.
        let option = TransactionOptions::new_pessimistic().drop_check(CheckLevel::Warn);
        let mut txn = self.begin_with_options(option).await?;
        let mut started_commit = false;

        let start_time = Instant::now();
        let mut last_error: Option<tikv_client::Error> = None;
        while start_time.saturating_elapsed() < timeout {
            match self
                .kv_mutate_inner(&mut txn, muts.clone(), &mut started_commit)
                .await
            {
                Ok(_) => return Ok(()),
                Err(err) if Self::is_kv_error_retryable(&tag, &err) => {
                    self.log_kv_error(&tag, &err);
                    last_error = Some(err);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                Err(err) => {
                    self.log_kv_error(&tag, &err);
                    return Err(err.into());
                }
            }
        }
        Err(last_error.unwrap().into())
    }

    pub async fn kv_unsafe_destroy_range(
        &self,
        start: &[u8],
        end: &[u8],
        timeout: Duration,
    ) -> Result<()> {
        let tag = self.tag_from_key("kv_unsafe_destroy_range", start);
        let is_error_retryable = |err: &tikv_client::Error| {
            // Will get `MultipleKeyErrors` when region version not match.
            matches!(err, tikv_client::Error::MultipleKeyErrors(_))
                || Self::is_kv_error_retryable(&tag, err)
        };

        let start_time = Instant::now();
        let mut last_error: Option<tikv_client::Error> = None;
        while start_time.saturating_elapsed() < timeout {
            match self
                .inner
                .unsafe_destroy_range((start, end).into_owned())
                .await
            {
                Ok(_) => return Ok(()),
                Err(err) if is_error_retryable(&err) => {
                    self.log_kv_error(&tag, &err);
                    last_error = Some(err);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                Err(err) => {
                    self.log_kv_error(&tag, &err);
                    return Err(err.into());
                }
            }
        }
        Err(last_error.unwrap().into())
    }

    pub(crate) fn is_kv_error_retryable(tag: &str, err: &tikv_client::Error) -> bool {
        match err {
            tikv_client::Error::Grpc(_)
            | tikv_client::Error::GrpcAPI(_)
            | tikv_client::Error::Channel(_) => true,
            tikv_client::Error::RegionError(_) => true,
            tikv_client::Error::PessimisticLockError { inner, .. } => {
                Self::is_kv_error_retryable(tag, inner)
            }
            tikv_client::Error::MultipleKeyErrors(key_errors) => key_errors
                .iter()
                .all(|err| Self::is_kv_error_retryable(tag, err)),
            tikv_client::Error::KeyError(key_err) => {
                Self::is_key_error_retryable(tag, key_err.as_ref())
            }
            tikv_client::Error::ResolveLockError(_) => true,
            _ => false,
        }
    }

    fn is_key_error_retryable(tag: &str, key_err: &tikv_client::proto::kvrpcpb::KeyError) -> bool {
        let ok = !key_err.retryable.is_empty();
        info!("{} ClusterTxnClient::is_key_error_retryable", tag; "key_err" => ?key_err, "ok" => ok);
        ok
    }

    fn log_kv_error(&self, tag: &str, err: &tikv_client::Error) {
        match err {
            tikv_client::Error::PessimisticLockError {
                inner,
                success_keys,
            } => {
                error!(
                    "{} tikv_client::PessimisticLockError: inner: {:?}, success_keys: {:?}",
                    tag, inner, success_keys
                );
                self.log_kv_error(tag, inner);
            }
            tikv_client::Error::ResolveLockError(lock_info) => {
                if let Some(first) = lock_info.first() {
                    let region = self.pd_client.get_region(&first.key).unwrap();
                    error!(
                        "{} tikv_client::ResolveLockError (first), lock region: {}:{}, lock_info: {:?}",
                        tag,
                        region.get_id(),
                        region.get_region_epoch().get_version(),
                        lock_info
                    );
                }
            }
            _ => {
                error!("{} tikv_client::Error: {:?}", tag, err);
            }
        }
    }

    fn log_verify_error(
        &mut self,
        key: &[u8],
        expect_val: Option<&[u8]>,
        version: u64,
        err: &Error,
    ) {
        let res = self.cluster_client.verify_key_value(
            key,
            expect_val,
            version,
            Instant::now(),
            &RequestOptions::default(),
        );
        assert!(
            res.is_err(),
            "verify_key_value should fail, verify error: {:?}",
            err
        );
        error!("verify_key_value error: {:?}", res.unwrap_err());
    }

    fn tag_from_key(&self, interface: &str, key: &[u8]) -> String {
        let region = self.pd_client.get_region(&encode_bytes(key)).unwrap();
        format!(
            "[{}] {}:{}",
            interface,
            region.get_id(),
            region.get_region_epoch().get_version()
        )
    }
}

#[derive(Clone, Copy, Debug)]
pub enum TxnWriteMethod {
    Normal,
    FileBased,
}

#[derive(Clone)]
pub enum TxnMutations {
    Muts {
        muts: Vec<Mutation>,
    },
    Chunks {
        primary: Bytes,
        chunks: Vec<TxnFileChunk>,
        sample_keys: Option<Vec<Bytes>>, // Filled by `group_by_regions`.
        helper: Arc<TxnFileHelper>,
    },
}

pub enum PrimaryFilter {
    All,
    PrimaryOnly,
    Secondaries,
}

impl TxnMutations {
    pub async fn build(
        muts: Vec<Mutation>,
        write_method: TxnWriteMethod,
        txn_file_helper: Option<Arc<TxnFileHelper>>,
    ) -> Result<Self> {
        match write_method {
            TxnWriteMethod::Normal => Ok(TxnMutations::Muts { muts }),
            TxnWriteMethod::FileBased => {
                let primary = muts.first().unwrap().key.clone();
                let helper = txn_file_helper.unwrap();
                let chunks = helper.build_txn_chunks(muts).await?;
                Ok(TxnMutations::Chunks {
                    primary,
                    chunks,
                    sample_keys: None,
                    helper,
                })
            }
        }
    }

    pub fn from_normal(muts: Vec<Mutation>) -> Self {
        TxnMutations::Muts { muts }
    }

    pub fn primary(&self) -> Bytes {
        match self {
            Self::Muts { muts } => muts.first().unwrap().key.clone(),
            Self::Chunks { primary, .. } => primary.clone(),
        }
    }

    pub fn secondaries(&self) -> Vec<Bytes> {
        match self {
            Self::Muts { muts } => muts.iter().skip(1).map(|m| m.key.clone()).collect(),
            Self::Chunks { .. } => panic!("async commit for file based is not supported"),
        }
    }

    pub fn first(&self) -> Option<&Mutation> {
        match self {
            Self::Muts { muts } => muts.first(),
            Self::Chunks { .. } => None,
        }
    }

    pub fn group_by_regions(
        &self,
        client: &mut ClusterClient,
        primary_filter: PrimaryFilter,
    ) -> Result<Vec<(RegionIdVer, Self)>> {
        Ok(match self {
            Self::Muts { muts } => {
                let muts = match primary_filter {
                    PrimaryFilter::All => muts.clone(),
                    PrimaryFilter::PrimaryOnly => muts[..1].to_vec(),
                    PrimaryFilter::Secondaries => muts[1..].to_vec(),
                };
                client
                    .group_mutations_by_region(muts)
                    .into_iter()
                    .map(|(r, muts)| (r, TxnMutations::Muts { muts }))
                    .collect()
            }
            Self::Chunks {
                primary,
                chunks,
                helper,
                ..
            } => {
                let chunks = match primary_filter {
                    PrimaryFilter::All => Cow::from(chunks),
                    PrimaryFilter::PrimaryOnly => Cow::from(vec![chunks[0].clone()]),
                    PrimaryFilter::Secondaries => Cow::from(chunks),
                };

                let region_muts = Self::group_txn_chunks_by_regions(primary, &chunks, client)
                    .into_iter()
                    .map(|(r, chunks, sample_keys)| {
                        (
                            r.id_ver(),
                            TxnMutations::Chunks {
                                primary: primary.clone(),
                                chunks,
                                sample_keys: Some(sample_keys),
                                helper: helper.clone(),
                            },
                        )
                    });
                match primary_filter {
                    PrimaryFilter::All => region_muts.collect(),
                    PrimaryFilter::PrimaryOnly => region_muts.take(1).collect(),
                    PrimaryFilter::Secondaries => region_muts.skip(1).collect(),
                }
            }
        })
    }

    fn group_txn_chunks_by_regions(
        primary: &Bytes,
        chunks: &[TxnFileChunk],
        client: &mut ClusterClient,
    ) -> Vec<(RawRegion, Vec<TxnFileChunk>, Vec<Bytes>)> {
        let mut region_chunks = HashMap::new();
        for chunk in chunks {
            let regions = Self::get_txn_chunks_regions(primary, chunk, client);
            for (region, sample_key) in regions {
                let (_, chunks, sample_keys) = region_chunks
                    .entry(region.id_ver())
                    .or_insert_with(|| (region, vec![], vec![]));
                chunks.push(chunk.clone());
                if let Some(sample_key) = sample_key {
                    sample_keys.push(sample_key);
                }
            }
        }
        let mut region_chunks = region_chunks.into_values().collect::<Vec<_>>();
        // Sort by chunks first, and then by region, to make sure that primary
        // key is in the first batch:
        // 1. Different batches may contain the same chunks.
        // 2. Different batches may have regions with same start key (if region merge
        //    happens during grouping).
        // 3. Bigger batches may have regions with smaller start key (if region merge
        //    happens during grouping).
        region_chunks.sort_by(|a, b| {
            a.1[0]
                .outer_smallest()
                .cmp(b.1[0].outer_smallest())
                .then_with(|| a.0.start_key().cmp(b.0.start_key()))
        });
        info!("group_txn_chunks_by_regions"; "region_chunks" => ?region_chunks);
        region_chunks
    }

    fn get_txn_chunks_regions(
        primary: &Bytes,
        chunk: &TxnFileChunk,
        client: &mut ClusterClient,
    ) -> Vec<(RawRegion, Option<Bytes>)> {
        let mut regions = vec![];
        let mut start_key = chunk.outer_smallest().to_vec();
        while start_key.as_slice() <= chunk.outer_biggest().as_bytes() {
            let region = client.get_region_by_key(&start_key);
            start_key.clear();
            start_key.extend_from_slice(region.end_key());
            let (first_data_key, ok) =
                chunk.has_data_in_range(primary, region.start_key(), region.end_key());
            if ok {
                regions.push((region, first_data_key));
            }
        }
        regions
    }

    pub fn set_prewrite_req(&self, req: &mut PrewriteRequest) {
        match self {
            Self::Muts { muts } => {
                let mutations = muts
                    .iter()
                    .map(|m| kvrpcpb::Mutation::from(m))
                    .collect::<Vec<_>>();
                req.set_mutations(mutations.into());
            }
            Self::Chunks { chunks, .. } => {
                let chunk_ids = chunks.iter().map(|c| c.chunk_id).collect::<Vec<_>>();
                req.set_txn_file_chunks(chunk_ids);
            }
        }
    }

    pub fn set_commit_req(&self, req: &mut CommitRequest) {
        match self {
            Self::Muts { muts } => {
                let keys = muts.iter().map(|m| m.key.to_vec()).collect::<Vec<_>>();
                req.set_keys(keys.into());
            }
            Self::Chunks { sample_keys, .. } => {
                let keys = sample_keys
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|x| x.to_vec())
                    .collect::<Vec<_>>();
                req.set_keys(keys.into());
                req.set_is_txn_file(true);
            }
        }
    }

    pub fn set_rollback_req(&self, req: &mut kvrpcpb::BatchRollbackRequest) {
        match self {
            Self::Muts { muts } => {
                let keys = muts.iter().map(|m| m.key.to_vec()).collect::<Vec<_>>();
                req.set_keys(keys.into());
            }
            Self::Chunks { sample_keys, .. } => {
                let keys = sample_keys
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|x| x.to_vec())
                    .collect::<Vec<_>>();
                req.set_keys(keys.into());
                req.set_is_txn_file(true);
            }
        }
    }
}

fn api_version_of_key(key: &[u8]) -> kvrpcpb::ApiVersion {
    match ApiV2::parse_key_mode(key) {
        KeyMode::Txn | KeyMode::Raw => kvrpcpb::ApiVersion::V2,
        _ => kvrpcpb::ApiVersion::V1,
    }
}

pub struct Transaction {
    start_ts: TimeStamp,
    min_commit_ts: TimeStamp,
    async_commit: bool,

    pd_client: Arc<dyn PdClient>,
}

impl Transaction {
    fn get_commit_ts(&self) -> TimeStamp {
        if !self.async_commit {
            block_on(self.pd_client.get_tso()).unwrap()
        } else {
            self.min_commit_ts
        }
    }
}
