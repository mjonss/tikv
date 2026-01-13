// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{num::NonZeroU64, sync::Arc};

use engine_traits::{CfName, IterOptions, ReadOptions};
use kvengine::{SnapAccess, ValueCache};
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use pd_client::BucketMeta;
use raftstore::store::TxnExt;
use rfstore::Error as RaftServerError;
use txn_types::{Key, Value};

use crate::{self as kv, Error, ErrorInner, Iterator, Snapshot, SnapshotExt};

impl From<RaftServerError> for Error {
    fn from(e: RaftServerError) -> Error {
        Error(Box::new(ErrorInner::Request(e.into())))
    }
}

pub struct RegionSnapshotExt<'a> {
    snapshot: &'a rfstore::store::RegionSnapshot,
}

impl SnapshotExt for RegionSnapshotExt<'_> {
    #[inline]
    fn get_data_version(&self) -> Option<u64> {
        None
    }

    fn is_max_ts_synced(&self) -> bool {
        self.snapshot
            .txn_ext
            .as_ref()
            .map(|txn_ext| txn_ext.is_max_ts_synced())
            .unwrap_or(false)
    }

    fn get_term(&self) -> Option<NonZeroU64> {
        self.snapshot.term
    }

    fn get_txn_extra_op(&self) -> TxnExtraOp {
        self.snapshot.txn_extra_op
    }

    fn get_txn_ext(&self) -> Option<&Arc<TxnExt>> {
        self.snapshot.txn_ext.as_ref()
    }

    fn get_buckets(&self) -> Option<Arc<BucketMeta>> {
        self.snapshot.bucket_meta.clone()
    }
}

impl Snapshot for rfstore::store::RegionSnapshot {
    type Iter = rfstore::store::RegionSnapshotIterator;
    type Ext<'a> = RegionSnapshotExt<'a>;

    fn get(&self, _key: &Key) -> kv::Result<Option<Value>> {
        unreachable!()
    }

    fn get_cf(&self, _cf: CfName, _key: &Key) -> kv::Result<Option<Value>> {
        unreachable!()
    }

    fn get_cf_opt(&self, _opts: ReadOptions, _cf: CfName, _key: &Key) -> kv::Result<Option<Value>> {
        unreachable!()
    }

    fn iter(&self, _cf: CfName, _iter_opt: IterOptions) -> kv::Result<Self::Iter> {
        unreachable!()
    }

    #[inline]
    fn lower_bound(&self) -> Option<&[u8]> {
        unreachable!()
    }

    #[inline]
    fn upper_bound(&self) -> Option<&[u8]> {
        unreachable!()
    }

    fn ext(&self) -> RegionSnapshotExt<'_> {
        RegionSnapshotExt { snapshot: self }
    }

    fn get_kvengine_snap(&self) -> Option<&SnapAccess> {
        Some(&self.snap)
    }

    fn get_value_cache(&self) -> Option<&ValueCache> {
        self.value_cache.as_ref()
    }

    fn is_sync(&self) -> bool {
        self.snap.is_sync()
    }
}

#[allow(dead_code)]
impl Iterator for rfstore::store::RegionSnapshotIterator {
    fn next(&mut self) -> kv::Result<bool> {
        unreachable!()
    }

    fn prev(&mut self) -> kv::Result<bool> {
        unreachable!()
    }

    fn seek(&mut self, _key: &Key) -> kv::Result<bool> {
        unreachable!()
    }

    fn seek_for_prev(&mut self, _key: &Key) -> kv::Result<bool> {
        unreachable!()
    }

    fn seek_to_first(&mut self) -> kv::Result<bool> {
        unreachable!()
    }

    fn seek_to_last(&mut self) -> kv::Result<bool> {
        unreachable!()
    }

    fn valid(&self) -> kv::Result<bool> {
        unreachable!()
    }

    fn key(&self) -> &[u8] {
        unreachable!()
    }

    fn value(&self) -> &[u8] {
        unreachable!()
    }
}
