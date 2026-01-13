// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use engine_rocks::{RocksDbVector, RocksEngine, RocksEngineIterator, RocksSnapshot};
use engine_traits::{
    CF_RAFT, Error as EngineError, IterOptions, Peekable, ReadOptions, Result as EngineResult,
    util::check_key_in_range,
};
use fail::fail_point;
use keys::DATA_PREFIX_KEY;
use kvproto::{kvrpcpb::ExtraOp as TxnExtraOp, metapb::Region, raft_serverpb::RaftApplyState};
use pd_client::BucketMeta;
use tikv_util::{
    box_err, error, keybuilder::KeyBuilder, metrics::CRITICAL_ERROR,
    panic_when_unexpected_key_or_data, set_panic_mark,
};

use crate::{
    Error, Result,
    store::{TxnExt, util},
};

/// Snapshot of a region.
///
/// Only data within a region can be accessed.
#[derive(Debug)]
pub struct RegionSnapshot {
    snap: Arc<RocksSnapshot>,
    region: Arc<Region>,
    apply_index: Arc<AtomicU64>,
    pub term: Option<NonZeroU64>,
    pub txn_extra_op: TxnExtraOp,
    // `None` means the snapshot does not provide peer related transaction extensions.
    pub txn_ext: Option<Arc<TxnExt>>,
    pub bucket_meta: Option<Arc<BucketMeta>>,
}

impl RegionSnapshot {
    pub fn from_raw(db: RocksEngine, region: Region) -> RegionSnapshot {
        RegionSnapshot::from_snapshot(Arc::new(db.snapshot()), Arc::new(region))
    }

    pub fn from_snapshot(snap: Arc<RocksSnapshot>, region: Arc<Region>) -> RegionSnapshot {
        RegionSnapshot {
            snap,
            region,
            // Use 0 to indicate that the apply index is missing and we need to KvGet it,
            // since apply index must be >= RAFT_INIT_LOG_INDEX.
            apply_index: Arc::new(AtomicU64::new(0)),
            term: None,
            txn_extra_op: TxnExtraOp::Noop,
            txn_ext: None,
            bucket_meta: None,
        }
    }

    #[inline]
    pub fn get_region(&self) -> &Region {
        &self.region
    }

    #[inline]
    pub fn get_snapshot(&self) -> &RocksSnapshot {
        self.snap.as_ref()
    }

    #[inline]
    pub fn get_apply_index(&self) -> Result<u64> {
        let apply_index = self.apply_index.load(Ordering::SeqCst);
        if apply_index == 0 {
            self.get_apply_index_from_storage()
        } else {
            Ok(apply_index)
        }
    }

    fn get_apply_index_from_storage(&self) -> Result<u64> {
        let apply_state: Option<RaftApplyState> = self
            .snap
            .get_msg_cf(CF_RAFT, &keys::apply_state_key(self.region.get_id()))?;
        match apply_state {
            Some(s) => {
                let apply_index = s.get_applied_index();
                self.apply_index.store(apply_index, Ordering::SeqCst);
                Ok(apply_index)
            }
            None => Err(box_err!("Unable to get applied index")),
        }
    }

    pub fn iter(&self, cf: &str, iter_opt: IterOptions) -> Result<RegionIterator> {
        Ok(RegionIterator::new(
            &self.snap,
            Arc::clone(&self.region),
            iter_opt,
            cf,
        ))
    }

    // scan scans database using an iterator in range [start_key, end_key), calls
    // function f for each iteration, if f returns false, terminates this scan.
    pub fn scan<F>(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start = KeyBuilder::from_slice(start_key, DATA_PREFIX_KEY.len(), 0);
        let end = KeyBuilder::from_slice(end_key, DATA_PREFIX_KEY.len(), 0);
        let iter_opt = IterOptions::new(Some(start), Some(end), fill_cache);

        let mut it = self.iter(cf, iter_opt)?;
        let mut it_valid = it.seek(start_key)?;
        while it_valid {
            it_valid = f(it.key(), it.value())? && it.next()?;
        }
        Ok(())
    }

    #[inline]
    pub fn get_start_key(&self) -> &[u8] {
        self.region.get_start_key()
    }

    #[inline]
    pub fn get_end_key(&self) -> &[u8] {
        self.region.get_end_key()
    }
}

impl Clone for RegionSnapshot {
    fn clone(&self) -> Self {
        RegionSnapshot {
            snap: self.snap.clone(),
            region: Arc::clone(&self.region),
            apply_index: Arc::clone(&self.apply_index),
            term: self.term,
            txn_extra_op: self.txn_extra_op,
            txn_ext: self.txn_ext.clone(),
            bucket_meta: self.bucket_meta.clone(),
        }
    }
}

impl Peekable for RegionSnapshot {
    type DbVector = RocksDbVector;

    fn get_value_opt(
        &self,
        opts: &ReadOptions,
        key: &[u8],
    ) -> EngineResult<Option<Self::DbVector>> {
        check_key_in_range(
            key,
            self.region.get_id(),
            self.region.get_start_key(),
            self.region.get_end_key(),
        )
        .map_err(|e| EngineError::Other(box_err!(e)))?;
        let data_key = keys::data_key(key);
        self.snap
            .get_value_opt(opts, &data_key)
            .map_err(|e| self.handle_get_value_error(e, "", key))
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> EngineResult<Option<Self::DbVector>> {
        check_key_in_range(
            key,
            self.region.get_id(),
            self.region.get_start_key(),
            self.region.get_end_key(),
        )
        .map_err(|e| EngineError::Other(box_err!(e)))?;
        let data_key = keys::data_key(key);
        self.snap
            .get_value_cf_opt(opts, cf, &data_key)
            .map_err(|e| self.handle_get_value_error(e, cf, key))
    }
}

impl RegionSnapshot {
    #[inline(never)]
    fn handle_get_value_error(&self, e: EngineError, cf: &str, key: &[u8]) -> EngineError {
        CRITICAL_ERROR.with_label_values(&["rocksdb get"]).inc();
        if panic_when_unexpected_key_or_data() {
            set_panic_mark();
            panic!(
                "failed to get value of key {} in region {}: {:?}",
                log_wrappers::Value::key(key),
                self.region.get_id(),
                e,
            );
        } else {
            error!(
                "failed to get value of key in cf";
                "key" => log_wrappers::Value::key(key),
                "region" => self.region.get_id(),
                "cf" => cf,
                "error" => ?e,
            );
            e
        }
    }
}

/// `RegionIterator` wrap a rocksdb iterator and only allow it to
/// iterate in the region. It behaves as if underlying
/// db only contains one region.
pub struct RegionIterator {
    iter: RocksEngineIterator,
    region: Arc<Region>,
}

fn update_lower_bound(iter_opt: &mut IterOptions, region: &Region) {
    let region_start_key = keys::enc_start_key(region);
    if iter_opt.lower_bound().is_some() && !iter_opt.lower_bound().as_ref().unwrap().is_empty() {
        iter_opt.set_lower_bound_prefix(keys::DATA_PREFIX_KEY);
        if region_start_key.as_slice() > *iter_opt.lower_bound().as_ref().unwrap() {
            iter_opt.set_vec_lower_bound(region_start_key);
        }
    } else {
        iter_opt.set_vec_lower_bound(region_start_key);
    }
}

fn update_upper_bound(iter_opt: &mut IterOptions, region: &Region) {
    let region_end_key = keys::enc_end_key(region);
    if iter_opt.upper_bound().is_some() && !iter_opt.upper_bound().as_ref().unwrap().is_empty() {
        iter_opt.set_upper_bound_prefix(keys::DATA_PREFIX_KEY);
        if region_end_key.as_slice() < *iter_opt.upper_bound().as_ref().unwrap() {
            iter_opt.set_vec_upper_bound(region_end_key, 0);
        }
    } else {
        iter_opt.set_vec_upper_bound(region_end_key, 0);
    }
}

// we use engine::rocks's style iterator, doesn't need to impl std iterator.
impl RegionIterator {
    pub fn new(
        snap: &RocksSnapshot,
        region: Arc<Region>,
        mut iter_opt: IterOptions,
        cf: &str,
    ) -> RegionIterator {
        update_lower_bound(&mut iter_opt, &region);
        update_upper_bound(&mut iter_opt, &region);
        let iter = snap
            .iterator_opt(cf, iter_opt)
            .expect("creating snapshot iterator"); // FIXME error handling
        RegionIterator { iter, region }
    }

    pub fn seek_to_first(&mut self) -> Result<bool> {
        self.iter.seek_to_first().map_err(Error::from)
    }

    pub fn seek_to_last(&mut self) -> Result<bool> {
        self.iter.seek_to_last().map_err(Error::from)
    }

    pub fn seek(&mut self, key: &[u8]) -> Result<bool> {
        fail_point!("region_snapshot_seek", |_| {
            Err(box_err!("region seek error"))
        });
        self.should_seekable(key)?;
        let key = keys::data_key(key);
        self.iter.seek(&key).map_err(Error::from)
    }

    pub fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        self.should_seekable(key)?;
        let key = keys::data_key(key);
        self.iter.seek_for_prev(&key).map_err(Error::from)
    }

    pub fn prev(&mut self) -> Result<bool> {
        self.iter.prev().map_err(Error::from)
    }

    pub fn next(&mut self) -> Result<bool> {
        self.iter.next().map_err(Error::from)
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        keys::origin_key(self.iter.key())
    }

    #[inline]
    pub fn value(&self) -> &[u8] {
        self.iter.value()
    }

    #[inline]
    pub fn valid(&self) -> Result<bool> {
        self.iter.valid().map_err(Error::from)
    }

    #[inline]
    pub fn should_seekable(&self, key: &[u8]) -> Result<()> {
        if let Err(e) = util::check_key_in_region_inclusive(key, &self.region) {
            return handle_check_key_in_region_error(e);
        }
        Ok(())
    }
}

#[inline(never)]
fn handle_check_key_in_region_error(e: crate::Error) -> Result<()> {
    // Split out the error case to reduce hot-path code size.
    CRITICAL_ERROR
        .with_label_values(&["key not in region"])
        .inc();
    if panic_when_unexpected_key_or_data() {
        set_panic_mark();
        panic!("key exceed bound: {:?}", e);
    } else {
        Err(e)
    }
}
