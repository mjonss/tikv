// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{borrow::Cow, marker::PhantomData};

use bytes::{Buf, Bytes};
use kvengine::{Item, SnapAccess, UserMeta, ValueCacheValue, read};
use kvproto::kvrpcpb::IsolationLevel;
use tikv_kv::{Snapshot, Statistics};
use txn_types::{Key, Lock, OldValue, TimeStamp, TsSet, Value, Write, WriteType, is_short_value};

use crate::storage::{
    REQUEST_EXCEED_BOUND, mvcc,
    mvcc::NewerTsCheckState,
    txn::{Error, ErrorInner, Result, TxnEntry, TxnEntryStore},
};

pub struct CloudStore<S: Snapshot> {
    marker: PhantomData<S>,
    snapshot: kvengine::SnapAccess,
    value_cache: Option<kvengine::ValueCache>,
    start_ts: u64,
    bypass_locks: TsSet,
    fill_cache: bool,
    stats: Statistics,
}

const WRITE_CF: usize = 0;
const LOCK_CF: usize = 1;

#[maybe_async::async_trait]
impl<S: Snapshot> super::Store for CloudStore<S> {
    type Scanner = CloudStoreScanner;

    #[maybe_async]
    async fn get(&self, user_key: &Key, statistics: &mut Statistics) -> Result<Option<Value>> {
        let item = Self::get_inner(
            user_key,
            &self.snapshot,
            self.start_ts,
            &self.bypass_locks,
            statistics,
            self.value_cache.as_ref(),
        )
        .await?;
        if item.value_len() > 0 {
            Ok(Some(item.get_value().to_vec()))
        } else {
            Ok(None)
        }
    }

    #[maybe_async]
    async fn incremental_get(&mut self, user_key: &Key) -> Result<Option<Value>> {
        let stat = &mut self.stats;
        let item = Self::get_inner(
            user_key,
            &self.snapshot,
            self.start_ts,
            &self.bypass_locks,
            stat,
            self.value_cache.as_ref(),
        )
        .await?;
        if item.value_len() > 0 {
            Ok(Some(item.get_value().to_vec()))
        } else {
            Ok(None)
        }
    }

    fn incremental_get_take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.stats)
    }

    fn incremental_get_met_newer_ts_data(&self) -> NewerTsCheckState {
        NewerTsCheckState::Unknown
    }

    #[maybe_async]
    async fn batch_get(
        &self,
        keys: &[Key],
        statistics: &mut Vec<Statistics>,
    ) -> Result<Vec<Result<Option<Value>>>> {
        let mut res_vec = Vec::with_capacity(keys.len());
        for key in keys {
            let mut stats = Statistics::default();
            let res = self.get(key, &mut stats).await;
            res_vec.push(res);
            statistics.push(stats);
        }
        Ok(res_vec)
    }

    #[maybe_async]
    async fn scanner(
        &mut self,
        desc: bool,
        _key_only: bool,
        _check_has_newer_ts_data: bool,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
    ) -> Result<Self::Scanner> {
        self.scanner_inner(desc, lower_bound, upper_bound, false)
            .await
    }

    fn get_kvengine_snap(&self) -> Option<kvengine::SnapAccess> {
        Some(self.snapshot.clone())
    }

    fn is_sync(&self) -> bool {
        self.snapshot.is_sync()
    }

    fn get_read_ts(&self) -> u64 {
        self.start_ts
    }

    #[inline]
    fn is_check_has_newer_ts_data(&self) -> bool {
        false
    }
}

impl<S: Snapshot> CloudStore<S> {
    pub fn new(snapshot: S, start_ts: u64, bypass_locks: TsSet, fill_cache: bool) -> Self {
        Self {
            marker: PhantomData,
            snapshot: snapshot.get_kvengine_snap().unwrap().clone(),
            value_cache: snapshot.get_value_cache().cloned(),
            start_ts,
            bypass_locks,
            fill_cache,
            stats: Statistics::default(),
        }
    }

    #[maybe_async::both]
    async fn get_inner<'a>(
        user_key: &Key,
        snap: &'a kvengine::SnapAccess,
        start_ts: u64,
        bypass_locks: &TsSet,
        statistics: &mut Statistics,
        value_cache: Option<&'a kvengine::ValueCache>,
    ) -> mvcc::Result<Item<'a>> {
        tikv_util::set_current_region(snap.get_id());

        let raw_key = user_key.to_raw()?;
        let item = snap.get(
            LOCK_CF,
            &raw_key,
            snap.get_mem_table_snap_version().into_inner(),
        );
        statistics.lock.get += 1;
        statistics.lock.flow_stats.read_keys += 1;
        statistics.lock.flow_stats.read_bytes += raw_key.len() + item.value_len();
        statistics.lock.processed_keys += 1;
        let has_lock = item.value_len() > 0;
        if has_lock {
            let lock = Lock::parse(item.get_value()).unwrap();
            Lock::check_ts_conflict(
                Cow::Borrowed(&lock),
                user_key,
                TimeStamp::new(start_ts),
                bypass_locks,
                IsolationLevel::Si,
            )?;
        }
        if snap.get_start_key() > raw_key.as_slice() || snap.get_end_key() <= raw_key.as_slice() {
            error!(
                "get key {:?} out of snap range {:?}, {:?}, {}:{}",
                raw_key.as_slice(),
                snap.get_start_key(),
                snap.get_end_key(),
                snap.get_id(),
                snap.get_version()
            );
            return Ok(Item::default());
        }
        let item = if let Some(cache) = value_cache {
            if let Some(val) = cache.get(snap, &raw_key, start_ts, has_lock) {
                let hit_item = if cache.is_double_check() {
                    let origin_item = snap.get(WRITE_CF, &raw_key, start_ts).await;
                    if origin_item.version != val.version
                        || origin_item.get_value() != val.value.chunk()
                    {
                        error!(
                            "value cache mismatch: read missing or invalid";
                            "key" => log_wrappers::Value::key(&raw_key),
                            "region_id" => snap.get_id(),
                            "shard_ver" => snap.get_version(),
                            "keyspace_id" => snap.get_keyspace_id(),
                            "start_ts" => start_ts,
                            "cached_version" => val.version,
                            "read_version" => origin_item.version,
                        );
                        debug_assert!(false);
                    }
                    origin_item
                } else {
                    Item::from_cached_value(&val)
                };
                return Ok(hit_item);
            }
            // We use the u64::MAX to get first to fill the cache, if version is newer,
            // use start_ts to get again. If the we use start_ts first to get the value,
            // we do not know if the value can be cached, because the value may be updated
            // after the start_ts.
            let mut item = snap.get(WRITE_CF, &raw_key, u64::MAX).await;
            if item.version > start_ts {
                item = snap.get(WRITE_CF, &raw_key, start_ts).await;
            } else if item.user_meta_len() > 0 && !has_lock {
                let um = UserMeta::from_slice(item.user_meta());
                let val = ValueCacheValue::new(
                    Bytes::copy_from_slice(item.get_value()),
                    um.start_ts,
                    item.version,
                    snap.get_mem_table_snap_version(),
                );
                cache.set(snap, &raw_key, val);
            }
            item
        } else {
            snap.get(WRITE_CF, &raw_key, start_ts).await
        };
        statistics.write.get += 1;
        statistics.write.flow_stats.read_keys += 1;
        statistics.write.flow_stats.read_bytes += user_key.len() + item.value_len();
        statistics.write.processed_keys += 1;
        statistics.processed_size += user_key.len() + item.value_len();
        Ok(item)
    }

    pub fn check_locks_in_range(&self, lower_bound: &[u8], upper_bound: &[u8]) -> Result<()> {
        let lower_bound = Some(Key::from_raw(lower_bound));
        let upper_bound = Some(Key::from_raw(upper_bound));
        let (lower_bound, upper_bound) = verify_range(&self.snapshot, lower_bound, upper_bound)?;
        let mut stats = Statistics::default();
        let mut iter = self
            .snapshot
            .new_iterator(LOCK_CF, false, false, None, self.fill_cache);
        if iter.set_range(lower_bound, upper_bound) {
            stats.lock.seek += 1;
        }
        check_locks(&mut iter, &self.bypass_locks, self.start_ts, &mut stats)?;
        Ok(())
    }

    pub fn new_memtable_iterator(&self) -> kvengine::read::Iterator {
        self.snapshot
            .new_memtable_iterator(WRITE_CF, false, false, Some(self.start_ts))
    }

    #[maybe_async::both]
    async fn scanner_inner(
        &self,
        desc: bool,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
        output_delete: bool,
    ) -> Result<CloudStoreScanner> {
        tikv_util::set_current_region(self.snapshot.get_id());
        CloudStoreScanner::new(
            self.snapshot.clone(),
            desc,
            self.fill_cache,
            self.bypass_locks.clone(),
            self.start_ts,
            lower_bound,
            upper_bound,
            output_delete,
        )
        .await
    }

    #[cfg(test)]
    pub fn get_start_ts(&self) -> u64 {
        self.start_ts
    }

    #[cfg(test)]
    pub fn get_by_pass_locks(&self) -> TsSet {
        self.bypass_locks.clone()
    }

    #[cfg(test)]
    pub fn is_fill_cache(&self) -> bool {
        self.fill_cache
    }
}

pub fn check_locks(
    lock_iter: &mut read::Iterator,
    bypass_locks: &TsSet,
    start_ts: u64,
    stats: &mut Statistics,
) -> mvcc::Result<()> {
    while lock_iter.valid() {
        let raw_key = lock_iter.key();
        let key = Key::from_raw(raw_key);
        let raw_key_len = raw_key.len();
        let val = lock_iter.val();
        stats.lock.next += 1;
        stats.lock.flow_stats.read_keys += 1;
        stats.lock.flow_stats.read_bytes += raw_key_len + val.len();
        stats.lock.processed_keys += 1;
        let lock = Lock::parse(val)?;
        Lock::check_ts_conflict(
            Cow::Borrowed(&lock),
            &key,
            start_ts.into(),
            bypass_locks,
            IsolationLevel::Si,
        )?;
        lock_iter.next();
    }
    Ok(())
}

fn verify_range(
    snap: &SnapAccess,
    lower_bound: Option<Key>,
    upper_bound: Option<Key>,
) -> Result<(Bytes, Bytes)> {
    let lower = if let Some(lower_key) = &lower_bound {
        let lower = Bytes::from(lower_key.to_raw()?);
        if lower.chunk() < snap.get_start_key() {
            range_error(snap, &lower_bound, &upper_bound)?;
        }
        lower
    } else {
        snap.get_start_key().to_vec().into()
    };
    let upper = if let Some(upper_key) = &upper_bound {
        let upper = Bytes::from(upper_key.to_raw()?);
        if upper.chunk() > snap.get_end_key() {
            range_error(snap, &lower_bound, &upper_bound)?;
        }
        upper
    } else {
        snap.get_end_key().to_vec().into()
    };
    Ok((lower, upper))
}

fn range_error(
    snap: &SnapAccess,
    lower_bound: &Option<Key>,
    upper_bound: &Option<Key>,
) -> Result<()> {
    REQUEST_EXCEED_BOUND.inc();
    let start = lower_bound.as_ref().map(|k| k.as_encoded().clone());
    let end = upper_bound.as_ref().map(|k| k.as_encoded().clone());
    let snap_start = Key::from_raw(snap.get_start_key());
    let snap_end = Key::from_raw(snap.get_end_key());
    Err(Error::from(ErrorInner::InvalidReqRange {
        start,
        end,
        lower_bound: Some(snap_start.into_encoded()),
        upper_bound: Some(snap_end.into_encoded()),
    }))
}

pub struct CloudStoreScanner {
    snap: SnapAccess,
    lock_iter: kvengine::read::Iterator,
    bypass_locks: TsSet,
    start_ts: u64,
    iter: kvengine::read::Iterator,
    stats: Statistics,
    is_started: bool,
    lower_bound: Bytes,
    upper_bound: Bytes,
    output_delete: bool,
}

impl CloudStoreScanner {
    #[maybe_async::both]
    pub async fn new(
        snap: SnapAccess,
        desc: bool,
        fill_cache: bool,
        bypass_locks: TsSet,
        start_ts: u64,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
        output_delete: bool,
    ) -> Result<Self> {
        let stats = Statistics::default();
        let lock_iter = snap.new_iterator(LOCK_CF, desc, false, None, fill_cache);
        let iter = snap
            .new_iterator(WRITE_CF, desc, false, Some(start_ts), fill_cache)
            .await;
        let (lower_bound, upper_bound) = verify_range(&snap, lower_bound, upper_bound)?;
        Ok(Self {
            snap,
            lock_iter,
            bypass_locks,
            start_ts,
            iter,
            stats,
            is_started: false,
            lower_bound,
            upper_bound,
            output_delete,
        })
    }

    #[maybe_async::both]
    async fn init(&mut self) -> Result<()> {
        if self
            .lock_iter
            .set_range(self.lower_bound.clone(), self.upper_bound.clone())
        {
            self.stats.lock.seek += 1;
        };
        check_locks(
            &mut self.lock_iter,
            &self.bypass_locks,
            self.start_ts,
            &mut self.stats,
        )?;
        if self
            .iter
            .set_range(self.lower_bound.clone(), self.upper_bound.clone())
            .await
        {
            self.stats.write.seek += 1;
        }
        Ok(())
    }

    #[maybe_async::both]
    async fn next_inner(&mut self) -> Result<Option<(Key, UserMeta, Value)>> {
        if self.is_started {
            self.iter.next().await;
        } else {
            self.init().await?;
            self.is_started = true;
        }
        loop {
            if !self.iter.valid() {
                return Ok(None);
            }
            let iter_key = self.iter.key();
            let key_len = iter_key.len();
            let key = Key::from_raw(iter_key);
            let user_meta = UserMeta::from_slice(self.iter.user_meta());
            let val = self.iter.val();
            self.stats.write.next += 1;
            self.stats.write.flow_stats.read_keys += 1;
            self.stats.write.flow_stats.read_bytes += key_len + val.len();
            self.stats.write.processed_keys += 1;
            self.stats.processed_size += key_len + val.len();
            if !val.is_empty() || self.output_delete {
                return Ok(Some((key, user_meta, val.to_vec())));
            }
            self.stats.write.next_tombstone += 1;
            // Skip delete record.
            self.iter.next().await;
            continue;
        }
    }

    pub fn next_with_user_meta(&mut self) -> Result<Option<(Key, UserMeta, Value)>> {
        self.next_inner()
    }
}

#[maybe_async::async_trait]
impl super::Scanner for CloudStoreScanner {
    #[maybe_async]
    async fn next(&mut self) -> Result<Option<(Key, Value)>> {
        Ok(self
            .next_inner()
            .await?
            .map(|(key, _user_meta, val)| (key, val)))
    }

    fn met_newer_ts_data(&self) -> NewerTsCheckState {
        NewerTsCheckState::Unknown
    }

    fn take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.stats)
    }

    fn reset_range(
        &mut self,
        desc: bool,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
    ) -> Result<bool> {
        if self.iter.is_reverse() != desc {
            return Ok(false);
        }
        let (lower_bound, upper_bound) = verify_range(&self.snap, lower_bound, upper_bound)?;
        self.lower_bound = lower_bound;
        self.upper_bound = upper_bound;
        self.is_started = false;
        Ok(true)
    }
}

impl super::TxnEntryScanner for CloudStoreScanner {
    fn next_entry(&mut self) -> Result<Option<TxnEntry>> {
        Ok(self.next_inner()?.map(|(key, user_meta, val)| {
            let write_key = key.append_ts(TimeStamp::new(user_meta.commit_ts));
            let write_type = if val.is_empty() {
                WriteType::Delete
            } else {
                WriteType::Put
            };
            let (write, default_key, default_val) = if is_short_value(&val) {
                (
                    Write::new(write_type, user_meta.start_ts.into(), Some(val)),
                    vec![],
                    vec![],
                )
            } else {
                let default_key = write_key
                    .clone()
                    .truncate_ts()
                    .unwrap()
                    .append_ts(user_meta.start_ts.into());
                (
                    Write::new(write_type, user_meta.start_ts.into(), None),
                    default_key.into_encoded(),
                    val,
                )
            };
            TxnEntry::Commit {
                default: (default_key, default_val),
                write: (write_key.into_encoded(), write.as_ref().to_bytes()),
                old_value: OldValue::None,
            }
        }))
    }

    fn take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.stats)
    }
}

impl<S: Snapshot> TxnEntryStore for CloudStore<S> {
    type Scanner = CloudStoreScanner;
    fn entry_scanner(
        &self,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
        _after_ts: TimeStamp,
        output_delete: bool,
    ) -> Result<Self::Scanner> {
        self.scanner_inner(false, lower_bound, upper_bound, output_delete)
    }
}
