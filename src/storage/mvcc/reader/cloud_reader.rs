// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::Bytes;
use kvengine::{EXTRA_CF, LOCK_CF, UserMeta, WRITE_CF};
use txn_types::{Key, Lock, OldValue, TimeStamp, Value, Write, WriteType};

use crate::storage::mvcc::{Result, TxnCommitRecord};

pub struct CloudReader {
    snapshot: kvengine::SnapAccess,
    fill_cache: bool,
    pub statistics: tikv_kv::Statistics,
}

impl CloudReader {
    pub fn new(snapshot: kvengine::SnapAccess, fill_cache: bool) -> Self {
        Self {
            snapshot,
            fill_cache,
            statistics: tikv_kv::Statistics::default(),
        }
    }

    fn get_commit_by_item(
        user_meta: &UserMeta,
        value: &[u8],
        start_ts: TimeStamp,
    ) -> Option<TxnCommitRecord> {
        if user_meta.start_ts == start_ts.into_inner() {
            let (write_type, short_value) = if value.is_empty() {
                (WriteType::Delete, None)
            } else {
                (WriteType::Put, Some(value.to_vec()))
            };
            let write = Write::new(write_type, TimeStamp::new(user_meta.start_ts), short_value);
            return Some(TxnCommitRecord::SingleRecord {
                commit_ts: TimeStamp::new(user_meta.commit_ts),
                write,
            });
        }
        None
    }

    /// Note: This method is also used by resolving locks during restoring
    /// keyspace.
    #[maybe_async::both]
    pub async fn get_txn_commit_record(
        &mut self,
        key: &Key,
        start_ts: TimeStamp,
    ) -> Result<TxnCommitRecord> {
        let raw_key = key.to_raw()?;
        let item = self.snapshot.get(WRITE_CF, &raw_key, 0).await;
        if item.user_meta_len() > 0 {
            let user_meta = UserMeta::from_slice(item.user_meta());
            if let Some(record) = Self::get_commit_by_item(&user_meta, item.get_value(), start_ts) {
                return Ok(record);
            }
        }
        let mut data_iter = self
            .snapshot
            .new_iterator(WRITE_CF, false, true, None, self.fill_cache)
            .await;
        let mut next_key = Vec::with_capacity(raw_key.len() + 1);
        next_key.extend_from_slice(&raw_key);
        next_key.push(0);
        data_iter
            .set_range(Bytes::from(raw_key), Bytes::from(next_key))
            .await;
        while data_iter.valid() {
            debug_assert!(!kvengine::table::is_deleted(data_iter.meta()));
            // TODO: remove this check, iterator should not return deleted records.
            if kvengine::table::is_deleted(data_iter.meta()) {
                break;
            }

            let user_meta = UserMeta::from_slice(data_iter.user_meta());
            if user_meta.commit_ts < start_ts.into_inner() {
                // A transaction's commit_ts must be greater than start_ts, if current commit_ts
                // is already smaller than the start_ts, we don't need to look for older
                // version.
                break;
            }
            if let Some(record) = Self::get_commit_by_item(&user_meta, data_iter.val(), start_ts) {
                return Ok(record);
            }
            data_iter.next().await;
        }
        match self.get_extra(key, start_ts) {
            Some((commit_ts, write)) => Ok(TxnCommitRecord::SingleRecord { commit_ts, write }),
            None => Ok(TxnCommitRecord::None {
                overlapped_write: None,
            }),
        }
    }

    pub fn get_extra(&mut self, key: &Key, start_ts: TimeStamp) -> Option<(TimeStamp, Write)> {
        let raw_key = key.to_raw().unwrap();
        let extra_key = kvengine::encode_extra_txn_status_key(&raw_key, start_ts.into_inner());
        let item = self.snapshot.get(EXTRA_CF, &extra_key, 0);
        if item.user_meta_len() == 0 {
            return None;
        }
        let user_meta = UserMeta::from_slice(item.user_meta());
        Some(if user_meta.commit_ts == 0 {
            (start_ts, Write::new(WriteType::Rollback, start_ts, None))
        } else {
            (
                user_meta.commit_ts.into(),
                Write::new(WriteType::Lock, start_ts, None),
            )
        })
    }

    pub fn load_lock(&mut self, key: &Key) -> Result<Option<Lock>> {
        let raw_key = key.to_raw().unwrap();
        let item = self.snapshot.get(LOCK_CF, &raw_key, 0);
        self.statistics.lock.get += 1;
        self.statistics.lock.flow_stats.read_keys += 1;
        self.statistics.lock.flow_stats.read_bytes += item.value_len();
        self.statistics.lock.processed_keys += 1;
        if item.value_len() == 0 {
            return Ok(None);
        }
        let lock = Lock::parse(item.get_value())?;
        Ok(Some(lock))
    }

    #[maybe_async::both]
    pub async fn get(
        &mut self,
        key: &Key,
        ts: TimeStamp,
        _gc_fence_limit: Option<TimeStamp>,
    ) -> Result<Option<Value>> {
        let raw_key = key.to_raw()?;
        let item = self.snapshot.get(WRITE_CF, &raw_key, ts.into_inner()).await;
        self.statistics.write.get += 1;
        self.statistics.write.flow_stats.read_bytes += raw_key.len() + item.value_len();
        self.statistics.write.flow_stats.read_keys += 1;
        self.statistics.write.processed_keys += 1;
        self.statistics.processed_size += raw_key.len() + item.value_len();
        if item.value_len() > 0 {
            return Ok(Some(item.get_value().to_vec()));
        }
        Ok(None)
    }

    #[maybe_async::both]
    pub async fn get_write(
        &mut self,
        key: &Key,
        ts: TimeStamp,
        _gc_fence_limit: Option<TimeStamp>,
    ) -> Result<Option<Write>> {
        self.seek_write(key, ts)
            .await
            .map(|opt| opt.map(|(_, write)| write))
    }

    #[maybe_async::both]
    pub async fn seek_write(
        &mut self,
        key: &Key,
        ts: TimeStamp,
    ) -> Result<Option<(TimeStamp, Write)>> {
        let raw_key = key.to_raw()?;
        let item = self.snapshot.get(WRITE_CF, &raw_key, ts.into_inner()).await;
        self.statistics.write.seek += 1;
        self.statistics.write.flow_stats.read_keys += 1;
        self.statistics.write.flow_stats.read_bytes += raw_key.len() + item.value_len();
        self.statistics.write.processed_keys += 1;
        self.statistics.processed_size += raw_key.len() + item.value_len();
        if item.user_meta_len() > 0 {
            let user_meta = UserMeta::from_slice(item.user_meta());
            let (commit_ts, write) = parse_write(&user_meta, item.get_value());
            return Ok(Some((commit_ts, write)));
        }
        Ok(None)
    }

    #[maybe_async::both]
    #[inline(always)]
    pub async fn get_old_value(
        &mut self,
        key: &Key,
        start_ts: TimeStamp,
        prev_write: Option<Write>,
    ) -> Result<OldValue> {
        if let Some(write) = prev_write {
            if write.write_type == WriteType::Delete {
                return Ok(OldValue::None);
            }
            // Locks and Rolbacks are stored in extra CF, will not be seeked by seek_write.
            assert_eq!(write.write_type, WriteType::Put);
            return Ok(OldValue::value(write.short_value.unwrap()));
        }
        let raw_key = key.to_raw()?;
        let item = self
            .snapshot
            .get(WRITE_CF, &raw_key, start_ts.into_inner())
            .await;
        if item.value_len() > 0 {
            return Ok(OldValue::value(item.get_value().to_vec()));
        }
        Ok(OldValue::None)
    }

    /// Scan locks that satisfies `filter(lock)` returns true, from the given
    /// start key `start`. At most `limit` locks will be returned. If
    /// `limit` is set to `0`, it means unlimited.
    ///
    /// The return type is `(locks, is_remain)`. `is_remain` indicates whether
    /// there MAY be remaining locks that can be scanned.
    pub fn scan_locks<F>(
        &mut self,
        start: Option<&Key>,
        end: Option<&Key>,
        filter: F,
        limit: usize,
    ) -> Result<(Vec<(Key, Lock)>, bool)>
    where
        F: Fn(&Lock) -> bool,
    {
        let mut locks = vec![];
        let mut lock_iter =
            self.snapshot
                .new_iterator(LOCK_CF, false, false, None, self.fill_cache);
        let lower_bound: Bytes = if let Some(start) = start {
            Bytes::from(start.to_raw()?)
        } else {
            Bytes::copy_from_slice(self.snapshot.get_start_key())
        };
        let upper_bound = if let Some(k) = end {
            Bytes::from(k.to_raw()?)
        } else {
            self.snapshot.clone_end_key()
        };
        if lock_iter.set_range(lower_bound, upper_bound) {
            self.statistics.lock.seek += 1;
        }
        while lock_iter.valid() {
            let key = Key::from_raw(lock_iter.key());
            if let Some(end) = end {
                if key >= *end {
                    return Ok((locks, false));
                }
            }
            let val = lock_iter.val();
            self.statistics.lock.next += 1;
            self.statistics.lock.flow_stats.read_keys += 1;
            self.statistics.lock.flow_stats.read_bytes += val.len();
            self.statistics.lock.processed_keys += 1;
            let lock = Lock::parse(val)?;
            if filter(&lock) {
                locks.push((key, lock));
                if limit > 0 && locks.len() == limit {
                    return Ok((locks, true));
                }
            }
            lock_iter.next();
        }
        Ok((locks, false))
    }

    /// Returns an arbitrary write that is newer than `ts`, or None if no such
    /// write exists.
    #[maybe_async::both]
    pub async fn get_newer(
        &mut self,
        key: &Key,
        ts: TimeStamp,
    ) -> Result<Option<(TimeStamp, Write)>> {
        let raw_key = key.to_raw()?;
        let item = self
            .snapshot
            .get_newer(WRITE_CF, &raw_key, ts.into_inner())
            .await;
        if item.user_meta_len() > 0 {
            let user_meta = UserMeta::from_slice(item.user_meta());
            return Ok(Some(parse_write(&user_meta, item.get_value())));
        }
        Ok(None)
    }

    /// Return the first committed key for which `start_ts` equals to `ts`
    // TODO: support async.
    pub fn seek_ts(&mut self, ts: TimeStamp) -> Result<Option<Key>> {
        let mut it = self
            .snapshot
            .new_iterator(WRITE_CF, false, true, None, self.fill_cache);
        it.rewind();
        while it.valid() {
            debug_assert!(!kvengine::table::is_deleted(it.meta()));
            // TODO: remove this check, iterator should not return deleted records.
            if kvengine::table::is_deleted(it.meta()) {
                it.next();
                continue;
            }
            let user_meta = UserMeta::from_slice(it.user_meta());
            if user_meta.start_ts == ts.into_inner() {
                return Ok(Some(Key::from_raw(it.key())));
            }
            it.next()
        }
        Ok(None)
    }

    pub fn get_extras(&mut self, key: &Key) -> Vec<(TimeStamp, Write)> {
        let raw_key = key.to_raw().unwrap();
        let mut writes = vec![];
        let mut extra_iter = self
            .snapshot
            .new_iterator(EXTRA_CF, false, false, None, true);
        extra_iter.seek(&raw_key);
        while extra_iter.valid() {
            if !extra_iter.key().starts_with(&raw_key) {
                break;
            }
            if extra_iter.key().len() == raw_key.len() + 8 {
                let um = UserMeta::from_slice(extra_iter.user_meta());
                let (ts, write_type) = if um.commit_ts == 0 {
                    (um.start_ts, WriteType::Rollback)
                } else {
                    (um.commit_ts, WriteType::Lock)
                };
                writes.push((ts.into(), Write::new(write_type, um.start_ts.into(), None)));
            }
            extra_iter.next();
        }
        writes
    }
}

pub fn parse_write(user_meta: &UserMeta, value: &[u8]) -> (TimeStamp, Write) {
    let commit_ts = user_meta.commit_ts;
    let write_type: WriteType;
    let short_value: Option<Value>;
    if value.is_empty() {
        write_type = WriteType::Delete;
        short_value = None;
    } else {
        write_type = WriteType::Put;
        short_value = Some(value.to_vec())
    }
    (
        TimeStamp::new(commit_ts),
        Write::new(write_type, TimeStamp::new(user_meta.start_ts), short_value),
    )
}
