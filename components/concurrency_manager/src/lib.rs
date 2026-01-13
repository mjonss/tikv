// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! The concurrency manager is responsible for concurrency control of
//! transactions.
//!
//! The concurrency manager contains a lock table in memory. Lock information
//! can be stored in it and reading requests can check if these locks block
//! the read.
//!
//! In order to mutate the lock of a key stored in the lock table, it needs
//! to be locked first using `lock_key` or `lock_keys`.

use fail::fail_point;

mod key_handle;
mod lock_table;

use std::{
    mem::MaybeUninit,
    ops::DerefMut,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use parking_lot::{Mutex, RwLock};
use tikv_util::{retry::sleep_async, time::Instant, warn};
use txn_types::{Key, Lock, TimeStamp};

pub use self::{
    key_handle::{KeyHandle, KeyHandleGuard},
    lock_table::LockTable,
};

// Pay attention that the async functions of ConcurrencyManager should not hold
// the mutex.
#[derive(Clone)]
pub struct ConcurrencyManager {
    max_ts: Arc<AtomicU64>,
    lock_table: LockTable,

    need_check_backup_ts: bool,
    latest_backup_ts: Arc<RwLock<TrackedBackupTs>>,
    old_backup_ts_set: TrackedBackupTsSet,
}

impl ConcurrencyManager {
    pub fn new_opt(latest_ts: TimeStamp, need_check_backup_ts: bool) -> Self {
        ConcurrencyManager {
            max_ts: Arc::new(AtomicU64::new(latest_ts.into_inner())),
            lock_table: LockTable::default(),
            need_check_backup_ts,
            latest_backup_ts: Arc::new(RwLock::new(TrackedBackupTs::new(latest_ts))),
            old_backup_ts_set: TrackedBackupTsSet::default(),
        }
    }

    pub fn new(latest_ts: TimeStamp) -> Self {
        Self::new_opt(latest_ts, false)
    }

    pub fn max_ts(&self) -> TimeStamp {
        TimeStamp::new(self.max_ts.load(Ordering::SeqCst))
    }

    /// Updates max_ts with the given new_ts. It has no effect if
    /// max_ts >= new_ts or new_ts is TimeStamp::max().
    pub fn update_max_ts(&self, new_ts: TimeStamp) {
        fail_point!("cm_before_update_max_ts", |_| {});

        if new_ts != TimeStamp::max() {
            self.max_ts.fetch_max(new_ts.into_inner(), Ordering::SeqCst);
        }
    }

    /// Acquires a mutex of the key and returns an RAII guard. When the guard
    /// goes out of scope, the mutex will be unlocked.
    ///
    /// The guard can be used to store Lock in the table. The stored lock
    /// is visible to `read_key_check` and `read_range_check`.
    pub async fn lock_key(&self, key: &Key) -> KeyHandleGuard {
        self.lock_table.lock_key(key).await
    }

    /// Acquires mutexes of the keys and returns the RAII guards. The order of
    /// the guards is the same with the given keys.
    ///
    /// The guards can be used to store Lock in the table. The stored lock
    /// is visible to `read_key_check` and `read_range_check`.
    pub async fn lock_keys(&self, keys: impl Iterator<Item = &Key>) -> Vec<KeyHandleGuard> {
        let mut keys_with_index: Vec<_> = keys.enumerate().collect();
        // To prevent deadlock, we sort the keys and lock them one by one.
        keys_with_index.sort_by_key(|(_, key)| *key);
        let mut result: Vec<MaybeUninit<KeyHandleGuard>> = Vec::new();
        result.resize_with(keys_with_index.len(), MaybeUninit::uninit);
        for (index, key) in keys_with_index {
            result[index] = MaybeUninit::new(self.lock_table.lock_key(key).await);
        }
        unsafe { tikv_util::memory::vec_transmute(result) }
    }

    /// Checks if there is a memory lock of the key which blocks the read.
    /// The given `check_fn` should return false iff the lock passed in
    /// blocks the read.
    pub fn read_key_check<E>(
        &self,
        key: &Key,
        check_fn: impl FnOnce(&Lock) -> Result<(), E>,
    ) -> Result<(), E> {
        let res = self.lock_table.check_key(key, check_fn);
        fail_point!("cm_after_read_key_check");
        res
    }

    /// Checks if there is a memory lock in the range which blocks the read.
    /// The given `check_fn` should return false iff the lock passed in
    /// blocks the read.
    pub fn read_range_check<E>(
        &self,
        start_key: Option<&Key>,
        end_key: Option<&Key>,
        check_fn: impl FnMut(&Key, &Lock) -> Result<(), E>,
    ) -> Result<(), E> {
        let res = self.lock_table.check_range(start_key, end_key, check_fn);
        fail_point!("cm_after_read_range_check");
        res
    }

    /// Find the minimum start_ts among all locks in memory.
    pub fn global_min_lock_ts(&self) -> Option<TimeStamp> {
        let mut min_lock_ts = None;
        // TODO: The iteration looks not so efficient. It's better to be optimized.
        self.lock_table.for_each(|handle| {
            if let Some(curr_ts) = handle.with_lock(|lock| lock.as_ref().map(|l| l.ts)) {
                if min_lock_ts.map(|ts| ts > curr_ts).unwrap_or(true) {
                    min_lock_ts = Some(curr_ts);
                }
            }
        });
        min_lock_ts
    }

    pub fn get_latest_backup_ts(&self) -> Option<TrackedBackupTs> {
        if self.need_check_backup_ts {
            Some(self.latest_backup_ts.read().clone())
        } else {
            None
        }
    }

    pub fn replace_backup_ts(&self, backup_ts: TimeStamp) -> bool /* replaced */ {
        // Hold the write lock when insert into `backup_ts_set`.
        // Otherwise, a concurrent request will miss to wait for the old backup_ts.
        // Be caution for deadlock. There is another lock in
        // `TrackedBackupTsSet::insert()`.
        let mut curr = self.latest_backup_ts.write();
        if curr.get() < backup_ts {
            let old = std::mem::replace(curr.deref_mut(), TrackedBackupTs::new(backup_ts));
            self.old_backup_ts_set.insert(old);
            true
        } else {
            false
        }
    }

    pub async fn wait_old_backup_ts_released(
        &self,
        current_backup_ts: TimeStamp,
        wait_timeout: Duration,
        ttl: Duration,
        retry_interval: Duration,
    ) -> bool /* ok */ {
        self.old_backup_ts_set
            .wait_released(current_backup_ts, wait_timeout, ttl, retry_interval)
            .await
    }
}

/// The backup timestamp using reference counting to track the transactions
/// which has checked `commit_ts` with it but not applied yet.
#[derive(Clone)]
pub struct TrackedBackupTs(Arc<AtomicU64>);

impl TrackedBackupTs {
    pub fn new(ts: TimeStamp) -> Self {
        TrackedBackupTs(Arc::new(AtomicU64::new(ts.into_inner())))
    }

    pub fn get(&self) -> TimeStamp {
        TimeStamp::new(self.0.load(Ordering::SeqCst))
    }

    pub fn external_ref_count(&self) -> usize {
        Arc::strong_count(&self.0).saturating_sub(1)
    }

    pub fn elapsed_secs(&self, current_ts: TimeStamp) -> u64 {
        current_ts.physical().saturating_sub(self.get().physical()) / 1000
    }
}

#[derive(Clone, Default)]
struct TrackedBackupTsSet {
    inner: Arc<Mutex<Vec<TrackedBackupTs>>>,
}

impl TrackedBackupTsSet {
    fn insert(&self, backup_ts: TrackedBackupTs) {
        self.inner.lock().push(backup_ts);
    }

    /// Return false when timeout.
    async fn wait_released(
        &self,
        current_backup_ts: TimeStamp,
        wait_timeout: Duration,
        ttl: Duration,
        retry_interval: Duration,
    ) -> bool /* ok */ {
        let ttl_secs = ttl.as_secs();

        let mut no_dropped = true;
        let start_time = Instant::now_coarse();
        loop {
            {
                let mut set = self.inner.lock();

                // Transactions tracked on larger `backup_ts` must have larger `commit_ts` as
                // well.
                let Some(pos) = set.iter().position(|x| x.get() < current_backup_ts) else {
                    return no_dropped;
                };

                let ref_count = set[pos].external_ref_count();
                if ref_count == 0 {
                    set.remove(pos);
                } else if set[pos].elapsed_secs(current_backup_ts) >= ttl_secs {
                    warn!(
                        "backup_ts is dropped: {}: ref count: {}",
                        set[pos].get(),
                        ref_count
                    );
                    set.remove(pos);
                    no_dropped = false;
                } else if start_time.saturating_elapsed() > wait_timeout {
                    warn!(
                        "backup_ts: wait_released timeout: {}: ref count: {}",
                        set[pos].get(),
                        ref_count
                    );
                    return false; // timeout
                }

                if set.is_empty() {
                    return no_dropped;
                }
            }

            sleep_async(retry_interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use txn_types::LockType;

    use super::*;

    #[tokio::test]
    async fn test_lock_keys_order() {
        let concurrency_manager = ConcurrencyManager::new(1.into());
        let keys: Vec<_> = [b"c", b"a", b"b"]
            .iter()
            .copied()
            .map(|k| Key::from_raw(k))
            .collect();
        let guards = concurrency_manager.lock_keys(keys.iter()).await;
        for (key, guard) in keys.iter().zip(&guards) {
            assert_eq!(key, guard.key());
        }
    }

    #[tokio::test]
    async fn test_update_max_ts() {
        let concurrency_manager = ConcurrencyManager::new(10.into());
        concurrency_manager.update_max_ts(20.into());
        assert_eq!(concurrency_manager.max_ts(), 20.into());

        concurrency_manager.update_max_ts(5.into());
        assert_eq!(concurrency_manager.max_ts(), 20.into());

        concurrency_manager.update_max_ts(TimeStamp::max());
        assert_eq!(concurrency_manager.max_ts(), 20.into());
    }

    fn new_lock(ts: impl Into<TimeStamp>, primary: &[u8], lock_type: LockType) -> Lock {
        let ts = ts.into();
        Lock::new(lock_type, primary.to_vec(), ts, 0, None, 0.into(), 1, ts)
    }

    #[tokio::test]
    async fn test_global_min_lock_ts() {
        let concurrency_manager = ConcurrencyManager::new(1.into());

        assert_eq!(concurrency_manager.global_min_lock_ts(), None);
        let guard = concurrency_manager.lock_key(&Key::from_raw(b"a")).await;
        assert_eq!(concurrency_manager.global_min_lock_ts(), None);
        guard.with_lock(|l| *l = Some(new_lock(10, b"a", LockType::Put)));
        assert_eq!(concurrency_manager.global_min_lock_ts(), Some(10.into()));
        drop(guard);
        assert_eq!(concurrency_manager.global_min_lock_ts(), None);

        let ts_seqs = vec![
            vec![20, 30, 40],
            vec![40, 30, 20],
            vec![20, 40, 30],
            vec![30, 20, 40],
        ];
        let keys: Vec<_> = [b"a", b"b", b"c"]
            .iter()
            .copied()
            .map(|k| Key::from_raw(k))
            .collect();

        for ts_seq in ts_seqs {
            let guards = concurrency_manager.lock_keys(keys.iter()).await;
            assert_eq!(concurrency_manager.global_min_lock_ts(), None);
            for (ts, guard) in ts_seq.into_iter().zip(guards.iter()) {
                guard.with_lock(|l| *l = Some(new_lock(ts, b"pk", LockType::Put)));
            }
            assert_eq!(concurrency_manager.global_min_lock_ts(), Some(20.into()));
        }
    }

    #[test]
    fn test_tracked_backup_ts() {
        let cm = ConcurrencyManager::new_opt(10.into(), true);
        assert!(!cm.replace_backup_ts(1.into()));
        assert!(cm.replace_backup_ts(20.into()));
    }

    #[tokio::test]
    async fn test_tracked_backup_ts_set() {
        let retry_interval = Duration::from_millis(100);
        let dur100ms = Duration::from_millis(100);
        let dur500ms = Duration::from_millis(500);
        let dur1s = Duration::from_secs(1);

        let set = TrackedBackupTsSet::default();

        let ts10 = TrackedBackupTs::new(make_ts(10));
        set.insert(ts10.clone());

        assert!(
            set.wait_released(make_ts(1), dur500ms, Duration::MAX, retry_interval)
                .await
        );

        let ts20 = make_ts(20);
        assert!(
            !set.wait_released(ts20, dur500ms, Duration::MAX, retry_interval)
                .await
        );
        drop(ts10);
        assert!(
            set.wait_released(ts20, dur500ms, Duration::MAX, retry_interval)
                .await
        );

        let bts20 = TrackedBackupTs::new(ts20);
        set.insert(bts20.clone());
        let ts30 = make_ts(30);
        assert!(
            !set.wait_released(ts30, dur500ms, Duration::MAX, retry_interval)
                .await
        );
        // TTL not expire.
        assert!(
            !set.wait_released(ts30, dur500ms, Duration::from_secs(11), retry_interval)
                .await
        );
        // TTL expired.
        assert!(
            !set.wait_released(ts30, Duration::MAX, Duration::from_secs(10), retry_interval)
                .await
        );
        // Should be dropped.
        assert!(
            set.wait_released(ts30, Duration::MAX, Duration::MAX, retry_interval)
                .await
        );
        assert_eq!(Arc::strong_count(&bts20.0), 1);

        // Test parallel.
        let bts30 = TrackedBackupTs::new(ts30);
        set.insert(bts30.clone());
        let ts50 = make_ts(50);
        assert!(
            !set.wait_released(ts50, dur500ms, Duration::MAX, retry_interval)
                .await
        );
        let mut js = tokio::task::JoinSet::new();
        for _ in 0..5 {
            let set = set.clone();
            js.spawn(async move {
                set.wait_released(ts50, dur1s, Duration::MAX, retry_interval)
                    .await
            });
        }
        tokio::time::sleep(dur100ms).await;
        drop(bts30);
        while let Some(res) = js.join_next().await {
            assert!(res.unwrap());
        }
    }

    fn make_ts(secs: u64) -> TimeStamp {
        TimeStamp::compose(secs * 1000, 0)
    }
}
