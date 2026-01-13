// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
    thread,
    time::Duration,
};

use kvproto::kvrpcpb;
use rfstore::store::RegionIdVer;
use tikv::storage::mvcc::TimeStamp;
use tikv_util::{box_err, debug, info, time::Instant, warn};

use crate::client::{ClusterClient, Error, Result};

#[derive(Debug, Clone, Default)]
pub struct TxnStatus {
    pub ttl: u64,
    pub commit_ts: u64,
    pub action: kvrpcpb::Action,
    pub primary_lock: Option<kvrpcpb::LockInfo>,
}

impl TxnStatus {
    pub fn is_committed(&self) -> bool {
        self.ttl == 0 && self.commit_ts > 0
    }

    /// `is_cacheable` checks whether the transaction status is certain, and can
    /// be cached.
    ///
    /// If transaction is already committed, the result could be cached.
    /// Otherwise:
    ///   - If l.lock_type is pessimistic lock type:
    ///     - If its primary lock is pessimistic too, the check txn status
    ///       result should not be cached.
    ///     - If its primary lock is prewrite lock type, the check txn status
    ///       could be cached.
    ///   - If l.lock_type is prewrite lock type:
    ///     - Always cache the check txn status result.
    ///
    /// For prewrite locks, their primary keys should ALWAYS be the correct one
    /// and will NOT change.
    pub fn is_cacheable(&self) -> bool {
        if self.is_committed() {
            return true;
        }
        self.ttl == 0
            && matches!(
                self.action,
                kvrpcpb::Action::NoAction
                    | kvrpcpb::Action::LockNotExistRollback
                    | kvrpcpb::Action::TtlExpireRollback
            )
    }

    pub fn has_async_commit_lock(&self) -> bool {
        match self.primary_lock.as_ref() {
            Some(primary_lock) => primary_lock.use_async_commit,
            None => false,
        }
    }
}

pub struct ResolveLocksOptions {
    pub caller_start_ts: u64,
    pub locks: Vec<kvrpcpb::LockInfo>,
    // TODO: lite, for_read.
}

#[derive(Default)]
pub struct ResolveLockResult {
    /// Minimum duration until locks expire.
    until_expire_ms: Option<i64>,
    // TODO: ignore_locks, access_locks.
}

impl ResolveLockResult {
    pub fn update(&mut self, mut until_lock_expire_ms: i64) {
        if until_lock_expire_ms < 0 {
            until_lock_expire_ms = 0;
        }
        if until_lock_expire_ms < self.until_expire_ms.unwrap_or(i64::MAX) {
            self.until_expire_ms = Some(until_lock_expire_ms);
        }
    }

    pub fn until_expire_ms(&self) -> u64 {
        self.until_expire_ms.unwrap_or(0) as u64
    }
}

pub struct LockResolver {
    cluster_client: ClusterClient,
    resolved: Mutex<HashMap<u64 /* txn_id */, TxnStatus>>,
}

/// `CleanTxnsMap` records resolved regions.
type CleanTxnsMap = HashMap<u64 /* txn_id */, HashSet<RegionIdVer>>;

impl LockResolver {
    pub fn new(cluster_client: ClusterClient) -> Self {
        Self {
            cluster_client,
            resolved: Default::default(),
        }
    }

    pub fn resolve_locks(&mut self, opts: ResolveLocksOptions) -> Result<ResolveLockResult> {
        let mut res = ResolveLockResult::default();
        // TODO: use a LRU cache and put it in LockResolver to share with all txns.
        let mut clean_txns: CleanTxnsMap = CleanTxnsMap::new();

        // TODO: optimize for read.
        for lock in opts.locks {
            let status = self.resolve_single_lock(&lock, opts.caller_start_ts, &mut clean_txns)?;
            info!("resolve_single_lock: lock {:?}, status {:?}", lock, status);
            // `status.ttl != 0` means the lock is not resolved.
            if status.ttl != 0 {
                let until_expire = self.lock_until_expired(lock.lock_version, status.ttl);
                res.update(until_expire);
            }
        }
        Ok(res)
    }

    fn resolve_single_lock(
        &mut self,
        lock: &kvrpcpb::LockInfo,
        caller_start_ts: u64,
        // TODO: force_sync_commit: bool,
        clean_txns: &mut CleanTxnsMap,
    ) -> Result<TxnStatus> {
        let status = self.get_txn_status_from_lock(lock, caller_start_ts)?;

        // TODO: handle primary mismatch error.

        if status.ttl != 0 {
            return Ok(status);
        }

        // `ttl == 0` means lock is committed or rollbacked, do resolve lock.
        let clean_regions = clean_txns.entry(lock.lock_version).or_default();
        if status.has_async_commit_lock() {
            // TODO: resolve async commit lock
            warn!("resolve async commit lock not implemented");
            return Ok(status);
        }
        if lock.lock_type == kvrpcpb::Op::PessimisticLock {
            // TODO: resolve pessimistic lock
            panic!("resolve pessimistic lock not implemented");
        } else {
            // TODO: optimize for read
            // TODO: support lite resolve lock on keys.
            let commit_version = status.is_committed().then_some(status.commit_ts);
            self.cluster_client.kv_resolve_lock(
                lock.lock_version,
                commit_version,
                lock.key.clone(),
                lock.is_txn_file,
                clean_regions,
            )?;
            Ok(status)
        }
    }

    fn get_txn_status_from_lock(
        &mut self,
        lock: &kvrpcpb::LockInfo,
        caller_start_ts: u64,
        // TODO: force_sync_commit: bool,
    ) -> Result<TxnStatus> {
        let current_ts = if lock.lock_ttl == 0 {
            // NOTE: lock.lock_ttl == 0 is a special protocol!!!
            // When the pessimistic txn prewrite meets locks of a txn, it should resolve the
            // lock **unconditionally**. In this case, TiKV use lock.lock_ttl = 0 to
            // notify that we should resolve the lock! Set current_ts to
            // max uint64 to make the lock expired.
            u64::MAX
        } else {
            self.cluster_client.get_ts().into_inner()
        };

        let mut rollback_if_not_exist = false;
        let start = Instant::now();
        let timeout = Duration::from_secs(5);
        let mut last_err = None;
        while start.saturating_elapsed() < timeout {
            match self.get_txn_status(
                lock.lock_version,
                &lock.primary_lock,
                caller_start_ts,
                current_ts,
                rollback_if_not_exist,
                Some(lock),
            ) {
                Ok(status) => return Ok(status),
                Err(err @ Error::TxnNotFound(_)) => {
                    last_err = Some(err);
                    if self.lock_is_expired(lock.lock_version, lock.lock_ttl) {
                        // For pessimistic lock resolving, if the primary lock does not exist and
                        // rollback_if_not_exist is true, the Action::LockNotExistDoNothing will be
                        // returned as the status.
                        rollback_if_not_exist = true;
                        continue;
                    } else {
                        // For the Rollback statement from user, the pessimistic locks will be
                        // rollbacked and the primary key in store has no related information. There
                        // are possibilities that some other transactions do check_txn_status on
                        // these locks and they will be blocked by waiting ttl time, so let the
                        // transaction retries to do pessimistic lock if txn
                        // not found and the lock does not expire yet.
                        if lock.lock_type == kvrpcpb::Op::PessimisticLock {
                            return Ok(TxnStatus {
                                ttl: lock.lock_ttl,
                                ..Default::default()
                            });
                        }
                    }

                    // Handle TxnNotFound error.
                    // get_txn_status() returns it when the secondary locks exist while the primary
                    // lock doesn't. This is likely to happen in the concurrently prewrite when
                    // secondary regions success before the primary region.
                    // Backoff and retry.
                    // TODO: use a backoff object through the whole process.
                    thread::sleep(Duration::from_millis(100));
                }
                Err(err) => return Err(err),
            }
        }
        panic!(
            "{} get_txn_status_from_lock failed, lock {:?}, error {:?}",
            lock.lock_version,
            lock,
            last_err.unwrap()
        );
    }

    /// `get_txn_status` sends the CheckTxnStatus request to the TiKV server.
    /// When rollback_if_not_exist is false, the caller should be careful with
    /// the TxnNotFound error.
    fn get_txn_status(
        &mut self,
        txn_id: u64,
        primary: &[u8],
        caller_start_ts: u64,
        current_ts: u64,
        rollback_if_not_exist: bool,
        // TODO: force_sync_commit: bool,
        lock: Option<&kvrpcpb::LockInfo>,
    ) -> Result<TxnStatus> {
        if let Some(txn_status) = self.get_resolved_txn(txn_id) {
            return Ok(txn_status);
        }

        // CheckTxnStatus may meet the following cases:
        // 1. LOCK
        // 1.1 Lock expired -- orphan lock, fail to update TTL, crash recovery etc.
        // 1.2 Lock TTL -- active transaction holding the lock.
        // 2. NO LOCK
        // 2.1 Txn Committed
        // 2.2 Txn Rollbacked -- rollback itself, rollback by others, GC tomb etc.
        // 2.3 No lock -- pessimistic lock rollback, concurrence prewrite.
        let resolving_pessimistic_lock =
            lock.is_some_and(|l| l.lock_type == kvrpcpb::Op::PessimisticLock);
        let is_txn_file = lock.is_some_and(|l| l.is_txn_file);
        let mut resp = self.cluster_client.kv_check_txn_status(
            primary,
            txn_id.into(),
            caller_start_ts.into(),
            current_ts.into(),
            rollback_if_not_exist,
            false,
            resolving_pessimistic_lock,
            is_txn_file,
        )?;
        if resp.has_error() {
            let mut key_err = resp.take_error();
            if key_err.has_txn_not_found() {
                return Err(Error::TxnNotFound(key_err.take_txn_not_found()));
            }

            // TODO: Handle primary mismatch error.
            // Current version of kvproto don't have this kind of error.

            return Err(box_err!(
                "txn {} get_txn_status error: {:?}",
                txn_id,
                key_err
            ));
        }

        let mut status = TxnStatus {
            action: resp.action,
            primary_lock: resp.has_lock_info().then_some(resp.take_lock_info()),
            ..Default::default()
        };
        if status.has_async_commit_lock() {
            if !self.lock_is_expired(txn_id, resp.lock_ttl) {
                status.ttl = resp.lock_ttl;
            }
        } else if resp.lock_ttl != 0 {
            status.ttl = resp.lock_ttl
        } else {
            status.commit_ts = resp.commit_version;
            if status.is_cacheable() {
                self.save_resolved_txn(txn_id, status.clone());
            }
        }
        debug!("txn {} get_txn_status: {:?}", txn_id, status);
        Ok(status)
    }

    fn lock_is_expired(&self, lock_ts: u64, ttl: u64) -> bool {
        let last_ts = self.cluster_client.get_ts();
        let lock_ts = TimeStamp::from(lock_ts);
        last_ts.physical() >= lock_ts.physical() + ttl
    }

    // Milliseconds until lock expired.
    // <= 0 when lock has expired.
    fn lock_until_expired(&self, lock_ts: u64, ttl: u64) -> i64 {
        let last_ts = self.cluster_client.get_ts();
        let lock_ts = TimeStamp::from(lock_ts);
        lock_ts.physical() as i64 + ttl as i64 - last_ts.physical() as i64
    }

    fn get_resolved_txn(&self, txn_id: u64) -> Option<TxnStatus> {
        self.resolved.lock().unwrap().get(&txn_id).cloned()
    }

    fn save_resolved_txn(&mut self, txn_id: u64, txn_status: TxnStatus) {
        self.resolved.lock().unwrap().insert(txn_id, txn_status);
    }
}
