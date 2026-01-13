// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, hash_map::Entry::Vacant},
    default::Default,
    sync::Arc,
};

use cloud_server::modifies_to_requests;
use dashmap::DashMap;
use kvengine::{Shard, UserMeta};
use kvproto::kvrpcpb;
use tikv::storage::{
    kv::{TxnFileWriteData, WriteData},
    mvcc::{CloudReader, Key, MvccTxn, TxnCommitRecord, WriteType},
};
use tikv_util::{box_err, debug, info, warn};
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};
use txn_types::{Lock, LockType, ReqType, TimeStamp};

use crate::{
    Result,
    common::{RawRegion, RegionMetaGetter},
    error::Error,
};

// Limit the batch size to avoid exceed the size of mem tables.
// At the same time, the batch size should not be too small to reduce the
// overhead of creating applier.
const DEFAULT_TXN_BATCH_SIZE: usize = 32 * 1024 * 1024; // 32MB

#[derive(Clone)]
pub struct LockResolver {
    tag: Arc<String>,
    en: kvengine::Engine,
    shards: Arc<Vec<RawRegion>>,
    shard_meta_getter: RegionMetaGetter,
    truncate_ts: u64,
    batch_size: usize,
    txn_status: TxnStatus,
    // `cm` is actually not used but meet the requirement of `MvccTxn`.
    cm: concurrency_manager::ConcurrencyManager,
    // `apply_locks` is used to make applying process mutual exclusive for each shard. As
    // commit/rollback locks will be conflict with `load_unloaded_tbls` during
    // `check_txn_status`.
    apply_locks: ApplyLocks,
}

#[derive(Debug)]
pub struct ResolvedLocks {
    pub total_normal_locks_cnt: usize,
    pub total_lock_txn_files_cnt: usize, // Number of lock txn files resolved.
    pub resolved_shards: Vec<u64>,       // The shards that have locks which are resolved.
}

impl LockResolver {
    pub fn new(
        tag: &str,
        en: kvengine::Engine,
        truncate_ts: u64,
        batch_size: usize,
        shard_meta_getter: RegionMetaGetter,
    ) -> Self {
        let shards = Arc::new(Self::collect_shards(&en));
        debug!("{} collect_shards", tag; "shards" => ?shards);

        let apply_locks = ApplyLocks::default();
        let txn_status = TxnStatus {
            en: en.clone(),
            shards: shards.clone(),
            txns: Default::default(),
            apply_locks: apply_locks.clone(),
        };
        Self {
            tag: Arc::new(tag.to_string()),
            en,
            shards,
            shard_meta_getter,
            truncate_ts,
            batch_size,
            txn_status,
            cm: concurrency_manager::ConcurrencyManager::new(1.into()),
            apply_locks,
        }
    }

    pub async fn resolve_locks(&self) -> Result<ResolvedLocks> {
        let mut resolved_locks = ResolvedLocks {
            total_normal_locks_cnt: 0,
            total_lock_txn_files_cnt: 0,
            resolved_shards: vec![],
        };
        let shards = self.shards.clone();
        let mut js = tokio::task::JoinSet::new();
        for shard in shards.iter() {
            let shard_id = shard.id;
            let clone = self.clone();
            js.spawn(async move { (shard_id, clone.resolve_shard(shard_id).await) });
        }
        while let Some(handle) = js.join_next().await {
            let handle = handle.unwrap();
            let (shard_id, res) = handle;
            match res {
                Ok((normal_locks_cnt, lock_txn_files_cnt)) => {
                    if normal_locks_cnt > 0 || lock_txn_files_cnt > 0 {
                        resolved_locks.total_normal_locks_cnt += normal_locks_cnt;
                        resolved_locks.total_lock_txn_files_cnt += lock_txn_files_cnt;
                        resolved_locks.resolved_shards.push(shard_id);
                    }
                }
                Err(err) => {
                    warn!("failed to resolve lock."; "err" => %err);
                    js.shutdown().await;
                    return Err(err);
                }
            };
        }
        Ok(resolved_locks)
    }

    fn collect_shards(en: &kvengine::Engine) -> Vec<RawRegion> {
        let mut shards: Vec<RawRegion> = en
            .collect_shard_id_vers()
            .into_iter()
            .map(|idver| {
                let shard = en.get_shard(idver.id).unwrap();
                RawRegion {
                    id: shard.id,
                    raw_start: shard.outer_start.to_vec(),
                    raw_end: shard.outer_end.to_vec(),
                    ..Default::default()
                }
            })
            .collect();
        shards.sort_unstable_by(|x, y| x.raw_start.cmp(&y.raw_start));
        shards
    }

    async fn resolve_shard(
        &self,
        shard_id: u64,
    ) -> Result<(
        usize, // normal_locks_cnt
        usize, // txn_file_locks_cnt
    )> {
        // Resolve txn file locks first. Otherwise, txn file locks will be met during
        // resolving normal locks.
        let txn_file_locks_cnt = self.resolve_shard_txn_file_locks(shard_id).await?;
        let normal_locks_cnt = self.resolve_shard_normal_locks(shard_id).await?;

        // Confirm all locks have been resolved.
        // Note: get a new SnapAccess for scan locks.
        let shard = self.en.get_shard(shard_id).unwrap();
        let snap = shard.new_snap_access();
        let mut reader = CloudReader::new(snap, false);
        let start = Key::from_raw(&shard.outer_start);
        let end = Key::from_raw(&shard.outer_end);
        let (kv_pairs, _) = reader.scan_locks(Some(&start), Some(&end), |_| true, 10)?;
        assert!(
            kv_pairs.is_empty(),
            "{} has more locks to resolve: {:?}",
            self.tag,
            kv_pairs
        );

        Ok((normal_locks_cnt, txn_file_locks_cnt))
    }

    async fn resolve_shard_normal_locks(&self, shard_id: u64) -> Result<usize /* locks_cnt */> {
        let mut locks_cnt = 0;
        let mut mvcc_txn = MvccTxn::new(TimeStamp::zero(), self.cm.clone());

        let shard = self.en.get_shard(shard_id).unwrap();
        let snap = shard.new_snap_access();
        let mut reader = CloudReader::new(snap, false); // TODO: check memory usage & enable fill_cache
        let mut start = Key::from_raw(&shard.outer_start);
        let end = Key::from_raw(&shard.outer_end);
        loop {
            // Resolve all locks, because LOCK_CF can't be truncated during truncate_ts. And
            // the lock value in LOCK_CF is invalid for the new keyspace.
            let (kv_pairs, is_remain) =
                reader.scan_locks(Some(&start), Some(&end), |_| true, self.batch_size)?;
            info!("{} scan_locks", self.tag; "shard_id" => shard_id, "locks" => ?kv_pairs);
            if is_remain {
                let mut raw_last = kv_pairs
                    .last()
                    .unwrap()
                    .0
                    .to_raw()
                    .map_err(|err| -> Error {
                        box_err!(
                            "to_raw error: key {:?}, err {:?}",
                            kv_pairs.last().unwrap().0,
                            err
                        )
                    })?;
                raw_last.push(0);
                start = Key::from_raw(&raw_last);
            }

            for (key, lock) in kv_pairs {
                let commit_ts = if lock.ts.into_inner() < self.truncate_ts {
                    self.txn_status.check_txn_status(&self.tag, &lock).await?
                } else {
                    0
                };
                if commit_ts > 0 && commit_ts <= self.truncate_ts {
                    Self::commit_lock(&self.tag, &mut mvcc_txn, key, &lock, commit_ts.into())?;
                    locks_cnt += 1;
                } else {
                    // Rollback lock.
                    Self::rollback(&self.tag, &mut mvcc_txn, key)?;
                    locks_cnt += 1;
                }

                if mvcc_txn.write_size() >= DEFAULT_TXN_BATCH_SIZE {
                    let mt = std::mem::replace(
                        &mut mvcc_txn,
                        MvccTxn::new(TimeStamp::zero(), self.cm.clone()),
                    );
                    self.apply(&shard, Some(mt), None).await?;
                }
            }

            if !is_remain {
                break;
            }
        }

        if !mvcc_txn.is_empty() {
            self.apply(&shard, Some(mvcc_txn), None).await?;
        }

        Ok(locks_cnt)
    }

    // Ref: tikv::storage::txn::actions::commit
    fn commit_lock(
        tag: &str,
        txn: &mut MvccTxn,
        key: Key,
        lock: &Lock,
        commit_ts: TimeStamp,
    ) -> Result<()> {
        info!("{} commit_lock", tag; "key" => ?key, "lock" => ?lock, "commit_ts" => ?commit_ts);
        if commit_ts < lock.min_commit_ts {
            return Err(box_err!(
                "{} trying to commit with smaller commit_ts than min_commit_ts, key {:?}, lock {:?}, commit_ts {:?}, min_commit_ts {:?}",
                tag,
                key,
                lock,
                commit_ts,
                lock.min_commit_ts
            ));
        }

        let write = txn_types::Write::new(
            WriteType::from_lock_type(lock.lock_type).ok_or_else(|| -> Error {
                box_err!("{} commit_lock: invalid lock: {:?}", tag, lock)
            })?,
            lock.ts,
            None,
        )
        .set_last_change(lock.last_change_ts, lock.versions_to_last_change)
        .set_txn_source(lock.txn_source);

        txn.put_write(key.clone(), commit_ts, write.as_ref().to_bytes());
        // `unlock_key` only used to build resolve locks requests, and then will be
        // ignored. The lock will be removed on applying commit.
        txn.unlock_key(
            key,
            true,              // unused
            TimeStamp::zero(), // unused
        );
        Ok(())
    }

    fn rollback(tag: &str, txn: &mut MvccTxn, key: Key) -> Result<()> {
        info!("{} rollback", tag; "key" => ?key);
        txn.unlock_key(
            key,
            true,              // unused
            TimeStamp::zero(), // unused
        );
        Ok(())
    }

    async fn apply(
        &self,
        shard: &Shard,
        txn: Option<MvccTxn>,
        txn_file_ref: Option<kvenginepb::TxnFileRef>,
    ) -> Result<()> {
        let store_id = self.en.get_engine_id();
        let region_meta = self
            .shard_meta_getter
            .load_region_meta(shard.id, shard.ver, store_id)
            .ok_or_else::<Error, _>(
                || box_err!(
                    "{} apply: load_region_meta failed, shard_id {}, shard_ver {}, shard_meta_getter {:?}",
                    self.tag,
                    shard.id,
                    shard.ver,
                    self.shard_meta_getter
                )
            )?;

        let mut write_data = if let Some(txn) = txn {
            let mut write_data = WriteData::from_modifies(txn.into_modifies());
            write_data.set_req_type(ReqType::ResolveLock);
            write_data
        } else if let Some(txn_file_ref) = txn_file_ref {
            let mut write_data = WriteData::default();
            write_data.txn_file = Some(TxnFileWriteData {
                txn_file_ref,
                write_bytes: 0, // write_bytes is unused here.
            });
            write_data
        } else {
            unreachable!()
        };

        let custom_req = modifies_to_requests(&kvrpcpb::Context::default(), &mut write_data);
        let guard = self.apply_locks.lock(shard.id).await;
        rfstore::store::apply_custom_log_in_recover(
            &self.en,
            store_id,
            shard,
            region_meta,
            custom_req,
        )
        .map_err(|err| -> Error {
            box_err!(
                "{} apply_custom_log_in_recover error: shard_id {:?}, err {:?}",
                self.tag,
                shard.id,
                err
            )
        })?;
        drop(guard);
        Ok(())
    }

    async fn resolve_shard_txn_file_locks(
        &self,
        shard_id: u64,
    ) -> Result<usize /* lock_txn_files_cnt */> {
        let shard = self.en.get_shard(shard_id).unwrap();
        let snap = shard.new_snap_access();

        let lock_txn_files = snap.get_lock_txn_files();
        for lock_txn_file in lock_txn_files {
            let lock =
                Lock::parse(lock_txn_file.get_lock_val_prefix()).map_err(|err| -> Error {
                    box_err!(
                        "{} parse lock error: shard_id {}, lock_txn_file {:?}, err {:?}",
                        self.tag,
                        shard_id,
                        lock_txn_file,
                        err
                    )
                })?;
            let commit_ts = if lock.ts.into_inner() < self.truncate_ts {
                self.txn_status.check_txn_status(&self.tag, &lock).await?
            } else {
                0
            };
            let txn_file_ref = if commit_ts > 0 && commit_ts <= self.truncate_ts {
                self.commit_txn_file_lock(&snap, lock_txn_file, &lock, commit_ts.into())?
            } else {
                self.rollback_txn_file_lock(&snap, lock_txn_file, &lock)?
            };
            self.apply(&shard, None, Some(txn_file_ref)).await?;
        }
        Ok(lock_txn_files.len())
    }

    fn commit_txn_file_lock(
        &self,
        snap: &kvengine::SnapAccess,
        lock_txn_file: &kvengine::table::TxnFile,
        lock: &Lock,
        commit_ts: TimeStamp,
    ) -> Result<kvenginepb::TxnFileRef> {
        info!("{} commit_txn_file_lock", self.tag;
            "lock_txn_file" => ?lock_txn_file,
            "lock" => ?lock,
            "commit_ts" => ?commit_ts);
        if commit_ts < lock.min_commit_ts {
            return Err(box_err!(
                "{} trying to commit with smaller commit_ts than min_commit_ts, lock {:?}, commit_ts {:?}, min_commit_ts {:?}",
                self.tag,
                lock,
                commit_ts,
                lock.min_commit_ts
            ));
        }
        self.build_txn_file_ref(snap, lock_txn_file, lock, commit_ts)
    }

    fn rollback_txn_file_lock(
        &self,
        snap: &kvengine::SnapAccess,
        lock_txn_file: &kvengine::table::TxnFile,
        lock: &Lock,
    ) -> Result<kvenginepb::TxnFileRef> {
        info!("{} rollback_txn_file_lock", self.tag;
            "lock_txn_file" => ?lock_txn_file,
            "lock" => ?lock);
        self.build_txn_file_ref(snap, lock_txn_file, lock, TimeStamp::zero())
    }

    // Ref: TxnFileCommand::build_commit_txn_file_ref
    // Keyspace ID is not prepend here, as the generated `TxnFileRef` will not be
    // used by old versions.
    fn build_txn_file_ref(
        &self,
        snap: &kvengine::SnapAccess,
        lock_txn_file: &kvengine::table::TxnFile,
        lock: &Lock,
        commit_ts: TimeStamp,
    ) -> Result<kvenginepb::TxnFileRef> {
        let mut txn_file_ref = kvenginepb::TxnFileRef::new();

        let user_meta = UserMeta::new(lock.ts.into_inner(), commit_ts.into_inner());
        txn_file_ref.set_user_meta(user_meta.to_array().to_vec());
        txn_file_ref.set_shard_ver(snap.get_version());
        txn_file_ref.set_start_ts(user_meta.start_ts);

        txn_file_ref.set_chunk_ids(lock_txn_file.chunk_ids());
        txn_file_ref.set_inner_upper_bound(lock_txn_file.lower_bound().to_vec());
        txn_file_ref.set_inner_upper_bound(lock_txn_file.upper_bound().to_vec());
        Ok(txn_file_ref)
    }
}

#[derive(Clone)]
struct TxnStatus {
    en: kvengine::Engine,
    shards: Arc<Vec<RawRegion>>,
    txns: Arc<DashMap<u64 /* txn_id */, Arc<RwLock<Option<u64 /* commit_id */>>>>>,
    apply_locks: ApplyLocks,
}

impl TxnStatus {
    pub async fn check_txn_status(&self, tag: &str, lock: &Lock) -> Result<u64 /* commit_ts */> {
        let txn_status = self.txns.entry(lock.ts.into_inner()).or_default().clone();
        {
            let status = txn_status.read().await;
            if let Some(commit_ts) = *status {
                return Ok(commit_ts);
            }
        }

        let mut status = txn_status.write().await;
        if let Some(commit_ts) = *status {
            return Ok(commit_ts);
        }

        let commit_ts = self.check_txn_status_from_engine(lock).await?;
        *status = Some(commit_ts);
        info!("{} check_txn_status", tag; "lock" => ?lock, "commit_ts" => ?commit_ts);
        Ok(commit_ts)
    }

    async fn check_txn_status_from_engine(&self, lock: &Lock) -> Result<u64 /* commit_id */> {
        let mut reader_cache = HashMap::new();
        let cloud_reader = self
            .load_cloud_reader_by_key(&lock.primary, lock, &mut reader_cache)
            .await?;
        let primary_key = Key::from_raw(&lock.primary);
        if let Some(pk_lock) = cloud_reader.load_lock(&primary_key).unwrap() {
            if lock.ts == pk_lock.ts {
                return if pk_lock.use_async_commit {
                    self.check_secondary_locks(&pk_lock, &mut reader_cache)
                        .await
                } else {
                    Ok(0)
                };
            }
        }
        Self::get_txn_status_from_cloud_reader(cloud_reader, &primary_key, lock.ts)
    }

    async fn load_cloud_reader_by_key<'a>(
        &'a self,
        key: &[u8],
        lock: &Lock,
        reader_cache: &'a mut HashMap<u64, CloudReader>,
    ) -> Result<&'a mut CloudReader> {
        let shard_id = self.get_shard_by_key(key);
        if shard_id.is_none() {
            return Err(box_err!(
                "shard not found for key {:?}, lock {:?}",
                tikv_util::escape(key),
                lock
            ));
        }
        let shard_id = shard_id.unwrap();
        if let Vacant(e) = reader_cache.entry(shard_id) {
            let mut snap = self.en.get_snap_access(shard_id).unwrap();
            if snap.has_unloaded_tables() {
                let guard = self.apply_locks.lock(shard_id).await;
                self.en
                    .load_unloaded_tables(snap.get_id(), snap.get_version(), false)?;
                drop(guard);
                snap = self.en.get_snap_access(shard_id).unwrap();
            }
            // TODO: check memory usage & enable fill_cache
            e.insert(CloudReader::new(snap, false));
        }
        Ok(reader_cache.get_mut(&shard_id).unwrap())
    }

    // TODO: Use IA.
    fn get_txn_status_from_cloud_reader(
        cloud_reader: &mut CloudReader,
        key: &Key,
        ts: TimeStamp,
    ) -> Result<u64 /* commit_id */> {
        // Ref: check_txn_status_missing_lock
        match cloud_reader.get_txn_commit_record(key, ts)? {
            TxnCommitRecord::SingleRecord { commit_ts, write } => {
                if write.write_type == WriteType::Rollback {
                    Ok(0)
                } else {
                    Ok(commit_ts.into_inner())
                }
            }
            _ => Ok(0),
        }
    }

    async fn check_secondary_locks(
        &self,
        pk_lock: &Lock,
        reader_cache: &mut HashMap<u64, CloudReader>,
    ) -> Result<u64 /* commit_id */> {
        if matches!(pk_lock.lock_type, LockType::Pessimistic) {
            return Ok(0);
        }

        let mut commit_ts = pk_lock.min_commit_ts;
        for key in &pk_lock.secondaries {
            let cloud_reader = self
                .load_cloud_reader_by_key(key, pk_lock, reader_cache)
                .await?;
            let key = Key::from_raw(key);
            match cloud_reader.load_lock(&key).unwrap() {
                // Here check the lock type is necessary.
                // Pessimistic locks may have the same `start_ts` with Put locks.
                // Also See https://github.com/tidbcloud/cloud-storage-engine/issues/2596
                Some(lock) if lock.ts == pk_lock.ts && lock.lock_type != LockType::Pessimistic => {
                    if commit_ts < lock.min_commit_ts {
                        commit_ts = lock.min_commit_ts;
                    }
                }
                _ => {
                    return Self::get_txn_status_from_cloud_reader(cloud_reader, &key, pk_lock.ts);
                }
            }
        }
        // All locks found, use min_commit_ts as commit_ts.
        Ok(commit_ts.into_inner())
    }

    fn get_shard_by_key(&self, key: &[u8]) -> Option<u64 /* shard_id */> {
        self.shards
            .binary_search_by(|x| x.compare_with_key(key))
            .ok()
            .map(|idx| self.shards[idx].id)
    }
}

#[derive(Default, Clone)]
struct ApplyLocks {
    inner: Arc<Mutex<HashMap<u64 /* shard_id */, Arc<Mutex<()>>>>>,
}

impl ApplyLocks {
    pub async fn lock(&self, shard_id: u64) -> OwnedMutexGuard<()> {
        let mut inner = self.inner.lock().await;
        let lock = inner.entry(shard_id).or_default().clone();
        drop(inner);
        lock.lock_owned().await
    }
}
