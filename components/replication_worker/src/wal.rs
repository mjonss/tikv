// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, collections::VecDeque, sync::Arc, time::Duration};

use chrono::NaiveDateTime;
use collections::{HashMap, HashMapExt, HashSet};
use kvengine::dfs::Dfs;
use kvproto::metapb;
use merged_engine::StoreProgress;
use native_br::{
    backup::IncrementalBackupFile, common::get_all_incremental_backups, wal::AssembledWalData,
};
use parking_lot::Mutex;
use pd_client::{PdClient, util::get_all_stores_except_tiflash_async};
use protobuf::Message;
use rfengine::service_worker::WalProgress;
use security::HttpClient;
use tikv_util::{box_err, box_try, box_try_join, debug, error, info, time::Instant, trace, warn};
use tokio::task::JoinSet;
use txn_types::TimeStamp;

use crate::{
    Error, ReplicationWorkerConfig, Result,
    metrics::REP_FEATCH_WAL_TARGET_COUNTER,
    util::{ArcTimeStamp, send_request_to_store},
};

const MAX_PENDING_TARGETS: usize = 64;

const SEARCH_BACKUPS_LIMIT: usize = 50;

// None: When the store is not ready.
pub(crate) type StoreWalProgresses = HashMap<u64 /* store_id */, Option<StoreProgress>>;

#[derive(Debug)]
pub(crate) struct StoreTargetAndLag {
    pub(crate) target: Option<StoreProgress>,
    pub(crate) epoch_lag: u32,
    pub(crate) offset_lag: i64, // Can be negative.
}

impl StoreTargetAndLag {
    pub(crate) fn compare(&self, other: &Self) -> cmp::Ordering {
        self.epoch_lag
            .cmp(&other.epoch_lag)
            .then_with(|| self.offset_lag.cmp(&other.offset_lag))
    }
}

#[derive(Clone, Default)]
pub(crate) struct WalProgressTargets {
    queue: Arc<Mutex<VecDeque<(TimeStamp, StoreWalProgresses)>>>,
}

impl WalProgressTargets {
    pub(crate) fn push_back(&self, target: (TimeStamp, StoreWalProgresses)) {
        // TODO: limit the queue length.
        self.queue.lock().push_back(target);
    }

    pub(crate) fn pop_front(&self) -> Option<(TimeStamp, StoreWalProgresses)> {
        self.queue.lock().pop_front()
    }

    pub(crate) fn front(&self) -> Option<(TimeStamp, StoreWalProgresses)> {
        self.queue.lock().front().cloned()
    }

    pub(crate) fn len(&self) -> usize {
        self.queue.lock().len()
    }
}

pub(crate) struct WalProgressFetcher {
    pd: Arc<dyn PdClient>,
    synced_target_ts: ArcTimeStamp,
    timeout: Duration,
    tolerate_store_err: bool,
    thread_pool: tokio::runtime::Handle,
    fs: Arc<dyn Dfs>,
    http_client: HttpClient,

    skip_store_addr_keywords: Vec<String>,

    /// The min/max time span of a target since the last one.
    ///
    /// Time span exceed the max limit will not be safe. We would miss the
    /// stores scale up then down during this period.
    ///
    /// As the targets fetched from rfengine are always of the current time, if
    /// the last target is too old, target will be fetched from backup.
    min_target_time_span: Duration,
    max_target_time_span: Duration,

    fetch_wal_target_from_backup: bool,
}

impl WalProgressFetcher {
    pub(crate) fn run(
        pd: Arc<dyn PdClient>,
        synced_target_ts: ArcTimeStamp,
        timeout: Duration,
        config: &ReplicationWorkerConfig,
        thread_pool: tokio::runtime::Handle,
        fs: Arc<dyn Dfs>,
        targets: WalProgressTargets,
    ) {
        let interval = config.sync_interval.0;
        let fetcher = Arc::new(Self::new(
            pd.clone(),
            synced_target_ts,
            timeout,
            config.tolerate_store_err,
            thread_pool.clone(),
            fs.clone(),
            config.skip_store_addr_keywords.clone(),
            config.min_wal_target_time_span.0,
            config.max_wal_target_time_span.0,
            config.fetch_wal_target_from_backup,
        ));
        let mut last_target_ts = None;
        let mut skipped_stores = SkippedStores::new(
            pd,
            config.skip_store_addr_keywords.clone(),
            config.min_wal_target_time_span.0,
        );
        thread_pool.spawn(async move {
            loop {
                let start_time = Instant::now_coarse();

                match fetcher
                    .fetch_target_ts_and_progress(&targets, last_target_ts, &mut skipped_stores)
                    .await
                {
                    Ok(Some((ts, progress))) => {
                        last_target_ts = Some(ts);
                        targets.push_back((ts, progress));
                    }
                    Ok(None) => {}
                    Err(err) => {
                        error!("WalProgressFetcher: fetch target failed: {:?}", err);
                        REP_FEATCH_WAL_TARGET_COUNTER
                            .with_label_values(&["error", ""])
                            .inc();
                    }
                }

                let elapsed = start_time.saturating_elapsed();
                tokio::time::sleep(interval.saturating_sub(elapsed)).await;
            }
        });
    }

    fn new(
        pd: Arc<dyn PdClient>,
        synced_target_ts: ArcTimeStamp,
        timeout: Duration,
        tolerate_store_err: bool,
        thread_pool: tokio::runtime::Handle,
        fs: Arc<dyn Dfs>,
        skip_store_addr_keywords: Vec<String>,
        min_target_time_span: Duration,
        max_target_time_span: Duration,
        fetch_wal_target_from_backup: bool,
    ) -> Self {
        let http_client = pd
            .get_security_mgr()
            .http_client(hyper::Client::builder())
            .unwrap();
        Self {
            pd,
            synced_target_ts,
            timeout,
            tolerate_store_err,
            http_client,
            thread_pool,
            fs,
            skip_store_addr_keywords,
            min_target_time_span,
            max_target_time_span,
            fetch_wal_target_from_backup,
        }
    }

    fn tolerate_store_err(&self) -> usize {
        self.tolerate_store_err as usize
    }

    async fn fetch_target_ts_and_progress(
        self: &Arc<Self>,
        targets: &WalProgressTargets,
        last_target_ts: Option<TimeStamp>,
        skipped_stores: &mut SkippedStores,
    ) -> Result<Option<(TimeStamp, StoreWalProgresses)>> {
        if targets.len() >= MAX_PENDING_TARGETS {
            debug!(
                "WalProgressFetcher: skip fetch, too many pending targets: {}",
                targets.len()
            );
            REP_FEATCH_WAL_TARGET_COUNTER
                .with_label_values(&["warn", "too_many_pending"])
                .inc();
            return Ok(None);
        }

        let last_target_ts = last_target_ts.unwrap_or_else(|| self.synced_target_ts.get());
        debug_assert!(!last_target_ts.is_zero());

        if self.should_fetch_from_backup(last_target_ts) {
            self.fetch_from_backup(last_target_ts, skipped_stores).await
        } else {
            self.fetch_from_rfengine(last_target_ts)
                .await
                .map(|x| Some(x))
        }
    }

    // Always fetch from backup when `min_target_time_span` is zero for test
    // purpose.
    fn should_fetch_from_backup(&self, last_target_ts: TimeStamp) -> bool {
        if !self.fetch_wal_target_from_backup {
            return false;
        }

        #[cfg(feature = "testexport")]
        if self.min_target_time_span.is_zero() {
            return true;
        }

        TimeStamp::now()
            .physical()
            .saturating_sub(last_target_ts.physical())
            >= self.max_target_time_span.as_millis() as u64
    }

    async fn fetch_from_rfengine(
        self: &Arc<Self>,
        last_target_ts: TimeStamp,
    ) -> Result<(TimeStamp, StoreWalProgresses)> {
        let ori_stores = get_all_stores_except_tiflash_async(self.pd.as_ref(), true).await?;
        let stores = filter_stores(&self.skip_store_addr_keywords, ori_stores);
        let target_ts = self.pd.get_min_tso().await?;

        let mut errors = vec![];
        let mut progresses = HashMap::with_capacity(stores.len());
        let mut join_set = JoinSet::new();
        for store in stores {
            let fetcher = self.clone();
            join_set.spawn_on(
                async move {
                    let res = fetcher.track_store_wal_progress(&store, target_ts).await;
                    (store.id, res)
                },
                &self.thread_pool,
            );
        }

        while let Some(task) = join_set.join_next().await {
            let (store_id, res) = box_try_join!(task);
            match res {
                Ok(wal_progress) => {
                    progresses.insert(
                        store_id,
                        Some(StoreProgress {
                            store_id,
                            epoch: wal_progress.epoch,
                            offset: wal_progress.offset,
                        }),
                    );
                }
                Err(err) => {
                    warn!("WalProgressFetcher: fetch from rfengine failed: {:?}", err; "store" => store_id);
                    progresses.insert(store_id, None);
                    errors.push(err);
                }
            }
        }

        if errors.len() > self.tolerate_store_err() {
            return Err(errors.pop().unwrap());
        }

        debug!("WalProgressFetcher: fetched from rfengine";
            "target_ts" => ?target_ts,
            "progresses" => ?progresses,
            "last_target_ts" => ?last_target_ts,
        );
        REP_FEATCH_WAL_TARGET_COUNTER
            .with_label_values(&["ok", "from_rfengine"])
            .inc();
        Ok((target_ts, progresses))
    }

    async fn track_store_wal_progress(
        &self,
        store: &metapb::Store,
        ts: TimeStamp,
    ) -> Result<StoreProgress> {
        let security_mgr = self.pd.get_security_mgr();
        let uri = security_mgr
            .build_uri(format!(
                "{}/rfengine/track_wal_progress",
                &store.status_address
            ))
            .unwrap();
        let track_req = cloud_server::TrackWalProgressRequest {
            ts: ts.into_inner(),
        };
        let req = http::Request::post(&uri)
            .body(serde_json::to_vec(&track_req).unwrap().into())
            .unwrap();
        let (status, data) = send_request_to_store(req, &self.http_client, self.timeout).await?;
        if !status.is_success() {
            let err_str = String::from_utf8_lossy(&data);
            return Err(box_err!(
                "track_store_wal_progress: send request error: {}, store {}",
                err_str,
                store.id
            ));
        }
        let progress: WalProgress = serde_json::from_slice(&data).map_err(|e| -> Error {
            box_err!(
                "track_store_wal_progress: decode error: {}, store {}",
                e,
                store.id
            )
        })?;
        debug_assert_ne!(progress.offset, 0); // 0 will be conflict with "read to end" during dump WAL.
        Ok(StoreProgress {
            store_id: store.id,
            epoch: progress.epoch,
            offset: progress.offset,
        })
    }

    async fn fetch_from_backup(
        &self,
        last_target_ts: TimeStamp,
        skipped_stores: &mut SkippedStores,
    ) -> Result<Option<(TimeStamp, StoreWalProgresses)>> {
        let (backup_file, backup_meta) = self.search_for_valid_backup(last_target_ts).await?;

        let target_ts = TimeStamp::new(backup_meta.backup_ts);

        box_try!(skipped_stores.refresh().await);

        let stores = backup_meta.get_stores();
        let mut progresses =
            HashMap::with_capacity(stores.len() + backup_meta.tolerated_err as usize);

        if backup_meta.tolerated_err > 0 {
            let error_stores = backup_meta.get_tolerated_err_stores();
            if error_stores.len() == backup_meta.tolerated_err as usize {
                for &store_id in error_stores {
                    if skipped_stores.contains(store_id) {
                        continue;
                    }
                    progresses.insert(store_id, None);
                }
            } else {
                // Backward compatibility. Use fake store id.
                for i in 0..backup_meta.tolerated_err as u64 {
                    progresses.insert(u64::MAX - i, None);
                }
            }
        }

        for store in stores {
            if skipped_stores.contains(store.store_id) {
                continue;
            }
            progresses.insert(
                store.store_id,
                Some(StoreProgress {
                    store_id: store.store_id,
                    epoch: store.epoch,
                    offset: store.offset,
                }),
            );
        }

        info!("WalProgressFetcher: fetched from backup";
            "target_ts" => ?target_ts,
            "progresses" => ?progresses,
            "last_target_ts" => ?last_target_ts,
            "backup" => backup_file.name(),
            "safe_ts" => ?TimeStamp::new(backup_meta.safe_ts),
        );
        REP_FEATCH_WAL_TARGET_COUNTER
            .with_label_values(&["ok", "from_backup"])
            .inc();
        Ok(Some((target_ts, progresses)))
    }

    async fn search_for_valid_backup(
        &self,
        last_target_ts: TimeStamp,
    ) -> Result<(IncrementalBackupFile, rfenginepb::ClusterBackupMeta)> {
        let timestamp_ms =
            last_target_ts.physical() as i64 + self.min_target_time_span.as_millis() as i64;
        let start_datetime = NaiveDateTime::from_timestamp_millis(timestamp_ms).unwrap();

        let max_backup_ts = TimeStamp::compose(
            last_target_ts.physical() + self.max_target_time_span.as_millis() as u64,
            0,
        );
        let max_backup = IncrementalBackupFile::from_backup_ts(max_backup_ts.into_inner());

        let (files, _) = box_try!(
            get_all_incremental_backups(
                self.fs.as_ref(),
                &start_datetime.date(),
                Some(&start_datetime.time()),
                SEARCH_BACKUPS_LIMIT,
            )
            .await
        );
        debug!("WalProgressFetcher: search_for_valid_backup, candidates: {:?}", files;
            "start_datetime" => ?start_datetime, "max_backup" => ?max_backup);
        let mut candidates = 0;
        for backup_file in files {
            if backup_file.name() > max_backup.name() {
                break;
            }
            candidates += 1;

            let full_path = backup_file.full_path(&self.fs.get_prefix());
            let object = box_try!(
                self.fs
                    .get_object(
                        full_path.clone(),
                        full_path.clone(),
                        engine_traits::GetObjectOptions::default()
                    )
                    .await
            );
            let mut backup_meta = rfenginepb::ClusterBackupMeta::new();
            box_try!(backup_meta.merge_from_bytes(&object));

            if self.backup_is_valid(&backup_meta, last_target_ts) {
                return Ok((backup_file, backup_meta));
            }

            debug!("WalProgressFetcher: backup is invalid";
                "meta" => %backup_meta, "last_target_ts" => ?last_target_ts);
            REP_FEATCH_WAL_TARGET_COUNTER
                .with_label_values(&["info", "invalid_backup"])
                .inc();
        }

        // No valid backup, the replication will stop.
        // Should send an alarm.
        error!("WalProgressFetcher: no valid backup";
            "last_target_ts" => ?last_target_ts, "start_datetime" => ?start_datetime,
            "candidates" => candidates);
        REP_FEATCH_WAL_TARGET_COUNTER
            .with_label_values(&["fatal", "no_backup"])
            .inc();
        Err(Error::NoValidBackup)
    }

    fn backup_is_valid(
        &self,
        backup_meta: &rfenginepb::ClusterBackupMeta,
        last_target_ts: TimeStamp,
    ) -> bool {
        backup_meta.is_lightweight
            && backup_meta.backup_ts > last_target_ts.into_inner()
            && backup_meta.tolerated_err as usize <= self.tolerate_store_err()
            && backup_meta.safe_ts <= last_target_ts.into_inner()
    }
}

#[derive(Default)]
pub(crate) struct WalCache {
    inner: HashMap<u64 /* store_id */, Option<(u32 /* epoch_id */, AssembledWalData)>>,
}

impl WalCache {
    pub(crate) fn get_mut(&mut self, store_id: u64, epoch: u32) -> Option<&mut AssembledWalData> {
        let entry = self.inner.get_mut(&store_id)?.as_mut()?;
        if entry.0 != epoch {
            debug_assert!(false);
            None
        } else {
            Some(&mut entry.1)
        }
    }

    pub(crate) fn insert(&mut self, store_id: u64, epoch: u32, wal_data: AssembledWalData) {
        self.inner.insert(store_id, Some((epoch, wal_data)));
    }

    pub(crate) fn remove_cache(&mut self, store_id: u64) {
        if let Some(entry) = self.inner.get_mut(&store_id) {
            entry.take();
        }
    }

    pub(crate) fn contains_store(&self, store_id: u64) -> bool {
        self.inner.contains_key(&store_id)
    }
}

#[derive(Debug)]
pub(crate) struct UpdateWalError {
    pub(crate) err: Error,
    pub(crate) target: Option<StoreProgress>,
}

#[derive(Debug)]
pub(crate) enum UpdateWalResult {
    Finished {
        wal_size: u64,
        errors: Vec<UpdateWalError>, // Finished with errors when not empty.
    },
    NotFinished {
        wal_size: u64,
    },
}

impl UpdateWalResult {
    pub(crate) fn metric_label(&self) -> &'static str {
        match self {
            Self::Finished { .. } => "finished",
            Self::NotFinished { .. } => "not_finished",
        }
    }
}

fn filter_stores(
    skip_store_addr_keywords: &[String],
    stores: Vec<metapb::Store>,
) -> Vec<metapb::Store> {
    if skip_store_addr_keywords.is_empty() {
        return stores;
    }
    stores
        .into_iter()
        .filter(|store| {
            // Store address NOT contains ANY keyword.
            !skip_store_addr_keywords
                .iter()
                .any(|keyword| store.get_address().contains(keyword))
        })
        .collect()
}

fn get_skipped_stores(
    skip_store_addr_keywords: &[String],
    stores: &[metapb::Store],
) -> Option<HashSet<u64 /* store_id */>> {
    if skip_store_addr_keywords.is_empty() {
        return None;
    }
    let skipped_store = stores
        .iter()
        .filter_map(|store| {
            // Store address contains ANY keyword.
            skip_store_addr_keywords
                .iter()
                .any(|keyword| store.get_address().contains(keyword))
                .then_some(store.id)
        })
        .collect::<HashSet<_>>();
    (!skipped_store.is_empty()).then_some(skipped_store)
}

struct SkippedStores {
    pd: Arc<dyn PdClient>,
    skip_store_addr_keywords: Vec<String>,
    refresh_interval: Duration,

    inner: Option<HashSet<u64 /* store_id */>>,
    update_time: Option<Instant>,
}

impl SkippedStores {
    fn new(
        pd: Arc<dyn PdClient>,
        skip_store_addr_keywords: Vec<String>,
        refresh_interval: Duration,
    ) -> Self {
        Self {
            pd,
            skip_store_addr_keywords,
            refresh_interval,
            inner: None,
            update_time: None,
        }
    }
    async fn refresh(&mut self) -> Result<()> {
        if self.skip_store_addr_keywords.is_empty() {
            return Ok(());
        }

        let now = Instant::now_coarse();
        if self
            .update_time
            .is_some_and(|x| now.saturating_duration_since(x) < self.refresh_interval)
        {
            return Ok(());
        }

        // Includes tombstone stores.
        let ori_stores = get_all_stores_except_tiflash_async(self.pd.as_ref(), false).await?;
        self.inner = get_skipped_stores(&self.skip_store_addr_keywords, &ori_stores);
        self.update_time = Some(now);
        trace!("SkippedStores refresh";
            "ori_stores" => ?ori_stores, "skipped_stores" => ?self.inner);
        Ok(())
    }

    fn contains(&self, store_id: u64) -> bool {
        self.inner.as_ref().is_some_and(|s| s.contains(&store_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_stores() {
        let store_addrs = vec![
            "tikv-p0-tikv-0", // id: 0
            "tikv-p1-tikv-0",
            "tikv-poc-p0-tikv-0", // id: 2
            "tikv-poc-p1-tikv-0",
            "tikv-poc-p2-tikv-0",
            "tikv-p2-tikv-0",
            "tikv-hot-p0-tikv-0", // id: 6
            "tikv-hot-p1-tikv-0",
            "tikv-hot-p2-tikv-0",
        ];
        let stores = store_addrs
            .into_iter()
            .enumerate()
            .map(|(i, addr)| {
                let mut store = metapb::Store::default();
                store.set_id(i as u64);
                store.set_address(addr.to_string());
                store
            })
            .collect::<Vec<_>>();

        let skip_keywords = vec!["poc".to_string(), "hot".to_string()];
        let filtered_stores = filter_stores(&skip_keywords, stores.clone());
        let filtered_addrs: Vec<&str> = filtered_stores
            .iter()
            .map(|store| store.get_address())
            .collect();
        let expected_addrs = vec!["tikv-p0-tikv-0", "tikv-p1-tikv-0", "tikv-p2-tikv-0"];
        assert_eq!(filtered_addrs, expected_addrs);

        let skipped_stores = get_skipped_stores(&skip_keywords, &stores).unwrap();
        assert_eq!(skipped_stores, HashSet::from_iter([2u64, 3, 4, 6, 7, 8]));
        assert_eq!(get_skipped_stores(&["nothing".into()], &stores), None);
    }
}
