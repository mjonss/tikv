// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp, fmt, mem,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::executor::block_on;
use pd_client::PdClient;
use rfenginepb::ClusterBackupMeta;
use tikv_util::{
    config::ReadableDuration,
    error,
    future::paired_future_callback,
    info,
    mpsc::paired_callback,
    retry::sleep_async,
    time::Instant,
    warn,
    worker::{LazyWorker, Runnable, RunnableWithTimer, Scheduler},
};

use crate::{
    backup,
    backup::{
        BackupConfig, IncrementalBackupFile, Result, SharedResult, update_service_safe_point,
    },
    error::{Error, SharedError},
    metrics::{NATIVE_BR_BACKUP_BATCH_SIZE, NATIVE_BR_BACKUP_ERROR, NATIVE_BR_BACKUP_SUCCESS},
};

pub const DEFAULT_TIMEOUT_INSTANT_BACKUP: ReadableDuration = ReadableDuration::secs(60);
const MIN_BACKUP_INTERVAL: Duration = Duration::from_millis(1050);
const MIN_BATCH_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub struct InstantBackupResult {
    pub backup_file: IncrementalBackupFile,
    pub backup_ts: u64,
    pub safe_ts: u64,
}

type InstantBackupCallback = Box<dyn FnOnce(SharedResult<Arc<InstantBackupResult>>) + Send>;

enum BackupTask {
    InstantBackup { cb: InstantBackupCallback },
    Stop { cb: Box<dyn FnOnce(()) + Send> },
}

impl fmt::Display for BackupTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BackupTask::InstantBackup { .. } => write!(f, "instant backup"),
            BackupTask::Stop { .. } => write!(f, "stop"),
        }
    }
}

pub struct BackupWorker {
    worker: LazyWorker<BackupTask>,
    scheduler: Scheduler<BackupTask>,
}

impl BackupWorker {
    pub fn new(
        config: BackupConfig,
        pd_client: Arc<dyn PdClient>,
        periodic_backup_interval: Duration,
        mut batch_interval: Duration,
    ) -> Self {
        if batch_interval < MIN_BATCH_INTERVAL {
            warn!(
                "batch interval too small ({:?}), adjust to {:?}",
                batch_interval, MIN_BATCH_INTERVAL
            );
            batch_interval = MIN_BATCH_INTERVAL;
        } else if !periodic_backup_interval.is_zero() && batch_interval > periodic_backup_interval {
            warn!(
                "batch interval too large ({:?}), adjust to {:?}",
                batch_interval, periodic_backup_interval
            );
            batch_interval = periodic_backup_interval;
        }

        let mut worker = LazyWorker::new("backup-worker");
        let scheduler = worker.scheduler();
        let runner = BackupRunner::new(config, pd_client, periodic_backup_interval, batch_interval);
        runner.init();
        let ok = worker.start_with_timer(runner);
        assert!(ok);
        Self { worker, scheduler }
    }

    pub fn stop(&mut self) {
        self.cancel_pending_requests();
        self.worker.stop();
    }

    pub async fn instant_backup(&self) -> Result<Arc<InstantBackupResult>> {
        let (cb, fut) = paired_future_callback();
        self.scheduler
            .schedule(BackupTask::InstantBackup { cb })
            .unwrap();
        fut.await.unwrap().map_err(|e| {
            warn!("instant backup failed"; "error" => ?e);
            e.into()
        })
    }

    fn is_err_retryable(e: &Error) -> bool {
        match e {
            Error::Stopped => false,
            Error::SharedError(e) => Self::is_err_retryable(e.0.as_ref()),
            _ => true,
        }
    }

    pub async fn instant_backup_with_retry(
        &self,
        timeout: Duration,
    ) -> Result<Arc<InstantBackupResult>> {
        let mut last_err = None;
        let start_time = Instant::now_coarse();
        while start_time.saturating_elapsed() < timeout {
            match self.instant_backup().await {
                Ok(res) => return Ok(res),
                Err(e) => {
                    if Self::is_err_retryable(&e) {
                        last_err = Some(e);
                        sleep_async(Duration::from_millis(500)).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        Err(last_err.unwrap())
    }

    fn cancel_pending_requests(&self) {
        let (cb, rx) = paired_callback();
        self.scheduler.schedule(BackupTask::Stop { cb }).unwrap();
        let _ = rx.recv();
    }
}

#[cfg(feature = "testexport")]
#[derive(Clone)]
pub struct BackupWorkerHandle {
    scheduler: Scheduler<BackupTask>,
}

#[cfg(feature = "testexport")]
impl BackupWorkerHandle {
    pub async fn instant_backup(&self) -> Result<Arc<InstantBackupResult>> {
        let (cb, fut) = paired_future_callback();
        self.scheduler
            .schedule(BackupTask::InstantBackup { cb })
            .map_err(|_| Error::Stopped)?;
        fut.await.unwrap().map_err(Into::into)
    }
}

#[cfg(feature = "testexport")]
impl BackupWorker {
    pub fn handle(&self) -> BackupWorkerHandle {
        BackupWorkerHandle {
            scheduler: self.scheduler.clone(),
        }
    }
}

struct BackupRunner {
    config: BackupConfig,
    pd_client: Arc<dyn PdClient>,
    periodic_backup_interval: Duration,
    batch_interval: Duration,

    last_backup_time: Instant, // The `backup_time` of last batch.
    last_backup_ts: u64,       // The `backup_ts` of last successful backup.
    last_backup_meta: Option<ClusterBackupMeta>,

    pending_reqs: Vec<BackupRequest>,
    pending_batches: Vec<BackupBatch>,

    stop: bool,
}

impl BackupRunner {
    fn new(
        config: BackupConfig,
        pd_client: Arc<dyn PdClient>,
        periodic_backup_interval: Duration,
        batch_interval: Duration,
    ) -> Self {
        Self {
            config,
            pd_client,
            periodic_backup_interval,
            batch_interval,
            last_backup_time: Instant::now() - MIN_BACKUP_INTERVAL,
            last_backup_ts: 0,
            last_backup_meta: None,
            pending_reqs: vec![],
            pending_batches: vec![],
            stop: false,
        }
    }

    fn init(&self) {
        if self.periodic_backup_enabled() {
            match Self::init_service_safe_point(self.pd_client.as_ref()) {
                Ok(safe_point) => {
                    info!("backup worker: init service safe point: {}", safe_point);
                }
                Err(e) => {
                    warn!("backup worker: init service safe point failed"; "err" => ?e);
                }
            }
        }
    }

    fn init_service_safe_point(pd_client: &dyn PdClient) -> Result<u64> {
        let gc_safe_point = block_on(pd_client.get_gc_safe_point())?;
        match update_service_safe_point(pd_client, gc_safe_point) {
            Ok(()) => Ok(gc_safe_point),
            Err(Error::PdError(pd_client::Error::UnsafeServiceGcSafePoint {
                requested,
                current_minimal,
            })) => {
                debug_assert_eq!(requested, gc_safe_point.into());
                info!("backup worker: service safe point has been set"; "current" => ?current_minimal);
                Ok(current_minimal.into_inner())
            }
            Err(e) => Err(e),
        }
    }

    fn handle_instant_backup(&mut self, cb: InstantBackupCallback) {
        self.pending_reqs.push(BackupRequest::new(cb));
    }

    fn handle_stop(&mut self) {
        let err = SharedError::from(Error::Stopped);
        for req in self.pending_reqs.drain(..) {
            req.cb(Err(err.clone()));
        }
        for req in self.pending_batches.drain(..).flat_map(|x| x.reqs) {
            req.cb(Err(err.clone()));
        }
    }

    fn handle_pending_backups(&mut self) {
        self.gather_batch();
        if let Some(batch) = self.extract_batch() {
            self.do_lightweight_backup(batch);
        }
    }

    fn gather_batch(&mut self) {
        if self.pending_reqs.is_empty() {
            return;
        }
        let reqs = mem::take(&mut self.pending_reqs);

        let backup_ts = match self.prepare_backup() {
            Ok(backup_ts) => backup_ts,
            Err(err) => {
                error!("backup worker: prepare backup failed"; "err" => ?err);
                NATIVE_BR_BACKUP_ERROR.inc();
                let err = SharedError::from(err);
                for req in reqs {
                    req.cb(Err(err.clone()));
                }
                return;
            }
        };

        let batch = BackupBatch {
            reqs,
            backup_ts,
            backup_time: Instant::now_coarse(),
        };
        self.last_backup_time = batch.backup_time;
        self.pending_batches.push(batch);
    }

    fn extract_batch(&mut self) -> Option<BackupBatch> {
        if self.pending_batches.is_empty() {
            return None;
        }

        let now = Instant::now_coarse();
        let mut batches = self
            .pending_batches
            .extract_if(.., |x| x.backup_time + self.config.backup_delay.0 <= now)
            .collect::<Vec<_>>();
        let mut last_batch = batches.pop()?;
        // Merge batch.
        for batch in batches {
            // Get maximum for safe.
            last_batch.backup_ts = cmp::max(batch.backup_ts, last_batch.backup_ts);
            last_batch.reqs.extend(batch.reqs);
        }
        Some(last_batch)
    }

    fn prepare_backup(&self) -> Result<u64 /* backup_ts */> {
        backup::get_backup_ts(self.pd_client.as_ref())
    }

    fn do_lightweight_backup(&mut self, batch: BackupBatch) {
        let backup_ts = batch.backup_ts;
        if self.last_backup_ts > 0
            && IncrementalBackupFile::from_backup_ts(self.last_backup_ts).name()
                == IncrementalBackupFile::from_backup_ts(backup_ts).name()
        {
            warn!("backup worker: backup_ts conflict, retry";
                "backup_ts" => backup_ts, "last_backup_ts" => self.last_backup_ts);
            self.pending_reqs.extend(batch.reqs);
            return;
        }

        info!("backup worker: start backup"; "backup_ts" => backup_ts);
        let res = backup::backup_cluster_with_ts(
            self.config.clone(),
            "".to_string(),
            self.pd_client.as_ref(),
            backup_ts,
            self.last_backup_meta.clone(),
        );
        self.handle_backup_result(backup_ts, res, batch);
    }

    fn handle_backup_result(
        &mut self,
        backup_ts: u64,
        res: Result<(String, ClusterBackupMeta)>,
        batch: BackupBatch,
    ) {
        match res {
            Ok((backup_path, backup_meta)) => {
                info!("backup succeeded"; "path" => ?backup_path, "meta" => %backup_meta);
                self.last_backup_ts = backup_ts;
                let safe_ts = backup_meta.safe_ts;
                if self.periodic_backup_enabled() {
                    self.last_backup_meta = Some(backup_meta);
                    self.try_update_service_safe_point(backup_ts);
                }

                NATIVE_BR_BACKUP_SUCCESS.inc();
                NATIVE_BR_BACKUP_BATCH_SIZE.observe(batch.reqs.len() as f64);
                let backup_file = IncrementalBackupFile::try_from_full_path(&backup_path).unwrap();

                let backup_result = Arc::new(InstantBackupResult {
                    backup_file,
                    backup_ts,
                    safe_ts,
                });
                for req in batch.reqs {
                    req.cb(Ok(backup_result.clone()));
                }
            }
            Err(err) => {
                NATIVE_BR_BACKUP_ERROR.inc();
                let err = SharedError::from(err);
                for req in batch.reqs {
                    req.cb(Err(err.clone()));
                }
            }
        }
    }

    fn try_update_service_safe_point(&mut self, backup_ts: u64) {
        if let Err(err) = update_service_safe_point(self.pd_client.as_ref(), backup_ts) {
            error!("backup worker: update safepoint failed"; "err" => ?err);
            NATIVE_BR_BACKUP_ERROR.inc();
        }
    }

    fn periodic_backup_enabled(&self) -> bool {
        !self.periodic_backup_interval.is_zero()
    }
}

impl Runnable for BackupRunner {
    type Task = BackupTask;

    fn run(&mut self, task: BackupTask) {
        match task {
            BackupTask::InstantBackup { cb } => {
                if self.stop {
                    cb(Err(Error::Stopped.into()));
                    return;
                }
                self.handle_instant_backup(cb);
            }
            BackupTask::Stop { cb } => {
                self.stop = true;
                self.handle_stop();
                cb(());
            }
        }
    }
}

impl RunnableWithTimer for BackupRunner {
    fn on_timeout(&mut self) {
        if self.stop {
            return;
        }

        if self.last_backup_time.saturating_elapsed() < MIN_BACKUP_INTERVAL {
            // The backup is named according to the seconds of TSO physical time.
            // So skip this round to avoid the name conflict.
            return;
        }

        if self.periodic_backup_enabled()
            && self.last_backup_time.saturating_elapsed() >= self.periodic_backup_interval
        {
            self.pending_reqs.push(BackupRequest::new_periodic_backup());
        }

        self.handle_pending_backups();
    }

    fn get_interval(&self) -> Duration {
        let mut interval = self.batch_interval.as_secs();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        interval -= now.as_secs() % interval;
        Duration::from_secs(interval)
    }
}

struct BackupRequest {
    cb: Option<InstantBackupCallback>,
}

impl BackupRequest {
    fn new(cb: InstantBackupCallback) -> Self {
        Self { cb: Some(cb) }
    }

    fn new_periodic_backup() -> Self {
        Self { cb: None }
    }

    fn cb(mut self, res: SharedResult<Arc<InstantBackupResult>>) {
        if let Some(cb) = self.cb.take() {
            cb(res);
        }
    }
}

impl Drop for BackupRequest {
    fn drop(&mut self) {
        // For safety
        if let Some(cb) = self.cb.take() {
            debug_assert!(false, "backup request dropped without callback");
            cb(Err(Error::Dropped.into()));
        }
    }
}

struct BackupBatch {
    reqs: Vec<BackupRequest>,
    backup_ts: u64,
    backup_time: Instant,
}

#[cfg(test)]
mod tests {
    use test_pd_client::TestPdClient;

    use super::*;

    #[test]
    fn test_init_service_safe_point() {
        test_util::init_log_for_test();

        let pd_client = TestPdClient::new(1, false);
        pd_client.set_bootstrap(true);
        pd_client.set_gc_safe_point(1000).unwrap();

        assert_eq!(
            BackupRunner::init_service_safe_point(&pd_client).unwrap(),
            1000
        );

        update_service_safe_point(&pd_client, 2000).unwrap();
        // Will try to init with 1000.
        assert_eq!(
            BackupRunner::init_service_safe_point(&pd_client).unwrap(),
            2000
        );
    }
}
