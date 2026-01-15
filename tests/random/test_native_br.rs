// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, atomic::Ordering},
    thread::{JoinHandle, sleep},
    time::Duration,
};

use engine_traits::ObjectCache;
use kvengine::dfs::{Dfs, new_dfs_from_config};
use native_br::{
    backup::BackupConfig,
    backup_worker,
    error::Error,
    limiter::ThroughputLimiter,
    restore::{RestoreConfig, get_cluster_backup_meta, get_cluster_backup_meta_async},
    restore_keyspace::{self, ReportRestoreStepTrait, RestoreStep},
    rfengine_cache::RfEngineCache,
};
use pd_client::PdClient;
use rand::Rng;
use test_cloud_server::{
    client::ClusterClient,
    keyspace::{ClusterKeyspaceClient, KeyspaceManager},
    try_wait_result,
};
use test_pd_client::TestPdClient;
use tikv_util::{config::ReadableDuration, info, time::Instant, warn};
use tokio::runtime::Runtime;

use crate::{
    BACKUP_COUNTER, BACKUP_TOLERATED_ERR_COUNTER, BROKEN_BACKUP_COUNTER, RESTORE_COUNTER,
    RESTORE_TOLERATED_ERR_COUNTER, create_new_keyspace,
};

const FETCH_WAL_TIMEOUT: ReadableDuration = ReadableDuration::secs(30);
// Use small duration to cover scene of error tolerance more easily.
const FETCH_WAL_TIMEOUT_TOLERATE_ERR: ReadableDuration = ReadableDuration::secs(1);
const RFENGINE_HTTP_ERROR_RETRY_TIMES: usize = 20;

pub(crate) fn do_restore_keyspace(
    pd_client: Arc<dyn PdClient>,
    runtime: &Runtime,
    config: RestoreConfig,
    keyspace: u32,
    target_keyspace: u32,
    backup_name: &str,
    truncate_ts: u64,
    reporter: Arc<dyn ReportRestoreStepTrait>,
    object_cache: Option<ObjectCache>,
    limiter: Option<Arc<ThroughputLimiter>>,
    rfengine_cache: Option<RfEngineCache>,
) -> native_br::Result<restore_keyspace::RestoredKeyspace> {
    let dfs_config = config.dfs.clone();
    let dfs = new_dfs_from_config(dfs_config);
    if let Some(rf_cache) = &rfengine_cache {
        rf_cache.fill_cache(backup_name, truncate_ts, object_cache.clone())?;
    }
    restore_keyspace::restore_keyspace(
        keyspace,
        target_keyspace,
        backup_name,
        None,
        dfs,
        config,
        pd_client,
        runtime,
        Some(truncate_ts),
        reporter,
        object_cache,
        limiter,
        rfengine_cache,
    )
}

pub(crate) fn spawn_backup(
    client: ClusterClient,
    keyspace_manager: KeyspaceManager,
    config: BackupConfig,
    backup_worker: Arc<backup_worker::BackupWorker>,
    dfs: Arc<dyn Dfs>,
    interval: Duration,
    timeout: Duration,
) -> tokio::task::JoinHandle<()> {
    let dfs = dfs.clone();
    tokio::spawn(async move {
        let start_time = Instant::now();
        let mut last_backup_time = start_time;
        while start_time.saturating_elapsed() < timeout {
            // Note:
            // 1. Backup for RefStore of single keyspace to avoid global locking.
            // 2. The backup of TiKV is still for the whole cluster.
            // 3. As the keyspace of backup determines the keyspace to restore, we pick the
            //    random keyspace uniformly, to generate the scenario that some big
            //    keyspaces are never restored.
            let keyspace_id = {
                let mut rng = rand::thread_rng();
                keyspace_manager.get_uniform_random_keyspace(&mut rng)
            };

            let lock = keyspace_manager.get_keyspace_lock(keyspace_id);
            let guard = lock.mutex_lock().await;
            let backup_ts = client.get_ts().into_inner();
            let mut keyspace_backup = keyspace_manager.backup_keyspace(keyspace_id, backup_ts);
            // Downgrade to shared lock, to enable write workloads on the keyspace, but
            // block restore workload.
            // As in scene of restoration, `truncate_ts` cannot truncate the extra restored
            // data after `backup_ts`.
            // See https://github.com/tidbcloud/cloud-storage-engine/issues/1094.
            let shared_guard = guard.downgrade();

            let backup_res = match backup_worker.instant_backup().await {
                Ok(backup_res) => backup_res,
                Err(err) if is_backup_error_retryable(&err) => {
                    warn!("backup failed, retry: {:?}", err);
                    continue;
                }
                Err(err) => {
                    panic!("backup failed: {:?}", err);
                }
            };

            keyspace_backup.backup_name = Some(backup_res.backup_file.name().to_string());
            keyspace_manager.add_backup(keyspace_backup);
            drop(shared_guard);

            let backup_meta = get_cluster_backup_meta_async(
                dfs.as_ref(),
                backup_res.backup_file.name().to_string(),
            )
            .await;
            info!(
                "instant backup success, keyspace {}, res {:?}, backup_meta {:?}",
                keyspace_id, backup_res, backup_meta,
            );

            BACKUP_COUNTER.fetch_add(1, Ordering::SeqCst);

            assert!(backup_meta.tolerated_err as usize <= config.tolerate_err);
            if backup_meta.tolerated_err > 0 {
                BACKUP_TOLERATED_ERR_COUNTER
                    .fetch_add(backup_meta.tolerated_err as usize, Ordering::Relaxed);
            }

            let backup_elapsed = last_backup_time.saturating_elapsed();
            // No less than 1 second to avoid generating the same backup name.
            let sleep_time = interval
                .saturating_sub(backup_elapsed)
                .max(Duration::from_millis(1100));
            tokio::time::sleep(sleep_time).await;
            last_backup_time = Instant::now();
        }
        info!("backup thread exit");
    })
}

// `Error::HttpError` is retryable even thought `tolerate_err` > 0, as there are
// chances that backup is performed across the restart of more than one store.
fn is_backup_error_retryable(err: &Error) -> bool {
    match err {
        Error::MetaNotFound(_)
        | Error::HttpRequestError(_)
        | Error::RfengineDfsWorkerUnhealthy(_) => true,
        Error::BackupErrorOnStores(errs, _) => errs.iter().all(|e| is_backup_error_retryable(e)),
        Error::SharedError(err) => is_backup_error_retryable(err.inner()),
        _ => false,
    }
}

pub(crate) fn check_br() {
    let total_backup_count = BACKUP_COUNTER.load(Ordering::SeqCst);
    let total_restore_count = RESTORE_COUNTER.load(Ordering::SeqCst);

    assert!(
        // The total_backup_count is unstable and lower than expected.
        // TODO: investigate the reason.
        total_backup_count > 0,
        "backup count too small: {}",
        total_backup_count
    );
    assert!(
        total_restore_count > 0,
        "restore count too small: {}",
        total_restore_count
    );
}

pub(crate) fn spawn_restore_keyspace(
    pd_client: Arc<TestPdClient>,
    mut client: ClusterKeyspaceClient,
    restore_config: RestoreConfig,
    keyspace_manager: KeyspaceManager,
    dfs: Arc<dyn Dfs>,
    _enable_oss_chaos: bool,
    object_cache: Option<ObjectCache>,
    limiter: Option<Arc<ThroughputLimiter>>,
    timeout: Duration,
    rfengine_cache: Option<RfEngineCache>,
) -> JoinHandle<()> {
    let dfs = dfs.clone();
    std::thread::spawn(move || {
        let mut rng = rand::thread_rng();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .thread_name("restore-keyspace")
            .build()
            .unwrap();
        let reporter = Arc::new(DummyStepReporter::default());

        let start_time = Instant::now();
        sleep(Duration::from_secs(3));
        'next_restore: while start_time.saturating_elapsed() < timeout {
            let backup = match keyspace_manager.get_random_backup(&mut rng) {
                Some(backup) => backup,
                None => {
                    sleep(Duration::from_millis(500));
                    continue;
                }
            };
            if let Some(rf_cache) = &rfengine_cache {
                rf_cache.register_keyspace(backup.keyspace_id, backup.backup_ts);
            }

            let source_keyspace = backup.keyspace_id;
            let branching = source_keyspace > 0 && rng.gen_bool(0.5);
            let (target_keyspace, target_keyspace_lock_guard) = if branching {
                let (new_keyspace, lock_guard) = runtime.block_on(create_new_keyspace(
                    &pd_client,
                    &keyspace_manager,
                    dfs.as_ref(),
                    0,
                    0.0,
                    true,
                ));
                assert!(lock_guard.is_some());
                info!(
                    "branching restore {}->{}, create new keyspace",
                    source_keyspace, new_keyspace
                );
                (new_keyspace, lock_guard)
            } else {
                (source_keyspace, None)
            };
            let backup_name = backup.backup_name().to_string();
            let backup_meta = get_cluster_backup_meta(dfs.as_ref(), backup_name.clone());
            let tag = format!("{}->{}[{}]", source_keyspace, target_keyspace, backup_name);

            let mut config = restore_config.clone();
            config.timeout_fetch_wal = if config.tolerate_err > backup_meta.tolerated_err as usize {
                FETCH_WAL_TIMEOUT_TOLERATE_ERR
            } else {
                FETCH_WAL_TIMEOUT
            };
            let config_tolerate_err = config.tolerate_err;

            // TODO: test without blocking write workloads.
            let mut restored_keyspace = None;
            'inner_retry: for _ in 0..RFENGINE_HTTP_ERROR_RETRY_TIMES {
                let _guard = target_keyspace_lock_guard.is_none().then(|| {
                    let lock = keyspace_manager.get_keyspace_lock(target_keyspace);
                    runtime.block_on(lock.mutex_lock())
                });

                // The process of destroying ranges is not determined. So skip verifying the
                // destroying ranges.
                runtime
                    .block_on(client.verify_keyspace_and_skip_destroyed_ranges(target_keyspace))
                    .unwrap_or_else(|err| {
                        panic!(
                            "{} verify_keyspace_with_ref_store (before restore): {:?}",
                            tag, err
                        )
                    });

                // We always perform PiTR here. Snapshot restore will block write workload
                // during the whole backup process, which is not efficient.
                // And actually there are only trivial differences between PiTR and snapshot
                // restore.
                restored_keyspace = match do_restore_keyspace(
                    pd_client.clone(),
                    &runtime,
                    config.clone(),
                    source_keyspace,
                    target_keyspace,
                    &backup_name,
                    backup.backup_ts,
                    reporter.clone(),
                    object_cache.clone(),
                    limiter.clone(),
                    rfengine_cache.clone(),
                ) {
                    Ok(res) => Some(res),
                    Err(Error::BackupEmptyForKeyspace(_)) => {
                        // Empty backup will happen on newly created keyspace. Retry.
                        warn!("{} backup is empty, retry", tag);
                        continue 'next_restore;
                    }
                    Err(Error::WalChunkIntegrityError(msg)) => {
                        // TODO: retry on next backup.
                        warn!(
                            "{} backup is broken: WAL chunk integrity error ({}), retry",
                            tag, msg
                        );
                        // TODO: assert!(enable_oss_chaos);
                        BROKEN_BACKUP_COUNTER.fetch_add(1, Ordering::SeqCst);
                        continue 'next_restore;
                    }
                    Err(Error::RfengineHttpRequestError(err)) => {
                        // We would still meet `RfengineHttpError` even thought `tolerate_err` > 0,
                        // as there are chances that restoration is performed across the restart of
                        // more than one store.
                        warn!("{} meet RfengineHttpError, retry", tag; "err" => ?err);
                        continue 'inner_retry;
                    }
                    Err(err) => panic!("{} restore failed: {:?}", tag, err),
                };
                keyspace_manager.restore_keyspace(&tag, backup, target_keyspace);

                // To find data corruption early, and generate read workload as well.
                // The retry should not be necessary.
                // TODO: Remove the retry after verification issue is addressed.
                let verify_res = try_wait_result(
                    || {
                        let verify_res = runtime.block_on(
                            client.verify_keyspace_and_skip_destroyed_ranges(target_keyspace),
                        );
                        if verify_res.is_err() {
                            warn!(
                                "{} verify_keyspace_with_ref_store failed (after restore): {:?}",
                                tag, verify_res
                            );
                        }
                        verify_res
                    },
                    10,
                );
                if verify_res.is_err() {
                    info!("{} backup_meta: {:?}", tag, backup_meta);
                    panic!(
                        "{} verify_keyspace_with_ref_store (after restore): {:?}",
                        tag, verify_res
                    );
                }

                break;
            }
            drop(target_keyspace_lock_guard);
            let restored_keyspace = restored_keyspace.unwrap_or_else(|| {
                panic!("{} RfengineHttpError retry limit exceeded", tag);
            });
            info!("{} restore keyspace success", tag; "res" => ?restored_keyspace);
            RESTORE_COUNTER.fetch_add(1, Ordering::Relaxed);

            assert!(restored_keyspace.tolerated_err <= config_tolerate_err);
            if restored_keyspace.tolerated_err > 0 {
                RESTORE_TOLERATED_ERR_COUNTER
                    .fetch_add(restored_keyspace.tolerated_err, Ordering::Relaxed);
            }

            sleep(Duration::from_secs(rng.gen_range(0..10)));
        }
        info!("restore keyspace thread exit");
    })
}

#[derive(Default)]
struct DummyStepReporter {}

impl ReportRestoreStepTrait for DummyStepReporter {
    fn report_step(&self, _step: RestoreStep) {}
}
