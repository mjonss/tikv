// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod sql_util;
mod test_all;
mod test_columnar;
mod test_drop_table;
mod test_jepsen;
mod test_load_data;
mod test_native_br;
mod test_replication;
mod test_storage_class;
mod test_tidb;
mod test_tpc;
mod test_txn_file;
mod test_unique;
mod test_upgrade;

use std::{
    collections::HashSet,
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    thread::{JoinHandle, sleep},
    time::Duration,
};

use api_version::ApiV2;
use bytes::Bytes;
use cloud_encryption::KeyspaceEncryptionConfig;
use codec::number::NumberEncoder;
use futures::executor::block_on;
use http::{Request, StatusCode};
use hyper::Body;
use kvengine::{
    dfs::{self, Dfs, FileType},
    table::{
        file::InMemFile,
        schema_file::{SchemaFile, build_schema_file},
    },
};
use kvproto::metapb::Store;
use native_br::{common::send_request_to_store_with_retry, error::Error::HttpError};
use pd_client::PdClient;
use raftstore::coprocessor::RegionInfoProvider;
use rand::prelude::*;
use rfengine::set_dfs_worker_failpoint_target_store_id;
use security::{GetSecurityManager, SecurityConfig, SecurityManager};
use test_cloud_server::{
    ServerCluster,
    client::ClusterTxnClient,
    keyspace::{ClusterKeyspaceClient, CreateKeyspaceOptions, KeyspaceManager},
    must_wait_result,
    oss::ObjectStorageService,
    scheduler::Scheduler,
    tidb::TidbCluster,
    try_wait_result,
    util::broadcast_schema_file_request_and_check,
};
pub use test_cloud_server::{alloc_node_id, alloc_node_id_vec};
use test_pd_client::TestPdClient;
use tidb_query_datatype::{
    codec::row::v2::encoder_for_test::{Column, RowEncoder},
    expr::EvalContext,
};
use tikv::config::TikvConfig;
use tikv_client::TimestampExt;
use tikv_util::{box_err, codec::bytes::encode_bytes, error, info, mpsc, time::Instant, warn};
use tokio::sync::{OwnedRwLockWriteGuard, Semaphore};
use txn_types::Key;

pub(crate) type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub(crate) type Result<T> = std::result::Result<T, Error>;

lazy_static::lazy_static! {
    pub static ref WRITE_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref TXN_FILE_WRITE_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref MOVE_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref MERGE_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref TRANSFER_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref NODE_RESTART_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref BACKUP_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref BACKUP_TOLERATED_ERR_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref RESTORE_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref RESTORE_TOLERATED_ERR_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref BROKEN_BACKUP_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref LOAD_DATA_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref KEYSPACE_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref TABLE_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref DROP_TABLE_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref MANUAL_MAJOR_COMPACT_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref GC_ADVANCE_SAFE_POINT_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref TPCC_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref JEPSEN_BANK_TXN_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref JEPSEN_BANK_TXN_RETRY_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref UNIQUE_WORKLOAD_TXN_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref UNIQUE_WORKLOAD_CONFLICT_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref UNIQUE_TABLE_DDL_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref COLUMNAR_WRITE_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref COLUMNAR_RETRY_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref ALTER_TABLE_NON_IA_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref ALTER_TABLE_IA_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref ALTER_TABLE_AUTO_IA_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref ASYNC_SHARD_COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref DFS_UNHEALTHY_COUNTER: AtomicUsize = AtomicUsize::new(0);
}

pub const TIMEOUT: Duration = Duration::from_secs(90);
pub const WRITE_CONCURRENCY: usize = 4;
pub const TXN_FILE_WRITE_CONCURRENCY: usize = 2;

const REQUEST_MAJOR_COMPACT_ON_STORE_TIMEOUT: Duration = Duration::from_secs(60);

const KEYSPACE_CLEANUP_LOCKS_CONCURRENCY: usize = 4;
const KEYSPACE_CLEANUP_LOCKS_TIMEOUT: Duration = Duration::from_secs(30);
const GC_INTERVAL: Duration = Duration::from_secs(10);

const DROP_TABLE_CONCURRENCY: usize = 2; // More than 1 thread to reduce the chance of blocked by other mutual-exclusive workloads for a long time.

pub(crate) fn spawn_move(scheduler: Scheduler, two_node_down: Arc<RwLock<()>>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let start_time = Instant::now();
        while start_time.saturating_elapsed() < TIMEOUT {
            sleep(Duration::from_millis(1000));
            let guard = two_node_down.read().unwrap();
            let ok = scheduler.move_random_region();
            drop(guard);
            if ok {
                MOVE_COUNTER.fetch_add(1, Ordering::SeqCst);
            }
        }
        info!("move thread exit");
    })
}

pub(crate) fn spawn_merge(scheduler: Scheduler, disallow_cross_keyspace: bool) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let start_time = Instant::now();
        while start_time.saturating_elapsed() < TIMEOUT {
            if scheduler.merge_random_region(disallow_cross_keyspace) {
                MERGE_COUNTER.fetch_add(1, Ordering::SeqCst);
            }
        }
        info!("merge thread exit");
    })
}

pub(crate) fn spawn_transfer(scheduler: Scheduler) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let start_time = Instant::now();
        while start_time.saturating_elapsed() < TIMEOUT {
            sleep(Duration::from_millis(100));
            if scheduler.transfer_random_leader() {
                TRANSFER_COUNTER.fetch_add(1, Ordering::SeqCst);
            }
        }
        info!("transfer thread exit");
    })
}

pub(crate) fn spawn_gc_worker(
    client: ClusterTxnClient,
    pd_client: Arc<TestPdClient>,
    keyspace_manager: KeyspaceManager,
    timeout: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let semaphore = Arc::new(Semaphore::new(KEYSPACE_CLEANUP_LOCKS_CONCURRENCY));
        let start_time = Instant::now();
        while start_time.saturating_elapsed() < timeout {
            let safepoint = client.current_timestamp().await.unwrap();
            let all_keyspaces = keyspace_manager.get_all_keyspaces();

            tokio::time::sleep(GC_INTERVAL).await;
            info!(
                "gc_worker: try advance safepoint to {}",
                safepoint.version()
            );

            let mut handles = Vec::with_capacity(all_keyspaces.len());
            for &keyspace_id in &all_keyspaces {
                let start = ApiV2::get_txn_keyspace_prefix(keyspace_id);
                let end = ApiV2::get_txn_keyspace_prefix(keyspace_id + 1);

                let semaphore = semaphore.clone();
                let inner_client = client.inner.clone();
                let safepoint = safepoint.clone();
                let h = tokio::spawn(async move {
                    let permit = semaphore.acquire().await.unwrap();
                    let res = tokio::time::timeout(
                        KEYSPACE_CLEANUP_LOCKS_TIMEOUT,
                        inner_client.cleanup_locks(start..end, &safepoint, Default::default()),
                    )
                    .await;
                    drop(permit);

                    match res {
                        Ok(Ok(res)) => {
                            // Should not have any error in `res`.
                            assert!(
                                res.key_error.is_none() && res.region_error.is_none(),
                                "unexpected CleanupLocksResult, check client rust: key_error {:?}, region_error {:?}, keyspace_id {}",
                                res.key_error,
                                res.region_error,
                                keyspace_id,
                            );
                            info!("gc_worker: cleanup_locks succeed"; "resolved_locks" => res.resolved_locks, "safepoint" => safepoint.version(), "keyspace_id" => keyspace_id);
                            true
                        }
                        Ok(Err(err)) => {
                            error!("gc_worker: cleanup_locks failed"; "err" => ?err, "safepoint" => safepoint.version(), "keyspace_id" => keyspace_id);
                            false
                        }
                        Err(_elapsed) => {
                            // Should not timeout for 30s.
                            // TODO: inspect the reason.
                            warn!("gc_worker: cleanup_locks timeout"; "safepoint" => safepoint.version(), "keyspace_id" => keyspace_id);
                            false
                        }
                    }
                });
                handles.push(h);
            }
            let results = futures::future::join_all(handles).await;
            if results.into_iter().all(|r| r.unwrap()) {
                let new_safepoint = pd_client.set_gc_safe_point(safepoint.version()).unwrap();
                assert!(new_safepoint <= safepoint.version());
                if new_safepoint == safepoint.version() {
                    info!("gc_worker: advance safepoint to {}", safepoint.version());
                    GC_ADVANCE_SAFE_POINT_COUNTER.fetch_add(1, Ordering::SeqCst);
                } else {
                    warn!("gc_worker: update safepoint failed"; "safepoint" => safepoint.version());
                }

                for keyspace_id in all_keyspaces {
                    test_drop_table::pick_and_run_pending_destroy_range(
                        keyspace_id,
                        new_safepoint,
                        &keyspace_manager,
                        &client,
                    )
                    .await
                    .unwrap();
                }
            }
        }
        info!("gc_worker thread exit");
    })
}

pub(crate) fn check_gc() {
    // TODO: check times of GC after address the cleanup_locks timeout issue.
    // let counter = GC_ADVANCE_SAFE_POINT_COUNTER.load(Ordering::SeqCst);
    // assert!(
    //     counter > 0,
    //     "gc_worker: times of safe point advanced less than expected, actual
    // {}",     counter,
    // );
}

pub(crate) fn spawn_major_compact(
    pd_client: Arc<TestPdClient>,
    keyspace_manager: KeyspaceManager,
    timeout: Duration,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut rng = rand::thread_rng();
        let security_mgr = pd_client.get_security_mgr();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("major-compact")
            .build()
            .unwrap();

        let start_time = Instant::now();
        while start_time.saturating_elapsed() < timeout {
            // Manual major compact should not conform the zipf distribution.
            let keyspace_id = keyspace_manager.get_uniform_random_keyspace(&mut rng);

            let stores = pd_client.get_all_stores(true).unwrap();
            {
                // To be mutual-exclusive with load_data.
                let lock = keyspace_manager.get_keyspace_lock(keyspace_id);
                let _guard = lock.extra_lock();

                let mut handles = Vec::with_capacity(stores.len());
                for store in stores {
                    let security_mgr = security_mgr.clone();
                    handles.push(runtime.spawn(request_major_compact_on_store(
                        security_mgr,
                        store.clone(),
                        keyspace_id,
                        false,
                    )));
                }
                for handle in handles {
                    runtime.block_on(handle).unwrap().unwrap();
                }
            }

            MANUAL_MAJOR_COMPACT_COUNTER.fetch_add(1, Ordering::SeqCst);
            sleep(Duration::from_secs(10));
        }
        info!("major compact worker thread exit");
    })
}

pub(crate) async fn request_major_compact_on_store(
    security_mgr: Arc<SecurityManager>,
    store: Store,
    keyspace_id: u32,
    columnar: bool,
) -> Result<()> {
    let uri = security_mgr
        .build_uri(format!(
            "{}/major-compact?major_compact=true&keyspace_id={}&columnar={}",
            &store.status_address, keyspace_id, columnar
        ))
        .unwrap();
    let req = || Request::post(&uri).body(Body::empty()).unwrap();
    match send_request_to_store_with_retry(
        req,
        &store,
        security_mgr.as_ref(),
        REQUEST_MAJOR_COMPACT_ON_STORE_TIMEOUT,
    )
    .await
    {
        Ok(resp) => {
            let msg = String::from_utf8_lossy(&resp);
            info!(
                "{} request_major_compact_on_store success: {}",
                keyspace_id,
                msg.as_ref()
            );
            Ok(())
        }
        Err(HttpError(status, msg)) if status == StatusCode::NOT_FOUND => {
            info!(
                "{} request_major_compact_on_store: region not found", keyspace_id;
                "msg" => msg
            );
            Ok(())
        }
        Err(err) => Err(box_err!(
            "{} request_major_compact_on_store failed: {:?}",
            keyspace_id,
            err
        )),
    }
}

pub(crate) fn spawn_keyspace_write(
    idx: usize,
    client: ClusterKeyspaceClient,
    timeout: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Make sure each write thread don't conflict with others.
        let begin = idx * 2000;
        let end = begin + 2000 - 10;
        let start_time = Instant::now();
        while start_time.saturating_elapsed() < timeout {
            let random_keyspace = || {
                let mut rng = rand::thread_rng();
                client.keyspace_manager().get_zipf_random_keyspace(&mut rng)
            };
            let keyspace_id = random_keyspace();
            {
                let lock = client.keyspace_manager().get_keyspace_lock(keyspace_id);
                let guard = lock.try_shared_lock();
                if guard.is_none() {
                    continue;
                }
                let _guard = guard.unwrap();

                let random = || {
                    let mut rng = rand::thread_rng();
                    let table_meta = client.keyspace_manager().get_random_available_table(
                        keyspace_id,
                        &mut rng,
                        true,
                    );
                    let i = rng.gen_range(begin..end);
                    let put_kv = rng.gen_ratio(2, 3);
                    (table_meta, i, put_kv)
                };
                let (table_meta, i, put_kv) = random();
                let table_id = match table_meta.as_ref() {
                    Some(table_meta) => table_meta.id(),
                    None => continue,
                };
                let is_schema_enabled = table_meta.unwrap().is_schema_enabled();

                let ver = client.current_timestamp().await.unwrap().version();
                info!(
                    "[{}] thread write on keyspace {} table {} schema_enabled {}, ver {}",
                    idx, keyspace_id, table_id, is_schema_enabled, ver
                );

                if put_kv {
                    client
                        .keyspace_put_kv(
                            keyspace_id,
                            table_id,
                            i..(i + 10),
                            gen_row_key_suffix,
                            |i| gen_row_val(&mut EvalContext::default(), i, ver),
                        )
                        .await
                        .unwrap();
                } else {
                    client
                        .keyspace_del_kv(keyspace_id, table_id, i..(i + 10), gen_row_key_suffix)
                        .await
                        .unwrap();
                }
            }
            WRITE_COUNTER.fetch_add(10, Ordering::SeqCst);
        }
        info!("keyspace write thread {} exit", idx);
    })
}

const OSS_CHAOS_INTERVAL: Duration = Duration::from_secs(10);

pub(crate) fn spawn_oss_chaos(
    oss: &ObjectStorageService,
    interval: Duration,
    running: Running,
) -> JoinHandle<()> {
    let write_limiter = oss.write_limiter();
    let origin_rate = write_limiter.get_io_rate_limit();
    let rates = [
        origin_rate / 10,
        origin_rate / 4,
        origin_rate / 2,
        origin_rate,
        origin_rate * 2,
    ];
    std::thread::spawn(move || {
        let mut rng = thread_rng();
        while running.get() {
            let interval_secs = (1..=interval.as_secs()).choose(&mut rng).unwrap();
            thread::sleep(Duration::from_secs(interval_secs));

            let rate = *rates.choose(&mut rng).unwrap();
            write_limiter.set_io_rate_limit(rate);
            info!("spawn_oss_chao: set write rate {} bytes/sec", rate);
        }

        write_limiter.set_io_rate_limit(origin_rate);
    })
}

pub(crate) fn spawn_dfs_unhealthy_chaos(
    store_id: u64,
    interval: Duration,
    timeout: Duration,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let put_wal_chunk_error_fp = "dfs_worker_put_wal_chunk_error";
        let unhealthy_fp = "dfs_worker_set_unhealthy";
        let healthy_fp = "dfs_worker_recover_healthy";

        let (unhealthy_tx, unhealthy_rx) = mpsc::unbounded();
        let (healthy_tx, healthy_rx) = mpsc::unbounded();

        set_dfs_worker_failpoint_target_store_id(store_id);

        fail::cfg_callback(unhealthy_fp, move || {
            let _ = unhealthy_tx.send(());
        })
        .unwrap();
        fail::cfg_callback(healthy_fp, move || {
            let _ = healthy_tx.send(());
        })
        .unwrap();

        let mut rng = thread_rng();
        let max_ms = interval.as_millis().max(1) as u64;
        let start_time = Instant::now_coarse();
        while start_time.saturating_elapsed() < timeout {
            let wait_ms = rng.gen_range(1..=max_ms);
            sleep(Duration::from_millis(wait_ms));

            fail::cfg(put_wal_chunk_error_fp, "1*return").unwrap();

            match unhealthy_rx.recv_timeout(Duration::from_secs(30)) {
                Ok(()) => {
                    let count = DFS_UNHEALTHY_COUNTER.fetch_add(1, Ordering::SeqCst) + 1;
                    info!(
                        "spawn_dfs_unhealthy_chaos: store {} unhealthy triggered {}",
                        store_id, count
                    );
                }
                Err(err) => {
                    warn!(
                        "spawn_dfs_unhealthy_chaos: unhealthy wait timeout: {:?}",
                        err
                    );
                    continue;
                }
            }

            match healthy_rx.recv_timeout(Duration::from_secs(60)) {
                Ok(()) => {
                    info!(
                        "spawn_dfs_unhealthy_chaos: store {} recovered healthy",
                        store_id
                    );
                }
                Err(err) => {
                    warn!("spawn_dfs_unhealthy_chaos: healthy wait timeout: {:?}", err);
                }
            }
        }

        fail::remove(put_wal_chunk_error_fp);
        fail::remove(unhealthy_fp);
        fail::remove(healthy_fp);
        set_dfs_worker_failpoint_target_store_id(0);

        info!("spawn_dfs_unhealthy_chaos thread exit");
    })
}

pub fn spawn_create_keyspace(
    pd_client: Arc<TestPdClient>,
    keyspace_manager: KeyspaceManager,
    fs: Arc<dyn Dfs>,
    initial_table_count: usize,
    schema_enable_ratio: f64,
    timeout: Duration,
) -> JoinHandle<()> {
    let fs = fs.clone();
    std::thread::spawn(move || {
        let mut rng = rand::thread_rng();
        let start_time = Instant::now();
        while start_time.saturating_elapsed() < timeout {
            sleep(Duration::from_secs(rng.gen_range(0..5)));

            let _ = fs.get_runtime().block_on(create_new_keyspace(
                &pd_client,
                &keyspace_manager,
                fs.as_ref(),
                initial_table_count,
                schema_enable_ratio,
                false,
            ));
        }
        info!("create keyspace thread exit");
    })
}

async fn create_new_keyspace(
    pd_client: &Arc<TestPdClient>,
    keyspace_manager: &KeyspaceManager,
    fs: &dyn Dfs,
    initial_table_count: usize,
    schema_enable_ratio: f64,
    locked: bool,
) -> (u32, Option<OwnedRwLockWriteGuard<()>>) {
    let random = || -> (u32, bool) {
        let mut rng = thread_rng();
        let delta = rng.gen_range(1..=3);
        let enable_encryption = rng.gen();
        (delta, enable_encryption)
    };
    let (delta, enable_encryption) = random();

    let new_keyspace = keyspace_manager.new_keyspace_id(delta);
    must_split_region_for_keyspace(pd_client, new_keyspace, enable_encryption).await;
    // `create_single_keyspace` do not shuffle keyspaces. Then we will have a few
    // big keyspaces.
    let lock = keyspace_manager
        .create_single_keyspace(
            new_keyspace,
            TidbCluster::keyspace_name(new_keyspace as u16),
            &CreateKeyspaceOptions {
                table_count: initial_table_count,
                schema_enable_ratio,
                ..Default::default()
            },
            locked,
        )
        .await;
    // Setup schema file to stores
    // TODO: support tables with schema enabled created dynamically.
    let keyspace_meta = keyspace_manager.get_keyspace_meta(new_keyspace).unwrap();
    let stores = pd_client.get_all_stores(true).unwrap();
    if !keyspace_meta.schemas().is_empty() {
        let schema_version = 10;
        let schema_data: Bytes =
            build_schema_file(new_keyspace, schema_version, keyspace_meta.schemas(), 0).into();
        let schema_file_id = pd_client.alloc_id().unwrap();
        fs.create(
            schema_file_id,
            schema_data.clone(),
            dfs::Options::default().with_type(FileType::Schema),
        )
        .await
        .unwrap();
        let schema_file =
            SchemaFile::open(Arc::new(InMemFile::new(schema_file_id, schema_data))).unwrap();
        // Setup schema file to stores.
        broadcast_schema_file_request_and_check(
            &stores,
            new_keyspace,
            &schema_file,
            Duration::from_secs(30),
        )
        .await;
    }
    KEYSPACE_COUNTER.fetch_add(1, Ordering::Relaxed);
    TABLE_COUNTER.fetch_add(initial_table_count, Ordering::Relaxed);

    (new_keyspace, lock)
}

pub fn gen_row_key_suffix(i: usize) -> Vec<u8> {
    let mut suffix = vec![];
    suffix.write_i64(i as i64).unwrap();
    suffix
}

pub fn gen_row_val(ctx: &mut EvalContext, i: usize, ver: u64) -> Vec<u8> {
    let mut row_val = vec![];
    let str_val = generate_random_string(format!("write-{}-", ver));
    let cols = vec![
        Column::new(1, Some(i as i64)),
        Column::new(2, Some(str_val(10))),
    ];
    row_val.write_row(ctx, cols).unwrap();
    row_val
}

async fn must_split_region_for_keyspace(
    pd_client: &TestPdClient,
    keyspace_id: u32,
    enable_encryption: bool,
) {
    let keys = [
        ApiV2::get_txn_keyspace_prefix(keyspace_id),
        ApiV2::get_txn_keyspace_prefix(keyspace_id + 1),
    ];
    if enable_encryption {
        let cfg = KeyspaceEncryptionConfig { enabled: true };
        pd_client.set_keyspace_encryption(keyspace_id, cfg).unwrap();
    }
    let split_keys = keys
        .iter()
        .map(|k| Key::from_raw(k).into_encoded())
        .collect::<Vec<_>>();
    pd_client
        .split_regions_with_retry(split_keys, Duration::from_secs(60))
        .await
        .unwrap();
    let new_region = pd_client.get_region(&keys[0]).unwrap();
    info!(
        "split region for keyspace {}: {:?}",
        keyspace_id, new_region
    );
}

pub(crate) fn random_node_restart<F>(
    cluster: &mut ServerCluster,
    update_conf: F,
    always_force_stop: bool,
) where
    F: Fn(u16, &mut TikvConfig),
{
    let mut rng = rand::thread_rng();

    // Some regions would lose majority for a wile when the sleep duration is small.
    // Data corruption should not happen, and workloads should tolerate this.
    sleep(Duration::from_secs(rng.gen_range(3..17)));

    let nodes = cluster.get_nodes();
    let node_id = *nodes.choose(&mut rng).unwrap();
    let sleep_sec = rng.gen_range(0..3);
    let force_stop = always_force_stop || rng.gen();
    cluster.restart_node(
        node_id,
        Duration::from_secs(sleep_sec),
        force_stop,
        update_conf,
    );
    NODE_RESTART_COUNTER.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn i_to_key(i: usize) -> Vec<u8> {
    format!("tkey{:08}", i).into_bytes()
}

pub(crate) fn generate_random_string(prefix: String) -> impl Fn(usize) -> Vec<u8> {
    move |i: usize| -> Vec<u8> {
        let mut val = prefix.as_bytes().to_vec();
        let n = i % 512 + 1;
        val.reserve(n);
        val.extend(
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(n),
        );
        val
    }
}

pub(crate) fn new_security_config() -> SecurityConfig {
    let mut conf = SecurityConfig::default();
    conf.master_key.vendor = "test".to_string();
    conf.master_key.key_id = "random".to_string();
    conf
}

// Switch acquired from env variable, `0` for false, `>0` for true.
pub(crate) fn env_switch(env_key: &str) -> bool {
    env_switch_opt(env_key, 1)
}

pub(crate) fn env_switch_opt(env_key: &str, default: i32) -> bool {
    std::env::var(env_key)
        .map(|s| s.parse().unwrap())
        .unwrap_or(default)
        > 0
}

pub(crate) fn env_param<T>(env_key: &str, default: T) -> T
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Debug,
{
    std::env::var(env_key)
        .map(|s| s.parse::<T>().unwrap())
        .unwrap_or(default)
}

pub(crate) fn verify_cluster_stats(cluster: &ServerCluster, bucket_size: u64, timeout: Duration) {
    let mut success_shards = HashSet::new();
    try_wait_result(
        || {
            let stats = cluster.get_data_stats_ext(Some(&success_shards));
            stats.check_data().map_err(|(errs, this_success_shards)| {
                success_shards.extend(this_success_shards);
                (errs, stats)
            })
        },
        timeout.as_secs() as usize,
    )
    .unwrap_or_else(|(err, stats)| {
        stats.log_all();
        panic!("check_data failed: {:?}", err);
    });

    let mut starts_from_encoded_key = vec![];
    try_wait_result(
        || {
            let data_stats = cluster.get_data_stats();
            let res = data_stats.check_buckets(
                cluster.get_pd_client_ext().as_ref(),
                bucket_size,
                &starts_from_encoded_key,
            );
            if let Err((reason, region)) = &res {
                warn!(
                    "check_buckets failed, region {:?}, reason {:?}",
                    region, reason
                );
                if let Some(region) = region {
                    starts_from_encoded_key = region.start_key.clone();
                }
            }
            res.map_err(|err| (err, data_stats))
        },
        timeout.as_secs() as usize,
    )
    .unwrap_or_else(|(err, stats)| {
        stats.log_all();
        panic!("check_buckets failed: {:?}", err);
    });
}

fn try_verify_region_info_accessor(
    cluster: &ServerCluster,
    node_id: u16,
) -> std::result::Result<(), String> {
    let accessor = cluster.get_region_info_accessor(node_id);
    let kvengine = cluster.get_kvengine(node_id);
    let id_vers = kvengine.get_all_shard_id_vers();
    let map_len = accessor.map_len();
    let skl_len = accessor.skl_len();
    if id_vers.len() != map_len || id_vers.len() != skl_len {
        return Err(format!(
            "region count not match (kvengine {}, map {}, skiplist {})",
            id_vers.len(),
            map_len,
            skl_len
        ));
    }

    for id_ver in id_vers {
        let shard = kvengine
            .get_shard(id_ver.id)
            .ok_or_else(|| format!("shard {} not found", id_ver))?;
        let region = accessor.find_region_by_id(id_ver.id).ok_or_else(|| {
            format!(
                "region {} not found in region info accessor on node {}",
                id_ver, node_id
            )
        })?;
        let region_version = region.region.get_region_epoch().get_version();
        if shard.ver != region_version {
            return Err(format!(
                "region {} version mismatch (shard {}, accessor {})",
                id_ver, shard.ver, region_version
            ));
        }
        if shard.is_active() != region.is_leader() {
            return Err(format!(
                "region {} leader flag mismatch (shard_active {}, is_leader {})",
                id_ver,
                shard.is_active(),
                region.is_leader()
            ));
        }
        let encoded_start_key = encode_bytes(&shard.outer_start);
        let region2 = accessor
            .find_region_by_key(&encoded_start_key)
            .map_err(|err| {
                format!(
                    "region for start key {:?} not found in region info accessor on node {}: {}",
                    shard.outer_start, node_id, err
                )
            })?;
        if shard.id != region2.id {
            return Err(format!(
                "region mismatch for start key {:?} shard {} got {}",
                shard.outer_start, shard.id, region2.id
            ));
        }
    }
    Ok(())
}

pub(crate) fn verify_region_info_accessor(cluster: &ServerCluster) {
    let nodes = cluster.get_nodes();
    for &node_id in &nodes {
        must_wait_result(
            || try_verify_region_info_accessor(cluster, node_id),
            30,
            || format!("verify_region_info_accessor failed on node {}", node_id),
        );
    }
}

pub(crate) fn verify_rfengine_keyspace_id(cluster: &ServerCluster) {
    let nodes = cluster.get_nodes();
    for &node_id in &nodes {
        let raft = cluster.get_rfengine(node_id);
        let region_peer_map = raft.get_region_peer_map();
        let kvengine = cluster.get_kvengine(node_id);
        let id_vers = kvengine.get_all_shard_id_vers();
        for id_ver in id_vers {
            let shard = kvengine.get_shard(id_ver.id).unwrap();
            let peer_id = region_peer_map.get(&id_ver.id).cloned().unwrap();
            let peer_stat = raft.get_peer_stats(peer_id);
            assert_eq!(
                peer_stat.keyspace_id,
                shard.keyspace_id,
                "shard {}:{}",
                raft.get_engine_id(),
                id_ver
            );
        }
    }
}

#[derive(Clone)]
pub(crate) struct Running(Arc<AtomicBool>);

impl Running {
    fn new_start() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }

    pub fn stop(&self) {
        self.0.store(false, Ordering::Release);
    }

    pub fn get(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

const LOG_MAX_SIZE_MB: u64 = 150; // Can be uploaded to GitHub after compressed.
const LOG_MAX_BACKUPS: usize = 20;

pub(crate) fn init_logger() {
    let filename = std::env::var("LOG_FILE")
        .map(|s| PathBuf::from(s))
        .unwrap_or_else(|_| std::env::temp_dir().join("tikv.log"));
    let log_config = tikv::config::LogConfig {
        file: tikv::config::File {
            filename: filename.to_str().unwrap().to_owned(),
            max_size: LOG_MAX_SIZE_MB,
            max_backups: LOG_MAX_BACKUPS,
            ..Default::default()
        },
        ..Default::default()
    };
    let tikv_config = TikvConfig {
        log: log_config,
        ..Default::default()
    };
    cloud_server::setup::initial_logger(&tikv_config);
}

#[cfg(feature = "env-logger")]
fn get_log_level() -> slog::Level {
    get_log_level_from_rust_log()
}

#[cfg(any(feature = "env-logger", test))]
fn get_log_level_from_rust_log() -> slog::Level {
    // Critical < Error < Warning < Info < Debug < Trace.
    let mut max_level = None;
    if let Ok(log_env) = std::env::var("RUST_LOG") {
        // E.g., "info,raft=debug"
        for part in log_env.split(',') {
            let level = part.split('=').next_back().unwrap_or(part);
            if let Ok(level) = level.parse::<slog::Level>() {
                if max_level.is_none_or(|x| x < level) {
                    max_level = Some(level);
                }
            }
        }
    }
    max_level.unwrap_or(slog::Level::Info)
}

#[cfg(not(feature = "env-logger"))]
fn get_log_level() -> slog::Level {
    slog::Level::Info
}

pub(crate) fn test_id() -> String {
    std::env::var("TEST_ID").unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_log_level() {
        let cases = vec![
            ("info", slog::Level::Info),
            ("warn", slog::Level::Warning),
            ("info,raft=debug", slog::Level::Debug),
            ("error,raft=warn,tikv=info", slog::Level::Info),
        ];
        for (rust_log, expected_level) in cases {
            std::env::set_var("RUST_LOG", rust_log);
            assert_eq!(get_log_level_from_rust_log(), expected_level);
        }
    }
}
