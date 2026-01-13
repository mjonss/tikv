// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::Write as _,
    path::Path,
    sync::{Arc, RwLock, atomic::Ordering},
    time::Duration,
};

use api_version::ApiV2;
use cloud_encryption::KeyspaceEncryptionConfig;
use futures::executor::block_on;
use kvengine::{
    dfs::{self, DFSConfig, DFSConnOptions, FileType, S3Fs},
    ia::util::IaConfig,
    metrics::{ENGINE_IA_SYNC_READ_COUNTER, ENGINE_REMOTE_COMPACT_EXCEED_MEMORY_LIMIT_COUNTER},
    table::{ChecksumType, schema_file::build_schema_file},
};
use kvproto::pdpb::CheckPolicy;
use load_data::task::LoadDataConfig;
use native_br::{backup, backup_worker, restore::RestoreConfig, rfengine_cache::RfEngineCache};
use pd_client::PdClient;
use rand::prelude::*;
use rfengine::RFENGINE_DFS_WORKER_BECOME_UNHEALTHY_COUNTER;
use schema::schema::{StorageClass, StorageClassSpec};
use security::SecurityConfig;
use test_cloud_server::{
    IA_DISK_CAP_DEF, IA_FREQ_UPDATE_INTERVAL_DEF, IA_MEM_CAP_DEF, ServerCluster,
    ServerClusterBuilder, TikvWorkerOptions,
    client::ClusterClientOptions,
    keyspace::make_row_key,
    oss::{ObjectStorageService, prepare_builtin_dfs, prepare_dfs},
    tidb::TidbCluster,
    util::broadcast_schema_file_request_and_check,
};
use test_pd_client::{PdClientExt, PdWrapper};
use tikv::server::config::GrpcCompressionType;
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    info,
    sys::SysQuota,
    time::Instant,
    warn,
};
use txn_types::Key;

use crate::{
    TikvConfig, test_drop_table::*, test_load_data::*, test_native_br::*, test_txn_file::*, *,
};

const MAX_IN_MEM_SIZE: usize = 10 * 1024; // 10KiB
const FLUSH_BATCH_SIZE: usize = 4 * 1024; // 4KiB

const INITIAL_KEYSPACE_COUNT: usize = 10;
const BIG_REGION_SIZE_KEYSPACE_COUNT: usize = 2;
const BIG_REGION_SIZE_FACTOR_OPTIONS: &[f64] = &[2.0, 3.0, 4.0];
const INITIAL_TABLE_COUNT: usize = 3;
const TABLE_SCHEMA_ENABLE_RATIO: f64 = 0.5;

const NODES_COUNT: usize = 4;
const TIKV_WORKERS_COUNT: usize = 2;

const RESTORE_CONCURRENCY: usize = 2;
const LOAD_DATA_CONCURRENCY: usize = 2;
const PERIODIC_BACKUP_INTERVAL: Duration = Duration::from_secs(4);
const BACKUP_BATCH_INTERVAL: Duration = Duration::from_secs(2);

const KV_TARGET_FILE_SIZE: ReadableSize = ReadableSize::kb(16);
const REGION_BUCKET_SIZE: ReadableSize = ReadableSize::kb(64);

const COP_BLOCK_CACHE_SIZE: ReadableSize = ReadableSize::mb(4); // Small size to make eviction more frequent.

#[test]
fn test_random_all() {
    test_random_all_impl(false);
}

#[test]
fn test_random_builtin_all() {
    test_random_all_impl(true);
}

fn test_random_all_impl(use_builtin_dfs: bool) {
    init_logger();
    let prepare_time = Instant::now_coarse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .thread_name("random-workload")
        .build()
        .unwrap();
    let _guard = runtime.enter();

    let mut switches = Switches::from_env();
    if use_builtin_dfs {
        switches.ia_table_ratio = 0 as f64;
    }
    info!("switches: {:?}", switches);

    // Prepare.
    let (temp_dir, oss, dfs_config) = if use_builtin_dfs {
        prepare_builtin_dfs("builtin_")
    } else {
        prepare_dfs("oss_")
    };
    let security_conf = new_security_config();
    let mut cluster = prepare_cluster(
        dfs_config.clone(),
        &security_conf,
        NODES_COUNT,
        INITIAL_KEYSPACE_COUNT,
        &switches,
        &oss,
        use_builtin_dfs,
    );
    let pd_client = cluster.get_pd_client();
    let keyspace_manager = cluster.keyspace_manager().clone();

    let load_data_config = {
        let tikv_config = cluster.get_node_config(cluster.get_nodes()[0]);
        let region_size = tikv_config.coprocessor.region_split_size.0 as usize;
        LoadDataConfig {
            kvpairs_worker_num: 2,
            building_worker_num: 2,
            max_in_mem_size: MAX_IN_MEM_SIZE,
            flush_batch_size: FLUSH_BATCH_SIZE,
            block_size: tikv_config.rocksdb.writecf.block_size.0 as usize,
            sst_file_size: tikv_config.rocksdb.writecf.target_file_size_base.0 as usize,
            region_size,
            coarse_split_size: region_size * 4,
            enable_checkpoint: false,
            rg_config: None,
            checksum_type: ChecksumType::Crc32,
        }
    };

    // Set the first service safe point for backup before starting workloads.
    let ts = block_on(pd_client.get_tso()).unwrap();
    backup::update_service_safe_point(pd_client.as_ref(), ts.into_inner()).unwrap();

    let dfs_conf = dfs_config.clone();
    let s3fs = S3Fs::new_from_config(dfs_conf);

    // Start workloads & schedulers.
    let running = Running::new_start();
    let mut handles = vec![
        spawn_merge(cluster.new_scheduler(), true),
        spawn_transfer(cluster.new_scheduler()),
        spawn_move(cluster.new_scheduler(), Arc::new(RwLock::new(()))),
    ];

    if !use_builtin_dfs {
        handles.push(spawn_create_keyspace(
            cluster.get_pd_client(),
            keyspace_manager.clone(),
            &s3fs,
            INITIAL_TABLE_COUNT,
            TABLE_SCHEMA_ENABLE_RATIO,
            TIMEOUT,
        ))
    }
    handles.push(spawn_major_compact(
        cluster.get_pd_client(),
        keyspace_manager.clone(),
        TIMEOUT,
    ));

    let restore_config = RestoreConfig {
        dfs: dfs_config.clone(),
        security: security_conf.clone(),
        timeout_wait_flush: ReadableDuration::minutes(3), /* Flush is slow when OSS has low rate
                                                           * limit. */
        timeout_restore_snapshot: ReadableDuration::secs(30),
        timeout_fetch_wal: ReadableDuration::secs(10),
        tolerate_err: 1,
        strict_tolerate: true,
        max_retry: 20,
        lower_memory: switches.restore_lower_memory,
        ..Default::default()
    };
    let rfengine_cache_dir = temp_dir.path().join("rfengine_cache");
    let rfengine_cache = if switches.enable_rfengine_cache {
        Some(RfEngineCache::new(
            rfengine_cache_dir,
            restore_config.clone(),
            Arc::new(s3fs.clone()),
            pd_client.clone(),
        ))
    } else {
        None
    };
    if !use_builtin_dfs {
        let object_cache = cluster.create_object_cache_randomly();
        let limiter = cluster.create_restore_limiter_randomly(runtime.handle().clone());
        for _ in 0..RESTORE_CONCURRENCY {
            handles.push(spawn_restore_keyspace(
                cluster.get_pd_client(),
                runtime.block_on(cluster.new_keyspace_client()),
                restore_config.clone(),
                keyspace_manager.clone(),
                &s3fs,
                switches.enable_oss_chaos,
                object_cache.clone(),
                limiter.clone(),
                TIMEOUT,
                rfengine_cache.clone(),
            ));
        }
        let load_data_task_timeout =
            Duration::from_secs(env_param("LOAD_DATA_TASK_TIMEOUT_SEC", 90));
        for _i in 0..LOAD_DATA_CONCURRENCY {
            let load_data_config = load_data_config.clone();
            handles.push(spawn_load_data(
                cluster.get_pd_client(),
                runtime.block_on(cluster.new_keyspace_client()),
                dfs_config.clone(),
                security_conf.clone(),
                load_data_config.clone(),
                keyspace_manager.clone(),
                load_data_task_timeout,
                Duration::from_secs(15),
                TIMEOUT,
            ));
        }
    }

    let mut async_handles = vec![];
    if !use_builtin_dfs {
        let backup_config = backup::BackupConfig {
            dfs: dfs_config.clone(),
            backup_delay: ReadableDuration::secs(1),
            tolerate_err: 1,
            skip_keyspace_meta: true,
            ..Default::default()
        };
        let backup_worker = {
            Arc::new(backup_worker::BackupWorker::new(
                backup_config.clone(),
                pd_client.clone(),
                PERIODIC_BACKUP_INTERVAL,
                BACKUP_BATCH_INTERVAL,
            ))
        };
        async_handles.push(spawn_backup(
            cluster.new_client(),
            keyspace_manager.clone(),
            backup_config,
            backup_worker,
            &s3fs,
            Duration::from_secs(5),
            TIMEOUT,
        ));
    }
    async_handles.push(spawn_gc_worker(
        runtime.block_on(cluster.new_txn_client()),
        pd_client.clone(),
        keyspace_manager.clone(),
        TIMEOUT,
    ));

    for _ in 0..DROP_TABLE_CONCURRENCY {
        async_handles.push(spawn_drop_table(
            runtime.block_on(cluster.new_keyspace_client()),
            Duration::from_secs(10),
            TIMEOUT,
        ));
    }

    // Write workloads:
    for idx in 0..WRITE_CONCURRENCY {
        async_handles.push(spawn_keyspace_write(
            idx,
            runtime.block_on(cluster.new_keyspace_client()),
            TIMEOUT,
        ));
    }
    if !use_builtin_dfs {
        for i in 0..TXN_FILE_WRITE_CONCURRENCY {
            handles.push(spawn_txn_file_write(
                WRITE_CONCURRENCY,
                i,
                cluster.new_client_opt(ClusterClientOptions {
                    txn_file_max_chunk_size: Some(TXN_CHUNK_MAX_SIZE),
                    ..Default::default()
                }),
                keyspace_manager.clone(),
                TIMEOUT,
            ));
        }
    }

    if switches.enable_oss_chaos && !use_builtin_dfs {
        handles.push(spawn_oss_chaos(&oss, OSS_CHAOS_INTERVAL, running.clone()))
    }

    // Main loop.
    let start_time = Instant::now();
    while start_time.saturating_elapsed() < TIMEOUT {
        // Restart nodes.
        // Always force stop when OSS chaos is enabled. Otherwise, it would take too
        // long to stop compact worker.
        random_node_restart(&mut cluster, |_, _| {}, switches.enable_oss_chaos);
    }

    // Finish.
    info!("test finished, stopping all workers");
    running.stop();
    for handle in handles {
        handle.join().unwrap();
    }
    runtime.block_on(async move {
        for handle in async_handles {
            handle.await.unwrap();
        }
    });

    info!("run all pending destroy range");
    let txn_client = runtime.block_on(cluster.new_txn_client());
    let all_keyspaces = keyspace_manager.get_all_keyspaces();
    for keyspace_id in all_keyspaces {
        runtime
            .block_on(pick_and_run_pending_destroy_range(
                keyspace_id,
                u64::MAX,
                &keyspace_manager,
                &txn_client,
            ))
            .unwrap();
    }
    // Read requests can still get data before destroy range is done by compaction.
    // So must wait for destroy range to finish before verifying.
    info!("wait destroy range");
    cluster.wait_for_destroy_range(Duration::from_secs(30));

    // Verify.
    info!("verify cluster");
    let verified_records_count = runtime.block_on(verify_cluster(&mut cluster, use_builtin_dfs));

    // Stop cluster.
    info!("stopping cluster");
    cluster.stop();

    // Statistics.
    let stats = WorkloadStats::collect();
    let region_number = pd_client.get_regions_number();
    let stdout = std::io::stdout();
    writeln!(
        stdout.lock(),
        "{} TEST SUCCEED: elapsed {:?},{:?}, region {}, verified_records {}, {:?}",
        test_id(),
        prepare_time.saturating_elapsed(),
        start_time.saturating_elapsed(),
        region_number,
        verified_records_count,
        stats,
    )
    .unwrap();
    stdout.lock().flush().unwrap();
}

fn prepare_cluster(
    mut dfs_config: DFSConfig,
    security_conf: &SecurityConfig,
    nodes_count: usize,
    initial_keyspace_count: usize,
    switches: &Switches,
    oss: &ObjectStorageService,
    use_builtin_dfs: bool,
) -> ServerCluster {
    let mut rng = rand::thread_rng();
    let nodes = alloc_node_id_vec(nodes_count);
    let cpu_cores = SysQuota::cpu_cores_quota() as usize;

    let big_region_size_keyspaces = (0..initial_keyspace_count as u32)
        .choose_multiple(&mut rng, BIG_REGION_SIZE_KEYSPACE_COUNT);
    let per_keyspace_configs = big_region_size_keyspaces
        .into_iter()
        .map(|keyspace| kvengine::config::PerKeyspaceConfig {
            keyspace,
            split_size_factor: *BIG_REGION_SIZE_FACTOR_OPTIONS
                .iter()
                .choose(&mut rng)
                .unwrap(),
            ..Default::default()
        })
        .collect::<Vec<_>>();

    let mut dfs_max_retry_count = 9;
    let mut dfs_dispatch_timeout = ReadableDuration::secs(15);
    let mut rfengine_target_file_size = ReadableSize::mb(8);
    let mut dfs_worker_memory_limit = ReadableSize::mb(32);
    if switches.enable_oss_chaos {
        match rng.gen_range(0..=3) {
            0 => {
                // To cover the scene of DFS worker exceed memory limit & get compacted WAL.
                rfengine_target_file_size = ReadableSize::kb(512);
                dfs_worker_memory_limit = ReadableSize::kb(512);
            }
            1 => {
                // To cover the scene of DFS put object failure.
                dfs_max_retry_count = 2;
                dfs_dispatch_timeout = ReadableDuration::secs(3);
            }
            _ => {}
        }
    }

    // TODO: use same `conn_options` for other workloads to cover scene of slow DFS.
    dfs_config.conn_options = DFSConnOptions {
        max_retry_count: dfs_max_retry_count,
        retry_sleep_interval: ReadableDuration::millis(100),
        dispatch_timeout: dfs_dispatch_timeout,
        read_body_timeout: ReadableDuration::secs(15),
        ..Default::default()
    };
    // 10MB/s (Prod env: 10GB/s, 16MB/sst -> 640 sst/s)
    oss.set_max_read_bytes_per_sec(KV_TARGET_FILE_SIZE.0 as usize * 640);
    // 10MB/s (Prod env: 1GB/s)
    // TODO: adjust rate for oss chaos.
    oss.set_max_write_bytes_per_sec(KV_TARGET_FILE_SIZE.0 as usize * 640);

    info!("prepare_cluster";
        "per_keyspace_configs" => ?per_keyspace_configs,
        "dfs_conn_opts" => ?dfs_config.conn_options,
        "rfengine_target_file_size" => ?rfengine_target_file_size,
        "dfs_worker_memory_limit" => ?dfs_worker_memory_limit,
    );

    let dfs = dfs_config.clone();
    let enable_ia = switches.ia_table_ratio > 0.0;
    let update_conf_fn = move |_, conf: &mut TikvConfig| {
        let mut rng = rand::thread_rng();
        conf.dfs = dfs.clone();
        conf.dfs.allow_fallback_local = false;
        conf.server.grpc_concurrency = (cpu_cores / nodes_count).max(2);
        conf.server.grpc_compression_type = GrpcCompressionType::Gzip;
        conf.security = security_conf.clone();

        conf.coprocessor.region_split_size = ReadableSize::kb(256);
        conf.coprocessor.region_bucket_size = REGION_BUCKET_SIZE;

        conf.raft_store.peer_stale_state_check_interval = ReadableDuration::secs(1);
        conf.raft_store.abnormal_leader_missing_duration = ReadableDuration::secs(3);
        conf.raft_store.max_leader_missing_duration = ReadableDuration::secs(5);

        let mut lightweight_backup = true;
        if use_builtin_dfs {
            conf.raft_store.local_file_gc_timeout = ReadableDuration::secs(120);
            conf.raft_store.local_file_gc_tick_interval = ReadableDuration::secs(10);
            lightweight_backup = false;
        }

        conf.rocksdb.writecf.block_size = ReadableSize::kb(4);
        conf.rocksdb.writecf.write_buffer_size = ReadableSize::kb(96);
        conf.rocksdb.writecf.target_file_size_base = KV_TARGET_FILE_SIZE;

        conf.rfengine.target_file_size = rfengine_target_file_size;
        conf.rfengine.rlog_file_size =
            rfengine_target_file_size / *[2, 8, 32].choose(&mut rng).unwrap();
        conf.rfengine.batch_compression_threshold = ReadableSize::kb(rng.gen_range(0..2));
        conf.rfengine.lightweight_backup = lightweight_backup;
        conf.rfengine.wal_chunk_target_file_size = rfengine_target_file_size / 8;
        conf.rfengine.dfs_worker_memory_limit = dfs_worker_memory_limit.into();
        conf.rfengine.wal_secondary_dir = Path::new(&conf.rfengine.wal_sync_dir)
            .with_file_name("wal2")
            .to_string_lossy()
            .to_string();

        conf.kvengine.compaction_tombs_count = 100;
        conf.kvengine.max_del_range_delay = ReadableDuration(Duration::from_secs(3));
        conf.kvengine.flush_split_l0 = true;
        conf.kvengine.per_keyspace_configs = per_keyspace_configs.clone();
        conf.kvengine.value_cache_capacity = if switches.enable_value_cache {
            ReadableSize::mb(1).into()
        } else {
            0.into()
        };
        if enable_ia {
            conf.kvengine.ia = IaConfig {
                mem_cap: IA_MEM_CAP_DEF.into(),
                disk_cap: IA_DISK_CAP_DEF.into(),
                segment_size: conf.rocksdb.writecf.block_size.0 as i64 * 2,
                freq_update_interval: ReadableDuration(IA_FREQ_UPDATE_INTERVAL_DEF),
                ..Default::default()
            }
        }

        conf.storage.flow_control.enable = true;
        conf.storage.flow_control.min_region_speed_limit = ReadableSize(16);
        conf.storage.flow_control.max_region_speed_limit = ReadableSize::mb(1);
        conf.storage.scheduler_worker_pool_size = cpu_cores;
        conf.storage.check_backup_ts = switches.txn_check_backup_ts;
    };
    let pd_wrapper = PdWrapper::new_test(1, security_conf, None);
    let mut cluster = ServerClusterBuilder::new(nodes, update_conf_fn)
        .pd(pd_wrapper)
        .build();
    if !use_builtin_dfs {
        cluster.start_tikv_workers(
            alloc_node_id_vec(TIKV_WORKERS_COUNT),
            TikvWorkerOptions {
                kv_target_file_size: KV_TARGET_FILE_SIZE,
                cop_block_cache_size: COP_BLOCK_CACHE_SIZE,
                ..Default::default()
            },
        );
    }
    cluster.wait_region_replicated(&[], 3);
    let pd_client = cluster.get_pd_client();
    pd_client.disable_default_operator();
    let region0 = pd_client.get_all_regions().first().unwrap().clone();

    // Split keyspaces.
    let mut keyspace_keys = vec![];
    let mut keyspaces: Vec<u32> = vec![];
    let mut data_keys = vec![];
    let mut next_table_id = 1; // Table id is global unique. See `TableMeta::new`.
    for _ in 0..initial_keyspace_count {
        // New keyspace must allocated by keyspace manager to avoid conflicts.
        // TODO: test for default keyspace.
        let keyspace_id = cluster.keyspace_manager().new_keyspace_id(1);
        keyspaces.push(keyspace_id);
        keyspace_keys.push(ApiV2::get_txn_keyspace_prefix(keyspace_id));
        keyspace_keys.push(ApiV2::get_txn_keyspace_prefix(keyspace_id + 1));

        for _ in 0..INITIAL_TABLE_COUNT {
            let table_id = next_table_id;
            next_table_id += 1;
            for i in 0..rng.gen_range(0..10) {
                let key = make_row_key(keyspace_id, table_id, &gen_row_key_suffix(i * 100));
                data_keys.push(Key::from_raw(&key).into_encoded());
            }
        }

        let cfg = KeyspaceEncryptionConfig { enabled: rng.gen() };
        match pd_client.set_keyspace_encryption(keyspace_id, cfg) {
            Ok(_) => {}
            Err(err) => {
                panic!("set_keyspace_encryption failed: {:?}", err)
            }
        }
    }
    keyspace_keys.dedup();
    let encoded_keys = keyspace_keys
        .iter()
        .map(|k| Key::from_raw(k).into_encoded())
        .collect();
    pd_client.must_split_region(region0, CheckPolicy::Usekey, encoded_keys);
    cluster.wait_pd_region_min_count(keyspace_keys.len() + 1);
    let keyspace_names = keyspaces
        .iter()
        .map(|&keyspace_id| TidbCluster::keyspace_name(keyspace_id as u16))
        .collect();
    let ia_table_ratio = switches.ia_table_ratio;
    let sc_spec_fn = Box::new(move |_| {
        if thread_rng().gen_bool(ia_table_ratio) {
            StorageClass::Ia.into()
        } else {
            StorageClassSpec::default()
        }
    });
    cluster.keyspace_manager().create_keyspaces(
        &keyspaces,
        keyspace_names,
        &CreateKeyspaceOptions {
            table_count: INITIAL_TABLE_COUNT,
            schema_enable_ratio: TABLE_SCHEMA_ENABLE_RATIO,
            storage_class_spec_fn: sc_spec_fn,
        },
        Some(&mut rng),
    );
    KEYSPACE_COUNTER.store(initial_keyspace_count, Ordering::Relaxed);
    TABLE_COUNTER.store(
        initial_keyspace_count * INITIAL_TABLE_COUNT,
        Ordering::Relaxed,
    );
    let data_keys_len = data_keys.len();
    let res = block_on(pd_client.split_regions_with_retry(data_keys, Duration::from_secs(60)));
    if let Err(err) = res {
        warn!("split regions failed: {:?}", err);
    }

    let all_keyspaces = cluster.keyspace_manager().get_all_keyspaces();
    let fs = cluster.get_dfs().unwrap();
    let runtime = fs.get_runtime();
    let stores = pd_client.get_all_stores(true).unwrap();
    for keyspace_id in all_keyspaces {
        let keyspace_meta = cluster
            .keyspace_manager()
            .get_keyspace_meta(keyspace_id)
            .unwrap();
        if !keyspace_meta.schemas().is_empty() {
            let schema_version = 10;
            let schema_data: Bytes =
                build_schema_file(keyspace_id, schema_version, keyspace_meta.schemas(), 0).into();
            let schema_file_id = pd_client.alloc_id().unwrap();
            runtime
                .block_on(fs.create(
                    schema_file_id,
                    schema_data.clone(),
                    dfs::Options::default().with_type(FileType::Schema),
                ))
                .unwrap();
            let schema_file =
                SchemaFile::open(Arc::new(InMemFile::new(schema_file_id, schema_data))).unwrap();
            // Setup schema file to stores.
            runtime.block_on(broadcast_schema_file_request_and_check(
                &stores,
                keyspace_id,
                &schema_file,
                Duration::from_secs(30),
            ));
        }
    }

    // Scatter regions.
    let move_scheduler = cluster.new_scheduler();
    for _ in 0..(keyspace_keys.len() + data_keys_len + 1) {
        move_scheduler.move_random_region();
    }

    cluster
}

async fn verify_cluster(cluster: &mut ServerCluster, use_builtin_dfs: bool) -> usize /* records count in ref store */
{
    // Verify data.
    let mut handles = vec![];
    for keyspace_id in cluster.keyspace_manager().ref_stores().all_keyspace_ids() {
        let mut client = cluster.new_keyspace_client().await;
        handles.push(tokio::spawn(async move {
            (keyspace_id, client.verify_keyspace(keyspace_id).await)
        }));
    }
    let mut records_cnt = 0;
    for handle in handles {
        let (keyspace_id, res) = handle.await.unwrap();
        records_cnt += res.unwrap_or_else(|err| {
            panic!(
                "{} verify_keyspace_with_ref_store failed: {:?}",
                keyspace_id, err
            );
        });
    }
    assert!(records_cnt > 1000, "too few records in ref store");

    // Check statistics.
    // Check after verify data, to ensure that PD heartbeat have updated region
    // stats.
    verify_cluster_stats(cluster, REGION_BUCKET_SIZE.0, Duration::from_secs(30));
    verify_region_info_accessor(cluster);
    verify_rfengine_keyspace_id(cluster);

    if !use_builtin_dfs {
        check_br();
        check_load_data();
    }
    check_gc();
    check_drop_table();

    // TODO: check storage class property.
    assert_eq!(ENGINE_IA_SYNC_READ_COUNTER.get(), 0);

    records_cnt
}

#[derive(Debug)]
pub(crate) struct Switches {
    pub ia_table_ratio: f64,
    pub enable_oss_chaos: bool,
    pub txn_check_backup_ts: bool,
    pub enable_value_cache: bool,
    pub enable_rfengine_cache: bool,
    pub restore_lower_memory: bool,
}

impl Switches {
    pub fn from_env() -> Self {
        let mut rng = thread_rng();
        let ia_table_ratio: f64 = env_param("IA_TABLE_RATIO", 0.5);
        let enable_oss_chaos = rng.gen_bool(env_param("OSS_CHAOS_RATIO", 0.2));
        let txn_check_backup_ts = env_switch("TXN_CHECK_BACKUP_TS");
        let enable_value_cache = env_switch("ENABLE_VALUE_CACHE");
        let enable_rfengine_cache = env_switch_opt("ENABLE_RFENGINE_CACHE", 0);
        let lower_memory = rng.gen_bool(0.5);

        Self {
            ia_table_ratio,
            enable_oss_chaos,
            txn_check_backup_ts,
            enable_value_cache,
            enable_rfengine_cache,
            restore_lower_memory: lower_memory,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct WorkloadStats {
    write_count: usize,
    txn_file_write_count: usize,
    keyspace_count: usize,
    table_count: usize,
    drop_table_count: usize,
    merge_count: usize,
    move_count: usize,
    transfer_count: usize,
    node_restart: usize,
    backup_count: usize,
    backup_tolerated_err_count: usize,
    restore_count: usize,
    restore_tolerated_err_count: usize,
    broken_backup_count: usize,
    load_data_count: usize,
    manual_major_compact: usize,
    gc_resolved_locks: usize,
    remote_compact_exceed_memory_limit: u64,
    rfengine_dfs_worker_unhealthy: u64,
}

impl WorkloadStats {
    fn collect() -> Self {
        let write_count = WRITE_COUNTER.load(Ordering::SeqCst);
        let txn_file_write_count = TXN_FILE_WRITE_COUNTER.load(Ordering::SeqCst);
        let keyspace_count = KEYSPACE_COUNTER.load(Ordering::SeqCst);
        let table_count = TABLE_COUNTER.load(Ordering::SeqCst);
        let drop_table_count = DROP_TABLE_COUNTER.load(Ordering::SeqCst);
        let merge_count = MERGE_COUNTER.load(Ordering::SeqCst);
        let move_count = MOVE_COUNTER.load(Ordering::SeqCst);
        let transfer_count = TRANSFER_COUNTER.load(Ordering::SeqCst);
        let node_restart = NODE_RESTART_COUNTER.load(Ordering::SeqCst);
        let backup_count = BACKUP_COUNTER.load(Ordering::SeqCst);
        let backup_tolerated_err_count = BACKUP_TOLERATED_ERR_COUNTER.load(Ordering::SeqCst);
        let restore_count = RESTORE_COUNTER.load(Ordering::SeqCst);
        let restore_tolerated_err_count = RESTORE_TOLERATED_ERR_COUNTER.load(Ordering::SeqCst);
        let broken_backup_count = BROKEN_BACKUP_COUNTER.load(Ordering::SeqCst);
        let load_data_count = LOAD_DATA_COUNTER.load(Ordering::SeqCst);
        let manual_major_compact = MANUAL_MAJOR_COMPACT_COUNTER.load(Ordering::SeqCst);
        let gc_resolved_locks = GC_ADVANCE_SAFE_POINT_COUNTER.load(Ordering::SeqCst);
        let remote_compact_exceed_memory_limit =
            ENGINE_REMOTE_COMPACT_EXCEED_MEMORY_LIMIT_COUNTER.get();
        let rfengine_dfs_worker_unhealthy = RFENGINE_DFS_WORKER_BECOME_UNHEALTHY_COUNTER.get();

        Self {
            write_count,
            txn_file_write_count,
            keyspace_count,
            table_count,
            drop_table_count,
            merge_count,
            move_count,
            transfer_count,
            node_restart,
            backup_count,
            backup_tolerated_err_count,
            restore_count,
            restore_tolerated_err_count,
            broken_backup_count,
            load_data_count,
            manual_major_compact,
            gc_resolved_locks,
            remote_compact_exceed_memory_limit,
            rfengine_dfs_worker_unhealthy,
        }
    }
}
