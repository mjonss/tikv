// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::Write as _,
    ops::Div,
    path::{Path, PathBuf},
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use anyhow::Context;
use dashmap::{DashMap, mapref::entry::Entry as DashMapEntry};
use kvengine::{
    dfs::DFSConfig,
    ia::util::IaConfig,
    metrics::{
        ENGINE_IA_SYNC_READ_COUNTER, ENGINE_REMOTE_COMPACT_EXCEED_MEMORY_LIMIT_COUNTER,
        ENGINE_STORAGE_CLASS_TRANSITION_COUNTER,
    },
};
use native_br::metrics::NATIVE_BR_BACKUP_SUCCESS;
use pd_client::{
    pd_control,
    pd_control::{OpKind, PdControl, PdScheduleConfig},
};
use rand::prelude::*;
use schema::schema::{StorageClassSpec, StorageClassTransitionInfo};
use security::SecurityConfig;
use serde_derive::Deserialize;
use sqlx::{ConnectOptions, Executor, Row as _};
use test_cloud_server::{
    IA_FREQ_UPDATE_INTERVAL_DEF, ServerCluster, ServerClusterBuilder, TikvConfigExt,
    TikvWorkerOptions, oss::prepare_dfs, table::TableMeta, tidb::*, tikv_worker_cop_url,
    try_wait_async,
};
use test_pd_client::PdWrapper;
use tikv::{config::TikvConfig, server::config::GrpcCompressionType};
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    info,
    sys::SysQuota,
    time::Instant,
};
use tokio::runtime::Runtime;

use crate::{
    test_columnar::{prepare_columnar, run_columnar_workload},
    test_jepsen::*,
    test_storage_class::*,
    test_tpc::*,
    test_txn_file::{TXN_CHUNK_MAX_SIZE, TXN_FILE_MIN_SIZE},
    test_unique::*,
    *,
};

pub(crate) type Result<T> = anyhow::Result<T>;

const KV_TARGET_FILE_SIZE: ReadableSize = ReadableSize::kb(32);
pub(crate) const REGION_SIZE: ReadableSize = ReadableSize::mb(1);
// TiDB has records with 200kb+ size (see "mysql.stats_history"), so set bucket
// size to 256kb.
pub(crate) const REGION_BUCKET_SIZE: ReadableSize = ReadableSize::kb(256);
// Ref: https://docs.pingcap.com/tidb/stable/pd-configuration-file#split-merge-interval
const SPLIT_MERGE_INTERVAL: ReadableDuration = ReadableDuration::secs(10);

// Test on only one keyspace to simulate a heavy tenant. Scenes of multiple
// keyspaces are covered by test_random_all.
pub(crate) const INITIAL_KEYSPACE_COUNT: usize = 1;
pub(crate) const NODES_COUNT: usize = 4;
const TEST_DURATION: Duration = Duration::from_secs(120); // Test for longer as TiDB bootstrap may cost 30s+.

pub(crate) const TIKV_WORKERS_COUNT: usize = 5;

const PD_COUNT: usize = 1;
const PD_BIN_ENV_KEY: &str = "PD_BIN";
const PD_PORT_ENV_KEY: &str = "PD_PORT";
const PD_PORT_DEFAULT: u16 = 2379;
const PD_HEALTHY_TIMEOUT: Duration = Duration::from_secs(30);
const PD_TSO_SVC_COUNT: usize = 2;

const TIDB_BIN_ENV_KEY: &str = "TIDB_BIN";
const TIDB_NEXT_GEN_BIN_ENV_KEY: &str = "TIDB_NEXT_GEN_BIN";
const TIDB_PORT_ENV_KEY: &str = "TIDB_PORT";
const TIDB_PORT_DEFAULT: u16 = 4000;
const TIDB_STATUS_PORT_ENV_KEY: &str = "TIDB_STATUS_PORT";
const TIDB_STATUS_PORT_DEFAULT: u16 = 10080;
pub(crate) const TIDB_HEALTHY_TIMEOUT: Duration = Duration::from_secs(600); // TODO: improve the efficiency of TiDB start up.
pub(crate) const TIDB_LOG_LEVEL: &str = "info";
pub(crate) const TIDB_GC_INTERVAL: &str = "60s";
pub(crate) const TIDB_GC_LIFETIME: &str = "90s";

pub(crate) const TIKV_SERVER_BIN_ENV_KEY: &str = "TIKV_SERVER_BIN";
pub(crate) const TIKV_WORKER_BIN_ENV_KEY: &str = "TIKV_WORKER_BIN";

pub(crate) const TIFLASH_SWITCH_ENV_KEY: &str = "USE_TIFLASH";
pub(crate) const TIFLASH_BIN_ENV_KEY: &str = "TIFLASH_BIN";
pub(crate) const TIFLASH_SERVER_COUNT: usize = 1;
pub(crate) const TIFLASH_HEALTHY_TIMEOUT: Duration = Duration::from_secs(120);

pub(crate) const TPC_WORKLOAD_SWITCH_ENV_KEY: &str = "TPC_WORKLOAD";
pub(crate) const TPC_BIN_ENV_KEY: &str = "TPC_BIN";
pub(crate) const TPCC_RUN_DURATION: Duration = Duration::from_secs(10); // Duration of each TPCC run.
pub(crate) const TPCC_WORKLOAD_CONCURRENCY: usize = 1;

pub(crate) const JEPSEN_WORKLOAD_SWITCH_ENV_KEY: &str = "JEPSEN_WORKLOAD";
pub(crate) const JEPSEN_WORKLOAD_USE_TXN_FILE_ENV_KEY: &str = "JEPSEN_TXN_FILE";
pub(crate) const JEPSEN_WORKLOAD_KEYSPACE: u32 = 1; // Keyspace starts from 1.

pub(crate) const UNIQUE_WORKLOAD_SWITCH_ENV_KEY: &str = "UNIQUE_WORKLOAD";
pub(crate) const UNIQUE_WORKLOAD_KEYSPACE: u32 = 1; // Keyspace starts from 1.

pub(crate) const COLUMNAR_WORKLOAD_SWITCH_ENV_KEY: &str = "COLUMNAR_WORKLOAD";
pub(crate) const COLUMNAR_WORKLOAD_KEYSPACE: u32 = 1;
pub(crate) const ENABLE_TIFLASH_WRITE_NODE_ENV_KEY: &str = "ENABLE_TIFLASH_WRITE_NODE";

pub(crate) const VERIFY_HEALTHY_TIMEOUT: Duration = Duration::from_secs(120);

pub(crate) const ENABLE_GLOBAL_TXN_FILE_RATIO: f64 = 0.8; // 80% chance enable txn file globally.
pub(crate) const ENABLE_GLOBAL_TXN_FILE_ENV_KEY: &str = "GLOBAL_TXN_FILE";

pub(crate) const USE_REMOTE_COP_ENV_KEY: &str = "USE_REMOTE_COP";
pub(crate) const REMOTE_COP_MIN_BLOCK_SIZE_OPTIONS: [usize; 3] =
    [64 * 1024, 512 * 1024, 1024 * 1024];
pub(crate) const COP_BLOCK_CACHE_SIZE: ReadableSize = ReadableSize::mb(16); // Small size to make eviction more frequent.

pub(crate) const RESTART_TSO_SVC_ENV_KEY: &str = "RESTART_TSO_SVC";

pub(crate) const MEMORY_CAPACITY_RATIO: f64 = 0.8; // Reserve 20% memory for PD, TiDB, and TiFlash.

const DFS_LOAD_MEMORY_USAGE: u64 = 256 * 1024 * 1024; // 256MB

#[test]
fn test_random_with_tidb() {
    init_logger();
    let prepare_time = Instant::now_coarse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .thread_name("random-workload")
        .build()
        .unwrap();
    let _guard = runtime.enter();

    let switches = Switches::from_env();
    info!("switches: {:?}", switches);

    // Prepare.
    let (_temp_dir, _oss, dfs_config) = prepare_dfs("oss_");
    let security_conf = new_security_config();
    let tc = prepare_tidb_cluster(&security_conf, &switches);
    let mut cluster = prepare_cluster(
        &dfs_config,
        &security_conf,
        NODES_COUNT,
        INITIAL_KEYSPACE_COUNT,
        &switches,
        &tc,
    );
    let pd_client = cluster.get_pd_client_ext();
    let pd_ctl = Arc::new(cluster.get_pd_control().unwrap());
    let keyspace_manager = cluster.keyspace_manager().clone();

    let tikv_worker_addr = cluster.tikv_worker_endpoints().pop().unwrap();
    start_components(&tc, tikv_worker_addr, &switches, &dfs_config, &runtime);
    prepare_workloads(&tc, &keyspace_manager, &switches, &runtime);
    let tables = block_on(collect_tables(&tc, &keyspace_manager, &switches));
    let running = Running::new_start();
    let async_handles = start_workloads(
        &tc,
        pd_client.clone(),
        &keyspace_manager,
        &switches,
        &runtime,
        &tables,
        running.clone(),
    );
    assert!(!async_handles.is_empty(), "no workload to run");

    // Main loop.
    let start_time = Instant::now();
    while start_time.saturating_elapsed() < TEST_DURATION {
        // Restart nodes.
        random_node_restart(&mut cluster, |_, _| {}, false);
    }

    // Finish.
    runtime.block_on(async {
        info!("test finished, stopping all workers");
        running.stop();
        for handle in async_handles {
            handle.await.unwrap();
        }

        // Make stats stable
        info!("stop TiDB and schedulers");
        check_and_stop_components(&tc).await;
        stop_schedulers(pd_ctl).await;

        info!("verify cluster");
        verify_cluster(&mut cluster, &switches, &tables).await;
    });

    // Stop cluster.
    info!("stopping cluster");
    cluster.stop();

    let region_number = pd_client.get_regions_number();
    tc.pd.stop_all();

    // Statistics.
    let stats = WorkloadStats::collect();
    let stdout = std::io::stdout();
    writeln!(
        stdout.lock(),
        "{} TEST SUCCEED: elapsed {:?},{:?}, region_number {}, {:?}",
        test_id(),
        prepare_time.saturating_elapsed(),
        start_time.saturating_elapsed(),
        region_number,
        stats
    )
    .unwrap();
    stdout.lock().flush().unwrap();
}

pub(crate) fn prepare_tidb_cluster(
    security_config: &SecurityConfig,
    switches: &Switches,
) -> TidbCluster {
    let pd_bin = std::env::var(PD_BIN_ENV_KEY).expect("env PD_BIN is not set");
    let pd_port_base = std::env::var(PD_PORT_ENV_KEY)
        .map(|s| s.parse().unwrap())
        .unwrap_or(PD_PORT_DEFAULT);

    let tidb_bin_env = if !switches.tidb_next_gen {
        TIDB_BIN_ENV_KEY
    } else {
        TIDB_NEXT_GEN_BIN_ENV_KEY
    };
    let tidb_bin = std::env::var(tidb_bin_env).expect("env TIDB_BIN is not set");
    let tidb_port_base = std::env::var(TIDB_PORT_ENV_KEY)
        .map(|s| s.parse().unwrap())
        .unwrap_or(TIDB_PORT_DEFAULT);
    let tidb_status_port_base = std::env::var(TIDB_STATUS_PORT_ENV_KEY)
        .map(|s| s.parse().unwrap())
        .unwrap_or(TIDB_STATUS_PORT_DEFAULT);

    let tiflash_bin = switches
        .tiflash_switch_on
        .then(|| std::env::var(TIFLASH_BIN_ENV_KEY).expect("env TIFLASH_BIN is not set"));

    let max_merge_region_size = REGION_SIZE.div(5);
    let max_merge_region_keys = max_merge_region_size.0 / 100; // Assume 100 bytes per key, about 2000 keys.
    let pd_scheduler_config = PdScheduleConfig {
        max_merge_region_size: max_merge_region_size.as_mb().max(1),
        max_merge_region_keys,
        split_merge_interval: SPLIT_MERGE_INTERVAL,
        ..Default::default()
    };

    let mut rng = rand::thread_rng();
    let pd_mode = if rng.gen_ratio(1, 10) {
        PdServerMode::Normal
    } else {
        PdServerMode::MicroServices {
            tso_count: PD_TSO_SVC_COUNT as u16,
        }
    };
    let tc = TidbCluster::new(
        pd_mode,
        PathBuf::from(pd_bin),
        pd_port_base,
        pd_scheduler_config,
        PathBuf::from(tidb_bin),
        tidb_port_base,
        tidb_status_port_base,
        tiflash_bin.map(PathBuf::from),
        INITIAL_KEYSPACE_COUNT as u16,
        security_config,
        switches.tidb_next_gen,
    );
    block_on(tc.start_pd(PD_COUNT as u16, PD_HEALTHY_TIMEOUT));
    tc
}

pub(crate) fn prepare_cluster(
    dfs_config: &DFSConfig,
    security_conf: &SecurityConfig,
    nodes_count: usize,
    initial_keyspace_count: usize,
    switches: &Switches,
    tc: &TidbCluster,
) -> ServerCluster {
    let mut rng = rand::thread_rng();
    let nodes = alloc_node_id_vec(nodes_count);
    let tikv_worker_nodes = alloc_node_id_vec(TIKV_WORKERS_COUNT);
    let update_conf_fn = generate_update_conf_fn(
        dfs_config,
        security_conf,
        nodes_count,
        &tikv_worker_nodes,
        switches,
    );
    let pd = PdWrapper::new_real(tc.pd.endpoints(), security_conf, PD_CLIENT_UPDATE_INTERVAL);
    let mut cluster = ServerClusterBuilder::new(nodes, update_conf_fn)
        .pd(pd)
        .memory_capacity_ratio(MEMORY_CAPACITY_RATIO)
        .build();
    cluster.start_tikv_workers(
        tikv_worker_nodes,
        TikvWorkerOptions {
            kv_target_file_size: KV_TARGET_FILE_SIZE,
            cop_block_cache_size: COP_BLOCK_CACHE_SIZE,
            backup_interval: Duration::from_secs(5),
            backup_delay: Duration::from_secs(3),
            backup_skip_keyspace_meta: false,
            ..Default::default()
        },
    );
    if switches.columnar_switch_on || switches.ia_table_ratio > 0.0 {
        cluster.start_schema_manager(alloc_node_id());
    }
    cluster.wait_region_replicated(&[], 3);

    let pd_control = tc.pd.get_pd_control();
    // Initial keyspaces have been created by `pre_alloc_keyspaces` of PD.
    let (keyspace_ids, keyspace_names) =
        get_pre_alloc_keyspaces(initial_keyspace_count, &pd_control);

    // TODO: create by API to be uniform with test PD.
    // TODO: enable encryption.
    cluster.keyspace_manager().create_keyspaces(
        &keyspace_ids,
        keyspace_names,
        &CreateKeyspaceOptions::default(),
        Some(&mut rng),
    );
    KEYSPACE_COUNTER.store(initial_keyspace_count, Ordering::Relaxed);

    // TODO: scatter regions.
    // TODO: restart cluster with inner key offset enabled

    cluster
}

pub(crate) fn generate_update_conf_fn<'a>(
    dfs_config: &'a DFSConfig,
    security_conf: &'a SecurityConfig,
    tikv_server_nodes_count: usize,
    tikv_worker_nodes: &'a [u16],
    switches: &'a Switches,
) -> impl Fn(u16, &mut TikvConfig) + 'a {
    let cpu_cores = SysQuota::cpu_cores_quota() as u64;
    // It's 512 for 4 cores & 4 nodes.
    let dfs_load_concurrency_per_core =
        DFS_LOAD_MEMORY_USAGE / cpu_cores / tikv_server_nodes_count as u64 / KV_TARGET_FILE_SIZE.0;
    let dfs_load_concurrency_per_request = dfs_load_concurrency_per_core / 8; // 64
    let rfengine_target_file_size = switches.rfengine_target_file_size;

    move |_node_id: u16, conf: &mut TikvConfig| {
        let mut rng = thread_rng();
        conf.dfs = dfs_config.clone();
        conf.dfs.allow_fallback_local = false;
        conf.server.grpc_concurrency = (cpu_cores as usize / tikv_server_nodes_count).max(2);
        conf.server.grpc_compression_type = GrpcCompressionType::Gzip;
        conf.security = security_conf.clone();

        conf.coprocessor.region_split_size = REGION_SIZE;
        conf.coprocessor.region_bucket_size = REGION_BUCKET_SIZE;

        // conf.raft_store.raft_base_tick_interval is `50ms`, see `new_test_config`.
        conf.raft_store.raft_election_timeout_ticks = 20;
        conf.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(800);
        conf.raft_store.peer_stale_state_check_interval = ReadableDuration::secs(5);
        conf.raft_store.peer_long_check_interval = ReadableDuration::secs(5);
        conf.raft_store.abnormal_leader_missing_duration = ReadableDuration::secs(15);
        conf.raft_store.max_leader_missing_duration = ReadableDuration::secs(25);
        conf.raft_store.split_region_check_tick_interval = ReadableDuration::millis(500);
        conf.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(500);
        conf.raft_store.pd_heartbeat_tick_interval = ReadableDuration::secs(5);
        conf.raft_store.pd_store_heartbeat_tick_interval = ReadableDuration::millis(500);
        conf.raft_store.local_file_gc_timeout = ReadableDuration::secs(60);
        conf.raft_store.local_file_gc_tick_interval = ReadableDuration::secs(10);

        conf.rocksdb.writecf.block_size = ReadableSize::kb(2);
        conf.rocksdb.writecf.target_file_size_base = KV_TARGET_FILE_SIZE;

        conf.rfengine.target_file_size = rfengine_target_file_size;
        conf.rfengine.rlog_file_size =
            rfengine_target_file_size / *[2, 8, 32].choose(&mut rng).unwrap();
        conf.rfengine.batch_compression_threshold = ReadableSize::kb(rng.gen_range(0..2));
        conf.rfengine.lightweight_backup = true;
        conf.rfengine.wal_chunk_target_file_size = rfengine_target_file_size / 16;
        conf.rfengine.dfs_worker_memory_limit = (conf.rfengine.target_file_size * 8).into();
        conf.rfengine.wal_secondary_dir = Path::new(&conf.rfengine.wal_sync_dir)
            .with_file_name("wal2")
            .to_string_lossy()
            .to_string();

        conf.kvengine.compaction_tombs_count = 100;
        conf.kvengine.max_del_range_delay = ReadableDuration(Duration::from_secs(3));

        conf.kvengine.build_columnar = switches.columnar_switch_on;
        conf.kvengine
            .columnar_table_build_options
            .pack_max_row_count = 32;
        conf.kvengine.vector_index_build_options.max_file_size = 32 * 1024;
        conf.kvengine.columnar_table_build_options.pack_max_size = 32 * 128;
        conf.kvengine.vector_index_build_options.delta_size = 128;
        conf.kvengine.vector_index_build_options.rebuild_file_count = 2;
        conf.kvengine.dfs_load_concurrency_per_core = dfs_load_concurrency_per_core as usize;
        conf.kvengine.dfs_load_concurrency_per_request = dfs_load_concurrency_per_request as usize;
        conf.kvengine.value_cache_capacity = if switches.enable_value_cache {
            ReadableSize::mb(1).into()
        } else {
            0.into()
        };
        conf.kvengine.ia = IaConfig {
            segment_size: conf.rocksdb.writecf.block_size.0 as i64 * 4,
            freq_update_interval: ReadableDuration(IA_FREQ_UPDATE_INTERVAL_DEF),
            auto_ia_check_interval: ReadableDuration::secs(3),
            ..Default::default()
        };
        conf.enable_ia();

        conf.storage.flow_control.enable = true;
        conf.storage.flow_control.min_region_speed_limit = ReadableSize(16);
        conf.storage.flow_control.max_region_speed_limit = ReadableSize::mb(1);
        conf.storage.scheduler_worker_pool_size = cpu_cores as usize;
        conf.storage.check_backup_ts = switches.txn_check_backup_ts;
        conf.gc.enable_safe_point_v2 = true;

        if switches.remote_cop_min_block_size > 0 {
            let tikv_worker_idx = *tikv_worker_nodes.choose(&mut rng).unwrap();
            let cop_worker_url = tikv_worker_cop_url(tikv_worker_idx);
            conf.kvengine.remote_worker_addr = cop_worker_url.clone();
            conf.kvengine.remote_coprocessor_addr = cop_worker_url;
            conf.kvengine.remote_coprocessor_min_blocks_size = switches.remote_cop_min_block_size;
        }
    }
}

pub(crate) fn get_pre_alloc_keyspaces(
    initial_keyspace_count: usize,
    pd_control: &PdControl,
) -> (
    Vec<u32>,    // keyspace_ids
    Vec<String>, // keyspace_names
) {
    let mut ids = vec![];
    let mut names = vec![];
    // Note: keyspaces allocation starts from 1.
    for idx in 1..=initial_keyspace_count {
        let keyspace_name = TidbCluster::keyspace_name(idx as u16);
        let keyspace = block_on(pd_control.get_keyspace_by_name(&keyspace_name)).unwrap();
        ids.push(keyspace.id);
        names.push(keyspace_name);
    }
    (ids, names)
}

pub(crate) fn start_components(
    tc: &TidbCluster,
    tikv_worker_addr: String,
    switches: &Switches,
    dfs_config: &DFSConfig,
    runtime: &Runtime,
) {
    let mut tasks = vec![];

    let start_tidb = {
        let tc = tc.clone();
        let columnar_switch_on = switches.columnar_switch_on;
        let enable_tiflash_write_node = switches.enable_tiflash_write_node;
        let gc_lifetime = switches.tidb_gc_lifetime.clone();
        runtime.spawn(async move {
            tc.start_tidb(
                INITIAL_KEYSPACE_COUNT as u16,
                TIDB_HEALTHY_TIMEOUT,
                TIDB_LOG_LEVEL,
                StartTidbOptions {
                    tikv_worker_addr,
                    txn_chunk_max_size: TXN_CHUNK_MAX_SIZE as u64,
                    txn_file_min_mutation_size: Some(TXN_FILE_MIN_SIZE as u64),
                    gc_interval: TIDB_GC_INTERVAL.to_owned(),
                    gc_lifetime,
                    tiflash_disaggregated_mode: columnar_switch_on,
                    enable_tiflash_write_node,
                },
            )
            .await
        })
    };
    tasks.push(start_tidb);

    if switches.tiflash_switch_on {
        let tc = tc.clone();
        let columnar_switch_on = switches.columnar_switch_on;
        let enable_tiflash_write_node = switches.enable_tiflash_write_node;
        let dfs_config = dfs_config.clone();
        let start_tiflash = runtime.spawn(async move {
            tc.start_tiflash(
                TIFLASH_SERVER_COUNT as u16,
                &dfs_config,
                TIFLASH_HEALTHY_TIMEOUT,
                columnar_switch_on,
                enable_tiflash_write_node,
            )
            .await
        });
        tasks.push(start_tiflash);
    };

    runtime
        .block_on(futures::future::try_join_all(tasks))
        .unwrap();
}

pub(crate) fn prepare_workloads(
    tc: &TidbCluster,
    keyspace_manager: &KeyspaceManager,
    switches: &Switches,
    runtime: &Runtime,
) {
    runtime.block_on(prepare_tidb_variables(tc, keyspace_manager, switches));

    let mut prepare_tasks = vec![];
    if switches.tpc_switch_on {
        let tpc_bin = std::env::var(TPC_BIN_ENV_KEY).expect("env TPC_BIN is not set");
        check_tpc_binary(&tpc_bin);

        let all_keyspaces = keyspace_manager.get_all_keyspaces();
        prepare_tasks.push(runtime.spawn(prepare_tpcc(
            tc.clone(),
            keyspace_manager.clone(),
            tpc_bin.clone(),
            all_keyspaces,
            switches.global_use_txn_file,
            TPCC_WORKLOAD_CONCURRENCY,
        )));
    }
    if switches.jepsen_switch_on {
        prepare_tasks.push(runtime.spawn(prepare_jepsen_bank(
            tc.clone(),
            keyspace_manager.clone(),
            JEPSEN_WORKLOAD_KEYSPACE,
            switches.tiflash_switch_on.then_some(TIFLASH_SERVER_COUNT),
        )));
    }
    if switches.unique_workload_switch_on {
        prepare_tasks.push(runtime.spawn(prepare_unique_workload(
            tc.clone(),
            keyspace_manager.clone(),
            UNIQUE_WORKLOAD_KEYSPACE,
        )));
    }
    if switches.columnar_switch_on {
        info!("prepare_columnar");
        prepare_tasks.push(runtime.spawn(prepare_columnar(
            tc.clone(),
            keyspace_manager.clone(),
            COLUMNAR_WORKLOAD_KEYSPACE,
            switches.vector_common_handle,
        )));
    }
    runtime
        .block_on(futures::future::try_join_all(prepare_tasks))
        .unwrap();
}

async fn prepare_tidb_variables(
    tc: &TidbCluster,
    keyspace_manager: &KeyspaceManager,
    switches: &Switches,
) {
    let mut variables = vec![
        ("tidb_enable_async_commit", switches.async_commit_switch_on),
        ("tidb_enable_1pc", switches.async_commit_switch_on),
    ];
    if !switches.tidb_next_gen {
        variables.push(("tidb_disable_txn_file", !switches.global_use_txn_file));
    } else {
        variables.push(("tidb_pessimistic_txn_fair_locking", false));
    }

    for keyspace_id in keyspace_manager.get_all_keyspaces() {
        let pool = connect_tidb(tc, keyspace_manager, keyspace_id).await;

        let mut conn = pool.acquire().await.unwrap();
        for &(var, value) in &variables {
            let sql = format!("SET GLOBAL {} = {}", var, value as i64);
            info!("{}: TiDB var: {}", keyspace_id, sql);
            conn.execute(sql.as_str()).await.unwrap();
        }
        conn.detach(); // Drop the connection, as we set global variables only.
    }

    // Verify variables.
    for keyspace_id in keyspace_manager.get_all_keyspaces() {
        let pool = connect_tidb(tc, keyspace_manager, keyspace_id).await;
        for &(var, expect) in &variables {
            let sql = format!("SELECT @@{var}");
            let row = pool.fetch_one(sql.as_str()).await.unwrap();
            let val: i64 = row.get(0);
            info!("{}: TiDB var: {} = {}", keyspace_id, var, val);
            assert_eq!(val, expect as i64, "{} {} {}", sql, val, expect);
        }
    }
}

pub(crate) async fn collect_tables(
    tc: &TidbCluster,
    keyspace_manager: &KeyspaceManager,
    switches: &Switches,
) -> Vec<Arc<TableMeta>> {
    if switches.ia_table_ratio <= 0.0 {
        return vec![];
    }
    let mut tables = vec![];
    let all_keyspaces = keyspace_manager.get_all_keyspaces();

    if switches.tpc_switch_on {
        for &keyspace_id in &all_keyspaces {
            for tpc_idx in 0..TPCC_WORKLOAD_CONCURRENCY {
                let db = db_name_by_tpc_idx(tpc_idx);
                for table_name in TPCC_TABLES {
                    tables.push(
                        query_table_meta(tc, keyspace_manager, keyspace_id, &db, table_name)
                            .await
                            .unwrap(),
                    );
                }
            }
        }
    }

    if switches.jepsen_switch_on {
        tables.push(
            query_table_meta(
                tc,
                keyspace_manager,
                JEPSEN_WORKLOAD_KEYSPACE,
                BANK_DB_NAME,
                ACCOUNTS_TABLE_NAME,
            )
            .await
            .unwrap(),
        );
    }

    if switches.unique_workload_switch_on {
        tables.push(
            query_table_meta(
                tc,
                keyspace_manager,
                UNIQUE_WORKLOAD_KEYSPACE,
                UNIQUE_DB_NAME,
                UNIQUE_TABLE_NAME,
            )
            .await
            .unwrap(),
        );
    }

    // TODO: tables of columnar workload

    info!("workload tables: {:?}", &tables);
    tables.into_iter().map(|t| Arc::new(t)).collect()
}

pub(crate) fn collect_db_names(switches: &Switches) -> Vec<String> {
    let mut db_names = vec![];
    if switches.tpc_switch_on {
        for tpc_idx in 0..TPCC_WORKLOAD_CONCURRENCY {
            db_names.push(db_name_by_tpc_idx(tpc_idx));
        }
    }
    if switches.jepsen_switch_on {
        db_names.push(BANK_DB_NAME.into());
    }
    if switches.unique_workload_switch_on {
        db_names.push(UNIQUE_DB_NAME.into());
    }
    db_names
}

pub(crate) fn start_workloads(
    tc: &TidbCluster,
    pd_client: Arc<dyn PdClient>,
    keyspace_manager: &KeyspaceManager,
    switches: &Switches,
    runtime: &Runtime,
    tables: &[Arc<TableMeta>],
    running: Running,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut async_handles = vec![];
    if switches.tpc_switch_on {
        let tpc_bin = std::env::var(TPC_BIN_ENV_KEY).unwrap();
        for tpc_idx in 0..TPCC_WORKLOAD_CONCURRENCY {
            async_handles.push(spawn_tpcc(
                tc.clone(),
                keyspace_manager.clone(),
                &tpc_bin,
                tpc_idx,
                TPCC_RUN_DURATION,
                running.clone(),
            ));
        }
    }
    if switches.jepsen_switch_on {
        async_handles.push(runtime.spawn(run_jepsen_bank(
            tc.clone(),
            keyspace_manager.clone(),
            JEPSEN_WORKLOAD_KEYSPACE,
            switches.global_use_txn_file && switches.jepsen_use_txn_file,
            switches.tiflash_switch_on,
            running.clone(),
        )));
    }
    if switches.unique_workload_switch_on {
        async_handles.push(runtime.spawn(run_unique_workload(
            tc.clone(),
            keyspace_manager.clone(),
            UNIQUE_WORKLOAD_KEYSPACE,
            switches.global_use_txn_file,
            switches.unique_ddl_switch_on,
            running.clone(),
        )));
    }
    if switches.columnar_switch_on {
        async_handles.push(runtime.spawn(run_columnar_workload(
            tc.clone(),
            pd_client.clone(),
            keyspace_manager.clone(),
            COLUMNAR_WORKLOAD_KEYSPACE,
            running.clone(),
            switches.clone(),
        )));
    }
    if switches.ia_table_ratio > 0.0 && !tables.is_empty() {
        async_handles.push(runtime.spawn(spawn_alter_storage_class(
            tc.clone(),
            keyspace_manager.clone(),
            tables.to_vec(),
            switches.ia_table_ratio,
            Duration::from_secs(10),
            running.clone(),
        )));
    }

    if switches.restart_tso_svc {
        async_handles.push(spawn_restart_tso_svc(
            tc.clone(),
            Duration::from_secs(10),
            running,
        ));
    }

    async_handles
}

pub(crate) async fn stop_schedulers(pd_ctl: Arc<pd_control::PdControl>) {
    // Disable scheduler limit config.
    let configs = vec![
        ("merge-schedule-limit", "0"),
        ("region-schedule-limit", "0"),
        ("replica-schedule-limit", "0"),
        ("leader-schedule-limit", "0"),
        ("hot-region-schedule-limit", "0"),
    ];
    pd_ctl.set_config(&configs).await.unwrap();
    // Pause all schedulers to make stats stable.
    pd_ctl
        .pause_or_resume_scheduler("all", Duration::from_secs(3600))
        .await
        .expect("pause schedulers failed");
    let all_schedulers = pd_ctl.list_schedulers(None).await.unwrap();
    // `list_schedulers(None)` does not return `paused_at` field. So invoke
    // `list_schedulers(Paused)` again.
    let paused_schedulers = pd_ctl
        .list_schedulers(Some(pd_control::SchedulerStatus::Paused))
        .await
        .unwrap();
    info!(
        "all schedulers: {:?}, paused schedulers: {:?}",
        all_schedulers, paused_schedulers
    );

    // Wait operators finished.
    let ok = try_wait_async(
        || {
            let pd_ctl = pd_ctl.clone();
            Box::pin(async move {
                let is_region_op = |op: &pd_control::Operator| -> bool {
                    for kind in [OpKind::OpMerge, OpKind::OpSplit] {
                        if op.kind_mask & kind as u32 != 0 {
                            return true;
                        }
                    }
                    false
                };
                let operators = pd_ctl
                    .get_operators()
                    .await
                    .unwrap()
                    .into_iter()
                    .filter(is_region_op)
                    .collect::<Vec<_>>();
                if operators.is_empty() {
                    return true;
                }

                for op in operators {
                    if let Err(err) = pd_ctl.cancel_operator_by_region(op.region_id).await {
                        // Would fail when the operator has finished.
                        warn!("cancel operator failed"; "op" => ?op, "err" => ?err);
                    } else {
                        info!("operator canceled"; "op" => ?op);
                    }
                }
                false
            })
        },
        10,
    )
    .await;
    if !ok {
        // It's OK to fail here. `verify_cluster` will retry.
        warn!(
            "wait operators finished timeout: {:?}",
            pd_ctl.get_operators().await.unwrap()
        );
    }
}

pub(crate) async fn check_and_stop_components(tc: &TidbCluster) {
    tc.pd.must_healthy(VERIFY_HEALTHY_TIMEOUT).await;
    tc.tidb.must_all_healthy(VERIFY_HEALTHY_TIMEOUT).await;
    tc.tidb.stop_all(); // To stop background tasks.
    if let Some(tiflash) = tc.tiflash.as_ref() {
        tiflash.must_all_healthy(VERIFY_HEALTHY_TIMEOUT).await;
        tiflash.stop_all();
    }
}

// TODO: merge to `verify_cluster` in `test_all.rs`.
pub(crate) async fn verify_cluster(
    cluster: &mut ServerCluster,
    switches: &Switches,
    tables: &[Arc<TableMeta>],
) {
    if switches.ia_table_ratio > 0.0 {
        check_storage_class(cluster, tables, Duration::from_secs(240));
    }
    assert_eq!(ENGINE_IA_SYNC_READ_COUNTER.get(), 0);

    // Check statistics.
    // Check after verify data, to ensure that PD heartbeat have updated region
    // stats.
    verify_cluster_stats(cluster, REGION_BUCKET_SIZE.0, Duration::from_secs(60));
    verify_region_info_accessor(cluster);
    verify_rfengine_keyspace_id(cluster);

    if switches.tpc_switch_on {
        check_tpc();
    }
    if switches.jepsen_switch_on {
        check_jepsen();
    }
}

pub(crate) fn spawn_restart_tso_svc(
    tc: TidbCluster,
    restart_interval: Duration,
    running: Running,
) -> tokio::task::JoinHandle<()> {
    let task = async move {
        let tso_svc_count = match tc.pd.mode() {
            PdServerMode::Normal => return,
            PdServerMode::MicroServices { tso_count } => *tso_count,
        };
        let start_time = Instant::now_coarse();
        while running.get() {
            let random = || {
                let mut rng = rand::thread_rng();
                let tso_svc_idx = rng.gen_range(0..tso_svc_count);
                let interval_secs = restart_interval.as_secs();
                let stop_dur =
                    Duration::from_secs(rng.gen_range(interval_secs / 2..interval_secs * 3 / 2));
                // Give some time for TSO service to campaign about the leader before next loop.
                let loop_interval =
                    Duration::from_secs(rng.gen_range(interval_secs / 2..interval_secs));
                (tso_svc_idx, stop_dur, loop_interval)
            };
            let (tso_svc_idx, stop_dur, loop_interval) = random();

            tc.pd
                .restart_tso_svc(tso_svc_idx, stop_dur, PD_HEALTHY_TIMEOUT)
                .await;
            tokio::time::sleep(loop_interval).await;
        }
        info!("restart tso workload exit"; "dur" => ?start_time.saturating_elapsed());
    };
    tokio::spawn(task)
}

pub(crate) fn get_tidb_conn_params(
    tc: &TidbCluster,
    keyspace_manager: &KeyspaceManager,
    keyspace_id: u32,
) -> ConnParams {
    let keyspace_name = keyspace_manager
        .get_keyspace_meta(keyspace_id)
        .unwrap()
        .name();
    let tidb_idx = TidbCluster::get_idx_by_keyspace_name(&keyspace_name);
    tc.tidb.conn_params(tidb_idx)
}

#[derive(Clone, Copy)]
pub(crate) struct ConnectTidbOptions {
    pub statements_log_level: log::LevelFilter,
}

impl Default for ConnectTidbOptions {
    fn default() -> Self {
        Self {
            statements_log_level: log::LevelFilter::Debug,
        }
    }
}

impl ConnectTidbOptions {
    pub(crate) fn log_statements() -> Self {
        Self {
            statements_log_level: log::LevelFilter::Info,
        }
    }
}

async fn connect_tidb_impl(
    tc: &TidbCluster,
    keyspace_manager: &KeyspaceManager,
    keyspace_id: u32,
    tidb_opts: ConnectTidbOptions,
) -> sqlx::MySqlPool {
    let params = get_tidb_conn_params(tc, keyspace_manager, keyspace_id);
    let mut opts = sqlx::mysql::MySqlConnectOptions::new()
        .host(&params.host)
        .port(params.port)
        .username(&params.user)
        .database("test");
    opts.log_statements(tidb_opts.statements_log_level)
        .log_slow_statements(log::LevelFilter::Warn, Duration::from_secs(30));
    sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(100)
        .connect_with(opts)
        .await
        .unwrap()
}

pub(crate) async fn connect_tidb_opts(
    tc: &TidbCluster,
    keyspace_manager: &KeyspaceManager,
    keyspace_id: u32,
    tidb_opts: ConnectTidbOptions,
) -> sqlx::MySqlPool {
    lazy_static::lazy_static! {
        static ref POOLS: DashMap<u32 /* keyspace_id */, sqlx::MySqlPool> = DashMap::new();
    }

    match POOLS.entry(keyspace_id) {
        DashMapEntry::Occupied(entry) => entry.get().clone(),
        DashMapEntry::Vacant(entry) => {
            let pool = connect_tidb_impl(tc, keyspace_manager, keyspace_id, tidb_opts).await;
            entry.insert(pool.clone());
            pool
        }
    }
}

pub(crate) async fn connect_tidb(
    tc: &TidbCluster,
    keyspace_manager: &KeyspaceManager,
    keyspace_id: u32,
) -> sqlx::MySqlPool {
    connect_tidb_opts(tc, keyspace_manager, keyspace_id, Default::default()).await
}

pub(crate) struct TidbTableSchema {
    pub id: i64,
    pub storage_class_spec: StorageClassSpec,
}

// Ref: https://github.com/tidbcloud/tidb-cse/blob/release-7.5-keyspace/pkg/parser/model/model.go, buildStorageClassString
#[derive(Debug, Default, Deserialize)]
struct TidbStorageClassSpec {
    tier: String,
    transitions: Vec<StorageClassTransitionInfo>,
}

pub(crate) async fn query_tidb_table_schema<'a, E>(
    executor: E,
    db: &str,
    table: &str,
) -> Result<TidbTableSchema>
where
    E: sqlx::Executor<'a, Database = sqlx::MySql>,
{
    let query = "SELECT TIDB_TABLE_ID, TIDB_STORAGE_CLASS FROM INFORMATION_SCHEMA.TABLES \
                       WHERE TABLE_SCHEMA = ? and TABLE_NAME = ? LIMIT 1";
    let row = sqlx::query(query)
        .bind(db)
        .bind(table)
        .fetch_one(executor)
        .await
        .context("select_schema")?;
    let table_id: i64 = row.get("TIDB_TABLE_ID");
    let tidb_storage_class: String = row.get("TIDB_STORAGE_CLASS");
    let sc_spec = if let Ok(tidb_sc_spec) =
        serde_json::from_str::<TidbStorageClassSpec>(&tidb_storage_class)
    {
        StorageClassSpec::from_tidb(Some(&tidb_sc_spec.tier), Some(&tidb_sc_spec.transitions))
    } else {
        StorageClassSpec::from_tidb(Some(&tidb_storage_class), None)
    };
    let meta = TidbTableSchema {
        id: table_id,
        storage_class_spec: sc_spec,
    };
    Ok(meta)
}

async fn query_table_meta(
    tc: &TidbCluster,
    keyspace_manager: &KeyspaceManager,
    keyspace_id: u32,
    db: &str,
    table: &str,
) -> Result<TableMeta> {
    let pool = connect_tidb(tc, keyspace_manager, keyspace_id).await;
    let schema = query_tidb_table_schema(&pool, db, table)
        .await
        .context("query_schema")?;
    Ok(TableMeta::new_tidb_table(
        schema.id,
        keyspace_id,
        db.to_string(),
        table.to_string(),
        schema.storage_class_spec,
    ))
}

#[derive(Clone, Debug)]
pub(crate) struct Switches {
    pub remote_cop_min_block_size: usize,
    pub columnar_switch_on: bool,
    pub tiflash_switch_on: bool,
    pub tpc_switch_on: bool,
    pub jepsen_switch_on: bool,
    pub jepsen_use_txn_file: bool,
    pub unique_workload_switch_on: bool,
    pub unique_ddl_switch_on: bool,
    pub global_use_txn_file: bool,
    pub restart_tso_svc: bool,
    pub async_commit_switch_on: bool,
    pub ia_table_ratio: f64,
    pub vector_common_handle: bool,
    pub txn_check_backup_ts: bool,
    pub enable_tiflash_write_node: bool,
    pub enable_value_cache: bool,
    pub tidb_next_gen: bool,
    pub tidb_gc_lifetime: String, // ReadableDuration, e.g. "90s".
    pub rfengine_target_file_size: ReadableSize,
    pub enable_oss_chaos: bool,
    pub enable_columnar_normal_workload: bool,
    pub enable_columnar_dynamic_workload: bool,
    pub enable_columnar_partition_workload: bool,
    pub rep_log_level: String, // Log level of "rep-" components: "info", "debug".
}

impl Switches {
    pub fn from_env() -> Self {
        let mut rng = thread_rng();

        // Random min block size to generate more or less workloads for cop workers.
        let remote_cop_min_block_size = env_switch(USE_REMOTE_COP_ENV_KEY) as usize
            * (*REMOTE_COP_MIN_BLOCK_SIZE_OPTIONS.choose(&mut rng).unwrap());
        let columnar_switch_on = env_switch_opt(COLUMNAR_WORKLOAD_SWITCH_ENV_KEY, 0);
        let tiflash_switch_on = env_switch(TIFLASH_SWITCH_ENV_KEY);
        let tpc_switch_on = env_switch(TPC_WORKLOAD_SWITCH_ENV_KEY);
        let jepsen_switch_on = env_switch(JEPSEN_WORKLOAD_SWITCH_ENV_KEY);
        let jepsen_use_txn_file = env_switch(JEPSEN_WORKLOAD_USE_TXN_FILE_ENV_KEY);
        let unique_workload_switch_on = env_switch_opt(UNIQUE_WORKLOAD_SWITCH_ENV_KEY, 0);
        let unique_ddl_switch_on = rng.gen_bool(0.0); // TODO: enable after consistency issue fixed.

        let global_use_txn_file = env_switch(ENABLE_GLOBAL_TXN_FILE_ENV_KEY);
        let global_use_txn_file = global_use_txn_file && rng.gen_bool(ENABLE_GLOBAL_TXN_FILE_RATIO);

        let restart_tso_svc = env_switch(RESTART_TSO_SVC_ENV_KEY);
        let async_commit_switch_on = rng.gen_bool(env_param("ASYNC_COMMIT_RATIO", 0.1));
        let txn_check_backup_ts = env_switch("TXN_CHECK_BACKUP_TS");
        let ia_table_ratio = env_param("IA_TABLE_RATIO", 0.5);
        let enable_tiflash_write_node = env_switch(ENABLE_TIFLASH_WRITE_NODE_ENV_KEY);
        let enable_value_cache = env_switch("ENABLE_VALUE_CACHE");
        let tidb_next_gen = env_switch_opt("TIDB_NEXT_GEN", 0);
        let tidb_gc_lifetime = env_param("TIDB_GC_LIFETIME", TIDB_GC_LIFETIME.to_string());
        let rfengine_target_file_size = ReadableSize::mb(8);
        let enable_oss_chaos = rng.gen_bool(env_param("OSS_CHAOS_RATIO", 0.2));
        let enable_columnar_normal_workload = env_switch("ENABLE_COLUMNAR_NORMAL_WORKLOAD");
        let enable_columnar_dynamic_workload = env_switch("ENABLE_COLUMNAR_DYNAMIC_WORKLOAD");
        let enable_columnar_partition_workload = env_switch("ENABLE_COLUMNAR_PARTITION_WORKLOAD");
        let rep_log_level = env_param("REP_LOG_LEVEL", "info".to_string());

        Self {
            remote_cop_min_block_size,
            columnar_switch_on,
            tiflash_switch_on,
            tpc_switch_on,
            jepsen_switch_on,
            jepsen_use_txn_file,
            unique_workload_switch_on,
            unique_ddl_switch_on,
            global_use_txn_file,
            restart_tso_svc,
            async_commit_switch_on,
            ia_table_ratio,
            vector_common_handle: rng.gen_bool(0.8),
            txn_check_backup_ts,
            enable_tiflash_write_node,
            enable_value_cache,
            tidb_next_gen,
            tidb_gc_lifetime,
            rfengine_target_file_size,
            enable_oss_chaos,
            enable_columnar_normal_workload,
            enable_columnar_dynamic_workload,
            enable_columnar_partition_workload,
            rep_log_level,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct WorkloadStats {
    pub keyspace_count: usize,
    pub node_restart: usize,
    pub tpcc_txns: usize,
    pub jepsen_bank: usize,
    pub jepsen_bank_retry: usize,
    pub unique_workload: usize,
    pub unique_conflict: usize,
    pub unique_ddl: usize,
    pub async_shards: usize,
    pub remote_compact_exceed_memory_limit: u64,
    pub backup_count: u64,
    pub alter_tables: (usize, usize, usize), // (NON_IA, IA, AUTO)
    pub transited_to_ia: u64,
    pub dfs_unhealthy: usize,
}

impl WorkloadStats {
    pub fn collect() -> Self {
        let keyspace_count = KEYSPACE_COUNTER.load(Ordering::SeqCst);
        let node_restart = NODE_RESTART_COUNTER.load(Ordering::SeqCst);
        let tpcc_txns = TPCC_COUNTER.load(Ordering::SeqCst);
        let jepsen_bank = JEPSEN_BANK_TXN_COUNTER.load(Ordering::SeqCst);
        let jepsen_bank_retry = JEPSEN_BANK_TXN_RETRY_COUNTER.load(Ordering::SeqCst);
        let unique_workload = UNIQUE_WORKLOAD_TXN_COUNTER.load(Ordering::SeqCst);
        let unique_conflict = UNIQUE_WORKLOAD_CONFLICT_COUNTER.load(Ordering::SeqCst);
        let unique_ddl = UNIQUE_TABLE_DDL_COUNTER.load(Ordering::SeqCst);
        let async_shards = ASYNC_SHARD_COUNTER.load(Ordering::SeqCst);
        let remote_compact_exceed_memory_limit =
            ENGINE_REMOTE_COMPACT_EXCEED_MEMORY_LIMIT_COUNTER.get();
        let backup_count = NATIVE_BR_BACKUP_SUCCESS.get();
        let alter_tables = (
            ALTER_TABLE_NON_IA_COUNTER.load(Ordering::SeqCst),
            ALTER_TABLE_IA_COUNTER.load(Ordering::SeqCst),
            ALTER_TABLE_AUTO_IA_COUNTER.load(Ordering::SeqCst),
        );
        let transited_to_ia = ENGINE_STORAGE_CLASS_TRANSITION_COUNTER
            .with_label_values(&["to_ia"])
            .get();
        let dfs_unhealthy = DFS_UNHEALTHY_COUNTER.load(Ordering::SeqCst);
        Self {
            keyspace_count,
            node_restart,
            tpcc_txns,
            jepsen_bank,
            jepsen_bank_retry,
            unique_workload,
            unique_conflict,
            unique_ddl,
            async_shards,
            remote_compact_exceed_memory_limit,
            backup_count,
            alter_tables,
            transited_to_ia,
            dfs_unhealthy,
        }
    }
}
