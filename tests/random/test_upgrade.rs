// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    collections::HashMap,
    io::Write as _,
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use kvengine::dfs::DFSConfig;
use pd_client::{
    pd_control,
    pd_control::{PdControl, StoreInfo},
};
use rand::prelude::*;
use security::SecurityConfig;
use test_cloud_server::{
    ServerCluster, ServerClusterBuilder, TikvWorkerOptions, must_wait,
    oss::prepare_dfs,
    tidb::*,
    tikv_bin::{TikvServers, TikvWorkers},
};
use test_pd_client::PdWrapper;
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    info,
    time::Instant,
};

use crate::{test_tidb::*, *};

const TIKV_STORE_RESTART_INTERVAL: Duration = Duration::from_secs(10); // Interval between restarting TiKV stores.
const TEST_DURATION_BEFORE_UPGRADE: Duration = Duration::from_secs(60);
const TEST_DURATION_AFTER_UPGRADE: Duration = Duration::from_secs(60);
const TEST_DURATION_AFTER_DOWNGRADE: Duration = Duration::from_secs(60);

const EVICT_LEADERS_TIMEOUT: Duration = Duration::from_secs(30);

const WAIT_STORE_STATE_TIMEOUT: Duration = Duration::from_secs(60);
const WAIT_TIKV_SERVER_HEALTHY_TIMEOUT: Duration = Duration::from_secs(150); // Recover will take a long time.
const WAIT_TIKV_WORKER_HEALTHY_TIMEOUT: Duration = Duration::from_secs(15);
const CHECK_HEALTHY_TIMEOUT: Duration = Duration::from_secs(15);

const TIKV_WORKER_THREADS_COUNT: usize = 2;

#[test]
fn test_random_upgrade() {
    init_logger();
    let prepare_time = Instant::now_coarse();
    let temp_dir = std::env::temp_dir();
    tikv_util::set_panic_hook(false, temp_dir.to_str().unwrap()); // To prevent temp dirs from being dropped on error.

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .thread_name("random-workload")
        .build()
        .unwrap();
    let _guard = runtime.enter();

    let switches = Switches::from_env();
    let upgrade_switches = UpgradeTestSwitches::from_env();
    info!("switches: {:?}, {:?}", switches, upgrade_switches);

    // Prepare.
    let (_temp_dir, _oss, dfs_config) = prepare_dfs("oss_");
    let security_conf = new_security_config();
    let tc = prepare_tidb_cluster(&security_conf, &switches);
    let (cluster, tikv_servers, tikv_workers) = prepare_cluster(
        &dfs_config,
        &security_conf,
        NODES_COUNT,
        INITIAL_KEYSPACE_COUNT,
        &switches,
        &upgrade_switches,
        &tc,
    );
    let pd_client = cluster.get_pd_client_ext();
    let pd_ctl = Arc::new(cluster.get_pd_control().unwrap());
    let keyspace_manager = cluster.keyspace_manager().clone();

    let tikv_worker_addr = tikv_workers.endpoints().pop().unwrap();
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

    let worker_configs = cluster.tikv_worker_configs().clone();
    let server_configs = cluster.get_node_configs().clone();

    let cluster = RefCell::new(cluster);

    // Before upgrade.
    let start_time = Instant::now_coarse();
    while start_time.saturating_elapsed() < upgrade_switches.test_dur_before_upgrade.0 {
        random_tikv_servers_restart(&tikv_servers);
    }
    info!("before upgrade: finished"; "stats" => ?WorkloadStats::collect());

    // Upgrade.
    switch_workers_version(
        Workers::TikvWorkers(&tikv_workers),
        Workers::ServerCluster(&cluster),
        &worker_configs,
    );
    switch_servers_version(
        Servers::TikvServers(&tikv_servers),
        Servers::ServerCluster(&cluster),
        |_, _conf| {},
        &server_configs,
        pd_ctl.as_ref(),
        &upgrade_switches,
    );

    // After upgrade.
    let start_time = Instant::now_coarse();
    while start_time.saturating_elapsed() < upgrade_switches.test_dur_after_upgrade.0 {
        random_node_restart(&mut cluster.borrow_mut(), |_, _| {}, false);
    }
    info!("after upgrade: finished"; "stats" => ?WorkloadStats::collect());

    // Downgrade.
    switch_workers_version(
        Workers::ServerCluster(&cluster),
        Workers::TikvWorkers(&tikv_workers),
        &worker_configs,
    );
    switch_servers_version(
        Servers::ServerCluster(&cluster),
        Servers::TikvServers(&tikv_servers),
        |_, _conf| {},
        &server_configs,
        pd_ctl.as_ref(),
        &upgrade_switches,
    );

    // After downgrade.
    let start_time = Instant::now_coarse();
    while start_time.saturating_elapsed() < upgrade_switches.test_dur_after_downgrade.0 {
        random_tikv_servers_restart(&tikv_servers);
    }
    info!("after downgrade: finished"; "stats" => ?WorkloadStats::collect());

    // Upgrade again for verify cluster.
    switch_workers_version(
        Workers::TikvWorkers(&tikv_workers),
        Workers::ServerCluster(&cluster),
        &worker_configs,
    );
    switch_servers_version(
        Servers::TikvServers(&tikv_servers),
        Servers::ServerCluster(&cluster),
        |_, _conf| {},
        &server_configs,
        pd_ctl.as_ref(),
        &upgrade_switches,
    );

    let mut cluster = cluster.into_inner();

    // Finish.
    info!("test finished, stopping all workers");
    running.stop();
    block_on(futures::future::try_join_all(async_handles)).unwrap();

    // NOTE: Start cluster for verify it's NOT up.
    runtime.block_on(async {
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

    let rfstore_propose_switch_mem_table =
        rfstore::store::metrics::STORE_PROPOSE_SWITCH_MEM_TABLE_COUNTER.get();
    assert!(rfstore_propose_switch_mem_table > 0);

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

fn prepare_tikv_servers(
    tag: String,
    env_key: &str,
    data_path: PathBuf,
    working_path: PathBuf,
    nodes_count: usize,
    security_conf: &SecurityConfig,
) -> TikvServers {
    let tikv_server_bin =
        std::env::var(env_key).unwrap_or_else(|_| panic!("env {} is not set", env_key));
    TikvServers::new(
        tag,
        PathBuf::from(tikv_server_bin),
        data_path,
        working_path,
        nodes_count,
        MEMORY_CAPACITY_RATIO,
        security_conf,
    )
}

fn prepare_tikv_workers(
    tag: String,
    env_key: &str,
    working_path: PathBuf,
    security_conf: &SecurityConfig,
) -> TikvWorkers {
    let tikv_worker_bin =
        std::env::var(env_key).unwrap_or_else(|_| panic!("env {} is not set", env_key));
    TikvWorkers::new(
        tag,
        PathBuf::from(tikv_worker_bin),
        working_path,
        security_conf,
    )
}

fn prepare_cluster(
    dfs_config: &DFSConfig,
    security_conf: &SecurityConfig,
    nodes_count: usize,
    initial_keyspace_count: usize,
    switches: &Switches,
    _upgrade_switches: &UpgradeTestSwitches,
    tc: &TidbCluster,
) -> (ServerCluster, TikvServers, TikvWorkers) {
    let mut rng = thread_rng();
    let nodes = alloc_node_id_vec(nodes_count);
    let tikv_worker_nodes = alloc_node_id_vec(TIKV_WORKERS_COUNT);
    let update_conf_fn = generate_update_conf_fn(
        dfs_config,
        security_conf,
        nodes_count,
        &tikv_worker_nodes,
        switches,
    );
    let update_conf_fn_override = |node_id: u16, conf: &mut TikvConfig| {
        update_conf_fn(node_id, conf);

        // Old version (release build) will check the config value (see #3494).
        conf.rfengine.rlog_file_size = ReadableSize(rfengine::engine::MIN_RLOG_FILE_SIZE);
    };
    let pd_wrapper =
        PdWrapper::new_real(tc.pd.endpoints(), security_conf, PD_CLIENT_UPDATE_INTERVAL);
    let mut cluster = ServerClusterBuilder::new(vec![], |_, _| {})
        .pd(pd_wrapper)
        .memory_capacity_ratio(MEMORY_CAPACITY_RATIO)
        .build();

    // Start tikv-servers.
    let tikv_servers = prepare_tikv_servers(
        "tikv_servers".to_string(),
        TIKV_SERVER_BIN_ENV_KEY,
        cluster.data_dir().to_path_buf(),
        tc.data_path().to_path_buf(),
        nodes_count,
        security_conf,
    );
    for node_id in nodes {
        // Start tikv-servers one by one to work around the conflict on bootstrap
        // cluster.
        tikv_servers.start_node(node_id, update_conf_fn_override);
        block_on(tikv_servers.must_healthy(node_id, WAIT_TIKV_SERVER_HEALTHY_TIMEOUT));
    }
    for (node_id, conf) in tikv_servers.configs() {
        cluster.update_node_config(node_id, conf);
    }

    // Start tikv-workers.
    let tikv_worker_opts = TikvWorkerOptions {
        cop_block_cache_size: COP_BLOCK_CACHE_SIZE,
        ..Default::default()
    };
    cluster.generate_tikv_worker_configs(tikv_worker_nodes.clone(), tikv_worker_opts);
    let tikv_workers = prepare_tikv_workers(
        "tikv_workers".to_string(),
        TIKV_WORKER_BIN_ENV_KEY,
        tc.data_path().to_path_buf(),
        security_conf,
    );
    tikv_workers.start_all(cluster.tikv_worker_configs());
    block_on(tikv_workers.must_all_healthy(WAIT_TIKV_WORKER_HEALTHY_TIMEOUT));

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

    (cluster, tikv_servers, tikv_workers)
}

fn store_is_down(store: &StoreInfo) -> bool {
    store.store.state_name != "Up"
}

fn store_is_up(store: &StoreInfo) -> bool {
    store.store.state_name == "Up"
}

fn must_wait_store_state<F>(ctx: &str, pd_ctl: &PdControl, store_id: u64, expect: F)
where
    F: Fn(&pd_client::pd_control::StoreInfo) -> bool,
{
    must_wait(
        || {
            let store = block_on(pd_ctl.get_store(store_id)).unwrap();
            expect(&store)
        },
        WAIT_STORE_STATE_TIMEOUT.as_secs() as usize,
        || {
            let store = block_on(pd_ctl.get_store(store_id)).unwrap();
            format!("{ctx}: store state not match: {store:?}")
        },
    );
}

// TODO: rolling switch.
fn switch_workers_version(
    from: Workers<'_>,
    to: Workers<'_>,
    confs: &HashMap<u16 /* idx */, cloud_worker::Config>,
) {
    from.stop_all();
    to.start_all(confs);
    info!("tikv-worker: switch version finished"; "from" => from.tag(), "to" => to.tag());
}

fn switch_servers_version<F>(
    from: Servers<'_>,
    to: Servers<'_>,
    update_conf: F,
    configs: &HashMap<u16, TikvConfig>,
    pd_ctl: &PdControl,
    upgrade_switches: &UpgradeTestSwitches,
) where
    F: Fn(u16, &mut TikvConfig),
{
    block_on(from.must_all_healthy(CHECK_HEALTHY_TIMEOUT));
    let nodes = from.get_all_nodes();
    assert_eq!(nodes.len(), NODES_COUNT);
    for node_id in nodes {
        // Sleep first to have longer test duration for different version between
        // servers & workers.
        std::thread::sleep(TIKV_STORE_RESTART_INTERVAL);

        let conf = configs.get(&node_id).unwrap();
        let store = block_on(pd_ctl.find_store_by_status_address(&conf.server.status_addr))
            .unwrap()
            .unwrap();
        let store_id = store.store.id;

        if upgrade_switches.graceful_restart {
            info!("switch version: evict leader"; "node_id" => node_id, "store_id" => store_id, "store" => ?store);
            let (store, scheduler_name) =
                block_on(pd_ctl.evict_store_leaders(store_id, EVICT_LEADERS_TIMEOUT)).unwrap();
            if store.status.leader_count > 0 {
                warn!("switch version: store still has leaders after evicting"; "node_id" => node_id, "store" => ?store);
            }
            match block_on(pd_ctl.remove_scheduler(&scheduler_name)) {
                Ok(_) => {}
                Err(pd_control::Error::NotFound(msg)) => {
                    // Maybe caused by request retry.
                    warn!("switch version: scheduler already removed"; "node_id" => node_id, "msg" => msg);
                }
                Err(err) => {
                    panic!(
                        "switch version: remove scheduler failed, node_id: {}, err: {:?}",
                        node_id, err
                    );
                }
            }
        }

        info!("switch version: stop server"; "node_id" => node_id, "from" => from.tag());
        from.stop_node(node_id, false);
        must_wait_store_state(
            "switch version: wait for store is down",
            pd_ctl,
            store_id,
            store_is_down,
        );

        info!("switch version: start server"; "node_id" => ?node_id, "to" => to.tag());
        to.start_node(node_id, store_id, &update_conf);
        must_wait_store_state(
            "switch version: wait for store is up",
            pd_ctl,
            store_id,
            store_is_up,
        );
    }
    info!("tikv-server: switch version finished"; "from" => from.tag(), "to" => to.tag());
}

#[allow(unused)]
fn update_servers_configs<F>(
    servers: Servers<'_>,
    update_conf: F,
    configs: &HashMap<u16, TikvConfig>,
    pd_ctl: &PdControl,
    upgrade_switches: &UpgradeTestSwitches,
) where
    F: Fn(u16, &mut TikvConfig),
{
    switch_servers_version(
        servers.clone(),
        servers,
        update_conf,
        configs,
        pd_ctl,
        upgrade_switches,
    );
}

enum Workers<'a> {
    TikvWorkers(&'a TikvWorkers),
    ServerCluster(&'a RefCell<ServerCluster>),
}

impl Workers<'_> {
    fn tag(&self) -> &str {
        match self {
            Self::TikvWorkers(workers) => workers.tag(),
            Self::ServerCluster(_) => "server_cluster",
        }
    }

    fn stop_all(&self) {
        match self {
            Self::TikvWorkers(workers) => {
                block_on(workers.must_all_healthy(CHECK_HEALTHY_TIMEOUT));
                workers.stop_all();
            }
            Self::ServerCluster(cluster) => {
                let mut cluster = cluster.borrow_mut();
                block_on(cluster.tikv_workers_must_healthy(CHECK_HEALTHY_TIMEOUT));
                cluster.stop_tikv_workers();
            }
        }
    }

    fn start_all(&self, confs: &HashMap<u16 /* idx */, cloud_worker::Config>) {
        match self {
            Self::TikvWorkers(workers) => {
                workers.start_all(confs);
                block_on(workers.must_all_healthy(WAIT_TIKV_WORKER_HEALTHY_TIMEOUT));
            }
            Self::ServerCluster(cluster) => {
                let mut cluster = cluster.borrow_mut();
                cluster.start_tikv_workers_on_existed_configs(TIKV_WORKER_THREADS_COUNT);
                block_on(cluster.tikv_workers_must_healthy(WAIT_TIKV_WORKER_HEALTHY_TIMEOUT));
            }
        }
    }
}

#[derive(Clone)]
enum Servers<'a> {
    TikvServers(&'a TikvServers),
    ServerCluster(&'a RefCell<ServerCluster>),
}

impl Servers<'_> {
    fn tag(&self) -> &str {
        match self {
            Self::TikvServers(servers) => servers.tag(),
            Self::ServerCluster(_) => "server_cluster",
        }
    }

    fn stop_node(&self, node_id: u16, force: bool) {
        match self {
            Self::TikvServers(servers) => {
                let exit_status = servers.stop_node(node_id, force);
                if !force && !exit_status.success() {
                    panic!(
                        "tikv-server exit with error, node_id {}, exit_status {:?}",
                        node_id, exit_status
                    );
                }
            }
            Self::ServerCluster(cluster) => {
                cluster.borrow_mut().stop_node_force(node_id, force);
            }
        }
    }

    fn start_node<F>(&self, node_id: u16, store_id: u64, update_conf: F)
    where
        F: Fn(u16, &mut TikvConfig),
    {
        match self {
            Self::TikvServers(servers) => {
                servers.start_node(node_id, update_conf);
                block_on(servers.must_healthy(node_id, WAIT_TIKV_SERVER_HEALTHY_TIMEOUT));
            }
            Self::ServerCluster(cluster) => {
                let mut cluster = cluster.borrow_mut();
                cluster.start_node(node_id, update_conf);
                let new_store_id = cluster.get_store_id(node_id);
                assert_eq!(new_store_id, store_id);
            }
        }
    }

    async fn must_all_healthy(&self, timeout: Duration) {
        match self {
            Self::TikvServers(servers) => {
                servers.must_all_healthy(timeout).await;
            }
            Self::ServerCluster(_) => {}
        }
    }

    fn get_all_nodes(&self) -> Vec<u16> {
        match self {
            Self::TikvServers(servers) => servers.get_all_nodes(),
            Self::ServerCluster(cluster) => cluster.borrow().get_nodes(),
        }
    }
}

fn random_tikv_servers_restart(tikv_servers: &TikvServers) {
    let mut rng = thread_rng();

    // Ref: random::random_node_start
    sleep(Duration::from_secs(rng.gen_range(3..17)));

    let node_id = tikv_servers.random_restart_node(&mut rng).unwrap();
    block_on(tikv_servers.must_healthy(node_id, WAIT_TIKV_SERVER_HEALTHY_TIMEOUT));
    NODE_RESTART_COUNTER.fetch_add(1, Ordering::Relaxed);
}

#[derive(Debug)]
struct UpgradeTestSwitches {
    test_dur_before_upgrade: ReadableDuration,
    test_dur_after_upgrade: ReadableDuration,
    test_dur_after_downgrade: ReadableDuration,
    graceful_restart: bool,
}

impl UpgradeTestSwitches {
    fn from_env() -> Self {
        let mut rng = thread_rng();

        let test_dur_before_upgrade = env_param(
            "TEST_DUR_BEFORE_UPGRADE",
            ReadableDuration(TEST_DURATION_BEFORE_UPGRADE),
        );
        let test_dur_after_upgrade = env_param(
            "TEST_DUR_AFTER_UPGRADE",
            ReadableDuration(TEST_DURATION_AFTER_UPGRADE),
        );
        let test_dur_after_downgrade = env_param(
            "TEST_DUR_AFTER_DOWNGRADE",
            ReadableDuration(TEST_DURATION_AFTER_DOWNGRADE),
        );

        let graceful_restart = rng.gen_ratio(1, 5);

        Self {
            test_dur_before_upgrade,
            test_dur_after_upgrade,
            test_dur_after_downgrade,
            graceful_restart,
        }
    }
}
