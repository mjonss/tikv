// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs, ops,
    path::{Path, PathBuf},
    process::{
        Child, Command, {self},
    },
    sync::Arc,
    time::Duration,
};

use dashmap::DashMap;
use futures::executor::block_on;
use grpcio::EnvBuilder;
use kvengine::dfs::DFSConfig;
use kvproto::metapb;
use nix::sys::signal::Signal;
use pd_client::{
    PdClient,
    pd_control::{PdControl, PdScheduleConfig},
};
use security::{RestfulClient, SecurityConfig, SecurityManager};
use serde_derive::{Deserialize, Serialize};
use tempfile::TempDir;
use tikv_util::{box_err, config::ReadableDuration, info, time::Instant, warn};

use crate::{
    tiflash::{TiFlashRole, TiFlashServers},
    try_wait, try_wait_result_async,
};

// Set small update_interval to speed up recover new tso from legacy tso.
pub const PD_CLIENT_UPDATE_INTERVAL: ReadableDuration = ReadableDuration::secs(10);

const TXN_CHUNK_WRITER_CONCURRENCY: u64 = 4;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Sync + Send>>;

pub enum PdServerMode {
    Normal,
    MicroServices { tso_count: u16 },
}

/// Starts/stops pd-servers and provides interfaces such as PD endpoints.
pub struct PdServers {
    mode: PdServerMode,
    bin_path: PathBuf,
    data_path: PathBuf,
    port_base: u16,
    pre_alloc_keyspaces: u16,
    schedule_config: PdScheduleConfig,

    security_mgr: Arc<SecurityManager>,

    servers: DashMap<u16 /* idx */, process::Child>,
    tso_svcs: DashMap<u16 /* idx */, process::Child>,
}

impl PdServers {
    pub fn new(
        mode: PdServerMode,
        bin_path: PathBuf,
        data_path: PathBuf,
        port_base: u16,
        pre_alloc_keyspaces: u16,
        schedule_config: PdScheduleConfig,
        security_mgr: Arc<SecurityManager>,
    ) -> Self {
        Self {
            mode,
            bin_path,
            data_path,
            port_base,
            pre_alloc_keyspaces,
            schedule_config,
            security_mgr,
            servers: DashMap::new(),
            tso_svcs: DashMap::new(),
        }
    }

    pub fn mode(&self) -> &PdServerMode {
        &self.mode
    }

    pub fn start(&self, count: u16) {
        for idx in 0..count {
            self.start_single(idx, count);
        }

        if let PdServerMode::MicroServices { tso_count } = self.mode {
            for idx in 0..tso_count {
                self.start_single_tso_service(idx);
            }
        }
    }

    fn start_single(&self, idx: u16, total_count: u16) {
        let data_dir = self.data_path.join(format!("pd-{idx}"));
        let log_file = self.data_path.join(format!("pd-{idx}.log"));
        let initial_cluster = (0..total_count)
            .map(|i| format!("pd-{}=http://127.0.0.1:{}", i, self.peer_port(i)))
            .collect::<Vec<_>>()
            .join(",");

        let config_file = self.data_path.join(format!("pd-{idx}.toml"));
        let config = PdConfig {
            keyspace: PdKeyspaceConfig {
                pre_alloc: (1..=self.pre_alloc_keyspaces)
                    .map(|i| keyspace_name_by_idx(i))
                    .collect(),
                enable_global_safe_point_v2: true,
                ..Default::default()
            },
            schedule: self.schedule_config.clone(),
            ..Default::default()
        };
        let toml = toml::to_string(&config).unwrap();
        fs::write(&config_file, toml).unwrap();

        let mut cmd = Command::new(&self.bin_path);
        if matches!(self.mode, PdServerMode::MicroServices { .. }) {
            cmd.arg("services").arg("api");
        }
        cmd.arg(format!("--name=pd-{}", idx))
            .arg(format!("--data-dir={}", data_dir.display()))
            .arg(format!("--log-file={}", log_file.display()))
            .arg(format!(
                "--client-urls=http://127.0.0.1:{}",
                self.client_port(idx)
            ))
            .arg(format!(
                "--peer-urls=http://127.0.0.1:{}",
                self.peer_port(idx)
            ))
            .arg(format!("--initial-cluster={}", initial_cluster))
            .arg(format!("--config={}", config_file.display()));
        info!("start pd-server"; "cmd" => ?cmd);
        let child = cmd.spawn().unwrap();
        let old = self.servers.insert(idx, child);
        assert!(old.is_none(), "pd-{} already started (but dead ?)", idx);
    }

    fn start_single_tso_service(&self, idx: u16) {
        let log_file = self.data_path.join(format!("pd-tso-{idx}.log"));
        let mut cmd = Command::new(&self.bin_path);
        cmd.arg("services")
            .arg("tso")
            .arg(format!(
                "--backend-endpoints={}",
                self.endpoints_with_scheme().join(",")
            ))
            .arg(format!(
                "--listen-addr=http://127.0.0.1:{}",
                self.tso_svc_port(idx)
            ))
            .arg(format!(
                "--advertise-listen-addr=http://127.0.0.1:{}",
                self.tso_svc_port(idx)
            ))
            .arg(format!("--log-file={}", log_file.display()));
        info!("start pd-server tso"; "cmd" => ?cmd);
        let child = cmd.spawn().unwrap();
        let old = self.tso_svcs.insert(idx, child);
        assert!(old.is_none(), "pd-tso-{} already started (but dead ?)", idx);
    }

    pub fn client_port(&self, idx: u16) -> u16 {
        self.port_base + idx * 10
    }

    pub fn peer_port(&self, idx: u16) -> u16 {
        self.client_port(idx) + 1
    }

    pub fn tso_svc_port(&self, idx: u16) -> u16 {
        self.client_port(idx) + 100
    }

    pub fn endpoints(&self) -> Vec<String> {
        self.servers
            .iter()
            .map(|kv| {
                let idx = *kv.key();
                format!("127.0.0.1:{}", self.client_port(idx))
            })
            .collect()
    }

    pub fn endpoints_with_scheme(&self) -> Vec<String> {
        self.servers
            .iter()
            .map(|kv| {
                let idx = *kv.key();
                format!("http://127.0.0.1:{}", self.client_port(idx))
            })
            .collect()
    }

    pub async fn must_healthy(&self, timeout: Duration) {
        let pd_ctl = Arc::new(self.get_pd_control());
        try_wait_result_async(
            || {
                let pd_ctl = pd_ctl.clone();
                Box::pin(async move {
                    match pd_ctl.health().await {
                        Ok(health) if health => Ok(()),
                        Ok(_) => Err("PD unhealthy".to_string()),
                        Err(e) => {
                            let err = Err(format!("check PD healthy failed: {:?}", e));
                            info!("{:?}", err);
                            err
                        }
                    }
                })
            },
            timeout.as_secs() as usize,
        )
        .await
        .unwrap_or_else(|e| panic!("wait PD healthy timeout: {:?}", e));

        let pd_client = Arc::new(self.get_client_async().await);
        try_wait_result_async(
            || {
                let pd_client = pd_client.clone();
                Box::pin(async move {
                    match pd_client.get_tso().await {
                        Ok(ts) => {
                            info!("get_tso: {:?}", ts);
                            Ok(())
                        }
                        Err(e) => {
                            let err = Err(format!("get_tso failed: {:?}", e));
                            info!("{:?}", err);
                            err
                        }
                    }
                })
            },
            timeout.as_secs() as usize,
        )
        .await
        .unwrap_or_else(|e| panic!("wait PD get_tso timeout: {:?}", e));
        info!("PD is ready");
    }

    pub fn get_pd_control(&self) -> PdControl {
        let endpoints = self.endpoints();
        assert!(
            !endpoints.is_empty(),
            "no PD endpoints, invoke start() first"
        );
        let cfg = pd_client::Config::new(endpoints);
        let mgr = self.security_mgr.clone();
        PdControl::new(cfg, mgr).unwrap()
    }

    pub fn get_client(&self) -> pd_client::RpcClient {
        block_on(self.get_client_async())
    }

    pub async fn get_client_async(&self) -> pd_client::RpcClient {
        let endpoints = self.endpoints();
        let env = Arc::new(EnvBuilder::new().cq_count(1).build());
        let mgr = self.security_mgr.clone();
        let mut cfg = pd_client::Config::new(endpoints);
        cfg.update_interval = PD_CLIENT_UPDATE_INTERVAL;
        cfg.validate().unwrap();
        pd_client::RpcClient::new_async(&cfg, Some(env), mgr)
            .await
            .unwrap_or_else(|e| panic!("failed to create rpc client: {:?}", e))
    }

    // TODO: Randomly force stop.
    pub fn stop(&self, idx: u16, children: &DashMap<u16, process::Child>) {
        let (_, mut child) = children.remove(&idx).unwrap();
        // Ref: https://github.com/tidbcloud/pd-cse/blob/release-8.1-keyspace/pkg/mcs/tso/server/server.go#L498
        send_signal_to_child(&child, Signal::SIGTERM).unwrap();
        let exit_status = child.wait().unwrap();
        if !exit_status.success() {
            warn!("pd-{} exit with error", idx; "exit_status" => ?exit_status);
        }
    }

    pub async fn restart_tso_svc(&self, idx: u16, stop_dur: Duration, healthy_timeout: Duration) {
        self.stop(idx, &self.tso_svcs);
        info!("tso-svc stopped"; "idx" => idx);
        tokio::time::sleep(stop_dur).await;
        self.start_single_tso_service(idx);
        self.tso_svc_must_health(idx, healthy_timeout).await;
        info!("tso-svc started"; "idx" => idx);
    }

    pub async fn tso_svc_must_health(&self, idx: u16, timeout: Duration) {
        let client = RestfulClient::new(
            "tso-svc-ctl",
            vec![format!("127.0.0.1:{}", self.tso_svc_port(idx))],
            self.security_mgr.clone(),
        )
        .unwrap();
        try_wait_result_async(
            || {
                let client = client.clone();
                Box::pin(async move {
                    let res = client.get::<TsoSvcStatus>(PD_TSO_SVC_STATUS_PATH).await;
                    match res {
                        Ok(status) => {
                            info!("tso-svc[{}]: status: {:?}", idx, status);
                            Ok(())
                        }
                        Err(e) => {
                            warn!("tso-svc[{}]: check healthy failed: {:?}", idx, e);
                            Err(e)
                        }
                    }
                })
            },
            timeout.as_secs() as usize,
        )
        .await
        .unwrap_or_else(|e| panic!("wait PD TSO healthy timeout: {:?}", e));
        info!("PD TSO service is ready"; "idx" => idx);
    }

    pub fn stop_all(&self) {
        let tso_svcs = self.tso_svcs.iter().map(|kv| *kv.key()).collect::<Vec<_>>();
        for idx in tso_svcs {
            self.stop(idx, &self.tso_svcs);
        }

        let servers = self.servers.iter().map(|kv| *kv.key()).collect::<Vec<_>>();
        for idx in servers {
            self.stop(idx, &self.servers);
        }
    }
}

/// Starts/stops tidb-servers and provides interfaces such as host, port,
/// username, and password.
pub struct TidbServers {
    bin_path: PathBuf,
    data_path: PathBuf,
    port_base: u16,
    status_port_base: u16,

    security_mgr: Arc<SecurityManager>,

    children: DashMap<u16 /* idx */, process::Child>,

    next_gen: bool,
}

impl TidbServers {
    pub fn new(
        bin_path: PathBuf,
        data_path: PathBuf,
        port_base: u16,
        status_port_base: u16,
        security_mgr: Arc<SecurityManager>,
        next_gen: bool,
    ) -> Self {
        Self {
            bin_path,
            data_path,
            port_base,
            status_port_base,
            security_mgr,
            children: DashMap::new(),
            next_gen,
        }
    }

    pub fn root(&self, idx: u16) -> String {
        if !self.next_gen {
            format!("{}.root", keyspace_name_by_idx(idx))
        } else {
            "root".to_string()
        }
    }

    pub fn port(&self, idx: u16) -> u16 {
        self.port_base + idx
    }

    pub fn status_port(&self, idx: u16) -> u16 {
        self.status_port_base + idx
    }

    pub fn conn_params(&self, idx: u16) -> ConnParams {
        ConnParams {
            host: "127.0.0.1".to_string(),
            port: self.port(idx),
            user: self.root(idx),
            password: "".to_string(),
        }
    }

    pub fn start(
        &self,
        idx: u16,
        pd_endpoints: &[String],
        log_level: &str,
        options: StartTidbOptions,
    ) {
        let pd_endpoints = pd_endpoints.join(",");
        let log_file = self.data_path.join(format!("tidb-{idx}.log"));
        let slow_log_file = self.data_path.join(format!("tidb-slow-{idx}.log"));

        let config_file = self.data_path.join(format!("tidb-{idx}.toml"));
        let columnar_store_type = if options.tiflash_disaggregated_mode {
            if options.enable_tiflash_write_node {
                "both".to_string()
            } else {
                "columnar".to_string()
            }
        } else {
            "tiflash".to_string()
        };
        let mut config = TidbConfig {
            keyspace_name: keyspace_name_by_idx(idx),
            enable_safe_point_v2: true,
            cse: CseConfig {
                columnar_store_type,
                enable_region_client: false,
            },
            ..Default::default()
        };
        if let Some(txn_file_min_mutation_size) = options.txn_file_min_mutation_size {
            config.tikv_client.txn_chunk_writer_addr = options.tikv_worker_addr;
            config.tikv_client.txn_chunk_writer_concurrency = TXN_CHUNK_WRITER_CONCURRENCY;
            config.tikv_client.txn_chunk_max_size = options.txn_chunk_max_size;
            config.tikv_client.txn_file_min_mutation_size = txn_file_min_mutation_size;
        }
        if options.tiflash_disaggregated_mode {
            config.disaggregated_tiflash = true;
            config.use_autoscaler = false;
            config.split_table = false;
            let tiflash_replicas_config = TiFlashReplicas {
                group_id: "enable_s3_wn_region".to_string(),
                extra_s3_rule: false,
                min_count: 1,
                constraints: vec![
                    TiFlashReplicasConstraints {
                        key: "engine".to_string(),
                        op: "in".to_string(),
                        values: vec!["tiflash".to_string()],
                    },
                    TiFlashReplicasConstraints {
                        key: "engine_role".to_string(),
                        op: "in".to_string(),
                        values: vec!["write".to_string()],
                    },
                ],
            };
            config.tiflash_replicas = Some(tiflash_replicas_config);
        }
        let toml = toml::to_string(&config).unwrap();
        fs::write(&config_file, toml).unwrap();

        let mut cmd = Command::new(&self.bin_path);
        cmd.arg(format!("-L={}", log_level))
            .arg("--store=tikv")
            .arg("--host=127.0.0.1")
            .arg("--status-host=127.0.0.1")
            .arg(format!("--path={}", pd_endpoints))
            .arg(format!("-P={}", self.port(idx)))
            .arg(format!("--status={}", self.status_port(idx)))
            .arg(format!("--log-file={}", log_file.display()))
            .arg(format!("--log-slow-query={}", slow_log_file.display()))
            .arg(format!("--config={}", config_file.display()));
        if !options.gc_interval.is_empty() {
            cmd.env("HACK_GC_RUN_INTERVAL", &options.gc_interval);
        }
        if !options.gc_lifetime.is_empty() {
            cmd.env("HACK_GC_LIFE_TIME", &options.gc_lifetime);
        }
        info!("start tidb-server"; "cmd" => ?cmd);
        let child = cmd.spawn().unwrap();
        let old = self.children.insert(idx, child);
        assert!(old.is_none(), "tidb-{} already started", idx);
    }

    pub fn get_tidb_control(&self, idx: u16) -> TidbControl {
        let endpoint = format!("127.0.0.1:{}", self.status_port(idx));
        TidbControl::new(endpoint, self.security_mgr.clone(), self.next_gen)
    }

    pub async fn must_healthy(&self, idx: u16, timeout: Duration) {
        let tidb_ctl = self.get_tidb_control(idx);
        try_wait_result_async(
            || {
                let tidb_ctl = tidb_ctl.clone();
                Box::pin(async move {
                    match tidb_ctl.health().await {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            let err = Err(format!("check TiDB healthy failed: {:?}", e));
                            info!("{:?}", err);
                            err
                        }
                    }
                })
            },
            timeout.as_secs() as usize,
        )
        .await
        .unwrap_or_else(|e| panic!("wait TiDB-{} healthy timeout: {:?}", idx, e));
        info!("TiDB-{idx} is ready");
    }

    pub async fn must_all_healthy(&self, timeout: Duration) {
        let all = self.get_all_indexes();
        for idx in all {
            self.must_healthy(idx, timeout).await;
        }
    }

    pub fn stop(&self, idx: u16) {
        let (_, mut child) = self.children.remove(&idx).unwrap();
        // TODO: gracefully kill by SIGINT
        child.kill().unwrap_or_else(|err| {
            panic!("tidb-{} has exited unexpectedly: {}", idx, err);
        })
    }

    pub fn stop_all(&self) {
        let all = self.get_all_indexes();
        for idx in all {
            self.stop(idx);
        }
    }

    fn get_all_indexes(&self) -> Vec<u16> {
        self.children.iter().map(|kv| *kv.key()).collect::<Vec<_>>()
    }
}

#[derive(Clone)]
pub struct TidbCluster {
    inner: Arc<TidbClusterCore>,
}

impl ops::Deref for TidbCluster {
    type Target = TidbClusterCore;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TidbCluster {
    pub fn new(
        pd_mode: PdServerMode,
        pd_bin: PathBuf,
        pd_port_base: u16,
        pd_schedule_config: PdScheduleConfig,
        tidb_bin: PathBuf,
        tidb_port_base: u16,
        tidb_status_port_base: u16,
        tiflash_bin: Option<PathBuf>,
        pre_alloc_keyspaces: u16,
        security_conf: &SecurityConfig,
        next_gen: bool,
    ) -> Self {
        check_binary("pd_server", &pd_bin);
        check_binary("tidb-server", &tidb_bin);
        if let Some(ref tiflash_bin) = tiflash_bin {
            check_binary("tiflash", tiflash_bin);
        }

        let security_mgr = Arc::new(SecurityManager::new(security_conf).unwrap());
        let base_path = tempfile::Builder::new().prefix("tc_").tempdir().unwrap();

        let pd = PdServers::new(
            pd_mode,
            pd_bin,
            base_path.path().to_owned(),
            pd_port_base,
            pre_alloc_keyspaces,
            pd_schedule_config,
            security_mgr.clone(),
        );
        let tidb = TidbServers::new(
            tidb_bin,
            base_path.path().to_owned(),
            tidb_port_base,
            tidb_status_port_base,
            security_mgr.clone(),
            next_gen,
        );
        let tiflash = tiflash_bin.map(|tiflash_bin| {
            TiFlashServers::new(tiflash_bin, base_path.path().to_owned(), security_mgr)
        });

        let inner = TidbClusterCore {
            data_path: base_path,
            pd,
            tidb,
            tiflash,
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn keyspace_name(idx: u16) -> String {
        keyspace_name_by_idx(idx)
    }

    pub fn get_idx_by_keyspace_name(name: &str) -> u16 {
        get_idx_from_keyspace_name(name)
    }
}

#[derive(Default, Clone)]
pub struct StartTidbOptions {
    pub tikv_worker_addr: String,
    pub txn_chunk_max_size: u64,
    pub txn_file_min_mutation_size: Option<u64>,
    pub gc_interval: String,
    pub gc_lifetime: String,
    pub tiflash_disaggregated_mode: bool,
    pub enable_tiflash_write_node: bool,
}

pub struct TidbClusterCore {
    data_path: TempDir,

    pub pd: PdServers,
    pub tidb: TidbServers,
    pub tiflash: Option<TiFlashServers>,
}

impl TidbClusterCore {
    pub async fn start_pd(&self, count: u16, timeout: Duration) {
        self.pd.start(count);
        // tikv-servers should be started after PD is healthy. Otherwise it will panic.
        self.pd.must_healthy(timeout).await;
    }

    pub async fn start_tidb(
        &self,
        count: u16,
        timeout: Duration,
        log_level: &str,
        options: StartTidbOptions,
    ) {
        let start_time = Instant::now_coarse();
        let pd_endpoints = self.pd.endpoints();
        // Start from 1 as keyspace 0 is reserved.
        for idx in 1..=count {
            self.tidb
                .start(idx, &pd_endpoints, log_level, options.clone());
        }
        self.tidb.must_all_healthy(timeout).await;
        info!("start TiDB success"; "takes" => ?start_time.saturating_elapsed());
    }

    pub async fn start_tiflash(
        &self,
        count: u16,
        dfs: &DFSConfig,
        timeout: Duration,
        tiflash_disaggregated_mode: bool,
        enable_tiflash_write_node: bool,
    ) {
        let Some(tiflash) = self.tiflash.as_ref() else {
            return;
        };

        let start_time = Instant::now_coarse();
        if tiflash_disaggregated_mode && enable_tiflash_write_node {
            tiflash.start_minio().await;
        }
        let pd_endpoints = self.pd.endpoints();
        for idx in 0..count {
            if !tiflash_disaggregated_mode {
                tiflash.start(idx, dfs.clone(), &pd_endpoints, TiFlashRole::Legacy);
            } else if enable_tiflash_write_node {
                tiflash.start(idx, dfs.clone(), &pd_endpoints, TiFlashRole::Write);
            }
        }
        // Start 1 compute node for compute mode.
        if tiflash_disaggregated_mode {
            tiflash.start(count, dfs.clone(), &pd_endpoints, TiFlashRole::Compute);
        }

        tiflash.must_all_healthy(timeout).await;
        // Note: TiFlash compute node may not register self to pd.
        if !tiflash_disaggregated_mode || enable_tiflash_write_node {
            self.wait_tiflash_up(count, timeout);
        }
        info!("start TiFlash success"; "takes" => ?start_time.saturating_elapsed());
    }

    pub fn wait_tiflash_up(&self, count: u16, timeout: Duration) {
        let pd_client = self.pd.get_client();
        let is_up_tiflash = |s: &metapb::Store| {
            s.get_state() == metapb::StoreState::Up
                && s.get_labels().iter().any(|l| {
                    l.key.to_lowercase() == "engine" && l.value.to_lowercase() == "tiflash"
                })
        };
        let ok = try_wait(
            || {
                let up_count = pd_client
                    .get_all_stores(true)
                    .unwrap()
                    .into_iter()
                    .filter(is_up_tiflash)
                    .count();
                up_count == count as usize
            },
            timeout.as_secs() as usize,
        );
        assert!(ok, "stores: {:?}", pd_client.get_all_stores(true).unwrap());
    }

    pub fn data_path(&self) -> &Path {
        self.data_path.path()
    }
}

pub struct ConnParams {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
}

impl ConnParams {
    pub fn conn_string(&self, db: &str) -> String {
        // mysql://user:password@host:port/db
        format!(
            "mysql://{}:{}@{}:{}/{db}",
            self.user, self.password, self.host, self.port
        )
    }
}

#[derive(Default, Serialize)]
#[serde(rename_all = "kebab-case")]
struct PdConfig {
    replication: PdReplicationConfig,
    keyspace: PdKeyspaceConfig,
    schedule: PdScheduleConfig,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct PdReplicationConfig {
    max_replicas: usize,
}

impl Default for PdReplicationConfig {
    fn default() -> Self {
        Self { max_replicas: 3 }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct PdKeyspaceConfig {
    pre_alloc: Vec<String>,
    enable_global_safe_point_v2: bool,
    disable_raw_kv_region_split: bool,
}

impl Default for PdKeyspaceConfig {
    fn default() -> Self {
        Self {
            pre_alloc: vec![],
            enable_global_safe_point_v2: true,
            disable_raw_kv_region_split: true,
        }
    }
}

const PD_TSO_SVC_STATUS_PATH: &str = "status";

// Ref: https://github.com/tidbcloud/pd-cse/blob/release-7.1-keyspace/pkg/mcs/utils/util.go, StatusHandler
// ```json
// {
//     "build_ts": "2024-01-16 03:25:40",
//     "version": "v7.1.1-serverless",
//     "git_hash": "d6175b7123e9e9ead933d26e9a9d331d5c53e0c0",
//     "start_timestamp": 1705408116
// }
// ```
#[derive(Default, Deserialize, Debug)]
#[serde(default)]
struct TsoSvcStatus {
    build_ts: String,
    version: String,
    git_hash: String,
    start_timestamp: u64,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "kebab-case")]
struct TidbConfig {
    keyspace_name: String,
    split_table: bool,          // Set to false.
    enable_safe_point_v2: bool, // Deprecated since v7.5
    disaggregated_tiflash: bool,
    use_autoscaler: bool,
    tikv_client: TikvClientConfig,
    tiflash_replicas: Option<TiFlashReplicas>,
    security: TidbConfigSecurity,
    cse: CseConfig,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "kebab-case")]
struct TidbConfigSecurity {
    enable_sem: bool, // Disable "sem" to enable async commit & 1pc.
}

#[derive(Default, Serialize)]
#[serde(rename_all = "kebab-case")]
struct CseConfig {
    columnar_store_type: String,
    enable_region_client: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct TiFlashReplicas {
    group_id: String,
    extra_s3_rule: bool,
    min_count: usize,
    constraints: Vec<TiFlashReplicasConstraints>,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct TiFlashReplicasConstraints {
    key: String,
    op: String,
    values: Vec<String>,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "kebab-case")]
struct TikvClientConfig {
    txn_chunk_writer_addr: String,
    txn_chunk_writer_concurrency: u64,
    txn_chunk_max_size: u64,
    txn_file_min_mutation_size: u64,
}

const TIDB_HEALTH_PATH: &str = "health";
const TIDB_STATUS_PATH: &str = "status";

// Ref: https://github.com/tidbcloud/tidb-cse/blob/release-7.1-keyspace/server/health_handler.go
// {"status":"up","token":"keyspace_a"}
#[derive(Default, Deserialize, Debug)]
#[serde(default)]
struct TidbHealth {
    status: String, // "up" or "down"
    token: String,  // keyspace name
}

// Ref: https://github.com/pingcap/tidb/blob/v8.5.2/pkg/server/http_status.go#L563
#[derive(Default, Deserialize, Debug)]
#[serde(default)]
struct TidbStatus {
    version: String,
    status: TidbStatusDetail,
}

#[derive(Default, Deserialize, Debug)]
#[serde(default)]
struct TidbStatusDetail {
    init_stats_percentage: f64,
}

/// TidbControl provides access to HTTP APIs of TiDB, which are not included in
/// gRPC interface. It's also expected to act like the tool `tidb-ctl`.
#[derive(Clone)]
pub struct TidbControl {
    client: RestfulClient,
    next_gen: bool,
}

impl TidbControl {
    pub fn new(endpoint: String, security_mgr: Arc<SecurityManager>, next_gen: bool) -> Self {
        let client = RestfulClient::new("tidb-ctl", vec![endpoint], security_mgr).unwrap();
        Self { client, next_gen }
    }

    pub async fn health(&self) -> Result<()> {
        if !self.next_gen {
            self.query_health().await
        } else {
            self.query_status().await
        }
    }

    async fn query_health(&self) -> Result<()> {
        let resp = self.client.get::<TidbHealth>(TIDB_HEALTH_PATH).await;
        match resp {
            Ok(health) if health.status == "up" => {
                info!("tidb-ctl: health: {:?}", health);
                Ok(())
            }
            Ok(health) => Err(box_err!("tidb-ctl: unhealthy: {:?}", health)),
            Err(e) => Err(box_err!("tidb-ctl: check healthy failed: {:?}", e)),
        }
    }

    async fn query_status(&self) -> Result<()> {
        let resp = self.client.get::<TidbStatus>(TIDB_STATUS_PATH).await;
        match resp {
            Ok(status) if status.status.init_stats_percentage >= 90.0 => {
                info!("tidb-ctl: status: {:?}", status);
                Ok(())
            }
            Ok(status) => Err(box_err!("tidb-ctl: not ready: {:?}", status)),
            Err(e) => Err(box_err!("tidb-ctl: check status failed: {:?}", e)),
        }
    }
}

/// On real PD, keyspaces are indexed by name other than id.
/// So we need the conversion between keyspace id & name, to locate TiDB in
/// TidbCluster.
const KEYSPACE_NAME_PREFIX: &str = "ks";

fn keyspace_name_by_idx(idx: u16) -> String {
    format!("{KEYSPACE_NAME_PREFIX}{idx}")
}

fn get_idx_from_keyspace_name(name: &str) -> u16 {
    assert_eq!(&name[..KEYSPACE_NAME_PREFIX.len()], KEYSPACE_NAME_PREFIX);
    name[KEYSPACE_NAME_PREFIX.len()..].parse().unwrap()
}

/// Check binaries by running with "-V".
pub(crate) fn check_binary(name: &str, bin_path: &Path) {
    let mut cmd = Command::new(bin_path);
    if name == "tiflash" {
        cmd.arg("--version");
    } else {
        cmd.arg("-V");
    }
    let output = cmd.output().unwrap_or_else(|e| {
        panic!("check_binary {} at {:?} failed: {:?}", name, bin_path, e);
    });
    assert!(
        output.status.success(),
        "{} --version failed: {:?}",
        name,
        output
    );
    info!(
        "{} version {}",
        name,
        String::from_utf8_lossy(&output.stdout).as_ref()
    );
}

pub(crate) fn send_signal_to_child(child: &Child, signal: Signal) -> nix::Result<()> {
    use nix::{libc::pid_t, sys::signal::kill, unistd::Pid};

    let pid = Pid::from_raw(child.id() as pid_t);
    kill(pid, signal)
}
