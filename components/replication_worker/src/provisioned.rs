// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use pd_client::PdClient;
use security::SecurityConfig;

use crate::{
    KeyspaceService, KeyspaceStates, ReplicationWorkerConfig, Result, bootstrap,
    util::new_keyspace_pd_client,
};

pub(crate) struct KeyspaceProvisionedService {
    keyspace_id: u32,
    conf: ReplicationWorkerConfig,
    sec_conf: SecurityConfig,
    pub(crate) states: KeyspaceStates,
    pd_client: Option<Arc<dyn PdClient>>,
}

impl KeyspaceProvisionedService {
    pub(crate) fn new(
        keyspace_id: u32,
        conf: &ReplicationWorkerConfig,
        sec_conf: &SecurityConfig,
        states: KeyspaceStates,
    ) -> Self {
        Self {
            keyspace_id,
            conf: conf.clone(),
            sec_conf: sec_conf.clone(),
            states,
            pd_client: None,
        }
    }
}

#[async_trait]
impl KeyspaceService for KeyspaceProvisionedService {
    fn keyspace_id(&self) -> u32 {
        self.keyspace_id
    }

    async fn start(&mut self) -> Result<()> {
        let pd_url = self.states.pd_url.clone();
        let pd_client = new_keyspace_pd_client(pd_url, &self.sec_conf).await;
        bootstrap(
            pd_client.clone(),
            self.conf.merged_engine.merged_store_id,
            self.conf.advertise_addr.clone(),
        )
        .await?;
        self.pd_client = Some(pd_client);
        Ok(())
    }

    async fn destroy(&mut self) -> Result<()> {
        Ok(())
    }

    fn get_states(&self) -> &KeyspaceStates {
        &self.states
    }

    fn get_states_mut(&mut self) -> &mut KeyspaceStates {
        &mut self.states
    }

    fn get_pd_client(&self) -> Arc<dyn PdClient> {
        self.pd_client.as_ref().unwrap().clone()
    }
}

#[cfg(feature = "testexport")]
pub mod local_provider {
    use std::{
        fs,
        path::PathBuf,
        process::{Child, Command},
        sync::Arc,
        time::Duration,
    };

    use futures::executor::block_on;
    use nix::{
        sys::signal::{Signal, kill},
        unistd::Pid,
    };
    use pd_client::pd_control::PdControl;
    use security::{RestfulClient, SecurityConfig, SecurityManager};
    use serde_derive::{Deserialize, Serialize};
    use tikv_util::{box_err, info, retry::try_wait_result_async};

    use crate::Result;

    pub struct LocalProvider {
        pub keyspace_id: u32,
        pub data_dir: PathBuf,
        pd_bin_path: String,
        cdc_bin_path: String,
        tidb_bin_path: String,
        base_port: u16,
        log_level: String, // E.g., "info", "debug".

        pub pd_child: Option<Child>,
        pub cdc_child: Option<Child>,
        pub tidb_child: Option<Child>,

        security_mgr: Arc<SecurityManager>,
    }

    impl LocalProvider {
        pub fn new(keyspace_id: u32, data_dir: PathBuf, base_port: u16, log_level: String) -> Self {
            let security_mgr = SecurityManager::new(&SecurityConfig::default()).unwrap();
            Self {
                keyspace_id,
                data_dir,
                pd_bin_path: std::env::var("REP_PD_BIN").unwrap(),
                cdc_bin_path: std::env::var("CDC_BIN").unwrap(),
                tidb_bin_path: std::env::var("TIDB_BIN").unwrap_or_default(),
                base_port,
                log_level,
                pd_child: None,
                cdc_child: None,
                tidb_child: None,
                security_mgr: Arc::new(security_mgr),
            }
        }

        pub fn start(&mut self, timeout: Duration) {
            self.start_local_pd();
            self.start_local_cdc(&self.cdc_server_addr(), &self.pd_client_url());
            self.tidb_child = Some(self.start_local_tidb());

            let wait_healthy = async {
                let (pd_res, tidb_res) = tokio::join!(
                    self.wait_pd_healthy(timeout),
                    self.wait_tidb_healthy(timeout)
                );
                pd_res.expect("Wait for PD health timeout");
                tidb_res.expect("Wait for TiDB health timeout");
            };
            block_on(wait_healthy);
        }

        pub fn start_local_pd(&mut self) {
            let name = format!("rep-pd-{}", self.keyspace_id);
            let data_dir = self.data_dir.join(&name);
            fs::create_dir_all(&data_dir).unwrap();
            let log_file = self.data_dir.join(format!("{}.log", name));
            let initial_cluster = format!("pd={}", self.local_pd_peer_url());
            let config_file = self.data_dir.join(format!("{}.toml", name));
            let pd_config = "[replication]\nmax-replicas = 1\n";
            fs::write(&config_file, pd_config).unwrap();
            let mut cmd = Command::new(&self.pd_bin_path);
            cmd.arg("--name=pd")
                .arg(format!("--data-dir={}", data_dir.display()))
                .arg(format!("--log-file={}", log_file.display()))
                .arg(format!("--log-level={}", &self.log_level))
                .arg(format!("--client-urls={}", self.pd_client_url()))
                .arg(format!("--peer-urls={}", self.local_pd_peer_url()))
                .arg(format!("--initial-cluster={}", initial_cluster))
                .arg(format!("--config={}", config_file.display()));
            info!("start pd-server"; "cmd" => ?cmd);
            self.pd_child = Some(cmd.spawn().unwrap())
        }

        pub fn start_local_cdc(&mut self, cdc_addr: &str, pd_url: &str) {
            let keyspace_id = self.keyspace_id;
            let data_dir = self.data_dir.join(format!("cdc-{keyspace_id}"));
            let log_file = self.data_dir.join(format!("cdc-{keyspace_id}.log"));
            let mut cmd = Command::new(&self.cdc_bin_path);
            cmd.arg("server")
                .arg(format!("--addr={}", cdc_addr))
                .arg(format!("--log-file={}", log_file.display()))
                .arg(format!("--log-level={}", &self.log_level))
                .arg(format!("--data-dir={}", data_dir.display()))
                .arg(format!("--pd={}", pd_url));
            info!("start cdc-server"; "cmd" => ?cmd);
            self.cdc_child = Some(cmd.spawn().unwrap());
        }

        fn start_local_tidb(&self) -> Child {
            let mut cmd = Command::new(&self.tidb_bin_path);
            let keyspace_id = self.keyspace_id;
            let data_dir = self.data_dir.join(format!("tidb-{keyspace_id}"));
            let log_file = self.data_dir.join(format!("rep-tidb-{keyspace_id}.log"));
            let slow_log_file = self
                .data_dir
                .join(format!("rep-tidb-slow-{keyspace_id}.log"));
            let config_file = self.data_dir.join(format!("rep-tidb-{keyspace_id}.toml"));
            let config = LocalTidbConfig::default();
            fs::write(&config_file, toml::to_string(&config).unwrap()).unwrap();
            cmd.arg(format!("--P={}", self.local_tidb_port()))
                .arg(format!("--log-file={}", log_file.display()))
                .arg(format!("--log-slow-query={}", slow_log_file.display()))
                .arg("--store=unistore")
                .arg(format!("--path={}", data_dir.display()))
                .arg(format!("--status={}", self.local_tidb_status_port()))
                .arg(format!("--config={}", config_file.display()));
            info!("start local tidb-server"; "cmd" => ?cmd);
            cmd.spawn().unwrap()
        }

        pub fn pd_client_url(&self) -> String {
            Self::local_proc_url(self.local_pd_client_port())
        }

        pub fn cdc_server_addr(&self) -> String {
            Self::local_proc_addr(self.local_cdc_server_port())
        }

        fn local_pd_peer_url(&self) -> String {
            Self::local_proc_url(self.local_pd_peer_port())
        }

        fn local_cdc_server_port(&self) -> u16 {
            self.base_port + 2000 + self.keyspace_id as u16
        }

        fn local_pd_peer_port(&self) -> u16 {
            self.base_port + 1000 + self.keyspace_id as u16
        }

        fn local_pd_client_port(&self) -> u16 {
            self.base_port + self.keyspace_id as u16
        }

        fn local_proc_addr(port: u16) -> String {
            format!("127.0.0.1:{}", port)
        }

        fn local_proc_url(port: u16) -> String {
            format!("http://{}", Self::local_proc_addr(port))
        }

        fn local_tidb_port(&self) -> u16 {
            self.base_port + 3000 + self.keyspace_id as u16
        }

        fn local_tidb_status_port(&self) -> u16 {
            self.base_port + 4000 + self.keyspace_id as u16
        }

        fn kill_process(tag: &str, mut child: Child) -> Result<()> {
            let pid = child.id();
            assert!(pid > 0);
            let pid = Pid::from_raw(pid as i32);
            kill(pid, Signal::SIGTERM)?;

            let exit_status = child.wait().unwrap();
            if !exit_status.success() {
                return Err(box_err!("{}: exit with error: {:?}", tag, exit_status));
            }
            Ok(())
        }

        pub fn destroy(&mut self) -> Result<()> {
            info!("destroy local provider");
            if let Some(cdc) = self.cdc_child.take() {
                Self::kill_process("rep-cdc", cdc)?;
            }
            if let Some(pd) = self.pd_child.take() {
                Self::kill_process("rep-pd", pd)?;
            }
            if let Some(tidb) = self.tidb_child.take() {
                Self::kill_process("rep-tidb", tidb)?;
            }
            Ok(())
        }

        pub fn restart_local_pd(&mut self) -> Result<()> {
            if let Some(pd) = self.pd_child.take() {
                Self::kill_process("rep-pd", pd)?;
            }
            self.start_local_pd();
            std::thread::sleep(Duration::from_secs(1));
            Ok(())
        }

        pub fn get_pd_control(&self) -> PdControl {
            let endpoints = vec![self.pd_client_url()];
            let cfg = pd_client::Config::new(endpoints);
            let mgr = self.security_mgr.clone();
            PdControl::new(cfg, mgr).unwrap()
        }

        pub async fn wait_pd_healthy(&self, timeout: Duration) -> Result<()> {
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
                timeout,
                || Duration::from_millis(200),
            )
            .await
            .map_err(|s| box_err!("{}", s))
        }

        pub async fn wait_tidb_healthy(&self, timeout: Duration) -> Result<()> {
            let status_url = Self::local_proc_url(self.local_tidb_status_port());
            let tidb_ctl = Arc::new(
                RestfulClient::new("tidb-ctl", vec![status_url], self.security_mgr.clone())
                    .unwrap(),
            );
            try_wait_result_async(
                || {
                    let tidb_ctl = tidb_ctl.clone();
                    Box::pin(async move {
                        match tidb_ctl.get::<TidbHealth>("health").await {
                            Ok(health) if health.status == "up" => Ok(()),
                            Ok(_) => Err("TiDB is not healthy".into()),
                            Err(e) => Err(format!("failed to get TiDB health: {:?}", e)),
                        }
                    })
                },
                timeout,
                || Duration::from_millis(200),
            )
            .await
            .map_err(|s| box_err!("{}", s))
        }
    }

    // Ref: https://github.com/tidbcloud/tidb-cse/blob/release-7.1-keyspace/server/health_handler.go
    // {"status":"up","token":"keyspace_a"}
    #[derive(Default, Deserialize, Debug)]
    #[serde(default)]
    struct TidbHealth {
        status: String, // "up" or "down"
        token: String,  // keyspace name
    }

    #[derive(Default, Serialize)]
    #[serde(rename_all = "kebab-case")]
    struct LocalTidbConfig {
        split_table: bool, // Set to false.
        security: TidbConfigSecurity,
    }

    #[derive(Default, Serialize)]
    #[serde(rename_all = "kebab-case")]
    struct TidbConfigSecurity {
        enable_sem: bool, // Disable "sem" to update `mysql.tidb`.
    }
}
