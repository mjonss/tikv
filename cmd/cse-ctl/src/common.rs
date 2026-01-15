// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{path::Path, sync::Arc};

use kvengine::dfs::{DFSConfig, Dfs, new_dfs_from_config};
use kvproto::metapb::Store;
use pd_client::util::get_all_stores_except_tiflash;
use security::{GetSecurityManager, SecurityConfig, SecurityManager};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CommonConfig {
    pub pd: pd_client::Config,
    pub security: SecurityConfig,
    pub dfs: DFSConfig,
}

impl CommonConfig {
    pub fn from_args(path: &Path, pd: &str) -> Self {
        let mut config = Self::default();
        if path.exists() {
            let data = std::fs::read(path).expect("failed to read config file");
            config = toml::from_slice(&data).unwrap();
        } else if !path.as_os_str().is_empty() {
            panic!("config file not found");
        }
        if !pd.is_empty() {
            config.pd.endpoints = pd.split(',').map(|x| x.to_owned()).collect();
        }
        // override from ENV
        config.dfs.override_from_env();
        config.security.override_from_env();
        config
    }

    pub fn create_context(&self) -> CtlContext {
        let dfs = new_dfs_from_config(self.dfs.clone());
        let pd_client = Arc::new(native_br::common::create_pd_client(
            &self.security,
            &self.pd,
        ));
        let security_mgr = pd_client.get_security_mgr();
        let http_client = security_mgr.http_client(hyper::Client::builder()).unwrap();
        CtlContext {
            dfs,
            security_mgr,
            pd_client,
            http_client,
        }
    }
}

#[derive(Clone)]
pub struct CtlContext {
    pub dfs: Arc<dyn Dfs>,
    pub security_mgr: Arc<SecurityManager>,
    pub pd_client: Arc<pd_client::RpcClient>,
    pub http_client: security::HttpClient,
}

impl CtlContext {
    pub fn get_tikv_stores(&self, exclude_store_arg: &str) -> Vec<Store> {
        let excludes: Vec<u64> = exclude_store_arg
            .split(',')
            .filter_map(|x| x.parse().ok())
            .collect();
        let mut stores = get_all_stores_except_tiflash(self.pd_client.as_ref()).unwrap();
        stores.retain(|store| !excludes.contains(&store.get_id()));
        stores
    }
}
