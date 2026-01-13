// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures::executor::block_on;
use grpcio::EnvBuilder;
use kvproto::{metapb, pdpb};
use pd_client::{Config, PdClient, pd_control::PdControl};
use security::{GetSecurityManager, SecurityConfig, SecurityManager};
use tikv_util::config::ReadableDuration;
use url::Url;

use crate::{PdClientExt, TestPdClient};

const SCAN_REGION_BATCH_SIZE: usize = 256;

impl PdClientExt for pd_client::RpcClient {
    fn get_regions_number(&self) -> usize {
        let pd_ctl = get_pd_control(self).unwrap();
        block_on(pd_ctl.get_regions_number()).unwrap() as usize
    }

    fn get_all_regions(&self) -> Vec<metapb::Region> {
        let mut start = vec![];
        let mut regions = vec![];
        loop {
            let pd_regions =
                block_on(self.scan_regions(start.clone(), vec![], SCAN_REGION_BATCH_SIZE)).unwrap();
            let len = pd_regions.len();
            regions.extend(pd_regions.into_iter().map(|mut r| r.take_region()));
            if len < SCAN_REGION_BATCH_SIZE {
                break;
            }
            let end_key = regions.last().unwrap().get_end_key();
            if end_key.is_empty() {
                break;
            }
            start = end_key.to_vec();
            start.push(0);
        }
        regions
    }

    fn transfer_leader(&self, _region_id: u64, _peer: metapb::Peer, _peers: Vec<metapb::Peer>) {
        unimplemented!()
    }
    fn region_leader_must_be(&self, _region_id: u64, _peer: metapb::Peer) {
        unimplemented!()
    }
    fn must_remove_peer(&self, _region_id: u64, _peer: metapb::Peer) {
        unimplemented!()
    }
    fn must_split_region_opt(
        &self,
        _region: metapb::Region,
        _policy: pdpb::CheckPolicy,
        _keys: Vec<Vec<u8>>,
        _timeout: Duration,
    ) -> pd_client::Result<()> {
        unimplemented!()
    }

    fn split_region(
        &self,
        _region: metapb::Region,
        _policy: pdpb::CheckPolicy,
        _keys: Vec<Vec<u8>>,
    ) {
        unimplemented!()
    }

    fn merge_region(&self, _from: u64, _target: u64) {
        unimplemented!()
    }

    fn try_merge_region(&self, _from: u64, _target: u64) {
        unimplemented!()
    }

    fn must_add_peer(&self, _region_id: u64, _peer: metapb::Peer) {
        unimplemented!()
    }
}

/// Wrap for `TestPdClient` as well as the test PD server to provide RPC
/// service.
pub struct TestPd {
    client: Arc<TestPdClient>,
    _server: Option<test_pd::Server<crate::Service>>,
    endpoints: Option<Vec<String>>,
}

impl TestPd {
    pub fn client(&self) -> Arc<TestPdClient> {
        self.client.clone()
    }
}

pub struct RealPd {
    client: Arc<pd_client::RpcClient>,
    endpoints: Vec<String>,
    security_mgr: Arc<SecurityManager>,
    client_update_interval: ReadableDuration,
}

impl RealPd {
    fn new_client_impl(
        endpoints: Vec<String>,
        security_mgr: Arc<SecurityManager>,
        client_update_interval: ReadableDuration,
    ) -> pd_client::RpcClient {
        let env = Arc::new(EnvBuilder::new().cq_count(1).build());
        let mut cfg = pd_client::Config::new(endpoints);
        cfg.update_interval = client_update_interval;
        cfg.validate().unwrap();
        pd_client::RpcClient::new(&cfg, Some(env), security_mgr)
            .unwrap_or_else(|e| panic!("failed to create rpc client: {:?}", e))
    }

    pub fn new_client(&self) -> pd_client::RpcClient {
        Self::new_client_impl(
            self.endpoints.clone(),
            self.security_mgr.clone(),
            self.client_update_interval,
        )
    }

    pub fn get_pd_control(&self) -> pd_client::pd_control::Result<PdControl> {
        get_pd_control(&self.client)
    }
}

static CLUSTER_ID_ALLOCATOR: AtomicU64 = AtomicU64::new(1);

/// A wrapper to provide an uniform interface for both real and test (mock) PD.
pub enum PdWrapper {
    Test(TestPd),
    Real(RealPd),
}

impl PdWrapper {
    /// No PD server when `pd_server_count` is zero.
    pub fn new_test(
        pd_server_count: usize,
        _security_conf: &SecurityConfig,
        cluster_id: Option<u64>,
    ) -> Self {
        let cluster_id =
            cluster_id.unwrap_or_else(|| CLUSTER_ID_ALLOCATOR.fetch_add(1, Ordering::Relaxed));
        let client = Arc::new(TestPdClient::new(cluster_id, false));
        let server = (pd_server_count > 0).then(|| {
            let pd_service = crate::Service::new(client.clone());
            test_pd::Server::with_case(pd_server_count, Arc::new(pd_service))
        });
        let endpoints = server.as_ref().map(|s| {
            s.bind_addrs()
                .into_iter()
                .map(|(host, port)| format!("{}:{}", host, port))
                .collect::<Vec<_>>()
        });
        Self::Test(TestPd {
            client,
            _server: server,
            endpoints,
        })
    }

    pub fn new_real(
        endpoints: Vec<String>,
        security_conf: &SecurityConfig,
        client_update_interval: ReadableDuration,
    ) -> Self {
        let security_mgr = Arc::new(SecurityManager::new(security_conf).unwrap());
        let client = RealPd::new_client_impl(
            endpoints.clone(),
            security_mgr.clone(),
            client_update_interval,
        );
        Self::Real(RealPd {
            client: Arc::new(client),
            endpoints,
            security_mgr,
            client_update_interval,
        })
    }

    pub fn new_client(&self) -> Arc<dyn PdClient> {
        match self {
            PdWrapper::Test(c) => c.client.clone(),
            PdWrapper::Real(c) => Arc::new(c.new_client()) as Arc<dyn PdClient>,
        }
    }

    /// Get a shared PD client.
    ///
    /// Client side should be safe to use a shared client. But do NOT use it in
    /// tikv-server side.
    pub fn client(&self) -> Arc<dyn PdClientExt> {
        match self {
            PdWrapper::Test(c) => c.client.clone(),
            PdWrapper::Real(c) => c.client.clone(),
        }
    }

    pub fn test_client(&self) -> Option<Arc<TestPdClient>> {
        match self {
            Self::Test(pd) => Some(pd.client.clone()),
            Self::Real(_) => None,
        }
    }

    pub fn endpoints(&self) -> Option<&[String]> {
        match self {
            PdWrapper::Test(c) => c.endpoints.as_deref(),
            PdWrapper::Real(c) => Some(&c.endpoints),
        }
    }

    pub fn get_pd_control(&self) -> pd_client::pd_control::Result<PdControl> {
        match self {
            PdWrapper::Test(_) => unimplemented!("test PD does not support PD control interfaces"),
            PdWrapper::Real(c) => c.get_pd_control(),
        }
    }
}

fn get_pd_control(rpc: &pd_client::RpcClient) -> pd_client::pd_control::Result<PdControl> {
    let endpoints: Vec<String> = rpc
        .get_leader()
        .get_client_urls()
        .iter()
        .map(|url| {
            let url = Url::parse(url).unwrap();
            format!("{}:{}", url.host_str().unwrap(), url.port().unwrap_or(2739))
        })
        .collect();

    PdControl::new(Config::new(endpoints), rpc.get_security_mgr())
}
