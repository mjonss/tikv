// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use grpcio::Service;
use grpcio_health::HealthService;
use kvproto::raft_cmdpb::*;
use raftstore::{
    Result,
    coprocessor::{CoprocessorHost, RegionInfoAccessor},
    store::{Callback, msg::RaftCmdExtraOpts},
};
use security::SecurityManager;
use tikv::{
    import::SstImporter,
    server::{RaftKv, Result as ServerResult, resolve::StoreAddrResolver},
};
use tikv_util::time::ThreadReadId;
use txn_types::TxnExtraScheduler;

use super::*;

pub type SimulateEngine = RaftKv<MockRaftStoreRouter>;

#[derive(Default, Clone)]
pub struct AddressMap {
    addrs: Arc<Mutex<HashMap<u64, String>>>,
}

impl AddressMap {
    pub fn get(&self, store_id: u64) -> Option<String> {
        let addrs = self.addrs.lock().unwrap();
        addrs.get(&store_id).cloned()
    }

    pub fn insert(&mut self, store_id: u64, addr: String) {
        self.addrs.lock().unwrap().insert(store_id, addr);
    }
}

impl StoreAddrResolver for AddressMap {
    fn resolve(
        &self,
        store_id: u64,
        cb: Box<dyn FnOnce(ServerResult<String>) + Send>,
    ) -> ServerResult<()> {
        let addr = self.get(store_id);
        match addr {
            Some(addr) => cb(Ok(addr)),
            None => cb(Err(box_err!(
                "unable to find address for store {}",
                store_id
            ))),
        }
        Ok(())
    }
}

type PendingServices = Vec<Box<dyn Fn() -> Service>>;
type CopHooks = Vec<Box<dyn Fn(&mut CoprocessorHost)>>;

pub struct ServerCluster {
    pub storages: HashMap<u64, SimulateEngine>,
    pub region_info_accessors: HashMap<u64, RegionInfoAccessor>,
    pub importers: HashMap<u64, Arc<SstImporter>>,
    pub pending_services: HashMap<u64, PendingServices>,
    pub coprocessor_hooks: HashMap<u64, CopHooks>,
    pub health_services: HashMap<u64, HealthService>,
    pub security_mgr: Arc<SecurityManager>,
    pub txn_extra_schedulers: HashMap<u64, Arc<dyn TxnExtraScheduler>>,
}

impl ServerCluster {
    pub fn get_addr(&self, _node_id: u64) -> String {
        unimplemented!()
    }

    pub fn get_server_router(&self, _node_id: u64) -> MockRaftStoreRouter {
        unimplemented!()
    }

    pub fn get_concurrency_manager(&self, _node_id: u64) -> ConcurrencyManager {
        unimplemented!()
    }
}

impl Simulator for ServerCluster {
    fn stop_node(&mut self, _node_id: u64) {
        unimplemented!()
    }

    fn get_node_ids(&self) -> HashSet<u64> {
        unimplemented!()
    }

    fn async_command_on_node_with_opts(
        &self,
        _node_id: u64,
        _request: RaftCmdRequest,
        _cb: Callback,
        _opts: RaftCmdExtraOpts,
    ) -> Result<()> {
        unimplemented!()
    }

    fn async_read(
        &mut self,
        _node_id: u64,
        _batch_id: Option<ThreadReadId>,
        _request: RaftCmdRequest,
        _cb: Callback,
    ) {
        unimplemented!()
    }
}

pub fn new_server_cluster(_id: u64, _count: usize) -> Cluster<ServerCluster> {
    unimplemented!()
}
