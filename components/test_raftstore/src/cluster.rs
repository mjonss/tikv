// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    result,
    sync::{Arc, RwLock, mpsc},
    thread,
    time::Duration,
};

use collections::{HashMap, HashSet};
use engine_rocks::RocksEngine;
use engine_traits::CF_DEFAULT;
use file_system::IoRateLimiter;
use futures::executor::block_on;
use kvproto::{
    errorpb::Error as PbError,
    kvrpcpb::ApiVersion,
    metapb::{self, PeerRole, RegionEpoch},
    pdpb,
    raft_cmdpb::*,
};
use pd_client::PdClient;
use raftstore::{Error, Result, store::*};
use tempfile::TempDir;
use test_pd_client::{PdClientExt, TestPdClient};
use tikv::server::Result as ServerResult;
use tikv_util::{
    HandyRwLock,
    time::{Instant, ThreadReadId},
    worker::LazyWorker,
};

use super::*;
use crate::Config;

// We simulate 3 or 5 nodes, each has a store.
// Sometimes, we use fixed id to test, which means the id
// isn't allocated by pd, and node id, store id are same.
// E,g, for node 1, the node id and store id are both 1.

pub trait Simulator {
    fn stop_node(&mut self, node_id: u64);
    fn get_node_ids(&self) -> HashSet<u64>;
    fn async_command_on_node(
        &self,
        node_id: u64,
        request: RaftCmdRequest,
        cb: Callback,
    ) -> Result<()> {
        self.async_command_on_node_with_opts(node_id, request, cb, Default::default())
    }
    fn async_command_on_node_with_opts(
        &self,
        node_id: u64,
        request: RaftCmdRequest,
        cb: Callback,
        opts: RaftCmdExtraOpts,
    ) -> Result<()>;

    fn call_command(&self, request: RaftCmdRequest, _timeout: Duration) -> Result<RaftCmdResponse> {
        let node_id = request.get_header().get_peer().get_store_id();
        self.call_command_on_node(node_id, request, _timeout)
    }

    fn read(
        &mut self,
        batch_id: Option<ThreadReadId>,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        let node_id = request.get_header().get_peer().get_store_id();
        let (cb, rx) = make_cb(&request);
        self.async_read(node_id, batch_id, request, cb);
        rx.recv_timeout(timeout)
            .map_err(|_| Error::Timeout(format!("request timeout for {:?}", timeout)))
    }

    fn async_read(
        &mut self,
        node_id: u64,
        batch_id: Option<ThreadReadId>,
        request: RaftCmdRequest,
        cb: Callback,
    );

    fn call_command_on_node(
        &self,
        node_id: u64,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        let (cb, rx) = make_cb(&request);

        match self.async_command_on_node(node_id, request, cb) {
            Ok(()) => {}
            Err(e) => {
                let mut resp = RaftCmdResponse::default();
                resp.mut_header().set_error(e.into());
                return Ok(resp);
            }
        }
        rx.recv_timeout(timeout)
            .map_err(|e| Error::Timeout(format!("request timeout for {:?}: {:?}", timeout, e)))
    }
}

pub struct Cluster<T: Simulator> {
    pub cfg: Config,
    leaders: HashMap<u64, metapb::Peer>,
    pub count: usize,

    pub paths: Vec<TempDir>,
    pub dbs: Vec<(RocksEngine, RocksEngine)>,
    pub io_rate_limiter: Option<Arc<IoRateLimiter>>,
    pub engines: HashMap<u64, (RocksEngine, RocksEngine)>,
    pub labels: HashMap<u64, HashMap<String, String>>,
    pub sst_workers: Vec<LazyWorker<String>>,
    pub sst_workers_map: HashMap<u64, usize>,
    pub sim: Arc<RwLock<T>>,
    pub pd_client: Arc<TestPdClient>,
}

impl<T: Simulator> Cluster<T> {
    // Create the default Store cluster.
    pub fn new(
        id: u64,
        count: usize,
        sim: Arc<RwLock<T>>,
        pd_client: Arc<TestPdClient>,
        api_version: ApiVersion,
    ) -> Cluster<T> {
        // TODO: In the future, maybe it's better to test both case where
        // `use_delete_range` is true and false
        Cluster {
            cfg: Config {
                tikv: new_tikv_config_with_api_ver(id, api_version),
                prefer_mem: true,
            },
            leaders: HashMap::default(),
            count,
            paths: vec![],
            dbs: vec![],
            io_rate_limiter: None,
            engines: HashMap::default(),
            labels: HashMap::default(),
            sim,
            pd_client,
            sst_workers: vec![],
            sst_workers_map: HashMap::default(),
        }
    }

    // To destroy temp dir later.
    pub fn take_path(&mut self) -> Vec<TempDir> {
        std::mem::take(&mut self.paths)
    }

    pub fn id(&self) -> u64 {
        self.cfg.server.cluster_id
    }

    // Bootstrap the store with fixed ID (like 1, 2, .. 5) and
    // initialize first region in all stores, then start the cluster.
    pub fn run(&mut self) {
        unimplemented!()
    }

    // Bootstrap the store with fixed ID (like 1, 2, .. 5) and
    // initialize first region in store 1, then start the cluster.
    pub fn run_conf_change(&mut self) -> u64 {
        unimplemented!()
    }

    pub fn get_node_ids(&self) -> HashSet<u64> {
        self.sim.rl().get_node_ids()
    }

    pub fn run_node(&mut self, _node_id: u64) -> ServerResult<()> {
        unimplemented!()
    }

    pub fn stop_node(&mut self, _node_id: u64) {
        unimplemented!()
    }

    pub fn get_engine(&self, node_id: u64) -> RocksEngine {
        self.engines[&node_id].0.clone()
    }

    pub fn get_raft_engine(&self, node_id: u64) -> RocksEngine {
        self.engines[&node_id].1.clone()
    }

    pub fn get_all_engines(&self, node_id: u64) -> (RocksEngine, RocksEngine) {
        self.engines[&node_id].clone()
    }

    pub fn read(
        &self,
        batch_id: Option<ThreadReadId>,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        match self.sim.wl().read(batch_id, request.clone(), timeout) {
            Err(e) => {
                warn!("failed to read {:?}: {:?}", request, e);
                Err(e)
            }
            a => a,
        }
    }

    pub fn call_command(
        &self,
        _request: RaftCmdRequest,
        _timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        unimplemented!()
    }

    pub fn call_command_on_leader(
        &mut self,
        mut request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        let timer = Instant::now();
        let region_id = request.get_header().get_region_id();
        loop {
            let leader = match self.leader_of_region(region_id) {
                None => return Err(Error::NotLeader(region_id, None)),
                Some(l) => l,
            };
            request.mut_header().set_peer(leader);
            let resp = match self.call_command(request.clone(), timeout) {
                e @ Err(_) => return e,
                Ok(resp) => resp,
            };
            if self.refresh_leader_if_needed(&resp, region_id)
                && timer.saturating_elapsed() < timeout
            {
                warn!(
                    "{:?} is no longer leader, let's retry",
                    request.get_header().get_peer()
                );
                continue;
            }
            return Ok(resp);
        }
    }

    fn valid_leader_id(&self, region_id: u64, leader_id: u64) -> bool {
        let store_ids = match self.voter_store_ids_of_region(region_id) {
            None => return false,
            Some(ids) => ids,
        };
        let node_ids = self.sim.rl().get_node_ids();
        store_ids.contains(&leader_id) && node_ids.contains(&leader_id)
    }

    fn voter_store_ids_of_region(&self, region_id: u64) -> Option<Vec<u64>> {
        block_on(self.pd_client.get_region_by_id(region_id))
            .unwrap()
            .map(|region| {
                region
                    .get_peers()
                    .iter()
                    .flat_map(|p| {
                        if p.get_role() != PeerRole::Learner {
                            Some(p.get_store_id())
                        } else {
                            None
                        }
                    })
                    .collect()
            })
    }

    pub fn query_leader(
        &self,
        store_id: u64,
        region_id: u64,
        timeout: Duration,
    ) -> Option<metapb::Peer> {
        // To get region leader, we don't care real peer id, so use 0 instead.
        let peer = new_peer(store_id, 0);
        let find_leader = new_status_request(region_id, peer, new_region_leader_cmd());
        let mut resp = match self.call_command(find_leader, timeout) {
            Ok(resp) => resp,
            Err(err) => {
                error!(
                    "fail to get leader of region {} on store {}, error: {:?}",
                    region_id, store_id, err
                );
                return None;
            }
        };
        let mut region_leader = resp.take_status_response().take_region_leader();
        // NOTE: node id can't be 0.
        if self.valid_leader_id(region_id, region_leader.get_leader().get_store_id()) {
            Some(region_leader.take_leader())
        } else {
            None
        }
    }

    pub fn leader_of_region(&mut self, region_id: u64) -> Option<metapb::Peer> {
        let timer = Instant::now_coarse();
        let timeout = Duration::from_secs(5);
        let mut store_ids = None;
        while timer.saturating_elapsed() < timeout {
            match self.voter_store_ids_of_region(region_id) {
                None => thread::sleep(Duration::from_millis(10)),
                Some(ids) => {
                    store_ids = Some(ids);
                    break;
                }
            };
        }
        let store_ids = store_ids?;
        if let Some(l) = self.leaders.get(&region_id) {
            // leader may be stopped in some tests.
            if self.valid_leader_id(region_id, l.get_store_id()) {
                return Some(l.clone());
            }
        }
        self.reset_leader_of_region(region_id);
        let mut leader = None;
        let mut leaders = HashMap::default();

        let node_ids = self.sim.rl().get_node_ids();
        // For some tests, we stop the node but pd still has this information,
        // and we must skip this.
        let alive_store_ids: Vec<_> = store_ids
            .iter()
            .filter(|id| node_ids.contains(id))
            .cloned()
            .collect();
        while timer.saturating_elapsed() < timeout {
            for store_id in &alive_store_ids {
                let l = match self.query_leader(*store_id, region_id, Duration::from_secs(1)) {
                    None => continue,
                    Some(l) => l,
                };
                leaders
                    .entry(l.get_id())
                    .or_insert((l, vec![]))
                    .1
                    .push(*store_id);
            }
            if let Some((_, (l, c))) = leaders.iter().max_by_key(|(_, (_, c))| c.len()) {
                // It may be a step down leader.
                if c.contains(&l.get_store_id()) {
                    leader = Some(l.clone());
                    // Technically, correct calculation should use two quorum when in joint
                    // state. Here just for simplicity.
                    if c.len() > store_ids.len() / 2 {
                        break;
                    }
                }
            }
            debug!("failed to detect leaders"; "leaders" => ?leaders, "store_ids" => ?store_ids);
            sleep_ms(10);
            leaders.clear();
        }

        if let Some(l) = leader {
            self.leaders.insert(region_id, l);
        }

        self.leaders.get(&region_id).cloned()
    }

    pub fn check_regions_number(&self, len: u32) {
        assert_eq!(self.pd_client.get_regions_number() as u32, len)
    }

    // For test when a node is already bootstrapped the cluster with the first
    // region But another node may request bootstrap at same time and get
    // is_bootstrap false Add Region but not set bootstrap to true
    pub fn add_first_region(&self) -> Result<()> {
        let mut region = metapb::Region::default();
        let region_id = self.pd_client.alloc_id().unwrap();
        let peer_id = self.pd_client.alloc_id().unwrap();
        region.set_id(region_id);
        region.set_start_key(keys::EMPTY_KEY.to_vec());
        region.set_end_key(keys::EMPTY_KEY.to_vec());
        region.mut_region_epoch().set_version(INIT_EPOCH_VER);
        region.mut_region_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);
        let peer = new_peer(peer_id, peer_id);
        region.mut_peers().push(peer);
        self.pd_client.add_region(&region);
        Ok(())
    }

    pub fn add_label(&mut self, node_id: u64, key: &str, value: &str) {
        self.labels
            .entry(node_id)
            .or_default()
            .insert(key.to_owned(), value.to_owned());
    }

    pub fn reset_leader_of_region(&mut self, region_id: u64) {
        self.leaders.remove(&region_id);
    }

    pub fn shutdown(&mut self) {
        unimplemented!()
    }

    // If the resp is "not leader error", get the real leader.
    // Otherwise reset or refresh leader if needed.
    // Returns if the request should retry.
    fn refresh_leader_if_needed(&mut self, resp: &RaftCmdResponse, region_id: u64) -> bool {
        if !is_error_response(resp) {
            return false;
        }

        let err = resp.get_header().get_error();
        if err
            .get_message()
            .contains("peer has not applied to current term")
        {
            // leader peer has not applied to current term
            return true;
        }

        // If command is stale, leadership may have changed.
        // EpochNotMatch is not checked as leadership is checked first in raftstore.
        if err.has_stale_command() {
            self.reset_leader_of_region(region_id);
            return true;
        }

        if !err.has_not_leader() {
            return false;
        }
        let err = err.get_not_leader();
        if !err.has_leader() {
            self.reset_leader_of_region(region_id);
            return true;
        }
        self.leaders.insert(region_id, err.get_leader().clone());
        true
    }

    pub fn request(
        &mut self,
        _key: &[u8],
        _reqs: Vec<Request>,
        _read_quorum: bool,
        _timeout: Duration,
    ) -> RaftCmdResponse {
        unimplemented!()
    }

    // Get region when the `filter` returns true.
    pub fn get_region_with<F>(&self, key: &[u8], filter: F) -> metapb::Region
    where
        F: Fn(&metapb::Region) -> bool,
    {
        for _ in 0..100 {
            if let Ok(region) = self.pd_client.get_region(key) {
                if filter(&region) {
                    return region;
                }
            }
            // We may meet range gap after split, so here we will
            // retry to get the region again.
            sleep_ms(20);
        }

        panic!("find no region for {}", log_wrappers::hex_encode_upper(key));
    }

    pub fn get_region(&self, key: &[u8]) -> metapb::Region {
        self.get_region_with(key, |_| true)
    }

    pub fn get_region_id(&self, key: &[u8]) -> u64 {
        self.get_region(key).get_id()
    }

    pub fn get_down_peers(&self) -> HashMap<u64, pdpb::PeerStats> {
        self.pd_client.get_down_peers()
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.get_impl(CF_DEFAULT, key, false)
    }

    pub fn get_cf(&mut self, cf: &str, key: &[u8]) -> Option<Vec<u8>> {
        self.get_impl(cf, key, false)
    }

    pub fn must_get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.get_impl(CF_DEFAULT, key, true)
    }

    fn get_impl(&mut self, cf: &str, key: &[u8], read_quorum: bool) -> Option<Vec<u8>> {
        let mut resp = self.request(
            key,
            vec![new_get_cf_cmd(cf, key)],
            read_quorum,
            Duration::from_secs(5),
        );
        if resp.get_header().has_error() {
            panic!("response {:?} has error", resp);
        }
        assert_eq!(resp.get_responses().len(), 1);
        assert_eq!(resp.get_responses()[0].get_cmd_type(), CmdType::Get);
        if resp.get_responses()[0].has_get() {
            Some(resp.mut_responses()[0].mut_get().take_value())
        } else {
            None
        }
    }

    pub fn async_request(
        &mut self,
        req: RaftCmdRequest,
    ) -> Result<mpsc::Receiver<RaftCmdResponse>> {
        self.async_request_with_opts(req, Default::default())
    }

    pub fn async_request_with_opts(
        &mut self,
        mut req: RaftCmdRequest,
        opts: RaftCmdExtraOpts,
    ) -> Result<mpsc::Receiver<RaftCmdResponse>> {
        let region_id = req.get_header().get_region_id();
        let leader = self.leader_of_region(region_id).unwrap();
        req.mut_header().set_peer(leader.clone());
        let (cb, rx) = make_cb(&req);
        self.sim
            .rl()
            .async_command_on_node_with_opts(leader.get_store_id(), req, cb, opts)?;
        Ok(rx)
    }

    pub fn async_put(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<mpsc::Receiver<RaftCmdResponse>> {
        let mut region = self.get_region(key);
        let reqs = vec![new_put_cmd(key, value)];
        let put = new_request(region.get_id(), region.take_region_epoch(), reqs, false);
        self.async_request(put)
    }

    pub fn must_put(&mut self, key: &[u8], value: &[u8]) {
        self.must_put_cf(CF_DEFAULT, key, value);
    }

    pub fn must_put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) {
        if let Err(e) = self.batch_put(key, vec![new_put_cf_cmd(cf, key, value)]) {
            panic!("has error: {:?}", e);
        }
    }

    pub fn batch_put(
        &mut self,
        region_key: &[u8],
        reqs: Vec<Request>,
    ) -> result::Result<RaftCmdResponse, PbError> {
        let resp = self.request(region_key, reqs, false, Duration::from_secs(5));
        if resp.get_header().has_error() {
            Err(resp.get_header().get_error().clone())
        } else {
            Ok(resp)
        }
    }

    pub fn must_delete(&mut self, key: &[u8]) {
        self.must_delete_cf(CF_DEFAULT, key)
    }

    pub fn must_delete_cf(&mut self, cf: &str, key: &[u8]) {
        let resp = self.request(
            key,
            vec![new_delete_cmd(cf, key)],
            false,
            Duration::from_secs(5),
        );
        if resp.get_header().has_error() {
            panic!("response {:?} has error", resp);
        }
    }

    pub fn get_region_epoch(&self, region_id: u64) -> RegionEpoch {
        block_on(self.pd_client.get_region_by_id(region_id))
            .unwrap()
            .unwrap()
            .take_region_epoch()
    }

    pub fn must_split(&mut self, _region: &metapb::Region, _split_key: &[u8]) {
        unimplemented!()
    }

    pub fn must_try_merge(&mut self, _source: u64, _target: u64) {
        unimplemented!()
    }
}

impl<T: Simulator> Drop for Cluster<T> {
    fn drop(&mut self) {
        test_util::clear_failpoints();
        self.shutdown();
    }
}
