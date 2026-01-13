// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use collections::HashSet;
use kvproto::{kvrpcpb::ApiVersion, raft_cmdpb::*};
use raftstore::{Result, store::*};
use test_pd_client::TestPdClient;
use tikv_util::time::ThreadReadId;

use super::*;

pub struct ChannelTransportCore {}

#[derive(Clone)]
pub struct ChannelTransport {}

impl ChannelTransport {
    pub fn new() -> ChannelTransport {
        ChannelTransport {}
    }
}

impl Default for ChannelTransport {
    fn default() -> Self {
        Self::new()
    }
}

pub struct NodeCluster {}

impl NodeCluster {
    pub fn new(_: Arc<TestPdClient>) -> NodeCluster {
        NodeCluster {}
    }
}

impl Simulator for NodeCluster {
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

pub fn new_node_cluster(id: u64, count: usize) -> Cluster<NodeCluster> {
    let pd_client = Arc::new(TestPdClient::new(id, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(Arc::clone(&pd_client))));
    Cluster::new(id, count, sim, pd_client, ApiVersion::V1)
}

pub fn new_incompatible_node_cluster(id: u64, count: usize) -> Cluster<NodeCluster> {
    let pd_client = Arc::new(TestPdClient::new(id, true));
    let sim = Arc::new(RwLock::new(NodeCluster::new(Arc::clone(&pd_client))));
    Cluster::new(id, count, sim, pd_client, ApiVersion::V1)
}
