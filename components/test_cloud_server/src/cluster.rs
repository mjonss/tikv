// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    fs, ops,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread::sleep,
    time::Duration,
};

use anyhow::bail;
use bstr::ByteSlice;
use bytes::Bytes;
use cloud_server::{TikvServer, server::GRPC_THREAD_PREFIX};
use cloud_worker::{CloudWorker, local_gc::LocalGcConfig, native_br::NativeBrConfig};
use dashmap::DashMap;
use engine_traits::ObjectCache;
use futures::{executor::block_on, future::try_join_all};
use grpcio::{Channel, ChannelBuilder, EnvBuilder, Environment};
use hyper::{Body, Request, http};
use kvengine::{
    ShardStats, dfs,
    dfs::{DFSConnOptions, Dfs, FileType},
    ia::{gc::IaGcConfig, util::IaConfig},
    table::{
        file::InMemFile,
        schema_file::{SchemaFile, build_schema_file},
        sstable::BlockCacheType,
    },
    txn_chunk_manager::TxnChunkManagerConfig,
};
use kvproto::{
    kvrpcpb::{Mutation, Op},
    metapb,
    metapb::{Peer, PeerRole, Store},
    raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, RaftRequestHeader},
};
use log_wrappers::Value;
use native_br::limiter::{RateLimitConfig, ThroughputLimiter};
use pd_client::{PdClient, check_regions_boundary, pd_control};
use raftstore::RegionInfoAccessor;
use rand::prelude::*;
use rfstore::{
    RaftStoreRouter,
    store::{Callback, CustomBuilder, cmd_resp::message_error},
};
use security::{SecurityConfig, SecurityManager};
use tempfile::TempDir;
use test_pd_client::{PdClientExt, PdWrapper, TestPdClient};
use tikv::{config::TikvConfig, import::SstImporter};
use tikv_util::{
    box_err,
    codec::bytes::encode_bytes,
    config::{AbsoluteOrPercentSize, ReadableDuration, ReadableSize},
    error, info,
    store::find_peer,
    sys::SysQuota,
    thd_name,
    thread_group::GroupProperties,
    time::Instant,
    warn,
};

use crate::{
    alloc_node_id_vec,
    client::{ApiV2NoPrefixCodec, ClusterClient, ClusterClientOptions, ClusterTxnClient, RefStore},
    keyspace::{ClusterKeyspaceClient, CreateKeyspaceOptions, KeyspaceManager},
    oss::{ObjectStorageService, prepare_dfs},
    scheduler::Scheduler,
    tikv_bin::wait_tikv_worker_healthy,
    txn::{lock_resolver::LockResolver, txn_file::TxnFileHelper},
    util::{
        broadcast_schema_file_request_and_check, get_keyspace_split_keys, get_table_split_keys,
    },
};

const REGION_MEM_LIMIT_RATIO: f64 = 0.2;
const TIKV_WORKER_UPDATE_INTERVAL: ReadableDuration = ReadableDuration::secs(10);

const TXN_CHUNK_MGR_GC_INTERVAL: ReadableDuration = ReadableDuration::secs(10);
const TXN_CHUNK_MGR_GC_TTL: ReadableDuration = ReadableDuration::secs(10);
const TXN_CHUNK_TARGET_BLOCK_SIZE: usize = 8092;

const BLOCK_SIZE_DEF: u64 = 4096;

const IA_SEGMENT_SIZE_DEF: i64 = BLOCK_SIZE_DEF as i64 * 8; // 32 KiB
pub const IA_FREQ_UPDATE_INTERVAL_DEF: Duration = Duration::from_secs(3);
pub const IA_MEM_CAP_DEF: u64 = 1 << 20; // 1 MiB
pub const IA_DISK_CAP_DEF: u64 = 10 << 20; // 10 MiB

pub const TIKV_WORKER_MEMORY_UPPER_THRESHOLD_DEF: AbsoluteOrPercentSize =
    AbsoluteOrPercentSize::Percent(20.0);

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub struct ServerCluster {
    servers: HashMap<u16 /* node_id */, TikvServer>,
    tmp_dir: TempDir,
    env: Arc<Environment>,
    pd_client: Arc<dyn PdClientExt>,
    pd: PdWrapper,
    security_mgr: Arc<SecurityManager>,
    dfs: Option<Arc<dyn Dfs>>,
    channels: HashMap<u64, Channel>,
    ref_store: Arc<Mutex<RefStore>>,
    schedule_lock: Arc<DashMap<u64, Arc<Mutex<()>>>>,
    confs: HashMap<u16 /* node_id */, TikvConfig>,
    keyspace_manager: KeyspaceManager,
    nodes_count: usize,
    tikv_worker_configs: HashMap<u16 /* idx */, cloud_worker::Config>,
    tikv_workers: HashMap<u16 /* idx */, CloudWorker>,
    schema_manager: Option<CloudWorker>,
    stopped: bool,

    /// The ratio of memory capacity to use from the total memory.
    /// Used for reserve memory for other components (PD, TiDB, and TiFlash).
    memory_capacity_ratio: f64,
}

impl ServerCluster {
    pub fn new<F>(nodes: Vec<u16>, update_conf: F) -> ServerCluster
    where
        F: Fn(u16, &mut TikvConfig),
    {
        ServerClusterBuilder::new(nodes, update_conf).build()
    }

    // The node id is statically assigned, the temp dir and server address are
    // calculated by the node id.
    pub fn new_opt<F>(
        nodes: Vec<u16>,
        update_conf: F,
        pd_wrapper: PdWrapper,
        memory_capacity_ratio: f64,
    ) -> ServerCluster
    where
        F: Fn(u16, &mut TikvConfig),
    {
        tikv_util::thread_group::set_properties(Some(GroupProperties::default()));
        // Use prefix to generate `tmp_dir` to indicate the usage of dir more clearly.
        let tmp_dir = tempfile::Builder::new()
            .prefix("cluster_")
            .tempdir()
            .unwrap();
        let pd_client = pd_wrapper.client();
        let nodes_count = nodes.len();
        let mut cluster = Self {
            servers: HashMap::new(),
            tmp_dir,
            env: Arc::new(EnvBuilder::new().cq_count(2).build()),
            pd_client,
            pd: pd_wrapper,
            security_mgr: Arc::new(SecurityManager::new(&Default::default()).unwrap()),
            dfs: None,
            channels: HashMap::new(),
            ref_store: Arc::new(Mutex::new(RefStore::default())),
            schedule_lock: Arc::new(DashMap::new()),
            confs: Default::default(),
            keyspace_manager: Default::default(),
            nodes_count,
            tikv_worker_configs: Default::default(),
            tikv_workers: Default::default(),
            schema_manager: None,
            stopped: false,
            memory_capacity_ratio,
        };
        for node_id in nodes {
            cluster.start_node(node_id, &update_conf);
        }
        if nodes_count > 0 {
            // When there is no node, PD will not bootstrap.
            cluster.wait_pd_region_min_count(1);
        }
        cluster
    }

    pub fn data_dir(&self) -> &Path {
        self.tmp_dir.path()
    }

    pub fn get_dfs(&self) -> Option<Arc<dyn Dfs>> {
        self.dfs.clone()
    }

    fn prepare_dfs(config: &TikvConfig, pd_client: Arc<dyn PdClient>) -> Arc<dyn Dfs> {
        let dfs_conf = &config.dfs;
        match dfs_conf.backend.to_ascii_lowercase().as_str() {
            "azure" => {
                info!("prepare_dfs: use Azure");
                kvengine::dfs::new_dfs_from_config(dfs_conf.clone())
            }
            _ => {
                if dfs_conf.s3_bucket.is_empty() && dfs_conf.s3_endpoint.is_empty()
                    || dfs_conf.s3_endpoint == "local"
                {
                    info!("prepare_dfs: use builtin dfs");
                    let builtin_dfs = builtin_dfs::BuiltinDfs::new(pd_client.clone());
                    Arc::new(builtin_dfs)
                } else if dfs_conf.s3_endpoint == "memory" {
                    info!("prepare_dfs: use memory");
                    Arc::new(kvengine::dfs::InMemFs::new())
                } else {
                    info!("prepare_dfs: use S3Fs");
                    kvengine::dfs::new_dfs_from_config(dfs_conf.clone())
                }
            }
        }
    }

    /// `update_conf` is based on the existed config.
    pub fn start_node<F>(&mut self, node_id: u16, update_conf: F)
    where
        F: Fn(u16, &mut TikvConfig),
    {
        let mut config = if let Some(config) = self.confs.remove(&node_id) {
            config
        } else {
            new_test_config(
                self.tmp_dir.path(),
                node_id,
                self.nodes_count,
                self.memory_capacity_ratio,
            )
        };
        update_conf(node_id, &mut config);
        let pd_client = self.pd.new_client(); // Different nodes must not share PD client.
        config.server.cluster_id = pd_client.get_cluster_id().unwrap();
        self.confs.insert(node_id, config.clone());

        std::fs::create_dir_all(&config.storage.data_dir).unwrap();
        // Use new DFS instance for each node to be the same as production env.
        let dfs = Self::prepare_dfs(&config, pd_client.clone());
        let _ = self.dfs.get_or_insert_with(|| dfs.clone());

        let props = tikv_util::thread_group::current_properties();
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(config.server.grpc_concurrency)
                .name_prefix(thd_name!(format!("{}-{}", GRPC_THREAD_PREFIX, node_id)))
                .after_start(move || {
                    tikv_alloc::add_thread_memory_accessor();
                    tikv_util::thread_group::set_properties(props.clone());
                })
                .before_stop(move || {
                    tikv_alloc::remove_thread_memory_accessor();
                })
                .build(),
        );

        let mut server = TikvServer::setup(config, self.security_mgr.clone(), env, pd_client, dfs);
        server.run();
        let store_id = server.get_store_id();
        if let std::collections::hash_map::Entry::Vacant(e) = self.channels.entry(store_id) {
            let addr = node_addr(node_id);
            let channel = ChannelBuilder::new(self.env.clone())
                .keepalive_time(Duration::from_secs(1))
                .keepalive_timeout(Duration::from_secs(1))
                .max_reconnect_backoff(Duration::from_secs(1))
                .initial_reconnect_backoff(Duration::from_secs(1))
                .connect(&addr);
            e.insert(channel);
        }
        self.servers.insert(node_id, server);
    }

    pub fn get_stores(&self) -> Vec<u64> {
        self.channels.keys().copied().collect()
    }

    pub fn get_store_id(&self, node_id: u16) -> u64 {
        self.servers.get(&node_id).unwrap().get_store_id()
    }

    /// Get `TestPdClient`.
    ///
    /// Panic if the cluster is not created with `TestPdClient`.
    ///
    /// It would be better to name as `get_test_pd_client` but just keep the old
    /// name to avoid touch too many existed codes.
    pub fn get_pd_client(&self) -> Arc<TestPdClient> {
        self.pd.test_client().unwrap_or_else(|| {
            panic!("cluster is not created with TestPdClient");
        })
    }

    pub fn get_pure_pd_client(&self) -> Arc<dyn PdClient> {
        self.pd_client.clone() as Arc<dyn PdClient>
    }

    pub fn get_pd_client_ext(&self) -> Arc<dyn PdClientExt> {
        self.pd_client.clone()
    }

    pub fn get_pd_control(&self) -> pd_control::Result<pd_control::PdControl> {
        self.pd.get_pd_control()
    }

    pub fn get_nodes(&self) -> Vec<u16> {
        self.servers.keys().copied().collect()
    }

    pub fn get_node_config(&self, node_id: u16) -> &TikvConfig {
        self.confs.get(&node_id).unwrap()
    }

    pub fn get_node_configs(&self) -> &HashMap<u16, TikvConfig> {
        &self.confs
    }

    pub fn get_any_node_config(&self) -> Option<&TikvConfig> {
        self.confs.values().next()
    }

    pub fn get_mem_table_size(&self) -> ReadableSize {
        self.get_any_node_config()
            .unwrap()
            .rocksdb
            .writecf
            .write_buffer_size
    }

    pub fn update_node_config(&mut self, node_id: u16, conf: TikvConfig) {
        self.confs.insert(node_id, conf);
    }

    pub fn stop(&mut self) {
        if self.stopped {
            return;
        }
        self.stopped = true;

        let nodes = self.get_nodes();
        for node_id in nodes {
            self.stop_node(node_id);
        }
        for (_, worker) in self.tikv_workers.drain() {
            worker.shutdown();
        }
        if let Some(schema_manager) = self.schema_manager.take() {
            schema_manager.shutdown()
        }
    }

    // Stop node gracefully.
    pub fn stop_node(&mut self, node_id: u16) {
        self.stop_node_force(node_id, false);
    }

    // Stop node without flush rfengine dfs worker if force is true. Used to cover
    // the case wal chunk recovery.
    pub fn stop_node_force(&mut self, node_id: u16, force: bool) {
        if let Some(node) = self.servers.remove(&node_id) {
            let store_id = node.get_store_id();
            info!("node stopping"; "node" => node_id, "store" => store_id, "force" => force);

            let start_time = Instant::now_coarse();
            node.force_stop(force);
            info!("node stopped"; "node" => node_id, "takes" => ?start_time.saturating_elapsed());
        }
    }

    pub fn restart_node<F>(&mut self, node_id: u16, stop_dur: Duration, force: bool, update_conf: F)
    where
        F: Fn(u16, &mut TikvConfig),
    {
        self.stop_node_force(node_id, force);

        std::thread::sleep(stop_dur);
        self.start_node(node_id, update_conf);
        info!("node restarted"; "node" => node_id);
    }

    pub fn get_kvengine(&self, node_id: u16) -> kvengine::Engine {
        let server = self.servers.get(&node_id).unwrap();
        server.get_kv_engine()
    }

    pub fn get_rfengine(&self, node_id: u16) -> rfengine::RfEngine {
        let server = self.servers.get(&node_id).unwrap();
        server.get_raft_engine()
    }

    pub fn get_region_info_accessor(&self, node_id: u16) -> RegionInfoAccessor {
        let server = self.servers.get(&node_id).unwrap();
        server.get_region_info_accessor()
    }

    pub fn get_snap(&self, node_id: u16, key: &[u8]) -> kvengine::SnapAccess {
        let engine = self.get_kvengine(node_id);
        let region = self
            .pd_client
            .get_region_with_retry(&encode_bytes(key), Duration::from_secs(10))
            .unwrap();
        engine.get_snap_access(region.id).unwrap()
    }

    pub fn get_latest_shard(&self, shard_id: u64) -> Option<Arc<kvengine::Shard>> {
        let mut shards = self
            .servers
            .values()
            .filter_map(|server| server.get_kv_engine().get_shard(shard_id));
        let mut target = shards.next()?;
        for shard in shards {
            if Self::compare_shard(&shard, &target).is_gt() {
                target = shard;
            }
        }
        Some(target)
    }

    fn compare_shard(m: &kvengine::Shard, n: &kvengine::Shard) -> std::cmp::Ordering {
        assert_eq!(m.id, n.id);
        m.ver
            .cmp(&n.ver)
            .then_with(|| m.get_write_sequence().cmp(&n.get_write_sequence()))
            .then_with(|| m.get_meta_sequence().cmp(&n.get_meta_sequence()))
            .then_with(|| m.is_active().cmp(&n.is_active()))
    }

    pub fn get_latest_shard_by_key(&self, key: &[u8]) -> Option<Arc<kvengine::Shard>> {
        let region = self
            .pd_client
            .get_region_with_retry(&encode_bytes(key), Duration::from_secs(10))
            .unwrap();
        self.get_latest_shard(region.id)
    }

    pub fn get_latest_snap(&self, key: &[u8]) -> Option<kvengine::SnapAccess> {
        let region = self
            .pd_client
            .get_region_with_retry(&encode_bytes(key), Duration::from_secs(10))
            .unwrap();
        let snap = self.get_latest_shard(region.id)?.new_snap_access();
        Some(snap)
    }

    pub fn get_sst_importer(&self, node_id: u16) -> Arc<SstImporter> {
        let server = self.servers.get(&node_id).unwrap();
        server.get_sst_importer()
    }

    /// Return `None` when peer with specified `store_id` is not found.
    pub fn send_raft_command(&self, cmd: RaftCmdRequest) -> Option<RaftCmdResponse> {
        let store_id = cmd.get_header().get_peer().get_store_id();
        let tag = format!(
            "{}:{}:{}",
            store_id,
            cmd.get_header().get_region_id(),
            cmd.get_header().get_region_epoch().get_version()
        );

        for server in self.servers.values() {
            if server.get_store_id() == store_id {
                let (cb, fut) = tikv_util::future::paired_future_callback();
                let callback = Callback::write(Box::new(move |res| {
                    cb(res);
                }));

                server.get_raft_router().send_command(cmd, callback);

                return match block_on(fut) {
                    Ok(res) => {
                        if res.response.get_header().has_error() {
                            warn!(
                                "{} send_raft_command return error: {:?}",
                                tag,
                                res.response.get_header().get_error()
                            );
                        }
                        Some(res.response)
                    }
                    Err(e) => {
                        warn!("{} send_raft_command fail to get response: {:?}", tag, e);
                        Some(message_error("fail to get response"))
                    }
                };
            }
        }
        None
    }

    pub fn wait_region_replicated(&self, key: &[u8], replica_cnt: usize) {
        self.wait_region_replicated_ext(key, replica_cnt, false);
    }

    pub fn wait_region_replicated_ext(&self, key: &[u8], replica_cnt: usize, accept_learner: bool) {
        let encoded_key = encode_bytes(key);
        for _ in 0..30 {
            let region_info = match self.pd_client.get_region_info(&encoded_key) {
                Ok(region_info) => region_info,
                Err(err) => {
                    // The region may not exist during split. Retry.
                    warn!("get_region_info failed"; "encoded_key" => Value::key(&encoded_key), "err" => ?err);
                    std::thread::sleep(Duration::from_millis(100));
                    continue;
                }
            };
            info!("wait region replicated"; "region" => ?region_info);
            let region_id = region_info.id;
            let region_ver = region_info.get_region_epoch().version;
            let peer_count = region_info
                .get_peers()
                .iter()
                .filter(|p| {
                    p.get_role() == PeerRole::Voter
                        || (accept_learner && p.get_role() == PeerRole::Learner)
                })
                .count();
            if peer_count >= replica_cnt {
                let all_applied_snapshot = region_info.get_peers().iter().all(|peer| {
                    match self.get_server_node_id(peer.store_id) {
                        Some(node_id) => {
                            let kv = self.get_kvengine(node_id);
                            kv.get_shard_with_ver(region_id, region_ver).is_ok()
                        }
                        None => {
                            // Skip to check shard version when the store is not existed.
                            // It means that the store is stopped or start externally.
                            true
                        }
                    }
                });
                if all_applied_snapshot {
                    return;
                }
            }
            std::thread::sleep(Duration::from_millis(1000));
        }
        panic!("region is not replicated");
    }

    pub fn wait_pd_region_count(&self, count: usize) {
        self.wait_pd_region_count_opt(count, Duration::from_secs(5));
    }

    pub fn wait_pd_region_count_opt(&self, count: usize, timeout: Duration) {
        let start_time = Instant::now_coarse();
        while start_time.saturating_elapsed() < timeout {
            if self.pd_client.get_regions_number() == count {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!(
            "pd region count not match, {} != {}",
            self.pd_client.get_regions_number(),
            count
        );
    }

    pub fn wait_pd_region_min_count(&self, min_count: usize) {
        let start_time = Instant::now_coarse();
        let timeout = Duration::from_secs(5);
        let mut region_count = 0;
        while start_time.saturating_elapsed() < timeout {
            region_count = self.pd_client.get_regions_number();
            if region_count >= min_count {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!(
            "pd region count {} < min_count({})",
            region_count, min_count
        );
    }

    pub fn evict_peer(&mut self, peer_id: u64) {
        let (peer_state, old_store_id) = self
            .servers
            .values()
            .find_map(|server| {
                let ps = server.get_raft_engine().get_peer_stats(peer_id);
                (ps.peer_id != 0).then_some((ps, server.get_store_id()))
            })
            .unwrap();

        let region_id = peer_state.region_id;
        let region = block_on(self.pd_client.get_region_by_id(region_id))
            .unwrap()
            .unwrap();

        let target_store_id = self
            .get_stores()
            .iter()
            .copied()
            .find(|id| region.peers.iter().all(|p| p.get_store_id() != *id))
            .unwrap();

        let mut new_peer = Peer::new();
        new_peer.set_store_id(target_store_id);
        new_peer.set_id(self.pd_client.alloc_id().unwrap());
        self.pd_client.must_add_peer(region_id, new_peer);
        let mut old_peer = Peer::new();
        old_peer.set_store_id(old_store_id);
        old_peer.set_id(peer_state.peer_id);
        self.pd_client.must_remove_peer(region_id, old_peer);
    }

    pub fn remove_node_peers(&mut self, node_id: u16) {
        let server = self.servers.get(&node_id).unwrap();
        let store_id = server.get_store_id();
        let all_id_vers = server.get_kv_engine().get_all_shard_id_vers();
        for id_ver in &all_id_vers {
            let (region, leader) = block_on(self.pd_client.get_region_leader_by_id(id_ver.id))
                .unwrap()
                .unwrap();
            let target = if leader.store_id != store_id {
                &leader
            } else {
                region
                    .get_peers()
                    .iter()
                    .find(|x| x.store_id != store_id)
                    .unwrap()
            };
            self.pd_client
                .transfer_leader(region.id, target.clone(), vec![]);
            self.pd_client
                .region_leader_must_be(region.id, target.clone());
            if let Some(peer) = find_peer(&region, store_id) {
                self.pd_client.must_remove_peer(region.id, peer.clone());
            }
        }
        let server = self.servers.get(&node_id).unwrap();
        for _ in 0..30 {
            if server.get_kv_engine().get_all_shard_id_vers().is_empty() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("kvengine is not empty");
    }

    fn get_server_node_id(&self, store_id: u64) -> Option<u16> {
        for (node_id, server) in &self.servers {
            if server.get_store_id() == store_id {
                return Some(*node_id);
            }
        }
        None
    }

    pub fn new_client(&self) -> ClusterClient {
        self.new_client_opt(ClusterClientOptions::default())
    }

    pub fn new_client_opt(&self, options: ClusterClientOptions) -> ClusterClient {
        let lock_resolver = options
            .with_lock_resolver
            .then(|| Box::new(self.new_lock_resolver()));
        let txn_file_helper = options
            .txn_file_max_chunk_size
            .and_then(|size| self.new_txn_client_helper(size));
        ClusterClient {
            opts: options,
            pd_client: self.pd_client.clone(),
            channels: self.channels.clone(),
            region_ranges: Default::default(),
            regions: Default::default(),
            ref_store: self.ref_store.clone(),
            max_ts: Default::default(),
            async_commit: false,
            lock_resolver,
            txn_file_helper,
        }
    }

    pub async fn new_keyspace_client(&self) -> ClusterKeyspaceClient {
        ClusterKeyspaceClient::new(self.new_txn_client().await, self.keyspace_manager.clone())
    }

    pub fn keyspace_manager(&self) -> &KeyspaceManager {
        &self.keyspace_manager
    }

    pub fn new_scheduler(&self) -> Scheduler {
        Scheduler {
            pd: self.get_pd_client(),
            store_ids: self.get_stores(),
            lock: self.schedule_lock.clone(),
        }
    }

    pub fn new_lock_resolver(&self) -> LockResolver {
        // `with_lock_resolver` must be false, otherwise it will cause dead loop.
        LockResolver::new(self.new_client_opt(ClusterClientOptions {
            with_lock_resolver: false,
            ..Default::default()
        }))
    }

    pub fn get_data_stats(&self) -> ClusterDataStats {
        self.get_data_stats_ext(None)
    }

    pub fn get_data_stats_ext(
        &self,
        skip_shards: Option<&HashSet<kvengine::IdVer>>,
    ) -> ClusterDataStats {
        let mut stats = ClusterDataStats::default();
        for server in self.servers.values() {
            let store_id = server.get_store_id();
            let kv_engine = server.get_kv_engine();
            let all_shard_stats = kv_engine.get_all_shard_stats_ext(skip_shards);
            let filtered_shards: Vec<u64> = all_shard_stats.iter().map(|s| s.id).collect();
            stats.add(store_id, all_shard_stats);
            let raft = server.get_raft_engine();
            let rpm = raft.get_region_peer_map();
            let mut truncated_indexes = vec![];
            for shard_id in filtered_shards {
                let Some(&peer_id) = rpm.get(&shard_id) else {
                    warn!(
                        "{}:{} get truncated index: peer not found",
                        store_id, shard_id
                    );
                    continue;
                };
                let truncated_idx = raft.get_truncated_index(peer_id).unwrap_or_default();
                truncated_indexes.push((shard_id, truncated_idx));
            }
            stats.add_truncated_index(store_id, truncated_indexes);
        }
        stats
    }

    pub fn find_shard_with_stats<F>(&self, mut f: F) -> Option<(u64 /* store_id */, ShardStats)>
    where
        F: FnMut(u64 /* store_id */, &ShardStats) -> bool, // found
    {
        for server in self.servers.values() {
            let store_id = server.get_store_id();
            let kv_engine = server.get_kv_engine();
            for shard in kv_engine.get_all_shard_id_vers() {
                if let Some(stats) = kv_engine.get_shard_stat_opt(shard.id) {
                    if f(store_id, &stats) {
                        return Some((store_id, stats));
                    }
                }
            }
        }
        None
    }

    pub fn get_shard_stats(&self, shard_id: u64) -> RegionShardStats {
        let mut stats = RegionShardStats::new(shard_id);
        for server in self.servers.values() {
            let store_id = server.get_store_id();
            let kv_engine = server.get_kv_engine();
            if let Some(shard_stats) = kv_engine.get_shard_stat_opt(shard_id) {
                stats.shard_stats.insert(store_id, shard_stats);
            }
        }
        stats
    }

    pub fn get_shards_has_del_prefixes(&self) -> Vec<ShardStats> {
        self.servers
            .values()
            .flat_map(|server| {
                server
                    .get_kv_engine()
                    .get_all_shard_stats()
                    .into_iter()
                    .filter(|stat| stat.has_del_prefixes)
            })
            .collect()
    }

    pub fn pd_endpoints(&self) -> &[String] {
        self.pd.endpoints().unwrap()
    }

    pub fn status_addr(&self, node_id: u16) -> String {
        node_status_addr(node_id)
    }

    pub async fn new_txn_client(&self) -> ClusterTxnClient {
        let pd_endpoints = self.pd_endpoints().to_vec();
        let client = tikv_client::TransactionClient::new_with_codec(
            pd_endpoints,
            tikv_client::Config::default(),
            ApiV2NoPrefixCodec::default(),
        )
        .await
        .unwrap();
        ClusterTxnClient::new(client, self.get_pure_pd_client(), self.new_client())
    }

    pub fn set_gc_safe_point(&self, ts: u64) {
        let _ = self.get_pd_client().set_gc_safe_point(ts).unwrap();
        for node_id in self.get_nodes() {
            self.get_kvengine(node_id).update_managed_safe_ts(ts);
        }
    }

    pub fn flush_memtable(&self, region_id: u64) -> std::result::Result<(), Error> {
        let mut client = self.new_client();
        let ctx = client.new_rpc_ctx(region_id, b"").unwrap();
        let store_id = ctx.get_peer().get_store_id();
        let version = ctx.get_region_epoch().get_version();
        let tag = format!("{}:{}:{}", store_id, region_id, version);

        let mut req = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(ctx.get_region_id());
        header.set_peer(ctx.get_peer().clone());
        header.set_region_epoch(ctx.get_region_epoch().clone());
        // header.set_term is not necessary, server side will skip checking term when
        // it's not set.

        info!("{} flush_memtable, header {:?}", tag, header);
        req.set_header(header);
        let mut custom_builder = CustomBuilder::new();
        custom_builder.set_switch_mem_table(1);
        req.set_custom_request(custom_builder.build());
        match self.send_raft_command(req) {
            Some(res) => {
                if res.get_header().has_error() {
                    Err(box_err!("{} flush_memtable err {:?}", tag, res))
                } else {
                    Ok(())
                }
            }
            None => Err(box_err!("{} flush_memtable leader peer not found", tag)),
        }
    }

    /// NOTE: Used only when there is no writes.
    pub fn wait_for_memtable_flushed(&self, region_id: u64, timeout: Duration) -> bool {
        try_wait(
            || {
                self.get_shard_stats(region_id)
                    .shard_stats
                    .iter()
                    .all(|(_, shard)| shard.mem_table_is_empty())
            },
            timeout.as_secs() as usize,
        )
    }

    /// Wait for destroy range finished.
    ///
    /// Trigger switch mem-tables if necessary.
    ///
    /// NOTE: Used only when there is no writes.
    pub fn wait_for_destroy_range(&self, timeout: Duration) {
        let get_pending_shards =
            |not_ready_only: bool| -> HashMap<u64 /* region_id */, Vec<ShardStats>> {
                let mut pending_shards = HashMap::new();
                for shard in self.get_shards_has_del_prefixes() {
                    if not_ready_only && shard.ready_to_destroy_range {
                        continue;
                    }

                    pending_shards
                        .entry(shard.id)
                        .or_insert_with(|| Vec::with_capacity(3 /* replicas count */))
                        .push(shard);
                }
                pending_shards
            };

        // Flush mem-table for pending shards which are not ready to destroy range.
        let ok = try_wait(
            || {
                let pending_shards = get_pending_shards(true);
                if pending_shards.is_empty() {
                    return true;
                }
                for &region_id in pending_shards.keys() {
                    if let Err(err) = self.flush_memtable(region_id) {
                        error!("flush_memtable failed"; "region_id" => region_id, "err" => ?err);
                    }
                }
                sleep(Duration::from_millis(500));
                pending_shards
                    .iter()
                    .all(|(&region_id, _)| self.wait_for_memtable_flushed(region_id, timeout / 5))
            },
            timeout.as_secs() as usize,
        );
        assert!(
            ok,
            "wait flush_memtable timeout, pending_shards: {:?}",
            get_pending_shards(true)
        );

        // Wait for delete prefixes.
        let ok = try_wait(
            || self.get_shards_has_del_prefixes().is_empty(),
            timeout.as_secs() as usize,
        );
        assert!(
            ok,
            "wait del_prefixes timeout, pending_shards: {:?}",
            get_pending_shards(false)
        );
    }

    pub fn tikv_worker_endpoints(&self) -> Vec<String> {
        self.tikv_workers
            .keys()
            .map(|&idx| tikv_worker_addr(idx))
            .collect()
    }

    pub fn generate_tikv_worker_configs(&mut self, worker_ids: Vec<u16>, opts: TikvWorkerOptions) {
        let (_, tikv_config) = self.confs.iter().next().unwrap_or_else(|| {
            panic!("no tikv config found");
        });
        let worker_count = worker_ids.len();
        let first_worker_idx = worker_ids[0];
        let last_worker_idx = worker_ids[worker_count - 1];
        for idx in worker_ids {
            let data_dir = self.tmp_dir.path().join(format!("worker-{idx}"));
            std::fs::create_dir_all(&data_dir)
                .unwrap_or_else(|e| panic!("create dir {:?} failed: {:?}", data_dir, e));

            let memory_upper_threshold = if worker_count > 1 && idx == last_worker_idx {
                // Small threshold to cover the process of handling memory limit exceeded.
                (opts.kv_target_file_size.0 * 32).into()
            } else {
                AbsoluteOrPercentSize::Percent(20.0)
            };

            // Timeouts of `tikv_config.dfs.conn_options` is small to cover DFS worker
            // unhealthy. Reset to normal values for tikv workers.
            // TODO: Use small timeouts for tikv workers to cover scene of slow DFS as well.
            let mut dfs = tikv_config.dfs.clone();
            dfs.conn_options = DFSConnOptions::default();

            // Periodical backup can be enabled on NO more than one tikv worker.
            let backup_interval = if idx == first_worker_idx {
                opts.backup_interval
            } else {
                Duration::ZERO
            };

            let mut tikv_worker_conf = cloud_worker::Config {
                addr: tikv_worker_addr(idx),
                cop_addr: "".to_string(),
                pd: pd_client::Config::new(self.pd_endpoints().to_vec()),
                update_interval: TIKV_WORKER_UPDATE_INTERVAL,
                security: tikv_config.security.clone(),
                dfs,
                register: opts.register,
                txn_chunk_manager: TxnChunkManagerConfig {
                    gc_interval: TXN_CHUNK_MGR_GC_INTERVAL,
                    gc_ttl: TXN_CHUNK_MGR_GC_TTL,
                },
                txn_chunk_target_block_size: TXN_CHUNK_TARGET_BLOCK_SIZE,
                cop_block_cache_size: opts.cop_block_cache_size,
                cop_block_size: tikv_config.rocksdb.writecf.block_size,
                cop_max_resp_size: tikv_config.server.cop_max_resp_size,
                data_dir: data_dir.to_string_lossy().into_owned(),
                native_br: NativeBrConfig {
                    backup_interval: ReadableDuration(backup_interval),
                    backup_delay: ReadableDuration(opts.backup_delay),
                    #[cfg(feature = "testexport")]
                    backup_skip_keyspace_meta: opts.backup_skip_keyspace_meta,
                    restore_timeout_pd_control: ReadableDuration(opts.restore_timeout_pd_control),
                    ..Default::default()
                },
                ia: IaConfig {
                    segment_size: opts.ia_segment_size,
                    freq_update_interval: ReadableDuration(opts.ia_freq_update_interval),
                    mem_cap: opts.ia_mem_cap.into(),
                    disk_cap: opts.ia_disk_cap.into(),
                    table_meta_mtime_interval: ReadableDuration::secs(10),
                    ..Default::default()
                },
                local_gc: LocalGcConfig {
                    interval: ReadableDuration::secs(10),
                    ia: IaGcConfig::new_for_test(),
                },
                memory_upper_threshold,
                ..Default::default()
            };

            // Temporary config to fix https://github.com/tidbcloud/cloud-storage-engine/issues/3525.
            // The value is not used as replication worker is disabled.
            // TODO: remove after next upgrade.
            tikv_worker_conf
                .replication_worker
                .merged_engine
                .block_cache_size = (1024 * 1024).into();

            self.tikv_worker_configs.insert(idx, tikv_worker_conf);
        }
    }

    pub fn tikv_worker_configs(&self) -> &HashMap<u16 /* idx */, cloud_worker::Config> {
        &self.tikv_worker_configs
    }

    pub fn start_tikv_workers(&mut self, worker_ids: Vec<u16>, opts: TikvWorkerOptions) {
        let threads_cnt = opts.threads_cnt;
        self.generate_tikv_worker_configs(worker_ids, opts);
        self.start_tikv_workers_on_existed_configs(threads_cnt);
    }

    pub fn start_tikv_workers_on_existed_configs(&mut self, threads_cnt: usize) {
        for (&idx, conf) in &self.tikv_worker_configs {
            let mut worker =
                CloudWorker::new(conf.clone(), None, threads_cnt, self.get_pure_pd_client());
            worker.start();
            let prev = self.tikv_workers.insert(idx, worker);
            assert!(prev.is_none());
        }
    }

    pub fn stop_tikv_workers(&mut self) {
        for (_, child) in self.tikv_workers.drain() {
            child.shutdown();
        }
    }

    pub async fn tikv_workers_must_healthy(&self, timeout: Duration) {
        for ep in self.tikv_worker_endpoints() {
            wait_tikv_worker_healthy(ep.clone(), self.security_mgr.clone(), timeout)
                .await
                .unwrap_or_else(|err| {
                    panic!(
                        "tikv-worker: wait healthy timeout: endpoint {}, err {:?}",
                        ep, err
                    );
                })
        }
    }

    pub fn start_schema_manager(&mut self, worker_idx: u16) {
        assert!(self.schema_manager.is_none());
        let tikv_config = self.confs.iter().next().unwrap().1;
        let dir = self
            .tmp_dir
            .path()
            .join(format!("schema-manager-{worker_idx}"));
        let worker_config = cloud_worker::Config {
            addr: tikv_worker_addr(worker_idx),
            cop_addr: "".to_string(),
            pd: pd_client::Config::new(self.pd_endpoints().to_vec()),
            security: tikv_config.security.clone(),
            dfs: tikv_config.dfs.clone(),
            schema_manager: cloud_worker::SchemaManagerConfig {
                dir,
                enabled: true,
                keyspace_refresh_interval: ReadableDuration::secs(3),
                http_timeout: ReadableDuration::secs(3),
                ..Default::default()
            },
            ..Default::default()
        };

        let mut schema_manager =
            CloudWorker::new(worker_config, None, 1, self.get_pure_pd_client());
        schema_manager.start();
        self.schema_manager = Some(schema_manager);
    }

    pub fn new_txn_client_helper(&self, max_chunk_size: usize) -> Option<Arc<TxnFileHelper>> {
        if self.tikv_workers.is_empty() {
            None
        } else {
            let endpoints = self.tikv_worker_endpoints();
            let runtime = self.dfs.as_ref().unwrap().get_runtime().handle().clone();
            let helper = TxnFileHelper::new(
                max_chunk_size,
                endpoints,
                self.security_mgr.clone(),
                runtime,
            )
            .unwrap();
            Some(Arc::new(helper))
        }
    }

    pub fn request_major_compaction(
        &self,
        target: MajorCompactionTarget,
        allow_not_found: bool,
        timeout: Duration,
    ) {
        let prefix = "major_compact=true";
        let query = match &target {
            MajorCompactionTarget::Keyspace(keyspace_id) => {
                format!("{prefix}&keyspace_id={}", *keyspace_id)
            }
            MajorCompactionTarget::Region(region_id) => {
                format!("{prefix}&region_id={}", *region_id)
            }
            MajorCompactionTarget::Table {
                keyspace_id,
                table_id,
            } => format!(
                "{prefix}&keyspace_id={}&table_id={}",
                *keyspace_id, *table_id
            ),
        };

        let _enter = self.get_dfs().unwrap().get_runtime().enter();

        let pd_client = self.get_pure_pd_client();
        let stores = pd_client.get_all_stores(true).unwrap();
        let mut handles = vec![];
        for store in stores {
            let security_mgr = self.security_mgr.clone();
            handles.push(tokio::spawn(Self::request_major_compaction_on_store(
                security_mgr,
                store,
                query.clone(),
                allow_not_found,
                timeout,
            )));
        }
        block_on(try_join_all(handles)).unwrap();
    }

    async fn request_major_compaction_on_store(
        security_mgr: Arc<SecurityManager>,
        store: Store,
        query: String,
        allow_not_found: bool,
        timeout: Duration,
    ) {
        let store_id = store.id;
        let uri = security_mgr
            .build_uri(format!("{}/major-compact?{}", &store.status_address, query))
            .unwrap();
        try_wait_result_async(
            || {
                let query = query.to_string();
                let uri = uri.clone();
                let client = security_mgr.http_client(hyper::Client::builder()).unwrap();
                Box::pin(async move {
                    let req = Request::post(uri).body(Body::empty()).unwrap();
                    let resp = client.request(req).await.map_err(|err| {
                        warn!("request major compaction failed"; "query" => query, "store" => store_id, "err" => ?err);
                        err
                    })?;
                    let status = resp.status();
                    let is_success = status.is_success()
                        || (allow_not_found && resp.status() == http::StatusCode::NOT_FOUND);
                    let resp_data = hyper::body::to_bytes(resp.into_body()).await.unwrap();
                    assert!(
                        is_success,
                        "request major compaction failed: store {}, {:?}: {}",
                        store_id,
                        status,
                        resp_data.to_str_lossy().as_ref()
                    );
                    Ok(())
                })
            },
            timeout.as_secs() as usize,
        )
        .await
        .unwrap_or_else(|err: hyper::Error| {
            panic!("request major compaction failed: {:?}: {:?}", query, err);
        });
    }

    // Note: Do not use with real PD
    fn keyspace_name(keyspace_id: u32) -> String {
        format!("ks{keyspace_id}")
    }

    // Note: Do not use with real PD. In that scene, keyspaces are allocated by PD.
    pub async fn create_keyspace(&self, options: &CreateKeyspaceOptions, timeout: Duration) -> u32 /* keyspace_id */
    {
        let km = self.keyspace_manager();

        let keyspace_id = km.new_keyspace_id(1);
        km.create_single_keyspace(
            keyspace_id,
            Self::keyspace_name(keyspace_id),
            options,
            false,
        )
        .await;

        let keyspace_split_keys = get_keyspace_split_keys(keyspace_id);
        let pd_client = self.get_pd_client();
        pd_client
            .split_regions_with_retry(keyspace_split_keys, timeout)
            .await
            .unwrap();

        let (table_ids, schemas) = {
            let ks_meta = km.get_keyspace_meta(keyspace_id).unwrap();
            let table_ids = ks_meta.get_all_available_tables();
            let schemas = ks_meta.schemas();
            (table_ids, schemas)
        };

        let table_split_keys = get_table_split_keys(keyspace_id, &table_ids);
        if !table_split_keys.is_empty() {
            pd_client
                .split_regions_with_retry(table_split_keys, timeout)
                .await
                .unwrap();
        }

        if !schemas.is_empty() {
            let schema_version = 10;
            let schema_data: Bytes =
                build_schema_file(keyspace_id, schema_version, schemas, 0).into();
            let schema_file_id = pd_client.alloc_id().unwrap();

            let fs = self.get_dfs().unwrap();
            let _enter = fs.get_runtime().enter();
            fs.create(
                schema_file_id,
                schema_data.clone(),
                dfs::Options::default().with_type(FileType::Schema),
            )
            .await
            .unwrap();
            let schema_file =
                SchemaFile::open(Arc::new(InMemFile::new(schema_file_id, schema_data))).unwrap();
            let stores = pd_client.get_all_stores(true).unwrap();
            broadcast_schema_file_request_and_check(
                &stores,
                keyspace_id,
                &schema_file,
                Duration::from_secs(30),
            )
            .await;
        }

        keyspace_id
    }

    pub fn create_object_cache_randomly(&self) -> Option<ObjectCache> {
        use rand::prelude::*;
        let factor = *[0, 4, 32, 128].choose(&mut thread_rng()).unwrap();
        (factor > 0).then(|| {
            // Ignore the "cache_for_decompressed_wal_chunks" for simplicity.
            let compressed_wal_chunk_size = self
                .get_any_node_config()
                .unwrap()
                .rfengine
                .wal_chunk_target_file_size
                .0
                / 3;
            ObjectCache::new(
                compressed_wal_chunk_size * factor,
                compressed_wal_chunk_size,
            )
        })
    }

    pub fn create_restore_limiter_randomly(
        &self,
        runtime: tokio::runtime::Handle,
    ) -> Option<Arc<ThroughputLimiter>> {
        use rand::prelude::*;
        let max_throughput_mb = *[0u64, 4, 32].choose(&mut thread_rng()).unwrap();
        (max_throughput_mb > 0).then(|| {
            let max_throughput = ReadableSize::mb(max_throughput_mb);
            let config = RateLimitConfig {
                enable: true,
                max_throughput,
                calibrate_restore_size_threshold: ReadableSize(max_throughput.0 / 16),
                store_req_timeout: ReadableDuration::secs(3), /* Small timeout to cover error
                                                               * tolerance. */
                store_cache_ttl: ReadableDuration::secs(5),
            };
            let limiter = ThroughputLimiter::new(&config, self.get_pure_pd_client(), runtime)
                .expect("create ThroughputLimiter failed");
            Arc::new(limiter)
        })
    }

    fn region_panic_mark_path(&self, node_id: u16, region_id: u64) -> PathBuf {
        Path::new(&self.get_node_config(node_id).storage.data_dir)
            .join(format!("panic_region_{}", region_id))
    }

    pub fn create_region_panic_mark(&self, node_id: u16, region_id: u64) {
        let path = self.region_panic_mark_path(node_id, region_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, "10").unwrap();
    }

    pub fn remove_region_panic_mark(&self, node_id: u16, region_id: u64) {
        let path = self.region_panic_mark_path(node_id, region_id);
        if let Err(e) = fs::remove_file(&path) {
            warn!("remove panic region mark failed: {:?}", e; "path" => ?path);
        }
    }
}

impl Drop for ServerCluster {
    fn drop(&mut self) {
        self.stop();
    }
}

pub struct ServerClusterExt {
    pub cluster: ServerCluster,
    pub oss: ObjectStorageService,
    temp_dir: TempDir,
}

impl ops::Deref for ServerClusterExt {
    type Target = ServerCluster;

    fn deref(&self) -> &Self::Target {
        &self.cluster
    }
}

impl ops::DerefMut for ServerClusterExt {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cluster
    }
}

impl Drop for ServerClusterExt {
    fn drop(&mut self) {
        self.cluster.stop();
        self.oss.shutdown();
    }
}

impl ServerClusterExt {
    pub fn path(&self) -> &Path {
        self.temp_dir.path()
    }
}

pub struct ServerClusterBuilder<F> {
    nodes: Vec<u16>,
    update_conf: F,
    pd: Option<PdWrapper>,
    memory_capacity_ratio: f64,
    pd_server_cnt: usize,
    tikv_worker_cnt: usize,
}

impl<F: Fn(u16, &mut TikvConfig)> ServerClusterBuilder<F> {
    pub fn new(nodes: Vec<u16>, update_conf: F) -> Self {
        Self {
            nodes,
            update_conf,
            pd: None,
            memory_capacity_ratio: 1.0,
            pd_server_cnt: 0,
            tikv_worker_cnt: 0,
        }
    }

    pub fn pd(mut self, pd: PdWrapper) -> Self {
        self.pd = Some(pd);
        self
    }

    pub fn memory_capacity_ratio(mut self, ratio: f64) -> Self {
        assert!(0.0 < ratio && ratio <= 1.0);
        self.memory_capacity_ratio = ratio;
        self
    }

    pub fn pd_server_cnt(mut self, cnt: usize) -> Self {
        self.pd_server_cnt = cnt;
        self
    }

    pub fn tikv_worker_cnt(mut self, cnt: usize) -> Self {
        self.tikv_worker_cnt = cnt;
        self
    }

    pub fn build(mut self) -> ServerCluster {
        let pd = self.build_pd();
        ServerCluster::new_opt(self.nodes, self.update_conf, pd, self.memory_capacity_ratio)
    }

    pub fn build_ext(mut self) -> ServerClusterExt {
        let (temp_dir, oss, dfs_config) = prepare_dfs("t");
        let pd = self.build_pd();
        let update_conf = |node_id, conf: &mut TikvConfig| {
            (self.update_conf)(node_id, conf);
            conf.dfs = dfs_config.clone();
        };
        let mut cluster =
            ServerCluster::new_opt(self.nodes, update_conf, pd, self.memory_capacity_ratio);
        if self.tikv_worker_cnt > 0 {
            cluster.start_tikv_workers(
                alloc_node_id_vec(self.tikv_worker_cnt),
                TikvWorkerOptions::default(),
            );
        }
        ServerClusterExt {
            cluster,
            oss,
            temp_dir,
        }
    }

    fn build_pd(&mut self) -> PdWrapper {
        self.pd.take().unwrap_or_else(|| {
            PdWrapper::new_test(self.pd_server_cnt, &SecurityConfig::default(), None)
        })
    }
}

pub fn new_test_config(
    base_dir: &Path,
    node_id: u16,
    nodes_count: usize,
    memory_capacity_ratio: f64,
) -> TikvConfig {
    let mut rng = thread_rng();
    let mut config = TikvConfig::default();
    config.security.master_key.vendor = "test".to_string();
    config.storage.data_dir = format!("{}/{}", base_dir.to_str().unwrap(), node_id);
    config.storage.api_version = 2;
    config.storage.enable_ttl = true;
    config.storage.scheduler_concurrency = 4096;
    config.storage.low_space_threshold = AbsoluteOrPercentSize::Abs(ReadableSize::mb(1));
    config.storage.check_backup_ts = true;
    config.server.cluster_id = 1;
    config.server.addr = node_addr(node_id);
    config.server.status_addr = node_status_addr(node_id);
    config.server.grpc_concurrency = 2;
    config.server.grpc_keepalive_time = ReadableDuration::secs(1);
    config.server.grpc_keepalive_timeout = ReadableDuration::secs(1);
    config.server.cop_max_resp_size = ReadableSize::kb(256);
    config.readpool.unified.max_tasks_per_worker = 4000;
    config.dfs.zstd_compression_level = "3".to_string();
    config.raft_store.raft_base_tick_interval = ReadableDuration::millis(50); // Note: affect rfstore::Config::from_old.
    config.raft_store.raft_election_timeout_ticks = 10;
    config.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(450);
    config.raft_store.split_region_check_tick_interval = ReadableDuration::millis(100);
    config.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(100);
    config.raft_store.raft_log_gc_no_kv_count = 1;
    config.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);
    config.raft_store.pd_store_heartbeat_tick_interval = ReadableDuration::millis(100);
    config.raft_store.peer_long_check_interval = ReadableDuration::secs(2);
    config.raft_store.max_peer_down_duration = ReadableDuration::secs(4);
    config.rocksdb.writecf.write_buffer_size = ReadableSize::kb(16);
    config.rocksdb.writecf.block_size = ReadableSize(BLOCK_SIZE_DEF);
    config.rocksdb.writecf.target_file_size_base = ReadableSize::kb(32);
    config.rocksdb.max_background_jobs = 2;
    config.rocksdb.max_sub_compactions = 1;
    config.rfengine.target_file_size = ReadableSize::kb(128);
    config.rfengine.rlog_file_size =
        config.rfengine.target_file_size / *[2, 8, 32].choose(&mut rng).unwrap();
    config.rfengine.wal_chunk_target_file_size = ReadableSize::kb(16);
    config.rfengine.wal_sync_dir = format!("{}/{}/wal", base_dir.to_str().unwrap(), node_id);
    config.rfengine.enable_compact_rate_limiter = rng.gen_bool(0.5);
    config.rfengine.epoch_rotate_len = rng.gen_range(4..=8);
    config.rfengine.delay_compaction_epoches = config.rfengine.epoch_rotate_len - 4;
    // config.rfengine.wal_secondary_dir = format!("{}/{}/wal2",
    // base_dir.to_str().unwrap(), node_id);
    config.kvengine.block_cache_type = BlockCacheType::Quick;
    config.kvengine.ia.auto_ia_check_interval = ReadableDuration::secs(10);
    config.kvengine.value_cache_capacity = AbsoluteOrPercentSize::Abs(ReadableSize::mb(1));
    config.kvengine.extra_dirs = vec![format!("{}/{}_extra", base_dir.to_str().unwrap(), node_id)];
    config.kvengine.value_cache_double_check = rng.gen_bool(0.5);

    // Work around https://github.com/tidbcloud/cloud-storage-engine/issues/882.
    config.server.raft_client_initial_reconnect_backoff = ReadableDuration::millis(100);
    config.server.raft_client_max_backoff = ReadableDuration::millis(250);

    update_config_by_total_mem(&mut config, nodes_count, memory_capacity_ratio);
    config
        .storage
        .flow_control
        .validate()
        .expect("storage.flow-control is invalid"); // To fill optional arguments.
    config
}

fn update_config_by_total_mem(
    config: &mut TikvConfig,
    nodes_count: usize,
    memory_capacity_ratio: f64,
) {
    let total_mem =
        (SysQuota::memory_limit_in_bytes() / nodes_count as u64) as f64 * memory_capacity_ratio;

    config.storage.block_cache.capacity = Some(ReadableSize(
        (total_mem * tikv::config::BLOCK_CACHE_RATE) as u64,
    ));

    let soft_store_mem_limit = total_mem * tikv::storage::config::SOFT_STORE_MEM_LIMIT_RATE;
    let hard_store_mem_limit = total_mem * tikv::storage::config::HARD_STORE_MEM_LIMIT_RATE;
    config.storage.flow_control.soft_store_mem_limit =
        Some(ReadableSize(soft_store_mem_limit as u64));
    config.storage.flow_control.hard_store_mem_limit =
        Some(ReadableSize(hard_store_mem_limit as u64));
    config.storage.flow_control.soft_region_mem_limit =
        ReadableSize((soft_store_mem_limit * REGION_MEM_LIMIT_RATIO) as u64);
    config.storage.flow_control.hard_region_mem_limit =
        ReadableSize((hard_store_mem_limit * REGION_MEM_LIMIT_RATIO) as u64);
}

pub trait TikvConfigExt {
    fn disable_ia(&mut self);
    fn enable_ia(&mut self);
}

impl TikvConfigExt for TikvConfig {
    fn disable_ia(&mut self) {
        self.kvengine.ia.mem_cap = 0.into();
        self.kvengine.ia.disk_cap = 0.into();
    }

    fn enable_ia(&mut self) {
        self.kvengine.ia.mem_cap = IA_MEM_CAP_DEF.into();
        self.kvengine.ia.disk_cap = IA_DISK_CAP_DEF.into();
    }
}

// Keep away from 20xxx ports to work around https://github.com/tidbcloud/cloud-storage-engine/issues/658.
// TODO: Remove this work around.
fn node_addr(node_id: u16) -> String {
    format!("127.0.0.1:{}", node_id + 21000)
}

// Keep away from 3xxxx ports to work around https://github.com/tidbcloud/cloud-storage-engine/issues/658.
// TODO: Remove this work around.
fn node_status_addr(node_id: u16) -> String {
    format!("127.0.0.1:{}", node_id + 25000)
}

fn tikv_worker_addr(idx: u16) -> String {
    format!("127.0.0.1:{}", 17000 + idx)
}

pub fn tikv_worker_cop_url(idx: u16) -> String {
    format!("http://{}/coprocessor", tikv_worker_addr(idx))
}

#[derive(Clone)]
pub struct TikvWorkerOptions {
    pub threads_cnt: usize,
    pub kv_target_file_size: ReadableSize,
    pub cop_block_cache_size: ReadableSize,
    pub register: bool,

    pub ia_segment_size: i64,
    pub ia_freq_update_interval: Duration,
    pub ia_mem_cap: u64,
    pub ia_disk_cap: u64,

    pub backup_interval: Duration,
    pub backup_delay: Duration,
    pub backup_skip_keyspace_meta: bool,

    pub restore_timeout_pd_control: Duration,
}

impl Default for TikvWorkerOptions {
    fn default() -> Self {
        Self {
            threads_cnt: 2,
            kv_target_file_size: ReadableSize::kb(16),
            cop_block_cache_size: ReadableSize::mb(8),
            register: true,
            ia_segment_size: IA_SEGMENT_SIZE_DEF,
            ia_freq_update_interval: IA_FREQ_UPDATE_INTERVAL_DEF,
            ia_mem_cap: IA_MEM_CAP_DEF,
            ia_disk_cap: IA_DISK_CAP_DEF,
            backup_interval: Duration::ZERO,
            backup_delay: Duration::ZERO,
            backup_skip_keyspace_meta: true,
            restore_timeout_pd_control: Duration::from_secs(10),
        }
    }
}

impl TikvWorkerOptions {
    pub fn disable_ia(&mut self) {
        self.ia_mem_cap = 0;
        self.ia_disk_cap = 0;
    }
}

pub fn put_mut(key: &str, val: &str) -> Mutation {
    let mut mutation = Mutation::new();
    mutation.op = Op::Put;
    mutation.key = key.as_bytes().to_vec();
    mutation.value = val.as_bytes().to_vec();
    mutation
}

pub struct TryWaiter {
    timeout: Duration,
    interval: Duration,
}

impl Default for TryWaiter {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            interval: Duration::from_millis(100),
        }
    }
}

impl TryWaiter {
    pub fn timeout_dur(timeout: Duration) -> Self {
        Self {
            timeout,
            ..Default::default()
        }
    }

    pub fn timeout(secs: usize) -> Self {
        Self::timeout_dur(Duration::from_secs(secs as u64))
    }

    #[must_use]
    pub fn interval_dur(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    #[must_use]
    pub fn interval(self, secs: usize) -> Self {
        self.interval_dur(Duration::from_secs(secs as u64))
    }

    #[must_use]
    pub fn try_wait<F>(&self, mut f: F) -> bool
    where
        F: FnMut() -> bool,
    {
        let begin = Instant::now_coarse();
        while begin.saturating_elapsed() < self.timeout {
            if f() {
                return true;
            }
            sleep(self.interval)
        }
        false
    }

    pub fn try_wait_result<T, E, F>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Result<T, E>,
    {
        let begin = Instant::now_coarse();
        let mut last_err: Option<E> = None;
        while begin.saturating_elapsed() < self.timeout {
            match f() {
                Ok(t) => return Ok(t),
                Err(e) => last_err = Some(e),
            }
            sleep(self.interval)
        }
        Err(last_err.unwrap())
    }

    #[track_caller]
    pub fn must_wait<F, FnMsg>(&self, f: F, fail_msg: FnMsg)
    where
        F: FnMut() -> bool,
        FnMsg: FnOnce() -> String,
    {
        if !self.try_wait(f) {
            panic!("{}", fail_msg());
        }
    }

    pub fn must_wait_result<T, E, F, FnMsg>(&self, f: F, fail_msg: FnMsg) -> T
    where
        E: std::fmt::Debug,
        F: FnMut() -> Result<T, E>,
        FnMsg: FnOnce() -> String,
    {
        match self.try_wait_result(f) {
            Ok(t) => t,
            Err(err) => {
                panic!("{}: {:?}", fail_msg(), err);
            }
        }
    }
}

#[track_caller]
pub fn must_wait<F, FnMsg>(f: F, seconds: usize, fail_msg: FnMsg)
where
    F: FnMut() -> bool,
    FnMsg: FnOnce() -> String,
{
    TryWaiter::timeout(seconds).must_wait(f, fail_msg);
}

#[must_use]
pub fn try_wait<F>(f: F, seconds: usize) -> bool
where
    F: FnMut() -> bool,
{
    TryWaiter::timeout(seconds).try_wait(f)
}

/// Return `None` when then the premise is not satisfied.
/// `retry_idx` starts from 0.
#[must_use]
pub fn try_wait_with_premise<T, P, F>(premise: P, mut f: F, seconds: usize) -> Option<bool>
where
    P: Fn() -> Option<T>,
    F: FnMut(&T, usize /* retry_idx */) -> bool,
{
    let begin = Instant::now_coarse();
    let mut retry_idx = 0;
    let timeout = Duration::from_secs(seconds as u64);
    while begin.saturating_elapsed() < timeout {
        let t = premise()?;
        if f(&t, retry_idx) {
            return Some(true);
        }
        sleep(Duration::from_millis(100));
        retry_idx += 1;
    }
    Some(false)
}

/// Return `None` when then the premise is not satisfied.
/// `retry_idx` starts from 0.
#[track_caller]
#[must_use]
pub fn must_wait_with_premise<T, P, F, FnMsg>(
    premise: P,
    f: F,
    seconds: usize,
    fail_msg: FnMsg,
) -> Option<()>
where
    P: Fn() -> Option<T>,
    F: FnMut(&T, usize /* retry_idx */) -> bool,
    FnMsg: FnOnce() -> String,
{
    let ok = try_wait_with_premise(premise, f, seconds)?;
    if !ok {
        panic!("{}", fail_msg());
    }
    Some(())
}

pub async fn try_wait_async<F>(mut f: F, seconds: usize) -> bool
where
    F: FnMut() -> futures::future::BoxFuture<'static, bool>,
{
    let begin = Instant::now_coarse();
    let timeout = Duration::from_secs(seconds as u64);
    while begin.saturating_elapsed() < timeout {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

pub async fn try_wait_result_async<T, F, E>(mut f: F, seconds: usize) -> std::result::Result<T, E>
where
    F: FnMut() -> futures::future::BoxFuture<'static, std::result::Result<T, E>>,
{
    let begin = Instant::now_coarse();
    let timeout = Duration::from_secs(seconds as u64);
    let mut last_err: Option<E> = None;
    while begin.saturating_elapsed() < timeout {
        match f().await {
            Ok(t) => return Ok(t),
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    Err(last_err.unwrap())
}

pub fn try_wait_result<F, T, E>(f: F, seconds: usize) -> std::result::Result<T, E>
where
    F: FnMut() -> std::result::Result<T, E>,
{
    TryWaiter::timeout(seconds).try_wait_result(f)
}

pub fn must_wait_result<F, T, E, FnMsg>(f: F, seconds: usize, fail_msg: FnMsg) -> T
where
    E: std::fmt::Debug,
    F: FnMut() -> Result<T, E>,
    FnMsg: FnOnce() -> String,
{
    TryWaiter::timeout(seconds).must_wait_result(f, fail_msg)
}

#[derive(Default, Debug)]
pub struct ClusterDataStats {
    regions: HashMap<u64, RegionShardStats>,
}

impl ClusterDataStats {
    fn add(&mut self, store_id: u64, shard_stats: Vec<ShardStats>) {
        for shard_stat in shard_stats {
            let region_shard_stats = self
                .regions
                .entry(shard_stat.id)
                .or_insert_with(|| RegionShardStats::new(shard_stat.id));
            region_shard_stats.shard_stats.insert(store_id, shard_stat);
        }
    }

    fn add_truncated_index(&mut self, store_id: u64, truncated_indexes: Vec<(u64, u64)>) {
        for (region_id, truncated_idx) in truncated_indexes {
            let region_shard_stats = self
                .regions
                .entry(region_id)
                .or_insert_with(|| RegionShardStats::new(region_id));
            region_shard_stats
                .truncated_index
                .insert(store_id, truncated_idx);
        }
    }

    pub fn check_data(
        &self,
    ) -> Result<
        (),
        (
            Vec<String>,              // error reasons
            HashSet<kvengine::IdVer>, // success_shards
        ),
    > {
        let mut success_shards = HashSet::with_capacity(self.regions.len());
        let mut errs = vec![];
        for stats in self.regions.values() {
            let map_err_fn = |e| format!("err {} stats: {:?}", e, stats);
            if let Err(err) = stats.check_consistency() {
                errs.push(map_err_fn(err));
                continue;
            }
            let leader = match stats.check_healthy() {
                Ok(leader) => leader,
                Err(err) => {
                    errs.push(map_err_fn(err));
                    continue;
                }
            };
            success_shards.insert(kvengine::IdVer::new(leader.id, leader.ver));
        }
        if errs.is_empty() {
            Ok(())
        } else {
            Err((errs, success_shards))
        }
    }

    pub fn check_leader(&self) -> Result<(), String> {
        for stats in self.regions.values() {
            let map_err_fn = |e| format!("err {} stats: {:?}", e, stats);
            stats.get_leader_stats().map_err(map_err_fn)?;
        }
        Ok(())
    }

    pub fn iter_shard_stats(&self, mut f: impl FnMut(u64, &ShardStats) -> bool) {
        for region in self.regions.values() {
            for (&store_id, shard) in &region.shard_stats {
                if f(store_id, shard) {
                    return;
                }
            }
        }
    }

    pub fn log_all(&self) {
        self.iter_shard_stats(|store_id, shard_stats| {
            info!("shard_stats: {}:{:?}", store_id, shard_stats);
            false
        });
    }

    fn get_region_shard_stats(&self, region_id: u64) -> Option<&ShardStats> {
        self.regions
            .get(&region_id)
            .map(|stats| stats.shard_stats.values().next().unwrap())
    }

    pub fn check_buckets(
        &self,
        pd_client: &dyn PdClientExt,
        bucket_size: u64,
        starts_from_encoded_key: &[u8],
    ) -> Result<(), (String /* reason */, Option<metapb::Region>)> {
        let regions = pd_client.get_all_regions(); // TODO: get regions from `starts_from_encoded_key`.
        check_regions_boundary(starts_from_encoded_key, &[], true, &regions)
            .map_err(|e| (format!("check_regions_boundary failed: {:?}", e), None))?;
        for region in regions {
            if !region.end_key.is_empty() && region.end_key.as_slice() <= starts_from_encoded_key {
                continue;
            }

            let region_id = region.get_id();
            let region_shard_stats = self.get_region_shard_stats(region_id).ok_or_else(|| {
                (
                    "region not found in cluster".to_owned(),
                    Some(region.clone()),
                )
            })?;
            let shard_level_size: u64 =
                region_shard_stats.total_size - region_shard_stats.mem_table_size;
            if shard_level_size == 0 {
                continue;
            }
            // If the entries is too small, the bucket count is not accurate. The region
            // only contains a few large keys.
            if region_shard_stats.entries < 10 {
                continue;
            }

            let region_pd_version = region.get_region_epoch().get_version();
            let region_shard_version = region_shard_stats.ver;
            if region_pd_version != region_shard_version {
                return Err((
                    format!(
                        "version not match, pd: {}, shard: {}",
                        region_pd_version, region_shard_version
                    ),
                    Some(region),
                ));
            }

            if let Some(buckets) = pd_client.get_buckets(region_id) {
                for i in 1..buckets.meta.keys.len() {
                    let prev_key = &buckets.meta.keys[i - 1];
                    let key = &buckets.meta.keys[i];
                    if !key.is_empty() {
                        assert!(prev_key < key, "region {} buckets {:?}", region_id, buckets);
                    }
                }
                let expected_bucket_count = shard_level_size.div_ceil(bucket_size);
                let actual_bucket_count = buckets.count() as u64;
                let ratio = expected_bucket_count as f64 / actual_bucket_count as f64;
                if !(0.3..=3.0).contains(&ratio) {
                    return Err((
                        format!(
                            "buckets {:?}, shard_level_size {}, expected {}, actual {}, shard stats {:?}",
                            buckets,
                            shard_level_size,
                            expected_bucket_count,
                            actual_bucket_count,
                            region_shard_stats,
                        ),
                        Some(region),
                    ));
                };
            } else {
                return Err((
                    format!("no buckets, shard_level_size {}", shard_level_size),
                    Some(region),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Default, Debug)]
#[allow(dead_code)]
pub struct RegionShardStats {
    region_id: u64,
    // store_id -> ShardStats
    shard_stats: HashMap<u64, ShardStats>,
    // store_id -> truncated index
    truncated_index: HashMap<u64, u64>,
}

impl RegionShardStats {
    fn new(region_id: u64) -> Self {
        Self {
            region_id,
            shard_stats: Default::default(),
            truncated_index: Default::default(),
        }
    }

    fn check_consistency(&self) -> anyhow::Result<()> {
        if self.shard_stats.len() <= 1 {
            return Ok(());
        }
        let store_ids: Vec<u64> = self.shard_stats.keys().copied().collect();
        let first_id = &store_ids[0];
        let first_stats = self.shard_stats.get(first_id).unwrap();
        for store_id in &store_ids[1..] {
            let stats = self.shard_stats.get(store_id).unwrap();
            if stats.total_size != first_stats.total_size
                || stats.mem_table_count != first_stats.mem_table_count
                || stats.mem_table_size != first_stats.mem_table_size
                || stats.entries != first_stats.entries
                || stats.l0_table_count != first_stats.l0_table_count
                || stats.ver != first_stats.ver
            {
                bail!(
                    "inconsistent stats, first: {}:{}:{}: {:?}, current: {}:{}:{}: {:?}",
                    first_id,
                    first_stats.id,
                    first_stats.ver,
                    first_stats,
                    store_id,
                    stats.id,
                    stats.ver,
                    stats
                );
            }
        }
        Ok(())
    }

    fn get_leader_stats(&self) -> anyhow::Result<&ShardStats> {
        let item = self.shard_stats.values().find(|stats| stats.active);
        if item.is_none() {
            bail!("no leader");
        }
        Ok(item.unwrap())
    }

    fn check_healthy(&self) -> anyhow::Result<&ShardStats> {
        let stats = self.get_leader_stats()?;
        if stats.mem_table_count > 1 {
            bail!("mem table count {} too large", stats.mem_table_count);
        }
        if !stats.flushed {
            bail!("not initial flushed");
        }
        if stats.compaction_score > 2.0 {
            bail!("compaction score too large: {}", stats.compaction_score);
        }
        self.check_truncate_index()?;
        Ok(stats)
    }

    fn check_truncate_index(&self) -> anyhow::Result<()> {
        let mut checked_shards = 0;
        for (store_id, shard_stat) in &self.shard_stats {
            if shard_stat.mem_table_size > 0 || shard_stat.mem_table_unpersisted_props_size > 0 {
                continue;
            }
            let Some(&truncated_index) = self.truncated_index.get(store_id) else {
                continue;
            };
            if truncated_index < shard_stat.write_sequence {
                bail!(
                    "{}:{}: empty mem-table shard not truncated, expect: {}, got: {}",
                    store_id,
                    self.region_id,
                    shard_stat.write_sequence,
                    truncated_index
                );
            }
            checked_shards += 1;
        }
        info!("check truncate index done, shards: {}", checked_shards);
        Ok(())
    }
}

pub enum MajorCompactionTarget {
    Keyspace(u32),
    Region(u64),
    Table { keyspace_id: u32, table_id: i64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_wait() {
        let mut cnt = 0;
        let ok = try_wait(
            || {
                cnt += 1;
                false
            },
            1,
        );
        assert!(!ok);
        // `>= 9` in theory, but use `2` to be stable.
        // In a busy env, it may not have enough chance to retry.
        assert!(cnt >= 2);

        let ok = try_wait(|| true, 1);
        assert!(ok);
    }
}
