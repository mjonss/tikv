// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(let_chains)]

#[allow(unused_extern_crates)]
extern crate tikv_alloc;

#[macro_use]
extern crate serde_derive;

pub mod client;
mod client_v2;
mod feature_gate;
pub mod metrics;
mod tso;
pub mod util;
use security::GetSecurityManager;
pub use util::{check_regions_boundary, grpc_error_is_unimplemented};

mod config;
pub mod errors;
pub mod pd_control;

use std::{cmp::Ordering, collections::HashMap, ops::Deref, sync::Arc, time::Duration};

use async_trait::async_trait;
use cloud_encryption::KeyspaceEncryptionConfig;
use dashmap::DashMap;
use futures::{compat::Future01CompatExt, future::BoxFuture};
use grpcio::ClientSStreamReceiver;
use kvproto::{
    metapb, pdpb,
    replication_modepb::{RegionReplicationStatus, ReplicationStatus, StoreDrAutoSyncStatus},
};
use pdpb::{LoadGlobalConfigRequest, QueryStats, WatchGlobalConfigResponse};
use tikv_util::{
    time::{Instant, UnixSecs},
    timer::GLOBAL_TIMER_HANDLE,
};
use txn_types::TimeStamp;

pub use self::{
    client::RpcClient,
    client_v2::{PdClient as PdClientV2, RpcClient as RpcClientV2},
    config::Config,
    errors::{Error, Result},
    feature_gate::{Feature, FeatureGate},
    util::{
        PdConnector, REQUEST_RECONNECT_INTERVAL, merge_bucket_stats, new_bucket_stats,
        new_bucket_write_stats, simple_merge_bucket_write_stats,
    },
};

pub type Key = Vec<u8>;
pub type PdFuture<T> = BoxFuture<'static, Result<T>>;

#[derive(Default, Clone)]
pub struct RegionStat {
    pub down_peers: Vec<pdpb::PeerStats>,
    pub pending_peers: Vec<metapb::Peer>,
    pub written_bytes: u64,
    pub written_keys: u64,
    pub read_bytes: u64,
    pub read_keys: u64,
    pub query_stats: QueryStats,
    pub approximate_size: u64,
    pub approximate_keys: u64,
    pub approximate_kv_size: u64,
    pub approximate_columnar_size: u64,
    pub approximate_columnar_kv_size: u64,
    pub last_report_ts: UnixSecs,
    // cpu_usage is the CPU time usage of the leader region since the last heartbeat,
    // which is calculated by cpu_time_delta/heartbeat_reported_interval.
    pub cpu_usage: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RegionInfo {
    pub region: metapb::Region,
    pub leader: Option<metapb::Peer>,
}

impl RegionInfo {
    pub fn new(region: metapb::Region, leader: Option<metapb::Peer>) -> RegionInfo {
        RegionInfo { region, leader }
    }
}

impl Deref for RegionInfo {
    type Target = metapb::Region;

    fn deref(&self) -> &Self::Target {
        &self.region
    }
}

#[derive(Default, Debug, Clone)]
pub struct BucketMeta {
    pub region_id: u64,
    pub version: u64,
    pub region_epoch: metapb::RegionEpoch,
    pub keys: Vec<Vec<u8>>, // keys are encoded.
    pub sizes: Vec<u64>,
}

impl Eq for BucketMeta {}

impl PartialEq for BucketMeta {
    fn eq(&self, other: &Self) -> bool {
        self.region_id == other.region_id
            && self.region_epoch.get_version() == other.region_epoch.get_version()
            && self.version == other.version
    }
}

impl PartialOrd for BucketMeta {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BucketMeta {
    fn cmp(&self, other: &Self) -> Ordering {
        match self
            .region_epoch
            .get_version()
            .cmp(&other.region_epoch.get_version())
        {
            Ordering::Equal => self.version.cmp(&other.version),
            ord => ord,
        }
    }
}

impl BucketMeta {
    pub fn new(
        region: &metapb::Region,
        bucket_keys: Vec<Vec<u8>>,
        bucket_initial_size: u64,
    ) -> Self {
        assert!(bucket_keys.len() >= 2);
        let bucket_count = bucket_keys.len() - 1;
        Self {
            region_id: region.get_id(),
            version: 0,
            region_epoch: region.get_region_epoch().clone(),
            keys: bucket_keys,
            sizes: vec![bucket_initial_size; bucket_count],
        }
    }

    pub fn incr_version(&mut self, term: u64) {
        // bucket version layout
        //     term     logical counter
        // |-----------|---------------|
        //   high bits     low bits
        // term: given 10s election timeout, the 32 bit means 1362 year running time
        let current_version_term = self.version >> 32;
        let bucket_version: u64 = if current_version_term == term {
            self.version + 1
        } else {
            term << 32
        };
        self.version = bucket_version;
    }

    pub fn split(&mut self, idx: usize, key: Vec<u8>) {
        assert!(idx != 0);
        self.keys.insert(idx, key);
        self.sizes.insert(idx, self.sizes[idx - 1]);
    }

    pub fn left_merge(&mut self, idx: usize) {
        self.sizes[idx - 1] += self.sizes[idx];
        self.keys.remove(idx);
        self.sizes.remove(idx);
    }

    pub fn span_count(&self, encoded_lower_bound: &[u8], encoded_upper_bound: &[u8]) -> usize {
        let mut overlap_count = 0;
        let start = 1;
        let end = self.keys.len() - 1;
        for key in &self.keys[start..end] {
            if encoded_lower_bound <= key.as_slice() && key.as_slice() < encoded_upper_bound {
                overlap_count += 1;
            }
        }
        overlap_count
    }
}

#[derive(Debug, Clone)]
pub struct BucketStat {
    pub meta: Arc<BucketMeta>,
    pub stats: metapb::BucketStats,
    pub create_time: Instant,
}

impl Default for BucketStat {
    fn default() -> Self {
        Self {
            create_time: Instant::now(),
            meta: Arc::default(),
            stats: metapb::BucketStats::default(),
        }
    }
}

impl BucketStat {
    pub fn new(meta: Arc<BucketMeta>, stats: metapb::BucketStats) -> Self {
        Self {
            meta,
            stats,
            create_time: Instant::now(),
        }
    }

    pub fn count(&self) -> usize {
        self.meta.keys.len() - 1
    }

    pub fn write_key(&mut self, key: &[u8], value_size: u64) {
        let idx = match util::find_bucket_index(key, &self.meta.keys) {
            Some(idx) => idx,
            None => return,
        };
        if let Some(keys) = self.stats.mut_write_keys().get_mut(idx) {
            *keys += 1;
        }
        if let Some(bytes) = self.stats.mut_write_bytes().get_mut(idx) {
            *bytes += key.len() as u64 + value_size;
        }
    }

    pub fn split(&mut self, idx: usize) {
        assert!(idx != 0);
        // inherit the traffic stats for splited bucket
        let val = self.stats.write_keys[idx - 1];
        self.stats.mut_write_keys().insert(idx, val);
        let val = self.stats.write_bytes[idx - 1];
        self.stats.mut_write_bytes().insert(idx, val);
        let val = self.stats.read_qps[idx - 1];
        self.stats.mut_read_qps().insert(idx, val);
        let val = self.stats.write_qps[idx - 1];
        self.stats.mut_write_qps().insert(idx, val);
        let val = self.stats.read_keys[idx - 1];
        self.stats.mut_read_keys().insert(idx, val);
        let val = self.stats.read_bytes[idx - 1];
        self.stats.mut_read_bytes().insert(idx, val);
    }

    pub fn left_merge(&mut self, idx: usize) {
        assert!(idx != 0);
        let val = self.stats.mut_write_keys().remove(idx);
        self.stats.mut_write_keys()[idx - 1] += val;
        let val = self.stats.mut_write_bytes().remove(idx);
        self.stats.mut_write_bytes()[idx - 1] += val;
        let val = self.stats.mut_read_qps().remove(idx);
        self.stats.mut_read_qps()[idx - 1] += val;
        let val = self.stats.mut_write_qps().remove(idx);
        self.stats.mut_write_qps()[idx - 1] += val;
        let val = self.stats.mut_read_keys().remove(idx);
        self.stats.mut_read_keys()[idx - 1] += val;
        let val = self.stats.mut_read_bytes().remove(idx);
        self.stats.mut_read_bytes()[idx - 1] += val;
    }

    // We create the buckets with write bytes and write keys.
    // Before report, we need to init other stats to avoid index out of bound panic.
    pub fn prepare_report(&mut self) {
        let count = self.stats.write_bytes.len();
        self.stats.set_write_qps(vec![0; count]);
        self.stats.set_read_bytes(vec![0; count]);
        self.stats.set_read_keys(vec![0; count]);
        self.stats.set_read_qps(vec![0; count]);
    }
}

pub const INVALID_ID: u64 = 0;

const SPLIT_REGIONS_INIT_RETRY_INTERVAL_MS: u64 = 100;
const SPLIT_REGIONS_MAX_RETRY_INTERVAL_MS: u64 = 3000;

/// PdClient communicates with Placement Driver (PD).
/// Because now one PD only supports one cluster, so it is no need to pass
/// cluster id in trait interface every time, so passing the cluster id when
/// creating the PdClient is enough and the PdClient will use this cluster id
/// all the time.
#[allow(clippy::diverging_sub_expression)]
#[async_trait]
pub trait PdClient: GetSecurityManager + Send + Sync {
    /// Load GlobalConfig from PD by name
    fn load_global_config_by_names(
        &self,
        _names: Vec<String>,
    ) -> PdFuture<HashMap<String, Vec<u8>>> {
        unimplemented!();
    }

    /// Load GlobalConfig from PD by path
    fn load_global_config_by_path(&self, _path: String) -> PdFuture<HashMap<String, Vec<u8>>> {
        unimplemented!();
    }

    /// Load GlobalConfig from PD
    fn load_global_config(
        &self,
        _req: LoadGlobalConfigRequest,
    ) -> PdFuture<HashMap<String, Vec<u8>>> {
        unimplemented!();
    }

    /// Store a list of GlobalConfig
    fn store_global_config(&self, _list: HashMap<String, String>) -> PdFuture<()> {
        unimplemented!();
    }

    /// Watching change of GlobalConfig
    fn watch_global_config(&self) -> Result<ClientSStreamReceiver<WatchGlobalConfigResponse>> {
        unimplemented!();
    }

    async fn watch_gc_safepoint_v2(&self) {
        unimplemented!();
    }

    fn get_keyspace_gc_safepoint_v2_cache(&self) -> Arc<DashMap<u32, u64>> {
        Arc::new(DashMap::default())
    }

    /// Returns the cluster ID.
    fn get_cluster_id(&self) -> Result<u64> {
        unimplemented!();
    }

    /// Creates the cluster with cluster ID, node, stores and first Region.
    /// If the cluster is already bootstrapped, return ClusterBootstrapped
    /// error. When a node starts, if it finds nothing in the node and
    /// cluster is not bootstrapped, it begins to create node, stores, first
    /// Region and then call bootstrap_cluster to let PD know it.
    /// It may happen that multi nodes start at same time to try to
    /// bootstrap, but only one can succeed, while others will fail
    /// and must remove their created local Region data themselves.
    fn bootstrap_cluster(
        &self,
        _stores: metapb::Store,
        _region: metapb::Region,
    ) -> Result<Option<ReplicationStatus>> {
        unimplemented!();
    }

    /// Returns whether the cluster is bootstrapped or not.
    ///
    /// Cluster must be bootstrapped when we use it, so when the
    /// node starts, `is_cluster_bootstrapped` must be called,
    /// and panics if cluster was not bootstrapped.
    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        unimplemented!();
    }

    /// Allocates a unique positive id.
    fn alloc_id(&self) -> Result<u64> {
        unimplemented!();
    }

    /// Returns whether the cluster is marked to start with snapshot recovery.
    ///
    /// Cluster is marked as recovering data before start up
    /// Nomally, marker has been set by BR (from now), and tikv have to run in
    /// recovery mode recovery mode will do
    /// 1. update tikv cluster id from pd
    /// 2. all peer apply the log to last of the leader peer which has the most
    /// log appended. 3. delete data to some point of time (resolved_ts)
    fn is_recovering_marked(&self) -> Result<bool> {
        unimplemented!();
    }

    /// Informs PD when the store starts or some store information changes.
    fn put_store(&self, _store: metapb::Store) -> Result<Option<ReplicationStatus>> {
        unimplemented!();
    }

    /// We don't need to support Region and Peer put/delete,
    /// because PD knows all Region and Peers itself:
    /// - For bootstrapping, PD knows first Region with `bootstrap_cluster`.
    /// - For changing Peer, PD determines where to add a new Peer in some store
    ///   for this Region.
    /// - For Region splitting, PD determines the new Region id and Peer id for
    ///   the split Region.
    /// - For Region merging, PD knows which two Regions will be merged and
    ///   which Region and Peers will be removed.
    /// - For auto-balance, PD determines how to move the Region from one store
    ///   to another.
    /// Gets store information if it is not a tombstone store.
    fn get_store(&self, _store_id: u64) -> Result<metapb::Store> {
        unimplemented!();
    }

    /// Gets store information if it is not a tombstone store asynchronously
    fn get_store_async(&self, _store_id: u64) -> PdFuture<metapb::Store> {
        unimplemented!();
    }

    /// Gets all stores information.
    fn get_all_stores(&self, _exclude_tombstone: bool) -> Result<Vec<metapb::Store>> {
        unimplemented!();
    }

    fn get_all_stores_async(&self, _exclude_tombstone: bool) -> PdFuture<Vec<metapb::Store>> {
        unimplemented!();
    }

    /// Gets cluster meta information.
    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        unimplemented!();
    }

    /// For route.
    /// Gets Region which the key belongs to.
    fn get_region(&self, _key: &[u8]) -> Result<metapb::Region> {
        unimplemented!();
    }

    /// Retry to tolerate region holes during split.
    fn get_region_with_retry(&self, key: &[u8], timeout: Duration) -> Result<metapb::Region> {
        use tikv_util::{backoff::ExponentialBackoff, retry::try_wait_result};
        let mut bo = ExponentialBackoff::new(
            Duration::from_millis(100),
            Duration::from_secs(1),
            usize::MAX,
        );
        try_wait_result(
            || self.get_region(key),
            timeout,
            || bo.next_delay().unwrap(),
        )
    }

    /// Gets Region which the key belongs to asynchronously.
    fn get_region_async<'k>(&'k self, _key: &'k [u8]) -> BoxFuture<'k, Result<metapb::Region>> {
        unimplemented!();
    }

    /// Gets Region info which the key belongs to.
    fn get_region_info(&self, _key: &[u8]) -> Result<RegionInfo> {
        unimplemented!();
    }

    /// Gets Region info which the key belongs to asynchronously.
    fn get_region_info_async<'k>(&'k self, _key: &'k [u8]) -> BoxFuture<'k, Result<RegionInfo>> {
        unimplemented!();
    }

    /// Gets Region by Region id.
    fn get_region_by_id(&self, _region_id: u64) -> PdFuture<Option<metapb::Region>> {
        unimplemented!();
    }

    /// Gets Region and its leader by Region id.
    fn get_region_leader_by_id(
        &self,
        _region_id: u64,
    ) -> PdFuture<Option<(metapb::Region, metapb::Peer)>> {
        unimplemented!();
    }

    /// Region's Leader uses this to heartbeat PD.
    fn region_heartbeat(
        &self,
        _term: u64,
        _region: metapb::Region,
        _leader: metapb::Peer,
        _region_stat: RegionStat,
        _replication_status: Option<RegionReplicationStatus>,
    ) -> PdFuture<()> {
        unimplemented!();
    }

    /// Gets a stream of Region heartbeat response.
    ///
    /// Please note that this method should only be called once.
    fn handle_region_heartbeat_response(
        &self,
        _store_id: u64,
        _f: Box<dyn Fn(pdpb::RegionHeartbeatResponse) + Send + 'static>,
    ) -> PdFuture<()> {
        unimplemented!();
    }

    /// Asks PD for split. PD returns the newly split Region id.
    fn ask_split(&self, _region: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
        unimplemented!();
    }

    /// Asks PD for batch split. PD returns the newly split Region ids.
    fn ask_batch_split(
        &self,
        _region: metapb::Region,
        _count: usize,
    ) -> PdFuture<pdpb::AskBatchSplitResponse> {
        unimplemented!();
    }

    /// Sends store statistics regularly.
    fn store_heartbeat(
        &self,
        _stats: pdpb::StoreStats,
        _report: Option<pdpb::StoreReport>,
        _status: Option<StoreDrAutoSyncStatus>,
    ) -> PdFuture<pdpb::StoreHeartbeatResponse> {
        unimplemented!();
    }

    /// Reports PD the split Region.
    fn report_batch_split(&self, _regions: Vec<metapb::Region>) -> PdFuture<()> {
        unimplemented!();
    }

    /// Scatters the Region across the cluster.
    fn scatter_region(&self, _: RegionInfo) -> Result<()> {
        unimplemented!();
    }

    /// Scatters the Regions across the cluster by Region id.
    fn scatter_regions_by_id(&self, _: Vec<u64>) -> Result<()> {
        unimplemented!();
    }

    /// Registers a handler to the client, which will be invoked after
    /// reconnecting to PD.
    ///
    /// Please note that this method should only be called once.
    fn handle_reconnect(&self, _: Box<dyn Fn() + Sync + Send + 'static>)
    where
        Self: Sized,
    {
    }

    fn get_gc_safe_point(&self) -> PdFuture<u64> {
        unimplemented!();
    }

    /// Gets store state if it is not a tombstone store asynchronously.
    fn get_store_stats_async(&self, _store_id: u64) -> BoxFuture<'_, Result<pdpb::StoreStats>> {
        unimplemented!();
    }

    /// Gets current operator of the region
    fn get_operator(&self, _region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        unimplemented!();
    }

    /// Gets a timestamp from PD.
    fn get_tso(&self) -> PdFuture<TimeStamp> {
        self.batch_get_tso(1)
    }

    /// Gets a batch of timestamps from PD.
    /// Return a timestamp with (physical, logical), indicating that timestamps
    /// allocated are: [Timestamp(physical, logical - count + 1),
    /// Timestamp(physical, logical)]
    fn batch_get_tso(&self, _count: u32) -> PdFuture<TimeStamp> {
        unimplemented!()
    }

    fn get_min_tso(&self) -> PdFuture<TimeStamp> {
        unimplemented!()
    }

    /// Set a service safe point.
    fn update_service_safe_point(
        &self,
        _name: String,
        _safepoint: TimeStamp,
        _ttl: Duration,
    ) -> PdFuture<()> {
        unimplemented!()
    }

    fn update_keyspace_service_safe_point(
        &self,
        _keyspace_id: u32,
        _name: String,
        _safepoint: TimeStamp,
        _ttl: Duration, // Set `Duration::ZERO` to remove the safe point
    ) -> PdFuture<u64 /* current_min_safe_point */> {
        unimplemented!()
    }

    fn remove_keyspace_service_safe_point(
        &self,
        keyspace_id: u32,
        name: String,
    ) -> PdFuture<u64 /* current_min_safe_point */> {
        self.update_keyspace_service_safe_point(keyspace_id, name, TimeStamp::max(), Duration::ZERO)
    }

    /// Gets the internal `FeatureGate`.
    fn feature_gate(&self) -> &FeatureGate {
        unimplemented!()
    }

    // Report min resolved_ts to PD.
    fn report_min_resolved_ts(&self, _store_id: u64, _min_resolved_ts: u64) -> PdFuture<()> {
        unimplemented!()
    }

    /// Region's Leader uses this to report buckets to PD.
    fn report_region_buckets(&self, _bucket_stat: &BucketStat, _period: Duration) -> PdFuture<()> {
        unimplemented!();
    }

    /// `retry_limit`: Used for both request retry & server side retry.
    ///
    /// Note:
    /// - PD server has backoff between retries with interval `min(1m,
    ///   pow(retry, 2) * 100ms)`.
    /// - Regions which are already split when PD receives the request will not
    ///   be returned.
    fn split_regions_opt(
        &self,
        _keys: Vec<Vec<u8>>,
        _request_timeout: Duration,
        _retry_limit: usize,
    ) -> BoxFuture<
        '_,
        Result<(
            Vec<u64>, // new_regions
            u64,      // finished_percent
        )>,
    > {
        unimplemented!();
    }

    /// tikv-worker uses this to load data.
    fn split_regions(&self, keys: Vec<Vec<u8>>) -> BoxFuture<'_, Result<Vec<u64>>> {
        let request_timeout = request_timeout_for_split_regions(keys.len());
        Box::pin(async move {
            let (regions_id, finished_percent) = self
                .split_regions_opt(keys, request_timeout, LEADER_CHANGE_RETRY)
                .await?;
            if finished_percent < 100 {
                Err(Error::SplitRegionsNotFinished {
                    regions_id: regions_id.clone(),
                    finished_percent,
                })
            } else {
                Ok(regions_id)
            }
        })
    }

    // Returns the ids of the regions that have the key as the start key.
    // If a key already exists as a start key, its region id will not be
    // returned.
    // Regions returned are not necessarily "new" regions, especially when
    // using right derive. e.g. region 1 [10 - 20), split at 15, resulting
    // in region 2 [10 - 15), region 1 [15 - 20). This function will return
    // region 1.
    fn split_regions_with_retry(
        &self,
        keys: Vec<Vec<u8>>,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<Vec<u64>> /* regions ids */> {
        use collections::HashSet;
        const RETRY_LIMIT: usize = 3;
        let request_timeout = request_timeout_for_split_regions(keys.len());
        let mut succeeded_regions: HashSet<u64> = HashSet::default();

        let timer = GLOBAL_TIMER_HANDLE.clone();
        let start = Instant::now_coarse();
        let mut retry_ms = SPLIT_REGIONS_INIT_RETRY_INTERVAL_MS;
        Box::pin(async move {
            loop {
                match self
                    .split_regions_opt(keys.clone(), request_timeout, RETRY_LIMIT)
                    .await
                {
                    Ok((regions, finished_percent)) if finished_percent >= 100 => {
                        return Ok(if succeeded_regions.is_empty() {
                            regions
                        } else {
                            succeeded_regions.extend(regions);
                            succeeded_regions.into_iter().collect()
                        });
                    }
                    res => {
                        let res = res.map(|(regions, _)| {
                            succeeded_regions.extend(regions);
                        });
                        if start.saturating_elapsed() >= timeout {
                            return match res {
                                Ok(()) => {
                                    let regions_id =
                                        succeeded_regions.into_iter().collect::<Vec<_>>();
                                    let finished_percent = regions_id.len() * 100 / keys.len();
                                    Err(Error::SplitRegionsNotFinished {
                                        regions_id,
                                        finished_percent: finished_percent as u64,
                                    })
                                }
                                Err(e) => Err(e),
                            };
                        }
                        let deadline = std::time::Instant::now() + Duration::from_millis(retry_ms);
                        timer.delay(deadline).compat().await.unwrap();
                        retry_ms =
                            std::cmp::min(SPLIT_REGIONS_MAX_RETRY_INTERVAL_MS, retry_ms << 1);
                    }
                }
            }
        })
    }

    fn split_and_scatter_regions(&self, _keys: Vec<Vec<u8>>) -> PdFuture<()> {
        unimplemented!();
    }

    fn scan_regions(
        &self,
        _key: Vec<u8>,
        _end_key: Vec<u8>,
        _limit: usize,
    ) -> PdFuture<Vec<pdpb::Region>> {
        unimplemented!();
    }

    fn set_keyspace_encryption(
        &self,
        _keyspace_id: u32,
        _cfg: KeyspaceEncryptionConfig,
    ) -> Result<()> {
        unimplemented!();
    }

    fn get_keyspace_encryption(&self, _keyspace_id: u32) -> Result<KeyspaceEncryptionConfig> {
        unimplemented!();
    }

    fn load_keyspace(&self, _keyspace_name: String) -> Result<kvproto::keyspacepb::KeyspaceMeta> {
        unimplemented!();
    }

    fn get_buckets(&self, _region_id: u64) -> Option<BucketStat> {
        unimplemented!();
    }

    fn get_buckets_async(&self, _region_id: u64) -> PdFuture<Option<BucketStat>> {
        unimplemented!();
    }
}

pub(crate) const LEADER_CHANGE_RETRY: usize = 10;
pub(crate) const REQUEST_TIMEOUT: u64 = 5; // 5s

/// Takes the peer address (for sending raft messages) from a store.
pub fn take_peer_address(store: &mut metapb::Store) -> String {
    if !store.get_peer_address().is_empty() {
        store.take_peer_address()
    } else {
        store.take_address()
    }
}

fn check_update_service_safe_point_resp(
    resp: &pdpb::UpdateServiceGcSafePointResponse,
    required_safepoint: u64,
) -> Result<()> {
    if resp.min_safe_point > required_safepoint {
        return Err(Error::UnsafeServiceGcSafePoint {
            requested: required_safepoint.into(),
            current_minimal: resp.min_safe_point.into(),
        });
    }
    Ok(())
}

fn check_update_keyspace_service_safe_point_resp(
    resp: &pdpb::UpdateServiceSafePointV2Response,
    required_safepoint: u64,
) -> Result<()> {
    if resp.min_safe_point > required_safepoint {
        return Err(Error::UnsafeServiceGcSafePoint {
            requested: required_safepoint.into(),
            current_minimal: resp.min_safe_point.into(),
        });
    }
    Ok(())
}

// It's a empirical value that split regions with 256 keys cost 10s, 39ms for
// each.
const SPLIT_REGIONS_REQUEST_TIMEOUT_PER_KEY: Duration = Duration::from_millis(50);

fn request_timeout_for_split_regions(keys_count: usize) -> Duration {
    (SPLIT_REGIONS_REQUEST_TIMEOUT_PER_KEY * keys_count as u32)
        .max(Duration::from_secs(REQUEST_TIMEOUT))
}
