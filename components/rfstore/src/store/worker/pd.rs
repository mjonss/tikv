// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering as CmpOrdering,
    collections::HashMap,
    fmt::{self, Display, Formatter},
    mem,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

use api_version::{
    ApiV2,
    api_v2::{is_one_or_multi_whole_keyspace_range, is_whole_keyspace_range},
};
use cloud_encryption::MasterKey;
use concurrency_manager::ConcurrencyManager;
#[cfg(feature = "failpoints")]
use fail::fail_point;
use futures::{FutureExt, compat::Future01CompatExt};
use kvengine::{
    CF_NAMES, GLOBAL_SHARD_END_KEY,
    context::IaCtx,
    table::{DataBound, InnerKey},
    table_id::get_table_id_from_data_bound,
};
use kvproto::{
    metapb,
    metapb::Region,
    pdpb,
    pdpb::{Peers, SyncRegionResponse},
    raft_cmdpb::{
        AdminCmdType, AdminRequest, ChangePeerRequest, ChangePeerV2Request, RaftCmdRequest,
        SplitRequest,
    },
    raft_serverpb::RaftMessage,
    replication_modepb::RegionReplicationStatus,
};
use pd_client::{BucketStat, PdClient, RegionStat, merge_bucket_stats, metrics::*};
use prometheus::local::LocalHistogram;
use raft::{StateRole, eraftpb::ConfChangeType};
use raftstore::store::{ReadStats, TxnExt, WriteStats, util, util::ConfChangeKind};
use resource_control::TransferLeaderLimiter;
use tikv_util::{
    codec::bytes::decode_bytes,
    debug, error, info,
    store::{QueryStats, find_peer},
    sys::disk::get_disks_stats,
    time::UnixSecs,
    timer::GLOBAL_TIMER_HANDLE,
    topn::TopN,
    warn,
    worker::{Runnable, Scheduler},
};
use txn_types::Key;
use yatp::Remote;

use crate::{
    RaftRouter, RaftStoreRouter,
    store::{
        Callback, CasualMessage, CpuUtilCollector, PeerMsg, PeerTag, RegionIdVer, RegionMap,
        StoreInfo, StoreMsg, encode_split_flag_encryption_keys, raw_end_key, raw_start_key,
    },
};

type RecordPairVec = Vec<pdpb::RecordPair>;

#[derive(Clone)]
pub struct FlowStatsReporter {
    scheduler: Scheduler<PdTask>,
}

impl FlowStatsReporter {
    pub fn new(scheduler: Scheduler<PdTask>) -> Self {
        Self { scheduler }
    }
}

impl raftstore::store::FlowStatsReporter for FlowStatsReporter {
    fn report_read_stats(&self, read_stats: ReadStats) {
        if let Err(e) = self.scheduler.schedule(PdTask::ReadStats { read_stats }) {
            error!("Failed to send read flow statistics"; "err" => ?e);
        }
    }

    fn report_write_stats(&self, write_stats: WriteStats) {
        if let Err(e) = self.scheduler.schedule(PdTask::WriteStats { write_stats }) {
            error!("Failed to send write flow statistics"; "err" => ?e);
        }
    }
}

pub struct HeartbeatTask {
    pub term: u64,
    pub region: metapb::Region,
    pub peer: metapb::Peer,
    pub down_peers: Vec<pdpb::PeerStats>,
    pub pending_peers: Vec<metapb::Peer>,
    pub written_bytes: u64,
    pub written_keys: u64,
    pub approximate_size: u64,
    pub approximate_keys: u64,
    pub approximate_kv_size: u64,
    pub approximate_columnar_size: u64,
    pub approximate_columnar_kv_size: u64,
    pub replication_status: Option<RegionReplicationStatus>,
    pub bucket_stat: Option<BucketStat>,
}

/// Uses an asynchronous thread to tell PD something.
pub enum PdTask {
    AskBatchSplit {
        region: metapb::Region,
        split_keys: Vec<Vec<u8>>,
        peer: metapb::Peer,
        // If true, right Region derives origin region_id.
        right_derive: bool,
        callback: Callback,
    },
    Heartbeat(HeartbeatTask),
    StoreHeartbeat {
        stats: pdpb::StoreStats,
        store_info: StoreInfo,
        send_detailed_report: bool,
    },
    ReportBatchSplit {
        regions: Vec<metapb::Region>,
    },
    ValidatePeer {
        region: metapb::Region,
        peer: metapb::Peer,
    },
    ReadStats {
        read_stats: ReadStats,
    },
    WriteStats {
        write_stats: WriteStats,
    },
    DestroyPeer {
        region_id: u64,
        keyspace_id: Option<u32>,
    },
    UpdateMaxTimestamp {
        region_id: u64,
        initial_status: u64,
        txn_ext: Arc<TxnExt>,
    },
    UpdateSafeTs,
    SyncRegion {
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
        reverse: bool,
        callback: Box<dyn FnOnce(SyncRegionResponse) + Send>,
    },
    SyncRegionById {
        region_id: u64,
        callback: Box<dyn FnOnce(SyncRegionResponse) + Send>,
    },
    RoleChanged {
        region_id: u64,
        keyspace_id: Option<u32>,
        role: StateRole,
    },
    UpdateRaftCpuUtil,
}

#[derive(Default, Clone)]
struct PeerCmpReadStat {
    pub region_id: u64,
    pub report_stat: u64,
}

impl Ord for PeerCmpReadStat {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.report_stat.cmp(&other.report_stat)
    }
}

impl Eq for PeerCmpReadStat {}

impl PartialEq for PeerCmpReadStat {
    fn eq(&self, other: &Self) -> bool {
        self.report_stat == other.report_stat
    }
}

impl PartialOrd for PeerCmpReadStat {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.report_stat.cmp(&other.report_stat))
    }
}

pub struct StoreStat {
    pub engine_total_bytes_read: u64,
    pub engine_total_keys_read: u64,
    pub engine_total_query_num: QueryStats,
    pub engine_last_total_bytes_read: u64,
    pub engine_last_total_keys_read: u64,
    pub engine_last_query_num: QueryStats,
    pub last_report_ts: UnixSecs,

    pub region_bytes_read: LocalHistogram,
    pub region_keys_read: LocalHistogram,
    pub region_bytes_written: LocalHistogram,
    pub region_keys_written: LocalHistogram,

    pub store_cpu_usages: RecordPairVec,
    pub store_read_io_rates: RecordPairVec,
    pub store_write_io_rates: RecordPairVec,
}

impl Default for StoreStat {
    fn default() -> StoreStat {
        StoreStat {
            region_bytes_read: REGION_READ_BYTES_HISTOGRAM.local(),
            region_keys_read: REGION_READ_KEYS_HISTOGRAM.local(),
            region_bytes_written: REGION_WRITTEN_BYTES_HISTOGRAM.local(),
            region_keys_written: REGION_WRITTEN_KEYS_HISTOGRAM.local(),

            last_report_ts: UnixSecs::zero(),
            engine_total_bytes_read: 0,
            engine_total_keys_read: 0,
            engine_last_total_bytes_read: 0,
            engine_last_total_keys_read: 0,
            engine_total_query_num: QueryStats::default(),
            engine_last_query_num: QueryStats::default(),

            store_cpu_usages: RecordPairVec::default(),
            store_read_io_rates: RecordPairVec::default(),
            store_write_io_rates: RecordPairVec::default(),
        }
    }
}

#[derive(Default)]
pub struct PeerStat {
    pub read_bytes: u64,
    pub read_keys: u64,
    pub query_stats: QueryStats,
    // last_region_report_attributes records the state of the last region heartbeat
    pub last_region_report_read_bytes: u64,
    pub last_region_report_read_keys: u64,
    pub last_region_report_query_stats: QueryStats,
    pub last_region_report_written_bytes: u64,
    pub last_region_report_written_keys: u64,
    pub last_region_report_ts: UnixSecs,
    // last_store_report_attributes records the state of the last store heartbeat
    pub last_store_report_read_bytes: u64,
    pub last_store_report_read_keys: u64,
    pub last_store_report_query_stats: QueryStats,
    pub approximate_keys: u64,
    pub approximate_size: u64,
    pub approximate_kv_size: u64,
    pub approximate_columnar_size: u64,
    pub approximate_columnar_kv_size: u64,
    pub role: StateRole,

    pub down_peers: Vec<pdpb::PeerStats>,
    pub pending_peers: Vec<metapb::Peer>,
}

#[derive(Default)]
pub struct ReportBucket {
    current_stat: BucketStat,
    last_report_stat: Option<BucketStat>,
    last_report_ts: UnixSecs,
}

impl ReportBucket {
    #[allow(unused)]
    fn new(current_stat: BucketStat) -> Self {
        Self {
            current_stat,
            ..Default::default()
        }
    }

    fn new_report(&mut self, report_ts: UnixSecs) -> BucketStat {
        self.last_report_ts = report_ts;
        match self.last_report_stat.replace(self.current_stat.clone()) {
            Some(last) => {
                let mut delta = BucketStat::new(
                    self.current_stat.meta.clone(),
                    pd_client::new_bucket_stats(&self.current_stat.meta),
                );
                if last.meta.version != self.current_stat.meta.version {
                    // Do not update delta if the bucket version is changed for simplicity.
                    return delta;
                }
                for i in 0..delta.meta.keys.len() - 1 {
                    delta.stats.write_bytes[i] =
                        self.current_stat.stats.write_bytes[i] - last.stats.write_bytes[i];
                    delta.stats.write_keys[i] =
                        self.current_stat.stats.write_keys[i] - last.stats.write_keys[i];
                    delta.stats.write_qps[i] =
                        self.current_stat.stats.write_qps[i] - last.stats.write_qps[i];

                    delta.stats.read_bytes[i] =
                        self.current_stat.stats.read_bytes[i] - last.stats.read_bytes[i];
                    delta.stats.read_keys[i] =
                        self.current_stat.stats.read_keys[i] - last.stats.read_keys[i];
                    delta.stats.read_qps[i] =
                        self.current_stat.stats.read_qps[i] - last.stats.read_qps[i];
                }
                delta
            }
            None => self.current_stat.clone(),
        }
    }
}

impl Display for PdTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PdTask::AskBatchSplit {
                ref region,
                ref split_keys,
                ..
            } => write!(
                f,
                "ask split region {} with {}",
                region.get_id(),
                util::KeysInfoFormatter(split_keys.iter())
            ),
            PdTask::Heartbeat(ref hb_task) => write!(
                f,
                "heartbeat for region {:?}, leader {}, replication status {:?}",
                hb_task.region,
                hb_task.peer.get_id(),
                hb_task.replication_status
            ),
            PdTask::StoreHeartbeat { ref stats, .. } => {
                write!(f, "store heartbeat stats: {:?}", stats)
            }
            PdTask::ReportBatchSplit { ref regions } => write!(f, "report split {:?}", regions),
            PdTask::ValidatePeer {
                ref region,
                ref peer,
            } => write!(f, "validate peer {:?} with region {:?}", peer, region),
            PdTask::ReadStats { ref read_stats } => {
                write!(f, "get the read statistics {:?}", read_stats)
            }
            PdTask::WriteStats { ref write_stats } => {
                write!(f, "get the write statistics {:?}", write_stats)
            }
            PdTask::DestroyPeer {
                ref region_id,
                keyspace_id,
            } => {
                write!(
                    f,
                    "destroy peer of region {} keyspace_id {:?}",
                    region_id, keyspace_id
                )
            }
            PdTask::UpdateMaxTimestamp { region_id, .. } => write!(
                f,
                "update the max timestamp for region {} in the concurrency manager",
                region_id
            ),
            PdTask::UpdateSafeTs => write!(f, "update safe ts"),
            PdTask::SyncRegion { start, end, .. } => {
                write!(
                    f,
                    "sync region, range: [{}, {})",
                    log_wrappers::Value(start),
                    log_wrappers::Value(end)
                )
            }
            PdTask::SyncRegionById { region_id, .. } => {
                write!(f, "sync region by id: {}", region_id)
            }
            PdTask::RoleChanged {
                region_id,
                keyspace_id,
                role,
            } => {
                write!(
                    f,
                    "region {} keyspace {} change role to {:?}",
                    region_id,
                    keyspace_id.unwrap_or_default(),
                    role
                )
            }
            PdTask::UpdateRaftCpuUtil => write!(f, "update raft cpu utilization"),
        }
    }
}

pub struct PdRunner {
    store_id: u64,
    cluster_id: u64,
    pd_client: Arc<dyn PdClient>,
    router: RaftRouter,
    region_peers: HashMap<u64, PeerStat>,
    region_map: RegionMap,
    region_buckets: HashMap<u64, ReportBucket>,
    store_stat: StoreStat,
    store_map: HashMap<u64, bool>,
    is_hb_receiver_scheduled: bool,
    // Records the boot time.
    start_ts: UnixSecs,

    // use for Runner inner handle function to send Task to itself
    // actually it is the sender connected to Runner's Worker which
    // calls Runner's run() on Task received.
    scheduler: Scheduler<PdTask>,

    concurrency_manager: ConcurrencyManager,
    remote: Remote<yatp::task::future::TaskCell>,
    kv: kvengine::Engine,
    raft_cpu_collector: CpuUtilCollector,
    transfer_leader_limiter: TransferLeaderLimiter,
    skip_storage_size_metrics_threshold: u64,
}

const HOTSPOT_KEY_RATE_THRESHOLD: u64 = 128;
const HOTSPOT_QUERY_RATE_THRESHOLD: u64 = 128;
const HOTSPOT_BYTE_RATE_THRESHOLD: u64 = 8 * 1024;
const HOTSPOT_REPORT_CAPACITY: usize = 1000;

// TODO: support dyamic configure threshold in future
fn hotspot_key_report_threshold() -> u64 {
    #[cfg(feature = "failpoints")]
    fail_point!("mock_hotspot_threshold", |_| { 0 });

    HOTSPOT_KEY_RATE_THRESHOLD * 10
}

fn hotspot_byte_report_threshold() -> u64 {
    #[cfg(feature = "failpoints")]
    fail_point!("mock_hotspot_threshold", |_| { 0 });

    HOTSPOT_BYTE_RATE_THRESHOLD * 10
}

fn hotspot_query_num_report_threshold() -> u64 {
    #[cfg(feature = "failpoints")]
    fail_point!("mock_hotspot_threshold", |_| { 0 });

    HOTSPOT_QUERY_RATE_THRESHOLD * 10
}

impl PdRunner {
    #[allow(unused)]
    const INTERVAL_DIVISOR: u32 = 2;

    pub fn new(
        store_id: u64,
        pd_client: Arc<dyn PdClient>,
        router: RaftRouter,
        scheduler: Scheduler<PdTask>,
        _store_heartbeat_interval: Duration,
        concurrency_manager: ConcurrencyManager,
        remote: Remote<yatp::task::future::TaskCell>,
        kv: kvengine::Engine,
        raft_cpu_collector: CpuUtilCollector,
        transfer_leader_limiter: TransferLeaderLimiter,
        skip_storage_size_metrics_threshold: u64,
    ) -> PdRunner {
        // TODO(x): support stats monitor.
        let cluster_id = pd_client.get_cluster_id().unwrap();
        PdRunner {
            store_id,
            cluster_id,
            pd_client,
            router,
            is_hb_receiver_scheduled: false,
            region_peers: HashMap::default(),
            region_map: Default::default(),
            region_buckets: HashMap::default(),
            store_stat: StoreStat::default(),
            store_map: HashMap::default(),
            start_ts: UnixSecs::now(),
            scheduler,
            concurrency_manager,
            remote,
            kv,
            raft_cpu_collector,
            transfer_leader_limiter,
            skip_storage_size_metrics_threshold,
        }
    }

    fn peer_tag(&self, region: &Region) -> PeerTag {
        let id_ver = RegionIdVer::from_region(region);
        PeerTag::new(self.store_id, id_ver)
    }

    // Note: The parameter doesn't contain `self` because this function may
    // be called in an asynchronous context.
    fn handle_ask_batch_split(
        tag: PeerTag,
        router: RaftRouter,
        _scheduler: Scheduler<PdTask>,
        pd_client: Arc<dyn PdClient>,
        mut region: metapb::Region,
        split_keys: Vec<Vec<u8>>,
        peer: metapb::Peer,
        right_derive: bool,
        callback: Callback,
        task: String,
        remote: Remote<yatp::task::future::TaskCell>,
        master_key: MasterKey,
    ) {
        if split_keys.is_empty() {
            info!("empty split key, skip ask batch split";
                "region" => tag);
            return;
        }
        let mut encryption_keys = vec![];
        for i in 0..=split_keys.len() {
            let raw_start = if i == 0 {
                raw_start_key(&region)
            } else {
                Key::from_encoded_slice(&split_keys[i - 1])
                    .into_raw()
                    .unwrap_or_default()
            };
            let raw_end = if i == split_keys.len() {
                raw_end_key(&region)
            } else {
                Key::from_encoded_slice(&split_keys[i])
                    .into_raw()
                    .unwrap_or_default()
            };
            if is_whole_keyspace_range(&raw_start, &raw_end) {
                let keyspace_id = ApiV2::get_u32_keyspace_id_by_key(&raw_start).unwrap_or_default();
                match pd_client.get_keyspace_encryption(keyspace_id) {
                    Err(e) if pd_client::grpc_error_is_unimplemented(&e) => {
                        warn!(
                            "get_keyspace_encryption is unimplemented, skip encryption";
                            "region" => tag,
                            "err" => ?e,
                        );
                    }
                    Err(e) => {
                        warn!(
                            "get keyspace encryption config failed";
                            "region" => tag,
                            "err" => ?e,
                        );
                        return;
                    }
                    Ok(cfg) => {
                        if cfg.enabled {
                            if !master_key.is_valid() {
                                error!(
                                    "keyspace encryption is enabled, but master key is invalid";
                                    "region" => tag,
                                    "keyspace_id" => keyspace_id,
                                );
                                return;
                            }
                            let encryption_key = master_key.generate_encryption_key().export();
                            info!(
                                "keyspace generate encryption key";
                                "keyspace_id" => keyspace_id,
                                "encryption_key" => ?encryption_key,
                            );
                            encryption_keys.push((keyspace_id, encryption_key));
                        }
                    }
                }
            }
        }
        let resp = pd_client.ask_batch_split(region.clone(), split_keys.len());
        let f = async move {
            match resp.await {
                Ok(mut resp) => {
                    info!(
                        "try to batch split region";
                        "region" => tag,
                        "new_region_ids" => ?resp.get_ids(),
                        "region" => ?region,
                        "task" => task,
                    );

                    let admin_req = new_batch_split_region_request(
                        split_keys,
                        resp.take_ids().into(),
                        right_derive,
                    );
                    let region_id = region.get_id();
                    let epoch = region.take_region_epoch();
                    let mut req = new_admin_command(region_id, epoch, peer, admin_req);
                    if !encryption_keys.is_empty() {
                        let header = req.mut_header();
                        header.set_flag_data(encode_split_flag_encryption_keys(encryption_keys));
                    }
                    router.send_command(req, callback);
                }
                Err(e) => {
                    warn!(
                        "ask batch split failed";
                        "region" => tag,
                        "err" => ?e,
                    );
                }
            }
        };
        remote.spawn(f);
    }

    fn handle_heartbeat(
        &mut self,
        term: u64,
        region: metapb::Region,
        peer: metapb::Peer,
        region_stat: RegionStat,
        replication_status: Option<RegionReplicationStatus>,
        skip_storage_size_metric: bool,
    ) {
        self.store_stat
            .region_bytes_written
            .observe(region_stat.written_bytes as f64);
        self.store_stat
            .region_keys_written
            .observe(region_stat.written_keys as f64);
        self.store_stat
            .region_bytes_read
            .observe(region_stat.read_bytes as f64);
        self.store_stat
            .region_keys_read
            .observe(region_stat.read_keys as f64);

        for p in region.get_peers() {
            let store_id = p.get_store_id();
            if let std::collections::hash_map::Entry::Vacant(entry) = self.store_map.entry(store_id)
            {
                let resp = self.pd_client.get_store(store_id);
                match resp {
                    Ok(store) => {
                        let is_tiflash = store.get_labels().iter().any(|label| {
                            label.get_key() == "engine" && label.get_value() == "tiflash"
                        });
                        entry.insert(is_tiflash);
                    }
                    Err(e) => {
                        error!("failed to get store from pd"; "store_id" => store_id, "err" => ?e);
                    }
                }
            }
        }

        let has_tiflash_replicas = region.get_peers().iter().any(|p| {
            let store_id = p.get_store_id();
            self.store_map
                .get(&store_id)
                .is_some_and(|is_tiflash| *is_tiflash)
        });

        // TODO: remove this after columnar kv size freshed.
        let columnar_one_table = if let (Ok(lower_bound), Ok(upper_bound)) = (
            decode_bytes(&mut region.get_start_key(), false),
            decode_bytes(&mut region.get_end_key(), false),
        ) {
            let data_bound = DataBound::new(
                InnerKey::from_outer_key(&lower_bound),
                InnerKey::from_outer_end_key(&upper_bound),
                false,
            );
            let (min_table_id, max_table_id) = get_table_id_from_data_bound(data_bound);
            min_table_id == max_table_id && region_stat.approximate_columnar_size > 0
        } else {
            false
        };
        // If there has tiflash replicas, use the kv size as the columnar kv size.
        let columnar_kv_size = if has_tiflash_replicas || columnar_one_table {
            Some(region_stat.approximate_kv_size)
        // TODO: uncomment after columnar kv size freshed.
        // } else if region_stat.approximate_columnar_kv_size > 0 {
        //     Some(region_stat.approximate_columnar_kv_size)
        } else if region_stat.approximate_columnar_size > 0 {
            Some(region_stat.approximate_columnar_size)
        } else {
            None
        };
        let keyspace_id = ApiV2::get_u32_keyspace_id_by_key(region.get_start_key());
        if !skip_storage_size_metric {
            PdRunner::set_storage_size_metric(
                region.id,
                keyspace_id,
                Some(region_stat.approximate_kv_size),
                columnar_kv_size,
            );
        } else {
            PdRunner::set_storage_size_metric(region.id, keyspace_id, None, None);
        }

        let changed = self
            .region_map
            .get(region.id)
            .map(|old| old.get_region_epoch() != region.get_region_epoch())
            .unwrap_or(true);
        if changed {
            self.region_map.put(region.clone())
        }

        STORE_ENGINE_FLOW_VEC
            .with_label_values(&["kv", "bytes_read"])
            .inc_by(region_stat.read_bytes);
        STORE_ENGINE_FLOW_VEC
            .with_label_values(&["kv", "keys_read"])
            .inc_by(region_stat.read_keys);
        STORE_ENGINE_FLOW_VEC
            .with_label_values(&["kv", "bytes_written"])
            .inc_by(region_stat.written_bytes);
        STORE_ENGINE_FLOW_VEC
            .with_label_values(&["kv", "keys_written"])
            .inc_by(region_stat.written_keys);
        STORE_ENGINE_FLOW_VEC
            .with_label_values(&["kv", "wal_file_bytes"])
            .inc_by(region_stat.written_bytes);

        let resp = self.pd_client.region_heartbeat(
            term,
            region.clone(),
            peer,
            region_stat,
            replication_status,
        );
        let f = async move {
            if let Err(e) = resp.await {
                debug!(
                    "failed to send heartbeat";
                    "region_id" => region.get_id(),
                    "err" => ?e
                );
            }
        };
        self.remote.spawn(f);
    }

    pub fn set_storage_size_metric(
        region_id: u64,
        keyspace_id: Option<u32>,
        kv_size: Option<u64>,
        columnar_kv_size: Option<u64>,
    ) {
        let region_id_string = region_id.to_string();
        let region_id_str = region_id_string.as_str();
        match keyspace_id {
            None => {}
            Some(keyspace_id) => {
                let keyspace_id_string = keyspace_id.to_string();
                let keyspace_id_str = keyspace_id_string.as_str();
                match kv_size {
                    None => {
                        let _ = STORE_SIZE_GAUGE_VEC.remove_label_values(&[
                            "used",
                            region_id_str,
                            keyspace_id_str,
                        ]);
                    }
                    Some(size) => {
                        debug!(
                            "update STORE_SIZE_GAUGE_VEC";
                            "region_id_str" => region_id_str,
                            "keyspace_id_str" => keyspace_id_str,
                            "kv_size"=>size,
                        );
                        STORE_SIZE_GAUGE_VEC
                            .with_label_values(&["used", region_id_str, keyspace_id_str])
                            .set(size as i64);
                    }
                }
                match columnar_kv_size {
                    None => {
                        if STORE_SIZE_GAUGE_VEC
                            .get_metric_with_label_values(&[
                                "tiflash_used",
                                region_id_str,
                                keyspace_id_str,
                            ])
                            .is_ok()
                        {
                            let _ = STORE_SIZE_GAUGE_VEC.remove_label_values(&[
                                "tiflash_used",
                                region_id_str,
                                keyspace_id_str,
                            ]);
                        }
                    }
                    Some(size) => {
                        debug!(
                            "update STORE_SIZE_GAUGE_VEC";
                            "region_id_str" => region_id_str,
                            "keyspace_id_str" => keyspace_id_str,
                            "columnar_kv_size"=> size,
                        );
                        STORE_SIZE_GAUGE_VEC
                            .with_label_values(&["tiflash_used", region_id_str, keyspace_id_str])
                            .set(size as i64);
                    }
                }
            }
        }
    }

    fn handle_store_heartbeat(
        &mut self,
        mut stats: pdpb::StoreStats,
        store_info: StoreInfo,
        _send_detailed_report: bool,
    ) {
        let (total_space, available_space) =
            match get_disks_stats(&store_info.kv_engine.opts.local_dirs) {
                Err(e) => {
                    error!(
                        "get disk stat for rocksdb failed";
                        "err" => ?e
                    );
                    return;
                }
                Ok(stats) => stats,
            };

        let mut report_peers = HashMap::default();
        for (region_id, region_peer) in &mut self.region_peers {
            let read_bytes = region_peer.read_bytes - region_peer.last_store_report_read_bytes;
            let read_keys = region_peer.read_keys - region_peer.last_store_report_read_keys;
            let query_stats = region_peer
                .query_stats
                .sub_query_stats(&region_peer.last_store_report_query_stats);
            region_peer.last_store_report_read_bytes = region_peer.read_bytes;
            region_peer.last_store_report_read_keys = region_peer.read_keys;
            region_peer
                .last_store_report_query_stats
                .fill_query_stats(&region_peer.query_stats);
            if read_bytes < hotspot_byte_report_threshold()
                && read_keys < hotspot_key_report_threshold()
                && query_stats.get_read_query_num() < hotspot_query_num_report_threshold()
            {
                continue;
            }
            let mut read_stat = pdpb::PeerStat::default();
            read_stat.set_region_id(*region_id);
            read_stat.set_read_keys(read_keys);
            read_stat.set_read_bytes(read_bytes);
            read_stat.set_query_stats(query_stats.0);
            report_peers.insert(*region_id, read_stat);
        }

        stats = collect_report_read_peer_stats(HOTSPOT_REPORT_CAPACITY, report_peers, stats);

        let disk_cap = total_space;
        let capacity = if store_info.capacity == 0 || disk_cap < store_info.capacity {
            disk_cap
        } else {
            store_info.capacity
        };
        stats.set_capacity(capacity);
        let used_size =
            store_info.kv_engine.size() + store_info.rf_engine.get_engine_stats().disk_size;
        stats.set_used_size(used_size);

        // Note: `available + used_size` may be larger than `capacity`, when some
        // regions are sharing the same table files.
        let available = available_space;
        store_info.kv_engine.set_available_space(available);

        if available == 0 {
            warn!("no available space");
        }

        stats.set_available(available);
        stats.set_bytes_read(
            self.store_stat.engine_total_bytes_read - self.store_stat.engine_last_total_bytes_read,
        );
        stats.set_keys_read(
            self.store_stat.engine_total_keys_read - self.store_stat.engine_last_total_keys_read,
        );

        self.store_stat
            .engine_total_query_num
            .add_query_stats(stats.get_query_stats()); // add write query stat
        let res = self
            .store_stat
            .engine_total_query_num
            .sub_query_stats(&self.store_stat.engine_last_query_num);
        stats.set_query_stats(res.0);

        stats.set_cpu_usages(self.store_stat.store_cpu_usages.clone().into());
        stats.set_read_io_rates(self.store_stat.store_read_io_rates.clone().into());
        stats.set_write_io_rates(self.store_stat.store_write_io_rates.clone().into());

        let mut interval = pdpb::TimeInterval::default();
        interval.set_start_timestamp(self.store_stat.last_report_ts.into_inner());
        stats.set_interval(interval);
        self.store_stat.engine_last_total_bytes_read = self.store_stat.engine_total_bytes_read;
        self.store_stat.engine_last_total_keys_read = self.store_stat.engine_total_keys_read;
        self.store_stat
            .engine_last_query_num
            .fill_query_stats(&self.store_stat.engine_total_query_num);
        self.store_stat.last_report_ts = UnixSecs::now();
        self.store_stat.region_bytes_written.flush();
        self.store_stat.region_keys_written.flush();
        self.store_stat.region_bytes_read.flush();
        self.store_stat.region_keys_read.flush();

        STORE_SIZE_GAUGE_VEC
            .with_label_values(&["capacity", "", ""])
            .set(capacity as i64);
        STORE_SIZE_GAUGE_VEC
            .with_label_values(&["available", "", ""])
            .set(available as i64);
        STORE_SIZE_GAUGE_VEC
            .with_label_values(&["all_used", "", ""])
            .set(used_size as i64);

        let kv_all_shard_stats = store_info.kv_engine.get_all_shard_stats();
        kvengine::Engine::update_region_huge_table_bytes_metrics(
            &kv_all_shard_stats,
            store_info.kv_engine.opts.max_mem_table_size,
        );
        let kv_engine_stats = store_info.kv_engine.get_engine_stats(kv_all_shard_stats);

        store_info
            .kv_engine
            .notify_memtables_size(kv_engine_stats.mem_tables_size);
        if let IaCtx::Enabled(mgr, _) = store_info.kv_engine.ia_ctx() {
            mgr.notify_total_data_size(kv_engine_stats.ia.data_size);
        }
        for &id_ver in &kv_engine_stats.ready_destroy_range_shards {
            if let Ok(shard) = store_info
                .kv_engine
                .get_shard_with_ver(id_ver.id, id_ver.ver)
            {
                // If the shard is not active, no need to trigger compact.
                if !shard.is_active() {
                    continue;
                }
                // Send trigger refresh states to peer.
                self.router.send(
                    shard.id,
                    PeerMsg::CasualMessage(CasualMessage::TriggerRefreshShardStates),
                );
            }
        }

        for cf in 0..kv_engine_stats.cf_total_sizes.len() {
            STORE_ENGINE_SIZE_GAUGE_VEC
                .with_label_values(&["kv", CF_NAMES[cf]])
                .set(kv_engine_stats.cf_total_sizes[cf] as i64);
        }
        STORE_ENGINE_MEM_SIZE_GAUGE_VEC
            .with_label_values(&["kv", "memtable"])
            .set(kv_engine_stats.mem_tables_size as i64);
        STORE_ENGINE_MEM_SIZE_GAUGE_VEC
            .with_label_values(&["kv", "block_cache"])
            .set(store_info.kv_engine.get_cache_size() as i64);
        STORE_ENGINE_MEM_SIZE_GAUGE_VEC
            .with_label_values(&["kv", "block_index"])
            .set(kv_engine_stats.in_mem_index_size as i64);
        STORE_ENGINE_MEM_SIZE_GAUGE_VEC
            .with_label_values(&["kv", "table_filter"])
            .set(kv_engine_stats.in_mem_filter_size as i64);

        STORE_ENGINE_SIZE_GAUGE_VEC
            .with_label_values(&["kv", "ia"])
            .set(kv_engine_stats.ia.data_size as i64);
        STORE_ENGINE_SIZE_GAUGE_VEC
            .with_label_values(&["kv", "ia_kv"])
            .set(kv_engine_stats.ia.kv_size as i64);

        let rf_engine_stats = store_info.rf_engine.get_engine_stats();
        STORE_ENGINE_SIZE_GAUGE_VEC
            .with_label_values(&["raft", "raft"])
            .set(rf_engine_stats.disk_size as i64);
        STORE_ENGINE_MEM_SIZE_GAUGE_VEC
            .with_label_values(&["raft", ""])
            .set(rf_engine_stats.total_mem_size as i64);

        // TODO(x): set slow score

        let optional_report = None;
        let resp = self.pd_client.store_heartbeat(stats, optional_report, None);
        let f = async move {
            match resp.await {
                Ok(_resp) => {
                    // TODO(x): UpdateReplicationMode
                    // TODO(x): support recovery plan.
                }
                Err(e) => {
                    error!("store heartbeat failed"; "err" => ?e);
                }
            }
        };
        self.remote.spawn(f);
    }

    fn handle_report_batch_split(&self, regions: Vec<metapb::Region>) {
        let resp = self.pd_client.report_batch_split(regions);
        let f = async move {
            if let Err(e) = resp.await {
                warn!("report split failed"; "err" => ?e);
            }
        };
        self.remote.spawn(f);
    }

    fn handle_validate_peer(&self, local_region: metapb::Region, peer: metapb::Peer) {
        let router = self.router.clone();
        let resp = self.pd_client.get_region_by_id(local_region.get_id());
        let tag = self.peer_tag(&local_region);
        let f = async move {
            match resp.await {
                Ok(Some(pd_region)) => {
                    if util::is_epoch_stale(
                        pd_region.get_region_epoch(),
                        local_region.get_region_epoch(),
                    ) {
                        // The local Region epoch is fresher than Region epoch in PD
                        // This means the Region info in PD is not updated to the latest even
                        // after `max_leader_missing_duration`. Something is wrong in the system.
                        // Just add a log here for this situation.
                        info!(
                            "local region epoch is greater the \
                             region epoch in PD ignore validate peer";
                            "region" => tag,
                            "peer_id" => peer.get_id(),
                            "local_region_epoch" => ?local_region.get_region_epoch(),
                            "pd_region_epoch" => ?pd_region.get_region_epoch()
                        );
                        PD_VALIDATE_PEER_COUNTER_VEC
                            .with_label_values(&["region epoch error"])
                            .inc();
                        return;
                    }

                    if pd_region
                        .get_peers()
                        .iter()
                        .all(|p| p.get_id() != peer.get_id())
                    {
                        // Peer is not a member of this Region anymore. Probably it's removed out.
                        // Send it a raft massage to destroy it since it's obsolete.
                        info!(
                            "peer is not a valid member of region, to be \
                             destroyed soon";
                            "region" => tag,
                            "peer_id" => peer.get_id(),
                            "pd_region" => ?pd_region
                        );
                        PD_VALIDATE_PEER_COUNTER_VEC
                            .with_label_values(&["peer stale"])
                            .inc();
                        send_destroy_peer_message(&router, local_region, peer, pd_region);
                    } else {
                        info!(
                            "peer is still a valid member of region";
                            "region" => tag,
                            "peer_id" => peer.get_id(),
                            "pd_region" => ?pd_region
                        );
                        PD_VALIDATE_PEER_COUNTER_VEC
                            .with_label_values(&["peer valid"])
                            .inc();
                    }
                }
                Ok(None) => {
                    // splitted Region has not yet reported to PD.
                    // TODO: handle merge
                }
                Err(e) => {
                    error!("{} get region failed", tag; "err" => ?e);
                }
            }
        };
        self.remote.spawn(f);
    }

    fn schedule_heartbeat_receiver(&mut self) {
        let router = self.router.clone();
        let store_id = self.store_id;
        let transfer_leader_limiter = self.transfer_leader_limiter.clone();

        let fut = self.pd_client
            .handle_region_heartbeat_response(store_id, Box::new(move |mut resp: pdpb::RegionHeartbeatResponse| {
                let region_id = resp.get_region_id();
                let epoch = resp.take_region_epoch();
                let peer = resp.take_target_peer();
                let tag = PeerTag::new(store_id, RegionIdVer::new(region_id, epoch.version));

                if resp.has_change_peer() {
                    PD_HEARTBEAT_COUNTER_VEC
                        .with_label_values(&["change peer"])
                        .inc();

                    let mut change_peer = resp.take_change_peer();
                    info!(
                        "try to change peer";
                        "region" => tag,
                        "change_type" => ?change_peer.get_change_type(),
                        "peer" => ?change_peer.get_peer()
                    );
                    let req = new_change_peer_request(
                        change_peer.get_change_type(),
                        change_peer.take_peer(),
                    );
                    send_admin_request(&router, region_id, epoch, peer, req, Callback::None);
                } else if resp.has_change_peer_v2() {
                    PD_HEARTBEAT_COUNTER_VEC
                        .with_label_values(&["change peer"])
                        .inc();

                    let mut change_peer_v2 = resp.take_change_peer_v2();
                    info!(
                        "try to change peer";
                        "region" => tag,
                        "changes" => ?change_peer_v2.get_changes(),
                        "kind" => ?ConfChangeKind::confchange_kind(change_peer_v2.get_changes().len()),
                    );
                    let req = new_change_peer_v2_request(change_peer_v2.take_changes().into());
                    send_admin_request(&router, region_id, epoch, peer, req, Callback::None);
                } else if resp.has_transfer_leader() {
                    let mut transfer_leader = resp.take_transfer_leader();
                    if !transfer_leader_limiter.allow(){
                        let rate = transfer_leader_limiter.rate();
                        info!(
                            "reject transfer leader";
                            "region" => tag,
                            "from_peer" => ?peer,
                            "to_peer" => ?transfer_leader.get_peer(),
                            "rate" => rate,
                        );
                        return;
                    }
                    PD_HEARTBEAT_COUNTER_VEC
                        .with_label_values(&["transfer leader"])
                        .inc();

                    info!(
                        "try to transfer leader";
                        "region" => tag,
                        "from_peer" => ?peer,
                        "to_peer" => ?transfer_leader.get_peer()
                    );
                    let req = new_transfer_leader_request(transfer_leader.take_peer());
                    send_admin_request(&router, region_id, epoch, peer, req, Callback::None);
                } else if resp.has_split_region() {
                    PD_HEARTBEAT_COUNTER_VEC
                        .with_label_values(&["split region"])
                        .inc();

                    let mut split_region = resp.take_split_region();
                    info!("try to split"; "region" => tag, "region_epoch" => ?epoch);
                    let msg = if split_region.get_policy() == pdpb::CheckPolicy::Usekey {
                        CasualMessage::SplitRegion {
                            region_epoch: epoch,
                            split_keys: split_region.take_keys().into(),
                            callback: Callback::None,
                            source: "pd".into(),
                        }
                    } else {
                        CasualMessage::HalfSplitRegion {
                            region_epoch: epoch,
                            policy: split_region.get_policy(),
                            source: "pd",
                        }
                    };
                    router.send(region_id, PeerMsg::CasualMessage(msg));
                } else if resp.has_merge() {
                    PD_HEARTBEAT_COUNTER_VEC.with_label_values(&["merge"]).inc();

                    let merge = resp.take_merge();
                    info!("try to merge"; "region" => tag, "merge" => ?merge);
                    let request = new_merge_request(merge);
                    let req = new_admin_command(region_id, epoch, peer, request);
                    router.send_store(StoreMsg::PrepareMerge {region_id, req});
                } else {
                    PD_HEARTBEAT_COUNTER_VEC.with_label_values(&["noop"]).inc();
                }
            }));
        let f = async move {
            match fut.await {
                Ok(_) => {
                    info!(
                        "region heartbeat response handler exit";
                        "store_id" => store_id,
                    );
                }
                Err(e) => panic!("unexpected error: {:?}", e),
            }
        };
        self.remote.spawn(f);
        self.is_hb_receiver_scheduled = true;
    }

    fn handle_read_stats(&mut self, mut read_stats: ReadStats) {
        for (region_id, region_info) in read_stats.region_infos.iter_mut() {
            let peer_stat = self.region_peers.entry(*region_id).or_default();
            peer_stat.read_bytes += region_info.flow.read_bytes as u64;
            peer_stat.read_keys += region_info.flow.read_keys as u64;
            self.store_stat.engine_total_bytes_read += region_info.flow.read_bytes as u64;
            self.store_stat.engine_total_keys_read += region_info.flow.read_keys as u64;
            peer_stat
                .query_stats
                .add_query_stats(&region_info.query_stats.0);
            self.store_stat
                .engine_total_query_num
                .add_query_stats(&region_info.query_stats.0);
        }
        for (_, region_buckets) in mem::take(&mut read_stats.region_buckets) {
            self.merge_buckets(region_buckets);
        }
        if !read_stats.region_infos.is_empty() {
            // TODO(x) send stats
            // if let Some(sender) = self.stats_monitor.get_sender() {
            // if sender.send(read_stats).is_err() {
            // warn!("send read_stats failed, are we shutting down?")
            // }
            // }
        }
    }

    fn handle_write_stats(&mut self, mut write_stats: WriteStats) {
        for (region_id, region_info) in write_stats.region_infos.iter_mut() {
            let peer_stat = self.region_peers.entry(*region_id).or_default();
            peer_stat.query_stats.add_query_stats(&region_info.0);
            self.store_stat
                .engine_total_query_num
                .add_query_stats(&region_info.0);
        }
    }

    fn handle_destroy_peer(&mut self, region_id: u64, keyspace_id: Option<u32>) {
        self.region_map.remove(region_id);
        self.region_buckets.remove(&region_id);
        match self.region_peers.remove(&region_id) {
            None => {}
            Some(_) => {
                let tag = PeerTag::new(self.store_id, RegionIdVer::new(region_id, 0));
                info!("remove peer statistic record in pd"; "region" => tag)
            }
        }
        Self::set_storage_size_metric(region_id, keyspace_id, None, None);
    }

    #[allow(unused)]
    fn handle_store_infos(
        &mut self,
        cpu_usages: RecordPairVec,
        read_io_rates: RecordPairVec,
        write_io_rates: RecordPairVec,
    ) {
        self.store_stat.store_cpu_usages = cpu_usages;
        self.store_stat.store_read_io_rates = read_io_rates;
        self.store_stat.store_write_io_rates = write_io_rates;
    }

    fn handle_update_max_timestamp(
        &mut self,
        region_id: u64,
        initial_status: u64,
        txn_ext: Arc<TxnExt>,
    ) {
        let pd_client = self.pd_client.clone();
        let concurrency_manager = self.concurrency_manager.clone();
        let tag = PeerTag::new(self.store_id, RegionIdVer::new(region_id, 0));
        let f = async move {
            let mut success = false;
            while txn_ext.max_ts_sync_status.load(Ordering::SeqCst) == initial_status {
                match pd_client.get_tso().await {
                    Ok(ts) => {
                        concurrency_manager.update_max_ts(ts);
                        // Set the least significant bit to 1 to mark it as synced.
                        success = txn_ext
                            .max_ts_sync_status
                            .compare_exchange(
                                initial_status,
                                initial_status | 1,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                            .is_ok();
                        break;
                    }
                    Err(e) => {
                        warn!("failed to update max timestamp for region {}: {:?}", tag, e);
                        let _ = GLOBAL_TIMER_HANDLE
                            .delay(Instant::now() + Duration::from_secs(1))
                            .compat()
                            .await;
                    }
                }
            }
            if success {
                info!("succeed to update max timestamp"; "region" => tag);
            } else {
                info!(
                    "updating max timestamp is stale";
                    "region" => tag,
                    "initial_status" => initial_status,
                );
            }
        };

        #[cfg(feature = "failpoints")]
        let delay = (|| {
            fail_point!("delay_update_max_ts", |_| true);
            false
        })();
        #[cfg(not(feature = "failpoints"))]
        let delay = false;

        if delay {
            info!("[failpoint] delay update max ts for 1s"; "region" => tag);
            let deadline = Instant::now() + Duration::from_secs(1);
            self.remote
                .spawn(GLOBAL_TIMER_HANDLE.delay(deadline).compat().then(|_| f));
        } else {
            self.remote.spawn(f);
        }
    }

    fn handle_update_safe_ts(&mut self) {
        let pd_client = self.pd_client.clone();
        let kv = self.kv.clone();
        let f = async move {
            match pd_client.get_gc_safe_point().await {
                Ok(ts) => {
                    kv.update_managed_safe_ts(ts);
                    raftstore::store::metrics::AUTO_GC_SAFE_POINT_GAUGE.set(ts as i64);
                    info!("update safe ts {}", ts);
                }
                Err(err) => {
                    warn!("failed to update safe ts {:?}", err);
                }
            }
        };
        self.remote.spawn(f);
    }

    fn handle_report_region_buckets(&mut self, mut region_buckets: BucketStat) {
        let store_id = self.store_id;
        let region_id = region_buckets.meta.region_id;
        region_buckets.prepare_report();
        self.merge_buckets(region_buckets);
        let report_buckets = self.region_buckets.get_mut(&region_id).unwrap();
        let last_report_ts = if report_buckets.last_report_ts.is_zero() {
            self.start_ts
        } else {
            report_buckets.last_report_ts
        };
        let now = UnixSecs::now();
        let interval_second = now.into_inner() - last_report_ts.into_inner();
        let delta = report_buckets.new_report(now);
        let resp = self
            .pd_client
            .report_region_buckets(&delta, Duration::from_secs(interval_second));
        let f = async move {
            let tag = || {
                PeerTag::new(
                    store_id,
                    RegionIdVer::new(region_id, delta.meta.region_epoch.version),
                )
            };
            if let Err(e) = resp.await {
                debug!(
                    "{} failed to send buckets", tag();
                    "region_id" => region_id,
                    "version" => delta.meta.version,
                    "region_epoch" => ?delta.meta.region_epoch,
                    "err" => ?e
                );
            } else {
                debug!("{} report_region_buckets", tag();
                    "version" => delta.meta.version,
                    "count" => delta.count(),
                );
            }
        };
        self.remote.spawn(f);
    }

    fn merge_buckets(&mut self, mut buckets: BucketStat) {
        let region_id = buckets.meta.region_id;
        self.region_buckets
            .entry(region_id)
            .and_modify(|report_bucket| {
                let current = &mut report_bucket.current_stat;
                if current.meta < buckets.meta {
                    mem::swap(current, &mut buckets);
                }
                merge_bucket_stats(
                    &current.meta.keys,
                    &mut current.stats,
                    &buckets.meta.keys,
                    &buckets.stats,
                );
            })
            .or_insert_with(|| ReportBucket::new(buckets));
    }

    fn handle_sync_region(
        &self,
        start: Vec<u8>,
        mut end: Vec<u8>,
        limit: usize,
        reverse: bool,
        callback: Box<dyn FnOnce(SyncRegionResponse) + Send>,
    ) {
        if end.is_empty() {
            end.extend_from_slice(GLOBAL_SHARD_END_KEY);
        }
        let regions = self.region_map.scan_regions(start, end, limit, reverse);
        let resp = self.make_sync_region_resp(regions);
        callback(resp);
    }

    fn handle_sync_region_by_id(
        &self,
        region_id: u64,
        callback: Box<dyn FnOnce(SyncRegionResponse) + Send>,
    ) {
        let region = self.region_map.regions.get(&region_id);
        let resp = self.make_sync_region_resp(region.into_iter().collect());
        callback(resp);
    }

    fn make_sync_region_resp(&self, regions: Vec<&Region>) -> SyncRegionResponse {
        let mut resp_regions = Vec::with_capacity(regions.len());
        let mut resp_stats = Vec::with_capacity(regions.len());
        let mut resp_leaders = Vec::with_capacity(regions.len());
        let mut resp_buckets = Vec::with_capacity(regions.len());
        let mut resp_down_peers: Vec<pdpb::PeersStats> = Vec::with_capacity(regions.len());
        let mut resp_pending_peers: Vec<Peers> = Vec::with_capacity(regions.len());
        for region in regions {
            resp_regions.push(region.clone());
            // The stats is used along with region, we need to push a default one if not
            // found.
            let mut region_stat = pdpb::RegionStat::new();
            let mut down_peers = pdpb::PeersStats::new();
            let mut pending_peers = Peers::new();
            if let Some(stats) = self.region_peers.get(&region.id) {
                region_stat.set_bytes_read(stats.read_bytes);
                region_stat.set_keys_read(stats.read_keys);
                region_stat.set_bytes_written(stats.last_region_report_written_bytes);
                region_stat.set_keys_written(stats.last_region_report_written_keys);
                down_peers.peers = stats.down_peers.clone().into();
                pending_peers.peers = stats.pending_peers.clone().into();
            }
            resp_stats.push(region_stat);
            resp_down_peers.push(down_peers);
            resp_pending_peers.push(pending_peers);
            let leader_peer = find_peer(region, self.store_id)
                .cloned()
                .unwrap_or_default();
            resp_leaders.push(leader_peer);
            if !self.region_buckets.is_empty() {
                // If there is any bucket, then all regions must push a bucket even if it's not
                // reported yet.
                let mut bucket = metapb::Buckets::new();
                if let Some(report_bucket) = self.region_buckets.get(&region.id) {
                    bucket.set_region_id(region.id);
                    bucket.set_version(report_bucket.current_stat.meta.version);
                    bucket.set_keys(report_bucket.current_stat.meta.keys.clone().into());
                    bucket.set_stats(report_bucket.current_stat.stats.clone());
                }
                resp_buckets.push(bucket);
            }
        }
        let mut resp = SyncRegionResponse::new();
        resp.mut_header().set_cluster_id(self.cluster_id);
        resp.set_regions(resp_regions.into());
        resp.set_region_leaders(resp_leaders.into());
        resp.set_region_stats(resp_stats.into());
        resp.set_buckets(resp_buckets.into());
        resp.set_down_peers(resp_down_peers.into());
        resp.set_pending_peers(resp_pending_peers.into());
        resp
    }

    fn handle_role_changed(&mut self, region_id: u64, keyspace_id: Option<u32>, role: StateRole) {
        let peer_stat = self.region_peers.entry(region_id).or_default();
        peer_stat.role = role;
        if role != StateRole::Leader {
            peer_stat.down_peers.clear();
            peer_stat.pending_peers.clear();
            Self::set_storage_size_metric(region_id, keyspace_id, None, None)
        }
    }
}

impl Runnable for PdRunner {
    type Task = PdTask;

    fn run(&mut self, task: PdTask) {
        debug!("executing task"; "task" => %task);

        if !self.is_hb_receiver_scheduled {
            self.schedule_heartbeat_receiver();
        }

        match task {
            PdTask::AskBatchSplit {
                region,
                split_keys,
                peer,
                right_derive,
                callback,
            } => Self::handle_ask_batch_split(
                self.peer_tag(&region),
                self.router.clone(),
                self.scheduler.clone(),
                self.pd_client.clone(),
                region,
                split_keys,
                peer,
                right_derive,
                callback,
                String::from("batch_split"),
                self.remote.clone(),
                self.kv.get_master_key(),
            ),

            PdTask::Heartbeat(hb_task) => {
                tikv_util::set_current_region(hb_task.region.id);
                let (
                    read_bytes_delta,
                    read_keys_delta,
                    written_bytes_delta,
                    written_keys_delta,
                    last_report_ts,
                    query_stats,
                ) = {
                    let peer_stat = self
                        .region_peers
                        .entry(hb_task.region.get_id())
                        .or_default();
                    peer_stat.approximate_size = hb_task.approximate_size;
                    peer_stat.approximate_keys = hb_task.approximate_keys;
                    peer_stat.approximate_kv_size = hb_task.approximate_kv_size;
                    peer_stat.approximate_columnar_size = hb_task.approximate_columnar_size;
                    peer_stat.approximate_columnar_kv_size = hb_task.approximate_columnar_kv_size;

                    let read_bytes_delta =
                        peer_stat.read_bytes - peer_stat.last_region_report_read_bytes;
                    let read_keys_delta =
                        peer_stat.read_keys - peer_stat.last_region_report_read_keys;
                    let written_bytes_delta =
                        hb_task.written_bytes - peer_stat.last_region_report_written_bytes;
                    let written_keys_delta =
                        hb_task.written_keys - peer_stat.last_region_report_written_keys;
                    let query_stats = peer_stat
                        .query_stats
                        .sub_query_stats(&peer_stat.last_region_report_query_stats);
                    let mut last_report_ts = peer_stat.last_region_report_ts;
                    peer_stat.last_region_report_written_bytes = hb_task.written_bytes;
                    peer_stat.last_region_report_written_keys = hb_task.written_keys;
                    peer_stat.last_region_report_read_bytes = peer_stat.read_bytes;
                    peer_stat.last_region_report_read_keys = peer_stat.read_keys;
                    peer_stat.last_region_report_query_stats = peer_stat.query_stats.clone();
                    let unix_secs_now = UnixSecs::now();
                    peer_stat.last_region_report_ts = unix_secs_now;

                    peer_stat.down_peers = hb_task.down_peers.clone();
                    peer_stat.pending_peers = hb_task.pending_peers.clone();

                    if last_report_ts.is_zero() {
                        last_report_ts = self.start_ts;
                    }
                    (
                        read_bytes_delta,
                        read_keys_delta,
                        written_bytes_delta,
                        written_keys_delta,
                        last_report_ts,
                        query_stats.0,
                    )
                };
                let skip_storage_size_metric =
                    if hb_task.approximate_kv_size < self.skip_storage_size_metrics_threshold {
                        let raw_start = decode_bytes(&mut hb_task.region.get_start_key(), false)
                            .unwrap_or_default();
                        let raw_end = decode_bytes(&mut hb_task.region.get_end_key(), false)
                            .unwrap_or_default();
                        is_one_or_multi_whole_keyspace_range(&raw_start, &raw_end)
                    } else {
                        false
                    };
                self.handle_heartbeat(
                    hb_task.term,
                    hb_task.region,
                    hb_task.peer,
                    RegionStat {
                        down_peers: hb_task.down_peers,
                        pending_peers: hb_task.pending_peers,
                        written_bytes: written_bytes_delta,
                        written_keys: written_keys_delta,
                        read_bytes: read_bytes_delta,
                        read_keys: read_keys_delta,
                        query_stats,
                        approximate_size: hb_task.approximate_size,
                        approximate_keys: hb_task.approximate_keys,
                        approximate_kv_size: hb_task.approximate_kv_size,
                        approximate_columnar_size: hb_task.approximate_columnar_size,
                        approximate_columnar_kv_size: hb_task.approximate_columnar_kv_size,
                        last_report_ts,
                        cpu_usage: 0,
                    },
                    hb_task.replication_status,
                    skip_storage_size_metric,
                );
                if let Some(bucket_stat) = hb_task.bucket_stat {
                    self.handle_report_region_buckets(bucket_stat);
                }
            }
            PdTask::StoreHeartbeat {
                stats,
                store_info,
                send_detailed_report,
            } => self.handle_store_heartbeat(stats, store_info, send_detailed_report),
            PdTask::ReportBatchSplit { regions } => self.handle_report_batch_split(regions),
            PdTask::ValidatePeer { region, peer } => self.handle_validate_peer(region, peer),
            PdTask::ReadStats { read_stats } => self.handle_read_stats(read_stats),
            PdTask::WriteStats { write_stats } => self.handle_write_stats(write_stats),
            PdTask::DestroyPeer {
                region_id,
                keyspace_id,
            } => self.handle_destroy_peer(region_id, keyspace_id),
            PdTask::UpdateMaxTimestamp {
                region_id,
                initial_status,
                txn_ext,
            } => self.handle_update_max_timestamp(region_id, initial_status, txn_ext),
            PdTask::UpdateSafeTs => self.handle_update_safe_ts(),
            PdTask::SyncRegion {
                start,
                end,
                limit,
                reverse,
                callback,
            } => {
                self.handle_sync_region(start, end, limit, reverse, callback);
            }
            PdTask::SyncRegionById {
                region_id,
                callback,
            } => {
                self.handle_sync_region_by_id(region_id, callback);
            }
            PdTask::RoleChanged {
                region_id,
                keyspace_id,
                role,
            } => {
                self.handle_role_changed(region_id, keyspace_id, role);
            }
            PdTask::UpdateRaftCpuUtil => {
                self.raft_cpu_collector.update();
            }
        };
    }

    fn shutdown(&mut self) {
        // TODO(x): self.stats_monitor.stop();
    }
}

fn new_change_peer_request(change_type: ConfChangeType, peer: metapb::Peer) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::ChangePeer);
    req.mut_change_peer().set_change_type(change_type);
    req.mut_change_peer().set_peer(peer);
    req
}

fn new_change_peer_v2_request(changes: Vec<pdpb::ChangePeer>) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::ChangePeerV2);
    let change_peer_reqs = changes
        .into_iter()
        .map(|mut c| {
            let mut cp = ChangePeerRequest::default();
            cp.set_change_type(c.get_change_type());
            cp.set_peer(c.take_peer());
            cp
        })
        .collect();
    let mut cp = ChangePeerV2Request::default();
    cp.set_changes(change_peer_reqs);
    req.set_change_peer_v2(cp);
    req
}

fn new_batch_split_region_request(
    split_keys: Vec<Vec<u8>>,
    ids: Vec<pdpb::SplitId>,
    right_derive: bool,
) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::BatchSplit);
    req.mut_splits().set_right_derive(right_derive);
    let mut requests = Vec::with_capacity(ids.len());
    for (mut id, key) in ids.into_iter().zip(split_keys) {
        let mut split = SplitRequest::default();
        split.set_split_key(key);
        split.set_new_region_id(id.get_new_region_id());
        split.set_new_peer_ids(id.take_new_peer_ids());
        requests.push(split);
    }
    req.mut_splits().set_requests(requests.into());
    req
}

fn new_transfer_leader_request(peer: metapb::Peer) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::TransferLeader);
    req.mut_transfer_leader().set_peer(peer);
    req
}

fn new_merge_request(merge: pdpb::Merge) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::PrepareMerge);
    req.mut_prepare_merge()
        .set_target(merge.get_target().to_owned());
    req
}

fn new_admin_command(
    region_id: u64,
    epoch: metapb::RegionEpoch,
    peer: metapb::Peer,
    request: AdminRequest,
) -> RaftCmdRequest {
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_region_id(region_id);
    req.mut_header().set_region_epoch(epoch);
    req.mut_header().set_peer(peer);
    req.set_admin_request(request);
    req
}

fn send_admin_request(
    router: &RaftRouter,
    region_id: u64,
    epoch: metapb::RegionEpoch,
    peer: metapb::Peer,
    request: AdminRequest,
    callback: Callback,
) {
    let req = new_admin_command(region_id, epoch, peer, request);
    router.send_command(req, callback);
}

/// Sends a raft message to destroy the specified stale Peer
fn send_destroy_peer_message(
    router: &RaftRouter,
    local_region: metapb::Region,
    peer: metapb::Peer,
    pd_region: metapb::Region,
) {
    let mut message = RaftMessage::default();
    message.set_region_id(local_region.get_id());
    message.set_from_peer(peer.clone());
    message.set_to_peer(peer);
    message.set_region_epoch(pd_region.get_region_epoch().clone());
    message.set_is_tombstone(true);
    router.send_raft_msg(message);
}

fn collect_report_read_peer_stats(
    capacity: usize,
    mut report_read_stats: HashMap<u64, pdpb::PeerStat>,
    mut stats: pdpb::StoreStats,
) -> pdpb::StoreStats {
    if report_read_stats.len() < capacity * 3 {
        for (_, read_stat) in report_read_stats {
            stats.peer_stats.push(read_stat);
        }
        return stats;
    }
    let mut keys_topn_report = TopN::new(capacity);
    let mut bytes_topn_report = TopN::new(capacity);
    let mut stats_topn_report = TopN::new(capacity);
    for read_stat in report_read_stats.values() {
        let mut cmp_stat = PeerCmpReadStat::default();
        cmp_stat.region_id = read_stat.region_id;
        let mut key_cmp_stat = cmp_stat.clone();
        key_cmp_stat.report_stat = read_stat.read_keys;
        keys_topn_report.push(key_cmp_stat);
        let mut byte_cmp_stat = cmp_stat.clone();
        byte_cmp_stat.report_stat = read_stat.read_bytes;
        bytes_topn_report.push(byte_cmp_stat);
        let mut query_cmp_stat = cmp_stat.clone();
        query_cmp_stat.report_stat = get_read_query_num(read_stat.get_query_stats());
        stats_topn_report.push(query_cmp_stat);
    }

    for x in keys_topn_report {
        if let Some(report_stat) = report_read_stats.remove(&x.region_id) {
            stats.peer_stats.push(report_stat);
        }
    }

    for x in bytes_topn_report {
        if let Some(report_stat) = report_read_stats.remove(&x.region_id) {
            stats.peer_stats.push(report_stat);
        }
    }

    for x in stats_topn_report {
        if let Some(report_stat) = report_read_stats.remove(&x.region_id) {
            stats.peer_stats.push(report_stat);
        }
    }
    stats
}

fn get_read_query_num(stat: &pdpb::QueryStats) -> u64 {
    stat.get_get() + stat.get_coprocessor() + stat.get_scan()
}
