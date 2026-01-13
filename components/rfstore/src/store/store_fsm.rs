// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet, btree_map::BTreeMap},
    ops::{
        Bound::{Excluded, Included, Unbounded},
        Deref, DerefMut,
    },
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::SeqCst},
    },
    thread::JoinHandle,
    time::Duration,
};

use api_version::ApiV2;
use concurrency_manager::ConcurrencyManager;
use fail::fail_point;
use kvproto::{
    metapb::{self, Region, RegionEpoch},
    pdpb,
    pdpb::SyncRegionResponse,
    raft_cmdpb::RaftCmdRequest,
    raft_serverpb::{ExtraMessageType, PeerState, RaftMessage, RegionLocalState},
};
use pd_client::PdClient;
use protobuf::Message;
use raft::{StateRole, eraftpb::ConfChangeType};
use raftstore::{
    coprocessor::{
        BoxAdminObserver, CoprocessorHost, RegionChangeEvent, RegionChangeReason,
        split_observer::SplitObserver,
    },
    store::{
        local_metrics::RaftMetrics,
        util,
        util::{is_initial_msg, is_region_initialized},
    },
};
use resource_control::ResourceController;
use rfengine::{REGION_META_KEY_BYTE, TRUNCATE_ALL_INDEX};
use sst_importer::SstImporter;
use tikv_util::{
    RingQueue, box_err,
    codec::bytes::encode_bytes,
    config::VersionTrack,
    debug, error, info,
    mpsc::{Receiver, Sender},
    store::{find_peer, is_learner},
    sys::thread::StdThreadBuildWrapper,
    time::Instant,
    warn,
    worker::{Builder, LazyWorker, Scheduler},
};
use time::Timespec;

use super::{Config, *};
use crate::{RaftRouter, RaftStoreRouter, Result, store::peer_worker::ApplyWorker};

pub const PENDING_MSG_CAP: usize = 100;
const UNREACHABLE_BACKOFF: Duration = Duration::from_secs(10);

struct Workers {
    pd_worker: LazyWorker<PdTask>,
    gc_worker: LazyWorker<GcTask>,
    schema_worker: LazyWorker<SchemaTask>,
    coprocessor_host: CoprocessorHost,
}

pub struct RaftBatchSystem {
    router: RaftRouter,

    // Change to some after spawn.
    workers: Option<Workers>,

    // Change to none after spawn.
    peer_receiver: Option<Receiver<(u64, Box<PeerMsg>)>>,
    store_fsm: Option<StoreFsm>,
    join_handles: Vec<JoinHandle<()>>,
}

impl RaftBatchSystem {
    pub fn new(engines: &Engines, conf: &Config) -> Self {
        let (store_sender, store_receiver) =
            engines.meta_change_channel.lock().unwrap().take().unwrap();
        let (peer_sender, peer_receiver) = tikv_util::mpsc::unbounded();
        let router = RaftRouter::new(peer_sender, store_sender);
        Self {
            router,
            workers: None,
            peer_receiver: Some(peer_receiver),
            store_fsm: Some(StoreFsm::new(store_receiver, conf)),
            join_handles: vec![],
        }
    }

    pub fn router(&self) -> RaftRouter {
        self.router.clone()
    }

    // TODO: reduce arguments
    pub fn spawn(
        &mut self,
        meta: metapb::Store,
        cfg: Arc<VersionTrack<Config>>,
        engines: Engines,
        trans: Box<dyn Transport>,
        trans_idle: Box<dyn Transport>,
        pd_client: Arc<dyn PdClient>,
        pd_worker: LazyWorker<PdTask>,
        mut store_meta: StoreMeta,
        mut coprocessor_host: CoprocessorHost,
        importer: Arc<SstImporter>,
        concurrency_manager: ConcurrencyManager,
        mut resource_controller: ResourceController,
    ) -> Result<()> {
        assert!(self.workers.is_none());
        // TODO: we can get cluster meta regularly too later.

        // TODO load coprocessors from configuration
        coprocessor_host
            .registry
            .register_admin_observer(100, BoxAdminObserver::new(SplitObserver));

        let mut gc_worker = LazyWorker::new("gc-worker");
        let gc_runner = GcRunner::new(
            engines.kv.clone(),
            Some(importer.clone()),
            cfg.value().local_file_gc_timeout.0,
        );
        gc_worker.start(gc_runner);
        let gc_scheduler = gc_worker.scheduler();

        let schema_worker_name = "schema-worker";
        let mut schema_worker = Builder::new(schema_worker_name)
            .thread_count(cfg.value().schema_worker_count)
            .pending_capacity(256)
            .create()
            .lazy_build(schema_worker_name);
        let schema_runner =
            SchemaRunner::new(meta.get_id(), engines.kv.clone(), self.router.clone());
        schema_worker.start(schema_runner);
        let schema_scheduler = schema_worker.scheduler();

        let mut workers = Workers {
            pd_worker,
            gc_worker,
            schema_worker,
            coprocessor_host: coprocessor_host.clone(),
        };
        let pd_scheduler = workers.pd_worker.scheduler();
        let ctx = GlobalContext {
            cfg,
            engines,
            store: meta,
            readers: store_meta.readers.clone(),
            router: self.router.clone(),
            trans,
            trans_idle,
            pd_scheduler,
            gc_scheduler,
            schema_scheduler,
            coprocessor_host,
            importer,
            destroying: HashSet::default(),
            engine_total_bytes_written: Arc::new(AtomicU64::new(0)),
            engine_total_keys_written: Arc::new(AtomicU64::new(0)),
        };
        let mut region_peers = self.load_peers(&ctx, &mut store_meta)?;
        let readers = store_meta.readers.pin();
        for peer_fsm in &region_peers {
            let peer = peer_fsm.get_peer();
            readers.insert(peer_fsm.region_id(), ReadDelegate::from_peer(peer));
        }
        drop(readers);
        let mut region_ids = Vec::with_capacity(region_peers.len());
        let mut store_ctx =
            StoreContext::new(RaftContext::new(ctx.clone(), WorkerType::Main), store_meta);
        let mut store_fsm = self.store_fsm.take().unwrap();
        for peer in region_peers.drain(..) {
            region_ids.push(peer.peer.region_id);
            let mut store_handler = StoreMsgHandler::new(&mut store_fsm, &mut store_ctx);
            store_handler.register(peer);
        }
        let store_id = ctx.store.get_id();
        let raft_cpu_util_collector = CpuUtilCollector::new("raftstore_".to_string());
        let cpu_util_ref = raft_cpu_util_collector.get_cpu_util_ref();
        let pd_runner = PdRunner::new(
            store_id,
            pd_client,
            self.router.clone(),
            workers.pd_worker.scheduler(),
            ctx.cfg.value().pd_store_heartbeat_tick_interval.into(),
            concurrency_manager,
            workers.pd_worker.remote(),
            ctx.engines.kv.clone(),
            raft_cpu_util_collector,
            resource_controller.get_transfer_leader_limiter().unwrap(),
            ctx.cfg.value().skip_storage_size_metrics_threshold.0,
        );
        assert!(workers.pd_worker.start(pd_runner));
        self.workers = Some(workers);

        let (mut io_worker, io_sender) = IoWorker::new(
            ctx.engines.raft.clone(),
            self.router.clone(),
            ctx.trans.clone(),
            ctx.cfg.value().raft_worker_max_batch_size.0 as usize,
            ctx.cfg.value().io_worker_min_write_duration.0,
        );
        let props = tikv_util::thread_group::current_properties();
        let handle = std::thread::Builder::new()
            .name("raft_io".to_string())
            .spawn_wrapper(move || {
                tikv_util::thread_group::set_properties(props);
                io_worker.run();
            })
            .unwrap();
        self.join_handles.push(handle);
        let peer_receiver = self.peer_receiver.take().unwrap();
        let apply_pool_size = store_ctx.cfg.apply_pool_size;
        let (mut rw, mut apply_receivers) = RaftWorker::new(
            store_ctx,
            peer_receiver,
            ctx.router.clone(),
            io_sender,
            store_fsm,
            cpu_util_ref,
        );
        let props = tikv_util::thread_group::current_properties();
        let handle = std::thread::Builder::new()
            .name("raftstore_0".to_string())
            .spawn_wrapper(move || {
                tikv_util::thread_group::set_properties(props);
                rw.run();
            })
            .unwrap();
        self.join_handles.push(handle);

        for (i, apply_receiver) in apply_receivers.drain(..).enumerate() {
            let thread_name = if i < apply_pool_size {
                format!("apply_{}", i)
            } else {
                format!("apply_follower_{}", i - apply_pool_size)
            };
            let props = tikv_util::thread_group::current_properties();
            let mut aw =
                ApplyWorker::new(ctx.engines.kv.clone(), ctx.router.clone(), apply_receiver);
            let handle = std::thread::Builder::new()
                .name(thread_name)
                .spawn_wrapper(move || {
                    tikv_util::thread_group::set_properties(props);
                    aw.run();
                })
                .unwrap();
            self.join_handles.push(handle);
        }
        self.router.send_store(StoreMsg::Start {
            store: ctx.store.clone(),
        });
        for region_id in region_ids {
            self.router.send(region_id, PeerMsg::Start);
        }
        Ok(())
    }

    pub fn shutdown(&mut self) {
        if self.workers.is_none() {
            return;
        }
        self.router.send_store(StoreMsg::Stop);
        for handle in self.join_handles.drain(..) {
            handle.join().unwrap();
        }
        let mut workers = self.workers.take().unwrap();
        // Wait all workers finish.
        workers.gc_worker.stop();
        workers.pd_worker.stop();
        workers.schema_worker.stop();
        fail_point!("after_shutdown_apply");
        workers.coprocessor_host.shutdown();
    }

    /// load_peers loads peers in this store. It scans the kv engine, loads all
    /// regions and their peers from it, and schedules snapshot worker if
    /// necessary. WARN: This store should not be used before initialized.
    fn load_peers(&self, ctx: &GlobalContext, store_meta: &mut StoreMeta) -> Result<Vec<PeerFsm>> {
        // Scan region meta to get saved regions.
        let mut local_states = vec![];
        let rfengine = &ctx.engines.raft;
        let mut regions_to_peers = rfengine.get_region_peer_map();
        if let Some(black_list) = store_meta.black_list.as_ref() {
            regions_to_peers.retain(|&region_id, _| !black_list.is_region_blocked(region_id));
        }
        let mut tomb_stone_peers = vec![];
        for (_, peer_id) in regions_to_peers {
            rfengine.iterate_peer_states(peer_id, true, |key, val| {
                if key[0] != REGION_META_KEY_BYTE {
                    return true;
                }
                let mut local_state = RegionLocalState::default();
                local_state.merge_from_bytes(val).unwrap();
                if local_state.state != PeerState::Tombstone {
                    local_states.push(local_state);
                } else {
                    let keyspace_id =
                        ApiV2::get_u32_keyspace_id_by_key(local_state.get_region().get_start_key())
                            .unwrap_or_default();
                    ctx.engines
                        .kv
                        .remove_shard(local_state.get_region().get_id());
                    tomb_stone_peers.push((
                        peer_id,
                        local_state.get_region().get_id(),
                        keyspace_id,
                    ));
                }
                false
            });
        }
        // When a source peer is merged, the tombstone state is set, but it's not
        // truncated at the same time, on restart we need to truncate the peers.
        self.clear_tombstone_peers_on_restart(rfengine, tomb_stone_peers);
        let mut peers = vec![];
        let store_id = ctx.store.id;
        for local_state in &local_states {
            let region = local_state.get_region();
            tikv_util::set_current_region_thread_local(region.get_id());
            let mut peer =
                PeerFsm::create(store_id, &ctx.cfg.value(), ctx.engines.clone(), region)?;
            if local_state.get_state() == PeerState::Merging {
                info!("{} region is merging", peer.peer.tag());
                peer.peer.pending_merge_state = Some(local_state.get_merge_state().to_owned());
            }
            store_meta.region_map.put(region.clone());
            ctx.coprocessor_host.on_region_changed(
                region,
                RegionChangeEvent::Create,
                StateRole::Follower,
            );
            peers.push(peer);
        }
        Ok(peers)
    }

    fn clear_tombstone_peers_on_restart(
        &self,
        rfengine: &rfengine::RfEngine,
        tombstone_peers: Vec<(u64, u64, u32)>,
    ) {
        let mut rwb = rfengine::WriteBatch::new();
        for (peer_id, region_id, keyspace_id) in tombstone_peers {
            rfengine.iterate_peer_states(peer_id, false, |k, _| {
                rwb.set_state(peer_id, region_id, keyspace_id, k, &[]);
                true
            });
            rwb.truncate_raft_log(peer_id, region_id, keyspace_id, TRUNCATE_ALL_INDEX);
        }
    }
}

pub struct StoreInfo {
    pub kv_engine: kvengine::Engine,
    pub rf_engine: rfengine::RfEngine,
    pub capacity: u64,
}

pub struct StoreMeta {
    /// store id
    pub store_id: Option<u64>,

    pub region_map: RegionMap,

    pub cop_host: Option<CoprocessorHost>,
    /// region_id -> reader
    pub readers: Arc<papaya::HashMap<u64, Arc<ReadDelegate>>>,
    /// `MsgRequestPreVote`, `MsgRequestVote` or `MsgAppend` messages from newly
    /// split Regions shouldn't be dropped if there is no such Region in
    /// this store now. So the messages are recorded temporarily and will be
    /// handled later.
    pub pending_msgs: RingQueue<RaftMessage>,

    pub black_list: Option<BlackList>,
}

impl StoreMeta {
    pub fn new(vote_capacity: usize) -> StoreMeta {
        StoreMeta {
            store_id: None,
            region_map: Default::default(),
            cop_host: None,
            readers: Arc::new(papaya::HashMap::new()),
            pending_msgs: RingQueue::with_capacity(vote_capacity),
            black_list: None,
        }
    }

    #[inline]
    pub(crate) fn set_region(
        &mut self,
        region: Region,
        peer: &mut crate::store::Peer,
        reason: RegionChangeReason,
    ) {
        let region_id = region.get_id();
        self.region_map.put(region.clone());
        peer.set_region(self.cop_host.as_ref().unwrap(), region, reason);
        let readers = self.readers.pin();
        readers.insert(region_id, ReadDelegate::from_peer(peer));
    }

    pub(crate) fn destroy_region(&mut self, region: &Region) {
        self.region_map.remove(region.id);
        let readers = self.readers.pin();
        readers.remove(&region.id);
    }
}

#[derive(Default)]
pub struct RegionMap {
    /// region_end_key -> region_id
    /// It may have less entries than regions because some entries are removed
    /// on overlap.
    pub region_ranges: BTreeMap<Vec<u8>, u64>,
    /// region_id -> region
    pub regions: HashMap<u64, Region>,
}

impl RegionMap {
    pub fn put(&mut self, region: Region) {
        let region_id = region.get_id();
        self.update_region_ranges(&region);
        self.regions.insert(region_id, region);
    }

    fn update_region_ranges(&mut self, region: &Region) {
        if is_region_initialized(region) {
            if let Some(overlap_regions) = self.get_overlap_regions(region) {
                for (_, end_key) in overlap_regions {
                    self.region_ranges.remove(&end_key);
                }
            }
            self.region_ranges
                .insert(raw_end_key(region), region.get_id());
        }
    }

    fn get_overlap_regions(&mut self, new_region: &Region) -> Option<Vec<(u64, Vec<u8>)>> {
        let start_key = raw_start_key(new_region);
        let end_key = raw_end_key(new_region);
        let mut regions = vec![];
        let mut outdated_keys = vec![];
        for (range_end_key, region_id) in self.region_ranges.range((Excluded(start_key), Unbounded))
        {
            let region = self.regions.get(region_id);
            if region.is_none() {
                outdated_keys.push(range_end_key.clone());
                continue;
            }
            let region = region.unwrap();
            let region_start_key = raw_start_key(region);
            if region_start_key >= end_key {
                break;
            }
            if Self::current_region_is_newer(region, new_region) {
                return None;
            }
            let raw_end_key = raw_end_key(region);
            regions.push((*region_id, raw_end_key));
        }
        for outdated_key in outdated_keys {
            self.region_ranges.remove(&outdated_key);
        }
        Some(regions)
    }

    fn current_region_is_newer(current_region: &Region, new_region: &Region) -> bool {
        use std::cmp::Ordering::{Equal, Greater, Less};
        let current_epoch = current_region.get_region_epoch();
        let new_epoch = new_region.get_region_epoch();
        match current_epoch.version.cmp(&new_epoch.version) {
            Greater => {
                warn!(
                    "region {:?} overlap {:?} has newer epoch version",
                    new_region, current_region
                );
                true
            }
            Equal => {
                if current_epoch.conf_ver >= new_epoch.conf_ver {
                    warn!(
                        "region {:?} overlap {:?} has newer or equal conf_ver",
                        new_region, current_region
                    );
                }
                // Return true to indicate that the current region do not need to be removed.
                true
            }
            Less => false,
        }
    }

    pub fn remove(&mut self, region_id: u64) {
        if let Some(region) = self.regions.remove(&region_id) {
            if is_region_initialized(&region) {
                let end_key = raw_end_key(&region);
                if let Some(&id) = self.region_ranges.get(&end_key) {
                    if id == region.id {
                        self.region_ranges.remove(&end_key);
                    }
                }
            }
        }
    }

    pub fn get(&self, region_id: u64) -> Option<&Region> {
        self.regions.get(&region_id)
    }

    pub fn len(&self) -> usize {
        self.regions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.regions.is_empty()
    }

    pub fn scan_regions(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
        reverse: bool,
    ) -> Vec<&Region> {
        if start >= end {
            error!("scan regions failed, start key is greater than or equal to end key";
                "start" => ?start,
                "end" => ?end
            );
            return vec![];
        }
        let mut regions = vec![];
        let mut right_bound = Unbounded;
        let encoded_end = encode_bytes(&end);
        if let Some((region_end, region_id)) = self
            .region_ranges
            .range((Included(end.clone()), Unbounded))
            .next()
        {
            right_bound = Included(end);
            let region = self.regions.get(region_id);
            if let Some(region) = region {
                if region.start_key < encoded_end {
                    right_bound = Included(region_end.clone());
                }
            }
        }
        let mut iter: Box<dyn DoubleEndedIterator<Item = (&Vec<u8>, &u64)>> =
            Box::new(self.region_ranges.range((Excluded(start), right_bound)));
        if reverse {
            iter = Box::new(iter.rev());
        }
        for (_, region_id) in iter {
            if limit > 0 && regions.len() >= limit {
                break;
            }
            let region = self.regions.get(region_id);
            if region.is_none() {
                continue;
            }
            let region = region.unwrap();
            regions.push(region);
        }
        regions
    }
}

#[derive(Clone)]
pub(crate) struct GlobalContext {
    pub(crate) cfg: Arc<VersionTrack<Config>>,
    pub(crate) engines: Engines,
    pub(crate) store: metapb::Store,
    pub(crate) readers: Arc<papaya::HashMap<u64, Arc<ReadDelegate>>>,
    pub(crate) router: RaftRouter,
    pub(crate) trans: Box<dyn Transport>,
    pub(crate) trans_idle: Box<dyn Transport>,
    pub(crate) pd_scheduler: Scheduler<PdTask>,
    pub(crate) gc_scheduler: Scheduler<GcTask>,
    pub(crate) schema_scheduler: Scheduler<SchemaTask>,
    pub(crate) coprocessor_host: CoprocessorHost,
    pub(crate) importer: Arc<SstImporter>,
    /// Saves destroying regions in one loop. It's used to solve the race
    /// between peer gc and split, i.e., split won't create a destroying
    /// region if they are in the same loop with checking it.
    pub(crate) destroying: HashSet<u64>,
    pub(crate) engine_total_bytes_written: Arc<AtomicU64>,
    pub(crate) engine_total_keys_written: Arc<AtomicU64>,
}

pub(crate) struct RaftContext {
    pub(crate) global: GlobalContext,
    pub(crate) apply_msgs: ApplyMsgs,
    pub(crate) persist_readies: Vec<PersistReady>,
    pub(crate) raft_wb: rfengine::WriteBatch,
    pub(crate) remove_dependents: Vec<(u64 /* parent_id */, u64 /* dependent_id */)>,
    pub(crate) current_time: Option<Timespec>,
    pub(crate) raft_metrics: RaftMetrics,
    pub(crate) cfg: Config,
    pub(crate) worker_type: WorkerType,
    pub(crate) pending_idles: Vec<u64>,
}

#[derive(PartialEq)]
pub(crate) enum WorkerType {
    Main,
    Idle,
    Aux,
}

// There is only one StoreContext owned by the main raft worker.
pub(crate) struct StoreContext {
    pub(crate) raft_ctx: RaftContext,
    pub(crate) peers: Vec<HashMap<u64, PeerStates>>,
    pub(crate) store_meta: StoreMeta,
    pub(crate) idle_regions: HashSet<u64>,
    pub(crate) idle_sender: Option<Sender<(u64, Box<PeerMsg>)>>,
}

pub(crate) const PEER_SEGMENTS: usize = 128;

impl StoreContext {
    pub(crate) fn new(raft_ctx: RaftContext, store_meta: StoreMeta) -> StoreContext {
        StoreContext {
            raft_ctx,
            peers: vec![HashMap::new(); PEER_SEGMENTS],
            store_meta,
            idle_regions: HashSet::new(),
            idle_sender: None,
        }
    }

    pub(crate) fn try_get_peer(&self, region_id: u64) -> Option<PeerStates> {
        self.peers[Self::region_idx(region_id)]
            .get(&region_id)
            .cloned()
    }

    pub(crate) fn get_peer(&self, region_id: u64) -> PeerStates {
        self.peers[Self::region_idx(region_id)]
            .get(&region_id)
            .unwrap()
            .clone()
    }

    pub(crate) fn insert_peer(&mut self, region_id: u64, peer_state: PeerStates) {
        self.peers[Self::region_idx(region_id)].insert(region_id, peer_state);
    }

    pub(crate) fn remove_peer(&mut self, region_id: u64) {
        self.peers[Self::region_idx(region_id)].remove(&region_id);
    }

    fn region_idx(region_id: u64) -> usize {
        crc32c::crc32c(&region_id.to_le_bytes()) as usize % PEER_SEGMENTS
    }
}

impl Deref for StoreContext {
    type Target = RaftContext;

    fn deref(&self) -> &Self::Target {
        &self.raft_ctx
    }
}

impl DerefMut for StoreContext {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.raft_ctx
    }
}

impl RaftContext {
    pub(crate) fn new(global: GlobalContext, worker_type: WorkerType) -> Self {
        let cfg = global.cfg.value().clone();
        Self {
            global,
            apply_msgs: ApplyMsgs { msgs: vec![] },
            persist_readies: vec![],
            raft_wb: rfengine::WriteBatch::new(),
            remove_dependents: vec![],
            current_time: None,
            raft_metrics: RaftMetrics::new(false),
            cfg,
            pending_idles: vec![],
            worker_type,
        }
    }

    pub fn handle_stale_msg(
        &mut self,
        msg: &RaftMessage,
        cur_epoch: RegionEpoch,
        target_region: Option<metapb::Region>,
    ) {
        let region_id = msg.get_region_id();
        let from_peer = msg.get_from_peer();
        let to_peer = msg.get_to_peer();
        let msg_type = msg.get_message().get_msg_type();
        let tag = PeerTag::new(
            self.store_id(),
            RegionIdVer::new(region_id, cur_epoch.get_version()),
        );

        info!(
            "raft message is stale, tell to gc";
            "region" => tag,
            "current_region_epoch" => ?cur_epoch,
            "msg_type" => ?msg_type,
        );

        self.raft_metrics.message_dropped.stale_msg.inc();

        let mut gc_msg = RaftMessage::default();
        gc_msg.set_region_id(region_id);
        gc_msg.set_from_peer(to_peer.clone());
        gc_msg.set_to_peer(from_peer.clone());
        gc_msg.set_region_epoch(cur_epoch);
        if let Some(r) = target_region {
            gc_msg.set_merge_target(r);
        } else {
            gc_msg.set_is_tombstone(true);
        }
        if let Err(e) = self.global.trans.send(gc_msg) {
            error!(?e;
                "send gc message failed";
                "tag" => tag,
            );
        }
    }

    pub(crate) fn store_id(&self) -> u64 {
        self.global.store.get_id()
    }

    // Region(dependent_id) depends on Region(parent_id).
    #[inline]
    pub fn remove_dependent(&self, parent_id: u64, dependent_id: u64) {
        remove_dependent(
            &self.global.engines.raft,
            &self.global.router,
            parent_id,
            dependent_id,
        );
    }
}

pub(crate) struct StoreFsm {
    pub(crate) id: u64,
    pub(crate) start_time: Option<Timespec>,
    pub(crate) receiver: Receiver<StoreMsg>,
    pub(crate) ticker: Ticker,
    pub(crate) last_tick: Instant,
    pub(crate) tick_millis: u64,
    last_unreachable_report: HashMap<u64, Instant>,
    pub(crate) stopped: bool,
}

impl StoreFsm {
    pub fn new(receiver: Receiver<StoreMsg>, cfg: &Config) -> Self {
        Self {
            id: 0,
            start_time: None,
            receiver,
            ticker: Ticker::new_store(cfg),
            last_tick: Instant::now(),
            tick_millis: cfg.raft_base_tick_interval.as_millis(),
            last_unreachable_report: HashMap::new(),
            stopped: false,
        }
    }
}

#[derive(Debug, PartialEq)]
enum CheckMsgStatus {
    // The message is the first message to an existing peer.
    FirstRequest,
    // The message can be dropped silently
    DropMsg,
    // Try to create the peer
    NewPeer,
    // Try to create the peer which is the first one of this region on local store.
    NewPeerFirst,
}

pub(crate) struct StoreMsgHandler<'a> {
    store: &'a mut StoreFsm,
    pub(crate) ctx: &'a mut StoreContext,
}

impl<'a> StoreMsgHandler<'a> {
    pub(crate) fn new(store: &'a mut StoreFsm, ctx: &'a mut StoreContext) -> StoreMsgHandler<'a> {
        Self { store, ctx }
    }

    pub(crate) fn handle_msg(&mut self, msg: StoreMsg) -> Option<u64> {
        if let Some(region_id) = self.need_wake_up_idle(&msg) {
            // For idle regions, we can not handle the message directly.
            // By sending PeerMsg::StoreMsgForWakeup to the idle worker, the region
            // will be woke up. Then in the main worker the store message will be sent
            // again.
            let wake_up_msg = PeerMsg::StoreMsgForWakeUp(msg);
            let idle_sender = self.ctx.idle_sender.as_ref().unwrap();
            idle_sender
                .send((region_id, Box::new(wake_up_msg)))
                .unwrap();
            return None;
        }
        let mut apply_region = None;
        match msg {
            StoreMsg::Tick => self.on_tick(),
            StoreMsg::Start { store } => self.start(store),
            StoreMsg::StoreUnreachable { store_id } => self.on_store_unreachable(store_id),
            StoreMsg::GenerateEngineChangeSet(cs, cb) => {
                self.on_generate_engine_meta_change(cs, cb)
            }
            StoreMsg::RaftMessage(msg) => {
                tikv_util::set_current_region_thread_local(msg.region_id);
                self.on_raft_message(msg)
            }
            StoreMsg::SnapshotReady(region_id) => {
                tikv_util::set_current_region_thread_local(region_id);
                apply_region = self.on_snapshot_ready(region_id);
            }
            StoreMsg::GetRegionsInRange {
                start,
                end,
                callback,
            } => {
                self.on_get_regions_in_range(start, end, callback);
            }
            StoreMsg::GetRegionById {
                region_id,
                callback,
            } => {
                self.on_get_region_by_id(region_id, callback);
            }
            StoreMsg::SyncRegion {
                start,
                end,
                limit,
                reverse,
                callback,
            } => {
                self.on_sync_region(start, end, limit, reverse, callback);
            }
            StoreMsg::SyncRegionById {
                region_id,
                callback,
            } => {
                self.on_sync_region_by_id(region_id, callback);
            }
            StoreMsg::ApplyResult { region_id, peer_id } => {
                tikv_util::set_current_region_thread_local(region_id);
                apply_region = self.on_apply_result(region_id, peer_id);
            }
            StoreMsg::DependentsEmpty(region_id) => {
                tikv_util::set_current_region_thread_local(region_id);
                self.on_dependents_empty(region_id);
            }
            StoreMsg::PrepareMerge { region_id, req } => {
                tikv_util::set_current_region_thread_local(region_id);
                self.on_prepare_merge_request(region_id, req);
            }
            StoreMsg::CheckMerge(region_id) => {
                tikv_util::set_current_region_thread_local(region_id);
                self.on_check_merge(region_id);
            }
            StoreMsg::Stop => {
                self.store.stopped = true;
            }
        }
        apply_region
    }

    fn need_wake_up_idle(&mut self, msg: &StoreMsg) -> Option<u64> {
        match msg {
            StoreMsg::SnapshotReady(region_id) => Some(*region_id),
            StoreMsg::ApplyResult { region_id, .. } => Some(*region_id),
            StoreMsg::DependentsEmpty(region_id) => Some(*region_id),
            StoreMsg::PrepareMerge { region_id, .. } => Some(*region_id),
            StoreMsg::CheckMerge(region_id) => Some(*region_id),
            _ => None,
        }
        .filter(|id| self.ctx.idle_regions.contains(id))
    }

    fn on_tick(&mut self) {
        self.store.ticker.tick_clock();
        if self.store.ticker.is_on_store_tick(STORE_TICK_PD_HEARTBEAT) {
            self.on_pd_heartbeat_tick();
        }
        if self
            .store
            .ticker
            .is_on_store_tick(STORE_TICK_UPDATE_SAFE_TS)
        {
            self.on_update_safe_ts();
        }
        if self.store.ticker.is_on_store_tick(STORE_TICK_LOCAL_FILE_GC) {
            self.on_local_file_gc();
        }
        if self.ctx.cfg.aux_worker_count > 0 {
            let _ = self
                .ctx
                .global
                .pd_scheduler
                .schedule(PdTask::UpdateRaftCpuUtil);
        }
    }

    fn store_heartbeat_pd(&mut self) {
        let mut stats = pdpb::StoreStats::default();

        stats.set_store_id(self.store.id);
        stats.set_region_count(self.ctx.store_meta.region_map.len() as u32);

        stats.set_start_time(self.store.start_time.unwrap().sec as u32);

        stats.set_bytes_written(self.ctx.global.engine_total_bytes_written.swap(0, SeqCst));
        stats.set_keys_written(self.ctx.global.engine_total_keys_written.swap(0, SeqCst));

        let store_info = StoreInfo {
            kv_engine: self.ctx.global.engines.kv.clone(),
            rf_engine: self.ctx.global.engines.raft.clone(),
            capacity: self.ctx.cfg.capacity.0,
        };

        let task = PdTask::StoreHeartbeat {
            stats,
            store_info,
            send_detailed_report: false,
        };
        if let Err(e) = self.ctx.global.pd_scheduler.schedule(task) {
            error!("notify pd failed";
                "store_id" => self.store.id,
                "err" => ?e
            );
        }
    }

    fn on_pd_heartbeat_tick(&mut self) {
        self.store_heartbeat_pd();
        self.store.ticker.schedule_store(STORE_TICK_PD_HEARTBEAT);
    }

    fn on_update_safe_ts(&mut self) {
        if let Err(e) = self.ctx.global.pd_scheduler.schedule(PdTask::UpdateSafeTs) {
            error!("update safe ts failed";
                "store_id" => self.store.id,
                "err" => ?e
            );
        }
        self.store.ticker.schedule_store(STORE_TICK_UPDATE_SAFE_TS);
    }

    fn on_local_file_gc(&mut self) {
        if let Err(e) = self.ctx.global.gc_scheduler.schedule(GcTask {}) {
            error!("local file gc failed";
                "store_id" => self.store.id,
                "err" => ?e
            );
        }
        self.store.ticker.schedule_store(STORE_TICK_LOCAL_FILE_GC);
    }

    fn start(&mut self, store: metapb::Store) {
        if self.store.start_time.is_some() {
            panic!("store unable to start again");
        }
        self.store.id = store.id;
        self.store.start_time = Some(time::get_time());
        self.store_heartbeat_pd();
        self.store.ticker.schedule_store(STORE_TICK_PD_HEARTBEAT);
        self.store.ticker.schedule_store(STORE_TICK_UPDATE_SAFE_TS);
        self.store.ticker.schedule_store(STORE_TICK_LOCAL_FILE_GC);
    }

    fn on_store_unreachable(&mut self, store_id: u64) {
        let now = Instant::now();
        if self
            .store
            .last_unreachable_report
            .get(&store_id)
            .map_or(UNREACHABLE_BACKOFF, |t| now.saturating_duration_since(*t))
            < UNREACHABLE_BACKOFF
        {
            return;
        }
        info!(
            "broadcasting unreachable";
            "store_id" => self.store.id,
            "unreachable_store_id" => store_id,
        );
        self.store.last_unreachable_report.insert(store_id, now);
        for (id, region) in &self.ctx.store_meta.region_map.regions {
            // Skip sending unreachable to idle peers because it is expensive to wake up
            // all the idle peers and not sending explicit unreachable doesn't break raft.
            if !self.ctx.idle_regions.contains(id)
                && region.get_peers().iter().any(|p| p.store_id == store_id)
            {
                self.ctx.global.router.report_unreachable(*id, store_id);
            }
        }
    }

    /// Checks if the message is targeting a stale peer.
    fn check_msg(&mut self, msg: &RaftMessage) -> Result<CheckMsgStatus> {
        let region_id = msg.get_region_id();
        let from_epoch = msg.get_region_epoch();
        let msg_type = msg.get_message().get_msg_type();
        let from_store_id = msg.get_from_peer().get_store_id();
        let to_peer_id = msg.get_to_peer().get_id();

        // Check if the target peer is tombstone.
        let local_state = match load_last_peer_state(&self.ctx.global.engines.raft, to_peer_id) {
            Some(state) => state,
            None => return Ok(CheckMsgStatus::NewPeerFirst),
        };
        let tag = PeerTag::new(
            self.store.id,
            RegionIdVer::new(
                region_id,
                local_state.get_region().get_region_epoch().get_version(),
            ),
        );
        if local_state.get_state() != PeerState::Tombstone {
            // Maybe split, but not registered yet.
            if !util::is_first_message(msg.get_message()) {
                return Err(box_err!(
                    "[region {}] region not exist but not tombstone: {:?}",
                    tag,
                    local_state
                ));
            }
            info!(
                "region doesn't exist yet, wait for it to be split";
                "region" => tag
            );
            return Ok(CheckMsgStatus::FirstRequest);
        }
        debug!(
            "region is in tombstone state";
            "region" => tag,
            "region_local_state" => ?local_state,
        );
        let region = local_state.get_region();
        let region_epoch = region.get_region_epoch();
        if local_state.has_merge_state() {
            info!(
                "merged peer receives a stale message";
                "region" => tag,
                "current_region_epoch" => ?region_epoch,
                "msg_type" => ?msg_type,
            );

            let merge_target = if let Some(peer) = find_peer(region, from_store_id) {
                // Maybe the target is promoted from learner to voter, but the follower
                // doesn't know it. So we only compare peer id.
                if peer.get_id() < msg.get_from_peer().get_id() {
                    panic!(
                        "peer id increased after region is merged, message peer id {}, local peer id {}, region {:?}",
                        msg.get_from_peer().get_id(),
                        peer.get_id(),
                        region
                    );
                }
                // Let stale peer decides whether it should wait for merging or just remove
                // itself.
                Some(local_state.get_merge_state().get_target().to_owned())
            } else {
                // If a peer is isolated before prepare_merge and conf remove, it should just
                // remove itself.
                None
            };
            self.ctx
                .handle_stale_msg(msg, region_epoch.clone(), merge_target);
            return Ok(CheckMsgStatus::DropMsg);
        }
        // The region in this peer is already destroyed
        if util::is_epoch_stale(from_epoch, region_epoch) {
            info!(
                "tombstone peer receives a stale message";
                "region" => tag,
                "from_region_epoch" => ?from_epoch,
                "current_region_epoch" => ?region_epoch,
                "msg_type" => ?msg_type,
            );
            if find_peer(region, from_store_id).is_none() {
                self.ctx.handle_stale_msg(msg, region_epoch.clone(), None);
            } else {
                let mut need_gc_msg = util::is_vote_msg(msg.get_message());
                if msg.has_extra_msg() {
                    // A learner can't vote so it sends the check-stale-peer msg to others to find
                    // out whether it is removed due to conf change or merge.
                    need_gc_msg |=
                        msg.get_extra_msg().get_type() == ExtraMessageType::MsgCheckStalePeer;
                    // For backward compatibility
                    need_gc_msg |=
                        msg.get_extra_msg().get_type() == ExtraMessageType::MsgRegionWakeUp;
                }
                if need_gc_msg {
                    let mut send_msg = RaftMessage::default();
                    send_msg.set_region_id(region_id);
                    send_msg.set_from_peer(msg.get_to_peer().clone());
                    send_msg.set_to_peer(msg.get_from_peer().clone());
                    send_msg.set_region_epoch(region_epoch.clone());
                    let extra_msg = send_msg.mut_extra_msg();
                    extra_msg.set_type(ExtraMessageType::MsgCheckStalePeerResponse);
                    extra_msg.set_check_peers(region.get_peers().into());
                    if let Err(e) = self.ctx.global.trans.send(send_msg) {
                        error!(?e;
                            "send check stale peer response message failed";
                            "region" => tag,
                        );
                    }
                }
            }

            return Ok(CheckMsgStatus::DropMsg);
        }
        // A tombstone peer may not apply the conf change log which removes itself.
        // In this case, the local epoch is stale and the local peer can be found from
        // region. We can compare the local peer id with to_peer_id to verify
        // whether it is correct to create a new peer.
        if let Some(local_peer_id) = find_peer(region, self.ctx.store_id()).map(|r| r.get_id()) {
            if to_peer_id <= local_peer_id {
                self.ctx
                    .raft_metrics
                    .message_dropped
                    .region_tombstone_peer
                    .inc();
                info!(
                    "tombstone peer receives a stale message, local_peer_id >= to_peer_id in msg";
                    "tag" => tag,
                    "local_peer_id" => local_peer_id,
                    "to_peer_id" => to_peer_id,
                    "msg_type" => ?msg_type
                );
                return Ok(CheckMsgStatus::DropMsg);
            }
        }
        Ok(CheckMsgStatus::NewPeer)
    }

    fn on_raft_message(&mut self, msg: RaftMessage) {
        let region_id = msg.get_region_id();
        if let Some(black_list) = self.ctx.store_meta.black_list.as_ref() {
            if black_list.is_region_blocked(region_id) {
                debug!("region {} blocked by black list", region_id);
                return;
            }
            if let Some(keyspace_id) = ApiV2::get_u32_keyspace_id_by_key(msg.get_start_key()) {
                if black_list.is_keyspace_blocked(keyspace_id) {
                    debug!("keyspace {} blocked by black list", keyspace_id);
                    return;
                }
            }
            if let Some((keyspace_id, table_id)) = ApiV2::get_keyspace_table_id(msg.get_start_key())
            {
                if black_list.is_table_blocked(keyspace_id, table_id) {
                    debug!("table {} blocked by black list", table_id);
                    return;
                }
            }
        }
        let region_epoch = msg.get_region_epoch();
        let tag = PeerTag::new(
            self.store.id,
            RegionIdVer::new(region_id, region_epoch.version),
        );
        info!("{} store handle raft message {}", tag, MsgDebug(&msg));

        if msg.get_to_peer().get_store_id() != self.ctx.store_id() {
            warn!(
                "store not match, ignore it";
                "to_store_id" => msg.get_to_peer().get_store_id(),
                "tag" => tag,
            );
            return;
        }

        if !msg.has_region_epoch() {
            error!(
                "missing epoch in raft message, ignore it";
                "tag" => tag,
            );
            return;
        }
        if msg.get_is_tombstone() || msg.has_merge_target() {
            // Target tombstone peer doesn't exist, so ignore it.
            return;
        }
        let check_msg_result = self.check_msg(&msg);
        if let Err(err) = check_msg_result {
            error!("{} failed to check message err:{:?}", tag, err);
            return;
        }
        let check_msg_status = check_msg_result.unwrap();
        let is_first_request = match check_msg_status {
            CheckMsgStatus::DropMsg => return,
            CheckMsgStatus::FirstRequest => true,
            CheckMsgStatus::NewPeer | CheckMsgStatus::NewPeerFirst => {
                if self.maybe_create_peer(
                    region_id,
                    region_epoch.clone(),
                    &msg,
                    check_msg_status == CheckMsgStatus::NewPeerFirst,
                ) {
                    // Peer created, send the message again.
                    let peer_msg = PeerMsg::RaftMessage(msg);
                    self.ctx.global.router.send(region_id, peer_msg);
                    return;
                }
                // Can't create peer, see if we should keep this message
                util::is_first_message(msg.get_message())
            }
        };
        if is_first_request {
            // To void losing messages, either put it to pending_msg or force send.
            if !self
                .ctx
                .store_meta
                .region_map
                .regions
                .contains_key(&region_id)
            {
                // Save one pending message for a peer is enough, remove
                // the previous pending message of this peer
                self.ctx
                    .store_meta
                    .pending_msgs
                    .swap_remove_front(|m| m.get_to_peer() == msg.get_to_peer());

                self.ctx.store_meta.pending_msgs.push(msg);
            } else {
                let peer_msg = PeerMsg::RaftMessage(msg);
                self.ctx.global.router.send(region_id, peer_msg);
            }
        }
    }

    fn on_generate_engine_meta_change(&self, change_set: kvenginepb::ChangeSet, cb: Callback) {
        debug!("store on generate engine meta change {:?}", &change_set);
        // GenerateEngineMetaChange message is first sent to store handler,
        // Then send it to the router to create a raft log then propose this log,
        // replicate to followers.
        let id = change_set.get_shard_id();
        let peer_msg = PeerMsg::GenerateEngineChangeSet(change_set, cb);
        // If the region is not found, there is no need to handle the engine meta
        // change, so we can ignore not found error.
        self.ctx.global.router.send(id, peer_msg);
    }

    /// If target peer doesn't exist, create it.
    ///
    /// return false to indicate that target peer is in invalid state or
    /// doesn't exist and can't be created.
    fn maybe_create_peer(
        &mut self,
        region_id: u64,
        region_epoch: metapb::RegionEpoch,
        msg: &RaftMessage,
        is_local_first: bool,
    ) -> bool {
        let tag = PeerTag::new(
            self.store.id,
            RegionIdVer::new(region_id, region_epoch.version),
        );
        if !is_initial_msg(msg.get_message()) {
            info!(
                "target peer doesn't exist, stale message";
                "target_peer" => ?msg.get_to_peer(),
                "tag" => tag,
                "msg" => %MsgDebug(msg),
            );
            self.ctx.raft_metrics.message_dropped.stale_msg.inc();
            return false;
        }
        match self.maybe_create_peer_internal(region_id, region_epoch, msg, is_local_first) {
            Ok(created) => created,
            Err(err) => {
                error!("{} failed to create peer {:?}", tag, err);
                false
            }
        }
    }

    fn maybe_create_peer_internal(
        &mut self,
        region_id: u64,
        region_epoch: metapb::RegionEpoch,
        msg: &RaftMessage,
        _is_local_first: bool,
    ) -> Result<bool> {
        if self.ctx.store_meta.region_map.get(region_id).is_some() {
            return Ok(true);
        }
        let target = msg.get_to_peer();
        // New created peers should know it's learner or not.
        let peer = PeerFsm::replicate(
            self.ctx.store_id(),
            &self.ctx.cfg,
            self.ctx.global.engines.clone(),
            region_id,
            region_epoch,
            target.clone(),
        )?;
        fail_point!("after_acquire_store_meta_on_maybe_create_peer_internal");

        // TODO(x) handle overlap exisitng region.

        // WARNING: The checking code must be above this line.
        // Now all checking passed

        // Following snapshot may overlap, should insert into region_ranges after
        // snapshot is applied.
        self.ctx
            .store_meta
            .region_map
            .put(peer.get_peer().region().to_owned());
        self.register(peer);
        self.ctx.global.router.send(region_id, PeerMsg::Start);
        Ok(true)
    }

    pub(crate) fn register(&mut self, peer: PeerFsm) {
        let id = peer.peer.region().id;
        let tag = peer.peer.tag();
        info!(
            "register region {}, peer {}, encryption_key {:?}",
            tag,
            peer.peer.peer_id(),
            peer.peer.encryption_key,
        );
        let applier = Applier::new_from_peer(&peer);
        let new_peer = PeerStates::new(applier, peer);
        self.ctx.insert_peer(id, new_peer);
    }

    fn get_peer(&mut self, region_id: u64) -> PeerStates {
        self.ctx.get_peer(region_id)
    }

    fn try_get_peer(&mut self, region_id: u64) -> Option<PeerStates> {
        self.ctx.try_get_peer(region_id)
    }

    fn on_split_region(&mut self, regions: Vec<metapb::Region>) {
        fail_point!("on_split", self.ctx.store_id() == 3, |_| {});
        let derived = regions.last().unwrap().clone();
        let derived_peer = self.get_peer(derived.get_id());
        let mut peer_fsm = derived_peer.peer_fsm.lock().unwrap();
        let region_id = derived.get_id();
        self.ctx
            .store_meta
            .set_region(derived, &mut peer_fsm.peer, RegionChangeReason::Split);

        let is_leader = peer_fsm.peer.is_leader();
        self.ctx
            .global
            .engines
            .kv
            .set_shard_active(region_id, is_leader);
        let tag = peer_fsm.peer.tag();
        if is_leader {
            peer_fsm.peer.heartbeat_pd(self.ctx);
            // Notify pd immediately to let it update the region meta.
            info!(
                "notify pd with split";
                "tag" => tag,
                "peer_id" => peer_fsm.peer_id(),
                "split_count" => regions.len(),
            );
            // Now pd only uses ReportBatchSplit for history operation show,
            // so we send it independently here.
            let task = PdTask::ReportBatchSplit {
                regions: regions.to_vec(),
            };
            if let Err(e) = self.ctx.global.pd_scheduler.schedule(task) {
                error!(
                    "failed to notify pd";
                    "tag" => tag,
                    "peer_id" => peer_fsm.peer_id(),
                    "err" => %e,
                );
            }
        }

        let mut new_peers = vec![];
        for new_region in regions {
            let new_peer_id = get_peer_id_by_store_id(&new_region, self.store.id).unwrap();
            let new_region_id = new_region.get_id();

            if new_region_id == region_id {
                continue;
            }
            if let Some(existing) = self.ctx.try_get_peer(new_region_id) {
                let peer_fsm = existing.peer_fsm.lock().unwrap();
                let tag = peer_fsm.peer.tag();
                let pending_snap = peer_fsm.peer.has_pending_snapshot();
                let initialized = peer_fsm.peer.is_initialized();
                let pending_remove = peer_fsm.peer.pending_remove;
                let peer_id_changed = !region_has_peer(&new_region, peer_fsm.peer_id());
                if pending_snap || initialized || pending_remove || peer_id_changed {
                    // We already received the snapshot, the snapshot must be newer than the newly
                    // split one, we should keep the newer peer.
                    info!(
                        "{} peer avoid replace by split, has pending snap {}, initialized {}, pending remove {}, peer_id changed {}",
                        tag, pending_snap, initialized, pending_remove, peer_id_changed,
                    );
                    self.ctx.remove_dependent(region_id, new_region_id);
                    continue;
                }
            } else if load_last_peer_state(&self.ctx.global.engines.raft, new_peer_id)
                .map(|state| state.get_state() == PeerState::Tombstone)
                .unwrap_or(false)
                || self.ctx.global.destroying.contains(&new_region_id)
            {
                // The peer has been destroyed.
                info!("{} region {} avoid created by split", tag, new_region_id);
                self.ctx.remove_dependent(region_id, new_region_id);
                self.ctx.global.engines.kv.remove_shard(new_region_id);
                continue;
            }
            // Now all checking passed.
            // Insert new regions and validation
            info!(
                "split insert new region";
                "tag" => tag,
                "region_id" => new_region_id,
                "region" => ?new_region,
            );

            let mut new_peer = match PeerFsm::create(
                self.ctx.store_id(),
                &self.ctx.cfg,
                self.ctx.global.engines.clone(),
                &new_region,
            ) {
                Ok(new_peer) => new_peer,
                Err(e) => {
                    // peer information is already written into db, can't recover.
                    // there is probably a bug.
                    panic!("create new split region {:?} err {:?}", new_region, e);
                }
            };

            let meta_peer = new_peer.peer.peer.clone();

            for p in new_region.get_peers() {
                // Add this peer to cache.
                new_peer.peer.insert_peer_cache(p.clone());
            }

            // New peer derive write flow from parent region,
            // this will be used by balance write flow.
            new_peer.peer.peer_stat = peer_fsm.peer.peer_stat.clone();
            new_peer.peer.need_campaign = is_leader;

            if is_leader {
                // The new peer is likely to become leader, send a heartbeat immediately to
                // reduce client query miss.
                new_peer.peer.heartbeat_pd(self.ctx);
            }
            self.ctx.global.coprocessor_host.on_region_changed(
                &new_region,
                RegionChangeEvent::Create,
                new_peer.peer.get_role(),
            );
            self.ctx.store_meta.region_map.put(new_region.clone());
            let read_delegate = ReadDelegate::from_peer(new_peer.get_peer());
            let readers = self.ctx.store_meta.readers.pin();
            readers.insert(new_region_id, read_delegate);
            drop(readers);

            new_peers.push(new_peer);
            self.ctx.global.router.send(new_region_id, PeerMsg::Start);
            if is_leader {
                self.ctx.global.router.send(new_region_id, PeerMsg::Tick);
            }

            if !is_leader {
                if let Some(msg) = self
                    .ctx
                    .store_meta
                    .pending_msgs
                    .swap_remove_front(|m| m.get_to_peer() == &meta_peer)
                {
                    self.ctx
                        .global
                        .router
                        .send(new_region_id, PeerMsg::RaftMessage(msg));
                }
            }
        }
        for new_peer in new_peers {
            self.register(new_peer);
        }
        fail_point!("after_split", self.ctx.store_id() == 3, |_| {});
    }

    fn on_change_peer(&mut self, cp: ChangePeer) -> bool {
        let region_id = cp.region.id;
        let peer = self.get_peer(region_id);
        let mut peer_fsm = peer.peer_fsm.lock().unwrap();

        if cp.index >= peer_fsm.peer.raft_group.raft.raft_log.first_index() {
            match peer_fsm.peer.raft_group.apply_conf_change(&cp.conf_change) {
                Ok(_) => {}
                // PD could dispatch redundant conf changes.
                Err(raft::Error::NotExists { .. }) | Err(raft::Error::Exists { .. }) => {}
                _ => unreachable!(),
            }
        } else {
            // Please take a look at test case
            // test_redundant_conf_change_by_snapshot.
        }
        self.update_region(&mut peer_fsm, cp.region, RegionChangeReason::ChangePeer);

        fail_point!("change_peer_after_update_region");

        let now = Instant::now();
        let (mut remove_self, mut need_ping) = (false, false);
        for mut change in cp.changes {
            let (change_type, peer) = (change.get_change_type(), change.take_peer());
            let (store_id, peer_id) = (peer.get_store_id(), peer.get_id());
            match change_type {
                ConfChangeType::AddNode | ConfChangeType::AddLearnerNode => {
                    // Add this peer to peer_heartbeats.
                    peer_fsm.peer.peer_heartbeats.insert(peer_id, now);
                    if peer_fsm.peer.is_leader() {
                        need_ping = true;
                        peer_fsm.peer.peers_start_pending_time.push((peer_id, now));
                    }
                }
                ConfChangeType::RemoveNode => {
                    // Remove this peer from cache.
                    peer_fsm.peer.peer_heartbeats.remove(&peer_id);
                    if peer_fsm.peer.is_leader() {
                        peer_fsm
                            .peer
                            .peers_start_pending_time
                            .retain(|&(p, _)| p != peer_id);
                    }
                    peer_fsm.peer.remove_peer_from_cache(peer_id);
                    // We only care remove itself now.
                    if self.store.id == store_id {
                        if peer_fsm.peer.peer_id() == peer_id {
                            remove_self = true;
                        } else {
                            panic!(
                                "{} trying to remove unknown peer {:?}",
                                peer_fsm.peer.tag(),
                                peer
                            );
                        }
                    }
                }
            }
        }

        // In pattern matching above, if the peer is the leader,
        // it will push the change peer into `peers_start_pending_time`
        // without checking if it is duplicated. We move `heartbeat_pd` here
        // to utilize `collect_pending_peers` in `heartbeat_pd` to avoid
        // adding the redundant peer.
        if peer_fsm.peer.is_leader() {
            // Notify pd immediately.
            info!(
                "notify pd with change peer region";
                "tag" => peer_fsm.peer.tag(),
                "peer_id" => peer_fsm.peer_id(),
                "region" => ?peer_fsm.peer.region(),
            );
            peer_fsm.peer.heartbeat_pd(self.ctx);

            // Remove or demote leader will cause this raft group unavailable
            // until new leader elected, but we can't revert this operation
            // because its result is already persisted in apply worker
            // TODO: should we transfer leader here?
            let demote_self = is_learner(&peer_fsm.peer.peer);
            if remove_self || demote_self {
                warn!(
                    "Removing or demoting leader";
                    "tag" => peer_fsm.peer.tag(),
                    "peer_id" => peer_fsm.peer_id(),
                    "remove" => remove_self,
                    "demote" => demote_self,
                );
                if demote_self {
                    let term = peer_fsm.peer.term();
                    peer_fsm
                        .peer
                        .raft_group
                        .raft
                        .become_follower(term, raft::INVALID_ID);
                }
                // Don't ping to speed up leader election
                need_ping = false;
            }
        }
        if need_ping {
            // Speed up snapshot instead of waiting another heartbeat.
            peer_fsm.peer.ping();
        }
        peer_fsm
            .peer
            .handle_raft_ready(&mut self.ctx.raft_ctx, None);
        if remove_self {
            drop(peer_fsm);
            self.on_destroy_peer(region_id, None);
        }
        remove_self
    }

    fn maybe_apply(&mut self, region_id: u64) -> Option<u64> {
        if !self.ctx.apply_msgs.msgs.is_empty() {
            Some(region_id)
        } else {
            None
        }
    }

    fn update_region(
        &mut self,
        peer_fsm: &mut PeerFsm,
        mut region: metapb::Region,
        reason: RegionChangeReason,
    ) {
        self.ctx
            .store_meta
            .set_region(region.clone(), &mut peer_fsm.peer, reason);
        for peer in region.take_peers().into_iter() {
            if peer_fsm.peer.peer_id() == peer.get_id() {
                peer_fsm.peer.peer = peer.clone();
            }
            peer_fsm.peer.insert_peer_cache(peer);
        }
    }

    fn on_destroy_peer(&mut self, region_id: u64, merged_target: Option<Region>) {
        let peer = self.get_peer(region_id);
        let mut peer_fsm = peer.peer_fsm.lock().unwrap();
        fail_point!("destroy_peer");
        info!(
            "starts destroy";
            "tag" => peer_fsm.peer.tag(),
            "peer_id" => peer_fsm.peer_id(),
        );
        let region_id = peer_fsm.region_id();
        // We can't destroy a peer which is applying snapshot.
        assert!(!peer_fsm.peer.is_applying_snapshot());

        // Mark itself as pending_remove
        peer_fsm.peer.pending_remove = true;

        let peer_store = peer_fsm.peer.get_store();
        if let Some(parent_id) = peer_store.parent_id() {
            self.ctx.remove_dependent(parent_id, region_id);
        }

        if peer_store.engines.raft.has_dependents(region_id) {
            // A region can't be destroyed until all dependents gone.
            // The peer still exists to prevent the region from moving back.
            info!(
                "delays destroy";
                "tag" => peer_fsm.peer.tag(),
                "peer_id" => peer_fsm.peer_id(),
            );
            peer_fsm.peer.delay_destroy = true;
            peer_fsm.peer.delay_destroy_merged_target = merged_target;
            return;
        }

        // Destroy read delegates.
        self.ctx.store_meta.destroy_region(peer_fsm.peer.region());

        // Trigger region change observer
        self.ctx.global.coprocessor_host.on_region_changed(
            peer_fsm.peer.region(),
            RegionChangeEvent::Destroy,
            peer_fsm.peer.get_role(),
        );
        let keyspace_id = ApiV2::get_u32_keyspace_id_by_key(peer_fsm.peer.region().get_start_key());
        let task = PdTask::DestroyPeer {
            region_id,
            keyspace_id,
        };
        if let Err(e) = self.ctx.global.pd_scheduler.schedule(task) {
            error!(
                "failed to notify pd";
                "tag" => peer_fsm.peer.tag(),
                "peer_id" => peer_fsm.peer_id(),
                "err" => %e,
            );
        }
        if let Err(e) = peer_fsm.peer.destroy(&mut self.ctx.raft_wb, merged_target) {
            // If not panic here, the peer will be recreated in the next restart,
            // then it will be gc again. But if some overlap region is created
            // before restarting, the gc action will delete the overlap region's
            // data too.
            panic!("{} destroy err {:?}", peer_fsm.peer.tag(), e);
        }
        // Some places use `force_send().unwrap()` if the StoreMeta lock is held.
        // So in here, it's necessary to held the StoreMeta lock when closing the
        // router.

        peer_fsm.stop();
        self.ctx.remove_peer(region_id);
        self.ctx.global.engines.kv.remove_shard(region_id);
        self.ctx.global.destroying.insert(region_id);
    }

    fn on_snapshot_ready(&mut self, region_id: u64) -> Option<u64> {
        let peer = self.get_peer(region_id);
        let mut peer_fsm = peer.peer_fsm.lock().unwrap();
        let raft_ctx = &mut self.ctx.raft_ctx;
        let store_meta = &mut self.ctx.store_meta;
        peer_fsm.peer.handle_raft_ready(raft_ctx, Some(store_meta));
        self.maybe_apply(region_id)
    }

    fn on_get_regions_in_range(
        &mut self,
        start: Vec<u8>,
        end: Vec<u8>,
        callback: Box<dyn FnOnce(Vec<RegionIdVer>) + Send>,
    ) {
        let regions = self
            .ctx
            .store_meta
            .region_map
            .scan_regions(start, end, 0, false)
            .into_iter()
            .map(|r| RegionIdVer::from_region(r))
            .collect();
        callback(regions)
    }

    fn on_get_region_by_id(
        &self,
        region_id: u64,
        callback: Box<dyn FnOnce(Option<metapb::Region>) + Send>,
    ) {
        let region_opt = self.ctx.store_meta.region_map.get(region_id).cloned();
        callback(region_opt);
    }

    fn on_sync_region(
        &mut self,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
        reverse: bool,
        callback: Box<dyn FnOnce(SyncRegionResponse) + Send>,
    ) {
        self.ctx
            .global
            .pd_scheduler
            .schedule(PdTask::SyncRegion {
                start,
                end,
                limit,
                reverse,
                callback,
            })
            .unwrap();
    }

    fn on_sync_region_by_id(
        &mut self,
        region_id: u64,
        callback: Box<dyn FnOnce(SyncRegionResponse) + Send>,
    ) {
        self.ctx
            .global
            .pd_scheduler
            .schedule(PdTask::SyncRegionById {
                region_id,
                callback,
            })
            .unwrap();
    }

    fn on_apply_result(&mut self, region_id: u64, peer_id: u64) -> Option<u64> {
        let peer = self.try_get_peer(region_id);
        if peer.is_none() {
            let peer_tag = PeerTag::new(self.store.id, RegionIdVer::new(region_id, 0));
            warn!("{} apply result peer doesn't exist", peer_tag);
            return None;
        }
        let peer = peer.unwrap();
        let (apply_results, is_applying_snapshot) = {
            let mut peer_fsm = peer.peer_fsm.lock().unwrap();
            let current_peer_id = peer_fsm.peer_id();
            if peer_id != current_peer_id {
                let tag = peer_fsm.peer.tag();
                warn!(
                    "{} apply result peer id {} doesn't match current id {}",
                    tag, peer_id, current_peer_id
                );
                return None;
            }
            (
                std::mem::take(&mut peer_fsm.peer.pending_apply_results),
                peer_fsm.peer.is_applying_snapshot(),
            )
        };
        if apply_results.is_empty() || is_applying_snapshot {
            // The apply_results can be empty if a uninitialized peer received gc message
            // and later replaced by split.
            //
            // The apply result is stale if the peer is applying snapshot.
            return None;
        }
        for mut apply_result in apply_results {
            while let Some(result) = apply_result.results.pop_front() {
                match result {
                    ExecResult::ChangePeer(cp) => {
                        if cp.index != raft::INVALID_INDEX {
                            let remove_self = self.on_change_peer(cp);
                            if remove_self {
                                return None;
                            }
                        }
                    }
                    ExecResult::SplitRegion { regions } => {
                        self.on_split_region(regions);
                    }
                    ExecResult::DeleteRange { .. } => {
                        // TODO: clean user properties?
                    }
                    ExecResult::UnsafeDestroy => {
                        self.on_destroy_peer(region_id, None);
                        return None;
                    }
                    ExecResult::PrepareMerge { region } => {
                        self.on_prepare_merge_result(region);
                    }
                    ExecResult::CommitMerge { region, source } => {
                        self.on_commit_merge_result(region, source);
                    }
                    ExecResult::RollbackMerge { region, commit } => {
                        self.on_rollback_merge(region, commit);
                    }
                    ExecResult::RestoreShard { cs } => {
                        self.on_restore_shard_result(cs);
                    }
                }
            }
            {
                let mut peer_fsm = peer.peer_fsm.lock().unwrap();
                peer_fsm
                    .peer
                    .post_apply(&mut self.ctx.raft_ctx, &apply_result);
            }
        }
        let mut peer_fsm = peer.peer_fsm.lock().unwrap();
        peer_fsm
            .peer
            .handle_raft_ready(&mut self.ctx.raft_ctx, None);
        self.maybe_apply(region_id)
    }

    fn on_dependents_empty(&mut self, region_id: u64) {
        let peer = match self.ctx.try_get_peer(region_id) {
            Some(peer) => peer,
            None => return,
        };
        let peer_fsm = peer.peer_fsm.lock().unwrap();
        if !peer_fsm.peer.delay_destroy {
            return;
        }
        assert!(peer_fsm.peer.pending_remove);
        info!(
            "Dependents become empty, continue to destroy peer";
            "tag" => peer_fsm.peer.tag(),
            "peer_id" => peer_fsm.peer_id(),
        );
        let merged_target = peer_fsm.peer.delay_destroy_merged_target.clone();
        drop(peer_fsm);
        let mut applier = peer.applier.lock().unwrap();
        applier.destroy();
        drop(applier);
        self.on_destroy_peer(region_id, merged_target);
    }

    fn on_prepare_merge_request(&mut self, region_id: u64, req: RaftCmdRequest) {
        let peer = match self.ctx.try_get_peer(region_id) {
            Some(peer) => peer,
            None => return,
        };
        let mut peer_fsm = peer.peer_fsm.lock().unwrap();
        let raft_ctx = &mut self.ctx.raft_ctx;
        let store_meta = &mut self.ctx.store_meta;
        let mut handler = PeerMsgHandler::new(&mut peer_fsm, raft_ctx);
        handler.propose_raft_command(req, Callback::None, Some(store_meta));
    }

    fn on_prepare_merge_result(&mut self, region: Region) {
        let peer = match self.ctx.try_get_peer(region.id) {
            Some(peer) => peer,
            None => return,
        };
        let mut peer_fsm = peer.peer_fsm.lock().unwrap();
        let is_leader = peer_fsm.peer.is_leader();
        self.ctx
            .global
            .engines
            .kv
            .set_shard_active(region.id, is_leader);
        self.ctx.store_meta.set_region(
            region,
            &mut peer_fsm.peer,
            RegionChangeReason::PrepareMerge,
        );
        if is_leader {
            peer_fsm.peer.heartbeat_pd(self.ctx);
            info!(
                "notify pd with prepare merge";
                "tag" => peer_fsm.peer.tag(),
                "peer_id" => peer_fsm.peer_id(),
            );
        }
    }

    fn on_check_merge(&mut self, region_id: u64) {
        let peer = match self.ctx.try_get_peer(region_id) {
            Some(peer) => peer,
            None => return,
        };
        let mut peer_fsm = peer.peer_fsm.lock().unwrap();
        let store_meta = &mut self.ctx.store_meta;
        let ctx = &mut self.ctx.raft_ctx;
        let mut handler = PeerMsgHandler::new(&mut peer_fsm, ctx);
        handler.on_check_merge(store_meta);
    }

    fn on_commit_merge_result(&mut self, region: Region, source: Region) {
        let peer = match self.ctx.try_get_peer(region.id) {
            Some(peer) => peer,
            None => return,
        };
        let mut peer_fsm = peer.peer_fsm.lock().unwrap();
        let is_leader = peer_fsm.peer.is_leader();
        self.ctx
            .global
            .engines
            .kv
            .set_shard_active(region.id, is_leader);
        self.ctx.store_meta.set_region(
            region.clone(),
            &mut peer_fsm.peer,
            RegionChangeReason::CommitMerge,
        );
        let tag = peer_fsm.peer.tag();
        if is_leader {
            peer_fsm.peer.heartbeat_pd(self.ctx);
            // Notify pd immediately to let it update the region meta.
            info!(
                "notify pd with commit merge";
                "tag" => tag,
                "peer_id" => peer_fsm.peer_id(),
            );
        }
        drop(peer_fsm);
        let source_id = source.get_id();
        if let Some(source_peer) = self.ctx.try_get_peer(source_id) {
            // The source peer may have dependents, we should not destroy it directly.
            // By sending a gc raft message, the source peer will call maybe_destory to
            // check dependents before destroy.
            let source_peer_fsm = source_peer.peer_fsm.lock().unwrap();
            let deps_empty_or_only_self = source_peer_fsm
                .get_peer()
                .dependents_is_empty_or_only_self(&self.ctx.raft_ctx);
            drop(source_peer_fsm);
            if deps_empty_or_only_self {
                let mut applier = source_peer.applier.lock().unwrap();
                applier.destroy();
            }
            self.on_destroy_peer(source.id, Some(region));
        }
    }

    fn on_rollback_merge(&mut self, region: Region, commit: u64) {
        let peer = match self.ctx.try_get_peer(region.id) {
            Some(peer) => peer,
            None => return,
        };
        let mut peer_fsm = peer.peer_fsm.lock().unwrap();
        self.ctx.store_meta.set_region(
            region,
            &mut peer_fsm.peer,
            RegionChangeReason::RollbackMerge,
        );
        if peer_fsm.peer.is_leader() {
            info!(
                "notify pd with rollback merge";
                "tag" => peer_fsm.peer.tag(),
                "peer_id" => peer_fsm.peer_id(),
                "commit_index" => commit,
            );
            peer_fsm.peer.heartbeat_pd(self.ctx);
        }
    }

    fn on_restore_shard_result(&mut self, cs: kvenginepb::ChangeSet) {
        let region_id = cs.shard_id;
        let ver = cs.shard_ver;

        let mut region = match self.ctx.store_meta.region_map.get(region_id) {
            Some(region) => region.clone(),
            None => return,
        };

        let peer = match self.ctx.try_get_peer(region.id) {
            Some(peer) => peer,
            None => return,
        };
        let mut peer_fsm = peer.peer_fsm.lock().unwrap();
        let tag = peer_fsm.peer.tag();
        debug_assert_eq!(region.get_region_epoch().get_version(), ver, "{}", tag);

        region.mut_region_epoch().set_version(ver + 1);
        info!(
            "{} store_fsm::on_restore_shard_result: set region {:?}",
            tag, region
        );

        let is_leader = peer_fsm.peer.is_leader();
        self.ctx.store_meta.set_region(
            region,
            &mut peer_fsm.peer,
            RegionChangeReason::RestoreShard,
        );

        if is_leader {
            let tag = peer_fsm.peer.tag();
            peer_fsm.peer.heartbeat_pd(self.ctx);
            info!(
                "{} store_fsm::on_restore_shard_result: notify pd, peer_id: {}",
                tag,
                peer_fsm.peer_id(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_region_is_newer() {
        // (current: (ver, conf_ver), new: (ver, conf_ver), expect)
        let cases = vec![
            ((2080, 74240), (2081, 74240), (false, true)),
            ((100, 1001), (100, 1000), (true, true)), // Consider newer when version is equal.
            ((100, 1000), (100, 1000), (true, true)),
        ];

        for (current, new, expect) in cases {
            let current_epoch = metapb::RegionEpoch {
                version: current.0,
                conf_ver: current.1,
                ..Default::default()
            };
            let mut current_region = metapb::Region::new();
            current_region.set_region_epoch(current_epoch);

            let new_epoch = metapb::RegionEpoch {
                version: new.0,
                conf_ver: new.1,
                ..Default::default()
            };
            let mut new_region = metapb::Region::new();
            new_region.set_region_epoch(new_epoch);

            assert_eq!(
                RegionMap::current_region_is_newer(&current_region, &new_region),
                expect.0,
            );
            assert_eq!(
                RegionMap::current_region_is_newer(&new_region, &current_region),
                expect.1,
            );
        }
    }
}
