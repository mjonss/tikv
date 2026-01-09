// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(let_chains)]
#![feature(box_patterns)]

mod error;
mod manifest;
mod preprocessor;
mod util;

use std::{
    cmp,
    collections::{
        hash_map::Entry as HashMapEntry, HashMap as StdHashMap, HashSet as StdHashSet, VecDeque,
    },
    fmt, fs, io, mem, ops,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    time::Duration,
};

use api_version::ApiV2;
use bytes::{Buf, BufMut, Bytes};
use cloud_encryption::MasterKey;
use collections::{HashMap, HashMapExt, HashSet};
pub use error::{Error, Result};
use file_system::{IoRateLimitMode, IoRateLimiter};
use kvengine::{
    dfs::{Dfs, S3Fs},
    ia::util::IaConfig,
    limiter::StoreLimiter,
    table::tiny_meta::{CompactKeeper, MetaPackConfig, MetaPacker},
    IdVer, MetaIterator, Shard, ShardMeta, ShardTag, TERM_KEY,
};
use kvenginepb::ChangeSet;
use kvproto::{
    metapb,
    metapb::Peer,
    raft_cmdpb::{AdminRequest, RaftCmdRequest},
    raft_serverpb::{MergeState, PeerState, RegionLocalState, StoreIdent},
};
use log_wrappers::Value as LogValue;
use native_br::{
    common::{
        collect_snapshot_meta_rlog_files, get_latest_backup_meta, replay_wal_logs_from_backup,
        ReplayWalLogsContext,
    },
    error::Error as BrError,
};
use pd_client::PdClient;
use protobuf::Message;
use raft_proto::{eraftpb, eraftpb::Entry};
use rfengine::{
    iterator::WalIterator, raft_state_key, region_state_key, RaftLogOp, RfEngine, WriteBatch,
    TRUNCATE_ALL_INDEX,
};
use rfenginepb::{ClusterBackupMeta, StoreBackupMeta};
use rfstore::{
    store::{
        get_preprocess_cmd, is_region_initialized, load_last_raft_state_from_wb, rlog,
        state::{RaftApplyState, RaftState},
        write_engine_meta, AffectMemtable, Applier, ApplyContext, ApplyMsgs, MetaChangeListener,
        PdIdAllocator, PeerMsg, PeerTag, PreprocessContext, PreprocessRef, RecoverHandler,
        RegionIdVer, StoreMsg, RAFT_INIT_LOG_INDEX,
    },
    RaftRouter,
};
use security::SecurityConfig;
use serde_derive::{Deserialize, Serialize};
use tikv::config::TikvConfig;
use tikv_util::{
    box_err, box_try,
    config::{AbsoluteOrPercentSize, ReadableDuration, ReadableSize},
    debug, error, info, mpsc,
    time::Instant,
    trace, warn,
};
use txn_types::TimeStamp;

use crate::{
    manifest::{Manifest, UncommittedEntries},
    preprocessor::Preprocessor,
    util::RegionPersistProgress,
};

macro_rules! try_force_stop {
    ($self:ident, $expr:expr) => {{
        #[cfg(feature = "testexport")]
        if $self.ctx.force_stop.get() {
            info!("merged engine force stopped");
            return $expr;
        }
    }};
}

macro_rules! try_force_stop_err {
    ($self:ident) => {{
        try_force_stop!($self, Err(Error::ForceStopped));
    }};
}

// The quorum size when replicas number is 3.
// Used to check whether the Raft log is committed.
const QUORUM_SIZE: u8 = 2;

const ALL_KV_ENGINE_META_KEYS: &[&[u8]] = &[
    rfengine::KV_ENGINE_META_KEY,
    rfengine::KV_ENGINE_META_DIFF_KEY,
    rfengine::KV_ENGINE_META_SNAP_DIFF_KEY,
];

#[derive(Clone)]
pub struct MergedEngineContext {
    pub pd: Arc<dyn PdClient>,
    pub fs: Arc<S3Fs>,
    pub local_dir: PathBuf,
    pub master_key: MasterKey,
    pub config: MergedEngineConfig,
    pub security_config: Arc<SecurityConfig>,

    #[allow(dead_code)]
    pub force_stop: ForceStop, // For test purpose.
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct MergedEngineConfig {
    pub block_cache_size: AbsoluteOrPercentSize,
    pub timeout_fetch_wal: ReadableDuration,
    pub merged_store_id: u64,
    pub mem_table_size: ReadableSize,
    pub raft_write_batch_size: ReadableSize,
    pub force_ia: bool,
    pub get_latest_backup_timeout: ReadableDuration,
    pub meta_pack: MetaPackConfig,
}

impl Default for MergedEngineConfig {
    fn default() -> Self {
        Self {
            block_cache_size: AbsoluteOrPercentSize::Percent(10.0),
            timeout_fetch_wal: ReadableDuration::secs(30),
            merged_store_id: 1024,
            mem_table_size: ReadableSize::mb(128),
            raft_write_batch_size: ReadableSize::mb(4),
            force_ia: true,
            get_latest_backup_timeout: ReadableDuration::minutes(30),
            meta_pack: MetaPackConfig::default(),
        }
    }
}

const MAX_PENDING_TARGETS: usize = 8;

/// Target of region progress.
///
/// `ts`: The target timestamp of the corresponding WAL.
/// `log_idx`: The last log index of the region in the corresponding WAL.
///
/// When `commit_index` reaches `log_idx`, the region can safely advance
/// `safe_target_ts` (resolved_ts) to `ts`.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
struct RegionProgressTarget {
    ts: TimeStamp,
    log_idx: u64,
}

#[derive(Clone)]
pub struct RegionProgress {
    pub keyspace_id: u32,
    pub region_id: u64,
    pub entries: HashMap<u64 /* log_index */, RaftLogOpWithCounter>,
    pub synced_index: u64,
    // Use `commit_index()`/`update_commit_index()` to read/write.
    commit_index: u64,
    // Last index of uncommitted raft logs (in `entries`).
    // Note that it's not necessary to be `commit_index <= last_index`.
    last_index: u64,
    persist_progress: RegionPersistProgress,

    safe_target_ts: TimeStamp,
    pending_targets: VecDeque<RegionProgressTarget>,
}

impl fmt::Debug for RegionProgress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegionProgress")
            .field("keyspace", &self.keyspace_id)
            .field("region", &self.region_id)
            .field("entries", &self.entries.len())
            .field("synced", &self.synced_index)
            .field("commit", &self.commit_index)
            .field("last", &self.last_index)
            .field("persist", &self.persist_progress)
            .field("safe_target", &self.safe_target_ts)
            .field("pending_targets", &self.pending_targets)
            .finish()
    }
}

impl RegionProgress {
    pub fn new(keyspace_id: u32, region_id: u64) -> Self {
        Self {
            keyspace_id,
            region_id,
            entries: HashMap::default(),
            synced_index: 0,
            commit_index: 0,
            last_index: 0,
            persist_progress: RegionPersistProgress::default(),
            safe_target_ts: TimeStamp::zero(),
            pending_targets: VecDeque::default(),
        }
    }

    #[inline]
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    #[inline]
    pub fn update_commit_index(&mut self, new_commit_index: u64) -> bool {
        if self.commit_index < new_commit_index {
            self.commit_index = new_commit_index;
            true
        } else {
            false
        }
    }

    #[inline]
    fn update_last_index(&mut self, new_last_index: u64) {
        if self.last_index < new_last_index {
            self.last_index = new_last_index;
        }
    }

    pub fn upsert_entry<F>(&mut self, tag: ShardTag, log_index: u64, term: u32, or_insert: F)
    where
        F: FnOnce() -> RaftLogOpWithCounter,
    {
        use std::cmp::Ordering::{Equal, Greater, Less};

        if let Some(existing_op) = self.entries.get_mut(&log_index) {
            debug_assert_eq!(log_index, existing_op.index);
            match existing_op.term.cmp(&term) {
                Greater => return,
                Equal => {
                    existing_op.inc_counter();
                    if existing_op.counter() >= QUORUM_SIZE && existing_op.index > self.commit_index
                    {
                        self.commit_index = existing_op.index;
                        debug!(
                            "{} upsert_entry: advance commit index: {}",
                            tag, self.commit_index
                        );
                    }
                    return;
                }
                Less => {}
            }
        }
        self.entries.insert(log_index, or_insert());
        self.update_last_index(log_index);
    }

    pub fn update_safe_target_ts(&mut self, tag: ShardTag, target_ts: TimeStamp) -> TimeStamp {
        debug_assert!(self.safe_target_ts <= target_ts);
        if self.safe_target_ts >= target_ts {
            return self.safe_target_ts;
        }

        // All logs are committed.
        if self.commit_index >= self.last_index {
            self.safe_target_ts = target_ts;
            debug!("{} update_safe_target_ts: {:?}", tag, self.safe_target_ts);
            if !self.pending_targets.is_empty() {
                self.pending_targets.clear();
                self.pending_targets.shrink_to_fit();
            }
            return self.safe_target_ts;
        }

        // Find the latest committed log and associated target ts.
        while let Some(pending) = self.pending_targets.front() {
            if self.commit_index >= pending.log_idx {
                self.safe_target_ts = pending.ts;
                debug!("{} update_safe_target_ts: {:?}", tag, self.safe_target_ts);
                let _ = self.pending_targets.pop_front();
            } else {
                break;
            }
        }

        // Append the new target.
        let new_pending = RegionProgressTarget {
            ts: target_ts,
            log_idx: self.last_index,
        };
        match self.pending_targets.back_mut() {
            Some(last) if *last >= new_pending => {}
            Some(last) if last.ts == target_ts => {
                last.log_idx = self.last_index;
            }
            Some(_) | None => {
                if self.pending_targets.len() >= MAX_PENDING_TARGETS {
                    self.pending_targets.pop_front();
                }
                self.pending_targets.push_back(new_pending);
            }
        }

        self.safe_target_ts
    }

    pub fn is_synced(&self) -> bool {
        debug_assert!(
            self.synced_index <= self.commit_index,
            "unexpected progress: {:?}",
            self
        );
        self.synced_index >= self.commit_index
    }

    pub fn data_persisted_log_index(&self) -> u64 {
        self.persist_progress.persisted_idx()
    }
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct StoreProgress {
    pub store_id: u64,
    pub epoch: u32,
    pub offset: u64,
}

impl fmt::Display for StoreProgress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ store: {}, epoch: {}, offset: {} }}",
            self.store_id, self.epoch, self.offset
        )
    }
}

impl PartialOrd for StoreProgress {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        (self.store_id == other.store_id).then(|| {
            self.epoch
                .cmp(&other.epoch)
                .then_with(|| self.offset.cmp(&other.offset))
        })
    }
}

impl Ord for StoreProgress {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl StoreProgress {
    pub(crate) fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u64_le(self.store_id);
        buf.put_u32_le(self.epoch);
        buf.put_u64_le(self.offset);
    }

    pub(crate) fn decode(buf: &mut impl Buf) -> Result<Self> {
        if (buf.remaining()) < 20 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "store progress buffer is too short",
            )
            .into());
        }
        let store_id = buf.get_u64_le();
        let epoch = buf.get_u32_le();
        let offset = buf.get_u64_le();
        Ok(Self {
            store_id,
            epoch,
            offset,
        })
    }
}

struct EmptyMetaIterator {}

impl MetaIterator for EmptyMetaIterator {
    fn iterate<F>(&mut self, _: F) -> kvengine::Result<()>
    where
        F: FnMut(ChangeSet),
    {
        Ok(())
    }

    fn engine_id(&self) -> u64 {
        0
    }
}

pub struct MergedEngine {
    pub ctx: MergedEngineContext,
    manifest: Manifest,
    region_progresses: HashMap<u64, RegionProgress>,
    updated_regions: HashSet<u64>,
    raft: RfEngine,
    pub kv: kvengine::Engine,
    pub recover_handler: RecoverHandler,
    preprocessors: HashMap<u64, Preprocessor>,
    appliers: HashMap<u64, Applier>,
    delay_destroy_regions: HashMap<u64 /* region_id */, u32 /* keyspace_id */>,
    pending_merge_states: HashMap<u64 /* source_region_id */, MergeState>,
    peer_receiver: mpsc::Receiver<(u64, Box<PeerMsg>)>,
    _store_receiver: mpsc::Receiver<StoreMsg>, // applier never send store message.
    router: RaftRouter,
    meta_packer: Option<MetaPacker>,
    closed: bool,
}

impl MergedEngine {
    pub fn new(ctx: MergedEngineContext, backup_meta: Option<ClusterBackupMeta>) -> Result<Self> {
        let merged_dir = ctx.local_dir.join("merged");
        let merged_cfg = rfengine::RfEngineConfig::default();
        let raft = box_try!(RfEngine::open(
            merged_dir.as_path(),
            &merged_cfg,
            None,
            None
        ));
        let merged_store_id = ctx.config.merged_store_id;
        if let Some(store_ident) = rfengine::load_store_ident(&raft) {
            if store_ident.get_store_id() != merged_store_id {
                panic!(
                    "store id mismatch, expect {}, got {}",
                    merged_store_id,
                    store_ident.get_store_id()
                );
            }
        } else {
            let mut store_ident = StoreIdent::new();
            store_ident.set_store_id(merged_store_id);
            rfengine::save_store_ident(&raft, &store_ident);
        }
        raft.set_engine_id(merged_store_id);
        let manifest_dir = ctx.local_dir.join("manifest");
        let mut manifest = box_try!(Manifest::open(&manifest_dir));
        let (region_progresses, tombstone_regions) = if manifest.store_progresses.is_empty() {
            let (region_progresses, store_progresses, backup_meta) =
                box_try!(Self::recover_from_backup(&ctx, backup_meta, &raft));
            manifest.store_progresses = store_progresses;
            // Note: If we persist the manifest and restart before sync to the `backup_ts`,
            // the time span of next target will exceed the
            // `max_wal_target_time_span`. The exceeded duration should be no more
            // that an interval of backup, i.e. 1 min in prod env, and should not
            // be a problem. So we do not handle this condition for easier.
            manifest.synced_target_ts = TimeStamp::new(backup_meta.backup_ts);

            // Note: manifest is not persisted here to avoid saving all entries. If
            // replication worker restart before next loop, we will recover from backup
            // again.
            (region_progresses, HashMap::default())
        } else {
            if manifest.synced_target_ts.is_zero() {
                // For backward compatibility. Then `WalProgressFetcher` will get latest
                // progress from rfengine.
                let now = TimeStamp::now();
                warn!("No synced_target_ts in manifest, use now: {}", now);
                manifest.synced_target_ts = now;
            }

            box_try!(Self::recover_from_merged_raft_engine(
                &raft,
                &manifest.uncommitted_entries
            ))
        };
        let mut pending_merge_states = HashMap::default();
        let mut preprocessors = HashMap::default();
        for (region_id, _) in raft.get_region_peer_map() {
            if region_id == 0 || tombstone_regions.contains_key(&region_id) {
                continue;
            }
            tikv_util::set_current_region(region_id);
            let Some(processor) =
                Preprocessor::new(&raft, merged_store_id, region_id, &ctx.master_key)
            else {
                error!(
                    "{}:{} failed to create preprocessor",
                    merged_store_id, region_id
                );
                debug_assert!(false);
                continue;
            };
            if let Some(merge_state) = processor.pending_merge_state() {
                pending_merge_states.insert(region_id, merge_state.clone());
            }
            preprocessors.insert(region_id, processor);
        }
        let io_rate_limiter = Arc::new(IoRateLimiter::new(IoRateLimitMode::WriteOnly, true, true));
        let store_limiter = Arc::new(StoreLimiter::dummy());
        let mut meta_packer = if ctx.config.meta_pack.enabled {
            let path = ctx.local_dir.join("metas.pack");
            let meta_packer = MetaPacker::new(path, ctx.config.meta_pack.clone())?;
            Some(meta_packer)
        } else {
            None
        };
        let (meta_pack_scheduler, meta_pack_reader) = meta_packer
            .as_mut()
            .map(|x| (x.get_scheduler(), x.take_reader().unwrap()))
            .unzip();
        let mut recover_handler = RecoverHandler::new(raft.clone());
        recover_handler.set_merged_engine(true);
        recover_handler.set_meta_pack(meta_pack_scheduler, meta_pack_reader);
        let mut meta_iter = EmptyMetaIterator {};
        let kv = box_try!(Self::init_kv_engine(
            &ctx,
            io_rate_limiter,
            store_limiter,
            &mut meta_iter,
            recover_handler.clone(),
        ));
        box_try!(tikv_util::init_task_local_sync(|| {
            Self::load_shards_impl(
                &ctx,
                &kv,
                recover_handler.clone(),
                &manifest.keyspace_states,
                Some(&tombstone_regions),
            )
        }));
        kv.set_loaded();

        if let Some(meta_pack) = meta_packer.as_mut() {
            box_try!(meta_pack.start_worker(kv.clone()));
        }

        // Should be invoked after `load_shards_impl` (to setup the dependency).
        let delay_destroy_regions =
            box_try!(Self::destroy_regions_on_startup(&raft, tombstone_regions));

        let (store_sender, store_receiver) = mpsc::unbounded();
        let (peer_sender, peer_receiver) = mpsc::unbounded();
        let router = RaftRouter::new(peer_sender, store_sender);
        Ok(Self {
            ctx,
            manifest,
            region_progresses,
            updated_regions: HashSet::default(),
            raft,
            kv,
            recover_handler,
            preprocessors,
            appliers: HashMap::default(),
            delay_destroy_regions,
            pending_merge_states,
            _store_receiver: store_receiver,
            peer_receiver,
            router,
            meta_packer,
            closed: false,
        })
    }

    fn merged_store_id(&self) -> u64 {
        self.ctx.config.merged_store_id
    }

    fn get_region_tag(&self, region_id: u64, region_ver: u64) -> ShardTag {
        ShardTag::new(self.merged_store_id(), IdVer::new(region_id, region_ver))
    }

    pub fn set_keyspace_states(&mut self, keyspace_id: u32, states: Bytes) -> Result<()> {
        let old = self
            .manifest
            .set_keyspace_states(keyspace_id, states.clone());
        if old == Some(states.clone()) {
            return Ok(()); // no change
        }
        self.manifest.persist()
    }

    pub fn remove_keyspace(&mut self, keyspace_id: u32) {
        let shards = self.kv.remove_keyspace_shards(keyspace_id);
        for shard_id in shards.into_iter().flatten() {
            self.appliers.remove(&shard_id);
        }
        self.manifest.keyspace_states.remove(&keyspace_id);
    }

    pub fn get_keyspaces(&self) -> Vec<u32> {
        self.manifest.keyspace_states.keys().cloned().collect()
    }

    pub fn get_keyspace_states(&self, keyspace_id: u32) -> Option<Bytes> {
        self.manifest.keyspace_states.get(&keyspace_id).cloned()
    }

    pub fn get_router(&self) -> RaftRouter {
        self.router.clone()
    }

    pub fn get_meta_pack_compact_keeper(&self) -> Option<CompactKeeper> {
        self.meta_packer.as_ref().map(|x| x.compact_keeper())
    }

    fn load_shards_impl(
        ctx: &MergedEngineContext,
        kv: &kvengine::Engine,
        mut recoverer: RecoverHandler,
        keyspace_states: &HashMap<u32, Bytes>,
        tombstone_regions: Option<&HashMap<u64 /* region_id */, u32 /* keyspace_id */>>,
    ) -> Result<()> {
        let metas =
            Self::load_shard_metas_impl(ctx, &mut recoverer, keyspace_states, tombstone_regions)?;
        kv.load_shards(metas, recoverer, None)?;
        Ok(())
    }

    fn load_shard_metas_impl(
        ctx: &MergedEngineContext,
        recoverer: &mut RecoverHandler,
        keyspace_states: &HashMap<u32, Bytes>,
        tombstone_regions: Option<&HashMap<u64 /* region_id */, u32 /* keyspace_id */>>,
    ) -> Result<StdHashMap<u64 /* region_id */, ShardMeta>> {
        let engine_id = ctx.config.merged_store_id;
        let mut metas = StdHashMap::default();
        recoverer.iterate(|cs| {
            debug_assert!(cs.has_snapshot());
            if tombstone_regions.is_some_and(|x| x.contains_key(&cs.shard_id)) {
                return;
            }
            let keyspace_id = get_keyspace_id_of_snapshot(cs.get_snapshot());
            if keyspace_states.contains_key(&keyspace_id) {
                let meta = ShardMeta::new(engine_id, &cs);
                info!("{} load shard meta: {:?}", meta.tag(), cs);
                metas.insert(meta.id, meta);
            }
        })?;
        Ok(metas)
    }

    pub fn load_shards(&self, keyspace_states: &HashMap<u32, Bytes>) -> Result<()> {
        Self::load_shards_impl(
            &self.ctx,
            &self.kv,
            self.recover_handler.clone(),
            keyspace_states,
            None,
        )
    }

    pub fn load_keyspace_shard_metas(
        &self,
        keyspace_id: u32,
    ) -> Result<StdHashMap<u64 /* region_id */, ShardMeta>> {
        let mut states = HashMap::with_capacity(1);
        states.insert(keyspace_id, Bytes::new());
        let mut recoverer = self.recover_handler.clone();
        Self::load_shard_metas_impl(&self.ctx, &mut recoverer, &states, None)
    }

    pub fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;

        if let Some(meta_packer) = self.meta_packer.take() {
            meta_packer.stop();
        }
        self.raft.stop_worker(false);
        self.raft.close_writer();
        self.kv.close();
    }

    fn get_latest_backup(ctx: &MergedEngineContext) -> Result<ClusterBackupMeta> {
        let cluster_id = box_try!(ctx.pd.get_cluster_id());
        let runtime = ctx.fs.get_runtime();
        let start_time = Instant::now_coarse();
        while start_time.saturating_elapsed() < ctx.config.get_latest_backup_timeout.0 {
            match runtime.block_on(get_latest_backup_meta(&ctx.fs, cluster_id)) {
                Ok(x) => return Ok(x),
                Err(BrError::MetaNotFound(_)) => {
                    warn!("get_latest_backup: not ready");
                    thread::sleep(Duration::from_secs(5));
                    continue;
                }
                Err(err) => return Err(box_err!(err)),
            }
        }
        Err(box_err!("get_latest_backup: timeout"))
    }

    fn recover_from_backup(
        ctx: &MergedEngineContext,
        backup_meta: Option<ClusterBackupMeta>,
        merged_raft: &RfEngine,
    ) -> Result<(
        HashMap<u64, RegionProgress>,
        HashMap<u64, StoreProgress>,
        ClusterBackupMeta,
    )> {
        let backup_meta = match backup_meta {
            Some(backup_meta) => backup_meta,
            None => box_try!(Self::get_latest_backup(ctx)),
        };

        let merged_store_id = ctx.config.merged_store_id;
        info!("recover_from_backup: {}", backup_meta; "store" => merged_store_id);

        let mut region_progresses = HashMap::default();
        let mut regions_raft_progress = RegionsRaftProgress::default();
        let mut store_progresses = HashMap::default();
        let mut raftdb_paths = Vec::new();
        let mut raft_wb = rfengine::WriteBatch::new();
        for store in backup_meta.get_stores() {
            let store_progress = StoreProgress {
                store_id: store.get_store_id(),
                epoch: store.get_epoch(),
                offset: store.get_offset(),
            };
            store_progresses.insert(store.get_store_id(), store_progress);
            let mut store_config = TikvConfig::default();
            let store_path = ctx.local_dir.join(store.store_id.to_string());
            store_config.storage.data_dir = store_path.to_str().unwrap().to_string();
            store_config.raft_store.raftdb_path =
                store_path.join("raft").to_str().unwrap().to_string();
            store_config.rfengine.lightweight_backup = false;
            let origin = box_try!(Self::setup_raft_engine(ctx, &backup_meta, store));
            raftdb_paths.push(store_config.raft_store.raftdb_path);
            let region_peers_map = origin.get_region_peer_map();
            for (&region_id, &peer_id) in &region_peers_map {
                if region_id == 0 {
                    continue;
                }
                let tag = ShardTag::new(merged_store_id, IdVer::new(region_id, 0));
                let origin_tag = ShardTag::new(store.store_id, IdVer::new(region_id, 0));

                // region state.
                let Some(region_state) = rfstore::store::load_last_peer_state(&origin, peer_id)
                else {
                    let states = origin.get_peer_all_states(peer_id, false);
                    warn!("{} recover_from_backup: no peer state", tag;
                        "origin" => %origin_tag, "peer" => peer_id, "states" => ?states);
                    debug_assert!(false);
                    continue;
                };
                let new_region = region_state.get_region();
                let region_version = new_region.get_region_epoch().get_version();
                let tag = tag.with_region_version(region_version);
                let origin_tag = origin_tag.with_region_version(region_version);

                if peer_is_skippable(&region_state) {
                    info!("{} recover_from_backup: skip peer", tag;
                        "origin" => %origin_tag, "region_state" => ?region_state);
                    continue;
                }
                let keyspace_id = ApiV2::get_u32_keyspace_id_by_key(new_region.get_start_key())
                    .unwrap_or_default();

                // raft state.
                let Some(raft_state) =
                    rfstore::store::load_peer_raft_state(&origin, peer_id, region_version)
                else {
                    let states = origin.get_peer_all_states(peer_id, false);
                    warn!("{} recover_from_backup: no raft state", tag;
                        "origin" => %origin_tag, "peer" => peer_id, "states" => ?states);
                    debug_assert!(false);
                    continue;
                };

                let Some(mut shard_meta) =
                    rfstore::store::load_engine_meta(&origin, store.store_id, peer_id)
                else {
                    let states = origin.get_peer_all_states(peer_id, false);
                    warn!("{} recover_from_backup: no engine meta", tag;
                        "origin" => %origin_tag, "peer" => peer_id, "states" => ?states);
                    debug_assert!(false);
                    continue;
                };
                debug_assert_eq!(shard_meta.range.keyspace_id, keyspace_id);
                shard_meta.engine_id = merged_store_id;

                let region_progress = region_progresses
                    .entry(region_id)
                    .or_insert(RegionProgress::new(keyspace_id, region_id));

                let preprocess_index = raft_state.get_last_preprocessed_index();
                let origin_commit = raft_state.get_commit();
                let origin_truncated_index = origin
                    .get_truncated_index(peer_id)
                    .unwrap_or_default()
                    .max(RAFT_INIT_LOG_INDEX);

                let merged_commit_index = region_progress.commit_index();

                // Fetch entries, and insert them into region progress, so that they
                // will be replayed when commit index advances (during sync_merged).
                // Entries earlier than truncated index of latest peer are also fetched, as they
                // would be required by dependents.
                {
                    let low_idx = origin_truncated_index + 1;
                    let high_idx = raft_state.get_last_index() + 1;
                    debug!(
                        "{} recover_from_backup: fetch entries: [{}, {})",
                        tag, low_idx, high_idx; "origin" => %origin_tag);
                    if low_idx < high_idx {
                        if let Err(err) = fetch_raft_entries_to_region_progress(
                            tag,
                            &origin,
                            peer_id,
                            low_idx,
                            high_idx,
                            region_progress,
                        ) {
                            panic!(
                                "{} recover_from_backup: fetch entries failed, low: {}, high: {}, err: {}, origin: {}",
                                tag, low_idx, high_idx, err, origin_tag
                            );
                        }
                    }
                }

                if merged_commit_index > origin_commit {
                    continue;
                }

                // merge states
                let peer_is_newer = regions_raft_progress.update(region_id, &raft_state);
                if peer_is_newer {
                    write_engine_meta(&mut raft_wb, region_id, &shard_meta);
                    origin.iterate_peer_states(peer_id, false, |k, v| {
                        trace!("{} recover_from_backup", tag;
                            "k" => LogValue::key(k), "v" => LogValue::value(v),
                            "origin" => %origin_tag, "peer" => peer_id,
                        );
                        update_peer_state_without_engine_meta(
                            &mut raft_wb,
                            k,
                            v,
                            merged_store_id,
                            region_id,
                            keyspace_id,
                        );
                        true
                    });

                    // Depends on other region.
                    if let Some(box parent) = &shard_meta.parent
                        && parent.id != region_id
                    {
                        let parent_tag = parent.tag();
                        let parent_peer_id = *region_peers_map.get(&parent.id).unwrap_or_else(|| {
                            panic!("{} recover_from_backup: parent region not found, origin: {}, parent: {}", tag, origin_tag, parent_tag);
                        });
                        // The state value of parent version should be the latest. It's safe to
                        // overwrite.
                        let state_keys = [raft_state_key(parent.ver), region_state_key(parent.ver)];
                        for k in state_keys {
                            let state_val = origin.get_state(parent_peer_id, &k).unwrap_or_else(|| {
                                panic!("{} recover_from_backup: parent state key not found, origin: {}, parent: {}", tag, origin_tag, parent_tag);
                            });
                            update_peer_state_without_engine_meta(
                                &mut raft_wb,
                                &k,
                                &state_val,
                                merged_store_id,
                                parent.id,
                                keyspace_id,
                            );
                        }

                        debug!("{} recover_from_backup: set parent states", tag;
                            "origin" => %origin_tag, "parent" => %parent_tag, "parent_peer" => parent_peer_id);
                    }

                    region_progress.update_commit_index(origin_commit);
                    region_progress.synced_index = preprocess_index;
                }

                if raft_wb.estimated_size() >= ctx.config.raft_write_batch_size.0 as usize {
                    merged_raft
                        .write(mem::take(&mut raft_wb))
                        .expect("raft write");
                }

                debug!(
                    "{} recover_from_backup", tag;
                    "region" => ?region_state, "raft" => ?raft_state,
                    "progress" => ?region_progress,
                    "origin" => %origin_tag, "keyspace" => keyspace_id);
            }
        }

        // Append raft logs to merged rfengine.
        for progress in region_progresses.values_mut() {
            let region_id = progress.region_id;
            let keyspace_id = progress.keyspace_id;

            if !progress.entries.is_empty() {
                let commit_index = progress.commit_index();
                let mut committed_entries = vec![];
                progress.entries.retain(|_, v| {
                    if v.index <= commit_index {
                        committed_entries.push(v.to_entry());
                    }
                    // Retain not-synced, so that they will be replayed when commit index advances
                    // (during sync_merged).
                    v.index > progress.synced_index
                });

                committed_entries.sort_unstable_by_key(|e| e.index);
                for entry in committed_entries {
                    raft_wb.append_raft_log(region_id, region_id, keyspace_id, &entry);
                }
            }

            // Always truncate raft log to properly set truncated index.
            raft_wb.truncate_raft_log(region_id, region_id, keyspace_id, RAFT_INIT_LOG_INDEX);
            if raft_wb.estimated_size() >= ctx.config.raft_write_batch_size.0 as usize {
                merged_raft
                    .write(mem::take(&mut raft_wb))
                    .expect("raft write");
            }
        }

        if !raft_wb.is_empty() {
            merged_raft.write(raft_wb).expect("raft write");
        }

        // destroy original raft engines
        for raftdb_path in raftdb_paths {
            let raft_path = Path::new(&raftdb_path);
            // clean up dir
            if let Err(e) = std::fs::remove_dir_all(raft_path) {
                warn!("remove raft path failed: {:?}", e; "path" => ?raft_path);
            }
        }
        Ok((region_progresses, store_progresses, backup_meta))
    }

    fn recover_from_merged_raft_engine(
        merged_raft: &RfEngine,
        uncommitted_entries: &UncommittedEntries,
    ) -> Result<(
        HashMap<u64, RegionProgress>,
        HashMap<u64, u32>, // tombstone_regions
    )> {
        let merged_store_id = merged_raft.get_engine_id();
        let mut region_progresses = HashMap::default();
        let mut tombstone_regions = HashMap::default();

        // Get region progresses from RfEngine.
        let region_peers_map = merged_raft.get_region_peer_map();
        for (region_id, peer_id) in region_peers_map {
            if region_id == 0 {
                continue;
            }
            let tag = ShardTag::new(merged_store_id, IdVer::new(region_id, 0));

            let Some(region_state) = rfstore::store::load_last_peer_state(merged_raft, peer_id)
            else {
                // The region is destroyed.
                let states = merged_raft.get_peer_all_states(peer_id, false);
                info!("{} recover_from_merged_raft_engine: no peer state, skip", tag;
                        "peer" => peer_id, "states" => ?states);
                continue;
            };
            let region_version = region_state.get_region().get_region_epoch().get_version();
            let tag = tag.with_region_version(region_version);
            let keyspace_id =
                ApiV2::get_u32_keyspace_id_by_key(region_state.get_region().get_start_key())
                    .unwrap_or_default();

            if region_state.state == PeerState::Tombstone {
                // The dependency is not setup yet. So tombstone regions should not be skipped
                // here.
                debug!("{} recover_from_merged_raft_engine: tombstone region", tag;
                    "region_state" => ?region_state);
                tombstone_regions.insert(region_id, keyspace_id);
            }
            if !is_region_initialized(region_state.get_region()) {
                // Should not happen, not initialized region is skipped during
                // `recover_from_backup` & `update_wal`.
                error!("{} recover_from_merged_raft_engine: region not initialized", tag;
                    "region_state" => ?region_state);
                debug_assert!(false);
                return Err(box_err!("region not initialized: {}", region_id));
            }

            let Some(raft_state) =
                rfstore::store::load_peer_raft_state(merged_raft, peer_id, region_version)
            else {
                let states = merged_raft.get_peer_all_states(peer_id, false);
                warn!("{} recover_from_merged_raft_engine: no raft state", tag;
                        "peer" => peer_id, "states" => ?states);
                debug_assert!(false);
                continue;
            };

            let region_progress = region_progresses
                .entry(region_id)
                .or_insert(RegionProgress::new(keyspace_id, region_id));
            region_progress.update_commit_index(raft_state.get_commit());
            region_progress.synced_index = raft_state.get_last_preprocessed_index();
            region_progress.persist_progress.reset(
                merged_raft
                    .get_truncated_index(region_id)
                    .unwrap_or(RAFT_INIT_LOG_INDEX),
            );
            if let Some(entries) = uncommitted_entries.get_region_entries(region_id) {
                region_progress.entries = entries.clone();
            }
            if let Some(last_index) = region_progress
                .entries
                .iter()
                .map(|(&log_idx, _)| log_idx)
                .max()
            {
                region_progress.update_last_index(last_index);
            }

            if region_progress.commit_index() > region_progress.synced_index {
                // Fetch committed entries for `sync_merged`.
                let low_idx = region_progress.synced_index + 1;
                let high_idx = region_progress.commit_index() + 1;
                debug!(
                    "{} recover_from_merged_raft_engine: fetch committed entries: [{}, {})",
                    tag, low_idx, high_idx; "progress" => ?region_progress);
                if let Err(err) = fetch_raft_entries_to_region_progress(
                    tag,
                    merged_raft,
                    peer_id,
                    low_idx,
                    high_idx,
                    region_progress,
                ) {
                    panic!(
                        "{} recover_from_merged_raft_engine: fetch raft entries failed, low: {}, high: {}, err: {}",
                        tag, low_idx, high_idx, err
                    );
                }
            }

            debug!(
                "{} recover_from_merged_raft_engine", tag;
                "region" => ?region_state,
                "raft_state" => ?raft_state,
                "progress" => ?region_progress,
                "keyspace" => keyspace_id,
            );
        }

        Ok((region_progresses, tombstone_regions))
    }

    fn setup_raft_engine(
        ctx: &MergedEngineContext,
        backup_meta: &ClusterBackupMeta,
        store: &StoreBackupMeta,
    ) -> Result<RfEngine> {
        let mut store_config = TikvConfig::default();
        let store_path = ctx.local_dir.join(store.store_id.to_string());
        store_config.storage.data_dir = store_path.to_str().unwrap().to_string();
        store_config.raft_store.raftdb_path = store_path.join("raft").to_str().unwrap().to_string();
        store_config.rfengine.lightweight_backup = false;
        let store_id = store.get_store_id();
        let rlog_files = collect_snapshot_meta_rlog_files(
            ctx.fs.clone(),
            &ctx.fs.get_prefix(),
            backup_meta,
            store_id,
            None,
        )?;
        rfengine::lightweight_restore(
            store_id,
            None,
            Path::new(&store_config.raft_store.raftdb_path),
            rlog_files.snap_epoch,
            rlog_files.snap_meta,
            rlog_files.snap_rlog,
            store_config.rfengine.epoch_rotate_len,
        )?;
        let raft_db_path = Path::new(&store_config.raft_store.raftdb_path);
        let data_dir = Path::new(&store_config.storage.data_dir);
        let rf_engine = RfEngine::open(raft_db_path, &store_config.rfengine, Some(data_dir), None)?;
        let cache_dir = store_path.join("cache");
        box_try!(fs::create_dir_all(&cache_dir));
        let ctx = ReplayWalLogsContext {
            pd_client: ctx.pd.clone(),
            dfs: ctx.fs.clone(),
            store_id,
            cluster_backup: backup_meta,
            rf_engine: &rf_engine,
            complete_wal_chunks: false,
            full_restore: false,
            fetch_wal_timeout: ctx.config.timeout_fetch_wal.0,
            cache_dir: Some(cache_dir),
            wal_chunks_cache: None,
            from_archive: false,
        };
        let tag = &format!("merged_{}", store_id);
        replay_wal_logs_from_backup(tag, &ctx, rlog_files.snap_epoch)?;
        Ok(rf_engine)
    }

    fn init_kv_engine(
        ctx: &MergedEngineContext,
        rate_limiter: Arc<IoRateLimiter>,
        store_limiter: Arc<StoreLimiter>,
        meta_iter: &mut impl kvengine::MetaIterator,
        recoverer: impl kvengine::RecoverHandler + 'static,
    ) -> Result<kvengine::Engine> {
        let kv_engine_path = ctx.local_dir.join("db");
        if !kv_engine_path.exists() {
            fs::create_dir_all(&kv_engine_path)?;
        }
        let mut kv_opts = kvengine::Options::default();
        kv_opts.local_dirs = vec![kv_engine_path];
        kv_opts.max_mem_table_size = ctx.config.mem_table_size.0;
        kv_opts.max_block_cache_size = ctx.config.block_cache_size.as_memory_size() as i64;
        kv_opts.for_restore = true;
        if ctx.config.force_ia {
            kv_opts.ia = IaConfig {
                mem_cap: AbsoluteOrPercentSize::Percent(10.0),
                disk_cap: AbsoluteOrPercentSize::Percent(60.0),
                dynamic_capacity: false,
                force_ia: true,
                ..Default::default()
            };
        }
        let kv_conf = kvengine::KvEngineConfig::default();
        let opts = Arc::new(kv_opts);
        let id_allocator = Arc::new(PdIdAllocator::new(ctx.pd.clone()));
        let (sender, _) = mpsc::unbounded();
        let meta_change_listener = Box::new(MetaChangeListener { sender });
        let kv_engine = kvengine::Engine::open(
            ctx.fs.clone(),
            opts,
            kv_conf,
            meta_iter,
            recoverer,
            id_allocator,
            meta_change_listener,
            rate_limiter,
            store_limiter,
            None,
            ctx.master_key.clone(),
            ctx.pd.get_security_mgr(),
            false,
        )?;
        kv_engine.set_engine_id(ctx.config.merged_store_id);
        Ok(kv_engine)
    }

    pub fn get_kv(&self) -> kvengine::Engine {
        self.kv.clone()
    }

    pub fn get_raft(&self) -> RfEngine {
        self.raft.clone()
    }

    pub fn get_region_progress(&self, region_id: u64) -> Option<&RegionProgress> {
        self.region_progresses.get(&region_id)
    }

    pub fn mut_region_progress(&mut self, region_id: u64) -> Option<&mut RegionProgress> {
        self.region_progresses.get_mut(&region_id)
    }

    pub fn get_store_progress(&self, store_id: u64) -> Option<StoreProgress> {
        self.manifest.store_progresses.get(&store_id).cloned()
    }

    pub fn get_or_insert_store_progress(&mut self, store_id: u64) -> StoreProgress {
        *self
            .manifest
            .store_progresses
            .entry(store_id)
            .or_insert_with(|| StoreProgress {
                store_id,
                epoch: 1,
                offset: 0,
            })
    }

    pub fn update_store_progress(&mut self, store_id: u64, epoch: u32, offset: u64) {
        self.manifest.update_store_progress(store_id, epoch, offset);
    }

    pub fn get_synced_target_ts(&self) -> TimeStamp {
        self.manifest.synced_target_ts
    }

    pub fn get_keyspace_regions(&self, keyspace_id: u32) -> Vec<u64> {
        let mut regions = Vec::new();
        for (&region_id, progress) in &self.region_progresses {
            if progress.keyspace_id == keyspace_id {
                regions.push(region_id);
            }
        }
        regions
    }

    pub fn update_wal<R: io::Read>(
        &mut self,
        store_id: u64,
        epoch_id: u32,
        start_off: u64,
        end_off: u64,
        reader: R,
    ) -> Result<()> {
        let merged_store_id = self.merged_store_id();
        if let Some(store_progress) = self.manifest.store_progresses.get(&store_id) {
            if store_progress.epoch != epoch_id || store_progress.offset != start_off {
                let err_msg = format!(
                    "store {} expect ({}, {}), got ({}, {}), end_off: {}",
                    store_id,
                    epoch_id,
                    start_off,
                    store_progress.epoch,
                    store_progress.offset,
                    end_off,
                );
                error!("{}", &err_msg);
                debug_assert!(false);
                return Err(Error::StoreProgressMismatch(err_msg));
            }
        } else {
            return Err(Error::StoreProgressNotFound(store_id));
        };
        let mut wal_iterator = WalIterator::new_from_reader(reader, epoch_id, start_off);
        let mut origin_batches = Vec::new();
        wal_iterator.iterate_write_batch(|origin_wb| {
            origin_batches.push(origin_wb);
        })?;
        for origin_wb in origin_batches {
            let region_peer_map = origin_wb.get_region_peer_map();
            for (&region_id, &peer_id) in &region_peer_map {
                if region_id == 0 {
                    continue;
                }
                tikv_util::set_current_region(region_id);
                let tag = ShardTag::new(merged_store_id, IdVer::new(region_id, 0));
                let origin_tag = ShardTag::new(store_id, IdVer::new(region_id, 0));

                let progress = match self.region_progresses.entry(region_id) {
                    HashMapEntry::Occupied(e) => e.into_mut(),
                    HashMapEntry::Vacant(e) => {
                        let Some(region_state) = origin_wb.get_latest_peer_state(peer_id) else {
                            info!("{} update_wal: no peer state in wb", tag; "origin" => %origin_tag);
                            continue;
                        };

                        if peer_is_skippable(&region_state) {
                            info!("{} update_wal: skip peer", tag;
                                "origin" => %origin_tag, "region_state" => ?region_state);
                            continue;
                        }
                        let new_region = region_state.get_region();
                        let keyspace_id =
                            ApiV2::get_u32_keyspace_id_by_key(new_region.get_start_key())
                                .unwrap_or_default();
                        debug!(
                            "{} update_wal: new region: {:?}", tag, region_state;
                            "origin" => %origin_tag, "keyspace" => keyspace_id);
                        e.insert(RegionProgress::new(keyspace_id, region_id))
                    }
                };
                if progress.data_persisted_log_index() == TRUNCATE_ALL_INDEX {
                    debug!("{} update_wal: truncate all", tag; "origin" => %origin_tag);
                    continue;
                }
                if let Some(raft_state) = load_last_raft_state_from_wb(&origin_wb, peer_id) {
                    let origin_commit = raft_state.get_commit();
                    if progress.update_commit_index(origin_commit) {
                        debug!(
                            "{} update_wal: advance commit index {}",
                            tag, progress.commit_index; "origin" => %origin_tag);
                    }
                }
                origin_wb.read_peer_logs(peer_id, |logs| {
                    for log_op in logs {
                        debug!("{} update_wal: insert log index {}", tag, log_op.index; "origin" => %origin_tag);
                        progress.upsert_entry(tag, log_op.index, log_op.term, || log_op.clone().into());
                    }
                });
                self.updated_regions.insert(region_id);
            }
        }
        self.manifest
            .update_store_progress(store_id, epoch_id, end_off);
        Ok(())
    }

    pub fn rotate_wal(&mut self, store_id: u64, epoch_id: u32, offset: u64) -> Result<()> {
        if let Some(store_progress) = self.manifest.store_progresses.get_mut(&store_id) {
            if store_progress.epoch != epoch_id || store_progress.offset != offset {
                return Err(Error::StoreProgressMismatch(format!(
                    "store {} expect epoch {}, offset {}, got epoch {}, offset {}",
                    store_id, epoch_id, offset, store_progress.epoch, store_progress.offset
                )));
            }
            info!(
                "rotate store {} at epoch {}, offset {}",
                store_id, epoch_id, offset
            );
            store_progress.epoch += 1;
            store_progress.offset = 0;
        } else {
            return Err(Error::StoreProgressNotFound(store_id));
        }
        Ok(())
    }

    pub fn sync_merged(
        &mut self,
        apply_ctx: &mut ApplyContext,
        synced_target_ts: Option<TimeStamp>,
    ) -> Result<()> {
        // Prepare context.
        let mut raft_wb = rfengine::WriteBatch::new();
        let mut remove_dependents = Vec::new();
        let mut apply_msgs = ApplyMsgs::default();
        let raft_cfg = rfstore::store::Config::default();
        let mut destroying = StdHashSet::default();
        let raft_engine = self.raft.clone();
        let router = self.router.clone();
        let pre_ctx = PreprocessContext {
            store_id: self.merged_store_id(),
            kv: None,
            raft: &raft_engine,
            raft_wb: &mut raft_wb,
            remove_dependents: &mut remove_dependents,
            apply_msgs: &mut apply_msgs,
            cfg: &raft_cfg,
            router: Some(&router),
            destroying: &mut destroying,
        };
        let mut ctx = SyncRegionsContext {
            pre_ctx,
            apply_ctx,
            prepared_msgs: HashMap::default(),
            destroyed_regions: HashMap::default(),
        };
        let updated_regions: Vec<u64> = self.updated_regions.drain().collect();
        self.sync_merged_with_ctx(&mut ctx, updated_regions, synced_target_ts)
            .map_err(|e| {
                debug!("sync_merged: clear context on error: {:?}", ctx; "err" => ?e);
                ctx.clear();
                e
            })
    }

    fn sync_merged_with_ctx(
        &mut self,
        ctx: &mut SyncRegionsContext<'_>,
        updated_regions: Vec<u64>,
        synced_target_ts: Option<TimeStamp>,
    ) -> Result<()> {
        self.sync_merged_for_regions(ctx, &updated_regions)?;
        self.handle_prepared_msgs(ctx);
        self.remove_dependents(ctx);
        self.update_progress_and_truncate(&updated_regions, ctx.raft_wb);
        self.destroy_regions(ctx);
        if !ctx.raft_wb.is_empty() {
            try_force_stop_err!(self);
            self.raft.write(mem::take(ctx.raft_wb)).expect("raft write");
        }
        self.manifest
            .update_region_progresses(&self.region_progresses);
        if let Some(synced_target_ts) = synced_target_ts {
            self.manifest.update_synced_target_ts(synced_target_ts);
        }
        try_force_stop_err!(self);
        self.manifest.persist().expect("persist manifest");
        Ok(())
    }

    fn sync_merged_for_regions(
        &mut self,
        ctx: &mut SyncRegionsContext<'_>,
        updated_regions: &[u64],
    ) -> Result<()> {
        let merged_store_id = self.merged_store_id();
        info!("sync merged for regions {:?}", updated_regions; "store" => merged_store_id);
        let mut update_queue = VecDeque::from(updated_regions.to_vec());
        let mut merged_wb = rfengine::WriteBatch::new();
        let mut merged_wb_estimated_size = 0;
        let mut continuous_postpone_count = 0;
        while let Some(updated_region) = update_queue.pop_front() {
            try_force_stop_err!(self);
            tikv_util::set_current_region(updated_region);
            let res = self.sync_region(
                ctx,
                updated_region,
                &mut merged_wb,
                &mut merged_wb_estimated_size,
            )?;
            match &res {
                SyncRegionResult::Finished => {}
                SyncRegionResult::Postponed => {
                    update_queue.push_back(updated_region);
                }
                SyncRegionResult::Resume => {
                    update_queue.push_front(updated_region);
                }
                SyncRegionResult::Dropped => {}
            }
            if res.is_postponed() {
                continuous_postpone_count += 1;
                if continuous_postpone_count >= update_queue.len() * 2 {
                    break;
                }
            } else {
                continuous_postpone_count = 0;
            }
        }
        for postponed_region in update_queue {
            debug!(
                "{}:{} sync_merged: region postponed to next round",
                merged_store_id, postponed_region
            );
            self.updated_regions.insert(postponed_region);
        }

        if !merged_wb.is_empty() {
            self.raft.persist(merged_wb).expect("raft persist");
        }
        Ok(())
    }

    fn sync_region(
        &mut self,
        ctx: &mut SyncRegionsContext<'_>,
        updated_region: u64,
        merged_wb: &mut rfengine::WriteBatch,
        merged_wb_estimated_size: &mut usize,
    ) -> Result<SyncRegionResult> {
        let merged_store_id = self.merged_store_id();
        let mut tag = PeerTag::new(merged_store_id, RegionIdVer::new(updated_region, 0));

        if self.raft.get_truncated_index(updated_region).is_none() {
            // region is newly inserted, should process parent first.
            info!("{} sync_merged: region is newly inserted", tag);
            return Ok(SyncRegionResult::Postponed);
        }

        let progress = self.region_progresses.get_mut(&updated_region).unwrap();
        let low = progress.synced_index.max(RAFT_INIT_LOG_INDEX) + 1;
        let high: u64 = progress.commit_index() + 1;
        debug!("{} sync_merged: [{}, {})", tag, low, high);
        if low >= high {
            return Ok(SyncRegionResult::Finished);
        }
        let preprocessor = match self.preprocessors.entry(updated_region) {
            HashMapEntry::Occupied(e) => e.into_mut(),
            HashMapEntry::Vacant(e) => {
                let Some(preprocessor) = Preprocessor::new(
                    &self.raft,
                    ctx.store_id,
                    updated_region,
                    &self.ctx.master_key,
                ) else {
                    info!("{} sync_merged: region is merged or destroyed", tag);
                    return Ok(SyncRegionResult::Dropped);
                };
                e.insert(preprocessor)
            }
        };
        let mut preprocessor_ref = preprocessor.as_ref();
        tag = preprocessor_ref.tag();
        let mut entries = Vec::new();
        let mut wb_encoded_len = 0;
        let mut res = SyncRegionResult::Finished;
        // preprocess entries.
        for log_index in low..high {
            try_force_stop_err!(self);
            let Some(raft_log) = progress.entries.get(&log_index) else {
                // Ref: https://github.com/tidbcloud/cloud-storage-engine/issues/3654
                info!("{} sync_region: no raft log: {}", tag, log_index;
                    "progress" => ?progress);
                break;
            };
            let mut entry = raft_log.to_entry();
            let mut preprocess_cmd = update_entry(&mut entry, merged_store_id);
            let mut admin_req = preprocess_cmd
                .as_mut()
                .and_then(|req| req.has_admin_request().then(|| req.take_admin_request()));
            if let Some(admin) = admin_req.as_ref() {
                match Self::sync_region_admin_req(
                    tag,
                    updated_region,
                    admin,
                    log_index,
                    &mut self.pending_merge_states,
                ) {
                    SyncRegionResult::Finished => {}
                    SyncRegionResult::Postponed => {
                        res = SyncRegionResult::Postponed;
                        break;
                    }
                    _ => unreachable!(),
                }
            }
            let err = preprocessor_ref.preprocess_committed_entry(ctx, &entry);
            if let Some(err) = err {
                warn!("{} preprocess committed entry failed: {:?}", tag, err);
                // clear failed command.
                admin_req = None;
                entry.set_data(Bytes::new());
            } else if let Some(admin) = admin_req.as_ref() {
                Self::sync_region_admin_req_post_preprocess(
                    tag,
                    updated_region,
                    admin,
                    log_index,
                    &preprocessor_ref,
                    &mut self.pending_merge_states,
                );
            }
            preprocessor_ref
                .raft_state
                .set_last_preprocessed_index(*preprocessor_ref.preprocessed_index);
            let mut hs = eraftpb::HardState::default();
            hs.set_term(1);
            hs.set_vote(updated_region);
            hs.set_commit(log_index);
            preprocessor_ref.raft_state.set_hard_state(&hs);
            preprocessor_ref.raft_state.set_last_index(log_index);
            let keyspace_id =
                ApiV2::get_u32_keyspace_id_by_key(preprocessor_ref.region.get_start_key())
                    .unwrap_or_default();
            wb_encoded_len +=
                ctx.raft_wb
                    .append_raft_log(updated_region, updated_region, keyspace_id, &entry);
            if let Some(admin_req) = admin_req {
                if admin_req.has_commit_merge() {
                    let last_change_set = ctx.apply_msgs.get_last_change_set();
                    let source = last_change_set.unwrap();
                    ctx.destroyed_regions.insert(source.shard_id, keyspace_id);
                }
            }
            let entry_is_empty = entry.get_data().is_empty();
            entries.push(entry);

            progress.synced_index = log_index;

            let base_version = preprocessor_ref.shard_meta.as_ref().map(|m| m.base_version);
            Self::update_persist_progress(
                tag,
                progress,
                entry_is_empty,
                preprocess_cmd.as_ref(),
                log_index,
                base_version,
            );

            if log_index + 1 < high
                && wb_encoded_len >= self.ctx.config.raft_write_batch_size.0 as i64
            {
                debug!("{} sync_region: wb exceed size limit, break at {}", tag, log_index;
                    "wb_size" => wb_encoded_len, "low" => low, "high" => high);
                res = SyncRegionResult::Resume;
                break;
            }
        }
        if ctx.raft_wb.is_empty() {
            return Ok(res);
        }
        preprocessor_ref.write_raft_state(ctx);
        let wb = mem::take(ctx.raft_wb);
        ctx.raft.apply(&wb);
        *merged_wb_estimated_size += wb.estimated_size();
        merged_wb.merge_write_batch(wb);
        if *merged_wb_estimated_size > self.ctx.config.raft_write_batch_size.0 as usize {
            ctx.raft.persist(mem::take(merged_wb)).expect("raft write");
            *merged_wb_estimated_size = 0;
        }
        let shard = self.kv.get_shard(updated_region);
        if shard.is_none() {
            // shard is not in the keyspace range, skip apply.
            debug!("{} sync_merged_for_regions: skip apply", tag);
            preprocessor.sync_region();
            ctx.skip_apply();
            return Ok(res);
        }
        let shard = shard.unwrap();
        // apply committed entries.
        let applier = self
            .appliers
            .entry(updated_region)
            .or_insert_with(|| Self::new_applier(&shard, preprocessor_ref, low - 1));
        ctx.build_apply_msg_for_replication(entries);
        ctx.pre_ctx
            .handle_apply_msgs_for_replication(applier, ctx.apply_ctx);
        // We keep waiting for paused region because later region may depend on it.
        while applier.is_paused() {
            try_force_stop_err!(self);
            let msgs = if let Some(msgs) = ctx.prepared_msgs.remove(&updated_region) {
                // received by previous region, handle it now.
                msgs
            } else {
                let (id, peer_msg) = match self.peer_receiver.recv_timeout(Duration::from_secs(3)) {
                    Ok((id, msg)) => (id, msg),
                    Err(err) => {
                        if err.is_timeout() {
                            warn!("{} waiting for region to unpause", tag);
                            continue;
                        }
                        return Err(Error::Other(Box::new(err)));
                    }
                };
                if id != updated_region {
                    // For another region, handle it later.
                    ctx.prepared_msgs
                        .entry(id)
                        .or_default()
                        .push((id, peer_msg));
                    continue;
                }
                vec![(id, peer_msg)]
            };
            Self::apply_prepared_msgs(ctx, applier, msgs);
        }
        preprocessor.sync_region();

        Ok(res)
    }

    fn sync_region_admin_req(
        tag: PeerTag,
        updated_region: u64,
        admin: &AdminRequest,
        log_index: u64,
        pending_merge_states: &mut HashMap<u64 /* source_region_id */, MergeState>,
    ) -> SyncRegionResult {
        if admin.has_commit_merge() {
            let commit_merge = admin.get_commit_merge();
            let source = commit_merge.get_source();
            let source_ready = if let HashMapEntry::Occupied(e) =
                pending_merge_states.entry(source.id)
            {
                let merge_state = e.get();
                let matched = merge_state.get_target().id == updated_region
                    && merge_state.commit == commit_merge.commit;
                if matched {
                    debug!("{} sync_region: commit merge at {}", tag, log_index;
                        "source" => source.id, "commit" => commit_merge.commit);
                    e.remove();
                } else {
                    debug!("{} sync_region: commit merge not match", tag;
                        "source" => source.id, "commit" => commit_merge.commit, "merge_state" => ?merge_state);
                }
                matched
            } else {
                false
            };
            if !source_ready {
                // need to process source region first.
                info!("{} sync_region: commit merge postponed at {}", tag, log_index;
                    "source" => source.id, "commit" => commit_merge.commit);
                return SyncRegionResult::Postponed;
            }
        }
        SyncRegionResult::Finished
    }

    fn sync_region_admin_req_post_preprocess(
        tag: PeerTag,
        updated_region: u64,
        admin: &AdminRequest,
        log_index: u64,
        preprocessor_ref: &PreprocessRef<'_>,
        pending_merge_states: &mut HashMap<u64 /* source_region_id */, MergeState>,
    ) {
        if admin.has_prepare_merge() {
            if let Some(merge_state) = preprocessor_ref.pending_merge_state.as_ref() {
                debug!("{} sync_region: prepare merge at {}", tag, log_index; "merge_state" => ?merge_state);
                pending_merge_states.insert(updated_region, merge_state.clone());
            } else {
                // Prepare merge is denied. Ref: `preprocess_prepare_merge`.
                debug!("{} sync_region: skip prepare merge at {}", tag, log_index);
            }
        } else if admin.has_rollback_merge() {
            if let Some(merge_state) = preprocessor_ref.pending_merge_state.as_ref() {
                // Should not happen. Rollback merge must succeed.
                // Ref: `preprocess_rollback_merge`.
                warn!("{} sync_region: skip rollback merge at {}", tag, log_index; "merge_state" => ?merge_state);
                debug_assert!(false);
            } else {
                let origin = pending_merge_states.remove(&updated_region);
                debug!("{} sync_region: rollback merge at {}", tag, log_index; "merge_state" => ?origin);
                debug_assert!(origin.is_some());
            }
        }
    }

    fn handle_prepared_msgs(&mut self, ctx: &mut SyncRegionsContext<'_>) {
        let msg_count = self.peer_receiver.len();
        for _ in 0..msg_count {
            let (id, peer_msg) = self.peer_receiver.recv().unwrap();
            ctx.prepared_msgs
                .entry(id)
                .or_default()
                .push((id, peer_msg));
        }
        let prepared_msgs = mem::take(&mut ctx.prepared_msgs);
        for (region_id, msgs) in prepared_msgs {
            tikv_util::set_current_region(region_id);
            let Some(applier) = self.appliers.get_mut(&region_id) else {
                continue;
            };
            Self::apply_prepared_msgs(ctx, applier, msgs);
        }
    }

    fn remove_dependents(&mut self, ctx: &mut SyncRegionsContext<'_>) {
        for (parent_id, dependent_id) in mem::take(ctx.remove_dependents) {
            let dependent_len = self.raft.remove_dependent(parent_id, dependent_id);
            if dependent_len == 0 {
                if let Some(keyspace_id) = self.delay_destroy_regions.remove(&parent_id) {
                    ctx.destroyed_regions.insert(parent_id, keyspace_id);
                }
            }
        }
    }

    fn update_persist_progress(
        tag: PeerTag,
        progress: &mut RegionProgress,
        entry_is_empty: bool,
        cmd: Option<&RaftCmdRequest>,
        log_idx: u64,
        base_version: Option<u64>,
    ) {
        debug_assert_eq!(progress.synced_index, log_idx);
        let affect_memtable = get_affect_memtable(tag, entry_is_empty, cmd, base_version);
        progress.persist_progress.update(log_idx, affect_memtable);
    }

    fn update_progress_and_truncate(&mut self, regions: &[u64], raft_wb: &mut WriteBatch) {
        for &region_id in regions {
            tikv_util::set_current_region(region_id);
            let tag = ShardTag::new(self.merged_store_id(), IdVer::new(region_id, 0));
            let progress = self.region_progresses.get_mut(&region_id).unwrap();
            let synced_index = progress.synced_index;
            // We need to keep the not-synced logs for the next round.
            progress.entries.retain(|&index, _| index > synced_index);

            let truncate_index = progress.data_persisted_log_index();
            let truncate_raft_log =
                self.truncate_region_raft_log(tag, region_id, truncate_index, raft_wb);
            debug!(
                "{} update_progress_and_truncate: truncate entries <= {}, raft log <= {:?}",
                tag, synced_index, truncate_raft_log;
                "truncate_idx" => truncate_index,
            );
        }
    }

    fn truncate_region_raft_log(
        &mut self,
        tag: ShardTag,
        region_id: u64,
        mut truncate_index: u64,
        raft_wb: &mut WriteBatch,
    ) -> Option<u64> {
        // Skip truncate region with dependents. The parent region may need the old raft
        // logs on recover.
        if self.raft.has_dependents(region_id) {
            return None;
        }
        let rf_truncated_index = self.raft.get_truncated_index(region_id)?;
        if truncate_index <= rf_truncated_index {
            return None;
        }

        let shard_meta = self.preprocessors.get_mut(&region_id)?.mut_shard_meta()?;
        if shard_meta.data_sequence < truncate_index {
            debug!(
                "{} truncate_region_raft_log: advance data_sequence: {} -> {}",
                tag, shard_meta.data_sequence, truncate_index
            );
            shard_meta.data_sequence = truncate_index;
            write_engine_meta(raft_wb, region_id, shard_meta);
        }

        let persisted_index = shard_meta.data_persisted_log_index();
        truncate_index = cmp::min(truncate_index, persisted_index);
        if truncate_index <= rf_truncated_index {
            return None;
        }
        raft_wb.truncate_raft_log(
            region_id,
            region_id,
            shard_meta.range.keyspace_id,
            truncate_index,
        );
        Some(truncate_index)
    }

    fn destroy_regions(&mut self, ctx: &mut SyncRegionsContext<'_>) {
        for (region_id, keyspace_id) in ctx.destroyed_regions.drain() {
            tikv_util::set_current_region(region_id);

            if self.raft.has_dependents(region_id) {
                info!("{} delay destroy", self.get_region_tag(region_id, 0));
                self.delay_destroy_regions.insert(region_id, keyspace_id);
                continue;
            }

            info!("{} destroy", self.get_region_tag(region_id, 0));
            self.raft.iterate_peer_states(region_id, false, |k, _| {
                ctx.pre_ctx.raft_wb.set_state_bytes(
                    region_id,
                    region_id,
                    keyspace_id,
                    k.clone(),
                    Bytes::new(),
                );
                true
            });
            ctx.pre_ctx.raft_wb.truncate_raft_log(
                region_id,
                region_id,
                keyspace_id,
                TRUNCATE_ALL_INDEX,
            );
            self.remove_shard(region_id);
            self.preprocessors.remove(&region_id);
            self.pending_merge_states.remove(&region_id);
            let progress = self.region_progresses.get_mut(&region_id).unwrap();
            progress.persist_progress.truncate_all();
        }
    }

    pub fn remove_shard(&mut self, region_id: u64) {
        self.appliers.remove(&region_id);
        self.kv.remove_shard(region_id);
    }

    fn destroy_regions_on_startup(
        raft: &RfEngine,
        tombstone_regions: HashMap<u64, u32>,
    ) -> Result<HashMap<u64, u32> /* delay_destroy_regions */> {
        let mut delay_destroy_regions = HashMap::default();
        if tombstone_regions.is_empty() {
            return Ok(delay_destroy_regions);
        }

        let merged_store_id = raft.get_engine_id();
        let get_region_tag = |region_id| ShardTag::new(merged_store_id, IdVer::new(region_id, 0));

        let mut raft_wb = rfengine::WriteBatch::new();
        for (region_id, keyspace_id) in tombstone_regions {
            if raft.has_dependents(region_id) {
                info!("{} delay destroy", get_region_tag(region_id));
                delay_destroy_regions.insert(region_id, keyspace_id);
                continue;
            }

            info!("{} destroy", get_region_tag(region_id));
            raft.iterate_peer_states(region_id, false, |k, _| {
                raft_wb.set_state_bytes(region_id, region_id, keyspace_id, k.clone(), Bytes::new());
                true
            });
            raft_wb.truncate_raft_log(region_id, region_id, keyspace_id, TRUNCATE_ALL_INDEX);
        }

        if !raft_wb.is_empty() {
            raft.write(raft_wb).expect("raft write");
        }
        Ok(delay_destroy_regions)
    }

    fn new_applier(
        shard: &Shard,
        preprocess_ref: PreprocessRef<'_>,
        applied_index: u64,
    ) -> Applier {
        let encryption_key = shard.get_encryption_key();
        let term_val = shard.get_property(TERM_KEY).unwrap();
        let term = term_val.chunk().get_u64_le();
        Applier::new_for_replication(
            preprocess_ref.region.clone(),
            encryption_key,
            RaftApplyState::new(applied_index, term),
        )
    }

    fn apply_prepared_msgs(
        ctx: &mut SyncRegionsContext<'_>,
        applier: &mut Applier,
        msgs: Vec<(u64, Box<PeerMsg>)>,
    ) {
        for (_, msg) in msgs {
            ctx.build_prepared_msg_for_replication(msg);
        }
        ctx.pre_ctx
            .handle_apply_msgs_for_replication(applier, ctx.apply_ctx);
    }
}

enum SyncRegionResult {
    Finished,
    Postponed,
    Resume,
    Dropped,
}

impl SyncRegionResult {
    fn is_postponed(&self) -> bool {
        matches!(self, Self::Postponed)
    }
}

struct SyncRegionsContext<'a> {
    pre_ctx: PreprocessContext<'a>,
    apply_ctx: &'a mut ApplyContext,
    prepared_msgs: HashMap<u64 /* region_id */, Vec<(u64 /* region_id */, Box<PeerMsg>)>>,
    destroyed_regions: HashMap<u64, u32>,
}

impl<'a> ops::Deref for SyncRegionsContext<'a> {
    type Target = PreprocessContext<'a>;

    fn deref(&self) -> &Self::Target {
        &self.pre_ctx
    }
}

impl ops::DerefMut for SyncRegionsContext<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.pre_ctx
    }
}

impl Drop for SyncRegionsContext<'_> {
    fn drop(&mut self) {
        debug_assert!(self.apply_msgs.is_empty());
        debug_assert!(self.prepared_msgs.is_empty());
        debug_assert!(self.destroyed_regions.is_empty());
        debug_assert!(self.raft_wb.is_empty());
    }
}

impl SyncRegionsContext<'_> {
    fn skip_apply(&mut self) {
        self.apply_msgs.clear();
    }

    fn clear(&mut self) {
        self.apply_msgs.clear();
        self.prepared_msgs.clear();
        self.destroyed_regions.clear();
        self.raft_wb.reset();
    }
}

impl fmt::Debug for SyncRegionsContext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SyncRegionsContext")
            .field("apply_msgs", &self.apply_msgs.len())
            .field("prepared_msgs", &self.prepared_msgs.len())
            .field("destroyed_regions", &self.destroyed_regions.len())
            .field("raft_wb", &self.raft_wb.len())
            .finish()
    }
}

fn update_peer_state_without_engine_meta(
    wb: &mut rfengine::WriteBatch,
    k: &Bytes,
    v: &Bytes,
    store_id: u64,
    region_id: u64,
    keyspace_id: u32,
) {
    if k.starts_with(rfengine::REGION_META_KEY_PREFIX) {
        let mut origin_state = kvproto::raft_serverpb::RegionLocalState::new();
        origin_state.merge_from_bytes(v).unwrap();
        let region_version = origin_state.get_region().get_region_epoch().get_version();
        let merged_region_state = merge_region_local_state(&origin_state, store_id);
        let data = merged_region_state.write_to_bytes().unwrap();
        wb.set_state_bytes(
            region_id,
            region_id,
            keyspace_id,
            rfengine::region_state_key(region_version),
            data.into(),
        );
    } else if !ALL_KV_ENGINE_META_KEYS.contains(&k.chunk()) {
        wb.set_state_bytes(region_id, region_id, keyspace_id, k.clone(), v.clone());
    }
}

// Update the entry if needed and return RaftCmdRequest for further processing
// if the entry is normal command.
// Note: Change `get_affect_memtable` if the logic here is changed.
fn update_entry(entry: &mut Entry, merged_store_id: u64) -> Option<RaftCmdRequest> {
    if entry.get_entry_type() != raft_proto::eraftpb::EntryType::EntryNormal {
        // We don't need to handle conf change, set it to empty.
        entry.set_entry_type(raft_proto::eraftpb::EntryType::EntryNormal);
        entry.set_data(Bytes::new());
        return None;
    }
    if entry.get_data().is_empty() {
        return None;
    }
    let mut cmd = get_preprocess_cmd(entry)?;
    if !cmd.has_admin_request() {
        return Some(cmd);
    }
    let header = cmd.mut_header();
    let region_id = header.get_region_id();
    let peer = header.mut_peer();
    peer.set_store_id(merged_store_id);
    peer.set_id(region_id);
    let epoch = header.mut_region_epoch();
    epoch.set_conf_ver(1);
    let admin_cmd = cmd.mut_admin_request();
    if admin_cmd.has_splits() {
        let splits = admin_cmd.mut_splits().mut_requests();
        for req in splits.iter_mut() {
            req.set_new_peer_ids(vec![req.new_region_id]);
        }
    } else if admin_cmd.has_prepare_merge() {
        let prepare_merge = admin_cmd.mut_prepare_merge();
        let new_target = merged_region_meta(prepare_merge.get_target(), merged_store_id);
        prepare_merge.set_target(new_target);
    } else if admin_cmd.has_commit_merge() {
        let commit_merge = admin_cmd.mut_commit_merge();
        let new_source = merged_region_meta(commit_merge.get_source(), merged_store_id);
        commit_merge.set_source(new_source);
    }
    let new_cmd = cmd.write_to_bytes().unwrap();
    entry.set_data(new_cmd.into());
    Some(cmd)
}

// Get affect memtable type of the raft command.
// Depends on the logic of `update_entry`.
fn get_affect_memtable(
    tag: PeerTag,
    entry_is_empty: bool,
    cmd: Option<&RaftCmdRequest>,
    base_version: Option<u64>,
) -> AffectMemtable {
    if entry_is_empty {
        // Conf changes or failed commands.
        return AffectMemtable::None;
    }
    let Some(cmd) = cmd else {
        // Normal writes (no preprocess).
        return AffectMemtable::Write;
    };
    let Some(custom) = rlog::get_custom_log(cmd) else {
        // Admin requests.
        return AffectMemtable::None;
    };
    custom.is_affect_memtable(tag, base_version)
}

// merged region meta has a single peer with id same as region id.
fn merged_region_meta(origin_region: &metapb::Region, store_id: u64) -> metapb::Region {
    let mut merged_peer = Peer::new();
    merged_peer.set_id(origin_region.get_id());
    merged_peer.set_store_id(store_id);
    let mut merged_region = origin_region.clone();
    merged_region.set_peers(vec![merged_peer].into());
    merged_region.mut_region_epoch().set_conf_ver(1);
    merged_region
}

fn merge_region_local_state(
    origin: &kvproto::raft_serverpb::RegionLocalState,
    store_id: u64,
) -> kvproto::raft_serverpb::RegionLocalState {
    let mut merged = origin.clone();
    merged.set_region(merged_region_meta(origin.get_region(), store_id));
    if merged.has_merge_state() {
        let merge_state = merged.get_merge_state();
        if merge_state.has_target() {
            let new_target = merged_region_meta(merge_state.get_target(), store_id);
            merged.mut_merge_state().set_target(new_target);
        }
    }
    merged
}

#[derive(Clone, Debug, PartialEq)]
pub struct RaftLogOpWithCounter {
    pub op: RaftLogOp,
    counter: u8,
}

impl ops::Deref for RaftLogOpWithCounter {
    type Target = RaftLogOp;

    fn deref(&self) -> &Self::Target {
        &self.op
    }
}

impl ops::DerefMut for RaftLogOpWithCounter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.op
    }
}

impl From<RaftLogOp> for RaftLogOpWithCounter {
    fn from(op: RaftLogOp) -> Self {
        RaftLogOpWithCounter { op, counter: 1 }
    }
}

impl RaftLogOpWithCounter {
    pub fn inc_counter(&mut self) {
        self.counter = self.counter.saturating_add(1);
    }

    pub fn counter(&self) -> u8 {
        self.counter
    }
}

fn get_keyspace_id_of_snapshot(snap: &kvenginepb::Snapshot) -> u32 {
    ApiV2::get_u32_keyspace_id_by_key(snap.get_outer_start()).unwrap_or_default()
}

#[cfg(feature = "testexport")]
#[derive(Default, Clone)]
pub struct ForceStop(Arc<std::sync::atomic::AtomicBool>);

#[cfg(feature = "testexport")]
impl ForceStop {
    pub fn set(&self) {
        use std::sync::atomic::Ordering;
        self.0.store(true, Ordering::Relaxed);
    }

    pub fn get(&self) -> bool {
        use std::sync::atomic::Ordering;
        self.0.load(Ordering::Relaxed)
    }
}

#[cfg(not(feature = "testexport"))]
pub type ForceStop = ();

struct RaftProgress {
    term: u64,
    last_index: u64,
    last_preprocess_index: u64,
}

impl From<&RaftState> for RaftProgress {
    fn from(rs: &RaftState) -> Self {
        RaftProgress {
            term: rs.get_term(),
            last_index: rs.get_last_index(),
            last_preprocess_index: rs.get_last_preprocessed_index(),
        }
    }
}

#[derive(Default)]
struct RegionsRaftProgress {
    inner: HashMap<u64, RaftProgress>,
}

impl RegionsRaftProgress {
    fn update(&mut self, region_id: u64, rs: &RaftState) -> bool /* updated (is_newer) */ {
        match self.inner.entry(region_id) {
            HashMapEntry::Vacant(e) => {
                e.insert(RaftProgress::from(rs));
                true
            }
            HashMapEntry::Occupied(mut e) => {
                let curr = e.get();
                let is_newer = (
                    rs.get_term(),
                    rs.get_last_index(),
                    rs.get_last_preprocessed_index(),
                ) > (curr.term, curr.last_index, curr.last_preprocess_index);
                if is_newer {
                    e.insert(RaftProgress::from(rs));
                }
                is_newer
            }
        }
    }
}

fn fetch_raft_entries_to_region_progress(
    tag: ShardTag,
    raft: &RfEngine,
    peer_id: u64,
    low_idx: u64,
    high_idx: u64,
    region_progress: &mut RegionProgress,
) -> Result<()> {
    let mut entry_buf = Vec::with_capacity((high_idx - low_idx) as usize);
    box_try!(raft.fetch_raft_entries_to(peer_id, low_idx, high_idx, None, &mut entry_buf)
        .map_err(|err| {
            let stats = raft.get_peer_stats(peer_id);
            let truncated_state = rfstore::store::load_raft_truncated_state(raft, peer_id);
            error!("{} fetch_raft_entries failed: {:?}", tag, err;
                "low" => low_idx, "high" => high_idx, "stats" => ?stats, "truncated_state" => ?truncated_state);
            err
        }));
    for entry in entry_buf {
        debug!(
            "{} fetch_raft_entries: insert log index {}",
            tag, entry.index
        );
        region_progress.upsert_entry(tag, entry.index, entry.term as u32, || {
            RaftLogOp::new(&entry).into()
        });
    }
    Ok(())
}

pub fn peer_is_skippable(region_local_state: &RegionLocalState) -> bool {
    // `start_key` is empty if region is not initialized, and we will get incorrect
    // keyspace.
    region_local_state.state == PeerState::Tombstone
        || !is_region_initialized(region_local_state.get_region())
}
