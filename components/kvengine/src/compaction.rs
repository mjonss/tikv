// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering as CmpOrdering,
    collections::{HashMap, HashSet, hash_map::Entry},
    hash::Hash,
    iter::Iterator as StdIterator,
    ops::{Deref, Sub},
    path::PathBuf,
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};

use api_version::ApiV2;
use bstr::ByteSlice;
use bytes::{Buf, Bytes, BytesMut};
use cloud_encryption::{EncryptionKey, MasterKey};
use fail::fail_point;
use file_system::IoType;
use futures::future::try_join_all;
use http::StatusCode;
use hyper::Client;
use itertools::{Either, Itertools};
use kvenginepb::{self as pb, ColumnarCreate, fts::TableIndexId};
use pb::{BlobCreate, TableCreate, VectorIndexFile};
use protobuf::Message;
use security::SecurityManager;
use slog_global::error;
use table::{
    columnar::{ColumnarFile, ColumnarTableReader},
    schema_file::Schema,
};
use tidb_query_common::util::convert_to_prefix_next;
use tidb_query_datatype::{
    VECTOR_INDEX_SPEC_KEY_DISTANCE_METRIC,
    codec::table::{
        ID_LEN, TABLE_PREFIX_LEN, decode_common_handle, decode_int_handle, decode_table_id,
        encode_row_key, encode_row_key_prefix,
    },
};
use tikv_util::{
    HandyRwLock, backoff::ExponentialBackoff, box_err, retry::sleep_async,
    sys::thread::ThreadBuildWrapper, time::Instant,
};
use tokio::sync::mpsc;

use crate::{
    EXTRA_CF,
    Error::{
        CompactionNotRetryable, FallbackLocalCompactorDisabled, IncompatibleRemoteCompactor,
        RemoteCompaction, RemoteCompactorIsBusy, TableError,
    },
    Iterator, LOCK_CF, WRITE_CF, dfs,
    dfs::FileType,
    metrics::{ENGINE_COMPACTION_RESULT_COUNTER, ENGINE_COMPACTION_TRIGGER_COUNTER},
    shard::DeletePrefixes,
    table::{
        BoundedDataSet, ChecksumType, DataBound, InnerKey, OwnedInnerKey, SnapVersion,
        blobtable::{
            blobtable::BlobTable,
            builder::{BlobTableBuildOptions, BlobTableBuilder},
        },
        columnar::{
            Block, ColumnarCompactReader, ColumnarConcatReader, ColumnarFileBuilder,
            ColumnarFilterReader, ColumnarMergeReader, ColumnarMetaCache, ColumnarReader,
            ColumnarRowTableReader, ColumnarTableBuildOptions, ColumnarTableBuilder,
            ColumnarTruncateTsReader, GLOBAL_COMMON_HANDLE_END,
        },
        file::{File, InMemFile, LocalFile},
        fts::{
            ColumnarToFtsL0Opts, DedicatedFile, DedicatedFileBuilderOptions, EDedicatedFile,
            FtsBuildOptions, FtsCache, FtsLevels, InplaceResult, InplaceSpec,
            KeyRange as FtsKeyRange, PackedFile, PackedFileBuilderOptions, PackedFileMergeOpt,
            columnar_to_fts_l0, merge_fts_l0_l1, merge_fts_l2_files, rewrite_dedicated_file,
            rewrite_packed_file,
        },
        get_local_dir,
        schema_file::SchemaFile,
        sstable::{self, BlockCache, L0Builder, SsTable, builder::TableBuilderOptions},
        vector_index::{VectorIndexBuildOptions, VectorIndexBuilder},
    },
    table_id::{get_table_id_from_data_bound, is_bound_overlap_with_table_ids},
    util::{
        append_fts_update, new_blob_create_pb, new_columnar_create_pb, new_table_create_pb,
        new_vector_index_file_pb, patch_remove_to_fts_update,
    },
    *,
};

const MAJOR_COMPACTION_MIN_REQUEST_VERSION: u32 = 3;

// Do not skip L1 tables if there are too many small L1 tables.
const MAX_SKIP_L1_TABLES: usize = 16;

const MAX_DELTA_VECTOR_SIZE_PERCENT: usize = 10; // 10% of the max_file_size

static RETRY_INTERVAL: Duration = Duration::from_secs(600);

#[derive(Default, Eq, PartialEq, Hash, Clone)]
pub struct RemoteCompactor {
    remote_url: String,
    permanent: bool,
}

impl RemoteCompactor {
    pub fn new(remote_url: String, permanent: bool) -> Self {
        Self {
            remote_url,
            permanent,
        }
    }
}

pub struct RemoteCompactors {
    remote_urls: Vec<RemoteCompactor>,
    index: usize,
    last_failure: Instant,
}

impl RemoteCompactors {
    pub fn new(remote_url: String) -> Self {
        let mut remote_urls = Vec::new();
        if !remote_url.is_empty() {
            remote_urls.push(RemoteCompactor::new(remote_url, true));
        }
        Self {
            remote_urls,
            index: 0,
            last_failure: Instant::now().sub(RETRY_INTERVAL),
        }
    }
}

#[derive(Clone)]
pub struct CompactionClient {
    dfs: Arc<dyn dfs::Dfs>,
    id_allocator: Arc<dyn IdAllocator>,
    remote_compactors: Arc<Mutex<RemoteCompactors>>,
    client: Option<security::HttpClient>,
    compression_lvl: i32,
    pub(crate) checksum_type: ChecksumType,
    allow_fallback_local: bool,
    master_key: MasterKey,
    local_dirs: Vec<PathBuf>,
    // Whether the compaction client is used during restore (trim over bound & truncate ts).
    for_restore: bool,
    columnar_meta_cache: ColumnarMetaCache,
}

impl CompactionClient {
    pub(crate) fn new(
        dfs: Arc<dyn dfs::Dfs>,
        remote_url: String,
        compression_lvl: i32,
        checksum_type: ChecksumType,
        allow_fallback_local: bool,
        id_allocator: Arc<dyn IdAllocator>,
        master_key: MasterKey,
        security_mgr: Arc<SecurityManager>,
        local_dirs: Vec<PathBuf>,
        for_restore: bool,
        columnar_meta_cache: ColumnarMetaCache,
    ) -> Self {
        let remote_compactors = RemoteCompactors::new(remote_url);
        let client = security_mgr
            .http_client(Client::builder().pool_max_idle_per_host(0).clone())
            .unwrap();
        Self {
            dfs,
            remote_compactors: Arc::new(Mutex::new(remote_compactors)),
            client: Some(client),
            compression_lvl,
            checksum_type,
            allow_fallback_local,
            id_allocator,
            master_key,
            local_dirs,
            for_restore,
            columnar_meta_cache,
        }
    }

    pub fn add_remote_compactor(&self, remote_url: String) {
        if !remote_url.is_empty() {
            let mut remote_compactors = self.remote_compactors.lock().unwrap();
            remote_compactors.last_failure = Instant::now().sub(RETRY_INTERVAL);
            if remote_compactors
                .remote_urls
                .iter()
                .any(|x| x.remote_url == remote_url.clone())
            {
                return;
            }
            remote_compactors
                .remote_urls
                .push(RemoteCompactor::new(remote_url.clone(), false));
            info!(
                "add remote compactor {}, total {}",
                remote_url,
                remote_compactors.remote_urls.len()
            );
        };
    }

    pub fn delete_remote_compactor(&self, remote_compactor: &RemoteCompactor) {
        let mut remote_compactors = self.remote_compactors.lock().unwrap();
        if !remote_compactor.permanent {
            remote_compactors
                .remote_urls
                .retain(|x| x.remote_url != remote_compactor.remote_url);
            info!(
                "delete remote compactor {}, total {}",
                remote_compactor.remote_url,
                remote_compactors.remote_urls.len()
            );
        } else if remote_compactors.remote_urls.len() == 1 {
            remote_compactors.last_failure = Instant::now();
        }
    }

    pub fn get_remote_compactors(&self) -> Vec<String> {
        self.remote_compactors
            .lock()
            .unwrap()
            .remote_urls
            .clone()
            .into_iter()
            .map(|r| r.remote_url)
            .collect()
    }

    pub fn get_remote_compactor(&self) -> RemoteCompactor {
        let mut remote_compactors = self.remote_compactors.lock().unwrap();
        // `last_failure` is updated when there has only 1 compactor. We ignore the
        // compactor when the compactor is not permanent and the last failure is
        // less than the retry interval.
        if remote_compactors.remote_urls.is_empty()
            || (remote_compactors
                .last_failure
                .saturating_elapsed()
                .le(&RETRY_INTERVAL)
                && !remote_compactors.remote_urls[0].permanent)
        {
            RemoteCompactor::default()
        } else {
            remote_compactors.index += 1;
            if remote_compactors.index >= remote_compactors.remote_urls.len() {
                remote_compactors.index = 0;
            }
            remote_compactors.remote_urls[remote_compactors.index].clone()
        }
    }

    pub(crate) async fn compact(&self, req: CompactionRequest) -> Result<pb::ChangeSet> {
        let encryption_key = if req.exported_encryption_key.is_empty() {
            None
        } else {
            Some(
                self.master_key
                    .decrypt_encryption_key(&req.exported_encryption_key)
                    .unwrap(),
            )
        };
        let ctx = CompactionCtx {
            req: Arc::new(req),
            dfs: self.dfs.clone(),
            compression_lvl: self.compression_lvl,
            checksum_type: self.checksum_type,
            id_allocator: self.id_allocator.clone(),
            encryption_key,
            local_dirs: self.local_dirs.clone(),
            for_restore: self.for_restore,
            columnar_meta_cache: self.columnar_meta_cache.clone(),
        };
        let req = &ctx.req;
        let mut remote_compactor = self.get_remote_compactor();
        if remote_compactor.remote_url.is_empty() {
            local_compact(&ctx).await
        } else {
            let tag = ShardTag::from_comp_req(req.as_ref());
            let mut bo =
                ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(10), 5);
            loop {
                match self.remote_compact(req, &remote_compactor.remote_url).await {
                    result @ Ok(_) => break result,
                    Err(e @ IncompatibleRemoteCompactor { .. }) => {
                        if self.allow_fallback_local {
                            warn!("{} fall back to local compact: {:?}", tag, e);
                            break local_compact(&ctx).await;
                        } else {
                            warn!("{} remote compactor is incompatible: {:?}", tag, e);
                            break Err(FallbackLocalCompactorDisabled);
                        }
                    }
                    Err(e) => {
                        error!(
                            "{} remote compaction failed: {:?}", tag, e;
                            "req" => ?req,
                            "retry" => bo.current_attempts(),
                            "remote_compactor" => &remote_compactor.remote_url,
                        );

                        let delay = bo.next_delay();
                        if delay.is_ok() {
                            if matches!(e, RemoteCompactorIsBusy(_)) {
                                remote_compactor = self.get_remote_compactor();
                            } else if !remote_compactor.permanent && bo.current_attempts() % 3 == 0
                            {
                                self.delete_remote_compactor(&remote_compactor);
                                remote_compactor = self.get_remote_compactor();
                            }
                        }

                        if delay.is_err() || remote_compactor.remote_url.is_empty() {
                            if self.allow_fallback_local {
                                break local_compact(&ctx).await;
                            } else {
                                warn!("{} no remote compactor available", tag);
                                break Err(FallbackLocalCompactorDisabled);
                            }
                        }
                        sleep_async(delay.unwrap()).await;
                    }
                }
            }
        }
    }

    async fn remote_compact(
        &self,
        comp_req: &CompactionRequest,
        remote_url: &str,
    ) -> Result<pb::ChangeSet> {
        let body_str = serde_json::to_string(&comp_req).unwrap();
        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(remote_url)
            .header("content-type", "application/json")
            .body(hyper::Body::from(body_str))?;
        let tag = ShardTag::from_comp_req(comp_req);
        debug!("{} send request to remote compactor", tag);
        let start_time = Instant::now_coarse();
        let response = self.client.as_ref().unwrap().request(req).await?;
        let status = response.status();
        info!("{} got response from remote compactor", tag;
            "status" => ?status, "takes" => ?start_time.saturating_elapsed(), "url" => ?remote_url);
        let body = hyper::body::to_bytes(response.into_body()).await?;
        if !status.is_success() {
            let err_msg = String::from_utf8_lossy(body.chunk()).to_string();
            return if status == INCOMPATIBLE_COMPACTOR_ERROR_CODE {
                Err(IncompatibleRemoteCompactor {
                    url: remote_url.to_string(),
                    msg: err_msg,
                })
            } else if status == StatusCode::SERVICE_UNAVAILABLE {
                Err(RemoteCompactorIsBusy(err_msg))
            } else {
                Err(RemoteCompaction(err_msg))
            };
        }
        let mut cs = pb::ChangeSet::new();
        if let Err(err) = cs.merge_from_bytes(&body) {
            return Err(RemoteCompaction(err.to_string()));
        }
        Ok(cs)
    }
}

/// `CURRENT_COMPACTOR_VERSION` is used for version compatibility checking of
/// remote compactor. NOTE: Increase `CURRENT_COMPACTOR_VERSION` by 1 when add
/// new feature to remote compactor.
pub const CURRENT_COMPACTOR_VERSION: u32 = 3;

pub const INCOMPATIBLE_COMPACTOR_ERROR_CODE: StatusCode = StatusCode::NOT_IMPLEMENTED;

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CompactionRequest {
    pub engine_id: u64,
    pub shard_id: u64,
    pub shard_ver: u64,

    #[serde(rename = "start")]
    pub outer_start: Vec<u8>,
    #[serde(rename = "end")]
    pub outer_end: Vec<u8>,
    pub inner_key_off: usize,

    pub file_ids: Vec<u64>,

    pub compaction_tp: CompactionType,
    /// Required version of remote compactor.
    /// Must be set to `CURRENT_COMPACTOR_VERSION`.
    pub compactor_version: u32,

    pub safe_ts: u64,
    pub exported_encryption_key: Vec<u8>,

    /// The size in bytes of input tables used as source of compaction.
    pub input_size: u64,
}

impl CompactionRequest {
    pub fn inner_start(&self) -> InnerKey<'_> {
        InnerKey::from_outer_key(&self.outer_start)
    }

    pub fn inner_end(&self) -> InnerKey<'_> {
        InnerKey::from_outer_end_key(&self.outer_end)
    }

    pub fn get_tag(&self) -> String {
        format!("[{}:{}:{}]", self.engine_id, self.shard_id, self.shard_ver)
    }

    pub fn keyspace_id(&self) -> u32 {
        ApiV2::get_u32_keyspace_id_by_key(&self.outer_start).unwrap_or_default()
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct MajorCompaction {
    safe_ts: u64,
    l0_tables: Vec<u64>,
    // L1 plus sstables of all cfs, map from cf id to sstables that are organized by level.
    ln_tables: HashMap<usize, Vec<(usize, Vec<u64>)>>,
    blob_tables: Vec<u64>,
    sst_config: TableBuilderOptions,
    bt_config: Option<BlobTableBuildOptions>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct L0Compaction {
    safe_ts: u64,
    l0_tables: Vec<u64>,
    multi_cf_l1_tables: Vec<Vec<u64>>,
    sst_config: TableBuilderOptions,
    bt_config: Option<BlobTableBuildOptions>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct L1PlusCompaction {
    cf: isize,
    level: usize,
    safe_ts: u64,
    // Whether to keep the latest tombstone before the safe ts.
    keep_latest_obsolete_tombstone: bool,
    upper_level: Vec<u64>,
    lower_level: Vec<u64>,
    sst_config: TableBuilderOptions,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ColumnarCompaction {
    level: u32,
    safe_ts: u64,
    snap_version: SnapVersion,
    source_row_files: Vec<(u32, u64)>,      // (level, id)
    source_columnar_files: Vec<(u32, u64)>, // (level, id)
    schema_file_id: u64,
    columnar_table_ids: Vec<i64>,
    columnar_config: ColumnarTableBuildOptions,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ColumnarMajorCompaction {
    safe_ts: u64,
    l0_tables: Vec<u64>,
    ln_tables: Vec<(usize /* level */, Vec<u64> /* file ids */)>,
    blob_tables: Vec<u64>,
    old_columnar_tables: Vec<(usize /* level */, u64 /* file id */)>,
    schema_file_id: u64,
    table_ids: (Vec<i64>, Vec<i64>), /* (table ids that need to add columnar, table ids that
                                      * need to clear columnar) */
    columnar_config: ColumnarTableBuildOptions,
    snap_version: SnapVersion,
    target_level: u32, // target level for columnar tables create
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub enum InPlaceCompaction {
    #[default]
    Unknown,
    TruncateTs(u64),
    TrimOverBound,
    DestroyRange(Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct InPlaceCompactionCtx {
    file_ids: Vec<(u64, u32, i32)>,
    col_file_ids: Vec<(u64, u32)>,
    block_size: usize,
    columnar_build_opts: ColumnarTableBuildOptions,
    schema_file_id: Option<u64>,
    columnar_table_ids: Vec<i64>,
    fts_tasks: Vec<FtsInplaceTask>,
    fts_packed_build_opts: PackedFileBuilderOptions,
    fts_dedicated_build_opts: DedicatedFileBuilderOptions,
    spec: InPlaceCompaction,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum FtsFileKind {
    L0,
    L1,
    L2,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FtsInplaceTask {
    pub id: u64,
    pub kind: FtsFileKind,
}

impl FtsInplaceTask {
    pub fn l0(id: u64) -> Self {
        Self {
            id,
            kind: FtsFileKind::L0,
        }
    }

    pub fn l1(id: u64) -> Self {
        Self {
            id,
            kind: FtsFileKind::L1,
        }
    }

    pub fn l2(id: u64) -> Self {
        Self {
            id,
            kind: FtsFileKind::L2,
        }
    }
}

#[derive(Default)]
pub struct FtsPendingWork {
    pub tasks: Vec<FtsInplaceTask>,
    pub remove_l0: Vec<u64>,
    pub remove_l1: Vec<u64>,
    pub remove_l2: Vec<u64>,
    input_bytes: u64,
}

impl FtsPendingWork {
    pub fn has_tasks(&self) -> bool {
        !self.tasks.is_empty()
    }

    pub fn is_empty(&self) -> bool {
        !self.has_tasks()
            && self.remove_l0.is_empty()
            && self.remove_l1.is_empty()
            && self.remove_l2.is_empty()
    }

    pub fn has_removes(&self) -> bool {
        !self.remove_l0.is_empty() || !self.remove_l1.is_empty() || !self.remove_l2.is_empty()
    }

    pub fn push_task(&mut self, task: FtsInplaceTask, bytes: u64) {
        self.tasks.push(task);
        self.input_bytes = self.input_bytes.saturating_add(bytes);
    }

    pub fn total_removal(&self) -> usize {
        self.remove_l0.len() + self.remove_l1.len() + self.remove_l2.len()
    }
}

fn collect_fts_trim_overbound_tasks(
    levels: &FtsLevels,
    shard_bound: DataBound<'_>,
) -> FtsPendingWork {
    let mut pending = FtsPendingWork::default();
    for file in levels.l0() {
        let (lower, upper) = (
            InnerKey::from_inner_buf(file.props().get_smallest_key()),
            InnerKey::from_inner_buf(file.props().get_biggest_key()),
        );
        let file_bound = DataBound::new(lower, upper, true);
        if !shard_bound.overlap_bound(file_bound) {
            pending.remove_l0.push(file.id());
        } else if !shard_bound.contains_bound(file_bound) {
            pending.push_task(FtsInplaceTask::l0(file.id()), file.file().size());
        }
    }
    for file in levels.l1() {
        let (lower, upper) = (
            InnerKey::from_inner_buf(file.props().get_smallest_key()),
            InnerKey::from_inner_buf(file.props().get_biggest_key()),
        );
        let file_bound = DataBound::new(lower, upper, true);
        if !shard_bound.overlap_bound(file_bound) {
            pending.remove_l1.push(file.id());
        } else if !shard_bound.contains_bound(file_bound) {
            pending.push_task(FtsInplaceTask::l1(file.id()), file.file().size());
        }
    }
    for files in levels.l2().values() {
        for file in files {
            let props = file.props();
            let (lower, upper) = (
                InnerKey::from_inner_buf(props.get_smallest_key()),
                InnerKey::from_inner_buf(props.get_biggest_key()),
            );
            let file_bound = DataBound::new(lower, upper, true);
            debug!(
                "id: {}, collect_fts_trim_overbound_tasks, file_bound: {:?}, shard_bound: {:?}, overlap: {:?}",
                file.id(),
                file_bound,
                shard_bound,
                shard_bound.overlap_bound(file_bound)
            );
            if !shard_bound.overlap_bound(file_bound) {
                pending.remove_l2.push(file.id());
            } else if !shard_bound.contains_bound(file_bound) {
                pending.push_task(FtsInplaceTask::l2(file.id()), file.file().size());
            }
        }
    }
    pending
}

fn collect_fts_destroy_tasks(levels: &FtsLevels, del_prefixes: &DeletePrefixes) -> FtsPendingWork {
    let mut pending = FtsPendingWork::default();
    for file in levels.l0() {
        let (lower, upper) = (
            InnerKey::from_inner_buf(file.props().get_smallest_key()),
            InnerKey::from_inner_buf(file.props().get_biggest_key()),
        );
        let bound = DataBound::new(lower, upper, true);
        if del_prefixes.cover_range(lower, upper) {
            pending.remove_l0.push(file.id());
        } else if del_prefixes
            .inner_delete_bounds()
            .any(|range| range.overlap_bound(bound))
        {
            pending.push_task(FtsInplaceTask::l0(file.id()), file.file().size());
        }
    }
    for file in levels.l1() {
        let (lower, upper) = (
            InnerKey::from_inner_buf(file.props().get_smallest_key()),
            InnerKey::from_inner_buf(file.props().get_biggest_key()),
        );
        let bound = DataBound::new(lower, upper, true);
        if del_prefixes.cover_range(lower, upper) {
            pending.remove_l1.push(file.id());
        } else if del_prefixes
            .inner_delete_bounds()
            .any(|range| range.overlap_bound(bound))
        {
            pending.push_task(FtsInplaceTask::l1(file.id()), file.file().size());
        }
    }
    for files in levels.l2().values() {
        for file in files {
            let props = file.props();
            let (lower, upper) = (
                InnerKey::from_inner_buf(props.get_smallest_key()),
                InnerKey::from_inner_buf(props.get_biggest_key()),
            );
            let bound = DataBound::new(lower, upper, true);
            if del_prefixes.cover_range(lower, upper) {
                pending.remove_l2.push(file.id());
            } else if del_prefixes
                .inner_delete_bounds()
                .any(|range| range.overlap_bound(bound))
            {
                pending.push_task(FtsInplaceTask::l2(file.id()), file.file().size());
            }
        }
    }
    pending
}

fn collect_fts_truncate_tasks(levels: &FtsLevels) -> FtsPendingWork {
    let mut pending = FtsPendingWork::default();
    for file in levels.l0() {
        pending.push_task(FtsInplaceTask::l0(file.id()), file.file().size());
    }
    for file in levels.l1() {
        pending.push_task(FtsInplaceTask::l1(file.id()), file.file().size());
    }
    for files in levels.l2().values() {
        for file in files {
            pending.push_task(FtsInplaceTask::l2(file.id()), file.file().size());
        }
    }
    pending
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct VectorIndexUpdate {
    table_id: i64,
    index_id: i64,
    col_id: i64,
    snap_version: SnapVersion,
    schema_file_id: u64,
    col_file_ids: Vec<(u64, u32)>,
    remove_file_ids: Vec<u64>,
    vector_index_build_opts: VectorIndexBuildOptions,
}

impl VectorIndexUpdate {
    /// `added` field should be set outside of this function if needed.
    pub fn to_pb_without_added(&self) -> pb::UpdateVectorIndex {
        let mut update = pb::UpdateVectorIndex::new();
        update.set_table_id(self.table_id);
        update.set_index_id(self.index_id);
        update.set_col_id(self.col_id);
        update.set_removed(self.remove_file_ids.clone());
        update.set_snap_version(self.snap_version.into_inner());
        update
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct FtsCreateL0Update {
    /// The effective indexes we should care about
    fts_indexes: Vec<(i64 /* table_id */, i64 /* index_id */)>,
    /// The schema file
    schema_file_id: u64,
    /// The source columnar L0 files to create FTS L0
    col_l0_file_ids: Vec<u64>,
    /// The target snap_version of the FTS L0 file, it should be the max
    /// snap_version of the source columnar files, means all data <= this
    /// snap_version is included in the FTS L0 file.
    snap_version: SnapVersion,
    /// Build options snapshot (includes L0 file split knobs).
    build_options: FtsBuildOptions,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct FtsAddIndexUpdate {
    /// New FTS indexes to build from historical data
    fts_indexes_to_add: Vec<(i64 /* table_id */, i64 /* index_id */)>,
    /// The schema file
    schema_file_id: u64,
    /// Columnar files to process: { L0 <= current_snap_version, L1, L2 }
    col_file_ids: Vec<(u64, u32)>, // (file_id, level)
    /// Current FTS snap_version (not updated by this operation)
    current_snap_version: SnapVersion,
    /// Build options snapshot (includes L0 file split knobs).
    build_options: FtsBuildOptions,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FtsCompactL0Update {
    build_options: FtsBuildOptions,
    /// Active tracked indexes; LPs outside this set are dropped during merge.
    tracked_indexes: Vec<(i64, i64)>,

    /// L0 file ids to compact
    l0_file_ids: Vec<u64>,
    /// L1 file ids to compact (who are overlapped with L0 files)
    l1_file_ids: Vec<u64>,
    /// Existing L2 logical partition keys (so we keep them in L2).
    l2_lp_keys: Vec<Vec<u8>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
/// Request payload for intra-L2 compaction of a single logical partition.
pub struct FtsCompactL2Update {
    /// Logical partition key being compacted; all files belong to this LP.
    lp_key: Vec<u8>,
    /// Dedicated-file IDs to merge for this LP.
    l2_file_ids: Vec<u64>,
    /// Build options snapshot (includes L2 merge knobs).
    build_options: FtsBuildOptions,
}

/// Request payload for removing obsolete L1/L2 files that no longer correspond
/// to any tracked FTS index.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FtsCleanupUpdate {
    l1_file_ids: Vec<u64>,
    l2_file_ids: Vec<u64>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub enum CompactionType {
    #[default]
    Unknown,
    Major(MajorCompaction),
    L0(L0Compaction),
    L1Plus(L1PlusCompaction),
    InPlaceWithColumnar(InPlaceCompactionCtx),
    Columnar(ColumnarCompaction),
    ColumnarMajor(ColumnarMajorCompaction),
    VectorIndex(VectorIndexUpdate),
    FtsCreateL0(FtsCreateL0Update),
    FtsAddIndex(FtsAddIndexUpdate),
    FtsCompactL0(FtsCompactL0Update),
    FtsCompactL2(FtsCompactL2Update),
    FtsCleanup(FtsCleanupUpdate),
}

const MAX_COMPACTION_EXPAND_SIZE: u64 = 256 * 1024 * 1024;

impl Engine {
    pub fn update_managed_safe_ts(&self, ts: u64) {
        loop {
            let old = load_u64(&self.managed_safe_ts);
            if old < ts
                && self
                    .managed_safe_ts
                    .compare_exchange(old, ts, Ordering::Release, Ordering::Relaxed)
                    .is_err()
            {
                continue;
            }
            break;
        }
    }

    pub fn get_managed_safe_ts(&self, keyspace_id: u32) -> u64 {
        let gc_safe_point_ts = load_u64(&self.managed_safe_ts);
        debug!(
            "Get gc safe point v1, keyspace_id:{:?}, gc safepoint:{}",
            keyspace_id, gc_safe_point_ts,
        );
        gc_safe_point_ts
    }

    pub fn get_keyspace_gc_safepoint_v2(&self, keyspace_id: u32) -> u64 {
        match &self.ks_safepoint_v2 {
            Some(sp_map) => {
                if keyspace_id > 0 {
                    // Api v2 key.
                    let keyspace_sp_ts = sp_map.get(&keyspace_id);
                    match keyspace_sp_ts {
                        None => {
                            if self.opts.disable_safe_point_fallback_v1 {
                                debug!(
                                    "Can not get gc safe point v2, and refuse to get gc safe point v1, keyspace_id:{}",
                                    keyspace_id
                                );
                                return 0;
                            }
                            debug!(
                                "Can not get gc safe point v2, get gc safe point v1, keyspace_id:{}",
                                keyspace_id
                            );
                            // Api v1 key.
                            self.get_managed_safe_ts(keyspace_id)
                        }
                        Some(ks2sp) => {
                            let ks_gc_sp = *ks2sp.value();
                            debug!(
                                "Get gc safe point v2, keyspace_id:{}, gc safepoint:{}",
                                keyspace_id, ks_gc_sp
                            );
                            ks_gc_sp
                        }
                    }
                } else {
                    // Api v1 key.
                    self.get_managed_safe_ts(keyspace_id)
                }
            }
            None => {
                // Api v1 key.
                self.get_managed_safe_ts(keyspace_id)
            }
        }
    }

    pub(crate) fn run_compaction(&self, compact_rx: tikv_util::mpsc::Receiver<CompactMsg>) {
        let mut runner = CompactRunner::new(self.clone(), compact_rx);
        runner.run();
    }

    pub(crate) async fn compact(&self, shard: Arc<Shard>) -> Option<Result<pb::ChangeSet>> {
        let tag = shard.tag();
        if !shard.ready_to_compact() {
            info!("Shard {} is not ready for compaction", tag);
            return None;
        }
        store_bool(&shard.compacting, true);
        fail_point!("kvengine_compact", |_| Some(Err(
            FallbackLocalCompactorDisabled
        )));
        match shard.get_compaction_priority() {
            Some(CompactionPriority::L0 { .. }) => {
                ENGINE_COMPACTION_TRIGGER_COUNTER
                    .with_label_values(&["l0"])
                    .inc();
                self.trigger_l0_compaction(&shard).await
            }
            Some(CompactionPriority::L1Plus { cf, level, .. }) => {
                ENGINE_COMPACTION_TRIGGER_COUNTER
                    .with_label_values(&["l1_plus"])
                    .inc();
                self.trigger_l1_plus_compaction(&shard, cf, level).await
            }
            Some(CompactionPriority::Major { is_manual, .. }) => {
                ENGINE_COMPACTION_TRIGGER_COUNTER
                    .with_label_values(&["major"])
                    .inc();
                self.trigger_major_compaction(&shard, is_manual).await
            }
            Some(CompactionPriority::DestroyRange) => {
                ENGINE_COMPACTION_TRIGGER_COUNTER
                    .with_label_values(&["destroy_range"])
                    .inc();
                self.destroy_range(&shard).await.transpose()
            }
            Some(CompactionPriority::TruncateTs) => {
                ENGINE_COMPACTION_TRIGGER_COUNTER
                    .with_label_values(&["truncate_ts"])
                    .inc();
                self.truncate_ts(&shard).await.transpose()
            }
            Some(CompactionPriority::TrimOverBound) => {
                ENGINE_COMPACTION_TRIGGER_COUNTER
                    .with_label_values(&["trim_over_bound"])
                    .inc();
                self.trim_over_bound(&shard).await.transpose()
            }
            Some(CompactionPriority::GcLockFile) => {
                ENGINE_COMPACTION_TRIGGER_COUNTER
                    .with_label_values(&["gc_lock_file"])
                    .inc();
                self.trigger_gc_lock_file(&shard).transpose()
            }
            Some(CompactionPriority::GcExtraFile) => self.trigger_gc_extra_file(&shard).transpose(),
            Some(CompactionPriority::L0ToColumnar) => {
                ENGINE_COMPACTION_TRIGGER_COUNTER
                    .with_label_values(&["l0_to_columnar"])
                    .inc();
                self.trigger_l0_to_columnar(&shard).await
            }
            Some(CompactionPriority::ColumnarL0 { .. }) => {
                ENGINE_COMPACTION_TRIGGER_COUNTER
                    .with_label_values(&["columnar_l0"])
                    .inc();
                self.trigger_columnar_l0_compaction(&shard).await
            }
            Some(CompactionPriority::ColumnarL1 { .. }) => {
                ENGINE_COMPACTION_TRIGGER_COUNTER
                    .with_label_values(&["columnar_l1"])
                    .inc();
                self.trigger_columnar_l1_compaction(&shard).await
            }
            Some(CompactionPriority::ColumnarMajor {
                table_ids_to_add,
                table_ids_to_clear,
                is_manual,
                schema_version,
            }) => {
                if is_manual {
                    ENGINE_COMPACTION_TRIGGER_COUNTER
                        .with_label_values(&["columnar_major_from_col"])
                        .inc();
                    self.trigger_columnar_major_compaction_from_col(&shard, schema_version)
                        .await
                } else {
                    ENGINE_COMPACTION_TRIGGER_COUNTER
                        .with_label_values(&["columnar_major_from_sst"])
                        .inc();
                    self.trigger_columnar_major_compaction_from_sst(
                        &shard,
                        (table_ids_to_add, table_ids_to_clear),
                        schema_version,
                    )
                    .await
                }
            }
            Some(CompactionPriority::ColumnarClear) => {
                ENGINE_COMPACTION_TRIGGER_COUNTER
                    .with_label_values(&["columnar_clear"])
                    .inc();
                self.trigger_remove_columnar_compaction(&shard).await
            }
            Some(CompactionPriority::UpdateVectorIndex {
                table_id,
                index_id,
                col_id,
                rebuild,
                ..
            }) => {
                ENGINE_COMPACTION_TRIGGER_COUNTER
                    .with_label_values(&["update_vector_index"])
                    .inc();
                self.trigger_vector_index_update(&shard, table_id, index_id, col_id, rebuild)
                    .await
            }
            Some(CompactionPriority::FtsCreateL0 {
                col_file_ids,
                active_tracked_indexes,
                snap_version,
            }) => {
                self.trigger_fts_create_l0(
                    &shard,
                    col_file_ids,
                    active_tracked_indexes,
                    snap_version,
                )
                .await
            }
            Some(CompactionPriority::FtsCompactL0) => self.trigger_fts_l0_compact(&shard).await,
            Some(CompactionPriority::FtsCompactL2 { lp_key, file_ids }) => {
                self.trigger_fts_l2_compact(&shard, lp_key, file_ids).await
            }
            Some(CompactionPriority::FtsAddIndex { new_indexes }) => {
                self.trigger_fts_add_index(&shard, new_indexes).await
            }
            Some(CompactionPriority::FtsDropIndex { dropped_indexes }) => {
                self.trigger_fts_drop_index(&shard, dropped_indexes)
            }
            Some(CompactionPriority::FtsCleanup {
                l1_file_ids,
                l2_file_ids,
            }) => {
                self.trigger_fts_cleanup(&shard, l1_file_ids, l2_file_ids)
                    .await
            }
            None => {
                info!("Shard {} is not urgent for compaction", tag);
                store_bool(&shard.compacting, false);
                None
            }
        }
    }

    pub(crate) async fn set_alloc_ids_for_request(
        &self,
        req: &mut CompactionRequest,
        cur_num_files: usize,
        num_files_at_most: usize,
    ) {
        let ids = self
            .id_allocator
            .alloc_id_async(num_files_at_most * 2 + 16 + cur_num_files)
            .await
            .unwrap();
        req.file_ids = ids;
    }

    pub(crate) fn new_compact_request_with_shard(&self, shard: &Shard) -> CompactionRequest {
        let exported_encryption_key = shard
            .properties
            .get(ENCRYPTION_KEY)
            .unwrap_or_default()
            .to_vec();
        let inner_key_off = shard.get_data().inner_key_off;
        self.new_compact_request(
            shard.engine_id,
            shard.id,
            shard.ver,
            shard.range.clone(),
            inner_key_off,
            exported_encryption_key,
        )
    }

    pub(crate) fn new_compact_request_with_meta(&self, meta: &ShardMeta) -> CompactionRequest {
        let exported_encryption_key = meta
            .properties
            .get(ENCRYPTION_KEY)
            .unwrap_or_default()
            .to_vec();
        self.new_compact_request(
            meta.engine_id,
            meta.id,
            meta.ver,
            meta.range.clone(),
            meta.inner_key_off,
            exported_encryption_key,
        )
    }

    fn new_compact_request(
        &self,
        engine_id: u64,
        shard_id: u64,
        shard_ver: u64,
        range: ShardRange,
        inner_key_off: usize,
        exported_encryption_key: Vec<u8>,
    ) -> CompactionRequest {
        CompactionRequest {
            engine_id,
            shard_id,
            shard_ver,
            outer_start: range.outer_start.to_vec(),
            outer_end: range.outer_end.to_vec(),
            inner_key_off,
            file_ids: vec![],
            compaction_tp: CompactionType::Unknown,
            compactor_version: self.opts.compaction_request_version,
            safe_ts: self.get_keyspace_gc_safepoint_v2(range.keyspace_id),
            exported_encryption_key,
            input_size: 0,
        }
    }

    async fn destroy_range(&self, shard: &Shard) -> Result<Option<pb::ChangeSet>> {
        let del_prefixes = shard.get_del_prefixes();
        if del_prefixes.is_empty() {
            info!(
                "{} Engine::destroy_range: del_prefixes is empty, skip",
                shard.tag()
            );
            return Ok(None);
        }
        let data = shard.get_data();
        // Tables that full covered by delete-prefixes.
        let mut deletes = vec![];
        // Tables that partially covered by delete-prefixes.
        let mut overlaps = vec![];
        let mut col_overlaps = vec![];
        let mut total_size = 0;
        for t in &data.l0_tbls {
            if del_prefixes.cover_range(t.smallest(), t.biggest()) {
                let mut delete = pb::TableDelete::default();
                delete.set_id(t.id());
                delete.set_level(0);
                delete.set_cf(-1);
                deletes.push(delete);
            } else if del_prefixes
                .inner_delete_bounds()
                .any(|bound| t.has_data_in_bound(bound))
            {
                overlaps.push((t.id(), 0, -1));
                total_size += t.size();
            }
        }
        for cf in 0..NUM_CFS {
            let scf = data.get_cf(cf);
            for lh in &scf.levels {
                for t in lh.tables.as_slice() {
                    if del_prefixes.cover_range(t.smallest(), t.biggest()) {
                        let mut delete = pb::TableDelete::default();
                        delete.set_id(t.id());
                        delete.set_level(lh.level as u32);
                        delete.set_cf(cf as i32);
                        deletes.push(delete);
                    } else if t
                        .has_any_overlap_async(del_prefixes.inner_delete_bounds())
                        .await
                    {
                        overlaps.push((t.id(), lh.level as u32, cf as i32));
                        total_size += t.size();
                    }
                }
            }
        }

        let mut columnar_deletes = vec![];
        data.for_each_columnar_level(|cl| {
            for t in cl.files.iter() {
                if del_prefixes.cover_range(t.get_smallest(), t.get_biggest()) {
                    let mut delete = pb::ColumnarDelete::default();
                    delete.set_id(t.id());
                    delete.set_level(cl.level as u32);
                    columnar_deletes.push(delete);
                } else if del_prefixes.inner_delete_bounds().any(|bound| {
                    let start = bound.lower_bound.deref();
                    match decode_table_id(start) {
                        Ok(table_id) => {
                            start.len() == TABLE_PREFIX_LEN + ID_LEN && t.has_table(table_id)
                        }
                        Err(_) => {
                            error!(
                                "{}, can not decode table_id from start key: {:?}",
                                shard.tag(),
                                start
                            );
                            false
                        }
                    }
                }) {
                    col_overlaps.push((t.id(), cl.level as u32));
                    total_size += t.size();
                }
            }
            false
        });

        let fts_pending = collect_fts_destroy_tasks(&data.fts_levels, &del_prefixes);
        total_size += fts_pending.input_bytes;

        info!(
            "{} start destroying range: {:?}", shard.tag(), del_prefixes;
            "destroyed" => deletes.len(),
            "columnar_destroyed" => columnar_deletes.len(),
            "overlaps" => overlaps.len(),
            "columnar_overlaps" => col_overlaps.len(),
            "fts_overlaps" => fts_pending.tasks.len(),
            "fts_destroyed" => fts_pending.total_removal(),
            "input_size" => total_size,
        );

        let need_fts = fts_pending.has_tasks();
        let mut cs = if overlaps.is_empty() && col_overlaps.is_empty() && !need_fts {
            let mut cs = pb::ChangeSet::default();
            cs.mut_destroy_range().set_table_deletes(deletes.into());
            cs.mut_destroy_range()
                .set_columnar_deletes(columnar_deletes.into());
            cs
        } else {
            let mut req = self.new_compact_request_with_shard(shard);
            req.file_ids = self
                .id_allocator
                .alloc_id_async(overlaps.len() + col_overlaps.len() + fts_pending.tasks.len())
                .await
                .unwrap();
            let in_place_compaction_type = InPlaceCompaction::DestroyRange(del_prefixes.marshal());
            let in_place_compaction = InPlaceCompactionCtx {
                file_ids: overlaps,
                col_file_ids: col_overlaps,
                block_size: self.opts.table_builder_options.block_size,
                columnar_build_opts: self.opts.columnar_build_options,
                schema_file_id: data.schema_file.as_ref().map(|f| f.get_file_id()),
                columnar_table_ids: data.get_columnar_table_ids_in_schema(),
                fts_tasks: fts_pending.tasks.clone(),
                fts_packed_build_opts: PackedFileBuilderOptions::default(),
                fts_dedicated_build_opts: DedicatedFileBuilderOptions::default(),
                spec: in_place_compaction_type,
            };
            req.input_size = total_size;
            req.compaction_tp = CompactionType::InPlaceWithColumnar(in_place_compaction);
            let mut cs = self.comp_client.compact(req).await?;
            let dr = cs.mut_destroy_range();
            deletes.extend(dr.take_table_deletes().into_iter());
            columnar_deletes.extend(dr.take_columnar_deletes().into_iter());
            dr.set_table_deletes(deletes.into());
            dr.set_columnar_deletes(columnar_deletes.into());
            cs
        };
        patch_remove_to_fts_update(&mut cs, &fts_pending);

        cs.set_shard_id(shard.id);
        cs.set_shard_ver(shard.ver);
        cs.set_property_key(DEL_PREFIXES_KEY.to_string());
        cs.set_property_value(del_prefixes.marshal());
        Ok(Some(cs))
    }

    pub(crate) async fn truncate_ts(&self, shard: &Shard) -> Result<Option<pb::ChangeSet>> {
        self.truncate_with_ts(shard, shard.get_truncate_ts().unwrap())
            .await
    }

    // Also used by keyspace restore
    pub async fn truncate_with_ts(
        &self,
        shard: &Shard,
        truncate_ts: u64,
    ) -> Result<Option<pb::ChangeSet>> {
        let data = shard.get_data();
        // TODO: record min_ts to directly delete a SSTable.

        let mut overlaps = vec![];
        let mut col_overlaps = vec![];
        let mut total_size = 0;
        for t in &data.l0_tbls {
            if truncate_ts < t.max_ts() {
                overlaps.push((t.id(), 0, -1));
                total_size += t.size();
            }
        }
        data.for_each_level(|cf, lh| {
            for t in lh.tables.iter() {
                if truncate_ts < t.max_ts {
                    overlaps.push((t.id(), lh.level as u32, cf as i32));
                    total_size += t.size();
                }
            }
            false
        });

        data.for_each_columnar_level(|cl| {
            for t in cl.files.iter() {
                if truncate_ts < t.get_max_version() {
                    col_overlaps.push((t.id(), cl.level as u32));
                    total_size += t.size();
                }
            }
            false
        });

        let fts_pending = collect_fts_truncate_tasks(&data.fts_levels);
        total_size += fts_pending.input_bytes;

        info!(
            "{} start truncate ts", shard.tag();
            "truncate_ts" => truncate_ts,
            "overlaps" => overlaps.len(),
            "col_overlaps" => col_overlaps.len(),
            "fts_overlaps" => fts_pending.tasks.len(),
            "input_size" => total_size,
        );

        let need_fts = fts_pending.has_tasks();
        let mut cs = if overlaps.is_empty() && col_overlaps.is_empty() && !need_fts {
            let mut cs = pb::ChangeSet::default();
            // Must `set_truncate_ts` for checking the type of ChangeSet properly.
            cs.set_truncate_ts(pb::TableChange::default());
            cs
        } else {
            let mut req = self.new_compact_request_with_shard(shard);
            req.file_ids = self
                .id_allocator
                .alloc_id_async(overlaps.len() + col_overlaps.len() + fts_pending.tasks.len())
                .await
                .unwrap();
            let in_place_compaction = InPlaceCompaction::TruncateTs(truncate_ts);
            let in_place_compaction_ctx = InPlaceCompactionCtx {
                file_ids: overlaps,
                col_file_ids: col_overlaps,
                block_size: self.opts.table_builder_options.block_size,
                columnar_build_opts: self.opts.columnar_build_options,
                schema_file_id: data.schema_file.as_ref().map(|f| f.get_file_id()),
                columnar_table_ids: data.get_columnar_table_ids_in_schema(),
                fts_tasks: fts_pending.tasks.clone(),
                fts_packed_build_opts: PackedFileBuilderOptions::default(),
                fts_dedicated_build_opts: DedicatedFileBuilderOptions::default(),
                spec: in_place_compaction,
            };
            req.input_size = total_size;
            req.compaction_tp = CompactionType::InPlaceWithColumnar(in_place_compaction_ctx);
            self.comp_client.compact(req).await?
        };
        patch_remove_to_fts_update(&mut cs, &fts_pending);

        cs.set_shard_id(shard.id);
        cs.set_shard_ver(shard.ver);
        Ok(Some(cs))
    }

    async fn trim_over_bound(&self, shard: &Shard) -> Result<Option<pb::ChangeSet>> {
        let data = shard.get_data();
        if !shard.get_trim_over_bound() {
            // `data.trim_over_bound is possible to be false.
            // See https://github.com/tidbcloud/cloud-storage-engine/issues/663.
            info!(
                "{} shard.data.trim_over_bound is false, skip trim_over_bound",
                shard.tag()
            );
            return Ok(None);
        }

        // Tables that are entirely over bound.
        let mut deletes = vec![];
        // Tables that are partially over bound.
        let mut overlaps = vec![];
        let mut col_overlaps = vec![];
        let mut total_size = 0;
        let shard_bound = shard.data_bound();
        for t in &data.l0_tbls {
            let table_bound = t.data_bound();
            if !shard_bound.overlap_bound(table_bound) {
                let mut delete = pb::TableDelete::default();
                delete.set_id(t.id());
                delete.set_level(0);
                delete.set_cf(-1);
                deletes.push(delete);
            } else if !shard_bound.contains_bound(table_bound) {
                overlaps.push((t.id(), 0, -1));
                total_size += t.size();
            }
        }
        data.for_each_level(|cf, lh| {
            for t in lh.tables.iter() {
                let table_bound = t.data_bound();
                if !shard_bound.overlap_bound(table_bound) {
                    let mut delete = pb::TableDelete::default();
                    delete.set_id(t.id());
                    delete.set_level(lh.level as u32);
                    delete.set_cf(cf as i32);
                    deletes.push(delete);
                } else if !shard_bound.contains_bound(table_bound) {
                    overlaps.push((t.id(), lh.level as u32, cf as i32));
                    total_size += t.size();
                }
            }
            false
        });
        let mut columnar_deletes = vec![];
        data.for_each_columnar_level(|cl| {
            for t in cl.files.iter() {
                let table_bound = t.data_bound();
                if !shard_bound.overlap_bound(table_bound) {
                    // -----smallest-----biggest-----[start----------end)
                    // [start----------end)-----smallest-----biggest-----
                    let mut delete = pb::ColumnarDelete::default();
                    delete.set_id(t.id());
                    delete.set_level(cl.level as u32);
                    columnar_deletes.push(delete);
                } else if !shard_bound.contains_bound(table_bound) {
                    col_overlaps.push((t.id(), cl.level as u32));
                    total_size += t.size();
                }
            }
            false
        });

        let fts_pending = collect_fts_trim_overbound_tasks(&data.fts_levels, shard_bound);
        total_size += fts_pending.input_bytes;

        info!(
            "{} start trim_over_bound", shard.tag();
            "destroyed" => deletes.len(),
            "col_destroyed" => columnar_deletes.len(),
            "overlaps" => overlaps.len(),
            "col_overlaps" => col_overlaps.len(),
            "fts_overlaps" => fts_pending.tasks.len(),
            "fts_destroyed" => fts_pending.total_removal(),
            "input_size" => total_size,
        );

        let need_fts = fts_pending.has_tasks();
        let mut cs = if overlaps.is_empty() && col_overlaps.is_empty() && !need_fts {
            let mut cs = pb::ChangeSet::default();
            cs.mut_trim_over_bound().set_table_deletes(deletes.into());
            cs.mut_trim_over_bound()
                .set_columnar_deletes(columnar_deletes.into());
            cs
        } else {
            let mut req = self.new_compact_request_with_shard(shard);
            req.file_ids = self
                .id_allocator
                .alloc_id_async(overlaps.len() + col_overlaps.len() + fts_pending.tasks.len())
                .await
                .unwrap();
            let inplace_compaction = InPlaceCompactionCtx {
                file_ids: overlaps,
                col_file_ids: col_overlaps,
                block_size: self.opts.table_builder_options.block_size,
                columnar_build_opts: self.opts.columnar_build_options,
                schema_file_id: data.schema_file.as_ref().map(|f| f.get_file_id()),
                columnar_table_ids: data.get_columnar_table_ids_in_schema(),
                fts_tasks: fts_pending.tasks.clone(),
                fts_packed_build_opts: PackedFileBuilderOptions::default(),
                fts_dedicated_build_opts: DedicatedFileBuilderOptions::default(),
                spec: InPlaceCompaction::TrimOverBound,
            };
            req.input_size = total_size;
            req.compaction_tp = CompactionType::InPlaceWithColumnar(inplace_compaction);
            let mut cs = self.comp_client.compact(req).await?;
            let tc = cs.mut_trim_over_bound();
            deletes.extend(tc.take_table_deletes().into_iter());
            columnar_deletes.extend(tc.take_columnar_deletes().into_iter());
            tc.set_table_deletes(deletes.into());
            tc.set_columnar_deletes(columnar_deletes.into());
            cs
        };
        patch_remove_to_fts_update(&mut cs, &fts_pending);

        cs.set_shard_id(shard.id);
        cs.set_shard_ver(shard.ver);
        cs.set_property_key(TRIM_OVER_BOUND.to_string());
        cs.set_property_value(TRIM_OVER_BOUND_DISABLE.to_vec());

        Ok(Some(cs))
    }

    pub async fn trim_over_bound_by_meta(&self, meta: &ShardMeta) -> Result<pb::ChangeSet> {
        // Tables that are entirely over bound.
        let mut deletes = vec![];
        let mut columnar_deletes = vec![];
        // Tables that are partially over bound.
        let mut over_bounds = vec![];
        let mut col_over_bounds = vec![];
        let mut total_size = 0;
        for (&id, f) in &meta.files {
            let shard_bound = meta.range.data_bound();
            let file_bound = f.data_bound();
            if !shard_bound.overlap_bound(file_bound) {
                if f.is_columnar_file() {
                    let mut columnar_delete = pb::ColumnarDelete::default();
                    columnar_delete.set_id(id);
                    columnar_delete.set_level(f.level as u32);
                    columnar_deletes.push(columnar_delete);
                } else {
                    let mut delete = pb::TableDelete::default();
                    delete.set_id(id);
                    delete.set_level(f.level as u32);
                    delete.set_cf(f.cf as i32);
                    deletes.push(delete);
                }
            } else if !shard_bound.contains_bound(file_bound) {
                if f.is_columnar_file() {
                    col_over_bounds.push((id, f.level as u32));
                } else {
                    over_bounds.push((id, f.level as u32, f.cf as i32));
                }
                total_size += if f.is_l0_sst_with_size() {
                    f.l0_size
                } else {
                    debug_assert!(f.table_meta_off > 0, "f: {:?}", f);
                    f.table_meta_off
                } as u64;
            }
        }

        debug!(
            "{}:{} start trim_over_bound_by_meta", meta.id, meta.ver;
            "destroyed" => deletes.len(),
            "col_destroyed" => columnar_deletes.len(),
            "overlaps" => over_bounds.len(),
            "col_overlaps" => col_over_bounds.len(),
            "input_size" => total_size,
        );

        let mut res_cs = if over_bounds.is_empty() && col_over_bounds.is_empty() {
            let mut cs = pb::ChangeSet::default();
            if !deletes.is_empty() {
                cs.mut_trim_over_bound().set_table_deletes(deletes.into());
            }
            if !columnar_deletes.is_empty() {
                cs.mut_trim_over_bound()
                    .set_columnar_deletes(columnar_deletes.into());
            }
            cs
        } else {
            let mut req = self.new_compact_request_with_meta(meta);
            req.file_ids = self
                .id_allocator
                .alloc_id_async(over_bounds.len())
                .await
                .unwrap();
            let mut columnar_table_ids = meta.columnar_table_ids.clone();
            columnar_table_ids.sort();
            let in_place_compaction_ctx = InPlaceCompactionCtx {
                file_ids: over_bounds,
                col_file_ids: col_over_bounds,
                block_size: self.opts.table_builder_options.block_size,
                columnar_build_opts: self.opts.columnar_build_options,
                schema_file_id: Some(meta.schema.file_id()),
                columnar_table_ids,
                fts_tasks: Vec::new(),
                fts_packed_build_opts: PackedFileBuilderOptions::default(),
                fts_dedicated_build_opts: DedicatedFileBuilderOptions::default(),
                spec: InPlaceCompaction::TrimOverBound,
            };
            req.input_size = total_size;
            req.compaction_tp = CompactionType::InPlaceWithColumnar(in_place_compaction_ctx);
            let mut cs = self.comp_client.compact(req).await?;
            let tc = cs.mut_trim_over_bound();
            deletes.extend(tc.take_table_deletes().into_iter());
            columnar_deletes.extend(tc.take_columnar_deletes().into_iter());
            tc.set_table_deletes(deletes.into());
            tc.set_columnar_deletes(columnar_deletes.into());
            cs
        };
        res_cs.set_shard_id(meta.id);
        res_cs.set_shard_ver(meta.ver);
        res_cs.set_property_key(TRIM_OVER_BOUND.to_string());
        res_cs.set_property_value(TRIM_OVER_BOUND_DISABLE.to_vec());
        Ok(res_cs)
    }

    fn get_blob_table_build_options_or_none(&self, shard: &Shard) -> Option<BlobTableBuildOptions> {
        if shard.keyspace_id > 0 {
            let conf = self.per_keyspace_configs.get(&shard.keyspace_id)?;
            if conf.enable_blob {
                return Some(self.opts.blob_table_build_options);
            }
        }
        None
    }

    pub(crate) fn trigger_gc_lock_file(&self, shard: &Shard) -> Result<Option<pb::ChangeSet>> {
        if !shard.get_initial_flushed() {
            // For commit merge, the persisted_version may increased by source region and do
            // not ensure that all the commit entry for the lock files has been flushed.
            return Ok(None);
        }
        let safe_ts = self.get_keyspace_gc_safepoint_v2(shard.keyspace_id);
        let data = shard.get_data();
        let lock_cf_files = data.get_cf(LOCK_CF);
        let mut pending_gc_lock_files = vec![];
        let lvl2 = lock_cf_files.get_level(2);
        for tbl in lvl2.tables.iter() {
            if tbl.get_max_lock_ts() < safe_ts {
                pending_gc_lock_files.push(tbl.id());
            }
        }
        if pending_gc_lock_files.len() == lvl2.tables.len() {
            // All level 2 lock files can be deleted, so we can safely check level 1.
            // otherwise, gc level 1 tombstone may cause some level 2 lock reappear.
            let lvl1 = lock_cf_files.get_level(1);
            for tbl in lvl1.tables.iter() {
                if tbl.get_max_lock_ts() < safe_ts {
                    pending_gc_lock_files.push(tbl.id());
                }
            }
        }
        let tag = shard.tag();
        let mut ready_gc_lock_files = vec![];
        if data.all_persisted() {
            ready_gc_lock_files = pending_gc_lock_files;
        } else {
            let mem_max_snap_ver = data
                .mem_tbls
                .iter()
                .map(|m| m.get_snap_version())
                .max()
                .unwrap_or_default();
            let new_snap_version = mem_max_snap_ver.max(data.persisted_version);
            let mut guard = shard.pending_ops.wl();
            for gc_lock_cf_file in pending_gc_lock_files {
                match guard.gc_lock_files.entry(gc_lock_cf_file) {
                    Entry::Occupied(old_file) => {
                        if *old_file.get() < data.persisted_version {
                            // The shard has been flushed after the lock file added to the pending
                            // gc list. It will not be used for recovery, so we can safely remove
                            // it.
                            ready_gc_lock_files.push(gc_lock_cf_file);
                            info!("{} add new ready {}", tag, gc_lock_cf_file);
                            old_file.remove();
                        }
                    }
                    Entry::Vacant(e) => {
                        info!("{} add new pending {}", tag, gc_lock_cf_file);
                        e.insert(new_snap_version);
                    }
                }
            }
        }
        if ready_gc_lock_files.is_empty() {
            return Ok(None);
        }
        let mut cs = new_change_set(shard.id, shard.ver);
        let comp = cs.mut_compaction();
        comp.cf = LOCK_CF as i32;
        comp.level = 1;
        let mut gc_count = 0;
        let lock_cf = &data.cfs[LOCK_CF];
        for lvl in &lock_cf.levels {
            for tbl in lvl.tables.iter() {
                if ready_gc_lock_files.contains(&tbl.id()) {
                    if lvl.level == 1 {
                        comp.mut_top_deletes().push(tbl.id());
                    } else {
                        comp.mut_bottom_deletes().push(tbl.id());
                    }
                    gc_count += 1;
                }
            }
        }
        if gc_count == 0 {
            return Ok(None);
        }
        info!("{} trigger gc lock file {:?}", tag, cs);
        Ok(Some(cs))
    }

    pub(crate) fn trigger_gc_extra_file(&self, shard: &Shard) -> Result<Option<pb::ChangeSet>> {
        let data = shard.get_data();
        let safe_ts = self.get_keyspace_gc_safepoint_v2(shard.keyspace_id);
        let mut cs = new_change_set(shard.id, shard.ver);
        let comp = cs.mut_compaction();
        comp.cf = EXTRA_CF as i32;
        comp.level = 1;
        let extra_cf = &data.cfs[EXTRA_CF];
        let extra_level = extra_cf.get_level(1);
        for tbl in extra_level.tables.iter() {
            if tbl.max_ts < safe_ts {
                comp.mut_top_deletes().push(tbl.id());
            }
        }
        if comp.get_top_deletes().is_empty() {
            info!("{} no extra files to gc", shard.tag());
            return Ok(None);
        }
        Ok(Some(cs))
    }

    pub(crate) async fn trigger_l0_compaction(
        &self,
        shard: &Shard,
    ) -> Option<Result<pb::ChangeSet>> {
        let tag = shard.tag();
        let data = shard.get_data();
        if data.l0_tbls.is_empty() {
            info!("{} zero L0 tables", tag);
            return None;
        }
        if let Some(res) = self.try_move_down_l0(shard, &data) {
            info!("{} move down l0", tag);
            return Some(Ok(res));
        }
        let mut req = self.new_compact_request_with_shard(shard);
        let mut l0_tbls = vec![];
        let mut multi_cfs_l1_tbls = vec![];
        let mut total_size = 0;
        let mut smallest = data.l0_tbls[0].smallest();
        let mut biggest = data.l0_tbls[0].biggest();
        let mut estimated_blob_size = 0;
        let unconverted_l0s: HashSet<u64> = data.get_unconverted_l0s().iter().cloned().collect();
        // Reversely iterate to ensure that older tables are compacted to L1 first.
        // When any L0 table will not be compacted, we should stop the iteration.
        // TODO: continue to choose newer L0 tables which have not overlapping with
        // older ones.
        for l0 in data.l0_tbls.iter().rev() {
            // Skip unconverted l0s.
            // After region merge there may have l0s with l0.version() <
            // columnar_snap_version already converted exists and will not be added
            // to unconverted_l0s. So we need check the unconverted_l0s.
            if unconverted_l0s.contains(&l0.id()) {
                info!("{} skip unconverted l0 {}", tag, l0.id());
                break;
            }

            if smallest > l0.smallest() {
                smallest = l0.smallest();
            }
            if biggest < l0.biggest() {
                biggest = l0.biggest();
            }
            l0_tbls.push(l0.id());
            if l0.size() > l0.entries() * self.opts.blob_table_build_options.min_blob_size as u64 {
                estimated_blob_size += l0.size()
                    - l0.entries() * self.opts.blob_table_build_options.min_blob_size as u64;
            }
            total_size += l0.size();
        }
        if l0_tbls.is_empty() {
            info!(
                "{} trigger_l0_compaction, none l0 table after filtering",
                tag
            );
            return None;
        }
        // Keep the L0 tables in order. It's not necessary (will be sorted again after
        // `load_table_files`) but for safety.
        l0_tbls.reverse();

        for cf in 0..NUM_CFS {
            let lh = data.get_cf(cf).get_level(1);
            let mut l1_tbls = vec![];
            for tbl in lh.tables.as_slice() {
                let non_overlapping = tbl.biggest() < smallest || tbl.smallest() > biggest;
                // Do not skip if there are too many L1 tables.
                if lh.tables.len() < MAX_SKIP_L1_TABLES && non_overlapping {
                    info!(
                        "{} skip L1 table {} for L0 compaction, tbl smallest {:?}, tbl biggest {:?}, L0 smallest {:?}, L0 biggest {:?}",
                        tag,
                        tbl.id(),
                        tbl.smallest(),
                        tbl.biggest(),
                        smallest,
                        biggest,
                    );
                    continue;
                }
                l1_tbls.push(tbl.id());
                total_size += tbl.size();
                if tbl.size()
                    > tbl.entries as u64 * self.opts.blob_table_build_options.min_blob_size as u64
                {
                    estimated_blob_size += tbl.size()
                        - tbl.entries as u64
                            * self.opts.blob_table_build_options.min_blob_size as u64;
                }
            }
            multi_cfs_l1_tbls.push(l1_tbls);
        }
        let sst_config = self.opts.table_builder_options;

        // Only opt-in users will use blob store.
        let mut bt_config = self.get_blob_table_build_options_or_none(shard);
        if let Some(build_opts) = &bt_config {
            if estimated_blob_size < build_opts.target_blob_table_size as u64 {
                bt_config = None;
            } else {
                info!(
                    "{} build blob in L0 compaction, estimated blob size {}, blob table build options {:?}",
                    tag, estimated_blob_size, bt_config
                );
            }
        }
        let estimated_num_files = total_size as usize
            / bt_config.map_or(sst_config.max_table_size, |c| {
                std::cmp::min(c.max_blob_table_size, sst_config.max_table_size)
            });
        self.set_alloc_ids_for_request(
            &mut req,
            l0_tbls.len() + multi_cfs_l1_tbls.len(),
            estimated_num_files,
        )
        .await;
        let l0_compaction = L0Compaction {
            safe_ts: self.get_keyspace_gc_safepoint_v2(shard.keyspace_id),
            l0_tables: l0_tbls,
            multi_cf_l1_tables: multi_cfs_l1_tbls,
            sst_config,
            bt_config,
        };
        req.input_size = total_size;
        req.compaction_tp = CompactionType::L0(l0_compaction);
        info!("{} start compact L0", tag; "input_size" => total_size);
        Some(self.comp_client.compact(req).await)
    }

    fn try_move_down_l0(&self, shard: &Shard, shard_data: &ShardData) -> Option<pb::ChangeSet> {
        if !shard_data.l0_tbls.iter().any(|l0| l0.is_write_cf_only()) {
            return None;
        }
        let mut move_down_l0s = vec![];
        let tag = shard.tag();
        let unconverted_l0s: HashSet<u64> =
            shard_data.get_unconverted_l0s().iter().cloned().collect();
        for (i, l0_tbl) in shard_data.l0_tbls.iter().enumerate() {
            if !l0_tbl.is_write_cf_only() {
                continue;
            }
            // Avoid unconverted l0s compaction.
            if unconverted_l0s.contains(&l0_tbl.id()) {
                info!("{} skip unconverted l0 {}", tag, l0_tbl.id());
                continue;
            }
            let l0_write_cf_tbl = l0_tbl.get_cf(WRITE_CF).as_ref().unwrap();
            let l0_write_cf_bound = l0_write_cf_tbl.data_bound();
            let overlap_below_l0 = shard_data.l0_tbls[i + 1..].iter().any(|below_l0| {
                if let Some(below_write_cf) = below_l0.get_cf(WRITE_CF) {
                    return below_write_cf.data_bound().overlap_bound(l0_write_cf_bound);
                }
                false
            });
            if overlap_below_l0 {
                continue;
            }
            let l1 = shard_data.cfs[WRITE_CF].get_level(1);
            if l0_write_cf_bound
                .find_overlap(l1.tables.as_slice(), true)
                .is_some()
            {
                continue;
            }
            move_down_l0s.push(l0_write_cf_tbl.to_table_create(WRITE_CF, 1));
        }
        if move_down_l0s.is_empty() {
            info!("{} no l0 to move down", tag);
            return None;
        }
        let mut cs = new_change_set(shard.id, shard.ver);
        let comp = cs.mut_compaction();
        comp.set_cf(-1);
        comp.set_level(0);
        comp.set_top_deletes(
            move_down_l0s
                .iter()
                .map(|tbl_create| tbl_create.id)
                .collect(),
        );
        comp.set_table_creates(move_down_l0s.into());
        Some(cs)
    }

    async fn trigger_l1_plus_compaction(
        &self,
        shard: &Shard,
        cf: isize,
        level: usize,
    ) -> Option<Result<pb::ChangeSet>> {
        let tag = shard.tag();
        let data = shard.get_data();
        let scf = data.get_cf(cf as usize);
        let upper_level = &scf.levels[level - 1];
        let lower_level = &scf.levels[level];
        if upper_level.tables.is_empty() {
            info!("{} no upper level {} table", tag, level - 1);
            return None;
        }
        let shard_bound = shard.data_bound();
        let upper_level_bound = upper_level.get_data_bound().unwrap();
        let upper_level_candidates = if !shard_bound.contains_bound(upper_level_bound) {
            if upper_level.tables.first().unwrap().smallest() < shard_bound.lower_bound {
                Arc::new(vec![upper_level.tables.first().unwrap().clone()])
            } else {
                Arc::new(vec![upper_level.tables.last().unwrap().clone()])
            }
        } else {
            upper_level.tables.clone()
        };

        let sum_tbl_size = |tbls: &[sstable::SsTable]| tbls.iter().map(|tbl| tbl.size()).sum();

        let calc_ratio = |upper_size: u64, lower_size: u64| {
            if lower_size == 0 {
                return upper_size as f64;
            }
            upper_size as f64 / lower_size as f64
        };
        // First pick one table has max topSize/bottomSize ratio.
        let mut candidate_ratio = 0f64;
        let mut upper_left_idx = 0;
        let mut upper_right_idx = 0;
        let mut upper_size = 0;
        let mut lower_left_idx = 0;
        let mut lower_right_idx = 0;
        let mut lower_size = 0;
        for (i, tbl) in upper_level_candidates.iter().enumerate() {
            let (left, right) = tbl.data_bound().get_overlap_data_sets(&lower_level.tables);
            let new_lower_size: u64 = sum_tbl_size(&lower_level.tables[left..right]);
            let ratio = calc_ratio(tbl.size(), new_lower_size);
            if candidate_ratio < ratio {
                candidate_ratio = ratio;
                upper_left_idx = i;
                upper_right_idx = i + 1;
                upper_size = tbl.size();
                lower_left_idx = left;
                lower_right_idx = right;
                lower_size = new_lower_size;
            }
        }
        if upper_left_idx == upper_right_idx {
            info!("{} no candidate for l1_plus_compaction", tag);
            return None;
        }
        // Expand to left to include more tops as long as the ratio doesn't decrease and
        // the total size do not exceed maxCompactionExpandSize.
        let cur_upper_left_idx = upper_left_idx;
        for i in (0..cur_upper_left_idx).rev() {
            let t = &upper_level_candidates[i];
            let (left, right) = t.data_bound().get_overlap_data_sets(&lower_level.tables);
            if right < lower_left_idx {
                // A bottom table is skipped, we can compact in another run.
                break;
            }
            let new_upper_size = t.size() + upper_size;
            let new_lower_size =
                sum_tbl_size(&lower_level.tables[left..lower_left_idx]) + lower_size;
            let new_ratio = calc_ratio(new_upper_size, new_lower_size);
            if new_ratio > candidate_ratio {
                upper_left_idx -= 1;
                lower_left_idx = left;
                upper_size = new_upper_size;
                lower_size = new_lower_size;
            } else {
                break;
            }
        }
        // Expand to right to include more tops as long as the ratio doesn't decrease
        // and the total size do not exceeds maxCompactionExpandSize.
        let cur_upper_right_idx = upper_right_idx;
        for i in cur_upper_right_idx..upper_level_candidates.len() {
            let t = &upper_level_candidates[i];
            let (left, right) = t.data_bound().get_overlap_data_sets(&lower_level.tables);
            if left > lower_right_idx {
                // A bottom table is skipped, we can compact in another run.
                break;
            }
            let new_upper_size = t.size() + upper_size;
            let new_lower_size =
                sum_tbl_size(&lower_level.tables[lower_right_idx..right]) + lower_size;
            let new_ratio = calc_ratio(new_upper_size, new_lower_size);
            if new_ratio > candidate_ratio
                && (new_upper_size + new_lower_size) < MAX_COMPACTION_EXPAND_SIZE
            {
                upper_right_idx += 1;
                lower_right_idx = right;
                upper_size = new_upper_size;
                lower_size = new_lower_size;
            } else {
                break;
            }
        }
        let upper_level_table_ids: Vec<_> = upper_level_candidates[upper_left_idx..upper_right_idx]
            .iter()
            .map(|t| t.id())
            .collect();
        let lower_level_table_ids: Vec<_> = lower_level.tables[lower_left_idx..lower_right_idx]
            .iter()
            .map(|t| t.id())
            .collect();
        if lower_level_table_ids.is_empty() && cf as usize == WRITE_CF {
            info!("move down L{} CF{} for {}", level, cf, tag);
            // Move down. only write CF benefits from this optimization.
            let mut comp = pb::Compaction::new();
            comp.set_cf(cf as i32);
            comp.set_level(level as u32);
            comp.set_top_deletes(upper_level_table_ids);
            let tbl_creates = upper_level_candidates[upper_left_idx..upper_right_idx]
                .iter()
                .map(|top_tbl| top_tbl.to_table_create(cf as usize, level + 1))
                .collect::<Vec<_>>();
            comp.set_table_creates(tbl_creates.into());
            let mut cs = new_change_set(shard.id, shard.ver);
            cs.set_compaction(comp);
            return Some(Ok(cs));
        }
        let mut has_overlap = false;
        let mut kr = KeyRange::default();
        // The range for overlapping check should include both upper level and lower
        // level.
        kr.update(&upper_level_candidates[upper_left_idx..upper_right_idx]);
        kr.update(&lower_level.tables[lower_left_idx..lower_right_idx]);
        for lvl_idx in (level + 1)..scf.levels.len() {
            let lh = &scf.levels[lvl_idx];
            let (left, right) = kr.data_bound().get_overlap_data_sets(&lh.tables);
            if left < right {
                has_overlap = true;
            }
        }
        let mut req = self.new_compact_request_with_shard(shard);
        let sst_config = self.opts.table_builder_options;
        let estimated_num_files = (upper_size + lower_size) as usize / sst_config.max_table_size;
        self.set_alloc_ids_for_request(
            &mut req,
            upper_level_table_ids.len() + lower_level_table_ids.len(),
            estimated_num_files,
        )
        .await;
        let l1_plus: L1PlusCompaction = L1PlusCompaction {
            cf,
            level,
            safe_ts: self.get_keyspace_gc_safepoint_v2(shard.keyspace_id),
            keep_latest_obsolete_tombstone: has_overlap,
            upper_level: upper_level_table_ids,
            lower_level: lower_level_table_ids,
            sst_config,
        };
        req.input_size = upper_size + lower_size;
        req.compaction_tp = CompactionType::L1Plus(l1_plus);
        info!(
            "{} start compact L{} CF{}, num_ids: {}, input_size: {}",
            tag,
            level,
            cf,
            req.file_ids.len(),
            upper_size + lower_size,
        );
        Some(self.comp_client.compact(req).await)
    }

    pub(crate) async fn trigger_major_compaction(
        &self,
        shard: &Shard,
        is_manual: bool,
    ) -> Option<Result<pb::ChangeSet>> {
        if self.opts.compaction_request_version < MAJOR_COMPACTION_MIN_REQUEST_VERSION {
            return Some(Err(CompactionNotRetryable(format!(
                "trigger_major_compaction: compaction_request_version must >= {}",
                MAJOR_COMPACTION_MIN_REQUEST_VERSION
            ))));
        }
        let data = shard.get_data();
        // We checked the unconverted_l0s in `refresh_compaction_priority`, but new l0s
        // may be added after the check. The compact is running in the background, so we
        // need to check it again.
        if data.has_unconverted_l0s() {
            info!(
                "{} trigger_major_compaction skipped due to unconverted_l0s {:?}",
                shard.tag(),
                data.get_unconverted_l0s()
            );
            return None;
        }
        let mut req = self.new_compact_request_with_shard(shard);
        let mut ln_tables: HashMap<usize, Vec<(usize, Vec<u64>)>> = HashMap::new();
        let mut total_size = 0;
        let mut num_ln_files = 0;
        for (cf, shard_cf) in data.cfs.iter().enumerate() {
            let mut ssts_for_cf = vec![];
            for lh in &shard_cf.levels {
                if lh.tables.is_empty() {
                    continue;
                }
                num_ln_files += lh.tables.len();
                ssts_for_cf.push((lh.level, lh.tables.iter().map(|t| t.id()).collect()));
                total_size += lh.tables.iter().map(|t| t.size()).sum::<u64>();
            }
            ln_tables.insert(cf, ssts_for_cf);
        }
        total_size += data.l0_tbls.iter().map(|t| t.size()).sum::<u64>();
        total_size += data.blob_tbl_map.values().map(|t| t.size()).sum::<u64>();

        if total_size == 0 {
            info!(
                "{} trigger_major_compaction skipped, no tables to compact", shard.tag();
                "is_manual" => is_manual,
            );
            if is_manual {
                // Return empty change set to remove the manual compaction property.
                let mut cs = pb::ChangeSet::new();
                cs.set_shard_id(shard.id);
                cs.set_shard_ver(shard.ver);
                cs.set_major_compaction(pb::MajorCompaction::new());
                return Some(Ok(cs));
            } else {
                return None;
            }
        }

        let sst_config = self.opts.table_builder_options;
        let bt_config = self.get_blob_table_build_options_or_none(shard);
        let estimated_num_files = total_size as usize
            / std::cmp::min(
                bt_config.map_or(usize::MAX, |bt_config| bt_config.max_blob_table_size),
                sst_config.max_table_size,
            );
        self.set_alloc_ids_for_request(
            &mut req,
            data.l0_tbls.len() + num_ln_files + data.blob_tbl_map.len(),
            estimated_num_files,
        )
        .await;
        let major_compaction = MajorCompaction {
            safe_ts: self.get_keyspace_gc_safepoint_v2(shard.keyspace_id),
            l0_tables: data.l0_tbls.iter().map(|t| t.id()).collect(),
            ln_tables,
            blob_tables: data.blob_tbl_map.keys().copied().collect(),
            sst_config,
            bt_config,
        };
        req.input_size = total_size;
        req.compaction_tp = CompactionType::Major(major_compaction);
        info!(
            "{} start major compact, num_ids: {}, input_size: {}",
            shard.tag(),
            req.file_ids.len(),
            total_size,
        );
        Some(self.comp_client.compact(req).await)
    }

    pub(crate) async fn trigger_remove_columnar_compaction(
        &self,
        shard: &Shard,
    ) -> Option<Result<pb::ChangeSet>> {
        let tag = shard.tag();
        let data = shard.get_data();
        let mut old_columnar_tables = vec![];
        data.col_levels.levels.iter().for_each(|l| {
            l.files.iter().for_each(|f| {
                old_columnar_tables.push((l.level, f.id()));
            })
        });
        let mut req = self.new_compact_request_with_shard(shard);
        let columnar_major_compaction = ColumnarMajorCompaction {
            safe_ts: 0,
            l0_tables: vec![],
            ln_tables: vec![],
            blob_tables: vec![],
            old_columnar_tables,
            schema_file_id: 0,
            table_ids: (vec![], vec![]),
            snap_version: SnapVersion::zero(),
            columnar_config: self.opts.columnar_build_options,
            target_level: 2, // Set target level to 2 for clear columnar to reset l2_snap_version.
        };
        req.input_size = 1; // To help for detect that all `input_size` are set.
        req.compaction_tp = CompactionType::ColumnarMajor(columnar_major_compaction);
        info!("{} start remove columnar", tag);
        Some(self.comp_client.compact(req).await)
    }

    pub(crate) async fn trigger_l0_to_columnar(
        &self,
        shard: &Shard,
    ) -> Option<Result<pb::ChangeSet>> {
        let tag = shard.tag();
        let data = shard.get_data();
        if data.col_levels.unconverted_l0s.is_empty() {
            info!("{} no unconverted l0s to convert to columnar", tag);
            return None;
        }
        debug_assert!(!data.columnar_table_ids.is_empty());

        let schema_file = shard.get_schema_file()?;
        let mut req = self.new_compact_request_with_shard(shard);
        let mut source_row_tables = vec![];
        let mut snap_version = SnapVersion::zero();
        let mut total_size = 0;
        for l0 in &data.col_levels.unconverted_l0s {
            source_row_tables.push((0, l0.id()));
            snap_version = snap_version.max(l0.snap_version());
            total_size += l0.size();
        }

        // After merge, the target shard persisted snap version may be greater than the
        // snap_version of the unconverted l0s. We should use the greater one to
        // guarantee the columnar l0 file version monotonically increasing.
        snap_version = snap_version.max(data.fts_levels.l0_snap_version + 1);

        let num_l0s = source_row_tables.len();
        // columnar_table_ids is sorted.
        let columnar_table_ids = data.get_columnar_table_ids_in_schema();
        let columnar_compaction = ColumnarCompaction {
            level: 0,
            safe_ts: self.get_keyspace_gc_safepoint_v2(shard.keyspace_id),
            snap_version,
            source_row_files: source_row_tables,
            source_columnar_files: vec![],
            schema_file_id: schema_file.get_file_id(),
            columnar_table_ids,
            columnar_config: self.opts.columnar_build_options,
        };
        req.input_size = total_size;
        req.compaction_tp = CompactionType::Columnar(columnar_compaction);
        self.set_alloc_ids_for_request(&mut req, num_l0s, num_l0s)
            .await;
        info!(
            "{} start covert L0 to columnar, num_l0s {}, total size {}",
            tag, num_l0s, total_size
        );
        Some(self.comp_client.compact(req).await)
    }

    pub(crate) async fn trigger_fts_create_l0(
        &self,
        shard: &Shard,
        col_file_ids: Vec<u64>,
        active_tracked_indexes: Vec<(i64, i64)>,
        snap_version: SnapVersion,
    ) -> Option<Result<pb::ChangeSet>> {
        let tag = shard.tag();
        let data = shard.get_data();

        let schema_file = match shard.get_schema_file() {
            Some(sf) => sf,
            None => {
                info!("{} no schema file for FTSCreateL0", tag);
                return None;
            }
        };

        if col_file_ids.is_empty() {
            // This should not happen
            info!("{} no columnar files to process for FTSCreateL0", tag);
            return None;
        }

        // Collect columnar L0 files newer than the FTS watermark for incremental
        // processing
        let l0_watermark = data.fts_levels.l0_snap_version;
        let n_col_files = col_file_ids.len();
        let n_fts_indexes = active_tracked_indexes.len();
        let id_set: HashSet<u64> = col_file_ids.iter().copied().collect();
        let total_size: u64 = data.col_levels.levels[0]
            .files
            .iter()
            .filter(|file| id_set.contains(&file.id()))
            .map(|file| file.size())
            .sum();
        drop(data);

        // Create compaction request
        let mut req = self.new_compact_request_with_shard(shard);
        req.input_size = total_size;
        req.compaction_tp = CompactionType::FtsCreateL0(FtsCreateL0Update {
            fts_indexes: active_tracked_indexes,
            schema_file_id: schema_file.get_file_id(),
            col_l0_file_ids: col_file_ids,
            snap_version,
            build_options: self.opts.fts_build_options,
        });

        info!(
            "{} trigger incremental FTS L0 indexing: {} indexes, {} L0 files, snap_version watermark: {} -> {}",
            tag,
            n_fts_indexes,
            n_col_files,
            l0_watermark,
            snap_version;
            "total_size" => total_size
        );

        Some(self.comp_client.compact(req).await)
    }

    pub(crate) async fn trigger_fts_add_index(
        &self,
        shard: &Shard,
        new_indexes: Vec<(i64, i64)>,
    ) -> Option<Result<pb::ChangeSet>> {
        let tag = shard.tag();
        let data = shard.get_data();
        let current_fts_snap_version = data.fts_levels.l0_snap_version;

        let schema_file = match shard.get_schema_file() {
            Some(sf) => sf,
            None => {
                info!("{} no schema file for FTSAddIndex", tag);
                return None;
            }
        };

        if new_indexes.is_empty() {
            // This should not happen
            info!("{} no new FTS indexes to add", tag);
            return None;
        }

        // Collect historical columnar files: { L0 <= current_snap_version, L1, L2 }
        let mut col_file_ids = Vec::new();
        let mut total_size = 0;

        for col_level in &data.col_levels.levels {
            for col_file in &col_level.files {
                let should_process = match col_level.level {
                    0 => {
                        // L0: Only files <= current FTS snap_version and not pending
                        let l0_version = col_file.get_snap_version().unwrap_or_default();
                        l0_version <= current_fts_snap_version
                            && !data
                                .fts_levels
                                .pending_columnar_l0_ids()
                                .contains(&col_file.id())
                    }
                    1 | 2 => {
                        // L1, L2: Process all files (historical data)
                        true
                    }
                    _ => false,
                };

                if should_process {
                    // Check if file contains any table that has new FTS indexes
                    let has_relevant_table = new_indexes
                        .iter()
                        .any(|(table_id, _)| col_file.has_table(*table_id));

                    if has_relevant_table {
                        col_file_ids.push((col_file.get_file().id(), col_level.level as u32));
                        total_size += col_file.get_file().size();
                    }
                }
            }
        }

        if col_file_ids.is_empty() {
            info!("{} no historical data to process for FTS add index", tag);

            // Even though there's no historical data, we should still mark these indexes as
            // tracked, otherwise trigger_fts_add_index will be called repeatedly due to
            // the presence of new indexes.

            let mut fts_update = pb::FtsUpdate::new();

            // Mark all new indexes as tracked without creating actual FTS files
            for (table_id, index_id) in new_indexes {
                let mut index_id_msg = TableIndexId::new();
                index_id_msg.set_table_id(table_id);
                index_id_msg.set_index_id(index_id);
                fts_update.mut_add_tracked_indexes().push(index_id_msg);
            }

            let num_indexes = fts_update.get_add_tracked_indexes().len();

            // Updating tracked index is a small metadata update, so no need to go through
            // the full remote compaction path.
            let mut ret = pb::ChangeSet::new();
            ret.set_shard_id(shard.id);
            ret.set_shard_ver(shard.ver);
            ret.set_fts_update(fts_update);

            info!(
                "{} tracking {} new FTS indexes (no historical data)",
                tag, num_indexes
            );

            return Some(Ok(ret));
        }

        // Store count before moving col_file_ids
        let n_col_files = col_file_ids.len();
        let n_fts_indexes = new_indexes.len();

        // Create compaction request
        let mut req = self.new_compact_request_with_shard(shard);
        req.input_size = total_size;
        req.compaction_tp = CompactionType::FtsAddIndex(FtsAddIndexUpdate {
            fts_indexes_to_add: new_indexes,
            schema_file_id: schema_file.get_file_id(),
            col_file_ids,
            current_snap_version: current_fts_snap_version,
            build_options: self.opts.fts_build_options,
        });

        info!(
            "{} trigger FTS add index: {} new indexes, {} columnar files, current_fts_snap_version: {}, total_size: {}",
            tag, n_fts_indexes, n_col_files, current_fts_snap_version, total_size,
        );

        Some(self.comp_client.compact(req).await)
    }

    pub(crate) fn trigger_fts_drop_index(
        &self,
        shard: &Shard,
        dropped_indexes: Vec<(i64, i64)>,
    ) -> Option<Result<pb::ChangeSet>> {
        let tag = shard.tag();
        let data = shard.get_data();
        if data.schema_file.is_none() {
            info!("{} no schema file for FtsDropIndex", tag);
            return None;
        };

        if dropped_indexes.is_empty() {
            // This should not happen
            info!("{} no FTS indexes to drop", tag);
            return None;
        }

        let mut update = pb::FtsUpdate::new();
        for (table_id, index_id) in dropped_indexes.iter().copied() {
            let mut tbl_idx_id = TableIndexId::new();
            tbl_idx_id.set_table_id(table_id);
            tbl_idx_id.set_index_id(index_id);
            update.mut_remove_tracked_indexes().push(tbl_idx_id);
        }

        let mut cs = pb::ChangeSet::default();
        cs.set_shard_id(shard.id);
        cs.set_shard_ver(shard.ver);
        cs.set_fts_update(update);

        info!(
            "{} drop {} tracked FTS indexes due to schema change",
            tag,
            dropped_indexes.len();
            "dropped_indexes" => ?dropped_indexes,
        );

        Some(Ok(cs))
    }

    pub(crate) async fn trigger_fts_l0_compact(
        &self,
        shard: &Shard,
    ) -> Option<Result<pb::ChangeSet>> {
        fn ranges_overlap(a_start: &[u8], a_end: &[u8], b_start: &[u8], b_end: &[u8]) -> bool {
            !(a_end < b_start || b_end < a_start)
        }
        let tag = shard.tag();

        let data = shard.get_data();
        let levels = &data.fts_levels;
        let has_overlapping_l1_files = levels
            .l1()
            .windows(2)
            .any(|w| w[0].props().get_largest_lp_key() >= w[1].props().get_smallest_lp_key());
        if levels.l0().is_empty() && !has_overlapping_l1_files {
            info!(
                "{} no FTS L0 files to compact and no overlapping L1 files, skip",
                tag
            );
            return None;
        }

        // We first collect the min/max lp keys from all L0 files,
        // Then, all L1 files overlapped with [min_lp, max_lp] will be compacted
        // together with L0 files to form new L1 files.

        // There maybe no L0 files to compact if the L1 files have overlapping lp keys.
        let mut min_lp = vec![];
        let mut max_lp = vec![];
        if !levels.l0().is_empty() {
            min_lp = levels.l0()[0].props().get_smallest_lp_key().to_vec();
            max_lp = levels.l0()[0].props().get_largest_lp_key().to_vec();
            for l0_file in levels.l0() {
                let l0_begin = l0_file.props().get_smallest_lp_key();
                let l0_end = l0_file.props().get_largest_lp_key();
                if l0_begin < min_lp.as_slice() {
                    min_lp.clear();
                    min_lp.extend_from_slice(l0_begin);
                }
                if l0_end > max_lp.as_slice() {
                    max_lp.clear();
                    max_lp.extend_from_slice(l0_end);
                }
            }

            assert!(!min_lp.is_empty());
            assert!(!max_lp.is_empty());
            assert!(min_lp <= max_lp);
        }

        let mut l1_file_ids: HashSet<u64> = HashSet::new();
        for l1_file in levels.l1() {
            if l1_file_ids.contains(&l1_file.id()) {
                continue;
            }
            let l1_begin = l1_file.props().get_smallest_lp_key();
            let l1_end = l1_file.props().get_largest_lp_key();
            if has_overlapping_l1_files || ranges_overlap(&min_lp, &max_lp, l1_begin, l1_end) {
                l1_file_ids.insert(l1_file.id());
            }
        }

        let tracked_indexes: Vec<(i64, i64)> = levels
            .iter_tracked_indexes()
            .flat_map(|(table_id, indexes)| {
                indexes.iter().map(move |index_id| (*table_id, *index_id))
            })
            .collect();

        let plan = FtsCompactL0Update {
            build_options: self.opts.fts_build_options,
            tracked_indexes,
            l0_file_ids: levels.l0().iter().map(|file| file.id()).collect(),
            l1_file_ids: l1_file_ids.into_iter().collect(),
            l2_lp_keys: levels.l2().keys().cloned().collect(),
        };

        let mut total_size: u64 = levels.l0().iter().map(|file| file.file().size()).sum();
        if !plan.l1_file_ids.is_empty() {
            let l1_set: HashSet<u64> = plan.l1_file_ids.iter().copied().collect();
            total_size += levels
                .l1()
                .iter()
                .filter(|file| l1_set.contains(&file.id()))
                .map(|file| file.file().size())
                .sum::<u64>();
        }

        let mut req = self.new_compact_request_with_shard(shard);
        req.input_size = total_size;

        info!(
            "{} trigger FTS L0 compact", tag;
            "lp_min" => ?hexhex::hex(min_lp),
            "lp_max" => ?hexhex::hex(max_lp),
            "l0_inputs" => ?plan.l0_file_ids,
            "l1_inputs" => ?plan.l1_file_ids,
            "tracked_indexes" => plan.tracked_indexes.len(),
            "total_size" => total_size,
        );

        req.compaction_tp = CompactionType::FtsCompactL0(plan);

        Some(self.comp_client.compact(req).await)
    }

    pub(crate) async fn trigger_fts_l2_compact(
        &self,
        shard: &Shard,
        lp_key: Vec<u8>,
        file_ids: Vec<u64>,
    ) -> Option<Result<pb::ChangeSet>> {
        if file_ids.len() < 2 {
            return None;
        }

        let file_id_set: HashSet<u64> = file_ids.iter().copied().collect();
        let shard_data = shard.get_data();
        let total_size = shard_data
            .fts_levels
            .l2()
            .get(&lp_key)
            .map(|files| {
                files
                    .iter()
                    .filter(|file| file_id_set.contains(&file.id()))
                    .map(|file| file.file().size())
                    .sum()
            })
            .unwrap_or(0);
        drop(shard_data);

        let plan = FtsCompactL2Update {
            lp_key,
            l2_file_ids: file_ids.clone(),
            build_options: self.opts.fts_build_options,
        };

        let mut req = self.new_compact_request_with_shard(shard);
        req.input_size = total_size;
        req.compaction_tp = CompactionType::FtsCompactL2(plan.clone());

        info!(
            "{} trigger FTS L2 compact", shard.tag();
            "lp_key" => ?hexhex::hex(&plan.lp_key),
            "file_ids" => ?file_ids,
            "total_size" => total_size,
        );

        Some(self.comp_client.compact(req).await)
    }

    pub(crate) async fn trigger_fts_cleanup(
        &self,
        shard: &Shard,
        l1_file_ids: Vec<u64>,
        l2_file_ids: Vec<u64>,
    ) -> Option<Result<pb::ChangeSet>> {
        if l1_file_ids.is_empty() && l2_file_ids.is_empty() {
            return None;
        }
        let mut req = self.new_compact_request_with_shard(shard);
        let data = shard.get_data();
        let l1_set: HashSet<u64> = l1_file_ids.iter().copied().collect();
        let l2_set: HashSet<u64> = l2_file_ids.iter().copied().collect();
        let mut input_size: u64 = 0;
        for file in data.fts_levels.l1() {
            if l1_set.contains(&file.id()) {
                input_size = input_size.saturating_add(file.file().size());
            }
        }
        for files in data.fts_levels.l2().values() {
            for file in files {
                if l2_set.contains(&file.id()) {
                    input_size = input_size.saturating_add(file.file().size());
                }
            }
        }
        req.input_size = input_size;
        let plan = FtsCleanupUpdate {
            l1_file_ids,
            l2_file_ids,
        };
        info!(
            "{} trigger FTS cleanup", shard.tag();
            "l1_remove" => ?plan.l1_file_ids,
            "l2_remove" => ?plan.l2_file_ids,
        );
        req.compaction_tp = CompactionType::FtsCleanup(plan);
        Some(self.comp_client.compact(req).await)
    }

    pub(crate) async fn trigger_columnar_major_compaction_from_sst(
        &self,
        shard: &Shard,
        table_ids: (Vec<i64>, Vec<i64>),
        schema_version: i64,
    ) -> Option<Result<pb::ChangeSet>> {
        let (tables_to_add, tables_to_clear) = table_ids;
        let mut req = self.new_compact_request_with_shard(shard);
        let data = shard.get_data();
        if !shard.opt.build_columnar() || data.schema_file.is_none() {
            warn!(
                "{} trigger_columnar_major_compaction: disabled or no schema, skip",
                shard.tag()
            );
            return None;
        }
        // Check if the schema file changed. Schema file may be updated since
        // refresh_compaction_priority.
        let schema_file = data.schema_file.as_ref().unwrap();
        if schema_version != schema_file.get_version() {
            warn!(
                "{} trigger_columnar_major_compaction: schema_version mismatch, schema_version: {}, schema_file_version: {}",
                shard.tag(),
                schema_version,
                schema_file.get_version()
            );
            return None;
        }
        let schema_file_id = data.schema_file.as_ref().unwrap().get_file_id();
        info!(
            "{} trigger_columnar_major_compaction, table_ids_to_add: {:?}, table_ids_to_clear: {:?}",
            shard.tag(),
            tables_to_add,
            tables_to_clear,
        );
        let mut total_size = 0;
        let shard_cf = data.get_cf(WRITE_CF);
        let mut ln_tables: Vec<(usize, Vec<u64>)> = vec![];
        for lh in &shard_cf.levels {
            if lh.tables.is_empty() {
                continue;
            }
            ln_tables.push((
                lh.level,
                lh.tables
                    .iter()
                    .filter_map(|t| {
                        let data_bound = t.data_bound();
                        let (min_table_id, max_table_id) = get_table_id_from_data_bound(data_bound);
                        if tables_to_add
                            .iter()
                            .any(|&id| id >= min_table_id && id <= max_table_id)
                        {
                            total_size += t.size();
                            Some(t.id())
                        } else {
                            None
                        }
                    })
                    .collect(),
            ));
        }

        // Check if columnar file in level 2 overlaps with the tables to add.
        let has_columnar_file_overlap = data.col_levels.levels[2].files.iter().any(|col| {
            let data_bound = col.data_bound();
            is_bound_overlap_with_table_ids(data_bound, &tables_to_add)
        });
        // NOTE: add the columnar table to old_columnar_tables if the columnar file
        // contains the table.
        let mut old_columnar_tables = vec![];
        data.col_levels.levels.iter().for_each(|col_level| {
            col_level
                .files
                .iter()
                .filter(|col| tables_to_clear.iter().any(|&id| col.has_table(id)))
                .for_each(|table| {
                    old_columnar_tables.push((col_level.level, table.get_file().id()));
                });
        });

        let l0_tbls = data
            .l0_tbls
            .iter()
            .filter(|t| {
                let data_bound = t.data_bound();
                let (min_table_id, max_table_id) = get_table_id_from_data_bound(data_bound);
                tables_to_add
                    .iter()
                    .any(|&id| id >= min_table_id && id <= max_table_id)
                    && !data.get_unconverted_l0s().contains(&t.id())
            })
            .collect::<Vec<_>>();

        let mut snap_version = shard.get_columnar_l2_snap_version();
        let target_level = if !has_columnar_file_overlap {
            // If target_level is 2 and snap_version is 0, snap_version should be updated.
            if snap_version.is_zero() {
                snap_version = l0_tbls
                    .iter()
                    .map(|t| t.snap_version())
                    .max()
                    .unwrap_or(data.persisted_version);
            }
            2
        } else {
            1
        };

        let blob_tbls = data
            .blob_tbl_map
            .values()
            .filter(|t| {
                let data_bound = t.data_bound();
                let (min_table_id, max_table_id) = get_table_id_from_data_bound(data_bound);
                tables_to_add
                    .iter()
                    .any(|&id| id >= min_table_id && id <= max_table_id)
            })
            .collect::<Vec<_>>();

        total_size += l0_tbls.iter().map(|t| t.size()).sum::<u64>();
        total_size += blob_tbls.iter().map(|t| t.size()).sum::<u64>();
        let columnar_config = self.opts.columnar_build_options;
        let estimated_num_files = total_size as usize / columnar_config.max_columnar_table_size;
        let num_ln_files = ln_tables
            .iter()
            .map(|(_, files)| files.len())
            .sum::<usize>();
        self.set_alloc_ids_for_request(
            &mut req,
            l0_tbls.len() + num_ln_files + blob_tbls.len(),
            estimated_num_files,
        )
        .await;
        let major_compaction = ColumnarMajorCompaction {
            safe_ts: self.get_keyspace_gc_safepoint_v2(shard.keyspace_id),
            l0_tables: l0_tbls.iter().map(|t| t.id()).collect(),
            ln_tables,
            blob_tables: blob_tbls.iter().map(|t| t.id()).collect(),
            old_columnar_tables,
            schema_file_id,
            table_ids: (tables_to_add, tables_to_clear),
            columnar_config,
            snap_version,
            target_level,
        };
        req.input_size = total_size;
        req.compaction_tp = CompactionType::ColumnarMajor(major_compaction);
        info!(
            "{} start columnar major compact from sst, num_ids: {}, input_size: {}",
            shard.tag(),
            req.file_ids.len(),
            total_size,
        );

        Some(self.comp_client.compact(req).await)
    }

    pub(crate) async fn trigger_columnar_major_compaction_from_col(
        &self,
        shard: &Shard,
        schema_version: i64,
    ) -> Option<Result<pb::ChangeSet>> {
        let mut req = self.new_compact_request_with_shard(shard);
        let data = shard.get_data();
        if !shard.opt.build_columnar() || data.schema_file.is_none() {
            warn!(
                "{} trigger_columnar_major_compaction_from_col: disabled or no schema, skip",
                shard.tag()
            );
            return None;
        }
        // Check if the schema file changed. Schema file may be updated since
        // refresh_compaction_priority.
        let schema_file = data.schema_file.as_ref().unwrap();
        if schema_version != schema_file.get_version() {
            warn!(
                "{} trigger_columnar_major_compaction_from_col: schema_version mismatch, schema_version: {}, schema_file_version: {}",
                shard.tag(),
                schema_version,
                schema_file.get_version()
            );
            return None;
        }
        let schema_file_id = data.schema_file.as_ref().unwrap().get_file_id();
        info!(
            "{} trigger_columnar_major_compaction_from_col, schema_version: {}",
            shard.tag(),
            schema_version
        );
        let mut snap_version = data.col_levels.l2_snap_version;
        let mut total_size = 0;
        let mut all_columnar_ids = vec![];
        let vec_index_version = data
            .vector_indexes
            .iter()
            .min_by_key(|v| v.snap_version())
            .map(|v| v.snap_version())
            .unwrap_or(SnapVersion::zero());
        for (level, files) in data.col_levels.levels.iter().enumerate() {
            for file in &files.files {
                if level < 2 {
                    let version = file.get_snap_version().unwrap();
                    // Keep the columnar file if has not converted to vector index.
                    if vec_index_version > SnapVersion::zero() && vec_index_version < version {
                        continue;
                    }
                    snap_version = snap_version.max(version);
                }
                all_columnar_ids.push((level as u32, file.get_file().id()));
                total_size += file.get_file().size();
            }
        }
        // columnar_table_ids is sorted.
        let columnar_table_ids = data.get_columnar_table_ids_in_schema();
        if all_columnar_ids.is_empty() {
            info!(
                "{} no columnar files to compact, skip trigger_columnar_major_compaction_from_col",
                shard.tag()
            );
            return None;
        }
        let num_col_files = all_columnar_ids.len();
        let columnar_compaction = ColumnarCompaction {
            level: 2,
            safe_ts: self.get_keyspace_gc_safepoint_v2(shard.keyspace_id),
            snap_version,
            source_row_files: vec![],
            source_columnar_files: all_columnar_ids,
            schema_file_id,
            columnar_table_ids,
            columnar_config: self.opts.columnar_build_options,
        };
        req.input_size = total_size;
        req.compaction_tp = CompactionType::Columnar(columnar_compaction);
        self.set_alloc_ids_for_request(&mut req, num_col_files, num_col_files)
            .await;
        info!(
            "{} start columnar major compact from col, num_ids: {}, input_size: {}",
            shard.tag(),
            req.file_ids.len(),
            total_size,
        );
        Some(self.comp_client.compact(req).await)
    }

    pub(crate) async fn trigger_columnar_l0_compaction(
        &self,
        shard: &Shard,
    ) -> Option<Result<pb::ChangeSet>> {
        let tag = shard.tag();
        let data = shard.get_data();
        let l0_tbls = &data.col_levels.levels[0].files;
        if l0_tbls.is_empty() {
            info!(
                "{} no columnar base level tables, skip trigger_columnar_l0_compaction",
                tag
            );
            return None;
        }
        let Some(schema_file) = data.schema_file.as_ref() else {
            info!(
                "{} no schema file found, skip trigger_columnar_l0_compaction",
                tag
            );
            return None;
        };
        let mut req = self.new_compact_request_with_shard(shard);
        let mut total_size = 0;
        let mut smallest = l0_tbls[0].get_smallest();
        let mut biggest = l0_tbls[0].get_biggest();
        let mut l0_tbl_ids = Vec::with_capacity(l0_tbls.len());
        let mut snap_version = SnapVersion::zero();
        for col in l0_tbls {
            if smallest > col.get_smallest() {
                smallest = col.get_smallest();
            }
            if biggest < col.get_biggest() {
                biggest = col.get_biggest();
            }
            let col_snap_version = col.get_snap_version().unwrap();
            snap_version = snap_version.max(col_snap_version);
            l0_tbl_ids.push((0, col.get_file().id()));
            total_size += col.get_file().size();
        }

        let schema_file_id = schema_file.get_file_id();
        let columnar_config = self.opts.columnar_build_options;
        let estimated_num_files = total_size as usize / columnar_config.max_columnar_table_size;
        self.set_alloc_ids_for_request(&mut req, l0_tbl_ids.len(), estimated_num_files)
            .await;
        // columnar_table_ids is sorted.
        let columnar_table_ids = data.get_columnar_table_ids_in_schema();
        let col_compaction = ColumnarCompaction {
            level: 0,
            safe_ts: self.get_keyspace_gc_safepoint_v2(shard.keyspace_id),
            snap_version,
            source_row_files: vec![],
            source_columnar_files: l0_tbl_ids,
            schema_file_id,
            columnar_table_ids,
            columnar_config,
        };
        req.input_size = total_size;
        req.compaction_tp = CompactionType::Columnar(col_compaction);
        info!("{} start columnar compact L0", tag; "total_size" => total_size);
        Some(self.comp_client.compact(req).await)
    }

    pub(crate) async fn trigger_columnar_l1_compaction(
        &self,
        shard: &Shard,
    ) -> Option<Result<pb::ChangeSet>> {
        let tag = shard.tag();
        let data = shard.get_data();
        let level = 1;
        let l1_tbls = &data.col_levels.levels[1].files;
        if l1_tbls.is_empty() {
            info!(
                "{} no columnar level 1 tables, skip trigger_columnar_l1_compaction",
                tag
            );
            return None;
        }
        let Some(schema_file) = data.schema_file.as_ref() else {
            info!(
                "{} no schema file found, skip trigger_columnar_l1_compaction",
                tag
            );
            return None;
        };

        let mut total_size = 0;

        let mut smallest = l1_tbls[0].get_smallest();
        let mut biggest = l1_tbls[0].get_biggest();
        let mut l1_tbl_ids = Vec::with_capacity(l1_tbls.len());
        let mut snap_version = SnapVersion::zero();
        for col in l1_tbls {
            if smallest > col.get_smallest() {
                smallest = col.get_smallest();
            }
            if biggest < col.get_biggest() {
                biggest = col.get_biggest();
            }
            let col_snap_version = col.get_snap_version().unwrap();
            snap_version = snap_version.max(col_snap_version);
            l1_tbl_ids.push((1, col.get_file().id()));
            total_size += col.get_file().size();
        }

        let mut l2_tbl_ids = vec![];
        let l2_tbls = &data.col_levels.levels[2].files;
        for tbl in l2_tbls {
            if tbl.get_biggest() < smallest || tbl.get_smallest() > biggest {
                info!(
                    "{} skip columnar table {} for compaction, level {}, tbl smallest {:?}, tbl biggest {:?}, base smallest {:?}, base biggest {:?}",
                    tag,
                    tbl.get_file().id(),
                    level,
                    tbl.get_smallest(),
                    tbl.get_biggest(),
                    smallest,
                    biggest,
                );
                continue;
            }
            l2_tbl_ids.push((2, tbl.get_file().id()));
            total_size += tbl.get_file().size();
        }

        let mut req = self.new_compact_request_with_shard(shard);

        let columnar_config = self.opts.columnar_build_options;
        let estimated_num_files = total_size as usize / columnar_config.max_columnar_table_size;
        self.set_alloc_ids_for_request(
            &mut req,
            l1_tbl_ids.len() + l2_tbl_ids.len(),
            estimated_num_files,
        )
        .await;
        let schema_file_id = schema_file.get_file_id();
        // columnar_table_ids is sorted.
        let columnar_table_ids = data.get_columnar_table_ids_in_schema();
        let columnar_compaction = ColumnarCompaction {
            level,
            safe_ts: self.get_keyspace_gc_safepoint_v2(shard.keyspace_id),
            snap_version,
            source_row_files: vec![],
            source_columnar_files: [l1_tbl_ids, l2_tbl_ids].concat(),
            schema_file_id,
            columnar_table_ids,
            columnar_config,
        };
        req.input_size = total_size;
        req.compaction_tp = CompactionType::Columnar(columnar_compaction);
        info!(
            "{} start compact columnar L{}, num_ids: {}, input_size: {}",
            tag,
            level,
            req.file_ids.len(),
            total_size,
        );
        Some(self.comp_client.compact(req).await)
    }

    pub(crate) async fn trigger_vector_index_update(
        &self,
        shard: &Shard,
        table_id: i64,
        index_id: i64,
        col_id: i64,
        rebuild: bool,
    ) -> Option<Result<pb::ChangeSet>> {
        let tag = shard.tag();
        let data = shard.get_data();
        // `Shard::get_columnar_snap_version` may not monotonically increase after
        // split, so we need to use the max of the vector index snap version and
        // the columnar snap version to avoid vector snap version rollback.
        let snap_version = std::cmp::max(
            shard
                .get_vector_index(table_id, index_id, col_id)
                .map(|v| v.snap_version())
                .unwrap_or_default(),
            shard.get_columnar_snap_version(),
        );
        let Some(schema_file) = data.schema_file.as_ref() else {
            warn!(
                "{} no schema file found, skip trigger_vector_index_update",
                tag
            );
            return None;
        };
        let vector_index_removed = if let Some(table_schema) = schema_file.get_table(table_id) {
            !table_schema
                .vector_indexes
                .iter()
                .any(|idx| idx.index_id == index_id && idx.col_id == col_id)
        } else {
            true
        };
        let mut col_file_ids = vec![];
        let mut total_size = 0;
        let mut req = self.new_compact_request_with_shard(shard);
        let mut remove_file_ids = vec![];
        // If the vector index is not in the schema, we need to remove the vector index.
        if vector_index_removed {
            if let Some(old_vec_idx) = data.vector_indexes.get(table_id, index_id, col_id) {
                for file in &old_vec_idx.files {
                    // The vector index will be removed from vector_indexes if all files of the
                    // vector index are removed.
                    remove_file_ids.push(file.file_id());
                }
            }
        } else {
            let old_idx_snap_ver =
                if let Some(old_vec_idx) = data.vector_indexes.get(table_id, index_id, col_id) {
                    if rebuild {
                        for file in &old_vec_idx.files {
                            remove_file_ids.push(file.file_id());
                        }
                        SnapVersion::zero()
                    } else {
                        old_vec_idx.snap_version()
                    }
                } else {
                    SnapVersion::zero()
                };
            for col_lvl in &data.col_levels.levels {
                for col_file in &col_lvl.files {
                    let col_snap_version = col_file.get_snap_version().unwrap_or(1.into());
                    if col_file.has_table(table_id) && col_snap_version > old_idx_snap_ver {
                        let f = col_file.get_file();
                        col_file_ids.push((f.id(), col_lvl.level as u32));
                        total_size += f.size();
                    }
                }
            }
        }
        let vector_index_update = VectorIndexUpdate {
            table_id,
            index_id,
            col_id,
            snap_version,
            schema_file_id: schema_file.get_file_id(),
            col_file_ids,
            remove_file_ids,
            vector_index_build_opts: self.opts.vector_index_build_options,
        };
        if vector_index_removed {
            info!(
                "{} trigger vector index removed in local, table_id: {}, index_id: {}, col_id: {}",
                tag, table_id, index_id, col_id
            );
            let mut cs = pb::ChangeSet::default();
            cs.set_shard_id(shard.id);
            cs.set_shard_ver(shard.ver);
            cs.set_update_vector_index(vector_index_update.to_pb_without_added());
            return Some(Ok(cs));
        }
        req.compaction_tp = CompactionType::VectorIndex(vector_index_update);
        req.input_size = total_size;
        info!("{} trigger vector index update", tag; "total_size" => total_size);
        Some(self.comp_client.compact(req).await)
    }

    pub(crate) fn handle_compact_response(&self, cs: pb::ChangeSet) {
        self.meta_change_listener.on_change_set(cs);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum CompactionPriority {
    L0 {
        score: f64,
    },
    L1Plus {
        cf: isize,
        score: f64,
        level: usize,
    },
    Major {
        score: f64,
        is_manual: bool,
    },
    DestroyRange,
    TruncateTs,
    TrimOverBound,
    GcLockFile,
    GcExtraFile,
    L0ToColumnar,
    ColumnarL0 {
        score: f64,
    },
    ColumnarL1 {
        score: f64,
    },
    ColumnarMajor {
        table_ids_to_add: Vec<i64>,
        table_ids_to_clear: Vec<i64>,
        is_manual: bool,
        schema_version: i64,
    },
    ColumnarClear,
    UpdateVectorIndex {
        score: f64,
        table_id: i64,
        index_id: i64,
        col_id: i64,
        rebuild: bool,
    },
    FtsCreateL0 {
        col_file_ids: Vec<u64>,
        active_tracked_indexes: Vec<(i64, i64)>, /* Only these indexes within tracked index will
                                                  * be built */
        snap_version: SnapVersion,
    },
    FtsAddIndex {
        new_indexes: Vec<(i64, i64)>,
    },
    FtsDropIndex {
        dropped_indexes: Vec<(i64, i64)>,
    },
    FtsCompactL0,
    FtsCompactL2 {
        lp_key: Vec<u8>,
        file_ids: Vec<u64>,
    },
    FtsCleanup {
        l1_file_ids: Vec<u64>,
        l2_file_ids: Vec<u64>,
    },
}

impl CompactionPriority {
    pub(crate) fn score(&self) -> f64 {
        match self {
            CompactionPriority::L0 { score } => *score,
            CompactionPriority::L1Plus { score, .. } => *score,
            CompactionPriority::Major { score, .. } => *score,
            CompactionPriority::DestroyRange => f64::MAX,
            CompactionPriority::TruncateTs => f64::MAX,
            CompactionPriority::TrimOverBound => f64::MAX,
            CompactionPriority::GcLockFile => 1.5,
            CompactionPriority::GcExtraFile => 1.5,
            CompactionPriority::L0ToColumnar => 2.0,
            CompactionPriority::ColumnarL0 { score } => *score,
            CompactionPriority::ColumnarL1 { score } => *score,
            CompactionPriority::ColumnarMajor { .. } => 1.5,
            CompactionPriority::ColumnarClear => f64::MAX,
            CompactionPriority::UpdateVectorIndex { score, .. } => *score,
            CompactionPriority::FtsCreateL0 { .. } => 2.1,
            CompactionPriority::FtsAddIndex { .. } => 2.2,
            CompactionPriority::FtsDropIndex { .. } => 2.3,
            CompactionPriority::FtsCompactL0 => 1.9,
            CompactionPriority::FtsCompactL2 { .. } => 1.8,
            CompactionPriority::FtsCleanup { .. } => 1.7,
        }
    }

    pub(crate) fn level(&self) -> isize {
        match self {
            CompactionPriority::L0 { .. } => 0,
            CompactionPriority::L1Plus { level, .. } => *level as isize,
            CompactionPriority::Major { .. } => -1,
            CompactionPriority::DestroyRange => -1,
            CompactionPriority::TruncateTs => -1,
            CompactionPriority::TrimOverBound => -1,
            CompactionPriority::GcLockFile => -1,
            CompactionPriority::GcExtraFile => -1,
            CompactionPriority::L0ToColumnar => 0,
            CompactionPriority::ColumnarL0 { .. } => 0,
            CompactionPriority::ColumnarL1 { .. } => 1,
            CompactionPriority::ColumnarMajor { .. } => -1,
            CompactionPriority::ColumnarClear => -1,
            CompactionPriority::UpdateVectorIndex { .. } => -1,
            CompactionPriority::FtsCreateL0 { .. } => 0,
            CompactionPriority::FtsAddIndex { .. } => 0,
            CompactionPriority::FtsDropIndex { .. } => 0,
            CompactionPriority::FtsCompactL0 => 0,
            CompactionPriority::FtsCompactL2 { .. } => 0,
            CompactionPriority::FtsCleanup { .. } => 0,
        }
    }

    pub(crate) fn cf(&self) -> isize {
        match self {
            CompactionPriority::L0 { .. } => -1,
            CompactionPriority::L1Plus { cf, .. } => *cf,
            CompactionPriority::Major { .. } => -1,
            CompactionPriority::DestroyRange => -1,
            CompactionPriority::TruncateTs => -1,
            CompactionPriority::TrimOverBound => -1,
            CompactionPriority::GcLockFile => 1,
            CompactionPriority::GcExtraFile => 2,
            CompactionPriority::L0ToColumnar => 0,
            CompactionPriority::ColumnarL0 { .. } => -1,
            CompactionPriority::ColumnarL1 { .. } => -1,
            CompactionPriority::ColumnarMajor { .. } => -1,
            CompactionPriority::ColumnarClear => -1,
            CompactionPriority::UpdateVectorIndex { .. } => -1,
            CompactionPriority::FtsCreateL0 { .. } => -1,
            CompactionPriority::FtsAddIndex { .. } => -1,
            CompactionPriority::FtsDropIndex { .. } => -1,
            CompactionPriority::FtsCompactL0 => -1,
            CompactionPriority::FtsCompactL2 { .. } => -1,
            CompactionPriority::FtsCleanup { .. } => -1,
        }
    }
}

impl Ord for CompactionPriority {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.score()
            .partial_cmp(&other.score())
            .unwrap_or(CmpOrdering::Equal)
    }
}

impl PartialOrd for CompactionPriority {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        self.score().partial_cmp(&other.score())
    }
}

impl PartialEq for CompactionPriority {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == CmpOrdering::Equal
    }
}

impl Eq for CompactionPriority {}

#[derive(Clone, Default)]
pub(crate) struct KeyRange {
    pub left: Bytes,
    pub right: Bytes,
}

impl KeyRange {
    pub(crate) fn left_key(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(&self.left)
    }

    pub(crate) fn right_key(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(&self.right)
    }

    pub(crate) fn update(&mut self, tables: &[SsTable]) {
        if tables.is_empty() {
            return;
        }
        let lower_smallest = tables.first().unwrap().smallest();
        if self.left.is_empty() || lower_smallest < self.left_key() {
            self.left = Bytes::copy_from_slice(lower_smallest.deref());
        }
        let lower_biggest = tables.last().unwrap().biggest();
        if lower_biggest > self.right_key() {
            self.right = Bytes::copy_from_slice(lower_biggest.deref());
        }
    }
}

impl BoundedDataSet for KeyRange {
    fn data_bound(&self) -> DataBound<'_> {
        DataBound::new(self.left_key(), self.right_key(), true)
    }
}

async fn load_table_files(
    tbl_ids: &[u64],
    fs: Arc<dyn dfs::Dfs>,
    opts: dfs::Options,
    local_dirs: &[PathBuf],
    for_restore: bool,
) -> Result<Vec<Arc<dyn File>>> {
    if local_dirs.is_empty() {
        return load_table_files_from_dfs(tbl_ids, fs, opts).await;
    }

    let (mut files_loaded, files_failed) =
        load_table_files_from_local(tbl_ids, local_dirs, for_restore, opts);
    if !files_failed.is_empty() {
        files_loaded.extend(load_table_files_from_dfs(&files_failed, fs, opts).await?);
    }
    Ok(files_loaded)
}

fn load_table_files_from_local(
    tbl_ids: &[u64],
    local_dirs: &[PathBuf],
    for_restore: bool,
    opts: dfs::Options,
) -> (
    Vec<Arc<dyn File>>, // files_loaded
    Vec<u64>,           // files_failed
) {
    let mut files_loaded: Vec<Arc<dyn File>> = vec![];
    let mut files_failed: Vec<u64> = vec![];
    for &id in tbl_ids {
        let file_name = match opts.file_type {
            FileType::Sst => new_sst_filename(id),
            FileType::Schema => new_schema_filename(id),
            FileType::Columnar => new_columnar_filename(id),
            FileType::TxnChunk => unreachable!("txn chunk should not be loaded from local"),
            FileType::VectorIndex => new_vector_index_filename(id),
            FileType::Blob => new_blob_filename(id),
            FileType::FtsPackedFile => new_fts_packed_filename(id),
            FileType::FtsDedicatedFile => new_fts_dedicated_filename(id),
        };
        let local_dir = get_local_dir(local_dirs, id);
        let file_path = local_dir.join(file_name);
        match LocalFile::open(id, file_path) {
            Ok(f) => files_loaded.push(Arc::new(f)),
            Err(e) => {
                files_failed.push(id);
                if !for_restore {
                    warn!(
                        "load_table_files_from_local: failed to open {}: {:?}",
                        id, e
                    );
                    metrics::ENGINE_LOAD_TABLE_FILES_ERROR
                        .with_label_values(&["compaction"])
                        .inc();
                }
            }
        }
    }
    (files_loaded, files_failed)
}

async fn load_table_files_from_dfs(
    tbl_ids: &[u64],
    fs: Arc<dyn dfs::Dfs>,
    opts: dfs::Options,
) -> Result<Vec<Arc<dyn File>>> {
    let mut tasks = Vec::with_capacity(tbl_ids.len());
    for &id in tbl_ids {
        let afs = fs.clone();
        let task = fs.get_runtime().spawn(async move {
            afs.read_file(id, opts)
                .await
                .map(|data| (id, data))
                .map_err(|e| Error::DfsError(e))
        });
        tasks.push(task);
    }
    let results: Vec<(u64, Bytes)> = try_join_all(tasks)
        .await
        .map_err(|e| -> Error { box_err!("load_table_files_from_dfs: {:?}", e) })?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    let files: Vec<Arc<dyn File>> = results
        .into_iter()
        .map(|(id, data)| Arc::new(InMemFile::new(id, data)) as _)
        .collect();
    debug_assert_eq!(files.len(), tbl_ids.len());
    Ok(files)
}

fn files_to_l0_tables(
    files: Vec<Arc<dyn File>>,
    encryption_key: Option<EncryptionKey>,
) -> Vec<sstable::L0Table> {
    files
        .into_iter()
        .map(|f| {
            sstable::L0Table::new(f, BlockCache::None, false, encryption_key.clone())
                .unwrap()
                .unwrap()
        })
        .collect()
}

fn files_to_tables(
    files: Vec<Arc<dyn File>>,
    encryption_key: Option<EncryptionKey>,
) -> Vec<sstable::SsTable> {
    files
        .into_iter()
        .map(|f| sstable::SsTable::new(f, BlockCache::None, encryption_key.clone()).unwrap())
        .collect()
}

fn files_to_columnar_tables(
    files: Vec<Arc<dyn File>>,
    columnar_meta_cache: ColumnarMetaCache,
) -> Vec<ColumnarFile> {
    files
        .into_iter()
        .map(|f| ColumnarFile::open(f, None, columnar_meta_cache.clone()).unwrap())
        .collect()
}

async fn load_blob_tables(
    fs: Arc<dyn dfs::Dfs>,
    blob_table_ids: &[u64],
    opts: dfs::Options,
) -> Result<HashMap<u64, BlobTable>> {
    let mut tasks = Vec::with_capacity(blob_table_ids.len());
    for &id in blob_table_ids {
        let afs = fs.clone();
        let task = fs.get_runtime().spawn(async move {
            afs.read_file(id, opts)
                .await
                .map(|data| (id, data))
                .map_err(|e| Error::DfsError(e))
        });
        tasks.push(task);
    }
    let results = try_join_all(tasks)
        .await
        .map_err(|e| -> Error { box_err!("load_blob_tables: {:?}", e) })?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    let mut blob_tables = HashMap::new();
    for (id, data) in results {
        blob_tables.insert(id, BlobTable::from_bytes(data)?);
    }
    debug_assert_eq!(blob_tables.len(), blob_table_ids.len());
    Ok(blob_tables)
}

pub struct CompactionCtx {
    pub req: Arc<CompactionRequest>,
    pub dfs: Arc<dyn dfs::Dfs>,
    pub compression_lvl: i32,
    pub checksum_type: ChecksumType,
    pub id_allocator: Arc<dyn IdAllocator>,
    pub encryption_key: Option<EncryptionKey>,
    pub local_dirs: Vec<PathBuf>,
    pub for_restore: bool,
    pub columnar_meta_cache: ColumnarMetaCache,
}

fn merge_table_change(
    mut row_tbl: pb::TableChange,
    mut col_tbl: pb::TableChange,
) -> pb::TableChange {
    let mut tb = pb::TableChange::new();
    tb.set_table_deletes(row_tbl.take_table_deletes());
    tb.set_table_creates(row_tbl.take_table_creates());
    tb.set_columnar_deletes(col_tbl.take_columnar_deletes());
    tb.set_columnar_creates(col_tbl.take_columnar_creates());
    tb.file_ids_map = row_tbl.file_ids_map;
    tb.file_ids_map.extend(col_tbl.file_ids_map);
    tb
}

struct LocalIdAllocator {
    id_allocator: Arc<dyn IdAllocator>,
    file_ids: Vec<u64>,
    num_files_quota: usize,
}

impl LocalIdAllocator {
    fn new(id_allocator: Arc<dyn IdAllocator>, file_ids: Vec<u64>) -> Self {
        let num_files_quota = 4096usize.saturating_sub(file_ids.len());
        Self {
            id_allocator,
            file_ids,
            num_files_quota,
        }
    }

    async fn alloc_id(&mut self) -> u64 {
        if self.file_ids.is_empty() && self.num_files_quota > 0 {
            let allocated = self
                .id_allocator
                .alloc_id_async(std::cmp::min(64, self.num_files_quota))
                .await
                .unwrap_or_else(|e| {
                    panic!("failed to allocate id: {:?}", e);
                });
            self.file_ids.extend_from_slice(&allocated);
            self.num_files_quota -= allocated.len();
        }
        self.file_ids.pop().unwrap_or_else(|| {
            panic!("compaction runs out of file ids");
        })
    }
}

pub async fn local_compact(ctx: &CompactionCtx) -> Result<pb::ChangeSet> {
    let req = &ctx.req;
    if req.compactor_version != CURRENT_COMPACTOR_VERSION {
        return Err(CompactionNotRetryable(format!(
            "invalid compactor_version {}",
            req.compactor_version
        )));
    }
    let mut cs = pb::ChangeSet::new();
    cs.set_shard_id(req.shard_id);
    cs.set_shard_ver(req.shard_ver);
    let tag = ShardTag::from_comp_req(req);
    info!("start compaction for {}, req {:?}", tag, req);
    let mut id_allocator = LocalIdAllocator::new(ctx.id_allocator.clone(), req.file_ids.clone());
    match &req.compaction_tp {
        CompactionType::InPlaceWithColumnar(InPlaceCompactionCtx {
            file_ids,
            col_file_ids,
            block_size,
            columnar_build_opts,
            schema_file_id,
            columnar_table_ids,
            fts_tasks,
            fts_packed_build_opts,
            fts_dedicated_build_opts,
            spec,
        }) => match spec {
            InPlaceCompaction::DestroyRange(del_prefix) => {
                let row_tb = if !file_ids.is_empty() {
                    compact_destroy_range(ctx, *block_size, file_ids, &mut id_allocator, del_prefix)
                        .await?
                } else {
                    pb::TableChange::new()
                };
                let col_tb = if !col_file_ids.is_empty() {
                    compact_destroy_range_for_columnar(
                        ctx,
                        columnar_build_opts,
                        col_file_ids,
                        &mut id_allocator,
                        *schema_file_id,
                        columnar_table_ids,
                        del_prefix,
                    )
                    .await?
                } else {
                    pb::TableChange::new()
                };
                cs.set_destroy_range(merge_table_change(row_tb, col_tb));
                let update = compact_fts_inplace(
                    ctx,
                    spec,
                    fts_tasks,
                    fts_packed_build_opts,
                    fts_dedicated_build_opts,
                    &mut id_allocator,
                )
                .await?;
                append_fts_update(&mut cs, update);
            }
            InPlaceCompaction::TrimOverBound => {
                let row_tb = if !file_ids.is_empty() {
                    compact_trim_over_bound(ctx, *block_size, file_ids, &mut id_allocator).await?
                } else {
                    pb::TableChange::new()
                };
                let col_tb = if !col_file_ids.is_empty() {
                    compact_trim_over_bound_for_columnar(
                        ctx,
                        columnar_build_opts,
                        col_file_ids,
                        &mut id_allocator,
                        *schema_file_id,
                        columnar_table_ids,
                    )
                    .await?
                } else {
                    pb::TableChange::new()
                };
                cs.set_trim_over_bound(merge_table_change(row_tb, col_tb));
                let update = compact_fts_inplace(
                    ctx,
                    spec,
                    fts_tasks,
                    fts_packed_build_opts,
                    fts_dedicated_build_opts,
                    &mut id_allocator,
                )
                .await?;
                append_fts_update(&mut cs, update);
            }
            InPlaceCompaction::TruncateTs(truncate_ts) => {
                let row_tb = if !file_ids.is_empty() {
                    compact_truncate_ts(ctx, *block_size, file_ids, &mut id_allocator, *truncate_ts)
                        .await?
                } else {
                    pb::TableChange::new()
                };
                let col_tb = if !col_file_ids.is_empty() {
                    compact_truncate_ts_for_columnar(
                        ctx,
                        columnar_build_opts,
                        col_file_ids,
                        &mut id_allocator,
                        *schema_file_id,
                        columnar_table_ids,
                        *truncate_ts,
                    )
                    .await?
                } else {
                    pb::TableChange::new()
                };
                cs.set_truncate_ts(merge_table_change(row_tb, col_tb));
                let update = compact_fts_inplace(
                    ctx,
                    spec,
                    fts_tasks,
                    fts_packed_build_opts,
                    fts_dedicated_build_opts,
                    &mut id_allocator,
                )
                .await?;
                append_fts_update(&mut cs, update);
            }
            InPlaceCompaction::Unknown => unreachable!(),
        },
        CompactionType::L0(l0_compaction) => {
            cs.set_compaction(l0_compact(ctx, l0_compaction, &mut id_allocator).await?);
        }
        CompactionType::L1Plus(l1_plus_compaction) => {
            cs.set_compaction(l1_plus_compact(ctx, l1_plus_compaction, &mut id_allocator).await?);
        }
        CompactionType::Major(major_compaction) => {
            cs.set_major_compaction(major_compact(ctx, major_compaction, &mut id_allocator).await?);
        }
        CompactionType::Columnar(columnar_compaction) => {
            cs.set_columnar_compaction(
                columnar_compact(ctx, columnar_compaction, &mut id_allocator).await?,
            );
        }
        CompactionType::ColumnarMajor(columnar_major_compaction) => {
            cs.set_columnar_compaction(
                columnar_major_compact(ctx, columnar_major_compaction, &mut id_allocator).await?,
            );
        }
        CompactionType::VectorIndex(update_vec_idx) => {
            cs.set_update_vector_index(
                update_vector_index(ctx, update_vec_idx, &mut id_allocator).await?,
            );
        }
        CompactionType::FtsCreateL0(fts_create_l0_req) => {
            cs.set_fts_update(
                fts_create_l0_update(ctx, fts_create_l0_req, &mut id_allocator).await?,
            );
        }
        CompactionType::FtsAddIndex(fts_add_index_req) => {
            cs.set_fts_update(
                fts_add_index_update(ctx, fts_add_index_req, &mut id_allocator).await?,
            );
        }
        CompactionType::FtsCompactL0(fts_l0_compact_req) => {
            cs.set_fts_update(fts_l0_compact(ctx, fts_l0_compact_req, &mut id_allocator).await?);
        }
        CompactionType::FtsCompactL2(fts_l2_compact_req) => {
            cs.set_fts_update(fts_l2_compact(ctx, fts_l2_compact_req, &mut id_allocator).await?);
        }
        CompactionType::FtsCleanup(fts_cleanup_req) => {
            cs.set_fts_update(fts_cleanup(ctx, fts_cleanup_req).await?);
        }
        CompactionType::Unknown => unreachable!(),
    }
    Ok(cs)
}

pub fn build_inplace_spec_from_ctx(
    spec: &InPlaceCompaction,
    req: &CompactionRequest,
) -> Result<InplaceSpec> {
    match spec {
        InPlaceCompaction::TrimOverBound => {
            let start = req.inner_start().deref().to_vec();
            let end = req.inner_end().deref().to_vec();
            Ok(InplaceSpec::TrimOverbound(FtsKeyRange {
                start: Some(start),
                end: Some(end),
            }))
        }
        InPlaceCompaction::TruncateTs(ts) => Ok(InplaceSpec::TruncateTs { truncate_ts: *ts }),
        InPlaceCompaction::DestroyRange(data) => {
            let prefixes = DeletePrefixes::unmarshal(data, req.keyspace_id())
                .prefixes
                .into_iter()
                .map(|p| OwnedInnerKey::new(Bytes::from(p)).to_vec())
                .collect();
            Ok(InplaceSpec::DestroyRange { prefixes })
        }
        InPlaceCompaction::Unknown => Err(box_err!("unknown inplace compaction spec")),
    }
}

async fn compact_fts_inplace(
    ctx: &CompactionCtx,
    spec: &InPlaceCompaction,
    tasks: &[FtsInplaceTask],
    fts_packed_build_opts: &PackedFileBuilderOptions,
    fts_dedicated_build_opts: &DedicatedFileBuilderOptions,
    id_allocator: &mut LocalIdAllocator,
) -> Result<pb::FtsUpdate> {
    if tasks.is_empty() {
        return Ok(pb::FtsUpdate::default());
    }
    let inplace_spec = build_inplace_spec_from_ctx(spec, &ctx.req)?;
    let mut update = pb::FtsUpdate::default();

    let mut packed_ids = Vec::new();
    let mut dedicated_ids = Vec::new();
    for task in tasks {
        match task.kind {
            FtsFileKind::L0 | FtsFileKind::L1 => packed_ids.push(task.id),
            FtsFileKind::L2 => dedicated_ids.push(task.id),
        }
    }

    let mut packed_files = HashMap::new();
    if !packed_ids.is_empty() {
        let opts = dfs::Options::default()
            .with_shard(ctx.req.shard_id, ctx.req.shard_ver)
            .with_type(FileType::FtsPackedFile);
        let files = load_table_files(
            &packed_ids,
            ctx.dfs.clone(),
            opts,
            &ctx.local_dirs,
            ctx.for_restore,
        )
        .await?;
        for file in files {
            let file_id = file.id();
            let packed = PackedFile::new(file, FtsCache::disabled()).map_err(|e| -> Error {
                box_err!("Failed to open FTS packed file {}: {}", file_id, e)
            })?;
            packed_files.insert(file_id, packed);
        }
    }

    let mut dedicated_files = HashMap::new();
    if !dedicated_ids.is_empty() {
        let opts = dfs::Options::default()
            .with_shard(ctx.req.shard_id, ctx.req.shard_ver)
            .with_type(FileType::FtsDedicatedFile);
        let files = load_table_files(
            &dedicated_ids,
            ctx.dfs.clone(),
            opts,
            &ctx.local_dirs,
            ctx.for_restore,
        )
        .await?;
        for file in files {
            let file_id = file.id();
            let dedicated =
                EDedicatedFile::new(file, FtsCache::disabled()).map_err(|e| -> Error {
                    box_err!("Failed to open FTS dedicated file {}: {}", file_id, e)
                })?;
            dedicated_files.insert(file_id, dedicated);
        }
    }

    let packed_dfs_opts = dfs::Options::default()
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver)
        .with_type(FileType::FtsPackedFile);
    let dedicated_dfs_opts = dfs::Options::default()
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver)
        .with_type(FileType::FtsDedicatedFile);

    for task in tasks {
        match task.kind {
            FtsFileKind::L0 => {
                let file = packed_files.get(&task.id).ok_or_else(|| -> Error {
                    box_err!("missing packed file {} for FTS compaction", task.id)
                })?;
                match rewrite_packed_file(file, &inplace_spec, fts_packed_build_opts)
                    .await
                    .map_err(|e| -> Error {
                        box_err!("Failed to rewrite FTS packed file {}: {}", task.id, e)
                    })? {
                    InplaceResult::NoChange => {}
                    InplaceResult::Removed => {
                        update.mut_l0_remove_files().push(task.id);
                    }
                    InplaceResult::Rewritten { data, summary } => {
                        let new_id = id_allocator.alloc_id().await;
                        ctx.dfs
                            .create(new_id, data, packed_dfs_opts)
                            .await
                            .map_err(Error::DfsError)?;
                        let info = PackedFile::info(new_id, summary.meta_offset, summary.props);
                        update.mut_l0_add_files().push(info);
                        update.mut_l0_remove_files().push(task.id);
                    }
                }
            }
            FtsFileKind::L1 => {
                let file = packed_files.get(&task.id).ok_or_else(|| -> Error {
                    box_err!("missing packed file {} for FTS compaction", task.id)
                })?;
                match rewrite_packed_file(file, &inplace_spec, fts_packed_build_opts)
                    .await
                    .map_err(|e| -> Error {
                        box_err!("Failed to rewrite FTS packed file {}: {}", task.id, e)
                    })? {
                    InplaceResult::NoChange => {}
                    InplaceResult::Removed => {
                        update.mut_l1_remove_files().push(task.id);
                    }
                    InplaceResult::Rewritten { data, summary } => {
                        let new_id = id_allocator.alloc_id().await;
                        ctx.dfs
                            .create(new_id, data, packed_dfs_opts)
                            .await
                            .map_err(Error::DfsError)?;
                        let info = PackedFile::info(new_id, summary.meta_offset, summary.props);
                        update.mut_l1_add_files().push(info);
                        update.mut_l1_remove_files().push(task.id);
                    }
                }
            }
            FtsFileKind::L2 => {
                let file = dedicated_files.get(&task.id).ok_or_else(|| -> Error {
                    box_err!("missing dedicated file {} for FTS compaction", task.id)
                })?;
                match rewrite_dedicated_file(file, &inplace_spec, fts_dedicated_build_opts)
                    .await
                    .map_err(|e| -> Error {
                        box_err!("Failed to rewrite FTS dedicated file {}: {}", task.id, e)
                    })? {
                    InplaceResult::NoChange => {}
                    InplaceResult::Removed => {
                        update.mut_l2_remove_files().push(task.id);
                    }
                    InplaceResult::Rewritten { data, summary } => {
                        let new_id = id_allocator.alloc_id().await;
                        ctx.dfs
                            .create(new_id, data, dedicated_dfs_opts)
                            .await
                            .map_err(Error::DfsError)?;
                        let info = DedicatedFile::info(new_id, summary.meta_offset, summary.props);
                        update.mut_l2_add_files().push(info);
                        update.mut_l2_remove_files().push(task.id);
                    }
                }
            }
        }
    }

    Ok(update)
}

/// Compact files in place to remove data covered by delete prefixes.
async fn compact_destroy_range(
    ctx: &CompactionCtx,
    block_size: usize,
    files: &[(u64, u32, i32)],
    id_allocator: &mut LocalIdAllocator,
    del_prefix: &[u8],
) -> Result<pb::TableChange> {
    let req = &ctx.req;
    let dfs = &ctx.dfs;
    let compression_lvl = ctx.compression_lvl;
    let checksum_type = ctx.checksum_type;
    assert!(!del_prefix.is_empty() && !files.is_empty());
    assert!(files.len() <= req.file_ids.len());

    let opts = dfs::Options::default().with_shard(req.shard_id, req.shard_ver);
    let mut table_files: HashMap<u64, Arc<dyn File>> = load_table_files(
        &files.iter().map(|(id, ..)| *id).collect::<Vec<_>>(),
        dfs.clone(),
        opts,
        &ctx.local_dirs,
        ctx.for_restore,
    )
    .await?
    .into_iter()
    .map(|file| (file.id(), file))
    .collect();

    let mut destroy = pb::TableChange::new();
    let mut deletes = vec![];
    let mut creates = vec![];
    let del_prefixes = DeletePrefixes::unmarshal(del_prefix, req.keyspace_id());
    let mut tasks = Vec::with_capacity(req.file_ids.len());
    for &(id, level, cf) in files.iter() {
        let new_id = id_allocator.alloc_id().await;
        let mut delete = pb::TableDelete::new();
        delete.set_id(id);
        delete.set_level(level);
        delete.set_cf(cf);
        deletes.push(delete);
        let file = table_files.remove(&id).unwrap();
        let (data, smallest, biggest, meta_offset) = if level == 0 {
            let t =
                sstable::L0Table::new(file, BlockCache::None, false, ctx.encryption_key.clone())
                    .unwrap()
                    .unwrap();
            let mut builder = L0Builder::new(
                new_id,
                block_size,
                t.snap_version(),
                checksum_type,
                ctx.encryption_key.clone(),
            );
            for cf in 0..NUM_CFS {
                if let Some(cf_t) = t.get_cf(cf) {
                    let mut iter = cf_t.new_iterator(false, false);
                    iter.seek(req.inner_start());
                    while iter.valid() {
                        let key = iter.key();
                        if key >= req.inner_end() {
                            break;
                        }
                        if !del_prefixes.cover_prefix(key) {
                            builder.add(cf, key, &iter.value(), None);
                        }
                        iter.next_all_version();
                    }
                }
            }
            if builder.is_empty() {
                continue;
            }
            let (mut l0_create, data) = builder.finish();
            (data, l0_create.take_smallest(), l0_create.take_biggest(), 0)
        } else {
            let t =
                sstable::SsTable::new(file, BlockCache::None, ctx.encryption_key.clone()).unwrap();
            let mut builder = sstable::Builder::new(
                new_id,
                block_size,
                t.compression_type(),
                compression_lvl,
                ctx.checksum_type,
                ctx.encryption_key.clone(),
            );
            let mut iter = t.new_iterator(false, false);
            iter.seek(req.inner_start());
            while iter.valid() {
                let key = iter.key();
                if key >= req.inner_end() {
                    break;
                }
                if !del_prefixes.cover_prefix(key) {
                    builder.add(key, &iter.value(), None);
                }
                iter.next_all_version();
            }
            if builder.is_empty() {
                continue;
            }
            let mut buf = Vec::with_capacity(builder.estimated_size());
            let res = builder.finish(0, &mut buf);
            (buf.into(), res.smallest, res.biggest, res.meta_offset)
        };
        let dfs_clone = dfs.clone();
        let task = dfs.get_runtime().spawn(async move {
            dfs_clone
                .create(new_id, data, opts)
                .await
                .map_err(|e| Error::DfsError(e))
        });
        tasks.push(task);
        let create = new_table_create_pb(new_id, level, cf, smallest, biggest, meta_offset);
        creates.push(create);

        destroy.mut_file_ids_map().push(id);
        destroy.mut_file_ids_map().push(new_id);
    }
    let results: Vec<()> = try_join_all(tasks)
        .await
        .map_err(|e| -> Error { box_err!("compact_destroy_range: {:?}", e) })?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    debug_assert_eq!(creates.len(), results.len());

    destroy.set_table_deletes(deletes.into());
    destroy.set_table_creates(creates.into());
    Ok(destroy)
}

fn clear_all_columnar_files(files: &[(u64, u32)]) -> pb::TableChange {
    let mut deletes = vec![];
    for &(id, level) in files.iter() {
        let mut delete = pb::ColumnarDelete::new();
        delete.set_id(id);
        delete.set_level(level);
        deletes.push(delete);
    }
    let mut tc = pb::TableChange::new();
    tc.set_columnar_deletes(deletes.into());
    tc
}

async fn compact_destroy_range_for_columnar(
    ctx: &CompactionCtx,
    columnar_build_opts: &ColumnarTableBuildOptions,
    files: &[(u64, u32)],
    id_allocator: &mut LocalIdAllocator,
    schema_file_id: Option<u64>,
    columnar_table_ids: &[i64],
    del_prefix: &[u8],
) -> Result<pb::TableChange> {
    let req = &ctx.req;
    let dfs = &ctx.dfs;
    assert!(!del_prefix.is_empty() && !files.is_empty());

    if schema_file_id.is_none() || schema_file_id.unwrap() == 0 {
        return Ok(clear_all_columnar_files(files));
    }

    let opts = dfs::Options::default()
        .with_shard(req.shard_id, req.shard_ver)
        .with_type(FileType::Columnar);
    let mut columnar_files: HashMap<u64, Arc<dyn File>> = load_table_files(
        &files.iter().map(|(id, ..)| *id).collect::<Vec<_>>(),
        dfs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        ctx.for_restore,
    )
    .await?
    .into_iter()
    .map(|file| (file.id(), file))
    .collect();

    let schema_file = {
        let file = load_table_files(
            &[schema_file_id.unwrap()],
            dfs.clone(),
            dfs::Options::default().with_type(FileType::Schema),
            ctx.local_dirs.as_ref(),
            false,
        )
        .await?
        .pop()
        .unwrap();
        SchemaFile::open(file).unwrap()
    };

    let mut deletes = vec![];
    let mut creates = vec![];
    let del_prefixes = Arc::new(DeletePrefixes::unmarshal(del_prefix, req.keyspace_id()));
    let (tx, mut rx) = mpsc::channel(req.file_ids.len());
    let mut cnt = 0;
    let keyspace_id = ApiV2::get_u32_keyspace_id_by_key(&req.outer_start).unwrap_or_default();
    // TODO: remove the sort here after tikv-server upgraded.
    let mut columnar_table_ids = columnar_table_ids.to_vec();
    columnar_table_ids.sort();
    for &(id, level) in files.iter() {
        let file = columnar_files.remove(&id).unwrap();
        let columnar_file =
            ColumnarFile::open(file, None, ctx.columnar_meta_cache.clone()).unwrap();
        let mut delete = pb::ColumnarDelete::new();
        delete.set_id(id);
        delete.set_level(level);
        deletes.push(delete);
        if columnar_table_ids.is_empty() {
            continue;
        }
        let mut file_builder = ColumnarFileBuilder::new(
            id_allocator.alloc_id().await,
            columnar_file.get_snap_version(),
            ctx.encryption_key.clone(),
        );
        for &table_id in &columnar_table_ids {
            let table_row_key_prefix = [
                api_version::ApiV2::get_txn_keyspace_prefix(keyspace_id),
                encode_row_key(table_id, 0),
            ]
            .concat();
            // destroy range used by drop table in TiDB, if the row key be covered by delete
            // prefixes, it means the table is dropped.
            let inner_key = InnerKey::from_outer_key(&table_row_key_prefix);
            if del_prefixes.cover_prefix(inner_key) {
                continue;
            }
            if !columnar_file.has_table(table_id) {
                continue;
            }
            let schema = schema_file.get_table(table_id).unwrap();
            let reader = ColumnarTableReader::new(
                &columnar_file,
                schema.clone(),
                None,
                ctx.encryption_key.clone(),
            );
            let mut compact_reader =
                ColumnarCompactReader::new(Box::new(reader), level, schema, req.safe_ts);
            compact_reader.set_unbounded_handle_range().await?;
            compact_table_for_columnar(
                ctx,
                &mut compact_reader,
                &mut file_builder,
                schema,
                level,
                &mut cnt,
                tx.clone(),
                columnar_build_opts,
                id_allocator,
            )
            .await?;
        }
        if file_builder.num_tables() > 0 {
            cnt += 1;
            persist_columnar_file(level, &mut file_builder, tx.clone(), ctx.dfs.clone(), opts);
        }
    }
    let mut errors = vec![];
    for _ in 0..cnt {
        match rx.recv().await.unwrap() {
            Err(err) => errors.push(err),
            Ok(create) => {
                creates.push(create);
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }
    let mut destroy = pb::TableChange::new();
    destroy.set_columnar_deletes(deletes.into());
    destroy.set_columnar_creates(creates.into());
    Ok(destroy)
}

async fn compact_truncate_ts(
    ctx: &CompactionCtx,
    block_size: usize,
    files: &[(u64, u32, i32)],
    id_allocator: &mut LocalIdAllocator,
    truncate_ts: u64,
) -> Result<pb::TableChange> {
    let req = &ctx.req;
    let dfs = &ctx.dfs;
    let compression_lvl = ctx.compression_lvl;
    let checksum_type = ctx.checksum_type;
    assert!(!files.is_empty());

    let opts = dfs::Options::default().with_shard(req.shard_id, req.shard_ver);
    let mut table_files: HashMap<u64, Arc<dyn File>> = load_table_files(
        &files.iter().map(|(id, ..)| *id).collect::<Vec<_>>(),
        dfs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        ctx.for_restore,
    )
    .await?
    .into_iter()
    .map(|file| (file.id(), file))
    .collect();

    let mut table_change = pb::TableChange::new();
    let mut deletes = vec![];
    let mut creates = vec![];
    let mut tasks = Vec::with_capacity(req.file_ids.len());
    for &(id, level, cf) in files.iter() {
        let new_id = id_allocator.alloc_id().await;
        let file = table_files.remove(&id).unwrap();
        let mut delete = pb::TableDelete::new();
        delete.set_id(id);
        delete.set_level(level);
        delete.set_cf(cf);
        deletes.push(delete);

        let (data, smallest, biggest, meta_offset) = if level == 0 {
            let t =
                sstable::L0Table::new(file, BlockCache::None, false, ctx.encryption_key.clone())
                    .unwrap()
                    .unwrap();
            let mut builder = L0Builder::new(
                new_id,
                block_size,
                t.snap_version(),
                checksum_type,
                ctx.encryption_key.clone(),
            );
            for cf in 0..NUM_CFS {
                if let Some(cf_t) = t.get_cf(cf) {
                    let mut iter = cf_t.new_iterator(false, false);
                    iter.rewind();
                    while iter.valid() {
                        let key = iter.key();
                        let value = iter.value();
                        // Keep data in LOCK_CF. Locks would be resolved by TiDB.
                        // TODO: handle async commit
                        if cf == LOCK_CF || value.version <= truncate_ts {
                            builder.add(cf, key, &value, None);
                        }
                        iter.next_all_version();
                    }
                }
            }
            if builder.is_empty() {
                continue;
            }
            let (mut l0_create, data) = builder.finish();
            (data, l0_create.take_smallest(), l0_create.take_biggest(), 0)
        } else {
            let t =
                sstable::SsTable::new(file, BlockCache::None, ctx.encryption_key.clone()).unwrap();
            let mut builder = sstable::Builder::new(
                new_id,
                block_size,
                t.compression_type(),
                compression_lvl,
                checksum_type,
                ctx.encryption_key.clone(),
            );
            let mut iter = t.new_iterator(false, false);
            iter.rewind();
            while iter.valid() {
                let key = iter.key();
                let value = iter.value();
                // Keep data in LOCK_CF. Locks would be resolved by TiDB.
                // TODO: handle async commit
                if cf as usize == LOCK_CF || value.version <= truncate_ts {
                    builder.add(key, &value, None);
                }
                iter.next_all_version();
            }
            if builder.is_empty() {
                continue;
            }
            let mut buf = Vec::with_capacity(builder.estimated_size());
            let res = builder.finish(0, &mut buf);
            (buf.into(), res.smallest, res.biggest, res.meta_offset)
        };

        let dfs_clone = dfs.clone();
        let task = dfs.get_runtime().spawn(async move {
            dfs_clone
                .create(new_id, data, opts)
                .await
                .map_err(|e| Error::DfsError(e))
        });
        tasks.push(task);

        let create = new_table_create_pb(new_id, level, cf, smallest, biggest, meta_offset);
        creates.push(create);

        table_change.mut_file_ids_map().push(id);
        table_change.mut_file_ids_map().push(new_id);
    }

    let results: Vec<()> = try_join_all(tasks)
        .await
        .map_err(|e| -> Error { box_err!("compact_truncate_ts: {:?}", e) })?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    debug_assert_eq!(creates.len(), results.len());

    table_change.set_table_deletes(deletes.into());
    table_change.set_table_creates(creates.into());
    Ok(table_change)
}

async fn compact_truncate_ts_for_columnar(
    ctx: &CompactionCtx,
    columnar_build_opts: &ColumnarTableBuildOptions,
    files: &[(u64, u32)],
    id_allocator: &mut LocalIdAllocator,
    schema_file_id: Option<u64>,
    columnar_table_ids: &[i64],
    truncate_ts: u64,
) -> Result<pb::TableChange> {
    let req = &ctx.req;
    let dfs = &ctx.dfs;
    assert!(!files.is_empty());

    if schema_file_id.is_none() || schema_file_id.unwrap() == 0 {
        return Ok(clear_all_columnar_files(files));
    }

    let opts = dfs::Options::default()
        .with_shard(req.shard_id, req.shard_ver)
        .with_type(FileType::Columnar);
    let mut columnar_files: HashMap<u64, Arc<dyn File>> = load_table_files(
        &files.iter().map(|(id, ..)| *id).collect::<Vec<_>>(),
        dfs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        ctx.for_restore,
    )
    .await?
    .into_iter()
    .map(|file| (file.id(), file))
    .collect();

    let schema_file = {
        let file = load_table_files(
            &[schema_file_id.unwrap()],
            dfs.clone(),
            dfs::Options::default().with_type(FileType::Schema),
            ctx.local_dirs.as_ref(),
            false,
        )
        .await?
        .pop()
        .unwrap();
        SchemaFile::open(file).unwrap()
    };

    let mut deletes = vec![];
    let mut creates = vec![];
    let (tx, mut rx) = mpsc::channel(req.file_ids.len());
    let mut cnt = 0;
    // TODO: remove the sort here after tikv-server upgraded.
    let mut columnar_table_ids = columnar_table_ids.to_vec();
    columnar_table_ids.sort();
    for &(id, level) in files.iter() {
        let file = columnar_files.remove(&id).unwrap();
        let columnar_file =
            ColumnarFile::open(file, None, ctx.columnar_meta_cache.clone()).unwrap();
        let mut delete = pb::ColumnarDelete::new();
        delete.set_id(id);
        delete.set_level(level);
        deletes.push(delete);

        if columnar_table_ids.is_empty() {
            continue;
        }
        let mut file_builder = ColumnarFileBuilder::new(
            id_allocator.alloc_id().await,
            columnar_file.get_snap_version(),
            ctx.encryption_key.clone(),
        );
        for &table_id in &columnar_table_ids {
            let schema = schema_file.get_table(table_id).unwrap();
            if !columnar_file.has_table(table_id) {
                continue;
            }
            let reader = ColumnarTableReader::new(
                &columnar_file,
                schema.clone(),
                None,
                ctx.encryption_key.clone(),
            );
            let mut truncate_ts_reader =
                ColumnarTruncateTsReader::new(Box::new(reader), schema, truncate_ts);
            truncate_ts_reader.set_unbounded_handle_range().await?;
            compact_table_for_columnar(
                ctx,
                &mut truncate_ts_reader,
                &mut file_builder,
                schema,
                level,
                &mut cnt,
                tx.clone(),
                columnar_build_opts,
                id_allocator,
            )
            .await?;
        }
        if file_builder.num_tables() > 0 {
            cnt += 1;
            persist_columnar_file(level, &mut file_builder, tx.clone(), ctx.dfs.clone(), opts);
        }
    }
    let mut errors = vec![];
    for _ in 0..cnt {
        match rx.recv().await.unwrap() {
            Err(err) => errors.push(err),
            Ok(create) => {
                creates.push(create);
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }
    let mut table_change = pb::TableChange::new();
    table_change.set_columnar_deletes(deletes.into());
    table_change.set_columnar_creates(creates.into());
    info!(
        "finish columnar truncate_ts compaction, table_change {:?}",
        table_change
    );
    Ok(table_change)
}

/// Compact files in place to remove data out of shard bound.
async fn compact_trim_over_bound(
    ctx: &CompactionCtx,
    block_size: usize,
    files: &[(u64, u32, i32)],
    id_allocator: &mut LocalIdAllocator,
) -> Result<pb::TableChange> {
    let req = &ctx.req;
    let dfs = &ctx.dfs;
    let compression_lvl = ctx.compression_lvl;
    let checksum_tp = ctx.checksum_type;
    assert!(!files.is_empty());
    assert!(files.len() <= req.file_ids.len());

    let opts = dfs::Options::default().with_shard(req.shard_id, req.shard_ver);
    let mut table_files: HashMap<u64, Arc<dyn File>> = load_table_files(
        &files.iter().map(|(id, ..)| *id).collect::<Vec<_>>(),
        dfs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        ctx.for_restore,
    )
    .await?
    .into_iter()
    .map(|file| (file.id(), file))
    .collect();

    let mut table_change = pb::TableChange::new();
    let mut deletes = vec![];
    let mut creates = vec![];
    let mut tasks = Vec::with_capacity(req.file_ids.len());
    for &(id, level, cf) in files.iter() {
        let new_id = id_allocator.alloc_id().await;
        let file = table_files.remove(&id).unwrap();

        let mut delete = pb::TableDelete::new();
        delete.set_id(id);
        delete.set_level(level);
        delete.set_cf(cf);
        deletes.push(delete);

        let (data, smallest, biggest, meta_offset) = if level == 0 {
            let t =
                sstable::L0Table::new(file, BlockCache::None, false, ctx.encryption_key.clone())
                    .unwrap()
                    .unwrap();
            let mut builder = L0Builder::new(
                new_id,
                block_size,
                t.snap_version(),
                checksum_tp,
                ctx.encryption_key.clone(),
            );
            for cf in 0..NUM_CFS {
                if let Some(cf_t) = t.get_cf(cf) {
                    let mut iter = cf_t.new_iterator(false, false);
                    iter.seek(req.inner_start());
                    while iter.valid() {
                        let key = iter.key();
                        if key >= req.inner_end() {
                            break;
                        }
                        builder.add(cf, key, &iter.value(), None);
                        iter.next_all_version();
                    }
                }
            }
            if builder.is_empty() {
                continue;
            }
            let (mut l0_create, data) = builder.finish();
            (data, l0_create.take_smallest(), l0_create.take_biggest(), 0)
        } else {
            let t =
                sstable::SsTable::new(file, BlockCache::None, ctx.encryption_key.clone()).unwrap();
            let mut builder = sstable::Builder::new(
                new_id,
                block_size,
                t.compression_type(),
                compression_lvl,
                checksum_tp,
                ctx.encryption_key.clone(),
            );
            let mut iter = t.new_iterator(false, false);
            iter.seek(req.inner_start());
            while iter.valid() {
                let key = iter.key();
                if key >= req.inner_end() {
                    break;
                }
                builder.add(key, &iter.value(), None);
                iter.next_all_version();
            }
            if builder.is_empty() {
                continue;
            }
            let mut buf = Vec::with_capacity(builder.estimated_size());
            let res = builder.finish(0, &mut buf);
            (buf.into(), res.smallest, res.biggest, res.meta_offset)
        };
        let dfs_clone = dfs.clone();
        let task = dfs.get_runtime().spawn(async move {
            dfs_clone
                .create(new_id, data, opts)
                .await
                .map_err(|e| Error::DfsError(e))
        });
        tasks.push(task);

        let create = new_table_create_pb(new_id, level, cf, smallest, biggest, meta_offset);
        creates.push(create);

        table_change.mut_file_ids_map().push(id);
        table_change.mut_file_ids_map().push(new_id);
    }

    let results: Vec<()> = try_join_all(tasks)
        .await
        .map_err(|e| -> Error { box_err!("compact_trim_over_bound: {:?}", e) })?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    debug_assert_eq!(creates.len(), results.len());

    table_change.set_table_deletes(deletes.into());
    table_change.set_table_creates(creates.into());
    Ok(table_change)
}

async fn compact_trim_over_bound_for_columnar(
    ctx: &CompactionCtx,
    columnar_build_opts: &ColumnarTableBuildOptions,
    files: &[(u64, u32)],
    id_allocator: &mut LocalIdAllocator,
    schema_file_id: Option<u64>,
    columnar_table_ids: &[i64],
) -> Result<pb::TableChange> {
    let req = &ctx.req;
    let dfs = &ctx.dfs;
    assert!(!files.is_empty());
    assert!(files.len() <= req.file_ids.len());

    if schema_file_id.is_none() || schema_file_id.unwrap() == 0 {
        return Ok(clear_all_columnar_files(files));
    }

    let opts = dfs::Options::default()
        .with_shard(req.shard_id, req.shard_ver)
        .with_type(FileType::Columnar);
    let mut columnar_files: HashMap<u64, Arc<dyn File>> = load_table_files(
        &files.iter().map(|(id, ..)| *id).collect::<Vec<_>>(),
        dfs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        ctx.for_restore,
    )
    .await?
    .into_iter()
    .map(|file| (file.id(), file))
    .collect();

    let schema_file = {
        let file = load_table_files(
            &[schema_file_id.unwrap()],
            dfs.clone(),
            dfs::Options::default().with_type(FileType::Schema),
            ctx.local_dirs.as_ref(),
            false,
        )
        .await?
        .pop()
        .unwrap();
        SchemaFile::open(file).unwrap()
    };

    let mut deletes = vec![];
    let mut creates = vec![];
    let (tx, mut rx) = mpsc::channel(req.file_ids.len());
    let mut cnt = 0;
    // TODO: remove the sort here after tikv-server upgraded.
    let mut columnar_table_ids = columnar_table_ids.to_vec();
    columnar_table_ids.sort();
    for &(id, level) in files.iter() {
        let file = columnar_files.remove(&id).unwrap();
        let columnar_file =
            ColumnarFile::open(file, None, ctx.columnar_meta_cache.clone()).unwrap();
        let mut delete = pb::ColumnarDelete::new();
        delete.set_id(id);
        delete.set_level(level);
        deletes.push(delete);
        if columnar_table_ids.is_empty() {
            continue;
        }
        let bound_start =
            if let Some(keyspace_prefix) = ApiV2::get_keyspace_prefix(&req.outer_start) {
                &req.outer_start[keyspace_prefix.len()..]
            } else {
                &req.outer_start
            };
        let bound_end = if let Some(keyspace_prefix) = ApiV2::get_keyspace_prefix(&req.outer_end) {
            if req.outer_end.len() == keyspace_prefix.len() {
                GLOBAL_SHARD_END_KEY
            } else {
                &req.outer_end[keyspace_prefix.len()..]
            }
        } else {
            &req.outer_end
        };
        let mut file_builder = ColumnarFileBuilder::new(
            id_allocator.alloc_id().await,
            columnar_file.get_snap_version(),
            ctx.encryption_key.clone(),
        );
        for &table_id in &columnar_table_ids {
            let row_key_prefix = encode_row_key_prefix(table_id);
            let mut row_key_prefix_next = row_key_prefix.clone();
            convert_to_prefix_next(&mut row_key_prefix_next);
            if bound_start >= row_key_prefix_next.as_slice()
                || bound_end <= row_key_prefix.as_slice()
            {
                // The whole table is over bound.
                continue;
            }
            if !columnar_file.has_table(table_id) {
                continue;
            }
            let schema = schema_file.get_table(table_id).unwrap();
            let reader = ColumnarTableReader::new(
                &columnar_file,
                schema.clone(),
                None,
                ctx.encryption_key.clone(),
            );
            let mut compact_reader =
                ColumnarCompactReader::new(Box::new(reader), level, schema, req.safe_ts);
            if schema.is_common_handle() {
                let start_handle = if bound_start > row_key_prefix.as_slice() {
                    decode_common_handle(bound_start).unwrap()
                } else {
                    &[]
                };
                let end_handle = if bound_end < row_key_prefix_next.as_slice() {
                    decode_common_handle(bound_end).unwrap()
                } else {
                    GLOBAL_COMMON_HANDLE_END
                };
                compact_reader
                    .set_handle_range(start_handle, end_handle)
                    .await?;
            } else {
                let start_handle = if bound_start > row_key_prefix.as_slice() {
                    decode_int_handle(bound_start).unwrap()
                } else {
                    i64::MIN
                };
                let end_handle = if bound_end < row_key_prefix_next.as_slice() {
                    Some(decode_int_handle(bound_end).unwrap())
                } else {
                    None
                };
                compact_reader
                    .set_int_handle_range(start_handle, end_handle)
                    .await?;
            }
            compact_table_for_columnar(
                ctx,
                &mut compact_reader,
                &mut file_builder,
                schema,
                level,
                &mut cnt,
                tx.clone(),
                columnar_build_opts,
                id_allocator,
            )
            .await?;
        }
        if file_builder.num_tables() > 0 {
            cnt += 1;
            persist_columnar_file(level, &mut file_builder, tx.clone(), ctx.dfs.clone(), opts);
        }
    }

    let mut errors = vec![];
    for _ in 0..cnt {
        match rx.recv().await.unwrap() {
            Err(err) => errors.push(err),
            Ok(create) => {
                creates.push(create);
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }
    let mut table_change = pb::TableChange::new();
    table_change.set_columnar_deletes(deletes.into());
    table_change.set_columnar_creates(creates.into());
    info!(
        "finish columnar trim over bound compaction, table_change: {:?}",
        table_change
    );
    Ok(table_change)
}

#[derive(Debug)]
enum FilePersistResult {
    TableCreate(pb::TableCreate),
    BlobTableCreate(pb::BlobCreate),
}

fn persist_sst(
    id: u64,
    cf: usize,
    target_lvl: u32,
    builder: &mut sstable::Builder,
    tx: mpsc::Sender<dfs::Result<FilePersistResult>>,
    fs: Arc<dyn dfs::Dfs>,
    opts: dfs::Options,
) {
    let mut buf = Vec::with_capacity(builder.estimated_size());
    let res = builder.finish(0, &mut buf);
    let tbl_create = new_table_create_pb(
        id,
        target_lvl,
        cf as i32,
        res.smallest,
        res.biggest,
        res.meta_offset,
    );

    let fs_clone = fs.clone();
    fs.get_runtime().spawn(async move {
        let _ = tx
            .send(
                fs_clone
                    .create(id, buf.into(), opts)
                    .await
                    .map(|_| FilePersistResult::TableCreate(tbl_create)),
            )
            .await
            .map_err(|e| warn!("{} persist_sst: {:?}", id, e));
    });
}

fn persist_blob_table(
    id: u64,
    builder: &mut BlobTableBuilder,
    tx: mpsc::Sender<dfs::Result<FilePersistResult>>,
    fs: Arc<dyn dfs::Dfs>,
    opts: dfs::Options,
) {
    let buf = builder.finish();
    let (smallest, biggest) = builder.smallest_biggest_key();
    let meta_off = builder.meta_offset() as u32;
    let blob_table_create = new_blob_create_pb(id, smallest.to_vec(), biggest.to_vec(), meta_off);
    let fs_clone = fs.clone();
    fs.get_runtime().spawn(async move {
        let _ = tx
            .send(
                fs_clone
                    .create(id, buf, opts)
                    .await
                    .map(|_| FilePersistResult::BlobTableCreate(blob_table_create)),
            )
            .await
            .map_err(|e| warn!("{} persist_blob_table: {:?}", id, e));
    });
}

fn persist_columnar_file(
    target_lvl: u32,
    builder: &mut ColumnarFileBuilder,
    tx: mpsc::Sender<dfs::Result<pb::ColumnarCreate>>,
    fs: Arc<dyn dfs::Dfs>,
    opts: dfs::Options,
) {
    let id = builder.file_id;
    let (buf, meta_offset) = builder.build();
    let columnar_create = new_columnar_create_pb(
        id,
        target_lvl,
        builder.smallest.clone(),
        builder.biggest.clone(),
        meta_offset as u32,
        builder.get_snap_version(),
    );
    let fs_clone = fs.clone();
    fs.get_runtime().spawn(async move {
        let _ = tx
            .send(
                fs_clone
                    .create(id, buf.into(), opts.with_type(FileType::Columnar))
                    .await
                    .map(|_| columnar_create),
            )
            .await
            .map_err(|e| warn!("{} persist_columnar_file: {:?}", id, e));
    });
}

async fn compact_for_cf(
    ctx: &CompactionCtx,
    iter: &mut Box<dyn table::Iterator>,
    safe_ts: u64,
    opts: dfs::Options,
    cf: usize,
    target_lvl: u32,
    sst_config: &TableBuilderOptions,
    bt_config: &Option<BlobTableBuildOptions>,
    keep_latest_obsolete_tombstone: bool,
    blob_tables: &HashMap<u64, BlobTable>,
    id_allocator: &mut LocalIdAllocator,
) -> Result<(Vec<TableCreate>, Vec<BlobCreate>)> {
    let shard_id = ctx.req.shard_id;
    let start = ctx.req.inner_start();
    let end = ctx.req.inner_end();
    let fs = &ctx.dfs;
    let compression_lvl = ctx.compression_lvl;
    let checksum_tp = ctx.checksum_type;
    let (tx, mut rx) = mpsc::channel(ctx.req.file_ids.len());
    let mut cur_sst_id = id_allocator.alloc_id().await;

    let mut sst_builder = sstable::Builder::new(
        cur_sst_id,
        sst_config.block_size,
        sst_config.compression_tps[target_lvl as usize - 1],
        compression_lvl,
        checksum_tp,
        ctx.encryption_key.clone(),
    );
    if cf == LOCK_CF {
        sst_builder.set_is_lock_cf();
    }
    let mut cur_blob_table_id = 0;
    // Owns the decompressed blob value while reading from the orginal blob
    // table, to reduce the memory re-allocation.
    let mut decompressed_blob_buf = vec![];
    let mut decryption_buf = vec![];
    let mut blob_table_builder = if let Some(config) = bt_config {
        cur_blob_table_id = id_allocator.alloc_id().await;
        Some((
            config,
            BlobTableBuilder::new(
                cur_blob_table_id,
                config.compression_type,
                compression_lvl,
                config.min_blob_size,
                config.block_size,
                ctx.encryption_key.clone(),
            ),
        ))
    } else {
        None
    };
    let mut cnt = 0;
    let mut last_key = BytesMut::new();
    // Whether to skip the entry if it has the same key as the last entry iterated.
    // This will be set to true when:
    //   * The last entry is the latest version of a key that is before the GC safe
    //     point.
    //   * Or this function is iterating the LOCK CF.
    let mut skip_same_key = false;
    iter.seek(start);
    while iter.valid() && iter.key() < end {
        let mut val = iter.value();
        let key = iter.key();
        // See if we need to skip this key.
        if key.deref() == last_key {
            if skip_same_key {
                iter.next_all_version();
                continue;
            }
        } else {
            // A new key is met.
            if !last_key.is_empty() {
                // Check if we need to rotate to a new SST or blob table.
                if sst_builder.estimated_size() > sst_config.max_table_size {
                    cnt += 1;
                    persist_sst(
                        cur_sst_id,
                        cf,
                        target_lvl,
                        &mut sst_builder,
                        tx.clone(),
                        fs.clone(),
                        opts,
                    );
                    cur_sst_id = id_allocator.alloc_id().await;
                    sst_builder.reset(cur_sst_id);
                }
                if let Some((bt_config, bt_builder)) = &mut blob_table_builder {
                    if bt_builder.total_blob_size() as usize > bt_config.max_blob_table_size {
                        cnt += 1;
                        persist_blob_table(
                            cur_blob_table_id,
                            bt_builder,
                            tx.clone(),
                            fs.clone(),
                            opts.with_type(FileType::Blob),
                        );
                        cur_blob_table_id = id_allocator.alloc_id().await;
                        bt_builder.reset(cur_blob_table_id);
                    }
                }
            }
            last_key.clear();
            last_key.extend_from_slice(key.deref());
            skip_same_key = false;
        }

        // Only consider the versions which are below the minReadTs, otherwise, we might
        // end up discarding the only valid version for a running transaction.
        if cf == LOCK_CF || val.version <= safe_ts {
            // key is the latest readable version of this key, so we simply discard all the
            // rest of the versions.

            skip_same_key = true;

            if val.is_deleted() {
                if !keep_latest_obsolete_tombstone {
                    iter.next_all_version();
                    continue;
                }
            } else {
                let user_meta = val.user_meta();
                if user_meta.len() == USER_META_SIZE {
                    let um = UserMeta::from_slice(user_meta);
                    if cf == WRITE_CF && um.commit_ts < safe_ts && val.is_value_empty() {
                        if keep_latest_obsolete_tombstone {
                            sst_builder.add(key, &table::Value::new_tombstone(val.version), None);
                        }
                        iter.next_all_version();
                        continue;
                    }
                    if cf == EXTRA_CF && um.start_ts < safe_ts {
                        iter.next_all_version();
                        continue;
                    }
                }
            }
        }
        if let Some((bt_config, bt_builder)) = &mut blob_table_builder {
            let mut need_recompress_blob = false;
            if val.is_blob_ref() {
                if blob_tables.is_empty() {
                    sst_builder.add(key, &val, None);
                } else {
                    // If it is a blob link, we need to deref it and may need to decompress it as
                    // well.
                    let blob_ref = val.get_blob_ref();
                    let blob_table = blob_tables.get(&blob_ref.fid).unwrap_or_else(|| {
                        panic!(
                            "[{}] blob table {} not found for key {:?}",
                            shard_id, blob_ref.fid, key,
                        )
                    });
                    // If the encryption ver is changed, we should decrypt the blob value and
                    // encrypt with the new encryption ver.
                    let need_re_encrypt = if let Some(encryption_key) = &ctx.encryption_key {
                        blob_table.encryption_ver != encryption_key.current_ver
                    } else {
                        false
                    };
                    if blob_table.compression_tp() != bt_config.compression_type
                        || blob_table.min_blob_size() != bt_config.min_blob_size
                    {
                        need_recompress_blob = true;
                    }
                    // The blob value is either the original blob value or the compressed blob
                    // depending on whether need_recompress_blob is true.
                    let blob_or_compressed_blob = blob_table
                        .get_from_preloaded(
                            &blob_ref,
                            need_recompress_blob,
                            need_re_encrypt,
                            &mut decompressed_blob_buf,
                            &mut decryption_buf,
                            ctx.encryption_key.clone(),
                        )
                        .unwrap();
                    // The value needs to be added to the blob table, if:
                    //   1. need_recompress_blob is false, which means the val was already in the
                    //      blob table, and it is guaranteed that the blob size is larger than
                    //      min_blob_size.
                    //   2. need_recompress_blob is true, but the blob size is larger than
                    //      min_blob_size.
                    if !need_recompress_blob
                        || blob_or_compressed_blob.len() >= bt_config.min_blob_size as usize
                    {
                        let new_blob_ref = bt_builder.add_blob(
                            key,
                            blob_or_compressed_blob,
                            Some(blob_ref.original_len),
                            need_re_encrypt,
                        );
                        sst_builder.add(key, &val, Some(new_blob_ref));
                    } else {
                        val.fill_in_blob(blob_or_compressed_blob);
                        sst_builder.add(key, &val, None);
                    }
                }
            } else if val.value_len() >= bt_config.min_blob_size as usize {
                let blob_ref = bt_builder.add(key, &val);
                val.set_blob_ref();
                sst_builder.add(key, &val, Some(blob_ref));
            } else {
                sst_builder.add(key, &val, None);
            }
        } else {
            // If we are not building blob tables, we can write to the SST blindly.
            sst_builder.add(key, &val, None);
        }
        iter.next_all_version();
    }
    if !sst_builder.is_empty() {
        cnt += 1;
        persist_sst(
            cur_sst_id,
            cf,
            target_lvl,
            &mut sst_builder,
            tx.clone(),
            fs.clone(),
            opts,
        );
    }
    if let Some((_, bt_builder)) = &mut blob_table_builder {
        if !bt_builder.is_empty() {
            cnt += 1;
            persist_blob_table(
                cur_blob_table_id,
                bt_builder,
                tx,
                fs.clone(),
                opts.with_type(FileType::Blob),
            );
        }
    }
    let mut errors = vec![];
    let mut sst_creates = vec![];
    let mut blob_table_creates = vec![];
    for _ in 0..cnt {
        match rx.recv().await.unwrap() {
            Err(err) => errors.push(err),
            Ok(FilePersistResult::TableCreate(tbl_create)) => {
                sst_creates.push(tbl_create);
            }
            Ok(FilePersistResult::BlobTableCreate(blob_table_create)) => {
                blob_table_creates.push(blob_table_create);
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }
    Ok((sst_creates, blob_table_creates))
}

async fn l0_compact(
    ctx: &CompactionCtx,
    l0_compaction: &L0Compaction,
    id_allocator: &mut LocalIdAllocator,
) -> Result<pb::Compaction> {
    let req = &ctx.req;
    let fs = &ctx.dfs;
    let opts = dfs::Options::default().with_shard(req.shard_id, req.shard_ver);
    let l0_files = load_table_files(
        &l0_compaction.l0_tables,
        fs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        ctx.for_restore,
    )
    .await?;
    let mut l0_tbls = files_to_l0_tables(l0_files, ctx.encryption_key.clone());
    l0_tbls.sort_by(|a, b| b.snap_version().cmp(&a.snap_version()));
    let mut comp = pb::Compaction::new();
    comp.set_top_deletes(l0_compaction.l0_tables.clone());
    comp.set_level(0_u32);
    comp.set_bottom_deletes(
        l0_compaction
            .multi_cf_l1_tables
            .clone()
            .into_iter()
            .flatten()
            .collect(),
    );
    let mut all_sst_creates = vec![];
    let mut all_bt_creates = vec![];
    for cf in 0..NUM_CFS {
        let l1_ids = &l0_compaction.multi_cf_l1_tables[cf];
        let l1_files = load_table_files(
            l1_ids,
            fs.clone(),
            opts,
            ctx.local_dirs.as_ref(),
            ctx.for_restore,
        )
        .await?;
        let mut l1_tbls = files_to_tables(l1_files, ctx.encryption_key.clone());
        l1_tbls.sort_by(|a, b| a.smallest().cmp(&b.smallest()));
        let mut iters: Vec<Box<dyn table::Iterator>> = vec![];
        for l0_tbl in &l0_tbls {
            if let Some(tbl) = l0_tbl.get_cf(cf) {
                let iter = tbl.new_iterator(false, false);
                iters.push(iter);
            }
        }
        if !l1_tbls.is_empty() {
            let iter = ConcatIterator::new_with_tables(l1_tbls, false, false);
            iters.push(Box::new(iter));
        }
        let mut iter = table::new_merge_iterator(iters, false);
        let (sst_creates, bt_creates) = compact_for_cf(
            ctx,
            &mut iter,
            l0_compaction.safe_ts,
            opts,
            cf,
            1,
            &l0_compaction.sst_config,
            &l0_compaction.bt_config,
            //&None,
            true,
            &HashMap::new(),
            id_allocator,
        )
        .await?;
        all_sst_creates.extend(sst_creates);
        all_bt_creates.extend(bt_creates);
    }
    comp.set_table_creates(all_sst_creates.into());
    comp.set_blob_tables(all_bt_creates.into());
    Ok(comp)
}

async fn l1_plus_compact(
    ctx: &CompactionCtx,
    l1_plus_compaction: &L1PlusCompaction,
    id_allocator: &mut LocalIdAllocator,
) -> Result<pb::Compaction> {
    let req = &ctx.req;
    let fs = &ctx.dfs;
    let opts = dfs::Options::default().with_shard(req.shard_id, req.shard_ver);
    let upper_files = load_table_files(
        &l1_plus_compaction.upper_level,
        fs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        ctx.for_restore,
    )
    .await?;
    let mut upper_tables = files_to_tables(upper_files, ctx.encryption_key.clone());
    upper_tables.sort_by(|a, b| a.smallest().cmp(&b.smallest()));
    let lower_files = load_table_files(
        &l1_plus_compaction.lower_level,
        fs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        ctx.for_restore,
    )
    .await?;
    let mut lower_tables = files_to_tables(lower_files, ctx.encryption_key.clone());
    lower_tables.sort_by(|a, b| a.smallest().cmp(&b.smallest()));
    let upper_iter = Box::new(ConcatIterator::new_with_tables(upper_tables, false, false));
    let lower_iter = Box::new(ConcatIterator::new_with_tables(lower_tables, false, false));
    let mut iter = table::new_merge_iterator(vec![upper_iter, lower_iter], false);

    let (sst_creates, _) = compact_for_cf(
        ctx,
        &mut iter,
        l1_plus_compaction.safe_ts,
        opts,
        l1_plus_compaction.cf as usize,
        l1_plus_compaction.level as u32 + 1,
        &l1_plus_compaction.sst_config,
        &None,
        l1_plus_compaction.keep_latest_obsolete_tombstone,
        &HashMap::new(),
        id_allocator,
    )
    .await?;
    let mut comp = pb::Compaction::new();
    comp.set_top_deletes(l1_plus_compaction.upper_level.clone());
    comp.set_cf(l1_plus_compaction.cf as i32);
    comp.set_level(l1_plus_compaction.level as u32);
    comp.set_table_creates(sst_creates.into());
    comp.set_bottom_deletes(l1_plus_compaction.lower_level.clone());
    Ok(comp)
}

async fn major_compact(
    ctx: &CompactionCtx,
    major_compaction: &MajorCompaction,
    id_allocator: &mut LocalIdAllocator,
) -> Result<pb::MajorCompaction> {
    let req = &ctx.req;
    let fs = &ctx.dfs;
    let opts = dfs::Options::default().with_shard(req.shard_id, req.shard_ver);
    let mut ret = pb::MajorCompaction::new();
    ret.mut_old_blob_tables()
        .extend_from_slice(&major_compaction.blob_tables);
    let blob_tables = load_blob_tables(fs.clone(), &major_compaction.blob_tables, opts).await?;
    let l0_files = load_table_files(
        &major_compaction.l0_tables,
        fs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        ctx.for_restore,
    )
    .await?;
    let mut l0_tbls = files_to_l0_tables(l0_files, ctx.encryption_key.clone());
    l0_tbls.sort_by(|a, b| b.snap_version().cmp(&a.snap_version()));
    l0_tbls.iter().for_each(|tbl| {
        let mut tbl_delete = pb::TableDelete::new();
        tbl_delete.set_id(tbl.id());
        ret.mut_sstable_change()
            .mut_table_deletes()
            .push(tbl_delete);
    });
    for cf in 0..NUM_CFS {
        let mut iters: Vec<Box<dyn table::Iterator>> = vec![];
        for l0_tbl in &l0_tbls {
            if let Some(tbl) = l0_tbl.get_cf(cf) {
                let iter = tbl.new_iterator(false, false);
                iters.push(iter);
            }
        }
        if let Some(sstables) = major_compaction.ln_tables.get(&cf) {
            for (level, table_ids) in sstables.iter() {
                table_ids.iter().for_each(|t| {
                    let mut tbl_delete = pb::TableDelete::new();
                    tbl_delete.set_cf(cf as i32);
                    tbl_delete.set_level(*level as u32);
                    tbl_delete.set_id(*t);
                    ret.mut_sstable_change()
                        .mut_table_deletes()
                        .push(tbl_delete);
                });
                let files = load_table_files(
                    table_ids,
                    fs.clone(),
                    opts,
                    ctx.local_dirs.as_ref(),
                    ctx.for_restore,
                )
                .await?;
                let mut tables = files_to_tables(files, ctx.encryption_key.clone());
                tables.sort_by(|a, b| a.smallest().cmp(&b.smallest()));
                let level_concat_iter =
                    Box::new(ConcatIterator::new_with_tables(tables, false, false));
                iters.push(level_concat_iter);
            }
        }
        if iters.is_empty() {
            continue;
        }
        let mut iter = table::new_merge_iterator(iters, false);
        let (sst_creates, blob_table_creates) = compact_for_cf(
            ctx,
            &mut iter,
            major_compaction.safe_ts,
            opts,
            cf,
            CF_LEVELS[cf] as u32,
            &major_compaction.sst_config,
            &major_compaction.bt_config,
            false,
            &blob_tables,
            id_allocator,
        )
        .await?;
        for sst_create in sst_creates {
            ret.mut_sstable_change()
                .mut_table_creates()
                .push(sst_create);
        }
        for blob_table_create in blob_table_creates {
            ret.mut_new_blob_tables().push(blob_table_create);
        }
    }
    Ok(ret)
}

async fn compact_table_for_columnar(
    ctx: &CompactionCtx,
    reader: &mut dyn ColumnarFilterReader,
    file_builder: &mut ColumnarFileBuilder,
    schema: &Schema,
    target_lvl: u32,
    cnt: &mut usize,
    tx: mpsc::Sender<dfs::Result<pb::ColumnarCreate>>,
    columnar_config: &ColumnarTableBuildOptions,
    id_allocator: &mut LocalIdAllocator,
) -> Result<()> {
    let fs = &ctx.dfs;
    let opts = dfs::Options::default()
        .with_type(FileType::Columnar)
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver);
    let mut block = Block::new(schema);
    let mut res = reader
        .read_block(&mut block, columnar_config.pack_max_row_count)
        .await?;
    let mut tbl_builder = ColumnarTableBuilder::new(
        schema.clone(),
        *columnar_config,
        ctx.encryption_key.clone(),
        file_builder.file_id,
        target_lvl,
    );
    let mut block_offset = 0;
    while res > 0 {
        block_offset = tbl_builder.append_block(&block, block_offset);
        let estimated_size = tbl_builder.get_estimated_size() + file_builder.estimated_size;
        if estimated_size > columnar_config.max_columnar_table_size {
            file_builder.add_table(tbl_builder);
            *cnt += 1;
            persist_columnar_file(target_lvl, file_builder, tx.clone(), fs.clone(), opts);
            file_builder.reset(id_allocator.alloc_id().await);
            tbl_builder = ColumnarTableBuilder::new(
                schema.clone(),
                *columnar_config,
                ctx.encryption_key.clone(),
                file_builder.file_id,
                target_lvl,
            );
        }
        if block_offset < block.length() {
            res = block.length() - block_offset;
        } else {
            block.reset();
            block_offset = 0;
            res = reader
                .read_block(&mut block, columnar_config.pack_max_row_count)
                .await?;
        }
    }
    if !tbl_builder.is_empty() {
        file_builder.add_table(tbl_builder);
    }
    Ok(())
}

async fn transform_for_columnar(
    ctx: &CompactionCtx,
    tbls: &Vec<SsTable>,
    blob_tbls: Option<Arc<HashMap<u64, BlobTable>>>,
    table_ids: Vec<i64>,
    schema_file: &SchemaFile,
    target_lvl: u32,
    snap_version: Option<SnapVersion>,
    columnar_config: &ColumnarTableBuildOptions,
    id_allocator: &mut LocalIdAllocator,
) -> Result<Vec<ColumnarCreate>> {
    let fs = &ctx.dfs;
    let opts = dfs::Options::default()
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver)
        .with_type(FileType::Columnar);
    let (tx, mut rx) = mpsc::channel(ctx.req.file_ids.len());
    let mut file_builder = ColumnarFileBuilder::new(
        id_allocator.alloc_id().await,
        snap_version,
        ctx.encryption_key.clone(),
    );
    let mut cnt = 0;
    // Please ensure table_ids is sorted.
    for &table_id in &table_ids {
        let schema = schema_file.get_table(table_id).unwrap();
        let mut columnar_readers: Vec<Box<dyn ColumnarReader>> = vec![];
        for tbl in tbls {
            let iter = tbl.new_iterator(false, false);
            let reader = ColumnarRowTableReader::new(
                schema.clone(),
                iter,
                blob_tbls.clone(),
                true,
                ctx.encryption_key.clone(),
            );
            columnar_readers.push(Box::new(reader));
        }
        let merge_reader = ColumnarMergeReader::new(schema.clone(), columnar_readers);
        let mut compact_reader =
            ColumnarCompactReader::new(Box::new(merge_reader), target_lvl, schema, ctx.req.safe_ts);
        compact_reader.set_unbounded_handle_range().await?;
        compact_table_for_columnar(
            ctx,
            &mut compact_reader,
            &mut file_builder,
            schema,
            target_lvl,
            &mut cnt,
            tx.clone(),
            columnar_config,
            id_allocator,
        )
        .await?;
    }
    if file_builder.num_tables() > 0 {
        cnt += 1;
        persist_columnar_file(target_lvl, &mut file_builder, tx, fs.clone(), opts);
    }
    let mut errors = vec![];
    let mut columnar_creates = Vec::with_capacity(cnt);
    for _ in 0..cnt {
        match rx.recv().await.unwrap() {
            Err(err) => errors.push(err),
            Ok(tbl_create) => {
                columnar_creates.push(tbl_create);
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }

    Ok(columnar_creates)
}

// NOTE: For tables that need to add columnar, the table should not exist in the
// shard columnar files, so the old_columnar_tables should be empty.
async fn columnar_major_compact_for_add_tables(
    ctx: &CompactionCtx,
    major_compaction: &ColumnarMajorCompaction,
    id_allocator: &mut LocalIdAllocator,
    schema_file: SchemaFile,
) -> Result<pb::TableChange> {
    let mut table_changes = pb::TableChange::new();
    let fs = &ctx.dfs;
    let opts = dfs::Options::default()
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver)
        .with_type(FileType::Blob);
    let blob_tbls = load_blob_tables(fs.clone(), &major_compaction.blob_tables, opts).await?;
    let opts = opts.with_type(FileType::Sst);
    let l0_files = load_table_files(
        &major_compaction.l0_tables,
        fs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        ctx.for_restore,
    )
    .await?;
    let mut l0_tbls = files_to_l0_tables(l0_files, ctx.encryption_key.clone());
    l0_tbls.sort_by(|a, b| b.snap_version().cmp(&a.snap_version()));
    let mut tbls = vec![];
    for l0_tbl in &l0_tbls {
        if let Some(tbl) = l0_tbl.get_cf(WRITE_CF) {
            tbls.push(tbl.clone());
        }
    }
    for (_, table_ids) in major_compaction.ln_tables.iter() {
        let files = load_table_files(
            table_ids,
            fs.clone(),
            opts,
            ctx.local_dirs.as_ref(),
            ctx.for_restore,
        )
        .await?;
        let mut tables = files_to_tables(files, ctx.encryption_key.clone());
        tables.sort_by(|a, b| a.smallest().cmp(&b.smallest()));
        tables.iter().for_each(|tbl| {
            tbls.push(tbl.clone());
        });
    }
    if tbls.is_empty() {
        return Ok(table_changes);
    }
    let snap_version = if major_compaction.target_level == 2 {
        None
    } else {
        // Level 1 columnar file need sort by snap_version. Use the shard's
        // l2_snap_version as the new columnar file snap_version.
        Some(major_compaction.snap_version)
    };
    // TODO: remove the sort here after tikv-server upgraded.
    let mut table_ids = major_compaction.table_ids.0.clone();
    table_ids.sort();
    let columnar_creates = transform_for_columnar(
        ctx,
        &tbls,
        Some(Arc::new(blob_tbls)),
        table_ids,
        &schema_file,
        major_compaction.target_level,
        snap_version,
        &major_compaction.columnar_config,
        id_allocator,
    )
    .await?;
    for columnar_create in columnar_creates {
        table_changes.mut_columnar_creates().push(columnar_create);
    }
    Ok(table_changes)
}

async fn columnar_major_compact_for_clear_tables(
    ctx: &CompactionCtx,
    major_compaction: &ColumnarMajorCompaction,
    id_allocator: &mut LocalIdAllocator,
    schema_file: SchemaFile,
) -> Result<pb::TableChange> {
    let mut table_changes = pb::TableChange::new();
    let fs = &ctx.dfs;
    let opts = dfs::Options::default()
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver)
        .with_type(FileType::Columnar);
    let file_ids = major_compaction
        .old_columnar_tables
        .iter()
        .map(|(_, id)| *id)
        .collect::<Vec<_>>();
    let columnar_files = load_table_files(
        &file_ids,
        fs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        ctx.for_restore,
    )
    .await?;
    let mut columnar_files = columnar_files
        .into_iter()
        .map(|f| (f.id(), f))
        .collect::<HashMap<_, _>>();
    let mut creates = vec![];
    let mut deletes = vec![];
    let (tx, mut rx) = mpsc::channel(major_compaction.old_columnar_tables.len());
    let schema_file = &schema_file;
    let mut cnt = 0;
    // Inplace compact the columnar tables.
    for &(level, file_id) in major_compaction.old_columnar_tables.iter() {
        let mut tbl_delete = pb::ColumnarDelete::new();
        tbl_delete.set_level(level as u32);
        tbl_delete.set_id(file_id);
        deletes.push(tbl_delete);

        let file = columnar_files.remove(&file_id).unwrap();
        let columnar_table = ColumnarFile::open(file, None, ctx.columnar_meta_cache.clone())?;
        let mut overlap_tables = schema_file
            .overlap_columnar_tables(columnar_table.get_smallest(), columnar_table.get_biggest());
        if overlap_tables.is_empty() {
            continue;
        }
        overlap_tables.sort();
        let mut file_builder = ColumnarFileBuilder::new(
            id_allocator.alloc_id().await,
            columnar_table.get_snap_version(),
            ctx.encryption_key.clone(),
        );
        // The table_ids_to_clear not exist in overlap_tables, so after the compaction,
        // the table_ids_to_clear will be deleted.
        for table_id in overlap_tables {
            let schema = schema_file.get_table(table_id).unwrap();
            if !columnar_table.has_table(table_id) {
                continue;
            }
            let reader = ColumnarTableReader::new(
                &columnar_table,
                schema.clone(),
                None,
                ctx.encryption_key.clone(),
            );
            let mut compact_reader =
                ColumnarCompactReader::new(Box::new(reader), level as u32, schema, ctx.req.safe_ts);
            compact_reader.set_unbounded_handle_range().await?;
            compact_table_for_columnar(
                ctx,
                &mut compact_reader,
                &mut file_builder,
                schema,
                level as u32,
                &mut cnt,
                tx.clone(),
                &major_compaction.columnar_config,
                id_allocator,
            )
            .await?;
        }
        if file_builder.num_tables() > 0 {
            cnt += 1;
            persist_columnar_file(
                level as u32,
                &mut file_builder,
                tx.clone(),
                ctx.dfs.clone(),
                opts,
            );
        }
    }
    let mut errors = vec![];
    for _ in 0..cnt {
        match rx.recv().await.unwrap() {
            Err(err) => errors.push(err),
            Ok(create) => {
                creates.push(create);
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }
    table_changes.set_columnar_creates(creates.into());
    table_changes.set_columnar_deletes(deletes.into());

    Ok(table_changes)
}

async fn columnar_major_compact(
    ctx: &CompactionCtx,
    major_compaction: &ColumnarMajorCompaction,
    id_allocator: &mut LocalIdAllocator,
) -> Result<pb::ColumnarCompaction> {
    let mut ret = pb::ColumnarCompaction::new();
    ret.set_snap_version(major_compaction.snap_version.into_inner());
    ret.set_target_level(major_compaction.target_level);
    // Clear all columnar tables.
    if major_compaction.snap_version.is_zero() {
        let columnar_changes = ret.mut_columnar_change();
        for &(level, file_id) in major_compaction.old_columnar_tables.iter() {
            let mut tbl_delete = pb::ColumnarDelete::new();
            tbl_delete.set_level(level as u32);
            tbl_delete.set_id(file_id);
            columnar_changes.mut_columnar_deletes().push(tbl_delete);
        }
        return Ok(ret);
    }
    // Set row l0s for filter the new flushed l0 tables during apply major
    // compaction
    ret.set_row_l0s(major_compaction.l0_tables.clone());
    let req = &ctx.req;
    let tag = req.get_tag();
    let fs = &ctx.dfs;
    let schema_file_data = load_table_files(
        &[major_compaction.schema_file_id],
        fs.clone(),
        dfs::Options::default().with_type(FileType::Schema),
        ctx.local_dirs.as_ref(),
        false,
    )
    .await?
    .pop()
    .unwrap();
    let schema_file = SchemaFile::open(schema_file_data)?;
    let (table_ids_to_add, table_ids_to_clear) = &major_compaction.table_ids;
    ret.set_columnar_table_ids(table_ids_to_add.clone());
    ret.set_columnar_table_ids_to_clear(table_ids_to_clear.clone());
    // TiDB table ids that need to add or clear columnar. For table ids needed to
    // add columnar, we filter out the sst tables overlapped with the table range
    // and create new columnar tables. For table ids needed to clear columnar, we
    // delete the existing columnar tables through columnar inplace compaction.
    let mut add_table_changes = if !table_ids_to_add.is_empty() {
        columnar_major_compact_for_add_tables(
            ctx,
            major_compaction,
            id_allocator,
            schema_file.clone(),
        )
        .await?
    } else {
        pb::TableChange::new()
    };
    let mut clear_table_changes = if !table_ids_to_clear.is_empty()
        && !major_compaction.old_columnar_tables.is_empty()
    {
        columnar_major_compact_for_clear_tables(ctx, major_compaction, id_allocator, schema_file)
            .await?
    } else {
        pb::TableChange::new()
    };

    let columnar_changes = ret.mut_columnar_change();
    columnar_changes.set_columnar_creates(add_table_changes.take_columnar_creates());
    clear_table_changes
        .take_columnar_creates()
        .into_iter()
        .for_each(|c| columnar_changes.mut_columnar_creates().push(c));
    columnar_changes.set_columnar_deletes(clear_table_changes.take_columnar_deletes());
    info!(
        "{} columnar_major_compaction, tbl_changes: {:?}",
        tag, columnar_changes
    );
    Ok(ret)
}

async fn columnar_compact(
    ctx: &CompactionCtx,
    columnar_compaction: &ColumnarCompaction,
    id_allocator: &mut LocalIdAllocator,
) -> Result<pb::ColumnarCompaction> {
    if !columnar_compaction.source_row_files.is_empty() {
        convert_row_file_to_columnar_file(ctx, columnar_compaction, id_allocator).await
    } else {
        compact_columnar_files(ctx, columnar_compaction, id_allocator).await
    }
}

async fn convert_row_file_to_columnar_file(
    ctx: &CompactionCtx,
    columnar_compaction: &ColumnarCompaction,
    id_allocator: &mut LocalIdAllocator,
) -> Result<pb::ColumnarCompaction> {
    let mut ret = pb::ColumnarCompaction::new();
    ret.set_snap_version(columnar_compaction.snap_version.into_inner());
    ret.set_target_level(columnar_compaction.level);
    let row_l0s: Vec<u64> = columnar_compaction
        .source_row_files
        .iter()
        .filter(|(lvl, _)| *lvl == 0)
        .map(|(_, id)| *id)
        .collect();
    ret.set_row_l0s(row_l0s);

    let opts = dfs::Options::default()
        .with_type(FileType::Schema)
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver);
    let schema_file_id = columnar_compaction.schema_file_id;
    let schema_file_data = {
        let dfs = ctx.dfs.clone();
        ctx.dfs
            .get_runtime()
            .spawn(async move { dfs.read_file(schema_file_id, opts).await })
            .await
            .map_err(|e| -> Error { box_err!("read schema file: {:?}", e) })??
    };
    let schema_file = SchemaFile::open(Arc::new(InMemFile::new(schema_file_id, schema_file_data)))?;
    let l0_ids: Vec<u64> = columnar_compaction
        .source_row_files
        .iter()
        .map(|(_, id)| *id)
        .collect();
    let opts = dfs::Options::default().with_shard(ctx.req.shard_id, ctx.req.shard_ver);
    let l0_files = load_table_files(
        &l0_ids,
        ctx.dfs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        ctx.for_restore,
    )
    .await?;
    let l0_tbls = files_to_l0_tables(l0_files, ctx.encryption_key.clone());
    if columnar_compaction.columnar_table_ids.is_empty() {
        return Ok(ret);
    }
    let mut file_builder = ColumnarFileBuilder::new(
        id_allocator.alloc_id().await,
        Some(columnar_compaction.snap_version),
        ctx.encryption_key.clone(),
    );
    let mut cnt = 0;
    let (tx, mut rx) = mpsc::channel(ctx.req.file_ids.len());
    // TODO: remove the sort here after tikv-server upgraded.
    let mut columnar_table_ids = columnar_compaction.columnar_table_ids.to_vec();
    columnar_table_ids.sort();
    for table_id in columnar_table_ids {
        let schema = schema_file.get_table(table_id).unwrap();
        let mut columnar_readers: Vec<Box<dyn ColumnarReader>> = vec![];
        for l0_tbl in &l0_tbls {
            if let Some(tbl) = l0_tbl.get_cf(WRITE_CF) {
                let iter = tbl.new_iterator(false, false);
                let reader = ColumnarRowTableReader::new(
                    schema.clone(),
                    iter,
                    None,
                    true,
                    ctx.encryption_key.clone(),
                );
                columnar_readers.push(Box::new(reader));
            }
        }
        if columnar_readers.is_empty() {
            continue;
        }
        let merge_reader = ColumnarMergeReader::new(schema.clone(), columnar_readers);
        let mut compact_reader =
            ColumnarCompactReader::new(Box::new(merge_reader), 0, schema, ctx.req.safe_ts);
        compact_reader.set_unbounded_handle_range().await?;
        compact_table_for_columnar(
            ctx,
            &mut compact_reader,
            &mut file_builder,
            schema,
            0,
            &mut cnt,
            tx.clone(),
            &columnar_compaction.columnar_config,
            id_allocator,
        )
        .await?;
    }
    if file_builder.num_tables() > 0 {
        cnt += 1;
        persist_columnar_file(0, &mut file_builder, tx, ctx.dfs.clone(), opts);
    }
    let change = ret.mut_columnar_change();
    let mut errors = vec![];
    for _ in 0..cnt {
        match rx.recv().await.unwrap() {
            Err(err) => errors.push(err),
            Ok(col_create) => {
                change.mut_columnar_creates().push(col_create);
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }
    Ok(ret)
}

async fn compact_columnar_files(
    ctx: &CompactionCtx,
    columnar_compaction: &ColumnarCompaction,
    id_allocator: &mut LocalIdAllocator,
) -> Result<pb::ColumnarCompaction> {
    match columnar_compaction.level {
        0 => compact_columnar_l0_files(ctx, columnar_compaction, id_allocator).await,
        1 => compact_columnar_l1_files(ctx, columnar_compaction, id_allocator).await,
        2 => compact_all_columnar_files(ctx, columnar_compaction, id_allocator).await,
        _ => unreachable!(),
    }
}

async fn compact_columnar_l0_files(
    ctx: &CompactionCtx,
    columnar_compaction: &ColumnarCompaction,
    id_allocator: &mut LocalIdAllocator,
) -> Result<pb::ColumnarCompaction> {
    let mut ret = pb::ColumnarCompaction::default();
    ret.set_snap_version(columnar_compaction.snap_version.into_inner());
    ret.set_target_level(1);
    let tag = ctx.req.get_tag();
    let fs = &ctx.dfs;
    let schema_file_data = load_table_files(
        &[columnar_compaction.schema_file_id],
        fs.clone(),
        dfs::Options::default().with_type(FileType::Schema),
        ctx.local_dirs.as_ref(),
        false,
    )
    .await?
    .pop()
    .unwrap();
    let schema_file = SchemaFile::open(schema_file_data)?;
    let opts = dfs::Options::default()
        .with_type(FileType::Columnar)
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver);

    let col_file_ids: Vec<u64> = columnar_compaction
        .source_columnar_files
        .iter()
        .map(|f| {
            assert_eq!(f.0, 0);
            f.1
        })
        .collect();
    {
        let tbl_changes = ret.mut_columnar_change();
        for id in &col_file_ids {
            let mut col_delete = pb::ColumnarDelete::default();
            col_delete.set_id(*id);
            col_delete.set_level(0);
            tbl_changes.mut_columnar_deletes().push(col_delete);
        }
    }
    let col_files = load_table_files(
        &col_file_ids,
        fs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        false,
    )
    .await?;
    let col_tbls = files_to_columnar_tables(col_files, ctx.columnar_meta_cache.clone());
    let snap_version = col_tbls
        .iter()
        .map(|f| f.get_snap_version().unwrap())
        .max()
        .unwrap();

    let columnar_table_ids = columnar_compaction.columnar_table_ids.as_slice();
    if columnar_table_ids.is_empty() {
        return Ok(ret);
    }
    let mut file_builder = ColumnarFileBuilder::new(
        id_allocator.alloc_id().await,
        Some(snap_version),
        ctx.encryption_key.clone(),
    );
    let (tx, mut rx) = mpsc::channel(ctx.req.file_ids.len());
    let mut cnt = 0;
    // TODO: remove the sort here after tikv-server upgraded.
    let mut columnar_table_ids = columnar_table_ids.to_vec();
    columnar_table_ids.sort();
    for table_id in columnar_table_ids {
        let schema = schema_file.get_table(table_id).unwrap();
        let mut readers: Vec<Box<dyn ColumnarReader>> = vec![];
        for columnar_file in &col_tbls {
            if !columnar_file.has_table(table_id) {
                continue;
            }
            let reader = ColumnarTableReader::new(
                columnar_file,
                schema.clone(),
                None,
                ctx.encryption_key.clone(),
            );
            readers.push(Box::new(reader));
        }
        if readers.is_empty() {
            continue;
        }
        let merge_reader = ColumnarMergeReader::new(schema.clone(), readers);
        let mut compact_reader =
            ColumnarCompactReader::new(Box::new(merge_reader), 1, schema, ctx.req.safe_ts);
        compact_reader.set_unbounded_handle_range().await?;
        compact_table_for_columnar(
            ctx,
            &mut compact_reader,
            &mut file_builder,
            schema,
            1,
            &mut cnt,
            tx.clone(),
            &columnar_compaction.columnar_config,
            id_allocator,
        )
        .await?;
    }
    if file_builder.num_tables() > 0 {
        cnt += 1;
        persist_columnar_file(1, &mut file_builder, tx, ctx.dfs.clone(), opts);
    }
    let tbl_changes = ret.mut_columnar_change();
    let mut errors = vec![];
    for _ in 0..cnt {
        match rx.recv().await.unwrap() {
            Err(err) => errors.push(err),
            Ok(col_create) => {
                tbl_changes.mut_columnar_creates().push(col_create);
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }
    info!(
        "{} compact_columnar_l0_files done, tbl_changes: {:?}",
        tag, tbl_changes
    );
    Ok(ret)
}

async fn compact_columnar_l1_files(
    ctx: &CompactionCtx,
    columnar_compaction: &ColumnarCompaction,
    id_allocator: &mut LocalIdAllocator,
) -> Result<pb::ColumnarCompaction> {
    let mut ret = pb::ColumnarCompaction::default();
    ret.set_snap_version(columnar_compaction.snap_version.into_inner());
    ret.set_target_level(2);
    let tag = ctx.req.get_tag();
    let fs = &ctx.dfs;
    let schema_file_data = load_table_files(
        &[columnar_compaction.schema_file_id],
        fs.clone(),
        dfs::Options::default().with_type(FileType::Schema),
        ctx.local_dirs.as_ref(),
        false,
    )
    .await?
    .pop()
    .unwrap();
    let schema_file = SchemaFile::open(schema_file_data)?;
    let opts = dfs::Options::default()
        .with_type(FileType::Columnar)
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver);
    let tbl_changes = ret.mut_columnar_change();
    let (l1_col_file_ids, l2_col_file_ids): (Vec<u64>, Vec<u64>) = columnar_compaction
        .source_columnar_files
        .iter()
        .partition_map(|f| {
            if f.0 == 1 {
                Either::Left(f.1)
            } else if f.0 == 2 {
                Either::Right(f.1)
            } else {
                panic!("invalid file level {}", f.0);
            }
        });
    let l1_tbl_files = load_table_files(
        &l1_col_file_ids,
        fs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        false,
    )
    .await?;
    if l1_tbl_files.is_empty() {
        return Ok(ret);
    }
    let l1_tbls = files_to_columnar_tables(l1_tbl_files, ctx.columnar_meta_cache.clone());
    let mut smallest = l1_tbls.iter().map(|f| f.get_smallest()).min().unwrap();
    let mut biggest = l1_tbls.iter().map(|f| f.get_biggest()).max().unwrap();
    for tbl in &l1_tbls {
        let mut col_delete = pb::ColumnarDelete::default();
        col_delete.set_id(tbl.get_file().id());
        col_delete.set_level(1);
        tbl_changes.mut_columnar_deletes().push(col_delete);
    }

    let l2_tbl_files = load_table_files(
        &l2_col_file_ids,
        fs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        false,
    )
    .await?;
    let mut l2_tbls = files_to_columnar_tables(l2_tbl_files, ctx.columnar_meta_cache.clone());
    l2_tbls.sort_by(|a, b| a.get_smallest().cmp(&b.get_smallest()));
    for tbl in &l2_tbls {
        smallest = smallest.min(tbl.get_smallest());
        biggest = biggest.max(tbl.get_biggest());
        let mut col_delete = pb::ColumnarDelete::default();
        col_delete.set_id(tbl.get_file().id());
        col_delete.set_level(2);
        tbl_changes.mut_columnar_deletes().push(col_delete);
    }

    let mut overlap_tables = schema_file.overlap_columnar_tables(smallest, biggest);
    overlap_tables.sort();
    // TODO: use columnar_table_ids in compaction directly after tikv-worker
    // upgraded.
    let columnar_table_ids = if !columnar_compaction.columnar_table_ids.is_empty() {
        columnar_compaction.columnar_table_ids.as_slice()
    } else {
        overlap_tables.as_slice()
    };
    if columnar_table_ids.is_empty() {
        return Ok(ret);
    }

    let mut file_builder = ColumnarFileBuilder::new(
        id_allocator.alloc_id().await,
        None,
        ctx.encryption_key.clone(),
    );
    let (tx, mut rx) = mpsc::channel(ctx.req.file_ids.len());
    let mut cnt = 0;
    // TODO: remove the sort here after tikv-server upgraded.
    let mut columnar_table_ids = columnar_table_ids.to_vec();
    columnar_table_ids.sort();
    for table_id in columnar_table_ids {
        let schema = schema_file.get_table(table_id).unwrap();
        let mut readers: Vec<Box<dyn ColumnarReader>> = vec![];
        for columnar_file in &l1_tbls {
            if !columnar_file.has_table(table_id) {
                continue;
            }
            let reader = ColumnarTableReader::new(
                columnar_file,
                schema.clone(),
                None,
                ctx.encryption_key.clone(),
            );
            readers.push(Box::new(reader));
        }
        let concat_reader =
            ColumnarConcatReader::new(&l2_tbls, schema.clone(), None, ctx.encryption_key.clone());
        readers.push(Box::new(concat_reader));
        let merge_reader = ColumnarMergeReader::new(schema.clone(), readers);
        let mut compact_reader =
            ColumnarCompactReader::new(Box::new(merge_reader), 2, schema, ctx.req.safe_ts);
        compact_reader.set_unbounded_handle_range().await?;
        compact_table_for_columnar(
            ctx,
            &mut compact_reader,
            &mut file_builder,
            schema,
            2,
            &mut cnt,
            tx.clone(),
            &columnar_compaction.columnar_config,
            id_allocator,
        )
        .await?;
    }
    if file_builder.num_tables() > 0 {
        cnt += 1;
        persist_columnar_file(2, &mut file_builder, tx, ctx.dfs.clone(), opts);
    }
    let tbl_changes = ret.mut_columnar_change();
    let mut errors = vec![];
    for _ in 0..cnt {
        match rx.recv().await.unwrap() {
            Err(err) => errors.push(err),
            Ok(col_create) => {
                tbl_changes.mut_columnar_creates().push(col_create);
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }
    info!(
        "{} compact_columnar_l1_files done, tbl_changes: {:?}",
        tag, tbl_changes
    );

    Ok(ret)
}

async fn compact_all_columnar_files(
    ctx: &CompactionCtx,
    columnar_compaction: &ColumnarCompaction,
    id_allocator: &mut LocalIdAllocator,
) -> Result<pb::ColumnarCompaction> {
    let mut ret = pb::ColumnarCompaction::default();
    ret.set_snap_version(columnar_compaction.snap_version.into_inner());
    ret.set_target_level(2);
    ret.set_is_manual_major_compaction(true);
    let tag = ctx.req.get_tag();
    let fs = &ctx.dfs;
    let schema_file_data = load_table_files(
        &[columnar_compaction.schema_file_id],
        fs.clone(),
        dfs::Options::default().with_type(FileType::Schema),
        ctx.local_dirs.as_ref(),
        false,
    )
    .await?
    .pop()
    .unwrap();
    let schema_file = SchemaFile::open(schema_file_data)?;
    let opts = dfs::Options::default()
        .with_type(FileType::Columnar)
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver);
    let tbl_changes = ret.mut_columnar_change();
    let mut col_file_ids: HashMap<u32, Vec<u64>> = HashMap::new();
    for (level, file_id) in &columnar_compaction.source_columnar_files {
        col_file_ids.entry(*level).or_default().push(*file_id);
    }
    if columnar_compaction.columnar_table_ids.is_empty() {
        return Ok(ret);
    }

    let mut file_builder = ColumnarFileBuilder::new(
        id_allocator.alloc_id().await,
        None,
        ctx.encryption_key.clone(),
    );
    let (tx, mut rx) = mpsc::channel(ctx.req.file_ids.len());
    let mut cnt = 0;
    let mut all_col_tbls = HashMap::new();
    for (&level, tbl_ids) in &col_file_ids {
        let tbl_files =
            load_table_files(tbl_ids, fs.clone(), opts, ctx.local_dirs.as_ref(), false).await?;
        let mut tbls = files_to_columnar_tables(tbl_files, ctx.columnar_meta_cache.clone());
        if level == 2 {
            tbls.sort_by(|a, b| a.get_smallest().cmp(&b.get_smallest()));
        }
        for tbl in &tbls {
            let mut col_delete = pb::ColumnarDelete::default();
            col_delete.set_id(tbl.get_file().id());
            col_delete.set_level(level);
            tbl_changes.mut_columnar_deletes().push(col_delete);
        }
        all_col_tbls.insert(level, tbls);
    }
    // TODO: remove the sort here after tikv-server upgraded.
    let mut columnar_table_ids = columnar_compaction.columnar_table_ids.to_vec();
    columnar_table_ids.sort();
    for &table_id in &columnar_table_ids {
        let Some(schema) = schema_file.get_table(table_id) else {
            continue;
        };
        let mut readers: Vec<Box<dyn ColumnarReader>> = vec![];
        for (&level, tbls) in &all_col_tbls {
            if level < 2 {
                for tbl in tbls {
                    if tbl.has_table(table_id) {
                        let reader = ColumnarTableReader::new(
                            tbl,
                            schema.clone(),
                            None,
                            ctx.encryption_key.clone(),
                        );
                        readers.push(Box::new(reader));
                    }
                }
            } else {
                let reader = ColumnarConcatReader::new(
                    tbls,
                    schema.clone(),
                    None,
                    ctx.encryption_key.clone(),
                );
                readers.push(Box::new(reader));
            }
        }
        let merge_reader = ColumnarMergeReader::new(schema.clone(), readers);
        let mut compact_reader =
            ColumnarCompactReader::new(Box::new(merge_reader), 2, schema, ctx.req.safe_ts);
        compact_reader.set_unbounded_handle_range().await?;
        compact_table_for_columnar(
            ctx,
            &mut compact_reader,
            &mut file_builder,
            schema,
            2,
            &mut cnt,
            tx.clone(),
            &columnar_compaction.columnar_config,
            id_allocator,
        )
        .await?;
    }
    if file_builder.num_tables() > 0 {
        cnt += 1;
        persist_columnar_file(2, &mut file_builder, tx, ctx.dfs.clone(), opts);
    }

    let tbl_changes = ret.mut_columnar_change();
    let mut errors = vec![];
    for _ in 0..cnt {
        match rx.recv().await.unwrap() {
            Err(err) => errors.push(err),
            Ok(col_create) => {
                tbl_changes.mut_columnar_creates().push(col_create);
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }
    info!(
        "{} compact_all_columnar_files done, tbl_changes: {:?}",
        tag, tbl_changes
    );

    Ok(ret)
}

async fn update_vector_index(
    ctx: &CompactionCtx,
    update_vec_idx: &VectorIndexUpdate,
    id_allocator: &mut LocalIdAllocator,
) -> Result<pb::UpdateVectorIndex> {
    let fs = &ctx.dfs;
    let schema_file_data = load_table_files(
        &[update_vec_idx.schema_file_id],
        fs.clone(),
        dfs::Options::default().with_type(FileType::Schema),
        ctx.local_dirs.as_ref(),
        false,
    )
    .await?
    .pop()
    .unwrap();
    let schema_file = SchemaFile::open(schema_file_data)?;
    let full_schema = schema_file.get_table(update_vec_idx.table_id).unwrap();
    let mut ret = update_vec_idx.to_pb_without_added();
    let vec_idx_def = full_schema
        .vector_indexes
        .iter()
        .find(|idx| idx.index_id == update_vec_idx.index_id)
        .unwrap();
    let schema_buf = full_schema
        .to_schema_buf()
        .retain_columns(|c| c.get_column_id() == vec_idx_def.col_id);
    let vector_col_schema = Schema::new(schema_buf);
    let dimension = vector_col_schema.columns[0].get_column_len() as usize;
    let metric = vec_idx_def
        .specs
        .get(VECTOR_INDEX_SPEC_KEY_DISTANCE_METRIC)
        .unwrap()
        .to_str_lossy();
    let opts = dfs::Options::default()
        .with_type(FileType::Columnar)
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver);
    let col_file_ids: Vec<u64> = update_vec_idx
        .col_file_ids
        .iter()
        .map(|&(id, _)| id)
        .collect();
    let files = load_table_files(
        &col_file_ids,
        fs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        false,
    )
    .await?;
    let mut columnar_files = vec![];
    for file in files {
        let columnar_file = ColumnarFile::open(file, None, ctx.columnar_meta_cache.clone())?;
        columnar_files.push(columnar_file);
    }
    let mut readers: Vec<Box<dyn ColumnarReader>> = vec![];
    for columnar_file in &columnar_files {
        // NOTE: columnar_file must have the table, already checked in
        // trigger_update_vector_index.
        let reader = ColumnarTableReader::new(
            columnar_file,
            vector_col_schema.clone(),
            None,
            ctx.encryption_key.clone(),
        );
        readers.push(Box::new(reader));
    }
    if readers.is_empty() {
        info!("update vector index result {:?}", ret);
        return Ok(ret);
    }

    let build_opts = update_vec_idx.vector_index_build_opts;
    let is_common_handle = vector_col_schema.is_common_handle();
    let mut vec_builder = VectorIndexBuilder::new(
        dimension,
        metric.as_ref(),
        update_vec_idx.snap_version,
        update_vec_idx.table_id,
        update_vec_idx.index_id,
        update_vec_idx.col_id,
        is_common_handle,
    )?;
    let mut block = Block::new(&vector_col_schema);
    let mut merge_reader = ColumnarMergeReader::new(vector_col_schema, readers);
    merge_reader.seek(&[]).await?;
    let mut vector_index_files = vec![];
    let mut res = merge_reader.read(&mut block, 1024).await?;
    let mut delta_vector_size = 0;
    while res > 0 {
        vec_builder.add_block(&block, 0)?;
        delta_vector_size += block.length() * dimension * std::mem::size_of::<f32>();
        block.reset();
        if should_build_vector_index(&vec_builder, &build_opts, delta_vector_size) {
            let vec_idx_file =
                build_vector_index_file(ctx, &mut vec_builder, id_allocator, update_vec_idx, &opts)
                    .await?;
            vector_index_files.push(vec_idx_file);
            delta_vector_size = 0;
            vec_builder = VectorIndexBuilder::new(
                dimension,
                metric.as_ref(),
                update_vec_idx.snap_version,
                update_vec_idx.table_id,
                update_vec_idx.index_id,
                update_vec_idx.col_id,
                is_common_handle,
            )?;
        }
        res = merge_reader.read(&mut block, 1024).await?;
    }
    if vec_builder.entry_count() > 0 {
        let vec_idx_file =
            build_vector_index_file(ctx, &mut vec_builder, id_allocator, update_vec_idx, &opts)
                .await?;
        vector_index_files.push(vec_idx_file);
    }
    ret.set_added(vector_index_files.into());
    info!("update vector index result {:?}", ret);
    Ok(ret)
}

/// The Compaction Workload: Create FTS L0 from columnar files (typically L0).
async fn fts_create_l0_update(
    ctx: &CompactionCtx,
    req: &FtsCreateL0Update,
    id_allocator: &mut LocalIdAllocator,
) -> Result<pb::FtsUpdate> {
    let fs = &ctx.dfs;

    // Load schema file
    let schema_file_data = load_table_files(
        &[req.schema_file_id],
        fs.clone(),
        dfs::Options::default().with_type(FileType::Schema),
        ctx.local_dirs.as_ref(),
        false,
    )
    .await?
    .pop()
    .unwrap();
    let schema_file = SchemaFile::open(schema_file_data)?;

    // Load columnar files
    let opts = dfs::Options::default()
        .with_type(FileType::Columnar)
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver);
    let files = load_table_files(
        &req.col_l0_file_ids,
        fs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        false,
    )
    .await?;
    let mut columnar_files = Vec::with_capacity(files.len());
    for file in files {
        let columnar_file = ColumnarFile::open(file, None, ctx.columnar_meta_cache.clone())?;
        columnar_files.push(columnar_file);
    }

    // Convert columnar files to FTS L0
    // Pass the specific (table_id, index_id) pairs to ensure only tracked indexes
    // are processed for incremental updates.
    let conversion_opts = ColumnarToFtsL0Opts {
        columnar_files: &columnar_files,
        schema_file,
        index_pairs: &req.fts_indexes,
        encryption_key: ctx.encryption_key.clone(),
        snap_version: req.snap_version,
        l0_file_max_size: req.build_options.l0_file_max_size.0,
    };

    let convert_outputs = columnar_to_fts_l0(conversion_opts)
        .await
        .map_err(|e| -> Error { box_err!("Failed to convert columnar L0 to FTS: {:?}", e) })?;

    // Create result protobuf message
    let mut ret = pb::FtsUpdate::default();
    ret.mut_pending_columnar_l0_remove_files()
        .extend_from_slice(&req.col_l0_file_ids);
    ret.set_set_snap_version(req.snap_version.into_inner());

    if convert_outputs.is_empty() {
        info!("FTS L0 update result: no data generated");
        return Ok(ret);
    }

    let dfs_opts = dfs::Options::default()
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver)
        .with_type(dfs::FileType::FtsPackedFile);

    for f in convert_outputs {
        // Allocate file ID for the new FTS L0 file
        let file_id = id_allocator.alloc_id().await;

        // Create FTS L0 file in DFS
        fs.create(file_id, f.data, dfs_opts).await?;

        // Create FtsPackedFileInfo for the new FTS file
        let info = PackedFile::info(file_id, f.summary.meta_offset, f.summary.props);
        ret.mut_l0_add_files().push(info);
        info!("FTS L0 update result: created file {}", file_id);
    }

    Ok(ret)
}

/// The Compaction Workload: Create FTS L0 for new indexes from historical
/// columnar files.
async fn fts_add_index_update(
    ctx: &CompactionCtx,
    req: &FtsAddIndexUpdate,
    id_allocator: &mut LocalIdAllocator,
) -> Result<pb::FtsUpdate> {
    let fs = &ctx.dfs;

    // Load schema file
    let schema_file_data = load_table_files(
        &[req.schema_file_id],
        fs.clone(),
        dfs::Options::default().with_type(FileType::Schema),
        ctx.local_dirs.as_ref(),
        false,
    )
    .await?
    .pop()
    .unwrap();
    let schema_file = SchemaFile::open(schema_file_data)?;

    // Load columnar files
    let opts = dfs::Options::default()
        .with_type(FileType::Columnar)
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver);

    let col_file_ids: Vec<u64> = req.col_file_ids.iter().map(|(id, _)| *id).collect();
    let files = load_table_files(
        &col_file_ids,
        fs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        false,
    )
    .await?;

    let mut columnar_files = Vec::with_capacity(files.len());
    for file in files {
        let columnar_file = ColumnarFile::open(file, None, ctx.columnar_meta_cache.clone())?;
        columnar_files.push(columnar_file);
    }

    // Convert historical columnar files to FTS L0 for new indexes
    // Process each new index separately to ensure proper partitioning
    let conversion_opts = ColumnarToFtsL0Opts {
        columnar_files: &columnar_files,
        schema_file,
        index_pairs: &req.fts_indexes_to_add,
        encryption_key: ctx.encryption_key.clone(),
        snap_version: req.current_snap_version,
        l0_file_max_size: req.build_options.l0_file_max_size.0,
    };

    let convert_outputs = columnar_to_fts_l0(conversion_opts)
        .await
        .map_err(|e| -> Error {
            box_err!("Failed to convert historical columnar to FTS: {:?}", e)
        })?;

    // Create result protobuf message
    // FtsAddIndex sets add_tracked_indexes with the new indexes being added
    let mut ret = pb::FtsUpdate::default();
    // NOTE: Do NOT touch `pending_columnar_l0_ids` here.
    //
    // `pending_columnar_l0_ids` is a shard-global marker for columnar L0 files that
    // are <= the FTS watermark but are *not* guaranteed to be indexed for all
    // currently tracked indexes (typically after shard merge when the watermark
    // advances).
    //
    // FtsAddIndex only builds indexes for `req.fts_indexes_to_add` (new indexes).
    // It does not guarantee those columnar L0s are indexed for existing tracked
    // indexes, so emitting `pending_columnar_l0_remove_files` here can
    // incorrectly clear pending work and permanently hide unindexed data from
    // reads.
    for info in &req.fts_indexes_to_add {
        let mut index_id = TableIndexId::new();
        index_id.set_table_id(info.0);
        index_id.set_index_id(info.1);
        ret.mut_add_tracked_indexes().push(index_id);
    }

    if convert_outputs.is_empty() {
        info!("FTS add index result: no data generated");
        return Ok(ret);
    }

    let dfs_opts = dfs::Options::default()
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver)
        .with_type(dfs::FileType::FtsPackedFile);

    for f in convert_outputs {
        // Allocate file ID for the new FTS L0 file
        let file_id = id_allocator.alloc_id().await;

        // Create FTS L0 file in DFS
        fs.create(file_id, f.data, dfs_opts).await?;

        // Create FtsPackedFileInfo for the new FTS file
        let info = PackedFile::info(file_id, f.summary.meta_offset, f.summary.props);
        ret.mut_l0_add_files().push(info);

        info!(
            "FTS add index result: created file {} for {} new indexes",
            file_id,
            req.fts_indexes_to_add.len()
        );
    }
    Ok(ret)
}

async fn fts_l0_compact(
    ctx: &CompactionCtx,
    req: &FtsCompactL0Update,
    id_allocator: &mut LocalIdAllocator,
) -> Result<pb::FtsUpdate> {
    let fs = &ctx.dfs;

    let mut ret = pb::FtsUpdate::default();

    if req.l0_file_ids.is_empty() && req.l1_file_ids.is_empty() {
        return Ok(ret);
    }

    ret.mut_l0_remove_files()
        .extend_from_slice(&req.l0_file_ids);
    ret.mut_l1_remove_files()
        .extend_from_slice(&req.l1_file_ids);

    let mut source_ids = Vec::with_capacity(req.l0_file_ids.len() + req.l1_file_ids.len());
    source_ids.extend(req.l0_file_ids.iter().copied());
    source_ids.extend(req.l1_file_ids.iter().copied());

    let opts = dfs::Options::default()
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver)
        .with_type(FileType::FtsPackedFile);

    let files = load_table_files(
        &source_ids,
        fs.clone(),
        opts,
        ctx.local_dirs.as_ref(),
        false,
    )
    .await?;

    let mut packed_files = Vec::with_capacity(files.len());
    for file in files {
        let packed = PackedFile::new(file, FtsCache::disabled())
            .map_err(|e| -> Error { box_err!("Failed to open FTS packed file: {}", e) })?;
        packed_files.push(packed);
    }

    let existing_l2_lp_keys: HashSet<Vec<u8>> = req.l2_lp_keys.iter().cloned().collect();
    let tracked_indexes: HashSet<(i64, i64)> = req.tracked_indexes.iter().copied().collect();
    let build_opts = &req.build_options;
    let merge_opt = PackedFileMergeOpt {
        max_pack_file_size: build_opts.l1_file_max_size.0,
        min_l2_lp_size: build_opts.min_l2_lp_size.0,
        pack_opt: PackedFileBuilderOptions::default(),
        ded_opt: DedicatedFileBuilderOptions::default(),
    };
    let merge_outputs = merge_fts_l0_l1(
        packed_files.as_slice(),
        merge_opt,
        ctx.req.safe_ts,
        &existing_l2_lp_keys,
        &tracked_indexes,
    )
    .await
    .map_err(|e| -> Error { box_err!("Failed to merge FTS packed files: {}", e) })?;

    if merge_outputs.l1_files.is_empty() && merge_outputs.l2_files.is_empty() {
        return Ok(ret);
    }

    let dfs_opts = dfs::Options::default()
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver)
        .with_type(FileType::FtsPackedFile);
    let dfs_opts_ded = dfs::Options::default()
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver)
        .with_type(FileType::FtsDedicatedFile);

    for f in merge_outputs.l1_files.into_iter() {
        let file_id = id_allocator.alloc_id().await;

        ctx.dfs.create(file_id, f.data.clone(), dfs_opts).await?;

        let info = PackedFile::info(file_id, f.summary.meta_offset, f.summary.props);
        ret.mut_l1_add_files().push(info);
    }

    for f in merge_outputs.l2_files.into_iter() {
        let file_id = id_allocator.alloc_id().await;
        ctx.dfs
            .create(file_id, f.data.clone(), dfs_opts_ded)
            .await?;

        let info = DedicatedFile::info(file_id, f.summary.meta_offset, f.summary.props);
        ret.mut_l2_add_files().push(info);
    }

    Ok(ret)
}

async fn fts_l2_compact(
    ctx: &CompactionCtx,
    req: &FtsCompactL2Update,
    id_allocator: &mut LocalIdAllocator,
) -> Result<pb::FtsUpdate> {
    let mut ret = pb::FtsUpdate::default();
    if req.l2_file_ids.is_empty() {
        return Ok(ret);
    }

    ret.mut_l2_remove_files()
        .extend_from_slice(&req.l2_file_ids);

    let dfs_opts = dfs::Options::default()
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver)
        .with_type(FileType::FtsDedicatedFile);
    let files = load_table_files(
        &req.l2_file_ids,
        ctx.dfs.clone(),
        dfs_opts,
        ctx.local_dirs.as_ref(),
        ctx.for_restore,
    )
    .await?;

    let mut dedicated_files = Vec::with_capacity(files.len());
    for file in files {
        let dedicated = EDedicatedFile::new(file, FtsCache::disabled())
            .map_err(|e| -> Error { box_err!("Failed to open dedicated file: {}", e) })?;
        dedicated_files.push(dedicated);
    }

    if dedicated_files.len() < 2 {
        return Ok(ret);
    }

    let merged = merge_fts_l2_files(
        dedicated_files.as_slice(),
        ctx.req.safe_ts,
        DedicatedFileBuilderOptions::default(),
    )
    .await
    .map_err(|e| -> Error { box_err!("Failed to merge dedicated files: {}", e) })?;

    let Some(output) = merged else {
        return Ok(ret);
    };

    let dfs_opts = dfs::Options::default()
        .with_shard(ctx.req.shard_id, ctx.req.shard_ver)
        .with_type(FileType::FtsDedicatedFile);
    let file_id = id_allocator.alloc_id().await;
    ctx.dfs
        .create(file_id, output.data.clone(), dfs_opts)
        .await?;

    let summary = output.summary;
    if !req.lp_key.is_empty() && summary.props.get_lp_key() != req.lp_key.as_slice() {
        return Err(box_err!(
            "Merged L2 file lp_key mismatch, expect {}, got {}",
            hexhex::hex(&req.lp_key),
            hexhex::hex(summary.props.get_lp_key())
        ));
    }

    let info = DedicatedFile::info(file_id, summary.meta_offset, summary.props);
    ret.mut_l2_add_files().push(info);

    Ok(ret)
}

async fn fts_cleanup(_ctx: &CompactionCtx, req: &FtsCleanupUpdate) -> Result<pb::FtsUpdate> {
    let mut ret = pb::FtsUpdate::default();
    if !req.l1_file_ids.is_empty() {
        ret.mut_l1_remove_files()
            .extend_from_slice(&req.l1_file_ids);
    }
    if !req.l2_file_ids.is_empty() {
        ret.mut_l2_remove_files()
            .extend_from_slice(&req.l2_file_ids);
    }
    Ok(ret)
}

// NOTE: Call `estimated_size` to avoid expensive calculation of the vector
// index size.
fn should_build_vector_index(
    builder: &VectorIndexBuilder,
    build_opts: &VectorIndexBuildOptions,
    delta_vector_size: usize,
) -> bool {
    // Split vector index feature is disabled.
    if build_opts.max_file_size == 0 {
        return false;
    }
    if delta_vector_size < build_opts.max_file_size / 100 * MAX_DELTA_VECTOR_SIZE_PERCENT {
        return false;
    }

    builder.estimated_size() >= build_opts.max_file_size
}

async fn build_vector_index_file(
    ctx: &CompactionCtx,
    vec_builder: &mut VectorIndexBuilder,
    id_allocator: &mut LocalIdAllocator,
    update_vec_idx: &VectorIndexUpdate,
    opts: &dfs::Options,
) -> Result<VectorIndexFile> {
    let buf = vec_builder.build()?;
    let file_id = id_allocator.alloc_id().await;
    let fs = &ctx.dfs;
    let _enter = fs.get_runtime().enter();
    fs.create(file_id, buf.into(), opts.with_type(FileType::VectorIndex))
        .await?;
    let vec_idx_file = new_vector_index_file_pb(
        file_id,
        update_vec_idx.snap_version,
        vec_builder.smallest.clone(),
        vec_builder.biggest.clone(),
        vec_builder.meta_offset,
    );
    Ok(vec_idx_file)
}

pub(crate) enum CompactMsg {
    /// The shard is ready to be compacted / destroyed range / truncated ts.
    Compact(IdVer),

    /// Compaction finished.
    Finish {
        task_id: u64,
        id_ver: IdVer,
        // This variant is too large(256 bytes). Box it to reduce size.
        result: Box<Option<Result<pb::ChangeSet>>>,
    },

    /// The compaction change set is applied to the engine, so the shard is
    /// ready for next compaction.
    Applied(IdVer),

    /// Clear message is sent when a shard changed its version or set to
    /// inactive. Then all the previous tasks will be discarded.
    /// This simplifies the logic, avoid race condition.
    Clear(IdVer),

    /// Pause message is used to pause compaction when a shard becomes leader
    /// but there are scheduled compactions.
    ///
    /// In this scene, we need to prevent new compaction tasks. Otherwise,
    /// compaction based on stale shard meta may be conflict with scheduled
    /// compactions.
    ///
    /// `seq` is the maximum log index sequence of scheduled compactions.
    Pause { id_ver: IdVer, seq: u64 },

    /// Resume compaction for previously blocked keyspace shards.
    UnblockKeyspace,

    /// Stop background compact thread.
    Stop,
}

pub(crate) struct CompactRunner {
    engine: Engine,
    rx: tikv_util::mpsc::Receiver<CompactMsg>,
    /// task_id is the identity of each compaction job.
    ///
    /// When a shard is set to inactive, all the compaction jobs of this shard
    /// should be discarded, then when it is set to active again, old result
    /// may arrive and conflict with the new tasks. So we use task_id to
    /// detect and discard obsolete tasks.
    task_id: u64,
    /// `running` contains shards' running compaction `task_id`s.
    running: HashMap<IdVer, u64 /* task_id */>,
    /// `notified` contains shards that have finished a compaction job and been
    /// notified to apply it.
    notified: HashSet<IdVer>,
    /// `pending` contains shards that want to do compaction but the job queue
    /// is full now.
    pending: HashSet<IdVer>,
    /// `paused_seq` is used to indicate that compaction should be paused until
    /// `Shard::meta_seq >= paused_seq`.
    paused_seq: HashMap<IdVer, u64>,

    /// blocked contains the keyspace shards that are blocked for compaction.
    blocked: HashMap<u32, HashSet<IdVer>>,

    thread_pool: tokio::runtime::Runtime,
}

impl CompactRunner {
    pub(crate) fn new(engine: Engine, rx: tikv_util::mpsc::Receiver<CompactMsg>) -> Self {
        let thread_pool = tokio::runtime::Builder::new_multi_thread()
            .thread_name("compact-runner")
            .worker_threads(engine.opts.num_compactors)
            .enable_all()
            .after_start_wrapper(|| {
                file_system::set_io_type(IoType::Compaction);
            })
            .before_stop_wrapper(|| {})
            .build()
            .unwrap();
        Self {
            engine,
            rx,
            task_id: 0,
            running: Default::default(),
            notified: Default::default(),
            pending: Default::default(),
            paused_seq: Default::default(),
            blocked: Default::default(),
            thread_pool,
        }
    }

    fn run(&mut self) {
        while let Ok(msg) = self.rx.recv() {
            match msg {
                CompactMsg::Compact(id_ver) => self.compact(id_ver),

                CompactMsg::Finish {
                    task_id,
                    id_ver,
                    result,
                } => self.compaction_finished(id_ver, task_id, *result),

                CompactMsg::Applied(id_ver) => self.compaction_applied(id_ver),

                CompactMsg::Clear(id_ver) => self.clear(id_ver),

                CompactMsg::Pause { id_ver, seq } => {
                    let old_seq = self.paused_seq.insert(id_ver, seq);

                    let tag = ShardTag::new(self.engine.get_engine_id(), id_ver);
                    info!("{} compaction paused", tag; "seq" => seq, "old_seq" => ?old_seq);
                    debug_assert!(
                        old_seq.is_none_or(|old| old <= seq),
                        "{}: compaction paused old_seq {:?} > seq {}",
                        tag,
                        old_seq,
                        seq
                    );
                }

                CompactMsg::UnblockKeyspace => self.schedule_unblocked_compaction(),

                CompactMsg::Stop => {
                    info!(
                        "Engine {} compaction worker receive stop msg and stop now",
                        self.engine.get_engine_id()
                    );
                    break;
                }
            }
        }
    }

    fn compact(&mut self, id_ver: IdVer) {
        // The shard is compacting, and following compaction will be trigger after
        // apply.
        if self.is_compacting(id_ver) {
            return;
        }
        if self.is_full() {
            self.pending.insert(id_ver);
            return;
        }

        let shard = match self.engine.get_shard_with_ver(id_ver.id, id_ver.ver) {
            Ok(shard) => shard,
            Err(_) => {
                self.pending.remove(&id_ver);
                return;
            }
        };
        if is_compaction_blocked(shard.keyspace_id) {
            self.blocked
                .entry(shard.keyspace_id)
                .or_default()
                .insert(id_ver);
            let tag = ShardTag::new(self.engine.get_engine_id(), id_ver);
            info!("{} compaction blocked", tag);
            return;
        }
        if self.is_paused(id_ver, shard.get_meta_sequence()) {
            return;
        }

        self.task_id += 1;
        let task_id = self.task_id;
        self.running.insert(id_ver, task_id);
        self.pending.remove(&id_ver); // Remove from pending if it's there.
        let engine = self.engine.clone();
        self.thread_pool
            .spawn(tikv_util::init_task_local(async move {
                tikv_util::set_current_region(id_ver.id);
                let result = Box::new(engine.compact(shard).await);
                engine.send_compact_msg(CompactMsg::Finish {
                    task_id,
                    id_ver,
                    result,
                });
            }));
    }

    fn compaction_finished(
        &mut self,
        id_ver: IdVer,
        task_id: u64,
        result: Option<Result<pb::ChangeSet>>,
    ) {
        let tag = ShardTag::new(self.engine.get_engine_id(), id_ver);
        if self
            .running
            .get(&id_ver)
            .map(|id| *id != task_id)
            .unwrap_or(true)
        {
            info!(
                "shard {} discard old term compaction result {:?}",
                tag, result
            );
            return;
        }
        self.running.remove(&id_ver);

        let need_reset_compacting = match result {
            Some(Ok(resp)) => {
                ENGINE_COMPACTION_RESULT_COUNTER
                    .with_label_values(&["success"])
                    .inc();
                self.engine.handle_compact_response(resp);
                self.notified.insert(id_ver);
                self.schedule_pending_compaction();
                false
            }
            Some(Err(FallbackLocalCompactorDisabled)) => {
                ENGINE_COMPACTION_RESULT_COUNTER
                    .with_label_values(&["local_compaction_disabled"])
                    .inc();
                error!("shard {} local compaction disabled, no need retry", tag);
                true
            }
            Some(Err(CompactionNotRetryable(e))) => {
                ENGINE_COMPACTION_RESULT_COUNTER
                    .with_label_values(&["not_retryable"])
                    .inc();
                error!("shard {} compaction failed {}, not retryable", tag, e);
                true
            }
            Some(Err(TableError(table::Error::SchemaOutOfDate(e)))) => {
                ENGINE_COMPACTION_RESULT_COUNTER
                    .with_label_values(&["schema_out_of_date"])
                    .inc();
                error!("shard {} decode row to columnar failed {}", tag, e);
                if let Ok(shard) = self.engine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    if let Some(schema_file) = shard.get_schema_file() {
                        shard.set_outdated_schema_ver(schema_file.get_version());
                    }
                }
                true
            }
            Some(Err(e)) => {
                ENGINE_COMPACTION_RESULT_COUNTER
                    .with_label_values(&["failure"])
                    .inc();
                error!("shard {} compaction failed {}, retrying", tag, e);
                self.compact(id_ver);
                false
            }
            None => {
                ENGINE_COMPACTION_RESULT_COUNTER
                    .with_label_values(&["empty"])
                    .inc();
                info!("shard {} got empty compaction result", tag);
                // The compaction result can be none under complex situations. To avoid
                // compaction not making progress, we trigger it actively.
                if let Ok(shard) = self.engine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    self.engine.refresh_shard_states(&shard);
                }
                self.schedule_pending_compaction();
                true
            }
        };
        if need_reset_compacting {
            let shard = self.engine.get_shard_with_ver(id_ver.id, id_ver.ver).ok();
            if let Some(shard) = shard {
                store_bool(&shard.compacting, false);
            }
        }
    }

    fn compaction_applied(&mut self, id_ver: IdVer) {
        // It's possible the shard is not being pending applied, because it may apply a
        // compaction from the former leader, so we use both `running` and
        // `notified` to detect whether it's compacting. It can avoid spawning
        // more compaction jobs, e.g., a new leader spawns one but not yet
        // finished, and it applies the compaction from the former leader, then the
        // new leader spawns another one. However, it's possible the new compaction is
        // finished and not yet applied, it can result in duplicated compaction
        // but no exceeding compaction jobs.
        self.notified.remove(&id_ver);
    }

    fn clear(&mut self, id_ver: IdVer) {
        self.running.remove(&id_ver);
        self.notified.remove(&id_ver);
        self.pending.remove(&id_ver);
        self.schedule_pending_compaction()
    }

    fn schedule_pending_compaction(&mut self) {
        while let Some(id_ver) = self.pick_highest_pri_pending_shard() {
            if self.is_full() {
                break;
            }
            self.pending.remove(&id_ver);
            self.compact(id_ver);
        }
    }

    fn pick_highest_pri_pending_shard(&mut self) -> Option<IdVer> {
        let engine = self.engine.clone();
        self.pending
            .retain(|id_ver| engine.get_shard_with_ver(id_ver.id, id_ver.ver).is_ok());

        // Find the shard with highest compaction priority.
        self.pending
            .iter()
            .max_by_key(|&&id_ver| {
                self.engine
                    .get_shard_with_ver(id_ver.id, id_ver.ver)
                    .ok()
                    .and_then(|shard| shard.get_compaction_priority())
            })
            .copied()
    }

    fn is_full(&self) -> bool {
        self.running.len() >= self.engine.opts.num_compactors
    }

    fn is_compacting(&self, id_ver: IdVer) -> bool {
        self.running.contains_key(&id_ver) || self.notified.contains(&id_ver)
    }

    fn is_paused(&mut self, id_ver: IdVer, meta_seq: u64) -> bool {
        match self.paused_seq.entry(id_ver) {
            Entry::Occupied(entry) => {
                let paused_seq = *entry.get();
                if meta_seq < paused_seq {
                    true
                } else {
                    entry.remove();
                    let tag = ShardTag::new(self.engine.get_engine_id(), id_ver);
                    info!("{} compaction resumed", tag; "meta_seq" => meta_seq, "paused_seq" => paused_seq);
                    false
                }
            }
            Entry::Vacant(_) => false,
        }
    }

    fn schedule_unblocked_compaction(&mut self) {
        if self.blocked.is_empty() {
            return;
        }
        for (_, blocked_ids) in self
            .blocked
            .extract_if(|keyspace_id, _| !is_compaction_blocked(*keyspace_id))
        {
            for id_ver in blocked_ids {
                let tag = ShardTag::new(self.engine.get_engine_id(), id_ver);
                info!("{} compaction unblocked", tag);
                self.pending.insert(id_ver);
            }
        }
        self.schedule_pending_compaction();
    }
}

fn is_compaction_blocked(keyspace_id: u32) -> bool {
    recovery::is_in_recovery_mode() && !recovery::in_white_list(keyspace_id)
}
