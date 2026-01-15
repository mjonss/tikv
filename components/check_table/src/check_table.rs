// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
    fs,
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::{Buf, BufMut};
use cloud_encryption::MasterKey;
use futures::executor::block_on;
use kvengine::{
    Engine, Shard, ShardMeta, SnapAccess, UserMeta,
    context::{IaCtx, PrepareType, SnapCtx, new_meta_file_cache},
    dfs::{DFSConfig, Dfs, new_dfs_from_config},
    table::{
        columnar::ColumnarMetaCache,
        fts::{FtsCache, FtsDeltaCache},
        sstable::BlockCache,
    },
    txn_chunk_manager::{TxnChunkManager, TxnChunkManagerConfig, with_pool_size},
};
use kvproto::keyspacepb::{KeyspaceMeta, KeyspaceState};
use native_br::{
    common::create_pd_client,
    lock::LockResolver,
    restore::{RestoreConfig, get_cluster_backup_meta},
    restore_keyspace::BackupCluster,
};
use protobuf::Message;
use schema::schema::{DbInfo, STATE_PUBLIC, TableInfo};
use security::SecurityConfig;
use serde_derive::{Deserialize, Serialize};
use tidb_query_datatype::codec::{
    datum,
    datum::INT_FLAG,
    table::{
        ID_LEN, INDEX_VALUE_COMMON_HANDLE_FLAG, INDEX_VALUE_VERSION_FLAG,
        MAX_OLD_ENCODED_VALUE_LEN, PREFIX_LEN, encode_index_seek_key, encode_row_key,
    },
};
use tikv_util::{
    codec::number::{decode_i64, decode_u64},
    error, info,
};
use txn_types::{KvPair, TimeStamp};

use crate::check_table::Handle::{Common, Int};

const TXN_CHUNK_WORKER_POOL_SIZE: usize = 2;
const RESOLVE_LOCKS_BATCH_SIZE: usize = 1024;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CheckTableConfig {
    pub pd: pd_client::Config,
    pub security: SecurityConfig,
    pub dfs: DFSConfig,
    pub data_dir: String,
    pub backup_name: String,
    pub keyspace_ids: Vec<u32>,
    pub skip_keyspace_ids: Vec<u32>,
    pub timestamp: u64,
    pub all: bool,
}

pub struct CheckTableParams {
    /// Effective only when `CheckTableConfig.all` is true.
    pub starts_from_keyspace_id: Option<u32>,
    /// Effective only on first keyspace. Used as partition id as well.
    pub starts_from_table_id: Option<i64>,
    /// Effective only on first keyspace. Used as partition id as well.
    pub ends_to_table_id: Option<i64>,
}

pub fn execute_check_table(config: CheckTableConfig, params: CheckTableParams) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pd_client = Arc::new(create_pd_client(&config.security, &config.pd));
    let dfs_cfg = config.dfs.clone();
    let dfs = new_dfs_from_config(dfs_cfg);
    let master_key = dfs.get_runtime().block_on(config.security.new_master_key());
    let txn_chunk_manager = TxnChunkManager::new(
        vec![],
        dfs.clone(),
        BlockCache::None,
        None,
        with_pool_size(TXN_CHUNK_WORKER_POOL_SIZE),
        TxnChunkManagerConfig::default(),
    );
    let cluster_backup = get_cluster_backup_meta(dfs.as_ref(), config.backup_name.clone());
    let mut keyspace_ids = if config.all {
        if cluster_backup.keyspace_meta.is_empty() {
            panic!("check table: No keyspace meta in backup");
        }
        let mut all_keyspace_ids = vec![];
        let skip_keyspace_ids: HashSet<u32> =
            HashSet::from_iter(config.skip_keyspace_ids.iter().copied());
        for (k, v) in &cluster_backup.keyspace_meta {
            let key_str = String::from_utf8_lossy(k);
            if key_str.contains("/keyspaces/meta/") {
                let mut keyspace_meta = KeyspaceMeta::default();
                let res = keyspace_meta.merge_from_bytes(v);
                if res.is_ok()
                    && keyspace_meta.state == KeyspaceState::Enabled
                    && !skip_keyspace_ids.contains(&keyspace_meta.id)
                    && params
                        .starts_from_keyspace_id
                        .is_none_or(|id| keyspace_meta.id >= id)
                {
                    all_keyspace_ids.push(keyspace_meta.id);
                }
            }
        }
        all_keyspace_ids
    } else {
        config.keyspace_ids.clone()
    };
    keyspace_ids.sort_unstable();
    let keyspace_id = keyspace_ids[0];
    let restore_conf = RestoreConfig {
        security: config.security.clone(),
        ..Default::default()
    };
    let mut cluster = BackupCluster::new(
        &cluster_backup,
        PathBuf::from(&config.data_dir),
        pd_client,
        dfs.clone(),
        restore_conf,
        keyspace_id,
        keyspace_id,
        cluster_backup.backup_ts,
        false,
        None,
        true,
        None,
        None,
    )
    .unwrap();
    let check_table_ts = if config.timestamp > 0 {
        config.timestamp
    } else {
        cluster_backup.backup_ts
    };
    for (idx, keyspace_id) in keyspace_ids.into_iter().enumerate() {
        if idx > 0 {
            cluster.reset_keyspace(keyspace_id, keyspace_id).unwrap();
        }

        let lock_resolver = LockResolver::new(
            cluster.tag(),
            cluster.get_kvengine(),
            cluster_backup.backup_ts,
            RESOLVE_LOCKS_BATCH_SIZE,
            cluster.get_shard_meta_getter(),
        );
        let resolved_locks = rt
            .block_on(lock_resolver.resolve_locks())
            .unwrap_or_else(|err| {
                panic!(
                    "check table: resolve locks failed, keyspace {}, err {:?}",
                    keyspace_id, err
                );
            });
        info!(
            "{} resolve {} locks / {} lock_txn_files of shards {:?}",
            cluster.tag(),
            resolved_locks.total_normal_locks_cnt,
            resolved_locks.total_lock_txn_files_cnt,
            resolved_locks.resolved_shards
        );

        let kv = cluster.get_kvengine();
        let shards = cluster.get_shard_metas_before_flush();
        let backup_reader = Arc::new(BackupReader::new(
            check_table_ts,
            kv,
            shards,
            dfs.clone(),
            master_key.clone(),
            txn_chunk_manager.clone(),
        ));
        let keyspace_prefix = api_version::ApiV2::get_keyspace_prefix_by_id(keyspace_id);
        let dbs = block_on(schema::load_schema(
            backup_reader.clone(),
            &keyspace_prefix,
            TimeStamp::new(check_table_ts),
        ))
        .unwrap();
        let starts_from_table_id = if idx == 0 {
            params.starts_from_table_id
        } else {
            None
        };
        let ends_to_table_id = if idx == 0 {
            params.ends_to_table_id
        } else {
            None
        };
        let tasks = create_check_table_tasks(dbs, starts_from_table_id, ends_to_table_id);
        for task in tasks {
            info!("check table {:?}", task);
            run_task(
                &config,
                keyspace_id,
                &backup_reader,
                &keyspace_prefix,
                &task,
            );
        }
    }
}

#[derive(Debug)]
struct CheckTableTask {
    is_common_handle: bool,
    table_name: String,
    table_id: i64,
    partition_id: Option<i64>,
    indices: Vec<(i64, usize)>,
    table_info: TableInfo,
}

fn create_check_table_tasks(
    dbs: Vec<DbInfo>,
    starts_from_table_id: Option<i64>,
    ends_to_table_id: Option<i64>,
) -> Vec<CheckTableTask> {
    let mut tasks = vec![];
    for db in dbs {
        for tbl in db.tables {
            if tbl.index_info.is_none() {
                info!("skip table with no index to check: {}", &tbl.name.o);
                continue;
            }
            let idx_infos = tbl.index_info.as_ref().unwrap();
            let mut indices = vec![];
            for idx_info in idx_infos {
                if (tbl.is_common_handle || tbl.pk_is_handle) && idx_info.is_primary {
                    continue;
                }
                if idx_info.mv_index == Some(true) || idx_info.vector_index.is_some() {
                    // Multi value index may have multiple or no entries points to a single
                    // handle, we should ignore it.
                    // Vector index is persisted in other forms and not supported yet.
                    continue;
                }
                if idx_info.state == STATE_PUBLIC {
                    indices.push((idx_info.id, idx_info.idx_cols.len()));
                }
            }
            if indices.is_empty() {
                info!("skip table with no index to check: {}", &tbl.name.o);
                continue;
            }
            // sort the indices by id to increase the hit rate of the shard cache.
            indices.sort_by(|a, b| a.0.cmp(&b.0));
            if let Some(partition) = &tbl.partition {
                for partition_def in &partition.definitions {
                    if starts_from_table_id.is_some_and(|id| partition_def.id < id) {
                        continue;
                    }
                    if ends_to_table_id.is_some_and(|id| partition_def.id > id) {
                        continue;
                    }
                    tasks.push(CheckTableTask {
                        table_name: tbl.name.o.clone(),
                        table_id: tbl.id,
                        is_common_handle: tbl.is_common_handle,
                        partition_id: Some(partition_def.id),
                        indices: indices.clone(),
                        table_info: tbl.clone(),
                    })
                }
            } else {
                if starts_from_table_id.is_some_and(|id| tbl.id < id) {
                    continue;
                }
                if ends_to_table_id.is_some_and(|id| tbl.id > id) {
                    continue;
                }
                tasks.push(CheckTableTask {
                    table_name: tbl.name.o.clone(),
                    table_id: tbl.id,
                    is_common_handle: tbl.is_common_handle,
                    partition_id: None,
                    indices,
                    table_info: tbl,
                })
            }
        }
    }
    // sort the tasks by table_id to increase the hit rate of the shard cache.
    tasks.sort_by(|a, b| a.table_id.cmp(&b.table_id));
    tasks
}

fn run_task(
    cfg: &CheckTableConfig,
    keyspace_id: u32,
    reader: &BackupReader,
    keyspace: &[u8],
    task: &CheckTableTask,
) {
    let table_id = task.partition_id.unwrap_or(task.table_id);
    let mut row_start = keyspace.to_vec();
    row_start.extend_from_slice(&encode_row_key(table_id, i64::MIN));
    let mut row_end = keyspace.to_vec();
    row_end.extend_from_slice(&encode_row_key(table_id, i64::MAX));
    let shard_count = reader.shard_count(&row_start, &row_end);
    let segment_count = std::cmp::min(shard_count, 1024);
    let data_dir_path = PathBuf::from(&cfg.data_dir);
    let table_path = table_path(&data_dir_path, keyspace_id, table_id);
    for (idx_id, num_cols) in &task.indices {
        let mut idx_start = keyspace.to_vec();
        idx_start.extend_from_slice(&encode_index_seek_key(table_id, *idx_id, &[]));
        let mut idx_end = keyspace.to_vec();
        idx_end.extend_from_slice(&encode_index_seek_key(table_id, *idx_id + 1, &[]));
        let idx_count = reader
            .dump_index_meta(
                keyspace.len(),
                &idx_start,
                &idx_end,
                task.is_common_handle,
                &table_path,
                *idx_id,
                *num_cols,
                segment_count,
            )
            .unwrap();
        info!("table {} index {} count {}", table_id, idx_id, idx_count);
    }
    let row_count = reader
        .dump_row_meta(
            keyspace.len(),
            &row_start,
            &row_end,
            task.is_common_handle,
            &table_path,
            segment_count,
        )
        .unwrap();
    info!("table {} row count {}", table_id, row_count);
    let checker = TableChecker::new(table_path.clone(), segment_count, task);
    let (invalid_rows, invalid_indices) = checker.check();
    fs::remove_dir_all(&table_path).unwrap();
    if invalid_rows.len() + invalid_indices.len() == 0 {
        info!("table {} {} check ok", task.table_name, task.table_id);
        return;
    }
    error!(
        "keyspace {} table {} {} invalid rows {}, invalid indices {}",
        keyspace_id,
        task.table_name,
        task.table_id,
        invalid_rows.len(),
        invalid_indices.len()
    );
    let invalid_path = invalid_path(&data_dir_path, keyspace_id);
    fs::create_dir_all(&invalid_path).unwrap();

    if !invalid_rows.is_empty() {
        let row_file_path = invalid_path.join(format!("{}.row", table_id));
        let row_file = File::create(row_file_path).unwrap();
        let mut row_writer = BufWriter::new(row_file);
        for (handle, meta) in invalid_rows {
            let mut idx_strs = vec![];
            for (i, &idx_id) in meta.idx_ids.iter().enumerate() {
                let um = meta.idx_metas[i];
                idx_strs.push(format!("idx_{}:{}:{}", idx_id, um.start_ts, um.commit_ts))
            }
            let row_str = format!(
                "{}|{}|{}|{}\n",
                handle,
                meta.user_meta.start_ts,
                meta.user_meta.commit_ts,
                idx_strs.join(",")
            );
            row_writer.write_all(row_str.as_bytes()).unwrap();
        }
        row_writer.flush().unwrap();
    }

    if !invalid_indices.is_empty() {
        let idx_file_path = invalid_path.join(format!("{}.idx", table_id));
        let idx_file = File::create(idx_file_path).unwrap();
        let mut idx_writer = BufWriter::new(idx_file);
        for (idx_id, index_meta) in invalid_indices {
            let idx_str = format!(
                "{}|{}|{}|{}|{}\n",
                idx_id,
                index_meta.handle,
                index_meta.unique,
                index_meta.user_meta.start_ts,
                index_meta.user_meta.commit_ts,
            );
            idx_writer.write_all(idx_str.as_bytes()).unwrap();
        }
        idx_writer.flush().unwrap();
    }

    let schema_file_path = invalid_path.join(format!("{}.schema", table_id));
    let mut schema_file = File::create(schema_file_path).unwrap();
    let schema_str = serde_json::to_string_pretty(&task.table_info).unwrap();
    schema_file.write_all(schema_str.as_bytes()).unwrap();
}

fn table_path(dir: &Path, keyspace_id: u32, table_id: i64) -> PathBuf {
    dir.join(format!("check/{}/{}", keyspace_id, table_id))
}

fn invalid_path(dir: &Path, keyspace_id: u32) -> PathBuf {
    dir.join(format!("invalid/{}", keyspace_id))
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Handle {
    Int(i64),
    Common(Vec<u8>),
}

impl Display for Handle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Int(v) => write!(f, "{}", v),
            Common(v) => write!(f, "{}", hex::encode(v)),
        }
    }
}

impl Handle {
    fn parse_from_row_key(mut key: &[u8], common_handle: bool) -> Self {
        if !common_handle {
            Int(decode_i64(&mut key).unwrap())
        } else {
            Common(key.to_vec())
        }
    }

    fn parse_from_index_key(mut key: &[u8], common_handle: bool) -> Self {
        if !common_handle {
            let flag = key.get_u8();
            let i = if flag == INT_FLAG {
                decode_i64(&mut key).unwrap()
            } else {
                decode_u64(&mut key).unwrap() as i64
            };
            Int(i)
        } else {
            Common(key.to_vec())
        }
    }

    fn parse_from_index_val(mut val: &[u8], common_handle: bool) -> Self {
        if val.len() <= MAX_OLD_ENCODED_VALUE_LEN {
            let i = val.get_u64() as i64;
            Int(i)
        } else {
            let tail_len = val.get_u8() as usize;
            if common_handle {
                let mut flag = val.get_u8();
                if flag == INDEX_VALUE_VERSION_FLAG {
                    val.get_u8();
                    flag = val.get_u8();
                }
                assert_eq!(flag, INDEX_VALUE_COMMON_HANDLE_FLAG);
                let handle_len = val.get_u16();
                val = &val[..handle_len as usize];
                Common(val.to_vec())
            } else {
                let mut tail = &val[val.len() - tail_len..];
                Int(tail.get_u64() as i64)
            }
        }
    }

    fn segment_id(&self, segment_count: usize) -> usize {
        match self {
            Int(i) => farmhash::fingerprint64(&i.to_be_bytes()) as usize % segment_count,
            Common(v) => farmhash::fingerprint64(v) as usize % segment_count,
        }
    }
}

#[derive(Debug, Clone)]
struct RowMeta {
    user_meta: UserMeta,
    idx_ids: Vec<i16>,
    idx_metas: Vec<UserMeta>,
}

#[derive(Debug, Clone)]
struct IndexMeta {
    handle: Handle,
    unique: bool,
    user_meta: UserMeta,
}

#[derive(Clone)]
pub(crate) struct BackupReader {
    ts: u64,
    kv: Engine,
    snap_ctx: SnapCtx,
    metas: Arc<Vec<ShardMeta>>,
    snap_cache: Arc<Mutex<Option<SnapAccess>>>,
}

impl BackupReader {
    pub(crate) fn new(
        ts: u64,
        kv: Engine,
        metas: Vec<ShardMeta>,
        dfs: Arc<dyn Dfs>,
        master_key: MasterKey,
        txn_chunk_manager: TxnChunkManager,
    ) -> Self {
        let snap_ctx = SnapCtx {
            dfs,
            master_key,
            block_cache: BlockCache::None,
            vector_index_cache: None,
            columnar_file_cache: None,
            fts_cache: FtsCache::disabled(),
            fts_delta_cache: FtsDeltaCache::disabled(),
            schema_files: None,
            txn_chunk_manager,
            ia_ctx: IaCtx::Disabled,
            prepare_type: PrepareType::All,
            read_columnar: true,
            meta_file_cache: new_meta_file_cache(1024),
            columnar_meta_cache: ColumnarMetaCache::default(),
        };
        Self {
            ts,
            kv,
            snap_ctx,
            metas: Arc::new(metas),
            snap_cache: Arc::new(Mutex::new(None)),
        }
    }

    fn dump_row_meta(
        &self,
        keyspace_len: usize,
        start: &[u8],
        end: &[u8],
        common_handle: bool,
        table_path: &PathBuf,
        segment_count: usize,
    ) -> Result<usize, String> {
        let mut meta_writer = MetaWriter::new(table_path, segment_count, None);
        self.iterate(start, end, |key, user_meta, _| -> bool {
            let handle =
                Handle::parse_from_row_key(&key[keyspace_len + PREFIX_LEN..], common_handle);
            meta_writer.write_row(handle, user_meta);
            true
        })?;
        Ok(meta_writer.finish())
    }

    fn dump_index_meta(
        &self,
        keyspace_len: usize,
        start: &[u8],
        end: &[u8],
        common_handle: bool,
        table_path: &PathBuf,
        idx_id: i64,
        num_cols: usize,
        segment_count: usize,
    ) -> Result<usize, String> {
        let mut meta_writer = MetaWriter::new(table_path, segment_count, Some(idx_id));
        self.iterate(start, end, |key, user_meta, val| -> bool {
            let idx_cols_data = &key[keyspace_len + PREFIX_LEN + ID_LEN..];
            let mut buf = idx_cols_data;
            datum::skip_n(&mut buf, num_cols)
                .map_err(|e| e.to_string())
                .unwrap();
            let (handle, unique) = if !buf.is_empty() {
                (Handle::parse_from_index_key(buf, common_handle), false)
            } else {
                (Handle::parse_from_index_val(val, common_handle), true)
            };
            meta_writer.write_index(handle, user_meta, unique);
            true
        })?;
        Ok(meta_writer.finish())
    }

    fn get_snap(&self, meta: &ShardMeta) -> SnapAccess {
        if let Some(shard) = self.kv.get_shard(meta.id) {
            return shard.new_snap_access();
        }
        let cached = self.snap_cache.lock().unwrap().clone();
        if let Some(snap) = cached {
            if snap.get_id() == meta.id {
                return snap;
            }
        }
        let runtime = self.snap_ctx.dfs.get_runtime();
        let tag = format!("backup_reader_{}", meta.tag());
        let snap = runtime
            .block_on(SnapAccess::from_change_set(
                &tag,
                &self.snap_ctx,
                meta.to_change_set(),
                true,
            ))
            .unwrap();
        let mut guard = self.snap_cache.lock().unwrap();
        *guard = Some(snap.clone());
        snap
    }

    fn iterate<F>(&self, start: &[u8], end: &[u8], mut f: F) -> Result<(), String>
    where
        F: FnMut(&[u8], UserMeta, &[u8]) -> bool, // key, meta, user_meta, value
    {
        let mut key_buf = vec![];
        let mut seek_key = start;
        let idx = self
            .metas
            .binary_search_by(|a| a.range.outer_end.chunk().cmp(start))
            .unwrap_or_else(|e| e);
        for meta in &self.metas[idx..] {
            if meta.range.outer_end.chunk() == start {
                continue;
            }
            if meta.range.outer_start.chunk() >= end {
                return Ok(());
            }
            let snap = self.get_snap(meta);
            let mut it = snap.new_iterator(0, false, false, Some(self.ts), false);
            it.seek(seek_key);
            while it.valid() {
                if it.key() >= end {
                    return Ok(());
                }
                key_buf.truncate(0);
                key_buf.extend_from_slice(it.key());
                let um = UserMeta::from_slice(it.user_meta());
                let val = it.val();
                if !val.is_empty() && !f(&key_buf, um, val) {
                    return Ok(());
                }
                it.next();
            }
            seek_key = meta.range.outer_end.chunk();
        }
        Ok(())
    }

    fn shard_count(&self, start: &[u8], end: &[u8]) -> usize {
        let idx = self
            .metas
            .binary_search_by(|a| a.range.outer_end.chunk().cmp(start))
            .unwrap_or_else(|e| e);
        let mut count = 0;
        for meta in &self.metas[idx..] {
            if meta.range.outer_end.chunk() == start {
                continue;
            }
            if meta.range.outer_start.chunk() >= end {
                break;
            }
            count += 1;
        }
        count
    }
}

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct NoopRecoverHandler {}

impl kvengine::RecoverHandler for NoopRecoverHandler {
    fn recover(&self, _: &Engine, _: &Arc<Shard>, _: &ShardMeta, _: bool) -> kvengine::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl schema::KvScanner for BackupReader {
    async fn scan(
        &self,
        start: &[u8],
        end: &[u8],
        _start_ts: TimeStamp,
    ) -> Result<Vec<KvPair>, String> {
        let mut pairs = vec![];
        self.iterate(start, end, |key, _, val| {
            pairs.push((key.to_vec(), val.to_vec()));
            true
        })?;
        Ok(pairs)
    }
}

struct SegmentWriter {
    file: fs::File,
    buf: Vec<u8>,
    cnt: u64,
}

impl SegmentWriter {
    fn new(table_path: &PathBuf, seg_id: usize, idx_id: Option<i64>) -> Self {
        fs::create_dir_all(table_path).unwrap();
        let file_path = if let Some(idx_id) = idx_id {
            table_path.join(format!("{}.idx_{}", seg_id, idx_id))
        } else {
            table_path.join(format!("{}.row", seg_id))
        };
        let file = fs::File::create(file_path).unwrap();
        Self {
            file,
            buf: vec![],
            cnt: 0,
        }
    }

    fn write_handle_user_meta(&mut self, handle: Handle, um: UserMeta) {
        match handle {
            Int(i) => self.buf.put_i64(i),
            Common(v) => {
                self.buf.put_u16(v.len() as u16);
                self.buf.extend_from_slice(&v);
            }
        }
        self.buf.put_u64(um.start_ts);
        self.buf.put_u64(um.commit_ts);
    }

    fn maybe_flush(&mut self) {
        if self.buf.len() > 256 * 1024 {
            self.file.write_all(&self.buf).unwrap();
            self.buf.truncate(0);
        }
    }

    fn write_row(&mut self, handle: Handle, um: UserMeta) {
        self.write_handle_user_meta(handle, um);
        self.cnt += 1;
        self.maybe_flush();
    }

    fn write_idx(&mut self, handle: Handle, um: UserMeta, unique: bool) {
        self.write_handle_user_meta(handle, um);
        self.buf.put_u8(unique as u8);
        self.cnt += 1;
        self.maybe_flush();
    }

    fn finish(&mut self) -> usize {
        self.buf.put_u64(self.cnt);
        self.file.write_all(&self.buf).unwrap();
        self.cnt as usize
    }
}

struct MetaWriter {
    segments: Vec<SegmentWriter>,
}

impl MetaWriter {
    fn new(table_path: &PathBuf, segment_count: usize, idx_id: Option<i64>) -> Self {
        let mut segments = Vec::with_capacity(segment_count);
        for segment in 0..segment_count {
            let seg_writer = SegmentWriter::new(table_path, segment, idx_id);
            segments.push(seg_writer);
        }
        Self { segments }
    }

    fn write_row(&mut self, handle: Handle, um: UserMeta) {
        let shard_id = handle.segment_id(self.segments.len());
        self.segments[shard_id].write_row(handle, um);
    }

    fn write_index(&mut self, handle: Handle, um: UserMeta, unique: bool) {
        let shard_id = handle.segment_id(self.segments.len());
        self.segments[shard_id].write_idx(handle, um, unique);
    }

    fn finish(&mut self) -> usize {
        let mut count = 0;
        for shard in &mut self.segments {
            count += shard.finish();
        }
        count
    }
}

struct TableChecker {
    table_path: PathBuf,
    segment_count: usize,
    idx_ids: Vec<i16>,
    common_handle: bool,
}

impl TableChecker {
    fn new(table_path: PathBuf, segment_count: usize, task: &CheckTableTask) -> Self {
        let idx_ids = task
            .indices
            .iter()
            .map(|(idx_id, ..)| *idx_id as i16)
            .collect();
        let common_handle = task.is_common_handle;
        Self {
            table_path,
            segment_count,
            idx_ids,
            common_handle,
        }
    }

    fn check(mut self) -> (Vec<(Handle, RowMeta)>, Vec<(i16, IndexMeta)>) {
        let mut invalid_rows = vec![];
        let mut invalid_indices = vec![];
        for segment_id in 0..self.segment_count {
            let (segment_invalid_rows, segment_invalid_indices) = self.check_segment(segment_id);
            invalid_rows.extend_from_slice(&segment_invalid_rows);
            invalid_indices.extend_from_slice(&segment_invalid_indices);
        }
        (invalid_rows, invalid_indices)
    }

    fn check_segment(
        &mut self,
        segment_id: usize,
    ) -> (Vec<(Handle, RowMeta)>, Vec<(i16, IndexMeta)>) {
        let mut invalid_rows = vec![];
        let mut invalid_indices = vec![];
        let mut row_metas = self.load_segment_row_meta(segment_id);
        for &idx_id in &self.idx_ids {
            self.iterate_segment_idx_meta(segment_id, idx_id, |idx_meta| {
                if let Some(row_meta) = row_metas.get_mut(&idx_meta.handle) {
                    row_meta.idx_ids.push(idx_id);
                    row_meta.idx_metas.push(idx_meta.user_meta);
                } else {
                    invalid_indices.push((idx_id, idx_meta));
                }
            });
        }
        for (handle, row_meta) in row_metas {
            if row_meta.idx_ids != self.idx_ids {
                invalid_rows.push((handle, row_meta))
            }
        }
        (invalid_rows, invalid_indices)
    }

    fn load_segment_row_meta(&self, segment_id: usize) -> HashMap<Handle, RowMeta> {
        let row_file = row_file_path(&self.table_path, segment_id);
        let data = fs::read(row_file).unwrap();
        let mut count_buf = &data[data.len() - 8..];
        let count = count_buf.get_u64() as usize;
        let mut row_metas = HashMap::with_capacity(count);
        let mut buf = &data[..data.len() - 8];
        while !buf.is_empty() {
            let (handle, user_meta) = self.get_handle_user_meta(&mut buf);
            row_metas.insert(
                handle,
                RowMeta {
                    user_meta,
                    idx_ids: Vec::with_capacity(self.idx_ids.len()),
                    idx_metas: Vec::with_capacity(self.idx_ids.len()),
                },
            );
        }
        row_metas
    }

    fn iterate_segment_idx_meta<F>(&self, seg_id: usize, idx_id: i16, mut f: F)
    where
        F: FnMut(IndexMeta),
    {
        let idx_file = idx_file_path(&self.table_path, seg_id, idx_id);
        let data = fs::read(idx_file).unwrap();
        let mut buf = &data[..data.len() - 8];
        while !buf.is_empty() {
            let (handle, user_meta) = self.get_handle_user_meta(&mut buf);
            let unique = buf.get_u8() > 0;
            let idx_meta = IndexMeta {
                handle,
                user_meta,
                unique,
            };
            f(idx_meta);
        }
    }

    fn get_handle_user_meta(&self, buf: &mut &[u8]) -> (Handle, UserMeta) {
        let handle = if self.common_handle {
            let handle_len = buf.get_u16() as usize;
            let handle = buf.get(..handle_len).unwrap().to_vec();
            buf.advance(handle_len);
            Common(handle)
        } else {
            let handle = buf.get_i64();
            Int(handle)
        };
        let start_ts = buf.get_u64();
        let commit_ts = buf.get_u64();
        (handle, UserMeta::new(start_ts, commit_ts))
    }
}

fn row_file_path(table_path: &Path, seg_id: usize) -> PathBuf {
    table_path.join(format!("{}.row", seg_id))
}

fn idx_file_path(table_path: &Path, seg_id: usize, idx_id: i16) -> PathBuf {
    table_path.join(format!("{}.idx_{}", seg_id, idx_id))
}
