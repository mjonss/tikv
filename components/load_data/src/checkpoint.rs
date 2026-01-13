// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    fs,
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use chrono::Utc;
use dashmap::DashMap;
use serde_derive::{Deserialize, Serialize};
use tikv_client::Value;
use tikv_util::{debug, error, info};

use crate::{
    Error,
    kv::{DuplicateEntry, SstMeta},
    metrics::LOAD_DATA_TASK_STATE,
    task::{LoadTaskMsg, LoadTaskScheduler, LoadTaskStates, TaskContext},
};

pub type Result<T> = std::result::Result<T, Error>;
pub const CHECKPOINT_WORKER_PREFIX: &str = "LOAD_DATA_CHECK_POINT_";
pub const CHECKPOINT_TMP_FILE_SUFFIX: &str = ".tmp";

pub const CANCELLED_TASK_EXPIRE_SEC: i64 = 3 * 60 * 60; // 3h
pub const FINISHED_TASK_EXPIRE_SEC: i64 = 3 * 60; // 3m
pub const IDLE_TASK_EXPIRE_SEC: i64 = 3 * 24 * 60 * 60; // 3d
pub const CLEANUP_INTERVAL_SEC: u64 = 60; // 1m

lazy_static::lazy_static! {
    static ref FILE_LOCK: Mutex<()> = Mutex::new(());
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub enum LoadDataWorkerState {
    InitTask = 0,
    AddingChunks = 20,
    BuildingSst = 60,
    IngestingSst = 90,
    IngestedSst = 100,
}

impl Default for LoadDataWorkerState {
    fn default() -> Self {
        Self::InitTask
    }
}

impl LoadDataWorkerState {
    pub fn transition(&mut self, new_state: LoadDataWorkerState) -> bool {
        if !self.check_state(new_state) {
            return false;
        }

        *self = new_state;
        true
    }

    pub fn check_state(&self, new_state: LoadDataWorkerState) -> bool {
        if *self == new_state {
            return true;
        }

        match self {
            LoadDataWorkerState::InitTask => {
                if new_state == LoadDataWorkerState::AddingChunks
                    || new_state == LoadDataWorkerState::BuildingSst
                {
                    return true;
                }
            }
            LoadDataWorkerState::AddingChunks => {
                if new_state == LoadDataWorkerState::BuildingSst {
                    return true;
                }
            }
            LoadDataWorkerState::BuildingSst => {
                if new_state == LoadDataWorkerState::IngestingSst
                    || new_state == LoadDataWorkerState::IngestedSst
                {
                    return true;
                }
            }
            LoadDataWorkerState::IngestingSst => {
                if new_state == LoadDataWorkerState::IngestedSst {
                    return true;
                }
            }
            LoadDataWorkerState::IngestedSst => {
                if new_state == LoadDataWorkerState::IngestedSst {
                    return true;
                }
            }
        }
        false
    }

    pub(crate) fn as_str(&self) -> &str {
        match *self {
            LoadDataWorkerState::InitTask => "InitTask",
            LoadDataWorkerState::AddingChunks => "AddingChunks",
            LoadDataWorkerState::BuildingSst => "BuildingSst",
            LoadDataWorkerState::IngestingSst => "IngestingSst",
            LoadDataWorkerState::IngestedSst => "IngestedSst",
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Default, PartialEq)]
#[serde(default)]
pub struct FileMeta {
    pub file_path: PathBuf,
    pub kv_count: usize,
    pub kv_size: usize,
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
#[serde(default)]
pub struct KvPairsWorkerCtx {
    pub key_comm_prefix: Vec<u8>,
    pub flushed_chunk_ids: HashMap<u64, u64>,
    pub l0_file_metas: Vec<FileMeta>,
    pub l1_file_metas: Vec<FileMeta>,
    pub duplicated_entries: Vec<DuplicateEntry>,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
#[serde(default)]
pub struct BuildingWorkerCtx {
    pub sst_metas: Vec<SstMeta>,
    pub duplicated_entries: Vec<DuplicateEntry>,
    pub ingested: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
#[serde(default)]
pub struct LoadDataCheckpointCtx {
    // TaskContext
    pub task_id: String,
    start_ts: u64,
    commit_ts: u64,
    first_key: Bytes,

    // KVPairsWorker & BuildingWorker
    kvpairs_workers_ctx: HashMap<u64 /* worker_id */, KvPairsWorkerCtx>,
    building_workers_ctx: HashMap<u64 /* worker_id */, BuildingWorkerCtx>,

    // common
    compression: u8,
    state: LoadDataWorkerState,
    duplicated_entries: Vec<DuplicateEntry>,
    is_recover: bool,
    pub canceled: bool,
    pub error: String,
}

impl LoadDataCheckpointCtx {
    pub fn new(task_ctx: TaskContext) -> Self {
        let now = Utc::now();
        let millis = now.timestamp_millis();
        LOAD_DATA_TASK_STATE
            .with_label_values(&[&task_ctx.task_id, LoadDataWorkerState::InitTask.as_str()])
            .set(millis as f64);

        Self {
            task_id: task_ctx.clone().task_id,
            start_ts: task_ctx.start_ts,
            commit_ts: task_ctx.commit_ts,
            compression: 0,
            state: LoadDataWorkerState::InitTask,
            duplicated_entries: vec![],
            is_recover: false,
            first_key: Default::default(),
            canceled: false,
            error: "".to_string(),
            kvpairs_workers_ctx: HashMap::default(),
            building_workers_ctx: HashMap::default(),
        }
    }

    pub fn get_first_key(&self) -> Bytes {
        self.first_key.clone()
    }

    pub fn get_compression(&self) -> u8 {
        self.compression
    }

    pub fn get_state(&self) -> LoadDataWorkerState {
        self.state
    }

    pub fn get_duplicated_entries(&self) -> Vec<DuplicateEntry> {
        self.duplicated_entries.clone()
    }

    pub fn get_is_recover(&self) -> bool {
        self.is_recover
    }

    pub fn set_is_recover(&mut self, is_recover: bool) {
        self.is_recover = is_recover;
    }

    pub fn get_commit_ts(&self) -> u64 {
        self.commit_ts
    }

    pub fn get_start_ts(&self) -> u64 {
        self.start_ts
    }

    pub fn get_task_id(&self) -> String {
        self.task_id.clone()
    }

    pub fn get_kvpairs_worker_ctx(&mut self, worker_id: u64) -> &KvPairsWorkerCtx {
        self.kvpairs_workers_ctx.entry(worker_id).or_default()
    }

    pub fn get_building_worker_ctx(&mut self, worker_id: u64) -> &BuildingWorkerCtx {
        self.building_workers_ctx.entry(worker_id).or_default()
    }
}

pub struct LocalFileCheckpointStorage {
    data_path: PathBuf,
    file_name: String,
    pub checkpoint_ctx: LoadDataCheckpointCtx,
}

impl LocalFileCheckpointStorage {
    pub fn get_file_name_by_taskid(task_id: String) -> String {
        CHECKPOINT_WORKER_PREFIX.to_string() + &task_id
    }
    pub fn new(checkpoint_ctx: LoadDataCheckpointCtx, data_path: PathBuf) -> Result<Self> {
        let file_name =
            LocalFileCheckpointStorage::get_file_name_by_taskid(checkpoint_ctx.clone().task_id);

        Ok(Self {
            data_path,
            file_name,
            checkpoint_ctx,
        })
    }

    fn checkpoint_ctx_to_binary(&self) -> Vec<u8> {
        let res = serde_json::to_string(&self.checkpoint_ctx.clone()).unwrap();
        res.as_bytes().to_owned()
    }

    pub fn binary_to_checkpoint(json_str: &str) -> LoadDataCheckpointCtx {
        serde_json::from_str(json_str).unwrap()
    }

    fn write_atomic_file(&mut self, content: &[u8]) -> Result<()> {
        // Get mutex lock.
        let _lock = FILE_LOCK.lock();

        let tmp_file_path = self.file_name.clone() + CHECKPOINT_TMP_FILE_SUFFIX;
        let tmp_file = self.data_path.join(tmp_file_path);

        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&tmp_file)?;
        file.write_all(content)?;
        file.flush()?;

        fs::rename(tmp_file, self.get_file_path())?;
        Ok(())
    }

    pub fn clean_checkpoint_data(&self) {
        let task_id = &self.checkpoint_ctx.task_id;
        let file_path = self.get_file_path();
        info!("{} remove checkpoint data :{:?}", task_id, file_path);
        if let Err(err) = fs::remove_file(file_path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                error!("{} failed to delete checkpoint file: {}", task_id, err);
            }
        }
    }

    pub fn update_cancel_and_errmsg(&mut self, canceled: bool, errmsg: String) -> Result<()> {
        self.checkpoint_ctx.canceled = canceled;
        self.checkpoint_ctx.error = errmsg;
        self.flush_checkpoint_ctx()
    }

    fn get_file_path(&self) -> PathBuf {
        self.data_path.join(self.file_name.clone())
    }

    pub fn read_file(path: PathBuf) -> String {
        fs::read_to_string(path).unwrap()
    }

    fn transition(&mut self, new_state: LoadDataWorkerState) -> bool {
        let old_state = self.checkpoint_ctx.state;
        let is_succ = self.checkpoint_ctx.state.transition(new_state);
        if old_state != new_state {
            let now = Utc::now();
            let millis = now.timestamp_millis();
            LOAD_DATA_TASK_STATE
                .with_label_values(&[&self.checkpoint_ctx.task_id, new_state.as_str()])
                .set(millis as f64);
        }
        if !is_succ {
            error!(
                "{} [checkpoint] transition try check state failed, from {:?} to {:?}",
                self.checkpoint_ctx.task_id, old_state, new_state
            );
        } else {
            info!(
                "{} [checkpoint] transition try check state succeed, from {:?} to {:?}",
                self.checkpoint_ctx.task_id, old_state, new_state
            );
        }
        is_succ
    }

    pub fn update_build_msg(&mut self, compression_type: u8) -> Result<()> {
        self.checkpoint_ctx.compression = compression_type;
        self.flush_checkpoint_ctx_with_state(LoadDataWorkerState::BuildingSst)?;
        Ok(())
    }

    pub fn get_state(&self) -> LoadDataWorkerState {
        self.checkpoint_ctx.get_state()
    }

    pub fn flush_checkpoint_ctx_with_state(
        &mut self,
        new_state: LoadDataWorkerState,
    ) -> Result<()> {
        let is_succ = self.transition(new_state);

        if !is_succ {
            return Err(Error::CheckError("transition state err".to_string()));
        }
        self.checkpoint_ctx.state = new_state;
        self.flush_checkpoint_ctx()?;
        Ok(())
    }

    pub fn flush_checkpoint_ctx(&mut self) -> Result<()> {
        let value: Value = self.checkpoint_ctx_to_binary().to_vec();
        self.write_atomic_file(value.as_slice())?;
        Ok(())
    }

    pub fn load_checkpoint_ctx(&self) -> LoadDataCheckpointCtx {
        let file_data = LocalFileCheckpointStorage::read_file(self.get_file_path());
        let checkpoint = LocalFileCheckpointStorage::binary_to_checkpoint(file_data.as_str());
        debug!(
            "{} [checkpoint store] loaded checkpoint: {:?},",
            self.checkpoint_ctx.task_id, checkpoint
        );
        checkpoint
    }
}

// The following methods are used by KvPairsWorker & BuildingWorker.
impl LocalFileCheckpointStorage {
    pub fn get_is_recover(&self) -> bool {
        self.checkpoint_ctx.get_is_recover()
    }

    pub fn update_first_key(&mut self, first_key: Bytes) -> Result<()> {
        self.checkpoint_ctx.first_key = first_key;
        self.flush_checkpoint_ctx_with_state(LoadDataWorkerState::AddingChunks)
    }

    pub fn update_l0_flushed_info(
        &mut self,
        worker_id: u64,
        handled_chunk_ids: HashMap<u64, u64>,
        mut l0_file_metas: Vec<FileMeta>,
        key_comm_prefix: Vec<u8>,
    ) -> Result<()> {
        let worker_ctx = self
            .checkpoint_ctx
            .kvpairs_workers_ctx
            .get_mut(&worker_id)
            .unwrap();
        worker_ctx.flushed_chunk_ids = handled_chunk_ids;
        worker_ctx.key_comm_prefix = key_comm_prefix;
        worker_ctx.l0_file_metas.append(&mut l0_file_metas);

        self.flush_checkpoint_ctx_with_state(LoadDataWorkerState::AddingChunks)
    }

    pub fn update_l1_flushed_info(
        &mut self,
        worker_id: u64,
        l1_file_metas: Vec<FileMeta>,
        duplicated_entries: Vec<DuplicateEntry>,
    ) -> Result<()> {
        let worker_ctx = self
            .checkpoint_ctx
            .kvpairs_workers_ctx
            .get_mut(&worker_id)
            .unwrap();
        worker_ctx.l1_file_metas = l1_file_metas;
        worker_ctx.duplicated_entries = duplicated_entries;

        self.flush_checkpoint_ctx_with_state(LoadDataWorkerState::BuildingSst)
    }

    pub fn update_sst_metas(
        &mut self,
        worker_id: u64,
        sst_metas: Vec<SstMeta>,
        duplicated_entries: Vec<DuplicateEntry>,
    ) -> Result<()> {
        let worker_ctx = self
            .checkpoint_ctx
            .building_workers_ctx
            .get_mut(&worker_id)
            .unwrap();

        worker_ctx.sst_metas = sst_metas;
        worker_ctx.duplicated_entries = duplicated_entries;
        self.flush_checkpoint_ctx_with_state(LoadDataWorkerState::BuildingSst)
    }

    pub fn set_worker_ingested(&mut self, worker_id: u64) -> Result<()> {
        let worker_ctx = self
            .checkpoint_ctx
            .building_workers_ctx
            .get_mut(&worker_id)
            .unwrap();
        worker_ctx.ingested = true;
        self.flush_checkpoint_ctx_with_state(LoadDataWorkerState::IngestingSst)
    }

    pub fn set_ingested(&mut self, duplicated_entries: Vec<DuplicateEntry>) -> Result<()> {
        self.checkpoint_ctx.duplicated_entries = duplicated_entries;
        self.flush_checkpoint_ctx_with_state(LoadDataWorkerState::IngestedSst)
    }
}

struct TracingTaskState {
    updated_at: i64,
    canceled_at: i64,
    canceled: bool,
    flushed_files: usize,
    created_files: usize,
    ingested_regions: usize,
}

pub struct LoadDataCleanupWorker {
    running_tasks: Arc<DashMap<String, LoadTaskScheduler>>,
    tracing_task_states: HashMap<String, TracingTaskState>,
    cleanup_interval_secs: u64,
    cancelled_task_expire_secs: i64,
    finished_task_expire_secs: i64,
    idle_task_expire_secs: i64,
}

impl LoadDataCleanupWorker {
    pub fn new(
        running_tasks: Arc<DashMap<String, LoadTaskScheduler>>,
        cleanup_interval_secs: u64,
        cancelled_task_expire_secs: i64,
        finished_task_expire_secs: i64,
        idle_task_expire_secs: i64,
    ) -> Self {
        Self {
            running_tasks,
            tracing_task_states: HashMap::default(),
            cleanup_interval_secs,
            cancelled_task_expire_secs,
            finished_task_expire_secs,
            idle_task_expire_secs,
        }
    }

    pub fn run(&mut self) {
        let interval = Duration::from_secs(self.cleanup_interval_secs);
        info!("start to run cleanup worker");
        loop {
            let task_states: Vec<LoadTaskStates> = self
                .running_tasks
                .iter()
                .map(|x| {
                    x.check_task_thread_finished();
                    x.states.read().unwrap().clone()
                })
                .collect();

            let now_timestamp = chrono::Utc::now().timestamp();
            for task_state in task_states {
                let task_id = task_state.task_id;
                let tracing_task_state =
                    self.tracing_task_states
                        .entry(task_id.clone())
                        .or_insert(TracingTaskState {
                            updated_at: now_timestamp,
                            canceled_at: 0,
                            canceled: false,
                            flushed_files: 0,
                            created_files: 0,
                            ingested_regions: 0,
                        });

                if tracing_task_state.flushed_files != task_state.flushed_files
                    || tracing_task_state.created_files != task_state.created_files
                    || tracing_task_state.ingested_regions != task_state.ingested_regions
                {
                    tracing_task_state.flushed_files = task_state.flushed_files;
                    tracing_task_state.created_files = task_state.created_files;
                    tracing_task_state.ingested_regions = task_state.ingested_regions;
                    tracing_task_state.updated_at = now_timestamp;
                }

                if tracing_task_state.canceled != task_state.canceled {
                    tracing_task_state.canceled = task_state.canceled;
                    tracing_task_state.canceled_at = now_timestamp;
                }

                if tracing_task_state.canceled_at > 0 {
                    let duration = now_timestamp - tracing_task_state.canceled_at;
                    if (task_state.finished && duration > self.finished_task_expire_secs)
                        || duration > self.cancelled_task_expire_secs
                    {
                        let log_msg = if task_state.finished {
                            "finished"
                        } else {
                            "canceled"
                        };
                        info!("clean up {} task {}", log_msg, task_id);
                        if let Some((_, scheduler)) = self.running_tasks.remove(&task_id) {
                            scheduler.sender.send(LoadTaskMsg::Cleanup).unwrap();
                        }
                    }
                }

                if now_timestamp - tracing_task_state.updated_at > self.idle_task_expire_secs {
                    info!("clean up idle task {}", task_id,);
                    if let Some((_, scheduler)) = self.running_tasks.remove(&task_id) {
                        scheduler.cancel("gc by cleanup worker".to_string());
                        scheduler.sender.send(LoadTaskMsg::Cleanup).unwrap();
                    }
                }
            }

            let task_ids: Vec<String> = self
                .tracing_task_states
                .iter()
                .map(|x| x.0.clone())
                .collect();
            for task_id in &task_ids {
                if !self.running_tasks.contains_key(task_id) {
                    self.tracing_task_states.remove(task_id);
                }
            }
            std::thread::sleep(interval);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::RwLock;

    use tempfile::TempDir;
    use tikv_util::mpsc::Receiver;

    use super::*;

    #[test]
    fn test_local_file_store() {
        let task_id = "task_id_001".to_string();
        let state = LoadDataWorkerState::AddingChunks;
        let task_ctx = TaskContext {
            task_id: task_id.clone(),
            start_ts: 1_u64,
            commit_ts: 1_u64,
            inner_key_off: None,
            outer_key_prefix: vec![],
            encryption_key: None,
            keyspace_id: None,
        };
        let checkpoint_ctx = LoadDataCheckpointCtx::new(task_ctx);
        let checkpoint_dir = TempDir::new().unwrap();

        let mut store =
            LocalFileCheckpointStorage::new(checkpoint_ctx, checkpoint_dir.path().to_owned())
                .unwrap();
        store.flush_checkpoint_ctx_with_state(state).unwrap();
        let checkpoint_ctx = store.load_checkpoint_ctx();
        assert_eq!(task_id, checkpoint_ctx.task_id);
        assert_eq!(state, checkpoint_ctx.state);

        // add chunks
        // update_first_key
        let expect_first_key = Bytes::from_static(b"test_first_key");
        store.update_first_key(expect_first_key.clone()).unwrap();
        let checkpoint_ctx = store.load_checkpoint_ctx();
        assert_eq!(expect_first_key, checkpoint_ctx.first_key);

        let expect_is_recover = true;
        store.checkpoint_ctx.set_is_recover(true);
        store.flush_checkpoint_ctx().unwrap();
        let checkpoint_ctx = store.load_checkpoint_ctx();
        assert_eq!(expect_is_recover, checkpoint_ctx.is_recover);

        // update_l0_flushed_info
        let _ = store.checkpoint_ctx.get_kvpairs_worker_ctx(0);
        let mut handled_chunk_ids: HashMap<u64, u64> = HashMap::new();
        handled_chunk_ids.insert(1, 100);
        handled_chunk_ids.insert(2, 200);
        handled_chunk_ids.insert(3, 300);
        let l0_file_metas: Vec<FileMeta> = vec![
            FileMeta {
                file_path: PathBuf::from("/path/to/file1"),
                kv_count: 10,
                kv_size: 100,
                first_key: "test_first_key1".as_bytes().to_vec(),
                last_key: "test_last_key1".as_bytes().to_vec(),
            },
            FileMeta {
                file_path: PathBuf::from("/path/to/file2"),
                kv_count: 20,
                kv_size: 200,
                first_key: "test_first_key2".as_bytes().to_vec(),
                last_key: "test_last_key2".as_bytes().to_vec(),
            },
            FileMeta {
                file_path: PathBuf::from("/path/to/file3"),
                kv_count: 30,
                kv_size: 300,
                first_key: "test_first_key3".as_bytes().to_vec(),
                last_key: "test_last_key3".as_bytes().to_vec(),
            },
        ];
        let key_comm_prefix = "test_".as_bytes().to_vec();
        store
            .update_l0_flushed_info(
                0,
                handled_chunk_ids,
                l0_file_metas.clone(),
                key_comm_prefix.clone(),
            )
            .unwrap();
        let mut checkpoint_ctx = store.load_checkpoint_ctx();
        let worker_ctx = checkpoint_ctx.get_kvpairs_worker_ctx(0);
        assert_eq!(l0_file_metas, worker_ctx.l0_file_metas);
        assert_eq!(key_comm_prefix, worker_ctx.key_comm_prefix);

        // build sst
        // update_build_msg
        let compression_type = 1;
        store.update_build_msg(compression_type).unwrap();
        let checkpoint_ctx = store.load_checkpoint_ctx();
        assert_eq!(compression_type, checkpoint_ctx.compression);
        assert_eq!(LoadDataWorkerState::BuildingSst, checkpoint_ctx.get_state());

        // update_l1_flushed_info
        let l1_file_metas = l0_file_metas;
        let dup_entries = vec![DuplicateEntry {
            key: "test_duplicated_key".to_string(),
            values: vec![],
        }];
        store
            .update_l1_flushed_info(0, l1_file_metas.clone(), dup_entries.clone())
            .unwrap();
        let mut checkpoint_ctx = store.load_checkpoint_ctx();
        let worker_ctx = checkpoint_ctx.get_kvpairs_worker_ctx(0);
        assert_eq!(l1_file_metas, worker_ctx.l1_file_metas);
        assert_eq!(dup_entries, worker_ctx.duplicated_entries);

        // update_sst_meta
        let _ = store.checkpoint_ctx.get_building_worker_ctx(0);
        let sst_metas = vec![SstMeta {
            id: 1,
            smallest: vec![1, 2, 3],
            biggest: vec![4, 5, 6],
            size: 3,
            meta_offset: 0,
            uncompressed_size: 3,
            keys: 3,
        }];
        store
            .update_sst_metas(0, sst_metas.clone(), dup_entries.clone())
            .unwrap();
        let mut checkpoint_ctx = store.load_checkpoint_ctx();
        let worker_ctx = checkpoint_ctx.get_building_worker_ctx(0);
        assert_eq!(sst_metas, worker_ctx.sst_metas);
        assert_eq!(dup_entries, worker_ctx.duplicated_entries);

        // set_worker_ingested
        store.set_worker_ingested(0).unwrap();
        let mut checkpoint_ctx = store.load_checkpoint_ctx();
        let worker_ctx = checkpoint_ctx.get_building_worker_ctx(0);
        assert!(worker_ctx.ingested);

        // set_ingested
        store.set_ingested(dup_entries.clone()).unwrap();
        let checkpoint_ctx = store.load_checkpoint_ctx();
        assert_eq!(LoadDataWorkerState::IngestedSst, checkpoint_ctx.get_state());
        assert_eq!(dup_entries, checkpoint_ctx.get_duplicated_entries());
    }

    #[test]
    fn test_load_data_worker_state() {
        let mut state = LoadDataWorkerState::InitTask;
        let is_succ = state.transition(LoadDataWorkerState::InitTask);
        assert_eq!(true, is_succ);

        let is_succ = state.transition(LoadDataWorkerState::AddingChunks);
        assert_eq!(true, is_succ);

        let is_succ = state.transition(LoadDataWorkerState::BuildingSst);
        assert_eq!(true, is_succ);

        let is_succ = state.transition(LoadDataWorkerState::IngestingSst);
        assert_eq!(true, is_succ);

        let is_succ = state.transition(LoadDataWorkerState::IngestedSst);
        assert_eq!(true, is_succ);

        let is_succ = state.transition(LoadDataWorkerState::IngestedSst);
        assert_eq!(true, is_succ);

        let is_succ = state.transition(LoadDataWorkerState::IngestingSst);
        assert_eq!(false, is_succ);
    }

    #[test]
    fn test_checkpoint_default() {
        let _ = LocalFileCheckpointStorage::binary_to_checkpoint("{}");
    }

    #[test]
    fn test_cleanup_worker() {
        let running_tasks: Arc<DashMap<String, LoadTaskScheduler>> = Arc::new(DashMap::new());
        let checkpoint_dir = TempDir::new().unwrap();
        let path = checkpoint_dir.path().to_owned();

        // canceled task
        let task_id = "task_id1".to_string();
        let (scheduler1, receiver1) = make_test_scheduler(task_id.clone(), path.clone());
        scheduler1.cancel("cancel for test".to_string());
        running_tasks.insert(task_id, scheduler1);

        // idle task
        let task_id = "task_id2".to_string();
        let (scheduler2, receiver2) = make_test_scheduler(task_id.clone(), path.clone());
        running_tasks.insert(task_id, scheduler2.clone());

        // normal task
        let task_id = "task_id3".to_string();
        let (scheduler3, receiver3) = make_test_scheduler(task_id.clone(), path.clone());
        running_tasks.insert(task_id, scheduler3.clone());

        // normal finished task
        let task_id = "task_id4".to_string();
        let (scheduler4, receiver4) = make_test_scheduler(task_id.clone(), path);
        scheduler4.states.write().unwrap().finished = true;
        scheduler4.cancel("finished".to_string());
        running_tasks.insert(task_id.to_owned(), scheduler4.clone());

        let mut cleanup_worker = LoadDataCleanupWorker::new(running_tasks.clone(), 1, 10, 5, 10);
        std::thread::spawn(move || {
            cleanup_worker.run();
        });

        for i in 0..10 {
            let mut states = scheduler3.states.write().unwrap();
            states.flushed_files += i;
            drop(states);
            std::thread::sleep(Duration::from_secs(2));
        } // takes 20s = 10 * 2s
        receiver1.try_recv().unwrap();
        receiver2.try_recv().unwrap();
        let msg = receiver3.try_recv();
        assert!(msg.is_err());
        receiver4.try_recv().unwrap();

        assert!(scheduler2.states.read().unwrap().canceled);
        assert!(!scheduler3.states.read().unwrap().canceled);
        assert!(running_tasks.len() == 1);
    }

    fn make_test_scheduler(
        task_id: String,
        path: PathBuf,
    ) -> (LoadTaskScheduler, Receiver<LoadTaskMsg>) {
        let checkpoint_store = make_test_checkpoint_storage(path.to_owned(), task_id.clone());
        let (sender, receiver) = tikv_util::mpsc::unbounded();
        let thread_handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(60));
        });
        let scheduler = LoadTaskScheduler {
            sender,
            states: Arc::new(RwLock::new(LoadTaskStates::default())),
            thread_handle: Some(Arc::new(Mutex::new(thread_handle))),
            checkpoint_store: Arc::new(Mutex::new(checkpoint_store)),
            io_runtime: Arc::new(tokio::runtime::Runtime::new().unwrap()),
        };
        let mut states = scheduler.states.write().unwrap();
        states.task_id = task_id;
        drop(states);
        (scheduler, receiver)
    }

    fn make_test_checkpoint_storage(
        checkpoint_dir: PathBuf,
        task_id: String,
    ) -> LocalFileCheckpointStorage {
        let task_ctx = TaskContext {
            task_id,
            start_ts: 1_u64,
            commit_ts: 1_u64,
            inner_key_off: None,
            outer_key_prefix: vec![],
            encryption_key: None,
            keyspace_id: None,
        };

        let checkpoint = LoadDataCheckpointCtx::new(task_ctx);
        let mut store = LocalFileCheckpointStorage::new(checkpoint, checkpoint_dir).unwrap();
        store.flush_checkpoint_ctx().unwrap();
        store
    }
}
