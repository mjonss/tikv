// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, fs, path::PathBuf, sync::Arc, time::Duration};

use bytes::Bytes;
use cloud_encryption::MasterKey;
use dashmap::{DashMap, mapref::entry::Entry};
use futures::executor::block_on;
use http::{Method, Response, StatusCode, header, request::Parts};
use hyper::Body;
use kvengine::{
    dfs,
    table::{LZ4_COMPRESSION, NO_COMPRESSION, ZSTD_COMPRESSION},
};
use load_data::{
    checkpoint,
    checkpoint::{
        CANCELLED_TASK_EXPIRE_SEC, CLEANUP_INTERVAL_SEC, FINISHED_TASK_EXPIRE_SEC,
        IDLE_TASK_EXPIRE_SEC, LoadDataCheckpointCtx, LoadDataCleanupWorker,
        LoadDataWorkerState::{BuildingSst, IngestedSst},
        LocalFileCheckpointStorage,
    },
    dispatcher::Dispatcher,
    task::{
        FlushResult, FlushStates, LoadDataConfig, LoadDataContext, LoadTaskMsg, LoadTaskScheduler,
        LoadTaskStates, PutChunkResult, TaskContext,
    },
};
use pd_client::PdClient;
use tikv_util::{debug, error, info};

use crate::{
    common::{get_param, make_response},
    worker_scaler::{WorkerScaler, WorkerScalerConfig},
};

/// Remote load data worker API:
///
/// 1. init task: POST
///    /load_data?cluster_id=%d&task_id=%s&start_ts=%d&commit_ts=%d
///
/// 2. put chunk: PUT
///    /load_data?cluster_id=%d&task_id=%s&writer_id=%d&chunk_id=%d key_len(2) +
///    key(key_len) + val_len(4) + value(val_len) + row_id_len(2) +
///    row_id(row_id_len) key_len(2) + key(key_len) + val_len(4) +
///    value(val_len) + row_id_len(2) + row_id(row_id_len) ...
///
/// 3. flush: POST /load_data?cluster_id=%d&task_id=%s&flush=true
///
/// 4. build task: POST
///    /load_data?cluster_id=%d&task_id=%s&build=true&compression=zstd&
///    split_size=%d&split_keys=%d
///
/// 5. get task states: GET /load_data?cluster_id=%d&task_id=%s {"canceled":
///    false, "finished": false, "error": "", "created-files": 10,
///    "ingested-regions": 3}
///
/// 6. clean up task: DELETE /load_data?cluster_id=%d&task_id=%s
pub(crate) async fn handle_load_data(
    manager: Arc<LoadDataManager>,
    parts: Parts,
    body: Bytes,
) -> hyper::Result<hyper::Response<hyper::Body>> {
    let query = parts.uri.query().unwrap_or("");
    let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
    let cluster_id = get_param::<u64>(&query_pairs, "cluster_id").unwrap_or_default();
    let pd_cluster_id = manager.ctx.pd.get_cluster_id().unwrap();
    if cluster_id != pd_cluster_id {
        return Ok(make_response(
            StatusCode::BAD_REQUEST,
            format!(
                "cluster id mismatch, got {}, expected {}",
                cluster_id, pd_cluster_id
            ),
        ));
    }
    let task_id = query_pairs
        .get("task_id")
        .map(|x| x.to_string())
        .unwrap_or_default();
    if task_id.is_empty() {
        if parts.method == Method::GET {
            let tasks = manager.list_tasks();
            let json = serde_json::to_string(&tasks).unwrap();
            return Ok(Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(json.into())
                .unwrap());
        }
        if parts.method != Method::DELETE {
            return Ok(make_response(StatusCode::BAD_REQUEST, "task_id is missing"));
        }
    }
    match parts.method {
        Method::GET => {
            if let Some(states) = manager.get_task_states(&task_id) {
                let json = serde_json::to_string(&states).unwrap();
                return Ok(Response::builder()
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(json.into())
                    .unwrap());
            }
            if manager.worker_scaler.is_some() {
                let worker_scaler = manager.worker_scaler.as_ref().unwrap();
                let worker_pod_addr = worker_scaler.get_worker_addr_by_task_id(&task_id).await;
                if let Some(addr) = worker_pod_addr {
                    info!("Get worker addr from cache: {}", addr);
                    let resp = Response::builder()
                        .header("Location", addr)
                        .status(StatusCode::FOUND)
                        .body(Body::empty())
                        .unwrap();
                    return Ok(resp);
                }
            }
            Ok(make_response(StatusCode::NOT_FOUND, ""))
        }
        Method::POST => {
            if query_pairs.get("build").map(|x| x.as_ref()) == Some("true") {
                if !manager.has_task(&task_id) {
                    Ok(make_response(StatusCode::NOT_FOUND, ""))
                } else {
                    let compression = query_pairs
                        .get("compression")
                        .map(|x| x.to_string())
                        .unwrap_or_default();
                    manager.build(&task_id, &compression).await;
                    Ok(make_response(StatusCode::OK, ""))
                }
            } else if query_pairs.get("flush").map(|x| x.as_ref()) == Some("true") {
                if !manager.has_task(&task_id) {
                    Ok(make_response(StatusCode::NOT_FOUND, ""))
                } else {
                    let writer_id = get_param::<u64>(&query_pairs, "writer_id");
                    if writer_id.is_none() {
                        return Ok(make_response(
                            StatusCode::BAD_REQUEST,
                            "writer id is missing",
                        ));
                    }
                    let flush_res = manager.flush(&task_id, writer_id.unwrap()).await;
                    let json = serde_json::to_string(&flush_res).unwrap();
                    Ok(Response::builder()
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(json.into())
                        .unwrap())
                }
            } else if manager.has_task(&task_id) {
                Ok(make_response(StatusCode::BAD_REQUEST, "task exists"))
            } else {
                let start_ts = get_param::<u64>(&query_pairs, "start_ts").unwrap_or_default();
                let commit_ts = get_param::<u64>(&query_pairs, "commit_ts").unwrap_or_default();
                let data_size = get_param::<u64>(&query_pairs, "data_size").unwrap_or_default();
                if data_size > manager.worker_scaler_conf.max_size.0 {
                    return Ok(make_response(
                        StatusCode::BAD_REQUEST,
                        format!(
                            "data size {} exceeds max data size {}",
                            data_size, manager.worker_scaler_conf.max_size.0
                        ),
                    ));
                }
                info!("{} init task with data size {}", task_id, data_size);

                if manager.worker_scaler.is_some() {
                    let worker_scaler = manager.worker_scaler.as_ref().unwrap();
                    let worker_addr = worker_scaler.get_worker_addr_by_task_id(&task_id).await;
                    if let Some(addr) = worker_addr {
                        info!("{} get worker addr from cache: {}", task_id, addr);
                        let resp = Response::builder()
                            .header("Location", addr)
                            .status(StatusCode::FOUND)
                            .body(Body::empty())
                            .unwrap();
                        return Ok(resp);
                    }
                }

                let spawn_load_data_worker = manager.worker_scaler.is_some()
                    && (data_size > manager.worker_scaler_conf.spawn_data_size.0
                        || (manager.running_tasks.len()
                            > manager.worker_scaler_conf.spawn_running_tasks
                            && data_size != 0));
                info!("current running tasks {}", manager.running_tasks.len());
                if spawn_load_data_worker {
                    let worker_scaler = manager.worker_scaler.as_ref().unwrap();
                    let data_size_gb = data_size / 1024 / 1024 / 1024;
                    let res = worker_scaler
                        .create_worker(&task_id, data_size_gb as usize)
                        .await;
                    match res {
                        Ok(()) => {
                            let worker_addr = worker_scaler
                                .get_worker_addr_by_task_id(&task_id)
                                .await
                                .unwrap();
                            info!("{} get worker addr: {}", task_id, worker_addr);
                            let resp = Response::builder()
                                .header("Location", worker_addr)
                                .status(StatusCode::FOUND)
                                .body(Body::empty())
                                .unwrap();
                            return Ok(resp);
                        }
                        Err(err) => {
                            return Ok(make_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("{:?}", err),
                            ));
                        }
                    }
                }
                let task_ctx = TaskContext {
                    task_id: task_id.clone(),
                    start_ts,
                    commit_ts,
                    inner_key_off: None,
                    outer_key_prefix: vec![],
                    encryption_key: None,
                    keyspace_id: None,
                };
                // step 1: on start, client call init task
                manager.init_task(task_ctx);
                info!("{} init task successfully", task_id);
                Ok(make_response(StatusCode::OK, ""))
            }
        }
        Method::PUT => {
            if !manager.has_task(&task_id) {
                return Ok(make_response(StatusCode::BAD_REQUEST, "task not found"));
            }
            let chunk_id = get_param::<u64>(&query_pairs, "chunk_id");
            if chunk_id.is_none() {
                return Ok(make_response(
                    StatusCode::BAD_REQUEST,
                    "chunk id is missing",
                ));
            }
            let writer_id = get_param::<u64>(&query_pairs, "writer_id");
            if writer_id.is_none() {
                return Ok(make_response(
                    StatusCode::BAD_REQUEST,
                    "writer id is missing",
                ));
            }
            let put_chunk_res = manager
                .put_chunk(&task_id, writer_id.unwrap(), chunk_id.unwrap(), body)
                .await;

            let json = serde_json::to_string(&put_chunk_res).unwrap();
            Ok(Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(json.into())
                .unwrap())
        }
        Method::DELETE => {
            let task_id_prefix =
                get_param::<String>(&query_pairs, "task_id_prefix").unwrap_or_default();
            // Handle bulk delete by task_id_prefix
            if !task_id_prefix.is_empty() {
                manager
                    .delete_by_task_id_prefix(task_id_prefix.as_str())
                    .await;
                return Ok(make_response(StatusCode::OK, ""));
            }

            // For single task deletion, validate task_id
            if task_id.is_empty() {
                return Ok(make_response(
                    StatusCode::BAD_REQUEST,
                    "task_id is required",
                ));
            }
            if !manager.has_task(&task_id) {
                Ok(make_response(StatusCode::NOT_FOUND, ""))
            } else {
                // step 4: on finish, client call DELETE task
                manager.delete(&task_id);
                Ok(make_response(StatusCode::OK, ""))
            }
        }
        _ => Ok(make_response(StatusCode::BAD_REQUEST, "invalid method")),
    }
}

pub(crate) struct LoadDataManager {
    running_tasks: Arc<DashMap<String, LoadTaskScheduler>>,
    config: LoadDataConfig,
    ctx: LoadDataContext,
    worker_scaler: Option<WorkerScaler>,
    worker_scaler_conf: WorkerScalerConfig,
}

impl LoadDataManager {
    pub(crate) fn new(
        pd: Arc<dyn PdClient>,
        dir: PathBuf,
        dfs: Arc<dyn dfs::Dfs>,
        runtime: Arc<tokio::runtime::Runtime>,
        master_key: MasterKey,
        worker_scaler: Option<WorkerScaler>,
        worker_scaler_conf: WorkerScalerConfig,
        config: LoadDataConfig,
    ) -> Self {
        let context = LoadDataContext {
            pd,
            dir,
            dfs,
            runtime,
            master_key,
        };
        Self {
            running_tasks: Arc::new(DashMap::default()),
            config,
            ctx: context,
            worker_scaler,
            worker_scaler_conf,
        }
    }

    fn recover_task_by_checkpoint_file(&self, path: PathBuf) {
        let file_data = LocalFileCheckpointStorage::read_file(path);
        let mut checkpoint_ctx =
            LocalFileCheckpointStorage::binary_to_checkpoint(file_data.as_str());
        checkpoint_ctx.set_is_recover(true);
        debug!(
            "[checkpoint] try recover task from checkpoint, file exists {}",
            file_data
        );

        self.exec_task_by_checkpoint(checkpoint_ctx.clone());

        if checkpoint_ctx.get_state() >= BuildingSst
            && checkpoint_ctx.get_state() < IngestedSst
            && !checkpoint_ctx.canceled
        {
            block_on(self.build(
                checkpoint_ctx.get_task_id().as_str(),
                LoadDataManager::compression_num_to_str(checkpoint_ctx.clone().get_compression()),
            ));
        }
    }

    pub fn try_recover_or_clean_tasks_by_checkpoint(&self) {
        if !self.config.enable_checkpoint {
            return;
        }

        let mut checkpoint_dir = self.ctx.dir.clone();
        if checkpoint_dir.as_os_str().is_empty() {
            checkpoint_dir = PathBuf::from(".");
        }

        let mut cleanup_worker = LoadDataCleanupWorker::new(
            self.running_tasks.clone(),
            CLEANUP_INTERVAL_SEC,
            CANCELLED_TASK_EXPIRE_SEC,
            FINISHED_TASK_EXPIRE_SEC,
            IDLE_TASK_EXPIRE_SEC,
        );
        std::thread::spawn(move || {
            cleanup_worker.run();
        });

        // recover tasks from checkpoint files
        let files = fs::read_dir(checkpoint_dir).unwrap();
        let dir_entries: Vec<fs::DirEntry> = files.filter_map(|r| r.ok()).collect();
        for file in dir_entries {
            let file_name = file.file_name();
            let str_file_name = file_name.to_string_lossy();

            if str_file_name.starts_with(checkpoint::CHECKPOINT_WORKER_PREFIX) {
                let path = file.path();
                if str_file_name.ends_with(checkpoint::CHECKPOINT_TMP_FILE_SUFFIX) {
                    if let Err(err) = fs::remove_file(path) {
                        error!("failed to remove checkpoint tmp file: {}", err);
                    }
                } else {
                    self.recover_task_by_checkpoint_file(path.clone());
                }
            }
        }
    }

    pub(crate) fn get_task_states(&self, task_id: &str) -> Option<LoadTaskStates> {
        self.running_tasks.get(task_id).map(|x| {
            x.check_task_thread_finished();
            x.states.read().unwrap().clone()
        })
    }

    pub(crate) fn list_tasks(&self) -> Vec<LoadTaskStates> {
        self.running_tasks
            .iter()
            .map(|x| {
                x.check_task_thread_finished();
                x.states.read().unwrap().clone()
            })
            .collect()
    }

    pub(crate) fn has_task(&self, task_id: &str) -> bool {
        self.running_tasks.contains_key(task_id)
    }

    pub(crate) fn exec_task_by_checkpoint(&self, checkpoint_ctx: LoadDataCheckpointCtx) {
        let task_id = checkpoint_ctx.get_task_id();
        let task_ctx = TaskContext {
            task_id: task_id.clone(),
            start_ts: checkpoint_ctx.get_start_ts(),
            commit_ts: checkpoint_ctx.get_commit_ts(),
            inner_key_off: None,
            outer_key_prefix: vec![],
            encryption_key: None,
            keyspace_id: None,
        };

        let mut dispatcher = Dispatcher::new(
            self.config.clone(),
            self.ctx.clone(),
            task_ctx,
            checkpoint_ctx,
        );
        let mut scheduler = dispatcher.get_scheduler();
        let thread_handle = std::thread::spawn(move || {
            dispatcher.run();
        });

        scheduler.set_thread_handle(thread_handle);
        self.running_tasks.insert(task_id, scheduler);
    }

    pub(crate) fn init_task(&self, task_ctx: TaskContext) {
        let task_id = task_ctx.task_id.clone();
        match self.running_tasks.entry(task_id.clone()) {
            Entry::Occupied(_) => {
                info!("task {} already exists", task_id);
            }
            Entry::Vacant(entry) => {
                let checkpoint_ctx = LoadDataCheckpointCtx::new(task_ctx.clone());
                let mut dispatcher = Dispatcher::new(
                    self.config.clone(),
                    self.ctx.clone(),
                    task_ctx,
                    checkpoint_ctx,
                );
                let mut scheduler = dispatcher.get_scheduler();
                let thread_handle = std::thread::spawn(move || {
                    dispatcher.run();
                });

                scheduler.set_thread_handle(thread_handle);
                entry.insert(scheduler);
            }
        }
    }

    fn compression_num_to_str(compression_type: u8) -> &'static str {
        if compression_type == LZ4_COMPRESSION {
            return "lz4";
        } else if compression_type == ZSTD_COMPRESSION {
            return "zstd";
        }
        ""
    }

    pub(crate) async fn build(&self, task_id: &str, compression: &str) {
        let compression_type = match compression {
            "lz4" => LZ4_COMPRESSION,
            "zstd" => ZSTD_COMPRESSION,
            _ => NO_COMPRESSION,
        };
        let (cb, fut) = tikv_util::future::paired_future_callback();
        let scheduler = self.running_tasks.get(task_id).unwrap().clone();
        scheduler
            .sender
            .send(LoadTaskMsg::Build {
                compression_type,
                cb,
            })
            .unwrap();
        fut.await.unwrap();
    }

    pub(crate) async fn flush(&self, task_id: &str, writer_id: u64) -> FlushResult {
        let scheduler = self.running_tasks.get(task_id).unwrap().clone();

        let interval = Duration::from_secs(3);
        let mut ticker = tokio::time::interval(interval);
        let mut file_count: Option<usize> = None;
        loop {
            let (cb, fut) = tikv_util::future::paired_future_callback();
            scheduler
                .sender
                .send(LoadTaskMsg::Flush {
                    writer_id,
                    flush_file_count: file_count,
                    cb,
                })
                .unwrap();
            let flush_states = fut.await.unwrap();
            match flush_states {
                FlushStates::FlushFileCount { flush_file_count } => {
                    file_count = Some(flush_file_count);
                    ticker.tick().await;
                }
                FlushStates::FlushResult { flush_result } => {
                    return flush_result;
                }
            }
        }
    }

    pub(crate) async fn put_chunk(
        &self,
        task_id: &str,
        writer_id: u64,
        chunk_id: u64,
        chunk_data: Bytes,
    ) -> PutChunkResult {
        let scheduler = self.running_tasks.get(task_id).unwrap().clone();
        let (cb, fut) = tikv_util::future::paired_future_callback();
        scheduler
            .sender
            .send(LoadTaskMsg::AddChunk {
                writer_id,
                chunk_id,
                chunk_data,
                cb,
            })
            .unwrap();
        fut.await.unwrap()
    }

    pub(crate) fn delete(&self, task_id: &str) {
        if let Some(scheduler) = self.running_tasks.get(task_id) {
            scheduler.cancel("deleted".to_string());
        }
    }

    pub(crate) async fn delete_by_task_id_prefix(&self, task_id_prefix: &str) {
        info!("Deleting tasks with prefix: {}", task_id_prefix);
        self.running_tasks
            .iter()
            .filter(|entry| entry.key().starts_with(task_id_prefix))
            .for_each(|entry| {
                info!("{} task is being cleaned up", entry.key());
                entry.value().cancel("deleted".to_string());
            });

        // clean up task in scaler
        if let Some(scaler) = &self.worker_scaler {
            scaler.delete_by_task_id_prefix(task_id_prefix).await;
        }
    }
}
