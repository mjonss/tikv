// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    cmp::{Ordering, max, min},
    collections::HashMap,
    fs,
    fs::File,
    io::Write,
    mem,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use api_version::api_v2::KEYSPACE_PREFIX_LEN;
use bytes::{Buf, BufMut, Bytes};
use encryption::{DecrypterReader, EncrypterWriter, Iv};
use futures::executor;
use http::Request;
use hyper::Body;
use keys::next_key;
use kvengine::{
    IdVer, ShardTag, UserMeta, WRITE_CF, WRITE_CF_BOTTOM_LEVEL,
    dfs::Options,
    table::{InnerKey, Value, sstable::Builder},
    table_id::encode_table_prefix_key,
    util::new_table_create_pb,
};
use kvproto::{encryptionpb::EncryptionMethod, metapb, pdpb};
use pd_client::PdClient;
use protobuf::Message;
use rfengine::compress_lz4;
use rfstore::store::{raw_end_key, raw_start_key};
use tidb_query_datatype::codec::table::{
    ID_LEN, INDEX_PREFIX_SEP, RECORD_PREFIX_SEP, TABLE_PREFIX_KEY_LEN, decode_table_id,
};
use tikv_util::{
    box_err,
    codec::bytes::{decode_bytes, encode_bytes},
    error, info,
    merge_range::MergeRanges,
    mpsc::{Receiver, Sender},
    time::Instant,
    warn,
};
use tokio::runtime;

use crate::{
    checkpoint::{FileMeta, LocalFileCheckpointStorage},
    error::{Error, Result},
    kv::{DuplicateEntry, KvPair, KvPairsReader, MergeIterator, SstMeta},
    metrics::{
        LOAD_DATA_INGEST_RANEG_GROUP_FAILURES_COUNTER, LOAD_DATA_INGEST_SST_FILE_COUNTER,
        LOAD_DATA_SPLIT_REGION_FAILURES_COUNTER, LOAD_DATA_WRU_COST_COUNTER,
    },
    task::{
        FlushResult, FlushStates, LoadDataConfig, LoadDataContext, LoadTaskScheduler,
        PutChunkResult, TaskContext,
    },
};

pub const ZSTD_COMPRESSION_LEVEL: i32 = 3;

pub const ALLOCATE_ID_TIMEOUT: Duration = Duration::from_secs(10 * 60);
pub const RETRY_SLEEP_DURATION: Duration = Duration::from_secs(1);
pub const MAX_RETRY_TIMES: usize = 1000;
pub const MAX_SLEEP_DURATION: Duration = Duration::from_secs(30);

// the following constants are used to calculate RU consumption
pub const DEFAULT_AVG_BATCH_PROPORTION: f64 = 0.5;
pub const REPLICA_NUMS: f64 = 3.0;
pub const TXN_FILE_RU_DISCOUNT_RATIO: f64 = 0.0625;
pub const FILE_COUNTING_STEP: usize = 100;

pub enum KvPairsWorkerMsg {
    AddChunk {
        writer_id: u64,
        chunk_id: u64,
        chunk_data: Bytes,
        cb: Box<dyn FnOnce(PutChunkResult) + Send>,
    },
    Flush {
        #[allow(dead_code)]
        writer_id: u64,
        flush_file_count: Option<usize>,
        cb: Box<dyn FnOnce(FlushStates) + Send>,
    },
    CollectFileMetas {
        skip_sort: bool,
        cb: Box<dyn FnOnce((Vec<FileMeta>, Vec<DuplicateEntry>, Vec<u8>)) + Send>,
    },
    Cleanup,
}

pub struct UnhandledFlushFile {
    pub handled_chunk_ids: HashMap<u64 /* writer_id */, u64 /* chunk_id */>,
    pub file_idx: usize,
    pub key_comm_prefix: Vec<u8>,
    pub file_meta: FileMeta,
}

pub struct KvPairsWorker {
    worker_id: u64,
    config: LoadDataConfig,
    task_ctx: TaskContext,
    l0_data_dir: PathBuf,
    l1_data_dir: PathBuf,
    flush_file_concurrency: usize,

    kv_pairs: Vec<KvPair>,
    in_mem_size: usize,
    l1_file_idx: usize,
    flush_file_errs: Vec<Error>,
    unhandled_flush_files: Vec<UnhandledFlushFile>,

    scheduler: LoadTaskScheduler,
    receiver: Receiver<KvPairsWorkerMsg>,
    file_tx: Sender<Result<UnhandledFlushFile>>,
    file_rx: Receiver<Result<UnhandledFlushFile>>,
    checkpoint: Arc<Mutex<LocalFileCheckpointStorage>>,

    // the following fields need to be persisted
    l0_file_idx: usize,
    l0_file_metas: Vec<FileMeta>,
    l1_file_metas: Vec<FileMeta>,
    dup_entries: Vec<DuplicateEntry>,
    key_comm_prefix: Vec<u8>,
    handled_chunk_ids: HashMap<u64, u64>,
    flushed_chunk_ids: HashMap<u64, u64>,
}

impl KvPairsWorker {
    pub fn new(
        worker_id: u64,
        config: LoadDataConfig,
        task_ctx: TaskContext,
        task_dir: PathBuf,
        flush_file_concurrency: usize,
        receiver: Receiver<KvPairsWorkerMsg>,
        scheduler: LoadTaskScheduler,
        checkpoint: Arc<Mutex<LocalFileCheckpointStorage>>,
    ) -> Self {
        let mut checkpoint_guard = checkpoint.lock().unwrap();
        let worker_ctx = checkpoint_guard
            .checkpoint_ctx
            .get_kvpairs_worker_ctx(worker_id);

        let l0_file_metas = worker_ctx.l0_file_metas.clone();
        let l0_file_idx = l0_file_metas.len();
        let handled_chunk_ids = worker_ctx.flushed_chunk_ids.clone();
        let flushed_chunk_ids = worker_ctx.flushed_chunk_ids.clone();
        let l1_file_metas = worker_ctx.l1_file_metas.clone();
        let dup_entries = worker_ctx.duplicated_entries.clone();

        let mut key_comm_prefix = worker_ctx.key_comm_prefix.clone();
        if key_comm_prefix.is_empty() {
            let first_key = checkpoint_guard.checkpoint_ctx.get_first_key();
            key_comm_prefix = first_key.slice(task_ctx.inner_key_off.unwrap()..).to_vec();
        }
        drop(checkpoint_guard);

        let (file_tx, file_rx) = tikv_util::mpsc::unbounded();
        let l0_data_dir = task_dir
            .join(format!("worker-{}", worker_id))
            .join("l0-files");
        let l1_data_dir = task_dir
            .join(format!("worker-{}", worker_id))
            .join("l1-files");

        Self {
            worker_id,
            config,
            task_ctx,
            l0_data_dir,
            l1_data_dir,
            kv_pairs: vec![],
            in_mem_size: 0,
            l0_file_idx,
            l1_file_idx: 0,
            l0_file_metas,
            l1_file_metas,
            flush_file_errs: vec![],
            flush_file_concurrency,
            scheduler,
            receiver,
            unhandled_flush_files: vec![],
            file_tx,
            file_rx,
            checkpoint,
            key_comm_prefix,
            handled_chunk_ids,
            flushed_chunk_ids,
            dup_entries,
        }
    }

    pub fn run(&mut self) {
        self.init_task_dir();
        info!(
            "{} run kvpairs worker-{}, key comm prefix: {:?}, l0 file idx: {}, l0 file metas: {}, l1 file metas: {}, duplicated entries: {}",
            self.task_ctx.task_id,
            self.worker_id,
            self.key_comm_prefix,
            self.l0_file_idx,
            self.l0_file_metas.len(),
            self.l1_file_metas.len(),
            self.dup_entries.len(),
        );

        while let Ok(msg) = self.receiver.recv() {
            match msg {
                KvPairsWorkerMsg::AddChunk {
                    writer_id,
                    chunk_id,
                    chunk_data,
                    cb,
                } => {
                    if let Err(err) = self.handle_add_chunk(writer_id, chunk_id, chunk_data, cb) {
                        error!(
                            "{} worker-{} failed to handle add chunk, error: {:?}",
                            self.task_ctx.task_id, self.worker_id, err
                        );
                        self.scheduler.cancel(format!(
                            "{} worker-{} error: {:?}",
                            self.task_ctx.task_id, self.worker_id, err
                        ));
                        let recv_count = self.l0_file_idx
                            - self.flush_file_errs.len()
                            - self.l0_file_metas.len()
                            - self.unhandled_flush_files.len();
                        if recv_count > 0 {
                            let _ = self.recv_flush_files(recv_count);
                        }
                    }
                }

                KvPairsWorkerMsg::Flush {
                    writer_id: _,
                    flush_file_count,
                    cb,
                } => {
                    self.handle_flush_msg(flush_file_count, cb);
                }

                KvPairsWorkerMsg::CollectFileMetas { skip_sort, cb } => {
                    match self.collect_file_metas(skip_sort) {
                        Ok((file_metas, dup_entries)) => {
                            cb((file_metas, dup_entries, self.key_comm_prefix.clone()));
                        }
                        Err(err) => {
                            error!(
                                "{} worker-{} failed to collect file metas, error: {:?}",
                                self.task_ctx.task_id, self.worker_id, err
                            );
                            self.scheduler.cancel(format!(
                                "{} worker-{} error: {:?}",
                                self.task_ctx.task_id, self.worker_id, err
                            ));
                            cb((vec![], vec![], vec![]));
                        }
                    }
                }
                KvPairsWorkerMsg::Cleanup => {
                    return;
                }
            }
        }
    }

    fn init_task_dir(&mut self) {
        if !self.l0_data_dir.is_dir() {
            if let Err(err) = fs::create_dir_all(&self.l0_data_dir) {
                error!(
                    "{} worker-{} failed to create dir {:?}, error {:?}",
                    self.task_ctx.task_id, self.worker_id, self.l0_data_dir, err
                );
                self.scheduler.cancel(format!(
                    "{} worker-{} error: {:?}",
                    self.task_ctx.task_id, self.worker_id, err
                ));
                return;
            }
        }

        // If worker restarts during the building l1 files phase, should clean
        // up the previously created files after restarting.
        if self.l1_data_dir.is_dir() && self.l1_file_metas.is_empty() {
            if let Err(err) = fs::remove_dir_all(&self.l1_data_dir) {
                error!(
                    "{} worker-{} failed to remove l1 data dir, {:?}",
                    self.task_ctx.task_id, self.worker_id, err,
                );
                self.scheduler.cancel(format!(
                    "{} worker-{} error: {:?}",
                    self.task_ctx.task_id, self.worker_id, err
                ));
                return;
            }
        }

        if let Err(err) = fs::create_dir_all(&self.l1_data_dir) {
            error!(
                "{} worker-{} failed to create dir {:?}, error {:?}",
                self.task_ctx.task_id, self.worker_id, self.l1_data_dir, err
            );
            self.scheduler.cancel(format!(
                "{} worker-{} error: {:?}",
                self.task_ctx.task_id, self.worker_id, err
            ));
        }
    }

    fn handle_add_chunk(
        &mut self,
        writer_id: u64,
        chunk_id: u64,
        chunk_data: Bytes,
        cb: Box<dyn FnOnce(PutChunkResult) + Send>,
    ) -> Result<()> {
        let handled_chunk_id = self.get_handled_chunk_id(writer_id);
        let flushed_chunk_id = self.get_flushed_chunk_id(writer_id);
        let mut result = PutChunkResult {
            handled_chunk_id,
            flushed_chunk_id,
            canceled: self.scheduler.is_canceled(),
            finished: self.scheduler.is_finished(),
            error: self.scheduler.error_msg(),
        };

        if result.canceled || result.finished {
            warn!(
                "{} worker-{} skip chunk {}, canceled: {}, finished: {}, error message: {}",
                self.task_ctx.task_id,
                self.worker_id,
                chunk_id,
                result.canceled,
                result.finished,
                result.error,
            );
            cb(result);
            return Err(Error::Canceled);
        }

        if chunk_data.is_empty() {
            warn!(
                "{} worker-{} skip chunk, chunk {} is empty for writer {}",
                self.task_ctx.task_id, self.worker_id, chunk_id, writer_id
            );
            cb(result);
            return Ok(());
        }

        if handled_chunk_id != chunk_id - 1 {
            warn!(
                "{} worker-{} skip chunk {} for writer {}, expect chunk {}",
                self.task_ctx.task_id,
                self.worker_id,
                chunk_id,
                writer_id,
                handled_chunk_id + 1
            );
            result.error = format!(
                "skip chunk {} for writer {}, expect chunk {}",
                chunk_id,
                writer_id,
                handled_chunk_id + 1
            );
            cb(result);
            return Ok(());
        }

        info!(
            "{} worker-{} handle add chunk {} with len {} for writer {}",
            self.task_ctx.task_id,
            self.worker_id,
            chunk_id,
            chunk_data.len(),
            writer_id,
        );

        self.handled_chunk_ids.insert(writer_id, chunk_id);
        result.handled_chunk_id = chunk_id;
        // call `cb` early to avoid waiting for flush
        cb(result);

        let inner_key_off = self.task_ctx.inner_key_off.unwrap();
        let mut offset = 0;
        while offset < chunk_data.len() {
            let key_len = (&chunk_data[offset..]).get_u16_le();
            offset += 2;
            let key = chunk_data.slice(offset..offset + key_len as usize);
            offset += key_len as usize;
            let val_len = (&chunk_data[offset..]).get_u32_le();
            offset += 4;
            let val = chunk_data.slice(offset..offset + val_len as usize);
            offset += val_len as usize;
            let row_id_len = (&chunk_data[offset..]).get_u16_le();
            offset += 2;
            let row_id = chunk_data.slice(offset..offset + row_id_len as usize);
            offset += row_id_len as usize;

            let outer_key_prefix = key.slice(..inner_key_off);
            if outer_key_prefix.chunk() != self.task_ctx.outer_key_prefix.as_slice() {
                let err_msg = format!(
                    "key prefix inconsistent, first key: {:?}, current key: {:?}",
                    self.task_ctx.outer_key_prefix,
                    outer_key_prefix.chunk()
                );
                error!(
                    "{} worker-{} {}",
                    self.task_ctx.task_id, self.worker_id, err_msg
                );
                return Err(Error::CheckError(err_msg));
            }

            let inner_key = key.slice(inner_key_off..);
            self.kv_pairs.push(KvPair::new(inner_key, val, row_id));
            self.in_mem_size += 2 + key_len as usize + 4 + val_len as usize;
        }

        if self.in_mem_size > self.config.max_in_mem_size {
            self.flush_mem_buf();
            if self.l0_file_idx
                > self.l0_file_metas.len()
                    + self.flush_file_errs.len()
                    + self.unhandled_flush_files.len()
                    + self.flush_file_concurrency
            {
                self.recv_flush_files(1)?;
                // handle received flush files
                self.handle_flush_files()?;
            }
        }
        self.try_recv_flush_files()
    }

    fn l0_file_path(&self, file_idx: usize) -> PathBuf {
        self.l0_data_dir.join(format!("l0_kv_pairs_{}", file_idx))
    }

    fn l1_file_path(&self, file_idx: usize) -> PathBuf {
        self.l1_data_dir.join(format!("l1_kv_pairs_{}", file_idx))
    }

    fn try_recv_flush_files(&mut self) -> Result<()> {
        while let Ok(flush_file_result) = self.file_rx.try_recv() {
            match flush_file_result {
                Ok(flush_file) => {
                    self.unhandled_flush_files.push(flush_file);
                }
                Err(err) => {
                    self.flush_file_errs.push(err);
                }
            }
        }
        if !self.flush_file_errs.is_empty() {
            return Err(self.flush_file_errs.pop().unwrap());
        }
        if self.unhandled_flush_files.is_empty() {
            return Ok(());
        }
        self.handle_flush_files()
    }

    fn handle_flush_files(&mut self) -> Result<()> {
        self.unhandled_flush_files
            .sort_by(|a, b| a.file_idx.cmp(&b.file_idx));

        let handled_file_idx = self.l0_file_metas.len() + self.flush_file_errs.len();
        let mut need_handled = self.unhandled_flush_files.len();
        for (idx, unhandled_flush_file) in self.unhandled_flush_files.iter().enumerate() {
            assert!(unhandled_flush_file.file_idx >= handled_file_idx + idx);
            if unhandled_flush_file.file_idx > handled_file_idx + idx {
                need_handled = idx;
                break;
            }
        }
        if need_handled == 0 {
            return Ok(());
        }
        info!(
            "{} worker-{} need handle {} flush files",
            self.task_ctx.task_id, self.worker_id, need_handled
        );

        let mut handled_chunk_ids: HashMap<u64, u64> = HashMap::new();
        let mut file_metas = vec![];
        let mut last_key_comm_prefix = self.key_comm_prefix.clone();
        for mut unhandled_flush_file in self.unhandled_flush_files.drain(0..need_handled) {
            handled_chunk_ids = mem::take(&mut unhandled_flush_file.handled_chunk_ids);
            last_key_comm_prefix =
                get_common_prefix(&last_key_comm_prefix, &unhandled_flush_file.key_comm_prefix);

            file_metas.push(unhandled_flush_file.file_meta);
        }
        let keyspace_id = self.task_ctx.keyspace_id.unwrap_or_default();
        self.scheduler.add_flushed_files(
            need_handled,
            "l0_kv_pairs",
            &self.task_ctx.task_id,
            keyspace_id,
        );

        let mut checkpoint_guard = self.checkpoint.lock().unwrap();
        checkpoint_guard.update_l0_flushed_info(
            self.worker_id,
            handled_chunk_ids.clone(),
            file_metas.clone(),
            last_key_comm_prefix.clone(),
        )?;

        self.flushed_chunk_ids = handled_chunk_ids;
        self.key_comm_prefix = last_key_comm_prefix;
        self.l0_file_metas.append(&mut file_metas);

        info!(
            "{} worker-{} handle {} flush files, current key common prefix: {:?}",
            self.task_ctx.task_id, self.worker_id, need_handled, self.key_comm_prefix
        );
        Ok(())
    }

    fn handle_flush_msg(
        &mut self,
        flush_file_count: Option<usize>,
        cb: Box<dyn FnOnce(FlushStates) + Send>,
    ) {
        if self.scheduler.is_canceled() || self.scheduler.is_finished() {
            let flush_result = FlushResult {
                flushed_chunk_ids: HashMap::default(),
                canceled: self.scheduler.is_canceled(),
                finished: self.scheduler.is_finished(),
                error: self.scheduler.error_msg(),
            };
            warn!(
                "{} worker-{} skip flushing, canceled: {}, finished: {}, error message: {}",
                self.task_ctx.task_id,
                self.worker_id,
                flush_result.canceled,
                flush_result.finished,
                flush_result.error,
            );
            cb(FlushStates::FlushResult { flush_result });
            return;
        }

        let file_count;
        match flush_file_count {
            Some(flush_file_count) => {
                let result = if self.l0_file_idx < flush_file_count {
                    self.flush()
                } else {
                    self.try_recv_flush_files()
                };
                if let Err(err) = result {
                    error!(
                        "{} worker-{} failed to flush {:?}",
                        self.task_ctx.task_id, self.worker_id, err
                    );
                    self.scheduler.cancel(format!(
                        "{} worker-{} error: {:?}",
                        self.task_ctx.task_id, self.worker_id, err
                    ));
                    let flush_result = FlushResult {
                        flushed_chunk_ids: HashMap::new(),
                        canceled: true,
                        finished: false,
                        error: self.scheduler.error_msg(),
                    };
                    cb(FlushStates::FlushResult { flush_result });
                    let recv_count = self.l0_file_idx
                        - self.flush_file_errs.len()
                        - self.l0_file_metas.len()
                        - self.unhandled_flush_files.len();
                    if recv_count > 0 {
                        let _ = self.recv_flush_files(recv_count);
                    }
                    return;
                }
                file_count = flush_file_count;
            }
            None => {
                file_count = self.l0_file_idx + if self.kv_pairs.is_empty() { 0 } else { 1 };
                info!(
                    "{} worker-{} flush memory buffer, got file count {}",
                    self.task_ctx.task_id, self.worker_id, file_count
                );
            }
        }

        if self.l0_file_metas.len() + self.flush_file_errs.len() >= file_count {
            info!(
                "{} worker-{} has flushed files {}",
                self.task_ctx.task_id, self.worker_id, file_count
            );
            let flush_result = FlushResult {
                flushed_chunk_ids: self.flushed_chunk_ids.clone(),
                canceled: false,
                finished: false,
                error: self.scheduler.error_msg(),
            };
            cb(FlushStates::FlushResult { flush_result });
        } else {
            cb(FlushStates::FlushFileCount {
                flush_file_count: file_count,
            });
        }
    }

    fn flush(&mut self) -> Result<()> {
        if !self.flush_file_errs.is_empty() {
            return Err(self.flush_file_errs.pop().unwrap());
        }

        if !self.kv_pairs.is_empty() {
            self.flush_mem_buf();
        }

        self.try_recv_flush_files()
    }

    fn recv_flush_files(&mut self, mut recv_count: usize) -> Result<()> {
        while recv_count != 0 {
            match self.file_rx.recv().unwrap() {
                Ok(flush_file) => {
                    self.unhandled_flush_files.push(flush_file);
                }
                Err(err) => {
                    self.flush_file_errs.push(err);
                }
            }
            recv_count -= 1;
        }
        if !self.flush_file_errs.is_empty() {
            return Err(self.flush_file_errs.pop().unwrap());
        }
        Ok(())
    }

    fn collect_file_metas(
        &mut self,
        skip_sort: bool,
    ) -> Result<(Vec<FileMeta>, Vec<DuplicateEntry>)> {
        if self.scheduler.is_canceled() || self.scheduler.is_finished() {
            warn!(
                "{} worker-{} skip collecting file meats, canceled: {}, finished: {}, error message: {}",
                self.task_ctx.task_id,
                self.worker_id,
                self.scheduler.is_canceled(),
                self.scheduler.is_finished(),
                self.scheduler.error_msg(),
            );
            return Err(Error::Canceled);
        }

        if !self.kv_pairs.is_empty() {
            warn!(
                "{} worker-{} has unflushed kv pairs: {} in memory, skip them",
                self.task_ctx.task_id,
                self.worker_id,
                self.kv_pairs.len()
            );
            self.kv_pairs.clear();
        }

        if self.l0_file_metas.len() + self.flush_file_errs.len() < self.l0_file_idx {
            warn!(
                "{} worker-{} has unhandled l0 files: {}, skip them",
                self.task_ctx.task_id,
                self.worker_id,
                self.l0_file_idx - self.l0_file_metas.len() - self.flush_file_errs.len()
            );
            let recv_count = self.l0_file_idx
                - self.l0_file_metas.len()
                - self.flush_file_errs.len()
                - self.unhandled_flush_files.len();
            let _ = self.recv_flush_files(recv_count);
        }

        if skip_sort {
            return Ok((mem::take(&mut self.l0_file_metas), vec![]));
        }

        if !self.l1_file_metas.is_empty() {
            return Ok((
                mem::take(&mut self.l1_file_metas),
                mem::take(&mut self.dup_entries),
            ));
        }

        if self.l0_file_metas.is_empty() {
            info!(
                "{} worker-{} skip sorting empty data",
                self.task_ctx.task_id, self.worker_id
            );
            return Ok((vec![], vec![]));
        }

        info!(
            "{} worker-{} start to sort with key common prefix {:?}",
            self.task_ctx.task_id, self.worker_id, self.key_comm_prefix
        );
        let start = Instant::now();
        let (tx, rx) = tikv_util::mpsc::unbounded();
        let mut sent_count = 0;
        let mut recv_count = 0;

        let file_metas = mem::take(&mut self.l0_file_metas);
        let readers = build_readers(
            &self.task_ctx,
            file_metas,
            self.key_comm_prefix.len(),
            vec![],
            vec![],
            self.scheduler.io_runtime.clone(),
        );
        let mut merge_iter = MergeIterator::new(readers, &self.task_ctx.outer_key_prefix)?;

        let mut errs = vec![];
        let mut batches = vec![];
        let mut kv_count = 0;
        let mut kv_size = 0;
        let mut flushed_files_count = 0;
        let keyspace_id = self.task_ctx.keyspace_id.unwrap_or_default();
        while merge_iter.valid() {
            let (batch, batch_kv_count) =
                self.read_batch(&mut merge_iter, self.config.flush_batch_size)?;
            if batch.is_empty() {
                break;
            }

            kv_size += batch.len();
            kv_count += batch_kv_count;
            batches.push(batch);
            if kv_size > self.config.max_in_mem_size {
                self.flush_batches(
                    &mut batches,
                    kv_size,
                    kv_count,
                    merge_iter.prev_key().to_vec(),
                    tx.clone(),
                );
                kv_size = 0;
                kv_count = 0;
                sent_count += 1;
                if sent_count > self.flush_file_concurrency {
                    recv_count += 1;
                    match rx.recv().unwrap() {
                        Ok(l1_file) => {
                            self.l1_file_metas.push(l1_file);
                            flushed_files_count += 1;
                            if flushed_files_count == FILE_COUNTING_STEP {
                                self.scheduler.add_flushed_files(
                                    FILE_COUNTING_STEP,
                                    "l1_kv_pairs",
                                    &self.task_ctx.task_id,
                                    keyspace_id,
                                );
                                flushed_files_count = 0;
                            }
                        }
                        Err(err) => {
                            errs.push(err);
                            break;
                        }
                    }
                }
            }
        }
        if !batches.is_empty() {
            self.flush_batches(
                &mut batches,
                kv_size,
                kv_count,
                merge_iter.prev_key().to_vec(),
                tx.clone(),
            );
            sent_count += 1;
        }
        for _ in 0..(sent_count - recv_count) {
            match rx.recv().unwrap() {
                Ok(l1_file) => {
                    self.l1_file_metas.push(l1_file);
                    flushed_files_count += 1;
                    if flushed_files_count == FILE_COUNTING_STEP {
                        self.scheduler.add_flushed_files(
                            FILE_COUNTING_STEP,
                            "l1_kv_pairs",
                            &self.task_ctx.task_id,
                            keyspace_id,
                        );
                        flushed_files_count = 0;
                    }
                }
                Err(err) => {
                    errs.push(err);
                }
            }
        }
        if !errs.is_empty() {
            return Err(errs.pop().unwrap());
        }
        self.scheduler.add_flushed_files(
            flushed_files_count,
            "l1_kv_pairs",
            &self.task_ctx.task_id,
            keyspace_id,
        );

        if !merge_iter.duplicated_entries.is_empty() {
            info!(
                "{} worker-{} got {} duplicated entries, size {}",
                self.task_ctx.task_id,
                self.worker_id,
                merge_iter.duplicated_entries.len(),
                merge_iter.duplicated_entries_size
            );
            self.dup_entries = merge_iter.duplicated_entries;
        }

        let mut checkpoint_guard = self.checkpoint.lock().unwrap();
        checkpoint_guard.update_l1_flushed_info(
            self.worker_id,
            self.l1_file_metas.clone(),
            self.dup_entries.clone(),
        )?;
        info!(
            "{} worker-{} finish sorting, takes {:?}",
            self.task_ctx.task_id,
            self.worker_id,
            start.saturating_elapsed(),
        );

        Ok((
            mem::take(&mut self.l1_file_metas),
            mem::take(&mut self.dup_entries),
        ))
    }

    fn flush_mem_buf(&mut self) {
        let kv_pairs = mem::take(&mut self.kv_pairs);
        let kv_count = kv_pairs.len();
        let tx = self.file_tx.clone();
        let task_ctx = self.task_ctx.clone();
        let file_path = self.l0_file_path(self.l0_file_idx);
        let batch_size = self.config.flush_batch_size;
        let handled_chunk_ids = self.handled_chunk_ids.clone();
        let file_idx = self.l0_file_idx;
        let worker_id = self.worker_id;
        let in_mem_size = self.in_mem_size;
        self.in_mem_size = 0;
        self.l0_file_idx += 1;
        std::thread::spawn(move || {
            let task_id = task_ctx.task_id.clone();
            let start = Instant::now();
            match flush_l0_file_to_local(kv_pairs, task_ctx, file_path.clone(), batch_size) {
                Ok((first_key, last_key, key_comm_prefix, kv_size)) => {
                    tx.send(Ok(UnhandledFlushFile {
                        handled_chunk_ids,
                        file_idx,
                        key_comm_prefix,
                        file_meta: FileMeta {
                            file_path,
                            kv_count,
                            kv_size,
                            first_key,
                            last_key,
                        },
                    }))
                    .unwrap();
                }
                Err(err) => {
                    tx.send(Err(err)).unwrap();
                }
            }
            info!(
                "{} worker-{} flush to local file on in_mem_size {}, file index: {}, takes {:?}",
                task_id,
                worker_id,
                in_mem_size,
                file_idx,
                start.saturating_elapsed(),
            );
        });
    }

    fn flush_batches(
        &mut self,
        batches: &mut Vec<Vec<u8>>,
        kv_size: usize,
        kv_count: usize,
        last_key: Vec<u8>,
        tx: Sender<Result<FileMeta>>,
    ) {
        let batches = mem::take(batches);
        let task_ctx = self.task_ctx.clone();
        let file_idx = self.l1_file_idx;
        let file_path = self.l1_file_path(file_idx);
        let worker_id = self.worker_id;

        self.l1_file_idx += 1;
        std::thread::spawn(move || {
            let first_batch = &batches[0];
            let first_key_len = (&first_batch[0..]).get_u16_le();
            let first_key = first_batch[2..2 + first_key_len as usize].to_vec();
            let task_id = task_ctx.task_id.clone();
            let start = Instant::now();
            match flush_l1_file_to_local(batches, task_ctx, file_path.clone()) {
                Ok(()) => {
                    tx.send(Ok(FileMeta {
                        file_path,
                        kv_count,
                        kv_size,
                        first_key,
                        last_key,
                    }))
                    .unwrap();
                }
                Err(err) => {
                    tx.send(Err(err)).unwrap();
                }
            }
            info!(
                "{} worker-{} flush batches to local disk, file index: {}, takes {:?}",
                task_id,
                worker_id,
                file_idx,
                start.saturating_elapsed(),
            );
        });
    }

    fn read_batch(
        &mut self,
        merge_iter: &mut MergeIterator,
        batch_size: usize,
    ) -> Result<(Vec<u8>, usize)> {
        let mut buf = Vec::with_capacity(batch_size);
        let mut kv_count = 0;
        while merge_iter.valid() {
            let key = merge_iter.key();
            let key_len = key.len();
            let val = merge_iter.value();
            let val_len = val.len();
            let row_id = merge_iter.row_id();
            let row_id_len = row_id.len();
            if buf.len() + 2 + key_len + 4 + val_len + 2 + row_id_len > buf.capacity()
                && !buf.is_empty()
            {
                return Ok((buf, kv_count));
            }

            buf.put_u16_le(key_len as u16);
            buf.extend_from_slice(key);
            buf.put_u32_le(val_len as u32);
            buf.extend_from_slice(val);
            buf.put_u16_le(row_id_len as u16);
            buf.extend_from_slice(row_id);
            kv_count += 1;
            merge_iter.next()?;
        }
        Ok((buf, kv_count))
    }

    fn get_handled_chunk_id(&mut self, writer_id: u64) -> u64 {
        let chunk_id = self.handled_chunk_ids.entry(writer_id).or_default();
        *chunk_id
    }

    fn get_flushed_chunk_id(&mut self, writer_id: u64) -> u64 {
        let chunk_id = self.flushed_chunk_ids.entry(writer_id).or_default();
        *chunk_id
    }
}

fn new_file_writer(task_ctx: TaskContext, path: PathBuf) -> Result<EncrypterWriter<File>> {
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(path.as_path())?;
    let iv = if task_ctx.encryption_key.is_some() {
        let mut iv_buf = Vec::with_capacity(16);
        iv_buf.put_u64(task_ctx.start_ts);
        iv_buf.put_u64(task_ctx.commit_ts);
        Iv::from_slice(&iv_buf).unwrap()
    } else {
        Iv::Empty
    };
    let (method, key) = if let Some(key) = &task_ctx.encryption_key {
        (EncryptionMethod::Aes256Ctr, key.current_key.as_slice())
    } else {
        (EncryptionMethod::Plaintext, "".as_bytes())
    };
    Ok(EncrypterWriter::new(file, method, key, iv).unwrap())
}

fn flush_l1_file_to_local(
    batches: Vec<Vec<u8>>,
    task_ctx: TaskContext,
    path: PathBuf,
) -> Result<()> {
    let mut writer = new_file_writer(task_ctx, path)?;
    let mut compressed_buf = vec![];
    for batch in &batches {
        let compressed_size = compress_lz4(batch, &mut compressed_buf)? as u32;
        writer.write_all(&compressed_size.to_le_bytes())?;
        writer.write_all(&compressed_buf)?;
        compressed_buf.clear();
    }
    writer.flush()?;
    Ok(())
}

pub fn flush_l0_file_to_local(
    mut kv_pairs: Vec<KvPair>,
    task_ctx: TaskContext,
    path: PathBuf,
    batch_size: usize,
) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>, usize)> {
    kv_pairs.sort_by(|a, b| {
        let order = a.key.cmp(&b.key);
        if order == Ordering::Equal {
            return a.row_id.cmp(&b.row_id);
        }
        order
    });

    let first_key = kv_pairs.first().unwrap().key.to_vec();
    let last_key = kv_pairs.last().unwrap().key.to_vec();
    let key_comm_prefix = get_common_prefix(&first_key, &last_key);

    let mut writer = new_file_writer(task_ctx, path)?;
    let mut kv_size = 0;
    let mut buf: Vec<u8> = Vec::with_capacity(batch_size + batch_size / 8);
    let mut compressed_buf: Vec<u8> = Vec::with_capacity(batch_size + batch_size / 8);
    for pair in &kv_pairs {
        buf.put_u16_le(pair.key.len() as u16);
        buf.extend_from_slice(pair.key.chunk());
        buf.put_u32_le(pair.val.len() as u32);
        buf.extend_from_slice(pair.val.chunk());
        buf.put_u16_le(pair.row_id.len() as u16);
        buf.extend_from_slice(pair.row_id.chunk());

        if buf.len() >= batch_size {
            let compressed_size = compress_lz4(&buf, &mut compressed_buf)? as u32;
            writer.write_all(&compressed_size.to_le_bytes())?;
            writer.write_all(&compressed_buf)?;
            kv_size += buf.len();
            buf.clear();
            compressed_buf.clear();
        }
    }
    if !buf.is_empty() {
        let compressed_size = compress_lz4(&buf, &mut compressed_buf)? as u32;
        writer.write_all(&compressed_size.to_le_bytes())?;
        writer.write_all(&compressed_buf)?;
        kv_size += buf.len();
    }
    writer.flush()?;
    Ok((first_key, last_key, key_comm_prefix, kv_size))
}

pub enum BuildingWorkerMsg {
    Build {
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        file_metas: Vec<FileMeta>,
        compression_type: u8,
        cb: Box<dyn FnOnce(Vec<DuplicateEntry>) + Send>,
    },
    Ingest {
        cb: Box<dyn FnOnce(()) + Send>,
    },
    Cleanup,
}

pub struct BuildingWorker {
    worker_id: u64,
    config: LoadDataConfig,
    ctx: LoadDataContext,
    task_ctx: TaskContext,
    scheduler: LoadTaskScheduler,
    receiver: Receiver<BuildingWorkerMsg>,
    cached_file_ids: Vec<u64>,
    create_file_concurrency: usize,
    ingest_concurrency: usize,

    key_comm_prefix: Vec<u8>,
    checkpoint_store: Arc<Mutex<LocalFileCheckpointStorage>>,

    // the following fields need to be persisted
    sst_metas: Vec<SstMeta>,
    dup_entries: Vec<DuplicateEntry>,
    ingested: bool,
}

impl BuildingWorker {
    pub fn new(
        worker_id: u64,
        config: LoadDataConfig,
        ctx: LoadDataContext,
        task_ctx: TaskContext,
        create_file_concurrency: usize,
        ingest_concurrency: usize,
        key_comm_prefix: Vec<u8>,
        receiver: Receiver<BuildingWorkerMsg>,
        scheduler: LoadTaskScheduler,
        checkpoint_store: Arc<Mutex<LocalFileCheckpointStorage>>,
    ) -> Self {
        let mut checkpoint_guard = checkpoint_store.lock().unwrap();
        let worker_ctx = checkpoint_guard
            .checkpoint_ctx
            .get_building_worker_ctx(worker_id);

        let sst_metas = worker_ctx.sst_metas.clone();
        let ingested = worker_ctx.ingested;
        let dup_entries = worker_ctx.duplicated_entries.clone();
        drop(checkpoint_guard);

        Self {
            worker_id,
            config,
            ctx,
            task_ctx,
            receiver,
            scheduler,
            cached_file_ids: vec![],
            create_file_concurrency,
            ingest_concurrency,
            key_comm_prefix,
            checkpoint_store,
            sst_metas,
            dup_entries,
            ingested,
        }
    }

    pub fn run(&mut self) {
        info!(
            "{} run building worker-{}, key comm prefix: {:?}, sst metas: {}, duplicated entries: {}",
            self.task_ctx.task_id,
            self.worker_id,
            self.key_comm_prefix,
            self.sst_metas.len(),
            self.dup_entries.len(),
        );
        while let Ok(msg) = self.receiver.recv() {
            match msg {
                BuildingWorkerMsg::Build {
                    start_key,
                    end_key,
                    file_metas,
                    compression_type,
                    cb,
                } => match self.build(file_metas, start_key, end_key, compression_type) {
                    Ok(dup_entries) => {
                        cb(dup_entries);
                    }
                    Err(err) => {
                        error!(
                            "{} worker-{} failed to build sst files, error: {:?}",
                            self.task_ctx.task_id, self.worker_id, err
                        );
                        cb(vec![]);
                        self.scheduler.cancel(format!(
                            "{} worker-{} error: {:?}",
                            self.task_ctx.task_id, self.worker_id, err
                        ));
                    }
                },
                BuildingWorkerMsg::Ingest { cb } => {
                    if let Err(err) = self.ingest() {
                        error!(
                            "{} worker-{} failed to ingest sst files, error: {:?}",
                            self.task_ctx.task_id, self.worker_id, err
                        );
                        self.scheduler.cancel(format!(
                            "{} worker-{} error: {:?}",
                            self.task_ctx.task_id, self.worker_id, err
                        ));
                    }
                    cb(());
                }
                BuildingWorkerMsg::Cleanup => {
                    return;
                }
            }
        }
    }

    fn ingest(&mut self) -> Result<()> {
        if self.scheduler.is_canceled() || self.scheduler.is_finished() {
            warn!(
                "{} worker-{} skip building sst, canceled: {}, finished: {}, error message: {}",
                self.task_ctx.task_id,
                self.worker_id,
                self.scheduler.is_canceled(),
                self.scheduler.is_finished(),
                self.scheduler.error_msg(),
            );
            return Err(Error::Canceled);
        }

        if self.ingested {
            return Ok(());
        }

        if self.sst_metas.is_empty() {
            info!(
                "{} worker-{} skip ingesting empty data",
                self.task_ctx.task_id, self.worker_id
            );
            let mut checkpoint_guard = self.checkpoint_store.lock().unwrap();
            checkpoint_guard.set_worker_ingested(self.worker_id)?;
            return Ok(());
        }

        let start = Instant::now();
        let mut data_size = 0;
        let mut total_kvs = 0;
        for sst_meta in &self.sst_metas {
            data_size += sst_meta.uncompressed_size;
            total_kvs += sst_meta.keys;
        }
        let sst_metas = mem::take(&mut self.sst_metas);
        self.ingest_sst(sst_metas)?;
        self.scheduler.add_total_kvs(total_kvs);

        let mut wru = 0.0;
        if let (Some(ru_config), Some(keyspace_id)) =
            (&self.config.rg_config, self.task_ctx.keyspace_id)
        {
            let request_unit = &ru_config.request_unit;
            // The calculation formula is a reference to the pd's ru consumption,
            // ref https://github.com/tikv/pd/blob/master/client/resource_group/controller/model.go#L103.
            //
            // `write_base_cost + write_per_batch_base_cost * avg_batch_proportion +
            // write_cost_per_byte * data_size * replica_nums`
            //
            // In the formula, we use the default value of `avg_batch_proportion` and
            // `replica_nums`, which are 0.5 (same as the default value of
            // `avg_batch_proportion` in pd) and 3.0 respectively.
            //
            // Set import billing same as txn file, though the underlying mechanisms are not
            // the same.
            wru = request_unit.write_base_cost
                + request_unit.write_per_batch_base_cost * DEFAULT_AVG_BATCH_PROPORTION
                + request_unit.write_cost_per_byte
                    * data_size as f64
                    * TXN_FILE_RU_DISCOUNT_RATIO
                    * REPLICA_NUMS;

            LOAD_DATA_WRU_COST_COUNTER
                .with_label_values(&[&self.task_ctx.task_id, &keyspace_id.to_string()])
                .inc_by(wru as u64);
        }

        let mut checkpoint_guard = self.checkpoint_store.lock().unwrap();
        checkpoint_guard.set_worker_ingested(self.worker_id)?;
        info!(
            "{} worker-{} finish ingesting, data size: {}, kvs: {}, ru consumption: {}, keyspace id: {:?}, takes {:?}",
            self.task_ctx.task_id,
            self.worker_id,
            data_size,
            total_kvs,
            wru,
            self.task_ctx.keyspace_id,
            start.saturating_elapsed(),
        );
        Ok(())
    }

    fn build(
        &mut self,
        file_metas: Vec<FileMeta>,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        compression_type: u8,
    ) -> Result<Vec<DuplicateEntry>> {
        if self.scheduler.is_canceled() || self.scheduler.is_finished() {
            warn!(
                "{} worker-{} skip building sst, canceled: {}, finished: {}, error message: {}",
                self.task_ctx.task_id,
                self.worker_id,
                self.scheduler.is_canceled(),
                self.scheduler.is_finished(),
                self.scheduler.error_msg(),
            );
            return Err(Error::Canceled);
        }
        if !self.sst_metas.is_empty() {
            return Ok(mem::take(&mut self.dup_entries));
        }
        if file_metas.is_empty() {
            info!(
                "{} worker-{} skip building empty data",
                self.task_ctx.task_id, self.worker_id
            );
            return Ok(vec![]);
        }

        info!(
            "{} worker-{} start to build sst with key common prefix {:?}",
            self.task_ctx.task_id, self.worker_id, self.key_comm_prefix
        );
        let start = Instant::now();
        let readers = build_readers(
            &self.task_ctx,
            file_metas,
            self.key_comm_prefix.len(),
            start_key,
            end_key,
            self.scheduler.io_runtime.clone(),
        );
        let (tx, rx) = tikv_util::mpsc::unbounded();
        let mut sent_count = 0;
        let mut recv_count = 0;
        let mut created_file_count = 0;
        let mut merge_iter = MergeIterator::new(readers, &self.task_ctx.outer_key_prefix)?;
        let keyspace_id = self.task_ctx.keyspace_id.unwrap_or_default();

        let mut errs = vec![];
        while merge_iter.valid() {
            let batch = self.read_batch(&mut merge_iter, self.config.sst_file_size)?;
            if batch.is_empty() {
                break;
            }
            let file_id = self.alloc_file_id();
            if let Err(err) = file_id {
                errs.push(err);
                break;
            }
            self.spawn_build_file(file_id.unwrap(), batch, tx.clone(), compression_type);
            sent_count += 1;
            if sent_count > self.create_file_concurrency {
                recv_count += 1;
                match rx.recv().unwrap() {
                    Err(err) => {
                        errs.push(err);
                        break;
                    }
                    Ok(sst_meta) => {
                        self.sst_metas.push(sst_meta);
                        created_file_count += 1;
                        if created_file_count == FILE_COUNTING_STEP {
                            self.scheduler.add_created_files(
                                FILE_COUNTING_STEP,
                                &self.task_ctx.task_id,
                                keyspace_id,
                            );
                            created_file_count = 0;
                        }
                    }
                }
            }
        }
        for _ in 0..(sent_count - recv_count) {
            match rx.recv().unwrap() {
                Err(err) => {
                    error!(
                        "{} worker-{} failed to create sst file, error: {}",
                        self.task_ctx.task_id, self.worker_id, err
                    );
                    errs.push(err);
                }
                Ok(sst_meta) => {
                    self.sst_metas.push(sst_meta);
                    created_file_count += 1;
                    if created_file_count == FILE_COUNTING_STEP {
                        self.scheduler.add_created_files(
                            FILE_COUNTING_STEP,
                            &self.task_ctx.task_id,
                            keyspace_id,
                        );
                        created_file_count = 0;
                    }
                }
            }
        }
        if !errs.is_empty() {
            return Err(errs.pop().unwrap());
        }
        self.scheduler
            .add_created_files(created_file_count, &self.task_ctx.task_id, keyspace_id);

        if !merge_iter.duplicated_entries.is_empty() {
            info!(
                "{} worker-{} got {} duplicated entries, size {}",
                self.task_ctx.task_id,
                self.worker_id,
                merge_iter.duplicated_entries.len(),
                merge_iter.duplicated_entries_size
            );
            self.dup_entries = merge_iter.duplicated_entries;
        }

        let mut checkpoint_guard = self.checkpoint_store.lock().unwrap();
        checkpoint_guard.update_sst_metas(
            self.worker_id,
            self.sst_metas.clone(),
            self.dup_entries.clone(),
        )?;

        info!(
            "{} worker-{} finish building, sst metas: {}, duplicated entries: {}, takes {:?}",
            self.task_ctx.task_id,
            self.worker_id,
            self.sst_metas.len(),
            self.dup_entries.len(),
            start.saturating_elapsed(),
        );
        Ok(mem::take(&mut self.dup_entries))
    }

    fn read_batch(&mut self, merge_iter: &mut MergeIterator, batch_size: usize) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(batch_size);
        let mut pre_table_id = vec![];
        while merge_iter.valid() {
            let key = merge_iter.key();
            let key_len = key.len();
            let val = merge_iter.value();
            let val_len = val.len();

            if buf.len() + 2 + key_len + 4 + val_len > buf.capacity() && !buf.is_empty() {
                return Ok(buf);
            }
            let table_id = merge_iter.table_id();
            if !pre_table_id.is_empty() && pre_table_id.as_slice() != table_id {
                return Ok(buf);
            }
            pre_table_id = table_id.to_vec();

            buf.put_u16_le(key_len as u16);
            buf.extend_from_slice(key);
            buf.put_u32_le(val_len as u32);
            buf.extend_from_slice(val);
            merge_iter.next()?;
        }
        Ok(buf)
    }

    fn alloc_file_id(&mut self) -> Result<u64> {
        if let Some(id) = self.cached_file_ids.pop() {
            return Ok(id);
        }
        let start = Instant::now();
        let count = 64;
        loop {
            match executor::block_on(self.ctx.pd.batch_get_tso(count as u32)) {
                Ok(ts) => {
                    let last = ts.into_inner();
                    let first = last - count as u64 + 1;
                    self.cached_file_ids = (first..=last).rev().collect();
                    return Ok(self.cached_file_ids.pop().unwrap());
                }
                Err(err) => {
                    error!(
                        "{} worker-{} failed to allocate file id from PD {:?}",
                        self.task_ctx.task_id, self.worker_id, err
                    );
                    std::thread::sleep(Duration::from_secs(1));
                    if start.saturating_elapsed() > ALLOCATE_ID_TIMEOUT {
                        return Err(Error::PdError(err));
                    }
                }
            }
        }
    }

    fn spawn_build_file(
        &self,
        file_id: u64,
        mut batch: Vec<u8>,
        sender: Sender<Result<SstMeta>>,
        compression_type: u8,
    ) {
        let ctx = self.ctx.clone();
        let block_size = self.config.block_size;
        let task_id = self.task_ctx.task_id.clone();
        let worker_id = self.worker_id;
        let checksum_type = self.config.checksum_type;
        let encryption_key = self.task_ctx.encryption_key.clone();
        let um = UserMeta::new(self.task_ctx.start_ts, self.task_ctx.commit_ts);
        let mut val_buf = Value::encode_buf(0, &um.to_array(), self.task_ctx.commit_ts, &[]);
        let base_val_len = val_buf.len();

        self.ctx.runtime.spawn(async move {
            info!(
                "{} worker-{} start to build sst file {}",
                task_id, worker_id, file_id
            );
            let mut builder = Builder::new(
                file_id,
                block_size,
                compression_type,
                ZSTD_COMPRESSION_LEVEL,
                checksum_type,
                encryption_key,
            );
            let mut entries = 0;
            let mut offset = 0;
            let mut uncompressed_size = 0;
            while offset < batch.len() {
                let key_len = (&batch[offset..]).get_u16_le() as usize;
                offset += 2;
                let key = &batch[offset..offset + key_len];
                offset += key_len;
                uncompressed_size += key_len;
                let val_len = (&batch[offset..]).get_u32_le() as usize;
                offset += 4;
                let val = &batch[offset..offset + val_len];
                offset += val_len;
                uncompressed_size += base_val_len + val_len;
                val_buf.resize(base_val_len, 0);
                val_buf.extend_from_slice(val);

                // The key is already trimmed prefix, so we can use `from_inner_buf` here.
                let inner_key = InnerKey::from_inner_buf(key);
                builder.add(inner_key, &Value::decode(&val_buf), None);
                entries += 1;
            }
            batch.clear();
            let res = builder.finish(0, &mut batch);
            let data: Bytes = batch.into();
            let sst_meta = SstMeta {
                id: file_id,
                smallest: builder.get_smallest().to_vec(),
                biggest: builder.get_biggest().to_vec(),
                size: data.len(),
                meta_offset: res.meta_offset,
                uncompressed_size,
                keys: entries,
            };
            info!(
                "{} worker-{} finish building sst file {:?}",
                task_id, worker_id, sst_meta
            );
            let opts = Options::default();
            let res = ctx
                .dfs
                .create(file_id, data, opts)
                .await
                .map(|_| sst_meta.clone())
                .map_err(|e| Error::from(e));

            sender.send(res).unwrap();
        });
    }

    fn ingest_sst(&mut self, mut sst_metas: Vec<SstMeta>) -> Result<()> {
        if sst_metas.is_empty() {
            return Ok(());
        }
        info!(
            "{} worker-{} start to ingest, sst metas: {}",
            self.task_ctx.task_id,
            self.worker_id,
            sst_metas.len()
        );
        sst_metas.sort_by(|a, b| a.id.cmp(&b.id));
        let start = Instant::now();
        let outer_key_prefix = self.task_ctx.outer_key_prefix.to_vec();
        let sst_groups = group_ssts_by_table_id(&sst_metas);
        let mut coarse_split_keys = vec![];
        for sst_group in sst_groups {
            coarse_split_keys.extend(gen_split_keys(
                &outer_key_prefix,
                sst_group,
                self.config.coarse_split_size,
                true,
            ));
        }
        coarse_split_keys.dedup();
        let new_regions_id = self.split_regions(&coarse_split_keys)?;
        let result = self.ctx.pd.scatter_regions_by_id(new_regions_id);
        if let Err(err) = result {
            error!(
                "{} worker-{} scatter regions failed {:?}",
                self.task_ctx.task_id, self.worker_id, err
            );
        }
        for i in 0..coarse_split_keys.len() - 1 {
            let mut encoded_start_key = coarse_split_keys[i].as_slice();
            let raw_start_key = decode_bytes(&mut encoded_start_key, false).unwrap();
            let mut encoded_end_key = coarse_split_keys[i + 1].as_slice();
            let raw_end_key = decode_bytes(&mut encoded_end_key, false).unwrap();
            let inner_start_key = InnerKey::from_outer_key(&raw_start_key);
            let inner_end_key = InnerKey::from_outer_end_key(&raw_end_key);
            let group_ssts = get_ssts_in_range(&sst_metas, inner_start_key, inner_end_key);
            if group_ssts.is_empty() {
                // It's possible that the range between two bound keys has no sst files.
                continue;
            }
            self.ingest_group(group_ssts)?;
        }
        info!(
            "{} worker-{} finish ingesting, takes {:?}",
            self.task_ctx.task_id,
            self.worker_id,
            start.saturating_elapsed()
        );
        Ok(())
    }

    fn split_regions(&self, split_keys: &[Vec<u8>]) -> Result<Vec<u64>> {
        info!(
            "{} worker-{} start split, keys {:?}",
            self.task_ctx.task_id, self.worker_id, split_keys
        );
        let mut retry = 0;
        let mut split_keys = split_keys.to_owned();
        let mut new_regions_id = Vec::with_capacity(split_keys.len());
        loop {
            let mut unprocessed_keys = Vec::with_capacity(split_keys.len());
            for split_key in &split_keys {
                let region = self
                    .ctx
                    .pd
                    .get_region_with_retry(split_key, Duration::from_secs(60))?;
                let start_key = region.get_start_key();
                if start_key == split_key {
                    new_regions_id.push(region.get_id());
                    continue;
                }
                unprocessed_keys.push(split_key.clone());
            }
            if unprocessed_keys.is_empty() {
                break;
            }

            let result = self
                .ctx
                .runtime
                .block_on(self.ctx.pd.split_regions(unprocessed_keys.clone()));
            match result {
                Err(err) => {
                    error!(
                        "{} worker-{} split failed {:?}",
                        self.task_ctx.task_id, self.worker_id, err
                    );
                    LOAD_DATA_SPLIT_REGION_FAILURES_COUNTER
                        .with_label_values(&[&self.task_ctx.task_id])
                        .inc();
                    if retry >= MAX_RETRY_TIMES {
                        return Err(Error::PdError(err));
                    }

                    std::thread::sleep(std::cmp::min(
                        MAX_SLEEP_DURATION,
                        2_u32.pow(retry as u32) * RETRY_SLEEP_DURATION,
                    ));
                }
                Ok(regions_id) => {
                    new_regions_id.extend_from_slice(&regions_id);
                    LOAD_DATA_SPLIT_REGION_FAILURES_COUNTER
                        .with_label_values(&[&self.task_ctx.task_id])
                        .reset();
                    break;
                }
            }
            split_keys = unprocessed_keys.clone();
            retry += 1;
        }
        new_regions_id.sort();
        new_regions_id.dedup();
        info!(
            "{} worker-{} finish split, new regions_id {:?}",
            self.task_ctx.task_id, self.worker_id, new_regions_id
        );
        Ok(new_regions_id)
    }

    fn ingest_group(&self, sst_metas: Vec<SstMeta>) -> Result<()> {
        let outer_key_prefix = self.task_ctx.outer_key_prefix.to_vec();
        let split_keys = gen_split_keys(
            &outer_key_prefix,
            &sst_metas,
            self.config.region_size,
            false,
        );
        if !split_keys.is_empty() {
            self.split_regions(&split_keys)?;
        }
        let outer_first_key = new_region_key(
            &outer_key_prefix,
            sst_metas.first().unwrap().smallest.as_slice(),
        );
        let outer_last_key = {
            let mut last_key = sst_metas.last().unwrap().biggest.clone();
            last_key.push(0);
            new_region_key(&outer_key_prefix, &last_key)
        };

        let mut success_ranges = MergeRanges::default(); // keys of `success_ranges` are encoded.
        let mut last_error: Option<Error> = None;
        for retry in 0..MAX_RETRY_TIMES {
            match self.ingest_group_to_range(
                &outer_key_prefix,
                &sst_metas,
                outer_first_key.clone(),
                outer_last_key.clone(),
                &mut success_ranges,
            ) {
                Ok(_) => {
                    debug_assert!(success_ranges.covered(&outer_first_key, &outer_last_key));
                    LOAD_DATA_INGEST_RANEG_GROUP_FAILURES_COUNTER
                        .with_label_values(&[&self.task_ctx.task_id])
                        .reset();
                    return Ok(());
                }
                Err(err) if Self::is_ingest_error_retryable(&err) => {
                    LOAD_DATA_INGEST_RANEG_GROUP_FAILURES_COUNTER
                        .with_label_values(&[&self.task_ctx.task_id])
                        .inc();
                    warn!(
                        "{} worker-{} ingest_group_to_range failed {:?}, retry {}",
                        self.task_ctx.task_id, self.worker_id, err, retry
                    );
                    last_error = Some(err);
                    std::thread::sleep(std::cmp::min(
                        MAX_SLEEP_DURATION,
                        2_u32.pow(retry as u32) * RETRY_SLEEP_DURATION,
                    ));
                }
                Err(err) => return Err(err),
            }
        }
        Err(last_error.unwrap())
    }

    fn is_ingest_error_retryable(err: &Error) -> bool {
        match err {
            Error::RegionNotFound(_)
            | Error::LeaderNotFound(_)
            | Error::StoreDiskFull(_)
            | Error::RegionError(..)
            | Error::PdError(_)
            | Error::HyperError(_)
            | Error::RegionsIntegrityError(_) => true,
            Error::MultiErrors(errs) => errs.iter().all(Self::is_ingest_error_retryable),
            _ => false,
        }
    }

    fn ingest_group_to_range(
        &self,
        outer_key_prefix: &[u8],
        sst_metas: &[SstMeta],
        outer_first_key: Vec<u8>,
        outer_last_key: Vec<u8>,
        success_ranges: &mut MergeRanges,
    ) -> Result<()> {
        let mut regions = self.ctx.runtime.block_on(self.ctx.pd.scan_regions(
            outer_first_key.clone(),
            outer_last_key.clone(),
            usize::MAX,
        ))?;
        verify_regions_boundary(&outer_first_key, &outer_last_key, &regions)?;
        if !success_ranges.is_empty() {
            regions.retain(|region| {
                !success_ranges
                    .covered(&region.get_region().start_key, &region.get_region().end_key)
            });
        }
        info!(
            "{} worker-{} scanned and filtered regions {:?}",
            self.task_ctx.task_id, self.worker_id, regions
        );

        let mut disk_full_regions = vec![];
        let mut errors = vec![];
        let mut handle_ingest_res =
            |(region, res, ingested_sst): (metapb::Region, Result<()>, usize)| match res {
                Ok(()) => {
                    let success_start = max(region.get_start_key(), outer_first_key.as_slice());
                    let success_end = min(region.get_end_key(), outer_last_key.as_slice());
                    success_ranges.insert(success_start.to_vec(), success_end.to_vec());
                    self.scheduler.add_ingested_regions();
                    let keyspace_id = self.task_ctx.keyspace_id.unwrap_or_default().to_string();
                    info!(
                        "{} worker-{} ingested sst {} to region {:?}",
                        self.task_ctx.task_id, self.worker_id, ingested_sst, region
                    );
                    LOAD_DATA_INGEST_SST_FILE_COUNTER
                        .with_label_values(&[&self.task_ctx.task_id, &keyspace_id])
                        .inc_by(ingested_sst as u64);
                }
                Err(err) => {
                    if matches!(err, Error::StoreDiskFull(_)) {
                        disk_full_regions.push(region.id);
                    }
                    errors.push(err);
                }
            };

        let (tx, rx) = tikv_util::mpsc::unbounded();
        let mut msg_cnt = 0;
        for mut pd_region in regions {
            let region = pd_region.get_region();
            let cs = build_ingest_files(
                outer_key_prefix.len(),
                region,
                sst_metas,
                self.task_ctx.commit_ts,
            );
            let ingested_sst = cs.get_ingest_files().get_table_creates().len();
            if ingested_sst == 0 {
                continue;
            }
            if self.scheduler.is_canceled() {
                return Err(Error::Canceled);
            }
            let pd_cli = self.ctx.pd.clone();
            let task_id = self.task_ctx.task_id.clone();
            let worker_id = self.worker_id;
            let tx = tx.clone();
            self.ctx.runtime.spawn(async move {
                let region = pd_region.take_region();
                let leader = pd_region.take_leader();
                info!(
                    "{} worker-{} ingest_group_to_range: region: {:?}, leader: {:?}, cs: {:?}",
                    task_id, worker_id, region, leader, cs
                );
                let res = ingest_files_to_leader(pd_cli, cs, &region, leader).await;
                let _ = tx.send((region, res, ingested_sst));
            });
            if msg_cnt < self.ingest_concurrency {
                msg_cnt += 1;
            } else {
                handle_ingest_res(rx.recv().unwrap());
            }
        }
        for _ in 0..msg_cnt {
            handle_ingest_res(rx.recv().unwrap());
        }

        if !disk_full_regions.is_empty() {
            info!("{} worker-{} scatter regions due to disk full", self.task_ctx.task_id, self.worker_id;
                "regions" => ?disk_full_regions);
            if let Err(err) = self.ctx.pd.scatter_regions_by_id(disk_full_regions.clone()) {
                warn!("{} worker-{} scatter regions failed", self.task_ctx.task_id, self.worker_id;
                    "regions" => ?disk_full_regions, "err" => ?err);
            }
        }

        if !errors.is_empty() {
            return Err(Error::MultiErrors(errors));
        }
        Ok(())
    }
}

pub fn build_readers(
    task_ctx: &TaskContext,
    file_metas: Vec<FileMeta>,
    key_comm_prefix_len: usize,
    lower_bound: Vec<u8>,
    upper_bound: Vec<u8>,
    io_runtime: Arc<runtime::Runtime>,
) -> Vec<KvPairsReader> {
    let table_prefix_offset = KEYSPACE_PREFIX_LEN - task_ctx.inner_key_off.unwrap();
    let mut readers = Vec::with_capacity(file_metas.len());
    for file_meta in file_metas {
        let file = File::open(file_meta.file_path).unwrap();
        let (method, key) = if let Some(key) = &task_ctx.encryption_key {
            (EncryptionMethod::Aes256Ctr, key.current_key.as_slice())
        } else {
            (EncryptionMethod::Plaintext, "".as_bytes())
        };
        let iv = if task_ctx.encryption_key.is_some() {
            let mut iv_buf = Vec::with_capacity(16);
            iv_buf.put_u64(task_ctx.start_ts);
            iv_buf.put_u64(task_ctx.commit_ts);
            Iv::from_slice(&iv_buf).unwrap()
        } else {
            Iv::Empty
        };
        let decrypter_reader = DecrypterReader::new(file, method, key, iv).unwrap();

        let lower_bound_suffix = if !lower_bound.is_empty() && lower_bound > file_meta.first_key {
            lower_bound.as_slice()[key_comm_prefix_len..].to_vec()
        } else {
            vec![]
        };
        let upper_bound_suffix = if !upper_bound.is_empty() && upper_bound <= file_meta.last_key {
            upper_bound.as_slice()[key_comm_prefix_len..].to_vec()
        } else {
            vec![]
        };

        let reader = KvPairsReader::new(
            file_meta.kv_count,
            decrypter_reader,
            key_comm_prefix_len,
            lower_bound_suffix,
            upper_bound_suffix,
            table_prefix_offset,
            io_runtime.clone(),
        );
        readers.push(reader);
    }
    readers
}

pub async fn ingest_files_to_leader(
    pd: Arc<dyn PdClient>,
    cs: kvenginepb::ChangeSet,
    region: &metapb::Region,
    mut leader: metapb::Peer,
) -> Result<()> {
    let security_mgr = pd.get_security_mgr();
    let http_client = security_mgr.http_client(hyper::Client::builder())?;
    // Loop for retry on "not leader".
    loop {
        let store = get_leader_store(
            pd.clone() as Arc<dyn PdClient>,
            region.get_id(),
            Some(&leader),
        )
        .await?;
        let uri = security_mgr.build_uri(format!(
            "{}/ingest_files?cluster_id={}",
            &store.status_address,
            pd.get_cluster_id().unwrap()
        ))?;
        let body = cs.write_to_bytes().unwrap();
        let req = Request::post(uri).body(Body::from(body))?;
        let resp = http_client.request(req).await?;
        if !resp.status().is_success() {
            let body = hyper::body::to_bytes(resp.into_body()).await?;
            let mut errpb = kvproto::errorpb::Error::new();
            errpb.merge_from_bytes(&body).unwrap();
            let tag = ShardTag::new(
                store.get_id(),
                IdVer::new(region.get_id(), region.get_region_epoch().get_version()),
            );
            warn!("{} ingest_files_to_leader failed {:?}", tag, errpb);
            if errpb.has_region_not_initialized() {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            } else if errpb.has_not_leader() {
                leader = errpb.mut_not_leader().take_leader();
                if leader.store_id == 0 {
                    return Err(Error::LeaderNotFound(region.get_id()));
                }
                continue;
            } else if errpb
                .get_message()
                .starts_with(rfstore::errors::INGEST_OVERLAP_ERROR_TAG)
            {
                return Err(Error::IngestOverlap(errpb.take_message()));
            } else if errpb.has_disk_full() {
                return Err(Error::StoreDiskFull(errpb.take_disk_full().store_id));
            } else {
                return Err(Error::RegionError(region.get_id(), errpb));
            }
        }
        return Ok(());
    }
}

#[allow(clippy::unnecessary_unwrap)]
pub async fn get_leader_store(
    pd: Arc<dyn PdClient>,
    region_id: u64,
    leader: Option<&metapb::Peer>,
) -> Result<metapb::Store> {
    let store_id = if leader.is_none() || leader.unwrap().store_id == 0 {
        let res = pd.get_region_leader_by_id(region_id).await?;
        if res.is_none() {
            return Err(Error::RegionNotFound(region_id));
        }
        let (_, leader) = res.unwrap();
        if leader.store_id == 0 {
            return Err(Error::LeaderNotFound(region_id));
        }
        leader.store_id
    } else {
        leader.unwrap().store_id
    };
    Ok(pd.get_store_async(store_id).await?)
}

pub fn build_ingest_files(
    inner_key_off: usize,
    region: &metapb::Region,
    sst_metas: &[SstMeta],
    commit_ts: u64,
) -> kvenginepb::ChangeSet {
    let raw_start_key = raw_start_key(region);
    let inner_start_key = &raw_start_key[inner_key_off..];
    let raw_end_key = raw_end_key(region);
    let inner_end_key = &raw_end_key[inner_key_off..];
    let mut cs = kvenginepb::ChangeSet::default();
    cs.set_shard_id(region.get_id());
    cs.set_shard_ver(region.get_region_epoch().get_version());
    let ingest_files = cs.mut_ingest_files();
    ingest_files.set_max_ts(commit_ts);
    // Don't set INGEST_ID_KEY property to indicate that it's from load data.
    let table_creates = ingest_files.mut_table_creates();
    for sst_meta in sst_metas {
        if sst_meta.biggest.as_slice() < inner_start_key {
            continue;
        }
        if !inner_end_key.is_empty() && sst_meta.smallest.as_slice() >= inner_end_key {
            break;
        }
        let table_create = new_table_create_pb(
            sst_meta.id,
            WRITE_CF_BOTTOM_LEVEL,
            WRITE_CF as i32,
            sst_meta.smallest.clone(),
            sst_meta.biggest.clone(),
            sst_meta.meta_offset,
        );
        table_creates.push(table_create);
    }
    cs
}

pub fn group_ssts_by_table_id(sst_metas: &[SstMeta]) -> Vec<&[SstMeta]> {
    let mut groups = vec![];
    let mut start = 0;
    let mut cur_table_id = decode_table_id(&sst_metas[0].smallest).unwrap();
    for (i, sst_meta) in sst_metas.iter().enumerate() {
        let table_id = decode_table_id(&sst_meta.smallest).unwrap();
        if table_id != cur_table_id {
            groups.push(&sst_metas[start..i]);
            start = i;
            cur_table_id = table_id;
            continue;
        }
    }
    groups.push(&sst_metas[start..]);
    groups
}

pub fn gen_split_keys(
    outer_key_prefix: &[u8],
    ssts: &[SstMeta],
    split_size: usize,
    include_bound: bool,
) -> Vec<Vec<u8>> {
    let mut keys = vec![];
    let smallest = ssts.first().unwrap().smallest.as_slice();
    let biggest = ssts.last().unwrap().biggest.as_slice();
    let (start_key, end_key) = calculate_bound_key(smallest, biggest).unwrap_or_else(|| {
        warn!("failed to calculate bound key, fallback to use first and last key");
        // split at last key so the last region will not be split by other concurrent
        // load_data and get epoch not match error.
        let mut last_key = ssts.last().unwrap().biggest.to_vec();
        last_key.push(0);
        (smallest.to_vec(), last_key)
    });
    if include_bound {
        keys.push(new_region_key(outer_key_prefix, &start_key));
    }
    let mut size = 0;
    for sst in ssts {
        if size > split_size {
            keys.push(new_region_key(outer_key_prefix, sst.smallest.as_slice()));
            size = 0;
        }
        size += sst.size;
    }
    if include_bound {
        keys.push(new_region_key(outer_key_prefix, &end_key));
    }
    keys
}

fn calculate_bound_key(smallest: &[u8], biggest: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let first_table_id = decode_table_id(smallest).ok()?;
    let last_table_id = decode_table_id(biggest).ok()?;
    if first_table_id != last_table_id {
        // ingest task should only contains one table.
        return None;
    }
    let smallest_is_row_key = smallest[TABLE_PREFIX_KEY_LEN..].starts_with(RECORD_PREFIX_SEP);
    let biggest_is_row_key = biggest[TABLE_PREFIX_KEY_LEN..].starts_with(RECORD_PREFIX_SEP);
    let table_bound = (
        encode_table_prefix_key(first_table_id).to_vec(),
        encode_table_prefix_key(first_table_id + 1).to_vec(),
    );
    if smallest_is_row_key != biggest_is_row_key {
        // ingest task should only contains row key or only contains index key.
        return None;
    }
    if smallest_is_row_key {
        return Some(table_bound);
    }
    // index key
    let index_prefix_len = TABLE_PREFIX_KEY_LEN + INDEX_PREFIX_SEP.len() + ID_LEN;
    if smallest.len() <= index_prefix_len || biggest.len() <= index_prefix_len {
        // invalid index key.
        return None;
    }
    let smallest_prefix = &smallest[..index_prefix_len];
    let biggest_prefix = &biggest[..index_prefix_len];
    if smallest_prefix != biggest_prefix {
        // ingest task should only contain one index.
        return None;
    }
    Some((smallest_prefix.to_vec(), next_key(smallest_prefix)))
}

pub fn new_region_key(outer_key_prefix: &[u8], raw_key: &[u8]) -> Vec<u8> {
    let mut key = outer_key_prefix.to_vec();
    key.extend_from_slice(raw_key);
    encode_bytes(&key)
}

pub fn get_ssts_in_range(ssts: &[SstMeta], start: InnerKey<'_>, end: InnerKey<'_>) -> Vec<SstMeta> {
    let position = ssts
        .binary_search_by(|sst| InnerKey::from_inner_buf(&sst.smallest).cmp(&start))
        .unwrap_or_else(|not_found_pos| not_found_pos);
    let mut matched = vec![];
    for i in position..ssts.len() {
        let sst = &ssts[i];
        if !end.is_empty() && InnerKey::from_inner_buf(&sst.smallest) >= end {
            break;
        }
        matched.push(sst.clone())
    }
    matched
}

pub fn get_common_prefix(k1: &[u8], k2: &[u8]) -> Vec<u8> {
    let len = std::cmp::min(k1.len(), k2.len());
    let mut offset = len;
    for i in 0..len {
        if k1[i] != k2[i] {
            offset = i;
            break;
        }
    }
    k1[..offset].to_vec()
}

pub fn verify_regions_boundary(
    start_key: &[u8],
    end_key: &[u8],
    regions: &[pdpb::Region],
) -> Result<()> {
    if regions.is_empty() {
        return Err(box_err!("no region"));
    }

    let first_region = regions.first().unwrap();
    let last_region = regions.last().unwrap();
    if first_region.get_region().get_start_key() > start_key {
        return Err(Error::RegionsIntegrityError(format!(
            "unexpected start key of first region: {:?}, start_key: {:?}",
            first_region, start_key
        )));
    } else if last_region.get_region().get_end_key() < end_key {
        return Err(Error::RegionsIntegrityError(format!(
            "unexpected end key of last region: {:?}, end_key: {:?}",
            last_region, end_key
        )));
    }

    for region in regions.windows(2) {
        if region[0].get_region().get_end_key() != region[1].get_region().get_start_key() {
            return Err(Error::RegionsIntegrityError(format!(
                "region boundary not match: {:?}, {:?}",
                region[0], region[1]
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::codec::table::encode_row_key;

    use super::*;

    #[test]
    fn test_verify_regions_boundary() {
        let make_key = |key: u64| -> Vec<u8> { format!("k{:02}", key).into_bytes() };
        let make_region = |start: u64, end: u64| -> pdpb::Region {
            let mut region = metapb::Region::default();
            region.set_start_key(make_key(start));
            region.set_end_key(make_key(end));

            let mut pd_region = pdpb::Region::default();
            pd_region.set_region(region);
            pd_region
        };

        let regions1 = vec![
            make_region(1, 2),
            make_region(2, 4),
            make_region(4, 6),
            make_region(6, 10),
            make_region(10, 14),
        ];
        let regions2 = vec![make_region(1, 2), make_region(4, 6)];

        let cases = vec![
            (&regions1, 1, 14, true), // start, end, expect_is_ok
            (&regions1, 1, 2, true),
            (&regions1, 0, 2, false),
            (&regions1, 2, 15, false),
            (&regions2, 1, 6, false),
        ];
        for (idx, (regions, start, end, expect_is_ok)) in cases.into_iter().enumerate() {
            let res = verify_regions_boundary(&make_key(start), &make_key(end), regions);
            assert_eq!(res.is_ok(), expect_is_ok, "case {}: {:?}", idx, res);
        }
    }

    #[test]
    fn test_get_common_prefix() {
        let keys = [
            vec![b't', 128, 0, 0, 0, 0, 0, 0, 1, b'_', 1],
            vec![b't', 128, 0, 0, 0, 0, 0, 0, 1, b'_', 2],
            vec![b't', 128, 0, 0, 0, 0, 0, 0, 1, b'_', 3],
        ];
        let mut key_comm_prefix = keys[0].clone();

        for key in keys.iter() {
            key_comm_prefix = get_common_prefix(&key_comm_prefix, key);
        }

        let target_key_comm_prefix = vec![b't', 128, 0, 0, 0, 0, 0, 0, 1, b'_'];
        assert_eq!(key_comm_prefix, target_key_comm_prefix);
    }

    #[test]
    fn test_calculate_bound_key() {
        let cases: Vec<(&[u8], &[u8], Option<(Vec<u8>, Vec<u8>)>)> = vec![
            (b"t00000001_r00000001", b"t00000002_r00000002", None),
            (b"t00000001_i00000004", b"t00000001_r00000001", None),
            (b"t00000001_i00000", b"t00000001_i00000004aaa", None),
            (b"t00000001_i00000003aaa", b"t00000001_i00000004aaa", None),
            (
                b"t00000001_r",
                b"t00000001_r",
                Some((b"t00000001".to_vec(), b"t00000002".to_vec())),
            ),
            (
                b"t00000001_i00000003aba",
                b"t00000001_i00000003abc",
                Some((
                    b"t00000001_i00000003".to_vec(),
                    b"t00000001_i00000004".to_vec(),
                )),
            ),
        ];
        for (start, end, expected) in cases {
            let start = start.to_vec();
            let end = end.to_vec();
            assert_eq!(calculate_bound_key(&start, &end), expected);
        }
    }

    fn make_sst(table_id: i64, smallest: i64, biggest: i64) -> SstMeta {
        SstMeta {
            id: (table_id * 1000 + smallest) as u64,
            smallest: encode_row_key(table_id, smallest),
            biggest: encode_row_key(table_id, biggest),
            size: 100,
            meta_offset: 10,
            uncompressed_size: 0,
            keys: 10,
        }
    }

    #[test]
    fn test_group_ssts_by_table_id() {
        let ssts = [
            make_sst(1, 1, 2),
            make_sst(1, 3, 4),
            make_sst(2, 1, 2),
            make_sst(2, 3, 4),
            make_sst(2, 5, 6),
            make_sst(5, 3, 4),
            make_sst(6, 5, 6),
            make_sst(6, 7, 8),
        ];
        let groups = group_ssts_by_table_id(&ssts);
        assert_eq!(groups.len(), 4);
        assert_eq!(groups[0].len(), 2);
        assert_eq!(groups[1].len(), 3);
        assert_eq!(groups[2].len(), 1);
        assert_eq!(groups[3].len(), 2);
    }
}
