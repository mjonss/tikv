// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::VecDeque,
    convert::TryInto,
    fmt::Debug,
    fs,
    io::{Read, Seek, SeekFrom},
    mem,
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use fail::fail_point;
use kvengine::dfs::Dfs;
use protobuf::Message;
use rfenginepb::StoreBackupMeta;
use slog_global::*;
use tikv_util::{
    errors::{Context as _, IoError},
    mpsc::{Receiver, Sender},
};
use tokio::task::JoinHandle;

use crate::{
    Error, MAX_EPOCH_BACKWARD, Result, WalChunkMeta,
    compact_worker::CompactTask,
    compress_lz4, decompress_lz4, decompress_lz4_to_buffer,
    engine::get_delayed_to_epoch_id,
    find_latest_snapshot, get_integral_wal_chunks, get_lz4_decompressed_size,
    last_wal_chunk_file_key,
    manifest::Manifest,
    metrics::{self, RFENGINE_DFS_WORKER_HEALTHY_GAUGE},
    parse_delayed_to_epoch_from_snapshot_key, snapshot_store_meta_key, wal_chunk_file_key,
    wal_chunk_file_prefix, wal_file_name,
    writer::EPOCH_SNAPSHOT_LEN,
};

#[derive(Debug)]
pub(crate) struct LightweightBackupConfig {
    pub(crate) dir: PathBuf,
    pub(crate) wal_chunk_target_file_size: usize,
    pub(crate) compression_type: CompressionType,
    // for upgrade compatibility, set rlog_compression_type to false first
    // so we need another compress configuration.
    pub(crate) rlog_compression_type: CompressionType,

    pub(crate) rlog_cache_capacity: usize,
    pub(crate) rlog_cache_size_threshold: usize,
    pub(crate) memory_limit: usize,
}

impl LightweightBackupConfig {
    pub(crate) fn new(
        dir: PathBuf,
        wal_chunk_target_file_size: usize,
        compression_type: CompressionType,
        rlog_compression_type: CompressionType,
        rlog_cache_capacity: usize,
        rlog_cache_size_threshold: usize,
        memory_limit: usize,
    ) -> Self {
        Self {
            dir,
            wal_chunk_target_file_size,
            compression_type,
            rlog_compression_type,
            rlog_cache_capacity,
            rlog_cache_size_threshold,
            memory_limit,
        }
    }
}

struct BackgroundWal {
    chunk: WalChunkMeta,
    join_handle: JoinHandle<Result<()>>,
}

struct BackgroundSnapshot {
    delayed_to_epoch_id: u32,
    join_handle: JoinHandle<Result<()>>,
}

enum SnapshotState {
    Idle,
    Preparing,
    WaitingWal(PreparedSnapshot),
    Persisting(BackgroundSnapshot),
}

impl SnapshotState {
    fn get_delayed_to_epoch(&self) -> Option<u32> {
        match self {
            SnapshotState::WaitingWal(prepared) => Some(prepared.get_delayed_to_epoch()),
            _ => None,
        }
    }

    fn take_prepared_snapshot(&mut self) -> PreparedSnapshot {
        match mem::replace(self, SnapshotState::Idle) {
            SnapshotState::WaitingWal(prepared) => prepared,
            _ => unreachable!(),
        }
    }

    fn is_persisting_finished(&self) -> Option<bool> {
        match self {
            SnapshotState::Persisting(background_snapshot) => {
                Some(background_snapshot.join_handle.is_finished())
            }
            _ => None,
        }
    }

    fn try_take_background_snapshot(&mut self) -> Option<BackgroundSnapshot> {
        if !matches!(self, SnapshotState::Persisting(_)) {
            return None;
        }
        match mem::replace(self, SnapshotState::Idle) {
            SnapshotState::Persisting(background_snapshot) => Some(background_snapshot),
            _ => unreachable!(),
        }
    }
}

#[cfg(feature = "failpoints")]
static DFS_WORKER_FAILPOINT_TARGET_STORE_ID: AtomicU64 = AtomicU64::new(0);

// To ensure that no more than one store becomes unhealthy in tests.
// Currently we can only tolerate one unhealthy store.
#[cfg(feature = "failpoints")]
pub fn set_dfs_worker_failpoint_target_store_id(store_id: u64) {
    DFS_WORKER_FAILPOINT_TARGET_STORE_ID.store(store_id, Ordering::Release);
}

#[cfg(feature = "failpoints")]
fn failpoint_target_matches_store(store_id: u64) -> bool {
    let target = DFS_WORKER_FAILPOINT_TARGET_STORE_ID.load(Ordering::Acquire);
    target == 0 || target == store_id
}

#[cfg(not(feature = "failpoints"))]
#[allow(dead_code)]
#[inline]
fn failpoint_target_matches_store(_: u64) -> bool {
    false
}

pub(crate) struct ObjectStorageWorker {
    config: LightweightBackupConfig,
    engine_id: Arc<AtomicU64>,
    task_rx: Receiver<ObjectStorageTask>,
    compact_worker_tx: Sender<CompactTask>,
    service_worker_epoch: Arc<AtomicU32>,
    buf: Vec<u8>,
    async_wal_file: Option<fs::File>,
    epoch_id: u32,
    epoch_rotate_len: usize,
    start_off: u64, // The start offset of the current chunk.
    sync_off: u64,  // The offset of the syncing of current wal.
    dfs: Arc<dyn Dfs>,
    healthy: Healthy,
    memory_limiter: MemoryLimiter,
    background_uploads: VecDeque<BackgroundWal>,
    upload_results: VecDeque<(WalChunkMeta, bool)>,
    snapshot_state: SnapshotState,
    persisted_snap_delayed_to_epoch: u32,

    // When dfs worker failed to persist chunk, the healthy would be set to false and it will
    // not be recovered until next success snapshot, so we skip the epoch before the next
    // snapshot.
    skip_sync_before_epoch: u32,
}

impl ObjectStorageWorker {
    fn reset(&mut self, epoch_id: u32) {
        self.epoch_id = epoch_id;
        self.buf.clear();
        self.async_wal_file = None;
        self.start_off = 0;
        self.sync_off = 0;
    }

    pub(crate) fn new(
        config: LightweightBackupConfig,
        dfs: Arc<dyn Dfs>,
        epoch_id: u32,
        epoch_rotate_len: usize,
        engine_id: Arc<AtomicU64>,
        dfs_worker_healthy: Healthy,
        task_rx: Receiver<ObjectStorageTask>,
        compact_worker_tx: Sender<CompactTask>,
        service_worker_epoch: Arc<AtomicU32>,
    ) -> Self {
        info!("dfs worker config: {:?}", config);
        let wal_chunk_target_file_size = config.wal_chunk_target_file_size;
        let memory_limiter = MemoryLimiter::new(config.memory_limit);
        Self {
            config,
            epoch_id,
            engine_id,
            task_rx,
            compact_worker_tx,
            service_worker_epoch,
            buf: Vec::with_capacity(wal_chunk_target_file_size),
            async_wal_file: None,
            epoch_rotate_len,
            start_off: 0,
            sync_off: 0,
            dfs,
            healthy: dfs_worker_healthy,
            memory_limiter,
            background_uploads: Default::default(),
            upload_results: Default::default(),
            snapshot_state: SnapshotState::Idle,
            persisted_snap_delayed_to_epoch: 0,
            skip_sync_before_epoch: 0,
        }
    }

    // `init` will rebuild the last wal chunk persistence states. If no wal chunk
    // found in the epoch range from `epoch_id - 3` to `epoch_id`, trigger an
    // instant rfengine snapshot.
    pub(crate) fn init(&mut self) -> Result<()> {
        // Wait for node bootstrapped.
        info!("dfs worker wait for store bootstrapped.");
        let store_id = self.wait_for_bootstrapped();
        debug_assert!(store_id > 0);
        info!("{}: dfs worker start init.", store_id);
        self.init_snapshot();
        if self.is_snap_lag_too_much() {
            return Err(Error::Dfs("snapshot lag too much".to_string()));
        }
        // We start to check WAL integrity from the persisted_snap_delayed_to_epoch + 1,
        // so if there is chunk missing and we not able to rebuild, the init
        // failed and the healthy would be remain false.
        let mut check_epoch = self.persisted_snap_delayed_to_epoch + 1;
        let store_id = self.get_engine_id();
        loop {
            if check_epoch >= self.epoch_id {
                return Ok(());
            }
            let scan_prefix = wal_chunk_file_prefix(store_id, check_epoch);
            info!(
                "{}: rebuild last wal chunk list chunks with prefix {}",
                store_id, scan_prefix
            );

            // Chunks in an epoch should be listed in one iterate.
            let (chunks, has_more) = self.dfs.list_objects("", Some(&scan_prefix), None)?;
            debug_assert_eq!(has_more, None);
            let chunk_metas: Vec<WalChunkMeta> = chunks
                .into_iter()
                .filter_map(|x| {
                    x.key
                        .try_into()
                        .map_err(|e| warn!("skip invalid WAL chunk: {:?}", e))
                        .ok()
                })
                .collect();
            let (integral_chunks, last_end_off, has_last) =
                get_integral_wal_chunks(&chunk_metas, check_epoch);
            for chunk in integral_chunks {
                self.upload_results.push_back((chunk, true));
            }
            if has_last {
                check_epoch += 1;
                continue;
            }
            if check_epoch == self.epoch_id {
                // It is the latest epoch, it is expected that has_last is false.
                self.start_off = last_end_off;
                self.sync_off = last_end_off;
                return Ok(());
            }
            // Now the wal chunk of the current epoch is incomplete.
            if check_epoch < self.near_overwritten_epoch() {
                return Err(Error::Dfs("chunk lag too much".to_string()));
            }
            // By reset the epoch to an earlier one, we may rewrite the already uploaded
            // WAL epoch files, but as we handle the overlap in get_integral_wal_chunks,
            // it is ok.
            self.reset(check_epoch);
            return Ok(());
        }
    }

    fn need_snapshot_after_init(&self) -> bool {
        self.persisted_snap_delayed_to_epoch + EPOCH_SNAPSHOT_LEN <= self.epoch_id
    }

    fn init_snapshot(&mut self) {
        let prefix = self.dfs.get_prefix();
        let store_id = self.get_engine_id();
        match find_latest_snapshot(self.dfs.clone(), &prefix, store_id, self.epoch_id) {
            Ok(snap_key) => {
                if let Some(snap_delayed_to_epoch) =
                    parse_delayed_to_epoch_from_snapshot_key(snap_key.as_deref())
                {
                    self.persisted_snap_delayed_to_epoch = snap_delayed_to_epoch;
                }
            }
            Err(err) => {
                error!("{}: failed to find snapshot key: {}", store_id, err);
            }
        }
    }

    fn wait_for_bootstrapped(&self) -> u64 {
        let mut engine_id = self.get_engine_id();
        while engine_id == 0 {
            std::thread::sleep(std::time::Duration::from_millis(100));
            engine_id = self.get_engine_id();
        }
        engine_id
    }

    pub(crate) fn run(&mut self) {
        match self.init() {
            Ok(()) => {
                if self.need_snapshot_after_init() {
                    // Send task to compact worker to trigger a snapshot.
                    self.compact_worker_tx
                        .send(CompactTask::PrepareSnapshot)
                        .unwrap();
                    self.snapshot_state = SnapshotState::Preparing;
                }
                info!("dfs worker init ok, set healthy");
                self.healthy.set_healthy();
            }
            Err(err) => {
                error!("dfs worker init failed, keep unhealthy"; "err" => ?err);
                // calling set_unhealthy to update metrics to trigger alarm.
                self.healthy.set_unhealthy();
                // trigger snapshot to recover the healthy.
                self.compact_worker_tx
                    .send(CompactTask::PrepareSnapshot)
                    .unwrap();
                self.snapshot_state = SnapshotState::Preparing;
            }
        }
        loop {
            let recv_res = self.task_rx.recv_timeout(Duration::from_millis(100));
            self.try_wait_uploads();
            self.try_wait_snap_and_recover_healthy();
            let task = match recv_res {
                Ok(task) => task,
                Err(err) => {
                    if err.is_timeout() {
                        continue;
                    }
                    info!("ObjectStorageWorker recv error {:?}", err);
                    return;
                }
            };
            if let ObjectStorageTask::Close = task {
                info!("ObjectStorageWorker close");
                return;
            }
            match task {
                ObjectStorageTask::Sync { epoch_id, file_off } => {
                    if let Err(err) = self.handle_sync(epoch_id, file_off) {
                        error!("dfs worker handle_sync failed, set unhealthy"; "err" => ?err);
                        self.set_unhealthy(epoch_id);
                    }
                }
                ObjectStorageTask::Rotate { epoch_id, file_off } => {
                    if self.need_sync_on_rotate(epoch_id, file_off) {
                        info!("{} dfs worker need sync before rotate", self.get_engine_id();
                            "current_epoch" => self.epoch_id, "current_sync_off" => self.sync_off,
                            "epoch_id" => epoch_id, "file_off" => file_off,
                        );
                        if let Err(err) = self.handle_sync(epoch_id, file_off) {
                            error!("dfs worker handle_sync failed, set unhealthy"; "err" => ?err);
                            self.set_unhealthy(epoch_id);
                            // always update epoch_id on rotate, so the epoch_id never fall
                            // behind and we never rebuild_wal after first sync.
                            self.reset(epoch_id + 1);
                            return;
                        }
                    }
                    self.handle_rotate(epoch_id);
                }
                ObjectStorageTask::Flush => {
                    // Send flush task before close in normal case. If close without flush, we can
                    // construct the case for wal chunk recovery in random test.
                    if !self.buf.is_empty() {
                        self.next_chunk(false);
                    }
                    self.wait_uploads();
                    self.wait_snap_and_recover_healthy();
                }
                ObjectStorageTask::Snapshot(res) => match res {
                    Ok(prepared_snap) => {
                        self.handle_snapshot(prepared_snap);
                    }
                    Err(err) => {
                        error!("dfs worker prepare snapshot failed, set unhealthy"; "err" => ?err);
                        self.set_unhealthy(self.epoch_id);
                        self.snapshot_state = SnapshotState::Idle;
                    }
                },
                ObjectStorageTask::Close => unreachable!(),
            }
        }
    }

    fn try_wait_uploads(&mut self) {
        while let Some(upload) = self.background_uploads.pop_front() {
            if !upload.join_handle.is_finished() {
                self.background_uploads.push_front(upload);
                return;
            }
            self.wait_upload(upload);
        }
    }

    fn wait_upload(&mut self, upload: BackgroundWal) {
        let join_res = self.dfs.get_runtime().block_on(upload.join_handle);
        match join_res {
            Ok(Ok(())) => {
                self.upload_results.push_back((upload.chunk, true));
                if let Some(delayed_to_epoch) = self.snapshot_state.get_delayed_to_epoch() {
                    // Try to wake waiting prepared snapshot.
                    let uploading_wal_epoch = self.uploading_wal_epoch().unwrap_or(u32::MAX);
                    if delayed_to_epoch < uploading_wal_epoch {
                        let prepared = self.snapshot_state.take_prepared_snapshot();
                        self.persist_snapshot(prepared);
                    }
                }
            }
            res => {
                error!(
                    "{} dfs worker put WAL chunk {:?} failed, error {:?}",
                    self.get_engine_id(),
                    upload.chunk,
                    res,
                );
                self.set_unhealthy(upload.chunk.epoch);
                self.upload_results.push_back((upload.chunk, false));
                if let Some(delayed_to_epoch) = self.snapshot_state.get_delayed_to_epoch() {
                    error!(
                        "{}: discard waiting snapshot delayed to wal {}",
                        self.get_engine_id(),
                        delayed_to_epoch,
                    );
                    self.snapshot_state = SnapshotState::Idle;
                }
            }
        }
    }

    fn uploading_wal_epoch(&self) -> Option<u32> {
        self.background_uploads.front().map(|w| w.chunk.epoch)
    }

    fn set_unhealthy(&mut self, failed_epoch: u32) {
        let next_snap_epoch = Manifest::next_snapshot_epoch(failed_epoch);
        if self.skip_sync_before_epoch < next_snap_epoch + 1 {
            self.skip_sync_before_epoch = next_snap_epoch + 1;
        }
        self.healthy.set_unhealthy();
        fail_point!(
            "dfs_worker_set_unhealthy",
            failpoint_target_matches_store(self.get_engine_id()),
            |_| {}
        );
    }

    fn try_wait_snap_and_recover_healthy(&mut self) {
        if self.snapshot_state.is_persisting_finished() != Some(true) {
            return;
        }
        self.wait_snap_and_recover_healthy();
    }

    fn wait_snap_and_recover_healthy(&mut self) {
        let Some(snap) = self.snapshot_state.try_take_background_snapshot() else {
            return;
        };
        let join_res = self.dfs.get_runtime().block_on(snap.join_handle);
        match join_res {
            Ok(Ok(())) => {
                info!(
                    "{} dfs worker joined snapshot recovery",
                    self.get_engine_id()
                );
                self.persisted_snap_delayed_to_epoch = snap.delayed_to_epoch_id;
                if !self.healthy.is_healthy() {
                    for (chunk, success) in self.upload_results.iter() {
                        if !success {
                            warn!(
                                "{} dfs worker unable to recover healthy by snapshot {} as chunk {:?} failed",
                                self.get_engine_id(),
                                snap.delayed_to_epoch_id,
                                chunk,
                            );
                            return;
                        }
                    }
                    info!(
                        "{} dfs worker recovered healthy by snapshot {}",
                        self.get_engine_id(),
                        snap.delayed_to_epoch_id
                    );
                    self.healthy.set_healthy();
                    fail_point!(
                        "dfs_worker_recover_healthy",
                        failpoint_target_matches_store(self.get_engine_id()),
                        |_| {}
                    );
                }
            }
            res => {
                error!(
                    "dfs worker put snapshot {} failed, error {:?}",
                    snap.delayed_to_epoch_id, res
                );
                if self.is_snap_lag_too_much() {
                    self.set_unhealthy(snap.delayed_to_epoch_id);
                }
            }
        }
    }

    // Write wal chunk from `start_off` to end of current epoch in a single write.
    fn rebuild_wal_chunk(&mut self) -> Result<()> {
        let store_id = self.get_engine_id();
        let mut fd = fs::File::open(wal_file_name(
            self.config.dir.as_path(),
            self.epoch_id,
            self.epoch_rotate_len,
        ))?;
        if self.start_off > 0 {
            fd.seek(SeekFrom::Start(self.start_off))?;
        }
        let sync_len = fd
            .read_to_end(&mut self.buf)
            .ctx("rebuild_wal_chunk_read_wal")?;
        self.check_overwritten_epoch("rebuild_wal_chunk")?;

        self.sync_off = self.start_off + sync_len as u64;
        info!("{}: rebuild_wal_chunk", store_id; "epoch" => self.epoch_id,
            "start_off" => self.start_off, "sync_off" => self.sync_off);
        let file_key =
            last_wal_chunk_file_key(store_id, self.epoch_id, self.start_off, self.sync_off);
        let chunk = self.take_chunk_data()?;
        let wal_chunk = self.new_wal_chunk(true);
        let fs = self.dfs.clone();
        let mut mem_limiter = self.memory_limiter.clone();
        let handle = self.dfs.get_runtime().spawn_blocking(move || {
            let _acquired = mem_limiter.acquire(chunk.len())?;
            metrics::RFENGINE_DFS_RUNNING_UPLOADS.inc();
            let res = fs
                .put_objects(vec![(file_key, Bytes::from(chunk))])
                .map_err(|err| Error::Dfs(err));
            metrics::RFENGINE_DFS_RUNNING_UPLOADS.dec();
            res
        });
        let bg_upload = BackgroundWal {
            chunk: wal_chunk,
            join_handle: handle,
        };
        self.background_uploads.push_back(bg_upload);
        Ok(())
    }

    fn need_sync_on_rotate(&self, epoch_id: u32, file_off: u64) -> bool {
        if self.skip_sync(epoch_id) {
            return false;
        }
        self.epoch_id < epoch_id || (self.epoch_id == epoch_id && self.sync_off < file_off)
    }

    fn skip_sync(&self, epoch_id: u32) -> bool {
        if epoch_id < self.skip_sync_before_epoch {
            debug_assert!(
                !self.healthy.is_healthy(),
                "{} skip sync epoch {}, skip_sync_before {}",
                self.get_engine_id(),
                epoch_id,
                self.skip_sync_before_epoch,
            );
            return true;
        }
        false
    }

    fn handle_sync(&mut self, epoch_id: u32, file_off: u64) -> Result<()> {
        if self.skip_sync(epoch_id) {
            return Ok(());
        }
        let store_id = self.get_engine_id();

        if (epoch_id, file_off) < (self.epoch_id, self.sync_off) {
            // Happens when in-place restore TiKV store.
            // It's not safe to overwrite the existed remote WAL chunks. Return error and
            // set unhealthy, then wait for next snapshot to become healthy.
            error!("{}: handle_sync for early data", store_id;
                "epoch_id" => epoch_id, "file_off" => file_off,
                "self.epoch_id" => self.epoch_id, "start_off" => self.start_off, "sync_off" => self.sync_off,
            );
            debug_assert!(false);
            return Err(Error::Other("handle_sync for early data".to_string()));
        }

        if epoch_id != self.epoch_id {
            // If epoch is overwritten, the WAL chunks in DFS must be incomplete.
            // Return error to make unhealthy.
            self.check_overwritten_epoch("handle_sync")?;

            // Need to rebuild all previous epoch.
            for rebuild_epoch in self.epoch_id..epoch_id {
                self.rebuild_wal_chunk()?;
                self.reset(rebuild_epoch + 1);
            }
        }
        assert_eq!(epoch_id, self.epoch_id);
        assert!(file_off >= self.sync_off);
        if file_off == self.sync_off {
            // Should not happen, but not a fatal error. Allow for safety.
            warn!("{}: handle_sync: no new data", store_id;
                "epoch_id" => epoch_id, "file_off" => file_off,
                "self.epoch_id" => self.epoch_id, "start_off" => self.start_off, "sync_off" => self.sync_off,
            );
            debug_assert!(false);
        }

        let sync_len = file_off - self.sync_off;

        if self.should_chunk(sync_len as usize) {
            if let Some(async_wal_file) = &self.async_wal_file {
                // sync data before write to S3, to avoid S3 file ahead of async local file
                // after restart.
                async_wal_file.sync_data()?;
            }
            info!(
                "{}: handle_sync put wal epoch {} start_off {} sync_off {}",
                store_id, epoch_id, self.start_off, self.sync_off
            );
            self.next_chunk(false);
        }

        // Sync WAL of `epoch_id` from `self.sync_off` to file_off
        let async_wal_file = match self.async_wal_file {
            Some(ref mut fd) => fd,
            None => {
                let filename =
                    wal_file_name(self.config.dir.as_path(), epoch_id, self.epoch_rotate_len);
                let fd = fs::File::open(filename)?;
                self.async_wal_file = Some(fd);
                self.async_wal_file.as_mut().unwrap()
            }
        };

        let buf_start = self.buf.len();
        let buf_end = buf_start + sync_len as usize;
        debug!(
            "{}: handle_sync epoch {} from {} to {} buf_start {} buf_end {}",
            store_id, epoch_id, self.sync_off, file_off, buf_start, buf_end
        );
        self.buf.resize(buf_end, 0);
        async_wal_file
            .read_exact_at(&mut self.buf[buf_start..buf_end], self.sync_off)
            .ctx("handle_sync_read_async_wal")?;
        self.check_overwritten_epoch("handle_sync")?;

        // Update the sync offset.
        self.sync_off = file_off;
        Ok(())
    }

    // The epoch <= `overwritten_epoch` is overwritten and should not read.
    #[inline]
    fn overwritten_epoch(&self) -> u32 {
        self.service_worker_epoch
            .load(Ordering::SeqCst)
            .saturating_sub(self.epoch_rotate_len as u32)
    }

    // `service_worker_epoch - 3` (`self.epoch_rotate_len - 1` == 3) is the cut-off
    // value and would be overwritten soon.
    // So `service_worker_epoch - 2` is used.
    #[inline]
    fn near_overwritten_epoch(&self) -> u32 {
        self.service_worker_epoch
            .load(Ordering::SeqCst)
            .saturating_sub(self.epoch_rotate_len as u32 - 2)
    }

    fn check_overwritten_epoch(&self, ctx: &str) -> Result<()> {
        let overwritten_epoch = self.overwritten_epoch();
        if self.epoch_id <= overwritten_epoch {
            error!("{}: {}: epoch is overwritten", self.get_engine_id(), ctx;
                    "dfs_worker.epoch" => self.epoch_id, "overwritten_epoch" => overwritten_epoch);
            Err(Error::Other(format!("{ctx}: epoch is overwritten")))
        } else {
            Ok(())
        }
    }

    fn handle_rotate(&mut self, epoch_id: u32) {
        debug!("{}: handle_rotate epoch {}", self.get_engine_id(), epoch_id);
        if self.skip_sync_before_epoch <= epoch_id {
            // Call next_chunk even self.buf is empty. This can cover the case the last
            // chunk flushed during stop with no `.last` suffix.
            self.next_chunk(true);
        }
        self.reset(epoch_id + 1);
        if epoch_id % EPOCH_SNAPSHOT_LEN == 0 {
            if !matches!(self.snapshot_state, SnapshotState::Idle) {
                warn!(
                    "{}: snapshot state is not idle, skip snap epoch {}",
                    self.get_engine_id(),
                    epoch_id
                );
                return;
            }
            let _ = self.compact_worker_tx.send(CompactTask::PrepareSnapshot);
            self.snapshot_state = SnapshotState::Preparing;
        }
    }

    fn is_snap_lag_too_much(&self) -> bool {
        self.persisted_snap_delayed_to_epoch + MAX_EPOCH_BACKWARD <= self.epoch_id
    }

    fn handle_snapshot(&mut self, prepared_snap: PreparedSnapshot) {
        let uploading_wal_epoch = self.uploading_wal_epoch().unwrap_or(u32::MAX);
        if prepared_snap.get_delayed_to_epoch() >= uploading_wal_epoch {
            // wait for the delayed wal epoches to finish upload to eliminate the
            // possibility that the delayed wal may fail.
            self.snapshot_state = SnapshotState::WaitingWal(prepared_snap);
            return;
        }
        self.persist_snapshot(prepared_snap);
    }

    fn persist_snapshot(&mut self, prepared_snap: PreparedSnapshot) {
        let engine_id = self.get_engine_id();
        let epoch_id = prepared_snap.store_meta.epoch;
        self.upload_results.retain(|(wal, _)| wal.epoch > epoch_id);
        // Make sure all the delayed wal chunks has been successfully written to
        // Dfs So we can be sure the snapshot is valid.
        let delayed_to_epoch_id = prepared_snap.get_delayed_to_epoch();
        let all_delayed_epoch_persisted = self
            .upload_results
            .iter()
            .all(|(wal, ok)| *ok || wal.epoch > delayed_to_epoch_id);
        if !all_delayed_epoch_persisted {
            debug_assert!(!self.healthy.is_healthy());
            error!(
                "{}: skip persist snapshot as delayed wal failed to upload",
                engine_id
            );
            self.snapshot_state = SnapshotState::Idle;
            return;
        }
        // Also need snapshot backup_meta.
        // Use delayed_to_epoch_id
        let meta_key = snapshot_store_meta_key(engine_id, delayed_to_epoch_id);
        let meta_data = prepared_snap.store_meta.write_to_bytes().unwrap();
        let meta_obj = (meta_key, Bytes::from(meta_data));

        // `rlog_obj` should be written to DFS at the end, as we scan for latest
        // snapshot by the rlog object.
        // See https://github.com/tidbcloud/cloud-storage-engine/issues/1840.
        let dfs = self.dfs.clone();
        let join_handle: JoinHandle<Result<()>> = self.dfs.get_runtime().spawn_blocking(move || {
                for obj in [meta_obj, prepared_snap.rlog_obj] {
                    if let Err(err) = dfs.put_objects(vec![obj]) {
                        error!("{} put snapshot object failed", engine_id; "err" => ?err, "epoch" => epoch_id);
                        return Err(Error::Dfs(err));
                    }
                }
                Ok(())
            });
        self.snapshot_state = SnapshotState::Persisting(BackgroundSnapshot {
            delayed_to_epoch_id,
            join_handle,
        });
    }

    pub(crate) fn should_chunk(&mut self, to_read: usize) -> bool {
        if !self.buf.is_empty()
            && self.buf.len() + ChunkHeader::len() + to_read
                > self.config.wal_chunk_target_file_size
        {
            return true;
        }
        false
    }

    #[cfg(test)]
    pub(crate) fn set_buf(&mut self, buf: Vec<u8>) {
        self.buf = buf;
        self.sync_off = self.start_off + self.buf.len() as u64;
    }

    pub(crate) fn take_chunk_data(&mut self) -> Result<Vec<u8>> {
        debug_assert_eq!(self.buf.len() as u64, self.sync_off - self.start_off);

        let chunk_header = ChunkHeader::new(self.compression_type());
        let buf_len = self.buf.len();
        let mut chunk = Vec::with_capacity(ChunkHeader::len() + buf_len);
        chunk_header.encode_to(&mut chunk);
        // Get slice of the buffer but keep the memory.
        let res = if self.need_compression() {
            compress_lz4(&self.buf, &mut chunk).map(|_| chunk).map_err(|err| {
                error!("{} take chunk data: compress_lz4 failed", self.get_engine_id(); "err" => ?err);
                Error::Io(IoError::new(err, "compress_chunk".to_string()))
            })
        } else {
            chunk.extend_from_slice(&self.buf);
            Ok(chunk)
        };

        // Always clear the buf. Otherwise, when error occurs, chunks of different epoch
        // will be combined.
        self.buf.clear();
        self.start_off = self.sync_off;
        res
    }

    /// Wait all background uploads to finish.
    fn wait_uploads(&mut self) {
        std::mem::take(&mut self.background_uploads)
            .into_iter()
            .for_each(|upload| self.wait_upload(upload));
    }

    fn new_wal_chunk(&self, rotate: bool) -> WalChunkMeta {
        let store_id = self.get_engine_id();
        let key = if rotate {
            last_wal_chunk_file_key(store_id, self.epoch_id, self.start_off, self.sync_off)
        } else {
            wal_chunk_file_key(store_id, self.epoch_id, self.start_off, self.sync_off)
        };
        WalChunkMeta {
            key,
            epoch: self.epoch_id,
            start_off: self.start_off,
            end_off: self.sync_off,
            last: rotate,
        }
    }

    fn next_chunk(&mut self, rotate: bool) {
        let store_id = self.get_engine_id();
        let wal_chunk = self.new_wal_chunk(rotate);
        let file_key = wal_chunk.key.clone();
        let buf_len = self.buf.len();
        let chunk_res = self.take_chunk_data();
        // chunk_res return only compress_lz4 error which is very unlikely.
        // So we keep the the error handling logic in one place for simplicity.
        let fs = self.dfs.clone();
        let mut mem_limiter = self.memory_limiter.clone();
        let handle = {
            self.dfs.get_runtime().spawn_blocking(move || {
                let chunk = chunk_res?;
                info!(
                    "{}: put wal chunk {} len {} compress len {}",
                    store_id,
                    file_key,
                    ChunkHeader::len() + buf_len,
                    chunk.len()
                );
                let _acquired = mem_limiter.acquire(chunk.len())?;
                fail_point!(
                    "dfs_worker_put_wal_chunk_error",
                    failpoint_target_matches_store(store_id),
                    |_| Err(Error::Dfs("dfs_worker_put_wal_chunk_error".to_string()))
                );
                metrics::RFENGINE_DFS_RUNNING_UPLOADS.inc();
                let res = fs
                    .put_objects(vec![(file_key, Bytes::from(chunk))])
                    .map_err(|err| Error::Dfs(err));
                metrics::RFENGINE_DFS_RUNNING_UPLOADS.dec();
                res
            })
        };
        let bg_upload = BackgroundWal {
            chunk: wal_chunk,
            join_handle: handle,
        };
        self.background_uploads.push_back(bg_upload);
    }

    fn need_compression(&self) -> bool {
        self.config.compression_type != CompressionType::NoCompression
    }

    fn compression_type(&self) -> CompressionType {
        self.config.compression_type
    }

    fn get_engine_id(&self) -> u64 {
        self.engine_id.load(Ordering::Acquire)
    }
}

// More data will be appended to epoch_wal after call this, so return BytesMut.
pub fn assemble_wal_chunks(chunks: Vec<Bytes>) -> Result<BytesMut> {
    let mut epoch_wal = BytesMut::new();
    for chunk in chunks.into_iter() {
        epoch_wal.put(decompress_wal_chunk(&chunk)?);
    }
    Ok(epoch_wal)
}

pub fn decompress_wal_chunk(chunk: &Bytes) -> Result<Bytes> {
    // Read chunk header.
    let header = ChunkHeader::decode(chunk.slice(0..ChunkHeader::len()).chunk())?;
    let decompressed_data = match header.compression_type {
        CompressionType::Lz4Compression => {
            Bytes::from(decompress_lz4(chunk.slice(ChunkHeader::len()..).chunk())?)
        }
        CompressionType::NoCompression => chunk.slice(ChunkHeader::len()..),
    };
    Ok(decompressed_data)
}

pub fn convert_to_decompressed_wal_chunk(chunk: Bytes) -> Result<Bytes> {
    let header = ChunkHeader::decode(chunk.slice(0..ChunkHeader::len()).chunk())?;
    match header.compression_type {
        CompressionType::Lz4Compression => {
            let src = chunk.slice(ChunkHeader::len()..);

            let decompressed_size = get_lz4_decompressed_size(&src).ctx("get_decompressed_size")?;
            let cap = ChunkHeader::len() + decompressed_size;
            let mut buffer = Vec::with_capacity(cap);

            let chunk_header = ChunkHeader::new(CompressionType::NoCompression);
            chunk_header.encode_to(&mut buffer);
            buffer.resize(cap, 0);
            let size = decompress_lz4_to_buffer(&src, &mut buffer[ChunkHeader::len()..])
                .ctx("decompress_to_buffer")?;
            debug_assert_eq!(size, decompressed_size);

            Ok(Bytes::from(buffer))
        }
        CompressionType::NoCompression => Ok(chunk),
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(u32)]
pub enum CompressionType {
    NoCompression,
    Lz4Compression,
    // Add more compression types here.
}

impl CompressionType {
    pub fn from(v: u32) -> CompressionType {
        match v {
            0 => CompressionType::NoCompression,
            1 => CompressionType::Lz4Compression,
            _ => panic!("unknown compression type"),
        }
    }

    pub fn to(&self) -> u32 {
        match self {
            CompressionType::NoCompression => 0,
            CompressionType::Lz4Compression => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(u32)]
enum ChunkVersion {
    V1 = 1,
}

impl From<u32> for ChunkVersion {
    fn from(v: u32) -> ChunkVersion {
        match v {
            1 => ChunkVersion::V1,
            _ => panic!("unknown chunk version"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ChunkHeader {
    version: ChunkVersion,
    pub compression_type: CompressionType,
}

impl ChunkHeader {
    pub fn new(compression_type: CompressionType) -> Self {
        Self {
            version: ChunkVersion::V1,
            compression_type,
        }
    }

    pub const fn len() -> usize {
        8
    }

    pub fn encode_to(&self, buf: &mut Vec<u8>) {
        buf.put_u32_le(self.version as u32);
        buf.put_u32_le(self.compression_type as u32);
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self> {
        if buf.len() < Self::len() {
            return Err(Error::Corruption {
                msg: format!("chunk header mismatch: len {}", buf.len()),
                epoch_id: 0,
                offset: 0,
                data: buf.to_vec(),
            });
        }

        let version = ChunkVersion::from(buf.get_u32_le());
        if version != ChunkVersion::V1 {
            return Err(Error::Corruption {
                msg: format!("chunk version mismatch: version {:?}", version),
                epoch_id: 0,
                offset: 0,
                data: buf.to_vec(),
            });
        }
        let compression_type = CompressionType::from(buf.get_u32_le());
        Ok(Self {
            version,
            compression_type,
        })
    }
}

#[derive(Debug)]
pub(crate) enum ObjectStorageTask {
    Sync { epoch_id: u32, file_off: u64 }, // Sync the `epoch_id` wal file to `file_off`.
    Rotate { epoch_id: u32, file_off: u64 }, // Rotate to next epoch.
    Snapshot(Result<PreparedSnapshot>),
    Flush, // Trigger flush the last chunk, mainly for test.
    Close,
}

pub(crate) struct PreparedSnapshot {
    pub(crate) store_meta: StoreBackupMeta,
    pub(crate) rlog_obj: (String, Bytes),
}

impl PreparedSnapshot {
    fn get_delayed_to_epoch(&self) -> u32 {
        get_delayed_to_epoch_id(self.store_meta.get_manifest())
    }
}

impl Debug for PreparedSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PreparedSnapshot epoch {}", self.store_meta.epoch)
    }
}

/// Healthy is read by ServiceWorker on handle backup request.
/// On start up the default healthy is false until the DfsWorker init.
/// When dfs worker failed to persist the WAL chunk or snapshot, it is set to
/// unhealthy. It will be recovered to healthy after persisted a snapshot.
#[derive(Clone)]
pub(crate) struct Healthy(Arc<AtomicBool>);

impl Default for Healthy {
    fn default() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }
}

impl Healthy {
    pub(crate) fn set_healthy(&self) {
        RFENGINE_DFS_WORKER_HEALTHY_GAUGE.set(1);
        self.0.store(true, Ordering::Release);
    }

    pub(crate) fn set_unhealthy(&self) {
        RFENGINE_DFS_WORKER_HEALTHY_GAUGE.set(0);
        self.0.store(false, Ordering::Release);
        warn!("dfs worker unhealthy");
        #[cfg(feature = "testexport")]
        {
            crate::metrics::RFENGINE_DFS_WORKER_BECOME_UNHEALTHY_COUNTER.inc();
        }
    }

    pub(crate) fn is_healthy(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
struct MemoryLimiter {
    available: Arc<AtomicI64>, // Use i64 to avoid overflow.
}

impl MemoryLimiter {
    fn new(cap: usize) -> Self {
        Self {
            available: Arc::new(AtomicI64::new(cap as i64)),
        }
    }

    // Mutable ref to ensure that it's used in single threading-context. As the
    // get-and-set is not atomic.
    fn acquire(&mut self, request: usize) -> Result<MemoryLimiterGuard> {
        let available = self.available.load(Ordering::Acquire);
        if available >= request as i64 {
            self.available.fetch_sub(request as i64, Ordering::AcqRel);
            Ok(MemoryLimiterGuard {
                limiter: self.clone(),
                request,
            })
        } else {
            Err(Error::MemoryLimitExceed { request, available })
        }
    }

    fn release(&self, request: usize) {
        self.available.fetch_add(request as i64, Ordering::AcqRel);
    }
}

struct MemoryLimiterGuard {
    limiter: MemoryLimiter,
    request: usize,
}

impl Drop for MemoryLimiterGuard {
    fn drop(&mut self) {
        self.limiter.release(self.request);
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use kvengine::dfs::{DFSConfig, new_dfs_from_config};
    use rand::prelude::*;

    use super::*;

    #[test]
    fn test_chunk_header() {
        let header = ChunkHeader::new(CompressionType::Lz4Compression);
        let mut buf = Vec::with_capacity(ChunkHeader::len());
        header.encode_to(&mut buf);
        let header2 = ChunkHeader::decode(&buf).unwrap();
        assert_eq!(header, header2);
    }

    #[test]
    fn test_chunk() {
        let mut data = vec![0u8; 1024];
        thread_rng().fill_bytes(data.as_mut_slice());

        let chunk_compressed = make_wal_chunk(&data, CompressionType::Lz4Compression);
        assert_eq!(&decompress_wal_chunk(&chunk_compressed).unwrap(), &data);

        let chunk_no_compress = make_wal_chunk(&data, CompressionType::NoCompression);
        assert_eq!(&decompress_wal_chunk(&chunk_no_compress).unwrap(), &data);

        let chunk_no_compress1 = convert_to_decompressed_wal_chunk(chunk_compressed).unwrap();
        assert_eq!(chunk_no_compress1, chunk_no_compress);
        assert_eq!(decompress_wal_chunk(&chunk_no_compress1).unwrap(), data);
    }

    #[test]
    fn test_wal_chunk_integrity() {
        let (_, rx) = tikv_util::mpsc::unbounded();
        let (tx, _) = tikv_util::mpsc::unbounded();
        let dfs = new_dfs_from_config(DFSConfig::default());
        let service_worker_epoch = Arc::new(AtomicU32::new(0));
        let mut worker = ObjectStorageWorker::new(
            LightweightBackupConfig::new(
                std::env::temp_dir(),
                1024 * 1024,
                CompressionType::Lz4Compression,
                CompressionType::Lz4Compression,
                1024 * 1024,
                4096,
                1 << 20,
            ),
            dfs,
            1,
            4,
            Arc::new(AtomicU64::new(1)),
            Healthy::default(),
            rx,
            tx,
            service_worker_epoch,
        );

        let mut origin_data = vec![];
        let mut chunks_data = vec![];

        for _ in 0..10 {
            let buf = generate_random_bytes(1024 * 128);
            // Save buf to data first.
            origin_data.extend_from_slice(&buf);
            worker.set_buf(buf);
            let chunk = worker.take_chunk_data().unwrap();
            // Save chunk data to chunks_data.
            chunks_data.push(Bytes::from(chunk));
        }
        // Append empty chunk to chunks_data should not affect the result.
        worker.set_buf(vec![]);
        let chunk = worker.take_chunk_data().unwrap();
        chunks_data.push(Bytes::from(chunk));

        // Assemble chunk data and verify with origin data.
        let assembled_data = assemble_wal_chunks(chunks_data).unwrap();
        let assembled_data = assembled_data.to_vec();
        assert_eq!(origin_data, assembled_data);
    }

    #[test]
    fn test_overwritten_epoch() {
        let (_, rx) = tikv_util::mpsc::unbounded();
        let (tx, _) = tikv_util::mpsc::unbounded();
        let dfs = new_dfs_from_config(DFSConfig::default());
        let service_worker_epoch = Arc::new(AtomicU32::new(0));
        let worker = ObjectStorageWorker::new(
            LightweightBackupConfig::new(
                std::env::temp_dir(),
                1024 * 1024,
                CompressionType::Lz4Compression,
                CompressionType::Lz4Compression,
                1024 * 1024,
                4096,
                1 << 20,
            ),
            dfs,
            1,
            4,
            Arc::new(AtomicU64::new(1)),
            Healthy::default(),
            rx,
            tx,
            service_worker_epoch.clone(),
        );

        let cases = vec![
            (1, 0, 0),
            (2, 0, 0),
            (3, 0, 1),
            (4, 0, 2),
            (5, 1, 3),
            (6, 2, 4),
            (7, 3, 5),
            (u32::MAX, 0xffff_fffb, 0xffff_fffd),
        ];

        for (service_epoch_id, overwritten_epoch, near_overwritten_epoch) in cases {
            service_worker_epoch.store(service_epoch_id, Ordering::SeqCst);
            assert_eq!(worker.overwritten_epoch(), overwritten_epoch);
            assert_eq!(worker.near_overwritten_epoch(), near_overwritten_epoch);
        }
    }

    fn generate_random_bytes(size: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let mut random_bytes = Vec::with_capacity(size);

        for _ in 0..size {
            random_bytes.push(rng.gen::<u8>());
        }

        random_bytes
    }

    fn make_wal_chunk(buf: &[u8], compression_type: CompressionType) -> Bytes {
        let chunk_header = ChunkHeader::new(compression_type);
        let buf_len = buf.len();
        let mut chunk = Vec::with_capacity(ChunkHeader::len() + buf_len);
        chunk_header.encode_to(&mut chunk);
        if compression_type == CompressionType::Lz4Compression {
            compress_lz4(buf, &mut chunk).unwrap();
        } else {
            chunk.extend_from_slice(buf);
        };
        chunk.into()
    }
}
