// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(any(test, feature = "testexport"))]
use std::sync::atomic::AtomicBool;
use std::{
    fmt, fs,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    thread::JoinHandle,
};

use bytes::Bytes;
use file_system::IoRateLimiter;
use kvengine::dfs::Dfs;
use rfenginepb::StoreBackupMeta;
use slog_global::{error, info};
use tikv_util::{
    mpsc::{Receiver, SendError, Sender},
    sys::thread::StdThreadBuildWrapper,
    time::Instant,
    warn, DFS_WORKER_THREAD_NAME,
};

use crate::{
    compact_worker::{backup_callback, wal_file_name, CompactTask, CompactWorker, WorkerHandle},
    dfs_worker::{Healthy, LightweightBackupConfig, ObjectStorageTask, ObjectStorageWorker},
    log_batch::RaftLogBlock,
    manifest::Manifest,
    write_batch::PeerBatch,
    writer::WalWriter,
    BackupTask, Error, Result, RfEngineConfig,
};

pub(crate) struct ObjectStorageWorkerHandle {
    task_sender: Sender<ObjectStorageTask>,
    handle: JoinHandle<()>,
}

impl ObjectStorageWorkerHandle {
    fn try_send(&self, task: ObjectStorageTask) {
        if let Err(SendError(t)) = self.task_sender.send(task) {
            warn!("send task failed"; "task" => ?t);
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "kebab-case")] // Be uniform with status server interfaces.
pub struct WalProgress {
    pub epoch: u32,
    pub offset: u64,
}

pub(crate) enum ServiceTask {
    Dump {
        epoch_id: u32,
        start_off: u64,
        end_off: u64,
        callback: Box<dyn FnOnce(Result<(Bytes, bool)>) + Send>,
    },
    GetProgress {
        callback: Box<dyn FnOnce(Result<WalProgress>) + Send>,
    },
    Rotate {
        epoch_id: u32,
    },
    Write {
        wb: Arc<Vec<PeerBatch>>,
    },
    Backup(BackupTask),
    Truncates(Vec<Vec<RaftLogBlock>>),
    Upload,
    Close {
        force: bool,
    },
}

impl fmt::Debug for ServiceTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceTask::Dump { .. } => write!(f, "ServiceTask::Dump"),
            ServiceTask::GetProgress { .. } => write!(f, "ServiceTask::GetProgress"),
            ServiceTask::Rotate { .. } => write!(f, "ServiceTask::Rotate"),
            ServiceTask::Write { .. } => write!(f, "ServiceTask::Write"),
            ServiceTask::Backup(_) => write!(f, "ServiceTask::Backup"),
            ServiceTask::Truncates(_) => write!(f, "ServiceTask::Truncates"),
            ServiceTask::Upload => write!(f, "ServiceTask::Upload"),
            ServiceTask::Close { force } => write!(f, "ServiceTask::Close({})", force),
        }
    }
}

const ASYNC_WRITER_SYNC_SIZE: u64 = 256 * 1024;

/// Service worker maintains the async WAL files, provide read service for HTTP
/// API, compaction and backup tasks.
pub(crate) struct ServiceWorker {
    engine_id: Arc<AtomicU64>,
    epoch_id: Arc<AtomicU32>,
    async_wal_writer: Option<WalWriter>,
    bytes_since_last_sync: u64,
    rx: Receiver<ServiceTask>,
    compact_worker_handle: WorkerHandle,
    dfs_worker_handle: Option<ObjectStorageWorkerHandle>,
    dfs_worker_healthy: Healthy,
}

impl ServiceWorker {
    pub(crate) fn new(
        dir: PathBuf,
        cfg: &RfEngineConfig,
        async_wal_writer: Option<WalWriter>,
        rx: Receiver<ServiceTask>,
        manifest: Manifest,
        compacted_epoch: Arc<AtomicU32>,
        lightweight_backup: Option<(LightweightBackupConfig, Arc<dyn Dfs>)>,
        healthy: Healthy,
        compact_rate_limiter: Option<Arc<IoRateLimiter>>,
        #[cfg(any(test, feature = "testexport"))] compact_force_stop: Arc<AtomicBool>,
    ) -> Self {
        let engine_id = manifest.engine_id.clone();
        let epoch_id = manifest.epoch_id + 1;
        let (compact_worker_tx, compact_rx) = tikv_util::mpsc::unbounded();
        let (dfs_worker_tx, dfs_worker_rx) = tikv_util::mpsc::unbounded();
        let mut compact_worker = CompactWorker::new(
            dir.clone(),
            cfg,
            compact_rx,
            dfs_worker_tx.clone(),
            manifest,
            compacted_epoch.clone(),
            lightweight_backup.as_ref(),
            compact_rate_limiter,
            #[cfg(any(test, feature = "testexport"))]
            compact_force_stop,
        );
        let handle = std::thread::Builder::new()
            .name("compact-wal-worker".to_string())
            .spawn_wrapper(move || compact_worker.run())
            .unwrap();
        let compact_worker_handle = WorkerHandle {
            task_sender: compact_worker_tx.clone(),
            handle: Some(handle),
        };
        let epoch_rotate_len = cfg.epoch_rotate_len;

        let service_worker_epoch = Arc::new(AtomicU32::new(epoch_id));
        let dfs_worker_handle = if let Some((cfg, s3fs)) = lightweight_backup {
            let mut dfs_worker = ObjectStorageWorker::new(
                cfg,
                s3fs,
                epoch_id,
                epoch_rotate_len,
                engine_id.clone(),
                healthy.clone(),
                dfs_worker_rx,
                compact_worker_tx,
                service_worker_epoch.clone(),
            );
            let handle = std::thread::Builder::new()
                .name(DFS_WORKER_THREAD_NAME.to_string())
                .spawn_wrapper(move || dfs_worker.run())
                .unwrap();
            Some(ObjectStorageWorkerHandle {
                task_sender: dfs_worker_tx,
                handle,
            })
        } else {
            None
        };
        ServiceWorker {
            engine_id,
            epoch_id: service_worker_epoch,
            async_wal_writer,
            bytes_since_last_sync: 0,
            dfs_worker_handle,
            rx,
            compact_worker_handle,
            dfs_worker_healthy: healthy,
        }
    }

    pub(crate) fn run(&mut self) {
        while let Ok(task) = self.rx.recv() {
            match task {
                ServiceTask::Write { wb } => {
                    self.handle_write(&wb);
                }
                ServiceTask::Dump {
                    epoch_id,
                    start_off,
                    end_off,
                    callback,
                } => {
                    self.handle_dump(epoch_id, start_off, end_off, callback);
                }
                ServiceTask::GetProgress { callback } => {
                    self.handle_get_progress(callback);
                }
                ServiceTask::Backup(task) => {
                    self.handle_backup(task);
                }
                ServiceTask::Rotate { epoch_id } => {
                    self.handle_rotate(epoch_id);
                }
                ServiceTask::Truncates(truncates) => drop(truncates),
                ServiceTask::Upload => {
                    self.handle_flush();
                }
                ServiceTask::Close { force } => {
                    self.handle_close(force);
                    break;
                }
            }
        }
    }

    fn is_lightweight_enabled(&self) -> bool {
        self.dfs_worker_handle.is_some()
    }

    fn get_engine_id(&self) -> u64 {
        self.engine_id.load(Ordering::SeqCst)
    }

    fn handle_write(&mut self, wb: &[PeerBatch]) {
        if let Some(wal_writer) = &mut self.async_wal_writer {
            let old_file_off = wal_writer.file_off;
            let (epoch_id, file_off, rotated) = wal_writer.write_batch(wb).unwrap();
            if rotated {
                // When rotated, the old file is already synced.
                self.bytes_since_last_sync = 0;
            } else {
                self.bytes_since_last_sync += file_off - old_file_off;
            }
            if self.bytes_since_last_sync > ASYNC_WRITER_SYNC_SIZE {
                wal_writer.file().sync_data().unwrap();
                self.bytes_since_last_sync = 0;
            }
            if self.is_lightweight_enabled() {
                // Send write task to object storage worker.
                let task = crate::dfs_worker::ObjectStorageTask::Sync { epoch_id, file_off };
                self.dfs_worker_handle.as_ref().unwrap().try_send(task);
            }
        }
        let mut truncated_idxes = vec![];
        for batch in wb {
            if batch.truncated_idx > 0 {
                truncated_idxes.push((batch.peer_id, batch.truncated_idx));
            }
        }
        let _ = self
            .compact_worker_handle
            .task_sender
            .send(CompactTask::UpdateTruncatedIndexes { truncated_idxes });
    }

    /// `partial_content`:
    /// - When `end_off == 0`, `partial_content` is true means that there is
    ///   more epoches.
    /// - When `end_off > 0`, `partial_content` is true means that the epoch (of
    ///   `epoch_id`) has more content after `end_off`.
    fn handle_dump(
        &mut self,
        epoch_id: u32,
        start_off: u64,
        end_off: u64,
        callback: Box<dyn FnOnce(Result<(Bytes, bool /* partial_content */)>) + Send>,
    ) {
        let res = if let Some(writer) = self.async_wal_writer.as_ref() {
            let mut partial_content = false;
            let store_id = self.get_engine_id();
            // Check the WAL chunk meta is valid.
            if epoch_id > writer.epoch_id || (end_off > 0 && start_off >= end_off) {
                let msg = format!(
                    "{}: invalid dump wal chunk epoch {} start_off {} end_off {:?} writer epoch {}, file_off {}",
                    store_id, epoch_id, start_off, end_off, writer.epoch_id, writer.file_off
                );
                error!("{}", msg);
                callback(Err(Error::Other(msg)));
                return;
            } else if epoch_id + writer.epoch_rotate_len as u32 - 1 < writer.epoch_id {
                // The epoch_id is too old and has been overwritten.
                info!(
                    "{}: handle dump: epoch is overwritten: epoch {} writer epoch {}",
                    store_id, epoch_id, writer.epoch_id
                );
                callback(Err(Error::WalEpochOverwritten { epoch_id }));
                return;
            }
            // Dump the WAL chunk from offset start_off to end_off.
            #[allow(clippy::collapsible_else_if)]
            let effective_end_off = if end_off == 0 {
                if writer.epoch_id == epoch_id {
                    writer.file_off
                } else {
                    partial_content = true;
                    let wal_file_name =
                        wal_file_name(&writer.dir, epoch_id, writer.epoch_rotate_len);
                    let file_meta = fs::metadata(wal_file_name).unwrap();
                    file_meta.len()
                }
            } else {
                if writer.epoch_id == epoch_id {
                    debug_assert!(end_off <= writer.file_off);
                    partial_content = true;
                    end_off.min(writer.file_off)
                } else {
                    let wal_file_name =
                        wal_file_name(&writer.dir, epoch_id, writer.epoch_rotate_len);
                    let file_len = fs::metadata(wal_file_name).unwrap().len();
                    debug_assert!(end_off <= file_len);
                    partial_content = end_off < file_len;
                    end_off.min(file_len)
                }
            };
            info!(
                "{}: dump latest wal epoch {} start_off {} end_off {} effective_end_off {} writer epoch {}",
                store_id, epoch_id, start_off, end_off, effective_end_off, writer.epoch_id,
            );
            match dump_wal_chunk(
                &writer.dir,
                epoch_id,
                writer.epoch_rotate_len,
                start_off,
                effective_end_off,
            ) {
                Ok(chunk) => Ok((chunk, partial_content)),
                Err(err) => {
                    let msg = format!(
                        "{}: dump wal chunk epoch {} start_off {} end_off {} failed {:?}",
                        self.get_engine_id(),
                        epoch_id,
                        start_off,
                        effective_end_off,
                        err
                    );
                    error!("{}", msg);
                    Err(Error::Other(msg))
                }
            }
        } else {
            Err(Error::AsyncWriterDisabled)
        };
        callback(res);
    }

    fn handle_get_progress(&mut self, callback: Box<dyn FnOnce(Result<WalProgress>) + Send>) {
        let res = if let Some(writer) = &self.async_wal_writer {
            let progress = WalProgress {
                epoch: writer.epoch_id,
                offset: writer.file_off,
            };
            Ok(progress)
        } else {
            Err(Error::AsyncWriterDisabled)
        };
        callback(res);
    }

    fn handle_flush(&mut self) {
        if self.is_lightweight_enabled() {
            // Send flush task to object storage worker.
            self.dfs_worker_handle
                .as_ref()
                .unwrap()
                .try_send(ObjectStorageTask::Flush);
        }
    }

    fn handle_rotate(&mut self, epoch_id: u32) {
        let prev_region_id = self.epoch_id.swap(epoch_id + 1, Ordering::SeqCst);
        assert_eq!(prev_region_id, epoch_id); // panic on unexpected to avoid data corruption.
        if let Some(writer) = self.async_wal_writer.as_mut() {
            assert!(
                writer.epoch_id >= epoch_id,
                "invalid rotate epoch {}, async writer epoch {}",
                epoch_id,
                writer.epoch_id,
            );
            if writer.epoch_id == epoch_id {
                let file_off = writer.file_off;
                writer.rotate().unwrap();
                if let Some(dfs_worker_handle) = &self.dfs_worker_handle {
                    // Send rotate task to object storage worker.
                    dfs_worker_handle.try_send(ObjectStorageTask::Rotate { epoch_id, file_off });
                }
            }
        }
        self.compact_worker_handle
            .try_send(CompactTask::Rotate { epoch_id });
    }

    fn handle_close(&mut self, force: bool) {
        // Close and join object storage thread.
        if let Some(dfs_worker_handle) = self.dfs_worker_handle.take() {
            // If force close, we skip flushing wal chunk and close task thread.
            if !force {
                dfs_worker_handle.try_send(ObjectStorageTask::Flush);
            }
            dfs_worker_handle.try_send(ObjectStorageTask::Close);
            dfs_worker_handle.handle.join().unwrap();
        }
        self.compact_worker_handle.try_send(CompactTask::Close);
        let join_handle = self.compact_worker_handle.handle.take().unwrap();
        join_handle.join().unwrap();
    }

    fn handle_backup(&mut self, mut task: BackupTask) {
        if let Some(async_writer) = self.async_wal_writer.as_ref() {
            task.file_off = async_writer.file_off;
        }
        if task.config.lightweight {
            self.lightweight_backup(task);
        } else {
            (task.callback)(Err(Error::Backup("backup not enabled".into())));
        }
    }

    fn lightweight_backup(&mut self, task: BackupTask) {
        if self.async_wal_writer.is_none() {
            return backup_callback(
                task,
                Err(Error::Backup("async wal writer is not set".to_string())),
                "light_fail",
                Instant::now(),
            );
        }
        let async_writer = self.async_wal_writer.as_ref().unwrap();

        if !self.dfs_worker_healthy.is_healthy() {
            return backup_callback(
                task,
                Err(Error::DfsWorkerUnhealthy {
                    store_id: self.get_engine_id(),
                    epoch_id: async_writer.epoch_id,
                }),
                "light_fail",
                Instant::now(),
            );
        }

        let mut backup_meta = StoreBackupMeta::default();
        backup_meta.set_store_id(self.engine_id.load(Ordering::SeqCst));
        backup_meta.set_epoch(async_writer.epoch_id);
        backup_meta.set_offset(task.file_off);

        backup_callback(task, Ok(backup_meta), "light_success", Instant::now());
    }
}

fn dump_wal_chunk(
    dir: &Path,
    epoch_id: u32,
    epoch_rotate_len: usize,
    start_off: u64,
    end_off: u64,
) -> crate::Result<Bytes> {
    // `epoch_id` already checked in the caller.
    let mut file = fs::File::open(wal_file_name(dir, epoch_id, epoch_rotate_len))?;
    let file_len = file.metadata()?.len();
    if end_off > file_len {
        return Err(Error::Eof);
    }

    // `start_off` < `end_off` already checked in the caller.
    file.seek(SeekFrom::Start(start_off))?;
    let dump_len = (end_off - start_off) as usize;
    let mut buf = vec![0; dump_len];
    file.read_exact(&mut buf)?;
    Ok(Bytes::from(buf))
}
