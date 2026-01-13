// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    default::Default,
    fs,
    ops::Deref,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::{Buf, Bytes};
use cloud_encryption::EncryptionKey;
use dashmap::{DashMap, mapref::entry::Entry};
use futures::executor::block_on;
use regex::Regex;
use tikv_util::{
    box_err,
    config::ReadableDuration,
    time::Instant,
    worker_pool::{WorkerPool, WorkerPoolHandle},
};
use tokio::sync::{OwnedRwLockWriteGuard, RwLock};

use crate::{
    Error, Result, dfs,
    dfs::{Dfs, FileType},
    error::IoContext,
    table,
    table::{
        TxnCtx, TxnFile, TxnFileId,
        file::{FdCache, InMemFile, LocalFile},
        get_local_dir,
        sstable::BlockCache,
        txn_file::TxnChunk,
    },
};

const READ_DFS_CONCURRENCY: usize = 4;

#[derive(Clone)]
pub struct TxnChunkManager {
    core: Arc<TxnChunkManagerCore>,
}

impl TxnChunkManager {
    pub fn new(
        local_paths: Vec<PathBuf>,
        dfs: Arc<dyn Dfs>,
        cache: BlockCache,
        fd_cache: Option<FdCache>,
        worker_pool: WorkerPool,
        config: TxnChunkManagerConfig,
    ) -> Self {
        info!("create txn chunk manager"; "worker_pool" => ?worker_pool);
        let txn_chunks = Arc::new(DashMap::new());
        if local_paths.is_empty() {
            worker_pool
                .handle()
                .spawn(run_gc_worker(txn_chunks.clone(), config));
        }
        let manager = Self {
            core: Arc::new(TxnChunkManagerCore {
                local_paths,
                dfs,
                txn_chunks,
                cache,
                fd_cache,
                worker_pool,
            }),
        };
        manager.init().unwrap();
        manager
    }
}

impl Deref for TxnChunkManager {
    type Target = TxnChunkManagerCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TxnChunkManagerConfig {
    // For memory based only.
    pub gc_interval: ReadableDuration,
    pub gc_ttl: ReadableDuration,
}

impl Default for TxnChunkManagerConfig {
    fn default() -> Self {
        Self {
            gc_interval: ReadableDuration::minutes(1),
            gc_ttl: ReadableDuration::minutes(10),
        }
    }
}

pub fn with_pool_size(pool_size: usize) -> WorkerPool {
    WorkerPool::from(
        tokio::runtime::Builder::new_multi_thread()
            .thread_name("txn-chunk-worker")
            .worker_threads(1) // for gc worker.
            .max_blocking_threads(pool_size)
            .enable_all()
            .build()
            .unwrap(),
    )
}

// Memory based if `local_path` is None.
// TODO: unify the process of file-based & memory-based to eliminate duplicated
// codes.
pub struct TxnChunkManagerCore {
    local_paths: Vec<PathBuf>,
    dfs: Arc<dyn Dfs>,
    txn_chunks: Arc<DashMap<u64, TxnChunkEntry>>,
    cache: BlockCache,
    // fd_cache is None in tikv-worker
    fd_cache: Option<FdCache>,
    worker_pool: WorkerPool,
}

struct TxnChunkEntryData {
    chunk: TxnChunk,
    prepare_time_secs: AtomicI64,
}

impl TxnChunkEntryData {
    fn new(chunk: TxnChunk) -> Self {
        Self::new_with(chunk, Instant::now_coarse())
    }

    fn new_with(chunk: TxnChunk, time: Instant) -> Self {
        Self {
            chunk,
            prepare_time_secs: AtomicI64::new(time.second()),
        }
    }

    #[inline]
    fn touch(&self) {
        self.touch_with(Instant::now_coarse());
    }

    #[inline]
    fn touch_with(&self, time: Instant) {
        self.prepare_time_secs
            .fetch_max(time.second(), Ordering::Relaxed);
    }

    #[inline]
    pub fn prepare_time(&self) -> Instant {
        Instant::from_timespec_second_coarse(self.prepare_time_secs.load(Ordering::Relaxed))
    }
}

#[derive(Clone)]
struct TxnChunkEntry {
    chunk_data: Arc<RwLock<Option<TxnChunkEntryData>>>,
}

impl Default for TxnChunkEntry {
    fn default() -> Self {
        Self {
            chunk_data: Arc::new(RwLock::new(None)),
        }
    }
}

impl TxnChunkManagerCore {
    fn init(&self) -> Result<()> {
        for local_path in &self.local_paths {
            if !local_path.exists() {
                fs::create_dir_all(local_path).ctx("txn_chunk_mgr.init.create_dir")?;
            }
            if local_path.is_dir() {
                let read_dir = fs::read_dir(local_path).ctx("txn_chunk_mgr.init.read_dir")?;
                let now = Instant::now_coarse();
                for entry in read_dir.flatten() {
                    let file_name = entry.file_name();
                    if let Some(txn_file_id) = parse_txn_chunk_id(file_name.to_str().unwrap()) {
                        // We don't have the encryption key here, we can do nothing but skip the
                        // encrypted files.
                        // As the number of encrypted files is expected to be small, this should be
                        // fine.
                        let txn_chunk = match self.load_txn_chunk(txn_file_id, entry.path(), None) {
                            Ok(txn_chunk) => txn_chunk,
                            Err(Error::TableError(table::Error::NeedEncryptionKey { .. })) => {
                                continue;
                            }
                            Err(err) => return Err(err),
                        };
                        let files_entry = self.txn_chunks.entry(txn_file_id).or_default().clone();
                        let mut guard = block_on(files_entry.chunk_data.write());
                        *guard = Some(TxnChunkEntryData::new_with(txn_chunk, now))
                    }
                }
            }
        }
        Ok(())
    }

    pub fn worker_pool(&self) -> WorkerPoolHandle {
        self.worker_pool.handle()
    }

    pub fn prepare(
        &self,
        txn_chunk_id: u64,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<TxnChunk> {
        let entry = self.txn_chunks.entry(txn_chunk_id).or_default().clone();
        let mut guard = block_on(entry.chunk_data.write());
        if let Some(txn_chunk) = guard.as_mut() {
            txn_chunk.touch();
            return Ok(txn_chunk.chunk.clone());
        }

        if let Some(local_file_path) = self.local_file_path(txn_chunk_id) {
            if local_file_path.exists() {
                let txn_chunk =
                    self.load_txn_chunk(txn_chunk_id, local_file_path, encryption_key)?;
                *guard = Some(TxnChunkEntryData::new(txn_chunk.clone()));
                return Ok(txn_chunk);
            }
        }

        let runtime = self.dfs.get_runtime();
        let opts = dfs::Options::default().with_type(FileType::TxnChunk);
        let file_data = runtime.block_on(self.dfs.read_file(txn_chunk_id, opts))?;

        let txn_chunk = if let Some(local_path) = self.get_local_path(txn_chunk_id) {
            let txn_file_tmp_path = local_path.join(Self::tmp_file_name(txn_chunk_id));
            fs::write(&txn_file_tmp_path, file_data.chunk())
                .table_ctx(txn_chunk_id, "txn_chunk_mgr.prepare.write_tmp")?;
            let local_file_path = self.local_file_path(txn_chunk_id).unwrap();
            fs::rename(&txn_file_tmp_path, &local_file_path)
                .table_ctx(txn_chunk_id, "txn_chunk.prepare.rename")?;
            self.load_txn_chunk(txn_chunk_id, local_file_path, encryption_key)?
        } else {
            let file = InMemFile::new(txn_chunk_id, file_data);
            TxnChunk::new(Arc::new(file), self.cache.clone(), encryption_key)?
        };
        *guard = Some(TxnChunkEntryData::new(txn_chunk.clone()));
        Ok(txn_chunk)
    }

    pub fn prepare_txn_chunks(
        &self,
        mut txn_chunks_id: Vec<u64>,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<()> {
        // Sort to avoid deadlocks.
        txn_chunks_id.sort();
        info!("prepare txn chunks: {:?}", txn_chunks_id);

        let (tx, rx) = tikv_util::mpsc::bounded(READ_DFS_CONCURRENCY);
        let runtime = self.dfs.get_runtime();
        let mut msg_count: usize = 0;
        let now = Instant::now_coarse();
        for chunk_id in txn_chunks_id {
            let entry = self.txn_chunks.entry(chunk_id).or_default().clone();
            let mut guard = block_on(entry.chunk_data.write_owned());
            if let Some(txn_chunk) = guard.as_mut() {
                txn_chunk.touch_with(now);
                continue;
            }

            if let Some(local_file_path) = self.local_file_path(chunk_id) {
                if local_file_path.exists() {
                    let txn_chunk =
                        self.load_txn_chunk(chunk_id, local_file_path, encryption_key.clone())?;
                    *guard = Some(TxnChunkEntryData::new(txn_chunk));
                    continue;
                }
            }

            let dfs = self.dfs.clone();
            let tx = tx.clone();
            runtime.spawn(async move {
                let opts = dfs::Options::default().with_type(FileType::TxnChunk);
                let file_data = dfs.read_file(chunk_id, opts).await;
                if let Err(err) = tx.send((chunk_id, file_data, guard)) {
                    // Error should happen only when prepare_txn_chunks exit with error.
                    warn!("prepare_txn_chunks: send error: {:?}", err; "chunk_id" => chunk_id);
                }
            });
            msg_count += 1;

            if msg_count >= READ_DFS_CONCURRENCY {
                self.recv_txn_chunk_file_data(&rx, encryption_key.clone())?;
                msg_count -= 1;
            }
        }
        for _ in 0..msg_count {
            self.recv_txn_chunk_file_data(&rx, encryption_key.clone())?;
        }
        Ok(())
    }

    fn recv_txn_chunk_file_data(
        &self,
        rx: &tikv_util::mpsc::Receiver<(
            u64,
            crate::dfs::Result<Bytes>,
            OwnedRwLockWriteGuard<Option<TxnChunkEntryData>>,
        )>,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<()> {
        let (chunk_id, file_data, mut guard) = rx.recv().unwrap();
        let file_data = file_data.map_err(|err| -> Error {
            box_err!("read_txn_chunk failed: {:?}, chunk_id {}", err, chunk_id)
        })?;
        let txn_chunk = if let Some(local_dir) = self.get_local_path(chunk_id) {
            let local_file_path = self.local_file_path(chunk_id).unwrap();
            if !local_file_path.exists() {
                let txn_file_tmp_path = local_dir.join(Self::tmp_file_name(chunk_id));
                fs::write(&txn_file_tmp_path, file_data.chunk())
                    .table_ctx(chunk_id, "txn_chunk_mgr.recv_chunk.write_tmp")?;
                fs::rename(&txn_file_tmp_path, &local_file_path)
                    .table_ctx(chunk_id, "txn_chunk_mgr.recv_chunk.rename")?;
            }
            self.load_txn_chunk(chunk_id, local_file_path, encryption_key)?
        } else {
            let file = InMemFile::new(chunk_id, file_data);
            TxnChunk::new(Arc::new(file), self.cache.clone(), encryption_key)?
        };
        *guard = Some(TxnChunkEntryData::new(txn_chunk));
        Ok(())
    }

    fn load_txn_chunk(
        &self,
        txn_chunk_id: u64,
        path: PathBuf,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<TxnChunk> {
        let file = LocalFile::open_ext(txn_chunk_id, path, None, self.fd_cache.clone(), false)?;
        let txn_chunk = TxnChunk::new(Arc::new(file), self.cache.clone(), encryption_key)?;
        Ok(txn_chunk)
    }

    pub fn write_local_chunk(&self, chunk_id: u64, file_data: Bytes) -> Result<()> {
        if self.local_paths.is_empty() {
            return Err(Error::Other(box_err!("local_dirs is empty")));
        }
        let local_file_path = self.local_file_path(chunk_id).unwrap();
        if !local_file_path.exists() {
            let txn_file_tmp_path = local_file_path.with_file_name(Self::tmp_file_name(chunk_id));
            fs::write(&txn_file_tmp_path, file_data.chunk())
                .table_ctx(chunk_id, "write_local_chunk")?;
            fs::rename(&txn_file_tmp_path, &local_file_path)
                .table_ctx(chunk_id, "rename_local_chunk")?;
        }
        Ok(())
    }

    pub fn read_local_chunk(&self, chunk_id: u64, check_exists_only: bool) -> Result<Bytes> {
        if self.local_paths.is_empty() {
            return Err(Error::Other(box_err!("local_dirs is empty")));
        }
        let local_file_path = self.local_file_path(chunk_id).unwrap();
        if check_exists_only {
            let _ =
                fs::metadata(local_file_path).table_ctx(chunk_id, "read_local_chunk.metadata")?;
            Ok(Bytes::new())
        } else {
            let data = fs::read(local_file_path).table_ctx(chunk_id, "read_local_chunk.read")?;
            Ok(data.into())
        }
    }

    pub fn all_chunks_exists(&self, chunk_ids: &[u64]) -> bool {
        let now = Instant::now_coarse();
        for chunk_id in chunk_ids {
            if let Some(entry) = self.txn_chunks.get(chunk_id) {
                if let Ok(guard) = entry.value().chunk_data.try_read() {
                    match guard.as_ref() {
                        Some(chunk_data) => chunk_data.touch_with(now),
                        None => {
                            // load chunk failed.
                            return false;
                        }
                    }
                } else {
                    // The chunk is loading by another thread.
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    pub fn get(&self, txn_chunk_id: u64) -> Option<TxnChunk> {
        let entry = self.txn_chunks.get(&txn_chunk_id)?.clone();
        let guard = block_on(entry.chunk_data.read());
        guard.as_ref().map(|x| x.chunk.clone())
    }

    fn remove_chunk_file(&self, txn_chunk_id: u64) -> Result<()> {
        if let Some(path) = self.local_file_path(txn_chunk_id) {
            if let Some(cache) = self.fd_cache.as_ref() {
                cache.remove(txn_chunk_id)
            }
            let res = fs::remove_file(&path);
            match &res {
                Ok(()) => info!("remove chunk file"; "id" => txn_chunk_id, "path" => ?path),
                Err(err) => {
                    warn!("remove chunk file failed"; "id" => txn_chunk_id, "path" => ?path, "err" => ?err)
                }
            }
            res.table_ctx(txn_chunk_id, "txn_chunk.remove")?;
        }
        Ok(())
    }

    // NOTE: used by test & TiFlash proxy.
    pub fn unsafe_remove(&self, txn_chunk_id: u64) -> bool {
        let _ = self.remove_chunk_file(txn_chunk_id);
        self.txn_chunks.remove(&txn_chunk_id).is_some()
    }

    pub fn gc_chunk_file(&self, txn_chunk_id: u64, timeout: Duration, store_id: u64) -> bool {
        match self.txn_chunks.entry(txn_chunk_id) {
            Entry::Vacant(_entry) => {
                warn!("{} gc: chunk file is leak", store_id; "id" => txn_chunk_id);
                let _ = self.remove_chunk_file(txn_chunk_id);
                true
            }
            Entry::Occupied(entry) => {
                let chunk_data = entry.get().chunk_data.clone();
                let Ok(mut guard) = chunk_data.try_write() else {
                    return false;
                };
                if Arc::strong_count(&chunk_data) > 2 {
                    // 2: entry + guard.
                    // Another thread has cloned the `chunk_data` and is waiting for the lock.
                    return false;
                }
                let to_remove = match guard.as_ref() {
                    Some(x) => x.prepare_time().saturating_elapsed() >= timeout,
                    None => {
                        warn!("{} gc: chunk file is leak", store_id; "id" => txn_chunk_id);
                        true
                    }
                };
                if to_remove {
                    let _ = self.remove_chunk_file(txn_chunk_id);
                    *guard = None;

                    // The safety to remove the entry:
                    // 1. No other thread is holding the entry of `txn_chunks`, as `entry` has the
                    //    write lock.
                    // 2. Threads has cloned the `chunk_data` but waiting for lock of `chunk_data`
                    //    must result in `Arc::strong_count(&chunk_data) > 2`.
                    entry.remove();
                    debug_assert_eq!(Arc::strong_count(&chunk_data), 1);
                }
                to_remove
            }
        }
    }

    fn local_file_path(&self, txn_chunk_id: u64) -> Option<PathBuf> {
        let local_dir = self.get_local_path(txn_chunk_id)?;
        Some(local_dir.join(format!("{:016x}.txn", txn_chunk_id)))
    }

    fn get_local_path(&self, txn_chunk_id: u64) -> Option<&PathBuf> {
        if self.local_paths.is_empty() {
            return None;
        }
        Some(get_local_dir(&self.local_paths, txn_chunk_id))
    }

    // `encryption_key` is required only when `is_prepared` is false.
    pub fn load_txn_file_from_ref(
        &self,
        shard_id: u64,
        shard_ver: u64,
        txn_file_ref: &kvenginepb::TxnFileRef,
        is_prepared: bool,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<TxnFile> {
        let mut chunks = Vec::with_capacity(txn_file_ref.chunk_ids.len());
        for &chunk_id in &txn_file_ref.chunk_ids {
            let txn_chunk = if !is_prepared {
                self.prepare(chunk_id, encryption_key.clone())?
            } else {
                // The chunk may be removed by `unsafe_remove` in some use cases even if it is
                // prepared. If the chunk is not prepared, give a chance to prepare it.
                self.get(chunk_id)
                    .or_else(|| self.prepare(chunk_id, encryption_key.clone()).ok())
                    .ok_or_else(|| -> Error {
                        box_err!("txn chunk is not prepared, chunk_id {}", chunk_id)
                    })?
            };
            chunks.push(txn_chunk);
        }
        let txn_ctx = TxnCtx::from_txn_file_ref(txn_file_ref);
        let txn_file_id = TxnFileId::new(shard_id, shard_ver, txn_file_ref.start_ts);
        Ok(TxnFile::new(txn_file_id, chunks, txn_ctx)?)
    }

    pub fn load_txn_files_from_refs(
        &self,
        shard_id: u64,
        shard_ver: u64,
        txn_file_refs: &[kvenginepb::TxnFileRef],
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Vec<TxnFile>> {
        let txn_chunks_id = txn_file_refs
            .iter()
            .flat_map(|txn_file_ref| txn_file_ref.chunk_ids.iter())
            .copied()
            .collect::<Vec<_>>();
        self.prepare_txn_chunks(txn_chunks_id, encryption_key.clone())?;
        txn_file_refs
            .iter()
            .map(|txn_file_ref| {
                self.load_txn_file_from_ref(
                    shard_id,
                    shard_ver,
                    txn_file_ref,
                    true,
                    encryption_key.clone(),
                )
            })
            .collect()
    }

    fn tmp_file_name(chunk_id: u64) -> String {
        lazy_static::lazy_static! {
            static ref TMP_FILE_ID: AtomicU64 = AtomicU64::default();
        }
        let tmp_id = TMP_FILE_ID.fetch_add(1, Ordering::Relaxed);
        format!("{}.{}.tmp", chunk_id, tmp_id)
    }
}

pub fn parse_txn_chunk_id(txn_chunk_name: &str) -> Option<u64> {
    lazy_static::lazy_static! {
        static ref RE: Regex = Regex::new(r"([0-9a-fA-F]{16})\.txn$").unwrap();
    }
    let caps = RE.captures(txn_chunk_name)?;
    let chunk_id = u64::from_str_radix(&caps[1], 16).unwrap();
    Some(chunk_id)
}

async fn run_gc_worker(
    txn_chunks: Arc<DashMap<u64, TxnChunkEntry>>,
    config: TxnChunkManagerConfig,
) {
    info!("run gc worker"; "config" => ?config);
    loop {
        tokio::time::sleep(config.gc_interval.0).await;
        let chunk_ids: Vec<_> = txn_chunks.iter().map(|x| *x.key()).collect();

        let mut expired_count = 0;
        let mut expired_size = 0;
        let total_count = chunk_ids.len();
        let mut total_size = 0;
        let now = Instant::now_coarse();
        for chunk_id in chunk_ids {
            let (is_expired, size) =
                gc_single_txn_chunk(&txn_chunks, chunk_id, config.gc_ttl.0, now).await;
            total_size += size;
            if is_expired {
                expired_count += 1;
                expired_size += size;
            }
        }

        if total_count > 0 {
            info!(
                "run gc worker: total: {total_count}/{total_size}, expired: {expired_count}/{expired_size} (count/size)"
            );
        }
    }
}

async fn gc_single_txn_chunk(
    txn_chunks: &DashMap<u64, TxnChunkEntry>,
    chunk_id: u64,
    ttl: Duration,
    now: Instant,
) -> (bool /* is_expired */, u64 /* size */) {
    let Some(entry_ref) = txn_chunks.get(&chunk_id) else {
        return (false, 0);
    };
    let entry = entry_ref.clone();
    drop(entry_ref); // Release the lock. Otherwise, it will cause deadlock when remove entry.
    let txn_chunk = entry.chunk_data.read().await;
    let (is_expired, size) = txn_chunk.as_ref().map_or((true, 0), |x| {
        (
            now.saturating_duration_since(x.prepare_time()) >= ttl,
            x.chunk.size() as u64,
        )
    });
    if is_expired {
        txn_chunks.remove(&chunk_id);
    }
    (is_expired, size)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        dfs::InMemFs,
        table::{InnerKey, OP_PUT, TxnChunkBuilder, sstable::BlockCacheType},
    };

    #[rstest]
    #[case(Some(TempDir::new().unwrap()))]
    #[case::in_mem(None)]
    fn test_txn_chunk_manager(#[case] tmp_dir: Option<TempDir>) {
        ::test_util::init_log_for_test();
        let local_paths = tmp_dir
            .as_ref()
            .map(|dir| vec![dir.path().to_path_buf()])
            .unwrap_or_default();
        let dfs: Arc<dyn Dfs> = Arc::new(InMemFs::new());
        let cache = BlockCache::new(BlockCacheType::Quick, 1024 * 1024, 4 * 1024);
        let txn_chunk_manager = TxnChunkManager::new(
            local_paths.clone(),
            dfs.clone(),
            cache.clone(),
            None,
            with_pool_size(2),
            TxnChunkManagerConfig::default(),
        );
        let runtime = dfs.get_runtime();
        let opts = dfs::Options::default().with_type(FileType::TxnChunk);
        for chunk_id in 1u64..=6 {
            let buf = make_txn_chunk(chunk_id);
            runtime
                .block_on(dfs.create(chunk_id, buf.into(), opts))
                .unwrap();
        }

        for chunk_id in 1u64..=3 {
            txn_chunk_manager.prepare(chunk_id, None).unwrap();
            assert!(txn_chunk_manager.get(chunk_id).is_some());
        }
        txn_chunk_manager
            .prepare_txn_chunks(vec![4, 5, 6], None)
            .unwrap();
        assert!(txn_chunk_manager.all_chunks_exists(&[4, 5, 6]));

        txn_chunk_manager.unsafe_remove(1);
        assert!(txn_chunk_manager.get(1).is_none());
        drop(txn_chunk_manager);

        let txn_chunk_manager = TxnChunkManager::new(
            local_paths.clone(),
            dfs,
            cache,
            None,
            with_pool_size(2),
            TxnChunkManagerConfig::default(),
        );
        if !local_paths.is_empty() {
            // After process restart, the remained txn chunks are all loaded.
            assert!(txn_chunk_manager.get(1).is_none());
            assert!(txn_chunk_manager.all_chunks_exists(&[2, 3]));
        } else {
            assert!(!txn_chunk_manager.all_chunks_exists(&[2, 3]));
        }
    }

    #[test]
    fn test_gc() {
        ::test_util::init_log_for_test();
        let temp_dir = TempDir::new().unwrap();
        let local_path = temp_dir.path();

        let dfs: Arc<dyn Dfs> = Arc::new(InMemFs::new());
        let cache = BlockCache::new(BlockCacheType::None, 0, 0);
        let mgr = TxnChunkManager::new(
            vec![local_path.to_path_buf()],
            dfs,
            cache,
            None,
            with_pool_size(2),
            TxnChunkManagerConfig::default(),
        );

        let chunk1 = make_txn_chunk(1);
        let chunk1_path = mgr.local_file_path(1).unwrap();
        fs::write(&chunk1_path, &chunk1).unwrap();
        mgr.init().unwrap();
        mgr.get(1).unwrap();

        // Not timeout yet:
        assert!(!mgr.gc_chunk_file(1, Duration::from_secs(60), 1));
        mgr.get(1).unwrap();

        // Timeout:
        assert!(mgr.gc_chunk_file(1, Duration::from_secs(0), 1));
        assert!(mgr.get(1).is_none());

        // Leak:
        fs::write(&chunk1_path, &chunk1).unwrap();
        assert!(chunk1_path.try_exists().unwrap());
        assert!(mgr.get(1).is_none());
        assert!(mgr.gc_chunk_file(1, Duration::from_secs(60), 1));
        assert!(!chunk1_path.try_exists().unwrap());

        // Test for `all_chunks_exists`:
        fs::write(&chunk1_path, &chunk1).unwrap();
        mgr.init().unwrap();
        std::thread::sleep(Duration::from_secs(3));
        assert!(mgr.all_chunks_exists(&[1]));
        assert!(!mgr.gc_chunk_file(1, Duration::from_secs(1), 1));
        mgr.get(1).unwrap();
    }

    fn make_txn_chunk(chunk_id: u64) -> Vec<u8> {
        let mut chunk_builder = TxnChunkBuilder::new(chunk_id, 64, None);
        for i in 0..100 {
            let key = format!("{:02}/{:02}", chunk_id, i);
            chunk_builder.add_entry(
                InnerKey::from_outer_key(key.as_bytes()),
                OP_PUT,
                key.as_bytes(),
            );
        }
        let mut buf = vec![];
        chunk_builder.finish(&mut buf);
        buf
    }
}
