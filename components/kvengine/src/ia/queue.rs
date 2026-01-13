// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! An *Single-Threading* implementation of S3-FIFO algorithm.

use std::{
    cell::Cell,
    cmp,
    collections::{HashMap, VecDeque},
    fmt,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

use bytes::Bytes;
use tikv_util::{
    sys::thread::StdThreadBuildWrapper, time::UnixSecs, worker_pool::WorkerPoolHandle,
};

#[cfg(feature = "debug-trace-ia-segments")]
use crate::ia::debug::*;
use crate::{
    ia::{
        manager::SegmentDataContext,
        types::{FileSegmentData, FileSegmentIdent},
    },
    table::{Error, Result},
};

/// A simple wrapper for `S3Fifo`.
pub(crate) struct S3FifoHandle {
    task_tx: crossbeam::channel::Sender<FifoTask>,
    handle: Option<JoinHandle<()>>,
}

impl S3FifoHandle {
    /// `small_cap` can be `0` to disable small queue for test.
    pub(crate) fn new(
        small_cap: Arc<AtomicI64>,
        main_cap: Arc<AtomicI64>,
        item_size: i64,
        freq_update_interval: Duration,
        runtime: WorkerPoolHandle,
        segment_data_ctx: SegmentDataContext,
    ) -> Self {
        let (task_tx, task_rx) = crossbeam::channel::unbounded();
        let task_tx_clone = task_tx.clone();
        let handle = std::thread::Builder::new()
            .name("s3fifo".to_string())
            .spawn_wrapper(move || {
                let mut fifo = S3Fifo::new(
                    small_cap,
                    main_cap,
                    item_size,
                    freq_update_interval,
                    task_tx_clone,
                    task_rx,
                    runtime,
                    segment_data_ctx,
                );
                fifo.run()
            })
            .unwrap();
        Self {
            task_tx,
            handle: Some(handle),
        }
    }

    pub(crate) fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            self.task_tx.send(FifoTask::Stop).unwrap();
            handle.join().unwrap();
            info!("s3fifo stopped");
        }
    }

    pub(crate) fn read(&self, ident: FileSegmentIdent, insert_main: bool) -> Result<()> {
        self.task_tx
            .send(FifoTask::Read { ident, insert_main })
            .map_err(|e| Error::IaMgr(format!("read: send failed: {ident}: {e:?}")))?;
        Ok(())
    }
}

#[cfg(any(test, feature = "testexport"))]
impl S3FifoHandle {
    pub(crate) async fn get_item(&self, ident: FileSegmentIdent) -> Result<Option<QueueItem>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let cb = Box::new(move |x| {
            let _ = tx.send(x);
        });
        self.task_tx
            .send(FifoTask::GetItem { ident, cb })
            .map_err(|e| Error::IaMgr(format!("get_item: send failed: {ident}: {e:?}")))?;
        Ok(rx.await.unwrap())
    }

    pub(crate) async fn flush_tasks(&self) -> Result<usize /* pending_tasks */> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let cb = Box::new(move |pending_tasks: usize| {
            let _ = tx.send(pending_tasks);
        });
        self.task_tx
            .send(FifoTask::Flush(cb))
            .map_err(|e| Error::IaMgr(format!("flush_tasks: send failed: {e:?}")))?;
        Ok(rx.await.unwrap())
    }
}

impl Drop for S3FifoHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

enum FifoTask {
    Read {
        ident: FileSegmentIdent,
        insert_main: bool,
    },
    SavedToMainStore(FileSegmentIdent, Arc<()> /* task counter */),
    Stop,

    #[cfg(any(test, feature = "testexport"))]
    GetItem {
        ident: FileSegmentIdent,
        cb: Box<dyn FnOnce(Option<QueueItem>) + Send>,
    },
    #[cfg(any(test, feature = "testexport"))]
    Flush(Box<dyn FnOnce(usize /* pending_tasks */) + Send>),
}

impl fmt::Debug for FifoTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FifoTask::Read { ident, insert_main } => f
                .debug_struct("Read")
                .field("ident", ident)
                .field("insert_main", insert_main)
                .finish(),
            FifoTask::SavedToMainStore(ident, _) => f
                .debug_struct("SavedToMainStore")
                .field("ident", ident)
                .finish(),
            FifoTask::Stop => f.debug_struct("Stop").finish(),

            #[cfg(any(test, feature = "testexport"))]
            FifoTask::GetItem { ident, cb: _ } => {
                f.debug_struct("GetItem").field("ident", ident).finish()
            }
            #[cfg(any(test, feature = "testexport"))]
            FifoTask::Flush(..) => f.debug_struct("Flush").finish(),
        }
    }
}

struct S3Fifo {
    small_queue: Queue,
    main_queue: Queue,
    ghost_queue: Vec<Option<FileSegmentIdent>>,

    /// Minimum interval to update `FileSegmentQueueInfo.freq`.
    freq_update_interval: Duration,

    task_tx: crossbeam::channel::Sender<FifoTask>,
    task_rx: crossbeam::channel::Receiver<FifoTask>,

    runtime: WorkerPoolHandle,
    segment_data_ctx: SegmentDataContext,
    task_counter: Arc<()>,
}

impl S3Fifo {
    /// `small_cap` can be `0` to disable small queue for test.
    fn new(
        small_cap: Arc<AtomicI64>,
        main_cap: Arc<AtomicI64>,
        avg_item_size: i64,
        freq_update_interval: Duration,
        task_tx: crossbeam::channel::Sender<FifoTask>,
        task_rx: crossbeam::channel::Receiver<FifoTask>,
        runtime: WorkerPoolHandle,
        segment_data_ctx: SegmentDataContext,
    ) -> Self {
        #[cfg(not(any(test, feature = "testexport")))]
        assert!(small_cap.load(Ordering::Relaxed) > 0);

        let main_queue_len = main_cap.load(Ordering::Relaxed) / avg_item_size;
        let mut ghost_queue = vec![];
        ghost_queue.resize(main_queue_len as usize, None);
        Self {
            // items: Default::default(),
            small_queue: Queue::new(small_cap, avg_item_size),
            main_queue: Queue::new(main_cap, avg_item_size),
            ghost_queue,
            freq_update_interval,
            task_tx,
            task_rx,
            runtime,
            segment_data_ctx,
            task_counter: Default::default(),
        }
    }

    fn run(&mut self) {
        info!("s3fifo core started");
        while let Ok(task) = self.task_rx.recv() {
            match task {
                FifoTask::Read { ident, insert_main } => match self.read(ident, insert_main) {
                    Ok(pos) => debug!("read"; "ident" => %ident, "pos" => ?pos),
                    Err(err) => error!("read failed"; "ident" => %ident, "err" => ?err),
                },
                FifoTask::SavedToMainStore(ident, _task_counter) => {
                    self.post_move_item_to_store(ident);
                }
                FifoTask::Stop => {
                    break;
                }

                #[cfg(any(test, feature = "testexport"))]
                FifoTask::GetItem { ident, cb } => {
                    cb(self.get_item(ident));
                }
                #[cfg(any(test, feature = "testexport"))]
                FifoTask::Flush(cb) => cb(self.pending_tasks()),
            }
        }
    }

    fn get_item_ref(&self, ident: FileSegmentIdent) -> Option<(&QueueItemMeta, QueueItemPos)> {
        self.small_queue
            .get(ident)
            .map(|item| (item, QueueItemPos::Small))
            .or_else(|| {
                self.main_queue
                    .get(ident)
                    .map(|item| (item, QueueItemPos::Main))
            })
    }

    fn read(&mut self, ident: FileSegmentIdent, insert_main: bool) -> Result<QueueItemPos> {
        debug!("fifo.read"; "ident" => %ident);
        let small_queue_cap = self.small_queue.capacity();
        if small_queue_cap > 0 && ident.size() > small_queue_cap as u64 {
            debug_assert!(false, "too large item: {:?} > {}", ident, small_queue_cap);
            return Err(Error::IaMgr(format!(
                "too large item, ident: {:?}, size: {}",
                ident,
                ident.size()
            )));
        }
        let pos = if let Some((queue_item, pos)) = self.get_item_ref(ident) {
            queue_item.access(self.freq_update_interval);
            pos
        } else if insert_main || small_queue_cap == 0 || self.is_in_ghost(ident) {
            self.insert_main(ident);
            QueueItemPos::Main
        } else {
            self.insert_small(ident);
            QueueItemPos::Small
        };
        Ok(pos)
    }

    fn insert_small(&mut self, ident: FileSegmentIdent) {
        debug!("fifo.insert_small"; "ident" => %ident);
        #[cfg(feature = "debug-trace-ia-segments")]
        trace_segment_action(ident, SegmentAction::InsertSmall);

        self.small_queue.push(ident);
        self.evict_small(ident);
    }

    fn evict_small(&mut self, ident: FileSegmentIdent) {
        debug!("fifo.evict_small"; "for" => %ident);
        let mut cap = None;
        while self.small_queue.is_oversize(&mut cap) {
            let Some((tail, queue_item)) = self.small_queue.pop() else {
                break;
            };

            debug!("fifo.evict_tail (small)";
                "tail" => %tail,
                "queue_item" => ?queue_item,
                "for" => %ident);
            #[cfg(feature = "debug-trace-ia-segments")]
            trace_segment_action(tail, SegmentAction::EvictFromSmall);

            if queue_item.freq() > 1 {
                // Item meta is cleared when move to main queue.
                self.insert_main(tail);
            } else {
                self.insert_ghost(tail);
                self.evict_item(tail);
            };
        }
    }

    fn insert_main(&mut self, ident: FileSegmentIdent) {
        debug!("fifo.insert_main"; "ident" => %ident);
        #[cfg(feature = "debug-trace-ia-segments")]
        trace_segment_action(ident, SegmentAction::InsertMain);

        let current = self.move_item_to_store(ident);
        if current.is_some() {
            self.main_queue.push(ident);
            self.evict_main(ident);
        } else {
            warn!("insert main: skip, segment data is not cached"; "ident" => %ident);
        }
    }

    fn evict_main(&mut self, ident: FileSegmentIdent) {
        debug!("fifo.evict_main"; "for" => %ident);
        let mut cap = None;
        while self.main_queue.is_oversize(&mut cap) {
            let Some((tail, queue_item)) = self.main_queue.pop_with_zero_freq() else {
                break;
            };
            debug_assert_eq!(queue_item.freq(), 0);
            debug!("fifo.evict_tail (main)";
                "tail" => %tail,
                "queue_item" => ?queue_item,
                "for" => %ident);
            #[cfg(feature = "debug-trace-ia-segments")]
            trace_segment_action(tail, SegmentAction::EvictFromMain);

            self.evict_item(tail);
        }
    }

    fn is_in_ghost(&self, ident: FileSegmentIdent) -> bool {
        let fingerprint = ident.fingerprint();
        let idx = fingerprint as usize % self.ghost_queue.len();
        self.ghost_queue[idx].as_ref().is_some_and(|&x| x == ident)
    }

    fn insert_ghost(&mut self, ident: FileSegmentIdent) {
        #[cfg(feature = "debug-trace-ia-segments")]
        trace_segment_action(ident, SegmentAction::InsertGhost);

        let fingerprint = ident.fingerprint();
        let idx = fingerprint as usize % self.ghost_queue.len();
        self.ghost_queue[idx] = Some(ident);
    }

    fn evict_item(&self, ident: FileSegmentIdent) {
        match self.segment_data_ctx.remove_segment_data(ident) {
            None => {
                warn!("evict item: skip, not cached"; "ident" => %ident);
            }
            Some(FileSegmentData::InMem(_)) => {
                debug!("evict item: done, removed from mem"; "ident" => %ident);
            }
            Some(FileSegmentData::InStore) => {
                self.spawn_remove_from_main_store(ident);
            }
        }
    }

    fn spawn_remove_from_main_store(&self, ident: FileSegmentIdent) {
        let task_counter = self.task_counter.clone();
        let ctx = self.segment_data_ctx.clone();
        self.runtime.spawn_blocking(move || {
            let res = ctx.remove_from_main_store(ident);
            match &res {
                Ok(Some(())) => {
                    debug!("remove from main store: done"; "ident" => %ident);
                }
                Ok(None) => {
                    // There should be another evict task for the same segment.
                    warn!("remove from main store: failed, not in main store"; "ident" => %ident);
                }
                Err(err) => {
                    error!("remove from main store: failed"; "ident" => %ident, "err" => ?err);
                }
            }

            #[cfg(feature = "debug-trace-ia-segments")]
            trace_segment_action(ident, SegmentAction::RemoveFromMainStore(res));

            drop(task_counter);
        });
    }

    // Return current segment data before move.
    fn move_item_to_store(&self, ident: FileSegmentIdent) -> Option<FileSegmentData> {
        let segment_data = self.segment_data_ctx.get_segment_data(ident);
        match &segment_data {
            Some(FileSegmentData::InMem(bytes)) => {
                self.spawn_save_to_main_store(ident, bytes.clone());
            }
            Some(FileSegmentData::InStore) => {
                debug!("move item: skip, already in main store"; "ident" => %ident);
                #[cfg(feature = "debug-trace-ia-segments")]
                trace_segment_action(ident, SegmentAction::SkipMoveAlreadyInStore);
            }
            None => {
                warn!("move item: skip, not cached"; "ident" => %ident);
                #[cfg(feature = "debug-trace-ia-segments")]
                trace_segment_action(ident, SegmentAction::SkipMoveNotCached);
            }
        }
        segment_data
    }

    fn spawn_save_to_main_store(&self, ident: FileSegmentIdent, bytes: Bytes) {
        let task_counter = self.task_counter.clone();
        let ctx = self.segment_data_ctx.clone();
        let task_tx = self.task_tx.clone();
        self.runtime.spawn_blocking(move || {
            let res = ctx.save_to_main_store(ident, bytes);
            match &res {
                Ok(()) => {
                    debug!("save to main store: done"; "ident" => %ident);
                }
                Err(err) => {
                    // The main store is full ?
                    // Still notify to remove data from memory. Otherwise, the memory usage would be
                    // exceeded.
                    // `read_segment` can handle the scene that segment is not existed in main
                    // store.
                    error!("save to main store: failed"; "ident" => %ident, "err" => ?err);
                }
            }

            #[cfg(feature = "debug-trace-ia-segments")]
            trace_segment_action(ident, SegmentAction::MoveToMainStore(res));

            // Hold the `task_counter` to help detecting there is another new task.
            if let Err(err) = task_tx.send(FifoTask::SavedToMainStore(ident, task_counter)) {
                warn!("save to main store: send task failed"; "ident" => %ident, "err" => ?err);
            }
        });
    }

    fn post_move_item_to_store(&self, ident: FileSegmentIdent) {
        let res = self
            .segment_data_ctx
            .set_segment_data_from_mem_to_store(ident);
        match &res {
            Ok(()) => {
                debug!("post move item: done"; "ident" => %ident);
            }
            Err(Some(FileSegmentData::InMem(_))) => unreachable!(),
            Err(Some(FileSegmentData::InStore)) => {
                warn!("post move item: duplicated, already in store"; "ident" => %ident);
            }
            Err(None) => {
                // Aggressively remove from main store, as disk usage is more important.
                warn!("move item: skip, segment has been evicted"; "ident" => %ident);
                self.spawn_remove_from_main_store(ident);
            }
        }

        #[cfg(feature = "debug-trace-ia-segments")]
        trace_segment_action(ident, SegmentAction::PostMoveToMainStore(res));
    }
}

#[cfg(any(test, feature = "testexport"))]
impl S3Fifo {
    fn get_item(&self, ident: FileSegmentIdent) -> Option<QueueItem> {
        self.small_queue
            .get(ident)
            .map(|meta| QueueItem::from_small(meta.clone()))
            .or_else(|| {
                self.main_queue
                    .get(ident)
                    .map(|meta| QueueItem::from_main(meta.clone()))
            })
    }

    fn pending_tasks(&self) -> usize {
        Arc::strong_count(&self.task_counter) - 1
    }
}

#[derive(Clone, Default)]
pub struct QueueItemMeta {
    freq: Cell<u8>,
    access_time: Cell<u64>,
}

impl fmt::Debug for QueueItemMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueItemMeta")
            .field("freq", &self.freq.get())
            .field("access_time", &self.access_time.get())
            .finish()
    }
}

impl QueueItemMeta {
    pub(crate) fn freq(&self) -> u8 {
        self.freq.get()
    }

    pub(crate) fn freq_desc(&self) {
        self.freq.update(|x| x - 1);
    }

    pub(crate) fn access(&self, freq_update_interval: Duration) {
        let now = UnixSecs::now().into_inner();
        if now.saturating_sub(self.access_time.get()) >= freq_update_interval.as_secs() {
            self.access_time.set(now);
            self.freq.set(cmp::min(self.freq.get() + 1, 3))
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueueItemPos {
    Small,
    Main,
}

#[derive(Clone, Debug)]
pub struct QueueItem {
    // For debug info.
    #[allow(dead_code)]
    pub meta: QueueItemMeta,
    pub pos: QueueItemPos,
}

#[cfg(any(test, feature = "testexport"))]
impl QueueItem {
    pub(crate) fn from_small(meta: QueueItemMeta) -> Self {
        Self {
            meta,
            pos: QueueItemPos::Small,
        }
    }

    pub(crate) fn from_main(meta: QueueItemMeta) -> Self {
        Self {
            meta,
            pos: QueueItemPos::Main,
        }
    }
}

struct Queue {
    items: HashMap<FileSegmentIdent, QueueItemMeta>,
    queue: VecDeque<FileSegmentIdent>,
    cap: Arc<AtomicI64>,
    total_size: i64,
}

impl Queue {
    fn new(cap: Arc<AtomicI64>, item_size: i64) -> Self {
        Self {
            items: HashMap::new(),
            queue: VecDeque::with_capacity((cap.load(Ordering::Relaxed) / item_size) as usize),
            cap,
            total_size: 0,
        }
    }

    fn get(&self, ident: FileSegmentIdent) -> Option<&QueueItemMeta> {
        self.items.get(&ident)
    }

    fn push(&mut self, ident: FileSegmentIdent) {
        self.total_size += ident.size() as i64;
        self.items.insert(ident, QueueItemMeta::default());
        self.queue.push_back(ident);
    }

    fn pop(&mut self) -> Option<(FileSegmentIdent, QueueItemMeta)> {
        let ident = self.queue.pop_front()?;
        self.total_size -= ident.size() as i64;
        let item = self.items.remove(&ident).unwrap();
        Some((ident, item))
    }

    /// Pop item with `freq == 0`. Other items will decrease freq and push to
    /// queue again.
    fn pop_with_zero_freq(&mut self) -> Option<(FileSegmentIdent, QueueItemMeta)> {
        while let Some(ident) = self.queue.pop_front() {
            let item = self.items.get(&ident).unwrap();
            if item.freq() > 0 {
                item.freq_desc();
                self.queue.push_back(ident);
            } else {
                self.total_size -= ident.size() as i64;
                let item = self.items.remove(&ident).unwrap();
                return Some((ident, item));
            }
        }
        None
    }

    fn is_oversize(&self, cap: &mut Option<i64>) -> bool {
        self.total_size > *cap.get_or_insert_with(|| self.capacity())
    }

    fn capacity(&self) -> i64 {
        self.cap.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod benches {
    use futures::future::join_all;
    use rand::prelude::*;
    use test::black_box;
    use tikv_util::worker_pool::WorkerPool;

    use super::*;

    #[bench]
    fn bench_s3fifo_1(b: &mut test::Bencher) {
        bench_s3fifo_impl(b, 100, 900, 100000, 10000, 1);
    }

    #[bench]
    fn bench_s3fifo_8(b: &mut test::Bencher) {
        bench_s3fifo_impl(b, 100, 900, 100000, 10000, 8);
    }

    fn bench_s3fifo_impl(
        b: &mut test::Bencher,
        small_cap: i64,
        main_cap: i64,
        data_range: usize,
        data_count: usize,
        concurrency: usize,
    ) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let worker_pool = WorkerPool::from(runtime.handle().clone());
        let segment_data_ctx = SegmentDataContext::new_for_test();
        let fifo = Arc::new(S3FifoHandle::new(
            Arc::new(AtomicI64::new(small_cap)),
            Arc::new(AtomicI64::new(main_cap)),
            1,
            Duration::ZERO,
            worker_pool.handle(),
            segment_data_ctx,
        ));

        let mut access_seqs = vec![];
        access_seqs.resize_with(concurrency, || {
            generate_access_seq(data_range, data_count / concurrency)
        });

        b.iter(|| {
            let mut handles = Vec::with_capacity(concurrency);
            for seq in access_seqs.clone() {
                let fifo = fifo.clone();
                let h = runtime.spawn_blocking(move || {
                    for i in seq {
                        let ident = FileSegmentIdent {
                            file_id: 42,
                            start_off: i,
                            end_off: i + 1,
                        };
                        black_box(fifo.read(ident, false)).unwrap();
                    }
                });
                handles.push(h);
            }
            runtime.block_on(join_all(handles));
        })
    }

    pub(crate) fn generate_access_seq(range: usize, count: usize) -> Vec<u64> {
        let mut rng = thread_rng();
        let zipf = zipf::ZipfDistribution::new(range, 1.03).unwrap();
        (0..count).map(|_| zipf.sample(&mut rng) as u64).collect()
    }
}
