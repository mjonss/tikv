// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    iter::Iterator as StdIterator,
    mem,
    ops::Deref,
};

use bytes::{Bytes, BytesMut};
use cloud_encryption::EncryptionKey;
use fail::fail_point;
use file_system::IoType;
use kvenginepb as pb;
use kvenginepb::L0Create;
use tikv_util::{
    Either, info, mpsc,
    sys::{SysQuota, thread::ThreadBuildWrapper},
    time::{monotonic_raw_now, timespec_to_ns},
};
use tokio::sync::mpsc::{UnboundedSender as Sender, unbounded_channel};

use crate::{
    table::{
        BoundedDataSet, InnerKey, NO_COMPRESSION, SnapVersion, memtable, memtable::CfTable,
        sstable, sstable::Builder,
    },
    util::{TxnFileRefPropertyHelper, new_l0_create_pb},
    *,
};

#[derive(Debug)]
pub enum Persisted {
    L0 { table: pb::L0Create },
    Blob { table: Option<pb::BlobCreate> },
}

#[derive(Clone)]
pub(crate) struct FlushTask {
    pub(crate) id_ver: IdVer,
    pub(crate) range: ShardRange,
    pub(crate) normal: Option<memtable::CfTable>,
    pub(crate) initial: Option<InitialFlush>,
    pub(crate) encryption_key: Option<EncryptionKey>,
}

impl FlushTask {
    fn new(
        shard: &Shard,
        normal: Option<memtable::CfTable>,
        initial: Option<InitialFlush>,
        encryption_key: Option<EncryptionKey>,
    ) -> Self {
        Self {
            id_ver: IdVer::new(shard.id, shard.ver),
            range: shard.range.clone(),
            normal,
            initial,
            encryption_key,
        }
    }

    pub(crate) fn inner_start(&self) -> InnerKey<'_> {
        self.range.inner_start()
    }

    pub(crate) fn inner_end(&self) -> InnerKey<'_> {
        self.range.inner_end()
    }

    pub(crate) fn new_normal(shard: &Shard, mem_tbl: memtable::CfTable) -> Self {
        Self::new(shard, Some(mem_tbl), None, shard.encryption_key.clone())
    }

    pub(crate) fn new_initial(shard: &Shard, initial: InitialFlush) -> Self {
        Self::new(shard, None, Some(initial), shard.encryption_key.clone())
    }

    pub(crate) fn table_double_overbound(
        &self,
        smallest: InnerKey<'_>,
        biggest: InnerKey<'_>,
    ) -> bool {
        smallest < self.inner_start() && self.inner_end() <= biggest
    }

    pub(crate) fn snap_version(&self) -> SnapVersion {
        match (&self.normal, &self.initial) {
            (Some(normal), None) => normal.get_snap_version(),
            (None, Some(initial)) => initial.snap_version(),
            _ => unreachable!(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct InitialFlush {
    pub(crate) mem_tbls: Vec<memtable::CfTable>,
    pub(crate) base_version: u64,
    pub(crate) data_sequence: u64,
    pub(crate) shard_data: ShardData,
    pub(crate) columnar_table_ids: Vec<i64>,
    pub(crate) max_ts: u64,
    pub(crate) properties: Option<kvenginepb::Properties>,
}

impl InitialFlush {
    fn snap_version(&self) -> SnapVersion {
        SnapVersion::new(self.base_version, self.data_sequence)
    }
}

impl Engine {
    pub(crate) fn run_flush_worker(&self, rx: mpsc::Receiver<FlushMsg>) {
        let concurrency = (SysQuota::cpu_cores_quota() as usize / 3).max(2);
        let pool = tokio::runtime::Builder::new_multi_thread()
            .thread_name("flush-worker")
            .worker_threads(concurrency)
            .enable_all()
            .after_start_wrapper(|| {
                file_system::set_io_type(IoType::Flush);
            })
            .before_stop_wrapper(|| {})
            .build()
            .unwrap();
        let mut worker = FlushWorker {
            shards: Default::default(),
            receiver: rx,
            engine: self.clone(),
            pool,
        };
        worker.run();
    }

    pub(crate) async fn flush_normal(&self, task: FlushTask) -> Result<pb::ChangeSet> {
        fail_point!("kvengine_flush_normal", |_| Err(dfs::Error::Io(
            "injected error".to_string()
        )
        .into()));

        let mut cs = new_change_set(task.id_ver.id, task.id_ver.ver);
        let m = task.normal.as_ref().unwrap();
        let snap_version = m.get_snap_version();
        let tag = ShardTag::new(self.get_engine_id(), task.id_ver);
        let max_ts = m.data_max_ts();
        info!(
            "{} flush mem-table snap_version {}, size {}, data max ts {}",
            tag,
            snap_version,
            m.size(),
            max_ts,
        );
        let flush = cs.mut_flush();
        flush.set_version(snap_version.into_inner());
        flush.set_max_ts(max_ts);
        if let Some(props) = m.get_properties() {
            let mut filtered_props = kvenginepb::Properties::default();
            debug_assert_eq!(props.get_keys().len(), props.get_values().len());
            for (key, mut val) in props.keys.into_iter().zip(props.values.into_iter()) {
                if is_property_need_flush(&key) {
                    if key == TXN_FILE_REF {
                        val = clear_finished_txn_files(&tag, val, snap_version);
                    }
                    filtered_props.mut_keys().push(key);
                    filtered_props.mut_values().push(val);
                }
            }
            flush.set_properties(filtered_props);
        }
        let results = self.build_l0_tables(m, &task).await;
        if results.is_empty() {
            return Ok(cs);
        }
        let num_l0s = results.len();
        let (tx, mut rx) = unbounded_channel();
        for (l0_create, data) in results {
            self.persist_table(l0_create, data, tx.clone(), task.id_ver);
        }
        let mut errs = vec![];
        for _ in 0..num_l0s {
            match rx.recv().await.unwrap() {
                Ok(l0_create) => {
                    if self.opts.table_builder_options.flush_split_l0 {
                        flush.mut_l0_creates().push(l0_create);
                    } else {
                        flush.set_l0_create(l0_create);
                    }
                }
                Err(err) => {
                    errs.push(err);
                }
            }
        }
        if !errs.is_empty() {
            error!("{} flush mem-tables failed {:?}", tag, errs);
            return Err(errs.pop().unwrap());
        }
        Ok(cs)
    }

    pub(crate) async fn flush_initial(&self, mut task: FlushTask) -> Result<pb::ChangeSet> {
        fail_point!("kvengine_flush_initial");
        let flush = task.initial.take().unwrap();
        let tag = ShardTag::new(self.get_engine_id(), task.id_ver);
        info!(
            "{} initial flush {} mem-tables, base_version {}, data_sequence {}, columnar_table_ids {:?}",
            tag,
            flush.mem_tbls.len(),
            flush.base_version,
            flush.data_sequence,
            flush.columnar_table_ids
        );
        let mut cs = new_change_set(task.id_ver.id, task.id_ver.ver);
        let initial_flush = cs.mut_initial_flush();
        initial_flush.set_outer_start(task.range.outer_start.to_vec());
        initial_flush.set_outer_end(task.range.outer_end.to_vec());
        initial_flush.set_inner_key_off(flush.shard_data.inner_key_off as u32);
        initial_flush.set_base_version(flush.base_version);
        initial_flush.set_data_sequence(flush.data_sequence);
        initial_flush.set_max_ts(flush.max_ts);
        if let Some(props) = flush.properties {
            let flush_version = SnapVersion::new(flush.base_version, flush.data_sequence);
            let mut filtered_props = kvenginepb::Properties::default();
            debug_assert_eq!(props.get_keys().len(), props.get_values().len());
            for (key, mut val) in props.keys.into_iter().zip(props.values.into_iter()) {
                if is_property_need_initial_flush(&key) {
                    if key == TXN_FILE_REF {
                        val = clear_finished_txn_files(&tag, val, flush_version);
                    }
                    filtered_props.mut_keys().push(key);
                    filtered_props.mut_values().push(val);
                }
            }
            initial_flush.set_properties(filtered_props);
        }
        let mut l0_ids = HashSet::new();
        for l0 in &flush.shard_data.l0_tbls {
            if task.table_double_overbound(l0.smallest(), l0.biggest())
                && !l0.has_data_in_bound(task.range.data_bound())
            {
                // only double overbound tables may not have any data in the shard range.
                continue;
            }
            l0_ids.insert(l0.id());
            initial_flush.mut_l0_creates().push(l0.to_l0_create());
        }
        for cf in 0..NUM_CFS {
            let scf = flush.shard_data.get_cf(cf);
            for lvl in &scf.levels {
                for tbl in lvl.tables.iter() {
                    if task.table_double_overbound(tbl.smallest(), tbl.biggest())
                        && !tbl.has_overlap_async(task.range.data_bound()).await
                    {
                        // only double overbound tables may not have any data in the shard range.
                        continue;
                    }
                    initial_flush
                        .mut_table_creates()
                        .push(tbl.to_table_create(cf, lvl.level));
                }
            }
        }

        for blob in flush.shard_data.blob_tbl_map.values() {
            initial_flush.mut_blob_creates().push(blob.to_blob_create());
        }
        if let Some(schema_file) = &flush.shard_data.schema_file {
            // After split, the schema file may not overlap the schema file anymore.
            if schema_file.overlap(
                &task.range.outer_start,
                &task.range.outer_end,
                task.range.keyspace_id,
            ) {
                initial_flush.set_schema_meta(schema_file.to_schema_meta());

                if !self.opts.ignore_columnar_table_load {
                    // Filter out the unconverted_l0s not in the l0s created in this flush.
                    let unconverted_l0s = flush
                        .shard_data
                        .col_levels
                        .unconverted_l0s
                        .iter()
                        .filter_map(|l0| {
                            if l0_ids.contains(&l0.id()) {
                                Some(l0.id())
                            } else {
                                None
                            }
                        })
                        .collect();
                    initial_flush.set_unconverted_l0s(unconverted_l0s);

                    flush.shard_data.for_each_columnar_level(|cl| {
                        for col_file in cl.files.iter() {
                            // TODO: check col_file has_overlap with task.range to avoid useless
                            // flush.
                            initial_flush
                                .mut_columnar_creates()
                                .push(col_file.to_columnar_create(cl.level));
                        }
                        false
                    });
                    if flush.shard_data.col_levels.l2_snap_version.is_not_zero() {
                        initial_flush.set_columnar_l2_snap_version(
                            flush.shard_data.col_levels.l2_snap_version.into_inner(),
                        );
                    }

                    initial_flush.set_columnar_table_ids(flush.columnar_table_ids.clone());
                    for vec_idx in flush.shard_data.vector_indexes.get_all() {
                        initial_flush
                            .mut_vector_indexes()
                            .push(vec_idx.to_vector_index_pb());
                    }
                }
            }
        }
        // If the shard is restored, we need to retain the restore version.
        if flush.shard_data.restore_version > 0 {
            let schema_meta = initial_flush.mut_schema_meta();
            schema_meta.set_restore_version(flush.shard_data.restore_version);
        }

        let (tx, mut rx) = unbounded_channel();
        let mut send_cnt = 0;
        for m in &flush.mem_tbls {
            let results = self.build_l0_tables(m, &task).await;
            for (l0_create, data) in results {
                self.persist_table(l0_create, data, tx.clone(), task.id_ver);
                send_cnt += 1;
            }
        }
        let mut errs = vec![];
        for _ in 0..send_cnt {
            match rx.recv().await.unwrap() {
                Ok(l0_create) => {
                    if !self.opts.ignore_columnar_table_load
                        && initial_flush.has_schema_meta()
                        && !initial_flush.get_columnar_table_ids().is_empty()
                    {
                        initial_flush.mut_unconverted_l0s().push(l0_create.get_id());
                    }
                    initial_flush.mut_l0_creates().push(l0_create);
                }
                Err(err) => {
                    errs.push(err);
                }
            }
        }
        if errs.is_empty() {
            return Ok(cs);
        }
        Err(errs.pop().unwrap())
    }

    pub(crate) async fn build_l0_tables(
        &self,
        m: &CfTable,
        task: &FlushTask,
    ) -> Vec<(L0Create, Bytes)> {
        let start = task.inner_start();
        let end = task.inner_end();
        let opts = &self.opts.table_builder_options;
        let split_l0_size = opts.max_table_size as u64 * 3 / 2; // use fixed ratio for split l0 size.
        let write_cf_size = m.get_cf(WRITE_CF).size() as u64;
        let (fid_count, l0_builder_start_cf) =
            if opts.flush_split_l0 && write_cf_size > split_l0_size {
                ((write_cf_size / split_l0_size) + 2, LOCK_CF)
            } else {
                (1, WRITE_CF)
            };
        let checksum_type = self.comp_client.checksum_type;
        let mut l0s = vec![];
        let mut fids = self
            .id_allocator
            .alloc_id_async(fid_count as usize)
            .await
            .unwrap();
        let l0_fid = fids.pop().unwrap();
        if l0_builder_start_cf == LOCK_CF {
            let mut write_cf_builder = Builder::new(
                fids.pop().unwrap(),
                opts.block_size,
                NO_COMPRESSION,
                0,
                checksum_type,
                task.encryption_key.clone(),
            );
            write_cf_builder.set_snap_version(m.get_snap_version());
            let mut it = m.get_cf(WRITE_CF).new_iterator(false);
            it.seek(start);
            let mut last_key = vec![];
            while it.valid() {
                let key = it.key();
                if key >= end {
                    break;
                }
                let is_new_key = key.deref() != last_key.as_slice();
                if is_new_key {
                    last_key.truncate(0);
                    last_key.extend_from_slice(key.deref());
                }
                if write_cf_builder.estimated_size() as u64 > split_l0_size && is_new_key {
                    // We can not ensure the order of l0 files with the same version, so we must
                    // ensure a single key's multiple versions are stored
                    // in a single file.
                    l0s.push(Self::finish_builder_for_l0(&mut write_cf_builder));
                    let next_fid = match fids.pop() {
                        Some(fid) => fid,
                        None => self
                            .id_allocator
                            .alloc_id_async(1)
                            .await
                            .unwrap()
                            .pop()
                            .unwrap(),
                    };
                    write_cf_builder.reset(next_fid);
                }
                let v = it.value();
                write_cf_builder.add(key, &v, None);
                it.next_all_version();
            }
            if !write_cf_builder.is_empty() {
                l0s.push(Self::finish_builder_for_l0(&mut write_cf_builder));
            }
        }
        let mut l0_builder = sstable::L0Builder::new(
            l0_fid,
            self.opts.table_builder_options.block_size,
            m.get_snap_version(),
            checksum_type,
            task.encryption_key.clone(),
        );
        for cf in l0_builder_start_cf..NUM_CFS {
            let skl = m.get_cf(cf);
            if skl.is_empty() {
                continue;
            }
            let mut it = skl.new_iterator(false);
            // If CF is not managed, we only need to keep the latest version.
            let rc = !CF_MANAGED[cf];
            let mut prev_key = BytesMut::new();
            it.seek(start);
            while it.valid() {
                let key = it.key();
                if key >= end {
                    break;
                }
                if rc && prev_key == key.deref() {
                    // For read committed CF, we can discard all the old
                    // versions.
                } else {
                    let v = it.value();
                    l0_builder.add(cf, key, &v, None);
                    if rc {
                        prev_key.truncate(0);
                        prev_key.extend_from_slice(key.deref());
                    }
                }
                it.next_all_version();
            }
        }
        if !l0_builder.is_empty() {
            l0s.push(l0_builder.finish());
        }
        if l0s.len() > 1 {
            let tag = ShardTag::new(self.get_engine_id(), task.id_ver);
            info!("{} flush split to {} files", tag, l0s.len());
        }
        l0s
    }

    fn finish_builder_for_l0(builder: &mut Builder) -> (L0Create, Bytes) {
        let mut data_buf = Vec::with_capacity(builder.estimated_size());
        let mut res = builder.finish(0, &mut data_buf);
        let l0_create = new_l0_create_pb(
            res.id,
            mem::take(&mut res.smallest),
            mem::take(&mut res.biggest),
            data_buf.len() as u32,
        );
        (l0_create, data_buf.into())
    }

    pub(crate) fn persist_table(
        &self,
        l0_create: pb::L0Create,
        data: Bytes,
        tx: Sender<Result<pb::L0Create>>,
        id_ver: IdVer,
    ) {
        let fs_clone = self.fs.clone();
        self.fs.get_runtime().spawn(async move {
            let res = fs_clone
                .create(
                    l0_create.get_id(),
                    data,
                    dfs::Options::default().with_shard(id_ver.id, id_ver.ver),
                )
                .await;
            if let Err(e) = res {
                let _ = tx
                    .send(Err(e.into()))
                    .map_err(|e| warn!("{} send error {:?}", id_ver, e));
                return;
            }
            let _ = tx
                .send(Ok(l0_create))
                .map_err(|e| warn!("{} send error {:?}", id_ver, e));
        });
    }
}

pub(crate) enum FlushMsg {
    /// Tasks is send when trigger_flush is called.
    /// Flush tasks must be in ascending order by version of mem tables.
    Tasks(Vec<FlushTask>),

    /// Result is sent from the background flush thread when a flush task is
    /// finished.
    Result(FlushResult),

    /// Committed message is sent when a flush is committed to the raft group,
    /// so we can notify the next finished task.
    Committed((IdVer, SnapVersion)),

    /// Clear message is sent when a shard changed its version or set to
    /// inactive. Then all the previous tasks will be discarded.
    /// This simplifies the logic, avoid race condition.
    Clear(u64),

    /// Stop background flush thread.
    Stop,
}

// FlushManager manages the flush tasks, make them concurrent and ensure the
// order for each shard.
pub(crate) struct FlushWorker {
    shards: HashMap<u64, ShardTaskManager>,
    receiver: mpsc::Receiver<FlushMsg>,
    engine: Engine,
    pool: tokio::runtime::Runtime,
}

impl FlushWorker {
    fn run(&mut self) {
        while let Ok(msg) = self.receiver.recv() {
            match msg {
                FlushMsg::Tasks(tasks) => {
                    for task in tasks {
                        let task_manager = self.get_shard_task_manager(task.id_ver.id);
                        if task_manager.enqueue_task(&task) {
                            let term = task_manager.term;
                            self.spawn_flush_task(task, term);
                        }
                    }
                }
                FlushMsg::Result(res) => {
                    let tag = ShardTag::new(self.engine.get_engine_id(), res.id_ver);
                    let task_manager = self.get_shard_task_manager(res.id_ver.id);
                    match task_manager.handle_flush_result(tag, res) {
                        Either::Left(finished) => {
                            if let Some(finished) = finished {
                                self.engine.meta_change_listener.on_change_set(finished);
                            }
                        }
                        Either::Right(task) => {
                            let term = task_manager.term;
                            self.spawn_flush_task(task, term);
                        }
                    }
                }
                FlushMsg::Committed((id_ver, snap_version)) => {
                    let task_manager = self.get_shard_task_manager(id_ver.id);
                    if let Some(finished) = task_manager.handle_committed(snap_version) {
                        self.engine.meta_change_listener.on_change_set(finished);
                    }
                }
                FlushMsg::Clear(shard_id) => {
                    self.shards.remove(&shard_id);
                }
                FlushMsg::Stop => {
                    info!(
                        "Engine {} flush worker receive stop msg and stop now",
                        self.engine.get_engine_id()
                    );
                    break;
                }
            }
        }
    }

    fn get_shard_task_manager(&mut self, id: u64) -> &mut ShardTaskManager {
        self.shards
            .entry(id)
            .or_insert_with(|| ShardTaskManager::new())
    }

    fn spawn_flush_task(&mut self, task: FlushTask, term: u64) {
        let engine = self.engine.clone();
        self.pool.spawn(tikv_util::init_task_local(async move {
            let snap_version = task.snap_version();
            let id_ver = task.id_ver;
            tikv_util::set_current_region(id_ver.id);
            let res = if task.normal.is_some() {
                engine.flush_normal(task).await
            } else {
                engine.flush_initial(task).await
            };
            engine.send_flush_msg(FlushMsg::Result(FlushResult {
                id_ver,
                snap_version,
                term,
                res,
            }));
        }));
    }
}

/// ShardTaskManager manage flush tasks for a shard.
#[derive(Default)]
pub(crate) struct ShardTaskManager {
    /// When a Shard is set to inactive, all the running tasks should be
    /// discarded, then when it is set to active again, old result may
    /// arrive and conflict with the new tasks. So we use term to detect and
    /// discard obsolete tasks.
    term: u64,
    /// task_queue contains the running tasks.
    /// Incoming flush tasks are pushed back to the queue.
    task_queue: VecDeque<FlushTask>,
    /// finished contains tasks that successfully flushed, but not yet notified
    /// to the meta listener.
    finished: HashMap<SnapVersion, kvenginepb::ChangeSet>,
    /// The flush notified the meta listener but not yet committed.
    notified: Option<kvenginepb::ChangeSet>,
    /// committed_snap_version is updated when the notified change set is
    /// committed in the raft group.
    committed_snap_version: SnapVersion,
}

impl ShardTaskManager {
    fn new() -> Self {
        let term = timespec_to_ns(monotonic_raw_now());
        Self {
            term,
            ..Default::default()
        }
    }

    fn enqueue_task(&mut self, task: &FlushTask) -> bool {
        if task.snap_version() <= self.last_enqueued_snap_version() {
            debug!("{} flush task dropped", task.id_ver;
                "task.snap_version" => task.snap_version(),
                "last_task" => self.task_queue.back().map(|t| t.snap_version()),
                "notified" => self.notified.as_ref().map(|cs| change_set_snap_version(cs)),
                "committed" => self.committed_snap_version,
            );
            return false;
        }
        self.task_queue.push_back(task.clone());
        true
    }

    fn last_enqueued_snap_version(&self) -> SnapVersion {
        if let Some(task) = self.task_queue.back() {
            task.snap_version()
        } else if let Some(notified) = self.notified.as_ref() {
            change_set_snap_version(notified)
        } else {
            self.committed_snap_version
        }
    }

    fn handle_flush_result(
        &mut self,
        tag: ShardTag,
        res: FlushResult,
    ) -> Either<Option<kvenginepb::ChangeSet>, FlushTask> {
        if self.term != res.term {
            info!("{} discard old term flush result {:?}", tag, res.res);
            return Either::Left(None);
        }
        let snap_version = res.snap_version;
        match res.res {
            Ok(cs) => {
                self.finished.insert(snap_version, cs);
                Either::Left(
                    self.notified
                        .is_none()
                        .then(|| self.take_finished_task_for_notify())
                        .flatten(),
                )
            }
            Err(err) => {
                info!(
                    "{} flush task failed, snap version {}, error {:?}, retrying",
                    tag, res.snap_version, err
                );
                let task = self
                    .task_queue
                    .iter()
                    .find(|task| task.snap_version() == snap_version)
                    .expect("failed task should exist")
                    .clone();
                Either::Right(task)
            }
        }
    }

    fn handle_committed(&mut self, snap_version: SnapVersion) -> Option<kvenginepb::ChangeSet> {
        if self.committed_snap_version < snap_version {
            self.committed_snap_version = snap_version;
        }
        if let Some(notified) = self.notified.take() {
            let notified_snap_version = change_set_snap_version(&notified);
            if notified_snap_version == snap_version {
                return self.take_finished_task_for_notify();
            }
            self.notified = Some(notified);
        }
        None
    }

    fn take_finished_task_for_notify(&mut self) -> Option<kvenginepb::ChangeSet> {
        if let Some(task) = self.task_queue.front() {
            if let Some(cs) = self.finished.remove(&task.snap_version()) {
                self.task_queue.pop_front();
                self.notified = Some(cs.clone());
                return Some(cs);
            }
        }
        None
    }
}

pub(crate) fn change_set_snap_version(cs: &kvenginepb::ChangeSet) -> SnapVersion {
    if cs.has_flush() {
        return cs.get_flush().version.into();
    } else if cs.has_initial_flush() {
        let initial_flush = cs.get_initial_flush();
        return SnapVersion::new(initial_flush.base_version, initial_flush.data_sequence);
    }
    unreachable!("unexpected change set {:?}", cs);
}

pub(crate) fn clear_finished_txn_files(
    tag: &ShardTag,
    v: Vec<u8>,
    version: SnapVersion,
) -> Vec<u8> {
    let mut prop = TxnFileRefPropertyHelper::from_property(Some(Bytes::from(v))).unwrap();
    prop.clear_finished_txn_file_lock(version);
    debug!("{} flush mem-table: clear finished txn files", tag; "prop" => ?prop, "version" => version);
    prop.marshall()
}

pub(crate) struct FlushResult {
    id_ver: IdVer,
    snap_version: SnapVersion,
    term: u64,
    res: Result<kvenginepb::ChangeSet>,
}
