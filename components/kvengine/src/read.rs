// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use core::panic;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::{Debug, Formatter},
    iter::Iterator as _,
    marker::PhantomData,
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use api_version::api_v2::KEYSPACE_PREFIX_LEN;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use cloud_encryption::EncryptionKey;
use kvenginepb as pb;
use kvenginepb::TxnFileRefs;
use log_wrappers::Value as LogValue;
use protobuf::Message;
use tidb_query_datatype::{
    codec::{
        mysql::VectorFloat32Decoder,
        table::{decode_common_handle, decode_int_handle, decode_table_id},
    },
    FieldTypeFlag,
};
use tikv_util::{
    box_try,
    codec::number::{U32_SIZE, U64_SIZE},
    memory::{MemoryLimiter, MemoryLimiterGuard},
    mpsc,
    time::Instant,
};
use tipb::{AnnQueryInfo, ColumnInfo, FtsQueryInfo};
use tokio::runtime::Handle;
use txn_types::Lock;

use crate::{
    context::{IaCtx, SnapCtx},
    dfs::FileType,
    ia::types::FileSegmentIdent,
    limiter::RegionLimiter,
    metrics::{COLUMNAR_PREFETCH_CACHE_HIT_HISTOGRAM, COLUMNAR_PREFETCH_HISTOGRAM},
    table::{
        blobtable::blobtable::{BlobPrefetcher, BlobTable},
        columnar::{
            filter::TableScanCtx, Block, ColumnarConcatReader, ColumnarFile, ColumnarFilterReader,
            ColumnarMergeReader, ColumnarMvccReader, ColumnarReader, ColumnarRowTableReader,
            ColumnarTableReader, GLOBAL_COMMON_HANDLE_END, HANDLE_COL_ID,
        },
        fts_index::FtsBruteForceReader,
        memtable::{CfTable, Hint, SkipList, WriteBatch},
        schema_file::{Schema, SchemaBuf, SchemaFile},
        sstable::SsTable,
        table,
        vector_index::{VectorDistanceProjector, VectorItemsReader},
        AsyncMergeIterator, BoundedDataSet, ConstraintChecker, DataBound, InnerKey,
        Iterator as TableIterator, SkipOpTxnFileIterator, SnapVersion, TxnFile, TxnFileIterator,
        Value,
    },
    table_id::encode_table_prefix_key,
    txn_chunk_manager::TxnChunkManager,
    *,
};

/// Mem data format V1: format_ver,skls_data
const MEM_DATA_FORMAT_V1: u32 = 1;
/// Mem data format V2:
/// format_ver,skls_length,skls_data,txn_file_refs_length,txn_file_refs_data
const MEM_DATA_FORMAT_V2: u32 = 2;

pub struct Item<'a> {
    val: table::Value,
    pub path: AccessPath,
    phantom: PhantomData<&'a i32>,

    // Uses to hold the value's memory when necessary, so that the life time of val can be at least
    // long as the life time of Item itself. This is necessary when the caller is not responsible
    // (or impossible) to manage the life time. e.g. During point get, caller does not hold the
    // memory as in scan (held by the iterator).
    owned_val: Option<Vec<u8>>,
    owned_blob: Option<Vec<u8>>,
}

impl std::ops::Deref for Item<'_> {
    type Target = table::Value;

    fn deref(&self) -> &Self::Target {
        &self.val
    }
}

impl Item<'_> {
    fn new() -> Self {
        Self {
            val: table::Value::new(),
            path: AccessPath::default(),
            phantom: Default::default(),
            owned_val: None,
            owned_blob: None,
        }
    }

    pub fn from_cached_value(cached: &ValueCacheValue) -> Self {
        let um = UserMeta::new(cached.start_ts, cached.version);
        let um_buf = um.to_array();
        let buf = Value::encode_buf(0, &um_buf, cached.version, cached.value.chunk());
        let val = Value::decode(&buf);
        Self {
            val,
            path: Default::default(),
            phantom: Default::default(),
            owned_val: Some(buf),
            owned_blob: None,
        }
    }
}

impl Default for Item<'_> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct AccessPath {
    pub mem_table: u8,
    pub l0: u8,
    pub ln: u8,
}

#[derive(Clone)]
pub struct SnapAccess {
    pub core: Arc<SnapAccessCore>,
}

impl SnapAccess {
    pub fn new(shard: &Shard) -> Self {
        let core = Arc::new(SnapAccessCore::new(shard));
        Self { core }
    }

    pub async fn from_change_set(
        tag: &str,
        ctx: &SnapCtx,
        change_set: pb::ChangeSet,
        write_cf_only: bool,
    ) -> Result<Self> {
        let core = Arc::new(
            SnapAccessCore::from_change_set(tag, ctx, change_set, vec![], write_cf_only).await?,
        );
        Ok(Self { core })
    }

    async fn from_change_set_and_memtable_data(
        tag: &str,
        ctx: &SnapCtx,
        change_set: pb::ChangeSet,
        mem_tbls: Vec<CfTable>,
    ) -> Result<Self> {
        let core =
            Arc::new(SnapAccessCore::from_change_set(tag, ctx, change_set, mem_tbls, true).await?);
        Ok(Self { core })
    }

    pub async fn construct_snapshot<'a>(
        tag: &str,
        ctx: &SnapCtx,
        mem_table_data: &[u8],
        snapshot: &[u8],
        memory_limiter: MemoryLimiter,
    ) -> Result<(Self, MemoryLimiterGuard)> {
        let mut change_set = kvenginepb::ChangeSet::default();
        change_set.merge_from_bytes(snapshot).unwrap();
        if !change_set.has_snapshot() {
            error!("no snapshot in change set: {:?}", change_set);
            return Err(Error::RemoteRead("no snapshot in change set".to_string()));
        }

        let snap = change_set.get_snapshot();

        let mut tables_size = mem_table_data.len();
        if matches!(ctx.ia_ctx, IaCtx::Disabled) {
            tables_size += estimate_tables_size_from_snapshot(snap, ctx.prepare_type);
        }
        let mem_limiter_guard = match memory_limiter.acquire(tables_size as u64) {
            Ok(guard) => guard,
            Err(exceeded_size) => {
                warn!("{} memory limit exceeded", tag;
                    "tables_size" => tables_size, "exceeded" => exceeded_size,
                    "limiter" => ?memory_limiter);
                return Err(Error::MemoryLimitExceeded(exceeded_size));
            }
        };

        let encryption_key = get_shard_property(ENCRYPTION_KEY, snap.get_properties())
            .map(|v| ctx.master_key.decrypt_encryption_key(&v).unwrap());

        let shard_id = change_set.shard_id;
        let shard_ver = change_set.shard_ver;
        let mem_tbls = Self::construct_memtables(
            shard_id,
            shard_ver,
            mem_table_data,
            &ctx.txn_chunk_manager,
            encryption_key,
        )
        .await?;
        let snap = Self::from_change_set_and_memtable_data(tag, ctx, change_set, mem_tbls).await?;
        Ok((snap, mem_limiter_guard))
    }

    pub async fn construct_memtables(
        shard_id: u64,
        shard_ver: u64,
        mut mem_table_data: &[u8],
        txn_chunk_manager: &TxnChunkManager,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Vec<CfTable>> {
        if mem_table_data.is_empty() {
            return Ok(vec![]);
        }

        let format_version = mem_table_data.get_u32_le();
        if format_version == MEM_DATA_FORMAT_V1 {
            let mut mem_tbl = CfTable::new();
            Self::construct_skip_list(mem_table_data, &mut mem_tbl)?;
            Ok(vec![mem_tbl])
        } else if format_version == MEM_DATA_FORMAT_V2 {
            Self::construct_memtables_format_v2(
                shard_id,
                shard_ver,
                mem_table_data,
                txn_chunk_manager,
                encryption_key,
            )
            .await
        } else {
            Err(Error::RemoteRead(format!(
                "unsupported mem data format {}",
                format_version
            )))
        }
    }

    fn construct_skip_list(skl_data: &[u8], mem_tbl: &mut CfTable) -> Result<()> {
        let mut wb = WriteBatch::new();
        let rows: Vec<table::Row> = bincode::deserialize(skl_data).map_err(|e| {
            Error::RemoteRead(format!("failed to deserialize mem table data: {}", e))
        })?;
        for row in rows {
            let key = InnerKey::from_outer_key(&row.key);
            wb.put(
                key,
                0,
                &row.user_meta.to_array(),
                row.user_meta.commit_ts,
                &row.value,
            );
        }
        mem_tbl.get_cf(WRITE_CF).put_batch(&mut wb, None, WRITE_CF);
        Ok(())
    }

    async fn construct_memtables_format_v2(
        shard_id: u64,
        shard_ver: u64,
        mut mem_data: &[u8],
        txn_chunk_manager: &TxnChunkManager,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Vec<CfTable>> {
        let mut mem_tbl = CfTable::new();

        // Skiplists:
        let mem_size = mem_data.get_u64_le() as usize;
        let skl_data = &mem_data[..mem_size];
        mem_data.advance(mem_size);
        Self::construct_skip_list(skl_data, &mut mem_tbl)?;

        // Txn file refs:
        let msg_len = mem_data.get_u32_le() as usize;
        let msg_data = &mem_data[..msg_len];
        mem_data.advance(msg_len);

        let mut txn_file_refs = TxnFileRefs::default();
        box_try!(txn_file_refs.merge_from_bytes(msg_data));
        if !txn_file_refs.get_txn_file_refs().is_empty() {
            let worker_pool = txn_chunk_manager.worker_pool().clone();
            let txn_chunk_manager = txn_chunk_manager.clone();
            let txn_files = worker_pool
                .spawn_blocking(move || {
                    txn_chunk_manager.load_txn_files_from_refs(
                        shard_id,
                        shard_ver,
                        txn_file_refs.get_txn_file_refs(),
                        encryption_key,
                    )
                })
                .await
                .unwrap()?;
            mem_tbl = mem_tbl.add_write_cf_txn_files(&txn_files);
        }

        debug_assert!(
            mem_data.is_empty(),
            "mem_data: {:?}",
            LogValue::value(mem_data)
        );
        Ok(vec![mem_tbl])
    }
}

#[cfg(any(test, feature = "testexport"))]
impl SnapAccess {
    pub fn display(&self) -> pb::ChangeSet {
        let outer_range = (self.data.outer_start.clone(), self.data.outer_end.clone());
        self.to_change_set(&[outer_range], false)
    }

    pub fn write_cf_level_n_is_empty(&self) -> bool {
        self.data
            .get_cf(WRITE_CF)
            .levels
            .iter()
            .all(|lh| lh.tables.is_empty())
    }
}

impl Deref for SnapAccess {
    type Target = SnapAccessCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl Debug for SnapAccess {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "snap access {}, seq: {}", self.tag, self.write_sequence,)
    }
}

impl BoundedDataSet for SnapAccess {
    fn data_bound(&self) -> DataBound<'_> {
        self.data.range.data_bound()
    }
}

pub struct SnapAccessCore {
    tag: ShardTag,
    managed_ts: u64,
    base_version: u64,
    meta_seq: u64,
    write_sequence: u64,
    data: ShardData,
    get_hint: Mutex<Hint>,
    blob_table_prefetch_size: usize,
    deleting_prefixes: Arc<DeletePrefixes>,
    encryption_key: Option<EncryptionKey>,
    is_sync: bool,
    read_columnar: bool,
}

impl SnapAccessCore {
    pub fn new(shard: &Shard) -> Self {
        let base_version = shard.get_base_version();
        let meta_seq = shard.get_meta_sequence();
        let write_sequence = shard.get_write_sequence();
        let data = shard.get_data();
        let is_sync = data.is_sync();
        Self {
            tag: shard.tag(),
            write_sequence,
            meta_seq,
            base_version,
            managed_ts: 0,
            data,
            get_hint: Mutex::new(Hint::new()),
            blob_table_prefetch_size: shard.opt.blob_prefetch_size,
            deleting_prefixes: shard.get_del_prefixes(),
            encryption_key: shard.encryption_key.clone(),
            is_sync,
            read_columnar: shard.opt.read_columnar,
        }
    }

    pub async fn from_change_set(
        tag: &str,
        ctx: &SnapCtx,
        change_set: pb::ChangeSet,
        mem_tbls: Vec<CfTable>,
        write_cf_only: bool,
    ) -> Result<Self> {
        let shard = Shard::from_change_set(tag, ctx, change_set, mem_tbls, write_cf_only).await?;
        Ok(Self::new(&shard))
    }

    #[maybe_async::both]
    pub async fn new_iterator_skip_blob(
        &self,
        cf: usize,
        reversed: bool,
        all_versions: bool,
        read_ts: Option<u64>,
        fill_cache: bool,
    ) -> Iterator {
        let read_ts = if let Some(ts) = read_ts {
            ts
        } else if CF_MANAGED[cf] && self.managed_ts != 0 {
            self.managed_ts
        } else {
            u64::MAX
        };
        let data = self.data.clone();
        let mut key = BytesMut::new();
        key.extend_from_slice(data.keyspace_prefix());
        Iterator {
            all_versions,
            reversed,
            read_ts,
            key,
            val: table::Value::new(),
            inner: self
                .new_table_iterator(cf, reversed, fill_cache, None)
                .await,
            blob_prefetcher: None,
            data,
            range: None,
        }
    }

    #[maybe_async::both]
    pub async fn new_iterator(
        &self,
        cf: usize,
        reversed: bool,
        all_versions: bool,
        read_ts: Option<u64>,
        fill_cache: bool,
    ) -> Iterator {
        let blob_prefetcher = Some(BlobPrefetcher::new(
            self.data.blob_tbl_map.clone(),
            self.blob_table_prefetch_size,
            self.encryption_key.clone(),
        ));
        let data = self.data.clone();
        let mut key = BytesMut::new();
        key.extend_from_slice(data.keyspace_prefix());
        Iterator {
            all_versions,
            reversed,
            read_ts: self.get_read_ts(cf, read_ts),
            key,
            val: table::Value::new(),
            inner: self
                .new_table_iterator(cf, reversed, fill_cache, None)
                .await,
            blob_prefetcher,
            data,
            range: None,
        }
    }

    pub fn new_memtable_iterator(
        &self,
        cf: usize,
        reversed: bool,
        all_versions: bool,
        read_ts: Option<u64>,
    ) -> Iterator {
        let data = self.data.clone();
        let mut key = BytesMut::new();
        key.extend_from_slice(data.keyspace_prefix());
        Iterator {
            all_versions,
            reversed,
            read_ts: self.get_read_ts(cf, read_ts),
            key,
            val: table::Value::new(),
            inner: self.new_mem_table_iterator(cf, reversed),
            blob_prefetcher: None,
            data,
            range: None,
        }
    }

    fn get_read_ts(&self, cf: usize, read_ts: Option<u64>) -> u64 {
        if let Some(ts) = read_ts {
            ts
        } else if CF_MANAGED[cf] && self.managed_ts != 0 {
            self.managed_ts
        } else if !CF_MANAGED[cf] {
            self.get_mem_table_snap_version().into_inner()
        } else {
            u64::MAX
        }
    }

    fn fetch_blob(&self, key: InnerKey<'_>, val: &table::Value) -> Vec<u8> {
        assert!(val.is_blob_ref());
        let blob_ref = val.get_blob_ref();
        let blob_table = self
            .data
            .blob_tbl_map
            .get(&blob_ref.fid)
            .unwrap_or_else(|| {
                panic!(
                    "[{}] blob table not found {:?}, blob table id: {}",
                    self.tag, key, blob_ref.fid
                )
            });
        // TODO: avoid reallocation
        let mut decryption_buf = vec![];
        blob_table
            .get(&blob_ref, &mut decryption_buf, self.encryption_key.clone())
            .unwrap_or_else(|e| panic!("[{}] blob table get failed {:?} {:?}", self.tag, key, e))
    }

    /// get an Item by key. Caller need to call is_some() before get_value.
    /// We don't return Option because we may need AccessPath even if the item
    /// is none.
    #[maybe_async::both]
    pub async fn get(&self, cf: usize, key: &[u8], version: u64) -> Item<'_> {
        let mut version = version;
        if version == 0 {
            version = u64::MAX;
        }

        let inner_key = InnerKey::from_outer_key(key);
        let mut item = Item::new();
        item.owned_val = Some(vec![]);
        item.val = self
            .get_value(
                cf,
                inner_key,
                version,
                &mut item.path,
                item.owned_val.as_mut().unwrap(),
                &self.data.lock_txn_files,
            )
            .await;
        if item.val.is_blob_ref() {
            item.owned_blob = Some(self.fetch_blob(inner_key, &item.val));
            item.val.fill_in_blob(item.owned_blob.as_ref().unwrap());
        }
        item
    }

    pub fn get_non_txn_file_lock(&self, key: &[u8]) -> Item<'_> {
        let inner_key = InnerKey::from_outer_key(key);
        let mut item = Item::new();
        item.owned_val = Some(vec![]);
        item.val = self.get_value(
            LOCK_CF,
            inner_key,
            u64::MAX,
            &mut item.path,
            item.owned_val.as_mut().unwrap(),
            &[],
        );
        item
    }

    #[maybe_async::both]
    async fn get_value(
        &self,
        cf: usize,
        inner_key: InnerKey<'_>,
        version: u64,
        path: &mut AccessPath,
        out_val_owner: &mut Vec<u8>,
        with_lock_txn_files: &[TxnFile],
    ) -> table::Value {
        self.check_sync(cf).await;
        if cf == LOCK_CF {
            for txn_file in with_lock_txn_files {
                if txn_file.version() > version {
                    continue;
                }
                let (_, val) = txn_file.get_value(inner_key, out_val_owner);
                if val.is_valid() {
                    return val;
                }
            }
        }
        for i in 0..self.data.mem_tbls.len() {
            let tbl = self.data.mem_tbls.as_slice()[i].get_cf(cf);
            let v = if i == 0 && cf == 0 {
                // only use hint for the first mem-table and cf 0.
                let mut hint = self.get_hint.lock().unwrap();
                tbl.get_with_hint(inner_key, version, &mut hint, out_val_owner)
            } else {
                tbl.get(inner_key, version, out_val_owner)
            };
            path.mem_table = path.mem_table.saturating_add(1);
            if v.is_valid() {
                return v;
            }
        }
        let key_hash = farmhash::fingerprint64(inner_key.deref());
        for l0 in &self.data.l0_tbls {
            if let Some(tbl) = &l0.get_cf(cf) {
                if inner_key < tbl.smallest() || tbl.biggest() < inner_key {
                    continue;
                }
                let v = tbl.get(inner_key, version, key_hash, out_val_owner, 0);
                path.l0 = path.l0.saturating_add(1);
                if v.is_valid() {
                    return v;
                }
            }
        }
        let scf = self.data.get_cf(cf);
        for lh in &scf.levels {
            #[allow(clippy::if_same_then_else)]
            let v = if cf == WRITE_CF {
                lh.get(inner_key, version, key_hash, out_val_owner).await
            } else {
                lh.get(inner_key, version, key_hash, out_val_owner)
            };
            path.ln += 1;
            if v.is_valid() {
                return v;
            }
        }
        table::Value::new()
    }

    #[maybe_async::both]
    pub async fn multi_get(&self, cf: usize, keys: &[Vec<u8>], version: u64) -> Vec<Item<'_>> {
        let mut items = Vec::with_capacity(keys.len());
        for key in keys {
            let item = self.get(cf, key, version).await;
            items.push(item);
        }
        items
    }

    pub fn set_managed_ts(&mut self, managed_ts: u64) {
        self.managed_ts = managed_ts;
    }

    fn new_table_iterator(
        &self,
        cf: usize,
        reversed: bool,
        fill_cache: bool,
        skip_txn_file_with_start_ts: Option<u64>,
    ) -> Box<dyn table::Iterator> {
        self.check_sync(cf);
        let mut iters: Vec<Box<dyn table::Iterator>> = Vec::new();
        if cf == LOCK_CF && !self.data.lock_txn_files.is_empty() {
            for txn_file in &self.data.lock_txn_files {
                if skip_txn_file_with_start_ts
                    .map_or(false, |start_ts| txn_file.start_ts() == start_ts)
                {
                    continue;
                }
                let txn_file_iter = TxnFileIterator::new(txn_file.clone(), reversed);
                let skip_op_iter = SkipOpTxnFileIterator::new(txn_file_iter, false, true);
                iters.push(Box::new(skip_op_iter))
            }
        }
        for mem_tbl in &self.data.mem_tbls {
            iters.push(mem_tbl.get_cf(cf).new_iterator(reversed));
        }
        for l0 in &self.data.l0_tbls {
            if let Some(tbl) = &l0.get_cf(cf) {
                iters.push(tbl.new_iterator(reversed, fill_cache));
            }
        }
        let scf = self.data.get_cf(cf);
        for lh in scf.levels.as_slice() {
            if lh.tables.len() == 0 {
                continue;
            }
            if lh.tables.len() == 1 {
                iters.push(lh.tables[0].new_iterator(reversed, fill_cache));
                continue;
            }
            iters.push(Box::new(ConcatIterator::new(
                lh.clone(),
                reversed,
                fill_cache,
            )));
        }
        table::new_merge_iterator(iters, reversed)
    }

    // This method is actually sync but mark as async to corporate with
    // `maybe_async`.
    async fn new_table_iterator_async(
        &self,
        cf: usize,
        reversed: bool,
        fill_cache: bool,
        skip_txn_file_with_start_ts: Option<u64>,
    ) -> Box<dyn table::Iterator> {
        if cf != WRITE_CF {
            return self.new_table_iterator(cf, reversed, fill_cache, skip_txn_file_with_start_ts);
        }

        let sync_merge_iter: Box<dyn table::Iterator> = {
            let mut sync_iters: Vec<Box<dyn table::Iterator>> =
                Vec::with_capacity(self.data.mem_tbls.len() + self.data.l0_tbls.len());
            for mem_tbl in &self.data.mem_tbls {
                sync_iters.push(mem_tbl.get_cf(cf).new_iterator(reversed));
            }
            for l0 in &self.data.l0_tbls {
                if let Some(tbl) = &l0.get_cf(cf) {
                    sync_iters.push(tbl.new_iterator(reversed, fill_cache));
                }
            }
            Box::new(AsyncMergeIterator::new(sync_iters, reversed, true)) as _
        };

        let mut iters = vec![sync_merge_iter];

        let scf = self.data.get_cf(cf);
        for lh in scf.levels.as_slice() {
            if lh.tables.len() == 0 {
                continue;
            }
            if lh.tables.len() == 1 {
                iters.push(lh.tables[0].new_iterator(reversed, fill_cache));
                continue;
            }
            iters.push(Box::new(ConcatIterator::new(
                lh.clone(),
                reversed,
                fill_cache,
            )));
        }
        Box::new(AsyncMergeIterator::new(iters, reversed, false))
    }

    pub fn new_mem_table_iterator(&self, cf: usize, reversed: bool) -> Box<dyn table::Iterator> {
        let mut iters: Vec<Box<dyn table::Iterator>> = Vec::new();
        for mem_tbl in &self.data.mem_tbls {
            iters.push(mem_tbl.get_cf(cf).new_iterator(reversed));
        }
        table::new_merge_iterator(iters, reversed)
    }

    /// Create a delta iterator of WRITE_CF that iterates over all entries with
    /// ts > since_ts.
    ///
    /// NOTE: The iterator will still contain entries with ts <= since_ts.
    /// Callers need to filter them out.
    #[maybe_async::both]
    pub async fn new_delta_write_iterator(&self, since_ts: u64) -> Box<dyn table::Iterator> {
        self.check_sync(WRITE_CF).await;
        let sync_merge_iter: Box<dyn table::Iterator> = {
            let mut iters: Vec<Box<dyn table::Iterator>> = Vec::new();
            for mem_tbl in &self.data.mem_tbls {
                if mem_tbl.data_max_ts() > since_ts {
                    iters.push(mem_tbl.get_cf(WRITE_CF).new_delta_write_iterator(since_ts));
                }
            }
            for l0 in &self.data.l0_tbls {
                if let Some(tbl) = &l0.get_cf(WRITE_CF) {
                    if tbl.max_ts > since_ts {
                        iters.push(tbl.new_iterator(false, true));
                    }
                }
            }
            Box::new(AsyncMergeIterator::new(iters, false, true)) as _
        };

        let mut iters = vec![sync_merge_iter];

        let scf = self.data.get_cf(WRITE_CF);
        for lh in scf.levels.as_slice() {
            if lh.tables.len() == 0 || lh.max_ts <= since_ts {
                continue;
            }
            if lh.tables.len() == 1 {
                iters.push(lh.tables[0].new_iterator(false, true));
                continue;
            }
            let tables = lh
                .tables
                .iter()
                .filter(|x| x.max_ts > since_ts)
                .cloned()
                .collect::<Vec<_>>();
            debug_assert!(!tables.is_empty()); // Should not be empty.
            if !tables.is_empty() {
                iters.push(Box::new(ConcatIterator::new_with_tables(
                    tables, false, true,
                )));
            }
        }
        Box::new(AsyncMergeIterator::new(iters, false, false))
    }

    pub fn get_write_sequence(&self) -> u64 {
        self.write_sequence
    }

    pub fn get_mem_table_snap_version(&self) -> SnapVersion {
        SnapVersion::new(self.base_version, self.write_sequence)
    }

    pub fn get_start_key(&self) -> &[u8] {
        self.data.outer_start.chunk()
    }

    pub fn get_end_key(&self) -> &[u8] {
        self.data.outer_end.chunk()
    }

    pub fn key_is_in_range(&self, key: &[u8]) -> bool {
        self.get_start_key() <= key && key < self.get_end_key()
    }

    pub fn get_inner_start(&self) -> InnerKey<'_> {
        self.data.inner_start()
    }

    pub fn get_inner_end(&self) -> InnerKey<'_> {
        self.data.inner_end()
    }

    pub fn clone_start_key(&self) -> Bytes {
        self.data.outer_start.clone()
    }

    pub fn clone_end_key(&self) -> Bytes {
        self.data.outer_end.clone()
    }

    pub fn get_inner_key_offset(&self) -> usize {
        self.data.inner_key_off
    }

    pub fn get_tag(&self) -> ShardTag {
        self.tag
    }

    pub fn get_id(&self) -> u64 {
        self.tag.id_ver.id
    }

    pub fn get_version(&self) -> u64 {
        self.tag.id_ver.ver
    }

    pub fn get_meta_seq(&self) -> u64 {
        self.meta_seq
    }

    pub fn get_columnar_levels(&self) -> Vec<(ColumnarFile, usize)> {
        self.data
            .col_levels
            .levels
            .iter()
            .flat_map(|l| {
                l.files
                    .iter()
                    .map(|f| (f.clone(), l.level))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    // Sync only.
    pub(crate) fn contains_in_older_table(&self, key: InnerKey<'_>, cf: usize) -> bool {
        self.check_sync(cf);
        let key_hash = farmhash::fingerprint64(key.deref());
        let mut outer_val_owner = vec![];
        for tbl in &self.data.mem_tbls[1..] {
            let val = tbl.get_cf(cf).get(key, u64::MAX, &mut outer_val_owner);
            if val.is_valid() {
                return !val.is_deleted();
            }
        }
        for l0 in &self.data.l0_tbls {
            let l0_cf = l0.get_cf(cf);
            if l0_cf.is_none() {
                continue;
            }
            let l0_cf = l0_cf.as_ref().unwrap();
            let mut owned_val = vec![];
            let val = l0_cf.get(key, u64::MAX, key_hash, &mut owned_val, 0);
            if val.is_valid() {
                return !val.is_deleted();
            }
        }
        for l in self.data.get_cf(cf).levels.as_slice() {
            if let Some(tbl) = l.get_table(key) {
                let mut owned_val = vec![];
                let val = tbl.get(key, u64::MAX, key_hash, &mut owned_val, l.level);
                if val.is_valid() {
                    return !val.is_deleted();
                }
            }
        }
        false
    }

    /// NOTE: `ChangeSet.snapshot.data_sequence` is not set, as it's not able to
    /// get the accurate data sequence of ShardMeta from Shard.
    fn to_change_set(&self, outer_ranges: &[(Bytes, Bytes)], write_cf_only: bool) -> pb::ChangeSet {
        let mut cs = new_change_set(self.get_tag().id_ver.id, self.get_tag().id_ver.ver);
        cs.set_sequence(self.meta_seq);

        let snap = cs.mut_snapshot();
        let mut properties = pb::Properties::new();
        properties.shard_id = self.get_tag().id_ver.id;
        if let Some(encryption_key) = &self.encryption_key {
            properties.mut_keys().push(ENCRYPTION_KEY.to_string());
            properties.mut_values().push(encryption_key.export());
        }

        snap.set_outer_start(self.get_start_key().to_vec());
        snap.set_outer_end(self.get_end_key().to_vec());
        snap.set_inner_key_off(self.data.inner_key_off as u32);
        snap.set_properties(properties);
        let mut count = 0;
        let mut overlapped_count = 0;
        let mut l0_ids = HashSet::new();
        let mut range_bounds = Vec::with_capacity(outer_ranges.len());
        for (outer_start, outer_end) in outer_ranges {
            let inner_start = InnerKey::from_outer_key(outer_start);
            let inner_end = InnerKey::from_outer_end_key(outer_end);
            range_bounds.push(DataBound::new(inner_start, inner_end, false));
        }
        for v in &self.data.l0_tbls {
            count += 1;
            if write_cf_only {
                if let Some(cf) = v.get_cf(WRITE_CF) {
                    if cf.size() == 0 {
                        continue;
                    }
                } else {
                    continue;
                }
            }
            if !range_bounds.iter().any(|bound| v.overlap_bound(*bound)) {
                continue;
            }
            overlapped_count += 1;
            snap.mut_l0_creates().push(v.to_l0_create());
            l0_ids.insert(v.id());
        }
        for (k, v) in self.data.blob_tbl_map.iter() {
            assert_eq!(k, &v.id());
            count += 1;
            // FIXME: Overlap check
            snap.mut_blob_creates().push(v.to_blob_create());
        }
        self.data.for_each_level(|cf, lh| {
            if write_cf_only && cf != WRITE_CF {
                return false;
            }
            let mut overlap_ids = HashSet::new();
            for bound in &range_bounds {
                let (left, right) = bound.get_overlap_data_sets(&lh.tables);
                if left == right {
                    continue;
                }
                for tbl in &lh.tables[left..right] {
                    overlap_ids.insert(tbl.id());
                }
            }
            for tbl in lh.tables.iter() {
                count += 1;
                if overlap_ids.contains(&tbl.id()) {
                    overlapped_count += 1;
                    snap.mut_table_creates()
                        .push(tbl.to_table_create(cf, lh.level));
                }
            }
            false
        });
        self.data.for_each_columnar_level(|cl| {
            for col_file in cl.files.iter() {
                count += 1;
                // check overlap
                let mut overlap = false;
                for (outer_start, outer_end) in outer_ranges {
                    let inner_start = InnerKey::from_outer_key(outer_start);
                    let inner_end = InnerKey::from_outer_end_key(outer_end);
                    if col_file.has_data_in_range(inner_start, inner_end) {
                        overlap = true;
                        break;
                    }
                }
                if !overlap {
                    continue;
                }
                overlapped_count += 1;
                snap.mut_columnar_creates()
                    .push(col_file.to_columnar_create(cl.level));
            }
            false
        });
        self.data.col_levels.unconverted_l0s.iter().for_each(|l0| {
            // l0 may be skip due to non-overlap
            if l0_ids.contains(&l0.id()) {
                snap.mut_unconverted_l0s().push(l0.id());
            }
        });
        if let Some(schema_file) = &self.data.schema_file {
            snap.set_schema_meta(schema_file.to_schema_meta());
        }
        snap.set_columnar_table_ids(self.data.columnar_table_ids.clone());
        snap.set_columnar_l2_snap_version(self.data.col_levels.l2_snap_version.into_inner());
        let vector_indexes = snap.mut_vector_indexes();
        for index in self.data.vector_indexes.get_all() {
            vector_indexes.push(index.to_vector_index_pb());
        }

        let vector_indexes_count = vector_indexes.iter().map(|v| v.files.len()).sum::<usize>();
        info!(
            "{} convert snap access to change set, total {}, overlapped {}, unconverted_l0s {}, columnar {}, vector {}",
            self.get_tag(),
            count,
            overlapped_count,
            snap.get_unconverted_l0s().len(),
            snap.get_columnar_creates().len(),
            vector_indexes_count,
        );
        cs
    }

    pub fn marshal(
        &self,
        ranges: &[(Bytes, Bytes)],
        write_cf_only: bool,
        cache_key: bool,
    ) -> (String, Vec<u8>) {
        let cs = self.to_change_set(ranges, write_cf_only);
        let key = if cache_key {
            let mut ranges_bytes = Vec::new();
            for (start, end) in ranges {
                ranges_bytes.append(start.to_vec().as_mut());
                ranges_bytes.append(end.to_vec().as_mut());
            }
            let ranges_key = hex::encode(ranges_bytes);
            format!(
                "{}:{}:{}:{}",
                cs.shard_id, cs.shard_ver, cs.sequence, ranges_key,
            )
        } else {
            "".to_string()
        };
        (key, cs.write_to_bytes().unwrap())
    }

    pub fn build_mem_data(&self, outer_ranges: &[(Bytes, Bytes)], start_ts: u64) -> Vec<u8> {
        let data_bounds: Vec<DataBound<'_>> = outer_ranges
            .iter()
            .map(|range| {
                DataBound::new(
                    InnerKey::from_outer_key(&range.0),
                    InnerKey::from_outer_key(&range.1),
                    false,
                )
            })
            .collect();

        let mut mem_data = Vec::with_capacity(U32_SIZE /* format */);
        let (skls, txn_file_refs) = self.get_mem_tables_group_by_type(WRITE_CF, &data_bounds);
        mem_data.put_u32_le(MEM_DATA_FORMAT_V2);
        self.build_skl_data(outer_ranges, start_ts, skls, &mut mem_data);
        self.build_txn_file_data(txn_file_refs, &mut mem_data);
        mem_data
    }

    fn build_skl_data(
        &self,
        ranges: &[(Bytes, Bytes)],
        start_ts: u64,
        skls: Vec<SkipList>,
        mem_data: &mut Vec<u8>,
    ) {
        let skl_iters = skls
            .iter()
            .map(|skl| Box::new(skl.new_iterator(false)) as _)
            .collect();
        let merge_iter = table::new_merge_iterator(skl_iters, false);

        let data = self.data.clone();
        let mut key = BytesMut::new();
        key.extend_from_slice(data.keyspace_prefix());
        // If start_ts is u64::MAX, it means all versions are needed.
        let all_versions = start_ts == u64::MAX;
        let mut mem_iterator = Iterator {
            all_versions,
            reversed: false,
            read_ts: start_ts,
            key,
            val: table::Value::new(),
            inner: merge_iter,
            blob_prefetcher: None,
            data,
            range: None,
        };

        let mut rows = vec![];
        // Save mvcc versions of the same key temporarily, and append to rows with
        // reverse order.
        let mut mvcc_rows = vec![];
        let mut last_key = vec![];
        for (range_start, range_end) in ranges {
            mem_iterator.seek(range_start.chunk());
            while mem_iterator.valid() {
                let key = mem_iterator.key().to_vec();
                if key.as_slice() >= range_end.chunk() {
                    break;
                }
                if key != last_key {
                    for _ in 0..mvcc_rows.len() {
                        rows.push(mvcc_rows.pop().unwrap());
                    }
                }
                mvcc_rows.push(table::Row {
                    key: key.clone(),
                    user_meta: UserMeta::from_slice(mem_iterator.user_meta()),
                    value: mem_iterator.val().to_vec(),
                });
                mem_iterator.next();
                last_key = key;
            }
        }
        for _ in 0..mvcc_rows.len() {
            rows.push(mvcc_rows.pop().unwrap());
        }
        let mem_size = bincode::serialized_size(&rows)
            .map_err(|e| Error::Other(e))
            .unwrap();
        mem_data.reserve(U64_SIZE /* mem_size */ + mem_size as usize);
        mem_data.put_u64_le(mem_size);
        bincode::serialize_into(mem_data, &rows).unwrap();
    }

    fn build_txn_file_data(&self, txn_file_refs: TxnFileRefs, mem_data: &mut Vec<u8>) {
        let msg_len = txn_file_refs.compute_size();
        mem_data.reserve(U32_SIZE /* msg_len */ + msg_len as usize);
        mem_data.put_u32_le(msg_len);
        txn_file_refs.write_to_vec(mem_data).unwrap();
    }

    fn get_mem_tables_group_by_type(
        &self,
        cf: usize,
        bounds: &[DataBound<'_>],
    ) -> (Vec<SkipList>, TxnFileRefs) {
        let mut skls = Vec::new();
        let mut txn_file_refs = TxnFileRefs::default();
        for mem_tbl in &self.data.mem_tbls {
            let skl_ext = mem_tbl.get_cf(cf);

            for txn_file_ref in skl_ext
                .get_txn_files()
                .into_iter()
                .filter_map(|txn_file| txn_file.to_txn_file_ref(Some(bounds)))
            {
                txn_file_refs.mut_txn_file_refs().push(txn_file_ref);
            }

            let skl = skl_ext.get_skl();
            if !skl.is_empty() {
                skls.push(skl);
            }
        }
        (skls, txn_file_refs)
    }

    pub fn get_all_sst_files(&self) -> Vec<u64> {
        self.data.get_all_sst_files()
    }

    pub fn has_schema_file(&self) -> bool {
        self.data.schema_file.is_some()
    }

    // Used by proxy kvengine.
    pub fn get_schema_file(&self) -> Option<SchemaFile> {
        self.data.schema_file.clone()
    }

    #[maybe_async::both]
    pub async fn get_newer(&self, cf: usize, key: &[u8], version: u64) -> Item<'_> {
        let inner_key = InnerKey::from_outer_key(key);
        let mut item = Item::new();
        item.owned_val = Some(vec![]);
        item.val = self
            .get_newer_val(cf, inner_key, version, item.owned_val.as_mut().unwrap())
            .await;
        if item.val.is_blob_ref() {
            item.owned_blob = Some(self.fetch_blob(inner_key, &item.val));
            item.val.fill_in_blob(item.owned_blob.as_ref().unwrap());
        }
        item
    }

    #[maybe_async::both]
    async fn get_newer_val(
        &self,
        cf: usize,
        inner_key: InnerKey<'_>,
        version: u64,
        out_val_owner: &mut Vec<u8>,
    ) -> table::Value {
        self.check_sync(cf).await;
        let key_hash = farmhash::fingerprint64(inner_key.deref());
        for i in 0..self.data.mem_tbls.len() {
            let tbl = self.data.mem_tbls.as_slice()[i].get_cf(cf);
            let v = tbl.get_newer(inner_key, version, out_val_owner);
            if v.is_valid() {
                return v;
            }
        }
        for l0 in &self.data.l0_tbls {
            if let Some(tbl) = &l0.get_cf(cf) {
                let v = tbl.get_newer(inner_key, version, key_hash, out_val_owner, 0);
                if v.is_valid() {
                    return v;
                }
            }
        }
        let scf = self.data.get_cf(cf);
        for lh in &scf.levels {
            let v = lh
                .get_newer(inner_key, version, key_hash, out_val_owner)
                .await;
            if v.is_valid() {
                return v;
            }
        }
        table::Value::new()
    }

    #[maybe_async::both]
    pub async fn has_data_in_prefix<'a>(&'a self, mut prefix: &'a [u8]) -> bool {
        self.check_sync(WRITE_CF).await;
        let keyspace_prefix = self.data.keyspace_prefix();

        let min_off = std::cmp::min(prefix.len(), keyspace_prefix.len());
        if min_off > 0 {
            if prefix[0..min_off] != keyspace_prefix[0..min_off] {
                return false;
            }
            if prefix.len() < keyspace_prefix.len() {
                // If prefix is less than keyspace_prefix_len, the data in shard always have
                // `keyspace_prefix`, just update prefix to keyspace_prefix.
                prefix = keyspace_prefix;
            }
        }

        let inner_prefix = InnerKey::from_outer_key(prefix);
        if self.deleting_prefixes.cover_prefix(inner_prefix) {
            return false;
        }
        let mut it = self
            .new_iterator(WRITE_CF, false, false, Some(u64::MAX), true)
            .await;
        it.seek(prefix).await;
        if !it.valid() {
            return false;
        }
        it.key().starts_with(prefix)
    }

    pub fn has_unloaded_tables(&self) -> bool {
        !self.data.unloaded_tbls.is_empty()
    }

    pub fn get_encryption_key(&self) -> Option<EncryptionKey> {
        self.encryption_key.clone()
    }

    pub fn estimated_range_blocks_size(&self, ranges: &[(Bytes, Bytes)]) -> usize {
        // ignore L0 tables, only estimate L1+ for simplicity and performance.
        let mut blocks_size = 0;
        let write_cf = &self.data.cfs[0];
        for lvl in &write_cf.levels {
            blocks_size += lvl.range_blocks_size(ranges);
        }
        blocks_size
    }

    pub fn get_keyspace_id(&self) -> u32 {
        self.data.keyspace_id
    }

    #[maybe_async::both]
    async fn seek_txn_file(&self, iter: &mut Box<dyn table::Iterator>, txn_file: &TxnFile) {
        if txn_file.smallest() < self.data.inner_start() {
            iter.seek(self.data.inner_start()).await;
        } else {
            iter.seek(txn_file.smallest()).await;
        }
    }

    fn get_upper_bound<'a>(&'a self, buf: &'a mut Vec<u8>, txn_file: &TxnFile) -> InnerKey<'_> {
        if txn_file.biggest() < self.data.inner_end() {
            buf.extend_from_slice(txn_file.biggest().deref());
            buf.push(0);
            InnerKey::from_inner_buf(buf)
        } else {
            self.data.inner_end()
        }
    }

    pub fn get_txn_file_conflict_lock(&self, txn_file: &TxnFile) -> Option<(Vec<u8>, Lock)> {
        if txn_file.is_empty() {
            return None;
        }
        if txn_file.size() < self.data.get_cf(LOCK_CF).size() as usize {
            // It is more efficient to iterate the txn file when it is small.
            let mut other_lock_txn_files = self.data.lock_txn_files.clone();
            other_lock_txn_files.retain(|f| f.start_ts() != txn_file.start_ts());
            let mut txn_file_iter = TxnFileIterator::new(txn_file.clone(), false);
            txn_file_iter.rewind();
            while txn_file_iter.valid() {
                let mut val = vec![];
                let txn_file_key = txn_file_iter.key();
                let version = self.get_mem_table_snap_version().into_inner();
                let lock_val = self.get_value(
                    LOCK_CF,
                    txn_file_key,
                    version,
                    &mut AccessPath::default(),
                    &mut val,
                    &other_lock_txn_files,
                );
                if lock_val.is_valid() && !lock_val.is_deleted() {
                    let conflict_lock = Lock::parse(lock_val.get_value()).unwrap();
                    // Return outer key.
                    let mut key = self.data.keyspace_prefix().to_vec();
                    key.extend_from_slice(txn_file_key.deref());
                    return Some((key, conflict_lock));
                }
                txn_file_iter.next();
            }
            return None;
        }
        let mut lock_iter =
            self.new_table_iterator(LOCK_CF, false, true, Some(txn_file.start_ts()));
        self.seek_txn_file(&mut lock_iter, txn_file);
        let mut upper_bound_buf = vec![];
        let upper_bound = self.get_upper_bound(&mut upper_bound_buf, txn_file);
        let mut outer_val_buf = vec![];
        while lock_iter.valid() {
            if lock_iter.key() >= upper_bound {
                return None;
            }
            let lock_iter_val = lock_iter.value();
            if !lock_iter_val.is_deleted()
                && txn_file
                    .get_value(lock_iter.key(), &mut outer_val_buf)
                    .1
                    .is_valid()
            {
                let conflict_lock = Lock::parse(lock_iter_val.get_value()).unwrap();
                // Return outer key.
                let mut key = self.data.keyspace_prefix().to_vec();
                key.extend_from_slice(lock_iter.key().as_ref());
                return Some((key, conflict_lock));
            }
            lock_iter.next();
        }
        None
    }

    // Get writes with `commit_ts` LARGER than `txn_file.start_ts()`.
    // These writes are not seen by clients and should be considered as conflicts.
    // Ref: `SnapAccessCore::get_newer`.
    #[maybe_async::both]
    pub async fn get_txn_file_conflict_write(
        &self,
        txn_file: &TxnFile,
    ) -> Option<(Vec<u8>, UserMeta)> {
        self.check_sync(WRITE_CF).await;
        if txn_file.is_empty() {
            return None;
        }
        let mut write_iter = self.new_delta_write_iterator(txn_file.start_ts()).await;
        self.seek_txn_file(&mut write_iter, txn_file).await;
        let mut upper_bound_buf = vec![];
        let upper_bound = self.get_upper_bound(&mut upper_bound_buf, txn_file);
        let mut outer_val_buf = vec![];
        while write_iter.valid() {
            if write_iter.key() >= upper_bound {
                return None;
            }
            let write_iter_val = write_iter.value();
            if !write_iter_val.is_deleted()
                && txn_file
                    .get_value(write_iter.key(), &mut outer_val_buf)
                    .1
                    .is_valid()
            {
                debug_assert!(
                    !write_iter_val.user_meta().is_empty(),
                    "write_iter_val: {:?}",
                    write_iter_val,
                );
                let um = UserMeta::from_slice(write_iter_val.user_meta());
                if um.commit_ts > txn_file.start_ts() {
                    // Return outer key.
                    let mut key = self.data.keyspace_prefix().to_vec();
                    key.extend_from_slice(write_iter.key().as_ref());
                    return Some((key, um));
                }
            }
            next!(write_iter).await;
        }
        None
    }

    #[maybe_async::both]
    pub async fn check_txn_file_constraint(
        &self,
        txn_file: &TxnFile,
        read_ts: u64,
    ) -> Option<Vec<u8> /* already_exist_key */> {
        self.check_sync(WRITE_CF).await;
        let mut checker = TxnFileConstraintChecker::new(self, read_ts);
        txn_file
            .iter_check_constraint_keys(self.data.data_bound(), &mut checker)
            .await;
        checker.already_exist_key
    }

    pub fn get_lock_txn_file(&self, start_ts: u64) -> Option<TxnFile> {
        for txn_file in &self.data.lock_txn_files {
            if txn_file.start_ts() == start_ts {
                return Some(txn_file.clone());
            }
        }
        None
    }

    pub fn get_lock_txn_files(&self) -> &[TxnFile] {
        &self.data.lock_txn_files
    }

    pub fn get_limiter(&self) -> &RegionLimiter {
        &self.data.limiter
    }

    pub fn new_schema_from_columns(&self, table_id: i64, columns: &[ColumnInfo]) -> Option<Schema> {
        if !self.read_columnar || !self.data.columnar_table_ids.contains(&table_id) {
            return None;
        }
        let schema_file = self.data.schema_file.as_ref()?;
        let table_schema = schema_file.get_table(table_id)?;
        if !table_schema.with_columnar() {
            return None;
        }
        let columns = columns
            .iter()
            .filter(|c| !c.get_pk_handle() && c.get_column_id() != HANDLE_COL_ID as i64)
            .cloned()
            .collect::<Vec<_>>();
        let schema_buf = SchemaBuf::new(
            table_id,
            table_schema.handle_column.clone(),
            table_schema.version_column.clone(),
            columns,
            table_schema.pk_col_ids.clone(),
            table_schema.max_col_id,
            table_schema.vector_indexes.clone(),
            table_schema.fulltext_indexes.clone(),
            table_schema.get_storage_class_spec().clone(),
            None,
        );
        Some(Schema::new(schema_buf))
    }

    pub fn new_columnar_mvcc_reader_from_row(
        &self,
        table_id: i64,
        columns: &[ColumnInfo],
        read_ts: u64,
    ) -> Option<ColumnarMvccReader> {
        let Some(schema) = self.new_schema_from_columns(table_id, columns) else {
            return None;
        };
        let mut readers: Vec<Box<dyn ColumnarReader>> = vec![];
        for mem in &self.data.mem_tbls {
            let skl = mem.get_cf(WRITE_CF);
            if !skl.is_empty() {
                let iter = skl.new_iterator(false);
                let row_reader = ColumnarRowTableReader::new(
                    schema.clone(),
                    iter,
                    None,
                    false,
                    self.encryption_key.clone(),
                );
                readers.push(Box::new(row_reader));
            }
        }
        for l0 in &self.data.l0_tbls {
            if let Some(l0_write) = l0.get_cf(WRITE_CF) {
                let iter = l0_write.new_iterator(false, false);
                let row_reader = ColumnarRowTableReader::new(
                    schema.clone(),
                    iter,
                    None,
                    false,
                    self.encryption_key.clone(),
                );
                readers.push(Box::new(row_reader));
            }
        }
        self.data.for_each_level(|cf, lh| {
            if cf != WRITE_CF {
                return false;
            }
            for tbl in lh.tables.iter() {
                let iter = tbl.new_iterator(false, false);
                let row_reader = ColumnarRowTableReader::new(
                    schema.clone(),
                    iter,
                    None,
                    false,
                    self.encryption_key.clone(),
                );
                readers.push(Box::new(row_reader));
            }
            false
        });

        let merge_reader = ColumnarMergeReader::new(schema.clone(), readers);
        Some(ColumnarMvccReader::new(
            Box::new(merge_reader),
            &schema,
            read_ts,
        ))
    }

    /// `new_columnar_mvcc_reader` will try to construct a reader in columnar
    /// mode. If `None` is returned, normal tikv row reading will be used.
    pub fn new_columnar_mvcc_reader(
        &self,
        table_id: i64,
        columns: &[ColumnInfo],
        scan_ctx: Option<&TableScanCtx>,
        read_ts: u64,
        ann_query: Option<Arc<AnnQueryInfo>>,
    ) -> Result<Option<ColumnarMvccReader>> {
        // If vector distance projection is enabled, a virtual distance column is
        // included in the schema (which is what TiDB expects the storage layer to
        // return) and there is no vector column in the schema. In this case, we
        // will first read using a new schema with the vector column included, then
        // return the data (what TiDB wants) with a distance column calculated from the
        // vector column.
        let has_vector_distance_proj = ann_query
            .as_ref()
            .map(|q| q.get_enable_distance_proj())
            .unwrap_or(false);

        let Some(schema) = self.new_schema_from_columns(table_id, columns) else {
            return Ok(None);
        };
        let filter_op = scan_ctx.map(|ctx| ctx.to_filter_operator());

        let schema_to_read = if has_vector_distance_proj {
            VectorDistanceProjector::generate_inner_schema(&schema, ann_query.as_ref().unwrap())?
        } else {
            schema.clone()
        };
        let mut readers = self.collect_column_row_readers(&schema_to_read);
        for columnar_level in &self.data.col_levels.levels {
            if columnar_level.level == 2 {
                let concat_reader = ColumnarConcatReader::new(
                    &columnar_level.files,
                    schema_to_read.clone(),
                    filter_op.clone(),
                    self.encryption_key.clone(),
                );
                readers.push(Box::new(concat_reader));
            } else {
                for col_file in &columnar_level.files {
                    if !col_file.has_table(schema_to_read.table_id) {
                        continue;
                    }
                    let col_reader = ColumnarTableReader::new(
                        col_file,
                        schema_to_read.clone(),
                        filter_op.clone(),
                        self.encryption_key.clone(),
                    );
                    readers.push(Box::new(col_reader));
                }
            }
        }
        let mut merged_reader: Box<dyn ColumnarReader> =
            Box::new(ColumnarMergeReader::new(schema_to_read.clone(), readers));
        // As commented above, if vector distance projection is enabled,
        // we will wrap a VectorDistanceProjector as the outside layer
        // to produce a distance column from the vector column.
        if has_vector_distance_proj {
            merged_reader = Box::new(VectorDistanceProjector::new(
                merged_reader,
                schema.clone(),
                ann_query.unwrap().clone(),
            )?);
        }

        let mvcc_reader = ColumnarMvccReader::new(merged_reader, &schema, read_ts);
        Ok(Some(mvcc_reader))
    }

    /// `new_fts_columnar_mvcc_reader` will try to construct a reader in
    /// columnar mode. If `None` is returned, normal tikv row reading will
    /// be used.
    /// In `FtsBruteForceReader`, the matching fts_col may be filtered to return
    /// only the matching rows.
    pub fn new_fts_columnar_mvcc_reader(
        &self,
        table_id: i64,
        columns: &[ColumnInfo],
        scan_ctx: Option<&TableScanCtx>,
        read_ts: u64,
        fts_query: Arc<FtsQueryInfo>,
    ) -> Result<Option<FtsBruteForceReader>> {
        let Some(schema) = self.new_schema_from_columns(table_id, columns) else {
            return Ok(None);
        };

        let filter_op = scan_ctx.map(|ctx| ctx.to_filter_operator());

        // Fts index is not supported yet, so no matter for FtsQueryTypeNoScore or
        // FtsQueryTypeWithScore, we need to read the fts_col column to
        // perform fts matching or calculate scores. Therefore, we need to
        // insert fts_col at the end of the schema if fts_col does not exist.
        let schema_to_read = FtsBruteForceReader::generate_inner_schema(&schema, &fts_query)?;

        let mut readers = self.collect_column_row_readers(&schema_to_read);

        for columnar_level in &self.data.col_levels.levels {
            if columnar_level.level == 2 {
                let concat_reader = ColumnarConcatReader::new(
                    &columnar_level.files,
                    schema_to_read.clone(),
                    filter_op.clone(),
                    self.encryption_key.clone(),
                );
                readers.push(Box::new(concat_reader));
            } else {
                for col_file in &columnar_level.files {
                    if !col_file.has_table(schema_to_read.table_id) {
                        continue;
                    }
                    let col_reader = ColumnarTableReader::new(
                        col_file,
                        schema_to_read.clone(),
                        filter_op.clone(),
                        self.encryption_key.clone(),
                    );
                    readers.push(Box::new(col_reader));
                }
            }
        }

        let merged_reader: Box<dyn ColumnarReader> =
            Box::new(ColumnarMergeReader::new(schema_to_read.clone(), readers));

        let inner_mvcc_reader = ColumnarMvccReader::new(merged_reader, &schema_to_read, read_ts);
        let fts_mvcc_reader = FtsBruteForceReader::new(
            Box::new(inner_mvcc_reader),
            schema.clone(),
            fts_query.clone(),
        )?;
        Ok(Some(fts_mvcc_reader))
    }

    fn collect_column_row_readers(&self, schema: &Schema) -> Vec<Box<dyn ColumnarReader>> {
        let mut readers: Vec<Box<dyn ColumnarReader>> = vec![];

        for mem in &self.data.mem_tbls {
            let skl = mem.get_cf(WRITE_CF);
            if !skl.is_empty() {
                let iter = skl.new_iterator(false);
                let row_reader = ColumnarRowTableReader::new(
                    schema.clone(),
                    iter,
                    None,
                    false,
                    self.encryption_key.clone(),
                );
                readers.push(Box::new(row_reader));
            }
        }
        for l0 in &self.data.col_levels.unconverted_l0s {
            if let Some(l0_write) = l0.get_cf(WRITE_CF) {
                info!(
                    "{} add unconverted l0 columnar file {}",
                    self.tag,
                    l0_write.id()
                );
                let iter = l0_write.new_iterator(false, true);
                let row_reader = ColumnarRowTableReader::new(
                    schema.clone(),
                    iter,
                    None,
                    false,
                    self.encryption_key.clone(),
                );
                readers.push(Box::new(row_reader));
            }
        }
        readers
    }

    pub fn new_vector_index_reader(
        &self,
        table_id: i64,
        index_id: i64,
        col_id: i64,
        target: &[f32],
        top_k: usize,
        schema: Schema,
        read_ts: u64,
        start_handle: Option<&[u8]>,
        end_handle: Option<&[u8]>,
        ann_query: Arc<AnnQueryInfo>,
    ) -> Result<Option<ColumnarMvccReader>> {
        if !self.read_columnar {
            return Ok(None);
        }
        if !self.data.col_levels.levels[2].files.is_empty() {
            // Make sure the l2_snap_version is set.
            debug_assert!(self.data.col_levels.l2_snap_version.is_not_zero());
        }

        let Some(vector_index) = self.data.vector_indexes.get(table_id, index_id, col_id) else {
            return Ok(None);
        };
        let enable_distance_proj = ann_query.get_enable_distance_proj();
        let metric = ann_query.get_distance_metric();

        let vector_items_reader = match VectorItemsReader::new(
            schema.clone(),
            vector_index,
            target,
            top_k,
            read_ts,
            start_handle,
            end_handle,
            &self.data.col_levels,
            self.encryption_key.clone(),
            metric,
            enable_distance_proj,
        ) {
            Ok(reader) => reader,
            Err(e) => {
                warn!("{} failed to search vector index {:?}", self.tag, e);
                return Ok(None);
            }
        };

        // When vector search distance proj is enabled, TableScan's schema
        // contains a VirtualDistance column at the end, and does not
        // contain the vector column. However, when we are reading from
        // data that vector index is not built yet, we need to read out
        // the vector column in order to calculate the distance column
        // that TableScan asks for.
        let schema_with_vec = if enable_distance_proj {
            VectorDistanceProjector::generate_inner_schema(&schema, &ann_query)?
        } else {
            schema.clone()
        };

        // For all readers except for the vector index reader, they will always read
        // with the vector column. We wrap a distance projection transformation
        // over these column readers so that they finally produce the expected output.
        // For vector index reader, it produces the vector distance column directly.
        let mut readers = self.collect_column_row_readers(&schema_with_vec);
        let mem_l0_readers_count = readers.len();
        if enable_distance_proj {
            readers = readers
                .into_iter()
                .map(|r| {
                    VectorDistanceProjector::new(r, schema.clone(), ann_query.clone())
                        .map(|vr| Box::new(vr) as Box<dyn ColumnarReader>)
                        .map_err(|e| Error::Other(e.into()))
                })
                .collect::<Result<Vec<_>>>()?;
        }

        let mut columnar_readers_count = 0;
        for columnar_level in &self.data.col_levels.levels {
            if columnar_level.level == 2 {
                // level 2 columnar files are all included in the vector index.
                break;
            }
            for file in &columnar_level.files {
                // NOTE: the snap_version of columnar file maybe smaller than the vector index
                // and not generated the vector index after region merge. We should also need to
                // read the columnar file.
                if vector_index.contains_columnar_file(file) {
                    continue;
                }
                if !file.has_table(schema.table_id) {
                    continue;
                }
                let col_reader = ColumnarTableReader::new(
                    file,
                    schema_with_vec.clone(),
                    None,
                    self.encryption_key.clone(),
                );
                // The same as above, in distance projection, we wrap a distance projector over
                // the reader who reads the vector column.
                if enable_distance_proj {
                    let col_reader_distance = VectorDistanceProjector::new(
                        Box::new(col_reader),
                        schema.clone(),
                        ann_query.clone(),
                    )?;
                    readers.push(Box::new(col_reader_distance));
                } else {
                    readers.push(Box::new(col_reader));
                }
                columnar_readers_count += 1;
            }
        }
        readers.push(Box::new(vector_items_reader));
        info!(
            "{} use vector index reader, vector_index_file_count: {}, mem_l0_readers_count: {}, columnar_readers_count: {}",
            self.tag,
            vector_index.files.len(),
            mem_l0_readers_count,
            columnar_readers_count
        );
        let merged_reader = ColumnarMergeReader::new(schema.clone(), readers);
        let mvcc_reader = ColumnarMvccReader::new(Box::new(merged_reader), &schema, read_ts);
        Ok(Some(mvcc_reader))
    }

    #[inline]
    pub fn is_sync(&self) -> bool {
        self.is_sync
    }

    #[inline]
    pub fn is_cf_sync(&self, cf: usize) -> bool {
        cf != WRITE_CF || self.is_sync()
    }

    /// Get IA segments which are in DFS. Used to prefetch in parallel.
    // TODO: Support columnar tables
    pub fn get_ia_remote_segments(
        &self,
        outer_range: (&[u8], &[u8]),
    ) -> Result<(
        Vec<(FileSegmentIdent, FileType)>,
        usize, // total_segments
    )> {
        let start_key = InnerKey::from_outer_key(outer_range.0);
        let end_key = InnerKey::from_outer_end_key(outer_range.1);
        let data_bound = DataBound::new(start_key, end_key, false);

        let mut segments = vec![];
        let mut total_segments = 0;
        let (sstables, blob_tables) = self.get_overlap_async_tables(data_bound);
        for t in sstables {
            let (segs, total) = t.get_remote_segments(data_bound)?;
            segments.extend(segs.into_iter().map(|ident| (ident, FileType::Sst)));
            total_segments += total;
        }
        for blob_table in blob_tables {
            let (segs, total) = blob_table.get_remote_segments(data_bound)?;
            segments.extend(segs.into_iter().map(|ident| (ident, FileType::Blob)));
            total_segments += total;
        }
        Ok((segments, total_segments))
    }

    fn get_overlap_async_tables(
        &self,
        data_bound: DataBound<'_>,
    ) -> (Vec<SsTable>, Vec<BlobTable>) {
        let mut tables = vec![];
        for lh in &self.data.cfs[WRITE_CF].levels {
            let (left, right) = data_bound.get_overlap_data_sets(&lh.tables);
            for t in &lh.tables[left..right] {
                if !t.is_sync() {
                    tables.push(t.clone());
                }
            }
        }
        let mut blob_tables = vec![];
        for blob_table in self.data.blob_tbl_map.values() {
            if !blob_table.is_sync() {
                blob_tables.push(blob_table.clone());
            }
        }
        (tables, blob_tables)
    }

    pub fn get_columnar_table_ids(&self) -> Vec<i64> {
        self.data.columnar_table_ids.clone()
    }

    // Filter the columnar table ids that are not empty.
    // `table_ids` is the list of table ids to filter.
    // Returns the list of columnar table ids that are not empty. If the table_id is
    // not in the snapshot, it will be filtered out.
    // NOTE: The caller should guarantee that table_ids is sorted.
    pub fn filter_columnar_table_ids_not_empty(&self, table_ids: &[i64]) -> HashSet<i64> {
        if table_ids.is_empty() {
            return HashSet::new();
        }
        let columnar_table_ids = self
            .data
            .columnar_table_ids
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let mut filtered_table_ids = HashSet::new();
        let mut iter = self.new_memtable_unconverted_l0_iterator();
        let mut delta_data_table_id = table_ids[0];
        for &table_id in table_ids {
            if !columnar_table_ids.contains(&table_id) {
                continue;
            }

            // Optimize the seek if there are lots of tables are empty.
            if table_id >= delta_data_table_id {
                // Check data exists in the snapshot delta data.
                let table_key_prefix = encode_table_prefix_key(table_id);
                iter.seek(table_key_prefix.as_ref());
                if iter.valid() {
                    let key = iter.key();
                    delta_data_table_id = decode_table_id(key.deref()).unwrap_or(i64::MAX);
                    if key.starts_with(table_key_prefix.as_ref().deref()) {
                        filtered_table_ids.insert(table_id);
                        continue;
                    }
                } else {
                    delta_data_table_id = i64::MAX;
                }
            }

            // Check data exists in the snapshots columnar files.
            if self.columnar_files_contains_table(table_id) {
                filtered_table_ids.insert(table_id);
            }
        }
        filtered_table_ids
    }

    fn new_memtable_unconverted_l0_iterator(&self) -> Box<dyn table::Iterator> {
        let mut iters: Vec<Box<dyn table::Iterator>> = Vec::new();
        for mem_tbl in &self.data.mem_tbls {
            iters.push(mem_tbl.get_cf(WRITE_CF).new_iterator(false));
        }
        for l0 in &self.data.col_levels.unconverted_l0s {
            let Some(l0_write_cf) = l0.get_cf(WRITE_CF) else {
                continue;
            };
            iters.push(l0_write_cf.new_iterator(false, false));
        }
        table::new_merge_iterator(iters, false)
    }

    fn columnar_files_contains_table(&self, table_id: i64) -> bool {
        for columnar_level in &self.data.col_levels.levels {
            for file in &columnar_level.files {
                if file.has_table(table_id) {
                    return true;
                }
            }
        }
        false
    }

    // validate_cached_value validate the cached value for the given inner_key and
    // snap_version.
    // By checking the mem tables with greater snap_version, we ensure the cached
    // value is still valid.
    // If there are newer values the newer values is returned.
    // If there maybe newer values in the persisted tables, None is returned.
    pub(crate) fn validate_cached_value(
        &self,
        inner_key: InnerKey<'_>,
        snap_version: SnapVersion,
        out_val_owner: &mut Vec<u8>,
    ) -> Option<table::Value> {
        for i in 0..self.data.mem_tbls.len() {
            let cf_tbl = &self.data.mem_tbls[i];
            let mem_tbl_snap_version = cf_tbl.get_snap_version();
            if mem_tbl_snap_version.is_not_zero() && mem_tbl_snap_version < snap_version {
                return Some(Value::new());
            }
            let tbl = cf_tbl.get_cf(WRITE_CF);
            let v = tbl.get(inner_key, u64::MAX, out_val_owner);
            if v.is_valid() {
                return Some(v);
            }
        }
        if self.data.persisted_version < snap_version {
            return Some(Value::new());
        }
        None
    }

    // To ensure that async `SnapAccess` must be called by async methods.
    // The checking in `IaFile` may be not reached when the block has been
    // loaded.
    #[inline]
    fn check_sync(&self, cf: usize) {
        debug_assert!(self.is_cf_sync(cf));
    }

    #[inline]
    async fn check_sync_async(&self, _: usize) {}
}

pub struct Iterator {
    all_versions: bool,
    reversed: bool,
    read_ts: u64,
    pub(crate) key: BytesMut,
    val: table::Value,
    pub(crate) inner: Box<dyn table::Iterator>,
    blob_prefetcher: Option<BlobPrefetcher>,
    data: ShardData,
    range: Option<(Bytes, Bytes)>, // [outer_lower_bound, outer_upper_bound)
}

impl Iterator {
    pub fn valid(&self) -> bool {
        self.val.is_valid()
    }

    pub fn key(&self) -> &[u8] {
        self.key.chunk()
    }

    pub fn val(&mut self) -> &[u8] {
        if self.val.is_blob_ref() {
            if let Some(prefetcher) = &mut self.blob_prefetcher {
                let blob_ref = self.val.get_blob_ref();
                return prefetcher.get(&blob_ref).unwrap_or_else(|e| {
                    panic!("failed to get blob, blob_ref: {:?}, err: {:?}", blob_ref, e)
                });
            }
        }
        self.val.get_value()
    }

    pub fn version(&self) -> u64 {
        self.val.version
    }

    pub fn meta(&self) -> u8 {
        self.val.meta
    }

    pub fn user_meta(&self) -> &[u8] {
        self.val.user_meta()
    }

    pub fn valid_for_prefix(&self, prefix: &[u8]) -> bool {
        self.key.starts_with(prefix)
    }

    #[maybe_async::both]
    pub async fn next(&mut self) {
        if self.all_versions
            && self.valid()
            && next_version!(self.inner).await
            && !self.inner.value().is_deleted()
        {
            self.update_item();
            return;
        }
        next!(self.inner).await;
        self.parse_item().await;
    }

    fn update_item(&mut self) {
        self.key.truncate(self.data.keyspace_prefix_len());
        self.key.extend_from_slice(self.inner.key().deref());
        self.val = self.inner.value();
    }

    #[maybe_async::both]
    async fn parse_item(&mut self) {
        while self.inner.valid() {
            if self.is_inner_key_over_bound() {
                break;
            }
            let val = self.inner.value();
            if val.version > self.read_ts && !self.inner.seek_to_version(self.read_ts).await {
                next!(self.inner).await;
                continue;
            }
            if self.inner.value().is_deleted() {
                next!(self.inner).await;
                continue;
            }
            self.update_item();
            return;
        }
        self.val = table::Value::new();
    }

    // seek would seek to the provided key if present. If absent, it would seek to
    // the next smallest key greater than provided if iterating in the forward
    // direction. Behavior would be reversed is iterating backwards.
    #[maybe_async::both]
    pub async fn seek(&mut self, key: &[u8]) {
        if key.len() <= self.data.inner_key_off {
            self.inner.rewind().await;
        } else {
            self.inner.seek(InnerKey::from_outer_key(key)).await;
        }
        self.parse_item().await;
    }

    // rewind would rewind the iterator cursor all the way to zero-th position,
    // which would be the smallest key if iterating forward, and largest if
    // iterating backward. It does not keep track of whether the cursor started
    // with a seek().
    #[maybe_async::both]
    pub async fn rewind(&mut self) {
        self.inner.rewind().await;
        if self.inner.valid() {
            if self.reversed {
                if self.inner.key() >= self.data.inner_end() {
                    self.inner.seek(self.data.inner_end()).await;
                    if self.inner.key() == self.data.inner_end() {
                        next!(self.inner).await;
                    }
                }
            } else if self.inner.key() < self.data.inner_start() {
                self.inner.seek(self.data.inner_start()).await
            }
        }
        self.parse_item().await;
    }

    pub fn set_all_versions(&mut self, all_versions: bool) {
        self.all_versions = all_versions;
    }

    pub fn is_reverse(&self) -> bool {
        self.reversed
    }

    // set the new range of the iterator, it the range is monotonic, we can avoid
    // seek. return true if seek is performed.
    #[maybe_async::both]
    #[allow(clippy::collapsible_else_if)]
    pub async fn set_range(
        &mut self,
        outer_lower_bound_include: Bytes,
        outer_upper_bound_exclude: Bytes,
    ) -> bool {
        let inner_lower_bound = InnerKey::from_outer_key(&outer_lower_bound_include);
        let inner_upper_bound = InnerKey::from_outer_end_key(&outer_upper_bound_exclude);
        let mut seeked = false;
        // reset monotonic range can be optimized to avoid seek.
        if self.is_reset_monotonic_range(inner_lower_bound, inner_upper_bound) {
            // If inner is not valid, the iterator has reached the end, there is no more
            // data to return, we can avoid the seek.
            if self.inner.valid() {
                if self.reversed {
                    // If the new inner_upper_bound is greater than the current key, we can
                    // continue to use the current key to iterate backward, avoid the seek.
                    if self.inner.key() > inner_upper_bound {
                        self.inner.seek(inner_upper_bound).await;
                        seeked = true;
                    }
                    // the upper bound is exclusive, so we need to skip the current key.
                    if self.inner.key() == inner_upper_bound {
                        next!(self.inner).await;
                    }
                } else {
                    // If the new inner_lower_bound is greater than or equal to the current key,
                    // we can continue to use the current key to iterate forward, avoid the seek.
                    if self.inner.key() < inner_lower_bound {
                        self.inner.seek(inner_lower_bound).await;
                        seeked = true;
                    }
                }
            }
        } else {
            // always seek if not reset monotonic range.
            if self.reversed {
                self.inner.seek(inner_upper_bound).await;
                if self.inner.valid() && self.inner.key() == inner_upper_bound {
                    next!(self.inner).await;
                }
            } else {
                self.inner.seek(inner_lower_bound).await;
            }
            seeked = true;
        }
        self.range = Some((outer_lower_bound_include, outer_upper_bound_exclude));
        self.parse_item().await;
        seeked
    }

    fn is_reset_monotonic_range(
        &self,
        inner_lower_bound: InnerKey<'_>,
        inner_upper_bound: InnerKey<'_>,
    ) -> bool {
        if let Some((outer_old_lower, outer_old_upper)) = &self.range {
            if self.reversed {
                inner_upper_bound <= InnerKey::from_outer_key(outer_old_lower)
            } else {
                InnerKey::from_outer_end_key(outer_old_upper) <= inner_lower_bound
            }
        } else {
            false
        }
    }

    pub(crate) fn is_inner_key_over_bound(&self) -> bool {
        if let Some((outer_lower, outer_upper)) = &self.range {
            if self.inner.valid() {
                if self.reversed {
                    self.inner.key() < InnerKey::from_outer_key(outer_lower)
                } else {
                    self.inner.key() >= InnerKey::from_outer_end_key(outer_upper)
                }
            } else {
                true
            }
        } else if self.reversed {
            self.inner.key() < self.data.inner_start()
        } else {
            self.inner.key() >= self.data.inner_end()
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableCtx {
    pub table_id: i64,
    pub ranges: Vec<tipb::KeyRange>,
}

/// CloudColumnarReaders coordinates reading columnar data from multiple tables,
/// dispatching per-table work onto worker threads to leverage CPU parallelism.
pub struct CloudColumnarReaders {
    runtime: Handle,
    snap_access: SnapAccess,
    tables: Vec<TableCtx>,
    columns: Arc<Vec<ColumnInfo>>,
    scan_ctx: TableScanCtx,
    ann_query_info: Arc<tipb::AnnQueryInfo>,
    ia_ctx: IaCtx,
    start_ts: u64,
    concurrency: usize,
    reader: Option<CloudColumnarReader>,
    result_rx: Option<mpsc::Receiver<ReadResult>>,
    current_block: Option<BlockResult>,
    read_limit: Option<usize>,
    finished: bool,
}

impl CloudColumnarReaders {
    pub fn new(
        runtime: Handle,
        snap_access: SnapAccess,
        tables: Vec<TableCtx>,
        columns: Vec<ColumnInfo>,
        scan_ctx: TableScanCtx,
        ann_query_info: tipb::AnnQueryInfo,
        ia_ctx: IaCtx,
        start_ts: u64,
        concurrency: usize,
    ) -> Self {
        let mut table_ids = tables
            .iter()
            .map(|table| table.table_id)
            .collect::<Vec<_>>();
        table_ids.sort_unstable();
        let filtered_table_ids = snap_access.filter_columnar_table_ids_not_empty(&table_ids);
        let tables = tables
            .into_iter()
            .filter(|table| filtered_table_ids.contains(&table.table_id))
            .collect::<Vec<_>>();
        let concurrency = if tables.len() > 1 && concurrency > 1 {
            concurrency
        } else {
            0
        };
        Self {
            runtime,
            snap_access,
            tables,
            columns: Arc::new(columns),
            scan_ctx,
            ann_query_info: Arc::new(ann_query_info),
            ia_ctx,
            start_ts,
            concurrency,
            reader: None,
            result_rx: None,
            current_block: None,
            read_limit: None,
            finished: false,
        }
    }

    fn need_concurrency(&self) -> bool {
        self.concurrency > 0
    }

    pub fn ffi_read_block(&mut self, read_limit: usize) -> Result<usize> {
        // If there is only one table, we use sequential read.
        if !self.need_concurrency() {
            return self.sequential_read_block(read_limit);
        }

        if self.finished {
            self.current_block = None;
            return Ok(0);
        }
        if read_limit == 0 {
            self.finished = true;
            self.current_block = None;
            return Ok(0);
        }
        if self.result_rx.is_none() {
            self.start_workers(read_limit)?;
        }
        if let Some(expected) = self.read_limit {
            debug_assert_eq!(expected, read_limit);
        }
        let Some(rx) = self.result_rx.as_ref() else {
            return Ok(0);
        };
        match rx.recv() {
            Ok(ReadResult::Block(block)) => {
                let read_count = block.read_count;
                self.current_block = Some(block);
                Ok(read_count)
            }
            Ok(ReadResult::Finished) => {
                self.finished = true;
                self.current_block = None;
                Ok(0)
            }
            Ok(ReadResult::Error(err)) => {
                self.finished = true;
                self.current_block = None;
                Err(err)
            }
            Err(_) => {
                self.finished = true;
                self.current_block = None;
                Ok(0)
            }
        }
    }

    pub fn ffi_read_handle(&mut self) -> Vec<u8> {
        if !self.need_concurrency() {
            if let Some(reader) = self.reader.as_mut() {
                return reader.ffi_read_handle();
            }
            return vec![];
        }
        self.current_block
            .as_mut()
            .map(|block| std::mem::take(&mut block.handle_data))
            .unwrap_or_default()
    }

    pub fn ffi_read_version(&mut self) -> Vec<u8> {
        if !self.need_concurrency() {
            if let Some(reader) = self.reader.as_mut() {
                return reader.ffi_read_version();
            }
            return vec![];
        }
        self.current_block
            .as_mut()
            .map(|block| std::mem::take(&mut block.version_data))
            .unwrap_or_default()
    }

    pub fn ffi_read_column(&mut self, col_id: i64) -> Vec<u8> {
        if !self.need_concurrency() {
            if let Some(reader) = self.reader.as_mut() {
                return reader.ffi_read_column(col_id);
            }
            return vec![];
        }
        self.current_block
            .as_mut()
            .map(|block| block.take_column_data(col_id))
            .unwrap_or_default()
    }

    pub fn ffi_physical_table_id(&self) -> i64 {
        if !self.need_concurrency() {
            return self
                .reader
                .as_ref()
                .map(|reader| reader.physical_table_id())
                .unwrap_or(-1);
        }
        self.current_block
            .as_ref()
            .map(|block| block.table_id)
            .unwrap_or(-1)
    }

    fn start_workers(&mut self, read_limit: usize) -> Result<()> {
        debug_assert!(self.concurrency > 0);

        let (tx, rx) = mpsc::bounded(self.concurrency * 2);
        self.read_limit = Some(read_limit);

        let tables = std::mem::take(&mut self.tables);
        if tables.is_empty() {
            self.finished = true;
            self.result_rx = Some(rx);
            let _ = tx.send(ReadResult::Finished);
            return Ok(());
        }

        let table_count = tables.len();
        let queue = Arc::new(Mutex::new(VecDeque::from(tables)));
        let worker_count = std::cmp::min(self.concurrency, table_count);
        let active_workers = Arc::new(AtomicUsize::new(worker_count));
        let ann_query_info = Arc::clone(&self.ann_query_info);
        let columns = Arc::clone(&self.columns);
        let start_ts = self.start_ts;

        for _ in 0..worker_count {
            let tx_clone = tx.clone();
            let queue_clone = Arc::clone(&queue);
            let ann_query = Arc::clone(&ann_query_info);
            let columns = Arc::clone(&columns);
            let snap_access = self.snap_access.clone();
            let ia_ctx = self.ia_ctx.clone();
            let scan_ctx = self.scan_ctx.clone();
            let active_workers = Arc::clone(&active_workers);

            self.runtime.spawn_blocking(move || {
                let send_finished = Self::worker_loop(
                    &queue_clone,
                    &tx_clone,
                    &snap_access,
                    &ia_ctx,
                    &columns,
                    &scan_ctx,
                    &ann_query,
                    start_ts,
                    read_limit,
                );

                let prev = active_workers.fetch_sub(1, Ordering::AcqRel);
                if send_finished && prev == 1 {
                    let _ = tx_clone.send(ReadResult::Finished);
                }
            });
        }
        drop(tx);
        self.result_rx = Some(rx);
        Ok(())
    }

    fn worker_loop(
        queue: &Arc<Mutex<VecDeque<TableCtx>>>,
        tx: &mpsc::Sender<ReadResult>,
        snap_access: &SnapAccess,
        ia_ctx: &IaCtx,
        columns: &Arc<Vec<ColumnInfo>>,
        scan_ctx: &TableScanCtx,
        ann_query: &Arc<tipb::AnnQueryInfo>,
        start_ts: u64,
        read_limit: usize,
    ) -> bool {
        while let Some(table_ctx) = Self::pop_table(queue) {
            if !Self::process_table(
                table_ctx,
                read_limit,
                tx,
                snap_access,
                ia_ctx,
                columns,
                scan_ctx,
                ann_query,
                start_ts,
            ) {
                return false;
            }
        }
        true
    }

    fn pop_table(queue: &Arc<Mutex<VecDeque<TableCtx>>>) -> Option<TableCtx> {
        let mut guard = queue.lock().unwrap();
        guard.pop_front()
    }

    fn process_table(
        table_ctx: TableCtx,
        read_limit: usize,
        tx: &mpsc::Sender<ReadResult>,
        snap_access: &SnapAccess,
        ia_ctx: &IaCtx,
        columns: &Arc<Vec<ColumnInfo>>,
        scan_ctx: &TableScanCtx,
        ann_query: &Arc<tipb::AnnQueryInfo>,
        start_ts: u64,
    ) -> bool {
        let mut reader = match CloudColumnarReader::new(
            snap_access.clone(),
            ia_ctx.clone(),
            table_ctx.table_id,
            table_ctx.ranges.clone(),
            columns,
            scan_ctx,
            Arc::clone(ann_query),
            start_ts,
        ) {
            Ok(reader) => reader,
            Err(err) => {
                let err = crate::Error::from(crate::table::Error::Other(err.to_string()));
                let _ = tx.send(ReadResult::Error(err));
                return false;
            }
        };

        Self::stream_table_blocks(&mut reader, read_limit, tx)
    }

    fn stream_table_blocks(
        reader: &mut CloudColumnarReader,
        read_limit: usize,
        tx: &mpsc::Sender<ReadResult>,
    ) -> bool {
        loop {
            match reader.ffi_read_block(read_limit) {
                Ok(0) => return true,
                Ok(read_count) => {
                    let block = BlockResult::new(read_count, reader);
                    if tx.send(ReadResult::Block(block)).is_err() {
                        return false;
                    }
                }
                Err(e) => {
                    let _ = tx.send(ReadResult::Error(e));
                    return false;
                }
            }
        }
    }

    fn sequential_read_block(&mut self, read_limit: usize) -> Result<usize> {
        if read_limit == 0 {
            return Ok(0);
        }

        if self.reader.is_none() {
            if self.tables.is_empty() {
                return Ok(0);
            }
            let table_ctx = self.tables.remove(0);
            self.reader = Some(
                CloudColumnarReader::new(
                    self.snap_access.clone(),
                    self.ia_ctx.clone(),
                    table_ctx.table_id,
                    table_ctx.ranges.clone(),
                    &self.columns,
                    &self.scan_ctx,
                    Arc::clone(&self.ann_query_info),
                    self.start_ts,
                )
                .map_err(|e| crate::table::Error::Other(e.to_string()))?,
            );
        }

        loop {
            let read_count = self.reader.as_mut().unwrap().ffi_read_block(read_limit)?;
            if read_count == 0 {
                if self.tables.is_empty() {
                    return Ok(0);
                }
                let table_ctx = self.tables.remove(0);
                self.reader = Some(
                    CloudColumnarReader::new(
                        self.snap_access.clone(),
                        self.ia_ctx.clone(),
                        table_ctx.table_id,
                        table_ctx.ranges.clone(),
                        &self.columns,
                        &self.scan_ctx,
                        Arc::clone(&self.ann_query_info),
                        self.start_ts,
                    )
                    .map_err(|e| crate::table::Error::Other(e.to_string()))?,
                );
                continue;
            }
            return Ok(read_count);
        }
    }
}

struct BlockResult {
    table_id: i64,
    read_count: usize,
    schema: Schema,
    handle_column_id: i64,
    version_column_id: i64,
    handle_data: Vec<u8>,
    version_data: Vec<u8>,
    columns: HashMap<i64, Vec<u8>>,
}

impl BlockResult {
    fn new(read_count: usize, reader: &mut CloudColumnarReader) -> Self {
        let schema = reader.schema.clone();
        let handle_data = reader.ffi_read_handle();
        let version_data = reader.ffi_read_version();
        let mut columns = HashMap::with_capacity(schema.columns.len());
        for col_info in &schema.columns {
            let col_id = col_info.get_column_id();
            if col_info.get_pk_handle() {
                continue;
            }
            columns.insert(col_id, reader.ffi_read_column(col_id));
        }
        let handle_column_id = schema.handle_column.get_column_id();
        let version_column_id = schema.version_column.get_column_id();
        let table_id = schema.table_id;
        Self {
            table_id,
            read_count,
            schema,
            handle_column_id,
            version_column_id,
            handle_data,
            version_data,
            columns,
        }
    }

    // Take the column data and remove it from the block result to avoid cloning.
    // NOTE: This method can only be called once for each column.
    fn take_column_data(&mut self, col_id: i64) -> Vec<u8> {
        if col_id == self.handle_column_id {
            return std::mem::take(&mut self.handle_data);
        }
        if col_id == self.version_column_id {
            return std::mem::take(&mut self.version_data);
        }
        if let Some(col_info) = self.schema.find_column_by_id(col_id) {
            if col_info.get_pk_handle() {
                return std::mem::take(&mut self.handle_data);
            }
        }
        self.columns.remove(&col_id).unwrap_or_default()
    }
}

enum ReadResult {
    Block(BlockResult),
    Error(crate::Error),
    Finished,
}

pub struct CloudColumnarReader {
    tag: String,
    reader: Box<ColumnarMvccReader>,
    ranges: Vec<tipb::KeyRange>,
    block: Block,
    schema: Schema,
    keyspace_id: u32,
    ia_ctx: IaCtx,
    inited: bool,

    read_block_cost: f64,
    serialize_cost: f64,
}

impl CloudColumnarReader {
    pub fn new(
        snap_access: SnapAccess,
        ia_ctx: IaCtx,
        table_id: i64,
        ranges: Vec<tipb::KeyRange>,
        columns: &[ColumnInfo],
        scan_ctx: &TableScanCtx,
        ann_query: Arc<tipb::AnnQueryInfo>,
        start_ts: u64,
    ) -> Result<Self> {
        let mvcc_reader = if ann_query.get_query_type() != tipb::AnnQueryType::InvalidQueryType {
            let index_id = ann_query.get_index_id();
            let target = ann_query
                .get_ref_vec_f32()
                .read_vector_float32()
                .ok()
                .ok_or(crate::Error::Other(
                    "read vector float32 failed".to_string().into(),
                ))?;
            let col_id = get_ann_vec_col_id(&ann_query)?;
            let top_k = ann_query.get_top_k();
            let schema = snap_access
                .new_schema_from_columns(table_id, columns)
                .ok_or(crate::Error::Other(
                    "construct schema from columns failed".to_string().into(),
                ))?;

            let vector_reader_result = snap_access.new_vector_index_reader(
                table_id,
                index_id,
                col_id,
                target.as_ref().data(),
                top_k as usize,
                schema,
                start_ts,
                None, // Pass None to disable filter by handle range.
                None,
                Arc::clone(&ann_query),
            )?;
            if let Some(reader) = vector_reader_result {
                reader
            } else {
                warn!("no vector index reader available, use columnar reader instead");
                snap_access
                    .new_columnar_mvcc_reader(
                        table_id,
                        columns,
                        Some(scan_ctx),
                        start_ts,
                        Some(Arc::clone(&ann_query)),
                    )?
                    .ok_or(crate::Error::Other(
                        "no vector index reader available after fallback"
                            .to_string()
                            .into(),
                    ))?
            }
        } else {
            snap_access
                .new_columnar_mvcc_reader(table_id, columns, Some(scan_ctx), start_ts, None)?
                .ok_or(crate::Error::Other(
                    "no columnar available".to_string().into(),
                ))?
        };
        let schema_file = snap_access.get_schema_file().unwrap();
        let table_schema = schema_file.get_table(table_id).unwrap();
        let mut columns = columns.to_vec();
        columns.retain(|c| !c.get_pk_handle() && c.get_column_id() != HANDLE_COL_ID as i64);
        let schema_buf = SchemaBuf::new(
            table_id,
            table_schema.handle_column.clone(),
            table_schema.version_column.clone(),
            columns,
            table_schema.pk_col_ids.clone(),
            table_schema.max_col_id,
            table_schema.vector_indexes.clone(),
            table_schema.fulltext_indexes.clone(),
            table_schema.get_storage_class_spec().clone(),
            table_schema.partitions.clone(),
        );
        let schema = Schema::new(schema_buf);
        let block = Block::new(&schema);

        let reader = Self {
            tag: snap_access.get_tag().to_string(),
            reader: Box::new(mvcc_reader),
            ranges,
            block,
            schema: schema.clone(),
            keyspace_id: snap_access.get_keyspace_id(),
            read_block_cost: 0.0,
            serialize_cost: 0.0,
            ia_ctx,
            inited: false,
        };
        // Do not call init here to avoid blocking the dispatch thread.
        Ok(reader)
    }

    fn init(&mut self) -> Result<()> {
        if let IaCtx::Enabled(ia_mgr, _) = &self.ia_ctx {
            let start = Instant::now_coarse();
            let prefetch = futures::executor::block_on(self.reader.prefetch_ia_remote_segments(
                &self.tag,
                ia_mgr,
                self.keyspace_id,
                Duration::from_secs(600), // 10 minutes is enough for prefetching one region.
            ))
            .map_err(|e| {
                crate::Error::Other(format!("prefetch_ia_remote_segments failed: {:?}", e).into())
            })?;
            COLUMNAR_PREFETCH_HISTOGRAM.observe(start.saturating_elapsed_secs());
            COLUMNAR_PREFETCH_CACHE_HIT_HISTOGRAM.observe(prefetch.unwrap_or(1.0) * 100.0);
        }
        futures::executor::block_on(self.update_range_handle());
        self.inited = true;
        Ok(())
    }

    // Set next range handle.
    // Return true if next range handle is set, false if no more range.
    async fn update_range_handle(&mut self) -> bool {
        if self.ranges.is_empty() {
            return false;
        }
        let range = self.ranges.remove(0);
        info!(
            "{} update range handle, table_id: {}, range: {:?}",
            self.tag, self.schema.table_id, range
        );
        let is_common_handle = self.reader.get_schema().is_common_handle();
        if is_common_handle {
            let start_handle =
                decode_common_handle(&range.get_low()[KEYSPACE_PREFIX_LEN..]).unwrap_or(&[]);
            let end_handle = decode_common_handle(&range.get_high()[KEYSPACE_PREFIX_LEN..])
                .unwrap_or(GLOBAL_COMMON_HANDLE_END);
            self.reader
                .set_handle_range(start_handle, end_handle)
                .await
                .unwrap();
        } else {
            let start_handle =
                decode_int_handle(&range.get_low()[KEYSPACE_PREFIX_LEN..]).unwrap_or(i64::MIN);
            let end_handle = decode_int_handle(&range.get_high()[KEYSPACE_PREFIX_LEN..])
                .map(|h| Some(h))
                .unwrap_or(None);
            self.reader
                .set_int_handle_range(start_handle, end_handle)
                .await
                .unwrap();
        }
        true
    }

    pub fn ffi_read_block(&mut self, read_limit: usize) -> Result<usize> {
        if !self.inited {
            // Postpone the initialization to here to use concurrent threads.
            self.init()
                .map_err(|e| crate::table::Error::Other(e.to_string()))?;
        }
        self.block.reset();
        let start = Instant::now_coarse();
        let ret = futures::executor::block_on(self.read_block_with_range(read_limit));
        self.read_block_cost += start.saturating_elapsed_secs();
        ret
    }

    pub async fn read_block_with_range(&mut self, read_limit: usize) -> Result<usize> {
        loop {
            let read_count = self.reader.read_block(&mut self.block, read_limit).await?;
            if read_count > 0 {
                return Ok(read_count);
            }
            // Data may be read out and filtered out in previous range, we need to reset the
            // reader here to continue reading.
            self.reader.reset();
            if !self.update_range_handle().await {
                return Ok(0);
            }
        }
    }

    pub fn ffi_read_handle(&mut self) -> Vec<u8> {
        let start = Instant::now_coarse();
        let mut data = vec![];
        let col_info = &self.schema.handle_column;
        self.block.get_handle_buf().serialize_for_tiflash(
            &mut data,
            col_info.get_tp(),
            Self::is_unsigned(col_info),
            col_info.get_column_len() as usize,
        );
        self.serialize_cost += start.saturating_elapsed_secs();
        data
    }

    pub fn ffi_read_version(&mut self) -> Vec<u8> {
        let start = Instant::now_coarse();
        let mut data = vec![];
        let col_info = &self.schema.version_column;
        self.block.get_version_buf().serialize_for_tiflash(
            &mut data,
            col_info.get_tp(),
            Self::is_unsigned(col_info),
            col_info.get_column_len() as usize,
        );
        self.serialize_cost += start.saturating_elapsed_secs();
        data
    }

    fn is_unsigned(col_info: &ColumnInfo) -> bool {
        match FieldTypeFlag::from_bits(col_info.get_flag() as u32) {
            Some(flag) => flag.contains(FieldTypeFlag::UNSIGNED),
            None => false,
        }
    }

    pub fn ffi_read_column(&mut self, col_id: i64) -> Vec<u8> {
        let start = Instant::now_coarse();
        let mut data = vec![];
        let col_info = self.schema.find_column_by_id(col_id).unwrap();
        if col_info.get_pk_handle() {
            self.block.get_handle_buf().serialize_for_tiflash(
                &mut data,
                col_info.get_tp(),
                Self::is_unsigned(col_info),
                col_info.get_column_len() as usize,
            );
        } else {
            self.block.get_column(col_id).serialize_for_tiflash(
                &mut data,
                col_info.get_tp(),
                Self::is_unsigned(col_info),
                col_info.get_column_len() as usize,
            );
        }
        self.serialize_cost += start.saturating_elapsed_secs();
        data
    }

    pub fn physical_table_id(&self) -> i64 {
        self.schema.table_id
    }
}

impl Drop for CloudColumnarReader {
    fn drop(&mut self) {
        info!(
            "{} CloudColumnarReader dropped, table_id: {} read_block_cost: {:.3}s, serialize_cost: {:.3}s",
            self.tag, self.schema.table_id, self.read_block_cost, self.serialize_cost
        );
    }
}

// Due to the discarding of column_id in the vector index, the value assigned to
// ann_query_info may be different in different versions of tidb. For
// compatibility, use this function to get column_id.
// For field changes, see: https://github.com/pingcap/tipb/pull/358
fn get_ann_vec_col_id(ann_query: &AnnQueryInfo) -> Result<i64> {
    // Check `has_deprecated_column_id` first, because `column` is marked as
    // not null in TiDB side so that it will be always populated with some value.
    if ann_query.has_deprecated_column_id() {
        Ok(ann_query.get_deprecated_column_id())
    } else if ann_query.has_column() {
        Ok(ann_query.get_column().get_column_id())
    } else {
        Err(crate::Error::Other(
            "unexpected empty vector column id in ANNQueryInfo"
                .to_string()
                .into(),
        ))
    }
}

/// To check that the key already exists or not.
struct TxnFileConstraintChecker<'a> {
    already_exist_key: Option<Vec<u8>>,
    item: Item<'a>,
    snap: &'a SnapAccessCore,
    read_ts: u64,
}

impl<'a> TxnFileConstraintChecker<'a> {
    fn new(snap: &'a SnapAccessCore, read_ts: u64) -> Self {
        let mut item = Item::new();
        item.owned_val = Some(vec![]);
        Self {
            already_exist_key: None,
            item,
            snap,
            read_ts,
        }
    }
}

#[maybe_async::async_trait]
impl ConstraintChecker for TxnFileConstraintChecker<'_> {
    #[maybe_async]
    async fn check(&mut self, key: InnerKey<'_>) -> bool {
        self.item.path = AccessPath::default();
        self.item.val = self
            .snap
            .get_value(
                WRITE_CF,
                key,
                self.read_ts,
                &mut self.item.path,
                self.item.owned_val.as_mut().unwrap(),
                &[],
            )
            .await;
        // Blob value is not needed here.

        if self.item.value_len() > 0 {
            let outer_key = self.snap.data.to_outer_key(key);
            self.already_exist_key = Some(outer_key);
            return false; // Return false to stop iteration.
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, iter::Iterator, ops::Deref, sync::Arc};

    use api_version::{api_v2::KEYSPACE_PREFIX_LEN, ApiV2};
    use bytes::{Buf, Bytes};
    use cloud_encryption::{EncryptionKey, MasterKey};
    use futures::executor::block_on;
    use kvenginepb::TableCreate;
    use proptest::prelude::*;
    use protobuf::Message;
    use tikv_util::memory::MemoryLimiter;

    use crate::{
        apply::create_snapshot_tables,
        context::{new_meta_file_cache, IaCtx, PrepareType, SnapCtx},
        dfs::{self, Dfs, InMemFs},
        read::MEM_DATA_FORMAT_V2,
        shard::ShardDataBuilder,
        table::{
            self,
            columnar::ColumnarMetaCache,
            file::InMemFile,
            memtable::CfTable,
            sstable::{test_util::build_test_table_with_kvs, BlockCache},
            InnerKey, OwnedInnerKey, TxnChunk, TxnChunkBuilder, TxnCtx, TxnFile, TxnFileId, OP_PUT,
        },
        txn_chunk_manager::{with_pool_size, TxnChunkManager, TxnChunkManagerConfig},
        util::test_util::KeyBuilder,
        ChangeSet, Shard, ShardRange, SnapAccess, UserMeta, ENCRYPTION_KEY, GLOBAL_SHARD_END_KEY,
        WRITE_CF,
    };

    const KEYSPACE_ID: u32 = 42;

    #[test]
    fn test_estimated_range_blocks_size() {
        let mut cs_pb = kvenginepb::ChangeSet::default();
        let mut cs = ChangeSet::new(cs_pb.clone());
        let snap = cs_pb.mut_snapshot();
        let mut build_table_fn =
            |start: i32, end: i32, level: u32, tbl_entries: usize, step: usize| {
                let mut kvs = vec![];
                for i in (start..end).step_by(step) {
                    let key = format!("key{:05x}", i);
                    let val = key.repeat(10);
                    kvs.push((key, val));
                    if kvs.len() == tbl_entries {
                        let tbl = build_test_table_with_kvs(&kvs);
                        let mut tbl_create = TableCreate::default();
                        tbl_create.id = tbl.id();
                        tbl_create.level = level;
                        tbl_create.smallest = tbl.smallest().to_vec();
                        tbl_create.biggest = tbl.biggest().to_vec();
                        tbl_create.meta_offset = tbl.meta_offset();
                        kvs.truncate(0);
                        cs.ln_tables.insert(tbl.id(), tbl.clone());
                        snap.mut_table_creates().push(tbl_create);
                    }
                }
            };
        build_table_fn(0, 10000, 3, 1000, 1);
        build_table_fn(0, 10000, 2, 500, 5);
        build_table_fn(0, 10000, 1, 200, 20);
        cs.change_set = cs_pb;
        let range = ShardRange::new(&[], GLOBAL_SHARD_END_KEY);
        let opt = Arc::new(crate::options::Options::default());
        let master_key = MasterKey::new(&[1u8; 32]);
        let shard = Shard::new(
            1,
            &kvenginepb::Properties::new(),
            1,
            range,
            0,
            opt,
            &master_key,
        );
        let mut builder = ShardDataBuilder::new(shard.get_data());
        create_snapshot_tables(
            &mut builder,
            cs.get_snapshot(),
            &cs,
            false,
            PrepareType::All,
        );
        builder.set_schema(
            cs.get_schema_version(),
            cs.get_restore_version(),
            cs.get_schema_file(),
        );
        shard.set_data(builder.build());
        let snap = shard.new_snap_access();

        let build_range_fn = |start: i32, end: i32| {
            (
                Bytes::from(format!("key{:05x}", start)),
                Bytes::from(format!("key{:05x}", end)),
            )
        };
        let total_blocks_size = snap.estimated_range_blocks_size(&[build_range_fn(0, 10000)]);
        assert_eq!(total_blocks_size, 1103566);

        // verify that many small ranges are properly deduplicated, num blocks never
        // exceed total.
        let mut many_small_ranges = vec![];
        for i in (0..10000).step_by(10) {
            let small_range = build_range_fn(i, i + 5);
            many_small_ranges.push(small_range);
        }
        let many_small_ranges_blocks_size = snap.estimated_range_blocks_size(&many_small_ranges);
        assert_eq!(many_small_ranges_blocks_size + 112, total_blocks_size);

        let half_num_blocks = snap.estimated_range_blocks_size(&[build_range_fn(5000, 10000)]);
        assert_eq!(half_num_blocks, 548206);

        for i in (100..10000).step_by(100) {
            let blocks_size = snap.estimated_range_blocks_size(&[build_range_fn(i, i + 1)]);
            // some range on level 1 doesn't overlap any table, so blocks_size may vary.
            assert!(blocks_size == 11137 || blocks_size == 7577);
        }

        let blocks_size = snap.estimated_range_blocks_size(&[
            build_range_fn(1, 2),
            build_range_fn(2, 3),
            build_range_fn(3, 4),
            build_range_fn(4, 5),
        ]);
        // each level only access one block.
        assert_eq!(blocks_size, 11137);

        let blocks_size = snap.estimated_range_blocks_size(&[
            build_range_fn(1, 2),
            build_range_fn(2000, 2001),
            build_range_fn(3000, 3001),
            build_range_fn(4000, 4001),
        ]);
        // each range on each level access one block.
        assert_eq!(blocks_size, 44548);
    }

    const MAX_I: usize = 100;

    fn mem_table_op_strategy() -> impl Strategy<Value = MemTableOp> {
        prop_oneof![
            (
                prop::collection::vec(1..MAX_I, 0..=20usize),
                any::<bool>(),
                0..5usize,
            )
                .prop_map(|(batch, switch, mvcc_count)| MemTableOp::WriteBatch(
                    batch, switch, mvcc_count
                )),
            (prop::collection::vec(1..MAX_I, 0..=20usize), any::<bool>())
                .prop_map(|(batch, switch)| MemTableOp::WriteTxnFile(batch, switch)),
        ]
    }

    prop_compose! {
        fn arb_mem_table_ops(size: usize)
            (ops in prop::collection::vec(mem_table_op_strategy(), 0..=size))
            -> Vec<MemTableOp> {
            ops
        }
    }

    // [10, 20, 30, 40, 50] -> [(10, 20), (30, 40)]
    fn query_ranges() -> impl Strategy<Value = Vec<(usize, usize)>> {
        prop::collection::vec(0..MAX_I + 1, 0..=10usize).prop_map(|mut is| -> Vec<(usize, usize)> {
            is.sort();
            let ranges = is
                .chunks_exact(2)
                .filter_map(|chunk| (chunk[0] != chunk[1]).then_some((chunk[0], chunk[1])))
                .collect::<Vec<_>>();
            if ranges.is_empty() {
                vec![(0, MAX_I)]
            } else {
                ranges
            }
        })
    }

    proptest! {
        #[test]
        fn test_serde_mem_tables(
            ops in arb_mem_table_ops(10),
            ranges in query_ranges(),
            enable_enc in any::<bool>(),
        ) {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let _enter = runtime.enter();

            let engine_id = 1;
            let shard_id = 1;
            let shard_ver = 10;

            let kb = KeyBuilder::new(KEYSPACE_ID, "t_");
            let dfs: Arc<dyn crate::dfs::Dfs> = Arc::new(InMemFs::new());
            let txn_chunk_manager = TxnChunkManager::new(vec![], dfs.clone(), BlockCache::None, None, with_pool_size(2), TxnChunkManagerConfig::default());

            let master_key = MasterKey::new(&[1u8; 32]);
            let enc_key = enable_enc.then(||master_key.generate_encryption_key());

            let (mem_tbls, ref_store) = make_mem_tables(ops, &kb, dfs.clone(), enc_key.as_ref());
            verify_mem_tables(&mem_tbls, &ref_store, &[(kb.i_to_inner_key(0).as_ref(), kb.i_to_inner_key(MAX_I).as_ref())])?;

            let (outer_start, outer_end) = ApiV2::get_txn_keyspace_range(KEYSPACE_ID);
            let range = ShardRange::new(&outer_start, &outer_end);
            let inner_key_off = KEYSPACE_PREFIX_LEN;
            let opt = Arc::new(crate::options::Options::default());

            let inner_ranges = ranges.iter().map(|&(start, end)| {
                (kb.i_to_inner_key(start), kb.i_to_inner_key(end))
            }).collect::<Vec<_>>();
            let outer_ranges = ranges.into_iter().map(|(start, end)| {
                (Bytes::from(kb.i_to_outer_key(start)), Bytes::from(kb.i_to_outer_key(end)))
            }).collect::<Vec<_>>();

            // Serialize
            let mem_bin = {
                let mut props = kvenginepb::Properties {
                    shard_id,
                    ..Default::default()
                };
                if let Some(enc_key) = &enc_key {
                    props.mut_keys().push(ENCRYPTION_KEY.to_string());
                    props.mut_values().push(enc_key.export());
                }
                let shard = Shard::new(
                    engine_id,
                    &props,
                    shard_ver,
                    range,
                    inner_key_off,
                    opt,
                    &master_key,
                );
                let mut builder = ShardDataBuilder::new(shard.get_data());
                builder.set_mem_tbls(mem_tbls);
                shard.set_data(builder.build());
                let snap_access = shard.new_snap_access();

                snap_access.build_mem_data(&outer_ranges, u64::MAX)
            };
            let format_ver = mem_bin.as_slice().get_u32_le();
            prop_assert_eq!(format_ver, MEM_DATA_FORMAT_V2);

            // Deserialize
            let snap_ctx = SnapCtx {
                dfs,
                master_key,
                block_cache: BlockCache::None,
                vector_index_cache: None,
                columnar_file_cache: None,
                schema_files: None,
                txn_chunk_manager,
                ia_ctx: IaCtx::Disabled,
                prepare_type: PrepareType::All,
                read_columnar: true,
                meta_file_cache: new_meta_file_cache(1024),
                columnar_meta_cache: ColumnarMetaCache::default(),
            };
            let mut snap_pb = kvenginepb::Snapshot::default();
            snap_pb.set_inner_key_off(KEYSPACE_PREFIX_LEN as u32);
            let props = snap_pb.mut_properties();
            if let Some(enc_key) = &enc_key {
                props.mut_keys().push(ENCRYPTION_KEY.to_string());
                props.mut_values().push(enc_key.export());
            }
            let mut cs = kvenginepb::ChangeSet::default();
            cs.set_shard_id(shard_id);
            cs.set_shard_ver(shard_ver);
            cs.set_snapshot(snap_pb);
            let snap_bin = cs.write_to_bytes().unwrap();
            let mem_limiter = MemoryLimiter::new(u64::MAX, None);
            let remote_snap = block_on(SnapAccess::construct_snapshot("test", &snap_ctx, &mem_bin, &snap_bin, mem_limiter)).unwrap().0;

            let inner_ranges = inner_ranges.iter().map(|(start, end)| (start.as_ref(), end.as_ref())).collect::<Vec<_>>();
            let ref_store_in_ranges = ref_store.new_in_ranges(&inner_ranges);
            verify_mem_tables(remote_snap.data.mem_tbls.as_slice(), &ref_store_in_ranges, &inner_ranges)?;
        }
    }

    #[derive(Debug, Clone)]
    enum MemTableOp {
        WriteBatch(
            Vec<usize>,
            bool,  // switch
            usize, // mvcc version count
        ),
        WriteTxnFile(Vec<usize>, bool /* switch */),
    }

    #[derive(Default, Debug)]
    struct RefStore {
        inner: BTreeMap<(Bytes, u64), String>,
    }

    impl RefStore {
        fn put(&mut self, key: OwnedInnerKey, ver: u64, val: String) {
            self.inner.insert((key.into_inner(), ver), val);
        }

        fn get_value(&self, key: InnerKey<'_>, ver: u64) -> Option<&String> {
            self.inner.get(&(Bytes::copy_from_slice(key.deref()), ver))
        }

        fn new_in_ranges(&self, ranges: &[(InnerKey<'_>, InnerKey<'_>)]) -> Self {
            let mut new_store = RefStore::default();
            for &(start, end) in ranges {
                let start = Bytes::copy_from_slice(start.deref());
                let end = Bytes::copy_from_slice(end.deref());
                for ((k, ver), v) in self
                    .inner
                    .range::<(Bytes, u64), _>((start.clone(), 0)..(end, 0))
                {
                    new_store.inner.insert((k.clone(), *ver), v.clone());
                }
            }
            new_store
        }

        fn len(&self) -> usize {
            self.inner.len()
        }
    }

    fn make_mem_tables(
        ops: Vec<MemTableOp>,
        kb: &KeyBuilder,
        dfs: Arc<dyn Dfs>,
        enc_key: Option<&EncryptionKey>,
    ) -> (Vec<CfTable>, RefStore) {
        let mut mem_tbls = vec![CfTable::new()];
        let mut ref_store = RefStore::default();
        let mut wb = crate::table::memtable::WriteBatch::new();
        let mut next_txn_chunk_id = 1000;

        let switch_mem_table = |mem_tbls: &mut Vec<CfTable>| {
            let mut new_mem_tbls = Vec::with_capacity(mem_tbls.len() + 1);
            new_mem_tbls.push(CfTable::new());
            new_mem_tbls.append(mem_tbls);
            *mem_tbls = new_mem_tbls;
        };

        for (idx, op) in ops.into_iter().enumerate() {
            let start_ts = idx as u64 * 100;
            let commit_ts = (idx as u64 + 1) * 100;
            let user_meta = UserMeta::new(start_ts, commit_ts);

            match op {
                MemTableOp::WriteBatch(is, switch, mvcc_count) => {
                    for i in is {
                        let key = kb.i_to_inner_key(i);
                        let val = kb.i_to_val(start_ts as usize + i);
                        wb.put(
                            key.as_ref(),
                            0,
                            &user_meta.to_array(),
                            commit_ts,
                            val.as_bytes(),
                        );
                        ref_store.put(key.clone(), commit_ts, val.clone());
                        let mut start_ts = start_ts;
                        let mut commit_ts = commit_ts;
                        for _ in 0..mvcc_count {
                            start_ts += 1;
                            commit_ts += 1;
                            let user_meta = UserMeta::new(start_ts, commit_ts);
                            let val = kb.i_to_val(start_ts as usize + i);
                            wb.put(
                                key.as_ref(),
                                0,
                                &user_meta.to_array(),
                                commit_ts,
                                val.as_bytes(),
                            );
                            ref_store.put(key.clone(), commit_ts, val.clone());
                        }
                    }
                    let mem_tbl = mem_tbls[0].get_cf(WRITE_CF);
                    mem_tbl.put_batch(&mut wb, None, WRITE_CF);
                    wb.reset();

                    if switch {
                        switch_mem_table(&mut mem_tbls);
                    }
                }
                MemTableOp::WriteTxnFile(mut is, switch) => {
                    is.sort();
                    is.dedup();
                    let mut txn_chunks = vec![];
                    for chunk in is.chunks(3) {
                        next_txn_chunk_id += 1;
                        let mut builder =
                            TxnChunkBuilder::new(next_txn_chunk_id, 64, enc_key.cloned());
                        for &i in chunk {
                            let key = kb.i_to_inner_key(i);
                            let val = kb.i_to_val(start_ts as usize + i);
                            builder.add_entry(
                                InnerKey::from_outer_key(&kb.i_to_key(i)),
                                OP_PUT,
                                val.as_bytes(),
                            );
                            ref_store.put(key, commit_ts, val.clone());
                        }
                        let mut chunk_data = vec![];
                        builder.finish(&mut chunk_data);

                        let opts = dfs::Options::default().with_type(dfs::FileType::TxnChunk);
                        block_on(dfs.create(next_txn_chunk_id, chunk_data.clone().into(), opts))
                            .unwrap();

                        let chunk_file =
                            Arc::new(InMemFile::new(next_txn_chunk_id, chunk_data.into()));
                        let txn_chunk =
                            TxnChunk::new(chunk_file, BlockCache::None, enc_key.cloned()).unwrap();
                        txn_chunks.push(txn_chunk);
                    }

                    if !txn_chunks.is_empty() {
                        let id = TxnFileId::new(1, 1, start_ts);
                        let lower_bound = InnerKey::from_inner_buf(b"");
                        let upper_bound = InnerKey::from_inner_buf(GLOBAL_SHARD_END_KEY);
                        let txn_ctx = TxnCtx::new(
                            user_meta.to_array().to_vec().into(),
                            Default::default(),
                            commit_ts,
                            lower_bound,
                            upper_bound,
                        );
                        let txn_file = TxnFile::new(id, txn_chunks, txn_ctx).unwrap();
                        mem_tbls[0] = mem_tbls[0].add_write_cf_txn_files(&[txn_file]);
                        if switch {
                            switch_mem_table(&mut mem_tbls);
                        }
                    }
                }
            }
        }
        (mem_tbls, ref_store)
    }

    fn verify_mem_tables(
        mem_tbls: &[CfTable],
        ref_store: &RefStore,
        ranges: &[(InnerKey<'_>, InnerKey<'_>)],
    ) -> std::result::Result<(), TestCaseError> {
        let mem_iters = mem_tbls
            .iter()
            .map(|m| m.get_cf(WRITE_CF).new_iterator(false))
            .collect();
        let mut iter = table::new_merge_iterator(mem_iters, false);
        let mut count = 0;
        for &(start, end) in ranges {
            iter.seek(start);
            while iter.valid() && iter.key() < end {
                count += 1;
                let expect = ref_store.get_value(iter.key(), iter.value().version);
                prop_assert!(expect.is_some(), "key: {:?}", iter.key());
                let val = iter.value();
                prop_assert_eq!(val.get_value(), expect.unwrap().as_bytes());

                iter.next_all_version();
            }
        }
        prop_assert_eq!(count, ref_store.len());
        Ok(())
    }
}
