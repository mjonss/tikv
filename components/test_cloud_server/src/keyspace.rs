// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt, mem, ops,
    ops::{Deref, DerefMut, Range},
    sync::{Arc, Mutex, MutexGuard, atomic::AtomicU32},
    time::Duration,
};

use api_version::ApiV2;
use bytes::BufMut;
use codec::number::NumberEncoder;
use dashmap::{
    DashMap,
    mapref::{entry::Entry, one::Ref},
};
use kvengine::table::schema_file::Schema;
use kvproto::kvrpcpb::Op;
use rand::{
    Rng,
    distributions::Distribution,
    prelude::{IteratorRandom, SliceRandom, ThreadRng},
};
use schema::schema::StorageClassSpec;
use tikv_client::TimestampExt;
use tikv_util::info;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use crate::{
    client::{ClusterTxnClient, RefStore, Result},
    table::TableMeta,
    util::{Mutation, TableSchemaOptions, build_schemas},
};

#[derive(Clone, Default)]
pub struct KeyspaceManager {
    core: Arc<KeyspaceManagerCore>,
}

impl Deref for KeyspaceManager {
    type Target = KeyspaceManagerCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

#[derive(Default)]
pub struct KeyspaceManagerCore {
    keyspaces: DashMap<u32 /* keyspace_id */, KeyspaceMeta>,
    ref_stores: KeyspaceRefStores,
    backups: Mutex<Vec<KeyspaceBackup>>,
    random: Mutex<RandomHelper>,
    max_keyspace_id: AtomicU32,
}

impl KeyspaceManagerCore {
    pub fn create_keyspaces(
        &self,
        keyspace_ids: &[u32], // keyspace_ids must be allocated by new_keyspace_id().
        keyspace_names: Vec<String>,
        options: &CreateKeyspaceOptions,
        need_shuffle: Option<&mut ThreadRng>,
    ) {
        assert_eq!(keyspace_ids.len(), keyspace_names.len());
        for (&keyspace_id, keyspace_name) in keyspace_ids.iter().zip(keyspace_names.into_iter()) {
            match self.keyspaces.entry(keyspace_id) {
                Entry::Occupied(_) => panic!("duplicated keyspace {}", keyspace_id),
                Entry::Vacant(entry) => {
                    entry.insert(KeyspaceMeta::new(keyspace_id, keyspace_name, options));
                }
            }
        }

        self.random
            .lock()
            .unwrap()
            .add_keyspaces(keyspace_ids, need_shuffle);
    }

    pub async fn create_single_keyspace(
        &self,
        keyspace_id: u32,
        keyspace_name: String,
        options: &CreateKeyspaceOptions,
        locked: bool,
    ) -> Option<OwnedRwLockWriteGuard<()>> {
        let ks_meta = KeyspaceMeta::new(keyspace_id, keyspace_name, options);
        let lock_opt = if locked {
            let lock = ks_meta.get_locks();
            Some(lock.mutex_lock().await)
        } else {
            None
        };
        let old = self.keyspaces.insert(keyspace_id, ks_meta);
        assert!(old.is_none());
        self.random
            .lock()
            .unwrap()
            .add_keyspaces(&[keyspace_id], None);
        lock_opt
    }

    pub fn get_uniform_random_keyspace(&self, rng: &mut ThreadRng) -> u32 {
        self.random.lock().unwrap().uniformly_choose(rng)
    }

    /// Get a random keyspace conforms zipf distribution.
    ///
    /// Note that zipf distribution generate sample from 1 to n conforms the
    /// probability mass function (see https://en.wikipedia.org/wiki/Zipf's_law).
    ///
    /// We need to shuffle keyspaces in advance before pick a random keyspace.
    /// Otherwise the first keyspace will always have the maximum chance.
    pub fn get_zipf_random_keyspace(&self, rng: &mut ThreadRng) -> u32 {
        self.random.lock().unwrap().zipf_choose(rng)
    }

    pub fn new_keyspace_id(&self, delta: u32) -> u32 {
        assert!(delta > 0);
        self.max_keyspace_id
            .fetch_add(delta, std::sync::atomic::Ordering::AcqRel)
            + delta
    }

    pub fn get_all_keyspaces(&self) -> Vec<u32> {
        self.keyspaces.iter().map(|item| *item.key()).collect()
    }

    pub fn ref_stores(&self) -> &KeyspaceRefStores {
        &self.ref_stores
    }

    // Note: use with caution.
    // The returned value will keep a read lock on `self.keyspaces`.
    pub fn get_keyspace_meta(&self, keyspace_id: u32) -> Option<Ref<'_, u32, KeyspaceMeta>> {
        self.keyspaces.get(&keyspace_id)
    }

    fn set_keyspace_meta(&self, keyspace_id: u32, mut meta: KeyspaceMetaCore) {
        let mut keyspace = self.keyspaces.get_mut(&keyspace_id).unwrap();

        // Rewrite the del_prefixes with new keyspace prefix.
        let keyspace_prefix = ApiV2::get_txn_keyspace_prefix(keyspace_id);
        meta.del_prefixes.rewrite_range_prefix(&keyspace_prefix);

        info!(
            "KeyspaceManager.set_keyspace_meta, keyspace_id {}, old {:?}, new {:?}",
            keyspace_id, keyspace.core, meta
        );

        keyspace.core = meta;
    }

    pub fn get_random_available_table(
        &self,
        keyspace_id: u32,
        rng: &mut ThreadRng,
        include_schema: bool,
    ) -> Option<TableMeta> {
        self.keyspaces
            .get(&keyspace_id)
            .unwrap()
            .get_random_available_table(rng, include_schema)
    }

    pub fn get_table(&self, keyspace_id: u32, table_id: i64) -> Option<TableMeta> {
        self.keyspaces
            .get(&keyspace_id)
            .unwrap()
            .get_table(table_id)
            .map(|t| t.clone())
    }

    pub fn add_backup(&self, backup: KeyspaceBackup) {
        self.backups.lock().unwrap().push(backup);
    }

    pub fn get_random_backup(&self, rng: &mut ThreadRng) -> Option<KeyspaceBackup> {
        let backups = self.backups.lock().unwrap();
        backups.iter().choose(rng).cloned()
    }

    pub fn backup_keyspace(&self, keyspace_id: u32, backup_ts: u64) -> KeyspaceBackup {
        let ref_store = self.ref_stores.dump(keyspace_id);
        let meta = self.keyspaces.get(&keyspace_id).unwrap().core.clone();
        KeyspaceBackup {
            backup_name: None,
            backup_ts,
            keyspace_id,
            ref_store,
            meta,
        }
    }

    pub fn restore_keyspace(&self, tag: &str, backup: KeyspaceBackup, target_keyspace: u32) {
        let KeyspaceBackup {
            ref_store,
            meta,
            keyspace_id,
            ..
        } = backup;
        self.set_keyspace_meta(target_keyspace, meta);
        self.ref_stores
            .restore_keyspace(tag, ref_store, keyspace_id, target_keyspace);
    }

    pub fn drop_table(&self, keyspace_id: u32, table_id: i64, ts: u64) {
        let start = make_row_key(keyspace_id, table_id, &[]);
        let end = make_row_key(keyspace_id, table_id + 1, &[]);
        debug_assert!(tidb_query_common::util::is_prefix_next(&start, &end));

        self.keyspaces
            .get_mut(&keyspace_id)
            .unwrap()
            .del_prefixes
            .merge_prefix_in_place(&start);
        self.ref_stores.destroy_range(keyspace_id, &start, &end);
        self.schedule_destroy_range(ts, keyspace_id, table_id);

        info!(
            "KeyspaceManager.drop_table, keyspace {}, table {}, start {}, end {}, del_prefixes {:?}",
            keyspace_id,
            table_id,
            log_wrappers::hex_encode_upper(start),
            log_wrappers::hex_encode_upper(end),
            self.keyspaces.get(&keyspace_id).unwrap().del_prefixes
        );
    }

    /// Get complementary ranges from destroyed ranges.
    pub fn get_complementary_ranges(&self, keyspace_id: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
        let keyspace = self.keyspaces.get(&keyspace_id).unwrap();
        let (start, end) = ApiV2::get_txn_keyspace_range(keyspace_id);
        keyspace.del_prefixes.get_complementary_ranges(&start, &end)
    }

    /// Schedule destroy range to run after GC safepoint has gone by `ts`.
    ///
    /// To ensure that, the backup which contains the data before "drop table"
    /// action, does not contain the "destroy range" process. As the "destroy
    /// range" is "unsafe" and not recoverable.
    ///
    /// Note: also depends on the GC safe point is advanced only after the
    /// backup is finished (see "service safe point").
    fn schedule_destroy_range(&self, ts: u64, keyspace_id: u32, table_id: i64) {
        self.keyspaces
            .get_mut(&keyspace_id)
            .unwrap()
            .pending_destroy_range
            .push(DestroyRangeTask { ts, table_id });
    }

    pub fn pick_destroy_range_tasks(
        &self,
        keyspace_id: u32,
        gc_safepoint: u64,
    ) -> Vec<DestroyRangeTask> {
        self.keyspaces
            .get_mut(&keyspace_id)
            .unwrap()
            .pending_destroy_range
            .extract_if(.., |task| task.ts < gc_safepoint)
            .collect()
    }
}

pub struct CreateKeyspaceOptions {
    pub table_count: usize,
    pub schema_enable_ratio: f64,
    pub storage_class_spec_fn: Box<dyn Fn(i64) -> StorageClassSpec>,
}

impl Default for CreateKeyspaceOptions {
    fn default() -> Self {
        Self {
            table_count: 0,
            schema_enable_ratio: 0.0,
            storage_class_spec_fn: Box::new(|_| StorageClassSpec::default()),
        }
    }
}

pub struct KeyspaceMeta {
    core: KeyspaceMetaCore,
    inner_lock: Arc<RwLock<()>>,
    extra_lock: Arc<Mutex<()>>,
}

impl ops::Deref for KeyspaceMeta {
    type Target = KeyspaceMetaCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl ops::DerefMut for KeyspaceMeta {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.core
    }
}

/// Used for keyspace meta backup & restore without locks.
#[derive(Clone)]
pub struct KeyspaceMetaCore {
    keyspace_id: u32,

    /// Used to locate TiDB in `tidb::TidbCluster`.
    ///
    /// As keyspace id is allocated by PD (in real PD servers), it may not be
    /// equal to TiDB index in `tidb::TidbCluster`.
    name: String,

    tables: DashMap<i64 /* table_id */, TableMeta>,

    /// Used to verify data considering the delete prefixes state of shard.
    ///
    /// As the operation of destroy range is async, in some scene the data
    /// within the range to be destroyed is not determined.
    del_prefixes: kvengine::DeletePrefixes,

    /// Used to make pending destroy range tasks be able to be backup and
    /// restored.
    pending_destroy_range: Vec<DestroyRangeTask>,

    schemas: Vec<Schema>,
}

impl fmt::Debug for KeyspaceMetaCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyspaceMetaCore")
            .field("id", &self.keyspace_id)
            .field("name", &self.name)
            .field(
                "tables",
                &self
                    .tables
                    .iter()
                    .map(|x| (*x.key(), x.value().is_available()))
                    .collect::<Vec<_>>(),
            )
            .field("del_prefixes", &self.del_prefixes)
            .finish()
    }
}

impl KeyspaceMeta {
    pub fn new(keyspace_id: u32, name: String, options: &CreateKeyspaceOptions) -> Self {
        let tables = DashMap::default();
        let mut tables_schema_opts = vec![];
        for _ in 0..options.table_count {
            let is_schema_enabled = rand::thread_rng().gen_bool(options.schema_enable_ratio);
            let table = TableMeta::new(
                keyspace_id,
                true,
                is_schema_enabled,
                StorageClassSpec::default(),
            );
            let table_id = table.id();
            let sc_spec = (options.storage_class_spec_fn)(table_id);
            table.set_storage_class_spec(sc_spec.clone());
            info!("keyspace manager: create table"; "keyspace" => keyspace_id, "meta" => ?table);
            tables.insert(table_id, table);

            if is_schema_enabled || sc_spec.is_specified() {
                tables_schema_opts.push(TableSchemaOptions {
                    table_id,
                    with_columns: is_schema_enabled,
                    storage_class_spec: sc_spec,
                });
            }
        }
        let schemas = build_schemas(&tables_schema_opts);
        Self {
            core: KeyspaceMetaCore {
                keyspace_id,
                name,
                tables,
                del_prefixes: kvengine::DeletePrefixes::new_with_keyspace_id(keyspace_id),
                pending_destroy_range: Default::default(),
                schemas,
            },
            inner_lock: Default::default(),
            extra_lock: Default::default(),
        }
    }

    pub fn get_locks(&self) -> KeyspaceLockHelper {
        KeyspaceLockHelper {
            inner: self.inner_lock.clone(),
            extra: self.extra_lock.clone(),
        }
    }

    pub fn get_random_available_table(
        &self,
        rng: &mut ThreadRng,
        include_schema: bool,
    ) -> Option<TableMeta> {
        self.tables
            .iter()
            .filter_map(|x| {
                if !include_schema && x.is_schema_enabled() {
                    return None;
                }
                x.is_available().then_some(x.value().clone())
            })
            .choose(rng)
    }

    pub fn new_table(
        &self,
        is_available: bool,
        is_schema_enabled: bool,
        storage_class_spec: StorageClassSpec,
    ) -> i64 {
        let table = TableMeta::new(
            self.keyspace_id,
            is_available,
            is_schema_enabled,
            storage_class_spec,
        );
        let table_id = table.id();
        self.tables.insert(table.id(), table);
        table_id
    }

    /// Use with caution, as the returned value will keep a read lock on
    /// `tables`.
    pub fn get_table(&self, table_id: i64) -> Option<Ref<'_, i64, TableMeta>> {
        self.tables.get(&table_id)
    }

    pub fn get_all_available_tables(&self) -> Vec<i64> {
        let mut ids: Vec<i64> = self
            .tables
            .iter()
            .filter(|t| t.is_available())
            .map(|t| t.id())
            .collect();
        ids.sort();
        ids
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn schemas(&self) -> Vec<Schema> {
        self.schemas.clone()
    }
}

/// Helper to get locks for different workloads.
///
/// Shared (read) lock for: reads/writes, restore (without verification),
/// load_data.
///
/// Mutual-exclusive (write) lock for: backup, restore (with verification),
/// destroy_table.
///
/// For load_data:
/// * Data is ingested to new table, and the new table is not available until
///   the ingest finished. So load_data doesn't need to be mutual-exclusive with
///   reads/writes.
/// * Besides, acquire extra_lock to be mutual-exclusive with major_compaction.
///
/// For destroy_table: downgrade to shared lock after set table to unavailable.
///
/// For major_compaction: acquire extra_lock to be mutual-exclusive with
/// load_data.
pub struct KeyspaceLockHelper {
    inner: Arc<RwLock<()>>,
    extra: Arc<Mutex<()>>,
}

impl KeyspaceLockHelper {
    pub fn try_shared_lock(&self) -> Option<OwnedRwLockReadGuard<()>> {
        self.inner.clone().try_read_owned().ok()
    }

    pub async fn shared_lock(&self) -> OwnedRwLockReadGuard<()> {
        self.inner.clone().read_owned().await
    }

    pub async fn mutex_lock(&self) -> OwnedRwLockWriteGuard<()> {
        self.inner.clone().write_owned().await
    }

    pub fn extra_lock(&self) -> MutexGuard<'_, ()> {
        self.extra.lock().unwrap()
    }
}

impl KeyspaceManager {
    pub fn get_keyspace_lock(&self, keyspace_id: u32) -> KeyspaceLockHelper {
        self.keyspaces.get(&keyspace_id).unwrap().get_locks()
    }
}

/// `RandomHelper` helps to get a random keyspace.
///
/// Note that zipf distribution generate sample from 1 to n conforms the
/// probability mass function (see https://en.wikipedia.org/wiki/Zipf's_law).
///
/// We need to shuffle keyspaces in advance before choose randomly.
/// Otherwise the first keyspace will always have the maximum chance.
#[derive(Default)]
struct RandomHelper {
    shuffle_keyspace_ids: Vec<u32>,
    zipf: Option<zipf::ZipfDistribution>,
}

impl RandomHelper {
    pub fn add_keyspaces(&mut self, keyspace_ids: &[u32], need_shuffle: Option<&mut ThreadRng>) {
        for &keyspace_id in keyspace_ids {
            self.shuffle_keyspace_ids.push(keyspace_id);
        }
        if let Some(rng) = need_shuffle {
            self.shuffle_keyspace_ids.shuffle(rng);
        }

        // ZipfDistribution do not support to update `num_elements` after created.
        self.zipf =
            Some(zipf::ZipfDistribution::new(self.shuffle_keyspace_ids.len(), 1.03).unwrap());
    }

    pub fn zipf_choose(&self, rng: &mut ThreadRng) -> u32 {
        let zipf = self
            .zipf
            .unwrap_or_else(|| panic!("add keyspace before choose"));
        self.shuffle_keyspace_ids[zipf.sample(rng) - 1]
    }

    pub fn uniformly_choose(&self, rng: &mut ThreadRng) -> u32 {
        self.shuffle_keyspace_ids
            .iter()
            .choose(rng)
            .copied()
            .unwrap()
    }
}

#[derive(Clone, Debug)]
pub struct DestroyRangeTask {
    pub ts: u64,
    pub table_id: i64,
}

pub struct ClusterKeyspaceClient {
    pub inner: ClusterTxnClient,
    keyspace_manager: KeyspaceManager,
}

impl Deref for ClusterKeyspaceClient {
    type Target = ClusterTxnClient;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for ClusterKeyspaceClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl ClusterKeyspaceClient {
    pub fn new(inner: ClusterTxnClient, keyspace_manager: KeyspaceManager) -> Self {
        Self {
            inner,
            keyspace_manager,
        }
    }

    pub fn keyspace_manager(&self) -> &KeyspaceManager {
        &self.keyspace_manager
    }

    pub async fn keyspace_put_kv<F, G>(
        &self,
        keyspace_id: u32,
        table_id: i64,
        rng: Range<usize>,
        gen_user_key: F,
        gen_val: G,
    ) -> Result<()>
    where
        F: Fn(usize) -> Vec<u8>,
        G: Fn(usize) -> Vec<u8>,
    {
        let mut mutations = vec![];
        for i in rng {
            let mut m = Mutation::default();
            m.set_op(Op::Put);
            m.set_key(make_row_key(keyspace_id, table_id, &gen_user_key(i)));
            m.set_value(gen_val(i));
            mutations.push(m)
        }
        self.kv_mutate(mutations.clone(), Duration::from_secs(30))
            .await?;

        self.keyspace_manager
            .ref_stores()
            .keyspace_put_kv(keyspace_id, mutations);
        Ok(())
    }

    pub async fn keyspace_del_kv<F>(
        &self,
        keyspace_id: u32,
        table_id: i64,
        rng: Range<usize>,
        gen_user_key: F,
    ) -> Result<()>
    where
        F: Fn(usize) -> Vec<u8>,
    {
        let mut mutations = vec![];
        for i in rng {
            let mut m = Mutation::default();
            m.set_op(Op::Del);
            m.set_key(make_row_key(keyspace_id, table_id, &gen_user_key(i)));
            mutations.push(m)
        }
        self.kv_mutate(mutations.clone(), Duration::from_secs(30))
            .await?;

        self.keyspace_manager
            .ref_stores()
            .keyspace_del_kv(keyspace_id, mutations);
        Ok(())
    }

    pub async fn verify_keyspace(&mut self, keyspace_id: u32) -> Result<usize> {
        let ref_store = self
            .keyspace_manager
            .ref_stores()
            .get_keyspace_ref_store(keyspace_id);
        let ref_store = ref_store.lock().unwrap().clone();
        let (start_key, end_key) = ApiV2::get_txn_keyspace_range(keyspace_id);
        self.verify_data_by_scan(&ref_store, Some((&start_key, &end_key)))
            .await
    }

    pub async fn verify_keyspace_and_skip_destroyed_ranges(
        &mut self,
        keyspace_id: u32,
    ) -> Result<usize> {
        let ref_store = self
            .keyspace_manager
            .ref_stores()
            .get_keyspace_ref_store(keyspace_id);
        let ref_store = ref_store.lock().unwrap().clone();

        let ranges = self.keyspace_manager.get_complementary_ranges(keyspace_id);
        info!(
            "verify_keyspace_and_skip_destroyed_ranges, complementary_ranges: {:?}",
            ranges
        );
        let mut cnt = 0;
        for range in ranges {
            cnt += self
                .verify_data_by_scan(&ref_store, Some((&range.0, &range.1)))
                .await?;
        }
        Ok(cnt)
    }

    pub async fn verify_all_keyspaces(&mut self) -> Result<usize> {
        let mut cnt = 0;
        for keyspace_id in self.keyspace_manager.ref_stores().all_keyspace_ids() {
            cnt += self.verify_keyspace(keyspace_id).await?;
        }
        Ok(cnt)
    }

    pub async fn drop_table(&mut self, keyspace_id: u32, table_id: i64) -> Result<()> {
        let lock = self.keyspace_manager.get_keyspace_lock(keyspace_id);
        let _guard = lock.mutex_lock().await;

        // Set unavailable to block writes to this table.
        // Place in block to avoid dead lock. `keyspace` & `table` hold the lock of
        // `DashMap`.
        {
            let keyspace = self
                .keyspace_manager
                .get_keyspace_meta(keyspace_id)
                .unwrap();
            let table = keyspace.get_table(table_id);
            if table.is_none() {
                // Should cause by restore.
                info!("table not exists, skip drop table"; "keyspace" => keyspace_id, "table" => table_id);
                return Ok(());
            }
            let previous_available = table.unwrap().set_available(false);
            if !previous_available {
                // Should be newly created and loading data.
                info!("table unavailable, skip drop table"; "keyspace" => keyspace_id, "table" => table_id);
                return Ok(());
            }
        }

        let ts = self.inner.current_timestamp().await.unwrap().version();
        self.keyspace_manager.drop_table(keyspace_id, table_id, ts);
        Ok(())
    }
}

// Ref: tidb_query_datatype::codec::table::{append_table_record_prefix,
// append_table_index_prefix}
const TABLE_PREFIX: &[u8] = b"t";
const RECORD_PREFIX_SEP: &[u8] = b"_r";
const INDEX_PREFIX_SEP: &[u8] = b"_i";

pub fn make_table_key(keyspace_id: u32, table_id: i64, sep: &[u8], user_key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(
        4 + TABLE_PREFIX.len() + mem::size_of_val(&table_id) + sep.len() + user_key.len(),
    );
    buf.extend_from_slice(&ApiV2::get_txn_keyspace_prefix(keyspace_id));
    buf.put_slice(TABLE_PREFIX);
    buf.write_i64(table_id).unwrap();
    if !user_key.is_empty() {
        buf.put_slice(sep);
        buf.put_slice(user_key);
    }
    buf
}

#[inline]
pub fn make_row_key(keyspace_id: u32, table_id: i64, user_key: &[u8]) -> Vec<u8> {
    make_table_key(keyspace_id, table_id, RECORD_PREFIX_SEP, user_key)
}

#[inline]
pub fn make_index_key(keyspace_id: u32, table_id: i64, user_key: &[u8]) -> Vec<u8> {
    make_table_key(keyspace_id, table_id, INDEX_PREFIX_SEP, user_key)
}

#[derive(Clone)]
pub struct KeyspaceBackup {
    pub backup_name: Option<String>,
    pub backup_ts: u64,
    pub keyspace_id: u32,
    pub ref_store: RefStore,
    pub meta: KeyspaceMetaCore,
}

impl KeyspaceBackup {
    pub fn backup_name(&self) -> &str {
        self.backup_name.as_ref().unwrap()
    }
}

#[derive(Default)]
pub struct KeyspaceRefStores {
    ref_stores: DashMap<u32 /* keyspace_id */, Arc<Mutex<RefStore>>>,
}

impl KeyspaceRefStores {
    pub fn get_keyspace_ref_store(&self, keyspace_id: u32) -> Arc<Mutex<RefStore>> {
        match self.ref_stores.entry(keyspace_id) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let ref_store = Arc::new(Mutex::new(RefStore::default()));
                e.insert(ref_store.clone());
                ref_store
            }
        }
    }

    pub fn dump(&self, keyspace_id: u32) -> RefStore {
        self.get_keyspace_ref_store(keyspace_id)
            .lock()
            .unwrap()
            .clone()
    }

    pub fn restore_keyspace(
        &self,
        tag: &str,
        mut ref_store: RefStore,
        keyspace_id: u32,
        target_keyspace_id: u32,
    ) {
        if ref_store.is_empty() {
            info!("{} ref store is empty in backup", tag);
        }

        if keyspace_id != target_keyspace_id {
            // Convert the keyspace prefix in ref store to the target keyspace.
            ref_store.rewrite_keyspace_prefix(target_keyspace_id);
        }

        let target_ref_store = self.get_keyspace_ref_store(target_keyspace_id);
        let mut target_ref_store = target_ref_store.lock().unwrap();
        *target_ref_store = ref_store;
    }

    pub fn keyspace_put_kv(&self, keyspace_id: u32, mutations: Vec<Mutation>) {
        let ref_store = self.get_keyspace_ref_store(keyspace_id);
        let mut ref_store = ref_store.lock().unwrap();
        for mut m in mutations {
            ref_store.put_kv(m.take_key(), m.take_value());
        }
    }

    pub fn keyspace_del_kv(&self, keyspace_id: u32, mutations: Vec<Mutation>) {
        let ref_store = self.get_keyspace_ref_store(keyspace_id);
        let mut ref_store = ref_store.lock().unwrap();
        for mut m in mutations {
            ref_store.del_kv(m.take_key());
        }
    }

    pub fn all_keyspace_ids(&self) -> Vec<u32> {
        self.ref_stores.iter().map(|r| *r.key()).collect()
    }

    pub fn ingest(&self, keyspace_id: u32, ref_store: RefStore) {
        let target_ref_store = self.get_keyspace_ref_store(keyspace_id);
        let mut target_ref_store = target_ref_store.lock().unwrap();
        target_ref_store.ingest(ref_store);
    }

    pub fn destroy_range(&self, keyspace_id: u32, start: &[u8], end: &[u8]) {
        let ref_store = self.get_keyspace_ref_store(keyspace_id);
        ref_store.lock().unwrap().destroy_range(start, end);
    }
}
