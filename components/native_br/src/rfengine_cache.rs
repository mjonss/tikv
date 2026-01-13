// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, hash_map::Entry},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};

use engine_traits::ObjectCache;
use kvengine::dfs::Dfs;
use pd_client::PdClient;
use rfengine::{RfEngine, RfEngineConfig};
use rfenginepb::{ClusterBackupMeta, StoreBackupMeta};
use tikv::config::TikvConfig;
use tikv_util::{config::ReadableSize, info};

// The minimum GC life time is actually 10 minutes, we reserve 2 minute in case
// backup takes too long.
const CONSERVATIVE_MIN_GC_DURATION: Duration = Duration::from_secs(8 * 60);

use crate::{
    archive::get_cluster_backup_file_and_meta,
    error::Error,
    metrics::{NATIVE_BR_RFENGINE_CACHE_HIT, NATIVE_BR_RFENGINE_CACHE_MISS},
    restore::RestoreConfig,
    restore_keyspace::BackupCluster,
};

#[derive(Clone)]
pub struct RfEngineCache {
    core: Arc<RwLock<RfEngineCacheCore>>,
    keyspace_tasks: Arc<RwLock<HashMap<u32, u64>>>,
}

impl RfEngineCache {
    pub fn new(
        path: PathBuf,
        config: RestoreConfig,
        dfs: Arc<dyn Dfs>,
        pd_client: Arc<dyn PdClient>,
    ) -> Self {
        Self {
            core: Arc::new(RwLock::new(RfEngineCacheCore {
                path,
                config,
                dfs,
                pd_client,
                backup_ts: 0,
                conservative_safe_ts: 0,
                engines: HashMap::new(),
                keyspace_ids: Vec::new(),
            })),
            keyspace_tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

struct RfEngineCacheCore {
    path: PathBuf,
    config: RestoreConfig,
    dfs: Arc<dyn Dfs>,
    pd_client: Arc<dyn PdClient>,
    backup_ts: u64,
    conservative_safe_ts: u64,
    engines: HashMap<u64, RfEngine>,
    keyspace_ids: Vec<u32>,
}

impl RfEngineCache {
    pub fn refill(&self) {}
}

impl RfEngineCache {
    pub fn get_for_keyspace(
        &self,
        store_id: u64,
        keyspace_id: u32,
        truncate_ts: u64,
        dir: &Path,
    ) -> Option<RfEngine> {
        let core = self.core.read().unwrap();
        if truncate_ts > core.backup_ts || truncate_ts < core.conservative_safe_ts {
            info!(
                "RfengineCache miss for keyspace {}, truncate_ts {} not in range [{}, {}]",
                keyspace_id, truncate_ts, core.conservative_safe_ts, core.backup_ts,
            );
            NATIVE_BR_RFENGINE_CACHE_MISS.inc();
            return None;
        }
        if !core.keyspace_ids.contains(&keyspace_id) {
            info!(
                "RfengineCache miss for keyspace {}, not registered",
                keyspace_id,
            );
            NATIVE_BR_RFENGINE_CACHE_MISS.inc();
            return None;
        }
        let cached = match core.engines.get(&store_id).cloned() {
            Some(cached) => cached,
            None => {
                info!(
                    "RfengineCache miss for keyspace {}, store {} not found",
                    keyspace_id, store_id,
                );
                NATIVE_BR_RFENGINE_CACHE_MISS.inc();
                return None;
            }
        };
        let mut cfg = RfEngineConfig::default();
        // As we may need to truncate some peers during recover, so write maybe called.
        // Use a small value to reduce disk usage.
        cfg.target_file_size = ReadableSize::mb(8);
        cfg.cli_mode = true;
        cfg.disable_compaction = true;
        let cloned = cached.clone_for_keyspace(keyspace_id, dir, &cfg).ok();
        if cloned.is_some() {
            info!(
                "RfengineCache hit for keyspace {} on store {}",
                keyspace_id, store_id,
            );
            NATIVE_BR_RFENGINE_CACHE_HIT.inc();
        } else {
            NATIVE_BR_RFENGINE_CACHE_MISS.inc();
        }
        cloned
    }

    pub fn register_keyspace(&self, keyspace_id: u32, truncate_ts: u64) {
        let mut keyspace_tasks = self.keyspace_tasks.write().unwrap();
        match keyspace_tasks.entry(keyspace_id) {
            Entry::Vacant(e) => {
                e.insert(truncate_ts);
            }
            Entry::Occupied(mut e) => {
                let existing_ts = e.get();
                if truncate_ts > *existing_ts {
                    e.insert(truncate_ts);
                }
            }
        }
    }

    fn collect_valid_keyspaces(&self, backup_meta: &ClusterBackupMeta) -> Vec<u32> {
        let mut valid_keyspaces = Vec::new();
        let mut keyspace_tasks = self.keyspace_tasks.write().unwrap();
        for (keyspace_id, truncate_ts) in keyspace_tasks.drain() {
            let conservative_safe_ts = get_conservative_safe_ts(backup_meta.backup_ts);
            if conservative_safe_ts <= truncate_ts && truncate_ts <= backup_meta.backup_ts {
                valid_keyspaces.push(keyspace_id);
            }
        }
        valid_keyspaces
    }

    pub fn fill_cache(
        &self,
        backup_name: &str,
        backup_ts: u64,
        object_cache: Option<ObjectCache>,
    ) -> crate::Result<()> {
        let mut core = self.core.write().unwrap();
        if core.backup_ts >= backup_ts {
            info!(
                "RfengineCache already filled for backup {}, ts {}",
                backup_name, backup_ts
            );
            return Ok(());
        }
        let Ok((_, backup_meta)) =
            get_cluster_backup_file_and_meta(core.dfs.as_ref(), backup_name.to_string())
        else {
            return Err(Error::BackupError("failed to get backup meta".to_string()));
        };
        let keyspace_ids: Vec<u32> = self.collect_valid_keyspaces(&backup_meta);
        // clear previous cache.
        // TODO: support incremental update rfengine cache.
        for (_, engine) in core.engines.drain() {
            let _ = fs::remove_dir_all(engine.dir.as_path());
        }
        for store_meta in backup_meta.get_stores() {
            let store_keyspace_ids = self.get_store_keyspace_ids(store_meta, &keyspace_ids);
            if store_keyspace_ids.is_empty() {
                continue;
            }
            let store_id = store_meta.get_store_id();
            let tag = format!("rfengine_cache:{}", store_id);
            let (store_path, rf_engine_path) = (
                core.path.join(store_id.to_string()),
                core.path.join(store_id.to_string()).join("raft"),
            );
            let mut config = TikvConfig::default();
            config.storage.data_dir = store_path.to_str().unwrap().to_string();
            config.raft_store.raftdb_path = rf_engine_path.to_str().unwrap().to_string();
            config.rfengine.disable_compaction = true;
            config.rfengine.cli_mode = true;
            let rfengine = BackupCluster::setup_raft_engine_for_lightweight(
                &tag,
                store_meta.get_store_id(),
                store_keyspace_ids,
                &backup_meta,
                &config,
                core.pd_client.clone(),
                core.dfs.clone(),
                false,
                None,
                &core.config,
                object_cache.clone(),
            )?;
            core.engines.insert(store_id, rfengine);
        }
        core.backup_ts = backup_meta.get_backup_ts();
        core.conservative_safe_ts =
            get_conservative_safe_ts(backup_meta.backup_ts).max(backup_meta.safe_ts);
        core.keyspace_ids = keyspace_ids;
        info!(
            "RfengineCache fill for keyspaces {:?}, ts {}",
            core.keyspace_ids, core.backup_ts
        );
        Ok(())
    }

    fn get_store_keyspace_ids(
        &self,
        store_meta: &StoreBackupMeta,
        all_keyspace_ids: &[u32],
    ) -> Vec<u32> {
        let mut store_keyspace_ids = Vec::new();
        for keyspace_id in all_keyspace_ids.iter() {
            if store_meta.keyspace_size.contains_key(keyspace_id) {
                store_keyspace_ids.push(*keyspace_id);
            }
        }
        store_keyspace_ids
    }
}

// The safe_ts from the ClusterBackupMeta is not reliable as we use keyspace
// level safe ts v2. So we use a more conservative value here.
fn get_conservative_safe_ts(backup_ts: u64) -> u64 {
    let backup_ts = txn_types::TimeStamp::new(backup_ts);
    let new_physical =
        backup_ts.physical() as i64 - CONSERVATIVE_MIN_GC_DURATION.as_millis() as i64;
    txn_types::TimeStamp::compose(new_physical as u64, 0).into_inner()
}
