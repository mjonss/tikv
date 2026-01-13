// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use api_version::{
    ApiV2,
    api_v2::{DEFAULT_KEYSPACE_ID, KEYSPACE_PREFIX_LEN, is_whole_keyspace_range},
};
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes};
use dashmap::DashMap;
use futures::future::join_all;
use http::Request;
use hyper::Body;
use kvengine::{
    IdAllocator, ShardStatsLite,
    dfs::{self, Dfs},
    table::{
        ChecksumType, NO_COMPRESSION,
        columnar::{
            ColumnarMetaCache, VectorIndexDef, new_common_handle_column_info,
            new_int_handle_column_info, new_version_column_info,
        },
        file::{File, LocalFile},
        schema_file,
        schema_file::{Schema, SchemaBufBuilder, SchemaFile},
    },
};
use kvproto::metapb::Store;
use native_br::common::send_request_to_store_with_retry;
use pd_client::PdClient;
use rfstore::store::PdIdAllocator;
use schema::schema::{
    ColumnInfo, IndexInfo, StorageClassSpec, TableInfo, VectorIndexInfo,
    convert_column_infos_to_tipb,
};
use security::{SecurityConfig, SecurityManager};
use tidb_query_datatype::VECTOR_INDEX_SPEC_KEY_DISTANCE_METRIC;
use tikv::storage::mvcc::TimeStamp;
use tikv_client::{BoundRange, Key, KvPair, TimestampExt, TransactionOptions, Value};
use tikv_util::{box_err, config::ReadableDuration, debug, error, info, warn};
use tokio::sync::Semaphore;

use crate::{
    error::{Error, Error::SchemaError, Result},
    get_all_stores_except_tiflash,
    metrics::{SCHEMA_MANAGER_SYNC_LOOP_COUNT, SCHEMA_MANAGER_SYNC_LOOP_ERROR_COUNT},
    server::Context,
};

const DEFAULT_TIMEOUT: ReadableDuration = ReadableDuration::secs(5);
const DEFAULT_GRPC_MAX_DECODING_MESSAGE_SIZE: usize = 128 * 1024 * 1024; // 128MB
const SCAN_BATCH_SIZE: u32 = 1024;
const KEYSPACE_REFRESH_INTERVAL: ReadableDuration = ReadableDuration::secs(30);
const DEFAULT_SCHEMA_UPLOAD_CONCURRENCY: usize = 16;

const META_FILE_MAGIC: u32 = 0x5E9EDFF4;
const META_FILE_FORMAT_VER: u16 = 1;
const META_FILE_FORMAT_VER_V2: u16 = 2;
const META_FILE_NAME: &str = "schemas.meta";

const TIKV_STORE_LABEL_TIER_KEY: &str = "serverless.tidbcloud.com/tier";

// Default number of historical schema file versions to keep per keyspace
const DEFAULT_SCHEMA_FILE_HISTORY_VERSIONS: usize = 3;
// GC interval for schema files (once per day)
const SCHEMA_FILE_GC_INTERVAL: ReadableDuration = ReadableDuration::hours(24);

#[derive(Clone, Default)]
pub struct ApiV2NoPrefixCodec {}

impl tikv_client::codec::Codec for ApiV2NoPrefixCodec {
    fn encode_request<R: tikv_client::request::KvRequest>(&self, req: &mut R) {
        req.set_api_version(tikv_client::proto::kvrpcpb::ApiVersion::V2);
    }
}

type TxnClient = tikv_client::TransactionClient<ApiV2NoPrefixCodec>;

#[derive(Clone)]
struct MetaFile {
    core: Arc<MetaFileCore>,
}

struct MetaFileCore {
    files: DashMap<u32 /* keyspace_id */, Vec<(u64 /* file_id */, i64 /* schema_version */)>>,
    checked_versions: DashMap<u32 /* keyspace_id */, i64 /* schema_version */>,
    write_sequences: DashMap<u32 /* keyspace_id */, u64 /* write_sequence */>,
}

#[derive(Clone, Copy, Debug)]
struct MetaFileFooter {
    pub checksum: u32,
    pub checksum_type: u8,
    pub compression_type: u8,
    pub format_version: u16,
    pub magic: u32,
}

impl MetaFileFooter {
    fn new() -> Self {
        MetaFileFooter {
            checksum: 0,
            checksum_type: ChecksumType::Crc32.value(),
            compression_type: NO_COMPRESSION,
            format_version: META_FILE_FORMAT_VER_V2,
            magic: META_FILE_MAGIC,
        }
    }

    fn parse(mut buf: &[u8]) -> Self {
        MetaFileFooter {
            checksum: buf.get_u32_le(),
            checksum_type: buf.get_u8(),
            compression_type: buf.get_u8(),
            format_version: buf.get_u16_le(),
            magic: buf.get_u32_le(),
        }
    }

    fn is_valid_version(&self) -> bool {
        matches!(
            self.format_version,
            META_FILE_FORMAT_VER_V2 | META_FILE_FORMAT_VER
        )
    }

    fn write_to(&self, data: &mut Vec<u8>) {
        data.put_u32_le(self.checksum);
        data.put_u8(self.checksum_type);
        data.put_u8(self.compression_type);
        data.put_u16_le(self.format_version);
        data.put_u32_le(self.magic);
    }
}

impl MetaFile {
    fn new() -> Self {
        let core = MetaFileCore {
            files: DashMap::default(),
            checked_versions: DashMap::default(),
            write_sequences: DashMap::default(),
        };
        Self {
            core: Arc::new(core),
        }
    }

    fn open(file: LocalFile) -> Result<Self> {
        let file_data = file.read(0, file.size() as usize)?;
        let footer_size = std::mem::size_of::<MetaFileFooter>();
        if file_data.len() < footer_size {
            return Err(crate::error::Error::FileCorrupted);
        }
        let footer_offset = file_data.len() - footer_size;
        let footer = MetaFileFooter::parse(&file_data[footer_offset..]);
        let mut data = &file_data[..footer_offset];
        if footer.magic != META_FILE_MAGIC {
            return Err(crate::error::Error::FileCorrupted);
        }
        if !footer.is_valid_version() {
            return Err(crate::error::Error::CheckError(
                "invalid meta file version".to_string(),
            ));
        }
        let checksum_type = ChecksumType::from(footer.checksum_type);
        let got_checksum = checksum_type.checksum(data);
        if got_checksum != footer.checksum {
            return Err(crate::error::Error::FileCorrupted);
        }
        let keyspace_count = data.get_u64_le();
        let files = DashMap::with_capacity(keyspace_count as usize);
        for _ in 0..keyspace_count {
            let keyspace_id = data.get_u32_le();
            let file_count = data.get_u64_le();
            let mut keyspace_files = Vec::with_capacity(file_count as usize);
            for _ in 0..file_count {
                let file_id = data.get_u64_le();
                let schema_version = data.get_i64_le();
                keyspace_files.push((file_id, schema_version));
            }
            files.insert(keyspace_id, keyspace_files);
        }
        let checked_count = data.get_u64_le();
        let checked_versions = DashMap::with_capacity(checked_count as usize);
        for _ in 0..checked_count {
            let keyspace_id = data.get_u32_le();
            let version = data.get_i64_le();
            checked_versions.insert(keyspace_id, version);
        }
        let write_sequences = if footer.format_version == META_FILE_FORMAT_VER_V2 {
            let seq_count = data.get_u64_le();
            let write_sequences = DashMap::with_capacity(seq_count as usize);
            for _ in 0..seq_count {
                let keyspace_id = data.get_u32_le();
                let seq = data.get_u64_le();
                write_sequences.insert(keyspace_id, seq);
            }
            write_sequences
        } else {
            DashMap::default()
        };
        let core = MetaFileCore {
            files,
            checked_versions,
            write_sequences,
        };
        Ok(MetaFile {
            core: Arc::new(core),
        })
    }

    fn write(&self) -> Vec<u8> {
        let mut data = Vec::new();
        let keyspace_count = self.core.files.len();
        data.put_u64_le(keyspace_count as u64);
        for kv in self.core.files.iter() {
            let keyspace_id = kv.key();
            let v = kv.value();
            data.put_u32_le(*keyspace_id);
            data.put_u64_le(v.len() as u64);
            for (file_id, schema_version) in v {
                data.put_u64_le(*file_id);
                data.put_i64_le(*schema_version);
            }
        }
        let checked_count = self.core.checked_versions.len();
        data.put_u64_le(checked_count as u64);
        for kv in self.core.checked_versions.iter() {
            let keyspace_id = kv.key();
            let ver = kv.value();
            data.put_u32_le(*keyspace_id);
            data.put_i64_le(*ver);
        }
        let write_sequence_count = self.core.write_sequences.len();
        data.put_u64_le(write_sequence_count as u64);
        for kv in self.core.write_sequences.iter() {
            let keyspace_id = kv.key();
            let seq = kv.value();
            data.put_u32_le(*keyspace_id);
            data.put_u64_le(*seq);
        }
        let mut footer = MetaFileFooter::new();
        let checksum_type = ChecksumType::Crc32;
        footer.checksum = checksum_type.checksum(&data);
        footer.write_to(&mut data);
        data
    }

    fn add_file(&self, keyspace_id: u32, file_id: u64, schema_version: i64) -> Result<()> {
        if let Some((_, version)) = self.get_latest_file(keyspace_id) {
            if version > schema_version {
                return Err(SchemaError(format!(
                    "new schema version {} is older than existing version {} for keyspace {}",
                    schema_version, version, keyspace_id
                )));
            }
        }

        self.core
            .files
            .entry(keyspace_id)
            .and_modify(|v| v.push((file_id, schema_version)))
            .or_insert(vec![(file_id, schema_version)]);
        Ok(())
    }

    fn add_default_file(&self, keyspace_id: u32, schema_version: i64) -> Result<()> {
        self.add_file(keyspace_id, 0, schema_version)
    }

    fn add_checked_version(&self, keyspace_id: u32, version: i64) {
        self.core.checked_versions.insert(keyspace_id, version);
    }

    fn get_checked_version(&self, keyspace_id: u32) -> Option<i64> {
        self.core
            .checked_versions
            .get(&keyspace_id)
            .as_deref()
            .cloned()
    }

    fn add_write_sequence(&self, keyspace_id: u32, seq: u64) {
        self.core.write_sequences.insert(keyspace_id, seq);
    }

    fn get_write_sequence(&self, keyspace_id: u32) -> Option<u64> {
        self.core
            .write_sequences
            .get(&keyspace_id)
            .as_deref()
            .cloned()
    }

    #[allow(dead_code)]
    fn get_files(&self, keyspace_id: u32) -> Option<Vec<(u64, i64)>> {
        self.core.files.get(&keyspace_id).as_deref().cloned()
    }

    fn get_latest_file(&self, keyspace_id: u32) -> Option<(u64, i64)> {
        self.core
            .files
            .get(&keyspace_id)
            .map(|m| m.last().cloned().unwrap())
    }

    fn remove_keyspace(&self, keyspace_id: u32) -> Option<(u32, Vec<(u64, i64)>)> {
        // The schema file will be removed by gc.
        self.core.checked_versions.remove(&keyspace_id);
        self.core.write_sequences.remove(&keyspace_id);
        self.core.files.remove(&keyspace_id)
    }

    /// GC old schema file versions for a specific keyspace.
    /// Returns the list of file_ids that should be removed from disk.
    /// keep_versions: number of recent versions to keep (0 means keep all)
    fn gc_old_files(&self, keyspace_id: u32, keep_versions: usize) -> Vec<u64> {
        if keep_versions == 0 {
            return vec![];
        }

        let mut to_remove = vec![];
        self.core.files.alter(&keyspace_id, |_, mut files| {
            if files.len() > keep_versions {
                // Split off the files to remove (older versions)
                let remove_count = files.len() - keep_versions;
                let removed: Vec<(u64, i64)> = files.drain(..remove_count).collect();
                to_remove = removed
                    .into_iter()
                    .map(|(file_id, _)| file_id)
                    .filter(|&id| id > 0) // Skip default files (file_id = 0)
                    .collect();
            }
            files
        });
        to_remove
    }

    /// GC old schema file versions for all keyspaces.
    /// Returns a map of keyspace_id -> file_ids to remove.
    fn gc_all_old_files(&self, keep_versions: usize) -> HashMap<u32, Vec<u64>> {
        if keep_versions == 0 {
            return HashMap::new();
        }

        // First, collect all keyspace_ids to avoid holding the iterator
        // while calling gc_old_files (which would cause deadlock)
        let keyspace_ids: Vec<u32> = self.core.files.iter().map(|entry| *entry.key()).collect();

        // Then process each keyspace
        let mut result = HashMap::new();
        for keyspace_id in keyspace_ids {
            let to_remove = self.gc_old_files(keyspace_id, keep_versions);
            if !to_remove.is_empty() {
                result.insert(keyspace_id, to_remove);
            }
        }
        result
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct SchemaManagerConfig {
    pub dir: PathBuf,
    pub keyspace_refresh_interval: ReadableDuration,
    pub http_timeout: ReadableDuration,
    pub scan_batch_size: u32,
    pub enabled: bool,
    // `blacklist_file` is a json file contains a list of keyspace_id.
    pub blacklist_file: PathBuf,
    // The tier of TiKV stores to push schema file. Used for canary release.
    pub tikv_stores_tier: String,
    // Maximum concurrent schema upload tasks
    pub schema_upload_concurrency: usize,
    // Number of historical schema file versions to keep per keyspace (0 means keep all)
    pub schema_file_history_versions: usize,
}

impl Default for SchemaManagerConfig {
    fn default() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap().into_path(),
            keyspace_refresh_interval: KEYSPACE_REFRESH_INTERVAL,
            http_timeout: DEFAULT_TIMEOUT,
            scan_batch_size: SCAN_BATCH_SIZE,
            enabled: false,
            blacklist_file: PathBuf::new(), // Empty means no blacklist filtering.
            tikv_stores_tier: "".to_string(), // Empty means match all stores.
            schema_upload_concurrency: DEFAULT_SCHEMA_UPLOAD_CONCURRENCY,
            schema_file_history_versions: DEFAULT_SCHEMA_FILE_HISTORY_VERSIONS,
        }
    }
}

pub struct SchemaMgrContext {
    pub dfs: Arc<dyn Dfs>,
    pub pd: Arc<dyn PdClient>,
    pub columnar_meta_cache: ColumnarMetaCache,
}

impl From<Arc<Context>> for SchemaMgrContext {
    fn from(ctx: Arc<Context>) -> Self {
        Self {
            dfs: ctx.dfs.clone(),
            pd: ctx.pd.clone(),
            columnar_meta_cache: ctx.columnar_meta_cache.clone(),
        }
    }
}

#[derive(Clone)]
pub struct SchemaManager {
    core: Arc<SchemaManagerCore>,
}

impl Deref for SchemaManager {
    type Target = SchemaManagerCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl SchemaManager {
    pub fn new(
        ctx: Arc<SchemaMgrContext>,
        security_mgr: Arc<SecurityManager>,
        security_config: SecurityConfig,
        config: SchemaManagerConfig,
        endpoints: &[String],
    ) -> Self {
        let runtime = ctx.dfs.get_runtime();
        let mut client_config = tikv_client::Config::default()
            .with_grpc_max_decoding_message_size(DEFAULT_GRPC_MAX_DECODING_MESSAGE_SIZE);
        if !security_config.ca_path.is_empty()
            || !security_config.cert_path.is_empty()
            || !security_config.key_path.is_empty()
        {
            let SecurityConfig {
                ca_path,
                cert_path,
                key_path,
                ..
            } = security_config;
            client_config = client_config.with_security(ca_path, cert_path, key_path);
        }
        let txn_client = runtime
            .block_on(tikv_client::TransactionClient::new_with_codec(
                endpoints.to_vec(),
                client_config,
                ApiV2NoPrefixCodec::default(),
            ))
            .unwrap();
        info!("SchemaManager started with config: {:?}", config);
        Self {
            core: Arc::new(SchemaManagerCore::new(
                ctx,
                security_mgr,
                config,
                txn_client,
            )),
        }
    }

    pub(crate) fn run(&self, runtime: Arc<tokio::runtime::Runtime>) {
        if let Err(e) = self.repair_meta_file_if_needed() {
            error!("repair meta file error: {:?}", e);
        }

        let self_clone = self.clone();
        runtime.spawn(async move {
            let mut last_gc_time = tokio::time::Instant::now();

            loop {
                // Get all keyspaces stats from store.
                let (stores, _) = self_clone.get_tikv_stores();
                if stores.is_empty() {
                    tokio::time::sleep(self_clone.config.keyspace_refresh_interval.0).await;
                    continue;
                }

                let mut keyspace_stats = HashMap::new();
                if let Err(e) = self_clone
                    .refresh_keyspace_stats(&mut keyspace_stats, &stores)
                    .await
                {
                    error!("refresh keyspace stats error: {:?}", e);
                    SCHEMA_MANAGER_SYNC_LOOP_ERROR_COUNT
                        .with_label_values(&["refresh_keyspace_stats"])
                        .inc();
                    tokio::time::sleep(self_clone.config.keyspace_refresh_interval.0).await;
                    continue;
                }
                debug!("schema manager: keyspace stats: {:?}", keyspace_stats);
                // Refresh keyspaces schema version.
                if let Err(e) = self_clone
                    .refresh_keyspace_schema(&keyspace_stats, &stores)
                    .await
                {
                    error!("refresh schema version error: {:?}", e);
                }
                SCHEMA_MANAGER_SYNC_LOOP_COUNT.inc();

                if last_gc_time.elapsed() >= SCHEMA_FILE_GC_INTERVAL.0 {
                    let gc_start_time = tokio::time::Instant::now();
                    info!("starting periodic schema file GC");
                    if let Err(e) = self_clone.gc_schema_files() {
                        warn!("gc_schema_files failed: {:?}", e);
                        SCHEMA_MANAGER_SYNC_LOOP_ERROR_COUNT
                            .with_label_values(&["gc_schema_files"])
                            .inc();
                    } else {
                        info!(
                            "periodic schema file GC completed in {:?}",
                            gc_start_time.elapsed()
                        );
                    }
                    last_gc_time = tokio::time::Instant::now();
                }

                tokio::time::sleep(self_clone.config.keyspace_refresh_interval.0).await;
            }
        });
    }

    pub async fn refresh_keyspace_stats(
        &self,
        keyspace_stats: &mut HashMap<u32, Vec<ShardStatsLite>>,
        stores: &[Store],
    ) -> Result<()> {
        let mut all_shard_stats = vec![];
        for store in stores.iter() {
            let shard_stats = get_keyspace_stats_from_store(
                store,
                self.security_mgr.clone(),
                self.config.http_timeout.0,
            )
            .await?;
            all_shard_stats.extend(shard_stats);
        }

        for shard_stats in all_shard_stats {
            Self::update_keyspace_stats(keyspace_stats, shard_stats)
        }
        Ok(())
    }

    fn update_keyspace_stats(
        keyspace_stats: &mut HashMap<u32, Vec<ShardStatsLite>>,
        shard_stats: ShardStatsLite,
    ) {
        let Some(keyspace_id) = ApiV2::get_u32_keyspace_id_by_key(&shard_stats.start) else {
            return;
        };
        let Some(end_keyspace_id) = ApiV2::get_u32_keyspace_id_by_key(&shard_stats.end) else {
            return;
        };
        if keyspace_id != end_keyspace_id
            && !is_whole_keyspace_range(&shard_stats.start[..KEYSPACE_PREFIX_LEN], &shard_stats.end)
        {
            return;
        }
        // Skip the empty keyspace or tombstone keyspace.
        if is_whole_keyspace_range(&shard_stats.start, &shard_stats.end)
            && shard_stats.total_size == 0
        {
            return;
        }
        keyspace_stats
            .entry(keyspace_id)
            .or_default()
            .push(shard_stats);
    }

    /// Validates if a keyspace should be processed for schema refresh
    /// Returns None if keyspace should be skipped, Some(write_sequence) if
    /// should continue
    fn validate_keyspace_for_refresh(
        &self,
        keyspace_id: u32,
        keyspace_shard_stats: &[ShardStatsLite],
    ) -> Option<Option<u64>> {
        // Skip the default keyspace. The tikv-client not support the default keyspace
        // with ApiV2NoPrefixCodec.
        if keyspace_id == DEFAULT_KEYSPACE_ID {
            return None;
        }

        if self.in_blacklist(keyspace_id) {
            // Remove the keyspace schema file and index from meta file.
            if let Ok(true) = self.remove_keyspace_local_file(keyspace_id) {
                info!(
                    "{}: keyspace is in blacklist, remove local schema files",
                    keyspace_id
                );
            }
            return None;
        }

        // Check write sequence for single-shard keyspaces
        let update_write_sequence = if keyspace_shard_stats.len() == 1 {
            let seq = self.meta_file.get_write_sequence(keyspace_id);
            let shard_stats = keyspace_shard_stats.first().unwrap();
            if seq.is_some_and(|seq| seq == shard_stats.write_sequence) {
                debug!(
                    "{}: write sequence is not changed, skip",
                    keyspace_id;
                    "seq" => ?seq, "shard_stats" => ?shard_stats
                );
                return None;
            }
            Some(shard_stats.write_sequence)
        } else {
            None
        };

        debug!(
            "{}: update write sequence: {}",
            keyspace_id,
            update_write_sequence.is_some()
        );

        // Check if keyspace has any data
        let keyspace_total_size = keyspace_shard_stats
            .iter()
            .map(|s| s.total_size)
            .sum::<u64>();
        if keyspace_total_size == 0 {
            debug!("{}: keyspace total size is 0, ignore keyspace", keyspace_id);
            return None;
        }

        // Check if keyspace restore is in progress
        if self.check_if_keyspace_restore_in_progress(keyspace_shard_stats) {
            info!("{}: keyspace restore in progress, skip", keyspace_id);
            return None;
        }

        Some(update_write_sequence)
    }

    /// Processes local schema file and determines current schema version
    /// Returns (local_schema_file, cur_schema_version, handle_next_keyspace)
    async fn process_local_schema_file(
        &self,
        keyspace_id: u32,
        keyspace_shard_stats: &[ShardStatsLite],
        stores: &[Store],
    ) -> Result<(Option<SchemaFile>, Option<i64>, bool)> {
        // Try to read schema file from local.
        let local_schema_file =
            match read_schema_file_from_local(&self.config.dir, &self.meta_file, keyspace_id) {
                Ok(Some(local_schema_file)) => Some(local_schema_file),
                Ok(None) => None,
                Err(err) => {
                    warn!("read schema file from local failed: {:?}", err);
                    None
                }
            };

        // Check the remote schema_version in store shard_stats to ensure the state
        // applied to kvengine.
        let cur_schema_version = if let Some(schema_file) = &local_schema_file {
            let cur_schema_version = schema_file.get_version();
            let cur_restore_version = schema_file.get_restore_version();
            debug!(
                "{}: schema_file_id: {}, schema_version: {}, restore_version: {}",
                keyspace_id,
                schema_file.get_file_id(),
                schema_file.get_version(),
                cur_restore_version,
            );
            if self.check_if_keyspace_restored(
                keyspace_id,
                keyspace_shard_stats,
                cur_schema_version,
                cur_restore_version,
            ) {
                // If the keyspace is just restored, the schema_restore_version in shard will be
                // reset to backup_ts after restoration. In this case, we should remove the old
                // schema file and try to rebuild it in next loop.
                self.remove_keyspace_local_file(keyspace_id)?;
                info!(
                    "{}: keyspace is restored, remove local schema files",
                    keyspace_id
                );
                return Ok((None, None, true));
            }
            if self
                .check_store_schema_version(
                    keyspace_id,
                    cur_schema_version,
                    schema_file,
                    keyspace_shard_stats,
                    stores,
                )
                .await
            {
                return Ok((local_schema_file, Some(cur_schema_version), true));
            }
            Some(cur_schema_version)
        } else {
            get_stats_schema_version(keyspace_shard_stats)
        };

        Ok((local_schema_file, cur_schema_version, false))
    }

    /// Synchronizes schema from TiKV and handles up-to-date cases
    /// Returns (schema_version, table_infos, handle_next_keyspace)
    async fn sync_schema_and_check_updates(
        &self,
        keyspace_id: u32,
        local_schema_file: &Option<SchemaFile>,
        cur_schema_version: Option<i64>,
        update_write_sequence: Option<u64>,
    ) -> Result<(i64, Vec<TableInfo>, bool)> {
        // Check the latest schema version compare with cache, if any
        // update, fetch all the new schemas and update to S3.
        let checked_version = self.meta_file.get_checked_version(keyspace_id);
        if let (Some(checked), Some(cur)) = (checked_version, cur_schema_version) {
            debug_assert!(checked >= cur, "{} {}", checked, cur);
        }
        let checked_version = checked_version.or(cur_schema_version);
        let sync_from_version = if local_schema_file.is_some() {
            checked_version
        } else {
            // If local schema file not exists, trigger full schema sync.
            None
        };

        let kv_scanner = Arc::new(self.clone());
        let kv_getter = Arc::new(self.clone());

        let start_ts = self.ctx.pd.get_tso().await?;
        // Get schema version without sync schema diff. We can avoid scanning schema
        // diffs if the schema version not changed.
        let schema_version =
            match schema::get_schema_version(kv_getter.clone(), keyspace_id, start_ts).await {
                // If the schema version is 0, it means the keyspace has been unsafe destroyed or
                // created without bootstraped.
                Ok(0) => {
                    info!(
                        "{}: schema version is 0, skip, remove keyspace",
                        keyspace_id
                    );
                    self.remove_keyspace_local_file(keyspace_id)?;
                    return Ok((0, vec![], true));
                }
                Ok(schema_version) => schema_version,
                Err(err) => {
                    error!("{}: get schema version failed, skip", keyspace_id; "err" => ?err);
                    return Err(Error::Other(box_err!(
                        "get schema version failed: {:?}",
                        err
                    )));
                }
            };

        if checked_version.is_some_and(|v| v == schema_version) {
            debug!("{}: schema is up-to-date, skip", keyspace_id; "schema_ver" => schema_version,
                "cur_ver" => ?cur_schema_version, "checked_ver" => ?checked_version);
            if self.meta_file.get_checked_version(keyspace_id).is_none() {
                self.meta_file
                    .add_checked_version(keyspace_id, schema_version);
            }
            if let Some(write_sequence) = update_write_sequence {
                self.meta_file
                    .add_write_sequence(keyspace_id, write_sequence);
            }
            if local_schema_file.is_none() {
                let res = self.meta_file.add_default_file(keyspace_id, schema_version);
                if res.is_err() {
                    self.remove_keyspace_local_file(keyspace_id)?;
                }
            }
            return Ok((schema_version, vec![], true));
        }

        let (schema_version, table_infos) = match schema::sync_schema(
            kv_getter,
            kv_scanner,
            keyspace_id,
            sync_from_version,
            start_ts,
        )
        .await
        {
            Ok(result) => result,
            Err(err) => {
                error!("{}: sync schema failed, skip", keyspace_id; "err" => ?err);
                return Err(Error::Other(box_err!("sync schema failed: {:?}", err)));
            }
        };
        if schema_version == 0 {
            info!(
                "{}: schema version is 0, skip, remove keyspace",
                keyspace_id
            );
            self.remove_keyspace_local_file(keyspace_id)?;
            return Ok((0, vec![], true));
        }

        info!(
            "{}: sync schema: schema_version: {}, checked_version: {:?}, table_infos: {}",
            keyspace_id,
            schema_version,
            checked_version,
            table_infos.len()
        );
        debug!("{}: sync schema", keyspace_id; "schema_ver" => schema_version, "tables" => ?table_infos);

        Ok((schema_version, table_infos, false))
    }

    /// Checks if schema needs update and prepares schemas for building
    /// Returns (schemas, handle_next_keyspace)
    fn check_schema_update_requirements(
        &self,
        keyspace_id: u32,
        local_schema_file: &Option<SchemaFile>,
        table_infos: Vec<TableInfo>,
        schema_version: i64,
        checked_version: Option<i64>,
        update_write_sequence: Option<u64>,
    ) -> Result<(Option<Vec<Schema>>, bool)> {
        let old_storage_class_tables = local_schema_file
            .as_ref()
            .map(|schema_file| schema_file.tables_with_storage_class());
        // If the old specified storage class becomes unspecified, the storage class is
        // removed from the schema file.
        let sc_need_update_schema = table_infos.iter().any(|ti| {
            ti.with_storage_class_spec()
                || old_storage_class_tables
                    .as_ref()
                    .is_some_and(|tables| tables.contains(&ti.id))
        });
        let columnar_need_update_schema = table_infos.iter().any(|ti| {
            // local schema file not exist, need to update.
            if local_schema_file.is_none() {
                return true;
            }
            // If ti has columnar or schema has columnar, need to update.
            let local_schema = local_schema_file.as_ref().unwrap().get_table(ti.id);
            ti.with_columnar() || local_schema.map(|s| s.with_columnar()).unwrap_or_default()
        });

        if local_schema_file.is_some() && !sc_need_update_schema && !columnar_need_update_schema {
            debug!("{}: schema has no required changes, skip", keyspace_id;
                "schema_version" => schema_version, "tables" => ?table_infos, "old_sc_tables" => ?old_storage_class_tables);
            self.meta_file
                .add_checked_version(keyspace_id, schema_version);
            if let Some(write_sequence) = update_write_sequence {
                self.meta_file
                    .add_write_sequence(keyspace_id, write_sequence);
            }
            return Ok((None, true));
        }

        info!("{}: sync schema: rebuild schema", keyspace_id;
            "cur_schema_ver" => ?checked_version,
            "schema_ver" => schema_version,
            "table_infos" => table_infos.len());

        // Build the schema file and upload to S3.
        let schemas = match self.build_new_schema(local_schema_file.as_ref(), table_infos) {
            Ok(schema) => schema,
            Err(err) => {
                error!("{}: build new schema failed", keyspace_id; "err" => ?err);
                SCHEMA_MANAGER_SYNC_LOOP_ERROR_COUNT
                    .with_label_values(&["build_new_schema"])
                    .inc();
                return Err(err);
            }
        };
        debug!("{}: build new schema", keyspace_id; "schemas" => ?schemas, "cur_schema_ver" => ?checked_version, "schema_ver" => schema_version);

        if schemas.is_none() {
            self.meta_file
                .add_checked_version(keyspace_id, schema_version);
            if let Some(write_sequence) = update_write_sequence {
                self.meta_file
                    .add_write_sequence(keyspace_id, write_sequence);
            }
            return Ok((None, true));
        }

        // No tables need to build.
        if local_schema_file.is_none() && schemas.as_ref().unwrap().is_empty() {
            self.meta_file
                .add_checked_version(keyspace_id, schema_version);
            // Add default file to indicate the keyspace is already synced. There is no
            // valid schema file in local.
            let res = self.meta_file.add_default_file(keyspace_id, schema_version);
            if res.is_err() {
                self.remove_keyspace_local_file(keyspace_id)?;
            }
            if let Some(write_sequence) = update_write_sequence {
                self.meta_file
                    .add_write_sequence(keyspace_id, write_sequence);
            }
            return Ok((None, true));
        }

        Ok((schemas, false))
    }

    /// Spawns async task to build and upload schema file
    /// Returns the spawned task count increment
    fn spawn_schema_upload_task(
        &self,
        keyspace_id: u32,
        schema_version: i64,
        schemas: Vec<Schema>,
        keyspace_shard_stats: &[ShardStatsLite],
        stores: &[Store],
        tx: tikv_util::mpsc::Sender<Result<(u32, u64, i64)>>,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<u32> {
        let schema_restore_version = keyspace_shard_stats
            .first()
            .map(|s| s.schema_restore_version)
            .unwrap_or(0);

        // Build schema file with the schema restore version from shard stats.
        let new_schema_file_data = schema_file::build_schema_file(
            keyspace_id,
            schema_version,
            schemas,
            schema_restore_version,
        );
        let file_id = *self
            .id_allocator
            .alloc_id(1)
            .map_err(|e| Error::Other(box_err!(e.to_string())))?
            .first()
            .unwrap();

        let dfs = self.ctx.dfs.clone();
        let tx_clone = tx.clone();
        let self_clone = self.clone();
        let stores = stores.to_vec();
        let semaphore = self.schema_upload_semaphore.clone();

        runtime.spawn(async move {
            // Acquire semaphore permit to limit concurrency.
            let _permit = semaphore.acquire().await.unwrap();

            let data = Bytes::from(new_schema_file_data.clone());
            let opts = dfs::Options::default().with_type(dfs::FileType::Schema);
            let res: Result<u64> = dfs
                .create(file_id, data.clone(), opts)
                .await
                .map(|()| file_id)
                .map_err(Into::into);
            if let Err(err) = res {
                error!("{}: failed to update schema file", keyspace_id;
                    "file_id" => file_id, "schema_ver" => schema_version, "err" => ?err);
                let _ = tx_clone
                    .send(Err(err))
                    .map_err(|e| warn!("{}: send failed: {:?}", keyspace_id, e));
                SCHEMA_MANAGER_SYNC_LOOP_ERROR_COUNT
                    .with_label_values(&["upload_schema_file"])
                    .inc();
                return;
            }
            if let Err(err) =
                write_schema_file_to_local(&self_clone.config.dir, keyspace_id, file_id, data)
            {
                error!("{}: failed to write schema file to local", keyspace_id;
                    "file_id" => file_id, "schema_ver" => schema_version, "err" => ?err);
                let _ = tx_clone
                    .send(Err(err))
                    .map_err(|e| warn!("{}: send failed: {:?}", keyspace_id, e));
                SCHEMA_MANAGER_SYNC_LOOP_ERROR_COUNT
                    .with_label_values(&["write_schema_file_to_local"])
                    .inc();
                return;
            }

            // Callback TiKV to update the new schema file to shard meta.
            info!("{}: broadcast schema update to stores", keyspace_id;
                "file_id" => file_id, "schema_ver" => schema_version);
            broadcast_schema_update_to_all_stores(
                &stores,
                self_clone.security_mgr.clone(),
                self_clone.config.http_timeout.0,
                keyspace_id,
                file_id,
            )
            .await;
            let _ = tx_clone
                .send(Ok((keyspace_id, file_id, schema_version)))
                .map_err(|err| {
                    // Should happen only when `refresh_keyspace_schema` is aborted.
                    warn!("{} refresh keyspace schema: send failed: {:?}", keyspace_id, err;
                        "file_id" => file_id, "schema_ver" => schema_version);
                });
        });

        Ok(1)
    }

    /// Handles completion of all spawned tasks and saves meta file
    fn handle_task_completion(
        &self,
        rx: tikv_util::mpsc::Receiver<Result<(u32, u64, i64)>>,
        spawn_task_count: u32,
    ) -> Result<()> {
        for _ in 0..spawn_task_count {
            let Ok((keyspace_id, file_id, schema_version)) = rx.recv().unwrap() else {
                continue;
            };
            let res = self
                .meta_file
                .add_file(keyspace_id, file_id, schema_version);
            if res.is_err() {
                warn!(
                    "{}: schema version step back detected, {} -> {}, clear keyspace schema files and retry",
                    keyspace_id,
                    self.meta_file
                        .get_latest_file(keyspace_id)
                        .map(|(_, v)| v)
                        .unwrap_or_default(),
                    schema_version
                );
                SCHEMA_MANAGER_SYNC_LOOP_ERROR_COUNT
                    .with_label_values(&["schema_version_step_back"])
                    .inc();
                self.remove_keyspace_local_file(keyspace_id)?;
                continue;
            }
            self.meta_file
                .add_checked_version(keyspace_id, schema_version);
            // Note: we don't update the write sequence here in case of
            // broadcast some stores failure. We can retry check the store
            // status to broadcast again for small keyspaces. The write sequence
            // will be updated if schema up to date.
        }
        // Save meta_file.
        let meta = self.meta_file.write();
        write_meta_file_to_local(&self.config.dir, Bytes::from(meta))
            .map_err(|err| -> Error { box_err!("write_meta_file_to_local failed: {:?}", err) })?;
        Ok(())
    }

    /// GC old schema file versions for all keyspaces.
    /// This should be called periodically to avoid accumulating too many
    /// historical versions.
    fn gc_schema_files(&self) -> Result<()> {
        let keep_versions = self.config.schema_file_history_versions;
        if keep_versions == 0 {
            // 0 means keep all versions, no GC needed
            return Ok(());
        }

        let to_remove_map = self.meta_file.gc_all_old_files(keep_versions);
        if to_remove_map.is_empty() {
            return Ok(());
        }

        let mut total_removed = 0;
        for (keyspace_id, file_ids) in to_remove_map {
            match remove_schema_file_from_local(&self.config.dir, keyspace_id, &file_ids) {
                Ok(()) => {
                    total_removed += file_ids.len();
                    info!(
                        "{}: GC removed {} old schema file versions",
                        keyspace_id,
                        file_ids.len();
                        "file_ids" => ?file_ids
                    );
                }
                Err(err) => {
                    warn!(
                        "{}: failed to remove old schema files during GC",
                        keyspace_id;
                        "err" => ?err, "file_ids" => ?file_ids
                    );
                }
            }
        }

        if total_removed > 0 {
            // Save updated meta_file after GC
            let meta = self.meta_file.write();
            write_meta_file_to_local(&self.config.dir, Bytes::from(meta)).map_err(
                |err| -> Error { box_err!("write_meta_file_to_local failed after GC: {:?}", err) },
            )?;
            info!(
                "GC completed: removed {} schema file versions in total",
                total_removed
            );
        }

        Ok(())
    }

    pub(crate) async fn refresh_keyspace_schema(
        &self,
        keyspace_stats: &HashMap<u32, Vec<ShardStatsLite>>,
        stores: &[Store],
    ) -> Result<()> {
        let (tx, rx) = tikv_util::mpsc::unbounded::<Result<(u32, u64, i64)>>();
        let runtime = self.ctx.dfs.get_runtime();
        let mut spawn_task_count = 0;

        for (&keyspace_id, keyspace_shard_stats) in keyspace_stats {
            // 1. Validate keyspace for processing
            let update_write_sequence =
                match self.validate_keyspace_for_refresh(keyspace_id, keyspace_shard_stats) {
                    Some(write_seq) => write_seq,
                    None => continue, // Skip this keyspace
                };

            // 2. Process local schema file
            let (local_schema_file, cur_schema_version, handle_next_keyspace) = match self
                .process_local_schema_file(keyspace_id, keyspace_shard_stats, stores)
                .await
            {
                Ok((local, cur, cont)) => (local, cur, cont),
                Err(err) => {
                    error!("{}: process local schema file failed", keyspace_id; "err" => ?err);
                    SCHEMA_MANAGER_SYNC_LOOP_ERROR_COUNT
                        .with_label_values(&["process_local_schema_file"])
                        .inc();
                    continue;
                }
            };
            if handle_next_keyspace {
                continue;
            }

            // 3. Sync schema and check for updates
            let (schema_version, table_infos, handle_next_keyspace) = match self
                .sync_schema_and_check_updates(
                    keyspace_id,
                    &local_schema_file,
                    cur_schema_version,
                    update_write_sequence,
                )
                .await
            {
                Ok((ver, tables, cont)) => (ver, tables, cont),
                Err(err) => {
                    error!("{}: sync schema and check updates failed", keyspace_id; "err" => ?err);
                    SCHEMA_MANAGER_SYNC_LOOP_ERROR_COUNT
                        .with_label_values(&["sync_schema_and_check_updates"])
                        .inc();
                    continue;
                }
            };
            if handle_next_keyspace {
                continue;
            }

            // 4. Check schema update requirements
            let checked_version = self
                .meta_file
                .get_checked_version(keyspace_id)
                .or(cur_schema_version);
            let (schemas, handle_next_keyspace) = match self.check_schema_update_requirements(
                keyspace_id,
                &local_schema_file,
                table_infos,
                schema_version,
                checked_version,
                update_write_sequence,
            ) {
                Ok((schemas, cont)) => (schemas, cont),
                Err(err) => {
                    error!("{}: check schema update requirements failed", keyspace_id; "err" => ?err);
                    // NOTE: metrics reported in `check_schema_update_requirements`.
                    continue;
                }
            };
            if handle_next_keyspace {
                continue;
            }

            // 5. Spawn schema upload task
            let schemas = schemas.unwrap();
            match self.spawn_schema_upload_task(
                keyspace_id,
                schema_version,
                schemas,
                keyspace_shard_stats,
                stores,
                tx.clone(),
                runtime,
            ) {
                Ok(task_count) => spawn_task_count += task_count,
                Err(err) => {
                    error!("{}: spawn schema upload task failed", keyspace_id; "err" => ?err);
                    // Note: metrics reported in `spawn_schema_upload_task`.
                    continue;
                }
            }
        }

        // 6. Handle task completion
        self.handle_task_completion(rx, spawn_task_count)?;
        Ok(())
    }

    // return true if the keyspace is just restored.
    fn check_if_keyspace_restored(
        &self,
        keyspace_id: u32,
        keyspace_shard_stats: &[ShardStatsLite],
        cur_schema_version: i64,
        cur_restore_version: u64,
    ) -> bool {
        let stats_schema_version = keyspace_shard_stats
            .iter()
            .map(|s| s.schema_version)
            .max()
            .unwrap_or_default();
        // NOTE: schema_restore_version is the same for all shards.
        let stats_restore_version = keyspace_shard_stats
            .first()
            .map(|s| s.schema_restore_version)
            .unwrap_or_default();
        // If the restored_version in local schema file is not the same as shard stats,
        // it means the keyspace is just restored.
        if stats_restore_version != cur_restore_version {
            info!("{}: keyspace restored", keyspace_id;
                "stats_restore_ver" => stats_restore_version,
                "cur_restore_ver" => cur_restore_version,
                "stats_schema_ver" => stats_schema_version,
                "cur_schema_ver" => cur_schema_version,
                "keyspace_shard_stats" => ?keyspace_shard_stats,
            );
            return true;
        }
        false
    }

    fn check_if_keyspace_restore_in_progress(
        &self,
        keyspace_shard_stats: &[ShardStatsLite],
    ) -> bool {
        let first_restore_version = keyspace_shard_stats
            .first()
            .map(|s| s.schema_restore_version)
            .unwrap_or(0);
        keyspace_shard_stats
            .iter()
            .any(|s| s.schema_restore_version != first_restore_version)
    }

    // return true if sent broadcast to stores
    async fn check_store_schema_version(
        &self,
        keyspace_id: u32,
        cur_schema_version: i64,
        schema_file: &SchemaFile,
        keyspace_shard_stats: &[ShardStatsLite],
        stores: &[Store],
    ) -> bool {
        let mut need_broadcast = false;
        for shard_stats in keyspace_shard_stats {
            if cur_schema_version <= shard_stats.schema_version {
                // Schema version fallback will happen in production env, e.g. re-deploy the
                // schema manager.
                debug_assert_eq!(
                    cur_schema_version, shard_stats.schema_version,
                    "schema version fallback: {:?}, cur: {}",
                    shard_stats, cur_schema_version,
                );
                continue;
            }

            // When shard is `with_schema` but not overlapped with schema file, it means
            // that the schema has changed to having no required changes (i.e.
            // storage class & columnar) for the shard, but the relevant data
            // has not been cleared.
            if shard_stats.with_schema()
                || schema_file.overlap(&shard_stats.start, &shard_stats.end, keyspace_id)
            {
                info!(
                    "{}: store has stale schema version", keyspace_id;
                    "shard_ver" => shard_stats.schema_version, "cur_ver" => cur_schema_version, "shard" => ?shard_stats,
                );
                need_broadcast = true;
                break;
            }
        }
        // Broadcast schema update to stores without building schema again.
        if need_broadcast {
            let file_id = schema_file.get_file_id();
            broadcast_schema_update_to_all_stores(
                stores,
                self.security_mgr.clone(),
                self.config.http_timeout.0,
                keyspace_id,
                file_id,
            )
            .await;
            info!(
                "broadcast schema update to all stores, keyspace_id: {} file_id: {}",
                keyspace_id, file_id
            );
            return true;
        }
        false
    }

    fn build_new_schema(
        &self,
        local_schema_file: Option<&SchemaFile>,
        table_infos: Vec<TableInfo>,
    ) -> Result<Option<Vec<Schema>>> {
        // 3. Build the schema file and upload to S3.
        let mut schemas = if let Some(schema_file) = local_schema_file {
            Vec::with_capacity(table_infos.len() + schema_file.schema_count())
        } else {
            Vec::with_capacity(table_infos.len())
        };
        let mut to_be_removed = vec![];
        for ti in table_infos {
            if !ti.with_required_changes() {
                to_be_removed.push(ti.id);
                continue;
            }
            schemas.push(table_info_to_schema(&ti).map_err(|err| {
                error!("convert table info to schema failed"; "err" => ?err, "table" => ?ti);
                SchemaError(format!("{err:?}"))
            })?);
        }

        if let Some(schema_file) = &local_schema_file {
            // Check if the schemas contains in schema file to avoid useless update.
            if schema_file.contains(&schemas) && !schema_file.has_overlap_ids(&to_be_removed) {
                // The schema has no change or not relevant, skip.
                return Ok(None);
            }

            // Merge schemas in file to build the new one.
            let base = schema_file.export_schemas();
            schemas = merge_schema_diffs(base, schemas, &to_be_removed);
        }

        Ok(Some(schemas))
    }

    fn remove_keyspace_local_file(&self, keyspace_id: u32) -> Result<bool> {
        if let Some((_, files)) = self.meta_file.remove_keyspace(keyspace_id) {
            let file_ids: Vec<u64> = files.iter().map(|f| f.0).filter(|f| *f > 0).collect();
            remove_schema_file_from_local(&self.config.dir, keyspace_id, &file_ids).map_err(
                |err| -> Error {
                    box_err!(
                        "{}: remove_schema_file_from_local failed {:?}",
                        keyspace_id,
                        err
                    )
                },
            )?;
            return Ok(true);
        }
        Ok(false)
    }

    fn repair_meta_file_if_needed(&self) -> Result<()> {
        let mut repaired_schemas = 0;
        let mut repaired_keyspaces = 0;

        let entries = match fs::read_dir(&self.config.dir) {
            Ok(entries) => entries,
            Err(_) => return Ok(()),
        };

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if !path.is_dir() {
                continue;
            }
            let dir_name = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };
            let keyspace_id = match dir_name.parse::<u32>() {
                Ok(id) => id,
                Err(_) => continue,
            };
            let latest_file_id = self
                .meta_file
                .get_latest_file(keyspace_id)
                .map(|f| f.0)
                .unwrap_or(0);
            let mut keyspace_repaired = false;
            if let Ok(file_names) = get_all_schema_files_in_order(&path) {
                for file_name in file_names {
                    if let Ok(file_id) =
                        u64::from_str_radix(file_name.strip_suffix(".schema").unwrap_or(""), 16)
                    {
                        if file_id <= latest_file_id {
                            continue;
                        }
                        if let Ok(schema_file) =
                            self.read_and_validate_schema_file(keyspace_id, file_id, &path)
                        {
                            let schema_version = schema_file.get_version();
                            let res = self
                                .meta_file
                                .add_file(keyspace_id, file_id, schema_version);
                            if res.is_err() {
                                warn!(
                                    "{}: schema version step back detected during repair, {} -> {}, clear keyspace schema files",
                                    keyspace_id,
                                    self.meta_file
                                        .get_latest_file(keyspace_id)
                                        .map(|(_, v)| v)
                                        .unwrap_or_default(),
                                    schema_version
                                );
                                self.remove_keyspace_local_file(keyspace_id)?;
                            }
                            info!(
                                "repaired meta_file for keyspace {}: file_id={:016x}, schema_version={}",
                                keyspace_id, file_id, schema_version
                            );
                            repaired_schemas += 1;
                            keyspace_repaired = true;
                        }
                    }
                }
            }
            if keyspace_repaired {
                repaired_keyspaces += 1;
            }
        }

        if repaired_keyspaces > 0 {
            let meta = self.meta_file.write();
            write_meta_file_to_local(&self.config.dir, Bytes::from(meta)).map_err(
                |err| -> Error { box_err!("failed to persist repaired meta_file: {:?}", err) },
            )?;

            info!(
                "meta_file repaired for {} keyspaces, {} schemas",
                repaired_keyspaces, repaired_schemas
            );
        }

        Ok(())
    }

    fn read_and_validate_schema_file(
        &self,
        keyspace_id: u32,
        file_id: u64,
        keyspace_dir: &Path,
    ) -> Result<SchemaFile> {
        let file_path = keyspace_dir.join(format!("{:016x}.schema", file_id));

        if !file_path.exists() {
            return Err(Error::CheckError(format!(
                "schema file not found: {}",
                file_path.display()
            )));
        }
        let fd = Arc::new(fs::File::open(&file_path)?);
        let local_file = Arc::new(LocalFile::from_file(file_id, file_path, fd)?);
        let schema_file = SchemaFile::open(local_file)?;
        if schema_file.get_file_id() != file_id {
            return Err(Error::CheckError(format!(
                "schema file ID mismatch: expected {}, got {}",
                file_id,
                schema_file.get_file_id()
            )));
        }
        debug!(
            "validated schema file for keyspace {}: file_id={:016x}, version={}",
            keyspace_id,
            file_id,
            schema_file.get_version()
        );
        Ok(schema_file)
    }
}

fn table_info_to_partition_sc_spec(ti: &TableInfo) -> Option<Vec<(i64, StorageClassSpec)>> {
    ti.partition.as_ref().map(|p| {
        p.definitions
            .iter()
            .map(|d| (d.id, d.storage_class_spec()))
            .collect::<Vec<_>>()
    })
}

fn table_info_to_schema(ti: &TableInfo) -> Result<Schema> {
    let mut builder = SchemaBufBuilder::new(ti.id);
    builder
        .storage_class_spec(ti.storage_class_spec())
        .partitions(table_info_to_partition_sc_spec(ti));

    if ti.with_columnar() {
        let ti_cols = ti.cols.as_ref().unwrap();
        let mut ti_pk_cols = vec![];
        if let Some(idx_info) = ti.index_info.as_ref() {
            let pk_idx = idx_info.iter().find(|idx| idx.is_primary);
            if let Some(pk_idx) = pk_idx {
                for idx_col in &pk_idx.idx_cols {
                    ti_pk_cols.push(ti_cols[idx_col.offset as usize].clone());
                }
            }
        }
        let pk_col_ids: Vec<i64> = ti_pk_cols.iter().map(|c| c.id).collect();
        let pk_cols = convert_column_infos_to_tipb(&ti_pk_cols, ti.pk_is_handle)?;
        let mut columns = convert_column_infos_to_tipb(ti.cols.as_ref().unwrap(), ti.pk_is_handle)?;
        columns.retain(|c| !pk_col_ids.contains(&c.get_column_id()));
        if !ti.pk_is_handle {
            // make sure the common handle columns are ordered by offset.
            columns.extend_from_slice(&pk_cols);
        }
        let handle_column = if ti.is_common_handle {
            new_common_handle_column_info()
        } else if ti.pk_is_handle {
            let pk_handle_col = columns.iter().find(|c| c.get_pk_handle()).unwrap().clone();
            columns.retain(|c| !c.get_pk_handle());
            pk_handle_col
        } else {
            new_int_handle_column_info()
        };
        let vector_indexes = parse_vector_indexes(ti_cols, ti.index_info.as_ref());
        let fulltext_indexes = parse_fulltext_indexes(ti_cols, ti.index_info.as_ref());

        builder.columns(
            handle_column,
            new_version_column_info(),
            columns,
            pk_col_ids,
            ti.max_col_id,
            vector_indexes,
            fulltext_indexes,
        );
    }

    Ok(builder.build().into())
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct BlacklistKeyspace {
    keyspace_ids: Vec<u32>,
}

pub struct SchemaManagerCore {
    ctx: Arc<SchemaMgrContext>,
    security_mgr: Arc<SecurityManager>,
    config: SchemaManagerConfig,
    txn_client: TxnClient,
    meta_file: MetaFile,
    id_allocator: Arc<dyn IdAllocator>,
    blacklist_keyspaces: Option<HashSet<u32>>,
    schema_upload_semaphore: Arc<Semaphore>,
}

impl SchemaManagerCore {
    pub(crate) fn new(
        ctx: Arc<SchemaMgrContext>,
        security_mgr: Arc<SecurityManager>,
        config: SchemaManagerConfig,
        txn_client: TxnClient,
    ) -> Self {
        let blacklist_keyspaces: Option<HashSet<u32>> =
            (!config.blacklist_file.as_os_str().is_empty()).then(|| {
                let data = fs::read_to_string(&config.blacklist_file).unwrap();
                let blacklist: BlacklistKeyspace = serde_json::from_str(&data).unwrap();
                let blacklist_keyspaces: HashSet<u32> =
                    blacklist.keyspace_ids.iter().cloned().collect();
                info!(
                    "blacklist keyspaces count: {}, keyspaces: {:?}",
                    blacklist_keyspaces.len(),
                    blacklist_keyspaces
                );
                blacklist_keyspaces
            });
        let meta_file_path = config.dir.join(META_FILE_NAME);
        let meta_file = if meta_file_path.exists() {
            MetaFile::open(LocalFile::open(0, meta_file_path).unwrap()).unwrap()
        } else {
            MetaFile::new()
        };
        let id_allocator = Arc::new(PdIdAllocator::new(ctx.pd.clone()));
        let concurrency = if config.schema_upload_concurrency > 0 {
            config.schema_upload_concurrency
        } else {
            DEFAULT_SCHEMA_UPLOAD_CONCURRENCY
        };
        let schema_upload_semaphore = Arc::new(Semaphore::new(concurrency));

        let mgr = Self {
            ctx,
            security_mgr,
            config,
            txn_client,
            meta_file,
            id_allocator,
            blacklist_keyspaces,
            schema_upload_semaphore,
        };

        if !mgr.config.tikv_stores_tier.is_empty() {
            let (stores, stores_not_match) = mgr.get_tikv_stores();
            info!("schema manager: TiKV stores with matched tier: {:?}", stores_status_addr(&stores);
                "tier" => &mgr.config.tikv_stores_tier,
                "not_match" => ?stores_status_addr(&stores_not_match),
            );
        }

        mgr
    }

    // Return whether the keyspace_id is in the blacklist, return false if blacklist
    // not configured.
    fn in_blacklist(&self, keyspace_id: u32) -> bool {
        self.blacklist_keyspaces
            .as_ref()
            .is_some_and(|blacklist| blacklist.contains(&keyspace_id))
    }

    pub fn get_tikv_stores(&self) -> (Vec<Store>, Vec<Store> /* stores_not_match */) {
        let all_stores = match get_all_stores_except_tiflash(&self.ctx.pd) {
            Ok(stores) => stores,
            Err(err) => {
                warn!("schema manager: get stores failed: {:?}", err);
                return (vec![], vec![]);
            }
        };

        if !self.config.tikv_stores_tier.is_empty() {
            all_stores.into_iter().partition(|store| {
                store.get_labels().iter().any(|label| {
                    label.key.eq_ignore_ascii_case(TIKV_STORE_LABEL_TIER_KEY)
                        && label
                            .value
                            .eq_ignore_ascii_case(&self.config.tikv_stores_tier)
                })
            })
        } else {
            (all_stores, vec![])
        }
    }

    pub fn get_schema_file_from_local(&self, keyspace_id: u32) -> Result<Option<SchemaFile>> {
        read_schema_file_from_local(&self.config.dir, &self.meta_file, keyspace_id)
    }
}

#[async_trait]
impl schema::KvScanner for SchemaManager {
    async fn scan(
        &self,
        start: &[u8],
        end: &[u8],
        start_ts: TimeStamp,
    ) -> std::result::Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let txn_start_ts = tikv_client::Timestamp::from_version(start_ts.into_inner());
        let mut snapshot = self
            .txn_client
            .snapshot(txn_start_ts, TransactionOptions::new_pessimistic());

        let mut pairs = Vec::new();
        let mut current_key = start.to_vec();

        loop {
            let scan_range: BoundRange = (current_key.clone()..end.to_vec()).into();
            let batch: Vec<KvPair> = snapshot
                .scan(scan_range, self.config.scan_batch_size)
                .await
                .map_err(|e| e.to_string())?
                .collect();
            let batch_len = batch.len();
            if batch_len == 0 {
                // end of scan
                break;
            }
            current_key = batch.last().unwrap().key().clone().into();
            current_key.push(0);

            for KvPair(key, val) in batch {
                pairs.push((key.into(), val));
            }
            if batch_len < self.config.scan_batch_size as usize {
                // end of scan, no need to continue
                break;
            }
        }

        Ok(pairs)
    }
}

#[async_trait]
impl schema::KvGetter for SchemaManager {
    async fn get(
        &self,
        key: &[u8],
        start_ts: TimeStamp,
    ) -> std::result::Result<Option<Vec<u8>>, String> {
        let txn_start_ts = tikv_client::Timestamp::from_version(start_ts.into_inner());
        let mut snapshot = self
            .txn_client
            .snapshot(txn_start_ts, TransactionOptions::new_pessimistic());
        let val = snapshot.get(key.to_vec()).await.map_err(|e| {
            format!(
                "schema manager: kv get failed: {}: {e:?})",
                log_wrappers::Value::key(key)
            )
        })?;
        Ok(val)
    }

    async fn batch_get(
        &self,
        keys: &[Vec<u8>],
        start_ts: TimeStamp,
    ) -> std::result::Result<Vec<Option<Vec<u8>>>, String> {
        let txn_start_ts = tikv_client::Timestamp::from_version(start_ts.into_inner());
        let mut snapshot = self
            .txn_client
            .snapshot(txn_start_ts, TransactionOptions::new_pessimistic());
        let pairs: HashMap<Key, Value> = snapshot
            .batch_get(keys.to_vec())
            .await
            .map_err(|e| format!("schema manager: kv batch get failed: {e:?})"))?
            .map(|pair| (pair.0, pair.1))
            .collect();
        let mut vals = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(val) = pairs.get(&Key::from(key.to_vec())) {
                vals.push(Some(val.clone()));
            } else {
                vals.push(None);
            }
        }
        Ok(vals)
    }
}

fn parse_vector_indexes(
    ti_cols: &[ColumnInfo],
    idx_infos: Option<&Vec<IndexInfo>>,
) -> Vec<VectorIndexDef> {
    let mut vec_idxes = vec![];
    for col in ti_cols {
        if let Some(vec_idx_info) = &col.vector_index {
            vec_idxes.push(new_vec_idx_def(vec_idx_info, 0, col.id));
        }
    }
    if let Some(idx_infos) = idx_infos {
        for idx_info in idx_infos {
            if let Some(vec_idx_info) = &idx_info.vector_index {
                let column_offset = idx_info.idx_cols[0].offset as usize;
                let col_id = ti_cols[column_offset].id;
                vec_idxes.push(new_vec_idx_def(vec_idx_info, idx_info.id, col_id));
            }
        }
    }
    vec_idxes
}

fn parse_fulltext_indexes(
    ti_cols: &[ColumnInfo],
    idx_infos: Option<&Vec<IndexInfo>>,
) -> Vec<kvenginepb::fts::FullTextIndexDef> {
    let mut idxes = vec![];
    if let Some(idx_infos) = idx_infos {
        for idx_info in idx_infos {
            if let Some(fts_info) = &idx_info.full_text_index {
                let column_offset = idx_info.idx_cols[0].offset as usize;
                let col_id = ti_cols[column_offset].id; // TODO (wenxuan): Support multi-column
                idxes.push(kvenginepb::fts::FullTextIndexDef {
                    index_id: idx_info.id,
                    col_id,
                    parser_type: fts_info.parser_type.clone(),
                    ..Default::default()
                });
            }
        }
    }
    idxes
}

fn new_vec_idx_def(info: &VectorIndexInfo, index_id: i64, col_id: i64) -> VectorIndexDef {
    let mut vec_idx_def = VectorIndexDef::default();
    vec_idx_def.index_id = index_id;
    vec_idx_def.col_id = col_id;
    vec_idx_def.dimension = info.dimension as usize;
    vec_idx_def.index_kind = info.kind.clone();
    vec_idx_def.specs.insert(
        VECTOR_INDEX_SPEC_KEY_DISTANCE_METRIC.to_string(),
        info.distance_metric.as_bytes().to_vec(),
    );
    vec_idx_def
}

fn find_latest_schema_file<P: AsRef<Path>>(dir_path: P) -> Result<Option<String>> {
    let mut latest_file: Option<String> = None;
    info!(
        "searching latest schema file in {}",
        dir_path.as_ref().display()
    );
    let entries = fs::read_dir(&dir_path)?;
    for entry in entries {
        let entry = entry?;
        if let Ok(file_name) = entry.file_name().into_string() {
            if latest_file.is_none()
                || (file_name.ends_with(".schema") && &file_name > latest_file.as_ref().unwrap())
            {
                latest_file = Some(file_name);
            }
        }
    }
    Ok(latest_file)
}

fn get_all_schema_files_in_order<P: AsRef<Path>>(dir_path: P) -> Result<Vec<String>> {
    let mut files: Vec<String> = Vec::new();
    info!(
        "searching all schema files in order in {}",
        dir_path.as_ref().display()
    );
    let entries = fs::read_dir(&dir_path)?;
    for entry in entries {
        let entry = entry?;
        if let Ok(file_name) = entry.file_name().into_string() {
            if file_name.ends_with(".schema") {
                files.push(file_name);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn merge_schema_diffs(
    mut base: BTreeMap<i64, Schema>,
    added: Vec<Schema>,
    removed_ids: &[i64],
) -> Vec<Schema> {
    for schema in added {
        base.insert(schema.table_id, schema);
    }
    for id in removed_ids {
        base.remove(id);
    }
    base.values().cloned().collect::<Vec<_>>()
}

fn read_schema_file_from_local<P: AsRef<Path>>(
    base_dir: P,
    meta_file: &MetaFile,
    keyspace_id: u32,
) -> Result<Option<SchemaFile>> {
    let dir = base_dir.as_ref().join(keyspace_id.to_string());
    // Try get file_id from meta_file.
    let file_id = if let Some((file_id, _)) = meta_file.get_latest_file(keyspace_id) {
        file_id
    } else {
        let latest_schema_filename = find_latest_schema_file(&dir)?;
        // If no schema file found, return None.
        if latest_schema_filename.is_none() {
            info!("no schema file found, keyspace_id: {}", keyspace_id);
            return Ok(None);
        }
        let latest_schema_filename = latest_schema_filename.unwrap();
        u64::from_str_radix(latest_schema_filename.strip_suffix(".schema").unwrap(), 16)?
    };
    if file_id == 0 {
        return Ok(None);
    }

    let file_path = dir.join(format!("{:016x}.schema", file_id));
    let fd = Arc::new(fs::File::open(file_path.as_path())?);
    let local_file = Arc::new(LocalFile::from_file(file_id, file_path, fd)?);
    let file = SchemaFile::open(local_file)?;
    Ok(Some(file))
}

fn write_schema_file_to_local<P: AsRef<Path>>(
    base_dir: P,
    keyspace_id: u32,
    id: u64,
    data: Bytes,
) -> Result<()> {
    let dir = base_dir.as_ref().join(keyspace_id.to_string());
    let filename = format!("{:016x}.schema", id);
    let tmp_file = format!("{:016x}.schema.tmp", id);
    let file_path = dir.join(filename);
    let tmp_file_path = dir.join(tmp_file);
    fs::create_dir_all(dir)?;
    fs::write(tmp_file_path.as_path(), data)?;
    fs::rename(tmp_file_path.as_path(), file_path)?;
    Ok(())
}

fn remove_schema_file_from_local<P: AsRef<Path>>(
    base_dir: P,
    keyspace_id: u32,
    file_ids: &[u64],
) -> Result<()> {
    let dir = base_dir.as_ref().join(keyspace_id.to_string());
    for file_id in file_ids {
        let filename = format!("{:016x}.schema", file_id);
        let file_path = dir.join(filename);
        // Ignore NotFound errors and continue; propagate other errors.
        fs::remove_file(file_path.as_path()).or_else(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Ok(())
            } else {
                Err(e)
            }
        })?;
    }
    Ok(())
}

fn write_meta_file_to_local<P: AsRef<Path>>(dir: P, data: Bytes) -> Result<()> {
    let tmp_file = format!("{}.tmp", META_FILE_NAME);
    let file_path = dir.as_ref().join(META_FILE_NAME);
    let tmp_file_path = dir.as_ref().join(tmp_file);
    fs::create_dir_all(dir)?;
    fs::write(tmp_file_path.as_path(), data)?;
    fs::rename(tmp_file_path.as_path(), file_path)?;
    Ok(())
}

pub async fn broadcast_schema_update_to_all_stores(
    stores: &[Store],
    security_mgr: Arc<SecurityManager>,
    timeout: Duration,
    keyspace_id: u32,
    file_id: u64,
) {
    let mut handles = Vec::with_capacity(stores.len());
    for store in stores {
        let store = store.clone();
        let security_mgr = security_mgr.clone();
        let handle = tokio::spawn(async move {
            let status_addr = store.get_status_address();
            let uri = security_mgr
                .build_uri(format!(
                    "{}/schema_file?keyspace_id={}&file_id={}",
                    status_addr, keyspace_id, file_id
                ))
                .unwrap();
            let req = || Request::post(uri.clone()).body(Body::empty()).unwrap();
            if let Err(err) =
                send_request_to_store_with_retry(req, &store, security_mgr.as_ref(), timeout).await
            {
                error!(
                    "broadcast schema update to store {} {} failed: {:?}",
                    store.id, status_addr, err
                );
                SCHEMA_MANAGER_SYNC_LOOP_ERROR_COUNT
                    .with_label_values(&["broadcast_schema_update_to_all_stores"])
                    .inc();
            }
        });
        handles.push(handle);
    }
    join_all(handles).await;
}

pub async fn get_keyspace_stats_from_store(
    store: &Store,
    security_mgr: Arc<SecurityManager>,
    timeout: Duration,
) -> Result<Vec<ShardStatsLite>> {
    let status_addr = store.get_status_address();
    let uri = security_mgr
        .build_uri(format!("{}/kvengine/active_lite", status_addr))
        .unwrap();
    let req = || Request::get(uri.clone()).body(Body::empty()).unwrap();
    let resp_bytes =
        send_request_to_store_with_retry(req, store, security_mgr.as_ref(), timeout).await?;
    let resp: Vec<ShardStatsLite> = serde_json::from_slice(&resp_bytes)?;
    Ok(resp)
}

fn get_stats_schema_version(keyspace_shard_stats: &[ShardStatsLite]) -> Option<i64> {
    let mut valid_versions = keyspace_shard_stats
        .iter()
        .filter(|s| s.schema_version > 0)
        .map(|s| s.schema_version);

    if let Some(first_version) = valid_versions.next() {
        if valid_versions.all(|v| v == first_version) {
            Some(first_version)
        } else {
            None
        }
    } else {
        // No valid schema version found, return 0.
        Some(0)
    }
}

#[inline]
fn stores_status_addr(stores: &[Store]) -> Vec<&str> {
    stores.iter().map(|s| s.get_status_address()).collect()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs};

    use bytes::Bytes;
    use kvengine::{
        ShardStatsLite,
        table::{
            columnar::{new_int_handle_column_info, new_version_column_info},
            file::LocalFile,
            schema_file::{SchemaBuf, build_schema_file},
        },
    };
    use schema::schema::StorageClassSpec;
    use tikv_util::info;

    use super::*;

    #[test]
    fn test_find_latest_schema_file() {
        ::test_util::init_log_for_test();

        let dir = tempfile::tempdir().unwrap();
        let _ = fs::create_dir_all(dir.path());
        for i in 50..=100 {
            let filename = format!("{:016x}.schema", i);
            fs::write(dir.path().join(filename), "test").unwrap();
        }
        for i in 0..50 {
            let filename = format!("{:016x}.schema", i);
            fs::write(dir.path().join(filename), "test").unwrap();
        }
        let latest_file = find_latest_schema_file(dir.path()).unwrap();
        assert_eq!(latest_file, Some(format!("{:016x}.schema", 100)));
    }

    #[test]
    fn test_local_schema_file() {
        ::test_util::init_log_for_test();

        let dir = tempfile::tempdir().unwrap();
        let mut schemas = vec![];
        for i in 0..=10 {
            let schema = SchemaBuf::new(
                i,
                new_int_handle_column_info(),
                new_version_column_info(),
                vec![new_int_handle_column_info()],
                vec![],
                0,
                vec![],
                vec![],
                StorageClassSpec::default(),
                None,
            );
            schemas.push(schema.into());
        }
        let schema_file_data = build_schema_file(1234, 100, schemas.clone(), 0);
        write_schema_file_to_local(dir.path(), 1234, 1000, Bytes::from(schema_file_data)).unwrap();
        schemas.push(
            SchemaBuf::new(
                11,
                new_int_handle_column_info(),
                new_version_column_info(),
                vec![new_int_handle_column_info()],
                vec![],
                0,
                vec![],
                vec![],
                StorageClassSpec::default(),
                None,
            )
            .into(),
        );
        let schema_file_data = build_schema_file(1234, 201, schemas, 12345);
        write_schema_file_to_local(dir.path(), 1234, 1001, Bytes::from(schema_file_data)).unwrap();

        // schema_file is the newest schema file of the keyspace.
        let schema_file = read_schema_file_from_local(dir.path(), &MetaFile::new(), 1234)
            .unwrap()
            .unwrap();
        assert_eq!(schema_file.get_keyspace_id(), 1234);
        assert_eq!(schema_file.get_version(), 201);
        assert_eq!(schema_file.get_restore_version(), 12345);
        info!(
            "schema file keyspace_id: {}, schema_version: {}, file_id: {}",
            schema_file.get_keyspace_id(),
            schema_file.get_version(),
            schema_file.get_file_id()
        );
        assert_eq!(schema_file.get_file_id(), 1001);
    }

    #[test]
    fn test_meta_file() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaFile::new();
        for i in 1..100 {
            meta.add_file(i, (i * 10) as u64, (i + i * 10) as i64)
                .unwrap();
            meta.add_file(i, (i * 10 + 1) as u64, (i + i * 10 + 1) as i64)
                .unwrap();
        }
        for i in 1..10 {
            meta.add_checked_version(i, (i + i * 11) as i64);
        }
        let data = meta.write();
        write_meta_file_to_local(&dir, Bytes::from(data)).unwrap();
        let meta_file_path = dir.as_ref().join(META_FILE_NAME);
        let meta_file = LocalFile::open(0, meta_file_path).unwrap();
        let read_meta = MetaFile::open(meta_file).unwrap();
        for i in 1..100 {
            let (file_id, schema_version) = read_meta.get_latest_file(i).unwrap();
            assert_eq!(file_id, (i * 10 + 1) as u64);
            assert_eq!(schema_version, (i + i * 10 + 1) as i64);
        }
        for i in 1..10 {
            let checked_version = read_meta.get_checked_version(i).unwrap();
            assert_eq!(checked_version, (i + i * 11) as i64);
        }
    }

    #[test]
    fn test_update_shard_stats() {
        let make_shard_stats = |id: u64, start: Vec<u8>, end: Vec<u8>| -> ShardStatsLite {
            ShardStatsLite {
                id,
                ver: 1,
                start: start.into(),
                end: end.into(),
                inner_key_off: 0,
                total_size: 0,
                schema_version: 1000,
                schema_restore_version: 0,
                write_sequence: 0,
                storage_class_spec: Default::default(),
                columnar_tables: 0,
            }
        };
        let mut keyspace_stats = HashMap::new();
        let shard_stats = make_shard_stats(1, vec![120, 255, 255, 255], vec![]);
        SchemaManager::update_keyspace_stats(&mut keyspace_stats, shard_stats);
        assert!(keyspace_stats.is_empty());
        let shard_stats = make_shard_stats(1, vec![120, 0, 0, 1], vec![120, 0, 0, 3]);
        SchemaManager::update_keyspace_stats(&mut keyspace_stats, shard_stats);
        assert!(keyspace_stats.is_empty());
        let shard_stats = make_shard_stats(1, vec![120, 0, 0, 2], vec![120, 0, 0, 3, 4]);
        assert!(keyspace_stats.is_empty());
        SchemaManager::update_keyspace_stats(&mut keyspace_stats, shard_stats);
        let shard_stats = make_shard_stats(1, vec![120, 0, 0, 1, 3], vec![120, 0, 0, 1, 4]);
        SchemaManager::update_keyspace_stats(&mut keyspace_stats, shard_stats);
        assert_eq!(keyspace_stats.len(), 1);
        let shard_stats = make_shard_stats(2, vec![120, 0, 0, 2], vec![120, 0, 0, 3]);
        SchemaManager::update_keyspace_stats(&mut keyspace_stats, shard_stats);
        assert_eq!(keyspace_stats.len(), 1);
        let mut shard_stats = make_shard_stats(2, vec![120, 0, 0, 2], vec![120, 0, 0, 3]);
        shard_stats.total_size = 100;
        SchemaManager::update_keyspace_stats(&mut keyspace_stats, shard_stats);
        assert_eq!(keyspace_stats.len(), 2);
        let shard_stats = make_shard_stats(3, vec![120, 0, 0, 3, 3], vec![120, 0, 0, 4]);
        SchemaManager::update_keyspace_stats(&mut keyspace_stats, shard_stats);
        assert_eq!(keyspace_stats.len(), 3);
    }

    #[test]
    fn test_schema_file_gc() {
        ::test_util::init_log_for_test();

        let dir = tempfile::tempdir().unwrap();
        let meta = MetaFile::new();

        // Create schema files on disk
        let schemas = vec![
            SchemaBuf::new(
                1,
                new_int_handle_column_info(),
                new_version_column_info(),
                vec![new_int_handle_column_info()],
                vec![],
                0,
                vec![],
                vec![],
                StorageClassSpec::default(),
                None,
            )
            .into(),
        ];

        let keyspace_id = 100;
        // Add default file to keyspace
        meta.add_default_file(keyspace_id, 0).unwrap();
        meta.add_default_file(keyspace_id, 10).unwrap();
        for i in 1..=5i64 {
            let file_id = (i * 1000) as u64;
            let schema_version = i * 10;
            let schema_file_data =
                build_schema_file(keyspace_id, schema_version, schemas.clone(), 0);
            write_schema_file_to_local(
                dir.path(),
                keyspace_id,
                file_id,
                Bytes::from(schema_file_data),
            )
            .unwrap();
            meta.add_file(keyspace_id, file_id, schema_version).unwrap();
        }

        // Verify all files exist
        for i in 1..=5 {
            let file_id = i * 1000u64;
            let file_path = dir
                .path()
                .join(keyspace_id.to_string())
                .join(format!("{:016x}.schema", file_id));
            assert!(file_path.exists(), "File should exist: {:?}", file_path);
        }

        // Run GC to keep only last 2 versions
        let to_remove = meta.gc_old_files(keyspace_id, 2);
        assert_eq!(to_remove.len(), 3);
        assert_eq!(to_remove, vec![1000, 2000, 3000]);

        info!("to_remove: {:?}", to_remove);
        // Remove old files from disk
        remove_schema_file_from_local(dir.path(), keyspace_id, &to_remove).unwrap();

        // Verify old files are removed
        for &file_id in &to_remove {
            let file_path = dir
                .path()
                .join(keyspace_id.to_string())
                .join(format!("{:016x}.schema", file_id));
            assert!(
                !file_path.exists(),
                "File should be removed: {:?}",
                file_path
            );
        }

        // Verify latest files still exist
        for file_id in &[4000u64, 5000u64] {
            let file_path = dir
                .path()
                .join(keyspace_id.to_string())
                .join(format!("{:016x}.schema", file_id));
            assert!(
                file_path.exists(),
                "File should still exist: {:?}",
                file_path
            );
        }

        // Verify meta file reflects the changes
        let remaining = meta.get_files(keyspace_id).unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0], (4000, 40));
        assert_eq!(remaining[1], (5000, 50));
    }
}
