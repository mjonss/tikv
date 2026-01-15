// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use bstr::ByteSlice;
use bytes::{Buf, BufMut, Bytes};
use chrono::NaiveDate;
use collections::{HashMap, HashSet};
use engine_traits::GetObjectOptions;
use kvengine::dfs::{self, DFSConfig, Dfs, FileType, Options, StorageClass, new_dfs_from_config};
use pd_client::PdClient;
use protobuf::Message;
use rfenginepb::ClusterBackupMeta;
use security::SecurityConfig;
use tikv_util::{error, info, mpsc::Receiver, time::Instant, warn};

use crate::{
    backup::{IncrementalBackupFile, backup_file_full_path},
    common::{
        INCREMENTAL_BACKUP_FOLDER_FORMAT, LocalObject, StoreRlog, StoreWalRlog, TableFile,
        TempLocalObject, collect_store_wal_rlog_files, create_pd_client,
        get_all_incremental_backups,
    },
    error::{Error, Result},
    restore::RestoreConfig,
    restore_keyspace::{BackupCluster, RESTORE_RFENGINE_CONCURRENCY},
    wal::WalChunkData,
};

pub const DEFAULT_MAX_ARCHIVE_FILE_SIZE: u64 = 1024 * 1024 * 1024;
const ARCHIVE_PATH_PREFIX: &str = "archive";

pub const LOAD_FILE_CONCURRENCY: usize = 64;
pub const OBJECT_ADDR_SIZE: usize = 20;

/// Magic Number of the Archive index file. It's picked by running
///    echo archive.idx | sha1sum
/// and taking the leading 32 bits.
const ARCHIVE_MAGIC_NUMBER: u32 = 0x923d4deb;

pub const ARCHIVE_INDEX_FORMAT_V1: u32 = 1;
pub const ARCHIVE_INDEX_FORMAT_V2: u32 = 2;

// Object address format:
//
// +---------------------------------+
// |         package id: u32         |
// +---------------------------------+
// |           offset: u64           |
// +---------------------------------+
// |           length: u64           |
// +---------------------------------+
//
// Archive index format v1:
//
// +---------------------------------+
// |        magic number: u32        |
// +---------------------------------+
// |          version: u32           |
// +---------------------------------+
// |   backup meta object address    |
// +---------------------------------+
// |      num of file ids: u32       |
// +---------------------------------+
// |            file id 1            |
// +---------------------------------+
// |            file id 2            |
// +---------------------------------+
// |             ...                 |
// +---------------------------------+
// |            file id n            |
// +---------------------------------+
// |       sst object address 1      |
// +---------------------------------+
// |       sst object address 2      |
// +---------------------------------+
// |             ...                 |
// +---------------------------------+
// |       sst object address n      |
// +---------------------------------+
//
// Wal meta format:
//
// +---------------------------------+
// |          epoch id: u32          |
// +---------------------------------+
// |      num of wal chunks: u32     |
// +---------------------------------+
// |    wal chunk object address 1   |
// +---------------------------------+
// |    wal chunk object address 2   |
// +---------------------------------+
// |               ...               |
// +---------------------------------+
// |    wal chunk object address n   |
// +---------------------------------+
//
// Store backup meta format:
//
// +---------------------------------+
// |          store id: u64          |
// +---------------------------------+
// |     snapshot epoch id: u32      |
// +---------------------------------+
// |  snapshot meta object address   |
// +---------------------------------+
// |  snapshot rlog object address   |
// +---------------------------------+
// |      num of wal metas: u32      |
// +---------------------------------+
// |      wal meta length 1: u32     |
// +---------------------------------+
// |      wal meta length 2: u32     |
// +---------------------------------+
// |               ...               |
// +---------------------------------+
// |      wal meta length n: u32     |
// +---------------------------------+
// |            wal meta 1           |
// +---------------------------------+
// |            wal meta 2           |
// +---------------------------------+
// |               ...               |
// +---------------------------------+
// |            wal meta n           |
// +---------------------------------+
//
// Archive index format v2:
//
// +---------------------------------+
// |        magic number: u32        |
// +---------------------------------+
// |          version: u32           |
// +---------------------------------+
// |   cluster meta object address   |
// +---------------------------------+
// |        num of stores: u32       |
// +---------------------------------+
// |    store meta length 1: u32     |
// +---------------------------------+
// |    store meta length 2: u32     |
// +---------------------------------+
// |               ...               |
// +---------------------------------+
// |    store meta length n: u32     |
// +---------------------------------+
// |          store meta 1           |
// +---------------------------------+
// |          store meta 2           |
// +---------------------------------+
// |               ...               |
// +---------------------------------+
// |          store meta n           |
// +---------------------------------+
// |      num of file ids: u32       |
// +---------------------------------+
// |          file id 1: u64         |
// +---------------------------------+
// |          file id 2: u64         |
// +---------------------------------+
// |             ...                 |
// +---------------------------------+
// |          file id n: u64         |
// +---------------------------------+
// |       sst object address 1      |
// +---------------------------------+
// |       sst object address 2      |
// +---------------------------------+
// |             ...                 |
// +---------------------------------+
// |       sst object address n      |
// +---------------------------------+
//
// Archive package format:
//
// +---------------------------------+
// |          object file 1          |
// +---------------------------------+
// |          object file 2          |
// +---------------------------------+
// |             ...                 |
// +---------------------------------+
// |          object file n          |
// +---------------------------------+

pub fn archive_with_cfg(config: ArchiveConfig) -> Result<()> {
    let pd_client = Arc::new(create_pd_client(&config.security, &config.pd));
    let dfs_conf = config.dfs.clone();
    let dfs = new_dfs_from_config(dfs_conf);
    let expiration_date = NaiveDate::parse_from_str(
        config.expiration_date.as_str(),
        INCREMENTAL_BACKUP_FOLDER_FORMAT,
    )
    .unwrap();
    let start_archive_duration = chrono::Duration::from_std(config.start_archive_duration).unwrap();
    let end_archive_date = (chrono::Utc::now() - start_archive_duration).date_naive();
    if expiration_date >= end_archive_date {
        warn!(
            "not time for archiving yet. expiration_date {}, start_archive_duration {}",
            expiration_date, start_archive_duration
        );
        return Ok(());
    }
    let begin_archive_date = expiration_date
        .checked_add_days(chrono::Days::new(1))
        .unwrap();
    archive_cluster_backup(config, pd_client, dfs, begin_archive_date, end_archive_date).map(|_| ())
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ArchiveConfig {
    pub pd: pd_client::Config,
    pub security: SecurityConfig,
    pub dfs: DFSConfig,
    pub data_dir: String,
    pub max_archive_file_size: u64,
    pub start_archive_duration: Duration,
    pub expiration_date: String,
    pub concurrency: usize,
    pub store_concurrency: usize,
    pub skip_no_meta_days: usize,
    pub skip_shards: Option<HashSet<u64>>,
    pub skip_keyspace_names: Option<HashSet<String>>,
    pub dry_run: bool,
    pub fetch_wal_timeout: Duration,
    pub file_types_blacklist: Vec<String>,
}

impl ArchiveConfig {
    pub fn default() -> Self {
        let expiration_date_naive = chrono::Utc::now().date_naive() - chrono::Days::new(1);
        let expiration_date = archive_format_date(&expiration_date_naive);
        Self {
            pd: pd_client::Config::default(),
            security: SecurityConfig::default(),
            dfs: DFSConfig::default(),
            data_dir: String::default(),
            max_archive_file_size: DEFAULT_MAX_ARCHIVE_FILE_SIZE,
            start_archive_duration: Duration::from_secs(0),
            expiration_date,
            concurrency: LOAD_FILE_CONCURRENCY,
            store_concurrency: RESTORE_RFENGINE_CONCURRENCY,
            skip_no_meta_days: 0,
            skip_shards: None,
            skip_keyspace_names: None,
            dry_run: true,
            fetch_wal_timeout: Duration::from_secs(600), // 10 minutes
            file_types_blacklist: vec![],
        }
    }

    pub fn check_data_dir(&mut self) {
        if self.data_dir.is_empty() {
            let data_dir = tempdir::TempDir::new(ARCHIVE_PATH_PREFIX).unwrap();
            let data_path = data_dir.path().to_path_buf();
            self.data_dir = data_path.to_str().unwrap().to_string();
        }
    }
}

#[derive(Default, Clone)]
pub struct ArchiveBackup {
    pub date: NaiveDate,
    pub meta_data: Bytes,
    pub files: HashMap<u64, FileType>,
    pub shards_count: usize,
}

impl ArchiveBackup {
    pub fn new(
        date: NaiveDate,
        meta_data: Bytes,
        files: HashMap<u64, FileType>,
        shards_count: usize,
    ) -> Self {
        Self {
            date,
            meta_data,
            files,
            shards_count,
        }
    }
}

pub fn archive_cluster_backup(
    config: ArchiveConfig,
    pd_client: Arc<dyn PdClient>,
    dfs: Arc<dyn Dfs>,
    begin_archive_date: NaiveDate,
    end_archive_date: NaiveDate,
) -> Result<usize> /* latest archived shards count */ {
    let cluster_id = pd_client.get_cluster_id()?;

    let skip_keyspace_ids = if let Some(skip_keyspace_names) = &config.skip_keyspace_names {
        let mut skip_keyspace_ids: HashSet<u32> = HashSet::default();
        for keyspace_name in skip_keyspace_names.iter() {
            match pd_client.load_keyspace(keyspace_name.clone()) {
                Err(e) if pd_client::grpc_error_is_unimplemented(&e) => {
                    warn!(
                        "load_keyspace is unimplemented";
                        "err" => ?e,
                    );
                    break;
                }
                Err(e) => {
                    warn!(
                        "get keyspace meta failed";
                        "keyspace_name" => keyspace_name,
                        "err" => ?e,
                    );
                    return Err(Error::PdError(e));
                }
                Ok(keyspace_meta) => {
                    if keyspace_meta.id > 0 {
                        info!(
                            "get keyspace id by name";
                            "keyspace_name" => keyspace_name,
                            "keyspace_id" => keyspace_meta.id,
                        );
                        skip_keyspace_ids.insert(keyspace_meta.id);
                    }
                }
            }
        }
        if !skip_keyspace_ids.is_empty() {
            Some(skip_keyspace_ids)
        } else {
            None
        }
    } else {
        None
    };

    let mut backup_date = match get_latest_archive_date(dfs.as_ref(), &begin_archive_date) {
        Ok(latest_archive_date) => latest_archive_date
            .checked_add_days(chrono::Days::new(1))
            .unwrap(),
        Err(Error::DfsError(e)) => return Err(Error::DfsError(e)),
        Err(e) => {
            warn!(
                "failed to get latest archive date from date {}, err {}",
                begin_archive_date,
                e.to_string()
            );
            let runtime = dfs.get_runtime();
            let (backups, _) = runtime.block_on(get_all_incremental_backups(
                dfs.as_ref(),
                &begin_archive_date,
                None,
                1,
            ))?;
            if backups.is_empty() {
                return Err(Error::ArchiveError(format!(
                    "failed to get first backup meta from {}",
                    begin_archive_date,
                )));
            }
            let backup_file = backups.first().unwrap();
            let first_backup_date = backup_file.created_at().date_naive();
            begin_archive_date.max(first_backup_date)
        }
    };

    info!(
        "archive from date {} to end date {}",
        backup_date, end_archive_date,
    );

    let path = PathBuf::from(&config.data_dir).join("stores");
    let mut old: Option<ArchiveBackup> = None;
    let mut skip_once: Option<i64> = None;
    if config.skip_no_meta_days > 0 {
        skip_once = Some(config.skip_no_meta_days as i64)
    }
    loop {
        if backup_date > end_archive_date {
            break;
        }
        let new = archive_backup_files(
            config.clone(),
            pd_client.clone(),
            dfs.clone(),
            cluster_id,
            path.clone(),
            backup_date,
            skip_keyspace_ids.clone(),
            old,
        )?;
        old = Some(new);
        std::fs::remove_dir_all(&path).unwrap();
        if let Some(skip_no_meta_days) = skip_once.take() {
            backup_date += chrono::Duration::days(skip_no_meta_days + 1);
            warn!(
                "skip {} days without meta to get backup meta on {}",
                skip_no_meta_days, backup_date
            );
            continue;
        }
        backup_date += chrono::Duration::days(1);
    }
    let shards_count = old.map(|a| a.shards_count).unwrap_or_default();
    Ok(shards_count)
}

fn archive_backup_files(
    config: ArchiveConfig,
    pd_client: Arc<dyn PdClient>,
    dfs: Arc<dyn Dfs>,
    cluster_id: u64,
    path: PathBuf,
    backup_date: NaiveDate,
    skip_keyspace_ids: Option<HashSet<u32>>,
    old: Option<ArchiveBackup>,
) -> Result<ArchiveBackup> {
    let runtime = dfs.get_runtime();
    let (backups, _) =
        runtime.block_on(get_daily_incremental_backups(dfs.as_ref(), &backup_date, 1))?;
    if backups.is_empty() {
        return Err(Error::ArchiveError(format!(
            "failed to get first backup meta on {}",
            backup_date,
        )));
    }
    let back_file = backups.first().unwrap();
    let file_name = back_file.name().to_string();
    let (meta_file_data, cluster_backup) =
        get_cluster_backup_file_and_meta(dfs.as_ref(), file_name.clone())
            .map_err(|e| Error::DfsError(e))?;
    let (files, shards_count) = get_cluster_backup_files_and_shards_count(
        pd_client.clone(),
        dfs.clone(),
        cluster_id,
        file_name,
        cluster_backup,
        config.skip_shards.clone(),
        skip_keyspace_ids,
        path,
        config.security.clone(),
    )?;
    if let Some(old_archive_backup) = old {
        if old_archive_backup
            .date
            .checked_add_days(chrono::Days::new(1))
            .unwrap()
            .eq(&backup_date)
            || old_archive_backup
                .date
                .checked_add_days(chrono::Days::new(config.skip_no_meta_days as u64 + 1))
                .unwrap()
                .eq(&backup_date)
        {
            write_archive_packages_and_index(config, &pd_client, dfs, old_archive_backup, &files)?
        } else {
            return Err(Error::ArchiveError(format!(
                "old backup {} is not {}'s last day",
                old_archive_backup.date, backup_date,
            )));
        }
    }
    Ok(ArchiveBackup::new(
        backup_date,
        meta_file_data,
        files,
        shards_count,
    ))
}

fn write_archive_packages_and_index(
    config: ArchiveConfig,
    pd_client: &Arc<dyn PdClient>,
    dfs: Arc<dyn Dfs>,
    archive_backup: ArchiveBackup,
    next_day_files: &HashMap<u64, FileType>,
) -> Result<()> {
    let deleted = get_sorted_deleted_files(
        &archive_backup.files,
        next_day_files,
        &config.file_types_blacklist,
    );
    info!(
        "get deleted files {} on {}",
        deleted.len(),
        archive_backup.date,
    );
    let format_date = archive_format_date(&archive_backup.date);
    if config.dry_run {
        let not_found_files = get_not_found_files(dfs.clone(), deleted)?;
        if !not_found_files.is_empty() {
            return Err(Error::ArchiveError(format!(
                "deleted files not found {:?} on {}",
                not_found_files, archive_backup.date,
            )));
        }
    } else {
        let mut cluster_backup = ClusterBackupMeta::new();
        cluster_backup
            .merge_from_bytes(&archive_backup.meta_data)
            .unwrap();
        let mut writer = ArchiveWriter::new(
            config.max_archive_file_size,
            config.concurrency,
            dfs.clone(),
            format_date.clone(),
            archive_backup.meta_data,
        );
        let (result_tx, result_rx) = tikv_util::mpsc::bounded(cluster_backup.stores.len());
        let mut recv_store_wal_rlog_files =
            |result_rx: &Receiver<Result<StoreWalRlog>>| -> Result<()> {
                let store_wal_rlog_files = result_rx.recv().unwrap()?;
                writer.append_store_wal_rlog_files(store_wal_rlog_files);
                Ok(())
            };
        let mut msg_count = 0;
        for store in &cluster_backup.stores {
            let store_id = store.get_store_id();
            let pd_client = pd_client.clone();
            let dfs = dfs.clone();
            let cluster_backup_meta = cluster_backup.clone();
            let tx = result_tx.clone();
            let tag = format!("archive:{format_date}:{store_id}");
            std::thread::spawn(move || {
                let store_wal_rlog_files = collect_store_wal_rlog_files(
                    &tag,
                    pd_client,
                    dfs,
                    &cluster_backup_meta,
                    store_id,
                    config.fetch_wal_timeout,
                );
                let _ = tx.send(store_wal_rlog_files);
            });
            if msg_count < config.store_concurrency {
                msg_count += 1;
            } else {
                recv_store_wal_rlog_files(&result_rx)?;
            }
        }
        for _ in 0..msg_count {
            recv_store_wal_rlog_files(&result_rx)?;
        }
        writer.append_table_files(deleted)?;
        writer.finish();
    }
    Ok(())
}

fn get_sorted_deleted_files(
    old: &HashMap<u64, FileType>,
    new: &HashMap<u64, FileType>,
    file_types_blacklist: &[String],
) -> Vec<TableFile> {
    let file_types_blacklist = file_types_blacklist
        .iter()
        .map(|s| FileType::try_from(s.as_str()).unwrap())
        .collect::<Vec<_>>();
    let mut deleted: Vec<_> = old
        .iter()
        .filter_map(|(&file_id, &ftype)| {
            (!file_types_blacklist.contains(&ftype) && !new.contains_key(&file_id)).then_some(
                TableFile {
                    id: file_id,
                    ftype,
                    shard_id: 0,
                },
            )
        })
        .collect();
    deleted.sort_by_key(|f| f.id);
    deleted
}

fn get_cluster_backup_files_and_shards_count(
    pd_client: Arc<dyn PdClient>,
    dfs: Arc<dyn Dfs>,
    cluster_id: u64,
    backup_name: String,
    cluster_backup: ClusterBackupMeta,
    skip_shards: Option<HashSet<u64>>,
    skip_keyspace_ids: Option<HashSet<u32>>,
    path: PathBuf,
    security_conf: SecurityConfig,
) -> Result<(
    HashMap<u64, FileType>, // all files
    usize,                  // shards count
)> {
    if cluster_backup.cluster_id != cluster_id {
        return Err(Error::ArchiveError(format!(
            "cluster id not match, pd cluster id {}, meta cluster id {}",
            cluster_id, cluster_backup.cluster_id,
        )));
    }
    if !cluster_backup.is_lightweight {
        return Err(Error::ArchiveError(
            "only support lightweight backups".to_string(),
        ));
    }
    let start_time = Instant::now_coarse();

    let restore_conf = RestoreConfig {
        security: security_conf,
        lower_memory: true,
        ..Default::default()
    };
    let cluster = BackupCluster::new(
        &cluster_backup,
        path,
        pd_client.clone(),
        dfs,
        restore_conf,
        0,
        0,
        cluster_backup.backup_ts,
        true,
        None,
        false,
        None,
        None,
    )?;
    let all_files = HashMap::from_iter(
        cluster
            .get_all_shard_files(skip_shards, skip_keyspace_ids)
            .into_iter()
            .map(|f| (f.id, f.ftype)),
    );
    let shards_count = cluster.shards_count();
    drop(cluster);
    info!(
        "backup {} all_shards {} all_files {} takes {:?}",
        backup_name,
        shards_count,
        all_files.len(),
        start_time.saturating_elapsed()
    );
    Ok((all_files, shards_count))
}

/// Return full path of daily incremental backups in S3.
pub async fn get_daily_incremental_backups(
    dfs: &dyn Dfs,
    date: &chrono::NaiveDate,
    max_count: usize,
) -> dfs::Result<(Vec<IncrementalBackupFile>, bool)> {
    let mut files = Vec::with_capacity(std::cmp::min(max_count, 1000));
    let mut start_key = "".to_string();
    let prefix = format!("backup/{}/", date.format(INCREMENTAL_BACKUP_FOLDER_FORMAT));
    let mut reach_limit = false;
    loop {
        match dfs.list(&start_key, Some(&prefix), None).await {
            Ok((backup_files, more, next_start_after)) => {
                let mut inc_files = backup_files
                    .into_iter()
                    .filter_map(|f| IncrementalBackupFile::try_from_full_path(&f.key))
                    .collect::<Vec<_>>();
                files.append(&mut inc_files);
                if files.len() > max_count {
                    files.truncate(max_count);
                    reach_limit = true;
                }
                if reach_limit || !more {
                    break;
                }
                start_key = next_start_after.unwrap();
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok((files, reach_limit))
}

pub fn get_incremental_backup_with_name(prefix: String, name: String) -> IncrementalBackupFile {
    let backup_key = backup_file_full_path(prefix, name, None);
    IncrementalBackupFile::try_from_full_path(&backup_key).unwrap()
}

pub fn get_cluster_backup_file_and_meta(
    dfs: &dyn Dfs,
    name: String,
) -> dfs::Result<(Bytes, ClusterBackupMeta)> {
    let backup_key = backup_file_full_path(dfs.get_prefix(), name.clone(), None);
    let runtime = dfs.get_runtime();
    let data = runtime.block_on(dfs.get_object(
        backup_key.clone(),
        name,
        engine_traits::GetObjectOptions::default(),
    ))?;
    let mut cluster_backup = ClusterBackupMeta::new();
    cluster_backup.merge_from_bytes(&data).map_err(|e| {
        dfs::Error::Other(format!(
            "Incorrect encoded data from s3 {}, err {}",
            backup_key, e
        ))
    })?;
    info!(
        "Restore cluster_id {}, alloc_id {}, backup_ts {}, safe_ts {}, store cnt {}",
        cluster_backup.cluster_id,
        cluster_backup.alloc_id,
        cluster_backup.backup_ts,
        cluster_backup.safe_ts,
        cluster_backup.stores.len()
    );
    Ok((data, cluster_backup))
}

pub async fn get_all_archive_index_paths(
    dfs: &dyn Dfs,
    start_date: String,
    max_count: usize,
) -> dfs::Result<(Vec<String>, bool)> {
    let mut indexes = Vec::with_capacity(std::cmp::min(max_count, 1000));
    let mut start_key = start_date;
    let prefix = "archive/index/";

    let mut reach_limit = false;
    loop {
        // TODO: pass in `max_count` for limit.
        match dfs.list(&start_key, Some(prefix), None).await {
            Ok((archive_indexes, more, next_start_after)) => {
                let mut inc_indexes = archive_indexes
                    .into_iter()
                    .map(|f| f.key)
                    .collect::<Vec<_>>();
                indexes.append(&mut inc_indexes);
                if indexes.len() > max_count {
                    indexes.truncate(max_count);
                    reach_limit = true;
                }
                if reach_limit || !more {
                    break;
                }
                start_key = next_start_after.unwrap();
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok((indexes, reach_limit))
}

pub fn get_latest_archive_date(dfs: &dyn Dfs, start_date: &chrono::NaiveDate) -> Result<NaiveDate> {
    let (indexes, _) = dfs
        .get_runtime()
        .block_on(get_all_archive_index_paths(
            dfs,
            archive_format_date(start_date),
            usize::MAX,
        ))
        .map_err(|e| {
            error!(
                "failed to get latest archive date from date {}, err {}",
                start_date,
                e.to_string()
            );
            Error::DfsError(e)
        })?;
    if indexes.is_empty() {
        return Err(Error::ArchiveError(format!(
            "No archives have been found since that day {}",
            start_date
        )));
    }
    let latest_archive_date = NaiveDate::parse_from_str(
        parse_index_date(indexes.last().unwrap()).as_str(),
        INCREMENTAL_BACKUP_FOLDER_FORMAT,
    )
    .ok()
    .unwrap();
    Ok(latest_archive_date)
}

pub fn get_archive_index(dfs: &dyn Dfs, date: String) -> Result<(ArchiveIndex, Bytes)> {
    let index_key = archive_index_key(dfs.get_prefix(), date.clone());
    let data = dfs
        .get_runtime()
        .block_on(dfs.get_object(
            index_key.clone(),
            index_key.clone(),
            GetObjectOptions::default(),
        ))
        .map_err(|e| {
            error!(
                "failed to get archived index with date {}, err {}",
                date,
                e.to_string()
            );
            Error::DfsError(e)
        })?;
    let archive_index = ArchiveIndex::unmarshal(&data).map_err(|e| {
        dfs::Error::Other(format!(
            "Incorrect archive index data from s3 {}, err {}",
            index_key, e
        ))
    })?;
    Ok((archive_index, data))
}

pub async fn get_archived_object(dfs: &dyn Dfs, archive_addr: ArchiveAddress) -> Result<Bytes> {
    if archive_addr.object_addr.length == 0 {
        return Ok(Bytes::new());
    }
    let package_key = archive_package_key(
        dfs.get_prefix(),
        archive_addr.date.clone(),
        archive_addr.object_addr.package_id,
    );
    let opts = GetObjectOptions {
        start_off: Some(archive_addr.object_addr.offset),
        end_off: Some(archive_addr.object_addr.offset + archive_addr.object_addr.length),
    };
    dfs.get_object(package_key.clone(), package_key, opts)
        .await
        .map_err(|e| {
            error!(
                "failed to get archived object with archive addr {:?}, err {}",
                archive_addr,
                e.to_string()
            );
            Error::DfsError(e)
        })
}

pub async fn get_archived_object_to_file(
    dfs: &dyn Dfs,
    archive_addr: ArchiveAddress,
    dir: &Path,
) -> Result<TempLocalObject> {
    let package_key = archive_package_key(
        dfs.get_prefix(),
        archive_addr.date.clone(),
        archive_addr.object_addr.package_id,
    );
    let opts = GetObjectOptions {
        start_off: Some(archive_addr.object_addr.offset),
        end_off: Some(archive_addr.object_addr.offset + archive_addr.object_addr.length),
    };
    let tmp_key = format!(
        "{}-{}-{}",
        package_key,
        opts.start_off.unwrap(),
        opts.end_off.unwrap()
    );
    let path = dir.join(tmp_key.replace('/', "_"));
    match dfs
        .get_object_to_path(package_key.clone(), package_key, opts, &path)
        .await
    {
        Ok(len) => {
            let mut temp_obj = TempLocalObject::from(LocalObject {
                file: None,
                path: Arc::new(path),
                len,
            });
            temp_obj.close();
            Ok(temp_obj)
        }
        Err(err) => {
            error!(
                "failed to get archived object to file with archive addr {:?}, err {}",
                archive_addr,
                err.to_string()
            );
            Err(Error::DfsError(err))
        }
    }
}

pub fn get_archived_wal_addresses(
    store_meta: &StoreMeta,
) -> Result<Vec<(u32, Vec<ObjectAddress>)>> {
    let mut wals = Vec::with_capacity(store_meta.wal_metas.len());
    let mut wal_addrs = vec![];
    for wal_meta in &store_meta.wal_metas {
        let epoch = wal_meta.epoch_id;
        for wal_chunk_address in &wal_meta.wal_chunk_addresses {
            let key = (
                epoch,
                wal_chunk_address.package_id,
                wal_chunk_address.offset,
            );
            wal_addrs.push((key, epoch, *wal_chunk_address));
        }
    }
    wal_addrs.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let mut last_wal = (0, vec![]);
    for (_, epoch, addr) in wal_addrs {
        if epoch == last_wal.0 {
            last_wal.1.push(addr);
            continue;
        }
        if !last_wal.1.is_empty() {
            wals.push(last_wal)
        }
        last_wal = (epoch, vec![addr]);
    }
    if !last_wal.1.is_empty() {
        wals.push(last_wal)
    }
    Ok(wals)
}

pub fn get_archived_wals_from_addresses(
    dfs: Arc<dyn Dfs>,
    date: &str,
    addrs: Vec<ObjectAddress>,
    cache_dir: Option<PathBuf>,
) -> Result<Vec<WalChunkData>> {
    let runtime = dfs.get_runtime();
    let addrs_len = addrs.len();
    let mut handles = Vec::with_capacity(addrs_len);

    // There is no rate limit here, as the number of WAL chunks should be limited.
    // By default, there are 8 chunks per epoch (wal_size/wal_chunk_target_file_size
    // = 512MB/64MB).
    for addr in addrs {
        let wal_chunk_archive_addr = ArchiveAddress::new(date.to_string(), addr);
        let fs = dfs.clone();
        let dir = cache_dir.clone();
        handles.push(runtime.spawn(async move {
            if let Some(dir) = &dir {
                get_archived_object_to_file(fs.as_ref(), wal_chunk_archive_addr, dir)
                    .await
                    .map(|local_obj| WalChunkData::LocalFileWithoutMeta(local_obj))
            } else {
                get_archived_object(fs.as_ref(), wal_chunk_archive_addr)
                    .await
                    .map(|data| WalChunkData::Memory(data))
            }
        }));
    }

    let mut wal_chunks = Vec::with_capacity(addrs_len);
    let mut errs = vec![];
    // `join_all` will keep the order.
    for res in runtime.block_on(futures::future::join_all(handles)) {
        match res.unwrap() {
            Ok(chunk_data) => {
                wal_chunks.push(chunk_data);
            }
            Err(err) => {
                errs.push(err);
            }
        }
    }
    if !errs.is_empty() {
        return Err(Error::ArchiveError(format!("{:?}", errs)));
    }
    Ok(wal_chunks)
}

pub fn get_archived_wals(
    dfs: Arc<dyn Dfs>,
    date: &str,
    store_meta: &StoreMeta,
    cache_dir: Option<PathBuf>,
) -> Result<Vec<(u32, Vec<WalChunkData>)>> {
    let wal_addrs = get_archived_wal_addresses(store_meta)?;
    let mut wals = Vec::with_capacity(wal_addrs.len());
    for (epoch, addrs) in wal_addrs {
        let wal_chunks =
            get_archived_wals_from_addresses(dfs.clone(), date, addrs, cache_dir.clone())?;
        wals.push((epoch, wal_chunks));
    }
    Ok(wals)
}

pub fn get_not_found_files(dfs: Arc<dyn Dfs>, files: Vec<TableFile>) -> Result<Vec<TableFile>> {
    let (result_tx, result_rx) = tikv_util::mpsc::bounded(files.len());
    let mut not_found_files = Vec::default();
    let recv_table_file_existence = |not_found_files: &mut Vec<TableFile>,
                                     result_tx: &Receiver<dfs::Result<(TableFile, bool)>>|
     -> Result<()> {
        let (f, exist) = result_tx.recv().unwrap()?;
        if !exist {
            not_found_files.push(f);
        }
        Ok(())
    };
    let mut msg_count = 0;
    for f in files {
        let dfs_clone = dfs.clone();
        let tx = result_tx.clone();
        dfs.get_runtime().spawn(async move {
            let res = dfs_clone
                .exist(
                    dfs_clone.file_key(f.id, f.ftype),
                    format!("{}.{}", f.id, f.ftype.suffix()),
                )
                .await;
            let _ = tx.send(res.map(|exist| (f, exist)));
        });
        if msg_count < LOAD_FILE_CONCURRENCY {
            msg_count += 1;
        } else {
            recv_table_file_existence(&mut not_found_files, &result_rx)?;
        }
    }
    for _ in 0..msg_count {
        recv_table_file_existence(&mut not_found_files, &result_rx)?;
    }
    Ok(not_found_files)
}

#[derive(Default, Clone, Copy, Debug)]
pub struct ObjectAddress {
    pub package_id: u32,
    pub offset: u64,
    pub length: u64,
}

impl ObjectAddress {
    pub fn new(package_id: u32, offset: u64, length: u64) -> Self {
        Self {
            package_id,
            offset,
            length,
        }
    }

    pub fn marshal(&self, buf: &mut Vec<u8>) {
        buf.put_u32_le(self.package_id);
        buf.put_u64_le(self.offset);
        buf.put_u64_le(self.length);
    }

    pub fn unmarshal(mut buf: &[u8]) -> Self {
        let package_id = buf.get_u32_le();
        let offset = buf.get_u64_le();
        let length = buf.get_u64_le();
        Self {
            package_id,
            offset,
            length,
        }
    }
}

#[derive(Default, Clone, Debug)]
pub struct WalMeta {
    pub epoch_id: u32,
    pub wal_chunk_addresses: Vec<ObjectAddress>,
}

impl WalMeta {
    pub fn new(epoch_id: u32, wal_chunk_addresses: Vec<ObjectAddress>) -> Self {
        Self {
            epoch_id,
            wal_chunk_addresses,
        }
    }

    pub fn marshal(&self, buf: &mut Vec<u8>) {
        buf.put_u32_le(self.epoch_id);
        let num_wal_chunks = self.wal_chunk_addresses.len();
        buf.put_u32_le(num_wal_chunks as u32);
        for i in 0..num_wal_chunks {
            self.wal_chunk_addresses[i].marshal(buf);
        }
    }

    pub fn unmarshal(mut buf: &[u8]) -> Self {
        let epoch_id = buf.get_u32_le();
        let num_wal_chunks = buf.get_u32_le();
        let mut wal_chunk_addresses = Vec::with_capacity(num_wal_chunks as usize);
        for _i in 0..num_wal_chunks {
            wal_chunk_addresses.push(ObjectAddress::unmarshal(buf));
            buf.advance(OBJECT_ADDR_SIZE);
        }
        Self {
            epoch_id,
            wal_chunk_addresses,
        }
    }
}

#[derive(Default, Clone, Debug)]
pub struct StoreMeta {
    pub store_id: u64,
    pub snapshot_epoch_id: u32,
    pub snapshot_meta_address: ObjectAddress,
    pub snapshot_rlog_address: ObjectAddress,
    pub wal_metas: Vec<WalMeta>,
}

impl StoreMeta {
    pub fn new(
        store_id: u64,
        snapshot_epoch_id: u32,
        snapshot_meta_address: ObjectAddress,
        snapshot_rlog_address: ObjectAddress,
        wal_metas: Vec<WalMeta>,
    ) -> Self {
        Self {
            store_id,
            snapshot_epoch_id,
            snapshot_meta_address,
            snapshot_rlog_address,
            wal_metas,
        }
    }

    pub fn marshal(&self, buf: &mut Vec<u8>) {
        buf.put_u64_le(self.store_id);
        buf.put_u32_le(self.snapshot_epoch_id);
        self.snapshot_meta_address.marshal(buf);
        self.snapshot_rlog_address.marshal(buf);
        let num_wal_metas = self.wal_metas.len();
        buf.put_u32_le(num_wal_metas as u32);
        let length_start_off = buf.len();
        unsafe {
            buf.advance_mut(4 * num_wal_metas);
        }
        let mut offset = buf.len();
        let mut lengths = Vec::with_capacity(num_wal_metas);
        for i in 0..num_wal_metas {
            self.wal_metas[i].marshal(buf);
            lengths.push(buf.len() - offset);
            offset = buf.len();
        }
        let (_, mut length_buf) = buf.split_at_mut(length_start_off);
        for i in 0..num_wal_metas {
            length_buf.put_u32_le(lengths[i] as u32);
        }
    }

    pub fn unmarshal(mut buf: &[u8]) -> Self {
        let store_id = buf.get_u64_le();
        let snapshot_epoch_id = buf.get_u32_le();
        let snapshot_meta_address = ObjectAddress::unmarshal(buf);
        buf.advance(OBJECT_ADDR_SIZE);
        let snapshot_rlog_address = ObjectAddress::unmarshal(buf);
        buf.advance(OBJECT_ADDR_SIZE);
        let num_wal_metas = buf.get_u32_le() as usize;
        let mut lengths = Vec::with_capacity(num_wal_metas);
        for _i in 0..num_wal_metas {
            lengths.push(buf.get_u32_le() as usize);
        }
        let mut wal_metas = Vec::with_capacity(num_wal_metas);
        for i in 0..num_wal_metas {
            let wal_meta = WalMeta::unmarshal(buf);
            wal_metas.push(wal_meta);
            buf.advance(lengths[i]);
        }

        Self {
            store_id,
            snapshot_epoch_id,
            snapshot_meta_address,
            snapshot_rlog_address,
            wal_metas,
        }
    }
}

#[derive(Default)]
pub struct ArchiveIndex {
    version: u32,
    meta_address: ObjectAddress,
    store_metas: Vec<StoreMeta>,
    table_file_ids: Vec<u64>,
    table_file_addrs: Vec<ObjectAddress>,
}

#[allow(dead_code)]
impl ArchiveIndex {
    pub fn new(version: u32, meta_address: ObjectAddress) -> Self {
        Self {
            version,
            meta_address,
            store_metas: Vec::new(),
            table_file_ids: Vec::new(),
            table_file_addrs: Vec::new(),
        }
    }

    pub fn append_store_meta(&mut self, store_meta: StoreMeta) {
        self.store_metas.push(store_meta);
    }

    pub fn append_table_file_meta(&mut self, f: TableFile, file_address: ObjectAddress) {
        self.table_file_ids.push(f.id);
        self.table_file_addrs.push(file_address);
    }

    pub fn marshal(&self, buf: &mut Vec<u8>) {
        buf.put_u32_le(ARCHIVE_MAGIC_NUMBER);
        match self.version {
            ARCHIVE_INDEX_FORMAT_V1 => {
                buf.put_u32_le(ARCHIVE_INDEX_FORMAT_V1);
                self.marshal_v1(buf);
            }
            ARCHIVE_INDEX_FORMAT_V2 => {
                buf.put_u32_le(ARCHIVE_INDEX_FORMAT_V2);
                self.marshal_v2(buf);
            }
            _ => {
                panic!(
                    "archive version is not match, got {}, expect {} or {}",
                    self.version, ARCHIVE_INDEX_FORMAT_V1, ARCHIVE_INDEX_FORMAT_V2
                )
            }
        }
    }

    pub fn marshal_v1(&self, buf: &mut Vec<u8>) {
        self.meta_address.marshal(buf);
        let num_file_ids = self.table_file_ids.len();
        buf.put_u32_le(num_file_ids as u32);
        for i in 0..num_file_ids {
            buf.put_u64_le(self.table_file_ids[i]);
        }
        for i in 0..num_file_ids {
            self.table_file_addrs[i].marshal(buf);
        }
    }

    pub fn marshal_v2(&self, buf: &mut Vec<u8>) {
        self.meta_address.marshal(buf);
        let num_store_metas = self.store_metas.len();
        buf.put_u32_le(num_store_metas as u32);
        let length_start_off = buf.len();
        unsafe {
            buf.advance_mut(4 * num_store_metas);
        }
        let mut offset = buf.len();
        let mut lengths = Vec::with_capacity(num_store_metas);
        for i in 0..num_store_metas {
            self.store_metas[i].marshal(buf);
            lengths.push(buf.len() - offset);
            offset = buf.len();
        }
        let (_, mut length_buf) = buf.split_at_mut(length_start_off);
        for i in 0..num_store_metas {
            length_buf.put_u32_le(lengths[i] as u32);
        }
        let num_file_ids = self.table_file_ids.len();
        buf.put_u32_le(num_file_ids as u32);
        for i in 0..num_file_ids {
            buf.put_u64_le(self.table_file_ids[i]);
        }
        for i in 0..num_file_ids {
            self.table_file_addrs[i].marshal(buf);
        }
    }

    pub fn unmarshal(mut buf: &[u8]) -> Result<Self> {
        let magic_number = buf.get_u32_le();
        if magic_number != ARCHIVE_MAGIC_NUMBER {
            return Err(Error::ArchiveError(
                "archive magic number mismatch".to_owned(),
            ));
        }
        let version = buf.get_u32_le();
        match version {
            ARCHIVE_INDEX_FORMAT_V1 => ArchiveIndex::unmarshal_v1(buf),
            ARCHIVE_INDEX_FORMAT_V2 => ArchiveIndex::unmarshal_v2(buf),
            _ => Err(Error::ArchiveError(format!(
                "archive version is not match, got {}, expect {} or {}",
                version, ARCHIVE_INDEX_FORMAT_V1, ARCHIVE_INDEX_FORMAT_V2
            ))),
        }
    }

    pub fn unmarshal_v1(mut buf: &[u8]) -> Result<Self> {
        let meta_address = ObjectAddress::unmarshal(buf);
        buf.advance(OBJECT_ADDR_SIZE);
        let num_file_ids = buf.get_u32_le();
        let mut table_file_ids = Vec::new();
        for _i in 0..num_file_ids {
            table_file_ids.push(buf.get_u64_le());
        }
        let mut table_file_addrs = Vec::new();
        for _i in 0..num_file_ids {
            table_file_addrs.push(ObjectAddress::unmarshal(buf));
            buf.advance(OBJECT_ADDR_SIZE);
        }
        Ok(Self {
            version: ARCHIVE_INDEX_FORMAT_V1,
            meta_address,
            store_metas: Vec::new(),
            table_file_ids,
            table_file_addrs,
        })
    }

    pub fn unmarshal_v2(mut buf: &[u8]) -> Result<Self> {
        let meta_address = ObjectAddress::unmarshal(buf);
        buf.advance(OBJECT_ADDR_SIZE);
        let num_store_metas = buf.get_u32_le() as usize;
        let mut lengths = Vec::with_capacity(num_store_metas);
        for _i in 0..num_store_metas {
            lengths.push(buf.get_u32_le() as usize);
        }
        let mut store_metas = Vec::with_capacity(num_store_metas);
        for i in 0..num_store_metas {
            let store_meta = StoreMeta::unmarshal(buf);
            store_metas.push(store_meta);
            buf.advance(lengths[i]);
        }
        let num_file_ids = buf.get_u32_le();
        let mut table_file_ids = Vec::new();
        for _i in 0..num_file_ids {
            table_file_ids.push(buf.get_u64_le());
        }
        let mut table_file_addrs = Vec::new();
        for _i in 0..num_file_ids {
            table_file_addrs.push(ObjectAddress::unmarshal(buf));
            buf.advance(OBJECT_ADDR_SIZE);
        }
        Ok(Self {
            version: ARCHIVE_INDEX_FORMAT_V2,
            meta_address,
            store_metas,
            table_file_ids,
            table_file_addrs,
        })
    }
}

struct ArchiveWriter {
    max_size: u64,
    concurrency: usize,
    package_id: u32,
    index: ArchiveIndex,
    buf: Vec<u8>,
    dfs: Arc<dyn Dfs>,
    date: String,
}

impl ArchiveWriter {
    fn new(
        max_size: u64,
        concurrency: usize,
        dfs: Arc<dyn Dfs>,
        date: String,
        meta_data: Bytes,
    ) -> Self {
        let package_id: u32 = 0;
        let mut buf = Vec::new();
        let meta_address = ObjectAddress::new(package_id, buf.len() as u64, meta_data.len() as u64);
        buf.extend_from_slice(meta_data.as_bytes());

        Self {
            max_size,
            concurrency,
            package_id,
            index: ArchiveIndex::new(ARCHIVE_INDEX_FORMAT_V2, meta_address),
            buf,
            dfs,
            date,
        }
    }

    fn append_store_wal_rlog_files(&mut self, store_wal_rlog: StoreWalRlog) {
        info!("append {}", store_wal_rlog);
        let snap_meta_address = self.append_file_data(store_wal_rlog.snap_meta);
        let snap_rlog_address = self.append_file_data(store_wal_rlog.snap_rlog);
        let mut wal_metas: Vec<WalMeta> = Vec::with_capacity(store_wal_rlog.wals.len());
        for (epoch_id, wal_chunks) in store_wal_rlog.wals {
            let mut wal_chunk_addresses = Vec::with_capacity(wal_chunks.len());
            for wal_chunk in wal_chunks {
                let wal_chunk_address = self.append_file_data(wal_chunk);
                wal_chunk_addresses.push(wal_chunk_address);
            }
            let wal_meta = WalMeta::new(epoch_id, wal_chunk_addresses);
            wal_metas.push(wal_meta);
        }
        let store_meta = StoreMeta::new(
            store_wal_rlog.store_id,
            store_wal_rlog.snap_epoch,
            snap_meta_address,
            snap_rlog_address,
            wal_metas,
        );
        self.index.append_store_meta(store_meta);
    }

    fn append_table_files(&mut self, files: Vec<TableFile>) -> Result<()> {
        let (result_tx, result_rx) = tikv_util::mpsc::bounded(files.len());
        let mut msg_count = 0;
        let mut vec_files = Vec::new();
        for f in files {
            // The vec files are usually large, about several hundred MB. It is not
            // necessary to concurrently request these vec files, but concurrently
            // requesting other small files is still normal.
            if f.ftype == FileType::VectorIndex {
                vec_files.push(f);
                continue;
            }
            let dfs = self.dfs.clone();
            let tx = result_tx.clone();
            self.dfs.get_runtime().spawn(async move {
                let res = dfs
                    .read_file(f.id, Options::default().with_type(f.ftype))
                    .await;
                let _ = tx.send(res.map(|sst_data| (f, sst_data)));
            });
            if msg_count < self.concurrency {
                msg_count += 1;
            } else {
                self.recv_table_file_data(&result_rx)?;
            }
        }
        for _ in 0..msg_count {
            self.recv_table_file_data(&result_rx)?;
        }
        for f in vec_files {
            let file_data = self.dfs.get_runtime().block_on(
                self.dfs
                    .read_file(f.id, Options::default().with_type(f.ftype)),
            )?;
            self.append_table_file(f, file_data);
        }
        Ok(())
    }

    fn recv_table_file_data(
        &mut self,
        result_tx: &Receiver<dfs::Result<(TableFile, Bytes)>>,
    ) -> Result<()> {
        let (file_id, sst_data) = result_tx.recv().unwrap()?;
        self.append_table_file(file_id, sst_data);
        Ok(())
    }

    fn append_table_file(&mut self, f: TableFile, sst_data: Bytes) {
        let object_address = self.append_file_data(sst_data);
        self.index.append_table_file_meta(f, object_address);
    }

    fn append_file_data(&mut self, file_data: Bytes) -> ObjectAddress {
        if self.should_rotate() {
            self.rotate();
        }
        let object_address = ObjectAddress::new(
            self.package_id,
            self.buf.len() as u64,
            file_data.len() as u64,
        );
        self.buf.extend_from_slice(file_data.as_bytes());
        object_address
    }

    fn should_rotate(&self) -> bool {
        self.buf.len() as u64 > self.max_size
    }

    fn rotate(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        let runtime = self.dfs.get_runtime();
        let key = archive_package_key(self.dfs.get_prefix(), self.date.clone(), self.package_id);
        let data = Bytes::from(self.buf.to_vec());
        runtime
            .block_on(self.dfs.put_object_with_storage_class(
                key.clone(),
                data,
                key.clone(),
                StorageClass::GlacierInstantRetrieval,
            ))
            .unwrap();
        info!("cluster archive package {} on {}", key, self.date.clone());
        self.package_id += 1;
        self.buf.truncate(0);
    }

    fn finish(&mut self) {
        self.rotate();
        self.index.marshal(&mut self.buf);
        let runtime = self.dfs.get_runtime();
        let key = archive_index_key(self.dfs.get_prefix(), self.date.clone());
        let data = Bytes::from(self.buf.to_vec());
        runtime
            .block_on(self.dfs.put_object(key.clone(), data, key.clone()))
            .unwrap();
        info!("cluster archive index {} on {}", key, self.date.clone());
    }
}

#[derive(Default, Clone, Debug)]
pub struct ArchiveAddress {
    date: String,
    object_addr: ObjectAddress,
}

impl ArchiveAddress {
    pub fn new(date: String, object_addr: ObjectAddress) -> Self {
        Self { date, object_addr }
    }
}

pub struct ArchiveReader {
    start_date: String,
    meta_archive_address: Option<ArchiveAddress>,
    store_metas: Vec<StoreMeta>,
    archive_addresses: HashMap<u64, ArchiveAddress>,
    dfs: Arc<dyn Dfs>,
}

impl ArchiveReader {
    pub fn new(dfs: Arc<dyn Dfs>, date: &NaiveDate) -> Result<Self> {
        let start_date = archive_format_date(date);
        let (index_keys, _) = dfs.get_runtime().block_on(get_all_archive_index_paths(
            dfs.as_ref(),
            start_date.clone(),
            usize::MAX,
        ))?;
        if index_keys.is_empty() {
            return Err(Error::ArchiveError(format!(
                "failed to get archive reader on {}",
                start_date,
            )));
        }
        let mut meta_archive_address: Option<ArchiveAddress> = None;
        let mut store_metas: Vec<StoreMeta> = Vec::new();
        let mut archive_addresses: HashMap<u64, ArchiveAddress> = HashMap::default();
        for index_key in index_keys {
            let date = parse_index_date(&index_key.clone());
            let (archive_index, _) = get_archive_index(dfs.as_ref(), date.clone())?;
            if date.eq(&start_date) {
                meta_archive_address = Some(ArchiveAddress::new(
                    date.clone(),
                    archive_index.meta_address,
                ));
                store_metas = archive_index.store_metas;
            }

            let num_file_ids = archive_index.table_file_addrs.len();
            for i in 0..num_file_ids {
                archive_addresses.insert(
                    archive_index.table_file_ids[i],
                    ArchiveAddress::new(date.clone(), archive_index.table_file_addrs[i]),
                );
            }
        }
        Ok(Self {
            start_date,
            meta_archive_address,
            store_metas,
            archive_addresses,
            dfs,
        })
    }

    pub fn get_start_date(&self) -> &str {
        &self.start_date
    }

    pub fn get_meta_archive_addr(&self) -> Option<ArchiveAddress> {
        self.meta_archive_address.clone()
    }

    pub fn read_meta_file(&self) -> Result<ClusterBackupMeta> {
        if let Some(archive_addr) = self.get_meta_archive_addr() {
            return self
                .dfs
                .get_runtime()
                .block_on(get_archived_object(self.dfs.as_ref(), archive_addr))
                .and_then(|data| {
                    let mut cluster_backup_meta = ClusterBackupMeta::new();
                    cluster_backup_meta.merge_from_bytes(&data).map_err(|e| {
                        Error::ArchiveError(format!(
                            "failed to decode cluster backup meta on {}, err {}",
                            self.start_date, e,
                        ))
                    })?;
                    Ok(cluster_backup_meta)
                });
        }
        Err(Error::ArchiveError(format!(
            "failed to find archive meta file on {}",
            self.start_date
        )))
    }

    pub fn get_store_wal_rlog_meta(&self, store_id: u64) -> Result<StoreMeta> {
        self.store_metas
            .iter()
            .find(|x| x.store_id == store_id)
            .cloned()
            .ok_or(Error::ArchiveError(format!(
                "failed to find archive store {} wal rlog files on {}, store_metas {}",
                store_id,
                self.start_date,
                self.store_metas.len()
            )))
    }

    pub fn read_store_rlog_files(
        dfs: &dyn Dfs,
        date: &str,
        store_meta: &StoreMeta,
    ) -> Result<StoreRlog> {
        let snap_epoch = store_meta.snapshot_epoch_id;
        let meta_archive_addr =
            ArchiveAddress::new(date.to_string(), store_meta.snapshot_meta_address);
        let snap_meta = dfs
            .get_runtime()
            .block_on(get_archived_object(dfs, meta_archive_addr))?;
        let rlog_archive_addr =
            ArchiveAddress::new(date.to_string(), store_meta.snapshot_rlog_address);
        let snap_rlog = dfs
            .get_runtime()
            .block_on(get_archived_object(dfs, rlog_archive_addr))?;
        let store_rlog = StoreRlog {
            store_id: store_meta.store_id,
            snap_epoch,
            snap_meta,
            snap_rlog,
        };
        info!("read rlog files done: {:?}", store_rlog);
        Ok(store_rlog)
    }

    pub fn get_file_archive_addr(&self, file_id: u64) -> Option<ArchiveAddress> {
        self.archive_addresses.get(&file_id).cloned()
    }

    pub fn read_file(&self, file_id: u64) -> Result<Bytes> {
        if let Some(archive_addr) = self.get_file_archive_addr(file_id) {
            return self
                .dfs
                .get_runtime()
                .block_on(get_archived_object(self.dfs.as_ref(), archive_addr.clone()))
                .inspect_err(|e| {
                    error!(
                        "{}, file id {}, archive addr {:?}",
                        e.to_string(),
                        file_id,
                        archive_addr
                    );
                });
        }
        Err(Error::ArchiveError(format!(
            "failed to find archive file {}",
            file_id,
        )))
    }

    pub fn restore_file(&self, f: TableFile) -> Result<()> {
        let date = self.read_file(f.id)?;
        self.dfs
            .get_runtime()
            .block_on(
                self.dfs
                    .create(f.id, date, Options::default().with_type(f.ftype)),
            )
            .map_err(|e| {
                error!("failed to restore archive file {:?}, err {:?}", f, e);
                Error::DfsError(e)
            })
    }

    pub fn restore_files(&self, files: Vec<TableFile>) -> Result<()> {
        let mut archive_addrs = Vec::default();
        for f in files {
            if let Some(archive_addr) = self.get_file_archive_addr(f.id) {
                archive_addrs.push((f, archive_addr));
            } else {
                return Err(Error::ArchiveError(format!(
                    "failed to find archive file {:?}",
                    f,
                )));
            }
        }
        let recv_restore_file = |result_tx: &Receiver<Result<TableFile>>| -> Result<()> {
            let f = result_tx.recv().unwrap()?;
            info!("restore archived table file {:?} done", f);
            Ok(())
        };
        let (result_tx, result_rx) = tikv_util::mpsc::bounded(archive_addrs.len());
        let mut msg_count = 0;
        for (f, archive_addr) in archive_addrs {
            let dfs = self.dfs.clone();
            let tx = result_tx.clone();
            self.dfs.get_runtime().spawn(async move {
                let res = get_archived_object(dfs.as_ref(), archive_addr.clone())
                    .await
                    .map_err(|e| {
                        error!(
                            "get archived object failed: {:?}, file {:?}, archive addr {:?}",
                            e, f, archive_addr
                        );
                        e
                    });
                if res.is_err() {
                    let _ = tx.send(res.map(|_| f));
                    return;
                }
                let data = res.unwrap();
                let res = dfs
                    .create(f.id, data, Options::default().with_type(f.ftype))
                    .await
                    .map_err(|e| {
                        error!(
                            "create file failed: {:?}, file {:?}, archive addr {:?}",
                            e, f, archive_addr
                        );
                        Error::DfsError(e)
                    });
                let _ = tx.send(res.map(|_| f));
            });
            if msg_count < LOAD_FILE_CONCURRENCY {
                msg_count += 1;
            } else {
                recv_restore_file(&result_rx)?;
            }
        }
        for _ in 0..msg_count {
            recv_restore_file(&result_rx)?;
        }
        Ok(())
    }

    pub fn get_file_ids(&self) -> Vec<u64> {
        self.archive_addresses.keys().copied().collect::<Vec<_>>()
    }
}

pub fn archive_format_date(date: &chrono::NaiveDate) -> String {
    date.format(INCREMENTAL_BACKUP_FOLDER_FORMAT).to_string()
}

pub fn archive_index_key(prefix: String, date: String) -> String {
    format!("{}/archive/index/{}.idx", prefix, date)
}

pub fn archive_package_key(prefix: String, date: String, package_id: u32) -> String {
    format!(
        "{}/archive/package/{}/{:08x}.pack",
        prefix, date, package_id
    )
}

pub fn parse_index_date(index_key: &str) -> String {
    let end_idx = index_key.len() - 4;
    let start_idx = end_idx - 8;
    let data_part = &index_key[start_idx..end_idx];
    data_part.to_string()
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Read};

    use bytes::Bytes;
    use protobuf::Message;
    use rfenginepb::{ClusterBackupMeta, StoreBackupMeta};
    use test_cloud_server::oss::prepare_dfs;

    use super::*;

    #[test]
    fn test_archive_writer() {
        test_util::init_log_for_test();

        const CLUSTER_ID: u64 = 1;
        const NUM_STORES: u64 = 4;
        const NUM_FILE_IDS: u64 = 24;

        let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test_archive_writer_");
        let dfs = new_dfs_from_config(dfs_config);
        let get_file_id = |i: u64| i;
        let get_file_data =
            |file_id: u64| Bytes::from(b"x".repeat(100 + file_id as usize).to_vec());
        dfs.get_runtime().block_on(async {
            for i in 0..NUM_FILE_IDS {
                let file_id = get_file_id(i);
                let opts = dfs::Options::default().with_type(get_file_type(file_id));
                let sst_data = get_file_data(file_id);
                dfs.create(file_id, sst_data, opts).await.unwrap();
            }
        });

        let mut cluster_meta = ClusterBackupMeta::new();
        cluster_meta.set_cluster_id(CLUSTER_ID);
        for i in 0..NUM_STORES {
            cluster_meta.mut_stores().push(StoreBackupMeta {
                store_id: i,
                ..Default::default()
            });
        }
        assert_eq!(cluster_meta.stores.len(), NUM_STORES as usize);
        let meta_data = Bytes::from(cluster_meta.write_to_bytes().unwrap());
        assert!(!meta_data.is_empty());
        let backup_date = chrono::Utc::now().date_naive();
        let format_date = archive_format_date(&backup_date);
        let mut writer = ArchiveWriter::new(1024, 16, dfs.clone(), format_date.clone(), meta_data);

        let get_snap_epoch = |i: u64| (i + 10) as u32;
        let get_wal_epoch = |i: u64| (i + 11) as u32;
        for i in 0..NUM_STORES {
            let store_id = i;
            let snap_epoch = get_snap_epoch(store_id);
            let mut store_backup_meta = StoreBackupMeta::default();
            store_backup_meta.set_store_id(store_id);
            store_backup_meta.set_epoch(snap_epoch);
            let snap_meta = Bytes::from(store_backup_meta.write_to_bytes().unwrap());
            let snap_rlog = Bytes::from(format!("store:{}", store_id));
            let wal_epoch = get_wal_epoch(store_id);
            let wals = vec![(
                wal_epoch,
                vec![Bytes::from(format!("wal_epoch:{}", wal_epoch))],
            )];
            let store_wal_rlog_files =
                StoreWalRlog::new(store_id, snap_epoch, snap_meta, snap_rlog, wals);
            writer.append_store_wal_rlog_files(store_wal_rlog_files)
        }

        let mut files = Vec::with_capacity(NUM_FILE_IDS as usize);
        for file_id in 0..NUM_FILE_IDS {
            files.push(TableFile {
                id: file_id,
                ftype: get_file_type(file_id),
                shard_id: 0,
            });
        }
        writer.append_table_files(files).unwrap();
        writer.finish();
        let num_packages = writer.package_id;
        dfs.get_runtime().block_on(async {
            let (objects, ..) = dfs.list("", None, None).await.unwrap();
            assert_eq!(
                objects.len(),
                NUM_FILE_IDS as usize + 1 /* archive_index */ + num_packages as usize
            );
        });
        let (archive_index, data) = get_archive_index(dfs.as_ref(), format_date.clone()).unwrap();
        assert!(!data.is_empty());
        assert_eq!(archive_index.version, ARCHIVE_INDEX_FORMAT_V2);
        assert_eq!(archive_index.store_metas.len(), NUM_STORES as usize);
        assert_eq!(archive_index.table_file_ids.len(), NUM_FILE_IDS as usize);
        {
            let meta_address = archive_index.meta_address;
            let archive_address = ArchiveAddress::new(format_date.clone(), meta_address);
            let data = dfs
                .get_runtime()
                .block_on(get_archived_object(dfs.as_ref(), archive_address))
                .unwrap();
            let mut cluster_backup = ClusterBackupMeta::new();
            cluster_backup.merge_from_bytes(&data).unwrap();
            assert_eq!(cluster_backup.cluster_id, CLUSTER_ID);
            assert_eq!(cluster_backup.stores.len(), NUM_STORES as usize);
            for i in 0..NUM_STORES {
                assert_eq!(cluster_backup.stores[i as usize].store_id, i);
            }
        }

        for store_meta in &archive_index.store_metas {
            assert_eq!(
                store_meta.snapshot_epoch_id,
                get_snap_epoch(store_meta.store_id)
            );
            assert_eq!(store_meta.wal_metas.len(), 1);
            assert_eq!(
                store_meta.wal_metas[0].epoch_id,
                get_wal_epoch(store_meta.store_id)
            );
        }

        for i in 0..NUM_FILE_IDS as usize {
            let file_id = archive_index.table_file_ids[i];
            let address = archive_index.table_file_addrs[i];
            let archive_address = ArchiveAddress::new(format_date.clone(), address);
            let data = dfs
                .get_runtime()
                .block_on(get_archived_object(dfs.as_ref(), archive_address))
                .unwrap();
            assert_eq!(data.len(), address.length as usize);
            let file_data = get_file_data(file_id);
            assert!(data.eq(&file_data));
        }

        oss.shutdown();
    }

    #[test]
    fn test_archive_reader() {
        test_util::init_log_for_test();

        const CLUSTER_ID: u64 = 1;
        const NUM_DATES: u64 = 8;

        let (temp_dir, mut oss, dfs_config) = prepare_dfs("test_archive_reader_");
        let cache_dir = temp_dir.path().join("cache");
        fs::create_dir_all(cache_dir.clone()).unwrap();
        let dfs = new_dfs_from_config(dfs_config);
        let first_date = chrono::Utc::now().date_naive() - chrono::Duration::days(NUM_DATES as i64);
        let get_date = |j: u64| first_date + chrono::Duration::days(j as i64);
        let get_num_stores = |j: u64| j + 8;
        let get_snap_epoch = |j: u64| (j + 10) as u32;
        let get_snap_rlog = |j: u64| Bytes::from(b"r".repeat(j as usize + 10).to_vec());
        let get_wal_epoch = |j: u64| (j + 11) as u32;
        let get_wal_chunk = |j: u64| Bytes::from(b"r".repeat(j as usize + 11).to_vec());
        let get_num_file_ids = |j: u64| j + 3;
        let get_file_id = |j: u64, i: u64| j * 1000 + i + 200;
        let get_file_data =
            |file_id: u64| Bytes::from(b"x".repeat(file_id as usize % 1000).to_vec());
        for j in 0..NUM_DATES {
            let num_file_ids = get_num_file_ids(j);
            dfs.get_runtime().block_on(async {
                for i in 0..num_file_ids {
                    let file_id = get_file_id(j, i);
                    let opts = dfs::Options::default().with_type(get_file_type(file_id));
                    let sst_data = get_file_data(file_id);
                    dfs.create(file_id, sst_data, opts).await.unwrap();
                }
            });
            let mut cluster_meta = ClusterBackupMeta::new();
            cluster_meta.set_cluster_id(CLUSTER_ID);
            let num_stores = get_num_stores(j);
            for i in 0..num_stores {
                cluster_meta.mut_stores().push(StoreBackupMeta {
                    store_id: i,
                    ..Default::default()
                });
            }
            let meta_data = Bytes::from(cluster_meta.write_to_bytes().unwrap());
            let backup_date = get_date(j);
            let format_date = archive_format_date(&backup_date);
            let mut writer =
                ArchiveWriter::new(512, 16, dfs.clone(), format_date.clone(), meta_data);

            for i in 0..num_stores {
                let store_id = i;
                let snap_epoch = get_snap_epoch(store_id);
                let mut store_backup_meta = StoreBackupMeta::default();
                store_backup_meta.set_store_id(store_id);
                let snap_meta = Bytes::from(store_backup_meta.write_to_bytes().unwrap());
                let snap_rlog = get_snap_rlog(store_id);
                let wal_epoch = get_wal_epoch(store_id);
                let wals = vec![(wal_epoch, vec![get_wal_chunk(store_id)])];
                let store_wal_rlog_files =
                    StoreWalRlog::new(store_id, snap_epoch, snap_meta, snap_rlog, wals);
                writer.append_store_wal_rlog_files(store_wal_rlog_files)
            }

            let mut files = Vec::with_capacity(num_file_ids as usize);
            for i in 0..num_file_ids {
                let file_id = get_file_id(j, i);
                files.push(TableFile {
                    id: file_id,
                    ftype: get_file_type(get_file_id(j, i)),
                    shard_id: 0,
                });
            }
            writer.append_table_files(files).unwrap();
            writer.finish();
        }
        let start_date = get_date(0);
        let reader = ArchiveReader::new(dfs, &start_date).unwrap();
        let num_stores = get_num_stores(0);
        {
            let cluster_backup_meta = reader.read_meta_file().unwrap();
            assert_eq!(cluster_backup_meta.cluster_id, CLUSTER_ID);
            assert_eq!(cluster_backup_meta.stores.len(), num_stores as usize);
            for i in 0..num_stores {
                assert_eq!(cluster_backup_meta.stores[i as usize].store_id, i);
            }
        }
        {
            assert_eq!(reader.store_metas.len(), num_stores as usize);
            for i in 0..num_stores {
                let store_id = i;
                let store_meta = reader.get_store_wal_rlog_meta(store_id).unwrap();
                let store_rlog = ArchiveReader::read_store_rlog_files(
                    reader.dfs.as_ref(),
                    reader.get_start_date(),
                    &store_meta,
                )
                .unwrap();
                assert_eq!(store_rlog.snap_epoch, get_snap_epoch(store_id));
                let mut store_backup_meta = StoreBackupMeta::default();
                store_backup_meta
                    .merge_from_bytes(store_rlog.snap_meta.chunk())
                    .unwrap();
                assert_eq!(store_backup_meta.store_id, store_id);
                assert_eq!(store_rlog.snap_rlog, get_snap_rlog(store_id));

                {
                    let wals = get_archived_wals(
                        reader.dfs.clone(),
                        reader.get_start_date(),
                        &store_meta,
                        None,
                    )
                    .unwrap();
                    assert_eq!(wals.len(), 1);
                    assert_eq!(wals[0].0, get_wal_epoch(store_id));
                    assert_eq!(wals[0].1[0].must_get_bytes(), get_wal_chunk(store_id));
                }
                {
                    let wals = get_archived_wals(
                        reader.dfs.clone(),
                        reader.get_start_date(),
                        &store_meta,
                        Some(cache_dir.clone()),
                    )
                    .unwrap();
                    assert_eq!(wals.len(), 1);
                    assert_eq!(wals[0].0, get_wal_epoch(store_id));
                    let mut buffer = Vec::new();
                    let mut local_obj = wals[0].1[0].must_into_local_file_without_meta();
                    local_obj
                        .file(true)
                        .unwrap()
                        .read_to_end(&mut buffer)
                        .unwrap();
                    assert_eq!(buffer, get_wal_chunk(store_id));
                }
            }
        }
        for j in 0..NUM_DATES {
            let num_file_ids = get_num_file_ids(j);
            for i in 0..num_file_ids {
                let file_id = get_file_id(j, i);
                let data = reader.read_file(file_id).unwrap();
                let file_data = get_file_data(file_id);
                assert!(data.eq(&file_data));
            }
        }

        oss.shutdown();
    }

    #[test]
    fn test_get_all_archive_index_paths() {
        test_util::init_log_for_test();

        const NUM_INDEXES: i64 = 3;

        let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test_get_all_archive_index_paths_");
        let dfs = new_dfs_from_config(dfs_config);
        let first_date = chrono::Utc::now().date_naive() - chrono::Duration::days(NUM_INDEXES);
        let get_date = |i: i64| first_date + chrono::Duration::days(i);
        dfs.get_runtime().block_on(async {
            for i in 0..NUM_INDEXES {
                let date = get_date(i);
                let format_date = archive_format_date(&date);
                let key = archive_index_key(dfs.get_prefix(), format_date);
                let sst_data = Bytes::from(b"x".repeat(100 + i as usize).to_vec());
                dfs.put_object(key.clone(), sst_data, key.clone())
                    .await
                    .unwrap();
            }
            for i in 0..NUM_INDEXES {
                let date = get_date(i);
                let start_date = archive_format_date(&date);
                let (index_keys, _) =
                    get_all_archive_index_paths(dfs.as_ref(), start_date, usize::MAX)
                        .await
                        .unwrap();
                assert_eq!(index_keys.len(), (NUM_INDEXES - i) as usize);
            }
        });

        oss.shutdown();
    }

    fn get_file_type(file_id: u64) -> FileType {
        FileType::from_u8(file_id as u8 % 6).unwrap()
    }
}
