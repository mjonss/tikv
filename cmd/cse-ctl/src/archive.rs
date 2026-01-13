// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{path::PathBuf, str::FromStr, sync::Arc, time::Duration};

use chrono::NaiveDate;
use clap::Args;
use kvengine::{
    dfs::{DFSConfig, S3Fs},
    table::{
        blobtable::blobtable::BlobTable,
        file::InMemFile,
        sstable::{BlockCache, L0Table, SsTable},
    },
};
use native_br::{
    archive::{
        ArchiveConfig, ArchiveReader, DEFAULT_MAX_ARCHIVE_FILE_SIZE, LOAD_FILE_CONCURRENCY,
        archive_with_cfg,
    },
    common::INCREMENTAL_BACKUP_FOLDER_FORMAT,
};
use tikv_util::{config::ReadableDuration, info};

use crate::{
    backup::ShowBackupConfig,
    sst::{print_blob_table, print_l0_table, print_sstable},
};

#[derive(Args)]
pub struct ArchiveArgs {
    /// PD endpoints, use `,` to separate multiple PDs
    #[clap(long, default_value_t = String::new())]
    pub pd: String,
    /// Path of file that contains list of trusted SSL CAs
    #[clap(long, default_value = "")]
    pub cacert: PathBuf,
    /// Path of file that contains X509 certificate in PEM format
    #[clap(long, default_value = "")]
    pub cert: PathBuf,
    /// Path of file that contains X509 key in PEM format
    #[clap(long, default_value = "")]
    pub key: PathBuf,
    /// Data dir for load_data
    #[clap(long, default_value_t = String::new())]
    pub data_dir: String,
    #[clap(long, default_value_t = DEFAULT_MAX_ARCHIVE_FILE_SIZE)]
    pub max_archive_file_size: u64,
    /// Start archive duration, in duration string (see `ReadableDuration`).
    /// `start_archive_time` = now() - `start-archive-duration`.
    #[clap(long, default_value = "91d")]
    pub start_archive_duration: ReadableDuration,
    /// The expiration date of backups. The date format is
    /// "%Y%m%d". e.g. 20060102
    #[clap(long, default_value_t = String::new())]
    pub expiration_date: String,
    /// Concurrently do s3 requests.
    #[clap(long, default_value_t = LOAD_FILE_CONCURRENCY)]
    pub concurrency: usize,
    /// Concurrency on number of TiKV stores when perform restoration.
    #[clap(long, default_value_t = 3)]
    pub store_concurrency: usize,
    /// Skip days without cluster backup meta.
    #[clap(long, default_value_t = 0)]
    pub skip_no_meta_days: usize,
    /// Skip abnormal shards, use `,` to separate multiple shard ids
    #[clap(long)]
    pub skip_abnormal_shards: Option<String>,
    /// Skip keyspace names, use `,` to separate multiple keyspace names
    #[clap(long)]
    pub skip_keyspace_names: Option<String>,
    #[clap(long)]
    pub dry_run: bool,
    /// The timeout for fetching WAL chunks.
    #[clap(long, default_value = "10m")]
    pub fetch_wal_timeout: ReadableDuration,
    /// File types blacklist no need to archive, use `,` to separate multiple
    /// file types.
    #[clap(long, value_delimiter = ',', default_value = "col,vec")]
    pub file_types_blacklist: Vec<String>,
}

pub fn execute_archive(args: ArchiveArgs) {
    let config: ArchiveConfig = get_archive_config_from_args(&args);
    info!("Begin archive with config {:?}", config);
    if let Err(e) = archive_with_cfg(config) {
        panic!("failed to archive cluster backup, err {:?}", e)
    }
}

fn get_archive_config_from_args(args: &ArchiveArgs) -> ArchiveConfig {
    let mut config = ArchiveConfig::default();
    // override from args and ENV
    if !args.pd.is_empty() {
        config.pd.endpoints = args.pd.split(',').map(|x| x.to_owned()).collect();
    }
    if args.cacert.exists() {
        config.security.ca_path = args.cacert.to_str().unwrap().to_owned();
    }
    if args.cert.exists() {
        config.security.cert_path = args.cert.to_str().unwrap().to_owned();
    }
    if args.key.exists() {
        config.security.key_path = args.key.to_str().unwrap().to_owned();
    }
    config.skip_shards = args.skip_abnormal_shards.as_ref().map(|skip_shards_str| {
        skip_shards_str
            .split(',')
            .map(|x| u64::from_str(x).unwrap())
            .collect()
    });
    config.skip_keyspace_names = args
        .skip_keyspace_names
        .as_ref()
        .map(|skip_keyspace_names_str| {
            skip_keyspace_names_str
                .split(',')
                .map(|x| x.to_string())
                .collect()
        });
    config.max_archive_file_size = args.max_archive_file_size;
    config.start_archive_duration = Duration::from(args.start_archive_duration);
    config.expiration_date = args.expiration_date.clone();
    config.concurrency = args.concurrency;
    config.store_concurrency = args.store_concurrency;
    config.skip_no_meta_days = args.skip_no_meta_days;
    config.dry_run = args.dry_run;
    config.dfs.override_from_env();
    config.security.override_from_env();
    config.data_dir = args.data_dir.clone();
    config.fetch_wal_timeout = args.fetch_wal_timeout.0;
    config.file_types_blacklist = args.file_types_blacklist.clone();
    config.check_data_dir();
    config
}

const BLOB_LEVEL_FLAG: u32 = 255;

#[derive(Args)]
pub struct ShowArchiveArgs {
    /// The path of the config file.
    #[clap(long, default_value = "")]
    pub config: PathBuf,
    /// The name of the archive file.
    #[clap(long)]
    pub date: String,
    /// The id of th sst file in the archive.
    /// If not specified, it shows all the ids in the archive file.
    /// If specified, it shows the stats of the file and validate it.
    #[clap(long)]
    pub file_id: Option<u64>,
    /// The level of the sst file in the archive.
    /// If not specified, level one is
    #[clap(long)]
    pub file_level: Option<u32>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ShowArchiveConfig {
    pub dfs: DFSConfig,
}

pub fn execute_show_archive(args: ShowArchiveArgs) {
    let mut config = ShowBackupConfig::default();
    if args.config.exists() {
        let data = std::fs::read(args.config.clone()).expect("failed to read config file");
        config = toml::from_slice(&data).unwrap();
    }
    config.dfs.override_from_env();
    let s3fs = Arc::new(S3Fs::new_from_config(config.dfs));
    let date = NaiveDate::parse_from_str(&args.date, INCREMENTAL_BACKUP_FOLDER_FORMAT).unwrap();
    let reader = ArchiveReader::new(s3fs, &date).unwrap();
    if let Some(id) = args.file_id {
        let data = reader.read_file(id).unwrap();
        let file = Arc::new(InMemFile::new(id, data));
        let level = args.file_level.unwrap_or(1);
        if level == 0 {
            let l0 = L0Table::new(file, BlockCache::None, false, None)
                .unwrap()
                .unwrap();
            print_l0_table(&l0);
        } else if level == BLOB_LEVEL_FLAG {
            let blob = BlobTable::new(file).unwrap();
            print_blob_table(&blob);
        } else {
            let ln = SsTable::new(file, BlockCache::None, None).unwrap();
            println!("[SST {}, level {}]", ln.id(), level);
            print_sstable(&ln, 2);
        }
    } else {
        println!("archive file ids: {:?}", reader.get_file_ids());
    }
}
