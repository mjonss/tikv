// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    fmt::{Debug, Formatter},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Buf;
use chrono::DateTime;
use clap::Args;
use kvengine::dfs::{
    DFSConfig, Dfs, FileType, OSS_STORAGE_CLASS_ARCHIVE, OSS_STORAGE_CLASS_IA,
    OSS_STORAGE_CLASS_STANDARD, STORAGE_CLASS_GLACIER_IR, STORAGE_CLASS_INTELLIGENT_TIERING,
    STORAGE_CLASS_STANDARD, STORAGE_CLASS_STANDARD_IA, new_dfs_from_config, try_parse_all_file_id,
};
use kvproto::metapb::Store;
use native_br::{
    common::create_pd_client,
    error::{Error, Result},
};
use pd_client::{RpcClient, util::get_all_stores_except_tiflash};
use security::{GetSecurityManager, SecurityConfig, SecurityManager};
use tikv_util::{box_err, error, info};
use tokio::sync::{Semaphore, mpsc::Sender};

pub const STORAGE_SIZE_KB: f64 = 1024f64;
pub const STORAGE_SIZE_MB: f64 = 1024f64 * 1024f64;
pub const STORAGE_SIZE_GB: f64 = 1024f64 * 1024f64 * 1024f64;
pub const STORAGE_SIZE_TB: f64 = 1024f64 * 1024f64 * 1024f64 * 1024f64;
pub const STORAGE_SIZE_PB: f64 = 1024f64 * 1024f64 * 1024f64 * 1024f64 * 1024f64;

pub fn format_size(size: u64) -> String {
    let size = size as f64;
    if size > STORAGE_SIZE_PB {
        format!("{:.1} PB", size / STORAGE_SIZE_PB)
    } else if size > STORAGE_SIZE_TB {
        format!("{:.1} TB", size / STORAGE_SIZE_TB)
    } else if size > STORAGE_SIZE_GB {
        format!("{:.1} GB", size / STORAGE_SIZE_GB)
    } else if size > STORAGE_SIZE_MB {
        format!("{:.1} MB", size / STORAGE_SIZE_MB)
    } else if size > STORAGE_SIZE_KB {
        format!("{:.1} KB", size / STORAGE_SIZE_KB)
    } else {
        format!("{:.0} B", size)
    }
}

#[derive(Args)]
pub struct StatsArgs {
    /// The path of the config file.
    #[clap(long, default_value = "")]
    pub config: PathBuf,

    /// Concurrently do s3 requests.
    #[clap(long, default_value = "32")]
    pub concurrency: usize,

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
    /// Folders, use `,` to separate multiple folders. For example,
    /// archive,backup,col,schema,store_backup,txn,vec
    #[clap(long)]
    pub folders: Option<String>,
}

pub(crate) fn execute_stats(arg: StatsArgs) {
    let config = StatsConfig::from_args(&arg);

    let pd_client = Arc::new(create_pd_client(&config.security, &config.pd));
    let dfs = new_dfs_from_config(config.dfs);

    let mut stats_worker = StatsWorker::new(pd_client, dfs, arg.concurrency);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut stats = || -> Result<()> {
        stats_worker.collect_valid_files()?;
        runtime.block_on(stats_worker.stats_files(&config.folders))?;
        Ok(())
    };
    if let Err(err) = stats() {
        panic!("execute stats error: {:?}", err);
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct StatsConfig {
    pub pd: pd_client::Config,
    pub security: SecurityConfig,
    pub dfs: DFSConfig,
    pub folders: Option<HashSet<String>>,
}

impl StatsConfig {
    pub fn from_args(args: &StatsArgs) -> Self {
        let mut config = Self::default();
        if args.config.exists() {
            let data = std::fs::read(args.config.as_path()).expect("failed to read config file");
            config = toml::from_slice(&data).unwrap();
        }
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
        config.dfs.override_from_env();
        config.security.override_from_env();
        config.folders = args
            .folders
            .as_ref()
            .map(|folders_str| folders_str.split(',').map(|x| format!("{}/", x)).collect());

        config
    }
}

#[derive(Clone)]
struct StatsWorker {
    pd: Arc<RpcClient>,
    dfs: Arc<dyn Dfs>,
    valid_files: Arc<HashSet<u64>>,
    concurrency: usize,
}

#[derive(Default, Clone)]
struct StatContent {
    num_of_objs: u64,
    storage: u64,
}

impl StatContent {
    fn add(&mut self, storage: u64) {
        self.num_of_objs += 1;
        self.storage += storage;
    }

    fn append(&mut self, stat_content: StatContent) {
        self.num_of_objs += stat_content.num_of_objs;
        self.storage += stat_content.storage;
    }
}

impl fmt::Display for StatContent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "[{} / {}]", self.num_of_objs, format_size(self.storage))
    }
}

#[derive(Default, Clone)]
struct ObjectStat {
    checked: StatContent,
    in_a_day: StatContent,
    in_a_week: StatContent,
    in_a_month: StatContent,
    in_three_months: StatContent,
    more_than_three_months: StatContent,
}

impl ObjectStat {
    fn add(&mut self, duration: chrono::Duration, storage: u64) {
        if duration < chrono::Duration::days(1) {
            self.in_a_day.add(storage);
        }
        if duration < chrono::Duration::days(7) {
            self.in_a_week.add(storage);
        }
        if duration < chrono::Duration::days(30) {
            self.in_a_month.add(storage);
        }
        if duration < chrono::Duration::days(90) {
            self.in_three_months.add(storage);
        } else {
            self.more_than_three_months.add(storage);
        }
        self.checked.add(storage);
    }

    fn append(&mut self, object_stat: ObjectStat) {
        self.in_a_day.append(object_stat.in_a_day);
        self.in_a_week.append(object_stat.in_a_week);
        self.in_a_month.append(object_stat.in_a_month);
        self.in_three_months.append(object_stat.in_three_months);
        self.more_than_three_months
            .append(object_stat.more_than_three_months);
        self.checked.append(object_stat.checked);
    }
}

impl fmt::Display for ObjectStat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} checked, {} in a day, {} in a week, {} in a month, {} in three months, {} more than three months",
            self.checked,
            self.in_a_day,
            self.in_a_week,
            self.in_a_month,
            self.in_three_months,
            self.more_than_three_months,
        )
    }
}

#[derive(Default, Clone)]
struct Stats {
    all_stat: ObjectStat,
    file_stat: HashMap<FileType, ObjectStat>,
    in_used_file_stat: HashMap<FileType, ObjectStat>,
    wal_stat: ObjectStat,
    rlog_stat: ObjectStat,
    meta_stat: ObjectStat,
    pack_stat: ObjectStat,
    // S3 storage classes stats
    intelligent_tiering_stat: ObjectStat,
    standard_stat: ObjectStat,
    standard_ia_stat: ObjectStat,
    glacier_ir_stat: ObjectStat,
}

impl Stats {
    fn append(&mut self, mut stats: Stats) {
        self.all_stat.append(stats.all_stat);
        for (ftype, stat) in stats.file_stat.drain() {
            self.file_stat.entry(ftype).or_default().append(stat);
        }
        for (ftype, stat) in stats.in_used_file_stat.drain() {
            self.in_used_file_stat
                .entry(ftype)
                .or_default()
                .append(stat);
        }
        self.wal_stat.append(stats.wal_stat);
        self.rlog_stat.append(stats.rlog_stat);
        self.meta_stat.append(stats.meta_stat);
        self.pack_stat.append(stats.pack_stat);
        self.intelligent_tiering_stat
            .append(stats.intelligent_tiering_stat);
        self.standard_stat.append(stats.standard_stat);
        self.standard_ia_stat.append(stats.standard_ia_stat);
        self.glacier_ir_stat.append(stats.glacier_ir_stat);
    }
}

impl StatsWorker {
    fn new(pd: Arc<RpcClient>, dfs: Arc<dyn Dfs>, concurrency: usize) -> Self {
        Self {
            pd,
            dfs,
            valid_files: Arc::new(HashSet::default()),
            concurrency,
        }
    }

    fn collect_valid_files(&mut self) -> Result<()> {
        let all_stores = get_all_stores_except_tiflash(self.pd.as_ref())?;
        let start_time = Instant::now();
        let stores_len = all_stores.len();
        let (tx, rx) = std::sync::mpsc::sync_channel(stores_len);
        for store in all_stores {
            let tx = tx.clone();
            let security_mgr = self.pd.get_security_mgr().clone();
            self.dfs.get_runtime().spawn(async move {
                // Send failed when receive error from following `rx.recv()` and close the
                // channel. So it can be ignored.
                let _ = tx.send(Self::get_store_files(store, security_mgr).await);
            });
        }
        let mut valid_files = HashSet::default();
        for _ in 0..stores_len {
            let store_files_ids = rx.recv().unwrap()?;
            for (_, region_file_ids) in store_files_ids {
                valid_files.extend(region_file_ids.into_iter());
            }
        }
        let elapsed = start_time.elapsed();
        info!(
            "collect {} valid files from {} stores in {:?}",
            valid_files.len(),
            stores_len,
            elapsed
        );
        // If all peers of a region are moved during the collection, it is possible that
        // some files are not collected, cause data lost.
        // As it takes time to move peer, we concurrently collect the store files to
        // make the the collection faster and assume it takes at least 30s to
        // move all peers.
        if elapsed >= Duration::from_secs(30) {
            return Err(box_err!(
                "duration of collect_valid_files exceed 30s: elapsed {:?}",
                elapsed
            ));
        }

        self.valid_files = Arc::new(valid_files);
        Ok(())
    }

    async fn get_store_files(
        store: Store,
        security_mgr: Arc<SecurityManager>,
    ) -> Result<Vec<(u64, Vec<u64>)>> {
        let uri = security_mgr.build_uri(format!("{}/kvengine/files", &store.status_address))?;
        let client = security_mgr.http_client(hyper::Client::builder())?;
        let resp = client
            .get(uri)
            .await
            .map_err(|e| Error::Other(box_err!("get_store_files error {:?}", e)))?;
        let body = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        Ok(serde_json::from_slice(body.chunk()).unwrap())
    }

    async fn stats_files(&self, folders: &Option<HashSet<String>>) -> Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(String, Result<Stats>)>(self.concurrency);
        let sema = Arc::new(Semaphore::new(self.concurrency));

        let mut stats = Stats::default();

        let start_time = Instant::now();
        let prefixes = self
            .dfs
            .list_folders("", None)
            .await
            .map_err(|e| Error::DfsError(e))?;
        let mut prefixes_len = prefixes.len();
        let strip_prefix = format!("{}/", self.dfs.get_prefix());
        for prefix in prefixes {
            let prefix = prefix.strip_prefix(&strip_prefix).unwrap().to_owned();
            if let Some(folders) = folders {
                if folders.contains(&prefix) {
                    let sub_prefixes = self
                        .dfs
                        .list_folders(&prefix, None)
                        .await
                        .map_err(|e| Error::DfsError(e))?;
                    let sub_prefixes_len = sub_prefixes.len();
                    info!("prefix {}, sub prefixes {}", prefix, sub_prefixes_len);
                    if sub_prefixes_len > 0 {
                        prefixes_len += sub_prefixes_len - 1;
                        for sub_prefix in sub_prefixes {
                            let sub_prefix =
                                sub_prefix.strip_prefix(&strip_prefix).unwrap().to_owned();
                            self.spawn_stats_files_with_prefix(
                                sub_prefix,
                                tx.clone(),
                                sema.clone(),
                            );
                        }
                        continue;
                    }
                }
            }
            self.spawn_stats_files_with_prefix(prefix, tx.clone(), sema.clone());
        }
        for _ in 0..prefixes_len {
            let (prefix, result) = rx.recv().await.unwrap();
            if let Err(e) = result {
                error!("{} result error: {:?}", prefix, e);
                return Err(e);
            }
            let stats_with_prefix = result.unwrap();
            stats.append(stats_with_prefix);
        }
        let elapsed = start_time.elapsed();

        info!(
            "finished in {:?}: all files valid, {}",
            elapsed, stats.all_stat
        );
        for (ftype, stat) in stats.file_stat.iter() {
            info!("finished: {} files valid, {}", ftype.suffix(), stat);
        }
        info!(
            "finished: total {} in-used files valid",
            self.valid_files.len(),
        );
        for (ftype, stat) in stats.in_used_file_stat.iter() {
            info!("finished: in-used {} files valid, {}", ftype.suffix(), stat);
        }
        info!("finished: wal files valid, {}", stats.wal_stat);
        info!("finished: rlog files valid, {}", stats.rlog_stat);
        info!("finished: meta files valid, {}", stats.meta_stat);
        info!("finished: pack files valid, {}", stats.pack_stat);
        info!(
            "finished: intelligent-tiering files valid, {}",
            stats.intelligent_tiering_stat
        );
        info!("finished: standard files valid, {}", stats.standard_stat);
        info!(
            "finished: standard-ia files valid, {}",
            stats.standard_ia_stat
        );
        info!(
            "finished: glacier-ir files valid, {}",
            stats.glacier_ir_stat
        );
        Ok(())
    }

    fn spawn_stats_files_with_prefix(
        &self,
        prefix: String,
        tx: Sender<(String, Result<Stats>)>,
        sema: Arc<Semaphore>,
    ) {
        let stats_worker = self.clone();
        self.dfs.get_runtime().spawn(async move {
            let permit = sema.acquire().await.unwrap();
            let res = stats_worker.stats_files_with_prefix(prefix.clone()).await;
            drop(permit);
            let _ = tx.send((prefix, res)).await;
            Ok::<(), String>(())
        });
    }

    async fn stats_files_with_prefix(&self, prefix: String) -> Result<Stats> {
        let mut stats = Stats::default();
        let mut start_after = "".to_string();
        let start_time = Instant::now();
        info!("stats files with prefix: {}", prefix);
        loop {
            info!("loop start: {}{}", prefix, start_after);
            let (files, _, next_start_after) = self
                .dfs
                .list(start_after.as_str(), Some(&prefix), None)
                .await
                .map_err(|e| Error::DfsError(e))?;
            info!("listed {} {} files", prefix, files.len());
            for obj in files {
                let size = obj.size;
                let last_modified: DateTime<chrono::Utc> =
                    DateTime::parse_from_rfc3339(&obj.last_modified)
                        .expect("parse last_modified")
                        .into();
                let duration = chrono::Utc::now() - last_modified;
                match obj.storage_class.as_ref() {
                    STORAGE_CLASS_INTELLIGENT_TIERING => {
                        stats.intelligent_tiering_stat.add(duration, size)
                    }
                    STORAGE_CLASS_STANDARD => stats.standard_stat.add(duration, size),
                    STORAGE_CLASS_STANDARD_IA => stats.standard_ia_stat.add(duration, size),
                    STORAGE_CLASS_GLACIER_IR => stats.glacier_ir_stat.add(duration, size),
                    OSS_STORAGE_CLASS_STANDARD => stats.standard_stat.add(duration, size),
                    OSS_STORAGE_CLASS_IA => stats.standard_ia_stat.add(duration, size),
                    OSS_STORAGE_CLASS_ARCHIVE => stats.glacier_ir_stat.add(duration, size),
                    _ => {}
                };
                if obj.key.ends_with(".wal") || obj.key.ends_with(".wal.last") {
                    stats.wal_stat.add(duration, size);
                } else if obj.key.ends_with(".rlog") {
                    stats.rlog_stat.add(duration, size);
                } else if obj.key.ends_with(".meta") {
                    stats.meta_stat.add(duration, size);
                } else if obj.key.ends_with(".pack") {
                    stats.pack_stat.add(duration, size);
                } else if let Some((file_id, ftype)) = try_parse_all_file_id(&obj.key) {
                    if self.valid_files.contains(&file_id) {
                        stats
                            .in_used_file_stat
                            .entry(ftype)
                            .or_insert_with(ObjectStat::default)
                            .add(duration, size);
                    }
                    stats
                        .file_stat
                        .entry(ftype)
                        .or_insert_with(ObjectStat::default)
                        .add(duration, size);
                }
                stats.all_stat.add(duration, size);
            }
            if let Some(next_start_after) = next_start_after {
                start_after = next_start_after;
            } else {
                break;
            }
        }
        let elapsed = start_time.elapsed();
        info!(
            "finished in {:?}: all files with prefix {} valid, {}",
            elapsed, prefix, stats.all_stat
        );
        for (ftype, stat) in &stats.file_stat {
            info!(
                "finished: {} files with prefix {} valid, {}",
                ftype.suffix(),
                prefix,
                stat
            );
        }
        for (ftype, stat) in &stats.in_used_file_stat {
            info!(
                "finished: in-used {} files with prefix {} valid, {}",
                ftype.suffix(),
                prefix,
                stat
            );
        }
        info!(
            "finished: wal files with prefix {} valid, {}",
            prefix, stats.wal_stat
        );
        info!(
            "finished: rlog files with prefix {} valid, {}",
            prefix, stats.rlog_stat
        );
        info!(
            "finished: meta files with prefix {} valid, {}",
            prefix, stats.meta_stat
        );
        info!(
            "finished: pack files with prefix {} valid, {}",
            prefix, stats.pack_stat
        );
        info!(
            "finished: intelligent-tiering files with prefix {} valid, {}",
            prefix, stats.intelligent_tiering_stat
        );
        info!(
            "finished: standard files with prefix {} valid, {}",
            prefix, stats.standard_stat
        );
        info!(
            "finished: standard-ia files with prefix {} valid, {}",
            prefix, stats.standard_ia_stat
        );
        info!(
            "finished: glacier-ir files with prefix {} valid, {}",
            prefix, stats.glacier_ir_stat
        );
        Ok(stats)
    }
}
