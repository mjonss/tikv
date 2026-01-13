// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashSet,
    fmt,
    fmt::{Debug, Formatter},
    fs,
    path::PathBuf,
    result::Result as StdResult,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use bytes::Buf;
use chrono::DateTime;
use clap::Args;
use engine_traits::ListObjectContent;
use kvengine::{
    dfs,
    dfs::{DFSConfig, Dfs, FileType, S3Fs, try_parse_all_file_id},
};
use kvproto::metapb::Store;
use native_br::{
    common::create_pd_client,
    error::{Error, Result},
};
use pd_client::{RpcClient, util::get_all_stores_except_tiflash};
use security::{GetSecurityManager, SecurityConfig, SecurityManager};
use tikv_util::{box_err, config::ReadableDuration, error, info, warn};
use tokio::sync::{Mutex, Semaphore, mpsc::Sender};

/// DFS GC Rules:
///
/// * For files with last modified time later than (now -
///   `start_time_safe_interval`), do nothing.
///
/// * For files IN USED: If it's removed, retain it. Also write a warning in
///   logs.
///
/// * For files NOT in used:
///
///   1. The file will be removed if it's NOT removed.
///
///   2. The file will be permanently removed when:
///
///   2a. The file IS removed, and,
///
///   2b. `gc_lifetime` is configured, and,
///
///   2c. the duration since file last modified is longer than
///   `gc_lifetime`.
///
/// NOTE:
///
/// * `gc_lifetime` MUST NOT be set for production environment, in which files
///   are recycled by S3 lifecycle.
///
/// * A file is "removed" when it's in "STANDARD_IA" storage class or tagged
///   with "delete=true".
///
/// DFSGC arguments
#[derive(Args)]
pub struct DfsGcArgs {
    /// The path of the config file.
    #[clap(long, default_value = "")]
    pub config: PathBuf,

    /// GC after the start file prefix string.
    #[clap(long)]
    pub start: Option<String>,

    /// GC lifetime in duration string (see `ReadableDuration`).
    ///
    /// WARNING: will permanently remove files if this argument is set. Should
    /// be used for test environment ONLY.
    #[clap(long)]
    pub gc_lifetime: Option<ReadableDuration>,

    /// Loop interval, in seconds. Don't loop if not set.
    #[clap(long)]
    pub interval: Option<u64>,

    /// Concurrently do s3 requests.
    #[clap(long, default_value = "50")]
    pub concurrency: usize,

    /// Start time safe interval, in duration string (see `ReadableDuration`).
    /// GC `start_time` = now() - `start-time-safe-interval`.
    /// It's recommended to be at least 1 hour.
    #[clap(long, default_value = "1h")]
    pub start_time_safe_interval: ReadableDuration,

    /// File types whitelist, only files with these file types will be GCed.
    /// Use `,` to separate multiple file types.
    /// E.g. "sst,txn,blob,col,schema,vec".
    #[clap(long, value_delimiter = ',', default_value = "sst")]
    pub file_types_whitelist: Vec<String>,

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
}

fn convert_file_types(file_types: &[String]) -> StdResult<Vec<FileType>, String> {
    file_types.iter().map(|s| s.as_str().try_into()).collect()
}

pub(crate) fn execute_dfsgc(arg: DfsGcArgs) {
    let config = DfsGcConfig::from_args(&arg);
    validate_config(&config);

    let start_after = arg.start.unwrap_or_default();
    let pd_client = Arc::new(create_pd_client(&config.security, &config.pd));
    let s3fs = S3Fs::new_from_config(config.dfs);
    let start_time_safe_interval =
        chrono::Duration::from_std(Duration::from(arg.start_time_safe_interval)).unwrap();
    let progress_file_path = PathBuf::from(format!("{}/{}", &config.data_dir, "dfsgc.progress"));

    let file_types_whitelist =
        convert_file_types(&arg.file_types_whitelist).expect("invalid file types whitelist");

    let mut gc_worker = GcWorker::new(
        pd_client,
        s3fs,
        progress_file_path,
        config.gc_lifetime,
        arg.concurrency,
        file_types_whitelist,
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    loop {
        let mut gc = || -> Result<()> {
            // `start_time` should be acquired before `collect_valid_files`, and skip GC on
            // SST files modified after `start_time`.
            // Subtract by `start_time_safe_interval` to tolerate clock drift, and deal with
            // situations such as SST files are created long before applied to kvengine.
            let start_time = chrono::Utc::now() - start_time_safe_interval;

            gc_worker.collect_valid_files()?;
            runtime.block_on(gc_worker.remove_garbage_files(start_after.clone(), start_time))?;
            Ok(())
        };

        if let Some(interval) = &arg.interval {
            if let Err(err) = gc() {
                error!("execute gc error, try again: {:?}", err);
            }
            thread::sleep(Duration::from_secs(*interval));
        } else {
            if let Err(err) = gc() {
                panic!("execute gc error: {:?}", err);
            }
            break;
        }
    }
}

fn validate_config(config: &DfsGcConfig) {
    if config.gc_lifetime.is_some() {
        let s3_endpoint = url::Url::parse(&config.dfs.s3_endpoint).expect("parse dfs.s3_endpoint");
        // Assume that in test environment (e.g. minio) s3 endpoint must be configured
        // with port.
        assert!(
            s3_endpoint.port().is_some(),
            "gc_lifetime should be used on test environment only (dfs.s3_endpoint: {:?})",
            config.dfs.s3_endpoint
        );
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct DfsGcConfig {
    pub pd: pd_client::Config,
    pub security: SecurityConfig,
    pub dfs: DFSConfig,

    // If the task is not finished, a progress file is stored in the data dir, the next run
    // will load the progress and continue the task.
    pub data_dir: String,

    pub gc_lifetime: Option<Duration>,
}

impl DfsGcConfig {
    pub fn from_args(args: &DfsGcArgs) -> Self {
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

        if config.data_dir.is_empty() {
            config.data_dir = ".".to_string();
        }

        config.gc_lifetime = args.gc_lifetime.map(Into::into);
        config
    }
}

#[derive(Clone)]
struct GcWorker {
    pd: Arc<RpcClient>,
    s3fs: S3Fs,
    progress_file_path: PathBuf,
    valid_files: Arc<HashSet<u64>>,
    gc_lifetime: Option<chrono::Duration>,
    concurrency: usize,
    file_types_whitelist: Vec<FileType>,
}

#[derive(Default)]
struct GcStat {
    checked: usize,
    removed: usize,
    permanently_removed: usize,
    in_used_and_removed: usize,
    retained: usize,
}

impl fmt::Display for GcStat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} files checked, {} files removed, {} files permanently removed, {} files in-used but removed, {} files retained",
            self.checked,
            self.removed,
            self.permanently_removed,
            self.in_used_and_removed,
            self.retained,
        )
    }
}

#[derive(Debug)]
struct S3Object {
    pub file_id: u64,
    pub ftype: FileType,
    pub key: String,
    pub last_modified: DateTime<chrono::Utc>,
    pub storage_class: String,
    pub size: u64, // in bytes.
}

impl S3Object {
    pub fn from_list_object_content(file_id: u64, ftype: FileType, obj: ListObjectContent) -> Self {
        Self {
            file_id,
            ftype,
            key: obj.key,
            last_modified: DateTime::parse_from_rfc3339(&obj.last_modified)
                .expect("parse last_modified")
                .into(),
            storage_class: obj.storage_class,
            size: obj.size,
        }
    }
}

impl GcWorker {
    fn new(
        pd: Arc<RpcClient>,
        s3fs: S3Fs,
        progress_file_path: PathBuf,
        gc_lifetime: Option<Duration>,
        concurrency: usize,
        file_types_whitelist: Vec<FileType>,
    ) -> Self {
        Self {
            pd,
            s3fs,
            progress_file_path,
            valid_files: Arc::new(HashSet::default()),
            gc_lifetime: gc_lifetime.map(|d| chrono::Duration::from_std(d).unwrap()),
            concurrency,
            file_types_whitelist,
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
            self.s3fs.get_runtime().spawn(async move {
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

    async fn remove_garbage_files(
        &self,
        mut start_after: String,
        start_time: DateTime<chrono::Utc>,
    ) -> Result<()> {
        if let Ok(data) = fs::read_to_string(self.progress_file_path.as_path()) {
            let state_start_after = data;
            if start_after < state_start_after {
                info!(
                    "update start_after {} to {} from state file",
                    start_after, state_start_after
                );
                start_after = state_start_after;
            }
        }
        info!(
            "start remove garbage files after {}, start_time {}, gc lifetime {:?}",
            start_after, start_time, self.gc_lifetime
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel::<(u64, Result<()>)>(self.concurrency);
        let sema = Arc::new(Semaphore::new(self.concurrency));

        let stat = Arc::new(Mutex::new(GcStat::default()));
        loop {
            info!("loop start: {}", start_after);
            let (files, _, next_start_after) =
                self.s3fs.list(start_after.as_str(), None, None).await?;
            info!("listed {} files", files.len());
            let file_objs = files
                .into_iter()
                .filter_map(|obj| {
                    try_parse_all_file_id(&obj.key)
                        .map(|(file_id, ftype)| {
                            S3Object::from_list_object_content(file_id, ftype, obj)
                        })
                        .filter(|obj| obj.last_modified < start_time)
                })
                .collect::<Vec<_>>();

            let mut objs_len = 0;
            for s3_obj in file_objs {
                let in_whitelist = self.file_types_whitelist.contains(&s3_obj.ftype);
                if !in_whitelist {
                    continue;
                }

                objs_len += 1;
                self.spawn_remove_file_task(
                    tx.clone(),
                    stat.clone(),
                    sema.clone(),
                    s3_obj,
                    start_time,
                );
            }

            for _ in 0..objs_len {
                let (file_id, exec_result) = rx.recv().await.unwrap();
                if let Err(e) = exec_result {
                    error!("{} exec_result error: {:?}", file_id, e);
                    return Err(e);
                }
                stat.lock().await.checked += 1;
            }

            if let Some(next_start_after) = next_start_after {
                start_after = next_start_after;
                fs::write(self.progress_file_path.as_path(), start_after.as_bytes()).unwrap();
            } else {
                break;
            }
        }
        if self.progress_file_path.exists() {
            fs::remove_file(self.progress_file_path.as_path()).unwrap();
        }

        info!(
            "finished: {} files valid, {}",
            self.valid_files.len(),
            stat.lock().await
        );
        Ok(())
    }

    fn spawn_remove_file_task(
        &self,
        tx: Sender<(u64, Result<()>)>,
        mut stat: Arc<Mutex<GcStat>>,
        sema: Arc<Semaphore>,
        s3_obj: S3Object,
        start_time: DateTime<chrono::Utc>,
    ) {
        let gc_worker = self.clone();
        self.s3fs.get_runtime().spawn(async move {
            let permit = sema.acquire().await.unwrap();
            let exec_result = if !gc_worker.valid_files.contains(&s3_obj.file_id) {
                gc_worker
                    .remove_garbage_file(&s3_obj, &start_time, &mut stat)
                    .await
            } else {
                match gc_worker.is_file_removed(&s3_obj).await {
                    Ok(true) => {
                        warn!("{} in-used but removed: {:?}", s3_obj.file_id, s3_obj);
                        stat.lock().await.in_used_and_removed += 1;
                        if let Err(e) = gc_worker.s3fs.retain_file(&s3_obj.key).await {
                            Err(Error::DfsError(e))
                        } else {
                            info!("{} is retained", s3_obj.file_id);
                            stat.lock().await.retained += 1;
                            Ok(())
                        }
                    }
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                }
            };
            drop(permit);
            let _ = tx.send((s3_obj.file_id, exec_result)).await;
        });
    }

    fn is_storage_class_for_remove(storage_class: &str) -> bool {
        storage_class == dfs::STORAGE_CLASS_STANDARD_IA
            || storage_class == dfs::OSS_STORAGE_CLASS_IA
    }

    async fn is_file_removed(&self, s3_obj: &S3Object) -> Result<bool> {
        Ok(Self::is_storage_class_for_remove(&s3_obj.storage_class)
            || self.s3fs.is_removed(&s3_obj.key).await?)
    }

    async fn remove_garbage_file(
        &self,
        s3_obj: &S3Object,
        start_time: &DateTime<chrono::Utc>,
        stat: &mut Arc<Mutex<GcStat>>,
    ) -> Result<()> {
        let opts = dfs::Options::default().with_type(s3_obj.ftype);
        let removed = match self.is_file_removed(s3_obj).await {
            Ok(removed) => removed,
            Err(e) => {
                error!("{} check is_file_removed failed: {:?}", s3_obj.file_id, e);
                return Err(e);
            }
        };

        if !removed {
            self.s3fs
                .remove(s3_obj.file_id, Some(s3_obj.size), opts)
                .await;
            stat.lock().await.removed += 1;
            info!(
                "{}.{} ({}) is removed",
                s3_obj.file_id, s3_obj.ftype, &s3_obj.key
            );
        } else if let Some(gc_lifetime) = self.gc_lifetime {
            let duration = *start_time - s3_obj.last_modified;

            if duration > gc_lifetime {
                if let Err(err) = self.s3fs.permanently_remove(s3_obj.file_id, opts).await {
                    warn!("{} permanently_remove error: {:?}", s3_obj.file_id, err);
                    return Err(Error::DfsError(err));
                } else {
                    info!(
                        "{}.{} ({}) is permanently removed, removed at {} ({:.2} min)",
                        s3_obj.file_id,
                        s3_obj.ftype,
                        &s3_obj.key,
                        s3_obj.last_modified,
                        duration.num_seconds() as f64 / 60.0
                    );
                    stat.lock().await.permanently_removed += 1;
                }
            }
        }
        Ok(())
    }
}
