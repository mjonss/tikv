// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Cow,
    cmp,
    collections::HashMap,
    fmt, fs,
    io::Write,
    ops::Deref,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    thread,
    time::Duration,
};

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use http::{request::Parts, Method, StatusCode};
use hyper::{Body, Response};
use kvengine::dfs::Dfs;
use native_br::{
    backup,
    backup::IncrementalBackupFile,
    backup_worker,
    backup_worker::BackupWorker,
    common::get_all_incremental_backups,
    restore, restore_keyspace,
    restore_keyspace::{
        restore_keyspace_with_cfg, ObjectCache, ReportRestoreStepTrait, RestoreStep,
        RestoredKeyspace,
    },
    rfengine_cache::RfEngineCache,
};
use pd_client::{pd_control::PdControl, PdClient};
use serde::Deserialize;
use tikv::storage::mvcc::TimeStamp;
use tikv_util::{
    config::{AbsoluteOrPercentSize, ReadableDuration, ReadableSize},
    debug, error,
    errors::Context as _,
    info,
    sys::SysQuota,
    time::Instant,
    warn, HandyRwLock,
};
use tokio::runtime::Runtime;

use crate::{
    common::{get_param, make_json_response, make_response},
    error::{Error, Result},
    metrics::{NATIVE_BR_COUNTER_VEC, NATIVE_BR_HISTOGRAM_VEC},
    Config,
};

const MIN_PITR_INTERVAL_GAP_SECONDS: i64 = 1; // 1s
const MAX_BACKUP_COUNT_PER_PAGE: usize = 1000; // Same with dfs list.
const JSON_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.3f"; // e.g. 2006-01-02 15:04:05.000
const BACKUP_NAME_FORMAT: &str = "%Y%m%d%H%M%S";
pub(crate) const BACKUPS_API_PATH: &str = "/api/v1/backups";
pub(crate) const RESTORE_KEYSPACE_API_PATH: &str = "/api/v1/restore_keyspace/";

const RESTORE_TASK_WORKING_PATH_PREFIX: &str = "r";

/// Backup and restore keyspace API:
///
/// 1. backup:
///   * GET    /api/v1/backups?cluster_id=%d&last_backup_time=<JSON_TIME_FORMAT>
///
/// 2. restore keyspace: Specify backup_id&backup_name for normal restore or
///    point_in_time for pitr
///   * PUT    /api/v1/restore_keyspace/<restore_id>?cluster_id=%d&keyspace=%s&
///     backup_id=%d&backup_name=%s[&source_keyspace=%s]&
///     point_in_time=<JSON_TIME_FORMAT>
///   * GET    /api/v1/restore_keyspace/<restore_id>?cluster_id=%d&keyspace=%s
///   * DELETE /api/v1/restore_keyspace/<restore_id>?cluster_id=%d&keyspace=%s
///   * GET    /api/v1/restore_keyspace/?cluster_id=%d

pub(crate) async fn handle_backup(
    manager: Arc<NativeBrManager>,
    req: hyper::Request<hyper::Body>,
) -> hyper::Result<hyper::Response<hyper::Body>> {
    let query = req.uri().query().unwrap_or("");
    let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
    match get_param::<u64>(&query_pairs, "cluster_id") {
        Some(cluster_id) if cluster_id == manager.get_cluster_id().unwrap() => {}
        _ => {
            return Ok(make_response(
                StatusCode::BAD_REQUEST,
                "Cluster ID mismatch",
            ));
        }
    }
    match *req.method() {
        Method::GET => {
            let ob_start_time = Instant::now();
            let last_backup_time = query_pairs.get("last_backup_time").and_then(|s| {
                NaiveDateTime::parse_from_str(s, JSON_TIME_FORMAT)
                    .ok()
                    .map(|t| DateTime::<Utc>::from_utc(t, Utc))
            });
            if last_backup_time.is_none() {
                return Ok(make_response(
                    StatusCode::BAD_REQUEST,
                    "Last backup time is none or invalid",
                ));
            }
            let start_backup_time = last_backup_time
                .unwrap()
                .checked_add_signed(chrono::Duration::seconds(1))
                .unwrap();
            let max_count = if let Some(count) = query_pairs.get("max_count") {
                match count.parse::<usize>() {
                    Ok(c) => c,
                    Err(e) => {
                        return Ok(make_response(
                            StatusCode::BAD_REQUEST,
                            format!("max count is invalid {:?}", e),
                        ));
                    }
                }
            } else {
                MAX_BACKUP_COUNT_PER_PAGE
            };
            match manager.list_backups(&start_backup_time, max_count).await {
                Ok((backups, has_more)) => {
                    let resp = ListBackupResponse {
                        items: backups.into_iter().map(Into::into).collect(),
                        has_more,
                    };
                    NATIVE_BR_HISTOGRAM_VEC
                        .with_label_values(&["list_backup"])
                        .observe(ob_start_time.saturating_elapsed_secs());
                    Ok(make_json_response(StatusCode::OK, &resp))
                }
                Err(e) => {
                    NATIVE_BR_COUNTER_VEC
                        .with_label_values(&["list_backup_fail"])
                        .inc();
                    Ok(make_response(
                        StatusCode::NOT_FOUND,
                        format!("Backups not found: {:?}", e),
                    ))
                }
            }
        }
        _ => Ok(make_response(StatusCode::BAD_REQUEST, "Invalid method")),
    }
}

fn parse_restore_type(query_pairs: &HashMap<Cow<'_, str>, Cow<'_, str>>) -> Result<RestoreType> {
    let existed_backup =
        query_pairs.get("backup_id").is_some() || query_pairs.get("backup_name").is_some();
    let pitr = query_pairs.get("point_in_time").is_some();

    if existed_backup && pitr {
        return Err(Error::CheckError(
            "Request for normal restore and PiTR at the same time".to_string(),
        ));
    }
    if existed_backup {
        Ok(RestoreType::Normal)
    } else {
        Ok(RestoreType::Pitr)
    }
}

async fn get_backup_from_query(
    manager: &Arc<NativeBrManager>,
    query_pairs: &HashMap<Cow<'_, str>, Cow<'_, str>>,
    restore_type: RestoreType,
) -> Result<RestoreSource> {
    match restore_type {
        RestoreType::Normal => {
            let backup_id = match get_param::<u64>(query_pairs, "backup_id") {
                Some(id) => id,
                None => {
                    return Err(Error::CheckError("Backup ID is invalid".to_string()));
                }
            };
            let backup_name = query_pairs
                .get("backup_name")
                .map(|s| s.to_string())
                .unwrap_or_default();
            let backup = IncrementalBackupFile::from_id(backup_id);
            if backup_name != backup.created_at().format(BACKUP_NAME_FORMAT).to_string() {
                return Err(Error::CheckError("Backup ID & name mismatch".to_string()));
            }
            Ok(RestoreSource::ExistFile(backup, None))
        }
        RestoreType::Pitr => {
            let ts = query_pairs
                .get("point_in_time")
                .map(|s| s.to_string())
                .unwrap_or_default();
            if ts.is_empty() {
                return Err(Error::CheckError("Recover time is empty".to_string()));
            }
            let utc_time = NaiveDateTime::parse_from_str(&ts, JSON_TIME_FORMAT)
                .map(|t| DateTime::<Utc>::from_utc(t, Utc))?;
            // To avoid the system time gap between tikv-api and pd nodes.
            if Utc::now().signed_duration_since(utc_time).num_seconds()
                < MIN_PITR_INTERVAL_GAP_SECONDS
            {
                return Err(Error::CheckError(format!(
                    "Future time {:?} is not supported",
                    ts
                )));
            }
            match manager.get_next_backup_after_ts(&utc_time).await? {
                Some(f) => Ok(RestoreSource::ExistFile(f, Some(utc_time))),
                None => Ok(RestoreSource::InstantBackup(utc_time)),
            }
        }
    }
}

pub(crate) async fn handle_restore_keyspace(
    manager: Arc<NativeBrManager>,
    parts: Parts,
) -> hyper::Result<hyper::Response<hyper::Body>> {
    let query = parts.uri.query().unwrap_or("");
    let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();

    match get_param::<u64>(&query_pairs, "cluster_id") {
        Some(cluster_id) if cluster_id == manager.get_cluster_id().unwrap() => {}
        _ => {
            return Ok(make_response(
                StatusCode::BAD_REQUEST,
                "Cluster ID mismatch",
            ));
        }
    }
    let sub_path = parts
        .uri
        .path()
        .strip_prefix(RESTORE_KEYSPACE_API_PATH)
        .unwrap();
    if sub_path.is_empty() && parts.method == Method::GET {
        return handle_get_all_restore_task(&manager);
    }

    let restore_id = match sub_path.parse::<u64>() {
        Ok(id) => id,
        Err(_) => {
            return Ok(make_response(StatusCode::BAD_REQUEST, "Invalid restore id"));
        }
    };

    let target_keyspace = match query_pairs.get("keyspace") {
        Some(ks) if !ks.is_empty() => ks.to_string(),
        Some(_) => {
            return Ok(make_response(
                StatusCode::BAD_REQUEST,
                "Keyspace name is empty",
            ));
        }
        None => {
            return Ok(make_response(
                StatusCode::BAD_REQUEST,
                "Keyspace name is none",
            ));
        }
    };

    // It's inplace restore for "keyspace" when "source_keyspace" is not specified.
    let source_keyspace = match query_pairs.get("source_keyspace") {
        Some(ks) if !ks.is_empty() => ks.to_string(),
        Some(_) => {
            return Ok(make_response(
                StatusCode::BAD_REQUEST,
                "Source keyspace name is empty",
            ));
        }
        None => target_keyspace.clone(),
    };

    let keyspace_tag = format!("{}->{}", source_keyspace, target_keyspace);

    match parts.method {
        Method::GET => {
            debug!(
                "{} request to GET restore_keyspace, restore_id {}",
                keyspace_tag, restore_id
            );
            handle_restore_status(&manager, restore_id, &target_keyspace)
        }
        Method::PUT => {
            let restore_type = match parse_restore_type(&query_pairs) {
                Ok(t) => t,
                Err(e) => {
                    return Ok(make_response(StatusCode::BAD_REQUEST, e.to_string()));
                }
            };
            let backup = get_backup_from_query(&manager, &query_pairs, restore_type).await;
            if backup.is_err() {
                return Ok(make_response(
                    StatusCode::BAD_REQUEST,
                    format!("Fail to get backup: {}", backup.unwrap_err()),
                ));
            }
            let backup = backup.unwrap();
            debug!(
                "{} request to PUT restore_keyspace, restore_id {}, backup {:?}",
                keyspace_tag, restore_id, backup
            );
            match manager.restore_keyspace(
                restore_id,
                source_keyspace,
                target_keyspace.clone(),
                backup.clone(),
                restore_type,
            ) {
                Ok(true) => {
                    info!(
                        "{} restore keyspace started, restore_id {}, backup {:?}",
                        keyspace_tag, restore_id, backup
                    );
                    let (progress, step_name) =
                        RestoreProgressReporter::step_to_progress(&RestoreStep::Pending);
                    let resp = RestoreProgressResponse {
                        status: RestoreState::Pending,
                        error: String::new(),
                        duration: 0,
                        id: restore_id,
                        keyspace: target_keyspace,
                        restore_type,
                        restore_bytes: 0,
                        progress: RestoreProgress {
                            step: step_name.to_string(),
                            progress,
                        },
                    };
                    Ok(make_json_response(StatusCode::OK, &resp))
                }
                Ok(false) => {
                    info!(
                        "{} restore keyspace ignored, restore_id {}, backup {:?}",
                        keyspace_tag, restore_id, backup
                    );
                    // Return current status when `restore_keyspace` request is ignored.
                    handle_restore_status(&manager, restore_id, &target_keyspace)
                }
                Err(Error::RestoreKeyspaceTaskConflict(conflict_restore_id)) => {
                    info!(
                        "{} restore keyspace conflict, restore_id {}, backup {:?}, conflict restore_id {}",
                        keyspace_tag, restore_id, backup, conflict_restore_id,
                    );
                    let resp = RestoreConflictResponse {
                        keyspace: target_keyspace,
                        id: restore_id,
                        conflict_restore_id,
                    };
                    Ok(make_json_response(StatusCode::CONFLICT, &resp))
                }
                Err(e) => {
                    error!(
                        "{} restore keyspace error, restore_id {}, backup {:?}, error {:?}",
                        keyspace_tag, restore_id, backup, e
                    );
                    handle_error(e)
                }
            }
        }
        Method::DELETE => {
            debug!(
                "{} request to DELETE restore_keyspace, restore_id {}",
                keyspace_tag, restore_id
            );
            match manager.delete_restore(restore_id, &target_keyspace) {
                Ok(Some(true)) => {
                    info!(
                        "{} restore keyspace task deleted, restore_id {}",
                        keyspace_tag, restore_id
                    );
                    Ok(make_response(StatusCode::OK, "Restore task deleted"))
                }
                Ok(Some(false)) => {
                    info!(
                        "{} delete restore keyspace task ignored, restore_id {}",
                        keyspace_tag, restore_id
                    );
                    Ok(make_response(
                        StatusCode::CONFLICT,
                        "Restore task is not in final state",
                    ))
                }
                Ok(None) => {
                    info!(
                        "{} delete restore keyspace task not found, restore_id {}",
                        keyspace_tag, restore_id
                    );
                    Ok(make_response(
                        StatusCode::NOT_FOUND,
                        format!("Restore task not found: {}", restore_id),
                    ))
                }
                Err(err) => {
                    error!(
                        "{} delete restore keyspace task error, restore_id {}, error {:?}",
                        keyspace_tag, restore_id, err
                    );
                    handle_error(err)
                }
            }
        }
        _ => Ok(make_response(StatusCode::BAD_REQUEST, "Invalid method")),
    }
}

fn handle_get_all_restore_task(manager: &Arc<NativeBrManager>) -> hyper::Result<Response<Body>> {
    let resp_vec: Vec<RestoreProgressResponse> = manager
        .get_all_restore_task()
        .into_iter()
        .map(|(id, task)| RestoreProgressResponse::from_restore_status(id, task))
        .collect();
    Ok(make_json_response(StatusCode::OK, &resp_vec))
}

fn handle_restore_status(
    manager: &Arc<NativeBrManager>,
    restore_id: u64,
    keyspace: &str,
) -> hyper::Result<Response<Body>> {
    match manager.restore_status(restore_id, keyspace) {
        Ok(Some(task)) => {
            let resp = RestoreProgressResponse::from_restore_status(restore_id, task);
            Ok(make_json_response(StatusCode::OK, &resp))
        }
        Ok(None) => Ok(make_response(
            StatusCode::NOT_FOUND,
            format!("Restore task not found: {}", restore_id),
        )),
        Err(err) => {
            error!(
                "{} query restore keyspace task status error, restore_id {}, error {:?}",
                keyspace, restore_id, err
            );
            handle_error(err)
        }
    }
}

fn handle_error(err: Error) -> hyper::Result<Response<Body>> {
    match err {
        Error::CheckError(msg) => Ok(make_response(StatusCode::BAD_REQUEST, msg)),
        err => Ok(make_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            err.to_string(),
        )),
    }
}

#[derive(Clone, Debug)]
enum RestoreSource {
    ExistFile(
        IncrementalBackupFile,
        Option<DateTime<Utc>>, // point_in_time
    ),
    InstantBackup(DateTime<Utc> /* point_in_time */),
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct BackupItem {
    pub id: u64,
    pub name: String,
    pub time: String,
}

impl From<IncrementalBackupFile> for BackupItem {
    fn from(backup: IncrementalBackupFile) -> Self {
        Self {
            id: backup.id(),
            name: backup.created_at().format(BACKUP_NAME_FORMAT).to_string(),
            time: backup.created_at().format(JSON_TIME_FORMAT).to_string(),
        }
    }
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct ListBackupResponse {
    pub items: Vec<BackupItem>,
    pub has_more: bool,
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct RestoreProgress {
    pub step: String,
    pub progress: i32,
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct RestoreProgressResponse {
    pub status: RestoreState,
    pub error: String,
    pub duration: i64, // in seconds
    pub id: u64,
    pub keyspace: String,
    pub restore_type: RestoreType,
    pub restore_bytes: u64,
    pub progress: RestoreProgress,
}

impl RestoreProgressResponse {
    fn from_restore_status(restore_id: u64, status: RestoreTask) -> Self {
        let duration = status
            .end
            .unwrap_or_else(|| Utc::now())
            .signed_duration_since(status.start);
        Self {
            status: status.state,
            error: status.error,
            duration: duration.num_seconds().clamp(0, i64::MAX),
            id: restore_id,
            keyspace: status.keyspace_name,
            restore_type: status.restore_type,
            restore_bytes: status.restore_bytes,
            progress: status.progress_reporter.get_progress(),
        }
    }
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
struct RestoreConflictResponse {
    keyspace: String,
    id: u64,
    conflict_restore_id: u64,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub enum RestoreState {
    #[default]
    Pending,
    Init,
    Running,
    Succeed,
    Error,
}

impl RestoreState {
    fn is_final(&self) -> bool {
        *self == Self::Succeed || *self == Self::Error
    }
}

#[derive(Clone, Copy, Default, Serialize, Deserialize, Debug, PartialEq)]
pub enum RestoreType {
    #[default]
    Normal,
    Pitr,
}

#[derive(Clone)]
pub(crate) struct RestoreTask {
    state: RestoreState,
    keyspace_name: String,
    error: String,
    start: DateTime<Utc>,
    end: Option<DateTime<Utc>>,
    restore_type: RestoreType, // deprecated, remove after next upgrade.
    restore_params: RestoreParams,
    restore_bytes: u64,
    progress_reporter: Arc<RestoreProgressReporter>,
}

impl RestoreTask {
    fn ttl_expired(&self, ttl: Duration) -> bool {
        self.state.is_final()
            && Utc::now()
                .signed_duration_since(self.end.unwrap())
                .num_seconds()
                >= ttl.as_secs() as i64
    }

    fn get_meta(&self) -> RestoreTaskMeta {
        RestoreTaskMeta {
            keyspace_name: self.keyspace_name.clone(),
            restore_type: self.restore_type,
            restore_params: self.restore_params.clone(),
            start: self.start.timestamp(),
        }
    }

    fn from_meta(meta: RestoreTaskMeta) -> Self {
        let start = meta.start();
        Self {
            state: RestoreState::Error, // Consider as being interrupted by node restart.
            keyspace_name: meta.keyspace_name,
            error: "interrupted".to_string(),
            start,
            end: Some(Utc::now()),
            restore_type: meta.restore_type,
            restore_params: meta.restore_params.clone(),
            restore_bytes: 0,
            progress_reporter: Arc::new(RestoreProgressReporter::new(0, RestoreStep::Init)),
        }
    }
}

#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct RestoreTaskMeta {
    keyspace_name: String,
    restore_type: RestoreType, // deprecated, remove after next upgrade.
    restore_params: RestoreParams,
    start: i64, // DateTime<Utc>::timestamp().
}

impl fmt::Debug for RestoreTaskMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestoreTaskMeta")
            .field("keyspace_name", &self.keyspace_name)
            .field("restore_type", &self.restore_type)
            .field("restore_params", &self.restore_params)
            .field("start", &self.start())
            .finish()
    }
}

impl RestoreTaskMeta {
    fn start(&self) -> DateTime<Utc> {
        Utc.timestamp_opt(self.start, 0).unwrap()
    }
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
struct RestoreParams {
    /// The backup name. For "Normal" restore.
    #[serde(skip_serializing_if = "Option::is_none")]
    backup_name: Option<String>,

    /// The point in time. For "PiTR".
    #[serde(skip_serializing_if = "Option::is_none")]
    point_in_time: Option<i64>,

    /// The name of source keyspace for restore.
    ///
    /// For "Branch" or restore to another cluster.
    #[serde(skip_serializing_if = "Option::is_none")]
    source_keyspace: Option<String>,
}

impl RestoreParams {
    fn new(
        restore_type: &RestoreType,
        restore_source: &RestoreSource,
        source_keyspace: &str,
        target_keyspace: &str,
    ) -> Result<Self> {
        let src_ks_opt = (source_keyspace != target_keyspace).then(|| source_keyspace.to_string());
        match (restore_type, restore_source) {
            (RestoreType::Normal, RestoreSource::ExistFile(backup, None)) => Ok(Self {
                backup_name: Some(backup.name().to_string()),
                source_keyspace: src_ks_opt,
                ..Default::default()
            }),
            (RestoreType::Pitr, RestoreSource::ExistFile(_, Some(point_in_time)))
            | (RestoreType::Pitr, RestoreSource::InstantBackup(point_in_time)) => Ok(Self {
                point_in_time: Some(point_in_time.timestamp()),
                source_keyspace: src_ks_opt,
                ..Default::default()
            }),
            _ => {
                let msg = format!(
                    "illegal restore params: {:?}, {:?}",
                    restore_type, restore_source
                );
                Err(Error::CheckError(msg))
            }
        }
    }

    #[inline]
    fn is_none(&self) -> bool {
        self.backup_name.is_none() && self.point_in_time.is_none()
    }
}

struct RestoreProgressReporter {
    restore_id: u64,
    step: Mutex<RestoreStep>,
}

impl RestoreProgressReporter {
    fn new(restore_id: u64, step: RestoreStep) -> Self {
        Self {
            restore_id,
            step: Mutex::new(step),
        }
    }

    fn step_to_progress(step: &RestoreStep) -> (i32, &'static str) {
        match step {
            // The difference value between this and next step is the estimated time cost of current
            // step.
            // Note: the message will be seen by customers.
            RestoreStep::Pending => (0, "pending"),
            RestoreStep::Init => (5, "init"),
            RestoreStep::InstantBackup => (10, "prepare backup"),
            RestoreStep::RemoveTiFlashReplicas => (30, "remove tiflash replicas"),
            RestoreStep::LoadBackupMeta => (35, "load backup meta"),
            RestoreStep::ExtractBackupShards => (40, "extract backup shards"),
            RestoreStep::ResolveLocks => (48, "resolve locks"),
            RestoreStep::FlushShards => (50, "flush shards"),
            RestoreStep::TruncateTs => (52, "truncate ts"),
            RestoreStep::SplitRegions => (55, "split regions"),
            RestoreStep::AlignRegions => (60, "align regions"),
            RestoreStep::RestoreSnapshotsToServers => (65, "restore snapshots to servers"),
            RestoreStep::RestoreSnapshotsFinished => (85, "restore snapshots finished"),
            RestoreStep::RetainSstFiles => (85, "retain files"),
            RestoreStep::Finalize => (90, "finalize"), // Wait for ClusterCR become normal.
        }
    }

    fn get_progress(&self) -> RestoreProgress {
        let step = self.step.lock().unwrap();
        let (progress, step_name) = Self::step_to_progress(&step);
        RestoreProgress {
            step: step_name.to_string(),
            progress,
        }
    }
}

impl ReportRestoreStepTrait for RestoreProgressReporter {
    fn report_step(&self, step: RestoreStep) {
        info!("restore {} report_step: {:?}", self.restore_id, step);
        let mut current_step = self.step.lock().unwrap();
        if *current_step < step {
            *current_step = step;
        }
    }
}

type TasksMap = HashMap<u64 /* restore_id */, RestoreTask>;

type KeyspacesMap = HashMap<String /* keyspace */, u64 /* restore_id */>;

pub(crate) struct BrContext {
    pub pd_client: Arc<dyn PdClient>,
    pub data_dir: PathBuf,
    pub dfs: Arc<dyn Dfs>,
    pub runtime: Arc<Runtime>,
    pub restore_tasks: RwLock<TasksMap>,
    pub keyspace_tasks: RwLock<KeyspacesMap>,
    pub backup_worker: BackupWorker,
    restore_concurrency: usize,
    object_cache: Option<ObjectCache>,
    limiter: Option<Arc<native_br::limiter::ThroughputLimiter>>,
    rfengine_cache: Option<RfEngineCache>,
}

impl BrContext {
    // Return false if the state is falling back.
    fn init_restore_state(
        &self,
        restore_id: u64,
        keyspace_name: &str,
        restore_type: RestoreType,
        restore_params: RestoreParams,
    ) -> Result<bool> {
        let new_task =
            |tasks: &mut TasksMap, restore_params: RestoreParams| -> Result<RestoreTaskMeta> {
                let mut keyspaces = self.keyspace_tasks.write().unwrap();
                if let Some(&restore_id) = keyspaces.get(keyspace_name) {
                    return Err(Error::RestoreKeyspaceTaskConflict(restore_id));
                }
                keyspaces.insert(keyspace_name.to_owned(), restore_id);
                let task = RestoreTask {
                    state: RestoreState::Init,
                    keyspace_name: keyspace_name.to_string(),
                    error: String::new(),
                    start: Utc::now(),
                    end: None,
                    restore_type,
                    restore_params,
                    restore_bytes: 0,
                    progress_reporter: Arc::new(RestoreProgressReporter::new(
                        restore_id,
                        RestoreStep::Init,
                    )),
                };
                let task_meta = task.get_meta();
                tasks.insert(restore_id, task);
                Ok(task_meta)
            };

        let mut tasks = self.restore_tasks.write().unwrap();
        Ok(match tasks.get_mut(&restore_id) {
            None => {
                let meta = new_task(&mut tasks, restore_params)?;
                self.persist_task_meta(restore_id, &meta)?;
                true
            }
            Some(task) if task.state == RestoreState::Error => {
                // Retry (Error -> Init):
                check_task(task, keyspace_name, Some(&restore_params))?;
                let meta = new_task(&mut tasks, restore_params)?;
                self.persist_task_meta(restore_id, &meta)?;
                true
            }
            Some(_) => false,
        })
    }

    // Return false if the state is falling back.
    fn change_restore_state(
        &self,
        restore_id: u64,
        keyspace_name: &str,
        new_state: RestoreState,
        err: Option<Error>,
        restore_bytes: u64,
    ) -> Result<bool> {
        let mut tasks = self.restore_tasks.write().unwrap();
        match tasks.get_mut(&restore_id) {
            None => {
                debug_assert!(false);
                return Ok(false);
            }
            Some(task) => {
                check_task(task, keyspace_name, None)?;
                if task.state >= new_state {
                    return Ok(false);
                }
                task.restore_bytes = restore_bytes;
                task.state = new_state;
                task.error = err.map(|err| format!("{:?}", err)).unwrap_or_default();

                if task.state.is_final() {
                    task.end = Some(Utc::now());
                    self.keyspace_tasks.write().unwrap().remove(keyspace_name);
                }
            }
        }
        Ok(true)
    }

    fn get_progress_reporter(&self, restore_id: u64) -> Option<Arc<RestoreProgressReporter>> {
        let tasks = self.restore_tasks.read().unwrap();
        tasks
            .get(&restore_id)
            .map(|task| task.progress_reporter.clone())
    }

    fn restore_keyspace_core(
        &self,
        config: Config,
        working_path: PathBuf,
        keyspace_name: &str,
        target_keyspace_name: &str,
        restore_source: RestoreSource,
        restore_type: RestoreType,
        progress_reporter: Arc<RestoreProgressReporter>,
    ) -> Result<RestoredKeyspace> {
        // We need to register rfengine cache first, so when the instant backup is done
        // and we fill the cache, the registered keyspace id can be used to load
        // related rfengines to the cache.
        self.register_rfengine_cache(&config, keyspace_name, &restore_source)?;
        // Instant backup must be performed before every restore.
        // Otherwise the data from previous backup to now will be lost, and can not be
        // restored by PiTR.
        progress_reporter.report_step(RestoreStep::InstantBackup);
        let instant_backup = self.runtime.block_on(
            self.backup_worker
                .instant_backup_with_retry(config.native_br.instant_backup_timeout.0),
        )?;
        info!("restore keyspace: instant backup: {:?}", instant_backup;
            "keyspace" => &keyspace_name, "target_keyspace" => &target_keyspace_name);

        let get_truncate_ts =
            |utc_time: Option<DateTime<Utc>>, restore_type: RestoreType| -> Option<u64> {
                match utc_time {
                    Some(t) => {
                        debug_assert!(restore_type == RestoreType::Pitr);
                        let tso = TimeStamp::compose(t.timestamp_millis() as u64, 0);
                        Some(tso.into_inner())
                    }
                    None => {
                        debug_assert!(restore_type == RestoreType::Normal);
                        None
                    }
                }
            };

        let (backup_file, truncate_ts) = match restore_source {
            RestoreSource::ExistFile(f, utc_time) => (f, get_truncate_ts(utc_time, restore_type)),
            RestoreSource::InstantBackup(utc_time) => (
                instant_backup.deref().backup_file.clone(),
                get_truncate_ts(Some(utc_time), restore_type),
            ),
        };
        let rfengien_cache_may_hit = match truncate_ts {
            Some(ts) => instant_backup.safe_ts <= ts && ts <= instant_backup.backup_ts,
            None => false,
        };
        let rfengine_cache = if rfengien_cache_may_hit {
            self.fill_rfengine_cache(instant_backup.backup_file.name(), instant_backup.backup_ts);
            self.rfengine_cache.clone()
        } else {
            None
        };
        let restore_config = config.to_restore_config();
        Ok(restore_keyspace_with_cfg(
            restore_config,
            keyspace_name,
            target_keyspace_name,
            backup_file.name(),
            Some(working_path),
            self.dfs.clone(),
            self.pd_client.clone(),
            &self.runtime,
            truncate_ts,
            progress_reporter,
            self.object_cache.clone(),
            self.limiter.clone(),
            rfengine_cache,
        )?)
    }

    fn register_rfengine_cache(
        &self,
        config: &Config,
        keyspace_name: &str,
        restore_source: &RestoreSource,
    ) -> Result<()> {
        let Some(rfengine_cache) = self.rfengine_cache.as_ref() else {
            return Ok(());
        };
        let utc_time = match restore_source {
            RestoreSource::ExistFile(_, utc_time_opt) => {
                let Some(utc_time) = utc_time_opt else {
                    // Only fill cache for PiTR restore.
                    return Ok(());
                };
                utc_time
            }
            RestoreSource::InstantBackup(utc_time) => utc_time,
        };
        let truncate_ts = TimeStamp::compose(utc_time.timestamp_millis() as u64, 0).into_inner();
        let mut pd_control = PdControl::new(config.pd.clone(), self.pd_client.get_security_mgr())?;
        pd_control.set_timeout(config.native_br.restore_timeout_pd_control.0);
        let keyspace = self
            .runtime
            .block_on(pd_control.get_keyspace_by_name(keyspace_name))?;
        assert_eq!(keyspace_name, keyspace.name);
        rfengine_cache.register_keyspace(keyspace.id, truncate_ts);
        Ok(())
    }

    fn fill_rfengine_cache(&self, backup_name: &str, backup_ts: u64) {
        let Some(rfengine_cache) = self.rfengine_cache.as_ref() else {
            return;
        };
        if let Err(err) =
            rfengine_cache.fill_cache(backup_name, backup_ts, self.object_cache.clone())
        {
            warn!("fail to fill rfengine cache: {:?}", err; "backup_name" => backup_name);
        }
    }

    fn restore_keyspace(
        &self,
        config: Config,
        restore_id: u64,
        keyspace_name: String,
        target_keyspace_name: String,
        restore_source: RestoreSource,
        restore_type: RestoreType,
    ) -> Result<()> {
        let ob_start_time = Instant::now();

        self.change_restore_state(
            restore_id,
            &target_keyspace_name,
            RestoreState::Running,
            None,
            0,
        )?;
        let progress_reporter = self.get_progress_reporter(restore_id).unwrap();

        let working_path = self.working_path(restore_id);
        let res = match self.restore_keyspace_core(
            config,
            working_path,
            &keyspace_name,
            &target_keyspace_name,
            restore_source,
            restore_type,
            progress_reporter.clone(),
        ) {
            Ok(ret) => {
                NATIVE_BR_COUNTER_VEC
                    .with_label_values(&["restore_keyspace_succeed"])
                    .inc();
                NATIVE_BR_HISTOGRAM_VEC
                    .with_label_values(&["restore_keyspace"])
                    .observe(ob_start_time.saturating_elapsed_secs());
                progress_reporter.report_step(RestoreStep::Finalize);
                let res = self.change_restore_state(
                    restore_id,
                    &target_keyspace_name,
                    RestoreState::Succeed,
                    None,
                    ret.restore_bytes,
                );
                self.remove_working_path(restore_id);
                res
            }
            Err(err) => {
                error!(
                    "{}->{} restore_keyspace error, restore_id {}, error {:?}",
                    keyspace_name, target_keyspace_name, restore_id, err
                );
                NATIVE_BR_COUNTER_VEC
                    .with_label_values(&["restore_keyspace_fail"])
                    .inc();
                self.change_restore_state(
                    restore_id,
                    &target_keyspace_name,
                    RestoreState::Error,
                    Some(err),
                    0,
                )
            }
        };
        if let Err(e) = res {
            NATIVE_BR_COUNTER_VEC
                .with_label_values(&["change_restore_state_fail"])
                .inc();
            return Err(e);
        }
        Ok(())
    }

    fn working_path(&self, restore_id: u64) -> PathBuf {
        self.data_dir
            .join(format!("{RESTORE_TASK_WORKING_PATH_PREFIX}{restore_id}"))
    }

    fn remove_working_path(&self, restore_id: u64) {
        let working_path = self.working_path(restore_id);
        if let Err(e) = fs::remove_dir_all(&working_path) {
            warn!("fail to remove working path: {:?}", e; "path" => working_path.display());
        }
    }

    fn persist_task_meta(&self, restore_id: u64, meta: &RestoreTaskMeta) -> Result<()> {
        static TMP_ID: AtomicU64 = AtomicU64::new(0);

        let working_path = self.working_path(restore_id);
        fs::create_dir_all(&working_path).ctx("create_working_path")?;
        let tmp_path = working_path.join(format!(
            "meta.json.{}",
            TMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let mut tmp = fs::File::create(&tmp_path).ctx("create_tmp")?;
        tmp.write_all(&serde_json::to_vec(&meta)?)
            .ctx("write_meta")?;
        tmp.sync_data().ctx("sync")?;
        drop(tmp);

        let meta_file = working_path.join("meta.json");
        fs::rename(tmp_path, meta_file).ctx("rename")?;
        Ok(())
    }

    fn read_task_meta(&self, restore_id: u64) -> Result<RestoreTaskMeta> {
        let working_path = self.working_path(restore_id);
        let meta_file = working_path.join("meta.json");
        if !meta_file.exists() {
            return Err(Error::CheckError(format!(
                "Meta file not found: {}",
                meta_file.display()
            )));
        }
        let meta = fs::read(&meta_file).ctx("read_meta")?;
        let meta = serde_json::from_slice::<RestoreTaskMeta>(&meta)?;
        Ok(meta)
    }

    fn init(&mut self) -> Result<()> {
        let entries = fs::read_dir(&self.data_dir).ctx("read_dir")?;
        for entry in entries {
            let entry = entry.ctx("entry")?;
            let path = entry.path();
            if path.is_dir() && path.file_name().is_some() {
                let name = path.file_name().unwrap().to_string_lossy();
                let name = name.as_ref();
                if let Some(restore_id) = name
                    .strip_prefix(RESTORE_TASK_WORKING_PATH_PREFIX)
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    match self.read_task_meta(restore_id) {
                        Ok(meta) => {
                            info!("restore task: {:?}", meta);
                            let task = RestoreTask::from_meta(meta);
                            self.restore_tasks.wl().insert(restore_id, task);
                        }
                        Err(err) => {
                            warn!("fail to read task meta: {:?}", err; "restore_id" => restore_id);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl Drop for BrContext {
    fn drop(&mut self) {
        self.backup_worker.stop();
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct NativeBrConfig {
    /// The time-to-live when restore task has been in final state.
    pub restore_task_ttl: ReadableDuration,

    /// The timeout for waiting the flush of mem-tables.
    pub restore_timeout_wait_flush: ReadableDuration,
    /// The timeout for the requests of restoring snapshots to TiKV servers.
    pub restore_timeout_restore_snapshot: ReadableDuration,
    pub restore_snapshot_concurrency: usize,
    /// The timeout for fetching the latest wal chunk from store.
    pub restore_timeout_fetch_wal: ReadableDuration,
    pub restore_timeout_pd_control: ReadableDuration,
    /// The maximum number of retries for the process from split regions to
    /// restore snapshots.
    pub restore_max_retry: usize,
    pub restore_coarse_split_regions_factor: usize,

    /// The timeout for instant backup.
    pub instant_backup_timeout: ReadableDuration,
    /// Interval for periodic backup. `0s` to disable periodic backup.
    pub backup_interval: ReadableDuration,
    /// Interval for gathering backup requests as a batch.
    pub backup_batch_interval: ReadableDuration,
    /// Delay before perform backup. Also used as the switch for passing
    /// `backup_ts` to rfengine during backup.
    pub backup_delay: ReadableDuration,
    pub backup_ts_wait_timeout: ReadableDuration,
    pub backup_ts_ttl: ReadableDuration,
    #[cfg(feature = "testexport")]
    pub backup_skip_keyspace_meta: bool,

    /// Whether to tolerate unavailability of no more than one store when
    /// backup.
    pub backup_tolerate_err: bool,
    /// Whether to tolerate unavailability of no more than one store when
    /// restore.
    pub restore_tolerate_err: bool,
    /// Whether to use rfengine cache on restore.
    pub retore_use_rfengine_cache: bool,

    pub restore_store_concurrency: usize,
    pub restore_concurrency_per_core: f64,
    pub restore_object_cache_capacity: AbsoluteOrPercentSize,
    pub restore_cache_for_decompressed_wal_chunks: bool,
    pub wal_chunk_target_file_size: ReadableSize,

    /// See `RestoreConfig::lower_memory`.
    pub lower_memory: bool,

    pub restore_rate_limit: native_br::limiter::RateLimitConfig,
}

impl Default for NativeBrConfig {
    fn default() -> Self {
        Self {
            restore_task_ttl: ReadableDuration::minutes(60),
            restore_timeout_wait_flush: restore::DEFAULT_TIMEOUT_WAIT_FLUSH,
            restore_timeout_restore_snapshot: restore::DEFAULT_TIMEOUT_RESTORE_SNAPSHOT,
            restore_snapshot_concurrency: restore::DEFAULT_RESTORE_SNAPSHOT_CONCURRENCY,
            restore_timeout_fetch_wal: restore::DEFAULT_TIMEOUT_FETCH_WAL,
            restore_timeout_pd_control: restore::DEFAULT_TIMEOUT_PD_CONTROL,
            restore_max_retry: restore::DEFAULT_RESTORE_MAX_RETRY,
            restore_coarse_split_regions_factor: 64,
            instant_backup_timeout: backup_worker::DEFAULT_TIMEOUT_INSTANT_BACKUP,
            backup_interval: ReadableDuration::ZERO,
            backup_batch_interval: ReadableDuration::secs(15),
            backup_delay: ReadableDuration::ZERO,
            backup_ts_wait_timeout: backup::BACKUP_TS_WAIT_TIMEOUT_DEFAULT,
            backup_ts_ttl: backup::BACKUP_TS_TTL_DEFAULT,
            #[cfg(feature = "testexport")]
            backup_skip_keyspace_meta: false,
            backup_tolerate_err: false,
            restore_tolerate_err: false,
            retore_use_rfengine_cache: false,
            restore_store_concurrency: restore_keyspace::RESTORE_RFENGINE_CONCURRENCY,
            restore_concurrency_per_core: 1.0,
            restore_object_cache_capacity: 0.into(),
            restore_cache_for_decompressed_wal_chunks: false,
            wal_chunk_target_file_size: ReadableSize::mb(64),
            lower_memory: false,
            restore_rate_limit: native_br::limiter::RateLimitConfig::default(),
        }
    }
}

impl NativeBrConfig {
    fn object_cache_average_item_size(&self) -> u64 {
        if self.restore_cache_for_decompressed_wal_chunks {
            self.wal_chunk_target_file_size.0
        } else {
            self.wal_chunk_target_file_size.0 / 3
        }
    }
}

pub(crate) struct NativeBrManager {
    pub context: Arc<BrContext>,
    pub config: RwLock<Config>,
}

impl NativeBrManager {
    pub(crate) fn new(
        runtime: Arc<Runtime>,
        pd_client: Arc<dyn PdClient>,
        dfs: Arc<dyn Dfs>,
        data_dir: PathBuf,
        config: Config,
    ) -> Self {
        let backup_config = config.to_backup_config();
        let backup_worker = BackupWorker::new(
            backup_config,
            pd_client.clone(),
            config.native_br.backup_interval.0,
            config.native_br.backup_batch_interval.0,
        );
        let restore_concurrency = cmp::max(
            (SysQuota::cpu_cores_quota() * config.native_br.restore_concurrency_per_core) as usize,
            2,
        );
        info!(
            "native_br: restore concurrency limit: {}",
            restore_concurrency
        );
        let object_cache_capacity = config
            .native_br
            .restore_object_cache_capacity
            .as_memory_size();
        let object_cache = (object_cache_capacity > 0).then(|| {
            info!("native_br: create object cache"; "capacity" => object_cache_capacity);
            ObjectCache::new(
                object_cache_capacity,
                config.native_br.object_cache_average_item_size(),
            )
        });
        let limiter = if config.native_br.restore_rate_limit.enable {
            Some(Arc::new(
                native_br::limiter::ThroughputLimiter::new(
                    &config.native_br.restore_rate_limit,
                    pd_client.clone(),
                    runtime.handle().clone(),
                )
                .expect("create restore rate limiter failed"),
            ))
        } else {
            None
        };
        let rfengine_cache_dir = data_dir.join("rfengine_cache");
        let rfengine_cache = if config.native_br.retore_use_rfengine_cache {
            Some(RfEngineCache::new(
                rfengine_cache_dir,
                config.to_restore_config(),
                dfs.clone(),
                pd_client.clone(),
            ))
        } else {
            None
        };
        let mut context = BrContext {
            pd_client,
            dfs,
            data_dir,
            runtime,
            restore_tasks: Default::default(),
            keyspace_tasks: Default::default(),
            backup_worker,
            restore_concurrency,
            object_cache,
            limiter,
            rfengine_cache,
        };
        if let Err(err) = context.init() {
            warn!("BR context init failed: {:?}", err);
        }
        Self {
            context: Arc::new(context),
            config: RwLock::new(config),
        }
    }

    pub(crate) fn update_native_br_config(&self, config: NativeBrConfig) {
        let mut ori_config = self.config.write().unwrap();
        if ori_config.native_br != config {
            info!(
                "Update native br config from {:?} to {:?}",
                ori_config.native_br, config
            );
            ori_config.native_br = config;
        }
    }

    async fn list_backups(
        &self,
        start_backup_time: &DateTime<Utc>,
        max_count: usize,
    ) -> Result<(Vec<IncrementalBackupFile>, bool)> {
        let (backups, has_more) = get_all_incremental_backups(
            self.context.dfs.as_ref(),
            &start_backup_time.date_naive(),
            Some(&start_backup_time.time()),
            max_count,
        )
        .await?;
        Ok((backups, has_more))
    }

    fn get_cluster_id(&self) -> Result<u64> {
        Ok(self.context.pd_client.get_cluster_id()?)
    }

    /// Return:
    ///   Ok(true): task started.
    ///   Ok(false): request ignored due to duplicated.
    ///   Err(err): error occurred.
    fn restore_keyspace(
        &self,
        restore_id: u64,
        keyspace_name: String,
        target_keyspace_name: String,
        restore_source: RestoreSource,
        restore_type: RestoreType,
    ) -> Result<bool> {
        let current_tasks = self.get_not_final_task_count();
        let limit = self.context.restore_concurrency;
        if current_tasks >= limit {
            warn!("reach concurrency limit"; "current" => current_tasks, "limit" => limit);
            // TODO: pending in a queue, other than return error directly.
            return Err(Error::ReachConcurrencyLimit {
                current: current_tasks,
                limit,
            });
        }

        let restore_params = RestoreParams::new(
            &restore_type,
            &restore_source,
            &keyspace_name,
            &target_keyspace_name,
        )?;

        if self.context.init_restore_state(
            restore_id,
            &target_keyspace_name,
            restore_type,
            restore_params,
        )? {
            let context = self.context.clone();
            let config = self.config.read().unwrap().clone();
            thread::spawn(move || {
                context.restore_keyspace(
                    config,
                    restore_id,
                    keyspace_name,
                    target_keyspace_name,
                    restore_source,
                    restore_type,
                )
            });
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Return:
    ///   Ok(Some): succeed.
    ///   Ok(None): not found.
    ///   Err(err): error occurred.
    fn restore_status(&self, restore_id: u64, keyspace_name: &str) -> Result<Option<RestoreTask>> {
        let tasks = self.context.restore_tasks.rl();
        if let Some(task) = tasks.get(&restore_id) {
            check_task(task, keyspace_name, None)?;
            Ok(Some(task.clone()))
        } else {
            Ok(None)
        }
    }

    /// Return all restore tasks in memory.
    fn get_all_restore_task(&self) -> HashMap<u64, RestoreTask> {
        self.context.restore_tasks.rl().clone()
    }

    /// Return number of tasks which are in not final state.
    fn get_not_final_task_count(&self) -> usize {
        self.context
            .restore_tasks
            .rl()
            .values()
            .filter(|task| !task.state.is_final())
            .count()
    }

    /// Return:
    ///   Ok(Some(true)): deleted.
    ///   Ok(Some(false)): ignored due state is not final.
    ///   Ok(None): restore task not found.
    ///   Err(err): error occurred.
    fn delete_restore(&self, restore_id: u64, keyspace_name: &str) -> Result<Option<bool>> {
        let mut tasks = self.context.restore_tasks.wl();
        if let Some(task) = tasks.get(&restore_id) {
            check_task(task, keyspace_name, None)?;
            if task.state.is_final() {
                tasks.remove(&restore_id);
                self.context.remove_working_path(restore_id);
                Ok(Some(true))
            } else {
                Ok(Some(false))
            }
        } else {
            Ok(None)
        }
    }

    pub fn cleanup_expired_restores(&self) {
        let ttl = self.config.rl().native_br.restore_task_ttl.0;
        let restores = {
            let tasks = self.context.restore_tasks.rl();
            tasks
                .iter()
                .filter_map(|(&restore_id, task)| {
                    task.ttl_expired(ttl)
                        .then_some((restore_id, task.keyspace_name.clone()))
                })
                .collect::<Vec<_>>()
        };
        for (restore_id, keyspace_name) in restores {
            if let Ok(Some(true)) = self.delete_restore(restore_id, &keyspace_name) {
                info!("{}({}) expired and removed", keyspace_name, restore_id);
            }
        }
    }

    async fn get_next_backup_after_ts(
        &self,
        ts: &DateTime<Utc>,
    ) -> Result<Option<IncrementalBackupFile>> {
        let (backup_files, _) = self.list_backups(ts, 1).await?;
        Ok(backup_files.first().cloned())
    }
}

fn check_task(
    task: &RestoreTask,
    keyspace_name: &str,
    request_params: Option<&RestoreParams>,
) -> Result<()> {
    if task.keyspace_name != keyspace_name {
        return Err(Error::CheckError(
            "restore id & keyspace not match".to_string(),
        ));
    }
    if let Some(request_params) = request_params {
        check_task_params(task, request_params)?;
    }
    Ok(())
}

fn check_task_params(task: &RestoreTask, request_params: &RestoreParams) -> Result<()> {
    // `task.restore_params.is_none()` is true when startup from meta of old
    // version.
    if task.restore_params.is_none() || &task.restore_params == request_params {
        Ok(())
    } else {
        let msg = format!(
            "restore params not match: task: {:?}, request: {:?}",
            task.restore_params, request_params
        );
        Err(Error::CheckError(msg))
    }
}

#[cfg(any(test, feature = "testexport"))]
pub mod test_utils {
    use std::sync::Arc;

    use chrono::{DateTime, Utc};
    use security::{HttpResult, RestfulClient, SecurityManager};

    use crate::native_br::{
        BackupItem, ListBackupResponse, RestoreProgressResponse, JSON_TIME_FORMAT,
    };

    #[derive(Default, Serialize, Deserialize, Debug)]
    #[serde(default)]
    struct DummyRequest {}

    pub struct NativeBrSvcClient {
        cluster_id: u64,
        inner: RestfulClient,
    }

    impl NativeBrSvcClient {
        pub fn new(
            cluster_id: u64,
            endpoints: Vec<String>,
            security_mgr: Arc<SecurityManager>,
        ) -> HttpResult<Self> {
            Ok(Self {
                cluster_id,
                inner: RestfulClient::new("native_br_cli", endpoints, security_mgr)?,
            })
        }

        pub async fn list_backups(
            &self,
            last_backup_time: &DateTime<Utc>,
        ) -> HttpResult<ListBackupResponse> {
            let query = url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs([
                    ("cluster_id", self.cluster_id.to_string()),
                    (
                        "last_backup_time",
                        last_backup_time.format(JSON_TIME_FORMAT).to_string(),
                    ),
                ])
                .finish();
            self.inner.get(format!("api/v1/backups?{query}")).await
        }

        pub async fn restore_keyspace_to_backup(
            &self,
            restore_id: u64,
            keyspace: String,
            source_keyspace: Option<String>,
            backup: &BackupItem,
        ) -> HttpResult<RestoreProgressResponse> {
            let mut serializer = url::form_urlencoded::Serializer::new(String::new());
            serializer.extend_pairs([
                ("cluster_id", self.cluster_id.to_string()),
                ("keyspace", keyspace),
                ("backup_id", backup.id.to_string()),
                ("backup_name", backup.name.clone()),
            ]);
            if let Some(source_keyspace) = source_keyspace {
                serializer.append_pair("source_keyspace", &source_keyspace);
            }
            let query = serializer.finish();
            self.inner
                .put(
                    format!("api/v1/restore_keyspace/{restore_id}?{query}"),
                    &DummyRequest {},
                )
                .await
        }

        pub async fn restore_keyspace_to_point_in_time(
            &self,
            restore_id: u64,
            keyspace: String,
            point_in_time: DateTime<Utc>,
        ) -> HttpResult<RestoreProgressResponse> {
            let query = url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs([
                    ("cluster_id", self.cluster_id.to_string()),
                    ("keyspace", keyspace),
                    (
                        "point_in_time",
                        point_in_time.format(JSON_TIME_FORMAT).to_string(),
                    ),
                ])
                .finish();
            self.inner
                .put(
                    format!("api/v1/restore_keyspace/{restore_id}?{query}"),
                    &DummyRequest {},
                )
                .await
        }

        pub async fn get_restore_progress(
            &self,
            restore_id: u64,
            keyspace: String,
        ) -> HttpResult<RestoreProgressResponse> {
            let query = url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs([
                    ("cluster_id", self.cluster_id.to_string()),
                    ("keyspace", keyspace),
                ])
                .finish();
            self.inner
                .get(format!("api/v1/restore_keyspace/{restore_id}?{query}"))
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use kvengine::dfs::test_util::new_test_s3fs;
    use pd_client::PdClient;
    use security::GetSecurityManager;
    use tikv_util::config::ReadableDuration;

    use super::*;
    use crate::native_br::RestoreState::{Init, Running, Succeed};

    #[test]
    fn test_restore_task_state() {
        let (mgr, _temp_dir) = new_test_br_manager("2s");
        let ctx = &mgr.context;

        // TODO: test for illegal state transition.
        let states_cases = vec![
            vec![Init],
            vec![Init, Running],
            vec![Init, Running, Succeed],
            vec![Init, Running, RestoreState::Error],
            vec![Init, Running, RestoreState::Error, Init],
        ];

        let backup = IncrementalBackupFile::from_datetime(Utc::now());
        let restore_params = RestoreParams {
            backup_name: Some(backup.name().to_string()),
            ..Default::default()
        };
        for (restore_id, states) in states_cases.into_iter().enumerate() {
            for state in states {
                let res = if state == Init {
                    ctx.init_restore_state(
                        restore_id as u64,
                        &format!("ks{}", restore_id),
                        RestoreType::Normal,
                        restore_params.clone(),
                    )
                } else {
                    ctx.change_restore_state(
                        restore_id as u64,
                        &format!("ks{}", restore_id),
                        state,
                        None,
                        0,
                    )
                };
                assert!(res.unwrap());
            }
        }

        assert_eq!(mgr.get_not_final_task_count(), 3);
        assert_eq!(mgr.get_all_restore_task().len(), 5);

        thread::sleep(Duration::from_secs(2));
        mgr.cleanup_expired_restores();
        assert_eq!(mgr.get_not_final_task_count(), 3);
        assert_eq!(mgr.get_all_restore_task().len(), 3);

        assert!(
            ctx.change_restore_state(1, "ks1", Succeed, None, 0)
                .unwrap()
        );
        mgr.cleanup_expired_restores();
        assert_eq!(mgr.get_not_final_task_count(), 2);
        assert_eq!(mgr.get_all_restore_task().len(), 3);

        // Test restore_params checking.
        // Incorrect keyspace name.
        ctx.change_restore_state(0, "ks1", Running, None, 0)
            .unwrap_err();
        // Transit to running.
        assert!(
            ctx.change_restore_state(0, "ks0", Running, None, 0)
                .unwrap()
        );
        assert!(
            ctx.change_restore_state(0, "ks0", RestoreState::Error, None, 0)
                .unwrap()
        );
        // Incorrect backup.
        let mut params1 = restore_params.clone();
        let backup1 =
            IncrementalBackupFile::try_from_full_path("cse-local/backup/20230321/112233.meta")
                .unwrap();
        params1.backup_name = Some(backup1.name().to_string());
        ctx.init_restore_state(0, "ks0", RestoreType::Normal, params1)
            .unwrap_err();
    }

    #[test]
    fn test_restore_task_meta() {
        // Test compatibility with meta of old version (without "restore_params").
        let mut meta = serde_json::from_str::<RestoreTaskMeta>(
            r#"{"keyspace_name":"ks","restore_type":"Normal","start":1000}"#,
        )
        .unwrap();
        assert!(meta.restore_params.is_none());

        for params in [
            RestoreParams::new(
                &RestoreType::Normal,
                &RestoreSource::ExistFile(IncrementalBackupFile::from_datetime(Utc::now()), None),
                "ks0",
                "ks1",
            )
            .unwrap(),
            RestoreParams::new(
                &RestoreType::Pitr,
                &RestoreSource::InstantBackup(Utc::now()),
                "ks2",
                "ks2",
            )
            .unwrap(),
        ] {
            meta.restore_params = params;
            assert_eq!(
                serde_json::from_slice::<RestoreTaskMeta>(&serde_json::to_vec(&meta).unwrap())
                    .unwrap(),
                meta
            );
        }
    }

    #[test]
    fn test_restore_params() {
        let utc = Utc::now();
        let backup = IncrementalBackupFile::from_datetime(utc);
        assert_eq!(
            RestoreParams::new(
                &RestoreType::Normal,
                &RestoreSource::ExistFile(backup.clone(), None),
                "ks0",
                "ks0",
            )
            .unwrap(),
            RestoreParams {
                backup_name: Some(backup.name().to_string()),
                ..Default::default()
            }
        );
        assert_eq!(
            RestoreParams::new(
                &RestoreType::Pitr,
                &RestoreSource::ExistFile(backup.clone(), Some(utc)),
                "ks1",
                "ks0",
            )
            .unwrap(),
            RestoreParams {
                point_in_time: Some(utc.timestamp()),
                source_keyspace: Some("ks1".to_string()),
                ..Default::default()
            }
        );
        assert_eq!(
            RestoreParams::new(
                &RestoreType::Pitr,
                &RestoreSource::InstantBackup(utc),
                "ks1",
                "ks1",
            )
            .unwrap(),
            RestoreParams {
                point_in_time: Some(utc.timestamp()),
                ..Default::default()
            }
        );

        RestoreParams::new(
            &RestoreType::Normal,
            &RestoreSource::InstantBackup(utc),
            "ks1",
            "ks1",
        )
        .unwrap_err();
        RestoreParams::new(
            &RestoreType::Pitr,
            &RestoreSource::ExistFile(backup, None),
            "ks1",
            "ks1",
        )
        .unwrap_err();
    }

    fn new_test_br_manager(restore_task_ttl: &str) -> (NativeBrManager, tempfile::TempDir) {
        let thread_pool = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(1)
                .thread_name("br_manager_test")
                .build()
                .unwrap(),
        );

        let file_data = "abcdefgh".to_string().into_bytes();
        let dfs = Arc::new(new_test_s3fs(&file_data));
        let temp_dir = tempfile::tempdir().unwrap();

        (
            NativeBrManager::new(
                thread_pool,
                Arc::new(MockPdClient {}),
                dfs,
                temp_dir.path().to_path_buf(),
                Config {
                    native_br: NativeBrConfig {
                        restore_task_ttl: ReadableDuration::from_str(restore_task_ttl).unwrap(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ),
            temp_dir,
        )
    }

    struct MockPdClient {}

    impl PdClient for MockPdClient {}

    impl GetSecurityManager for MockPdClient {}
}
