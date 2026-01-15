// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cell::Cell, fs, path::PathBuf, process::Command, thread, time::Duration};

use anyhow::{Context, Result};
use futures::executor::block_on;
use lazy_static::lazy_static;
use serde_derive::Serialize;
use tikv_util::{error, future::paired_future_callback, info, time::Instant, warn};
use txn_types::TimeStamp;

use crate::{TryWaiter, tidb::ConnParams};

const OUTPUT_DIR: &str = "output";
const DIFF_CONFIG_FILE: &str = "diff_config.toml";
const SYNC_DIFF_INSPECTOR_BIN: &str = "/usr/bin/sync_diff_inspector";

/// Wrapper of sync_diff_inspector https://github.com/pingcap/tidb-tools/tree/master/sync_diff_inspector
pub struct SyncDiffInspector {
    work_dir: PathBuf,
}

impl SyncDiffInspector {
    /// `tables`: Can use wildcard, e.g. ["test.*"]
    pub fn new(
        work_dir: PathBuf,
        upstream: ConnParams,
        downstream: ConnParams,
        tables: Vec<String>,
        use_snapshot: bool,
        skip_non_existing_table: bool,
    ) -> Self {
        fs::create_dir_all(&work_dir).unwrap();
        let config = Config {
            check_thread_count: 4,
            export_fix_sql: true,
            check_struct_only: false,
            skip_non_existing_table,
            task: Task {
                output_dir: work_dir.join(OUTPUT_DIR).to_string_lossy().to_string(),
                target_check_tables: tables,
                ..Default::default()
            },
            data_sources: DataSources {
                upstream: DbConfig::new(upstream, use_snapshot),
                downstream: DbConfig::new(downstream, use_snapshot),
            },
        };
        let toml = toml::to_string(&config).unwrap();
        let config_file = work_dir.join(DIFF_CONFIG_FILE);
        fs::write(config_file, toml).unwrap();

        Self { work_dir }
    }

    pub fn compare(&self) -> Result<CompareSummary> {
        let config_file = self.work_dir.join(DIFF_CONFIG_FILE);
        let mut cmd = Command::new(SYNC_DIFF_INSPECTOR_BIN);
        cmd.arg(format!("--config={}", config_file.display()));
        let output = cmd.output().expect("failed to execute sync_diff_inspector");

        let success = output.status.success();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        if success {
            info!("sync_diff_inspector success"; "stdout" => stdout.as_ref(), "stderr" => &stderr.as_ref());
        } else {
            error!("sync_diff_inspector failed"; "stdout" => stdout.as_ref(), "stderr" => &stderr.as_ref());
        }

        let summary_file = self.work_dir.join(OUTPUT_DIR).join("summary.txt");
        let mut summary = CompareSummary {
            success,
            ..Default::default()
        };
        let content = fs::read_to_string(summary_file).unwrap();
        parse_compare_summary(&content, &mut summary).unwrap();

        Ok(summary)
    }
}

// Diff config:
#[rustfmt::skip]
/*
check-thread-count = 4
export-fix-sql = true
check-struct-only = false

[task]
    output-dir = "output"
    source-instances = ["upstream"]
    target-instance = "downstream"
    target-check-tables = ["db.*"]

[data-sources]
[data-sources.upstream]
    host = "10.2.8.125"
    port = 4444
    user = "test8.root"
    password = ""
    snapshot = "auto"

[data-sources.downstream]
    host = "10.2.8.125"
    port = 4455
    user = "root"
    password = ""
    snapshot = "auto"
 */
#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct Config {
    check_thread_count: u32,
    export_fix_sql: bool,
    check_struct_only: bool,
    skip_non_existing_table: bool,
    task: Task,
    data_sources: DataSources,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct Task {
    output_dir: String,
    source_instances: Vec<String>,
    target_instance: String,
    target_check_tables: Vec<String>,
}

impl Default for Task {
    fn default() -> Self {
        Self {
            output_dir: OUTPUT_DIR.into(),
            source_instances: vec!["upstream".to_string()],
            target_instance: "downstream".to_string(),
            target_check_tables: vec!["test.*".to_string()],
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct DataSources {
    upstream: DbConfig,
    downstream: DbConfig,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct DbConfig {
    host: String,
    port: u16,
    user: String,
    password: String,
    snapshot: String,
}

impl DbConfig {
    fn new(params: ConnParams, use_snapshot: bool) -> Self {
        Self {
            host: params.host,
            port: params.port,
            user: params.user,
            password: params.password,
            snapshot: if use_snapshot {
                "auto".to_string()
            } else {
                "".to_string()
            },
        }
    }
}

// Result summary:
#[rustfmt::skip]
/*
Summary



Source Database



host = "10.2.8.125"
port = 4444
user = "test8.root"
snapshot = "460427039207849984"

Target Databases



host = "10.2.8.125"
port = 4455
user = "root"
snapshot = "460427040118800384"

Comparison Result



The table structure and data in following tables are equivalent

+--------------------+---------+-----------+
|       TABLE        | UPCOUNT | DOWNCOUNT |
+--------------------+---------+-----------+
| `sbtest`.`sbtest1` |    1000 |      1000 |
| `sbtest`.`sbtest2` |    1000 |      1000 |
+--------------------+---------+-----------+



Time Cost: 35.593733ms
Average Speed: 10.556569MB/s
 */
#[derive(Default, Debug, PartialEq)]
pub struct CompareSummary {
    pub success: bool,
    pub upstream_snapshot: Option<u64>,
    pub downstream_snapshot: Option<u64>,
}

const SOURCE_DATABASE_SECTION: &str = "Source Database";
const TARGET_DATABASES_SECTION: &str = "Target Databases";

fn parse_compare_summary(content: &str, summary: &mut CompareSummary) -> Result<()> {
    lazy_static! {
        static ref SNAPSHOT_RE: regex::Regex =
            regex::Regex::new(r#"snapshot\s*=\s*"(\d+)""#).unwrap();
    }
    let mut section = "";
    for line in content.lines() {
        let line = line.trim();
        match line {
            SOURCE_DATABASE_SECTION => section = SOURCE_DATABASE_SECTION,
            TARGET_DATABASES_SECTION => section = TARGET_DATABASES_SECTION,
            line if line.starts_with("snapshot = ") => {
                let caps = SNAPSHOT_RE.captures(line).context("captures")?;
                let m = caps.get(1).unwrap();
                let snapshot = m.as_str().parse::<u64>().context("parse::u64")?;
                match section {
                    SOURCE_DATABASE_SECTION => summary.upstream_snapshot = Some(snapshot),
                    TARGET_DATABASES_SECTION => summary.downstream_snapshot = Some(snapshot),
                    _ => {}
                }
            }
            _ => {}
        }
    }

    Ok(())
}

pub enum SyncDiffTask {
    Compare(Box<dyn FnOnce(Option<CompareSummary>) + Send>),
    Stop(Box<dyn FnOnce(()) + Send>),

    /// To skip the assertion of compare result UNTIL the
    /// `CompareSummary.upstream_snapshot` >= `SkipUntil.snapshot`.
    ///
    /// When changefeed is paused & resumed, TiCDC will sync from the latest
    /// checkpoint. In this case, changes will be replayed from the checkpoint
    /// to last actual sync point, and the upstream & downstream will not be
    /// identical during this process.
    SkipUntil {
        snapshot: u64,
        cb: Box<dyn FnOnce(()) + Send>,
    },
}

pub struct SyncDiffer {
    task_tx: tikv_util::mpsc::Sender<SyncDiffTask>,
}

impl SyncDiffer {
    pub fn new(
        work_dir: PathBuf,
        upstream: ConnParams,
        downstream: ConnParams,
        tables: Vec<String>,
        compare_interval: Duration,
    ) -> Self {
        fs::create_dir_all(&work_dir).unwrap();

        let downstream_cp = downstream.clone();
        let wait_syncpoint_task =
            thread::spawn(move || wait_syncpoint_table(&downstream_cp, Duration::from_secs(180)));

        let (task_tx, task_rx) = tikv_util::mpsc::unbounded();
        thread::spawn(move || {
            let use_snapshot = true;
            // Tolerate the error when the "create table" has not been synced.
            let skip_non_existing_table = true;
            let sync_diff_inspector = SyncDiffInspector::new(
                work_dir,
                upstream,
                downstream,
                tables,
                use_snapshot,
                skip_non_existing_table,
            );

            let mut runner = SyncDiffRunner {
                sync_diff_inspector,
                task_rx,
                compare_interval,
                skip_until_snapshot: None,
                wait_syncpoint_task: Some(wait_syncpoint_task),
            };
            runner.run();
        });

        Self { task_tx }
    }

    pub async fn compare(&self) -> Option<CompareSummary> {
        let (cb, fut) = paired_future_callback();
        self.task_tx
            .send(SyncDiffTask::Compare(Box::new(cb)))
            .unwrap();
        fut.await.unwrap()
    }

    pub async fn stop(&self) {
        let (cb, fut) = paired_future_callback();
        self.task_tx.send(SyncDiffTask::Stop(Box::new(cb))).unwrap();
        fut.await.unwrap();
    }

    pub async fn skip_until(&self, snapshot: u64) {
        let (cb, fut) = paired_future_callback();
        self.task_tx
            .send(SyncDiffTask::SkipUntil {
                snapshot,
                cb: Box::new(cb),
            })
            .unwrap();
        fut.await.unwrap();
    }

    #[track_caller]
    pub fn must_wait_sync_to(
        &self,
        sync_ts: u64,
        wait_timeout: Duration,
        no_progress_timeout: Duration,
        retry_interval: Duration,
    ) {
        let last_upstream_snapshot = Cell::new(0);
        let mut last_upstream_time = Instant::now_coarse();

        TryWaiter::timeout_dur(wait_timeout)
            .interval_dur(retry_interval)
            .must_wait(
                || {
                    let Some(summary) = block_on(self.compare()) else {
                        return false;
                    };
                    info!("sync_diff: compare result: {:?}", summary; "sync_ts" => sync_ts);
                    assert!(summary.success);

                    let upstream_snapshot = summary.upstream_snapshot.unwrap_or_default();
                    let ok = upstream_snapshot >= sync_ts;
                    if !ok {
                        if last_upstream_snapshot.get() != upstream_snapshot {
                            last_upstream_snapshot.set(upstream_snapshot);
                            last_upstream_time = Instant::now_coarse();
                        } else {
                            let elapsed = last_upstream_time.saturating_elapsed();
                            if elapsed > no_progress_timeout {
                                panic!("sync_diff: no progress for {:?}, snapshot {}", elapsed, upstream_snapshot);
                            }
                        }
                    }
                    ok
                },
                || {
                    let last_snapshot = last_upstream_snapshot.get();
                    let lag = Duration::from_millis(
                        TimeStamp::from(sync_ts.saturating_sub(last_snapshot))
                            .physical(),
                    );
                    format!("sync_diff: wait sync to {sync_ts} timeout, lag: {lag:?}, last_snapshot: {last_snapshot}")
                },
            );
    }
}

struct SyncDiffRunner {
    sync_diff_inspector: SyncDiffInspector,
    task_rx: tikv_util::mpsc::Receiver<SyncDiffTask>,
    compare_interval: Duration,
    skip_until_snapshot: Option<u64>,
    wait_syncpoint_task: Option<thread::JoinHandle<Result<()>>>,
}

impl SyncDiffRunner {
    fn run(&mut self) {
        loop {
            if let Ok(task) = self.task_rx.recv_timeout(self.compare_interval) {
                match task {
                    SyncDiffTask::Compare(cb) => {
                        self.wait_syncpoint_task_finished_blocking();
                        let summary = self.compare();
                        cb(summary);
                    }
                    SyncDiffTask::Stop(cb) => {
                        self.wait_syncpoint_task_finished_blocking();
                        cb(());
                        return;
                    }
                    SyncDiffTask::SkipUntil { snapshot, cb } => {
                        info!("sync_diff_inspector: pause until {}", snapshot);
                        self.skip_until_snapshot = Some(snapshot);
                        cb(());
                    }
                }
            } else {
                if !self.try_wait_syncpoint_task_finished() {
                    warn!("sync_diff_inspector: syncpoint table not ready, skip periodic compare");
                    continue;
                }
                let Some(summary) = self.compare() else {
                    continue;
                };
                info!("sync_diff_inspector compare"; "summary" => ?summary);
                assert!(summary.success);
            }
        }
    }

    fn wait_syncpoint_task_finished_blocking(&mut self) {
        let Some(handle) = self.wait_syncpoint_task.take() else {
            return;
        };
        match handle.join() {
            Ok(res) => res.unwrap_or_else(|e| panic!("wait syncpoint table failed: {e:#}")),
            Err(panic_err) => std::panic::resume_unwind(panic_err),
        }
    }

    fn try_wait_syncpoint_task_finished(&mut self) -> bool {
        let Some(handle) = self.wait_syncpoint_task.as_ref() else {
            return true;
        };
        if !handle.is_finished() {
            return false;
        }
        self.wait_syncpoint_task_finished_blocking();
        true
    }

    /// Return `None` when the snapshot of compare result is skipped.
    fn compare(&mut self) -> Option<CompareSummary> {
        let summary = self.sync_diff_inspector.compare().unwrap();
        if let (Some(skip_until), Some(upstream_snapshot)) =
            (self.skip_until_snapshot, summary.upstream_snapshot)
        {
            if upstream_snapshot <= skip_until {
                info!("sync_diff_inspector compare skipped"; "summary" => ?summary, "skip_until" => skip_until);
                return None;
            } else {
                self.skip_until_snapshot = None;
            }
        }
        Some(summary)
    }
}

fn wait_syncpoint_table(downstream: &ConnParams, timeout: Duration) -> Result<()> {
    const SYNCPOINT_SCHEMA: &str = "tidb_cdc";
    const SYNCPOINT_TABLE: &str = "syncpoint_v1";

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    runtime.block_on(async move {
        let start = Instant::now_coarse();
        let mut pool: Option<sqlx::MySqlPool> = None;

        loop {
            if start.saturating_elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "wait syncpoint table timeout: {SYNCPOINT_SCHEMA}.{SYNCPOINT_TABLE} \
                        is not ready after {:?} (downstream {}:{})",
                    timeout,
                    downstream.host,
                    downstream.port,
                ));
            }

            if pool.is_none() {
                let mut opts = sqlx::mysql::MySqlConnectOptions::new()
                    .host(&downstream.host)
                    .port(downstream.port)
                    .username(&downstream.user)
                    .database("information_schema");
                if !downstream.password.is_empty() {
                    opts = opts.password(&downstream.password);
                }
                match sqlx::mysql::MySqlPoolOptions::new()
                    .max_connections(1)
                    .connect_with(opts)
                    .await
                {
                    Ok(p) => pool = Some(p),
                    Err(e) => {
                        info!(
                            "wait syncpoint table: connect downstream tidb failed, retrying";
                            "err" => ?e,
                            "host" => &downstream.host,
                            "port" => downstream.port,
                        );
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                }
            }

            let table_rows = match sqlx::query_scalar::<_, i64>(
                "SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES \
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? LIMIT 1",
            )
            .bind(SYNCPOINT_SCHEMA)
            .bind(SYNCPOINT_TABLE)
            .fetch_optional(pool.as_ref().unwrap())
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    info!(
                        "wait syncpoint table: query failed, retrying";
                        "err" => ?e,
                        "host" => &downstream.host,
                        "port" => downstream.port,
                    );
                    pool = None;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            if table_rows.unwrap_or_default() > 0 {
                info!(
                    "wait syncpoint table: table is ready";
                    "table" => format!("{SYNCPOINT_SCHEMA}.{SYNCPOINT_TABLE}"),
                    "host" => &downstream.host,
                    "port" => downstream.port,
                    "elapsed" => ?start.saturating_elapsed(),
                );
                return Ok(());
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_compare_summary() {
        #[rustfmt::skip]
        let content = r#"
Summary



Source Database



host = "10.2.8.125"
port = 4444
user = "test8.root"
snapshot = "460427039207849984"

Target Databases



host = "10.2.8.125"
port = 4455
user = "root"
snapshot = "460427040118800384"

Comparison Result



The table structure and data in following tables are equivalent

+--------------------+---------+-----------+
|       TABLE        | UPCOUNT | DOWNCOUNT |
+--------------------+---------+-----------+
| `sbtest`.`sbtest1` |    1000 |      1000 |
| `sbtest`.`sbtest2` |    1000 |      1000 |
+--------------------+---------+-----------+



Time Cost: 35.593733ms
Average Speed: 10.556569MB/s
"#;

        let mut summary = CompareSummary {
            success: true,
            ..Default::default()
        };
        parse_compare_summary(content, &mut summary).unwrap();
        let expect = CompareSummary {
            success: true,
            upstream_snapshot: Some(460427039207849984),
            downstream_snapshot: Some(460427040118800384),
        };
        assert_eq!(summary, expect);
    }
}
