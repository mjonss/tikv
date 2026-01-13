// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! Common utilities for building workload run by SQLs.

use std::time::Duration;

use anyhow::{Context, Result};
use rand::{
    Rng,
    prelude::{SliceRandom, ThreadRng},
};
use sqlx::{Executor, MySql, MySqlPool, Row, pool::PoolConnection};
use tikv_util::{error, info, time::Instant};

use crate::test_txn_file::{TXN_CHUNK_MAX_SIZE, TXN_FILE_MIN_SIZE};

// Ref: https://github.com/tidbcloud/tidb-cse/blob/release-7.1-keyspace/kv/error.go, TxnRetryableMark
const TIDB_TXN_RETRYABLE_MARK: &str = "[try again later]";
pub(crate) const DEADLOCK_ERR_MSG: &str = "Deadlock found";
const RETRYABLE_DB_ERR_MSGS: &[&str] = &[
    TIDB_TXN_RETRYABLE_MARK,
    "Write conflict",
    "Region is unavailable", // Reason: region merged / server is busy.
    // TODO: verify following errors.
    DEADLOCK_ERR_MSG,
    "Information schema is out of date",
    "Lock wait timeout exceeded",
    "lock wait timeout",
    "Region epoch not match for region",
    "Region epoch not match after retries",
    "tikv aborts txn",
    "Resolve lock timeout",
    "public column val0 has changed", // Reason: modify column val0/val1 in unique workload.
    "public column val1 has changed",
];

const RETRYABLE_TSO_ERR_MSGS: &[&str] = &[
    "server not started", // TSO service is stopping.
    "requested pd is not leader of cluster",
];

pub(crate) fn is_db_error_retryable(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => RETRYABLE_DB_ERR_MSGS
            .iter()
            .chain(RETRYABLE_TSO_ERR_MSGS.iter())
            .any(|msg| db_err.message().contains(msg)),
        sqlx::Error::Io(_) => true,
        _ => false,
    }
}

pub(crate) fn is_error_retryable(err: &anyhow::Error) -> bool {
    err.downcast_ref::<sqlx::Error>()
        .is_some_and(is_db_error_retryable)
}

macro_rules! retry_or_panic {
    ($expr:expr, $on_retry:expr) => {{
        match $expr {
            Ok(r) => r,
            Err(e) if $crate::sql_util::is_error_retryable(&e) => {
                info!("meet error, retry: {:?}", e);

                // To fix clippy error: "try not to call a closure in the expression where it is
                // declared".
                let on_retry = $on_retry;
                on_retry(&e);

                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(e) => panic!("meet error: {:?}", e),
        }
    }};
    ($expr:expr) => {{ retry_or_panic!($expr, |_| {}) }};
}

pub(crate) use retry_or_panic;

const DUPLICATE_ENTRY_ERR_MSGS: &[&str] = &["Duplicate entry"];

fn is_db_error_duplicate_entry(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => DUPLICATE_ENTRY_ERR_MSGS
            .iter()
            .any(|msg| db_err.message().contains(msg)),
        sqlx::Error::Io(_) => true,
        _ => false,
    }
}

pub(crate) fn is_duplicate_entry_err(err: &anyhow::Error) -> bool {
    err.downcast_ref::<sqlx::Error>()
        .is_some_and(is_db_error_duplicate_entry)
}

pub(crate) const MAX_PADDING_SIZE: usize = TXN_CHUNK_MAX_SIZE * 2;

/// Generate padding to meet transaction size requirement of txn file.
pub(crate) fn gen_padding(rows: usize, rng: &mut ThreadRng, buf: &mut [u8]) -> usize {
    static TRANSACTION_SIZE: [usize; 4] = [
        TXN_FILE_MIN_SIZE,
        TXN_CHUNK_MAX_SIZE / 2,
        TXN_CHUNK_MAX_SIZE,
        MAX_PADDING_SIZE,
    ];
    let trans_size = *TRANSACTION_SIZE.choose(rng).unwrap();
    let row_size = trans_size.div_ceil(rows);
    rng.fill(&mut buf[..row_size]);
    row_size
}

pub async fn query_tiflash_or_columnar_progress<'a, E>(
    executor: E,
    db: &str,
    tb: &str,
) -> Result<(bool /* available */, f64 /* progress */)>
where
    E: sqlx::Executor<'a, Database = MySql>,
{
    let sql = format!(
        "select available, progress from information_schema.tiflash_replica where TABLE_SCHEMA='{db}' and TABLE_NAME='{tb}'"
    );
    let row = sqlx::query(&sql).fetch_one(executor).await.context(sql)?;
    let available: i32 = row.get("available");
    let progress: f64 = row.get("progress");
    Ok((available != 0, progress))
}

pub async fn wait_tiflash_or_columnar_replicas_available(
    tag: &str,
    pool: &sqlx::pool::Pool<MySql>,
    db: &str,
    tb: &str,
    timeout: Duration,
) {
    let start = Instant::now_coarse();
    while start.saturating_elapsed() < timeout {
        let (available, progress) = query_tiflash_or_columnar_progress(pool, db, tb)
            .await
            .unwrap();
        info!("{} wait TiFlash/Columnar replicas", tag; "db" => db, "tb" => tb,
            "available" => available, "progress" => progress);
        if available {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let (available, progress) = query_tiflash_or_columnar_progress(pool, db, tb)
        .await
        .unwrap();
    panic!(
        "{} TiFlash/Columnar replicas not available, db {}, tb {}, available {}, progress {}",
        tag, db, tb, available, progress,
    );
}

// Hint: /*+ READ_FROM_STORAGE(TIFLASH[t1], TIKV[t2]) */
pub fn get_engine_hint(use_tiflash: bool, tb: &str) -> String {
    let engine = if use_tiflash { "TIFLASH" } else { "TIKV" };
    format!("/*+ READ_FROM_STORAGE({engine}[{tb}]) */")
}

pub struct Transaction {
    conn: Option<PoolConnection<MySql>>,
    tag: String,
    start_ts: u64,
    committed: bool,
    read_only: bool,
}

impl Transaction {
    pub async fn begin(tag: &str, pool: &MySqlPool, optimistic_txn: bool) -> anyhow::Result<Self> {
        let mut conn = pool.acquire().await.context("acquire")?;

        let txn_mode = if optimistic_txn {
            "optimistic"
        } else {
            "pessimistic"
        };
        conn.execute(format!("begin {txn_mode}").as_ref())
            .await
            .context("begin")?;

        let tso: i64 = conn
            .fetch_one("select TIDB_CURRENT_TSO() as tso")
            .await
            .context("select_current_tso")?
            .get("tso");

        let tag = format!("{}:{}", tag, tso);
        Ok(Self {
            tag,
            conn: Some(conn),
            start_ts: tso as u64,
            committed: false,
            read_only: false,
        })
    }

    pub async fn begin_read(tag: &str, pool: &MySqlPool) -> anyhow::Result<Self> {
        let mut txn = Self::begin(tag, pool, false).await?;
        txn.read_only = true;
        Ok(txn)
    }

    pub async fn commit(&mut self) -> anyhow::Result<()> {
        assert!(!self.committed & !self.read_only);
        match self.conn().execute("commit").await {
            Ok(_) => {
                self.committed = true;
                Ok(())
            }
            Err(err) => {
                error!("{} commit failed", self.tag; "err" => ?err);
                Err(err).context("commit")
            }
        }
    }

    pub fn conn(&mut self) -> &mut PoolConnection<MySql> {
        self.conn.as_mut().unwrap()
    }

    pub fn start_ts(&self) -> u64 {
        self.start_ts
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.committed && !self.read_only {
            let tag = self.tag.clone();
            let mut conn = self.conn.take().unwrap();
            let task = async move {
                let res = conn.execute("rollback").await;
                if let Err(err) = res {
                    error!("{} rollback failed", tag; "err" => ?err);
                    // Detach to abort the connection. Otherwise, the current progress would be
                    // committed by another transaction.
                    // See https://github.com/tidbcloud/cloud-storage-engine/issues/2300.
                    let _ = conn.detach();
                }
            };
            drop(tokio::spawn(task));
        }
    }
}

mod tests {
    use std::io;

    use super::*;

    #[test]
    fn test_is_error_retryable() {
        let res: anyhow::Result<()> = Err(sqlx::Error::Io(io::Error::new(
            io::ErrorKind::Other,
            "test",
        )))
        .context("io");
        let Err(err) = res else { panic!("unexpected") };
        assert!(is_error_retryable(&err));
    }
}
