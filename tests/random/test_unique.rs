// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! Test for the scene that data have unique constraint.

use std::{fmt, sync::atomic::Ordering::Relaxed, time::Duration};

use anyhow::{Context, Result};
use futures::future::join_all;
use rand::prelude::*;
use sqlx::{Executor, MySql, MySqlPool, Row};
use test_cloud_server::{keyspace::KeyspaceManager, tidb::TidbCluster};
use tikv_util::{debug, error, info, time::Instant, warn};

use crate::{
    Running, UNIQUE_TABLE_DDL_COUNTER, UNIQUE_WORKLOAD_CONFLICT_COUNTER,
    UNIQUE_WORKLOAD_TXN_COUNTER,
    sql_util::{
        MAX_PADDING_SIZE, Transaction, gen_padding, is_duplicate_entry_err, is_error_retryable,
        retry_or_panic,
    },
    test_tidb::connect_tidb,
};

const UNIQUE_WORKLOAD_CONCURRENCY: usize = 4;
pub(crate) const UNIQUE_DB_NAME: &str = "uniq";
pub(crate) const UNIQUE_TABLE_NAME: &str = "rows";

const VALUE0_MAX: i32 = 1000;

/// 0.1% stock data which are inserted during preparation and will not be
/// deleted. So the probability of conflict for a insert with 100 rows will be
/// about (1-0.1%)^100 = 90%
const STOCK_DATA_NUM: i32 = VALUE0_MAX / 1000;

const INSERT_BATCH_SIZE_OPTS: &[i32] = &[
    VALUE0_MAX / 100,
    VALUE0_MAX / 20,
    VALUE0_MAX / 10,
    VALUE0_MAX / 5,
]; // 10, 50, 100, 200

pub(crate) async fn prepare_unique_workload(
    tc: TidbCluster,
    keyspace_manager: KeyspaceManager,
    keyspace_id: u32,
) {
    let tag = format!("unique-ks{}", keyspace_id);
    let pool = connect_tidb(&tc, &keyspace_manager, keyspace_id).await;

    info!("{} prepare_unique_workload", tag);

    let mut sqls = vec![
        format!("drop database if exists `{UNIQUE_DB_NAME}`"),
        format!("create database `{UNIQUE_DB_NAME}`"),
        format!(
            "create table `{UNIQUE_DB_NAME}`.`{UNIQUE_TABLE_NAME}` \
            (id int not null auto_increment, \
            val0 int not null, \
            val1 int not null, \
            padding varbinary({MAX_PADDING_SIZE}) not null default 0x0, \
            primary key (id), \
            unique key `val0_unique_idx` (val0), \
            key `val1_idx` (val1))"
        ),
    ];

    {
        // Insert stock data.
        let mut rng = thread_rng();
        let mut padding = [0u8; MAX_PADDING_SIZE];
        let padding_len = gen_padding(STOCK_DATA_NUM as usize, &mut rng, &mut padding);
        let hex_padding = hex::encode(&padding[..padding_len]);

        let mut values = Vec::with_capacity(STOCK_DATA_NUM as usize);
        for val0 in 0..STOCK_DATA_NUM {
            let val1 = val0 * 10;
            values.push(format!("({val0}, {val1}, 0x{hex_padding})"));
        }

        let sql = format!(
            "insert into `{UNIQUE_DB_NAME}`.`{UNIQUE_TABLE_NAME}` (val0, val1, padding) values {}",
            values.join(", ")
        );
        sqls.push(sql);
    }

    for sql in sqls {
        info!("{} prepare_unique_workload", tag; "sql" => &sql);
        sqlx::query(&sql).execute(&pool).await.unwrap();
    }
}

pub(crate) async fn run_unique_workload(
    tc: TidbCluster,
    keyspace_manager: KeyspaceManager,
    keyspace_id: u32,
    use_txn_file: bool,
    run_ddl: bool,
    running: Running,
) {
    info!("run_unique_workload"; "use_txn_file" => use_txn_file);
    let pool = connect_tidb(&tc, &keyspace_manager, keyspace_id).await;

    let mut handles = Vec::with_capacity(UNIQUE_WORKLOAD_CONCURRENCY);
    for tid in 0..UNIQUE_WORKLOAD_CONCURRENCY {
        let pool = pool.clone();
        let running = running.clone();
        let handle = tokio::spawn(async move {
            let tag = format!("unique-{}-{}", keyspace_id, tid);

            let mut padding = [0u8; MAX_PADDING_SIZE];
            let start_time = Instant::now();
            while running.get() {
                let (insert_sql, delete_sql) = {
                    let mut rng = thread_rng();

                    let mut contains_stock_data = false;

                    let mut val0 = rng.gen_range(0..VALUE0_MAX);
                    let batch_size = *INSERT_BATCH_SIZE_OPTS.choose(&mut rng).unwrap() as usize;
                    // Generate padding based on 2 x smallest batch size, to generate hybrid
                    // workload with both pessimistic & txn file transactions.
                    let padding_len = gen_padding(
                        INSERT_BATCH_SIZE_OPTS[0] as usize * 2,
                        &mut rng,
                        &mut padding,
                    );
                    let hex_padding = hex::encode(&padding[..padding_len]);

                    let mut val0s = Vec::with_capacity(batch_size);
                    let mut values = Vec::with_capacity(batch_size);
                    for _ in 0..batch_size {
                        if val0 < STOCK_DATA_NUM {
                            contains_stock_data = true;
                        }
                        if !contains_stock_data {
                            val0s.push(val0);
                        }

                        let val1 = val0 * 10;
                        values.push(format!("({val0}, {val1}, 0x{hex_padding})"));

                        val0 = (val0 + 1) % VALUE0_MAX;
                    }

                    let insert_sql = format!(
                        "insert into `{UNIQUE_DB_NAME}`.`{UNIQUE_TABLE_NAME}` (val0, val1, padding) values {}",
                        values.join(", ")
                    );
                    let delete_sql: Option<String> = (!contains_stock_data).then(|| format!(
                        "delete from `{UNIQUE_DB_NAME}`.`{UNIQUE_TABLE_NAME}` where val0 in ({})",
                        val0s
                            .iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                    (insert_sql, delete_sql)
                };

                let expect_conflict = delete_sql.is_none();
                match do_sql(&tag, &pool, &insert_sql, use_txn_file).await {
                    Ok(()) => {
                        if expect_conflict {
                            let mut conn = pool.acquire().await.unwrap();
                            match list_rows(&mut conn).await {
                                Ok(rows) => {
                                    info!("{} unique workload list rows", tag; "rows" => ?rows);
                                }
                                Err(err) => {
                                    warn!("{} unique workload list rows failed: {:?}", tag, err);
                                }
                            }
                            panic!(
                                "{} unique workload insert should meet conflict, sql {}",
                                tag, insert_sql
                            );
                        }
                        UNIQUE_WORKLOAD_TXN_COUNTER.fetch_add(1, Relaxed);
                    }
                    Err(err) if is_duplicate_entry_err(&err) => {
                        info!("{} unique workload meet conflict", tag; "err" => ?err, "expect_conflict" => expect_conflict);
                        UNIQUE_WORKLOAD_CONFLICT_COUNTER.fetch_add(1, Relaxed);
                    }
                    Err(err) if is_error_retryable(&err) => {
                        warn!("{} unique workload perform insert failed, retry", tag; "err" => ?err);
                    }
                    Err(err) => panic!(
                        "{} unique workload perform insert failed: {:?}, sql: {}",
                        tag, err, insert_sql
                    ),
                }

                if let Some(delete_sql) = delete_sql {
                    match do_sql(&tag, &pool, &delete_sql, use_txn_file).await {
                        Ok(()) => {}
                        Err(err) if is_error_retryable(&err) => {
                            warn!("{} unique workload perform delete failed, retry", tag; "err" => ?err);
                        }
                        Err(err) => {
                            #[cfg(feature = "debug-trace-txn-tasks")]
                            {
                                tikv::storage::txn::debug::dump_txn_tasks();
                            }
                            panic!("{} unique workload perform delete failed: {:?}", tag, err)
                        }
                    }
                }
            }
            info!("unique workload exit"; "tag" => tag, "dur" => ?start_time.saturating_elapsed());
        });
        handles.push(handle);
    }

    if run_ddl {
        let pool = pool.clone();
        let running = running.clone();
        handles.push(tokio::spawn(async move {
            let start_time = Instant::now();
            while running.get() {
                retry_or_panic!(run_modify_column(&pool).await.context("run_modify_column"));
                UNIQUE_TABLE_DDL_COUNTER.fetch_add(1, Relaxed);
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
            info!("modify column thread exit"; "dur" => ?start_time.saturating_elapsed());
        }));
    }

    {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let start_time = Instant::now();
            while running.get() {
                retry_or_panic!(verify(&pool).await.context("verify"));
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            info!("verify unique thread exit"; "dur" => ?start_time.saturating_elapsed());
        }));
    }

    join_all(handles).await;

    let mut retry = 0;
    while retry <= 30 {
        retry += 1;
        let check = async {
            verify(&pool).await.context("verify")?;
            let mut conn = pool.acquire().await.unwrap();
            let rows = list_rows(&mut conn).await.context("list")?;
            info!("unique rows: {:?}", rows);
            Ok(())
        };
        retry_or_panic!(check.await);
        return;
    }
    panic!("unique: final check retry limit exceeded");
}

async fn run_modify_column(pool: &MySqlPool) -> Result<()> {
    let column = if thread_rng().gen_bool(0.5) {
        "val0"
    } else {
        "val1"
    };

    // int -> bigint.
    let sql = format!(
        "alter table `{UNIQUE_DB_NAME}`.`{UNIQUE_TABLE_NAME}` \
         modify column `{column}` bigint not null"
    );
    info!("run_modify_column"; "sql" => &sql);
    {
        pool.execute(sql.as_str())
            .await
            .context("run_modify_column")?;
    }

    // bigint -> int, need reorg.
    let sql = format!(
        "alter table `{UNIQUE_DB_NAME}`.`{UNIQUE_TABLE_NAME}` \
         modify column `{column}` int not null"
    );
    info!("run_modify_column (reorg)"; "sql" => &sql);
    {
        pool.execute(sql.as_str())
            .await
            .context("run_modify_column")?;
    }

    Ok(())
}

async fn do_sql(tag: &str, pool: &MySqlPool, sql: &str, optimistic_txn: bool) -> Result<()> {
    let mut txn = Transaction::begin(tag, pool, optimistic_txn)
        .await
        .context("begin")?;
    debug!("{} unique_workload: do sql", tag; "sql" => sql, "start_ts" => txn.start_ts());
    sqlx::query(sql)
        .execute(txn.conn())
        .await
        .context(format!("query: {sql}"))?;
    txn.commit().await.context("do_sql_commit")
}

async fn verify_unique(pool: &MySqlPool) -> Result<()> {
    let mut txn = Transaction::begin_read("verify_unique", pool)
        .await
        .context("begin")?;
    let read_ts = txn.start_ts();
    let sql = format!(
        "select val0, count(*) as cnt \
         from `{UNIQUE_DB_NAME}`.`{UNIQUE_TABLE_NAME}` \
         group by val0 having cnt > 1 "
    );
    let rows = {
        sqlx::query(&sql)
            .fetch_all(txn.conn())
            .await
            .context("select duplicated")?
    };
    if !rows.is_empty() {
        for row in rows {
            let val0: i32 = row.get("val0");
            let cnt: i64 = row.get("cnt");
            error!("unique value has duplicated rows"; "val0" => val0, "cnt" => cnt, "read_ts" => read_ts);
        }

        let rows = list_rows(txn.conn()).await?;
        error!("duplicated unique rows"; "rows" => ?rows, "read_ts" => read_ts);

        panic!("unique value duplicated rows");
    }
    Ok(())
}

async fn verify_value(pool: &MySqlPool) -> Result<()> {
    let mut txn = Transaction::begin_read("verify_value", pool)
        .await
        .context("begin")?;
    let read_ts = txn.start_ts();
    let sql = format!(
        "select id, val0, val1 \
         from `{UNIQUE_DB_NAME}`.`{UNIQUE_TABLE_NAME}` \
         where val1 != val0 * 10"
    );
    let rows = {
        sqlx::query(&sql)
            .fetch_all(txn.conn())
            .await
            .context("select value")?
    };
    if !rows.is_empty() {
        for row in rows {
            let id: i32 = row.get("id");
            let val0: i32 = row.get("val0");
            let val1: i32 = row.get("val1");
            error!("unique table have incorrect values"; "id" => id, "val0" => val0, "val1" => val1, "read_ts" => read_ts);
        }

        let rows = list_rows(txn.conn()).await?;
        info!("list rows"; "rows" => ?rows, "read_ts" => read_ts);

        panic!("unique table incorrect values");
    }
    Ok(())
}

async fn check_table(pool: &MySqlPool) -> Result<()> {
    let check_table = format!("admin check table `{UNIQUE_DB_NAME}`.`{UNIQUE_TABLE_NAME}`");
    let rows = {
        pool.fetch_all(check_table.as_str())
            .await
            .context("check_table")?
    };
    if !rows.is_empty() {
        for row in rows {
            warn!("check table result: {:?}", row);
        }
        panic!("unique check table error");
    }
    Ok(())
}

async fn verify(pool: &MySqlPool) -> Result<()> {
    verify_unique(pool).await.context("verify_unique")?;
    verify_value(pool).await.context("verify_value")?;
    check_table(pool).await.context("check_table")
}

async fn list_rows<'a, E>(executor: E) -> Result<Vec<UniqueRow>>
where
    E: sqlx::Executor<'a, Database = MySql>,
{
    let sql = format!(
        "select id, val0, val1, padding from `{UNIQUE_DB_NAME}`.`{UNIQUE_TABLE_NAME}` order by id"
    );
    let rows = {
        sqlx::query(&sql)
            .fetch_all(executor)
            .await
            .context("select all")?
    };
    let unique_rows = rows
        .into_iter()
        .map(|row| {
            let id: i32 = row.get("id");
            let val0: i32 = row.get("val0");
            let val1: i32 = row.get("val1");
            let mut padding: Vec<u8> = row.get("padding");
            padding.truncate(16);
            UniqueRow {
                id,
                val0,
                val1,
                padding,
            }
        })
        .collect::<Vec<_>>();
    Ok(unique_rows)
}

#[derive(PartialEq)]
struct UniqueRow {
    id: i32,
    val0: i32,
    val1: i32,
    padding: Vec<u8>,
}

impl fmt::Debug for UniqueRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UniqueRow")
            .field("id", &self.id)
            .field("val0", &self.val0)
            .field("val1", &self.val1)
            .field("padding", &hex::encode(&self.padding))
            .finish()
    }
}
