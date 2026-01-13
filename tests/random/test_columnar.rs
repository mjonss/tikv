// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use futures::future::join_all;
use pd_client::PdClient;
use rand::prelude::*;
use sqlx::{
    Column, MySql, Pool, Row, TypeInfo,
    types::{
        BigDecimal,
        chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc},
    },
};
use test_cloud_server::{keyspace::KeyspaceManager, tidb::TidbCluster};
use tikv_util::{debug, error, info, time::Instant};
use tokio::sync::Mutex;

use crate::{
    COLUMNAR_RETRY_COUNTER, COLUMNAR_WRITE_COUNTER, Running, request_major_compact_on_store,
    sql_util::{
        DEADLOCK_ERR_MSG, get_engine_hint, is_db_error_retryable,
        wait_tiflash_or_columnar_replicas_available,
    },
    test_tidb::Switches,
};

const COLUMNAR_DB_NAME: &str = "columnar_db";
const COLUMNAR_TABLE_NAME: &str = "columnar_table";
const EMBEDDED_DOC_TABLE_NAME: &str = "embedded_documents";
const DYNAMIC_TABLE_NAME: &str = "dynamic_columns_test";
const PARTITION_TABLE_NAME: &str = "partition_test_table";
const WORKLOAD_CONCURRENCY: usize = 1;
const COLUMNAR_REPLICAS_AVAILABLE_TIMEOUT: Duration = Duration::from_secs(60);

// Generate a random vector dimension between 1 and 64.
lazy_static::lazy_static! {
    static ref VECTOR_DIMENSION: u32 = rand::random::<u32>() % 64 + 1;
}

pub(crate) async fn prepare_columnar(
    tc: TidbCluster,
    keyspace_manager: KeyspaceManager,
    keyspace_id: u32,
    vector_common_handle: bool,
) {
    let keyspace_name = keyspace_manager
        .get_keyspace_meta(keyspace_id)
        .unwrap()
        .name();
    let tag = format!("columnar-{}[{}]", keyspace_id, keyspace_name);
    let tidb_idx = TidbCluster::get_idx_by_keyspace_name(&keyspace_name);
    let params = tc.tidb.conn_params(tidb_idx);

    let conn_string = params.conn_string("test");
    let pool = sqlx::MySqlPool::connect(&conn_string).await.unwrap();

    info!("{} prepare columnar table", tag);

    let decimal_precision: u8 = rand::random::<u8>() % 50 + 10;
    let decimal_points: u8 = rand::random::<u8>() % decimal_precision.min(10);
    let partition_table = if rand::random::<u8>() % 2 == 0 {
        format!(
            "partition by KEY() partitions {}",
            rand::random::<u8>() % 4 + 1
        )
    } else {
        "".to_string()
    };
    let vector_primary_key = if vector_common_handle {
        "id VARCHAR(36) NOT NULL PRIMARY KEY DEFAULT (UUID())"
    } else {
        "id INT AUTO_INCREMENT PRIMARY KEY"
    };
    let dimension = *VECTOR_DIMENSION;

    let sqls = vec![
        format!("drop database if exists `{COLUMNAR_DB_NAME}`"),
        format!("create database `{COLUMNAR_DB_NAME}`"),
        format!(
            "create table `{COLUMNAR_DB_NAME}`.`{COLUMNAR_TABLE_NAME}` \
            (id INT AUTO_INCREMENT PRIMARY KEY, \
            int_col INT, \
            tinyint_col TINYINT, \
            smallint_col SMALLINT, \
            mediumint_col MEDIUMINT, \
            bigint_col BIGINT, \
            float_col FLOAT, \
            double_col DOUBLE, \
            decimal_col DECIMAL({decimal_precision}, {decimal_points}), \
            date_col DATE, \
            datetime_col DATETIME, \
            timestamp_col TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
            time_col TIME, \
            year_col YEAR, \
            char_col CHAR(10), \
            varchar_col VARCHAR(255), \
            text_col TEXT, \
            mediumtext_col MEDIUMTEXT, \
            longtext_col LONGTEXT, \
            binary_col BINARY(16), \
            varbinary_col VARBINARY(255), \
            blob_col BLOB, \
            mediumblob_col MEDIUMBLOB, \
            longblob_col LONGBLOB, \
            enum_col ENUM('enum-1', 'enum-2', 'enum-3'), \
            set_col SET('set-1', 'set-2', 'set-3'), \
            bool_col BOOLEAN, \
            json_col JSON, \
            default_col INT DEFAULT 100) {partition_table}"
        ),
        // We should set tiflash replica to enable tidb plan tiflash replica.
        format!("alter table `{COLUMNAR_DB_NAME}`.`{COLUMNAR_TABLE_NAME}` set tiflash replica 1"),
        format!(
            "create table `{COLUMNAR_DB_NAME}`.`{EMBEDDED_DOC_TABLE_NAME}` (
            {vector_primary_key},
            document TEXT,
            embedding VECTOR({dimension}),
            VECTOR INDEX idx_embedding((VEC_COSINE_DISTANCE(embedding)))
            )"
        ),
        format!(
            "alter table `{COLUMNAR_DB_NAME}`.`{EMBEDDED_DOC_TABLE_NAME}` set tiflash replica 1"
        ),
    ];

    for sql in sqls {
        info!("{} prepare columnar table: executing sql", tag; "sql" => &sql);
        sqlx::query(&sql).execute(&pool).await.unwrap();
    }

    wait_tiflash_or_columnar_replicas_available(
        &tag,
        &pool,
        COLUMNAR_DB_NAME,
        COLUMNAR_TABLE_NAME,
        COLUMNAR_REPLICAS_AVAILABLE_TIMEOUT,
    )
    .await;

    wait_tiflash_or_columnar_replicas_available(
        &tag,
        &pool,
        COLUMNAR_DB_NAME,
        EMBEDDED_DOC_TABLE_NAME,
        COLUMNAR_REPLICAS_AVAILABLE_TIMEOUT,
    )
    .await;
}

async fn trigger_columnar_major_compaction(pool: &Pool<MySql>) {
    let tag = "trigger_columnar_major_compaction";
    // remove columnar replica.
    let sql =
        format!("alter table `{COLUMNAR_DB_NAME}`.`{COLUMNAR_TABLE_NAME}` set tiflash replica 0");
    info!("{}: executing sql", tag; "sql" => &sql);
    sqlx::query(&sql).execute(pool).await.unwrap();
    // wait columnar replica cleared
    tokio::time::sleep(Duration::from_secs(10)).await;

    // add columnar replica.
    let sql =
        format!("alter table `{COLUMNAR_DB_NAME}`.`{COLUMNAR_TABLE_NAME}` set tiflash replica 1");
    info!("{}: executing sql", tag; "sql" => &sql);
    sqlx::query(&sql).execute(pool).await.unwrap();
    // wait columnar replica ready
    wait_tiflash_or_columnar_replicas_available(
        tag,
        pool,
        COLUMNAR_DB_NAME,
        COLUMNAR_TABLE_NAME,
        COLUMNAR_REPLICAS_AVAILABLE_TIMEOUT * 2,
    )
    .await;
}

async fn trigger_manual_columnar_major_compaction(pd_client: Arc<dyn PdClient>, keyspace_id: u32) {
    let tag = "trigger_manual_columnar_major_compaction";
    info!("{}: trigger manual columnar major compaction", tag);
    let security_mgr = pd_client.get_security_mgr();
    let stores = pd_client.get_all_stores(true).unwrap();
    for store in stores {
        request_major_compact_on_store(security_mgr.clone(), store, keyspace_id, true)
            .await
            .unwrap();
    }
}

pub(crate) async fn run_columnar_workload(
    tc: TidbCluster,
    pd_client: Arc<dyn PdClient>,
    keyspace_manager: KeyspaceManager,
    keyspace_id: u32,
    running: Running,
    switches: Switches,
) {
    let keyspace_name = keyspace_manager
        .get_keyspace_meta(keyspace_id)
        .unwrap()
        .name();
    let tidb_idx = TidbCluster::get_idx_by_keyspace_name(&keyspace_name);
    let params = tc.tidb.conn_params(tidb_idx);
    let conn_string = params.conn_string(COLUMNAR_DB_NAME);
    let pool = sqlx::MySqlPool::connect(&conn_string).await.unwrap();

    let mut handles = Vec::new();

    if switches.enable_columnar_normal_workload {
        info!("run_columnar_workload, run normal columnar workload");
        let normal_workload = NormalColumnarWorkload::new(
            pool.clone(),
            pd_client.clone(),
            running.clone(),
            keyspace_id,
            switches.vector_common_handle,
        );
        handles.push(tokio::spawn(async move {
            normal_workload.run_workload().await;
        }));
    }

    if switches.enable_columnar_dynamic_workload {
        info!("run_columnar_workload, run dynamic column management workload");
        let pool_copy = pool.clone();
        let running_copy = running.clone();
        handles.push(tokio::spawn(async move {
            let dynamic_workload = DynamicColumnWorkload::new(pool_copy, running_copy, keyspace_id);
            dynamic_workload.run_workload().await;
        }));
    }

    if switches.enable_columnar_partition_workload {
        info!("run_columnar_workload, run partition table workload");
        let pool_copy = pool.clone();
        let running_copy = running.clone();
        handles.push(tokio::spawn(async move {
            let partition_count = rand::random::<u32>() % 64 + 64;
            let partition_workload =
                PartitionTableWorkload::new(pool_copy, running_copy, keyspace_id, partition_count);
            partition_workload.run_workload().await;
        }));
    }

    join_all(handles).await;
}

fn random_str(rng: &mut ThreadRng, len: usize, is_var: bool) -> String {
    let real_len = if is_var { rng.gen_range(1..=len) } else { len };
    (1..=real_len)
        .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
        .collect()
}

fn generate_insert_sqls(max_count: usize) -> Vec<String> {
    let mut rng = rand::thread_rng();
    let count = rng.gen_range(1..=max_count);
    // Use a small range for easy match in where condition.
    let int_col: i32 = rng.gen_range(-100..300);
    let smallint_col: i16 = rng.gen();
    let tinyint_col: i8 = rng.gen();
    let mediumint_col: i16 = rng.gen();
    let bigint_col: i64 = rng.gen();
    let float_col: f32 = rng.gen();
    let double_col: f64 = rng.gen();
    let decimal_col: f32 = rng.gen();
    let naive_date: NaiveDate = NaiveDate::from_ymd_opt(
        rng.gen_range(2000..2030),
        rng.gen_range(1..=12),
        rng.gen_range(1..=28),
    )
    .unwrap_or(NaiveDate::MIN);
    let naive_time: NaiveTime = NaiveTime::from_hms_opt(
        rng.gen_range(0..24),
        rng.gen_range(0..60),
        rng.gen_range(0..60),
    )
    .unwrap_or(NaiveTime::MIN);
    let date_col = naive_date.clone().to_string();
    let datetime_col = NaiveDateTime::new(naive_date, naive_time).to_string();
    let timestamp_col = "CURRENT_TIMESTAMP";
    let time_col = naive_time.to_string();
    let year_col = rng.gen_range(2000..2100).to_string();
    let char_col = random_str(&mut rng, 10, false);
    let varchar_col = random_str(&mut rng, 2, true);
    let text_col = random_str(&mut rng, 255, true);
    let mediumtext_col = random_str(&mut rng, 512, true);
    let longtext_col = random_str(&mut rng, 1024, true);
    let binary_col = random_str(&mut rng, 16, false);
    let varbinary_col = random_str(&mut rng, 255, true);
    let blob_col = random_str(&mut rng, 255, true);
    let mediumblob_col = random_str(&mut rng, 512, true);
    let longblob_col = random_str(&mut rng, 1024, true);
    let idx = rng.gen_range(1..=3);
    let enum_col = format!("enum-{}", idx);
    let set_col = format!("set-{}", idx);
    let bool_col = rng.gen_bool(0.5);
    let json_col = format!(
        r#"{{"key": "key", "value": "{}"}}"#,
        random_str(&mut rng, 10, true)
    );

    let mut sqls = vec![format!("begin pessimistic")];
    for _ in 0..count {
        let sql = format!(
            "INSERT INTO `{COLUMNAR_DB_NAME}`.`{COLUMNAR_TABLE_NAME}` (
                int_col, tinyint_col, smallint_col, mediumint_col, bigint_col,
                float_col, double_col, decimal_col,
                date_col, datetime_col, timestamp_col, time_col, year_col,
                char_col, varchar_col, text_col, mediumtext_col, longtext_col,
                binary_col, varbinary_col, blob_col, mediumblob_col, longblob_col,
                enum_col, set_col,
                bool_col,
                json_col
            ) VALUES (
                {int_col}, {tinyint_col}, {smallint_col}, {mediumint_col}, {bigint_col},
                {float_col}, {double_col}, {decimal_col},
                '{date_col}', '{datetime_col}', {timestamp_col}, '{time_col}', '{year_col}',
                '{char_col}', '{varchar_col}', '{text_col}', '{mediumtext_col}', '{longtext_col}',
                '{binary_col}', '{varbinary_col}', '{blob_col}', '{mediumblob_col}', '{longblob_col}',
                '{enum_col}', '{set_col}',
                {bool_col},
                '{json_col}'
            );"
        );

        sqls.push(sql);
    }

    for _ in 0..count {
        let document = random_str(&mut rng, 30, true);
        let embedding: Vec<f32> = (0..*VECTOR_DIMENSION)
            .map(|_| rng.gen::<f32>() * 10.0)
            .collect();
        let embedding_str = format!(
            "[{}]",
            embedding
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
        let sql = format!(
            "INSERT INTO `{COLUMNAR_DB_NAME}`.`{EMBEDDED_DOC_TABLE_NAME}` (
                document, embedding
            ) VALUES (
                '{document}', '{embedding_str}'
            );"
        );
        sqls.push(sql);
    }
    sqls
}

fn generate_delete_sqls(count: usize, max_id: u64, vector_common_handle: bool) -> Vec<String> {
    // Generate random count ids with max value max_id.
    let mut rng = rand::thread_rng();
    let ids: Vec<u64> = (1..=max_id).choose_multiple(&mut rng, count);
    let mut sqls = vec![];
    for id in ids {
        let sql =
            format!("delete from `{COLUMNAR_DB_NAME}`.`{COLUMNAR_TABLE_NAME}` where id = {id};");
        sqls.push(sql);
        let sql = if vector_common_handle {
            let random_str = random_str(&mut rng, 3, false).to_lowercase();
            format!(
                "delete from `{COLUMNAR_DB_NAME}`.`{EMBEDDED_DOC_TABLE_NAME}` where id like '{random_str}%';"
            )
        } else {
            format!("delete from `{COLUMNAR_DB_NAME}`.`{EMBEDDED_DOC_TABLE_NAME}` where id = {id};")
        };
        sqls.push(sql);
    }
    sqls
}

async fn insert_delete_random_records(
    tag: &str,
    pool: &Pool<MySql>,
    sqls: &[String],
) -> Result<bool> {
    let mut conn = pool.acquire().await.context("acquire")?;
    for sql in sqls {
        info!("{} columnar_workload: executing sql", tag; "sql" => sql);
        match sqlx::query(sql).execute(&mut conn).await {
            Ok(_) => {}
            Err(sqlx::Error::Database(err)) if err.message().contains(DEADLOCK_ERR_MSG) => {
                info!("{} columnar_workload: ignore deadlock, retry", tag; "sql" => sql, "err" => ?err);
                return Ok(false);
            }
            Err(err) => {
                error!("{} columnar_workload: execute failed", tag; "sql" => sql, "err" => ?err);
                return Err(err).with_context(|| format!("columnar_workload: sql: {}", sql));
            }
        }
    }
    match sqlx::query("commit").execute(&mut conn).await {
        Ok(_) => {
            info!(
                "{} columnar_workload: commit success, insert rows: {}",
                tag,
                sqls.len() - 1
            );
            Ok(true)
        }
        Err(err) if is_db_error_retryable(&err) => {
            info!("{} columnar_workload: commit ignore retryable error", tag; "err" => ?err);
            Ok(false)
        }
        Err(err) => {
            error!("{} columnar_workload: commit failed", tag; "err" => ?err);
            Err(err).context("columnar_workload commit")
        }
    }
}

async fn verify_vector_data(
    pool: &Pool<MySql>,
    dump_rows: bool,
    vector_common_handle: bool,
) -> Result<()> {
    let embedding: Vec<f32> = (0..*VECTOR_DIMENSION).map(|i| i as f32).collect();
    let embedding_str = format!(
        "[{}]",
        embedding
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    let mut tx = pool.begin().await.context("begin")?;
    let query_engine = |use_tiflash: bool| {
        format!(
            "select {} id, document, vec_cosine_distance(embedding, '{embedding_str}') AS distance from `{COLUMNAR_DB_NAME}`.`{EMBEDDED_DOC_TABLE_NAME}` ORDER BY distance LIMIT 3",
            get_engine_hint(use_tiflash, EMBEDDED_DOC_TABLE_NAME)
        )
    };
    let scan_engine = |use_tiflash: bool| {
        format!(
            "select {} id, document, vec_cosine_distance(embedding, '{embedding_str}') AS distance from `{COLUMNAR_DB_NAME}`.`{EMBEDDED_DOC_TABLE_NAME}` ORDER BY id",
            get_engine_hint(use_tiflash, EMBEDDED_DOC_TABLE_NAME)
        )
    };

    info!("verify_vector_data: execute sql: {}", query_engine(false));
    let row_result = sqlx::query(&query_engine(false))
        .fetch_all(&mut tx)
        .await
        .context("select vector data from row engine")?;
    info!("verify_vector_data: execute sql: {}", query_engine(true));
    let col_result = sqlx::query(&query_engine(true))
        .fetch_all(&mut tx)
        .await
        .context("select vector data from columnar engine")?;
    if row_result.len() != col_result.len() {
        let all_row_result = sqlx::query(&scan_engine(false))
            .fetch_all(&mut tx)
            .await
            .context("select from row engine")?;
        let all_col_result = sqlx::query(&scan_engine(true))
            .fetch_all(&mut tx)
            .await
            .context("select from columnar engine")?;
        if dump_rows {
            for row in &all_row_result {
                let id = if vector_common_handle {
                    row.get::<&str, _>("id").to_string()
                } else {
                    row.get::<i64, _>("id").to_string()
                };
                info!(
                    "verify_vector_data: row1 id: {:?}, text: {:?}, distance: {:?}",
                    id,
                    row.get::<&str, _>("document"),
                    row.get::<Option<f32>, _>("distance")
                );
            }
            for row in &all_col_result {
                let id = if vector_common_handle {
                    row.get::<&str, _>("id").to_string()
                } else {
                    row.get::<i64, _>("id").to_string()
                };
                info!(
                    "verify_vector_data: row2 id: {:?}, text: {:?}, distance: {:?}",
                    id,
                    row.get::<&str, _>("document"),
                    row.get::<Option<f32>, _>("distance")
                );
            }
        }
        // TODO: items read vector index from columnar engine may be fewer than row
        // engine if the item be mvcc deleted in vector index. Just log error here.
        error!(
            "verify_vector_data: row_result.len() {} != col_result.len() {}, all_row_result: {}, all_col_result: {}",
            row_result.len(),
            col_result.len(),
            all_row_result.len(),
            all_col_result.len()
        );
        return Ok(());
    }
    for (row1, row2) in row_result.iter().zip(col_result.iter()) {
        let id1 = if vector_common_handle {
            row1.get::<&str, _>("id").to_string()
        } else {
            row1.get::<i64, _>("id").to_string()
        };
        let id2 = if vector_common_handle {
            row2.get::<&str, _>("id").to_string()
        } else {
            row2.get::<i64, _>("id").to_string()
        };
        info!(
            "verify_vector_data: row1 id: {:?}, text: {:?}, distance: {:?}, row2 id: {:?}, text: {:?}, distance: {:?}",
            id1,
            row1.get::<&str, _>("document"),
            row1.get::<Option<f32>, _>("distance"),
            id2,
            row2.get::<&str, _>("document"),
            row2.get::<Option<f32>, _>("distance")
        );
        if let Err(err) = compare_rows(row1, row2) {
            if dump_rows {
                let all_row_result = sqlx::query(&scan_engine(false))
                    .fetch_all(&mut tx)
                    .await
                    .context("select from row engine")?;
                let all_col_result = sqlx::query(&scan_engine(true))
                    .fetch_all(&mut tx)
                    .await
                    .context("select from columnar engine")?;
                for row in all_row_result {
                    let id = if vector_common_handle {
                        row.get::<&str, _>("id").to_string()
                    } else {
                        row.get::<i64, _>("id").to_string()
                    };
                    info!(
                        "verify_vector_data: row1 id: {:?}, text: {:?}, distance: {:?}",
                        id,
                        row.get::<&str, _>("document"),
                        row.get::<Option<f32>, _>("distance")
                    );
                }
                for row in all_col_result {
                    let id = if vector_common_handle {
                        row.get::<&str, _>("id").to_string()
                    } else {
                        row.get::<i64, _>("id").to_string()
                    };
                    info!(
                        "verify_vector_data: row2 id: {:?}, text: {:?}, distance: {:?}",
                        id,
                        row.get::<&str, _>("document"),
                        row.get::<Option<f32>, _>("distance")
                    );
                }
            }
            // TODO: the distance result may be different between
            // TiDB/TiKV/TiFlash, and the result precision is related to CPU
            // Architecture. We ignore to check the consistency.
            error!("verify_vector_data failed, err: {}", err);
        }
    }
    Ok(())
}

async fn verify_data(
    pool: &Pool<MySql>,
    full_scan: bool,
    vector_common_handle: bool,
) -> Result<()> {
    let mut tx = pool.begin().await.context("begin")?;
    let query_engine = |use_tiflash: bool| {
        format!(
            "select {} count(id) as count from `{COLUMNAR_DB_NAME}`.`{COLUMNAR_TABLE_NAME}`",
            get_engine_hint(use_tiflash, COLUMNAR_TABLE_NAME)
        )
    };
    // Verify the row count first.
    info!("verify_data: execute sql: {}", query_engine(false));
    let row_result = sqlx::query(&query_engine(false))
        .fetch_one(&mut tx)
        .await
        .context("select count(id) row engine")?;
    let row_count: i64 = row_result.get("count");
    info!("verify_data: execute sql: {}", query_engine(true));
    let col_result = sqlx::query(&query_engine(true))
        .fetch_one(&mut tx)
        .await
        .context("select count(id) col engine")?;
    let col_count: i64 = col_result.get("count");
    if row_count != col_count {
        // dump the data
        let scan_engine = |use_tiflash: bool| {
            format!(
                "select {} id from `{COLUMNAR_DB_NAME}`.`{COLUMNAR_TABLE_NAME}`",
                get_engine_hint(use_tiflash, COLUMNAR_TABLE_NAME)
            )
        };
        let row_result = sqlx::query(&scan_engine(false))
            .fetch_all(&mut tx)
            .await
            .context("select id row engine")?;
        let col_result = sqlx::query(&scan_engine(true))
            .fetch_all(&mut tx)
            .await
            .context("select id columnar engine")?;
        if row_result.len() != col_result.len() {
            for row in &row_result {
                let id: i64 = row.get("id");
                info!("verify_data: row_result id: {}", id);
            }
            for row in &col_result {
                let id: i64 = row.get("id");
                info!("verify_data: col_result id: {}", id);
            }
        }

        // Retry the query to check if the same with previous result.
        let row_result1 = sqlx::query(&query_engine(false))
            .fetch_one(&mut tx)
            .await
            .context("select count(id) row engine")?;
        let row_count1: i64 = row_result1.get("count");
        let col_result1 = sqlx::query(&query_engine(true))
            .fetch_one(&mut tx)
            .await
            .context("select count(id) col engine")?;
        let col_count1: i64 = col_result1.get("count");
        panic!(
            "row count {} != col count {}, row_result: {}, col_result: {}, retry row_count: {}, retry col_count: {}",
            row_count,
            col_count,
            row_result.len(),
            col_result.len(),
            row_count1,
            col_count1
        );
    }
    if row_count == 0 {
        info!("verify_data success, empty dataset");
        return Ok(());
    }
    info!("verify_data begin compare_rows for {} rows", row_count);

    // Verify all records data.
    let scan_engine = |use_tiflash: bool, offset: u64, limit: u64| {
        format!(
            "select {} id, int_col, tinyint_col, smallint_col, mediumint_col, bigint_col,
                float_col, double_col, decimal_col,
                date_col, datetime_col, timestamp_col, time_col, year_col,
                char_col, varchar_col, text_col, mediumtext_col, longtext_col,
                binary_col, varbinary_col, blob_col, mediumblob_col, longblob_col,
                enum_col, set_col,
                bool_col,
                json_col from `{COLUMNAR_DB_NAME}`.`{COLUMNAR_TABLE_NAME}` order by id limit {limit} offset {offset} ",
            get_engine_hint(use_tiflash, COLUMNAR_TABLE_NAME)
        )
    };

    let (offset, limit) = if full_scan {
        (0, row_count as u64)
    } else {
        (
            rand::random::<u64>() % row_count as u64 / 2,
            rand::random::<u64>() % row_count as u64 / 2,
        )
    };

    let row_result = sqlx::query(&scan_engine(false, offset, limit))
        .fetch_all(&mut tx)
        .await
        .context("select * row engine")?;
    let col_result = sqlx::query(&scan_engine(true, offset, limit))
        .fetch_all(&mut tx)
        .await
        .context("select * columnar engine")?;
    for (row1, row2) in row_result.iter().zip(col_result.iter()) {
        compare_rows(row1, row2).unwrap();
    }

    let scan_engine_with_primary_condition =
        |use_tiflash: bool, lower_bound: u32, upper_bound: u32| {
            format!(
            "select {} id, int_col, tinyint_col, smallint_col, mediumint_col, bigint_col,
                float_col, double_col, decimal_col,
                date_col, datetime_col, timestamp_col, time_col, year_col,
                char_col, varchar_col, text_col, mediumtext_col, longtext_col,
                binary_col, varbinary_col, blob_col, mediumblob_col, longblob_col,
                enum_col, set_col,
                bool_col,
                json_col from `{COLUMNAR_DB_NAME}`.`{COLUMNAR_TABLE_NAME}` where id >= {lower_bound} and id <= {upper_bound} order by id",
            get_engine_hint(use_tiflash, COLUMNAR_TABLE_NAME)
        )
        };

    let scan_engine_with_condition = |use_tiflash: bool, condition: &str| {
        format!(
            "select {} id, int_col, tinyint_col, smallint_col, mediumint_col, bigint_col,
                float_col, double_col, decimal_col,
                date_col, datetime_col, timestamp_col, time_col, year_col,
                char_col, varchar_col, text_col, mediumtext_col, longtext_col,
                binary_col, varbinary_col, blob_col, mediumblob_col, longblob_col,
                enum_col, set_col,
                bool_col,
                json_col from `{COLUMNAR_DB_NAME}`.`{COLUMNAR_TABLE_NAME}`
                where {condition} order by id",
            get_engine_hint(use_tiflash, COLUMNAR_TABLE_NAME)
        )
    };

    // Generate random number between 0 and row_count.
    let lower_bound = rand::random::<u32>() % row_count as u32;
    // Generate random number between row_count and u32::MAX.
    let upper_bound = rand::random::<u32>() % (u32::MAX - row_count as u32) + row_count as u32;

    // Verify the data with condition on primary id.
    let row_result = sqlx::query(&scan_engine_with_primary_condition(
        false,
        lower_bound,
        upper_bound,
    ))
    .fetch_all(&mut tx)
    .await
    .context("select * row engine with primary condition")?;
    let col_result = sqlx::query(&scan_engine_with_primary_condition(
        true,
        lower_bound,
        upper_bound,
    ))
    .fetch_all(&mut tx)
    .await
    .context("select * columnar engine with primary condition")?;
    if row_result.len() != col_result.len() {
        for row in &row_result {
            let id: i64 = row.get("id");
            info!("verify_data: row_result id: {}", id);
        }
        for row in &col_result {
            let id: i64 = row.get("id");
            info!("verify_data: col_result id: {}", id);
        }
        panic!(
            "scan_with_primary_condition row count {} != col count {}",
            row_result.len(),
            col_result.len()
        );
    }
    for (row1, row2) in row_result.iter().zip(col_result.iter()) {
        compare_rows(row1, row2).unwrap();
    }

    let condition = generate_complex_where_condition();
    // Verify the data with condition on int_col.
    let row_result = sqlx::query(&scan_engine_with_condition(false, &condition))
        .fetch_all(&mut tx)
        .await
        .context("select * row engine with primary condition")?;
    let col_result = sqlx::query(&scan_engine_with_condition(true, &condition))
        .fetch_all(&mut tx)
        .await
        .context("select * columnar engine with primary condition")?;
    if row_result.len() != col_result.len() {
        for row in &row_result {
            let id: i64 = row.get("id");
            let int_col: i64 = row.get("int_col");
            info!("verify_data: row_result id: {}, int_col: {}", id, int_col);
        }
        for row in &col_result {
            let id: i64 = row.get("id");
            let int_col: i64 = row.get("int_col");
            info!("verify_data: col_result id: {}, int_col: {}", id, int_col);
        }
        panic!(
            "scan_with_condition row count {} != col count {}",
            row_result.len(),
            col_result.len()
        );
    }
    for (row1, row2) in row_result.iter().zip(col_result.iter()) {
        compare_rows(row1, row2).unwrap();
    }

    verify_vector_data(pool, false, vector_common_handle).await?;

    Ok(())
}

fn compare_rows(row1: &sqlx::mysql::MySqlRow, row2: &sqlx::mysql::MySqlRow) -> Result<()> {
    assert_eq!(row1.len(), row2.len(), "Row length mismatch");
    debug!("verify_data compare_rows, cols: {}", row1.len());

    for (col1, col2) in row1.columns().iter().zip(row2.columns().iter()) {
        assert_eq!(col1.name(), col2.name(), "Column name mismatch");
        assert_eq!(col1.type_info(), col2.type_info(), "Column type mismatch");
        match col1.type_info().name() {
            "TINYINT" | "SMALLINT" | "INT" | "MEDIUMINT" | "BIGINT" => {
                let value1: i64 = row1.get::<i64, _>(col1.ordinal());
                let value2: i64 = row2.get::<i64, _>(col2.ordinal());
                if value1 != value2 {
                    return Err(anyhow::anyhow!("Column {} value mismatch", col1.name()));
                }
            }
            "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "INT UNSIGNED" | "MEDIUMINT UNSIGNED"
            | "BIGINT UNSIGNED" => {
                let value1: u64 = row1.get::<u64, _>(col1.ordinal());
                let value2: u64 = row2.get::<u64, _>(col2.ordinal());
                if value1 != value2 {
                    return Err(anyhow::anyhow!("Column {} value mismatch", col1.name()));
                }
            }
            "FLOAT" => {
                let value1: f32 = row1.get::<f32, _>(col1.ordinal());
                let value2: f32 = row2.get::<f32, _>(col2.ordinal());
                if value1 != value2 {
                    return Err(anyhow::anyhow!("Column {} value mismatch", col1.name()));
                }
            }
            "DOUBLE" => {
                let value1: f64 = row1.get::<f64, _>(col1.ordinal());
                let value2: f64 = row2.get::<f64, _>(col2.ordinal());
                if value1 != value2 {
                    return Err(anyhow::anyhow!("Column {} value mismatch", col1.name()));
                }
            }
            "NULL" => {
                assert_eq!(col1.type_info().is_null(), col2.type_info().is_null());
            }
            "DATETIME" => {
                let value1: NaiveDateTime = row1.get::<NaiveDateTime, _>(col1.ordinal());
                let value2: NaiveDateTime = row2.get::<NaiveDateTime, _>(col2.ordinal());
                if value1 != value2 {
                    return Err(anyhow::anyhow!("Column {} value mismatch", col1.name()));
                }
            }
            "TIMESTAMP" => {
                let value1: DateTime<Utc> = row1.get::<DateTime<Utc>, _>(col1.ordinal());
                let value2: DateTime<Utc> = row2.get::<DateTime<Utc>, _>(col2.ordinal());
                if value1 != value2 {
                    return Err(anyhow::anyhow!("Column {} value mismatch", col1.name()));
                }
            }
            "DATE" => {
                let value1: NaiveDate = row1.get::<NaiveDate, _>(col1.ordinal());
                let value2: NaiveDate = row2.get::<NaiveDate, _>(col2.ordinal());
                if value1 != value2 {
                    return Err(anyhow::anyhow!("Column {} value mismatch", col1.name()));
                }
            }
            "TIME" => {
                let value1: NaiveTime = row1.get::<NaiveTime, _>(col1.ordinal());
                let value2: NaiveTime = row2.get::<NaiveTime, _>(col2.ordinal());
                if value1 != value2 {
                    return Err(anyhow::anyhow!("Column {} value mismatch", col1.name()));
                }
            }
            "YEAR" => {
                let value1: u16 = row1.get::<u16, _>(col1.ordinal());
                let value2: u16 = row2.get::<u16, _>(col2.ordinal());
                if value1 != value2 {
                    return Err(anyhow::anyhow!("Column {} value mismatch", col1.name()));
                }
            }
            "ENUM" | "JSON" => {}
            "SET" | "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" => {
                let value1: &str = row1.get::<&str, _>(col1.ordinal());
                let value2: &str = row2.get::<&str, _>(col2.ordinal());
                if value1 != value2 {
                    return Err(anyhow::anyhow!("Column {} value mismatch", col1.name()));
                }
            }
            "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
                let value1: Vec<u8> = row1.get::<Vec<u8>, _>(col1.ordinal());
                let value2: Vec<u8> = row2.get::<Vec<u8>, _>(col2.ordinal());
                if value1 != value2 {
                    return Err(anyhow::anyhow!("Column {} value mismatch", col1.name()));
                }
            }
            "DECIMAL" => {
                let value1: BigDecimal = row1.get::<BigDecimal, _>(col1.ordinal());
                let value2: BigDecimal = row2.get::<BigDecimal, _>(col2.ordinal());
                if value1 != value2 {
                    return Err(anyhow::anyhow!("Column {} value mismatch", col1.name()));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Represents different comparison operators
#[derive(Copy, Clone)]
enum ComparisonOp {
    Eq,
    Ne,
    Gt,
    Lt,
    Gte,
    Lte,
    Like,
    Between,
    In,
}

impl ComparisonOp {
    fn as_str(&self) -> &'static str {
        match self {
            ComparisonOp::Eq => "=",
            ComparisonOp::Ne => "!=",
            ComparisonOp::Gt => ">",
            ComparisonOp::Lt => "<",
            ComparisonOp::Gte => ">=",
            ComparisonOp::Lte => "<=",
            ComparisonOp::Like => "LIKE",
            ComparisonOp::Between => "BETWEEN",
            ComparisonOp::In => "IN",
        }
    }

    fn random() -> Self {
        let ops = [
            ComparisonOp::Eq,
            ComparisonOp::Ne,
            ComparisonOp::Gt,
            ComparisonOp::Lt,
            ComparisonOp::Gte,
            ComparisonOp::Lte,
            ComparisonOp::Like,
            ComparisonOp::Between,
            ComparisonOp::In,
        ];
        ops[rand::thread_rng().gen_range(0..ops.len())]
    }
}

/// Generates a random condition for a specific column
fn generate_column_condition(column: &str, column_type: &str) -> String {
    let mut rng = rand::thread_rng();
    let op = ComparisonOp::random();

    match column_type {
        "int" | "bigint" => match op {
            ComparisonOp::Between => {
                let v1 = rng.gen_range(-100..100);
                let v2 = rng.gen_range(v1..v1 + 200);
                format!("{} BETWEEN {} AND {}", column, v1, v2)
            }
            ComparisonOp::In => {
                let values: Vec<i32> = (0..3).map(|_| rng.gen_range(-100..100)).collect();
                format!(
                    "{} IN ({})",
                    column,
                    values
                        .iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            _ => format!("{} {} {}", column, op.as_str(), rng.gen_range(-1000..1000)),
        },
        "varchar" | "text" => match op {
            ComparisonOp::Like => format!("{} LIKE '%{}%'", column, random_str(&mut rng, 2, true)),
            ComparisonOp::In => {
                let values: Vec<String> = (0..3)
                    .map(|_| format!("'{}'", random_str(&mut rng, 2, true)))
                    .collect();
                format!("{} IN ({})", column, values.join(","))
            }
            ComparisonOp::Between => {
                let v1 = random_str(&mut rng, 2, true);
                let v2 = random_str(&mut rng, 2, true);
                format!("{} BETWEEN '{}' AND '{}'", column, v1, v2)
            }
            _ => format!(
                "{} {} '{}'",
                column,
                op.as_str(),
                random_str(&mut rng, 2, true)
            ),
        },
        "datetime" => match op {
            ComparisonOp::In => {
                let date = NaiveDateTime::from_timestamp_opt(
                    rng.gen_range(946684800..1893456000), // 2000-01-01 to 2030-01-01
                    0,
                )
                .unwrap();
                format!("{} IN ('{}')", column, date.format("%Y-%m-%d %H:%M:%S"),)
            }
            ComparisonOp::Between => {
                let v1 = NaiveDateTime::from_timestamp_opt(
                    rng.gen_range(946684800..1893456000), // 2000-01-01 to 2030-01-01
                    0,
                )
                .unwrap();
                let v2 = NaiveDateTime::from_timestamp_opt(
                    rng.gen_range(946684800..1893456000), // 2000-01-01 to 2030-01-01
                    0,
                )
                .unwrap();
                format!("{} BETWEEN '{}' AND '{}'", column, v1, v2)
            }
            _ => {
                let date = NaiveDateTime::from_timestamp_opt(
                    rng.gen_range(946684800..1893456000), // 2000-01-01 to 2030-01-01
                    0,
                )
                .unwrap();
                format!(
                    "{} {} '{}'",
                    column,
                    op.as_str(),
                    date.format("%Y-%m-%d %H:%M:%S")
                )
            }
        },
        _ => "".to_string(),
    }
}

fn generate_complex_where_condition() -> String {
    let mut rng = rand::thread_rng();

    // Define available columns and their types
    let columns = [
        ("int_col", "int"),
        ("varchar_col", "varchar"),
        ("datetime_col", "datetime"),
        ("text_col", "text"),
        ("bigint_col", "bigint"),
    ];

    // Generate 1-3 conditions
    let condition_count = rng.gen_range(1..=3);
    let mut conditions = Vec::with_capacity(condition_count);

    for _ in 0..condition_count {
        let (col, col_type) = columns.choose(&mut rng).unwrap();
        conditions.push(generate_column_condition(col, col_type));
    }

    // Randomly combine conditions with AND/OR/NOT
    let mut combined = String::new();
    for (i, condition) in conditions.iter().enumerate() {
        if i > 0 {
            combined.push_str(if rng.gen_bool(0.7) { " AND " } else { " OR " });
        }
        // Randomly group conditions with parentheses
        if rng.gen_bool(0.3) && i < conditions.len() - 1 {
            let not_prefix = if rng.gen_bool(0.2) { "NOT " } else { "" };
            combined.push_str(not_prefix);
            combined.push('(');
            combined.push_str(condition);
            combined.push_str(if rng.gen_bool(0.7) { " AND " } else { " OR " });
            combined.push_str(&conditions[i + 1]);
            combined.push(')');
        } else {
            combined.push_str(condition);
        }
    }

    combined
}

/// Normal columnar workload for testing basic columnar storage operations
/// including insert, delete, query, and consistency verification
struct NormalColumnarWorkload {
    pool: Pool<MySql>,
    pd_client: Arc<dyn PdClient>,
    running: Running,
    keyspace_id: u32,
    vector_common_handle: bool,
    max_id: Arc<AtomicU64>,
    running_mutex: Arc<Mutex<()>>,
}

impl NormalColumnarWorkload {
    fn new(
        pool: Pool<MySql>,
        pd_client: Arc<dyn PdClient>,
        running: Running,
        keyspace_id: u32,
        vector_common_handle: bool,
    ) -> Self {
        Self {
            pool,
            pd_client,
            running,
            keyspace_id,
            vector_common_handle,
            max_id: Arc::new(AtomicU64::new(0)),
            running_mutex: Arc::new(Mutex::new(())),
        }
    }

    /// Execute insert/delete workload for a single thread
    async fn run_insert_delete_thread(&self, tid: usize) {
        let tag = format!("columnar-{}-{}", self.keyspace_id, tid);
        let start_time = Instant::now();

        while self.running.get() {
            let mut sqls = generate_insert_sqls(10);
            let del_sqls =
                generate_delete_sqls(10, self.max_id.load(Relaxed), self.vector_common_handle);
            sqls.extend(del_sqls);
            self.max_id.fetch_add(10, Relaxed);

            let ok = insert_delete_random_records(&tag, &self.pool, &sqls)
                .await
                .unwrap_or_else(|err| {
                    panic!("{} columnar insert/delete error: {:?}", tag, err);
                });

            if ok {
                COLUMNAR_WRITE_COUNTER.fetch_add(1, Relaxed);
            } else {
                COLUMNAR_RETRY_COUNTER.fetch_add(1, Relaxed);
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!("columnar workload exit"; "tag" => tag, "dur" => ?start_time.saturating_elapsed());
    }

    /// Periodic verification workload
    async fn run_verification_workload(&self) {
        let tag = format!("columnar-verify-{}", self.keyspace_id);
        let start_time = Instant::now();

        while self.running.get() {
            let guard = self.running_mutex.lock().await;
            info!("{} verify_data randomly", tag);
            match verify_data(&self.pool, false, self.vector_common_handle).await {
                Ok(_) => {
                    info!("{} verify_data randomly success", tag);
                }
                Err(err) => {
                    panic!("{} verify_data randomly error: {}", tag, err);
                }
            }
            drop(guard);
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        info!("{} columnar verify thread exit", tag; "dur" => ?start_time.saturating_elapsed());
    }

    /// Trigger columnar major compaction once after initial delay
    async fn run_major_compaction_once(&self) {
        tokio::time::sleep(Duration::from_secs(60)).await;
        info!("pause columnar workload trigger columnar major compaction");
        let _guard = self.running_mutex.lock().await;
        trigger_columnar_major_compaction(&self.pool).await;
    }

    /// Periodically trigger manual major compaction
    async fn run_periodic_manual_compaction(&self) {
        let tag = format!("columnar-manual-compact-{}", self.keyspace_id);

        while self.running.get() {
            tokio::time::sleep(Duration::from_secs(30)).await;
            trigger_manual_columnar_major_compaction(self.pd_client.clone(), self.keyspace_id)
                .await;
        }

        info!("{} manual compaction thread exit", tag);
    }

    /// Main workload entry point
    async fn run_workload(&self) {
        let tag = format!("normal-columnar-{}", self.keyspace_id);
        info!("{} starting normal columnar workload", tag);

        let mut handles = Vec::with_capacity(WORKLOAD_CONCURRENCY + 3);

        // Start multiple insert/delete threads
        for tid in 0..WORKLOAD_CONCURRENCY {
            let self_clone = self.clone_for_workload();
            handles.push(tokio::spawn(async move {
                self_clone.run_insert_delete_thread(tid).await;
            }));
        }

        // Start verification thread
        let self_clone = self.clone_for_workload();
        handles.push(tokio::spawn(async move {
            self_clone.run_verification_workload().await;
        }));

        // Start one-time major compaction thread
        let self_clone = self.clone_for_workload();
        handles.push(tokio::spawn(async move {
            self_clone.run_major_compaction_once().await;
        }));

        // Start periodic manual compaction thread
        let self_clone = self.clone_for_workload();
        handles.push(tokio::spawn(async move {
            self_clone.run_periodic_manual_compaction().await;
        }));

        // Wait for all threads to complete
        join_all(handles).await;

        info!("{} normal columnar workload completed", tag);
    }

    /// Clone necessary fields for workload tasks
    fn clone_for_workload(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            pd_client: self.pd_client.clone(),
            running: self.running.clone(),
            keyspace_id: self.keyspace_id,
            vector_common_handle: self.vector_common_handle,
            max_id: self.max_id.clone(),
            running_mutex: self.running_mutex.clone(),
        }
    }
}

/// Independent dynamic column management workload with simplified schema
struct DynamicColumnWorkload {
    pool: Pool<MySql>,
    running: Running,
    keyspace_id: u32,
    current_dynamic_columns: Arc<Mutex<Vec<String>>>,
    max_id: Arc<AtomicU64>,
}

impl DynamicColumnWorkload {
    fn new(pool: Pool<MySql>, running: Running, keyspace_id: u32) -> Self {
        Self {
            pool,
            running,
            keyspace_id,
            current_dynamic_columns: Arc::new(Mutex::new(Vec::new())),
            max_id: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Initialize the dynamic test table with basic schema
    async fn prepare_dynamic_table(&self) -> Result<()> {
        let tag = format!("dynamic-table-{}", self.keyspace_id);

        // Create table with simplified schema: only INT and VARCHAR columns
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS `{COLUMNAR_DB_NAME}`.`{DYNAMIC_TABLE_NAME}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                base_int_col INT DEFAULT 0,
                base_varchar_col VARCHAR(100) DEFAULT 'default'
            )"
        );

        info!("{} creating dynamic test table: {}", tag, sql);
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .with_context(|| "Failed to create dynamic test table")?;

        // Set tiflash replica for the new table
        let replica_sql = format!(
            "ALTER TABLE `{COLUMNAR_DB_NAME}`.`{DYNAMIC_TABLE_NAME}` SET tiflash replica 1"
        );
        info!("{} setting tiflash replica: {}", tag, replica_sql);
        sqlx::query(&replica_sql)
            .execute(&self.pool)
            .await
            .with_context(|| "Failed to set tiflash replica")?;

        // Wait for replica to be available
        wait_tiflash_or_columnar_replicas_available(
            &tag,
            &self.pool,
            COLUMNAR_DB_NAME,
            DYNAMIC_TABLE_NAME,
            COLUMNAR_REPLICAS_AVAILABLE_TIMEOUT,
        )
        .await;

        Ok(())
    }

    /// Generate a random column definition (only INT or VARCHAR)
    fn generate_column_def(&self, column_name: &str) -> String {
        let mut rng = rand::thread_rng();
        // column_name has prefix "col_int_" or "col_varchar_", choose column type by
        // prefix.
        let column_type = if column_name.starts_with("col_int_") {
            "INT"
        } else {
            "VARCHAR(50)"
        };

        let has_default = rng.gen_bool(0.5); // 50% chance to have default value

        if has_default {
            let default_value = match column_type {
                "INT" => rng.gen_range(-100..100).to_string(),
                "VARCHAR(50)" => format!("'{}'", random_str(&mut rng, 10, true)),
                _ => unreachable!(),
            };
            format!(
                "{} {} DEFAULT {} NOT NULL",
                column_name, column_type, default_value
            )
        } else {
            format!("{} {}", column_name, column_type)
        }
    }

    /// Generate insert SQL for dynamic table
    fn generate_insert_sqls(&self, count: usize, dynamic_columns: &[String]) -> Vec<String> {
        let mut rng = rand::thread_rng();
        let mut sqls = vec!["BEGIN".to_string()];

        for _ in 0..count {
            let base_int_value = rng.gen_range(-1000..1000);
            let base_varchar_value = random_str(&mut rng, 20, true);

            let mut column_names = vec!["base_int_col", "base_varchar_col"];
            let mut column_values = vec![
                base_int_value.to_string(),
                format!("'{}'", base_varchar_value),
            ];

            // Add values for dynamic columns
            for col_name in dynamic_columns {
                column_names.push(col_name);
                if col_name.contains("int") || col_name.starts_with("col_int_") {
                    column_values.push(rng.gen_range(-100..100).to_string());
                } else {
                    column_values.push(format!("'{}'", random_str(&mut rng, 10, true)));
                }
            }

            let sql = format!(
                "INSERT INTO `{COLUMNAR_DB_NAME}`.`{DYNAMIC_TABLE_NAME}` ({}) VALUES ({})",
                column_names.join(", "),
                column_values.join(", ")
            );
            sqls.push(sql);
        }

        sqls.push("COMMIT".to_string());
        sqls
    }

    /// Generate delete SQL for dynamic table
    fn generate_delete_sqls(&self, count: usize) -> Vec<String> {
        let mut rng = rand::thread_rng();
        let max_id = self.max_id.load(Relaxed);
        let mut sqls = vec![];

        if max_id > 0 {
            for _ in 0..count {
                let id = rng.gen_range(1..=max_id);
                let sql = format!(
                    "DELETE FROM `{COLUMNAR_DB_NAME}`.`{DYNAMIC_TABLE_NAME}` WHERE id = {}",
                    id
                );
                sqls.push(sql);
            }
        }

        sqls
    }

    /// Add a new dynamic column
    async fn add_column(&self, column_name: &str) -> Result<()> {
        let tag = format!("dynamic-column-{}", self.keyspace_id);
        let column_def = self.generate_column_def(column_name);
        let sql = format!(
            "ALTER TABLE `{COLUMNAR_DB_NAME}`.`{DYNAMIC_TABLE_NAME}` ADD COLUMN {}",
            column_def
        );

        info!("{} adding column: {}", tag, sql);
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .with_context(|| format!("Failed to add column {}", column_name))?;

        // Add to tracking list
        let mut columns = self.current_dynamic_columns.lock().await;
        columns.push(column_name.to_string());

        Ok(())
    }

    /// Drop a dynamic column
    async fn drop_column(&self, column_name: &str) -> Result<()> {
        let tag = format!("dynamic-column-{}", self.keyspace_id);
        let sql = format!(
            "ALTER TABLE `{COLUMNAR_DB_NAME}`.`{DYNAMIC_TABLE_NAME}` DROP COLUMN {}",
            column_name
        );

        info!("{} dropping column: {}", tag, sql);
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .with_context(|| format!("Failed to drop column {}", column_name))?;

        // Remove from tracking list
        let mut columns = self.current_dynamic_columns.lock().await;
        columns.retain(|col| col != column_name);

        Ok(())
    }

    /// Check if a column is NOT NULL
    async fn is_column_not_null(&self, column_name: &str) -> Result<bool> {
        let sql = format!(
            "SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS 
             WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}' AND COLUMN_NAME = '{}'",
            COLUMNAR_DB_NAME, DYNAMIC_TABLE_NAME, column_name
        );

        let row = sqlx::query(&sql).fetch_one(&self.pool).await?;
        let is_nullable: String = row.get("IS_NULLABLE");
        Ok(is_nullable == "NO")
    }

    /// Modify a column from NOT NULL to NULLABLE
    async fn modify_column_to_nullable(&self, column_name: &str) -> Result<()> {
        let tag = format!("dynamic-column-{}", self.keyspace_id);

        // Determine column type based on name prefix
        let column_type = if column_name.starts_with("col_int_") || column_name == "base_int_col" {
            "INT"
        } else {
            "VARCHAR(50)"
        };

        let sql = format!(
            "ALTER TABLE `{COLUMNAR_DB_NAME}`.`{DYNAMIC_TABLE_NAME}` MODIFY COLUMN {} {} NULL",
            column_name, column_type
        );

        info!("{} modifying column to nullable: {}", tag, sql);
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .with_context(|| format!("Failed to modify column {} to nullable", column_name))?;

        Ok(())
    }

    /// Execute insert/delete workload
    async fn execute_insert_delete_workload(&self) -> Result<()> {
        let tag = format!("dynamic-insert-delete-{}", self.keyspace_id);
        let current_columns = {
            let cols = self.current_dynamic_columns.lock().await;
            cols.clone()
        };

        // Generate insert and delete SQLs
        let insert_sqls = self.generate_insert_sqls(5, &current_columns);
        let delete_sqls = self.generate_delete_sqls(2);

        // Execute insert SQLs
        for sql in &insert_sqls {
            info!("{} executing insert: {}", tag, sql);
            match sqlx::query(sql).execute(&self.pool).await {
                Ok(_) => {
                    if sql.starts_with("INSERT") {
                        self.max_id.fetch_add(1, Relaxed);
                    }
                }
                Err(err) => {
                    error!("{} insert failed: {}", tag, err);
                }
            }
        }

        // Execute delete SQLs
        for sql in &delete_sqls {
            info!("{} executing delete: {}", tag, sql);
            match sqlx::query(sql).execute(&self.pool).await {
                Ok(_) => {}
                Err(err) => {
                    error!("{} delete failed: {}", tag, err);
                }
            }
        }

        Ok(())
    }

    /// Perform queries using dynamic columns
    async fn query_with_dynamic_columns(&self) -> Result<()> {
        let columns = self.current_dynamic_columns.lock().await;
        let tag = format!("dynamic-column-query-{}", self.keyspace_id);

        // Always query base columns
        let mut query_columns = vec!["base_int_col", "base_varchar_col"];
        query_columns.extend(columns.iter().map(|s| s.as_str()));

        if query_columns.is_empty() {
            return Ok(());
        }

        // Select a random column to query
        let column_idx = rand::random::<usize>() % query_columns.len();
        let column_name = query_columns[column_idx];

        // Generate different types of queries
        let query_types = if column_name.contains("int") || column_name == "base_int_col" {
            // INT column queries
            let value = rand::random::<i32>() % 100 - 50;
            vec![
                format!(
                    "SELECT COUNT(*) FROM `{COLUMNAR_DB_NAME}`.`{DYNAMIC_TABLE_NAME}` WHERE {} > {}",
                    column_name, value
                ),
                format!(
                    "SELECT COUNT(*) FROM `{COLUMNAR_DB_NAME}`.`{DYNAMIC_TABLE_NAME}` WHERE {} < {}",
                    column_name, value
                ),
                format!(
                    "SELECT COUNT(*) FROM `{COLUMNAR_DB_NAME}`.`{DYNAMIC_TABLE_NAME}` WHERE {} = {}",
                    column_name, value
                ),
                format!(
                    "SELECT {} COUNT(*) FROM `{COLUMNAR_DB_NAME}`.`{DYNAMIC_TABLE_NAME}` WHERE {} BETWEEN {} AND {}",
                    get_engine_hint(true, DYNAMIC_TABLE_NAME),
                    column_name,
                    value - 10,
                    value + 10
                ),
            ]
        } else {
            // VARCHAR column queries
            let value = random_str(&mut rand::thread_rng(), 5, true);
            vec![
                format!(
                    "SELECT COUNT(*) FROM `{COLUMNAR_DB_NAME}`.`{DYNAMIC_TABLE_NAME}` WHERE {} LIKE '%{}%'",
                    column_name, value
                ),
                format!(
                    "SELECT COUNT(*) FROM `{COLUMNAR_DB_NAME}`.`{DYNAMIC_TABLE_NAME}` WHERE {} IS NOT NULL",
                    column_name
                ),
                format!(
                    "SELECT {} COUNT(*) FROM `{COLUMNAR_DB_NAME}`.`{DYNAMIC_TABLE_NAME}` WHERE {} != ''",
                    get_engine_hint(true, DYNAMIC_TABLE_NAME),
                    column_name
                ),
            ]
        };

        for sql in &query_types {
            info!("{} executing query: {}", tag, sql);
            match sqlx::query(sql).fetch_all(&self.pool).await {
                Ok(rows) => {
                    info!("{} query returned {} rows", tag, rows.len());
                }
                Err(err) => {
                    error!("{} query failed: {}", tag, err);
                }
            }
        }

        Ok(())
    }

    /// Main workload entry point
    async fn run_workload(&self) {
        let tag = format!("dynamic-workload-{}", self.keyspace_id);
        info!("{} starting independent dynamic column workload", tag);

        // Step 1: Prepare the dynamic test table
        if let Err(err) = self.prepare_dynamic_table().await {
            error!("{} failed to prepare dynamic table: {}", tag, err);
            return;
        }

        // Step 2: Start concurrent workloads
        let mut handles = Vec::new();

        // Insert/Delete workload
        let self_clone = self.clone_for_workload();
        handles.push(tokio::spawn(async move {
            self_clone.run_insert_delete_workload().await;
        }));

        // Column management workload
        let self_clone = self.clone_for_workload();
        handles.push(tokio::spawn(async move {
            self_clone.run_column_management_workload().await;
        }));

        // Query workload
        let self_clone = self.clone_for_workload();
        handles.push(tokio::spawn(async move {
            self_clone.run_query_workload().await;
        }));

        // Wait for all workloads to complete
        futures::future::join_all(handles).await;

        info!("{} dynamic workload completed", tag);
    }

    /// Clone necessary fields for workload tasks
    fn clone_for_workload(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            running: self.running.clone(),
            keyspace_id: self.keyspace_id,
            current_dynamic_columns: self.current_dynamic_columns.clone(),
            max_id: self.max_id.clone(),
        }
    }

    /// Insert/Delete workload loop
    async fn run_insert_delete_workload(&self) {
        let tag = format!("dynamic-insert-delete-{}", self.keyspace_id);
        let start_time = Instant::now();

        while self.running.get() {
            if let Err(err) = self.execute_insert_delete_workload().await {
                error!("{} insert/delete workload error: {}", tag, err);
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        info!("{} insert/delete workload exit", tag; "dur" => ?start_time.saturating_elapsed());
    }

    /// Column management workload loop - randomly add, drop, or modify columns
    async fn run_column_management_workload(&self) {
        let tag = format!("dynamic-column-mgmt-{}", self.keyspace_id);
        let start_time = Instant::now();
        let mut column_counter = 0u32;

        while self.running.get() {
            let current_columns = {
                let cols = self.current_dynamic_columns.lock().await;
                cols.clone()
            };

            // Generate random number to decide operation type
            let operation_chance = rand::random::<usize>() % 100;

            if operation_chance < 10 && !current_columns.is_empty() {
                // 10% chance: Check and modify the last column to nullable
                let last_column = &current_columns[current_columns.len() - 1];

                info!(
                    "{} cycle {}: checking nullable status of last column {} (current columns: {})",
                    tag,
                    column_counter,
                    last_column,
                    current_columns.len()
                );

                match self.is_column_not_null(last_column).await {
                    Ok(true) => {
                        // Column is NOT NULL, modify it to nullable
                        info!(
                            "{} last column {} is NOT NULL, modifying to nullable",
                            tag, last_column
                        );
                        match self.modify_column_to_nullable(last_column).await {
                            Ok(_) => {
                                info!(
                                    "{} successfully modified column {} to nullable",
                                    tag, last_column
                                );
                            }
                            Err(err) => {
                                error!(
                                    "{} failed to modify column {} to nullable: {}",
                                    tag, last_column, err
                                );
                            }
                        }
                    }
                    Ok(false) => {
                        info!(
                            "{} last column {} is already nullable, no action needed",
                            tag, last_column
                        );
                    }
                    Err(err) => {
                        error!(
                            "{} failed to check nullable status of column {}: {}",
                            tag, last_column, err
                        );
                    }
                }
            } else {
                // 90% chance: Normal add/drop operations
                let should_add = if current_columns.is_empty() {
                    // If no columns exist, we must add one
                    true
                } else if current_columns.len() >= 10 {
                    // If too many columns exist, we must drop one
                    false
                } else {
                    // Otherwise, randomly choose (adjust probabilities for remaining 90%)
                    let add_chance = rand::random::<usize>() % 10;
                    // 60% chance to add, 30% chance to drop within the 90%
                    add_chance < 6
                };

                if should_add {
                    // Add a new column
                    column_counter += 1;
                    let column_name = if column_counter % 2 == 1 {
                        format!("col_int_{}", column_counter)
                    } else {
                        format!("col_varchar_{}", column_counter)
                    };

                    info!(
                        "{} cycle {}: adding column {} (current columns: {})",
                        tag,
                        column_counter,
                        column_name,
                        current_columns.len()
                    );

                    match self.add_column(&column_name).await {
                        Ok(_) => {
                            info!("{} successfully added column {}", tag, column_name);
                        }
                        Err(err) => {
                            error!("{} failed to add column {}: {}", tag, column_name, err);
                        }
                    }
                } else {
                    // Drop an existing column
                    let column_idx = rand::random::<usize>() % current_columns.len();
                    let column_to_drop = &current_columns[column_idx];

                    info!(
                        "{} cycle {}: dropping column {} (current columns: {})",
                        tag,
                        column_counter,
                        column_to_drop,
                        current_columns.len()
                    );

                    match self.drop_column(column_to_drop).await {
                        Ok(_) => {
                            info!("{} successfully dropped column {}", tag, column_to_drop);
                        }
                        Err(err) => {
                            error!("{} failed to drop column {}: {}", tag, column_to_drop, err);
                        }
                    }
                }
            }

            // Wait before next iteration
            tokio::time::sleep(Duration::from_secs(10)).await;
        }

        info!("{} column management workload exit after {} operations", tag, column_counter; "dur" => ?start_time.saturating_elapsed());
    }

    /// Query workload loop
    async fn run_query_workload(&self) {
        let tag = format!("dynamic-query-{}", self.keyspace_id);
        let start_time = Instant::now();

        while self.running.get() {
            if let Err(err) = self.query_with_dynamic_columns().await {
                error!("{} query workload error: {}", tag, err);
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        info!("{} query workload exit", tag; "dur" => ?start_time.saturating_elapsed());
    }
}

/// Workload for testing massive partition tables
struct PartitionTableWorkload {
    pool: Pool<MySql>,
    running: Running,
    keyspace_id: u32,
    max_id: Arc<AtomicU64>,
    partition_count: u32,
}

impl PartitionTableWorkload {
    fn new(pool: Pool<MySql>, running: Running, keyspace_id: u32, partition_count: u32) -> Self {
        Self {
            pool,
            running,
            keyspace_id,
            max_id: Arc::new(AtomicU64::new(0)),
            partition_count,
        }
    }

    /// Initialize the partition test table with hash partitioning
    async fn prepare_partition_table(&self) -> Result<()> {
        let tag = format!("partition-table-{}", self.keyspace_id);

        // Create table with simplified schema and HASH partitioning on id
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS `{COLUMNAR_DB_NAME}`.`{PARTITION_TABLE_NAME}` (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                data VARCHAR(100),
                created_at DATETIME
            ) PARTITION BY HASH(id) PARTITIONS {}",
            self.partition_count
        );

        info!(
            "{} creating partition test table with {} partitions: {}",
            tag, self.partition_count, sql
        );
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .with_context(|| "Failed to create partition test table")?;

        // Set tiflash replica for the partitioned table
        let replica_sql = format!(
            "ALTER TABLE `{COLUMNAR_DB_NAME}`.`{PARTITION_TABLE_NAME}` SET tiflash replica 1"
        );
        info!("{} setting tiflash replica: {}", tag, replica_sql);
        sqlx::query(&replica_sql)
            .execute(&self.pool)
            .await
            .with_context(|| "Failed to set tiflash replica")?;

        // Wait for replica to be available
        wait_tiflash_or_columnar_replicas_available(
            &tag,
            &self.pool,
            COLUMNAR_DB_NAME,
            PARTITION_TABLE_NAME,
            COLUMNAR_REPLICAS_AVAILABLE_TIMEOUT * 5,
        )
        .await;

        // Analyze the table to improve query multiple partitions.
        let analyze_sql = format!("ANALYZE TABLE `{COLUMNAR_DB_NAME}`.`{PARTITION_TABLE_NAME}`");
        info!("{} analyzing table: {}", tag, analyze_sql);
        sqlx::query(&analyze_sql)
            .execute(&self.pool)
            .await
            .with_context(|| "Failed to analyze table")?;

        info!("{} partition table prepared, columnar table is ready", tag);

        Ok(())
    }

    /// Generate insert SQLs for partition table, data will be distributed
    /// across partitions by HASH(id)
    fn generate_insert_sqls(&self, count: usize) -> Vec<String> {
        let mut rng = rand::thread_rng();
        let mut sqls = vec!["BEGIN".to_string()];

        for _ in 0..count {
            let data = random_str(&mut rng, 50, true);
            let created_at = NaiveDateTime::from_timestamp_opt(
                rng.gen_range(946684800..1893456000), // 2000-01-01 to 2030-01-01
                0,
            )
            .unwrap_or(NaiveDateTime::MIN);

            let sql = format!(
                "INSERT INTO `{COLUMNAR_DB_NAME}`.`{PARTITION_TABLE_NAME}` 
                (data, created_at) 
                VALUES ('{}', '{}')",
                data,
                created_at.format("%Y-%m-%d %H:%M:%S")
            );
            sqls.push(sql);
        }

        sqls.push("COMMIT".to_string());
        sqls
    }

    /// Generate delete SQLs, deletion by id will test partition pruning
    fn generate_delete_sqls(&self, count: usize) -> Vec<String> {
        let mut rng = rand::thread_rng();
        let max_id = self.max_id.load(Relaxed);
        let mut sqls = vec![];

        if max_id > 0 {
            for _ in 0..count {
                let operation_type = rng.gen_range(0..3);

                let sql = match operation_type {
                    0 => {
                        // Delete by id range
                        let id = rng.gen_range(1..=max_id);
                        format!(
                            "DELETE FROM `{COLUMNAR_DB_NAME}`.`{PARTITION_TABLE_NAME}` 
                            WHERE id BETWEEN {} AND {} LIMIT 5",
                            id,
                            id + 10
                        )
                    }
                    1 => {
                        // Delete by specific id (tests partition pruning)
                        let id = rng.gen_range(1..=max_id);
                        format!(
                            "DELETE FROM `{COLUMNAR_DB_NAME}`.`{PARTITION_TABLE_NAME}` 
                            WHERE id = {}",
                            id
                        )
                    }
                    _ => {
                        // Delete by data condition
                        format!(
                            "DELETE FROM `{COLUMNAR_DB_NAME}`.`{PARTITION_TABLE_NAME}` 
                            WHERE data LIKE '%{}%' LIMIT 3",
                            random_str(&mut rng, 2, true)
                        )
                    }
                };
                sqls.push(sql);
            }
        }

        sqls
    }

    /// Execute insert/delete workload
    async fn execute_insert_delete_workload(&self) -> Result<()> {
        let tag = format!("partition-insert-delete-{}", self.keyspace_id);

        // Generate insert and delete SQLs
        let insert_sqls = self.generate_insert_sqls(10);
        let delete_sqls = self.generate_delete_sqls(3);

        let mut all_sqls = insert_sqls;
        all_sqls.extend(delete_sqls);

        // Execute all SQLs
        for sql in &all_sqls {
            info!("{} executing: {}", tag, sql);
            match sqlx::query(sql).execute(&self.pool).await {
                Ok(_) => {
                    if sql.starts_with("INSERT") {
                        self.max_id.fetch_add(1, Relaxed);
                    }
                }
                Err(sqlx::Error::Database(err)) if err.message().contains(DEADLOCK_ERR_MSG) => {
                    info!("{} ignore deadlock, retry next time", tag; "err" => ?err);
                }
                Err(err) if is_db_error_retryable(&err) => {
                    info!("{} ignore retryable error", tag; "err" => ?err);
                }
                Err(err) => {
                    error!("{} execution failed: {}", tag, err);
                }
            }
        }

        Ok(())
    }

    /// Verify consistency between TiFlash and TiKV for partition table
    async fn verify_consistency(&self) -> Result<()> {
        let tag = format!("partition-verify-{}", self.keyspace_id);
        info!("{} starting consistency verification", tag);

        let mut tx = self.pool.begin().await.context("begin transaction")?;

        // Step 1: Verify row count consistency
        let count_query = |use_tiflash: bool| {
            format!(
                "SELECT {} COUNT(*) as count FROM `{COLUMNAR_DB_NAME}`.`{PARTITION_TABLE_NAME}`",
                get_engine_hint(use_tiflash, PARTITION_TABLE_NAME)
            )
        };

        let tikv_count_row = sqlx::query(&count_query(false))
            .fetch_one(&mut tx)
            .await
            .context("fetch count from TiKV")?;
        let tikv_count: i64 = tikv_count_row.get("count");

        let tiflash_count_row = sqlx::query(&count_query(true))
            .fetch_one(&mut tx)
            .await
            .context("fetch count from TiFlash")?;
        let tiflash_count: i64 = tiflash_count_row.get("count");

        if tikv_count != tiflash_count {
            error!(
                "{} row count mismatch: TiKV={}, TiFlash={}",
                tag, tikv_count, tiflash_count
            );
            return Err(anyhow::anyhow!(
                "Row count mismatch: TiKV={}, TiFlash={}",
                tikv_count,
                tiflash_count
            ));
        }
        info!("{} row count matched: {} rows", tag, tikv_count);

        if tikv_count == 0 {
            info!("{} empty dataset, verification complete", tag);
            return Ok(());
        }

        // Step 2: Verify data consistency with id range query (tests partition pruning)
        let id_start = rand::random::<u64>() % tikv_count.max(1) as u64;
        let id_end = id_start + 50;
        let verify_query = |use_tiflash: bool| {
            format!(
                "SELECT {} id, data, created_at 
                FROM `{COLUMNAR_DB_NAME}`.`{PARTITION_TABLE_NAME}` 
                WHERE id BETWEEN {} AND {} 
                ORDER BY id 
                LIMIT 50",
                get_engine_hint(use_tiflash, PARTITION_TABLE_NAME),
                id_start,
                id_end
            )
        };

        info!(
            "{} verifying data with id range [{}, {}]",
            tag, id_start, id_end
        );
        let tikv_rows = sqlx::query(&verify_query(false))
            .fetch_all(&mut tx)
            .await
            .context("fetch data from TiKV")?;

        let tiflash_rows = sqlx::query(&verify_query(true))
            .fetch_all(&mut tx)
            .await
            .context("fetch data from TiFlash")?;

        if tikv_rows.len() != tiflash_rows.len() {
            error!(
                "{} result count mismatch: TiKV={}, TiFlash={}",
                tag,
                tikv_rows.len(),
                tiflash_rows.len()
            );
            return Err(anyhow::anyhow!(
                "Result count mismatch: TiKV={}, TiFlash={}",
                tikv_rows.len(),
                tiflash_rows.len()
            ));
        }

        // Step 3: Compare row data
        for (tikv_row, tiflash_row) in tikv_rows.iter().zip(tiflash_rows.iter()) {
            let tikv_id: i64 = tikv_row.get("id");
            let tiflash_id: i64 = tiflash_row.get("id");
            if tikv_id != tiflash_id {
                error!(
                    "{} id mismatch: TiKV id={}, TiFlash id={}",
                    tag, tikv_id, tiflash_id
                );
                return Err(anyhow::anyhow!(
                    "ID mismatch: TiKV id={}, TiFlash id={}",
                    tikv_id,
                    tiflash_id
                ));
            }

            let tikv_data: &str = tikv_row.get("data");
            let tiflash_data: &str = tiflash_row.get("data");
            if tikv_data != tiflash_data {
                error!(
                    "{} data mismatch for id={}: TiKV={}, TiFlash={}",
                    tag, tikv_id, tikv_data, tiflash_data
                );
                return Err(anyhow::anyhow!(
                    "Data mismatch for id={}: TiKV={}, TiFlash={}",
                    tikv_id,
                    tikv_data,
                    tiflash_data
                ));
            }
        }

        info!(
            "{} consistency verification passed: {} rows verified",
            tag,
            tikv_rows.len()
        );

        // Step 4: Verify aggregation across all partitions
        let agg_query = |use_tiflash: bool| {
            format!(
                "SELECT {} 
                COUNT(*) as cnt 
                FROM `{COLUMNAR_DB_NAME}`.`{PARTITION_TABLE_NAME}`",
                get_engine_hint(use_tiflash, PARTITION_TABLE_NAME)
            )
        };

        let tikv_agg = sqlx::query(&agg_query(false))
            .fetch_one(&mut tx)
            .await
            .context("fetch aggregation from TiKV")?;

        let tiflash_agg = sqlx::query(&agg_query(true))
            .fetch_one(&mut tx)
            .await
            .context("fetch aggregation from TiFlash")?;

        let tikv_cnt: i64 = tikv_agg.get("cnt");
        let tiflash_cnt: i64 = tiflash_agg.get("cnt");
        if tikv_cnt != tiflash_cnt {
            error!(
                "{} aggregation count mismatch: TiKV={}, TiFlash={}",
                tag, tikv_cnt, tiflash_cnt
            );
            return Err(anyhow::anyhow!(
                "Aggregation count mismatch: TiKV={}, TiFlash={}",
                tikv_cnt,
                tiflash_cnt
            ));
        }

        info!(
            "{} aggregation verification passed: count={}",
            tag, tikv_cnt
        );

        Ok(())
    }

    /// Main workload entry point
    async fn run_workload(&self) {
        let tag = format!("partition-workload-{}", self.keyspace_id);
        info!(
            "{} starting partition table workload with {} partitions",
            tag, self.partition_count
        );

        // Step 1: Prepare the partition test table
        if let Err(err) = self.prepare_partition_table().await {
            error!("{} failed to prepare partition table: {}", tag, err);
            return;
        }

        // Step 2: Start concurrent workloads
        let mut handles = Vec::new();

        // Insert/Delete workload
        let self_clone = self.clone_for_workload();
        handles.push(tokio::spawn(async move {
            self_clone.run_insert_delete_workload().await;
        }));

        // Consistency verification workload
        let self_clone = self.clone_for_workload();
        handles.push(tokio::spawn(async move {
            self_clone.run_verification_workload().await;
        }));

        // Wait for all workloads to complete
        futures::future::join_all(handles).await;

        info!("{} partition workload completed", tag);
    }

    /// Clone necessary fields for workload tasks
    fn clone_for_workload(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            running: self.running.clone(),
            keyspace_id: self.keyspace_id,
            max_id: self.max_id.clone(),
            partition_count: self.partition_count,
        }
    }

    /// Insert/Delete workload loop
    async fn run_insert_delete_workload(&self) {
        let tag = format!("partition-insert-delete-{}", self.keyspace_id);
        let start_time = Instant::now();

        while self.running.get() {
            if let Err(err) = self.execute_insert_delete_workload().await {
                error!("{} insert/delete workload error: {}", tag, err);
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        info!("{} insert/delete workload exit", tag; "dur" => ?start_time.saturating_elapsed());
    }

    /// Consistency verification workload loop
    async fn run_verification_workload(&self) {
        let tag = format!("partition-verify-{}", self.keyspace_id);
        let start_time = Instant::now();

        // Wait a bit before starting verification to allow some data to be inserted
        tokio::time::sleep(Duration::from_secs(5)).await;

        while self.running.get() {
            match self.verify_consistency().await {
                Ok(_) => {
                    info!("{} consistency verification passed", tag);
                }
                Err(err) => {
                    panic!("{} consistency verification failed: {}", tag, err);
                }
            }
            tokio::time::sleep(Duration::from_secs(8)).await;
        }

        info!("{} verification workload exit", tag; "dur" => ?start_time.saturating_elapsed());
    }
}
