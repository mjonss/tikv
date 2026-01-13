// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{path::PathBuf, str::FromStr, sync::atomic::Ordering, time::Duration};

use test_cloud_server::{keyspace::KeyspaceManager, tidb::TidbCluster, tpc::Tpc};
use tikv_util::{info, time::Instant};

use crate::{Running, TPCC_COUNTER, env_param};

const TPCC_WAREHOUSES: usize = 2;
const TPCC_MAX_PROCS: usize = 1;
const TPCC_THREADS: usize = 4; // Number of threads for each TPCC workload.

pub(crate) const TPCC_TABLES: &[&str] = &[
    "customer",
    "district",
    "history",
    "item",
    "new_order",
    "order_line",
    "orders",
    "stock",
    "warehouse",
];

pub(crate) async fn prepare_tpcc(
    tc: TidbCluster,
    keyspace_manager: KeyspaceManager,
    tpc_bin: String,
    keyspace_ids: Vec<u32>,
    use_txn_file: bool,
    concurrency: usize,
) {
    let mut handles = Vec::with_capacity(concurrency);
    for tpc_idx in 0..concurrency {
        let tc = tc.clone();
        let keyspace_manager = keyspace_manager.clone();
        let tpc_bin = PathBuf::from_str(&tpc_bin).unwrap();
        let keyspace_ids = keyspace_ids.to_vec();
        let task = async move {
            let db = db_name_by_tpc_idx(tpc_idx);

            for keyspace_id in keyspace_ids {
                let keyspace_name = keyspace_manager
                    .get_keyspace_meta(keyspace_id)
                    .unwrap()
                    .name();
                let tag = format!("tpcc-{}[{}]-{}", keyspace_id, keyspace_name, tpc_idx);
                let tidb_idx = TidbCluster::get_idx_by_keyspace_name(&keyspace_name);
                let params = tc.tidb.conn_params(tidb_idx);

                let mut tpc = Tpc::new(tag.clone(), tpc_bin.clone());
                tpc.tpcc()
                    .host(&params.host)
                    .port(params.port)
                    .user(&params.user)
                    .password(&params.password)
                    .db(&db)
                    .warehouses(TPCC_WAREHOUSES)
                    .max_procs(TPCC_MAX_PROCS);

                let conn_string = params.conn_string("test");
                let pool = sqlx::MySqlPool::connect(&conn_string).await.unwrap();

                info!("{} prepare_tpcc", tag; "use_txn_file" => use_txn_file);

                let mut sqls = vec![format!("CREATE DATABASE IF NOT EXISTS `{}`", db)];
                if use_txn_file {
                    sqls.push("SET GLOBAL tidb_txn_mode = 'optimistic'".to_string());
                }
                for sql in sqls {
                    info!("{} executing sql", tag; "sql" => &sql);
                    sqlx::query(&sql).execute(&pool).await.unwrap();
                }

                tpc.prepare(TPCC_THREADS).await.unwrap();
                tpc.check().await.unwrap();

                // Use txn file during preparation only. As using optimistic transaction for
                // TPC-C will meet lots of write conflicts.
                if use_txn_file {
                    let sql = "SET GLOBAL tidb_txn_mode = 'pessimistic'";
                    info!("{} executing sql", tag; "sql" => &sql);
                    sqlx::query(sql).execute(&pool).await.unwrap();
                }
            }
        };
        handles.push(tokio::spawn(task));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

pub(crate) fn spawn_tpcc(
    tc: TidbCluster,
    keyspace_manager: KeyspaceManager,
    tpc_bin: &str,
    tpc_idx: usize,
    run_duration: Duration,
    running: Running,
) -> tokio::task::JoinHandle<()> {
    let tpc_bin = PathBuf::from_str(tpc_bin).unwrap();
    tokio::spawn(async move {
        let db = db_name_by_tpc_idx(tpc_idx);

        let start_time = Instant::now_coarse();
        while running.get() {
            let random_keyspace = || {
                let mut rng = rand::thread_rng();
                keyspace_manager.get_zipf_random_keyspace(&mut rng)
            };
            let keyspace_id = random_keyspace();
            let keyspace_name = keyspace_manager
                .get_keyspace_meta(keyspace_id)
                .unwrap()
                .name();
            {
                let lock = keyspace_manager.get_keyspace_lock(keyspace_id);
                let guard = lock.try_shared_lock();
                if guard.is_none() {
                    tokio::task::yield_now().await;
                    continue;
                }
                let _guard = guard.unwrap();

                let tag = format!("tpcc-{}[{}]-{}", keyspace_id, keyspace_name, tpc_idx);
                let tidb_idx = TidbCluster::get_idx_by_keyspace_name(&keyspace_name);
                let params = tc.tidb.conn_params(tidb_idx);

                let mut tpc = Tpc::new(tag, tpc_bin.clone());
                tpc.tpcc()
                    .host(&params.host)
                    .port(params.port)
                    .user(&params.user)
                    .password(&params.password)
                    .db(&db)
                    .warehouses(TPCC_WAREHOUSES)
                    .max_procs(TPCC_MAX_PROCS);

                let txns = tpc
                    .run(TPCC_THREADS, false, true, run_duration)
                    .await
                    .unwrap();
                TPCC_COUNTER.fetch_add(txns, Ordering::Relaxed);
                tpc.check().await.unwrap();
            }
        }
        info!("tpcc workload exit"; "dur" => ?start_time.saturating_elapsed());
    })
}

pub(crate) fn db_name_by_tpc_idx(tpc_idx: usize) -> String {
    format!("tpcc_{tpc_idx}")
}

pub(crate) fn check_tpc_binary(tpc_bin: &str) {
    let mut cmd = std::process::Command::new(tpc_bin);
    cmd.arg("version");
    let output = cmd.output().unwrap();
    assert!(
        output.status.success(),
        "tpc binary check failed: {:?}",
        output
    );

    info!("tpc binary check passed"; "output" => ?output);
}

pub(crate) fn check_tpc() {
    let tpc_txns = TPCC_COUNTER.load(Ordering::Relaxed);
    let threshold = env_param("TPCC_TXNS_THRESHOLD", 100);
    assert!(
        tpc_txns >= threshold,
        "TPC-C transactions are too few: {} (threshold: {})",
        tpc_txns,
        threshold
    );
}
