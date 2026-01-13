// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    sync::atomic::{Ordering, Ordering::Relaxed},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use crossbeam::queue::ArrayQueue;
use futures::future::join_all;
use rand::prelude::*;
use sqlx::{MySql, MySqlPool, Row};
use test_cloud_server::{keyspace::KeyspaceManager, tidb::TidbCluster};
use tikv_util::{debug, error, info, time::Instant};

use crate::{
    JEPSEN_BANK_TXN_COUNTER, JEPSEN_BANK_TXN_RETRY_COUNTER, Running, env_param,
    sql_util::{
        MAX_PADDING_SIZE, Transaction, gen_padding, get_engine_hint, retry_or_panic,
        wait_tiflash_or_columnar_replicas_available,
    },
    test_tidb::connect_tidb,
};

pub(crate) const JEPSEN_BANK_WORKLOAD_CONCURRENCY: usize = 4;
pub(crate) const BANK_DB_NAME: &str = "jepsen-bank";
pub(crate) const ACCOUNTS_TABLE_NAME: &str = "accounts";
const BANK_ACCOUNTS: usize = 10;
const BANK_TXN_FILE_RATIO: f64 = 0.5;

const TIFLASH_REPLICAS_AVAILABLE_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) async fn prepare_jepsen_bank(
    tc: TidbCluster,
    keyspace_manager: KeyspaceManager,
    keyspace_id: u32,
    tiflash_replicas: Option<usize>,
) {
    let tag = format!("bank-ks{}", keyspace_id);
    let pool = connect_tidb(&tc, &keyspace_manager, keyspace_id).await;
    info!("{} prepare_jepsen_bank", tag);

    let mut sqls = vec![
        format!("drop database if exists `{BANK_DB_NAME}`"),
        format!("create database `{BANK_DB_NAME}`"),
        format!(
            "create table `{BANK_DB_NAME}`.`{ACCOUNTS_TABLE_NAME}` \
            (id int not null primary key, \
            balance bigint not null default 0, \
            ts bigint unsigned not null default 0, \
            padding varbinary({MAX_PADDING_SIZE}) not null default 0x0)"
        ),
    ];
    if let Some(tiflash_replicas) = tiflash_replicas {
        sqls.push(format!(
            "alter table `{BANK_DB_NAME}`.`{ACCOUNTS_TABLE_NAME}` set tiflash replica {tiflash_replicas}"
        ));
    }
    for account_id in 0..BANK_ACCOUNTS {
        sqls.push(format!(
            "insert into `{BANK_DB_NAME}`.`{ACCOUNTS_TABLE_NAME}` (id) values ({account_id})"
        ));
    }
    for sql in sqls {
        info!("{} prepare_jepsen_bank", tag; "sql" => &sql);
        sqlx::query(&sql).execute(&pool).await.unwrap();
    }

    if tiflash_replicas.is_some() {
        wait_tiflash_or_columnar_replicas_available(
            &tag,
            &pool,
            BANK_DB_NAME,
            ACCOUNTS_TABLE_NAME,
            TIFLASH_REPLICAS_AVAILABLE_TIMEOUT,
        )
        .await;
    }
}

pub(crate) async fn run_jepsen_bank(
    tc: TidbCluster,
    keyspace_manager: KeyspaceManager,
    keyspace_id: u32,
    jepsen_use_txn_file: bool,
    use_tiflash: bool,
    running: Running,
) {
    info!("run_jepsen_bank"; "use_txn_file" => jepsen_use_txn_file, "use_tiflash" => use_tiflash);
    let pool = connect_tidb(&tc, &keyspace_manager, keyspace_id).await;

    let mut handles = Vec::with_capacity(JEPSEN_BANK_WORKLOAD_CONCURRENCY);
    for tid in 0..JEPSEN_BANK_WORKLOAD_CONCURRENCY {
        let pool = pool.clone();
        let running = running.clone();
        let handle = tokio::spawn(async move {
            let tag = format!("bank-{}-{}", keyspace_id, tid);

            let mut padding = [0u8; MAX_PADDING_SIZE];
            let start_time = Instant::now();
            while running.get() {
                let mut random = || {
                    let mut rng = thread_rng();
                    let from_to = (0..BANK_ACCOUNTS).choose_multiple(&mut rng, 2);
                    let amount = rng.gen_range(0..100000);
                    let padding_len = gen_padding(2, &mut rng, &mut padding);
                    let use_txn_file = if jepsen_use_txn_file {
                        rng.gen_bool(BANK_TXN_FILE_RATIO)
                    } else {
                        false
                    };
                    (from_to[0], from_to[1], amount, padding_len, use_txn_file)
                };
                let (from, to, amount, padding_len, use_txn_file) = random();
                let padding = &padding[..padding_len];

                retry_or_panic!(
                    bank_transfer(&tag, &pool, from, to, amount, use_txn_file, padding)
                        .await
                        .context("bank_transfer"),
                    |_| JEPSEN_BANK_TXN_RETRY_COUNTER.fetch_add(1, Relaxed)
                );
                JEPSEN_BANK_TXN_COUNTER.fetch_add(1, Relaxed);
            }
            info!("jepsen bank workload exit"; "tag" => tag, "dur" => ?start_time.saturating_elapsed());
        });
        handles.push(handle);
    }

    let pool_copy = pool.clone();
    handles.push(tokio::spawn(async move {
        let tag = format!("bank-verify-{}", keyspace_id);
        let start_time = Instant::now();
        while running.get() {
            retry_or_panic!(verify_bank(&tag, &pool_copy, use_tiflash).await);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        info!("verify bank accounts thread exit"; "dur" => ?start_time.saturating_elapsed());
    }));

    join_all(handles).await;

    let tag = format!("bank-final-{}", keyspace_id);
    let mut retry = 0;
    while retry <= 30 {
        retry += 1;
        retry_or_panic!(verify_bank(&tag, &pool, use_tiflash).await);

        dump_accounts_history();
        return;
    }
    panic!("jepsen_bank: final check retry limit exceeded");
}

async fn bank_transfer(
    tag: &str,
    pool: &MySqlPool,
    from: usize,
    to: usize,
    amount: i32,
    use_txn_file: bool,
    padding: &[u8],
) -> Result<()> {
    lazy_static::lazy_static! {
        static ref TRANSFER_QUERY: String = format!(
            "update `{}`.`{}` set balance = balance + ?, ts = ?, padding = ? where id = ?",
            BANK_DB_NAME, ACCOUNTS_TABLE_NAME
        );
    }
    let mut txn = Transaction::begin(tag, pool, use_txn_file)
        .await
        .context("begin")?;

    let start_ts = txn.start_ts();
    info!("{} bank_transfer: {}->{}", tag, from, to; "amount" => amount, "start_ts" => start_ts, "padding.len" => padding.len());
    sqlx::query(&TRANSFER_QUERY)
        .bind(-amount)
        .bind(start_ts)
        .bind(padding)
        .bind(from as i32)
        .execute(txn.conn())
        .await
        .with_context(|| format!("bank_transfer:{start_ts}:[{from}]->{amount}"))?;
    sqlx::query(&TRANSFER_QUERY)
        .bind(amount)
        .bind(start_ts)
        .bind(padding)
        .bind(to as i32)
        .execute(txn.conn())
        .await
        .with_context(|| format!("bank_transfer:{start_ts}:{amount}->[{to}]"))?;
    txn.commit().await.context("bank_transfer_commit")
}

async fn verify_bank_accounts(
    txn: &mut Transaction,
    pool: &MySqlPool,
    use_tiflash: bool,
) -> Result<()> {
    let engine_hint = get_engine_hint(use_tiflash, ACCOUNTS_TABLE_NAME);
    // Cast sum to "signed", as sum() return Decimal which is not easy to handle.
    let sql = format!(
        "select {engine_hint} cast(sum(balance) as signed) as sum from `{BANK_DB_NAME}`.`{ACCOUNTS_TABLE_NAME}`"
    );
    let row = sqlx::query(&sql)
        .fetch_one(txn.conn())
        .await
        .context("select sum")?;
    let sum: i64 = row.get("sum");
    if sum != 0 {
        let accounts = list_bank_accounts(txn.conn(), use_tiflash).await;

        // Try again.
        {
            let mut txn = Transaction::begin_read("verify_again", pool)
                .await
                .context("begin")?;
            let row = sqlx::query(&sql)
                .fetch_one(txn.conn())
                .await
                .context("select sum")?;
            let sum: i32 = row.get("sum");
            let accounts = list_bank_accounts(txn.conn(), use_tiflash).await;
            info!("verify_bank_accounts: try again"; "sum" => sum, "accounts" => ?accounts,
                "read_ts" => txn.start_ts(), "use_tiflash" => use_tiflash);
        }

        #[cfg(feature = "debug-trace-txn-tasks")]
        {
            tikv::storage::txn::debug::dump_txn_tasks();
        }
        dump_accounts_history();

        panic!(
            "sum is not zero: {}, read_ts {}, accounts {:?}, use_tiflash {}",
            sum,
            txn.start_ts(),
            accounts,
            use_tiflash
        );
    }
    Ok(())
}

async fn verify_bank(tag: &str, pool: &MySqlPool, use_tiflash: bool) -> Result<()> {
    let mut txn = Transaction::begin_read(tag, pool).await.context("begin")?;
    let read_ts = txn.start_ts();

    verify_bank_accounts(&mut txn, pool, false)
        .await
        .context("verify")?;
    let accounts = list_bank_accounts(txn.conn(), false)
        .await
        .context("list")?;
    debug!("accounts: {:?}", accounts; "read_ts" => read_ts);

    if use_tiflash {
        verify_bank_accounts(&mut txn, pool, true)
            .await
            .context("verify_tiflash")?;
        let tiflash_accounts = list_bank_accounts(txn.conn(), true)
            .await
            .context("list_tiflash")?;
        debug!("accounts from TiFlash: {:?}", accounts; "read_ts" => read_ts);
        assert_eq!(accounts, tiflash_accounts);
    }

    trace_accounts(read_ts, &accounts);
    Ok(())
}

#[derive(PartialEq)]
struct Account {
    id: i32,
    balance: i64,
    ts: u64,
    padding: Vec<u8>,
}

impl fmt::Debug for Account {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Account")
            .field("id", &self.id)
            .field("balance", &self.balance)
            .field("ts", &self.ts)
            .field("padding", &hex::encode(&self.padding))
            .finish()
    }
}

async fn list_bank_accounts<'a, E>(executor: E, use_tiflash: bool) -> Result<Vec<Account>>
where
    E: sqlx::Executor<'a, Database = MySql>,
{
    let engine_hint = get_engine_hint(use_tiflash, ACCOUNTS_TABLE_NAME);
    let sql = format!(
        "select {engine_hint} id, balance, ts, padding from `{BANK_DB_NAME}`.`{ACCOUNTS_TABLE_NAME}` order by id"
    );
    let rows = sqlx::query(&sql)
        .fetch_all(executor)
        .await
        .context("select all")?;
    let rows_len = rows.len();
    let accounts = rows
        .into_iter()
        .map(|row| {
            let id: i32 = row.get("id");
            let balance: i64 = row.get("balance");
            let ts: u64 = row.get("ts");
            let mut padding: Vec<u8> = row.get("padding");
            padding.truncate(16);
            Account {
                id,
                balance,
                ts,
                padding,
            }
        })
        .collect::<Vec<_>>();

    if rows_len != BANK_ACCOUNTS {
        error!("list bank accounts: rows len not match";
            "rows_len" => rows_len, "accounts" => ?accounts, "use_tiflash" => use_tiflash);
        bail!(
            "rows len not match: {}, detail {:?}, use_tiflash {}",
            rows_len,
            accounts,
            use_tiflash
        );
    }

    Ok(accounts)
}

pub(crate) fn check_jepsen() {
    let jepsen_txns = JEPSEN_BANK_TXN_COUNTER.load(Ordering::Relaxed);
    let threshold = env_param("JEPSEN_TXNS_THRESHOLD", 100);
    assert!(
        jepsen_txns >= threshold,
        "Jepsen transactions are too few: {} (threshold: {})",
        jepsen_txns,
        threshold
    );
}

lazy_static::lazy_static! {
    static ref ACCOUNTS_HISTORY: ArrayQueue<(u64 /* read_ts */, Vec<Account>)> = ArrayQueue::new(16);
}

fn trace_accounts(read_ts: u64, accounts: &[Account]) {
    let accounts = accounts
        .iter()
        .map(|acc| Account {
            id: acc.id,
            balance: acc.balance,
            ts: acc.ts,
            padding: acc.padding[..acc.padding.len().min(16)].to_vec(),
        })
        .collect::<Vec<_>>();
    ACCOUNTS_HISTORY.force_push((read_ts, accounts));
}

fn dump_accounts_history() {
    let mut history = Vec::with_capacity(ACCOUNTS_HISTORY.len());
    while let Some((read_ts, accounts)) = ACCOUNTS_HISTORY.pop() {
        history.push((read_ts, accounts));
    }
    info!("accounts history: {:?}", history);
}
