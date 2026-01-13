// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use anyhow::Context;
use collections::HashSet;
use kvengine::IdVer;
use rand::prelude::*;
use schema::schema::{
    STORAGE_CLASS_TIER_STANDARD, StorageClass, StorageClassSpec, StorageClassTransitRule,
    StorageClassTransitionInfo,
};
use serde_derive::Serialize;
use sqlx::Executor;
use test_cloud_server::{
    ServerCluster, TryWaiter,
    keyspace::{KeyspaceManager, make_row_key},
    table::TableMeta,
    tidb::TidbCluster,
};
use tikv_util::{codec::bytes::decode_bytes, debug, error, info, time::Instant};

use crate::{
    ALTER_TABLE_AUTO_IA_COUNTER, ALTER_TABLE_IA_COUNTER, ALTER_TABLE_NON_IA_COUNTER,
    ASYNC_SHARD_COUNTER, Running,
    sql_util::retry_or_panic,
    test_tidb::{connect_tidb, query_tidb_table_schema},
};

type Result<T> = anyhow::Result<T>;

pub(crate) async fn spawn_alter_storage_class(
    tc: TidbCluster,
    keyspace_manager: KeyspaceManager,
    tables: Vec<Arc<TableMeta>>,
    ia_table_ratio: f64,
    max_interval: Duration,
    running: Running,
) {
    let random = || {
        let mut rng = thread_rng();
        let interval = rng.gen_range(Duration::ZERO..=max_interval);
        let skip = !rng.gen_bool(ia_table_ratio);
        let table = tables.choose(&mut rng).unwrap();
        (interval, skip, table)
    };
    let random_sc_spec = || {
        let mut rng = thread_rng();
        match rng.gen_range(0..=2) {
            0 => StorageClass::Unspecified.into(),
            1 => StorageClass::Ia.into(),
            2 => StorageClassSpec {
                default_tier: StorageClass::Unspecified,
                transit_rules: vec![StorageClassTransitRule {
                    tier: StorageClass::Ia,
                    transit_after: Duration::from_secs(rng.gen_range(1..=10)),
                }],
            },
            _ => unreachable!(),
        }
    };

    while running.get() {
        let (interval, skip, table) = random();
        tokio::time::sleep(interval).await;
        if skip {
            continue;
        }

        let target_sc_spec = loop {
            let target_sc_spec = random_sc_spec();
            if target_sc_spec != table.storage_class_spec() {
                break target_sc_spec;
            }
        };
        info!("alter storage class"; "table" => ?table, "target" => ?target_sc_spec);
        retry_or_panic!(
            alter_storage_class(&tc, &keyspace_manager, table, target_sc_spec.clone())
                .await
                .context("alter_sc")
        );
        if target_sc_spec.must_be_ia() {
            ALTER_TABLE_IA_COUNTER.fetch_add(1, Ordering::Relaxed);
        } else if target_sc_spec.can_be_ia() {
            ALTER_TABLE_AUTO_IA_COUNTER.fetch_add(1, Ordering::Relaxed);
        } else {
            ALTER_TABLE_NON_IA_COUNTER.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Default, Serialize)]
struct TidbEngineAttrStr {
    storage_class: String,
}

#[derive(Default, Serialize)]
struct TidbEngineAttr {
    storage_class: TidbStorageClass,
}

#[derive(Default, Serialize)]
struct TidbStorageClass {
    tier: String,
    transitions: Vec<StorageClassTransitionInfo>,
}

async fn alter_storage_class(
    tc: &TidbCluster,
    keyspace_manager: &KeyspaceManager,
    table: &TableMeta,
    sc_spec: StorageClassSpec,
) -> Result<()> {
    let pool = connect_tidb(tc, keyspace_manager, table.keyspace_id()).await;
    let mut conn = pool.acquire().await.context("acquire")?;
    let engine_attr_str = match sc_spec.to_tidb() {
        (None, None) => "".to_string(),
        (tier, None) => serde_json::to_string(&TidbEngineAttrStr {
            storage_class: tier.unwrap_or_else(|| STORAGE_CLASS_TIER_STANDARD.to_string()),
        })
        .unwrap(),
        (tier, Some(trans)) => serde_json::to_string(&TidbEngineAttr {
            storage_class: TidbStorageClass {
                tier: tier.unwrap_or_else(|| STORAGE_CLASS_TIER_STANDARD.to_string()),
                transitions: trans,
            },
        })
        .unwrap(),
    };

    let alter_table = format!(
        "ALTER TABLE `{}`.`{}` ENGINE_ATTRIBUTE = '{}'",
        table.db_name(),
        table.table_name(),
        engine_attr_str
    );
    info!("alter table: {}", &alter_table);
    conn.execute(alter_table.as_str())
        .await
        .context("alter_table")?;

    let schema = query_tidb_table_schema(&mut conn, table.db_name(), table.table_name())
        .await
        .context("query_schema")?;
    assert_eq!(table.id(), schema.id);
    assert_eq!(sc_spec, schema.storage_class_spec);
    table.set_storage_class_spec(sc_spec);
    Ok(())
}

pub(crate) fn check_storage_class(
    cluster: &ServerCluster,
    tables: &[Arc<TableMeta>],
    timeout: Duration,
) {
    let pd_client = cluster.get_pd_client_ext();
    let mut async_shards = 0;
    let mut success_shards: HashSet<IdVer> = HashSet::default();
    let mut last_errs: Vec<String> = vec![];

    let start_time = Instant::now_coarse();
    while start_time.saturating_elapsed() < timeout {
        last_errs.clear();
        for table in tables {
            // Get the final target storage class.
            // All transitions should have `transit_after` less than test duration to make
            // result determined.
            let expect_sc = match table
                .storage_class_spec()
                .target_storage_class(Duration::MAX)
            {
                StorageClass::Unspecified | StorageClass::Standard => StorageClass::Unspecified, /* CSE will convert STANDARD to UNSPECIFIED. */
                StorageClass::Ia => StorageClass::Ia,
            };
            let expect_sync = expect_sc.is_sync();

            let mut key = make_row_key(table.keyspace_id(), table.id(), b"");
            let end_key = make_row_key(table.keyspace_id(), table.id() + 1, b"");
            while key < end_key {
                debug!("check storage class";
                    "table" => ?table,
                    "expect_sc" => ?expect_sc,
                    "expect_sync" => expect_sync,
                    "start_key" => log_wrappers::Value::key(&key),
                    "end_key" => log_wrappers::Value::key(&end_key),
                );

                let mut next_key: Option<Vec<u8>> = None;
                let mut shard_id_ver: Option<IdVer> = None;
                let ok = TryWaiter::timeout(1).interval_dur(Duration::from_millis(300)).try_wait(
                    || {
                        let Some(shard) = cluster.get_latest_shard_by_key(&key) else {
                            return false;
                        };
                        next_key = Some(shard.outer_end.to_vec());

                        let id_ver = IdVer::new(shard.id, shard.ver);
                        shard_id_ver = Some(id_ver);
                        if success_shards.contains(&id_ver) {
                            return true;
                        }

                        let snap = shard.new_snap_access();
                        let shard_sc = shard.get_storage_class_spec().target_storage_class(Duration::MAX);
                        let ok = expect_sc == shard_sc;
                        let check_sync = || {
                            if expect_sync {
                                snap.is_sync()
                            } else {
                                !snap.is_sync() || snap.write_cf_level_n_is_empty()
                            }
                        };
                        let ok = ok && check_sync();
                        info!("{} check storage class: ok: {}", id_ver, ok;
                            "table" => ?table, "snap" => ?snap.display(),
                            "expect_sc" => ?expect_sc, "shard_sc" => ?shard_sc,
                            "table_spec" => ?table.storage_class_spec(), "shard_spec" => ?shard.get_storage_class_spec(),
                            "expect_sync" => expect_sync, "sync" => snap.is_sync(), "write_cf_level_n_is_empty" => snap.write_cf_level_n_is_empty(),
                        );
                        if ok {
                            success_shards.insert(id_ver);
                            if !expect_sync {
                                async_shards += 1;
                            }
                        }
                        ok
                    },
                );

                if !ok {
                    let snap = shard_id_ver.and_then(|id_ver| {
                        cluster
                            .get_latest_shard(id_ver.id)
                            .map(|x| x.new_snap_access().display())
                    });
                    let err_msg = format!(
                        "{} check storage class failed: table: {:?}, shard: {:?}",
                        shard_id_ver.unwrap_or_default(),
                        table,
                        snap,
                    );
                    last_errs.push(err_msg);
                }

                key = match next_key {
                    Some(k) => k,
                    None => {
                        let region = pd_client.get_region(&key).unwrap_or_else(|err| {
                            panic!("get region failed: {:?}, {:?}", key, err)
                        });
                        decode_bytes(&mut region.end_key.as_slice(), false).unwrap()
                    }
                };
            }
        }

        if last_errs.is_empty() {
            break;
        }
    }

    if !last_errs.is_empty() {
        let errs_count = last_errs.len();
        for err_msg in last_errs {
            error!("{}", err_msg);
        }
        panic!("check storage class failed: {}", errs_count);
    }

    ASYNC_SHARD_COUNTER.fetch_add(async_shards, Ordering::Relaxed);
}
