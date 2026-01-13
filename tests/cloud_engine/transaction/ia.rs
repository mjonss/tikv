// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{assert_matches::assert_matches, convert::TryInto, time::Duration};

use bytes::Bytes;
use futures::{executor::block_on, future::try_join_all};
use kvengine::ia::util::IaConfig;
use kvproto::kvrpcpb;
use rstest::rstest;
use schema::schema::{StorageClass, StorageClassSpec};
use test_cloud_server::{
    MajorCompactionTarget, ServerClusterBuilder, ServerClusterExt, TryWaiter, alloc_node_id_vec,
    client::{
        ApiV2NoPrefixCodec, ClusterClientOptions, CommitAction, Error as ClientError,
        MutateOptions, TxnMutations, TxnWriteMethod,
    },
    keyspace::{CreateKeyspaceOptions, make_row_key},
    try_wait_result_async,
    util::Mutation,
};
use test_util::init_log_for_test;
use tikv_client::{Transaction, TransactionOptions, transaction::HeartbeatOption};
use tikv_util::{config::ReadableSize, info, time::Instant};

use crate::random_value;

const DATA_COUNT: usize = 500;
const TXN_FILE_MAX_CHUNK_SIZE: usize = 1024;

#[rstest]
#[case(false, TxnWriteMethod::Normal)]
#[case::ia(true, TxnWriteMethod::Normal)]
#[case::txn_file(true, TxnWriteMethod::FileBased)]
fn test_get_put(#[case] enable_ia: bool, #[case] write_method: TxnWriteMethod) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _enter = rt.enter();

    let (cluster, keyspace_id, table_id) = prepare_cluster(enable_ia);
    let gen_key = i_to_key(keyspace_id, table_id);

    let mut client = cluster.new_client_opt(ClusterClientOptions {
        txn_file_max_chunk_size: Some(TXN_FILE_MAX_CHUNK_SIZE),
        ..Default::default()
    });

    let key = Bytes::from(gen_key(100));
    let val = Bytes::from("v100");

    let ts0 = client.get_ts().into_inner();
    let v0 = client.get_key_version(&key, ts0).unwrap().unwrap();

    // Prewrite:
    let start_ts = client.get_ts();
    let mutation = make_put(key.clone(), val.clone());
    let txn_muts = block_on(TxnMutations::build(
        vec![mutation.clone()],
        write_method,
        client.txn_file_helper(),
    ))
    .unwrap();
    client
        .kv_prewrite(key.clone(), None, txn_muts.clone(), start_ts)
        .unwrap();
    let commit_ts = client.get_ts();

    // "get" advance the min_commit_ts:
    let ts2 = client.get_ts().into_inner();
    let v = client.get_key_version(&key, ts2).unwrap();
    assert_eq!(v.as_ref(), Some(&v0));

    // Commit:
    let new_commit_ts = client
        .kv_commit(txn_muts.clone(), start_ts, commit_ts, false)
        .unwrap();
    assert!(new_commit_ts > commit_ts);

    // Verify:
    let v = client.get_key_version(&key, ts2).unwrap();
    assert_eq!(v.as_ref(), Some(&v0));
    let v = client
        .get_key_version(&key, new_commit_ts.into_inner())
        .unwrap();
    assert_eq!(v.as_deref(), Some(val.as_ref()));

    client.put_kv_in_ref_store(vec![mutation]);
    client.verify_data_with_ref_store();
}

#[rstest]
#[case(false)]
#[case::ia(true)]
fn test_get_put_pessimistic(#[case] enable_ia: bool) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _enter = rt.enter();

    let (cluster, keyspace_id, table_id) = prepare_cluster(enable_ia);
    let gen_key = i_to_key(keyspace_id, table_id);

    let mut client = cluster.new_client();

    let key = gen_key(100);
    let val = b"v100".to_vec();

    let ts0 = client.get_ts().into_inner();
    let v0 = client.get_key_version(&key, ts0).unwrap().unwrap();

    let task = async {
        let txn_client = cluster.new_txn_client().await;

        let mut tx = txn_client.begin_pessimistic().await.unwrap();
        // Lock:
        let v = tx.get_for_update(key.clone()).await.unwrap().unwrap();
        assert_eq!(v, v0);

        let ts2 = client.get_ts().into_inner();
        let v = client.get_key_version(&key, ts2).unwrap();
        assert_eq!(v.as_ref(), Some(&v0));

        tx.put(key.clone(), val.clone()).await.unwrap();
        tx.commit().await.unwrap();
    };
    rt.block_on(task);

    client.put_kv_in_ref_store(vec![make_put(key.into(), val.into())]);
    client.verify_data_with_ref_store();
}

#[rstest]
#[case(false)]
#[case::ia(true)]
fn test_scan(#[case] enable_ia: bool) {
    let (cluster, ..) = prepare_cluster(enable_ia);

    let client = cluster.new_client();
    let ref_store = client.ref_store();
    let ref_store = ref_store.lock().unwrap();

    let task = async {
        let mut txn_client = cluster.new_txn_client().await;
        let cnt = txn_client
            .verify_data_by_scan(&ref_store, None)
            .await
            .unwrap();
        assert_eq!(cnt, DATA_COUNT);
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(task);
}

#[rstest]
#[case(false)]
#[case::ia(true)]
fn test_batch_get(#[case] enable_ia: bool) {
    let (cluster, keyspace_id, table_id) = prepare_cluster(enable_ia);
    let gen_key = i_to_key(keyspace_id, table_id);

    let client = cluster.new_client();
    let ref_store = client.ref_store();
    let ref_store = ref_store.lock().unwrap();
    let keys = vec![gen_key(0), gen_key(100), gen_key(200)];
    let values = keys
        .iter()
        .map(|k| ref_store.get(k.as_slice()).cloned().unwrap().unwrap())
        .collect::<Vec<_>>();
    drop(ref_store);

    let task = async {
        let txn_client = cluster.new_txn_client().await;
        let start_ts = txn_client.current_timestamp().await.unwrap();
        let mut snapshot = txn_client.snapshot(start_ts, TransactionOptions::default().read_only());
        let kvs = snapshot.batch_get(keys.clone()).await.unwrap();
        let mut cnt = 0;
        for (i, kv) in kvs.enumerate() {
            assert_eq!(kv.key(), keys[i].as_ref());
            assert_eq!(kv.value(), &values[i]);
            cnt += 1;
        }
        assert_eq!(cnt, keys.len());
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(task);
}

#[rstest]
#[case(false, TxnWriteMethod::Normal)]
#[case::ia(true, TxnWriteMethod::Normal)]
#[case::txn_file(true, TxnWriteMethod::FileBased)]
fn test_write_conflict(#[case] enable_ia: bool, #[case] write_method: TxnWriteMethod) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _enter = rt.enter();

    let (cluster, keyspace_id, table_id) = prepare_cluster(enable_ia);
    let gen_key = i_to_key(keyspace_id, table_id);

    let mut client = cluster.new_client_opt(ClusterClientOptions {
        txn_file_max_chunk_size: Some(TXN_FILE_MAX_CHUNK_SIZE),
        ..Default::default()
    });

    let key = Bytes::from(gen_key(100));
    let val = Bytes::from("v100");

    let ts0 = client.get_ts();

    // Write:
    let mutation = make_put(key.clone(), val.clone());
    let txn_muts = block_on(TxnMutations::build(
        vec![mutation.clone()],
        write_method,
        client.txn_file_helper(),
    ))
    .unwrap();
    let start_ts = client.get_ts();
    client
        .kv_prewrite(key.clone(), None, txn_muts.clone(), start_ts)
        .unwrap();
    let commit_ts = client.get_ts();
    client
        .kv_commit(txn_muts.clone(), start_ts, commit_ts, false)
        .unwrap();

    // Another write:
    let err = client
        .kv_prewrite(key.clone(), None, txn_muts.clone(), ts0)
        .unwrap_err();
    assert_matches!(err, ClientError::WriteConflict { .. });

    client.put_kv_in_ref_store(vec![mutation]);
    client.verify_data_with_ref_store();
}

#[rstest]
#[case(false)]
#[case::ia(true)]
fn test_write_conflict_pessimistic(#[case] enable_ia: bool) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _enter = rt.enter();

    let (cluster, keyspace_id, table_id) = prepare_cluster(enable_ia);
    let gen_key = i_to_key(keyspace_id, table_id);

    let key = gen_key(1000);
    const CONCURRENCY: usize = 16;

    let mut client = cluster.new_client();
    let val = CONCURRENCY.to_be_bytes().to_vec();
    client.put_kv_in_ref_store(vec![make_put(key.clone().into(), val.into())]);
    let ref_store = client.dump_ref_store();

    let task = async {
        let mut txn_client = cluster.new_txn_client().await;

        let mut tx = txn_client.begin_pessimistic().await.unwrap();
        tx.insert(key.clone(), 0u64.to_be_bytes().to_vec())
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Concurrent "v = v + 1":
        let mut handles = Vec::with_capacity(CONCURRENCY);
        for _ in 0..CONCURRENCY {
            let cli = txn_client.inner.clone();
            let key = key.clone();

            let t = async move {
                loop {
                    let mut tx = cli.begin_pessimistic().await.unwrap();
                    match inc(&mut tx, &key).await {
                        Ok(()) => {
                            tx.commit().await.unwrap();
                            break;
                        }
                        Err(err) if is_client_err_retryable(&err, &is_write_conflict) => {
                            tx.rollback().await.unwrap();
                            continue;
                        }
                        Err(err) => {
                            let _ = tx.rollback().await;
                            panic!("unexpected error: {:?}", err)
                        }
                    }
                }
            };
            handles.push(tokio::spawn(t));
        }
        try_join_all(handles).await.unwrap();

        // Verify:
        let ts = txn_client.current_timestamp().await.unwrap();
        let mut snapshot = txn_client.snapshot(ts, TransactionOptions::default());
        let v = snapshot.get(key.clone()).await.unwrap().unwrap();
        let v = u64::from_be_bytes(v.try_into().unwrap());
        assert_eq!(v, CONCURRENCY as u64);

        let cnt = txn_client
            .verify_data_by_scan(&ref_store, None)
            .await
            .unwrap();
        assert_eq!(cnt, DATA_COUNT + 1);
    };
    rt.block_on(task);
}

#[rstest]
#[case(false, TxnWriteMethod::Normal)]
#[case::ia(true, TxnWriteMethod::Normal)]
#[case::txn_file(true, TxnWriteMethod::FileBased)]
fn test_insert(#[case] enable_ia: bool, #[case] write_method: TxnWriteMethod) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _enter = rt.enter();

    let (cluster, keyspace_id, table_id) = prepare_cluster(enable_ia);
    let gen_key = i_to_key(keyspace_id, table_id);

    let mut client = cluster.new_client_opt(ClusterClientOptions {
        txn_file_max_chunk_size: Some(TXN_FILE_MAX_CHUNK_SIZE),
        ..Default::default()
    });

    {
        // Insert:
        let key = Bytes::from(gen_key(100));
        let val = Bytes::from("v100");
        let mutation = make_insert(key.clone(), val.clone());
        let txn_muts = block_on(TxnMutations::build(
            vec![mutation.clone()],
            write_method,
            client.txn_file_helper(),
        ))
        .unwrap();
        let err = client
            .kv_prewrite(key.clone(), None, txn_muts, client.get_ts())
            .unwrap_err();
        assert_matches!(err, ClientError::AlreadyExist { .. });
    }

    {
        // Another insert:
        let key = Bytes::from(gen_key(1000));
        let val = Bytes::from("v1000");
        let mutation = make_insert(key.clone(), val.clone());
        let txn_muts = block_on(TxnMutations::build(
            vec![mutation.clone()],
            write_method,
            client.txn_file_helper(),
        ))
        .unwrap();
        let start_ts = client.get_ts();
        client
            .kv_prewrite(key.clone(), None, txn_muts.clone(), start_ts)
            .unwrap();
        client
            .kv_commit(txn_muts.clone(), start_ts, client.get_ts(), false)
            .unwrap();

        client.put_kv_in_ref_store(vec![mutation]);
    }

    client.verify_data_with_ref_store();
}

#[rstest]
#[case(false, TxnWriteMethod::Normal, false)]
#[case::ia(true, TxnWriteMethod::Normal, false)]
#[case::cleanup(true, TxnWriteMethod::Normal, true)]
#[case::txn_file(true, TxnWriteMethod::FileBased, false)]
fn test_resolve_lock(
    #[case] enable_ia: bool,
    #[case] write_method: TxnWriteMethod,
    #[case] use_cleanup: bool,
) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _enter = rt.enter();

    let (cluster, keyspace_id, table_id) = prepare_cluster(enable_ia);
    let gen_key = i_to_key(keyspace_id, table_id);

    let mut client = cluster.new_client_opt(ClusterClientOptions {
        txn_file_max_chunk_size: Some(TXN_FILE_MAX_CHUNK_SIZE),
        ..Default::default()
    });

    let cases = vec![
        (
            0,
            100,
            CommitAction::AsyncCommitSecondaryKeys(Duration::ZERO),
        ),
        (
            200,
            300,
            CommitAction::AsyncCommitSecondaryKeys(Duration::MAX),
        ),
        (400, 600, CommitAction::NoCommit),
    ];
    for (start, end, commit_action) in cases {
        client
            .try_put_kv(
                start..end,
                &gen_key,
                |_| random_value(8),
                MutateOptions {
                    commit_action,
                    write_method,
                    ..Default::default()
                },
            )
            .unwrap();
    }

    if use_cleanup {
        // TxnClient rust use "cleanup" interface to rollback locks.
        let ref_store = client.dump_ref_store();
        let task = async {
            let mut txn_client = cluster.new_txn_client().await;
            let cnt = txn_client
                .verify_data_by_scan(&ref_store, None)
                .await
                .unwrap();
            assert_eq!(cnt, DATA_COUNT);
        };
        rt.block_on(task);
    }

    client.verify_data_with_ref_store();
}

#[rstest]
#[case(false, false)]
#[case::cleanup(false, true)]
#[case::ia(true, false)]
#[case::ia_cleanup(true, true)]
fn test_resolve_pessimistic_lock(#[case] enable_ia: bool, #[case] use_cleanup: bool) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _enter = rt.enter();

    let (cluster, keyspace_id, table_id) = prepare_cluster(enable_ia);
    let gen_key = i_to_key(keyspace_id, table_id);

    let key_ids = vec![100usize, 200, 300];
    let keys = key_ids.iter().map(|&k| gen_key(k)).collect::<Vec<_>>();

    let mut client = cluster.new_client();
    client.set_req_timeout(Duration::from_secs(15));

    let task = async {
        let txn_client = cluster.new_txn_client().await;

        let mut tx = txn_client
            .begin_with_options(
                TransactionOptions::new_pessimistic()
                    .heartbeat_option(HeartbeatOption::NoHeartbeat),
            )
            .await
            .unwrap();
        // TTL: 20s
        // TODO: reduce TTL.
        tx.lock_keys(keys.clone()).await.unwrap();

        // Wait lock timeout: 15s
        let err = client
            .try_put_kv(
                key_ids.clone(),
                &gen_key,
                |_| random_value(8),
                MutateOptions::default(),
            )
            .unwrap_err();
        assert_matches!(err, ClientError::KeyErrors(errs) if errs.iter().all(|e| e.has_locked()));

        if use_cleanup {
            // Retry as `lock_keys` would timeout before TTL expired.
            try_wait_result_async(
                || {
                    let txn_client = txn_client.clone();
                    let keys = keys.clone();
                    Box::pin(async move {
                        let mut tx1 = txn_client.begin_pessimistic().await.unwrap();
                        tx1.lock_keys(keys).await?;
                        tx1.rollback().await
                    })
                },
                20, // TTL 20s
            )
            .await
            .unwrap();

            tx.rollback().await.unwrap();
        } else {
            tx.rollback().await.unwrap();
            // FIXME: resolve pessimistic lock is not implemented.
            client
                .try_put_kv(
                    key_ids,
                    &gen_key,
                    |_| random_value(8),
                    MutateOptions::default(),
                )
                .unwrap();
        }
    };
    rt.block_on(task);

    client.verify_data_with_ref_store();
}

// TODO: test_unsafe_destroy_range
// TODO: test_mvcc_by_key
// TODO: test_mvcc_by_start_ts

fn prepare_cluster(
    enable_ia: bool,
) -> (
    ServerClusterExt,
    u32, // keyspace_id
    i64, // table_id
) {
    init_log_for_test();
    let node_ids = alloc_node_id_vec(3);
    let cluster = ServerClusterBuilder::new(node_ids, |_, conf| {
        conf.rocksdb.writecf.write_buffer_size = ReadableSize(1024);
        conf.rocksdb.writecf.block_size = ReadableSize(256);
        conf.rocksdb.writecf.target_file_size_base = ReadableSize(2048);
        if enable_ia {
            conf.kvengine.ia = IaConfig {
                mem_cap: (64 * 1024).into(),
                disk_cap: (1024 * 1024).into(),
                segment_size: 1024,
                ..Default::default()
            }
        }
    })
    .pd_server_cnt(1)
    .tikv_worker_cnt(1)
    .build_ext();

    let ks_opts = CreateKeyspaceOptions {
        table_count: 1,
        storage_class_spec_fn: Box::new(move |_| {
            if enable_ia {
                StorageClass::Ia.into()
            } else {
                StorageClassSpec::default()
            }
        }),
        ..Default::default()
    };
    let keyspace_id = block_on(cluster.create_keyspace(&ks_opts, Duration::from_secs(10)));

    let ks_meta = cluster
        .keyspace_manager()
        .get_keyspace_meta(keyspace_id)
        .unwrap();
    let table_id = ks_meta.get_all_available_tables()[0];
    drop(ks_meta);
    let table_key = make_row_key(keyspace_id, table_id, &[]);
    cluster.wait_region_replicated(&table_key, 3);

    // Put base data to generate level 1+ tables.
    let mut client = cluster.new_client();
    client.put_kv(
        0..(DATA_COUNT - 100),
        i_to_key(keyspace_id, table_id),
        i_to_value(64),
    );
    cluster.request_major_compaction(
        MajorCompactionTarget::Table {
            keyspace_id,
            table_id,
        },
        true,
        Duration::from_secs(10),
    );
    let mut last_retry_time = Instant::now_coarse();
    TryWaiter::timeout(60)
        .interval_dur(Duration::from_millis(500))
        .must_wait(
            || {
                let Some(shard) = cluster.get_latest_shard_by_key(&table_key) else {
                    return false;
                };
                let stats = shard.get_stats();
                let compacted = !stats.write_cf_level_n_is_empty();
                if !compacted && last_retry_time.saturating_elapsed() >= Duration::from_secs(5) {
                    info!("retry major compaction, stats {:?}", stats);
                    cluster.request_major_compaction(
                        MajorCompactionTarget::Table {
                            keyspace_id,
                            table_id,
                        },
                        true,
                        Duration::from_secs(10),
                    );
                    last_retry_time = Instant::now_coarse();
                }
                compacted && shard.new_snap_access().is_sync() != enable_ia
            },
            || {
                let stats = cluster
                    .get_latest_shard_by_key(&table_key)
                    .map(|x| x.get_stats());
                format!("wait for compacted & async timeout: {:?}", stats)
            },
        );

    // Make mem-tables & L0 data.
    client.put_kv(
        (DATA_COUNT - 200)..(DATA_COUNT),
        i_to_key(keyspace_id, table_id),
        i_to_value(32),
    );

    let (existed, deleted) = client.verify_data_with_ref_store();
    assert_eq!(existed, DATA_COUNT);
    assert_eq!(deleted, 0);
    info!(
        "shard: {:?}",
        cluster
            .get_latest_shard_by_key(&table_key)
            .map(|x| x.get_stats())
    );
    (cluster, keyspace_id, table_id)
}

fn i_to_key(keyspace_id: u32, table_id: i64) -> impl Fn(usize) -> Vec<u8> {
    move |i| make_row_key(keyspace_id, table_id, format!("k{i:08}").as_bytes())
}

fn i_to_value(value_len: usize) -> impl Fn(usize) -> Vec<u8> {
    move |_| random_value(value_len)
}

fn make_put(key: Bytes, value: Bytes) -> Mutation {
    Mutation {
        op: kvrpcpb::Op::Put,
        key,
        value,
        ..Default::default()
    }
}

fn make_insert(key: Bytes, value: Bytes) -> Mutation {
    Mutation {
        op: kvrpcpb::Op::Insert,
        key,
        value,
        ..Default::default()
    }
}

async fn inc(tx: &mut Transaction<ApiV2NoPrefixCodec>, key: &[u8]) -> tikv_client::Result<()> {
    let v = tx.get_for_update(key.to_vec()).await?.unwrap();
    let v = u64::from_be_bytes(v.try_into().unwrap());
    tx.put(key.to_vec(), (v + 1).to_be_bytes().to_vec()).await?;
    Ok(())
}

fn is_write_conflict(err: &tikv_client::proto::kvrpcpb::KeyError) -> bool {
    err.conflict.is_some()
}

fn is_client_err_retryable(
    err: &tikv_client::Error,
    f: &dyn Fn(&tikv_client::proto::kvrpcpb::KeyError) -> bool,
) -> bool {
    match err {
        tikv_client::Error::KeyError(e) => f(e),
        tikv_client::Error::MultipleKeyErrors(errs) => {
            errs.iter().all(|e| is_client_err_retryable(e, f))
        }
        tikv_client::Error::PessimisticLockError { inner, .. } => is_client_err_retryable(inner, f),
        _ => false,
    }
}
