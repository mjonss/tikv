// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{assert_matches::assert_matches, ops::Range, sync::Arc, thread, time::Duration};

use anyhow::{self, bail};
use bytes::Bytes;
use cloud_encryption::KeyspaceEncryptionConfig;
use futures::{executor::block_on, future::join_all};
use kvengine::{
    dfs::{self, Dfs, FileType},
    table::{
        InnerKey,
        txn_file::{OP_PUT, TxnChunkBuilder},
    },
};
use kvproto::{
    kvrpcpb,
    kvrpcpb::{
        BatchRollbackRequest, CheckTxnStatusRequest, CommitRequest, PrewriteRequest,
        ResolveLockRequest, TxnHeartBeatRequest,
    },
    tikvpb::TikvClient,
};
use pd_client::PdClient;
use security::SecurityConfig;
use test_cloud_server::{
    ServerCluster, ServerClusterBuilder, TikvWorkerOptions, client,
    client::{
        ClusterClient, ClusterClientOptions, CommitAction, Error as ClientError, MutateOptions,
        RequestOptions, TxnMutations, TxnWriteMethod,
    },
    must_wait,
    oss::prepare_dfs,
    try_wait,
    txn::txn_file::TxnFileHelper,
    util::Mutation,
};
use test_pd_client::PdWrapper;
use tikv_util::{info, time::Instant, warn};

use crate::{alloc_node_id_vec, generate_keyspace_key, i_to_tidb_key, i_to_val, i_to_val_opt};

const NODES_COUNT: usize = 3;
const KEYSPACE_ID: u32 = 10;

#[test]
fn test_txn_file_commands() {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(alloc_node_id_vec(3), |_, _| {});
    cluster.wait_region_replicated(&[], 3);
    let dfs = cluster.get_dfs().unwrap();

    let mut client = cluster.new_client();
    let mut prepare_ctx = |key: &[u8]| -> (u64, kvrpcpb::Context, TikvClient) {
        let region_id = client.get_region_id(key);
        let ctx = client.new_rpc_ctx(region_id, key).unwrap();
        let kv_client = client.get_kv_client(ctx.get_peer().get_store_id());
        (region_id, ctx, kv_client)
    };

    let mut client = cluster.new_client();
    client.split_keyspace(KEYSPACE_ID);

    let gen_key = generate_keyspace_key(KEYSPACE_ID);
    let primary_lock = gen_key(0);
    cluster.wait_region_replicated(&primary_lock, 3);
    let (mut region_id, mut ctx, mut kv_client) = prepare_ctx(&primary_lock);

    // test rollback lock.
    let start_ts = client.get_ts().into_inner();
    let chunk_ids = build_txn_files(&dfs, start_ts, 0, 300);

    let mut req = PrewriteRequest::new();
    req.set_context(ctx.clone());
    req.set_txn_file_chunks(chunk_ids);
    req.set_primary_lock(primary_lock.clone());
    req.set_lock_ttl(6000);
    req.set_start_version(start_ts);
    req.set_min_commit_ts(start_ts + 1);
    let ok = try_wait(
        || {
            let resp = kv_client.kv_prewrite(&req).unwrap();
            assert!(resp.get_errors().is_empty(), "resp {:?}", resp);
            if resp.has_region_error() {
                warn!("prewrite got region error {:?}", resp.get_region_error());
                (region_id, ctx, kv_client) = prepare_ctx(&primary_lock);
                req.set_context(ctx.clone());
                false
            } else {
                true
            }
        },
        10,
    );
    assert!(ok);

    let mut req = CheckTxnStatusRequest::new();
    req.set_context(ctx.clone());
    req.set_primary_key(primary_lock.clone());
    req.set_lock_ts(start_ts);
    req.set_caller_start_ts(start_ts + 5);
    req.set_is_txn_file(true);
    let resp = kv_client.kv_check_txn_status(&req).unwrap();
    assert!(
        !resp.has_error() && !resp.has_region_error(),
        "resp {:?}",
        resp
    );
    let lock_info = resp.get_lock_info();
    // verify that min commit ts is pushed.
    assert_eq!(
        lock_info.min_commit_ts,
        start_ts + 6,
        "lock info {:?}",
        lock_info
    );

    let mut req = TxnHeartBeatRequest::new();
    req.set_context(ctx.clone());
    req.set_start_version(start_ts);
    req.set_primary_lock(primary_lock);
    req.set_advise_lock_ttl(10000);
    req.set_is_txn_file(true);
    let resp = kv_client.kv_txn_heart_beat(&req).unwrap();
    assert!(
        !resp.has_error() && !resp.has_region_error(),
        "resp {:?}",
        resp
    );
    // verify that lock ttl is pushed.
    assert_eq!(resp.lock_ttl, 10000);

    let mut req = BatchRollbackRequest::new();
    req.set_context(ctx.clone());
    req.set_is_txn_file(true);
    req.set_start_version(start_ts);
    let resp = kv_client.kv_batch_rollback(&req).unwrap();
    assert!(
        !resp.has_error() && !resp.has_region_error(),
        "resp {:?}",
        resp
    );

    // test rollback lock.
    let start_ts = client.get_ts().into_inner();
    let chunk_ids = build_txn_files(&dfs, start_ts, 0, 300);
    let primary_lock = gen_key(0);

    let mut req = PrewriteRequest::new();
    req.set_context(ctx.clone());
    req.set_txn_file_chunks(chunk_ids);
    req.set_primary_lock(primary_lock);
    req.set_lock_ttl(6000);
    req.set_start_version(start_ts);
    req.set_min_commit_ts(start_ts + 1);
    let resp = kv_client.kv_prewrite(&req).unwrap();
    assert!(
        resp.get_errors().is_empty() && !resp.has_region_error(),
        "resp {:?}",
        resp
    );

    let mut req = BatchRollbackRequest::new();
    req.set_context(ctx.clone());
    req.set_is_txn_file(true);
    req.set_start_version(start_ts);
    let resp = kv_client.kv_batch_rollback(&req).unwrap();
    assert!(
        !resp.has_error() && !resp.has_region_error(),
        "resp {:?}",
        resp
    );

    // test resolve lock.
    let start_ts = client.get_ts().into_inner();
    let chunk_ids = build_txn_files(&dfs, start_ts, 0, 300);
    let primary_lock = gen_key(0);

    let mut req = PrewriteRequest::new();
    req.set_context(ctx.clone());
    req.set_txn_file_chunks(chunk_ids);
    req.set_primary_lock(primary_lock);
    req.set_lock_ttl(6000);
    req.set_start_version(start_ts);
    req.set_min_commit_ts(start_ts + 1);
    let resp = kv_client.kv_prewrite(&req).unwrap();
    assert!(
        resp.get_errors().is_empty() && !resp.has_region_error(),
        "resp {:?}",
        resp
    );

    let mut req = ResolveLockRequest::new();
    req.set_context(ctx.clone());
    req.set_start_version(start_ts);
    req.set_is_txn_file(true);
    let resp = kv_client.kv_resolve_lock(&req).unwrap();
    assert!(
        !resp.has_error() && !resp.has_region_error(),
        "resp {:?}",
        resp
    );

    // test resolve lock (batch).
    let start_ts = client.get_ts().into_inner();
    let chunk_ids = build_txn_files(&dfs, start_ts, 0, 300);
    let primary_lock = gen_key(0);

    let mut req = PrewriteRequest::new();
    req.set_context(ctx.clone());
    req.set_txn_file_chunks(chunk_ids);
    req.set_primary_lock(primary_lock);
    req.set_lock_ttl(6000);
    req.set_start_version(start_ts);
    req.set_min_commit_ts(start_ts + 1);
    let resp = kv_client.kv_prewrite(&req).unwrap();
    assert!(
        resp.get_errors().is_empty() && !resp.has_region_error(),
        "resp {:?}",
        resp
    );
    let txn_info = kvrpcpb::TxnInfo {
        txn: start_ts,
        status: 0,
        is_txn_file: true,
        ..Default::default()
    };
    let mut req = ResolveLockRequest::new();
    req.set_context(ctx.clone());
    req.set_txn_infos(vec![txn_info].into());
    let resp = kv_client.kv_resolve_lock(&req).unwrap();
    assert!(
        !resp.has_error() && !resp.has_region_error(),
        "resp {:?}",
        resp
    );

    // test success 2pc.
    let start_ts = client.get_ts().into_inner();
    let chunk_ids = build_txn_files(&dfs, start_ts, 0, 300);
    let primary_lock = gen_key(0);

    let mut req = PrewriteRequest::new();
    req.set_context(ctx.clone());
    req.set_txn_file_chunks(chunk_ids);
    req.set_primary_lock(primary_lock);
    req.set_lock_ttl(6000);
    req.set_start_version(start_ts);
    req.set_min_commit_ts(start_ts + 1);
    let resp = kv_client.kv_prewrite(&req).unwrap();
    assert!(
        resp.get_errors().is_empty() && !resp.has_region_error(),
        "resp {:?}",
        resp
    );

    let mut req = CommitRequest::new();
    req.set_context(ctx);
    req.set_start_version(start_ts);
    req.set_is_txn_file(true);
    let commit_ts = client.get_ts().into_inner();
    req.set_commit_version(commit_ts);
    let resp = kv_client.kv_commit(&req).unwrap();
    assert!(
        !resp.has_error() && !resp.has_region_error(),
        "resp {:?}",
        resp
    );

    verify_range(&mut client, 0, 300, commit_ts);
    cluster.stop();
}

#[test]
fn test_txn_file_basic() {
    test_util::init_log_for_test();

    let cases = vec![
        // size_factor, write_method, enable_encryption
        (10, TxnWriteMethod::FileBased, false),
        (10, TxnWriteMethod::FileBased, true),
        // Regression tests:
        (1, TxnWriteMethod::FileBased, false),
        (1, TxnWriteMethod::Normal, false),
    ];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name("test-txn-file-basic")
        .worker_threads(cases.len())
        .enable_all()
        .build()
        .unwrap();
    let mut handles = Vec::with_capacity(cases.len());
    for (size_factor, txn_write_method, enable_encryption) in cases {
        handles.push(rt.spawn_blocking(move || {
            test_txn_file_basic_impl(size_factor, txn_write_method, enable_encryption)
        }));
    }
    rt.block_on(join_all(handles));
}

fn test_txn_file_basic_impl(
    size_factor: usize,
    write_method: TxnWriteMethod,
    enable_encryption: bool,
) {
    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test");

    let node_ids = alloc_node_id_vec(NODES_COUNT);
    let pd_wrapper = PdWrapper::new_test(1, &SecurityConfig::default(), None);
    let cluster_id = pd_wrapper.client().get_cluster_id().unwrap();
    info!("test_txn_file_basic";
        "size_factor" => size_factor,
        "enable_encryption" => enable_encryption,
        "write_method" => ?write_method,
        "cluster_id" => cluster_id);
    let mut cluster = ServerClusterBuilder::new(node_ids, |_, conf| {
        conf.dfs = dfs_config.clone();
    })
    .pd(pd_wrapper)
    .build();
    cluster.start_tikv_workers(alloc_node_id_vec(1), TikvWorkerOptions::default());
    cluster.wait_region_replicated(&[], 3);

    let gen_key = generate_keyspace_key(KEYSPACE_ID);

    let mut verify_data = {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut client = cluster.new_client();
        let mut txn_client = rt.block_on(cluster.new_txn_client());

        move |range: Option<(&[u8], &[u8])>, expected: usize| -> anyhow::Result<()> {
            let guard = client.ref_store();
            let ref_store = guard.lock().unwrap();
            let cnt = rt
                .block_on(txn_client.verify_data_by_scan(&ref_store, range))
                .unwrap();
            if cnt != expected {
                bail!("txn client verify data failed, expect {expected}, got {cnt}");
            }
            let (cnt, _) = client
                .verify_data_with_given_ref_store(&ref_store, range, &RequestOptions::default())
                .unwrap();
            if cnt != expected {
                bail!("client verify data failed, expect {expected}, got {cnt}");
            }
            Ok(())
        }
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let _enter = rt.enter();

    if enable_encryption {
        let cfg = KeyspaceEncryptionConfig { enabled: true };
        cluster
            .get_pd_client()
            .set_keyspace_encryption(KEYSPACE_ID, cfg)
            .unwrap();
    }

    let mut client = cluster.new_client_opt(ClusterClientOptions {
        txn_file_max_chunk_size: Some(1024),
        ..Default::default()
    });

    client.split_keyspace(KEYSPACE_ID);

    {
        // To work around that data not existed in ref store will not be checked.
        let guard = client.ref_store();
        let mut ref_store = guard.lock().unwrap();
        for k in 0..20 * size_factor {
            ref_store.del_kv(gen_key(k));
        }
    }

    {
        client
            .try_put_kv(
                0..10 * size_factor,
                &gen_key,
                i_to_val_opt("value0_", 3),
                MutateOptions {
                    commit_action: CommitAction::AsyncCommitSecondaryKeys(Duration::ZERO),
                    write_method,
                    ..Default::default()
                },
            )
            .unwrap();
        verify_data(None, 10 * size_factor).unwrap();
    }

    {
        // Verify locks of committed txn.
        client
            .try_put_kv(
                size_factor..11 * size_factor,
                &gen_key,
                i_to_val_opt("value1_", 3),
                MutateOptions {
                    commit_action: CommitAction::AsyncCommitSecondaryKeys(Duration::MAX),
                    write_method,
                    ..Default::default()
                },
            )
            .unwrap();
        // Verify on non-primary txn file when there are more than one.
        verify_data(
            Some((&gen_key(6 * size_factor), &gen_key(11 * size_factor))),
            5 * size_factor,
        )
        .unwrap();
    }

    {
        // Verify rollback & resolve locks.
        client
            .try_put_kv(
                2 * size_factor..12 * size_factor,
                &gen_key,
                i_to_val_opt("value2_", 3),
                MutateOptions {
                    commit_action: CommitAction::NoCommit,
                    write_method,
                    ..Default::default()
                },
            )
            .unwrap();
        let start = Instant::now_coarse();
        // Verify on non-primary txn file when there are more than one.
        verify_data(
            Some((&gen_key(7 * size_factor), &gen_key(12 * size_factor))),
            4 * size_factor,
        )
        .unwrap();
        // Wait until locks expired.
        // Note that txn client will not wait lock timeout during scan because of min
        // commit ts pushed.
        let elapsed = start.saturating_elapsed();
        assert!(elapsed > Duration::from_secs(2), "elapsed {:?}", elapsed);
    }

    verify_data(None, 11 * size_factor).unwrap();
    cluster.stop();
    oss.shutdown();
}

#[test]
fn test_txn_file_split_merge() {
    test_util::init_log_for_test();
    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test");

    let node_ids = alloc_node_id_vec(NODES_COUNT);
    let pd_wrapper = PdWrapper::new_test(1, &SecurityConfig::default(), None);
    let mut cluster = ServerClusterBuilder::new(node_ids, |_, conf| {
        conf.dfs = dfs_config.clone();
    })
    .pd(pd_wrapper)
    .build();
    cluster.start_tikv_workers(alloc_node_id_vec(1), TikvWorkerOptions::default());
    cluster.wait_region_replicated(&[], 3);

    let gen_key = generate_keyspace_key(KEYSPACE_ID);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let _enter = rt.enter();

    let mut client = cluster.new_client_opt(ClusterClientOptions {
        txn_file_max_chunk_size: Some(1024),
        ..Default::default()
    });
    client.split_keyspace(KEYSPACE_ID);
    client.try_split(&gen_key(100), 5).unwrap();
    {
        // To work around that data not existed in ref store will not be checked.
        let guard = client.ref_store();
        let mut ref_store = guard.lock().unwrap();
        for k in 0..200 {
            ref_store.del_kv(gen_key(k));
        }
    }

    // Put the locks:
    client
        .try_put_kv(
            100..200,
            &gen_key,
            i_to_val_opt("value0_", 3),
            MutateOptions {
                commit_action: CommitAction::NoCommit,
                write_method: TxnWriteMethod::FileBased,
                ..Default::default()
            },
        )
        .unwrap();
    client
        .try_put_kv(
            0..100,
            &gen_key,
            i_to_val_opt("value1_", 3),
            MutateOptions {
                commit_action: CommitAction::NoCommit,
                write_method: TxnWriteMethod::FileBased,
                ..Default::default()
            },
        )
        .unwrap();

    // Fail to split:
    let err = client.try_split(&gen_key(20), 1).unwrap_err();
    match err {
        client::Error::KeyErrors(key_errs) => {
            assert_eq!(key_errs.len(), 1);
            let key_err = &key_errs[0];
            assert!(key_err.has_locked());
            let lock_info = key_err.get_locked();
            assert_eq!(lock_info.key, gen_key(20));
            assert_eq!(lock_info.primary_lock, gen_key(0));
        }
        _ => {
            panic!("unexpected error: {:?}", err);
        }
    }
    // Another split path. Note this split request would succeed after locks are
    // cleanup.
    client.try_split_by_pd(&gen_key(50), 1).unwrap_err();

    // Clean up the lock:
    info!("cleanup the locks (0..100");
    client
        .try_put_kv(
            0..100,
            &gen_key,
            i_to_val_opt("value2_", 3),
            MutateOptions {
                commit_action: CommitAction::SyncCommit,
                write_method: TxnWriteMethod::FileBased,
                ..Default::default()
            },
        )
        .unwrap();
    // Can split:
    let ok = try_wait(
        || {
            info!("split on {:?}", gen_key(20));
            match client.try_split(&gen_key(20), 5) {
                Ok(_) => true,
                Err(client::Error::KeyErrors(key_errs)) => {
                    info!("try split encounter locks, try again"; "key_errs" => ?key_errs);
                    false
                }
                Err(err) => panic!("split failed: {:?}", err),
            }
        },
        5,
    );
    assert!(ok, "split key 20 failed");
    client
        .try_split(&gen_key(80), 5)
        .expect("split key 80 failed");

    // Clean up the lock:
    info!("cleanup the locks (100..200");
    // Note: verify data will not clean up locks when locks are not expired.
    client.verify_data_with_ref_store();
    // Can split:
    let ok = try_wait(
        || {
            info!("split on {:?}", gen_key(150));
            match client.try_split(&gen_key(150), 5) {
                Ok(_) => true,
                Err(client::Error::KeyErrors(key_errs)) => {
                    info!("try split encounter locks, try again"; "key_errs" => ?key_errs);
                    false
                }
                Err(err) => panic!("split failed: {:?}", err),
            }
        },
        5,
    );
    assert!(ok, "split key 150 failed");

    // Current regions in [, 100):
    // [, 20), [20, 80), [80, 100), or [,20), [20,50), [50,80), [80, 100)

    // Write the lock to the middle region(s):
    client
        .try_put_kv(
            40..60,
            &gen_key,
            i_to_val_opt("value3_", 3),
            MutateOptions {
                commit_action: CommitAction::NoCommit,
                write_method: TxnWriteMethod::FileBased,
                ..Default::default()
            },
        )
        .unwrap();

    // Fail to merge source region has locks.
    let ok = client.try_merge_and_wait(&gen_key(50), &gen_key(100), 1);
    assert!(!ok, "source region having locks merged");

    // Succeed to merge when target region has locks
    // [, 80), [80, 100), or [,50), [50,80), [80, 100)
    let ok = client.try_merge_and_wait(&gen_key(0), &gen_key(45), 5);
    assert!(ok, "source region having no lock not merged");

    // Clean up locks:
    client
        .try_put_kv(
            40..60,
            &gen_key,
            i_to_val_opt("value4_", 3),
            MutateOptions {
                commit_action: CommitAction::SyncCommit,
                write_method: TxnWriteMethod::FileBased,
                ..Default::default()
            },
        )
        .unwrap();

    // Succeed to merge:
    let ok = client.try_merge_and_wait(&gen_key(50), &gen_key(80), 5);
    assert!(ok, "region having no lock not merged");

    // Verify:
    client.verify_data_with_ref_store();
    cluster.stop();
    oss.shutdown();
}

// Tests for abnormal processes.
#[test]
fn test_txn_file_abnormal() {
    test_util::init_log_for_test();

    let cases = vec![
        (10, true), // data_count, use_txn_file
        (100, true),
        // To verify that the behavior is the same as normal txn.
        (100, false),
    ];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name("test-txn-file-abnormal")
        .worker_threads(cases.len())
        .enable_all()
        .build()
        .unwrap();
    let handles = cases
        .into_iter()
        .map(|(data_count, use_txn_file)| {
            rt.spawn_blocking(move || test_txn_file_abnormal_impl(data_count, use_txn_file))
        })
        .collect::<Vec<_>>();
    rt.block_on(join_all(handles));
}

fn test_txn_file_abnormal_impl(data_count: usize, use_txn_file: bool) {
    let write_method = if use_txn_file {
        TxnWriteMethod::FileBased
    } else {
        TxnWriteMethod::Normal
    };
    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test");

    let node_ids = alloc_node_id_vec(NODES_COUNT);
    let pd_wrapper = PdWrapper::new_test(1, &SecurityConfig::default(), None);
    let cluster_id = pd_wrapper.client().get_cluster_id().unwrap();
    info!("test_txn_file_abnormal_process"; "data_count" => data_count, "write_method" => ?write_method, "cluster_id" => cluster_id);
    let mut cluster = ServerClusterBuilder::new(node_ids, |_, conf| {
        conf.dfs = dfs_config.clone();
    })
    .pd(pd_wrapper)
    .build();
    cluster.start_tikv_workers(alloc_node_id_vec(1), TikvWorkerOptions::default());
    cluster.wait_region_replicated(&[], 3);

    let gen_key = generate_keyspace_key(KEYSPACE_ID);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name("test-abnormal-process")
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let _enter = rt.enter();

    let mut client = cluster.new_client_opt(ClusterClientOptions {
        txn_file_max_chunk_size: Some(1024),
        ..Default::default()
    });
    client.split_keyspace(KEYSPACE_ID);
    client.try_split(&gen_key(data_count / 2), 5).unwrap();
    {
        // To work around that data not existed in ref store will not be checked.
        let guard = client.ref_store();
        let mut ref_store = guard.lock().unwrap();
        for k in 0..data_count {
            ref_store.del_kv(gen_key(k));
        }
    }

    let txn_file_helper = client.txn_file_helper();
    let make_mutations_impl = |value, insert: bool| {
        let op_fn = if insert {
            |_: usize| kvrpcpb::Op::Insert
        } else {
            |_: usize| kvrpcpb::Op::Put
        };
        make_txn_mutations(
            value,
            0,
            data_count,
            &gen_key,
            op_fn,
            write_method,
            txn_file_helper.clone(),
        )
    };
    let make_mutations = |value| make_mutations_impl(value, false);
    let make_insert_mutations = |value| make_mutations_impl(value, true);

    // Already exist.
    {
        let (muts0, txn_muts0) = make_insert_mutations("already_exist");
        let start_ts0 = client.get_ts();
        client
            .kv_prewrite(txn_muts0.primary(), None, txn_muts0.clone(), start_ts0)
            .unwrap();
        let commit_ts0 = client.get_ts();
        client
            .kv_commit(txn_muts0.clone(), start_ts0, commit_ts0, false)
            .unwrap();
        client.put_kv_in_ref_store(muts0);
        client.verify_data_with_ref_store();

        let (_, txn_muts1) = make_insert_mutations("already_exist_1");
        let start_ts1 = client.get_ts();
        let err = client
            .kv_prewrite(txn_muts1.primary(), None, txn_muts1, start_ts1)
            .unwrap_err();
        assert_matches!(err, ClientError::AlreadyExist(_));
        client.verify_data_with_ref_store();
    }

    // Commit without prewrite:
    {
        let (_, txn_muts) = make_mutations("with_prewrite");
        let start_ts = client.get_ts();
        let commit_ts = client.get_ts();
        let err = client
            .kv_commit(txn_muts, start_ts, commit_ts, false)
            .unwrap_err();
        expect_err_msg(&err, "TxnLockNotFound");
        client.verify_data_with_ref_store();
    }

    // Commit after rollback.
    {
        let (_, txn_muts0) = make_mutations("after_rollback");
        let start_ts0 = client.get_ts();
        client
            .kv_prewrite(txn_muts0.primary(), None, txn_muts0.clone(), start_ts0)
            .unwrap();

        let (muts1, txn_muts1) = make_mutations("after_rollback_1");
        let start_ts1 = client.get_ts();
        // Will rollback txn0.
        client
            .kv_prewrite(txn_muts1.primary(), None, txn_muts1.clone(), start_ts1)
            .unwrap();

        let commit_ts0 = client.get_ts();
        let err = client
            .kv_commit(txn_muts0, start_ts0, commit_ts0, false)
            .unwrap_err();
        expect_err_msg(&err, "TxnLockNotFound");

        let commit_ts1 = client.get_ts();
        client
            .kv_commit(txn_muts1, start_ts1, commit_ts1, false)
            .unwrap();
        client.put_kv_in_ref_store(muts1);
        client.verify_data_with_ref_store();
    }

    // Write conflict & duplicated commit.
    {
        let (muts0, txn_muts0) = make_mutations("write_conflict");
        let start_ts0 = client.get_ts();
        client
            .kv_prewrite(txn_muts0.primary(), None, txn_muts0.clone(), start_ts0)
            .unwrap();

        let commit_ts0 = client.get_ts();
        let txn_muts0_clone = txn_muts0.clone();
        let mut client_clone = client.clone();
        let txn0 = rt.spawn_blocking(move || {
            thread::sleep(Duration::from_secs(1));
            let new_commit_ts = client_clone
                .kv_commit(txn_muts0_clone.clone(), start_ts0, commit_ts0, false)
                .unwrap();
            assert!(
                new_commit_ts > commit_ts0,
                "commit_ts0: {}, new_commit_ts: {}",
                commit_ts0,
                new_commit_ts
            );
            new_commit_ts
        });

        let (_, txn_muts1) = make_mutations("write_conflict_1");
        let start_ts1 = client.get_ts();
        let err = client
            .kv_prewrite(txn_muts1.primary(), None, txn_muts1, start_ts1)
            .unwrap_err();
        assert_matches!(err, ClientError::WriteConflict(_));

        let _new_commit_ts0 = rt.block_on(txn0).unwrap();
        client.put_kv_in_ref_store(muts0);
        client.verify_data_with_ref_store();

        // Duplicated commit.
        // Note: commit for secondary keys would fail for TxnLockNotFound error, when
        // there is no key in the region.
        let dup_commit_ts = client
            .kv_commit_ext(
                txn_muts0,
                start_ts0,
                commit_ts0,
                CommitAction::AsyncCommitSecondaryKeys(Duration::ZERO),
            )
            .unwrap();
        // Will return the commit_ts in request other than of committed record.
        assert_eq!(dup_commit_ts, commit_ts0);
        client.verify_data_with_ref_store();
    }

    // Rollback
    {
        let (muts, txn_muts) = make_mutations("rollback");

        // Rollback a not existed txn.
        let mut start_ts = client.get_ts();
        client.kv_rollback(txn_muts.clone(), start_ts).unwrap();
        if !use_txn_file {
            // Normal txn will write rollback record even the txn is not exist.
            // But file based txn can not do so.
            let err = client
                .kv_prewrite(txn_muts.primary(), None, txn_muts.clone(), start_ts)
                .unwrap_err();
            assert_matches!(err, ClientError::WriteConflict(_));
            start_ts = client.get_ts();
        }
        client
            .kv_prewrite(txn_muts.primary(), None, txn_muts.clone(), start_ts)
            .unwrap();
        client.kv_rollback(txn_muts.clone(), start_ts).unwrap();

        // Duplicated rollback.
        client.kv_rollback(txn_muts.clone(), start_ts).unwrap();

        // Prewrite after rollback.
        let err = client
            .kv_prewrite(txn_muts.primary(), None, txn_muts.clone(), start_ts)
            .unwrap_err();
        assert_matches!(err, ClientError::WriteConflict(_));

        // Commit after rollback.
        let commit_ts = client.get_ts();
        let err = client
            .kv_commit(txn_muts.clone(), start_ts, commit_ts, false)
            .unwrap_err();
        expect_err_msg(&err, "TxnLockNotFound");

        client.verify_data_with_ref_store();

        // Committed.
        let start_ts = client.get_ts();
        let commit_ts = client.get_ts();
        client
            .kv_prewrite(txn_muts.primary(), None, txn_muts.clone(), start_ts)
            .unwrap();
        client
            .kv_commit(txn_muts.clone(), start_ts, commit_ts, false)
            .unwrap();
        // Rollback after committed.
        let err = client.kv_rollback(txn_muts, start_ts).unwrap_err();
        expect_err_msg(&err, "Committed");

        client.put_kv_in_ref_store(muts);
        client.verify_data_with_ref_store();
    }

    // Resolve lock
    {
        let (muts, txn_muts) = make_mutations("resolve_lock");

        // Resolve a not existed txn.
        let mut start_ts = client.get_ts();
        client
            .kv_resolve_lock_for_mutations(start_ts.into_inner(), None, muts.clone(), use_txn_file)
            .unwrap();
        if !use_txn_file {
            // Normal txn will write rollback record even the txn is not exist.
            // But file based txn can not do so.
            let err = client
                .kv_prewrite(txn_muts.primary(), None, txn_muts.clone(), start_ts)
                .unwrap_err();
            assert_matches!(err, ClientError::WriteConflict(_));
            start_ts = client.get_ts();
        }
        client
            .kv_prewrite(txn_muts.primary(), None, txn_muts.clone(), start_ts)
            .unwrap();
        // Rollback the not committed txn.
        client
            .kv_resolve_lock_for_mutations(start_ts.into_inner(), None, muts.clone(), use_txn_file)
            .unwrap();

        // Duplicated resolve lock.
        client
            .kv_resolve_lock_for_mutations(start_ts.into_inner(), None, muts.clone(), use_txn_file)
            .unwrap();

        // Prewrite after rollback.
        let err = client
            .kv_prewrite(txn_muts.primary(), None, txn_muts.clone(), start_ts)
            .unwrap_err();
        assert_matches!(err, ClientError::WriteConflict(_));

        // Commit after rollback.
        let commit_ts = client.get_ts();
        let err = client
            .kv_commit(txn_muts.clone(), start_ts, commit_ts, false)
            .unwrap_err();
        expect_err_msg(&err, "TxnLockNotFound");

        client.verify_data_with_ref_store();

        // Committed.
        let start_ts = client.get_ts();
        let commit_ts = client.get_ts();
        client
            .kv_prewrite(txn_muts.primary(), None, txn_muts.clone(), start_ts)
            .unwrap();
        client
            .kv_commit(txn_muts, start_ts, commit_ts, false)
            .unwrap();
        // Rollback after committed.
        let err = client
            .kv_resolve_lock_for_mutations(start_ts.into_inner(), None, muts.clone(), use_txn_file)
            .unwrap_err();
        expect_err_msg(&err, "Committed");

        // Duplicated commit.
        client
            .kv_resolve_lock_for_mutations(
                start_ts.into_inner(),
                Some(commit_ts.into_inner()),
                muts.clone(),
                use_txn_file,
            )
            .unwrap();
        let commit_ts = client.get_ts();
        client
            .kv_resolve_lock_for_mutations(
                start_ts.into_inner(),
                Some(commit_ts.into_inner()),
                muts.clone(),
                use_txn_file,
            )
            .unwrap();

        client.put_kv_in_ref_store(muts);
        client.verify_data_with_ref_store();
    }

    // Check txn status
    {
        let (muts, txn_muts) = make_mutations("check_txn_status");
        let start_ts = client.get_ts();
        client
            .kv_prewrite(txn_muts.primary(), None, txn_muts.clone(), start_ts)
            .unwrap();
        {
            // primary mismatch.
            let second_key = muts[1].key.clone();
            let resp = client
                .kv_check_txn_status(
                    &second_key,
                    start_ts,
                    start_ts,
                    start_ts,
                    false,
                    false,
                    false,
                    use_txn_file,
                )
                .unwrap();
            assert!(resp.has_error());
            assert!(resp.get_error().has_primary_mismatch());
        }

        {
            let resp = client
                .kv_check_txn_status(
                    &txn_muts.primary(),
                    start_ts,
                    start_ts,
                    start_ts,
                    false,
                    false,
                    false,
                    use_txn_file,
                )
                .unwrap();
            assert!(!resp.has_error());
            assert_eq!(resp.commit_version, 0);
            assert_eq!(resp.get_lock_info().lock_version, start_ts.into_inner());
            assert_eq!(
                resp.get_lock_info().get_primary_lock(),
                txn_muts.primary().as_ref()
            );
        }
    }

    client.verify_data_with_ref_store();
    cluster.stop();
    oss.shutdown();
}

#[test]
fn test_txn_file_move_down() {
    test_util::init_log_for_test();
    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test");
    let node_ids = alloc_node_id_vec(NODES_COUNT);
    let first_node = node_ids[0];
    let pd_wrapper = PdWrapper::new_test(1, &SecurityConfig::default(), None);
    let cluster_id = pd_wrapper.client().get_cluster_id().unwrap();
    info!("test_txn_file_move_down"; "cluster_id" => cluster_id);
    let mut cluster = ServerClusterBuilder::new(node_ids, |_, conf| {
        conf.dfs = dfs_config.clone();
        conf.kvengine.flush_split_l0 = true;
    })
    .pd(pd_wrapper)
    .build();
    cluster.start_tikv_workers(alloc_node_id_vec(1), TikvWorkerOptions::default());
    cluster.wait_region_replicated(&[], 3);

    let gen_key = generate_keyspace_key(KEYSPACE_ID);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let _enter = rt.enter();

    let mut client = cluster.new_client_opt(ClusterClientOptions {
        txn_file_max_chunk_size: Some(256 * 1024),
        ..Default::default()
    });

    client.split_keyspace(KEYSPACE_ID);
    let num_keys = 10000;
    {
        client
            .try_put_kv(
                0..num_keys,
                &gen_key,
                i_to_val_opt("value0_", 3),
                MutateOptions {
                    commit_action: CommitAction::AsyncCommitSecondaryKeys(Duration::ZERO),
                    write_method: TxnWriteMethod::FileBased,
                    ..Default::default()
                },
            )
            .unwrap();
    }
    must_wait(
        || {
            let kvengine = cluster.get_kvengine(first_node);
            let stats = kvengine.get_all_shard_stats();
            stats
                .iter()
                .all(|s| s.l0_table_count == 0 && s.mem_table_size == 0)
        },
        5,
        || "wait for compaction".to_string(),
    );
    client.verify_data_with_ref_store();
    cluster.stop();
    oss.shutdown();
}

#[test]
fn test_txn_file_merge() {
    test_util::init_log_for_test();

    let cases = vec![
        vec![1..2, 3..5, 0..1, 2..3],
        vec![1..3, 2..4, 3..5, 0..2],
        vec![0..1, 1..2, 2..3, 3..4, 4..5],
        vec![0..2, 2..5],
        vec![0..5; 3],
    ];

    for ranges in cases {
        test_txn_file_merge_impl(ranges);
    }
}

fn test_txn_file_merge_impl(ranges: Vec<Range<usize>>) {
    let mut cluster = ServerCluster::new(alloc_node_id_vec(3), |_, conf| {
        conf.storage.scheduler_worker_pool_size = 8;
        conf.kvengine.txn_file_worker_pool_size = Some(16);
    });
    let dfs = cluster.get_dfs().unwrap();
    cluster.wait_region_replicated(&[], 3);

    let prepare_ctx =
        |client: &mut ClusterClient, key: &[u8]| -> (u64, kvrpcpb::Context, TikvClient) {
            let region_id = client.get_region_id(key);
            let ctx = client.new_rpc_ctx(region_id, key).unwrap();
            let kv_client = client.get_kv_client(ctx.get_peer().get_store_id());
            (region_id, ctx, kv_client)
        };

    let mut client = cluster.new_client();
    client.split_keyspace(KEYSPACE_ID);

    let start_ts = client.get_ts().into_inner();
    let chunk_ids = build_txn_files(&dfs, start_ts, 0, 500);
    assert_eq!(chunk_ids.len(), 5);

    let gen_key = generate_keyspace_key(KEYSPACE_ID);
    let primary_lock = gen_key(0);

    // Note: the primary key should be prewrite first. But it's OK here, as long as
    // there is no other command on primary.
    let mut handles = Vec::with_capacity(ranges.len());
    for range in ranges {
        let mut client = cluster.new_client();
        let sub_chunk_ids = chunk_ids[range].to_vec();
        let primary_lock = primary_lock.clone();
        let handle = std::thread::spawn(move || {
            let (mut region_id, mut ctx, mut kv_client) = prepare_ctx(&mut client, &primary_lock);

            let mut req = PrewriteRequest::new();
            req.set_context(ctx.clone());
            req.set_txn_file_chunks(sub_chunk_ids);
            req.set_primary_lock(primary_lock.clone());
            req.set_lock_ttl(6000);
            req.set_start_version(start_ts);
            req.set_min_commit_ts(start_ts + 1);

            let ok = try_wait(
                || {
                    let resp = kv_client.kv_prewrite(&req).unwrap();
                    assert!(resp.get_errors().is_empty(), "resp {:?}", resp);
                    if resp.has_region_error() {
                        warn!("prewrite got region error {:?}", resp.get_region_error());
                        (region_id, ctx, kv_client) = prepare_ctx(&mut client, &primary_lock);
                        req.set_context(ctx.clone());
                        false
                    } else {
                        true
                    }
                },
                10,
            );
            assert!(ok);
        });
        handles.push(handle);
    }
    for h in handles {
        h.join().unwrap();
    }

    let (_, ctx, kv_client) = prepare_ctx(&mut client, &primary_lock);
    let mut req = CommitRequest::new();
    req.set_context(ctx);
    req.set_start_version(start_ts);
    req.set_is_txn_file(true);
    let commit_ts = client.get_ts().into_inner();
    req.set_commit_version(commit_ts);
    let resp = kv_client.kv_commit(&req).unwrap();
    assert!(
        !resp.has_error() && !resp.has_region_error(),
        "resp {:?}",
        resp
    );

    verify_range(&mut client, 0, 500, commit_ts);
    cluster.stop();
}

// Test for commit "not primary" lock to primary region.
// See https://github.com/tidbcloud/cloud-storage-engine/issues/1800.
#[test]
fn test_commit_primary_region() {
    test_util::init_log_for_test();
    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test");

    let node_ids = alloc_node_id_vec(NODES_COUNT);
    let pd_wrapper = PdWrapper::new_test(1, &SecurityConfig::default(), None);
    let mut cluster = ServerClusterBuilder::new(node_ids, |_, conf| {
        conf.dfs = dfs_config.clone();
    })
    .pd(pd_wrapper)
    .build();
    cluster.start_tikv_workers(alloc_node_id_vec(1), TikvWorkerOptions::default());
    cluster.wait_region_replicated(&[], 3);

    let gen_key = generate_keyspace_key(KEYSPACE_ID);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let _enter = rt.enter();

    let mut client = cluster.new_client_opt(ClusterClientOptions {
        txn_file_max_chunk_size: Some(1024),
        ..Default::default()
    });
    let txn_file_helper = client.txn_file_helper();
    let make_mutations = |value, start, end| {
        make_txn_mutations(
            value,
            start,
            end,
            &gen_key,
            |_| kvrpcpb::Op::Put,
            TxnWriteMethod::FileBased,
            txn_file_helper.clone(),
        )
    };
    client.split_keyspace(KEYSPACE_ID);

    // Prewrite to two regions.
    client.try_split(&gen_key(100), 5).unwrap();
    let (_, txn_muts) = make_mutations("value_", 0, 200);
    let start_ts = client.get_ts();
    client
        .kv_prewrite(txn_muts.primary(), None, txn_muts.clone(), start_ts)
        .unwrap();

    // Rollback the primary region.
    let (_, txn_muts0) = make_mutations("value0_", 0, 1);
    client.kv_rollback(txn_muts0, start_ts).unwrap();
    let ok = client.try_merge_and_wait(&gen_key(0), &gen_key(100), 5);
    assert!(ok);

    // Commit must fail.
    let commit_ts = client.get_ts();
    let err = client
        .kv_commit(txn_muts, start_ts, commit_ts, false)
        .unwrap_err();
    expect_err_msg(&err, "TxnLockNotFound");

    {
        // To work around that data not existed in ref store will not be checked.
        let guard = client.ref_store();
        let mut ref_store = guard.lock().unwrap();
        for k in 0..200 {
            ref_store.del_kv(gen_key(k));
        }
    }
    client.verify_data_with_ref_store();
    cluster.stop();
    oss.shutdown();
}

fn make_txn_mutations<KeyF, OpF>(
    value: &str,
    start: usize,
    end: usize,
    gen_key: KeyF,
    gen_op: OpF,
    write_method: TxnWriteMethod,
    txn_file_helper: Option<Arc<TxnFileHelper>>,
) -> (Vec<Mutation>, TxnMutations)
where
    KeyF: Fn(usize) -> Vec<u8>,
    OpF: Fn(usize) -> kvrpcpb::Op,
{
    let gen_val = i_to_val_opt(value, 3);
    let mut mutations = vec![];
    for i in start..end {
        let mut m = Mutation::default();
        m.set_op(gen_op(i));
        m.set_key(gen_key(i));
        m.set_value(gen_val(i));
        mutations.push(m);
    }
    let txn_muts = block_on(TxnMutations::build(
        mutations.clone(),
        write_method,
        txn_file_helper,
    ))
    .unwrap();
    (mutations, txn_muts)
}

fn build_txn_files(dfs: &Arc<dyn Dfs>, start_ts: u64, start: usize, end: usize) -> Vec<u64> {
    let mut chunk_ids = vec![];
    let mut chunk_id = start_ts + 1;
    let mut txn_chunk_builder = TxnChunkBuilder::new(chunk_id, 64, None);
    for i in start..end {
        let key = i_to_tidb_key(i);
        let val = i_to_val(i);
        txn_chunk_builder.add_entry(InnerKey::from_outer_key(&key), OP_PUT, &val);
        if (i + 1) % 100 == 0 {
            let mut data_buf = vec![];
            txn_chunk_builder.finish(&mut data_buf);
            txn_chunk_builder = TxnChunkBuilder::new(chunk_id, 64, None);
            let opts = dfs::Options::default().with_type(FileType::TxnChunk);
            dfs.get_runtime()
                .block_on(dfs.create(chunk_id, Bytes::from(data_buf), opts))
                .unwrap();
            chunk_ids.push(chunk_id);
            chunk_id += 1;
        }
    }
    chunk_ids
}

fn verify_range(client: &mut ClusterClient, start: usize, end: usize, version: u64) {
    let put_time = Instant::now();
    let gen_key = generate_keyspace_key(KEYSPACE_ID);
    for i in start..end {
        let key = gen_key(i);
        let val = i_to_val(i);
        let opt = RequestOptions::default();
        client
            .verify_key_value(&key, Some(&val), version, put_time, &opt)
            .unwrap();
    }
}

fn expect_err_msg(err: &client::Error, msg: &str) {
    let err_str = err.to_string();
    assert!(
        err_str.contains(msg),
        "err: {:?}({}), expect: {}",
        err,
        err_str,
        msg
    );
}
