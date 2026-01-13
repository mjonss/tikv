// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb;
use test_cloud_server::{
    ServerCluster,
    client::{Error, TxnMutations},
    util::Mutation,
};
use tikv_util::info;
use txn_types::{LockType, WriteType};

use super::helper::{
    must_locked_with_properties, must_not_written, must_unlocked, must_written_with_properties,
};
use crate::{alloc_node_id_vec, i_to_key, i_to_val};

#[test]
fn test_commit_ok() {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(alloc_node_id_vec(1), |_, _| {});
    cluster.wait_region_replicated(&[], 1);
    let mut client = cluster.new_client();

    let k1 = i_to_key(1);
    let v1 = i_to_val(1);
    let k2 = i_to_key(2); // For Lock type
    let k3 = i_to_key(3); // For Delete type
    let primary_key = k1.clone();

    let start_ts = client.get_ts();

    // Prewrite all keys in one transaction
    let mutations = vec![
        Mutation {
            key: k1.to_vec().into(),
            value: v1.to_vec().into(),
            op: kvrpcpb::Op::Put,
            ..Default::default()
        },
        Mutation {
            key: k2.to_vec().into(),
            value: vec![].into(), // Lock has no value usually
            op: kvrpcpb::Op::Lock,
            ..Default::default()
        },
        Mutation {
            key: k3.to_vec().into(),
            value: vec![].into(),
            op: kvrpcpb::Op::Del,
            ..Default::default()
        },
    ];

    client
        .kv_prewrite(
            primary_key.to_vec().into(),
            None, // Let client figure out secondaries
            TxnMutations::from_normal(mutations.clone()),
            start_ts,
        )
        .expect("prewrite failed");

    // Check locks
    must_locked_with_properties(&mut client, &k1, &primary_key, start_ts, LockType::Put);
    must_locked_with_properties(&mut client, &k2, &primary_key, start_ts, LockType::Lock);
    must_locked_with_properties(&mut client, &k3, &primary_key, start_ts, LockType::Delete);

    let commit_ts = client.get_ts();

    // Commit all keys
    client
        .kv_commit(
            TxnMutations::from_normal(mutations.clone()),
            start_ts,
            commit_ts,
            false,
        )
        .expect("commit failed");

    // Check writes and that locks are gone
    must_unlocked(&mut client, &k1);
    must_unlocked(&mut client, &k2);
    must_unlocked(&mut client, &k3);
    must_written_with_properties(&mut client, &k1, start_ts, commit_ts, WriteType::Put);
    must_not_written(&mut client, &k2, commit_ts);
    must_written_with_properties(&mut client, &k3, start_ts, commit_ts, WriteType::Delete);

    // Idempotency check: Commit again should succeed
    client
        .kv_commit(
            TxnMutations::from_normal(mutations.clone()),
            start_ts,
            commit_ts,
            false,
        )
        .expect("idempotent commit failed");

    // Re-Check writes to ensure commit didn't change anything
    must_written_with_properties(&mut client, &k1, start_ts, commit_ts, WriteType::Put);
    // LOCK type is not written to write CF anymore.
    must_not_written(&mut client, &k2, commit_ts);
    must_written_with_properties(&mut client, &k3, start_ts, commit_ts, WriteType::Delete);

    client.verify_data_with_ref_store();
    cluster.stop();
}

#[test]
fn test_commit_err() {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(alloc_node_id_vec(1), |_, _| {});
    cluster.wait_region_replicated(&[], 1);
    let mut client = cluster.new_client();

    let k = i_to_key(1);
    let v = i_to_val(1);
    let primary_key = k.clone();

    let ts1 = client.get_ts();
    let ts2 = client.get_ts();
    info!("ts1: {}", ts1);
    info!("ts2: {}", ts2);

    // 1. Not prewrite yet
    let commit_muts_k = TxnMutations::from_normal(vec![Mutation {
        key: k.to_vec().into(),
        value: vec![].into(),
        op: kvrpcpb::Op::Put,
        ..Default::default()
    }]);
    let err = client
        .kv_commit(commit_muts_k.clone(), ts1, ts2, false)
        .unwrap_err();
    match err {
        Error::KeyError(key_error) => {
            assert!(
                key_error.has_txn_not_found()
                    || key_error.get_retryable().contains("TxnLockNotFound")
            );
        }
        _ => panic!("Expected KeyError(TxnLockNotFound), got {:?}", err),
    }
    must_unlocked(&mut client, &k);

    // 2. Prewrite, then commit with wrong start_ts
    let start_ts_ok = client.get_ts();
    let start_ts_wrong = client.get_ts();
    let commit_ts_ok = client.get_ts();
    let prewrite_muts = vec![Mutation {
        key: k.to_vec().into(),
        value: v.to_vec().into(),
        op: kvrpcpb::Op::Put,
        ..Default::default()
    }];
    client
        .kv_prewrite(
            primary_key.to_vec().into(),
            None,
            TxnMutations::from_normal(prewrite_muts.clone()),
            start_ts_ok,
        )
        .expect("prewrite failed");

    let _lock_info =
        must_locked_with_properties(&mut client, &k, &primary_key, start_ts_ok, LockType::Put);

    let err_wrong_ts = client
        .kv_commit(
            TxnMutations::from_normal(prewrite_muts.clone()),
            start_ts_wrong,
            commit_ts_ok,
            false,
        )
        .unwrap_err();
    // Should still be TxnLockNotFound because the lock's start_ts doesn't match
    // start_ts_wrong
    match err_wrong_ts {
        Error::KeyError(key_error) => {
            assert!(
                key_error.has_txn_not_found()
                    || key_error.get_retryable().contains("TxnLockNotFound")
            );
        }
        _ => panic!(
            "Expected KeyError(TxnLockNotFound) for wrong start_ts, got {:?}",
            err_wrong_ts
        ),
    }
    // Lock should still exist
    let _lock_info =
        must_locked_with_properties(&mut client, &k, &primary_key, start_ts_ok, LockType::Put);

    // 3. Rollback, then commit
    client
        .kv_rollback(
            TxnMutations::from_normal(prewrite_muts.clone()),
            start_ts_ok,
        )
        .expect("rollback failed");

    must_unlocked(&mut client, &k); // Lock should be gone
    // rollback is not written to write CF
    must_not_written(&mut client, &k, start_ts_ok);

    let commit_ts_after_rollback = client.get_ts();
    let err_after_rollback = client
        .kv_commit(
            TxnMutations::from_normal(prewrite_muts.clone()),
            start_ts_ok,
            commit_ts_after_rollback,
            false,
        )
        .unwrap_err();
    // Commit after rollback should also result in TxnLockNotFound or a conflict
    // error indicating the rollback.
    match err_after_rollback {
        Error::KeyError(key_error) => {
            assert!(
                key_error.has_txn_not_found()
                    || key_error.get_retryable().contains("TxnLockNotFound")
            );
        }
        _ => panic!(
            "Expected KeyError(TxnLockNotFound) after rollback, got {:?}",
            err_after_rollback
        ),
    }

    // Verify final state (should be unlocked and no write record)
    must_unlocked(&mut client, &k);
    must_not_written(&mut client, &k, start_ts_ok);
    client.verify_data_with_ref_store();
    cluster.stop();
}

// TODO: migrate test_min_commit_ts, which requires adding min_commit_ts to
// MvccInfo
