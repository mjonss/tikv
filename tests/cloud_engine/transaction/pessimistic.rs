// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{thread, time::Duration};

use kvproto::kvrpcpb::Op;
use test_cloud_server::{
    ServerCluster,
    client::{MutateOptions, PrewriteExt, TxnMutations},
    util::Mutation,
};
use txn_types::{LockType, WriteType};

use super::{helper::*, util::*};
use crate::{alloc_node_id_vec, i_to_key, i_to_val};

#[test]
fn test_pes_acquire_lock_single_new_key_then_commit() {
    test_util::init_log_for_test();
    for pipelined in [true, false] {
        let mut cluster = ServerCluster::new(alloc_node_id_vec(1), |_, cfg| {
            cfg.pessimistic_txn.pipelined = pipelined;
        });
        cluster.wait_region_replicated(&[], 1);
        let mut client = cluster.new_client();

        // Arrange
        let k1 = i_to_key(1);
        let v1 = i_to_val(1);

        // Act: T1 acquires pessimistic lock on K1
        let start_ts = client.get_ts();
        let for_update_ts = client.get_ts();
        let lock_ttl = 3000;

        let (_, key_errors) = client
            .kv_pessimistic_lock(
                k1.clone().into(),
                vec![k1.clone().into()],
                start_ts,
                lock_ttl,
                for_update_ts,
                None,
            )
            .expect("T1 pessimistic lock should succeed");

        assert!(key_errors.is_empty(), "No key errors expected");

        // Assert: K1 is locked by T1 with pessimistic lock type

        if pipelined {
            thread::sleep(Duration::from_millis(100));
        }
        let _ = must_locked_with_properties(&mut client, &k1, &k1, start_ts, LockType::Pessimistic);

        // prewrite the value and commit the transaction
        let commit_mutations = vec![new_put_mutation(k1.clone(), v1.clone())];
        let commit_ts = client
            .prewrite_and_commit(
                commit_mutations,
                MutateOptions::start_ts(start_ts),
                PrewriteExt::with_for_update_ts(start_ts),
            )
            .expect("T1 commit failed");

        // Assert: K1 is unlocked after commit
        must_unlocked(&mut client, &k1);

        must_written_with_properties(&mut client, &k1, start_ts, commit_ts, WriteType::Put);

        // Assert: T2 can read V1 from K1
        let t2_ts = client.get_ts();
        let val_read_by_t2 = client
            .get_key_version(&k1, t2_ts.into_inner())
            .expect("T2 Get failed")
            .expect("Value not found for K1 by T2");

        assert_eq!(val_read_by_t2, v1, "T2 Get(K1) should return V1");

        cluster.stop();
    }
}

#[test]
fn test_pes_acquire_lock_key_locked_wait_and_write_conflict_after_release() {
    test_util::init_log_for_test();
    for pipelined in [true, false] {
        let mut cluster = ServerCluster::new(alloc_node_id_vec(1), |_, cfg| {
            cfg.pessimistic_txn.pipelined = pipelined;
        });
        cluster.wait_region_replicated(&[], 1);
        let mut client = cluster.new_client();

        // Arrange: Key K1
        let k1 = i_to_key(2);

        // T_other acquires a pessimistic lock on K1
        let start_ts_other = client.get_ts();
        let for_update_ts_other = client.get_ts();
        let lock_ttl = 10000;

        client
            .kv_pessimistic_lock(
                k1.clone().into(),
                vec![k1.clone().into()],
                start_ts_other,
                lock_ttl,
                for_update_ts_other,
                None,
            )
            .expect("T_other pessimistic lock should succeed");

        // Verify K1 is locked by T_other
        if pipelined {
            thread::sleep(Duration::from_millis(100));
        }
        let _ = must_locked_with_properties(
            &mut client,
            &k1,
            &k1,
            start_ts_other,
            LockType::Pessimistic,
        );

        // Create a cloned client for T1 in a separate thread
        let client_clone = client.clone();
        let k1_clone = k1.clone();

        // Start T1 that will try to acquire the same lock and wait
        let t1_handle = thread::spawn(move || {
            let mut t1_client = client_clone;
            let start_ts_t1 = t1_client.get_ts();
            let for_update_ts_t1 = t1_client.get_ts();

            // T1 will wait up to 5 seconds for the lock
            let wait_timeout = Some(5000);

            let result = t1_client.kv_pessimistic_lock(
                k1_clone.clone().into(),
                vec![k1_clone.clone().into()],
                start_ts_t1,
                lock_ttl,
                for_update_ts_t1,
                wait_timeout,
            );

            (t1_client, start_ts_t1, result)
        });

        // Give T1 some time to start waiting for the lock
        thread::sleep(Duration::from_millis(500));

        // T_other releases its lock (e.g., by rollback)
        client
            .kv_rollback(
                TxnMutations::from_normal(vec![Mutation {
                    op: Op::PessimisticLock,
                    key: k1.clone().into(),
                    value: vec![].into(),
                    ..Default::default()
                }]),
                start_ts_other,
            )
            .expect("T_other rollback should succeed");

        // Wait for T1 to complete and get its result
        let (_, _, t1_result) = t1_handle.join().unwrap();

        // Assert: T1's lock acquisition returns write conflict error
        let (_, key_errors) =
            t1_result.expect("T1 pessimistic lock should return write conflict error");
        assert!(
            key_errors[0].has_conflict(),
            "T1 pessimistic lock should return write conflict error"
        );
        cluster.stop();
    }
}

#[test]
fn test_pes_acquire_lock_key_locked_wait_and_timeout() {
    test_util::init_log_for_test();
    for pipelined in [true, false] {
        let mut cluster = ServerCluster::new(alloc_node_id_vec(1), |_, cfg| {
            cfg.pessimistic_txn.pipelined = pipelined;
        });
        cluster.wait_region_replicated(&[], 1);
        let mut client = cluster.new_client();

        // Arrange: Key K1
        let k1 = i_to_key(3);

        // T_other acquires a pessimistic lock on K1
        let start_ts_other = client.get_ts();
        let for_update_ts_other = client.get_ts();
        let lock_ttl = 10000;

        client
            .kv_pessimistic_lock(
                k1.clone().into(),
                vec![k1.clone().into()],
                start_ts_other,
                lock_ttl,
                for_update_ts_other,
                None,
            )
            .expect("T_other pessimistic lock should succeed");

        // Verify K1 is locked by T_other

        if pipelined {
            thread::sleep(Duration::from_millis(100));
        }
        let _ = must_locked_with_properties(
            &mut client,
            &k1,
            &k1,
            start_ts_other,
            LockType::Pessimistic,
        );

        // T1 tries to acquire the same lock with a short timeout
        let start_ts_t1 = client.get_ts();
        let for_update_ts_t1 = client.get_ts();

        // T1 will only wait 100ms for the lock
        let wait_timeout = Some(100);

        let (_lock_result, errors) = client
            .kv_pessimistic_lock(
                k1.clone().into(),
                vec![k1.clone().into()],
                start_ts_t1,
                lock_ttl,
                for_update_ts_t1,
                wait_timeout,
            )
            .unwrap();

        // Assert: T1's lock acquisition failed with timeout
        assert!(
            errors.len() == 1,
            "T1 pessimistic lock should return one error"
        );
        assert!(
            errors[0].has_locked(),
            "T1 pessimistic lock should return locked error, but got {:?}",
            errors[0]
        );

        // Assert: K1 is still locked by T_other
        let _ = must_locked_with_properties(
            &mut client,
            &k1,
            &k1,
            start_ts_other,
            LockType::Pessimistic,
        );

        cluster.stop();
    }
}

#[test]
fn test_pes_rollback_after_lock_and_prewrite() {
    test_util::init_log_for_test();
    for pipelined in [true, false] {
        let mut cluster = ServerCluster::new(alloc_node_id_vec(1), |_, cfg| {
            cfg.pessimistic_txn.pipelined = pipelined;
        });
        cluster.wait_region_replicated(&[], 1);
        let mut client = cluster.new_client();

        // Arrange
        let k1 = i_to_key(20);
        let v1 = i_to_val(20);

        // Act: Transaction T1
        // 1. T1 acquires pessimistic lock on K1 and stages write of V1
        let t1_start_ts = client.get_ts();
        let t1_for_update_ts = client.get_ts();
        let lock_ttl = 3000; // ms

        let t1_mutations = vec![new_pessimistic_lock_mutation(k1.clone())];

        let (_lock_result, key_errors) = client
            .kv_pessimistic_lock(
                k1.clone().into(),
                vec![k1.clone().into()],
                t1_start_ts,
                lock_ttl,
                t1_for_update_ts,
                None,
            )
            .expect("T1: pessimistic_lock failed");
        assert!(
            key_errors.is_empty(),
            "T1: pessimistic_lock resulted in key_errors: {:?}",
            key_errors
        );

        // Verify K1 is locked by T1 (pessimistically)
        if pipelined {
            thread::sleep(Duration::from_millis(100));
        }
        let _ =
            must_locked_with_properties(&mut client, &k1, &k1, t1_start_ts, LockType::Pessimistic);

        // T1 prewrites the value
        let prewrite_mutations = vec![new_put_mutation(k1.clone(), v1.clone())];
        let mut prewrite_txn = client.begin_transaction(Some(t1_start_ts));
        client
            .kv_prewrite_ext(
                k1.clone().into(),
                None,
                TxnMutations::from_normal(prewrite_mutations),
                &mut prewrite_txn,
                PrewriteExt {
                    resolve_lock: false,
                    for_update_ts: t1_start_ts,
                    ..Default::default()
                },
            )
            .expect("T1 prewrite failed");

        // 2. T1 rolls back
        let t1_rollback_txns = TxnMutations::from_normal(t1_mutations);
        client
            .kv_rollback(t1_rollback_txns, t1_start_ts)
            .expect("T1: kv_rollback failed");

        // K1 should be unlocked after T1's rollback
        must_unlocked(&mut client, &k1);

        // Subsequent transaction T2 Get(K1) gets nothing
        let t2_start_ts = client.get_ts();
        let val_read_by_t2 = client
            .get_key_version(&k1, t2_start_ts.into_inner())
            .expect("T2: Get_key_version failed");

        assert!(
            val_read_by_t2.is_none(),
            "T2: Get(K1) should return None (KeyNotFound) as K1 was new and T1 rolled back, but got {:?}",
            val_read_by_t2
        );

        // Subsequent transaction T3 LockKeys(K1) succeeds (T1's lock is released)
        let t3_start_ts = client.get_ts();
        let t3_for_update_ts = client.get_ts();
        let (_t3_lock_result, t3_key_errors) = client
            .kv_pessimistic_lock(
                k1.clone().into(),
                vec![k1.clone().into()],
                t3_start_ts,
                lock_ttl,
                t3_for_update_ts,
                None,
            )
            .expect("T3: pessimistic_lock failed");
        assert!(
            t3_key_errors.is_empty(),
            "T3: pessimistic_lock should succeed without errors, but got: {:?}",
            t3_key_errors
        );

        // Verify T3's lock and then clean it up (good practice)
        if pipelined {
            thread::sleep(Duration::from_millis(100));
        }
        let _lock_info_t3 =
            must_locked_with_properties(&mut client, &k1, &k1, t3_start_ts, LockType::Pessimistic);

        cluster.stop();
    }
}

#[test]
fn test_lock_waiting() {
    let cases = vec![
        case!(lock!(10, 10); ok!()),
        case!(lock!(10, 10, async, 100); ok!()),
        // should wait 1s, test 900ms
        case!(lock!(10, 10), lock!(20, 20, async, 900); timeout!()),
        // 1s timeout, write conflict
        case!(lock!(10, 10), lock!(20, 20, async, 2000); fail!()),
        case!(concurrent!(
            sequence!(lock!(10, 10), sleep!(500), rollback!(10); ok!()),
            // fail => it is woken up
            sequence!(sleep!(300), lock!(20, 20, async, 500); fail!())
        ); ok!()),
        // 2 txns waiting
        case!(concurrent!(
            sequence!(lock!(10, 10), sleep!(500), rollback!(10); ok!()),
            sequence!(sleep!(300), lock!(20, 20, async, 500); fail!()),
            sequence!(sleep!(300), lock!(30, 30, async, 150); timeout!())
        ); ok!()),
    ];
    run_test_cases(cases);
}

#[test]
fn test_deadlock_detection() {
    let k1 = i_to_key(100);
    let k2 = i_to_key(101);

    let k3 = i_to_key(102);
    let k4 = i_to_key(103);
    let k5 = i_to_key(104);

    let cases = vec![
        case!(concurrent!(
            sequence!(lock!(10, 10, k1), sleep!(200), lock!(10, 10, k2); fail!()),
            sequence!(lock!(20, 20, k2), sleep!(400), lock!(20, 20, k1); deadlock!())
        ); ok!()),
        case!(concurrent!(
            sequence!(lock!(10, 10, k3), sleep!(200), lock!(10, 10, k4); fail!()),
            sequence!(lock!(20, 20, k4), sleep!(300), lock!(20, 20, k5); fail!()),
            sequence!(lock!(30, 30, k5), sleep!(400), lock!(30, 30, k3); deadlock!())
        ); ok!()),
    ];

    run_test_cases(cases);
}
