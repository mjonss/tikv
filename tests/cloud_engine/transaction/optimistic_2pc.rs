// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{thread, time::Duration};

use test_cloud_server::{
    ServerCluster,
    client::{ClusterClient, Error, MutateOptions, PrewriteExt, TxnMutations},
};
use txn_types::{LockType, TimeStamp, WriteType};

use super::helper::*;
use crate::{alloc_node_id_vec, i_to_key, i_to_val};

// Helper to set an initial value for a key and verify it
fn setup_initial_value(
    client: &mut ClusterClient,
    key: &[u8],
    value: Vec<u8>,
) -> (TimeStamp, TimeStamp) {
    let start_ts_init = client.get_ts();
    let init_mutations_vec = vec![new_put_mutation(key.to_vec(), value)];
    let commit_ts_init = client
        .prewrite_and_commit(
            init_mutations_vec,
            MutateOptions::start_ts(start_ts_init),
            PrewriteExt::default(),
        )
        .expect("Initial prewrite_and_commit failed");
    must_written_with_properties(client, key, start_ts_init, commit_ts_init, WriteType::Put);
    (start_ts_init, commit_ts_init)
}

#[test]
fn test_opt_commit_single_new_key() {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(alloc_node_id_vec(1), |_, _| {});
    cluster.wait_region_replicated(&[], 1);
    let mut client = cluster.new_client();

    // Arrange
    let k1 = i_to_key(1);
    let v1 = i_to_val(1);
    let mutations = vec![new_put_mutation(k1.clone(), v1.clone())];
    let txn_muts = TxnMutations::from_normal(mutations);

    // Act
    let start_ts = client.get_ts();
    client
        .kv_prewrite(txn_muts.primary(), None, txn_muts.clone(), start_ts)
        .expect("Prewrite failed for T1");

    let commit_ts = client.get_ts();
    client
        .kv_commit(txn_muts.clone(), start_ts, commit_ts, false)
        .expect("Commit failed for T1");

    // Assert
    must_unlocked(&mut client, &k1);
    must_written_with_properties(&mut client, &k1, start_ts, commit_ts, WriteType::Put);

    let t2_ts = client.get_ts();
    let t2_ver = t2_ts.into_inner();
    let val_read_by_t2 = client
        .get_key_version(&k1, t2_ver)
        .expect("Get failed for T2")
        .expect("Value not found for K1 by T2");
    assert_eq!(val_read_by_t2, v1, "T2 Get(K1) should return V1");

    cluster.stop();
}

#[test]
fn test_opt_commit_single_existing_key_overwrite() {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(alloc_node_id_vec(1), |_, _| {});
    cluster.wait_region_replicated(&[], 1);
    let mut client = cluster.new_client();

    // Arrange: Key K1 already has V_old
    let k1 = i_to_key(21);
    let v_old = i_to_val(210);
    let start_ts_init = client.get_ts();
    let init_mutations_vec = vec![new_put_mutation(k1.clone(), v_old.clone())];
    let commit_ts_init = client
        .prewrite_and_commit(
            init_mutations_vec,
            MutateOptions::start_ts(start_ts_init),
            PrewriteExt::default(),
        )
        .expect("Initial prewrite_and_commit failed");
    must_written_with_properties(
        &mut client,
        &k1,
        start_ts_init,
        commit_ts_init,
        WriteType::Put,
    );

    // Arrange: T1 wants to write V_new to K1
    let v_new = i_to_val(211);
    let t1_mutations_vec = vec![new_put_mutation(k1.clone(), v_new.clone())];
    let t1_txn_muts = TxnMutations::from_normal(t1_mutations_vec);

    // Act: T1 prewrites and commits
    let t1_start_ts = client.get_ts();
    client
        .kv_prewrite(
            t1_txn_muts.primary(),
            None,
            t1_txn_muts.clone(),
            t1_start_ts,
        )
        .expect("Prewrite failed for T1");

    let t1_commit_ts = client.get_ts();
    client
        .kv_commit(t1_txn_muts, t1_start_ts, t1_commit_ts, false)
        .expect("Commit failed for T1");

    // Assert
    must_unlocked(&mut client, &k1);
    must_written_with_properties(&mut client, &k1, t1_start_ts, t1_commit_ts, WriteType::Put);

    let t2_ts = client.get_ts();
    let t2_ver = t2_ts.into_inner();
    let val_read_by_t2 = client
        .get_key_version(&k1, t2_ver)
        .expect("Get failed for T2")
        .expect("Value not found for K1 by T2");
    assert_eq!(val_read_by_t2, v_new, "T2 Get(K1) should return V_new");

    cluster.stop();
}

#[test]
fn test_opt_commit_multi_key_same_region() {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(alloc_node_id_vec(1), |_, _| {});
    cluster.wait_region_replicated(&[], 1);
    let mut client = cluster.new_client();

    // Arrange
    let k1 = i_to_key(31);
    let v1 = i_to_val(31);
    let k2 = i_to_key(32);
    let v2 = i_to_val(32);
    let k3 = i_to_key(33);
    let v3 = i_to_val(33);

    let mutations = vec![
        new_put_mutation(k1.clone(), v1.clone()),
        new_put_mutation(k2.clone(), v2.clone()),
        new_put_mutation(k3.clone(), v3.clone()),
    ];
    let txn_muts = TxnMutations::from_normal(mutations);
    let secondaries_keys = txn_muts.secondaries();

    // Act
    let start_ts = client.get_ts();
    client
        .kv_prewrite(
            txn_muts.primary(),
            Some(&secondaries_keys),
            txn_muts.clone(),
            start_ts,
        )
        .expect("Prewrite failed for T1");

    let commit_ts = client.get_ts();
    client
        .kv_commit(txn_muts, start_ts, commit_ts, false)
        .expect("Commit failed for T1");

    // Assert
    must_unlocked(&mut client, &k1);
    must_unlocked(&mut client, &k2);
    must_unlocked(&mut client, &k3);
    must_written_with_properties(&mut client, &k1, start_ts, commit_ts, WriteType::Put);
    must_written_with_properties(&mut client, &k2, start_ts, commit_ts, WriteType::Put);
    must_written_with_properties(&mut client, &k3, start_ts, commit_ts, WriteType::Put);

    let t2_ts = client.get_ts();
    let t2_ver = t2_ts.into_inner();
    assert_eq!(client.get_key_version(&k1, t2_ver).unwrap().unwrap(), v1);
    assert_eq!(client.get_key_version(&k2, t2_ver).unwrap().unwrap(), v2);
    assert_eq!(client.get_key_version(&k3, t2_ver).unwrap().unwrap(), v3);

    cluster.stop();
}

#[test]
fn test_opt_write_conflict_key_locked_by_uncommitted_opt_txn() {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(alloc_node_id_vec(1), |_, _| {});
    cluster.wait_region_replicated(&[], 1);
    let mut client = cluster.new_client();

    // Arrange
    let k1 = i_to_key(41);
    let v_other = i_to_val(410);
    let v1 = i_to_val(411);

    // T_other prewrites K1
    let start_ts_other = client.get_ts();
    let muts_other = TxnMutations::from_normal(vec![new_put_mutation(k1.clone(), v_other.clone())]);
    client
        .kv_prewrite(
            muts_other.primary(),
            None,
            muts_other.clone(),
            start_ts_other,
        )
        .expect("T_other prewrite failed");
    let _ = must_locked_with_properties(&mut client, &k1, &k1, start_ts_other, LockType::Put);

    // T1 attempts to prewrite and commit K1
    let start_ts1 = client.get_ts();
    assert!(
        start_ts1 > start_ts_other,
        "T1 start_ts should be greater than T_other start_ts"
    );
    let muts1 = TxnMutations::from_normal(vec![new_put_mutation(k1.clone(), v1.clone())]);
    let mut txn1 = client.begin_transaction(Some(start_ts1));
    // Act: T1 prewrites
    let res = client.kv_prewrite_ext(
        muts1.primary(),
        None,
        muts1.clone(),
        &mut txn1,
        PrewriteExt::no_resolve_lock(),
    );

    // Assert
    assert!(res.is_err(), "T1 prewrite should fail");
    match res.unwrap_err() {
        Error::KeyErrors(key_errors) => {
            assert!(
                key_errors.len() == 1,
                "T1 prewrite should fail with 1 KeyError, got {:?}",
                key_errors
            );
            assert!(
                key_errors[0].has_locked(),
                "T1 prewrite should fail with KeyIsLocked, got {:?}",
                key_errors[0]
            );
        }
        err => panic!("Expected KeyError(Locked) for T1 prewrite, got {:?}", err),
    }

    // K1 should still be locked by T_other
    must_locked_with_properties(&mut client, &k1, &k1, start_ts_other, LockType::Put);

    cluster.stop();
}

#[test]
fn test_opt_write_conflict_during_prewrite_check() {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(alloc_node_id_vec(1), |_, _| {});
    cluster.wait_region_replicated(&[], 1);
    let mut client = cluster.new_client();

    // Arrange: K1 initial value V_old
    let k1 = i_to_key(51);
    let v_old = i_to_val(510);
    let start_ts_init = client.get_ts();
    let init_mutations_vec = vec![new_put_mutation(k1.clone(), v_old.clone())];
    let commit_ts_init = client
        .prewrite_and_commit(
            init_mutations_vec,
            MutateOptions::start_ts(start_ts_init),
            PrewriteExt::default(),
        )
        .expect("Initial prewrite_and_commit failed");
    must_written_with_properties(
        &mut client,
        &k1,
        start_ts_init,
        commit_ts_init,
        WriteType::Put,
    );

    // Arrange: T1 reads K1 (implicitly, its start_ts is after V_old's commit_ts)
    let start_ts1 = client.get_ts();

    // Arrange: T_other modifies K1 to V_T_other and commits before T1 commits
    let v_t_other = i_to_val(512);
    let start_ts_other = client.get_ts();
    assert!(
        start_ts_other > start_ts1,
        "T_other start_ts should be > T1 start_ts for this scenario type, though conflict check is on T1's start_ts"
    );
    let muts_other_vec = vec![new_put_mutation(k1.clone(), v_t_other.clone())];
    let commit_ts_other = client
        .prewrite_and_commit(
            muts_other_vec,
            MutateOptions::start_ts(start_ts_other),
            PrewriteExt::default(),
        )
        .expect("T_other prewrite_and_commit failed");
    must_written_with_properties(
        &mut client,
        &k1,
        start_ts_other,
        commit_ts_other,
        WriteType::Put,
    );

    // Act: T1 attempts to modify K1 to V_T1 and commit
    let v_t1 = i_to_val(511);
    let muts1 = TxnMutations::from_normal(vec![new_put_mutation(k1.clone(), v_t1.clone())]);
    let res = client.kv_prewrite(muts1.primary(), None, muts1.clone(), start_ts1);

    // Assert: T1 prewrite should fail with WriteConflict error
    assert!(res.is_err(), "T1 prewrite should fail");
    match res.unwrap_err() {
        Error::WriteConflict(_) => {}
        err => panic!("Expected WriteConflict for T1 prewrite, got {:?}", err),
    }

    // Assert: Subsequent transaction T_read Get(K1) returns V_T_other
    let t_read_ts = client.get_ts();
    let t_read_ver = t_read_ts.into_inner();
    let val_read = client
        .get_key_version(&k1, t_read_ver)
        .expect("T_read Get failed")
        .expect("K1 not found by T_read");
    assert_eq!(val_read, v_t_other, "T_read should get V_T_other for K1");

    must_unlocked(&mut client, &k1);

    cluster.stop();
}

#[test]
fn test_opt_rollback_explicitly_after_prewrite() {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(alloc_node_id_vec(1), |_, _| {});
    cluster.wait_region_replicated(&[], 1);
    let mut client = cluster.new_client();

    let k1 = i_to_key(61);
    let v1_t1 = i_to_val(611);

    // Arrange: T1 prewrites K1 with V1_T1
    let start_ts_t1 = client.get_ts();
    let muts_t1 = TxnMutations::from_normal(vec![new_put_mutation(k1.clone(), v1_t1.clone())]);
    client
        .kv_prewrite(muts_t1.primary(), None, muts_t1.clone(), start_ts_t1)
        .expect("T1 prewrite failed");

    // Verify K1 is locked by T1
    let lock_info = must_locked_with_properties(&mut client, &k1, &k1, start_ts_t1, LockType::Put);
    assert_eq!(lock_info.get_short_value(), v1_t1.as_slice());

    // Act: T1 explicitly rolls back
    client
        .kv_rollback(muts_t1.clone(), start_ts_t1)
        .expect("T1 rollback failed");

    // Assert: K1 should be unlocked
    must_unlocked(&mut client, &k1);

    // Assert: Subsequent transaction T2 Get(K1) gets KeyNotFound
    let start_ts_t2 = client.get_ts();
    let val_read_by_t2 = client
        .get_key_version(&k1, start_ts_t2.into_inner())
        .expect("T2 Get failed");
    assert!(
        val_read_by_t2.is_none(),
        "K1 should be KeyNotFound for T2 after T1 rollback"
    );

    cluster.stop();
}

#[test]
fn test_opt_rollback_lock_cleaned_by_other_txn_access() {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(alloc_node_id_vec(1), |_, _| {});
    cluster.wait_region_replicated(&[], 1);
    let mut client = cluster.new_client();

    // Arrange: Key K1 initially has value V_old
    let k1 = i_to_key(71);
    let v_old = i_to_val(710);
    setup_initial_value(&mut client, &k1, v_old.clone());

    // Arrange: T1 prewrites K1 with V1_T1, but does not commit
    // Use a small TTL to make the lock expire faster
    let v1_t1 = i_to_val(711);
    let start_ts_t1 = client.get_ts();
    let muts_t1 = TxnMutations::from_normal(vec![new_put_mutation(k1.clone(), v1_t1.clone())]);
    let mut txn_t1 = client.begin_transaction(Some(start_ts_t1));
    client
        .kv_prewrite_ext(
            muts_t1.primary(),
            None,
            muts_t1.clone(),
            &mut txn_t1,
            PrewriteExt {
                resolve_lock: false,
                lock_ttl: Duration::from_millis(1), // 1ms TTL
                ..Default::default()
            },
        )
        .expect("T1 prewrite failed");

    // Verify K1 is locked by T1
    let lock_info = must_locked_with_properties(&mut client, &k1, &k1, start_ts_t1, LockType::Put);
    assert_eq!(lock_info.get_short_value(), v1_t1.as_slice());

    // Act: T2 (start_ts > T1.start_ts) accesses K1 (Get).
    // This access should trigger lock resolution for T1's lock, leading to its
    // rollback.
    thread::sleep(Duration::from_millis(2));
    let start_ts_t2 = client.get_ts();
    let val_read_by_t2 = client
        .get_key_version(&k1, start_ts_t2.into_inner())
        .expect("T2 Get failed for K1")
        .expect("Value not found for K1 by T2, but V_old was expected");

    // Assert: T2's Get(K1) should return V_old
    assert_eq!(
        val_read_by_t2, v_old,
        "T2 Get(K1) should return V_old after T1's lock was cleaned"
    );

    // Assert: K1 should be unlocked after T1's lock was resolved by T2's access
    must_unlocked(&mut client, &k1);

    // Assert: Subsequent transaction T3 Get(K1) also gets V_old
    let start_ts_t3 = client.get_ts();
    let val_read_by_t3 = client
        .get_key_version(&k1, start_ts_t3.into_inner())
        .expect("T3 Get failed for K1")
        .expect("Value not found for K1 by T3, but V_old was expected");
    assert_eq!(val_read_by_t3, v_old, "T3 Get(K1) should return V_old");

    cluster.stop();
}
