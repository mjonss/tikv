// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use test_cloud_server::{
    ServerCluster,
    client::{PrewriteExt, TxnMutations},
};
use txn_types::LockType;

use super::helper::*;
use crate::{alloc_node_id_vec, i_to_key, i_to_val};

#[test]
fn test_verify_is_primary() {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(alloc_node_id_vec(1), |_, _| {});
    cluster.wait_region_replicated(&[], 1);
    let mut client = cluster.new_client();

    let key = i_to_key(1);
    let primary = i_to_key(2);
    let value = i_to_val(3);
    let start_ts = client.get_ts();

    // Test with pessimistic lock
    let res = client
        .kv_pessimistic_lock(
            primary.clone().into(),
            vec![primary.clone().into(), key.clone().into()],
            start_ts,
            20000,
            start_ts,
            None,
        )
        .expect("pessimistic lock should succeed");
    println!("res: {:?}", res);
    // Verify the lock exists with correct properties
    // sleep 1s
    std::thread::sleep(Duration::from_secs(1));
    let _ =
        must_locked_with_properties(&mut client, &key, &primary, start_ts, LockType::Pessimistic);

    // Check txn status with verify_is_primary=true should fail with PrimaryMismatch
    let result = client.kv_check_txn_status(
        &key, start_ts, start_ts, start_ts, true, false, false, false,
    );
    assert!(result.unwrap().get_error().has_primary_mismatch());

    let mut txn = client.begin_transaction(Some(start_ts));
    // Convert pessimistic lock to prewrite
    client
        .kv_prewrite_ext(
            primary.to_vec().into(),
            Some(&vec![key.to_vec().into()]),
            TxnMutations::from_normal(vec![
                new_put_mutation(primary.to_vec(), value.to_vec()),
                new_put_mutation(key.to_vec(), value.to_vec()),
            ]),
            &mut txn,
            PrewriteExt {
                for_update_ts: start_ts,
                ..Default::default()
            },
        )
        .expect("pessimistic prewrite should succeed");

    // Verify the lock exists with correct properties after prewrite
    let _ = must_locked_with_properties(&mut client, &key, &primary, start_ts, LockType::Put);

    // Check txn status with verify_is_primary=true should still fail with
    // PrimaryMismatch
    let result = client.kv_check_txn_status(
        &key, start_ts, start_ts, start_ts, true, false, false, false,
    );
    assert!(result.unwrap().get_error().has_primary_mismatch());

    client.verify_data_with_ref_store();
    cluster.stop();
}
