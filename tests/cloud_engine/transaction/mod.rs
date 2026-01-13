// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
mod util;
mod check_txn_status;
mod commit;
mod helper;
mod ia;
mod optimistic_2pc;
mod pessimistic;
mod resolve_lock;
mod txn;
mod txn_file;

use bytes::Bytes;
use kvproto::{kvrpcpb, kvrpcpb::WriteConflictReason};
use test_cloud_server::{
    ServerCluster,
    client::{Error, RequestOptions, TxnMutations},
    util::Mutation,
};
use tikv_util::time::Instant;

use crate::{alloc_node_id_vec, i_to_key, i_to_val};

#[test]
fn test_rollback_before_prewrite() {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(alloc_node_id_vec(3), |_, _| {});
    cluster.wait_region_replicated(&[], 3);
    let mut client = cluster.new_client();
    // Split to make secondary keys in another region.
    let split_keys = [50, 150];
    for i in split_keys {
        client.split(&i_to_key(i));
    }
    cluster.wait_pd_region_min_count(split_keys.len() + 1);
    let mut mutations = vec![];
    for i in 0..200 {
        mutations.push(Mutation {
            key: Bytes::from(i_to_key(i)),
            value: Bytes::from(i_to_val(i)),
            op: kvrpcpb::Op::Put,
            ..Default::default()
        });
    }
    let start_ts = client.get_ts();
    let put_time = Instant::now();
    let pk = mutations[0].key.clone();
    let (primary_muts, secondary_muts) = mutations.split_at(50);
    client
        .kv_prewrite(
            pk.clone(),
            None,
            TxnMutations::from_normal(secondary_muts.to_vec()),
            start_ts,
        )
        .expect("kv_prewrite");
    // The get will rollback the transaction.
    let (val, _) = client
        .get_key_version_opt(
            &i_to_key(180),
            u64::MAX,
            put_time,
            &RequestOptions::default(),
        )
        .unwrap();
    assert!(val.is_none());
    let err = client
        .kv_prewrite(
            pk,
            None,
            TxnMutations::from_normal(primary_muts.to_vec()),
            start_ts,
        )
        .unwrap_err();
    match err {
        Error::WriteConflict(write_conflict)
            if write_conflict.get_reason() == WriteConflictReason::SelfRolledBack => {}
        _ => panic!("unexpected error: {:?}", err),
    }
    client.verify_data_with_ref_store();
    cluster.stop();
}
