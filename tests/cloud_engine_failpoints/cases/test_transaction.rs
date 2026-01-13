// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::assert_matches::assert_matches;

use cloud_server::ASYNC_WRITE_CALLBACK_DROPPED_ERR_MSG;
use kvproto::kvrpcpb::Op;
use test_cloud_server::{
    ServerCluster, alloc_node_id_vec,
    client::{Error as ClientError, TxnMutations},
    util::Mutation,
};

use crate::cases::i_to_key;

#[test]
fn test_undetermined_write_err() {
    test_util::init_log_for_test();
    let cluster = ServerCluster::new(alloc_node_id_vec(1), |_, _| {});
    cluster.wait_region_replicated(&[], 1);
    let mut client = cluster.new_client();

    let mut mutation = Mutation::default();
    let k1 = i_to_key(1);
    mutation.set_op(Op::Put);
    mutation.set_key(k1.clone());
    mutation.set_value(b"v".to_vec());
    let primary_key = k1;
    let start_ts = client.get_ts();
    let mutations = vec![mutation];

    fail::cfg("applied_cb_return_undetermined_err", "return()").unwrap();
    let err = client
        .kv_prewrite(
            primary_key.to_vec().into(),
            None,
            TxnMutations::from_normal(mutations.clone()),
            start_ts,
        )
        .unwrap_err();
    assert_matches!(err, ClientError::RegionError(ref region_err) if {
        assert!(region_err.has_undetermined_result());
        assert_eq!(
            region_err.get_undetermined_result().get_message(),
            ASYNC_WRITE_CALLBACK_DROPPED_ERR_MSG,
        );
        true
    });
    fail::remove("applied_cb_return_undetermined_err");
}

#[test]
fn test_raftkv_early_error_report() {
    test_util::init_log_for_test();
    let nodes = alloc_node_id_vec(1);
    let cluster = ServerCluster::new(nodes.clone(), |_, _| {});
    cluster.wait_region_replicated(&[], 1);
    let mut client = cluster.new_client();

    let mut mutation = Mutation::default();
    let k1 = i_to_key(1);
    mutation.set_op(Op::Put);
    mutation.set_key(k1.clone());
    mutation.set_value(b"v".to_vec());
    let primary_key = k1;
    let start_ts = client.get_ts();
    let mutations = vec![mutation];

    fail::cfg("raftkv_early_error_report", "return").unwrap();
    let prewrite_resp = client
        .kv_prewrite_single_region_without_retry(
            primary_key.to_vec().into(),
            None,
            TxnMutations::from_normal(mutations.clone()),
            start_ts,
        )
        .unwrap();
    assert!(prewrite_resp.has_region_error(), "{:?}", prewrite_resp);
    assert!(
        prewrite_resp.get_region_error().has_region_not_found(),
        "{:?}",
        prewrite_resp
    );
    fail::remove("raftkv_early_error_report");
}

#[test]
fn test_scheduler_shard_is_active() {
    test_util::init_log_for_test();
    let nodes = alloc_node_id_vec(3);
    let cluster = ServerCluster::new(nodes.clone(), |_, _| {});
    cluster.wait_region_replicated(&[], 3);
    let mut client = cluster.new_client();

    fail::cfg("check_leader_peer_is_leader", "return(false)").unwrap();

    let mut mutation = Mutation::default();
    let k1 = i_to_key(1);
    mutation.set_op(Op::Put);
    mutation.set_key(k1.clone());
    mutation.set_value(b"v".to_vec());
    let primary_key = k1;
    let start_ts = client.get_ts();
    let mutations = vec![mutation];

    let prewrite_resp = client
        .kv_prewrite_single_region_without_retry(
            primary_key.to_vec().into(),
            None,
            TxnMutations::from_normal(mutations.clone()),
            start_ts,
        )
        .unwrap();
    let err = prewrite_resp.get_region_error();
    assert!(err.has_not_leader(), "{:?}", err);
    assert!(err.get_not_leader().has_leader(), "{:?}", err);
    assert!(err.get_not_leader().get_leader().get_id() > 0);
    fail::remove("check_leader_peer_is_leader");
}
