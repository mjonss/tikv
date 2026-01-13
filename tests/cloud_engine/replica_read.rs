// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use futures::executor::block_on;
use kvproto::metapb;
use pd_client::PdClient;
use test_cloud_server::{ServerCluster, client::RequestOptions, try_wait};
use tikv_util::{
    store::{find_peer, new_learner_peer},
    time::Instant,
};

use crate::alloc_node_id_vec;

#[test]
fn test_replica_read() {
    const DATA_COUNT: usize = 10;

    test_util::init_log_for_test();
    let node_ids = alloc_node_id_vec(4);
    let mut cluster = ServerCluster::new(node_ids.clone(), |_, _| {});
    let pd_client = cluster.get_pd_client();
    let mut client = cluster.new_client();
    cluster.wait_region_replicated(&[], 3);
    pd_client.disable_default_operator();

    let put_time = Instant::now();
    client.put_kv(0..DATA_COUNT, i_to_key, i_to_val);

    let region_id = client.get_region_id(&i_to_key(0));
    let region = block_on(pd_client.get_region_by_id(region_id))
        .unwrap()
        .unwrap();

    let learner_node_id = node_ids
        .iter()
        .find(|node_id| {
            let store_id = cluster.get_store_id(**node_id);
            find_peer(&region, store_id).is_none()
        })
        .unwrap()
        .to_owned();
    let learner_store_id = cluster.get_store_id(learner_node_id);
    let learner_peer = new_learner_peer(learner_store_id, 2);

    assert!(
        cluster
            .get_kvengine(learner_node_id)
            .get_shard(region_id)
            .is_none()
    );
    pd_client.must_add_peer(region_id, learner_peer.clone());
    // Ensure learner exists.
    assert!(try_wait(
        || {
            cluster
                .get_kvengine(learner_node_id)
                .get_shard(region_id)
                .is_some()
        },
        10
    ));

    let options = RequestOptions::learner();
    for i in 0..DATA_COUNT {
        let (val, ctx) = client
            .get_key_version_opt(&i_to_key(i), u64::MAX, put_time, &options)
            .unwrap();
        assert_eq!(ctx.get_peer().get_role(), metapb::PeerRole::Learner);
        assert_eq!(ctx.get_peer().get_id(), learner_peer.id);
        assert_eq!(val.unwrap(), i_to_val(i));
    }
    let ref_store = client.dump_ref_store();
    client
        .verify_data_with_given_ref_store(&ref_store, None, &options)
        .unwrap();

    cluster.get_data_stats().check_data().unwrap();
    cluster.stop();
}

fn i_to_key(i: usize) -> Vec<u8> {
    format!("xkey_{:03}", i).into_bytes()
}

fn i_to_val(i: usize) -> Vec<u8> {
    format!("val_{:03}", i).into_bytes().repeat(3)
}
