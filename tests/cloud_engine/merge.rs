// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{thread, time::Duration};

use futures::executor::block_on;
use pd_client::PdClient;
use test_cloud_server::{ServerCluster, client::RequestOptions, try_wait};
use test_pd_client::PdClientExt;
use tikv::config::TikvConfig;
use tikv_util::{
    config::ReadableDuration,
    store::{find_peer, new_learner_peer},
};

use crate::{
    alloc_node_id, destroy_range, generate_keyspace_key, get_keyspace_prefix, i_to_key,
    i_to_key_v1, i_to_val, is_region_belongs_to_keyspace,
};

#[test]
fn test_region_merge() {
    test_util::init_log_for_test();
    let node_ids = vec![alloc_node_id(), alloc_node_id(), alloc_node_id()];
    let mut cluster = ServerCluster::new(node_ids.clone(), |_, _| {});
    cluster.wait_region_replicated(&[], 3);
    let mut client = cluster.new_client();
    // Split a keyspace region, because the first region start key is empty
    // and it can't be merged with keyspace region within data.
    let prev_keyspace = i_to_key(0);
    client.split(&prev_keyspace);
    let split_key = i_to_key(5);
    client.split(&split_key);
    cluster.wait_pd_region_count(3);
    client.put_kv(0..10, i_to_key, i_to_val);
    client.merge(&i_to_key(0), &i_to_key(10));
    cluster.wait_pd_region_count(2);
    client.verify_data_with_ref_store();
    for &node_id in &node_ids {
        cluster.stop_node(node_id);
    }
    for &node_id in &node_ids {
        cluster.start_node(node_id, |_, _| {});
    }
    client.verify_data_with_ref_store();
    cluster.stop();
}

/// Test if a peer can be destroyed properly in such conditions as follows
/// 1. A peer is isolated
/// 2. Then its region merges to another region.
/// 3. Isolation disappears
#[test]
fn test_region_merge_isolated_peer() {
    test_util::init_log_for_test();
    let node_ids = vec![
        alloc_node_id(),
        alloc_node_id(),
        alloc_node_id(),
        alloc_node_id(),
    ];
    let update_conf_fn = |_, conf: &mut TikvConfig| {
        conf.raft_store.peer_stale_state_check_interval = ReadableDuration::secs(1);
        conf.raft_store.abnormal_leader_missing_duration = ReadableDuration::secs(3);
        conf.raft_store.max_leader_missing_duration = ReadableDuration::secs(5);
    };
    let mut cluster = ServerCluster::new(node_ids.clone(), update_conf_fn);
    let pd_client = cluster.get_pd_client();
    let mut client = cluster.new_client();
    cluster.wait_region_replicated(&[], 3);
    pd_client.disable_default_operator();

    let prev_keyspace = i_to_key(0);
    client.split(&prev_keyspace);

    let split_key = i_to_key(5);
    client.split(&split_key);
    cluster.wait_pd_region_count(3);
    client.put_kv(0..10, i_to_key, i_to_val);

    let left_id = client.get_region_id(&i_to_key(0));
    let left = block_on(pd_client.get_region_by_id(left_id))
        .unwrap()
        .unwrap();

    let isolated_node_id = node_ids
        .iter()
        .find(|node_id| {
            let store_id = cluster.get_store_id(**node_id);
            find_peer(&left, store_id).is_none()
        })
        .unwrap()
        .to_owned();
    let isolated_store_id = cluster.get_store_id(isolated_node_id);
    let learner_peer = new_learner_peer(isolated_store_id, 2);

    pd_client.must_add_peer(left_id, learner_peer.clone());
    // Ensure this learner exists.
    assert!(try_wait(
        || {
            cluster
                .get_kvengine(isolated_node_id)
                .get_shard(left_id)
                .is_some()
        },
        10
    ));

    cluster.stop_node(isolated_node_id);
    pd_client.must_remove_peer(left_id, learner_peer);
    thread::sleep(Duration::from_millis(500)); // Wait for node to stop completely.

    client.merge(&i_to_key(0), &i_to_key(10));
    cluster.wait_pd_region_count(2);
    client.verify_data_with_ref_store();

    cluster.start_node(isolated_node_id, update_conf_fn);
    assert!(try_wait(
        || {
            cluster
                .get_kvengine(isolated_node_id)
                .get_shard(left_id)
                .is_none()
        },
        10
    ));

    cluster.get_data_stats().check_data().unwrap();
    client.verify_data_with_ref_store();
    cluster.stop();
}

#[test]
fn test_region_split_merge() {
    test_util::init_log_for_test();

    let node_ids = vec![alloc_node_id(), alloc_node_id(), alloc_node_id()];
    let mut cluster = ServerCluster::new(node_ids.clone(), |_, _| {});
    cluster.wait_region_replicated(&[], 3);

    let mut client = cluster.new_client();
    let pd_client = cluster.get_pd_client();
    // 1. Generate 1 keyspace region.
    // 2. Split it into 10 regions.
    // 3. Check all regions inner_key_off.
    // 4. Merge 10 inner regions into 1 region.
    // 5. Check the keyspace region's inner_key_off.
    let (ks100, ks101) = (get_keyspace_prefix(100), get_keyspace_prefix(101));
    let generate_ks100_keys = generate_keyspace_key(100);
    let split_keys = vec![ks100.clone(), ks101.clone()];
    for sk in split_keys {
        client.split(sk.as_slice());
    }
    cluster.wait_pd_region_count(3);

    // split keyspace inner regions
    for i in 0..10 {
        let split_key = generate_ks100_keys(i);
        client.split(split_key.as_slice());
    }

    cluster.wait_pd_region_count(13);

    let mut regions = pd_client.get_all_regions();
    regions.sort_by(|a, b| a.get_start_key().cmp(b.get_start_key()));

    // merge inner regions
    for region in &regions {
        if is_region_belongs_to_keyspace(region, 100) {
            let start_key = region.get_start_key();
            let end_key = region.get_end_key();
            if end_key.starts_with(ks101.as_slice()) {
                continue;
            }

            client
                .try_merge_adjacent_region(
                    start_key,
                    Some(ks100.as_slice()),
                    Duration::from_secs(3),
                )
                .unwrap();
        }
    }

    cluster.wait_pd_region_count(3);

    for &node_id in &node_ids {
        cluster.stop_node(node_id);
    }
    for &node_id in &node_ids {
        cluster.start_node(node_id, |_, _| {});
    }
    client.verify_data_with_ref_store();
    cluster.stop();
}

#[test]
fn test_region_merge_with_del_prefixes() {
    test_util::init_log_for_test();

    let node_id = alloc_node_id();
    let mut cluster = ServerCluster::new(vec![node_id], |_, _| {});

    let mut client = cluster.new_client();
    let pd_client = cluster.get_pd_client();
    // 1. Generate 1 keyspace region.
    // 2. Split it into 2 regions.
    // 3. Put some keys to the 2 regions.
    // 4. Destroy range in each region.
    // 5. Merge the 2 regions and check the result.
    let (ks100, ks101) = (get_keyspace_prefix(100), get_keyspace_prefix(101));
    let generate_ks100_keys = generate_keyspace_key(100);
    let split_keys = vec![ks100.clone(), ks101.clone()];
    for sk in split_keys {
        client.split(sk.as_slice());
    }
    cluster.wait_pd_region_count(3);

    // split keyspace 2 inner regions
    let split_key = generate_ks100_keys(1);
    client.split(split_key.as_slice());
    cluster.wait_pd_region_count(4);

    let generate_region_0_key = |i: usize| {
        let mut key = generate_ks100_keys(0);
        key.extend_from_slice(i.to_string().as_bytes());
        key
    };
    let generate_region_1_key = |i: usize| {
        let mut key = generate_ks100_keys(1);
        key.extend_from_slice(i.to_string().as_bytes());
        key
    };
    // put some keys into 2 inner regions
    client.put_kv(0..100, generate_region_0_key, i_to_val);
    client.put_kv(0..100, generate_region_1_key, i_to_val);

    // destroy range in both regions
    let store_id = cluster.get_stores()[0];
    destroy_range(&mut client, store_id, &generate_ks100_keys(0));
    destroy_range(&mut client, store_id, &generate_ks100_keys(1));

    let snap = cluster.get_snap(node_id, &generate_ks100_keys(0));
    assert!(!snap.has_data_in_prefix(&generate_ks100_keys(0)));
    let snap = cluster.get_snap(node_id, &generate_ks100_keys(1));
    assert!(!snap.has_data_in_prefix(&generate_ks100_keys(1)));

    let mut regions = pd_client.get_all_regions();
    regions.sort_by(|a, b| a.get_start_key().cmp(b.get_start_key()));

    // merge inner regions
    for region in &regions {
        if is_region_belongs_to_keyspace(region, 100) {
            let start_key = region.get_start_key();
            let end_key = region.get_end_key();
            if end_key.starts_with(ks101.as_slice()) {
                continue;
            }

            client
                .try_merge_adjacent_region(
                    start_key,
                    Some(ks100.as_slice()),
                    Duration::from_secs(3),
                )
                .unwrap();
        }
    }
    cluster.wait_pd_region_count(3);

    let snap = cluster.get_snap(node_id, &generate_ks100_keys(0));
    assert!(!snap.has_data_in_prefix(&generate_ks100_keys(0)));
    assert!(!snap.has_data_in_prefix(&generate_ks100_keys(1)));

    cluster.stop_node(node_id);
    cluster.stop();
}

#[test]
fn test_region_merge_keyspaces() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let mut cluster = ServerCluster::new(vec![node_id], |_, _| {});

    let mut client = cluster.new_client();
    // 1. Generate 1 APIv1 region and 2 keyspace regions.
    // 2. Put some keys to the regions.
    // 3. 2 keyspaces can not merge with data.
    // 4. Destroy range in each keyspace region.
    // 5. Merge the 2 keyspaces and check the result.
    // 6. Merge the APIv1 region and keyspace region, and check the result.
    let (ks100, ks101, ks102) = (
        get_keyspace_prefix(100),
        get_keyspace_prefix(101),
        get_keyspace_prefix(102),
    );
    let generate_ks100_keys = generate_keyspace_key(100);
    let generate_ks101_keys = generate_keyspace_key(101);
    let split_keys = vec![ks100.clone(), ks101.clone(), ks102];
    for sk in split_keys {
        client.split(sk.as_slice());
    }
    cluster.wait_pd_region_count(4);

    let generate_ks100_ext_key = |i: usize| {
        let mut key = generate_ks100_keys(0);
        key.extend_from_slice(i.to_string().as_bytes());
        key
    };
    let generate_ks101_ext_key = |i: usize| {
        let mut key = generate_ks101_keys(0);
        key.extend_from_slice(i.to_string().as_bytes());
        key
    };

    // put some APIv1 keys
    client.put_kv(0..10, i_to_key_v1, i_to_val);
    let ref_store = client.dump_ref_store();
    // put some keys into 2 keyspace regions
    client.put_kv(0..100, generate_ks100_ext_key, i_to_val);
    client.put_kv(0..100, generate_ks101_ext_key, i_to_val);

    client.try_merge(&ks100, &ks101);
    // the 2 keyspaces can not merge with data
    cluster.wait_pd_region_count(4);

    // destroy range in both keyspace
    let store_id = cluster.get_stores()[0];
    destroy_range(&mut client, store_id, &ks100);
    destroy_range(&mut client, store_id, &ks101);
    let snap = cluster.get_snap(node_id, &ks100);
    assert!(!snap.has_data_in_prefix(&ks100));
    let snap = cluster.get_snap(node_id, &ks101);
    assert!(!snap.has_data_in_prefix(&ks101));

    client.try_merge(&ks100, &ks101);
    // wait for region merge
    cluster.wait_pd_region_count(3);
    let snap = cluster.get_snap(node_id, &ks100);
    assert!(!snap.has_data_in_prefix(&ks100));
    let snap = cluster.get_snap(node_id, &ks101);
    assert!(!snap.has_data_in_prefix(&ks101));

    // merge APIv1 & keyspace region.
    client.try_merge(&[], &ks100);
    cluster.wait_pd_region_count(2);
    client
        .verify_data_with_given_ref_store(&ref_store, None, &RequestOptions::default())
        .unwrap();

    cluster.stop_node(node_id);
    cluster.stop();
}
