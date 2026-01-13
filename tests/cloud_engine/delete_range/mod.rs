// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{thread, time::Duration};

use api_version::ApiV2;
use pd_client::PdClient;
use test_cloud_server::{ServerCluster, must_wait, oss};
use tikv::config::TikvConfig;
use tikv_util::config::ReadableDuration;

use crate::{alloc_node_id, destroy_range};

#[test]
fn test_delete_range() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let mut cluster = ServerCluster::new(vec![node_id], |_, _| {});
    let mut client = cluster.new_client();

    // split keyspace region for inner key offset to take effect.
    client.split_keyspace(ApiV2::get_u32_keyspace_id_by_key("x123".as_bytes()).unwrap_or_default());

    // insert "key_100".."key_500"
    client.put_kv(100..500, i_to_key_with_prefix, i_to_val);
    let store_id = cluster.get_stores()[0];
    destroy_range(&mut client, store_id, "x123key_20".as_bytes());
    let snap = cluster.get_snap(node_id, "x123key_".as_bytes());
    assert!(!snap.has_data_in_prefix("x123key_200".as_bytes()));
    assert!(!snap.has_data_in_prefix("x123key_209".as_bytes()));

    assert!(snap.has_data_in_prefix("x123key_24".as_bytes()));

    destroy_range(&mut client, store_id, "x123key_22".as_bytes());
    let snap = cluster.get_snap(node_id, "x123key_".as_bytes());
    assert!(!snap.has_data_in_prefix("x123key_20".as_bytes()));
    assert!(!snap.has_data_in_prefix("x123key_22".as_bytes()));

    assert!(snap.has_data_in_prefix("x123key_21".as_bytes()));

    destroy_range(&mut client, store_id, "x123key_2".as_bytes());
    let snap = cluster.get_snap(node_id, "x123key_".as_bytes());
    assert!(!snap.has_data_in_prefix("x123key_20".as_bytes()));
    assert!(!snap.has_data_in_prefix("x123key_21".as_bytes()));
    assert!(!snap.has_data_in_prefix("x123key_22".as_bytes()));
    assert!(!snap.has_data_in_prefix("x123key_23".as_bytes()));

    assert!(snap.has_data_in_prefix("x123key_3".as_bytes()));
    cluster.stop();
}

#[test]
fn test_delete_range_recover() {
    test_util::init_log_for_test();
    let node_ids = &[alloc_node_id(), alloc_node_id(), alloc_node_id()];
    let (_temp_dir, oss, dfs_config) = oss::prepare_dfs("oss_");
    let mut cluster = ServerCluster::new(node_ids.to_vec(), |_, conf: &mut TikvConfig| {
        conf.dfs = dfs_config.clone();
    });
    let region = cluster.get_pd_client().get_region_info(&[]).unwrap();
    let leader_store_id = region.leader.unwrap().store_id;
    let &stop_node_id = node_ids
        .iter()
        .find(|&&node_id| cluster.get_store_id(node_id) != leader_store_id)
        .unwrap();
    oss.set_delay(Duration::from_millis(10));
    for _ in 0..3 {
        let mut client = cluster.new_client();
        for i in 0..10 {
            client.put_kv(i * 100..i * 100 + 10, i_to_key_with_prefix, i_to_val);
        }
        destroy_range(&mut client, leader_store_id, "x123key_".as_bytes());
        client.put_kv(0..10, i_to_key, i_to_val);
        cluster.stop_node(stop_node_id);
        cluster.start_node(stop_node_id, |_, _| {});
        thread::sleep(Duration::from_secs(1));
    }
    cluster.stop();
}

#[test]
fn test_delete_range_lost_table_delete() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let cluster = ServerCluster::new(vec![node_id], |_, conf| {
        // 3 seconds max delay.
        conf.raft_store.local_file_gc_timeout = ReadableDuration(Duration::from_secs(3));
        conf.raft_store.local_file_gc_tick_interval = ReadableDuration(Duration::from_secs(1));
        conf.kvengine.max_del_range_delay = ReadableDuration(Duration::from_secs(3));
    });
    let mut client = cluster.new_client();
    client.split_keyspace(ApiV2::get_u32_keyspace_id_by_key("x123".as_bytes()).unwrap_or_default());

    client.put_kv(200..350, i_to_key_with_prefix, i_to_val);
    client.put_kv(350..450, i_to_key_with_prefix, i_to_val);
    client.put_kv(450..500, i_to_key_with_prefix, i_to_val);
    client.split("x123key_25".as_bytes());
    client.split("x123key_35".as_bytes());
    client.split("x123key_45".as_bytes());
    let store_id = cluster.get_stores()[0];
    destroy_range(&mut client, store_id, "x123key_2".as_bytes());
    destroy_range(&mut client, store_id, "x123key_3".as_bytes());
    destroy_range(&mut client, store_id, "x123key_4".as_bytes());
    must_wait(
        || {
            let all_stats = cluster.get_kvengine(node_id).get_all_shard_stats();
            all_stats.into_iter().map(|s| s.total_size).sum::<u64>() == 0
        },
        15,
        || "wait for total size to be 0".to_string(),
    );
}

#[test]
fn test_delete_range_delay() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let cluster = ServerCluster::new(vec![node_id], |_, conf| {
        // 10 seconds max delay.
        conf.raft_store.local_file_gc_timeout = ReadableDuration(Duration::from_secs(5));
        conf.raft_store.local_file_gc_tick_interval = ReadableDuration(Duration::from_secs(1));
        conf.kvengine.max_del_range_delay = ReadableDuration(Duration::from_secs(10));
    });
    let mut client = cluster.new_client();
    for i in 1..=30 {
        client.split(&i_to_key_with_prefix(i * 100));
        client.put_kv(i * 100..(i + 1) * 100, i_to_key_with_prefix, i_to_val);
    }
    let store_id = cluster.get_stores()[0];
    // 30 regions randomly delay delete range.
    destroy_range(&mut client, store_id, "x123key_".as_bytes());
    let kvengine = cluster.get_kvengine(node_id);

    // Most of the del_prefixes are not destroyed.
    let has_del_prefix_count = get_del_prefix_shard_count(&kvengine);
    assert!(has_del_prefix_count > 25);

    // After half of the max delay duration, some of the del_prefixes are destroyed.
    thread::sleep(Duration::from_secs(5));
    let has_del_prefix_count = get_del_prefix_shard_count(&kvengine);
    assert!(
        has_del_prefix_count > 2 && has_del_prefix_count < 28,
        "has_del_prefix_count: {}",
        has_del_prefix_count
    );

    // After more than the max delay duration, all of the del_prefixes are
    // destroyed.
    thread::sleep(Duration::from_secs(7));
    let has_del_prefix_count = get_del_prefix_shard_count(&kvengine);
    assert_eq!(has_del_prefix_count, 0);
}

fn get_del_prefix_shard_count(kvengine: &kvengine::Engine) -> usize {
    kvengine
        .get_all_shard_stats()
        .iter()
        .filter(|stat| stat.has_del_prefixes)
        .count()
}

fn i_to_key(i: usize) -> Vec<u8> {
    format!("xkey_{:03}", i).into_bytes()
}

fn i_to_val(i: usize) -> Vec<u8> {
    format!("val_{:03}", i).into_bytes()
}

fn i_to_key_with_prefix(i: usize) -> Vec<u8> {
    format!("x123key_{:03}", i).into_bytes()
}
