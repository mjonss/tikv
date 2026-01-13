// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{str::FromStr, thread, time::Duration};

use cloud_encryption::KeyspaceEncryptionConfig;
use futures::executor::block_on;
use http::{Request, Uri};
use hyper::Body;
use kvengine::ShardStats;
use pd_client::PdClient;
use rstest::rstest;
use test_cloud_server::{ServerCluster, alloc_node_id_vec, must_wait};
use test_util::init_log_for_test;
use tikv_util::{codec::bytes::encode_bytes, info};

use crate::generate_keyspace_key;

#[rstest]
#[case(false)]
#[case::encryption(true)]
fn test_unsafe_recover(#[case] encryption: bool) {
    init_log_for_test();

    let keyspace_id = 42;

    let nodes = alloc_node_id_vec(3);
    let mut cluster = ServerCluster::new(nodes.clone(), |_, _| {});
    cluster.wait_region_replicated(&[], 3);

    let pd_client = cluster.get_pd_client();
    let mut client = cluster.new_client();
    let i_to_key = generate_keyspace_key(keyspace_id);

    let cfg = KeyspaceEncryptionConfig {
        enabled: encryption,
    };
    pd_client.set_keyspace_encryption(keyspace_id, cfg).unwrap();
    client.split_keyspaces([keyspace_id]);
    cluster.wait_region_replicated(&i_to_key(0), 3);

    let encoded_split_key = encode_bytes(&i_to_key(100));
    block_on(
        pd_client
            .split_regions_with_retry(vec![encoded_split_key.clone()], Duration::from_secs(30)),
    )
    .unwrap();
    cluster.wait_region_replicated(&i_to_key(100), 3);

    let encoded_left_key = encode_bytes(&i_to_key(0));
    let left_region = pd_client.get_region(&encoded_left_key).unwrap();
    let right_region = pd_client.get_region(&encoded_split_key).unwrap();

    let target_node_id = nodes[2];
    let mut right_stats = ShardStats::default();
    must_wait(
        || {
            right_stats = cluster
                .get_kvengine(target_node_id)
                .get_shard_stat_opt(right_region.id)
                .unwrap();
            right_stats.flushed
        },
        10,
        || "wait for initial flushed timeout".to_string(),
    );
    assert_eq!(right_stats.encryption, encryption);

    // Perform unsafe recover.
    {
        let mut stopped_node = None;
        cluster.create_region_panic_mark(target_node_id, right_region.id);
        cluster.restart_node(target_node_id, Duration::from_secs(0), false, |_, _| {});
        thread::sleep(Duration::from_secs(1));

        request_unsafe_recover(&cluster, target_node_id, right_region.id);
        // Stop one non-target node so quorum is lost while the target is down,
        // preventing a competing leader from being elected with a stale log.
        let current_leader_node_id = block_on(pd_client.get_region_leader_by_id(right_region.id))
            .ok()
            .flatten()
            .map(|(_region, leader)| {
                *nodes
                    .iter()
                    .find(|node_id| cluster.get_store_id(**node_id) == leader.store_id)
                    .unwrap()
            });
        let node_to_stop = match current_leader_node_id {
            Some(leader_node_id) if leader_node_id != target_node_id => Some(leader_node_id),
            _ => nodes
                .iter()
                .copied()
                .find(|node_id| *node_id != target_node_id),
        };
        if let Some(node_id) = node_to_stop {
            cluster.stop_node(node_id);
            stopped_node = Some(node_id);
        }
        cluster.remove_region_panic_mark(target_node_id, right_region.id);
        cluster.restart_node(target_node_id, Duration::from_secs(0), false, |_, _| {});
        thread::sleep(Duration::from_secs(3));

        let target_store_id = cluster.get_store_id(target_node_id);
        must_wait(
            || {
                let Some((_region, leader)) =
                    block_on(pd_client.get_region_leader_by_id(right_region.id))
                        .ok()
                        .flatten()
                else {
                    return false;
                };
                leader.store_id == target_store_id
            },
            60,
            || format!("wait for leader on store {}", target_store_id),
        );
        if let Some(node_id) = stopped_node {
            cluster.restart_node(node_id, Duration::from_secs(0), false, |_, _| {});
            cluster.wait_region_replicated(&i_to_key(100), 3);
        }

        right_stats = cluster
            .get_kvengine(target_node_id)
            .get_shard_stat_opt(right_region.id)
            .unwrap();
        assert_eq!(right_stats.encryption, encryption);
        pd_client.must_merge(right_region.id, left_region.id);
    }

    cluster.stop();
}

fn request_unsafe_recover(cluster: &ServerCluster, node_id: u16, region_id: u64) {
    let _enter = cluster.get_dfs().unwrap().get_runtime().enter();
    let cluster_id = cluster.get_pd_client().get_cluster_id().unwrap();
    let status_addr = cluster.status_addr(node_id);
    let addr = format!(
        "http://{status_addr}/unsafe_recover/clear?cluster_id={cluster_id}&region_id={region_id}"
    );
    let uri = Uri::from_str(&addr).unwrap();

    let req = Request::post(uri).body(Body::empty()).unwrap();
    let client = hyper::Client::new();
    let resp = block_on(client.request(req)).unwrap();
    assert!(resp.status().is_success());
    let resp = block_on(hyper::body::to_bytes(resp.into_body())).unwrap();
    let resp = String::from_utf8_lossy(&resp);
    info!("request_unsafe_recover"; "req" => &addr, "resp" => resp.as_ref());
}
