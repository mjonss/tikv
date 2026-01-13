// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use api_version::ApiV2;
use kvengine::{CF_LEVELS, KvEnginePerKeyspaceConfig, WRITE_CF};
use kvproto::{
    metapb::Store,
    pdpb::CheckPolicy,
    raft_cmdpb::{RaftCmdRequest, RaftRequestHeader},
};
use pd_client::PdClient;
use rfstore::store::CustomBuilder;
use test_cloud_server::ServerCluster;
use test_pd_client::PdClientExt;
use tikv_util::time::Instant;

use crate::{
    alloc_node_id, i_to_key_with_keyspace, i_to_val_with_size, request_major_compact_on_store,
};

#[test]
fn test_per_keyspace_config() {
    test_util::init_log_for_test();
    let mut nodes = vec![];
    let node_cnt = 3;
    for _ in 0..node_cnt {
        nodes.push(alloc_node_id());
    }
    let mut cluster = ServerCluster::new(nodes.clone(), |_, conf| {
        conf.kvengine.blob_table_build_options.min_blob_size = 1000;
        conf.kvengine
            .blob_table_build_options
            .target_blob_table_size = 1;
        conf.kvengine.per_keyspace_configs = vec![
            KvEnginePerKeyspaceConfig {
                keyspace: 1,
                split_size_factor: 1.0,
                enable_blob: true,
            },
            KvEnginePerKeyspaceConfig {
                keyspace: 2,
                split_size_factor: 1.0,
                enable_blob: false,
            },
            KvEnginePerKeyspaceConfig {
                keyspace: 3,
                split_size_factor: 1.0,
                enable_blob: true,
            },
        ];
    });
    cluster.wait_region_replicated(&[], 3);
    let mut client = cluster.new_client();
    let pd_client = cluster.get_pd_client();
    pd_client.disable_default_operator();

    // Prepare 2 keyspaces, each has 2 regions, each region has 500 keys.
    let region0 = pd_client.get_all_regions().first().unwrap().clone();
    let keys = vec![
        ApiV2::get_txn_keyspace_prefix(1),
        ApiV2::get_txn_keyspace_prefix(2),
        ApiV2::get_txn_keyspace_prefix(3),
        ApiV2::get_txn_keyspace_prefix(4),
        ApiV2::get_txn_keyspace_prefix(5),
    ];
    let split_keys = keys
        .into_iter()
        .map(|k| txn_types::Key::from_raw(&k).into_encoded())
        .collect::<Vec<_>>();
    pd_client.must_split_region(region0, CheckPolicy::Usekey, split_keys.clone());
    cluster.wait_pd_region_min_count(split_keys.len() + 1);
    client.put_kv(0..1000, i_to_key_with_keyspace(1), i_to_val_with_size(3000));
    client.put_kv(0..1000, i_to_key_with_keyspace(2), i_to_val_with_size(3000));
    client.put_kv(0..1000, i_to_key_with_keyspace(3), i_to_val_with_size(3000));
    client.put_kv(0..1000, i_to_key_with_keyspace(4), i_to_val_with_size(3000));
    let ks1 = pd_client.get_region(&split_keys[0]).unwrap();
    let ks2 = pd_client.get_region(&split_keys[1]).unwrap();
    let ks3 = pd_client.get_region(&split_keys[2]).unwrap();
    let ks4 = pd_client.get_region(&split_keys[3]).unwrap();

    let wait_for_memtable_flush =
        |cluster: &ServerCluster, node_ids: &[u16], region_id: u64, timeout: Duration| {
            let mut curr_shard_stats;
            let mut flushed;
            let start = Instant::now_coarse();
            loop {
                curr_shard_stats = node_ids
                    .iter()
                    .map(|id| cluster.get_kvengine(*id).get_shard_stat(region_id))
                    .collect::<Vec<_>>();
                flushed = curr_shard_stats
                    .iter()
                    .all(|curr| curr.mem_table_size == 0 && curr.mem_table_count == 1);
                if flushed {
                    break;
                }
                if start.saturating_elapsed() >= timeout {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1000));
            }
            flushed
        };

    // Trigger switching and flushing memtable.
    let flush_memtable = |cluster: &ServerCluster, node_ids: &[u16], region_id: u64| {
        let mut client = cluster.new_client();
        let ctx = client.new_rpc_ctx(region_id, b"").unwrap();
        let mut req = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(ctx.get_region_id());
        header.set_peer(ctx.get_peer().clone());
        header.set_region_epoch(ctx.get_region_epoch().clone());
        header.set_term(6);
        req.set_header(header);
        let mut custom_builder = CustomBuilder::new();
        custom_builder.set_switch_mem_table(1);
        req.set_custom_request(custom_builder.build());
        cluster.send_raft_command(req);

        let flushed = wait_for_memtable_flush(cluster, node_ids, region_id, Duration::from_secs(6));
        assert!(flushed);
    };

    flush_memtable(&cluster, &nodes, ks1.get_id());
    flush_memtable(&cluster, &nodes, ks2.get_id());
    flush_memtable(&cluster, &nodes, ks3.get_id());
    flush_memtable(&cluster, &nodes, ks4.get_id());

    let stores: Vec<Store> = pd_client
        .get_all_stores(true)
        .unwrap()
        .into_iter()
        .filter(|s| {
            !s.get_labels().iter().any(|l| {
                l.key.to_lowercase() == "engine" && l.value.to_lowercase().starts_with("tiflash")
            })
        })
        .collect();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    for store in &stores {
        let query = "major_compact=true&keyspace_id=1";
        runtime.block_on(request_major_compact_on_store(store, query, false));
        let query = "major_compact=true&keyspace_id=2";
        runtime.block_on(request_major_compact_on_store(store, query, false));
        let query = "major_compact=true&keyspace_id=3";
        runtime.block_on(request_major_compact_on_store(store, query, false));
        let query = "major_compact=true&keyspace_id=4";
        runtime.block_on(request_major_compact_on_store(store, query, false));
    }

    let wait_for_major_compaction =
        |cluster: &ServerCluster, node_ids: &[u16], region_id: u64, timeout: Duration| {
            let mut curr_shard_stats;
            let mut major_compacted;
            let start = Instant::now_coarse();
            loop {
                curr_shard_stats = node_ids
                    .iter()
                    .map(|id| cluster.get_kvengine(*id).get_shard_stat(region_id))
                    .collect::<Vec<_>>();
                major_compacted = curr_shard_stats.iter().all(|curr| {
                    let bottom_most_level = curr.cfs[WRITE_CF].levels.last().unwrap();
                    curr.mem_table_size == 0
                        && curr.mem_table_count == 1
                        && curr.l0_table_count == 0
                        && bottom_most_level.level == CF_LEVELS[WRITE_CF]
                        && bottom_most_level.num_tables != 0
                });
                if major_compacted {
                    break;
                }
                if start.saturating_elapsed() >= timeout {
                    break;
                }
                std::thread::sleep(Duration::from_millis(200));
            }
            major_compacted
        };
    assert!(wait_for_major_compaction(
        &cluster,
        &nodes,
        ks1.get_id(),
        Duration::from_secs(6)
    ));
    assert!(wait_for_major_compaction(
        &cluster,
        &nodes,
        ks2.get_id(),
        Duration::from_secs(6)
    ));
    assert!(wait_for_major_compaction(
        &cluster,
        &nodes,
        ks3.get_id(),
        Duration::from_secs(6)
    ));
    assert!(wait_for_major_compaction(
        &cluster,
        &nodes,
        ks4.get_id(),
        Duration::from_secs(6)
    ));
    let num_blob_tables_in_shard = |cluster: &ServerCluster, node_ids: &[u16], region_id: u64| {
        node_ids
            .iter()
            .map(|id| {
                let shard_stat = cluster.get_kvengine(*id).get_shard_stat(region_id);
                shard_stat.blob_table_count
            })
            .sum::<usize>()
    };
    assert_ne!(num_blob_tables_in_shard(&cluster, &nodes, ks1.get_id()), 0);
    assert_eq!(num_blob_tables_in_shard(&cluster, &nodes, ks2.get_id()), 0);
    assert_ne!(num_blob_tables_in_shard(&cluster, &nodes, ks3.get_id()), 0);
    assert_eq!(num_blob_tables_in_shard(&cluster, &nodes, ks4.get_id()), 0);

    cluster.stop();
}
