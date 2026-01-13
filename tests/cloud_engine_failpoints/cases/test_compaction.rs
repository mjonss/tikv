// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{thread, time::Duration};

use test_cloud_server::{ServerCluster, must_wait};
use tikv_util::config::{ReadableDuration, ReadableSize};

use super::{i_to_key, i_to_val};
use crate::cases::alloc_node_id;

#[test]
fn test_retry_failed_flush() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let mut cluster = ServerCluster::new(vec![node_id], |_, cfg| {
        cfg.rocksdb.writecf.write_buffer_size = ReadableSize::kb(16);
    });

    let flush_fp = "kvengine_flush_normal";
    let mut client = cluster.new_client();
    let region_id = client.get_region_id(&[]);
    fail::cfg(flush_fp, "return").unwrap();
    client.put_kv(0..100, i_to_key, i_to_val);
    client.put_kv(100..200, i_to_key, i_to_val);
    thread::sleep(Duration::from_secs(1));
    assert_eq!(
        cluster
            .get_kvengine(node_id)
            .get_shard_stat(region_id)
            .l0_table_count,
        0
    );
    fail::remove(flush_fp);
    must_wait(
        || {
            cluster
                .get_kvengine(node_id)
                .get_shard_stat(region_id)
                .l0_table_count
                > 0
        },
        3,
        || "failed to flush memtables in time".to_string(),
    );
    cluster.stop();
}

#[test]
fn test_resume_failed_compaction() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let mut cluster = ServerCluster::new(vec![node_id], |_, cfg| {
        cfg.rocksdb.writecf.write_buffer_size = ReadableSize::kb(4);
        cfg.raft_store.peer_long_check_interval = ReadableDuration::millis(500);
    });
    let compact_fp = "kvengine_compact";
    fail::cfg(compact_fp, "return").unwrap();
    let mut client = cluster.new_client();
    let region_id = client.get_region_id(&[]);
    for i in 0..10 {
        client.put_kv(i * 100..(i + 1) * 100, i_to_key, i_to_val);
    }
    let kv = cluster.get_kvengine(node_id);
    let stats = kv.get_shard_stat(region_id);
    assert!(stats.compaction_score > 1.0);
    fail::remove(compact_fp);
    must_wait(
        || kv.get_shard_stat(region_id).compaction_score == 0.0,
        10,
        || "failed to resume compaction in time".to_string(),
    );
    cluster.stop();
}
