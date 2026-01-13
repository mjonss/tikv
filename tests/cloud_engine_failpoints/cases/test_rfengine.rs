// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use rfengine::RFENGINE_DFS_WORKER_HEALTHY_GAUGE;
use test_cloud_server::{
    ServerCluster, alloc_node_id_vec, client::MutateOptions, must_wait, oss::prepare_dfs,
};
use tikv::config::TikvConfig;
use tikv_util::{config::ReadableSize, info};

use crate::cases::{i_to_keyspace_key, random_value_1kb};

#[test]
fn test_rfengine_recover_from_unhealthy() {
    const KEYSPACE_ID: u32 = 1;
    let s3fs_put_wal_chunks_error_fp = "s3fs_put_wal_chunks_error";

    test_util::init_log_for_test();
    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("t");

    // Create a cluster with a single store. As we use the metric
    // RFENGINE_DFS_WORKER_HEALTHY_GAUGE to detect the unhealthy, which is shared by
    // all stores.
    let mut cluster = ServerCluster::new(alloc_node_id_vec(1), |_, conf: &mut TikvConfig| {
        conf.dfs = dfs_config.clone();
        conf.rfengine.target_file_size = ReadableSize::kb(512);
        conf.rfengine.wal_chunk_target_file_size = ReadableSize::kb(32);
        conf.rfengine.wal_sync_dir = "${data-dir}/raft-wal".to_string();
        conf.rfengine.lightweight_backup = true;
    });
    cluster.wait_region_replicated(&[], 1);
    let mut client = cluster.new_client();
    client.split_keyspace(KEYSPACE_ID);

    let gen_key = i_to_keyspace_key(KEYSPACE_ID);
    client.put_kv(0..1000, &gen_key, random_value_1kb);
    client.verify_data_with_ref_store();

    // Workload to continuously generate WAL.
    let (tx, rx) = std::sync::mpsc::sync_channel(0);
    let workload_handle = {
        let mut client = cluster.new_client();
        std::thread::spawn(move || {
            while let Err(std::sync::mpsc::TryRecvError::Empty) = rx.try_recv() {
                let _ = client.try_put_kv(
                    10000..10050,
                    &gen_key,
                    random_value_1kb,
                    MutateOptions::default(),
                );
                let _ = client.try_del_kv(10000..10050, &gen_key, MutateOptions::default());
            }
        })
    };

    fail::cfg(s3fs_put_wal_chunks_error_fp, "return").unwrap();
    must_wait(
        || RFENGINE_DFS_WORKER_HEALTHY_GAUGE.get() == 0,
        30,
        || "wait for dfs worker unhealthy timeout".to_string(),
    );
    info!("DFS worker is unhealthy");

    fail::remove(s3fs_put_wal_chunks_error_fp);
    must_wait(
        || RFENGINE_DFS_WORKER_HEALTHY_GAUGE.get() == 1,
        30,
        || "wait for dfs worker healthy timeout".to_string(),
    );
    info!("DFS worker is healthy");

    tx.send(()).unwrap();
    workload_handle.join().unwrap();

    client.verify_data_with_ref_store();
    cluster.stop();
    oss.shutdown();
}
