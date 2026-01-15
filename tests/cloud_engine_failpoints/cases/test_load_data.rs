// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{assert_matches::assert_matches, fs, sync::Arc, time::Duration};

use api_version::ApiV2;
use futures::executor::block_on;
use kvengine::table::{ChecksumType, ZSTD_COMPRESSION};
use load_data::task::{LoadDataConfig, LoadDataContext};
use pd_client::PdClient;
use rand::Rng;
use test_cloud_server::{
    ServerCluster, alloc_node_id_vec,
    client::RequestOptions,
    load_data::{Error as LoadDataError, build, cleanup, init_task, put_chunks, try_wait_finished},
    oss::prepare_dfs,
};
use tikv::config::TikvConfig;

use crate::cases::{i_to_row_key, i_to_val};

const KEYSPACE_ID: u32 = 123;
const TABLE_ID: i64 = 5;
const DATA_COUNT: usize = 2000;
const WRITER_COUNT: usize = 5;
const DATA_BATCH_SIZE: usize = 10;
const COMPRESSION_TYPE: u8 = ZSTD_COMPRESSION;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn test_load_data() {
    test_util::init_log_for_test();

    let low_space_fp = if rand::thread_rng().gen_bool(0.5) {
        "engine_is_low_space" // leader is low space.
    } else {
        "is_store_low_space" // follower is low space.
    };

    let (temp_dir, _oss, dfs_conf) = prepare_dfs("test");
    let base_dir = temp_dir.path();

    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("load_data_worker")
            .enable_all()
            .build()
            .unwrap(),
    );
    let node_ids = alloc_node_id_vec(3);
    let mut cluster = ServerCluster::new(node_ids.clone(), |_, conf: &mut TikvConfig| {
        conf.dfs = dfs_conf.clone();
    });
    cluster.wait_region_replicated(&[], 3);
    let pd_client = cluster.get_pd_client();
    let mut client = cluster.new_client();
    client.split_keyspace(KEYSPACE_ID);

    // Init task.
    // Total data size is about 1.3MB = 10000 * (23 + 120)
    let load_data_config = LoadDataConfig {
        kvpairs_worker_num: 2,
        building_worker_num: 2,
        max_in_mem_size: 1024, // 1KB
        flush_batch_size: 512,
        block_size: 1024,
        sst_file_size: 4 * 1024,
        region_size: 16 * 1024,
        coarse_split_size: 128 * 1024,
        enable_checkpoint: false,
        rg_config: None,
        checksum_type: ChecksumType::Crc32,
    };

    let dfs = kvengine::dfs::new_dfs_from_config(dfs_conf);
    let master_key = cluster.get_kvengine(node_ids[0]).get_master_key();
    let load_data_dir = base_dir.join("load_data");
    fs::create_dir_all(&load_data_dir).unwrap();
    let start_ts = block_on(pd_client.get_tso()).unwrap().into_inner();
    let commit_ts = block_on(pd_client.get_tso()).unwrap().into_inner();
    let load_data_ctx = LoadDataContext {
        dir: load_data_dir,
        dfs,
        pd: pd_client,
        runtime,
        master_key,
    };

    let (scheduler, worker_handle) =
        init_task(load_data_config, load_data_ctx, start_ts, commit_ts);

    // Put chunks.
    let keyspace_prefix = ApiV2::get_keyspace_prefix_by_id(KEYSPACE_ID);
    let i_to_key = move |i: usize| -> Vec<u8> { i_to_row_key(&keyspace_prefix, TABLE_ID, i) };
    let i_to_row_id = move |i: usize| -> Vec<u8> { i.to_be_bytes().to_vec() };
    let ref_store = put_chunks(
        &scheduler,
        WRITER_COUNT,
        DATA_COUNT,
        DATA_BATCH_SIZE,
        i_to_key,
        i_to_val,
        i_to_row_id,
        DEFAULT_TIMEOUT,
        |_| 0,
    );

    fail::cfg(low_space_fp, "return").unwrap();

    // Build. Must be timeout due to low space.
    let res = build(&scheduler, COMPRESSION_TYPE, Duration::from_secs(5));
    assert_matches!(res.unwrap_err(), LoadDataError::Timeout(_));

    // Remove fp and retry again.
    fail::remove(low_space_fp);
    try_wait_finished(&scheduler, DEFAULT_TIMEOUT).unwrap();

    // Cleanup.
    cleanup(&scheduler, worker_handle);

    // Verify data consistency.
    let verified_count = client
        .verify_data_with_given_ref_store(&ref_store, None, &RequestOptions::default())
        .expect("verify_data_with_given_ref_store");
    assert_eq!(verified_count, (DATA_COUNT, 0));

    cluster.stop();
}
