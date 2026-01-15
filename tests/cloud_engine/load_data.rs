// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, fs, sync::Arc, time::Duration};

use api_version::ApiV2;
use bytes::Bytes;
use cloud_encryption::KeyspaceEncryptionConfig;
use futures::executor::block_on;
use kvengine::{
    dfs::DFSConfig,
    table::{ChecksumType, ZSTD_COMPRESSION},
};
use kvenginepb::ChangeSet;
use load_data::task::{LoadDataConfig, LoadDataContext};
use log_wrappers::hex;
use pd_client::PdClient;
use protobuf::Message;
use test_cloud_server::{
    ServerCluster,
    client::{RefStore, RequestOptions},
    load_data::{
        Error as LoadDataError, Result as LoadDataResult, build, cleanup, init_task, put_chunks,
    },
    oss::ObjectStorageService,
};
use tidb_query_datatype::codec::table;
use tikv::config::TikvConfig;
use tikv_util::{codec::bytes::encode_bytes, info};

use crate::{alloc_node_id_vec, request_major_compact_on_store};

const KEYSPACE_ID: u32 = 123;
const DATA_COUNT: usize = 2000;
const WRITER_COUNT: usize = 5;
const DATA_BATCH_SIZE: usize = 10;
const COMPRESSION_TYPE: u8 = ZSTD_COMPRESSION;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn test_load_data() {
    test_util::init_log_for_test();
    let base_dir = tempfile::Builder::new()
        .prefix("test_load_data")
        .tempdir()
        .unwrap();

    let oss_dir = base_dir.path().join("oss");
    let mut oss = ObjectStorageService::new(oss_dir);
    oss.start_server();
    let dfs_conf = DFSConfig {
        prefix: "load_data".to_string(),
        s3_endpoint: format!("http://127.0.0.1:{}", oss.port()),
        s3_key_id: "admin".to_string(),
        s3_secret_key: "admin".to_string(),
        s3_bucket: "load_data".to_string(),
        s3_region: "local".to_string(),
        zstd_compression_level: "3".to_string(),
        ..Default::default()
    };

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
    match pd_client.set_keyspace_encryption(KEYSPACE_ID, KeyspaceEncryptionConfig { enabled: true })
    {
        Ok(_) => {}
        Err(err) if pd_client::grpc_error_is_unimplemented(&err) => {
            info!("set_keyspace_encryption is not supported, skip");
        }
        Err(err) => {
            panic!("set_keyspace_encryption failed: {:?}", err)
        }
    }
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
    let load_data_dir = base_dir.path().join("load_data");
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

    // write table 1 and table 11, then trigger major compaction to build L3 file.
    for i in (0..100).step_by(10) {
        let key_prefix_1 = table_key_prefix(1);
        let key_prefix_11 = table_key_prefix(11);
        client.put_kv(
            i..i + 10,
            |i| i_to_key_with_prefix(&key_prefix_1, i),
            i_to_val,
        );
        client.put_kv(
            i..i + 10,
            |i| i_to_key_with_prefix(&key_prefix_11, i),
            i_to_val,
        );
    }
    let stores = client.pd_client().get_all_stores(true).unwrap();
    for store in &stores {
        let query = format!("major_compact=true&keyspace_id={}", KEYSPACE_ID);
        load_data_ctx
            .runtime
            .block_on(request_major_compact_on_store(store, query.as_str(), false))
    }

    let (scheduler, worker_handle) =
        init_task(load_data_config, load_data_ctx, start_ts, commit_ts);

    // Put chunks.
    let keyspace_prefix = table_key_prefix(5);
    let i_to_key = move |i: usize| -> Vec<u8> { i_to_key_with_prefix(&keyspace_prefix, i) };
    let i_to_row_id = move |i: usize| -> Vec<u8> { i.to_be_bytes().to_vec() };
    let dup_count_fn = |i| {
        if i % DATA_BATCH_SIZE == 0 {
            i % 3 + 1
        } else {
            0
        }
    };
    let ref_store = put_chunks(
        &scheduler,
        WRITER_COUNT,
        DATA_COUNT,
        DATA_BATCH_SIZE,
        i_to_key,
        i_to_val,
        i_to_row_id,
        DEFAULT_TIMEOUT,
        dup_count_fn,
    );

    // Build.
    build(&scheduler, COMPRESSION_TYPE, DEFAULT_TIMEOUT).unwrap();
    let states = scheduler.states();
    assert_eq!(
        states.duplicated_entries.len(),
        DATA_COUNT / DATA_BATCH_SIZE
    );
    let dup_key_prefix = hex::hex_encode(ApiV2::get_txn_keyspace_prefix(KEYSPACE_ID));
    for (i, dup_entry) in states.duplicated_entries.iter().enumerate() {
        assert!(dup_entry.key.starts_with(dup_key_prefix.as_str()));

        let idx = DATA_BATCH_SIZE * i;
        let dup_count = dup_count_fn(idx);
        for j in 0..=dup_count {
            assert_eq!(
                dup_entry.values[j],
                hex::hex_encode(i_to_val(idx + j)),
                "i={} j={}",
                i,
                j
            );
        }
    }

    // Cleanup.
    cleanup(&scheduler, worker_handle);

    // Verify data consistency.
    let verified_count = client
        .verify_data_with_given_ref_store(&ref_store, None, &RequestOptions::default())
        .expect("verify_data_with_given_ref_store");
    assert_eq!(verified_count, (DATA_COUNT, 0));

    // Verify that a `table create` contains only one table id
    verify_table_creates(&cluster);

    cluster.stop();
}

#[test]
fn test_load_data_overlap() {
    test_util::init_log_for_test();

    let base_dir = tempfile::Builder::new()
        .prefix("test_load_data")
        .tempdir()
        .unwrap();

    let oss_dir = base_dir.path().join("oss");
    let mut oss = ObjectStorageService::new(oss_dir);
    oss.start_server();
    let dfs_conf = DFSConfig {
        prefix: "load_data".to_string(),
        s3_endpoint: format!("http://127.0.0.1:{}", oss.port()),
        s3_key_id: "admin".to_string(),
        s3_secret_key: "admin".to_string(),
        s3_bucket: "load_data".to_string(),
        s3_region: "local".to_string(),
        zstd_compression_level: "3".to_string(),
        ..Default::default()
    };

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

    let do_load_data = || -> LoadDataResult<RefStore> {
        let load_data_dir = base_dir.path().join("load_data");
        fs::create_dir_all(&load_data_dir).unwrap();
        let start_ts = block_on(pd_client.get_tso()).unwrap().into_inner();
        let commit_ts = block_on(pd_client.get_tso()).unwrap().into_inner();
        let load_data_ctx = LoadDataContext {
            dir: load_data_dir,
            dfs: dfs.clone(),
            pd: pd_client.clone(),
            runtime: runtime.clone(),
            master_key: master_key.clone(),
        };
        let (scheduler, worker_handle) =
            init_task(load_data_config.clone(), load_data_ctx, start_ts, commit_ts);

        // Put chunks.
        let keyspace_prefix = table_key_prefix(5);
        let i_to_key = move |i: usize| -> Vec<u8> { i_to_key_with_prefix(&keyspace_prefix, i) };
        let i_to_row_id = move |i: usize| -> Vec<u8> { i.to_be_bytes().to_vec() };
        let ref_store = put_chunks(
            &scheduler,
            WRITER_COUNT,
            DATA_COUNT,
            DATA_BATCH_SIZE,
            i_to_key,
            i_to_val,
            i_to_row_id,
            Duration::from_secs(10),
            |_| 0,
        );

        // Build.
        build(&scheduler, COMPRESSION_TYPE, Duration::from_secs(10))?;

        // Cleanup.
        cleanup(&scheduler, worker_handle);

        Ok(ref_store)
    };

    // Normal load data.
    let ref_store0 = do_load_data().unwrap();
    let verified_count = client
        .verify_data_with_given_ref_store(&ref_store0, None, &RequestOptions::default())
        .expect("verify_data_with_given_ref_store");
    assert_eq!(verified_count, (DATA_COUNT, 0));

    // Ingest overlap data.
    match do_load_data().unwrap_err() {
        LoadDataError::Canceled(msg) => {
            assert!(msg.contains("Ingest::Overlap: region has overlap data"))
        }
        err => panic!("unexpected error: {:?}", err),
    }

    // Verify no data corruption.
    let verified_count = client
        .verify_data_with_given_ref_store(&ref_store0, None, &RequestOptions::default())
        .expect("verify_data_with_given_ref_store");
    assert_eq!(verified_count, (DATA_COUNT, 0));

    cluster.stop();
}

// 120 bytes
fn i_to_val(i: usize) -> Vec<u8> {
    format!("val_{:08}", i).repeat(10).into_bytes()
}

fn i_to_key_with_prefix(prefix: &[u8], i: usize) -> Vec<u8> {
    let mut key = prefix.to_vec();
    // load_data will parse table id from key
    key.extend(table::encode_row_key((i / 100 + 1) as i64, i as i64));
    key
}

fn table_key_prefix(table_id: u64) -> Vec<u8> {
    let mut key = vec![];
    key.extend(ApiV2::get_txn_keyspace_prefix(KEYSPACE_ID));
    key.extend(table::TABLE_PREFIX);
    key.extend_from_slice(&table_id.to_be_bytes());
    key.extend(table::RECORD_PREFIX_SEP);
    key
}

// verify_table_creats will verify that a `table create` contains only one table
// id.
fn verify_table_creates(cluster: &ServerCluster) {
    let pd_client = cluster.get_pd_client();
    let nodes = cluster.get_nodes();
    let mut store_ids: HashMap<u64, u16> = HashMap::new();
    for node in nodes {
        let store_id = cluster.get_store_id(node);
        store_ids.insert(store_id, node);
    }

    let keyspace_prefix = ApiV2::get_txn_keyspace_prefix(KEYSPACE_ID);
    let start_key = i_to_key_with_prefix(&keyspace_prefix, 0);
    let end_key = i_to_key_with_prefix(&keyspace_prefix, DATA_COUNT);
    let encoded_start_key = encode_bytes(&start_key);
    let encoded_end_key = encode_bytes(&end_key);
    let regions = block_on(pd_client.scan_regions(encoded_start_key, encoded_end_key, 0)).unwrap();
    for region in regions {
        let node = store_ids.get(&region.get_leader().get_store_id()).unwrap();
        let kvengine = cluster.get_kvengine(*node);
        let snap = kvengine
            .get_snap_access(region.get_region().get_id())
            .unwrap();
        let (_, cs_bin) = snap.marshal(
            &[(Bytes::from(start_key.clone()), Bytes::from(end_key.clone()))],
            false,
            false,
        );
        let mut cs = ChangeSet::default();
        cs.merge_from_bytes(&cs_bin).unwrap();
        let cs_snap = cs.get_snapshot();
        let table_creates = cs_snap.get_table_creates();
        for table_create in table_creates {
            let smallest = table_create.get_smallest();
            let biggest = table_create.get_biggest();
            let (smallest_table_id, biggest_table_id) = (
                table::decode_table_id(smallest).unwrap(),
                table::decode_table_id(biggest).unwrap(),
            );
            assert_eq!(smallest_table_id, biggest_table_id);
        }
    }
}
