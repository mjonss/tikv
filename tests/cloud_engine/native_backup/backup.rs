// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    assert_matches::assert_matches,
    iter::FromIterator,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    time::Duration,
};

use api_version::ApiV2;
use chrono::Utc;
use kvengine::dfs::{DFSConfig, S3Fs};
use native_br::{
    backup, backup_worker, common::get_all_incremental_backups, error::Error as BrError, restore,
    restore::RestoreConfig, restore_keyspace,
};
use rand::prelude::*;
use security::SecurityConfig;
use test_cloud_server::{
    ServerCluster, ServerClusterBuilder, alloc_node_id_vec, client,
    client::RequestOptions,
    oss::{ObjectStorageService, prepare_dfs},
    try_wait_result,
};
use test_pd_client::PdWrapper;
use tikv::config::TikvConfig;
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    info,
};
use tokio::runtime::Runtime;

use crate::{
    alloc_node_id,
    native_backup::{DummyStepReporter, random_value},
};

const CLUSTER_ID: u64 = 10000;
const NODES_SIZE: usize = 3;
const DATA_SIZE: usize = 2000;

const WAL_TARGET_SIZE: ReadableSize = ReadableSize::mb(1);

// TODO: test on more conditions (e.g. split and merge).

fn start_cluster_and_backup(
    backup_name: String,
    dfs_config: DFSConfig,
) -> (rfenginepb::ClusterBackupMeta, client::RefStore) {
    let nodes = Vec::from_iter((0..NODES_SIZE).map(|_| alloc_node_id()));
    let pd = PdWrapper::new_test(0, &SecurityConfig::default(), Some(CLUSTER_ID));
    let mut cluster = ServerClusterBuilder::new(nodes, |_, conf: &mut TikvConfig| {
        conf.dfs = dfs_config.clone();
        conf.rfengine.lightweight_backup = true;
        conf.rfengine.target_file_size = WAL_TARGET_SIZE;
        conf.rfengine.wal_chunk_target_file_size = ReadableSize::kb(128);
    })
    .pd(pd)
    .build();
    cluster.wait_region_replicated(&[], 3);
    let mut client = cluster.new_client();

    // split to 3 regions
    client.split(&i_to_key(DATA_SIZE / 3));
    client.split(&i_to_key(DATA_SIZE * 2 / 3));
    cluster.wait_pd_region_count(3);

    // import data
    client.put_kv(0..DATA_SIZE, i_to_key, i_to_val);
    client.verify_data_with_ref_store();

    // execute backup
    // `skip_keyspace_meta: true` as there is no ETCD in CI.
    // TODO: support testing for keyspace meta backup.
    let backup_config = backup::BackupConfig {
        dfs: dfs_config,
        backup_delay: ReadableDuration::secs(1),
        skip_keyspace_meta: true,
        ..Default::default()
    };
    let backup_ts = client.get_ts().into_inner();
    let (_, backup_meta) = backup::backup_cluster_with_ts(
        backup_config,
        backup_name,
        cluster.get_pd_client().as_ref(),
        backup_ts,
        None,
    )
    .expect("backup::backup_cluster");
    info!("backup_cluster result: {:?}", backup_meta);

    cluster.stop();

    let ref_store = client.dump_ref_store();
    assert_eq!(ref_store.len(), DATA_SIZE);

    (backup_meta, ref_store)
}

fn restore_cluster(
    backup_name: String,
    base_path: &Path,
    dfs_config: DFSConfig,
    backup_meta: rfenginepb::ClusterBackupMeta,
    ref_store: client::RefStore,
) {
    let get_storage_path =
        |node_id: u16| -> PathBuf { base_path.join(format!("restore_{node_id}")) };

    let mut nodes = Vec::with_capacity(backup_meta.get_stores().len());
    for store in backup_meta.get_stores() {
        let node_id = alloc_node_id();
        nodes.push(node_id);

        let raft_db_path = get_storage_path(node_id).join("raft");
        let restore_config = restore::RestoreConfig {
            dfs: dfs_config.clone(),
            wal_target_size: WAL_TARGET_SIZE,
            ..Default::default()
        };
        restore::restore_tikv(
            &restore_config,
            backup_name.to_string(),
            store.get_store_id(),
            100,
            raft_db_path.to_str().unwrap(),
        );
    }

    let pd = PdWrapper::new_test(0, &SecurityConfig::default(), Some(CLUSTER_ID));
    let mut cluster = ServerClusterBuilder::new(nodes, |node_id, conf: &mut TikvConfig| {
        conf.storage.data_dir = get_storage_path(node_id).to_str().unwrap().to_string();
        conf.dfs = dfs_config.clone();
    })
    .pd(pd)
    .build();
    cluster.wait_region_replicated(&[], 3);

    let mut client = cluster.new_client();
    client.ingest_ref_store(ref_store);
    client.verify_data_with_ref_store();

    cluster.stop();
}

#[test]
fn test_native_lightweight_backup() {
    test_util::init_log_for_test();

    let base_dir = tempfile::Builder::new()
        .prefix("test_restore_cluster")
        .tempdir()
        .unwrap();

    let oss_dir = base_dir.path().join("oss");
    let mut oss = ObjectStorageService::new(oss_dir);
    oss.start_server();

    let dfs_config = DFSConfig {
        prefix: "test_lightweight_backup".to_string(),
        s3_endpoint: format!("http://127.0.0.1:{}", oss.port()),
        s3_key_id: "admin".to_string(),
        s3_secret_key: "admin".to_string(),
        s3_bucket: "test_lightweight_backup".to_string(),
        s3_region: "local".to_string(),
        zstd_compression_level: "3".to_string(),
        ..Default::default()
    };

    let backup_name = format!("backup_{}", rand::thread_rng().gen::<u16>());

    let (backup_meta, ref_store) =
        start_cluster_and_backup(backup_name.clone(), dfs_config.clone());

    let backup_dir = base_dir.path().join("backup");
    restore_cluster(
        backup_name,
        backup_dir.as_path(),
        dfs_config,
        backup_meta,
        ref_store,
    );

    oss.shutdown();
}

#[test]
fn test_periodic_backup() {
    test_util::init_log_for_test();
    const KEYSPACE_ID: u32 = 1;
    const DATA_LEN: usize = 10;
    const VALUE_SIZE: usize = 64;

    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("t_");
    let s3fs = Arc::new(S3Fs::new_from_config(dfs_config.clone()));
    let reporter = Arc::new(DummyStepReporter::default());
    let runtime = Runtime::new().unwrap();

    let nodes = alloc_node_id_vec(3);
    let mut cluster = ServerCluster::new(nodes.clone(), |_, conf: &mut TikvConfig| {
        conf.dfs = dfs_config.clone();
        conf.rfengine.lightweight_backup = true;
    });
    cluster.wait_region_replicated(&[], 3);
    let pd_client = cluster.get_pd_client();
    let mut client = cluster.new_client();
    client.split_keyspace(KEYSPACE_ID);

    let gen_key = i_to_keyspace_key(KEYSPACE_ID);
    client.put_kv(0..DATA_LEN, &gen_key, random_value::<VALUE_SIZE>);
    client.verify_data_with_ref_store();
    let origin_ref_store = client.dump_ref_store();

    let backup_config = backup::BackupConfig {
        dfs: dfs_config.clone(),
        skip_keyspace_meta: true,
        backup_delay: ReadableDuration::secs(1),
        ..Default::default()
    };
    let mut backup_worker = backup_worker::BackupWorker::new(
        backup_config,
        pd_client.clone(),
        Duration::from_secs(2),
        Duration::from_secs(2),
    );

    let now = Utc::now().date_naive();
    let files = try_wait_result(
        || match runtime.block_on(get_all_incremental_backups(
            s3fs.as_ref(),
            &now,
            None,
            usize::MAX,
        )) {
            Ok((files, _)) if files.len() >= 3 => Ok(files),
            Ok(_) => Err("not enough files".to_string()),
            Err(e) => Err(format!("failed: {}", e)),
        },
        20,
    )
    .unwrap();
    info!("backup files: {:?}", &files);

    // Put another data.
    client.put_kv(
        DATA_LEN / 4..DATA_LEN / 2,
        &gen_key,
        random_value::<VALUE_SIZE>,
    );
    client.verify_data_with_ref_store();

    let backup_file = files.choose(&mut thread_rng()).unwrap();

    let pd_client = cluster.get_pd_client();
    let object_cache = cluster.create_object_cache_randomly();
    let limiter = cluster.create_restore_limiter_randomly(runtime.handle().clone());
    let res = restore_keyspace::restore_keyspace(
        KEYSPACE_ID,
        KEYSPACE_ID,
        backup_file.name(),
        None,
        s3fs.clone(),
        RestoreConfig::default(),
        pd_client,
        &runtime,
        None,
        reporter,
        object_cache,
        limiter,
        None,
    )
    .unwrap();

    info!("restore keyspace result: {:?}", res);
    let (existed, deleted) = client
        .verify_data_with_given_ref_store(&origin_ref_store, None, &RequestOptions::default())
        .unwrap();
    assert_eq!(existed, DATA_LEN);
    assert_eq!(deleted, 0);

    backup_worker.stop();
    cluster.stop();
    oss.shutdown();
}

#[test]
fn test_batch_backup() {
    test_util::init_log_for_test();
    const KEYSPACE_ID: u32 = 1;
    const DATA_LEN: usize = 10;
    const VALUE_SIZE: usize = 64;

    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("t_");
    let s3fs = Arc::new(S3Fs::new_from_config(dfs_config.clone()));
    let reporter = Arc::new(DummyStepReporter::default());
    let runtime = Runtime::new().unwrap();

    let nodes = alloc_node_id_vec(3);
    let mut cluster = ServerCluster::new(nodes.clone(), |_, conf: &mut TikvConfig| {
        conf.dfs = dfs_config.clone();
        conf.rfengine.lightweight_backup = true;
    });
    cluster.wait_region_replicated(&[], 3);
    let pd_client = cluster.get_pd_client();
    let mut client = cluster.new_client();
    client.split_keyspace(KEYSPACE_ID);

    let gen_key = i_to_keyspace_key(KEYSPACE_ID);
    client.put_kv(0..DATA_LEN, &gen_key, random_value::<VALUE_SIZE>);
    client.verify_data_with_ref_store();
    let origin_ref_store = client.dump_ref_store();

    let backup_config = backup::BackupConfig {
        dfs: dfs_config.clone(),
        skip_keyspace_meta: true,
        backup_delay: ReadableDuration::secs(1),
        ..Default::default()
    };
    let backup_worker = Arc::new(backup_worker::BackupWorker::new(
        backup_config,
        pd_client.clone(),
        Duration::from_secs(4),
        Duration::from_secs(2),
    ));

    let mut js = tokio::task::JoinSet::new();
    for _ in 0..50 {
        let backup_worker = backup_worker.clone();
        let task = async move {
            let sleep_secs = thread_rng().gen_range(0..6);
            tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
            backup_worker.instant_backup().await.unwrap()
        };
        js.spawn_on(task, runtime.handle());
    }

    let backup_files = runtime.block_on(async {
        let mut backup_files = vec![];
        while let Some(r) = js.join_next().await {
            let backup_res = r.unwrap();
            backup_files.push(backup_res.backup_file.name().to_string());
        }
        backup_files.sort_unstable();
        backup_files.dedup();
        backup_files
    });

    info!("backup files: {:?}", &backup_files);
    assert!(backup_files.len() <= 5);

    // Put another data.
    client.put_kv(
        DATA_LEN / 4..DATA_LEN / 2,
        &gen_key,
        random_value::<VALUE_SIZE>,
    );
    client.verify_data_with_ref_store();

    let backup_file = backup_files.choose(&mut thread_rng()).unwrap();

    let pd_client = cluster.get_pd_client();
    let object_cache = cluster.create_object_cache_randomly();
    let limiter = cluster.create_restore_limiter_randomly(runtime.handle().clone());
    let res = restore_keyspace::restore_keyspace(
        KEYSPACE_ID,
        KEYSPACE_ID,
        backup_file,
        None,
        s3fs.clone(),
        RestoreConfig::default(),
        pd_client,
        &runtime,
        None,
        reporter,
        object_cache,
        limiter,
        None,
    )
    .unwrap();

    info!("restore keyspace result: {:?}", res);
    let (existed, deleted) = client
        .verify_data_with_given_ref_store(&origin_ref_store, None, &RequestOptions::default())
        .unwrap();
    assert_eq!(existed, DATA_LEN);
    assert_eq!(deleted, 0);

    cluster.stop();
    oss.shutdown();
}

#[test]
fn test_backup_worker_stop() {
    test_util::init_log_for_test();
    const KEYSPACE_ID: u32 = 1;

    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("t_");
    let runtime = Runtime::new().unwrap();

    let nodes = alloc_node_id_vec(3);
    let mut cluster = ServerCluster::new(nodes.clone(), |_, conf: &mut TikvConfig| {
        conf.dfs = dfs_config.clone();
        conf.rfengine.lightweight_backup = true;
    });
    cluster.wait_region_replicated(&[], 3);
    let pd_client = cluster.get_pd_client();
    let mut client = cluster.new_client();
    client.split_keyspace(KEYSPACE_ID);

    let backup_config = backup::BackupConfig {
        dfs: dfs_config.clone(),
        skip_keyspace_meta: true,
        backup_delay: ReadableDuration::secs(1),
        ..Default::default()
    };
    let mut backup_worker = backup_worker::BackupWorker::new(
        backup_config,
        pd_client,
        Duration::from_secs(30),
        Duration::from_secs(30),
    );

    let mut js = tokio::task::JoinSet::new();
    for _ in 0..10 {
        let backup_worker_h = backup_worker.handle();
        let task = async move { backup_worker_h.instant_backup().await };
        js.spawn_on(task, runtime.handle());
    }

    thread::sleep(Duration::from_secs(1)); // Wait for instant backup request sent.
    backup_worker.stop();

    let mut shared_stopped = false;
    runtime.block_on(async {
        while let Some(r) = js.join_next().await {
            match r.unwrap().unwrap_err() {
                BrError::SharedError(e) => {
                    assert_matches!(e.inner(), BrError::Stopped);
                    shared_stopped = true;
                }
                BrError::Stopped => {
                    // Happens when stop before instant backup request is sent.
                    info!("meet not-shared stopped")
                }
                e => panic!("Unexpected br error: {:?}", e),
            }
        }
    });
    assert!(shared_stopped);

    cluster.stop();
    oss.shutdown();
}

fn i_to_key(i: usize) -> Vec<u8> {
    format!("xkey_{:08}", i).into_bytes()
}

fn i_to_val(i: usize) -> Vec<u8> {
    format!("val_{:08}", i).into_bytes().repeat(100)
}

fn i_to_keyspace_key(keyspace_id: u32) -> impl Fn(usize) -> Vec<u8> {
    move |i: usize| -> Vec<u8> {
        let mut key = ApiV2::get_keyspace_prefix_by_id(keyspace_id);
        key.extend(format!("tkey_{:08}", i).into_bytes());
        key
    }
}
