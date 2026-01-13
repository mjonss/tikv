// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use bytes::Bytes;
use chrono::Utc;
use cloud_worker::native_br::{BackupItem, RestoreState, test_utils::NativeBrSvcClient};
use collections::HashMap;
use fail::cfg_callback;
use futures::executor::block_on;
use http::StatusCode;
use kvengine::dfs::S3Fs;
use kvproto::{
    kvrpcpb::Op,
    metapb,
    metapb::StoreState::{Offline, Tombstone, Up},
};
use native_br::{
    backup,
    restore::RestoreConfig,
    restore_keyspace,
    restore_keyspace::{ReportRestoreStepTrait, RestoreStep},
};
use pd_client::PdClient;
use rand::Rng;
use security::{HttpClientError, SecurityConfig, SecurityManager};
use test_cloud_server::{
    ServerCluster, ServerClusterBuilder, TikvWorkerOptions, TryWaiter, alloc_node_id,
    alloc_node_id_vec,
    client::{
        ClusterClientOptions, CommitAction, MutateOptions, PrewriteExt, RequestOptions,
        TxnMutations, TxnWriteMethod,
    },
    oss::prepare_dfs,
    util::Mutation,
};
use test_pd_client::PdWrapper;
use tikv::config::TikvConfig;
use tikv_util::{codec::bytes::encode_bytes, config::ReadableDuration, info};
use tokio::{
    runtime::Runtime,
    sync::oneshot::{Receiver, Sender},
};
use txn_types::Key;

use crate::cases::{i_to_keyspace_key, new_security_config, random_value};

#[test]
fn test_backup_on_scaling_up() {
    test_util::init_log_for_test();
    const KEYSPACE_ID: u32 = 1;
    const DATA_LEN: usize = 100;
    const VALUE_SIZE: usize = 64;

    let get_all_stores_fp = "test_pd::get_all_stores";

    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("t_");
    let s3fs = Arc::new(S3Fs::new_from_config(dfs_config.clone()));
    let reporter = Arc::new(DummyStepReporter::default());
    let runtime = Runtime::new().unwrap();

    let nodes = alloc_node_id_vec(6);
    let mut cluster = ServerCluster::new(nodes.clone(), |_, conf: &mut TikvConfig| {
        conf.dfs = dfs_config.clone();
        conf.rfengine.lightweight_backup = true;
    });
    cluster.wait_region_replicated(&[], 3);
    let pd_client = cluster.get_pd_client();
    let mut client = cluster.new_client();
    client.split_keyspace(KEYSPACE_ID);

    let stores = nodes
        .iter()
        .map(|&node_id| {
            let store = pd_client.get_store(cluster.get_store_id(node_id)).unwrap();
            (node_id, store)
        })
        .collect::<HashMap<_, _>>();

    for idx in [3, 4, 5] {
        stop_node(&mut cluster, nodes[idx], &stores, Tombstone);
    }

    let i_to_key = i_to_keyspace_key(KEYSPACE_ID);
    client.put_kv(0..DATA_LEN, &i_to_key, random_value::<VALUE_SIZE>);
    client.verify_data_with_ref_store();
    let origin_ref_store = client.dump_ref_store();

    let backup_config = backup::BackupConfig {
        dfs: dfs_config.clone(),
        backup_delay: ReadableDuration::secs(1),
        tolerate_err: 1,
        skip_keyspace_meta: true,
        ..Default::default()
    };

    // The first invoke of `get_all_stores` during backup will succeed, then
    // blocked.
    fail::cfg(get_all_stores_fp, "1*off->pause").unwrap();

    // Perform backup.
    let backup_name = generate_backup_name();
    thread::scope(|s| {
        let stopped_store_id = stop_node(&mut cluster, nodes[0], &stores, Offline);

        let backup_config = backup_config.clone();
        let backup_name = backup_name.clone();
        let pd_client = pd_client.clone();
        let backup_ts = client.get_ts().into_inner();
        let h = s.spawn(move || {
            backup::backup_cluster_with_ts(
                backup_config,
                backup_name,
                pd_client.as_ref(),
                backup_ts,
                None,
            )
        });

        thread::sleep(Duration::from_secs(2));

        // This topology change will be tolerated.
        start_node(&mut cluster, nodes[3], &stores, Up);
        fail::cfg(get_all_stores_fp, "off").unwrap();

        let (_, backup_meta) = h.join().unwrap().expect("backup");
        info!("backup cluster result: {}", backup_meta);
        assert_eq!(backup_meta.get_stores().len(), 3);
        assert_eq!(backup_meta.tolerated_err, 1);
        assert_eq!(backup_meta.tolerated_err_stores, vec![stopped_store_id]);
    });

    // Put another data.
    client.put_kv(
        DATA_LEN / 4..DATA_LEN / 2,
        &i_to_key,
        random_value::<VALUE_SIZE>,
    );
    client.verify_data_with_ref_store();
    let origin_ref_store_2 = client.dump_ref_store();

    fail::cfg(get_all_stores_fp, "1*off->pause").unwrap();
    // Perform backup.
    let backup_name_2 = generate_backup_name();
    thread::scope(|s| {
        start_node(&mut cluster, nodes[0], &stores, Up);

        let backup_name = backup_name_2.clone();
        let backup_ts = client.get_ts().into_inner();
        let h = s.spawn(move || {
            backup::backup_cluster_with_ts(
                backup_config,
                backup_name,
                pd_client.as_ref(),
                backup_ts,
                None,
            )
        });

        thread::sleep(Duration::from_secs(2));

        // This topology change can not be tolerated, then retry to succeed.
        for idx in [4, 5] {
            start_node(&mut cluster, nodes[idx], &stores, Up);
        }
        fail::cfg(get_all_stores_fp, "off").unwrap();

        let (_, backup_meta) = h.join().unwrap().expect("backup");
        info!("backup cluster result: {}", backup_meta);
        assert_eq!(backup_meta.get_stores().len(), 6);
        assert_eq!(backup_meta.tolerated_err, 0);
    });

    // More data at last.
    client.put_kv(
        DATA_LEN / 3..DATA_LEN * 2 / 3,
        &i_to_key,
        random_value::<VALUE_SIZE>,
    );
    client.verify_data_with_ref_store();

    let object_cache = cluster.create_object_cache_randomly();
    let limiter = cluster.create_restore_limiter_randomly(runtime.handle().clone());
    // Perform restore to verify backup.
    for (bk_name, ref_store) in [
        (backup_name, origin_ref_store),
        (backup_name_2, origin_ref_store_2),
    ] {
        let restore_config = RestoreConfig {
            tolerate_err: 0,
            strict_tolerate: true,
            lower_memory: RestoreConfig::use_lower_memory(),
            ..Default::default()
        };
        let res = restore_keyspace::restore_keyspace(
            KEYSPACE_ID,
            KEYSPACE_ID,
            &bk_name,
            None,
            s3fs.clone(),
            restore_config.clone(),
            cluster.get_pd_client(),
            &runtime,
            None,
            reporter.clone(),
            object_cache.clone(),
            limiter.clone(),
            None,
        )
        .expect("restore");
        info!("restore keyspace result for backup {}: {:?}", bk_name, res);
        let (existed, deleted) = client
            .verify_data_with_given_ref_store(&ref_store, None, &RequestOptions::default())
            .unwrap();
        assert_eq!(existed, DATA_LEN);
        assert_eq!(deleted, 0);
    }

    fail::remove(get_all_stores_fp);
    cluster.stop();
    oss.shutdown();
}

#[test]
fn test_restore_on_disk_full() {
    test_util::init_log_for_test();
    const KEYSPACE_ID: u32 = 1;
    const DATA_LEN: usize = 10;
    const VALUE_SIZE: usize = 64;

    let low_space_fp = if rand::thread_rng().gen_bool(0.5) {
        "engine_is_low_space" // leader is low space.
    } else {
        "is_store_low_space" // follower is low space.
    };

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

    let i_to_key = i_to_keyspace_key(KEYSPACE_ID);
    client.put_kv(0..DATA_LEN, &i_to_key, random_value::<VALUE_SIZE>);
    client.verify_data_with_ref_store();
    let origin_ref_store = client.dump_ref_store();

    let backup_config = backup::BackupConfig {
        dfs: dfs_config.clone(),
        backup_delay: ReadableDuration::secs(1),
        skip_keyspace_meta: true,
        ..Default::default()
    };

    // Perform backup.
    let backup_name = generate_backup_name();
    let pd_client = pd_client.clone();
    let backup_ts = client.get_ts().into_inner();
    backup::backup_cluster_with_ts(
        backup_config,
        backup_name.clone(),
        pd_client.as_ref(),
        backup_ts,
        None,
    )
    .expect("backup");

    // Put another data.
    client.put_kv(
        DATA_LEN / 4..DATA_LEN / 2,
        &i_to_key,
        random_value::<VALUE_SIZE>,
    );
    client.verify_data_with_ref_store();

    fail::cfg(low_space_fp, "return").unwrap();

    let restore_config = RestoreConfig::default_for_test();
    let object_cache = cluster.create_object_cache_randomly();
    let limiter = cluster.create_restore_limiter_randomly(runtime.handle().clone());
    thread::scope(|s| {
        let pd_client = cluster.get_pd_client();
        let h = s.spawn(move || {
            restore_keyspace::restore_keyspace(
                KEYSPACE_ID,
                KEYSPACE_ID,
                &backup_name,
                None,
                s3fs.clone(),
                restore_config,
                pd_client,
                &runtime,
                None,
                reporter,
                object_cache,
                limiter,
                None,
            )
        });

        thread::sleep(Duration::from_secs(3));
        fail::cfg(low_space_fp, "off").unwrap();

        let res = h.join().unwrap().expect("restore");
        info!("restore keyspace result: {:?}", res);
    });
    let (existed, deleted) = client
        .verify_data_with_given_ref_store(&origin_ref_store, None, &RequestOptions::default())
        .unwrap();
    assert_eq!(existed, DATA_LEN);
    assert_eq!(deleted, 0);

    fail::remove(low_space_fp);
    cluster.stop();
    oss.shutdown();
}

#[test]
fn test_native_br_service() {
    test_util::init_log_for_test();
    const KEYSPACE_A_ID: u32 = 1;
    const KEYSPACE_B_ID: u32 = 42;
    const DATA_LEN: usize = 100;
    const VALUE_SIZE: usize = 64;

    let mock_get_keyspace_fp = "pd_ctl::mock_get_keyspace_by_name";
    let mock_no_tiflash = "pd_ctl::mock_no_tiflash_placement_rule_group";

    let runtime = Runtime::new().unwrap();
    let _enter = runtime.enter();

    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("t_");
    let pd_wrapper = PdWrapper::new_test(1, &SecurityConfig::default(), None);
    let mut cluster = ServerClusterBuilder::new(alloc_node_id_vec(3), |_, conf| {
        conf.dfs = dfs_config.clone();
        conf.rfengine.lightweight_backup = true;
    })
    .pd(pd_wrapper)
    .build();
    let tikv_worker_id = alloc_node_id();
    let tikv_worker_opts = TikvWorkerOptions {
        backup_interval: Duration::from_millis(1010),
        backup_skip_keyspace_meta: true,
        restore_timeout_pd_control: Duration::from_secs(3),
        ..Default::default()
    };
    cluster.start_tikv_workers(vec![tikv_worker_id], tikv_worker_opts.clone());
    cluster.wait_region_replicated(&[], 3);

    let pd_client = cluster.get_pd_client();
    let cluster_id = pd_client.get_cluster_id().unwrap();

    let mut client = cluster.new_client();
    client.split_keyspace(KEYSPACE_A_ID);
    client.split_keyspace(KEYSPACE_B_ID);

    let i_to_key = i_to_keyspace_key(KEYSPACE_A_ID);
    client.put_kv(0..DATA_LEN, &i_to_key, random_value::<VALUE_SIZE>);
    client.verify_data_with_ref_store();

    let datetime0 = Utc::now();
    let ref_store0 = client.dump_ref_store();

    let security_mgr = Arc::new(SecurityManager::default());
    let br_cli =
        NativeBrSvcClient::new(cluster_id, cluster.tikv_worker_endpoints(), security_mgr).unwrap();
    let backup = TryWaiter::timeout(10)
        .interval(1)
        .try_wait_result(|| {
            let mut backups = block_on(br_cli.list_backups(&datetime0)).unwrap();
            backups
                .items
                .pop()
                .ok_or(Err::<BackupItem, String>("no backup found".to_string()))
        })
        .unwrap();
    info!("backup: {:?}", backup);

    // Empty keyspace is illegal and should return HTTP 400.
    let err = block_on(br_cli.get_restore_progress(1000, "".to_string())).unwrap_err();
    match err {
        HttpClientError::Http(status, _) => assert_eq!(status, StatusCode::BAD_REQUEST),
        err => panic!("unexpected error: {:?}", err),
    }
    let err = block_on(br_cli.restore_keyspace_to_backup(100, "".to_string(), None, &backup))
        .unwrap_err();
    match err {
        HttpClientError::Http(status, _) => assert_eq!(status, StatusCode::BAD_REQUEST),
        err => panic!("unexpected error: {:?}", err),
    }
    let err = block_on(br_cli.restore_keyspace_to_backup(
        101,
        "ks101".to_string(),
        Some("".to_string()),
        &backup,
    ))
    .unwrap_err();
    match err {
        HttpClientError::Http(status, _) => assert_eq!(status, StatusCode::BAD_REQUEST),
        err => panic!("unexpected error: {:?}", err),
    }

    // More data.
    client.put_kv(
        DATA_LEN / 3..DATA_LEN * 2 / 3,
        &i_to_key,
        random_value::<VALUE_SIZE>,
    );
    client.verify_data_with_ref_store();

    // Inplace restore.
    let keyspace_a_name = format!("ks{KEYSPACE_A_ID}");
    let progress =
        block_on(br_cli.restore_keyspace_to_backup(1, keyspace_a_name.clone(), None, &backup))
            .unwrap();
    info!("create restore: {:?}", progress);

    // Must fail due to get keyspace name error.
    TryWaiter::timeout(30).interval(1).must_wait(
        || {
            let progress =
                block_on(br_cli.get_restore_progress(1, keyspace_a_name.clone())).unwrap();
            info!("restore progress: {:?}", progress);
            progress.status == RestoreState::Error
        },
        || "restore not error".to_string(),
    );

    // Mock PD control & retry.
    fail::cfg(mock_get_keyspace_fp, "return").unwrap();
    fail::cfg(mock_no_tiflash, "return").unwrap();

    // Restore tikv worker.
    cluster.stop_tikv_workers();
    cluster.start_tikv_workers(vec![tikv_worker_id], tikv_worker_opts.clone());

    // Check task persistence.
    let progress = block_on(br_cli.get_restore_progress(1, keyspace_a_name.clone())).unwrap();
    info!("restore progress (after restart): {:?}", progress);
    assert_eq!(progress.status, RestoreState::Error);
    assert_eq!(progress.error, "interrupted");

    // Retry with incorrect restore params.
    let err =
        block_on(br_cli.restore_keyspace_to_point_in_time(1, keyspace_a_name.clone(), datetime0))
            .unwrap_err();
    info!("retry restore error: {:?}", err);
    assert!(err.to_string().contains("restore params not match"));

    // Retry restore.
    let progress =
        block_on(br_cli.restore_keyspace_to_backup(1, keyspace_a_name.clone(), None, &backup))
            .unwrap();
    info!("retry restore: {:?}", progress);

    // Wait succeed.
    TryWaiter::timeout(30).interval(1).must_wait(
        || {
            let progress =
                block_on(br_cli.get_restore_progress(1, keyspace_a_name.clone())).unwrap();
            info!("restore progress: {:?}", progress);
            assert_ne!(
                progress.status,
                RestoreState::Error,
                "progress: {:?}",
                progress
            );
            progress.status == RestoreState::Succeed
        },
        || "restore not succeed".to_string(),
    );

    // Verify data.
    let (existed, deleted) = client
        .verify_data_with_given_ref_store(&ref_store0, None, &RequestOptions::default())
        .unwrap();
    assert_eq!(existed, DATA_LEN);
    assert_eq!(deleted, 0);

    // Restore from keyspace A to B and verify data.
    {
        let keyspace_b_name = format!("ks{KEYSPACE_B_ID}");
        let progress = block_on(br_cli.restore_keyspace_to_backup(
            2,
            keyspace_b_name.clone(),
            Some(keyspace_a_name.clone()),
            &backup,
        ))
        .unwrap();
        info!("create restore (A->B): {:?}", progress);

        TryWaiter::timeout(60).interval(1).must_wait(
            || {
                let progress =
                    block_on(br_cli.get_restore_progress(2, keyspace_b_name.clone())).unwrap();
                info!("restore progress (A->B): {:?}", progress);
                progress.status == RestoreState::Succeed
            },
            || "restore (A->B) not succeed".to_string(),
        );

        let mut expected_ref_store = ref_store0.clone();
        expected_ref_store.rewrite_keyspace_prefix(KEYSPACE_B_ID);
        let (existed, deleted) = client
            .verify_data_with_given_ref_store(&expected_ref_store, None, &RequestOptions::default())
            .unwrap();
        assert_eq!(existed, DATA_LEN);
        assert_eq!(deleted, 0);
    }

    fail::remove(mock_get_keyspace_fp);
    fail::remove(mock_no_tiflash);

    cluster.stop();
    oss.shutdown();
}

#[test]
fn test_backup_pessimistic_lock() {
    test_util::init_log_for_test();
    const KEYSPACE_ID: u32 = 1;
    const DATA_LEN: usize = 100;
    const VALUE_SIZE: usize = 64;

    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("t_");
    let s3fs = Arc::new(S3Fs::new_from_config(dfs_config.clone()));
    let reporter = Arc::new(DummyStepReporter::default());
    let runtime = Runtime::new().unwrap();

    let nodes = alloc_node_id_vec(2);
    let mut cluster = ServerCluster::new(nodes.clone(), |_, conf: &mut TikvConfig| {
        conf.dfs = dfs_config.clone();
        conf.rfengine.lightweight_backup = true;
        conf.raft_store.allow_remove_leader = true;
    });
    cluster.wait_region_replicated_ext(&[], 2, true);

    let pd_client = cluster.get_pd_client();
    let mut client = cluster.new_client();

    pd_client.disable_default_operator();
    cluster.remove_node_peers(nodes[1]);
    info!("ConfChange removed node peers {}", nodes[1]);

    client.split_keyspace(KEYSPACE_ID);
    client.set_async_commit();

    let i_to_key = i_to_keyspace_key(KEYSPACE_ID);

    client.put_kv(1..DATA_LEN, &i_to_key, random_value::<VALUE_SIZE>);
    client.verify_data_with_ref_store();

    // split at DATA_LEN / 2
    let split_key = i_to_key(DATA_LEN / 2);
    client.split(&split_key);
    let enc = |k| Key::from_raw(k).into_encoded();

    let pk = i_to_key(DATA_LEN);
    let sk = i_to_key(0);

    let region = client.pd_client().get_region(&enc(&pk)).unwrap();
    assert_eq!(region.peers.len(), 1, "{:?}", region);
    cluster.evict_peer(region.peers[0].id);
    let region = client.pd_client().get_region(&enc(&sk)).unwrap();
    let region2 = client.pd_client().get_region(&enc(&pk)).unwrap();
    assert_eq!(region.peers.len(), 1, "{:?}", region);
    assert_eq!(region2.peers.len(), 1, "{:?}", region);
    assert_ne!(
        region.peers[0].store_id, region2.peers[0].store_id,
        "{:?}\n{:?}",
        region, region2
    );

    let start_ts = client.get_ts();
    client
        .kv_pessimistic_lock(
            pk.clone().into(),
            vec![pk.clone().into(), sk.clone().into()],
            start_ts,
            20000,
            start_ts,
            None,
        )
        .unwrap();
    let origin_ref_store = client.dump_ref_store();

    let backup_config = backup::BackupConfig {
        dfs: dfs_config.clone(),
        backup_delay: ReadableDuration::secs(1),
        tolerate_err: 1,
        skip_keyspace_meta: true,
        ..Default::default()
    };

    // Perform backup.
    let backup_name = generate_backup_name();
    thread::scope(|s| {
        let backup_config = backup_config.clone();
        let backup_name = backup_name.clone();
        let pd_client = pd_client.clone();
        let backup_ts = client.get_ts().into_inner();

        let rx = cfg_notify_once("native_br::backup_store").unwrap();
        let rx_finish = cfg_notify_once("native_br::backup_store::ret").unwrap();

        let h = s.spawn(move || {
            backup::backup_cluster_with_ts(
                backup_config,
                backup_name,
                pd_client.as_ref(),
                backup_ts,
                None,
            )
        });

        let tx = rx.blocking_recv().unwrap();
        let muts = vec![
            put(pk.clone(), random_value::<VALUE_SIZE>(0)),
            put(sk.clone(), random_value::<VALUE_SIZE>(0)),
        ];
        let txn_muts = TxnMutations::from_normal(muts);
        let _ = rx_finish.blocking_recv();
        let mut txn = client.begin_transaction(Some(start_ts));
        client
            .kv_prewrite_ext(
                Bytes::copy_from_slice(&pk),
                Some(&vec![Bytes::copy_from_slice(&sk)]),
                txn_muts,
                &mut txn,
                PrewriteExt {
                    resolve_lock: false,
                    for_update_ts: start_ts,
                    ..Default::default()
                },
            )
            .unwrap();
        tx.send(()).unwrap();

        let (_, backup_meta) = h.join().unwrap().expect("backup");
        info!("backup cluster result: {}", backup_meta);
    });

    // Perform restore to verify backup.
    let restore_config = RestoreConfig {
        tolerate_err: 0,
        strict_tolerate: true,
        lower_memory: RestoreConfig::use_lower_memory(),
        ..Default::default()
    };
    let object_cache = cluster.create_object_cache_randomly();
    let limiter = cluster.create_restore_limiter_randomly(runtime.handle().clone());
    let res = restore_keyspace::restore_keyspace(
        KEYSPACE_ID,
        KEYSPACE_ID,
        &backup_name,
        None,
        s3fs.clone(),
        restore_config.clone(),
        cluster.get_pd_client(),
        &runtime,
        None,
        reporter.clone(),
        object_cache,
        limiter,
        None,
    )
    .expect("restore");
    info!(
        "restore keyspace result for backup {}: {:?}",
        backup_name, res
    );
    let (existed, deleted) = client
        .verify_data_with_given_ref_store(&origin_ref_store, None, &RequestOptions::default())
        .unwrap();
    assert_eq!(existed, DATA_LEN - 1);
    assert_eq!(deleted, 0);

    cluster.stop();
    oss.shutdown();
}

#[rstest::rstest]
#[case(TxnWriteMethod::Normal)]
#[case::txn_file(TxnWriteMethod::FileBased)]
fn test_check_backup_ts(#[case] write_method: TxnWriteMethod) {
    test_util::init_log_for_test();
    const KEYSPACE_ID: u32 = 1;
    const DATA_LEN: usize = 10;
    const VALUE_SIZE: usize = 64;

    let process_commit_finish_fp = if matches!(write_method, TxnWriteMethod::Normal) {
        "txn::commit_process_write_finish"
    } else {
        "txn::txn_file_process_commit_finish"
    };
    let client_before_kv_commit_fp = "client::before_kv_commit";

    // Set to `false` to reproduce https://github.com/tidbcloud/cloud-storage-engine/issues/2779.
    // i.e., Txn2 has `commit_ts` < `backup_ts`, but not in the backup.
    let check_backup_ts = true;

    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("t_");
    let s3fs = Arc::new(S3Fs::new_from_config(dfs_config.clone()));
    let reporter = Arc::new(DummyStepReporter::default());
    let runtime = Runtime::new().unwrap();

    let nodes = alloc_node_id_vec(3);
    let security_config = new_security_config();
    let pd_wrapper = PdWrapper::new_test(1, &security_config, None);
    let mut cluster = ServerClusterBuilder::new(nodes, |_, conf: &mut TikvConfig| {
        conf.dfs = dfs_config.clone();
        conf.security = security_config.clone();
        conf.rfengine.lightweight_backup = true;
        conf.storage.check_backup_ts = check_backup_ts;
    })
    .pd(pd_wrapper)
    .build();
    cluster.start_tikv_workers(alloc_node_id_vec(1), TikvWorkerOptions::default());
    cluster.wait_region_replicated(&[], 3);
    let pd_client = cluster.get_pd_client();
    let mut client = cluster.new_client();
    client.split_keyspace(KEYSPACE_ID);
    let i_to_key = i_to_keyspace_key(KEYSPACE_ID);

    if matches!(write_method, TxnWriteMethod::FileBased) {
        // Split region to avoid txn file command blocking by latch.
        block_on(pd_client.split_regions_with_retry(
            vec![encode_bytes(&i_to_key(DATA_LEN))],
            Duration::from_secs(30),
        ))
        .unwrap();
    }

    // To work around that not existed keys are not checked.
    client.del_kv(0..DATA_LEN * 3, i_to_key);

    let backup_name = generate_backup_name();
    let backup_name_cp = backup_name.clone();
    let ref_store0 = thread::scope(|s| {
        // Txn1 pause at "commit_process_write_finish":
        let rx1 = cfg_notify_once(process_commit_finish_fp).unwrap();
        let mut cli = cluster.new_client_opt(ClusterClientOptions {
            txn_file_max_chunk_size: Some(1024),
            ..Default::default()
        });
        let h1 = s.spawn(move || {
            cli.try_put_kv(
                0..DATA_LEN,
                i_to_keyspace_key(KEYSPACE_ID),
                random_value::<VALUE_SIZE>,
                MutateOptions {
                    write_method,
                    ..Default::default()
                },
            )
            .unwrap()
        });
        let tx1 = rx1.blocking_recv().unwrap();

        // Txn2, pause at "before_kv_commit":
        let rx2 = cfg_notify_once(client_before_kv_commit_fp).unwrap();
        let mut cli = cluster.new_client_opt(ClusterClientOptions {
            txn_file_max_chunk_size: Some(1024),
            ..Default::default()
        });
        let h2 = s.spawn(move || {
            cli.try_put_kv(
                DATA_LEN..DATA_LEN * 3,
                i_to_keyspace_key(KEYSPACE_ID),
                random_value::<VALUE_SIZE>,
                MutateOptions {
                    write_method,
                    ..Default::default()
                },
            )
            .unwrap()
        });
        let tx2 = rx2.blocking_recv().unwrap();

        // Perform backup.
        let backup_config = backup::BackupConfig {
            dfs: dfs_config.clone(),
            backup_delay: ReadableDuration::secs(1),
            skip_keyspace_meta: true,
            ..Default::default()
        };
        let pd_client = pd_client.clone();
        let backup_ts = client.get_ts().into_inner();
        let h_backup = s.spawn(move || {
            backup::backup_cluster_with_ts(
                backup_config,
                backup_name_cp,
                pd_client.as_ref(),
                backup_ts,
                None,
            )
            .expect("backup")
        });

        // Resume txn1.
        tx1.send(()).unwrap();
        let commit_ts1 = h1.join().unwrap().into_inner();
        let ref_store0 = client.dump_ref_store();
        fail::cfg(process_commit_finish_fp, "off").unwrap();

        // Wait backup.
        let (name, backup_meta) = h_backup.join().unwrap();
        info!("backup: {} {}", name, backup_meta);

        // Resume txn2.
        tx2.send(()).unwrap();
        let commit_ts2 = h2.join().unwrap().into_inner();
        fail::cfg(client_before_kv_commit_fp, "off").unwrap();

        let (existed, deleted) = client.verify_data_with_ref_store();
        assert_eq!(existed, DATA_LEN * 3);
        assert_eq!(deleted, 0);

        assert!(commit_ts1 < backup_ts);
        if check_backup_ts {
            assert!(backup_ts < commit_ts2);
        } else {
            assert!(commit_ts2 < backup_ts);
        }

        ref_store0
    });

    // Perform restore.
    let restore_config = RestoreConfig::default_for_test();
    let object_cache = cluster.create_object_cache_randomly();
    let limiter = cluster.create_restore_limiter_randomly(runtime.handle().clone());
    let res = restore_keyspace::restore_keyspace(
        KEYSPACE_ID,
        KEYSPACE_ID,
        &backup_name,
        None,
        s3fs,
        restore_config,
        cluster.get_pd_client(),
        &runtime,
        None,
        reporter.clone(),
        object_cache,
        limiter,
        None,
    )
    .expect("restore");
    info!("restore: {:?}", res);

    // Verify that only txn1 is restored.
    let (existed, deleted) = client
        .verify_data_with_given_ref_store(&ref_store0, None, &RequestOptions::default())
        .unwrap();
    assert_eq!(existed, DATA_LEN);
    assert_eq!(deleted, DATA_LEN * 2);

    fail::remove(process_commit_finish_fp);
    fail::remove(client_before_kv_commit_fp);
    cluster.stop();
    oss.shutdown();
}

#[test]
fn test_check_backup_ts_with_async_commit() {
    test_util::init_log_for_test();
    const KEYSPACE_ID: u32 = 1;
    const DATA_LEN: usize = 10;
    const VALUE_SIZE: usize = 64;

    let process_prewrite_finish_fp = "txn::prewrite_process_write_finish";
    let client_before_kv_prewrite_fp = "client::before_kv_prewrite";
    let cm_before_update_max_ts_fp = "cm_before_update_max_ts";

    // Set to `false` to reproduce https://github.com/tidbcloud/cloud-storage-engine/issues/2779.
    // i.e., Txn2 has `commit_ts` < `backup_ts`, but not in the backup.
    let check_backup_ts = true;

    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("t_");
    let s3fs = Arc::new(S3Fs::new_from_config(dfs_config.clone()));
    let reporter = Arc::new(DummyStepReporter::default());
    let runtime = Runtime::new().unwrap();

    let nodes = alloc_node_id_vec(3);
    let security_config = new_security_config();
    let pd_wrapper = PdWrapper::new_test(1, &security_config, None);
    let mut cluster = ServerClusterBuilder::new(nodes, |_, conf: &mut TikvConfig| {
        conf.dfs = dfs_config.clone();
        conf.security = security_config.clone();
        conf.rfengine.lightweight_backup = true;
        conf.storage.check_backup_ts = check_backup_ts;
    })
    .pd(pd_wrapper)
    .build();
    cluster.wait_region_replicated(&[], 3);
    let pd_client = cluster.get_pd_client();
    let mut client = cluster.new_client();
    client.set_async_commit();
    client.split_keyspace(KEYSPACE_ID);
    let i_to_key = i_to_keyspace_key(KEYSPACE_ID);

    // Split region to write to multiple regions.
    block_on(pd_client.split_regions_with_retry(
        vec![encode_bytes(&i_to_key(DATA_LEN))],
        Duration::from_secs(30),
    ))
    .unwrap();

    // To work around that not existed keys are not checked.
    client.del_kv(0..DATA_LEN * 3, i_to_key);

    let backup_name = generate_backup_name();
    let backup_name_cp = backup_name.clone();
    let ref_store0 = thread::scope(|s| {
        // Skip update_max_ts to ensure that `min_commit_ts` is not advanced by
        // `max_ts`.
        fail::cfg(cm_before_update_max_ts_fp, "return").unwrap();

        // Txn1 pause at "prewrite_process_write_finish":
        let rx1 = cfg_notify_once(process_prewrite_finish_fp).unwrap();
        let mut cli = cluster.new_client();
        cli.set_async_commit();
        let h1 = s.spawn(move || {
            cli.try_put_kv(
                0..DATA_LEN,
                i_to_keyspace_key(KEYSPACE_ID),
                random_value::<VALUE_SIZE>,
                MutateOptions {
                    commit_action: CommitAction::AsyncCommit(Duration::from_secs(3)),
                    no_verify: true,
                    ..Default::default()
                },
            )
            .unwrap()
        });
        let tx1 = rx1.blocking_recv().unwrap();

        // Txn2, pause at "before_kv_prewrite":
        let rx2 = cfg_notify_once(client_before_kv_prewrite_fp).unwrap();
        let mut cli = cluster.new_client();
        cli.set_async_commit();
        let h2 = s.spawn(move || {
            cli.try_put_kv(
                DATA_LEN..DATA_LEN * 3,
                i_to_keyspace_key(KEYSPACE_ID),
                random_value::<VALUE_SIZE>,
                MutateOptions {
                    commit_action: CommitAction::AsyncCommit(Duration::from_secs(3)),
                    no_verify: true,
                    ..Default::default()
                },
            )
            .unwrap()
        });
        let tx2 = rx2.blocking_recv().unwrap();

        // Perform backup.
        let backup_config = backup::BackupConfig {
            dfs: dfs_config.clone(),
            backup_delay: ReadableDuration::secs(1),
            skip_keyspace_meta: true,
            ..Default::default()
        };
        let pd_client = pd_client.clone();
        // Get 2 TSO to ensure that backup_ts is greater than txn2.start_ts + 1;
        let backup_ts = block_on(pd_client.batch_get_tso(2)).unwrap().into_inner();
        let h_backup = s.spawn(move || {
            backup::backup_cluster_with_ts(
                backup_config,
                backup_name_cp,
                pd_client.as_ref(),
                backup_ts,
                None,
            )
            .expect("backup")
        });

        // Resume txn1.
        tx1.send(()).unwrap();
        let commit_ts1 = h1.join().unwrap().into_inner();
        let ref_store0 = client.dump_ref_store();
        fail::cfg(process_prewrite_finish_fp, "off").unwrap();

        // Wait backup.
        let (name, backup_meta) = h_backup.join().unwrap();
        info!("backup: {} {}", name, backup_meta);

        // Resume txn2.
        tx2.send(()).unwrap();
        let commit_ts2 = h2.join().unwrap().into_inner();
        fail::cfg(client_before_kv_prewrite_fp, "off").unwrap();

        fail::cfg(cm_before_update_max_ts_fp, "off").unwrap();

        let (existed, deleted) = client.verify_data_with_ref_store();
        assert_eq!(existed, DATA_LEN * 3);
        assert_eq!(deleted, 0);

        info!(
            "backup_ts: {}, commit_ts1: {}, commit_ts2: {}",
            backup_ts, commit_ts1, commit_ts2
        );
        assert!(commit_ts1 < backup_ts);
        if check_backup_ts {
            assert!(backup_ts <= commit_ts2); // Can be equal.
        } else {
            assert!(commit_ts2 < backup_ts);
        }

        ref_store0
    });

    let object_cache = cluster.create_object_cache_randomly();
    let limiter = cluster.create_restore_limiter_randomly(runtime.handle().clone());
    // Perform restore.
    let res = restore_keyspace::restore_keyspace(
        KEYSPACE_ID,
        KEYSPACE_ID,
        &backup_name,
        None,
        s3fs,
        RestoreConfig::default(),
        cluster.get_pd_client(),
        &runtime,
        None,
        reporter.clone(),
        object_cache,
        limiter,
        None,
    )
    .expect("restore");
    info!("restore: {:?}", res);

    // Verify that only txn1 is restored.
    let (existed, deleted) = client
        .verify_data_with_given_ref_store(&ref_store0, None, &RequestOptions::default())
        .unwrap();
    assert_eq!(existed, DATA_LEN);
    assert_eq!(deleted, DATA_LEN * 2);

    fail::remove(process_prewrite_finish_fp);
    fail::remove(client_before_kv_prewrite_fp);
    fail::remove(cm_before_update_max_ts_fp);
    cluster.stop();
    oss.shutdown();
}

#[derive(Default)]
struct DummyStepReporter {}

impl ReportRestoreStepTrait for DummyStepReporter {
    fn report_step(&self, _step: RestoreStep) {}
}

fn generate_backup_name() -> String {
    static BACKUP_ID: AtomicUsize = AtomicUsize::new(0);
    format!("{:04}", BACKUP_ID.fetch_add(1, Ordering::Relaxed))
}

fn stop_node(
    cluster: &mut ServerCluster,
    node_id: u16,
    stores: &HashMap<u16, metapb::Store>,
    state: metapb::StoreState,
) -> u64 /* store_id */ {
    cluster.stop_node(node_id);
    let store_id = set_store_state(cluster, node_id, stores, state);
    info!("node stopped"; "node" => node_id, "store" => store_id, "state" => ?state);
    store_id
}

fn start_node(
    cluster: &mut ServerCluster,
    node_id: u16,
    stores: &HashMap<u16, metapb::Store>,
    state: metapb::StoreState,
) -> u64 /* store_id */ {
    let store_id = set_store_state(cluster, node_id, stores, state);
    cluster.start_node(node_id, |_, _| {});
    info!("node started"; "node" => node_id, "store" => store_id, "state" => ?state);
    store_id
}

fn set_store_state(
    cluster: &ServerCluster,
    node_id: u16,
    stores: &HashMap<u16, metapb::Store>,
    state: metapb::StoreState,
) -> u64 /* store_id */ {
    let mut store = stores.get(&node_id).unwrap().clone();
    let store_id = store.id;
    store.state = state;
    cluster.get_pd_client().put_store(store).unwrap();
    store_id
}

fn put(k: Vec<u8>, v: Vec<u8>) -> Mutation {
    let mut m = Mutation::default();
    m.set_op(Op::Put);
    m.set_key(k);
    m.set_value(v);
    m
}

fn cfg_notify_once(name: &str) -> std::result::Result<Receiver<Sender<()>>, String> {
    let name_clone = name.to_string();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let pack = std::sync::Mutex::new(Some(tx));
    cfg_callback(name, move || {
        info!("injecting failpoint: {}", name_clone);
        let mut v = pack.lock().unwrap();
        if v.is_none() {
            return;
        }
        let tx = v.take().unwrap();
        drop(v);
        let (tx2, rx) = tokio::sync::oneshot::channel();

        let _ = tx.send(tx2);
        tokio::task::block_in_place(|| {
            let _ = rx.blocking_recv();
        })
    })?;

    Ok(rx)
}
