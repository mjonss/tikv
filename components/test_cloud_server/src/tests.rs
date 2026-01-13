// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! Test for test_cloud_server & test_pd_client themselves.

use std::{sync::Arc, time::Duration};

use api_version::ApiV2;
use bstr::ByteSlice;
use bytes::BytesMut;
use futures::executor::block_on;
use kvengine::dfs;
use kvproto::{metapb, pdpb};
use pd_client::{
    PdClient,
    pd_control::{CreateKeyspaceParams, CreateSchedulerParam, SchedulerStatus},
};
use rand::prelude::*;
use security::{RestfulClient, SecurityConfig, SecurityManager};
use test_pd_client::{PdClientExt, PdWrapper, TestPdClient};
use tikv_client::TimestampExt;
use tikv_util::{codec::bytes::encode_bytes, config::ReadableDuration, info, store::new_peer};
use tokio::runtime::Runtime;
use txn_types::Key;

use crate::{
    ServerCluster, ServerClusterBuilder, TikvWorkerOptions, alloc_node_id_vec,
    client::{CommitAction, MutateOptions},
    oss::prepare_dfs,
    try_wait, try_wait_result_async,
};

#[test]
fn it_works() {
    test_util::init_log_for_test();
    let node_ids = alloc_node_id_vec(3);
    let mut cluster = ServerCluster::new(node_ids.clone(), |_, _| {});
    let stores = cluster.get_stores();
    assert_eq!(stores.len(), 3);
    let mut client = cluster.new_client();
    client.put_kv(0..100, i_to_key, i_to_val);
    client.put_kv(100..200, i_to_key, i_to_val);
    client.put_kv(200..300, i_to_key, i_to_val);
    let split_keys = vec![i_to_key(50), i_to_key(150), i_to_key(200)];
    for split_key in &split_keys {
        client.split(split_key);
    }
    cluster.wait_pd_region_count(4);
    cluster.get_pd_client().disable_default_operator();
    cluster.remove_node_peers(node_ids[0]);
    cluster.stop_node(node_ids[0]);
    std::thread::sleep(Duration::from_millis(100));
    cluster.start_node(node_ids[0], |_, _| {});
    cluster.get_pd_client().enable_default_operator();
    info!("enable replica operator");
    cluster.wait_region_replicated(&[], 3);
    for split_key in &split_keys {
        cluster.wait_region_replicated(split_key, 3);
    }
    client.verify_data_with_ref_store();

    client
        .try_put_kv(
            50..250,
            i_to_key,
            prefixed_i_to_val("async".to_string()),
            MutateOptions {
                commit_action: CommitAction::AsyncCommitSecondaryKeys(Duration::from_millis(100)),
                ..Default::default()
            },
        )
        .unwrap();
    client.verify_data_with_ref_store();

    client
        .try_put_kv(
            150..300,
            i_to_key,
            prefixed_i_to_val("no".to_string()),
            MutateOptions {
                commit_action: CommitAction::NoCommit,
                ..Default::default()
            },
        )
        .unwrap();
    client.verify_data_with_ref_store();

    client
        .try_del_kv(
            100..200,
            i_to_key,
            MutateOptions {
                commit_action: CommitAction::AsyncCommitSecondaryKeys(Duration::MAX),
                ..Default::default()
            },
        )
        .unwrap();
    client.verify_data_with_ref_store();

    cluster.stop();
}

#[rstest::rstest]
#[case(false)]
#[case::async_commit(true)]
fn test_client_basic(#[case] async_commit: bool) {
    test_util::init_log_for_test();
    let node_ids = alloc_node_id_vec(3);
    let mut cluster = ServerCluster::new(node_ids.clone(), |_, _| {});
    let stores = cluster.get_stores();
    assert_eq!(stores.len(), 3);
    let mut client = cluster.new_client();
    if async_commit {
        client.set_async_commit();
    }
    client.put_kv(0..100, i_to_key, i_to_val);
    client.put_kv(100..200, i_to_key, i_to_val);
    client.put_kv(200..300, i_to_key, i_to_val);
    client.verify_data_with_ref_store();

    client.del_kv(100..200, i_to_key);
    client.verify_data_with_ref_store();

    cluster.stop();
}

#[test]
fn test_default_keyspace() {
    test_util::init_log_for_test();
    let node_ids = alloc_node_id_vec(3);
    let mut cluster = ServerCluster::new(node_ids.clone(), |_, _| {});
    let mut client = cluster.new_client();

    client.put_kv(0..100, i_to_tidb_key, i_to_val);
    assert_eq!(client.verify_data_with_ref_store(), (100, 0));
    cluster.stop();
}

#[test]
fn test_split_regions() {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(alloc_node_id_vec(3), |_, _| {});
    cluster.wait_region_replicated(&[], 3);
    let pd_client = cluster.get_pd_client();

    let keys0: Vec<Vec<u8>> = [10, 20, 30]
        .into_iter()
        .map(|i| encode_bytes(&i_to_key(i)))
        .collect();
    {
        let new_regions =
            block_on(pd_client.split_regions_with_retry(keys0.clone(), Duration::from_secs(10)))
                .unwrap();
        cluster.wait_pd_region_count(keys0.len() + 1);
        let mut region_keys = new_regions
            .into_iter()
            .map(|region_id| {
                block_on(pd_client.get_region_by_id(region_id))
                    .unwrap()
                    .unwrap()
                    .start_key
            })
            .collect::<Vec<_>>();
        region_keys.sort();
        assert_eq!(region_keys, keys0);

        let mut all_regions = block_on(pd_client.scan_regions(vec![], vec![], 100))
            .unwrap()
            .into_iter()
            .map(|mut r| r.take_region().take_start_key())
            .collect::<Vec<_>>();
        all_regions.sort();
        assert_eq!(all_regions[1..], keys0[..]); // Skip first region with empty start key.
    }

    let mut keys1: Vec<Vec<u8>> = [5, 10, 15, 25, 30, 40, 50]
        .into_iter()
        .map(|i| encode_bytes(&i_to_key(i)))
        .collect();
    {
        let new_regions =
            block_on(pd_client.split_regions_with_retry(keys1.clone(), Duration::from_secs(30)))
                .unwrap();
        cluster.wait_pd_region_count(keys1.len() + 1 + 1); // The `1` is 20 of keys0.
        let mut new_region_keys = new_regions
            .into_iter()
            .map(|region_id| {
                block_on(pd_client.get_region_by_id(region_id))
                    .unwrap()
                    .unwrap()
                    .start_key
            })
            .collect::<Vec<_>>();
        new_region_keys.sort();
        keys1.retain(|k| !keys0.contains(k));
        if new_region_keys != keys1 {
            // There are chances that some new regions are not returned. See
            // `split_regions_with_retry`.
            // Just check that no more region than `keys1` are returned.
            assert!(
                new_region_keys.iter().all(|k| keys1.contains(k)),
                "new_region_keys: {:?}, keys1: {:?}",
                new_region_keys,
                keys1
            );
        }

        let mut all_regions = block_on(pd_client.scan_regions(vec![], vec![], 100))
            .unwrap()
            .into_iter()
            .map(|mut r| r.take_region().take_start_key())
            .collect::<Vec<_>>();
        let expected = {
            let mut expected = keys0.clone();
            expected.append(&mut keys1);
            expected.sort();
            expected.dedup();
            expected
        };
        all_regions.sort();
        assert_eq!(all_regions[1..], expected); // Skip first region with empty start key.
    }

    cluster.stop();
}

// Test for update region cache after split.
// https://github.com/tidbcloud/cloud-storage-engine/pull/933.
#[test]
fn test_client_split_region() {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(alloc_node_id_vec(3), |_, _| {});
    cluster.wait_region_replicated(&[], 3);
    let mut client = cluster.new_client();

    let split_key = b"xkey";
    client.split(split_key);
    cluster.wait_pd_region_count(2);

    assert_ne!(client.get_region_id(b""), client.get_region_id(split_key));
    {
        let region = client.get_region_by_key(b"");
        assert_eq!(region.raw_start(), b"");
        assert_eq!(region.raw_end(), split_key);
    }
    {
        let region = client.get_region_by_key(split_key);
        assert_eq!(region.raw_start(), split_key);
        assert_eq!(region.raw_end(), kvengine::GLOBAL_SHARD_END_KEY);
    }

    cluster.stop();
}

#[test]
fn test_txn_client() {
    test_util::init_log_for_test();
    let node_ids = alloc_node_id_vec(3);
    let pd_wrapper = PdWrapper::new_test(1, &SecurityConfig::default(), None);
    let mut cluster = ServerClusterBuilder::new(node_ids, |_, conf| {
        conf.kvengine.max_del_range_delay = ReadableDuration(Duration::from_secs(1));
    })
    .pd(pd_wrapper)
    .build();
    let mut client = cluster.new_client();
    let keyspace_id = ApiV2::get_u32_keyspace_id_by_key(&i_to_key(0)).unwrap();
    client.split_keyspace(keyspace_id);
    cluster.wait_pd_region_count(3);

    Runtime::new().unwrap().block_on(async {
        let mut txn_client = cluster.new_txn_client().await;

        // Check TSO.
        {
            let pd_client = cluster.get_pd_client();
            let mut last_tso = 0;
            for i in 0..=10 {
                let tso = if i % 2 == 0 {
                    pd_client.get_tso().await.unwrap().into_inner()
                } else {
                    txn_client.current_timestamp().await.unwrap().version()
                };
                assert!(tso > last_tso);
                last_tso = tso;
            }
        }

        // Prepare data.
        let mut client = cluster.new_client();
        {
            client.put_kv(0..100, i_to_key, i_to_val);
            client.put_kv(100..200, i_to_key, i_to_val);
            client.put_kv(200..300, i_to_key, i_to_val);
            let split_keys = vec![i_to_key(50), i_to_key(150), i_to_key(200)];
            for split_key in &split_keys {
                client.split(split_key);
            }
            cluster.wait_pd_region_count(6);
            client.verify_data_with_ref_store();
        }
        let mut ref_store = client.dump_ref_store();

        // Verify by scan.
        assert_eq!(
            txn_client
                .verify_data_by_scan(&ref_store, None)
                .await
                .unwrap(),
            300
        );
        assert_eq!(
            txn_client
                .verify_data_by_scan(&ref_store, Some((&i_to_key(100), &i_to_key(250))))
                .await
                .unwrap(),
            150
        );

        // Unsafe destroy range.
        {
            let key0 = i_to_key(0);
            let key100 = i_to_key(100);
            let key200 = i_to_key(200);
            let prefix0 = &key0.as_slice()[..key0.len() - 2];
            let prefix1 = &key100.as_slice()[..key100.len() - 2];
            let prefix2 = &key200.as_slice()[..key200.len() - 2];
            txn_client
                .kv_unsafe_destroy_range(prefix1, prefix2, Duration::from_secs(10))
                .await
                .unwrap();
            ref_store.destroy_range(&key100, &key200);

            let snap0 = cluster.get_latest_snap(&key0).unwrap();
            assert!(snap0.has_data_in_prefix(prefix0), "snap: {:?}", snap0);

            let snap50 = cluster.get_latest_snap(&i_to_key(50)).unwrap();
            assert!(snap50.has_data_in_prefix(prefix0));
            assert!(!snap50.has_data_in_prefix(prefix1));

            let snap150 = cluster.get_latest_snap(&i_to_key(150)).unwrap();
            assert!(!snap150.has_data_in_prefix(prefix1));
            let snap200 = cluster.get_latest_snap(&i_to_key(200)).unwrap();
            assert!(snap200.has_data_in_prefix(prefix2));

            // Wait for del prefixes finished.
            let ok = try_wait(|| cluster.get_shards_has_del_prefixes().is_empty(), 10);
            assert!(ok, "{:?}", cluster.get_shards_has_del_prefixes());

            let verified = txn_client
                .verify_data_by_scan(&ref_store, None)
                .await
                .unwrap();
            assert_eq!(verified, 200);
        }
    });

    cluster.stop();
}

// Start pd-server to run this test:
// ```
//   bin/pd-server --client-urls http://127.0.0.1:4379 --peer-urls http://127.0.0.1:4380 --config pd-cse.toml
//   PD_ADDRS=127.0.0.1:4379 cargo test -p test_cloud_server test_on_real_pd
// ```
// Note the pd-server must be cleared before test, otherwise cluster will fail
// when put store.
#[test]
fn test_on_real_pd() {
    test_util::init_log_for_test();

    let pd_addrs = match std::env::var("PD_ADDRS") {
        Ok(s) => s.split(',').map(|s| s.to_owned()).collect(),
        Err(_) => {
            info!("PD_ADDRS not set, skip test_on_real_pd");
            return;
        }
    };

    let runtime = Runtime::new().unwrap();
    let _guard = runtime.enter();

    let node_ids = alloc_node_id_vec(3);
    let pd_wrapper = PdWrapper::new_real(
        pd_addrs,
        &SecurityConfig::default(),
        ReadableDuration::secs(30),
    );
    let mut cluster = ServerClusterBuilder::new(node_ids, |_, _| {})
        .pd(pd_wrapper)
        .build();

    let mut client = cluster.new_client();
    let keyspace_id = ApiV2::get_u32_keyspace_id_by_key(&i_to_key(0)).unwrap();
    client.split_keyspace(keyspace_id);

    runtime.block_on(async {
        let pd_client = cluster.get_pd_client_ext();
        info!(
            "cluster id {}, tso {:?}",
            pd_client.get_cluster_id().unwrap(),
            pd_client.get_tso().await.unwrap()
        );

        // Prepare data.
        let mut client = cluster.new_client();
        client.put_kv(0..100, i_to_key, i_to_val);
        client.put_kv(100..200, i_to_key, i_to_val);
        client.put_kv(200..300, i_to_key, i_to_val);
        client.verify_data_with_ref_store();

        let mut txn_client = cluster.new_txn_client().await;
        let ref_store = client.dump_ref_store();
        assert_eq!(
            txn_client
                .verify_data_by_scan(&ref_store, None)
                .await
                .unwrap(),
            300
        );
    });

    cluster.stop();
}

// Start pd-server to run this test:
// ```
//   bin/pd-server --client-urls http://127.0.0.1:4379 --peer-urls http://127.0.0.1:4380 --config pd-cse.toml
//   PD_ADDRS=127.0.0.1:4379 cargo test -p test_cloud_server test_pd_control
// ```
// Note the pd-server must be cleared before test, otherwise cluster will fail
// when put store.
#[test]
fn test_pd_control() {
    test_util::init_log_for_test();

    let pd_addrs = match std::env::var("PD_ADDRS") {
        Ok(s) => s.split(',').map(|s| s.to_owned()).collect(),
        Err(_) => {
            info!("PD_ADDRS not set, skip test_pd_control");
            return;
        }
    };

    let runtime = Runtime::new().unwrap();
    let _guard = runtime.enter();

    let node_ids = alloc_node_id_vec(3);
    let pd_wrapper = PdWrapper::new_real(
        pd_addrs,
        &SecurityConfig::default(),
        ReadableDuration::secs(30),
    );
    let pd_ctl = Arc::new(pd_wrapper.get_pd_control().unwrap());
    let mut cluster = ServerClusterBuilder::new(node_ids, |_, _| {})
        .pd(pd_wrapper)
        .build();

    runtime.block_on(async {
        let ks_name = "ks_for_test";
        let keyspace = pd_ctl
            .create_keyspace(CreateKeyspaceParams {
                name: ks_name.to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        info!("pd_control::create_keyspace: {:?}", keyspace);
        assert!(keyspace.created_at > 0);

        let keyspace_get = pd_ctl.get_keyspace_by_name(ks_name).await.unwrap();
        info!("pd_control::get_keyspace_by_name: {:?}", keyspace_get);
        assert_eq!(keyspace, keyspace_get);

        let pause_dur = Duration::from_secs(2);
        try_wait_result_async(
            || {
                let pd_ctl = pd_ctl.clone();
                Box::pin(async move {
                    let schedulers = pd_ctl.list_schedulers(None).await.unwrap();
                    if !schedulers.is_empty() {
                        info!("pd_control::list_schedulers: {:?}", schedulers);
                        Ok(())
                    } else {
                        Err("schedulers is empty".to_string())
                    }
                })
            },
            10,
        )
        .await
        .unwrap();

        let config = pd_ctl.get_config().await.unwrap();
        info!("pd_control::get_config: {:?}", config);
        assert!(!config.schedule.max_store_down_time.is_empty());

        for store_id in cluster.get_stores() {
            let store_regions = pd_ctl.get_store_regions(store_id).await.unwrap();
            info!(
                "pd_control::get_store_regions: store_id {}, regions: {:?}",
                store_id, store_regions
            );
            if let Some(region) = store_regions.regions.first() {
                assert!(!region.start_key.is_empty() || !region.end_key.is_empty())
            }
        }

        let regions_num = pd_ctl.get_regions_number().await.unwrap();
        info!("pd_control::get_regions_number: {:?}", regions_num);
        assert!(regions_num > 0);

        let health = pd_ctl.health().await.unwrap();
        info!("pd_control::get_health: {:?}", health);

        let stores = pd_ctl.get_stores().await.unwrap();
        info!("pd_control::get_stores: {:?}", stores);

        {
            let node_id = *cluster.get_nodes().first().unwrap();
            let status_addr = &cluster.get_node_config(node_id).server.status_addr;
            let store = pd_ctl
                .find_store_by_status_address(status_addr)
                .await
                .unwrap()
                .unwrap();
            info!(
                "pd_control::find_store_by_status_address: status_addr {}, store {:?}",
                status_addr, store
            );
            assert_eq!(store.store.id, cluster.get_store_id(node_id));
        }

        {
            let store_id = *cluster.get_stores().first().unwrap();
            let store = pd_ctl.get_store(store_id).await.unwrap();
            info!("pd_control::get_store: {:?}", store);
            assert_eq!(store.store.id, store_id);
        }

        let tiflash_rule_group = pd_ctl.get_tiflash_placement_rule_group().await.unwrap();
        info!(
            "pd_control::get_tiflash_placement_rule_group: {:?}",
            tiflash_rule_group
        );

        for scheduler_name in &[
            "shuffle-leader-scheduler",
            "shuffle-region-scheduler",
            "random-merge-scheduler",
        ] {
            let param = CreateSchedulerParam {
                name: scheduler_name.to_string(),
                ..Default::default()
            };
            pd_ctl.create_scheduler(param).await.unwrap();
        }

        {
            for store_id in cluster.get_stores() {
                let (store, scheduler_name) = pd_ctl
                    .evict_store_leaders(store_id, Duration::from_secs(5))
                    .await
                    .unwrap();
                info!("pd_control::evict_store_leaders: {:?}", store);

                let msg = pd_ctl.remove_scheduler(&scheduler_name).await.unwrap();
                info!("pd_control::remove_scheduler: {}: {}", scheduler_name, msg);
            }
        }

        let operators = pd_ctl.get_operators().await.unwrap();
        info!("pd_control::get_operators: {:?}", operators);
        if let Some(op) = operators.first() {
            pd_ctl
                .cancel_operator_by_region(op.region_id)
                .await
                .unwrap();
            info!("pd_control::cancel_operator_by_region: {:?}", op);

            let operators = pd_ctl.get_operators().await.unwrap();
            info!(
                "pd_control::get_operators (after cancel the first one): {:?}",
                operators
            );
        }

        pd_ctl
            .pause_or_resume_scheduler("all", pause_dur)
            .await
            .unwrap();
        let schedulers = pd_ctl
            .list_schedulers(Some(SchedulerStatus::Paused))
            .await
            .unwrap();
        info!(
            "pd_control::list_schedulers after pause {:?}: {:?}",
            pause_dur, schedulers
        );
        if let Some(scheduler) = schedulers.first() {
            assert!(!scheduler.paused_at.is_empty());
        }

        tokio::time::sleep(pause_dur + Duration::from_secs(1)).await;
        let paused_schedulers = pd_ctl
            .list_schedulers(Some(SchedulerStatus::Paused))
            .await
            .unwrap();
        assert!(
            paused_schedulers.is_empty(),
            "paused_schedulers: {:?}",
            paused_schedulers
        );
        let schedulers = pd_ctl.list_schedulers(None).await.unwrap();
        info!("pd_control::list_schedulers after resume: {:?}", schedulers);
    });

    cluster.stop();
}

#[test]
fn test_tikv_worker() {
    test_util::init_log_for_test();

    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test");

    let node_ids = alloc_node_id_vec(3);
    let pd_wrapper = PdWrapper::new_test(1, &SecurityConfig::default(), None);
    let mut cluster = ServerClusterBuilder::new(node_ids.clone(), |_, conf| {
        conf.dfs = dfs_config.clone();
        conf.kvengine.max_del_range_delay = ReadableDuration(Duration::from_secs(1));
    })
    .pd(pd_wrapper)
    .build();
    cluster.start_tikv_workers(alloc_node_id_vec(2), TikvWorkerOptions::default());
    let mut client = cluster.new_client();
    let keyspace_id = ApiV2::get_u32_keyspace_id_by_key(&i_to_key(0)).unwrap();
    client.split_keyspace(keyspace_id);
    cluster.wait_pd_region_count(3);

    let rt = Runtime::new().unwrap();

    let security_mgr = Arc::new(SecurityManager::default());
    for node_id in node_ids {
        let tikv_ctl = RestfulClient::new(
            "tikv_ctl",
            vec![cluster.status_addr(node_id)],
            security_mgr.clone(),
        )
        .unwrap();
        let ok = try_wait(
            || {
                let compactors: Vec<String> =
                    rt.block_on(tikv_ctl.get("kvengine/compactor")).unwrap();
                !compactors.is_empty()
            },
            10,
        );
        assert!(ok);
    }

    rt.block_on(async {
        let mut txn_client = cluster.new_txn_client().await;

        // Prepare data.
        let mut client = cluster.new_client();
        client.put_kv(0..100, i_to_key, i_to_val);
        client.verify_data_with_ref_store();
        let mut ref_store = client.dump_ref_store();

        // Unsafe destroy range.
        {
            let key80 = i_to_key(80);
            let key90 = i_to_key(90);
            let prefix08 = &key80.as_slice()[..key80.len() - 1];
            let prefix09 = &key90.as_slice()[..key90.len() - 1];
            txn_client
                .kv_unsafe_destroy_range(prefix08, prefix09, Duration::from_secs(10))
                .await
                .unwrap();
            ref_store.destroy_range(&key80, &key90);

            // Wait for del prefixes finished.
            let ok = try_wait(|| cluster.get_shards_has_del_prefixes().is_empty(), 10);
            assert!(ok, "{:?}", cluster.get_shards_has_del_prefixes());

            let verified = txn_client
                .verify_data_by_scan(&ref_store, None)
                .await
                .unwrap();
            assert_eq!(verified, 90);
        }

        assert!(cloud_worker::REMOTE_COMPACT_REQ_HANDLE_HISTOGRAM.get_sample_count() > 0);
    });

    cluster.stop();
    oss.shutdown();
}

#[test]
fn test_builtin_dfs() {
    test_util::init_log_for_test();
    let node_ids = alloc_node_id_vec(6);
    let cluster = ServerCluster::new(node_ids.clone(), |_, conf| {
        conf.dfs.s3_endpoint = "local".to_string();
    });
    let pd_client = cluster.get_pd_client();
    cluster.wait_region_replicated(&[], 3);
    pd_client.disable_default_operator();
    let fs = cluster.get_dfs().unwrap();

    let mut rng = thread_rng();
    let mut write_data = BytesMut::new();
    write_data.resize(64, 0);
    rng.fill_bytes(write_data.as_bytes_mut());
    let write_data = write_data.freeze();
    let start_off = rng.gen_range(0..write_data.len());

    // move region to store [1, 2, 3]
    let mut region = pd_client.get_region(b"").unwrap();
    let stores: Vec<u64> = pd_client
        .get_stores()
        .unwrap()
        .into_iter()
        .map(|s| s.get_id())
        .collect();
    region_must_peer_stores(&pd_client, &region, stores[..3].to_vec());
    region = pd_client.get_region(b"").unwrap();

    let rt = fs.get_runtime().handle().clone();
    rt.block_on(async move {
        // Create file
        let mut opts = dfs::Options::default()
            .with_shard(region.id, region.get_region_epoch().version)
            .with_type(dfs::FileType::Sst);
        fs.create(42, write_data.clone(), opts).await.unwrap();

        // The file can be read correctly
        let read_data = fs.read_file(42, opts).await.unwrap();
        assert_eq!(read_data, write_data);

        let partial_read_data = fs
            .read_file(42, opts.with_start_off(start_off as u64))
            .await
            .unwrap();
        assert_eq!(partial_read_data, write_data.slice(start_off..));

        // The file can still be read correctly even if the Region has been completely
        // moved.
        region_must_peer_stores(&pd_client, &region, stores[3..].to_vec());
        pd_client.must_split_region(
            pd_client.get_region(b"").unwrap(),
            pdpb::CheckPolicy::Usekey,
            vec![Key::from_raw(b"test").into_encoded()],
        ); // Increase the epoch version to invalidate region_cache in builtin_dfs.
        region = pd_client.get_region(b"").unwrap();
        opts = dfs::Options::default()
            .with_shard(region.id, region.get_region_epoch().version)
            .with_type(dfs::FileType::Sst);
        let read_data = fs.read_file(42, opts).await.unwrap();
        assert_eq!(read_data, write_data);
        // The file can still be read correctly even if the Region does not exist.
        opts = dfs::Options::default()
            .with_shard(region.id + 1234, region.get_region_epoch().version + 5678)
            .with_type(dfs::FileType::Sst);
        let read_data = fs.read_file(42, opts).await.unwrap();
        assert_eq!(read_data, write_data);
    })
}

fn region_must_peer_stores(pd_client: &TestPdClient, region: &metapb::Region, store_ids: Vec<u64>) {
    for (i, &store_id) in store_ids.iter().enumerate() {
        let Some(peer) = region.peers.iter().find(|p| p.store_id == store_id) else {
            let peer = new_peer(store_id, 10000 + store_id);
            pd_client.must_add_peer(region.id, peer.clone());
            if i == 0 {
                pd_client.must_transfer_leader(region.id, peer);
            }
            continue;
        };
        if i == 0 {
            pd_client.must_transfer_leader(region.id, peer.clone());
        }
    }
    for peer in region.peers.iter() {
        if !store_ids.contains(&peer.store_id) {
            pd_client.must_remove_peer(region.id, peer.clone());
        }
    }
}

fn i_to_key(i: usize) -> Vec<u8> {
    format!("xkey{:08}", i).into_bytes()
}

fn i_to_tidb_key(i: usize) -> Vec<u8> {
    format!("t_key{:08}", i).into_bytes()
}

fn i_to_val(i: usize) -> Vec<u8> {
    // `repeat` must > 0. CSE treat empty value as not found.
    format!("val{:04}", i).repeat(i % 32 + 1).into_bytes()
}

fn prefixed_i_to_val(prefix: String) -> impl Fn(usize) -> Vec<u8> {
    move |i| {
        format!("{}:{:04}", prefix, i)
            .repeat(i % 32 + 1)
            .into_bytes()
    }
}
