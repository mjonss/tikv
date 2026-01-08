// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Arc, time::Duration};

use api_version::ApiV2;
use bytes::{Buf, Bytes};
use futures::executor::block_on;
use kvengine::{dfs::S3Fs, table::BIT_DELETE, WRITE_CF};
use kvproto::metapb;
use merged_engine::{MergedEngine, MergedEngineConfig, MergedEngineContext};
use native_br::{backup, common::send_request_to_store};
use pd_client::PdClient;
use rand::Rng;
use rfstore::store::{load_raft_engine_meta, ApplyContext, RAFT_INIT_LOG_INDEX};
use security::{GetSecurityManager, SecurityManager};
use test_cloud_server::{
    client::{RefStore, RequestOptions},
    oss::prepare_dfs,
    ServerCluster,
};
use test_pd_client::TestPdClient;
use tikv::config::TikvConfig;
use tikv_util::{
    codec::bytes::encode_bytes,
    config::{env_or_default, ReadableDuration, ReadableSize},
};
use tokio::runtime::Runtime;

use crate::{alloc_node_id_vec, i_to_key};

#[test]
fn test_merged_engine() {
    let mut count = 1;
    env_or_default("TEST_MERGED_ENGINE_COUNT", &mut count);
    test_util::init_log_for_test();
    for _ in 0..count {
        test_merged_engine_once();
    }
}

fn test_merged_engine_once() {
    let (base_dir, _oss, dfs_conf) = prepare_dfs("test_merged_engine_restart");
    let node_ids = alloc_node_id_vec(4);
    let mut cluster = ServerCluster::new(node_ids.clone(), |_, conf: &mut TikvConfig| {
        conf.dfs = dfs_conf.clone();
        conf.rfengine.lightweight_backup = true;
        conf.rfengine.target_file_size = ReadableSize::kb(256);
    });
    cluster.wait_region_replicated(&[], 3);
    let pd_client = cluster.get_pd_client();
    pd_client.disable_default_operator();
    let keyspace_id = ApiV2::get_u32_keyspace_id_by_key(&i_to_key(0)).unwrap();
    let mut client = cluster.new_client();
    client.split_keyspace(keyspace_id);
    let split_key = encode_bytes(&crate::i_to_key(700));
    block_on(pd_client.split_regions(vec![split_key])).unwrap();
    for i in 0..10 {
        client.put_kv(i * 100..i * 100 + 50, crate::i_to_key, crate::i_to_val);
    }
    let backup_config = backup::BackupConfig {
        dfs: dfs_conf.clone(),
        tolerate_err: 1,
        skip_keyspace_meta: true,
        ..Default::default()
    };
    let (_backup_key, backup_meta) = backup::backup_cluster(
        backup_config.clone(),
        "merged_engine_restart".to_string(),
        pd_client.as_ref(),
        None,
    )
    .unwrap();
    let s3fs = Arc::new(S3Fs::new_from_config(dfs_conf));
    let ctx = MergedEngineContext {
        pd: pd_client.clone(),
        fs: s3fs,
        local_dir: base_dir.path().join("merged_engine_restart"),
        master_key: cluster.get_kvengine(node_ids[0]).get_master_key(),
        config: MergedEngineConfig {
            block_cache_size: ReadableSize::mb(1).into(),
            timeout_fetch_wal: ReadableDuration::secs(10),
            merged_store_id: 1024,
            force_ia: false,
            mem_table_size: cluster.get_mem_table_size(),
            raft_write_batch_size: ReadableSize::kb(256),
            ..Default::default()
        },
        security_config: Arc::new(cluster.get_node_config(node_ids[0]).security.clone()),
        force_stop: Default::default(),
    };
    let mut merged_engine = MergedEngine::new(ctx.clone(), Some(backup_meta.clone())).unwrap();
    let merged_kv = merged_engine.get_kv();
    let mut keyspaces = HashMap::default();
    keyspaces.insert(keyspace_id, Bytes::new());
    merged_engine.load_shards(&keyspaces).unwrap();
    merged_engine
        .set_keyspace_states(keyspace_id, vec![0].into())
        .unwrap();
    let all_shards = merged_kv.get_all_shard_id_vers();
    assert_eq!(all_shards.len(), 2);
    let mut ref_store = kv_engine_to_ref_store(&merged_kv);
    client
        .verify_data_with_given_ref_store(&ref_store, None, &RequestOptions::default())
        .unwrap();
    let scheduler = cluster.new_scheduler();
    let mut rng = rand::thread_rng();
    for i in 0..10 {
        client.put_kv(i * 100 + 50..i * 100 + 60, crate::i_to_key, crate::i_to_val);
        if i < 8 {
            if rng.gen_bool(0.5) {
                let encoded_key_500 = encode_bytes(&crate::i_to_key(i * 100));
                block_on(pd_client.split_regions(vec![encoded_key_500])).unwrap();
            } else {
                scheduler.move_random_region();
            }
        } else {
            scheduler.merge_random_region(true);
        }
        if rng.gen_bool(0.2) {
            update_merged_engine(&pd_client, &cluster, &mut merged_engine);
            let ref_store = kv_engine_to_ref_store(&merged_kv);
            client
                .verify_data_with_given_ref_store(&ref_store, None, &RequestOptions::default())
                .unwrap();
        }
    }
    update_merged_engine(&pd_client, &cluster, &mut merged_engine);
    ref_store = kv_engine_to_ref_store(&merged_kv);
    client
        .verify_data_with_given_ref_store(&ref_store, None, &RequestOptions::default())
        .unwrap();
    let merged_raft = merged_engine.get_raft();
    let region_peers = merged_raft.get_region_peer_map();
    for (region_id, _) in region_peers {
        if let Some(progress) = merged_engine.get_region_progress(region_id) {
            let truncated_index = merged_raft.get_truncated_index(region_id).unwrap();

            if progress.keyspace_id != keyspace_id {
                // Regions out of the keyspace may have no change (when all peers split fast
                // enough). Then truncated index will not be updated.
                assert!(
                    truncated_index == progress.data_persisted_log_index()
                        || truncated_index == RAFT_INIT_LOG_INDEX
                );
                continue;
            }

            if progress.data_persisted_log_index() != truncated_index {
                if let Some(cs) = load_raft_engine_meta(&merged_raft, region_id) {
                    // When the shard has parent/dependents, raft logs are not truncated to
                    // `progress.truncated_index`.
                    let has_dependents = merged_raft.has_dependents(region_id);
                    assert!(
                        cs.has_parent() || has_dependents,
                        "truncated index mismatch: cs: {:?}, has_dependents {}, progress: {:?}, truncated_index: {}",
                        cs,
                        has_dependents,
                        progress,
                        truncated_index
                    );
                }
            }
        }
    }
    merged_engine.close();
    let mut merged_engine = MergedEngine::new(ctx.clone(), Some(backup_meta.clone())).unwrap();
    let merged_kv = merged_engine.get_kv();
    let ref_store = kv_engine_to_ref_store(&merged_kv);
    client
        .verify_data_with_given_ref_store(&ref_store, None, &RequestOptions::default())
        .unwrap();
    for i in 0..10 {
        client.put_kv(i * 100 + 60..i * 100 + 70, crate::i_to_key, crate::i_to_val);
        if i < 8 {
            if rng.gen_bool(0.5) {
                let encoded_key_500 = encode_bytes(&crate::i_to_key(i * 100));
                block_on(pd_client.split_regions(vec![encoded_key_500])).unwrap();
            } else {
                scheduler.move_random_region();
            }
        } else {
            scheduler.merge_random_region(true);
        }
        if rng.gen_bool(0.2) {
            update_merged_engine(&pd_client, &cluster, &mut merged_engine);
            let ref_store = kv_engine_to_ref_store(&merged_kv);
            client
                .verify_data_with_given_ref_store(&ref_store, None, &RequestOptions::default())
                .unwrap();
        }
    }
    update_merged_engine(&pd_client, &cluster, &mut merged_engine);
    let ref_store = kv_engine_to_ref_store(&merged_kv);
    client
        .verify_data_with_given_ref_store(&ref_store, None, &RequestOptions::default())
        .unwrap();
    let merged_raft = merged_engine.get_raft();
    let region_peers = merged_raft.get_region_peer_map();
    for (region_id, _) in region_peers {
        if let Some(progress) = merged_engine.get_region_progress(region_id) {
            let truncated_index = merged_raft.get_truncated_index(region_id).unwrap();

            if progress.keyspace_id != keyspace_id {
                assert!(
                    truncated_index == progress.data_persisted_log_index()
                        || truncated_index == RAFT_INIT_LOG_INDEX
                );
                continue;
            }

            assert_eq!(progress.data_persisted_log_index(), truncated_index);
        }
    }
    cluster.stop();
}

fn update_merged_engine(
    pd_client: &TestPdClient,
    cluster: &ServerCluster,
    merged_engine: &mut MergedEngine,
) {
    let security_mgr = pd_client.get_security_mgr();
    let dfs = cluster.get_dfs().unwrap();
    let stores = cluster.get_stores();
    for store_id in stores {
        let store = pd_client.get_store(store_id).unwrap();
        update_merged_engine_for_store(&security_mgr, merged_engine, &store, dfs.get_runtime());
    }
    let router = merged_engine.get_router();
    let mut apply_ctx = ApplyContext::new(merged_engine.get_kv(), Some(router));
    merged_engine.sync_merged(&mut apply_ctx, None).unwrap();
}

fn update_merged_engine_for_store(
    security_mgr: &SecurityManager,
    merged_engine: &mut MergedEngine,
    store: &metapb::Store,
    runtime: &Runtime,
) {
    let store_id = store.id;
    let store_progress = merged_engine.get_store_progress(store_id).unwrap();
    let mut epoch = store_progress.epoch;
    let mut start_off = store_progress.offset;
    let http_client = security_mgr.http_client(hyper::Client::builder()).unwrap();
    loop {
        let uri = security_mgr
            .build_uri(format!(
                "{}/rfengine/wal_chunk?epoch_id={}&start_off={}&end_off=0",
                store.status_address, epoch, start_off
            ))
            .unwrap();
        let req = http::Request::get(uri.clone())
            .body(hyper::Body::empty())
            .unwrap();
        let (status, data) = runtime
            .block_on(send_request_to_store(
                req,
                store,
                &http_client,
                Duration::from_secs(10),
            ))
            .unwrap();
        let end_off = start_off + data.len() as u64;
        merged_engine
            .update_wal(store_id, epoch, start_off, end_off, data.reader())
            .unwrap();
        if status == http::StatusCode::PARTIAL_CONTENT {
            merged_engine.rotate_wal(store_id, epoch, end_off).unwrap();
            epoch += 1;
            start_off = 0;
            continue;
        }
        return;
    }
}

fn kv_engine_to_ref_store(kv: &kvengine::Engine) -> RefStore {
    let mut ref_store = RefStore::default();
    for id_ver in kv.get_all_shard_id_vers() {
        let shard = kv.get_shard(id_ver.id).unwrap();
        let snap_access = shard.new_snap_access();
        let mut iter = snap_access.new_iterator(WRITE_CF, false, false, None, false);
        iter.rewind();
        while iter.valid() {
            if iter.meta() == BIT_DELETE {
                ref_store.insert(iter.key().to_vec(), None);
            } else {
                ref_store.insert(iter.key().to_vec(), Some(iter.val().to_vec()));
            }
            iter.next()
        }
    }
    ref_store
}
