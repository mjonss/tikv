// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]
#![feature(box_patterns)]
#![feature(custom_test_frameworks)]
#![feature(assert_matches)]
#![test_runner(test_util::run_tests)]

use std::{str::FromStr, time::Duration};

use api_version::ApiV2;
use bytes::Bytes;
use http::Uri;
use hyper::{Body, Request};
use kvengine::ShardStats;
use kvproto::{kvrpcpb::UnsafeDestroyRangeRequest, metapb::Store};
use pd_client::PdClient;
use security::SecurityConfig;
use test_cloud_server::{ServerCluster, client::ClusterClient, try_wait};
use test_pd_client::TestPdClient;
use tidb_query_common::util::convert_to_prefix_next;
use tikv_util::{codec::bytes::encode_bytes, info};
use tokio::runtime::Runtime;
mod backup;
mod columnar;
mod delete_range;
mod engine_basic;
mod gc;
mod ia_file;
mod load_data;
mod major_compaction;
mod merge;
mod merged_engine;
mod native_backup;
mod replica_read;
mod rfengine;
mod split;
mod storage_class;
mod transaction;
mod unsafe_recover;

pub use test_cloud_server::{alloc_node_id, alloc_node_id_vec};

pub(crate) fn get_keyspace_prefix(keyspace_id: u32) -> Vec<u8> {
    let mut prefix = keyspace_id.to_be_bytes();
    prefix[0] = b'x';
    prefix.to_vec()
}

pub(crate) fn generate_keyspace_key(keyspace_id: u32) -> impl Fn(usize) -> Vec<u8> {
    move |i: usize| -> Vec<u8> {
        let mut key = get_keyspace_prefix(keyspace_id);
        key.extend(i_to_tidb_key(i));
        key
    }
}

pub(crate) fn is_region_belongs_to_keyspace(
    region: &kvproto::metapb::Region,
    keyspace_id: u32,
) -> bool {
    let keyspace_prefix = get_keyspace_prefix(keyspace_id);
    let keypsace_next_prefix = get_keyspace_prefix(keyspace_id + 1);
    let start_key = region.get_start_key();
    let end_key = region.get_end_key();
    if start_key.is_empty() || end_key.is_empty() {
        return false;
    }
    start_key.starts_with(&keyspace_prefix)
        && (end_key.starts_with(&keyspace_prefix) || end_key == keypsace_next_prefix.as_slice())
}

pub(crate) fn i_to_key(i: usize) -> Vec<u8> {
    format!("xkey_{:08}", i).into_bytes()
}

pub(crate) fn i_to_tidb_key(i: usize) -> Vec<u8> {
    format!("t_key_{:08}", i).into_bytes()
}

pub(crate) fn i_to_val(i: usize) -> Vec<u8> {
    format!("val_{:03}", i).into_bytes().repeat(3)
}

pub(crate) fn i_to_val_opt(prefix: &str, repeat: usize) -> impl Fn(usize) -> Vec<u8> + '_ {
    move |i: usize| -> Vec<u8> { format!("{}{:03}", prefix, i).into_bytes().repeat(repeat) }
}

/// Generate keys of API v1 (TiDB metas)
pub(crate) fn i_to_key_v1(i: usize) -> Vec<u8> {
    format!("m_{:08}", i).into_bytes()
}

pub(crate) fn random_value(value_len: usize) -> Vec<u8> {
    use rand::RngCore as _;
    let mut value = vec![0u8; value_len];
    rand::thread_rng().fill_bytes(value.as_mut_slice());
    value
}

pub(crate) async fn request_major_compact_on_store(
    store: &Store,
    query: &str,
    permit_not_found: bool,
) {
    let uri = Uri::from_str(&format!(
        "http://{}/major-compact?{}",
        &store.status_address, query
    ))
    .unwrap();
    let req = Request::post(uri).body(Body::empty()).unwrap();
    let client = hyper::Client::new();
    let resp: http::Response<Body> = client.request(req).await.unwrap();
    let is_success = resp.status().is_success()
        || (permit_not_found && resp.status() == http::StatusCode::NOT_FOUND);
    assert!(
        is_success,
        "{:?}",
        hyper::body::to_bytes(resp.into_body()).await.unwrap()
    );
    hyper::body::to_bytes(resp.into_body()).await.unwrap();
}

pub(crate) fn i_to_key_with_keyspace(keyspace_id: u32) -> impl Fn(usize) -> Vec<u8> {
    move |i: usize| -> Vec<u8> {
        let mut key = ApiV2::get_txn_keyspace_prefix(keyspace_id);
        key.extend(format!("tkey{:08}", i).into_bytes());
        key
    }
}

pub(crate) fn i_to_val_with_size(size: usize) -> impl Fn(usize) -> Vec<u8> {
    move |i: usize| -> Vec<u8> { format!("{:0size$}", i, size = size).into_bytes() }
}

pub(crate) fn new_destroy_range_req(prefix: &[u8]) -> UnsafeDestroyRangeRequest {
    let mut req = UnsafeDestroyRangeRequest::default();
    let mut end_key = prefix.to_vec();
    convert_to_prefix_next(&mut end_key);
    req.set_start_key(prefix.to_vec());
    req.set_end_key(end_key);
    req
}

pub(crate) fn destroy_range(client: &mut ClusterClient, store_id: u64, prefix: &[u8]) {
    let req = new_destroy_range_req(prefix);
    let kv_client = client.get_kv_client(store_id);
    let resp = kv_client.unsafe_destroy_range(&req).unwrap();
    assert!(resp.get_error().is_empty(), "{:?}", resp.get_error());
    assert!(!resp.has_region_error());
}

pub(crate) async fn request_dump_snapshot_on_store(
    store: &Store,
    shard_id: u64,
    shard_ver: u64,
    start_ts: u64,
) -> Bytes {
    let uri = Uri::from_str(&format!(
        "http://{}/kvengine/snapshot/{}?start_ts={}&shard_ver={}",
        &store.status_address, shard_id, start_ts, shard_ver,
    ))
    .unwrap();
    let req = Request::get(uri).body(Body::empty()).unwrap();
    let client = hyper::Client::new();
    let resp: http::Response<Body> = client.request(req).await.unwrap();
    assert!(
        resp.status().is_success(),
        "{:?}",
        hyper::body::to_bytes(resp.into_body()).await.unwrap()
    );
    hyper::body::to_bytes(resp.into_body()).await.unwrap()
}

pub(crate) fn new_security_config() -> SecurityConfig {
    let mut conf = SecurityConfig::default();
    conf.master_key.vendor = "test".to_string();
    conf.master_key.key_id = "random".to_string();
    conf
}

pub(crate) fn request_major_compaction(
    runtime: &Runtime,
    pd_client: &TestPdClient,
    keyspace_id: u32,
) {
    let stores = pd_client.get_all_stores(true).unwrap();
    let mut handles = Vec::with_capacity(stores.len());
    for store in stores {
        handles.push(runtime.spawn(async move {
            let query = format!("major_compact=true&keyspace_id={}", keyspace_id);
            request_major_compact_on_store(&store, query.as_str(), true).await;
        }));
    }
    for handle in handles {
        runtime.block_on(handle).unwrap();
    }
}

/// Wait for stats of all regions in a keyspace to be expected.
///
/// Note that when `expect_all = false`, any peer has the expected stats, the
/// region will be considered as expected. This can be used in the scene of
/// restoration, as we restore from rfengine of leader peer, when any peer
/// become expected, the rfengine meta of leader must be expected.
fn wait_for_keyspace_stats(
    runtime: &Runtime,
    cluster: &ServerCluster,
    pd_client: &TestPdClient,
    keyspace_id: u32,
    expect: impl Fn(&ShardStats) -> bool,
    expect_all: bool,
    timeout: Duration,
) -> std::result::Result<(), String> {
    let regions = runtime
        .block_on(pd_client.scan_regions(
            encode_bytes(&get_keyspace_prefix(keyspace_id)),
            encode_bytes(&get_keyspace_prefix(keyspace_id + 1)),
            100,
        ))
        .unwrap();
    info!("regions {:?}", regions);
    let node_ids = cluster.get_nodes();
    let get_stats = |region_id: u64| -> Vec<ShardStats> {
        node_ids
            .iter()
            .filter_map(|id| cluster.get_kvengine(*id).get_shard_stat_opt(region_id))
            .collect()
    };
    for region in regions {
        let region_id = region.get_region().id;
        let ok = try_wait(
            || {
                let stats = get_stats(region_id);
                if expect_all {
                    stats.iter().all(|stats| expect(stats))
                } else {
                    stats.iter().any(|stats| expect(stats))
                }
            },
            timeout.as_secs() as usize,
        );

        if !ok {
            let err = format!(
                "wait_for_keyspace_stats timeout, region_id: {}, stats: {:?}",
                region_id,
                get_stats(region_id),
            );
            return Err(err);
        }
    }
    Ok(())
}
