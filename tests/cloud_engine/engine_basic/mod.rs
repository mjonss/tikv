// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod test_blob_store;
mod test_remote_coprocessor;
mod test_stats;
mod test_store;

use std::{sync::Arc, thread, time::Duration};

use api_version::ApiV2;
use bytes::Bytes;
use futures::executor::block_on;
use kvengine::{Shard, SnapAccess};
use kvenginepb::ChangeSet;
use kvproto::kvrpcpb::Op;
use pd_client::PdClient;
use protobuf::Message;
use replication_worker::CdcApplyObserver;
use rfstore::store::ApplyContext;
use test_cloud_server::{ServerCluster, client::TxnMutations, must_wait, try_wait, util::Mutation};
use test_util::init_log_for_test;
use tikv::storage::{Scanner, txn::CloudStoreScanner};
use tikv_util::{codec::bytes::encode_bytes, config::ReadableSize};
use txn_types::{Key, TsSet};

use crate::alloc_node_id;

#[test]
fn test_engine_auto_switch() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let mut cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.rocksdb.writecf.write_buffer_size = ReadableSize::kb(256);
    });
    let mut client = cluster.new_client();
    client.put_kv(0..100, i_to_key, i_to_val);
    client.put_kv(100..200, i_to_key, i_to_val);
    client.put_kv(200..300, i_to_key, i_to_val);
    let region_id = client.get_region_id(&[]);
    let engine = cluster.get_kvengine(node_id);
    let stats = engine.get_shard_stat(region_id);
    assert!(stats.mem_table_count + stats.l0_table_count > 1);
    cluster.stop();
}

fn i_to_key(i: usize) -> Vec<u8> {
    format!("x123key_{:05}", i).into_bytes()
}

fn i_to_val(i: usize) -> Vec<u8> {
    format!("val_{:05}", i).into_bytes().repeat(100)
}

#[test]
fn test_split_by_key() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let mut cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.coprocessor.region_split_keys = Some(3);
    });
    let mut client = cluster.new_client();
    client.put_kv(0..5, i_to_key, i_to_key);
    let engine = cluster.get_kvengine(node_id);
    let _ = try_wait(|| engine.get_all_shard_id_vers().len() == 2, 10);
    let shard_stats = engine.get_all_shard_stats();
    assert!(shard_stats.len() == 2, "{:?}", &shard_stats);
    client.put_kv(6..15, i_to_key, i_to_key);
    let _ = try_wait(|| engine.get_all_shard_id_vers().len() == 5, 10);
    let shard_stats = engine.get_all_shard_stats();
    assert!(shard_stats.len() == 5, "{:?}", &shard_stats);
    cluster.stop();
}

#[test]
fn test_split_whole_keyspace() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let mut cluster = ServerCluster::new(vec![node_id], |_, _| {});
    let mut client = cluster.new_client();
    let default_keyspace_i_to_key =
        |i: usize| -> Vec<u8> { format!("t123key_{:05}", i).into_bytes() };
    client.put_kv(0..10, default_keyspace_i_to_key, i_to_val);
    client.split_keyspace(1);
    cluster.wait_pd_region_count(3);
    let pd_client = cluster.get_pd_client();
    let keyspace_prefix = ApiV2::get_txn_keyspace_prefix(1);
    let region = pd_client
        .get_region(&encode_bytes(&keyspace_prefix))
        .unwrap();
    let kv = cluster.get_kvengine(node_id);
    let shard = kv.get_shard(region.id).unwrap();
    let stats = shard.get_stats();
    assert_eq!(stats.kv_size, 0);
    cluster.stop();
}

#[test]
fn test_remove_and_add_peer() {
    test_util::init_log_for_test();
    let node_ids = vec![alloc_node_id(), alloc_node_id(), alloc_node_id()];
    let mut cluster = ServerCluster::new(node_ids.clone(), |_, _| {});
    cluster.wait_region_replicated(&[], 3);
    let mut client = cluster.new_client();
    let split_key = i_to_key(5);
    client.split(&split_key);
    // Wait for region heartbeat to update region epoch in PD.
    sleep();

    client.put_kv(0..10, i_to_key, i_to_key);
    let pd = cluster.get_pd_client();
    cluster.wait_pd_region_count(2);
    pd.disable_default_operator();
    let &first_node = node_ids.first().unwrap();
    cluster.remove_node_peers(first_node);
    // After one store has removed peer, the cluster is still available.
    let mut client = cluster.new_client();
    client.put_kv(0..10, i_to_key, i_to_key);
    cluster.stop_node(first_node);
    thread::sleep(Duration::from_millis(100));
    cluster.start_node(first_node, |_, _| {});
    pd.enable_default_operator();
    cluster.wait_region_replicated(&[], 3);
    cluster.wait_region_replicated(&split_key, 3);
    cluster.stop();
}

#[test]
fn test_increasing_put_and_split() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let mut cluster = ServerCluster::new(vec![node_id], |_, _| {});
    let mut client = cluster.new_client();
    client.put_kv(0..50, i_to_key, i_to_val);
    for i in 1..5 {
        let split_idx = i * 10;
        let split_key = i_to_key(split_idx);
        client.split(&split_key);
        for _ in 0..10 {
            client.put_kv(split_idx..split_idx + 5, i_to_key, i_to_val);
        }
    }
    cluster.stop()
}

#[test]
fn test_cloud_store_reverse_scan() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let cluster = ServerCluster::new(vec![node_id], |_, _| {});
    let mut client = cluster.new_client();
    let keyspace_id = ApiV2::get_u32_keyspace_id_by_key("x123".as_bytes()).unwrap();
    client.split_keyspace(keyspace_id);
    cluster.wait_pd_region_count(3);
    client.put_kv(1..6, i_to_key, i_to_val);
    let region_id = client.get_region_id("x123".as_bytes());
    let engine = cluster.get_kvengine(node_id);
    let snapshot = engine.get_snap_access(region_id).unwrap();
    for rev in [true, false] {
        let lower_bound = Some(Key::from_raw(&i_to_key(2)));
        let upper_bound = Some(Key::from_raw(&i_to_key(5)));
        let mut scanner = CloudStoreScanner::new(
            snapshot.clone(),
            rev,
            true,
            TsSet::Empty,
            u64::MAX,
            lower_bound,
            upper_bound,
            false,
        )
        .unwrap();
        let mut indices = vec![2, 3, 4];
        if rev {
            indices.reverse();
        }
        for i in indices {
            let (key, _) = scanner.next().unwrap().unwrap();
            assert_eq!(key.into_raw().unwrap(), i_to_key(i));
        }
        assert!(scanner.next().unwrap().is_none());
    }
}

#[test]
fn test_snap_marshal() {
    init_log_for_test();
    let node_id = alloc_node_id();
    let mut cluster = ServerCluster::new(vec![node_id], |_, _| {});
    let mut client = cluster.new_client();
    let keyspace_id = ApiV2::get_u32_keyspace_id_by_key("x123".as_bytes()).unwrap_or_default();
    client.split_keyspace(keyspace_id);
    client.put_kv(0..100, i_to_key, i_to_val);
    client.put_kv(100..200, i_to_key, i_to_val);
    let region_id = client.get_region_id(&i_to_key(0));
    let kvengine = cluster.get_kvengine(node_id);
    let check_table_count = |start: Bytes, end: Bytes, check_l0: bool| -> bool {
        let snap = kvengine.get_snap_access(region_id).unwrap();
        let (_, cs_bin) = snap.marshal(&[(start, end)], false, false);
        let mut cs = ChangeSet::default();
        cs.merge_from_bytes(&cs_bin).unwrap();
        let cs_snap = cs.get_snapshot();
        if check_l0 {
            !cs_snap.get_l0_creates().is_empty()
        } else {
            !cs_snap.get_table_creates().is_empty()
        }
    };
    must_wait(
        || check_table_count(Bytes::from(i_to_key(0)), Bytes::from(i_to_key(500)), true),
        10,
        || "snapshot table count is zero".to_string(),
    );
    client.put_kv(200..300, i_to_key, i_to_val);
    client.put_kv(300..400, i_to_key, i_to_val);
    client.put_kv(400..500, i_to_key, i_to_val);
    must_wait(
        || check_table_count(Bytes::from("x123"), Bytes::from("x124"), false),
        10,
        || "snapshot table count is zero".to_string(),
    );
    cluster.stop();
}

#[test]
fn test_apply_observer() {
    init_log_for_test();
    let node_id = alloc_node_id();
    let cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.rocksdb.writecf.write_buffer_size = ReadableSize::mb(1);
    });
    let mut client = cluster.new_client();
    let region_id = client.get_region_id(&i_to_key(0));
    let rfengine = cluster.get_rfengine(node_id);
    let store_id = rfengine.get_engine_id();
    let region_peer_map = rfengine.get_region_peer_map();
    let &peer_id = region_peer_map.get(&region_id).unwrap();
    let kvengine = cluster.get_kvengine(node_id);
    let shard_meta = rfstore::store::load_engine_meta(&rfengine, store_id, peer_id).unwrap();
    let shard_cs = shard_meta.to_change_set();
    let shard = Arc::new(Shard::new_for_ingest(
        kvengine.get_engine_id(),
        &shard_cs,
        kvengine.opts.clone(),
        &kvengine.get_master_key(),
    ));
    for i in 0..10 {
        let start = i * 10;
        client.put_kv(start..start + 10, i_to_key, i_to_val);
    }
    let recoverer = rfstore::store::RecoverHandler::new(rfengine.clone());
    let mut apply_ctx = ApplyContext::new(kvengine.clone(), None);

    let (tx, rx) = tikv_util::mpsc::unbounded();
    let runtime = cluster.get_dfs().unwrap().get_runtime().handle().clone();
    let observer = CdcApplyObserver::new(kvengine.clone(), tx.clone(), runtime);
    apply_ctx.set_apply_observer(Box::new(observer));
    recoverer
        .recover_with_apply_ctx(&mut apply_ctx, &shard, &shard_meta, false)
        .unwrap();
    let mut observer = apply_ctx.take_apply_observer().unwrap();
    observer.flush();
    let msg = rx.recv().unwrap();
    match msg {
        replication_worker::CdcMsg::Applied { region_events, .. } => {
            let mut i = 0;
            for event in &region_events.events {
                for row in event.get_entries().get_entries() {
                    if row.get_type() == kvproto::cdcpb::EventLogType::Committed {
                        let trim_keyspace_key = i_to_key(i)[4..].to_vec();
                        assert_eq!(row.get_key(), &trim_keyspace_key);
                        assert_eq!(row.get_value(), i_to_val(i));
                        i += 1;
                    }
                }
            }
            assert_eq!(i, 100);
        }
        _ => panic!("unexpected message"),
    }
}

#[test]
fn test_cloud_store_reset_range() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let cluster = ServerCluster::new(vec![node_id], |_, _| {});
    let mut client = cluster.new_client();

    let keyspace_id =
        api_version::ApiV2::get_u32_keyspace_id_by_key("x123".as_bytes()).unwrap_or_default();
    client.split_keyspace(keyspace_id);

    client.put_kv(3..10, i_to_key, i_to_val);
    client.put_kv(30..40, i_to_key, i_to_val);

    let region_id = client.get_region_id("x123".as_bytes());
    let engine = cluster.get_kvengine(node_id);
    let snap = engine.get_snap_access(region_id).unwrap();
    let read_ts = block_on(cluster.get_pd_client().get_tso())
        .unwrap()
        .into_inner();
    let mut scanner = new_scanner(&snap, false, read_ts);
    let cases = vec![
        ((9, 10), Some(9), true), // old range is not set should seek.
        ((10, 19), None, true),   // inner is smaller, should seek.
        ((19, 25), None, false),
        ((23, 27), None, true), // none monotonic reset range do not skip seek.
        ((28, 35), Some(30), false),
        ((37, 39), Some(37), true),
        ((40, 70), None, true),  // inner is smaller, should seek.
        ((70, 80), None, false), // inner is invalid, should not seek.
    ];
    for ((start, end), key, seek) in cases {
        check_scanner(&mut scanner, false, start, end, key, seek);
    }
    let mut desc_scanner = new_scanner(&snap, true, read_ts);
    let desc_cases = vec![
        ((40, 70), None, true),      // old range is not set should seek.
        ((37, 40), Some(39), false), // inner is equal to upper, skip seek.
        ((36, 38), Some(37), true),  // none monotonic reset range do not skip seek.
        ((23, 27), None, true),      // inner is greater should seek.
        ((15, 20), None, false),     // inner is at 9, skip seek.
        ((5, 9), Some(8), false),
        ((2, 3), None, true),  // inner is greater, should seek.
        ((0, 2), None, false), // inner is invalid, should not seek.
    ];
    for ((start, end), key, seek) in desc_cases {
        check_scanner(&mut desc_scanner, true, start, end, key, seek);
    }

    // test lock.
    let keys_nums = [21, 31, 51];
    let mutations: Vec<Mutation> = keys_nums
        .iter()
        .map(|&k_num| {
            let mut m = Mutation::default();
            m.set_key(i_to_key(k_num));
            m.set_value(i_to_val(k_num));
            m.set_op(Op::Put);
            m
        })
        .collect();
    let start_ts = block_on(cluster.get_pd_client().get_tso()).unwrap();
    let txn_muts = TxnMutations::from_normal(mutations);
    client
        .kv_prewrite(txn_muts.primary(), None, txn_muts, start_ts)
        .expect("kv_prewrite");

    let snap = engine.get_snap_access(region_id).unwrap();
    let (locked, seeked) = (true, true);
    let read_ts = block_on(cluster.get_pd_client().get_tso())
        .unwrap()
        .into_inner();
    let mut scanner = new_scanner(&snap, false, read_ts);
    let lock_cases = vec![
        ((0, 10), false, seeked),
        ((10, 20), false, false),
        ((20, 25), locked, false),
        ((25, 30), false, seeked),
        ((30, 40), locked, false),
        ((55, 60), false, seeked),
        ((60, 65), false, false),
        ((70, 85), false, false),
    ];
    for ((lower, upper), is_err, seeked) in lock_cases {
        reset_range(&mut scanner, false, lower, upper);
        if is_err {
            scanner.next().unwrap_err();
        } else {
            scanner.next().unwrap();
        }
        assert_eq!(scanner.take_statistics().lock.seek, seeked as usize);
    }

    let mut desc_scanner = new_scanner(&snap, true, read_ts);
    let desc_lock_cases = vec![
        ((70, 85), false, seeked),
        ((60, 65), false, false),
        ((55, 60), false, false),
        ((30, 40), locked, seeked), // locked(31)
        ((25, 30), false, seeked),
        ((20, 25), locked, false), // locked(21)
        ((10, 20), false, seeked),
        ((0, 10), false, false),
    ];
    for ((lower, upper), is_err, seeked) in desc_lock_cases {
        reset_range(&mut desc_scanner, true, lower, upper);
        if is_err {
            desc_scanner.next().unwrap_err();
        } else {
            desc_scanner.next().unwrap();
        }
        assert_eq!(desc_scanner.take_statistics().lock.seek, seeked as usize);
    }
}

fn new_scanner(snap: &SnapAccess, desc: bool, read_ts: u64) -> CloudStoreScanner {
    CloudStoreScanner::new(
        snap.clone(),
        desc,
        true,
        TsSet::Empty,
        read_ts,
        None,
        None,
        false,
    )
    .unwrap()
}

fn check_scanner(
    scanner: &mut CloudStoreScanner,
    desc: bool,
    start: usize,
    end: usize,
    key: Option<usize>,
    seek: bool,
) {
    reset_range(scanner, desc, start, end);
    if let Some(key) = key {
        assert_eq!(next_key(scanner), i_to_key(key));
    } else {
        assert!(scanner.next().unwrap().is_none());
    }
    let stats = scanner.take_statistics();
    if seek {
        assert_eq!(stats.write.seek, 1);
    } else {
        assert_eq!(stats.write.seek, 0);
        assert_eq!(stats.lock.seek, 0);
    }
}

fn reset_range(scanner: &mut CloudStoreScanner, desc: bool, lower: usize, upper: usize) {
    scanner
        .reset_range(
            desc,
            Some(Key::from_raw(&i_to_key(lower))),
            Some(Key::from_raw(&i_to_key(upper))),
        )
        .unwrap();
}

fn next_key(scanner: &mut CloudStoreScanner) -> Vec<u8> {
    scanner
        .next()
        .unwrap()
        .map(|(k, _)| k.into_raw().unwrap())
        .unwrap()
}

fn sleep() {
    std::thread::sleep(Duration::from_millis(100));
}
