// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, fs, time::Duration};

use collections::HashSet;
use kvengine::{
    ShardStats, new_schema_filename, new_tmp_filename, new_vector_index_filename,
    table::{get_local_dir, sstable::new_filename},
};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftRequestHeader};
use pd_client::PdClient;
use rand::Rng;
use rfstore::store::rlog::*;
use test_cloud_server::{ServerCluster, must_wait};
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    info,
    time::Instant,
};

use crate::alloc_node_id;

#[test]
fn test_local_file_gc() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let mut cluster = ServerCluster::new(vec![node_id], |_, cfg| {
        cfg.raft_store.local_file_gc_timeout = ReadableDuration::secs(1);
        cfg.raft_store.local_file_gc_tick_interval = ReadableDuration::millis(300);
    });
    let kv = cluster.get_kvengine(node_id);
    let mut client = cluster.new_client();
    let mut new_file_paths = vec![];
    let mut new_tmp_file_paths = vec![];
    let mut new_schema_file_paths = vec![];
    let mut new_vector_index_file_paths = vec![];
    for local_dir in &kv.opts.local_dirs {
        info!("local_dir {}", local_dir.to_string_lossy());
        let new_file_id = client.get_ts().into_inner();
        let new_file_path = new_filename(new_file_id, local_dir.as_path());
        fs::write(&new_file_path, "abc").unwrap();
        new_file_paths.push(new_file_path);
        let new_tmp_file_path = local_dir.join(new_tmp_filename(new_file_id, 1));
        fs::write(&new_tmp_file_path, "def").unwrap();
        new_tmp_file_paths.push(new_tmp_file_path);
        let new_file_id = client.get_ts().into_inner();
        let new_schema_file_path = local_dir.join(new_schema_filename(new_file_id));
        fs::write(&new_schema_file_path, "jkl").unwrap();
        new_schema_file_paths.push(new_schema_file_path);
        let new_file_id = client.get_ts().into_inner();
        let new_vector_index_file_path = local_dir.join(new_vector_index_filename(new_file_id));
        fs::write(&new_vector_index_file_path, "mno").unwrap();
        new_vector_index_file_paths.push(new_vector_index_file_path);
    }
    cluster.wait_pd_region_count(1);
    client.put_kv(0..1000, gen_key, gen_val);
    client.split(&gen_key(500));
    let shard_ids = kv.get_all_shard_id_vers();
    assert_eq!(shard_ids.len(), 2);
    let mut all_files = HashSet::default();
    for _ in 0..10 {
        for shard_id in &shard_ids {
            let snap = kv.get_snap_access(shard_id.id).unwrap();
            all_files.extend(snap.get_all_sst_files());
        }
        if !all_files.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    assert!(!all_files.is_empty());
    for &file_id in &all_files {
        let local_dir = get_local_dir(&kv.opts.local_dirs, file_id);
        let file_path = new_filename(file_id, local_dir.as_path());
        assert!(file_path.exists());
    }
    for (i, (new_file_path, new_tmp_file_path)) in new_file_paths
        .iter()
        .zip(new_tmp_file_paths.iter())
        .enumerate()
    {
        for _ in 0..20 {
            if !new_file_path.exists() && !new_tmp_file_path.exists() {
                break;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        assert!(!new_file_path.exists());
        assert!(!new_tmp_file_path.exists());
        assert!(!new_schema_file_paths[i].exists());
        assert!(!new_vector_index_file_paths[i].exists());
    }
    cluster.stop();
}

fn gen_key(i: usize) -> Vec<u8> {
    format!("xkey_{:08}", i).into_bytes()
}

fn gen_val(i: usize) -> Vec<u8> {
    format!("val_{:08}", i).into_bytes()
}

#[test]
fn test_raft_log_gc() {
    test_util::init_log_for_test();
    let mut node_ids = (0..3).map(|_| alloc_node_id()).collect::<Vec<_>>();
    let mut cluster = ServerCluster::new(node_ids.clone(), |_, cfg| {
        cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(100);
        cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(50);
        cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(100);
        cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);
        cfg.raft_store.hibernate_regions = false;
        cfg.raft_store.max_peer_down_duration = ReadableDuration::secs(7);
        cfg.rocksdb.writecf.write_buffer_size = ReadableSize::kb(16);
    });
    cluster.wait_region_replicated(&[], 3);
    let mut client = cluster.new_client();

    let region_id = client.get_region_id(&[]);
    let mut peer_ids = HashMap::new();
    for &node_id in &node_ids {
        let store_id = cluster.get_store_id(node_id);
        let peer_id = client.get_peer_id(&[], store_id);
        peer_ids.insert(node_id, peer_id);
    }
    let before_truncated_idxes = node_ids
        .iter()
        .map(|id| {
            let &peer_id = peer_ids.get(id).unwrap();
            cluster
                .get_rfengine(*id)
                .get_peer_stats(peer_id)
                .truncated_idx
        })
        .collect::<Vec<_>>();

    // Trigger switching and flushing memtable.
    for i in 0..50 {
        client.put_kv(i * 20..(i + 1) * 20, gen_key, gen_val);
    }
    let shard_stats = get_shard_stats(&cluster, region_id);
    assert!(
        shard_stats
            .iter()
            .all(|stat| stat.mem_table_count + stat.l0_table_count > 1 && stat.mem_table_size > 0)
    );

    // Wait for raftlog truncation.
    let wait_truncated = |before_truncated_idxes: Vec<_>| -> Vec<_> {
        let mut curr_truncated_idxes = vec![];
        for i in 0..10 {
            curr_truncated_idxes = node_ids
                .iter()
                .map(|id| {
                    let &peer_id = peer_ids.get(id).unwrap();
                    cluster
                        .get_rfengine(*id)
                        .get_peer_stats(peer_id)
                        .truncated_idx
                })
                .collect::<Vec<_>>();
            if curr_truncated_idxes
                .iter()
                .zip(before_truncated_idxes.iter())
                .all(|(curr, before)| curr > before)
            {
                break;
            }
            if i == 9 {
                panic!("waiting for raftlog truncation timeouts");
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        curr_truncated_idxes
    };
    let curr_truncated_idxes = wait_truncated(before_truncated_idxes);

    let wait_for_memtable_flush = |cluster: &ServerCluster, node_ids: &[u16], timeout: Duration| {
        let mut curr_shard_stats;
        let mut flushed;
        let start = Instant::now_coarse();
        loop {
            curr_shard_stats = node_ids
                .iter()
                .map(|id| cluster.get_kvengine(*id).get_shard_stat(region_id))
                .collect::<Vec<_>>();
            flushed = curr_shard_stats
                .iter()
                .all(|curr| curr.mem_table_size == 0 && curr.mem_table_count == 1);
            if flushed {
                break;
            }
            if start.saturating_elapsed() >= timeout {
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        (flushed, curr_shard_stats)
    };

    // Trigger switching and flushing memtable.
    let flush_memtable = |cluster: &ServerCluster, node_ids: &[u16]| {
        let mut client = cluster.new_client();
        let region_id = client.get_region_id(&[]);
        let ctx = client.new_rpc_ctx(region_id, b"").unwrap();
        ctx.get_peer().get_store_id();
        let mut req = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(ctx.get_region_id());
        header.set_peer(ctx.get_peer().clone());
        header.set_region_epoch(ctx.get_region_epoch().clone());
        header.set_term(6);
        req.set_header(header);
        let mut custom_builder = CustomBuilder::new();
        custom_builder.set_switch_mem_table(1);
        req.set_custom_request(custom_builder.build());
        cluster.send_raft_command(req);

        let (flushed, curr_shard_stats) =
            wait_for_memtable_flush(cluster, node_ids, Duration::from_secs(6));
        assert!(flushed, "shard_stats: {:?}", curr_shard_stats);
        curr_shard_stats
    };

    let (flushed, mut curr_shard_stats) =
        wait_for_memtable_flush(&cluster, &node_ids, Duration::from_secs(1));
    if !flushed {
        // Invoke `flush_memtable` only when memtable has not flushed.
        // Otherwise, `switch_mem_table` on empty memtable will advance
        // `Shard.sequence` only, but `ShardMeta.data_sequence` is unchanged.
        // In this situation, raft log truncate index will be less than `Shard.sequence`
        // (and `shard_stat.write_sequence`).
        curr_shard_stats = flush_memtable(&cluster, &node_ids);
    }
    let curr_truncated_idxes = wait_truncated(curr_truncated_idxes);
    std::thread::sleep(Duration::from_secs(1));
    // Flushing memtable will propose a ChangeSet request which won't write data to
    // memtable, so it can't trigger memtable flush. We should be able to
    // truncate these logs.
    assert!(
        curr_truncated_idxes
            .iter()
            .zip(curr_shard_stats.iter())
            .any(|(truncated_idx, shard_stat)| *truncated_idx >= shard_stat.write_sequence),
        "truncated_stat: {:?} shard_stats: {:?}",
        curr_truncated_idxes,
        curr_shard_stats
    );

    // Stop one node, and leader shouldn't truncate logs immediately.
    cluster.stop_node(node_ids[2]);
    node_ids.pop();
    let mut client = cluster.new_client();
    for i in 1000..1010 {
        client.put_kv(i..i + 1, gen_key, gen_val);
    }
    flush_memtable(&cluster, &node_ids);
    std::thread::sleep(Duration::from_millis(200));
    // Leader's truncated index doesn't change immediately.
    let region = client.get_region_by_key(&[]);
    let leader_peer_id = region.peers[region.leader_idx].id;
    let &leader_peer_node = peer_ids
        .iter()
        .find(|(_, &peer_id)| peer_id == leader_peer_id)
        .unwrap()
        .0;
    let leader_truncate_idx = cluster
        .get_rfengine(leader_peer_node)
        .get_peer_stats(leader_peer_id)
        .truncated_idx;
    assert_eq!(leader_truncate_idx, curr_truncated_idxes[0]);
    // Wait for marking down peer.
    must_wait(
        || {
            let leader_truncate_idx = cluster
                .get_rfengine(leader_peer_node)
                .get_peer_stats(leader_peer_id)
                .truncated_idx;
            leader_truncate_idx > curr_truncated_idxes[0]
        },
        10,
        || {
            let leader_truncate_idx = cluster
                .get_rfengine(leader_peer_node)
                .get_peer_stats(leader_peer_id)
                .truncated_idx;
            format!("{:?} {:?}", leader_truncate_idx, curr_truncated_idxes[0])
        },
    );
    cluster.stop();
}

#[test]
fn test_raft_log_gc_no_kv_logs() {
    test_util::init_log_for_test();
    let node_ids = (0..3).map(|_| alloc_node_id()).collect::<Vec<_>>();
    let mut cluster = ServerCluster::new(node_ids.clone(), |_, conf| {
        conf.raft_store.raft_log_gc_no_kv_count = 1;
    });
    cluster.wait_region_replicated(&[], 3);
    let mut client = cluster.new_client();
    client.put_kv(0..100, gen_key, gen_val);
    let region = client.get_region_by_key(&[]);
    let pd_client = cluster.get_pd_client();
    pd_client.disable_default_operator();
    let remove_peer = region.peers[2].clone();
    pd_client.remove_peer(region.id, remove_peer.clone());
    must_wait(
        || pd_client.get_region(&[]).unwrap().peers.len() == 2,
        10,
        || "add peer failed".to_string(),
    );
    let mut new_peer = remove_peer.clone();
    new_peer.id = pd_client.alloc_id().unwrap();
    pd_client.add_peer(region.id, new_peer.clone());
    must_wait(
        || pd_client.get_region(&[]).unwrap().peers.len() == 3,
        10,
        || "remove peer failed".to_string(),
    );
    let scheduler = cluster.new_scheduler();
    let mut counter = 0;
    let transfer_count = rand::thread_rng().gen_range(0usize..5);
    while counter < transfer_count {
        if scheduler.transfer_random_leader() {
            counter += 1;
        }
    }
    let no_kv_count = cluster
        .get_node_config(node_ids[0])
        .raft_store
        .raft_log_gc_no_kv_count;
    must_wait(
        || {
            let rfengine = cluster.get_rfengine(node_ids[0]);
            let region_peers = rfengine.get_region_peer_map();
            let peer_id = region_peers.get(&region.id).cloned().unwrap();
            let truncated_idx = rfengine.get_truncated_index(peer_id).unwrap();
            let last_idx = rfengine.get_last_index(peer_id).unwrap_or(truncated_idx);
            info!(
                "truncated_idx: {:?} last_idx: {:?}",
                truncated_idx, last_idx
            );
            truncated_idx + no_kv_count >= last_idx
        },
        10,
        || "truncate failed".to_string(),
    );
    cluster.stop();
}

#[test]
fn test_raft_log_gc_size_limit() {
    test_util::init_log_for_test();
    let node_ids = (0..3).map(|_| alloc_node_id()).collect::<Vec<_>>();
    let mut cluster = ServerCluster::new(node_ids.clone(), |_, cfg| {
        cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(100);
        cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(50);
        cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(100);
        cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);
        cfg.rocksdb.writecf.write_buffer_size = ReadableSize::kb(16);
        cfg.raft_store.raft_log_gc_size_limit = Some(ReadableSize::kb(1));
    });
    cluster.wait_region_replicated(&[], 3);
    cluster.stop_node(node_ids[2]);

    let mut client = cluster.new_client();
    let store_id = cluster.get_store_id(node_ids[0]);
    let peer_id = client.get_peer_id(&[], store_id);
    std::thread::sleep(Duration::from_millis(300));
    let prev_truncated_idx = cluster
        .get_rfengine(node_ids[0])
        .get_peer_stats(peer_id)
        .truncated_idx;
    for i in 0..50 {
        client.put_kv(i * 20..(i + 1) * 20, gen_key, gen_val);
    }
    must_wait(
        || {
            let curr_truncated_idx = cluster
                .get_rfengine(node_ids[0])
                .get_peer_stats(peer_id)
                .truncated_idx;
            curr_truncated_idx > prev_truncated_idx && curr_truncated_idx > 40
        },
        3,
        || "raft log size limit doesn't take effect".to_string(),
    );
    cluster.stop();
}

fn get_shard_stats(cluster: &ServerCluster, region_id: u64) -> Vec<ShardStats> {
    let nodes = cluster.get_nodes();
    nodes
        .iter()
        .map(|id| cluster.get_kvengine(*id).get_shard_stat(region_id))
        .collect::<Vec<_>>()
}
