// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fs, path::Path, sync::Arc, time::Duration};

use kvengine::dfs::{Dfs, S3Fs};
use kvproto::{metapb::Region, raft_serverpb::RegionLocalState};
use protobuf::Message;
use raft_proto::eraftpb::{Entry, EntryType};
use rand::RngCore;
use rfengine::{region_state_key, WriteBatch, MAX_EPOCH_BACKWARD};
use test_cloud_server::{must_wait, oss::prepare_dfs};
use test_util::init_log_for_test;
use tikv_util::config::{ReadableDuration, ReadableSize};

#[test]
fn test_rfengine_dfs_worker() {
    init_log_for_test();
    let (temp_dir, mut oss, mut dfs_conf) = prepare_dfs("rfengine_dfs_worker");
    // By modifying the dispatch_timeout and write_timeout_per_mb, we can make
    // dfs_worker fail.
    dfs_conf.conn_options.dispatch_timeout = ReadableDuration::millis(100);
    dfs_conf.conn_options.write_timeout_per_mb = ReadableDuration::millis(100);

    dfs_conf.conn_options.max_retry_count = 1;

    let mut rf_cfg = rfengine::RfEngineConfig::default();
    rf_cfg.wal_chunk_target_file_size = ReadableSize::kb(128);
    rf_cfg.target_file_size = ReadableSize::kb(512);
    rf_cfg.lightweight_backup = true;
    rf_cfg.wal_sync_dir = format!("{}/wal", temp_dir.path().to_str().unwrap());
    fs::create_dir_all(&rf_cfg.wal_sync_dir).unwrap();
    let dir = temp_dir.path().join("raftdb");
    let s3fs = Arc::new(S3Fs::new_from_config(dfs_conf));
    let mut raft = open_engine(dir.as_path(), &rf_cfg, s3fs.clone());
    init_raft_data(&raft);

    // The dfs worker starts healthy after init.
    wait_health(&raft, true, "wait init");

    // Make DFS fail all so the worker becomes unhealthy.
    oss.set_fail_all(true);
    let mut entry_index = 1;
    let entry_size = 64 * 1024;
    write_wal_to_epoch(&raft, &mut entry_index, entry_size, 6);
    wait_health(&raft, false, "wait unhealthy");
    raft.stop_worker(false);

    // Restart the engine while the worker is unhealthy; it should remain unhealthy.
    raft = open_engine(dir.as_path(), &rf_cfg, s3fs.clone());
    std::thread::sleep(Duration::from_secs(1));
    assert!(!raft.is_dfs_worker_healthy());

    // Restore OSS so WAL uploads can succeed again, but the worker should
    // recover healthy after a snapshot is persisted.
    oss.set_fail_all(false);
    oss.set_put_delay("snapshots", Duration::from_secs(1));
    write_wal_to_epoch(&raft, &mut entry_index, entry_size, 9);
    assert!(
        !raft.is_dfs_worker_healthy(),
        "dfs worker should remain unhealthy until snapshot upload finishes"
    );
    oss.set_put_delay("snapshots", Duration::from_millis(0));
    write_wal_to_epoch(&raft, &mut entry_index, entry_size, 10);
    wait_health(&raft, true, "wait recover after snapshot");
    raft.stop_worker(false);

    // Restart the engine while healthy; it should remain healthy after restart.
    raft = open_engine(dir.as_path(), &rf_cfg, s3fs);
    wait_health(&raft, true, "stay healthy after restart");
    oss.set_fail_all(true);
    write_wal_to_epoch(&raft, &mut entry_index, entry_size, 11);
    wait_health(&raft, false, "became unhealthy after oss fail");
    oss.set_fail_all(false);
    write_wal_to_epoch(&raft, &mut entry_index, entry_size, 17);
    wait_health(&raft, true, "recover healthy after snapshot");

    write_wal_to_epoch(&raft, &mut entry_index, entry_size, 56);
    std::thread::sleep(Duration::from_secs(1));
    assert!(raft.is_dfs_worker_healthy()); // snap 48 should be succeed.
    // tolerate snapshot fail, still healthy
    oss.set_put_delay("snapshots", Duration::from_secs(1));

    // snap 144 fails, but still not lag too much.
    write_wal_to_epoch(
        &raft,
        &mut entry_index,
        entry_size,
        48 + MAX_EPOCH_BACKWARD - 1,
    );
    std::thread::sleep(Duration::from_secs(1));
    assert!(raft.is_dfs_worker_healthy());

    // if snapshot lag too much, it became unhealthy.
    write_wal_to_epoch(
        &raft,
        &mut entry_index,
        entry_size,
        48 + MAX_EPOCH_BACKWARD + 1,
    );
    // snap 152 fail, lag too much
    wait_health(&raft, false, "wait snap lag too much");

    oss.set_put_delay("snapshots", Duration::from_secs(0));
    write_wal_to_epoch(
        &raft,
        &mut entry_index,
        entry_size,
        48 + MAX_EPOCH_BACKWARD + 10,
    );
    wait_health(&raft, true, "recover by snapshot");

    raft.stop_worker(false);
    oss.shutdown();
}

fn init_raft_data(raft: &rfengine::RfEngine) {
    let mut wb = WriteBatch::new();
    let key = region_state_key(1);
    let mut region_local_state = RegionLocalState::new();
    let mut region = Region::new();
    region.set_id(1);
    region_local_state.set_region(region);
    let val = region_local_state.write_to_bytes().unwrap();
    wb.set_state_bytes(1, 1, 0, key, val.into());
    raft.write(wb).unwrap();
}

fn write_wal_to_epoch(
    engine: &rfengine::RfEngine,
    entry_index: &mut u64,
    payload: usize,
    target_epoch: u32,
) {
    let mut current_epoch = engine.get_epoch_offset().0;
    while current_epoch < target_epoch {
        append_logs(engine, entry_index, payload, 1);
        let new_epoch = engine.get_epoch_offset().0;
        if new_epoch != current_epoch {
            // avoid dfs worker fall behind and WAL be overwritten.
            std::thread::sleep(Duration::from_millis(50));
            current_epoch = new_epoch;
        }
    }
}

fn open_engine(
    dir: &Path,
    cfg: &rfengine::RfEngineConfig,
    dfs: Arc<dyn Dfs>,
) -> rfengine::RfEngine {
    let raft = rfengine::RfEngine::open(dir, cfg, None, Some(dfs)).unwrap();
    raft.set_engine_id(100);
    raft
}

fn wait_health(raft: &rfengine::RfEngine, healthy: bool, label: &str) {
    must_wait(
        || raft.is_dfs_worker_healthy() == healthy,
        15,
        || label.to_string(),
    );
}

// Helper to append some raft logs to advance WAL progress.
fn append_logs(engine: &rfengine::RfEngine, entry_index: &mut u64, payload: usize, count: usize) {
    let mut rng = rand::thread_rng();
    for _ in 0..count {
        let mut entry = Entry::new();
        entry.set_index(*entry_index);
        entry.set_term(1);
        entry.set_entry_type(EntryType::EntryNormal);
        let mut data_buf = vec![0u8; payload];
        rng.fill_bytes(&mut data_buf);
        entry.set_data(data_buf.into());
        let mut wb = WriteBatch::new();
        wb.append_raft_log(1, 1, 0, &entry);
        engine.write(wb).unwrap();
        *entry_index += 1;
    }
}
