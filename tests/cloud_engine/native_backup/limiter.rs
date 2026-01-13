// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use api_version::ApiV2;
use kvengine::{ShardStats, WRITE_CF, dfs::S3Fs};
use native_br::{
    backup,
    limiter::{RateLimitConfig, ThroughputLimiter},
    metrics::NATIVE_BR_RESTORED_DATA_SIZE,
    restore::RestoreConfig,
    restore_keyspace,
};
use rand::Rng;
use test_cloud_server::{ServerCluster, client::RequestOptions, oss::prepare_dfs};
use tikv::config::TikvConfig;
use tikv_util::config::{ReadableDuration, ReadableSize};
use tokio::runtime::Runtime;

use super::DummyStepReporter;

fn gen_keyspace_key(keyspace_id: u32) -> impl Fn(usize) -> Vec<u8> {
    move |i: usize| -> Vec<u8> {
        let mut key = ApiV2::get_keyspace_prefix_by_id(keyspace_id);
        key.extend(format!("tkey_{:08}", i).into_bytes());
        key
    }
}

#[test]
fn test_restore_keyspace_throughput_limit() {
    test_util::init_log_for_test();

    const NODES_COUNT: usize = 4;
    const KEYSPACE_ID: u32 = 1;

    const REPLICAS_NUM: usize = 3;
    const VALUE_SIZE: usize = 1024; // 1 KiB
    // The `native_br_restored_data_size` metric is based on estimated snapshot
    // file sizes (not raw user value bytes). But as values are random, the
    // compression ratio is low, so the estimated size should be close to raw size.
    const BACKUP_KEYS: usize = 1024;
    const TARGET_RESTORED_BYTES: u64 = (BACKUP_KEYS * VALUE_SIZE * REPLICAS_NUM) as u64;
    const EXTRA_KEYS: usize = 5 * 1024; // ~5 MiB

    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test_restore_keyspace_throughput_limit_");
    let s3fs = Arc::new(S3Fs::new_from_config(dfs_config.clone()));
    let runtime = Runtime::new().unwrap();

    let mut cluster = ServerCluster::new(
        crate::alloc_node_id_vec(NODES_COUNT),
        |_, conf: &mut TikvConfig| {
            conf.dfs = dfs_config.clone();
            // Set small mem-table size to make data reach L1 and reduce estimation error.
            conf.rocksdb.writecf.write_buffer_size = ReadableSize::kb(1);
            conf.rocksdb.writecf.target_file_size_base = ReadableSize::kb(32);
            conf.coprocessor.region_split_size = ReadableSize::kb(128);
            conf.coprocessor.region_bucket_size = ReadableSize::kb(64);
            conf.rfengine.target_file_size = ReadableSize::mb(1);
            conf.rfengine.lightweight_backup = true;
            conf.rfengine.wal_chunk_target_file_size = ReadableSize::kb(128);
            conf.memory.enable_heap_profiling = false;
        },
    );
    cluster.wait_region_replicated(&[], 3);

    let pd_client = cluster.get_pd_client();
    let mut client = cluster.new_client();
    client.split_keyspace(KEYSPACE_ID);

    let gen_key = gen_keyspace_key(KEYSPACE_ID);

    client.put_kv(0..BACKUP_KEYS, &gen_key, |_| {
        crate::random_value(VALUE_SIZE)
    });
    client.verify_data_with_ref_store();
    let ref_store_at_backup = client.dump_ref_store();

    // Wait compaction to make stable and reduce estimation error.
    crate::wait_for_keyspace_stats(
        &runtime,
        &cluster,
        pd_client.as_ref(),
        KEYSPACE_ID,
        expect_compacted,
        true,
        Duration::from_secs(60),
    )
    .unwrap();

    let backup_config = backup::BackupConfig {
        dfs: dfs_config.clone(),
        backup_delay: ReadableDuration::secs(1),
        skip_keyspace_meta: true,
        ..Default::default()
    };
    let backup_name = format!("backup_{}", rand::thread_rng().gen::<u16>());
    let backup_ts = client.get_ts().into_inner();
    backup::backup_cluster_with_ts(
        backup_config,
        backup_name.clone(),
        cluster.get_pd_client().as_ref(),
        backup_ts,
        None,
    )
    .expect("backup::backup_cluster_with_ts");

    // Write extra 5MiB x 2 data (10MiB in a batch will exceed raft entry limit).
    client.put_kv(0..EXTRA_KEYS, &gen_key, |_| crate::random_value(VALUE_SIZE));
    client.put_kv(EXTRA_KEYS..EXTRA_KEYS * 2, &gen_key, |_| {
        crate::random_value(VALUE_SIZE)
    });
    client.verify_data_with_ref_store();

    let reporter = Arc::new(DummyStepReporter::default());
    let restore_config = RestoreConfig {
        dfs: dfs_config,
        ..RestoreConfig::default_for_test()
    };

    let restore_once_with_threshold = |calibrate_threshold: u64| -> u64 {
        let before = NATIVE_BR_RESTORED_DATA_SIZE.get();
        let limiter = {
            let config = RateLimitConfig {
                enable: true,
                max_throughput: ReadableSize::mb(1024),
                calibrate_restore_size_threshold: ReadableSize(calibrate_threshold),
                store_req_timeout: ReadableDuration::secs(30),
                store_cache_ttl: ReadableDuration::ZERO,
            };
            Arc::new(
                ThroughputLimiter::new(
                    &config,
                    cluster.get_pure_pd_client(),
                    runtime.handle().clone(),
                )
                .expect("create ThroughputLimiter"),
            )
        };
        restore_keyspace::restore_keyspace(
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
            None,
            Some(limiter),
            None,
        )
        .unwrap();
        NATIVE_BR_RESTORED_DATA_SIZE.get() - before
    };

    // Condition 1: disable requesting store files (no calibration), so metric is
    // based on local estimation and should be about ~1MiB x 3.
    let delta_not_calibrated = restore_once_with_threshold(u64::MAX);
    assert!(
        (TARGET_RESTORED_BYTES / 2..=TARGET_RESTORED_BYTES * 2).contains(&delta_not_calibrated),
        "restored metric delta out of range, delta={}, expected~{}",
        delta_not_calibrated,
        TARGET_RESTORED_BYTES
    );

    // Condition 2: enable requesting store files for calibration. After one
    // restore, the needed SST files should already exist on TiKV stores, so the
    // calibrated restored size is 0.
    let delta_calibrated = restore_once_with_threshold(ReadableSize::kb(1).0);
    assert_eq!(
        delta_calibrated, 0,
        "restored metric should be 0 when calibration is enabled"
    );

    client
        .verify_data_with_given_ref_store(&ref_store_at_backup, None, &RequestOptions::default())
        .unwrap();

    cluster.stop();
    oss.shutdown();
}

fn expect_compacted(stats: &ShardStats) -> bool {
    if stats.total_size == 0 {
        // Skip empty shards.
        return true;
    }
    stats.compaction_score < 1.0 && stats.cfs[WRITE_CF].levels.iter().any(|l| l.num_tables > 0)
}
