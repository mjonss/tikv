// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use api_version::ApiV2;
use bytes::Bytes;
use cloud_encryption::KeyspaceEncryptionConfig;
use cloud_worker::broadcast_schema_update_to_all_stores;
use collections::HashSet;
use kvengine::{
    WRITE_CF,
    dfs::{DFSConfig, Dfs, FileType, Options, new_dfs_from_config},
    table::{
        columnar::{new_int_handle_column_info, new_version_column_info},
        schema_file::{SchemaBuf, build_schema_file},
    },
};
use kvproto::metapb;
use native_br::{
    archive, backup,
    common::now,
    metrics::{
        NATIVE_BR_RFENGINE_CACHE_HIT, NATIVE_BR_RFENGINE_CACHE_MISS,
        NATIVE_BR_RFENGINE_WAL_EPOCH_OVERWRITTEN_ERROR,
    },
    restore::RestoreConfig,
    restore_keyspace,
    rfengine_cache::RfEngineCache,
    step,
};
use pd_client::PdClient;
use rand::prelude::*;
use rstest::rstest;
use schema::schema::StorageClassSpec;
use security::{SecurityConfig, SecurityManager};
use test_cloud_server::{
    ServerCluster, ServerClusterBuilder, TikvWorkerOptions,
    client::{ClusterClientOptions, CommitAction, MutateOptions, RequestOptions, TxnWriteMethod},
    must_wait,
    oss::prepare_dfs,
    try_wait,
};
use test_pd_client::PdWrapper;
use tikv::config::TikvConfig;
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    debug, info,
    store::new_learner_peer,
    time, warn,
};
use tokio::runtime::Runtime;
use txn_types::TimeStamp;

use crate::{
    alloc_node_id_vec, native_backup::DummyStepReporter, new_security_config,
    request_major_compaction, wait_for_keyspace_stats,
};

const BASIC_DATA_COUNT: usize = 10;
const RANDOM_VALUE_LEN: usize = 64;
const NODES_COUNT: usize = 5;
const KEYSPACE_COUNT: usize = 3;
const DEFAULT_LOOP_COUNT: usize = 3;
const DEFAULT_TARGET_REGIONS: usize = 4;
const BACKUP_DAYS: usize = 4;

// The numbers have no special meaning, just to make them different.
// Require to be larger than 72, see `i_to_val`.
const BASIC_DATA_LEN: usize = 80;
const IMPORT_DATA_LEN: usize = 82;
const PRE_BACKUP_DATA_LEN: usize = 84;
const POST_BACKUP_DATA_LEN: usize = 86;
const PRE_PITR_DATA_LEN: usize = 88;
const FINAL_DATA_LEN: usize = 90;

#[test]
fn test_restore_keyspace() {
    test_util::init_log_for_test();
    let loop_count = std::env::var("LOOP")
        .unwrap_or_default()
        .parse::<usize>()
        .unwrap_or(DEFAULT_LOOP_COUNT);
    let target_regions = std::env::var("TARGET_REGIONS")
        .unwrap_or_default()
        .parse::<usize>()
        .unwrap_or(DEFAULT_TARGET_REGIONS);

    test_restore_keyspace_opt(TestRestoreKeyspaceOptions {
        loop_count,
        target_regions,
        ..Default::default()
    });
}

#[rstest]
#[case::disable_encrytion(false)]
#[case::encrytion(true)]
fn test_restore_keyspace_with_archive(#[case] enable_encryption: bool) {
    test_util::init_log_for_test();
    test_restore_archived_keyspace_opt(TestRestoreKeyspaceOptions {
        enable_encryption,
        ..Default::default()
    });
}

struct TestRestoreKeyspaceOptions {
    loop_count: usize,
    target_regions: usize,
    enable_encryption: bool,
}

impl Default for TestRestoreKeyspaceOptions {
    fn default() -> Self {
        TestRestoreKeyspaceOptions {
            loop_count: DEFAULT_LOOP_COUNT,
            target_regions: DEFAULT_TARGET_REGIONS,
            enable_encryption: false,
        }
    }
}

fn test_restore_keyspace_opt(options: TestRestoreKeyspaceOptions) {
    // keyspace_id, data_count, shuffle_regions, has_learner, loop_count
    let cases = [
        (0, 100, None, false, 1),
        (1, 1, None, false, 1),
        (
            1,
            100,
            Some(options.target_regions),
            false,
            options.loop_count,
        ),
        (1, 100, None, true, 1), // Don't shuffle regions for stability.
        (2, 1, None, false, 1),
    ];

    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test_restore_keyspace_");
    let mut cluster = ServerCluster::new(
        alloc_node_id_vec(NODES_COUNT),
        |_, conf: &mut TikvConfig| {
            conf.dfs = dfs_config.clone();
            // Set small mem-table size to make data reach L1 and generate over bound
            // shards.
            conf.rocksdb.writecf.write_buffer_size = ReadableSize::kb(1);
            conf.coprocessor.region_split_size = ReadableSize::kb(128); // kv_opts.base_size = 8kb
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

    // Split keyspaces.
    client.split_keyspaces(0..KEYSPACE_COUNT as u32);
    cluster.wait_pd_region_count(KEYSPACE_COUNT + 2);

    // Import basic data.
    for keyspace_id in 0..KEYSPACE_COUNT as u32 {
        client.put_kv(
            0..BASIC_DATA_COUNT,
            gen_keyspace_key(keyspace_id),
            i_to_val(BASIC_DATA_LEN),
        );
    }

    let runtime = Runtime::new().unwrap();

    // Run cases.
    for (case_idx, &(keyspace_id, data_count, shuffle_regions, has_learner, loop_count)) in
        cases.iter().enumerate()
    {
        step!("case group: {case_idx}");

        if has_learner {
            pd_client.disable_default_operator(); // To prevent PD removing learners due to exceed `max-replicas`.
            let (start, end) = ApiV2::get_keyspace_range_by_id(keyspace_id);
            add_learners(&mut cluster, &start, &end);
            check_learners(&cluster, &start, &end, NODES_COUNT - 3, 10);
            step!("add learner done");
        } else {
            pd_client.enable_default_operator(); // Remove learners by peer count check.
        }

        for loop_idx in 0..loop_count {
            test_restore_keyspace_impl(
                case_idx,
                loop_idx,
                &mut cluster,
                &dfs_config,
                keyspace_id,
                data_count,
                shuffle_regions,
                has_learner,
                &runtime,
            );
        }
    }

    cluster.stop();
    oss.shutdown();
}

fn test_restore_keyspace_impl(
    case_idx: usize,
    loop_idx: usize,
    cluster: &mut ServerCluster,
    dfs_config: &DFSConfig,
    keyspace_id: u32,
    data_count: usize,
    shuffle_regions: Option<usize>,
    has_learner: bool,
    runtime: &Runtime,
) {
    step!("case: {}:{}", case_idx, loop_idx);
    let mut rng = rand::thread_rng();
    let mut client = cluster.new_client();
    let i_to_key = gen_keyspace_key(keyspace_id);
    let (keyspace_start, keyspace_end) = ApiV2::get_keyspace_range_by_id(keyspace_id);

    let backup_config = backup::BackupConfig {
        dfs: dfs_config.clone(),
        backup_delay: ReadableDuration::secs(1),
        skip_keyspace_meta: true,
        ..Default::default()
    };
    let restore_config = RestoreConfig::default_for_test();
    let s3fs = new_dfs_from_config(dfs_config.clone());
    let reporter = Arc::new(DummyStepReporter::default());

    // Import data.
    let commit_action = if rng.gen_ratio(2, 3) {
        CommitAction::AsyncCommitSecondaryKeys(Duration::ZERO)
    } else {
        CommitAction::AsyncCommitSecondaryKeys(Duration::MAX)
    };
    client
        .try_put_kv(
            0..data_count,
            &i_to_key,
            i_to_val(IMPORT_DATA_LEN),
            MutateOptions {
                commit_action,
                ..Default::default()
            },
        )
        .unwrap();
    let origin_ref_store = client.dump_ref_store();

    // Prepare for PiTR.
    let truncate_ts = if rng.gen_ratio(1, 2) {
        let ts = client.get_ts();
        client.put_kv(
            0..(data_count / 2).max(1),
            &i_to_key,
            i_to_val(PRE_PITR_DATA_LEN),
        );
        step!("another writes for pitr done");
        client.verify_data_with_ref_store();
        Some(ts.into_inner())
    } else {
        None
    };

    // Perform snapshot backup.
    let snapshot_backup_name = generate_backup_name();
    let backup_meta = {
        let backup_ts = client.get_ts().into_inner();
        // Put more data before backup to verify truncate ts take affect.
        client.put_kv(
            0..(data_count / 2).max(1),
            &i_to_key,
            i_to_val(PRE_BACKUP_DATA_LEN),
        );
        step!("another writes done");
        client.verify_data_with_ref_store();
        assert!(
            client
                .verify_data_with_given_ref_store(
                    &origin_ref_store,
                    None,
                    &RequestOptions::default()
                )
                .is_err(),
            "case: {}:{}",
            case_idx,
            loop_idx
        );
        step!("verify before backup ok");
        let (_, backup_meta) = backup::backup_cluster_with_ts(
            backup_config.clone(),
            snapshot_backup_name.clone(),
            cluster.get_pd_client().as_ref(),
            backup_ts,
            None,
        )
        .expect("backup::backup_cluster");
        info!("backup_cluster result: {:?}", backup_meta);
        client.verify_data_with_ref_store();
        step!("backup done");

        backup_meta
    };

    // Shuffle regions.
    if let (Some(shuffle_regions), 0) = (shuffle_regions, loop_idx) {
        let keyspace_prefix = ApiV2::get_keyspace_prefix_by_id(keyspace_id);
        let region_count = client.pd_client().get_regions_number();
        let mut i = 0;
        while i < shuffle_regions {
            let split_key = rng.gen::<usize>() % data_count;
            if let Err(e) = client.try_split(&i_to_key(split_key), 1) {
                warn!("try split error: {:?}", e);
                continue;
            }
            i += 1;
        }

        cluster.wait_pd_region_min_count(region_count + shuffle_regions);
        let split_region_count = client.pd_client().get_regions_number();

        for _ in 0..(shuffle_regions / 2) {
            let source_key = rng.gen::<usize>() % data_count;
            if let Err(e) = client.try_merge_adjacent_region(
                &i_to_key(source_key),
                Some(&keyspace_prefix),
                Duration::from_secs(3),
            ) {
                warn!("try_merge_adjacent_region fail: {:?}", e);
            }
        }

        let merge_region_count = client.pd_client().get_regions_number();
        step!(
            "shuffle regions done, regions {} -> {} -> {}",
            region_count,
            split_region_count,
            merge_region_count
        );
    }

    // Put another data.
    client.put_kv(0..data_count, &i_to_key, i_to_val(POST_BACKUP_DATA_LEN));
    step!("another writes done");
    client.verify_data_with_ref_store();
    assert!(
        client
            .verify_data_with_given_ref_store(&origin_ref_store, None, &RequestOptions::default())
            .is_err(),
        "case: {}:{}",
        case_idx,
        loop_idx,
    );
    step!("verify ok");

    // Test for "PiTR on restored data". For the following scenario:
    // T0: put data0.
    // T1: perform backup.
    // T2: put data1.
    // T3: restore to backup. Now data0 should be in store.
    // T4: PiTR to T2. Now data1 should be in store.
    let truncate_ts_pitr = client.get_ts().into_inner();
    let ref_store_pitr = client.dump_ref_store();

    // Instant backup before restore
    let instant_backup_name = {
        let backup_ts = client.get_ts().into_inner();
        let instant_backup_name = generate_backup_name();
        let (_, backup_meta) = backup::backup_cluster_with_ts(
            backup_config,
            instant_backup_name.clone(),
            cluster.get_pd_client().as_ref(),
            backup_ts,
            Some(backup_meta),
        )
        .expect("backup::backup_cluster");
        info!("backup_cluster result (instant): {:?}", backup_meta);
        step!("backup for pitr done");
        instant_backup_name
    };

    let object_cache = cluster.create_object_cache_randomly();
    let limiter = cluster.create_restore_limiter_randomly(runtime.handle().clone());
    // Restore keyspace.
    restore_keyspace::restore_keyspace(
        keyspace_id,
        keyspace_id,
        &snapshot_backup_name,
        None,
        s3fs.clone(),
        restore_config.clone(),
        cluster.get_pd_client(),
        runtime,
        truncate_ts,
        reporter.clone(),
        object_cache.clone(),
        limiter.clone(),
        None,
    )
    .unwrap();
    step!("restore done");

    // Verify restored data.
    {
        let mut verify_options: Vec<(Option<(&[u8], &[u8])>, RequestOptions)> =
            vec![(None, RequestOptions::default())];
        if has_learner {
            verify_options.push((
                Some((&keyspace_start, &keyspace_end)),
                RequestOptions::learner(),
            ));
        }
        let mut verify_res = vec![];
        for (range, opt) in verify_options {
            verify_res.push(
                client
                    .verify_data_with_given_ref_store(&origin_ref_store, range, &opt)
                    .unwrap(),
            );
        }
        step!(
            "verify restore data done{}, kv count: {:?}",
            if has_learner { " (with learner)" } else { "" },
            verify_res,
        );
    }

    // PiTR on restored data.
    {
        restore_keyspace::restore_keyspace(
            keyspace_id,
            keyspace_id,
            &instant_backup_name,
            None,
            s3fs,
            restore_config,
            cluster.get_pd_client(),
            runtime,
            Some(truncate_ts_pitr),
            reporter,
            object_cache,
            limiter,
            None,
        )
        .unwrap();
        step!("restore (pitr on restored data) done");

        let verify_res = client
            .verify_data_with_given_ref_store(
                &ref_store_pitr,
                Some((&keyspace_start, &keyspace_end)),
                &RequestOptions::default(),
            )
            .unwrap();
        step!(
            "verify (pitr on restored data) done, kv count: {}",
            verify_res.0
        );
    }

    // Write more data to verify the sequences.
    for id in 0..KEYSPACE_COUNT as u32 {
        let count = if id == keyspace_id {
            data_count
        } else {
            BASIC_DATA_COUNT
        };
        client.put_kv(0..count, gen_keyspace_key(id), i_to_val(FINAL_DATA_LEN));
    }
    client.verify_data_with_ref_store();
    step!("verify more writes done");
}

fn test_restore_archived_keyspace_opt(options: TestRestoreKeyspaceOptions) {
    let cases = [
        // keyspace_id, data_count, loop_count
        (1, 1, 1),
        (1, 100, options.loop_count),
        (2, 1, 1),
    ];
    let max_data_count = cases.iter().map(|x| x.1).max().unwrap();

    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test_restore_keyspace_");
    let security_config = new_security_config();
    let pd_wrapper = PdWrapper::new_test(1, &security_config, None);
    let mut cluster = ServerClusterBuilder::new(
        alloc_node_id_vec(NODES_COUNT),
        |_, conf: &mut TikvConfig| {
            conf.dfs = dfs_config.clone();
            // Set small mem-table size to make data reach L1 and generate over bound
            // shards.
            conf.rocksdb.writecf.write_buffer_size = ReadableSize::kb(1);
            conf.coprocessor.region_split_size = ReadableSize::kb(128); // kv_opts.base_size = 8kb
            conf.coprocessor.region_bucket_size = ReadableSize::kb(64);
            conf.rfengine.target_file_size = ReadableSize::kb(512);
            conf.rfengine.lightweight_backup = true;
            conf.rfengine.wal_chunk_target_file_size = ReadableSize::kb(32);
            conf.security = security_config.clone();
        },
    )
    .pd(pd_wrapper)
    .build();
    cluster.start_tikv_workers(alloc_node_id_vec(1), TikvWorkerOptions::default());
    cluster.wait_region_replicated(&[], 3);
    let pd_client = cluster.get_pd_client();
    let mut client = cluster.new_client();

    pd_client.set_tso(TimeStamp::compose(
        chrono::Utc::now().timestamp_millis() as u64,
        0,
    ));

    // Split keyspaces.
    for keyspace_id in 0..KEYSPACE_COUNT as u32 {
        if options.enable_encryption {
            let cfg = KeyspaceEncryptionConfig { enabled: true };
            pd_client.set_keyspace_encryption(keyspace_id, cfg).unwrap();
        }
    }
    client.split_keyspaces(0..KEYSPACE_COUNT as u32);

    cluster.wait_pd_region_count(KEYSPACE_COUNT + 2);

    for keyspace_id in 0..KEYSPACE_COUNT as u32 {
        let i_to_key = gen_keyspace_key(keyspace_id);

        // To make secondary keys in another region.
        if max_data_count >= 10 {
            client.try_split(&i_to_key(max_data_count / 3), 5).unwrap();
            client.try_split(&i_to_key(max_data_count / 2), 5).unwrap();
        }

        // Import basic data.
        client.put_kv(0..BASIC_DATA_COUNT, &i_to_key, i_to_val(BASIC_DATA_LEN));
    }

    let runtime = Runtime::new().unwrap();
    let _enter = runtime.enter();

    // Run cases.
    for (case_idx, &(keyspace_id, data_count, loop_count)) in cases.iter().enumerate() {
        step!("case group: {case_idx}");

        pd_client.enable_default_operator(); // Remove learners by peer count check.

        for loop_idx in 0..loop_count {
            test_restore_archived_keyspace_impl(
                case_idx,
                loop_idx,
                &mut cluster,
                &dfs_config,
                security_config.clone(),
                keyspace_id,
                data_count,
                &runtime,
            );
        }
    }

    cluster.stop();
    oss.shutdown();
}

fn test_restore_archived_keyspace_impl(
    case_idx: usize,
    loop_idx: usize,
    cluster: &mut ServerCluster,
    dfs_config: &DFSConfig,
    security_config: SecurityConfig,
    keyspace_id: u32,
    data_count: usize,
    runtime: &Runtime,
) {
    step!("case: {}:{}", case_idx, loop_idx);
    let mut rng = thread_rng();
    let mut client = cluster.new_client_opt(ClusterClientOptions {
        txn_file_max_chunk_size: Some(1024),
        ..Default::default()
    });
    let i_to_key = gen_keyspace_key(keyspace_id);

    let backup_config = backup::BackupConfig {
        dfs: dfs_config.clone(),
        backup_delay: ReadableDuration::secs(1),
        security: security_config.clone(),
        skip_keyspace_meta: true,
        ..Default::default()
    };
    let s3fs = new_dfs_from_config(dfs_config.clone());
    let reporter = Arc::new(DummyStepReporter::default());

    let pd_client = cluster.get_pd_client();
    let mut archive_config = archive::ArchiveConfig {
        dfs: dfs_config.clone(),
        max_archive_file_size: 1024 * 32,
        dry_run: false,
        security: security_config.clone(),
        ..Default::default()
    };
    archive_config.check_data_dir();

    // Import, backup and archive every day.
    let begin_archive_date =
        chrono::NaiveDateTime::from_timestamp_millis(client.get_ts().physical() as i64).unwrap();
    let mut date_time = begin_archive_date;
    let mut backup_ts_list = Vec::with_capacity(BACKUP_DAYS);
    let mut ref_store_list = Vec::with_capacity(BACKUP_DAYS);
    let mut last_ref_store = client.dump_ref_store();
    for idx in 0..BACKUP_DAYS {
        // Import data.
        let commit_action = CommitAction::AsyncCommitSecondaryKeys(Duration::MAX);
        let write_method = [TxnWriteMethod::Normal, TxnWriteMethod::FileBased]
            .choose(&mut rng)
            .unwrap();
        client
            .try_put_kv(
                0..data_count,
                &i_to_key,
                i_to_val(IMPORT_DATA_LEN + idx),
                MutateOptions {
                    commit_action,
                    write_method: *write_method,
                    ..Default::default()
                },
            )
            .unwrap();
        step!("write done on {}", date_time.date());

        client.verify_data_with_ref_store();
        assert!(
            client
                .verify_data_with_given_ref_store(&last_ref_store, None, &RequestOptions::default())
                .is_err(),
            "case: {}:{}:{}",
            case_idx,
            loop_idx,
            date_time.date(),
        );
        last_ref_store = client.dump_ref_store();
        step!("verify ok on {}", date_time.date());

        // Perform snapshot backup.
        let backup_ts = client.get_ts().into_inner();
        backup_ts_list.push(backup_ts);
        ref_store_list.push(client.dump_ref_store());
        let (_, backup_meta) = backup::backup_cluster_with_ts(
            backup_config.clone(),
            String::default(),
            cluster.get_pd_client().as_ref(),
            backup_ts,
            None,
        )
        .expect("backup::backup_cluster");
        info!("backup_cluster result: {:?}", backup_meta);

        for node_id in cluster.get_nodes() {
            cluster.get_rfengine(node_id).upload_wal_chunk();
        }

        // Archive backup.
        let shards_count = archive::archive_cluster_backup(
            archive_config.clone(),
            cluster.get_pd_client(),
            s3fs.clone(),
            begin_archive_date.date(),
            date_time.date(),
        )
        .unwrap();
        let mut shard_ids = HashSet::default();
        for node_id in cluster.get_nodes() {
            let kv_engine = cluster.get_kvengine(node_id);
            for id_ver in &kv_engine.get_all_shard_id_vers() {
                shard_ids.insert(id_ver.id);
            }
        }
        assert_eq!(shards_count, shard_ids.len());
        step!(
            "archive shards {} done on {}",
            shards_count,
            date_time.date()
        );

        date_time = date_time.checked_add_days(chrono::Days::new(1)).unwrap();
        pd_client.set_tso(TimeStamp::compose(date_time.timestamp_millis() as u64, 0));
    }

    // Delete old backup.
    {
        let snapshot_backup_ts = backup_ts_list[0];
        let snapshot_backup_date = chrono::NaiveDateTime::from_timestamp_millis(
            TimeStamp::from(snapshot_backup_ts).physical() as i64,
        )
        .unwrap()
        .date();
        let archive_reader =
            archive::ArchiveReader::new(s3fs.clone(), &snapshot_backup_date).unwrap();

        let last_archived_snapshot_backup_date = chrono::NaiveDateTime::from_timestamp_millis(
            TimeStamp::from(backup_ts_list[BACKUP_DAYS - 2]).physical() as i64,
        )
        .unwrap()
        .date();
        let last_archive_reader =
            archive::ArchiveReader::new(s3fs.clone(), &last_archived_snapshot_backup_date).unwrap();

        let mut old_wal_rlog_keys = Vec::new();
        let mut old_wal_chunks_len = 0;
        for node_id in cluster.get_nodes() {
            let store_id = cluster.get_store_id(node_id);
            let store_meta = archive_reader.get_store_wal_rlog_meta(store_id).unwrap();
            let last_archived_store_meta = last_archive_reader
                .get_store_wal_rlog_meta(store_id)
                .unwrap();
            for epoch_id in
                store_meta.snapshot_epoch_id..=last_archived_store_meta.snapshot_epoch_id
            {
                if epoch_id < last_archived_store_meta.snapshot_epoch_id {
                    let old_meta_key = rfengine::snapshot_store_meta_key(store_id, epoch_id);
                    old_wal_rlog_keys.push(format!("{}/{}", s3fs.get_prefix(), old_meta_key));
                    let old_rlog_key = rfengine::snapshot_rlog_key(store_id, epoch_id);
                    old_wal_rlog_keys.push(format!("{}/{}", s3fs.get_prefix(), old_rlog_key));
                }
                let old_wal_chunk_prefix = rfengine::wal_chunk_file_prefix(store_id, epoch_id);
                let old_wal_chunk_keys =
                    Dfs::list_objects(s3fs.as_ref(), "", Some(&old_wal_chunk_prefix), None)
                        .map(|(objects, _)| {
                            objects
                                .iter()
                                .map(|object| object.key.clone())
                                .collect::<Vec<_>>()
                        })
                        .unwrap();
                old_wal_chunks_len += old_wal_chunk_keys.len();
                old_wal_rlog_keys.extend(old_wal_chunk_keys)
            }
        }
        let old_file_ids = archive_reader.get_file_ids();
        let mut deleted_old_wal_rlog_cnt = 0;
        s3fs.get_runtime().block_on(async {
            for idx in 0..BACKUP_DAYS - 1 {
                let backup_ts = backup_ts_list[idx];
                let backup_key = backup::backup_file_full_path(
                    s3fs.get_prefix(),
                    String::default(),
                    Some(backup_ts),
                );
                s3fs.delete_object(backup_key.clone(), backup_ts.to_string())
                    .await
                    .unwrap();
            }

            for wal_rlog_key in old_wal_rlog_keys {
                if s3fs
                    .exist(wal_rlog_key.clone(), wal_rlog_key.clone())
                    .await
                    .unwrap()
                {
                    s3fs.delete_object(wal_rlog_key.clone(), wal_rlog_key)
                        .await
                        .unwrap();
                    deleted_old_wal_rlog_cnt += 1;
                }
            }

            for &file_id in &old_file_ids {
                let mut errs = vec![];
                // There is no file type in archive, so we try all types.
                for ftype in [FileType::Sst, FileType::TxnChunk] {
                    match s3fs
                        .delete_object(
                            s3fs.file_key(file_id, ftype),
                            format!("{}.{}", file_id, ftype.suffix()),
                        )
                        .await
                    {
                        Ok(_) => {
                            errs.clear();
                            break;
                        }
                        Err(e) => errs.push(e),
                    }
                }
                assert!(errs.is_empty(), "errs: {:?}", errs);
            }
        });
        let old_rlog_len = (deleted_old_wal_rlog_cnt - old_wal_chunks_len) / 2;
        step!(
            "delete old backup meta, snap rlog {}, wal chunks {} and table files {:?} done. case: {}:{}",
            old_rlog_len,
            old_wal_chunks_len,
            old_file_ids,
            case_idx,
            loop_idx,
        );
    }

    let object_cache = cluster.create_object_cache_randomly();
    let limiter = cluster.create_restore_limiter_randomly(runtime.handle().clone());
    for idx in 0..BACKUP_DAYS - 1 {
        // Restore keyspace.
        {
            let snapshot_backup_ts = backup_ts_list[idx];
            let snapshot_backup_name =
                backup::IncrementalBackupFile::from_backup_ts(snapshot_backup_ts).into_name();
            let restore_config = RestoreConfig {
                security: security_config.clone(),
                ..Default::default()
            };
            restore_keyspace::restore_keyspace(
                keyspace_id,
                keyspace_id,
                &snapshot_backup_name,
                None,
                s3fs.clone(),
                restore_config,
                cluster.get_pd_client(),
                runtime,
                None,
                reporter.clone(),
                object_cache.clone(),
                limiter.clone(),
                None,
            )
            .unwrap();
            step!("restore done. case: {}:{}:{}", case_idx, loop_idx, idx);
        }

        // Verify restored data.
        {
            let verify_options: Vec<(Option<(&[u8], &[u8])>, RequestOptions)> =
                vec![(None, RequestOptions::default())];
            let mut verify_res = vec![];
            for (range, opt) in verify_options {
                verify_res.push(
                    client
                        .verify_data_with_given_ref_store(&ref_store_list[idx], range, &opt)
                        .unwrap(),
                );
            }
            step!(
                "verify restore data done, kv count: {:?}, case: {}:{}:{}",
                verify_res,
                case_idx,
                loop_idx,
                idx
            );
        }
    }

    // Write more data to verify the sequences.
    for id in 0..KEYSPACE_COUNT as u32 {
        let count = if id == keyspace_id {
            data_count
        } else {
            BASIC_DATA_COUNT
        };
        client.put_kv(0..count, gen_keyspace_key(id), i_to_val(FINAL_DATA_LEN));
    }
    let verify_res = client.verify_data_with_ref_store();
    step!("verify more writes done, verify result: {:?}", verify_res);
}

/// This test is to verify that `restore_keyspace` can resolve locks.
///
/// Resolving locks is necessary when a backup is performed after the primary
/// key is committed but the secondary keys are not in a transaction.
///
/// See https://github.com/tidbcloud/cloud-storage-engine/issues/1134.
#[rstest]
#[case::sync(false)]
#[case::async_commit(true)]
fn test_restore_keyspace_with_resolve_locks(#[case] async_commit: bool) {
    const KEYSPACE_ID: u32 = 1;

    test_util::init_log_for_test();
    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test_restore_keyspace_");
    let s3fs = new_dfs_from_config(dfs_config.clone());
    let reporter = Arc::new(DummyStepReporter::default());
    let runtime = Runtime::new().unwrap();
    let _enter = runtime.enter();

    let pd_wrapper = PdWrapper::new_test(1, &SecurityConfig::default(), None);
    let mut cluster = ServerClusterBuilder::new(
        alloc_node_id_vec(NODES_COUNT),
        |_, conf: &mut TikvConfig| {
            conf.dfs = dfs_config.clone();
            // Set small mem-table size to make data reach L0+.
            conf.rocksdb.writecf.write_buffer_size = ReadableSize(128);
            conf.coprocessor.region_split_size = ReadableSize::kb(128); // kv_opts.base_size = 8kb
            conf.coprocessor.region_bucket_size = ReadableSize::kb(64);
            conf.rfengine.target_file_size = ReadableSize::mb(1);
            conf.rfengine.lightweight_backup = true;
            conf.rfengine.wal_chunk_target_file_size = ReadableSize::kb(128);
        },
    )
    .pd(pd_wrapper)
    .build();
    cluster.start_tikv_workers(alloc_node_id_vec(1), TikvWorkerOptions::default());
    cluster.wait_region_replicated(&[], 3);
    let pd_client = cluster.get_pd_client();
    let mut client = cluster.new_client_opt(ClusterClientOptions {
        txn_file_max_chunk_size: Some(1024),
        ..Default::default()
    });
    if async_commit {
        client.set_async_commit();
    }
    client.split_keyspace(KEYSPACE_ID);

    // Import data
    let i_to_key = gen_keyspace_key(KEYSPACE_ID);
    client.put_kv(0..200, &i_to_key, i_to_val(BASIC_DATA_LEN));
    // Delete data without committing secondary keys, to simulate the case that if
    // we don't resolve locks during restoration, the secondary keys will be rolled
    // back unexpectedly after deletion of primary key is compacted.
    for range in [0..25, 25..50, 50..100] {
        let commit_action = if async_commit {
            CommitAction::AsyncCommit(Duration::MAX)
        } else {
            CommitAction::AsyncCommitSecondaryKeys(Duration::MAX)
        };
        client
            .try_del_kv(
                range,
                &i_to_key,
                MutateOptions {
                    commit_action,
                    ..Default::default()
                },
            )
            .unwrap();
    }

    // Wait for data of WRITE_CF being compacted to L1+, and trigger
    // `kvengine::Engine::load_unloaded_tables`.
    wait_for_keyspace_stats(
        &runtime,
        &cluster,
        &pd_client,
        KEYSPACE_ID,
        |stats| stats.cfs[WRITE_CF].levels.iter().any(|l| l.num_tables > 0),
        false,
        Duration::from_secs(10),
    )
    .unwrap();

    let truncate_ts_a = client.get_ts().into_inner();
    let ref_store_a = client.dump_ref_store();

    // Import data by async commit / txn file
    {
        for i in [125, 150, 220] {
            // Split region to make secondary batches of txn chunks.
            client.split(&i_to_key(i));
        }
        let options = if async_commit {
            MutateOptions {
                commit_action: CommitAction::AsyncCommit(Duration::MAX),
                write_method: TxnWriteMethod::Normal,
                ..Default::default()
            }
        } else {
            MutateOptions {
                commit_action: CommitAction::AsyncCommitSecondaryKeys(Duration::MAX),
                write_method: TxnWriteMethod::FileBased,
                ..Default::default()
            }
        };
        client.try_del_kv(100..200, &i_to_key, options).unwrap();

        // Test rollback of txn file locks.
        if !async_commit {
            client
                .try_put_kv(
                    200..300,
                    &i_to_key,
                    i_to_val(IMPORT_DATA_LEN),
                    MutateOptions {
                        commit_action: CommitAction::NoCommit,
                        write_method: TxnWriteMethod::FileBased,
                        ..Default::default()
                    },
                )
                .unwrap();
        }
    }

    let origin_ref_store = client.dump_ref_store();

    // Perform backup.
    let snapshot_backup_name = generate_backup_name();
    {
        let backup_ts = client.get_ts().into_inner();
        let backup_config = backup::BackupConfig {
            dfs: dfs_config,
            backup_delay: ReadableDuration::secs(1),
            skip_keyspace_meta: true,
            ..Default::default()
        };
        let (_, backup_meta) = backup::backup_cluster_with_ts(
            backup_config,
            snapshot_backup_name.clone(),
            cluster.get_pd_client().as_ref(),
            backup_ts,
            None,
        )
        .expect("backup::backup_cluster");
        info!("backup_cluster result: {:?}", backup_meta);
    }

    let restore_config = RestoreConfig::default_for_test();
    let object_cache = cluster.create_object_cache_randomly();
    let limiter = cluster.create_restore_limiter_randomly(runtime.handle().clone());
    // Restore keyspace.
    {
        restore_keyspace::restore_keyspace(
            KEYSPACE_ID,
            KEYSPACE_ID,
            &snapshot_backup_name,
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
        .unwrap();

        // Invoke major compaction to reproduce issue by compacting the deletion of
        // primary key.
        // If restore keyspace do not resolve locks, the secondary keys of "put_kv" will
        // not be compacted, and the following `wait_for_keyspace_stats` will fail.
        let ts = client.get_ts();
        cluster.set_gc_safe_point(ts.into_inner());
        request_major_compaction(&runtime, &pd_client, KEYSPACE_ID);
        wait_for_keyspace_stats(
            &runtime,
            &cluster,
            &pd_client,
            KEYSPACE_ID,
            |stats| stats.cfs[WRITE_CF].levels.iter().all(|l| l.num_tables == 0),
            false,
            Duration::from_secs(10),
        )
        .unwrap();
        std::thread::sleep(Duration::from_secs(10));

        // Verify restored data.
        let verified_cnt = client
            .verify_data_with_given_ref_store(&origin_ref_store, None, &RequestOptions::default())
            .unwrap();
        assert_eq!(verified_cnt, (0, 200));
    }

    // Restore keyspace with truncate ts (truncate_ts_a).
    // To test for resolving locks after truncate ts.
    {
        restore_keyspace::restore_keyspace(
            KEYSPACE_ID,
            KEYSPACE_ID,
            &snapshot_backup_name,
            None,
            s3fs,
            restore_config,
            cluster.get_pd_client(),
            &runtime,
            Some(truncate_ts_a),
            reporter,
            object_cache,
            limiter,
            None,
        )
        .unwrap();
        // Verify restored data.
        let verified_cnt = client
            .verify_data_with_given_ref_store(&ref_store_a, None, &RequestOptions::default())
            .unwrap();
        assert_eq!(verified_cnt, (100, 100));
    }

    cluster.stop();
    oss.shutdown();
}

#[test]
fn test_restore_multiple_keyspaces_uses_rfengine_cache() {
    const KEYSPACE_1: u32 = 1;
    const KEYSPACE_2: u32 = 2;

    test_util::init_log_for_test();
    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test_restore_rfengine_cache_");
    let s3fs = new_dfs_from_config(dfs_config.clone());
    let reporter = Arc::new(DummyStepReporter::default());

    let mut cluster = ServerCluster::new(alloc_node_id_vec(3), |_, conf: &mut TikvConfig| {
        conf.dfs = dfs_config.clone();
        conf.rfengine.lightweight_backup = true;
    });
    cluster.wait_region_replicated(&[], 3);
    let mut client = cluster.new_client();
    client.split_keyspaces(0..3);

    // Import data for two keyspaces in multiple batches to generate SSTs instead of
    // keeping everything in a single mem-table.
    for keyspace_id in [KEYSPACE_1, KEYSPACE_2] {
        for round in 0..10 {
            let start = round * BASIC_DATA_COUNT;
            let end = start + BASIC_DATA_COUNT;
            client.put_kv(
                start..end,
                gen_keyspace_key(keyspace_id),
                i_to_val(BASIC_DATA_LEN),
            );
        }
    }
    let origin_ref_store = client.dump_ref_store();

    // Perform backup.
    let backup_name = generate_backup_name();
    let backup_ts = client.get_ts().into_inner();
    let backup_config = backup::BackupConfig {
        dfs: dfs_config.clone(),
        backup_delay: ReadableDuration::secs(1),
        skip_keyspace_meta: true,
        ..Default::default()
    };
    let (_, backup_meta) = backup::backup_cluster_with_ts(
        backup_config,
        backup_name.clone(),
        cluster.get_pd_client().as_ref(),
        backup_ts,
        None,
    )
    .expect("backup_cluster_with_ts");

    // Mutate data after backup so restore has real work.
    for keyspace_id in [KEYSPACE_1, KEYSPACE_2] {
        client.put_kv(
            BASIC_DATA_COUNT..(BASIC_DATA_COUNT * 2),
            gen_keyspace_key(keyspace_id),
            i_to_val(FINAL_DATA_LEN),
        );
    }

    let restore_config = RestoreConfig::default_for_test();
    let object_cache = cluster.create_object_cache_randomly();
    let limiter_runtime = Runtime::new().unwrap();
    let limiter = cluster.create_restore_limiter_randomly(limiter_runtime.handle().clone());

    // Prepare rfengine cache before running concurrent restores.
    let cache_dir = tempfile::tempdir().unwrap();
    let rfengine_cache = RfEngineCache::new(
        cache_dir.path().to_path_buf(),
        restore_config.clone(),
        s3fs.clone(),
        cluster.get_pd_client(),
    );
    rfengine_cache.register_keyspace(KEYSPACE_1, backup_meta.get_backup_ts());
    rfengine_cache.register_keyspace(KEYSPACE_2, backup_meta.get_backup_ts());
    rfengine_cache
        .fill_cache(
            &backup_name,
            backup_meta.get_backup_ts(),
            object_cache.clone(),
        )
        .unwrap();

    // Run restores concurrently and expect cache hits.
    NATIVE_BR_RFENGINE_CACHE_HIT.reset();
    NATIVE_BR_RFENGINE_CACHE_MISS.reset();
    let store_count = backup_meta.get_stores().len() as u64;
    let barrier = Arc::new(Barrier::new(2));

    let backup_name_cloned = backup_name.clone();
    let cache_for_ks1 = rfengine_cache.clone();
    let reporter_ks1 = reporter.clone();
    let s3fs_ks1 = s3fs.clone();
    let object_cache_ks1 = object_cache.clone();
    let limiter_ks1 = limiter.clone();
    let pd_client_ks1 = cluster.get_pd_client();
    let restore_config_ks1 = restore_config.clone();
    let barrier_ks1 = barrier.clone();
    let handle_ks1 = std::thread::spawn(move || {
        let runtime = Runtime::new().unwrap();
        barrier_ks1.wait();
        restore_keyspace::restore_keyspace(
            KEYSPACE_1,
            KEYSPACE_1,
            &backup_name_cloned,
            None,
            s3fs_ks1,
            restore_config_ks1,
            pd_client_ks1,
            &runtime,
            None,
            reporter_ks1,
            object_cache_ks1,
            limiter_ks1,
            Some(cache_for_ks1),
        )
        .unwrap();
    });

    let cache_for_ks2 = rfengine_cache.clone();
    let reporter_ks2 = reporter.clone();
    let s3fs_ks2 = s3fs.clone();
    let object_cache_ks2 = object_cache.clone();
    let limiter_ks2 = limiter.clone();
    let pd_client_ks2 = cluster.get_pd_client();
    let restore_config_ks2 = restore_config.clone();
    let barrier_ks2 = barrier.clone();
    let handle_ks2 = std::thread::spawn(move || {
        let runtime = Runtime::new().unwrap();
        barrier_ks2.wait();
        restore_keyspace::restore_keyspace(
            KEYSPACE_2,
            KEYSPACE_2,
            &backup_name,
            None,
            s3fs_ks2,
            restore_config_ks2,
            pd_client_ks2,
            &runtime,
            None,
            reporter_ks2,
            object_cache_ks2,
            limiter_ks2,
            Some(cache_for_ks2),
        )
        .unwrap();
    });

    handle_ks1.join().unwrap();
    handle_ks2.join().unwrap();

    // Verify restored data and cache effectiveness.
    let verified_cnt = client
        .verify_data_with_given_ref_store(&origin_ref_store, None, &RequestOptions::default())
        .unwrap();
    assert_eq!(verified_cnt, (BASIC_DATA_COUNT * 20, 0));
    assert_eq!(NATIVE_BR_RFENGINE_CACHE_MISS.get(), 0);
    let cache_hits = NATIVE_BR_RFENGINE_CACHE_HIT.get();
    assert!(
        cache_hits >= store_count * 2,
        "expected at least {} cache hits, got {}",
        store_count * 2,
        cache_hits
    );

    cluster.stop();
    oss.shutdown();
}

/// This test is to verify that `restore_keyspace` can handle the case that in
/// lightweight backup, the last epoch has no chunk in DFS.
///
/// See https://github.com/tidbcloud/cloud-storage-engine/issues/1319.
#[test]
fn test_restore_keyspace_with_no_chunk() {
    const KEYSPACE_ID: u32 = 1;

    test_util::init_log_for_test();
    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test_restore_keyspace_");
    let s3fs = new_dfs_from_config(dfs_config.clone());
    let reporter = Arc::new(DummyStepReporter::default());
    let runtime = Runtime::new().unwrap();

    let mut cluster = ServerCluster::new(
        alloc_node_id_vec(NODES_COUNT),
        |_, conf: &mut TikvConfig| {
            conf.dfs = dfs_config.clone();
            conf.rfengine.target_file_size = ReadableSize::mb(512); // To prevent WAL to compact.
            conf.rfengine.wal_chunk_target_file_size = ReadableSize::mb(64);
            conf.rfengine.lightweight_backup = true;
        },
    );
    cluster.wait_region_replicated(&[], 3);
    let mut client = cluster.new_client();
    client.split_keyspace(KEYSPACE_ID);

    // Import small data
    let i_to_key = gen_keyspace_key(KEYSPACE_ID);
    client.put_kv(0..1, &i_to_key, i_to_val(BASIC_DATA_LEN));
    client.verify_data_with_ref_store();

    // Perform backup.
    let snapshot_backup_name = generate_backup_name();
    {
        let backup_ts = client.get_ts().into_inner();
        let backup_config = backup::BackupConfig {
            dfs: dfs_config,
            backup_delay: ReadableDuration::secs(1),
            skip_keyspace_meta: true,
            ..Default::default()
        };
        let (_, backup_meta) = backup::backup_cluster_with_ts(
            backup_config,
            snapshot_backup_name.clone(),
            cluster.get_pd_client().as_ref(),
            backup_ts,
            None,
        )
        .expect("backup::backup_cluster");
        info!("backup_cluster result: {:?}", backup_meta);
    }

    // Restore keyspace.
    let restore_config = RestoreConfig::default_for_test();
    let object_cache = cluster.create_object_cache_randomly();
    let limiter = cluster.create_restore_limiter_randomly(runtime.handle().clone());
    restore_keyspace::restore_keyspace(
        KEYSPACE_ID,
        KEYSPACE_ID,
        &snapshot_backup_name,
        None,
        s3fs,
        restore_config,
        cluster.get_pd_client(),
        &runtime,
        None,
        reporter,
        object_cache,
        limiter,
        None,
    )
    .unwrap();

    // Verify restored data.
    client.verify_data_with_ref_store();
    cluster.stop();
    oss.shutdown();
}

/// This test is to verify that `restore_keyspace` can properly handle when DFS
/// is slow.
///
/// See https://github.com/tidbcloud/cloud-storage-engine/issues/1977.
#[test]
#[ignore]
fn test_restore_keyspace_with_slow_dfs() {
    const KEYSPACE_ID: u32 = 1;

    test_util::init_log_for_test();
    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test_restore_keyspace_");
    let s3fs = new_dfs_from_config(dfs_config.clone());
    let reporter = Arc::new(DummyStepReporter::default());
    let runtime = Runtime::new().unwrap();

    let mut cluster = ServerCluster::new(
        alloc_node_id_vec(NODES_COUNT),
        |_, conf: &mut TikvConfig| {
            conf.dfs = dfs_config.clone();
            conf.rfengine.target_file_size = ReadableSize::kb(512);
            conf.rfengine.wal_chunk_target_file_size = ReadableSize::kb(32);
            conf.rfengine.lightweight_backup = true;
        },
    );
    cluster.wait_region_replicated(&[], 3);
    let mut client = cluster.new_client();
    client.split_keyspace(KEYSPACE_ID);

    // Import data.
    let i_to_key = gen_keyspace_key(KEYSPACE_ID);
    for i in 0..100 {
        client.put_kv(i * 10..(i + 1) * 10, &i_to_key, i_to_val(BASIC_DATA_LEN));
    }
    client.verify_data_with_ref_store();

    // Workload to continuously generate WAL.
    let (tx, rx) = std::sync::mpsc::sync_channel(0);
    {
        let mut client = cluster.new_client();
        std::thread::spawn(move || {
            while let Err(std::sync::mpsc::TryRecvError::Empty) = rx.try_recv() {
                let _ = client.try_put_kv(
                    10000..10050,
                    &i_to_key,
                    i_to_val(POST_BACKUP_DATA_LEN),
                    MutateOptions::default(),
                );
                let _ = client.try_del_kv(10000..10050, &i_to_key, MutateOptions::default());
            }
        });
    }

    // Delay "wal_chunks" but not snapshots.
    oss.set_put_delay("wal_chunks", Duration::from_secs(3));

    // Perform backup.
    let snapshot_backup_name = generate_backup_name();
    {
        let backup_ts = client.get_ts().into_inner();
        let backup_config = backup::BackupConfig {
            dfs: dfs_config.clone(),
            backup_delay: ReadableDuration::secs(1),
            skip_keyspace_meta: true,
            ..Default::default()
        };
        let (_, backup_meta) = backup::backup_cluster_with_ts(
            backup_config,
            snapshot_backup_name.clone(),
            cluster.get_pd_client().as_ref(),
            backup_ts,
            None,
        )
        .expect("backup::backup_cluster");
        info!("backup_cluster result: {:?}", backup_meta);
    }

    let restore_config = RestoreConfig::default_for_test();
    let object_cache = cluster.create_object_cache_randomly();
    let limiter = cluster.create_restore_limiter_randomly(runtime.handle().clone());
    NATIVE_BR_RFENGINE_WAL_EPOCH_OVERWRITTEN_ERROR.reset();
    let mut ok = false;
    for _ in 0..30 {
        // Restore keyspace.
        restore_keyspace::restore_keyspace(
            KEYSPACE_ID,
            KEYSPACE_ID,
            &snapshot_backup_name,
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
        .unwrap();

        // Note: the counter is not reliable enough that the error must happen, as there
        // are other concurrent test cases. But it's enough to verify the
        // process.
        if NATIVE_BR_RFENGINE_WAL_EPOCH_OVERWRITTEN_ERROR.get() > 0 {
            ok = true;
            break;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    assert!(ok);
    tx.send(()).unwrap();

    // Verify restored data.
    client.verify_data_with_ref_store();
    cluster.stop();
    oss.shutdown();
}

#[test]
fn test_restore_keyspace_with_schema() {
    const KEYSPACE_ID: u32 = 1;

    test_util::init_log_for_test();
    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test_restore_keyspace_");
    let s3fs = new_dfs_from_config(dfs_config.clone());
    let reporter = Arc::new(DummyStepReporter::default());
    let runtime = Runtime::new().unwrap();

    let mut cluster = ServerCluster::new(
        alloc_node_id_vec(NODES_COUNT),
        |_, conf: &mut TikvConfig| {
            conf.dfs = dfs_config.clone();
            conf.rfengine.lightweight_backup = true;
        },
    );
    cluster.wait_region_replicated(&[], 3);
    let mut client = cluster.new_client();
    client.split_keyspace(KEYSPACE_ID);

    // Import small data
    let i_to_key = gen_keyspace_key(KEYSPACE_ID);
    client.put_kv(0..1, &i_to_key, i_to_val(BASIC_DATA_LEN));
    client.verify_data_with_ref_store();

    // Set schema file
    let mut schemas = vec![];
    for i in 0..=10 {
        let schema = SchemaBuf::new(
            i,
            new_int_handle_column_info(),
            new_version_column_info(),
            vec![new_int_handle_column_info()],
            vec![],
            0,
            vec![],
            vec![],
            StorageClassSpec::default(),
            None,
        )
        .into();
        schemas.push(schema);
    }
    let schema_version = 100;
    let schema_file_data = build_schema_file(KEYSPACE_ID, schema_version, schemas.clone(), 0);
    let broadcast_schema_update = |schema_file_data: Vec<u8>| {
        let file_id = client.pd_client().alloc_id().unwrap();
        runtime
            .block_on(s3fs.create(
                file_id,
                Bytes::from(schema_file_data),
                Options::default().with_type(FileType::Schema),
            ))
            .unwrap();
        let stores = client.pd_client().get_all_stores(true).unwrap();
        let security_mgr = Arc::new(SecurityManager::new(&Default::default()).unwrap());
        runtime.block_on(broadcast_schema_update_to_all_stores(
            &stores,
            security_mgr.clone(),
            time::Duration::from_secs(5),
            KEYSPACE_ID,
            file_id,
        ));
    };
    broadcast_schema_update(schema_file_data);

    // Check the schema file already installed.
    must_wait(
        || {
            let data_stats = cluster.get_data_stats();
            let mut schema_file_exists = false;
            data_stats.iter_shard_stats(|_, shard_stats| {
                if shard_stats.schema_version > 0 {
                    schema_file_exists = true;
                    return true;
                }
                false
            });
            schema_file_exists
        },
        10,
        || "failed to install schema file".to_string(),
    );

    // Perform backup.
    let snapshot_backup_name = generate_backup_name();
    {
        let backup_ts = client.get_ts().into_inner();
        let backup_config = backup::BackupConfig {
            dfs: dfs_config,
            backup_delay: ReadableDuration::secs(1),
            skip_keyspace_meta: true,
            ..Default::default()
        };
        let (_, backup_meta) = backup::backup_cluster_with_ts(
            backup_config,
            snapshot_backup_name.clone(),
            cluster.get_pd_client().as_ref(),
            backup_ts,
            None,
        )
        .expect("backup::backup_cluster");
        info!("backup_cluster result: {:?}", backup_meta);
    }

    // Restore keyspace.
    let restore_config = RestoreConfig::default_for_test();
    let object_cache = cluster.create_object_cache_randomly();
    let limiter = cluster.create_restore_limiter_randomly(runtime.handle().clone());
    restore_keyspace::restore_keyspace(
        KEYSPACE_ID,
        KEYSPACE_ID,
        &snapshot_backup_name,
        None,
        s3fs.clone(),
        restore_config,
        cluster.get_pd_client(),
        &runtime,
        None,
        reporter,
        object_cache,
        limiter,
        None,
    )
    .unwrap();

    let mut restore_version = 0;
    // Check if the schema file is update the restore_version after restoration.
    must_wait(
        || {
            let data_stats = cluster.get_data_stats();
            let mut has_restore_version = false;
            data_stats.iter_shard_stats(|_, shard_stats| {
                if shard_stats.schema_restore_version > 0 {
                    has_restore_version = true;
                    restore_version = shard_stats.schema_restore_version;
                    return true;
                }
                false
            });
            has_restore_version
        },
        10,
        || "failed to check schema file restore_version".to_string(),
    );

    // Try to update schema with different restore_version.
    let schema_file_data =
        build_schema_file(KEYSPACE_ID, schema_version, schemas, restore_version + 1);
    broadcast_schema_update(schema_file_data);

    // Check if the schema file is update the restore_version after restoration.
    let restore_version_changed = try_wait(
        || {
            let data_stats = cluster.get_data_stats();
            let mut version_changed = false;
            data_stats.iter_shard_stats(|_, shard_stats| {
                if shard_stats.schema_restore_version > 0 {
                    version_changed = restore_version != shard_stats.schema_restore_version;
                    return true;
                }
                false
            });
            version_changed
        },
        5,
    );
    assert!(!restore_version_changed);

    // Verify restored data.
    client.verify_data_with_ref_store();
    cluster.stop();
    oss.shutdown();
}

#[derive(PartialEq, Debug)]
enum RestoreResult {
    AlwaysSucceed,
    AlwaysFail,
    DependsOnTolerateErr,
}

// Arguments: (fail_on_backup, fail_on_restore, fail_on_same_node, expected)
// fail_on_same_node: Whether backup and restore fail on the same node.
#[rstest]
#[case::fail_on_backup(true, false, false, RestoreResult::AlwaysSucceed)]
#[case::fail_on_restore(false, true, false, RestoreResult::DependsOnTolerateErr)]
#[case::both(true, true, false, RestoreResult::AlwaysFail)]
#[case::fail_on_same_node(true, true, true, RestoreResult::AlwaysSucceed)]
fn test_restore_keyspace_with_failed_store(
    #[case] fail_on_backup: bool,
    #[case] fail_on_restore: bool,
    #[case] fail_on_same_node: bool,
    #[case] expected: RestoreResult,
) {
    test_util::init_log_for_test();
    info!("test_restore_keyspace_with_failed_store";
        "fail_on_backup" => fail_on_backup,
        "fail_on_restore" => fail_on_restore,
        "fail_on_same_node" => fail_on_same_node,
        "expected" => ?expected,
    );
    const KEYSPACE_ID: u32 = 1;

    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test_restore_keyspace_");
    let s3fs = new_dfs_from_config(dfs_config.clone());
    let reporter = Arc::new(DummyStepReporter::default());
    let runtime = Runtime::new().unwrap();

    // Use number of nodes of 3 to make sure that every node has WAL chunks not
    // flushed yet.
    let mut cluster = ServerCluster::new(alloc_node_id_vec(3), |_, conf: &mut TikvConfig| {
        conf.dfs = dfs_config.clone();
        conf.rfengine.target_file_size = ReadableSize::mb(512); // To prevent WAL to compact.
        conf.rfengine.wal_chunk_target_file_size = ReadableSize::mb(64);
        conf.rfengine.lightweight_backup = true;
    });
    cluster.wait_region_replicated(&[], 3);
    let mut client = cluster.new_client();
    client.split_keyspace(KEYSPACE_ID);

    // Import small data
    let i_to_key = gen_keyspace_key(KEYSPACE_ID);
    client.put_kv(0..1, &i_to_key, i_to_val(BASIC_DATA_LEN));
    client.verify_data_with_ref_store();
    let origin_ref_store = client.dump_ref_store();

    // Perform backup.
    let snapshot_backup_name = generate_backup_name();
    let backup_failed_node = {
        let backup_ts = client.get_ts().into_inner();
        let backup_config = backup::BackupConfig {
            dfs: dfs_config,
            backup_delay: ReadableDuration::secs(1),
            tolerate_err: 1,
            skip_keyspace_meta: true,
            timeout: ReadableDuration::secs(3),
            ..Default::default()
        };

        let failed_node_id = if fail_on_backup {
            Some(stop_node(&mut cluster, true, false, None))
        } else {
            None
        };

        let (_, backup_meta) = backup::backup_cluster_with_ts(
            backup_config,
            snapshot_backup_name.clone(),
            cluster.get_pd_client().as_ref(),
            backup_ts,
            None,
        )
        .expect("backup::backup_cluster");
        info!("backup_cluster result: {:?}", backup_meta);

        if let Some(node_id) = failed_node_id {
            cluster.start_node(node_id, |_, _| {});
        }
        failed_node_id
    };

    // Put another data.
    client.put_kv(1..2, &i_to_key, i_to_val(POST_BACKUP_DATA_LEN));
    client.verify_data_with_ref_store();

    if fail_on_restore {
        // Force stop one node without flush the last WAL chunk to DFS.
        let restore_failed_node = match backup_failed_node {
            Some(node_id) => {
                let node_id = if fail_on_same_node {
                    node_id
                } else {
                    let mut node_ids = cluster.get_nodes();
                    node_ids.retain(|&id| id != node_id);
                    *node_ids.choose(&mut rand::thread_rng()).unwrap()
                };
                Some(node_id)
            }
            _ => None,
        };
        let store_tombstone = thread_rng().gen_bool(0.5);
        stop_node(&mut cluster, true, store_tombstone, restore_failed_node);
    }

    // Set small timeouts to make the test faster.
    let mut restore_config = RestoreConfig {
        tolerate_err: 0,
        timeout_fetch_wal: ReadableDuration::secs(1),
        timeout_restore_snapshot: ReadableDuration::secs(3),
        strict_tolerate: true,
        lower_memory: RestoreConfig::use_lower_memory(),
        ..Default::default()
    };
    let object_cache = cluster.create_object_cache_randomly();
    let limiter = cluster.create_restore_limiter_randomly(runtime.handle().clone());

    let res = restore_keyspace::restore_keyspace(
        KEYSPACE_ID,
        KEYSPACE_ID,
        &snapshot_backup_name,
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
    );
    assert_eq!(
        res.is_ok(),
        expected == RestoreResult::AlwaysSucceed,
        "res: {:?}, expected: {:?}",
        res,
        expected
    );

    // Update tolerate error config and restore keyspace again.
    restore_config.tolerate_err = 1;

    // Restore keyspace.
    let res_tolerated = restore_keyspace::restore_keyspace(
        KEYSPACE_ID,
        KEYSPACE_ID,
        &snapshot_backup_name,
        None,
        s3fs,
        restore_config,
        cluster.get_pd_client(),
        &runtime,
        None,
        reporter,
        object_cache,
        limiter,
        None,
    );
    assert_eq!(
        res_tolerated.is_err(),
        expected == RestoreResult::AlwaysFail,
        "res: {:?}, expected: {:?}",
        res,
        expected
    );

    // Verify restored data.
    if res.is_err() && res_tolerated.is_err() {
        client.verify_data_with_ref_store();
    } else {
        client
            .verify_data_with_given_ref_store(&origin_ref_store, None, &RequestOptions::default())
            .unwrap();
    }
    cluster.stop();
    oss.shutdown();
}

// Stop random node when `node_id` is `None`.
fn stop_node(
    cluster: &mut ServerCluster,
    force: bool,
    store_tombstone: bool,
    node_id: Option<u16>,
) -> u16 {
    let failed_node_id = node_id.unwrap_or_else(|| {
        let node_ids = cluster.get_nodes();
        let mut rng = rand::thread_rng();
        *node_ids.choose(&mut rng).unwrap()
    });
    let store_id = cluster.get_store_id(failed_node_id);

    cluster.stop_node_force(failed_node_id, force);
    if store_tombstone {
        let mut store = cluster.get_pd_client().get_store(store_id).unwrap();
        store.state = metapb::StoreState::Tombstone;
        cluster.get_pd_client().put_store(store).unwrap();
    }
    info!("node stopped"; "node_id" => failed_node_id, "store_id" => store_id, "tombstone" => store_tombstone);

    failed_node_id
}

fn i_to_key(i: usize) -> Vec<u8> {
    format!("tkey_{:08}", i).into_bytes()
}

fn random_val() -> Vec<u8> {
    let mut bytes = [0u8; RANDOM_VALUE_LEN];
    rand::thread_rng().fill(&mut bytes);
    bytes.to_vec()
}

// Generate `i_to_val` by specifying expected length of value, to make it easier
// to know where the data is written.
// `expected_len` must be > 72
// `expected_len` = 64(random_val) + i(8) + padding
fn i_to_val(expected_len: usize) -> impl Fn(usize) -> Vec<u8> {
    assert!(
        expected_len > 8 + RANDOM_VALUE_LEN,
        "expected_len: {} is not larger than 72",
        expected_len
    );
    move |i: usize| -> Vec<u8> {
        let mut val = format!("{:08}", i).into_bytes();
        let mut padding = vec![b'0'; expected_len - 8 - RANDOM_VALUE_LEN];
        val.append(&mut padding);
        val.append(&mut random_val());
        val
    }
}

fn gen_keyspace_key(keyspace_id: u32) -> impl Fn(usize) -> Vec<u8> {
    move |i: usize| -> Vec<u8> {
        let mut key = ApiV2::get_keyspace_prefix_by_id(keyspace_id);
        key.extend(i_to_key(i));
        key
    }
}

// Add learners to all stores with no peer of keyspace regions.
// TODO: add to `test_cloud_server` for reuse.
fn add_learners(cluster: &mut ServerCluster, start: &[u8], end: &[u8]) {
    let pd_client = cluster.get_pd_client();
    let mut client = cluster.new_client();
    let stores = cluster.get_stores();

    client.clear_region_cache();

    let mut next_key = start.to_owned();
    while next_key.as_slice() < end {
        let region = client.get_region_by_key(&next_key);
        next_key = region.raw_end().to_owned();

        let learner_stores = stores
            .iter()
            .filter(|&&store_id| region.peers.iter().all(|peer| peer.store_id != store_id))
            .collect::<Vec<_>>();
        for &store_id in learner_stores {
            let learner_peer = new_learner_peer(store_id, pd_client.alloc_id().unwrap());
            pd_client.must_add_peer(region.id, learner_peer);
        }
    }
}

fn check_learners_impl(
    cluster: &ServerCluster,
    start: &[u8],
    end: &[u8],
    min_count: usize,
) -> Vec<test_cloud_server::util::RawRegion> {
    let mut client = cluster.new_client();

    let mut res = vec![];
    let mut next_key = start.to_owned();
    while next_key.as_slice() < end {
        let region = client.get_region_by_key(&next_key);
        next_key = region.raw_end().to_owned();

        let learner_cnt = region
            .peers
            .iter()
            .filter(|peer| peer.get_role() == metapb::PeerRole::Learner)
            .count();
        debug!(
            "check region for learner: {:?}, count: {}",
            region, learner_cnt
        );
        if learner_cnt < min_count {
            res.push(region);
        }
    }
    res
}

fn check_learners(
    cluster: &ServerCluster,
    start: &[u8],
    end: &[u8],
    min_count: usize,
    timeout_seconds: usize,
) {
    let ok = try_wait(
        || check_learners_impl(cluster, start, end, min_count).is_empty(),
        timeout_seconds,
    );
    assert!(
        ok,
        "{:?}",
        check_learners_impl(cluster, start, end, min_count)
    );
}

fn generate_backup_name() -> String {
    static BACKUP_ID: AtomicUsize = AtomicUsize::new(0);
    format!("{:04}", BACKUP_ID.fetch_add(1, Ordering::Relaxed))
}
