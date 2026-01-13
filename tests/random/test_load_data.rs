// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, atomic::Ordering},
    thread::{JoinHandle, sleep},
    time::Duration,
};

use cloud_encryption::MasterKey;
use futures::executor::block_on;
use kvengine::{dfs, dfs::DFSConfig, table::ZSTD_COMPRESSION};
use load_data::task::{LoadDataConfig, LoadDataContext};
use pd_client::PdClient;
use rand::{Rng, rngs::ThreadRng};
use schema::schema::StorageClassSpec;
use security::SecurityConfig;
use test_cloud_server::{
    keyspace::{ClusterKeyspaceClient, KeyspaceManager, make_row_key},
    load_data::{build, cleanup, init_task, put_chunks},
};
use tikv_util::{info, time::Instant};
use tokio::runtime::Runtime;

use crate::{LOAD_DATA_COUNTER, TABLE_COUNTER, generate_random_string, i_to_key};

const COMPRESSION_TYPE: u8 = ZSTD_COMPRESSION;

pub(crate) fn spawn_load_data(
    pd_client: Arc<dyn PdClient>,
    mut client: ClusterKeyspaceClient,
    dfs_conf: DFSConfig,
    security_config: SecurityConfig,
    load_data_config: LoadDataConfig,
    keyspace_manager: KeyspaceManager,
    task_timeout: Duration,
    interval: Duration,
    timeout: Duration,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut rng = rand::thread_rng();
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .thread_name("load-data")
                .build()
                .unwrap(),
        );
        let dfs = Arc::new(kvengine::dfs::S3Fs::new_from_config(dfs_conf));
        let master_key = runtime.block_on(security_config.new_master_key());

        let start_time = Instant::now();
        let mut last_time = start_time;
        while start_time.saturating_elapsed() < timeout {
            let keyspace_id = keyspace_manager.get_uniform_random_keyspace(&mut rng);
            let table_id = keyspace_manager
                .get_keyspace_meta(keyspace_id)
                .unwrap()
                .new_table(false, false, StorageClassSpec::default());
            TABLE_COUNTER.fetch_add(1, Ordering::Relaxed);

            let success = do_load_data(
                keyspace_id,
                table_id,
                pd_client.clone(),
                &mut client,
                runtime.clone(),
                dfs.clone(),
                &keyspace_manager,
                &mut rng,
                master_key.clone(),
                load_data_config.clone(),
                task_timeout,
            );
            if !success {
                // table is removed by restore.
                continue;
            }

            LOAD_DATA_COUNTER.fetch_add(1, Ordering::Relaxed);
            let elapsed = last_time.saturating_elapsed();
            sleep(interval.saturating_sub(elapsed));
            last_time = Instant::now();
        }
        info!("load data thread exit");
    })
}

fn do_load_data(
    keyspace_id: u32,
    table_id: i64,
    pd_client: Arc<dyn PdClient>,
    client: &mut ClusterKeyspaceClient,
    runtime: Arc<Runtime>,
    dfs: Arc<dyn dfs::Dfs>,
    keyspace_manager: &KeyspaceManager,
    rng: &mut ThreadRng,
    master_key: MasterKey,
    config: LoadDataConfig,
    task_timeout: Duration,
) -> bool /* success */ {
    let temp_dir = tempfile::Builder::new()
        .prefix("load_data_")
        .tempdir()
        .unwrap();

    // Should lock from acquire `start_ts` to ingest ref store, to block backup.
    // See https://github.com/tidbcloud/cloud-storage-engine/issues/1036.
    let lock = keyspace_manager.get_keyspace_lock(keyspace_id);
    let guard = runtime.block_on(lock.shared_lock());

    // Load data would be conflict with major compaction, as major compaction will
    // rewrite SST files and cause the deduplicate process of load data doesn't
    // work. Currently we block major compaction by lock to work around this
    // problem.
    // Also note that in production environment the retry of ingest will fail (and
    // will never succeed) due to the conflict.
    // See `ShardMeta::check_overlap_for_load_data`.
    let extra_guard = lock.extra_lock();

    if keyspace_manager
        .get_keyspace_meta(keyspace_id)
        .unwrap()
        .get_table(table_id)
        .is_none()
    {
        // Should be caused by keyspace restore.
        info!(
            "do_load_data: skip load data, keyspace {} table {} not exists (removed by restore)",
            keyspace_id, table_id
        );
        return false;
    }

    // Init task.
    let start_ts = block_on(pd_client.get_tso()).unwrap().into_inner();
    let commit_ts = block_on(pd_client.get_tso()).unwrap().into_inner();
    let load_data_ctx = LoadDataContext {
        dir: temp_dir.path().to_path_buf(),
        dfs,
        pd: pd_client.clone(),
        runtime: runtime.clone(),
        master_key,
    };
    let (scheduler, worker_handle) = init_task(config, load_data_ctx, start_ts, commit_ts);
    info!(
        "load_data.init_task: keyspace {}, table {}, start_ts {}, commit_ts {}",
        keyspace_id, table_id, start_ts, commit_ts
    );

    // Put chunks.
    let data_count = rng.gen_range(1..=10) * 1000_usize; // generate at most about 2.5MB (10000 x 256) data, 160 (2.5MB / 16KB) SST files.
    let data_batch_size = rng.gen_range(1..=10) * 10_usize;
    let writer_count = rng.gen_range(1..=5);
    let generate_key =
        move |i: usize| -> Vec<u8> { make_row_key(keyspace_id, table_id, &i_to_key(i)) };
    let generate_row_id = move |i: usize| -> Vec<u8> { i.to_be_bytes().to_vec() };
    let ref_store = put_chunks(
        &scheduler,
        writer_count,
        data_count,
        data_batch_size,
        generate_key,
        generate_random_string(format!("ingest-{}-", commit_ts)),
        generate_row_id,
        task_timeout,
        |_| 0,
    );
    info!(
        "load_data.put_chunks: keyspace {}, table {}, data_count {}",
        keyspace_id, table_id, data_count
    );

    // Build.
    build(&scheduler, COMPRESSION_TYPE, task_timeout).unwrap();
    info!(
        "load_data: build finished, keyspace {}, table {}",
        keyspace_id, table_id
    );

    drop(extra_guard);

    // Verify the data consistency, to find problems earlier.
    // Must be in lock context to block restore.
    // TODO: remove when it's stable.
    let start_key = make_row_key(keyspace_id, table_id, &[]);
    let end_key = make_row_key(keyspace_id, table_id + 1, &[]);
    let verified_count = runtime
        .block_on(client.verify_data_by_scan(&ref_store, Some((&start_key, &end_key))))
        .expect("verify data of load_data failed");
    assert_eq!(verified_count, data_count);
    info!(
        "load_data: verified ok, keyspace {}, table {}, verified_count {}",
        keyspace_id, table_id, verified_count
    );

    keyspace_manager.ref_stores().ingest(keyspace_id, ref_store);
    keyspace_manager
        .get_keyspace_meta(keyspace_id)
        .unwrap()
        .get_table(table_id)
        .unwrap()
        .set_available(true);

    drop(guard);

    // Cleanup.
    cleanup(&scheduler, worker_handle);
    true
}

pub(crate) fn check_load_data() {
    let load_data_counter = LOAD_DATA_COUNTER.load(Ordering::Relaxed);
    assert!(
        load_data_counter > 0, // increase after optimize ref store verification.
        "load_data_counter too small: {}",
        load_data_counter
    );
}
