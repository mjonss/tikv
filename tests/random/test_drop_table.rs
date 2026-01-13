// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::atomic::Ordering, time::Duration};

use schema::schema::StorageClassSpec;
use test_cloud_server::{
    client::ClusterTxnClient,
    keyspace::{ClusterKeyspaceClient, KeyspaceManager, make_row_key},
};
use tikv_util::{info, time::Instant};

use crate::{DROP_TABLE_COUNTER, TABLE_COUNTER};

pub fn spawn_drop_table(
    mut client: ClusterKeyspaceClient,
    interval: Duration,
    timeout: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let start_time = Instant::now_coarse();
        while start_time.saturating_elapsed() < timeout {
            let loop_start_time = Instant::now_coarse();
            let random = || {
                let mut rng = rand::thread_rng();
                let keyspace_id = client.keyspace_manager().get_zipf_random_keyspace(&mut rng);
                let table_meta = client.keyspace_manager().get_random_available_table(
                    keyspace_id,
                    &mut rng,
                    false,
                );
                (keyspace_id, table_meta)
            };
            let (keyspace_id, table_meta) = random();
            let Some(table_meta) = table_meta else {
                continue;
            };
            let table_id = table_meta.id();
            client.drop_table(keyspace_id, table_id).await.unwrap();
            DROP_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed);

            // Add a new table to keep number of tables.
            // FIXME: copy the storage class & build schema.
            client
                .keyspace_manager()
                .get_keyspace_meta(keyspace_id)
                .unwrap()
                .new_table(
                    true,
                    table_meta.is_schema_enabled(),
                    StorageClassSpec::default(),
                );
            TABLE_COUNTER.fetch_add(1, Ordering::Relaxed);

            let sleep_time = interval.saturating_sub(loop_start_time.saturating_elapsed());
            tokio::time::sleep(sleep_time).await;
        }
        info!("drop table thread exit");
    })
}

pub(crate) fn check_drop_table() {
    let counter = DROP_TABLE_COUNTER.load(Ordering::SeqCst);
    assert!(counter >= 3, "too few drop table: {}", counter);
}

pub(crate) async fn pick_and_run_pending_destroy_range(
    keyspace_id: u32,
    gc_safepoint: u64,
    keyspace_manager: &KeyspaceManager,
    client: &ClusterTxnClient,
) -> crate::Result<()> {
    // Locking to make picking pending task & invoking destroy range (set
    // `_del_prefixes` property) atomic (against backup & restore).
    let lock = keyspace_manager.get_keyspace_lock(keyspace_id);
    let _guard = lock.mutex_lock().await;

    let tasks = keyspace_manager.pick_destroy_range_tasks(keyspace_id, gc_safepoint);
    for task in tasks {
        {
            let keyspace = keyspace_manager.get_keyspace_meta(keyspace_id).unwrap();
            let table = keyspace.get_table(task.table_id).unwrap();
            assert!(
                !table.is_available(),
                "table should not be available: keyspace_id: {}, table: {:?}",
                keyspace_id,
                table.value(),
            );
        }

        let start_key = make_row_key(keyspace_id, task.table_id, &[]);
        let end_key = make_row_key(keyspace_id, task.table_id + 1, &[]);

        info!("pick_and_run_pending_destroy_range: kv_unsafe_destroy_range";
            "start_key" => log_wrappers::hex_encode_upper(&start_key),
            "end_key" => log_wrappers::hex_encode_upper(&end_key),
            "keyspace_id" => keyspace_id,
            "task" => ?task,
            "gc_safepoint" => gc_safepoint);
        client
            .kv_unsafe_destroy_range(&start_key, &end_key, Duration::from_secs(30))
            .await?;
    }
    Ok(())
}
