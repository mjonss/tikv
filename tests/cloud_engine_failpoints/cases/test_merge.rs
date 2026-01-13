// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    thread::JoinHandle,
    time::Duration,
};

use test_cloud_server::{ServerCluster, client::ClusterClient};
use tikv_util::{info, time::Instant};

use crate::cases::{alloc_node_id_vec, i_to_key, i_to_val};

// Test for the scenario that prepare merge & rollback merge have write conflict
// on pending merge state.
// See https://github.com/tidbcloud/cloud-storage-engine/issues/664
#[test]
fn test_merge_pending_state_conflict() {
    const TEST_DURATION: Duration = Duration::from_secs(10);
    const APPLY_DELAY: &str = "sleep(100)";

    let schedule_merge_error_fp = "on_schedule_merge_error";
    let on_follower_exec_rollback_merge_fp = "on_follower_exec_rollback_merge";

    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(alloc_node_id_vec(3), |_, _| {});
    cluster.wait_region_replicated(&[], 3);
    cluster.get_pd_client().disable_default_operator();

    let mut client = cluster.new_client();
    let prev_keyspace = i_to_key(0);
    client.split(&prev_keyspace);

    let split_key = i_to_key(5);
    client.split(&split_key);
    cluster.wait_pd_region_count(3);

    client.put_kv(0..10, i_to_key, i_to_val);

    // `schedule_merge` returns error to make merges rollback.
    fail::cfg(schedule_merge_error_fp, "return").unwrap();

    let merge_thread = spawn_merge(
        cluster.new_client(),
        i_to_key(0),
        i_to_key(10),
        TEST_DURATION,
    );

    // Delay the apply of rollback merge.
    // The delay is applied to followers only, as this issue would not happen on
    // leader (when there is a pending merge, leader will reject other merge
    // proposals).
    fail::cfg(on_follower_exec_rollback_merge_fp, APPLY_DELAY).unwrap();

    merge_thread.join().unwrap();

    fail::remove(schedule_merge_error_fp);
    fail::remove(on_follower_exec_rollback_merge_fp);

    // `try_merge` may be rollback by target region changed.
    for _ in 0..10 {
        client.try_merge(&i_to_key(0), &i_to_key(10));
        if cluster.get_pd_client_ext().get_regions_number() == 2 {
            break;
        }
        std::thread::sleep(Duration::from_secs(1));
    }

    client.verify_data_with_ref_store();
    cluster.stop();
}

fn spawn_merge(
    mut client: ClusterClient,
    mut source_key: Vec<u8>,
    mut target_key: Vec<u8>,
    duration: Duration,
) -> JoinHandle<()> {
    let merge_counter = AtomicUsize::new(0);
    std::thread::spawn(move || {
        let start_time = Instant::now();
        while start_time.saturating_elapsed() < duration {
            let req_sent = client.try_merge(&source_key, &target_key);
            assert!(req_sent);
            merge_counter.fetch_add(1, Ordering::SeqCst);

            // Swap keys to propose more merges, and make issue reproduction more easily.
            std::mem::swap(&mut source_key, &mut target_key);
        }
        info!(
            "merge thread exit, merge counter {}",
            merge_counter.load(Ordering::Relaxed)
        );
    })
}
