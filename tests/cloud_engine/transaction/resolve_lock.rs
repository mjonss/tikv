// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{thread, time::Duration};

use bytes::Bytes;
use kvproto::kvrpcpb;
use test_cloud_server::{ServerCluster, client::TxnMutations, util::Mutation};
use tikv_util::time::Instant;

use crate::{alloc_node_id_vec, i_to_key, i_to_val};

#[test]
fn test_resolve_lock() {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(alloc_node_id_vec(3), |_, _| {});
    cluster.wait_region_replicated(&[], 3);
    let mut client = cluster.new_client();

    // Split to test for resolve locks on multiple regions.
    let split_keys = [5, 15, 33, 35];
    for i in split_keys {
        client.split(&i_to_key(i));
    }
    cluster.wait_pd_region_min_count(split_keys.len() + 1);

    // Test for kv_prewrite.
    {
        let mut mutations = vec![];
        for i in 0..10 {
            mutations.push(Mutation {
                key: Bytes::from(i_to_key(i)),
                value: Bytes::from(i_to_val(i)),
                op: kvrpcpb::Op::Put,
                ..Default::default()
            });
        }
        let start_ts = client.get_ts();
        // prewrite but no commit.
        let txn_muts = TxnMutations::from_normal(mutations);
        client
            .kv_prewrite(txn_muts.primary(), None, txn_muts, start_ts)
            .expect("kv_prewrite");

        // prewrite should meet locks of previous prewrite.
        client.put_kv(0..20, i_to_key, i_to_val);
        client.verify_data_with_ref_store();
    }

    // Test for kv_get.
    {
        let mut mutations = vec![];
        for i in 30..40 {
            mutations.push(Mutation {
                key: Bytes::from(i_to_key(i)),
                value: Bytes::from(i_to_val(i)),
                op: kvrpcpb::Op::Put,
                ..Default::default()
            });
        }
        let start_ts = client.get_ts();
        let put_time = Instant::now();
        let txn_muts = TxnMutations::from_normal(mutations);
        client
            .kv_prewrite(txn_muts.primary(), None, txn_muts.clone(), start_ts)
            .expect("kv_prewrite");

        let mut client1 = cluster.new_client();
        let handle = thread::spawn(move || {
            // `get_key` should wait until committed, and get the committed value.
            let (val, _) = client1.must_get_key(&i_to_key(35), put_time);
            assert_eq!(val, i_to_val(35));
        });

        thread::sleep(Duration::from_secs(1));
        let commit_ts = client.get_ts();
        client
            .kv_commit(txn_muts, start_ts, commit_ts, false)
            .expect("kv_commit");

        handle.join().unwrap();
        client.verify_data_with_ref_store();
    }

    cluster.stop();
}
