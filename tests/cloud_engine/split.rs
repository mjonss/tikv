// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use futures::executor::block_on;
use pd_client::PdClient;
use test_cloud_server::{ServerCluster, alloc_node_id_vec};
use tikv_util::codec::bytes::encode_bytes;

use crate::i_to_key;

#[test]
fn test_split_regions_exceed_limit() {
    test_util::init_log_for_test();

    const SPLIT_REGION_MAX_KEYS: usize = 64;

    let mut cluster = ServerCluster::new(alloc_node_id_vec(3), |_, conf| {
        conf.raft_store.split_region_max_keys = SPLIT_REGION_MAX_KEYS;
    });
    cluster.wait_region_replicated(&[], 3);
    let pd_client = cluster.get_pd_client();

    {
        let split_keys: Vec<Vec<u8>> = (0..=SPLIT_REGION_MAX_KEYS)
            .map(|i| encode_bytes(&i_to_key(i)))
            .collect();
        let (region_ids, percent) =
            block_on(pd_client.split_regions_opt(split_keys, Duration::from_secs(3), 1)).unwrap();
        assert!(region_ids.is_empty());
        assert_eq!(percent, 0);
    }

    {
        let split_keys: Vec<Vec<u8>> = (0..SPLIT_REGION_MAX_KEYS)
            .map(|i| encode_bytes(&i_to_key(i)))
            .collect();
        let region_ids =
            block_on(pd_client.split_regions_with_retry(split_keys, Duration::from_secs(60)))
                .unwrap();
        assert!(!region_ids.is_empty());
    }

    cluster.stop();
}
