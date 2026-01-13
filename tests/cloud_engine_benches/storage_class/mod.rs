// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

mod util;

use std::time::Duration;

use criterion::{Bencher, Criterion, black_box};
use futures::executor::block_on;
use kvengine::{SnapAccess, WRITE_CF};
use pd_client::PdClient;
use rfstore::store::RegionSnapshot;
use test_cloud_server::{ServerCluster, alloc_node_id, must_wait};
use tikv::storage::{CloudStore, Scanner, Store as _};
use tikv_util::{config::ReadableSize, info};
use txn_types::{Key, TsSet};

use crate::util::*;

const DATA_COUNT: usize = 1 << 16; // 64k
const VALUE_SIZE: usize = 1 << 12; // 4k

fn main() {
    test_util::init_log_for_test();

    let node_id = alloc_node_id();
    let cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.coprocessor.region_split_size = ReadableSize::gb(1); // To prevent split.

        // Use configs the same as prod env.
        conf.rocksdb.writecf.write_buffer_size = ReadableSize::mb(16);
        conf.rocksdb.writecf.block_size = ReadableSize::kb(32);
        conf.rocksdb.writecf.target_file_size_base = ReadableSize::mb(16);
    });
    let mut client = cluster.new_client();

    let gen_val = |_| random_value(VALUE_SIZE);
    let batch_size = DATA_COUNT >> 8;
    for i in (0..DATA_COUNT).step_by(batch_size) {
        client.put_kv(i..i + batch_size, i_to_key, gen_val);
    }

    let region_id = client.get_region_id(KEY_PREFIX.as_bytes());
    let engine = cluster.get_kvengine(node_id);

    must_wait(
        || {
            let stats = engine.get_shard_stat(region_id);
            let ok = stats.compaction_score < 1.0
                && stats.cfs[WRITE_CF]
                    .levels
                    .iter()
                    .any(|lv| lv.num_tables > 0);
            if ok {
                info!("shard stats"; "stats" => ?stats);
            }
            ok
        },
        10,
        || {
            let stats = engine.get_shard_stat(region_id);
            format!(
                "shard is not ready, compaction_score: {}, write_cf levels: {:?}",
                stats.compaction_score, stats.cfs[WRITE_CF].levels
            )
        },
    );

    let snap = engine.get_snap_access(region_id).unwrap();
    let read_ts = block_on(cluster.get_pd_client().get_tso())
        .unwrap()
        .into_inner();

    let mut criterion = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .configure_from_args();
    bench_cloud_store_async(&mut criterion, snap, read_ts);
    criterion.final_summary();
}

fn cloud_store_async_scan(b: &mut Bencher<'_>, options: &(usize, bool, SnapAccess, u64)) {
    let (data_count, use_async, snap, read_ts) = (options.0, options.1, &options.2, options.3);
    let upper_bound = Key::from_raw(&i_to_key(data_count));

    b.iter(|| {
        let mut scanner = new_scanner(snap, false, read_ts, None, Some(upper_bound.clone()));
        if use_async {
            block_on(async move {
                let mut cnt = 0;
                while let Some((k, v)) = scanner.next_async().await.unwrap() {
                    black_box((k, v));
                    cnt += 1;
                }
                assert_eq!(cnt, data_count);
            });
        } else {
            let mut cnt = 0;
            while let Some((k, v)) = scanner.next().unwrap() {
                black_box((k, v));
                cnt += 1;
            }
            assert_eq!(cnt, data_count);
        }
    })
}

fn cloud_store_async_get(b: &mut Bencher<'_>, options: &(usize, bool, SnapAccess, u64)) {
    let (data_count, use_async, snap, read_ts) = (options.0, options.1, &options.2, options.3);

    b.iter(|| {
        let snapshot = RegionSnapshot::from_snapshot(snap.clone(), None);
        let mut store = CloudStore::new(snapshot, read_ts, TsSet::Empty, true);
        if use_async {
            block_on(async move {
                for i in 0..data_count {
                    let key = Key::from_raw(&i_to_key(i));
                    let val = store.incremental_get_async(&key).await.unwrap();
                    black_box(val);
                }
            })
        } else {
            for i in 0..data_count {
                let key = Key::from_raw(&i_to_key(i));
                let val = store.incremental_get(&key).unwrap();
                black_box(val);
            }
        }
    });
}

fn bench_cloud_store_async(c: &mut Criterion, snap: SnapAccess, read_ts: u64) {
    let mut group = c.benchmark_group("cloud_store_async");

    for data_count in [10, 100, 1000, 10000, DATA_COUNT] {
        for use_async in [false, true] {
            group.bench_with_input(
                format!("cloud_store_async_get/{}/async:{}", data_count, use_async),
                &(data_count, use_async, snap.clone(), read_ts),
                cloud_store_async_get,
            );
        }
    }

    for data_count in [10, 100, 1000, 10000, DATA_COUNT] {
        for use_async in [false, true] {
            group.bench_with_input(
                format!("cloud_store_async_scan/{}/async:{}", data_count, use_async),
                &(data_count, use_async, snap.clone(), read_ts),
                cloud_store_async_scan,
            );
        }
    }

    group.finish();
}
