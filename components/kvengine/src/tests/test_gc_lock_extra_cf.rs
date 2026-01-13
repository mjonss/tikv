// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, thread, time::Duration};

use bytes::Bytes;
use txn_types::{Lock, TimeStamp};

use crate::{
    EXTRA_CF, LOCK_CF, WriteBatch, dfs,
    shard::{ShardCf, ShardCfBuilder, ShardDataBuilder},
    table,
    table::{
        BIT_DELETE, ChecksumType, SnapVersion,
        file::InMemFile,
        sstable::{BlockCache, SsTable},
    },
    tests::{DEF_BLOCK_SIZE, TABLE_KEY_PREFIX, TestEngine, new_table, new_test_engine_opt},
};

#[test]
fn test_gc_lock_extra_cf() {
    ::test_util::init_log_for_test();
    let (engine, _) = new_test_engine_opt(true, DEF_BLOCK_SIZE, TABLE_KEY_PREFIX);
    let shard = engine.get_shard(1).unwrap();

    let physical_now = TimeStamp::physical_now();
    let gc_safepoint = TimeStamp::compose(physical_now - 50000, 0);
    let lock_ts = TimeStamp::compose(physical_now - 80000, 0);
    let extra_ts = TimeStamp::compose(physical_now - 60000, 0);

    let mut lock_cf_builder = ShardCfBuilder::new(1);
    lock_cf_builder.add_table(
        new_lock_cf_table(&engine, 11, 0, 100, lock_ts.into_inner(), false),
        2,
    );

    let mut saved_vals = Vec::new();
    let mut extra_cf_builder = ShardCfBuilder::new(2);
    extra_cf_builder.add_table(
        new_table(
            &engine,
            12,
            0,
            100,
            extra_ts.into_inner(),
            false,
            &mut saved_vals,
        ),
        1,
    );

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_cfs([
        ShardCf::new(0),
        lock_cf_builder.build(),
        extra_cf_builder.build(),
    ]);
    builder.set_persisted_version(SnapVersion::new(100, 100));
    shard.set_data(builder.build());
    let mut wb = WriteBatch::new(1);
    wb.put(LOCK_CF, b"t_abc", b"t_def", 0, &[], 0);
    engine.write(&mut wb, &[]);
    shard.refresh_estimated_size_and_entries();

    // before update gc safe point, the lock and extra should be kept.
    let safe_ts = engine.get_keyspace_gc_safepoint_v2(0);
    assert!(!shard.check_need_gc_tombstones(safe_ts, true));

    // update gc safe point, the lock file should be in pending gc state, the extra
    // cf should be removed.
    engine.update_managed_safe_ts(gc_safepoint.into_inner());
    let safe_ts = engine.get_managed_safe_ts(0);
    let stats = shard.get_stats();
    assert_eq!(stats.cfs[EXTRA_CF].levels[0].entries, 100);
    assert_eq!(stats.cfs[LOCK_CF].levels[1].entries, 100);
    assert!(shard.check_need_gc_tombstones(safe_ts, true));
    engine.trigger_compact(shard.id_ver());
    thread::sleep(Duration::from_secs(1));
    let stats = shard.get_stats();
    // extra cf is removed
    assert_eq!(stats.cfs[EXTRA_CF].levels[0].entries, 0);
    // lock cf is kept.
    assert_eq!(stats.cfs[LOCK_CF].levels[1].entries, 100);

    // check again, lock cf doesn't trigger gc.
    assert!(shard.check_need_gc_tombstones(safe_ts, true));
    engine.trigger_compact(shard.id_ver());
    thread::sleep(Duration::from_secs(1));
    let stats = shard.get_stats();
    // lock cf is kept.
    assert_eq!(stats.cfs[LOCK_CF].levels[1].entries, 100);

    // update the persisted version, then the lock cf can be gc.
    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_persisted_version(SnapVersion::new(100, 103));
    shard.set_data(builder.build());
    shard.refresh_estimated_size_and_entries();
    assert!(shard.check_need_gc_tombstones(safe_ts, true));
    engine.trigger_compact(shard.id_ver());
    thread::sleep(Duration::from_secs(1));
    let stats = shard.get_stats();
    // lock cf is removed.
    assert_eq!(stats.cfs[LOCK_CF].levels[1].entries, 0);
}

fn new_lock_cf_table(
    engine: &TestEngine,
    id: u64,
    begin: usize,
    end: usize,
    version: u64,
    del: bool,
) -> SsTable {
    let block_size = engine.opts.table_builder_options.block_size;
    let comp_tp = engine.opts.table_builder_options.compression_tps[0];
    let comp_lvl = engine.opts.table_builder_options.compression_lvl;

    let mut builder = table::sstable::builder::Builder::new(
        id,
        block_size,
        comp_tp,
        comp_lvl,
        ChecksumType::Crc32,
        None,
    );
    builder.set_is_lock_cf();
    for i in begin..end {
        let key = engine.key_builder.i_to_inner_key(i);
        let val = if del {
            table::Value::new_with_meta_version(BIT_DELETE, version, 0, &[])
        } else {
            let lock = Lock::new(
                txn_types::LockType::Put,
                key.to_vec(),
                version.into(),
                3,
                Some(key.to_vec()),
                version.into(),
                100,
                (version + 1).into(),
            );
            let value = lock.to_bytes();
            table::Value::new_with_meta_version(0, version, 0, &value)
        };
        builder.add(key.as_ref(), &val, None);
    }
    let mut data_buf = Vec::new();
    builder.finish(0, &mut data_buf);
    let data = Bytes::from(data_buf);
    let opts = dfs::Options::default();
    let fs = engine.fs.as_ref();
    let data1 = data.clone();
    let runtime = fs.get_runtime();
    runtime.block_on(fs.create(id, data1, opts)).unwrap();
    let file = InMemFile::new(id, data);
    SsTable::new(Arc::new(file), BlockCache::None, None).unwrap()
}
