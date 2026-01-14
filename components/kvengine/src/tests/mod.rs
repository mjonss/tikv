// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod test_columnar;
mod test_fts;
mod test_gc_lock_extra_cf;
mod test_ia_auto_file;
mod test_ia_file;
mod test_storage_class;
mod test_txn_file;

use std::{
    env,
    iter::Iterator,
    ops::Deref,
    path::Path,
    rc::Rc,
    sync::{Arc, atomic::AtomicU64},
    thread,
    time::Duration,
    vec,
};

use api_version::{ApiV2, api_v2::KEYSPACE_PREFIX_LEN};
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use cloud_encryption::{EncryptionKey, MasterKey};
use file_system::IoRateLimiter;
use kvenginepb as pb;
use rand::prelude::*;
use rstest::rstest;
use security::SecurityManager;
use tempfile::TempDir;
use tikv_util::{mpsc, time::Instant};
use util::test_util::KeyBuilder;

use crate::{
    dfs::InMemFs,
    ia::manager::{IaManager, IaManagerOptions},
    limiter::StoreLimiter,
    table::{
        BIT_DELETE, BoundedDataSet, ChecksumType, DataBound, InnerKey, OP_PUT,
        file::{File, InMemFile},
        sstable::{BlockCache, L0Builder, L0Table, SsTable},
    },
    tests::test_txn_file::{build_txn_chunk, make_lock_prefix, make_txn_file_refs},
    *,
};

macro_rules! unwrap_or_return {
    ($e:expr, $m:expr) => {
        match $e {
            Ok(x) => x,
            Err(y) => {
                error!("{:?} {:?}", y, $m);
                return;
            }
        }
    };
}

const KEYSPACE_ID: u32 = 1;

const DEF_BLOCK_SIZE: usize = 4 << 10;
const DEF_MIN_BLOB_SIZE: u32 = 64;

const TABLE_KEY_PREFIX: &str = "t_";

/// Wrap `Engine` to make sure that it will be closed after the test, and not
/// interfere with other tests.
pub(crate) struct TestEngine {
    engine: Engine,
    key_builder: KeyBuilder,
}

impl std::ops::Deref for TestEngine {
    type Target = Engine;

    fn deref(&self) -> &Self::Target {
        &self.engine
    }
}

impl Drop for TestEngine {
    fn drop(&mut self) {
        self.engine.close();
    }
}

impl TestEngine {
    fn key_builder(&self) -> &KeyBuilder {
        &self.key_builder
    }

    fn new_ia_manager(&self, opts: IaManagerOptions) -> IaManager {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("ia-mgr")
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        IaManager::new(opts, self.fs.clone(), None, runtime.into()).unwrap()
    }
}

pub(crate) fn new_test_engine() -> (TestEngine, mpsc::Sender<ApplyTask>) {
    new_test_engine_opt(false, DEF_BLOCK_SIZE, "")
}

pub(crate) fn new_test_engine_opt(
    enable_inner_key_off: bool,
    block_size: usize,
    key_prefix: &str,
) -> (TestEngine, mpsc::Sender<ApplyTask>) {
    let tester = EngineTester::new(enable_inner_key_off, block_size);
    build_test_engine_from_tester(tester, enable_inner_key_off, key_prefix)
}

fn new_test_engine_opt_with_custom_options(
    enable_inner_key_off: bool,
    block_size: usize,
    key_prefix: &str,
    customize: impl FnOnce(&mut Options),
) -> (TestEngine, mpsc::Sender<ApplyTask>) {
    let tester = EngineTester::new_with_custom_options(enable_inner_key_off, block_size, customize);
    build_test_engine_from_tester(tester, enable_inner_key_off, key_prefix)
}

fn build_test_engine_from_tester(
    tester: EngineTester,
    enable_inner_key_off: bool,
    key_prefix: &str,
) -> (TestEngine, mpsc::Sender<ApplyTask>) {
    let (listener_tx, listener_rx) = mpsc::unbounded();
    let meta_change_listener = Box::new(TestMetaChangeListener {
        sender: listener_tx,
    });
    let rate_limiter = Arc::new(IoRateLimiter::new_for_test());
    let store_limiter = Arc::new(StoreLimiter::dummy());
    let mut meta_iter = tester.clone();
    let engine = Engine::open(
        tester.fs.clone(),
        tester.opts.clone(),
        tester.config.clone(),
        &mut meta_iter,
        tester.clone(),
        tester.core.clone(),
        meta_change_listener,
        rate_limiter,
        store_limiter,
        None,
        MasterKey::new(&[1u8; 32]),
        Arc::new(SecurityManager::default()),
        true,
    )
    .unwrap();
    {
        let shard = engine.get_shard(1).unwrap();
        store_bool(&shard.active, true);
    }
    let (applier_tx, applier_rx) = mpsc::unbounded();
    let meta_listener = MetaListener::new(listener_rx, applier_tx.clone());
    thread::spawn(move || {
        meta_listener.run();
    });
    let applier = Applier::new(engine.clone(), applier_rx);
    thread::spawn(move || {
        applier.run();
    });
    let keyspace_id = if enable_inner_key_off { 1 } else { 0 };
    let key_builder = KeyBuilder::new(keyspace_id, key_prefix);
    (
        TestEngine {
            engine,
            key_builder,
        },
        applier_tx,
    )
}

// NOTE: In async test, access underlying file with async methods is not
// checked, as `load_data` can only generate tables with `LocalFile`.
// TODO: generate sstables with `IaFile`.
#[maybe_async::test]
async fn test_engine() {
    ::test_util::init_log_for_test();
    let (engine, applier_tx) = new_test_engine();
    // FIXME(youjiali1995): split has bugs.
    //
    // let mut keys = vec![];
    // for i in &[1000, 3000, 6000, 9000] {
    // keys.push(i_to_key(*i,
    // engine.opts.blob_table_build_options.min_blob_size)); }
    // let mut splitter = Splitter::new(keys.clone(), applier_tx.clone());
    // let handle = thread::spawn(move || {
    // splitter.run();
    // });
    let (begin, end) = (0, 1000);
    load_data(
        begin,
        end,
        1,
        applier_tx,
        engine.opts.blob_table_build_options.min_blob_size,
    );
    // handle.join().unwrap();

    wait_for_compact_to_level_1_plus(&engine, begin, Duration::from_secs(10));

    check_get(
        begin,
        end,
        2,
        &[0, 1, 2],
        &engine,
        true,
        None,
        engine.opts.blob_table_build_options.min_blob_size,
    )
    .await;
    check_iterator(begin, end, &engine).await;
}

#[test]
fn test_destroy_range() {
    ::test_util::init_log_for_test();
    let (engine, applier_tx) = new_test_engine();
    let mem_table_count = engine.get_shard_stat(1).mem_table_count;
    load_data(
        10,
        50,
        1,
        applier_tx.clone(),
        engine.opts.blob_table_build_options.min_blob_size,
    );
    // Unsafe destroy keys [10, 30).
    for prefix in [10, 20] {
        let mut wb = WriteBatch::new(1);
        let key = i_to_key(prefix, engine.opts.blob_table_build_options.min_blob_size);
        wb.set_property(DEL_PREFIXES_KEY, &key.as_bytes()[..key.len() - 1]);
        write_data(wb, &applier_tx);
    }
    assert!(!engine.get_shard(1).unwrap().get_del_prefixes().is_empty());
    // Memtable is switched because it contains data covered by the delete-prefixes.
    let stats = engine.get_shard_stat(1);
    assert_eq!(
        stats.mem_table_count + stats.l0_table_count + stats.blob_table_count,
        mem_table_count + 1
    );
    let wait_for_destroying_range = || {
        for _ in 0..30 {
            let shard = engine.get_shard(1).unwrap();
            let data = shard.get_data();
            let del_prefixes = shard.get_del_prefixes();
            if del_prefixes.is_empty() {
                break;
            }
            if Shard::ready_to_destroy_range(&del_prefixes, &data) {
                engine.trigger_compact(shard.id_ver());
            }
            thread::sleep(Duration::from_millis(100));
        }
        let shard = engine.get_shard(1).unwrap();
        let length = shard
            .get_property(DEL_PREFIXES_KEY)
            .map(|v| v.len())
            .unwrap_or_default();
        // Delete-prefixes is cleaned up.
        assert_eq!(length, 0);
    };
    wait_for_destroying_range();
    // After destroying range, key [10, 30) should be removed.
    check_get(
        10,
        30,
        2,
        &[0, 1, 2],
        &engine,
        false,
        None,
        engine.opts.blob_table_build_options.min_blob_size,
    );
    check_get(
        30,
        50,
        2,
        &[0, 1, 2],
        &engine,
        true,
        None,
        engine.opts.blob_table_build_options.min_blob_size,
    );
    check_iterator(30, 50, &engine);

    // Trigger L0 compaction.
    for i in 1..=10 {
        load_data(
            50 + (i - 1) * 10,
            50 + i * 10,
            1,
            applier_tx.clone(),
            engine.opts.blob_table_build_options.min_blob_size,
        );
        let mut wb = WriteBatch::new(1);
        wb.set_switch_mem_table();
        write_data(wb, &applier_tx);
    }
    // Waiting for L0 compaction.
    for _ in 0..30 {
        if engine.get_shard_stat(1).l0_table_count == 0 {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(engine.get_shard_stat(1).l0_table_count < 10);
    // Unsafe destroy keys [100, 150).
    let mut wb = WriteBatch::new(1);
    let key = i_to_key(100, engine.opts.blob_table_build_options.min_blob_size);
    wb.set_property(DEL_PREFIXES_KEY, &key.as_bytes()[..key.len() - 2]);
    write_data(wb, &applier_tx);
    wait_for_destroying_range();
    check_get(
        100,
        150,
        2,
        &[0, 1, 2],
        &engine,
        false,
        None,
        engine.opts.blob_table_build_options.min_blob_size,
    );
    check_get(
        50,
        100,
        2,
        &[0, 1, 2],
        &engine,
        true,
        None,
        engine.opts.blob_table_build_options.min_blob_size,
    );

    // Clean all data.
    let mut wb = WriteBatch::new(1);
    wb.set_property(DEL_PREFIXES_KEY, b"key");
    write_data(wb, &applier_tx);
    wait_for_destroying_range();
    check_get(
        10,
        150,
        2,
        &[0, 1, 2],
        &engine,
        false,
        None,
        engine.opts.blob_table_build_options.min_blob_size,
    );

    // No data exists and delete-prefixes can be cleaned too.
    let mut wb = WriteBatch::new(1);
    wb.set_property(DEL_PREFIXES_KEY, b"key");
    write_data(wb, &applier_tx);
    wait_for_destroying_range();
}

// This test case construct a LSM tree and trigger a particular compaction to
// verify deleted entry will not reappear after the compaction.
// In the LSM true, L3 contains data, L2 contains tombstone, L1 contains data.
// The range for overlap check should use both L1 and L2 instead of only L1.
// The tombstone entries will be discarded when the compaction is
// non-overlapping. If only use L1's range for overlap check, some of the L2's
// tombstone will be lost, cause deleted entries reappear.
#[test]
fn test_lost_tombstone_issue() {
    ::test_util::init_log_for_test();
    let (engine, _) = new_test_engine();
    let shard = engine.get_shard(1).unwrap();

    let mut cf_builder = ShardCfBuilder::new(0);
    let mut saved_vals: Vec<Rc<Vec<u8>>> = Vec::new();

    cf_builder.add_table(
        new_table(&engine, 11, 0, 100, 101, false, &mut saved_vals),
        3,
    );
    cf_builder.add_table(
        new_table(&engine, 12, 50, 150, 102, true, &mut saved_vals),
        2,
    );
    cf_builder.add_table(
        new_table(&engine, 13, 120, 200, 103, false, &mut saved_vals),
        1,
    );
    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_cfs([cf_builder.build(), ShardCf::new(1), ShardCf::new(2)]);
    shard.set_data(builder.build());
    let pri = CompactionPriority::L1Plus {
        cf: 0,
        score: 2.0,
        level: 1,
    };
    let mut guard = shard.compaction_priority.write().unwrap();
    *guard = Some(pri);
    drop(guard);
    engine.update_managed_safe_ts(104);
    engine.trigger_compact(IdVer::new(1, 1));
    thread::sleep(Duration::from_secs(1));
    check_get(50, 100, 104, &[0], &engine, false, None, 0);
}

// This test case construct a three level LSM tree and with different version
// and add tombstone with latest version. Iterator should return all versions of
// the key, excluding the tombstoned key.
#[maybe_async::test]
async fn test_read_iterator_all_versions() {
    ::test_util::init_log_for_test();
    let (engine, _) = new_test_engine();
    let shard = engine.get_shard(1).unwrap();
    let cf = 0;

    let mut cf_builder = ShardCfBuilder::new(cf);
    let mut saved_vals: Vec<Rc<Vec<u8>>> = Vec::new();

    // 0..50 has only version 101
    // 50..70 has version 101 and 102
    // 70..90 has version 103 marked deleted, and older version 101 and 102
    // 90..100 has version 101 and 102
    // 100..150 has version 102
    cf_builder.add_table(
        new_table(&engine, 11, 0, 100, 101, false, &mut saved_vals).await,
        3,
    );
    cf_builder.add_table(
        new_table(&engine, 12, 50, 150, 102, false, &mut saved_vals).await,
        2,
    );
    cf_builder.add_table(
        new_table(&engine, 13, 70, 90, 103, true, &mut saved_vals).await,
        1,
    );

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_cfs([cf_builder.build(), ShardCf::new(1), ShardCf::new(2)]);
    shard.set_data(builder.build());

    let snap = SnapAccess::new(&shard);
    let mut iter = snap.new_iterator(cf, false, true, None, false).await;
    iter.seek(shard.outer_start.chunk()).await;

    let mut expected_keys = Vec::with_capacity(160);
    for i in 0..50 {
        expected_keys.push(i);
    }
    for i in 50..70 {
        expected_keys.push(i);
        expected_keys.push(i);
    }

    // 70..90 should not returned

    for i in 90..100 {
        expected_keys.push(i);
        expected_keys.push(i);
    }
    for i in 100..150 {
        expected_keys.push(i);
    }
    let mut expected_keys_iter = expected_keys.iter();

    while iter.valid() {
        let key = engine
            .key_builder
            .i_to_outer_key(expected_keys_iter.next().unwrap().to_owned());
        assert_eq!(iter.key(), key.as_slice());
        assert_eq!(iter.val(), key.repeat(2).as_slice());
        iter.next().await;
    }
}

#[test]
fn test_new_delta_write_iterator_filters_mixed_tables() {
    ::test_util::init_log_for_test();
    let (engine, _) = new_test_engine();
    let shard = engine.get_shard(1).unwrap();

    let since_ts = 150;

    let mut saved_vals: Vec<Rc<Vec<u8>>> = Vec::new();
    let mut write_cf_builder = ShardCfBuilder::new(WRITE_CF);
    write_cf_builder.add_table(new_table(&engine, 11, 0, 5, 120, false, &mut saved_vals), 1);
    write_cf_builder.add_table(
        new_table(&engine, 12, 5, 10, 200, false, &mut saved_vals),
        1,
    );
    write_cf_builder.add_table(
        new_table(&engine, 13, 10, 15, 250, false, &mut saved_vals),
        1,
    );
    write_cf_builder.add_table(
        new_table_ext(
            &engine,
            14,
            15,
            20,
            |i| if i < 18 { 140 } else { 160 },
            false,
            &mut saved_vals,
        ),
        1,
    );
    write_cf_builder.add_table(
        new_table(&engine, 15, 20, 25, 210, false, &mut saved_vals),
        1,
    );
    write_cf_builder.add_table(
        new_table(&engine, 16, 20, 30, 150, false, &mut saved_vals),
        2,
    );

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_cfs([write_cf_builder.build(), ShardCf::new(1), ShardCf::new(2)]);
    shard.set_data(builder.build());

    let snap = SnapAccess::new(&shard);
    let mut iter = snap.new_delta_write_iterator(since_ts);
    iter.rewind();

    let mut expected = 5;
    while iter.valid() {
        let expected_key = engine.key_builder.i_to_inner_key(expected);
        assert_eq!(iter.key(), expected_key.as_ref());
        let expected_version = if expected < 10 {
            200
        } else if expected < 15 {
            250
        } else if expected < 18 {
            140 // caller should filter out this
        } else if expected < 20 {
            160
        } else {
            210
        };
        assert_eq!(iter.value().version, expected_version);
        expected += 1;
        iter.next();
    }
    assert_eq!(expected, 25);
}

#[test]
fn test_lock_cf_repeatable_read() {
    let enable_inner_key_off = true;

    ::test_util::init_log_for_test();
    let (engine, applier_tx) =
        new_test_engine_opt(enable_inner_key_off, DEF_BLOCK_SIZE, TABLE_KEY_PREFIX);
    let kb = engine.key_builder();
    let shard = engine.get_shard(1).unwrap();

    let verify_locks = |snap: &SnapAccess, start: usize, end: usize, version: u64, del: bool| {
        let tag = format!("{start}->{end}, {version}, {del}");
        let mut it = snap.new_iterator(LOCK_CF, false, false, None, false);
        it.seek(&kb.i_to_outer_key(start));

        if del {
            assert!(
                !it.valid() || it.key() >= kb.i_to_outer_key(end).as_slice(),
                "{}",
                tag
            );
            return;
        }

        for i in start..end {
            assert!(it.valid(), "{}", tag);
            assert_eq!(it.key(), kb.i_to_outer_key(i).as_slice(), "{}", tag);
            assert_eq!(it.version(), version, "{}", tag);
            assert_eq!(table::is_deleted(it.meta()), del, "{}", tag);
            it.next();
        }
    };

    // Prepare level data.
    {
        let mut cf_builder = ShardCfBuilder::new(LOCK_CF);
        let mut saved_vals: Vec<Rc<Vec<u8>>> = Vec::new();
        cf_builder.add_table(
            new_table(&engine, 1000, 100, 300, 1000, false, &mut saved_vals),
            2,
        );
        cf_builder.add_table(
            new_table(&engine, 2000, 110, 300, 2000, true, &mut saved_vals),
            1,
        );

        let l0_file3 = new_l0table_file(
            &engine,
            3000,
            [0, 120, 0],
            [0, 300, 0],
            3000,
            [false, false, false],
        );
        let l0_tbl3 = L0Table::new(l0_file3, BlockCache::None, false, None)
            .unwrap()
            .unwrap();
        let l0_file4 = new_l0table_file(
            &engine,
            4000,
            [0, 130, 0],
            [0, 300, 0],
            4000,
            [false, true, false],
        );
        let l0_tbl4 = L0Table::new(l0_file4, BlockCache::None, false, None)
            .unwrap()
            .unwrap();

        let mut cfs = shard.get_data().cfs.clone();
        cfs[LOCK_CF] = cf_builder.build();
        let mut builder = ShardDataBuilder::new(shard.get_data());
        builder.set_l0_tbls(vec![l0_tbl4, l0_tbl3]);
        builder.set_cfs(cfs);
        shard.set_data(builder.build());
        shard.set_base_version(10000);
    }

    let base_ver = shard.get_base_version();

    // Write mem table.
    let mem_tbl_ver5 = load_data_ext(
        &engine,
        [0, 140, 0],
        [0, 300, 0],
        5000, // useless
        [false, false, false],
        &applier_tx,
    ) + base_ver;

    // Write txn file lock.
    let txn_file_ver6 = {
        let chunk_id = 6000;
        build_txn_chunk(&engine, 150, 300, chunk_id, |_| OP_PUT, None);
        let primary = kb.i_to_outer_key(150);
        let mut wb = WriteBatch::new(1);
        let txn_file_refs = make_txn_file_refs(
            6000, // useless
            vec![chunk_id],
            make_lock_prefix(primary.clone(), 6000),
            vec![],
        );
        wb.set_property(TXN_FILE_REF, &txn_file_refs);
        engine.txn_chunk_mgr.prepare(chunk_id, None).unwrap();
        write_data(wb, &applier_tx)
    } + base_ver;

    let snap = shard.new_snap_access();
    print_locks(&snap, false, None);
    verify_locks(&snap, 100, 110, 1000, false);
    verify_locks(&snap, 110, 120, 2000, true);
    verify_locks(&snap, 120, 130, 3000, false);
    verify_locks(&snap, 130, 140, 4000, true);
    verify_locks(&snap, 140, 150, mem_tbl_ver5, false);
    verify_locks(
        &snap,
        150,
        300,
        snap.get_mem_table_snap_version().into_inner(),
        false,
    );

    // Write more mem table data.
    {
        switch_mem_table(&applier_tx);
        load_data_ext(
            &engine,
            [0, 160, 0],
            [0, 300, 0],
            7000, // useless
            [false, false, false],
            &applier_tx,
        );
    }

    // Verify repeatable read.
    verify_locks(&snap, 140, 150, mem_tbl_ver5, false);
    verify_locks(
        &snap,
        150,
        300,
        snap.get_mem_table_snap_version().into_inner(),
        false,
    );

    // Check latest snapshot.
    let snap = shard.new_snap_access();
    verify_locks(&snap, 140, 150, mem_tbl_ver5, false);
    verify_locks(&snap, 150, 160, txn_file_ver6, false);
    verify_locks(
        &snap,
        160,
        300,
        snap.get_mem_table_snap_version().into_inner(),
        false,
    );
}

#[rstest]
#[case::enable_key_off(true)]
#[case::disable_key_off(false)]
fn test_level_overlapping_tables(#[case] enable_inner_key_off: bool) {
    ::test_util::init_log_for_test();
    let (engine, _) = new_test_engine_opt(enable_inner_key_off, DEF_BLOCK_SIZE, "");
    let shard = engine.get_shard(1).unwrap();

    let mut cf_builder = ShardCfBuilder::new(0);
    let mut saved_vals: Vec<Rc<Vec<u8>>> = Vec::new();

    let table_ranges: Vec<(usize, usize)> = vec![(10, 20), (50, 100), (120, 200)];
    for (i, (start, end)) in table_ranges.into_iter().enumerate() {
        cf_builder.add_table(
            new_table(
                &engine,
                i as u64,
                start,
                end,
                i as u64 + 100,
                false,
                &mut saved_vals,
            ),
            1, // level
        );
    }
    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_cfs([cf_builder.build(), ShardCf::new(1), ShardCf::new(2)]);
    let data = builder.build();

    let cf0 = data.get_cf(0);
    let level1 = cf0.get_level(1);
    let get_overlapping_tables = |start: usize, end: usize| -> (usize, usize) {
        let inner_start = engine.key_builder.i_to_inner_key(start);
        let inner_end = engine.key_builder.i_to_inner_key(end);
        let bound = DataBound::new(inner_start.as_ref(), inner_end.as_ref(), false);
        bound.get_overlap_data_sets(&level1.tables)
    };

    assert_eq!(get_overlapping_tables(0, 10), (0, 0));
    assert_eq!(get_overlapping_tables(0, 20), (0, 1));
    assert_eq!(get_overlapping_tables(0, 50), (0, 1));
    assert_eq!(get_overlapping_tables(0, 100), (0, 2));
    assert_eq!(get_overlapping_tables(0, 120), (0, 2));
    assert_eq!(get_overlapping_tables(0, 200), (0, 3));
    assert_eq!(get_overlapping_tables(0, 300), (0, 3));

    assert_eq!(get_overlapping_tables(10, 20), (0, 1));
    assert_eq!(get_overlapping_tables(10, 50), (0, 1));
    assert_eq!(get_overlapping_tables(10, 100), (0, 2));
    assert_eq!(get_overlapping_tables(10, 120), (0, 2));
    assert_eq!(get_overlapping_tables(10, 200), (0, 3));
    assert_eq!(get_overlapping_tables(10, 300), (0, 3));

    assert_eq!(get_overlapping_tables(20, 50), (1, 1));
    assert_eq!(get_overlapping_tables(20, 100), (1, 2));
    assert_eq!(get_overlapping_tables(20, 120), (1, 2));
    assert_eq!(get_overlapping_tables(20, 200), (1, 3));
    assert_eq!(get_overlapping_tables(20, 300), (1, 3));

    assert_eq!(get_overlapping_tables(50, 100), (1, 2));
    assert_eq!(get_overlapping_tables(50, 120), (1, 2));
    assert_eq!(get_overlapping_tables(50, 200), (1, 3));
    assert_eq!(get_overlapping_tables(50, 300), (1, 3));

    assert_eq!(get_overlapping_tables(100, 120), (2, 2));
    assert_eq!(get_overlapping_tables(100, 200), (2, 3));
    assert_eq!(get_overlapping_tables(100, 300), (2, 3));

    assert_eq!(get_overlapping_tables(120, 200), (2, 3));
    assert_eq!(get_overlapping_tables(120, 300), (2, 3));

    assert_eq!(get_overlapping_tables(200, 300), (3, 3));
}

#[rstest]
#[case::enable_key_off(true)]
#[case::disable_key_off(false)]
fn test_get_suggest_split_key(#[case] enable_inner_key_off: bool) {
    ::test_util::init_log_for_test();
    let (engine, _) = new_test_engine_opt(enable_inner_key_off, 4096, "");
    let shard = engine.get_shard(1).unwrap();

    let range = ShardRange::new(
        &engine.key_builder.i_to_outer_key(30),
        &engine.key_builder.i_to_outer_key(600),
    );
    let inner_key_off = shard.get_data().inner_key_off;
    let shard = Shard::new(
        shard.engine_id,
        &shard.properties.to_pb(shard.id),
        shard.ver,
        range.clone(),
        inner_key_off,
        engine.opts.clone(),
        &engine.master_key,
    );

    let cases: Vec<(
        Vec<(usize, usize)>,
        Vec<(usize, usize)>, // table ranges
        // expected suggest split key, inner key off (enable,disable)
        Option<usize>,
    )> = vec![
        (vec![], vec![(20, 50)], None),
        (vec![], vec![(20, 50), (100, 150), (150, 180)], Some(150)),
        (
            vec![],
            vec![
                (20, 50),
                (100, 150), // in range
                (150, 180), // in range
                (180, 300), // in range
                (300, 400),
            ],
            Some(180),
        ),
        (
            vec![],
            vec![
                (20, 50),
                (100, 150), // in range, key in block
            ],
            Some(100),
        ),
        (
            vec![(50, 400), (60, 460), (70, 670)], // L0 only
            vec![],
            Some(320),
        ),
        (
            vec![(70, 370)], // L0 + L1+
            vec![(50, 80), (80, 100), (100, 120)],
            Some(100),
        ),
        (
            vec![(80, 380), (190, 570)], // more L0 + L1+
            vec![(50, 80), (80, 100), (100, 120)],
            Some(330),
        ),
    ];
    let mut id_alloc = 1000;
    for (idx, (l0_ranges, table_ranges, expect_split_key)) in cases.into_iter().enumerate() {
        let mut l0_tables = vec![];
        for (start, end) in l0_ranges {
            let id = id_alloc;
            id_alloc += 1;
            let (begins, ends) = if idx % 2 == 0 {
                ([start, 0, 0], [end, 0, 0])
            } else {
                ([0, start, 0], [0, end, 0])
            };
            let l0_file = new_l0table_file(&engine, id, begins, ends, id, [false, false, false]);
            let l0_tbl = L0Table::new(l0_file, BlockCache::None, false, None)
                .unwrap()
                .unwrap();
            l0_tables.push(l0_tbl);
        }
        let mut cf_builder = ShardCfBuilder::new(idx % 2);
        let mut saved_vals: Vec<Rc<Vec<u8>>> = Vec::new();

        for (i, (start, end)) in table_ranges.into_iter().enumerate() {
            cf_builder.add_table(
                new_table(
                    &engine,
                    i as u64 + 1,
                    start,
                    end,
                    100 + i as u64,
                    false,
                    &mut saved_vals,
                ),
                1,
            );
        }
        let mut builder = ShardDataBuilder::new(shard.get_data());
        builder.set_range(range.clone());
        builder.set_l0_tbls(l0_tables);
        builder.set_cfs([cf_builder.build(), ShardCf::new(1), ShardCf::new(2)]);
        shard.set_data_opt(builder.build(), false);

        let key = shard.get_suggest_split_key(1);
        assert_eq!(
            key.map(|k| k.to_vec()),
            expect_split_key.map(|i| engine.key_builder.i_to_outer_key(i)),
            "case {}",
            idx
        );
    }
}

#[rstest]
#[case::enable_key_off(true)]
#[case::disable_key_off(false)]
fn test_get_evenly_split_keys(#[case] enable_inner_key_off: bool) {
    ::test_util::init_log_for_test();
    let (engine, _) = new_test_engine_opt(enable_inner_key_off, 4096, "");
    let shard = engine.get_shard(1).unwrap();

    let range = ShardRange::new(
        &engine.key_builder.i_to_outer_key(30),
        &engine.key_builder.i_to_outer_key(600),
    );
    let inner_key_off = shard.get_data().inner_key_off;
    let shard = Shard::new(
        shard.engine_id,
        &shard.properties.to_pb(shard.id),
        shard.ver,
        range.clone(),
        inner_key_off,
        engine.opts.clone(),
        &engine.master_key,
    );

    let cases: Vec<(
        Vec<(usize, usize)>, // l0 ranges
        Vec<(usize, usize)>, // table ranges
        usize,               // split count
        // expected evenly split keys
        Option<Vec<usize>>,
    )> = vec![
        (vec![], vec![(20, 50), (50, 100)], 1, None),
        (vec![], vec![(20, 50), (50, 100)], 2, None),
        (
            vec![],
            vec![(0, 20), (20, 50), (100, 150), (150, 180)],
            2,
            Some(vec![150]),
        ),
        (
            vec![],
            vec![
                (20, 50),
                (100, 150), // in range
                (150, 180), // in range
                (180, 300), // in range
                (300, 400),
            ],
            1,
            None,
        ),
        (
            vec![],
            vec![
                (20, 50),
                (100, 150), // in range
                (150, 180), // in range
                (180, 300), // in range
            ],
            2,
            Some(vec![180]),
        ),
        (
            vec![],
            vec![
                (100, 120), // in range
                (120, 150), // in range
                (150, 180), // in range
                (180, 300), // in range
            ],
            3,
            Some(vec![150, 180]),
        ),
        (
            vec![],
            vec![
                (20, 50),
                (100, 150), // in range
                (150, 180), // in range
                (180, 300), // in range
                (300, 400),
            ],
            4,
            Some(vec![150, 180, 300]),
        ),
        (
            vec![],
            vec![
                (20, 50),
                (100, 150), // in range
                (150, 180), // in range
                (180, 300), // in range
                (300, 400),
            ],
            5,
            Some(vec![150, 180, 300]),
        ),
        (
            vec![],
            vec![
                (20, 50),
                (100, 150), // in range
                (150, 180), // in range
                (180, 300), // in range
                (300, 400),
            ],
            6,
            Some(vec![150, 180, 300]),
        ),
        (
            vec![(100, 400), (200, 500), (300, 600), (400, 700)], // L0 only
            vec![],
            4,
            Some(vec![450, 550]),
        ),
        (
            vec![(100, 400), (150, 550), (300, 700), (200, 800)], // L0 only
            vec![],
            4,
            Some(vec![400, 450, 550]),
        ),
        (
            vec![(100, 800), (200, 900)], // L0 + L1+
            vec![(30, 80), (80, 100), (100, 130)],
            4,
            Some(vec![100, 350, 450]),
        ),
    ];
    let mut id_alloc = 1000;
    for (idx, (l0_ranges, table_ranges, split_count, expect_split_keys)) in
        cases.into_iter().enumerate()
    {
        let mut l0_tables = vec![];
        for (start, end) in l0_ranges {
            let id = id_alloc;
            id_alloc += 1;
            let (begins, ends) = if idx % 2 == 0 {
                ([start, 0, 0], [end, 0, 0])
            } else {
                ([0, start, 0], [0, end, 0])
            };
            let l0_file = new_l0table_file(&engine, id, begins, ends, id, [false, false, false]);
            let l0_tbl = L0Table::new(l0_file, BlockCache::None, false, None)
                .unwrap()
                .unwrap();
            l0_tables.push(l0_tbl);
        }
        let mut cf_builder = ShardCfBuilder::new(idx % 2);
        let mut saved_vals: Vec<Rc<Vec<u8>>> = Vec::new();

        for (i, (start, end)) in table_ranges.into_iter().enumerate() {
            cf_builder.add_table(
                new_table(
                    &engine,
                    i as u64 + 1,
                    start,
                    end,
                    100 + i as u64,
                    false,
                    &mut saved_vals,
                ),
                1, // level
            );
        }
        let mut builder = ShardDataBuilder::new(shard.get_data());
        builder.set_range(range.clone());
        builder.set_l0_tbls(l0_tables);
        builder.set_cfs([cf_builder.build(), ShardCf::new(1), ShardCf::new(2)]);
        shard.set_data_opt(builder.build(), false);

        let split_keys = shard.get_evenly_split_keys(split_count, 1);
        assert_eq!(
            split_keys.map(|keys| keys.into_iter().map(|k| k.to_vec()).collect::<Vec<_>>()),
            expect_split_keys.map(|keys| keys
                .into_iter()
                .map(|i| engine.key_builder.i_to_outer_key(i))
                .collect::<Vec<_>>()),
            "case {}",
            idx
        );
    }
}

#[test]
fn test_refresh_stats() {
    ::test_util::init_log_for_test();
    let (engine, _) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let shard = engine.get_shard(1).unwrap();

    let mut saved_vals: Vec<Rc<Vec<u8>>> = Vec::new();

    let mut write_cf_builder = ShardCfBuilder::new(WRITE_CF);
    write_cf_builder.add_table(
        new_table(&engine, 11, 100, 200, 101, true, &mut saved_vals),
        3,
    );
    write_cf_builder.add_table(
        new_table(&engine, 12, 50, 150, 102, false, &mut saved_vals),
        2,
    );
    write_cf_builder.add_table(
        new_table(&engine, 13, 70, 90, 103, true, &mut saved_vals),
        1,
    );

    let mut lock_cf_builder = ShardCfBuilder::new(LOCK_CF);
    lock_cf_builder.add_table(
        new_table(&engine, 21, 50, 150, 1001, false, &mut saved_vals),
        2,
    );
    lock_cf_builder.add_table(
        new_table(&engine, 22, 70, 90, 1002, true, &mut saved_vals),
        1,
    );

    let mut extra_cf_builder = ShardCfBuilder::new(EXTRA_CF);
    extra_cf_builder.add_table(
        new_table(&engine, 31, 50, 150, 150, false, &mut saved_vals),
        1,
    );
    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_cfs([
        write_cf_builder.build(),
        lock_cf_builder.build(),
        extra_cf_builder.build(),
    ]);
    shard.set_data(builder.build());
    shard.refresh_states();

    // size per entry: key 9, value 18
    assert_eq!(shard.get_estimated_entries(), 440); // 100 + 100 + 20 + 100 + 20 + 100
    assert_eq!(load_u64(&shard.sst_max_ts), 150); // max(WRITE_CF, EXTRA_CF). TODO: test with mem tables.
    assert_eq!(shard.get_max_ts(), 150); // max(WRITE_CF, EXTRA_CF)
    assert_eq!(shard.get_estimated_kv_size(), 3780); // 100*27 + 120*9
    assert_eq!(load_u64(&shard.lv2plus_max_ts), 102); // max(WRITE_CF & Level 2+)
    assert_eq!(load_u64(&shard.lv2plus_tombs), 100); // WRITE_CF & Level 2+ only
    assert_eq!(load_u64(&shard.lv2plus_entries_write_cf), 200); // WRITE_CF & Level 2+ only
}

#[test]
fn test_l0table_write_cf_only() {
    ::test_util::init_log_for_test();
    let (engine, _) = new_test_engine();
    let file = new_l0table_file(
        &engine,
        1,
        [0, 0, 0],
        [0, 100, 0],
        100,
        [false, false, false],
    );

    let l0table = L0Table::new(file.clone(), BlockCache::None, false, None).unwrap();
    assert!(l0table.is_some());

    // l0table would be none when `write_cf_only` is true and WRITE_CF is empty.
    // See https://github.com/tidbcloud/cloud-storage-engine/issues/1026.
    let l0table_write_cf_only = L0Table::new(file, BlockCache::None, true, None).unwrap();
    assert!(l0table_write_cf_only.is_none());
}

fn print_locks(snap: &SnapAccess, all_versions: bool, read_ts: Option<u64>) {
    let mut it = snap.new_iterator(LOCK_CF, false, all_versions, read_ts, false);
    it.rewind();
    while it.valid() {
        debug!(
            "key: {:?}, version: {:?}, meta: {:?}",
            tikv_util::escape(it.key()),
            it.version(),
            it.meta()
        );
        it.next();
    }
}

fn keyspace_prefix(keyspace_id: u32) -> Vec<u8> {
    let mut prefix = keyspace_id.to_be_bytes().to_vec();
    prefix[0] = b'x';
    prefix
}

#[inline]
fn encode_i64_to_comparable_u64(v: i64) -> u64 {
    (v as u64) ^ 1 << 63_u64
}

fn prepare_table_region(
    engine: &TestEngine,
    apply_tx: &mpsc::Sender<ApplyTask>,
    keyspace_id: u32,
    table_id: i64,
) -> u64 {
    // Split keyspace
    let keyspace_1_prefix = keyspace_prefix(keyspace_id);
    let mut table_1_prefix = keyspace_1_prefix.clone();
    table_1_prefix.extend_from_slice(b"t");
    table_1_prefix.extend_from_slice(
        encode_i64_to_comparable_u64(table_id)
            .to_be_bytes()
            .as_slice(),
    );
    let mut table_2_prefix = keyspace_1_prefix;
    table_2_prefix.extend_from_slice(b"t");
    table_2_prefix.extend_from_slice(
        encode_i64_to_comparable_u64(table_id + 1)
            .to_be_bytes()
            .as_slice(),
    );
    let split_keys = vec![
        keyspace_prefix(keyspace_id),
        table_1_prefix,
        table_2_prefix,
        keyspace_prefix(keyspace_id + 1),
    ];
    let mut splitter = Splitter::new(split_keys, IdVer::new(1, 1), 1, apply_tx.clone());
    let handle = thread::spawn(move || {
        splitter.run();
    });
    let ok = try_wait(|| engine.shards.len() == 5, 2);
    assert!(ok, "split failed");
    handle.join().unwrap();
    4
}

#[derive(Clone)]
struct TestMetaChangeListener {
    sender: mpsc::Sender<pb::ChangeSet>,
}

impl MetaChangeListener for TestMetaChangeListener {
    fn on_change_set(&self, cs: pb::ChangeSet) {
        info!("on meta change listener");
        self.sender.send(cs).unwrap();
    }
}

#[derive(Clone)]
struct EngineTester {
    core: Arc<EngineTesterCore>,
}

impl Deref for EngineTester {
    type Target = EngineTesterCore;
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl EngineTester {
    fn new(enable_inner_key_off: bool, block_size: usize) -> Self {
        Self::new_with_custom_options(enable_inner_key_off, block_size, |_| {})
    }

    fn new_with_custom_options(
        enable_inner_key_off: bool,
        block_size: usize,
        customize: impl FnOnce(&mut Options),
    ) -> Self {
        let initial_cs = new_initial_cs(enable_inner_key_off);
        let initial_meta = ShardMeta::new(1, &initial_cs);
        let metas = dashmap::DashMap::new();
        metas.insert(1, Arc::new(initial_meta));
        let tmp_dir = TempDir::new().unwrap();
        let mut opts = new_test_options(tmp_dir.path(), enable_inner_key_off, block_size);
        customize(&mut opts);
        let config = KvEngineConfig::default();

        Self {
            core: Arc::new(EngineTesterCore {
                _tmp_dir: tmp_dir,
                metas,
                fs: Arc::new(InMemFs::new()),
                opts: Arc::new(opts),
                config,
                id: AtomicU64::new(0),
            }),
        }
    }
}

struct EngineTesterCore {
    _tmp_dir: TempDir,
    metas: dashmap::DashMap<u64, Arc<ShardMeta>>,
    fs: Arc<dfs::InMemFs>,
    opts: Arc<Options>,
    config: KvEngineConfig,
    id: AtomicU64,
}

impl MetaIterator for EngineTester {
    fn iterate<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(kvenginepb::ChangeSet),
    {
        for meta in &self.metas {
            f(meta.value().to_change_set())
        }
        Ok(())
    }

    fn engine_id(&self) -> u64 {
        1
    }
}

impl RecoverHandler for EngineTester {
    fn recover(
        &self,
        _engine: &Engine,
        _shard: &Arc<Shard>,
        _info: &ShardMeta,
        _is_parent: bool,
    ) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl IdAllocator for EngineTesterCore {
    fn alloc_id(&self, count: usize) -> Result<Vec<u64>> {
        let start_id = self
            .id
            .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed)
            + 1;
        let end_id = start_id + count as u64;
        let mut ids = Vec::with_capacity(count);
        for id in start_id..end_id {
            ids.push(id);
        }
        Ok(ids)
    }

    async fn alloc_id_async(&self, count: usize) -> Result<Vec<u64>> {
        self.alloc_id(count)
    }
}

struct MetaListener {
    applier_tx: mpsc::Sender<ApplyTask>,
    meta_rx: mpsc::Receiver<pb::ChangeSet>,
}

impl MetaListener {
    fn new(meta_rx: mpsc::Receiver<pb::ChangeSet>, applier_tx: mpsc::Sender<ApplyTask>) -> Self {
        Self {
            meta_rx,
            applier_tx,
        }
    }

    fn run(&self) {
        loop {
            let cs = unwrap_or_return!(self.meta_rx.recv(), "meta_listener_a");
            let (tx, rx) = mpsc::bounded(1);
            let task = ApplyTask::new_cs(cs, tx);
            self.applier_tx.send(task).unwrap();
            let res = unwrap_or_return!(rx.recv(), "meta_listener_b");
            unwrap_or_return!(res, "meta_listener_c");
        }
    }
}

struct Applier {
    engine: Engine,
    task_rx: mpsc::Receiver<ApplyTask>,
}

impl Applier {
    fn new(engine: Engine, task_rx: mpsc::Receiver<ApplyTask>) -> Self {
        Self { engine, task_rx }
    }

    fn run(&self) {
        let mut seq = 2;
        loop {
            let mut task = unwrap_or_return!(self.task_rx.recv(), "apply recv task");
            seq += 1;
            if let Some(wb) = task.wb.as_mut() {
                wb.set_sequence(seq);
                self.engine.write(wb, &[]);
            }
            if let Some(mut cs) = task.cs.take() {
                cs.set_sequence(seq);
                if cs.has_split() {
                    let mut ids = vec![];
                    for new_shard in cs.get_split().get_new_shards() {
                        ids.push(new_shard.shard_id);
                    }
                    unwrap_or_return!(self.engine.split(cs, 1), "apply split");
                    for id in ids {
                        let shard = self.engine.get_shard(id).unwrap();
                        shard.set_active(true);
                    }
                    info!("applier executed split");
                } else {
                    self.engine.meta_committed(&cs, false);
                    unwrap_or_return!(
                        self.engine.apply_change_set(
                            &self
                                .engine
                                .prepare_change_set(cs, PrepareOpts::default())
                                .unwrap()
                        ),
                        "applier apply changeset"
                    );
                }
            }
            task.result_tx.send(Ok(seq)).unwrap();
        }
    }
}

pub(crate) struct ApplyTask {
    wb: Option<WriteBatch>,
    cs: Option<pb::ChangeSet>,
    result_tx: mpsc::Sender<Result<u64 /* write_sequence */>>,
}

impl ApplyTask {
    fn new_cs(cs: pb::ChangeSet, result_tx: mpsc::Sender<Result<u64>>) -> Self {
        Self {
            wb: None,
            cs: Some(cs),
            result_tx,
        }
    }

    fn new_wb(wb: WriteBatch, result_tx: mpsc::Sender<Result<u64>>) -> Self {
        Self {
            wb: Some(wb),
            cs: None,
            result_tx,
        }
    }
}

struct Splitter {
    apply_sender: mpsc::Sender<ApplyTask>,
    keys: Vec<Vec<u8>>,
    current_shard_id: u64,
    shard_ver: u64,
    new_id: u64,
}

#[allow(dead_code)]
impl Splitter {
    fn new(
        keys: Vec<Vec<u8>>,
        current_shard_id_ver: IdVer,
        new_id_base: u64,
        apply_sender: mpsc::Sender<ApplyTask>,
    ) -> Self {
        Self {
            keys,
            apply_sender,
            current_shard_id: current_shard_id_ver.id,
            shard_ver: current_shard_id_ver.ver,
            new_id: new_id_base,
        }
    }

    fn run(&mut self) {
        let keys = self.keys.clone();
        for key in keys {
            thread::sleep(Duration::from_millis(200));
            self.new_id += 1;
            self.split(key.clone(), vec![self.new_id, self.current_shard_id]);
        }
    }

    fn send_task(&mut self, cs: pb::ChangeSet) {
        let (tx, rx) = mpsc::bounded(1);
        let task = ApplyTask {
            cs: Some(cs),
            wb: None,
            result_tx: tx,
        };
        self.apply_sender.send(task).unwrap();
        let res = unwrap_or_return!(rx.recv(), "splitter recv");
        res.unwrap();
    }

    fn split(&mut self, key: Vec<u8>, new_ids: Vec<u64>) {
        let mut cs = pb::ChangeSet::new();
        cs.set_shard_id(self.current_shard_id);
        cs.set_shard_ver(self.shard_ver);
        let mut finish_split = pb::Split::new();
        finish_split.set_keys(protobuf::RepeatedField::from_vec(vec![key]));
        let mut new_shards = Vec::new();
        for new_id in &new_ids {
            let mut new_shard = pb::Properties::new();
            new_shard.set_shard_id(*new_id);
            new_shards.push(new_shard);
        }
        finish_split.set_new_shards(protobuf::RepeatedField::from_vec(new_shards));
        cs.set_split(finish_split);
        self.send_task(cs);
        self.shard_ver += 1;
    }
}

fn new_initial_cs(enable_inner_key_off: bool) -> pb::ChangeSet {
    let mut cs = pb::ChangeSet::new();
    cs.set_shard_id(1);
    cs.set_shard_ver(1);
    cs.set_sequence(1);
    let mut snap = pb::Snapshot::new();
    snap.set_base_version(1);
    if enable_inner_key_off {
        let (start, end) = ApiV2::get_txn_keyspace_range(KEYSPACE_ID);
        snap.set_outer_start(start);
        snap.set_outer_end(end);
        snap.set_inner_key_off(KEYSPACE_PREFIX_LEN as u32);
    } else {
        snap.set_outer_end(GLOBAL_SHARD_END_KEY.to_vec());
    }
    let props = snap.mut_properties();
    props.shard_id = 1;
    cs.set_snapshot(snap);
    cs
}

fn new_test_options(
    path: impl AsRef<Path>,
    _enable_inner_key_off: bool,
    block_size: usize,
) -> Options {
    let min_blob_size: u32 = match env::var("MIN_BLOB_SIZE") {
        Ok(val) => match val.trim().parse() {
            Ok(n) => n,
            Err(e) => {
                warn!("MIN_BLOB_SIZE=<number>, got {}", e);
                DEF_MIN_BLOB_SIZE
            }
        },
        Err(_) => DEF_MIN_BLOB_SIZE,
    };
    info!("MIN_BLOB_SIZE={}", min_blob_size);
    let mut opts = Options::default();
    opts.local_dirs = vec![path.as_ref().to_path_buf()];
    opts.base_size = 64 << 10;
    opts.table_builder_options.block_size = block_size;
    opts.table_builder_options.max_table_size = 8 << 10;
    opts.table_builder_options.flush_split_l0 = true;
    opts.columnar_build_options.max_columnar_table_size = 1024;
    opts.columnar_build_options.pack_max_row_count = 9;
    opts.max_mem_table_size = 32 << 10; // mem-table size should be much larger than max_table_size.
    opts.num_compactors = 2;
    opts.blob_table_build_options.min_blob_size = min_blob_size;
    opts.max_del_range_delay = Duration::from_secs(1);
    opts.read_columnar = true;
    opts
}

fn i_to_key(i: i32, min_blob_size: u32) -> String {
    if min_blob_size > 0 {
        // 3 -> strlen("key")
        format!("key{:0>1$}", i, min_blob_size as usize - 3)
    } else {
        format!("key{:0>1$}", i, 6)
    }
}

#[maybe_async::both]
async fn new_table_ext<F>(
    engine: &TestEngine,
    id: u64,
    begin: usize,
    end: usize,
    version: F,
    del: bool,
    saved_vals: &mut Vec<Rc<Vec<u8>>>,
) -> SsTable
where
    F: Fn(usize) -> u64,
{
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
    for i in begin..end {
        let version = version(i);
        let key = engine.key_builder.i_to_inner_key(i);
        let val = if del {
            table::Value::new_with_meta_version(BIT_DELETE, version, 0, &[])
        } else {
            let val = Rc::new(key.as_ref().repeat(2));
            // Save the val with Rc, make sure the val_rc stay valid during the iterator
            // lifecycle.
            saved_vals.push(val.clone());
            table::Value::new_with_meta_version(0, version, 0, &val)
        };
        builder.add(key.as_ref(), &val, None);
    }
    let mut data_buf = Vec::new();
    builder.finish(0, &mut data_buf);
    let data = Bytes::from(data_buf);
    let opts = dfs::Options::default();
    dfs_create_table(engine.fs.as_ref(), id, data.clone(), opts)
        .await
        .unwrap();
    let file = InMemFile::new(id, data).await;
    SsTable::new(Arc::new(file), BlockCache::None, None).unwrap()
}

#[maybe_async::both]
async fn new_table(
    engine: &TestEngine,
    id: u64,
    begin: usize,
    end: usize,
    version: u64,
    del: bool,
    saved_vals: &mut Vec<Rc<Vec<u8>>>,
) -> SsTable {
    new_table_ext(engine, id, begin, end, move |_| version, del, saved_vals).await
}

fn dfs_create_table(
    fs: &dyn dfs::Dfs,
    file_id: u64,
    data: Bytes,
    opts: dfs::Options,
) -> dfs::Result<()> {
    let runtime = fs.get_runtime();
    runtime.block_on(fs.create(file_id, data, opts))
}

async fn dfs_create_table_async(
    dfs: &dyn dfs::Dfs,
    file_id: u64,
    data: Bytes,
    opts: dfs::Options,
) -> dfs::Result<()> {
    dfs.create(file_id, data, opts).await
}

fn new_l0table_file(
    engine: &TestEngine,
    id: u64,
    begin: [usize; NUM_CFS],
    end: [usize; NUM_CFS],
    version: u64,
    del: [bool; NUM_CFS],
) -> Arc<dyn File> {
    let block_size = engine.opts.table_builder_options.block_size;
    let fs = engine.fs.clone();

    let mut builder = L0Builder::new(id, block_size, version.into(), ChecksumType::Crc32, None);
    for cf in 0..NUM_CFS {
        for i in begin[cf]..end[cf] {
            let key = engine.key_builder.i_to_inner_key(i);
            if del[cf] {
                let val = table::Value::new_with_meta_version(BIT_DELETE, version, 0, &[]);
                builder.add(cf, key.as_ref(), &val, None);
            } else {
                let val_buf = key.as_ref().repeat(2);
                let val = table::Value::new_with_meta_version(0, version, 0, &val_buf);
                builder.add(cf, key.as_ref(), &val, None);
            }
        }
    }
    let (_, data) = builder.finish();
    let opts = dfs::Options::default();
    let runtime = fs.get_runtime();
    runtime.block_on(fs.create(id, data.clone(), opts)).unwrap();
    Arc::new(InMemFile::new(id, data))
}

fn load_data(
    begin: usize,
    end: usize,
    version: u64,
    tx: mpsc::Sender<ApplyTask>,
    min_blob_size: u32,
) {
    let mut wb = WriteBatch::new(1);
    for i in begin..end {
        let key = i_to_key(i as i32, min_blob_size);
        for cf in 0..3 {
            let val = key.repeat(cf + 2);
            let version = if cf == 1 { 0 } else { version };
            wb.put(cf, key.as_bytes(), val.as_bytes(), 0, &[], version);
        }
        if i % 100 == 99 {
            info!("load data {}:{}", i - 99, i);
            write_data(wb, &tx);
            wb = WriteBatch::new(1);
            thread::sleep(Duration::from_millis(10));
        }
    }
    if wb.num_entries() > 0 {
        write_data(wb, &tx);
    }
}

fn load_data_ext(
    engine: &TestEngine,
    begin: [usize; NUM_CFS],
    end: [usize; NUM_CFS],
    version: u64,
    del: [bool; NUM_CFS],
    tx: &mpsc::Sender<ApplyTask>,
) -> u64 {
    let mut wb = WriteBatch::new(1);
    for cf in 0..NUM_CFS {
        for i in begin[cf]..end[cf] {
            let key = engine.key_builder.i_to_outer_key(i);
            let version = if cf == 1 { 0 } else { version };
            if del[cf] {
                wb.put(cf, &key, &[], BIT_DELETE, &[], version);
            } else {
                let val = key.repeat(2);
                wb.put(cf, &key, &val, 0, &[], version);
            }
        }
    }
    if wb.num_entries() > 0 {
        write_data(wb, tx)
    } else {
        0
    }
}

fn wait_for_compact_to_level_1_plus(en: &Engine, i: usize, timeout: Duration) {
    let key = i_to_key(i as i32, en.opts.blob_table_build_options.min_blob_size);
    let ok = try_wait(
        || {
            let shard = get_shard_for_key(key.as_bytes(), en);
            let stats = en.get_shard_stat(shard.id);
            stats.compaction_score < 1.0
                && stats.cfs[WRITE_CF]
                    .levels
                    .iter()
                    .any(|lv| lv.num_tables > 0)
        },
        timeout.as_secs() as usize,
    );
    assert!(ok, "wait for compact to level 1+ timeout");
}

fn switch_mem_table(tx: &mpsc::Sender<ApplyTask>) {
    let mut wb = WriteBatch::new(1);
    wb.set_switch_mem_table();
    write_data(wb, tx);
}

fn write_data(wb: WriteBatch, applier_tx: &mpsc::Sender<ApplyTask>) -> u64 /* write_sequence */ {
    let (result_tx, result_rx) = mpsc::bounded(1);
    let task = ApplyTask::new_wb(wb, result_tx);
    if let Err(err) = applier_tx.send(task) {
        panic!("{:?}", err);
    }
    result_rx.recv().unwrap().unwrap()
}

#[maybe_async::both]
async fn check_get(
    begin: usize,
    end: usize,
    version: u64,
    cfs: &[usize],
    en: &Engine,
    exist: bool,
    check_version: Option<u64>,
    min_blob_size: u32,
) {
    for i in begin..end {
        let key = i_to_key(i as i32, min_blob_size);
        let shard = get_shard_for_key(key.as_bytes(), en);
        let snap = SnapAccess::new(&shard);
        for &cf in cfs {
            let version = if cf == 1 { 0 } else { version };
            let item = snap.get(cf, key.as_bytes(), version).await;
            if item.is_valid() {
                if !exist {
                    if item.is_deleted() {
                        continue;
                    }
                    let shard_stats = shard.get_stats();
                    panic!(
                        "got key {}, shard {}:{}, cf {}, stats {:?}",
                        key, shard.id, shard.ver, cf, shard_stats,
                    );
                }
                assert_eq!(item.get_value(), key.repeat(cf + 2).as_bytes());
                if cf != 1 && check_version.is_some() {
                    assert_eq!(item.version, check_version.unwrap());
                }
            } else if exist {
                let shard_stats = shard.get_stats();
                panic!(
                    "failed to get key {}, shard {}, stats {:?}",
                    key,
                    shard.tag(),
                    shard_stats,
                );
            }
        }
    }
}

#[maybe_async::both]
async fn check_iterator(begin: usize, end: usize, en: &Engine) {
    for cf in 0..3 {
        let mut i = begin;
        // let ids = vec![2, 3, 4, 5, 1];
        let ids = vec![1];
        for id in ids {
            let shard = en.get_shard(id).unwrap();
            let snap = SnapAccess::new(&shard);
            let mut iter = snap.new_iterator(cf, false, false, None, true).await;
            iter.seek(shard.outer_start.chunk()).await;
            while iter.valid() {
                if iter.key.chunk() >= shard.outer_end.chunk() {
                    break;
                }
                let key = i_to_key(i as i32, en.opts.blob_table_build_options.min_blob_size);
                assert_eq!(iter.key(), key.as_bytes());
                assert_eq!(iter.val(), key.repeat(cf + 2).as_bytes());
                i += 1;
                iter.next().await;
            }
        }
        assert_eq!(i, end);
    }
}

fn get_shard_for_key(key: &[u8], en: &Engine) -> Arc<Shard> {
    for id in 1_u64..=5 {
        if let Some(shard) = en.get_shard(id) {
            if shard
                .data_bound()
                .overlap_key(InnerKey::from_inner_buf(key))
            {
                return shard;
            }
        }
    }
    en.get_shard(1).unwrap()
}

#[must_use]
fn try_wait<F>(f: F, seconds: usize) -> bool
where
    F: Fn() -> bool,
{
    let begin = Instant::now_coarse();
    let timeout = Duration::from_secs(seconds as u64);
    while begin.saturating_elapsed() < timeout {
        if f() {
            return true;
        }
        thread::sleep(Duration::from_millis(100))
    }
    false
}

pub(crate) fn generate_encryption_key() -> EncryptionKey {
    let master_key_plain_text = thread_rng().gen::<[u8; 32]>().to_vec();
    let master_key = MasterKey::new(&master_key_plain_text);
    master_key.generate_encryption_key()
}
