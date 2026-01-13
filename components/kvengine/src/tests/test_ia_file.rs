// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{rc::Rc, sync::Arc, time::Duration};

use futures::executor::block_on;
use test_util::init_log_for_test;
use tikv_util::time::Instant;

use crate::{
    Iterator,
    ia::{ia_file::IaFile, manager::IaManager, util::IaManagerOptionsBuilder},
    next, next_async,
    table::{
        file::InMemFile,
        sstable::{BlockCache, NewSsTableCtx, SsTable},
        tiny_meta::SstTinyMeta,
    },
    tests::{new_table, new_test_engine_opt},
    util::test_util::KeyBuilder,
};

#[rstest::rstest]
#[case::concurrency(2)]
#[case::concurrency(8)]
fn test_sync_read(#[case] concurrency: usize) {
    init_log_for_test();

    let file_id = 1;
    let (engine, _) = new_test_engine_opt(true, 1024, "");
    let mut saved_vals: Vec<Rc<Vec<u8>>> = Vec::new();
    let t = new_table(&engine, file_id, 0, 100, 1000, false, &mut saved_vals);

    let ia_rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name("ia-mgr")
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    let sync_read_concurrency = concurrency.div_ceil(2);
    let options = IaManagerOptionsBuilder::default()
        .segment_size(4096)
        .sync_read(false, sync_read_concurrency, Duration::from_secs(3))
        .build()
        .unwrap();
    let mgr = IaManager::new(options, engine.fs.clone(), None, ia_rt.into()).unwrap();
    let t_async = convert_local_sst_to_ia(&t, mgr.clone());

    // Test in tokio async context.
    // Simulate unified pool.
    let unified_pool = tokio::runtime::Builder::new_multi_thread()
        .thread_name("sync-read-unified-pool")
        .enable_all()
        .worker_threads(concurrency)
        .build()
        .unwrap();
    let start_time = Instant::now_coarse();
    let mut js = tokio::task::JoinSet::new();
    for i in 0..concurrency {
        let kb = engine.key_builder.clone();
        let tt = if i & 1 == 0 {
            t_async.clone()
        } else {
            t.clone()
        };
        let task = unified_pool.spawn(async move {
            verify_table_by_scan(&kb, &tt, 0, 100, 1000);
        });
        js.spawn_on(task, unified_pool.handle());
    }
    block_on(async move {
        while let Some(r) = js.join_next().await {
            r.unwrap().unwrap();
        }
    });
    println!(
        "test_sync_read: concurrency {}, take {:?}",
        concurrency,
        start_time.saturating_elapsed()
    );

    // Test in sync context.
    verify_table_by_scan(&engine.key_builder, &t_async, 0, 100, 1000);
}

#[maybe_async::both]
#[track_caller]
pub(crate) async fn verify_table_by_scan(
    kb: &KeyBuilder,
    t: &SsTable,
    begin: usize,
    end: usize,
    ver: u64,
) {
    let mut i = begin;
    let mut it = t.new_iterator(false, false);
    it.rewind().await;
    while it.valid() {
        let key = it.key();
        assert_eq!(kb.i_to_inner_key(i).as_ref(), key);
        let val = it.value();
        assert_eq!(val.version, ver);
        assert_eq!(val.get_value(), key.repeat(2));

        next!(it).await;
        i += 1;
    }
    assert_eq!(i, end);
}

pub(crate) fn open_ia_file_from_sst(t: &SsTable, mgr: IaManager) -> IaFile {
    let meta_data = t
        .file()
        .read(
            t.meta_offset() as u64,
            t.size() as usize - t.meta_offset() as usize,
        )
        .unwrap();
    let meta_file = InMemFile::new(t.id(), meta_data);
    IaFile::open_for_sst(t.id(), Arc::new(meta_file), mgr, None).unwrap()
}

pub(crate) fn convert_local_sst_to_ia(t: &SsTable, mgr: IaManager) -> SsTable {
    let ia_file = open_ia_file_from_sst(t, mgr);
    SsTable::new(Arc::new(ia_file), BlockCache::None, None).unwrap()
}

#[test]
fn test_open_for_sst_with_tiny_meta_matches() {
    init_log_for_test();

    let file_id = 1;
    let (engine, _) = new_test_engine_opt(true, 256, "");
    let mut saved_vals: Vec<Rc<Vec<u8>>> = Vec::new();
    let t = new_table(&engine, file_id, 0, 100, 1000, false, &mut saved_vals);

    let ia_rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name("ia-mgr")
        .enable_all()
        .worker_threads(1)
        .build()
        .unwrap();
    let options = IaManagerOptionsBuilder::default()
        .segment_size(512)
        .build()
        .unwrap();
    let mgr = IaManager::new(options, engine.fs.clone(), None, ia_rt.into()).unwrap();

    let ia_file = Arc::new(open_ia_file_from_sst(&t, mgr.clone()));
    let meta_file = ia_file.table_meta_file().clone();

    let mut ctx = NewSsTableCtx::default();
    let ia_table =
        SsTable::new_with_ctx(ia_file.clone(), BlockCache::None, None, &mut ctx).unwrap();

    let (footer, props_data) = ctx
        .footer_and_properties_data
        .expect("footer and props data");
    let sst_tiny_meta = SstTinyMeta::from_sstable(&ia_table, footer, props_data.as_ref());
    let segment_offsets = sst_tiny_meta
        .segment_offsets
        .as_ref()
        .expect("segment offsets should be present");
    assert!(!segment_offsets.is_empty());
    let ia_with_tiny = IaFile::open_for_sst(t.id(), meta_file, mgr, Some(&sst_tiny_meta)).unwrap();

    assert_eq!(ia_file.id, ia_with_tiny.id);
    assert_eq!(ia_file.size, ia_with_tiny.size);
    assert_eq!(ia_file.ftype, ia_with_tiny.ftype);
    assert_eq!(ia_file.table_meta_off, ia_with_tiny.table_meta_off);
    let segment_offsets_u64: Vec<u64> = segment_offsets.iter().copied().map(u64::from).collect();
    assert_eq!(ia_file.segment_offsets, segment_offsets_u64);
    assert_eq!(ia_file.segment_offsets, ia_with_tiny.segment_offsets);
}
