// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    iter::FromIterator,
    rc::Rc,
    sync::{Arc, Mutex, atomic::Ordering},
    thread,
    time::Duration,
};

use bytes::{Buf, Bytes};
use collections::HashSet;
use futures::executor::block_on;
use rand::prelude::*;
use tidb_query_datatype::{
    codec::table::{encode_row_key, encode_row_key_prefix},
    expr::EvalContext,
};

use crate::{
    DEL_PREFIXES_KEY, DeletePrefixes, EXTRA_CF, IdVer, LOCK_CF, LevelHandler, SnapAccess, WRITE_CF,
    compaction::CompactionPriority,
    dfs,
    dfs::FileType,
    shard::{ShardCf, ShardDataBuilder},
    table,
    table::{
        ChecksumType, SnapVersion,
        columnar::{
            Block, ColumnarFile, ColumnarFilterReader, ColumnarLevels, ColumnarMetaCache,
            ColumnarReader, ColumnarRowTableReader, MinMaxIndex,
            tests::{
                build_table, i_to_common_handle, merge_refs, new_schema, new_schema_with_nullable,
                verify_with_ref_rows,
            },
        },
        file::{File, InMemFile},
        schema_file::{SchemaFile, build_schema_file},
        sstable::{BlockCache, SsTable},
    },
    tests::{
        DEF_BLOCK_SIZE, KEYSPACE_ID, Splitter, TestEngine, keyspace_prefix, new_test_engine_opt,
        prepare_table_region, try_wait,
    },
};

const COLUMNAR_COMPACTION_WAIT_TIME: usize = 10;

#[test]
fn test_columnar_l0_compaction() {
    ::test_util::init_log_for_test();
    let keyspace_id = 1;
    let table_id = 30;
    let table_id2 = 31;
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let allocate_id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();
    let schema = new_schema(table_id, true);
    let schema2 = new_schema(table_id2, true);
    let schemas = vec![schema.clone(), schema2.clone()];
    let schema_version = 10;
    let schema_file_data = build_schema_file(keyspace_id, schema_version, schemas, 0);
    let fs = engine.fs.clone();
    let schema_raw_file = Arc::new(InMemFile::new(allocate_id(), Bytes::from(schema_file_data)));
    fs.get_runtime()
        .block_on(
            fs.create(
                schema_raw_file.id(),
                schema_raw_file
                    .read(0, schema_raw_file.size() as usize)
                    .unwrap(),
                dfs::Options::default().with_type(FileType::Schema),
            ),
        )
        .unwrap();
    let schema_file = SchemaFile::open(schema_raw_file).unwrap();
    let opts = dfs::Options::default().with_type(FileType::Columnar);
    let (l0_tbl_0, l0_tbl_0_ref) = build_table(allocate_id(), &schema, 0, 600, 300);
    let (l0_tbl_1, l0_tbl_1_ref) = build_table(allocate_id(), &schema, 300, 900, 400);
    let (l0_tbl_2, l0_tbl_2_ref) = build_table(allocate_id(), &schema, 1000, 2000, 400);
    let (l0_tbl_3, l0_tbl_3_ref) = build_table(allocate_id(), &schema2, 0, 600, 300);
    let (l1_tbl_0, l1_tbl_0_ref) = build_table(allocate_id(), &schema, 0, 100, 100);
    let (l1_tbl_1, l1_tbl_1_ref) = build_table(allocate_id(), &schema2, 0, 100, 100);
    for file in [
        &l0_tbl_0, &l0_tbl_1, &l0_tbl_2, &l0_tbl_3, &l1_tbl_0, &l1_tbl_1,
    ] {
        info!("build file_id: {}, file size: {}", file.id(), file.size());
        fs.get_runtime()
            .block_on(fs.create(file.id(), file.read(0, file.size() as usize).unwrap(), opts))
            .unwrap()
    }

    let columnar_meta_cache = ColumnarMetaCache::default();
    let mut col_levels = ColumnarLevels::new();
    col_levels.add_file(
        0,
        ColumnarFile::open(l0_tbl_0, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        0,
        ColumnarFile::open(l0_tbl_1, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        0,
        ColumnarFile::open(l0_tbl_2, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        0,
        ColumnarFile::open(l0_tbl_3, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        1,
        ColumnarFile::open(l1_tbl_0, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        1,
        ColumnarFile::open(l1_tbl_1, None, columnar_meta_cache.clone()).unwrap(),
    );

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file));
    builder.set_columnar_levels(col_levels);
    builder.set_columnar_table_ids(vec![table_id, table_id2]);
    shard.set_data(builder.build());
    shard.initial_flushed.store(true, Ordering::SeqCst);
    let id_ver = shard.id_ver();
    *shard.compaction_priority.write().unwrap() =
        Some(CompactionPriority::ColumnarL0 { score: 2.0 });
    engine.trigger_compact(id_ver);
    info!("trigger columnar l0 compaction {}", shard.tag());
    let ok = try_wait(
        || {
            info!(
                "wait columnar l0 compaction {} l0 files: {}, l1 files: {}",
                shard.tag(),
                shard.get_data().col_levels.levels[0].files.len(),
                shard.get_data().col_levels.levels[1].files.len()
            );
            shard.get_data().col_levels.levels[0].files.is_empty()
        },
        COLUMNAR_COMPACTION_WAIT_TIME,
    );
    assert!(ok, "columnar l0 compaction failed");
    let snap = shard.new_snap_access();
    let mut mvcc_reader = snap
        .new_columnar_mvcc_reader(table_id, &schema.columns, None, 500, None)
        .unwrap()
        .unwrap();
    block_on(mvcc_reader.set_handle_range(&i_to_common_handle(0), &i_to_common_handle(2100)))
        .unwrap();
    let mut block = Block::new(&schema);
    block_on(mvcc_reader.read_block(&mut block, usize::MAX)).unwrap();
    let tbl_refs = merge_refs(
        vec![l0_tbl_0_ref, l0_tbl_1_ref, l0_tbl_2_ref, l1_tbl_0_ref],
        1,
        Some(500),
        None,
        Some((i_to_common_handle(0), i_to_common_handle(2100))),
    );
    assert_eq!(block.length(), tbl_refs.len());
    verify_with_ref_rows(&block, &tbl_refs);

    let mut mvcc_reader2 = snap
        .new_columnar_mvcc_reader(table_id2, &schema2.columns, None, 500, None)
        .unwrap()
        .unwrap();
    block_on(mvcc_reader2.set_handle_range(&i_to_common_handle(0), &i_to_common_handle(2100)))
        .unwrap();
    let mut block2 = Block::new(&schema2);
    block_on(mvcc_reader2.read_block(&mut block2, usize::MAX)).unwrap();
    let tbl_refs2 = merge_refs(
        vec![l0_tbl_3_ref, l1_tbl_1_ref],
        1,
        Some(500),
        None,
        Some((i_to_common_handle(0), i_to_common_handle(2100))),
    );
    assert_eq!(block2.length(), tbl_refs2.len());
    verify_with_ref_rows(&block2, &tbl_refs2);
}

#[test]
fn test_columnar_l1_compaction() {
    ::test_util::init_log_for_test();
    let keyspace_id = 1;
    let table_id = 30;
    let table_id2 = 31;
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let allocate_id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();
    let schema = new_schema(table_id, true);
    let schema2 = new_schema(table_id2, true);
    let schemas = vec![schema.clone(), schema2.clone()];
    let schema_version = 10;
    let schema_file_data = build_schema_file(keyspace_id, schema_version, schemas, 0);
    let fs = engine.fs.clone();
    let schema_raw_file = Arc::new(InMemFile::new(allocate_id(), Bytes::from(schema_file_data)));
    fs.get_runtime()
        .block_on(
            fs.create(
                schema_raw_file.id(),
                schema_raw_file
                    .read(0, schema_raw_file.size() as usize)
                    .unwrap(),
                dfs::Options::default().with_type(FileType::Schema),
            ),
        )
        .unwrap();
    let schema_file = SchemaFile::open(schema_raw_file).unwrap();
    let opts = dfs::Options::default().with_type(FileType::Columnar);
    let (l1_tbl_0, l1_tbl_0_ref) = build_table(allocate_id(), &schema, 0, 600, 300);
    let (l1_tbl_1, l1_tbl_1_ref) = build_table(allocate_id(), &schema, 300, 900, 400);
    let (l1_tbl_2, l1_tbl_2_ref) = build_table(allocate_id(), &schema, 1000, 1500, 400);
    let (l1_tbl_3, l1_tbl_3_ref) = build_table(allocate_id(), &schema2, 300, 1200, 500);
    let (l2_tbl_0, l2_tbl_0_ref) = build_table(allocate_id(), &schema, 0, 1000, 100);
    let (l2_tbl_1, l2_tbl_1_ref) = build_table(allocate_id(), &schema, 1000, 2000, 100);
    let (l2_tbl_2, l2_tbl_2_ref) = build_table(allocate_id(), &schema2, 0, 1000, 200);
    for file in [
        &l1_tbl_0, &l1_tbl_1, &l1_tbl_2, &l1_tbl_3, &l2_tbl_0, &l2_tbl_1, &l2_tbl_2,
    ] {
        info!("build file_id: {}, file size: {}", file.id(), file.size());
        fs.get_runtime()
            .block_on(fs.create(file.id(), file.read(0, file.size() as usize).unwrap(), opts))
            .unwrap()
    }
    let columnar_meta_cache = ColumnarMetaCache::default();
    let mut col_levels = ColumnarLevels::new();
    col_levels.add_file(
        1,
        ColumnarFile::open(l1_tbl_0, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        1,
        ColumnarFile::open(l1_tbl_1, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        1,
        ColumnarFile::open(l1_tbl_2, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        1,
        ColumnarFile::open(l1_tbl_3, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        2,
        ColumnarFile::open(l2_tbl_0, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        2,
        ColumnarFile::open(l2_tbl_1, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        2,
        ColumnarFile::open(l2_tbl_2, None, columnar_meta_cache.clone()).unwrap(),
    );

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file));
    builder.set_columnar_levels(col_levels);
    builder.set_columnar_table_ids(vec![table_id, table_id2]);
    shard.set_data(builder.build());
    shard.initial_flushed.store(true, Ordering::SeqCst);
    let id_ver = shard.id_ver();
    *shard.compaction_priority.write().unwrap() =
        Some(CompactionPriority::ColumnarL1 { score: 2.0 });
    engine.trigger_compact(id_ver);
    info!("trigger columnar l1 compaction {}", shard.tag());
    let ok = try_wait(
        || {
            info!(
                "wait columnar l1 compaction {} l1 files: {}, l2 files: {}",
                shard.tag(),
                shard.get_data().col_levels.levels[1].files.len(),
                shard.get_data().col_levels.levels[2].files.len()
            );
            shard.get_data().col_levels.levels[1].files.is_empty()
        },
        COLUMNAR_COMPACTION_WAIT_TIME,
    );
    assert!(ok, "columnar l1 compaction failed");
    let snap = shard.new_snap_access();
    let mut mvcc_reader = snap
        .new_columnar_mvcc_reader(table_id, &schema.columns, None, 500, None)
        .unwrap()
        .unwrap();
    block_on(mvcc_reader.set_handle_range(&i_to_common_handle(0), &i_to_common_handle(2100)))
        .unwrap();
    let mut block = Block::new(&schema);
    block_on(mvcc_reader.read_block(&mut block, usize::MAX)).unwrap();
    let tbl_refs = merge_refs(
        vec![
            l1_tbl_0_ref,
            l1_tbl_1_ref,
            l1_tbl_2_ref,
            l2_tbl_0_ref,
            l2_tbl_1_ref,
        ],
        2,
        Some(500),
        None,
        Some((i_to_common_handle(0), i_to_common_handle(2100))),
    );
    assert_eq!(block.length(), tbl_refs.len());
    verify_with_ref_rows(&block, &tbl_refs);

    let mut mvcc_reader2 = snap
        .new_columnar_mvcc_reader(table_id2, &schema2.columns, None, 600, None)
        .unwrap()
        .unwrap();
    block_on(mvcc_reader2.set_handle_range(&i_to_common_handle(0), &i_to_common_handle(2100)))
        .unwrap();
    let mut block2 = Block::new(&schema2);
    block_on(mvcc_reader2.read_block(&mut block2, usize::MAX)).unwrap();
    let tbl_refs2 = merge_refs(
        vec![l1_tbl_3_ref, l2_tbl_2_ref],
        2,
        Some(600),
        None,
        Some((i_to_common_handle(0), i_to_common_handle(2100))),
    );
    assert_eq!(block2.length(), tbl_refs2.len());
    verify_with_ref_rows(&block2, &tbl_refs2);
}

#[test]
fn test_columnar_major_compaction() {
    use crate::table::columnar::{ColumnarMergeReader, ColumnarMvccReader};

    ::test_util::init_log_for_test();
    let keyspace_id = KEYSPACE_ID;
    let table_id = 30;
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let allocate_id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    engine.opts.set_build_columnar(true);
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();
    let schema = new_schema(table_id, false);
    let schemas = vec![schema.clone()];
    let schema_version = 10;
    let schema_file_data = build_schema_file(keyspace_id, schema_version, schemas, 0);
    let fs = engine.fs.clone();
    let schema_raw_file = Arc::new(InMemFile::new(allocate_id(), Bytes::from(schema_file_data)));
    fs.get_runtime()
        .block_on(
            fs.create(
                schema_raw_file.id(),
                schema_raw_file
                    .read(0, schema_raw_file.size() as usize)
                    .unwrap(),
                dfs::Options::default().with_type(FileType::Schema),
            ),
        )
        .unwrap();
    let schema_file = SchemaFile::open(schema_raw_file).unwrap();
    let mut saved_vals = vec![];
    let l1_tbl_0 = new_sst_table_for_columnar(
        &engine,
        allocate_id(),
        table_id,
        300,
        900,
        350,
        &mut saved_vals,
    );
    let l1_tbl_1 = new_sst_table_for_columnar(
        &engine,
        allocate_id(),
        table_id,
        1000,
        1500,
        350,
        &mut saved_vals,
    );
    let l2_tbl_0 = new_sst_table_for_columnar(
        &engine,
        allocate_id(),
        table_id,
        0,
        1000,
        100,
        &mut saved_vals,
    );
    let l2_tbl_1 = new_sst_table_for_columnar(
        &engine,
        allocate_id(),
        table_id,
        1000,
        2000,
        100,
        &mut saved_vals,
    );

    let level_handler_1 = LevelHandler::new(1, vec![l1_tbl_0.clone(), l1_tbl_1.clone()]);
    let level_handler_2 = LevelHandler::new(2, vec![l2_tbl_0.clone(), l2_tbl_1.clone()]);
    let mut write_cf = ShardCf::new(WRITE_CF);
    write_cf.set_level(level_handler_1);
    write_cf.set_level(level_handler_2);

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_cfs([write_cf, ShardCf::new(LOCK_CF), ShardCf::new(EXTRA_CF)]);
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file.clone()));
    builder.set_persisted_version(SnapVersion::new(0, 1));
    shard.set_data(builder.build());
    shard.initial_flushed.store(true, Ordering::SeqCst);
    let id_ver = shard.id_ver();
    *shard.compaction_priority.write().unwrap() = Some(CompactionPriority::ColumnarMajor {
        table_ids_to_add: vec![table_id],
        table_ids_to_clear: vec![],
        is_manual: false,
        schema_version: schema_file.get_version(),
    });
    engine.trigger_compact(id_ver);
    info!("trigger columnar major compaction {}", shard.tag());
    let ok = try_wait(
        || {
            info!(
                "wait columnar major compaction {} l0 files: {}, l1 files: {}, l2 files: {}",
                shard.tag(),
                shard.get_data().col_levels.levels[0].files.len(),
                shard.get_data().col_levels.levels[1].files.len(),
                shard.get_data().col_levels.levels[2].files.len()
            );
            !shard.get_data().col_levels.levels[2].files.is_empty()
        },
        COLUMNAR_COMPACTION_WAIT_TIME,
    );
    assert!(ok, "columnar major compaction failed");
    let snap = shard.new_snap_access();
    let mut mvcc_reader = snap
        .new_columnar_mvcc_reader(table_id, &schema.columns, None, 500, None)
        .unwrap()
        .unwrap();
    // Use a random end int handle
    let end_handle = thread_rng().gen_range(500..2100);
    block_on(mvcc_reader.set_int_handle_range(0, Some(end_handle))).unwrap();
    let mut block = Block::new(&schema);
    let mvcc_reader_counts = block_on(mvcc_reader.read_block(&mut block, usize::MAX)).unwrap();
    info!("mvcc_reader read block with {} rows", mvcc_reader_counts);
    let mut columnar_readers: Vec<Box<dyn ColumnarReader>> = vec![];
    for tbl in [l1_tbl_0, l1_tbl_1, l2_tbl_0, l2_tbl_1] {
        let iter = tbl.new_iterator(false, false);
        let reader = ColumnarRowTableReader::new(schema.clone(), iter, None, false, None);
        columnar_readers.push(Box::new(reader));
    }
    let merged_reader = ColumnarMergeReader::new(schema.clone(), columnar_readers);
    let mut mvcc_row_reader = ColumnarMvccReader::new(Box::new(merged_reader), &schema, 500);
    block_on(mvcc_row_reader.set_int_handle_range(0, Some(end_handle))).unwrap();
    let mut row_block = Block::new(&schema);
    let merge_reader_counts =
        block_on(mvcc_row_reader.read_block(&mut row_block, usize::MAX)).unwrap();
    info!("merge_reader read block with {} rows", merge_reader_counts);
    verify_columnar_with_blocks(&row_block, &block);

    let ok = try_wait(|| shard.compaction_priority.read().unwrap().is_none(), 5);
    assert!(ok, "wait other compaction failed");
    assert!(shard.get_data().schema_file.is_some());

    // Remove columnar compaction.
    let data = shard.get_data();
    let mut builder = ShardDataBuilder::new(data);
    // Remove schema file to avoid columnar major compaction to be re-triggered.
    builder.set_schema(0, 0, None);
    shard.set_data(builder.build());
    engine.trigger_compact(id_ver);
    info!("trigger remove columnar compaction {}", shard.tag());
    let ok = try_wait(
        || {
            info!(
                "wait remove columnar compaction {} l0 files: {}, l1 files: {}, l2 files: {}",
                shard.tag(),
                shard.get_data().col_levels.levels[0].files.len(),
                shard.get_data().col_levels.levels[1].files.len(),
                shard.get_data().col_levels.levels[2].files.len()
            );
            shard.get_data().col_levels.levels[2].files.is_empty()
        },
        COLUMNAR_COMPACTION_WAIT_TIME,
    );
    assert!(ok);
    assert_eq!(shard.get_columnar_snap_version(), SnapVersion::zero());
}

#[test]
fn test_columnar_major_compaction_multiple_tables() {
    use crate::{
        table::{
            columnar::{ColumnarMergeReader, ColumnarMvccReader},
            table::BoundedDataSet,
        },
        table_id::get_table_id_from_data_bound,
    };

    ::test_util::init_log_for_test();
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    engine.opts.set_build_columnar(true);
    let keyspace_id = KEYSPACE_ID;
    let table_ids = vec![1, 2, 3, 4, 5];
    let mut schemas = vec![];
    // Split to keep tables in same region.
    let split_key_1 = [keyspace_prefix(keyspace_id), encode_row_key_prefix(1)].concat();
    let split_key_2 = [keyspace_prefix(keyspace_id), encode_row_key_prefix(6)].concat();
    let mut splitter = Splitter::new(
        vec![split_key_1, split_key_2],
        IdVer::new(1, 1),
        5,
        apply_tx.clone(),
    );
    let handle = thread::spawn(move || {
        splitter.run();
    });
    handle.join().unwrap();

    for &table_id in &table_ids {
        let schema = new_schema(table_id, false);
        schemas.push(schema);
    }
    let allocate_id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let schema_version = 10;
    // Build schema file for the first 3 tables.
    let schemas_1 = schemas[0..3].to_vec();
    let schema_file_data_1 = build_schema_file(keyspace_id, schema_version, schemas_1, 0);
    let fs = engine.fs.clone();
    let schema_raw_file_1 = Arc::new(InMemFile::new(
        allocate_id(),
        Bytes::from(schema_file_data_1),
    ));
    fs.get_runtime()
        .block_on(
            fs.create(
                schema_raw_file_1.id(),
                schema_raw_file_1
                    .read(0, schema_raw_file_1.size() as usize)
                    .unwrap(),
                dfs::Options::default().with_type(FileType::Schema),
            ),
        )
        .unwrap();
    let schema_file = SchemaFile::open(schema_raw_file_1).unwrap();
    let mut saved_vals = vec![];
    let mut l1_tbls = vec![];
    let mut l2_tbls = vec![];
    for (idx, &table_id) in table_ids.iter().enumerate().rev() {
        let l1_tbl_0 = new_sst_table_for_columnar(
            &engine,
            allocate_id(),
            table_id,
            300,
            900,
            350 + idx as u64,
            &mut saved_vals,
        );
        l1_tbls.push(l1_tbl_0);
        let l1_tbl_1 = new_sst_table_for_columnar(
            &engine,
            allocate_id(),
            table_id,
            1000,
            1500,
            350 + idx as u64,
            &mut saved_vals,
        );
        l1_tbls.push(l1_tbl_1);
        let l2_tbl_0 = new_sst_table_for_columnar(
            &engine,
            allocate_id(),
            table_id,
            0,
            1000,
            100 + idx as u64,
            &mut saved_vals,
        );
        l2_tbls.push(l2_tbl_0);
        let l2_tbl_1 = new_sst_table_for_columnar(
            &engine,
            allocate_id(),
            table_id,
            1000,
            2000,
            100 + idx as u64,
            &mut saved_vals,
        );
        l2_tbls.push(l2_tbl_1);
    }
    l1_tbls.sort_by_key(|tbl| tbl.smallest().to_vec());
    l2_tbls.sort_by_key(|tbl| tbl.smallest().to_vec());
    let level_handler_1 = LevelHandler::new(1, l1_tbls.clone());
    let level_handler_2 = LevelHandler::new(2, l2_tbls.clone());
    let mut write_cf = ShardCf::new(WRITE_CF);
    write_cf.set_level(level_handler_1);
    write_cf.set_level(level_handler_2);

    let shard = engine.get_shard(7).unwrap();
    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_cfs([write_cf, ShardCf::new(LOCK_CF), ShardCf::new(EXTRA_CF)]);
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file.clone()));
    shard.set_data(builder.build());
    shard.initial_flushed.store(true, Ordering::SeqCst);
    let id_ver = shard.id_ver();
    *shard.compaction_priority.write().unwrap() = Some(CompactionPriority::ColumnarMajor {
        table_ids_to_add: schema_file.export_backend_table_ids(),
        table_ids_to_clear: vec![],
        is_manual: false,
        schema_version: schema_file.get_version(),
    });
    engine.trigger_compact(id_ver);
    info!("trigger columnar major compaction {}", shard.tag());
    let ok = try_wait(
        || {
            info!(
                "wait columnar major compaction {} l0 files: {}, l1 files: {}, l2 files: {}",
                shard.tag(),
                shard.get_data().col_levels.levels[0].files.len(),
                shard.get_data().col_levels.levels[1].files.len(),
                shard.get_data().col_levels.levels[2].files.len()
            );
            !shard.get_data().col_levels.levels[2].files.is_empty()
        },
        COLUMNAR_COMPACTION_WAIT_TIME,
    );
    assert!(ok, "columnar major compaction failed");
    let verify_columnar_for_table = |schema_file: &SchemaFile, snap: &SnapAccess, table_id: i64| {
        let schema = schema_file.get_table(table_id).unwrap();
        let mut mvcc_reader = snap
            .new_columnar_mvcc_reader(table_id, &schema.columns, None, 500, None)
            .unwrap()
            .unwrap();
        // Use a random end int handle
        let end_handle = thread_rng().gen_range(500..2100);
        block_on(mvcc_reader.set_int_handle_range(0, Some(end_handle))).unwrap();
        let mut block = Block::new(schema);
        let mvcc_reader_counts = block_on(mvcc_reader.read_block(&mut block, usize::MAX)).unwrap();
        info!("mvcc_reader read block with {} rows", mvcc_reader_counts);
        let mut columnar_readers: Vec<Box<dyn ColumnarReader>> = vec![];
        for tbl in l1_tbls.iter().chain(l2_tbls.iter()) {
            let data_bound = tbl.data_bound();
            let (min_table_id, max_table_id) = get_table_id_from_data_bound(data_bound);
            if table_id < min_table_id || table_id > max_table_id {
                continue;
            }
            let iter = tbl.new_iterator(false, false);
            let reader = ColumnarRowTableReader::new(schema.clone(), iter, None, false, None);
            columnar_readers.push(Box::new(reader));
        }
        let merged_reader = ColumnarMergeReader::new(schema.clone(), columnar_readers);
        let mut mvcc_row_reader = ColumnarMvccReader::new(Box::new(merged_reader), schema, 500);
        block_on(mvcc_row_reader.set_int_handle_range(0, Some(end_handle))).unwrap();
        let mut row_block = Block::new(schema);
        let merge_reader_counts =
            block_on(mvcc_row_reader.read_block(&mut row_block, usize::MAX)).unwrap();
        info!("merge_reader read block with {} rows", merge_reader_counts);
        verify_columnar_with_blocks(&row_block, &block);
    };
    let snap = shard.new_snap_access();
    for table_id in schema_file.export_backend_table_ids() {
        verify_columnar_for_table(&schema_file, &snap, table_id);
    }

    // Add table id 4 & 5 to schema file and remove table id 3 from schema file.
    let schemas_2 = vec![
        schemas[0].clone(),
        schemas[1].clone(),
        schemas[3].clone(),
        schemas[4].clone(),
    ];
    let schema_file_data_2 = build_schema_file(keyspace_id, schema_version + 1, schemas_2, 0);
    let schema_raw_file_2 = Arc::new(InMemFile::new(
        allocate_id(),
        Bytes::from(schema_file_data_2),
    ));
    fs.get_runtime()
        .block_on(
            fs.create(
                schema_raw_file_2.id(),
                schema_raw_file_2
                    .read(0, schema_raw_file_2.size() as usize)
                    .unwrap(),
                dfs::Options::default().with_type(FileType::Schema),
            ),
        )
        .unwrap();
    let schema_file = SchemaFile::open(schema_raw_file_2).unwrap();

    let ok = try_wait(|| shard.compaction_priority.read().unwrap().is_none(), 5);
    assert!(ok, "wait other compaction failed");

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file.clone()));
    shard.set_data(builder.build());
    *shard.compaction_priority.write().unwrap() = Some(CompactionPriority::ColumnarMajor {
        table_ids_to_add: vec![4, 5],
        table_ids_to_clear: vec![3],
        is_manual: false,
        schema_version: schema_file.get_version(),
    });
    engine.trigger_compact(id_ver);
    info!("trigger columnar major compaction {}", shard.tag());
    let ok = try_wait(
        || {
            info!(
                "wait columnar major compaction {} l0 files: {}, l1 files: {}, l2 files: {}, columnar_table_ids: {:?}",
                shard.tag(),
                shard.get_data().col_levels.levels[0].files.len(),
                shard.get_data().col_levels.levels[1].files.len(),
                shard.get_data().col_levels.levels[2].files.len(),
                shard.get_data().columnar_table_ids
            );
            !shard.get_data().columnar_table_ids.contains(&3)
                && shard.get_data().columnar_table_ids.contains(&4)
                && shard.get_data().columnar_table_ids.contains(&5)
        },
        COLUMNAR_COMPACTION_WAIT_TIME,
    );
    assert!(ok, "columnar major compaction failed");
    let snap = shard.new_snap_access();
    let table_ids = schema_file.export_backend_table_ids();
    assert!(table_ids.contains(&4));
    assert!(table_ids.contains(&5));
    assert!(!table_ids.contains(&3));
    for table_id in table_ids {
        verify_columnar_for_table(&schema_file, &snap, table_id);
    }

    let ok = try_wait(|| shard.compaction_priority.read().unwrap().is_none(), 5);
    assert!(ok, "wait other compaction failed");

    // Test manual columnar major compaction.
    let data = shard.get_data();
    shard.set_data(ShardDataBuilder::new(data).build());
    *shard.compaction_priority.write().unwrap() = Some(CompactionPriority::ColumnarMajor {
        table_ids_to_add: vec![1, 2, 4, 5],
        table_ids_to_clear: vec![],
        is_manual: true,
        schema_version: schema_file.get_version(),
    });
    engine.trigger_compact(id_ver);
    info!("trigger manual columnar major compaction {}", shard.tag());
    let old_file_ids = HashSet::from_iter(shard.get_all_col_files().iter().cloned());
    let ok = try_wait(
        || {
            let new_file_ids = HashSet::from_iter(shard.get_all_col_files().iter().cloned());
            info!(
                "wait manual columnar major compaction {} file_ids: {:?}",
                shard.tag(),
                new_file_ids
            );
            old_file_ids.difference(&new_file_ids).count() == 0
        },
        COLUMNAR_COMPACTION_WAIT_TIME,
    );
    assert!(ok);
    let table_ids = shard.get_data().columnar_table_ids.clone();
    let snap = shard.new_snap_access();
    assert_eq!(table_ids, vec![1, 2, 4, 5]);
    for table_id in table_ids {
        verify_columnar_for_table(&schema_file, &snap, table_id);
    }
    let ok = try_wait(|| shard.compaction_priority.read().unwrap().is_none(), 5);
    assert!(ok, "wait other compaction failed");

    // Remove columnar compaction.
    let data = shard.get_data();
    shard.set_data(ShardDataBuilder::new(data).build());
    *shard.compaction_priority.write().unwrap() = Some(CompactionPriority::ColumnarClear);
    engine.trigger_compact(id_ver);
    info!("trigger remove columnar compaction {}", shard.tag());
    let ok = try_wait(
        || {
            info!(
                "wait remove columnar compaction {} l0 files: {}, l1 files: {}, l2 files: {}",
                shard.tag(),
                shard.get_data().col_levels.levels[0].files.len(),
                shard.get_data().col_levels.levels[1].files.len(),
                shard.get_data().col_levels.levels[2].files.len()
            );
            shard.get_data().col_levels.levels[2].files.is_empty()
        },
        COLUMNAR_COMPACTION_WAIT_TIME,
    );
    assert!(ok);
    assert_eq!(shard.get_columnar_snap_version(), SnapVersion::zero());
    assert!(shard.get_data().schema_file.is_some());
}

#[test]
fn test_columnar_destroy_range() {
    ::test_util::init_log_for_test();
    let keyspace_id = 1;
    let table_id = 30;
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let allocate_id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();
    let schema = new_schema(table_id, true);
    let schemas = vec![schema.clone()];
    let schema_version = 10;
    let schema_file_data = build_schema_file(keyspace_id, schema_version, schemas, 0);
    let fs = engine.fs.clone();
    let schema_raw_file = Arc::new(InMemFile::new(allocate_id(), Bytes::from(schema_file_data)));
    fs.get_runtime()
        .block_on(
            fs.create(
                schema_raw_file.id(),
                schema_raw_file
                    .read(0, schema_raw_file.size() as usize)
                    .unwrap(),
                dfs::Options::default().with_type(FileType::Schema),
            ),
        )
        .unwrap();
    let schema_file = SchemaFile::open(schema_raw_file).unwrap();
    let opts = dfs::Options::default().with_type(FileType::Columnar);
    let (l0_tbl_0, _) = build_table(allocate_id(), &schema, 0, 600, 500);
    let (l0_tbl_1, _) = build_table(allocate_id(), &schema, 300, 900, 500);
    let (l0_tbl_2, _) = build_table(allocate_id(), &schema, 1000, 2000, 500);
    let (l1_tbl_0, _) = build_table(allocate_id(), &schema, 0, 600, 300);
    let (l1_tbl_1, _) = build_table(allocate_id(), &schema, 300, 900, 400);
    let (l1_tbl_2, _) = build_table(allocate_id(), &schema, 1000, 1500, 400);
    let (l2_tbl_0, _) = build_table(allocate_id(), &schema, 0, 1000, 100);
    let (l2_tbl_1, _) = build_table(allocate_id(), &schema, 1000, 2000, 100);
    for file in [
        &l0_tbl_0, &l0_tbl_1, &l0_tbl_2, &l1_tbl_0, &l1_tbl_1, &l1_tbl_2, &l2_tbl_0, &l2_tbl_1,
    ] {
        info!("build file_id: {}, file size: {}", file.id(), file.size());
        fs.get_runtime()
            .block_on(fs.create(file.id(), file.read(0, file.size() as usize).unwrap(), opts))
            .unwrap()
    }

    let columnar_meta_cache = ColumnarMetaCache::default();
    let mut col_levels = ColumnarLevels::new();
    col_levels.add_file(
        0,
        ColumnarFile::open(l0_tbl_0, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        0,
        ColumnarFile::open(l0_tbl_1, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        0,
        ColumnarFile::open(l0_tbl_2, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        1,
        ColumnarFile::open(l1_tbl_0, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        1,
        ColumnarFile::open(l1_tbl_1, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        1,
        ColumnarFile::open(l1_tbl_2, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        2,
        ColumnarFile::open(l2_tbl_0, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        2,
        ColumnarFile::open(l2_tbl_1, None, columnar_meta_cache.clone()).unwrap(),
    );
    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file));
    builder.set_columnar_levels(col_levels);
    builder.set_columnar_table_ids(vec![table_id]);
    shard.set_data(builder.build());
    let mut del_prefixes = DeletePrefixes::new_with_keyspace_id(shard.keyspace_id);
    let mut table_prefix = api_version::ApiV2::get_txn_keyspace_prefix(keyspace_id);
    table_prefix.extend_from_slice(b"t");
    del_prefixes.merge_prefix_in_place(&table_prefix);
    shard.set_property(DEL_PREFIXES_KEY, &del_prefixes.marshal());
    shard.initial_flushed.store(true, Ordering::SeqCst);
    let id_ver = shard.id_ver();
    *shard.compaction_priority.write().unwrap() = Some(CompactionPriority::DestroyRange);
    engine.trigger_compact(id_ver);
    info!("trigger columnar destroy range compaction {}", shard.tag());
    let ok = try_wait(
        || {
            shard
                .get_data()
                .col_levels
                .levels
                .iter()
                .any(|l| l.files.is_empty())
        },
        COLUMNAR_COMPACTION_WAIT_TIME,
    );
    assert!(ok, "columnar destroy range compaction failed");
}

#[test]
fn test_columnar_truncate_ts() {
    ::test_util::init_log_for_test();
    let keyspace_id = KEYSPACE_ID;
    let table_id = 30;
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let allocate_id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();
    let schema = new_schema(table_id, false);
    let schemas = vec![schema.clone()];
    let schema_version = 10;
    let schema_file_data = build_schema_file(keyspace_id, schema_version, schemas, 0);
    let fs = engine.fs.clone();
    let schema_raw_file = Arc::new(InMemFile::new(allocate_id(), Bytes::from(schema_file_data)));
    fs.get_runtime()
        .block_on(
            fs.create(
                schema_raw_file.id(),
                schema_raw_file
                    .read(0, schema_raw_file.size() as usize)
                    .unwrap(),
                dfs::Options::default().with_type(FileType::Schema),
            ),
        )
        .unwrap();
    let schema_file = SchemaFile::open(schema_raw_file).unwrap();
    let opts = dfs::Options::default().with_type(FileType::Columnar);
    let (l0_tbl_0, _) = build_table(allocate_id(), &schema, 0, 600, 500);
    let (l0_tbl_1, _) = build_table(allocate_id(), &schema, 300, 900, 500);
    let (l0_tbl_2, _) = build_table(allocate_id(), &schema, 1000, 2000, 500);
    let (l1_tbl_0, _) = build_table(allocate_id(), &schema, 0, 600, 300);
    let (l1_tbl_1, _) = build_table(allocate_id(), &schema, 300, 900, 400);
    let (l1_tbl_2, _) = build_table(allocate_id(), &schema, 1000, 1500, 400);
    let (l2_tbl_0, _) = build_table(allocate_id(), &schema, 0, 1000, 100);
    let (l2_tbl_1, _) = build_table(allocate_id(), &schema, 1000, 2000, 100);
    for file in [
        &l0_tbl_0, &l0_tbl_1, &l0_tbl_2, &l1_tbl_0, &l1_tbl_1, &l1_tbl_2, &l2_tbl_0, &l2_tbl_1,
    ] {
        info!("build file_id: {}, file size: {}", file.id(), file.size());
        fs.get_runtime()
            .block_on(fs.create(file.id(), file.read(0, file.size() as usize).unwrap(), opts))
            .unwrap()
    }
    let columnar_meta_cache = ColumnarMetaCache::default();
    let mut col_levels = ColumnarLevels::new();
    col_levels.add_file(
        0,
        ColumnarFile::open(l0_tbl_0, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        0,
        ColumnarFile::open(l0_tbl_1, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        0,
        ColumnarFile::open(l0_tbl_2, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        1,
        ColumnarFile::open(l1_tbl_0, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        1,
        ColumnarFile::open(l1_tbl_1, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        1,
        ColumnarFile::open(l1_tbl_2, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        2,
        ColumnarFile::open(l2_tbl_0, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        2,
        ColumnarFile::open(l2_tbl_1, None, columnar_meta_cache.clone()).unwrap(),
    );
    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file));
    builder.set_columnar_levels(col_levels);
    builder.set_columnar_table_ids(vec![table_id]);
    shard.set_data(builder.build());
    shard.initial_flushed.store(true, Ordering::SeqCst);
    let id_ver = shard.id_ver();
    shard.pending_ops.write().unwrap().truncate_ts = Some(200);
    *shard.compaction_priority.write().unwrap() = Some(CompactionPriority::TruncateTs);
    engine.trigger_compact(id_ver);
    info!("trigger columnar truncate ts compaction {}", shard.tag());
    let ok = try_wait(
        || {
            shard.get_data().col_levels.levels[0].files.is_empty()
                && shard.get_data().col_levels.levels[1].files.is_empty()
                && shard.get_data().col_levels.levels[2].files.len() == 2
        },
        COLUMNAR_COMPACTION_WAIT_TIME,
    );
    assert!(ok, "columnar truncate_ts compaction failed");
    let snap = shard.new_snap_access();
    let mut mvcc_reader = snap
        .new_columnar_mvcc_reader(table_id, &schema.columns, None, u64::MAX, None)
        .unwrap()
        .unwrap();
    block_on(mvcc_reader.set_int_handle_range(0, Some(3000))).unwrap();
    let mut block = Block::new(&schema);
    let counts = block_on(mvcc_reader.read_block(&mut block, usize::MAX)).unwrap();
    for i in 0..counts {
        assert_eq!(100, block.versions.get_version(i));
    }
}

#[test]
fn test_columnar_trim_over_bound() {
    ::test_util::init_log_for_test();
    let keyspace_id = KEYSPACE_ID;
    let table_id = 30;
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let allocate_id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();
    let schema = new_schema(table_id, false);
    let schemas = vec![schema.clone()];
    let schema_version = 10;
    let schema_file_data = build_schema_file(keyspace_id, schema_version, schemas, 0);
    let fs = engine.fs.clone();
    let schema_raw_file = Arc::new(InMemFile::new(allocate_id(), Bytes::from(schema_file_data)));
    fs.get_runtime()
        .block_on(
            fs.create(
                schema_raw_file.id(),
                schema_raw_file
                    .read(0, schema_raw_file.size() as usize)
                    .unwrap(),
                dfs::Options::default().with_type(FileType::Schema),
            ),
        )
        .unwrap();
    let schema_file = SchemaFile::open(schema_raw_file).unwrap();
    let opts = dfs::Options::default().with_type(FileType::Columnar);
    let (l0_tbl_0, _) = build_table(allocate_id(), &schema, 0, 600, 500);
    let (l0_tbl_1, _) = build_table(allocate_id(), &schema, 300, 900, 600);
    let (l0_tbl_2, _) = build_table(allocate_id(), &schema, 1000, 2000, 510);
    let (l1_tbl_0, _) = build_table(allocate_id(), &schema, 0, 600, 300);
    let (l1_tbl_1, _) = build_table(allocate_id(), &schema, 300, 900, 410);
    let (l1_tbl_2, _) = build_table(allocate_id(), &schema, 1000, 1500, 420);
    let (l2_tbl_0, _) = build_table(allocate_id(), &schema, 0, 1000, 100);
    let (l2_tbl_1, _) = build_table(allocate_id(), &schema, 1000, 2000, 110);
    for file in [
        &l0_tbl_0, &l0_tbl_1, &l0_tbl_2, &l1_tbl_0, &l1_tbl_1, &l1_tbl_2, &l2_tbl_0, &l2_tbl_1,
    ] {
        info!("build file_id: {}, file size: {}", file.id(), file.size());
        fs.get_runtime()
            .block_on(fs.create(file.id(), file.read(0, file.size() as usize).unwrap(), opts))
            .unwrap()
    }
    let columnar_meta_cache = ColumnarMetaCache::default();
    let mut col_levels = ColumnarLevels::new();
    col_levels.add_file(
        0,
        ColumnarFile::open(l0_tbl_0, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        0,
        ColumnarFile::open(l0_tbl_1, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        0,
        ColumnarFile::open(l0_tbl_2, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        1,
        ColumnarFile::open(l1_tbl_0, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        1,
        ColumnarFile::open(l1_tbl_1, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        1,
        ColumnarFile::open(l1_tbl_2, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        2,
        ColumnarFile::open(l2_tbl_0, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        2,
        ColumnarFile::open(l2_tbl_1, None, columnar_meta_cache.clone()).unwrap(),
    );

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file));
    builder.set_columnar_levels(col_levels);
    builder.set_columnar_table_ids(vec![table_id]);
    shard.set_data(builder.build());
    // Split shard by handle key 800
    let split_key = [keyspace_prefix(keyspace_id), encode_row_key(table_id, 800)].concat();
    let mut splitter = Splitter::new(vec![split_key], IdVer::new(4, 4), 5, apply_tx);
    let handle = thread::spawn(move || {
        splitter.run();
    });
    let ok = try_wait(|| engine.shards.len() == 6, 2);
    assert!(ok, "split failed");
    handle.join().unwrap();

    let shard = engine.get_shard(shard_id).unwrap();
    // Read before trim over bound data.
    shard.initial_flushed.store(true, Ordering::SeqCst);
    let snap = shard.new_snap_access();
    let mut mvcc_reader = snap
        .new_columnar_mvcc_reader(table_id, &schema.columns, None, u64::MAX, None)
        .unwrap()
        .unwrap();
    block_on(mvcc_reader.set_int_handle_range(0, Some(3000))).unwrap();
    let mut block = Block::new(&schema);
    let read_before_trim = block_on(mvcc_reader.read_block(&mut block, usize::MAX)).unwrap();

    let id_ver = shard.id_ver();
    shard.pending_ops.write().unwrap().trim_over_bound = true;
    *shard.compaction_priority.write().unwrap() = Some(CompactionPriority::TrimOverBound);
    engine.trigger_compact(id_ver);
    info!(
        "trigger columnar trim over bound compaction {}",
        shard.tag()
    );
    let ok = try_wait(
        || shard.get_data().get_col_table_counts(0) == 2,
        COLUMNAR_COMPACTION_WAIT_TIME,
    );
    assert!(ok, "columnar trim over bound compaction failed");
    std::thread::sleep(Duration::from_secs(2));
    let snap = shard.new_snap_access();
    let mut mvcc_reader = snap
        .new_columnar_mvcc_reader(table_id, &schema.columns, None, u64::MAX, None)
        .unwrap()
        .unwrap();
    block_on(mvcc_reader.set_int_handle_range(0, Some(3000))).unwrap();
    let mut block = Block::new(&schema);
    let read_after_trim = block_on(mvcc_reader.read_block(&mut block, usize::MAX)).unwrap();
    info!(
        "read_before_trim: {}, read_after_trim: {}",
        read_before_trim, read_after_trim
    );
    assert!(read_before_trim > read_after_trim);
    for i in 0..read_after_trim {
        let handle = i64::from_le_bytes(
            std::convert::TryInto::try_into(block.handles.get_not_null_value(i)).unwrap(),
        );
        assert!(handle >= 800);
        let version = block.versions.get_version(i);
        if handle == 800 {
            assert!(version == 600 || version == 599 || version == 598);
        }
    }
    // Check old version exists
    let mut mvcc_reader = snap
        .new_columnar_mvcc_reader(table_id, &schema.columns, None, 300, None)
        .unwrap()
        .unwrap();
    block_on(mvcc_reader.set_int_handle_range(0, Some(3000))).unwrap();
    let mut block = Block::new(&schema);
    let read_old_version = block_on(mvcc_reader.read_block(&mut block, usize::MAX)).unwrap();
    for i in 0..read_old_version {
        let handle = i64::from_le_bytes(
            std::convert::TryInto::try_into(block.handles.get_not_null_value(i)).unwrap(),
        );
        if handle == 800 {
            assert!(block.versions.get_version(i) <= 100);
        }
    }
}

#[test]
fn test_min_max_index_not_in() {
    // not-in condition_values, data, has_null, expected
    let cases: Vec<(Vec<i64>, Vec<i64>, bool, bool)> = vec![
        (vec![2, 3, 5], vec![1, 3, 10], false, true),
        (vec![2, 3, 5], vec![1, 4, 6, 10], false, true),
        (vec![2, 3, 5], vec![0, 3, 15], false, true),
        (vec![2, 3, 5], vec![1, 1, 1], false, true),
        (vec![2, 3, 5], vec![3, 3, 3], false, false),
        (vec![2, 3, 5], vec![3, 3, 3], true, true),
    ];
    for (condition_values, data, has_null, expected) in cases {
        let mut min_max_index = MinMaxIndex::new(1, 8, true);
        let min = *data.iter().min().unwrap();
        let max = *data.iter().max().unwrap();
        min_max_index.push_value(&min.to_le_bytes());
        min_max_index.push_value(&max.to_le_bytes());
        let values = condition_values
            .iter()
            .map(|v| v.to_le_bytes().to_vec())
            .collect::<Vec<Vec<u8>>>();
        min_max_index.push_has_null(has_null);
        min_max_index.push_has_value(true);
        assert_eq!(
            min_max_index.check_not_in(
                0,
                &values,
                tidb_query_datatype::FieldTypeTp::LongLong,
                false
            ),
            expected
        );
    }
}

fn verify_columnar_with_blocks(b1: &Block, b2: &Block) {
    assert_eq!(b1.length(), b2.length());
    let len = b1.length();
    for i in 0..len {
        assert_eq!(
            b1.handles.get_not_null_value(i),
            b2.handles.get_not_null_value(i),
            "{}",
            i
        );
        assert_eq!(b1.versions.get_version(i), b2.versions.get_version(i));
        assert_eq!(b1.versions.is_null(i), b2.versions.is_null(i));
        assert_eq!(b1.columns[0].is_null(i), b2.columns[0].is_null(i));
        assert_eq!(b1.columns[1].is_null(i), b2.columns[1].is_null(i));
        assert_eq!(
            b1.columns[0].get_value(i).unwrap().get_i64_le(),
            b2.columns[0].get_value(i).unwrap().get_i64_le()
        );
        assert_eq!(
            b1.columns[1].get_value(i).unwrap(),
            b2.columns[1].get_value(i).unwrap()
        );
    }
}

fn new_sst_table_for_columnar(
    engine: &TestEngine,
    id: u64,
    table_id: i64,
    begin: usize,
    end: usize,
    version: u64,
    saved_vals: &mut Vec<Rc<Vec<u8>>>,
) -> SsTable {
    let block_size = engine.opts.table_builder_options.block_size;
    let comp_tp = engine.opts.table_builder_options.compression_tps[0];
    let comp_lvl = engine.opts.table_builder_options.compression_lvl;
    let fs = engine.fs.clone();

    let mut builder = table::sstable::builder::Builder::new(
        id,
        block_size,
        comp_tp,
        comp_lvl,
        ChecksumType::Crc32,
        None,
    );
    let ctx = Mutex::new(EvalContext::default());
    for i in begin..end {
        let key = engine.key_builder.gen_row_inner_key(table_id, i);
        let val = Rc::new(engine.key_builder.gen_row_val(&ctx, i));
        // Save the val with Rc, make sure the val_rc stay valid during the iterator
        // lifecycle.
        saved_vals.push(val.clone());
        let val = table::Value::new_with_meta_version(0, version, 0, &val);
        builder.add(key.as_ref(), &val, None);
    }
    let mut data_buf = Vec::new();
    builder.finish(0, &mut data_buf);
    let data = Bytes::from(data_buf);
    let opts = dfs::Options::default();
    let runtime = fs.get_runtime();
    runtime.block_on(fs.create(id, data.clone(), opts)).unwrap();
    let file = InMemFile::new(id, data);
    SsTable::new(Arc::new(file), BlockCache::None, None).unwrap()
}

#[test]
fn test_columnar_not_nullable_to_nullable() {
    ::test_util::init_log_for_test();
    let keyspace_id = 1;
    let table_id = 30;
    let table_id2 = 31;

    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let allocate_id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();
    let schema = new_schema_with_nullable(table_id, true, false);
    let schemas = vec![schema.clone()];
    let schema_version = 10;
    let schema_file_data = build_schema_file(keyspace_id, schema_version, schemas, 0);
    let fs = engine.fs.clone();
    let schema_raw_file = Arc::new(InMemFile::new(allocate_id(), Bytes::from(schema_file_data)));
    fs.get_runtime()
        .block_on(
            fs.create(
                schema_raw_file.id(),
                schema_raw_file
                    .read(0, schema_raw_file.size() as usize)
                    .unwrap(),
                dfs::Options::default().with_type(FileType::Schema),
            ),
        )
        .unwrap();
    let schema_file = SchemaFile::open(schema_raw_file).unwrap();
    let opts = dfs::Options::default().with_type(FileType::Columnar);
    let (l0_tbl_0, _) = build_table(allocate_id(), &schema, 0, 600, 300);
    let (l0_tbl_1, _) = build_table(allocate_id(), &schema, 300, 900, 400);
    for file in [&l0_tbl_0, &l0_tbl_1] {
        info!("build file_id: {}, file size: {}", file.id(), file.size());
        fs.get_runtime()
            .block_on(fs.create(file.id(), file.read(0, file.size() as usize).unwrap(), opts))
            .unwrap()
    }
    let columnar_meta_cache = ColumnarMetaCache::default();
    let mut col_levels = ColumnarLevels::new();
    col_levels.add_file(
        0,
        ColumnarFile::open(l0_tbl_0, None, columnar_meta_cache.clone()).unwrap(),
    );
    col_levels.add_file(
        0,
        ColumnarFile::open(l0_tbl_1, None, columnar_meta_cache.clone()).unwrap(),
    );

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file));
    builder.set_columnar_levels(col_levels);
    builder.set_columnar_table_ids(vec![table_id, table_id2]);
    shard.set_data(builder.build());
    shard.initial_flushed.store(true, Ordering::SeqCst);
    let id_ver = shard.id_ver();
    *shard.compaction_priority.write().unwrap() =
        Some(CompactionPriority::ColumnarL0 { score: 2.0 });
    engine.trigger_compact(id_ver);
    info!("trigger columnar l0 compaction {}", shard.tag());
    let ok = try_wait(
        || {
            info!(
                "wait columnar l0 compaction {} l0 files: {}, l1 files: {}",
                shard.tag(),
                shard.get_data().col_levels.levels[0].files.len(),
                shard.get_data().col_levels.levels[1].files.len()
            );
            shard.get_data().col_levels.levels[0].files.is_empty()
        },
        COLUMNAR_COMPACTION_WAIT_TIME,
    );
    assert!(ok, "columnar l0 compaction failed");
    let snap = shard.new_snap_access();
    // Read with schema with nullable columns.
    let new_schema = new_schema_with_nullable(table_id, true, true);
    let mut mvcc_reader = snap
        .new_columnar_mvcc_reader(table_id, &new_schema.columns, None, 500, None)
        .unwrap()
        .unwrap();
    block_on(mvcc_reader.set_handle_range(&i_to_common_handle(0), &i_to_common_handle(2100)))
        .unwrap();
    let mut block = Block::new(&schema);
    block_on(mvcc_reader.read_block(&mut block, usize::MAX)).unwrap();
}
