// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{convert::TryInto, sync::Arc};

use bytes::Bytes;
use clara_fts::test_util::{PlainFtsQueryInfo, make_unscored_query};
use futures::executor::block_on;
use kvenginepb::fts::FullTextIndexDef;
use tidb_query_datatype::{FieldTypeTp, codec::table::encode_row_key};
use tikv_util::config::ReadableSize;

use crate::{
    DEL_PREFIXES_KEY, DeletePrefixes,
    compaction::CompactionPriority,
    dfs,
    dfs::FileType,
    shard::ShardDataBuilder,
    table::{
        SnapVersion,
        columnar::{ColumnarFile, ColumnarLevels, ColumnarMetaCache},
        file::{File, InMemFile},
        fts::{
            EDedicatedFile, FtsCache, FtsLevels, IntPk, OrderedPkIterator, PackedFile, PkReader,
            PkType, lp_key,
            test_util::{
                SchemaBuilder, TestColumnarFileBuilder, TestDedicatedFileBuilder,
                TestPackedFileBuilder, new_columnar, new_ded, new_packed,
            },
        },
        schema_file::{Schema, SchemaFile, build_schema_file},
    },
    tests::{
        DEF_BLOCK_SIZE, KEYSPACE_ID, TestEngine, keyspace_prefix, new_test_engine_opt,
        new_test_engine_opt_with_custom_options, prepare_table_region, try_wait,
    },
};

#[test]
fn test_fts_trim_overbound() {
    ::test_util::init_log_for_test();
    let keyspace_id = KEYSPACE_ID;
    let table_id = 30;
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();

    // Build a schema file so shard metadata looks consistent.
    let schema = crate::table::columnar::tests::new_schema(table_id, false);
    let schema_file = put_schema_file(&engine, id(), &[&schema]);

    let l0_id = id();
    let l0_file = put_packed(
        &engine,
        l0_id,
        new_packed(l0_id, 500)
            .lp(table_id, 1, |d| {
                d(8, 220, false, "trim doc");
                d(11, 210, false, "trim doc");
                d(15, 205, true, "trim doc");
                d(20, 150, false, "trim doc");
                d(35, 120, false, "trim doc");
            })
            .lp(table_id + 1, 1, |d| {
                d(8, 220, false, "trim doc");
                d(11, 210, false, "trim doc");
                d(15, 205, true, "trim doc");
                d(20, 150, false, "trim doc");
                d(35, 120, false, "trim doc");
            }),
    );
    let ded_id = id();
    let l2_file = put_ded(
        &engine,
        ded_id,
        new_ded(ded_id).lp(table_id, 1, |d| {
            d(5, 300, false, "trim doc");
            d(6, 290, false, "trim doc");
        }),
    );
    let ded_id_1 = id();
    let l2_file_1 = put_ded(
        &engine,
        ded_id_1,
        new_ded(ded_id_1).lp(table_id + 1, 1, |d| {
            d(5, 300, false, "trim doc");
            d(6, 290, false, "trim doc");
        }),
    );
    let mut levels = FtsLevels::default();
    levels.track_index(table_id, 1);
    levels.mut_l0(|files| files.push(l0_file));
    levels.insert_l2_file(l2_file);
    levels.insert_l2_file(l2_file_1); // File is out of range and should be removed.
    // TODO: add more cases after suupport split & merge.

    let base_data = shard.get_data();
    let mut builder = ShardDataBuilder::new(base_data);
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file));
    builder.set_columnar_table_ids(vec![table_id]);
    builder.set_fts_levels(levels);
    shard.set_data(builder.build());
    shard
        .initial_flushed
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let shard = engine.get_shard(shard_id).unwrap();
    shard.pending_ops.write().unwrap().trim_over_bound = true;
    *shard.compaction_priority.write().unwrap() = Some(CompactionPriority::TrimOverBound);
    let id_ver = shard.id_ver();
    engine.trigger_compact(id_ver);

    let ok = try_wait(
        || {
            let data = shard.get_data();
            !data.fts_levels.l0().is_empty() && data.fts_levels.l0()[0].id() != l0_id
        },
        10,
    );
    assert!(ok, "fts trim over bound compaction failed");

    let levels = shard.get_data().fts_levels.clone();
    assert_eq!(levels.l2().len(), 1, "l2 files should be removed");
    let rewritten = &levels.l0()[0];
    assert!(
        rewritten.id() != l0_id,
        "packed file should be rewritten with new id"
    );
}

#[test]
fn test_fts_truncate_ts() {
    ::test_util::init_log_for_test();
    let keyspace_id = KEYSPACE_ID;
    let table_id = 40;
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();

    // Build a schema file so shard metadata looks consistent.
    let schema = crate::table::columnar::tests::new_schema(table_id, false);
    let schema_file = put_schema_file(&engine, id(), &[&schema]);

    shard
        .initial_flushed
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let l0_id = id();
    let l0_file = put_packed(
        &engine,
        l0_id,
        new_packed(l0_id, 1000).lp(table_id, 1, |d| {
            d(10, 300, false, "truncate doc");
            d(10, 100, false, "truncate doc");
            d(11, 220, true, "truncate doc");
            d(11, 150, false, "truncate doc");
            d(12, 90, false, "truncate doc");
        }),
    );

    let mut levels = FtsLevels::default();
    levels.track_index(table_id, 1);
    levels.mut_l0(|files| files.push(l0_file));

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file));
    builder.set_columnar_table_ids(vec![table_id]);
    builder.set_fts_levels(levels);
    shard.set_data(builder.build());

    shard.pending_ops.write().unwrap().truncate_ts = Some(175);
    *shard.compaction_priority.write().unwrap() = Some(CompactionPriority::TruncateTs);
    let id_ver = shard.id_ver();
    engine.trigger_compact(id_ver);

    let ok = try_wait(
        || {
            let data = shard.get_data();
            !data.fts_levels.l0().is_empty() && data.fts_levels.l0()[0].id() != l0_id
        },
        10,
    );
    assert!(ok, "fts truncate ts compaction failed");

    let rewritten = shard.get_data().fts_levels.l0()[0].clone();
    let handles = collect_packed_handles(&rewritten, table_id);
    assert_eq!(handles, vec![10, 11, 12]);
}

#[test]
fn test_fts_destroy_range() {
    ::test_util::init_log_for_test();
    let keyspace_id = KEYSPACE_ID;
    let table_id = 50;
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();

    // Build a schema file so shard metadata looks consistent.
    let schema = crate::table::columnar::tests::new_schema(table_id, false);
    let schema_file = put_schema_file(&engine, id(), &[&schema]);

    shard
        .initial_flushed
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let l0_id = id();
    let l0_file = put_packed(
        &engine,
        l0_id,
        new_packed(l0_id, 600).lp(table_id, 1, |d| {
            d(10, 200, false, "destroy doc");
            d(11, 180, true, "destroy doc");
            d(12, 170, false, "destroy doc");
            d(13, 160, false, "destroy doc");
        }),
    );
    let l2_id = id();
    let l2_file = put_ded(
        &engine,
        l2_id,
        new_ded(l2_id).lp(table_id, 1, |d| {
            d(20, 200, false, "destroy doc");
            d(21, 180, true, "destroy doc");
            d(22, 170, false, "destroy doc");
            d(23, 160, false, "destroy doc");
        }),
    );
    let mut levels = FtsLevels::default();
    levels.track_index(table_id, 1);
    levels.mut_l0(|files| files.push(l0_file));
    levels.insert_l2_file(l2_file);
    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file));
    builder.set_columnar_table_ids(vec![table_id]);
    builder.set_fts_levels(levels);
    shard.set_data(builder.build());

    let mut del_prefixes = DeletePrefixes::new_with_keyspace_id(keyspace_id);
    del_prefixes.merge_prefix_in_place(&outer_row_key(keyspace_id, table_id, 12));
    del_prefixes.merge_prefix_in_place(&outer_row_key(keyspace_id, table_id, 22));
    shard.pending_ops.write().unwrap().del_prefixes = Arc::new(del_prefixes.clone());
    shard.set_property(DEL_PREFIXES_KEY, &del_prefixes.marshal());
    *shard.compaction_priority.write().unwrap() = Some(CompactionPriority::DestroyRange);
    let id_ver = shard.id_ver();
    engine.trigger_compact(id_ver);

    let ok = try_wait(
        || {
            let data = shard.get_data();
            !data.fts_levels.l0().is_empty()
                && data.fts_levels.l0()[0].id() != l0_id
                && data
                    .fts_levels
                    .l2()
                    .iter()
                    .any(|(_, files)| files.iter().any(|f| f.id() != l2_id))
        },
        10,
    );
    assert!(ok, "fts destroy range compaction failed");

    let rewritten_l0 = shard.get_data().fts_levels.l0()[0].clone();
    let handles = collect_packed_handles(&rewritten_l0, table_id);
    assert_eq!(handles, vec![10, 11, 13]);

    let rewritten_l2 = shard
        .get_data()
        .fts_levels
        .l2()
        .iter()
        .next()
        .unwrap()
        .1
        .first()
        .unwrap()
        .clone();
    let handles = collect_dedicated_handles(&rewritten_l2);
    assert_eq!(handles, vec![20, 21, 23]);
}

#[test]
fn test_fts_intra_l2() {
    ::test_util::init_log_for_test();
    let keyspace_id = KEYSPACE_ID;
    let table_id = 60;
    let index_id = 5;
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();

    // Build a schema file so shard metadata looks consistent.
    let schema = crate::table::columnar::tests::new_schema(table_id, false);
    let schema_file = put_schema_file(&engine, id(), &[&schema]);
    shard
        .initial_flushed
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let lp_key_bytes = lp_key(table_id, index_id).to_vec();

    // Prepare multiple dedicated files for the same logical partition.
    let expected_docs = 5;

    let file_id_a = id();
    let file_a = put_ded(
        &engine,
        file_id_a,
        new_ded(file_id_a).lp(table_id, index_id, |d| {
            d(3, 120, false, "doc_3_put");
            d(5, 80, false, "doc_5_put");
        }),
    );

    let file_id_b = id();
    let file_b = put_ded(
        &engine,
        file_id_b,
        new_ded(file_id_b).lp(table_id, index_id, |d| {
            d(3, 110, true, "doc_3_del");
            d(6, 90, false, "doc_6_put");
        }),
    );

    let file_id_c = id();
    let file_c = put_ded(
        &engine,
        file_id_c,
        new_ded(file_id_c).lp(table_id, index_id, |d| {
            d(7, 70, false, "doc_7_put");
        }),
    );

    let mut levels = FtsLevels::default();
    levels.track_index(table_id, index_id);
    levels.insert_l2_file(file_a);
    levels.insert_l2_file(file_b);
    levels.insert_l2_file(file_c);

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file));
    builder.set_columnar_table_ids(vec![table_id]);
    builder.set_fts_levels(levels);
    shard.set_data(builder.build());

    let mut expected_ids = vec![file_id_a, file_id_b, file_id_c];
    expected_ids.sort_unstable();
    let priority = shard
        .test_get_fts_l2_compact_priority()
        .expect("l2 compaction priority must exist");
    let source_ids = match &priority {
        CompactionPriority::FtsCompactL2 { lp_key, file_ids } => {
            assert_eq!(lp_key.as_slice(), lp_key_bytes.as_slice());
            let mut picked = file_ids.clone();
            picked.sort_unstable();
            assert_eq!(picked, expected_ids);
            file_ids.clone()
        }
        other => panic!("unexpected priority {:?}", other),
    };
    *shard.compaction_priority.write().unwrap() = Some(priority);

    let id_ver = shard.id_ver();
    engine.trigger_compact(id_ver);

    let merged_ready = try_wait(
        || {
            let data = shard.get_data();
            let Some(files) = data.fts_levels.l2().get(&lp_key_bytes) else {
                return false;
            };
            if files.len() != 1 {
                return false;
            }
            let new_id = files[0].id();
            !source_ids.contains(&new_id) && files[0].props().get_pk_total() == expected_docs
        },
        20,
    );
    assert!(
        merged_ready,
        "fts intra-l2 compaction should output a single merged file"
    );

    let data = shard.get_data();
    let merged_file = data
        .fts_levels
        .l2()
        .get(&lp_key_bytes)
        .and_then(|files| files.first())
        .cloned()
        .expect("merged file should exist");

    let rows = collect_dedicated_rows(&merged_file);
    assert_eq!(
        rows,
        vec![
            (3, 120, false),
            (3, 110, true),
            (5, 80, false),
            (6, 90, false),
            (7, 70, false),
        ]
    );

    let doc3_hits = search_l2(&merged_file, "doc_3_put");
    assert!(
        doc3_hits
            .iter()
            .all(|doc_id| *doc_id < expected_docs as u32),
        "merged L2 should contain searchable docs from both source files"
    );

    let missing_hits = search_l2(&merged_file, "nonexistent");
    assert!(missing_hits.is_empty(), "unrelated terms should not match");
}

#[test]
fn test_fts_create_l0() {
    ::test_util::init_log_for_test();
    let keyspace_id = KEYSPACE_ID;
    let table_id = 80;
    let index_id = 6;
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();

    let schema = SchemaBuilder::<IntPk>::new(table_id)
        .column(1, FieldTypeTp::LongLong)
        .column(2, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id,
            col_id: 2,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file = put_schema_file(&engine, id(), &[&schema]);

    let snap_version = 500;
    let columnar_file_id = id();
    let columnar_file = put_columnar(
        &engine,
        new_columnar(columnar_file_id, snap_version).table::<IntPk>(&schema, |row| {
            row(1, 100, false, |datum| {
                datum(1);
                datum("Who knew the people would adore me so much?");
            });
            row(2, 200, false, |datum| {
                datum(2);
                datum("Being too popular can be such a hassle");
            });
            row(3, 300, false, |datum| {
                datum(3);
                datum("The world is but a stage.");
            });
        }),
    );
    let mut col_levels = ColumnarLevels::new();
    col_levels.add_file(0, columnar_file);

    let mut fts_levels = FtsLevels::default();
    fts_levels.track_index(table_id, index_id);

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file));
    builder.set_columnar_levels(col_levels);
    builder.set_columnar_table_ids(vec![table_id]);
    builder.set_fts_levels(fts_levels);
    shard.set_data(builder.build());
    shard
        .initial_flushed
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let priority = match shard
        .test_get_fts_incremental_update_priority()
        .expect("incremental FTS priority should exist")
    {
        CompactionPriority::FtsCreateL0 {
            col_file_ids,
            active_tracked_indexes,
            snap_version: ver,
        } => {
            assert_eq!(col_file_ids, vec![columnar_file_id]);
            assert_eq!(active_tracked_indexes, vec![(table_id, index_id)]);
            assert_eq!(ver, snap_version.into());
            CompactionPriority::FtsCreateL0 {
                col_file_ids,
                active_tracked_indexes,
                snap_version: ver,
            }
        }
        other => panic!("unexpected priority {:?}", other),
    };
    *shard.compaction_priority.write().unwrap() = Some(priority);
    let id_ver = shard.id_ver();
    engine.trigger_compact(id_ver);

    let ok = try_wait(|| !shard.get_data().fts_levels.l0().is_empty(), 20);
    assert!(ok, "fts create L0 compaction failed");

    let data = shard.get_data();
    assert_eq!(data.fts_levels.l1().len(), 0);
    assert_eq!(data.fts_levels.l0().len(), 1);
    assert_eq!(
        data.fts_levels.l0_snap_version,
        SnapVersion::from(snap_version)
    );

    let l0_file = data.fts_levels.l0()[0].clone();
    let mut hits = search_packed(&l0_file, table_id, index_id, "the");
    hits.sort_unstable();
    assert_eq!(hits, vec![0, 2], "unexpected doc hits for 'the'");

    let none_hits = search_packed(&l0_file, table_id, index_id, "nonexistent");
    assert!(none_hits.is_empty(), "unexpected hits for missing term");
}

#[test]
fn test_fts_l0_compaction() {
    ::test_util::init_log_for_test();
    let keyspace_id = KEYSPACE_ID;
    let table_id = 90;
    let index_id = 7;
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();

    let schema = SchemaBuilder::<IntPk>::new(table_id)
        .column(1, FieldTypeTp::LongLong)
        .column(2, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id,
            col_id: 2,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file = put_schema_file(&engine, id(), &[&schema]);

    let batches: Vec<Vec<&str>> = vec![
        vec!["alpha apple", "alpha banana"],
        vec!["beta cherry", "beta date"],
        vec!["gamma elderberry", "gamma fig"],
        vec!["delta grape", "delta honey"],
    ];
    let mut l0_files = Vec::new();
    let mut expected_ids = Vec::new();
    for (idx, docs) in batches.into_iter().enumerate() {
        let file_id = id();
        let file = put_packed(
            &engine,
            file_id,
            new_packed(file_id, 1000 + idx as u64).lp(table_id, index_id, |d| {
                for (doc_idx, body) in docs.into_iter().enumerate() {
                    d((idx * 100 + doc_idx) as i64, 1000, false, body);
                }
            }),
        );
        expected_ids.push(file_id);
        l0_files.push(file);
    }

    let mut fts_levels = FtsLevels::default();
    fts_levels.track_index(table_id, index_id);
    fts_levels.mut_l0(|files| files.extend(l0_files));

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file));
    builder.set_columnar_table_ids(vec![table_id]);
    builder.set_fts_levels(fts_levels);
    shard.set_data(builder.build());
    shard
        .initial_flushed
        .store(true, std::sync::atomic::Ordering::SeqCst);

    *shard.compaction_priority.write().unwrap() = Some(CompactionPriority::FtsCompactL0);
    engine.trigger_compact(shard.id_ver());

    let ok = try_wait(
        || {
            let data = shard.get_data();
            data.fts_levels.l0().is_empty() && data.fts_levels.l1().len() == 1
        },
        30,
    );
    assert!(ok, "fts l0->l1 compaction failed");

    let data = shard.get_data();
    assert!(
        data.fts_levels.l0().is_empty(),
        "fts l0 compaction should clear all L0 files"
    );
    assert_eq!(
        data.fts_levels.l1().len(),
        1,
        "fts l0 compaction should output exactly one L1 file"
    );
    let l1_file = data.fts_levels.l1()[0].clone();
    assert!(
        !expected_ids.contains(&l1_file.id()),
        "compaction should output new file id"
    );

    let search = |term: &str| {
        let mut hits = search_packed(&l1_file, table_id, index_id, term);
        hits.sort_unstable();
        hits
    };

    assert_eq!(search("alpha").len(), 2, "alpha should hit two docs");
    assert_eq!(search("beta").len(), 2, "beta should hit two docs");
    assert_eq!(search("banana").len(), 1, "banana should hit once");
    assert_eq!(search("cherry").len(), 1, "cherry should hit once");
    assert!(
        search("nonexistent").is_empty(),
        "unexpected hits for missing term"
    );
}

#[test]
fn test_fts_l0_compaction_skips_untracked_indexes() {
    ::test_util::init_log_for_test();
    let keyspace_id = KEYSPACE_ID;
    let table_id = 92;
    let tracked_index = 11;
    let untracked_index = 12;
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();

    let schema = SchemaBuilder::<IntPk>::new(table_id)
        .column(1, FieldTypeTp::LongLong)
        .column(2, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id: tracked_index,
            col_id: 2,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .fts_index(FullTextIndexDef {
            index_id: untracked_index,
            col_id: 2,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file = put_schema_file(&engine, id(), &[&schema]);

    let file_id = id();
    let file = put_packed(
        &engine,
        file_id,
        new_packed(file_id, 600)
            .lp(table_id, tracked_index, |d| {
                d(1, 500, false, "tracked alpha");
                d(2, 500, false, "tracked beta");
            })
            .lp(table_id, untracked_index, |d| {
                d(100, 500, false, "untracked apple");
                d(200, 500, false, "untracked grape");
            }),
    );
    let mut fts_levels = FtsLevels::default();
    fts_levels.track_index(table_id, tracked_index);
    fts_levels.mut_l0(|files| files.push(file));

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file));
    builder.set_columnar_table_ids(vec![table_id]);
    builder.set_fts_levels(fts_levels);
    shard.set_data(builder.build());
    shard
        .initial_flushed
        .store(true, std::sync::atomic::Ordering::SeqCst);

    *shard.compaction_priority.write().unwrap() = Some(CompactionPriority::FtsCompactL0);
    engine.trigger_compact(shard.id_ver());

    let ok = try_wait(
        || {
            let data = shard.get_data();
            data.fts_levels.l0().is_empty() && data.fts_levels.l1().len() == 1
        },
        30,
    );
    assert!(ok, "fts l0 compaction failed");

    let data = shard.get_data();
    let l1_file = data.fts_levels.l1()[0].clone();
    let mut tracked_hits = search_packed(&l1_file, table_id, tracked_index, "tracked");
    tracked_hits.sort_unstable();
    assert_eq!(tracked_hits.len(), 2, "tracked docs should remain");

    let untracked_hits = search_packed(&l1_file, table_id, untracked_index, "untracked");
    assert!(
        untracked_hits.is_empty(),
        "untracked index should be dropped from L1 output"
    );

    // Ensure no remaining L0 files
    assert!(data.fts_levels.l0().is_empty());
}

#[test]
fn test_fts_schema_drop_makes_l0_compaction_drop_data() {
    ::test_util::init_log_for_test();
    let keyspace_id = KEYSPACE_ID;
    let table_id = 93;
    let kept_index = 16;
    let dropped_index = 17;
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();

    let schema_with_two = SchemaBuilder::<IntPk>::new(table_id)
        .column(1, FieldTypeTp::LongLong)
        .column(2, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id: kept_index,
            col_id: 2,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .fts_index(FullTextIndexDef {
            index_id: dropped_index,
            col_id: 2,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file_with_two = put_schema_file(&engine, id(), &[&schema_with_two]);

    let l0_id = id();
    let l0 = put_packed(
        &engine,
        l0_id,
        new_packed(l0_id, 600)
            .lp(table_id, kept_index, |d| {
                d(1, 500, false, "kept alpha");
                d(2, 500, false, "kept beta");
            })
            .lp(table_id, dropped_index, |d| {
                d(100, 500, false, "dropped apple");
                d(200, 500, false, "dropped grape");
            }),
    );

    let mut fts_levels = FtsLevels::default();
    fts_levels.track_index(table_id, kept_index);
    fts_levels.track_index(table_id, dropped_index);
    fts_levels.mut_l0(|files| files.push(l0));

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(
        schema_file_with_two.get_version(),
        0,
        Some(schema_file_with_two),
    );
    builder.set_columnar_table_ids(vec![table_id]);
    builder.set_fts_levels(fts_levels);
    shard.set_data(builder.build());
    shard
        .initial_flushed
        .store(true, std::sync::atomic::Ordering::SeqCst);

    // Drop one index from schema; it should become untracked and thus be dropped by
    // compaction.
    let schema_with_one = SchemaBuilder::<IntPk>::new(table_id)
        .column(1, FieldTypeTp::LongLong)
        .column(2, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id: kept_index,
            col_id: 2,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file_with_one = put_schema_file(&engine, id(), &[&schema_with_one]);

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(
        schema_file_with_one.get_version(),
        0,
        Some(schema_file_with_one),
    );
    shard.set_data(builder.build());

    assert!(
        shard
            .get_data()
            .fts_levels
            .has_tracked_index(table_id, kept_index),
        "kept index should remain tracked"
    );
    assert!(
        shard
            .get_data()
            .fts_levels
            .has_tracked_index(table_id, dropped_index),
        "dropped index is still tracked until FtsDropIndex is applied"
    );

    shard.refresh_states();
    let priority = shard
        .get_compaction_priority()
        .expect("fts drop index priority expected");
    match &priority {
        CompactionPriority::FtsDropIndex { dropped_indexes } => {
            assert_eq!(dropped_indexes, &vec![(table_id, dropped_index)]);
        }
        other => panic!("unexpected priority {:?}", other),
    }
    engine.trigger_compact(shard.id_ver());

    let ok = try_wait(
        || {
            let data = shard.get_data();
            data.fts_levels.has_tracked_index(table_id, kept_index)
                && !data.fts_levels.has_tracked_index(table_id, dropped_index)
        },
        20,
    );
    assert!(ok, "fts drop index did not finish in time");

    *shard.compaction_priority.write().unwrap() = Some(CompactionPriority::FtsCompactL0);
    engine.trigger_compact(shard.id_ver());

    let ok = try_wait(
        || {
            let data = shard.get_data();
            data.fts_levels.l0().is_empty() && data.fts_levels.l1().len() == 1
        },
        30,
    );
    assert!(ok, "fts l0 compaction failed");

    let l1_file = shard.get_data().fts_levels.l1()[0].clone();
    let kept_hits = search_packed(&l1_file, table_id, kept_index, "kept");
    assert!(!kept_hits.is_empty(), "kept index data should remain");

    let dropped_hits = search_packed(&l1_file, table_id, dropped_index, "dropped");
    assert!(
        dropped_hits.is_empty(),
        "dropped index data should be removed by compaction"
    );
}

#[test]
fn test_fts_cleanup_compaction_removes_orphans() {
    ::test_util::init_log_for_test();
    let keyspace_id = KEYSPACE_ID;
    let table_id = 95;
    let tracked_index = 13;
    let orphan_index = 14;
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();

    let schema = SchemaBuilder::<IntPk>::new(table_id)
        .column(1, FieldTypeTp::LongLong)
        .column(2, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id: tracked_index,
            col_id: 2,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file = put_schema_file(&engine, id(), &[&schema]);

    let tracked_l1_id = id();
    let tracked_l1 = put_packed(
        &engine,
        tracked_l1_id,
        new_packed(tracked_l1_id, 0).lp(table_id, tracked_index, |d| {
            d(1, 800, false, "tracked apple");
            d(2, 800, false, "tracked berry");
        }),
    );
    let orphan_l1_id = id();
    let orphan_l1 = put_packed(
        &engine,
        orphan_l1_id,
        new_packed(orphan_l1_id, 0).lp(table_id, orphan_index, |d| {
            d(100, 800, false, "orphan citrus");
            d(200, 800, false, "orphan date");
        }),
    );
    let tracked_l2_id = id();
    let tracked_l2 = put_ded(
        &engine,
        tracked_l2_id,
        new_ded(tracked_l2_id).lp(table_id, tracked_index, |d| {
            d(1, 800, false, "tracked apple");
            d(2, 800, false, "tracked berry");
        }),
    );
    let orphan_l2_id = id();
    let orphan_l2 = put_ded(
        &engine,
        orphan_l2_id,
        new_ded(orphan_l2_id).lp(table_id, orphan_index, |d| {
            d(100, 800, false, "orphan citrus");
            d(200, 800, false, "orphan date");
        }),
    );

    let mut fts_levels = FtsLevels::default();
    fts_levels.track_index(table_id, tracked_index);
    fts_levels.mut_l1(|files| {
        files.push(tracked_l1);
        files.push(orphan_l1);
    });
    fts_levels.insert_l2_file(tracked_l2);
    fts_levels.insert_l2_file(orphan_l2);

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file));
    builder.set_columnar_table_ids(vec![table_id]);
    builder.set_fts_levels(fts_levels);
    shard.set_data(builder.build());
    shard
        .initial_flushed
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let priority = shard
        .test_get_fts_cleanup_priority()
        .expect("cleanup priority expected");
    let (cleanup_l1, cleanup_l2) = match &priority {
        CompactionPriority::FtsCleanup {
            l1_file_ids,
            l2_file_ids,
        } => (l1_file_ids.clone(), l2_file_ids.clone()),
        _ => panic!("unexpected priority {:?}", priority),
    };
    assert!(
        cleanup_l1.contains(&orphan_l1_id),
        "orphan L1 file should be scheduled for cleanup"
    );
    assert!(
        cleanup_l2.contains(&orphan_l2_id),
        "orphan L2 file should be scheduled for cleanup"
    );
    assert!(
        !cleanup_l1.contains(&tracked_l1_id),
        "tracked L1 file should not be removed"
    );

    *shard.compaction_priority.write().unwrap() = Some(priority);
    engine.trigger_compact(shard.id_ver());

    let ok = try_wait(
        || {
            let data = shard.get_data();
            let l1_ids: Vec<u64> = data.fts_levels.l1().iter().map(|f| f.id()).collect();
            let l2_ids: Vec<u64> = data
                .fts_levels
                .l2()
                .values()
                .flat_map(|files| files.iter().map(|f| f.id()))
                .collect();
            l1_ids == vec![tracked_l1_id] && l2_ids == vec![tracked_l2_id]
        },
        20,
    );
    assert!(ok, "fts cleanup compaction did not finish in time");

    let data = shard.get_data();
    assert_eq!(
        data.fts_levels.l1().len(),
        1,
        "cleanup should keep exactly one L1 file"
    );
    let remaining_l1 = data.fts_levels.l1()[0].clone();
    let mut hits = search_packed(&remaining_l1, table_id, tracked_index, "tracked");
    hits.sort_unstable();
    assert_eq!(hits.len(), 2, "tracked index data should remain searchable");
    let l2_ids: Vec<u64> = data
        .fts_levels
        .l2()
        .values()
        .flat_map(|files| files.iter().map(|f| f.id()))
        .collect();
    assert_eq!(
        l2_ids,
        vec![tracked_l2_id],
        "cleanup should retain only tracked L2 file"
    );
}

#[test]
fn test_fts_schema_drop_untracks_and_cleans_files() {
    ::test_util::init_log_for_test();
    let keyspace_id = KEYSPACE_ID;
    let table_id = 96;
    let index_id = 15;
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();

    let schema_with_fts = SchemaBuilder::<IntPk>::new(table_id)
        .column(1, FieldTypeTp::LongLong)
        .column(2, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id,
            col_id: 2,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file_with_fts = put_schema_file(&engine, id(), &[&schema_with_fts]);

    let l1_id = id();
    let l1 = put_packed(
        &engine,
        l1_id,
        new_packed(l1_id, 0).lp(table_id, index_id, |d| {
            d(1, 800, false, "tracked apple");
            d(2, 800, false, "tracked berry");
        }),
    );
    let l2_id = id();
    let l2 = put_ded(
        &engine,
        l2_id,
        new_ded(l2_id).lp(table_id, index_id, |d| {
            d(1, 800, false, "tracked apple");
            d(2, 800, false, "tracked berry");
        }),
    );

    let mut fts_levels = FtsLevels::default();
    fts_levels.track_index(table_id, index_id);
    fts_levels.mut_l1(|files| files.push(l1));
    fts_levels.insert_l2_file(l2);

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(
        schema_file_with_fts.get_version(),
        0,
        Some(schema_file_with_fts),
    );
    builder.set_columnar_table_ids(vec![table_id]);
    builder.set_fts_levels(fts_levels);
    shard.set_data(builder.build());
    shard
        .initial_flushed
        .store(true, std::sync::atomic::Ordering::SeqCst);

    // Drop the fulltext index from schema.
    let schema_without_fts = SchemaBuilder::<IntPk>::new(table_id)
        .column(1, FieldTypeTp::LongLong)
        .column(2, FieldTypeTp::String)
        .schema();
    let schema_file_without_fts = put_schema_file(&engine, id(), &[&schema_without_fts]);

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(
        schema_file_without_fts.get_version(),
        0,
        Some(schema_file_without_fts),
    );
    shard.set_data(builder.build());

    assert!(
        shard
            .get_data()
            .fts_levels
            .has_tracked_index(table_id, index_id),
        "tracked index is still tracked until FtsDropIndex is applied"
    );

    shard.refresh_states();
    let priority = shard
        .get_compaction_priority()
        .expect("fts drop index priority expected");
    match &priority {
        CompactionPriority::FtsDropIndex { dropped_indexes } => {
            assert_eq!(dropped_indexes, &vec![(table_id, index_id)]);
        }
        other => panic!("unexpected priority {:?}", other),
    }
    engine.trigger_compact(shard.id_ver());

    let ok = try_wait(
        || {
            let data = shard.get_data();
            !data.fts_levels.has_tracked_index(table_id, index_id)
        },
        20,
    );
    assert!(ok, "fts drop index did not finish in time");

    let priority = shard
        .test_get_fts_cleanup_priority()
        .expect("cleanup priority expected after schema drop");
    let (cleanup_l1, cleanup_l2) = match &priority {
        CompactionPriority::FtsCleanup {
            l1_file_ids,
            l2_file_ids,
        } => (l1_file_ids.clone(), l2_file_ids.clone()),
        _ => panic!("unexpected priority {:?}", priority),
    };
    assert!(
        cleanup_l1.contains(&l1_id),
        "L1 file for dropped index should be scheduled for cleanup"
    );
    assert!(
        cleanup_l2.contains(&l2_id),
        "L2 file for dropped index should be scheduled for cleanup"
    );

    *shard.compaction_priority.write().unwrap() = Some(priority);
    engine.trigger_compact(shard.id_ver());

    let ok = try_wait(
        || {
            let data = shard.get_data();
            data.fts_levels.l1().is_empty() && data.fts_levels.l2().is_empty()
        },
        20,
    );
    assert!(ok, "fts cleanup after schema drop did not finish in time");
}

#[test]
fn test_fts_add_index() {
    ::test_util::init_log_for_test();
    let keyspace_id = KEYSPACE_ID;
    let table_id = 91;
    let index_id = 8;
    let (engine, apply_tx) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();

    let schema = SchemaBuilder::<IntPk>::new(table_id)
        .column(1, FieldTypeTp::LongLong)
        .column(2, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id,
            col_id: 2,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file_id = engine.id_allocator.alloc_id(1).unwrap()[0];
    let schema_file = put_schema_file(&engine, schema_file_id, &[&schema]);

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file));
    builder.set_columnar_table_ids(vec![table_id]);
    builder.set_fts_levels(FtsLevels::default());
    shard.set_data(builder.build());
    shard
        .initial_flushed
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let priority = shard
        .test_get_fts_add_index_priority()
        .expect("fts add index priority should exist");
    match &priority {
        CompactionPriority::FtsAddIndex { new_indexes } => {
            assert_eq!(new_indexes, &vec![(table_id, index_id)]);
        }
        other => panic!("unexpected priority {:?}", other),
    }

    // Simulate compaction: track the index and ensure the state reflects it.
    *shard.compaction_priority.write().unwrap() = Some(priority);
    engine.trigger_compact(shard.id_ver());

    let ok = try_wait(
        || {
            shard
                .get_data()
                .fts_levels
                .has_tracked_index(table_id, index_id)
        },
        20,
    );
    assert!(ok, "fts add index should track the new index");
    assert!(
        shard.test_get_fts_add_index_priority().is_none(),
        "priority should disappear after index is tracked"
    );
}

#[test]
fn test_fts_l0_compaction_promote_l2() {
    ::test_util::init_log_for_test();
    let keyspace_id = KEYSPACE_ID;
    let table_id = 95;
    let index_id = 9;
    let (engine, apply_tx) =
        new_test_engine_opt_with_custom_options(true, DEF_BLOCK_SIZE, "", |opts| {
            opts.fts_build_options.max_l0_files = 1;
            // Ensure L1->L2 promotion relies on LP serialized size (regression coverage for
            // EPackedFileLp::serialized_size).
            opts.fts_build_options.min_l2_lp_size = ReadableSize(100);
        });
    let id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();

    let schema = SchemaBuilder::<IntPk>::new(table_id)
        .column(1, FieldTypeTp::LongLong)
        .column(2, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id,
            col_id: 2,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file = put_schema_file(&engine, id(), &[&schema]);

    let l0_id = id();
    let l0_file = put_packed(
        &engine,
        l0_id,
        new_packed(l0_id, 2000).lp(table_id, index_id, |d| {
            for i in 0..16 {
                d(i, 2000, false, format!("promote payload {i}"));
            }
        }),
    );
    let l0_lp_size = block_on(async {
        let mut iter = l0_file.lp_iter().unwrap();
        iter.next_lp().await.unwrap().unwrap().serialized_size()
    });
    assert!(
        l0_lp_size >= 100,
        "test setup: expected LP size >= 100, got {}",
        l0_lp_size
    );

    let mut fts_levels = FtsLevels::default();
    fts_levels.track_index(table_id, index_id);
    fts_levels.mut_l0(|files| files.push(l0_file));

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file.clone()));
    builder.set_columnar_table_ids(vec![table_id]);
    builder.set_fts_levels(fts_levels);
    shard.set_data(builder.build());
    shard
        .initial_flushed
        .store(true, std::sync::atomic::Ordering::SeqCst);

    *shard.compaction_priority.write().unwrap() = Some(CompactionPriority::FtsCompactL0);
    engine.trigger_compact(shard.id_ver());

    let ok = try_wait(
        || {
            let data = shard.get_data();
            data.fts_levels.l0().is_empty()
                && (!data.fts_levels.l1().is_empty()
                    || data.fts_levels.l2().values().any(|files| !files.is_empty()))
        },
        30,
    );
    assert!(ok, "fts L0 compaction did not finish");

    let mut current_data = shard.get_data();
    assert!(
        current_data.fts_levels.l1().is_empty(),
        "expected L0 compaction to promote to L2, but got L1 files"
    );
    let initial_l2_files: Vec<EDedicatedFile> = current_data
        .fts_levels
        .l2()
        .values()
        .flat_map(|files| files.iter().cloned())
        .collect();
    assert_eq!(initial_l2_files.len(), 1);
    let promote_hits: usize = initial_l2_files
        .iter()
        .map(|file| search_l2(file, "promote").len())
        .sum();
    assert_eq!(promote_hits, 16, "promote docs should move to L2");

    let follow_id = id();
    let follow_file = put_packed(
        &engine,
        follow_id,
        new_packed(follow_id, 3000).lp(table_id, index_id, |d| {
            for i in 0..8 {
                d(
                    1_000 + i as i64,
                    3000,
                    false,
                    format!("followup payload {i}"),
                );
            }
        }),
    );

    let mut levels = (*current_data.fts_levels).clone();
    levels.mut_l0(|files| files.push(follow_file));
    let mut builder = ShardDataBuilder::new(current_data);
    builder.set_fts_levels(levels);
    shard.set_data(builder.build());

    *shard.compaction_priority.write().unwrap() = Some(CompactionPriority::FtsCompactL0);
    engine.trigger_compact(shard.id_ver());

    let ok = try_wait(
        || {
            let data = shard.get_data();
            data.fts_levels.l0().is_empty()
                && (!data.fts_levels.l1().is_empty()
                    || data.fts_levels.l2().values().any(|files| files.len() >= 2))
        },
        30,
    );
    assert!(ok, "fts follow-up compaction failed");

    current_data = shard.get_data();
    assert!(current_data.fts_levels.l1().is_empty());

    let all_l2_files: Vec<EDedicatedFile> = current_data
        .fts_levels
        .l2()
        .values()
        .flat_map(|files| files.iter().cloned())
        .collect();
    assert!(
        all_l2_files.len() >= 2,
        "expected multiple L2 files after follow-up"
    );
    for prev in &initial_l2_files {
        assert!(
            all_l2_files.iter().any(|file| file.id() == prev.id()),
            "original promoted file should remain"
        );
    }

    let search_all = |term: &str| -> usize {
        all_l2_files
            .iter()
            .map(|file| search_l2(file, term).len())
            .sum()
    };

    assert_eq!(search_all("promote"), 16, "promote docs should persist");
    assert_eq!(
        search_all("followup"),
        8,
        "followup docs should be searchable"
    );
}

#[test]
fn test_fts_l0_compaction_no_promotion_below_l2_threshold() {
    ::test_util::init_log_for_test();
    let keyspace_id = KEYSPACE_ID;
    let table_id = 96;
    let index_id = 10;
    let min_l2_lp_size = ReadableSize::mb(64);
    let (engine, apply_tx) =
        new_test_engine_opt_with_custom_options(true, DEF_BLOCK_SIZE, "", |opts| {
            opts.fts_build_options.max_l0_files = 1;
            opts.fts_build_options.min_l2_lp_size = min_l2_lp_size;
        });
    let id = || engine.id_allocator.alloc_id(1).unwrap()[0];
    let shard_id = prepare_table_region(&engine, &apply_tx, keyspace_id, table_id);
    let shard = engine.get_shard(shard_id).unwrap();

    let schema = SchemaBuilder::<IntPk>::new(table_id)
        .column(1, FieldTypeTp::LongLong)
        .column(2, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id,
            col_id: 2,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file = put_schema_file(&engine, id(), &[&schema]);

    let l0_id = id();
    let l0_file = put_packed(
        &engine,
        l0_id,
        new_packed(l0_id, 4000).lp(table_id, index_id, |d| {
            for i in 0..2 {
                d(i, 4000, false, format!("stayl1 payload {i}"));
            }
        }),
    );
    let l0_lp_size = block_on(async {
        let mut iter = l0_file.lp_iter().unwrap();
        iter.next_lp().await.unwrap().unwrap().serialized_size()
    });
    let min_l2_lp_size_usize: usize = min_l2_lp_size.0.try_into().unwrap();
    assert!(
        l0_lp_size < min_l2_lp_size_usize,
        "test setup: expected LP size < {}, got {l0_lp_size}",
        min_l2_lp_size.0
    );

    let mut fts_levels = FtsLevels::default();
    fts_levels.track_index(table_id, index_id);
    fts_levels.mut_l0(|files| files.push(l0_file));

    let mut builder = ShardDataBuilder::new(shard.get_data());
    builder.set_schema(schema_file.get_version(), 0, Some(schema_file.clone()));
    builder.set_columnar_table_ids(vec![table_id]);
    builder.set_fts_levels(fts_levels);
    shard.set_data(builder.build());
    shard
        .initial_flushed
        .store(true, std::sync::atomic::Ordering::SeqCst);

    *shard.compaction_priority.write().unwrap() = Some(CompactionPriority::FtsCompactL0);
    engine.trigger_compact(shard.id_ver());

    let ok = try_wait(
        || {
            let data = shard.get_data();
            data.fts_levels.l0().is_empty()
                && (!data.fts_levels.l1().is_empty()
                    || data.fts_levels.l2().values().any(|files| !files.is_empty()))
        },
        30,
    );
    assert!(ok, "fts L0 compaction did not finish");

    let data = shard.get_data();
    assert!(
        data.fts_levels.l2().values().all(|files| files.is_empty()),
        "expected no L2 promotion when below threshold"
    );
    assert!(
        !data.fts_levels.l1().is_empty(),
        "expected L1 outputs when not promoted to L2"
    );
    let stayl1_hits: usize = data
        .fts_levels
        .l1()
        .iter()
        .map(|file| search_packed(file, table_id, index_id, "stayl1").len())
        .sum();
    assert_eq!(stayl1_hits, 2, "stayl1 docs should be searchable in L1");
}

fn collect_packed_handles(file: &PackedFile, table_id: i64) -> Vec<i64> {
    block_on(async {
        let mut iter = file.lp_iter().unwrap();
        let mut handles = Vec::new();
        while let Some(lp) = iter.next_lp().await.unwrap() {
            let props = lp.props();
            if props.get_table_id() != table_id || !props.get_is_int_handle() {
                continue;
            }
            let lp_int = lp.as_int_lp().unwrap();
            let mut pk_iter = lp_int.pk_iter().unwrap();
            while let Some((_doc_id, pk, ..)) = pk_iter.next().await.unwrap() {
                handles.push(IntPk::decode(pk.as_ref()).unwrap());
            }
        }
        handles.sort_unstable();
        handles
    })
}

fn collect_dedicated_handles(file: &EDedicatedFile) -> Vec<i64> {
    block_on(async {
        let file = file.as_int().unwrap();
        let mut iter = file.pk_iter().unwrap();
        let mut handles = Vec::new();
        while let Some((_doc_id, pk, ..)) = iter.next().await.unwrap() {
            handles.push(IntPk::decode(pk.as_ref()).unwrap());
        }
        handles.sort_unstable();
        handles
    })
}

fn collect_dedicated_rows(file: &EDedicatedFile) -> Vec<(i64, u64, bool)> {
    block_on(async {
        let file = file.as_int().unwrap();
        let mut iter = file.pk_iter().unwrap();
        let mut rows = Vec::new();
        while let Some((_doc_id, pk, version, deleted)) = iter.next().await.unwrap() {
            rows.push((IntPk::decode(pk.as_ref()).unwrap(), version, deleted != 0));
        }
        rows
    })
}

fn put_schema_file(engine: &TestEngine, file_id: u64, schemas: &[&Schema]) -> SchemaFile {
    let schema_version = engine.id_allocator.alloc_id(1).unwrap()[0] as i64;
    let schema_vec: Vec<Schema> = schemas.iter().map(|schema| (*schema).clone()).collect();
    let schema_file_data = build_schema_file(KEYSPACE_ID, schema_version, schema_vec, 0);
    let schema_raw = Arc::new(InMemFile::new(file_id, Bytes::from(schema_file_data)));
    let fs = engine.fs.clone();
    fs.get_runtime()
        .block_on(fs.create(
            schema_raw.id(),
            schema_raw.read(0, schema_raw.size() as usize).unwrap(),
            dfs::Options::default().with_type(FileType::Schema),
        ))
        .unwrap();
    SchemaFile::open(schema_raw).unwrap()
}

fn put_columnar(engine: &TestEngine, builder: TestColumnarFileBuilder) -> ColumnarFile {
    let file = builder.finish_as_file();
    let backing = file.get_file();
    let bytes = backing.read(0, backing.size() as usize).unwrap();
    engine
        .fs
        .get_runtime()
        .block_on(engine.fs.clone().create(
            backing.id(),
            bytes.clone(),
            dfs::Options::default().with_type(FileType::Columnar),
        ))
        .unwrap();
    ColumnarFile::open(
        Arc::new(InMemFile::new(backing.id(), bytes)),
        None,
        ColumnarMetaCache::default(),
    )
    .unwrap()
}

fn put_packed(engine: &TestEngine, file_id: u64, builder: TestPackedFileBuilder) -> PackedFile {
    let file_bytes = builder.finish_as_bytes();
    engine
        .fs
        .get_runtime()
        .block_on(engine.fs.clone().create(
            file_id,
            file_bytes.clone(),
            dfs::Options::default().with_type(FileType::FtsPackedFile),
        ))
        .unwrap();
    PackedFile::new(
        Arc::new(InMemFile::new(file_id, file_bytes)),
        FtsCache::disabled(),
    )
    .unwrap()
}

fn put_ded(engine: &TestEngine, file_id: u64, builder: TestDedicatedFileBuilder) -> EDedicatedFile {
    let file_bytes = builder.finish_as_bytes();
    engine
        .fs
        .get_runtime()
        .block_on(engine.fs.clone().create(
            file_id,
            file_bytes.clone(),
            dfs::Options::default().with_type(FileType::FtsDedicatedFile),
        ))
        .unwrap();
    EDedicatedFile::new(
        Arc::new(InMemFile::new(file_id, file_bytes)),
        FtsCache::disabled(),
    )
    .unwrap()
}

fn search_packed(file: &PackedFile, table_id: i64, index_id: i64, term: &str) -> Vec<u32> {
    block_on(async {
        let mut iter = file.lp_iter().unwrap();
        while let Some(lp) = iter.next_lp().await.unwrap() {
            if lp.props().get_table_id() != table_id || lp.props().get_index_id() != index_id {
                continue;
            }
            if lp.props().get_n_pk() == 0 {
                return Vec::new();
            }
            if lp.props().get_tantivy_layout().get_meta().get_size() == 0 {
                return Vec::new();
            }
            let reader = lp.read_tantivy_index().unwrap();
            let mut results = Vec::new();
            let query = make_unscored_query(&PlainFtsQueryInfo {
                query: term.to_string(),
                ..Default::default()
            });
            reader.search(&query, &mut results).unwrap();
            return results.into_iter().map(|r| r.doc_id).collect();
        }
        Vec::new()
    })
}

fn search_l2(file: &EDedicatedFile, term: &str) -> Vec<u32> {
    block_on(async {
        let dedicated = file.as_int().unwrap();
        let reader = dedicated.cached_read_index().await.unwrap();
        let mut results = Vec::new();
        let query = make_unscored_query(&PlainFtsQueryInfo {
            query: term.to_string(),
            ..Default::default()
        });
        reader.search(&query, &mut results).unwrap();
        results.into_iter().map(|r| r.doc_id).collect()
    })
}

fn outer_row_key(keyspace_id: u32, table_id: i64, handle: i64) -> Vec<u8> {
    let mut key = keyspace_prefix(keyspace_id);
    key.extend_from_slice(&encode_row_key(table_id, handle));
    key
}
