// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{convert::TryInto, sync::Mutex, time::Duration};

use clara_fts::test_util::{PlainFtsQueryInfo, make_scored_query, make_unscored_query};
use futures::executor::block_on;
use kvengine::{
    dfs,
    dfs::FileType,
    table::{
        columnar::Block,
        fts::{
            IntPk, VIRTUAL_SCORE_COLUMN_ID,
            test_util::{SchemaBuilder, new_block},
        },
        schema_file::build_schema_file,
    },
};
use kvenginepb::fts::FullTextIndexDef;
use pd_client::PdClient;
use test_cloud_server::{ServerCluster, must_wait, oss::prepare_dfs};
use tidb_query_datatype::{
    FieldTypeTp,
    codec::row::v2::encoder_for_test::{Column, RowEncoder},
    expr::EvalContext,
};
use tikv_util::codec::bytes::encode_bytes;

use crate::{
    alloc_node_id,
    columnar::{create_keyspace_and_split_tables, gen_row_key, send_schema_file_request},
};

fn encode_fts_row_val(ctx: &Mutex<EvalContext>, col1: i64, text: &str) -> Vec<u8> {
    let mut row_val = vec![];
    let cols = vec![
        Column::new(1, Some(col1)),
        Column::new(2, Some(text.as_bytes().to_vec())),
    ];
    let mut guard = ctx.lock().unwrap();
    row_val.write_row(&mut guard, cols).unwrap();
    row_val
}

#[test]
fn test_fts_indexed_and_unindexed_mvcc_end_to_end_scored_and_unscored() {
    test_util::init_log_for_test();

    let node_id = alloc_node_id();
    let (_temp_dir, mut oss, dfs_config) =
        prepare_dfs("test_fts_indexed_and_unindexed_mvcc_end_to_end");
    let mut cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.kvengine
            .columnar_table_build_options
            .max_columnar_table_size = 1024;
        conf.kvengine
            .columnar_table_build_options
            .pack_max_row_count = 9;
        conf.kvengine.build_columnar = true;
        conf.kvengine.read_columnar = true;
        conf.kvengine.build_fts_index = true;
        // Keep thresholds small so compaction triggers quickly in tests.
        conf.kvengine.fts_build_options.max_l0_files = 1;
        conf.kvengine.fts_build_options.min_l2_lp_size = tikv_util::config::ReadableSize::kb(0);
        conf.dfs = dfs_config.clone();
    });

    let dfs = cluster.get_dfs().unwrap();
    let (keyspace_id, table_ids) = dfs
        .get_runtime()
        .block_on(create_keyspace_and_split_tables(&mut cluster));
    let table_id = table_ids[1];
    let index_id = 1i64;
    let text_col_id = 2i64;

    let table_schema = SchemaBuilder::<IntPk>::new(table_id)
        .column(1, FieldTypeTp::LongLong)
        .column(text_col_id, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id,
            col_id: text_col_id,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();

    let schema_version = 10;
    let schema_file_data =
        build_schema_file(keyspace_id, schema_version, vec![table_schema.clone()], 0);
    let schema_file_id = 100;
    let opts = dfs::Options::default().with_type(FileType::Schema);
    dfs.get_runtime()
        .block_on(dfs.create(schema_file_id, schema_file_data.into(), opts))
        .unwrap();
    let status_addr = cluster.status_addr(node_id);

    let pd_client = cluster.get_pd_client();
    let region = pd_client
        .get_region(&encode_bytes(&gen_row_key(keyspace_id, table_id, 0)))
        .unwrap();
    let shard_id = region.id;

    let kvengine = cluster.get_kvengine(node_id);
    must_wait(
        || {
            dfs.get_runtime().block_on(send_schema_file_request(
                &status_addr,
                keyspace_id,
                schema_file_id,
            ));
            let Some(shard) = kvengine.get_shard(shard_id) else {
                return false;
            };
            shard.get_schema_file().is_some()
        },
        10,
        || "failed to sync schema file".to_string(),
    );

    let mut client = cluster.new_client();
    let eval_ctx = Mutex::new(EvalContext::default());

    // ======================================================
    // Stage 1: insert rows and flush to build a real FTS index
    // ======================================================
    // Query term: "alpha"
    // Visible at ts_before: 1, 2, 4
    // Visible at ts_after: 4 (updated), 6 (new)
    client.put_kv(
        1..6,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i| match i {
            1 => encode_fts_row_val(&eval_ctx, 101, "alpha alpha"),
            2 => encode_fts_row_val(&eval_ctx, 102, "alpha beta"),
            3 => encode_fts_row_val(&eval_ctx, 103, "beta"),
            4 => encode_fts_row_val(&eval_ctx, 104, "alpha"),
            5 => encode_fts_row_val(&eval_ctx, 105, "gamma"),
            _ => unreachable!(),
        },
    );

    cluster.flush_memtable(shard_id).unwrap();
    assert!(
        cluster.wait_for_memtable_flushed(shard_id, Duration::from_secs(10)),
        "memtable flush timeout"
    );

    must_wait(
        || {
            let Some(shard) = kvengine.get_shard(shard_id) else {
                return false;
            };
            shard.get_columnar_table_ids().contains(&table_id)
        },
        10,
        || "failed to build columnar files".to_string(),
    );

    must_wait(
        || {
            let Some(shard) = kvengine.get_shard(shard_id) else {
                return false;
            };
            !shard.get_all_fts_files().is_empty()
        },
        20,
        || "failed to build FTS index files".to_string(),
    );

    let ts_before = client.get_ts().into_inner();

    // ============================================================
    // Stage 2: insert/update/delete WITHOUT flushing (unindexed part)
    // ============================================================
    client.put_kv(
        vec![2usize, 4usize, 6usize],
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i| match i {
            // Indexed hit at pk=2 becomes non-matching in the unindexed part.
            2 => encode_fts_row_val(&eval_ctx, 202, "beta"),
            // Indexed hit at pk=4 stays matching but changes content.
            4 => encode_fts_row_val(&eval_ctx, 404, "alpha updated"),
            // New unindexed row.
            6 => encode_fts_row_val(&eval_ctx, 106, "alpha new"),
            _ => unreachable!(),
        },
    );
    client.del_kv(1..2, |i: usize| gen_row_key(keyspace_id, table_id, i));

    let shard_stats = kvengine.get_shard_stat(shard_id);
    assert!(
        !shard_stats.mem_table_is_empty(),
        "expected unflushed memtable (unindexed part), got empty memtable: {:?}",
        shard_stats
    );

    let ts_after = client.get_ts().into_inner();

    // ============================================================
    // Query helpers
    // ============================================================
    let runtime = dfs.get_runtime().handle().clone();
    let scored_query = make_scored_query(&PlainFtsQueryInfo {
        query: "alpha".to_string(),
        top_k: 10,
        column_id: text_col_id,
        index_id,
        ..Default::default()
    });
    assert_eq!(
        scored_query.info().get_query_type(),
        tipb::FtsQueryType::FtsQueryTypeWithScore
    );

    let scored_schema = SchemaBuilder::<IntPk>::new(table_id)
        .column(1, FieldTypeTp::LongLong)
        .column(text_col_id, FieldTypeTp::String)
        .fts_score_column(false)
        .schema();
    kvengine::table::fts::validate_schema(&scored_schema, &scored_query).unwrap();

    let read_scored = |read_ts: u64| -> Block {
        let shard = kvengine.get_shard(shard_id).unwrap();
        let snap = shard.new_snap_access();
        let mut reader = snap
            .new_fts_reader(
                &runtime,
                table_id,
                scored_query.clone(),
                scored_schema.clone(),
                read_ts,
                None,
                None,
            )
            .unwrap();
        block_on(async {
            reader.set_int_handle_range(0, Some(10)).await.unwrap();
            reader.read_all().await
        })
    };

    let noscore_query = make_unscored_query(&PlainFtsQueryInfo {
        query: "alpha".to_string(),
        top_k: 0,
        column_id: text_col_id,
        index_id,
        ..Default::default()
    });
    assert_eq!(
        noscore_query.info().get_query_type(),
        tipb::FtsQueryType::FtsQueryTypeNoScore
    );

    let noscore_schema = SchemaBuilder::<IntPk>::new(table_id)
        .column(1, FieldTypeTp::LongLong)
        .column(text_col_id, FieldTypeTp::String)
        .schema();
    kvengine::table::fts::validate_schema(&noscore_schema, &noscore_query).unwrap();

    let read_noscore = |read_ts: u64| -> Block {
        let shard = kvengine.get_shard(shard_id).unwrap();
        let snap = shard.new_snap_access();
        let mut reader = snap
            .new_fts_reader(
                &runtime,
                table_id,
                noscore_query.clone(),
                noscore_schema.clone(),
                read_ts,
                None,
                None,
            )
            .unwrap();
        block_on(async {
            reader.set_int_handle_range(0, Some(10)).await.unwrap();
            reader.read_all().await
        })
    };

    // ============================================================
    // Scored query
    // ============================================================
    let mut scored_before = read_scored(ts_before);
    scored_before.sort();
    let expected_scored_before = new_block::<IntPk>(&scored_schema, |row| {
        row(1, 0, false, |datum| {
            datum(101);
            datum("alpha alpha");
            datum(0.0);
        });
        row(2, 0, false, |datum| {
            datum(102);
            datum("alpha beta");
            datum(0.0);
        });
        row(4, 0, false, |datum| {
            datum(104);
            datum("alpha");
            datum(0.0);
        });
    });
    assert_eq!(
        (0..scored_before.length())
            .map(|row| scored_before.get_handle_buf().get_int_handle_value(row))
            .collect::<Vec<_>>(),
        vec![1, 2, 4]
    );
    assert!(scored_before.cols_eq(&expected_scored_before, false, &[1, text_col_id]));
    let score_col = scored_before.get_column(VIRTUAL_SCORE_COLUMN_ID);
    for row in 0..scored_before.length() {
        let score = f64::from_le_bytes(score_col.get_not_null_value(row).try_into().unwrap());
        assert!(score > 0.0, "scores must be positive for matched rows");
    }

    let mut scored_after = read_scored(ts_after);
    scored_after.sort();
    let expected_scored_after = new_block::<IntPk>(&scored_schema, |row| {
        row(4, 0, false, |datum| {
            datum(404);
            datum("alpha updated");
            datum(0.0);
        });
        row(6, 0, false, |datum| {
            datum(106);
            datum("alpha new");
            datum(0.0);
        });
    });
    assert_eq!(
        (0..scored_after.length())
            .map(|row| scored_after.get_handle_buf().get_int_handle_value(row))
            .collect::<Vec<_>>(),
        vec![4, 6]
    );
    assert!(scored_after.cols_eq(&expected_scored_after, false, &[1, text_col_id]));
    let score_col = scored_after.get_column(VIRTUAL_SCORE_COLUMN_ID);
    for row in 0..scored_after.length() {
        let score = f64::from_le_bytes(score_col.get_not_null_value(row).try_into().unwrap());
        assert!(score > 0.0, "scores must be positive for matched rows");
    }

    // ============================================================
    // Unscored query
    // ============================================================
    let mut noscore_before = read_noscore(ts_before);
    noscore_before.sort();
    let expected_noscore_before = new_block::<IntPk>(&noscore_schema, |row| {
        row(1, 0, false, |datum| {
            datum(101);
            datum("alpha alpha");
        });
        row(2, 0, false, |datum| {
            datum(102);
            datum("alpha beta");
        });
        row(4, 0, false, |datum| {
            datum(104);
            datum("alpha");
        });
    });
    assert_eq!(
        (0..noscore_before.length())
            .map(|row| noscore_before.get_handle_buf().get_int_handle_value(row))
            .collect::<Vec<_>>(),
        vec![1, 2, 4]
    );
    assert!(noscore_before.cols_eq(&expected_noscore_before, false, &[1, text_col_id]));

    let mut noscore_after = read_noscore(ts_after);
    noscore_after.sort();
    let expected_noscore_after = new_block::<IntPk>(&noscore_schema, |row| {
        row(4, 0, false, |datum| {
            datum(404);
            datum("alpha updated");
        });
        row(6, 0, false, |datum| {
            datum(106);
            datum("alpha new");
        });
    });
    assert_eq!(
        (0..noscore_after.length())
            .map(|row| noscore_after.get_handle_buf().get_int_handle_value(row))
            .collect::<Vec<_>>(),
        vec![4, 6]
    );
    assert!(noscore_after.cols_eq(&expected_noscore_after, false, &[1, text_col_id]));

    cluster.stop();
    oss.shutdown();
}
