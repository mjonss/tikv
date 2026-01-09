// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    convert::TryInto,
    sync::{Arc, Mutex},
};

use futures::executor::block_on;
use kvengine::{
    dfs,
    dfs::FileType,
    table::{
        columnar::{
            new_int_handle_column_info, new_version_column_info, Block, ColumnarFilterReader,
        },
        schema_file::{build_schema_file, Schema, SchemaBuf},
    },
};
use schema::schema::StorageClassSpec;
use test_cloud_server::{
    copr::{build_row_key, build_row_val_for_fts, SCORE_SAMPLE_DOCS},
    must_wait, ServerCluster,
};
use tidb_query_datatype::{expr::EvalContext, FieldTypeFlag, FieldTypeTp};
use tipb::{ColumnInfo, FtsQueryInfo};

use crate::{
    alloc_node_id,
    columnar::{
        build_where_expr, create_keyspace_and_split_tables, send_schema_file_request, TableScanCtx,
    },
};

struct FtsOpts {
    with_score: bool,
    with_fts_col: bool,
}

#[test]
fn test_fts_brute_read() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let mut cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.kvengine
            .columnar_table_build_options
            .max_columnar_table_size = 1024;
        conf.kvengine
            .columnar_table_build_options
            .pack_max_row_count = 9;
        conf.kvengine.build_columnar = true;
        conf.kvengine.read_columnar = true;
    });
    let dfs = cluster.get_dfs().unwrap();
    let (keyspace_id, table_ids) = dfs
        .get_runtime()
        .block_on(create_keyspace_and_split_tables(&mut cluster));
    let table_id = table_ids[1];
    let schema = build_fts_schema(
        table_id,
        FtsOpts {
            with_score: false,
            with_fts_col: true,
        },
    );
    let schema_version = 10;
    let schema_file_data = build_schema_file(keyspace_id, schema_version, vec![schema.clone()], 0);
    let schema_file_id = 100;
    let opts = dfs::Options::default().with_type(FileType::Schema);
    dfs.get_runtime()
        .block_on(dfs.create(schema_file_id, schema_file_data.into(), opts))
        .unwrap();
    let status_addr = cluster.status_addr(node_id);

    let kvengine = cluster.get_kvengine(node_id);
    must_wait(
        || {
            dfs.get_runtime().block_on(send_schema_file_request(
                &status_addr,
                keyspace_id,
                schema_file_id,
            ));
            let all_id_vers = kvengine.get_all_shard_id_vers();
            for id_ver in all_id_vers {
                if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    if shard.get_schema_file().is_some() {
                        return true;
                    }
                }
            }
            false
        },
        10,
        || "failed to build schema file".to_string(),
    );

    let mut client = cluster.new_client();
    let ctx = Mutex::new(EvalContext::default());
    let step = 1;
    let total_cnt = 10;
    for i in (0..total_cnt).step_by(step) {
        client.put_kv(
            i..i + step,
            |i: usize| {
                let mut guard = ctx.lock().unwrap();
                build_row_key(keyspace_id, &schema, &mut guard, i)
            },
            |i| {
                let mut guard = ctx.lock().unwrap();
                build_row_val_for_fts(&schema, &mut guard, i)
            },
        );
    }

    let mut shard_id = None;
    must_wait(
        || {
            let all_id_vers = kvengine.get_all_shard_id_vers();
            for id_ver in all_id_vers {
                if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    if shard.get_columnar_table_ids().contains(&table_id) {
                        shard_id = Some(id_ver.id);
                        return true;
                    }
                }
            }
            false
        },
        10,
        || "failed to build columnar file".to_string(),
    );
    let shard_id = shard_id.unwrap();
    let shard = kvengine.get_shard(shard_id).unwrap();
    let snap_access = shard.new_snap_access();
    let ts = client.get_ts().into_inner();
    let expr = build_where_expr(&schema.columns, 1, 100);
    let mut table_scan = tipb::Executor::default();
    table_scan
        .mut_tbl_scan()
        .set_columns(schema.columns.clone().into());
    let scan_ctx = TableScanCtx::new(table_scan, vec![expr]);

    // fts_type=FtsQueryTypeWithScore, read schema: [fts_col, score_col]
    {
        let schema_to_read = build_fts_schema(
            table_id,
            FtsOpts {
                with_score: true,
                with_fts_col: true,
            },
        );

        let mut fts_query = FtsQueryInfo::new();
        let src = "machine learning machine learning";
        fts_query.set_query_text(src.to_string());
        fts_query.set_query_tokenizer("STANDARD_V1".to_string());
        fts_query.set_query_type(tipb::FtsQueryType::FtsQueryTypeWithScore);

        let mut fts_column = ColumnInfo::new();
        fts_column.set_column_id(2);
        fts_column.set_tp(FieldTypeTp::String as i32);
        fts_column.set_flag(schema_to_read.columns[0].get_flag());
        fts_query.set_columns(vec![fts_column].into());

        let fts_query_arc = Arc::new(fts_query);

        let mut fts_reader = snap_access
            .new_fts_columnar_mvcc_reader(
                schema_to_read.table_id,
                &schema_to_read.columns,
                Some(&scan_ctx),
                ts,
                fts_query_arc,
            )
            .unwrap()
            .unwrap();
        block_on(fts_reader.set_unbounded_handle_range()).unwrap();
        let mut block = Block::new(&schema_to_read);
        let read_rows = block_on(fts_reader.read_block(&mut block, 10)).unwrap();
        (100..110).contains(&read_rows);

        // (handle, score)
        let mut scored_rows: Vec<(i64, f64)> = Vec::new();

        for i in 0..read_rows {
            let handle = block.get_handle_buf().get_int_handle_value(i);

            let column_text = &block.get_columns()[0];
            assert_eq!(
                std::str::from_utf8(column_text.get_not_null_value(i)).unwrap(),
                SCORE_SAMPLE_DOCS[handle as usize],
                "test read schema: [fts_col, score_col] failed, not equal index in :{}",
                i,
            );

            let column_score = &block.get_columns()[1];
            let score_bytes = column_score.get_not_null_value(i);
            let score =
                f64::from_le_bytes(score_bytes.try_into().expect("Invalid byte length for f64"));

            scored_rows.push((handle, score));
        }
        // Sort by score descending
        scored_rows.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        let actual_ordered_handles: Vec<i64> =
            scored_rows.iter().map(|(handle, _)| *handle).collect();

        let expected_ordered_handles: Vec<i64> = vec![2, 0, 6, 1, 3, 4, 5, 9];

        assert_eq!(
            actual_ordered_handles, expected_ordered_handles,
            "FTS result handles not in expected descending score order.\nExpected: {:?}\nActual: {:?}",
            expected_ordered_handles, actual_ordered_handles
        );
    }

    // fts_type=FtsQueryTypeWithScore, read schema: [score_col]
    {
        let schema_to_read = build_fts_schema(
            table_id,
            FtsOpts {
                with_score: true,
                with_fts_col: false,
            },
        );

        let mut fts_query = FtsQueryInfo::new();
        let src = "machine learning machine learning";
        fts_query.set_query_text(src.to_string());
        fts_query.set_query_tokenizer("STANDARD_V1".to_string());
        fts_query.set_query_type(tipb::FtsQueryType::FtsQueryTypeWithScore);

        let mut fts_column = ColumnInfo::new();
        fts_column.set_column_id(2);
        fts_column.set_tp(FieldTypeTp::String as i32);
        fts_column.set_flag(schema.columns[0].get_flag());
        fts_query.set_columns(vec![fts_column].into());

        let fts_query_arc = Arc::new(fts_query);

        let mut fts_reader = snap_access
            .new_fts_columnar_mvcc_reader(
                schema_to_read.table_id,
                &schema_to_read.columns,
                Some(&scan_ctx),
                ts,
                fts_query_arc,
            )
            .unwrap()
            .unwrap();
        block_on(fts_reader.set_unbounded_handle_range()).unwrap();
        let mut block = Block::new(&schema_to_read);
        let read_rows = block_on(fts_reader.read_block(&mut block, 10)).unwrap();
        (100..110).contains(&read_rows);

        // (handle, score)
        let mut scored_rows: Vec<(i64, f64)> = Vec::new();

        for i in 0..read_rows {
            let handle = block.get_handle_buf().get_int_handle_value(i);

            let column_score = &block.get_columns()[0];
            let score_bytes = column_score.get_not_null_value(i);
            let score =
                f64::from_le_bytes(score_bytes.try_into().expect("Invalid byte length for f64"));

            scored_rows.push((handle, score));
        }

        // Sort by score descending
        scored_rows.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        let actual_ordered_handles: Vec<i64> =
            scored_rows.iter().map(|(handle, _)| *handle).collect();

        let expected_ordered_handles: Vec<i64> = vec![2, 0, 6, 1, 3, 4, 5, 9];

        assert_eq!(
            actual_ordered_handles, expected_ordered_handles,
            "FTS result handles not in expected descending score order (read schema: [score_col]).\nExpected: {:?}\nActual: {:?}",
            expected_ordered_handles, actual_ordered_handles
        );
    }
    // fts_type=FtsQueryTypeNoScore, read schema: [fts_col]
    {
        let schema_to_read = build_fts_schema(
            table_id,
            FtsOpts {
                with_score: false,
                with_fts_col: true,
            },
        );

        let mut fts_query = FtsQueryInfo::new();
        let src = "machine learning machine learning";
        fts_query.set_query_text(src.to_string());
        fts_query.set_query_tokenizer("STANDARD_V1".to_string());
        fts_query.set_query_type(tipb::FtsQueryType::FtsQueryTypeNoScore);

        let mut fts_column = ColumnInfo::new();
        fts_column.set_column_id(2);
        fts_column.set_tp(FieldTypeTp::String as i32);
        fts_column.set_flag(schema.columns[0].get_flag());
        fts_query.set_columns(vec![fts_column].into());

        let fts_query_arc = Arc::new(fts_query);

        let mut fts_reader = snap_access
            .new_fts_columnar_mvcc_reader(
                schema_to_read.table_id,
                &schema_to_read.columns,
                Some(&scan_ctx),
                ts,
                fts_query_arc,
            )
            .unwrap()
            .unwrap();
        block_on(fts_reader.set_unbounded_handle_range()).unwrap();
        let mut block = Block::new(&schema_to_read);
        let read_rows = block_on(fts_reader.read_block(&mut block, 10)).unwrap();
        (100..110).contains(&read_rows);
        let handle_result: Vec<i64> = vec![0, 1, 2, 3, 4, 5, 6, 9];

        for i in 0..read_rows {
            let handle = block.get_handle_buf().get_int_handle_value(i);
            assert_eq!(
                handle, handle_result[i],
                "read schema: [fts_col] failed, not euqal in index: {}",
                i
            );

            let column_text = &block.get_columns()[0];
            assert_eq!(
                column_text.get_not_null_value(i),
                SCORE_SAMPLE_DOCS[handle_result[i] as usize].as_bytes(),
                "read schema: [fts_col] failed, not euqal in index: {}",
                i
            );
        }
    }
}

fn build_fts_schema(table_id: i64, opts: FtsOpts) -> Schema {
    // fts col
    let mut handle_column = new_int_handle_column_info();
    handle_column.set_column_id(1);
    handle_column.set_pk_handle(true);
    let version_column = new_version_column_info();
    let mut fts_column = ColumnInfo::new();
    fts_column.set_column_id(2);
    fts_column.set_tp(FieldTypeTp::String as i32);

    // score col
    let mut score_col = ColumnInfo::new();
    score_col.set_column_id(-2050);
    score_col.set_tp(FieldTypeTp::Float as i32);
    score_col.set_flag(FieldTypeFlag::NOT_NULL.bits() as i32);

    let mut columns: Vec<ColumnInfo> = vec![];
    if opts.with_fts_col {
        columns.push(fts_column.clone());
    }
    if opts.with_score {
        columns.push(score_col.clone());
    }

    SchemaBuf::new(
        table_id,
        handle_column,
        version_column,
        columns,
        vec![],
        0,
        vec![],
        vec![],
        StorageClassSpec::default(),
        None,
    )
    .into()
}
