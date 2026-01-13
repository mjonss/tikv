// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Mutex;

use api_version::ApiV2;
use kvengine::{GLOBAL_SHARD_END_KEY, dfs, dfs::FileType, table::schema_file::build_schema_file};
use kvproto::coprocessor::KeyRange;
use protobuf::Message;
use test_cloud_server::{
    ServerCluster,
    copr::{build_dag, build_row_key, build_row_val, col_val_datum},
    must_wait,
};
use test_coprocessor::DagChunkSpliter;
use tidb_query_datatype::{
    FieldTypeAccessor, FieldTypeFlag,
    codec::{
        Datum,
        table::{encode_common_handle_row_key, encode_row_key},
    },
    expr::EvalContext,
};
use tikv::coprocessor::REQ_TYPE_DAG;

use crate::{
    alloc_node_id,
    columnar::{create_keyspace_and_split_tables, send_schema_file_request},
};

#[test]
fn test_coprocessor() {
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
    let t1 = table_ids[0];
    let ddl_t1 = format!(
        "create table t{} (c1 int, c2 varchar, c3 int, c4 varchar, pk(c1))",
        t1
    );
    let t2 = table_ids[1];
    let ddl_t2 = format!(
        "create table t{} (c1 int, c2 varchar, c3 int, c4 varchar, c5 float, pk(c3, c2))",
        t2
    );
    let schema_t1 = test_cloud_server::copr::build_schema(&ddl_t1);
    let schema_t2 = test_cloud_server::copr::build_schema(&ddl_t2);
    let schemas = vec![schema_t1.clone(), schema_t2.clone()];
    let schema_file_data = build_schema_file(keyspace_id, 10, schemas, 0);
    let schema_file_id = 100;
    let opts = dfs::Options::default().with_type(FileType::Schema);
    dfs.get_runtime()
        .block_on(dfs.create(schema_file_id, schema_file_data.into(), opts))
        .unwrap();
    let status_addr = cluster.status_addr(node_id);

    let kvengine = cluster.get_kvengine(node_id);
    let all_ids_vers = kvengine.get_all_shard_id_vers();
    assert_eq!(all_ids_vers.len(), 8);
    must_wait(
        || {
            dfs.get_runtime().block_on(send_schema_file_request(
                &status_addr,
                keyspace_id,
                schema_file_id,
            ));
            let mut shard_with_schema_file_count = 0;
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                if shard.get_schema_file().is_some() {
                    shard_with_schema_file_count += 1;
                }
            }
            shard_with_schema_file_count == 2
        },
        10,
        || "failed to wait schema file".to_string(),
    );
    let mut client = cluster.new_client();
    let ctx = Mutex::new(EvalContext::default());
    let step = 200;
    let total_cnt = 1000;
    for i in (0..total_cnt).step_by(step) {
        client.put_kv(
            i..i + step,
            |i: usize| {
                let mut guard = ctx.lock().unwrap();
                build_row_key(keyspace_id, &schema_t1, &mut guard, i)
            },
            |i| {
                let mut guard = ctx.lock().unwrap();
                build_row_val(&schema_t1, &mut guard, i)
            },
        );
    }
    for i in (0..total_cnt).step_by(step) {
        client.put_kv(
            i..i + step,
            |i: usize| {
                let mut guard = ctx.lock().unwrap();
                build_row_key(keyspace_id, &schema_t2, &mut guard, i)
            },
            |i| {
                let mut guard = ctx.lock().unwrap();
                build_row_val(&schema_t2, &mut guard, i)
            },
        );
    }
    let statements = vec![
        (
            format!("select c1, c2, c3, c4 from t{t1}"),
            schema_t1.clone(),
        ),
        (format!("select c3, c2 from t{t1}"), schema_t1.clone()),
        (format!("select c3, c4 from t{t2}"), schema_t2.clone()),
        (format!("select c2, c1 from t{t2}"), schema_t2.clone()),
        (format!("select c5 from t{t2}"), schema_t2.clone()),
    ];
    for (stmt, schema) in statements {
        let dag = build_dag(&stmt, &schema);
        let tbl_id = schema.table_id;
        let dag_columns = dag
            .get_executors()
            .first()
            .unwrap()
            .get_tbl_scan()
            .get_columns()
            .to_vec();
        let mut req = kvproto::coprocessor::Request::new();
        req.set_tp(REQ_TYPE_DAG);
        let ts = client.get_ts().into_inner();
        req.set_start_ts(ts);
        req.set_data(dag.write_to_bytes().unwrap());
        let mut start_key = ApiV2::get_txn_keyspace_prefix(keyspace_id);
        let mut end_key = ApiV2::get_txn_keyspace_prefix(keyspace_id);
        let mut key_range = KeyRange::new();
        if schema.is_common_handle() {
            start_key.extend_from_slice(&encode_common_handle_row_key(tbl_id, &[]));
            end_key.extend_from_slice(&encode_common_handle_row_key(tbl_id, GLOBAL_SHARD_END_KEY));
        } else {
            start_key.extend_from_slice(&encode_row_key(tbl_id, 0));
            end_key.extend_from_slice(&encode_row_key(tbl_id, i64::MAX));
        }
        key_range.set_start(start_key);
        key_range.set_end(end_key);
        req.set_ranges(vec![key_range.clone()].into());
        let cop_resp = client.simple_cop_request(req.clone());
        let mut resp = tipb::SelectResponse::default();
        resp.merge_from_bytes(cop_resp.get_data()).unwrap();
        let chunk_splitter = DagChunkSpliter::new(resp.take_chunks().into(), dag_columns.len());
        let mut count = 0;
        for (row_i, row) in chunk_splitter.enumerate() {
            for (col_i, col_info) in dag_columns.iter().enumerate() {
                if row_i % 5 == 0 && !col_info.flag().contains(FieldTypeFlag::NOT_NULL) {
                    assert_eq!(row[col_i], Datum::Null);
                } else {
                    assert_eq!(
                        row[col_i],
                        col_val_datum(col_info, row_i),
                        "row_i: {}, col_id: {}",
                        row_i,
                        col_info.get_column_id()
                    );
                }
            }
            count += 1;
        }
        assert_eq!(count, total_cnt);

        // Enable paging
        req.set_paging_size(32);
        let mut count = 0;
        let mut paging_count = 0;
        loop {
            let cop_resp = client.simple_cop_request(req.clone());
            let mut resp = tipb::SelectResponse::default();
            resp.merge_from_bytes(cop_resp.get_data()).unwrap();
            let chunk_splitter = DagChunkSpliter::new(resp.take_chunks().into(), dag_columns.len());
            for (row_i, row) in chunk_splitter.enumerate() {
                let row_i = row_i + paging_count * 32;
                for (col_i, col_info) in dag_columns.iter().enumerate() {
                    if row_i % 5 == 0 && !col_info.flag().contains(FieldTypeFlag::NOT_NULL) {
                        assert_eq!(row[col_i], Datum::Null);
                    } else {
                        assert_eq!(
                            row[col_i],
                            col_val_datum(col_info, row_i),
                            "row_i: {}, col_id: {}",
                            row_i,
                            col_info.get_column_id()
                        );
                    }
                }
                count += 1;
            }
            paging_count += 1;
            let scanned_range = cop_resp.get_range();
            if scanned_range.get_end().is_empty() && scanned_range.get_start().is_empty() {
                break;
            }
            key_range.set_start(scanned_range.get_end().to_vec());
            req.set_ranges(vec![key_range.clone()].into());
        }
        assert_eq!(count, total_cnt);
    }
}
