// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use api_version::ApiV2;
use kvengine::table::{
    columnar::{
        new_common_handle_column_info, new_int_handle_column_info, new_version_column_info,
    },
    schema_file::{Schema, SchemaBuf},
};
use schema::schema::StorageClassSpec;
use tidb_query_datatype::{
    Collation, FieldTypeAccessor, FieldTypeFlag, FieldTypeTp,
    codec::{
        Datum,
        data_type::VectorFloat32,
        datum::encode_key,
        row::v2::encoder_for_test::{Column, RowEncoder},
        table::{encode_common_handle_row_key, encode_row_key},
    },
    expr::EvalContext,
};
use tipb::{ColumnInfo, ExecType, Executor, TableScan};

use crate::client::ClusterClient;

// used by full text search test.
pub const SCORE_SAMPLE_DOCS: &[&str] = &[
    // 0
    "machine learning machine learning machine learning",
    // 1
    "machine learning algorithms. Machine learning models. Machine learning optimization.",
    // 2
    "Machine learning (Machine learning (Machine learning (Machine learning (Machine learning (is used in AI. Machine learning (Machine learning (Machine learning (Machine learning (Machine learning (is used in AI. Machine learning (Machine learning (Machine learning (Machine learning (Machine learning (is used in AI. Machine learning ends.",
    // 3
    "Understanding machine learning: basics of learning from machines. Machine learning requires data.",
    // 4
    "machine learning",
    // 5
    "This 50-word document briefly mentions machine learning once. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. ",
    // 6
    "Learning machine learning machine. Learning machine applications.",
    // 7
    "Space Tourism: Risks and Opportunities - Private companies like SpaceX and Blue Origin are making suborbital flights accessible, but safety protocols remain a critical concern.",
    // 8
    "Sustainable Fashion Trends - Eco-friendly materials like mushroom leather and recycled polyester are reshaping the fashion industry’s environmental impact.",
    // 9
    "AI-powered tools are transforming medical imaging analysis. Deep learning algorithms now detect early-stage tumors with 95% accuracy.",
];

impl ClusterClient {
    pub fn simple_cop_request(
        &mut self,
        mut req: kvproto::coprocessor::Request,
    ) -> kvproto::coprocessor::Response {
        let region = self.get_region_by_key(&req.ranges[0].start);
        let ctx = self.new_rpc_ctx(region.id, &req.ranges[0].start).unwrap();
        let store_id = ctx.get_peer().get_store_id();
        let kv_client = self.get_kv_client(store_id);
        req.set_context(ctx);
        kv_client.coprocessor(&req).unwrap()
    }
}

pub fn build_schema(ddl: &str) -> Schema {
    let tbl_id = parse_table_id(ddl);
    let content = get_table_content(ddl);
    let cols_def_str = get_cols_def(content);
    let mut col_defs = vec![];
    for part in cols_def_str.split(", ") {
        col_defs.push(parse_col_def(part));
    }
    let pkd_def_str = get_pk_def(content);
    let mut pk_ids = vec![];
    for part in pkd_def_str.split(", ") {
        pk_ids.push(parse_col_id_str(part));
    }
    let mut pk_is_handle = false;
    let handle_column = if pk_ids.is_empty() {
        new_int_handle_column_info()
    } else if pk_ids.len() == 1 {
        let (pk_col_id, pk_col_type, _) = col_defs
            .iter()
            .find(|(col_id, ..)| col_id == &pk_ids[0])
            .unwrap();
        if *pk_col_type == FieldTypeTp::LongLong {
            let mut col_info = new_int_handle_column_info();
            col_info.set_column_id(*pk_col_id);
            col_info.set_pk_handle(true);
            pk_is_handle = true;
            col_info
        } else {
            new_common_handle_column_info()
        }
    } else {
        new_common_handle_column_info()
    };
    let version_column = new_version_column_info();
    let mut columns = vec![];
    let mut pk_columns = vec![];
    for (col_id, col_type, notnull) in col_defs {
        let mut col_info = make_column_info(col_id, col_type, notnull);
        if pk_ids.contains(&col_id) {
            if pk_is_handle {
                continue;
            }
            col_info.set_flag((FieldTypeFlag::PRIMARY_KEY | FieldTypeFlag::NOT_NULL).bits() as i32);
            pk_columns.push(col_info.clone());
        } else {
            if notnull {
                col_info.set_flag(FieldTypeFlag::NOT_NULL.bits() as i32);
            }
            columns.push(col_info);
        }
    }
    pk_columns.sort_by(|a, b| {
        let position_a = pk_ids
            .iter()
            .position(|&id| id == a.get_column_id())
            .unwrap();
        let position_b = pk_ids
            .iter()
            .position(|&id| id == b.get_column_id())
            .unwrap();
        position_a.cmp(&position_b)
    });
    columns.extend_from_slice(&pk_columns);
    SchemaBuf::new(
        tbl_id,
        handle_column,
        version_column,
        columns,
        pk_ids,
        0,
        vec![],
        vec![],
        StorageClassSpec::default(),
        None,
    )
    .into()
}

pub fn build_row_key(
    keyspace_id: u32,
    schema: &Schema,
    ctx: &mut EvalContext,
    i: usize,
) -> Vec<u8> {
    let mut key = ApiV2::get_txn_keyspace_prefix(keyspace_id);
    if schema.is_common_handle() {
        let mut handle_datums = vec![];
        for col in schema
            .columns
            .iter()
            .filter(|col| col.flag().contains(FieldTypeFlag::PRIMARY_KEY))
        {
            handle_datums.push(col_val_datum(col, i));
        }
        let common_handle = encode_key(ctx, &handle_datums).unwrap();
        let row_key = encode_common_handle_row_key(schema.table_id, &common_handle);
        key.extend_from_slice(&row_key);
    } else {
        key.extend_from_slice(&encode_row_key(schema.table_id, i as i64));
    }
    key
}

pub fn build_row_val(schema: &Schema, ctx: &mut EvalContext, i: usize) -> Vec<u8> {
    let mut cols = vec![];
    for col_info in schema
        .columns
        .iter()
        .filter(|ci| !ci.flag().contains(FieldTypeFlag::PRIMARY_KEY))
    {
        cols.push(col_val_column(col_info, i));
    }
    let mut val = vec![];
    val.write_row(ctx, cols).unwrap();
    val
}

pub fn build_row_val_for_fts(schema: &Schema, ctx: &mut EvalContext, i: usize) -> Vec<u8> {
    let mut cols = vec![];
    for col_info in schema
        .columns
        .iter()
        .filter(|ci| !ci.flag().contains(FieldTypeFlag::PRIMARY_KEY))
    {
        cols.push(col_fts_column(col_info, i));
    }
    let mut val = vec![];
    val.write_row(ctx, cols).unwrap();
    val
}

pub fn col_val_datum(col_info: &ColumnInfo, i: usize) -> Datum {
    match col_info.tp() {
        FieldTypeTp::LongLong => Datum::I64(i as i64),
        FieldTypeTp::Float => Datum::F64(i as f64),
        FieldTypeTp::VarChar => Datum::Bytes(format!("value{:06}", i).into_bytes()),
        _ => unimplemented!(),
    }
}

fn col_val_column(col_info: &ColumnInfo, i: usize) -> Column {
    if !col_info.flag().contains(FieldTypeFlag::NOT_NULL) && i % 5 == 0 {
        Column::new(col_info.get_column_id(), None::<i64>)
    } else {
        match col_info.tp() {
            FieldTypeTp::LongLong => Column::new(col_info.get_column_id(), Some(i as i64)),
            FieldTypeTp::Float => Column::new(col_info.get_column_id(), Some(i as f64)),
            FieldTypeTp::VarChar => {
                let str_val = format!("value{:06}", i);
                Column::new(col_info.get_column_id(), Some(str_val.into_bytes()))
            }
            FieldTypeTp::TiDbVectorFloat32 => {
                let mut f32_vals = vec![];
                for j in 0..col_info.get_column_len() {
                    f32_vals.push(i as f32 + j as f32);
                }
                let vec_f32 = VectorFloat32::copy_from_f32(&f32_vals);
                Column::new(col_info.get_column_id(), Some(vec_f32))
            }
            _ => unimplemented!(),
        }
    }
}

fn col_fts_column(col_info: &ColumnInfo, i: usize) -> Column {
    match col_info.tp() {
        FieldTypeTp::Float => Column::new(col_info.get_column_id(), Some(i as f64)),
        FieldTypeTp::String => {
            let str_val = SCORE_SAMPLE_DOCS[i].to_string();
            Column::new(col_info.get_column_id(), Some(str_val.into_bytes()))
        }
        _ => unimplemented!(),
    }
}

pub fn build_dag(select: &str, schema: &Schema) -> tipb::DagRequest {
    let table_id = parse_select_table_id(select);
    assert_eq!(table_id, schema.table_id);
    let col_ids = parse_select_column_ids(select);
    let columns: Vec<ColumnInfo> = col_ids
        .iter()
        .map(|&col_id| {
            if schema.handle_column.get_column_id() == col_id {
                schema.handle_column.clone()
            } else {
                schema
                    .columns
                    .iter()
                    .find(|col| col.get_column_id() == col_id)
                    .unwrap()
                    .clone()
            }
        })
        .collect();
    let mut exec = Executor::default();
    exec.set_tp(ExecType::TypeTableScan);
    let mut tbl_scan = TableScan::default();
    tbl_scan.set_table_id(schema.table_id);
    tbl_scan.set_columns(columns.into());
    tbl_scan.set_primary_column_ids(schema.pk_col_ids.clone());
    exec.set_tbl_scan(tbl_scan);
    let mut dag = tipb::DagRequest::new();
    dag.set_executors(vec![exec].into());
    dag.set_output_offsets((0..col_ids.len() as u32).collect());
    dag
}

fn parse_select_column_ids(select: &str) -> Vec<i64> {
    let pattern = "select ";
    let left = select.find(pattern).unwrap() + pattern.len();
    let right = select.rfind(" from").unwrap();
    let col_ids_str = &select[left..right];
    col_ids_str
        .split(", ")
        .map(|col_id_str| col_id_str[1..].parse().unwrap())
        .collect()
}

fn parse_select_table_id(select: &str) -> i64 {
    let pattern = "from t";
    let left = select.find(pattern).unwrap() + pattern.len();
    select[left..].parse().unwrap()
}

fn get_table_content(ddl: &str) -> &str {
    let left = ddl.find('(').unwrap();
    let right = ddl.rfind(')').unwrap();
    &ddl[left + 1..right]
}

fn get_cols_def(content: &str) -> &str {
    let pattern = ", pk(";
    match content.find(pattern) {
        None => content,
        Some(left) => &content[..left],
    }
}

fn get_pk_def(content: &str) -> &str {
    let pattern = "pk(";
    let left = content.find(pattern).unwrap() + pattern.len();
    let right = content.rfind(')').unwrap();
    &content[left..right]
}

fn parse_table_id(ddl: &str) -> i64 {
    let pattern = "table t";
    let left = ddl.rfind(pattern).unwrap() + pattern.len();
    let right = ddl.find(" (").unwrap();
    ddl[left..right].parse().unwrap()
}

fn parse_col_def(col_def_str: &str) -> (i64, FieldTypeTp, bool) {
    let col_def = col_def_str.split(' ').collect::<Vec<_>>();
    let col_id = parse_col_id_str(col_def[0]);
    let col_type = parse_col_type_str(col_def[1]);
    let notnull = if col_def.len() == 3 {
        col_def[2] == "notnull"
    } else {
        false
    };
    (col_id, col_type, notnull)
}

fn parse_col_id_str(col_id_str: &str) -> i64 {
    col_id_str[1..].parse().unwrap()
}

fn parse_col_type_str(col_type_str: &str) -> FieldTypeTp {
    match col_type_str {
        "int" => FieldTypeTp::LongLong,
        "float" => FieldTypeTp::Float,
        "varchar" => FieldTypeTp::VarChar,
        "blob" => FieldTypeTp::Blob,
        _ => unimplemented!(),
    }
}

fn make_column_info(col_id: i64, col_type: FieldTypeTp, notnull: bool) -> ColumnInfo {
    let mut col_info = ColumnInfo::new();
    col_info.set_column_id(col_id);
    col_info.set_tp(col_type as i32);
    if col_type == FieldTypeTp::VarChar {
        col_info.set_collation(Collation::Utf8Mb4Bin as i32);
    } else {
        col_info.set_collation(Collation::Binary as i32);
    }
    if notnull {
        col_info.set_flag(FieldTypeFlag::NOT_NULL.bits() as i32);
    }
    col_info
}

#[test]
fn test_build_schema_dag() {
    let schema =
        build_schema("create table t1 (c1 int, c2 float, c3 varchar, c4 int notnull, pk(c1, c3))");
    assert_eq!(schema.table_id, 1);
    assert_eq!(schema.columns.len(), 4);
    assert!(schema.is_common_handle());
    let c1 = schema.find_column_by_id(1).unwrap();
    assert_eq!(c1.get_tp(), FieldTypeTp::LongLong as i32);
    let c2 = schema.find_column_by_id(2).unwrap();
    assert_eq!(c2.get_tp(), FieldTypeTp::Float as i32);
    assert_eq!(c2.get_flag(), 0);
    let c3 = schema.find_column_by_id(3).unwrap();
    assert_eq!(c3.get_tp(), FieldTypeTp::VarChar as i32);
    let _ = build_schema("create table t1 (c1 int, c2 float, c3 varchar, pk(c1))");

    let dag = build_dag("select c1, c3 from t1", &schema);
    assert_eq!(dag.get_executors().len(), 1);
    let exec = dag.get_executors().first().unwrap();
    let tbl_scan = exec.get_tbl_scan();
    tbl_scan.get_columns().iter().for_each(|col| {
        let col_id = col.get_column_id();
        if col_id == 1 {
            assert_eq!(col.get_tp(), FieldTypeTp::LongLong as i32);
        } else if col_id == 3 {
            assert_eq!(col.get_tp(), FieldTypeTp::VarChar as i32);
        } else {
            panic!("unexpected column id: {}", col_id);
        }
    });
}
