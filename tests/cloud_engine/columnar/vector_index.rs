// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{atomic::Ordering, Arc, Mutex},
    time::Duration,
};

use api_version::ApiV2;
use dashmap::DashMap;
use futures::executor::block_on;
use kvengine::{
    context::{new_meta_file_cache, IaCtx, PrepareType, SnapCtx},
    dfs,
    dfs::{FileType, S3Fs},
    ia::{
        manager::IaManager,
        util::{IaCapacity, IaManagerOptionsBuilder},
    },
    table::{
        columnar::{
            new_int_handle_column_info, new_version_column_info, Block, ColumnarFilterReader,
            ColumnarMetaCache, VectorIndexDef,
        },
        fts::{FtsCache, FtsDeltaCache},
        schema_file::{build_schema_file, Schema, SchemaBuf},
        sstable::BlockCache,
        vector_index::{VectorIndexCache, VectorIndexConfig, VIRTUAL_DISTANCE_COLUMN_ID},
    },
    SnapAccess,
};
use pd_client::PdClient;
use protobuf::Message;
use schema::schema::StorageClassSpec;
use test_cloud_server::{
    copr::{build_row_key, build_row_val},
    must_wait,
    oss::prepare_dfs,
    ServerCluster,
};
use tidb_query_datatype::{
    codec::table::{decode_int_handle, encode_row_key},
    expr::EvalContext,
    FieldTypeAccessor, FieldTypeTp, VECTOR_INDEX_SPEC_KEY_DISTANCE_METRIC,
    VECTOR_INDEX_SPEC_KEY_DISTANCE_METRIC_VAL_COSINE, VECTOR_INDEX_SPEC_KEY_DISTANCE_METRIC_VAL_L2,
    VECTOR_INDEX_TYPE_VECTOR32_HNSW,
};
use tikv_util::memory::MemoryLimiter;
use tipb::{AnnQueryInfo, ColumnInfo};
use txn_types::Key;

use crate::{
    alloc_node_id,
    columnar::{
        create_keyspace_and_split_tables, send_collect_columnar_index_stats_request,
        send_schema_file_request, DelegateResponse,
    },
    info, request_dump_snapshot_on_store,
};

#[test]
fn test_build_vector_index() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let (temp_dir, mut oss, dfs_config) = prepare_dfs("test_build_vector_index");
    let mut cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.kvengine
            .columnar_table_build_options
            .max_columnar_table_size = 1024;
        conf.kvengine
            .columnar_table_build_options
            .pack_max_row_count = 9;
        conf.kvengine.vector_index_build_options.delta_size = 1024;
        conf.kvengine.build_columnar = true;
        conf.kvengine.read_columnar = true;
        conf.dfs = dfs_config.clone();
    });
    let dfs = cluster.get_dfs().unwrap();
    let (keyspace_id, table_ids) = dfs
        .get_runtime()
        .block_on(create_keyspace_and_split_tables(&mut cluster));
    let t1 = table_ids[0];
    let schema = build_vector_schema(t1, tipb::VectorDistanceMetric::Cosine);
    let schema_file_data = build_schema_file(keyspace_id, 10, vec![schema.clone()], 0);
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
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                if shard.get_schema_file().is_some() {
                    return true;
                }
            }
            false
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
                build_row_key(keyspace_id, &schema, &mut guard, i)
            },
            |i| {
                let mut guard = ctx.lock().unwrap();
                build_row_val(&schema, &mut guard, i)
            },
        );
    }
    must_wait(
        || {
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                let vec_idx_files = shard.get_all_vec_idx_files();
                if !vec_idx_files.is_empty() {
                    return true;
                }
            }
            false
        },
        10,
        || "failed to build vector index file".to_string(),
    );
    let row_key = encode_row_key(t1, 500);
    let mut split_key = ApiV2::get_txn_keyspace_prefix(keyspace_id);
    split_key.extend_from_slice(&row_key);
    let split_key = Key::from_raw(&split_key);
    let pd_cli = cluster.get_pd_client();
    block_on(
        pd_cli.split_regions_with_retry(vec![split_key.into_encoded()], Duration::from_secs(10)),
    )
    .unwrap();
    let mut region_ids = vec![];
    let mut vector_files_count_after_split = 0;
    must_wait(
        || {
            let all_ids_vers = kvengine.get_all_shard_id_vers();
            let mut vector_shard_count = 0;
            vector_files_count_after_split = 0;
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                let vec_idx_files = shard.get_all_vec_idx_files();
                if !vec_idx_files.is_empty() {
                    vector_shard_count += 1;
                    vector_files_count_after_split += vec_idx_files.len();
                    if !region_ids.contains(&id_ver.id) {
                        region_ids.push(id_ver.id);
                    }
                }
            }
            vector_shard_count == 2 && vector_files_count_after_split >= 2
        },
        20,
        || "failed to split vector index region".to_string(),
    );
    let source_region = region_ids[0];
    let target_region = region_ids[1];
    pd_cli.must_merge(source_region, target_region);
    must_wait(
        || {
            let all_ids_vers = kvengine.get_all_shard_id_vers();
            let mut vector_shard_count = 0;
            for &id_ver in &all_ids_vers {
                let Some(shard) = kvengine.get_shard(id_ver.id) else {
                    continue;
                };
                let mut vec_idx_files = shard.get_all_vec_idx_files();
                if !vec_idx_files.is_empty() {
                    vector_shard_count += 1;
                    let count_before_dedup = vec_idx_files.len();
                    vec_idx_files.sort();
                    vec_idx_files.dedup();
                    assert_eq!(count_before_dedup, vec_idx_files.len());
                }
            }
            vector_shard_count == 1
        },
        20,
        || "failed to merge vector index region".to_string(),
    );
    let mut shard_snaps = vec![];
    let shard = kvengine.get_shard(target_region).unwrap();
    shard_snaps.push(shard.new_snap_access());

    // Build remote shard with vector index cache.
    let pd_client = cluster.get_pd_client();
    let store_id = kvengine.get_engine_id();
    let store = pd_client.get_store(store_id).unwrap();
    let start_ts = client.get_ts().into_inner();

    let snapshot_from_remote = dfs.get_runtime().block_on(request_dump_snapshot_on_store(
        &store, shard.id, shard.ver, start_ts,
    ));
    let mut delegate_resp = DelegateResponse::default();
    delegate_resp
        .merge_from_bytes(&snapshot_from_remote)
        .unwrap();
    let schema_files = Arc::new(DashMap::new());
    let local_path = temp_dir.path().join("ia");
    let ia_cap = IaCapacity::MemoryAndDiskCap(
        0.into(),
        vec![local_path.clone()],
        (100 * 1024 * 1024).into(),
    );
    let options = IaManagerOptionsBuilder::default()
        .capacity(ia_cap)
        .segment_size(64)
        .build()
        .unwrap();
    let s3fs = S3Fs::new_from_config(dfs_config);
    let ia_mgr = IaManager::new(
        options,
        Arc::new(s3fs.clone()),
        None,
        dfs.get_runtime().handle().clone().into(),
    )
    .unwrap();
    let vector_index_cache = VectorIndexCache::new(
        VectorIndexConfig::default(),
        dfs.get_runtime().handle().clone(),
        ia_mgr.clone(),
    );
    let ia_ctx = IaCtx::Enabled(ia_mgr, Arc::new(vec![local_path]));
    let snap_ctx = SnapCtx {
        dfs: dfs.clone(),
        master_key: kvengine.get_master_key(),
        block_cache: BlockCache::None,
        vector_index_cache: Some(vector_index_cache.clone()),
        columnar_file_cache: None,
        fts_cache: FtsCache::disabled(),
        fts_delta_cache: FtsDeltaCache::disabled(),
        schema_files: Some(schema_files.clone()),
        txn_chunk_manager: kvengine.get_txn_chunk_manager(),
        ia_ctx,
        prepare_type: PrepareType::All,
        read_columnar: true,
        meta_file_cache: new_meta_file_cache(1024 * 1024),
        columnar_meta_cache: ColumnarMetaCache::default(),
    };
    let mem_limiter = MemoryLimiter::new(u64::MAX, None);
    let remote_snap = dfs
        .get_runtime()
        .block_on(SnapAccess::construct_snapshot(
            "test",
            &snap_ctx,
            delegate_resp.get_mem_table_data(),
            delegate_resp.get_snapshot(),
            mem_limiter.clone(),
        ))
        .unwrap()
        .0;
    shard_snaps.push(remote_snap);
    // Read twice to check cache hit.
    let remote_snap = dfs
        .get_runtime()
        .block_on(SnapAccess::construct_snapshot(
            "test",
            &snap_ctx,
            delegate_resp.get_mem_table_data(),
            delegate_resp.get_snapshot(),
            mem_limiter,
        ))
        .unwrap()
        .0;
    shard_snaps.push(remote_snap);

    let target = vec![99f32, 100f32, 101f32];
    let start_table_key = encode_row_key(t1, 99);
    let end_table_key = encode_row_key(t1, 1000);
    let mut outer_start_key = ApiV2::get_txn_keyspace_prefix(keyspace_id);
    outer_start_key.extend_from_slice(&start_table_key);
    let mut outer_end_key = ApiV2::get_txn_keyspace_prefix(keyspace_id);
    outer_end_key.extend_from_slice(&end_table_key);

    for shard_snap in shard_snaps {
        let mut ann_query = AnnQueryInfo::new();
        ann_query.set_enable_distance_proj(false);
        let ann_query_arc = Arc::new(ann_query);
        let mut vector_reader = shard_snap
            .new_vector_index_reader(
                t1,
                1,
                2,
                &target,
                5,
                schema.clone(),
                u64::MAX,
                Some(&decode_int_handle(&start_table_key).unwrap().to_le_bytes()),
                Some(&decode_int_handle(&end_table_key).unwrap().to_le_bytes()),
                ann_query_arc,
            )
            .unwrap()
            .unwrap();
        block_on(vector_reader.set_int_handle_range(0, Some(1000))).unwrap();
        let mut block = Block::new(&schema);
        let cnt = block_on(vector_reader.read_block(&mut block, 5)).unwrap();
        assert!(cnt >= 3, "cnt: {}", cnt);
        // NOTE: If the block is read from columnar file, the result is not
        // sorted by distance. We should check the read result in case
        // `test_read_distance_from_vector_index_and_table`.

        let stats = vector_index_cache.stats();
        info!(
            "stats, cache_hit: {}, cache_miss: {}",
            stats.cache_hit.load(Ordering::Relaxed),
            stats.cache_miss.load(Ordering::Relaxed)
        );
    }
    let stats = vector_index_cache.stats();
    assert!(stats.cache_hit.load(Ordering::Relaxed) > 0);
    let rt = dfs.get_runtime();
    let columnar_index_stats = rt.block_on(send_collect_columnar_index_stats_request(
        &status_addr,
        keyspace_id,
    ));
    assert_eq!(columnar_index_stats.len(), 1);
    assert!(columnar_index_stats[0].indexed_columnar_rows > 0);
    cluster.stop();
    oss.shutdown();
}

#[test]
fn test_build_vector_index_with_all_rows_deleted() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let (_temp_dir, mut oss, dfs_config) =
        prepare_dfs("test_build_vector_index_with_all_rows_deleted");
    let mut cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.kvengine
            .columnar_table_build_options
            .max_columnar_table_size = 1024;
        conf.kvengine
            .columnar_table_build_options
            .pack_max_row_count = 100;
        conf.kvengine.vector_index_build_options.delta_size = 32;
        conf.kvengine.build_columnar = true;
        conf.kvengine.read_columnar = true;
        conf.dfs = dfs_config.clone();
    });
    let dfs = cluster.get_dfs().unwrap();
    let (keyspace_id, table_ids) = dfs
        .get_runtime()
        .block_on(create_keyspace_and_split_tables(&mut cluster));
    let t1 = table_ids[0];
    let schema = build_vector_schema(t1, tipb::VectorDistanceMetric::Cosine);
    let schema_file_data = build_schema_file(keyspace_id, 10, vec![schema.clone()], 0);
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
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                if shard.get_schema_file().is_some() {
                    return true;
                }
            }
            false
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
                build_row_key(keyspace_id, &schema, &mut guard, i)
            },
            |i| {
                let mut guard = ctx.lock().unwrap();
                build_row_val(&schema, &mut guard, i)
            },
        );
    }
    must_wait(
        || {
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                let vec_idx_files = shard.get_all_vec_idx_files();
                if !vec_idx_files.is_empty() {
                    return true;
                }
            }
            false
        },
        10,
        || "failed to build vector index file".to_string(),
    );
    let row_key = encode_row_key(t1, 500);
    let mut split_key = ApiV2::get_txn_keyspace_prefix(keyspace_id);
    split_key.extend_from_slice(&row_key);
    let split_key = Key::from_raw(&split_key);
    let pd_cli = cluster.get_pd_client();
    block_on(
        pd_cli.split_regions_with_retry(vec![split_key.into_encoded()], Duration::from_secs(10)),
    )
    .unwrap();
    let mut region_ids = vec![];
    let mut vector_files_count_after_split = 0;
    must_wait(
        || {
            let all_ids_vers = kvengine.get_all_shard_id_vers();
            let mut vector_shard_count = 0;
            vector_files_count_after_split = 0;
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                let vec_idx_files = shard.get_all_vec_idx_files();
                if !vec_idx_files.is_empty() {
                    vector_shard_count += 1;
                    vector_files_count_after_split += vec_idx_files.len();
                    if !region_ids.contains(&id_ver.id) {
                        region_ids.push(id_ver.id);
                    }
                }
            }
            vector_shard_count == 2 && vector_files_count_after_split >= 2
        },
        20,
        || "failed to split vector index region".to_string(),
    );

    let source_region = region_ids[0];
    let target_region = region_ids[1];

    pd_cli.must_merge(source_region, target_region);
    must_wait(
        || {
            let all_ids_vers = kvengine.get_all_shard_id_vers();
            let mut vector_shard_count = 0;
            for &id_ver in &all_ids_vers {
                let Some(shard) = kvengine.get_shard(id_ver.id) else {
                    continue;
                };
                let mut vec_idx_files = shard.get_all_vec_idx_files();
                if !vec_idx_files.is_empty() {
                    vector_shard_count += 1;
                    let count_before_dedup = vec_idx_files.len();
                    vec_idx_files.sort();
                    vec_idx_files.dedup();
                    assert_eq!(count_before_dedup, vec_idx_files.len());
                }
            }
            vector_shard_count == 1
        },
        20,
        || "failed to merge vector index region".to_string(),
    );
    let vector_index_snap_version = kvengine
        .get_shard(target_region)
        .unwrap()
        .get_vector_index(t1, 1, 2)
        .unwrap()
        .snap_version();
    // delete all rows
    client.del_kv(0..total_cnt, |i: usize| {
        let mut guard = ctx.lock().unwrap();
        build_row_key(keyspace_id, &schema, &mut guard, i)
    });

    must_wait(
        || {
            let shard = kvengine.get_shard(target_region).unwrap();
            let new_vector_index_snap_version =
                shard.get_vector_index(t1, 1, 2).unwrap().snap_version();
            info!(
                "vector_index_snap_version: {}, new_vector_index_snap_version: {}",
                vector_index_snap_version, new_vector_index_snap_version
            );
            new_vector_index_snap_version > vector_index_snap_version
        },
        10,
        || "failed to wait vector index snap version".to_string(),
    );
    let rt = dfs.get_runtime();
    let columnar_index_stats = rt.block_on(send_collect_columnar_index_stats_request(
        &status_addr,
        keyspace_id,
    ));
    assert_eq!(columnar_index_stats.len(), 1);
    assert!(columnar_index_stats[0].indexed_columnar_rows > 0);
    cluster.stop();
    oss.shutdown();
}

// metric option: COSINE/L2
fn build_vector_schema(table_id: i64, metric: tipb::VectorDistanceMetric) -> Schema {
    let pk_col_ids = vec![1];
    let mut handle_column = new_int_handle_column_info();
    handle_column.set_column_id(1);
    handle_column.set_pk_handle(true);
    let version_column = new_version_column_info();
    let mut vector_column = ColumnInfo::new();
    vector_column.set_column_id(2);
    vector_column.set_flen(3);
    vector_column.set_tp(FieldTypeTp::TiDbVectorFloat32 as i32);
    let columns = vec![vector_column];
    let mut vector_index_def = VectorIndexDef::default();
    vector_index_def.index_id = 1;
    vector_index_def.col_id = 2;
    vector_index_def.index_kind = VECTOR_INDEX_TYPE_VECTOR32_HNSW.to_string();
    match metric {
        tipb::VectorDistanceMetric::Cosine => {
            vector_index_def.specs.insert(
                VECTOR_INDEX_SPEC_KEY_DISTANCE_METRIC.to_string(),
                VECTOR_INDEX_SPEC_KEY_DISTANCE_METRIC_VAL_COSINE
                    .as_bytes()
                    .to_vec(),
            );
        }
        tipb::VectorDistanceMetric::L2 => {
            vector_index_def.specs.insert(
                VECTOR_INDEX_SPEC_KEY_DISTANCE_METRIC.to_string(),
                VECTOR_INDEX_SPEC_KEY_DISTANCE_METRIC_VAL_L2
                    .as_bytes()
                    .to_vec(),
            );
        }
        _ => {
            panic!("Unsupported vector distance metric: {:?}", metric);
        }
    }
    let vector_indexes = vec![vector_index_def];
    SchemaBuf::new(
        table_id,
        handle_column,
        version_column,
        columns,
        pk_col_ids,
        0,
        vector_indexes,
        vec![],
        StorageClassSpec::default(),
        None,
    )
    .into()
}

fn build_empty_schema(table_id: i64) -> Schema {
    let pk_col_ids = vec![1];
    let mut handle_column = new_int_handle_column_info();
    handle_column.set_column_id(1);
    handle_column.set_pk_handle(true);
    let version_column = new_version_column_info();
    let columns = vec![];
    let vector_indexes = vec![];
    let fulltext_indexes = vec![];
    SchemaBuf::new(
        table_id,
        handle_column,
        version_column,
        columns,
        pk_col_ids,
        0,
        vector_indexes,
        fulltext_indexes,
        StorageClassSpec::default(),
        None,
    )
    .into()
}

#[test]
fn test_read_distance_from_vector_index_and_table() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let mut cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.kvengine
            .columnar_table_build_options
            .max_columnar_table_size = 1024;
        conf.kvengine
            .columnar_table_build_options
            .pack_max_row_count = 9;
        conf.kvengine.vector_index_build_options.delta_size = 11 * 1024; // set the size to control the builder of vector index
        conf.kvengine.build_columnar = true;
        conf.kvengine.read_columnar = true;
    });
    let dfs = cluster.get_dfs().unwrap();
    let (keyspace_id, table_ids) = dfs
        .get_runtime()
        .block_on(create_keyspace_and_split_tables(&mut cluster));
    let t1 = table_ids[0];
    // build vector index by L2 distance.
    let schema = build_vector_schema(t1, tipb::VectorDistanceMetric::L2);
    let schema_file_data = build_schema_file(keyspace_id, 10, vec![schema.clone()], 0);
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
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                if shard.get_schema_file().is_some() {
                    return true;
                }
            }
            false
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
                build_row_key(keyspace_id, &schema, &mut guard, i)
            },
            |i| {
                let mut guard = ctx.lock().unwrap();
                build_row_val(&schema, &mut guard, i)
            },
        );
    }
    must_wait(
        || {
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                let vec_idx_files = shard.get_all_vec_idx_files();
                if !vec_idx_files.is_empty() {
                    return true;
                }
            }
            false
        },
        10,
        || "failed to build vector index file".to_string(),
    );

    // insert 500 vector rows and will not trigger index build
    let start = 1000;
    let step = 100;
    let total_cnt = 500;
    for i in (start..start + total_cnt).step_by(step) {
        client.put_kv(
            i..i + step,
            |i| {
                let mut guard = ctx.lock().unwrap();
                build_row_key(keyspace_id, &schema, &mut guard, i)
            },
            |i| {
                let mut guard = ctx.lock().unwrap();
                build_row_val(&schema, &mut guard, i)
            },
        );
    }

    let row_key = encode_row_key(t1, 500);
    let mut split_key = ApiV2::get_txn_keyspace_prefix(keyspace_id);
    split_key.extend_from_slice(&row_key);
    let split_key = Key::from_raw(&split_key);
    let pd_cli = cluster.get_pd_client();
    block_on(
        pd_cli.split_regions_with_retry(vec![split_key.into_encoded()], Duration::from_secs(10)),
    )
    .unwrap();
    let mut region_ids = vec![];
    let mut vector_files_count_after_split = 0;
    must_wait(
        || {
            let all_ids_vers = kvengine.get_all_shard_id_vers();
            let mut vector_shard_count = 0;
            vector_files_count_after_split = 0;
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                let vec_idx_files = shard.get_all_vec_idx_files();
                if !vec_idx_files.is_empty() {
                    vector_shard_count += 1;
                    vector_files_count_after_split += vec_idx_files.len();
                    if !region_ids.contains(&id_ver.id) {
                        region_ids.push(id_ver.id);
                    }
                }
            }
            vector_shard_count == 2 && vector_files_count_after_split >= 2
        },
        20,
        || "failed to split vector index region".to_string(),
    );
    let source_region = region_ids[0];
    let target_region = region_ids[1];
    pd_cli.must_merge(source_region, target_region);
    must_wait(
        || {
            let all_ids_vers = kvengine.get_all_shard_id_vers();
            let mut vector_shard_count = 0;
            for &id_ver in &all_ids_vers {
                let Some(shard) = kvengine.get_shard(id_ver.id) else {
                    continue;
                };
                let mut vec_idx_files = shard.get_all_vec_idx_files();
                if !vec_idx_files.is_empty() {
                    vector_shard_count += 1;
                    let count_before_dedup = vec_idx_files.len();
                    vec_idx_files.sort();
                    vec_idx_files.dedup();
                    assert_eq!(count_before_dedup, vec_idx_files.len());
                }
            }
            vector_shard_count == 1
        },
        20,
        || "failed to merge vector index region".to_string(),
    );
    let shard = kvengine.get_shard(target_region).unwrap();
    let target = vec![1872f32, 324f32, 1024f32];
    let start_table_key = encode_row_key(t1, 99);
    let end_table_key = encode_row_key(t1, 1500);
    let mut outer_start_key = ApiV2::get_txn_keyspace_prefix(keyspace_id);
    outer_start_key.extend_from_slice(&start_table_key);
    let mut outer_end_key = ApiV2::get_txn_keyspace_prefix(keyspace_id);
    outer_end_key.extend_from_slice(&end_table_key);

    // test read distance from vector index
    let mut encoded_query_vector: Vec<u8> = (target.len() as u32).to_le_bytes().to_vec();
    encoded_query_vector.extend(target.iter().flat_map(|f| f.to_le_bytes()));

    let mut ann_query = AnnQueryInfo::new();
    let mut vector_column = ColumnInfo::new();
    vector_column.set_column_id(2);
    vector_column.set_flen(3);
    vector_column.set_tp(FieldTypeTp::TiDbVectorFloat32 as i32);
    vector_column.set_flag(schema.columns[0].get_flag());
    ann_query.set_column(vector_column);
    ann_query.set_distance_metric(tipb::VectorDistanceMetric::L2);
    ann_query.set_enable_distance_proj(true);
    ann_query.set_ref_vec_f32(encoded_query_vector);

    let schema_dis = build_vector_schema_with_distance(t1);
    let ann_query_arc = Arc::new(ann_query);

    // Read from no index data(range 1000~1500)
    // Test whether the distance result can be returned by reading the vector
    // according to the distance schema when creating a Vector Index Reader and
    // returning None.
    {
        let mut columnar_reader = shard
            .new_snap_access()
            .new_columnar_mvcc_reader(
                schema.table_id,
                &schema_dis.columns,
                None,
                u64::MAX,
                Some(ann_query_arc.clone()),
            )
            .unwrap()
            .unwrap();
        block_on(columnar_reader.set_int_handle_range(1000, Some(1500))).unwrap();
        let mut block = Block::new(&schema_dis);
        assert_eq!(block.get_columns().len(), 1);
        let cnt = block_on(columnar_reader.read_block(&mut block, usize::MAX)).unwrap();
        assert!(cnt >= 3, "cnt: {}", cnt);
        let handle_buf = block.get_handle_buf();
        let dis_col_buf = &block.get_columns()[0];
        let mut all_items: Vec<DistanceItem> = (0..cnt)
            .filter_map(|i| {
                dis_col_buf.get_value(i).and_then(|dis_val| {
                    keep_decimal_points(decode_f64_vec_from_bytes(dis_val))
                        .first()
                        .map(|&distance| DistanceItem { distance, index: i })
                })
            })
            .collect();
        all_items.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        let topn = all_items.into_iter().take(3).collect::<Vec<_>>();
        let cases = [(1072, 1097.0419), (1073, 1097.0423), (1071, 1097.0442)];
        for (i, (index, distance)) in cases.iter().enumerate() {
            assert_eq!(
                handle_buf.get_int_handle_value(topn[i].index),
                *index as i64
            );
            assert_eq!(topn[i].distance, *distance);
        }
    }

    let mut vector_reader = shard
        .new_snap_access()
        .new_vector_index_reader(
            t1,
            1,
            2,
            &target,
            10,
            schema_dis.clone(),
            u64::MAX,
            Some(&decode_int_handle(&start_table_key).unwrap().to_le_bytes()),
            Some(&decode_int_handle(&end_table_key).unwrap().to_le_bytes()),
            ann_query_arc,
        )
        .unwrap()
        .unwrap();
    block_on(vector_reader.set_int_handle_range(0, Some(1500))).unwrap();
    let mut block = Block::new(&schema_dis);
    assert_eq!(block.get_columns().len(), 1);

    // will read 510 rows. (10 rows indexed data and 500 rows not-indexed data)
    let cnt = block_on(vector_reader.read_block(&mut block, 1500)).unwrap();
    assert!(cnt >= 3, "cnt: {}", cnt);
    let dis_col_buf = &block.get_columns()[0];
    let mut all_items: Vec<DistanceItem> = (0..cnt)
        .filter_map(|i| {
            dis_col_buf.get_value(i).and_then(|dis_val| {
                keep_decimal_points(decode_f64_vec_from_bytes(dis_val))
                    .first()
                    .map(|&distance| DistanceItem { distance, index: i })
            })
        })
        .collect();
    all_items.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
    for item in &all_items {
        match item.index {
            988 => assert_eq!(item.distance, 1106.7235),
            1001 => assert_eq!(item.distance, 1103.9773),
            1002 => assert_eq!(item.distance, 1103.7848),
            1499 => assert_eq!(item.distance, 1322.7373),
            _ => {}
        }
    }
    let rt = dfs.get_runtime();
    let columnar_index_stats = rt.block_on(send_collect_columnar_index_stats_request(
        &status_addr,
        keyspace_id,
    ));
    assert_eq!(columnar_index_stats.len(), 1);
    assert!(columnar_index_stats[0].indexed_columnar_rows > 0);
    cluster.stop();
}

#[test]
fn test_read_distance_from_vector_index() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let mut cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.kvengine
            .columnar_table_build_options
            .max_columnar_table_size = 1024;
        conf.kvengine
            .columnar_table_build_options
            .pack_max_row_count = 9;
        conf.kvengine.vector_index_build_options.delta_size = 1024;
        conf.kvengine.build_columnar = true;
        conf.kvengine.read_columnar = true;
    });
    let dfs = cluster.get_dfs().unwrap();
    let (keyspace_id, table_ids) = dfs
        .get_runtime()
        .block_on(create_keyspace_and_split_tables(&mut cluster));
    let t1 = table_ids[0];
    // build vector index by L2 distance
    let schema = build_vector_schema(t1, tipb::VectorDistanceMetric::L2);
    let schema_file_data = build_schema_file(keyspace_id, 10, vec![schema.clone()], 0);
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
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                if shard.get_schema_file().is_some() {
                    return true;
                }
            }
            false
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
                build_row_key(keyspace_id, &schema, &mut guard, i)
            },
            |i| {
                let mut guard = ctx.lock().unwrap();
                build_row_val(&schema, &mut guard, i)
            },
        );
    }
    must_wait(
        || {
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                let vec_idx_files = shard.get_all_vec_idx_files();
                if !vec_idx_files.is_empty() {
                    return true;
                }
            }
            false
        },
        10,
        || "failed to build vector index file".to_string(),
    );
    let row_key = encode_row_key(t1, 500);
    let mut split_key = ApiV2::get_txn_keyspace_prefix(keyspace_id);
    split_key.extend_from_slice(&row_key);
    let split_key = Key::from_raw(&split_key);
    let pd_cli = cluster.get_pd_client();
    block_on(
        pd_cli.split_regions_with_retry(vec![split_key.into_encoded()], Duration::from_secs(10)),
    )
    .unwrap();
    let mut region_ids = vec![];
    let mut vector_files_count_after_split = 0;
    must_wait(
        || {
            let all_ids_vers = kvengine.get_all_shard_id_vers();
            let mut vector_shard_count = 0;
            vector_files_count_after_split = 0;
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                let vec_idx_files = shard.get_all_vec_idx_files();
                if !vec_idx_files.is_empty() {
                    vector_shard_count += 1;
                    vector_files_count_after_split += vec_idx_files.len();
                    if !region_ids.contains(&id_ver.id) {
                        region_ids.push(id_ver.id);
                    }
                }
            }
            vector_shard_count == 2 && vector_files_count_after_split >= 2
        },
        20,
        || "failed to split vector index region".to_string(),
    );
    let source_region = region_ids[0];
    let target_region = region_ids[1];
    pd_cli.must_merge(source_region, target_region);
    must_wait(
        || {
            let all_ids_vers = kvengine.get_all_shard_id_vers();
            let mut vector_shard_count = 0;
            for &id_ver in &all_ids_vers {
                let Some(shard) = kvengine.get_shard(id_ver.id) else {
                    continue;
                };
                let mut vec_idx_files = shard.get_all_vec_idx_files();
                if !vec_idx_files.is_empty() {
                    vector_shard_count += 1;
                    let count_before_dedup = vec_idx_files.len();
                    vec_idx_files.sort();
                    vec_idx_files.dedup();
                    assert_eq!(count_before_dedup, vec_idx_files.len());
                }
            }
            vector_shard_count == 1
        },
        20,
        || "failed to merge vector index region".to_string(),
    );
    let shard = kvengine.get_shard(target_region).unwrap();
    let target = vec![1872f32, 324f32, 1024f32];
    let start_table_key = encode_row_key(t1, 99);
    let end_table_key = encode_row_key(t1, 1000);
    let mut outer_start_key = ApiV2::get_txn_keyspace_prefix(keyspace_id);
    outer_start_key.extend_from_slice(&start_table_key);
    let mut outer_end_key = ApiV2::get_txn_keyspace_prefix(keyspace_id);
    outer_end_key.extend_from_slice(&end_table_key);

    // test read distance from vector index
    let mut encoded_query_vector: Vec<u8> = (target.len() as u32).to_le_bytes().to_vec();
    encoded_query_vector.extend(target.iter().flat_map(|f| f.to_le_bytes()));

    let mut ann_query = AnnQueryInfo::new();
    let mut vector_column = ColumnInfo::new();
    vector_column.set_column_id(2);
    vector_column.set_flen(3);
    vector_column.set_tp(FieldTypeTp::TiDbVectorFloat32 as i32);
    vector_column.set_flag(schema.columns[0].get_flag());
    ann_query.set_column(vector_column);
    ann_query.set_distance_metric(tipb::VectorDistanceMetric::L2);
    ann_query.set_enable_distance_proj(true);
    ann_query.set_ref_vec_f32(encoded_query_vector);

    let schema_dis = build_vector_schema_with_distance(t1);
    let ann_query_arc = Arc::new(ann_query);

    let mut vector_reader = shard
        .new_snap_access()
        .new_vector_index_reader(
            t1,
            1,
            2,
            &target,
            5,
            schema_dis.clone(),
            u64::MAX,
            Some(&decode_int_handle(&start_table_key).unwrap().to_le_bytes()),
            Some(&decode_int_handle(&end_table_key).unwrap().to_le_bytes()),
            ann_query_arc,
        )
        .unwrap()
        .unwrap();
    block_on(vector_reader.set_int_handle_range(0, Some(1000))).unwrap();
    let mut block = Block::new(&schema_dis);
    assert_eq!(block.get_columns().len(), 1);
    let cnt = block_on(vector_reader.read_block(&mut block, usize::MAX)).unwrap();
    assert!(cnt >= 3, "cnt: {}", cnt);
    let handle_buf = block.get_handle_buf();
    let dis_col_buf = &block.get_columns()[0];
    let mut all_items: Vec<DistanceItem> = (0..cnt)
        .filter_map(|i| {
            dis_col_buf.get_value(i).and_then(|dis_val| {
                keep_decimal_points(decode_f64_vec_from_bytes(dis_val))
                    .first()
                    .map(|&distance| DistanceItem { distance, index: i })
            })
        })
        .collect();
    all_items.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
    let topn = all_items.into_iter().take(5).collect::<Vec<_>>();
    let cases = [
        (999, 1104.3703),
        (998, 1104.5709),
        (997, 1104.7741),
        (996, 1104.9801),
        (994, 1105.3999),
    ];
    for (i, (index, distance)) in cases.iter().enumerate() {
        assert_eq!(
            handle_buf.get_int_handle_value(topn[i].index),
            *index as i64
        );
        assert_eq!(topn[i].distance, *distance);
    }
    let rt = dfs.get_runtime();
    let columnar_index_stats = rt.block_on(send_collect_columnar_index_stats_request(
        &status_addr,
        keyspace_id,
    ));
    assert_eq!(columnar_index_stats.len(), 1);
    assert!(columnar_index_stats[0].indexed_columnar_rows > 0);
    cluster.stop();
}

#[test]
fn test_drop_vector_index() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test_drop_vector_index");
    let mut cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.kvengine
            .columnar_table_build_options
            .max_columnar_table_size = 1024;
        conf.kvengine
            .columnar_table_build_options
            .pack_max_row_count = 9;
        conf.kvengine.vector_index_build_options.delta_size = 1024;
        conf.kvengine.build_columnar = true;
        conf.kvengine.read_columnar = true;
        conf.dfs = dfs_config.clone();
    });
    let dfs = cluster.get_dfs().unwrap();
    let (keyspace_id, table_ids) = dfs
        .get_runtime()
        .block_on(create_keyspace_and_split_tables(&mut cluster));
    let t1 = table_ids[0];
    let schema = build_vector_schema(t1, tipb::VectorDistanceMetric::Cosine);
    let schema_file_data = build_schema_file(keyspace_id, 10, vec![schema.clone()], 0);
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
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                if shard.get_schema_file().is_some() {
                    return true;
                }
            }
            false
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
                build_row_key(keyspace_id, &schema, &mut guard, i)
            },
            |i| {
                let mut guard = ctx.lock().unwrap();
                build_row_val(&schema, &mut guard, i)
            },
        );
    }
    must_wait(
        || {
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                let vec_idx_files = shard.get_all_vec_idx_files();
                if !vec_idx_files.is_empty() {
                    return true;
                }
            }
            false
        },
        10,
        || "failed to build vector index file".to_string(),
    );

    // Drop vector index in schema
    let no_vector_index_schema = build_empty_schema(t1);
    let schema_file_data = build_schema_file(keyspace_id, 11, vec![no_vector_index_schema], 0);
    let new_schema_file_id = schema_file_id + 1;
    dfs.get_runtime()
        .block_on(dfs.create(new_schema_file_id, schema_file_data.into(), opts))
        .unwrap();
    must_wait(
        || {
            dfs.get_runtime().block_on(send_schema_file_request(
                &status_addr,
                keyspace_id,
                new_schema_file_id,
            ));
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                if shard.get_schema_file().is_some() {
                    return true;
                }
            }
            false
        },
        10,
        || "failed to wait schema file".to_string(),
    );
    must_wait(
        || {
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                let vec_idx_files = shard.get_all_vec_idx_files();
                if !vec_idx_files.is_empty() {
                    return false;
                }
            }
            true
        },
        10,
        || "failed to drop vector index file".to_string(),
    );
    let rt = dfs.get_runtime();
    let columnar_index_stats = rt.block_on(send_collect_columnar_index_stats_request(
        &status_addr,
        keyspace_id,
    ));
    assert!(columnar_index_stats.is_empty());
    cluster.stop();
    oss.shutdown();
}

// only for L2 distance
fn build_vector_schema_with_distance(table_id: i64) -> Schema {
    let pk_col_ids = vec![1];
    let mut handle_column = new_int_handle_column_info();
    handle_column.set_column_id(1);
    handle_column.set_pk_handle(true);
    let version_column = new_version_column_info();
    let mut distance_column = ColumnInfo::new();
    distance_column.set_column_id(VIRTUAL_DISTANCE_COLUMN_ID);

    distance_column.set_tp(FieldTypeTp::Float as i32);
    let columns = vec![distance_column];
    let mut vector_index_def = VectorIndexDef::default();
    vector_index_def.index_id = 1;
    vector_index_def.col_id = 2;
    vector_index_def.index_kind = VECTOR_INDEX_TYPE_VECTOR32_HNSW.to_string();
    vector_index_def.specs.insert(
        VECTOR_INDEX_SPEC_KEY_DISTANCE_METRIC.to_string(),
        VECTOR_INDEX_SPEC_KEY_DISTANCE_METRIC_VAL_L2
            .as_bytes()
            .to_vec(),
    );
    let vector_indexes = vec![vector_index_def];
    SchemaBuf::new(
        table_id,
        handle_column,
        version_column,
        columns,
        pk_col_ids,
        0,
        vector_indexes,
        vec![],
        StorageClassSpec::default(),
        None,
    )
    .into()
}

fn decode_f64_vec_from_bytes(bytes: &[u8]) -> Vec<f64> {
    bytes
        .chunks_exact(8)
        .map(|chunk| {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(chunk);
            f64::from_le_bytes(buf)
        })
        .collect()
}

// keep 4 decimal points of value
fn keep_decimal_points(vec: Vec<f64>) -> Vec<f64> {
    let mut new_vec: Vec<f64> = vec![];
    for val in vec {
        new_vec.push((val * 1e4).trunc() / 1e4);
    }
    new_vec
}

#[derive(Debug, Clone, PartialEq)]
struct DistanceItem {
    distance: f64,
    index: usize,
}

impl Eq for DistanceItem {}

impl PartialOrd for DistanceItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DistanceItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}
