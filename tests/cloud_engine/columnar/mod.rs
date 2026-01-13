// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

mod copr;
mod vector_index;

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use api_version::ApiV2;
use bytes::Buf;
use codec::number::NumberEncoder;
use dashmap::DashMap;
use futures::{TryStreamExt, executor::block_on, future::ok};
use hyper::Body;
use kvengine::{
    ColumnarIndexStats, ColumnarStatusResp, Engine, STORAGE_CLASS_KEY, SnapAccess, WRITE_CF,
    context::{IaCtx, PrepareType, SnapCtx, new_meta_file_cache},
    dfs,
    dfs::{FileType, S3Fs},
    ia::{
        manager::IaManager,
        util::{IaCapacity, IaManagerOptionsBuilder},
    },
    table::{
        columnar,
        columnar::{
            ColumnarFilterReader, ColumnarMetaCache, filter::TableScanCtx,
            new_int_handle_column_info, new_version_column_info,
        },
        fts::{FtsCache, FtsDeltaCache},
        schema_file::{Schema, SchemaBuf, build_schema_file},
        sstable::{BlockCache, BlockCacheType},
    },
};
use kvproto::coprocessor::DelegateResponse;
use pd_client::PdClient;
use protobuf::Message;
use rand::Rng;
use schema::schema::{StorageClass, StorageClassSpec};
use test_cloud_server::{
    ServerCluster,
    client::{CommitAction, MutateOptions},
    keyspace::CreateKeyspaceOptions,
    must_wait,
    oss::prepare_dfs,
};
use test_pd_client::PdClientExt;
use tidb_query_datatype::{
    Collation, FieldTypeAccessor, FieldTypeTp,
    codec::{
        row::v2::encoder_for_test::{Column, RowEncoder},
        table::encode_row_key,
    },
    expr::EvalContext,
};
use tikv_util::{
    codec::bytes::encode_bytes,
    config::{AbsoluteOrPercentSize, ReadableDuration, ReadableSize},
    info,
    memory::MemoryLimiter,
};
use tipb::ColumnInfo;

use crate::{alloc_node_id, destroy_range, request_dump_snapshot_on_store};

const SEGMENT_SIZE: i64 = 64;
const FREQ_UPDATE_INTERVAL: Duration = Duration::from_secs(1);

/// Helper struct to manage background writer thread lifecycle.
/// Automatically stops the thread when dropped.
struct BackgroundWriter {
    handle: Option<std::thread::JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

impl BackgroundWriter {
    fn new<F>(task: F) -> Self
    where
        F: FnOnce(Arc<AtomicBool>) + Send + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();

        let handle = std::thread::spawn(move || {
            task(stop_clone);
        });

        BackgroundWriter {
            handle: Some(handle),
            stop,
        }
    }
}

impl Drop for BackgroundWriter {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[test]
fn test_schema_file() {
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
    });
    let dfs = cluster.get_dfs().unwrap();
    let (keyspace_id, table_ids) = dfs
        .get_runtime()
        .block_on(create_keyspace_and_split_tables(&mut cluster));
    let schemas = build_schemas(vec![table_ids[1], table_ids[3]]);
    let schema_version = 10;
    let schema_file_data = build_schema_file(keyspace_id, schema_version, schemas, 0);
    let schema_file_id = 100;
    let opts = dfs::Options::default().with_type(FileType::Schema);
    dfs.get_runtime()
        .block_on(dfs.create(schema_file_id, schema_file_data.into(), opts))
        .unwrap();
    let status_addr = cluster.status_addr(node_id);

    let mut client = cluster.new_client();
    let ctx = Mutex::new(EvalContext::default());
    client.put_kv(
        0..100,
        |i: usize| gen_row_key(keyspace_id, table_ids[1], i),
        |i: usize| gen_row_val(&ctx, i),
    );

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

    // test schema_file will be set to None if no longer overlap.
    let new_schemas = build_schemas(vec![table_ids[1]]);
    let new_schema_version = 11;
    let new_schema_file_data = build_schema_file(keyspace_id, new_schema_version, new_schemas, 0);
    let new_schema_file_id = 101;
    dfs.get_runtime()
        .block_on(dfs.create(new_schema_file_id, new_schema_file_data.into(), opts))
        .unwrap();

    must_wait(
        || {
            dfs.get_runtime().block_on(send_schema_file_request(
                &status_addr,
                keyspace_id,
                new_schema_file_id,
            ));
            let mut shard_with_schema_file_ids = vec![];
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                if shard.get_schema_file().is_some() {
                    let schema_file_id = shard.get_schema_file().unwrap().get_file_id();
                    shard_with_schema_file_ids.push(schema_file_id);
                }
            }
            shard_with_schema_file_ids.len() == 1
                && shard_with_schema_file_ids[0] == new_schema_file_id
        },
        5,
        || "failed to wait schema file".to_string(),
    );

    // test schema file will not update if version is older.
    dfs.get_runtime().block_on(send_schema_file_request(
        &status_addr,
        keyspace_id,
        schema_file_id,
    ));
    std::thread::sleep(Duration::from_secs(3));
    must_wait(
        || {
            for &id_ver in &all_ids_vers {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                if shard.get_schema_file().is_some() {
                    let sf = shard.get_schema_file().unwrap();
                    if sf.is_tombstone() {
                        assert_eq!(sf.get_version(), new_schema_version);
                    } else {
                        assert_eq!(sf.get_file_id(), new_schema_file_id);
                    }
                    let stats = shard.get_stats();
                    if stats.schema_version != new_schema_version {
                        return false;
                    }
                }
            }
            true
        },
        3,
        || "failed to wait schema file".to_string(),
    );

    // test collect columnar status api
    must_wait(
        || {
            let columnar_status = dfs
                .get_runtime()
                .block_on(send_collect_columnar_status_request(
                    &status_addr,
                    keyspace_id,
                    table_ids[1],
                ));
            info!("columnar status: {:#?}", columnar_status);
            columnar_status.total > 0 && columnar_status.ready == columnar_status.total
        },
        5,
        || "columnar status is not ready".to_string(),
    );
}

#[test]
fn test_covert_row_to_columnar_with_ia() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test_covert_row_to_columnar_with_ia");

    let mut cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.kvengine
            .columnar_table_build_options
            .max_columnar_table_size = 1024;
        conf.kvengine
            .columnar_table_build_options
            .pack_max_row_count = 9;
        conf.kvengine.build_columnar = true;
        conf.kvengine.read_columnar = true;
        conf.dfs = dfs_config.clone();
        conf.kvengine.ia.segment_size = SEGMENT_SIZE;
        conf.kvengine.ia.freq_update_interval = ReadableDuration(FREQ_UPDATE_INTERVAL);
        conf.kvengine.ia.mem_cap = AbsoluteOrPercentSize::Abs(ReadableSize(1024 * 128));
        conf.kvengine.ia.disk_cap = AbsoluteOrPercentSize::Abs(ReadableSize(1024 * 1024));
    });
    let dfs = cluster.get_dfs().unwrap();
    let (keyspace_id, table_ids) = dfs
        .get_runtime()
        .block_on(create_keyspace_and_split_tables(&mut cluster));
    let table_id = table_ids[1];
    let schemas = build_schemas(vec![table_id]);
    let schema = schemas[0].clone();
    let schema_version = 10;
    let schema_file_data = build_schema_file(keyspace_id, schema_version, schemas, 0);
    let schema_file_id = 100;
    let opts = dfs::Options::default().with_type(FileType::Schema);
    dfs.get_runtime()
        .block_on(dfs.create(schema_file_id, schema_file_data.into(), opts))
        .unwrap();
    let status_addr = cluster.status_addr(node_id);

    let kvengine = cluster.get_kvengine(node_id);
    assert!(kvengine.is_ia_enabled());
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
    client.put_kv(
        0..100,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
    client.put_kv(
        100..200,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
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
        || {
            // dump shard info
            let all_id_vers = kvengine.get_all_shard_id_vers();
            for id_ver in all_id_vers {
                if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    info!(
                        "shard: {:?}, columnar_table_ids: {:?}",
                        id_ver,
                        shard.get_columnar_table_ids()
                    );
                }
            }
            "failed to build columnar file".to_string()
        },
    );
    let shard_id = shard_id.unwrap();
    let shard = kvengine.get_shard(shard_id).unwrap();
    let snap_access = shard.new_snap_access();
    let ts = client.get_ts().into_inner();
    let mut columnar_reader = snap_access
        .new_columnar_mvcc_reader(schema.table_id, &schema.columns, None, ts, None)
        .unwrap()
        .unwrap();
    block_on(columnar_reader.set_int_handle_range(0, Some(190))).unwrap();
    let mut block = columnar::Block::new(&schema);
    let read_rows = block_on(columnar_reader.read_block(&mut block, 200)).unwrap();
    assert_eq!(read_rows, 190);
    for i in 0..read_rows {
        let handle = block.get_handle_buf().get_int_handle_value(i);
        assert_eq!(handle, i as i64);
        let columns = block.get_columns();
        assert_eq!(columns[0].get_not_null_value(i).get_i64_le(), i as i64);
        let str_val = gen_str_val(i);
        assert_eq!(columns[1].get_not_null_value(i), &str_val);
    }

    cluster.stop();
    oss.shutdown();
}

#[test]
fn test_sst_and_columnar_with_ia() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test_sst_and_columnar_with_ia");

    let mut cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.kvengine
            .columnar_table_build_options
            .max_columnar_table_size = 1024;
        conf.kvengine
            .columnar_table_build_options
            .pack_max_row_count = 9;
        conf.kvengine.build_columnar = true;
        conf.kvengine.read_columnar = true;
        conf.rocksdb.writecf.write_buffer_size = ReadableSize::kb(1);
        conf.rocksdb.writecf.block_size = ReadableSize(512);
        conf.rocksdb.writecf.target_file_size_base = ReadableSize::kb(2);
        conf.coprocessor.region_split_size = ReadableSize::kb(512);
        conf.coprocessor.region_bucket_size = ReadableSize::kb(64);
        conf.storage.block_cache.capacity = Some(ReadableSize::kb(16));
        conf.dfs = dfs_config.clone();
        conf.kvengine.ia.segment_size = SEGMENT_SIZE;
        conf.kvengine.ia.freq_update_interval = ReadableDuration(FREQ_UPDATE_INTERVAL);
        conf.kvengine.ia.mem_cap = AbsoluteOrPercentSize::Abs(ReadableSize::kb(64));
        conf.kvengine.ia.disk_cap = AbsoluteOrPercentSize::Abs(ReadableSize::kb(1024));
        conf.kvengine.block_cache_type = BlockCacheType::Quick;
    });
    let dfs = cluster.get_dfs().unwrap();
    let (keyspace_id, table_ids) = dfs
        .get_runtime()
        .block_on(create_keyspace_and_split_tables(&mut cluster));
    let table_id = table_ids[1];
    let mut schema_buf = build_columnar_schema_buf(table_id);
    let sc_spec_ia = StorageClassSpec::from(StorageClass::Ia);
    schema_buf.set_storage_class_spec(sc_spec_ia.clone());
    let schemas: Vec<Schema> = vec![schema_buf.clone().into()];
    let schema = schemas[0].clone();
    let schema_version = 10;
    let schema_file_data = build_schema_file(keyspace_id, schema_version, schemas, 0);
    let schema_file_id = 100;
    let opts = dfs::Options::default().with_type(FileType::Schema);
    dfs.get_runtime()
        .block_on(dfs.create(schema_file_id, schema_file_data.into(), opts))
        .unwrap();
    let status_addr = cluster.status_addr(node_id);

    let kvengine = cluster.get_kvengine(node_id);
    assert!(kvengine.is_ia_enabled());
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
                    if shard.get_schema_file().is_some()
                        && shard.storage_class_spec_equals(&sc_spec_ia)
                    {
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
    client.put_kv(
        0..100,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
    client.put_kv(
        100..200,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
    let mut shard_id = None;
    must_wait(
        || {
            let all_id_vers = kvengine.get_all_shard_id_vers();
            for id_ver in all_id_vers {
                if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    let stats = shard.get_stats();
                    let snap_version = shard.get_persisted_snap_version();
                    let columnar_snap_version = shard.get_columnar_snap_version();
                    if stats.cfs[0].levels[0].num_tables > 0
                        && snap_version == columnar_snap_version
                    {
                        let (_, sst_ia_file_ids) = shard.get_local_sst_files();
                        assert!(!sst_ia_file_ids.is_empty());
                        shard_id = Some(id_ver.id);
                        return true;
                    }
                }
            }
            false
        },
        10,
        || {
            // dump shard info
            let all_id_vers = kvengine.get_all_shard_id_vers();
            for id_ver in all_id_vers {
                if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    let snap_version = shard.get_persisted_snap_version();
                    let columnar_snap_version = shard.get_columnar_snap_version();
                    info!(
                        "shard: {:?}, snap_version: {}, columnar_snap_version: {}",
                        id_ver, snap_version, columnar_snap_version
                    );
                }
            }
            "failed to build columnar file".to_string()
        },
    );
    client.verify_data_with_ref_store();
    let shard_id = shard_id.unwrap();
    let shard = kvengine.get_shard(shard_id).unwrap();
    let snap_access = shard.new_snap_access();
    let ts = client.get_ts().into_inner();
    let mut columnar_reader = snap_access
        .new_columnar_mvcc_reader(schema.table_id, &schema.columns, None, ts, None)
        .unwrap()
        .unwrap();
    block_on(columnar_reader.set_int_handle_range(0, Some(190))).unwrap();
    let mut block = columnar::Block::new(&schema);
    let read_rows = block_on(columnar_reader.read_block(&mut block, 200)).unwrap();
    assert_eq!(read_rows, 190);
    for i in 0..read_rows {
        let handle = block.get_handle_buf().get_int_handle_value(i);
        assert_eq!(handle, i as i64);
        let columns = block.get_columns();
        assert_eq!(columns[0].get_not_null_value(i).get_i64_le(), i as i64);
        let str_val = gen_str_val(i);
        assert_eq!(columns[1].get_not_null_value(i), &str_val);
    }

    // Change IA files to Local amd remove storage class property
    let mut schema_buf_1 = schema_buf.clone();
    schema_buf_1.set_storage_class(StorageClass::Unspecified);
    let new_schemas: Vec<Schema> = vec![schema_buf_1.into()];
    let new_schema_version = 11;
    let new_schema_file_data = build_schema_file(keyspace_id, new_schema_version, new_schemas, 0);
    let new_schema_file_id = 101;
    dfs.get_runtime()
        .block_on(dfs.create(new_schema_file_id, new_schema_file_data.into(), opts))
        .unwrap();
    must_wait(
        || {
            dfs.get_runtime().block_on(send_schema_file_request(
                &status_addr,
                keyspace_id,
                new_schema_file_id,
            ));
            let all_id_vers = kvengine.get_all_shard_id_vers();
            for id_ver in all_id_vers {
                if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    if shard.get_schema_file().is_some()
                        && !shard.get_storage_class_spec().is_specified()
                    {
                        assert!(shard.get_property(STORAGE_CLASS_KEY).is_none());
                        let (_, sst_ia_file_ids) = shard.get_local_sst_files();
                        assert!(sst_ia_file_ids.is_empty());
                        return true;
                    }
                }
            }
            false
        },
        10,
        || "failed to reload local file and remove storage class property".to_string(),
    );
    client.verify_data_with_ref_store();

    cluster.stop();
    oss.shutdown();
}

#[test]
fn test_get_snapshot_from_leader_by_status_api() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    // prepare dfs
    let (_temp_dir, mut oss, dfs_config) =
        prepare_dfs("test_get_snapshot_from_leader_by_status_api");
    let mut cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.kvengine
            .columnar_table_build_options
            .max_columnar_table_size = 1024;
        conf.kvengine
            .columnar_table_build_options
            .pack_max_row_count = 9;
        conf.kvengine.build_columnar = true;
        conf.dfs = dfs_config.clone();
    });
    let dfs = cluster.get_dfs().unwrap();
    let (keyspace_id, table_ids) = dfs
        .get_runtime()
        .block_on(create_keyspace_and_split_tables(&mut cluster));
    let table_id = table_ids[1];
    let schemas = build_schemas(vec![table_id]);
    let schema_version = 10;
    let schema_file_data = build_schema_file(keyspace_id, schema_version, schemas, 0);
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
    let master_key = kvengine.get_master_key();
    let mut client = cluster.new_client();
    let eval_ctx = Mutex::new(EvalContext::default());
    client.put_kv(
        0..100,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i: usize| gen_row_val(&eval_ctx, i),
    );
    client.put_kv(
        100..200,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i: usize| gen_row_val(&eval_ctx, i),
    );
    must_wait(
        || {
            let all_id_vers = kvengine.get_all_shard_id_vers();
            for id_ver in all_id_vers {
                if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    if shard.get_columnar_table_ids().contains(&table_id) {
                        return true;
                    }
                }
            }
            false
        },
        10,
        || "failed to build columnar file".to_string(),
    );

    let pd_client = cluster.get_pd_client();
    let shard = pd_client
        .get_region(&encode_bytes(&gen_row_key(keyspace_id, table_id, 0)))
        .unwrap();

    let store_id = kvengine.get_engine_id();
    let store = pd_client.get_store(store_id).unwrap();
    let start_ts = client.get_ts().into_inner();

    // return region epoch not match in header
    let snapshot_from_remote = dfs.get_runtime().block_on(request_dump_snapshot_on_store(
        &store,
        shard.id,
        shard.region_epoch.as_ref().unwrap().version - 1,
        start_ts,
    ));
    let mut delegate_resp = DelegateResponse::default();
    delegate_resp
        .merge_from_bytes(&snapshot_from_remote)
        .unwrap();
    assert!(delegate_resp.get_region_error().has_epoch_not_match());
    assert!(delegate_resp.get_mem_table_data().is_empty());
    assert!(delegate_resp.get_snapshot().is_empty());

    let snapshot_from_remote = dfs.get_runtime().block_on(request_dump_snapshot_on_store(
        &store,
        shard.id,
        shard.region_epoch.as_ref().unwrap().version,
        start_ts,
    ));
    delegate_resp
        .merge_from_bytes(&snapshot_from_remote)
        .unwrap();
    let schema_files = Arc::new(DashMap::new());
    let snap_ctx = SnapCtx {
        dfs: dfs.clone(),
        master_key,
        block_cache: BlockCache::None,
        vector_index_cache: None,
        columnar_file_cache: None,
        fts_cache: FtsCache::disabled(),
        fts_delta_cache: FtsDeltaCache::disabled(),
        schema_files: Some(schema_files.clone()),
        txn_chunk_manager: kvengine.get_txn_chunk_manager(),
        ia_ctx: IaCtx::Disabled,
        prepare_type: PrepareType::All,
        read_columnar: true,
        meta_file_cache: new_meta_file_cache(1024 * 1024),
        columnar_meta_cache: ColumnarMetaCache::default(),
    };
    let mem_limiter = MemoryLimiter::new(u64::MAX, None);
    let snap_access = dfs
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
    assert!(snap_access.has_schema_file());
    assert!(schema_files.contains_key(&schema_file_id));

    let mut iter = snap_access.new_iterator(WRITE_CF, false, true, Some(start_ts), false);
    iter.rewind();
    let mut i = 0;
    while iter.valid() {
        assert_eq!(iter.key(), &gen_row_key(keyspace_id, table_id, i));
        assert_eq!(iter.val(), &gen_row_val(&eval_ctx, i));
        iter.next();
        i += 1;
    }
    assert_eq!(i, 200);

    // write some locks
    let mut opts = MutateOptions::default();
    opts.commit_action = CommitAction::NoCommit;
    client
        .try_put_kv(
            0..100,
            |i: usize| gen_row_key(keyspace_id, table_id, i),
            |i: usize| gen_row_val(&eval_ctx, i),
            opts,
        )
        .unwrap();
    // use old start_ts should not return error
    let snapshot_from_remote = dfs.get_runtime().block_on(request_dump_snapshot_on_store(
        &store,
        shard.id,
        shard.region_epoch.as_ref().unwrap().version,
        start_ts,
    ));
    let mut delegate_resp = DelegateResponse::default();
    delegate_resp
        .merge_from_bytes(&snapshot_from_remote)
        .unwrap();
    assert!(!delegate_resp.has_locked());
    // use new start_ts should return lock error
    let snapshot_from_remote = dfs.get_runtime().block_on(request_dump_snapshot_on_store(
        &store,
        shard.id,
        shard.region_epoch.unwrap().version,
        client.get_ts().into_inner(),
    ));
    let mut delegate_resp = DelegateResponse::default();
    delegate_resp
        .merge_from_bytes(&snapshot_from_remote)
        .unwrap();
    assert!(delegate_resp.has_locked());

    cluster.stop();
    oss.shutdown();
}

#[test]
fn test_region_merge_with_columnar() {
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
    let schemas = build_schemas(vec![table_id]);
    let schema = schemas[0].clone();
    let schema_version = 10;
    let schema_file_data = build_schema_file(keyspace_id, schema_version, schemas, 0);
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
    let pd_client = cluster.get_pd_client();
    let regions_count = pd_client.get_regions_number();
    let split_key = gen_row_key(keyspace_id, table_id, 100);
    client.split(&split_key);
    cluster.wait_pd_region_count(regions_count + 1);

    client.put_kv(
        0..100,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
    client.put_kv(
        100..200,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
    must_wait(
        || {
            let all_id_vers = kvengine.get_all_shard_id_vers();
            let mut columnar_count = 0;
            for id_ver in all_id_vers {
                if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    if shard.get_columnar_table_ids().contains(&table_id) {
                        columnar_count += 1;
                        if columnar_count == 2 {
                            return true;
                        }
                    }
                }
            }
            false
        },
        10,
        || {
            // dump shard info
            let all_id_vers = kvengine.get_all_shard_id_vers();
            for id_ver in all_id_vers {
                if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    info!(
                        "shard: {:?}, columnar_table_ids: {:?}",
                        id_ver,
                        shard.get_columnar_table_ids()
                    );
                }
            }
            "failed to build columnar file".to_string()
        },
    );
    client.try_merge(&gen_row_key(keyspace_id, table_id, 0), &split_key);
    cluster.wait_pd_region_count(regions_count);

    let shard_id = pd_client
        .get_region(&encode_bytes(&split_key))
        .unwrap()
        .get_id();
    let shard = kvengine.get_shard(shard_id).unwrap();
    let snap_access = shard.new_snap_access();
    let ts = client.get_ts().into_inner();
    let mut columnar_reader = snap_access
        .new_columnar_mvcc_reader(schema.table_id, &schema.columns, None, ts, None)
        .unwrap()
        .unwrap();
    block_on(columnar_reader.set_int_handle_range(0, Some(190))).unwrap();
    let mut block = columnar::Block::new(&schema);
    let read_rows = block_on(columnar_reader.read_block(&mut block, 200)).unwrap();
    assert_eq!(read_rows, 190);
    for i in 0..read_rows {
        let handle = block.get_handle_buf().get_int_handle_value(i);
        assert_eq!(handle, i as i64);
        let columns = block.get_columns();
        assert_eq!(columns[0].get_not_null_value(i).get_i64_le(), i as i64);
        let str_val = gen_str_val(i);
        assert_eq!(columns[1].get_not_null_value(i), &str_val);
    }
}

#[test]
fn test_region_merge_with_unconverted_l0() {
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
    let schemas = build_schemas(vec![table_id]);
    let schema = schemas[0].clone();
    let schema_version = 10;
    let schema_file_data = build_schema_file(keyspace_id, schema_version, schemas, 0);
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
    let pd_client = cluster.get_pd_client();
    let regions_count = pd_client.get_regions_number();
    let split_key = gen_row_key(keyspace_id, table_id, 500);
    client.split(&split_key);
    cluster.wait_pd_region_count(regions_count + 1);

    client.put_kv(
        400..500,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
    client.put_kv(
        500..600,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
    must_wait(
        || {
            let all_id_vers = kvengine.get_all_shard_id_vers();
            let mut columnar_count = 0;
            for id_ver in all_id_vers {
                if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    if shard.get_columnar_table_ids().contains(&table_id) {
                        columnar_count += 1;
                        if columnar_count == 2 {
                            return true;
                        }
                    }
                }
            }
            false
        },
        10,
        || "failed to build columnar file".to_string(),
    );
    // spawn a thread to put_kv in background
    let mut new_client = cluster.new_client();
    let _bg_writer = BackgroundWriter::new(move |stop| {
        let ctx = Mutex::new(EvalContext::default());
        while !stop.load(Ordering::Relaxed) {
            new_client.put_kv(
                0..10,
                |i: usize| gen_row_key(keyspace_id, table_id, i),
                |i: usize| gen_row_val(&ctx, i),
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let merged = client.try_merge_and_wait(&gen_row_key(keyspace_id, table_id, 0), &split_key, 10);
    assert!(merged, "failed to merge region");

    client.put_kv(
        300..400,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
    client.put_kv(
        600..700,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
    cluster.wait_pd_region_count(regions_count);

    let shard_id = pd_client
        .get_region(&encode_bytes(&split_key))
        .unwrap()
        .get_id();
    let shard = kvengine.get_shard(shard_id).unwrap();
    let snap_access = shard.new_snap_access();
    let ts = client.get_ts().into_inner();
    let mut columnar_reader = snap_access
        .new_columnar_mvcc_reader(schema.table_id, &schema.columns, None, ts, None)
        .unwrap()
        .unwrap();
    block_on(columnar_reader.set_int_handle_range(400, Some(600))).unwrap();
    let mut block = columnar::Block::new(&schema);
    info!("read block from columnar");
    let read_rows = block_on(columnar_reader.read_block(&mut block, 200)).unwrap();
    info!("read block from columnar done, read_rows: {}", read_rows);
    assert_eq!(read_rows, 200);
    for i in 0..200 {
        let handle = block.get_handle_buf().get_int_handle_value(i);
        assert_eq!(handle, (i + 400) as i64);
        let columns = block.get_columns();
        assert_eq!(
            columns[0].get_not_null_value(i).get_i64_le(),
            (i + 400) as i64
        );
        let str_val = gen_str_val(i + 400);
        assert_eq!(columns[1].get_not_null_value(i), &str_val);
    }
}

#[test]
fn test_region_split_by_max_columnar_tables() {
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
        conf.kvengine.max_del_range_delay = ReadableDuration(Duration::from_secs(1));
        conf.coprocessor.region_max_keys = Some(10000);
        conf.coprocessor.region_split_keys = Some(10000);
    });
    let dfs = cluster.get_dfs().unwrap();
    let (keyspace_id, table_ids) = dfs
        .get_runtime()
        .block_on(create_keyspace_and_split_tables(&mut cluster));
    let schemas = build_schemas(vec![table_ids[0]]);
    let schema = schemas[0].clone();
    let schema_version = 10;
    let schema_file_data = build_schema_file(keyspace_id, schema_version, schemas, 0);
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
    let table_id = schema.table_id;
    let kvengine = cluster.get_kvengine(node_id);
    let mut client = cluster.new_client();
    let ctx = Mutex::new(EvalContext::default());
    client.put_kv(
        0..5000,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
    let _shard_id = wait_columnar_built(&kvengine, table_id);
    let shards_count = kvengine.get_all_shard_stats().len();

    fail::cfg("max_columnar_tables_threshold", "return(0)").unwrap();
    // Enable estimated entries by table count which is larger than region_max_keys.
    cluster.wait_pd_region_min_count(shards_count + 1);
}

#[test]
fn test_columnar_ia_file() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    // prepare dfs
    let (temp_dir, mut oss, dfs_config) = prepare_dfs("test_columnar_ia_file");
    let mut cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.kvengine
            .columnar_table_build_options
            .max_columnar_table_size = 1024;
        conf.kvengine
            .columnar_table_build_options
            .pack_max_row_count = 9;
        conf.kvengine.build_columnar = true;
        conf.dfs = dfs_config.clone();
    });
    let dfs = cluster.get_dfs().unwrap();
    let runtime = dfs.get_runtime();
    let (keyspace_id, table_ids) = dfs
        .get_runtime()
        .block_on(create_keyspace_and_split_tables(&mut cluster));
    let table_id = table_ids[1];
    let schemas = build_schemas(vec![table_id]);
    let schema = schemas[0].clone();
    let schema_version = 10;
    let schema_file_data = build_schema_file(keyspace_id, schema_version, schemas, 0);
    let schema_file_id = 100;
    let opts = dfs::Options::default().with_type(FileType::Schema);
    runtime
        .block_on(dfs.create(schema_file_id, schema_file_data.into(), opts))
        .unwrap();
    let status_addr = cluster.status_addr(node_id);
    let kvengine = cluster.get_kvengine(node_id);
    must_wait(
        || {
            runtime.block_on(send_schema_file_request(
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
    let master_key = kvengine.get_master_key();
    let mut client = cluster.new_client();
    let eval_ctx = Mutex::new(EvalContext::default());
    let rows_count = 2000 + rand::thread_rng().gen::<usize>() % 3000;
    client.put_kv(
        0..rows_count,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i: usize| gen_row_val(&eval_ctx, i),
    );
    must_wait(
        || {
            let all_id_vers = kvengine.get_all_shard_id_vers();
            for id_ver in all_id_vers {
                if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    if shard.get_columnar_table_ids().contains(&table_id) {
                        return true;
                    }
                }
            }
            false
        },
        10,
        || "failed to build columnar file".to_string(),
    );

    let pd_client = cluster.get_pd_client();
    let shard = pd_client
        .get_region(&encode_bytes(&gen_row_key(keyspace_id, table_id, 0)))
        .unwrap();

    let store_id = kvengine.get_engine_id();
    let store = pd_client.get_store(store_id).unwrap();
    let start_ts = client.get_ts().into_inner();

    let snapshot_from_remote = dfs.get_runtime().block_on(request_dump_snapshot_on_store(
        &store,
        shard.id,
        shard.region_epoch.as_ref().unwrap().version,
        start_ts,
    ));
    let mut delegate_resp = DelegateResponse::default();
    delegate_resp
        .merge_from_bytes(&snapshot_from_remote)
        .unwrap();
    let schema_files = Arc::new(DashMap::new());
    let local_path = temp_dir.path().join("ia");
    let ia_cap = IaCapacity::MemoryAndDiskCap(0.into(), vec![local_path.clone()], 1000.into());
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
        runtime.handle().clone().into(),
    )
    .unwrap();
    let ia_ctx = IaCtx::Enabled(ia_mgr, Arc::new(vec![local_path]));
    let snap_ctx = SnapCtx {
        dfs: dfs.clone(),
        master_key,
        block_cache: BlockCache::None,
        vector_index_cache: None,
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
    let snap_access = runtime
        .block_on(SnapAccess::construct_snapshot(
            "test",
            &snap_ctx,
            delegate_resp.get_mem_table_data(),
            delegate_resp.get_snapshot(),
            mem_limiter,
        ))
        .unwrap()
        .0;
    assert!(snap_access.has_schema_file());
    assert!(schema_files.contains_key(&schema_file_id));
    let ts = client.get_ts().into_inner();
    let mut columnar_reader = snap_access
        .new_columnar_mvcc_reader(schema.table_id, &schema.columns, None, ts, None)
        .unwrap()
        .unwrap();
    block_on(columnar_reader.set_unbounded_handle_range()).unwrap();
    let mut block = columnar::Block::new(&schema);
    let read_rows = block_on(columnar_reader.read_block(&mut block, 5000)).unwrap();
    assert_eq!(read_rows, rows_count);
    for i in 0..read_rows {
        let handle = block.get_handle_buf().get_int_handle_value(i);
        assert_eq!(handle, i as i64);
        let columns = block.get_columns();
        assert_eq!(columns[0].get_not_null_value(i).get_i64_le(), i as i64);
        let str_val = gen_str_val(i);
        assert_eq!(columns[1].get_not_null_value(i), &str_val);
    }

    cluster.stop();
    oss.shutdown();
}

fn prepare_columnar_cluster(node_id: u16) -> (ServerCluster, Schema, u32) {
    let mut cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.kvengine
            .columnar_table_build_options
            .max_columnar_table_size = 1024;
        conf.kvengine
            .columnar_table_build_options
            .pack_max_row_count = 9;
        conf.kvengine.build_columnar = true;
        conf.kvengine.read_columnar = true;
        conf.kvengine.max_del_range_delay = ReadableDuration(Duration::from_secs(1));
    });
    let dfs = cluster.get_dfs().unwrap();
    let (keyspace_id, table_ids) = dfs
        .get_runtime()
        .block_on(create_keyspace_and_split_tables(&mut cluster));
    let table_id = table_ids[1];
    let schemas = build_schemas(vec![table_id]);
    let schema = schemas[0].clone();
    let schema_version = 10;
    let schema_file_data = build_schema_file(keyspace_id, schema_version, schemas, 0);
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
    (cluster, schema, keyspace_id)
}

fn wait_columnar_built(kvengine: &Engine, table_id: i64) -> u64 {
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
    shard_id.unwrap()
}

#[test]
fn test_columnar_destroy_range() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let (cluster, schema, keyspace_id) = prepare_columnar_cluster(node_id);
    let table_id = schema.table_id;
    let kvengine = cluster.get_kvengine(node_id);
    let mut client = cluster.new_client();
    let ctx = Mutex::new(EvalContext::default());
    client.put_kv(
        0..500,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
    let shard_id = wait_columnar_built(&kvengine, table_id);
    let compact_fp = "kvengine_l0_to_columnar";
    fail::cfg(compact_fp, "return").unwrap();
    client.put_kv(
        500..1000,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
    must_wait(
        || {
            let shard = kvengine.get_shard(shard_id).unwrap();
            let stats = shard.get_stats();
            stats.unconverted_l0_count > 0
        },
        10,
        || "failed to build columnar file".to_string(),
    );
    let store_id = cluster.get_stores()[0];
    let range_prefix = ApiV2::get_keyspace_prefix_by_id(keyspace_id);
    destroy_range(&mut client, store_id, &range_prefix);
    must_wait(
        || {
            let shard = kvengine.get_shard(shard_id).unwrap();
            let stats = shard.get_stats();
            stats.unconverted_l0_count == 0
        },
        10,
        || "failed to clear unconverted l0".to_string(),
    );
}

#[test]
fn test_columnar_scan_with_filter() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let (cluster, schema, keyspace_id) = prepare_columnar_cluster(node_id);
    let table_id = schema.table_id;
    let kvengine = cluster.get_kvengine(node_id);
    let mut client = cluster.new_client();
    let ctx = Mutex::new(EvalContext::default());
    client.put_kv(
        0..1000,
        |i: usize| gen_row_key(keyspace_id, table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
    let shard_id = wait_columnar_built(&kvengine, table_id);
    let shard = kvengine.get_shard(shard_id).unwrap();
    let snap_access = shard.new_snap_access();
    let ts = client.get_ts().into_inner();
    let expr = build_where_expr(&schema.columns, 1, 100);
    let mut table_scan = tipb::Executor::default();
    table_scan
        .mut_tbl_scan()
        .set_columns(schema.columns.clone().into());
    let scan_ctx = TableScanCtx::new(table_scan, vec![expr]);
    let mut columnar_reader = snap_access
        .new_columnar_mvcc_reader(schema.table_id, &schema.columns, Some(&scan_ctx), ts, None)
        .unwrap()
        .unwrap();
    block_on(columnar_reader.set_unbounded_handle_range()).unwrap();
    let mut block = columnar::Block::new(&schema);
    let read_rows = block_on(columnar_reader.read_block(&mut block, usize::MAX)).unwrap();
    (100..110).contains(&read_rows);
    for i in 0..read_rows {
        let handle = block.get_handle_buf().get_int_handle_value(i);
        assert_eq!(handle, i as i64);
        let columns = block.get_columns();
        assert_eq!(columns[0].get_not_null_value(i).get_i64_le(), i as i64);
        let str_val = gen_str_val(i);
        assert_eq!(columns[1].get_not_null_value(i), &str_val);
    }
}

fn build_where_expr(cols: &[ColumnInfo], col_id: i64, val: i64) -> tipb::Expr {
    let mut col = tipb::Expr::default();
    col.set_tp(tipb::ExprType::ColumnRef);
    let count_offset = test_coprocessor::offset_for_column(cols, col_id);
    col.mut_val().write_i64(count_offset).unwrap();
    col.mut_field_type()
        .as_mut_accessor()
        .set_tp(FieldTypeTp::LongLong);

    let mut value = tipb::Expr::default();
    value.set_tp(tipb::ExprType::Int64);
    let mut buf = Vec::with_capacity(8);
    buf.write_i64(val).unwrap();
    value.set_val(buf);
    value
        .mut_field_type()
        .as_mut_accessor()
        .set_tp(FieldTypeTp::LongLong);

    let mut cond = tipb::Expr::default();
    cond.set_tp(tipb::ExprType::ScalarFunc);
    cond.set_sig(tipb::ScalarFuncSig::LtInt);
    cond.mut_field_type()
        .as_mut_accessor()
        .set_tp(FieldTypeTp::LongLong);
    cond.mut_children().push(col);
    cond.mut_children().push(value);
    cond
}

pub(crate) fn gen_row_key(keyspace_id: u32, table_id: i64, i: usize) -> Vec<u8> {
    let mut key = ApiV2::get_txn_keyspace_prefix(keyspace_id);
    let table_key = encode_row_key(table_id, i as i64);
    key.extend_from_slice(&table_key);
    key
}

pub(crate) fn gen_row_val(ctx: &Mutex<EvalContext>, i: usize) -> Vec<u8> {
    let mut row_val = vec![];
    let str_val = gen_str_val(i);
    let cols = vec![
        Column::new(1, Some(i as i64)),
        Column::new(2, Some(str_val)),
    ];
    let mut guard = ctx.lock().unwrap();
    row_val.write_row(&mut guard, cols).unwrap();
    row_val
}

pub(crate) fn gen_str_val(i: usize) -> Vec<u8> {
    let repeat = 1 + i % 16;
    format!("abc_{}", i).repeat(repeat).into_bytes()
}

fn build_schemas(table_ids: Vec<i64>) -> Vec<Schema> {
    let mut schemas = vec![];
    for &columnar_table_id in &table_ids {
        let schema = build_columnar_schema_buf(columnar_table_id).into();
        schemas.push(schema);
    }
    schemas
}

fn build_columnar_schema_buf(table_id: i64) -> SchemaBuf {
    let mut c1 = ColumnInfo::new();
    c1.set_column_id(1);
    c1.set_tp(FieldTypeTp::LongLong.to_u8().unwrap() as i32);
    let mut c2 = ColumnInfo::new();
    c2.set_column_id(2);
    c2.set_tp(FieldTypeTp::VarChar.to_u8().unwrap() as i32);
    c2.set_column_len(255);
    c2.set_collation(Collation::Utf8Mb4Bin as i32);
    SchemaBuf::new(
        table_id,
        new_int_handle_column_info(),
        new_version_column_info(),
        vec![c1, c2],
        vec![],
        0,
        vec![],
        vec![],
        StorageClassSpec::default(),
        None,
    )
}

async fn send_schema_file_request(status_addr: &str, keyspace_id: u32, schema_file_id: u64) {
    let request = hyper::http::Request::builder()
        .method(http::method::Method::POST)
        .uri(format!(
            "http://{}/schema_file?keyspace_id={}&file_id={}",
            status_addr, keyspace_id, schema_file_id
        ))
        .body(Body::empty())
        .unwrap();
    let http_client = hyper::client::Client::new();
    let resp = http_client.request(request).await.unwrap();
    assert!(resp.status().is_success());
}

async fn send_collect_columnar_status_request(
    status_addr: &str,
    keyspace_id: u32,
    table_id: i64,
) -> ColumnarStatusResp {
    let request = hyper::http::Request::builder()
        .method(http::method::Method::GET)
        .uri(format!(
            "http://{}/kvengine/columnar_status?keyspace_id={}&table_id={}",
            status_addr, keyspace_id, table_id
        ))
        .body(Body::empty())
        .unwrap();
    let http_client = hyper::client::Client::new();
    let resp = http_client.request(request).await.unwrap();
    assert!(resp.status().is_success());
    let mut body = vec![];
    resp.into_body()
        .try_for_each(|bytes| {
            body.extend(bytes);
            ok(())
        })
        .await
        .unwrap();
    let columnar_status: ColumnarStatusResp = serde_json::from_slice(&body).unwrap();
    columnar_status
}

async fn send_collect_columnar_index_stats_request(
    status_addr: &str,
    keyspace_id: u32,
) -> Vec<ColumnarIndexStats> {
    let request = hyper::http::Request::builder()
        .method(http::method::Method::GET)
        .uri(format!(
            "http://{}/kvengine/columnar_index_stats?keyspace_id={}",
            status_addr, keyspace_id,
        ))
        .body(Body::empty())
        .unwrap();
    let http_client = hyper::client::Client::new();
    let resp = http_client.request(request).await.unwrap();
    assert!(resp.status().is_success());
    let mut body = vec![];
    resp.into_body()
        .try_for_each(|bytes| {
            body.extend(bytes);
            ok(())
        })
        .await
        .unwrap();
    let columnar_index_stats: Vec<ColumnarIndexStats> = serde_json::from_slice(&body).unwrap();
    columnar_index_stats
}

async fn create_keyspace_and_split_tables(
    cluster: &mut ServerCluster,
) -> (u32 /* keyspace_id */, Vec<i64> /* table_ids */) {
    let keyspace_id = cluster
        .create_keyspace(
            &CreateKeyspaceOptions {
                table_count: 4,
                ..Default::default()
            },
            Duration::from_secs(10),
        )
        .await;
    let table_ids = cluster
        .keyspace_manager()
        .get_keyspace_meta(keyspace_id)
        .unwrap()
        .get_all_available_tables();
    (keyspace_id, table_ids)
}
