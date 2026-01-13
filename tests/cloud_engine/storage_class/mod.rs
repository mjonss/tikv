// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Mutex, time::Duration};

use bytes::Bytes;
use kvengine::{
    self, STORAGE_CLASS_KEY,
    metrics::ENGINE_STORAGE_CLASS_TRANSITION_COUNTER,
    table::{BoundedDataSet, DataBound, sstable::BlockCacheType},
    table_id::encode_table_prefix_key,
};
use kvproto::kvrpcpb;
use pd_client::PdClient;
use rstest::rstest;
use schema::{
    generate_storage_class_schema_data_for_test,
    schema::{StorageClass, StorageClassSpec, StorageClassTransitRule},
};
use test_cloud_server::{
    ServerCluster, ServerClusterBuilder,
    client::ClusterClient,
    keyspace::CreateKeyspaceOptions,
    must_wait,
    oss::prepare_dfs,
    util::{Mutation, get_keyspace_split_keys},
};
use test_pd_client::{PdClientExt, PdWrapper};
use tidb_query_datatype::{codec::table::decode_table_id, expr::EvalContext};
use tikv::config::TikvConfig;
use tikv_util::{
    codec::bytes::encode_bytes,
    config::{AbsoluteOrPercentSize, ReadableDuration, ReadableSize},
};

use crate::{
    alloc_node_id,
    columnar::{gen_row_key, gen_row_val},
    new_security_config,
};

const SEGMENT_SIZE: i64 = 64;
const FREQ_UPDATE_INTERVAL: Duration = Duration::from_secs(1);

#[rstest]
#[case(false)]
#[case::partitioned(true)]
fn test_storage_class_with_schema_manager(#[case] partitioned: bool) {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let (_temp_dir, mut oss, dfs_config) = prepare_dfs("test_storage_class_with_schema_manager");
    let security_config = new_security_config();
    let pd_wrapper = PdWrapper::new_test(1, &security_config, None);
    let mut cluster = ServerClusterBuilder::new(vec![node_id], |_, conf: &mut TikvConfig| {
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
        conf.kvengine.ia.auto_ia_check_interval = ReadableDuration::secs(1);
        conf.kvengine.block_cache_type = BlockCacheType::Quick;
        conf.security = security_config.clone();
    })
    .pd(pd_wrapper)
    .build();
    cluster.start_schema_manager(alloc_node_id());
    let dfs = cluster.get_dfs().unwrap();
    let keyspace_id = 9;
    let db_id = 5;
    let table_count = 4;
    let table_ids = dfs
        .get_runtime()
        .block_on(create_keyspace_region_without_splitting_table(
            &mut cluster,
            keyspace_id,
            table_count,
        ));
    let kvengine = cluster.get_kvengine(node_id);
    assert!(kvengine.is_ia_enabled());
    let mut client = cluster.new_client();
    let ctx = Mutex::new(EvalContext::default());

    let table_id = table_ids[1];
    let partition_id = partitioned.then_some(table_ids[2]);
    let backend_table_id = partition_id.unwrap_or(table_id);

    let mut tables = None;
    let mut partitions = partition_id.map(|id| vec![id]);

    let table_start_key = encode_table_prefix_key(backend_table_id);
    let table_end_key = encode_table_prefix_key(backend_table_id + 1);
    let table_bound = DataBound::new(table_start_key.as_ref(), table_end_key.as_ref(), false);

    let mut schema_version = 10;
    let mut update_schema = |client: &mut ClusterClient, sc_spec: StorageClassSpec| {
        let kv_pairs = generate_storage_class_schema_data_for_test(
            keyspace_id,
            db_id,
            table_id,
            partition_id,
            partitions.take(),
            schema_version,
            sc_spec,
            &mut tables,
        )
        .unwrap();
        put_kv_pairs(client, kv_pairs);
        schema_version += 1;
    };

    // Init schema data
    update_schema(&mut client, StorageClassSpec::default());

    // Create WriteCF ln files
    client.put_kv(
        0..100,
        |i: usize| gen_row_key(keyspace_id, backend_table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
    client.put_kv(
        100..200,
        |i: usize| gen_row_key(keyspace_id, backend_table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
    client.verify_data_with_ref_store();
    must_wait(
        || {
            let all_id_vers = kvengine.get_all_shard_id_vers();
            for id_ver in all_id_vers {
                if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    let stats = shard.get_stats();
                    if stats.cfs[0].levels[0].num_tables > 0 {
                        return true;
                    }
                }
            }
            false
        },
        10,
        || "failed to get ln files".to_string(),
    );

    // Change Local files to IA
    let sc_spec_ia = StorageClassSpec::from(StorageClass::Ia);
    update_schema(&mut client, sc_spec_ia.clone());
    let mut num_ia_files = 0usize;
    must_wait(
        || {
            let all_id_vers = kvengine.get_all_shard_id_vers();
            for id_ver in all_id_vers {
                if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    if shard.get_schema_file().is_some()
                        && shard.storage_class_spec_equals(&sc_spec_ia)
                    {
                        assert!(table_bound.contains_bound(shard.range.data_bound()));
                        let (_, sst_ia_file_ids) = shard.get_local_sst_files();
                        assert!(!sst_ia_file_ids.is_empty());
                        num_ia_files = sst_ia_file_ids.len();
                        return true;
                    }
                }
            }
            false
        },
        10,
        || "failed to reload ia file".to_string(),
    );
    client.verify_data_with_ref_store();

    // Create incremental IA files
    client.put_kv(
        200..300,
        |i: usize| gen_row_key(keyspace_id, backend_table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
    client.put_kv(
        300..400,
        |i: usize| gen_row_key(keyspace_id, backend_table_id, i),
        |i: usize| gen_row_val(&ctx, i),
    );
    must_wait(
        || {
            let all_id_vers = kvengine.get_all_shard_id_vers();
            for id_ver in all_id_vers {
                if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    if shard.get_schema_file().is_some()
                        && shard.storage_class_spec_equals(&sc_spec_ia)
                    {
                        assert!(table_bound.contains_bound(shard.range.data_bound()));
                        let (_, sst_ia_file_ids) = shard.get_local_sst_files();
                        assert!(!sst_ia_file_ids.is_empty());
                        if sst_ia_file_ids.len() > num_ia_files {
                            return true;
                        }
                    }
                }
            }
            false
        },
        10,
        || "failed to load new ia file".to_string(),
    );
    client.verify_data_with_ref_store();

    // Change IA files to Local files
    update_schema(&mut client, StorageClass::Standard.into());
    must_wait(
        || {
            let all_id_vers = kvengine.get_all_shard_id_vers();
            for id_ver in all_id_vers {
                if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    if shard.get_schema_file().is_some()
                        || shard.get_property(STORAGE_CLASS_KEY).is_some()
                    {
                        return false;
                    }
                    let (_, sst_ia_file_ids) = shard.get_local_sst_files();
                    assert!(sst_ia_file_ids.is_empty());
                }
            }
            true
        },
        10,
        || "failed to reload local file or remove storage class property".to_string(),
    );
    client.verify_data_with_ref_store();

    // Change to auto IA (unspecified -> IA)
    let transited_to_ia_origin = ENGINE_STORAGE_CLASS_TRANSITION_COUNTER
        .with_label_values(&["to_ia"])
        .get();
    {
        let sc_spec_auto = StorageClassSpec {
            default_tier: StorageClass::Unspecified,
            transit_rules: vec![StorageClassTransitRule {
                tier: StorageClass::Ia,
                transit_after: Duration::from_secs(3),
            }],
        };
        update_schema(&mut client, sc_spec_auto.clone());
        must_wait(
            || {
                let all_id_vers = kvengine.get_all_shard_id_vers();
                for id_ver in all_id_vers {
                    if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                        if shard.get_schema_file().is_some()
                            && shard.storage_class_spec_equals(&sc_spec_auto)
                        {
                            assert!(table_bound.contains_bound(shard.range.data_bound()));
                            return true;
                        }
                    }
                }
                false
            },
            10,
            || "failed to update schema to auto IA".to_string(),
        );
        client.verify_data_with_ref_store();
    }

    // Verify unspecified -> IA.
    {
        client.put_kv(
            400..500,
            |i: usize| gen_row_key(keyspace_id, backend_table_id, i),
            |i: usize| gen_row_val(&ctx, i),
        );
        must_wait(
            || {
                let transited_to_ia = ENGINE_STORAGE_CLASS_TRANSITION_COUNTER
                    .with_label_values(&["to_ia"])
                    .get();
                transited_to_ia > transited_to_ia_origin
            },
            10,
            || "wait for transited to IA timeout".to_string(),
        );
        client.verify_data_with_ref_store();
    }

    cluster.stop();
    oss.shutdown();
}

#[rstest]
#[case(false)]
#[case::partitioned(true)]
fn test_region_split_and_merge_with_storage_class(#[case] partitioned: bool) {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let (_temp_dir, mut oss, dfs_config) =
        prepare_dfs("test_region_split_and_merge_with_storage_class");
    let security_config = new_security_config();
    let pd_wrapper = PdWrapper::new_test(1, &security_config, None);
    let mut cluster = ServerClusterBuilder::new(vec![node_id], |_, conf: &mut TikvConfig| {
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
        conf.security = security_config.clone();
    })
    .pd(pd_wrapper)
    .build();
    cluster.start_schema_manager(alloc_node_id());
    let dfs = cluster.get_dfs().unwrap();
    let keyspace_id = 9;
    let db_id = 5;
    let table_count = 6;
    let mut table_ids = dfs
        .get_runtime()
        .block_on(create_keyspace_region_without_splitting_table(
            &mut cluster,
            keyspace_id,
            table_count,
        ));
    table_ids.sort();
    let kvengine = cluster.get_kvengine(node_id);
    assert!(kvengine.is_ia_enabled());
    let mut client = cluster.new_client();
    let pd_client = cluster.get_pd_client();
    let ctx = Mutex::new(EvalContext::default());

    let mut tables = None;
    let mut partitions = partitioned.then(|| table_ids.clone());
    let mut schema_version = 10;
    let mut update_schema =
        |client: &mut ClusterClient, sc_spec: StorageClassSpec, backend_table_id: i64| {
            let (table_id, partition_id) = if partitioned {
                (65535, Some(backend_table_id))
            } else {
                (backend_table_id, None)
            };
            let kv_pairs = generate_storage_class_schema_data_for_test(
                keyspace_id,
                db_id,
                table_id,
                partition_id,
                partitions.take(),
                schema_version,
                sc_spec,
                &mut tables,
            )
            .unwrap();
            put_kv_pairs(client, kv_pairs);
            schema_version += 1;
        };

    let check_ia_shard_range = |range: DataBound<'_>| {
        let shard_inner_start = range.lower_bound.to_vec();
        let shard_table_id = decode_table_id(&shard_inner_start).unwrap();
        let table_start_key = encode_table_prefix_key(shard_table_id);
        let table_end_key = encode_table_prefix_key(shard_table_id + 1);
        let table_bound = DataBound::new(table_start_key.as_ref(), table_end_key.as_ref(), false);
        assert!(table_bound.contains_bound(range));
    };

    // Init schema data
    for &table_id in &table_ids {
        update_schema(&mut client, StorageClassSpec::default(), table_id);
    }

    // Write data for all tables
    for &table_id in &table_ids {
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
        client.verify_data_with_ref_store();
    }

    // Convert local files tables 1, 3 and 4 to IA.
    // ( t0 Local / t1 IA / t2 Local / t3 IA / t4 IA / t5 Local )
    let ia_table_indexes = [1, 3, 4];
    let mut last_table_id = None;
    let mut table_start_keys = Vec::with_capacity(table_ids.len());
    for &table_id in &table_ids {
        if let Some(last_table_id) = last_table_id {
            assert_eq!(last_table_id + 1, table_id);
        }
        last_table_id = Some(table_id);
        table_start_keys.push(gen_row_key(keyspace_id, table_id, 0))
    }
    let mut ia_file_tables = Vec::with_capacity(ia_table_indexes.len());
    let mut ia_table_start_keys = Vec::with_capacity(ia_table_indexes.len());
    for &ia_table_idx in &ia_table_indexes {
        ia_file_tables.push(table_ids[ia_table_idx]);
        ia_table_start_keys.push(&table_start_keys[ia_table_idx]);
    }
    let sc_spec_ia = StorageClassSpec::from(StorageClass::Ia);
    for &table_id in &ia_file_tables {
        update_schema(&mut client, sc_spec_ia.clone(), table_id);
    }
    must_wait(
        || {
            let mut shard_with_ia_count = 0;
            for &id_ver in &kvengine.get_all_shard_id_vers() {
                let shard = kvengine.get_shard(id_ver.id).unwrap();
                if shard.get_schema_file().is_some() && shard.storage_class_spec_equals(&sc_spec_ia)
                {
                    check_ia_shard_range(shard.range.data_bound());
                    let (_, sst_ia_file_ids) = shard.get_local_sst_files();
                    if sst_ia_file_ids.is_empty() {
                        return false;
                    }
                    shard_with_ia_count += 1;
                }
            }
            shard_with_ia_count == ia_file_tables.len()
        },
        20,
        || "failed to wait storage class ia".to_string(),
    );
    client.verify_data_with_ref_store();

    // Test regions that cannot be merged
    // ( t0 Local / t1 IA / t2 Local / t3 IA / t4 IA / t5 Local )
    // None of the two adjacent tables can be merged.
    for idx in 0..table_ids.len() - 1 {
        client.try_merge(&table_start_keys[idx], &table_start_keys[idx + 1]);
        std::thread::sleep(Duration::from_secs(1));
    }
    for &table_start in &ia_table_start_keys {
        let shard_id = pd_client
            .get_region(&encode_bytes(table_start))
            .unwrap()
            .get_id();
        let shard = kvengine.get_shard(shard_id).unwrap();
        assert!(shard.get_schema_file().is_some() && shard.storage_class_spec_equals(&sc_spec_ia));
        check_ia_shard_range(shard.range.data_bound());
        let (_, sst_ia_file_ids) = shard.get_local_sst_files();
        assert!(!sst_ia_file_ids.is_empty());
    }
    client.verify_data_with_ref_store();

    // Split IA region
    let mut split_keys = Vec::with_capacity(ia_file_tables.len());
    for &table_id in &ia_file_tables {
        let regions_count = pd_client.get_regions_number();
        let split_key = gen_row_key(keyspace_id, table_id, 200);
        client.split(&split_key);
        split_keys.push(split_key);
        cluster.wait_pd_region_count(regions_count + 1);
        client.verify_data_with_ref_store();
    }

    // Create IA files for new IA regions
    for &table_id in &ia_file_tables {
        client.put_kv(
            200..300,
            |i: usize| gen_row_key(keyspace_id, table_id, i),
            |i: usize| gen_row_val(&ctx, i),
        );
        client.put_kv(
            300..400,
            |i: usize| gen_row_key(keyspace_id, table_id, i),
            |i: usize| gen_row_val(&ctx, i),
        );
    }
    must_wait(
        || {
            let all_id_vers = kvengine.get_all_shard_id_vers();
            let mut shard_with_ia_count = 0;
            for id_ver in all_id_vers {
                if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    if shard.get_schema_file().is_some()
                        && shard.storage_class_spec_equals(&sc_spec_ia)
                    {
                        check_ia_shard_range(shard.range.data_bound());
                        let (_, sst_ia_file_ids) = shard.get_local_sst_files();
                        if sst_ia_file_ids.is_empty() {
                            return false;
                        }
                        shard_with_ia_count += 1;
                    }
                }
            }
            shard_with_ia_count == ia_file_tables.len() * 2
        },
        10,
        || "failed to load new ia file for new ia region".to_string(),
    );
    client.verify_data_with_ref_store();

    // Merge two adjacent IA regions of the same table.
    for idx in 0..ia_file_tables.len() {
        let table_id = ia_file_tables[idx];
        let split_key = &split_keys[idx];
        let regions_count = pd_client.get_regions_number();
        client.try_merge(&gen_row_key(keyspace_id, table_id, 0), split_key);
        cluster.wait_pd_region_count(regions_count - 1);
    }
    must_wait(
        || {
            let all_id_vers = kvengine.get_all_shard_id_vers();
            let mut shard_with_ia_count = 0;
            for id_ver in all_id_vers {
                if let Ok(shard) = kvengine.get_shard_with_ver(id_ver.id, id_ver.ver) {
                    if shard.get_schema_file().is_some()
                        && shard.storage_class_spec_equals(&sc_spec_ia)
                    {
                        check_ia_shard_range(shard.range.data_bound());
                        let (_, sst_ia_file_ids) = shard.get_local_sst_files();
                        if sst_ia_file_ids.is_empty() {
                            return false;
                        }
                        shard_with_ia_count += 1;
                    }
                }
            }
            shard_with_ia_count == ia_file_tables.len()
        },
        10,
        || "failed to merge two adjacent IA regions of the same table".to_string(),
    );
    client.verify_data_with_ref_store();

    cluster.stop();
    oss.shutdown();
}

fn put_kv_pairs(client: &mut ClusterClient, kv_pairs: HashMap<Vec<u8>, Vec<u8>>) {
    let mut muts: Vec<Mutation> = Vec::with_capacity(kv_pairs.len());
    for (key, value) in kv_pairs {
        let mut m = Mutation::default();
        m.key = Bytes::from(key);
        m.value = Bytes::from(value);
        m.op = kvrpcpb::Op::Put;
        muts.push(m);
    }
    client.kv_mutate(muts).unwrap();
}

async fn create_keyspace_region_without_splitting_table(
    cluster: &mut ServerCluster,
    keyspace_id: u32,
    table_count: usize,
) -> Vec<i64> /* table_ids */
{
    cluster
        .keyspace_manager()
        .create_single_keyspace(
            keyspace_id,
            format!("ks{}", keyspace_id),
            &CreateKeyspaceOptions {
                table_count,
                ..Default::default()
            },
            false,
        )
        .await;
    let keyspace_split_keys = get_keyspace_split_keys(keyspace_id);
    cluster
        .get_pd_client()
        .split_regions_with_retry(keyspace_split_keys, Duration::from_secs(10))
        .await
        .unwrap();
    let table_ids = cluster
        .keyspace_manager()
        .get_keyspace_meta(keyspace_id)
        .unwrap()
        .get_all_available_tables();
    table_ids
}
