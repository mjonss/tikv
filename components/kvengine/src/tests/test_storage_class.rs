// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{rc::Rc, time::Duration};

use api_version::api_v2::KEYSPACE_PREFIX_LEN;
use schema::schema::{StorageClass, StorageClassSpec, StorageClassTransitRule};
use test_util::init_log_for_test;

use crate::{
    LOCK_CF, STORAGE_CLASS_KEY, Shard, ShardRange, StorageClassStats, WRITE_CF,
    ia::util::IaManagerOptionsBuilder,
    shard::{ShardCf, ShardCfBuilder, ShardDataBuilder},
    table::sstable::{BlockCache, L0Table},
    tests::{
        DEF_BLOCK_SIZE, new_l0table_file, new_table, new_test_engine_opt,
        test_ia_auto_file::convert_local_sst_to_auto, test_ia_file::convert_local_sst_to_ia,
    },
};

const KV_SIZE_PER_ENTRY: u64 = 27;

#[test]
fn test_stats_and_gc() {
    init_log_for_test();

    let (engine, _) = new_test_engine_opt(true, DEF_BLOCK_SIZE, "");
    let shard = engine.get_shard(1).unwrap();
    let mut saved_vals: Vec<Rc<Vec<u8>>> = Vec::new();
    let mgr = engine.new_ia_manager(IaManagerOptionsBuilder::default().build().unwrap());

    let f01 = new_l0table_file(
        &engine,
        1,
        [0, 0, 0],
        [200, 200, 0],
        200,
        [false, false, false],
    );
    let t00 = L0Table::new(f01, BlockCache::None, false, None)
        .unwrap()
        .unwrap();
    info!("l0 sst kv size: {}", t00.kv_size());

    let t11 = new_table(&engine, 11, 0, 100, 101, false, &mut saved_vals);
    assert_eq!(t11.kv_size, 100 * KV_SIZE_PER_ENTRY);
    let t12 = new_table(&engine, 12, 50, 150, 102, true, &mut saved_vals);
    let t13 = new_table(&engine, 13, 120, 200, 103, false, &mut saved_vals);
    info!(
        "ln sst size {} {} {}",
        t11.kv_size, t12.kv_size, t13.kv_size
    );

    let t_lock = new_table(&engine, 14, 0, 200, 104, false, &mut saved_vals);

    let total_kv_size = t00.kv_size() + t11.kv_size + t12.kv_size + t13.kv_size;
    let all_file_ids = vec![1, 11, 12, 13, 14];

    // Normal shard.
    {
        let mut cf_builder = ShardCfBuilder::new(WRITE_CF);
        cf_builder.add_table(t11.clone(), 3);
        cf_builder.add_table(t12.clone(), 2);
        cf_builder.add_table(t13.clone(), 1);
        let mut lock_cf_builder = ShardCfBuilder::new(LOCK_CF);
        lock_cf_builder.add_table(t_lock.clone(), 2);

        let mut builder = ShardDataBuilder::new(shard.get_data());
        builder.set_l0_tbls(vec![t00.clone()]);
        builder.set_cfs([cf_builder.build(), lock_cf_builder.build(), ShardCf::new(2)]);
        shard.set_data(builder.build());

        let stats = shard.get_stats();
        info!("normal shard stats: {:?}", stats);
        assert_eq!(stats.kv_size, total_kv_size);
        assert_eq!(stats.cfs[LOCK_CF].data_size(), t_lock.size());
        assert_eq!(stats.ia, StorageClassStats::default());

        shard.refresh_states();
        assert_eq!(shard.get_estimated_kv_size(), total_kv_size);
        assert_eq!(shard.get_estimated_ia_kv_size(), 0);

        assert_eq!(shard.get_local_sst_files(), (all_file_ids.clone(), vec![]));
        assert_eq!(shard.get_all_sst_files(), all_file_ids);
    }

    // IA shard.
    {
        let sc_spec: StorageClassSpec = StorageClass::Ia.into();
        shard.set_property(STORAGE_CLASS_KEY, &sc_spec.marshal());

        let ia11 = convert_local_sst_to_ia(&t11, mgr.clone());
        let ia12 = convert_local_sst_to_ia(&t12, mgr.clone());
        let ia13 = convert_local_sst_to_ia(&t13, mgr.clone());
        let mut cf_builder = ShardCfBuilder::new(WRITE_CF);
        cf_builder.add_table(ia11.clone(), 3);
        cf_builder.add_table(ia12.clone(), 2);
        cf_builder.add_table(ia13.clone(), 1);
        let mut lock_cf_builder = ShardCfBuilder::new(LOCK_CF);
        lock_cf_builder.add_table(t_lock.clone(), 2);
        let mut builder = ShardDataBuilder::new(shard.get_data());
        builder.set_cfs([cf_builder.build(), lock_cf_builder.build(), ShardCf::new(2)]);
        shard.set_data(builder.build());

        let ia_kv_size = t11.kv_size + t12.kv_size + t13.kv_size;

        let stats = shard.get_stats();
        info!("IA shard stats: {:?}", stats);
        assert_eq!(stats.kv_size, total_kv_size);
        assert_eq!(stats.cfs[LOCK_CF].data_size(), t_lock.size());
        assert_eq!(
            stats.ia,
            StorageClassStats {
                num_shards: 1,
                num_tables: 3,
                data_size: t11.size() + t12.size() + t13.size(),
                kv_size: ia_kv_size,
            }
        );

        shard.refresh_states();
        assert_eq!(shard.get_estimated_kv_size(), total_kv_size);
        assert_eq!(shard.get_estimated_ia_kv_size(), ia_kv_size);

        assert_eq!(shard.get_local_sst_files(), (vec![1, 14], vec![11, 12, 13]));
        assert_eq!(shard.get_all_sst_files(), all_file_ids);
    }

    let spec_auto: StorageClassSpec = StorageClassSpec {
        default_tier: StorageClass::Unspecified,
        transit_rules: vec![StorageClassTransitRule {
            tier: StorageClass::Ia,
            transit_after: Duration::from_secs(10),
        }],
    };
    let auto11 = convert_local_sst_to_auto(&t11, spec_auto.clone(), None, mgr.clone());
    let auto12 = convert_local_sst_to_auto(&t12, spec_auto.clone(), None, mgr.clone());
    let auto13 = convert_local_sst_to_auto(&t13, spec_auto.clone(), None, mgr.clone());

    // Auto IA shard.
    {
        shard.set_property(STORAGE_CLASS_KEY, &spec_auto.marshal());

        let mut cf_builder = ShardCfBuilder::new(WRITE_CF);
        cf_builder.add_table(auto11.clone(), 3);
        cf_builder.add_table(auto12.clone(), 2);
        cf_builder.add_table(auto13.clone(), 1);
        let mut lock_cf_builder = ShardCfBuilder::new(LOCK_CF);
        lock_cf_builder.add_table(t_lock.clone(), 2);

        let mut builder = ShardDataBuilder::new(shard.get_data());
        builder.set_cfs([cf_builder.build(), lock_cf_builder.build(), ShardCf::new(2)]);
        shard.set_data(builder.build());

        let stats = shard.get_stats();
        info!("Auto IA shard stats (before transit): {:?}", stats);
        assert_eq!(stats.kv_size, total_kv_size);
        assert_eq!(stats.cfs[LOCK_CF].data_size(), t_lock.size());
        assert_eq!(
            stats.ia,
            StorageClassStats {
                num_shards: 1,
                ..Default::default()
            }
        );

        shard.refresh_states();
        assert_eq!(shard.get_estimated_kv_size(), total_kv_size);
        assert_eq!(shard.get_estimated_ia_kv_size(), 0);

        assert_eq!(
            shard.get_local_sst_files(),
            (all_file_ids.clone(), vec![11, 12, 13])
        );
        assert_eq!(shard.get_all_sst_files(), all_file_ids);

        // Transit to IA.
        auto11.try_get_auto_ia_file().unwrap().transit_to_ia();
        auto13.try_get_auto_ia_file().unwrap().transit_to_ia();

        let ia_kv_size = auto11.kv_size + auto13.kv_size;

        let stats = shard.get_stats();
        info!("Auto IA shard stats (after transit): {:?}", stats);
        assert_eq!(stats.kv_size, total_kv_size);
        assert_eq!(stats.cfs[LOCK_CF].data_size(), t_lock.size());
        assert_eq!(
            stats.ia,
            StorageClassStats {
                num_shards: 1,
                num_tables: 2,
                data_size: auto11.size() + auto13.size(),
                kv_size: ia_kv_size,
            }
        );

        shard.refresh_states();
        assert_eq!(shard.get_estimated_kv_size(), total_kv_size);
        assert_eq!(shard.get_estimated_ia_kv_size(), ia_kv_size);

        assert_eq!(
            shard.get_local_sst_files(),
            (vec![1, 12, 14], vec![11, 12, 13])
        );
        assert_eq!(shard.get_all_sst_files(), all_file_ids);
    }

    // Partial tables.
    {
        // 140: make t00, t12, t13 partial.
        let new_outer_end = engine.key_builder.i_to_outer_key(140);
        let new_range = ShardRange::new(&shard.outer_start, &new_outer_end);
        let new_shard = Shard::new(
            engine.get_engine_id(),
            &shard.properties.to_pb(1),
            shard.ver + 1,
            new_range,
            KEYSPACE_PREFIX_LEN,
            engine.opts.clone(),
            &engine.master_key,
        );

        let mut cf_builder = ShardCfBuilder::new(WRITE_CF);
        cf_builder.add_table(auto11.clone(), 3);
        cf_builder.add_table(auto12.clone(), 2);
        cf_builder.add_table(auto13.clone(), 1);
        let mut lock_cf_builder = ShardCfBuilder::new(LOCK_CF);
        lock_cf_builder.add_table(t_lock.clone(), 2);

        let mut builder = ShardDataBuilder::new(new_shard.get_data());
        builder.set_l0_tbls(vec![t00.clone()]);
        builder.set_cfs([cf_builder.build(), lock_cf_builder.build(), ShardCf::new(2)]);
        new_shard.set_data(builder.build());

        let total_kv_size = t00.kv_size() / 2 + t11.kv_size + t12.kv_size / 2 + t13.kv_size / 2;
        let ia_kv_size = auto11.kv_size + auto13.kv_size / 2;

        let stats = new_shard.get_stats();
        info!("Auto IA shard stats (partial): {:?}", stats);
        assert_eq!(stats.kv_size, total_kv_size);
        assert_eq!(stats.cfs[LOCK_CF].data_size(), t_lock.size() / 2);
        assert_eq!(
            stats.ia,
            StorageClassStats {
                num_shards: 1,
                num_tables: 2,
                data_size: auto11.size() + auto13.size() / 2,
                kv_size: ia_kv_size,
            }
        );

        new_shard.refresh_states();
        assert_eq!(new_shard.get_estimated_kv_size(), total_kv_size);
        assert_eq!(new_shard.get_estimated_ia_kv_size(), ia_kv_size);

        assert_eq!(
            new_shard.get_local_sst_files(),
            (vec![1, 12, 14], vec![11, 12, 13])
        );
        assert_eq!(new_shard.get_all_sst_files(), all_file_ids);
    }
}
