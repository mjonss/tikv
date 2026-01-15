// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{assert_matches::assert_matches, fs, path::PathBuf, sync::Arc, time::Duration};

use bytes::{Buf, Bytes};
use kvengine::{
    FileMeta, dfs,
    dfs::{FileType, new_dfs_from_config},
    ia::{
        gc::{IaGcConfig, IaGcRunner},
        ia_file::{IaFile, table_meta_file_local_path},
        manager::IaManager,
        types::{FileSegmentData, FileSegmentIdent},
        util::{
            IaCapacity, IaManagerOptionsBuilder, LocalFileStore, LocalStore,
            test_util::verify_local_segments,
        },
    },
    table::{
        ChecksumType, InnerKey, NO_COMPRESSION, Value,
        blobtable::builder::BlobTableBuilder,
        file::InMemFile,
        get_local_dir,
        sstable::{self},
    },
};
use proptest::prelude::*;
use rand::prelude::*;
use rstest::rstest;
use test_cloud_server::oss::prepare_dfs;
use test_util::init_log_for_test;
use tikv_util::{config::ReadableDuration, debug, info};

const BLOCK_SIZE: usize = 32;
const SEGMENT_SIZE: i64 = 64;
const FREQ_UPDATE_INTERVAL: Duration = Duration::from_secs(1);

prop_compose! {
    fn arb_range_args(min: u64, max: u64)
        (start in min..max)
        (
            start in Just(start),
            end in start+1..=max,
        )
        -> (u64, u64)
    {
        (start, end)
    }
}

fn make_file_meta(file_type: FileType) -> FileMeta {
    FileMeta {
        cf: 0,
        level: 0,
        file_type,
        smallest: Bytes::new(),
        biggest: Bytes::new(),
        l0_size: 0,
        table_meta_off: 0,
        snap_version: None,
    }
}

#[rstest]
#[case(IaCapacity::MemoryAndDiskCap(300.into(), vec![PathBuf::from("ia")], 3000.into()))]
#[case::memory(IaCapacity::MemoryCap(3000.into()))]
#[case::big_cap(IaCapacity::MemoryAndDiskCap(1000.into(), vec![PathBuf::from("ia")], 10000.into()))]
#[case::small_cap(IaCapacity::MemoryAndDiskCap(256.into(), vec![PathBuf::from("ia")], 1024.into()))]
fn test_read(#[case] mut ia_cap: IaCapacity) {
    init_log_for_test();

    let (temp_dir, mut oss, dfs_conf) = prepare_dfs("test");
    let temp_dir = temp_dir.path();

    let s3fs = new_dfs_from_config(dfs_conf);
    let _s3fs = s3fs.clone();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    ia_cap.set_parent_dir(temp_dir.to_path_buf());
    let options = IaManagerOptionsBuilder::default()
        .capacity(ia_cap)
        .segment_size(SEGMENT_SIZE)
        .freq_update_interval(Duration::ZERO)
        .build()
        .unwrap();
    let (small_cap, main_cap) = (options.small_queue.cap, options.main_queue.cap);

    let rt = runtime.handle().clone();
    let (mgr, user_data, ia_file) = runtime.block_on(async move {
        let file_id = 42;
        let file_type = FileType::Sst;
        // user_data: About 6.8 KiB.
        let (file_data, user_data, table_meta_off) =
            make_sstable(file_id, BLOCK_SIZE, 200, 7, 5, thread_rng().gen_ratio(1, 2));
        info!("make sstable"; "file size" => file_data.len(), "user data size" => user_data.len());

        s3fs.put_object(
            s3fs.file_key(file_id, file_type),
            file_data,
            format!("{}.{}", file_id, file_type.suffix()),
        )
        .await
        .unwrap();

        let mgr = IaManager::new(options, s3fs.clone(), None, rt.into()).unwrap();
        let dfs_opts = dfs::Options::default().with_shard(1, 1);
        let table_meta_data = IaFile::prepare_table_meta(
            file_id,
            file_type,
            table_meta_off,
            temp_dir,
            &dfs_opts,
            &mgr,
            None,
        )
        .await
        .unwrap();
        let table_meta_file = InMemFile::new(file_id, table_meta_data);
        let fm = make_file_meta(file_type);
        let ia_file = IaFile::open(file_id, &fm, Arc::new(table_meta_file), mgr.clone()).unwrap();

        let seg = ia_file.multi_read_async(0, user_data.len()).await.unwrap();
        assert_eq!(seg, user_data);

        let mut buf = vec![0; user_data.len()];
        ia_file.multi_read_at_async(&mut buf, 0).await.unwrap();
        assert_eq!(buf, user_data.chunk());

        (mgr, user_data, ia_file)
    });

    proptest!(|(
        (start_off, end_off) in arb_range_args(0, user_data.len() as u64)
    )| {
        debug!("test_read: start_off: {}, end_off: {}", start_off, end_off);
        let expected = user_data.slice(start_off as usize..end_off as usize);

        let seg = runtime.block_on(ia_file.multi_read_async(start_off, (end_off-start_off) as usize)).unwrap();
        prop_assert_eq!(&seg, &expected);

        let mut buf = vec![0; (end_off-start_off) as usize];
        runtime.block_on(ia_file.multi_read_at_async(&mut buf, start_off)).unwrap();
        prop_assert_eq!(buf, expected.chunk());
    });

    runtime.block_on(async {
        mgr.flush_tasks(Duration::from_secs(60)).await.unwrap();

        let segments = mgr.get_local_segments().await;
        verify_local_segments(&segments, small_cap, main_cap, Some(user_data.len() as u64));
    });

    info!("cache hit rate: {}", mgr.cache_hit_rate());
    oss.shutdown();
}

#[test]
fn test_init() {
    init_log_for_test();

    let (temp_dir, mut oss, dfs_conf) = prepare_dfs("test");
    let temp_dir = temp_dir.path();

    let s3fs = new_dfs_from_config(dfs_conf);
    let _s3fs = s3fs.clone();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let rt = runtime.handle().clone();
    runtime.block_on(async move {
        let local_path = temp_dir.join("ia");
        let ia_cap = IaCapacity::MemoryAndDiskCap(0.into(), vec![local_path.clone()], 1000.into());
        let options = IaManagerOptionsBuilder::default()
            .capacity(ia_cap)
            .segment_size(SEGMENT_SIZE)
            .freq_update_interval(FREQ_UPDATE_INTERVAL)
            .build()
            .unwrap();

        let file_type = FileType::Sst;
        let fm = make_file_meta(file_type);

        {
            let mgr =
                IaManager::new(options.clone(), s3fs.clone(), None, rt.clone().into()).unwrap();

            let mut files = Vec::with_capacity(10);
            for i in 1..10 {
                let file_id = i as u64;
                let (file_data, _, table_meta_off) =
                    make_sstable(file_id, BLOCK_SIZE, 10, 7, 5, false);

                s3fs.put_object(
                    s3fs.file_key(file_id, file_type),
                    file_data,
                    format!("{}.{}", file_id, file_type.suffix()),
                )
                .await
                .unwrap();

                files.push((file_id, file_type, table_meta_off));
            }

            let dfs_opts = dfs::Options::default().with_shard(1, 1);
            for (file_id, file_type, table_meta_off) in files {
                IaFile::prepare_table_meta(
                    file_id,
                    file_type,
                    table_meta_off,
                    &local_path,
                    &dfs_opts,
                    &mgr,
                    None,
                )
                .await
                .unwrap();
            }

            let ia1 = IaFile::open_in_path(1, &fm, &local_path, mgr.clone()).unwrap();
            let _ = ia1.multi_read_async(5, 10).await.unwrap();
            let _ = ia1.multi_read_async(5, 10).await.unwrap();

            let ia2 = IaFile::open_in_path(2, &fm, &local_path, mgr.clone()).unwrap();
            let _ = ia2.multi_read_async(100, 64).await.unwrap();
            let _ = ia2.multi_read_async(100, 64).await.unwrap();

            // To make sure that segments are written to local store.
            mgr.flush_tasks(Duration::from_secs(5)).await.unwrap();
        }

        {
            let mgr = IaManager::new(options, s3fs.clone(), None, rt.into()).unwrap();

            let mut segments_ident = mgr.get_local_segments().await;
            segments_ident.sort_by(|(m_ident, ..), (n_ident, ..)| m_ident.cmp(n_ident));
            let expected_segments = [
                // file_id, start_off, end_off
                (1, 0, 66),
                (2, 66, 132),
                (2, 132, 198),
            ]
            .iter()
            .map(|&(file_id, start_off, end_off)| FileSegmentIdent {
                file_id,
                start_off,
                end_off,
            })
            .collect::<Vec<_>>();
            assert_eq!(
                segments_ident.len(),
                expected_segments.len(),
                "{:?}",
                segments_ident
            );
            for ((ident, segment, _), expected) in segments_ident
                .into_iter()
                .zip(expected_segments.into_iter())
            {
                assert_eq!(ident, expected);
                assert_matches!(segment, FileSegmentData::InStore);
            }

            // Open file to verify meta exist.
            for file_id in 1..10 {
                let _ = IaFile::open_in_path(file_id, &fm, &local_path, mgr.clone()).unwrap();
            }
        }
    });

    oss.shutdown();
}

#[test]
fn test_abnormal_local_file() {
    init_log_for_test();

    let (temp_dir, mut oss, dfs_conf) = prepare_dfs("test");
    let temp_dir = temp_dir.path();
    let local_path = temp_dir.join("ia");

    let s3fs = new_dfs_from_config(dfs_conf);
    let _s3fs = s3fs.clone();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let rt = runtime.handle().clone();
    runtime.block_on(async move {
        let file_id = 42;
        let file_type = FileType::Sst;
        let fm = make_file_meta(file_type);
        let (file_data, user_data, table_meta_off) =
            make_sstable(file_id, BLOCK_SIZE, 10, 7, 5, false);
        s3fs.put_object(
            s3fs.file_key(file_id, file_type),
            file_data,
            format!("{}.{}", file_id, file_type.suffix()),
        )
        .await
        .unwrap();

        let ia_cap =
            IaCapacity::MemoryAndDiskCap(0.into(), vec![local_path.clone()], 100000.into());
        let options = IaManagerOptionsBuilder::default()
            .capacity(ia_cap)
            .segment_size(SEGMENT_SIZE)
            .freq_update_interval(FREQ_UPDATE_INTERVAL)
            .build()
            .unwrap();
        let mgr = IaManager::new(options, s3fs.clone(), None, rt.into()).unwrap();

        {
            let dfs_opts = dfs::Options::default().with_shard(1, 1);
            let table_meta_data = IaFile::prepare_table_meta(
                file_id,
                file_type,
                table_meta_off,
                &local_path,
                &dfs_opts,
                &mgr,
                None,
            )
            .await
            .unwrap();
            let table_meta_file = InMemFile::new(file_id, table_meta_data);
            let ia_file =
                IaFile::open(file_id, &fm, Arc::new(table_meta_file), mgr.clone()).unwrap();
            let seg = ia_file.multi_read_async(0, user_data.len()).await.unwrap();
            assert_eq!(seg, user_data);
        }

        // Remove the local meta & segment.
        {
            tokio::fs::remove_file(table_meta_file_local_path(file_id, file_type, &local_path))
                .await
                .unwrap();

            let main_store = LocalFileStore::new(vec![local_path.clone()], 1_usize);
            main_store
                .remove(
                    file_id,
                    &(FileSegmentIdent {
                        file_id,
                        start_off: 10,
                        end_off: 20,
                    }
                    .local_filename()),
                )
                .unwrap();
        }

        {
            // First open file failed due to local meta not found.
            IaFile::open_in_path(file_id, &fm, &local_path, mgr.clone()).unwrap_err();
            // Prepare again.
            let dfs_opts = dfs::Options::default().with_shard(1, 1);
            IaFile::prepare_table_meta(
                file_id,
                file_type,
                table_meta_off,
                &local_path,
                &dfs_opts,
                &mgr,
                None,
            )
            .await
            .unwrap();
            let ia_file = IaFile::open_in_path(file_id, &fm, &local_path, mgr.clone()).unwrap();

            // Read can handle local segment not found by retry to get from remote.
            let seg = ia_file.multi_read_async(0, user_data.len()).await.unwrap();
            assert_eq!(seg, user_data);
        }
    });

    oss.shutdown();
}

#[test]
fn test_local_gc() {
    init_log_for_test();

    let (temp_dir, mut oss, dfs_conf) = prepare_dfs("test");
    let temp_dir = temp_dir.path();

    let s3fs = new_dfs_from_config(dfs_conf);
    let _s3fs = s3fs.clone();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let rt = runtime.handle().clone();
    runtime.block_on(async move {
        let local_paths = [temp_dir.join("ia"), temp_dir.join("ia_extra")];
        let segment_paths = vec![local_paths[0].join("seg"), local_paths[1].join("seg")];
        let meta_paths = vec![local_paths[0].join("meta"), local_paths[1].join("meta")];
        for segment_path in &segment_paths {
            fs::create_dir_all(segment_path).unwrap();
        }
        for meta_path in &meta_paths {
            fs::create_dir_all(meta_path).unwrap();
        }

        let ia_cap = IaCapacity::MemoryAndDiskCap(0.into(), segment_paths.clone(), 1000.into());
        let options = IaManagerOptionsBuilder::default()
            .capacity(ia_cap)
            .segment_size(SEGMENT_SIZE)
            .freq_update_interval(FREQ_UPDATE_INTERVAL)
            .build()
            .unwrap();

        let file_type = FileType::Sst;
        let fm = make_file_meta(file_type);
        let file_count = 10;

        let mgr = IaManager::new(options, s3fs.clone(), None, rt.clone().into()).unwrap();

        let mut files = Vec::with_capacity(file_count);
        for i in 1..=file_count {
            let file_id = i as u64;
            let (file_data, _, table_meta_off) = make_sstable(file_id, BLOCK_SIZE, 10, 7, 5, false);

            s3fs.put_object(
                s3fs.file_key(file_id, file_type),
                file_data,
                format!("{}.{}", file_id, file_type.suffix()),
            )
            .await
            .unwrap();

            files.push((file_id, file_type, table_meta_off));
        }

        let dfs_opts = dfs::Options::default().with_shard(1, 1);
        for (file_id, file_type, table_meta_off) in files {
            let meta_path = get_local_dir(&meta_paths, file_id);
            IaFile::prepare_table_meta(
                file_id,
                file_type,
                table_meta_off,
                meta_path,
                &dfs_opts,
                &mgr,
                None,
            )
            .await
            .unwrap();
        }

        let meta_path_1 = get_local_dir(&meta_paths, 1);
        let ia1 = IaFile::open_in_path(1, &fm, meta_path_1, mgr.clone()).unwrap();
        let data1_5_10 = ia1.multi_read_async(5, 10).await.unwrap();
        assert_eq!(ia1.multi_read_async(5, 10).await.unwrap(), data1_5_10);

        let meta_path_2 = get_local_dir(&meta_paths, 2);
        let ia2 = IaFile::open_in_path(2, &fm, meta_path_2, mgr.clone()).unwrap();
        let data2_100_64 = ia2.multi_read_async(100, 64).await.unwrap();
        assert_eq!(ia2.multi_read_async(100, 64).await.unwrap(), data2_100_64);

        // To make sure that segments are written to local store.
        mgr.flush_tasks(Duration::from_secs(5)).await.unwrap();

        // No meta is GCed.
        {
            let config = IaGcConfig::default();
            let mut gc_runner = IaGcRunner::new(config, mgr.clone(), Arc::new(meta_paths.clone()));
            assert_eq!(gc_runner.meta_file_gc(|_| false).unwrap(), 0);
        }

        // All metas are GCed.
        {
            let config = IaGcConfig {
                meta_lifetime: ReadableDuration::ZERO,
                ..Default::default()
            };
            let mut gc_runner = IaGcRunner::new(config, mgr.clone(), Arc::new(meta_paths.clone()));
            assert_eq!(gc_runner.meta_file_gc(|_| false).unwrap(), file_count);

            // Opened IA files are not affected.
            assert_eq!(ia1.multi_read_async(5, 10).await.unwrap(), data1_5_10);
            assert_eq!(ia2.multi_read_async(100, 64).await.unwrap(), data2_100_64);
        }

        // No segment is GCed.
        {
            let config = IaGcConfig {
                segment_interval: ReadableDuration::ZERO,
                ..Default::default()
            };
            let mut gc_runner = IaGcRunner::new(config, mgr.clone(), Arc::new(meta_paths.clone()));
            assert_eq!(gc_runner.segment_gc(|_| false).unwrap(), 0);

            assert_eq!(ia1.multi_read_async(5, 10).await.unwrap(), data1_5_10);
            assert_eq!(ia2.multi_read_async(100, 64).await.unwrap(), data2_100_64);
        }

        // All segments are GCed.
        let mut seg_count = 0;
        for i in 0..local_paths.len() {
            let segment_path = &segment_paths[i];
            if fs::read_dir(segment_path).unwrap().next().is_some() {
                seg_count += 1;
            }
        }
        assert!(seg_count > 0);

        // Open IA manager on another path.
        let local_path = &local_paths[0];
        let another_segment_path = local_path.join("seg1");
        fs::create_dir_all(&another_segment_path).unwrap();
        let ia_cap =
            IaCapacity::MemoryAndDiskCap(0.into(), vec![another_segment_path], 1000.into());
        let options = IaManagerOptionsBuilder::default()
            .capacity(ia_cap)
            .build()
            .unwrap();
        let mgr = IaManager::new(options, s3fs.clone(), None, rt.clone().into()).unwrap();

        let config = IaGcConfig {
            segment_interval: ReadableDuration::ZERO,
            ..Default::default()
        };
        let mut gc_runner = IaGcRunner::new(config, mgr.clone(), Arc::new(meta_paths.clone()));
        gc_runner.set_segment_path(segment_paths.clone()); // Change to original path which has segments.
        assert!(gc_runner.segment_gc(|_| false).unwrap() > 0); // The number of segments is not determined.

        for segment_path in &segment_paths {
            assert!(fs::read_dir(segment_path).unwrap().next().is_none());
        }

        // GC for "*.tmp" files.
        let segment_path_1 = get_local_dir(&segment_paths, 1);
        fs::write(segment_path_1.join("1.sst.tmp"), b"data").unwrap();
        assert!(fs::read_dir(segment_path_1).unwrap().next().is_some());
        let meta_path = get_local_dir(&meta_paths, 2);
        fs::write(meta_path.join("2.sst.tmp"), b"data").unwrap();
        assert!(fs::read_dir(meta_path).unwrap().next().is_some());

        {
            let config = IaGcConfig::default();
            let mut gc_runner = IaGcRunner::new(config, mgr.clone(), Arc::new(meta_paths.clone()));
            gc_runner.set_segment_path(segment_paths.clone());
            assert_eq!(gc_runner.segment_gc(|_| false).unwrap(), 0);
            assert_eq!(gc_runner.meta_file_gc(|_| false).unwrap(), 0);
        }

        {
            let config = IaGcConfig {
                segment_interval: ReadableDuration::ZERO,
                tmp_lifetime: ReadableDuration::ZERO,
                ..Default::default()
            };
            let mut gc_runner = IaGcRunner::new(config, mgr.clone(), Arc::new(meta_paths.clone()));
            gc_runner.set_segment_path(segment_paths.clone());
            assert_eq!(gc_runner.segment_gc(|_| false).unwrap(), 1);
            assert!(fs::read_dir(segment_path_1).unwrap().next().is_none());

            assert_eq!(gc_runner.meta_file_gc(|_| false).unwrap(), 1);
            assert!(fs::read_dir(meta_path).unwrap().next().is_none());
        }
    });

    oss.shutdown();
}

#[test]
fn test_blob_ia() {
    init_log_for_test();

    let (temp_dir, mut oss, dfs_conf) = prepare_dfs("test");
    let temp_dir = temp_dir.path();

    let s3fs = new_dfs_from_config(dfs_conf);
    let _s3fs = s3fs.clone();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let rt = runtime.handle().clone();
    runtime.block_on(async move {
        let local_path = temp_dir.join("ia");
        let ia_cap = IaCapacity::MemoryAndDiskCap(0.into(), vec![local_path.clone()], 1000.into());
        let options = IaManagerOptionsBuilder::default()
            .capacity(ia_cap)
            .segment_size(SEGMENT_SIZE)
            .freq_update_interval(FREQ_UPDATE_INTERVAL)
            .build()
            .unwrap();

        let mgr = IaManager::new(options.clone(), s3fs.clone(), None, rt.clone().into()).unwrap();

        let kvs = generate_key_values("key", 1024);
        let (sst_data, sst_meta_off, blob_data, blob_meta_off) =
            make_blob_table_with_kvs(1, 2, &kvs);

        s3fs.put_object(
            s3fs.file_key(1, FileType::Sst),
            sst_data,
            "1.sst".to_string(),
        )
        .await
        .unwrap();
        s3fs.put_object(
            s3fs.file_key(2, FileType::Blob),
            blob_data.clone(),
            "2.blob".to_string(),
        )
        .await
        .unwrap();

        let dfs_opts = dfs::Options::default().with_shard(1, 1);
        // Prepare table meta.
        IaFile::prepare_table_meta(
            1,
            FileType::Sst,
            sst_meta_off,
            &local_path,
            &dfs_opts,
            &mgr,
            None,
        )
        .await
        .unwrap();
        IaFile::prepare_table_meta(
            2,
            FileType::Blob,
            blob_meta_off,
            &local_path,
            &dfs_opts,
            &mgr,
            None,
        )
        .await
        .unwrap();

        let ia_sst =
            IaFile::open_in_path(1, &make_file_meta(FileType::Sst), &local_path, mgr.clone())
                .unwrap();
        let _ = ia_sst.multi_read_async(5, 10).await.unwrap();
        let _ = ia_sst.multi_read_async(5, 10).await.unwrap();

        let ia_blob =
            IaFile::open_in_path(2, &make_file_meta(FileType::Blob), &local_path, mgr.clone())
                .unwrap();
        let data1 = ia_blob.multi_read_async(100, 64).await.unwrap();
        assert!(data1.len() < SEGMENT_SIZE as usize * 2);
        let data2 = ia_blob.multi_read_async(100, 64).await.unwrap();
        assert_eq!(data1, data2);

        // To make sure that segments are written to local store.
        mgr.flush_tasks(Duration::from_secs(5)).await.unwrap();
    });

    oss.shutdown();
}

pub(crate) fn generate_key_values(prefix: &str, n: usize) -> Vec<(String, String)> {
    assert!(n <= 10000);
    let mut results = Vec::with_capacity(n);
    for i in 0..n {
        let k = format!("{}{:04}", prefix, i);
        // Generate an random string with random length.
        let length = rand::thread_rng().gen_range(0..=128);
        let random_string: String = (0..length)
            .map(|_| rand::thread_rng().gen_range(b'a'..=b'z') as char)
            .collect();
        results.push((k, random_string));
    }
    results
}

fn make_sstable(
    file_id: u64,
    block_size: usize,
    n: usize,
    key_len: usize,
    val_len: usize,
    multi_ver: bool,
) -> (
    Bytes, // file_data
    Bytes, // user_data
    u64,   // meta_off
) {
    let mut rng = thread_rng();

    let mut builder = sstable::Builder::new(
        file_id,
        block_size,
        NO_COMPRESSION,
        0,
        ChecksumType::default(),
        None,
    );
    let mut val = vec![0; val_len];
    let mut ver = n as u64;
    let mut i = 0;
    for _ in 0..n {
        let key = format!("{:0key_len$}", i).into_bytes();
        rng.fill_bytes(val.as_mut_slice());
        let value_buf = Value::encode_buf(0u8, &[0], ver, &val);
        let value = Value::decode(&value_buf);
        builder.add(InnerKey::from_inner_buf(&key), &value, None);

        if multi_ver && rng.gen_ratio(1, 4) {
            ver -= 1;
        } else {
            i += 1;
            ver = n as u64;
        }
    }

    let mut buf = Vec::with_capacity(builder.estimated_size());
    let res = builder.finish(0, &mut buf);
    let file_data = Bytes::from(buf);
    let user_data = file_data.slice(0..res.meta_offset as usize);
    (file_data, user_data, res.meta_offset as u64)
}

fn make_blob_table_with_kvs(
    sst_fid: u64,
    blob_fid: u64,
    kvs: &Vec<(String, String)>,
) -> (Bytes, u64, Bytes, u64) {
    let mut sst_builder = sstable::Builder::new(
        sst_fid,
        BLOCK_SIZE,
        NO_COMPRESSION,
        0,
        ChecksumType::default(),
        None,
    );
    let mut blob_builder = BlobTableBuilder::new(blob_fid, NO_COMPRESSION, 0, 0, 32, None);
    let meta = 0u8;

    for (k, v) in kvs {
        let value_buf = Value::encode_buf(meta, &[0], 0, v.as_bytes());
        let mut v = Value::decode(value_buf.as_slice());
        v.set_blob_ref();
        let blob_ref = blob_builder.add(InnerKey::from_inner_buf(k.as_bytes()), &v);
        sst_builder.add(InnerKey::from_inner_buf(k.as_bytes()), &v, Some(blob_ref));
    }

    let mut buf = Vec::with_capacity(sst_builder.estimated_size());

    let build_result = sst_builder.finish(0, &mut buf);

    (
        buf.into(),
        build_result.meta_offset as u64,
        blob_builder.finish(),
        blob_builder.meta_offset() as u64,
    )
}
