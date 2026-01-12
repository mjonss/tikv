// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

use bytes::Buf;
use clap::Args;
use cloud_encryption::MasterKey;
use cloud_worker::{SchemaManager, SchemaManagerConfig, SchemaMgrContext};
use colored::*;
use futures::stream::{FuturesUnordered, StreamExt};
use http::{Request, StatusCode};
use hyper::Body;
use kvengine::{
    context::{new_meta_file_cache, IaCtx, PrepareType, SnapCtx},
    dfs::{DFSConfig, Dfs, S3Fs},
    ia::{manager::IaManager, util::IaConfig},
    table::{
        columnar::{Block, ColumnarFilterReader, ColumnarMetaCache, GLOBAL_COMMON_HANDLE_END},
        file::FdCache,
        schema_file::{Schema, SchemaFile},
        sstable::BlockCache,
    },
    txn_chunk_manager::{TxnChunkManager, TxnChunkManagerConfig},
    ShardStatsLite, SnapAccess,
};
use kvproto::{coprocessor::DelegateResponse, metapb::Store};
use native_br::common::{create_pd_client, send_request_to_store};
use pd_client::PdClient;
use protobuf::Message;
use security::{SecurityConfig, SecurityManager};
use tidb_query_datatype::codec::table::{decode_common_handle, decode_int_handle, decode_table_id};
use tikv_util::{
    config::AbsoluteOrPercentSize, error, info, memory::MemoryLimiter, worker_pool::WorkerPool,
};
use tokio::{fs::OpenOptions, io::AsyncWriteExt, sync::Semaphore};
const CHECK_RESULT_FILE: &str = "check_columnar_result.txt";

// This command is used to check the pk column loss in columnar files.
// See https://github.com/tidbcloud/cloud-storage-engine/pull/3210 for more details.
const CHECK_TYPE_PK_COLUMN: &str = "check-pk-column";
// This command is used to check the multiple table orders and overlap in
// columnar files.
const CHECK_TYPE_MUL_TABLE_ORDERS: &str = "check-mul-table-orders";
// This command is used to check biggest key in columnar file.
// See https://github.com/tidbcloud/cloud-storage-engine/pull/3561 for more details.
const CHECK_TYPE_BIGGEST_HANDLE: &str = "check-biggest-handle";
// This command is used to check the row count in columnar files consistent with
// the row count in the sstables.
const CHECK_TYPE_ROW_COUNT: &str = "check-row-count";

#[derive(Args)]
pub struct CheckColumnarArgs {
    /// The path of the config file.
    #[clap(long, default_value = "")]
    pub config: PathBuf,
    /// PD endpoints, use `,` to separate multiple PDs
    #[clap(long, default_value_t = String::new())]
    pub pd: String,
    /// Path of file that contains list of trusted SSL CAs
    #[clap(long, default_value = "")]
    pub cacert: PathBuf,
    /// Path of file that contains X509 certificate in PEM format
    #[clap(long, default_value = "")]
    pub cert: PathBuf,
    /// Path of file that contains X509 key in PEM format
    #[clap(long, default_value = "")]
    pub key: PathBuf,
    /// The keyspace id to check columnar, if not set, check all keyspaces.
    #[clap(long, default_value_t = 0)]
    pub keyspace_id: u32,
    /// The keyspace id start to check columnar, if not set, check from 0.
    #[clap(long, default_value_t = 0)]
    pub keyspace_id_start: u32,
    /// The shard id to check columnar, if not set, check all shards.
    #[clap(long, default_value_t = 0)]
    pub shard_id: u64,
    /// The path of the schema file.
    #[clap(long, default_value = "")]
    pub schemas_path: PathBuf,
    /// The path of the working directory.
    #[clap(long, default_value = "/tmp/cse-ctl-check-columnar")]
    pub working_dir: PathBuf,
    /// The type of check columnar.
    #[clap(long, default_value = "")]
    pub check_type: String,
    /// Maximum concurrent shards to process
    #[clap(long, default_value_t = 4)]
    pub max_concurrency: usize,
}

pub(crate) fn execute_check_columnar(args: CheckColumnarArgs) {
    let config = CheckColumnarConfig::from_args(&args);
    let pd_client = Arc::new(create_pd_client(&config.security, &config.pd));
    let dfs_cfg = config.dfs.clone();
    let s3fs = Arc::new(S3Fs::new_from_config(dfs_cfg));

    // Use more worker threads for better parallelism
    let worker_threads = std::cmp::max(4, args.max_concurrency);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(worker_threads)
        .build()
        .unwrap();
    let columnar_meta_cache = ColumnarMetaCache::default();

    let ctx = Arc::new(SchemaMgrContext {
        dfs: s3fs.clone(),
        pd: pd_client,
        columnar_meta_cache,
    });
    let security_mgr = Arc::new(SecurityManager::new(&config.security).unwrap());
    let mut schema_mgr_config = SchemaManagerConfig::default();
    if args.schemas_path.exists() {
        schema_mgr_config.dir = PathBuf::from(args.schemas_path.to_str().unwrap());
    }
    let schema_manager = SchemaManager::new(
        ctx.clone(),
        security_mgr.clone(),
        config.security.clone(),
        schema_mgr_config,
        &config.pd.endpoints,
    );

    runtime.block_on(check_columnar(
        ctx,
        schema_manager,
        security_mgr,
        &config,
        args.max_concurrency,
    ));
}

async fn check_columnar(
    ctx: Arc<SchemaMgrContext>,
    schema_manager: SchemaManager,
    security_mgr: Arc<SecurityManager>,
    config: &CheckColumnarConfig,
    max_concurrency: usize,
) {
    let (stores, _) = schema_manager.get_tikv_stores();
    if stores.is_empty() {
        panic!("no tikv stores found");
    }
    let mut keyspace_stats = HashMap::new();
    if let Err(e) = schema_manager
        .refresh_keyspace_stats(&mut keyspace_stats, &stores)
        .await
    {
        panic!("refresh keyspace stats error: {:?}", e);
    }

    // sort keyspace_stats by keyspace_id, only collect shards has columnar_tables
    let mut sorted_keyspace_stats = keyspace_stats
        .into_iter()
        .filter_map(|(keyspace_id, shard_stats)| {
            let filtered_shards: Vec<_> = shard_stats
                .into_iter()
                .filter(|shard| shard.columnar_tables > 0)
                .collect();
            if filtered_shards.is_empty() {
                None
            } else {
                Some((keyspace_id, filtered_shards))
            }
        })
        .collect::<Vec<_>>();
    sorted_keyspace_stats.sort_by_key(|(keyspace_id, _)| *keyspace_id);
    if config.keyspace_id > 0 {
        sorted_keyspace_stats.retain(|(keyspace_id, _)| *keyspace_id == config.keyspace_id);
    }
    if config.keyspace_id_start > 0 {
        sorted_keyspace_stats.retain(|(keyspace_id, _)| *keyspace_id >= config.keyspace_id_start);
    }

    let total_keyspace_count = if config.shard_id > 0 {
        1
    } else {
        sorted_keyspace_stats.len()
    };
    let total_regions = if config.shard_id > 0 {
        1
    } else {
        sorted_keyspace_stats
            .iter()
            .map(|(_, shard_stats)| shard_stats.len())
            .sum::<usize>()
    };

    info!(
        "Starting concurrent processing with {} max concurrent tasks",
        max_concurrency
    );
    info!(
        "Total keyspaces: {}, Total regions: {}",
        total_keyspace_count, total_regions
    );

    let master_key = config.security.new_master_key().await;
    let txn_chunk_manager = TxnChunkManager::new(
        vec![],
        ctx.dfs.clone(),
        BlockCache::None,
        None,
        WorkerPool::Handle(ctx.dfs.get_runtime().handle().clone()),
        TxnChunkManagerConfig::default(),
    );
    let ia_config = IaConfig {
        mem_cap: AbsoluteOrPercentSize::Percent(5.0),
        disk_cap: AbsoluteOrPercentSize::Percent(50.0),
        ..Default::default()
    };
    let ia_mgr = build_ia_mgr(
        ctx.dfs.clone(),
        ctx.dfs.get_runtime(),
        &config.working_dir,
        &ia_config,
    );

    let semaphore = Arc::new(Semaphore::new(max_concurrency));
    let mut tasks = FuturesUnordered::new();
    let total_processed = Arc::new(AtomicUsize::new(0));

    for (keyspace_idx, (keyspace_id, shard_stats)) in sorted_keyspace_stats.into_iter().enumerate()
    {
        let total_region_count_in_keyspace = if config.shard_id > 0 {
            1
        } else {
            shard_stats.len()
        };

        for (shard_idx, shard) in shard_stats.into_iter().enumerate() {
            if config.shard_id > 0 && shard.id != config.shard_id {
                continue;
            }
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let ctx_clone = ctx.clone();
            let security_mgr_clone = security_mgr.clone();
            let txn_chunk_manager_clone = txn_chunk_manager.clone();
            let ia_mgr_clone = ia_mgr.clone();
            let config_clone = config.clone();
            let master_key_clone = master_key.clone();
            let stores_clone = stores.clone();
            let check_type_clone = config.check_type.clone();
            let schema_manager_clone = schema_manager.clone();
            let total_processed_clone = total_processed.clone();

            let task = tokio::spawn(async move {
                let _permit = permit;

                let result = check_columnar_for_shard(
                    ctx_clone,
                    schema_manager_clone,
                    security_mgr_clone,
                    txn_chunk_manager_clone,
                    ia_mgr_clone,
                    &config_clone,
                    &master_key_clone,
                    &shard,
                    &stores_clone,
                    &check_type_clone,
                )
                .await;

                let current_processed =
                    total_processed_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                info!(
                    "Completed check for keyspace_id: {} ({}/{}), shard {} ({}/{}), total progress {}/{}",
                    keyspace_id,
                    keyspace_idx + 1,
                    total_keyspace_count,
                    shard.id,
                    shard_idx + 1,
                    total_region_count_in_keyspace,
                    current_processed,
                    total_regions,
                );

                match result {
                    Ok(true) => {
                        info!(
                            "check {} for shard {}:{}:{}",
                            "SUCCESS".green().bold(),
                            keyspace_id,
                            shard.id,
                            shard.ver
                        );
                    }
                    Ok(false) => {
                        error!(
                            "check {} for shard {}:{}:{}",
                            "FAILED".red().bold(),
                            keyspace_id,
                            shard.id,
                            shard.ver
                        );
                        write_failure_result(keyspace_id, &shard).await;
                    }
                    Err(ref e) => {
                        error!(
                            "check {} for shard {}:{}:{}: {}",
                            "ERROR".bright_yellow().bold(),
                            keyspace_id,
                            shard.id,
                            shard.ver,
                            e
                        );
                    }
                }

                result
            });

            tasks.push(task);
        }
    }

    while let Some(task_result) = tasks.next().await {
        if let Err(e) = task_result {
            error!("Task panicked: {:?}", e);
        }
    }

    info!(
        "All checks completed. Processed {} regions total.",
        total_processed.load(std::sync::atomic::Ordering::SeqCst)
    );
}

async fn write_failure_result(keyspace_id: u32, shard: &ShardStatsLite) {
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(CHECK_RESULT_FILE)
        .await
    {
        let _ = file
            .write_all(format!("{}:{}:{}\n", keyspace_id, shard.id, shard.ver).as_bytes())
            .await;
    }
}

async fn request_snapshot_from_shard(
    ctx: Arc<SchemaMgrContext>,
    security_mgr: Arc<SecurityManager>,
    txn_chunk_manager: TxnChunkManager,
    ia_mgr: IaManager,
    working_dir: &Path,
    master_key: &MasterKey,
    shard: &ShardStatsLite,
    stores: &[Store],
    check_type: &str,
) -> Result<SnapAccess, String> {
    let memory_limiter = MemoryLimiter::new(u64::MAX, None);
    let Some(leader_store_id) = get_leader_store(ctx.pd.clone(), shard.id).await else {
        return Err(format!(
            "get leader store failed for shard {}:{}, ignore, region has no leader",
            shard.id, shard.ver
        ));
    };
    let store = stores
        .iter()
        .find(|store| store.get_id() == leader_store_id);
    let Some(store) = store else {
        return Err(format!(
            "store {} not found in stores for shard {}:{}",
            leader_store_id, shard.id, shard.ver
        ));
    };
    let mut delegate_resp = DelegateResponse::default();
    let status_addr = store.get_status_address();
    let uri = security_mgr
        .build_uri(format!(
            "{}/kvengine/snapshot/{}?shard_ver={}&start_ts={}",
            status_addr,
            shard.id,
            shard.ver,
            u64::MAX,
        ))
        .unwrap();
    let req = Request::get(uri.clone()).body(Body::empty()).unwrap();
    let client = security_mgr
        .http_client(hyper::Client::builder())
        .map_err(|e| format!("create http client failed: {:?}", e))?;
    let Ok((resp_code, resp)) =
        send_request_to_store(req, store, &client, Duration::from_secs(10)).await
    else {
        return Err(format!(
            "get snapshot failed for shard {}:{}",
            shard.id, shard.ver
        ));
    };
    if resp_code != StatusCode::OK {
        return Err(format!(
            "get snapshot failed for shard {}:{}, status code: {}",
            shard.id, shard.ver, resp_code
        ));
    }
    delegate_resp.merge_from_bytes(resp.as_ref()).unwrap();
    if delegate_resp.has_region_error()
        && (delegate_resp.get_region_error().has_not_leader()
            || delegate_resp.get_region_error().has_epoch_not_match())
    {
        return Err(format!(
            "get snapshot failed for shard {}:{} due to region error: {:?}, skip",
            shard.id,
            shard.ver,
            delegate_resp.get_region_error()
        ));
    }
    if delegate_resp.has_locked() {
        return Err(format!(
            "get snapshot failed for shard {}:{} due to locked {:?}, skip",
            shard.id,
            shard.ver,
            delegate_resp.get_locked()
        ));
    }
    let tag = format!("{}:{}", shard.id, shard.ver);
    // Check row count needs to read all the data.
    let prepare_type = if check_type == CHECK_TYPE_ROW_COUNT {
        PrepareType::All
    } else {
        PrepareType::ColumnarOnly
    };
    let snap_ctx = SnapCtx {
        dfs: ctx.dfs.clone(),
        master_key: master_key.clone(),
        block_cache: BlockCache::None,
        vector_index_cache: None,
        columnar_file_cache: None,
        meta_file_cache: new_meta_file_cache(0),
        schema_files: None,
        txn_chunk_manager,
        ia_ctx: IaCtx::Enabled(ia_mgr, Arc::new(vec![working_dir.to_path_buf()])),
        prepare_type,
        read_columnar: true,
        columnar_meta_cache: ctx.columnar_meta_cache.clone(),
    };
    let mut delegate_resp = DelegateResponse::default();
    delegate_resp.merge_from_bytes(resp.as_ref()).unwrap();
    let (snap, _) = SnapAccess::construct_snapshot(
        &tag,
        &snap_ctx,
        delegate_resp.get_mem_table_data(),
        delegate_resp.get_snapshot(),
        memory_limiter.clone(),
    )
    .await
    .unwrap();

    Ok(snap)
}

async fn check_columnar_for_shard(
    ctx: Arc<SchemaMgrContext>,
    schema_manager: SchemaManager,
    security_mgr: Arc<SecurityManager>,
    txn_chunk_manager: TxnChunkManager,
    ia_mgr: IaManager,
    config: &CheckColumnarConfig,
    master_key: &MasterKey,
    shard: &ShardStatsLite,
    stores: &[Store],
    check_type: &str,
) -> Result<bool, String> {
    let snap = request_snapshot_from_shard(
        ctx,
        security_mgr,
        txn_chunk_manager,
        ia_mgr,
        &config.working_dir,
        master_key,
        shard,
        stores,
        check_type,
    )
    .await?;

    let keyspace_id = snap.get_keyspace_id();
    match config.check_type.as_str() {
        CHECK_TYPE_PK_COLUMN => {
            let Some(schema_file) = schema_manager
                .get_schema_file_from_local(keyspace_id)
                .unwrap()
            else {
                return Err(format!(
                    "no schema file found for keyspace_id: {}",
                    keyspace_id
                ));
            };
            check_pk_column(&snap, &schema_file, shard).await
        }
        CHECK_TYPE_MUL_TABLE_ORDERS => check_mul_table_orders(&snap),
        CHECK_TYPE_BIGGEST_HANDLE => check_biggest_handle(&snap),
        CHECK_TYPE_ROW_COUNT => check_row_count(&snap).await,
        _ => Err(format!(
            "invalid check type: {}, available check types: {}, {}, {}, {}",
            config.check_type,
            CHECK_TYPE_PK_COLUMN,
            CHECK_TYPE_MUL_TABLE_ORDERS,
            CHECK_TYPE_BIGGEST_HANDLE,
            CHECK_TYPE_ROW_COUNT
        )),
    }
}

async fn check_pk_column(
    snap: &SnapAccess,
    schema_file: &SchemaFile,
    shard: &ShardStatsLite,
) -> Result<bool, String> {
    for table_id in snap.get_columnar_table_ids() {
        let Some(schema) = schema_file.get_table(table_id) else {
            error!(
                "table {} not found in schema file for shard {}:{}",
                table_id, shard.id, shard.ver
            );
            continue;
        };
        if !schema.with_columnar() || !schema.is_common_handle() {
            continue;
        }

        let mut columns = vec![];
        for col_id in &schema.pk_col_ids {
            let column = schema.find_column_by_id(*col_id).unwrap();
            columns.push(column.clone());
        }
        let schema = snap.new_schema_from_columns(table_id, &columns).unwrap();
        let mut columnar_reader = snap
            .new_columnar_mvcc_reader(table_id, &columns, None, u64::MAX, None)
            .unwrap()
            .unwrap();
        let mut block = Block::new(&schema);
        columnar_reader
            .set_handle_range(&[], GLOBAL_COMMON_HANDLE_END)
            .await
            .unwrap();
        let mut read_rows = columnar_reader.read_block(&mut block, 1024).await.unwrap();
        while read_rows > 0 {
            // check if pk col empty
            for i in 0..read_rows {
                let handle = block.get_handle_buf().get_not_null_value(i);
                let version = block.get_version_buf().get_not_null_value(i);
                for col in block.get_columns().iter() {
                    let col_data = col.get_not_null_value(i);
                    if col_data.is_empty() {
                        error!(
                            "check_failed pk col empty: handle: {}, version: {}, col_id: {}",
                            log_wrappers::hex_encode_upper(handle),
                            log_wrappers::hex_encode_upper(version),
                            col.col_id()
                        );
                        // Check failed.
                        return Ok(false);
                    }
                }
            }
            block.reset();
            read_rows = columnar_reader.read_block(&mut block, 1024).await.unwrap();
        }
    }

    Ok(true)
}

fn check_mul_table_orders(snap: &SnapAccess) -> Result<bool, String> {
    let columnar_table_ids = snap.get_columnar_table_ids();
    if columnar_table_ids.len() <= 1 {
        return Ok(true);
    }
    let columnar_files = snap.get_columnar_levels();
    for (file, _) in &columnar_files {
        let smallest_key = file.get_smallest();
        let biggest_key = file.get_biggest();
        let smallest_table_id = decode_table_id(smallest_key.as_ref()).unwrap();
        let biggest_table_id = decode_table_id(biggest_key.as_ref()).unwrap();
        let tables_unordered = file
            .get_table_ids()
            .iter()
            .any(|&table_id| table_id < smallest_table_id || table_id > biggest_table_id);
        if tables_unordered {
            error!(
                "keyspace_id: {}, shard: {}, tables unordered in columnar file: {}",
                snap.get_keyspace_id(),
                snap.get_id(),
                file.id()
            );
            return Ok(false);
        }
    }
    // Check if tables in level 2 are overlapped.
    let mut col_files_in_l2 = columnar_files
        .iter()
        .filter(|(_, level)| *level == 2)
        .map(|(file, _)| file)
        .collect::<Vec<_>>();
    if col_files_in_l2.is_empty() {
        return Ok(true);
    }
    col_files_in_l2.sort_by(|a, b| a.get_smallest().cmp(&b.get_smallest()));

    let mut last_key = col_files_in_l2[0].get_biggest();
    let mut last_file_id = col_files_in_l2[0].id();
    for file in col_files_in_l2[1..].iter() {
        let smallest_key = file.get_smallest();
        let biggest_key = file.get_biggest();
        if smallest_key < last_key {
            error!(
                "keyspace_id: {}, shard: {}, tables overlapped in l2 columnar file: {} and {}",
                snap.get_keyspace_id(),
                snap.get_id(),
                file.id(),
                last_file_id
            );
            return Ok(false);
        }
        last_key = biggest_key;
        last_file_id = file.id();
    }
    Ok(true)
}

fn check_biggest_handle(snap: &SnapAccess) -> Result<bool, String> {
    let Some(schema_file) = snap.get_schema_file() else {
        return Ok(false);
    };
    let columnar_files = snap.get_columnar_levels();
    for (file, _) in &columnar_files {
        let biggest_key = file.get_biggest();
        let biggest_table_id = decode_table_id(biggest_key.as_ref()).unwrap();
        let Some(schema) = schema_file.get_table(biggest_table_id) else {
            continue;
        };
        let Some(last_handle) = file.get_table_last_handle(biggest_table_id) else {
            continue;
        };
        let is_common_handle = schema.is_common_handle();
        if is_common_handle {
            let handle_in_biggest_key = decode_common_handle(biggest_key.as_ref()).unwrap();
            let biggest_handle = &last_handle[..last_handle.len() - 1];
            if handle_in_biggest_key != biggest_handle {
                error!(
                    "keyspace_id: {}, shard: {}, file_id: {}, last handle mismatch: {}, {}",
                    snap.get_keyspace_id(),
                    snap.get_id(),
                    file.id(),
                    log_wrappers::hex_encode_upper(handle_in_biggest_key),
                    log_wrappers::hex_encode_upper(biggest_handle)
                );
                return Ok(false);
            }
        } else {
            let handle_in_biggest_key = decode_int_handle(biggest_key.as_ref()).unwrap();
            let biggest_handle = last_handle.as_slice().get_i64_le() - 1;
            if handle_in_biggest_key != biggest_handle {
                error!(
                    "keyspace_id: {}, shard: {}, file_id: {}, last handle mismatch: {}, {}",
                    snap.get_keyspace_id(),
                    snap.get_id(),
                    file.id(),
                    handle_in_biggest_key,
                    biggest_handle
                );
                return Ok(false);
            }
        }
    }
    Ok(true)
}

async fn check_row_count(snap: &SnapAccess) -> Result<bool, String> {
    let read_ts = u64::MAX;
    let columnar_table_ids = snap.get_columnar_table_ids();
    for table_id in columnar_table_ids {
        let row_count = read_from_sstable(snap, read_ts, table_id).await?;
        let columnar_row_count = read_from_columnar(snap, read_ts, table_id).await?;
        info!(
            "check_row_count for shard: {}, table_id: {}, row_count: {}, columnar_row_count: {}",
            snap.get_id(),
            table_id,
            row_count,
            columnar_row_count
        );
        if columnar_row_count != row_count {
            error!(
                "keyspace_id: {}, shard: {} row count mismatch, columnar: {}, row: {}",
                snap.get_keyspace_id(),
                snap.get_id(),
                columnar_row_count,
                row_count
            );
            return Ok(false);
        }
    }

    Ok(true)
}

async fn read_from_sstable(snap: &SnapAccess, read_ts: u64, table_id: i64) -> Result<u64, String> {
    let schema_file = snap.get_schema_file().unwrap();
    let is_common_handle = schema_file.get_table(table_id).unwrap().is_common_handle();
    let inner_start = snap.get_inner_start().to_vec();
    let inner_end = snap.get_inner_end().to_vec();
    let shard_start_table_id = decode_table_id(&inner_start).unwrap_or(i64::MIN);
    let shard_end_table_id = decode_table_id(&inner_end).unwrap_or(i64::MAX);
    let start_common_handle = if table_id == shard_start_table_id {
        decode_common_handle(&inner_start).unwrap_or(&[])
    } else {
        &[]
    };
    let end_common_handle = if table_id == shard_end_table_id {
        decode_common_handle(&inner_end).unwrap_or(GLOBAL_COMMON_HANDLE_END)
    } else {
        GLOBAL_COMMON_HANDLE_END
    };
    let start_int_handle = if table_id == shard_start_table_id {
        decode_int_handle(&inner_start).unwrap_or(i64::MIN)
    } else {
        i64::MIN
    };
    let end_int_handle = if table_id == shard_end_table_id {
        decode_int_handle(&inner_end).unwrap_or(i64::MAX)
    } else {
        i64::MAX
    };

    let mut mvcc_reader = snap
        .new_columnar_mvcc_reader_from_row(table_id, &[], read_ts)
        .unwrap();

    if is_common_handle {
        mvcc_reader
            .set_handle_range(start_common_handle, end_common_handle)
            .await
            .unwrap();
    } else {
        mvcc_reader
            .set_int_handle_range(start_int_handle, Some(end_int_handle))
            .await
            .unwrap();
    }
    let schema: Schema = schema_file
        .get_table(table_id)
        .unwrap()
        .retain_columns(|_col| false)
        .into();
    let mut block = Block::new(&schema);
    let mut row_count = 0;
    let mut read_rows = mvcc_reader.read_block(&mut block, 10240).await.unwrap();
    while read_rows > 0 {
        block.reset();
        row_count += read_rows;
        read_rows = mvcc_reader.read_block(&mut block, 10240).await.unwrap();
    }

    Ok(row_count as u64)
}

async fn read_from_columnar(snap: &SnapAccess, read_ts: u64, table_id: i64) -> Result<u64, String> {
    let schema_file = snap.get_schema_file().unwrap();
    let inner_start = snap.get_inner_start().to_vec();
    let inner_end = snap.get_inner_end().to_vec();
    let shard_start_table_id = decode_table_id(&inner_start).unwrap_or(i64::MIN);
    let shard_end_table_id = decode_table_id(&inner_end).unwrap_or(i64::MAX);
    let start_common_handle = if table_id == shard_start_table_id {
        decode_common_handle(&inner_start).unwrap_or(&[])
    } else {
        &[]
    };
    let end_common_handle = if table_id == shard_end_table_id {
        decode_common_handle(&inner_end).unwrap_or(GLOBAL_COMMON_HANDLE_END)
    } else {
        GLOBAL_COMMON_HANDLE_END
    };
    let start_int_handle = if table_id == shard_start_table_id {
        decode_int_handle(&inner_start).unwrap_or(i64::MIN)
    } else {
        i64::MIN
    };
    let end_int_handle = if table_id == shard_end_table_id {
        decode_int_handle(&inner_end).unwrap_or(i64::MAX)
    } else {
        i64::MAX
    };
    let mut columnar_reader = snap
        .new_columnar_mvcc_reader(table_id, &[], None, read_ts, None)
        .unwrap()
        .unwrap();

    let schema = schema_file.get_table(table_id).unwrap();
    let is_common_handle = schema.is_common_handle();
    if is_common_handle {
        columnar_reader
            .set_handle_range(start_common_handle, end_common_handle)
            .await
            .unwrap();
    } else {
        columnar_reader
            .set_int_handle_range(start_int_handle, Some(end_int_handle))
            .await
            .unwrap();
    }
    let schema: Schema = schema_file
        .get_table(table_id)
        .unwrap()
        .retain_columns(|_col| false)
        .into();
    let mut block = Block::new(&schema);
    let mut columnar_row_count = 0;
    let mut read_rows = columnar_reader.read_block(&mut block, 10240).await.unwrap();
    while read_rows > 0 {
        block.reset();
        columnar_row_count += read_rows;
        read_rows = columnar_reader.read_block(&mut block, 10240).await.unwrap();
    }
    Ok(columnar_row_count as u64)
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CheckColumnarConfig {
    pub pd: pd_client::Config,
    pub security: SecurityConfig,
    pub dfs: DFSConfig,
    pub keyspace_id: u32,
    pub keyspace_id_start: u32,
    pub shard_id: u64,
    pub working_dir: PathBuf,
    pub check_type: String,
}

impl CheckColumnarConfig {
    pub fn from_args(args: &CheckColumnarArgs) -> Self {
        let mut config = Self::default();
        if args.config.exists() {
            let data = std::fs::read(args.config.as_path()).expect("failed to read config file");
            config = toml::from_slice(&data).unwrap();
        }
        // override from args and ENV
        if !args.pd.is_empty() {
            config.pd.endpoints = args.pd.split(',').map(|x| x.to_owned()).collect();
        }
        if !args.working_dir.display().to_string().is_empty() {
            config.working_dir = args.working_dir.clone();
        }
        if args.keyspace_id > 0 {
            config.keyspace_id = args.keyspace_id;
        }
        if args.keyspace_id_start > 0 {
            config.keyspace_id_start = args.keyspace_id_start;
        }
        if args.shard_id > 0 {
            config.shard_id = args.shard_id;
        }
        if !args.check_type.is_empty() {
            config.check_type = args.check_type.clone();
        }
        if args.cacert.exists() {
            config.security.ca_path = args.cacert.to_str().unwrap().to_owned();
        }
        if args.cert.exists() {
            config.security.cert_path = args.cert.to_str().unwrap().to_owned();
        }
        if args.key.exists() {
            config.security.key_path = args.key.to_str().unwrap().to_owned();
        }
        config.dfs.override_from_env();
        config.security.override_from_env();

        config
    }
}

fn build_ia_mgr(
    dfs: Arc<dyn Dfs>,
    runtime: &tokio::runtime::Runtime,
    data_dir: &Path,
    ia: &IaConfig,
) -> IaManager {
    // Create the data directory if it doesn't exist.
    if !data_dir.exists() {
        std::fs::create_dir_all(data_dir).unwrap();
    }
    let options = ia.to_manager_options(vec![data_dir.to_path_buf()]).unwrap();
    let handle = runtime.handle().clone();
    let fd_cache = FdCache::new(ia.fd_cache_capacity);
    IaManager::new(options, dfs, Some(fd_cache), handle.into()).unwrap()
}

async fn get_leader_store(pd_client: Arc<dyn PdClient>, region_id: u64) -> Option<u64> {
    let (_, peer) = pd_client
        .get_region_leader_by_id(region_id)
        .await
        .unwrap()?;
    Some(peer.get_store_id())
}
