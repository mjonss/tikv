// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod common;
mod error;
mod load_data;
pub mod local_gc;
mod metrics;
pub mod native_br; // pub for tests.
mod remote_cop;
mod schema_manager;
mod server;
mod txn_chunk;
mod worker_limiter;
mod worker_scaler;

use std::{
    fs,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    thread,
    time::Duration,
};

use ::load_data::{
    metrics::LOAD_DATA_WRU_COST_COUNTER,
    task::{LoadDataConfig, ResourceGroupConfig},
};
use ::native_br::{backup::BackupConfig, restore::RestoreConfig};
use dashmap::DashMap;
use kvengine::{
    context::{new_meta_file_cache, IaCtx},
    dfs::{DFSConfig, Dfs},
    ia::{manager::IaManager, util::IaConfig},
    table::{
        columnar::ColumnarMetaCache,
        sstable::{BlockCache, BlockCacheType},
        ChecksumType,
    },
    txn_chunk_manager::{TxnChunkManager, TxnChunkManagerConfig},
};
use kvproto::metapb::Store;
#[cfg(feature = "testexport")]
pub use metrics::REMOTE_COMPACT_REQ_HANDLE_HISTOGRAM;
use metrics::WORKER_MEMORY_LIMITER_CURRENT_USED;
use pd_client::PdClient;
use prometheus::labels;
use replication_worker::{CdcMsg, ReplicationWorker, ReplicationWorkerConfig};
#[cfg(feature = "testexport")]
pub use schema_manager::get_keyspace_stats_from_store;
pub use schema_manager::{
    broadcast_schema_update_to_all_stores, SchemaManager, SchemaManagerConfig, SchemaMgrContext,
};
use security::{SecurityConfig, SecurityManager};
pub use server::get_cop_req_tag;
use slog_global::{error, info, warn};
use tikv::config::MemoryConfig;
use tikv_util::{
    config::{AbsoluteOrPercentSize, ReadableDuration, ReadableSize},
    memory::MemoryLimiter,
    quota_limiter::QuotaLimiter,
    sys::{record_global_memory_usage, SysQuota},
    time::Instant,
};
use tokio::{runtime::Runtime, sync::oneshot};
pub use txn_chunk::CreateTxnChunkResp;

use crate::{
    common::{Running, RunningController},
    load_data::LoadDataManager,
    local_gc::{LocalGcConfig, LocalGcRunner},
    native_br::{NativeBrConfig, NativeBrManager},
    remote_cop::RemoteCopServer,
    txn_chunk::TxnChunkHandler,
    worker_limiter::{WorkerLimiter, WorkerLimiterConfig},
    worker_scaler::{
        WorkerScaler, WorkerScalerConfig, LOAD_DATA_WORKER_ENV, LOAD_DATA_WORKER_WORKER_NUM_ENV,
    },
};

const BACKGROUND_WORKER_INTERVAL: Duration = Duration::from_secs(60); //1min
const RG_CONFIG_PATH: &str = "resource_group/controller";

const SERVER_READ_TIMEOUT: Duration = Duration::from_secs(600);
const SERVER_TCP_KEEPALIVE: Duration = Duration::from_secs(120);

struct ServerFuture {
    http_server: Box<dyn Future<Output = hyper::Result<()>> + Send + Unpin>,
    _grpc_server: Option<RemoteCopServer>,
}

impl ServerFuture {
    pub fn new(
        http_server: Box<dyn Future<Output = hyper::Result<()>> + Send + Unpin>,
        grpc_server: Option<RemoteCopServer>,
    ) -> Self {
        ServerFuture {
            http_server,
            _grpc_server: grpc_server,
        }
    }
}

impl Future for ServerFuture {
    type Output = hyper::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.http_server).poll(cx)
    }
}

// Entry for `tikv_worker` binary.
pub fn run_cloud_worker(config: Config, config_file_path: Option<PathBuf>, pd: Arc<dyn PdClient>) {
    let worker_threads =
        SysQuota::cpu_cores_quota() * config.thread_pool_size_factor.clamp(1.0, 8.0);
    let thread_pool = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(worker_threads as usize)
            .thread_name("worker-server")
            .build()
            .unwrap(),
    );
    let hyper_runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("hyper-server")
            .build()
            .unwrap(),
    );

    let running_ctl = RunningController::default();
    let (server, ctx, svc_handles) = start_server(
        config,
        config_file_path,
        thread_pool.clone(),
        hyper_runtime.clone(),
        pd,
        &running_ctl,
    );
    let (close_tx, close_rx) = tokio::sync::oneshot::channel();
    if cfg!(unix) {
        thread::spawn(move || {
            cloud_server::signal_handler::wait_for_signal();
            close_tx.send(()).unwrap();
        });
    }
    hyper_runtime.block_on(async move {
        tokio::select! {
            res = server => {
                res.expect("cloud worker exit with error");
            }
            _ = close_rx => {}
        }
    });

    drop(running_ctl);
    let force = false;
    stop_services(ctx, svc_handles, force);
}

// `config_file_path`: optional path to the config file which cloud_worker will
// periodically reload config from it.
fn start_server(
    config: Config,
    config_file_path: Option<PathBuf>,
    thread_pool: Arc<Runtime>,
    hyper_runtime: Arc<Runtime>,
    pd: Arc<dyn PdClient>,
    running_ctl: &RunningController,
) -> (ServerFuture, Arc<server::Context>, ServiceHandles) {
    tikv_util::init_task_local_sync(|| {
        start_server_impl(
            config,
            config_file_path,
            thread_pool,
            hyper_runtime,
            pd,
            running_ctl,
        )
    })
}

fn start_server_impl(
    config: Config,
    config_file_path: Option<PathBuf>,
    thread_pool: Arc<Runtime>,
    hyper_runtime: Arc<Runtime>,
    pd: Arc<dyn PdClient>,
    running_ctl: &RunningController,
) -> (ServerFuture, Arc<server::Context>, ServiceHandles) {
    let dfs_config = config.dfs.clone();
    let dfs = Arc::new(kvengine::dfs::S3Fs::new_from_config(dfs_config));

    let block_cache = BlockCache::new(
        BlockCacheType::Quick,
        config.cop_block_cache_size.0,
        config.cop_block_size.0 as usize,
    );
    let columnar_meta_cache = ColumnarMetaCache::default();

    let addr = config.addr.parse().expect("Unable to parse socket address");

    let compression_lvl: i32 = config
        .dfs
        .zstd_compression_level
        .parse()
        .expect("Unable to parse zstd compression level");
    let checksum_type = config.checksum_type;

    let mut incoming = {
        let _enter = hyper_runtime.enter();
        hyper::server::conn::AddrIncoming::bind(&addr)
    }
    .unwrap();

    incoming.set_keepalive(Some(SERVER_TCP_KEEPALIVE));
    incoming.set_nodelay(true);

    let security_mgr = pd.get_security_mgr();
    let master_key = dfs.get_runtime().block_on(config.security.new_master_key());
    let is_load_data_worker = std::env::var(LOAD_DATA_WORKER_ENV).is_ok();
    let mut worker_scaler_opt: Option<WorkerScaler> = None;
    if !is_load_data_worker && config.worker_scaler.run {
        let scaler_cfg = config.worker_scaler.clone();
        let cluster_id = pd.get_cluster_id().unwrap();
        let worker_scaler = thread_pool
            .block_on(WorkerScaler::new(
                &scaler_cfg,
                cluster_id,
                security_mgr.clone(),
            ))
            .unwrap();
        worker_scaler_opt = Some(worker_scaler.clone());
        thread_pool.spawn(async move {
            worker_scaler.run().await;
        });
    }

    if !config.data_dir.is_empty() {
        fs::create_dir_all(&config.data_dir).unwrap();

        match fs2::statvfs(PathBuf::from(&config.data_dir)) {
            Ok(stats) => {
                info!("capacity for data dir: {}, available: {}", stats.total_space(), stats.available_space();
                    "path" => &config.data_dir);
            }
            Err(err) => {
                warn!("get disk capacity failed: {:?}", err; "path" => &config.data_dir);
            }
        }
    }

    let load_data_config = init_load_data_config(&config, &pd, &thread_pool);
    let load_manager = Arc::new(LoadDataManager::new(
        pd.clone(),
        config.data_dir.clone().into(),
        dfs.clone(),
        thread_pool.clone(),
        master_key.clone(),
        worker_scaler_opt,
        config.worker_scaler.clone(),
        load_data_config,
    ));

    let native_br_data_dir = PathBuf::from(&config.data_dir).join("native_br");
    if let Err(e) = fs::create_dir_all(&native_br_data_dir) {
        // Avoid panic on disk full.
        warn!("create data dir failed: {:?}", e; "path" => native_br_data_dir.display());
    }
    let br_manager = Arc::new(NativeBrManager::new(
        thread_pool.clone(),
        pd.clone(),
        dfs.clone(),
        native_br_data_dir,
        config.clone(),
    ));
    spawn_br_background_worker(br_manager.clone(), config_file_path);

    let txn_chunk_handler = Arc::new(TxnChunkHandler::new(config.txn_chunk_target_block_size));

    let worker_limiter = WorkerLimiter::new(config.worker_limiter.clone());

    let memory_upper_threshold = config.memory_upper_threshold.as_memory_size();
    let memory_limiter = MemoryLimiter::new(
        memory_upper_threshold,
        Some(WORKER_MEMORY_LIMITER_CURRENT_USED.clone()),
    );

    let ia_ctx = create_ia_ctx(&config, dfs.clone(), thread_pool.handle())
        .expect("create IA context failed");

    run_local_gc(&config, &ia_ctx, running_ctl.handle());

    // Create `TxnChunkManager` using `thread_pool`. Otherwise, as `TxnChunkManager`
    // is hold in async context, we will meet the panic of dropping tokio
    // runtime in async context.
    let txn_chunk_manager = TxnChunkManager::new(
        vec![],
        dfs.clone(),
        block_cache.clone(),
        None,
        thread_pool.handle().clone().into(),
        config.txn_chunk_manager.clone(),
    );

    let (rep_scheduler, rep_handle) = if config.replication_worker.enabled {
        ReplicationWorker::new(
            pd.clone(),
            dfs.clone(),
            config.data_dir.clone(),
            config.security.clone(),
            config.replication_worker.clone(),
            thread_pool.clone(),
        )
        .map(|mut replication_worker| {
            let scheduler = replication_worker.scheduler();
            let handle =
                thread::spawn(move || tikv_util::init_task_local_sync(|| replication_worker.run()));
            (scheduler, handle)
        })
        .map_err(|e| {
            error!("failed to create replication worker"; "err" => ?e);
            debug_assert!(false, "failed to create replication worker: {:?}", e);
        })
        .ok()
    } else {
        None
    }
    .unzip();
    let meta_file_cache_size = config.cop_block_cache_size.0 / 4;
    let meta_file_cache = new_meta_file_cache(meta_file_cache_size);
    let http_client = Arc::new(
        security_mgr
            .http_client(hyper::Client::builder())
            .expect("create http client failed"),
    );
    let ctx = Arc::new(server::Context {
        config: config.clone(),
        compression_lvl,
        checksum_type,
        thread_pool: thread_pool.handle().clone(),
        dfs: dfs.clone(),
        pd: pd.clone(),
        load_manager: load_manager.clone(),
        br_manager,
        rep_scheduler,
        txn_chunk_handler,
        master_key,
        quota_limiter: Arc::new(QuotaLimiter::default()),
        memory_limiter,
        block_cache,
        columnar_meta_cache,
        schema_files: Some(Arc::new(DashMap::new())),
        worker_limiter,
        txn_chunk_manager,
        ia_ctx,
        read_columnar: config.read_columnar,
        meta_file_cache,
        http_client,
        remote_cache_ttl: config.cop_remote_dfs_cache_ttl.0,
    });
    let acceptor = security_mgr.acceptor(incoming).unwrap();
    let server = start_serve!(ctx.clone(), acceptor);

    if config.schema_manager.enabled {
        let schema_manager = SchemaManager::new(
            Arc::new(ctx.clone().into()),
            security_mgr.clone(),
            config.security.clone(),
            config.schema_manager.clone(),
            config.pd.endpoints.as_ref(),
        );
        schema_manager.run(thread_pool.clone());
    }

    // try recover task from checkpoint
    load_manager.try_recover_or_clean_tasks_by_checkpoint();

    if config.register {
        let remote_compact_url = security_mgr
            .build_uri(format!("{}/compact", config.addr))
            .unwrap()
            .to_string();
        let duration = config.update_interval.0;
        std::thread::spawn(move || {
            loop {
                register_compactor_to_all_stores(
                    pd.clone(),
                    dfs.clone(),
                    remote_compact_url.clone(),
                    security_mgr.clone(),
                );
                std::thread::sleep(duration);
            }
        });
    }

    let mut cop_server_opt = None;
    if !config.cop_addr.is_empty() {
        let cop_config = remote_cop::Config {
            addr: config.cop_addr,
            max_handle_duration: Duration::from_secs(60),
        };
        let cop_server = remote_cop::RemoteCopServer::new(ctx.clone(), cop_config);
        cop_server_opt = Some(cop_server);
    }
    if let Some(server) = cop_server_opt.as_mut() {
        server.start();
        info!("remote cop server started");
    }

    thread_pool.spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            record_global_memory_usage();
        }
    });

    if !config.push_metrics_addr.is_empty() && !config.push_metrics_interval.is_zero() {
        run_prometheus_push(config.push_metrics_addr, config.push_metrics_interval.0);
    }

    let svc_handles = ServiceHandles { rep_handle };
    (ServerFuture::new(server, cop_server_opt), ctx, svc_handles)
}

fn stop_services(ctx: Arc<server::Context>, svc_handles: ServiceHandles, force: bool) {
    if let Some(rep_scheduler) = &ctx.rep_scheduler {
        rep_scheduler.schedule(CdcMsg::Stop);
        if force {
            rep_scheduler.force_stop();
        }
    }
    if let Some(rep_handle) = svc_handles.rep_handle {
        rep_handle.join().expect("replication worker panic");
    }
}

fn run_prometheus_push(push_metrics_addr: String, push_metrics_interval: Duration) {
    let pod_name: String = std::env::var("HOSTNAME").unwrap_or_default();
    if pod_name.is_empty() {
        warn!("failed to get pod name, metrics push will be disabled");
        return;
    }
    let container_name: String =
        std::env::var("CONTAINER_NAME").unwrap_or("tikv-worker".to_owned());
    thread::spawn(move || {
        info!("start to push metrics to prometheus");
        loop {
            thread::sleep(push_metrics_interval);
            let res = prometheus::push_collector(
                "tikv-worker",
                labels! {"pod".to_owned() => pod_name.clone(), "container".to_owned() => container_name.clone()},
                &push_metrics_addr,
                vec![Box::new(LOAD_DATA_WRU_COST_COUNTER.clone())
                    as Box<dyn prometheus::core::Collector>],
                None,
            );
            if let Err(e) = res {
                error!("failed to push metrics to prometheus: {}", e);
            }
        }
    });
}

fn create_ia_ctx(
    config: &Config,
    dfs: Arc<dyn Dfs>,
    runtime: &tokio::runtime::Handle,
) -> Result<IaCtx, String> {
    if config.is_ia_enabled() {
        let ia_path = PathBuf::from(&config.data_dir).join("ia");

        let segment_path = ia_path.join("segment");
        fs::create_dir_all(&segment_path)
            .map_err(|err| format!("create segment path failed: {err:?}"))?;
        let mut opts = config
            .ia
            .to_manager_options(vec![segment_path])
            .map_err(|err| format!("build IA options failed: {err:?}"))?;
        opts.dynamic_capacity = false; // Always disable dynamic capacity.

        let ia_mgr = IaManager::new(opts, dfs, None, runtime.clone().into())
            .map_err(|err| format!("create IA manager failed: {err:?}"))?;

        let meta_path = ia_path.join("meta");
        fs::create_dir_all(&meta_path)
            .map_err(|err| format!("create meta path failed: {err:?}"))?;
        Ok(IaCtx::Enabled(ia_mgr, Arc::new(vec![meta_path])))
    } else {
        Ok(IaCtx::Disabled)
    }
}

fn run_local_gc(config: &Config, ia_ctx: &IaCtx, running: Running) {
    if let IaCtx::Enabled(ia_mgr, meta_path) = ia_ctx {
        let mut local_gc_runner =
            LocalGcRunner::new(config.local_gc.clone(), ia_mgr.clone(), meta_path.clone());
        thread::spawn(move || local_gc_runner.run(running));
    }
}

pub struct CloudWorker {
    config: Config,
    config_file_path: Option<PathBuf>,
    thread_pool: Arc<Runtime>,
    hyper_runtime: Arc<Runtime>,
    pd: Arc<dyn PdClient>,

    server_handle: Option<thread::JoinHandle<()>>,
    ctx: Option<Arc<server::Context>>,
    svc_handles: Option<ServiceHandles>,

    close_tx: oneshot::Sender<()>,
    close_rx: Option<oneshot::Receiver<()>>,
    running_ctl: RunningController,
}

impl CloudWorker {
    pub fn new(
        config: Config,
        config_file_path: Option<PathBuf>,
        threads_cnt: usize,
        pd: Arc<dyn PdClient>,
    ) -> Self {
        let thread_pool = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(threads_cnt)
                .thread_name("worker-server")
                .build()
                .unwrap(),
        );
        let hyper_runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("hyper-server")
                .build()
                .unwrap(),
        );
        let (close_tx, close_rx) = oneshot::channel();
        CloudWorker {
            config,
            config_file_path,
            thread_pool,
            hyper_runtime,
            pd,
            server_handle: None,
            ctx: None,
            svc_handles: None,
            close_tx,
            close_rx: Some(close_rx),
            running_ctl: RunningController::default(),
        }
    }

    pub fn addr(&self) -> &str {
        self.config.addr.as_str()
    }

    pub fn start(&mut self) {
        let close_rx = self.close_rx.take().expect("already started");

        let (server, ctx, svc_handles) = start_server(
            self.config.clone(),
            self.config_file_path.clone(),
            self.thread_pool.clone(),
            self.hyper_runtime.clone(),
            self.pd.clone(),
            &self.running_ctl,
        );
        let addr = self.addr().to_string();
        info!("{} cloud_worker server start", addr; "config" => ?self.config);

        let handle = self.hyper_runtime.handle().clone();
        let server_handle = thread::spawn(move || {
            handle.block_on(async move {
                tokio::select! {
                    _ = close_rx => {
                        info!("{} cloud_worker server shutdown", addr);
                    }
                    res = server => {
                        if let Err(e) = res {
                            error!("{} cloud_worker server error: {:?}", addr, e);
                        } else {
                            info!("{} cloud_worker server graceful shutdown", addr);
                        }
                    }
                }
            })
        });
        self.server_handle = Some(server_handle);
        self.ctx = Some(ctx);
        self.svc_handles = Some(svc_handles);
    }

    pub fn shutdown(self) {
        self.shutdown_opt(false);
    }

    pub fn shutdown_opt(mut self, force: bool) {
        self.running_ctl.stop();
        if let Some((ctx, svc_handles)) = self.ctx.take().zip(self.svc_handles.take()) {
            stop_services(ctx, svc_handles, force);
        }
        if let Some(handle) = self.server_handle.take() {
            if self.close_tx.send(()).is_err() {
                warn!("notify shutdown failed");
            }
            handle.join().unwrap();
        }
    }

    #[cfg(feature = "testexport")]
    pub fn random_force_shutdown(self, ratio: f64) {
        use rand::{thread_rng, Rng};
        let force = thread_rng().gen_bool(ratio);
        self.shutdown_opt(force);
    }
}

fn spawn_br_background_worker(br_manager: Arc<NativeBrManager>, path: Option<PathBuf>) {
    std::thread::spawn(move || {
        info!("start br background worker");
        loop {
            if let Some(path) = path.as_ref() {
                update_native_br_config(br_manager.clone(), path.as_path());
            }

            br_manager.cleanup_expired_restores();

            std::thread::sleep(BACKGROUND_WORKER_INTERVAL);
        }
    });
}

fn update_native_br_config(br_manager: Arc<NativeBrManager>, path: &Path) {
    match std::fs::read(path) {
        Ok(data) => match toml::from_slice::<Config>(&data) {
            Ok(config) => br_manager.update_native_br_config(config.native_br),
            Err(e) => error!("failed to parse config file {:?}", e),
        },
        Err(e) => error!("failed to read config file {:?}", e),
    }
}

fn get_rg_config_from_pd(
    pd_client: &Arc<dyn PdClient>,
    runtime: &Arc<tokio::runtime::Runtime>,
) -> Result<ResourceGroupConfig, error::Error> {
    let configs =
        runtime.block_on(pd_client.load_global_config_by_path(RG_CONFIG_PATH.to_string()))?;
    if let Some(config) = configs.get(RG_CONFIG_PATH) {
        let rg_config = serde_json::from_slice(config)?;
        Ok(rg_config)
    } else {
        Err(error::Error::PdError(
            pd_client::Error::GlobalConfigNotFound("ru config not found".to_string()),
        ))
    }
}

fn init_load_data_config(
    config: &Config,
    pd_client: &Arc<dyn PdClient>,
    runtime: &Arc<tokio::runtime::Runtime>,
) -> LoadDataConfig {
    let mut load_data_config = LoadDataConfig::default();
    load_data_config.enable_checkpoint = config.enable_load_data_check_point;
    load_data_config.checksum_type = config.checksum_type;

    let is_load_data_worker = std::env::var(LOAD_DATA_WORKER_ENV).is_ok();
    if is_load_data_worker {
        let worker_num = std::env::var(LOAD_DATA_WORKER_WORKER_NUM_ENV)
            .unwrap()
            .parse()
            .unwrap();
        load_data_config.kvpairs_worker_num = worker_num;
        load_data_config.building_worker_num = worker_num;
    }

    let rg_config = if config.report_wru {
        let rg_config = get_rg_config_from_pd(pd_client, runtime)
            .unwrap_or_else(|e| panic!("failed to ru config: {:?}", e));
        info!("get rg config from pd: {:?}", rg_config);
        Some(rg_config)
    } else {
        None
    };

    load_data_config.rg_config = rg_config;
    load_data_config
}

pub(crate) fn get_all_stores_except_tiflash(
    pd_client: &Arc<dyn PdClient>,
) -> Result<Vec<Store>, pd_client::Error> {
    Ok(pd_client
        .get_all_stores(true)?
        .into_iter()
        .filter(|s| {
            !s.get_labels().iter().any(|l| {
                // including "tiflash" & "tiflash_compute"
                l.key.to_lowercase() == "engine" && l.value.to_lowercase().starts_with("tiflash")
            })
        })
        .collect())
}

fn register_compactor_to_all_stores(
    pd: Arc<dyn PdClient>,
    dfs: Arc<dyn Dfs>,
    remote_url: String,
    security_mgr: Arc<SecurityManager>,
) {
    let all_stores = match get_all_stores_except_tiflash(&pd) {
        Ok(stores) => stores,
        Err(e) => {
            error!("failed to get all stores {:?}", e);
            return;
        }
    };
    let start_time = Instant::now();
    let stores_len = all_stores.len();
    let (tx, rx) = std::sync::mpsc::sync_channel(stores_len);
    for store in all_stores {
        let tx = tx.clone();
        let remote_url = remote_url.clone();
        let security_mgr = security_mgr.clone();
        dfs.get_runtime().spawn(async move {
            tx.send(register_compactor_to_store(store, remote_url.clone(), security_mgr).await)
                .unwrap();
        });
    }
    let mut finish_cnt = 0;
    for _ in 0..stores_len {
        let ok = rx.recv().unwrap();
        if ok {
            finish_cnt += 1;
        }
    }
    let elapsed = start_time.saturating_elapsed();
    let remain = stores_len - finish_cnt;
    info!(
        "register compactor to {} stores in {:?}, remain {} stores",
        stores_len, elapsed, remain
    );
}

async fn register_compactor_to_store(
    store: Store,
    remote_url: String,
    security_mgr: Arc<SecurityManager>,
) -> bool {
    let uri = security_mgr
        .build_uri(format!("{}/kvengine/compactor", store.status_address))
        .unwrap();
    let client = security_mgr.http_client(hyper::Client::builder()).unwrap();
    let req = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(uri)
        .body(hyper::Body::from(remote_url))
        .expect("request builder");
    match client.request(req).await {
        Ok(resp) => {
            let ok = resp.status() == hyper::StatusCode::OK;
            if !ok {
                error!(
                    "failed to register compactor to store";
                    "store" => ?store, "status" => ?resp.status()
                );
            }
            ok
        }
        Err(err) => {
            error!("failed to register compactor to store"; "store" => ?store, "err" => ?err);
            false
        }
    }
}

#[macro_use]
extern crate serde_derive;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub addr: String,
    pub update_interval: ReadableDuration,
    pub log_file: String,
    pub log_level: String,
    pub data_dir: String,
    pub register: bool,
    pub cop_addr: String,
    pub cop_block_cache_size: ReadableSize,
    // Used to calculate block cache capacity of items. Should be the same as tikv-server.
    pub cop_block_size: ReadableSize,
    // Used to determine if the file is expired on the remote dfs cache.
    pub cop_remote_dfs_cache_ttl: ReadableDuration,
    // When the coprocessor response reached this value, we update paging_size to return early.
    pub cop_max_resp_size: ReadableSize,
    // The thread pool size is cpu_cores * thread_pool_size_factor,
    pub thread_pool_size_factor: f64,
    pub report_wru: bool,
    pub enable_load_data_check_point: bool,
    pub checksum_type: ChecksumType,

    pub txn_chunk_target_block_size: usize,

    pub push_metrics_addr: String,
    pub push_metrics_interval: ReadableDuration,

    /// Enable columnar reader. Default is false.
    pub read_columnar: bool,

    pub memory_upper_threshold: AbsoluteOrPercentSize,

    // Embedded configurations must be placed at the end of the struct.
    // Otherwise, it will fail to serialize to toml.
    pub pd: pd_client::Config,
    pub security: SecurityConfig,
    pub dfs: DFSConfig,
    pub native_br: NativeBrConfig,
    pub worker_scaler: WorkerScalerConfig,
    pub worker_limiter: WorkerLimiterConfig,
    pub schema_manager: SchemaManagerConfig,
    pub txn_chunk_manager: TxnChunkManagerConfig,

    /// Enable IA by setting `!data_dir.is_empty() && ia.mem_cap > 0 &&
    /// ia.disk_cap > 0`.
    pub ia: IaConfig,

    pub local_gc: LocalGcConfig,
    pub replication_worker: ReplicationWorkerConfig,
    pub memory: MemoryConfig,
    // Note: Fields of simple (not structure) type can not be the last. Otherwise serializing the
    // config will meet the "ValueAfterTable" error.
    // See https://docs.rs/toml/0.5.11/toml/ser/enum.Error.html#variant.ValueAfterTable.
}

impl Default for Config {
    fn default() -> Self {
        let mut pd = pd_client::Config::default();
        pd.endpoints.clear();
        let block_cache_size = SysQuota::memory_limit_in_bytes() * 2 / 10;
        Config {
            addr: String::from("0.0.0.0:19000"),
            pd,
            security: SecurityConfig::default(),
            update_interval: ReadableDuration::minutes(10),
            dfs: DFSConfig::default(),
            log_file: String::default(),
            log_level: String::default(),
            data_dir: String::default(),
            register: false,
            native_br: NativeBrConfig::default(),
            cop_addr: String::from("0.0.0.0:9500"),
            cop_block_cache_size: ReadableSize(block_cache_size),
            cop_block_size: ReadableSize::kb(32),
            cop_remote_dfs_cache_ttl: ReadableDuration::minutes(10),
            cop_max_resp_size: ReadableSize::mb(32),
            thread_pool_size_factor: 1.0,
            worker_scaler: WorkerScalerConfig::default(),
            report_wru: false,
            enable_load_data_check_point: false,
            checksum_type: ChecksumType::Crc32,
            worker_limiter: WorkerLimiterConfig::default(),
            schema_manager: SchemaManagerConfig::default(),
            txn_chunk_manager: TxnChunkManagerConfig::default(),
            txn_chunk_target_block_size: txn_chunk::TARGET_BLOCK_SIZE_DEF,
            ia: IaConfig::default(),
            push_metrics_addr: String::default(),
            push_metrics_interval: ReadableDuration::secs(30),
            read_columnar: false,
            memory_upper_threshold: AbsoluteOrPercentSize::Percent(80.0),
            local_gc: LocalGcConfig::default(),
            replication_worker: ReplicationWorkerConfig::default(),
            memory: Default::default(),
        }
    }
}

impl Config {
    pub fn to_backup_config(&self) -> BackupConfig {
        let tolerate_err = usize::from(self.native_br.backup_tolerate_err);
        BackupConfig {
            pd: self.pd.clone(),
            security: self.security.clone(),
            dfs: self.dfs.clone(),
            tolerate_err,
            timeout: self.native_br.instant_backup_timeout / 2,
            backup_delay: self.native_br.backup_delay,
            backup_ts_wait_timeout: self.native_br.backup_ts_wait_timeout,
            backup_ts_ttl: self.native_br.backup_ts_ttl,
            #[cfg(feature = "testexport")]
            skip_keyspace_meta: self.native_br.backup_skip_keyspace_meta,
        }
    }
    pub fn to_restore_config(&self) -> RestoreConfig {
        let tolerate_err = usize::from(self.native_br.restore_tolerate_err);
        RestoreConfig {
            pd: self.pd.clone(),
            security: self.security.clone(),
            dfs: self.dfs.clone(),
            timeout_wait_flush: self.native_br.restore_timeout_wait_flush,
            timeout_restore_snapshot: self.native_br.restore_timeout_restore_snapshot,
            restore_snapshot_concurrency: self.native_br.restore_snapshot_concurrency,
            timeout_fetch_wal: self.native_br.restore_timeout_fetch_wal,
            timeout_pd_control: self.native_br.restore_timeout_pd_control,
            max_retry: self.native_br.restore_max_retry,
            store_concurrency: self.native_br.restore_store_concurrency,
            tolerate_err,
            coarse_split_regions_factor: self.native_br.restore_coarse_split_regions_factor,
            lower_memory: self.native_br.lower_memory,
            cache_for_decompressed_wal_chunks: self
                .native_br
                .restore_cache_for_decompressed_wal_chunks,
            ..Default::default()
        }
    }

    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.worker_limiter.validate()?;
        Ok(())
    }

    pub fn is_remote_cop_enabled(&self) -> bool {
        !self.addr.is_empty() || !self.cop_addr.is_empty()
    }

    pub fn is_ia_enabled(&self) -> bool {
        self.is_remote_cop_enabled()
            && !self.data_dir.is_empty()
            && (!self.ia.mem_cap.is_zero() && !self.ia.disk_cap.is_zero())
    }
}

struct ServiceHandles {
    rep_handle: Option<thread::JoinHandle<()>>,
}
