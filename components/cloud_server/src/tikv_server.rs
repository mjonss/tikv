// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! This module startups all the components of a TiKV server.
//!
//! It is responsible for reading from configs, starting up the various server
//! components, and handling errors (mostly by aborting and reporting to the
//! user).
//!
//! The entry point is `run_tikv`.
//!
//! Components are often used to initialize other components, and/or must be
//! explicitly stopped. We keep these components in the `TiKVServer` struct.

use std::{
    cell::RefCell,
    collections::HashMap,
    env, fmt,
    fs::{self, File},
    net::SocketAddr,
    ops::AddAssign,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Once},
    thread,
};

use api_version::{KvFormat, dispatch_api_version};
use cloud_encryption::MasterKey;
use concurrency_manager::ConcurrencyManager;
use engine_rocks::from_rocks_compression_type;
use engine_traits::{CF_DEFAULT, CF_WRITE};
use file_system::{
    BytesFetcher, IoRateLimitMode, IoRateLimiter, MetricsManager as IoMetricsManager,
};
use fs2::FileExt;
use futures::executor::block_on;
use grpcio::{EnvBuilder, Environment};
use kvengine::{
    IoContext,
    dfs::Dfs,
    ia::util::IaConfig,
    limiter::{LimiterOptions, StoreLimiter},
};
use kvproto::{
    brpb::create_backup, deadlock::create_deadlock, diagnosticspb_grpc::create_diagnostics,
    import_sstpb_grpc::create_import_sst, raft_serverpb::StoreIdent,
};
use overload_protector::{OverloadProtector, OverloadProtectorWorker};
use pd_client::{
    INVALID_ID, PdClient, RpcClient, metrics::STORE_SIZE_GAUGE_VEC, pd_control::PdControl,
};
use prometheus::labels;
use protobuf::Message;
use raftstore::{RegionInfoAccessor, coprocessor::CoprocessorHost};
use resource_control::{
    CpuType, ReadLimiter, ReadSubscriber, ResourceController, ResourceType, TransferLeaderLimiter,
    TransferLeaderSubscriber,
};
use rfengine::{RfEngine, STORE_IDENT_KEY};
use rfstore::{
    RaftRouter,
    store::{
        BlackList, Engines, LocalReader, MetaChangeListener, PENDING_MSG_CAP, PdIdAllocator,
        RaftBatchSystem, StoreMeta, StoreMsg, load_raft_engine_meta,
    },
};
use security::SecurityManager;
use sst_importer::SstImporter;
use tikv::{
    config::{ConfigController, TikvConfig},
    coprocessor,
    read_pool::{build_tokio_pool, build_yatp_read_pool},
    server::{
        CPU_CORES_QUOTA_GAUGE, DEFAULT_CLUSTER_ID, config::Config as ServerConfig,
        lock_manager::LockManager,
    },
    storage::{
        SCHED_WRITE_FLOW_GAUGE,
        txn::flow_controller::{CLOUD_MIN_THROTTLE_SPEED, FlowController},
    },
    tikv_build_version,
};
use tikv_util::{
    PANIC_REGION_FILE_PREFIX, check_environment_variables,
    config::{ReadableDuration, ReadableSize, VersionTrack, ensure_dir_exist},
    get_panic_region_count, mpsc,
    quota_limiter::{QuotaLimitConfigManager, QuotaLimiter},
    sys::{SysQuota, register_memory_usage_high_water, thread::ThreadBuildWrapper},
    thread_group::GroupProperties,
    time::{Duration, Instant, Monitor},
    worker::{Builder as WorkerBuilder, LazyWorker, Worker},
};
use tokio::runtime::Builder;

use crate::{
    node::*,
    raftkv::*,
    resolve,
    server::{GRPC_THREAD_PREFIX, Server},
    service::{DiagnosticsService, ImportSstService},
    setup::{initial_logger, initial_metric, validate_and_persist_config},
    status_server::StatusServer,
};

const RESERVED_OPEN_FDS: u64 = 1000;

const DEFAULT_METRICS_FLUSH_INTERVAL: Duration = Duration::from_millis(10_000);

const ZSTD_COMPRESSION_LEVEL_FOR_LOCAL: &str = "3";

const PD_CLIENT_RETRY_COUNT: usize = 10;
const ENV_K8S_HOST: &str = "KUBERNETES_SERVICE_HOST";
const K8S_MIN_DISK_CAPACITY: u64 = ReadableSize::gb(50).0;

const BLACKLIST_KEYSPACE_FILE: &str = "blacklist_keyspace.json";
const WHITELIST_KEYSPACE_FILE: &str = "whitelist_keyspace.json";

/// A complete TiKV server.
pub struct TikvServer {
    config: TikvConfig,
    cfg_controller: Option<ConfigController>,
    security_mgr: Arc<SecurityManager>,
    pd_client: Arc<dyn PdClient>,
    system: Option<RaftBatchSystem>,
    router: RaftRouter,
    resolver: resolve::PdStoreAddrResolver,
    store_path: PathBuf,
    raw_engines: Engines,
    raft_kv: Option<RaftKv>,
    servers: Option<Servers>,
    region_info_accessor: RegionInfoAccessor,
    coprocessor_host: Option<CoprocessorHost>,
    to_stop: Vec<Box<dyn Stop>>,
    lock_files: Vec<File>,
    concurrency_manager: ConcurrencyManager,
    env: Arc<Environment>,
    background_worker: Worker,
    quota_limiter: Arc<QuotaLimiter>,
    io_rate_limiter: Arc<IoRateLimiter>,
    overload_protector: OverloadProtector,
    flow_controller: Arc<FlowController>,
    resource_controller: ResourceController,
}

struct Servers {
    lock_mgr: LockManager,
    server: Server<RaftRouter, resolve::PdStoreAddrResolver>,
    node: Node,
    importer: Arc<SstImporter>,
}

impl TikvServer {
    pub fn new(mut config: TikvConfig) -> TikvServer {
        let (security_mgr, env, pd, dfs) = Self::prepare(&mut config);
        Self::setup(config, security_mgr, env, pd, dfs)
    }

    #[allow(clippy::type_complexity)]
    pub fn prepare(
        config: &mut TikvConfig,
    ) -> (
        Arc<SecurityManager>,
        Arc<Environment>,
        Arc<dyn PdClient>,
        Arc<dyn Dfs>,
    ) {
        // Sets the global logger ASAP.
        // It is okay to use the config w/o `validate()`,
        // because `initial_logger()` handles various conditions.
        initial_logger(config);

        // Print version information.
        let build_timestamp = option_env!("TIKV_BUILD_TIME");
        tikv::log_tikv_info("TiKV", build_timestamp);

        // Print resource quota.
        SysQuota::log_quota();
        CPU_CORES_QUOTA_GAUGE.set(SysQuota::cpu_cores_quota());

        // Do some prepare works before start.
        pre_start();

        let _m = Monitor::default();
        tikv_util::thread_group::set_properties(Some(GroupProperties::default()));
        // It is okay use pd config and security config before `init_config`,
        // because these configs must be provided by command line, and only
        // used during startup process.
        let security_mgr = Arc::new(
            SecurityManager::new(&config.security)
                .unwrap_or_else(|e| fatal!("failed to create security manager: {}", e)),
        );
        let cpu_cores = SysQuota::cpu_cores_quota();
        let grpc_concurrency =
            (cpu_cores * config.server.grpc_concurrency_factor).max(1.0) as usize;
        let props = tikv_util::thread_group::current_properties();
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(grpc_concurrency)
                .name_prefix(thd_name!(GRPC_THREAD_PREFIX))
                .after_start(move || {
                    // Don't need to `add_thread_name_to_map`. See `grpc_load_stats` in
                    // `Server::start`.
                    tikv_alloc::add_thread_memory_accessor();
                    tikv_util::thread_group::set_properties(props.clone());
                })
                .before_stop(move || {
                    tikv_alloc::remove_thread_memory_accessor();
                })
                .build(),
        );
        let pd_client =
            TikvServer::connect_to_pd_cluster(config, env.clone(), Arc::clone(&security_mgr));

        config.dfs.override_from_env();
        config.security.override_from_env();

        // If zstd_compression_level is not set, set it to default value
        if config.dfs.zstd_compression_level.is_empty() {
            config.dfs.zstd_compression_level = ZSTD_COMPRESSION_LEVEL_FOR_LOCAL.to_string();
        }

        let dfs_conf = &config.dfs;
        let dfs: Arc<dyn Dfs> = match dfs_conf.backend.to_ascii_lowercase().as_str() {
            "azure" => kvengine::dfs::new_dfs_from_config(dfs_conf.clone()),
            _ => {
                if dfs_conf.s3_bucket.is_empty() && dfs_conf.s3_endpoint.is_empty() {
                    info!("disable IA because builtin DFS is used");
                    config.kvengine.ia = IaConfig::disabled();
                    let builtin_dfs = builtin_dfs::BuiltinDfs::new(pd_client.clone());
                    Arc::new(builtin_dfs)
                } else if dfs_conf.s3_endpoint == "memory" {
                    Arc::new(kvengine::dfs::InMemFs::new())
                } else {
                    kvengine::dfs::new_dfs_from_config(dfs_conf.clone())
                }
            }
        };

        (security_mgr, env, pd_client, dfs)
    }

    pub fn setup(
        config: TikvConfig,
        security_mgr: Arc<SecurityManager>,
        env: Arc<Environment>,
        pd_client: Arc<dyn PdClient>,
        dfs: Arc<dyn Dfs>,
    ) -> TikvServer {
        // Initialize and check config
        let cfg_controller = Self::init_config(config);
        let config = cfg_controller.get_current();
        let (flow_controller, store_limiter) = Self::init_flow_control(&config);
        let io_rate_limiter = Arc::new(IoRateLimiter::new(IoRateLimitMode::WriteOnly, true, true));
        io_rate_limiter
            .set_io_rate_limit(config.storage.io_rate_limit.max_bytes_per_sec.0 as usize);
        let master_key = dfs.get_runtime().block_on(config.security.new_master_key());
        let raw_engines = Self::init_raw_engines(
            pd_client.clone(),
            &config,
            dfs,
            io_rate_limiter.clone(),
            store_limiter,
            master_key,
            security_mgr.clone(),
        );

        let store_path = Path::new(&config.storage.data_dir).to_owned();

        // Initialize raftstore channels.
        let rfstore_conf =
            rfstore::store::Config::from_old(&config.raft_store, &config.coprocessor);
        let system = rfstore::store::RaftBatchSystem::new(&raw_engines, &rfstore_conf);
        let router = system.router();

        let thread_count = config.server.background_thread_count;
        let background_worker = WorkerBuilder::new("background")
            .thread_count(thread_count)
            .create();

        let resolver =
            resolve::new_resolver(Arc::clone(&pd_client), &background_worker, router.clone());
        let mut coprocessor_host = Some(CoprocessorHost::default());
        let region_info_accessor = RegionInfoAccessor::new(coprocessor_host.as_mut().unwrap());

        // Initialize concurrency manager
        let mut latest_ts = None;
        for idx in 0..PD_CLIENT_RETRY_COUNT {
            match block_on(pd_client.get_tso()) {
                Ok(ts) => {
                    latest_ts = Some(ts);
                    break;
                }
                Err(e) => {
                    warn!(
                        "failed to get timestamp from PD, retry {} times, error: {}",
                        idx + 1,
                        e
                    );
                }
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        if latest_ts.is_none() {
            panic!("failed to get timestamp from PD");
        }
        let concurrency_manager =
            ConcurrencyManager::new_opt(latest_ts.unwrap(), config.storage.check_backup_ts);

        // use different quota for front-end and back-end requests
        let quota_limiter = Arc::new(QuotaLimiter::new(
            config.quota.foreground_cpu_time,
            config.quota.foreground_write_bandwidth,
            config.quota.foreground_read_bandwidth,
            config.quota.background_cpu_time,
            config.quota.background_write_bandwidth,
            config.quota.background_read_bandwidth,
            config.quota.max_delay_duration,
            config.quota.enable_auto_tune,
        ));
        let resource_controller = Self::init_resource_controller(&config);
        let mut overload_protector_worker = OverloadProtectorWorker::new(config.overload.clone());
        let overload_protector = overload_protector_worker.get_protector();
        std::thread::spawn(move || {
            overload_protector_worker.run();
        });
        info!("created tikv server");
        TikvServer {
            config,
            cfg_controller: Some(cfg_controller),
            security_mgr,
            pd_client,
            router,
            system: Some(system),
            resolver,
            store_path,
            raw_engines,
            raft_kv: None,
            servers: None,
            region_info_accessor,
            coprocessor_host,
            to_stop: vec![],
            lock_files: vec![],
            concurrency_manager,
            env,
            background_worker,
            quota_limiter,
            io_rate_limiter,
            overload_protector,
            flow_controller: Arc::new(flow_controller),
            resource_controller,
        }
    }

    pub fn run(&mut self) {
        let memory_limit = self.config.memory_usage_limit.unwrap().0;
        let high_water = (self.config.memory_usage_high_water * memory_limit as f64) as u64;
        register_memory_usage_high_water(high_water);

        self.check_conflict_addr();
        self.init_fs();
        self.init_yatp();
        let store_meta = StoreMeta::new(PENDING_MSG_CAP);
        self.init_raftkv(&store_meta);

        let server_config = dispatch_api_version!(self.config.storage.api_version(), {
            self.init_servers::<API>(store_meta)
        });

        self.register_services();
        let fetcher = self.init_io_utility();
        self.init_metrics_flusher(fetcher);
        self.run_server(server_config);
        self.run_status_server();
        if self.config.gc.enable_safe_point_v2 {
            self.run_watch_ks_gc_safepoint();
        }
        if !self.config.server.push_metrics_addr.is_empty()
            && !self.config.server.push_metrics_interval.is_zero()
        {
            self.run_prometheus_push();
        }
        self.resource_controller.run();
    }

    fn run_watch_ks_gc_safepoint(&mut self) {
        let pd_clone = self.pd_client.clone();
        self.background_worker.remote().spawn(async move {
            pd_clone.watch_gc_safepoint_v2().await;
        });
    }

    fn run_prometheus_push(&mut self) {
        let interval = self.config.server.push_metrics_interval;
        let push_addr = self.config.server.push_metrics_addr.clone();
        let pod_name = std::env::var("HOSTNAME").unwrap_or("".to_owned());
        if pod_name.is_empty() {
            warn!("failed to get pod name, metrics push will be disabled");
            return;
        }
        thread::spawn(move || {
            info!("start to push metrics to prometheus");
            loop {
                thread::sleep(interval.0);
                let res = prometheus::push_collector(
                    "tikv-server",
                    labels! {"pod".to_owned() => pod_name.clone(), "container".to_owned() => "tikv".to_owned()},
                    &push_addr,
                    vec![Box::new(STORE_SIZE_GAUGE_VEC.clone())
                        as Box<dyn prometheus::core::Collector>],
                    None,
                );
                if let Err(e) = res {
                    error!("failed to push metrics to prometheus: {}", e);
                }
            }
        });
    }

    /// Initialize and check the config
    ///
    /// Warnings are logged and fatal errors exist.
    /// This method is also used by cse-ctl for cluster restore.
    ///
    /// #  Fatal errors
    ///
    /// - If `dynamic config` feature is enabled and failed to register config
    ///   to PD
    /// - If some critical configs (like data dir) are differrent from last run
    /// - If the config can't pass `validate()`
    /// - If the max open file descriptor limit is not high enough to support
    ///   the main database and the raft database.
    pub fn init_config(mut config: TikvConfig) -> ConfigController {
        validate_and_persist_config(&mut config, true);

        ensure_dir_exist(&config.storage.data_dir).unwrap();
        if !config.rocksdb.wal_dir.is_empty() {
            ensure_dir_exist(&config.rocksdb.wal_dir).unwrap();
        }

        ensure_dir_exist(&config.raft_store.raftdb_path).unwrap();
        if !config.raftdb.wal_dir.is_empty() {
            ensure_dir_exist(&config.raftdb.wal_dir).unwrap();
        }

        check_system_config(&config);

        tikv_util::set_panic_hook(config.abort_on_panic, &config.storage.data_dir);

        // Init memory related settings.
        config.memory.init();

        info!(
            "using config";
            "config" => serde_json::to_string(&config).unwrap(),
        );
        if config.panic_when_unexpected_key_or_data {
            info!("panic-when-unexpected-key-or-data is on");
            tikv_util::set_panic_when_unexpected_key_or_data(true);
        }

        config.write_into_metrics();

        ConfigController::new(config)
    }

    fn connect_to_pd_cluster(
        config: &mut TikvConfig,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
    ) -> Arc<RpcClient> {
        let pd_client = Arc::new(
            RpcClient::new(&config.pd, Some(env), security_mgr)
                .unwrap_or_else(|e| fatal!("failed to create rpc client: {}", e)),
        );

        let cluster_id = pd_client
            .get_cluster_id()
            .unwrap_or_else(|e| fatal!("failed to get cluster id: {}", e));
        if cluster_id == DEFAULT_CLUSTER_ID {
            fatal!("cluster id can't be {}", DEFAULT_CLUSTER_ID);
        }
        config.server.cluster_id = cluster_id;
        info!(
            "connect to PD cluster";
            "cluster_id" => cluster_id
        );

        pd_client
    }

    fn check_conflict_addr(&mut self) {
        let cur_addr: SocketAddr = self
            .config
            .server
            .addr
            .parse()
            .expect("failed to parse into a socket address");
        let cur_ip = cur_addr.ip();
        let cur_port = cur_addr.port();
        let lock_dir = get_lock_dir();

        let search_base = env::temp_dir().join(lock_dir);
        std::fs::create_dir_all(&search_base)
            .unwrap_or_else(|_| panic!("create {} failed", search_base.display()));

        for entry in fs::read_dir(&search_base).unwrap().flatten() {
            if !entry.file_type().unwrap().is_file() {
                continue;
            }
            let file_path = entry.path();
            let file_name = file_path.file_name().unwrap().to_str().unwrap();
            if let Ok(addr) = file_name.replace('_', ":").parse::<SocketAddr>() {
                let ip = addr.ip();
                let port = addr.port();
                if cur_port == port
                    && (cur_ip == ip || cur_ip.is_unspecified() || ip.is_unspecified())
                {
                    let f = try_lock_conflict_addr(&file_path);
                    // The `unlock` is not necessary according to https://man7.org/linux/man-pages/man2/flock.2.html.
                    // But actually, without the `unlock` here, the `try_lock_conflict_addr` after
                    // the loop will occasionally fail.
                    // And the reason is unknown yet.
                    if let Err(e) = f.unlock() {
                        warn!("unlock failed: {:?}", e; "path" => file_path.display())
                    }
                }
            }
        }

        let cur_path = search_base.join(cur_addr.to_string().replace(':', "_"));
        let cur_file = try_lock_conflict_addr(cur_path);
        self.lock_files.push(cur_file);
    }

    fn init_fs(&mut self) {
        let lock_path = self.store_path.join(Path::new("LOCK"));

        let f = File::create(lock_path.as_path())
            .unwrap_or_else(|e| fatal!("failed to create lock at {}: {}", lock_path.display(), e));
        if f.try_lock_exclusive().is_err() {
            fatal!(
                "lock {} failed, maybe another instance is using this directory.",
                self.store_path.display()
            );
        }
        self.lock_files.push(f);

        if tikv_util::panic_mark_file_exists(&self.config.storage.data_dir) {
            fatal!(
                "panic_mark_file {} exists, there must be something wrong with the db. \
                     Do not remove the panic_mark_file and force the TiKV node to restart. \
                     Please contact TiKV maintainers to investigate the issue. \
                     If needed, use scale in and scale out to replace the TiKV node. \
                     https://docs.pingcap.com/tidb/stable/scale-tidb-using-tiup",
                tikv_util::panic_mark_file_path(&self.config.storage.data_dir).display()
            );
        }
    }

    fn init_yatp(&self) {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            yatp::metrics::set_namespace(Some("tikv"));
            prometheus::register(Box::new(yatp::metrics::MULTILEVEL_LEVEL0_CHANCE.clone()))
                .unwrap();
            prometheus::register(Box::new(yatp::metrics::MULTILEVEL_LEVEL_ELAPSED.clone()))
                .unwrap();
        })
    }

    fn init_raftkv(&mut self, store_meta: &StoreMeta) {
        info!("init raftkv");
        let raft_kv = RaftKv::new(LocalReader::new(
            self.raw_engines.kv.clone(),
            store_meta.readers.clone(),
            self.router.clone(),
        ));
        self.raft_kv = Some(raft_kv);
    }

    fn init_servers<F: KvFormat>(
        &mut self,
        store_meta: StoreMeta,
    ) -> Arc<VersionTrack<ServerConfig>> {
        info!("init servers");

        let cfg_controller = self.cfg_controller.as_mut().unwrap();

        cfg_controller.register(
            tikv::config::Module::Quota,
            Box::new(QuotaLimitConfigManager::new(Arc::clone(
                &self.quota_limiter,
            ))),
        );

        let lock_mgr = LockManager::new(&self.config.pessimistic_txn);
        lock_mgr.register_detector_role_change_observer(self.coprocessor_host.as_mut().unwrap());

        let raft_kv = self.raft_kv.as_mut().unwrap();

        let pd_worker = LazyWorker::new("pd-worker");
        let pd_sender = pd_worker.scheduler();
        let flow_reporter = rfstore::store::worker::FlowStatsReporter::new(pd_sender.clone());

        let unified_pool_cfg = &self.config.readpool.unified;
        let unified_read_pool = if unified_pool_cfg.use_tokio {
            build_tokio_pool(
                &self.config.readpool.unified,
                flow_reporter,
                raft_kv.clone(),
            )
        } else {
            build_yatp_read_pool(
                &self.config.readpool.unified,
                flow_reporter.clone(),
                raft_kv.clone(),
            )
        };

        // The `DebugService` and `DiagnosticsService` will share the same thread pool
        let props = tikv_util::thread_group::current_properties();
        let debug_thread_pool = Arc::new(
            Builder::new_multi_thread()
                .thread_name(thd_name!("debugger"))
                .worker_threads(1)
                .after_start_wrapper(move || {
                    tikv_alloc::add_thread_memory_accessor();
                    tikv_util::thread_group::set_properties(props.clone());
                })
                .before_stop_wrapper(tikv_alloc::remove_thread_memory_accessor)
                .build()
                .unwrap(),
        );
        // Start resource metering.
        let (recorder_notifier, collector_reg_handle, resource_tag_factory, recorder_worker) =
            resource_metering::init_recorder(self.config.resource_metering.precision.as_millis());
        self.to_stop.push(recorder_worker);
        let (reporter_notifier, data_sink_reg_handle, reporter_worker) =
            resource_metering::init_reporter(
                self.config.resource_metering.clone(),
                collector_reg_handle,
            );
        self.to_stop.push(reporter_worker);
        let (address_change_notifier, single_target_worker) = resource_metering::init_single_target(
            self.config.resource_metering.receiver_address.clone(),
            self.env.clone(),
            data_sink_reg_handle,
        );
        self.to_stop.push(single_target_worker);

        let cfg_manager = resource_metering::ConfigManager::new(
            self.config.resource_metering.clone(),
            recorder_notifier,
            reporter_notifier,
            address_change_notifier,
        );
        cfg_controller.register(
            tikv::config::Module::ResourceMetering,
            Box::new(cfg_manager),
        );

        let overload_cfg_manager = overload_protector::OverloadConfigManager::new(
            self.overload_protector.clone(),
            self.config.overload.clone(),
        );
        cfg_controller.register(
            tikv::config::Module::Overload,
            Box::new(overload_cfg_manager),
        );

        let resource_control_cfg_manager = resource_control::ConfigManager::new(
            self.resource_controller.publisher(),
            self.config.resource_control.clone(),
        );
        cfg_controller.register(
            tikv::config::Module::ResourceControl,
            Box::new(resource_control_cfg_manager),
        );

        let storage_read_pool_handle = unified_read_pool.handle();
        let reporter = rfstore::store::FlowStatsReporter::new(pd_sender);
        let storage = create_raft_storage::<_, F>(
            raft_kv.clone(),
            &self.config.storage,
            storage_read_pool_handle,
            lock_mgr.clone(),
            self.concurrency_manager.clone(),
            lock_mgr.get_storage_dynamic_configs(),
            self.flow_controller.clone(),
            reporter,
            resource_tag_factory.clone(),
            Arc::clone(&self.quota_limiter),
            self.pd_client.feature_gate().clone(),
            Some(self.region_info_accessor.clone()),
        )
        .unwrap_or_else(|e| fatal!("failed to create raft storage: {}", e));

        ReplicaReadLockChecker::new(self.concurrency_manager.clone())
            .register(self.coprocessor_host.as_mut().unwrap());

        // Create coprocessor endpoint.
        let cop_read_pool_handle = unified_read_pool.handle();

        let server_config = Arc::new(VersionTrack::new(self.config.server.clone()));

        self.config
            .raft_store
            .validate(
                self.config.coprocessor.region_split_size,
                self.config.coprocessor.enable_region_bucket,
                self.config.coprocessor.region_bucket_size,
            )
            .unwrap_or_else(|e| fatal!("failed to validate raftstore config {}", e));
        let raftstore_conf =
            rfstore::store::Config::from_old(&self.config.raft_store, &self.config.coprocessor);
        let raft_store = Arc::new(VersionTrack::new(raftstore_conf));
        let mut node = Node::new(
            self.system.take().unwrap(),
            &server_config.value().clone(),
            raft_store,
            self.pd_client.clone(),
            self.background_worker.clone(),
            tikv_build_version(),
        );
        info!("bootstrap store");
        node.try_bootstrap_store(self.raw_engines.clone())
            .unwrap_or_else(|e| fatal!("failed to bootstrap node id: {}", e));
        info!("store bootstrapped");

        let region_info_accessor = if self.config.server.enable_index_lookup_pushdown {
            Some(self.region_info_accessor.clone())
        } else {
            None
        };
        let mut copr = coprocessor::Endpoint::new(
            &server_config.value(),
            cop_read_pool_handle,
            self.concurrency_manager.clone(),
            region_info_accessor,
            resource_tag_factory,
            Arc::new(QuotaLimiter::default()),
            Some(self.overload_protector.clone()),
            self.security_mgr.clone(),
        );
        copr.set_remote_url(
            self.config.kvengine.remote_worker_addr.clone(),
            self.config.kvengine.remote_coprocessor_addr.clone(),
            self.config.kvengine.remote_coprocessor_min_blocks_size,
            self.config.kvengine.remote_coprocessor_num_ranges,
            self.config
                .kvengine
                .remote_coprocessor_min_process_duration
                .0,
        );
        copr.set_resource_controller(self.resource_controller.clone());
        // Create server
        let server = Server::new(
            node.id(),
            &server_config,
            &self.security_mgr,
            storage,
            copr,
            self.router.clone(),
            self.resolver.clone(),
            self.env.clone(),
            unified_read_pool,
            debug_thread_pool,
        )
        .unwrap_or_else(|e| fatal!("failed to create server: {}", e));

        let import_path = self.store_path.join("import");
        let mut importer = SstImporter::new(
            &self.config.import,
            import_path,
            None,
            self.config.storage.api_version(),
        )
        .unwrap();
        for (cf_name, compression_type) in &[
            (
                CF_DEFAULT,
                self.config.rocksdb.defaultcf.bottommost_level_compression,
            ),
            (
                CF_WRITE,
                self.config.rocksdb.writecf.bottommost_level_compression,
            ),
        ] {
            importer.set_compression_type(cf_name, from_rocks_compression_type(*compression_type));
        }
        let importer = Arc::new(importer);

        node.start(
            self.raw_engines.clone(),
            Box::new(server.transport()),
            Box::new(server.transport_idle()),
            pd_worker,
            store_meta,
            self.coprocessor_host.clone().unwrap(),
            importer.clone(),
            self.concurrency_manager.clone(),
            self.resource_controller.clone(),
        )
        .unwrap_or_else(|e| panic!("failed to start node: {:?}", e));

        initial_metric(&self.config.metric);

        self.servers = Some(Servers {
            lock_mgr,
            server,
            node,
            importer,
        });

        server_config
    }

    fn register_services(&mut self) {
        let servers = self.servers.as_mut().unwrap();
        let raft_kv = self.raft_kv.as_ref().unwrap();

        // Import SST service.
        let (import_service, threads_pool) = ImportSstService::new(
            self.config.import.clone(),
            self.config.raft_store.raft_entry_max_size,
            self.router.clone(),
            servers.importer.clone(),
        );
        if servers
            .server
            .register_service(create_import_sst(import_service))
            .is_some()
        {
            fatal!("failed to register import service");
        }
        self.to_stop
            .push(Box::new(RefCell::new(Some(threads_pool))));

        // Lock manager.
        if servers
            .server
            .register_service(create_deadlock(servers.lock_mgr.deadlock_service()))
            .is_some()
        {
            fatal!("failed to register deadlock service");
        }

        servers
            .lock_mgr
            .start(
                servers.node.id(),
                self.pd_client.clone(),
                self.resolver.clone(),
                self.security_mgr.clone(),
                &self.config.pessimistic_txn,
            )
            .unwrap_or_else(|e| fatal!("failed to start lock manager: {}", e));

        // Backup service.
        let mut backup_worker = Box::new(self.background_worker.lazy_build("backup-endpoint"));
        let backup_scheduler = backup_worker.scheduler();
        let backup_service = backup::Service::new(backup_scheduler);
        if servers
            .server
            .register_service(create_backup(backup_service))
            .is_some()
        {
            fatal!("failed to register backup service");
        }

        let backup_endpoint = backup::Endpoint::new(
            servers.node.id(),
            raft_kv.clone(),
            self.region_info_accessor.clone(),
            self.config.backup.clone(),
            self.concurrency_manager.clone(),
            self.config.storage.api_version(),
        );
        self.cfg_controller.as_mut().unwrap().register(
            tikv::config::Module::Backup,
            Box::new(backup_endpoint.get_config_manager()),
        );
        backup_worker.start(backup_endpoint);

        let diag_service = DiagnosticsService::new(
            servers.server.get_debug_thread_pool().clone(),
            self.config.log.file.filename.clone(),
            self.config.slow_log_file.clone(),
        );
        if servers
            .server
            .register_service(create_diagnostics(diag_service))
            .is_some()
        {
            fatal!("failed to register diagnostics service");
        }
    }

    fn init_io_utility(&mut self) -> BytesFetcher {
        let stats_collector_enabled = file_system::init_io_stats_collector()
            .map_err(|e| warn!("failed to init I/O stats collector: {}", e))
            .is_ok();

        if stats_collector_enabled {
            BytesFetcher::FromIoStatsCollector()
        } else {
            BytesFetcher::FromRateLimiter(self.io_rate_limiter.statistics().unwrap())
        }
    }

    fn init_metrics_flusher(&mut self, fetcher: BytesFetcher) {
        let mut io_metrics = IoMetricsManager::new(fetcher);
        self.background_worker
            .spawn_interval_task(DEFAULT_METRICS_FLUSH_INTERVAL, move || {
                let now = Instant::now();
                io_metrics.flush(now);
            });
    }

    fn run_server(&mut self, server_config: Arc<VersionTrack<ServerConfig>>) {
        let server = self.servers.as_mut().unwrap();
        server
            .server
            .build_and_bind()
            .unwrap_or_else(|e| fatal!("failed to build server: {}", e));
        server
            .server
            .start(server_config, self.security_mgr.clone())
            .unwrap_or_else(|e| fatal!("failed to start server: {}", e));
    }

    fn run_status_server(&mut self) {
        // Create a status server.
        let status_enabled = !self.config.server.status_addr.is_empty();
        if status_enabled {
            let mut status_server = match StatusServer::new(
                self.config.server.status_thread_pool_size,
                self.cfg_controller.take().unwrap(),
                Arc::new(self.config.security.clone()),
                self.router.clone(),
                self.raw_engines.kv.clone(),
                self.raw_engines.raft.clone(),
                self.concurrency_manager.clone(),
                self.pd_client.clone(),
                self.region_info_accessor.clone(),
            ) {
                Ok(status_server) => Box::new(status_server),
                Err(e) => {
                    error_unknown!(%e; "failed to start runtime for status service");
                    return;
                }
            };
            // Start the status server.
            if let Err(e) = status_server.start(self.config.server.status_addr.clone()) {
                error_unknown!(%e; "failed to bind addr for status service");
            } else {
                self.to_stop.push(status_server);
            }
        }
    }

    pub fn stop(self) {
        self.force_stop(false);
    }

    pub fn force_stop(self, force: bool) {
        tikv_util::thread_group::mark_shutdown();
        let mut servers = self.servers.unwrap();
        servers
            .server
            .stop()
            .unwrap_or_else(|e| fatal!("failed to stop server: {}", e));

        servers.node.stop();
        self.region_info_accessor.stop();

        servers.lock_mgr.stop();

        self.to_stop.into_iter().for_each(|s| s.stop());
        self.raw_engines.raft.stop_worker(force);
        self.raw_engines.raft.close_writer();
        self.overload_protector.stop();
        self.resource_controller.stop();
        self.background_worker.stop();
        self.raw_engines.kv.close();

        for f in self.lock_files {
            if let Err(e) = f.unlock() {
                warn!("unlock failed: {:?}", e);
            }
        }
    }

    pub fn get_kv_engine(&self) -> kvengine::Engine {
        self.raw_engines.kv.clone()
    }

    pub fn get_raft_engine(&self) -> rfengine::RfEngine {
        self.raw_engines.raft.clone()
    }

    pub fn get_region_info_accessor(&self) -> RegionInfoAccessor {
        self.region_info_accessor.clone()
    }

    pub fn get_store_id(&self) -> u64 {
        self.servers.as_ref().unwrap().node.id()
    }

    pub fn get_sst_importer(&self) -> Arc<SstImporter> {
        self.servers.as_ref().unwrap().importer.clone()
    }

    pub fn get_raft_router(&self) -> RaftRouter {
        self.router.clone()
    }
}

impl TikvServer {
    // This method is also used for restore.
    // NOTE: Pass `Some(dfs)` for usage of TiKV servers ONLY. Otherwise, it would
    // corrupt the WAL chunks in DFS.
    pub fn init_raft_engine(
        conf: &TikvConfig,
        dfs: Option<Arc<dyn Dfs>>,
    ) -> rfengine::Result<RfEngine> {
        let raft_db_path = Path::new(&conf.raft_store.raftdb_path);
        let data_dir = Path::new(&conf.storage.data_dir);
        RfEngine::open(raft_db_path, &conf.rfengine, Some(data_dir), dfs)
    }

    pub fn kv_engine_path(conf: &TikvConfig) -> PathBuf {
        PathBuf::from(conf.storage.data_dir.clone()).join(Path::new("db"))
    }

    // This method is also used for restore.
    pub fn init_kv_engine(
        pd: Arc<dyn PdClient>,
        conf: &TikvConfig,
        dfs: Arc<dyn Dfs>,
        rate_limiter: Arc<IoRateLimiter>,
        store_limiter: Arc<StoreLimiter>,
        meta_iter: &mut impl kvengine::MetaIterator,
        recoverer: impl kvengine::RecoverHandler + 'static,
        for_restore: bool,
        master_key: MasterKey,
        security_mgr: Arc<SecurityManager>,
    ) -> kvengine::Result<(
        kvengine::Engine,
        mpsc::Sender<StoreMsg>,
        mpsc::Receiver<StoreMsg>,
    )> {
        let kv_engine_path = Self::kv_engine_path(conf);
        let mut kv_opts = kvengine::Options::default();
        let total_mem = SysQuota::memory_limit_in_bytes();
        let capacity = match conf.storage.block_cache.capacity {
            None => ((total_mem as f64) * tikv::config::BLOCK_CACHE_RATE) as usize,
            Some(c) => c.0 as usize,
        };
        kv_opts.local_dirs = vec![kv_engine_path];
        for dir in &conf.kvengine.extra_dirs {
            kv_opts.local_dirs.push(PathBuf::from(dir));
        }
        for local_dir in &kv_opts.local_dirs {
            if !local_dir.exists() {
                fs::create_dir_all(local_dir).unwrap();
            }
            if !local_dir.is_dir() {
                panic!("path {:?} is not dir", local_dir);
            }
        }
        kv_opts.num_compactors = conf.rocksdb.max_background_jobs as usize;
        kv_opts.max_mem_table_size = conf.rocksdb.writecf.write_buffer_size.0;
        if kv_opts.max_mem_table_size > kvengine::KV_ENGINE_MEM_TABLE_MAX_SIZE {
            fatal!(
                "max_mem_table_size {} is too large, should be no more than {}",
                kv_opts.max_mem_table_size,
                kvengine::KV_ENGINE_MEM_TABLE_MAX_SIZE
            );
        }
        // base_size affects compaction priority a lot, we should cap it to a smaller
        // size when we increase the region_split_size.
        kv_opts.base_size = (conf.coprocessor.region_split_size.0 / 16).min(32 * 1024 * 1024);
        kv_opts.max_block_cache_size = capacity as i64;
        kv_opts.remote_compactor_addr = conf.dfs.remote_compactor_addr.clone();
        kv_opts.enable_safe_point_v2 = conf.gc.enable_safe_point_v2;
        kv_opts.disable_safe_point_fallback_v1 = conf.gc.disable_safe_point_fallback_v1;
        let cf_opt = &conf.rocksdb.writecf;
        kv_opts.table_builder_options.block_size = cf_opt.block_size.0 as usize;
        kv_opts.table_builder_options.max_table_size = cf_opt.target_file_size_base.0 as usize;
        kv_opts.table_builder_options.compression_lvl =
            conf.dfs.zstd_compression_level.parse().unwrap_or_else(|_| {
                fatal!(
                    "invalid zstd compression level: {}",
                    conf.dfs.zstd_compression_level
                )
            });
        kv_opts.table_builder_options.flush_split_l0 = conf.kvengine.flush_split_l0;
        kv_opts.blob_table_build_options = conf.kvengine.blob_table_build_options;
        kv_opts.allow_fallback_local = conf.dfs.allow_fallback_local;
        kv_opts.max_del_range_delay = conf.kvengine.max_del_range_delay.into();
        kv_opts.compaction_request_version = conf.kvengine.compaction_request_version;
        kv_opts.compaction_tombs_ratio = conf.kvengine.compaction_tombs_ratio;
        kv_opts.compaction_tombs_count = conf.kvengine.compaction_tombs_count;
        kv_opts.for_restore = for_restore;

        kv_opts.dfs_load_concurrency_per_request = conf.kvengine.dfs_load_concurrency_per_request;

        let flow_control = &conf.storage.flow_control;
        kv_opts.flow_control.enable = flow_control.enable;
        kv_opts.flow_control.soft_region_mem_limit = flow_control.soft_region_mem_limit.0;
        kv_opts.flow_control.hard_region_mem_limit = flow_control.hard_region_mem_limit.0;
        kv_opts.flow_control.soft_region_l0table_size_limit = flow_control
            .soft_region_l0table_size_limit
            .unwrap_or(flow_control.soft_region_mem_limit)
            .0;
        kv_opts.flow_control.hard_region_l0table_size_limit = flow_control
            .hard_region_l0table_size_limit
            .unwrap_or(flow_control.hard_region_mem_limit)
            .0;
        kv_opts.flow_control.max_region_speed_limit = flow_control.max_region_speed_limit.0;
        kv_opts.flow_control.min_region_speed_limit = flow_control.min_region_speed_limit.0;

        kv_opts.ia = conf.kvengine.ia.clone();
        kv_opts.ia.dynamic_capacity = true; // Always enable dynamic capacity.

        kv_opts.txn_file_worker_pool_size =
            conf.kvengine.txn_file_worker_pool_size.unwrap_or_else(|| {
                // 32GB -> 16, 16GB -> 8
                let size = (total_mem >> 30) as usize / 2;
                size.clamp(2, 64)
            });

        kv_opts.ignore_columnar_table_load = conf.kvengine.ignore_columnar_table_load;
        kv_opts.set_build_columnar(conf.kvengine.build_columnar);
        kv_opts.set_build_fts_index(conf.kvengine.build_fts_index);
        kv_opts.read_columnar = conf.kvengine.read_columnar;
        kv_opts.gc_lock_extra_cf = conf.kvengine.gc_lock_extra_cf;
        kv_opts.columnar_build_options = conf.kvengine.columnar_table_build_options;
        kv_opts.vector_index_build_options = conf.kvengine.vector_index_build_options;
        kv_opts.fts_build_options = conf.kvengine.fts_build_options;

        kv_opts.low_space_threshold = conf
            .storage
            .low_space_threshold
            .as_disks_size(&kv_opts.local_dirs)
            .ctx("get_disks_stats")?;

        let opts = Arc::new(kv_opts);
        let id_allocator = Arc::new(PdIdAllocator::new(pd.clone()));

        let (sender, receiver) = tikv_util::mpsc::unbounded();
        let meta_change_listener = Box::new(MetaChangeListener {
            sender: sender.clone(),
        });

        let mut opt_ks_gc_sp_cache = None;
        if conf.gc.enable_safe_point_v2 {
            opt_ks_gc_sp_cache = Some(pd.get_keyspace_gc_safepoint_v2_cache());
        }
        let kv_engine = kvengine::Engine::open(
            dfs,
            opts,
            conf.kvengine.clone(),
            meta_iter,
            recoverer,
            id_allocator,
            meta_change_listener,
            rate_limiter,
            store_limiter,
            opt_ks_gc_sp_cache,
            master_key,
            security_mgr,
            true,
        )?;
        Ok((kv_engine, sender, receiver))
    }

    fn init_raw_engines(
        pd: Arc<dyn PdClient>,
        conf: &TikvConfig,
        dfs: Arc<dyn Dfs>,
        rate_limiter: Arc<IoRateLimiter>,
        store_limiter: Arc<StoreLimiter>,
        master_key: MasterKey,
        security_mgr: Arc<SecurityManager>,
    ) -> Engines {
        Self::check_disk_capacity_on_k8s(&conf.storage.data_dir);
        let panic_regions = Self::load_panic_regions(&conf.storage.data_dir);
        let panic_region_ids = panic_regions.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let black_list_regions = panic_regions
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
        let rf_engine = Self::init_raft_engine(conf, Some(dfs.clone())).unwrap();
        let (black_list_tables, black_list_keyspaces) = Self::escalate_blacklist_level(
            &rf_engine,
            &panic_region_ids,
            conf.kvengine.table_auto_blacklist_threshold,
            conf.kvengine.keyspace_auto_blacklist_threshold,
        );
        warn!(
            "auto blacklisted table_ids: {:?}, keyspace_ids: {:?}, by black_list_regons: {:?}",
            black_list_tables, black_list_keyspaces, black_list_regions
        );
        let recoverer = rfstore::store::RecoverHandler::new(rf_engine.clone());
        let mut meta_iter = recoverer.clone();
        if let Some(mut black_list) = load_black_list(conf) {
            black_list.add_regions(black_list_regions);
            black_list.add_tables(black_list_tables);
            black_list.add_keyspaces(black_list_keyspaces);
            meta_iter.set_black_list(black_list);
        } else if !black_list_regions.is_empty() {
            let black_list =
                BlackList::new(black_list_keyspaces, black_list_tables, black_list_regions);
            meta_iter.set_black_list(black_list);
        }
        // TODO: This feature is risky when multiple stores are shut down improperly
        // if let Some(region_ids) = get_store_regions(pd.clone(), conf,
        // rf_engine.clone()) {     meta_iter.
        // set_contained_region_ids(region_ids); }
        let (kv_engine, sender, receiver) = Self::init_kv_engine(
            pd,
            conf,
            dfs,
            rate_limiter,
            store_limiter,
            &mut meta_iter,
            recoverer,
            false,
            master_key,
            security_mgr,
        )
        .unwrap();
        Engines::new(
            kv_engine,
            rf_engine,
            (sender, receiver),
            meta_iter.take_black_list(),
        )
    }

    // check the system disk capacity on k8s.
    // It is used to avoid the situation that the local disk is not mounted before
    // pod start.
    fn check_disk_capacity_on_k8s(data_dir: &str) {
        if env::var(ENV_K8S_HOST).is_err() {
            // It is not running on K8S, skip disk capacity check.
            return;
        }
        let data_path = PathBuf::from(data_dir);
        match fs2::statvfs(&data_path) {
            Ok(stats) => {
                let cap = stats.total_space();
                info!("capacity of data dir: {}, available: {}", cap, stats.available_space();
                    "path" => data_path.display());
                if cap < K8S_MIN_DISK_CAPACITY {
                    fatal!(
                        "insufficient disk space for k8s deploy, at least {} bytes required, but got {} bytes",
                        K8S_MIN_DISK_CAPACITY,
                        cap
                    );
                }
            }
            Err(err) => {
                fatal!(
                    "Unable to get disk capacity for data directory: {}: {:?}",
                    data_dir,
                    err
                );
            }
        }
    }

    fn load_panic_regions<P: AsRef<Path>>(data_dir: P) -> Vec<(u64, usize)> {
        let dir = fs::read_dir(data_dir).unwrap();
        let mut panic_regions = vec![];
        for entry in dir.into_iter().flatten() {
            let file_name = entry.file_name().into_string().unwrap_or_default();
            if let Some(region_id_str) = file_name.strip_prefix(PANIC_REGION_FILE_PREFIX) {
                if let Ok(region_id) = region_id_str.parse::<u64>() {
                    let count = get_panic_region_count(entry.path()) as usize;
                    panic_regions.push((region_id, count));
                }
            }
        }
        panic_regions
    }

    fn escalate_blacklist_level(
        rf_engine: &RfEngine,
        panic_regions: &[u64],
        table_auto_blacklist_threshold: u64,
        keyspace_auto_blacklist_threshold: u64,
    ) -> (
        Vec<(u32, i64)>, // blacklisted table ids
        Vec<u32>,        // blacklisted keyspace ids
    ) {
        let region_to_peers = rf_engine.get_region_peer_map();
        // table_ids blacklisted count by region.
        let mut table_ids = HashMap::new();
        for region_id in panic_regions {
            if let Some(peer_id) = region_to_peers.get(region_id) {
                if let Some(cs) = load_raft_engine_meta(rf_engine, *peer_id) {
                    if !cs.has_snapshot() {
                        continue;
                    }
                    if let Some((keyspace_id, table_id)) = api_version::ApiV2::get_keyspace_table_id(
                        cs.get_snapshot().get_outer_start(),
                    ) {
                        table_ids
                            .entry((keyspace_id, table_id))
                            .or_insert(0)
                            .add_assign(1);
                    }
                }
            }
        }
        // keyspace_ids blacklisted count by table.
        let mut keyspace_ids = HashMap::new();
        let blacklisted_table_ids = table_ids
            .iter()
            .filter(|(_, &count)| count >= table_auto_blacklist_threshold)
            .map(|((keyspace_id, table_id), _)| (*keyspace_id, *table_id))
            .collect::<Vec<_>>();
        for (keyspace_id, _) in &blacklisted_table_ids {
            keyspace_ids.entry(*keyspace_id).or_insert(0).add_assign(1);
        }
        let blacklisted_keyspace_ids = keyspace_ids
            .iter()
            .filter(|(_, &count)| count >= keyspace_auto_blacklist_threshold)
            .map(|(keyspace_id, _)| *keyspace_id)
            .collect::<Vec<_>>();
        (blacklisted_table_ids, blacklisted_keyspace_ids)
    }

    fn init_flow_control(config: &TikvConfig) -> (FlowController, Arc<StoreLimiter>) {
        let soft_limit = config
            .storage
            .flow_control
            .soft_store_mem_limit
            .as_ref()
            .unwrap()
            .0;
        let hard_limit = config
            .storage
            .flow_control
            .hard_store_mem_limit
            .as_ref()
            .unwrap()
            .0;
        let interval_ms = config
            .raft_store
            .pd_store_heartbeat_tick_interval
            .as_millis();

        // Memory usage of store is reported on every
        // `pd_store_heartbeat_tick_interval`, so it's safe to set `max_speed_limit` to
        // `hard_limit / interval`, that even if there is no flush in next period, the
        // memory usage will not exceed `hard_limit`.
        let max_speed_limit = hard_limit * 1_000 / interval_ms;

        let options = LimiterOptions {
            enable: config.storage.flow_control.enable,
            soft_limit,
            hard_limit,
            max_speed_limit: max_speed_limit.max(CLOUD_MIN_THROTTLE_SPEED),
            min_speed_limit: CLOUD_MIN_THROTTLE_SPEED,
        };
        info!("init_flow_control"; "options" => ?options);
        let limiter = Arc::new(StoreLimiter::new_with_metric(
            options,
            SCHED_WRITE_FLOW_GAUGE.clone(),
        ));
        let flow_controller = FlowController::Cloud(limiter.clone());
        (flow_controller, limiter)
    }

    fn init_resource_controller(config: &TikvConfig) -> ResourceController {
        let mut resource_controller = ResourceController::new(config.resource_control.clone());

        // Read
        let read_limiter = ReadLimiter::new(config.resource_control.clone());
        resource_controller.set_read_limiter(read_limiter.clone());
        let read_subscriber = ReadSubscriber::new(read_limiter, config.resource_control.clone());
        resource_controller.register_subscriber(
            ResourceType::Cpu {
                cpu_type: CpuType::UnifiedReadPool,
            },
            Box::new(read_subscriber.clone()),
        );
        resource_controller
            .register_subscriber(ResourceType::Read, Box::new(read_subscriber.clone()));

        // Transfer Leader
        let transfer_leader_limiter = TransferLeaderLimiter::new(config.resource_control.clone());
        resource_controller.set_transfer_leader_limiter(transfer_leader_limiter.clone());
        let transfer_leader_subscriber =
            TransferLeaderSubscriber::new(transfer_leader_limiter, config.resource_control.clone());
        resource_controller.register_subscriber(
            ResourceType::TransferLeader,
            Box::new(transfer_leader_subscriber.clone()),
        );

        resource_controller
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
struct BlackListConfig {
    keyspace_ids: Vec<u32>,
    table_ids: Vec<(u32 /* keyspace_id */, i64 /* table_id */)>,
    region_ids: Vec<u64>,
}

fn load_black_list(conf: &TikvConfig) -> Option<BlackList> {
    if conf.recovery_mode {
        recovery::set_recovery_mode(true);
    }
    let recovery_black_list = recovery::load_black_list(&conf.storage.data_dir);
    if conf.black_list_path.is_empty() && recovery_black_list.is_empty() {
        return None;
    }
    let mut black_list_conf = BlackListConfig::default();
    if let Ok(data) = fs::read(&conf.black_list_path) {
        match serde_json::from_slice::<BlackListConfig>(&data) {
            Ok(config) => {
                black_list_conf = config;
            }
            Err(err) => {
                error!("failed to load black list file {:?}", err);
            }
        }
    }
    black_list_conf
        .keyspace_ids
        .extend_from_slice(&recovery_black_list);

    // Load additional keyspace list from blacklist file.
    black_list_conf
        .keyspace_ids
        .extend(load_keyspace_list(conf, BLACKLIST_KEYSPACE_FILE));
    black_list_conf.keyspace_ids.sort();
    black_list_conf.keyspace_ids.dedup();
    // Load additional keyspace list from whitelist file, and remove the keyspace
    // ids in blacklist.
    // NOTE: The keyspace ids in whitelist has higher priority than the keyspace ids
    // in blacklist.
    let whitelist_keyspace_ids = load_keyspace_list(conf, WHITELIST_KEYSPACE_FILE);
    black_list_conf
        .keyspace_ids
        .retain(|id| !whitelist_keyspace_ids.contains(id));

    info!(
        "load_black_list, keyspace_id count: {}, region_id count: {}, keyspace_ids: {:?} region_ids: {:?}",
        black_list_conf.keyspace_ids.len(),
        black_list_conf.region_ids.len(),
        black_list_conf.keyspace_ids,
        black_list_conf.region_ids
    );
    Some(BlackList::new(
        black_list_conf.keyspace_ids,
        black_list_conf.table_ids,
        black_list_conf.region_ids,
    ))
}

fn load_keyspace_list(conf: &TikvConfig, file_name: &str) -> Vec<u32> {
    let path = PathBuf::from(&conf.storage.data_dir).join(file_name);
    if !path.exists() {
        return vec![];
    }
    if let Ok(data) = fs::read(&path) {
        return match serde_json::from_slice::<Vec<u32>>(&data) {
            Ok(keyspace_ids) => keyspace_ids,
            Err(err) => {
                error!(
                    "failed to load additional keyspace list file {:?} {:?}",
                    path.display(),
                    err
                );
                vec![]
            }
        };
    }
    vec![]
}

fn _get_store_regions(
    pd: Arc<dyn pd_client::PdClient>,
    conf: &TikvConfig,
    rf_engine: RfEngine,
) -> Option<Vec<u64>> {
    if !pd.is_cluster_bootstrapped().unwrap() {
        return None;
    };
    let store_id = match rf_engine.get_state(0, STORE_IDENT_KEY) {
        None => 0,
        Some(bin) => {
            let mut ident = StoreIdent::default();
            ident.merge_from_bytes(&bin).unwrap();
            ident.get_store_id()
        }
    };
    if store_id == INVALID_ID {
        return None;
    }
    info!("store_id: {}", store_id);
    let elapsed_secs = match pd.get_store(store_id) {
        Ok(store) => {
            let last_heartbeat = store.get_last_heartbeat();
            if last_heartbeat > 0 {
                let last_heartbeat_dur = Duration::from_nanos(last_heartbeat as u64);
                chrono::Local::now().timestamp() as u64 - last_heartbeat_dur.as_secs()
            } else {
                return None;
            }
        }
        Err(e) => {
            warn!("failed to get store: {}", e);
            return None;
        }
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pd_control = PdControl::new(conf.pd.clone(), pd.get_security_mgr()).unwrap();
    let max_store_down_time_secs = match rt.block_on(pd_control.get_config()) {
        Ok(pd_config) => {
            if !pd_config.schedule.max_store_down_time.is_empty() {
                ReadableDuration::from_str(pd_config.schedule.max_store_down_time.as_str())
                    .unwrap()
                    .as_secs()
            } else {
                return None;
            }
        }
        Err(e) => {
            warn!("failed to get config: {}", e);
            return None;
        }
    };
    info!(
        "elapsed secs: {}, max store down time secs: {}",
        elapsed_secs, max_store_down_time_secs
    );
    if max_store_down_time_secs > 0 && elapsed_secs > max_store_down_time_secs {
        match rt.block_on(pd_control.get_store_regions(store_id)) {
            Ok(regions_info) => {
                info!("get store regions: {}", regions_info.regions.len());
                let region_ids = regions_info
                    .regions
                    .into_iter()
                    .map(|region| region.id)
                    .collect();
                return Some(region_ids);
            }
            Err(e) => {
                warn!("failed to get store regions: {}", e);
            }
        };
    }
    None
}

/// Various sanity-checks and logging before running a server.
///
/// Warnings are logged.
///
/// # Logs
///
/// The presence of these environment variables that affect the database
/// behavior is logged.
///
/// - `GRPC_POLL_STRATEGY`
/// - `http_proxy` and `https_proxy`
///
/// # Warnings
///
/// - if `net.core.somaxconn` < 32768
/// - if `net.ipv4.tcp_syncookies` is not 0
/// - if `vm.swappiness` is not 0
/// - if data directories are not on SSDs
/// - if the "TZ" environment variable is not set on unix
fn pre_start() {
    check_environment_variables();
    for e in tikv_util::config::check_kernel() {
        warn!(
            "check: kernel";
            "err" => %e
        );
    }
}

fn check_system_config(config: &TikvConfig) {
    info!("beginning system configuration check");
    let mut rocksdb_max_open_files = config.rocksdb.max_open_files;
    if config.rocksdb.titan.enabled {
        // Titan engine maintains yet another pool of blob files and uses the same max
        // number of open files setup as rocksdb does. So we double the max required
        // open files here
        rocksdb_max_open_files *= 2;
    }
    if let Err(e) = tikv_util::config::check_max_open_fds(
        RESERVED_OPEN_FDS + (rocksdb_max_open_files + config.raftdb.max_open_files) as u64,
    ) {
        fatal!("{}", e);
    }

    // Check RocksDB data dir
    if let Err(e) = tikv_util::config::check_data_dir(&config.storage.data_dir) {
        warn!(
            "check: rocksdb-data-dir";
            "path" => &config.storage.data_dir,
            "err" => %e
        );
    }
    // Check raft data dir
    if let Err(e) = tikv_util::config::check_data_dir(&config.raft_store.raftdb_path) {
        warn!(
            "check: raftdb-path";
            "path" => &config.raft_store.raftdb_path,
            "err" => %e
        );
    }
}

fn try_lock_conflict_addr<P: AsRef<Path>>(path: P) -> File {
    let f = File::create(path.as_ref()).unwrap_or_else(|e| {
        fatal!(
            "failed to create lock at {}: {}",
            path.as_ref().display(),
            e
        )
    });

    if f.try_lock_exclusive().is_err() {
        fatal!(
            "{} already in use, maybe another instance is binding with this address.",
            path.as_ref().file_name().unwrap().to_str().unwrap()
        );
    }
    f
}

#[cfg(unix)]
fn get_lock_dir() -> String {
    format!("{}_TIKV_LOCK_FILES", unsafe { libc::getuid() })
}

#[cfg(not(unix))]
fn get_lock_dir() -> String {
    "TIKV_LOCK_FILES".to_owned()
}

/// A small trait for components which can be trivially stopped. Lets us keep
/// a list of these in `TiKV`, rather than storing each component individually.
trait Stop {
    fn stop(self: Box<Self>);
}

impl Stop for StatusServer {
    fn stop(self: Box<Self>) {
        (*self).stop()
    }
}

impl Stop for Worker {
    fn stop(self: Box<Self>) {
        Worker::stop(&self);
    }
}

impl<T: fmt::Display + Send + 'static> Stop for LazyWorker<T> {
    fn stop(self: Box<Self>) {
        self.stop_worker();
    }
}

const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

impl Stop for RefCell<Option<tokio::runtime::Runtime>> {
    fn stop(self: Box<Self>) {
        self.borrow_mut()
            .take()
            .unwrap()
            .shutdown_timeout(RUNTIME_SHUTDOWN_TIMEOUT);
    }
}
