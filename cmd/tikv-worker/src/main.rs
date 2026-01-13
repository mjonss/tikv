// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
mod metrics;

use std::{
    io,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
};

use clap::{App, Arg, ArgMatches};
use cloud_worker::Config;
use grpcio::EnvBuilder;
use pd_client::RpcClient;
use security::SecurityManager;
use slog::Level;
use slog_global::{error, info};
use tikv_util::{config, config::ReadableDuration, sys::SysQuota};

use crate::metrics::CPU_CORES_QUOTA_GAUGE;

const ZSTD_COMPRESSION_LEVEL_FOR_REMOTE: &str = "5";
const DEFAULT_LOG_LEVEL: Level = Level::Info;

static VERSION_INFO: LazyLock<String> = LazyLock::new(|| {
    let build_timestamp = option_env!("TIKV_BUILD_TIME");
    tikv::tikv_version_info(build_timestamp)
});

fn main() {
    init_logger(io::stdout(), DEFAULT_LOG_LEVEL);
    tikv_util::metrics::monitor_process()
        .unwrap_or_else(|e| panic!("failed to start process monitor: {}", e));
    tikv_util::set_panic_hook(false, "/tmp");
    CPU_CORES_QUOTA_GAUGE.set(SysQuota::cpu_cores_quota());
    let matches = App::new("tikv-worker")
        .about("tikv remote worker")
        .version(&**VERSION_INFO)
        .arg(
            Arg::with_name("config")
                .short("C")
                .long("config")
                .value_name("FILE")
                .help("Set the configuration file")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("addr")
                .short("A")
                .long("addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the listening address"),
        )
        .arg(
            Arg::with_name("log-file")
                .short("f")
                .long("log-file")
                .value_name("LOGFILE")
                .help("Sets log file")
                .long_help("Set the log file path. If not set, logs will output to stderr"),
        )
        .arg(
            Arg::with_name("log-level")
                .short("L")
                .long("log-level")
                .value_name("LOGLEVEL")
                .help("Sets log level")
                .long_help("Set the log level [debug,info,warn,error]"),
        )
        .arg(
            Arg::with_name("pd-endpoints")
                .long("pd-endpoints")
                .takes_value(true)
                .value_name("PD_URL")
                .multiple(true)
                .use_delimiter(true)
                .require_delimiter(true)
                .value_delimiter(",")
                .help("Sets PD endpoints")
                .long_help("Set the PD endpoints to use. Use `,` to separate multiple PDs"),
        )
        .arg(
            Arg::with_name("cacert")
                .long("cacert")
                .takes_value(true)
                .value_name("CERT")
                .help("Path of file that contains list of trusted SSL CAs"),
        )
        .arg(
            Arg::with_name("cert")
                .long("cert")
                .takes_value(true)
                .value_name("CERT")
                .help("Path of file that contains X509 certificate in PEM format"),
        )
        .arg(
            Arg::with_name("key")
                .long("key")
                .takes_value(true)
                .value_name("KEY")
                .help("Path of file that contains X509 key in PEM format"),
        )
        .arg(
            Arg::with_name("update-interval")
                .long("update-interval")
                .takes_value(true)
                .value_name("INTERVAL")
                .help("Sets registration update interval"),
        )
        .arg(
            Arg::with_name("register")
                .long("register")
                .takes_value(true)
                .value_name("Bool")
                .help("register compactor to stores"),
        )
        .arg(
            Arg::with_name("data-dir")
                .long("data-dir")
                .takes_value(true)
                .value_name("DIR")
                .help("data dir for load_data"),
        )
        .arg(
            Arg::with_name("cop-addr")
                .long("cop-addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the coprocessor listening address"),
        )
        .arg(
            Arg::with_name("run-worker-scaler")
                .long("run-worker-scaler")
                .takes_value(true)
                .value_name("Bool")
                .help("run worker-scaler"),
        )
        .arg(
            Arg::with_name("report-wru")
                .long("report-wru")
                .takes_value(true)
                .value_name("Bool")
                .help("report wru"),
        )
        .arg(
            Arg::with_name("enable-load-data-check-point")
                .long("enable-load-data-check-point")
                .takes_value(true)
                .value_name("Bool")
                .help("enable load data check point"),
        )
        .arg(
            Arg::with_name("enable-load-data-multi-threads")
                .long("enable-load-data-multi-threads")
                .takes_value(true)
                .value_name("Bool")
                .help("enable load data multi threads"),
        )
        .arg(
            Arg::with_name("push-metrics-addr")
                .long("push-metrics-addr")
                .value_name("ADDR")
                .help("Sets Prometheus Pushgateway address")
                .long_help(
                    "Sets push address to the Prometheus Pushgateway, \
                     leaves it empty will disable Prometheus push",
                ),
        )
        .arg(
            Arg::with_name("push-metrics-interval")
                .long("push-metrics-interval")
                .hidden(false)
                .help("Sets Prometheus Pushgateway push interval, in seconds")
                .default_value("30")
                .long_help(
                    "Sets push interval to the Prometheus Pushgateway, \
                     default is 30(s)",
                ),
        )
        .get_matches();

    let mut config_file_path = None;
    let mut config: Config = match matches.value_of_os("config") {
        Some(config_path) => {
            let path = PathBuf::from(config_path);
            info!("config path:{:?}", config_path);
            config_file_path = Some(path.clone());
            let result = std::fs::read(path);
            if result.is_err() {
                error!("failed to read config file {:?}", result.unwrap_err());
                return;
            }
            let data = result.unwrap();
            toml::from_slice(&data).unwrap()
        }
        None => Config::default(),
    };

    override_from_args(&mut config, &matches);
    // Convert to absolute path. Get disk capacity depends on this.
    config.data_dir = config::canonicalize_path(&config.data_dir)
        .unwrap_or_else(|e| panic!("failed to canonicalize data dir: {:?}", e));
    let log_level =
        tikv_util::logger::get_level_by_string(&config.log_level).unwrap_or(DEFAULT_LOG_LEVEL);
    if !config.log_file.is_empty() {
        let log = tikv_util::logger::file_writer(&config.log_file, 300, 0, 0, rename_by_timestamp)
            .unwrap();
        init_logger(log, log_level);
    } else if log_level != DEFAULT_LOG_LEVEL {
        init_logger(io::stdout(), log_level);
    }
    config.dfs.override_from_env();
    config.security.override_from_env();
    // If zstd_compression_level is not set, set it to default value
    if config.dfs.zstd_compression_level.is_empty() {
        config.dfs.zstd_compression_level = ZSTD_COMPRESSION_LEVEL_FOR_REMOTE.to_string();
    }
    config.validate().unwrap();

    let security_mgr = Arc::new(
        SecurityManager::new(&config.security)
            .unwrap_or_else(|e| panic!("failed to create security manager: {:?}", e)),
    );
    let env = Arc::new(EnvBuilder::new().cq_count(1).build());
    let pd = Arc::new(
        RpcClient::new(&config.pd, Some(env), security_mgr)
            .unwrap_or_else(|e| panic!("failed to create rpc client: {:?}", e)),
    );

    let build_timestamp = option_env!("TIKV_BUILD_TIME");
    tikv::log_tikv_info("TiKV worker", build_timestamp);

    info!("config is {:?}", &config);
    config.memory.init();
    cloud_worker::run_cloud_worker(config, config_file_path, pd);
    info!("TiKV worker exit");
}

fn init_logger<W: 'static + io::Write + Send>(writer: W, level: Level) {
    use slog::Drain;
    let decorator = slog_term::PlainDecorator::new(writer);
    let drain = slog_term::CompactFormat::new(decorator).build();
    let drain = std::sync::Mutex::new(drain).filter_level(level).fuse();
    let logger = slog::Logger::root(drain, slog::o!());
    slog_global::set_global(logger);
}

fn rename_by_timestamp(path: &Path) -> io::Result<PathBuf> {
    let mut new_path = path.parent().unwrap().to_path_buf();
    let mut new_fname = path.file_stem().unwrap().to_os_string();
    let dt = chrono::Local::now().format("%Y-%m-%dT%H-%M-%S%.3f");
    new_fname.push(format!("-{}", dt));
    if let Some(ext) = path.extension() {
        new_fname.push(".");
        new_fname.push(ext);
    };
    new_path.push(new_fname);
    Ok(new_path)
}

fn override_from_args(config: &mut Config, matches: &ArgMatches<'_>) {
    if let Some(file) = matches.value_of("log-file") {
        config.log_file = file.to_owned();
    }

    if let Some(addr) = matches.value_of("addr") {
        config.addr = addr.to_owned();
    }

    if let Some(endpoints) = matches.values_of("pd-endpoints") {
        config.pd.endpoints = endpoints.map(ToOwned::to_owned).collect();
    }

    if let Some(cacert) = matches.value_of("cacert") {
        config.security.ca_path = cacert.to_owned();
    }

    if let Some(cert) = matches.value_of("cert") {
        config.security.cert_path = cert.to_owned();
    }

    if let Some(key) = matches.value_of("key") {
        config.security.key_path = key.to_owned();
    }

    if let Some(interval) = matches.value_of("interval") {
        config.update_interval = ReadableDuration::secs(interval.parse().unwrap());
    }

    if let Some(register) = matches.value_of("register") {
        config.register = register == "true";
    }

    if let Some(dir) = matches.value_of("data-dir") {
        config.data_dir = dir.to_string();
    }

    if let Some(log_file) = matches.value_of("log-file") {
        config.log_file = log_file.to_string();
    }

    if let Some(log_level) = matches.value_of("log-level") {
        config.log_level = log_level.to_string();
    }

    if let Some(cop_addr) = matches.value_of("cop-addr") {
        config.cop_addr = cop_addr.to_string();
    }

    if let Some(run_worker_scaler) = matches.value_of("run-worker-scaler") {
        config.worker_scaler.run = run_worker_scaler == "true";
    }

    if let Some(report_wru) = matches.value_of("report-wru") {
        config.report_wru = report_wru == "true";
    }

    if let Some(enable_check_point) = matches.value_of("enable-load-data-check-point") {
        config.enable_load_data_check_point = enable_check_point == "true";
    }

    if let Some(push_metrics_addr) = matches.value_of("push-metrics-addr") {
        config.push_metrics_addr = push_metrics_addr.to_string();
    }

    if let Some(push_metrics_interval) = matches.value_of("push-metrics-interval") {
        config.push_metrics_interval =
            ReadableDuration::secs(push_metrics_interval.parse().unwrap());
    }
}
