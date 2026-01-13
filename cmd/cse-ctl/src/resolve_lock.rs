// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;

use clap::Args;
use log_wrappers::Value as LogValue;
use native_br::common::create_pd_client;
use pd_client::{PdClient, util::RegionLike};
use security::SecurityConfig;
use tikv_client::{
    Timestamp, TimestampExt,
    transaction::{Client as TiKVClient, ResolveLocksOptions},
};
use tikv_util::{codec::bytes::decode_bytes, config::ReadableDuration};

#[derive(Args)]
pub struct ResolveLockArgs {
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
    #[clap(long)]
    pub region_id: u64,
    #[clap(long)]
    pub dry_run: bool,
    #[clap(long, default_value = "60m")]
    pub safepoint_lifetime: ReadableDuration,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ResolveLockConfig {
    pub pd: pd_client::Config,
    pub security: SecurityConfig,
}

pub fn execute_resolve_lock(args: ResolveLockArgs) {
    let config = get_resolve_lock_config_from_args(&args);
    let pd_client = create_pd_client(&config.security, &config.pd);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime
        .block_on(resolve_lock_for_region(
            &config,
            &pd_client,
            args.region_id,
            args.safepoint_lifetime,
            args.dry_run,
        ))
        .unwrap_or_else(|e| {
            panic!("resolve lock failed: {:?}", e);
        });
}

async fn resolve_lock_for_region(
    config: &ResolveLockConfig,
    pd_client: &dyn PdClient,
    region_id: u64,
    safepoint_lifetime: ReadableDuration,
    dry_run: bool,
) -> Result<(), String> {
    let region = pd_client.get_region_by_id(region_id).await.unwrap();
    if region.is_none() {
        return Err(format!("region {} not found", region_id));
    }
    let region = region.unwrap();
    let start_key = if region.start_key.is_empty() {
        vec![]
    } else {
        decode_bytes(&mut region.start_key(), false).unwrap()
    };
    let end_key = if region.end_key.is_empty() {
        vec![]
    } else {
        decode_bytes(&mut region.end_key(), false).unwrap()
    };

    let tikv_client_config = if config.security.ca_path.is_empty() {
        tikv_client::Config::default()
    } else {
        tikv_client::Config::default().with_security(
            config.security.ca_path.clone(),
            config.security.cert_path.clone(),
            config.security.key_path.clone(),
        )
    };
    let tikv_client = TiKVClient::new_with_config(config.pd.endpoints.clone(), tikv_client_config)
        .await
        .unwrap();

    let now = tikv_client.current_timestamp().await.unwrap();
    let physical = now.physical - safepoint_lifetime.as_millis() as i64;
    let safepoint = Timestamp {
        physical,
        logical: 0,
        suffix_bits: now.suffix_bits,
    };

    println!(
        "Resolve lock for region: {:?}, range: {} - {}, safepoint: {:?}",
        region,
        LogValue::key(&start_key),
        LogValue::key(&end_key),
        safepoint.version()
    );

    if !dry_run {
        let options = ResolveLocksOptions {
            ..Default::default()
        };
        let range = start_key..end_key;

        let result = tikv_client
            .cleanup_locks(range, &safepoint, options)
            .await
            .unwrap();
        println!(
            "resolve lock succeed, resolved locks: {}",
            result.resolved_locks
        );
    }
    Ok(())
}

fn get_resolve_lock_config_from_args(args: &ResolveLockArgs) -> ResolveLockConfig {
    let mut config = ResolveLockConfig::default();
    if args.config.exists() {
        let data = std::fs::read(args.config.clone()).expect("failed to read config file");
        config = toml::from_slice(&data).unwrap();
    }
    config.security.override_from_env();
    // override from args
    if !args.pd.is_empty() {
        config.pd.endpoints = args.pd.split(',').map(|x| x.to_owned()).collect();
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
    config
}
