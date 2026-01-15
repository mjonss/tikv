// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{env, error::Error, time::Duration};

use tikv_util::config::ReadableDuration;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub backend: String,

    pub prefix: String,

    pub s3_endpoint: String,

    pub s3_key_id: String,

    pub s3_secret_key: String,

    pub s3_bucket: String,

    pub s3_region: String,

    pub remote_compactor_addr: String,

    pub zstd_compression_level: String,

    pub allow_fallback_local: bool,

    /// Set to true during recovery for safety.
    pub read_only: bool,

    pub conn_options: ConnOptions,

    pub azure: AzureConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            backend: "s3".to_string(),
            prefix: "".to_string(),
            s3_endpoint: "".to_string(),
            s3_key_id: "".to_string(),
            s3_secret_key: "".to_string(),
            s3_bucket: "".to_string(),
            s3_region: "".to_string(),
            remote_compactor_addr: "".to_string(),
            zstd_compression_level: "".to_string(),
            allow_fallback_local: true,
            read_only: false,
            conn_options: ConnOptions::default(),
            azure: AzureConfig::default(),
        }
    }
}

impl Config {
    #[allow(dead_code)]
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        // TODO(x) validate dfs config
        Ok(())
    }

    fn env_or_default(name: &str, val: &mut String) {
        if let Ok(v) = env::var(name) {
            *val = v;
        }
    }

    fn env_or_default_bool(name: &str, val: &mut bool) {
        if let Ok(v) = env::var(name) {
            *val = v == "true";
        }
    }

    pub fn override_from_env(&mut self) {
        Self::env_or_default("DFS_BACKEND", &mut self.backend);
        Self::env_or_default("DFS_S3_BUCKET", &mut self.s3_bucket);
        Self::env_or_default("DFS_S3_ENDPOINT", &mut self.s3_endpoint);
        Self::env_or_default("DFS_PREFIX", &mut self.prefix);
        Self::env_or_default("DFS_S3_KEY_ID", &mut self.s3_key_id);
        Self::env_or_default("DFS_S3_SECRET_KEY", &mut self.s3_secret_key);
        Self::env_or_default("DFS_S3_REGION", &mut self.s3_region);
        self.azure.override_from_env();
        Self::env_or_default("DFS_REMOTE_COMPACTOR_ADDR", &mut self.remote_compactor_addr);
        Self::env_or_default(
            "DFS_ZSTD_COMPRESSION_LEVEL",
            &mut self.zstd_compression_level,
        );

        Self::env_or_default_bool("DFS_ALLOW_FALLBACK_LOCAL", &mut self.allow_fallback_local);
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct AzureConfig {
    pub connection_string: String,
    pub account_name: String,
    pub account_key: String,
    pub container: String,
    pub endpoint: String,
}

impl AzureConfig {
    fn override_from_env(&mut self) {
        Config::env_or_default("DFS_AZURE_CONNECTION_STRING", &mut self.connection_string);
        Config::env_or_default("DFS_AZURE_ACCOUNT_NAME", &mut self.account_name);
        Config::env_or_default("DFS_AZURE_ACCOUNT_KEY", &mut self.account_key);
        Config::env_or_default("DFS_AZURE_CONTAINER", &mut self.container);
        Config::env_or_default("DFS_AZURE_ENDPOINT", &mut self.endpoint);
    }
}

const MAX_RETRY_COUNT: u32 = 9;
const RETRY_SLEEP_INTERVAL: Duration = Duration::from_millis(500);
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
const DISPATCH_TIMEOUT: Duration = Duration::from_secs(10);
const READ_BODY_TIMEOUT: Duration = Duration::from_secs(10);
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(20);
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ConnOptions {
    pub max_retry_count: u32,
    pub retry_sleep_interval: ReadableDuration,
    pub conn_timeout: ReadableDuration,
    pub dispatch_timeout: ReadableDuration,
    // The timeout adds to dispatch timeout per MB of body size.
    pub write_timeout_per_mb: ReadableDuration,
    pub read_body_timeout: ReadableDuration,
    pub keep_alive_duration: ReadableDuration,
    pub pool_idle_timeout: ReadableDuration,
}

impl Default for ConnOptions {
    fn default() -> Self {
        Self {
            max_retry_count: MAX_RETRY_COUNT,
            retry_sleep_interval: ReadableDuration(RETRY_SLEEP_INTERVAL),
            conn_timeout: ReadableDuration(CONNECTION_TIMEOUT),
            dispatch_timeout: ReadableDuration(DISPATCH_TIMEOUT),
            write_timeout_per_mb: ReadableDuration(Duration::from_millis(100)),
            read_body_timeout: ReadableDuration(READ_BODY_TIMEOUT),
            keep_alive_duration: ReadableDuration(KEEP_ALIVE_INTERVAL),
            pool_idle_timeout: ReadableDuration(POOL_IDLE_TIMEOUT),
        }
    }
}
