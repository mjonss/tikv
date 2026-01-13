// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error::Error, sync::Arc, time::Duration};

use online_config::{ConfigChange, OnlineConfig};
use serde_derive::{Deserialize, Serialize};
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    info,
};

use crate::{ResourceEvent, ResourcePublisher, SeverityThreshold, limiter::MAX_WAIT_TIME};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub enabled: bool,
    pub dry_run: bool,
    pub debug: bool,
    pub window_size: ReadableDuration,
    pub stats_interval: ReadableDuration,
    pub limiter_timeout: ReadableDuration,
    pub limiter_stats_interval: ReadableDuration,
    pub smoothing_factor: f64,
    pub active_quota_ratio: f64,
    pub min_quota_ratio: f64,
    pub severity_stressed_factor: f64,
    pub severity_critical_factor: f64,
    pub severity_exhausted_factor: f64,
    pub severity_threshold_stressed: f64,
    pub severity_threshold_critical: f64,
    pub severity_threshold_exhausted: f64,
    pub whitelist_keyspace_ids: String,
    pub ignore_keyspace_ids: String,
    // read
    pub read_enabled: bool,
    pub max_read_cpu_ratio: f64,
    pub max_read_bytes_factor: f64,
    pub max_read_bytes_per_core_per_second: ReadableSize,
    pub max_read_wait_time: ReadableDuration,
    // transfer leader
    pub transfer_leader_enabled: bool,
    pub max_transfer_leader_per_second: u64,
}

impl Default for Config {
    fn default() -> Config {
        let severity_threshold = SeverityThreshold::default();
        Config {
            enabled: false,
            dry_run: false,
            debug: false,
            window_size: ReadableDuration::minutes(1),
            stats_interval: ReadableDuration::secs(1),
            limiter_timeout: ReadableDuration::secs(2),
            limiter_stats_interval: ReadableDuration::secs(1),
            smoothing_factor: 0.9,
            active_quota_ratio: 0.8,
            min_quota_ratio: 0.01,
            severity_stressed_factor: 1.0,
            severity_critical_factor: 1.0,
            severity_exhausted_factor: 1.0,
            severity_threshold_stressed: severity_threshold.stressed,
            severity_threshold_critical: severity_threshold.critical,
            severity_threshold_exhausted: severity_threshold.exhausted,
            whitelist_keyspace_ids: "[]".to_string(),
            ignore_keyspace_ids: "[]".to_string(),
            read_enabled: false,
            max_read_cpu_ratio: 0.6,
            max_read_bytes_factor: 1.0,
            max_read_bytes_per_core_per_second: ReadableSize::mb(64),
            max_read_wait_time: ReadableDuration::millis(MAX_WAIT_TIME.as_millis() as u64),
            transfer_leader_enabled: false,
            max_transfer_leader_per_second: 64,
        }
    }
}

impl Config {
    /// Check whether the configuration is legal.
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.window_size.0 < Duration::from_millis(1) {
            return Err("window-size cannot be less than 1 milliseconds".into());
        }
        if self.stats_interval.0 < Duration::from_millis(1) {
            return Err("stats-interval cannot be less than 1 milliseconds".into());
        }
        if self.limiter_timeout.0 < Duration::from_millis(1) {
            return Err("limiter-timeout cannot be less than 1 milliseconds".into());
        }
        if self.limiter_stats_interval.0 < Duration::from_millis(1) {
            return Err("limiter-stats-interval cannot be less than 1 milliseconds".into());
        }
        if self.smoothing_factor <= 0.0 || self.smoothing_factor >= 1.0 {
            return Err("smoothing-factor must be a float in the range (0.0, 1.0).".into());
        }
        if self.active_quota_ratio <= 0.0 || self.active_quota_ratio >= 1.0 {
            return Err("active-quota-ratio must be a float in the range (0.0, 1.0).".into());
        }
        if self.min_quota_ratio <= 0.0 || self.min_quota_ratio >= 1.0 {
            return Err("min-quota-ratio must be a float in the range (0.0, 1.0).".into());
        }
        if self.severity_threshold_stressed <= 0.0
            || self.severity_threshold_stressed >= self.severity_threshold_critical
        {
            return Err("severity-threshold-stressed must be a float in the range (0.0, severity-threshold-critical).".into());
        }
        if self.severity_threshold_critical <= self.severity_threshold_stressed
            || self.severity_threshold_critical >= self.severity_threshold_exhausted
        {
            return Err("severity-threshold-critical must be a float in the range (severity-threshold-stressed, severity-threshold-exhausted).".into());
        }
        if self.severity_threshold_exhausted <= self.severity_threshold_critical
            || self.severity_threshold_exhausted >= 1.0
        {
            return Err("severity-threshold-exhausted must be a float in the range (severity-threshold-critical, 1.0).".into());
        }
        if self.max_read_cpu_ratio <= 0.0 || self.max_read_cpu_ratio >= 1.0 {
            return Err("max-read-cpu-ratio must be a float in the range (0.0, 1.0).".into());
        }
        if self.max_read_bytes_per_core_per_second.0 == 0 {
            return Err("max-read-bytes-per-core-per-second cannot be 0".into());
        }
        if self.max_read_wait_time.0 < Duration::from_millis(1) {
            return Err("max-read-wait-time cannot be less than 1 milliseconds".into());
        }
        if self.max_transfer_leader_per_second == 0 {
            return Err("max-transfer-leader-per-second cannot be 0".into());
        }
        Ok(())
    }

    pub fn get_read_enabled(&self) -> bool {
        self.enabled && self.read_enabled
    }

    pub fn get_transfer_leader_enabled(&self) -> bool {
        self.enabled && self.transfer_leader_enabled
    }
}

pub struct ConfigManager {
    pub(crate) event_publisher: Arc<dyn ResourcePublisher>,
    pub(crate) current_config: Config,
}

impl ConfigManager {
    pub fn new(event_publisher: Arc<dyn ResourcePublisher>, config: Config) -> Self {
        info!("new config manager for resource control {:?}", config);
        Self {
            event_publisher,
            current_config: config,
        }
    }
}

impl online_config::ConfigManager for ConfigManager {
    fn dispatch(&mut self, change: ConfigChange) -> online_config::Result<()> {
        info!("resource control config change {:?}", change);
        let mut new_config = self.current_config.clone();
        new_config.update(change)?;
        new_config.validate()?;
        self.event_publisher
            .publish(ResourceEvent::UpdateConfig(new_config.clone()));
        self.current_config = new_config;
        Ok(())
    }
}
