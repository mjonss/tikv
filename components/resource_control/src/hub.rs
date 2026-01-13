// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
    time::Duration,
};

use tikv_util::info;

use crate::{
    AtomicDuration, Config, GlobalInstantUsages, Metric, ResourceEvent, ResourceSubscriber,
    ResourceType, ResourceUsage, Scope, TimeUnit, Usages,
};

#[async_trait::async_trait]
pub trait ResourceHub: Send + Sync {
    fn register_subscriber(
        &self,
        resource_type: ResourceType,
        subscriber: Box<dyn ResourceSubscriber>,
    );
    fn on_events(&self, events: &[ResourceEvent]) -> bool /* stop */;
}

#[derive(Clone)]
pub struct EventHub {
    pub(crate) core: Arc<EventHubCore>,
}

pub struct EventHubCore {
    pub(crate) enabled: AtomicBool,
    pub(crate) dry_run: AtomicBool,
    pub(crate) debug: AtomicBool,
    pub(crate) stopped: AtomicBool,
    pub(crate) stats_interval: AtomicDuration,
    pub(crate) data: RwLock<EventHubData>,
}

impl EventHubCore {
    pub(crate) fn new(config: Config, usage: ResourceUsage) -> Self {
        Self {
            enabled: AtomicBool::from(config.enabled),
            dry_run: AtomicBool::from(config.dry_run),
            debug: AtomicBool::from(config.debug),
            stopped: AtomicBool::from(false),
            stats_interval: AtomicDuration::new(config.stats_interval.0, TimeUnit::Millisecond),
            data: RwLock::new(EventHubData::new(config, usage)),
        }
    }
}

impl Deref for EventHub {
    type Target = EventHubCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

pub struct EventHubData {
    pub(crate) config: Config,
    pub(crate) whitelist_keyspace_ids: Vec<u32>,
    pub(crate) ignore_keyspace_ids: Vec<u32>,
    pub(crate) usage: ResourceUsage,
    pub(crate) subscribers: HashMap<ResourceType, Vec<Box<dyn ResourceSubscriber>>>,
    pub(crate) metrics: Option<Vec<Metric>>,
}

impl EventHubData {
    pub(crate) fn new(config: Config, usage: ResourceUsage) -> Self {
        let whitelist_keyspace_ids =
            serde_json::from_str::<Vec<u32>>(&config.whitelist_keyspace_ids).unwrap_or_default();
        let ignore_keyspace_ids =
            serde_json::from_str::<Vec<u32>>(&config.ignore_keyspace_ids).unwrap_or_default();
        Self {
            config: config.clone(),
            whitelist_keyspace_ids,
            ignore_keyspace_ids,
            usage,
            subscribers: HashMap::new(),
            metrics: Some(Vec::new()),
        }
    }

    pub(crate) fn get_keyspace_usage(
        &self,
        resource_type: ResourceType,
        keyspace_id: u32,
    ) -> Option<GlobalInstantUsages> {
        self.usage.get_keyspace_usage(resource_type, keyspace_id)
    }

    fn allow(&mut self, keyspace_id: u32) -> bool {
        if !self.whitelist_keyspace_ids.is_empty()
            && !self.whitelist_keyspace_ids.contains(&keyspace_id)
        {
            return false;
        }
        !self.ignore_keyspace_ids.contains(&keyspace_id)
    }

    fn collect_metric(&mut self, metric: &Metric) {
        let keyspace_id = match metric.scope {
            Scope::Region {
                keyspace_id,
                region_id: _,
            } => Some(keyspace_id),
            Scope::Keyspace { keyspace_id } => Some(keyspace_id),
            Scope::Global => None,
        };

        if let Some(keyspace_id) = keyspace_id {
            if !self.allow(keyspace_id) {
                return;
            }
        }
        self.usage.on_metric(metric);
    }

    fn dispatch_usages(&mut self, usages: &Usages) {
        let resource_type = match usages {
            Usages::Global { usages } => usages.resource_type,
            Usages::Keyspace { usages } => usages.resource_type,
        };

        if let Some(subscribers) = self.subscribers.get_mut(&resource_type) {
            for sub in subscribers {
                sub.on_event(&ResourceEvent::DispatchUsages(usages.clone()));
            }
        }
    }

    fn dispatch_config(&mut self, config: &Config) {
        for (_, subscribers) in self.subscribers.iter_mut() {
            for sub in subscribers {
                sub.on_event(&ResourceEvent::UpdateConfig(config.clone()));
            }
        }
    }

    fn dispatch(&mut self, resource_event: &ResourceEvent) {
        match resource_event {
            ResourceEvent::DispatchUsages(usages) => {
                self.dispatch_usages(usages);
            }
            ResourceEvent::UpdateConfig(config) => self.dispatch_config(config),
            _ => {}
        };
    }

    fn collect_metrics(&mut self, metrics: &Vec<Metric>) {
        if metrics.is_empty() {
            return;
        }
        for metric in metrics {
            self.collect_metric(metric);
        }
    }

    fn on_tick(&mut self) {
        self.usage.on_tick();
        let (global_usages, keyspace_usages) = self.usage.usage();
        for usages in global_usages.iter() {
            self.dispatch_usages(&Usages::Global {
                usages: usages.clone(),
            });
        }
        for usages in keyspace_usages.iter() {
            self.dispatch_usages(&Usages::Keyspace {
                usages: usages.clone(),
            });
        }
    }
}

impl EventHub {
    pub(crate) fn new(config: Config, usage: ResourceUsage) -> Self {
        Self {
            core: Arc::new(EventHubCore::new(config, usage)),
        }
    }

    pub(crate) fn get_enabled(&self) -> bool {
        self.enabled.load(Relaxed)
    }

    pub(crate) fn get_dry_run(&self) -> bool {
        self.dry_run.load(Relaxed)
    }

    pub(crate) fn get_debug(&self) -> bool {
        self.debug.load(Relaxed)
    }

    pub(crate) fn get_stopped(&self) -> bool {
        self.stopped.load(Relaxed)
    }

    pub(crate) fn get_stats_interval(&self) -> Duration {
        self.stats_interval.load()
    }

    #[allow(dead_code)]
    pub(crate) fn get_keyspace_usage(
        &self,
        resource_type: ResourceType,
        keyspace_id: u32,
    ) -> Option<GlobalInstantUsages> {
        let data = self.data.read().unwrap();
        data.get_keyspace_usage(resource_type, keyspace_id)
    }

    fn update_config(&self, new_config: &Config, data: &mut EventHubData) {
        let mut config = data.config.clone();
        if config.enabled != new_config.enabled {
            config.enabled = new_config.enabled;
            self.enabled.store(config.enabled, Relaxed);
        }
        if config.dry_run != new_config.dry_run {
            config.dry_run = new_config.dry_run;
            self.dry_run.store(config.dry_run, Relaxed);
        }
        if config.debug != new_config.debug {
            config.debug = new_config.debug;
            self.debug.store(config.debug, Relaxed);
        }
        if config.window_size != new_config.window_size {
            config.window_size = new_config.window_size;
            data.usage.update_window_size(config.window_size.0);
        }
        if config.stats_interval != new_config.stats_interval {
            config.stats_interval = new_config.stats_interval;
            self.stats_interval.store(config.stats_interval.0);
        }
        if config.whitelist_keyspace_ids != new_config.whitelist_keyspace_ids {
            config.whitelist_keyspace_ids = new_config.whitelist_keyspace_ids.clone();
            data.whitelist_keyspace_ids =
                serde_json::from_str::<Vec<u32>>(&config.whitelist_keyspace_ids)
                    .unwrap_or_default();
        }
        if config.ignore_keyspace_ids != new_config.ignore_keyspace_ids {
            config.ignore_keyspace_ids = new_config.ignore_keyspace_ids.clone();
            data.ignore_keyspace_ids =
                serde_json::from_str::<Vec<u32>>(&config.ignore_keyspace_ids).unwrap_or_default();
        }
        if config.smoothing_factor != new_config.smoothing_factor {
            config.smoothing_factor = new_config.smoothing_factor;
            data.usage.update_smoothing_factor(config.smoothing_factor);
        }
        if config.active_quota_ratio != new_config.active_quota_ratio {
            config.active_quota_ratio = new_config.active_quota_ratio;
            data.usage
                .update_active_quota_ratio(config.active_quota_ratio);
        }
        if config.min_quota_ratio != new_config.min_quota_ratio {
            config.min_quota_ratio = new_config.min_quota_ratio;
            data.usage.update_min_quota_ratio(config.min_quota_ratio);
        }
        if config.severity_stressed_factor != new_config.severity_stressed_factor {
            config.severity_stressed_factor = new_config.severity_stressed_factor;
            data.usage.severity_stressed_factor = config.severity_stressed_factor;
        }
        if config.severity_critical_factor != new_config.severity_critical_factor {
            config.severity_critical_factor = new_config.severity_critical_factor;
            data.usage.severity_critical_factor = config.severity_critical_factor;
        }
        if config.severity_exhausted_factor != new_config.severity_exhausted_factor {
            config.severity_exhausted_factor = new_config.severity_exhausted_factor;
            data.usage.severity_exhausted_factor = config.severity_exhausted_factor;
        }
        if config.severity_threshold_stressed != new_config.severity_threshold_stressed {
            config.severity_threshold_stressed = new_config.severity_threshold_stressed;
            data.usage.severity_threshold.stressed = config.severity_threshold_stressed;
        }
        if config.severity_threshold_critical != new_config.severity_threshold_critical {
            config.severity_threshold_critical = new_config.severity_threshold_critical;
            data.usage.severity_threshold.critical = config.severity_threshold_critical;
        }
        if config.severity_threshold_exhausted != new_config.severity_threshold_exhausted {
            config.severity_threshold_exhausted = new_config.severity_threshold_exhausted;
            data.usage.severity_threshold.exhausted = config.severity_threshold_exhausted;
        }
        data.config = config;
    }

    fn handle_event(
        &self,
        metrics: &mut Vec<Metric>,
        event: &ResourceEvent,
        data: &mut EventHubData,
    ) -> bool /* stop */ {
        match event {
            ResourceEvent::CollectMetric(metric) => {
                if !self.enabled.load(Relaxed) {
                    return false;
                }
                metrics.push(*metric);
            }
            ResourceEvent::DispatchUsages(_usages) => {
                if !self.enabled.load(Relaxed) {
                    return false;
                }
                data.dispatch(event);
            }
            ResourceEvent::UpdateConfig(config) => {
                info!("resource control event hub update config {:?}", config);
                self.update_config(config, data);
                data.dispatch(event);
            }
            ResourceEvent::Tick => {
                if !metrics.is_empty() {
                    data.collect_metrics(metrics);
                    metrics.clear();
                }
                info!("resource control event hub tick");
                data.on_tick();
            }
            ResourceEvent::Stop => {
                self.stopped.store(true, Relaxed);
                info!("resource control event hub stop");
                return true;
            }
        }
        false
    }
}

#[async_trait::async_trait]
impl ResourceHub for EventHub {
    fn register_subscriber(
        &self,
        resource_type: ResourceType,
        subscriber: Box<dyn ResourceSubscriber>,
    ) {
        let mut data = self.data.write().unwrap();
        data.subscribers
            .entry(resource_type)
            .or_default()
            .push(subscriber);
    }

    fn on_events(&self, events: &[ResourceEvent]) -> bool /* stop */ {
        if self.get_debug() {
            info!("resource control on events {} ", events.len());
        }
        let mut guard = self.data.write().unwrap();
        let data = guard.deref_mut();
        let mut metrics = data.metrics.take().unwrap_or_default();
        for event in events {
            let stop = self.handle_event(&mut metrics, event, data);
            if stop {
                return true;
            }
        }
        data.collect_metrics(&metrics);
        metrics.clear();
        data.metrics = Some(metrics);
        false
    }
}
