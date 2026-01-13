// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use kvengine::limiter::{RegionLimiter, StoreLimiter};

pub enum FlowController {
    NoLimit,
    Cloud(Arc<StoreLimiter>),
}

macro_rules! flow_controller_fn {
    ($fn_name:ident, $region_id:ident, $type:ident) => {
        pub fn $fn_name(&self, $region_id: u64) -> $type {
            match self {
                FlowController::NoLimit => $type::default(),
                FlowController::Cloud(ref controller) => controller.$fn_name($region_id),
            }
        }
    };
    ($fn_name:ident, $region_id:ident, $bytes:ident, $type:ident) => {
        pub fn $fn_name(&self, $region_id: u64, $bytes: usize) -> $type {
            match self {
                FlowController::NoLimit => $type::default(),
                FlowController::Cloud(ref controller) => controller.$fn_name($region_id, $bytes),
            }
        }
    };
}

impl FlowController {
    flow_controller_fn!(should_drop, region_id, bool);
    #[cfg(test)]
    flow_controller_fn!(discard_ratio, region_id, f64);
    flow_controller_fn!(consume, region_id, bytes, Duration);
    #[cfg(test)]
    flow_controller_fn!(total_bytes_consumed, region_id, usize);
    flow_controller_fn!(is_unlimited, region_id, bool);

    pub fn unconsume(&self, region_id: u64, bytes: usize) {
        match self {
            FlowController::NoLimit => {}
            FlowController::Cloud(ref controller) => controller.unconsume(region_id, bytes),
        }
    }
    pub fn enable(&self, enable: bool) {
        match self {
            FlowController::NoLimit => {}
            FlowController::Cloud(ref controller) => controller.enable(enable),
        }
    }

    pub fn enabled(&self) -> bool {
        match self {
            FlowController::NoLimit => false,
            FlowController::Cloud(ref controller) => controller.enabled(),
        }
    }

    #[cfg(test)]
    pub fn set_speed_limit(&self, region_id: u64, speed_limit: f64) {
        match self {
            FlowController::NoLimit => {}
            FlowController::Cloud(ref controller) => {
                controller.set_speed_limit(region_id, speed_limit)
            }
        }
    }
}

pub const CLOUD_MIN_THROTTLE_SPEED: u64 = 1024 * 1024; // 1MB/s

pub struct FlowControlHelper {
    region_id: u64,
    write_size: usize,
    flow_controller: Arc<FlowController>,
    region_limiter: Option<RegionLimiter>,
}

impl FlowControlHelper {
    pub fn new(
        region_id: u64,
        write_size: usize,
        flow_controller: Arc<FlowController>,
        region_limiter: Option<RegionLimiter>,
    ) -> Self {
        Self {
            region_id,
            write_size,
            flow_controller,
            region_limiter,
        }
    }

    pub fn enabled(&self) -> bool {
        self.flow_controller.enabled() || self.region_limiter.as_ref().is_some_and(|s| s.enabled())
    }

    pub fn is_unlimited(&self) -> bool {
        self.flow_controller.is_unlimited(self.region_id)
            && self
                .region_limiter
                .as_ref()
                .is_none_or(|s| s.is_unlimited(self.region_id))
    }

    pub fn consume(&self) -> Duration {
        let mut dur = self
            .flow_controller
            .consume(self.region_id, self.write_size);
        if let Some(s) = self.region_limiter.as_ref() {
            dur = dur.max(s.consume(self.region_id, self.write_size));
        }
        dur
    }

    pub fn unconsume(&self) {
        self.flow_controller
            .unconsume(self.region_id, self.write_size);
        if let Some(s) = self.region_limiter.as_ref() {
            s.unconsume(self.region_id, self.write_size);
        }
    }
}
