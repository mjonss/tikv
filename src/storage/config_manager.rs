// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Storage online config manager.

use std::{convert::TryInto, sync::Arc};

use engine_traits::CF_DEFAULT;
use file_system::{IoPriority, IoType, get_io_rate_limiter};
use online_config::{ConfigChange, ConfigManager, ConfigValue, Result as CfgResult};
use strum::IntoEnumIterator;
use tikv_kv::Engine;
use tikv_util::config::ReadableSize;

use crate::{
    server::CONFIG_ROCKSDB_GAUGE,
    storage::{TxnScheduler, lock_manager::LockManager, txn::flow_controller::FlowController},
};

pub struct StorageConfigManger<E: Engine, L: LockManager> {
    shared_block_cache: bool,
    flow_controller: Arc<FlowController>,
    _scheduler: TxnScheduler<E, L>,
}

unsafe impl<E: Engine, L: LockManager> Send for StorageConfigManger<E, L> {}
unsafe impl<E: Engine, L: LockManager> Sync for StorageConfigManger<E, L> {}

impl<E: Engine, L: LockManager> StorageConfigManger<E, L> {
    pub fn new(
        shared_block_cache: bool,
        flow_controller: Arc<FlowController>,
        scheduler: TxnScheduler<E, L>,
    ) -> Self {
        StorageConfigManger {
            shared_block_cache,
            flow_controller,
            _scheduler: scheduler,
        }
    }
}

impl<EK: Engine, L: LockManager> ConfigManager for StorageConfigManger<EK, L> {
    fn dispatch(&mut self, mut change: ConfigChange) -> CfgResult<()> {
        if let Some(ConfigValue::Module(mut block_cache)) = change.remove("block_cache") {
            if !self.shared_block_cache {
                return Err("shared block cache is disabled".into());
            }
            if let Some(size) = block_cache.remove("capacity") {
                if size != ConfigValue::None {
                    let s: ReadableSize = size.into();
                    // Write config to metric
                    CONFIG_ROCKSDB_GAUGE
                        .with_label_values(&[CF_DEFAULT, "block_cache_size"])
                        .set(s.0 as f64);
                }
            }
        } else if let Some(ConfigValue::Module(mut flow_control)) = change.remove("flow_control") {
            if let Some(v) = flow_control.remove("enable") {
                let enable: bool = v.into();
                self.flow_controller.enable(enable);
            }
        }
        if let Some(ConfigValue::Module(mut io_rate_limit)) = change.remove("io_rate_limit") {
            let limiter = match get_io_rate_limiter() {
                None => return Err("IO rate limiter is not present".into()),
                Some(limiter) => limiter,
            };
            if let Some(limit) = io_rate_limit.remove("max_bytes_per_sec") {
                let limit: ReadableSize = limit.into();
                limiter.set_io_rate_limit(limit.0 as usize);
            }

            for t in IoType::iter() {
                if let Some(priority) = io_rate_limit.remove(&(t.as_str().to_owned() + "_priority"))
                {
                    let priority: IoPriority = priority.try_into()?;
                    limiter.set_io_priority(t, priority);
                }
            }
        }
        Ok(())
    }
}
