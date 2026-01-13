// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use futures_util::compat::Future01CompatExt;
use kvproto::import_sstpb::*;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tokio::runtime::Handle;

use super::{Config, Result};

struct ImportModeSwitcherInner {
    is_import: Arc<AtomicBool>,
    timeout: Duration,
    next_check: Instant,
}

impl ImportModeSwitcherInner {
    fn enter_normal_mode(&mut self) -> Result<bool> {
        if !self.is_import.load(Ordering::Acquire) {
            return Ok(false);
        }

        info!("enter normal mode");
        self.is_import.store(false, Ordering::Release);
        Ok(true)
    }

    fn enter_import_mode(&mut self) -> Result<bool> {
        if self.is_import.load(Ordering::Acquire) {
            return Ok(false);
        }
        info!("enter import mode");
        self.is_import.store(true, Ordering::Release);
        Ok(true)
    }
}

#[derive(Clone)]
pub struct ImportModeSwitcher {
    inner: Arc<Mutex<ImportModeSwitcherInner>>,
    is_import: Arc<AtomicBool>,
}

impl ImportModeSwitcher {
    pub fn new(cfg: &Config) -> ImportModeSwitcher {
        let timeout = cfg.import_mode_timeout.0;
        let is_import = Arc::new(AtomicBool::new(false));
        let inner = Arc::new(Mutex::new(ImportModeSwitcherInner {
            is_import: is_import.clone(),
            timeout,
            next_check: Instant::now() + timeout,
        }));
        ImportModeSwitcher { inner, is_import }
    }

    pub fn start(&self, executor: &Handle) {
        // spawn a background future to put TiKV back into normal mode after timeout
        let inner = self.inner.clone();
        let switcher = Arc::downgrade(&inner);
        let timer_loop = async move {
            // loop until the switcher has been dropped
            while let Some(switcher) = switcher.upgrade() {
                let next_check = {
                    let mut switcher = switcher.lock().unwrap();
                    let now = Instant::now();
                    if now >= switcher.next_check {
                        if switcher.is_import.load(Ordering::Acquire) {
                            if let Err(e) = switcher.enter_normal_mode() {
                                error!(?e; "failed to put TiKV back into normal mode");
                            }
                        }
                        switcher.next_check = now + switcher.timeout
                    }
                    switcher.next_check
                };

                let ok = GLOBAL_TIMER_HANDLE.delay(next_check).compat().await.is_ok();

                if !ok {
                    warn!("failed to delay with global timer");
                }
            }
        };
        executor.spawn(timer_loop);
    }

    pub fn enter_normal_mode(&self) -> Result<bool> {
        if !self.is_import.load(Ordering::Acquire) {
            return Ok(false);
        }
        self.inner.lock().unwrap().enter_normal_mode()
    }

    pub fn enter_import_mode(&self) -> Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        let ret = inner.enter_import_mode()?;
        inner.next_check = Instant::now() + inner.timeout;
        Ok(ret)
    }

    pub fn get_mode(&self) -> SwitchMode {
        if self.is_import.load(Ordering::Acquire) {
            SwitchMode::Import
        } else {
            SwitchMode::Normal
        }
    }
}
