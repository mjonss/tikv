// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use super::Result;

const STORE_ADDRESS_REFRESH_SECONDS: u64 = 60;

pub type Callback = Box<dyn FnOnce(Result<String>) + Send>;

pub fn store_address_refresh_interval_secs() -> u64 {
    fail_point!("mock_store_refresh_interval_secs", |arg| arg
        .map_or(0, |e| e.parse().unwrap()));
    STORE_ADDRESS_REFRESH_SECONDS
}

/// A trait for resolving store addresses.
pub trait StoreAddrResolver: Send + Clone {
    /// Resolves the address for the specified store id asynchronously.
    fn resolve(&self, store_id: u64, cb: Callback) -> Result<()>;
}
