// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{self, Result};
use rocksdb::{DB, DBIterator};

use crate::r2e;

// FIXME: Would prefer using &DB instead of Arc<DB>.  As elsewhere in
// this crate, it would require generic associated types.
pub struct RocksEngineIterator(DBIterator<Arc<DB>>);

impl RocksEngineIterator {
    pub fn from_raw(iter: DBIterator<Arc<DB>>) -> RocksEngineIterator {
        RocksEngineIterator(iter)
    }
}

impl RocksEngineIterator {
    pub fn seek(&mut self, key: &[u8]) -> Result<bool> {
        self.0.seek(rocksdb::SeekKey::Key(key)).map_err(r2e)
    }

    pub fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        self.0
            .seek_for_prev(rocksdb::SeekKey::Key(key))
            .map_err(r2e)
    }

    pub fn seek_to_first(&mut self) -> Result<bool> {
        self.0.seek(rocksdb::SeekKey::Start).map_err(r2e)
    }

    pub fn seek_to_last(&mut self) -> Result<bool> {
        self.0.seek(rocksdb::SeekKey::End).map_err(r2e)
    }

    pub fn prev(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(r2e("Iterator invalid"));
        }
        self.0.prev().map_err(r2e)
    }

    pub fn next(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(r2e("Iterator invalid"));
        }
        self.0.next().map_err(r2e)
    }

    pub fn key(&self) -> &[u8] {
        #[cfg(not(feature = "nortcheck"))]
        assert!(self.valid().unwrap());
        self.0.key()
    }

    pub fn value(&self) -> &[u8] {
        #[cfg(not(feature = "nortcheck"))]
        assert!(self.valid().unwrap());
        self.0.value()
    }

    pub fn valid(&self) -> Result<bool> {
        self.0.valid().map_err(r2e)
    }
}

/// Collect all items of `it` into a vector, generally used for tests.
///
/// # Panics
///
/// If any errors occur during iterator.
pub fn collect_db(mut it: RocksEngineIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut v = Vec::new();
    let mut it_valid = it.valid().unwrap();
    while it_valid {
        let kv = (it.key().to_vec(), it.value().to_vec());
        v.push(kv);
        it_valid = it.next().unwrap();
    }
    v
}
