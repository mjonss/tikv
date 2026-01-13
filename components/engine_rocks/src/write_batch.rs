// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{self, Result};
use rocksdb::{DB, Writable, WriteBatch as RawWriteBatch, WriteOptions};

use crate::{engine::RocksEngine, r2e, util::get_cf_handle};

const WRITE_BATCH_MAX_BATCH_NUM: usize = 16;
const WRITE_BATCH_MAX_KEY_NUM: usize = 16;

impl RocksEngine {
    pub const WRITE_BATCH_MAX_KEYS: usize = 256;
}

/// `RocksWriteBatchVec` is for method `MultiBatchWrite` of RocksDB, which
/// splits a large WriteBatch into many smaller ones and then any thread could
/// help to deal with these small WriteBatch when it is calling
/// `MultiBatchCommit` and wait the front writer to finish writing.
/// `MultiBatchWrite` will perform much better than traditional
/// `pipelined_write` when TiKV writes very large data into RocksDB.
/// We will remove this feature when `unordered_write` of RocksDB becomes more
/// stable and becomes compatible with Titan.
pub struct RocksWriteBatchVec {
    db: Arc<DB>,
    wbs: Vec<RawWriteBatch>,
    save_points: Vec<usize>,
    index: usize,
    batch_size_limit: usize,
    support_write_batch_vec: bool,
}

impl RocksWriteBatchVec {
    pub fn new(
        db: Arc<DB>,
        batch_size_limit: usize,
        cap: usize,
        support_write_batch_vec: bool,
    ) -> RocksWriteBatchVec {
        let wb = RawWriteBatch::with_capacity(cap);
        RocksWriteBatchVec {
            db,
            wbs: vec![wb],
            save_points: vec![],
            index: 0,
            batch_size_limit,
            support_write_batch_vec,
        }
    }

    pub fn with_unit_capacity(engine: &RocksEngine, cap: usize) -> RocksWriteBatchVec {
        Self::new(
            engine.as_inner().clone(),
            WRITE_BATCH_MAX_KEY_NUM,
            cap,
            engine.support_multi_batch_write(),
        )
    }

    pub fn as_inner(&self) -> &[RawWriteBatch] {
        &self.wbs[0..=self.index]
    }

    pub fn get_db(&self) -> &DB {
        self.db.as_ref()
    }

    /// `check_switch_batch` will split a large WriteBatch into many smaller
    /// ones. This is to avoid a large WriteBatch blocking write_thread too
    /// long.
    #[inline(always)]
    fn check_switch_batch(&mut self) {
        if self.support_write_batch_vec
            && self.batch_size_limit > 0
            && self.wbs[self.index].count() >= self.batch_size_limit
        {
            self.index += 1;
            if self.index >= self.wbs.len() {
                self.wbs.push(RawWriteBatch::default());
            }
        }
    }
}

impl RocksWriteBatchVec {
    /// Commit the WriteBatch to disk atomically
    pub fn write(&mut self) -> Result<u64> {
        let mut seq = 0;
        let opt = WriteOptions::default();
        if self.support_write_batch_vec {
            // FIXME(tabokie): Callback for empty write batch won't be called.
            self.get_db()
                .multi_batch_write_callback(self.as_inner(), &opt, |s| {
                    seq = s;
                })
                .map_err(r2e)?;
        } else {
            self.get_db()
                .write_callback(&self.wbs[0], &opt, |s| {
                    seq = s;
                })
                .map_err(r2e)?;
        }
        Ok(seq)
    }

    pub fn data_size(&self) -> usize {
        let mut size: usize = 0;
        for i in 0..=self.index {
            size += self.wbs[i].data_size();
        }
        size
    }

    pub fn count(&self) -> usize {
        self.wbs[self.index].count() + self.index * self.batch_size_limit
    }

    pub fn is_empty(&self) -> bool {
        self.wbs[0].is_empty()
    }

    pub fn clear(&mut self) {
        for i in 0..=self.index {
            self.wbs[i].clear();
        }
        self.save_points.clear();
        // Avoid making the wbs too big at one time, then the memory will be kept
        // after reusing
        if self.index > WRITE_BATCH_MAX_BATCH_NUM + 1 {
            self.wbs.shrink_to(WRITE_BATCH_MAX_BATCH_NUM + 1);
        }
        self.index = 0;
    }
}

impl RocksWriteBatchVec {
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_switch_batch();
        self.wbs[self.index].put(key, value).map_err(r2e)
    }

    pub fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wbs[self.index].put_cf(handle, key, value).map_err(r2e)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        self.wbs[self.index].delete(key).map_err(r2e)
    }

    pub fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wbs[self.index].delete_cf(handle, key).map_err(r2e)
    }

    pub fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        self.wbs[self.index]
            .delete_range(begin_key, end_key)
            .map_err(r2e)
    }

    pub fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wbs[self.index]
            .delete_range_cf(handle, begin_key, end_key)
            .map_err(r2e)
    }
}
