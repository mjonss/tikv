// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{IterOptions, Peekable, ReadOptions, Result, iter_option};
use rocksdb::{DB, DBIterator, Writable};

use crate::{
    RocksEngineIterator, RocksSnapshot, RocksWriteBatchVec, db_vector::RocksDbVector,
    options::RocksReadOptions, r2e, util::get_cf_handle,
};

#[derive(Clone, Debug)]
pub struct RocksEngine {
    db: Arc<DB>,
    shared_block_cache: bool,
    support_multi_batch_write: bool,
}

pub(crate) const WRITE_BATCH_MAX_KEY_NUM: usize = 16;

impl RocksEngine {
    pub(crate) fn new(db: DB) -> RocksEngine {
        RocksEngine::from_db(Arc::new(db))
    }

    pub fn from_db(db: Arc<DB>) -> Self {
        RocksEngine {
            db: db.clone(),
            shared_block_cache: false,
            support_multi_batch_write: db.get_db_options().is_enable_multi_batch_write(),
        }
    }

    pub fn snapshot(&self) -> RocksSnapshot {
        RocksSnapshot::new(self.db.clone())
    }

    pub fn sync(&self) -> Result<()> {
        self.db.sync_wal().map_err(r2e)
    }

    pub fn write_batch(&self) -> RocksWriteBatchVec {
        RocksWriteBatchVec::new(
            Arc::clone(self.as_inner()),
            WRITE_BATCH_MAX_KEY_NUM,
            1,
            self.support_multi_batch_write(),
        )
    }

    pub fn as_inner(&self) -> &Arc<DB> {
        &self.db
    }

    pub fn get_sync_db(&self) -> Arc<DB> {
        self.db.clone()
    }

    pub fn set_shared_block_cache(&mut self, enable: bool) {
        self.shared_block_cache = enable;
    }

    pub fn shared_block_cache(&self) -> bool {
        self.shared_block_cache
    }

    pub fn support_multi_batch_write(&self) -> bool {
        self.support_multi_batch_write
    }
}

impl RocksEngine {
    pub fn iterator(&self, cf: &str) -> Result<RocksEngineIterator> {
        self.iterator_opt(cf, IterOptions::default())
    }

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<RocksEngineIterator> {
        let handle = get_cf_handle(&self.db, cf)?;
        let opt: RocksReadOptions = opts.into();
        Ok(RocksEngineIterator::from_raw(DBIterator::new_cf(
            self.db.clone(),
            handle,
            opt.into_raw(),
        )))
    }

    pub fn scan<F>(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let iter_opt = iter_option(start_key, end_key, fill_cache);
        let mut it = self.iterator_opt(cf, iter_opt)?;
        let mut remained = it.seek(start_key)?;
        while remained {
            remained = f(it.key(), it.value())? && it.next()?;
        }
        Ok(())
    }
}

impl Peekable for RocksEngine {
    type DbVector = RocksDbVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<RocksDbVector>> {
        let opt: RocksReadOptions = opts.into();
        let v = self.db.get_opt(key, &opt.into_raw()).map_err(r2e)?;
        Ok(v.map(RocksDbVector::from_raw))
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<RocksDbVector>> {
        let opt: RocksReadOptions = opts.into();
        let handle = get_cf_handle(&self.db, cf)?;
        let v = self
            .db
            .get_cf_opt(handle, key, &opt.into_raw())
            .map_err(r2e)?;
        Ok(v.map(RocksDbVector::from_raw))
    }
}

impl RocksEngine {
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put(key, value).map_err(r2e)
    }

    pub fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = get_cf_handle(&self.db, cf)?;
        self.db.put_cf(handle, key, value).map_err(r2e)
    }

    pub fn put_msg<M: protobuf::Message>(&self, key: &[u8], m: &M) -> Result<()> {
        self.put(key, &m.write_to_bytes()?)
    }

    pub fn put_msg_cf<M: protobuf::Message>(&self, cf: &str, key: &[u8], m: &M) -> Result<()> {
        self.put_cf(cf, key, &m.write_to_bytes()?)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.delete(key).map_err(r2e)
    }

    pub fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        let handle = get_cf_handle(&self.db, cf)?;
        self.db.delete_cf(handle, key).map_err(r2e)
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::{CF_DEFAULT, Peekable};
    use kvproto::metapb::Region;
    use tempfile::Builder;

    use crate::util;

    #[test]
    fn test_base() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = util::new_engine(path.path().to_str().unwrap(), &[CF_DEFAULT, cf]).unwrap();

        let mut r = Region::default();
        r.set_id(10);

        let key = b"key";
        engine.put_msg(key, &r).unwrap();
        engine.put_msg_cf(cf, key, &r).unwrap();

        let snap = engine.snapshot();

        let mut r1: Region = engine.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r1);
        let r1_cf: Region = engine.get_msg_cf(cf, key).unwrap().unwrap();
        assert_eq!(r, r1_cf);

        let mut r2: Region = snap.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r2);
        let r2_cf: Region = snap.get_msg_cf(cf, key).unwrap().unwrap();
        assert_eq!(r, r2_cf);

        r.set_id(11);
        engine.put_msg(key, &r).unwrap();
        r1 = engine.get_msg(key).unwrap().unwrap();
        r2 = snap.get_msg(key).unwrap().unwrap();
        assert_ne!(r1, r2);

        let b: Option<Region> = engine.get_msg(b"missing_key").unwrap();
        assert!(b.is_none());
    }

    #[test]
    fn test_peekable() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = util::new_engine(path.path().to_str().unwrap(), &[CF_DEFAULT, cf]).unwrap();

        engine.put(b"k1", b"v1").unwrap();
        engine.put_cf(cf, b"k1", b"v2").unwrap();

        assert_eq!(&*engine.get_value(b"k1").unwrap().unwrap(), b"v1");
        engine.get_value_cf("foo", b"k1").unwrap_err();
        assert_eq!(&*engine.get_value_cf(cf, b"k1").unwrap().unwrap(), b"v2");
    }
}
