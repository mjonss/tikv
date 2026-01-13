// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use engine_rocks::RocksCfOptions;
use engine_traits::{ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE, CfName};
use file_system::IoRateLimiter;
use kvproto::kvrpcpb::ApiVersion;
use tikv_util::config::ReadableSize;

use crate::storage::{
    config::BlockCacheConfig,
    kv::{Result, RocksEngine},
};

// Duplicated from rocksdb_engine
const TEMP_DIR: &str = "";

/// A builder to build a temporary `RocksEngine`.
///
/// Only used for test purpose.
#[must_use]
pub struct TestEngineBuilder {
    path: Option<PathBuf>,
    cfs: Option<Vec<CfName>>,
    io_rate_limiter: Option<Arc<IoRateLimiter>>,
    api_version: ApiVersion,
}

impl TestEngineBuilder {
    pub fn new() -> Self {
        Self {
            path: None,
            cfs: None,
            io_rate_limiter: None,
            api_version: ApiVersion::V1,
        }
    }

    /// Customize the data directory of the temporary engine.
    ///
    /// By default, TEMP_DIR will be used.
    pub fn path(mut self, path: impl AsRef<Path>) -> Self {
        self.path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Customize the CFs that engine will have.
    ///
    /// By default, engine will have all CFs.
    pub fn cfs(mut self, cfs: impl AsRef<[CfName]>) -> Self {
        self.cfs = Some(cfs.as_ref().to_vec());
        self
    }

    pub fn api_version(mut self, api_version: ApiVersion) -> Self {
        self.api_version = api_version;
        self
    }

    pub fn io_rate_limiter(mut self, limiter: Option<Arc<IoRateLimiter>>) -> Self {
        self.io_rate_limiter = limiter;
        self
    }

    /// Build a `RocksEngine`.
    pub fn build(self) -> Result<RocksEngine> {
        let cfg_rocksdb = crate::config::DbConfig::default();
        self.do_build(&cfg_rocksdb, true)
    }

    pub fn build_with_cfg(self, cfg_rocksdb: &crate::config::DbConfig) -> Result<RocksEngine> {
        self.do_build(cfg_rocksdb, true)
    }

    pub fn build_without_cache(self) -> Result<RocksEngine> {
        let cfg_rocksdb = crate::config::DbConfig::default();
        self.do_build(&cfg_rocksdb, false)
    }

    fn do_build(
        self,
        cfg_rocksdb: &crate::config::DbConfig,
        enable_block_cache: bool,
    ) -> Result<RocksEngine> {
        let path = match self.path {
            None => TEMP_DIR.to_owned(),
            Some(p) => p.to_str().unwrap().to_owned(),
        };
        let api_version = self.api_version;
        let cfs = self.cfs.unwrap_or_else(|| ALL_CFS.to_vec());
        let mut cache_opt = BlockCacheConfig::default();
        if !enable_block_cache {
            cache_opt.capacity = Some(ReadableSize::kb(0));
        }
        let cache = cache_opt.build_shared_cache();
        let cfs_opts = cfs
            .iter()
            .map(|cf| match *cf {
                CF_DEFAULT => (
                    CF_DEFAULT,
                    cfg_rocksdb.defaultcf.build_opt(&cache, None, api_version),
                ),
                CF_LOCK => (CF_LOCK, cfg_rocksdb.lockcf.build_opt(&cache)),
                CF_WRITE => (CF_WRITE, cfg_rocksdb.writecf.build_opt(&cache, None)),
                CF_RAFT => (CF_RAFT, cfg_rocksdb.raftcf.build_opt(&cache)),
                _ => (*cf, RocksCfOptions::default()),
            })
            .collect();
        let engine =
            RocksEngine::new(&path, None, cfs_opts, cache.is_some(), self.io_rate_limiter)?;
        Ok(engine)
    }
}

impl Default for TestEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::IterOptions;
    use tikv_kv::tests::*;
    use txn_types::Key;

    use super::{
        super::{CfStatistics, Engine, Snapshot, TEST_ENGINE_CFS},
        *,
    };
    use crate::storage::{Cursor, ScanMode};

    #[test]
    fn test_rocksdb() {
        let mut engine = TestEngineBuilder::new()
            .cfs(TEST_ENGINE_CFS)
            .build()
            .unwrap();
        test_base_curd_options(&mut engine)
    }

    #[test]
    fn test_rocksdb_linear() {
        let mut engine = TestEngineBuilder::new()
            .cfs(TEST_ENGINE_CFS)
            .build()
            .unwrap();
        test_linear(&mut engine);
    }

    #[test]
    fn test_rocksdb_statistic() {
        let mut engine = TestEngineBuilder::new()
            .cfs(TEST_ENGINE_CFS)
            .build()
            .unwrap();
        test_cfs_statistics(&mut engine);
    }

    #[test]
    fn rocksdb_reopen() {
        let dir = tempfile::Builder::new()
            .prefix("rocksdb_test")
            .tempdir()
            .unwrap();
        {
            let engine = TestEngineBuilder::new()
                .path(dir.path())
                .cfs(TEST_ENGINE_CFS)
                .build()
                .unwrap();
            must_put_cf(&engine, "cf", b"k", b"v1");
        }
        {
            let mut engine = TestEngineBuilder::new()
                .path(dir.path())
                .cfs(TEST_ENGINE_CFS)
                .build()
                .unwrap();
            assert_has_cf(&mut engine, "cf", b"k", b"v1");
        }
    }

    #[test]
    fn test_max_skippable_internal_keys_error() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        must_put(&engine, b"foo", b"bar");
        must_delete(&engine, b"foo");
        must_put(&engine, b"foo1", b"bar1");
        must_delete(&engine, b"foo1");
        must_put(&engine, b"foo2", b"bar2");

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut iter_opt = IterOptions::default();
        iter_opt.set_max_skippable_internal_keys(1);
        let mut iter = Cursor::new(
            snapshot.iter(CF_DEFAULT, iter_opt).unwrap(),
            ScanMode::Forward,
            false,
        );

        let mut statistics = CfStatistics::default();
        let res = iter.seek(&Key::from_raw(b"foo"), &mut statistics);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("Result incomplete: Too many internal keys skipped")
        );
    }
}
