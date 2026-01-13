// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fs, path::Path, str::FromStr, sync::Arc};

use engine_traits::{CF_DEFAULT, Range, Result};
use rocksdb::{
    CColumnFamilyDescriptor, CFHandle, ColumnFamilyOptions, DB, Env, Range as RocksRange,
    SliceTransform, load_latest_options,
};
use slog_global::warn;

use crate::{
    RocksStatistics, cf_options::RocksCfOptions, db_options::RocksDbOptions, engine::RocksEngine,
    r2e, rocks_metrics_defs::*,
};

pub fn new_default_engine(path: &str) -> Result<RocksEngine> {
    new_engine(path, &[CF_DEFAULT])
}

pub fn new_engine(path: &str, cfs: &[&str]) -> Result<RocksEngine> {
    let mut db_opts = RocksDbOptions::default();
    db_opts.set_statistics(&RocksStatistics::new_titan());
    let cf_opts = cfs.iter().map(|name| (*name, Default::default())).collect();
    new_engine_opt(path, db_opts, cf_opts)
}

pub fn new_engine_opt(
    path: &str,
    db_opt: RocksDbOptions,
    cf_opts: Vec<(&str, RocksCfOptions)>,
) -> Result<RocksEngine> {
    let mut db_opt = db_opt.into_raw();
    if cf_opts.iter().all(|(name, _)| *name != CF_DEFAULT) {
        return Err(engine_traits::Error::Engine(
            engine_traits::Status::with_error(
                engine_traits::Code::InvalidArgument,
                "default cf must be specified",
            ),
        ));
    }
    let mut cf_opts: Vec<_> = cf_opts
        .into_iter()
        .map(|(name, opt)| (name, opt.into_raw()))
        .collect();

    // Creates a new db if it doesn't exist.
    if !db_exist(path) {
        db_opt.create_if_missing(true);
        db_opt.create_missing_column_families(true);

        let db = DB::open_cf(db_opt, path, cf_opts.into_iter().collect()).map_err(r2e)?;

        return Ok(RocksEngine::new(db));
    }

    db_opt.create_if_missing(false);

    // Lists all column families in current db.
    let cfs_list = DB::list_column_families(&db_opt, path).map_err(r2e)?;
    let existed: Vec<&str> = cfs_list.iter().map(|v| v.as_str()).collect();
    let needed: Vec<&str> = cf_opts.iter().map(|(name, _)| *name).collect();

    let cf_descs = if !existed.is_empty() {
        let env = match db_opt.env() {
            Some(env) => env,
            None => Arc::new(Env::default()),
        };
        // panic if OPTIONS not found for existing instance?
        let (_, tmp) = load_latest_options(path, &env, true)
            .unwrap_or_else(|e| panic!("failed to load_latest_options {:?}", e))
            .unwrap_or_else(|| panic!("couldn't find the OPTIONS file"));
        tmp
    } else {
        vec![]
    };

    for cf in &existed {
        if cf_opts.iter().all(|(name, _)| name != cf) {
            cf_opts.push((cf, ColumnFamilyOptions::default()));
        }
    }
    for (name, opt) in &mut cf_opts {
        adjust_dynamic_level_bytes(&cf_descs, name, opt);
    }

    let cfds: Vec<_> = cf_opts.into_iter().collect();
    // We have added all missing options by iterating `existed`. If two vecs still
    // have same length, then they must have same column families dispite their
    // orders. So just open db.
    if needed.len() == existed.len() && needed.len() == cfds.len() {
        let db = DB::open_cf(db_opt, path, cfds).map_err(r2e)?;
        return Ok(RocksEngine::new(db));
    }

    // Opens db.
    db_opt.create_missing_column_families(true);
    let mut db = DB::open_cf(db_opt, path, cfds).map_err(r2e)?;

    // Drops discarded column families.
    for cf in cfs_diff(&existed, &needed) {
        // We have checked it at the very beginning, so it must be needed.
        assert_ne!(cf, CF_DEFAULT);
        db.drop_cf(cf).map_err(r2e)?;
    }

    Ok(RocksEngine::new(db))
}

/// Turns "dynamic level size" off for the existing column family which was off
/// before. Column families are small, HashMap isn't necessary.
fn adjust_dynamic_level_bytes(
    cf_descs: &[CColumnFamilyDescriptor],
    name: &str,
    opt: &mut ColumnFamilyOptions,
) {
    if let Some(cf_desc) = cf_descs.iter().find(|cf_desc| cf_desc.name() == name) {
        let existed_dynamic_level_bytes =
            cf_desc.options().get_level_compaction_dynamic_level_bytes();
        if existed_dynamic_level_bytes != opt.get_level_compaction_dynamic_level_bytes() {
            warn!(
                "change dynamic_level_bytes for existing column family is danger";
                "old_value" => existed_dynamic_level_bytes,
                "new_value" => opt.get_level_compaction_dynamic_level_bytes(),
            );
        }
        opt.set_level_compaction_dynamic_level_bytes(existed_dynamic_level_bytes);
    }
}

pub fn db_exist(path: &str) -> bool {
    let path = Path::new(path);
    if !path.exists() || !path.is_dir() {
        return false;
    }
    let current_file_path = path.join("CURRENT");
    if !current_file_path.exists() || !current_file_path.is_file() {
        return false;
    }

    // If path is not an empty directory, and current file exists, we say db exists.
    // If path is not an empty directory but db has not been created,
    // `DB::list_column_families` fails and we can clean up the directory by
    // this indication.
    fs::read_dir(path).unwrap().next().is_some()
}

/// Returns a Vec of cf which is in `a' but not in `b'.
fn cfs_diff<'a>(a: &[&'a str], b: &[&str]) -> Vec<&'a str> {
    a.iter()
        .filter(|x| !b.iter().any(|y| *x == y))
        .cloned()
        .collect()
}

pub fn get_cf_handle<'a>(db: &'a DB, cf: &str) -> Result<&'a CFHandle> {
    db.cf_handle(cf)
        .ok_or_else(|| format!("cf {} not found", cf))
        .map_err(r2e)
}

pub fn range_to_rocks_range<'a>(range: &Range<'a>) -> RocksRange<'a> {
    RocksRange::new(range.start_key, range.end_key)
}

pub fn get_engine_cf_used_size(engine: &DB, handle: &CFHandle) -> u64 {
    let mut cf_used_size = engine
        .get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE)
        .expect("rocksdb is too old, missing total-sst-files-size property");
    // For memtable
    if let Some(mem_table) = engine.get_property_int_cf(handle, ROCKSDB_CUR_SIZE_ALL_MEM_TABLES) {
        cf_used_size += mem_table;
    }
    // For blob files
    if let Some(live_blob) = engine.get_property_int_cf(handle, ROCKSDB_TITANDB_LIVE_BLOB_FILE_SIZE)
    {
        cf_used_size += live_blob;
    }
    if let Some(obsolete_blob) =
        engine.get_property_int_cf(handle, ROCKSDB_TITANDB_OBSOLETE_BLOB_FILE_SIZE)
    {
        cf_used_size += obsolete_blob;
    }

    cf_used_size
}

/// Gets engine's compression ratio at given level.
pub fn get_engine_compression_ratio_at_level(
    engine: &DB,
    handle: &CFHandle,
    level: usize,
) -> Option<f64> {
    let prop = format!("{}{}", ROCKSDB_COMPRESSION_RATIO_AT_LEVEL, level);
    if let Some(v) = engine.get_property_value_cf(handle, &prop) {
        if let Ok(f) = f64::from_str(&v) {
            // RocksDB returns -1.0 if the level is empty.
            if f >= 0.0 {
                return Some(f);
            }
        }
    }
    None
}

/// Gets the number of files at given level of given column family.
pub fn get_cf_num_files_at_level(engine: &DB, handle: &CFHandle, level: usize) -> Option<u64> {
    let prop = format!("{}{}", ROCKSDB_NUM_FILES_AT_LEVEL, level);
    engine.get_property_int_cf(handle, &prop)
}

/// Gets the number of blob files at given level of given column family.
pub fn get_cf_num_blob_files_at_level(engine: &DB, handle: &CFHandle, level: usize) -> Option<u64> {
    let prop = format!("{}{}", ROCKSDB_TITANDB_NUM_BLOB_FILES_AT_LEVEL, level);
    engine.get_property_int_cf(handle, &prop)
}

/// Gets the number of immutable mem-table of given column family.
pub fn get_cf_num_immutable_mem_table(engine: &DB, handle: &CFHandle) -> Option<u64> {
    engine.get_property_int_cf(handle, ROCKSDB_NUM_IMMUTABLE_MEM_TABLE)
}

/// Gets the amount of pending compaction bytes of given column family.
pub fn get_cf_pending_compaction_bytes(engine: &DB, handle: &CFHandle) -> Option<u64> {
    engine.get_property_int_cf(handle, ROCKSDB_PENDING_COMPACTION_BYTES)
}

pub struct FixedSuffixSliceTransform {
    pub suffix_len: usize,
}

impl FixedSuffixSliceTransform {
    pub fn new(suffix_len: usize) -> FixedSuffixSliceTransform {
        FixedSuffixSliceTransform { suffix_len }
    }
}

impl SliceTransform for FixedSuffixSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        let mid = key.len() - self.suffix_len;
        let (left, _) = key.split_at(mid);
        left
    }

    fn in_domain(&mut self, key: &[u8]) -> bool {
        key.len() >= self.suffix_len
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}

pub struct FixedPrefixSliceTransform {
    pub prefix_len: usize,
}

impl FixedPrefixSliceTransform {
    pub fn new(prefix_len: usize) -> FixedPrefixSliceTransform {
        FixedPrefixSliceTransform { prefix_len }
    }
}

impl SliceTransform for FixedPrefixSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        &key[..self.prefix_len]
    }

    fn in_domain(&mut self, key: &[u8]) -> bool {
        key.len() >= self.prefix_len
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}

pub struct NoopSliceTransform;

impl SliceTransform for NoopSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        key
    }

    fn in_domain(&mut self, _: &[u8]) -> bool {
        true
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}
