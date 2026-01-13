// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ffi::OsStr,
    fmt::{Display, Formatter, Write},
    fs,
    fs::Metadata,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use collections::HashSet;
use kvengine::{
    IoContext,
    context::IaCtx,
    ia::gc::{IaGcConfig, IaGcRunner},
    table::sstable,
};
use kvproto::import_sstpb::SwitchMode;
use sst_importer::SstImporter;
use tikv_util::{
    error, info, warn,
    worker::{Runnable, RunnableWithTimer},
};

const SCHEMA_FILE_SUFFIX: &str = ".schema";
const VECTOR_INDEX_FILE_SUFFIX: &str = ".vec";

pub struct GcTask {}

impl Display for GcTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GcTask")
    }
}

/// Collect file ids that should be gc.
#[derive(Default)]
pub struct CollectFileIds {
    pub sst_file_ids: HashSet<u64>,
    pub ia_file_ids: HashSet<u64>,
    pub blacklist_file_ids: Arc<HashSet<u64>>,
    pub txn_chunk_ids: HashSet<u64>,
    pub col_file_ids: HashSet<u64>,
    pub schema_file_ids: HashSet<u64>,
    pub vec_idx_file_ids: HashSet<u64>,
}

/// The GC worker periodically removes unused sst files to release storage
/// resource.
pub struct GcRunner {
    kv: kvengine::Engine,
    ia_gc_runner: Option<IaGcRunner>,
    importer: Option<Arc<SstImporter>>,
    timeout: Duration,
}

impl Runnable for GcRunner {
    type Task = GcTask;

    fn run(&mut self, _: GcTask) {
        if let Err(err) = self.gc_kv_files() {
            error!("local file gc kv files failed {:?}", err);
        }
        if let Err(err) = self.gc_importer_files() {
            error!("local file gc importer files failed {:?}", err);
        }
    }
}

impl RunnableWithTimer for GcRunner {
    fn on_timeout(&mut self) {
        self.run(GcTask {});
    }

    fn get_interval(&self) -> Duration {
        // Use `timeout / 2` for easier.
        self.timeout / 2
    }
}

impl GcRunner {
    // The GC interval is `timeout / 2` if run with timer.
    pub fn new(
        kv: kvengine::Engine,
        importer: Option<Arc<SstImporter>>,
        timeout: Duration,
    ) -> Self {
        let ia_gc_runner = match kv.ia_ctx() {
            IaCtx::Enabled(ia_mgr, meta_paths) => {
                #[cfg_attr(not(feature = "testexport"), allow(unused_mut))]
                let mut config = IaGcConfig::default();

                // For test purpose:
                #[cfg(feature = "testexport")]
                if timeout <= Duration::from_secs(60) {
                    config = IaGcConfig::new_for_test();
                    config.meta_lifetime = tikv_util::config::ReadableDuration(timeout);
                    warn!("IA gc runner use test config: {:?}", config);
                }

                Some(IaGcRunner::new(config, ia_mgr.clone(), meta_paths.clone()))
            }
            IaCtx::Disabled => None,
        };
        info!("{} start gc runner", kv.get_engine_id(); "timeout" => ?timeout);
        Self {
            kv,
            ia_gc_runner,
            importer,
            timeout,
        }
    }

    fn gc_kv_files(&mut self) -> kvengine::Result<()> {
        let collect_file_ids = self.collect_file_ids();
        self.remove_garbage_files(&collect_file_ids)?;
        Ok(())
    }

    fn collect_file_ids(&self) -> CollectFileIds {
        loop {
            if let Some(collect_file_ids) = self.try_collect_file_ids() {
                return collect_file_ids;
            }
        }
    }

    fn try_collect_file_ids(&self) -> Option<CollectFileIds> {
        let mut collect_file_ids = CollectFileIds::default();
        let shard_id_vers = self.kv.get_all_shard_id_vers();
        collect_file_ids.blacklist_file_ids = self.kv.get_files_in_blacklist();
        for &id_ver in &shard_id_vers {
            let shard = self.kv.get_shard_with_ver(id_ver.id, id_ver.ver).ok()?;
            let (sst_files, ia_files) = shard.get_local_sst_files();
            collect_file_ids.sst_file_ids.extend(sst_files);
            collect_file_ids.ia_file_ids.extend(ia_files);
            collect_file_ids
                .txn_chunk_ids
                .extend(shard.get_txn_chunks());
            collect_file_ids
                .col_file_ids
                .extend(shard.get_all_col_files());
            shard
                .get_schema_file()
                .map(|f| collect_file_ids.schema_file_ids.insert(f.get_file_id()));
            collect_file_ids
                .vec_idx_file_ids
                .extend(shard.get_all_vec_idx_files());
        }
        Some(collect_file_ids)
    }

    /// Parse file id from file name. filename is like
    /// 0000000000000000000.sst/.col/.vec/.tmp/.schema
    /// suffix_len is 4 for .sst/.col/.vec/.tmp
    /// suffix_len is 7 for .schema
    fn parse_file_id(path: &Path, suffix_len: usize) -> kvengine::Result<u64> {
        let key = path.file_name().unwrap().to_str().unwrap();
        if key.len() < 20 {
            return Err(kvengine::Error::Other(
                format!("invalid file name {}", key).into(),
            ));
        }
        let end_idx = key.len() - suffix_len;
        let start_idx = end_idx - 16;
        let file_part = &key[start_idx..end_idx];
        u64::from_str_radix(file_part, 16)
            .map_err(|_| kvengine::Error::Other(format!("invalid file name {}", key).into()))
    }

    fn remove_garbage_files(&mut self, collect_file_ids: &CollectFileIds) -> kvengine::Result<()> {
        let CollectFileIds {
            sst_file_ids,
            ia_file_ids,
            blacklist_file_ids,
            txn_chunk_ids,
            col_file_ids,
            schema_file_ids,
            vec_idx_file_ids,
        } = collect_file_ids;
        let store_id = self.kv.get_engine_id();
        let opts = self.kv.opts.clone();
        for dir in &opts.local_dirs {
            let entries = fs::read_dir(dir).ctx("gc.read_dir")?;
            for e in entries {
                let entry = e.ctx("gc.entry")?;
                let path = entry.path();

                if path.is_dir() && path.file_name() == Some(OsStr::new("txn")) {
                    self.remove_kv_garbage_txn_files(
                        path,
                        txn_chunk_ids,
                        blacklist_file_ids.as_ref(),
                    )?;
                    continue;
                }
                if path.is_dir() && path.file_name() == Some(OsStr::new("ia")) {
                    self.remove_kv_garbage_ia_files(
                        path,
                        ia_file_ids,
                        col_file_ids,
                        blacklist_file_ids,
                    )?;
                    continue;
                }

                let path_str = path.to_str().unwrap();
                if path_str.ends_with(".tmp") {
                    let meta = entry
                        .metadata()
                        .with_ctx(|| format!("gc.tmp.metadata: {path_str}"))?;
                    if !self.is_old_file(meta) {
                        continue;
                    }
                    Self::remove_file(store_id, &path)
                        .with_ctx(|| format!("gc.tmp.remove_file: {path_str}"))?;
                } else if path_str.ends_with(".sst") {
                    let id = sstable::parse_file_id(&path)?;
                    if !sst_file_ids.contains(&id) {
                        let _guard = self.kv.lock_file(id);
                        if blacklist_file_ids.contains(&id) {
                            continue;
                        }
                        let meta = fs::metadata(&path).table_ctx(id, "gc.sst.metadata")?;
                        if self.is_old_file(meta) {
                            self.kv.remove_fd_cache(id);
                            Self::remove_file(store_id, &path)
                                .table_ctx(id, "gc.sst.remove_file")?;
                        }
                    }
                } else if path_str.ends_with(SCHEMA_FILE_SUFFIX) {
                    let id = Self::parse_file_id(&path, SCHEMA_FILE_SUFFIX.len())?;
                    if !schema_file_ids.contains(&id) {
                        let _guard = self.kv.lock_file(id);
                        if blacklist_file_ids.contains(&id) {
                            continue;
                        }
                        let meta = fs::metadata(&path).table_ctx(id, "gc.schema.metadata")?;
                        if self.is_old_file(meta) {
                            self.kv.remove_fd_cache(id);
                            self.kv.remove_schema_file(id);
                            Self::remove_file(store_id, &path)
                                .table_ctx(id, "gc.schema.remove_file")?;
                        }
                    }
                } else if path_str.ends_with(VECTOR_INDEX_FILE_SUFFIX) {
                    let id = Self::parse_file_id(&path, VECTOR_INDEX_FILE_SUFFIX.len())?;
                    if !vec_idx_file_ids.contains(&id) {
                        let _guard = self.kv.lock_file(id);
                        if blacklist_file_ids.contains(&id) {
                            continue;
                        }
                        let meta = fs::metadata(&path).table_ctx(id, "gc.vec.metadata")?;
                        if self.is_old_file(meta) {
                            self.kv.remove_fd_cache(id);
                            Self::remove_file(store_id, &path)
                                .table_ctx(id, "gc.vec.remove_file")?;
                        }
                    }
                } else if !path_str.ends_with("LOCK") {
                    warn!("unexpected file {:?}", path);
                }
            }
        }
        Ok(())
    }

    fn remove_file(store_id: u64, file: &Path) -> std::io::Result<()> {
        info!("{} local file GC remove file {:?}", store_id, file);
        fs::remove_file(file)
    }

    fn gc_importer_files(&self) -> sst_importer::Result<()> {
        let Some(importer) = &self.importer else {
            return Ok(());
        };
        if importer.get_mode() == SwitchMode::Import {
            return Ok(());
        }
        let store_id = self.kv.get_engine_id();
        let ssts = importer.list_ssts()?;
        for sst_meta in &ssts {
            let path = importer.get_path(sst_meta);
            let meta = fs::metadata(&path)?;
            if self.is_old_file(meta) {
                importer.delete(sst_meta)?;
                let mut uuid = String::new();
                for &b in sst_meta.get_uuid() {
                    write!(uuid, "{:X}", b).expect("Unable to write");
                }
                info!(
                    "{} gc runner delete sst uuid {} file {:?} timeout {:?}",
                    store_id, uuid, path, self.timeout
                );
            }
        }
        Ok(())
    }

    fn remove_kv_garbage_txn_files(
        &self,
        txn_path: PathBuf,
        kv_txn_chunk_ids: &HashSet<u64>,
        blacklist_file_ids: &HashSet<u64>,
    ) -> kvengine::Result<()> {
        let store_id = self.kv.get_engine_id();
        let entries = fs::read_dir(txn_path.as_path()).ctx("gc.txn.read_dir")?;
        let txn_chunk_manager = self.kv.get_txn_chunk_manager();
        for e in entries {
            let entry = e.ctx("gc.txn.entry")?;
            let path = entry.path();
            let meta = entry
                .metadata()
                .with_ctx(|| format!("gc.txn.metadata: {path:?}"))?;
            let filename = path
                .file_name()
                .unwrap_or_default()
                .to_str()
                .unwrap_or_default();
            if filename.ends_with(".tmp") {
                if !self.is_old_file(meta) {
                    continue;
                }
                Self::remove_file(store_id, &path)
                    .with_ctx(|| format!("gc.txn.tmp.remove_file: {path:?}"))?;
            } else if filename.ends_with(".txn") {
                if let Some(id) = kvengine::txn_chunk_manager::parse_txn_chunk_id(filename) {
                    if !kv_txn_chunk_ids.contains(&id)
                        && !blacklist_file_ids.contains(&id)
                        && txn_chunk_manager.gc_chunk_file(id, self.timeout, store_id)
                    {
                        info!("{} local file GC remove txn chunk", store_id; "filename" => filename, "id" => id);
                    }
                } else {
                    warn!("failed to parse txn chunk id {:?}", path);
                }
            } else {
                warn!("unexpected file {:?}", path);
            }
        }
        Ok(())
    }

    fn remove_kv_garbage_ia_files(
        &mut self,
        _: PathBuf,
        ia_file_ids: &HashSet<u64>,
        col_file_ids: &HashSet<u64>,
        blacklist_file_ids: &HashSet<u64>,
    ) -> kvengine::Result<()> {
        let Some(ia_gc_runner) = self.ia_gc_runner.as_mut() else {
            return Ok(());
        };

        let ignore = |file_id| {
            ia_file_ids.contains(&file_id)
                || col_file_ids.contains(&file_id)
                || blacklist_file_ids.contains(&file_id)
        };
        ia_gc_runner.run(ignore);
        Ok(())
    }

    fn is_old_file(&self, meta: Metadata) -> bool {
        let modified = meta.modified().unwrap();
        let dur = modified.elapsed().unwrap_or_default();
        dur > self.timeout
    }
}
