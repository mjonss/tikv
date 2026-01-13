// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ffi::OsStr,
    fs,
    fs::DirEntry,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use tikv_util::{box_err, config::ReadableDuration, time::Instant};

use crate::{
    Error as KvError, IoContext,
    ia::{
        ia_file::parse_table_meta_filename,
        manager::IaManager,
        types::{FileSegmentIdent, SEGMENT_LOCAL_FILE_SUFFIX, TABLE_META_LOCAL_FILE_SUFFIX},
        util::TEMPORARY_FILE_SUFFIX,
    },
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IA GC error {0}")]
    IaGc(#[from] Box<dyn std::error::Error + Send + Sync>),
    // #[error("IA GC IO error {0}")]
    // Io(#[from] io::Error),
    #[error("IA GC IO error {0}")]
    Io(#[from] crate::Error),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::Io(KvError::Io {
            err,
            ctx: "".to_string(),
        })
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct IaGcConfig {
    pub meta_lifetime: ReadableDuration,
    pub segment_interval: ReadableDuration,
    pub tmp_lifetime: ReadableDuration,
}

impl Default for IaGcConfig {
    fn default() -> Self {
        Self {
            meta_lifetime: ReadableDuration::days(1),
            segment_interval: ReadableDuration::days(1),
            tmp_lifetime: ReadableDuration::minutes(1),
        }
    }
}

#[cfg(any(test, feature = "testexport"))]
impl IaGcConfig {
    pub fn new_for_test() -> Self {
        Self {
            meta_lifetime: ReadableDuration::secs(30),
            segment_interval: ReadableDuration::secs(0), // Run on every `interval`.
            tmp_lifetime: ReadableDuration::secs(10),
        }
    }
}

macro_rules! try_exists {
    ($e:expr) => {{
        match $e {
            Ok(t) => Ok(t),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(false);
            }
            Err(err) => Err(err),
        }
    }};
}

pub struct IaGcRunner {
    config: IaGcConfig,
    ia_mgr: IaManager,

    meta_paths: Arc<Vec<PathBuf>>,

    segment_paths: Vec<PathBuf>,
    segment_last_gc_time: Instant,
}

impl IaGcRunner {
    pub fn new(config: IaGcConfig, ia_mgr: IaManager, meta_paths: Arc<Vec<PathBuf>>) -> Self {
        info!("create IaGcRunner"; "config" => ?config);
        let segment_paths = ia_mgr.main_store_paths().to_vec();
        Self {
            config,
            ia_mgr,
            meta_paths,
            segment_paths,
            segment_last_gc_time: Instant::now_coarse(),
        }
    }

    pub fn run(&mut self, ignore: impl Fn(u64) -> bool) {
        if let Err(err) = self.meta_file_gc(&ignore) {
            error!("ia gc: meta file gc failed"; "err" => ?err);
            debug_assert!(false, "meta file gc failed: {:?}", err);
        }
        if let Err(err) = self.segment_gc(ignore) {
            error!("ia gc: segment gc failed"; "err" => ?err);
            debug_assert!(false, "segment gc failed: {:?}", err);
        }
    }

    pub fn meta_file_gc(&mut self, ignore: impl Fn(u64) -> bool) -> Result<usize> {
        let mut removed_count = 0;
        for meta_path in self.meta_paths.iter() {
            removed_count += Self::walk_dir(
                meta_path,
                &[TABLE_META_LOCAL_FILE_SUFFIX, TEMPORARY_FILE_SUFFIX],
                |extension, path, entry| {
                    if extension == TABLE_META_LOCAL_FILE_SUFFIX {
                        self.handle_meta_file(path, &entry, &ignore)
                    } else if extension == TEMPORARY_FILE_SUFFIX {
                        self.handle_temp(path, &entry)
                    } else {
                        unreachable!()
                    }
                },
            )?;
        }
        Ok(removed_count)
    }

    fn handle_meta_file(
        &self,
        path: &Path,
        entry: &DirEntry,
        ignore: impl Fn(u64) -> bool,
    ) -> Result<bool /* is_removed */> {
        let filename = path
            .file_name()
            .ok_or_else(|| -> Error { box_err!("no filename: {path:?}") })?;
        let filename = filename.to_string_lossy();
        let Some((file_id, _)) = parse_table_meta_filename(filename.as_ref()) else {
            debug_assert!(false, "invalid filename of table meta: {:?}", path);
            warn!("ia gc: invalid filename of table meta"; "path" => ?path);
            return Ok(false);
        };
        if ignore(file_id) {
            return Ok(false);
        }
        let metadata = try_exists!(entry.metadata()).ctx("gc.metadata")?;
        let modified_dur = metadata
            .modified()
            .ctx("gc.modified")?
            .elapsed()
            .unwrap_or_default();
        if modified_dur > self.config.meta_lifetime.0 {
            self.ia_mgr.remove_table_meta(file_id);
            let is_removed = self
                .remove_file(path)
                .with_ctx(|| format!("remove_meta.{}", path.display()))?;
            debug_assert!(is_removed, "meta file not removed: {}:{:?}", file_id, path);
            return Ok(is_removed);
        }
        Ok(false)
    }

    pub fn segment_gc(&mut self, ignore: impl Fn(u64) -> bool) -> Result<usize> {
        if self.segment_paths.is_empty() {
            return Ok(0);
        };
        if self.segment_last_gc_time.saturating_elapsed() < self.config.segment_interval.0 {
            return Ok(0);
        }

        self.segment_last_gc_time = Instant::now_coarse();
        let mut removed_count = 0;
        for segment_path in self.segment_paths.iter() {
            removed_count += Self::walk_dir(
                segment_path,
                &[SEGMENT_LOCAL_FILE_SUFFIX, TEMPORARY_FILE_SUFFIX],
                |extension, path, entry| {
                    if extension == SEGMENT_LOCAL_FILE_SUFFIX {
                        self.handle_segment(path, &ignore)
                    } else if extension == TEMPORARY_FILE_SUFFIX {
                        self.handle_temp(path, &entry)
                    } else {
                        unreachable!()
                    }
                },
            )?;
        }
        Ok(removed_count)
    }

    fn handle_segment(
        &self,
        path: &Path,
        ignore: impl Fn(u64) -> bool,
    ) -> Result<bool /* is_removed */> {
        let file_name = path
            .file_name()
            .ok_or_else(|| -> Error { box_err!("gc.file_name.{path:?}") })?;
        let file_name = file_name.to_string_lossy();
        let Some(segment_ident) = FileSegmentIdent::parse_local_filename(file_name.as_ref()) else {
            debug_assert!(false, "illegal segment file name: {:?}", file_name.as_ref());
            return Ok(false);
        };
        if ignore(segment_ident.file_id) {
            return Ok(false);
        }
        if !self.ia_mgr.contains_segment(segment_ident) {
            warn!("ia gc: segment file is leaked"; "path" => ?path, "segment" => %segment_ident);
            let is_removed = self
                .remove_file(path)
                .with_ctx(|| format!("remove_segment.{}", path.display()))?;
            return Ok(is_removed);
        }
        Ok(false)
    }

    fn handle_temp(&self, path: &Path, entry: &DirEntry) -> Result<bool /* is_removed */> {
        let metadata = try_exists!(entry.metadata()).ctx("gc.metadata")?;
        let modified_dur = metadata
            .modified()
            .ctx("gc.modified")?
            .elapsed()
            .unwrap_or_default();
        if modified_dur >= self.config.tmp_lifetime.0 {
            let is_removed = self
                .remove_file(path)
                .with_ctx(|| format!("remove_seg_tmp.{}", path.display()))?;
            return Ok(is_removed);
        }
        Ok(false)
    }

    fn remove_file(&self, path: &Path) -> io::Result<bool /* is_removed */> {
        info!("ia gc: remove file {:?}", path);
        match fs::remove_file(path) {
            Ok(()) => Ok(true),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                // Removed by segment eviction of IA manager.
                info!("ia gc: remove file not found: {:?}", path);
                Ok(false)
            }
            Err(err) => Err(err),
        }
    }

    fn walk_dir<F>(dir: &Path, extensions: &[&str], mut f: F) -> Result<usize>
    where
        F: FnMut(&OsStr /* extension */, &Path, DirEntry) -> Result<bool>,
    {
        let mut removed_count = 0;
        let entries = fs::read_dir(dir).with_ctx(|| format!("gc.read_dir.{dir:?}"))?;
        for e in entries {
            let entry = e.ctx("gc.entry")?;
            let path = entry.path();
            let Some(ext) = path.extension() else {
                continue;
            };
            if extensions.iter().any(|&x| x == ext) {
                match f(ext, &path, entry) {
                    Ok(is_removed) => removed_count += is_removed as usize,
                    Err(err) => {
                        error!("ia gc: handle file failed"; "err" => ?err, "path" => ?path);
                        debug_assert!(false, "handle file failed: {:?}: {:?}", path, err);
                    }
                }
            }
        }
        Ok(removed_count)
    }

    // For test purpose.
    #[cfg(any(test, feature = "testexport"))]
    pub fn set_segment_path(&mut self, paths: Vec<PathBuf>) {
        self.segment_paths = paths;
    }
}
