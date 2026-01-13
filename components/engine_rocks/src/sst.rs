// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{CF_DEFAULT, Error, IterOptions, Result, SstCompressionType, SstMetaInfo};
use fail::fail_point;
use kvproto::import_sstpb::SstMeta;
use rocksdb::{
    ColumnFamilyOptions, DB, DBCompressionType, DBIterator, Env, EnvOptions,
    ExternalSstFileInfo as RawExternalSstFileInfo, SequentialFile, SstFileReader, SstFileWriter,
    rocksdb::supported_compression,
};

use crate::{engine::RocksEngine, options::RocksReadOptions, r2e};

pub struct RocksSstReader {
    inner: SstFileReader,
}

impl RocksSstReader {
    pub fn sst_meta_info(&self, sst: SstMeta) -> SstMetaInfo {
        let mut meta = SstMetaInfo {
            total_kvs: 0,
            total_bytes: 0,
            meta: sst,
        };
        self.inner.read_table_properties(|p| {
            meta.total_kvs = p.num_entries();
            meta.total_bytes = p.raw_key_size() + p.raw_value_size();
        });
        meta
    }

    pub fn open_with_env(path: &str, env: Option<Arc<Env>>) -> Result<Self> {
        let mut cf_options = ColumnFamilyOptions::new();
        if let Some(env) = env {
            cf_options.set_env(env);
        }
        let mut reader = SstFileReader::new(cf_options);
        reader.open(path).map_err(r2e)?;
        Ok(RocksSstReader { inner: reader })
    }

    pub fn compression_name(&self) -> String {
        let mut result = String::new();
        self.inner.read_table_properties(|p| {
            result = p.compression_name().to_owned();
        });
        result
    }
}

impl RocksSstReader {
    pub fn verify_checksum(&self) -> Result<()> {
        self.inner.verify_checksum().map_err(r2e)?;
        Ok(())
    }
}

impl RocksSstReader {
    #[inline]
    pub fn iter(&self, opts: IterOptions) -> Result<RocksSstIterator<'_>> {
        let opt: RocksReadOptions = opts.into();
        let opt = opt.into_raw();
        Ok(RocksSstIterator(SstFileReader::iter_opt(&self.inner, opt)))
    }
}

pub struct RocksSstIterator<'a>(DBIterator<&'a SstFileReader>);

// It's OK to send the iterator around.
// TODO: remove this when using tirocks.
unsafe impl Send for RocksSstIterator<'_> {}

impl RocksSstIterator<'_> {
    pub fn seek(&mut self, key: &[u8]) -> Result<bool> {
        self.0.seek(rocksdb::SeekKey::Key(key)).map_err(r2e)
    }

    /// Seek to the first key in the database.
    pub fn seek_to_first(&mut self) -> Result<bool> {
        self.0.seek(rocksdb::SeekKey::Start).map_err(r2e)
    }

    /// Seek to the last key in the database.
    pub fn seek_to_last(&mut self) -> Result<bool> {
        self.0.seek(rocksdb::SeekKey::End).map_err(r2e)
    }

    pub fn next(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(r2e("Iterator invalid"));
        }
        self.0.next().map_err(r2e)
    }

    pub fn key(&self) -> &[u8] {
        self.0.key()
    }

    pub fn value(&self) -> &[u8] {
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
pub fn collect_sst(mut it: RocksSstIterator<'_>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut v = Vec::new();
    let mut it_valid = it.valid().unwrap();
    while it_valid {
        let kv = (it.key().to_vec(), it.value().to_vec());
        v.push(kv);
        it_valid = it.next().unwrap();
    }
    v
}

pub struct RocksSstWriterBuilder {
    cf: Option<String>,
    db: Option<Arc<DB>>,
    in_memory: bool,
    compression_type: Option<DBCompressionType>,
    compression_level: i32,
}

impl Default for RocksSstWriterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RocksSstWriterBuilder {
    pub fn new() -> Self {
        RocksSstWriterBuilder {
            cf: None,
            in_memory: false,
            db: None,
            compression_type: None,
            compression_level: 0,
        }
    }

    pub fn set_db(mut self, db: &RocksEngine) -> Self {
        self.db = Some(db.as_inner().clone());
        self
    }

    pub fn set_cf(mut self, cf: &str) -> Self {
        self.cf = Some(cf.to_string());
        self
    }

    pub fn set_in_memory(mut self, in_memory: bool) -> Self {
        self.in_memory = in_memory;
        self
    }

    pub fn set_compression_type(mut self, compression: Option<SstCompressionType>) -> Self {
        self.compression_type = compression.map(to_rocks_compression_type);
        self
    }

    pub fn set_compression_level(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    pub fn build(self, path: &str) -> Result<RocksSstWriter> {
        let mut env = None;
        let mut io_options = if let Some(db) = self.db.as_ref() {
            env = db.env();
            let handle = db
                .cf_handle(self.cf.as_deref().unwrap_or(CF_DEFAULT))
                .ok_or_else(|| r2e(format!("CF {:?} is not found", self.cf)))?;
            db.get_options_cf(handle)
        } else {
            ColumnFamilyOptions::new()
        };
        if self.in_memory {
            // Set memenv.
            let mem_env = Arc::new(Env::new_mem());
            io_options.set_env(mem_env.clone());
            env = Some(mem_env);
        } else if let Some(env) = env.as_ref() {
            io_options.set_env(env.clone());
        }
        let compress_type = if let Some(ct) = self.compression_type {
            let all_supported_compression = supported_compression();
            if !all_supported_compression.contains(&ct) {
                return Err(Error::Other(
                    format!(
                        "compression type '{}' is not supported by rocksdb",
                        fmt_db_compression_type(ct)
                    )
                    .into(),
                ));
            }
            ct
        } else {
            get_fastest_supported_compression_type()
        };
        // TODO: 0 is a valid value for compression_level
        if self.compression_level != 0 {
            // other 4 fields are default value.
            io_options.set_compression_options(
                -14,
                self.compression_level,
                0, // strategy
                0, // max_dict_bytes
                0, // zstd_max_train_bytes
                1, // parallel_threads
            );
        }
        io_options.compression(compress_type);
        // in rocksdb 5.5.1, SstFileWriter will try to use bottommost_compression and
        // compression_per_level first, so to make sure our specified compression type
        // being used, we must set them empty or disabled.
        io_options.compression_per_level(&[]);
        io_options.bottommost_compression(DBCompressionType::Disable);
        let mut writer = SstFileWriter::new(EnvOptions::new(), io_options);
        fail_point!("on_open_sst_writer");
        writer.open(path).map_err(r2e)?;
        Ok(RocksSstWriter { writer, env })
    }
}

pub struct RocksSstWriter {
    writer: SstFileWriter,
    env: Option<Arc<Env>>,
}

impl RocksSstWriter {
    pub fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        self.writer.put(key, val).map_err(r2e)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.writer.delete(key).map_err(r2e)
    }

    pub fn finish(mut self) -> Result<RocksExternalSstFileInfo> {
        Ok(RocksExternalSstFileInfo(self.writer.finish().map_err(r2e)?))
    }

    pub fn finish_read(mut self) -> Result<(RocksExternalSstFileInfo, SequentialFile)> {
        let env = self
            .env
            .take()
            .ok_or_else(|| r2e("failed to read sequential file no env provided"))?;
        let sst_info = self.writer.finish().map_err(r2e)?;
        let p = sst_info.file_path();
        let path = p.as_os_str().to_str().ok_or_else(|| {
            r2e(format!(
                "failed to sequential file bad path {}",
                p.display()
            ))
        })?;
        let seq_file = env
            .new_sequential_file(path, EnvOptions::new())
            .map_err(r2e)?;
        Ok((RocksExternalSstFileInfo(sst_info), seq_file))
    }
}

pub struct RocksExternalSstFileInfo(RawExternalSstFileInfo);

impl RocksExternalSstFileInfo {
    pub fn file_size(&self) -> u64 {
        self.0.file_size()
    }
}

// Zlib and bzip2 are too slow.
const COMPRESSION_PRIORITY: [DBCompressionType; 3] = [
    DBCompressionType::Lz4,
    DBCompressionType::Snappy,
    DBCompressionType::Zstd,
];

fn get_fastest_supported_compression_type() -> DBCompressionType {
    let all_supported_compression = supported_compression();
    *COMPRESSION_PRIORITY
        .iter()
        .find(|c| all_supported_compression.contains(c))
        .unwrap_or(&DBCompressionType::No)
}

fn fmt_db_compression_type(ct: DBCompressionType) -> &'static str {
    match ct {
        DBCompressionType::Lz4 => "lz4",
        DBCompressionType::Snappy => "snappy",
        DBCompressionType::Zstd => "zstd",
        _ => unreachable!(),
    }
}

fn to_rocks_compression_type(ct: SstCompressionType) -> DBCompressionType {
    match ct {
        SstCompressionType::Lz4 => DBCompressionType::Lz4,
        SstCompressionType::Snappy => DBCompressionType::Snappy,
        SstCompressionType::Zstd => DBCompressionType::Zstd,
    }
}

pub fn from_rocks_compression_type(ct: DBCompressionType) -> Option<SstCompressionType> {
    match ct {
        DBCompressionType::Lz4 => Some(SstCompressionType::Lz4),
        DBCompressionType::Snappy => Some(SstCompressionType::Snappy),
        DBCompressionType::Zstd => Some(SstCompressionType::Zstd),
        _ => None,
    }
}
