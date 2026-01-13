// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use api_version::{KeyMode, KvFormat, dispatch_api_version};
use encryption::DataKeyManager;
use engine_rocks::RocksSstWriter;
use kvproto::{import_sstpb::*, kvrpcpb::ApiVersion};
use tikv_util::time::Instant;
use txn_types::{Key, TimeStamp, Write as KvWrite, WriteType, is_short_value};

use crate::{Error, Result, import_file::ImportPath, metrics::*};

#[derive(Debug)]
pub enum SstWriterType {
    Txn,
    Raw,
}

pub struct TxnSstWriter {
    default: RocksSstWriter,
    default_entries: u64,
    default_bytes: u64,
    default_path: ImportPath,
    default_meta: SstMeta,
    write: RocksSstWriter,
    write_entries: u64,
    write_bytes: u64,
    write_path: ImportPath,
    write_meta: SstMeta,
    key_manager: Option<Arc<DataKeyManager>>,
    api_version: ApiVersion,
}

impl TxnSstWriter {
    pub fn new(
        default: RocksSstWriter,
        write: RocksSstWriter,
        default_path: ImportPath,
        write_path: ImportPath,
        default_meta: SstMeta,
        write_meta: SstMeta,
        key_manager: Option<Arc<DataKeyManager>>,
        api_version: ApiVersion,
    ) -> Self {
        TxnSstWriter {
            default,
            default_path,
            default_entries: 0,
            default_bytes: 0,
            default_meta,
            write,
            write_path,
            write_entries: 0,
            write_bytes: 0,
            write_meta,
            key_manager,
            api_version,
        }
    }

    fn check_api_version<K: KvFormat>(&self, key: &[u8]) -> Result<()> {
        let mode = K::parse_key_mode(key);
        if self.api_version == ApiVersion::V2 && mode != KeyMode::Txn && mode != KeyMode::Tidb {
            return Err(Error::invalid_key_mode(
                SstWriterType::Txn,
                self.api_version,
                key,
            ));
        }
        Ok(())
    }

    pub fn write(&mut self, batch: WriteBatch) -> Result<()> {
        let start = Instant::now_coarse();

        let commit_ts = TimeStamp::new(batch.get_commit_ts());
        for m in batch.get_pairs().iter() {
            dispatch_api_version!(self.api_version, {
                self.check_api_version::<API>(m.get_key())?;
            });
            let k = Key::from_raw(m.get_key()).append_ts(commit_ts);
            self.put(k.as_encoded(), m.get_value(), m.get_op())?;
        }

        IMPORT_LOCAL_WRITE_CHUNK_DURATION_VEC
            .with_label_values(&["txn"])
            .observe(start.saturating_elapsed().as_secs_f64());
        Ok(())
    }

    fn put(&mut self, key: &[u8], value: &[u8], op: PairOp) -> Result<()> {
        let k = keys::data_key(key);
        let (_, commit_ts) = Key::split_on_ts_for(key)?;
        let w = match (op, is_short_value(value)) {
            (PairOp::Delete, _) => KvWrite::new(WriteType::Delete, commit_ts, None),
            (PairOp::Put, true) => KvWrite::new(WriteType::Put, commit_ts, Some(value.to_vec())),
            (PairOp::Put, false) => {
                self.default.put(&k, value)?;
                self.default_entries += 1;
                self.default_bytes += (k.len() + value.len()) as u64;
                KvWrite::new(WriteType::Put, commit_ts, None)
            }
        };
        let write = w.as_ref().to_bytes();
        self.write.put(&k, &write)?;
        self.write_entries += 1;
        self.write_bytes += (k.len() + write.len()) as u64;
        Ok(())
    }

    pub fn finish(self) -> Result<Vec<SstMeta>> {
        let default_meta = self.default_meta.clone();
        let write_meta = self.write_meta.clone();
        let mut metas = Vec::with_capacity(2);
        let (default_entries, write_entries) = (self.default_entries, self.write_entries);
        let (default_bytes, write_bytes) = (self.default_bytes, self.write_bytes);
        let (p1, p2) = (self.default_path.clone(), self.write_path.clone());
        let (w1, w2, key_manager) = (self.default, self.write, self.key_manager);

        if default_entries > 0 {
            w1.finish()?;
            p1.save(key_manager.as_deref())?;
            metas.push(default_meta);
        }
        if write_entries > 0 {
            w2.finish()?;
            p2.save(key_manager.as_deref())?;
            metas.push(write_meta);
        }

        info!("finish write to sst";
            "default entries" => default_entries,
            "default bytes" => default_bytes,
            "write entries" => write_entries,
            "write bytes" => write_bytes,
        );
        IMPORT_LOCAL_WRITE_KEYS_VEC
            .with_label_values(&["txn_default_cf"])
            .inc_by(default_entries);
        IMPORT_LOCAL_WRITE_BYTES_VEC
            .with_label_values(&["txn_default_cf"])
            .inc_by(default_bytes);
        IMPORT_LOCAL_WRITE_KEYS_VEC
            .with_label_values(&["txn_write_cf"])
            .inc_by(write_entries);
        IMPORT_LOCAL_WRITE_BYTES_VEC
            .with_label_values(&["txn_write_cf"])
            .inc_by(write_bytes);

        Ok(metas)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::{Config, SstImporter};

    // Return the temp dir path to avoid it drop out of the scope.
    fn new_writer<W, F: Fn(&SstImporter, SstMeta) -> Result<W>>(
        f: F,
        api_version: ApiVersion,
    ) -> (W, TempDir) {
        let mut meta = SstMeta::default();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());

        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer = SstImporter::new(&cfg, &importer_dir, None, api_version).unwrap();
        (f(&importer, meta).unwrap(), importer_dir)
    }

    #[test]
    fn test_write_txn_sst() {
        let (mut w, _handle) = new_writer(SstImporter::new_txn_writer, ApiVersion::V1);
        let mut batch = WriteBatch::default();
        let mut pairs = vec![];

        // put short value kv in write cf
        let mut pair = Pair::default();
        pair.set_key(b"k1".to_vec());
        pair.set_value(b"short_value".to_vec());
        pairs.push(pair);

        // put big value kv in default cf
        let big_value = vec![42; 256];
        let mut pair = Pair::default();
        pair.set_key(b"k2".to_vec());
        pair.set_value(big_value);
        pairs.push(pair);

        // put delete type key in write cf
        let mut pair = Pair::default();
        pair.set_key(b"k3".to_vec());
        pair.set_op(PairOp::Delete);
        pairs.push(pair);

        // generate two cf metas
        batch.set_commit_ts(10);
        batch.set_pairs(pairs.into());
        w.write(batch).unwrap();
        assert_eq!(w.write_entries, 3);
        assert_eq!(w.default_entries, 1);

        let metas = w.finish().unwrap();
        assert_eq!(metas.len(), 2);
    }

    #[test]
    fn test_txn_write_v2() {
        let (mut w, _handle) = new_writer(SstImporter::new_txn_writer, ApiVersion::V2);
        let mut batch = WriteBatch::default();
        batch.set_commit_ts(1);

        // put an invalid key
        let mut pair = Pair::default();
        pair.set_key(b"k1".to_vec());
        pair.set_value(b"short_value".to_vec());
        let pairs = vec![pair];
        batch.set_pairs(pairs.into());

        w.write(batch.clone()).unwrap_err();

        // put a valid key
        let mut pair = Pair::default();
        pair.set_key(b"xk1".to_vec());
        pair.set_value(b"short_value".to_vec());
        let pairs = vec![pair];
        batch.set_pairs(pairs.into());

        w.write(batch).unwrap();
    }
}
