// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use collections::HashMap;
use engine_traits::{CF_DEFAULT, CF_WRITE, IterOptions};
use kvengine::{
    ShardMeta, UserMeta,
    table::{InnerKey, Value, table},
};
use kvproto::raft_cmdpb::RaftCmdRequest;
use sst_importer::SstImporter;
use tikv_util::{codec, info};
use txn_types::{WriteRef, WriteType};

pub(crate) fn convert_sst(
    kv: kvengine::Engine,
    importer: Arc<SstImporter>,
    req: &RaftCmdRequest,
    shard_meta: ShardMeta,
) -> crate::Result<kvenginepb::ChangeSet> {
    info!("{} convert sst {:?}", shard_meta.tag(), req);
    let region_id = req.get_header().get_region_id();
    let region_ver = req.get_header().get_region_epoch().get_version();
    let (ingest_id, entries_iter) = build_entries_iterator(&importer, req)?;
    let cs = kv.build_ingest_files(
        region_id,
        region_ver,
        Box::new(entries_iter),
        ingest_id,
        shard_meta,
    )?;
    Ok(cs)
}

#[allow(clippy::type_complexity)]
fn collect_default_values(
    importer: &SstImporter,
    req: &RaftCmdRequest,
) -> crate::Result<HashMap<(Vec<u8>, u64), Vec<u8>>> {
    let mut default_values = HashMap::default();
    for import_req in req.get_requests() {
        let sst_meta = import_req.get_ingest_sst().get_sst();
        if sst_meta.get_cf_name() != CF_DEFAULT {
            continue;
        }
        let reader = importer.get_reader(sst_meta)?;
        let mut iter = reader.iter(IterOptions::default())?;
        if iter.seek_to_first()? {
            while iter.valid()? {
                let key = parse_rocksdb_key(iter.key())?;
                default_values.insert(key, iter.value().to_vec());
                iter.next()?;
            }
        }
    }
    Ok(default_values)
}

fn build_entries_iterator(
    importer: &SstImporter,
    req: &RaftCmdRequest,
) -> crate::Result<(Vec<u8>, EntriesIterator)> {
    let default_values = Arc::new(collect_default_values(importer, req)?);
    let mut sst_metas = vec![];
    let mut total_kvs = 0;
    for import_req in req.get_requests() {
        let sst_meta = import_req.get_ingest_sst().get_sst();
        if sst_meta.get_cf_name() != CF_WRITE {
            continue;
        }
        total_kvs += sst_meta.total_kvs;
        sst_metas.push(sst_meta);
    }
    sst_metas.sort_by(|a, b| a.get_range().get_start().cmp(b.get_range().get_start()));
    let ingest_id = sst_metas.first().unwrap().get_uuid().to_vec();
    let mut entries = Vec::with_capacity(total_kvs as usize);
    for sst_meta in sst_metas {
        let reader = importer.get_reader(sst_meta)?;
        let mut iter = reader.iter(IterOptions::default())?;
        iter.seek_to_first()?;
        while iter.valid()? {
            let (key, commit_ts) = parse_rocksdb_key(iter.key())?;
            let write_ref = WriteRef::parse(iter.value())?;
            let start_ts = write_ref.start_ts.into_inner();
            let default_key = (key, start_ts);
            let default_value = default_values.get(&default_key);
            let key = default_key.0;
            let user_meta = UserMeta::new(start_ts, commit_ts);
            let val = match write_ref.write_type {
                WriteType::Put => match write_ref.short_value {
                    Some(short_val) => encode_table_value(user_meta, short_val),
                    None => encode_table_value(user_meta, default_value.unwrap()),
                },
                WriteType::Delete => encode_table_value(user_meta, &[]),
                _ => panic!("unexpected write type"),
            };
            entries.push(Entry { key, val });
            iter.next()?;
        }
    }
    Ok((ingest_id, EntriesIterator::new(entries)))
}

struct Entry {
    key: Vec<u8>,
    val: Vec<u8>,
}

fn parse_rocksdb_key(data_key: &[u8]) -> codec::Result<(Vec<u8>, u64)> {
    let origin_key = keys::origin_key(data_key);
    let key = txn_types::Key::from_encoded(origin_key.to_vec());
    let ts = key.decode_ts()?.into_inner();
    let raw_key = key.to_raw()?;
    Ok((raw_key, ts))
}

fn encode_table_value(user_meta: UserMeta, val: &[u8]) -> Vec<u8> {
    Value::encode_buf(0, &user_meta.to_array(), user_meta.commit_ts, val)
}

struct EntriesIterator {
    entries: Vec<Entry>,
    idx: usize,
}

impl EntriesIterator {
    fn new(entries: Vec<Entry>) -> Self {
        Self { entries, idx: 0 }
    }
}

impl table::Iterator for EntriesIterator {
    fn next(&mut self) {
        self.idx += 1;
    }

    fn next_version(&mut self) -> bool {
        unreachable!()
    }

    fn rewind(&mut self) {
        self.idx = 0;
    }

    fn seek(&mut self, _key: InnerKey<'_>) {
        unreachable!()
    }

    fn key(&self) -> InnerKey<'_> {
        InnerKey::from_outer_key(&self.entries[self.idx].key)
    }

    fn value(&self) -> Value {
        Value::decode(&self.entries[self.idx].val)
    }

    fn valid(&self) -> bool {
        self.idx < self.entries.len()
    }
}
