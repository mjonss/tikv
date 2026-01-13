// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{intrinsics::likely, iter::Iterator as StdIterator, ops::Deref};

use crate::{
    Iterator, SnapAccess,
    table::{
        InnerKey, Value, encode_val_to_outer_val_owner,
        memtable::{Hint, SkipList, WriteBatch},
        new_merge_iterator,
        txn_file::{OP_CHECK_NOT_EXIST, OP_LOCK, SkipOpTxnFileIterator, TxnFile, TxnFileIterator},
    },
};

/// SkipListExt integrates txn files with SkipList, committed TxnFiles are added
/// to the WriteCF, and then switched, so txn files data in a mem-table always
/// have higher commit version. The Lock CF TxnFiles are handled separately in
/// SnapData.
#[derive(Clone)]
pub struct SkipListExt {
    skl: SkipList,
    txn_files: Vec<TxnFile>,
}

impl SkipListExt {
    pub fn new(skl: SkipList) -> Self {
        Self {
            skl,
            txn_files: vec![],
        }
    }

    #[must_use]
    pub fn add_txn_files(&self, txn_files: &[TxnFile]) -> Self {
        debug!(
            "add txn_files: {:?}",
            txn_files.iter().map(|x| x.id()).collect::<Vec<_>>(),
        );
        let mut new_txn_files = Vec::with_capacity(self.txn_files.len() + txn_files.len());
        new_txn_files.extend_from_slice(txn_files);
        new_txn_files.extend_from_slice(&self.txn_files);
        new_txn_files.sort_by(|a, b| b.version().cmp(&a.version()));
        Self {
            skl: self.skl.clone(),
            txn_files: new_txn_files,
        }
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.skl_size() + self.txn_files_size()
    }

    #[inline]
    pub fn skl_size(&self) -> usize {
        self.skl.size() as usize
    }

    #[inline]
    pub fn txn_files_size(&self) -> usize {
        self.txn_files.iter().map(|t| t.size()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    pub fn put_batch(&self, batch: &mut WriteBatch, snap: Option<&SnapAccess>, cf: usize) {
        self.skl.put_batch(batch, snap, cf);
    }

    pub fn new_iterator(&self, reverse: bool) -> Box<dyn Iterator> {
        let skl_iter = Box::new(self.skl.new_iterator(reverse));
        if self.txn_files.is_empty() {
            return skl_iter;
        }
        let mut iterators: Vec<Box<dyn Iterator>> = vec![skl_iter];
        for txn_file in &self.txn_files {
            let txn_file_iter = TxnFileIterator::new(txn_file.clone(), reverse);
            let skip_op_iter = Box::new(SkipOpTxnFileIterator::new(txn_file_iter, true, true));
            iterators.push(skip_op_iter);
        }
        new_merge_iterator(iterators, reverse)
    }

    pub fn new_delta_write_iterator(&self, since_ts: u64) -> Box<dyn Iterator> {
        let skl_iter = Box::new(self.skl.new_iterator(false));
        if self.txn_files.is_empty() {
            return skl_iter;
        }
        let mut iterators: Vec<Box<dyn Iterator>> = vec![skl_iter];
        for txn_file in &self.txn_files {
            // Txn files are sorted by version in descending order.
            // See `add_txn_files`.
            if txn_file.version() > since_ts {
                let txn_file_iter = TxnFileIterator::new(txn_file.clone(), false);
                let skip_op_iter = Box::new(SkipOpTxnFileIterator::new(txn_file_iter, true, true));
                iterators.push(skip_op_iter);
            } else {
                break;
            }
        }
        new_merge_iterator(iterators, false)
    }

    fn try_get_from_txn_file(
        &self,
        key: InnerKey<'_>,
        version: u64,
        outer_owner: &mut Vec<u8>,
    ) -> Option<Value> {
        for txn_file in &self.txn_files {
            if txn_file.version() > version {
                continue;
            }
            let (op, val) = txn_file.get_value(key, outer_owner);
            if val.is_valid() && op != OP_LOCK && op != OP_CHECK_NOT_EXIST {
                return Some(val);
            }
        }
        None
    }

    #[inline]
    fn merge_skl_and_txn_file_value(
        &self,
        skl_val: Value,
        txn_val: Option<Value>,
        outer_owner: &mut Vec<u8>,
    ) -> Value {
        let skl_is_valid = skl_val.is_valid();
        if likely(txn_val.is_none()) {
            return if skl_is_valid {
                encode_val_to_outer_val_owner(skl_val, outer_owner)
            } else {
                Value::new()
            };
        }

        let txn_val = txn_val.unwrap();
        if skl_is_valid && skl_val.version > txn_val.version {
            encode_val_to_outer_val_owner(skl_val, outer_owner)
        } else {
            txn_val
        }
    }

    pub fn get_with_hint(
        &self,
        key: InnerKey<'_>,
        version: u64,
        h: &mut Hint,
        outer_owner: &mut Vec<u8>,
    ) -> Value {
        let txn_val = self.try_get_from_txn_file(key, version, outer_owner);
        let skl_val = self.skl.get_with_hint(key.deref(), version, h);
        self.merge_skl_and_txn_file_value(skl_val, txn_val, outer_owner)
    }

    pub fn get(&self, key: InnerKey<'_>, version: u64, outer_owner: &mut Vec<u8>) -> Value {
        let txn_val = self.try_get_from_txn_file(key, version, outer_owner);
        let skl_val = self.skl.get(key.deref(), version);
        self.merge_skl_and_txn_file_value(skl_val, txn_val, outer_owner)
    }

    // `get_newer` is not necessary to return the entry of latest version.
    pub fn get_newer(&self, key: InnerKey<'_>, version: u64, outer_owner: &mut Vec<u8>) -> Value {
        for txn_file in &self.txn_files {
            let (op, val) = txn_file.get_value(key, outer_owner);
            if val.is_valid() && op != OP_LOCK && op != OP_CHECK_NOT_EXIST {
                if val.version > version {
                    return val;
                }
                break;
            }
        }
        let v = self.skl.get_newer(key.deref(), version);
        if v.is_valid() {
            return encode_val_to_outer_val_owner(v, outer_owner);
        }
        Value::new()
    }

    pub fn data_max_ts(&self) -> u64 {
        let mut max_ts = self.skl.data_max_ts();
        if let Some(txn_file) = self.txn_files.first() {
            max_ts = std::cmp::max(max_ts, txn_file.version())
        }
        max_ts
    }

    pub fn get_txn_files(&self) -> Vec<TxnFile> {
        self.txn_files.clone()
    }

    pub fn get_skl(&self) -> SkipList {
        self.skl.clone()
    }

    pub fn has_over_bound_data(&self, start: InnerKey<'_>, end: InnerKey<'_>) -> bool {
        self.skl.has_over_bound_data(start, end)
            || self
                .txn_files
                .iter()
                .any(|txn_file| txn_file.has_over_bound_data(start, end))
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, sync::Arc};

    use bytes::Bytes;

    use crate::{
        GLOBAL_SHARD_END_KEY, UserMeta, WRITE_CF,
        table::{
            InnerKey,
            file::InMemFile,
            memtable::{SkipList, WriteBatch, skl_ext::SkipListExt},
            sstable::BlockCache,
            txn_file::{OP_PUT, TxnChunk, TxnChunkBuilder, TxnCtx, TxnFile, TxnFileId},
        },
        util::test_util::KeyBuilder,
    };

    const KEYSPACE_ID: u32 = 42;

    fn new_val(i: usize) -> String {
        format!("val{:05}", i)
    }

    fn write_skl_write_cf(
        skl: &SkipList,
        keys: Vec<usize>,
        start_ts: u64,
        commit_ts: u64,
        kb: &KeyBuilder,
    ) {
        let mut wb = WriteBatch::new();
        for i in keys {
            let key = kb.i_to_inner_key(i);
            let val = new_val(i);
            wb.put(
                key.as_ref(),
                0,
                &UserMeta::new(start_ts, commit_ts).to_array(),
                commit_ts,
                val.as_bytes(),
            );
        }
        skl.put_batch(&mut wb, None, WRITE_CF);
    }

    fn build_txn_file_chunk_data(chunk_id: u64, keys: Vec<usize>, kb: &KeyBuilder) -> Bytes {
        let mut batch_builder = TxnChunkBuilder::new(chunk_id, 64, None);
        for i in keys {
            let key = kb.i_to_key(i);
            let val = new_val(i);
            batch_builder.add_entry(InnerKey::from_outer_key(&key), OP_PUT, val.as_bytes());
        }
        let mut data_buf = vec![];
        batch_builder.finish(&mut data_buf);
        data_buf.into()
    }

    #[test]
    fn test_skl_ext_write_cf() {
        let skl = SkipList::new(None);
        let kb = KeyBuilder::new(KEYSPACE_ID, "t_");
        write_skl_write_cf(&skl, vec![5, 10, 15, 20], 100, 101, &kb);

        let chunk_id = 1;
        let txn_file_chunk_data = build_txn_file_chunk_data(chunk_id, vec![10, 12, 18, 20], &kb);
        let txn_file_chunk_file = Arc::new(InMemFile::new(chunk_id, txn_file_chunk_data));
        let txn_file_chunk = TxnChunk::new(txn_file_chunk_file, BlockCache::None, None).unwrap();
        let user_meta = UserMeta::new(102, 103).to_array().to_vec();
        let lower_bound = InnerKey::from_inner_buf(b"");
        let upper_bound = InnerKey::from_inner_buf(GLOBAL_SHARD_END_KEY);
        let txn_ctx = TxnCtx::new(
            user_meta.into(),
            Bytes::new(),
            103,
            lower_bound,
            upper_bound,
        );
        let txn_file_id = TxnFileId::new(1, 1, 102);
        let txn_file = TxnFile::new(txn_file_id, vec![txn_file_chunk], txn_ctx).unwrap();
        let skl_ext = SkipListExt::new(skl).add_txn_files(&[txn_file]);

        write_skl_write_cf(&skl_ext.get_skl(), vec![20], 104, 105, &kb);

        // test find key in skl.
        let key = kb.i_to_inner_key(5);
        let mut outer_key_owner = vec![];
        let val = skl_ext.get(key.as_ref(), u64::MAX, &mut outer_key_owner);
        assert_eq!(val.get_value(), new_val(5).as_bytes());
        assert_eq!(val.user_meta(), &UserMeta::new(100, 101).to_array());
        assert_eq!(val.version, 101);

        // test find key in txn file.
        let key = kb.i_to_inner_key(10);
        let mut outer_key_owner = vec![];
        let val = skl_ext.get(key.as_ref(), u64::MAX, &mut outer_key_owner);
        assert_eq!(val.get_value(), new_val(10).as_bytes());
        assert_eq!(val.user_meta(), &UserMeta::new(102, 103).to_array());
        assert_eq!(val.version, 103);

        // test find key in skl newer than txn file.
        let key = kb.i_to_inner_key(20);
        let mut outer_key_owner = vec![];
        let val = skl_ext.get(key.as_ref(), u64::MAX, &mut outer_key_owner);
        assert_eq!(val.get_value(), new_val(20).as_bytes());
        assert_eq!(val.user_meta(), &UserMeta::new(104, 105).to_array());
        assert_eq!(val.version, 105);

        // test get newer.
        let key = kb.i_to_inner_key(10);
        let mut outer_key_owner = vec![];
        let mut val = skl_ext.get_newer(key.as_ref(), 102, &mut outer_key_owner);
        assert!(val.is_valid());
        assert_eq!(val.version, 103);
        val = skl_ext.get_newer(key.as_ref(), 102, &mut outer_key_owner);
        assert!(val.is_valid());
        assert_eq!(val.version, 103);
        val = skl_ext.get_newer(key.as_ref(), 103, &mut outer_key_owner);
        assert!(!val.is_valid());

        let key = kb.i_to_inner_key(20);
        val = skl_ext.get_newer(key.as_ref(), 102, &mut outer_key_owner);
        assert!(val.is_valid());
        assert_eq!(val.version, 103); // not necessary to return the latest version.
        val = skl_ext.get_newer(key.as_ref(), 104, &mut outer_key_owner);
        assert!(val.is_valid());
        assert_eq!(val.version, 105);

        // test iterator.
        let mut iter = skl_ext.new_iterator(false);
        let mut count = 0;
        iter.rewind();
        let mut prev_key = vec![];
        let mut prev_version = 0;
        while iter.valid() {
            count += 1;
            let key = iter.key();
            if prev_key.as_slice() == key.deref() {
                assert!(prev_version > iter.value().version)
            } else {
                assert!(prev_key.as_slice() < key.deref());
            }
            prev_key = key.to_vec();
            prev_version = iter.value().version;
            iter.next_all_version();
        }
        assert_eq!(count, 9);
    }

    #[test]
    fn test_get_newer_exclude_equal_ts() {
        let skl = SkipList::new(None);
        let kb = KeyBuilder::new(KEYSPACE_ID, "t_");
        let key = kb.i_to_key(1);

        write_skl_write_cf(&skl, vec![1], 100, 101, &kb);
        let val = skl.get_newer(&key, 101);
        assert!(val.is_empty());
        let val = skl.get_newer(&key, 100);
        assert!(val.is_valid());
        assert_eq!(val.version, 101);

        write_skl_write_cf(&skl, vec![1], 110, 110, &kb);
        let val = skl.get_newer(&key, 110);
        assert!(val.is_empty());
        let val = skl.get_newer(&key, 109);
        assert!(val.is_valid());
        assert_eq!(val.version, 110);
    }
}
