// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;

use bytes::{Buf, Bytes};
use collections::HashMap;
use kvproto::kvrpcpb;
use log_wrappers::Value;
use protobuf::Message;
use tikv_util::box_err;

use crate::{
    DEL_PREFIXES_KEY, DeletePrefixes, ShardMeta, ShardTag, UserMeta,
    table::{SnapVersion, TxnFile},
};

/// A helper function to evenly distribute `total` into `count` parts.
/// Note: when `total` <= `count`, return `[1; total]`.
pub fn evenly_distribute(total: usize, count: usize) -> Vec<usize> {
    debug_assert!(count > 0);
    if total <= count {
        return vec![1; total];
    }
    let quotient = total / count;
    let remainder = total % count;
    let mut ret = vec![quotient; count];
    for i in 0..remainder {
        ret[i] += 1;
    }
    ret
}

pub fn new_l0_create_pb(
    id: u64,
    smallest: Vec<u8>,
    biggest: Vec<u8>,
    size: u32,
) -> kvenginepb::L0Create {
    let mut l0_create = kvenginepb::L0Create::new();
    l0_create.set_id(id);
    l0_create.set_smallest(smallest);
    l0_create.set_biggest(biggest);
    l0_create.set_size(size);
    l0_create
}

pub fn new_table_create_pb(
    id: u64,
    level: u32,
    cf: i32,
    smallest: Vec<u8>,
    biggest: Vec<u8>,
    meta_offset: u32,
) -> kvenginepb::TableCreate {
    let mut tbl_create = kvenginepb::TableCreate::new();
    tbl_create.set_id(id);
    tbl_create.set_level(level);
    tbl_create.set_cf(cf);
    tbl_create.set_smallest(smallest);
    tbl_create.set_biggest(biggest);
    tbl_create.set_meta_offset(meta_offset);
    tbl_create
}

pub fn new_blob_create_pb(
    id: u64,
    smallest: Vec<u8>,
    biggest: Vec<u8>,
    meta_offset: u32,
) -> kvenginepb::BlobCreate {
    let mut blob_create = kvenginepb::BlobCreate::new();
    blob_create.set_id(id);
    blob_create.set_smallest(smallest);
    blob_create.set_biggest(biggest);
    blob_create.set_meta_offset(meta_offset);
    blob_create
}

pub fn new_columnar_create_pb(
    id: u64,
    level: u32,
    smallest: Vec<u8>,
    biggest: Vec<u8>,
    meta_offset: u32,
) -> kvenginepb::ColumnarCreate {
    let mut col_create = kvenginepb::ColumnarCreate::new();
    col_create.set_id(id);
    col_create.set_level(level);
    col_create.set_smallest(smallest);
    col_create.set_biggest(biggest);
    col_create.set_meta_offset(meta_offset);
    col_create
}

pub fn new_vector_index_file_pb(
    id: u64,
    snap_version: SnapVersion,
    smallest: Vec<u8>,
    biggest: Vec<u8>,
    offset: u32,
) -> kvenginepb::VectorIndexFile {
    let mut vec_idx_file = kvenginepb::VectorIndexFile::new();
    vec_idx_file.set_id(id);
    vec_idx_file.set_snap_version(snap_version.into_inner());
    vec_idx_file.set_smallest(smallest);
    vec_idx_file.set_biggest(biggest);
    vec_idx_file.set_meta_offset(offset);
    vec_idx_file
}

#[inline]
pub fn is_same_vector_index(a: &kvenginepb::VectorIndex, b: &kvenginepb::VectorIndex) -> bool {
    a.table_id == b.table_id && a.index_id == b.index_id && a.col_id == b.col_id
}

#[inline]
pub fn is_matched_vector_index(
    a: &kvenginepb::VectorIndex,
    b: &kvenginepb::UpdateVectorIndex,
) -> bool {
    a.table_id == b.table_id && a.index_id == b.index_id && a.col_id == b.col_id
}

/// Helper for merging or splitting properties.
///
/// Currently only delete prefixes are handled.
///
/// Splitting properties is necessary for the following scenarios:
/// 1. `preprocess_pending_splits` (`build_split_pb`).
///
/// Merging properties is necessary for the following scenarios:
/// 1. `ShardMeta.commit_merge` (merge `ShardMeta` on region merge).
/// 2. `kvengine::Engine::commit_merge` (merge `Shard` on region merge).
/// 3. `restore_keyspace::BackupCluster::gather_sstables` (merge `ShardMeta` on
///    merging backup regions).
pub struct PropertiesHelper {
    del_prefixes: DeletePrefixes,

    /// Prefix of shard range, see `ShardRange::prefix()`.
    range_prefix: Vec<u8>,
    /// Whether to rewrite range prefix of `del_prefixes`.
    /// When enabled, range prefix of `del_prefixes` to be merged will be
    /// rewritten to `range_prefix`.
    rewrite_range_prefix: bool,
}

impl PropertiesHelper {
    pub fn new_from_shard_meta(meta: &ShardMeta) -> Self {
        Self::new(
            meta.get_property(DEL_PREFIXES_KEY),
            meta.range.keyspace_id,
            meta.range.keyspace_prefix().to_owned(),
        )
    }

    fn new(del_prefixes_bytes: Option<Bytes>, keyspace_id: u32, range_prefix: Vec<u8>) -> Self {
        let del_prefixes = if let Some(bs) = del_prefixes_bytes {
            DeletePrefixes::unmarshal(bs.chunk(), keyspace_id)
        } else {
            DeletePrefixes::new_with_keyspace_id(keyspace_id)
        };
        Self {
            del_prefixes,
            range_prefix,
            rewrite_range_prefix: false,
        }
    }

    pub fn set_rewrite_range_prefix(&mut self, rewrite_range_prefix: bool) {
        self.rewrite_range_prefix = rewrite_range_prefix;
    }

    pub fn merge_shard_meta(&mut self, other: &ShardMeta) {
        self.merge(
            other
                .get_property(DEL_PREFIXES_KEY)
                .map(|prop| prop.to_vec()),
            other.range.keyspace_id,
        );
    }

    // TODO: pub fn merge_shard(..)

    fn merge(&mut self, del_prefixes_bytes: Option<Vec<u8>>, keyspace_id: u32) {
        // `self.del_prefixes.inner_key_off` & `inner_key_off` are not necessarily
        // equal.
        if let Some(bs) = del_prefixes_bytes {
            let mut del_prefixes = DeletePrefixes::unmarshal(&bs, keyspace_id);
            if self.rewrite_range_prefix
                && del_prefixes.keyspace_prefix_len > 0
                && del_prefixes.keyspace_prefix_len == self.range_prefix.len()
            {
                del_prefixes.rewrite_range_prefix(&self.range_prefix);
            }
            self.del_prefixes.merge(&del_prefixes);
        }
    }

    pub fn build_to_shard_meta(&self, meta: &mut ShardMeta) {
        if !self.del_prefixes.prefixes.is_empty() {
            info!(
                "{} PropertiesMerger.build_to_shard_meta: {:?}",
                meta.tag(),
                self.del_prefixes
            );
            meta.set_property(DEL_PREFIXES_KEY, &self.del_prefixes.marshal());
        }
    }

    fn split(&self, start_key: &[u8], end_key: &[u8]) -> DeletePrefixes {
        self.del_prefixes.build_split(start_key, end_key)
    }

    pub fn split_to_properties(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        props: &mut kvenginepb::Properties,
    ) {
        let split_del_prefixes = self.split(start_key, end_key);
        if !split_del_prefixes.is_empty() {
            props.mut_keys().push(DEL_PREFIXES_KEY.to_owned());
            props.mut_values().push(split_del_prefixes.marshal());
        }
    }
}

pub trait TxnFileRefExt {
    fn is_locked(&self) -> bool;
    fn is_commit_or_rollback(&self) -> bool {
        !self.is_locked()
    }
    fn get_commit_or_rollback(&self) -> (bool /* is_commit */, bool /* is_rollback */);
}

impl TxnFileRefExt for kvenginepb::TxnFileRef {
    #[inline]
    fn is_locked(&self) -> bool {
        self.get_user_meta().is_empty()
    }

    fn get_commit_or_rollback(&self) -> (bool, bool) {
        if self.is_locked() {
            (false, false)
        } else {
            let user_meta = UserMeta::from_slice(&self.user_meta);
            let is_rollback = user_meta.is_rollback();
            (!is_rollback, is_rollback)
        }
    }
}

#[derive(Debug)]
pub struct TxnFileRefPropertyHelper {
    txn_file_refs: kvenginepb::TxnFileRefs,
}

impl TxnFileRefPropertyHelper {
    pub fn from_property(data: Option<Bytes>) -> crate::Result<Self> {
        let mut txn_file_refs = kvenginepb::TxnFileRefs::new();
        if let Some(data) = data {
            txn_file_refs
                .merge_from_bytes(&data)
                .map_err(|err| -> crate::Error {
                    box_err!(
                        "failed to unmarshal TxnFileRefs: {:?}, data: {}",
                        err,
                        &Value::value(&data)
                    )
                })?;
        }
        Ok(Self { txn_file_refs })
    }

    pub fn marshall(&self) -> Vec<u8> {
        self.txn_file_refs.write_to_bytes().unwrap()
    }

    pub fn merge_txn_file_ref(
        &mut self,
        tag: &ShardTag,
        wb_ref: &kvenginepb::TxnFileRef,
    ) -> (bool /* is_commit */, bool /* is_rollback */) {
        let (is_commit, is_rollback) = wb_ref.get_commit_or_rollback();
        let mut refs = self.txn_file_refs.take_txn_file_refs().into_vec();
        if let Some(idx) = refs.iter().position(|x| x.start_ts == wb_ref.start_ts) {
            if is_rollback {
                refs.remove(idx);
            } else {
                refs[idx] = wb_ref.clone();
            }
        } else {
            debug_assert!(
                !is_commit && !is_rollback,
                "{} merge txn file ref: lock not found: wb_ref: {:?}, current: {:?}",
                tag,
                wb_ref,
                self.txn_file_refs
            );
            refs.push(wb_ref.clone());
        }
        self.txn_file_refs.set_txn_file_refs(refs.into());

        (is_commit, is_rollback)
    }

    pub fn clear_finished_txn_file_lock(&mut self, version: SnapVersion) {
        let mut refs = self.txn_file_refs.take_txn_file_refs().into_vec();
        refs.retain(|r| {
            r.get_version() > version.into_inner() || !r.get_lock_val_prefix().is_empty()
        });
        self.txn_file_refs.set_txn_file_refs(refs.into());
    }

    pub fn into_txn_file_locks(self, seq: u64) -> TxnFileLocks {
        let locks: HashMap<u64, Bytes> = self
            .txn_file_refs
            .txn_file_refs
            .into_iter()
            .filter_map(|r| {
                r.is_locked()
                    .then(|| (r.start_ts, Bytes::from(r.lock_val_prefix)))
            })
            .collect();
        TxnFileLocks { seq, inner: locks }
    }

    pub fn get_all_chunk_ids(&self) -> Vec<u64> {
        let mut chunk_ids = self
            .txn_file_refs
            .txn_file_refs
            .iter()
            .flat_map(|r| r.chunk_ids.clone())
            .collect::<Vec<_>>();
        // Txn files in properties contains transactions of both prewrite and commit. So
        // there would be duplicated txn chunks.
        chunk_ids.sort();
        chunk_ids.dedup();
        chunk_ids
    }
}

#[derive(Debug, Default, Clone)]
pub struct TxnFileLocks {
    // Log index on changed. Only used for (debug) assert that seq is increasing.
    seq: u64,
    inner: HashMap<u64 /* txn start_ts */, Bytes /* lock_val_prefix */>,
}

impl fmt::Display for TxnFileLocks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let txns = self.inner.keys().take(5).collect::<Vec<_>>();
        write!(
            f,
            "{{ seq:{}, len:{}, txns:{:?} }}",
            self.seq,
            self.inner.len(),
            txns
        )
    }
}

impl TxnFileLocks {
    pub fn from_lock_txn_files(seq: u64, lock_txn_files: &[TxnFile]) -> Self {
        let locks = lock_txn_files
            .iter()
            .map(|t| (t.start_ts(), t.lock_val_prefix()))
            .collect();
        Self { seq, inner: locks }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn seq(&self) -> u64 {
        self.seq
    }

    // Used by proxy to handle marshal & unmarshal.
    #[inline]
    pub fn get_locks(&self) -> &HashMap<u64, Bytes> {
        &self.inner
    }

    pub fn new(inner: HashMap<u64, Bytes>) -> Self {
        Self { seq: 0, inner }
    }

    // Duplication should be checked before insert.
    #[inline]
    pub fn insert(&mut self, seq: u64, start_ts: u64, lock_val_prefix: &[u8]) {
        debug_assert!(self.seq < seq);
        self.seq = seq;
        // Value of `lock_val_prefix` may be different (for `min_commit_ts`), but can be
        // ignored, as it does not affect resolving the lock.
        self.inner
            .entry(start_ts)
            .or_insert_with(|| Bytes::copy_from_slice(lock_val_prefix));
    }

    // Duplication should be checked before remove.
    #[inline]
    pub fn remove(&mut self, seq: u64, start_ts: u64) -> bool {
        debug_assert!(self.seq < seq);
        self.seq = seq;
        self.inner.remove(&start_ts).is_some()
    }

    // Note:
    // * `raw_key` is used to locate the region, it's not necessary to be in the txn
    //   file. Resolving txn file locks do not depend on the key.
    // * It's unreasonable that all locks from different transactions are of the
    //   same key. But currently TiDB/client-go does not complain about it.
    pub fn get_key_errors(&self, raw_key: &[u8]) -> crate::Result<Vec<kvrpcpb::KeyError>> {
        let mut key_errors = Vec::with_capacity(self.inner.len());
        for (start_ts, lock_val_prefix) in self.inner.iter() {
            let lock = txn_types::Lock::parse(lock_val_prefix).map_err(|err| {
                crate::Error::Other(box_err!(
                    "failed to parse lock: start_ts {}, lock_val_prefix {:?}, err {:?}",
                    start_ts,
                    lock_val_prefix,
                    err
                ))
            })?;
            let lock_info = lock.into_lock_info(raw_key.to_vec());
            let mut key_err = kvrpcpb::KeyError::default();
            key_err.set_locked(lock_info);
            key_errors.push(key_err);
        }
        Ok(key_errors)
    }
}

/// Unsafe function to extend lifetime
/// # Safety
/// Usually you should only use this for creating self-referencing structs, that
/// bound the actual lifetime to self.
pub unsafe fn extend_lifetime<'b, T: ?Sized>(r: &'b T) -> &'static T {
    std::mem::transmute::<&'b T, &'static T>(r)
}

pub fn estimated_entries_by_table_count(table_count: usize) -> u64 {
    table_count as u64 * 100_000
}

#[cfg(any(test, feature = "testexport"))]
pub mod test_util {
    use std::sync::Mutex;

    use api_version::ApiV2;
    use bytes::Bytes;
    use tidb_query_datatype::{
        codec::{
            row::v2::encoder_for_test::{Column, RowEncoder},
            table::encode_row_key,
        },
        expr::EvalContext,
    };

    use crate::table::OwnedInnerKey;

    #[derive(Debug, Clone)]
    pub struct KeyBuilder {
        keyspace_id: u32,
        prefix: String,
    }

    impl KeyBuilder {
        pub fn new(keyspace_id: u32, prefix: &str) -> Self {
            Self {
                keyspace_id,
                prefix: prefix.to_string(),
            }
        }

        pub fn i_to_inner_key(&self, i: usize) -> OwnedInnerKey {
            OwnedInnerKey::new(Bytes::from(self.i_to_key(i)))
        }

        pub fn i_to_outer_key(&self, i: usize) -> Vec<u8> {
            if self.keyspace_id == 0 {
                return self.i_to_key(i);
            }
            let mut key = ApiV2::get_txn_keyspace_prefix(self.keyspace_id);
            key.extend_from_slice(&self.i_to_key(i));
            key
        }

        #[inline]
        pub fn i_to_key(&self, i: usize) -> Vec<u8> {
            format!("{}key{:06}", self.prefix, i).into_bytes()
        }

        #[inline]
        pub fn i_to_val(&self, i: usize) -> String {
            format!("val{:06}", i)
        }

        pub fn gen_row_inner_key(&self, table_id: i64, i: usize) -> OwnedInnerKey {
            OwnedInnerKey::new(self.gen_row_key(table_id, i).into())
        }

        pub fn gen_row_outer_key(&self, table_id: i64, i: usize) -> Vec<u8> {
            let table_key = self.gen_row_key(table_id, i);
            let mut key = ApiV2::get_txn_keyspace_prefix(self.keyspace_id);
            key.extend_from_slice(&table_key);
            key
        }

        pub fn gen_row_key(&self, table_id: i64, i: usize) -> Vec<u8> {
            encode_row_key(table_id, i as i64)
        }

        pub fn gen_row_val(&self, ctx: &Mutex<EvalContext>, i: usize) -> Vec<u8> {
            let mut row_val = vec![];
            let str_val = format!("abc_{}", 1).repeat(1 + i % 16).into_bytes();
            let cols = vec![
                Column::new(1, Some(i as i64)),
                Column::new(2, Some(str_val)),
            ];
            let mut guard = ctx.lock().unwrap();
            row_val.write_row(&mut guard, cols).unwrap();
            row_val
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evenly_distribute() {
        assert_eq!(evenly_distribute(0, 1), Vec::<usize>::new());
        assert_eq!(evenly_distribute(10, 1), vec![10]);
        assert_eq!(evenly_distribute(10, 2), vec![5, 5]);
        assert_eq!(evenly_distribute(10, 3), vec![4, 3, 3]);
        assert_eq!(evenly_distribute(10, 4), vec![3, 3, 2, 2]);
        assert_eq!(evenly_distribute(10, 5), vec![2, 2, 2, 2, 2]);
        assert_eq!(evenly_distribute(10, 6), vec![2, 2, 2, 2, 1, 1]);
        assert_eq!(evenly_distribute(10, 7), vec![2, 2, 2, 1, 1, 1, 1]);
        assert_eq!(evenly_distribute(10, 8), vec![2, 2, 1, 1, 1, 1, 1, 1]);
        assert_eq!(evenly_distribute(10, 9), vec![2, 1, 1, 1, 1, 1, 1, 1, 1]);
        assert_eq!(evenly_distribute(10, 10), vec![1; 10]);
        assert_eq!(evenly_distribute(10, 11), vec![1; 10]);
        assert_eq!(evenly_distribute(10, 100), vec![1; 10]);
    }

    #[test]
    fn test_properties_helper() {
        let dp0 = DeletePrefixes::new_with_keyspace_id(1)
            .merge_prefix(b"x000101")
            .merge_prefix(b"x000103");

        let dp1 = DeletePrefixes::new_with_keyspace_id(1)
            .merge_prefix(b"x0101033")
            .merge_prefix(b"x0101066");

        let mut helper = PropertiesHelper::new(Some(dp0.marshal().into()), 1, b"x000".to_vec());
        helper.set_rewrite_range_prefix(true);
        helper.merge(Some(dp1.marshal()), 1);
        assert_eq!(
            helper.del_prefixes.prefixes,
            vec![
                b"x000101".to_vec(),
                b"x000103".to_vec(),
                b"x0001066".to_vec(),
            ]
        );

        helper.set_rewrite_range_prefix(false);
        let dp2 = DeletePrefixes::new_with_keyspace_id(1)
            .merge_prefix(b"x020101")
            .merge_prefix(b"x0201077");
        helper.merge(Some(dp2.marshal()), 1);
        assert_eq!(
            helper.del_prefixes.prefixes,
            vec![
                b"x000101".to_vec(),
                b"x000103".to_vec(),
                b"x0001066".to_vec(),
                b"x020101".to_vec(),
                b"x0201077".to_vec(),
            ]
        );
    }
}
