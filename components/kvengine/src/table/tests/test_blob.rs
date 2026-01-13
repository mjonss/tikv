// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{mem::size_of, sync::Arc};

use super::*;
use crate::table::{
    Iterator,
    blobtable::{BlobRef, blobtable::BlobTable, builder::BlobTableBuilder},
    file::InMemFile,
    sstable::{SsTable, test_util::*},
};

#[cfg(test)]
pub(crate) fn get_blob_test_value(n: usize) -> String {
    format!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa - {}", n)
}

#[cfg(test)]
pub(crate) fn generate_key_values(prefix: &str, n: usize) -> Vec<(String, String)> {
    assert!(n <= 10000);
    let mut results = Vec::with_capacity(n);
    for i in 0..n {
        let k = get_test_key(prefix, i);
        let v = get_blob_test_value(i);
        results.push((k, v));
    }
    results
}

#[cfg(test)]
fn test_fetch_value_from_blob_table(bt: &BlobTable, v: Value) -> Result<Vec<u8>> {
    assert!(v.is_blob_ref());
    let blob_ref = v.get_blob_ref();
    assert_eq!(bt.id(), blob_ref.fid);
    bt.get(&blob_ref, &mut vec![], None)
}

#[cfg(test)]
fn test_store_value_in_blob_table(
    blob_builder: &mut BlobTableBuilder,
    k: &String,
    v: &mut Value,
) -> BlobRef {
    assert!(v.value_len() > size_of::<BlobRef>() + v.user_meta_len());
    v.set_blob_ref();
    blob_builder.add(InnerKey::from_inner_buf(k.as_bytes()), v)
}

#[cfg(test)]
pub(crate) fn build_blob_test_table_with_kvs(kvs: &Vec<(String, String)>) -> (SsTable, BlobTable) {
    let sst_fid = TEST_ID_ALLOC.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    let blob_fid = TEST_ID_ALLOC.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    let mut sst_builder = new_table_builder_for_test(sst_fid);
    let mut blob_builder = BlobTableBuilder::new(blob_fid, NO_COMPRESSION, 0, 0, 1024 * 1024, None);
    let meta = 0u8;

    for (k, v) in kvs {
        let value_buf = Value::encode_buf(meta, &[0], 0, v.as_bytes());
        let mut v = Value::decode(value_buf.as_slice());
        let blob_ref = test_store_value_in_blob_table(&mut blob_builder, k, &mut v);
        sst_builder.add(InnerKey::from_inner_buf(k.as_bytes()), &v, Some(blob_ref));
    }

    let mut buf = Vec::with_capacity(sst_builder.estimated_size());

    sst_builder.finish(0, &mut buf);

    let bytes = blob_builder.finish();

    let sst_file = InMemFile::new(sst_fid, buf.into());
    let blob_file = InMemFile::new(blob_fid, bytes);

    (
        SsTable::new(Arc::new(sst_file), new_test_cache(), None).unwrap(),
        BlobTable::new(Arc::new(blob_file)).unwrap(),
    )
}

#[cfg(test)]
pub(crate) fn create_blob_sst_table(
    prefix: &str,
    n: usize,
) -> ((SsTable, BlobTable), Vec<(String, String)>) {
    let kvs = generate_key_values(prefix, n);
    (build_blob_test_table_with_kvs(&kvs), kvs)
}

#[test]
fn test_value_external_storage() {
    let ((t, bt), _) = create_blob_sst_table("key", 10000);
    let mut it = t.new_iterator(false, true);
    let mut count = 0;
    it.rewind();
    while it.valid() {
        let k = it.key();
        assert_eq!(k.deref(), get_test_key("key", count).as_bytes());
        let value = test_fetch_value_from_blob_table(&bt, it.value());
        assert_eq!(value.unwrap(), get_blob_test_value(count).as_bytes());
        count += 1;
        it.next()
    }
}
