// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::bail;
use cloud_encryption::EncryptionKey;
use kvenginepb::{TxnFileRef, TxnFileRefs};
use protobuf::Message;
use rstest::rstest;

use crate::{
    GLOBAL_SHARD_END_KEY, LOCK_CF, TXN_FILE_REF, UserMeta, WRITE_CF, WriteBatch, dfs,
    dfs::FileType,
    table::{
        InnerKey, OP_CHECK_NOT_EXIST, OP_INSERT, OP_PUT, TxnChunkBuilder, TxnCtx, TxnFile,
        TxnFileId,
    },
    tests::{
        DEF_BLOCK_SIZE, TABLE_KEY_PREFIX, TestEngine, generate_encryption_key, new_test_engine_opt,
        write_data,
    },
    util::test_util::KeyBuilder,
};

#[rstest]
#[case::base(None, true)]
#[case::enc(Some(generate_encryption_key()), true)]
#[case::disable_key_off(None, false)]
#[case::enc_disable_key_off(Some(generate_encryption_key()), false)]
fn test_txn_file(#[case] enc_key: Option<EncryptionKey>, #[case] enable_inner_key_off: bool) {
    ::test_util::init_log_for_test();
    let enc_key = enc_key.as_ref();
    let (engine, tx) = new_test_engine_opt(enable_inner_key_off, DEF_BLOCK_SIZE, TABLE_KEY_PREFIX);
    let kb = engine.key_builder();

    let build_chunk =
        |start, end, chunk_id| build_txn_chunk(&engine, start, end, chunk_id, |_| OP_PUT, enc_key);
    let check_constraint = |start, end, start_ts, expected: Option<usize>| -> anyhow::Result<()> {
        let op_fn = |i| {
            if i % 4 == 0 {
                OP_INSERT
            } else {
                OP_CHECK_NOT_EXIST
            }
        };
        let already_exist_key =
            check_txn_file_constraint(&engine, start, end, start_ts, op_fn, enc_key);
        let expected = expected.map(|i| kb.i_to_outer_key(i));
        if already_exist_key != expected {
            bail!(
                "check_constraint failed, already_exist_key: {:?}, expected: {:?}",
                already_exist_key,
                expected
            );
        }
        Ok(())
    };
    check_constraint(200, 400, 1000, None).unwrap();

    let chunk_id = 200;
    build_chunk(200, 300, chunk_id);
    let primary = kb.i_to_outer_key(0);
    let mut wb = WriteBatch::new(1);
    let txn_file_refs = make_txn_file_refs(
        1000,
        vec![chunk_id],
        make_lock_prefix(primary.clone(), 200),
        vec![],
    );
    wb.set_property(TXN_FILE_REF, &txn_file_refs);
    engine
        .txn_chunk_mgr
        .prepare(chunk_id, enc_key.cloned())
        .unwrap();
    // prewrite: [200,300), start_ts: 1000
    write_data(wb, &tx);
    verify_lock(&engine, 200, 300, kb);
    check_constraint(200, 400, 1000, None).unwrap();

    // rollback: start_ts: 1000
    let txn_file_refs = make_txn_file_refs(1000, vec![chunk_id], vec![], make_user_meta(1000, 0));
    let mut wb = WriteBatch::new(1);
    wb.set_property(TXN_FILE_REF, &txn_file_refs);
    write_data(wb, &tx);
    let shard = engine.get_shard(1).unwrap();
    assert!(shard.get_property(TXN_FILE_REF).unwrap().is_empty());
    assert!(shard.get_txn_chunks().is_empty());
    check_constraint(200, 400, 1000, None).unwrap();

    // concurrent txn files.
    let txn1_chunk_id = 201;
    build_chunk(200, 300, txn1_chunk_id);
    let txn1_start_ts = 1003;
    let txn1_lock = make_txn_file_refs(
        txn1_start_ts,
        vec![txn1_chunk_id],
        make_lock_prefix(primary.clone(), txn1_start_ts),
        vec![],
    );
    let mut wb = WriteBatch::new(1);
    wb.set_property(TXN_FILE_REF, &txn1_lock);
    engine
        .txn_chunk_mgr
        .prepare_txn_chunks(vec![txn1_chunk_id], enc_key.cloned())
        .unwrap();
    // prewrite: [200, 300), start_ts: 1003
    write_data(wb, &tx);
    let txn2_chunk_id = 202;
    build_chunk(300, 400, txn2_chunk_id);
    let txn2_start_ts = 1004;
    let txn2_lock = make_txn_file_refs(
        txn2_start_ts,
        vec![txn2_chunk_id],
        make_lock_prefix(primary.clone(), txn2_start_ts),
        vec![],
    );
    let mut wb = WriteBatch::new(1);
    wb.set_property(TXN_FILE_REF, &txn2_lock);
    engine
        .txn_chunk_mgr
        .prepare_txn_chunks(vec![txn2_chunk_id], enc_key.cloned())
        .unwrap();
    // prewrite: [300, 400): start_ts: 1004
    write_data(wb, &tx);
    verify_lock(&engine, 200, 400, kb);

    let txn1_commit = make_txn_file_refs(
        txn1_start_ts,
        vec![txn1_chunk_id],
        vec![],
        make_user_meta(txn1_start_ts, txn1_start_ts + 2),
    );
    let mut wb = WriteBatch::new(1);
    wb.set_property(TXN_FILE_REF, &txn1_commit);
    // commit: [200, 300), 1003 -> 1005
    write_data(wb, &tx);
    verify_write(&engine, 200, 300, kb);
    verify_lock(&engine, 300, 400, kb);
    for (start, end, start_ts, expected) in [
        (250, 400, 1004, None),
        (100, 400, 1006, Some(200)),
        (250, 350, 1006, Some(250)),
        (300, 400, 1006, None),
    ] {
        check_constraint(start, end, start_ts, expected).unwrap();
    }

    let conflict_chunk_id = 203;
    let conflict_start_ts = 999;
    build_chunk(250, 350, conflict_chunk_id);
    engine
        .txn_chunk_mgr
        .prepare(conflict_chunk_id, enc_key.cloned())
        .unwrap();
    let conflict_chunk = engine.txn_chunk_mgr.get(conflict_chunk_id).unwrap();
    let lower_bound = InnerKey::from_inner_buf(b"");
    let upper_bound = InnerKey::from_inner_buf(GLOBAL_SHARD_END_KEY);
    let conflict_ctx = TxnCtx::new(
        vec![].into(),
        make_lock_prefix(primary.clone(), conflict_start_ts).into(),
        conflict_start_ts,
        lower_bound,
        upper_bound,
    );
    let conflict_txn_file = TxnFile::new(
        TxnFileId::new(1, 1, conflict_start_ts),
        vec![conflict_chunk],
        conflict_ctx.clone(),
    )
    .unwrap();
    let snap = engine.get_snap_access(1).unwrap();
    let (key, um) = snap
        .get_txn_file_conflict_write(&conflict_txn_file)
        .unwrap();
    assert_eq!(key, kb.i_to_outer_key(250));
    assert_eq!(um.start_ts, txn1_start_ts);
    let (key, lock) = snap.get_txn_file_conflict_lock(&conflict_txn_file).unwrap();
    assert_eq!(key, kb.i_to_outer_key(300));
    assert_eq!(lock.ts.into_inner(), txn2_start_ts);

    let empty_txn_file = TxnFile::new(
        TxnFileId::new(1, 1, conflict_start_ts),
        vec![],
        conflict_ctx,
    )
    .unwrap();
    assert!(snap.get_txn_file_conflict_write(&empty_txn_file).is_none());
    assert!(snap.get_txn_file_conflict_lock(&empty_txn_file).is_none());

    verify_write(&engine, 200, 300, kb);
    verify_lock(&engine, 300, 400, kb);

    let txn2_commit = make_txn_file_refs(
        txn2_start_ts,
        vec![txn2_chunk_id],
        vec![],
        make_user_meta(txn2_start_ts, txn2_start_ts + 2),
    );
    let mut wb = WriteBatch::new(1);
    wb.set_property(TXN_FILE_REF, &txn2_commit);
    // commit: [300, 400), 1004 -> 1006
    write_data(wb, &tx);
    verify_write(&engine, 200, 400, kb);
    for (start, end, start_ts, expected) in [
        (250, 400, 1004, None),
        (100, 400, 1006, Some(200)),
        (250, 350, 1006, Some(250)),
        (300, 400, 1005, None),
        (300, 400, 1007, Some(300)),
    ] {
        check_constraint(start, end, start_ts, expected).unwrap();
    }

    // Test for get_txn_file_conflict_write
    {
        let snap = engine.get_snap_access(1).unwrap();

        let conflict_chunk_id = 204;
        build_chunk(250, 350, conflict_chunk_id);
        engine
            .txn_chunk_mgr
            .prepare(conflict_chunk_id, enc_key.cloned())
            .unwrap();
        let conflict_chunk = engine.txn_chunk_mgr.get(conflict_chunk_id).unwrap();
        let lower_bound = InnerKey::from_inner_buf(b"");
        let upper_bound = InnerKey::from_inner_buf(GLOBAL_SHARD_END_KEY);

        let cases = [
            // start_ts, Option<(conflict key, conflict start_ts, conflict commit_ts)
            (1000, Some((250, txn1_start_ts, txn1_start_ts + 2))),
            (1005, Some((300, txn2_start_ts, txn2_start_ts + 2))),
            (1007, None),
        ];

        for (conflict_start_ts, expected) in cases {
            let conflict_ctx = TxnCtx::new(
                vec![].into(),
                make_lock_prefix(primary.clone(), conflict_start_ts).into(),
                conflict_start_ts,
                lower_bound,
                upper_bound,
            );
            let conflict_txn_file = TxnFile::new(
                TxnFileId::new(1, 1, conflict_start_ts),
                vec![conflict_chunk.clone()],
                conflict_ctx.clone(),
            )
            .unwrap();
            match snap.get_txn_file_conflict_write(&conflict_txn_file) {
                Some((key, um)) => {
                    let (conflict_key, conflict_start_ts, conflict_commit_ts) = expected.unwrap();
                    assert_eq!(key, kb.i_to_outer_key(conflict_key));
                    assert_eq!(um.start_ts, conflict_start_ts);
                    assert_eq!(um.commit_ts, conflict_commit_ts);
                }
                None => {
                    assert!(expected.is_none());
                }
            }
        }
    }
}

#[rstest]
#[case::base(None, true)]
#[case::enc(Some(generate_encryption_key()), true)]
#[case::disable_key_off(None, false)]
#[case::enc_disable_key_off(Some(generate_encryption_key()), false)]
fn test_txn_file_multiple(
    #[case] enc_key: Option<EncryptionKey>,
    #[case] enable_inner_key_off: bool,
) {
    ::test_util::init_log_for_test();
    let enc_key = enc_key.as_ref();
    let (engine, tx) = new_test_engine_opt(enable_inner_key_off, DEF_BLOCK_SIZE, TABLE_KEY_PREFIX);
    let kb = engine.key_builder();

    let chunks_id: Vec<u64> = (100..500).step_by(10).collect();
    let primary = kb.i_to_outer_key(0);
    let start_ts = 2000;
    for &chunk_id in &chunks_id {
        let start = chunk_id as usize;
        build_txn_chunk(&engine, start, start + 10, chunk_id, |_| OP_PUT, enc_key);
    }
    let mut wb = WriteBatch::new(1);
    let txn_file_refs = make_txn_file_refs(
        start_ts,
        chunks_id.clone(),
        make_lock_prefix(primary, start_ts),
        vec![],
    );
    wb.set_property(TXN_FILE_REF, &txn_file_refs);
    engine
        .txn_chunk_mgr
        .prepare_txn_chunks(chunks_id.clone(), enc_key.cloned())
        .unwrap();
    write_data(wb, &tx);
    verify_lock(&engine, 100, 500, kb);

    let txn4_commit = make_txn_file_refs(
        start_ts,
        chunks_id,
        vec![],
        make_user_meta(start_ts, start_ts + 2),
    );
    let mut wb = WriteBatch::new(1);
    wb.set_property(TXN_FILE_REF, &txn4_commit);
    write_data(wb, &tx);
    verify_write(&engine, 100, 500, kb);
}

pub(crate) fn build_txn_chunk<OpF: Fn(usize) -> u8>(
    engine: &TestEngine,
    start: usize,
    end: usize,
    id: u64,
    op_fn: OpF,
    enc_key: Option<&EncryptionKey>,
) {
    let kb = engine.key_builder();
    let mut chunk_builder = TxnChunkBuilder::new(id, 64, enc_key.cloned());
    for i in start..end {
        let key = kb.i_to_key(i);
        chunk_builder.add_entry(InnerKey::from_outer_key(&key), op_fn(i), &key);
    }
    let mut buf = vec![];
    chunk_builder.finish(&mut buf);
    let runtime = engine.fs.get_runtime();
    let fs = engine.fs.clone();
    let opts = dfs::Options::default().with_type(FileType::TxnChunk);
    runtime.block_on(fs.create(id, buf.into(), opts)).unwrap();
}

fn build_txn_file<OpF: Fn(usize) -> u8>(
    engine: &TestEngine,
    start: usize,
    end: usize,
    start_ts: u64,
    commit_ts: Option<u64>, // None for prewrite, Some(commit_ts) for commit, Some(0) for rollback
    op_fn: OpF,
    enc_key: Option<&EncryptionKey>,
) -> TxnFile {
    static CHUNK_ID: AtomicU64 = AtomicU64::new(1_000_000);

    let chunk_id = CHUNK_ID.fetch_add(1, Ordering::Relaxed);
    build_txn_chunk(engine, start, end, chunk_id, op_fn, enc_key);
    engine
        .txn_chunk_mgr
        .prepare(chunk_id, enc_key.cloned())
        .unwrap();
    let chunk = engine.txn_chunk_mgr.get(chunk_id).unwrap();
    let lower_bound = InnerKey::from_inner_buf(b"");
    let upper_bound = InnerKey::from_inner_buf(GLOBAL_SHARD_END_KEY);

    let um = if let Some(commit_ts) = commit_ts {
        UserMeta::new(start_ts, commit_ts).to_array().to_vec()
    } else {
        vec![]
    };
    let ctx = TxnCtx::new(
        um.into(),
        make_lock_prefix(engine.key_builder().i_to_outer_key(start), start_ts).into(),
        start_ts,
        lower_bound,
        upper_bound,
    );
    TxnFile::new(TxnFileId::new(1, 1, start_ts), vec![chunk], ctx).unwrap()
}

pub(crate) fn make_txn_file_refs(
    start_ts: u64,
    chunks_id: Vec<u64>,
    lock_prefix: Vec<u8>,
    user_meta: Vec<u8>,
) -> Vec<u8> {
    let mut txn_file_ref = TxnFileRef::new();
    txn_file_ref.set_chunk_ids(chunks_id);
    txn_file_ref.set_shard_ver(1);
    txn_file_ref.set_start_ts(start_ts);
    if !lock_prefix.is_empty() {
        txn_file_ref.set_lock_val_prefix(lock_prefix);
    }
    if !user_meta.is_empty() {
        txn_file_ref.set_user_meta(user_meta);
    }
    txn_file_ref.set_inner_lower_bound(vec![]);
    txn_file_ref.set_inner_upper_bound(GLOBAL_SHARD_END_KEY.to_vec());
    let mut txn_file_refs = TxnFileRefs::new();
    txn_file_refs.mut_txn_file_refs().push(txn_file_ref);
    txn_file_refs.write_to_bytes().unwrap()
}

pub(crate) fn make_lock_prefix(primary: Vec<u8>, start_ts: u64) -> Vec<u8> {
    let min_commit_ts = start_ts + 1;
    let mut lock = txn_types::Lock::new(
        txn_types::LockType::Put,
        primary,
        start_ts.into(),
        3000,
        None,
        0.into(),
        100,
        min_commit_ts.into(),
    );
    lock.is_txn_file = true;
    lock.to_bytes()
}

fn check_txn_file_constraint<F: Fn(usize) -> u8>(
    engine: &TestEngine,
    start: usize,
    end: usize,
    start_ts: u64,
    op_fn: F,
    enc_key: Option<&EncryptionKey>,
) -> Option<Vec<u8> /* already_exist_key */> {
    let txn_file = build_txn_file(engine, start, end, start_ts, None, op_fn, enc_key);
    let snap = engine.get_snap_access(1).unwrap();
    snap.check_txn_file_constraint(&txn_file, start_ts)
}

fn make_user_meta(start_ts: u64, commit_ts: u64) -> Vec<u8> {
    UserMeta::new(start_ts, commit_ts).to_array().to_vec()
}

fn verify_lock(engine: &TestEngine, start: usize, end: usize, kb: &KeyBuilder) {
    let snap = engine.get_snap_access(1).unwrap();
    for i in start..end {
        let key = kb.i_to_outer_key(i);
        let val = kb.i_to_key(i);
        let item = snap.get(LOCK_CF, &key, 0);
        assert!(item.is_valid());
        let lock = txn_types::Lock::parse(item.get_value()).unwrap();
        assert_eq!(lock.primary, kb.i_to_outer_key(0));
        assert!(lock.is_txn_file);
        assert_eq!(lock.short_value.unwrap(), val.as_slice());
    }
    let mut it = snap.new_iterator(LOCK_CF, false, false, None, false);
    it.rewind();
    let mut i = start;
    while it.valid() {
        let lock = txn_types::Lock::parse(it.val()).unwrap();
        let val = kb.i_to_key(i);
        assert_eq!(lock.short_value.unwrap(), val.as_slice());
        it.next();
        i += 1;
    }
    assert_eq!(i, end);
}

fn verify_write(engine: &TestEngine, start: usize, end: usize, kb: &KeyBuilder) {
    let snap = engine.get_snap_access(1).unwrap();
    for i in start..end {
        let key = kb.i_to_outer_key(i);
        let val = kb.i_to_key(i);
        let item = snap.get(WRITE_CF, &key, u64::MAX);
        assert_eq!(item.get_value(), val.as_slice());
    }
    let mut it = snap.new_iterator(WRITE_CF, false, false, None, false);
    it.rewind();
    let mut i = start;
    while it.valid() {
        let val = kb.i_to_key(i);
        assert_eq!(it.val(), val.as_slice());
        it.next();
        i += 1;
    }
    assert_eq!(i, end);
}
