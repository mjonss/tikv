// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use bytes::Bytes;
use kvengine::{UserMeta, WRITE_CF, read::Iterator as KvIterator};
use kvproto::import_sstpb::{DuplicateDetectResponse, KvPair};
use sst_importer::{Error, Result};
use tikv_kv::Snapshot;
use txn_types::{TimeStamp, Write, WriteType};

use crate::storage::mvcc::parse_write;

#[cfg(test)]
const MAX_SCAN_BATCH_COUNT: usize = 256;

#[cfg(not(test))]
const MAX_SCAN_BATCH_COUNT: usize = 4096;

pub struct DuplicateDetector<S: Snapshot> {
    iter: KvIterator,
    key_only: bool,
    valid: bool,
    min_commit_ts: TimeStamp,
    _phantom: PhantomData<S>,
}

impl<S: Snapshot> DuplicateDetector<S> {
    pub fn new(
        snapshot: S,
        start_key: Vec<u8>,
        end_key: Option<Vec<u8>>,
        min_commit_ts: u64,
        key_only: bool,
    ) -> Result<DuplicateDetector<S>> {
        let snap = snapshot.get_kvengine_snap().unwrap().clone();
        let mut iter = snap.new_iterator(WRITE_CF, false, true, None, true);
        let lower_bound = Bytes::copy_from_slice(&start_key);
        let upper_bound = end_key
            .as_ref()
            .map(|e| Bytes::copy_from_slice(e))
            .unwrap_or_else(|| Bytes::copy_from_slice(snap.get_end_key()));
        iter.set_range(lower_bound, upper_bound);
        debug!(
            "snapshot meta";
            "start_key" => log_wrappers::Value::key(snap.get_start_key()),
            "end_key" => log_wrappers::Value::key(snap.get_end_key()),
            "request_start" => log_wrappers::Value::key(&start_key),
            "request_end" => end_key.as_ref().map(|k| log_wrappers::Value::key(k)),
            "valid" => iter.valid(),
        );
        Ok(DuplicateDetector {
            iter,
            key_only,
            min_commit_ts: TimeStamp::new(min_commit_ts),
            valid: true,
            _phantom: PhantomData,
        })
    }

    pub fn try_next(&mut self) -> Result<Option<Vec<KvPair>>> {
        let mut ret = vec![];
        while let Some((current_key, commit_ts)) = self.move_to_next_import_key() {
            self.collect_current_key_duplicate(current_key, commit_ts, &mut ret)?;
            if ret.len() >= MAX_SCAN_BATCH_COUNT {
                return Ok(Some(ret));
            }
        }
        if ret.is_empty() {
            return Ok(None);
        }
        Ok(Some(ret))
    }

    fn move_to_next_import_key(&mut self) -> Option<(Vec<u8>, TimeStamp)> {
        while self.iter.valid() {
            let user_meta = UserMeta::from_slice(self.iter.user_meta());
            let (current_key, commit_ts) = (self.iter.key(), TimeStamp::from(user_meta.commit_ts));
            if commit_ts > self.min_commit_ts {
                return Some((current_key.to_vec(), commit_ts));
            }
            self.iter.next();
        }
        None
    }

    fn collect_current_key_duplicate(
        &mut self,
        start_key: Vec<u8>,
        end_commit_ts: TimeStamp,
        duplicate_pairs: &mut Vec<KvPair>,
    ) -> Result<()> {
        let user_meta = UserMeta::from_slice(self.iter.user_meta());
        let val = self.iter.val();
        let (_, latest_write) = parse_write(&user_meta, val);
        if latest_write.write_type == WriteType::Delete {
            return Err(Error::Engine(box_err!(
                "found a {:?} key with commits ts {} larger than min_commit_ts of importer {}",
                latest_write.write_type,
                end_commit_ts,
                self.min_commit_ts
            )));
        }

        let mut latest_write = if self.key_only {
            None
        } else {
            Some(latest_write)
        };

        self.iter.next();
        while self.iter.valid() && self.iter.key() == start_key {
            let user_meta = UserMeta::from_slice(self.iter.user_meta());
            let val = self.iter.val();
            let (commit_ts, write) = parse_write(&user_meta, val);
            if commit_ts <= self.min_commit_ts {
                self.skip_all_version(&start_key);
                return Ok(());
            }
            if write.write_type == WriteType::Delete {
                self.iter.next();
                continue;
            }

            let write = if self.key_only { None } else { Some(write) };

            if latest_write.is_some() {
                duplicate_pairs.push(self.make_kv_pair(
                    &start_key,
                    latest_write.take(),
                    end_commit_ts,
                ));
            }
            duplicate_pairs.push(self.make_kv_pair(&start_key, write, commit_ts));
            self.iter.next();

            debug!(
                "found duplicate key";
                "key" => log_wrappers::Value::key(&start_key),
                "latest_commit_ts" => end_commit_ts,
                "commit_ts" => commit_ts,
            );
        }

        Ok(())
    }

    fn skip_all_version(&mut self, start_key: &[u8]) {
        self.iter.next();
        while self.iter.valid() && self.iter.key() == start_key {
            self.iter.next();
        }
    }

    fn make_kv_pair(&self, key: &[u8], write: Option<Write>, ts: TimeStamp) -> KvPair {
        let mut pair = KvPair::default();
        pair.set_key(key.to_vec());
        pair.set_commit_ts(ts.into_inner());
        if let Some(write) = write {
            // always fetch the value from `short_value`,
            // since there is no value length limit in cse.
            pair.set_value(write.short_value.unwrap());
        }
        pair
    }
}

impl<S: Snapshot> Iterator for DuplicateDetector<S> {
    type Item = DuplicateDetectResponse;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.valid {
            return None;
        }
        let mut resp = DuplicateDetectResponse::default();
        match self.try_next() {
            Ok(Some(pairs)) => {
                resp.set_pairs(pairs.into());
            }
            Err(e) => {
                resp.set_key_error(e.into());
                self.valid = false;
            }
            Ok(None) => {
                return None;
            }
        }
        Some(resp)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::channel;

    use api_version::KvFormat;
    use kvproto::kvrpcpb::Context;
    use tikv_kv::Engine;
    use txn_types::{Key, Mutation};

    use super::*;
    use crate::storage::{
        Storage, TestStorageBuilderApiV1,
        lock_manager::{LockManager, MockLockManager},
        txn::commands,
    };

    fn prewrite_data<E: Engine, L: LockManager, F: KvFormat>(
        storage: &Storage<E, L, F>,
        primary: Vec<u8>,
        data: Vec<(Vec<u8>, Vec<u8>)>,
        start_ts: u64,
    ) {
        let cmd = commands::Prewrite::with_defaults(
            data.into_iter()
                .map(|(key, value)| {
                    if value.is_empty() {
                        Mutation::make_delete(Key::from_raw(&key))
                    } else {
                        Mutation::make_put(Key::from_raw(&key), value)
                    }
                })
                .collect(),
            primary,
            start_ts.into(),
        );
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                cmd,
                Box::new(move |x| {
                    x.unwrap();
                    tx.send(()).unwrap();
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    fn rollback_data<E: Engine, L: LockManager, F: KvFormat>(
        storage: &Storage<E, L, F>,
        data: Vec<Vec<u8>>,
        start_ts: u64,
    ) {
        let cmd = commands::Rollback::new(
            data.into_iter().map(|key| Key::from_raw(&key)).collect(),
            start_ts.into(),
            false,
            Context::default(),
        );
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                cmd,
                Box::new(move |x| {
                    x.unwrap();
                    tx.send(()).unwrap();
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    fn write_data<E: Engine, L: LockManager, F: KvFormat>(
        storage: &Storage<E, L, F>,
        data: Vec<(Vec<u8>, Vec<u8>)>,
        ts: u64,
    ) {
        let primary = data[0].0.clone();
        let start_ts = ts - 1;
        let keys: Vec<Key> = data.iter().map(|(key, _)| Key::from_raw(key)).collect();
        prewrite_data(storage, primary, data, start_ts);
        let cmd = commands::Commit::new(
            keys,
            start_ts.into(),
            ts.into(),
            false,
            false,
            Context::default(),
        );
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                cmd,
                Box::new(move |x| {
                    x.unwrap();
                    tx.send(()).unwrap();
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    fn check_duplicate_data<S: Snapshot>(
        mut detector: DuplicateDetector<S>,
        expected_kvs: Vec<(Vec<u8>, Vec<u8>, u64)>,
    ) {
        let mut base = 0;
        while let Some(resp) = detector.try_next().unwrap() {
            let data: Vec<(Vec<u8>, Vec<u8>, u64)> = resp
                .into_iter()
                .map(|mut p| (p.take_key(), p.take_value(), p.get_commit_ts()))
                .collect();
            assert!(expected_kvs.len() >= base + data.len());
            for i in base..(base + data.len()) {
                assert_eq!(
                    expected_kvs[i],
                    data[i - base],
                    "base {}, the {} data,  {}, {}",
                    base,
                    i,
                    String::from_utf8(expected_kvs[i].0.clone()).unwrap(),
                    String::from_utf8(data[i - base].0.clone()).unwrap(),
                );
            }
            base += data.len();
        }
    }

    #[test]
    fn test_duplicate_detect() {
        let mut storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let mut data = vec![];
        for i in 0..1000 {
            let key = format!("{}", i);
            let value = format!("{}", i);
            data.push((key.as_bytes().to_vec(), value.as_bytes().to_vec()))
        }
        write_data(&storage, data, 3);
        let mut data = vec![];
        let big_value = vec![1u8; 300];
        for i in 0..1000 {
            let key = format!("{}", i * 2);
            data.push((key.as_bytes().to_vec(), big_value.clone()))
        }
        write_data(&storage, data, 5);
        // We have to do the prewrite manually so that the mem locks don't get released.
        let snapshot = storage.get_snapshot();
        let start = format!("{}", 0);
        let end = format!("{}", 800);
        let detector = DuplicateDetector::new(
            snapshot,
            start.as_bytes().to_vec(),
            Some(end.as_bytes().to_vec()),
            0,
            false,
        )
        .unwrap();
        let mut expected_kvs = vec![];
        for i in 0..400 {
            let key = format!("{}", i * 2);
            let value = format!("{}", i * 2);
            expected_kvs.push((key.as_bytes().to_vec(), big_value.clone(), 5));
            expected_kvs.push((key.as_bytes().to_vec(), value.as_bytes().to_vec(), 3));
        }
        expected_kvs.sort_by(|a, b| {
            if a.0 == b.0 {
                b.2.cmp(&a.2)
            } else {
                a.0.cmp(&b.0)
            }
        });
        check_duplicate_data(detector, expected_kvs);
    }

    // There are 40 key-value pairs in db, there are
    // - [100, 101, 102, 103, 104, 105, 106, 107, 108, 109] with commit timestamp 10
    // - [104, 105, 106, 107, 108, 109, 110, 111, 112, 113] with commit timestamp
    //   14, these 20 keys have existed in db before importing. So we do not think
    //   (105,10) is repeated with (105,14).
    // - [108, 109, 110, 111, 112, 113, 114, 115, 116, 117] with commit timestamp 18
    // - [112, 113, 114, 115, 116, 117, 118, 119, 120, 121] with commit timestamp
    //   22, these 20 keys
    // are imported by lightning. So (108,18) is repeated with (108,14), but
    // (108,18) is not repeated with (108,10).
    #[test]
    fn test_duplicate_detect_incremental() {
        let mut storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        for &start in &[100, 104, 108, 112] {
            let end = start + 10;
            let mut data = vec![];
            for i in start..end {
                let key = format!("{}", i);
                let value = format!("{}", i);
                data.push((key.as_bytes().to_vec(), value.as_bytes().to_vec()))
            }
            write_data(&storage, data, start - 90);
        }

        // We have to do the prewrite manually so that the mem locks don't get released.
        let snapshot = storage.get_snapshot();
        let start = format!("{}", 0);
        let detector =
            DuplicateDetector::new(snapshot, start.as_bytes().to_vec(), None, 16, false).unwrap();
        let mut expected_kvs = vec![];
        for &(i, ts) in &[
            (108u64, 14),
            (108, 18),
            (109, 14),
            (109, 18),
            (110, 14),
            (110, 18),
            (111, 14),
            (111, 18),
            (112, 14),
            (112, 18),
            (112, 22),
            (113, 14),
            (113, 18),
            (113, 22),
            (114, 18),
            (114, 22),
            (115, 18),
            (115, 22),
            (116, 18),
            (116, 22),
            (117, 18),
            (117, 22),
        ] {
            let key = format!("{}", i);
            let value = format!("{}", i);
            expected_kvs.push((key.as_bytes().to_vec(), value.as_bytes().to_vec(), ts));
        }

        expected_kvs.sort_by(|a, b| {
            if a.0 == b.0 {
                b.2.cmp(&a.2)
            } else {
                a.0.cmp(&b.0)
            }
        });
        check_duplicate_data(detector, expected_kvs);
    }

    #[test]
    fn test_duplicate_detect_rollback_and_delete() {
        let mut storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let data = vec![
            (b"100".to_vec(), b"100".to_vec()),
            (b"101".to_vec(), b"101".to_vec()),
            (b"102".to_vec(), b"102".to_vec()),
        ];
        write_data(&storage, data.clone(), 10);
        prewrite_data(&storage, b"100".to_vec(), data[..2].to_vec(), 11);
        rollback_data(&storage, vec![b"100".to_vec(), b"101".to_vec()], 11);
        write_data(&storage, vec![(b"102".to_vec(), vec![])], 12);
        write_data(&storage, data, 14);
        let expected_kvs = vec![
            (b"100".to_vec(), b"100".to_vec(), 14),
            (b"100".to_vec(), b"100".to_vec(), 10),
            (b"101".to_vec(), b"101".to_vec(), 14),
            (b"101".to_vec(), b"101".to_vec(), 10),
        ];
        let snapshot = storage.get_snapshot();
        let detector = DuplicateDetector::new(snapshot, b"0".to_vec(), None, 13, false).unwrap();
        check_duplicate_data(detector, expected_kvs);
    }
}
