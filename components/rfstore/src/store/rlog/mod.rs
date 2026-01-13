// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::mem;

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut};
use kvengine::{IdVer, table::SnapVersion};
use kvenginepb::get_any_snap_from_changeset;
use kvproto::raft_cmdpb::{CustomRequest, RaftCmdRequest};
use protobuf::Message;
use tikv_util::{codec::number::U64_SIZE, warn};

use crate::store::{PeerTag, is_change_set_affect_mem_table};

pub fn get_custom_log(req: &RaftCmdRequest) -> Option<CustomRaftLog<'_>> {
    if !req.has_custom_request() {
        return None;
    }
    Some(CustomRaftLog {
        data: req.get_custom_request().get_data(),
    })
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
pub enum CustomRaftLogType {
    Prewrite = 1,
    Commit = 2,
    Rollback = 3,
    PessimisticLock = 4,
    PessimisticRollback = 5,
    OnePc = 6,
    EngineMeta = 7,
    ResolveLock = 8,
    SwitchMemTable = 9,
    TriggerTrimOverBound = 10,
    TxnFileRef = 12,
    // Note: Please make sure tiflash proxy is updated if you add a new type.
}

impl From<u8> for CustomRaftLogType {
    fn from(v: u8) -> Self {
        match v {
            1 => CustomRaftLogType::Prewrite,
            2 => CustomRaftLogType::Commit,
            3 => CustomRaftLogType::Rollback,
            4 => CustomRaftLogType::PessimisticLock,
            5 => CustomRaftLogType::PessimisticRollback,
            6 => CustomRaftLogType::OnePc,
            7 => CustomRaftLogType::EngineMeta,
            8 => CustomRaftLogType::ResolveLock,
            9 => CustomRaftLogType::SwitchMemTable,
            10 => CustomRaftLogType::TriggerTrimOverBound,
            12 => CustomRaftLogType::TxnFileRef,
            _ => panic!("unexpected custom raft log type: {:?}", v),
        }
    }
}

const HEADER_SIZE: usize = 2;

// CustomRaftLog is the raft log format for unistore to store
// Prewrite/Commit/PessimisticLock.
// | type(1) | version(1) | entries
// It reduces the cost of marshal/unmarshal and avoid DB lookup during apply.
#[derive(Debug)]
pub struct CustomRaftLog<'a> {
    pub(crate) data: &'a [u8],
}

impl<'a> CustomRaftLog<'a> {
    // For debug trace.
    pub fn get_raw(&self) -> &[u8] {
        self.data
    }

    pub fn new_from_data(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub fn get_type(&self) -> CustomRaftLogType {
        self.data[0].into()
    }

    // F: (key, val)
    pub fn iterate_lock<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], &[u8]),
    {
        let mut i = HEADER_SIZE;
        while i < self.data.len() {
            let key_len = LittleEndian::read_u16(&self.data[i..]) as usize;
            i += 2;
            let key = &self.data[i..i + key_len];
            i += key_len;
            let val_len = LittleEndian::read_u32(&self.data[i..]) as usize;
            i += 4;
            let val = &self.data[i..i + val_len];
            i += val_len;
            f(key, val)
        }
    }

    // F: (key, commit_ts)
    pub fn iterate_commit<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], u64),
    {
        let mut i = HEADER_SIZE;
        while i < self.data.len() {
            let key_len = LittleEndian::read_u16(&self.data[i..]) as usize;
            i += 2;
            let key = &self.data[i..i + key_len];
            i += key_len;
            let commit_ts = LittleEndian::read_u64(&self.data[i..]);
            i += 8;
            f(key, commit_ts)
        }
    }

    // F: (key, val, is_extra, del_lock, start_ts, commit_ts)
    pub fn iterate_one_pc<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], &[u8], bool, bool, u64, u64),
    {
        let mut i = HEADER_SIZE;
        while i < self.data.len() {
            let key_len = LittleEndian::read_u16(&self.data[i..]) as usize;
            i += 2;
            let key = &self.data[i..i + key_len];
            i += key_len;
            let val_len = LittleEndian::read_u32(&self.data[i..]) as usize;
            i += 4;
            let val = &self.data[i..i + val_len];
            i += val_len;
            let is_extra = self.data[i] > 0;
            i += 1;
            let del_lock = self.data[i] > 0;
            i += 1;
            let start_ts = LittleEndian::read_u64(&self.data[i..]);
            i += 8;
            let commit_ts = LittleEndian::read_u64(&self.data[i..]);
            i += 8;
            f(key, val, is_extra, del_lock, start_ts, commit_ts)
        }
    }

    // F: (key, start_ts, delete_lock)
    pub fn iterate_rollback<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], u64, bool),
    {
        let mut i = HEADER_SIZE;
        while i < self.data.len() {
            let key_len = LittleEndian::read_u16(&self.data[i..]) as usize;
            i += 2;
            let key = &self.data[i..i + key_len];
            i += key_len;
            let start_ts = LittleEndian::read_u64(&self.data[i..]);
            i += 8;
            let del = self.data[i];
            i += 1;
            f(key, start_ts, del > 0)
        }
    }

    pub fn iterate_del_lock<F>(&self, mut f: F)
    where
        F: FnMut(&[u8]),
    {
        let mut i = HEADER_SIZE;
        while i < self.data.len() {
            let key_len = LittleEndian::read_u16(&self.data[i..]) as usize;
            i += 2;
            let key = &self.data[i..i + key_len];
            i += key_len;
            f(key)
        }
    }

    pub(crate) fn get_change_set(&self) -> crate::Result<kvenginepb::ChangeSet> {
        let mut cs = kvenginepb::ChangeSet::new();
        cs.merge_from_bytes(&self.data[HEADER_SIZE..])?;
        Ok(cs)
    }

    pub fn iterate_resolve_lock(&self, mut f: impl FnMut(CustomRaftLogType, &[u8], u64, bool)) {
        let mut data = &self.data[HEADER_SIZE..];
        while !data.is_empty() {
            let tp: CustomRaftLogType = data.get_u8().into();
            match tp {
                CustomRaftLogType::Commit => {
                    let key_len = data.get_u16_le() as usize;
                    let key = &data[..key_len];
                    data = &data[key_len..];
                    let commit_ts = data.get_u64_le();
                    f(tp, key, commit_ts, true);
                }
                CustomRaftLogType::Rollback => {
                    let key_len = data.get_u16_le() as usize;
                    let key = &data[..key_len];
                    data = &data[key_len..];
                    let start_ts = data.get_u64_le();
                    let del = data.get_u8() > 0;
                    f(tp, key, start_ts, del);
                }
                _ => unreachable!("unexpected custom raft log type: {:?}", tp),
            }
        }
    }

    pub(crate) fn get_switch_mem_table(&self) -> u64 {
        let mut bin = &self.data[HEADER_SIZE..];
        bin.get_u64_le()
    }

    pub(crate) fn get_trigger_trim_over_bound(&self) -> TrimOverBoundParameter {
        let bin = &self.data[HEADER_SIZE..];
        TrimOverBoundParameter::unmarshal(bin)
    }

    pub fn is_txn_file_ref(&self) -> bool {
        is_txn_file_ref(self.data)
    }

    pub fn get_txn_file_ref(&self) -> crate::Result<kvenginepb::TxnFileRef> {
        let mut txn_file_ref = kvenginepb::TxnFileRef::new();
        txn_file_ref.merge_from_bytes(&self.data[HEADER_SIZE..])?;
        Ok(txn_file_ref)
    }

    pub fn is_affect_memtable(&self, tag: PeerTag, base_version: Option<u64>) -> AffectMemtable {
        match self.get_type() {
            CustomRaftLogType::EngineMeta => {
                let cs = self.get_change_set().unwrap();
                if let Some(snap) = get_any_snap_from_changeset(&cs) {
                    AffectMemtable::Persist {
                        data_seq: snap.data_sequence,
                    }
                } else if cs.has_flush() {
                    AffectMemtable::from_flush(tag, cs.get_flush().version.into(), base_version)
                } else if is_change_set_affect_mem_table(&cs) {
                    AffectMemtable::Write
                } else {
                    AffectMemtable::None
                }
            }
            CustomRaftLogType::SwitchMemTable | CustomRaftLogType::TriggerTrimOverBound => {
                AffectMemtable::None
            }
            _ => AffectMemtable::Write,
        }
    }
}

pub struct CustomBuilder {
    buf: Vec<u8>,
    cnt: i32,
}

impl Default for CustomBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CustomBuilder {
    pub fn new() -> Self {
        Self {
            buf: vec![0; HEADER_SIZE],
            cnt: 0,
        }
    }

    pub fn append_lock(&mut self, key: &[u8], val: &[u8]) {
        self.buf.put_u16_le(key.len() as u16);
        self.buf.extend_from_slice(key);
        self.buf.put_u32_le(val.len() as u32);
        self.buf.extend_from_slice(val);
        self.cnt += 1;
    }

    pub fn append_commit(&mut self, key: &[u8], commit_ts: u64) {
        self.buf.put_u16_le(key.len() as u16);
        self.buf.extend_from_slice(key);
        self.buf.put_u64_le(commit_ts);
        self.cnt += 1;
    }

    pub fn append_one_pc(
        &mut self,
        key: &[u8],
        val: &[u8],
        is_extra: bool,
        del_lock: bool,
        start_ts: u64,
        commit_ts: u64,
    ) {
        self.buf.put_u16_le(key.len() as u16);
        self.buf.extend_from_slice(key);
        self.buf.put_u32_le(val.len() as u32);
        self.buf.extend_from_slice(val);
        self.buf.put_u8(is_extra as u8);
        self.buf.put_u8(del_lock as u8);
        self.buf.put_u64_le(start_ts);
        self.buf.put_u64_le(commit_ts);
        self.cnt += 1;
    }

    // start_ts == 0 means delete_lock only.
    pub fn append_rollback(&mut self, key: &[u8], start_ts: u64, delete_lock: bool) {
        self.buf.put_u16_le(key.len() as u16);
        self.buf.extend_from_slice(key);
        self.buf.put_u64_le(start_ts);
        self.buf.put_u8(delete_lock as u8);
        self.cnt += 1;
    }

    pub fn append_del_lock(&mut self, key: &[u8]) {
        self.buf.put_u16_le(key.len() as u16);
        self.buf.extend_from_slice(key);
        self.cnt += 1;
    }

    pub fn set_change_set(&mut self, cs: &kvenginepb::ChangeSet) {
        assert_eq!(self.buf.len(), HEADER_SIZE);
        let data = cs.write_to_bytes().unwrap();
        self.buf.extend_from_slice(&data);
        self.set_type(CustomRaftLogType::EngineMeta);
    }

    pub fn set_switch_mem_table(&mut self, current_size: u64) {
        assert_eq!(self.buf.len(), HEADER_SIZE);
        self.buf.put_u64_le(current_size);
        self.set_type(CustomRaftLogType::SwitchMemTable);
    }

    pub fn set_trigger_trim_over_bound(&mut self, parameter: &TrimOverBoundParameter) {
        assert_eq!(self.buf.len(), HEADER_SIZE);
        let data = parameter.marshal();
        self.buf.extend_from_slice(&data);
        self.set_type(CustomRaftLogType::TriggerTrimOverBound);
    }

    pub fn set_txn_file(&mut self, txn_file_ref: &kvenginepb::TxnFileRef) {
        assert_eq!(self.buf.len(), HEADER_SIZE);
        let data = txn_file_ref.write_to_bytes().unwrap();
        self.buf.extend_from_slice(&data);
        self.set_type(CustomRaftLogType::TxnFileRef);
    }

    pub fn set_type(&mut self, tp: CustomRaftLogType) {
        self.buf[0] = tp as u8;
    }

    pub fn get_type(&self) -> CustomRaftLogType {
        self.buf[0].into()
    }

    // Some custom logs may contains multiple types of logs, e.g., resolve-lock can
    // contain both commit and rollback. We use type to distinguish them.
    pub fn append_type(&mut self, tp: CustomRaftLogType) {
        self.buf.push(tp as u8);
    }

    pub fn build(&mut self) -> CustomRequest {
        let mut req = CustomRequest::default();
        let buf = mem::take(&mut self.buf);
        req.set_data(buf);
        req
    }

    pub fn len(&self) -> usize {
        self.cnt as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub fn is_engine_meta_log(data: &[u8]) -> bool {
    data[0] == CustomRaftLogType::EngineMeta as u8
}

pub fn is_trigger_trim_over_bound(data: &[u8]) -> bool {
    data[0] == CustomRaftLogType::TriggerTrimOverBound as u8
}

pub fn is_txn_file_ref(data: &[u8]) -> bool {
    data[0] == CustomRaftLogType::TxnFileRef as u8
}

#[derive(Clone, Copy, Default, Debug, PartialEq)]
pub struct TrimOverBoundParameter {
    pub source_shard: Option<IdVer>, // `None` means no trim.
    pub target_shard: Option<IdVer>,
}

impl TrimOverBoundParameter {
    pub fn marshal(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            2 + self.source_shard.is_some() as usize * 2 * U64_SIZE
                + self.target_shard.is_some() as usize * 2 * U64_SIZE,
        );

        if let Some(source) = self.source_shard {
            buf.put_u8(1);
            buf.put_u64_le(source.id);
            buf.put_u64_le(source.ver);
        } else {
            buf.put_u8(0);
        }

        if let Some(target) = self.target_shard {
            buf.put_u8(1);
            buf.put_u64_le(target.id);
            buf.put_u64_le(target.ver);
        } else {
            buf.put_u8(0);
        }

        buf
    }

    pub fn unmarshal(mut data: &[u8]) -> Self {
        if data.is_empty() {
            return Self::default();
        }
        let source_shard = if data.get_u8() == 1 {
            Some(IdVer::new(data.get_u64_le(), data.get_u64_le()))
        } else {
            None
        };
        let target_shard = if data.get_u8() == 1 {
            Some(IdVer::new(data.get_u64_le(), data.get_u64_le()))
        } else {
            None
        };
        Self {
            source_shard,
            target_shard,
        }
    }

    pub fn source_shard_id(&self) -> Option<u64> {
        self.source_shard.map(|x| x.id)
    }

    pub fn target_shard_id(&self) -> Option<u64> {
        self.target_shard.map(|x| x.id)
    }

    pub fn is_for_shard(&self, shard_id: u64) -> bool {
        Some(shard_id) == self.source_shard_id() || Some(shard_id) == self.target_shard_id()
    }
}

pub enum AffectMemtable {
    None,
    Write,
    Persist { data_seq: u64 },
}

impl AffectMemtable {
    fn from_flush(tag: PeerTag, snap_version: SnapVersion, base_version: Option<u64>) -> Self {
        let Some(base_version) = base_version else {
            // Shard is not initialized yet. Should not happen.
            warn!("{} AffectMemtable: no base version", tag);
            debug_assert!(false);
            return Self::None;
        };
        let Some(data_seq) = snap_version.into_inner().checked_sub(base_version) else {
            warn!("{} AffectMemtable: invalid snap version", tag;
                "snap_ver" => snap_version, "base_ver" => base_version);
            debug_assert!(false);
            return Self::None;
        };
        Self::Persist { data_seq }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_custom_log() {
        let mut builder = CustomBuilder::new();
        builder.set_switch_mem_table(2022);
        let req = builder.build();
        let cl = CustomRaftLog::new_from_data(req.get_data());
        assert_eq!(cl.get_type(), CustomRaftLogType::SwitchMemTable);
        assert_eq!(cl.get_switch_mem_table(), 2022);
    }

    #[test]
    fn test_trim_over_bound_parameter() {
        let cases = vec![
            (None, None),
            (Some(IdVer { id: 1, ver: 2 }), None),
            (None, Some(IdVer { id: 2, ver: 3 })),
            (Some(IdVer { id: 1, ver: 2 }), Some(IdVer { id: 2, ver: 3 })),
        ];

        for (source_shard, target_shard) in cases {
            let trim_over_bound = TrimOverBoundParameter {
                source_shard,
                target_shard,
            };
            assert_eq!(
                trim_over_bound,
                TrimOverBoundParameter::unmarshal(trim_over_bound.marshal().as_slice())
            );
        }

        assert_eq!(
            TrimOverBoundParameter::default(),
            TrimOverBoundParameter::unmarshal(vec![].as_slice())
        );
    }

    #[test]
    fn test_custom_raft_log_types() {
        // CustomRaftLogType tags from 1 to 12, except 11.
        let ignored = vec![11u8];
        for i in 1u8..=12 {
            if ignored.contains(&i) {
                continue;
            }
            let tp: CustomRaftLogType = i.into();
            assert_eq!(i, tp as u8);
        }

        // Test invalid type will panic.
        for &i in &ignored {
            let result = std::panic::catch_unwind(|| {
                let _: CustomRaftLogType = i.into();
            });
            assert!(result.is_err());
        }

        // 13 and above are invalid.
        for i in 13u8..=20 {
            let result = std::panic::catch_unwind(|| {
                let _: CustomRaftLogType = i.into();
            });
            assert!(result.is_err());
        }
    }
}
