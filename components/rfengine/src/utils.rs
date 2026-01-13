// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::{Path, PathBuf};

use api_version::ApiV2;
use bytes::{BufMut, Bytes, BytesMut};
use kvproto::raft_serverpb::RegionLocalState;
use protobuf::Message as _;
use regex::Regex;
use tikv_util::info;

pub const RAFT_STATE_KEY_BYTE: u8 = 1;
pub const REGION_META_KEY_BYTE: u8 = 2;
pub const REGION_META_KEY_PREFIX: &[u8] = &[REGION_META_KEY_BYTE];
pub const STORE_IDENT_KEY: &[u8] = &[3];
pub const PREPARE_BOOTSTRAP_KEY: &[u8] = &[4];
pub const KV_ENGINE_META_KEY: &[u8] = &[5];
pub const RAFT_TRUNCATED_STATE_KEY: &[u8] = &[6];
pub const KV_ENGINE_META_DIFF_KEY: &[u8] = &[7];
pub const KV_ENGINE_META_SNAP_DIFF_KEY: &[u8] = &[8];

pub const LAST_WAL_CHUNK_SUFFIX: &str = ".last";

pub fn raft_state_key(version: u64) -> Bytes {
    let mut key = BytesMut::with_capacity(5);
    key.put_u8(RAFT_STATE_KEY_BYTE);
    key.put_u32(version as u32);
    key.freeze()
}

pub fn region_state_key(version: u64) -> Bytes {
    let mut key = BytesMut::with_capacity(5);
    key.put_u8(REGION_META_KEY_BYTE);
    key.put_u32(version as u32);
    key.freeze()
}

pub fn compress_lz4(uncompressed: &[u8], compressed_buf: &mut Vec<u8>) -> std::io::Result<usize> {
    let compress_bound: i32 = unsafe { lz4::liblz4::LZ4_compressBound(uncompressed.len() as i32) };
    let existed_bytes = compressed_buf.len();
    compressed_buf.resize(existed_bytes + 4 + compress_bound as usize, 0);
    let size = lz4::block::compress_to_buffer(
        uncompressed,
        None,
        true,
        &mut compressed_buf[existed_bytes..],
    )?;
    compressed_buf.truncate(existed_bytes + size);
    Ok(existed_bytes + size)
}

pub fn decompress_lz4(content: &[u8]) -> std::io::Result<Vec<u8>> {
    lz4::block::decompress(content, None)
}

// Ref: https://github.com/10XGenomics/lz4-rs/blob/v1.25/src/block/mod.rs#L169
// Must use the above `compress_lz4` to compress (and `prepend_size` must be
// true).
// Besides, it's safe as `decompress_to_buffer` will check the size again.
pub fn get_lz4_decompressed_size(src: &[u8]) -> std::io::Result<usize> {
    if src.len() < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Source buffer must at least contain size prefix.",
        ));
    }
    let size =
        (src[0] as i32) | (src[1] as i32) << 8 | (src[2] as i32) << 16 | (src[3] as i32) << 24;
    Ok(size as usize)
}

pub fn decompress_lz4_to_buffer(src: &[u8], buffer: &mut [u8]) -> std::io::Result<usize> {
    lz4::block::decompress_to_buffer(src, None, buffer)
}

pub(crate) fn raft_log_file_name(dir: &Path, peer_id: u64, first: u64, last: u64) -> PathBuf {
    dir.join(format!(
        "{:016x}_{:016x}_{:016x}.rlog",
        peer_id, first, last,
    ))
}

pub fn wal_file_key(store_id: u64, epoch_id: u32, start_off: u64, end_off: u64) -> String {
    format!(
        "{:016x}/e{:08x}/{:016x}_{:016x}.wal",
        store_id, epoch_id, start_off, end_off
    )
}

pub fn snapshot_store_meta_key(store_id: u64, delayed_to_epoch: u32) -> String {
    format!(
        "store_backup/{:016x}/snapshots/m{:08x}.meta",
        store_id, delayed_to_epoch
    )
}

pub fn snapshot_rlog_key(store_id: u64, delayed_to_epoch: u32) -> String {
    format!(
        "store_backup/{:016x}/snapshots/r{:08x}.rlog",
        store_id, delayed_to_epoch
    )
}

pub(crate) fn snapshot_rlog_key_suffix(delayed_to_epoch: u32) -> String {
    format!("{:08x}.rlog", delayed_to_epoch)
}

pub(crate) fn snapshot_rlog_key_prefix(store_id: u64) -> String {
    format!("store_backup/{:016x}/snapshots/r", store_id)
}

pub fn parse_delayed_to_epoch_from_snapshot_key(key: Option<&str>) -> Option<u32> {
    key.and_then(|key| {
        let re = Regex::new(r"r([0-9a-fA-F]+)\.rlog").unwrap();
        if let Some(captures) = re.captures(key) {
            if let Some(epoch_hex) = captures.get(1) {
                let epoch = epoch_hex.as_str();
                return Some(u32::from_str_radix(epoch, 16).unwrap());
            }
        }
        None
    })
}

pub fn parse_wal_chunk_key(
    key: Option<&str>,
) -> Option<(
    u32,  // epoch
    u64,  // start_off
    u64,  // end_off
    bool, // is_last_chunk
)> {
    key.and_then(|key| {
        lazy_static::lazy_static! {
            static ref RE: Regex = Regex::new(r"e([0-9a-fA-F]+)_([0-9a-fA-F]+)_([0-9a-fA-F]+)\.wal").unwrap();
        }

        let chunk_name = if let Some(pos) = key.rfind('/') {
            &key[pos + 1..]
        } else {
            key
        };

        if let Some(captures) = RE.captures(chunk_name) {
            let epoch_hex = captures.get(1).unwrap().as_str();
            let start_off_hex = captures.get(2).unwrap().as_str();
            let end_off_hex = captures.get(3).unwrap().as_str();
            let epoch = u32::from_str_radix(epoch_hex, 16).unwrap();
            let start_off = u64::from_str_radix(start_off_hex, 16).unwrap();
            let end_off = u64::from_str_radix(end_off_hex, 16).unwrap();
            // `start_off == end_off` is valid (for last chunk).
            return (start_off <= end_off).then(|| {
                (
                    epoch,
                    start_off,
                    end_off,
                    key.ends_with(LAST_WAL_CHUNK_SUFFIX),
                )
            });
        }
        None
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalChunkMeta {
    pub key: String,
    pub epoch: u32,
    pub start_off: u64,
    pub end_off: u64,
    pub last: bool,
}

impl WalChunkMeta {
    pub fn size(&self) -> u64 {
        self.end_off - self.start_off
    }
}

impl std::convert::TryFrom<String> for WalChunkMeta {
    type Error = String; // err_msg

    fn try_from(key: String) -> Result<Self, Self::Error> {
        let (epoch, start_off, end_off, last) =
            parse_wal_chunk_key(Some(&key)).ok_or_else(|| {
                debug_assert!(false, "invalid pattern: {}", &key);
                format!("invalid pattern: {}", &key)
            })?;
        Ok(WalChunkMeta {
            key,
            epoch,
            start_off,
            end_off,
            last,
        })
    }
}

/// Try to get integral WAL chunks with maximum length, and ignore some
/// overlapping separated chunks.
///
/// The algorithm is simple and greedy, and may not be optimal. It just picks
/// the largest one from chunks with same start offset.
///
/// It's based on how abnormal chunks are generated, which would be lost, or
/// re-written with larger size.
///
/// See https://github.com/tidbcloud/cloud-storage-engine/issues/2033 for details.
pub fn get_integral_wal_chunks(
    chunks: &[WalChunkMeta],
    epoch_id: u32,
) -> (
    Vec<WalChunkMeta>, // integral_chunks
    u64,               // last_end_off
    bool,              // has_last_chunk
) {
    if chunks.is_empty() {
        return (vec![], 0, false);
    }

    let first_chunk = chunks.first().unwrap();
    if first_chunk.start_off != 0 {
        // Return as no chunk.
        return (vec![], 0, false);
    }
    let mut last_start_off = first_chunk.start_off;
    let mut last_end_off = first_chunk.end_off;
    let mut has_last_chunk = first_chunk.last;

    let mut integral_chunks = Vec::with_capacity(chunks.len());
    integral_chunks.push(first_chunk.clone());

    for chunk in chunks.iter().skip(1) {
        if chunk.epoch != epoch_id {
            continue;
        }

        if chunk.start_off == last_end_off {
            integral_chunks.push(chunk.clone());
            last_start_off = chunk.start_off;
            last_end_off = chunk.end_off;
            has_last_chunk = chunk.last;
        } else if chunk.start_off == last_start_off {
            debug_assert!(
                chunk.end_off > last_end_off,
                "disorder chunks: {:?}",
                chunks
            );
            integral_chunks.pop();
            integral_chunks.push(chunk.clone());
            last_end_off = chunk.end_off;
            has_last_chunk = chunk.last;
        } else if chunk.start_off > last_end_off {
            // Some chunks in the middle missed.
            break;
        } else {
            // last_start_off < start_off < last_end_off, ignore this chunk.
        }
    }

    if integral_chunks.len() < chunks.len() {
        info!(
            "get integral wal chunks: ignore some abnormal chunks";
            "integral_chunks" => ?integral_chunks,
            "all_chunks" => ?chunks,
        );
    }
    (integral_chunks, last_end_off, has_last_chunk)
}

pub fn wal_chunk_file_key(store_id: u64, epoch_id: u32, start_off: u64, end_off: u64) -> String {
    wal_chunk_file_key_with_suffix(store_id, epoch_id, start_off, end_off, false)
}

pub fn last_wal_chunk_file_key(
    store_id: u64,
    epoch_id: u32,
    start_off: u64,
    end_off: u64,
) -> String {
    wal_chunk_file_key_with_suffix(store_id, epoch_id, start_off, end_off, true)
}

fn wal_chunk_file_key_with_suffix(
    store_id: u64,
    epoch_id: u32,
    start_off: u64,
    end_off: u64,
    last: bool,
) -> String {
    let suffix = if last { LAST_WAL_CHUNK_SUFFIX } else { "" };
    format!(
        "store_backup/{:016x}/wal_chunks/e{:08x}_{:016x}_{:016x}.wal{}",
        store_id, epoch_id, start_off, end_off, suffix,
    )
}

pub fn wal_chunk_file_prefix(store_id: u64, epoch_id: u32) -> String {
    format!(
        "store_backup/{:016x}/wal_chunks/e{:08x}_",
        store_id, epoch_id
    )
}

pub fn wal_chunk_file_suffix(start_off: u64, end_off: u64) -> String {
    format!("{:016x}_{:016x}.wal", start_off, end_off)
}

pub fn get_keyspace_id_from_peer(peer_meta: &rfenginepb::PeerMeta) -> Option<u32> {
    if peer_meta.keyspace_id > 0 {
        return Some(peer_meta.keyspace_id);
    }
    peer_meta
        .get_states()
        .iter()
        .rev() // the states are got from BTreeMap iter, so the last one is latest.
        .find(|s| s.get_key().starts_with(REGION_META_KEY_PREFIX))
        .map(|state| {
            let mut local_state = RegionLocalState::default();
            local_state.merge_from_bytes(state.get_value()).unwrap();
            ApiV2::get_u32_keyspace_id_by_key(local_state.get_region().get_start_key())
                .unwrap_or_default()
        })
}

#[cfg(test)]
pub mod test_util {
    use std::{sync::Once, time::Duration};

    use api_version::api_v2::TXN_KEY_PREFIX;
    use byteorder::{BigEndian, ByteOrder};
    use bytes::{Buf, BufMut, BytesMut};
    use kvproto::raft_serverpb::RegionLocalState;
    use protobuf::Message;
    use raft_proto::{eraftpb, eraftpb::EntryType};
    use tikv_util::time::Instant;

    use crate::{RfEngine, WriteBatch, region_state_key};

    static INIT: Once = Once::new();

    pub fn init_logger() {
        INIT.call_once(test_util::init_log_for_test);
    }

    #[must_use]
    pub fn try_wait<F>(f: F, seconds: usize) -> bool
    where
        F: Fn() -> bool,
    {
        let begin = Instant::now_coarse();
        let timeout = Duration::from_secs(seconds as u64);
        while begin.saturating_elapsed() < timeout {
            if f() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(100))
        }
        false
    }

    pub fn get_txn_startkey_prefix(keyspace_id: u32) -> [u8; 4] {
        let mut keyspace_id_buf = [0u8; 4];
        BigEndian::write_u32(&mut keyspace_id_buf, keyspace_id);
        keyspace_id_buf[0] = TXN_KEY_PREFIX;
        keyspace_id_buf
    }

    pub fn get_txn_endkey_prefix(keyspace_id: u32) -> [u8; 4] {
        let mut keyspace_id_buf = get_txn_startkey_prefix(keyspace_id);
        keyspace_id_buf[3] += 1;
        keyspace_id_buf
    }

    pub fn make_log_data(index: u64, size: usize) -> eraftpb::Entry {
        let mut entry = eraftpb::Entry::new();
        entry.set_entry_type(eraftpb::EntryType::EntryConfChange);
        entry.set_index(index);
        entry.set_term(1);

        let mut data = BytesMut::with_capacity(size);
        data.resize(size, 0);
        entry.set_data(data.freeze());
        entry
    }

    pub fn make_state_kv(key_byte: u8, idx: u64) -> (BytesMut, BytesMut) {
        let mut key = BytesMut::new();
        key.put_u8(key_byte);
        let mut val = BytesMut::new();
        val.put_u64_le(idx);
        (key, val)
    }

    pub fn make_region_state(region_epoch: u64, keyspace_id: u32) -> (Vec<u8>, Vec<u8>) {
        let key = region_state_key(region_epoch).to_vec();

        let mut local_stat = RegionLocalState::default();
        local_stat.mut_region().start_key = get_txn_startkey_prefix(keyspace_id).to_vec();
        local_stat.mut_region().end_key = get_txn_endkey_prefix(keyspace_id).to_vec();
        let val = local_stat.write_to_bytes().unwrap();

        (key, val)
    }

    pub fn new_raft_entry(
        tp: EntryType,
        term: u64,
        index: u64,
        data: &[u8],
        context: u8,
    ) -> eraftpb::Entry {
        let mut entry = eraftpb::Entry::new();
        entry.set_entry_type(tp);
        entry.set_term(term);
        entry.set_index(index);
        entry.set_data(data.to_vec().into());
        if context > 0 {
            entry.set_context(vec![context].into());
        }
        entry
    }

    pub fn prepare_rfengine(engine: &RfEngine) {
        prepare_rfengine_with_idx(engine, 1, 1050);
    }

    pub fn prepare_rfengine_with_idx(engine: &RfEngine, start_idx: u64, end_idx: u64) {
        let mut wb = WriteBatch::new();
        for peer_id in 1..=10_u64 {
            let (key, val) = make_state_kv(3, 1);
            let region_id = peer_id + 1;
            wb.set_state(peer_id, region_id, 1, key.chunk(), val.chunk());
        }
        engine.write(wb).unwrap();
        for idx in start_idx..=end_idx {
            let mut wb = WriteBatch::new();
            for peer_id in 1..=10_u64 {
                let region_id = peer_id + 1;
                wb.append_raft_log(peer_id, region_id, 1, &make_log_data(idx, 128));
                let (key, val) = make_state_kv(1, idx);
                wb.set_state(peer_id, region_id, 1, key.chunk(), val.chunk());
            }
            engine.write(wb).unwrap();
        }
        assert_eq!(engine.peers.peers.len(), 10);
    }

    pub fn get_epoch_file_off(engine: &RfEngine) -> (u32, u64) {
        let writer = engine.writer.lock().unwrap();
        (writer.get_epoch_id(), writer.get_file_off())
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use super::*;

    #[test]
    fn test_parse_snapshot_key() {
        let cases = vec![
            (snapshot_rlog_key(1, 2), Some(2)),
            (snapshot_rlog_key(1, 1000), Some(1000)),
            ("xxxxx".to_string(), None),
            ("".to_string(), None),
        ];
        for case in cases {
            assert_eq!(
                parse_delayed_to_epoch_from_snapshot_key(Some(case.0.as_str())),
                case.1
            );
        }
        assert_eq!(parse_delayed_to_epoch_from_snapshot_key(None), None);
    }

    #[test]
    fn test_parse_wal_chunk_key() {
        let cases = vec![
            (wal_chunk_file_key(1, 1, 10, 20), Some((1, 10, 20, false))),
            (
                wal_chunk_file_key(1, 1000, 10, 20),
                Some((1000, 10, 20, false)),
            ),
            (
                last_wal_chunk_file_key(1, 1000, 1000, 2000),
                Some((1000, 1000, 2000, true)),
            ),
            ("xxxxx".to_string(), None),
            ("".to_string(), None),
        ];
        for case in cases {
            assert_eq!(parse_wal_chunk_key(Some(case.0.as_str())), case.1);
        }
        assert_eq!(parse_wal_chunk_key(None), None);
    }

    #[test]
    fn test_lz4_compress() {
        let data = b"hello world".repeat(100);
        let mut compressed = Vec::new();
        let size = super::compress_lz4(&data, &mut compressed).unwrap();
        assert!(size < data.len());
        let decompressed = super::decompress_lz4(&compressed).unwrap();
        assert_eq!(data, decompressed);

        let mut compressed_with_data = b"header".to_vec();
        let _ = super::compress_lz4(&data, &mut compressed_with_data).unwrap();
        assert_eq!(compressed_with_data.len(), size + 6);
        assert_eq!(&compressed_with_data[0..6], b"header".as_slice());
        assert_eq!(&compressed_with_data[6..], compressed.as_slice());
    }

    #[test]
    fn test_get_max_integral_wal_chunks() {
        let cases: Vec<(
            Vec<(u64, u64, bool)>, // (start_off, end_off, is_last)
            Vec<usize>,            // expected chunks (index of input)
            u64,                   // last_end_off
            bool,                  // has_last_chunk
        )> = vec![
            (vec![], vec![], 0, false),
            (vec![(1, 100, true)], vec![], 0, false),
            (vec![(0, 100, false)], vec![0], 100, false),
            (vec![(0, 100, true)], vec![0], 100, true),
            (
                vec![(0, 100, false), (100, 200, true)],
                vec![0, 1],
                200,
                true,
            ),
            (vec![(0, 100, false), (0, 200, true)], vec![1], 200, true),
            (vec![(0, 100, false), (50, 200, true)], vec![0], 100, false),
            (vec![(0, 100, false), (101, 200, true)], vec![0], 100, false),
            (
                vec![(0, 100, false), (100, 200, false), (200, 300, true)],
                vec![0, 1, 2],
                300,
                true,
            ),
            (
                vec![
                    (0, 100, false),
                    (100, 200, false),
                    (200, 201, false),
                    (200, 300, true),
                ],
                vec![0, 1, 3],
                300,
                true,
            ),
            (
                vec![
                    (0, 100, false),
                    (100, 200, false),
                    (200, 201, false),
                    (200, 202, false),
                    (200, 203, false),
                    (200, 204, false),
                    (204, 300, true),
                ],
                vec![0, 1, 5, 6],
                300,
                true,
            ),
            (
                vec![
                    (0, 100, false),
                    (100, 200, false),
                    (200, 201, false),
                    (200, 202, false),
                    (200, 203, false),
                    (200, 204, false),
                    (204, 300, false),
                    (300, 300, true),
                ],
                vec![0, 1, 5, 6, 7],
                300,
                true,
            ),
        ];

        fn make_chunks(chunks: Vec<(u64, u64, bool)>) -> Vec<WalChunkMeta> {
            chunks
                .into_iter()
                .map(|(start_off, end_off, is_last)| {
                    if is_last {
                        last_wal_chunk_file_key(1, 1, start_off, end_off)
                    } else {
                        wal_chunk_file_key(1, 1, start_off, end_off)
                    }
                    .try_into()
                    .unwrap()
                })
                .collect::<Vec<_>>()
        }

        fn pick_chunks(chunks: &[WalChunkMeta], idx: Vec<usize>) -> Vec<WalChunkMeta> {
            let mut res = Vec::with_capacity(idx.len());
            for i in idx {
                res.push(chunks[i].clone())
            }
            res
        }

        for (chunks, expected, last_end_off, has_last_chunk) in cases {
            let chunks = make_chunks(chunks);
            let expected_chunks = pick_chunks(&chunks, expected);
            assert_eq!(
                get_integral_wal_chunks(&chunks, 1),
                (expected_chunks, last_end_off, has_last_chunk)
            );
        }
    }
}
