// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::Buf;
use codec::{byte::MemComparableByteCodec, number::NumberDecoder};
use engine_traits::Result;
use tikv_util::codec::{
    Error, bytes as codecBytes,
    number::{self, NumberEncoder},
};
use txn_types::{Key, TimeStamp};

use super::*;

pub const RAW_KEY_PREFIX: u8 = b'r';
pub const RAW_KEY_PREFIX_END: u8 = RAW_KEY_PREFIX + 1;
pub const TXN_KEY_PREFIX: u8 = b'x';
pub const TIDB_META_KEY_PREFIX: u8 = b'm';
pub const TIDB_TABLE_KEY_PREFIX: u8 = b't';
pub const DEFAULT_KEYSPACE_ID: u32 = 0;
pub const DEFAULT_KEY_SPACE_ID: [u8; 3] = [0, 0, 0]; // reserve 3 bytes for key space id.
pub const DEFAULT_KEY_SPACE_ID_END: [u8; 3] = [0, 0, 1];
pub const KEYSPACE_ID_LEN: usize = DEFAULT_KEY_SPACE_ID.len();
pub const KEYSPACE_PREFIX_LEN: usize = KEYSPACE_ID_LEN + 1;
pub const UNKNOWN_KEYSPACE_ID: [u8; 3] = [0xFF, 0xFF, 0xFF];

pub const TIDB_RANGES: &[(&[u8], &[u8])] = &[
    (&[TIDB_META_KEY_PREFIX], &[TIDB_META_KEY_PREFIX + 1]),
    (&[TIDB_TABLE_KEY_PREFIX], &[TIDB_TABLE_KEY_PREFIX + 1]),
];
pub const TIDB_RANGES_COMPLEMENT: &[(&[u8], &[u8])] = &[
    (&[], &[TIDB_META_KEY_PREFIX]),
    (&[TIDB_META_KEY_PREFIX + 1], &[TIDB_TABLE_KEY_PREFIX]),
    (&[TIDB_TABLE_KEY_PREFIX + 1], &[]),
];

bitflags::bitflags! {
    struct ValueMeta: u8 {
        const EXPIRE_TS = 0b0000_0001;
        const DELETE_FLAG = 0b0000_0010;
    }
}

impl KvFormat for ApiV2 {
    const TAG: ApiVersion = ApiVersion::V2;
    #[cfg(any(test, feature = "testexport"))]
    const CLIENT_TAG: ApiVersion = ApiVersion::V2;
    const IS_TTL_ENABLED: bool = true;

    fn parse_key_mode(key: &[u8]) -> KeyMode {
        if key.len() < KEYSPACE_PREFIX_LEN {
            return KeyMode::Unknown;
        }

        match key[0] {
            TXN_KEY_PREFIX => KeyMode::Txn,
            TIDB_META_KEY_PREFIX | TIDB_TABLE_KEY_PREFIX => KeyMode::Tidb,
            _ => KeyMode::Unknown,
        }
    }

    fn parse_range_mode(range: (Option<&[u8]>, Option<&[u8]>)) -> KeyMode {
        match range {
            (Some(start), Some(end))
                if !start.is_empty()
                    && !end.is_empty()
                    && (start[0] == end[0] ||
                        // Special case to represent "".."" within a key mode
                        (end == [start[0] + 1])) =>
            {
                Self::parse_key_mode(start)
            }
            _ => KeyMode::Unknown,
        }
    }

    fn strip_keyspace(key: &[u8]) -> Result<(Option<u32>, &[u8])> {
        if key.len() < KEYSPACE_PREFIX_LEN {
            return Err(Error::KeyLength.into());
        }
        let id = Self::get_u32_keyspace_id_by_key(key);
        Ok((id, &key[KEYSPACE_PREFIX_LEN..]))
    }
}

impl ApiV2 {
    pub fn append_ts_on_encoded_bytes(encoded_bytes: &mut Vec<u8>, ts: TimeStamp) {
        debug_assert!(is_valid_encoded_bytes(encoded_bytes, false));
        debug_assert!(is_valid_ts(ts));
        encoded_bytes.encode_u64_desc(ts.into_inner()).unwrap();
    }

    fn get_encode_len(src_len: usize) -> usize {
        MemComparableByteCodec::encoded_len(src_len) + number::U64_SIZE
    }

    pub fn get_rawkv_range() -> (u8, u8) {
        (RAW_KEY_PREFIX, RAW_KEY_PREFIX_END)
    }

    pub fn decode_ts_from(key: &[u8]) -> Result<TimeStamp> {
        let ts = Key::decode_ts_from(key)?;
        Ok(ts)
    }

    pub fn split_ts(key: &[u8]) -> Result<(&[u8], TimeStamp)> {
        Ok(Key::split_on_ts_for(key)?)
    }

    pub fn add_prefix(key: &[u8], key_space: &[u8]) -> Vec<u8> {
        let mut apiv2_key =
            Vec::with_capacity(ApiV2::get_encode_len(key.len() + key_space.len() + 1));
        apiv2_key.push(RAW_KEY_PREFIX);
        apiv2_key.extend(key_space); // Reserved 3 bytes for key space id.
        apiv2_key.extend(key);
        apiv2_key
    }

    /// Return `None` when the key is not an API V2 key.
    pub fn get_u32_keyspace_id_by_key(key: &[u8]) -> Option<u32> {
        let key_mode = Self::parse_key_mode(key);
        if key_mode == KeyMode::Txn {
            Some([0, key[1], key[2], key[3]].as_slice().get_u32())
        } else {
            None
        }
    }

    pub fn get_keyspace_id_str(key: &[u8]) -> String {
        Self::get_u32_keyspace_id_by_key(key)
            .map(|id| id.to_string())
            .unwrap_or_default()
    }

    pub fn get_txn_keyspace_range(keyspace_id: u32) -> (Vec<u8>, Vec<u8>) {
        let start_key = Self::get_txn_keyspace_prefix(keyspace_id);
        let end_key = Self::get_txn_keyspace_prefix(keyspace_id + 1);
        (start_key, end_key)
    }

    pub fn get_txn_keyspace_prefix(keyspace_id: u32) -> Vec<u8> {
        let mut start_key = keyspace_id.to_be_bytes();
        start_key[0] = TXN_KEY_PREFIX;
        start_key.to_vec()
    }

    #[inline]
    pub fn is_default_keyspace(keyspace_id: u32) -> bool {
        keyspace_id == DEFAULT_KEYSPACE_ID
    }

    pub fn get_keyspace_prefix_by_id(keyspace_id: u32) -> Vec<u8> {
        if Self::is_default_keyspace(keyspace_id) {
            return vec![];
        }
        let mut start_key = keyspace_id.to_be_bytes();
        start_key[0] = TXN_KEY_PREFIX;
        start_key.to_vec()
    }

    pub fn get_keyspace_end_by_id(keyspace_id: u32) -> Vec<u8> {
        if Self::is_default_keyspace(keyspace_id) {
            return vec![TXN_KEY_PREFIX, 0, 0, 0];
        }
        Self::get_keyspace_prefix_by_id(keyspace_id + 1)
    }

    pub fn get_keyspace_range_by_id(keyspace_id: u32) -> (Vec<u8>, Vec<u8>) {
        let start_key = Self::get_keyspace_prefix_by_id(keyspace_id);
        let end_key = Self::get_keyspace_end_by_id(keyspace_id);
        (start_key, end_key)
    }

    pub fn get_keyspace_prefix(key: &[u8]) -> Option<&[u8]> {
        matches!(ApiV2::parse_key_mode(key), KeyMode::Txn).then(|| &key[0..KEYSPACE_PREFIX_LEN])
    }

    /// Check whether two keys are in the same keyspace.
    ///
    /// For keys of ApiV2 (including Raw and Txn), they are in the same keyspace
    /// only when the keyspace_id are equal.
    ///
    /// For keys of ApiV1 (including Unknown and TiDB), consider they are in the
    /// same keyspace.
    pub fn is_belongs_to_same_keyspace(first_key: &[u8], second_key: &[u8]) -> bool {
        match (
            Self::get_keyspace_prefix(first_key),
            Self::get_keyspace_prefix(second_key),
        ) {
            (Some(first_keyspace_prefix), Some(second_keyspace_prefix)) => {
                first_keyspace_prefix == second_keyspace_prefix
            }
            (Some(_), None) | (None, Some(_)) => false,
            (None, None) => true,
        }
    }

    #[inline]
    pub fn is_txn_key(key: &[u8]) -> bool {
        key.len() >= KEYSPACE_PREFIX_LEN && key[0] == TXN_KEY_PREFIX
    }

    pub fn get_keyspace_table_id(key: &[u8]) -> Option<(u32, i64)> {
        let keyspace_id = Self::get_u32_keyspace_id_by_key(key)?;
        let mut buf = &key[KEYSPACE_PREFIX_LEN..];
        if buf.is_empty() || buf[0] != b't' {
            return None;
        }
        buf = &buf[1..];
        let table_id = buf.read_i64().ok()?;
        Some((keyspace_id, table_id))
    }

    pub const ENCODED_LOGICAL_DELETE: [u8; 1] = [ValueMeta::DELETE_FLAG.bits];
}

// Note: `encoded_bytes` may not be `KeyMode::Raw`.
// E.g., backup service accept an exclusive end key just beyond the scope of raw
// keys. The validity is ensured by client & Storage interfaces.
#[inline]
fn is_valid_encoded_bytes(mut encoded_bytes: &[u8], with_ts: bool) -> bool {
    codecBytes::decode_bytes(&mut encoded_bytes, false).is_ok()
        && encoded_bytes.len() == number::U64_SIZE * (with_ts as usize)
}

/// TimeStamp::zero is not acceptable, as such entries can not be retrieved by
/// RawKV MVCC. See `RawMvccSnapshot::seek_first_key_value_cf`.
#[inline]
fn is_valid_ts(ts: TimeStamp) -> bool {
    !ts.is_zero()
}

#[inline]
pub fn is_whole_keyspace_range(mut start: &[u8], mut end: &[u8]) -> bool {
    start.len() == KEYSPACE_PREFIX_LEN
        && end.len() == KEYSPACE_PREFIX_LEN
        && start[0] == TXN_KEY_PREFIX
        && end[0] == TXN_KEY_PREFIX
        && start.get_u32() + 1 == end.get_u32()
}

#[inline]
pub fn is_one_or_multi_whole_keyspace_range(mut start: &[u8], mut end: &[u8]) -> bool {
    start.len() == KEYSPACE_PREFIX_LEN
        && end.len() == KEYSPACE_PREFIX_LEN
        && start[0] == TXN_KEY_PREFIX
        && end[0] == TXN_KEY_PREFIX
        && start.get_u32() < end.get_u32()
}

#[cfg(test)]
mod tests {
    use txn_types::Key;

    use crate::{
        ApiV2,
        api_v2::{TXN_KEY_PREFIX, is_one_or_multi_whole_keyspace_range, is_whole_keyspace_range},
    };

    #[test]
    #[ignore]
    fn test_append_ts_on_encoded_bytes() {
        let cases = vec![
            (true, vec![b'r', 2, 3, 4, 0, 0, 0, 0, 0xfb], 10),
            (
                true,
                vec![
                    b'r', 2, 3, 4, 5, 6, 7, 8, 0xff, 1, 2, 3, 4, 0, 0, 0, 0, 0xfb,
                ],
                20,
            ),
            (false, vec![b'r', 2, 3, 4, 0, 0, 0, 0, 0xfb], 0), // ts 0 is invalid.
            (false, vec![1, 2, 3, 4, 5, 6, 7, 8, 9], 1),
            (false, vec![b'r', 2, 3, 4, 5, 6, 7, 8], 2),
            (false, vec![b'r', 2, 3, 4, 5, 6, 7, 8, 9, 10], 3),
            (false, vec![b'r', 2, 3, 4, 0, 0, 1, 0, 0xfb], 4),
            (false, vec![b'r', 2, 3, 4, 5, 6, 7, 8, 0xf6], 5),
        ];

        for (idx, (is_valid, mut bytes, ts)) in cases.into_iter().enumerate() {
            if is_valid {
                let expected = Key::from_encoded(bytes.clone()).append_ts(ts.into());
                ApiV2::append_ts_on_encoded_bytes(&mut bytes, ts.into());
                assert_eq!(&bytes, expected.as_encoded(), "case {}", idx);
            } else {
                let r = panic_hook::recover_safe(|| {
                    ApiV2::append_ts_on_encoded_bytes(&mut bytes, ts.into());
                });
                assert!(r.is_err(), "case {}: {:?}", idx, r);
            }
        }
    }

    #[test]
    fn test_keyspace_id_to_string() {
        let keyspace_id_pd_alloc = 1_u32;
        let keyspace_id_pd_alloc_str = keyspace_id_pd_alloc.to_string();
        let keyspace_id_bytes = keyspace_id_pd_alloc.to_be_bytes();
        let user_key_prefix = &[
            TXN_KEY_PREFIX,
            keyspace_id_bytes[1],
            keyspace_id_bytes[2],
            keyspace_id_bytes[3],
        ];

        let keyspace_id_str = ApiV2::get_keyspace_id_str(user_key_prefix);
        assert_eq!(keyspace_id_pd_alloc_str, keyspace_id_str);
    }

    #[test]
    fn test_get_u32_keyspace_id() {
        let cases = vec![
            (vec![], None),
            (vec![b'x', 0], None),
            (vec![b'x', 0, 0, 1], Some(1)),
            (vec![b'x', 1, 2, 3, 4, 5], Some(0x10203)),
        ];

        for (key, expected) in cases.into_iter() {
            assert_eq!(ApiV2::get_u32_keyspace_id_by_key(&key), expected);
        }
    }

    #[test]
    fn test_get_txn_keyspace_range() {
        let test_cases = vec![
            (10u32, vec![b'x', 0, 0, 0x0A], vec![b'x', 0, 0, 0x0B]),
            (100, vec![b'x', 0, 0, 0x64], vec![b'x', 0, 0, 0x65]),
            (1000, vec![b'x', 0, 0x03, 0xE8], vec![b'x', 0, 0x03, 0xE9]),
            (
                100000,
                vec![b'x', 0x01, 0x86, 0xA0],
                vec![b'x', 0x01, 0x86, 0xA1],
            ),
        ];

        for (i, (id, start, end)) in test_cases.into_iter().enumerate() {
            assert_eq!(
                ApiV2::get_txn_keyspace_range(id),
                (start, end),
                "case {}",
                i
            );
        }
    }

    #[test]
    fn test_get_keyspace_prefix_by_id() {
        let cases: Vec<(u32, Vec<u8>, Vec<u8>)> = vec![
            (0, vec![], vec![b'x', 0, 0, 0]),
            (1, vec![b'x', 0, 0, 0x01], vec![b'x', 0, 0, 0x02]),
            (100, vec![b'x', 0, 0, 0x64], vec![b'x', 0, 0, 0x65]),
        ];
        for (keyspace_id, prefix, end) in cases {
            assert_eq!(&ApiV2::get_keyspace_prefix_by_id(keyspace_id), &prefix);
            assert_eq!(&ApiV2::get_keyspace_end_by_id(keyspace_id), &end);
            assert_eq!(ApiV2::get_keyspace_range_by_id(keyspace_id), (prefix, end),);
        }
    }

    #[test]
    fn test_is_whole_keyspace_range() {
        let test_cases = vec![
            (vec![b'x', 0, 0, 0x0A], vec![b'x', 0, 0, 0x0B], true),
            (vec![b'x', 0, 0, 0x0A], vec![b'x', 0, 0, 0x0C], false),
            (vec![b'x', 0, 0, 0x0A], vec![b'x', 0, 0, 0x0A], false),
            (vec![b'x', 0, 0, 0x0A], vec![b'x', 0, 0, 0x09], false),
            (vec![b'x', 0, 0, 0x0A, 0], vec![b'x', 0, 0, 0x0B], false),
            (vec![b'x', 0, 0, 0x0A], vec![b'x', 0, 0, 0x0B, 0], false),
            (vec![b'x', 0, 0, 0x0A, 0], vec![b'x', 0, 0, 0x0B, 0], false),
            (vec![b'r', 0, 0, 0x0A], vec![b'r', 0, 0, 0x0A], false),
        ];

        for (i, (start, end, is_whole)) in test_cases.into_iter().enumerate() {
            assert_eq!(
                is_whole_keyspace_range(&start, &end),
                is_whole,
                "case {}",
                i
            );
        }
    }

    #[test]
    fn test_is_one_or_multi_whole_keyspace_range() {
        let test_cases = vec![
            (vec![b'x', 0, 0, 0x0A], vec![b'x', 0, 0, 0x0B], true),
            (vec![b'x', 0, 0, 0x0A], vec![b'x', 0, 0, 0x0C], true),
            (vec![b'x', 0, 0, 0x0A], vec![b'x', 0, 0, 0x0A], false),
            (vec![b'x', 0, 0, 0x0A], vec![b'x', 0, 0, 0x09], false),
            (vec![b'x', 0, 0, 0x0A, 0], vec![b'x', 0, 0, 0x0B], false),
            (vec![b'x', 0, 0, 0x0A], vec![b'x', 0, 0, 0x0B, 0], false),
            (vec![b'x', 0, 0, 0x0A, 0], vec![b'x', 0, 0, 0x0B, 0], false),
            (vec![b'r', 0, 0, 0x0A], vec![b'r', 0, 0, 0x0A], false),
        ];

        for (i, (start, end, cover_whole)) in test_cases.into_iter().enumerate() {
            assert_eq!(
                is_one_or_multi_whole_keyspace_range(&start, &end),
                cover_whole,
                "case {}",
                i
            );
        }
    }

    #[test]
    fn test_is_belongs_to_same_keyspace() {
        let test_cases = vec![
            (b"x001111".to_vec(), b"x001112".to_vec(), true), // Txn
            (b"x001".to_vec(), b"x0011".to_vec(), true),
            (b"x0011".to_vec(), b"x002".to_vec(), false),
            (b"r0011".to_vec(), b"r0012".to_vec(), true), // Raw
            (b"r001".to_vec(), b"r0011".to_vec(), true),
            (b"r0011".to_vec(), b"r0021".to_vec(), true),
            (b"x0011".to_vec(), b"r0011".to_vec(), false), // Txn vs. Raw
            (b"x0011".to_vec(), b"00121".to_vec(), false), // Txn/Raw vs. Unknown/TiDB
            (b"r0011".to_vec(), b"00121".to_vec(), true),
            (b"01234".to_vec(), b"02345".to_vec(), true), // Unknown/TiDB
            (b"m1234".to_vec(), b"m2345".to_vec(), true),
            (b"m1234".to_vec(), b"t2345".to_vec(), true),
            (b"01234".to_vec(), b"t1234".to_vec(), true),
            (b"".to_vec(), b"t1234".to_vec(), true),
        ];

        for (i, (start, end, is_same)) in test_cases.into_iter().enumerate() {
            assert_eq!(
                ApiV2::is_belongs_to_same_keyspace(&start, &end),
                is_same,
                "case {}",
                i
            );
        }
    }

    #[test]
    fn test_get_keyspace_table_id() {
        let test_cases = vec![
            ("78000001748000000000000075", Some((1, 117))),
            (
                "780000017480000000000000755F720FFFFFFFFFFFFFFF",
                Some((1, 117)),
            ),
            ("78000001", None),
            ("748000000000000078", None),
            ("000001748000000000000079", None),
            (
                "7800000188800000FF00000000755F720FFFFFFFFFFFFFFFFF00FE",
                None,
            ),
        ];

        for (key, expected) in test_cases {
            let key_bytes = hex::decode(key).unwrap();
            assert_eq!(ApiV2::get_keyspace_table_id(&key_bytes), expected);
        }
    }
}
