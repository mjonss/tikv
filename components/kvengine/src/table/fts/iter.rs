// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::{Result, bail};
use bytes::Bytes;

/// A document id within a single FTS index segment/file.
///
/// This is the 0-based ordinal of a `(pk, version)` entry in the on-disk
/// ordering (PK ascending, version descending for the same PK). It matches
/// Tantivy's `DocId` used by the corresponding index segment and is only
/// meaningful within that segment/file.
pub type DocId = u32;

/// We don't allow PkType to be implemented outside this module, so here we use
/// the sealed trait pattern.
mod private {
    pub trait Sealed {}
}

impl private::Sealed for IntPk {}
impl private::Sealed for CommonPk {}

pub trait PkType: private::Sealed + Send + Sync + 'static {
    const IS_INT: bool;
    type T<'a>: Copy + Clone;

    /// Decodes the pk from memory comparable form into its original data.
    fn decode(encoded: &[u8]) -> Result<Self::T<'_>>;
}

pub struct IntPk;

impl IntPk {
    /// Returns the pk in memory comparable form.
    pub fn encode(pk: i64) -> [u8; 8] {
        let u = codec::convert::encode_i64_to_comparable_u64(pk);
        u.to_be_bytes()
    }
}

pub struct CommonPk;

impl PkType for IntPk {
    const IS_INT: bool = true;
    type T<'a> = i64;

    fn decode(encoded: &[u8]) -> Result<Self::T<'_>> {
        if encoded.len() != 8 {
            bail!(
                "Invalid IntPk encoded length: expected 8, got {}",
                encoded.len()
            );
        }
        Ok(codec::number::NumberCodec::decode_i64(encoded))
    }
}

impl PkType for CommonPk {
    const IS_INT: bool = false;
    type T<'a> = &'a [u8];

    fn decode(encoded: &[u8]) -> Result<Self::T<'_>> {
        Ok(encoded)
    }
}

#[allow(async_fn_in_trait)]
pub trait PkReader<Pk: PkType> {
    type Iterator: OrderedPkIterator;

    async fn has_newer_version(
        &self,
        pk_encoded: &[u8],
        version: u64,
        max_version: u64,
    ) -> Result<bool>;

    fn pk_iter(&self) -> Result<Self::Iterator>;

    /// This function also performs a version check: whether there are higher
    /// version that is <= read_ts for the same PK as the doc at doc_id. If
    /// found, None will be returned, indicating this doc id is shadowed by a
    /// newer version.
    ///
    /// The `doc_id` uses the same numbering as `OrderedPkIterator` (0-based
    /// within the file/segment).
    async fn async_at(&self, doc_id: usize, read_ts: u64) -> Result<Option<(Bytes, u64, u8)>>;
}

/// Iterator trait for iterating over sorted primary keys for **FTS files**.
pub trait OrderedPkIterator {
    /// Returns the next (doc_id, encoded_pk, version, is_deleted) tuple.
    ///
    /// Implementations must yield entries in strictly increasing `doc_id`
    /// order, starting from 0, and cover all documents in the file/segment.
    /// A note about encoded_pk:
    /// - For int PKs, i64 pk **in memory comparable form** is returned.
    /// - For common PKs, pk in bytes is returned. None is returned when the
    ///   iteration is finished.
    #[allow(async_fn_in_trait)]
    async fn next(&mut self) -> Result<Option<(DocId, Bytes, u64, u8)>>;
}

/// Find whether there is a newer version of a specified PK.
///
/// We suppose PKs and versions are ordered in the following way and accessed
/// through a xxx_at() fn:
///
/// N:       4
/// PK:      [1, 1, 3,   7] (ascending order)
/// Version: [3, 1, 100, 2] (newer version comes first for the same PK)
///
/// Internally, we treat it as a globally sorted sequence of (PK, !version)
/// tuples: (1, !3), (1, !1), (3, !100), (7, !2)
/// and then we perform a binary search to find the first position
/// where (PK, !version) >= (target_pk, !target_max_version).
///
/// Returns true, if the `target_pk` is found and it has a version that is newer
/// than `target_version` and not newer than `target_max_version`.
pub fn has_newer_version(
    target_pk_encoded: &[u8],
    target_version: u64,
    target_max_version: u64,

    n: usize,
    version_at: impl Fn(usize) -> Result<u64>,
    pk_encoded_at: impl Fn(usize) -> Result<Bytes>,
) -> Result<bool> {
    use std::cmp::Ordering;
    // Find the first position `pos` such that:
    // `pk_encoded_at(pos) >= target_pk_encoded` AND `version_at(pos) <=
    // target_max_version`.
    let pos = crate::table::try_search::<anyhow::Error>(n, |i| {
        match pk_encoded_at(i)?.as_ref().cmp(target_pk_encoded) {
            Ordering::Less => Ok(false),
            Ordering::Greater => Ok(true),
            Ordering::Equal => Ok(version_at(i)? <= target_max_version),
        }
    })?;
    if pos >= n {
        return Ok(false);
    }

    let pk_encoded = pk_encoded_at(pos)?;
    if pk_encoded.as_ref() == target_pk_encoded {
        let version = version_at(pos)?;
        // We already know version <= target_max_version from the search.
        // We just need to check if version > target_version.
        if version > target_version {
            return Ok(true);
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_has_newer_version() {
        // Test data: PK: [1, 1, 3, 7], Version: [3, 1, 100, 2]
        // This represents: (1,3), (1,1), (3,100), (7,2)
        let pks: &[&[u8]] = &[&[1], &[1], &[3], &[7]];
        let versions = [3u64, 1, 100, 2];

        let v_at = |i: usize| -> Result<u64> { Ok(versions[i]) };
        let pk_at = |i: usize| -> Result<Bytes> { Ok(Bytes::copy_from_slice(pks[i])) };

        // PK=1, version=0, max_version=5 -> should find (1,v3) and (1,v1)
        assert!(has_newer_version(&[1], 0, 5, pks.len(), v_at, pk_at).unwrap());

        // PK=1, version=1, max_version=5 -> should find (1,v3)
        assert!(has_newer_version(&[1], 1, 5, pks.len(), v_at, pk_at).unwrap());

        // PK=1, version=2, max_version=5 -> should find (1,v3)
        assert!(has_newer_version(&[1], 2, 5, pks.len(), v_at, pk_at).unwrap());

        // PK=1, version=3, max_version=5 -> no version > 3
        assert!(!has_newer_version(&[1], 3, 5, pks.len(), v_at, pk_at).unwrap());

        // PK=1, version=10, max_version=150 -> no version > 10
        assert!(!has_newer_version(&[1], 10, 150, pks.len(), v_at, pk_at).unwrap());

        // PK=1, version=0, max_version=2 -> should find (1,v3)
        assert!(has_newer_version(&[1], 0, 2, pks.len(), v_at, pk_at).unwrap());

        // PK=2, not exists
        assert!(!has_newer_version(&[2], 0, 1000, pks.len(), v_at, pk_at).unwrap());

        // PK=3, version=50, max_version=150 -> should find (3,100)
        assert!(has_newer_version(&[3], 50, 150, pks.len(), v_at, pk_at).unwrap());

        // PK=3, version=50, max_version=100 -> should find (3,100)
        assert!(has_newer_version(&[3], 50, 100, pks.len(), v_at, pk_at).unwrap());

        // PK=3, version=1, max_version=50 -> (3,100) is newer than
        // 1 but exceeds max_version
        assert!(!has_newer_version(&[3], 1, 50, pks.len(), v_at, pk_at).unwrap());

        // PK=7, version=1, max_version=3 -> should find (7,2)
        assert!(has_newer_version(&[7], 1, 3, pks.len(), v_at, pk_at).unwrap());

        // PK=99, not exists
        assert!(!has_newer_version(&[99], 0, 1000, pks.len(), v_at, pk_at).unwrap());

        // Edge case: empty data
        assert!(!has_newer_version(&[1], 0, 1000, 0, v_at, pk_at).unwrap());

        // Edge case: exact version match with max_version
        assert!(!has_newer_version(&[1], 3, 3, pks.len(), v_at, pk_at).unwrap());
        assert!(has_newer_version(&[1], 2, 3, pks.len(), v_at, pk_at).unwrap());
    }
}
