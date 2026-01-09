// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

mod inplace;
mod l0;
mod l0_merge;
mod l2;
mod merge_utils;

pub use inplace::*;
pub use l0::*;
pub use l0_merge::*;
pub use l2::*;

#[cfg(test)]
mod inplace_test;
#[cfg(test)]
mod l0_merge_test;
#[cfg(test)]
mod l0_test;
#[cfg(test)]
mod l2_test;

/// Generate a logical partition key for FTS indexes
///
/// Format:
///  (table_id, index_id)           - for simple indexes
///  (table_id, index_id, user_key) - for partitioned indexes
///
/// Currently there is no partitioned indexes yet. It is reserved for future
/// impl.
pub fn lp_key(table_id: i64, index_id: i64) -> [u8; 16] {
    use codec::number::NumberCodec;
    let mut key = [0u8; 16];
    // Let's always encode in memory comparable format,
    // so that it will be easier when building ordered logical partitions.
    NumberCodec::encode_i64(&mut key[..8], table_id);
    NumberCodec::encode_i64(&mut key[8..], index_id);
    key
}
