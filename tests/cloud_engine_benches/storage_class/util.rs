// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use kvengine::SnapAccess;
use rand::{RngCore as _, thread_rng};
use tikv::storage::txn::CloudStoreScanner;
use txn_types::{Key, TsSet};

pub(crate) const KEY_PREFIX: &str = "x123";

pub(crate) fn i_to_key(i: usize) -> Vec<u8> {
    format!("{KEY_PREFIX}key_{:08}", i).into_bytes()
}

#[allow(dead_code)]
pub(crate) fn i_to_val(i: usize) -> Vec<u8> {
    format!("val_{:05}", i).into_bytes().repeat(100)
}

/// `value_size` should be factor of 8.
pub(crate) fn random_value(value_size: usize) -> Vec<u8> {
    let mut value = vec![0u8; value_size];
    thread_rng().fill_bytes(value.as_mut_slice());
    value
}

pub(crate) fn new_scanner(
    snap: &SnapAccess,
    desc: bool,
    read_ts: u64,
    lower_bound: Option<Key>,
    upper_bound: Option<Key>,
) -> CloudStoreScanner {
    CloudStoreScanner::new(
        snap.clone(),
        desc,
        true,
        TsSet::Empty,
        read_ts,
        lower_bound,
        upper_bound,
        false,
    )
    .unwrap()
}
