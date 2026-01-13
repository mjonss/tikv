// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(elided_lifetimes_in_paths)]
#![allow(renamed_and_removed_lints)]
#![allow(static_mut_refs)]

pub mod changeset;
pub use changeset::*;

pub mod fts;

pub fn get_any_snap_from_changeset(cs: &ChangeSet) -> Option<&Snapshot> {
    if cs.has_snapshot() {
        Some(cs.get_snapshot())
    } else if cs.has_initial_flush() {
        Some(cs.get_initial_flush())
    } else if cs.has_restore_shard() {
        Some(cs.get_restore_shard())
    } else {
        None
    }
}
