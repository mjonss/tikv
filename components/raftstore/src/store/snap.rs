// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    fmt::{self, Display, Formatter},
    io::{self, ErrorKind},
};

use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE, CfName};
use kvproto::raft_serverpb::RaftSnapshotData;
use protobuf::Message;
use raft::eraftpb::Snapshot as RaftSnapshot;

use crate::store::metrics::*;

// Data in CF_RAFT should be excluded for a snapshot.
pub const SNAPSHOT_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];
pub const SNAPSHOT_CFS_ENUM_PAIR: &[(CfNames, CfName)] = &[
    (CfNames::default, CF_DEFAULT),
    (CfNames::lock, CF_LOCK),
    (CfNames::write, CF_WRITE),
];
pub const SNAPSHOT_VERSION: u64 = 2;
pub const TABLET_SNAPSHOT_VERSION: u64 = 3;
pub const IO_LIMITER_CHUNK_SIZE: usize = 4 * 1024;

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct SnapKey {
    pub region_id: u64,
    pub term: u64,
    pub idx: u64,
}

impl SnapKey {
    #[inline]
    pub fn new(region_id: u64, term: u64, idx: u64) -> SnapKey {
        SnapKey {
            region_id,
            term,
            idx,
        }
    }

    pub fn from_region_snap(region_id: u64, snap: &RaftSnapshot) -> SnapKey {
        let index = snap.get_metadata().get_index();
        let term = snap.get_metadata().get_term();
        SnapKey::new(region_id, term, index)
    }

    pub fn from_snap(snap: &RaftSnapshot) -> io::Result<SnapKey> {
        let mut snap_data = RaftSnapshotData::default();
        if let Err(e) = snap_data.merge_from_bytes(snap.get_data()) {
            return Err(io::Error::new(ErrorKind::Other, e));
        }

        Ok(SnapKey::from_region_snap(
            snap_data.get_region().get_id(),
            snap,
        ))
    }
}

impl Display for SnapKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}_{}", self.region_id, self.term, self.idx)
    }
}
pub struct Snapshot {
    _key: SnapKey,
}
