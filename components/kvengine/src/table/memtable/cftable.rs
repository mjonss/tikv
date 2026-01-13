// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    iter::Iterator as StdIterator,
    ops::Deref,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use super::{Arena, SkipList};
use crate::{
    EXTRA_CF, NUM_CFS, WRITE_CF,
    table::{AtomicSnapVersion, DataBound, SnapVersion, TxnFile, memtable::skl_ext::SkipListExt},
};

#[derive(Clone)]
pub struct CfTable {
    pub core: Arc<CfTableCore>,
}

impl Deref for CfTable {
    type Target = CfTableCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl Default for CfTable {
    fn default() -> Self {
        Self::new()
    }
}

impl CfTable {
    pub fn new() -> Self {
        Self {
            core: Arc::new(CfTableCore::new()),
        }
    }

    pub fn new_split(&self) -> Self {
        let tbls = self.core.tbls.clone();
        let arena = self.core.arena.clone();
        let snap_ver = AtomicSnapVersion::new(self.snap_ver.load());
        let force_switch = AtomicBool::new(self.force_switch.load(Ordering::Acquire));
        let props = Mutex::new(self.core.props.lock().unwrap().clone());
        let unpersisted_props_size = AtomicUsize::new(self.core.unpersisted_props_size());
        Self {
            core: Arc::new(CfTableCore {
                tbls,
                arena,
                snap_ver,
                force_switch,
                props,
                unpersisted_props_size,
            }),
        }
    }

    #[must_use]
    pub fn add_write_cf_txn_files(&self, txn_files: &[TxnFile]) -> Self {
        let mut tbls = self.core.tbls.clone();
        tbls[WRITE_CF] = tbls[WRITE_CF].add_txn_files(txn_files);
        let arena = self.core.arena.clone();
        let snap_ver = AtomicSnapVersion::new(self.snap_ver.load());
        let force_switch = AtomicBool::new(self.force_switch.load(Ordering::Acquire));
        let props = Mutex::new(self.core.props.lock().unwrap().clone());
        let unpersisted_props_size = AtomicUsize::new(self.core.unpersisted_props_size());
        Self {
            core: Arc::new(CfTableCore {
                tbls,
                arena,
                snap_ver,
                force_switch,
                props,
                unpersisted_props_size,
            }),
        }
    }
}

pub struct CfTableCore {
    tbls: [SkipListExt; NUM_CFS],
    arena: Arc<Arena>,
    snap_ver: AtomicSnapVersion,
    force_switch: AtomicBool,
    props: Mutex<Option<kvenginepb::Properties>>,
    unpersisted_props_size: AtomicUsize,
}

impl Default for CfTableCore {
    fn default() -> Self {
        Self::new()
    }
}

impl CfTableCore {
    pub fn new() -> Self {
        let arena = Arc::new(Arena::new());
        Self {
            tbls: [
                SkipListExt::new(SkipList::new(Some(arena.clone()))),
                SkipListExt::new(SkipList::new(Some(arena.clone()))),
                SkipListExt::new(SkipList::new(Some(arena.clone()))),
            ],
            arena,
            snap_ver: AtomicSnapVersion::new(SnapVersion::zero()),
            force_switch: AtomicBool::new(false),
            props: Mutex::new(None),
            unpersisted_props_size: AtomicUsize::default(),
        }
    }

    pub fn get_cf(&self, cf: usize) -> &SkipListExt {
        &self.tbls[cf]
    }

    pub fn is_empty(&self) -> bool {
        for tbl in &self.tbls {
            if !tbl.is_empty() {
                return false;
            }
        }
        true
    }

    pub fn size(&self) -> u64 {
        self.tbls.iter().map(|t| t.size() as u64).sum()
    }

    pub fn skip_list_size(&self) -> usize {
        self.tbls.iter().map(|t| t.skl_size()).sum()
    }

    pub fn set_snap_version(&self, snap_ver: SnapVersion) {
        self.snap_ver.store(snap_ver)
    }

    pub fn set_properties(&self, props: kvenginepb::Properties) {
        self.props.lock().unwrap().replace(props);
    }

    pub fn get_properties(&self) -> Option<kvenginepb::Properties> {
        self.props.lock().unwrap().clone()
    }

    pub fn add_unpersisted_props_size(&self, size: usize) {
        self.unpersisted_props_size
            .fetch_add(size, Ordering::Release);
    }

    pub fn unpersisted_props_size(&self) -> usize {
        self.unpersisted_props_size.load(Ordering::Acquire)
    }

    pub fn get_snap_version(&self) -> SnapVersion {
        self.snap_ver.load()
    }

    pub fn set_force_switch(&self) {
        self.force_switch.store(true, Ordering::Release);
    }

    pub fn is_force_switch(&self) -> bool {
        self.force_switch.load(Ordering::Acquire)
    }

    pub fn has_data_in_bound(&self, bound: DataBound<'_>) -> bool {
        if self.is_empty() {
            return false;
        }
        for cf in 0..NUM_CFS {
            let tbl = &self.tbls[cf];
            let mut iter = tbl.new_iterator(false);
            iter.seek(bound.lower_bound);
            if iter.valid() && !bound.less_than_key(iter.key()) {
                return true;
            }
        }
        false
    }

    pub(crate) fn data_max_ts(&self) -> u64 {
        // Ignore LOCK_CF, as `ts` in LOCK_CF is not a TSO.
        // TODO: in async_commit, LOCK_CF may contains data, need handle it later.
        cmp::max(
            self.tbls[WRITE_CF].data_max_ts(),
            self.tbls[EXTRA_CF].data_max_ts(),
        )
    }
}

#[cfg(feature = "debug-trace-mem-table")]
impl CfTableCore {
    pub fn skl_and_txn_files_size(&self) -> (u64 /* skl_size */, u64 /* txn_files_size */) {
        self.tbls
            .iter()
            .fold((0, 0), |(skl_size, txn_files_size), t| {
                (
                    skl_size + t.skl_size() as u64,
                    txn_files_size + t.txn_files_size() as u64,
                )
            })
    }
}
