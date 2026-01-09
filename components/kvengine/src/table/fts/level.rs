// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::{BTreeMap, HashMap, HashSet};

use super::{dedicated_file::EDedicatedFile, packed_file::PackedFile};
use crate::table::SnapVersion;

/// Management structure for FTS files across all levels
#[derive(Clone, Default)]
pub struct FtsLevels {
    tracked_indexes: HashMap<i64 /* table_id */, HashSet<i64 /* index_id */>>,

    /// Incremental processing boundary: all L0 data <= this version has been
    /// processed. This enables clean separation between historical and
    /// incremental processing. This is a watermark and never moves backwards.
    pub l0_snap_version: SnapVersion,
    /// Columnar L0 ids that still need incremental processing. This is mainly
    /// used after region merge when `l0_snap_version` has been advanced to the
    /// max of source/target, but some L0s are still unprocessed.
    pending_columnar_l0_ids: Vec<u64>,

    l0: Vec<PackedFile>,                        // Order by snap_version ascending
    l1: Vec<PackedFile>,                        // Order by smallest_lp_key ascending
    l2: BTreeMap<Vec<u8>, Vec<EDedicatedFile>>, // Grouped by LP key
}

impl std::fmt::Debug for FtsLevels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtsLevels")
            .field("tracked_indexes", &self.tracked_indexes)
            .field("l0_snap_version", &self.l0_snap_version)
            .field("pending_columnar_l0_ids", &self.pending_columnar_l0_ids)
            .field_with("l0", |f| {
                let mut list = f.debug_list();
                for file in &self.l0 {
                    list.entry_with(|f| {
                        write!(f, "{} @{}", file.id(), file.props().get_snap_version())
                    });
                }
                list.finish()
            })
            .field_with("l1", |f| {
                let mut list = f.debug_list();
                for file in &self.l1 {
                    list.entry_with(|f| {
                        write!(
                            f,
                            "{} [{}, {}]",
                            file.id(),
                            hexhex::hex(file.props().get_smallest_lp_key()),
                            hexhex::hex(file.props().get_largest_lp_key())
                        )
                    });
                }
                list.finish()
            })
            .field_with("l2", |f| {
                let mut map = f.debug_map();
                for (lp_key, files) in &self.l2 {
                    map.entry(
                        &hexhex::hex(lp_key),
                        &files.iter().map(|file| file.id()).collect::<Vec<u64>>(),
                    );
                }
                map.finish()
            })
            .finish()
    }
}

impl FtsLevels {
    /// Use this function to modify the L0 files.
    /// After modification, snap_version will be updated accordingly
    /// (advance only).
    pub fn mut_l0(&mut self, f: impl FnOnce(&mut Vec<PackedFile>)) {
        f(&mut self.l0);
        self.l0.sort_by(|a, b| {
            a.props()
                .get_snap_version()
                .cmp(&b.props().get_snap_version())
        });

        // We only advance l0_snap_version and never move it backwards, because
        // l0_snap_version is a watermark for incremental indexing - what has
        // already been indexed should not be indexed again.
        // When new L0 files are added, l0_snap_version will be advanced.
        // When L0 files are removed, l0_snap_version remains unchanged.
        if let Some(new_version) = self
            .l0
            .last()
            .map(|file| file.props().get_snap_version().into())
        {
            if new_version > self.l0_snap_version {
                self.l0_snap_version = new_version;
            }
        }
    }

    /// Use this function to set the pending L0 ids that need additional
    /// incremental processing. Normally these are L0 files with
    /// snap_version <= l0_snap_version.
    pub fn mut_pending_columnar_l0_ids(&mut self, f: impl FnOnce(&mut Vec<u64>)) {
        f(&mut self.pending_columnar_l0_ids);
        self.pending_columnar_l0_ids.sort_unstable();
        self.pending_columnar_l0_ids.dedup();
    }

    /// Get the pending L0 ids that need additional incremental processing.
    pub fn pending_columnar_l0_ids(&self) -> &[u64] {
        &self.pending_columnar_l0_ids
    }

    /// Use this function to modify the L1 files.
    pub fn mut_l1(&mut self, f: impl FnOnce(&mut Vec<PackedFile>)) {
        f(&mut self.l1);
        // Sort by smallest_lp_key first, then by smallest_key when lp_key is the same
        self.l1.sort_by(|a, b| {
            (
                a.props().get_smallest_lp_key(),
                a.props().get_smallest_key(),
            )
                .cmp(&(
                    b.props().get_smallest_lp_key(),
                    b.props().get_smallest_key(),
                ))
        });
        // Check that L1 files do not have invalid overlaps after modification.
        // LP key overlap is allowed temporarily, but key ranges must be ordered
        // properly. The l1 files with overlapping lp key will be compacted
        // later.
        for w in self.l1.windows(2) {
            let (prev, curr) = (&w[0], &w[1]);
            let (prev_props, curr_props) = (prev.props(), curr.props());
            let has_invalid_overlap = (
                prev_props.get_largest_lp_key(),
                prev_props.get_biggest_key(),
            ) > (
                curr_props.get_smallest_lp_key(),
                curr_props.get_smallest_key(),
            );
            debug_assert!(
                !has_invalid_overlap,
                "Overlapping L1 files after modification: id={} [lp: {}, {}] [key: {}, {}] \
                 overlap id={} [lp: {}, {}] [key: {}, {}]",
                prev.id(),
                hexhex::hex(prev_props.get_smallest_lp_key()),
                hexhex::hex(prev_props.get_largest_lp_key()),
                hexhex::hex(prev_props.get_smallest_key()),
                hexhex::hex(prev_props.get_biggest_key()),
                curr.id(),
                hexhex::hex(curr_props.get_smallest_lp_key()),
                hexhex::hex(curr_props.get_largest_lp_key()),
                hexhex::hex(curr_props.get_smallest_key()),
                hexhex::hex(curr_props.get_biggest_key()),
            );
        }
    }

    pub fn l0(&self) -> &[PackedFile] {
        &self.l0
    }

    pub fn l1(&self) -> &[PackedFile] {
        &self.l1
    }

    pub fn l2(&self) -> &BTreeMap<Vec<u8>, Vec<EDedicatedFile>> {
        &self.l2
    }

    /// Inserts multiple new L2 files into the levels structure.
    pub fn insert_l2_files(&mut self, files: impl IntoIterator<Item = EDedicatedFile>) {
        let mut touched_lps = HashSet::new();
        for file in files.into_iter() {
            let lp_key = file.props().get_lp_key();
            touched_lps.insert(lp_key.to_vec());
            let files_in_lp = self.l2.entry(lp_key.to_vec()).or_default();
            files_in_lp.push(file);
        }
        for lp_key in touched_lps {
            self.l2.get_mut(&lp_key).unwrap().sort_by_key(|f| f.id());
        }
    }

    /// Inserts a new L2 file into the levels structure.
    pub fn insert_l2_file(&mut self, file: EDedicatedFile) {
        self.insert_l2_files(std::iter::once(file));
    }

    /// Removes multiple L2 files from the levels structure.
    pub fn remove_l2_files(&mut self, file_ids: impl IntoIterator<Item = u64>) {
        let id_set = file_ids.into_iter().collect::<HashSet<u64>>();
        for files in self.l2.values_mut() {
            files.retain(|file| !id_set.contains(&file.id()));
        }
        self.l2.retain(|_, files| !files.is_empty());
    }

    /// Removes a specific L2 file from the levels structure.
    pub fn remove_l2_file(&mut self, file_id: u64) {
        self.remove_l2_files(std::iter::once(file_id));
    }

    /// Check if a specific index is tracked.
    pub fn has_tracked_index(&self, table_id: i64, index_id: i64) -> bool {
        self.tracked_indexes
            .get(&table_id)
            .map_or(false, |indexes| indexes.contains(&index_id))
    }

    /// Add a new FTS index for tracking.
    pub fn track_index(&mut self, table_id: i64, index_id: i64) {
        self.tracked_indexes
            .entry(table_id)
            .or_default()
            .insert(index_id);
    }

    /// Iterator over tracked indexes.
    pub fn iter_tracked_indexes(&self) -> impl Iterator<Item = (&i64, &HashSet<i64>)> {
        self.tracked_indexes.iter()
    }

    /// Reference to tracked indexes.
    pub fn tracked_indexes_ref(&self) -> &HashMap<i64, HashSet<i64>> {
        &self.tracked_indexes
    }

    /// Check if incremental indexing is needed.
    pub fn needs_delta_indexing(&self, max_columnar_l0_version: SnapVersion) -> bool {
        !self.tracked_indexes.is_empty()
            && (max_columnar_l0_version > self.l0_snap_version
                || !self.pending_columnar_l0_ids.is_empty())
    }

    /// Check if the FTS levels are empty.
    pub fn is_empty(&self) -> bool {
        self.tracked_indexes.is_empty()
    }

    /// Returns whether a columnar L0 file is already covered by the current FTS
    /// index.
    ///
    /// A columnar L0 is considered tracked when:
    /// - Its `snap_version` is **not newer** than the incremental watermark
    ///   `l0_snap_version`; and
    /// - Its `file_id` is **not** listed in `pending_l0_ids` (used after region
    ///   merge when the watermark may have advanced beyond some unprocessed
    ///   files).
    pub fn is_columnar_l0_tracked(&self, file_id: u64, snap_version: SnapVersion) -> bool {
        if snap_version > self.l0_snap_version {
            return false;
        }
        if self.pending_columnar_l0_ids.is_empty() {
            return true;
        }
        self.pending_columnar_l0_ids
            .binary_search(&file_id)
            .is_err()
    }

    /// Returns all tracked FTS file IDs grouped by level.
    /// The first entry contains L0 IDs, the second contains L1 IDs.
    pub fn get_all_file_ids(&self) -> [Vec<u64>; 3] {
        [
            self.l0.iter().map(|file| file.id()).collect(),
            self.l1.iter().map(|file| file.id()).collect(),
            self.l2
                .values()
                .flat_map(|files| files.iter())
                .map(|file| file.id())
                .collect(),
        ]
    }

    /// Retain only FTS indexes for tables that are still active.
    /// This cleans up index tracking for dropped tables and removes FTS files
    /// that no longer have active data.
    pub fn retain_for_tables(&mut self, active_table_ids: &[i64], columnar_l0_file_ids: &[u64]) {
        let active_set: HashSet<i64> = active_table_ids.iter().copied().collect();

        // Clean up tracked indexes
        self.tracked_indexes
            .retain(|table_id, _| active_set.contains(table_id));

        // Clean up L0/L1/L2 files that belong to dropped tables
        self.l0.retain(|l0_file| {
            active_table_ids
                .iter()
                .any(|&table_id| l0_file.has_table(table_id))
        });
        self.l1.retain(|file| {
            active_table_ids
                .iter()
                .any(|&table_id| file.has_table(table_id))
        });
        self.l2.retain(|_, files| {
            files.retain(|file| active_table_ids.contains(&file.props().get_table_id()));
            !files.is_empty()
        });

        // Cleanup pending columnar L0 IDs according to latest columnar L0 files
        if active_set.is_empty() || self.tracked_indexes.is_empty() {
            self.pending_columnar_l0_ids.clear();
        } else {
            let l0_id_set: HashSet<u64> = columnar_l0_file_ids.iter().copied().collect();
            self.mut_pending_columnar_l0_ids(|ids| ids.retain(|id| l0_id_set.contains(id)));
        }
    }

    /// Returns file IDs for L1/L2 files that are no longer covered by any
    /// tracked index.
    pub fn collect_cleanup_candidates(&self) -> (Vec<u64>, Vec<u64>) {
        let tracked: HashSet<(i64, i64)> = self
            .iter_tracked_indexes()
            .flat_map(|(table_id, indexes)| {
                indexes.iter().map(move |index_id| (*table_id, *index_id))
            })
            .collect();
        if tracked.is_empty() {
            return (Vec::new(), Vec::new());
        }

        let mut l1_remove = Vec::new();
        for file in &self.l1 {
            if should_remove_l1(file, &tracked) {
                l1_remove.push(file.id());
            }
        }

        let mut l2_remove = Vec::new();
        for files in self.l2.values() {
            for file in files {
                let props = file.props();
                let pair = (props.get_table_id(), props.get_index_id());
                if !tracked.contains(&pair) {
                    l2_remove.push(file.id());
                }
            }
        }

        (l1_remove, l2_remove)
    }
}

fn should_remove_l1(file: &PackedFile, tracked: &HashSet<(i64, i64)>) -> bool {
    let props = file.props();
    debug_assert!(
        props.has_smallest_table_index() && props.has_largest_table_index(),
        "L1 file {} missing table/index bounds",
        file.id()
    );
    let smallest = props.get_smallest_table_index();
    let largest = props.get_largest_table_index();
    let min_pair = (smallest.get_table_id(), smallest.get_index_id());
    let max_pair = (largest.get_table_id(), largest.get_index_id());
    !tracked
        .iter()
        .any(|pair| *pair >= min_pair && *pair <= max_pair)
}
