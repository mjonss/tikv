// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use rfengine::TRUNCATE_ALL_INDEX;
use rfstore::store::{AffectMemtable, RAFT_INIT_LOG_INDEX};

#[derive(Clone, Copy, Debug)]
pub(crate) struct RegionPersistProgress {
    /// Some(log_idx): There is no memtable writes since `log_idx`.
    none_memtable_since_idx: Option<u64>,

    /// The persisted log index.
    ///
    /// u64::MAX (TRUNCATE_ALL_INDEX) means all logs are truncated.
    persisted_idx: u64,
}

impl Default for RegionPersistProgress {
    fn default() -> Self {
        Self {
            none_memtable_since_idx: None,
            persisted_idx: RAFT_INIT_LOG_INDEX,
        }
    }
}

impl RegionPersistProgress {
    pub(crate) fn reset(&mut self, persisted_idx: u64) {
        self.none_memtable_since_idx = None;
        self.persisted_idx = persisted_idx;
    }

    // Note: `log_idx` may not be continuous, but the skipped indexes (e.g. conf
    // change) must have no memtable writes.
    pub(crate) fn update(&mut self, log_idx: u64, affect_memtable: AffectMemtable) {
        if self.persisted_idx == TRUNCATE_ALL_INDEX {
            // Once truncated-all, no further progress is meaningful.
            return;
        }
        match affect_memtable {
            AffectMemtable::None => {
                let since_idx = *self.none_memtable_since_idx.get_or_insert(log_idx);
                // Check if we can advance to log_idx
                if since_idx <= self.persisted_idx + 1 {
                    self.persisted_idx = log_idx;
                }
            }
            AffectMemtable::Write => {
                self.none_memtable_since_idx = None;
                // persisted_idx stays unchanged
            }
            AffectMemtable::Persist { data_seq } => {
                let since_idx = *self.none_memtable_since_idx.get_or_insert(log_idx);
                self.persisted_idx = self.persisted_idx.max(data_seq);
                // Check if we can advance to log_idx
                if since_idx <= self.persisted_idx + 1 {
                    self.persisted_idx = log_idx;
                }
            }
        }
    }

    /// Return TRUNCATE_ALL_INDEX if all logs are truncated.
    pub(crate) fn persisted_idx(&self) -> u64 {
        self.persisted_idx
    }

    pub(crate) fn truncate_all(&mut self) {
        self.persisted_idx = TRUNCATE_ALL_INDEX;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_persist_progress() {
        let mut progress = RegionPersistProgress::default();
        let cases = vec![
            (10, AffectMemtable::None, RAFT_INIT_LOG_INDEX),
            (11, AffectMemtable::None, RAFT_INIT_LOG_INDEX),
            (12, AffectMemtable::Write, RAFT_INIT_LOG_INDEX),
            (13, AffectMemtable::Persist { data_seq: 12 }, 13),
            (14, AffectMemtable::None, 14),
            (15, AffectMemtable::None, 15),
            (20, AffectMemtable::Write, 15),
            (21, AffectMemtable::None, 15),
            (22, AffectMemtable::Write, 15),
            (23, AffectMemtable::Write, 15),
            (24, AffectMemtable::None, 15),
            (25, AffectMemtable::Persist { data_seq: 20 }, 20),
            (26, AffectMemtable::Persist { data_seq: 23 }, 26),
            (30, AffectMemtable::None, 30),
        ];

        for (log_idx, affect, expected) in cases {
            progress.update(log_idx, affect);
            assert_eq!(
                progress.persisted_idx(),
                expected,
                "log_idx: {}, progress: {:?}",
                log_idx,
                progress
            );
        }

        progress.truncate_all();
        progress.update(100, AffectMemtable::None); // Should have no effect.
        assert_eq!(progress.persisted_idx(), TRUNCATE_ALL_INDEX,);
    }
}
