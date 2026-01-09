// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use serde::{Deserialize, Serialize};
use tikv_util::config::ReadableSize;

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct FtsBuildOptions {
    /// Max number of FTS L0 files kept before triggering L0→L1/L2 compaction.
    pub max_l0_files: usize,
    /// Max total size of FTS L0 files before triggering compaction.
    pub max_l0_sizes: ReadableSize,
    /// Hard cap for a single generated FTS L0 file. When exceeded, a new L0
    /// file will be created.
    pub l0_file_max_size: ReadableSize,
    /// Hard cap for a single generated FTS L1 packed file. When exceeded, a new
    /// L1 file will be created.
    pub l1_file_max_size: ReadableSize,
    /// Threshold for promoting an LP from L1 to L2.
    pub min_l2_lp_size: ReadableSize,
    /// Maximum number of dedicated files allowed per logical partition before
    /// we trigger intra-L2 compaction.
    pub l2_max_files_per_lp: usize,
    /// Minimum number of files to merge in a single compaction.
    pub l2_min_merge_files: usize,
    /// Upper bound for a merged L2 file.
    pub l2_max_output_file_size: ReadableSize,
    /// Minimum logical size (docs) for tiering; segments smaller than this are
    /// treated as this size when building merge levels.
    pub l2_min_layer_docs: u64,
    /// Log2 gap between successive tiers for intra-L2 merge grouping.
    pub l2_level_log_size: f64,
    /// Skip intra-L2 merge consideration for segments larger than this many
    /// docs.
    pub l2_max_docs_before_merge: u64,
}

impl Default for FtsBuildOptions {
    fn default() -> Self {
        Self {
            max_l0_files: 4,
            max_l0_sizes: ReadableSize::mb(192),
            l0_file_max_size: ReadableSize::mb(96),
            l1_file_max_size: ReadableSize::mb(96),
            min_l2_lp_size: ReadableSize::mb(64),
            l2_max_files_per_lp: 6,
            l2_min_merge_files: 3,
            l2_max_output_file_size: ReadableSize::gb(2),
            l2_min_layer_docs: 10_000,
            l2_level_log_size: 0.75,
            l2_max_docs_before_merge: 10_000_000,
        }
    }
}
