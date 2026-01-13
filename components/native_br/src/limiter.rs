// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use kvengine::{STORAGE_CLASS_KEY, get_shard_property};
use kvenginepb as pb;
use kvproto::{metapb, metapb::PeerRole};
use pd_client::PdClient;
use schema::schema::StorageClassSpec;
use tikv_util::{
    box_try,
    config::{ReadableDuration, ReadableSize},
    info,
    time::Limiter,
};

use crate::{
    error::Result,
    tikv::{FileWithId, StoresFiles},
};

/// The typical table file of 16MB has average meta size about 360KB.
const AVG_TABLE_META_SIZE: u64 = 360 * 1024; // 360KB

/// Used to estimate table file size based on meta offset.
///
/// 16MB / (16MB - 360KB) = 1.022
const TABLE_SIZE_MULTIPLIER: f64 = 1.022;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct RateLimitConfig {
    pub enable: bool,
    pub max_throughput: ReadableSize,
    /// Calibrate estimated restore size by requesting existed files on TiKV
    /// stores when exceeds this threshold.
    ///
    /// Current defaults to MAX to disable calibration. Set to 10GiB after TiKV
    /// supports it.
    pub calibrate_restore_size_threshold: ReadableSize,
    pub store_req_timeout: ReadableDuration,
    pub store_cache_ttl: ReadableDuration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enable: false,
            max_throughput: ReadableSize::mb(500), // 500 MB/s
            calibrate_restore_size_threshold: ReadableSize(u64::MAX),
            store_req_timeout: ReadableDuration::secs(15),
            store_cache_ttl: ReadableDuration::minutes(5),
        }
    }
}

pub struct ThroughputLimiter {
    limiter: Limiter,
    calibrate_threshold: u64,
    stores_files: StoresFiles,
}

#[derive(Clone)]
pub struct SnapshotSize {
    /// Estimated size of the snapshot (SINGLE replica).
    pub estimated_size: u64,
    /// `files` is empty if not need to calibrate from TiKV.
    pub files: Vec<FileWithSize>,
}

impl ThroughputLimiter {
    pub fn new(
        config: &RateLimitConfig,
        pd_client: Arc<dyn PdClient>,
        runtime: tokio::runtime::Handle,
    ) -> Result<Self> {
        info!(
            "create throughput limiter: {} MiB/sec",
            config.max_throughput.as_mb_f64()
        );
        let limiter = <Limiter>::builder(config.max_throughput.0 as f64)
            .refill(Duration::from_millis(10))
            .build();
        let stores_files = box_try!(StoresFiles::new(
            config.store_cache_ttl.0,
            config.store_req_timeout.0,
            pd_client,
            runtime,
        ));
        Ok(Self {
            limiter,
            calibrate_threshold: config.calibrate_restore_size_threshold.0,
            stores_files,
        })
    }

    pub fn calibrate_threshold(&self) -> u64 {
        self.calibrate_threshold
    }

    /// The returned value is the estimated data size of all replicas.
    pub fn estimate_snapshot_size_locally(cs: &pb::ChangeSet) -> SnapshotSize {
        if !cs.has_restore_shard() {
            debug_assert!(false);
            return SnapshotSize {
                estimated_size: 0,
                files: vec![],
            };
        }

        let snap = cs.get_restore_shard();

        let sc_spec = StorageClassSpec::unmarshal(
            get_shard_property(STORAGE_CLASS_KEY, snap.get_properties()).as_deref(),
        );
        if sc_spec.must_be_ia() {
            let estimated_size = Self::estimate_snapshot_data_size_for_ia(snap);
            return SnapshotSize {
                estimated_size,
                files: vec![],
            };
        }

        let mut files: Vec<FileWithSize> = vec![];
        files.extend(
            snap.get_l0_creates()
                .iter()
                .map(|l0| FileWithSize::from(l0)),
        );
        files.extend(
            snap.get_table_creates()
                .iter()
                .map(|tb| FileWithSize::from(tb)),
        );
        files.extend(
            snap.get_blob_creates()
                .iter()
                .map(|blob| FileWithSize::from(blob)),
        );

        let estimated_size = files.iter().map(|f| f.size).sum::<u64>();
        SnapshotSize {
            estimated_size,
            files,
        }
    }

    pub async fn calibrate_restore_size_from_tikv(
        &self,
        snapshot_size: &SnapshotSize,
        region: &metapb::Region,
    ) -> Result<u64> {
        if snapshot_size.files.is_empty() {
            return Ok(snapshot_size.estimated_size);
        }

        let stores = region
            .get_peers()
            .iter()
            .filter_map(|p| (p.role == PeerRole::Voter).then_some(p.store_id))
            .collect::<Vec<u64>>();

        let nonexisted_files_on_stores = box_try!(
            self.stores_files
                .get_sst_files_nonexisted_on_stores(snapshot_size.files.clone(), stores)
                .await
        );
        let total_size = nonexisted_files_on_stores
            .into_iter()
            .flat_map(|(_, fs)| fs)
            .map(|f| f.size)
            .sum::<u64>();
        Ok(total_size)
    }

    fn estimate_snapshot_data_size_for_ia(snap: &pb::Snapshot) -> u64 {
        let mut estimated_size = 0;

        estimated_size += snap
            .get_l0_creates()
            .iter()
            .map(|l0| l0.size as u64)
            .sum::<u64>();
        // Calculate only meta size of IA tables.
        estimated_size += snap.get_table_creates().len() as u64 * AVG_TABLE_META_SIZE;
        estimated_size += snap
            .get_blob_creates()
            .iter()
            .map(|blob| blob.meta_offset as u64)
            .sum::<u64>();

        estimated_size
    }

    pub async fn consume_restore_size(&self, restore_size: u64) {
        self.limiter.consume(restore_size as usize).await;
    }

    pub fn unconsume(&self, restore_size: u64) {
        self.limiter.unconsume(restore_size as usize);
    }
}

#[derive(Clone, Copy, Debug)]
pub struct FileWithSize {
    id: u64,
    size: u64,
}

impl FileWithId for FileWithSize {
    fn file_id(&self) -> u64 {
        self.id
    }
}

impl From<&pb::L0Create> for FileWithSize {
    fn from(l0: &pb::L0Create) -> Self {
        Self {
            id: l0.id,
            size: l0.size as u64,
        }
    }
}

impl From<&pb::TableCreate> for FileWithSize {
    fn from(table: &pb::TableCreate) -> Self {
        Self {
            id: table.id,
            size: (table.meta_offset as f64 * TABLE_SIZE_MULTIPLIER) as u64,
        }
    }
}

// TODO: estimate blob file size more accurately if needed.
impl From<&pb::BlobCreate> for FileWithSize {
    fn from(blob: &pb::BlobCreate) -> Self {
        Self {
            id: blob.id,
            size: blob.meta_offset as u64,
        }
    }
}
