// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Arc};

use kvproto::{
    kvrpcpb::KeyRange,
    metapb::{self, Peer},
    pdpb::QueryKind,
};
use pd_client::{BucketMeta, BucketStat, merge_bucket_stats, new_bucket_stats};
use rand::Rng;
use tikv_util::store::{QueryStats, is_read_query};

use crate::store::worker::{FlowStatistics, split_config::get_sample_num};

// RegionInfo will maintain key_ranges with sample_num length by reservoir
// sampling. And it will save qps num and peer.
#[derive(Debug, Clone)]
pub struct RegionInfo {
    pub sample_num: usize,
    pub query_stats: QueryStats,
    pub peer: Peer,
    pub key_ranges: Vec<KeyRange>,
    pub flow: FlowStatistics,
}

impl RegionInfo {
    fn new(sample_num: usize) -> RegionInfo {
        RegionInfo {
            sample_num,
            query_stats: QueryStats::default(),
            key_ranges: Vec::with_capacity(sample_num),
            peer: Peer::default(),
            flow: FlowStatistics::default(),
        }
    }

    fn get_read_qps(&self) -> usize {
        self.query_stats.get_read_query_num() as usize
    }

    fn add_key_ranges(&mut self, key_ranges: Vec<KeyRange>) {
        for (i, key_range) in key_ranges.into_iter().enumerate() {
            let n = self.get_read_qps() + i;
            if n == 0 || self.key_ranges.len() < self.sample_num {
                self.key_ranges.push(key_range);
            } else {
                let j = rand::thread_rng().gen_range(0..n);
                if j < self.sample_num {
                    self.key_ranges[j] = key_range;
                }
            }
        }
    }

    fn add_query_num(&mut self, kind: QueryKind, query_num: u64) {
        self.query_stats.add_query_num(kind, query_num);
    }

    fn update_peer(&mut self, peer: &Peer) {
        if self.peer != *peer {
            self.peer = peer.clone();
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReadStats {
    // RegionID -> RegionInfo
    // There're three methods could insert a `RegionInfo` into the map:
    //   1. add_query_num
    //   2. add_query_num_batch
    //   3. add_flow
    // Among these three methods, `add_flow` will not update `key_ranges` of `RegionInfo`,
    // and due to this, an `RegionInfo` without `key_ranges` may occur. The caller should be aware
    // of this.
    pub region_infos: HashMap<u64, RegionInfo>,
    pub sample_num: usize,
    pub region_buckets: HashMap<u64, BucketStat>,
}

impl ReadStats {
    pub fn with_sample_num(sample_num: usize) -> Self {
        ReadStats {
            region_infos: HashMap::default(),
            region_buckets: HashMap::default(),
            sample_num,
        }
    }

    pub fn add_query_num(
        &mut self,
        region_id: u64,
        peer: &Peer,
        key_range: KeyRange,
        kind: QueryKind,
    ) {
        self.add_query_num_batch(region_id, peer, vec![key_range], kind);
    }

    pub fn add_query_num_batch(
        &mut self,
        region_id: u64,
        peer: &Peer,
        key_ranges: Vec<KeyRange>,
        kind: QueryKind,
    ) {
        let sample_num = self.sample_num;
        let query_num = key_ranges.len() as u64;
        let region_info = self
            .region_infos
            .entry(region_id)
            .or_insert_with(|| RegionInfo::new(sample_num));
        region_info.update_peer(peer);
        if is_read_query(kind) {
            region_info.add_key_ranges(key_ranges);
        }
        region_info.add_query_num(kind, query_num);
    }

    pub fn add_flow(
        &mut self,
        region_id: u64,
        buckets: Option<&Arc<BucketMeta>>,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        write: &FlowStatistics,
        data: &FlowStatistics,
    ) {
        let num = self.sample_num;
        let region_info = self
            .region_infos
            .entry(region_id)
            .or_insert_with(|| RegionInfo::new(num));
        region_info.flow.add(write);
        region_info.flow.add(data);
        if let Some(buckets) = buckets {
            let bucket_stat = self.region_buckets.entry(region_id).or_insert_with(|| {
                let stats = new_bucket_stats(buckets);
                BucketStat::new(buckets.clone(), stats)
            });
            if bucket_stat.meta < *buckets {
                let stats = new_bucket_stats(buckets);
                let mut new = BucketStat::new(buckets.clone(), stats);
                merge_bucket_stats(
                    &new.meta.keys,
                    &mut new.stats,
                    &bucket_stat.meta.keys,
                    &bucket_stat.stats,
                );
                *bucket_stat = new;
            }
            let mut delta = metapb::BucketStats::default();
            delta.set_read_bytes(vec![(write.read_bytes + data.read_bytes) as u64]);
            delta.set_read_keys(vec![(write.read_keys + data.read_keys) as u64]);
            let start = start.unwrap_or_default();
            let end = end.unwrap_or_default();
            merge_bucket_stats(
                &bucket_stat.meta.keys,
                &mut bucket_stat.stats,
                &[start, end],
                &delta,
            );
        }
    }

    pub fn is_empty(&self) -> bool {
        self.region_infos.is_empty()
    }
}

impl Default for ReadStats {
    fn default() -> ReadStats {
        ReadStats {
            sample_num: get_sample_num(),
            region_infos: HashMap::default(),
            region_buckets: HashMap::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct WriteStats {
    pub region_infos: HashMap<u64, QueryStats>,
}

impl WriteStats {
    pub fn add_query_num(&mut self, region_id: u64, kind: QueryKind) {
        let query_stats = self.region_infos.entry(region_id).or_default();
        query_stats.add_query_num(kind, 1);
    }

    pub fn is_empty(&self) -> bool {
        self.region_infos.is_empty()
    }
}

#[derive(PartialEq, Debug)]
pub enum SplitConfigChange {
    _Noop,
    _UpdateRegionCpuCollector(bool),
}
