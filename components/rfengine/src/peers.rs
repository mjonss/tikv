// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::RwLock};

use bytes::Bytes;
use kvproto::raft_serverpb;
use protobuf::Message;
use raft_proto::eraftpb::Entry;
use tikv_util::time::Instant;

use crate::{
    PeerStats, TRUNCATE_ALL_INDEX, WriteBatch,
    engine::PeerData,
    log_batch::RaftLogBlock,
    metrics::{ENGINE_APPLY_DURATION_HISTOGRAM, ENGINE_FETCH_ENTRIES_DURATION_HISTOGRAM},
    region_state_key,
};

#[derive(Default)]
pub struct RaftPeers {
    pub(crate) peers: papaya::HashMap<u64, RwLock<PeerData>>,
}

impl RaftPeers {
    /// Applies the write batch to memory without persisting it to WAL.
    pub fn apply(&self, wb: &WriteBatch) -> Vec<Vec<RaftLogBlock>> {
        let timer = Instant::now_coarse();
        let mut truncated_logs = vec![];
        for (&peer_id, batch_data) in &wb.peers {
            let region_id = batch_data.meta.region_id;
            let keyspace_id = batch_data.meta.keyspace_id;
            tikv_util::set_current_region_thread_local(region_id);
            let guard = self.peers.guard();
            let peer_data = self.get_or_init_peer_data(peer_id, region_id, keyspace_id, &guard);
            let mut peer_data = peer_data.write().unwrap();
            let truncated = peer_data.apply(batch_data);
            drop(peer_data);
            if !truncated.is_empty() {
                truncated_logs.push(truncated);
            }
        }
        ENGINE_APPLY_DURATION_HISTOGRAM.observe(timer.saturating_elapsed_secs());
        truncated_logs
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    pub fn get_term(&self, peer_id: u64, index: u64) -> Option<u64> {
        let peers = self.peers.pin();
        peers
            .get(&peer_id)
            .and_then(|data| data.read().unwrap().term(index))
    }

    pub fn get_truncated_index(&self, peer_id: u64) -> Option<u64> {
        let peers = self.peers.pin();
        let peer_data_ref = peers.get(&peer_id)?;
        let data = peer_data_ref.read().unwrap();
        Some(data.truncated_idx)
    }

    pub fn get_last_index(&self, peer_id: u64) -> Option<u64> {
        let peers = self.peers.pin();
        peers
            .get(&peer_id)
            .map(|data| data.read().unwrap().raft_logs.last_index())
            .and_then(|index| if index != 0 { Some(index) } else { None })
    }

    pub fn get_state(&self, peer_id: u64, key: &[u8]) -> Option<Bytes> {
        let peers = self.peers.pin();
        peers.get(&peer_id).and_then(|data| {
            data.read().unwrap().get_state(key).and_then(|val| {
                // TODO: seems it's impossible.
                if !val.is_empty() {
                    Some(val.clone())
                } else {
                    None
                }
            })
        })
    }

    /// Get the value of the last state key with the `prefix`. `prefix` must be
    /// non-empty.
    pub fn get_last_state_with_prefix(&self, peer_id: u64, prefix: &[u8]) -> Option<Bytes> {
        debug_assert!(!prefix.is_empty());
        let peers = self.peers.pin();
        let peer_data = peers.get(&peer_id)?;
        let peer_data = peer_data.read().unwrap();

        let mut end_prefix = prefix.to_vec();
        end_prefix[prefix.len() - 1] += 1;
        let range = Bytes::copy_from_slice(prefix)..Bytes::from(end_prefix);
        peer_data
            .meta
            .states
            .range(range)
            .next_back()
            .map(|(_, v)| v.clone())
    }

    /// Iterates states of the region in order or in desc order if `desc` is
    /// true until `f` returns error. The ietrator will stop if the function
    /// returns false.
    pub fn iterate_peer_states<F>(&self, peer_id: u64, desc: bool, mut f: F)
    where
        F: FnMut(&Bytes, &Bytes) -> bool,
    {
        let peers = self.peers.pin();
        let peer_data = peers.get(&peer_id);
        let peer_data = match &peer_data {
            Some(data) => data.read().unwrap(),
            None => return,
        };

        let states = &peer_data.meta.states;
        if desc {
            for (k, v) in states.iter().rev() {
                if !f(k, v) {
                    break;
                }
            }
        } else {
            for (k, v) in states.iter() {
                if !f(k, v) {
                    break;
                }
            }
        }
    }

    pub fn get_peer_all_states(&self, peer_id: u64, desc: bool) -> Vec<(Bytes, Bytes)> {
        let mut states = vec![];
        self.iterate_peer_states(peer_id, desc, |k, v| {
            states.push((k.clone(), v.clone()));
            true
        });
        states
    }

    /// Iterates stats of all regions in order or in desc order if `desc` is
    /// true and breaks one regions iteration if `f` returns false.
    pub fn iterate_all_states<F>(&self, desc: bool, mut f: F)
    where
        F: FnMut(u64, u64, &[u8], &[u8]) -> bool,
    {
        let peers = self.peers.pin();
        peers.iter().for_each(|(_, data)| {
            let data = data.read().unwrap();
            if data.truncated_idx == TRUNCATE_ALL_INDEX {
                return;
            }
            let peer_id = data.peer_id;
            let region_id = data.region_id;
            if desc {
                data.states
                    .iter()
                    .rev()
                    .take_while(|(k, v)| f(peer_id, region_id, k, v))
                    .count();
            } else {
                data.states
                    .iter()
                    .take_while(|(k, v)| f(peer_id, region_id, k, v))
                    .count();
            }
        });
    }

    pub(crate) fn get_or_init_peer_data<'a>(
        &self,
        peer_id: u64,
        region_id: u64,
        keyspace_id: u32,
        guard: &'a papaya::LocalGuard<'a>,
    ) -> &'a RwLock<PeerData> {
        self.peers.get_or_insert_with(
            peer_id,
            || RwLock::new(PeerData::new(peer_id, region_id, keyspace_id)),
            guard,
        )
    }

    /// Dumps the state of the region.
    pub fn get_peer_stats(&self, peer_id: u64) -> PeerStats {
        let peers = self.peers.pin();
        peers
            .get(&peer_id)
            .map(|data| data.read().unwrap().get_stats())
            .unwrap_or_default()
    }

    /// Returns the index that truncating to the given index can limit the
    /// memory usage to size.
    pub fn index_to_truncate_to_size(&self, peer_id: u64, size: usize) -> u64 {
        let peers = self.peers.pin();
        peers
            .get(&peer_id)
            .map(|data| {
                data.read()
                    .unwrap()
                    .raft_logs
                    .index_to_truncate_to_size(size)
            })
            .unwrap_or_default()
    }

    pub fn get_region_peer_map(&self) -> HashMap<u64 /* region_id */, u64 /* peer_id */> {
        let mut region_to_peer = HashMap::with_capacity(self.peers.len());
        let mut id_pairs = Vec::with_capacity(self.peers.len());
        let peers = self.peers.pin();
        for (_, peer_ref) in peers.iter() {
            let peer_data = peer_ref.read().unwrap();
            let is_truncated = peer_data.truncated_idx == TRUNCATE_ALL_INDEX;
            id_pairs.push((peer_data.peer_id, peer_data.region_id, is_truncated));
        }
        // ensure the newer peer_id appear after the older peer_id, so it can replace
        // older.
        id_pairs.sort_by(|(peer_a, ..), (peer_b, ..)| peer_a.cmp(peer_b));
        for (peer_id, region_id, truncated) in id_pairs {
            if truncated {
                // The newer peer is already destroyed, the old peer is invalid too.
                region_to_peer.remove(&region_id);
            } else {
                // new peer_id replaces the older peer_id.
                region_to_peer.insert(region_id, peer_id);
            }
        }
        region_to_peer
    }

    pub fn get_raft_entry(&self, peer_id: u64, index: u64) -> Option<Entry> {
        let peers = self.peers.pin();
        peers
            .get(&peer_id)
            .and_then(|data| data.read().unwrap().get(index))
    }

    pub fn fetch_raft_entries_to(
        &self,
        peer_id: u64,
        low: u64,
        high: u64,
        max_size: Option<usize>, // size limit of fetched entries
        buf: &mut Vec<Entry>,
    ) -> engine_traits::Result<usize> /* entry count */ {
        if high <= low {
            return Ok(0);
        }
        let old_len = buf.len();
        let peers = self.peers.pin();
        let peer_data = peers
            .get(&peer_id)
            .ok_or(engine_traits::Error::EntriesCompacted)?;
        let peer_data = peer_data.read().unwrap();
        if low <= peer_data.meta.truncated_idx {
            return Err(engine_traits::Error::EntriesCompacted);
        }

        let timer = Instant::now_coarse();
        let mut total_size = 0;
        for i in low..high {
            let entry = peer_data
                .get(i)
                .ok_or(engine_traits::Error::EntriesUnavailable)?;
            total_size += entry.compute_size() as usize;
            buf.push(entry);
            if max_size.is_some_and(|s| total_size >= s) {
                // At least return one entry regardless of size limit.
                break;
            }
        }
        ENGINE_FETCH_ENTRIES_DURATION_HISTOGRAM.observe(timer.saturating_elapsed_secs());
        Ok(buf.len() - old_len)
    }

    pub fn load_region_state(
        &self,
        peer_id: u64,
        version: u64,
    ) -> Option<raft_serverpb::RegionLocalState> {
        let region_state_key = region_state_key(version);
        let region_state_val = self.get_state(peer_id, &region_state_key)?;
        let mut region_state = raft_serverpb::RegionLocalState::new();
        region_state.merge_from_bytes(&region_state_val).unwrap();
        Some(region_state)
    }

    pub fn clone_keyspace(&self, keyspace_id: u32) -> RaftPeers {
        let new_peers = RaftPeers::default();
        let new_peers_guard = new_peers.peers.pin();
        let peers = self.peers.pin();
        for (&peer_id, peer_data) in peers.iter() {
            let peer_data = peer_data.read().unwrap();
            if peer_data.keyspace_id != keyspace_id {
                continue;
            }
            new_peers_guard.insert(peer_id, RwLock::new(peer_data.clone()));
        }
        drop(new_peers_guard);
        new_peers
    }
}
