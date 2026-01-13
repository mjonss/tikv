// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    fmt,
    fmt::{Debug, Display, Formatter},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::{Buf, BufMut};
use cloud_encryption::EncryptionKey;
use futures::executor::block_on;
use futures_util::compat::Future01CompatExt;
use kvproto::{metapb, raft_cmdpb::RaftCmdRequest};
use protobuf::Message;
use raft_proto::eraftpb;
// Re-export for convenience.
pub use raftstore::store::util::is_region_initialized;
use slog::{Key, Record, Serializer};
use tikv_util::{
    box_err,
    codec::bytes::decode_bytes,
    debug, error,
    sys::thread::Pid,
    time::{Instant, InstantExt},
    timer::GLOBAL_TIMER_HANDLE,
};

use crate::{
    Error, RaftRouter, Result,
    store::{ProposalContext, SPLIT_FLAG_ENCRYPTION_KEYS, StoreMsg},
};

/// WARNING: `NORMAL_REQ_CHECK_VER` and `NORMAL_REQ_CHECK_CONF_VER` **MUST NOT**
/// be changed. The reason is the same as `admin_cmd_epoch_lookup`.
pub static NORMAL_REQ_CHECK_VER: bool = true;
pub static NORMAL_REQ_CHECK_CONF_VER: bool = false;

pub fn check_region_epoch(
    req: &RaftCmdRequest,
    region: &metapb::Region,
    include_region: bool,
) -> Result<()> {
    let (check_ver, check_conf_ver) = if !req.has_admin_request() {
        // for get/set/delete, we don't care conf_version.
        (NORMAL_REQ_CHECK_VER, NORMAL_REQ_CHECK_CONF_VER)
    } else {
        let epoch_state =
            raftstore::store::util::admin_cmd_epoch_lookup(req.get_admin_request().get_cmd_type());
        (epoch_state.check_ver, epoch_state.check_conf_ver)
    };

    if !check_ver && !check_conf_ver {
        return Ok(());
    }

    if !req.get_header().has_region_epoch() {
        return Err(box_err!("missing epoch!"));
    }

    let from_epoch = req.get_header().get_region_epoch();
    compare_region_epoch(
        from_epoch,
        region,
        check_conf_ver,
        check_ver,
        include_region,
    )
}

pub fn compare_region_epoch(
    from_epoch: &metapb::RegionEpoch,
    region: &metapb::Region,
    check_conf_ver: bool,
    check_ver: bool,
    include_region: bool,
) -> Result<()> {
    // We must check epochs strictly to avoid key not in region error.
    //
    // A 3 nodes TiKV cluster with merge enabled, after commit merge, TiKV A
    // tells TiDB with a epoch not match error contains the latest target Region
    // info, TiDB updates its region cache and sends requests to TiKV B,
    // and TiKV B has not applied commit merge yet, since the region epoch in
    // request is higher than TiKV B, the request must be denied due to epoch
    // not match, so it does not read on a stale snapshot, thus avoid the
    // KeyNotInRegion error.
    let current_epoch = region.get_region_epoch();
    if (check_conf_ver && from_epoch.get_conf_ver() != current_epoch.get_conf_ver())
        || (check_ver && from_epoch.get_version() != current_epoch.get_version())
    {
        debug!(
            "epoch not match";
            "region_id" => region.get_id(),
            "from_epoch" => ?from_epoch,
            "current_epoch" => ?current_epoch,
        );
        let regions = if include_region {
            vec![region.to_owned()]
        } else {
            vec![]
        };
        return Err(Error::EpochNotMatch(
            format!(
                "current epoch of region {} is {:?}, but you \
                 sent {:?}",
                region.get_id(),
                current_epoch,
                from_epoch
            ),
            regions,
        ));
    }

    Ok(())
}

#[inline]
pub fn check_store_id(req: &RaftCmdRequest, store_id: u64) -> Result<()> {
    let peer = req.get_header().get_peer();
    if peer.get_store_id() == store_id {
        Ok(())
    } else {
        Err(Error::StoreNotMatch {
            to_store_id: peer.get_store_id(),
            my_store_id: store_id,
        })
    }
}

#[inline]
pub fn check_term(req: &RaftCmdRequest, term: u64) -> Result<()> {
    let header = req.get_header();
    if header.get_term() == 0 || term <= header.get_term() + 1 {
        Ok(())
    } else {
        // If header's term is 2 verions behind current term,
        // leadership may have been changed away.
        Err(Error::StaleCommand)
    }
}

#[inline]
pub fn check_peer_id(req: &RaftCmdRequest, peer_id: u64) -> Result<()> {
    let header = req.get_header();
    if header.get_peer().get_id() == peer_id {
        Ok(())
    } else {
        Err(box_err!(
            "mismatch peer id {} != {}",
            header.get_peer().get_id(),
            peer_id
        ))
    }
}

/// Check if key in region range (`start_key`, `end_key`).
pub fn check_key_in_region_exclusive(key: &[u8], region: &metapb::Region) -> Result<()> {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    if start_key < key && (key < end_key || end_key.is_empty()) {
        Ok(())
    } else {
        Err(Error::KeyNotInRegion(key.to_vec(), region.clone()))
    }
}

/// Check if key in region range [`start_key`, `end_key`].
pub fn check_key_in_region_inclusive(key: &[u8], region: &metapb::Region) -> Result<()> {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    if key >= start_key && (end_key.is_empty() || key <= end_key) {
        Ok(())
    } else {
        Err(Error::KeyNotInRegion(key.to_vec(), region.clone()))
    }
}

/// Check if key in region range [`start_key`, `end_key`).
pub fn check_key_in_region(key: &[u8], region: &metapb::Region) -> Result<()> {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    if key >= start_key && (end_key.is_empty() || key < end_key) {
        Ok(())
    } else {
        Err(Error::KeyNotInRegion(key.to_vec(), region.clone()))
    }
}

pub fn cf_name_to_num(cf_name: &str) -> usize {
    match cf_name {
        "write" => 0,
        "lock" => 1,
        "extra" => 2,
        _ => 0,
    }
}

/// Parse data of entry `index`.
///
/// # Panics
///
/// If `data` is corrupted, this function will panic.
// TODO: make sure received entries are not corrupted
#[inline]
pub fn parse_data_at<T: Message + Default>(data: &[u8], index: u64, tag: PeerTag) -> T {
    let mut result = T::default();
    result.merge_from_bytes(data).unwrap_or_else(|e| {
        panic!("{} data is corrupted at {}: {:?}", tag, index, e);
    });
    result
}

pub fn parse_raft_cmd(
    tag: PeerTag,
    entry: &eraftpb::Entry,
    encryption_key: Option<&EncryptionKey>,
    decryption_buf: &mut Vec<u8>,
) -> RaftCmdRequest {
    let mut data = entry.get_data();
    let proposal_ctx = ProposalContext::from_bytes(entry.get_context());
    let index = entry.index;
    if proposal_ctx.contains(ProposalContext::ENCRYPTED) {
        let key_ver = data.get_u32();
        decryption_buf.truncate(0);
        encryption_key.unwrap().decrypt(
            data,
            tag.id_ver.id(),
            entry.index as u32,
            key_ver,
            decryption_buf,
        );
        parse_data_at(decryption_buf, index, tag)
    } else {
        parse_data_at(data, index, tag)
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub struct PeerTag {
    pub store_id: u64,
    pub id_ver: RegionIdVer,
}

impl PeerTag {
    pub fn new(store_id: u64, id_ver: RegionIdVer) -> Self {
        Self { store_id, id_ver }
    }
}

impl Display for PeerTag {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}:{}:{}]",
            self.store_id, self.id_ver.id, self.id_ver.ver
        )
    }
}

impl slog::Value for PeerTag {
    fn serialize(
        &self,
        _record: &Record<'_>,
        key: Key,
        serializer: &mut dyn Serializer,
    ) -> slog::Result {
        serializer.emit_str(key, &self.to_string())
    }
}

impl From<kvengine::ShardTag> for PeerTag {
    fn from(tag: kvengine::ShardTag) -> Self {
        Self {
            store_id: tag.engine_id,
            id_ver: RegionIdVer::new(tag.id_ver.id, tag.id_ver.ver),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct RegionIdVer {
    id: u64,
    ver: u64,
}

impl RegionIdVer {
    pub fn new(id: u64, ver: u64) -> Self {
        Self { id, ver }
    }

    pub fn from_region(region: &metapb::Region) -> Self {
        Self::new(region.get_id(), region.get_region_epoch().get_version())
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn ver(&self) -> u64 {
        self.ver
    }
}

pub(crate) const EMPTY_KEY: &[u8] = &[];
pub(crate) const RAW_INITIAL_END_KEY: &[u8] = &[255, 255, 255, 255, 255, 255, 255, 255];

// Get the `start_key` of current region in raw form.
pub fn raw_start_key(region: &metapb::Region) -> Vec<u8> {
    // only initialized region's start_key can be encoded, otherwise there must be
    // bugs somewhere.
    if region.start_key.is_empty() {
        return EMPTY_KEY.to_vec();
    }
    let mut slice = region.start_key.as_slice();
    decode_bytes(&mut slice, false).unwrap()
}

// Get the `end_key` of current region in raw form.
pub fn raw_end_key(region: &metapb::Region) -> Vec<u8> {
    // only initialized region's end_key can be encoded, otherwise there must be
    // bugs somewhere.
    if region.end_key.is_empty() {
        return RAW_INITIAL_END_KEY.to_vec();
    }
    let mut slice = region.end_key.as_slice();
    decode_bytes(&mut slice, false).unwrap()
}

pub fn region_has_peer(region: &metapb::Region, peer_id: u64) -> bool {
    region.get_peers().iter().any(|p| p.id == peer_id)
}

pub(crate) fn encode_split_flag_encryption_keys(encryption_keys: Vec<(u32, Vec<u8>)>) -> Vec<u8> {
    let mut buf = vec![];
    buf.put_u64(SPLIT_FLAG_ENCRYPTION_KEYS);
    buf.put_u32(encryption_keys.len() as u32);
    for (keyspace_id, key) in encryption_keys {
        buf.put_u32(keyspace_id);
        buf.put_u32(key.len() as u32);
        buf.extend_from_slice(&key);
    }
    buf
}

pub(crate) fn decode_split_flag_encryption_keys(mut flag_data: &[u8]) -> HashMap<u32, Vec<u8>> {
    let mut keyspace_encryption_keys = HashMap::new();
    let flag = flag_data.get_u64();
    assert_eq!(flag, SPLIT_FLAG_ENCRYPTION_KEYS);
    let mut num_keys = flag_data.get_u32();
    while num_keys > 0 {
        let keyspace_id = flag_data.get_u32();
        let key_len = flag_data.get_u32();
        let key = &flag_data[..key_len as usize];
        keyspace_encryption_keys.insert(keyspace_id, key.to_vec());
        flag_data = &flag_data[key_len as usize..];
        num_keys -= 1;
    }
    keyspace_encryption_keys
}

pub struct PdIdAllocator {
    pd: Arc<dyn pd_client::PdClient>,
}

const ALLOCATE_ID_TIMEOUT: Duration = Duration::from_secs(30 * 60);

impl PdIdAllocator {
    pub fn new(pd: Arc<dyn pd_client::PdClient>) -> Self {
        Self { pd }
    }
}

#[async_trait]
impl kvengine::IdAllocator for PdIdAllocator {
    fn alloc_id(&self, count: usize) -> kvengine::Result<Vec<u64>> {
        let start = Instant::now();
        loop {
            match block_on(self.pd.batch_get_tso(count as u32)) {
                Ok(ts) => {
                    let last = ts.into_inner();
                    let first = last - count as u64 + 1;
                    return Ok((first..=last).collect());
                }
                Err(err) => {
                    error!("failed to allocate file id from PD {:?}", err);
                    std::thread::sleep(Duration::from_secs(3));
                    if start.saturating_elapsed() > ALLOCATE_ID_TIMEOUT {
                        return Err(kvengine::Error::ErrAllocId(
                            "allocate file id timeout".to_string(),
                        ));
                    }
                }
            }
        }
    }

    async fn alloc_id_async(&self, count: usize) -> kvengine::Result<Vec<u64>> {
        let start = std::time::Instant::now();
        loop {
            match self.pd.batch_get_tso(count as u32).await {
                Ok(ts) => {
                    let last = ts.into_inner();
                    let first = last - count as u64 + 1;
                    return Ok((first..=last).collect());
                }
                Err(err) => {
                    error!("failed to allocate file id from PD {:?}", err);
                    let _ = GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + Duration::from_secs(3))
                        .compat()
                        .await;
                    if start.saturating_elapsed() > ALLOCATE_ID_TIMEOUT {
                        return Err(kvengine::Error::ErrAllocId(
                            "allocate file id timeout".to_string(),
                        ));
                    }
                }
            }
        }
    }
}

// Region(dependent_id) depends on Region(parent_id).
pub fn remove_dependent(
    rfengine: &rfengine::RfEngine,
    router: &RaftRouter,
    parent_id: u64,
    dependent_id: u64,
) {
    let dependent_len = rfengine.remove_dependent(parent_id, dependent_id);
    if dependent_len == 0 {
        router.send_store(StoreMsg::DependentsEmpty(parent_id));
    }
}

pub struct CpuUtilCollector {
    cpu_util: Arc<AtomicUsize>,
    thread_prefix: String,
    process_id: Pid,
    thread_ids: Vec<Pid>,
    update_time: std::time::Instant,
    cpu_total: f64,
}

impl CpuUtilCollector {
    pub fn new(thread_prefix: String) -> Self {
        let process_id = tikv_util::sys::thread::process_id();
        let update_time = std::time::Instant::now();
        Self {
            cpu_util: Arc::new(AtomicUsize::new(0)),
            thread_prefix,
            process_id,
            thread_ids: vec![],
            update_time,
            cpu_total: 0.0,
        }
    }

    pub(crate) fn update(&mut self) {
        if self.thread_ids.is_empty() {
            let name_map = tikv_util::sys::thread::THREAD_NAME_HASHMAP.lock().unwrap();
            for (k, v) in name_map.iter() {
                if v.starts_with(&self.thread_prefix) {
                    self.thread_ids.push(*k);
                }
            }
            drop(name_map);
        }
        let mut new_cpu_total = 0.0;
        let now = std::time::Instant::now();
        for tid in &self.thread_ids {
            if let Ok(stat) = tikv_util::sys::thread::thread_stat(self.process_id, *tid) {
                new_cpu_total += stat.total_cpu_time();
            }
        }
        let delta_cpu_time = new_cpu_total - self.cpu_total;
        let delta_duration = now.saturating_duration_since(self.update_time);
        let cpu_util = (delta_cpu_time * 100.0 / delta_duration.as_secs_f64()) as usize;
        self.cpu_util.store(cpu_util, Relaxed);
        self.update_time = now;
        self.cpu_total = new_cpu_total;
    }

    pub(crate) fn get_cpu_util_ref(&self) -> Arc<AtomicUsize> {
        self.cpu_util.clone()
    }
}

#[cfg(test)]
mod tests {
    use tikv_util::sys::thread::StdThreadBuildWrapper;

    use super::*;

    #[test]
    #[ignore]
    fn test_cpu_util_collector() {
        let mut collector = CpuUtilCollector::new("cpu_util_test".to_string());
        let mut handles = vec![];
        let sleep_micros = Arc::new(AtomicUsize::new(10));
        for i in 0..3 {
            let sleep_micros = sleep_micros.clone();
            let handle = std::thread::Builder::new()
                .name(format!("cpu_util_test_{}", i))
                .spawn_wrapper(move || {
                    let mut counter = 0u64;
                    loop {
                        // Busy work: incrementing a counter
                        counter = counter.wrapping_add(1);
                        if counter % (1 << 20) == 0 {
                            let us = sleep_micros.load(Relaxed);
                            std::thread::sleep(Duration::from_micros(us as u64));
                        }
                    }
                })
                .unwrap();
            handles.push(handle);
        }
        let cpu_ref = collector.get_cpu_util_ref();
        for sleep_dur in [10, 100, 1000] {
            sleep_micros.store(sleep_dur, Relaxed);
            println!("sleep {}us", sleep_dur);
            for _ in 0..5 {
                std::thread::sleep(Duration::from_millis(100));
                collector.update();
                println!("cpu:{}", cpu_ref.load(Relaxed));
            }
        }
    }
}
