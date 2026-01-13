// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, VecDeque},
    fs,
    fs::{File, OpenOptions},
    io,
    ops::{Deref, DerefMut},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::{Buf, BufMut};
use protobuf::Message;
use tikv_util::{error, info, warn};

use crate::{
    PeerMeta, TRUNCATE_ALL_INDEX, metrics::RFENGINE_RLOG_GC_SIZE, raft_log_file_name,
    writer::EPOCH_SNAPSHOT_LEN,
};

const REWRITE_DIFF: u32 = 10;

#[derive(Debug)]
pub(crate) struct Manifest {
    pub(crate) epoch_id: u32,
    pub(crate) delayed_epoches: u32,
    pub(crate) file_path: PathBuf,
    pub(crate) file: File,
    pub(crate) peers: HashMap<u64, PeerMetaFiles>,
    pub(crate) offset: u64,
    pub(crate) engine_id: Arc<AtomicU64>,
    first_epoch: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct PeerMetaFiles {
    meta: PeerMeta,
    pub(crate) files: VecDeque<PeerFile>,
}

impl Deref for PeerMetaFiles {
    type Target = PeerMeta;

    fn deref(&self) -> &Self::Target {
        &self.meta
    }
}

impl DerefMut for PeerMetaFiles {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.meta
    }
}

impl PeerMetaFiles {
    fn new(region_id: u64, keyspace_id: u32) -> Self {
        Self {
            meta: PeerMeta::new(region_id, keyspace_id),
            files: Default::default(),
        }
    }

    pub(crate) fn need_truncate(&self) -> bool {
        self.files
            .front()
            .map(|f| f.last_index <= self.truncated_idx)
            .unwrap_or_default()
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub struct PeerFile {
    pub first_index: u64,
    pub last_index: u64,
}

impl From<&rfenginepb::RaftLogFile> for PeerFile {
    fn from(file: &rfenginepb::RaftLogFile) -> Self {
        Self {
            first_index: file.first_index,
            last_index: file.last_index,
        }
    }
}

impl Manifest {
    pub(crate) fn open(dir: &Path, engine_id: Arc<AtomicU64>) -> crate::Result<Self> {
        let file_path = manifest_path(dir);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&file_path)?;
        let mut manifest = Self {
            epoch_id: 0,
            delayed_epoches: 0,
            engine_id,
            file_path,
            file,
            peers: HashMap::new(),
            offset: 0,
            first_epoch: 0,
        };
        manifest.init()?;
        Ok(manifest)
    }

    fn init(&mut self) -> crate::Result<()> {
        let file_data_vec = fs::read(&self.file_path)?;
        let mut file_data = file_data_vec.as_slice();
        while file_data.len() > 8 {
            let checksum = file_data.get_u32_le();
            let length = file_data.get_u32_le() as usize;
            if file_data.len() < length {
                warn!("manifest file unexpected EOF");
                break;
            }
            let change_set_data = &file_data[..length];
            if crc32c::crc32c(change_set_data) != checksum {
                warn!("manifest file checksum mismatch");
                break;
            }
            self.offset += length as u64 + 8;
            let mut change_set = rfenginepb::ChangeSet::new();
            change_set.merge_from_bytes(change_set_data).unwrap();
            self.apply_change_set(&change_set)?;
            file_data = &file_data[length..];
        }
        self.truncate_files();
        info!("init manifest epoch {}", self.epoch_id);
        Ok(())
    }

    pub(crate) fn handle_compaction(&mut self, cs: rfenginepb::ChangeSet) -> crate::Result<()> {
        self.apply_change_set(&cs)?;
        let engine_id = self.get_engine_id();
        if self.epoch_id - self.first_epoch > REWRITE_DIFF {
            info!("{}: rewrite manifest epoch: {}", engine_id, self.epoch_id);
            self.rewrite()?;
            self.first_epoch = self.epoch_id;
        } else {
            info!("{}: append manifest epoch: {}", engine_id, self.epoch_id);
            self.persist_change_set(cs)?;
        }
        self.truncate_files();
        Ok(())
    }

    fn persist_change_set(&mut self, cs: rfenginepb::ChangeSet) -> io::Result<()> {
        self.offset = persist_change_set(&self.file, self.offset, &cs)?;
        Ok(())
    }

    fn apply_change_set(&mut self, cs: &rfenginepb::ChangeSet) -> crate::Result<()> {
        assert!(cs.epoch_id > self.epoch_id);
        self.epoch_id = cs.epoch_id;
        self.delayed_epoches = cs.delayed_epoches;
        if self.first_epoch == 0 {
            self.first_epoch = cs.epoch_id;
        }
        for peer_meta_pb in cs.get_peers() {
            let peer_meta = self.peers.entry(peer_meta_pb.peer_id).or_insert_with(|| {
                PeerMetaFiles::new(peer_meta_pb.region_id, peer_meta_pb.keyspace_id)
            });
            if peer_meta.truncated_idx < peer_meta_pb.truncated_index {
                peer_meta.truncated_idx = peer_meta_pb.truncated_index;
            }
            for state_pb in peer_meta_pb.get_states() {
                if state_pb.value.is_empty() {
                    peer_meta.remove_state(state_pb.get_key());
                } else {
                    peer_meta.set_state(state_pb.get_key(), state_pb.get_value());
                }
            }
            let pb_keyspace_id = peer_meta_pb.get_keyspace_id();
            if pb_keyspace_id > 0 {
                peer_meta.keyspace_id = pb_keyspace_id;
            } else if peer_meta.keyspace_id == 0 {
                peer_meta.keyspace_id = peer_meta.get_keyspace_id().unwrap_or_default();
            }
            for file in peer_meta_pb.get_files() {
                peer_meta.files.push_back(file.into());
            }
        }
        Ok(())
    }

    fn rewrite(&mut self) -> crate::Result<()> {
        let dir = self.file_path.parent().unwrap();
        let tmp_path = self.file_path.with_extension("tmp");
        let tmp_file = File::create(&tmp_path)?;
        let change_set = self.to_change_set(false);
        self.offset = persist_change_set(&tmp_file, 0, &change_set)?;
        fs::rename(&tmp_path, &self.file_path)?;
        file_system::sync_dir(dir)?;
        self.file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.file_path)?;
        Ok(())
    }

    pub(crate) fn to_change_set(&self, exclude_tombstone: bool) -> rfenginepb::ChangeSet {
        let mut cs = rfenginepb::ChangeSet::default();
        cs.epoch_id = self.epoch_id;
        cs.delayed_epoches = self.delayed_epoches;
        for (&peer_id, peer_meta) in &self.peers {
            if exclude_tombstone && peer_meta.truncated_idx == TRUNCATE_ALL_INDEX {
                continue;
            }
            let mut meta_pb = rfenginepb::PeerMeta::new();
            meta_pb.set_peer_id(peer_id);
            meta_pb.set_region_id(peer_meta.region_id);
            meta_pb.set_truncated_index(peer_meta.truncated_idx);
            meta_pb.set_keyspace_id(peer_meta.keyspace_id);
            for (key, val) in &peer_meta.states {
                let mut state = rfenginepb::PeerState::new();
                state.set_key(key.to_vec());
                state.set_value(val.to_vec());
                meta_pb.mut_states().push(state);
            }
            for file in &peer_meta.files {
                let mut raft_log_file = rfenginepb::RaftLogFile::default();
                raft_log_file.set_first_index(file.first_index);
                raft_log_file.set_last_index(file.last_index);
                meta_pb.mut_files().push(raft_log_file);
            }
            cs.mut_peers().push(meta_pb);
        }
        cs
    }

    fn truncate_files(&mut self) {
        let dir = self.file_path.parent().unwrap();
        let engine_id = self.engine_id.load(Ordering::SeqCst);
        for (&peer_id, peer_meta) in &mut self.peers {
            let mut removed_count = 0;
            let mut last_index = 0;
            while peer_meta.need_truncate() {
                let file = peer_meta.files.pop_front().unwrap();
                let filename = raft_log_file_name(dir, peer_id, file.first_index, file.last_index);
                if filename.exists() {
                    let region_id = peer_meta.region_id;
                    let file_size = fs::metadata(filename.as_path()).unwrap().len();
                    if let Err(err) = fs::remove_file(filename.as_path()) {
                        error!(
                            "{}:{} failed to remove rlog file {:?}, {:?}",
                            engine_id, region_id, filename, err
                        );
                        RFENGINE_RLOG_GC_SIZE
                            .with_label_values(&["error"])
                            .observe(file_size as f64);
                    } else {
                        removed_count += 1;
                        last_index = file.last_index;
                        RFENGINE_RLOG_GC_SIZE
                            .with_label_values(&["success"])
                            .observe(file_size as f64);
                    }
                }
            }
            if removed_count > 0 {
                info!(
                    "{}:{} removed {} rlog files before index {}",
                    engine_id, peer_meta.region_id, removed_count, last_index
                );
            }
        }
    }

    pub(crate) fn get_engine_id(&self) -> u64 {
        self.engine_id.load(Ordering::SeqCst)
    }

    /// The epoch of next snapshot.
    pub(crate) fn next_snapshot_epoch(epoch_id: u32) -> u32 {
        epoch_id.saturating_add(EPOCH_SNAPSHOT_LEN - 1) / EPOCH_SNAPSHOT_LEN * EPOCH_SNAPSHOT_LEN
    }
}

pub(crate) fn manifest_path(dir: &Path) -> PathBuf {
    dir.join("MANIFEST")
}

pub(crate) fn persist_change_set(
    file: &File,
    mut offset: u64,
    cs: &rfenginepb::ChangeSet,
) -> io::Result<u64> {
    let buf = cs.write_to_bytes().unwrap();
    if buf.is_empty() {
        return Ok(0);
    }
    let mut header_buf = Vec::with_capacity(8);
    header_buf.put_u32_le(crc32c::crc32c(&buf));
    header_buf.put_u32_le(buf.len() as u32);
    file.write_all_at(&header_buf, offset)?;
    offset += 8;
    file.write_all_at(&buf, offset)?;
    offset += buf.len() as u64;
    file.sync_data()?;
    Ok(offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_snapshot_epoch() {
        assert_eq!(Manifest::next_snapshot_epoch(0), 0);
        assert_eq!(Manifest::next_snapshot_epoch(1), 8);
        assert_eq!(Manifest::next_snapshot_epoch(3), 8);
        assert_eq!(Manifest::next_snapshot_epoch(4), 8);
        assert_eq!(Manifest::next_snapshot_epoch(7), 8);
        assert_eq!(Manifest::next_snapshot_epoch(8), 8);
        assert_eq!(Manifest::next_snapshot_epoch(9), 16);
        assert_eq!(Manifest::next_snapshot_epoch(u32::MAX), 0xffff_fff8);
    }
}
