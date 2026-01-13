// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    fs::{self, File, create_dir_all},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use bytes::{Buf, BufMut, Bytes};
use collections::{HashMap, HashMapExt};
use protobuf::Message;
use raft_proto::eraftpb;
use rfengine::RaftLogOp;
use tikv_util::{box_err, codec::number::U64_SIZE, info};
use txn_types::TimeStamp;

use crate::{Error, RaftLogOpWithCounter, RegionProgress, Result, StoreProgress};

const CHANGESET_VERSION: u32 = 1;
const CHANGESET_META_SIZE: usize = 16; // 4(version) + 4 (num_keyspace_ids) + 4 (num_stores) + 4 (checksum)

fn manifest_path(dir: &Path) -> PathBuf {
    dir.join("MANIFEST")
}

#[derive(Debug)]
pub(crate) struct Manifest {
    file_path: PathBuf,
    pub(crate) store_progresses: HashMap<u64, StoreProgress>,
    pub(crate) keyspace_states: HashMap<u32, Bytes>,
    pub(crate) uncommitted_entries: UncommittedEntries,

    /// The timestamp of synced target WAL progress.
    ///
    /// Used to determine the source of WAL target for next startup.
    pub(crate) synced_target_ts: TimeStamp,
}

impl Manifest {
    pub(crate) fn open(dir: &Path) -> Result<Self> {
        if !dir.exists() {
            create_dir_all(dir)?;
        }
        let file_path = manifest_path(dir);
        let mut keyspace_states = HashMap::default();
        let mut store_progresses = HashMap::default();
        let file_data_vec = fs::read(&file_path).unwrap_or_default();
        if file_data_vec.len() < CHANGESET_META_SIZE {
            return Ok(Self {
                file_path,
                store_progresses,
                keyspace_states,
                uncommitted_entries: Default::default(),
                synced_target_ts: TimeStamp::zero(),
            });
        }
        let content_length = file_data_vec.len() - 4;
        let mut content = &file_data_vec[..content_length];
        let checksum = (&file_data_vec[content_length..]).get_u32_le();
        if crc32fast::hash(content) != checksum {
            return Err(
                std::io::Error::new(std::io::ErrorKind::InvalidData, "checksum mismatch").into(),
            );
        }
        let version = content.get_u32_le();
        if version > CHANGESET_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unsupported changeset version",
            )
            .into());
        }

        let num_keyspaces = content.get_u32_le() as usize;
        for _ in 0..num_keyspaces {
            let keyspace_id = content.get_u32_le();
            let keyspace_state_len = content.get_u32_le() as usize;
            let keyspace_state = &content[..keyspace_state_len];
            content.advance(keyspace_state_len);
            keyspace_states.insert(keyspace_id, keyspace_state.to_vec().into());
        }

        let num_stores = content.get_u32_le() as usize;
        for _ in 0..num_stores {
            let store_progress = StoreProgress::decode(&mut content)?;
            store_progresses.insert(store_progress.store_id, store_progress);
        }

        let uncommitted_entries = UncommittedEntries::decode(&mut content)?;

        let mut synced_target_ts = TimeStamp::zero();
        if content.remaining() >= U64_SIZE {
            synced_target_ts = TimeStamp::new(content.get_u64_le());
        }

        Ok(Self {
            file_path,
            store_progresses,
            keyspace_states,
            uncommitted_entries,
            synced_target_ts,
        })
    }

    pub(crate) fn persist(&self) -> Result<()> {
        self.persist_impl(true)
    }

    // TODO: remove `with_synced_target_ts` after next upgrade.
    fn persist_impl(&self, with_synced_target_ts: bool) -> Result<()> {
        let dir = self.file_path.parent().unwrap();
        let tmp_path = self.file_path.with_extension("tmp");
        let tmp_file = File::create(&tmp_path)?;

        let mut buf = vec![];
        buf.put_u32_le(CHANGESET_VERSION);
        buf.put_u32_le(self.keyspace_states.len() as u32);
        for (&keyspace_id, states) in &self.keyspace_states {
            buf.put_u32_le(keyspace_id);
            buf.put_u32_le(states.len() as u32);
            buf.extend_from_slice(states);
        }
        buf.put_u32_le(self.store_progresses.len() as u32);
        for progress in self.store_progresses.values() {
            progress.encode(&mut buf);
        }
        self.uncommitted_entries.encode(&mut buf);

        if with_synced_target_ts {
            buf.put_u64_le(self.synced_target_ts.into_inner());
        } else {
            assert!(cfg!(test));
        }

        let checksum = crc32fast::hash(&buf);
        buf.put_u32_le(checksum);

        let file_len = buf.len();

        tmp_file.write_all_at(&buf, 0)?;
        tmp_file.sync_all()?;
        fs::rename(&tmp_path, &self.file_path)?;
        file_system::sync_dir(dir)?;

        info!("manifest persist"; "file_len" => file_len);
        Ok(())
    }

    pub(crate) fn update_store_progress(&mut self, store_id: u64, epoch: u32, offset: u64) {
        let store_progress =
            self.store_progresses
                .entry(store_id)
                .or_insert_with(|| StoreProgress {
                    store_id,
                    epoch,
                    offset,
                });
        store_progress.epoch = epoch;
        store_progress.offset = offset;
    }

    pub(crate) fn update_region_progresses(
        &mut self,
        region_progresses: &HashMap<u64 /* region_id */, RegionProgress>,
    ) {
        let uncommited_entries = UncommittedEntries::from_region_progresses(region_progresses);
        self.uncommitted_entries = uncommited_entries;
    }

    pub(crate) fn update_synced_target_ts(&mut self, synced_target_ts: TimeStamp) {
        self.synced_target_ts = synced_target_ts;
    }

    pub(crate) fn set_keyspace_states(&mut self, keyspace_id: u32, states: Bytes) -> Option<Bytes> {
        self.keyspace_states.insert(keyspace_id, states)
    }
}

#[derive(Default)]
pub(crate) struct UncommittedEntries {
    regions: HashMap<u64 /* region_id */, HashMap<u64 /* log_index */, RaftLogOpWithCounter>>,
}

impl fmt::Debug for UncommittedEntries {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("UncommittedEntries");
        for (region_id, entries) in &self.regions {
            let mut indexes: Vec<_> = entries.keys().collect();
            indexes.sort_unstable();
            de.field(&format!("region_{}", region_id), &indexes);
        }
        de.finish()
    }
}

impl UncommittedEntries {
    pub(crate) fn from_region_progresses(
        region_progresses: &HashMap<u64 /* region_id */, RegionProgress>,
    ) -> Self {
        let regions = region_progresses
            .iter()
            .map(|(&region_id, progress)| (region_id, progress.entries.clone()))
            .collect();
        Self { regions }
    }

    pub(crate) fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u32_le(self.regions.len() as u32);
        for (&region_id, entries) in &self.regions {
            buf.put_u64_le(region_id);
            buf.put_u32_le(entries.len() as u32);
            for entry in entries.values() {
                buf.put_u8(entry.counter());
                let entry_bytes = entry.to_entry().write_to_bytes().unwrap();
                buf.put_u32_le(entry_bytes.len() as u32);
                buf.put_slice(&entry_bytes);
            }
        }
    }

    pub(crate) fn decode(buf: &mut impl Buf) -> Result<Self> {
        let num_regions = buf.get_u32_le() as usize;
        let mut regions = HashMap::with_capacity(num_regions);
        for _ in 0..num_regions {
            let region_id = buf.get_u64_le();
            let num_entries = buf.get_u32_le() as usize;
            let mut entries = HashMap::with_capacity(num_entries);
            for _ in 0..num_entries {
                let counter = buf.get_u8();
                let entry_len = buf.get_u32_le() as usize;
                if buf.remaining() < entry_len {
                    return Err(box_err!(
                        "invalid entry length: {} remaining: {}",
                        entry_len,
                        buf.remaining()
                    ));
                }
                let entry_bytes = &buf.chunk()[..entry_len];
                let mut entry = eraftpb::Entry::default();
                entry
                    .merge_from_bytes(entry_bytes)
                    .map_err(|e| -> Error { box_err!("invalid entry data: {}", e) })?;
                buf.advance(entry_len);
                let op = RaftLogOp::new(&entry);
                entries.insert(entry.index, RaftLogOpWithCounter { op, counter });
            }
            regions.insert(region_id, entries);
        }
        Ok(Self { regions })
    }

    pub(crate) fn get_region_entries(
        &self,
        region_id: u64,
    ) -> Option<&HashMap<u64, RaftLogOpWithCounter>> {
        self.regions.get(&region_id)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Buf;
    use collections::HashMap;
    use rfengine::RaftLogOp;

    use crate::{RaftLogOpWithCounter, RegionProgress, Result, StoreProgress, manifest::Manifest};

    #[test]
    fn test_manifest() -> Result<()> {
        let dir = tempfile::Builder::new().prefix("manifest_test").tempdir()?;

        let region_entry_indexes = vec![
            (1, vec![10, 11, 11]),
            (2, vec![20]),
            (3, vec![30, 31, 32, 30]),
        ];
        let (region_entries, region_progresses) =
            make_region_entries_and_progresses(region_entry_indexes);

        let mut manifest = Manifest::open(dir.path())?;
        manifest.update_store_progress(1, 2, 3);
        manifest.update_region_progresses(&region_progresses);
        manifest.set_keyspace_states(4, "abc".into());
        manifest.update_synced_target_ts(1000.into());
        manifest.persist()?;
        drop(manifest);
        let manifest = Manifest::open(dir.path())?;
        assert_eq!(manifest.store_progresses.len(), 1);
        let persisted = manifest.store_progresses.get(&1).unwrap();
        assert_eq!(persisted.store_id, 1);
        assert_eq!(persisted.epoch, 2);
        assert_eq!(persisted.offset, 3);
        assert_eq!(manifest.keyspace_states.len(), 1);
        assert_eq!(manifest.keyspace_states.get(&4).unwrap().chunk(), b"abc");
        assert_eq!(manifest.uncommitted_entries.regions, region_entries);
        assert_eq!(manifest.synced_target_ts.into_inner(), 1000);
        Ok(())
    }

    #[test]
    fn test_manifest_compat() -> Result<()> {
        let dir = tempfile::Builder::new().prefix("manifest_test").tempdir()?;

        let expected_store_progress = StoreProgress {
            store_id: 1,
            epoch: 2,
            offset: 3,
        };

        {
            let mut manifest = Manifest::open(dir.path())?;
            manifest.update_store_progress(1, 2, 3);
            manifest.update_synced_target_ts(1000.into());
            // Generate old version manifest.
            manifest.persist_impl(false)?;
        }

        {
            let mut manifest = Manifest::open(dir.path())?;
            assert_eq!(manifest.store_progresses.len(), 1);
            assert_eq!(
                manifest.store_progresses.get(&1).unwrap(),
                &expected_store_progress
            );
            assert_eq!(manifest.synced_target_ts.into_inner(), 0);

            manifest.update_synced_target_ts(2000.into());
            // Generate new version manifest.
            manifest.persist()?;
        }

        {
            let manifest = Manifest::open(dir.path())?;
            assert_eq!(manifest.store_progresses.len(), 1);
            assert_eq!(
                manifest.store_progresses.get(&1).unwrap(),
                &expected_store_progress
            );
            assert_eq!(manifest.synced_target_ts.into_inner(), 2000);
        }
        Ok(())
    }

    fn make_entries(entry_indexes: &[u64]) -> HashMap<u64, RaftLogOpWithCounter> {
        let mut m: HashMap<u64, RaftLogOpWithCounter> = Default::default();
        for &index in entry_indexes {
            m.entry(index)
                .and_modify(|op| op.inc_counter())
                .or_insert_with(|| {
                    RaftLogOp {
                        index,
                        ..Default::default()
                    }
                    .into()
                });
        }
        m
    }

    fn make_region_entries_and_progresses(
        region_entry_indexes: Vec<(u64 /* region_id */, Vec<u64> /* log_index */)>,
    ) -> (
        HashMap<u64, HashMap<u64, RaftLogOpWithCounter>>,
        HashMap<u64, RegionProgress>,
    ) {
        let region_entries: HashMap<u64, HashMap<u64, RaftLogOpWithCounter>> = region_entry_indexes
            .iter()
            .map(|(region_id, indexes)| (*region_id, make_entries(indexes)))
            .collect();
        let region_progresses: HashMap<u64, RegionProgress> = region_entry_indexes
            .iter()
            .map(|(region_id, indexes)| {
                let mut progress = RegionProgress::new(1, 100);
                progress.entries = make_entries(indexes);
                (*region_id, progress)
            })
            .collect();
        (region_entries, region_progresses)
    }
}
