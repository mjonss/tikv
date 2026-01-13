// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs,
    os::unix::fs::FileExt,
    path::Path,
    sync::{Arc, atomic::Ordering},
};

use byteorder::{ByteOrder, LittleEndian};
use bytes::Buf;
use tikv_util::{errors::Context as _, info, warn};

use crate::{log_batch::RaftLogOp, manifest::Manifest, service_worker::ServiceTask, *};

impl RfEngineCore {
    pub(crate) fn load(
        &mut self,
        manifest: &Manifest,
    ) -> Result<Option<(u32, u64)> /* async_epoch_offset */> {
        for (&peer_id, peer_meta) in &manifest.peers {
            let guard = self.peers.peers.guard();
            let peer_ref = self.get_or_init_peer_data(
                peer_id,
                peer_meta.region_id,
                peer_meta.keyspace_id,
                &guard,
            );
            let mut peer_data = peer_ref.write().unwrap();
            peer_data.meta.merge(peer_meta, false);
            drop(peer_data);
            drop(guard);
            for file in &peer_meta.files {
                self.load_raft_log_file(
                    peer_id,
                    peer_meta.region_id,
                    peer_meta.keyspace_id,
                    file.first_index,
                    file.last_index,
                )?;
            }
        }
        let first_wal_epoch = manifest.epoch_id + 1;
        let async_epoch = if self.is_async_wal_enabled() {
            Some(Self::get_last_wal_epoch(
                &self.dir,
                first_wal_epoch,
                self.epoch_rotate_len,
            ))
        } else {
            None
        };
        let sync_epoch =
            Self::get_last_wal_epoch(self.wal_dir(), first_wal_epoch, self.epoch_rotate_len);
        let mut sync_wal_offset = 0;
        let mut async_wal_offset = None;
        for epoch in first_wal_epoch..=sync_epoch {
            let send_rotate = epoch > first_wal_epoch;
            let send_write = async_epoch.is_some_and(|async_epoch| epoch >= async_epoch);
            let is_async_epoch = async_epoch == Some(epoch);
            let is_sync_epoch = epoch == sync_epoch;
            if send_rotate {
                self.try_send_task(ServiceTask::Rotate {
                    epoch_id: epoch - 1,
                });
            }
            let (sync_offset, async_offset) =
                self.load_wal_file(epoch, is_async_epoch, send_write)?;
            if is_sync_epoch {
                sync_wal_offset = sync_offset;
            }
            if is_async_epoch {
                async_wal_offset = Some(async_offset);
            }
        }
        let mut writer = self.writer.lock().unwrap();
        writer.open_file(sync_epoch, sync_wal_offset)?;
        self.current_epoch_id.store(sync_epoch, Ordering::SeqCst);
        Ok(async_epoch.zip(async_wal_offset))
    }

    fn get_last_wal_epoch(dir: &Path, since_epoch: u32, epoch_rotate_len: usize) -> u32 {
        let mut epoch = since_epoch;
        while wal_exists(dir, epoch + 1, epoch_rotate_len) {
            epoch += 1;
        }
        epoch
    }

    pub(crate) fn load_wal_file(
        &mut self,
        epoch_id: u32,
        load_async: bool,
        send_write: bool,
    ) -> Result<(u64 /* sync_offset */, u64 /* async_offset */)> {
        info!("load wal {}", epoch_id);
        let mut async_batch_cnt = 0;
        let mut async_offset = 0;
        if self.is_async_wal_enabled() && load_async {
            let mut async_it = WalIterator::new(&self.dir, epoch_id, self.epoch_rotate_len)?;
            let async_res = async_it.iterate_batch(|_, _| {
                async_batch_cnt += 1;
            });
            match async_res {
                Ok(_) => {
                    async_offset = async_it.offset;
                }
                Err(Error::Corruption { msg, offset, .. }) => {
                    warn!(
                        "async wal corrupted at epoch: {}, offset: {}, msg: {}, try to truncate it to offset {}",
                        epoch_id, offset, msg, offset
                    );
                    // If the async wal file is corrupted, just ignored the data after the offset,
                    // and the remaining data will be recovered from sync wal later.
                    async_offset = offset;
                }
                Err(e) => return Err(e),
            }
        }
        let mut sync_batch_idx = 0;
        let mut it = WalIterator::new(self.wal_dir(), epoch_id, self.epoch_rotate_len)?;
        let sync_res = it.iterate_batch(|data, _| {
            sync_batch_idx += 1;
            let mut wb =
                if self.is_async_wal_enabled() && sync_batch_idx > async_batch_cnt && send_write {
                    Some(WriteBatch::new())
                } else {
                    None
                };
            iterate_peer_batch(data, |peer_batch| {
                let guard = self.peers.peers.guard();
                let peer_ref = self.get_or_init_peer_data(
                    peer_batch.peer_id,
                    peer_batch.meta.region_id,
                    peer_batch.keyspace_id,
                    &guard,
                );
                let mut peer_data = peer_ref.write().unwrap();
                let _ = peer_data.apply(&peer_batch);
                if let Some(wb) = &mut wb {
                    wb.peers.insert(peer_batch.peer_id, peer_batch);
                }
            });
            if let Some(wb) = wb {
                let wb_vec = Arc::new(wb.into_vector());
                self.try_send_task(ServiceTask::Write { wb: wb_vec });
            }
        });
        match sync_res {
            Ok(_) => {}
            Err(Error::Corruption {
                msg, offset, data, ..
            }) => {
                if !is_last_wal(self.wal_dir(), epoch_id, self.epoch_rotate_len) {
                    return Err(Error::Corruption {
                        msg,
                        offset,
                        epoch_id,
                        data,
                    });
                }

                warn!(
                    "sync wal corrupted at epoch: {}, offset: {}, msg: {}, try to truncate it to offset {}",
                    epoch_id, offset, msg, offset,
                );
                // If the write batch data corrupted and it's in the last wal file,
                // we reset the aligned wal header to EOF.
                let file = fs::File::options().write(true).open(wal_file_name(
                    self.wal_dir(),
                    epoch_id,
                    self.epoch_rotate_len,
                ))?;
                file.write_all_at(&[0u8; BATCH_HEADER_SIZE], offset)?;
                file.sync_all()?;
            }
            Err(e) => return Err(e),
        }

        info!(
            "load wal done, it.offset {}, async_offset {}",
            it.offset, async_offset
        );
        Ok((it.offset, async_offset))
    }

    // `replay_wal_file` replay a wal chunk data to rfengine. It's used for
    // lightweight restoration to recover a `BackupCluster` from an previous
    // snapshot.
    pub fn replay_wal_file(
        &self,
        reader: Box<dyn std::io::Read>,
        epoch_id: u32,
        end_offset: u64, // u64::MAX means replay all the chunk
        full_restore: bool,
    ) -> Result<()> {
        let mut it = WalIterator::new_from_reader(reader, epoch_id, 0);
        it.iterate_batch(|data, offset| {
            // `offset` is the data read position after `data` be read.
            if offset > end_offset {
                return;
            }
            let mut wb = WriteBatch::new();
            iterate_peer_batch(data, |peer_batch| {
                wb.peers.insert(peer_batch.peer_id, peer_batch);
            });
            if full_restore {
                self.write(wb).unwrap();
            } else {
                // In scene of restore keyspace, we don't persist WAL to avoid unnecessary I/O,
                // as the rfengine is only used temporarily during the restoration process.
                self.apply(&wb);
            }
        })?;
        Ok(())
    }

    pub(crate) fn load_raft_log_file(
        &mut self,
        peer_id: u64,
        region_id: u64,
        keyspace_id: u32,
        first: u64,
        last: u64,
    ) -> Result<()> {
        let rlog_filename = raft_log_file_name(&self.dir, peer_id, first, last);
        let bin = fs::read(&rlog_filename)
            .with_ctx(|| format!("read rlog {}", rlog_filename.display()))?;
        // See format in Worker::write_raft_log_file
        let header = RlogHeader::decode(bin.as_slice())?;
        let mut data = &bin[RlogHeader::len()..];
        let mut end_offs = vec![];
        for _ in 0..header.count {
            end_offs.push(data.get_u32_le());
        }
        let guard = self.peers.peers.guard();
        let peer_data_ref = self.get_or_init_peer_data(peer_id, region_id, keyspace_id, &guard);
        let mut peer_data = peer_data_ref.write().unwrap();
        for i in 0..header.count as usize {
            if first + i as u64 <= peer_data.truncated_idx {
                continue;
            }
            let start_off = if i == 0 {
                0usize
            } else {
                end_offs[i - 1] as usize
            };
            let end_off = end_offs[i] as usize;
            let log_data = &data[start_off..end_off - 4];
            let checksum = LittleEndian::read_u32(&data[end_off - 4..]);
            let actual_checksum = crc32c::crc32c(log_data);
            if checksum != actual_checksum {
                return Err(Error::Corruption {
                    msg: format!(
                        "checksum mismatch: header.checksum {:x}, log_data.checksum {:x}",
                        checksum, actual_checksum
                    ),
                    epoch_id: 0,
                    offset: start_off as u64,
                    data: log_data.to_vec(),
                });
            }
            let raft_log = RaftLogOp::decode(log_data);
            peer_data.raft_logs.append(peer_id, raft_log);
        }
        Ok(())
    }
}

pub(crate) fn wal_exists(dir: &Path, epoch_id: u32, epoch_rotate_len: usize) -> bool {
    check_wal_header(dir, epoch_id, epoch_rotate_len).is_ok()
}

pub(crate) fn is_last_wal(dir: &Path, epoch_id: u32, epoch_rotate_len: usize) -> bool {
    wal_exists(dir, epoch_id, epoch_rotate_len) && !wal_exists(dir, epoch_id + 1, epoch_rotate_len)
}

#[cfg(test)]
mod tests {

    use std::{fs::OpenOptions, sync::atomic::Ordering, time::Duration};

    use super::{config::Config, *};
    use crate::test_util::{init_logger, make_log_data, make_state_kv, prepare_rfengine, try_wait};

    #[test]
    fn test_load_async_corruption() {
        init_logger();
        let tmp_dir = tempfile::tempdir().unwrap();
        let wal_size = 128 * 1024_usize;
        let dir_path = tmp_dir.path();
        let sync_wal_path = dir_path.join("wal-sync");
        let mut cfg = Config::new(wal_size);
        cfg.wal_sync_dir = sync_wal_path.to_str().unwrap().to_owned();

        let engine = RfEngine::open(dir_path, &cfg, None, None).unwrap();
        prepare_rfengine(&engine);
        engine.stop_worker(true);

        let writer = engine.writer.lock().unwrap();
        let current_epoch = writer.get_epoch_id();

        let mut it = WalIterator::new(dir_path, current_epoch, cfg.epoch_rotate_len).unwrap();
        it.iterate_batch(|_, _| {
            // Do nothing.
        })
        .unwrap();

        // Write some corrupted data to the last page of the async wal file.
        let filename = wal_file_name(dir_path, current_epoch, cfg.epoch_rotate_len);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(filename)
            .unwrap();
        let write_offset = it.offset - 4096 + BATCH_HEADER_SIZE as u64;

        info!("write corrupted data to async wal offset: {}", write_offset);
        file.write_all_at(&[0u8; 10], write_offset).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // RfEngine should be able to recover from the corrupted async wal file.
        let engine = RfEngine::open(dir_path, &cfg, None, None).unwrap();
        assert_eq!(engine.peers.peers.len(), 10);
        check_async_wal(&engine, current_epoch, 0, it.offset, Duration::from_secs(5));
    }

    #[test]
    fn test_load_sync_corruption() {
        init_logger();
        let tmp_dir = tempfile::tempdir().unwrap();
        let wal_size = 128 * 1024_usize;
        let dir_path = tmp_dir.path();
        let sync_wal_path = dir_path.join("wal-sync");
        let mut cfg = Config::new(wal_size);
        cfg.wal_sync_dir = sync_wal_path.to_str().unwrap().to_owned();

        let engine = RfEngine::open(dir_path, &cfg, None, None).unwrap();
        prepare_rfengine(&engine);
        engine.stop_worker(true);

        let writer = engine.writer.lock().unwrap();
        let current_epoch = writer.get_epoch_id();

        let mut async_it = WalIterator::new(dir_path, current_epoch, cfg.epoch_rotate_len).unwrap();
        async_it
            .iterate_batch(|_, _| {
                // Do nothing.
            })
            .unwrap();

        let offset = async_it.offset;
        // Truncate the last write batch of async wal file.
        let filename = wal_file_name(dir_path, current_epoch, cfg.epoch_rotate_len);
        let file = OpenOptions::new().write(true).open(filename).unwrap();
        info!("truncate async wal to offset: {}", offset - 4096);
        file.write_all_at(&[0u8; BATCH_HEADER_SIZE], offset - 4096)
            .unwrap();
        file.sync_all().unwrap();
        drop(file);

        let filename = wal_file_name(&sync_wal_path, current_epoch, cfg.epoch_rotate_len);
        let mut sync_it =
            WalIterator::new(&sync_wal_path, current_epoch, cfg.epoch_rotate_len).unwrap();
        sync_it
            .iterate_batch(|_, _| {
                // Do nothing.
            })
            .unwrap();

        // Write corrupted data to the last write batch of sync wal file.
        let file = OpenOptions::new().write(true).open(filename).unwrap();
        let write_offset = sync_it.offset - 4096 + BATCH_HEADER_SIZE as u64;
        info!(
            "write corrupted data to sync wal offset: {} at epoch: {}",
            write_offset, current_epoch
        );
        file.write_all_at(&[0u8; 10], write_offset).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // Backup MANIFEST to rollback later.
        let manifest_filename = dir_path.join("MANIFEST");
        let manifest_filename_bak = dir_path.join("MANIFEST.bak");
        fs::copy(&manifest_filename, &manifest_filename_bak).unwrap();

        // RfEngine should be able to recover from the last corrupted sync wal file.
        let engine = RfEngine::open(dir_path, &cfg, None, None).unwrap();
        assert_eq!(engine.peers.peers.len(), 10);

        let mut sync_it =
            WalIterator::new(&sync_wal_path, current_epoch, cfg.epoch_rotate_len).unwrap();
        sync_it.iterate_batch(|_, _| {}).unwrap();
        check_async_wal(
            &engine,
            current_epoch,
            0,
            sync_it.offset,
            Duration::from_secs(5),
        );

        // Write some more data to rotate the wal file.
        for idx in 1050..=1080 {
            let mut wb = WriteBatch::new();
            for peer_id in 1..=10_u64 {
                let region_id = peer_id + 1;
                wb.append_raft_log(peer_id, region_id, 1, &make_log_data(idx, 128));
                let (key, val) = make_state_kv(1, idx);
                wb.set_state(peer_id, region_id, 1, key.chunk(), val.chunk());
            }
            engine.write(wb).unwrap();
        }
        engine.stop_worker(false);
        drop(engine);

        // Rollback MANIFEST to the previous backup one.
        fs::copy(&manifest_filename_bak, &manifest_filename).unwrap();
        let engine = RfEngine::open(dir_path, &cfg, None, None).unwrap();
        let compacted_epoch = engine.compacted_epoch.load(Ordering::Relaxed);
        let current_epoch = {
            let writer = engine.writer.lock().unwrap();
            writer.get_epoch_id()
        };
        engine.stop_worker(false);
        drop(engine);

        info!(
            "compacted_epoch: {}, current_epoch: {}",
            compacted_epoch, current_epoch
        );
        assert!(compacted_epoch + 1 < current_epoch);

        // Write corrupted data to `current_epoch - 1` wal file.
        let filename = wal_file_name(&sync_wal_path, current_epoch - 1, cfg.epoch_rotate_len);
        let mut sync_it =
            WalIterator::new(&sync_wal_path, current_epoch - 1, cfg.epoch_rotate_len).unwrap();
        sync_it.iterate_batch(|_, _| {}).unwrap();
        let file = OpenOptions::new().write(true).open(filename).unwrap();
        let write_offset = sync_it.offset - 4096 + BATCH_HEADER_SIZE as u64;
        info!(
            "write corrupted data to sync wal offset: {} at epoch: {}",
            write_offset,
            current_epoch - 1
        );
        file.write_all_at(&[0u8; 10], write_offset).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // Rollback MANIFEST to the previous backup one.
        fs::copy(&manifest_filename_bak, &manifest_filename).unwrap();
        // RfEngine can not recover from the corrupted wal file in previous epoch.
        assert!(RfEngine::open(dir_path, &cfg, None, None).is_err());
    }

    fn check_async_wal(
        engine: &RfEngine,
        epoch: u32,
        start_off: u64,
        end_off: u64,
        timeout: Duration,
    ) {
        let ok = try_wait(
            || {
                let (cb, fut) = tikv_util::future::paired_future_callback();
                engine.dump_wal_chunk(epoch, start_off, end_off, cb);
                let chunks = match futures::executor::block_on(fut).unwrap() {
                    Ok((chunks, _)) => chunks,
                    Err(e) => {
                        info!("dump wal chunk failed: {:?}", e);
                        return false;
                    }
                };
                let mut async_it = WalIterator::new_from_chunks(chunks, epoch, 0);
                if let Err(e) = async_it.iterate_batch(|_, _| {
                    // Do nothing but verify checksum.
                }) {
                    // When async WAL has not catch up yet, it will meet the corrupted data again.
                    info!("iterate batch failed: {:?}", e);
                    return false;
                }
                async_it.offset == end_off
            },
            timeout.as_secs() as usize,
        );
        assert!(ok, "check async WAL failed");
    }
}
