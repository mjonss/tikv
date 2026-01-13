// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs,
    io::{BufReader, Read},
    path::Path,
};

use bytes::{Buf, Bytes, BytesMut};
use tikv_util::error;

use crate::{
    Error, Result, WriteBatch,
    compact_worker::wal_file_name,
    write_batch::PeerBatch,
    writer::{BATCH_HEADER_SIZE, DmaBuffer, WalHeader},
};

pub struct WalIterator<R: Read> {
    epoch_id: u32,
    buf: BytesMut,
    pub(crate) offset: u64,
    reader: R,
}

pub(crate) const MAX_BATCH_SIZE: usize = 256 * 1024 * 1024;

impl WalIterator<BufReader<fs::File>> {
    pub(crate) fn new(dir: &Path, epoch_id: u32, epoch_rotate_len: usize) -> std::io::Result<Self> {
        let filename = wal_file_name(dir, epoch_id, epoch_rotate_len);
        let fd = fs::File::open(filename)?;
        Ok(Self::new_from_reader(BufReader::new(fd), epoch_id, 0))
    }
}

impl<B: bytes::Buf> WalIterator<bytes::buf::Reader<B>> {
    pub fn new_from_chunks(file_data: B, epoch_id: u32, offset: u64) -> Self {
        Self::new_from_reader(file_data.reader(), epoch_id, offset)
    }
}

impl<R: Read> WalIterator<R> {
    pub fn new_from_reader(reader: R, epoch_id: u32, offset: u64) -> Self {
        Self {
            epoch_id,
            buf: BytesMut::new(),
            offset,
            reader,
        }
    }

    pub(crate) fn iterate_batch<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(Bytes, u64),
    {
        if self.offset == 0 {
            match self.check_wal_header() {
                Ok(()) => {}
                Err(Error::Eof) => {
                    return Ok(());
                }
                Err(e) => return Err(e),
            };
        }
        loop {
            match self.read_batch() {
                Err(err) => {
                    if let Error::Eof = err {
                        return Ok(());
                    }
                    return Err(err);
                }
                Ok(data) => {
                    if data.is_empty() {
                        return Ok(());
                    }
                    f(data, self.offset);
                }
            }
        }
    }

    pub fn iterate_write_batch<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(WriteBatch),
    {
        self.iterate_batch(|data, _| {
            let mut wb = WriteBatch::new();
            iterate_peer_batch(data, |peer_batch| {
                wb.peers.insert(peer_batch.peer_id, peer_batch);
            });
            f(wb);
        })
    }

    pub(crate) fn check_wal_header(&mut self) -> Result<()> {
        let mut buf = [0u8; WalHeader::len()];
        self.reader.read_exact(&mut buf)?;
        self.offset += WalHeader::len() as u64;
        match WalHeader::decode(&buf) {
            Ok(header) => {
                if header.epoch_id != self.epoch_id {
                    return Err(Error::Corruption {
                        msg: format!(
                            "check wal header: epoch mismatch: header.epoch_id {} != self.epoch_id {}",
                            header.epoch_id, self.epoch_id
                        ),
                        epoch_id: header.epoch_id,
                        offset: 0,
                        data: buf.to_vec(),
                    });
                }
                Ok(())
            }
            Err(err) => {
                // Haven't written the header.
                if buf.iter().all(|v| *v == 0) {
                    return Err(Error::Eof);
                }
                // Header is corrupt, but the first batch header is empty which means there
                // is no data in this WAL. Treat it like EOF and WAL writer will rewrite the
                // header.
                self.reader.read_exact(&mut buf[..BATCH_HEADER_SIZE])?;
                if buf.iter().take(BATCH_HEADER_SIZE).all(|v| *v == 0) {
                    return Err(Error::Eof);
                }
                // Header corruption.
                Err(err)
            }
        }
    }

    pub(crate) fn read_batch(&mut self) -> Result<Bytes> {
        let mut header_array = [0u8; BATCH_HEADER_SIZE];
        self.reader.read_exact(header_array.as_mut_slice())?;
        let mut header_buf = header_array.as_slice();
        let epoch_id = header_buf.get_u32_le();
        let checksum = header_buf.get_u32_le();
        let length = header_buf.get_u32_le() as usize;
        if epoch_id == 0 && checksum == 0 && length == 0 {
            return Err(Error::Eof);
        }
        if epoch_id != self.epoch_id {
            return Err(Error::Corruption {
                msg: format!(
                    "read batch: epoch mismatch: header.epoch_id {} != self.epoch_id {}",
                    epoch_id, self.epoch_id
                ),
                epoch_id,
                offset: self.offset,
                data: header_array.to_vec(),
            });
        }
        if length > MAX_BATCH_SIZE {
            return Err(Error::Corruption {
                msg: format!("length mismatch: length {}", length),
                epoch_id,
                offset: self.offset,
                data: header_array.to_vec(),
            });
        }
        let aligned_length = DmaBuffer::aligned_len(BATCH_HEADER_SIZE + length);
        let remained_length = aligned_length - BATCH_HEADER_SIZE;
        self.buf.resize(remained_length, 0);
        self.reader.read_exact(&mut self.buf[..])?;
        let batch = &self.buf[..length];
        let actual_checksum = crc32c::crc32c(batch);
        if checksum != actual_checksum {
            error!("read_batch:checksum mismatch";
                "epoch_id" => epoch_id,
                "checksum" => checksum,
                "actual_checksum" => actual_checksum,
                "length" => length,
                "aligned_length" => aligned_length,
                "remained_length" => remained_length,
                "self.offset" => self.offset,
                "header" => log_wrappers::hex_encode_upper(header_array),
                "batch" => log_wrappers::hex_encode_upper(batch),
            );
            return Err(Error::Corruption {
                msg: format!(
                    "checksum mismatch: header.checksum {:x}, batch.checksum {:x}",
                    checksum, actual_checksum
                ),
                epoch_id,
                offset: self.offset,
                data: batch.to_vec(),
            });
        }
        self.offset += aligned_length as u64;
        let (mut compression_type, batch_data) = batch.split_at(4);
        let compression = compression_type.get_u32_le() > 0;
        if compression {
            let dst = lz4::block::decompress(batch_data, None)?;
            Ok(Bytes::from(dst))
        } else {
            Ok(Bytes::from(batch_data.to_vec()))
        }
    }
}

pub(crate) fn iterate_peer_batch(data: Bytes, mut f: impl FnMut(PeerBatch)) {
    let mut batch = data.chunk();
    while !batch.is_empty() {
        let peer_data = PeerBatch::decode(batch);
        batch = &batch[peer_data.encoded_len()..];
        f(peer_data);
    }
}
