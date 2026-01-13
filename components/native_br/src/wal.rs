// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt, io,
    io::{Read, Write},
    ops::Deref,
    os::unix::fs::FileExt,
    sync::Arc,
};

use bytes::{Buf, Bytes, BytesMut};
use rfengine::WalChunkMeta;
use tikv_util::error;

use crate::{
    Result,
    common::{LocalObject, TempLocalObject},
};

/// Data of remote WAL chunk. Resides in memory or local file.
pub enum WalChunkData {
    Memory(Bytes),
    MemoryWithMeta {
        meta: WalChunkMeta,
        bytes: Bytes,
    },
    LocalFile {
        meta: WalChunkMeta,
        local_obj: TempLocalObject,
    },
    LocalFileWithoutMeta(TempLocalObject),
}

impl WalChunkData {
    pub fn in_memory(&self) -> bool {
        matches!(self, Self::Memory(_) | Self::MemoryWithMeta { .. })
    }

    pub fn without_meta(&self) -> bool {
        matches!(self, Self::Memory(_) | Self::LocalFileWithoutMeta { .. })
    }

    pub fn must_get_bytes(&self) -> Bytes {
        match self {
            Self::Memory(bytes) => bytes.clone(),
            Self::MemoryWithMeta { bytes, .. } => bytes.clone(),
            Self::LocalFile { .. } => unreachable!(),
            Self::LocalFileWithoutMeta { .. } => unreachable!(),
        }
    }

    pub fn must_get_bytes_as_data_holder(&self) -> (WalChunkMeta, WalChunkDataHolder) {
        match self {
            Self::Memory(_) => unreachable!(),
            Self::MemoryWithMeta { meta, bytes } => {
                (meta.clone(), WalChunkDataHolder::Memory(bytes.clone()))
            }
            Self::LocalFile { .. } => unreachable!(),
            Self::LocalFileWithoutMeta { .. } => unreachable!(),
        }
    }

    pub fn must_into_local_file(self) -> (WalChunkMeta, WalChunkDataHolder) {
        match self {
            Self::Memory(_) => unreachable!(),
            Self::MemoryWithMeta { .. } => unreachable!(),
            Self::LocalFile { meta, local_obj } => {
                (meta.clone(), WalChunkDataHolder::LocalObject(local_obj))
            }
            Self::LocalFileWithoutMeta { .. } => unreachable!(),
        }
    }

    pub fn must_into_local_file_without_meta(&self) -> LocalObject {
        match self {
            Self::Memory(_) => unreachable!(),
            Self::MemoryWithMeta { .. } => unreachable!(),
            Self::LocalFile { local_obj, .. } => local_obj.clone_inner(),
            Self::LocalFileWithoutMeta(local_obj) => local_obj.clone_inner(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct WalOnlineChunk {
    pub data: Bytes,
    pub start_off: u64, // Handle the case when WAL chunks is empty.
}

impl WalOnlineChunk {
    pub fn end_off(&self) -> u64 {
        self.start_off + self.data.len() as u64
    }
}

/// The WAL data (of an epoch) assembled from WAL chunks and online chunk.
pub enum AssembledWalData {
    BytesMut(BytesMut),
    Bytes(Bytes),
    LocalChunks(LocalWalChunks),
    Local(LocalWal),
}

impl AssembledWalData {
    pub fn len(&self) -> u64 {
        match self {
            Self::BytesMut(bytes_mut) => bytes_mut.len() as u64,
            Self::Bytes(bytes) => bytes.len() as u64,
            Self::LocalChunks(chunks) => chunks.len(),
            Self::Local(wal) => wal.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::BytesMut(bytes_mut) => bytes_mut.is_empty(),
            Self::Bytes(bytes) => bytes.is_empty(),
            Self::LocalChunks(chunks) => chunks.is_empty(),
            Self::Local(wal) => wal.is_empty(),
        }
    }

    pub fn must_get_local_chunks(&self) -> &LocalWalChunks {
        match self {
            Self::BytesMut(_) => unreachable!(),
            Self::Bytes(_) => unreachable!(),
            Self::LocalChunks(chunks) => chunks,
            Self::Local(_) => unreachable!(),
        }
    }

    pub fn freeze(&mut self) {
        match self {
            Self::BytesMut(bytes_mut) => {
                *self = Self::Bytes(std::mem::take(bytes_mut).freeze());
            }
            Self::Bytes(_) => {}
            Self::LocalChunks(chunks) => {
                chunks.close();
            }
            Self::Local(wal) => wal.close(),
        }
    }

    pub fn push_online_chunk(&mut self, online_chunk: WalOnlineChunk) {
        match self {
            Self::BytesMut(bytes) => bytes.extend(online_chunk.data),
            Self::Bytes(_) => unreachable!(),
            Self::LocalChunks(chunks) => {
                debug_assert!(chunks.online_chunk.is_none());
                if let Some(last_end_off) = chunks.end_off() {
                    debug_assert_eq!(last_end_off, online_chunk.start_off);
                }
                chunks.online_chunk.replace(online_chunk);
            }
            Self::Local(_) => unreachable!(), /* Reading WAL from the archive does not check the
                                               * online WAL chunk. */
        }
    }

    pub fn reader(&self) -> Box<dyn Read> {
        match self {
            Self::BytesMut(_) => unreachable!(),
            Self::Bytes(bytes) => Box::new(bytes.clone().reader()),
            Self::LocalChunks(chunks) => Box::new(chunks.reader()),
            Self::Local(wal) => Box::new(wal.reader()),
        }
    }

    pub fn range_reader(&mut self, start: u64, end: u64) -> Result<Box<dyn Read>> {
        debug_assert!(start <= end);
        Ok(match self {
            Self::BytesMut(_) => unreachable!(),
            Self::Bytes(bytes) => {
                // Currently, in memory chunks supports start from 0 only.
                debug_assert_eq!(start, 0);
                debug_assert!(end <= bytes.len() as u64);
                Box::new(bytes.slice(start as usize..end as usize).reader())
            }
            Self::LocalChunks(chunks) => Box::new(chunks.range_reader(start, end)),
            Self::Local(_) => unreachable!(), /* Reading WAL file from the archive always starts
                                               * from the beginning of the file and does not read
                                               * only a partial range in the middle. */
        })
    }
}

// Assemble WAL chunk files into a original WAL file.
pub fn assemble_wal_chunks_to_wal_file(
    mut chunks: Vec<LocalObject>,
) -> rfengine::Result<TempLocalObject> {
    let mut path = chunks.first().unwrap().path.deref().clone();
    let file_name = path.file_name().unwrap().to_string_lossy().to_string();
    path.set_file_name(format!("{}.wal", file_name)); // append suffix
    let mut wal_obj =
        TempLocalObject::create(path).map_err(|e| rfengine::Error::Other(e.to_string()))?;
    for local_object in &mut chunks {
        let mut buf = vec![0; local_object.len as usize];
        local_object
            .file(true)?
            .read_exact_at(buf.as_mut(), 0)
            .map_err(|e| rfengine::Error::Other(e.to_string()))?;
        let chunk_data = rfengine::decompress_wal_chunk(&Bytes::from(buf))
            .map_err(|e| rfengine::Error::Other(e.to_string()))?;
        wal_obj
            .write(&chunk_data)
            .map_err(|e| rfengine::Error::Other(e.to_string()))?;
    }
    wal_obj
        .flush()
        .map_err(|e| rfengine::Error::Other(e.to_string()))?;
    wal_obj.close();
    Ok(wal_obj)
}

pub struct LocalWal {
    local_obj: TempLocalObject,
}

impl LocalWal {
    pub fn new(local_obj: TempLocalObject) -> Self {
        Self { local_obj }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> u64 {
        self.local_obj.len
    }

    pub fn close(&mut self) {
        self.local_obj.close()
    }

    pub fn reader(&self) -> LocalWalReader {
        LocalWalReader {
            local_obj: self.local_obj.clone_inner(),
        }
    }
}

pub struct LocalWalReader {
    local_obj: LocalObject,
}

impl fmt::Debug for LocalWalReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalWalReader")
            .field("len", &self.local_obj.len)
            .finish()
    }
}

impl io::Read for LocalWalReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.local_obj.file(true)?.read(buf).map_err(|e| {
            error!("LocalWalReader: read failed: {:?}", e; "reader" => ?self);
            debug_assert!(false, "err: {:?}", e);
            e
        })
    }
}

pub enum WalChunkDataHolder {
    LocalObject(TempLocalObject),
    Memory(Bytes),
}

impl WalChunkDataHolder {
    fn close(&mut self) {
        match self {
            Self::LocalObject(local_object) => local_object.close(),
            Self::Memory(_) => {}
        }
    }

    fn reader(&self) -> WalChunkDataHolderForReader {
        match self {
            Self::LocalObject(local_object) => {
                WalChunkDataHolderForReader::Local(local_object.clone_inner())
            }
            Self::Memory(bytes) => WalChunkDataHolderForReader::Memory(bytes.clone()),
        }
    }
}

pub enum WalChunkDataHolderForReader {
    Local(LocalObject),
    Memory(Bytes),
}

impl WalChunkDataHolderForReader {
    fn load_chunk_data(&mut self) -> io::Result<Bytes> {
        match self {
            Self::Local(local_object) => {
                let mut buf = vec![0; local_object.len as usize];
                local_object.file(true)?.read_exact_at(buf.as_mut(), 0)?;
                rfengine::decompress_wal_chunk(&Bytes::from(buf)).map_err(|e| io::Error::other(e))
            }
            Self::Memory(bytes) => {
                rfengine::decompress_wal_chunk(bytes).map_err(|e| io::Error::other(e))
            }
        }
    }

    fn close(&mut self) {
        match self {
            Self::Local(local_object) => local_object.close(),
            Self::Memory(_) => {}
        }
    }
}

pub struct LocalWalChunks {
    metas: Arc<Vec<WalChunkMeta>>,
    data_holders: Vec<WalChunkDataHolder>,
    online_chunk: Option<WalOnlineChunk>,
}

impl LocalWalChunks {
    pub fn new(chunks: Vec<(WalChunkMeta, WalChunkDataHolder)>) -> Self {
        let (metas, data_holders) = chunks.into_iter().unzip();
        Self {
            metas: Arc::new(metas),
            data_holders,
            online_chunk: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.metas.is_empty() && self.online_chunk.is_none()
    }

    pub fn len(&self) -> u64 {
        if self.is_empty() {
            return 0;
        }
        self.end_off().unwrap() - self.start_off().unwrap()
    }

    pub fn start_off(&self) -> Option<u64> {
        self.metas
            .first()
            .map(|x| x.start_off)
            .or_else(|| self.online_chunk.as_ref().map(|x| x.start_off))
    }

    pub fn end_off(&self) -> Option<u64> {
        self.online_chunk
            .as_ref()
            .map(|x| x.end_off())
            .or_else(|| self.metas.last().map(|x| x.end_off))
    }

    pub fn close(&mut self) {
        for holder in &mut self.data_holders {
            holder.close();
        }
    }

    pub fn reader(&self) -> LocalWalChunksReader {
        LocalWalChunksReader {
            chunks: self.into(),
            read_off: self.start_off().unwrap_or(0),
            end_off: self.end_off().unwrap_or(0),
            current_chunk_idx: None,
            current_chunk: None,
        }
    }

    pub fn range_reader(&self, start: u64, end: u64) -> LocalWalChunksReader {
        debug_assert!(start <= end);
        debug_assert!(self.start_off().is_none_or(|s| s <= start));
        debug_assert!(self.end_off().is_none_or(|e| end <= e));

        LocalWalChunksReader {
            chunks: self.into(),
            read_off: start,
            end_off: end,
            current_chunk_idx: None,
            current_chunk: None,
        }
    }

    pub fn has_last_chunk(&self) -> bool {
        self.online_chunk.is_none() && self.metas.last().is_some_and(|x| x.last)
    }
}

struct LocalWalChunksForReader {
    metas: Arc<Vec<WalChunkMeta>>,
    data_holders: Vec<WalChunkDataHolderForReader>,
    online_chunk: Option<WalOnlineChunk>,
}

impl From<&LocalWalChunks> for LocalWalChunksForReader {
    fn from(chunks: &LocalWalChunks) -> Self {
        Self {
            metas: chunks.metas.clone(),
            data_holders: chunks.data_holders.iter().map(|x| x.reader()).collect(),
            online_chunk: chunks.online_chunk.clone(),
        }
    }
}

impl LocalWalChunksForReader {
    fn is_empty(&self) -> bool {
        self.metas.is_empty() && self.online_chunk.is_none()
    }
}

struct CurrentChunk {
    start_off: u64,
    data: Bytes,
}

impl fmt::Debug for CurrentChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CurrentChunk")
            .field("start_off", &self.start_off)
            .field("end_off", &self.end_off())
            .finish()
    }
}

impl CurrentChunk {
    fn end_off(&self) -> u64 {
        self.start_off + self.data.len() as u64
    }
}

pub struct LocalWalChunksReader {
    chunks: LocalWalChunksForReader,
    read_off: u64,
    end_off: u64,

    // The index of `chunks`. Equals to `chunks.metas.len()` means the online chunk.
    current_chunk_idx: Option<usize>,
    current_chunk: Option<CurrentChunk>,
}

impl fmt::Debug for LocalWalChunksReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalWalChunksReader")
            .field("read_off", &self.read_off)
            .field("end_off", &self.end_off)
            .field("current_chunk_idx", &self.current_chunk_idx)
            .field("current_chunk", &self.current_chunk)
            .finish()
    }
}

impl io::Read for LocalWalChunksReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_impl(buf).map_err(|e| {
            error!("LocalWalChunksReader: read failed: {:?}", e; "reader" => ?self);
            debug_assert!(false, "err: {:?}", e);
            e
        })
    }
}

impl LocalWalChunksReader {
    fn seek_chunk(&self, offset: u64) -> usize /* index */ {
        use std::cmp::Ordering;

        self.chunks
            .metas
            .binary_search_by(|meta| {
                if meta.end_off <= offset {
                    Ordering::Less
                } else if offset < meta.start_off {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            })
            .unwrap_or_else(|x| {
                debug_assert_eq!(
                    x,
                    self.chunks.metas.len(),
                    "search offset {} on metas {:?}",
                    offset,
                    self.chunks.metas
                );
                x
            })
    }

    fn load_chunk(&mut self, idx: usize) -> io::Result<()> {
        debug_assert!(idx <= self.chunks.metas.len());

        let chunk = if idx < self.chunks.metas.len() {
            let meta = &self.chunks.metas[idx];
            let data_holder = &mut self.chunks.data_holders[idx];
            let data = data_holder.load_chunk_data()?;
            Some(CurrentChunk {
                start_off: meta.start_off,
                data,
            })
        } else if let Some(online_chunk) = &self.chunks.online_chunk {
            Some(CurrentChunk {
                start_off: online_chunk.start_off,
                data: online_chunk.data.clone(),
            })
        } else {
            debug_assert!(false);
            None
        };
        self.current_chunk = chunk;
        Ok(())
    }

    fn read_current_chunk(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let Some(current_chunk) = self.current_chunk.as_ref() else {
            debug_assert!(false);
            return Ok(0);
        };

        debug_assert!(self.read_off >= current_chunk.start_off);
        let limit = buf.len();
        let end_off = (self.read_off + limit as u64)
            .min(current_chunk.end_off())
            .min(self.end_off);

        if self.read_off >= end_off {
            debug_assert!(false);
            return Ok(0);
        }

        let data = current_chunk.data.slice(
            (self.read_off - current_chunk.start_off) as usize
                ..(end_off - current_chunk.start_off) as usize,
        );
        let n = data.len();
        buf[..n].copy_from_slice(&data);

        self.read_off += n as u64;
        if self.read_off >= current_chunk.end_off() {
            debug_assert_eq!(self.read_off, current_chunk.end_off());
            self.current_chunk = None;
            let current_chunk_idx = self.current_chunk_idx.as_mut().unwrap();
            if let Some(holder) = self.chunks.data_holders.get_mut(*current_chunk_idx) {
                holder.close();
            }
            *current_chunk_idx += 1;
        }
        Ok(n)
    }

    fn read_impl(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.chunks.is_empty() {
            return Ok(0);
        }

        if self.read_off >= self.end_off {
            debug_assert_eq!(self.read_off, self.end_off);
            return Ok(0);
        }

        let current_chunk_idx = match self.current_chunk_idx {
            Some(idx) => idx,
            None => {
                let idx = self.seek_chunk(self.read_off);
                self.current_chunk_idx = Some(idx);
                idx
            }
        };
        if self.current_chunk.is_none() {
            self.load_chunk(current_chunk_idx)?;
        }

        self.read_current_chunk(buf)
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, path::Path};

    use bytes::{Bytes, BytesMut};
    use proptest::{prelude::*, test_runner::TestCaseResult};
    use rand::prelude::*;
    use rfengine::{ChunkHeader, CompressionType};
    use tempdir::TempDir;

    use super::*;

    const WAL_CHUNK_TARGET_SIZE: usize = 32;

    #[test]
    fn test_local_wal_chunks() {
        proptest!(|(arb in arb_wal_chunks(), local_obj in any::<bool>())| {
            test_local_wal_chunks_impl(arb, local_obj)?;
        });
    }

    fn test_local_wal_chunks_impl(arb: ArbWalChunks, local_obj: bool) -> TestCaseResult {
        // println!("{:?}", arb);
        let dir = TempDir::new("test_local_wal_chunks").unwrap();
        let assembled = arb.assembled.clone();
        let local_wal_chunks = if local_obj {
            make_local_wal_chunks(arb, dir.path())
        } else {
            make_memory_wal_chunks(arb)
        };

        {
            let mut reader = local_wal_chunks.reader();
            let mut data = Vec::with_capacity(assembled.len());
            reader.read_to_end(&mut data).unwrap();
            prop_assert_eq!(&assembled, &data);
        }

        proptest!(|(start_end in arb_range(assembled.len()))| {
            // println!("{:?}", start_end);
            let (start, end) = start_end;
            let mut reader = local_wal_chunks.range_reader(start as u64, end as u64);
            let mut data = Vec::with_capacity(end - start);
            reader.read_to_end(&mut data).unwrap();
            prop_assert_eq!(assembled.slice(start..end), data);
        });

        Ok(())
    }

    #[derive(Debug)]
    struct ArbWalChunks {
        assembled: Bytes,
        metas: Vec<WalChunkMeta>,
        chunks: Vec<Bytes>,
        online_chunk: Option<WalOnlineChunk>,
    }

    fn arb_wal_chunks() -> impl Strategy<Value = ArbWalChunks> {
        (0..=WAL_CHUNK_TARGET_SIZE * 8, 0..=WAL_CHUNK_TARGET_SIZE).prop_map(
            move |(wal_size, online_chunk_size)| {
                let mut rng = thread_rng();

                let mut wal = BytesMut::with_capacity(wal_size + online_chunk_size);
                let mut metas = vec![];
                let mut chunks = vec![];

                let mut start = 0;
                while start < wal_size {
                    let chunk_size = std::cmp::min(WAL_CHUNK_TARGET_SIZE, wal_size - start);
                    let mut chunk_data = vec![0u8; chunk_size];
                    rng.fill_bytes(&mut chunk_data);
                    wal.extend_from_slice(&chunk_data);

                    let meta = WalChunkMeta {
                        key: "".to_string(),
                        epoch: 1,
                        start_off: start as u64,
                        end_off: (start + chunk_size) as u64,
                        last: start + chunk_size >= wal_size,
                    };
                    metas.push(meta);

                    let chunk_header = ChunkHeader::new(CompressionType::Lz4Compression);
                    let mut chunk = Vec::with_capacity(ChunkHeader::len() + chunk_size);
                    chunk_header.encode_to(&mut chunk);
                    rfengine::compress_lz4(&chunk_data, &mut chunk).unwrap();
                    chunks.push(Bytes::from(chunk));

                    start += chunk_size;
                }

                let online_chunk = (online_chunk_size > 0).then(|| {
                    let mut data = vec![0u8; online_chunk_size];
                    rng.fill_bytes(&mut data);
                    wal.extend_from_slice(&data);
                    WalOnlineChunk {
                        data: Bytes::from(data),
                        start_off: wal_size as u64,
                    }
                });

                assert_eq!(wal.len(), wal_size + online_chunk_size);

                ArbWalChunks {
                    assembled: wal.freeze(),
                    metas,
                    chunks,
                    online_chunk,
                }
            },
        )
    }

    fn make_local_wal_chunks(arb: ArbWalChunks, dir: &Path) -> LocalWalChunks {
        let mut data_holders = vec![];

        let ArbWalChunks {
            metas,
            chunks,
            online_chunk,
            ..
        } = arb;

        for (idx, chunk) in chunks.into_iter().enumerate() {
            let path = dir.join(format!("chunk.{idx}"));
            let mut obj = TempLocalObject::create(path).unwrap();
            obj.write_all(&chunk).unwrap();
            obj.close();
            data_holders.push(WalChunkDataHolder::LocalObject(obj));
        }

        LocalWalChunks {
            metas: Arc::new(metas),
            data_holders,
            online_chunk,
        }
    }

    fn make_memory_wal_chunks(arb: ArbWalChunks) -> LocalWalChunks {
        let mut data_holders = vec![];

        let ArbWalChunks {
            metas,
            chunks,
            online_chunk,
            ..
        } = arb;

        for chunk in chunks {
            data_holders.push(WalChunkDataHolder::Memory(chunk));
        }

        LocalWalChunks {
            metas: Arc::new(metas),
            data_holders,
            online_chunk,
        }
    }

    fn arb_range(max: usize) -> impl Strategy<Value = (usize, usize)> {
        (0..=max)
            .prop_flat_map(move |start| (Just(start), start..=max))
            .prop_map(|(start, end)| (start, end))
    }
}
