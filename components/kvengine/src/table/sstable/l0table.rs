// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{ops::Deref, sync::Arc};

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use cloud_encryption::EncryptionKey;
use kvenginepb::L0Create;

use super::*;
use crate::{
    NUM_CFS, WRITE_CF, max_ts_by_cf,
    table::{
        BoundedDataSet, ChecksumType, DataBound, Error, InnerKey, NO_COMPRESSION, SnapVersion,
        Value, blobtable::BlobRef, file::File, table::Result,
    },
    util::new_l0_create_pb,
};

const L0_FOOTER_SIZE: usize = std::mem::size_of::<L0Footer>();

#[derive(Default, Clone)]
pub struct L0Footer {
    snap_version: SnapVersion,
    num_cfs: u32,
    magic: u32,
}

impl L0Footer {
    pub fn unmarshal(&mut self, bin: &[u8]) {
        self.snap_version = LittleEndian::read_u64(bin).into();
        self.num_cfs = LittleEndian::read_u32(&bin[8..]);
        self.magic = LittleEndian::read_u32(&bin[12..]);
    }

    pub fn is_match(&self) -> bool {
        self.magic == MAGIC_NUMBER || self.magic == MAGIC_NUMBER_SPLIT_L0
    }
}

#[derive(Clone)]
pub struct L0Table {
    core: Arc<L0TableCore>,
}

impl Deref for L0Table {
    type Target = L0TableCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl L0Table {
    /// Would return `None` only when `write_cf_only` is `true`.
    pub fn new(
        file: Arc<dyn File>,
        cache: BlockCache,
        write_cf_only: bool,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Option<Self>> {
        let core = L0TableCore::new(file, cache, write_cf_only, encryption_key)?;
        Ok(core.map(|core| Self {
            core: Arc::new(core),
        }))
    }

    pub const fn footer_size() -> usize {
        L0_FOOTER_SIZE
    }
}

pub struct L0TableCore {
    footer: L0Footer,
    file: Arc<dyn File>,
    cfs: [Option<sstable::SsTable>; NUM_CFS],
    max_ts: u64,
    entries: u64,
    kv_size: u64,
    smallest: Bytes,
    biggest: Bytes,
    total_blob_size: u64,
}

impl L0TableCore {
    pub fn new(
        file: Arc<dyn File>,
        cache: BlockCache,
        write_cf_only: bool,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Option<Self>> {
        let footer_off = file.size() - L0_FOOTER_SIZE as u64;
        let mut footer = L0Footer::default();
        let footer_buf = file.read_footer(L0_FOOTER_SIZE)?;
        footer.unmarshal(footer_buf.chunk());
        if footer.magic == MAGIC_NUMBER_SPLIT_L0 {
            return Self::new_write_cf_l0(file, cache, encryption_key);
        } else if footer.magic != MAGIC_NUMBER {
            return Err(Error::InvalidMagicNumber);
        }
        let cf_offs_off = footer_off - 4 * NUM_CFS as u64;
        let cf_offs_buf = file.read(cf_offs_off, 4 * NUM_CFS)?;
        let mut cf_offs = [0u32; NUM_CFS];
        for i in 0..NUM_CFS {
            cf_offs[i] = LittleEndian::read_u32(&cf_offs_buf[i * 4..]);
        }
        let mut cfs: [Option<SsTable>; NUM_CFS] = [None, None, None];
        let mut entries = 0;
        let mut kv_size = 0;
        for i in 0..NUM_CFS {
            let start_off = cf_offs[i] as u64;
            let mut end_off = cf_offs_off;
            if i + 1 < NUM_CFS {
                end_off = cf_offs[i + 1] as u64;
            }
            if start_off == end_off || write_cf_only && i != WRITE_CF {
                continue;
            }
            let tbl = sstable::SsTable::new_l0_cf(
                file.clone(),
                start_off,
                end_off,
                cache.clone(),
                encryption_key.clone(),
            )?;
            entries += tbl.entries as u64;
            if i == WRITE_CF {
                kv_size += tbl.kv_size;
            }

            cfs[i] = Some(tbl)
        }

        if cfs.iter().all(|t| t.is_none()) {
            // All CFs are empty only when `write_cf_only` is `true` and `WRITE_CF` is
            // empty.
            debug_assert!(write_cf_only);
            return Ok(None);
        }

        let (smallest, biggest, max_ts) = Self::compute_smallest_biggest(&cfs);
        let total_blob_size = Self::compute_total_blob_size(&cfs);
        Ok(Some(Self {
            footer,
            file,
            cfs,
            max_ts,
            entries,
            kv_size,
            smallest,
            biggest,
            total_blob_size,
        }))
    }

    fn new_write_cf_l0(
        file: Arc<dyn File>,
        cache: BlockCache,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Option<Self>> {
        let tbl = SsTable::new(file.clone(), cache, encryption_key)?;
        let entries = tbl.entries as u64;
        let kv_size = tbl.kv_size;
        let mut footer = L0Footer::default();
        footer.num_cfs = NUM_CFS as u32;
        footer.magic = MAGIC_NUMBER_SPLIT_L0;
        footer.snap_version = tbl.snap_version;
        let smallest = tbl.clone_smallest();
        let biggest = tbl.clone_biggest();
        let max_ts = tbl.max_ts;
        let total_blob_size = tbl.total_blob_size();
        Ok(Some(Self {
            footer,
            file,
            cfs: [Some(tbl), None, None],
            max_ts,
            entries,
            kv_size,
            smallest,
            biggest,
            total_blob_size,
        }))
    }

    // Return: smallest, biggest, max_ts
    fn compute_smallest_biggest(cfs: &[Option<SsTable>; NUM_CFS]) -> (Bytes, Bytes, u64) {
        let mut smallest_buf = BytesMut::new();
        let mut biggest_buf = BytesMut::new();
        let mut max_ts = 0;
        for i in 0..NUM_CFS {
            if let Some(cf_tbl) = &cfs[i] {
                let smallest = cf_tbl.smallest();
                if !smallest.is_empty()
                    && (smallest_buf.is_empty() || smallest_buf.chunk() > smallest.deref())
                {
                    smallest_buf.truncate(0);
                    smallest_buf.extend_from_slice(smallest.deref());
                }
                let biggest = cf_tbl.biggest();
                if biggest.deref() > biggest_buf.chunk() {
                    biggest_buf.truncate(0);
                    biggest_buf.extend_from_slice(biggest.deref());
                }
                max_ts = max_ts_by_cf(max_ts, i, cf_tbl.max_ts);
            }
        }
        assert!(!smallest_buf.is_empty());
        assert!(!biggest_buf.is_empty());
        (smallest_buf.freeze(), biggest_buf.freeze(), max_ts)
    }

    fn compute_total_blob_size(cfs: &[Option<SsTable>; NUM_CFS]) -> u64 {
        let mut total_blob_size = 0;
        for i in 0..NUM_CFS {
            if let Some(cf_tbl) = &cfs[i] {
                total_blob_size += cf_tbl.total_blob_size();
            }
        }
        total_blob_size
    }

    pub fn id(&self) -> u64 {
        self.file.id()
    }

    pub fn get_cf(&self, cf: usize) -> &Option<sstable::SsTable> {
        &self.cfs[cf]
    }

    pub fn size(&self) -> u64 {
        self.file.size()
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    pub fn entries(&self) -> u64 {
        self.entries
    }

    pub fn tombs(&self) -> u64 {
        self.cfs[WRITE_CF].as_ref().map_or(0, |t| t.tombs as u64)
    }

    pub fn entries_write_cf(&self) -> u64 {
        self.cfs[WRITE_CF].as_ref().map_or(0, |t| t.entries as u64)
    }

    pub fn kv_size(&self) -> u64 {
        self.kv_size
    }

    pub fn smallest(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(self.smallest.chunk())
    }

    pub fn biggest(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(self.biggest.chunk())
    }

    pub fn snap_version(&self) -> SnapVersion {
        self.footer.snap_version
    }

    pub fn has_data_in_bound(&self, bound: DataBound<'_>) -> bool {
        if !self.data_bound().overlap_bound(bound) {
            return false;
        }
        self.cfs
            .iter()
            .filter_map(|t| t.as_ref())
            .any(|t| t.has_overlap(bound))
    }

    pub fn total_blob_size(&self) -> u64 {
        self.total_blob_size
    }

    pub fn is_write_cf_only(&self) -> bool {
        self.footer.magic == MAGIC_NUMBER_SPLIT_L0
    }

    pub fn to_l0_create(&self) -> L0Create {
        let mut l0_create = L0Create::new();
        l0_create.set_id(self.id());
        l0_create.set_smallest(self.smallest().to_vec());
        l0_create.set_biggest(self.biggest().to_vec());
        l0_create.set_size(self.size() as u32);
        l0_create
    }
}

impl BoundedDataSet for L0TableCore {
    fn data_bound(&self) -> DataBound<'_> {
        DataBound::new(self.smallest(), self.biggest(), true)
    }
}

pub struct L0Builder {
    builders: Vec<Builder>,
    version: SnapVersion,
    count: usize,
    fid: u64,
}

impl L0Builder {
    pub fn new(
        fid: u64,
        block_size: usize,
        version: SnapVersion,
        checksum_type: ChecksumType,
        encryption_key: Option<EncryptionKey>,
    ) -> Self {
        let mut builders = Vec::with_capacity(4);
        for _ in 0..NUM_CFS {
            let builder = Builder::new(
                fid,
                block_size,
                NO_COMPRESSION,
                0,
                checksum_type,
                encryption_key.clone(),
            );
            builders.push(builder);
        }
        Self {
            builders,
            version,
            count: 0,
            fid,
        }
    }

    pub fn add(
        &mut self,
        cf: usize,
        key: InnerKey<'_>,
        val: &Value,
        external_link: Option<BlobRef>,
    ) {
        self.builders[cf].add(key, val, external_link);
        self.count += 1;
    }

    pub fn finish(&mut self) -> (L0Create, Bytes) {
        let mut estimated_size = 0;
        for builder in &self.builders {
            estimated_size += builder.estimated_size();
        }
        let mut buf = Vec::with_capacity(estimated_size);
        let mut offsets = Vec::with_capacity(NUM_CFS);
        for builder in &mut self.builders {
            let offset = buf.len() as u32;
            offsets.push(offset);
            if !builder.is_empty() {
                builder.finish(offset, &mut buf);
            }
        }
        for offset in offsets {
            buf.put_u32_le(offset);
        }
        buf.put_u64_le(self.version.into_inner());
        buf.put_u32_le(NUM_CFS as u32);
        buf.put_u32_le(MAGIC_NUMBER);
        let (smallest, biggest) = self.smallest_biggest();
        let l0_create = new_l0_create_pb(self.fid, smallest, biggest, buf.len() as u32);
        (l0_create, buf.into())
    }

    fn smallest_biggest(&self) -> (Vec<u8>, Vec<u8>) {
        let mut smallest_buf = vec![];
        let mut biggest_buf = vec![];
        for builder in &self.builders {
            if !builder.get_smallest().is_empty()
                && (smallest_buf.is_empty() || builder.get_smallest() < smallest_buf.as_slice())
            {
                smallest_buf.truncate(0);
                smallest_buf.extend_from_slice(builder.get_smallest());
            }
            if builder.get_biggest() > biggest_buf.as_slice() {
                biggest_buf.truncate(0);
                biggest_buf.extend_from_slice(builder.get_biggest());
            }
        }
        (smallest_buf, biggest_buf)
    }

    pub fn total_blob_size(&self) -> u64 {
        let mut total_blob_size = 0;
        for builder in &self.builders {
            total_blob_size += builder.get_total_blob_size();
        }
        total_blob_size
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn get_fid(&self) -> u64 {
        self.fid
    }
}
