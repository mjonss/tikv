// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt, mem, ops::Deref, sync::Arc};

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, Bytes, BytesMut};
use log_wrappers::Value as LogValue;

use super::SsTable;
use crate::table::{
    InnerKey, Iterator, LocalAddr, search,
    sstable::{BLOCK_FORMAT_V1, Index},
    table::{self, VALUE_VERSION_LEN, Value, is_old_version},
};

#[derive(Default)]
pub struct BlockIterator {
    num_entries: usize,
    b: Bytes,
    idx: i32,
    err: Option<table::Error>,

    // current entry fields.
    diff_key_addr: LocalAddr,
    meta: u8,
    user_meta_len: u8,
    ver: u64,
    old_ver: u64,
    val_addr: LocalAddr,

    // entry index fields.
    entry_offs: Bytes,
    common_prefix_addr: LocalAddr,
    entries_data_addr: LocalAddr,
}

impl fmt::Debug for BlockIterator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockIterator")
            .field("num_entries", &self.num_entries)
            .field("b", &LogValue::value(&self.b))
            .field("idx", &self.idx)
            .field("err", &self.err)
            .field("diff_key_addr", &self.diff_key_addr)
            .field("ver", &self.ver)
            .field("old_ver", &self.old_ver)
            .field("val_addr", &self.val_addr)
            .field("entry_offs", &self.entry_offs)
            .field("common_prefix_addr", &self.common_prefix_addr)
            .field("entries_data_addr", &self.entries_data_addr)
            .finish()
    }
}

impl BlockIterator {
    fn set_block(&mut self, b: Bytes) {
        self.b = b;
        self.err = None;
        self.idx = 0;
        self.reset_current_entry();
        self.load_entries();
    }

    fn reset_current_entry(&mut self) {
        self.diff_key_addr = LocalAddr::default();
        self.meta = 0;
        self.ver = 0;
        self.old_ver = 0;
        self.user_meta_len = 0;
        self.val_addr = LocalAddr::default();
    }

    fn load_entries(&mut self) {
        let mut data = self.b.clone();
        assert_eq!(data.get_u32_le(), BLOCK_FORMAT_V1);
        self.num_entries = data.get_u32_le() as usize;
        self.entry_offs = data.slice(..self.num_entries * mem::size_of::<u32>());
        data.advance(self.entry_offs.len());
        let common_prefix_len = data.get_u16_le() as usize;
        let common_prefix_off = self.b.len() - data.len();
        let entries_data_off = common_prefix_off + common_prefix_len;
        self.common_prefix_addr = LocalAddr::new(common_prefix_off, entries_data_off);
        self.entries_data_addr = LocalAddr::new(entries_data_off, self.b.len());
    }

    fn num_entries(&self) -> usize {
        self.num_entries
    }

    fn get_entry_off(&self, i: usize) -> usize {
        (&self.entry_offs[i * mem::size_of::<u32>()..]).get_u32_le() as usize
    }

    fn get_common_prefix(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(self.common_prefix_addr.get(self.b.chunk()))
    }

    fn get_diff_key(&self) -> &[u8] {
        self.diff_key_addr.get(self.b.chunk())
    }

    fn seek(&mut self, key: InnerKey<'_>) {
        let common_prefix = self.get_common_prefix();
        if key.len() <= common_prefix.len() {
            if key <= common_prefix {
                self.set_idx(0);
            } else {
                self.set_idx(self.num_entries() as i32);
            }
            return;
        }
        use std::cmp::Ordering::*;
        match key.slice(0, common_prefix.len()).cmp(&common_prefix) {
            Less => {
                self.set_idx(0);
                return;
            }
            Greater => {
                self.set_idx(self.num_entries() as i32);
                return;
            }
            Equal => {}
        };
        let diff_key = &key[common_prefix.len()..];
        let found_idx = search(self.num_entries(), |i| {
            self.set_idx(i as i32);
            self.get_diff_key() >= diff_key
        });
        self.set_idx(found_idx as i32);
    }

    fn seek_to_first(&mut self) {
        self.set_idx(0);
    }

    fn seek_to_last(&mut self) {
        self.set_idx(self.num_entries() as i32 - 1);
    }

    fn get_entry_addr(&self, i: usize) -> LocalAddr {
        let addr = self.entries_data_addr;
        let start = addr.start + self.get_entry_off(i);
        if i + 1 < self.num_entries() {
            let end = addr.start + self.get_entry_off(i + 1);
            return LocalAddr::new(start, end);
        }
        LocalAddr::new(start, addr.end)
    }

    fn set_idx(&mut self, i: i32) {
        self.idx = i;
        if i >= self.num_entries() as i32 || i < 0 {
            self.err = Some(table::Error::Eof);
            return;
        }
        self.err = None;
        let cur_entry_addr = self.get_entry_addr(i as usize);
        let entry_data = cur_entry_addr.get(self.b.chunk());

        let diff_key_len = LittleEndian::read_u16(entry_data) as usize;
        let diff_key_start = cur_entry_addr.start + 2;
        self.diff_key_addr = LocalAddr::new(diff_key_start, diff_key_start + diff_key_len);
        let mut field_off = 2 + diff_key_len;
        self.meta = entry_data[field_off];
        field_off += 1;
        {
            let version = Value::get_version(&entry_data[field_off..]);
            self.ver = version;
            field_off += VALUE_VERSION_LEN;
        }
        if is_old_version(self.meta) {
            let version = Value::get_version(&entry_data[field_off..]);
            self.old_ver = version;
            field_off += VALUE_VERSION_LEN;
        } else {
            self.old_ver = 0;
        }
        self.user_meta_len = entry_data[field_off];
        field_off += 1;
        let val_start = cur_entry_addr.start + field_off;
        let val_end = cur_entry_addr.end;
        self.val_addr = LocalAddr::new(val_start, val_end);
    }

    fn next(&mut self) {
        self.set_idx(self.idx + 1);
    }

    fn prev(&mut self) {
        self.set_idx(self.idx - 1);
    }
}

#[derive(PartialEq, Debug)]
enum IterState {
    NewVersion,
    OldVersion,
    OldVersioDone,
}

pub struct TableIterator {
    t: SsTable,
    idx: Arc<Index>,
    old_idx: Option<Arc<Index>>,
    b_pos: i32,
    bi: BlockIterator,
    old_b_pos: i32,
    old_bi: BlockIterator,
    reversed: bool,
    fill_cache: bool,
    err: Option<table::Error>,
    key_buf: BytesMut,
    iter_state: IterState,
    block_buf: Vec<u8>,
    decryption_buf: Vec<u8>,
}

impl fmt::Debug for TableIterator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableIterator")
            .field("t", &self.t)
            .field("b_pos", &self.b_pos)
            .field("bi", &self.bi)
            .field("old_b_pos", &self.old_b_pos)
            .field("old_bi", &self.old_bi)
            .field("reversed", &self.reversed)
            .field("err", &self.err)
            .field("key_buf", &self.key_buf)
            .field("iter_state", &self.iter_state)
            .field("is_sync", &self.t.is_sync())
            .finish()
    }
}

impl TableIterator {
    pub fn new(t: SsTable, reversed: bool, fill_cache: bool) -> Self {
        let idx = t.load_index();
        Self {
            t,
            idx,
            old_idx: None,
            b_pos: 0,
            bi: BlockIterator::default(),
            old_b_pos: 0,
            old_bi: BlockIterator::default(),
            reversed,
            fill_cache,
            err: None,
            key_buf: BytesMut::new(),
            iter_state: IterState::NewVersion,
            block_buf: vec![],
            decryption_buf: vec![],
        }
    }

    fn reset(&mut self) {
        self.b_pos = 0;
        self.err = None;
        self.iter_state = IterState::NewVersion;
        self.old_bi.reset_current_entry();
    }

    pub fn error(&self) -> &Option<table::Error> {
        &self.err
    }

    #[maybe_async::both]
    async fn set_block(&mut self, b_pos: i32) -> bool {
        self.b_pos = b_pos;
        let block = self
            .t
            .load_block(
                &self.idx,
                self.b_pos as usize,
                &mut self.block_buf,
                &mut self.decryption_buf,
                self.fill_cache,
            )
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "failed to load block: {:?}, file: {}, block: {:?}",
                    err,
                    self.t.id(),
                    self.b_pos
                );
            });
        self.bi.set_block(block);
        true
    }

    #[maybe_async::both]
    async fn set_old_block(&mut self, b_pos: i32) -> bool {
        self.old_b_pos = b_pos;
        let old_block = self.get_old_idx();
        let block = match self
            .t
            .load_old_block(
                &old_block,
                self.old_b_pos as usize,
                &mut self.block_buf,
                &mut self.decryption_buf,
                self.fill_cache,
            )
            .await
        {
            Ok(b) => b,
            Err(e) => {
                self.err = Some(e);
                return false;
            }
        };
        self.old_bi.set_block(block);
        true
    }

    #[maybe_async::both]
    async fn seek_to_first(&mut self) {
        self.reset();
        let num_blocks = self.idx.num_blocks();
        if num_blocks == 0 {
            self.err = Some(table::Error::Eof);
            return;
        }
        if !self.set_block(0).await {
            return;
        }
        self.bi.seek_to_first();
        self.sync_block_iterator();
    }

    #[maybe_async::both]
    async fn seek_to_last(&mut self) {
        self.reset();
        let num_blocks = self.idx.num_blocks();
        if num_blocks == 0 {
            self.err = Some(table::Error::Eof);
            return;
        }
        if !self.set_block(num_blocks as i32 - 1).await {
            return;
        }
        self.bi.seek_to_last();
        self.sync_block_iterator();
    }

    #[maybe_async::both]
    async fn seek_in_block(&mut self, b_pos: usize, key: InnerKey<'_>) {
        if !self.set_block(b_pos as i32).await {
            return;
        }
        self.bi.seek(key);
        self.sync_block_iterator();
    }

    #[maybe_async::both]
    async fn seek_from_offset(&mut self, b_pos: usize, offset: usize, key: InnerKey<'_>) {
        if !self.set_block(b_pos as i32).await {
            return;
        }
        self.bi.set_idx(offset as i32);
        self.sync_block_iterator();
        if self.key() >= key {
            return;
        }
        self.bi.seek(key);
        self.sync_block_iterator();
    }

    #[maybe_async::both]
    async fn seek_inner(&mut self, key: InnerKey<'_>) {
        self.reset();
        let idx = self.idx.seek_block(key);
        if idx == 0 {
            // The smallest key in our table is already strictly > key. We can return that.
            // This is like a SeekToFirst.
            self.seek_from_offset(0, 0, key).await;
            return;
        }

        // block[idx].smallest is > key.
        // Since idx>0, we know block[idx-1].smallest is <= key.
        // There are two cases.
        // 1) Everything in block[idx-1] is strictly < key. In this case, we should go
        // to the first    element of block[idx].
        // 2) Some element in block[idx-1] is >= key. We should go to that element.
        self.seek_in_block(idx - 1, key).await;
        if self.err.is_some() {
            // Case 1. Need to visit block[idx].
            if idx == self.idx.num_blocks() {
                // If idx == len(itr.t.blockEndOffsets), then input key is greater than ANY
                // element of table. There's nothing we can do. Valid() should
                // return false as we seek to end of table.
                return;
            }
            self.err = None;
            // Since block[idx].smallest is > key. This is essentially a
            // block[idx].SeekToFirst.
            self.seek_from_offset(idx, 0, key).await;
        }
        // Case 2: No need to do anything. We already did the seek in
        // block[idx-1].
    }

    #[maybe_async::both]
    async fn seek_for_prev(&mut self, key: InnerKey<'_>) {
        // TODO: Optimize this. We shouldn't have to take a Prev step.
        self.seek_inner(key).await;
        if self.key() != key {
            self.prev_inner().await;
        }
    }

    #[maybe_async::both(Recursion)]
    async fn next_inner(&mut self) {
        self.err = None;
        self.iter_state = IterState::NewVersion;
        if self.b_pos >= self.idx.num_blocks() as i32 {
            self.err = Some(table::Error::Eof);
            return;
        }
        if self.bi.b.is_empty() {
            if !self.set_block(self.b_pos).await {
                return;
            }
            self.bi.seek_to_first();
            self.sync_block_iterator();
            return;
        }
        self.bi.next();
        self.sync_block_iterator();
        if self.err.is_some() {
            self.b_pos += 1;
            self.bi.b.clear();
            self.next_inner().await;
        }
    }

    fn is_next_sync_inner(&self) -> bool {
        !self.bi.b.is_empty() && self.bi.idx + 1 < self.bi.num_entries() as i32
    }

    #[maybe_async::both(Recursion)]
    async fn prev_inner(&mut self) {
        self.err = None;
        self.iter_state = IterState::NewVersion;
        if self.b_pos < 0 {
            self.err = Some(table::Error::Eof);
            return;
        }
        if self.bi.b.is_empty() {
            if !self.set_block(self.b_pos).await {
                return;
            }
            self.bi.seek_to_last();
            self.sync_block_iterator();
            return;
        }

        self.bi.prev();
        self.sync_block_iterator();
        if self.err.is_some() {
            self.b_pos -= 1;
            self.bi.b.clear();
            self.prev_inner().await;
        }
    }

    fn is_prev_sync_inner(&self) -> bool {
        !self.bi.b.is_empty() && self.bi.idx > 0
    }

    fn sync_block_iterator(&mut self) {
        if self.bi.err.is_none() {
            self.key_buf.truncate(0);
            self.key_buf
                .extend_from_slice(self.bi.get_common_prefix().deref());
            self.key_buf.extend_from_slice(self.bi.get_diff_key());
        } else {
            self.err = self.bi.err.clone();
        }
    }

    fn same_old_key(&self) -> bool {
        if self.old_bi.ver == 0 {
            // Return false when old block is not initialized.
            // When there is only one entry in old block, the common prefix will be equal to
            // the key of the entry. Without this check, the function will return true
            // before other fields are corectly set.
            // See https://github.com/tidbcloud/cloud-storage-engine/issues/2062.
            return false;
        }

        let key = self.key();
        let old_common_prefix = self.old_bi.get_common_prefix();
        if old_common_prefix.len() + self.old_bi.diff_key_addr.len() != key.len() {
            return false;
        }
        key.slice(0, old_common_prefix.len()) == old_common_prefix
            && &key[old_common_prefix.len()..] == self.old_bi.get_diff_key()
    }

    fn get_old_idx(&mut self) -> Arc<Index> {
        if self.old_idx.is_none() {
            let old_idx = self.t.load_old_index();
            self.old_idx = Some(old_idx);
        }
        self.old_idx.as_ref().unwrap().clone()
    }

    #[maybe_async::both]
    async fn seek_old_block(&mut self) -> Option<table::Error> {
        assert!(self.iter_state == IterState::NewVersion);
        let old_idx = self.get_old_idx();
        let mut old_b_pos = old_idx.seek_block(self.key()) as i32 - 1;
        if old_b_pos == -1 {
            old_b_pos = 0;
        }
        if (self.old_bi.b.is_empty() || old_b_pos != self.old_b_pos)
            && !self.set_old_block(old_b_pos).await
        {
            return self.old_bi.err.clone();
        }
        self.old_bi
            .seek(InnerKey::from_inner_buf(self.key_buf.chunk()));
        assert!(self.old_bi.err.is_none());
        assert!(self.bi.old_ver == self.old_bi.ver);
        self.iter_state = IterState::OldVersion;
        None
    }

    pub fn set_reversed(&mut self, reversed: bool) {
        self.reversed = reversed;
    }
}

#[maybe_async::async_trait]
impl table::Iterator for TableIterator {
    #[maybe_async]
    async fn next(&mut self) {
        if !self.reversed {
            self.next_inner().await;
        } else {
            self.prev_inner().await;
        }
    }

    // Note: when `is_next_sync` is true, the next `next()` must be valid (i.e.,
    // `valid()` returns true). Correctness of `ConcatIterator.is_next_sync`
    // depends on this.
    fn is_next_sync(&self) -> bool {
        if self.t.is_sync() {
            return true;
        }
        if !self.reversed {
            self.is_next_sync_inner()
        } else {
            self.is_prev_sync_inner()
        }
    }

    #[maybe_async]
    async fn next_version(&mut self) -> bool {
        if self.bi.old_ver == 0 {
            return false;
        }
        if self.iter_state == IterState::OldVersioDone {
            return false;
        }
        if self.same_old_key() {
            if self.iter_state == IterState::NewVersion {
                // If it's the first time call, and the key is the same,
                // the old version key must be iterated by a previous key, we should not call
                // next.
                assert_eq!(self.bi.old_ver, self.old_bi.ver, "{:?}", self);
            } else {
                // It's the successive call of next_version, we need to move to the next
                // version.
                self.old_bi.next();
            }
            if self.old_bi.err.is_some() {
                self.iter_state = IterState::OldVersioDone;
                return false;
            }
            if !self.same_old_key() {
                self.iter_state = IterState::OldVersioDone;
                return false;
            }
            self.iter_state = IterState::OldVersion;
            return true;
        }
        self.err = self.seek_old_block().await;
        assert!(self.err.is_none());
        true
    }

    fn is_next_version_sync(&self) -> bool {
        if self.t.is_sync() {
            return true;
        }
        self.bi.old_ver == 0 || self.iter_state == IterState::OldVersioDone || self.same_old_key()
    }

    #[maybe_async]
    async fn rewind(&mut self) {
        if !self.reversed {
            self.seek_to_first().await;
        } else {
            self.seek_to_last().await;
        }
    }

    #[maybe_async]
    async fn seek(&mut self, key: InnerKey<'_>) {
        if !self.reversed {
            self.seek_inner(key).await;
        } else {
            self.seek_for_prev(key).await;
        }
    }

    fn is_cf_sync(&self, cf: usize) -> bool {
        self.t.is_cf_sync(cf)
    }

    fn key(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(self.key_buf.chunk())
    }

    fn value(&self) -> Value {
        let bi = if self.iter_state == IterState::NewVersion {
            &self.bi
        } else {
            &self.old_bi
        };
        let buf = bi.val_addr.get(bi.b.chunk());
        Value::new_with_meta_version(bi.meta, bi.ver, bi.user_meta_len, buf)
    }

    fn valid(&self) -> bool {
        self.err.is_none()
    }

    #[cfg(debug_assertions)]
    fn tag(&self) -> String {
        format!("sst:{:?}", self.t.id())
    }
}
