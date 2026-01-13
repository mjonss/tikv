// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::Display,
    mem, ptr, slice,
    sync::{
        Arc,
        atomic::{AtomicPtr, AtomicU32, Ordering},
    },
    time::Instant,
};

use byteorder::{ByteOrder, LittleEndian};
use rand::Rng;

use super::{
    super::table::{VALUE_VERSION_LEN, VALUE_VERSION_OFF, Value},
    WriteBatchEntry,
    skl::{MAX_HEIGHT, Node, deref},
};
use crate::metrics::{ENGINE_ARENA_GROW_DURATION_HISTOGRAM, elapsed_secs};

pub const NULL_ARENA_ADDR: u64 = 0;

const NULL_BLOCK_OFF: u32 = 0xffff_ffff;
const MAX_VAL_SIZE: u32 = 32 * 1024 * 1024; // 32MB

const BLOCK_ALIGN: u32 = 8;
const ALIGN_MASK: u32 = 0xffff_fff8;
const MAX_NUM_BLOCKS: usize = 256;
/// Block idx is encoded (see `ArenaAddr::new()`) as +1, so 254 is tha maximum
/// available value.
const BLOCK_IDX_MAX: usize = 254;
const BLOCK_IDX_MASK: u64 = 0xff00_0000_0000_0000;
const BLOCK_IDX_SHIFT: u64 = 56;
const BLOCK_OFF_MASK: u64 = 0x00ff_ffff_0000_0000;
const BLOCK_OFF_SHIFT: u64 = 32;
const SIZE_MASK: u64 = 0x0000_0000_00ff_ffff;
const FLAG_MASK: u64 = 0x0000_0000_ff00_0000;
const FLAG_SHIFT: u64 = 24;
const FLAG_VALUE_NODE: u8 = 1;

/// ArenaAddr is a u64 value encoded as:
/// | block_idx (8B, 63 - 56) | block_off (24B, 55 - 32) | size (32B, 31 - 0) |
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArenaAddr(pub u64);

impl Display for ArenaAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("{}_{}_{}", self.block_idx(), self.block_off(), self.size()).as_str())
    }
}

impl ArenaAddr {
    fn new(block_idx: usize, block_off: u32, size: u32) -> ArenaAddr {
        ArenaAddr(
            ((block_idx as u64 + 1) << BLOCK_IDX_SHIFT)
                | ((block_off as u64) << BLOCK_OFF_SHIFT)
                | (size as u64),
        )
    }

    pub fn null() -> ArenaAddr {
        ArenaAddr(NULL_ARENA_ADDR)
    }

    pub fn block_idx(self) -> usize {
        (((self.0 & BLOCK_IDX_MASK) >> BLOCK_IDX_SHIFT) - 1) as usize
    }

    pub fn block_off(self) -> u32 {
        ((self.0 & BLOCK_OFF_MASK) >> BLOCK_OFF_SHIFT) as u32
    }

    pub fn size(self) -> usize {
        (self.0 & SIZE_MASK) as usize
    }

    fn mark_value_node_addr(&mut self) {
        self.0 |= (FLAG_VALUE_NODE as u64) << FLAG_SHIFT
    }

    pub fn is_value_node_addr(self) -> bool {
        ((self.0 & FLAG_MASK) >> FLAG_SHIFT) as u8 & FLAG_VALUE_NODE > 0
    }

    pub fn is_null(self) -> bool {
        self.0 == NULL_ARENA_ADDR
    }
}

pub struct Arena {
    nodes: ArenaSegment,
    values: ArenaSegment,
    total_size: Arc<AtomicU32>,
    pub(crate) rand_id: i32,
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl Arena {
    pub fn new() -> Self {
        let mut rng = rand::thread_rng();
        let rand_id = rng.gen_range(0..i32::MAX);
        let total_size = Arc::new(AtomicU32::new(0));

        Self {
            nodes: ArenaSegment::new(total_size.clone()),
            values: ArenaSegment::new(total_size.clone()),
            total_size,
            rand_id,
        }
    }

    pub fn put_key(&self, key: &[u8]) -> ArenaAddr {
        let addr = self.nodes.alloc(key.len() as u32);
        let buf = self.nodes.get_mut_bytes(addr);
        unsafe {
            ptr::copy(key.as_ptr(), buf.as_mut_ptr(), key.len());
        }
        addr
    }

    pub fn put_val(&self, buf: &[u8], entry: &WriteBatchEntry) -> ArenaAddr {
        let size = entry.encoded_val_size();
        let addr = self.values.alloc(size as u32);
        let m_buf = self.values.get_mut_bytes(addr);
        m_buf[0] = entry.meta;
        m_buf[1] = entry.user_meta_len;
        let mut offset = VALUE_VERSION_OFF;
        LittleEndian::write_u64(&mut m_buf[offset..], entry.version);
        offset += VALUE_VERSION_LEN;
        m_buf[offset..offset + entry.user_meta_len as usize].copy_from_slice(entry.user_meta(buf));
        offset += entry.user_meta_len as usize;
        m_buf[offset..offset + entry.val_len as usize].copy_from_slice(entry.value(buf));
        addr
    }

    #[allow(clippy::mut_from_ref)]
    pub fn put_node(&self, height: usize, buf: &[u8], entry: &WriteBatchEntry) -> &mut Node {
        let node_size = mem::size_of::<Node>() - (MAX_HEIGHT - height) * 8;
        let node_addr = self.nodes.alloc(node_size as u32);
        let key_addr = self.put_key(entry.key(buf));
        let val_addr = self.put_val(buf, entry);
        let node_ptr = self.get_node(node_addr);
        let node = deref(node_ptr);
        node.addr = node_addr;
        node.height = height;
        node.key_addr = key_addr;
        node.value_addr.store(val_addr.0, Ordering::SeqCst);
        node
    }

    pub fn get_node(&self, addr: ArenaAddr) -> *mut Node {
        if addr.0 == NULL_ARENA_ADDR {
            return ptr::null_mut();
        }
        let bin = self.nodes.get_mut_bytes(addr);
        let ptr = bin.as_mut_ptr() as *mut Node;
        unsafe { &mut *ptr }
    }

    pub fn get_key(&self, node: &mut Node) -> &[u8] {
        self.nodes.get_bytes(node.key_addr)
    }

    pub fn get_val(&self, addr: ArenaAddr) -> Value {
        let bin = self.values.get_bytes(addr);
        Value::decode(bin)
    }

    pub fn put_val_node(&self, vn: ValueNode) -> ArenaAddr {
        let mut addr = self.nodes.alloc(VALUE_NODE_SIZE as u32);
        vn.encode(self.nodes.get_mut_bytes(addr));
        addr.mark_value_node_addr();
        addr
    }

    pub fn get_value_node(&self, addr: ArenaAddr) -> ValueNode {
        let mut vn: ValueNode = Default::default();
        vn.decode(self.nodes.get_bytes(addr));
        vn
    }

    pub fn size(&self) -> usize {
        self.total_size.load(Ordering::Acquire) as usize
    }
}

struct ArenaSegment {
    blocks: Vec<AtomicPtr<ArenaBlock>>,
    block_idx: AtomicU32,
    total_size: Arc<AtomicU32>,
}

impl ArenaSegment {
    fn new(total_size: Arc<AtomicU32>) -> Self {
        let mut blocks = Vec::with_capacity(MAX_NUM_BLOCKS);
        blocks.resize_with(MAX_NUM_BLOCKS, || AtomicPtr::<ArenaBlock>::default());
        let s = Self {
            blocks,
            block_idx: Default::default(),
            total_size,
        };
        let new_block = Box::into_raw(Box::new(ArenaBlock::new(0)));
        s.blocks[0].store(new_block, Ordering::Release);
        s
    }

    fn alloc(&self, size: u32) -> ArenaAddr {
        if size > MAX_VAL_SIZE {
            panic!("value {} is too large", size)
        }
        let block_idx = self.block_idx.load(Ordering::Acquire) as usize;
        let block = self.get_block(block_idx);
        let block_off = block.alloc(size);
        if block_off != NULL_BLOCK_OFF {
            self.total_size.fetch_add(size, Ordering::AcqRel);
            return ArenaAddr::new(block_idx, block_off, size);
        }
        self.grow(size);
        self.alloc(size)
    }

    fn grow(&self, min_size: u32) {
        let timer = Instant::now();
        let block_idx = self.block_idx.load(Ordering::Acquire) as usize;
        if block_idx >= BLOCK_IDX_MAX {
            self.panic_with_debug_info("grow failed: block idx exhausted");
        }

        let new_block_idx = block_idx + 1;
        let mut new_block_size = block_cap(block_idx);
        if new_block_size < min_size {
            new_block_size = (min_size + BLOCK_ALIGN - 1) & ALIGN_MASK;
        }
        let new_block = Box::into_raw(Box::new(ArenaBlock::new(new_block_size)));
        self.blocks[new_block_idx].store(new_block, Ordering::Release);
        self.block_idx
            .store(new_block_idx as u32, Ordering::Release);
        ENGINE_ARENA_GROW_DURATION_HISTOGRAM.observe(elapsed_secs(timer))
    }

    #[inline(always)]
    fn get_block(&self, block_idx: usize) -> &ArenaBlock {
        unsafe {
            if let Some(block) = self.blocks[block_idx].load(Ordering::Acquire).as_ref() {
                block
            } else {
                panic!(
                    "failed to get block idx {}, max {}",
                    block_idx,
                    self.block_idx.load(Ordering::Acquire),
                );
            }
        }
    }

    fn get_bytes(&self, addr: ArenaAddr) -> &[u8] {
        self.get_block(addr.block_idx())
            .get_bytes(addr.block_off(), addr.size())
    }

    #[allow(clippy::mut_from_ref)]
    fn get_mut_bytes(&self, addr: ArenaAddr) -> &mut [u8] {
        self.get_block(addr.block_idx())
            .get_mut_bytes(addr.block_off(), addr.size())
    }

    fn panic_with_debug_info(&self, msg: &str) {
        let blocks = (0..self.blocks.len())
            .map(|idx| {
                unsafe { self.blocks[idx].load(Ordering::Acquire).as_ref() }
                    .map(|block| (block.len.load(Ordering::Acquire), block.cap))
            })
            .collect::<Vec<_>>();
        panic!(
            "{}: blocks: {:?}, self.block_idx: {}",
            msg,
            blocks,
            self.block_idx.load(Ordering::Acquire)
        );
    }
}

impl Drop for ArenaSegment {
    fn drop(&mut self) {
        for i in 0..MAX_NUM_BLOCKS {
            let block = self.blocks[i].load(Ordering::Acquire);
            if block.is_null() {
                break;
            }
            unsafe {
                drop(Box::from_raw(block));
            }
        }
    }
}

/// Calculate capacity of blocks.
///
/// When blocks are used up (255 in maximum), there is no way to solve but
/// panic. So we allocate large memory when blocks are nearly exhausted.
///
/// Note: To avoid exhausted, another alternative solution is switching memtable
/// before used up, but we do not adopt this currently, as it's not easy to
/// switch memtable during applying a write batch. Besides, change timing of
/// switching memtable is not backward compatible.
fn block_cap(idx: usize) -> u32 {
    if idx >= MAX_NUM_BLOCKS - 32 {
        16 * 1024 * 1024 // block_off is encoded as 24 bytes, so the max value is 16M.
    } else if idx >= MAX_NUM_BLOCKS * 3 / 4 {
        2 * 1024 * 1024
    } else if idx >= MAX_NUM_BLOCKS / 2 {
        1024 * 1024
    } else {
        // Exponential growth for every 4 idx:
        //   1K, 1K, 1K, 1K, 2K, 2K, 2k, 2K, ...
        // starts from idx 36 to idx 127, the values is always 512KB.
        1 << ((idx / 4).min(9) + 10)
    }
}

struct ArenaBlock {
    len: AtomicU32,
    cap: u32,
    ptr: *mut u8,
}

impl ArenaBlock {
    fn new(cap: u32) -> Self {
        let mut buf: Vec<u64> = vec![0; (cap + 7) as usize / 8];
        let ptr = buf.as_mut_ptr() as *mut u8;
        mem::forget(buf);
        Self {
            len: AtomicU32::new(0),
            cap,
            ptr,
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_mut_bytes(&self, offset: u32, size: usize) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut::<u8>(self.ptr.add(offset as usize), size) }
    }

    pub fn get_bytes(&self, offset: u32, size: usize) -> &[u8] {
        unsafe { slice::from_raw_parts::<u8>(self.ptr.add(offset as usize), size) }
    }

    pub fn alloc(&self, size: u32) -> u32 {
        // The returned addr should be aligned in 8 bytes.
        let offset = (self.len.load(Ordering::SeqCst) + BLOCK_ALIGN - 1) & ALIGN_MASK;
        let length = offset + size;
        if length > self.cap {
            return NULL_BLOCK_OFF;
        }
        self.len.store(length, Ordering::Release);
        offset
    }
}

impl Drop for ArenaBlock {
    fn drop(&mut self) {
        unsafe {
            Vec::from_raw_parts(self.ptr as *mut u64, 0, self.cap as usize / 8);
        }
    }
}

const VALUE_NODE_SIZE: usize = 16;

#[derive(Clone, Copy, Default)]
pub struct ValueNode {
    pub val_addr: ArenaAddr,
    pub next_val_addr: ArenaAddr,
}

impl ValueNode {
    fn encode(&self, buf: &mut [u8]) {
        LittleEndian::write_u64(buf, self.val_addr.0);
        LittleEndian::write_u64(&mut buf[8..], self.next_val_addr.0);
    }

    fn decode(&mut self, bin: &[u8]) {
        self.val_addr = ArenaAddr(LittleEndian::read_u64(bin));
        self.next_val_addr = ArenaAddr(LittleEndian::read_u64(&bin[8..]));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_addr() {
        let addr = ArenaAddr::new(1, 2, 3);
        assert_eq!(addr.block_idx(), 1);
        assert_eq!(addr.block_off(), 2);
        assert_eq!(addr.size(), 3);
    }

    #[test]
    fn test_arena() {
        let arena = Arena::new();
        let addr = arena.nodes.alloc(8);
        let buf = arena.nodes.get_mut_bytes(addr);
        buf[0] = 3;
        println!("{:?}", arena.nodes.get_bytes(addr))
    }

    #[test]
    fn test_arena_blocks_exhausted() {
        let arena = Arena::new();
        // block 0 is created on `new()` with 0 capacity.
        for i in 1..=224 {
            if i & 0x1 == 0 {
                arena.values.alloc(1);
            } else {
                arena.values.alloc(block_cap(i));
            }
        }
        let total_size = arena.values.total_size.load(Ordering::Acquire);
        let entry_size = 8 * 1024 * 1024;
        // Alloc after blocks exhausted should panic.
        let result = std::panic::catch_unwind(|| {
            for _ in 0..100 {
                arena.values.alloc(entry_size);
            }
        });
        assert!(result.is_err());
        let max_total_size = arena.values.total_size.load(Ordering::Acquire);
        assert_eq!(max_total_size - total_size, entry_size * 60);
    }

    #[test]
    fn test_block_capacity() {
        assert_eq!(block_cap(0), 1024);
        assert_eq!(block_cap(4), 2048);
        assert_eq!(block_cap(8), 4096);
        assert_eq!(block_cap(12), 8192);
        assert_eq!(block_cap(16), 16384);
        assert_eq!(block_cap(20), 32768);
        assert_eq!(block_cap(24), 65536);
        assert_eq!(block_cap(28), 131072);
        assert_eq!(block_cap(32), 262144);
        for idx in 36..128 {
            assert_eq!(block_cap(idx), 512 * 1024);
        }
        for idx in 128..192 {
            assert_eq!(block_cap(idx), 1024 * 1024);
        }
        for idx in 192..224 {
            assert_eq!(block_cap(idx), 2 * 1024 * 1024);
        }
        for idx in 224..256 {
            assert_eq!(block_cap(idx), 16 * 1024 * 1024);
        }
        let mut sum = 0;
        for idx in 0..255 {
            sum += block_cap(idx);
        }
        // There is sufficient memory before switch mem-table.
        assert!(sum > 640 * 1024 * 1024);
    }
}
