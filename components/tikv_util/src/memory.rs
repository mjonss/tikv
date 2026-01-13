// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt, mem,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use kvproto::{
    encryptionpb::EncryptionMeta,
    kvrpcpb::LockInfo,
    metapb::{Peer, Region, RegionEpoch},
    raft_cmdpb::{self, RaftCmdRequest, ReadIndexRequest},
};

/// Transmute vec from one type to the other type.
///
/// # Safety
///
/// The two types should be with same memory layout.
#[inline]
pub unsafe fn vec_transmute<F, T>(from: Vec<F>) -> Vec<T> {
    debug_assert!(mem::size_of::<F>() == mem::size_of::<T>());
    debug_assert!(mem::align_of::<F>() == mem::align_of::<T>());
    let (ptr, len, cap) = from.into_raw_parts();
    Vec::from_raw_parts(ptr as _, len, cap)
}

pub trait HeapSize {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for Region {
    fn heap_size(&self) -> usize {
        let mut size = self.start_key.capacity() + self.end_key.capacity();
        size += mem::size_of::<RegionEpoch>();
        size += self.peers.capacity() * mem::size_of::<Peer>();
        // There is still a `bytes` in `EncryptionMeta`. Ignore it because it could be
        // shared.
        size += mem::size_of::<EncryptionMeta>();
        size
    }
}

impl HeapSize for ReadIndexRequest {
    fn heap_size(&self) -> usize {
        self.key_ranges
            .iter()
            .map(|r| r.start_key.capacity() + r.end_key.capacity())
            .sum()
    }
}

impl HeapSize for LockInfo {
    fn heap_size(&self) -> usize {
        self.primary_lock.capacity()
            + self.key.capacity()
            + self.secondaries.iter().map(|k| k.len()).sum::<usize>()
    }
}

impl HeapSize for RaftCmdRequest {
    fn heap_size(&self) -> usize {
        mem::size_of::<raft_cmdpb::RaftRequestHeader>()
            + self.requests.capacity() * mem::size_of::<raft_cmdpb::Request>()
            + mem::size_of_val(&self.admin_request)
            + mem::size_of_val(&self.status_request)
    }
}

#[derive(Clone)]
pub struct MemoryLimiter {
    cap: u64,
    used: Arc<AtomicU64>,
    metric: Option<prometheus::IntGauge>,
}

impl fmt::Debug for MemoryLimiter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryLimiter")
            .field("cap", &self.cap)
            .field("used", &self.used())
            .finish()
    }
}

impl MemoryLimiter {
    pub fn new(cap: u64, metric: Option<prometheus::IntGauge>) -> Self {
        Self {
            cap,
            used: Default::default(),
            metric,
        }
    }

    pub fn acquire(&self, request: u64) -> Result<MemoryLimiterGuard, u64 /* exceeded size */> {
        if request > self.cap {
            return Err(request - self.cap);
        }

        if request > 0 {
            let after = self.used.fetch_add(request, Ordering::AcqRel) + request;
            if after > self.cap {
                self.release(request, false);
                return Err(after - self.cap);
            }
            self.set_metric(after);
        }
        Ok(MemoryLimiterGuard {
            limiter: self.clone(),
            request,
        })
    }

    fn release(&self, request: u64, set_metric: bool) {
        if request > 0 {
            let after = self
                .used
                .fetch_sub(request, Ordering::AcqRel)
                .saturating_sub(request);
            if set_metric {
                self.set_metric(after);
            }
        }
    }

    fn set_metric(&self, current: u64) {
        if let Some(ref metric) = self.metric {
            metric.set(current as i64);
        }
    }

    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    #[cfg(feature = "testexport")]
    pub fn set_cap(&mut self, cap: u64) {
        self.cap = cap;
    }
}

pub struct MemoryLimiterGuard {
    limiter: MemoryLimiter,
    request: u64,
}

impl Drop for MemoryLimiterGuard {
    fn drop(&mut self) {
        self.limiter.release(self.request, true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_limiter() {
        let cap = 1024;
        let limiter = MemoryLimiter::new(cap, None);
        assert_eq!(limiter.used(), 0);
        assert_eq!(limiter.cap, cap);

        let guard = limiter.acquire(512).unwrap();
        assert_eq!(limiter.used(), 512);
        let guard1 = limiter.acquire(512).unwrap();
        assert_eq!(limiter.used(), cap);

        assert!(matches!(limiter.acquire(1), Err(1)));

        drop(guard);
        assert_eq!(limiter.used(), 512);
        drop(guard1);
        assert_eq!(limiter.used(), 0);

        {
            let _guard = limiter.acquire(0).unwrap();
            assert_eq!(limiter.used(), 0);
        }
        assert_eq!(limiter.used(), 0);
    }
}
