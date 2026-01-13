// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ops::{Add, Deref, Sub},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use dashmap::DashMap;
use tikv_util::{error, info};

use crate::{ACTIVE_KEYSPACE_READ_BYTES, AtomicDuration, AtomicTime, Config, TimeUnit};

const MIN_WAIT_TIME_INTERVAL: Duration = Duration::from_millis(1);

pub const MAX_WAIT_TIME: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub enum Action {
    None,
    ScaleOut,
    Throttle,
    Reject,
}

#[derive(Clone)]
pub struct ReadLimiter {
    pub(crate) core: Arc<ReadLimiterCore>,
}

pub struct ReadLimiterCore {
    pub(crate) enabled: AtomicBool,
    pub(crate) timeout: AtomicDuration,
    pub(crate) stats_interval: AtomicDuration,
    pub(crate) max_wait_time: AtomicDuration,
    pub(crate) keyspace_limiters: DashMap<u32, (Instant, KeyspaceReadLimiter)>,
}

impl Deref for ReadLimiter {
    type Target = ReadLimiterCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl ReadLimiterCore {
    pub fn new(config: Config) -> Self {
        let timeout = config.limiter_timeout.0;
        let stats_interval = config.limiter_stats_interval.0;
        Self {
            enabled: AtomicBool::from(config.get_read_enabled()),
            timeout: AtomicDuration::new(timeout, TimeUnit::Millisecond),
            stats_interval: AtomicDuration::new(stats_interval, TimeUnit::Millisecond),
            max_wait_time: AtomicDuration::new(MAX_WAIT_TIME, TimeUnit::Millisecond),
            keyspace_limiters: DashMap::new(),
        }
    }
}

impl ReadLimiter {
    pub fn new(config: Config) -> Self {
        Self {
            core: Arc::new(ReadLimiterCore::new(config)),
        }
    }

    pub(crate) fn update_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Relaxed);
        if !enabled {
            self.clear_all_limiter();
        }
    }

    pub(crate) fn get_enabled(&self) -> bool {
        self.enabled.load(Relaxed)
    }

    pub(crate) fn update_timeout(&self, timeout: Duration) {
        self.timeout.store(timeout);
    }

    pub(crate) fn update_stats_interval(&self, stats_interval: Duration) {
        self.stats_interval.store(stats_interval);
    }

    pub(crate) fn update_max_wait_time(&self, max_wait_time: Duration) {
        self.max_wait_time.store(max_wait_time);
    }

    pub(crate) fn clear_all_limiter(&self) {
        if self.keyspace_limiters.is_empty() {
            return;
        }
        self.keyspace_limiters.iter().for_each(|keyspace_limiter_ref|{
            let keyspace_id = *keyspace_limiter_ref.key();
            let keyspace_label = keyspace_id.to_string();
            let keyspace_str = keyspace_label.as_str();
            let _ =  ACTIVE_KEYSPACE_READ_BYTES.remove_label_values(&[keyspace_str]).map_err(
                |err| error!("failed to remove active keyspace read bytes metric"; "keyspace_id" => keyspace_str, "err" => %err),
            );
        });
        self.keyspace_limiters.clear();
    }

    pub(crate) fn update_limit(
        &self,
        keyspace_id: u32,
        req_speed_limit: Option<f64>,
        bytes_speed_limit: Option<f64>,
        instant: Instant,
    ) {
        if !self.get_enabled() {
            return;
        }
        if req_speed_limit.unwrap_or_default().is_infinite()
            && bytes_speed_limit.unwrap_or_default().is_infinite()
        {
            self.remove_limiter(keyspace_id);
            return;
        }
        self.keyspace_limiters
            .entry(keyspace_id)
            .and_modify(|(ts, keyspace_read_limiter)| {
                *ts = instant;
                keyspace_read_limiter.update_stats_interval(self.stats_interval.load());
                keyspace_read_limiter.update_max_wait_time(self.max_wait_time.load());
                keyspace_read_limiter.set_speed_limit(req_speed_limit, bytes_speed_limit);
            })
            .or_insert_with(|| {
                let keyspace_read_limiter = KeyspaceReadLimiter::new(
                    keyspace_id,
                    self.stats_interval.load(),
                    self.max_wait_time.load(),
                );
                keyspace_read_limiter.set_speed_limit(req_speed_limit, bytes_speed_limit);
                (instant, keyspace_read_limiter)
            });
    }

    pub fn get_limiter(&self, keyspace_id: u32) -> Option<KeyspaceReadLimiter> {
        if !self.get_enabled() {
            return None;
        }
        let timeout = self.timeout.load();
        if let Some((ts, keyspace_read_limiter)) =
            self.keyspace_limiters.get(&keyspace_id).map(|v| v.clone())
        {
            if ts.elapsed() < timeout {
                Some(keyspace_read_limiter.clone())
            } else {
                self.remove_limiter(keyspace_id);
                None
            }
        } else {
            None
        }
    }

    fn remove_limiter(&self, keyspace_id: u32) {
        self.keyspace_limiters.remove(&keyspace_id);
        let keyspace_label = keyspace_id.to_string();
        let keyspace_str = keyspace_label.as_str();
        let _ =  ACTIVE_KEYSPACE_READ_BYTES.remove_label_values(&[keyspace_str]).map_err(
            |err| error!("failed to remove active keyspace read bytes metric"; "keyspace_id" => keyspace_str, "err" => %err),
        );
    }
}

#[derive(Clone)]
pub struct KeyspaceReadLimiter {
    core: Arc<KeyspaceReadLimiterCore>,
}

pub struct KeyspaceReadLimiterCore {
    keyspace_id: u32,
    req_limiter: tikv_util::time::Limiter,
    bytes_limiter: tikv_util::time::Limiter,
    waiting_cnt: AtomicI64,
    allowed_time: AtomicTime, // Requests after the allowed time do not need to wait.
    last_time: AtomicTime,
    stats_interval: AtomicDuration,
    max_wait_time: AtomicDuration,
}

impl Deref for KeyspaceReadLimiter {
    type Target = KeyspaceReadLimiterCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl KeyspaceReadLimiterCore {
    pub fn new(keyspace_id: u32, stats_interval: Duration, max_wait_time: Duration) -> Self {
        let req_limiter = <tikv_util::time::Limiter>::builder(f64::INFINITY).build();
        let bytes_limiter = <tikv_util::time::Limiter>::builder(f64::INFINITY).build();
        let start = Instant::now();
        Self {
            keyspace_id,
            req_limiter,
            bytes_limiter,
            waiting_cnt: AtomicI64::default(),
            allowed_time: AtomicTime::new(start, TimeUnit::Microsecond),
            last_time: AtomicTime::new(start, TimeUnit::Millisecond),
            stats_interval: AtomicDuration::new(stats_interval, TimeUnit::Millisecond),
            max_wait_time: AtomicDuration::new(max_wait_time, TimeUnit::Millisecond),
        }
    }
}

impl Default for KeyspaceReadLimiter {
    fn default() -> Self {
        Self::new(0, Duration::from_secs(1), MAX_WAIT_TIME)
    }
}

impl KeyspaceReadLimiter {
    pub fn new(keyspace_id: u32, stats_interval: Duration, max_wait_time: Duration) -> Self {
        Self {
            core: Arc::new(KeyspaceReadLimiterCore::new(
                keyspace_id,
                stats_interval,
                max_wait_time,
            )),
        }
    }

    pub(crate) fn update_allowed_time<F>(&self, update: F) -> Duration
    where
        F: Fn(Instant) -> (Option<Instant>, Duration),
    {
        let mut allowed_time = self.allowed_time.load();
        loop {
            let (new_allowed_time, dur) = update(allowed_time);
            let Some(new_allowed_time) = new_allowed_time else {
                return dur;
            };
            match self
                .allowed_time
                .compare_exchange(allowed_time, new_allowed_time)
            {
                Some(current) => {
                    allowed_time = current;
                }
                None => {
                    return dur;
                }
            }
        }
    }

    pub fn take_wait_time(&self) -> Duration {
        let update = |allowed_time: Instant| {
            let now = Instant::now();
            let dur = allowed_time.duration_since(now);
            if dur < MIN_WAIT_TIME_INTERVAL {
                return (None, dur);
            }
            let new_allowed_time = now;
            (Some(new_allowed_time), dur)
        };
        self.update_allowed_time(update)
    }

    pub fn wait_time(&self) -> Duration {
        let update = |allowed_time: Instant| {
            let now = Instant::now();
            let dur = allowed_time.duration_since(now);
            if dur < MIN_WAIT_TIME_INTERVAL {
                return (None, dur);
            };
            let new_allowed_time = allowed_time.sub(MIN_WAIT_TIME_INTERVAL);
            (Some(new_allowed_time), MIN_WAIT_TIME_INTERVAL)
        };
        self.update_allowed_time(update)
    }

    pub async fn wait(&self) -> Duration {
        self.waiting_cnt.fetch_add(1, Relaxed);
        let mut wait_time = Duration::default();
        loop {
            let dur = self.wait_time();
            if dur.is_zero() {
                break;
            }
            tokio::time::sleep(dur).await;
            wait_time = wait_time.add(dur);
            let avg_wait_time = Duration::from_secs_f64(
                self.allowed_time
                    .load()
                    .duration_since(Instant::now())
                    .as_secs_f64()
                    / self.waiting_cnt.load(Relaxed) as f64,
            );
            if wait_time >= avg_wait_time.min(self.max_wait_time.load()) {
                break;
            }
        }
        self.waiting_cnt.fetch_add(-1, Relaxed);
        wait_time
    }

    pub fn consume(&self, bytes: usize) {
        let dur = self
            .req_limiter
            .consume_duration(1)
            .max(self.bytes_limiter.consume_duration(bytes));

        let update = |allowed_time: Instant| {
            let now = Instant::now();
            let new_allowed_time = if allowed_time.duration_since(now).is_zero() {
                now.add(dur)
            } else {
                allowed_time.add(dur)
            };
            (Some(new_allowed_time), dur)
        };
        self.update_allowed_time(update);
    }

    pub fn unconsume(&self, bytes: usize) {
        self.req_limiter.unconsume(1);
        self.bytes_limiter.unconsume(bytes);
    }

    pub fn is_unlimited(&self) -> bool {
        self.req_limiter.speed_limit() == f64::INFINITY
            && self.bytes_limiter.speed_limit() == f64::INFINITY
    }

    pub fn total_consumed(&self) -> (usize /* req */, usize /* bytes */) {
        (
            self.req_limiter.total_bytes_consumed(),
            self.bytes_limiter.total_bytes_consumed(),
        )
    }

    pub fn speed_limit(&self) -> (f64 /* req */, f64 /* bytes */) {
        (
            self.req_limiter.speed_limit(),
            self.bytes_limiter.speed_limit(),
        )
    }

    pub fn set_speed_limit(&self, req_speed_limit: Option<f64>, bytes_speed_limit: Option<f64>) {
        if let Some(req_speed_limit) = req_speed_limit {
            if req_speed_limit > 0.0 {
                self.req_limiter.set_speed_limit(req_speed_limit);
            } else {
                self.req_limiter.set_speed_limit(f64::INFINITY);
            }
        }
        if let Some(bytes_speed_limit) = bytes_speed_limit {
            if bytes_speed_limit > 0.0 {
                self.bytes_limiter.set_speed_limit(bytes_speed_limit);
            } else {
                self.bytes_limiter.set_speed_limit(f64::INFINITY);
            }
        }
        self.update_statistics();
    }

    fn update_statistics(&self) {
        let last_time = self.last_time.load();
        let dur = last_time.elapsed();
        if dur < self.stats_interval.load() {
            return;
        }
        self.last_time.store(Instant::now());
        let total_requests = self.req_limiter.total_bytes_consumed() as f64;
        let total_bytes = self.bytes_limiter.total_bytes_consumed() as f64;
        self.req_limiter.reset_statistics();
        self.bytes_limiter.reset_statistics();
        if total_requests == 0.0 {
            return;
        }
        let qps = total_requests / dur.as_secs_f64();
        let bytes_rate = total_bytes / dur.as_secs_f64();
        let bytes_per_req = total_bytes / total_requests;
        info!("resource control update_statistics dur {:?}", dur;
            "keyspace_id" => self.keyspace_id,
            "total_requests" => total_requests,
            "total_bytes" => total_bytes,
            "qps" => qps,
            "bytes_rate" => bytes_rate,
            "bytes_per_req" => bytes_per_req,
        );
    }

    fn update_stats_interval(&self, stats_interval: Duration) {
        self.stats_interval.store(stats_interval);
    }

    fn update_max_wait_time(&self, max_wait_time: Duration) {
        self.max_wait_time.store(max_wait_time);
    }
}

#[derive(Clone)]
pub struct TransferLeaderLimiter {
    pub(crate) core: Arc<TransferLeaderLimiterCore>,
}

pub struct TransferLeaderLimiterCore {
    pub(crate) enabled: AtomicBool,
    rate_limiter: UniformLimiter,
    last_time: AtomicTime,
    stats_interval: AtomicDuration,
    last_rate: AtomicU64,
}

impl Deref for TransferLeaderLimiter {
    type Target = TransferLeaderLimiterCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl TransferLeaderLimiterCore {
    pub fn new(config: Config) -> Self {
        let rate_limiter = UniformLimiter::new(config.max_transfer_leader_per_second);
        Self {
            enabled: AtomicBool::from(config.get_transfer_leader_enabled()),
            last_time: AtomicTime::new(Instant::now(), TimeUnit::Millisecond),
            stats_interval: AtomicDuration::new(
                config.limiter_stats_interval.0,
                TimeUnit::Millisecond,
            ),
            rate_limiter,
            last_rate: AtomicU64::default(),
        }
    }
}

impl TransferLeaderLimiter {
    pub fn new(config: Config) -> Self {
        Self {
            core: Arc::new(TransferLeaderLimiterCore::new(config)),
        }
    }

    pub(crate) fn update_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Relaxed);
    }

    pub(crate) fn get_enabled(&self) -> bool {
        self.enabled.load(Relaxed)
    }

    pub(crate) fn update_stats_interval(&self, stats_interval: Duration) {
        self.stats_interval.store(stats_interval);
    }

    pub(crate) fn update_limit(&self, speed_limit: u64) {
        self.rate_limiter.set_speed_limit(speed_limit);
        self.update_statistics();
    }

    pub fn allow(&self) -> bool {
        if !self.get_enabled() {
            return true;
        }
        let allow = self.rate_limiter.allow(1);

        self.update_statistics(); // TODO： Remove this line after implementing dynamic speed adjustment.
        allow
    }

    fn update_statistics(&self) {
        let last_time = self.last_time.load();
        let dur = last_time.elapsed();
        if dur < self.stats_interval.load() {
            return;
        }
        self.last_time.store(Instant::now());
        let transfer_leader = self.rate_limiter.total_bytes_consumed() as f64;
        self.rate_limiter.reset_statistics();
        let rate = transfer_leader / dur.as_secs_f64();
        info!("resource control update_statistics dur {:?}", dur;
            "transfer_leader" => transfer_leader,
            "rate" => rate,
        );
        self.last_rate.store(rate as u64, Relaxed)
    }

    pub fn rate(&self) -> u64 {
        self.last_rate.load(Relaxed)
    }
}

/// Lock-free, non-blocking, uniform rate limiter.
/// Ensures smooth execution by rejecting requests if they arrive too early.
///
/// - rate_bytes_per_sec: throughput in bytes/sec
pub struct UniformLimiter {
    rate_bytes_per_sec: AtomicU64,
    allowed_time: AtomicTime,
    total_bytes_consumed: AtomicU64,
}

impl UniformLimiter {
    pub fn new(rate_bytes_per_sec: u64) -> Self {
        assert!(rate_bytes_per_sec > 0);
        let base = Instant::now();
        Self {
            rate_bytes_per_sec: AtomicU64::new(rate_bytes_per_sec),
            allowed_time: AtomicTime::new(base, TimeUnit::Nanosecond),
            total_bytes_consumed: AtomicU64::default(),
        }
    }

    fn bytes_to_dur(rate_bytes_per_sec: u64, bytes: u64) -> Duration {
        let secs: f64 = if rate_bytes_per_sec == 0 || bytes == 0 {
            0.0
        } else {
            bytes as f64 / rate_bytes_per_sec as f64
        };
        Duration::from_secs_f64(secs)
    }

    /// Dynamically changes the speed limit. The new limit applies to all clones
    /// of this instance.
    pub fn set_speed_limit(&self, speed_limit: u64) {
        debug_assert!(speed_limit > 0, "speed limit must be positive");
        self.rate_bytes_per_sec.store(speed_limit, Relaxed);
    }

    /// Non-blocking check. Returns true if request can proceed immediately,
    /// false if it arrives too early (would break uniform pacing).
    pub fn allow(&self, bytes: u64) -> bool {
        if bytes == 0 {
            return true;
        }
        self.total_bytes_consumed
            .fetch_add(bytes, Ordering::Relaxed);
        let now = Instant::now();
        let duration = UniformLimiter::bytes_to_dur(self.rate_bytes_per_sec.load(Relaxed), bytes);
        let mut allowed = self.allowed_time.load();
        loop {
            if now < allowed {
                // Too early, must reject to maintain smoothness
                return false;
            }
            let scheduled_end = allowed.add(duration).max(now.sub(duration));
            match self.allowed_time.compare_exchange(allowed, scheduled_end) {
                Some(current) => {
                    allowed = current;
                }
                None => {
                    return true;
                }
            }
        }
    }

    /// Obtains the total number of bytes consumed by this limiter so far.
    ///
    /// If more than `usize::MAX` bytes have been consumed, the count will wrap
    /// around.
    pub fn total_bytes_consumed(&self) -> usize {
        self.total_bytes_consumed.load(Ordering::Relaxed) as usize
    }

    /// Resets the total number of bytes consumed to 0.
    pub fn reset_statistics(&self) {
        self.total_bytes_consumed.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;

    use super::*;

    #[test]
    fn test_uniform_limiter() {
        let limiter = UniformLimiter::new(1000); // ~1000 bytes/sec

        // First request should pass.
        assert!(limiter.allow(100));

        // Immediate second request likely rejected (too soon).
        assert!(!limiter.allow(100));

        // After enough time passes, allow again.
        sleep(Duration::from_millis(100));
        assert!(limiter.allow(100));

        sleep(Duration::from_millis(100));
        assert!(limiter.allow(200));

        // Request rejected due to insufficient wait time.
        sleep(Duration::from_millis(100));
        assert!(!limiter.allow(100));

        // After enough time passes, allow again.
        sleep(Duration::from_millis(100));
        assert!(limiter.allow(100));

        assert!(!limiter.allow(100));
        sleep(Duration::from_millis(100));

        for i in 10..30 {
            assert!(limiter.allow(i));
            sleep(Duration::from_millis(i));
        }
    }
}
