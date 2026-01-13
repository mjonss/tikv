// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::collections::VecDeque;

use api_version::ApiV2;
use crossbeam::utils::CachePadded;
use kvengine::table::TxnFile;
use parking_lot::{Mutex, MutexGuard};
use tikv_util::deadline::Deadline;
use txn_types::Key;

const WAITING_LIST_SHRINK_SIZE: usize = 8;
const WAITING_LIST_MAX_CAPACITY: usize = 16;

/// Latch which is used to serialize accesses to resources hashed to the same
/// slot.
///
/// Latches are indexed by slot IDs. The keys of a command are hashed into
/// unsigned numbers, then the command is added to the waiting queues of the
/// latches.
///
/// If command A is ahead of command B in one latch, it must be ahead of command
/// B in all the overlapping latches. This is an invariant ensured by the
/// `gen_lock`, `acquire` and `release`.
#[derive(Clone)]
pub struct Latch {
    // store keyspace_id, hash value of the key and command ID which requires this key.
    // The key hash value doesn't contains keyspace prefix, so it's possible that two tenants
    // conflict on the same hash value, so we must also compare keyspace_id for conflict check.
    pub waiting: VecDeque<Option<(u32, u64, u64)>>,
}

impl Latch {
    /// Creates a latch with an empty waiting queue.
    pub fn new() -> Latch {
        Latch {
            waiting: VecDeque::new(),
        }
    }

    /// Find the first command ID in the queue whose hash value is equal to
    /// hash.
    pub fn get_first_req_by_hash(&self, keyspace_id: u32, hash: u64) -> Option<u64> {
        for (ks, h, cid) in self.waiting.iter().flatten() {
            if *ks == keyspace_id && *h == hash {
                return Some(*cid);
            }
        }
        None
    }

    /// Remove the first command ID in the queue whose hash value is equal to
    /// hash_key. If the element which would be removed does not appear at the
    /// front of the queue, it will leave a hole in the queue. So we must remove
    /// consecutive hole when remove the head of the queue to make the queue not
    /// too long.
    pub fn pop_front(&mut self, keyspace_id: u32, key_hash: u64) -> Option<(u32, u64, u64)> {
        if let Some(item) = self.waiting.pop_front() {
            if let Some((ks, k, _)) = item.as_ref() {
                if *ks == keyspace_id && *k == key_hash {
                    self.maybe_shrink();
                    return item;
                }
                self.waiting.push_front(item);
            }
            // FIXME: remove this clippy attribute once https://github.com/rust-lang/rust-clippy/issues/6784 is fixed.
            #[allow(clippy::manual_flatten)]
            for it in self.waiting.iter_mut() {
                if let Some((ks, v, _)) = it {
                    if *ks == keyspace_id && *v == key_hash {
                        return it.take();
                    }
                }
            }
        }
        None
    }

    pub fn wait_for_wake(&mut self, keyspace_id: u32, key_hash: u64, cid: u64) {
        self.waiting.push_back(Some((keyspace_id, key_hash, cid)));
    }

    /// Pushes the cid to the front of the queue. Be careful when using it.
    pub(crate) fn push_preemptive(&mut self, keyspace_id: u32, key_hash: u64, cid: u64) {
        self.waiting.push_front(Some((keyspace_id, key_hash, cid)));
    }

    /// For some hot keys, the waiting list maybe very long, so we should shrink
    /// the waiting VecDeque after pop.
    fn maybe_shrink(&mut self) {
        // Pop item which is none to make queue not too long.
        while let Some(item) = self.waiting.front() {
            if item.is_some() {
                break;
            }
            self.waiting.pop_front().unwrap();
        }
        if self.waiting.capacity() > WAITING_LIST_MAX_CAPACITY
            && self.waiting.len() < WAITING_LIST_SHRINK_SIZE
        {
            self.waiting.shrink_to_fit();
        }
    }
}

/// Lock required for a command.
#[derive(Clone, Debug)]
pub struct Lock {
    /// The hash value of the keys that a command must acquire before being able
    /// to be processed.
    pub required_hashes: Vec<u64>,

    pub keyspace_id: u32,

    /// The number of latches that the command has acquired.
    pub owned_count: usize,

    pub region_id: u64,

    pub start_ts: u64,

    pub checked_txn_cid: u64,

    /// If txn_file_hashes is some, then required_hashes would be empty.
    pub txn_file: Option<TxnFile>,

    pub count_added: bool,

    pub deadline: Option<Deadline>,
}

impl Lock {
    /// Creates a lock specifing all the required latches for a command.
    pub fn new<'a, I>(mut keyspace_id: u32, keys: I, deadline: Option<Deadline>) -> Lock
    where
        I: IntoIterator<Item = &'a Key>,
    {
        // Some clients are not setting keyspace_id in context correctly. So we check
        // and fix here.
        let mut is_first = true;
        let mut check_and_fix_keyspace_id = |key: &Key| {
            if is_first {
                is_first = false;
                let keyspace_id_of_key =
                    ApiV2::get_u32_keyspace_id_by_key(key.as_encoded()).unwrap_or_default();
                if keyspace_id != keyspace_id_of_key {
                    // TODO: change to warning after client-rust is fixed.
                    debug!(
                        "Lock::new: keyspace_id is different";
                        "req" => keyspace_id, "first key" => keyspace_id_of_key,
                    );
                    keyspace_id = keyspace_id_of_key;
                }
            }
        };

        // prevent from deadlock, so we sort and deduplicate the index
        let mut required_hashes: Vec<u64> = keys
            .into_iter()
            .map(|key| {
                check_and_fix_keyspace_id(key);
                Self::hash(key)
            })
            .collect();
        required_hashes.sort_unstable();
        required_hashes.dedup();
        Lock {
            required_hashes,
            keyspace_id,
            owned_count: 0,
            txn_file: None,
            region_id: 0,
            start_ts: 0,
            checked_txn_cid: 0,
            count_added: false,
            deadline,
        }
    }

    pub fn set_region_id_start_ts(&mut self, region_id: u64, start_ts: u64) {
        self.region_id = region_id;
        self.start_ts = start_ts;
    }

    pub fn hash(key: &Key) -> u64 {
        let raw_key = key.to_raw().unwrap();
        if let Some(keyspace_prefix) = ApiV2::get_keyspace_prefix(&raw_key) {
            farmhash::fingerprint64(&raw_key[keyspace_prefix.len()..])
        } else {
            farmhash::fingerprint64(&raw_key)
        }
    }

    /// Returns true if all the required latches have be acquired, false
    /// otherwise.
    pub fn acquired(&self) -> bool {
        self.required_hashes.len() == self.owned_count
    }

    /// Force set the state of the `Lock` to be already-acquired. Be careful
    /// when using it.
    pub fn force_assume_acquired(&mut self) {
        self.owned_count = self.required_hashes.len();
    }

    pub fn is_write_lock(&self) -> bool {
        !self.required_hashes.is_empty() || self.txn_file.is_some()
    }
}

/// Latches which are used for concurrency control in the scheduler.
///
/// Each latch is indexed by a slot ID, hence the term latch and slot are used
/// interchangeably, but conceptually a latch is a queue, and a slot is an index
/// to the queue.
pub struct Latches {
    slots: Vec<CachePadded<Mutex<Latch>>>,
    size: usize,
}

impl Latches {
    /// Creates latches.
    ///
    /// The size will be rounded up to the power of 2.
    pub fn new(size: usize) -> Latches {
        let size = usize::next_power_of_two(size);
        let mut slots = Vec::with_capacity(size);
        (0..size).for_each(|_| slots.push(Mutex::new(Latch::new()).into()));
        Latches { slots, size }
    }

    /// Tries to acquire the latches specified by the `lock` for command with ID
    /// `who`.
    ///
    /// This method will enqueue the command ID into the waiting queues of the
    /// latches. A latch is considered acquired if the command ID is the first
    /// one of elements in the queue which have the same hash value. Returns
    /// true if all the Latches are acquired, false otherwise.
    pub fn acquire(&self, lock: &mut Lock, who: u64) -> bool {
        let mut acquired_count: usize = 0;
        for &key_hash in &lock.required_hashes[lock.owned_count..] {
            let mut latch = self.lock_latch(lock.keyspace_id, key_hash);
            match latch.get_first_req_by_hash(lock.keyspace_id, key_hash) {
                Some(cid) => {
                    if cid == who {
                        acquired_count += 1;
                    } else {
                        latch.wait_for_wake(lock.keyspace_id, key_hash, who);
                        break;
                    }
                }
                None => {
                    latch.wait_for_wake(lock.keyspace_id, key_hash, who);
                    acquired_count += 1;
                }
            }
        }
        lock.owned_count += acquired_count;
        lock.acquired()
    }

    /// Releases all latches owned by the `lock` of command with ID `who`,
    /// returns the wakeup list.
    ///
    /// Optionally, this function can release partial of the given `Lock` while
    /// leaving the renaming unlocked, so that some of the latches can be
    /// used in another command. This can be done by passing the cid of the
    /// command who will use the kept latch slots later, and the `Lock` that
    /// need to be kept via the parameter `keep_latches_for_next_cmd`. Note
    /// that the lock in it is assumed to be a subset of the parameter
    /// `lock` which is going to be released.
    ///
    /// Preconditions: the caller must ensure the command is at the front of the
    /// latches.
    pub fn release(
        &self,
        lock: &Lock,
        who: u64,
        keep_latches_for_next_cmd: Option<(u64, &Lock)>,
    ) -> Vec<u64> {
        // Used to
        let dummy_vec = [];
        let (keep_latches_for_cid, mut keep_latches_it) = match keep_latches_for_next_cmd {
            Some((cid, lock)) => (Some(cid), lock.required_hashes.iter().peekable()),
            None => (None, dummy_vec.iter().peekable()),
        };

        // `keep_latches_it` must be sorted and deduped since it's retrieved from a
        // `Lock` object.

        let mut wakeup_list: Vec<u64> = vec![];
        for &key_hash in &lock.required_hashes[..lock.owned_count] {
            let mut latch = self.lock_latch(lock.keyspace_id, key_hash);
            let (ks, v, front) = latch.pop_front(lock.keyspace_id, key_hash).unwrap();
            assert_eq!(front, who);
            assert_eq!(v, key_hash);

            let keep_for_next_cmd = if let Some(&&next_keep_hash) = keep_latches_it.peek() {
                assert!(next_keep_hash >= key_hash);
                if next_keep_hash == key_hash {
                    keep_latches_it.next();
                    true
                } else {
                    false
                }
            } else {
                false
            };

            if !keep_for_next_cmd {
                if let Some(wakeup) = latch.get_first_req_by_hash(ks, key_hash) {
                    wakeup_list.push(wakeup);
                }
            } else {
                latch.push_preemptive(ks, key_hash, keep_latches_for_cid.unwrap());
            }
        }

        assert!(keep_latches_it.next().is_none());

        wakeup_list
    }

    #[inline]
    fn lock_latch(&self, keyspace_id: u32, hash: u64) -> MutexGuard<'_, Latch> {
        self.slots[(keyspace_id as usize ^ hash as usize) & (self.size - 1)].lock()
    }
}

#[cfg(test)]
mod tests {
    use std::iter::once;

    use super::*;

    #[test]
    fn test_wakeup() {
        let latches = Latches::new(256);

        let keys_a = ["k1", "k3", "k5"];
        let mut lock_a = Lock::new(0, strs_to_keys(&keys_a).iter(), None);
        let keys_b = ["k4", "k5", "k6"];
        let mut lock_b = Lock::new(0, strs_to_keys(&keys_b).iter(), None);
        let cid_a: u64 = 1;
        let cid_b: u64 = 2;

        // a acquire lock success
        let acquired_a = latches.acquire(&mut lock_a, cid_a);
        assert_eq!(acquired_a, true);

        // b acquire lock failed
        let mut acquired_b = latches.acquire(&mut lock_b, cid_b);
        assert_eq!(acquired_b, false);

        // a release lock, and get wakeup list
        let wakeup = latches.release(&lock_a, cid_a, None);
        assert_eq!(wakeup[0], cid_b);

        // b acquire lock success
        acquired_b = latches.acquire(&mut lock_b, cid_b);
        assert_eq!(acquired_b, true);
    }

    fn strs_to_keys(strs: &[&str]) -> Vec<Key> {
        strs.iter().map(|s| Key::from_raw(s.as_bytes())).collect()
    }

    fn bytes_to_keys(bins: &[&[u8]]) -> Vec<Key> {
        bins.iter().map(|bin| Key::from_raw(bin)).collect()
    }

    #[test]
    fn test_wakeup_by_multi_cmds() {
        let latches = Latches::new(256);

        let keys_a = ["k1", "k2", "k3"];
        let keys_b = ["k4", "k5", "k6"];
        let keys_c = ["k3", "k4"];
        let mut lock_a = Lock::new(0, strs_to_keys(&keys_a).iter(), None);
        let mut lock_b = Lock::new(0, strs_to_keys(&keys_b).iter(), None);
        let mut lock_c = Lock::new(0, strs_to_keys(&keys_c).iter(), None);
        let cid_a: u64 = 1;
        let cid_b: u64 = 2;
        let cid_c: u64 = 3;

        // a acquire lock success
        let acquired_a = latches.acquire(&mut lock_a, cid_a);
        assert_eq!(acquired_a, true);

        // b acquire lock success
        let acquired_b = latches.acquire(&mut lock_b, cid_b);
        assert_eq!(acquired_b, true);

        // c acquire lock failed, cause a occupied slot 3
        let mut acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, false);

        // b release lock, and get wakeup list
        let wakeup = latches.release(&lock_b, cid_b, None);
        assert_eq!(wakeup[0], cid_c);

        // c acquire lock failed again, cause a occupied slot 3
        acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, false);

        // a release lock, and get wakeup list
        let wakeup = latches.release(&lock_a, cid_a, None);
        assert_eq!(wakeup[0], cid_c);

        // finally c acquire lock success
        acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, true);
    }

    #[test]
    fn test_wakeup_by_small_latch_slot() {
        let latches = Latches::new(5);

        let keys_a = ["k1", "k2", "k3"];
        let keys_b = ["k6", "k7", "k8"];
        let keys_c = ["k3", "k4"];
        let keys_d = ["k7", "k10"];
        let mut lock_a = Lock::new(0, strs_to_keys(&keys_a).iter(), None);
        let mut lock_b = Lock::new(0, strs_to_keys(&keys_b).iter(), None);
        let mut lock_c = Lock::new(0, strs_to_keys(&keys_c).iter(), None);
        let mut lock_d = Lock::new(0, strs_to_keys(&keys_d).iter(), None);
        let cid_a: u64 = 1;
        let cid_b: u64 = 2;
        let cid_c: u64 = 3;
        let cid_d: u64 = 4;

        let acquired_a = latches.acquire(&mut lock_a, cid_a);
        assert_eq!(acquired_a, true);

        // c acquire lock failed, cause a occupied slot 3
        let mut acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, false);

        // b acquire lock success
        let acquired_b = latches.acquire(&mut lock_b, cid_b);
        assert_eq!(acquired_b, true);

        // d acquire lock failed, cause a occupied slot 7
        let mut acquired_d = latches.acquire(&mut lock_d, cid_d);
        assert_eq!(acquired_d, false);

        // a release lock, and get wakeup list
        let wakeup = latches.release(&lock_a, cid_a, None);
        assert_eq!(wakeup[0], cid_c);

        // c acquire lock success
        acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, true);

        // b release lock, and get wakeup list
        let wakeup = latches.release(&lock_b, cid_b, None);
        assert_eq!(wakeup[0], cid_d);

        // finally d acquire lock success
        acquired_d = latches.acquire(&mut lock_d, cid_d);
        assert_eq!(acquired_d, true);
    }

    fn check_latch_holder(latches: &Latches, key: &[u8], expected_holder_cid: Option<u64>) {
        let hash = farmhash::fingerprint64(key);
        let actual_holder = latches.lock_latch(0, hash).get_first_req_by_hash(0, hash);
        assert_eq!(actual_holder, expected_holder_cid);
    }

    fn is_latches_empty(latches: &Latches) -> bool {
        for i in 0..(latches.size as u64) {
            if !latches.lock_latch(0, i).waiting.iter().all(|x| x.is_none()) {
                return false;
            }
        }
        true
    }

    fn test_partially_releasing_impl(size: usize) {
        let latches = Latches::new(size);

        // Single key.
        let key = b"k1";
        let mut lock = Lock::new(0, once(&Key::from_raw(key)), None);
        assert!(latches.acquire(&mut lock, 1));
        assert!(!is_latches_empty(&latches));
        let mut lock2 = Lock::new(0, once(&Key::from_raw(key)), None);
        let wakeup = latches.release(&lock, 1, Some((2, &lock2)));
        assert!(wakeup.is_empty());
        check_latch_holder(&latches, key, Some(2));
        lock2.force_assume_acquired();
        let wakeup = latches.release(&lock2, 2, None);
        assert!(wakeup.is_empty());
        assert!(is_latches_empty(&latches));

        // Single key with queueing commands.
        let mut lock = Lock::new(0, once(&Key::from_raw(key)), None);
        let mut queueing_lock = Lock::new(0, once(&Key::from_raw(key)), None);
        assert!(latches.acquire(&mut lock, 3));
        assert!(!latches.acquire(&mut queueing_lock, 4));
        let mut lock2 = Lock::new(0, once(&Key::from_raw(key)), None);
        let wakeup = latches.release(&lock, 3, Some((5, &lock2)));
        assert!(wakeup.is_empty());
        check_latch_holder(&latches, key, Some(5));
        lock2.force_assume_acquired();
        let wakeup = latches.release(&lock2, 5, None);
        assert_eq!(wakeup, vec![4u64]);
        assert!(latches.acquire(&mut queueing_lock, 4));
        let wakeup = latches.release(&queueing_lock, 4, None);
        assert!(wakeup.is_empty());
        assert!(is_latches_empty(&latches));

        // Multi keys, keep all.
        let keys: Vec<&[u8]> = vec![b"k1", b"k2", b"k3", b"k4"];
        let mut lock = Lock::new(0, bytes_to_keys(&keys).iter(), None);
        assert!(latches.acquire(&mut lock, 11));
        let mut lock2 = Lock::new(0, bytes_to_keys(&keys).iter(), None);
        let wakeup = latches.release(&lock, 11, Some((12, &lock2)));
        assert!(wakeup.is_empty());
        for &key in &keys {
            check_latch_holder(&latches, key, Some(12));
        }
        assert!(!is_latches_empty(&latches));
        lock2.force_assume_acquired();
        let wakeup = latches.release(&lock2, 12, None);
        assert!(wakeup.is_empty());
        assert!(is_latches_empty(&latches));

        // Multi keys, keep all, with queueing command.
        let mut lock = Lock::new(0, bytes_to_keys(&keys).iter(), None);
        assert!(latches.acquire(&mut lock, 11));
        let mut queueing_locks: Vec<_> = bytes_to_keys(&keys)
            .iter()
            .map(|k| Lock::new(0, once(k), None))
            .collect();
        for (cid, lock) in (12..16).zip(queueing_locks.iter_mut()) {
            assert!(!latches.acquire(lock, cid));
        }
        let mut lock2 = Lock::new(0, bytes_to_keys(&keys).iter(), None);
        let wakeup = latches.release(&lock, 11, Some((17, &lock2)));
        assert!(wakeup.is_empty());
        for &key in &keys {
            check_latch_holder(&latches, key, Some(17));
        }
        assert!(!is_latches_empty(&latches));
        lock2.force_assume_acquired();
        let mut wakeup = latches.release(&lock2, 17, None);
        wakeup.sort_unstable();
        // Wake up queueing commands.
        assert_eq!(wakeup, vec![12u64, 13, 14, 15]);
        for (cid, mut lock) in (12..16).zip(queueing_locks) {
            assert!(latches.acquire(&mut lock, cid));
            let wakeup = latches.release(&lock, cid, None);
            assert!(wakeup.is_empty());
        }
        assert!(is_latches_empty(&latches));

        // 4 keys, keep 2 of them.
        for (i1, k1) in bytes_to_keys(&keys[0..3]).iter().enumerate() {
            for k2 in bytes_to_keys(&keys[i1 + 1..4]).iter() {
                let mut lock = Lock::new(0, bytes_to_keys(&keys).iter(), None);
                assert!(latches.acquire(&mut lock, 21));
                let mut lock2 = Lock::new(0, [k1.clone(), k2.clone()].iter(), None);
                let wakeup = latches.release(&lock, 21, Some((22, &lock2)));
                assert!(wakeup.is_empty());
                check_latch_holder(&latches, &k1.to_raw().unwrap(), Some(22));
                check_latch_holder(&latches, &k2.to_raw().unwrap(), Some(22));
                lock2.force_assume_acquired();
                let wakeup = latches.release(&lock2, 22, None);
                assert!(wakeup.is_empty());
                assert!(is_latches_empty(&latches));
            }
        }

        // 4 keys keep 2 of them, with queueing commands.
        for (i1, k1) in bytes_to_keys(&keys[0..3]).iter().enumerate() {
            for (i2, k2) in bytes_to_keys(&keys[i1 + 1..4]).iter().enumerate() {
                let mut lock = Lock::new(0, bytes_to_keys(&keys).iter(), None);
                assert!(latches.acquire(&mut lock, 21));

                let mut queueing_locks: Vec<_> = bytes_to_keys(&keys)
                    .iter()
                    .map(|k| Lock::new(0, once(k), None))
                    .collect();
                for (cid, lock) in (22..26).zip(queueing_locks.iter_mut()) {
                    assert!(!latches.acquire(lock, cid));
                }

                let mut lock2 = Lock::new(0, [k1.clone(), k2.clone()].iter(), None);
                let mut wakeup = latches.release(&lock, 21, Some((27, &lock2)));
                assert_eq!(wakeup.len(), 2);

                // The latch of k1 and k2 is preempted, and queueing locks on the other two keys
                // will be woken up.
                let preempted_cids = vec![(i1 + 22) as u64, (i1 + 1 + i2 + 22) as u64];
                let expected_wakeup_cids: Vec<_> = (22..26u64)
                    .filter(|x| !preempted_cids.contains(x))
                    .collect();
                wakeup.sort_unstable();
                assert_eq!(wakeup, expected_wakeup_cids);

                check_latch_holder(&latches, k1.to_raw().unwrap().as_slice(), Some(27));
                check_latch_holder(&latches, k2.to_raw().unwrap().as_slice(), Some(27));

                lock2.force_assume_acquired();
                let mut wakeup = latches.release(&lock2, 27, None);
                wakeup.sort_unstable();
                assert_eq!(wakeup, preempted_cids);

                for (cid, mut lock) in (22..26).zip(queueing_locks) {
                    assert!(latches.acquire(&mut lock, cid));
                    let wakeup = latches.release(&lock, cid, None);
                    assert!(wakeup.is_empty());
                }

                assert!(is_latches_empty(&latches));
            }
        }
    }

    #[test]
    fn test_partially_releasing() {
        test_partially_releasing_impl(256);
        test_partially_releasing_impl(4);
        test_partially_releasing_impl(2);
    }
}
