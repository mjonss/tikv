// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, collections::VecDeque, mem};

/// MergeRanges is used to record the successful ranges to reduce unnecessary
/// retry.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct MergeRanges {
    ranges: VecDeque<Range>,
}

type Range = std::ops::Range<Vec<u8>>;

impl MergeRanges {
    #[cfg(test)]
    pub fn new(start: Vec<u8>, end: Vec<u8>) -> Self {
        let mut mr = Self::default();
        mr.ranges.push_back(start..end);
        mr
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Search range by `range.start`.
    /// Return:
    /// - OK(idx) when `ranges[idx].start == key`.
    /// - Err(idx) when not found, and `idx` is the index to insert. Simple
    ///   iteration is used as length of ranges is expected to be small, and
    ///   binary search is not efficient.
    fn search_by_start(&self, key: &Vec<u8>) -> std::result::Result<usize, usize> {
        for i in 0..self.ranges.len() {
            match self.ranges[i].start.cmp(key) {
                cmp::Ordering::Equal => return Ok(i),
                cmp::Ordering::Greater => return Err(i),
                cmp::Ordering::Less => continue,
            }
        }
        Err(self.ranges.len())
    }

    /// `search_overlapped` return the index of overlapped range for `key`.
    /// range.start <= key <= range.end.
    /// If not found, return the index where `key` should be inserted.
    fn search_overlapped(&self, key: &Vec<u8>) -> std::result::Result<usize, usize> {
        // Search range.start <= key.
        match self.search_by_start(key) {
            Ok(idx) => Ok(idx),
            Err(idx) if idx == 0 => Err(idx),
            Err(idx) => {
                let left = idx - 1;
                if key <= &self.ranges[left].end {
                    Ok(left)
                } else {
                    Err(idx)
                }
            }
        }
    }

    pub fn covered(&self, start: &Vec<u8>, end: &Vec<u8>) -> bool {
        match self.search_overlapped(start) {
            Ok(idx) => end <= &self.ranges[idx].end,
            Err(_) => false,
        }
    }

    pub fn insert(&mut self, mut start: Vec<u8>, mut end: Vec<u8>) {
        let left = self.search_overlapped(&start);
        if let Ok(idx) = left {
            if end <= self.ranges[idx].end {
                return;
            }
        }

        let right = self.search_overlapped(&end);

        if let Ok(ref left) = left {
            start = mem::take(&mut self.ranges[*left].start);
        }
        if let Ok(ref right) = right {
            end = mem::take(&mut self.ranges[*right].end);
        }

        // Remove overlapped.
        let remove_left = left.unwrap_or_else(|idx| idx);
        let remove_right = right.map(|idx| idx + 1).unwrap_or_else(|idx| idx);
        self.ranges.drain(remove_left..remove_right);

        self.ranges.insert(remove_left, start..end);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_ranges() {
        let i_to_key = |i: u64| format!("{:04}", i).into_bytes();
        let new_range = |i: u64, j: u64| MergeRanges::new(i_to_key(i), i_to_key(j));
        let new_ranges = |ranges: &[(u64, u64)]| {
            let mut mr = MergeRanges::default();
            for &(i, j) in ranges {
                mr.insert(i_to_key(i), i_to_key(j));
            }
            mr
        };
        let insert = |mr: &mut MergeRanges, i: u64, j: u64| {
            mr.insert(i_to_key(i), i_to_key(j));
        };
        let covered = |mr: &MergeRanges, i: u64, j: u64| mr.covered(&i_to_key(i), &i_to_key(j));

        // test `search_overlapped`
        {
            let mut mr = MergeRanges::default();
            assert_eq!(mr.search_overlapped(&i_to_key(0)), Err(0));

            insert(&mut mr, 0, 1);
            assert_eq!(mr.search_overlapped(&i_to_key(0)), Ok(0));
            assert_eq!(mr.search_overlapped(&i_to_key(1)), Ok(0));
            assert_eq!(mr.search_overlapped(&i_to_key(2)), Err(1));

            insert(&mut mr, 3, 5);
            assert_eq!(mr.search_overlapped(&i_to_key(0)), Ok(0));
            assert_eq!(mr.search_overlapped(&i_to_key(1)), Ok(0));
            assert_eq!(mr.search_overlapped(&i_to_key(2)), Err(1));
            assert_eq!(mr.search_overlapped(&i_to_key(3)), Ok(1));
            assert_eq!(mr.search_overlapped(&i_to_key(4)), Ok(1));
            assert_eq!(mr.search_overlapped(&i_to_key(5)), Ok(1));
            assert_eq!(mr.search_overlapped(&i_to_key(6)), Err(2));
        }

        // test `covered`
        {
            let mut mr = MergeRanges::default();
            assert!(!covered(&mr, 0, 1));

            insert(&mut mr, 0, 1);
            assert!(covered(&mr, 0, 1));
            assert!(!covered(&mr, 1, 2));

            insert(&mut mr, 2, 4);
            assert!(covered(&mr, 2, 3));
            assert!(covered(&mr, 2, 4));
            assert!(!covered(&mr, 1, 2));
            assert!(!covered(&mr, 1, 3));
            assert!(!covered(&mr, 1, 4));
            assert!(!covered(&mr, 1, 5));
            assert!(!covered(&mr, 2, 5));
            assert!(!covered(&mr, 4, 5));
            assert!(!covered(&mr, 0, 4));
        }

        // test `insert`
        {
            let mut mr = MergeRanges::default();
            insert(&mut mr, 2, 3);
            assert_eq!(mr, new_range(2, 3));
            insert(&mut mr, 3, 4);
            assert_eq!(mr, new_range(2, 4));

            insert(&mut mr, 0, 1);
            assert_eq!(mr, new_ranges(&[(0, 1), (2, 4)]));
            insert(&mut mr, 1, 2);
            assert_eq!(mr, new_range(0, 4));

            insert(&mut mr, 5, 6);
            assert_eq!(mr, new_ranges(&[(0, 4), (5, 6)]));
            insert(&mut mr, 4, 5);
            assert_eq!(mr, new_range(0, 6));

            insert(&mut mr, 10, 11);
            assert_eq!(mr, new_ranges(&[(0, 6), (10, 11)]));
            insert(&mut mr, 3, 12);
            assert_eq!(mr, new_range(0, 12));

            assert!(covered(&mr, 0, 12));
        }

        // test merge overlapping ranges
        {
            let mut mr = new_range(2, 12);
            insert(&mut mr, 2, 6);
            assert_eq!(mr, new_range(2, 12));

            insert(&mut mr, 3, 8);
            assert_eq!(mr, new_range(2, 12));

            insert(&mut mr, 6, 12);
            assert_eq!(mr, new_range(2, 12));

            insert(&mut mr, 1, 4);
            assert_eq!(mr, new_range(1, 12));

            insert(&mut mr, 4, 15);
            assert_eq!(mr, new_range(1, 15));

            insert(&mut mr, 20, 21);
            insert(&mut mr, 22, 23);
            insert(&mut mr, 24, 25);
            insert(&mut mr, 14, 30);
            assert_eq!(mr, new_range(1, 30));

            insert(&mut mr, 40, 41);
            insert(&mut mr, 42, 43);
            insert(&mut mr, 50, 51);
            insert(&mut mr, 53, 54);
            insert(&mut mr, 0, 42);
            assert_eq!(mr, new_ranges(&[(0, 43), (50, 51), (53, 54)]));
        }
    }
}
