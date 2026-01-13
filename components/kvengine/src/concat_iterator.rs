// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt, iter::Iterator as _};

use crate::{
    LevelHandler, next, next_async, next_version, next_version_async,
    table::{
        InnerKey, Iterator, Value, search,
        sstable::{SsTable, TableIterator},
    },
};

// ConcatIterator concatenates the sequences defined by several iterators.  (It
// only works with TableIterators, probably just because it's faster to not be
// so generic.)
pub(crate) struct ConcatIterator {
    // Should always use `set_idx` to set `idx` & `iter`.
    idx: i32,
    iter: Option<Box<TableIterator>>,

    level: LevelHandler,
    reversed: bool,
    fill_cache: bool,
    is_tables_sync: bool,
}

impl fmt::Debug for ConcatIterator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConcatIterator")
            .field("idx", &self.idx)
            .field("iter", &self.iter)
            .field("reversed", &self.reversed)
            .finish()
    }
}

impl ConcatIterator {
    pub(crate) fn new(level: LevelHandler, reversed: bool, fill_cache: bool) -> Self {
        let is_tables_sync = level.tables.iter().all(|f| f.is_sync());
        ConcatIterator {
            idx: -1,
            iter: None,
            level,
            reversed,
            fill_cache,
            is_tables_sync,
        }
    }

    pub(crate) fn new_with_tables(tables: Vec<SsTable>, reversed: bool, fill_cache: bool) -> Self {
        let level = LevelHandler::new(1, tables);
        let is_tables_sync = level.tables.iter().all(|f| f.is_sync());
        ConcatIterator {
            idx: -1,
            iter: None,
            level,
            reversed,
            fill_cache,
            is_tables_sync,
        }
    }

    fn get_table(&self, idx: usize) -> &SsTable {
        &self.level.tables[idx]
    }

    fn num_tables(&self) -> usize {
        self.level.tables.len()
    }

    #[maybe_async::both]
    async fn set_idx(&mut self, idx: i32) {
        if self.idx == idx {
            return;
        }
        self.idx = idx;
        if idx < 0 || idx as usize >= self.num_tables() {
            self.iter = None;
        } else {
            let mut iter = self
                .get_table(idx as usize)
                .new_iterator(self.reversed, self.fill_cache);
            iter.rewind().await;
            self.iter = Some(iter);
        }
    }
}

#[maybe_async::async_trait]
impl Iterator for ConcatIterator {
    #[maybe_async]
    async fn next(&mut self) {
        if self.iter.is_none() {
            return;
        }
        if let Some(iter) = &mut self.iter {
            next!(iter).await;
            if iter.valid() {
                return;
            }
        }
        loop {
            if !self.reversed {
                self.set_idx(self.idx + 1).await;
            } else {
                self.set_idx(self.idx - 1).await;
            }
            match &mut self.iter {
                None => return,
                Some(iter) => {
                    iter.rewind().await;
                    if iter.valid() {
                        return;
                    }
                }
            }
        }
    }

    fn is_next_sync(&self) -> bool {
        if self.is_tables_sync {
            return true;
        }
        // Correctness depends on the assumption that when `self.iter.is_next_sync()` is
        // true, the `self.iter` must have more data in current block, so the
        // `self.iter.next()` must be valid.
        self.iter.as_ref().is_none_or(|iter| iter.is_next_sync())
    }

    #[maybe_async]
    async fn next_version(&mut self) -> bool {
        next_version!(self.iter.as_mut().unwrap()).await
    }

    fn is_next_version_sync(&self) -> bool {
        if self.is_tables_sync {
            return true;
        }
        self.iter.as_ref().unwrap().is_next_version_sync()
    }

    #[maybe_async]
    async fn rewind(&mut self) {
        if self.num_tables() == 0 {
            return;
        }
        if !self.reversed {
            self.set_idx(0).await;
        } else {
            self.set_idx(self.num_tables() as i32 - 1).await;
        }
        self.iter.as_mut().unwrap().rewind().await;
    }

    #[maybe_async]
    async fn seek(&mut self, key: InnerKey<'_>) {
        use std::cmp::Ordering::*;
        let idx = if !self.reversed {
            search(self.num_tables(), |idx| {
                self.get_table(idx).biggest().cmp(&key) != Less
            }) as i32
        } else {
            let n = self.num_tables();
            let ridx = search(n, |idx| {
                self.get_table(n - 1 - idx).smallest().cmp(&key) != Greater
            }) as i32;
            n as i32 - 1 - ridx
        };
        self.set_idx(idx).await;
        if let Some(iter) = self.iter.as_mut() {
            iter.seek(key).await;
        }
    }

    fn key(&self) -> InnerKey<'_> {
        self.iter.as_ref().unwrap().key()
    }

    fn value(&self) -> Value {
        self.iter.as_ref().unwrap().value()
    }

    fn valid(&self) -> bool {
        match &self.iter {
            Some(x) => x.valid(),
            None => false,
        }
    }

    #[cfg(debug_assertions)]
    fn rewind_and_dump(&mut self) -> Vec<(String, Vec<crate::table::DumpKv>)> {
        let mut kvs = vec![];
        for idx in 0..self.num_tables() {
            self.set_idx(idx as i32);
            kvs.extend(self.iter.as_mut().unwrap().rewind_and_dump());
        }
        kvs
    }

    #[cfg(debug_assertions)]
    fn tag(&self) -> String {
        "concat".to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::{iter::Iterator as StdIterator, ops::Deref};

    use futures::executor::block_on;
    use proptest::{prelude::*, test_runner::TestCaseResult};
    use rstest::rstest;

    use crate::{
        concat_iterator::ConcatIterator,
        next, next_async, next_version, next_version_async,
        table::{
            InnerKey, Iterator,
            sstable::{SsTable, test_util::*},
        },
    };

    #[maybe_async::test]
    async fn test_concat_iterator_one_table() {
        let t = build_test_table_with_kvs(&vec![
            ("k1".to_string(), "a1".to_string()),
            ("k2".to_string(), "a2".to_string()),
        ])
        .await;
        let tables = vec![t];
        let mut it = ConcatIterator::new_with_tables(tables, false, true);
        it.rewind().await;
        assert_eq!(it.valid(), true);
        assert_eq!(it.key().deref(), "k1".as_bytes());
        let v = it.value();
        assert_eq!(v.get_value(), "a1".as_bytes());
    }

    #[maybe_async::test]
    async fn test_concat_iterator() {
        let (t1, _) = build_test_table_with_prefix("keya", 10000).await;
        let (t2, _) = build_test_table_with_prefix("keyb", 10000).await;
        let (t3, _) = build_test_table_with_prefix("keyc", 10000).await;
        let tables = vec![t1, t2, t3];
        {
            let mut it = ConcatIterator::new_with_tables(tables.clone(), false, true);
            it.rewind().await;
            assert_eq!(it.valid(), true);
            let mut cnt = 0;
            while it.valid() {
                let v = it.value();
                assert_eq!(v.get_value(), get_test_value(cnt % 10000).as_bytes());
                cnt += 1;
                next!(it).await;
            }
            assert_eq!(cnt, 30000);
            it.seek(InnerKey::from_inner_buf("a".as_bytes())).await;
            assert_eq!(it.key().deref(), "keya0000".as_bytes());
            assert_eq!(it.value().get_value(), get_test_value(0).as_bytes());

            it.seek(InnerKey::from_inner_buf("keyb".as_bytes())).await;
            assert_eq!(it.key().deref(), "keyb0000".as_bytes());
            assert_eq!(it.value().get_value(), get_test_value(0).as_bytes());

            it.seek(InnerKey::from_inner_buf("keyb9999b".as_bytes()))
                .await;
            assert_eq!(it.key().deref(), "keyc0000".as_bytes());
            assert_eq!(it.value().get_value(), get_test_value(0).as_bytes());

            it.seek(InnerKey::from_inner_buf("keyd".as_bytes())).await;
            assert_eq!(it.valid(), false);

            it.seek(InnerKey::from_inner_buf("keyb9999b".as_bytes()))
                .await;
            assert_eq!(it.key().deref(), "keyc0000".as_bytes());
            assert_eq!(it.value().get_value(), get_test_value(0).as_bytes());
        }
        {
            let mut it = ConcatIterator::new_with_tables(tables, true, true);
            it.rewind().await;
            assert_eq!(it.valid(), true);
            let mut cnt = 0;
            while it.valid() {
                let v = it.value();
                assert_eq!(
                    v.get_value(),
                    get_test_value(10000 - (cnt % 10000) - 1).as_bytes()
                );
                cnt += 1;
                next!(it).await;
            }
            assert_eq!(cnt, 30000);

            it.seek(InnerKey::from_inner_buf("a".as_bytes())).await;
            assert_eq!(it.valid(), false);

            it.seek(InnerKey::from_inner_buf("keyb".as_bytes())).await;
            assert_eq!(it.key().deref(), "keya9999".as_bytes());
            assert_eq!(it.value().get_value(), get_test_value(9999).as_bytes());

            it.seek(InnerKey::from_inner_buf("keyb9999b".as_bytes()))
                .await;
            assert_eq!(it.key().deref(), "keyb9999".as_bytes());
            assert_eq!(it.value().get_value(), get_test_value(9999).as_bytes());

            it.seek(InnerKey::from_inner_buf("keyd".as_bytes())).await;
            assert_eq!(it.key().deref(), "keyc9999".as_bytes());
            assert_eq!(it.value().get_value(), get_test_value(9999).as_bytes());
        }
    }

    /// Generate maximum `max_vecs` number of vectors, each with maximum
    /// `max_kvs` number of key-values.
    ///
    /// Given there are 10 kvs totally, the generated keys will be like:
    /// `xkey0001`, `xkey0003`, ... `xkey0019`
    fn arb_kvs_vec(
        max_vecs: usize,
        max_kvs: usize,
    ) -> impl Strategy<Value = Vec<Vec<(String, String)>>> {
        prop::collection::vec(1..=max_kvs, 1..max_vecs).prop_map(move |rows| {
            let mut kvs_vec = Vec::with_capacity(rows.len());
            let mut i = 1;
            for row in rows {
                let mut kvs = Vec::with_capacity(row);
                for _ in 0..row {
                    let key = get_test_key("xkey", i);
                    let val = get_test_value(i);
                    kvs.push((key, val));
                    i += 2;
                }
                kvs_vec.push(kvs);
            }
            kvs_vec
        })
    }

    /// Generate maximum `max_tables` number of SsTables, each with maximum
    /// `max_rows` number of rows.
    fn arb_sstables(
        max_tables: usize,
        max_rows: usize,
    ) -> impl Strategy<
        Value = (
            Vec<SsTable>,
            Vec<usize>, // vers_cnt
            Vec<(String, String)>,
        ),
    > {
        arb_kvs_vec(max_tables, max_rows).prop_map(move |kvs_vec| {
            let mut total_kvs = Vec::with_capacity(kvs_vec.iter().map(|x| x.len()).sum());
            let mut tables = Vec::with_capacity(kvs_vec.len());
            let mut vers_cnt = Vec::with_capacity(kvs_vec.len());
            for kvs in kvs_vec {
                let (t, ver_cnt) = create_multi_version_sst(&kvs);
                tables.push(t);
                vers_cnt.push(ver_cnt);
                total_kvs.extend(kvs);
            }
            (tables, vers_cnt, total_kvs)
        })
    }

    fn arb_async_sstables(
        max_tables: usize,
        max_rows: usize,
    ) -> impl Strategy<
        Value = (
            Vec<SsTable>,
            Vec<usize>, // vers_cnt
            Vec<(String, String)>,
        ),
    > {
        arb_kvs_vec(max_tables, max_rows).prop_map(move |kvs_vec| {
            let mut total_kvs = Vec::with_capacity(kvs_vec.iter().map(|x| x.len()).sum());
            let mut tables = Vec::with_capacity(kvs_vec.len());
            let mut vers_cnt = Vec::with_capacity(kvs_vec.len());
            for kvs in kvs_vec {
                let (t, ver_cnt) = block_on(create_multi_version_sst_async(&kvs));
                tables.push(t);
                vers_cnt.push(ver_cnt);
                total_kvs.extend(kvs);
            }
            (tables, vers_cnt, total_kvs)
        })
    }

    #[rstest]
    #[case(false)]
    #[case::reverse(true)]
    fn test_concat_iterator_arb(#[case] reverse: bool) {
        proptest!(|((tables, vers_cnt, total_kvs) in arb_sstables(5, 10), seek_keys in prop::collection::vec(0usize..=100, 20))| {
            test_concat_iterator_arb_impl(tables, vers_cnt, total_kvs, reverse, seek_keys)?;
        })
    }

    #[rstest]
    #[case(false)]
    #[case::reverse(true)]
    fn test_concat_iterator_arb_async(#[case] reverse: bool) {
        proptest!(|((tables, vers_cnt, total_kvs) in arb_async_sstables(5, 10), seek_keys in prop::collection::vec(0usize..=100, 20))| {
            block_on(test_concat_iterator_arb_impl_async(tables, vers_cnt, total_kvs, reverse, seek_keys))?;
        })
    }

    #[maybe_async::both]
    async fn test_concat_iterator_arb_impl(
        tables: Vec<SsTable>,
        vers_cnt: Vec<usize>,
        total_kvs: Vec<(String, String)>,
        reverse: bool,
        seek_keys: Vec<usize>,
    ) -> TestCaseResult {
        let mut it = ConcatIterator::new_with_tables(tables.clone(), reverse, true);
        let total_vers = vers_cnt.iter().sum::<usize>();

        let first_pos = if !reverse { 0 } else { total_kvs.len() };

        it.rewind().await;
        verify_next(&mut it, first_pos, &total_kvs, reverse).await?;

        it.rewind().await;
        verify_next_version(&mut it, first_pos, &total_kvs, reverse, Some(total_vers)).await?;

        for seek_only in [false, true] {
            for &i in &seek_keys {
                let key = get_test_key("xkey", i);
                it.seek(InnerKey::from_inner_buf(key.as_bytes())).await;

                let ref_pos = match total_kvs.binary_search_by(|(k, _)| k.cmp(&key)) {
                    Ok(pos) => {
                        if !reverse {
                            pos
                        } else {
                            pos + 1
                        }
                    }
                    Err(pos) => pos,
                };

                if seek_only {
                    // For https://github.com/tidbcloud/cloud-storage-engine/pull/1956. Which only happens without `next`.
                    verify_valid(&it, ref_pos, &total_kvs, reverse)?;
                } else {
                    verify_next(&mut it, ref_pos, &total_kvs, reverse).await?;

                    it.seek(InnerKey::from_inner_buf(key.as_bytes())).await;
                    verify_next_version(&mut it, ref_pos, &total_kvs, reverse, None).await?;
                }
            }
        }
        Ok(())
    }

    // NOTE: the `ref_pos` is exclusive for reverse iterator, to avoid to handle
    // negative `ref_pos`.
    #[maybe_async::both]
    async fn verify_next(
        it: &mut ConcatIterator,
        ref_pos: usize,
        total_kvs: &[(String, String)],
        reverse: bool,
    ) -> TestCaseResult {
        let rng: Box<dyn StdIterator<Item = usize>> = if !reverse {
            Box::new(ref_pos..total_kvs.len()) as _
        } else {
            Box::new((0..ref_pos).rev()) as _
        };
        for pos in rng {
            let (ref_key, ref_val) = &total_kvs[pos];
            let k = it.key();
            let v = it.value();

            prop_assert!(it.valid());
            prop_assert_eq!(k, InnerKey::from_inner_buf(ref_key.as_bytes()));
            prop_assert_eq!(v.get_value(), ref_val.as_bytes());

            next!(it).await;
        }
        prop_assert!(!it.valid(), "{:?}", it);
        Ok(())
    }

    // NOTE: the `ref_pos` is exclusive for reverse iterator, to avoid to handle
    // negative `ref_pos`.
    #[maybe_async::both]
    async fn verify_next_version(
        it: &mut ConcatIterator,
        ref_pos: usize,
        total_kvs: &[(String, String)],
        reverse: bool,
        expect_total_vers: Option<usize>,
    ) -> TestCaseResult {
        let rng: Box<dyn StdIterator<Item = usize>> = if !reverse {
            Box::new(ref_pos..total_kvs.len()) as _
        } else {
            Box::new((0..ref_pos).rev()) as _
        };
        let mut total_vers = 0;
        for pos in rng {
            let (ref_key, ref_val) = &total_kvs[pos];
            let k = it.key();
            let v = it.value();

            prop_assert!(it.valid());
            prop_assert_eq!(k, InnerKey::from_inner_buf(ref_key.as_bytes()));
            prop_assert_eq!(v.get_value(), ref_val.as_bytes());
            total_vers += 1;

            while next_version!(it).await {
                let k = it.key();
                let v = it.value();

                prop_assert_eq!(k, InnerKey::from_inner_buf(ref_key.as_bytes()));
                let ver = v.version;
                let expect_val = format!("{}_{}", ref_val, ver);
                prop_assert_eq!(v.get_value(), expect_val.as_bytes());
                total_vers += 1;
            }

            next!(it).await;
        }
        prop_assert!(!it.valid(), "{:?}", it);

        if let Some(expect_total_vers) = expect_total_vers {
            prop_assert_eq!(expect_total_vers, total_vers);
        }

        Ok(())
    }

    // The `ref_pos` is exclusive for reverse iterator.
    fn verify_valid(
        it: &ConcatIterator,
        ref_pos: usize,
        total_kvs: &[(String, String)],
        reverse: bool,
    ) -> TestCaseResult {
        let expect_valid = if !reverse {
            ref_pos < total_kvs.len()
        } else {
            ref_pos > 0
        };
        prop_assert_eq!(it.valid(), expect_valid);
        Ok(())
    }
}
