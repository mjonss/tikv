// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::Iterator as _, ops::Deref};

use bytes::{Buf, Bytes};
use futures::executor::block_on;
use proptest::prelude::*;
use rand::prelude::*;
use rstest::rstest;

use super::{
    ArbSimpleIterators, SimpleIterator, arb_simple_iterators, get_all, get_all_async, i_to_key,
    verify_iter, verify_iter_async,
};
use crate::{next, next_async, next_version, next_version_async, table::*};

#[maybe_async::test]
async fn test_merge_single() {
    let keys = vec!["1", "2", "3"];
    let vals = vec!["v1", "v2", "v3"];
    let it = SimpleIterator::new(keys.clone(), vals.clone(), false, 1).await;
    let mut merge_it = new_merge_iterator(vec![Box::new(it)], false).await;
    merge_it.rewind().await;
    let (n_keys, n_vals) = get_all(merge_it.as_mut()).await;
    for i in 0..keys.len() {
        assert_eq!(keys[i].as_bytes(), n_keys[i]);
        assert_eq!(vals[i].as_bytes(), n_vals[i]);
    }
}

#[maybe_async::test]
async fn test_merge_single_reversed() {
    let keys = vec!["1", "2", "3"];
    let vals = vec!["v1", "v2", "v3"];
    let it = SimpleIterator::new(keys.clone(), vals.clone(), true, 1).await;
    let mut merge_it = new_merge_iterator(vec![Box::new(it)], true).await;
    merge_it.rewind().await;
    let (n_keys, n_vals) = get_all(merge_it.as_mut()).await;
    for i in 0..keys.len() {
        let reverse_idx = keys.len() - 1 - i;
        assert_eq!(keys[reverse_idx].as_bytes(), n_keys[i]);
        assert_eq!(vals[reverse_idx].as_bytes(), n_vals[i]);
    }
}

#[maybe_async::test]
async fn test_merge_more() {
    let it1 =
        Box::new(SimpleIterator::new(vec!["1", "3", "7"], vec!["a1", "a3", "a7"], false, 10).await);
    let it2 =
        Box::new(SimpleIterator::new(vec!["2", "3", "5"], vec!["b2", "b3", "b5"], false, 9).await);
    let it3 = Box::new(SimpleIterator::new(vec!["1"], vec!["c1"], false, 8).await);
    let it4 =
        Box::new(SimpleIterator::new(vec!["1", "7", "9"], vec!["d1", "d7", "d9"], false, 7).await);
    let mut merge_it = new_merge_iterator(vec![it1, it2, it3, it4], false).await;
    let expected_keys = ["1", "2", "3", "5", "7", "9"];
    let expected_vals = ["a1", "b2", "a3", "b5", "a7", "d9"];
    merge_it.rewind().await;
    let (keys, vals) = get_all(merge_it.as_mut()).await;
    for i in 0..expected_keys.len() {
        assert_eq!(expected_keys[i].as_bytes(), keys[i]);
        assert_eq!(expected_vals[i].as_bytes(), vals[i]);
    }
}

#[maybe_async::test]
async fn test_merge_iterator_nested() {
    let keys = vec!["1", "2", "3"];
    let vals = vec!["v1", "v2", "v3"];
    let it = Box::new(SimpleIterator::new(keys.clone(), vals.clone(), false, 1).await);
    let merge1 = new_merge_iterator(vec![it], false).await;
    let mut merge2 = new_merge_iterator(vec![merge1], false).await;
    merge2.rewind().await;
    let (n_keys, n_vals) = get_all(merge2.as_mut()).await;
    for i in 0..keys.len() {
        assert_eq!(keys[i].as_bytes(), n_keys[i]);
        assert_eq!(vals[i].as_bytes(), n_vals[i])
    }
}

#[maybe_async::test]
async fn test_merge_iterator_seek() {
    let it1 =
        Box::new(SimpleIterator::new(vec!["1", "3", "7"], vec!["a1", "a3", "a7"], false, 9).await);
    let it2 =
        Box::new(SimpleIterator::new(vec!["2", "3", "5"], vec!["b2", "b3", "b5"], false, 8).await);
    let it3 = Box::new(SimpleIterator::new(vec!["1"], vec!["c1"], false, 7).await);
    let it4 =
        Box::new(SimpleIterator::new(vec!["1", "7", "9"], vec!["d1", "d7", "d9"], false, 6).await);
    let mut merge_it = new_merge_iterator(vec![it1, it2, it3, it4], false).await;
    merge_it
        .seek(InnerKey::from_inner_buf("4".as_bytes()))
        .await;
    let (keys, vals) = get_all(merge_it.as_mut()).await;
    let expected_keys = ["5", "7", "9"];
    let expected_vals = ["b5", "a7", "d9"];
    for i in 0..expected_keys.len() {
        assert_eq!(expected_keys[i].as_bytes(), keys[i]);
        assert_eq!(expected_vals[i].as_bytes(), vals[i]);
    }
}

#[maybe_async::test]
async fn test_merge_iterator_seek_reversed() {
    let it1 =
        Box::new(SimpleIterator::new(vec!["1", "3", "7"], vec!["a1", "a3", "a7"], true, 9).await);
    let it2 =
        Box::new(SimpleIterator::new(vec!["2", "3", "5"], vec!["b2", "b3", "b5"], true, 8).await);
    let it3 = Box::new(SimpleIterator::new(vec!["1"], vec!["c1"], true, 7).await);
    let it4 =
        Box::new(SimpleIterator::new(vec!["1", "7", "9"], vec!["d1", "d7", "d9"], true, 6).await);
    let mut merge_it = new_merge_iterator(vec![it1, it2, it3, it4], true).await;
    merge_it
        .seek(InnerKey::from_inner_buf("5".as_bytes()))
        .await;
    let (keys, vals) = get_all(merge_it.as_mut()).await;
    let expected_keys = ["5", "3", "2", "1"];
    let expected_vals = ["b5", "a3", "b2", "a1"];
    for i in 0..expected_keys.len() {
        assert_eq!(expected_keys[i].as_bytes(), keys[i]);
        assert_eq!(expected_vals[i].as_bytes(), vals[i]);
    }
}

#[maybe_async::test]
async fn test_merge_iterator_seek_invalid() {
    let it1 =
        Box::new(SimpleIterator::new(vec!["1", "3", "7"], vec!["a1", "a3", "a7"], false, 9).await);
    let it2 =
        Box::new(SimpleIterator::new(vec!["2", "3", "5"], vec!["b2", "b3", "b5"], false, 8).await);
    let it3 = Box::new(SimpleIterator::new(vec!["1"], vec!["c1"], false, 7).await);
    let it4 =
        Box::new(SimpleIterator::new(vec!["1", "7", "9"], vec!["d1", "d7", "d9"], false, 6).await);
    let mut merge_it = new_merge_iterator(vec![it1, it2, it3, it4], false).await;
    merge_it
        .seek(InnerKey::from_inner_buf("f".as_bytes()))
        .await;
    assert!(!merge_it.valid());
}

#[maybe_async::test]
async fn test_merge_iterator_seek_invalid_reversed() {
    let it1 =
        Box::new(SimpleIterator::new(vec!["1", "3", "7"], vec!["a1", "a3", "a7"], true, 9).await);
    let it2 =
        Box::new(SimpleIterator::new(vec!["2", "3", "5"], vec!["b2", "b3", "b5"], true, 8).await);
    let it3 = Box::new(SimpleIterator::new(vec!["1"], vec!["c1"], true, 7).await);
    let it4 =
        Box::new(SimpleIterator::new(vec!["1", "7", "9"], vec!["d1", "d7", "d9"], true, 6).await);
    let mut merge_it = new_merge_iterator(vec![it1, it2, it3, it4], true).await;
    merge_it
        .seek(InnerKey::from_inner_buf("0".as_bytes()))
        .await;
    assert!(!merge_it.valid());
}

#[maybe_async::test]
async fn merge_iterator_duplicated() {
    let it1 =
        Box::new(SimpleIterator::new(vec!["0", "1", "2"], vec!["0", "1", "2"], false, 9).await);
    let it2 = Box::new(SimpleIterator::new(vec!["1"], vec!["1"], false, 8).await);
    let it3 = Box::new(SimpleIterator::new(vec!["2"], vec!["2"], false, 7).await);
    let mut merge_it = new_merge_iterator(vec![it1, it2, it3], false).await;
    merge_it.rewind().await;
    let mut cnt = 0;
    while merge_it.valid() {
        assert_eq!(cnt + 48, merge_it.key()[0] as i32);
        cnt += 1;
        next!(merge_it).await;
    }
    assert_eq!(cnt, 3);
}

#[maybe_async::test]
async fn test_multi_version_merge_iterator() {
    let mut rnd = rand::thread_rng();
    for &reversed in &[false, true] {
        let it1 = Box::new(SimpleIterator::new_multi_version(100, 90, reversed).await);
        let it2 = Box::new(SimpleIterator::new_multi_version(90, 80, reversed).await);
        let it3 = Box::new(SimpleIterator::new_multi_version(80, 70, reversed).await);
        let it4 = Box::new(SimpleIterator::new_multi_version(70, 60, reversed).await);
        let mut it = new_merge_iterator(vec![it1, it2, it3, it4], reversed).await;
        it.rewind();
        let mut cur_key = Bytes::copy_from_slice(it.key().deref());
        for _ in 1..100 {
            next!(it).await;
            assert_eq!(it.valid(), true);
            assert_ne!(cur_key.chunk(), it.key().deref());
            cur_key = Bytes::copy_from_slice(it.key().deref());
            let cur_ver = it.value().version;
            while next_version!(it).await {
                assert_eq!(it.value().version < cur_ver, true);
            }
        }
        for _ in 0..100 {
            let key = Bytes::from(format!("key{:03}", rnd.gen_range::<u8, _>(0..100)));
            it.seek(InnerKey::from_inner_buf(&key));
            assert_eq!(it.valid(), true);
            assert_eq!(it.key().deref(), key.chunk());
            let mut cur_ver = it.value().version;
            while next_version!(it).await {
                assert_eq!(it.value().version < cur_ver, true);
                cur_ver = it.value().version;
            }
            assert_eq!(cur_ver <= 70, true);
        }
    }
}

#[rstest]
#[case::merge_iter(true, false)]
#[case::merge_iter_reverse(true, true)]
#[case::async_merge_iter(false, false)]
#[case::async_merge_iter_reverse(false, true)]
fn test_multi_version_merge_iterator_rnd(#[case] use_sync_merge_iter: bool, #[case] reverse: bool) {
    proptest!(|(arb in arb_simple_iterators(10, 20, 50, reverse, false))| {
        let ArbSimpleIterators{
            iters,
            mut ref_iter,
            max_key,
        } = arb;
        let iters = iters.into_iter().map(|it| Box::new(it) as Box<dyn Iterator>).collect();
        let mut merge_it = if use_sync_merge_iter {
            new_merge_iterator(iters, reverse)
        } else {
            Box::new(AsyncMergeIterator::new(iters, reverse, false))
        };

        verify_iter(merge_it.as_mut(), &mut ref_iter, None)?;

        let mut rng = thread_rng();
        for i in (0..=max_key).choose_multiple(&mut rng, max_key / 2) {
            let key = OwnedInnerKey::new(Bytes::from(i_to_key(i)));
            verify_iter(merge_it.as_mut(), &mut ref_iter, Some(key.as_ref()))?;
        }
    });
}

#[rstest]
#[case(false)]
#[case::reverse(true)]
fn test_async_merge_iterator(#[case] reverse: bool) {
    proptest!(|(arb in arb_simple_iterators(10, 20, 50, reverse, true))| {
        let ArbSimpleIterators{
            iters,
            mut ref_iter,
            max_key,
        } = arb;
        let iters = iters.into_iter().map(|it| Box::new(it) as Box<dyn Iterator>).collect();
        let mut merge_it = Box::new(AsyncMergeIterator::new(iters, reverse, false));

        block_on(async {
            verify_iter_async(merge_it.as_mut(), &mut ref_iter, None).await?;

            let mut rng = thread_rng();
            for i in (0..=max_key).choose_multiple(&mut rng, max_key / 2) {
                let key = OwnedInnerKey::new(Bytes::from(i_to_key(i)));
                verify_iter_async(merge_it.as_mut(), &mut ref_iter, Some(key.as_ref())).await?;
            }

            Ok::<(), TestCaseError>(())
        })?;
    });
}
