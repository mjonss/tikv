// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;

use anyhow::Result;
use clara_fts::test_util::{make_unscored_query, PlainFtsQueryInfo};

use super::{lp_key, merge_fts_l0_l1, PackedFileMergeOpt};
use crate::table::fts::{
    dedicated_file::{DedicatedFile, DedicatedFileBuilderOptions, EDedicatedFile},
    iter::{IntPk, OrderedPkIterator, PkReader, PkType},
    packed_file::{EPackedFileLp, PackedFile, PackedFileBuilderOptions, PackedFileLp},
    test_util::new_packed,
};

fn merge_opt() -> PackedFileMergeOpt {
    PackedFileMergeOpt {
        max_pack_file_size: 96 * 1024 * 1024,
        min_l2_lp_size: 64 * 1024 * 1024,
        pack_opt: PackedFileBuilderOptions::default(),
        ded_opt: DedicatedFileBuilderOptions::default(),
    }
}

fn tracked(pairs: &[(i64, i64)]) -> HashSet<(i64, i64)> {
    pairs.iter().copied().collect()
}

fn index_doc_count<Pk: PkType>(lp: &PackedFileLp<Pk>) -> Result<u32> {
    Ok(lp.cached_read_index()?.n_docs() as u32)
}

fn search_doc_ids<Pk: PkType>(lp: &PackedFileLp<Pk>, term: &str) -> Result<Vec<u32>> {
    let reader = lp.cached_read_index()?;
    let mut info = PlainFtsQueryInfo::default();
    info.query = term.to_owned();
    let query = make_unscored_query(&info);
    let mut results = Vec::new();
    reader.search(&query, &mut results)?;
    Ok(results.into_iter().map(|r| r.doc_id).collect())
}

#[tokio::test]
async fn merge_empty_sources() -> Result<()> {
    let existing = HashSet::new();
    let tracked = HashSet::new();
    let outputs = merge_fts_l0_l1(&[], merge_opt(), 0, &existing, &tracked).await?;
    assert!(outputs.l1_files.is_empty());
    assert!(outputs.l2_files.is_empty());
    Ok(())
}

#[tokio::test]
async fn merge_single_source() -> Result<()> {
    let lp1 = lp_key(10, 1);
    let lp2 = lp_key(10, 2);
    let file = new_packed(10, 0)
        .lp(10, 1, |d| {
            d(10, 100, false, "doc10");
        })
        .lp(10, 2, |d| {
            d(20, 200, false, "doc20");
        })
        .finish_as_file();

    let existing = HashSet::new();
    let tracked = tracked(&[(10, 1), (10, 2)]);
    let outputs = merge_fts_l0_l1(&[file], merge_opt(), 0, &existing, &tracked).await?;
    assert_eq!(outputs.l1_files.len(), 1);
    assert!(outputs.l2_files.is_empty());

    let super::L1FileOutput { data, summary } = &outputs.l1_files[0];
    assert_eq!(summary.props.get_total_lps(), 2);

    let merged = PackedFile::from_buffer(data.as_ref())?;
    let mut iter = merged.lp_iter()?;

    let lp = iter.next_lp().await?.unwrap();
    let lp_int = lp.as_int_lp().unwrap();
    assert_eq!(lp_int.lp_key().as_ref(), &lp1[..]);
    assert_eq!(lp_int.props().get_n_pk(), 1);
    assert_eq!(index_doc_count(lp_int)?, 1);
    assert_eq!(search_doc_ids(lp_int, "doc10")?, vec![0]);

    let lp = iter.next_lp().await?.unwrap();
    let lp_int = lp.as_int_lp().unwrap();
    assert_eq!(lp_int.lp_key().as_ref(), &lp2[..]);
    assert_eq!(lp_int.props().get_n_pk(), 1);
    assert_eq!(index_doc_count(lp_int)?, 1);
    assert_eq!(search_doc_ids(lp_int, "doc20")?, vec![0]);

    assert!(iter.next_lp().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn merge_skips_untracked_indexes() -> Result<()> {
    let file = new_packed(20, 0)
        .lp(20, 1, |d| {
            d(10, 100, false, "keep");
        })
        .lp(20, 2, |d| {
            d(20, 200, false, "drop");
        })
        .finish_as_file();

    let existing = HashSet::new();
    let tracked = tracked(&[(20, 1)]);
    let outputs = merge_fts_l0_l1(&[file], merge_opt(), 0, &existing, &tracked).await?;
    assert_eq!(outputs.l1_files.len(), 1);

    let super::L1FileOutput { data, .. } = &outputs.l1_files[0];
    let merged = PackedFile::from_buffer(data.as_ref())?;
    let mut iter = merged.lp_iter()?;
    let lp = iter.next_lp().await?.unwrap();
    let lp = lp.as_int_lp().unwrap();
    assert_eq!(lp.props().get_index_id(), 1);
    assert!(iter.next_lp().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn merge_skips_untracked_even_if_large() -> Result<()> {
    let file = new_packed(21, 0)
        .lp(21, 1, |d| {
            d(1, 100, false, "x");
            d(2, 90, false, "y");
            d(3, 80, false, "z");
        })
        .finish_as_file();

    let existing = HashSet::new();
    let tracked = HashSet::new();
    let mut opt = merge_opt();
    opt.min_l2_lp_size = 0; // Would normally force L2 promotion.
    let outputs = merge_fts_l0_l1(&[file], opt, 0, &existing, &tracked).await?;
    assert!(outputs.l1_files.is_empty());
    assert!(outputs.l2_files.is_empty());
    Ok(())
}

#[tokio::test]
async fn merge_skips_untracked_even_if_existing_l2() -> Result<()> {
    let lp = lp_key(22, 1);
    let file = new_packed(22, 0)
        .lp(22, 1, |d| {
            d(1, 100, false, "doc");
            d(2, 90, false, "doc");
        })
        .finish_as_file();

    let mut existing = HashSet::new();
    existing.insert(lp.to_vec());
    let tracked = HashSet::new();

    let outputs = merge_fts_l0_l1(&[file], merge_opt(), 0, &existing, &tracked).await?;
    assert!(outputs.l1_files.is_empty());
    assert!(outputs.l2_files.is_empty());
    Ok(())
}

#[tokio::test]
async fn merge_multiple_files_multiple_lps_preserve_order() -> Result<()> {
    let lp1 = lp_key(1, 1);
    let lp2 = lp_key(1, 2);
    let lp3 = lp_key(1, 3);
    let lp4 = lp_key(2, 1);

    let file1 = new_packed(1, 0)
        .lp(1, 1, |d| {
            d(11, 100, false, "lp1doc");
        })
        .lp(1, 3, |d| {
            d(33, 300, false, "lp3doc");
        })
        .finish_as_file();

    let file2 = new_packed(2, 0)
        .lp(1, 2, |d| {
            d(22, 200, false, "lp2doc");
        })
        .lp(2, 1, |d| {
            d(44, 400, false, "lp4doc");
        })
        .finish_as_file();

    let existing = HashSet::new();
    let tracked = tracked(&[(1, 1), (1, 2), (1, 3), (2, 1)]);
    let outputs = merge_fts_l0_l1(&[file1, file2], merge_opt(), 0, &existing, &tracked).await?;
    assert_eq!(outputs.l1_files.len(), 1);
    assert!(outputs.l2_files.is_empty());

    let super::L1FileOutput { data, summary } = &outputs.l1_files[0];
    assert_eq!(summary.props.get_total_lps(), 4);

    let merged = PackedFile::from_buffer(data.as_ref())?;
    let mut iter = merged.lp_iter()?;

    let expected = [
        (lp1, "lp1doc"),
        (lp2, "lp2doc"),
        (lp3, "lp3doc"),
        (lp4, "lp4doc"),
    ];

    for (expected_lp, term) in expected {
        let lp = iter.next_lp().await?.expect("expected LP");
        let lp = lp.as_int_lp().unwrap();
        assert_eq!(lp.lp_key().as_ref(), expected_lp.as_slice());
        assert_eq!(lp.props().get_n_pk(), 1);
        assert_eq!(index_doc_count(lp)?, 1);
        assert_eq!(search_doc_ids(lp, term)?, vec![0]);
    }

    assert!(iter.next_lp().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn merge_multi_source_int_pk_merges_versions() -> Result<()> {
    let lp = lp_key(1, 99);
    let file1 = new_packed(3, 0)
        .lp(1, 99, |d| {
            d(10, 300, false, "pk10v300");
            d(10, 100, true, "pk10v100");
            d(30, 200, false, "pk30v200");
        })
        .finish_as_file();

    let file2 = new_packed(4, 0)
        .lp(1, 99, |d| {
            d(10, 250, false, "pk10v250");
            d(20, 220, false, "pk20v220");
        })
        .finish_as_file();

    let existing = HashSet::new();
    let tracked = tracked(&[(1, 99)]);
    let outputs = merge_fts_l0_l1(&[file1, file2], merge_opt(), 0, &existing, &tracked).await?;
    assert_eq!(outputs.l1_files.len(), 1);
    assert!(outputs.l2_files.is_empty());

    let super::L1FileOutput { data, .. } = &outputs.l1_files[0];
    let merged = PackedFile::from_buffer(data.as_ref())?;
    let lp = merged.cached_get_lp(&lp).await?.unwrap();

    let lp = lp.as_int_lp().unwrap();
    assert_eq!(lp.props().get_n_pk(), 5);
    assert_eq!(index_doc_count(lp)?, 5);

    let mut iter = lp.pk_iter()?;
    let mut entries = Vec::new();
    while let Some((_doc_id, pk, version, delete_mark)) = iter.next().await? {
        entries.push((IntPk::decode(pk.as_ref())?, version, delete_mark != 0));
    }
    assert_eq!(
        entries,
        vec![
            (10, 300, false),
            (10, 250, false),
            (10, 100, true),
            (20, 220, false),
            (30, 200, false)
        ]
    );

    assert_eq!(search_doc_ids(lp, "pk10v300")?, vec![0]);
    assert_eq!(search_doc_ids(lp, "pk10v250")?, vec![1]);
    assert_eq!(search_doc_ids(lp, "pk10v100")?, vec![2]);
    assert_eq!(search_doc_ids(lp, "pk20v220")?, vec![3]);
    assert_eq!(search_doc_ids(lp, "pk30v200")?, vec![4]);

    Ok(())
}

#[tokio::test]
async fn merge_filters_tombstones_with_safe_ts() -> Result<()> {
    let lp = lp_key(3, 1);
    let file1 = new_packed(5, 0)
        .lp(3, 1, |d| {
            d(10, 300, false, "pk10v300");
            d(10, 150, true, "pk10deleted");
        })
        .finish_as_file();

    let file2 = new_packed(6, 0)
        .lp(3, 1, |d| {
            d(20, 220, false, "pk20v220");
        })
        .finish_as_file();

    let existing = HashSet::new();
    let tracked = tracked(&[(3, 1)]);
    let outputs = merge_fts_l0_l1(&[file1, file2], merge_opt(), 200, &existing, &tracked).await?;
    assert_eq!(outputs.l1_files.len(), 1);
    assert!(outputs.l2_files.is_empty());

    let super::L1FileOutput { data, .. } = &outputs.l1_files[0];
    let merged = PackedFile::from_buffer(data.as_ref())?;
    let lp = merged.cached_get_lp(&lp).await?.unwrap();
    let lp = lp.as_int_lp().unwrap();

    assert_eq!(lp.props().get_n_pk(), 2);
    assert_eq!(index_doc_count(lp)?, 2);

    let mut iter = lp.pk_iter()?;
    let mut entries = Vec::new();
    while let Some((_doc_id, pk, version, delete_mark)) = iter.next().await? {
        entries.push((IntPk::decode(pk.as_ref())?, version, delete_mark != 0));
    }
    assert_eq!(entries, vec![(10, 300, false), (20, 220, false)]);

    assert_eq!(search_doc_ids(lp, "pk10v300")?, vec![0]);
    assert!(search_doc_ids(lp, "pk10deleted")?.is_empty());
    assert_eq!(search_doc_ids(lp, "pk20v220")?, vec![1]);

    Ok(())
}

#[tokio::test]
async fn merge_preserves_last_alive_version_below_safe_ts() -> Result<()> {
    let lp = lp_key(3, 2);
    let file = new_packed(7, 0)
        .lp(3, 2, |d| {
            d(10, 100, false, "pk10v100");
            d(10, 95, false, "pk10v95");
            d(10, 80, false, "pk10v80");
            d(10, 75, false, "pk10v75");
            d(10, 70, false, "pk10v70");
            d(20, 90, false, "pk20v90");
            d(20, 83, false, "pk20v83");
            d(20, 60, false, "pk20v60");
        })
        .finish_as_file();

    let existing = HashSet::new();
    let tracked = tracked(&[(3, 2)]);
    let outputs = merge_fts_l0_l1(&[file], merge_opt(), 85, &existing, &tracked).await?;
    assert_eq!(outputs.l1_files.len(), 1);
    assert!(outputs.l2_files.is_empty());

    let super::L1FileOutput { data, .. } = &outputs.l1_files[0];
    let merged = PackedFile::from_buffer(data.as_ref())?;
    let lp = merged.cached_get_lp(&lp).await?.unwrap();
    let lp = lp.as_int_lp().unwrap();

    assert_eq!(lp.props().get_n_pk(), 5);
    assert_eq!(index_doc_count(lp)?, 5);

    let mut iter = lp.pk_iter()?;
    let mut entries = Vec::new();
    while let Some((_doc_id, pk, version, delete_mark)) = iter.next().await? {
        entries.push((IntPk::decode(pk.as_ref())?, version, delete_mark != 0));
    }
    assert_eq!(
        entries,
        vec![
            (10, 100, false),
            (10, 95, false),
            (10, 80, false),
            (20, 90, false),
            (20, 83, false),
        ]
    );

    assert_eq!(search_doc_ids(lp, "pk10v100")?, vec![0]);
    assert_eq!(search_doc_ids(lp, "pk10v95")?, vec![1]);
    assert_eq!(search_doc_ids(lp, "pk10v80")?, vec![2]);
    assert!(search_doc_ids(lp, "pk10v75")?.is_empty());
    assert!(search_doc_ids(lp, "pk10v70")?.is_empty());
    assert_eq!(search_doc_ids(lp, "pk20v90")?, vec![3]);
    assert_eq!(search_doc_ids(lp, "pk20v83")?, vec![4]);
    assert!(search_doc_ids(lp, "pk20v60")?.is_empty());

    Ok(())
}

#[tokio::test]
async fn merge_drops_versions_after_tombstone_below_safe_ts() -> Result<()> {
    let lp = lp_key(3, 3);
    let file = new_packed(8, 0)
        .lp(3, 3, |d| {
            d(10, 100, false, "pk10v100");
            d(10, 95, false, "pk10v95");
            d(10, 80, true, "pk10deleted80");
            d(10, 75, false, "pk10v75");
            d(10, 70, false, "pk10v70");
            d(20, 90, false, "pk20v90");
            d(20, 82, false, "pk20v82");
            d(20, 70, false, "pk20v70");
        })
        .finish_as_file();

    let existing = HashSet::new();
    let tracked = tracked(&[(3, 3)]);
    let outputs = merge_fts_l0_l1(&[file], merge_opt(), 85, &existing, &tracked).await?;
    assert_eq!(outputs.l1_files.len(), 1);
    assert!(outputs.l2_files.is_empty());

    let super::L1FileOutput { data, .. } = &outputs.l1_files[0];
    let merged = PackedFile::from_buffer(data.as_ref())?;
    let lp = merged.cached_get_lp(&lp).await?.unwrap();
    let lp = lp.as_int_lp().unwrap();

    assert_eq!(lp.props().get_n_pk(), 4);
    assert_eq!(index_doc_count(lp)?, 4);

    let mut iter = lp.pk_iter()?;
    let mut entries = Vec::new();
    while let Some((_doc_id, pk, version, delete_mark)) = iter.next().await? {
        entries.push((IntPk::decode(pk.as_ref())?, version, delete_mark != 0));
    }
    assert_eq!(
        entries,
        vec![
            (10, 100, false),
            (10, 95, false),
            (20, 90, false),
            (20, 82, false),
        ]
    );

    assert_eq!(search_doc_ids(lp, "pk10v100")?, vec![0]);
    assert_eq!(search_doc_ids(lp, "pk10v95")?, vec![1]);
    assert!(search_doc_ids(lp, "pk10deleted80")?.is_empty());
    assert!(search_doc_ids(lp, "pk10v75")?.is_empty());
    assert!(search_doc_ids(lp, "pk10v70")?.is_empty());
    assert_eq!(search_doc_ids(lp, "pk20v90")?, vec![2]);
    assert_eq!(search_doc_ids(lp, "pk20v82")?, vec![3]);
    assert!(search_doc_ids(lp, "pk20v70")?.is_empty());

    Ok(())
}

#[tokio::test]
async fn merge_single_lp_partial_safe_ts_filter() -> Result<()> {
    let lp = lp_key(4, 1);

    let file = new_packed(9, 0)
        .lp(4, 1, |d| {
            d(100, 500, false, "alive");
            d(100, 100, true, "tombstone");
        })
        .finish_as_file();

    let existing = HashSet::new();
    let tracked = tracked(&[(4, 1)]);
    let outputs = merge_fts_l0_l1(&[file], merge_opt(), 200, &existing, &tracked).await?;
    assert_eq!(outputs.l1_files.len(), 1);
    assert!(outputs.l2_files.is_empty());

    let super::L1FileOutput { data, .. } = &outputs.l1_files[0];
    let merged = PackedFile::from_buffer(data.as_ref())?;
    let lp = merged.cached_get_lp(&lp).await?.unwrap();
    let lp = lp.as_int_lp().unwrap();

    assert_eq!(lp.props().get_n_pk(), 1);
    assert_eq!(index_doc_count(lp)?, 1);

    let mut iter = lp.pk_iter()?;
    let mut entries = Vec::new();
    while let Some((_doc_id, pk, version, delete_mark)) = iter.next().await? {
        entries.push((IntPk::decode(pk.as_ref())?, version, delete_mark != 0));
    }
    assert_eq!(entries, vec![(100, 500, false)]);

    assert_eq!(search_doc_ids(lp, "alive")?, vec![0]);
    assert!(search_doc_ids(lp, "tombstone")?.is_empty());

    Ok(())
}

#[tokio::test]
async fn merge_single_lp_all_filtered_by_safe_ts() -> Result<()> {
    let file = new_packed(10, 0)
        .lp(4, 2, |d| {
            d(200, 150, true, "only_tombstone");
        })
        .finish_as_file();

    let existing = HashSet::new();
    let tracked = tracked(&[(4, 2)]);
    let outputs = merge_fts_l0_l1(&[file], merge_opt(), 200, &existing, &tracked).await?;
    assert!(outputs.l1_files.is_empty());
    assert!(outputs.l2_files.is_empty());
    Ok(())
}

#[tokio::test]
async fn merge_promotes_large_lp_to_l2() -> Result<()> {
    let lp = lp_key(6, 1);
    let file = new_packed(11, 0)
        .lp(6, 1, |d| {
            d(1, 100, false, "doc1");
            d(2, 90, false, "doc2");
        })
        .finish_as_file();

    let mut opt = merge_opt();
    opt.min_l2_lp_size = 0; // Force promotion for any LP.
    let existing = HashSet::new();
    let tracked = tracked(&[(6, 1)]);
    let outputs = merge_fts_l0_l1(&[file], opt, 0, &existing, &tracked).await?;

    assert!(outputs.l1_files.is_empty());
    assert_eq!(outputs.l2_files.len(), 1);

    let l2 = &outputs.l2_files[0];
    assert_eq!(l2.lp_key.as_slice(), lp.as_slice());
    assert_eq!(l2.summary.props.get_lp_key(), lp.to_vec());

    let dedicated = DedicatedFile::from_buffer(l2.data.as_ref())?;
    match dedicated {
        EDedicatedFile::Int(file) => {
            assert_eq!(file.props().get_lp_key(), lp.to_vec());
            assert_eq!(file.props().get_is_int_handle(), true);
        }
        _ => panic!("expected Int dedicated file"),
    }

    Ok(())
}

#[tokio::test]
async fn merge_routes_existing_l2_lp_to_dedicated() -> Result<()> {
    let lp = lp_key(6, 2);
    let file = new_packed(12, 0)
        .lp(6, 2, |d| {
            d(10, 50, false, "doc10");
            d(20, 40, false, "doc20");
        })
        .finish_as_file();

    let mut existing = HashSet::new();
    existing.insert(lp.to_vec());

    let tracked = tracked(&[(6, 2)]);
    let outputs = merge_fts_l0_l1(&[file], merge_opt(), 0, &existing, &tracked).await?;
    assert!(outputs.l1_files.is_empty());
    assert_eq!(outputs.l2_files.len(), 1);

    let l2 = &outputs.l2_files[0];
    assert_eq!(l2.lp_key.as_slice(), lp.as_slice());
    Ok(())
}

#[tokio::test]
async fn merge_multi_source_common_pk_merges_lexicographically() -> Result<()> {
    let lp = lp_key(2, 3);

    let file1 = new_packed(13, 0)
        .lp_common(2, 3, |d| {
            d("alpha", 300, false, "alpha");
            d("delta", 100, true, "delta");
        })
        .finish_as_file();

    let file2 = new_packed(14, 0)
        .lp_common(2, 3, |d| {
            d("alpha", 250, false, "alpha2");
            d("beta", 200, false, "beta");
        })
        .finish_as_file();

    let existing = HashSet::new();
    let tracked = tracked(&[(2, 3)]);
    let outputs = merge_fts_l0_l1(&[file1, file2], merge_opt(), 0, &existing, &tracked).await?;
    assert_eq!(outputs.l1_files.len(), 1);
    assert!(outputs.l2_files.is_empty());

    let super::L1FileOutput { data, .. } = &outputs.l1_files[0];
    let merged = PackedFile::from_buffer(data.as_ref())?;
    let lp = merged.cached_get_lp(&lp).await?.unwrap();

    let lp = lp.as_common_lp().unwrap();
    assert_eq!(lp.props().get_n_pk(), 4);
    assert_eq!(index_doc_count(lp)?, 4);

    let mut iter = lp.pk_iter()?;
    let mut entries = Vec::new();
    while let Some((_doc_id, pk, version, delete_mark)) = iter.next().await? {
        entries.push((pk.to_vec(), version, delete_mark != 0));
    }
    assert_eq!(
        entries,
        vec![
            (b"alpha".to_vec(), 300, false),
            (b"alpha".to_vec(), 250, false),
            (b"beta".to_vec(), 200, false),
            (b"delta".to_vec(), 100, true),
        ]
    );

    assert_eq!(search_doc_ids(lp, "alpha")?, vec![0]);
    assert_eq!(search_doc_ids(lp, "alpha2")?, vec![1]);
    assert_eq!(search_doc_ids(lp, "beta")?, vec![2]);
    assert_eq!(search_doc_ids(lp, "delta")?, vec![3]);

    Ok(())
}

#[tokio::test]
async fn merge_splits_when_size_limit_reached() -> Result<()> {
    let lp_keys: Vec<_> = (0..4).map(|i| lp_key(5, i)).collect();
    let mut builder = new_packed(15, 0);
    for i in 0..lp_keys.len() {
        builder = builder.lp(5, i as i64, |d| {
            d(i as i64, (i as u64) + 1, false, "payload");
        });
    }
    let file = builder.finish_as_file();

    let existing = HashSet::new();
    let tracked = tracked(&[(5, 0), (5, 1), (5, 2), (5, 3)]);
    let outputs = merge_fts_l0_l1(
        &[file],
        PackedFileMergeOpt {
            max_pack_file_size: 0,
            ..merge_opt()
        },
        0,
        &existing,
        &tracked,
    )
    .await?;
    assert_eq!(outputs.l1_files.len(), lp_keys.len());

    let mut seen = 0;
    for super::L1FileOutput { data, .. } in &outputs.l1_files {
        let file = PackedFile::from_buffer(data.as_ref())?;
        let mut iter = file.lp_iter()?;
        if let Some(lp_entry) = iter.next_lp().await? {
            seen += 1;
            assert!(iter.next_lp().await?.is_none());
            match lp_entry {
                EPackedFileLp::Int(_) => {}
                _ => panic!("expected Int LP"),
            }
        } else {
            panic!("output packed file without LPs");
        }
    }
    assert_eq!(seen, lp_keys.len());
    Ok(())
}

#[tokio::test]
async fn merge_errors_on_lp_property_mismatch() -> Result<()> {
    let file1 = new_packed(16, 0)
        .lp(8, 8, |d| {
            d(1, 100, false, "int");
        })
        .finish_as_file();

    let file2 = new_packed(17, 0)
        .lp_common(8, 8, |d| {
            d("c", 90, false, "common");
        })
        .finish_as_file();

    let existing = HashSet::new();
    let tracked = tracked(&[(8, 8)]);
    let err = merge_fts_l0_l1(&[file1, file2], merge_opt(), 0, &existing, &tracked)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("is_int_handle"),
        "unexpected error message: {:?}",
        err
    );
    Ok(())
}
