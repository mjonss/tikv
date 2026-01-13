// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::{Result, bail};
use bytes::Bytes;
use tidb_query_datatype::codec::table::append_row_key;

use super::inplace::{
    InplaceResult, InplaceSpec, KeyRange, rewrite_dedicated_file, rewrite_packed_file,
};
use crate::table::fts::{
    dedicated_file::{DedicatedFile, DedicatedFileBuilderOptions, EDedicatedFile},
    iter::{IntPk, OrderedPkIterator, PkReader, PkType},
    packed_file::{PackedFile, PackedFileBuilderOptions},
    test_util::{Doc, doc, new_ded, new_packed},
};

enum ExpectOutcome<'a> {
    NoChange,
    Handles(&'a [i64]),
    Removed,
}

const PACKED_INDEX_ID: i64 = 1;
const DEDICATED_INDEX_ID: i64 = 0;

#[tokio::test]
async fn test_fts_trim_overbound() -> Result<()> {
    let packed = new_packed(30, 0)
        .lp_with_docs(30, PACKED_INDEX_ID, trim_docs())
        .finish_as_file();
    let dedicated = new_ded(30)
        .lp_with_docs(30, DEDICATED_INDEX_ID, trim_docs())
        .finish_as_file();

    let full_range = InplaceSpec::TrimOverbound(KeyRange {
        start: Some(encode_row_key(30, 5)),
        end: Some(encode_row_key(30, 50)),
    });
    check_packed(&packed, &full_range, ExpectOutcome::NoChange).await?;
    check_dedicated(&dedicated, &full_range, ExpectOutcome::NoChange).await?;

    let partial_range = InplaceSpec::TrimOverbound(KeyRange {
        start: Some(encode_row_key(30, 11)),
        end: Some(encode_row_key(30, 30)),
    });
    check_packed(
        &packed,
        &partial_range,
        ExpectOutcome::Handles(&[11, 15, 20]),
    )
    .await?;
    check_dedicated(
        &dedicated,
        &partial_range,
        ExpectOutcome::Handles(&[11, 15, 20]),
    )
    .await?;

    let drop_all = InplaceSpec::TrimOverbound(KeyRange {
        start: Some(encode_row_key(30, 100)),
        end: None,
    });
    check_packed(&packed, &drop_all, ExpectOutcome::Removed).await?;
    check_dedicated(&dedicated, &drop_all, ExpectOutcome::Removed).await?;

    Ok(())
}

#[tokio::test]
async fn test_fts_truncate_ts() -> Result<()> {
    let packed = new_packed(40, 0)
        .lp_with_docs(40, PACKED_INDEX_ID, truncate_docs())
        .finish_as_file();
    let dedicated = new_ded(40)
        .lp_with_docs(40, DEDICATED_INDEX_ID, truncate_docs())
        .finish_as_file();

    let no_change = InplaceSpec::TruncateTs { truncate_ts: 500 };
    check_packed(&packed, &no_change, ExpectOutcome::NoChange).await?;
    check_dedicated(&dedicated, &no_change, ExpectOutcome::NoChange).await?;

    let partial = InplaceSpec::TruncateTs { truncate_ts: 175 };
    check_packed(&packed, &partial, ExpectOutcome::Handles(&[10, 11, 12])).await?;
    check_dedicated(&dedicated, &partial, ExpectOutcome::Handles(&[10, 11, 12])).await?;

    let drop_all = InplaceSpec::TruncateTs { truncate_ts: 80 };
    check_packed(&packed, &drop_all, ExpectOutcome::Removed).await?;
    check_dedicated(&dedicated, &drop_all, ExpectOutcome::Removed).await?;

    Ok(())
}

#[tokio::test]
async fn test_fts_truncate_ts_common_handle_packed() -> Result<()> {
    let packed = new_packed(41, 0)
        .lp_common(41, PACKED_INDEX_ID, |d| {
            d("a", 300, false, "doc_a");
            d("b", 200, false, "doc_b");
            d("c", 100, false, "doc_c");
        })
        .finish_as_file();

    let partial = InplaceSpec::TruncateTs { truncate_ts: 250 };
    match rewrite_packed_file(&packed, &partial, &PackedFileBuilderOptions::default()).await? {
        InplaceResult::Rewritten { data, .. } => {
            let file = PackedFile::from_buffer(data.as_ref())?;
            let mut lp_iter = file.lp_iter()?;
            let lp = lp_iter.next_lp().await?.expect("lp expected");
            let lp = lp.as_common_lp()?;
            assert_eq!(lp.cached_read_index()?.n_docs(), 2);
            let mut iter = lp.pk_iter()?;
            let mut pks = Vec::new();
            while let Some((_doc_id, pk, ..)) = iter.next().await? {
                pks.push(std::str::from_utf8(pk.as_ref()).unwrap().to_string());
            }
            assert_eq!(pks, vec!["b", "c"]);
            Ok(())
        }
        InplaceResult::NoChange => bail!("expected Rewritten, got NoChange"),
        InplaceResult::Removed => bail!("expected Rewritten, got Removed"),
    }
}

#[tokio::test]
async fn test_fts_destroy_range() -> Result<()> {
    let packed = new_packed(50, 0)
        .lp_with_docs(50, PACKED_INDEX_ID, destroy_docs())
        .finish_as_file();
    let dedicated = new_ded(50)
        .lp_with_docs(50, DEDICATED_INDEX_ID, destroy_docs())
        .finish_as_file();

    let no_change = InplaceSpec::DestroyRange {
        prefixes: vec![encode_row_key(50, 999)],
    };
    check_packed(&packed, &no_change, ExpectOutcome::NoChange).await?;
    check_dedicated(&dedicated, &no_change, ExpectOutcome::NoChange).await?;

    let partial = InplaceSpec::DestroyRange {
        prefixes: vec![encode_row_key(50, 12)],
    };
    check_packed(&packed, &partial, ExpectOutcome::Handles(&[10, 11, 13])).await?;
    check_dedicated(&dedicated, &partial, ExpectOutcome::Handles(&[10, 11, 13])).await?;

    let drop_all = InplaceSpec::DestroyRange {
        prefixes: vec![
            encode_row_key(50, 10),
            encode_row_key(50, 11),
            encode_row_key(50, 12),
            encode_row_key(50, 13),
        ],
    };
    check_packed(&packed, &drop_all, ExpectOutcome::Removed).await?;
    check_dedicated(&dedicated, &drop_all, ExpectOutcome::Removed).await?;

    Ok(())
}

// ---------- Helpers ----------

async fn check_packed(
    file: &PackedFile,
    spec: &InplaceSpec,
    expect: ExpectOutcome<'_>,
) -> Result<()> {
    match rewrite_packed_file(file, spec, &PackedFileBuilderOptions::default()).await? {
        InplaceResult::NoChange => match expect {
            ExpectOutcome::NoChange => Ok(()),
            _ => bail!("expected {:?}, got NoChange", expect_name(&expect)),
        },
        InplaceResult::Removed => match expect {
            ExpectOutcome::Removed => Ok(()),
            _ => bail!("expected {:?}, got Removed", expect_name(&expect)),
        },
        InplaceResult::Rewritten { data, .. } => match expect {
            ExpectOutcome::Handles(expected) => assert_packed_handles(&data, expected).await,
            other => bail!("expected {:?}, got Rewritten", expect_name(&other)),
        },
    }
}

async fn check_dedicated(
    file: &EDedicatedFile,
    spec: &InplaceSpec,
    expect: ExpectOutcome<'_>,
) -> Result<()> {
    match rewrite_dedicated_file(file, spec, &DedicatedFileBuilderOptions::default()).await? {
        InplaceResult::NoChange => match expect {
            ExpectOutcome::NoChange => Ok(()),
            _ => bail!("expected {:?}, got NoChange", expect_name(&expect)),
        },
        InplaceResult::Removed => match expect {
            ExpectOutcome::Removed => Ok(()),
            _ => bail!("expected {:?}, got Removed", expect_name(&expect)),
        },
        InplaceResult::Rewritten { data, .. } => match expect {
            ExpectOutcome::Handles(expected) => assert_dedicated_handles(&data, expected).await,
            other => bail!("expected {:?}, got Rewritten", expect_name(&other)),
        },
    }
}

fn expect_name(expect: &ExpectOutcome<'_>) -> &'static str {
    match expect {
        ExpectOutcome::NoChange => "NoChange",
        ExpectOutcome::Handles(_) => "Handles",
        ExpectOutcome::Removed => "Removed",
    }
}

async fn assert_packed_handles(data: &Bytes, expected: &[i64]) -> Result<()> {
    let file = PackedFile::from_buffer(data.as_ref())?;
    let mut lp_iter = file.lp_iter()?;
    let lp = lp_iter.next_lp().await?.expect("lp expected");
    let lp = lp.as_int_lp()?;
    assert_eq!(lp.cached_read_index()?.n_docs(), expected.len());
    let mut iter = lp.pk_iter()?;
    let mut handles = Vec::new();
    while let Some((_doc_id, pk, ..)) = iter.next().await? {
        handles.push(IntPk::decode(pk.as_ref())?);
    }
    assert_eq!(handles, expected);
    Ok(())
}

async fn assert_dedicated_handles(data: &Bytes, expected: &[i64]) -> Result<()> {
    let file = DedicatedFile::from_buffer(data.as_ref())?;
    let file = file.as_int()?;
    assert_eq!(file.cached_read_index().await?.n_docs(), expected.len());
    let mut iter = file.pk_iter()?;
    let mut handles = Vec::new();
    while let Some((_doc_id, pk, ..)) = iter.next().await? {
        handles.push(IntPk::decode(pk.as_ref())?);
    }
    assert_eq!(handles, expected);
    Ok(())
}

fn encode_row_key(table_id: i64, handle: i64) -> Vec<u8> {
    let mut key = Vec::new();
    append_row_key(&mut key, table_id, handle).expect("encode row key");
    key
}

fn trim_docs() -> Vec<Doc<IntPk>> {
    vec![
        doc(8, 220, false, "doc"),
        doc(11, 210, false, "doc"),
        doc(15, 205, true, "doc"),
        doc(20, 150, false, "doc"),
        doc(35, 120, false, "doc"),
    ]
}

fn truncate_docs() -> Vec<Doc<IntPk>> {
    vec![
        doc(10, 300, false, "doc"),
        doc(10, 100, false, "doc"),
        doc(11, 220, true, "doc"),
        doc(11, 150, false, "doc"),
        doc(12, 90, false, "doc"),
    ]
}

fn destroy_docs() -> Vec<Doc<IntPk>> {
    vec![
        doc(10, 200, false, "doc"),
        doc(11, 180, true, "doc"),
        doc(12, 170, false, "doc"),
        doc(13, 160, false, "doc"),
    ]
}
