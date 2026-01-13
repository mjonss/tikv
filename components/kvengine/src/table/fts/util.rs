// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;

use tidb_query_datatype::{
    Collation, FieldTypeAccessor,
    codec::collation::{Encoding, encoding::*},
};
use tipb::ColumnInfo;

use crate::table::{Error, Result};

/// Converts raw column bytes into UTF-8 text for FTS.
///
/// Callers may choose to treat decoding errors as "non-searchable" content by
/// falling back to an empty string, but this function itself does not hide
/// errors.
pub type StringifyFn = Box<dyn Fn(&[u8]) -> Result<Cow<'_, str>> + Send>;

fn decode<E: Encoding>(input: &[u8]) -> Result<Vec<u8>> {
    E::decode(input).map_err(|e| Error::Other(e.to_string()))
}

/// A stringify function that always returns empty text.
///
/// This is intended as a caller-side fallback for unsupported collations.
pub fn empty_stringify_fn() -> StringifyFn {
    Box::new(|_| Ok(Cow::Borrowed("")))
}

/// Builds a [`StringifyFn`] for the given column.
///
/// Returns an error when the column collation is unsupported for decoding.
/// Decoding errors are surfaced to the caller via the returned function.
pub fn build_stringify_fn(col_info: &ColumnInfo) -> Result<StringifyFn> {
    let collation = col_info
        .as_accessor()
        .collation()
        .map_err(|e| Error::Other(e.to_string()))?;
    match collation {
        Collation::Utf8Mb4Bin
        | Collation::Utf8Mb4BinNoPadding
        | Collation::Utf8Mb4GeneralCi
        | Collation::Utf8Mb4UnicodeCi
        | Collation::Utf8Mb40900AiCi
        | Collation::Utf8Mb40900Bin
        | Collation::Latin1Bin => Ok(Box::new(|a| {
            std::str::from_utf8(a)
                .map(Cow::Borrowed)
                .map_err(|e| Error::Other(e.to_string()))
        })),

        Collation::GbkBin | Collation::GbkChineseCi => Ok(Box::new(|a| {
            decode::<EncodingGbk>(a).and_then(|decoded_bytes| {
                String::from_utf8(decoded_bytes)
                    .map(Cow::Owned)
                    .map_err(|e| Error::Other(e.to_string()))
            })
        })),

        Collation::Gb18030Bin | Collation::Gb18030ChineseCi => Ok(Box::new(|a| {
            decode::<EncodingGb18030>(a).and_then(|decoded_bytes| {
                String::from_utf8(decoded_bytes)
                    .map(Cow::Owned)
                    .map_err(|e| Error::Other(e.to_string()))
            })
        })),

        Collation::Binary => Err(Error::Other(
            "FTS stringify does not support binary collation".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::FieldTypeTp;

    use super::*;

    fn new_col(tp: FieldTypeTp, collation: Collation) -> ColumnInfo {
        let mut c = ColumnInfo::new();
        c.set_column_id(1);
        c.set_tp(tp as i32);
        c.set_collation(collation as i32);
        c
    }

    #[test]
    fn test_build_stringify_fn_invalid_utf8_propagates_error() {
        let col = new_col(FieldTypeTp::String, Collation::Utf8Mb4Bin);
        let f = build_stringify_fn(&col).unwrap();
        f(&[0x80]).unwrap_err();
    }

    #[test]
    fn test_build_stringify_fn_binary_is_unsupported() {
        let col = new_col(FieldTypeTp::String, Collation::Binary);
        assert!(build_stringify_fn(&col).is_err());
    }

    #[test]
    fn test_build_stringify_fn_gbk_decode_roundtrip() {
        let col = new_col(FieldTypeTp::String, Collation::GbkBin);
        let f = build_stringify_fn(&col).unwrap();

        let src = "你好";
        let gbk = EncodingGbk::encode(src.as_bytes()).unwrap();
        let decoded = f(&gbk).unwrap();
        assert_eq!(decoded.as_ref(), src);
    }

    #[test]
    fn test_build_stringify_fn_gb18030_decode_roundtrip() {
        let col = new_col(FieldTypeTp::String, Collation::Gb18030Bin);
        let f = build_stringify_fn(&col).unwrap();

        let src = "你好";
        let gb18030 = EncodingGb18030::encode(src.as_bytes()).unwrap();
        let decoded = f(&gb18030).unwrap();
        assert_eq!(decoded.as_ref(), src);
    }
}
