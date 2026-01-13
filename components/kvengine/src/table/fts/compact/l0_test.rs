// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;

use anyhow::Result;
use clara_fts::test_util::{make_unscored_query, PlainFtsQueryInfo};
use kvenginepb::fts::FullTextIndexDef;
use tidb_query_datatype::FieldTypeTp;

use super::{columnar_to_fts_l0, ColumnarToFtsL0Opts};
use crate::table::{
    fts::{
        lp_key,
        test_util::{new_columnar, new_schema_file, SchemaBuilder},
        IntPk, PackedFile, PkReader,
    },
    SnapVersion,
};

#[tokio::test]
async fn test_columnar_l0_to_fts_basic() -> Result<()> {
    let table_id = 30;

    let schema = SchemaBuilder::<IntPk>::new(table_id)
        .column(7, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id: 1,
            col_id: 7, // Column 7 is the text column
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file = new_schema_file(&[&schema]);

    let columnar_file = new_columnar(1, 0)
        .table::<IntPk>(&schema, |row| {
            row(0, 1000, false, |datum| {
                datum("abc first entry");
            });
            row(1, 950, false, |datum| {
                datum("plain content");
            });
            row(2, 1020, false, |datum| {
                datum("another abc value");
            });
            row(3, 980, false, |datum| {
                datum("quiet note");
            });
        })
        .finish_as_file();

    // Convert to FTS L0
    let opts = ColumnarToFtsL0Opts {
        columnar_files: &[columnar_file],
        schema_file,
        index_pairs: &[(table_id, 1)], // Specify the explicit index_id from fts_index definition
        encryption_key: None,
        snap_version: SnapVersion::zero(),
        l0_file_max_size: 0,
    };

    let file_data = columnar_to_fts_l0(opts).await?;
    assert!(!file_data.is_empty(), "Should generate FTS data");

    let output = file_data.into_iter().next().unwrap();
    let packed_file = PackedFile::from_buffer(&output.data)?;

    // Test logical partition structure
    assert!(
        packed_file.cached_get_lp(&lp_key(1, 1)).await?.is_none(),
        "Should not find non-existent table"
    );

    let lp = packed_file.cached_get_lp(&lp_key(30, 1)).await?;
    assert!(lp.is_some(), "Should find the correct logical partition");

    assert!(
        packed_file.cached_get_lp(&lp_key(30, 5)).await?.is_none(),
        "Should not find non-existent index"
    );

    // Enhanced tests: Test IndexReader and search functionality
    // Extract the Int variant from the enum
    let lp = lp.unwrap();
    let lp = lp.as_int_lp().unwrap();
    assert!(lp.props().get_n_pk() > 0, "Should have documents");

    // Test Tantivy index reading and searching
    let reader = lp.cached_read_index()?;
    let mut results = Vec::new();

    // Test searching for "abc" - rows above embed "abc" so the query should
    // succeed.
    let query = make_unscored_query(&PlainFtsQueryInfo {
        query: "abc".to_string(),
        ..Default::default()
    });
    reader.search(&query, &mut results)?;
    assert!(
        !results.is_empty(),
        "Should find documents containing 'abc' from generated test data"
    );

    // Test searching for a term that doesn't exist
    let query = make_unscored_query(&PlainFtsQueryInfo {
        query: "nonexistent".to_string(),
        ..Default::default()
    });
    reader.search(&query, &mut results)?;
    assert_eq!(
        results.len(),
        0,
        "Should not find any documents containing 'nonexistent'"
    );

    // Test MVCC functionality with has_newer_version
    // Test that non-existent handles return false
    assert!(
        !lp.has_newer_version(&IntPk::encode(999), 0, u64::MAX)
            .await?,
        "Non-existent handle should return false"
    );

    // Test with version 1000 (row 0 uses version 1000)
    assert!(
        lp.has_newer_version(&IntPk::encode(0), 900, 1100).await?,
        "Handle 0 should have version around 1000"
    );
    assert!(
        !lp.has_newer_version(&IntPk::encode(0), 1000, 1100).await?,
        "Handle 0 version should not be > 1000"
    );

    Ok(())
}

#[tokio::test]
async fn test_columnar_l0_to_fts_invalid_utf8_should_not_fail() -> Result<()> {
    let table_id = 30;

    let schema = SchemaBuilder::<IntPk>::new(table_id)
        .column(7, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id: 1,
            col_id: 7,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file = new_schema_file(&[&schema]);

    // 0x80 is not a valid UTF-8 byte by itself.
    let invalid_utf8: &[u8] = &[b'a', b'b', b'c', 0x80, b'd', b'e', b'f'];

    let columnar_file = new_columnar(1, 0)
        .table::<IntPk>(&schema, |row| {
            row(0, 1000, false, |datum| {
                datum(invalid_utf8);
            });
            row(1, 1000, false, |datum| {
                datum("hello world");
            });
        })
        .finish_as_file();

    let opts = ColumnarToFtsL0Opts {
        columnar_files: &[columnar_file],
        schema_file,
        index_pairs: &[(table_id, 1)],
        encryption_key: None,
        snap_version: SnapVersion::zero(),
        l0_file_max_size: 0,
    };

    let outputs = columnar_to_fts_l0(opts).await?;
    assert!(!outputs.is_empty(), "Should generate FTS data");

    let packed_file = PackedFile::from_buffer(&outputs[0].data)?;
    let lp = packed_file
        .cached_get_lp(&lp_key(table_id, 1))
        .await?
        .expect("lp must exist");
    let lp = lp.as_int_lp().unwrap();

    let reader = lp.cached_read_index()?;
    let query = make_unscored_query(&PlainFtsQueryInfo {
        query: "hello".to_string(),
        ..Default::default()
    });
    let mut results = Vec::new();
    reader.search(&query, &mut results)?;
    assert!(!results.is_empty(), "Should still index valid UTF-8 rows");

    Ok(())
}

#[tokio::test]
async fn test_columnar_l0_to_fts_not_null_text_column() -> Result<()> {
    let table_id = 31;

    let schema = SchemaBuilder::<IntPk>::new(table_id)
        .column_not_null(7, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id: 1,
            col_id: 7,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file = new_schema_file(&[&schema]);

    let columnar_file = new_columnar(2, 0)
        .table::<IntPk>(&schema, |row| {
            row(0, 1000, false, |datum| {
                datum("abc not null");
            });
            row(1, 950, false, |datum| {
                datum("plain content");
            });
        })
        .finish_as_file();

    let opts = ColumnarToFtsL0Opts {
        columnar_files: &[columnar_file],
        schema_file,
        index_pairs: &[(table_id, 1)],
        encryption_key: None,
        snap_version: SnapVersion::zero(),
        l0_file_max_size: 0,
    };
    let file_data = columnar_to_fts_l0(opts).await?;
    assert!(!file_data.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_edge_cases() -> Result<()> {
    let table_id = 100;

    let schema = SchemaBuilder::<IntPk>::new(table_id)
        .column(7, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id: 1,
            col_id: 7,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file = new_schema_file(&[&schema]);

    let columnar_file = new_columnar(11, 0)
        .table::<IntPk>(&schema, |row| {
            row(10, 500, false, |datum| {
                datum("edge alpha");
            });
            row(11, 600, false, |datum| {
                datum("edge beta");
            });
        })
        .finish_as_file();

    // Edge Case 1: Empty index pairs list (no indexes to process)
    let opts_empty = ColumnarToFtsL0Opts {
        columnar_files: &[columnar_file.clone()],
        schema_file: schema_file.clone(),
        index_pairs: &[], // Empty index pairs list
        encryption_key: None,
        snap_version: SnapVersion::zero(),
        l0_file_max_size: 0,
    };
    let file_data_empty = columnar_to_fts_l0(opts_empty).await?;
    assert!(
        file_data_empty.is_empty(),
        "Empty index pairs list should produce no FTS file"
    );

    // Edge Case 2: Index pairs that don't exist in schema
    let opts_nonexistent = ColumnarToFtsL0Opts {
        columnar_files: &[columnar_file],
        schema_file,
        index_pairs: &[(999, 1)], // Non-existent table ID
        encryption_key: None,
        snap_version: SnapVersion::zero(),
        l0_file_max_size: 0,
    };
    let file_data_nonexistent = columnar_to_fts_l0(opts_nonexistent).await?;
    assert!(
        file_data_nonexistent.is_empty(),
        "Non-existent table should produce no FTS file"
    );

    Ok(())
}

#[tokio::test]
async fn test_multiple_columnar_tables() -> Result<()> {
    // Create two different tables with FTS indexes
    let table_id_1 = 200;
    let table_id_2 = 300;

    let schema_1 = SchemaBuilder::<IntPk>::new(table_id_1)
        .column(7, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id: 1,
            col_id: 7,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_2 = SchemaBuilder::<IntPk>::new(table_id_2)
        .column(7, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id: 2,
            col_id: 7,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file = new_schema_file(&[&schema_1, &schema_2]);

    // Create columnar files for both tables
    let columnar_file_1 = new_columnar(21, 0)
        .table::<IntPk>(&schema_1, |row| {
            row(1, 700, false, |datum| {
                datum("table200 alpha");
            });
            row(2, 720, false, |datum| {
                datum("table200 abc beta");
            });
        })
        .finish_as_file();
    let columnar_file_2 = new_columnar(22, 0)
        .table::<IntPk>(&schema_2, |row| {
            row(1, 800, false, |datum| {
                datum("table300 abc delta");
            });
            row(2, 830, false, |datum| {
                datum("table300 omega");
            });
        })
        .finish_as_file();

    // Convert both tables to FTS L0
    let opts = ColumnarToFtsL0Opts {
        columnar_files: &[columnar_file_1, columnar_file_2],
        schema_file,
        index_pairs: &[(table_id_1, 1), (table_id_2, 2)], // Specify both (table, index) pairs
        encryption_key: None,
        snap_version: SnapVersion::zero(),
        l0_file_max_size: 0,
    };

    let file_data = columnar_to_fts_l0(opts).await?;
    assert!(
        !file_data.is_empty(),
        "Should generate FTS data for multiple tables"
    );

    let output = file_data.into_iter().next().unwrap();
    let packed_file = PackedFile::from_buffer(&output.data)?;

    // Test both logical partitions exist
    let lp_1 = packed_file.cached_get_lp(&lp_key(200, 1)).await?;
    let lp_2 = packed_file.cached_get_lp(&lp_key(300, 2)).await?;

    assert!(
        lp_1.is_some(),
        "Should find logical partition for table 200"
    );
    assert!(
        lp_2.is_some(),
        "Should find logical partition for table 300"
    );

    // Test cross-table isolation
    assert!(
        packed_file.cached_get_lp(&lp_key(200, 2)).await?.is_none(),
        "Should not find table 200 with index 2"
    );
    assert!(
        packed_file.cached_get_lp(&lp_key(300, 1)).await?.is_none(),
        "Should not find table 300 with index 1"
    );

    // Test documents exist in both partitions
    // Extract Int variants
    let lp_1 = lp_1.unwrap();
    let lp_1 = lp_1.as_int_lp().unwrap();
    let lp_2 = lp_2.unwrap();
    let lp_2 = lp_2.as_int_lp().unwrap();
    assert!(
        lp_1.props().get_n_pk() > 0,
        "Table 200 should have documents"
    );
    assert!(
        lp_2.props().get_n_pk() > 0,
        "Table 300 should have documents"
    );

    Ok(())
}

#[tokio::test]
async fn test_multiple_indexes_per_table() -> Result<()> {
    let table_id = 400;

    // Create multiple FTS indexes on different columns
    let schema = SchemaBuilder::<IntPk>::new(table_id)
        .column(7, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id: 1,
            col_id: 7,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .fts_index(FullTextIndexDef {
            index_id: 3,
            col_id: 7, // Same column, different index
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file = new_schema_file(&[&schema]);

    let columnar_file = new_columnar(31, 0)
        .table::<IntPk>(&schema, |row| {
            row(1, 600, false, |datum| {
                datum("abc multi index one");
            });
            row(2, 620, false, |datum| {
                datum("plain multi index");
            });
            row(3, 640, false, |datum| {
                datum("abc multi index two");
            });
        })
        .finish_as_file();

    // Convert to FTS L0
    let opts = ColumnarToFtsL0Opts {
        columnar_files: &[columnar_file],
        schema_file,
        index_pairs: &[(table_id, 1), (table_id, 3)], // Specify both index IDs for the same table
        encryption_key: None,
        snap_version: SnapVersion::zero(),
        l0_file_max_size: 0,
    };

    let file_data = columnar_to_fts_l0(opts).await?;
    assert!(
        !file_data.is_empty(),
        "Should generate FTS data for multiple indexes"
    );

    let output = file_data.into_iter().next().unwrap();
    let packed_file = PackedFile::from_buffer(&output.data)?;

    // Test both logical partitions exist for the same table but different indexes
    let lp_index_1 = packed_file.cached_get_lp(&lp_key(400, 1)).await?;
    let lp_index_3 = packed_file.cached_get_lp(&lp_key(400, 3)).await?;

    assert!(
        lp_index_1.is_some(),
        "Should find logical partition for index 1"
    );
    assert!(
        lp_index_3.is_some(),
        "Should find logical partition for index 3"
    );

    // Test non-existent index
    assert!(
        packed_file.cached_get_lp(&lp_key(400, 2)).await?.is_none(),
        "Should not find non-existent index 2"
    );

    // Both indexes should have the same documents (since they index the same
    // column)
    // Extract Int variants
    let lp_index_1 = lp_index_1.unwrap();
    let lp_index_1 = lp_index_1.as_int_lp().unwrap();
    let lp_index_3 = lp_index_3.unwrap();
    let lp_index_3 = lp_index_3.as_int_lp().unwrap();
    assert!(
        lp_index_1.props().get_n_pk() > 0,
        "Index 1 should have documents"
    );
    assert!(
        lp_index_3.props().get_n_pk() > 0,
        "Index 3 should have documents"
    );
    assert_eq!(
        lp_index_1.props().get_n_pk(),
        lp_index_3.props().get_n_pk(),
        "Both indexes should have the same number of documents"
    );

    // Test search isolation between indexes
    let reader_1 = lp_index_1.cached_read_index()?;
    let reader_3 = lp_index_3.cached_read_index()?;

    let mut results_1 = Vec::new();
    let mut results_3 = Vec::new();

    let query = make_unscored_query(&PlainFtsQueryInfo {
        query: "abc".to_string(),
        ..Default::default()
    });

    reader_1.search(&query, &mut results_1)?;
    reader_3.search(&query, &mut results_3)?;

    // Both should find results since they index the same data
    assert!(!results_1.is_empty(), "Index 1 should find search results");
    assert!(!results_3.is_empty(), "Index 3 should find search results");

    Ok(())
}

#[tokio::test]
async fn test_multiple_columnar_files() -> Result<()> {
    let table_id = 500;

    let schema = SchemaBuilder::<IntPk>::new(table_id)
        .column(7, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id: 1,
            col_id: 7,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file = new_schema_file(&[&schema]);

    // Create multiple columnar files with explicit overlapping versions
    let columnar_files = vec![
        new_columnar(41, 0)
            .table::<IntPk>(&schema, |row| {
                row(0, 1000, false, |datum| {
                    datum("abc file1 handle0 v1000");
                });
                row(1, 1000, false, |datum| {
                    datum("abc file1 handle1 v1000");
                });
            })
            .finish_as_file(),
        new_columnar(42, 0)
            .table::<IntPk>(&schema, |row| {
                row(2, 2000, false, |datum| {
                    datum("abc file2 handle2 v2000");
                });
                row(3, 2000, false, |datum| {
                    datum("abc file2 handle3 v2000");
                });
            })
            .finish_as_file(),
        new_columnar(43, 0)
            .table::<IntPk>(&schema, |row| {
                row(1, 1500, false, |datum| {
                    datum("abc file3 handle1 v1500");
                });
                row(2, 1500, false, |datum| {
                    datum("abc file3 handle2 v1500");
                });
            })
            .finish_as_file(),
    ];

    // Convert to FTS L0
    let opts = ColumnarToFtsL0Opts {
        columnar_files: &columnar_files,
        schema_file,
        index_pairs: &[(table_id, 1)], // Specify the explicit index_id
        encryption_key: None,
        snap_version: SnapVersion::zero(),
        l0_file_max_size: 0,
    };

    let file_data = columnar_to_fts_l0(opts).await?;
    assert!(
        !file_data.is_empty(),
        "Should generate FTS data from multiple files"
    );

    let output = file_data.into_iter().next().unwrap();
    let packed_file = PackedFile::from_buffer(&output.data)?;

    let lp = packed_file.cached_get_lp(&lp_key(500, 1)).await?;
    assert!(lp.is_some(), "Should find logical partition");

    // Extract Int variant
    let lp = lp.unwrap();
    let lp = lp.as_int_lp().unwrap();

    // Should have documents from all files, with proper MVCC handling
    // The exact number depends on how the merge handles overlapping handles with
    // different versions
    assert!(
        lp.props().get_n_pk() > 0,
        "Should have documents from merged files"
    );

    // Test MVCC with different versions
    let reader = lp.cached_read_index()?;
    let mut results = Vec::new();

    let query = make_unscored_query(&PlainFtsQueryInfo {
        query: "abc".to_string(),
        ..Default::default()
    });

    reader.search(&query, &mut results)?;
    assert!(!results.is_empty(), "Should find documents in merged index");

    // Test MVCC version checking with overlapping versions
    // MVCC keeps ALL versions for each handle, not just the latest:
    // - Handle 0: version 1000 (from file 1)
    // - Handle 1: versions 1000 (file 1) AND 1500 (file 3)
    // - Handle 2: versions 1500 (file 3) AND 2000 (file 2)
    // - Handle 3: version 2000 (from file 2)

    // Test Handle 0: should have version 1000 only
    assert!(
        lp.has_newer_version(&IntPk::encode(0), 900, 1100).await?,
        "Handle 0 should have version 1000 (from file 1)"
    );
    assert!(
        !lp.has_newer_version(&IntPk::encode(0), 1000, 1100).await?,
        "Handle 0 should not have version > 1000"
    );
    assert!(
        !lp.has_newer_version(&IntPk::encode(0), 1400, 1600).await?,
        "Handle 0 should not have version 1500 (not in file 3)"
    );

    // Test Handle 1: should have BOTH versions 1000 and 1500
    assert!(
        lp.has_newer_version(&IntPk::encode(1), 900, 1100).await?,
        "Handle 1 should have version 1000 (from file 1)"
    );
    assert!(
        lp.has_newer_version(&IntPk::encode(1), 1400, 1600).await?,
        "Handle 1 should also have version 1500 (from file 3)"
    );
    assert!(
        !lp.has_newer_version(&IntPk::encode(1), 1500, 1600).await?,
        "Handle 1 should not have version > 1500"
    );
    assert!(
        !lp.has_newer_version(&IntPk::encode(1), 1900, 2100).await?,
        "Handle 1 should not have version 2000 (not in file 2)"
    );

    // Test Handle 2: should have BOTH versions 1500 and 2000
    assert!(
        lp.has_newer_version(&IntPk::encode(2), 1400, 1600).await?,
        "Handle 2 should have version 1500 (from file 3)"
    );
    assert!(
        lp.has_newer_version(&IntPk::encode(2), 1900, 2100).await?,
        "Handle 2 should also have version 2000 (from file 2)"
    );
    assert!(
        !lp.has_newer_version(&IntPk::encode(2), 2000, 2100).await?,
        "Handle 2 should not have version > 2000"
    );
    assert!(
        !lp.has_newer_version(&IntPk::encode(2), 900, 1100).await?,
        "Handle 2 should not have version 1000 (not in file 1)"
    );

    // Test Handle 3: should have version 2000 only
    assert!(
        lp.has_newer_version(&IntPk::encode(3), 1900, 2100).await?,
        "Handle 3 should have version 2000 (from file 2)"
    );
    assert!(
        !lp.has_newer_version(&IntPk::encode(3), 2000, 2100).await?,
        "Handle 3 should not have version > 2000"
    );
    assert!(
        !lp.has_newer_version(&IntPk::encode(3), 1400, 1600).await?,
        "Handle 3 should not have version 1500 (not in file 3)"
    );

    // Test non-existent handles
    assert!(
        !lp.has_newer_version(&IntPk::encode(4), 0, u64::MAX).await?,
        "Handle 4 should not exist"
    );
    assert!(
        !lp.has_newer_version(&IntPk::encode(999), 0, u64::MAX)
            .await?,
        "Handle 999 should not exist"
    );

    Ok(())
}

#[tokio::test]
async fn test_index_id_filtering() -> Result<()> {
    let table_id = 600;

    // Create multiple FTS indexes on the same column but with different index_ids
    // Create schema with ALL three indexes
    let schema = SchemaBuilder::<IntPk>::new(table_id)
        .column(7, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id: 1,
            col_id: 7,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .fts_index(FullTextIndexDef {
            index_id: 2,
            col_id: 7, // Same column, different index_id
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .fts_index(FullTextIndexDef {
            index_id: 3,
            col_id: 7, // Same column, different index_id
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file = new_schema_file(&[&schema]);

    let columnar_file = new_columnar(51, 0)
        .table::<IntPk>(&schema, |row| {
            row(1, 1000, false, |datum| {
                datum("abc index filter one");
            });
            row(2, 1010, false, |datum| {
                datum("index filter middle");
            });
            row(3, 1020, false, |datum| {
                datum("abc index filter two");
            });
        })
        .finish_as_file();

    // KEY TEST: Only specify index_id=2, even though schema has indexes 1, 2, 3
    let opts = ColumnarToFtsL0Opts {
        columnar_files: &[columnar_file],
        schema_file,
        index_pairs: &[(table_id, 2)], // Only index_id=2, NOT 1 or 3
        encryption_key: None,
        snap_version: SnapVersion::zero(),
        l0_file_max_size: 0,
    };

    let file_data = columnar_to_fts_l0(opts).await?;
    assert!(
        !file_data.is_empty(),
        "Should generate FTS data for index 2"
    );

    let output = file_data.into_iter().next().unwrap();
    let packed_file = PackedFile::from_buffer(&output.data)?;

    // Verify: ONLY index_id=2 should exist, others should NOT
    let lp_index_1 = packed_file.cached_get_lp(&lp_key(600, 1)).await?;
    let lp_index_2 = packed_file.cached_get_lp(&lp_key(600, 2)).await?;
    let lp_index_3 = packed_file.cached_get_lp(&lp_key(600, 3)).await?;

    assert!(
        lp_index_1.is_none(),
        "Index 1 should NOT be built (not requested)"
    );
    assert!(
        lp_index_2.is_some(),
        "Index 2 should be built (explicitly requested)"
    );
    assert!(
        lp_index_3.is_none(),
        "Index 3 should NOT be built (not requested)"
    );

    // Verify the built index actually works
    // Extract Int variant
    let lp_2 = lp_index_2.unwrap();
    let lp_2 = lp_2.as_int_lp().unwrap();
    assert!(lp_2.props().get_n_pk() > 0, "Index 2 should have documents");

    // Verify search works on index 2
    let reader = lp_2.cached_read_index()?;
    let mut results = Vec::new();

    let query = make_unscored_query(&PlainFtsQueryInfo {
        query: "abc".to_string(),
        ..Default::default()
    });

    reader.search(&query, &mut results)?;
    assert!(!results.is_empty(), "Search should find results in index 2");

    Ok(())
}

#[tokio::test]
async fn test_columnar_to_fts_splits_l0_files() -> Result<()> {
    let table_id = 700;

    let schema = SchemaBuilder::<IntPk>::new(table_id)
        .column(2, FieldTypeTp::String)
        .fts_index(FullTextIndexDef {
            index_id: 1,
            col_id: 2,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .fts_index(FullTextIndexDef {
            index_id: 2,
            col_id: 2,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .fts_index(FullTextIndexDef {
            index_id: 3,
            col_id: 2,
            parser_type: "STANDARD_V1".to_string(),
            ..Default::default()
        })
        .schema();
    let schema_file = new_schema_file(&[&schema]);

    let columnar_file = new_columnar(1, 0)
        .table::<IntPk>(&schema, |row| {
            row(0, 1000, false, |datum| {
                datum("abc first entry");
            });
            row(1, 950, false, |datum| {
                datum("plain content");
            });
            row(2, 1020, false, |datum| {
                datum("another abc value");
            });
            row(3, 980, false, |datum| {
                datum("quiet note");
            });
        })
        .finish_as_file();

    let opts = ColumnarToFtsL0Opts {
        columnar_files: &[columnar_file],
        schema_file,
        index_pairs: &[(table_id, 1), (table_id, 2), (table_id, 3)],
        encryption_key: None,
        snap_version: SnapVersion::zero(),
        l0_file_max_size: 1, // Force split at every LP boundary.
    };

    let outputs = columnar_to_fts_l0(opts).await?;
    assert!(outputs.len() > 1, "Should split into multiple output files");

    let mut built_indexes = HashSet::new();
    for out in outputs {
        let packed_file = PackedFile::from_buffer(&out.data)?;
        for index_id in 1..=3 {
            if packed_file
                .cached_get_lp(&lp_key(table_id, index_id))
                .await?
                .is_some()
            {
                built_indexes.insert(index_id);
            }
        }
    }

    assert_eq!(
        built_indexes.len(),
        3,
        "All requested indexes should be built"
    );
    Ok(())
}
