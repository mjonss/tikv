// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Arc};

use api_version::ApiV2;
use async_trait::async_trait;
use log_wrappers::Value as LogValue;
use tikv_util::{
    codec::{bytes::encode_bytes, number::NumberEncoder},
    debug, info, warn,
};
use txn_types::TimeStamp;

use crate::{
    KvScanner, load_schema,
    schema::{STATE_PUBLIC, SchemaDiff, TableInfo},
};

const TIDB_SCHEMA_VERSION_KEY: &[u8] = b"SchemaVersionKey";
const TIDB_SCHEMA_DIFF_PREFIX: &[u8] = b"Diff";
const TIDB_META_KEY_PREFIX: u8 = b'm';
const STRING_DATA_TYPE: u8 = b's';
const HASH_DATA_TYPE: u8 = b'h';

const MAX_DIFF_VERSION: i64 = 256;

// Get schema version from meta.
pub async fn get_schema_version(
    kv_getter: Arc<dyn KvGetter>,
    keyspace_id: u32,
    start_ts: TimeStamp,
) -> Result<i64, String> {
    let schema_version_data = kv_getter
        .get(&schema_version_key(keyspace_id), start_ts)
        .await?;
    if schema_version_data.is_none() {
        return Ok(0);
    }
    let schema_version = String::from_utf8(schema_version_data.unwrap())
        .unwrap()
        .parse::<i64>()
        .unwrap();
    Ok(schema_version)
}

// Sync schema for keyspace_id if needed.
pub async fn sync_schema(
    kv_getter: Arc<dyn KvGetter>,
    kv_scanner: Arc<dyn KvScanner>,
    keyspace_id: u32,
    cur_version: Option<i64>,
    start_ts: TimeStamp,
) -> Result<(i64, Vec<TableInfo>), String> {
    // Get schema version key from meta.
    let schema_version_data = kv_getter
        .get(&schema_version_key(keyspace_id), start_ts)
        .await?;
    // This is a empty keyspace.
    if schema_version_data.is_none() {
        return Ok((0, vec![]));
    }
    let schema_version = String::from_utf8(schema_version_data.unwrap())
        .unwrap()
        .parse::<i64>()
        .unwrap();
    if let Some(cur_version) = cur_version {
        if cur_version == schema_version {
            return Ok((schema_version, vec![]));
        }
        let mut table_infos = HashMap::new();
        if cur_version > schema_version {
            debug_assert!(false, "{} {} {}", keyspace_id, cur_version, schema_version);
            warn!("{}: sync_schema: current version > schema meta, need full sync", keyspace_id;
                "cur_ver" => cur_version, "schema_ver" => schema_version);
            return sync_all_schemas(kv_getter, kv_scanner, keyspace_id, schema_version, start_ts)
                .await;
        } else if cur_version + MAX_DIFF_VERSION < schema_version {
            info!("{}: sync_schema: diff too large, need full sync", keyspace_id;
                "cur_ver" => cur_version, "schema_ver" => schema_version);
            return sync_all_schemas(kv_getter, kv_scanner, keyspace_id, schema_version, start_ts)
                .await;
        }

        debug!(
            "{}: sync_schema: current version {}, schema meta version {}",
            keyspace_id, cur_version, schema_version
        );
        let version_range = cur_version + 1..=schema_version;
        let schema_diffs =
            get_schema_diff(kv_getter.clone(), version_range, keyspace_id, start_ts).await?;
        debug!(
            "{}: sync_schema: schema diffs {:?}",
            keyspace_id, schema_diffs
        );
        let last_diff_ver = schema_diffs
            .last()
            .map(|diff| diff.version)
            .unwrap_or(cur_version);
        if last_diff_ver < schema_version {
            warn!("{}: sync_schema: some schema diffs not synced", keyspace_id;
                "schema_ver" => schema_version, "last_diff_ver" => last_diff_ver);
        }
        for diff in schema_diffs {
            if diff.regenerate_schema_map {
                return sync_all_schemas(
                    kv_getter,
                    kv_scanner,
                    keyspace_id,
                    schema_version,
                    start_ts,
                )
                .await;
            }
            let db_id = diff.schema_id;
            if db_id == 1 {
                continue;
            }

            let table_id = diff.table_id;
            // If the table info already fetched, skip to avoid useless repeating fetching.
            if table_infos.contains_key(&table_id) {
                continue;
            }
            let schema_data_key = schema_data_key(keyspace_id, db_id, table_id);
            let schema_data = kv_getter
                .get(&schema_data_key, start_ts)
                .await?
                .unwrap_or_default();
            if schema_data.is_empty() {
                warn!(
                    "sync_schema, schema data in diff keyspace: {} db: {} table: {}, diff: {} not found, skip",
                    keyspace_id, db_id, table_id, diff.version
                );
                continue;
            }
            let res: serde_json::Result<TableInfo> = serde_json::from_slice(&schema_data);
            match res {
                Ok(tbl) => {
                    debug!(
                        "sync_schema, keyspace {} table {} schema {:?}",
                        keyspace_id, table_id, tbl
                    );
                    if tbl.state == STATE_PUBLIC {
                        table_infos.insert(table_id, tbl);
                    }
                }
                Err(err) => {
                    let val_str = String::from_utf8_lossy(&schema_data);
                    warn!(
                        "{}: invalid key {:?}, val {} err {:?}",
                        keyspace_id, schema_data_key, val_str, err
                    );
                }
            }
        }

        Ok((last_diff_ver, table_infos.into_values().collect()))
    } else {
        sync_all_schemas(kv_getter, kv_scanner, keyspace_id, schema_version, start_ts).await
    }
}

async fn sync_all_schemas(
    kv_getter: Arc<dyn KvGetter>,
    kv_scanner: Arc<dyn KvScanner>,
    keyspace_id: u32,
    schema_version: i64,
    start_ts: TimeStamp,
) -> Result<(i64, Vec<TableInfo>), String> {
    let keyspace_prefix = ApiV2::get_keyspace_prefix_by_id(keyspace_id);
    let db_infos = load_schema(kv_scanner, &keyspace_prefix, start_ts).await?;

    // Check the latest schema diff exists, if not, use the previous schema version.
    // In TiDB, the schema version key and schema diff key are not persisted in the
    // same transaction. If we return `schema_version` directly, the last schema
    // diff key will be missed.
    let latest_schema_diff_exists =
        schema_diff_key_exists(kv_getter.clone(), keyspace_id, schema_version, start_ts).await?;
    let schema_version = if latest_schema_diff_exists {
        schema_version
    } else {
        schema_version - 1
    };
    Ok((
        schema_version,
        db_infos
            .into_iter()
            .flat_map(|db_info| db_info.tables)
            .collect(),
    ))
}

async fn get_schema_diff(
    kv_getter: Arc<dyn KvGetter>,
    range: impl IntoIterator<Item = i64>,
    keyspace_id: u32,
    start_ts: TimeStamp,
) -> Result<Vec<SchemaDiff>, String> {
    let schema_vers = range.into_iter().collect::<Vec<_>>();
    let mut schema_diff_keys = Vec::with_capacity(schema_vers.len());
    schema_vers.iter().for_each(|&ver| {
        schema_diff_keys.push(schema_diff_key(keyspace_id, ver));
    });
    let schema_diffs = kv_getter.batch_get(&schema_diff_keys, start_ts).await?;
    debug_assert_eq!(schema_diff_keys.len(), schema_diffs.len());

    let mut schema_diffs_res = Vec::with_capacity(schema_diffs.len());
    for ((&ver, diff_key), schema_diff) in schema_vers
        .iter()
        .zip(schema_diff_keys.iter())
        .zip(schema_diffs.into_iter())
    {
        if schema_diff.is_none() {
            warn!("{}: sync_schema: schema diff not found", keyspace_id;
                "ver" => ver, "diff_key" => LogValue::key(diff_key));
            continue;
        }
        let schema_diff = schema_diff.unwrap();
        let res: serde_json::Result<SchemaDiff> = serde_json::from_slice(&schema_diff);
        match res {
            Ok(schema_diff) => {
                debug_assert_eq!(
                    schema_diff.version, ver,
                    "schema diff version mismatch: {:?}",
                    schema_diff
                );
                schema_diffs_res.push(schema_diff);
            }
            Err(err) => {
                return Err(format!(
                    "keyspace {keyspace_id} schema diff ver {ver} decode failed: {err:?}",
                ));
            }
        }
    }
    Ok(schema_diffs_res)
}

async fn schema_diff_key_exists(
    kv_getter: Arc<dyn KvGetter>,
    keyspace_id: u32,
    schema_version: i64,
    start_ts: TimeStamp,
) -> Result<bool, String> {
    let schema_diff_key = schema_diff_key(keyspace_id, schema_version);
    let exists = kv_getter.get(&schema_diff_key, start_ts).await?;
    Ok(exists.is_some())
}

fn schema_version_key(keyspace_id: u32) -> Vec<u8> {
    let mut key = api_version::ApiV2::get_keyspace_prefix_by_id(keyspace_id);
    let encoded_key = encode_bytes(TIDB_SCHEMA_VERSION_KEY);
    key.reserve(encoded_key.len() + 1 + 8);
    key.push(TIDB_META_KEY_PREFIX);
    key.extend_from_slice(&encoded_key);
    key.encode_u64(STRING_DATA_TYPE as u64).unwrap();
    key
}

fn schema_diff_key(keyspace_id: u32, ver: i64) -> Vec<u8> {
    let ver_str = ver.to_string();
    let mut key = api_version::ApiV2::get_keyspace_prefix_by_id(keyspace_id);
    let mut raw_key = TIDB_SCHEMA_DIFF_PREFIX.to_vec();
    raw_key.push(b':');
    raw_key.extend_from_slice(ver_str.as_bytes());
    let encoded_key = encode_bytes(&raw_key);
    key.reserve(encoded_key.len() + 1 + 8);
    key.push(TIDB_META_KEY_PREFIX);
    key.extend_from_slice(&encoded_key);
    key.encode_u64(STRING_DATA_TYPE as u64).unwrap();
    key
}

fn schema_data_key(keyspace_id: u32, db_id: i64, table_id: i64) -> Vec<u8> {
    let mut key = api_version::ApiV2::get_keyspace_prefix_by_id(keyspace_id);
    let enc_db_key = encode_bytes(format!("DB:{}", db_id).as_bytes());
    let enc_table_key = encode_bytes(format!("Table:{}", table_id).as_bytes());
    key.reserve(enc_db_key.len() + enc_table_key.len() + 1 + 8);
    key.push(TIDB_META_KEY_PREFIX);
    key.extend_from_slice(&enc_db_key);
    key.encode_u64(HASH_DATA_TYPE as u64).unwrap();
    key.extend_from_slice(&enc_table_key);

    key
}

#[async_trait]
pub trait KvGetter: Send + Sync {
    async fn get(&self, key: &[u8], start_ts: TimeStamp) -> Result<Option<Vec<u8>>, String>;
    async fn batch_get(
        &self,
        keys: &[Vec<u8>],
        start_ts: TimeStamp,
    ) -> Result<Vec<Option<Vec<u8>>>, String>;
}

#[cfg(feature = "testexport")]
pub mod test_utils {
    use std::collections::HashMap;

    use super::*;
    use crate::schema::{DbInfo, PartitionDefinition, PartitionInfo, StorageClassSpec};

    const TIDB_DBS: &[u8] = b"DBs";

    // The method generates storage class schema data for testing the schema
    // manager.
    pub fn generate_storage_class_schema_data_for_test(
        keyspace_id: u32,
        db_id: i64,
        table_id: i64,
        partition_id: Option<i64>, // The partition to be set as `storage_class`.
        partitions: Option<Vec<i64>>, // All partitions.
        schema_version: i64,
        spec: StorageClassSpec,
        current: &mut Option<HashMap<i64 /* table_id */, TableInfo>>,
    ) -> Result<HashMap<Vec<u8>, Vec<u8>>, String> {
        let mut kv_pairs: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        if current.is_none() {
            // db info
            let db_key = dbs_db_key(keyspace_id, db_id);
            let mut db_info = DbInfo::default();
            db_info.id = db_id;
            db_info.state = STATE_PUBLIC;
            let db_data = serde_json::to_string(&db_info).map_err(|e| e.to_string())?;
            kv_pairs.insert(db_key, db_data.into_bytes());
        } else {
            // schema diff
            let diff_key = schema_diff_key(keyspace_id, schema_version);
            let mut schema_diff = SchemaDiff::default();
            schema_diff.version = schema_version;
            schema_diff.schema_id = db_id;
            schema_diff.table_id = table_id;
            let diff_value = serde_json::to_string(&schema_diff).map_err(|e| e.to_string())?;
            kv_pairs.insert(diff_key, diff_value.into_bytes());
        }

        let mut tables = current.take().unwrap_or_default();

        // table info
        let schema_data_key = schema_data_key(keyspace_id, db_id, table_id);
        let mut table_info = tables.remove(&table_id).unwrap_or_else(|| {
            let mut table_info = TableInfo::default();
            table_info.id = table_id;
            table_info.state = STATE_PUBLIC;
            table_info
        });

        if let Some(partitions) = partitions {
            let definitions = partitions
                .into_iter()
                .map(|id| PartitionDefinition {
                    id,
                    ..Default::default()
                })
                .collect();
            let par_info = PartitionInfo {
                definitions,
                ..Default::default()
            };
            table_info.partition = Some(par_info);
        }

        let (sc_tier, sc_trans) = spec.to_tidb();
        if let Some(partition_id) = partition_id {
            let part_def = table_info
                .partition
                .as_mut()
                .unwrap()
                .definitions
                .iter_mut()
                .find(|p| p.id == partition_id)
                .unwrap();
            part_def.storage_class_tier = sc_tier;
            part_def.storage_class_transitions = sc_trans;
        } else {
            table_info.storage_class_tier = sc_tier;
            table_info.storage_class_transitions = sc_trans;
        };

        let schema_data = serde_json::to_string(&table_info).map_err(|e| e.to_string())?;
        kv_pairs.insert(schema_data_key, schema_data.into_bytes());

        // schema version
        kv_pairs.insert(
            schema_version_key(keyspace_id).to_vec(),
            schema_version.to_string().into_bytes(),
        );

        tables.insert(table_id, table_info.clone());
        *current = Some(tables);
        Ok(kv_pairs)
    }

    fn dbs_db_key(keyspace_id: u32, db_id: i64) -> Vec<u8> {
        let mut key = api_version::ApiV2::get_keyspace_prefix_by_id(keyspace_id);
        let enc_dbs = encode_bytes(TIDB_DBS);
        let enc_db_key = encode_bytes(format!("DB:{}", db_id).as_bytes());
        key.reserve(enc_dbs.len() + enc_db_key.len() + 1 + 8);
        key.push(TIDB_META_KEY_PREFIX);
        key.extend_from_slice(&enc_dbs);
        key.encode_u64(HASH_DATA_TYPE as u64).unwrap();
        key.extend_from_slice(&enc_db_key);

        key
    }
}
