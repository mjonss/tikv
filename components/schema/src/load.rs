// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use tikv_util::{
    codec::{
        bytes::{decode_bytes, encode_bytes},
        number::NumberEncoder,
    },
    debug, error, warn,
};
use txn_types::TimeStamp;

use crate::schema::{DbInfo, STATE_PUBLIC, TableInfo};

const M_DBS: &[u8] = b"mDBs";
const M_TABLE_PREFIX: &[u8] = b"Table";

pub async fn load_schema(
    kv_scanner: Arc<dyn KvScanner>,
    keyspace: &[u8],
    start_ts: TimeStamp,
) -> Result<Vec<DbInfo>, String> {
    let mut start = Vec::with_capacity(keyspace.len() + M_DBS.len());
    start.extend_from_slice(keyspace);
    start.extend_from_slice(M_DBS);
    let mut end = start.clone();
    let last_idx = end.len() - 1;
    end[last_idx] += 1;
    let pairs = kv_scanner.scan(&start, &end, start_ts).await?;
    debug!(
        "scan pairs {}, start {:?}, end {:?}",
        pairs.len(),
        start,
        end
    );
    let mut dbs = vec![];
    for (key, value) in pairs {
        let db: DbInfo = serde_json::from_slice(value.as_slice()).map_err(|err| {
            let key = log_wrappers::hex_encode_upper(&key);
            let val = String::from_utf8_lossy(&value);
            error!(
                "load schema: parse db info failed";
                "err" => ?err, "key" => &key, "val" => val.as_ref(),
            );
            format!("parse db info failed for {}", key)
        })?;
        if db.state == STATE_PUBLIC {
            dbs.push(db);
        }
    }
    for db in &mut dbs {
        let start_key = db_tables_start_key(keyspace, db.id);
        let prefix_len = start_key.len();
        let (start, end) = get_range_keys(start_key);
        let pairs = kv_scanner.scan(&start, &end, start_ts).await?;
        for (key, value) in pairs {
            let mut suffix = &key[prefix_len..];
            let field = decode_bytes(&mut suffix, false).unwrap();
            if !field.starts_with(M_TABLE_PREFIX) {
                continue;
            }
            let res: serde_json::Result<TableInfo> = serde_json::from_slice(&value);
            match res {
                Ok(tbl) => {
                    if tbl.state == STATE_PUBLIC {
                        db.tables.push(tbl);
                    }
                }
                Err(err) => {
                    let val_str = String::from_utf8_lossy(&value);
                    warn!("invalid key {:?}, val {} err {:?}", key, val_str, err);
                }
            }
        }
    }
    Ok(dbs)
}

fn db_tables_start_key(keyspace: &[u8], db_id: i64) -> Vec<u8> {
    let mut db_tables_key = vec![];
    db_tables_key.extend_from_slice(keyspace);
    db_tables_key.push(b'm');
    let encoded_db_str = encode_bytes(format!("DB:{}", db_id).as_bytes());
    db_tables_key.extend_from_slice(encoded_db_str.as_slice());
    db_tables_key.encode_u64('h' as u64).unwrap();
    db_tables_key
}

fn get_range_keys(start_key: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
    let mut end_key = start_key.to_vec();
    let last_idx = end_key.len() - 1;
    assert!(end_key[last_idx] != 0xff);
    end_key[last_idx] += 1;
    (start_key, end_key)
}

#[async_trait]
pub trait KvScanner: Send + Sync {
    async fn scan(
        &self,
        start: &[u8],
        end: &[u8],
        start_ts: TimeStamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String>;
}
