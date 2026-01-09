// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt, mem, sync::Arc, time::Duration};

use api_version::ApiV2;
use bytes::Bytes;
use cloud_worker::get_keyspace_stats_from_store;
use codec::number::NumberEncoder;
use hyper::{http, Body};
use kvengine::table::{
    columnar::{new_int_handle_column_info, new_version_column_info},
    schema_file::{Schema, SchemaBuf, SchemaFile},
};
use kvproto::{
    kvrpcpb, metapb,
    metapb::{Peer, RegionEpoch, Store},
};
use log_wrappers::Value;
use rfstore::store::RegionIdVer;
use schema::schema::StorageClassSpec;
use security::{SecurityConfig, SecurityManager};
use tidb_query_datatype::{codec::table::TABLE_PREFIX, Collation, FieldTypeTp};
use tikv::storage::mvcc::Key;
use tikv_util::{
    codec::bytes::{decode_bytes, encode_bytes},
    info,
    time::Instant,
    warn,
};
use tipb::ColumnInfo;

pub(crate) const DEFAULT_INNER_KEY_OFFSET: usize = 4;

/// A cheaply cloneable version of `kvrpcpb::Mutation`.
#[derive(Default, Clone)]
pub struct Mutation {
    pub op: kvrpcpb::Op,
    pub key: Bytes,
    pub value: Bytes,
    pub assertion: kvrpcpb::Assertion,
}

impl fmt::Debug for Mutation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mutation")
            .field("op", &self.op)
            .field("key", &Value::key(&self.key))
            .field("value", &Value::value(&self.value))
            .finish()
    }
}

impl From<&Mutation> for kvrpcpb::Mutation {
    fn from(m: &Mutation) -> Self {
        kvrpcpb::Mutation {
            op: m.op,
            key: m.key.to_vec(),
            value: m.value.to_vec(),
            assertion: m.assertion,
            ..Default::default()
        }
    }
}

impl Mutation {
    pub fn get_op(&self) -> kvrpcpb::Op {
        self.op
    }

    pub fn set_op(&mut self, op: kvrpcpb::Op) {
        self.op = op;
    }

    pub fn get_key(&self) -> &[u8] {
        &self.key
    }

    pub fn set_key(&mut self, key: Vec<u8>) {
        self.key = key.into();
    }

    pub fn get_value(&self) -> &[u8] {
        &self.value
    }

    pub fn set_value(&mut self, value: Vec<u8>) {
        self.value = value.into();
    }

    pub fn take_key(&mut self) -> Vec<u8> {
        mem::take(&mut self.key).into()
    }

    pub fn take_value(&mut self) -> Vec<u8> {
        mem::take(&mut self.value).into()
    }

    pub fn get_assertion(&self) -> kvrpcpb::Assertion {
        self.assertion
    }

    pub fn set_assertion(&mut self, assertion: kvrpcpb::Assertion) {
        self.assertion = assertion;
    }
}

#[derive(Clone)]
pub struct RawRegion {
    pub id: u64,
    pub raw_start: Vec<u8>,
    pub raw_end: Vec<u8>,
    pub epoch: RegionEpoch,
    pub peers: Vec<Peer>,
    pub leader_idx: usize,
}

impl fmt::Debug for RawRegion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawRegion")
            .field("id", &self.id)
            .field("raw_start", &Value::key(&self.raw_start))
            .field("raw_end", &Value::key(&self.raw_end))
            .field("epoch", &self.epoch)
            .field("peers", &self.peers)
            .field("leader_idx", &self.leader_idx)
            .finish()
    }
}

impl From<metapb::Region> for RawRegion {
    fn from(mut region: metapb::Region) -> Self {
        let raw_start = if region.start_key.is_empty() {
            vec![]
        } else {
            let mut slice = region.start_key.as_slice();
            decode_bytes(&mut slice, false).unwrap()
        };
        let raw_end = if region.end_key.is_empty() {
            vec![255; 8]
        } else {
            let mut slice = region.end_key.as_slice();
            decode_bytes(&mut slice, false).unwrap()
        };
        RawRegion {
            id: region.id,
            raw_start,
            raw_end,
            epoch: region.take_region_epoch(),
            peers: region.take_peers().into_vec(),
            leader_idx: 0,
        }
    }
}

impl RawRegion {
    pub fn get_leader(&self) -> &Peer {
        &self.peers[self.leader_idx]
    }

    pub fn id_ver(&self) -> RegionIdVer {
        RegionIdVer::new(self.id, self.epoch.version)
    }

    pub fn update_leader(&mut self, leader: &Peer) -> bool {
        if let Some(idx) = self.peers.iter().position(|p| p.id == leader.id) {
            self.leader_idx = idx;
            true
        } else {
            false
        }
    }

    pub fn raw_start(&self) -> &[u8] {
        &self.raw_start
    }

    pub fn raw_end(&self) -> &[u8] {
        &self.raw_end
    }

    pub fn equal(&self, other: &Self) -> bool {
        self.id == other.id
            && self.epoch == other.epoch
            && self.get_leader().id == other.get_leader().id
    }
}

impl pd_client::util::RegionLike for RawRegion {
    const KEY_ENCODED: bool = false;

    fn id(&self) -> u64 {
        self.id
    }

    fn epoch(&self) -> &metapb::RegionEpoch {
        &self.epoch
    }

    fn start_key(&self) -> &[u8] {
        &self.raw_start
    }

    fn end_key(&self) -> &[u8] {
        &self.raw_end
    }
}

#[derive(Default)]
pub struct TableSchemaOptions {
    pub table_id: i64,
    pub with_columns: bool,
    pub storage_class_spec: StorageClassSpec,
}

pub fn build_schemas(tables: &[TableSchemaOptions]) -> Vec<Schema> {
    let mut schemas = vec![];
    for opts in tables {
        let mut schema = SchemaBuf {
            table_id: opts.table_id,
            ..Default::default()
        };
        if opts.with_columns {
            let mut c1 = ColumnInfo::new();
            c1.set_column_id(1);
            c1.set_tp(FieldTypeTp::LongLong.to_u8().unwrap() as i32);
            let mut c2 = ColumnInfo::new();
            c2.set_column_id(2);
            c2.set_tp(FieldTypeTp::VarChar.to_u8().unwrap() as i32);
            c2.set_column_len(255);
            c2.set_collation(Collation::Utf8Mb4Bin as i32);

            schema = SchemaBuf::new(
                opts.table_id,
                new_int_handle_column_info(),
                new_version_column_info(),
                vec![c1, c2],
                vec![],
                0,
                vec![],
                vec![],
                StorageClassSpec::default(),
                None,
            )
        }

        if opts.storage_class_spec.is_specified() {
            schema.set_storage_class_spec(opts.storage_class_spec.clone());
        }

        schemas.push(schema.into());
    }
    schemas
}

pub fn get_keyspace_split_keys(keyspace_id: u32) -> Vec<Vec<u8>> {
    vec![
        ApiV2::get_keyspace_prefix_by_id(keyspace_id),
        ApiV2::get_keyspace_prefix_by_id(keyspace_id + 1),
    ]
    .into_iter()
    .filter_map(|k| (!k.is_empty()).then(|| Key::from_raw(&k).into_encoded()))
    .collect()
}

pub fn get_table_split_keys(keyspace_id: u32, table_ids: &[i64]) -> Vec<Vec<u8>> {
    let keyspace_prefix = ApiV2::get_keyspace_prefix_by_id(keyspace_id);
    let mut dup_table_ids = table_ids
        .iter()
        .flat_map(|&id| [id, id + 1])
        .collect::<Vec<_>>();
    dup_table_ids.sort();
    dup_table_ids.dedup();
    dup_table_ids
        .into_iter()
        .map(|tbl_id| {
            let mut buf = vec![];
            buf.extend_from_slice(&keyspace_prefix);
            buf.extend_from_slice(TABLE_PREFIX);
            buf.write_i64(tbl_id).unwrap();
            encode_bytes(&buf)
        })
        .collect()
}

pub async fn broadcast_schema_file_request(
    stores: &[Store],
    keyspace_id: u32,
    schema_file_id: u64,
    timeout: Duration,
) {
    let mut js = tokio::task::JoinSet::new();
    for store in stores {
        let status_addr = store.get_status_address();
        let uri = format!(
            "http://{}/schema_file?keyspace_id={}&file_id={}",
            status_addr, keyspace_id, schema_file_id
        );
        let task = async move {
            let http_client = hyper::client::Client::new();
            let start_time = Instant::now_coarse();
            while start_time.saturating_elapsed() < timeout {
                let request = hyper::http::Request::builder()
                    .method(http::method::Method::POST)
                    .uri(&uri)
                    .body(Body::empty())
                    .unwrap();
                match http_client.request(request).await {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            info!("broadcast schema file succeed"; "uri" => &uri);
                            return;
                        } else {
                            warn!("broadcast schema file failed"; "uri" => &uri, "resp" => ?resp);
                        }
                    }
                    Err(e) => {
                        warn!("broadcast schema file failed"; "uri" => &uri, "error" => ?e);
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            panic!("broadcast schema file timeout, uri {}", uri);
        };
        js.spawn(task);
    }

    while let Some(res) = js.join_next().await {
        res.unwrap();
    }
}

pub async fn check_schema_file_applied(
    stores: &[Store],
    keyspace_id: u32,
    schema_file: &SchemaFile,
    timeout: Duration,
) -> bool {
    let security_mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    let (ks_start, ks_end) = ApiV2::get_keyspace_range_by_id(keyspace_id);
    for store in stores {
        let shard_stats = get_keyspace_stats_from_store(store, security_mgr.clone(), timeout)
            .await
            .unwrap();
        let not_applied_shards = shard_stats
            .iter()
            .filter(|s| {
                ks_start <= s.start
                    && s.end <= ks_end
                    && schema_file.overlap(&s.start, &s.end, keyspace_id)
                    && s.schema_version < schema_file.get_version()
            })
            .collect::<Vec<_>>();
        if !not_applied_shards.is_empty() {
            info!("shards not applied to schema version {}", schema_file.get_version();
                "store" => store.id, "keyspace" => keyspace_id, "shards" => ?not_applied_shards,
            );
            return false;
        }
    }
    true
}

pub async fn broadcast_schema_file_request_and_check(
    stores: &[Store],
    keyspace_id: u32,
    schema_file: &SchemaFile,
    timeout: Duration,
) {
    let start_time = Instant::now_coarse();
    while start_time.saturating_elapsed() < timeout {
        broadcast_schema_file_request(stores, keyspace_id, schema_file.get_file_id(), timeout)
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        if check_schema_file_applied(stores, keyspace_id, schema_file, timeout).await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    panic!("broadcast_schema_file_request_and_check timeout");
}
