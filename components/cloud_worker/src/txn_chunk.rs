// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Arc, time::Duration};

use api_version::ApiV2;
use bytes::{Buf, Bytes};
use cloud_encryption::EncryptionKey;
use dashmap::DashMap;
use http::{Response, StatusCode, header, request::Parts};
use hyper::Body;
use kvengine::{
    ENCRYPTION_KEY,
    dfs::{self, Dfs},
    get_shard_property,
    table::{ChecksumType, InnerKey, txn_file::TxnChunkBuilder},
};
use load_data::dispatcher::get_shard_meta;
use tikv_util::{box_err, warn};

use crate::{
    common::{get_param, make_response},
    error::{Error, Result},
    server::Context,
};

const GET_SHARD_META_TIMEOUT: Duration = Duration::from_secs(60);
pub(crate) const TARGET_BLOCK_SIZE_DEF: usize = 65536; // 64KB

/// Txn Chunk API
///
/// * GET /txn_chunk?keyspace_id=<keyspace_id>: query txn file is available for
///   this keyspace or not
///
///   Return: { "available": true/false }
///
/// * POST /txn_chunk?keyspace_id=<keyspace_id>: submit data for creating txn
///   chunk.
///
///   Return: { "chunk_id": <chunk_id> }
pub(crate) async fn handle_txn_chunk(
    ctx: Arc<Context>,
    parts: Parts,
    body: Bytes,
) -> hyper::Result<Response<Body>> {
    let query = parts.uri.query().unwrap_or("");
    let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();

    let keyspace_id = match get_param::<u32>(&query_pairs, "keyspace_id") {
        None => {
            return Ok(make_response(
                StatusCode::BAD_REQUEST,
                "missing keyspace id",
            ));
        }
        Some(keyspace_id) => keyspace_id,
    };
    let _keyspace_permit = ctx.worker_limiter.acquire_permit(keyspace_id).await;
    let keyspace_info = match ctx
        .txn_chunk_handler
        .acquire_keyspace_info(ctx.clone(), keyspace_id)
        .await
    {
        Ok(keyspace_info) => keyspace_info,
        Err(err) => {
            return Ok(make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to acquire keyspace info {:?}", err),
            ));
        }
    };

    // TODO: remove this API after client-go is updated.
    if parts.method == http::Method::GET {
        let resp = GetAvailabilityResp { available: true };
        let json = serde_json::to_string(&resp).unwrap();
        return Ok(Response::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .body(json.into())
            .unwrap());
    }

    let chunk_id = match ctx.pd.get_tso().await {
        Ok(tso) => tso.into_inner(),
        Err(err) => {
            return Ok(make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to create txn chunk file {:?}", err),
            ));
        }
    };
    create_txn_chunk(
        chunk_id,
        ctx.dfs.clone(),
        parts,
        body,
        &keyspace_info,
        ctx.txn_chunk_handler.target_block_size,
    )
    .await
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct GetAvailabilityResp {
    pub available: bool,
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct CreateTxnChunkResp {
    pub chunk_id: u64,
}

pub(crate) async fn create_txn_chunk(
    chunk_id: u64,
    dfs: Arc<dyn Dfs>,
    parts: Parts,
    body: Bytes,
    keyspace_info: &KeyspaceInfo,
    target_block_size: usize,
) -> hyper::Result<Response<Body>> {
    if parts.method != http::Method::POST {
        return Ok(make_response(StatusCode::BAD_REQUEST, "invalid method"));
    }
    if body.len() < 4 {
        return Ok(make_response(StatusCode::BAD_REQUEST, "body is invalid"));
    }
    let data_len = body.len() - 4;
    let checksum = (&body[data_len..]).get_u32_le();
    let mut body_buf = &body[..data_len];
    if ChecksumType::Crc32.checksum(body_buf) != checksum {
        return Ok(make_response(StatusCode::BAD_REQUEST, "checksum mismatch"));
    }
    let mut txn_chunk_builder = TxnChunkBuilder::new(
        chunk_id,
        target_block_size,
        keyspace_info.encryption_key.clone(),
    );
    while !body_buf.is_empty() {
        let key_len = body_buf.get_u16_le() as usize;
        let key = &body_buf[..key_len];
        body_buf.advance(key_len);
        let op = body_buf.get_u8();
        let val_len = body_buf.get_u32_le() as usize;
        let val = &body_buf[..val_len];
        body_buf.advance(val_len);
        txn_chunk_builder.add_entry(InnerKey::from_outer_key(key), op, val);
    }
    drop(body);
    let mut txn_chunk_buf = vec![];
    txn_chunk_builder.finish(&mut txn_chunk_buf);
    let opts = dfs::Options::default().with_type(dfs::FileType::TxnChunk);
    if let Err(err) = dfs.create(chunk_id, txn_chunk_buf.into(), opts).await {
        return Ok(make_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to create txn chunk file {:?}", err),
        ));
    }
    let resp = CreateTxnChunkResp { chunk_id };
    let json = serde_json::to_string(&resp).unwrap();
    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .body(json.into())
        .unwrap())
}

#[derive(Clone)]
pub(crate) struct KeyspaceInfo {
    encryption_key: Option<EncryptionKey>,
}

pub(crate) struct TxnChunkHandler {
    target_block_size: usize,
    keyspaces: DashMap<u32 /* keyspace_id */, KeyspaceInfo>,
}

impl TxnChunkHandler {
    pub(crate) fn new(target_block_size: usize) -> Self {
        Self {
            target_block_size,
            keyspaces: DashMap::new(),
        }
    }

    async fn acquire_keyspace_info(
        &self,
        ctx: Arc<Context>,
        keyspace_id: u32,
    ) -> Result<KeyspaceInfo> {
        if let Some(keyspace_info) = self.keyspaces.get(&keyspace_id) {
            return Ok(keyspace_info.value().clone());
        }

        let (keyspace_start, keyspace_end) = ApiV2::get_txn_keyspace_range(keyspace_id);
        let shard_meta = get_shard_meta(ctx.pd.clone(), &keyspace_start, GET_SHARD_META_TIMEOUT)
            .await
            .map_err(|err| {
                Error::Other(box_err!(
                    "get shard meta failed: {:?}, keyspace_id: {}",
                    err,
                    keyspace_id
                ))
            })?;
        let snapshot = shard_meta.get_snapshot();

        if snapshot.outer_start < keyspace_start || keyspace_end < snapshot.outer_end {
            // Would happen if the keyspace has not been created or has been merged.
            warn!("acquire_keyspace_info: keyspace range mismatch"; "keyspace_id" => keyspace_id, "snapshot" => ?snapshot);
            return Err(box_err!("keyspace range mismatch"));
        }

        let encryption_key =
            get_shard_property(ENCRYPTION_KEY, snapshot.get_properties()).map(|exported_key| {
                ctx.master_key
                    .decrypt_encryption_key(&exported_key)
                    .unwrap()
            });

        let keyspace_info = KeyspaceInfo { encryption_key };
        self.keyspaces.insert(keyspace_id, keyspace_info.clone());
        Ok(keyspace_info)
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, sync::Arc};

    use bytes::{Buf, BufMut};
    use futures::{StreamExt, executor::block_on};
    use http::Method;
    use kvengine::{
        GLOBAL_SHARD_END_KEY, Iterator, UserMeta, dfs,
        dfs::{Dfs, FileType, InMemFs},
        table::{
            InnerKey, TxnChunk, TxnCtx, TxnFile, TxnFileId, TxnFileIterator, file::InMemFile,
            sstable::BlockCache,
        },
    };

    use crate::txn_chunk::{
        CreateTxnChunkResp, KeyspaceInfo, TARGET_BLOCK_SIZE_DEF, create_txn_chunk,
    };

    #[test]
    fn test_create_txn_chunk() {
        let mut req_body = vec![];
        let buf = &mut req_body;
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let val = format!("val{:03}", i);
            buf.put_u16_le(key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            buf.put_u8(1);
            buf.put_u32_le(val.len() as u32);
            buf.extend_from_slice(val.as_bytes());
        }
        let check_sum = crc32fast::hash(buf);
        buf.put_u32_le(check_sum);
        let chunk_id = 155;
        let dfs: Arc<dyn Dfs> = Arc::new(InMemFs::new());
        let req = http::Request::builder()
            .method(Method::POST)
            .body(hyper::Body::from(req_body))
            .unwrap();
        let keyspace_info = KeyspaceInfo {
            encryption_key: None,
        };
        let (parts, body) = req.into_parts();
        let body = block_on(hyper::body::to_bytes(body)).unwrap();
        let mut res = dfs
            .get_runtime()
            .block_on(create_txn_chunk(
                chunk_id,
                dfs.clone(),
                parts,
                body,
                &keyspace_info,
                TARGET_BLOCK_SIZE_DEF,
            ))
            .unwrap();
        assert!(res.status().is_success());
        let body = dfs
            .get_runtime()
            .block_on(res.body_mut().next())
            .unwrap()
            .unwrap();
        let resp: CreateTxnChunkResp = serde_json::from_slice(body.chunk()).unwrap();
        assert_eq!(resp.chunk_id, 155);
        let opts = dfs::Options::default().with_type(FileType::TxnChunk);
        let chunk_data = dfs
            .get_runtime()
            .block_on(dfs.read_file(chunk_id, opts))
            .unwrap();
        assert!(!chunk_data.is_empty());
        let txn_chunk = TxnChunk::new(
            Arc::new(InMemFile::new(155, chunk_data)),
            BlockCache::None,
            None,
        )
        .unwrap();
        let user_meta = UserMeta::new(1, 2).to_array().to_vec();
        let lower_bound = InnerKey::from_inner_buf(b"");
        let upper_bound = InnerKey::from_inner_buf(GLOBAL_SHARD_END_KEY);
        let txn_ctx = TxnCtx::new(user_meta.into(), vec![].into(), 2, lower_bound, upper_bound);
        let txn_file = TxnFile::new(TxnFileId::new(1, 1, 1), vec![txn_chunk], txn_ctx).unwrap();
        let mut iter = TxnFileIterator::new(txn_file, false);
        iter.rewind();
        let mut i = 0;
        while iter.valid() {
            assert_eq!(iter.key().deref(), format!("key{:03}", i).as_bytes());
            assert_eq!(iter.value().get_value(), format!("val{:03}", i).as_bytes());
            assert_eq!(iter.get_op(), 1);
            iter.next();
            i += 1;
        }
        assert_eq!(i, 100);
    }
}
