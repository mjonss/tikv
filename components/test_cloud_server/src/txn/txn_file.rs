// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use core::slice::SlicePattern;
use std::{error::Error, fmt, mem, sync::Arc};

use api_version::ApiV2;
use bytes::{BufMut, Bytes, BytesMut};
use cloud_worker::CreateTxnChunkResp;
use hyper::Method;
use kvengine::table::search;
use kvproto::kvrpcpb;
use log_wrappers::Value;
use security::{RestfulClient, SecurityManager};

use crate::util::{DEFAULT_INNER_KEY_OFFSET, Mutation};

const TXN_ENTRY_OVERHEAD: usize =
    mem::size_of::<u16>() + mem::size_of::<u8>() + mem::size_of::<u32>();
// key_len + op + value_len
const TXN_CHUNK_OVERHEAD: usize = mem::size_of::<u32>(); // checksum

pub type Result<T> = std::result::Result<T, Box<dyn Error + Sync + Send>>;

#[derive(Clone)]
pub struct TxnFileHelper {
    max_chunk_size: usize,
    cli: RestfulClient,
    runtime: tokio::runtime::Handle, // For hyper in cli.
}

impl TxnFileHelper {
    pub fn new(
        max_chunk_size: usize,
        tikv_worker_endpoints: Vec<String>,
        security_mgr: Arc<SecurityManager>,
        runtime: tokio::runtime::Handle,
    ) -> Result<Self> {
        Ok(TxnFileHelper {
            max_chunk_size,
            cli: RestfulClient::new("txn_file_helper", tikv_worker_endpoints, security_mgr)?,
            runtime,
        })
    }
}

impl TxnFileHelper {
    // `muts` should contain keyspace prefix.
    pub async fn build_txn_chunks(&self, muts: Vec<Mutation>) -> Result<Vec<TxnFileChunk>> {
        if muts.is_empty() {
            return Ok(vec![]);
        }

        let keyspace_id = ApiV2::get_u32_keyspace_id_by_key(&muts[0].key).unwrap();
        let total_size = muts
            .iter()
            .map(|m| m.key.len() - DEFAULT_INNER_KEY_OFFSET + m.value.len() + TXN_ENTRY_OVERHEAD)
            .sum::<usize>()
            + TXN_CHUNK_OVERHEAD;
        let mut buf = BytesMut::with_capacity(total_size.min(self.max_chunk_size));

        let mut chunks = vec![];
        let mut keys = vec![];
        let mut ops = vec![];

        for m in muts {
            let key = m.key.slice(DEFAULT_INNER_KEY_OFFSET..);
            if !buf.is_empty()
                && buf.len() + key.len() + m.value.len() + TXN_ENTRY_OVERHEAD + TXN_CHUNK_OVERHEAD
                    > self.max_chunk_size
            {
                let buf_to_flush =
                    mem::replace(&mut buf, BytesMut::with_capacity(self.max_chunk_size));
                let chunk_id = self.flush_to_tikv_worker(keyspace_id, buf_to_flush).await?;
                chunks.push(TxnFileChunk {
                    chunk_id,
                    keys: mem::take(&mut keys),
                    ops: mem::take(&mut ops),
                });
            }

            keys.push(m.key.clone());
            ops.push(m.op);

            buf.put_u16_le(key.len() as u16);
            buf.put(key);
            buf.put_u8(m.op as u8);
            buf.put_u32_le(m.value.len() as u32);
            buf.put(m.value);
        }

        if !buf.is_empty() {
            let chunk_id = self.flush_to_tikv_worker(keyspace_id, buf).await?;
            chunks.push(TxnFileChunk {
                chunk_id,
                keys,
                ops,
            });
        }

        Ok(chunks)
    }

    async fn flush_to_tikv_worker(
        &self,
        keyspace_id: u32,
        mut buf: BytesMut,
    ) -> Result<u64 /* chunk_id */> {
        let checksum = crc32fast::hash(buf.as_slice());
        buf.put_u32_le(checksum);

        let path = format!("txn_chunk?keyspace_id={keyspace_id}");
        let data = buf.freeze();
        let _enter = self.runtime.enter(); // For hyper.
        let resp = self.cli.request(path, Method::POST, Some(data)).await?;
        let resp: CreateTxnChunkResp = serde_json::from_slice(&resp)?;
        Ok(resp.chunk_id)
    }
}

#[derive(Clone)]
pub struct TxnFileChunk {
    pub chunk_id: u64,
    // `keys` of `ops` must be of the same length.
    pub keys: Vec<Bytes>,
    pub ops: Vec<kvrpcpb::Op>,
}

impl fmt::Debug for TxnFileChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxnFileChunk")
            .field("chunk_id", &self.chunk_id)
            .field("outer_smallest", &Value::key(self.outer_smallest()))
            .field("outer_biggest", &Value::key(self.outer_biggest()))
            .finish()
    }
}

impl TxnFileChunk {
    pub fn outer_smallest(&self) -> &Bytes {
        self.keys.first().unwrap()
    }

    pub fn outer_biggest(&self) -> &Bytes {
        self.keys.last().unwrap()
    }

    pub fn has_data_in_range(
        &self,
        primary: &Bytes,
        start: &[u8],
        end: &[u8],
    ) -> (Option<Bytes> /* first data key */, bool) {
        let is_in_range = |pos| self.keys.get(pos).is_some_and(|k: &Bytes| k.as_ref() < end);
        let is_primary = |pos| pos == 0 && self.keys[pos] == primary;
        let is_op_for_write = |op| {
            op != kvrpcpb::Op::CheckNotExists
                && op != kvrpcpb::Op::Lock
                && op != kvrpcpb::Op::PessimisticLock
        };

        let mut pos = search(self.keys.len(), |i| self.keys[i].as_ref() >= start);
        if is_in_range(pos) {
            let mut first_data_key = None;
            loop {
                // Always return primary as first data key if it's in range.
                // The `op` of primary would not be for write.
                if is_primary(pos) || is_op_for_write(self.ops[pos]) {
                    first_data_key = Some(self.keys[pos].clone());
                    break;
                }
                pos += 1;
                if !is_in_range(pos) {
                    break;
                }
            }
            (first_data_key, true)
        } else {
            (None, false)
        }
    }
}
