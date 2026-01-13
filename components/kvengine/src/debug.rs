// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

// Require "debug_assertions" to ensure that the tracer codes are not in
// release build.
#![cfg(all(debug_assertions, feature = "debug-trace-mem-table"))]

use std::{fmt, io::Write, sync::Arc};

use crossbeam::queue::ArrayQueue;
use dashmap::DashMap;
use log_wrappers::hex_encode;

use crate::{Shard, table::memtable};

const RAFT_LOG_QUEUE_CAP: usize = 1024;
const MEM_TABLE_ACTION_QUEUE_CAP_PER_REGION: usize = 512;

enum MemTableActionType {
    Write,
    Switch,
}

impl fmt::Debug for MemTableActionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemTableActionType::Write => write!(f, "write"),
            MemTableActionType::Switch => write!(f, "switch"),
        }
    }
}

struct MemTableProp {
    skl_size: u64,
    txn_files_size: u64,
    prefixed_size: u64,
    unpersisted_props_size: usize,
}

impl fmt::Debug for MemTableProp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "size:{}+{},{},props:{}",
            self.skl_size, self.txn_files_size, self.prefixed_size, self.unpersisted_props_size,
        )
    }
}

pub struct MemTableAction {
    store_id: u64,
    log_idx: u64,
    prop: MemTableProp,
    ty: MemTableActionType,
}

impl fmt::Debug for MemTableAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{} {:?} {:?}",
            self.store_id, self.log_idx, self.ty, self.prop
        )
    }
}

type MemTableActionsMap = DashMap<u64 /* region_id */, Arc<ArrayQueue<MemTableAction>>>;

struct RaftLog {
    region_id: u64,
    store_id: u64,
    log_idx: u64,
    data: Vec<u8>,
}

impl fmt::Debug for RaftLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{} {}",
            self.store_id,
            self.region_id,
            self.log_idx,
            hex_encode(&self.data)
        )
    }
}

pub struct Tracer {
    mem_table_actions: MemTableActionsMap,
    raft_logs: ArrayQueue<RaftLog>,
}

impl Default for Tracer {
    fn default() -> Self {
        Self {
            mem_table_actions: Default::default(),
            raft_logs: ArrayQueue::new(RAFT_LOG_QUEUE_CAP),
        }
    }
}

lazy_static::lazy_static! {
    pub static ref TRACER: Tracer = Tracer::default();
}

impl Tracer {
    fn trace_mem_table(
        &self,
        shard: &Shard,
        mem_tbl: &memtable::CfTable,
        prefixed_mem_tbl_size: u64,
        log_idx: u64,
        ty: MemTableActionType,
        raft_log: &[u8],
    ) {
        let store_id = shard.engine_id;
        let region_id = shard.id;
        let (skl_size, txn_files_size) = mem_tbl.skl_and_txn_files_size();
        let unpersisted_props_size = mem_tbl.unpersisted_props_size();
        let mem_tbl_action = MemTableAction {
            store_id,
            log_idx,
            prop: MemTableProp {
                skl_size,
                txn_files_size,
                prefixed_size: prefixed_mem_tbl_size,
                unpersisted_props_size,
            },
            ty,
        };
        let mem_tbl_queue = self
            .mem_table_actions
            .entry(region_id)
            .or_insert_with(|| Arc::new(ArrayQueue::new(MEM_TABLE_ACTION_QUEUE_CAP_PER_REGION)))
            .value()
            .clone();
        mem_tbl_queue.force_push(mem_tbl_action);

        self.raft_logs.force_push(RaftLog {
            region_id,
            store_id,
            log_idx,
            data: raft_log.to_vec(),
        });
    }

    pub fn drain_mem_table_actions(&self, region_id: u64) -> Vec<MemTableAction> {
        let Some(queue) = self
            .mem_table_actions
            .get(&region_id)
            .map(|x| x.value().clone())
        else {
            return vec![];
        };
        let mut actions = Vec::with_capacity(queue.len());
        while let Some(action) = queue.pop() {
            actions.push(action);
        }
        actions
    }

    fn drain_raft_logs(&self, region_id: u64, log_idx: Option<u64>) -> Vec<RaftLog> {
        let mut res = Vec::with_capacity(self.raft_logs.len());
        while let Some(raft_log) = self.raft_logs.pop() {
            if raft_log.region_id == region_id
                && log_idx.map_or(true, |log_idx| raft_log.log_idx == log_idx)
            {
                res.push(raft_log);
            }
        }
        res
    }
}

pub(crate) fn trace_write_mem_table(
    shard: &Shard,
    mem_tbl: &memtable::CfTable,
    prefixed_mem_tbl_size: u64, // May not be equal to skl_size + txn_files_size.
    log_idx: u64,
    raft_log: &[u8],
) {
    TRACER.trace_mem_table(
        shard,
        mem_tbl,
        prefixed_mem_tbl_size,
        log_idx,
        MemTableActionType::Write,
        raft_log,
    );
}

pub(crate) fn trace_switch_mem_table(shard: &Shard, mem_tbl: &memtable::CfTable, log_idx: u64) {
    TRACER.trace_mem_table(shard, mem_tbl, 0, log_idx, MemTableActionType::Switch, &[]);
}

pub(crate) fn dump_mem_table_actions(region_id: u64) {
    let actions = TRACER.drain_mem_table_actions(region_id);

    let stderr = std::io::stderr();
    let _ = writeln!(
        stderr.lock(),
        "mem table actions: region {}: {:?}",
        region_id,
        actions
    );
    let _ = stderr.lock().flush();
}

pub(crate) fn dump_raft_logs(region_id: u64) {
    let raft_logs = TRACER.drain_raft_logs(region_id, None);

    let stderr = std::io::stderr();
    let _ = writeln!(stderr.lock(), "raft logs: {}: {:?}", region_id, raft_logs);
    let _ = stderr.lock().flush();
}
