// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

// Require "debug_assertions" to ensure that the tracer codes are not in
// release build.
#![cfg(all(debug_assertions, feature = "debug-trace-txn-tasks"))]

use std::{collections::HashMap, fmt, io::Write as _};

use crossbeam::queue::ArrayQueue;
use error_code::{ErrorCode, ErrorCodeExt};

use crate::storage::{Error as StorageError, txn::Command};

const TXN_TASKS_QUEUE_CAP: usize = 65536;

struct TxnTask {
    cid: u64,
    ts: u64,
    cmd: String,
    err: Option<ErrorCode>,
}

impl fmt::Debug for TxnTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let err_str = if let Some(err_code) = self.err {
            format!(" (err:{})", err_code)
        } else {
            "".to_owned()
        };
        write!(f, "{}:{}: {}{}", self.cid, self.ts, self.cmd, err_str)
    }
}

lazy_static::lazy_static! {
    static ref TXN_TASKS: ArrayQueue<TxnTask> = ArrayQueue::new(TXN_TASKS_QUEUE_CAP);
    static ref TXN_TASKS_ERROR: ArrayQueue<(u64 /* cid */, ErrorCode)> = ArrayQueue::new(TXN_TASKS_QUEUE_CAP);
}

pub fn trace_txn_task(cid: u64, cmd: &Command) {
    let task = TxnTask {
        cid,
        ts: cmd.ts().into_inner(),
        cmd: format!("{}", cmd),
        err: None,
    };
    TXN_TASKS.force_push(task);
}

pub fn trace_txn_task_error(cid: u64, err: &StorageError) {
    TXN_TASKS_ERROR.force_push((cid, err.error_code()));
}

pub fn dump_txn_tasks() {
    let mut task_errors = HashMap::with_capacity(TXN_TASKS_ERROR.len());
    while let Some((cid, code)) = TXN_TASKS_ERROR.pop() {
        task_errors.insert(cid, code);
    }

    let stderr = std::io::stderr();
    let _ = writeln!(stderr.lock(), "dump txn tasks, count: {}", TXN_TASKS.len());
    while let Some(mut task) = TXN_TASKS.pop() {
        let cid = task.cid;
        task.err = task_errors.remove(&cid);
        let _ = writeln!(stderr.lock(), "txn task: {:?}", task);
    }
    let _ = stderr.lock().flush();
}
