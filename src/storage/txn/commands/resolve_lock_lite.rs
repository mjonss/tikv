// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, ReqType, TimeStamp};

use crate::storage::{
    ProcessResult, Snapshot,
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, SnapshotReader},
    txn::{
        Result, cleanup, cleanup_async,
        commands::{
            Command, CommandExt, ReaderWithStats, ReleasedLocks, ResponsePolicy, TypedCommand,
            WriteCommand, WriteContext, WriteResult,
        },
        commit, commit_async,
    },
};

command! {
    /// Resolve locks on `resolve_keys` according to `start_ts` and `commit_ts`.
    ResolveLockLite:
        cmd_ty => (),
        display => "kv::resolve_lock_lite resolve_keys({:?}) {} {} | {:?}", (resolve_keys, start_ts, commit_ts, ctx),
        content => {
            /// The transaction timestamp.
            start_ts: TimeStamp,
            /// The transaction commit timestamp.
            commit_ts: TimeStamp,
            /// The keys to resolve.
            resolve_keys: Vec<Key>,
            /// Used by file based transaction.
            is_txn_file: bool,
        }
}

impl CommandExt for ResolveLockLite {
    ctx!();
    tag!(resolve_lock_lite);
    request_type!(KvResolveLock);
    ts!(start_ts);
    property!(is_sys_cmd);
    write_bytes!(resolve_keys: multiple);
    gen_lock!(resolve_keys: multiple);
    can_build_txn_file!();
}

#[maybe_async::async_trait]
impl<S: Snapshot + 'static, L: LockManager> WriteCommand<S, L> for ResolveLockLite {
    #[maybe_async]
    async fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(self.start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.start_ts, snapshot, &self.ctx),
            context.statistics,
        );

        let rows = self.resolve_keys.len();
        // ti-client guarantees the size of resolve_keys will not too large, so no
        // necessary to control the write_size as ResolveLock.
        let mut released_locks = ReleasedLocks::new();
        for key in self.resolve_keys {
            released_locks.push(if !self.commit_ts.is_zero() {
                commit(&mut txn, &mut reader, key, self.commit_ts).await?
            } else {
                cleanup(&mut txn, &mut reader, key, TimeStamp::zero(), false).await?
            });
        }

        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.set_req_type(ReqType::ResolveLock);
        write_data.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr: ProcessResult::Res,
            lock_info: vec![],
            released_locks,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
