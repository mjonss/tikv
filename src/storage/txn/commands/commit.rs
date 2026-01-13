// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, ReqType, TxnExtra};

use crate::storage::{
    ProcessResult, Snapshot, TxnStatus,
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, SnapshotReader},
    txn::{
        Error, ErrorInner, Result,
        commands::{
            Command, CommandExt, ReaderWithStats, ReleasedLocks, ResponsePolicy, TypedCommand,
            WriteCommand, WriteContext, WriteResult,
        },
        commit, commit_async,
    },
};

command! {
    /// Commit the transaction that started at `lock_ts`.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite).
    Commit:
        cmd_ty => TxnStatus,
        display => "kv::command::commit {:?} {} -> {} | {:?}", (keys, lock_ts, commit_ts, ctx),
        content => {
            /// The keys affected.
            keys: Vec<Key>,
            /// The lock timestamp.
            lock_ts: txn_types::TimeStamp,
            /// The commit timestamp.
            commit_ts: txn_types::TimeStamp,
            /// The commit is using async commit.
            use_async_commit: bool,
            /// Used in file based transaction.
            is_txn_file: bool,
        }
}

impl CommandExt for Commit {
    ctx!();
    tag!(commit);
    request_type!(KvCommit);
    ts!(commit_ts);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
    can_build_txn_file!();
}

#[maybe_async::async_trait]
impl<S: Snapshot + 'static, L: LockManager> WriteCommand<S, L> for Commit {
    #[maybe_async]
    async fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        if self.commit_ts <= self.lock_ts {
            return Err(Error::from(ErrorInner::InvalidTxnTso {
                start_ts: self.lock_ts,
                commit_ts: self.commit_ts,
            }));
        }
        let mut txn = MvccTxn::new_with_backup_ts(
            self.lock_ts,
            context.concurrency_manager,
            self.use_async_commit,
        );
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.lock_ts, snapshot, &self.ctx),
            context.statistics,
        );

        let rows = self.keys.len();
        // Pessimistic txn needs key_hashes to wake up waiters
        let mut released_locks = ReleasedLocks::new();
        for k in self.keys {
            released_locks.push(commit(&mut txn, &mut reader, k, self.commit_ts).await?);
        }

        let pr = ProcessResult::TxnStatus {
            txn_status: TxnStatus::committed(self.commit_ts),
        };
        let backup_ts_checked = txn.take_checked_backup_ts();
        let mut write_data =
            WriteData::new(txn.into_modifies(), TxnExtra::default(), backup_ts_checked);
        write_data.set_req_type(ReqType::Commit);
        write_data.set_allowed_on_disk_almost_full();

        fail::fail_point!("txn::commit_process_write_finish");

        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr,
            lock_info: vec![],
            released_locks,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
