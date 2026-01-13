// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, TimeStamp};

use crate::storage::{
    Snapshot, Statistics,
    mvcc::SnapshotReader,
    txn::{
        ProcessResult, Result,
        commands::{
            Command, CommandExt, ReadCommand, TypedCommand, find_mvcc_infos_by_key,
            find_mvcc_infos_by_key_async,
        },
    },
    types::MvccInfo,
};

command! {
    /// Retrieve MVCC information for the given key.
    MvccByKey:
        cmd_ty => MvccInfo,
        display => "kv::command::mvccbykey {:?} | {:?}", (key, ctx),
        content => {
            key: Key,
        }
}

impl CommandExt for MvccByKey {
    ctx!();
    tag!(key_mvcc);
    property!(readonly);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}

#[maybe_async::async_trait]
impl<S: Snapshot + 'static> ReadCommand<S> for MvccByKey {
    #[maybe_async]
    async fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let mut reader = SnapshotReader::new(TimeStamp::max(), snapshot, true);
        let result = find_mvcc_infos_by_key(&mut reader, &self.key, TimeStamp::max()).await;
        statistics.add(&reader.take_statistics());
        let (lock, writes, values) = result?;
        Ok(ProcessResult::MvccKey {
            mvcc: MvccInfo {
                lock,
                writes,
                values,
            },
        })
    }
}
