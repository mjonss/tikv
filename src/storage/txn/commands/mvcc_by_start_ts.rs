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
    /// Retrieve MVCC info for the first committed key which `start_ts == ts`.
    MvccByStartTs:
        cmd_ty => Option<(Key, MvccInfo)>,
        display => "kv::command::mvccbystartts {:?} | {:?}", (start_ts, ctx),
        content => {
            start_ts: TimeStamp,
        }
}

impl CommandExt for MvccByStartTs {
    ctx!();
    tag!(start_ts_mvcc);
    ts!(start_ts);
    property!(readonly);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}

#[maybe_async::async_trait]
impl<S: Snapshot + 'static> ReadCommand<S> for MvccByStartTs {
    #[maybe_async]
    async fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let mut reader = SnapshotReader::new(TimeStamp::max(), snapshot, true);
        match reader.seek_ts(self.start_ts)? {
            Some(key) => {
                let result = find_mvcc_infos_by_key(&mut reader, &key, TimeStamp::max()).await;
                statistics.add(&reader.take_statistics());
                let (lock, writes, values) = result?;
                Ok(ProcessResult::MvccStartTs {
                    mvcc: Some((
                        key,
                        MvccInfo {
                            lock,
                            writes,
                            values,
                        },
                    )),
                })
            }
            None => Ok(ProcessResult::MvccStartTs { mvcc: None }),
        }
    }
}
