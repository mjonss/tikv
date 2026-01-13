// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, fmt};

use api_version::ApiV2;
use kvengine::{
    IdVer, LOCK_CF, ShardTag, SnapAccess, UserMeta, WRITE_CF, WriteBatch,
    table::memtable::WriteBatchEntry,
};
use kvproto::{cdcpb, raft_cmdpb::AdminRequest};
use log_wrappers::Value as LogValue;
use rfstore::store::ApplyObserver;
use tidb_query_datatype::codec::table::{INDEX_PREFIX_SEP, PREFIX_LEN, TABLE_PREFIX};
use tikv_util::{error, info, mpsc::Sender, trace};
use tokio::task::JoinHandle;
use txn_types::Lock;

use crate::CdcMsg;

pub struct CdcApplyObserver {
    store_id: u64,
    kv: kvengine::Engine,
    sender: Sender<CdcMsg>,
    runtime: tokio::runtime::Handle,
    join_handles: HashMap<u64 /* region_id */, Vec<JoinHandle<EventBuilder>>>,
}

#[derive(Default)]
pub struct RegionEvents {
    pub region_version: u64,
    pub events: Vec<cdcpb::Event>,
    pub tracked_locks: Vec<(Vec<u8>, u64)>,
}

impl fmt::Debug for RegionEvents {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegionEvents")
            .field("region_ver", &self.region_version)
            .field("events", &self.events.len())
            .field("tracked_locks", &self.tracked_locks.len())
            .finish()
    }
}

impl CdcApplyObserver {
    pub fn new(
        kv: kvengine::Engine,
        sender: Sender<CdcMsg>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let store_id = kv.get_engine_id();
        Self {
            store_id,
            kv,
            sender,
            runtime,
            join_handles: HashMap::default(),
        }
    }

    fn send_applied_msg(sender: &Sender<CdcMsg>, region_id: u64, region_events: RegionEvents) {
        let msg = CdcMsg::Applied {
            region_id,
            region_events,
        };
        if let Err(e) = sender.send(msg) {
            // Should happen only on stop.
            error!("send cdc event failed"; "region" => region_id, "error" => ?e);
        }
    }

    async fn flush_region_async(&mut self, region_id: u64) {
        if let Some(handles) = self.join_handles.remove(&region_id) {
            Self::flush_region_impl(region_id, handles, self.sender.clone()).await;
        };
    }

    async fn flush_region_impl(
        region_id: u64,
        handles: Vec<JoinHandle<EventBuilder>>,
        sender: Sender<CdcMsg>,
    ) {
        debug_assert!(!handles.is_empty());

        let mut events = RegionEvents::default();
        for (idx, handle) in handles.into_iter().enumerate() {
            // It's verbose to handle the task cancelled here.
            // Make sure the replication worker is stopped before tokio runtime shutdown.
            let mut event_builder = handle.await.expect("task panic/cancelled");

            if idx == 0 {
                events.region_version = event_builder.region_version();
            } else {
                debug_assert_eq!(
                    events.region_version,
                    event_builder.region_version(),
                    "region version changed, region_id: {}",
                    region_id,
                );
            }

            events.events.push(event_builder.build_event());
            events.tracked_locks.extend(event_builder.tracked_locks);
        }
        // Note: `sender` should be unbounded.
        Self::send_applied_msg(&sender, region_id, events);
    }

    async fn flush_all(&mut self) {
        let mut join_set = tokio::task::JoinSet::new();
        for (region_id, handles) in self.join_handles.drain() {
            let sender = self.sender.clone();
            join_set.spawn_on(
                async move {
                    Self::flush_region_impl(region_id, handles, sender).await;
                },
                &self.runtime,
            );
        }
        while let Some(res) = join_set.join_next().await {
            // It's verbose to handle the task cancelled here.
            // Make sure the replication worker is stopped before tokio runtime shutdown.
            res.expect("task panic/cancelled");
        }
    }
}

impl ApplyObserver for CdcApplyObserver {
    fn on_apply(&mut self, region_id: u64, region_version: u64, log_index: u64, wb: &WriteBatch) {
        let write_cf = wb.get_cf(WRITE_CF);
        let snap_access = self.kv.get_snap_access(region_id).unwrap();
        debug_assert_eq!(snap_access.get_version(), region_version);
        let mut event_builder = EventBuilder::new(snap_access, log_index);
        write_cf.iterate(|entry, buf| {
            event_builder.add_row(entry, buf);
        });
        let lock_cf = wb.get_cf(LOCK_CF);
        lock_cf.iterate(|entry, buf| {
            event_builder.add_lock(entry, buf);
        });
        let handle = self.runtime.spawn(async move {
            event_builder.fetch_old_values().await;
            event_builder
        });
        self.join_handles.entry(region_id).or_default().push(handle);
    }

    fn on_apply_admin(
        &mut self,
        region_id: u64,
        region_version: u64,
        log_index: u64,
        admin: &AdminRequest,
    ) {
        if admin.has_splits()
            || admin.has_prepare_merge()
            || admin.has_commit_merge()
            || admin.has_rollback_merge()
        {
            let tag = ShardTag::new(self.store_id, IdVer::new(region_id, region_version));
            self.flush_region(region_id);
            info!("{} on apply admin {:?}", tag, admin; "log_index" => log_index);
            let msg = CdcMsg::AppliedAdmin {
                region_id,
                region_version,
                admin: admin.clone(),
            };
            if let Err(e) = self.sender.send(msg) {
                error!("{} send cdc event failed", tag; "error" => ?e);
            }
        }
    }

    fn flush(&mut self) {
        self.runtime.clone().block_on(self.flush_all());
    }

    fn flush_region(&mut self, region_id: u64) {
        self.runtime
            .clone()
            .block_on(self.flush_region_async(region_id));
    }
}

struct EventBuilder {
    snap_access: SnapAccess,
    index: u64,
    rows: cdcpb::EventEntries,
    tracked_locks: Vec<(Vec<u8>, u64)>,
}

impl EventBuilder {
    fn new(snap_access: SnapAccess, index: u64) -> Self {
        Self {
            snap_access,
            index,
            rows: cdcpb::EventEntries::default(),
            tracked_locks: Vec::new(),
        }
    }

    fn region_version(&self) -> u64 {
        self.snap_access.get_version()
    }

    fn add_row(&mut self, entry: &WriteBatchEntry, buf: &[u8]) {
        let entry_key = entry.key(buf);
        if is_index_key(entry_key) {
            // Skip index keys as CDC does not need them.
            return;
        }
        let mut event_row = cdcpb::EventRow::default();
        event_row.set_key(entry_key.to_vec());
        event_row.set_value(entry.value(buf).to_vec());
        event_row.set_commit_ts(entry.version);
        let user_meta = UserMeta::from_slice(entry.user_meta(buf));
        event_row.set_start_ts(user_meta.start_ts);
        if event_row.get_value().is_empty() {
            event_row.set_op_type(cdcpb::EventRowOpType::Delete);
        } else {
            event_row.set_op_type(cdcpb::EventRowOpType::Put);
        }
        event_row.set_type(cdcpb::EventLogType::Committed);
        self.rows.mut_entries().push(event_row);

        trace!("{} add_row", self.snap_access.get_tag();
            "key" => LogValue::key(entry_key),
            "version" => entry.version,
            "start_ts" => user_meta.start_ts,
            "commit_ts" => user_meta.commit_ts,
            "log_index" => self.index,
        );
    }

    fn add_lock(&mut self, entry: &WriteBatchEntry, buf: &[u8]) {
        let entry_key = entry.key(buf);
        if is_index_key(entry_key) {
            // Skip index keys as CDC does not need them.
            return;
        }
        if entry.value(buf).is_empty() {
            self.tracked_locks.push((entry_key.to_vec(), 0));

            trace!("{} add_lock (untrack)", self.snap_access.get_tag();
                "key" => LogValue::key(entry_key),
                "version" => entry.version,
                "log_index" => self.index,
            );
            return;
        }
        let lock = Lock::parse(entry.value(buf)).unwrap();
        self.tracked_locks
            .push((entry.key(buf).to_vec(), lock.ts.into_inner()));
        trace!("{} add_lock", self.snap_access.get_tag();
            "key" => LogValue::key(entry_key),
            "version" => entry.version,
            "lock.ts" => lock.ts,
            "lock.ty" => ?lock.lock_type,
            "log_index" => self.index,
        );
    }

    async fn fetch_old_values(&mut self) {
        for row in self.rows.entries.iter_mut() {
            if row.get_type() != cdcpb::EventLogType::Committed {
                continue;
            }
            let mut keyspace_key =
                ApiV2::get_keyspace_prefix_by_id(self.snap_access.get_keyspace_id());
            keyspace_key.extend_from_slice(row.get_key());
            let old_item = self
                .snap_access
                .get_async(WRITE_CF, &keyspace_key, row.commit_ts - 1)
                .await;
            if !old_item.get_value().is_empty() {
                row.set_old_value(old_item.get_value().to_vec());
            }
        }
    }

    fn build_event(&mut self) -> cdcpb::Event {
        let mut event = cdcpb::Event::default();
        event.index = self.index;
        event.region_id = self.snap_access.get_id();
        event.mut_entries().set_entries(self.rows.take_entries());
        event
    }
}

pub(crate) fn is_index_key(key: &[u8]) -> bool {
    if key.len() < PREFIX_LEN {
        return false;
    }
    let trimmed_key = &key[..PREFIX_LEN];
    trimmed_key.starts_with(TABLE_PREFIX) && trimmed_key.ends_with(INDEX_PREFIX_SEP)
}
