# Rfstore Maintainer Guide
This document is for maintainers of the `rfstore` component in this repo.
It focuses on design intent, invariants, and failure behavior.
It intentionally skips routine API details that are easy to read in code.
When you need exact struct fields, read the source under `components/rfstore/src`.

## Quick index
- Scope and mental model
- Project map and reading order
- Invariants and guardrails
- High level architecture
- Threading model and workers
- Batching, fairness, and backpressure
- Message channels and queues
- Store level FSM and metadata
- Peer lifecycle and state
- Internal data structures and caches
- Raft ready pipeline
- Persistence pipeline (rfengine and IO worker)
- Apply pipeline (applier and apply workers)
- Preprocess pipeline (metadata and ordering)
- Kvengine change set pipeline
- Integration points with kvengine and rfengine
- Custom raft log format
- Read path and read index logic
- Write path and proposal lifecycle
- Split, merge, and conf change
- Snapshot creation and restore
- Dependencies and raft log truncation
- Maintenance and background tasks
- Security and encryption
- Recovery, blacklist, and restore flows
- Schema and storage class integration
- GC and file lifecycle
- PD interaction and statistics
- Errors, drops, and backpressure
- Stale peer handling and membership safety
- Configuration knobs with rationale
- Metrics and slow log signals
- Detailed event sequences
- Debugging playbook
- Tests and references

## Scope and mental model
rfstore is the Raft layer for the cloud engine stack.
It owns Raft groups, proposes commands, and applies committed logs.
It uses `rfengine` for raft log and state persistence.
It uses `kvengine` for user data and shard metadata.
Each rfstore region maps to exactly one kvengine shard.
Each region is replicated by a Raft group of peers.
A `Peer` is the local replica of a region in this store.
A `PeerFsm` wraps a `Peer` plus ticker and apply worker selection.
The store has one `RaftBatchSystem` which wires workers and routers.
All messages flow through typed channels (`StoreMsg`, `PeerMsg`, `ApplyMsg`).
The core execution pipeline is propose -> replicate -> persist -> apply -> post apply.

Key terms used in this guide.
- Region: Raft group metadata, key range, and epoch.
- Shard: kvengine storage unit for a region key range.
- Peer: local replica of a region.
- Peer storage: `PeerStorage` implements `raft::Storage` for a peer.
- Raft log: entries stored in rfengine and replicated by raft-rs.
- Apply: applying committed entries to kvengine state machine.
- Preprocess: rfstore only step that updates metadata before apply.
- Change set: kvengine meta change (flush, compaction, ingest, etc).
- Snapshot: a `kvenginepb::ChangeSet` embedded in raft snapshot data.
- Dependents: parent and child region relationships used to gate truncation.
- Lease: leader lease used for local reads.
- Read index: raft safe read mechanism used for linearizable reads.

Design principles that show up repeatedly in code.
- Keep Raft state persistence on the raft thread, but IO in a separate worker.
- Separate preprocessing from apply to keep metadata consistent under lag.
- Avoid blocking the raft loop with expensive tasks (use background workers).
- Make state transitions explicit and monotonic (apply index, preprocessed index).
- Prefer eventual cleanup over synchronous teardown (dependents and delayed destroy).

## Project map and reading order
Start here if you are new to the code.
- `components/rfstore/src/store/store_fsm.rs` for system wiring and store level FSM.
- `components/rfstore/src/store/peer_worker.rs` for raft worker loops and batching.
- `components/rfstore/src/store/peer_fsm.rs` for peer message handling and proposals.
- `components/rfstore/src/store/peer.rs` for core peer logic and raft ready handling.
- `components/rfstore/src/store/peer_storage.rs` for raft storage and snapshot handling.
- `components/rfstore/src/store/apply.rs` for apply worker logic and apply state machine.
- `components/rfstore/src/store/read.rs` for local read path and lease checks.
- `components/rfstore/src/store/read_queue.rs` for read index queueing and retries.
- `components/rfstore/src/store/msg.rs` for message types and callbacks.
- `components/rfstore/src/store/rlog/mod.rs` for custom raft log encoding.
- `components/rfstore/src/store/util.rs` for shared helpers and epoch checks.
- `components/rfstore/src/store/worker/pd.rs` for PD worker and heartbeats.
- `components/rfstore/src/store/worker/gc.rs` for local file GC.
- `components/rfstore/src/store/worker/schema.rs` for schema and storage class tasks.
- `components/rfstore/src/store/bootstrap.rs` for bootstrap and initial change set.
- `components/rfstore/src/store/recover.rs` for offline recovery flows and blacklist.
- `components/rfstore/src/store/config.rs` for configuration and defaults.
- `components/rfstore/src/router.rs` and `components/rfstore/src/store/transport.rs` for routing.

Related docs that provide feature level context.
- `doc/features/rfstore.md` for a concise runtime flow overview.
- `doc/features/txn_file.md` for transaction file handling details.
- `components/rfengine/README.md` for raft log persistence details.
- `components/kvengine/README.md` for shard behavior and change sets.

## Invariants and guardrails
These invariants are enforced across rfstore and should not be violated.
They explain many design choices in the code.

Raft state invariants.
- `applied_index` and `applied_index_term` are monotonic per peer.
- `RaftState::last_preprocessed_index` is monotonic per peer.
- `RaftState::last_index` is monotonic unless a snapshot is restored.
- `RaftTruncatedState` only moves forward unless in restore flows.

Metadata invariants.
- `RegionEpoch` must never move backward.
- `preprocessed_region` is always at least as new as `region`.
- `ShardMeta::seq` strictly increases with change sets.
- Change sets for a shard must be applied in sequence order.

Safety invariants.
- A snapshot cannot be applied while dependents exist.
- A peer cannot be destroyed while dependents exist.
- A split or merge proposal is rejected if metadata is known to be stale.
- Local reads require the leader lease to be valid.

Progress invariants.
- Persisted ready numbers must be applied before apply resumes.
- Read index responses are ordered by proposal time in the read queue.
- Pending apply results must be handled in order.

Guardrails in code.
- `check_region_epoch` and `check_peer_id` guard request validity.
- `cmd_epoch_checker` rejects proposals when metadata is stale.
- `paused_apply_queue` delays apply if a change set is pending.
- `dependents_is_empty_or_only_self` gates snapshot apply and destroy.

## High level architecture
rfstore sits between the network layer and kvengine.
It runs several worker threads and communicates through internal channels.
It also integrates with PD for heartbeats, splits, and stats.

High level flow.
Client request -> Router -> Raft worker -> Raft log persistence -> Apply worker -> kvengine

Key components in one diagram.
```ascii
+-----------------------+         +--------------------+
|   network / service   |         |        PD          |
|   (cloud_server etc)  |         | heartbeats, splits |
+-----------+-----------+         +---------+----------+
            |                               ^
            v                               |
     +------+-------------------------------+------+
     |                rfstore                     |
     | +-----------+   +-----------+   +--------+ |
     | | RaftWorker|   | ApplyWork |   | IoWork | |
     | | (peer FSM)|   |  (apply)  |   | persist| |
     | +-----------+   +-----------+   +--------+ |
     |    ^  ^  ^           ^                |    |
     |    |  |  |           |                v    |
     | StoreMsg PeerMsg   ApplyMsg        rfengine |
     |    |  |  |                                |
     +----+--+--+--------------------------------+
              |
              v
           kvengine
```

## Threading model and workers
rfstore is intentionally multi threaded to keep Raft loops responsive.
Each worker is single purpose and communicates through channels.

Raft worker threads.
- One main Raft worker (`RaftWorker`) runs the main loop.
- Optional auxiliary Raft workers (`RaftAuxWorker`) offload inbox processing.
- One idle Raft worker (`RaftIdleWorker`) handles idle peers with slow ticks.
- Aux workers are enabled dynamically based on `main_worker_max_util` and `aux_worker_max_util`.
- Aux workers only take work when the main worker is saturated and `raft_wb` is empty.
- The main worker syncs with aux workers each loop to avoid races.
- Raft workers handle `PeerMsg` and `StoreMsg`, then call `Peer::handle_raft_ready`.
- Raft workers own the `RaftContext` and `StoreContext` lifetimes.

Apply worker threads.
- `ApplyWorker` threads run the state machine apply path.
- Apply workers are split into leader and follower pools.
- The apply pool sizes are configured by `Config::apply_pool_size` and `apply_follower_pool_size`.
- Each peer chooses an apply worker index and may switch on role changes.

IO worker thread.
- `IoWorker` persists raft write batches to rfengine.
- It also sends raft messages after persistence and notifies `PeerMsg::Persisted`.
- IO writes are batched to reduce write amplification.

PD worker thread.
- `PdRunner` handles store and region heartbeats, split scheduling, and stats.
- It is wired by `RaftBatchSystem::spawn` and receives `PdTask` from store and peers.

GC worker thread.
- `GcRunner` periodically removes unused local files.
- It also runs IA GC when IA is enabled in kvengine.

Schema worker thread.
- `SchemaRunner` checks schema files and storage class metadata.
- It can request region splits or storage class updates.

Rationale for this layout.
- Raft loops must stay fast and are isolated from IO and heavy background work.
- Apply is CPU heavy and is parallelized across multiple threads.
- IO batching improves throughput and reduces sync overhead.
- Idle peers should not wake busy threads for heartbeats.

Idle peer offloading criteria.
- A peer is considered idle after `peer_idle_duration` with no activity.
- It must not be pending removal and must be initialized.
- It must not be applying a snapshot (`SnapState::Relax` only).
- It must have applied to `last_index` and have no pending persist ready.
- Idle peers are moved to the idle worker and woken only by non heartbeat messages.

## Batching, fairness, and backpressure
rfstore batches work to balance throughput and latency.

Batching in the raft worker.
- `RaftWorker::receive_msgs` coalesces peer messages into batches.
- `raft_worker_max_batch_size` caps batch size by estimated message bytes.
- `MAX_BATCH_COUNT` caps batch size by message count.
- Inbox processing is per peer to preserve ordering.

Tick segmentation.
- Peer ticks are segmented by region id hash.
- This spreads tick work over multiple loops.
- Idle peers use a slower tick interval to reduce overhead.

Apply batching.
- `ApplyBatch` groups `ApplyMsg` for a single peer.
- This reduces lock contention on the applier and peer FSM.

IO batching.
- `IoWorker` merges multiple write batches into one persist call.
- This reduces fsync frequency and write amplification.

Backpressure points.
- Peer and store channels are bounded by `channel_capacity` or worker specific limits.
- `DiscardReason::Full` is returned when channels are saturated.
- Raft worker logs slow loops to surface persistent overload.

Fairness tradeoffs.
- Larger batches increase throughput but can increase tail latency.
- Idle peers are moved to the idle worker to avoid starving active peers.

## Message channels and queues
rfstore uses typed channels to keep responsibilities clear.

`RaftRouter` contains two senders.
- A store sender for `StoreMsg`.
- A peer sender for `(region_id, PeerMsg)`.

Store messages (`StoreMsg`).
- Tick scheduling for store level periodic tasks.
- Store start and shutdown coordination.
- Incoming raft messages that need peer creation or validation.
- Engine change set notifications from kvengine.
- Region queries for PD or higher layers.
- Apply result forwarding to store FSM.

Peer messages (`PeerMsg`).
- Raft messages from transport (`RaftMessage`).
- Proposals from clients (`RaftCommand`).
- Peer ticks and lifecycle events.
- Apply results and post apply signals.
- Change set preparation results and snapshot events.
- Idle and wake up control for the idle worker.

Apply messages (`ApplyMsg`).
- Apply committed raft entries (`Apply`).
- Registration when a peer is created or recreated.
- Change set prepare and apply tasks.
- Merge preparation and commit resume tasks.
- Transaction file preparation and resume tasks.
- Maintenance and unsafe destroy requests.

Why three tiers of messages.
- Store level messages gate peer creation and global tasks.
- Peer level messages preserve per region ordering and isolation.
- Apply level messages keep heavy apply work off the raft loop.

## Store level FSM and metadata
The store level FSM (`StoreFsm`) coordinates peers and store tasks.
It is created by `RaftBatchSystem::new` and driven by the main Raft worker.

Store initialization.
- `RaftBatchSystem::spawn` wires workers, PD scheduler, and transports.
- It loads existing peers by scanning rfengine region states.
- Tombstone peers are cleared on restart to avoid resurrecting stale peers.

`StoreMeta` is the shared metadata cache.
- `RegionMap` indexes regions by id and by end key.
- `readers` is a concurrent map of `ReadDelegate` for local reads.
- `pending_msgs` keeps early vote or append messages for newly split regions.
- `black_list` optionally filters regions, keyspaces, or tables.

`RegionMap` tradeoffs.
- End key BTree allows fast range scans for sync region queries.
- Overlap checks remove older entries when a new region arrives.
- Epoch comparison prevents overwriting newer regions with stale metadata.

Store level ticks.
- `STORE_TICK_PD_HEARTBEAT` drives store stats to PD.
- `STORE_TICK_UPDATE_SAFE_TS` updates safe ts via PD.
- `STORE_TICK_LOCAL_FILE_GC` triggers local file GC.

Store message handling highlights.
- `on_raft_message` validates store id and epoch, and respects blacklist.
- `on_apply_result` drains pending apply results and updates store metadata.
- `on_prepare_merge_request` re proposes merge commands after apply.
- `on_destroy_peer` enforces dependent checks before destruction.

Design rationale.
- Store FSM owns region maps to avoid per peer scans.
- Apply results are handled at store scope because they update global maps.
- Blacklist is enforced early to avoid creating peers that should stay isolated.

## Peer lifecycle and state
Peers can be created locally or by incoming raft messages.

Creation paths.
- `PeerFsm::create` is used for bootstrap, split, and merge creation.
- `PeerFsm::replicate` is used for peers created by raft messages.
- `RaftBatchSystem::load_peers` restores peers from rfengine state.

Key state objects inside a peer.
- `PeerStorage` implements `raft::Storage` and owns raft state.
- `RaftState` stores term, vote, commit, last index, and last preprocessed index.
- `RaftApplyState` stores applied index and applied index term.
- `SnapState` tracks snapshot application state (`Relax`, `Applying`, `ApplyAborted`).
- `pending_merge_state` and `want_rollback_merge_peers` track merge progress.
- `preprocessed_region` tracks metadata that has been preprocessed but not yet applied.
- `apply_worker_idx` selects the apply worker for this peer.
- `leader_apply_worker_idx` and `follower_apply_worker_idx` keep leader and follower pools separate.
- `applying_cnt` prevents switching apply workers while apply is in flight.

Why `preprocessed_region` exists.
- Apply is async and can lag behind the raft loop.
- Preprocess must update metadata for split and merge validation immediately.
- Using `preprocessed_region` keeps epoch decisions consistent before apply catches up.

Peer destruction path.
- Peers can be destroyed by apply results or stale messages.
- Destruction is delayed if dependents exist or snapshot is applying.
- The store FSM enforces these gates to avoid unsafe log truncation.

## Internal data structures and caches
rfstore maintains several per peer caches for performance.
These caches also affect correctness in subtle ways.

Proposal queues.
- `ProposalQueue` tracks pending proposals with term and index ordering.
- It is used to match committed entries back to callbacks.
- Stale proposals are rejected when terms move forward.

Pending command queue in apply.
- `PendingCmdQueue` mirrors proposals on the apply side.
- Conf change callbacks are tracked separately from normal ones.
- This separation prevents losing a conf change when leadership flips.

Read index queue.
- `ReadIndexQueue` stores `ReadIndexRequest` objects.
- Each request has a UUID context and a propose timestamp.
- For followers, a map tracks UUID to queue offsets.

Lock cache.
- The applier keeps a lock cache for MVCC lock records.
- It avoids repeated reads from kvengine when applying commits.
- The cache is shrunk on maintenance ticks to control memory usage.

Memtable state.
- `MemTableState` tracks writable memtable size and flush thresholds.
- The applier updates it after each write batch.
- Leaders propose a `SwitchMemTable` custom log when needed.

Bucket statistics.
- Bucket metadata and stats are tracked per peer.
- They are updated on apply and reported to PD by the leader.

## Raft ready pipeline
`Peer::handle_raft_ready` is the core raft loop entry point.
It is called from the raft worker after processing peer inbox messages.

Ready handling flow.
- Skip if the peer is pending removal or applying snapshot.
- Skip if no ready is available.
- If a snapshot is pending and dependents exist, defer handling.
- Update reported leader id and notify coprocessor host if changed.
- Get `Ready` from raft-rs and collect `ReadyStats` for logging.
- On leader, build and send raft messages for `ready.messages`.
- Apply read states from `ready.read_states` to serve pending reads.
- Preprocess committed entries and build apply messages.
- Hand off raft entries and snapshot to `PeerStorage::handle_raft_ready`.
- Collect persist messages and build `PersistReady` for IO worker.
- Advance raft append asynchronously and update per ready bookkeeping.

Why snapshots are gated.
- Applying a snapshot clears metadata and truncates logs.
- If dependents exist, they might still need the parent log for recovery.
- The guard avoids destroying data needed by child peers.

Idle worker behavior.
- Idle peers bypass persist ready batching to avoid wakeups.
- In idle mode, persist messages are sent directly by the raft worker.
- This keeps idle peers idle while still making progress on heartbeats.

## Persistence pipeline (rfengine and IO worker)
Persistence is split between the raft loop and the IO worker.

In raft thread persistence.
- `RaftContext` collects `raft_wb` and `persist_readies` per batch.
- `persist_states` applies the write batch to rfengine and sends an `IoTask`.
- `IoTask` contains persisted readies and merged write batches.

IO worker flow.
- `IoWorker` receives `IoTask` and merges write batches.
- It persists a combined batch via `rfengine::RfEngine::persist`.
- It sends raft messages after persistence to avoid sending unpersisted logs.
- It returns `PeerMsg::Persisted` to the raft worker for each ready.
- It sleeps to enforce `io_worker_min_write_duration` to reduce write amplification.

Why persistence is split.
- Raft loop must not block on IO to maintain responsiveness.
- IO batching improves throughput and reduces sync overhead.
- The `Persisted` callback synchronizes raft state transitions.

## Apply pipeline (applier and apply workers)
Apply is the state machine execution path for committed entries.
It runs in apply workers to avoid blocking the raft loop.

Apply worker loop.
- Each `ApplyWorker` receives an `ApplyBatch`.
- The batch contains multiple `ApplyMsg` for a single peer.
- The worker calls `Applier::handle_msg` for each message.
- Apply latencies are recorded via local histograms.

Apply message types (non exhaustive).
- `Apply` holds committed raft entries and proposals.
- `Registration` reinitializes an applier for a peer.
- `PrepareChangeSet` and `ApplyChangeSet` handle kvengine change sets.
- `PrepareMerge` and `PrepareCommitMerge` handle merge sequencing.
- `ResumeCommitMerge` resumes apply after background preparation completes.
- `PrepareTxnFile` and `ResumeTxnFile` handle transaction file flows.
- `Maintenance` triggers lock cache shrink and memtable switching logic.
- `UnsafeDestroy` initiates peer destruction from apply side.

Apply result propagation.
- `Applier::finish_for` builds `MsgApplyResult` with exec results.
- Exec results include split, merge, conf change, and destroy operations.
- Results are sent back to the raft worker via `PeerMsg::ApplyResult`.
- The store FSM consumes results to update global region metadata.

Callback management.
- `PendingCmdQueue` keeps callbacks ordered by index and term.
- Conf change callbacks are separated from normal callbacks.
- Stale callbacks are notified when terms advance or roles change.

## Preprocess pipeline (metadata and ordering)
Preprocess is a rfstore specific step performed on the raft thread.
It exists to update metadata and schedule apply tasks before apply executes.

When preprocess is triggered.
- Entries with `ProposalContext::PRE_PROCESS` are preprocessed.
- The flag is set for split, merge, and kvengine change set proposals.
- Custom logs for engine meta and txn file refs also use this path.

Preprocess data structures.
- `PreprocessRef` borrows a peer but only exposes metadata and raft state.
- `PreprocessContext` carries rfengine write batch and router hooks.
- `preprocessed_region` holds metadata after preprocess but before apply.
- `preprocessed_index` prevents reprocessing already preprocessed entries.

Preprocess responsibilities.
- Update `ShardMeta` with change sets before they are applied.
- Persist updated raft state and engine meta into rfengine write batch.
- Schedule apply messages (`ApplyMsg`) for async apply threads.
- Validate split and merge conditions that depend on up to date metadata.

Why preprocess is separate from apply.
- Apply is async and can lag behind the raft loop.
- Merge and split checks need up to date metadata immediately.
- Preprocess provides a deterministic ordering point for metadata updates.

Failure handling in preprocess.
- Errors are recorded per (term, index) in `PreprocessErrors`.
- When apply callbacks are resolved, preprocess errors are surfaced to clients.
- This preserves correctness without aborting the whole apply pipeline.

## Kvengine change set pipeline
Change sets represent kvengine metadata updates (flush, compaction, ingest, etc).
They are generated by kvengine and replicated via raft to all peers.

Where change sets originate.
- `MetaChangeListener::on_change_set` sends `StoreMsg::GenerateEngineChangeSet`.
- The store FSM forwards it to the target peer for proposal.
- The peer wraps the change set in a custom raft log.

Preprocess for change sets.
- Preprocess validates shard version against `ShardMeta`.
- Duplicated change sets are detected and skipped.
- Ingest change sets check overlap (unless legacy ingest).
- `kvengine::Engine::meta_committed` is invoked for bookkeeping.

Meta persistence strategy.
- Full metadata is stored under `KV_ENGINE_META_KEY` in rfengine.
- Diffs are stored under `KV_ENGINE_META_DIFF_KEY` when enabled.
- Snapshot diffs are stored under `KV_ENGINE_META_SNAP_DIFF_KEY`.
- Diffs are merged on load to reconstruct full `ShardMeta`.
- When diffs grow beyond a threshold, metadata is rewritten fully.

Prepare and apply phases.
- `ApplyMsg::PrepareChangeSet` calls `engine.prepare_change_set` in a background thread.
- The result returns to the raft worker as `PeerMsg::PrepareChangeSetResult`.
- `ApplyMsg::ApplyChangeSet` applies prepared change sets in order.

Ordering and pausing.
- `scheduled_change_sets` records change set sequences.
- `prepared_change_sets` stores prepared results by sequence.
- `paused_apply_queue` delays apply when a change set must be applied first.
- This avoids applying raft logs that depend on a change set not yet applied.

Special cases.
- Snapshot change sets reset apply state from snapshot metadata.
- Restore shard change sets bump region version to avoid stale change sets.
- Storage class updates may require reloading existing files.

## Integration points with kvengine and rfengine
rfstore sits between kvengine and rfengine and must honor both contracts.

kvengine integration points.
- Apply path writes MVCC data via kvengine write batches.
- Change sets are prepared and applied by kvengine.
- Shard metadata is stored in rfengine but interpreted by kvengine.
- Memtable switching is triggered from apply and executed in kvengine.
- Storage class updates reload files in kvengine.

rfengine integration points.
- Raft logs and state are persisted to rfengine via write batches.
- Engine meta and meta diffs are stored as state keys in rfengine.
- Dependents are tracked in rfengine and used to gate truncation.
- Peer state is stored with epoch derived keys.

Why meta lives in rfengine.
- Raft metadata must be durable with raft logs to ensure consistency.
- kvengine snapshots use rfengine metadata for recovery coordination.

## Custom raft log format
Custom raft logs encode user data operations efficiently.
They avoid extra lookups and minimize apply overhead.

`CustomRaftLogType` highlights.
- Prewrite and PessimisticLock write to `LOCK_CF`.
- Commit writes to `WRITE_CF` and removes locks.
- Rollback records rollback info and optionally deletes locks.
- OnePc combines prewrite and commit semantics.
- EngineMeta carries kvengine change sets.
- ResolveLock applies commit or rollback for a batch.
- SwitchMemTable forces memtable switching by size.
- TriggerTrimOverBound triggers range trimming based on shard bounds.
- TxnFileRef applies transaction file references.

Why custom logs exist.
- MVCC operations are hot and need minimal overhead in apply.
- The log format is compact and avoids repeated parsing of RaftCmdRequest.
- It allows per entry direct iteration over payload data.

## Transaction file flow and locks
Transaction files are handled through the `TxnFileRef` custom log type.
This flow is easy to break if ordering is not respected.

Preprocess phase.
- `preprocess_txn_file_ref` validates shard version against `ShardMeta`.
- The txn file ref is merged into shard metadata at the log index.
- If txn chunks already exist locally, no apply blocking is needed.
- If chunks are missing, `ApplyMsg::PrepareTxnFile` is scheduled.

Apply phase.
- The applier pauses the apply queue at the entry index for txn file refs.
- `handle_prepare_txn_file` prepares txn chunks via `txn_chunk_manager`.
- `PeerMsg::PrepareTxnFileResult` resumes apply with `ApplyMsg::ResumeTxnFile`.
- `exec_txn_file_ref` writes metadata for primary keys into `EXTRA_CF`.
- It also sets `TXN_FILE_REF` property in the write batch for kvengine.

Interaction with split and merge.
- Splits are rejected when `ShardMeta` reports txn file locks.
- Split validation can return key errors derived from txn file locks.
- Merge preparation is rejected when txn file locks exist.
- This prevents range changes while txn file data is incomplete.

Recovery interaction.
- In restore or recovery flows, `ctx.kv` may be `None`.
- In that case, chunk preparation is skipped and only metadata is updated.

## Read path and read index logic
rfstore supports multiple read policies based on safety requirements.

Request policy selection.
- `RequestInspector` decides ReadLocal, ReadIndex, or Propose.
- Reads use `ReadIndex` when lease is not valid or term not applied.
- Reads can be handled locally under a valid leader lease.
- `RequestPolicy::StaleRead` is used when the request explicitly allows stale reads.
- Stale reads are treated like local reads and skip leader checks.
- The flag is carried in the request header via `WriteBatchFlags::STALE_READ`.

Local read fast path.
- `LocalReader` uses `ReadDelegate` to serve reads without raft.
- It validates store id, peer id, term, and region epoch.
- It checks the leader lease before executing local reads.
- If validation fails, it forwards the request to raftstore.
- `ThreadReadId` allows reuse of a snapshot within a single RPC context.
- The snapshot time is refreshed if the delegate last valid timestamp advances.

Read index queueing.
- `ReadIndexQueue` batches read index requests per peer.
- It tracks outstanding requests with UUID contexts.
- Requests are merged within a lease window to reduce raft traffic.
- On followers, read index responses are tracked via the contexts map.
- Requests can carry an additional read index request for lock checking.
- If a locked result is returned, the response is sent immediately.

Retry behavior.
- Followers retry read index requests after election timeout.
- The retry uses the same request context to avoid reordering.
- This prevents permanent stalls when `MsgReadIndex` is lost.

Serving read index results.
- Leaders serve read responses when applied index is current term.
- Followers can serve replica reads when safe and requested.
- If a read index response carries a lock check result, it is returned directly.
- Replica reads require `replica_read` on the request header.
- Unsafe replica reads are only served when apply state has caught up.

Lease renewal interactions.
- Read index responses can trigger lease renewal on leaders.
- When lease is suspect, a no-op proposal can be issued to renew.

## Write path and proposal lifecycle
Write and admin requests are funneled through `PeerMsgHandler`.

Proposal pre checks.
- Store id check ensures the request targets the right store.
- Peer id check ensures the request targets the right replica.
- Term check rejects stale leaders.
- Region epoch check prevents stale metadata reads or writes.

Request policy in peer.
- Admin requests choose propose or read behavior via `RequestInspector`.
- Transfer leader requests are handled separately and do not apply logs.
- Conf change requests use special proposal paths.

`pre_propose` flags.
- Split and merge proposals set `ProposalContext::PRE_PROCESS`.
- Engine meta and txn file proposals also set `PRE_PROCESS`.
- User data requests set `ENCRYPTED` when encryption is enabled.
- Prepare merge proposals inject `min_index` to guard log gaps.

Proposal execution.
- `propose_normal` submits data to raft-rs and returns a log index.
- It checks `cmd_epoch_checker` only after applied index reaches current term.
- Admin proposals are rejected if the peer has not applied current term.

Proposal callbacks.
- Proposals carry callbacks with optional proposed and committed hooks.
- Stale proposals are notified when terms advance or leadership changes.
- Conf change callbacks are tracked separately to avoid mixing with normal ops.

## Split, merge, and conf change
Split and merge are the most complex admin flows in rfstore.
They involve preprocess, apply, store FSM updates, and PD coordination.

Split flow (high level).
- A split is scheduled by PD or by local key count checks.
- `validate_split_region` verifies leader role and epoch match.
- Initial flush is required to avoid deadlocks during snapshot creation.
- Txn file locks are checked to prevent unsafe splitting.
- A split proposal carries a change set to update shard metadata.
- Preprocess updates `preprocessed_region` and `ShardMeta` for new shards.
- Apply creates new regions and sends `ExecResult::SplitRegion`.
- Store FSM updates `RegionMap` and registers new peers.

Why split checks are strict.
- Split changes region ranges and must be serialized with metadata updates.
- Txn file locks may be stored outside the in memory range checks.
- Initial flush ensures snapshot availability for the new shard.

Merge flow (high level).
- `PrepareMerge` and `CommitMerge` are separate admin commands.
- `pre_propose_prepare_merge` enforces log gap checks.
- Merge requires sibling regions and identical peer sets.
- `kvengine::Engine::check_merge` validates shard compatibility.
- Overbound data triggers trim operations instead of merging.
- Unconverted L0s block merges to avoid incorrect data layout.
- Txn file locks block merges for safety.

Merge rollback flow.
- Peers track `want_rollback_merge_peers` for quorum decisions.
- If scheduling fails on a leader, it may propose `RollbackMerge`.
- Followers send extra messages to request rollback.

Conf change flow.
- Conf changes are applied via `ApplyResult::ChangePeer`.
- Apply updates region peer lists and can mark a peer for removal.
- Store FSM updates `RegionMap` and may destroy peers if removed.

## Snapshot creation and restore
Snapshots are encoded as `kvenginepb::ChangeSet` plus region metadata.
The snapshot is carried in raft snapshot data.

Snapshot generation in `PeerStorage`.
- Snapshot is available only after initial flush.
- Snapshot is rejected if `preprocessed_region` is newer than region.
- Snapshot includes region data and change set metadata.
- When initial flush is not ready, the snapshot request is deferred.
- The peer id is recorded in `snapshot_not_ready_peers` to retry later.
- Leaders bump follower progress to avoid followers getting stuck on snapshot requests.

Snapshot restore flow.
- `PeerStorage::restore_snapshot` decodes region and change set.
- It clears old metadata and writes new peer state.
- It updates raft state and applied state to snapshot index.
- It sets `snap_state` to `Applying` and stores `restored_snapshot`.

After persistence.
- `Peer::on_persist_ready` detects restored snapshots.
- It schedules `ApplyMsg::Registration` and `ApplyMsg::PrepareChangeSet`.
- Apply then ingests the snapshot change set into kvengine.

Why snapshots are separated.
- Raft snapshot application must be durable before kvengine ingest.
- Applying the change set can be expensive and must run off the raft loop.

## Dependencies and raft log truncation
Dependencies protect parent logs during split and merge.
They are tracked in rfengine and surfaced to rfstore.

Dependency mechanics.
- A child region depends on its parent until initial flush or snapshot.
- rfengine stores dependent relationships between region ids.
- `remove_dependent` is called after preprocess is persisted.
- When dependents drop to zero, `StoreMsg::DependentsEmpty` is emitted.

Truncation and destroy gates.
- Raft log truncation checks `has_dependents` before truncating.
- Peer destruction is delayed if dependents exist or snapshot is applying.
- `delay_destroy` captures the intent and resumes once dependents clear.

Why this matters.
- A child peer may require parent logs to recover after restart.
- Truncation before child recovery can make data unrecoverable.

## Maintenance and background tasks
Maintenance is handled via ticks to avoid polling overhead.

Peer maintenance tick (`PEER_TICK_MAINTENANCE`).
- Sends `ApplyMsg::Maintenance` to the applier.
- Shrinks the lock cache to control memory usage.
- Triggers memtable switching when size thresholds are met.
- Calls `check_gc_tombstones` to request compaction if safe.

Memtable switching rationale.
- Memtable size is only known on the apply side.
- Leaders propose a `SwitchMemTable` custom log when needed.
- The proposal is replicated to keep peers consistent.

Store maintenance ticks.
- Store heartbeat to PD includes region count and write stats.
- Safe ts updates are issued at fixed intervals.
- Local file GC runs periodically to reclaim disk space.

## Security and encryption
rfstore integrates with encryption for user data.

Encryption flow.
- A per shard encryption key can be stored in shard properties.
- `ProposalContext::ENCRYPTED` marks entries to be encrypted on proposal.
- Encrypted entries are decrypted in `util::parse_raft_cmd`.
- Custom logs that are metadata only are not encrypted.

Snapshot and encryption.
- Snapshot change sets may carry encrypted properties.
- On restore, encryption keys are loaded from shard properties.
- Restore flows update `Peer` encryption key accordingly.

Why encryption is tied to proposal context.
- It avoids decrypting non user metadata unnecessarily.
- It keeps the raft log format consistent across peers.

## Recovery, blacklist, and restore flows
rfstore includes a recovery path for offline or special restore flows.

Recover handler overview.
- `RecoverHandler` loads region meta and raft state from rfengine.
- It replays entries from applied index to last preprocessed index.
- It uses `PreprocessRef` to keep metadata consistent during replay.

Blacklist handling.
- `BlackList` can filter keyspaces, tables, or region ids.
- It is applied during peer loading and message handling.
- Blacklisted regions are skipped or ignored, not created.

Merged engine mode.
- `RecoverHandler` can operate in merged engine mode.
- In this mode, `region_id` is used as `peer_id` mapping.

Restore shard flow (runtime).
- The request validates range and leader before proposing.
- The snapshot base version is adjusted using current shard write sequence.
- `learner_skip_idx` is advanced to avoid sending restore to learners.
- Preprocess sets `pending_truncate` to trigger later log truncation.
- Region epoch is bumped to invalidate older change sets.

Why blacklist exists.
- Recovery sometimes needs to skip corrupted or irrelevant ranges.
- Filtering early prevents expensive peer creation and log replay.

## Schema and storage class integration
rfstore cooperates with schema management and storage class updates.

Schema worker responsibilities.
- `SchemaRunner` checks shard schema files against metadata.
- It detects overlaps and can request splits for exclusive tables.
- It proposes storage class changes via change sets.

Storage class update flow.
- A change set is built with `STORAGE_CLASS_KEY` property.
- The proposal is sent to the leader via `StoreMsg::GenerateEngineChangeSet`.
- On apply, kvengine reloads files for the target storage class.

Why schema checks live in rfstore.
- Schema overlap can require region splits at the Raft layer.
- Storage class changes must be replicated to keep peers consistent.

## GC and file lifecycle
rfstore manages local file cleanup via the GC worker.

GC worker behavior.
- It scans local directories for orphaned files.
- It compares file ids against shard metadata and blacklist.
- It removes unused SST, columnar, vector, and schema files.
- It also cleans up importer files and txn chunk files.

IA GC behavior.
- When IA is enabled, `IaGcRunner` removes expired IA metadata.
- GC configuration may be adjusted in test builds.

Why GC is externalized.
- File deletion is expensive and should not block raft or apply threads.
- Periodic GC keeps disk usage predictable.

## PD interaction and statistics
rfstore relies on PD for scheduling and cluster metadata.

PD tasks issued by rfstore.
- Store heartbeat with engine stats and capacities.
- Region heartbeat with size, key counts, and replication status.
- Split scheduling and batch split reporting.
- Read and write flow statistics.
- Role change notifications.
- Safe ts updates and sync region queries.

Flow stats reporting.
- `FlowStatsReporter` forwards read and write stats to PD worker.
- It is also used by kvengine integration points.

CPU utilization feedback.
- `CpuUtilCollector` updates raft worker CPU usage.
- The main raft worker uses this to enable or disable aux workers.

## Errors, drops, and backpressure
rfstore contains explicit error types for common failure modes.

Notable error categories.
- `NotLeader` and `RegionNotFound` for stale routing.
- `EpochNotMatch` for stale region metadata.
- `ReadIndexNotReady` for safe read preconditions.
- `IngestOverlap` for ingest conflicts (tagged by `INGEST_OVERLAP_ERROR_TAG`).
- `Transport` with `DiscardReason` for message drop causes.

Message drop reasons.
- Store id mismatch.
- Stale or missing region epoch.
- Tombstone peers or stale peer ids.
- Blacklist filters.
- Paused or full transport channels (backpressure).

Why drops are explicit.
- Raft correctness requires some messages to be ignored rather than processed.
- Explicit counters and logs help diagnose unexpected drops.

## Stale peer handling and membership safety
rfstore contains logic to avoid resurrecting stale peers.

Stale message handling.
- `StoreMsgHandler::check_msg` validates tombstone state.
- `PeerMsgHandler::check_msg` drops messages to stale peer ids.
- Stale messages may trigger GC messages to peers.

Stale merge handling.
- If merge targets are stale, peers may destroy themselves.
- This is safe because merge prechecks prevent partial merges.

Why this matters.
- Stale peers can cause split brain or incorrect metadata.
- Aggressive rejection is safer than accepting uncertain state.

## Configuration knobs with rationale
rfstore exposes many configuration fields via `Config`.
This section focuses on fields with non obvious impact.

Raft timing and elections.
- `raft_base_tick_interval` sets the base tick for all timers.
- `raft_heartbeat_ticks` and `raft_election_timeout_ticks` determine election timing.
- `raft_store_max_leader_lease` sets the lease used for local reads.
- `renew_leader_lease_advance_duration` controls early renewal.

Worker pools and batching.
- `apply_pool_size` and `apply_follower_pool_size` control apply parallelism.
- `aux_worker_count` enables extra raft workers under CPU load.
- `raft_worker_max_batch_size` caps batch size for inbox processing.
- `io_worker_min_write_duration` reduces write amplification in IO worker.

Metadata persistence.
- `enable_kv_engine_meta_diff` enables diff based meta persistence.
- `kv_engine_meta_diff_rewrite_percent` controls when diffs are compacted.

Split and merge behavior.
- `split_region_check_tick_interval` controls periodic split checks.
- `split_region_max_keys` caps split key count per request.
- `region_split_size` and `region_split_keys` influence PD splits.

Peer idling.
- `peer_idle_duration` determines when peers move to the idle worker.
- `idle_worker_tick_slow` reduces heartbeat overhead for idle peers.

Log GC.
- `raft_log_gc_tick_interval` schedules log GC.
- `raft_log_gc_size_limit` and `raft_log_gc_no_kv_count` gate GC triggers.

GC and file cleanup.
- `local_file_gc_timeout` and `local_file_gc_tick_interval` control file GC.

Why some defaults differ from upstream raftstore.
- rfstore optimizes for cloud storage and larger shard sizes.
- IO batching and apply parallelism are tuned for higher throughput.

## Metrics and slow log signals
rfstore uses metrics to capture performance and health.

Key rfstore specific metrics.
- `rfstore_propose_switch_mem_table_counter` tracks memtable switch proposals.
- `rfstore_idle_peers_count` tracks idle peers.
- `rfstore_lock_cache_len` and `rfstore_lock_cache_capacity` monitor lock cache.

Slow log signals.
- `SLOW_LOG_DURATION` is used for raft ready and apply path warnings.
- IO worker logs slow raft db writes and large batch sizes.
- Raft worker logs slow batch loops with top slow peers.

How to use these metrics.
- A high idle peer count with low throughput can be normal.
- Repeated slow raft ready logs often indicate expensive preprocessing or IO backpressure.
- Spikes in lock cache metrics can hint at transaction or lock cleanup issues.

## Detailed event sequences
This section provides step by step flows for complex operations.
Each sequence aligns with the actual code paths and message types.

Write request (normal, leader).
- Client sends a write request to the leader peer.
- `PeerMsg::RaftCommand` reaches the raft worker inbox.
- `PeerMsgHandler::propose_raft_command` runs pre checks.
- `pre_propose` sets proposal context flags and validates merge state.
- `propose_normal` submits the entry to raft-rs and returns an index.
- A `Proposal` is queued with callbacks and propose time.
- Raft replicates the entry and eventually commits it.
- `Peer::handle_raft_ready` preprocesses committed entries.
- `ApplyMsg::Apply` is sent to the apply worker.
- The applier writes KV data via kvengine write batch.
- `MsgApplyResult` is sent back to raft worker.
- `Peer::post_apply` updates apply state and read delegate.

Read request (local lease fast path).
- Request hits `LocalReader::read`.
- `LocalReader::pre_propose_raft_command` validates delegate and epoch.
- `RequestInspector` selects `ReadLocal` based on lease state.
- `LocalReader::execute` returns data from a region snapshot.
- Callback is invoked without touching raft.

Read request (read index path).
- Request reaches `PeerMsgHandler::propose_raft_command`.
- `RequestInspector` selects `ReadIndex`.
- `Peer::read_index` issues a raft read index request.
- `ReadIndexQueue` stores the request with a UUID context.
- `Ready.read_states` returns the read index for the context.
- `Peer::apply_reads` advances the queue and serves reads.

Engine change set (flush or compaction).
- kvengine calls `MetaChangeListener::on_change_set`.
- Store FSM sends `StoreMsg::GenerateEngineChangeSet` to the peer.
- Peer proposes a custom log with the change set.
- Preprocess applies the change set to `ShardMeta` and writes meta diff.
- `ApplyMsg::PrepareChangeSet` is sent to apply worker.
- Background prepare finishes and returns `PeerMsg::PrepareChangeSetResult`.
- `ApplyMsg::ApplyChangeSet` applies the change set in kvengine.
- Apply resumes any paused raft entry processing.

Split sequence (batch split).
- Leader receives split keys and validates them.
- Leader sends `PdTask::AskBatchSplit` to PD.
- PD responds with split instructions.
- Split is proposed and preprocessed with new shard metadata.
- Apply creates new regions and emits `ExecResult::SplitRegion`.
- Store FSM updates `RegionMap` and registers new peers.

Merge sequence (prepare and commit).
- Leader proposes `PrepareMerge` with min index check.
- Preprocess validates sibling regions and shard compatibility.
- `ApplyResult::PrepareMerge` updates region metadata.
- Leader proposes `CommitMerge` after merge conditions hold.
- Source change set is prepared in background.
- Apply commits merge and emits `ExecResult::CommitMerge`.
- Store FSM updates target region and destroys source peer when safe.

Snapshot restore sequence.
- Follower receives a raft snapshot in `Ready`.
- `PeerStorage::restore_snapshot` updates region and raft state.
- `restored_snapshot` is saved with the ready number.
- IO worker persists the ready and sends `PeerMsg::Persisted`.
- `Peer::on_persist_ready` schedules registration and change set preparation.
- Apply ingests the snapshot change set into kvengine.

## Debugging playbook
Common issues and how to reason about them.

Stuck snapshot application.
- Check if the peer is `SnapState::Applying` in logs.
- Verify dependents are empty (`has_dependents` in rfengine).
- Ensure initial flush completed on the shard.

Read index stalls.
- Verify the leader has applied to the current term.
- Check if the peer is splitting or merging (reads are blocked).
- Look for lost read index responses and retry behavior.

Merge stuck or rolling back.
- Check for txn file locks or unconverted L0 files.
- Check `want_rollback_merge_peers` quorum logs.
- Verify `check_merge` errors from kvengine.

Split rejected or delayed.
- Confirm the split keys are encoded and within the region range.
- Ensure leader role and epoch match.
- Ensure initial flush is complete and no txn file locks exist.

Write latency spikes.
- Inspect IO worker batch size and write duration logs.
- Check `io_worker_min_write_duration` for intentional throttling.
- Look for apply worker backlog or paused apply queues.

Unexpected region destruction.
- Look for stale raft messages or tombstone handling.
- Check store FSM logs for `delay_destroy` and `DependentsEmpty`.

## Tests and references
Tests and references to keep handy.
- `tests/rfstore/mod.rs` for rfstore specific tests.
- `tests/cloud_engine` suites for integration flows.
- `cmd/cse-ctl` tools often exercise rfstore APIs for maintenance.

References in code.
- `components/rfstore/src/store/peer.rs` for the primary control flow.
- `components/rfstore/src/store/apply.rs` for apply state machine.
- `components/rfstore/src/store/peer_storage.rs` for snapshot restore details.
- `components/rfstore/src/store/read.rs` and `components/rfstore/src/store/read_queue.rs` for read index logic.
