# Replication Worker Maintainer Guide

## Scope and assumptions
- This document describes replication_worker internals that are not obvious from
  reading the code directly.
- It assumes you already know TiKV, TiCDC, and PD at a high level.
- It focuses on design intent, ordering guarantees, and failure handling.
- It avoids restating trivial structs, configs, or API schemas.

## Why replication_worker exists
- It provides a CDC-compatible event stream without running full TiKV Raft.
- It reconstructs committed state by replaying WAL from upstream stores into a
  local merged_engine instance.
- It exposes a TiCDC-like gRPC endpoint and proxies TiCDC HTTP API requests.
- It manages keyspaces and changefeeds, including GC safepoints.

## Mental model
- Think of replication_worker as a CDC capture that owns a synthetic store.
- The synthetic store is the merged_engine, which replays Raft logs gathered
  from multiple upstream stores.
- CDC clients (TiCDC) connect to replication_worker directly and never talk to
  upstream TiKV stores.
- The worker also fakes enough PD state (store and region heartbeats) for the
  replication PD (rep-pd) to accept region metadata.

## Core modules and responsibilities
- `worker.rs` drives the main loop, keyspace orchestration, CDC register state
  machines, WAL syncing, and resolved-ts emission.
- `apply_observer.rs` converts apply write batches into CDC events and lock
  tracking updates.
- `delegate.rs` keeps per-region request state, buffered events, and the region
  resolver used for resolved-ts.
- `wal.rs` fetches WAL progress targets (rfengine or backup) and queues them for
  syncing.
- `safepoint.rs` synchronizes changefeed checkpoints with PD service safepoints.
- `scheduler.rs` is the HTTP front door that forwards to TiCDC and schedules
  internal tasks.
- `kube.rs` and `provisioned.rs` create keyspace PD/CDC services.
- `util.rs` contains TiCDC request helpers, keyspace key range helpers, and
  resolved-ts stats helpers.

## Startup and recovery
- `ReplicationWorker::new` bootstraps merged_engine and loads its keyspace
  states from the manifest.
- For each keyspace with persisted state, it recreates the keyspace service and
  starts reporting regions to the keyspace PD.
- It starts a local GC runner that cleans up local files created by the merged
  engine.
- It creates a gRPC server exposing `ChangeData` and `Tikv` services.
- It initializes `CdcApplyObserver` and wires it into the merged_engine apply
  pipeline.
- It constructs `ServiceSafepointManager` with existing keyspaces and feeds.
- It intentionally delays starting the WAL progress fetcher until `run()` so
  that the runtime is ready.

## Main run loop summary
- The loop is message-driven and time-driven.
- It drains `CdcMsg` from a channel and processes them synchronously.
- It periodically evaluates `should_sync()` based on `sync_interval`.
- When there are new events or a sync tick, it emits resolved-ts and attempts
  to advance merged_engine.
- When a WAL target completes, it updates `last_update_ts` and notifies
  `WalProgressFetcher` through `synced_target_ts`.

## Threading and runtime model
- The main loop is synchronous, but it spawns async tasks for IO and scans.
- Incremental scans run on the tokio runtime and are rate-limited by a
  semaphore.
- Lock scans run on a blocking thread to avoid starving async tasks.
- HTTP requests to TiCDC and stores are performed by async tasks; failures are
  reported back through callbacks.
- The apply observer defers old-value reads onto the runtime to keep apply
  latency low.

## Keyspace services and their lifecycle
### Provisioned vs Kubernetes mode
- Provisioned mode expects explicit `pd_url` and `cdc_addr` per keyspace.
- Kubernetes mode derives PD/CDC addresses from StatefulSet templates.
- In Kubernetes mode, `KeyspaceKubeService` overwrites persisted PD/CDC fields
  to follow the latest deployment layout.
- Both modes run `bootstrap` against the keyspace PD to ensure the cluster is
  initialized with the merged store ID.

### Add keyspace flow (high level)
- The scheduler receives `POST /cdc/keyspace` and sends `CdcMsg::AddKeyspace`.
- The worker starts the keyspace service asynchronously on the runtime.
- It validates that the keyspace TiCDC endpoint is ready before loading shards.
- It pauses meta pack compaction to prevent prepared shard files from being
  compacted mid-load.
- It prepares shard metas twice to cover concurrent raft updates.
- It loads shards into merged_engine and persists keyspace states.
- It starts a periodic region-report loop to rep-pd for that keyspace.
- It registers the keyspace with the safepoint manager.

### Load shard metas, why two passes
- During keyspace add, rfengine continues to append WAL.
- The first pass prepares a consistent set of `ShardMeta` from the snapshot.
- The second pass reduces the chance of a missed update between preparation and
  load.
- This avoids subtle holes where a shard exists in rfengine but not in
  kvengine, which would later break CDC reads and apply.

### Remove keyspace flow
- The scheduler receives `DELETE /cdc/keyspace` and sends `CdcMsg::RemoveKeyspace`.
- The worker rejects removal if any changefeeds remain in the keyspace state.
- It unregisters the keyspace from safepoint management.
- It removes all regions from merged_engine and drops the keyspace manifest
  state.
- It destroys PD/CDC services (in Kubernetes mode) asynchronously.

### Keyspace state persistence
- Keyspace states are stored in the merged_engine manifest as opaque bytes.
- On restart, these states rebuild keyspace services and active feeds.
- The code treats missing keyspaces as a clean slate and ignores dangling
  regions that are not in the manifest.

## CDC connections and requests
### gRPC event feed sessions
- The gRPC `ChangeData::event_feed` handler creates a per-connection CDC
  channel with a memory quota.
- The memory quota is the primary backpressure mechanism for CDC message
  delivery.
- Each connection is tracked in `conns` and its subscribed regions are tracked
  in `conn_regions`.
- Both the request stream and event sink trigger a `Deregister::Conn` on close.

### Deregister paths and cleanup
- `Deregister::Conn` removes all requests for the connection across regions.
- `Deregister::Request` removes a request from every region it subscribed to.
- `Deregister::Region` removes a request for a single region only.
- When a region has no remaining requests, its delegate is dropped to free
  resolver state and buffered events.

### Register request handling goals
- Ensure no duplicate or out-of-order events reach TiCDC during initialization.
- Ensure resolved-ts only advances when all locks are accounted for.
- Avoid scanning or applying data while the region delegate is stale.
- Provide idempotent handling for repeated register requests.

### Register state machine (per region, per request)
- States: `Blocked`, `Initializing`, `Initialized`.
- New requests are `Blocked` if `checkpoint_ts` is ahead of current resolved-ts.
- `Initializing` buffers events until the initial scan finishes.
- `Initialized` sends events directly to the sink.
- Each initialization is tagged by `InitId` so stale tasks can be ignored.

### Register flow in detail
- Validate region existence and epoch using merged_engine shard metadata.
- If the delegate exists but the region version does not match, fail fast with
  `EpochNotMatch` to force TiCDC to retry.
- Decide whether to scan locks and whether to run incremental scan.
- Flush the apply observer for the region before scanning to avoid duplicates.
- Spawn a lock scan if the delegate does not yet have a resolver.
- Spawn an incremental scan handler if this request is not a duplicate.
- The incremental scan runs on the async runtime and respects a global
  semaphore to cap concurrency.

### Resuming blocked registrations
- When resolved-ts advances past a blocked request's checkpoint_ts, the worker
  resumes initialization using `CdcMsg::ResumeRegister`.
- Resume uses the same snapshot version as the delegate to avoid mixing epochs.
- The apply observer is flushed again before resume to avoid duplicate events.
- If the connection is already closed, the resume is safely ignored.

### Why we flush the apply observer before scanning
- The apply observer may have pending write batches for the region.
- If we scan locks or perform an incremental scan without flushing, we can
  duplicate events or miss lock tracking transitions.
- Flushing ensures a clean boundary between historical scan results and
  subsequent apply-driven events.

### Incremental scan handler details
- Uses `new_delta_write_iterator_async(checkpoint_ts)` over the write CF.
- Skips index keys and deletes, and ignores versions <= `checkpoint_ts`.
- Computes `old_value` by reading at `commit_ts - 1` to match TiCDC semantics.
- Batches rows to avoid huge events and uses a barrier to enforce ordering.
- Emits a final `Initialized` event and then sends `RegisterResult`.

### Why the barrier is required
- TiCDC expects resolved-ts to advance only after the scan is fully delivered.
- The barrier ensures the sink has flushed all scan events before we send the
  initialization completion signal.
- Without it, resolved-ts could overtake the scan and violate TiCDC ordering
  assumptions.

## Lock scanning and region resolver
### Initial lock scan
- The lock scan iterates the region lock CF and collects lock keys and start_ts.
- It ignores index keys and non-Put/Delete locks to reduce noise.
- It reports results back to the worker with the snapshot version used.

### Pending resolver state
- Before the scan completes, the resolver is in `Pending` mode.
- Lock track/untrack events from the apply observer are queued in memory.
- The pending list is converted to a real `Resolver` once scan results arrive.
- This avoids losing lock updates that occur during the scan window.

### Resolver upgrade and safety
- The scan result is applied only if the snapshot version matches the pending
  resolver's expected version.
- Stale scan results are discarded to avoid corrupting lock state.
- On scan error, the resolver is removed and errors are broadcast to the
  region's requests.

## Resolved-ts computation and emission
### Preconditions for resolved-ts
- `last_update_ts` must be non-zero (merged_engine has advanced).
- The region must be present in merged_engine and in a synced state.
- The resolver must have completed lock scanning.

### send_resolved_ts flow
- For each region delegate, the resolver advances using `last_update_ts`.
- Resolved-ts is recorded in the request state, and only advanced forward.
- Blocked requests are resumed once their `checkpoint_ts` is <= resolved-ts.
- Resolved-ts events are grouped by `(ts, request)` so multiple regions can be
  sent in one message.
- This grouping is a throughput optimization and preserves per-request ordering.

### Why we check `region_is_synced`
- Merged_engine can track regions that have not yet applied WAL data.
- Sending resolved-ts for those regions could move TiCDC ahead of actual data.
- The `region_is_synced` gate avoids this mis-ordering.

## Apply observer pipeline
### What the observer captures
- Applied writes from the write CF as `EventRow` entries.
- Lock CF changes as tracked lock updates for the resolver.
- Admin commands that affect region topology (split/merge) to trigger
  delegate cleanup and PD reporting.

### Asynchronous old value fetch
- The observer defers fetching `old_value` to a runtime task.
- This keeps the apply path lightweight while still delivering TiCDC-required
  old values for committed rows.
- A per-region task list ensures all queued batches for a region are flushed
  together and preserve ordering.

### Flushing behavior
- `flush_region` drains only one region and is used during register handling
  and admin events.
- `flush` drains all regions and is used for global synchronization.
- Flushing emits a single `CdcMsg::Applied` per region containing all events
  and lock tracking updates for that batch.

## Admin commands and region topology
### Applied admin handling
- On split/merge/rollback, the observer flushes the region and sends an
  `AppliedAdmin` message.
- The worker reports region metadata to the keyspace PD so TiCDC can locate
  region ranges.
- For splits, it also reports the new regions created by the split requests.

### Merge handling and delegate cleanup
- `commit_merge` implies the source region is gone.
- The worker sends a `region_not_found` error to all requests of the source
  region and removes the delegate.
- For the target region, an `epoch_not_match` error is sent to force TiCDC to
  re-register with the new epoch.

## WAL progress fetching and target selection
### Target queue
- `WalProgressFetcher` runs in the background and appends targets to a shared
  queue.
- Each target is `(target_ts, StoreWalProgresses)`.
- The queue is bounded (`MAX_PENDING_TARGETS`) to avoid unlimited lag.

### Choosing rfengine vs backup
- Normally the worker tracks WAL progress from rfengine for each store.
- If the last synced target is too old, targets are fetched from backups.
- This prevents missing stores that may have joined and left between targets.
- The min/max time span bounds are a safety mechanism, not a rate limiter.

### Skipping stores
- `skip_store_addr_keywords` allows filtering stores by address.
- It is applied during rfengine fetches and during backup target construction.
- This is used to ignore stores that should not be part of replication.

### Tolerated store errors
- Both rfengine fetch and backup selection can tolerate a limited number of
  missing stores.
- The tolerance is configurable and must match backup metadata semantics.
- If the number of missing stores exceeds tolerance, replication stops.

## Store WAL update pipeline
### Ordering stores for update
- `get_decreasing_stores_lag` sorts stores by lag (epoch, then offset).
- The worker pulls the most lagging stores first to converge faster.
- Lag is computed against merged_engine's stored progress, not PD.

### Global WAL size limit
- `update_stores_wal_size_limit` caps total WAL bytes applied per sync cycle.
- If the limit is reached, the target is treated as partially synced and will
  be retried in the next cycle.
- This bounds memory and apply latency, and prevents long sync stalls.

### Fetching WAL from a store
- For each store and epoch, the worker requests WAL chunks via the store HTTP
  endpoint `/rfengine/wal_chunk`.
- The request includes `start_off` and `end_off`, where `end_off` may be zero
  (read to end) depending on epoch.
- If the store responds `GONE`, the worker falls back to S3 for that epoch.

### Fetching WAL from S3
- S3 retrieval is used when the store is unavailable or the epoch is too old.
- The worker fetches a full epoch into `WalCache` and then slices ranges.
- `WalCache` reduces repeated downloads when multiple ranges are needed.
- The cache is cleared once the epoch is fully applied and rotated.

### Rotation rules
- Rotation is triggered when the end offset reaches the end of an epoch.
- The worker calls `merged_engine.rotate_wal` only when its store progress
  exactly matches the end offset.
- This prevents skipping data or double-applying WAL bytes.

## sync_merged and last_update_ts
- After WAL updates, `merged_engine.sync_merged` applies committed entries to
  the kv engine and advances region progress.
- The apply observer is flushed to ensure all events are visible to CDC before
  updating `last_update_ts`.
- `last_update_ts` advances only when the target is fully applied.
- This is why `send_resolved_ts` relies on `last_update_ts` rather than the
  current time.

## Changefeed creation and idempotency
### start_ts handling
- If the user passes `start_ts=0`, the worker sets it to `last_update_ts`.
- This guarantees the changefeed starts from data already materialized in the
  merged engine.
- If the user passes a non-zero `start_ts` greater than `last_update_ts`, the
  request is rejected to prevent gaps.

### Why the request body is persisted
- `KeyspaceStates.feeds` stores the original request body per changefeed ID.
- If TiCDC retries a create request, the worker reuses the stored body to avoid
  divergent parameters.
- This also allows the worker to rebuild changefeed state after restart.

### Changefeed deletion
- The scheduler forwards the delete request to TiCDC and then removes local
  state via `CdcMsg::RemoveTask`.
- The local deletion is idempotent; repeated deletes are treated as success.
- Safepoint state is updated when the changefeed is removed from local state,
  and the manifest is persisted afterward.

## HTTP scheduler behavior
- Requests under `/cdc/` are translated to TiCDC paths by stripping the
  prefix and forwarding with the original method and headers.
- `/cdc/keyspace` requests are intercepted to add or remove keyspaces.
- `/cdc/api/v2/changefeeds` is intercepted to inject keyspace-specific
  validation and to run start_ts safety checks.
- `/cdc/keyspace` GET returns the current set of keyspace IDs from the worker.

## Safepoint management
### Ensuring start_ts safety
- During changefeed creation, the worker updates a per-changefeed PD service
  safepoint with a short TTL.
- This prevents GC from removing data between request submission and feed
  initialization.
- The safepoint is not removed later, trading cleanup for operational safety.

### Periodic TiCDC sync
- A timer task fetches changefeed lists from each keyspace TiCDC.
- For active feeds, it updates the keyspace service safepoint to the minimal
  checkpoint_ts and extends the TTL.
- This keeps GC aligned with the slowest changefeed.

### Expired changefeeds
- Feeds whose checkpoint_ts is older than GC TTL are considered expired.
- The worker updates the replication PD GC safepoint to the max expired
  checkpoint_ts, which triggers TiCDC to mark those feeds failed.
- This mirrors TiCDC's GC blocking logic and prevents silent data loss.

### Keyspace removal
- When a keyspace is removed, its safepoint entry is removed as well.
- This avoids holding GC back for a keyspace that no longer exists.

## PD reporting loops
- The worker periodically re-reports region metadata to rep-pd.
- This handles rep-pd restarts that lose in-memory region leaders.
- Reporting uses trimmed keyspace prefixes so rep-pd sees per-keyspace ranges.
- The store heartbeat is also periodically sent to keep the merged store alive.

## Read RPCs (kv_get / kv_scan)
- The gRPC `Tikv` service reads from merged_engine snapshots.
- It verifies region epoch using merged_engine shard metadata.
- It prepends keyspace prefixes for internal reads and strips them for replies.
- It does not resolve locks; callers must tolerate stale or locked data.
- This is a deliberate simplification because replication_worker is not a full
  transactional read path.

## Backpressure and resource control
- CDC channels are memory-quotad to prevent unbounded buffering.
- Incremental scans are capped by a global semaphore.
- WAL updates are capped by a per-cycle byte limit.
- These limits prevent cascade failures when TiCDC or upstream stores are slow.

## Error handling patterns and invariants
- Many operations return `server_is_busy` to force client retries instead of
  accepting inconsistent state.
- Delegate version mismatches are treated as stale and trigger retries.
- `InitId` prevents a stale scan from completing a newer registration.
- Store progress mismatches are treated as fatal because they imply WAL
  corruption or reordering.
- Safepoint errors are logged and may not be retried immediately to avoid
  blocking the main loop.

## Operational debugging cues
- If CDC stalls but merged_engine keeps syncing, check `send_resolved_ts` paths
  and resolver initialization (lock scan completion).
- If add keyspace hangs, verify CDC status and meta pack compaction pause.
- If WAL sync stalls with `StoreProgressMismatch`, check for skipped or reordered
  WAL chunk responses from stores.
- If changefeed creation fails with `start_ts too large`, check `last_update_ts`
  advancement and WAL target completion.

## Common pitfalls for maintainers
- Do not remove the apply observer flush in register or resume flows.
- Do not advance `last_update_ts` unless the target is fully applied.
- Do not emit resolved-ts for regions that are not yet synced in merged_engine.
- Do not remove the meta pack compaction pause around shard preparation and
  loading.
- Do not assume kv_get/kv_scan are lock-aware; they are not.

## Extension points and safe changes
- Adding new CDC event types should go through the apply observer and maintain
  the pending-event buffer semantics for initializing requests.
- Changing WAL target selection should preserve min/max time span semantics or
  update backup logic accordingly.
- Adding new keyspace lifecycle steps should update keyspace states in the
  manifest to survive restarts.
