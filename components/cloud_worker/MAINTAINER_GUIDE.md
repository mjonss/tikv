# Cloud Worker (tikv-worker) Maintainer Guide

This document is for maintainers who need to reason about correctness,
performance, and operability of the `cloud_worker` component.
It emphasizes complex logic and design rationale rather than restating code.

Scope of this doc:
- The `components/cloud_worker` crate and its runtime behavior.
- The interactions with PD, TiKV stores, DFS (S3), and Kubernetes.
- The major subsystems hosted inside the worker process.

Out of scope:
- Full API parameter listings that are trivially read from code.
- Detailed TiKV or kvengine internals not directly exercised by worker logic.
- Detailed Kubernetes manifests outside of what the worker reads or writes.

If you are new to the codebase, skim these first:
- `components/cloud_worker/src/lib.rs`
- `components/cloud_worker/src/server.rs`
- `components/cloud_worker/src/load_data.rs`
- `components/cloud_worker/src/worker_scaler.rs`
- `components/cloud_worker/src/native_br.rs`
- `components/cloud_worker/src/schema_manager.rs`
- `components/cloud_worker/src/remote_cop.rs`
- `components/cloud_worker/src/txn_chunk.rs`

Related feature docs (read for background, not for code truth):
- `doc/features/remote_coprocessor.md`
- `doc/features/txn_file.md`
- `doc/features/schema_manager.md`
- `doc/features/worker-scaler.md`

--------------------------------------------------------------------------
## High-Level Responsibilities

The cloud worker is a sidecar-style service that offloads heavy or
specialized operations from TiKV or other components.
It is deployed as the `tikv-worker` binary in cloud environments.

It hosts multiple subsystems:
- Remote coprocessor execution (HTTP and gRPC paths).
- Remote compaction of SSTs (store-triggered).
- Load data ingestion and optional load-data worker scaling.
- Native backup and restore of keyspaces (native BR).
- Schema manager for columnar/schema-file propagation.
- Transaction chunk file creation for file-based transactions.
- Replication worker (CDC) request handling.
- Local GC for IA segments (kvengine IA integration).

The worker is intentionally broad in scope.
This is why it owns its own runtimes, caching, memory controls,
and several background loops.

--------------------------------------------------------------------------
## System Context and Data Flows

The worker sits between control plane services and storage nodes.
It talks to three main categories of dependencies:

- PD (Placement Driver)
  - cluster id, TSO, global config, store metadata.
- DFS / S3 (via kvengine::dfs::S3Fs)
  - read/write schema files, compaction IO, txn chunks, backups.
- TiKV stores
  - REST endpoints for schema update and stats.
  - gRPC delegate coprocessor requests.

A typical deployment has several worker instances.
Most requests are stateless per worker, but some tasks are long-running.
Per-process state is intentionally persisted in small local files
to survive restarts (e.g., native BR restore metadata,
load data checkpoints, schema meta file state).

Simplified interaction diagram:

  +------------------+        +-------------------------+
  |  Clients (TiDB)  |        |    Control Plane (PD)   |
  +------------------+        +-------------------------+
            |                           |
            |  offload / manage         |  metadata / TSO
            v                           v
  +------------------------------------------------------+
  |               tikv-worker (cloud_worker)             |
  |  - HTTP server  - gRPC server  - background loops    |
  |  - DFS client   - PD client    - security manager    |
  +------------------------------------------------------+
            |                           |
            |  schema / compaction      |  stores list
            v                           v
  +------------------+        +-------------------------+
  |   TiKV stores    |        |         S3/DFS          |
  +------------------+        +-------------------------+

Key idea:
- The worker is a coordinator and executor for data-plane tasks
  that would otherwise overload TiKV or require extra dependencies.

--------------------------------------------------------------------------
## Process Model and Startup Sequence

The startup sequence is layered to isolate heavy work from request IO.
This helps avoid deadlocks and reduces latency impact.

Main steps (rough order):
1. Build two Tokio runtimes.
   - One for heavy worker tasks (`worker-server`).
   - One for HTTP serving (`hyper-server`).
   - The split prevents hyper from being blocked by long tasks.
2. Create a `RunningController` to manage shutdown.
3. Initialize DFS client (`S3Fs`) from config.
4. Initialize caches and helpers.
   - block cache, columnar meta cache, meta file cache.
   - transaction chunk manager.
5. Initialize security manager and master key.
6. Optionally start worker scaler.
7. Create data directories (if configured).
8. Create `LoadDataManager` and possibly recover checkpointed tasks.
9. Create `NativeBrManager` and start its background loop.
10. Create `TxnChunkHandler`.
11. Create `WorkerLimiter` and `MemoryLimiter`.
12. Create IA context and start local GC if IA is enabled.
13. Optionally start replication worker.
14. Build server context (`server::Context`).
15. Start HTTP server and optional gRPC remote coprocessor server.
16. Start schema manager if enabled.
17. Optionally register remote compactor with all stores.
18. Spawn a background task to record global memory usage.
19. Optionally start Prometheus push loop.

Design rationale:
- `worker-server` runtime is used for CPU and IO-heavy tasks.
- `hyper-server` runtime is used for short-lived request parsing.
- Some tasks run in std threads to avoid Tokio drop panics
  or to keep background loops simple.

Shutdown behavior:
- A oneshot is used to break the hyper loop.
- `RunningController` stops background loops.
- Replication worker is stopped and joined.
- Shutdown can be forced in tests (random). 

--------------------------------------------------------------------------
## Configuration Model (Conceptual)

Config is a flat struct with embedded nested configs.
Fields are serialized with `kebab-case` names.

Key categories and why they exist:
- **Network / endpoints**
  - `addr` and `cop_addr` define HTTP and gRPC front doors.
  - Distinct addresses allow separating remote cop paths.
- **Storage / DFS**
  - `dfs` controls S3 and compression behavior.
  - `data_dir` is used for local caching and task metadata.
- **Resource control**
  - `thread_pool_size_factor` scales worker runtime threads.
  - `memory_upper_threshold` caps memory usage for snapshots and compaction.
  - `worker_limiter` throttles concurrency globally and per keyspace.
- **Subsystem toggles**
  - `schema_manager.enabled`, `replication_worker.enabled`,
    `worker_scaler.run`, etc.
- **Submodule knobs**
  - Each subsystem has its own config block.

Dynamic reloading:
- Only native BR configuration is reloaded by the background worker.
- Other config fields require restart to take effect.

IA enablement (conceptual):
- IA is enabled only when remote cop is enabled,
  `data_dir` is set, and IA mem/disk caps are non-zero.
- This avoids IA overhead when the worker is not serving cop requests.

--------------------------------------------------------------------------
## Server Surface and Request Dispatch

The worker exposes two servers:
- An HTTP server (hyper) for most APIs.
- An optional gRPC server for delegate remote coprocessor requests.

Request dispatch is centralized in `server::start`.
Handlers offload heavy work to the worker runtime.
This avoids blocking hyper IO threads.

The worker uses consistent patterns:
- Parse and validate inputs early.
- Enforce security and cluster id checks.
- Enforce concurrency and memory limits before heavy work.
- Use detailed logs and metrics for long operations.

Important endpoints (high-level):
- `/coprocessor`: HTTP remote coprocessor execution.
- `/compact`: remote compaction entry point.
- `/cdc/*`: CDC related requests delegated to replication worker.
- `/load_data`: load data task management and ingestion.
- `/txn_chunk`: transaction chunk creation for file-based transactions.
- `/api/v1/backups`: list backups for native BR.
- `/api/v1/restore_keyspace/*`: restore keyspace operations.
- `/metrics` and `/debug/pprof/*`: observability endpoints.

Why a single HTTP server?
- Most operations share common security and config context.
- We can enforce resource limits consistently.
- The hyper runtime can be sized to match request volume.

--------------------------------------------------------------------------
## Cross-Cutting Resource Controls

The worker has multiple layers of protection.
They are intentionally redundant because different operations
stress different resources.

### MemoryLimiter

- Constructed from `memory_upper_threshold`.
- Used when building snapshots for remote coprocessor.
- Used when performing remote compaction.
- Returns `SERVICE_UNAVAILABLE` for memory exhaustion.

Design rationale:
- Snapshot construction is expensive and must be bounded.
- Compaction can double memory use (read + write buffers).
- Separate memory limiter avoids OOM in the worker process.

### WorkerLimiter

- Global semaphore limits total concurrent work.
- Per-keyspace semaphores prevent a single keyspace from monopolizing
  worker capacity.
- Vector-index compaction has its own semaphore and is fail-fast.

Design rationale:
- Keyspace fairness is required for multi-tenant environments.
- Vector-index compaction is CPU heavy and must not queue long.
- Use semaphores instead of queues to keep timeouts simple.

### QuotaLimiter

- Used in remote coprocessor execution paths.
- Limits per-request resource usage inside coprocessor.

### Thread pools and runtime separation

- Heavy work is delegated to the `worker-server` runtime.
- HTTP parsing stays on the `hyper-server` runtime.
- Long synchronous loops (native BR cleanup, local GC)
  run on dedicated OS threads.

--------------------------------------------------------------------------
## Remote Coprocessor (HTTP Path)

This is the primary offload path for heavy TiDB queries.
The HTTP path is used when TiKV decides to offload requests
based on size or policy.

### Request Flow Summary

1. Hyper receives `/coprocessor` request.
2. Body is read into memory (bytes).
3. Request is decoded into three parts:
   - coprocessor request protobuf.
   - in-memory table data.
   - snapshot change data.
4. The request is validated and parsed.
5. Per-keyspace and global permits are acquired.
6. A `SnapCtx` is built with DFS and caches.
7. `SnapAccess::construct_snapshot` builds a snapshot.
8. Optional IA prefetch for DAG requests.
9. The coprocessor is executed via `parse_request_and_handle_remote_cop`.
10. Metrics and logs are emitted.
11. Response is serialized and returned.

### Why this design

- The request carries snapshot deltas from TiKV to avoid
  coordinating snapshot materialization in the worker.
- The worker reads data from DFS/S3 and can use a remote DFS cache
  to avoid repeated network reads.
- Timeouts at multiple stages prevent worker overload.
- Per-keyspace permits ensure fairness.

### Timeouts, response size, and DFS cache

- Request timeout uses the coprocessor context max duration.
- If max duration is zero, a default timeout is applied.
- `cop_max_resp_size` limits response size during execution.
- If the request includes `DFS_REMOTE_CACHE_ADDR_HEADER`,
  the worker wraps S3Fs with `RemoteCachedDfs`.
- The remote cache path uses `cop_remote_dfs_cache_ttl`
  to expire cached entries.
- `read_columnar` switches snapshot preparation from
  `SstOnly` to `All`, enabling columnar metadata reads.

### Snapshot construction and memory guard

`SnapAccess::construct_snapshot` is the expensive step.
It allocates memory for both memtable data and snapshot state.
The memory guard from `MemoryLimiter` must be held for the entire
coprocessor execution to avoid unbounded memory use.

Behavior:
- If snapshot construction exceeds memory limits,
  the handler returns `503 Service Unavailable`.
- If construction exceeds request timeout,
  the handler returns `503 Service Unavailable`.

### IA prefetch (when enabled)

If IA is enabled and the request is DAG:
- The worker prefetches IA segments for the requested key ranges.
- Cache hit ratios are tracked for observability.
- If prefetch exceeds the request deadline,
  the handler returns `503 Service Unavailable`.

Rationale:
- IA prefetch is helpful when the DFS cache misses.
- It avoids partially executing large DAG requests.

### Error handling

Errors are mapped to:
- `500` for internal failures.
- `503` for retryable failures (permit timeout, memory limit, deadline).
- If the client accepted protobuf, error responses are serialized
  in protobuf format to avoid client-side parsing issues.

### Observability

Key metrics:
- `tikv_worker_remote_cop_snapshot_duration_seconds`
- `tikv_worker_remote_cop_prefetch_duration_seconds`
- `tikv_worker_remote_cop_request_duration_seconds`
- Request counters and response size counters by request type.

Key log context:
- request tag (keyspace, region, epoch, start_ts)
- snapshot duration
- prefetch duration
- processing duration
- response size

--------------------------------------------------------------------------
## Remote Coprocessor (gRPC Delegate Path)

The gRPC path is used for a different deployment model.
In this model, TiKV sends a delegate request to the worker,
which then proxies a request back to TiKV.

### Request Flow Summary

1. TiKV calls worker gRPC `coprocessor`.
2. Worker builds a `DelegateRequest` with the ranges.
3. Worker looks up the TiKV store address from PD.
4. Worker sends delegate request to TiKV via gRPC.
5. TiKV replies with memtable data and snapshot deltas.
6. Worker constructs snapshot using those deltas.
7. Worker executes coprocessor locally.
8. Worker returns gRPC response.

### Why this path exists

- Some environments prefer gRPC and direct delegate requests.
- It allows TiKV to retain control of snapshot generation.
- It avoids large HTTP bodies at the expense of another gRPC hop.

### Concurrency and memory protection

- MemoryLimiter is applied to snapshot construction.
- QuotaLimiter is used for coprocessor execution.
- The gRPC server has explicit channel settings to avoid
  default low limits for message size.

### Store address caching

The worker caches store addresses and gRPC channels:
- Store addresses are fetched from PD and cached by store id.
- gRPC channels are cached by address and re-used.

Rationale:
- Reduce PD load and connection churn.
- Keep per-request latency stable under load.

--------------------------------------------------------------------------
## Remote Compaction

Remote compaction is a store-driven operation.
TiKV stores ask the worker to compact SST data and return a new file.

### Registration with TiKV stores

When `config.register` is enabled:
- The worker builds a remote compaction URL.
- A background loop periodically registers the URL with each store.
- Stores are fetched from PD, excluding TiFlash.

Design rationale:
- Periodic re-registration tolerates store restarts.
- Excluding TiFlash avoids sending irrelevant requests.

### Compaction request handling

Key steps in `/compact`:
- Parse JSON `CompactionRequest`.
- Validate compactor version for compatibility.
- Special-case vector-index compaction: try to acquire a
  vector-index permit without waiting.
- Acquire a memory guard for `input_size * 2`.
- Decrypt exported encryption key if present.
- Build `CompactionCtx` with DFS, checksum and compression.
- Spawn `local_compact` on the worker runtime.
- Serialize `CompactionResponse` or return errors.

Why the memory multiplier is 2x:
- Compaction simultaneously reads and writes.
- Buffering for both sides can exceed input size.

Why vector-index compaction is fail-fast:
- Vector-index compactions are heavy.
- They are safe to retry on other workers.
- This prevents long queues from stalling normal compactions.

Error semantics:
- `503` for memory limit or throttled vector-index compaction.
- Custom error code (`INCOMPATIBLE_COMPACTOR_ERROR_CODE`)
  for incompatible compactor version.
- `500` for other failures.

--------------------------------------------------------------------------
## Load Data Manager

Load data is a long-running ingestion flow.
The worker may execute the task locally or delegate to
separate load-data worker pods (via worker scaler).

The worker owns task scheduling, checkpoint recovery,
and load-data task state reporting.

### Core design choices

- Use a per-task scheduler running in its own OS thread.
- Keep the HTTP API small and use task IDs as the main handle.
- Persist task state with checkpoints if enabled.
- Delegate to load-data worker pods for large tasks.

### Load data config derivation

- `enable_checkpoint` and `checksum_type` are propagated to `LoadDataConfig`.
- If `TIKV_LOAD_DATA_WORKER` is set, the worker uses
  `TIKV_LOAD_DATA_WORKER_WORKER_NUM_ENV` to size local worker threads.
- If `report_wru` is enabled, the worker loads the resource group
  config from PD global config path `resource_group/controller`.
- The resource group config is attached to the load data runtime
  to drive WRU reporting in load data metrics.

### Task lifecycle (conceptual)

1. Client sends an init request with task id and timestamps.
2. Worker decides if it should run locally or spawn a pod.
3. Client streams chunk data via PUT requests.
4. Client triggers flush and build phases.
5. Client polls task state for progress.
6. Client deletes task when finished or canceled.

### Local execution path

- `LoadDataManager::init_task` spawns a dispatcher thread.
- `LoadTaskScheduler` receives commands via a channel.
- Each task is tracked in `running_tasks` (DashMap).
- `flush` and `build` operations are executed asynchronously
  and may block until completion.

### Checkpoint recovery

If `enable_checkpoint` is set:
- Checkpoint files are stored in `data_dir` (or `.` if unset).
- At startup, the manager scans for checkpoint files.
- Temporary checkpoint files are deleted.
- Checkpointed tasks are reconstructed and resumed.
- A cleanup worker runs to GC finished or idle tasks.

Why this design:
- Long ingestion tasks can survive worker restarts.
- The cleanup worker prevents checkpoint buildup.

### Redirect semantics with worker scaler

When worker scaler is enabled:
- A GET for an unknown task may return a 302 with Location
  pointing to a load-data worker pod.
- An init request may also return 302 if a pod is already available
  or if the task should run remotely.

Rationale:
- Clients keep the same HTTP API while workers scale dynamically.

--------------------------------------------------------------------------
## Worker Scaler (Kubernetes)

Worker scaler is responsible for provisioning dedicated
load-data worker pods when tasks are large or when the local
worker is saturated.

It is implemented directly in the worker process
and only used when `worker_scaler.run` is enabled.

### Inputs that influence scaling

- Task `data_size` parameter.
- Current number of local running tasks.
- `spawn_data_size` threshold.
- `spawn_running_tasks` threshold.
- Maximum allowed task size (`max_size`).

### Key responsibilities

- Create a task-specific StatefulSet and Service.
- Wait for the pod to become ready.
- Track task progress for cleanup.
- Delete StatefulSet, Service, and PVCs when tasks finish or expire.

### StatefulSet creation strategy

Worker scaler uses a template StatefulSet:
- The template is fetched by name from the same namespace.
- The template is cloned and adjusted per task.
- Labels are set to allow service selection.
- Liveness probes are disabled to prevent automatic restarts.
- Environment variables are injected to mark the pod as a
  load-data worker and to set its internal worker count.

Resource sizing logic:
- The selected config depends on `data_size`.
- Default configs are merged with user overrides.
- CPU requests are capped by `worker_max_cores`.
- Memory defaults to 4x CPU when not explicitly set.
- Storage defaults to `data_size / worker_num * 3` with a minimum.

Why a StatefulSet:
- It simplifies per-task volume management.
- Each task has stable naming for pods and PVCs.
- It avoids complex Deployment rollouts for ephemeral pods.

### Per-task volume layout

The scaler adds extra PVC templates:
- Each worker instance gets its own subdirectory.
- Volume mounts are constructed using task id and worker id.
- This prevents local IO contention between workers.

### Tracking and cleanup logic

The scaler tracks each task in memory using `WorkerPod`:
- `started_at` is set when the pod becomes Ready.
- `updated_at` is refreshed when progress changes.
- `stopped_at` is set when the task is canceled.
- `cleanup` flag determines whether resources are deleted.

A task is cleaned up when:
- The worker reports no tasks (GCed by worker).
- The task is canceled and expires after a timeout.
- The task is finished and expires after a timeout.
- The task is idle (no progress) beyond a timeout.
- Querying task states fails too many times.

Why these rules exist:
- Pods must survive short gaps in client activity.
- Load-data workers can be slow to update state.
- Cleanup must be aggressive when task status is unknown
  to avoid leaking resources.

### Querying task state

Two modes exist:
- In-cluster: use HTTP to the service address.
- Out-of-cluster: use `kubectl exec` via the API.

Reasoning:
- In-cluster mode is efficient and simple.
- Out-of-cluster mode helps local debugging or admin tools.

### Failure handling

- Kubernetes API errors are logged and retried on the next loop.
- Worker scaler has a hard limit of tasks to avoid runaway creation.
- If a pod never becomes Ready, the task is rejected.

--------------------------------------------------------------------------
## Transaction Chunk Builder

Transaction chunk files are part of file-based transactions.
The worker provides a small API to assemble chunks
and store them in DFS for later commit.

### Design rationale

- Chunk files reduce per-row overhead during commits.
- Clients can parallelize chunk uploads.
- Files are stored in DFS and referenced by id in 2PC.

### Key flow

1. Client sends `/txn_chunk` POST with keyspace id.
2. Worker acquires per-keyspace permit.
3. Worker fetches keyspace shard meta from PD / TiKV.
4. Worker derives encryption key from shard properties (if any).
5. Worker validates checksum and parses entries.
6. Worker builds a `TxnChunk` file with target block size.
7. Worker stores the file to DFS with type `TxnChunk`.
8. Worker returns the chunk id (from PD TSO).

### Why per-keyspace permits

- Prevent a single keyspace from exhausting CPU and memory.
- Protect DFS from overload due to large uploads.

### Encryption key handling

- Key is retrieved from shard properties.
- Master key decrypts the exported key.
- The derived key is cached per keyspace.

Reasoning:
- Avoid repeated PD or TiKV lookups.
- Keep encryption consistent with store metadata.

### Integrity checks

- The request body includes a trailing CRC32 checksum.
- The worker verifies checksum before parsing.
- Checksum mismatch returns `400`.

--------------------------------------------------------------------------
## Native BR (Backup and Restore)

Native BR is a core subsystem for keyspace backup and restore.
It runs inside the worker process and handles:
- Listing incremental backups stored in DFS.
- Triggering keyspace restore (normal or PiTR).
- Tracking restore progress and state.
- Enforcing concurrency and task conflict rules.

### API surface (conceptual)

- `GET /api/v1/backups`: list backups after a timestamp.
- `PUT /api/v1/restore_keyspace/<id>`: start restore.
- `GET /api/v1/restore_keyspace/<id>`: query status.
- `DELETE /api/v1/restore_keyspace/<id>`: delete task.
- `GET /api/v1/restore_keyspace`: list all tasks.

The actual parameters are in code and may evolve.
The key correctness points are in the restore state machine.

### Restore types

- **Normal**: restore from a specific backup file.
- **PiTR**: restore to a point-in-time.

A PiTR request can select:
- An existing incremental backup after the target time.
- An instant backup if no later backup exists.

Why instant backup is always taken:
- PiTR needs a consistent boundary for WAL replay.
- Even normal restore uses instant backup to protect new data.
- This avoids data loss between last backup and restore start.

### Restore task state machine

States:
- Pending
- Init
- Running
- Succeed (final)
- Error (final)

Transitions (simplified):
- Pending -> Init when task is accepted.
- Init -> Running when restore thread starts.
- Running -> Succeed on success.
- Running -> Error on failure.
- Error -> Init is allowed for retry.

State is stored in memory and persisted on disk.
On restart, tasks are reloaded and marked `Error` with `interrupted`.
This prevents silent partial restores.

### Task conflict and keyspace locking

Rules:
- Only one restore per target keyspace at a time.
- Tasks map keyspace -> restore id.
- Conflicts return `409` with the conflicting id.

Why this is required:
- Restores modify keyspace metadata and region layout.
- Concurrent restores could corrupt metadata.

### Task metadata persistence

Each restore task gets a working directory:
- `data_dir/r<restore_id>/meta.json`
- Metadata includes keyspace, restore params, start time.

Persistence details:
- Writes are done via a temp file and `rename`.
- This makes metadata updates atomic on POSIX filesystems.

Why persistence is needed:
- Restore is long and can outlive the process.
- On restart, tasks are marked interrupted and surfaced to clients.

### Restore execution

Main steps (simplified):
1. Register rfengine cache if enabled.
2. Run instant backup.
3. Resolve restore source and truncate timestamp (PiTR).
4. Build restore config and start restore core.
5. Update progress reporter at key steps.
6. Mark task as Succeed or Error.
7. Clean working directory on success.

Progress reporting:
- Each restore step maps to a progress percentage.
- Progress is monotonic by design.
- Steps are exposed via the REST response.

### Concurrency limiting

The worker enforces a global restore concurrency limit:
- Derived from CPU cores and config factor.
- Requests beyond the limit return an error.

Rationale:
- Restore is heavy on CPU and IO.
- Limiting concurrency prevents worker collapse.

### Object cache and throughput limiter

Optional features:
- Object cache for frequently accessed restore objects.
- Throughput limiter for rate-limiting restore IO.

Both are configured via `native_br` config.
They are optional and disabled by default.

### TTL cleanup

- Finished restore tasks are kept in memory for a TTL.
- Background loop deletes expired tasks and working dirs.

Why keep completed tasks:
- Clients may query status after completion.
- TTL provides a reasonable observation window.

### Restore parameter validation

Requests are validated to prevent ambiguous restores:
- Normal restore requires backup id and name consistency.
- PiTR requires a valid timestamp and time gap.
- Mixed normal and PiTR parameters are rejected.

--------------------------------------------------------------------------
## Schema Manager

The schema manager keeps columnar schema files
in sync with keyspaces on TiKV stores.
It runs as a background loop inside the worker.

This is one of the most complex parts of the worker.
The complexity comes from:
- Multiple keyspaces and shards.
- Schema evolution over time.
- Partial failures during sync.
- Keyspace restore scenarios.
- The need to avoid rebuilding schema files unnecessarily.

### Primary responsibilities

- Periodically scan keyspace stats from stores.
- Determine which keyspaces need schema updates.
- Fetch schema diffs from TiKV schema store.
- Build and upload schema files to DFS.
- Broadcast schema file updates to TiKV stores.
- Persist local metadata for recovery and GC.

### Key data structures

**Schema files**:
- Stored in DFS and cached locally per keyspace.
- Contain schema info for tables that require it
  (columnar or storage class changes).

**Meta file** (`schemas.meta`):
- Tracks schema file versions per keyspace.
- Tracks `checked_version` to skip redundant updates.
- Tracks `write_sequence` for small keyspaces.

Why a meta file:
- Avoid scanning local directories on every loop.
- Track progress across worker restarts.
- Maintain ordering and detect version rollback.

### Main loop overview

Each loop iteration:
1. Get stores from PD and optionally filter by tier.
2. Fetch shard stats from all stores.
3. Group shard stats by keyspace id.
4. For each keyspace:
   - Validate whether it should be processed.
   - Read and validate local schema files.
   - Detect restored keyspaces and clean stale files.
   - Fetch schema version from schema store.
   - Decide whether to sync schema changes.
   - Build a new schema file if needed.
   - Upload to DFS and broadcast update.
5. Save meta file updates.
6. Periodically GC old schema files.

### Keyspace validation logic

A keyspace is skipped if:
- It is the default keyspace (not supported by APIv2 client).
- It is in the blacklist file.
- Its total size is zero (empty or tombstone).
- The keyspace restore version is inconsistent across shards.
- A single-shard keyspace has unchanged write sequence.

Why these checks:
- Avoids work for irrelevant keyspaces.
- Prevents schema changes during restore.
- Allows very small keyspaces to avoid repeated scans.

### Local schema file handling

If a local schema file exists:
- Its schema version and restore version are extracted.
- If shard restore version differs, the keyspace
  is considered restored and files are cleared.
- Store shard versions are checked to determine
  if a broadcast is needed even without rebuilding.

Why clear on restore:
- A restore can roll back schema version.
- Old schema files are no longer valid for the new data.

### Schema sync and version checks

- Schema version is fetched from schema store (via txn client).
- If the version matches `checked_version`,
  the keyspace is considered up-to-date.
- If local file is missing, full schema sync is forced.
- `sync_schema` fetches incremental changes when possible.

Why use `checked_version`:
- If a schema file already includes recent changes,
  we do not need to rebuild the file.
- This reduces load on the schema store.

### Building a new schema file

A new schema file is built when:
- Any table has required changes (columnar or storage class).
- An existing schema file needs removals for tables
  that no longer require schema entries.

Steps:
1. Convert each table info to schema entries.
2. Remove tables that no longer require schema entries.
3. Merge with previous schema file contents.
4. Serialize into schema file format.

Key rationale:
- Schema files are incremental but must remain self-contained.
- Removing obsolete tables prevents stale metadata use.

### Upload and broadcast

For each new schema file:
- Allocate a new file id via PD.
- Upload to DFS (type `Schema`).
- Write the file to local disk.
- Broadcast to all stores: `/schema_file?keyspace_id&file_id`.

Why broadcast after upload:
- Stores must switch to the new schema file id.
- Doing it after upload avoids references to missing files.

### Upload concurrency and meta updates

- Uploads are spawned as async tasks.
- Concurrency is limited by `schema_upload_concurrency` via a semaphore.
- Each task sends its result through an mpsc channel.
- The meta file is updated only after all tasks complete.
- `checked_version` is updated on success.
- `write_sequence` is intentionally not updated on upload,
  so a later loop can re-broadcast if stores missed the update.

### Handling schema version rollback

If the new schema version is less than the latest:
- The meta file rejects the update.
- The worker clears local schema files for the keyspace.
- The keyspace will be rebuilt in the next loop.

Why this is safe:
- Rollbacks happen after process restart or restore.
- Clearing local state ensures consistency with stores.

### Write sequence optimization

Small keyspaces with a single shard can avoid
schema syncing if write sequence is unchanged.
The write sequence is stored per keyspace in the meta file.

Rationale:
- Schema changes correlate with write sequence changes.
- Avoids scanning schema store for idle keyspaces.

### Schema file GC

GC runs periodically:
- Keeps a fixed number of versions per keyspace.
- Removes old local schema files.
- Updates meta file accordingly.
 - A value of `0` disables GC and keeps all versions.

Why only local GC:
- DFS objects are managed by external retention policies.
- Local disk is limited and must be cleaned aggressively.

### Meta file repair

On startup, the manager may repair the meta file:
- Scans local schema file directories.
- Validates and re-adds missing file entries.
- Persists repaired meta file.

Rationale:
- Local meta file may be lost or corrupted.
- Repair avoids a full re-sync on restart.

--------------------------------------------------------------------------
## Local GC for IA

Local GC is a small but critical loop
when IA (segment cache) is enabled.

Behavior:
- Runs in a dedicated thread.
- Periodically invokes IA GC runner.
- Stops when `RunningController` is stopped.

Why a dedicated thread:
- IA GC is independent from request handling.
- It should not block the worker runtime.

--------------------------------------------------------------------------
## Replication Worker (CDC)

The worker optionally starts a replication worker.
This is controlled by `config.replication_worker.enabled`.

Integration points:
- The worker starts the replication worker in a thread.
- The scheduler is stored in the server context.
- Requests under `/cdc` are routed to replication worker handler.

Shutdown behavior:
- On shutdown, a `CdcMsg::Stop` is scheduled.
- In force mode, the scheduler is forced to stop.
- The worker thread is joined to avoid leaks.

Why in the worker:
- CDC benefits from proximity to DFS and worker cache.
- It offloads CDC work from TiKV stores.

--------------------------------------------------------------------------
## Background and Maintenance Loops

The worker has several loops:

- Native BR background loop
  - Reloads native BR config from file (if provided).
  - Cleans up expired restore tasks.

- Schema manager loop
  - Refreshes keyspace stats and schema files.
  - Performs periodic GC of schema files.

- Worker scaler loop (optional)
  - Cleans up finished load-data worker pods.
  - Periodically deletes orphan PVCs.

- Memory usage loop
  - Records global memory usage into metrics.

- Prometheus push loop (optional)
  - Pushes select metrics to a pushgateway endpoint.

These loops run independently and do not share a
common scheduler; they use std threads or Tokio tasks.

--------------------------------------------------------------------------
## Data Layout on Disk

`data_dir` is a common root for worker state:

- `native_br/`
  - restore working directories and metadata files.
- `rfengine_cache/` (optional)
  - caching for native BR restore.
- `ia/`
  - IA segments and metadata when IA is enabled.
- Load data checkpoints (if enabled)
  - checkpoint files stored at the data_dir root.

Schema manager uses its own directory:
- `schema_manager.dir` contains:
  - `schemas.meta`
  - `<keyspace_id>/` directories with `.schema` files.

Why these are local:
- They represent transient state and caches.
- They are not authoritative across workers.
- They enable faster recovery and avoid DFS roundtrips.

--------------------------------------------------------------------------
## Security and Networking

The worker relies on `SecurityManager` for:
- TLS settings.
- Building HTTPS URIs for store status endpoints.
- Creating HTTP clients for internal calls.
- Binding gRPC servers with TLS settings.

Key points:
- Many operations depend on store `status_address`.
- Worker must be able to reach those addresses.
- DNS names and service discovery are external concerns.

--------------------------------------------------------------------------
## Observability

The worker exposes multiple signals:

### Metrics

Key metric groups:
- Remote coprocessor durations and sizes.
- Remote compaction durations.
- Native BR durations and counters.
- Worker scaler query failures.
- Schema manager loop counts and errors.
- Memory limiter current usage.

Metrics are served at `/metrics` and can be pushed
to a pushgateway if configured.
The push loop currently pushes `LOAD_DATA_WRU_COST_COUNTER`.

### Logs

Most long-running operations log:
- The target keyspace or request tag.
- The operation type and timing.
- Key config values at startup.

For debugging native BR:
- The `LOG_FILE=/tmp/test.log` pattern can be used
  when running integration tests.

### Profiling

The worker exposes pprof endpoints:
- `/debug/pprof/profile`
- `/debug/pprof/heap`
- `/debug/pprof/symbol`

These are enabled by default in the HTTP server.

--------------------------------------------------------------------------
## Error Handling and Retry Semantics

The worker distinguishes between:
- Retryable errors (return 503) where clients can retry.
- Permanent errors (return 400/500) for invalid inputs.

Examples:
- Permit timeout -> 503 (retryable).
- Memory limit exceeded -> 503 (retryable).
- Invalid request format -> 400 (not retryable).

Internal errors are logged with context
but generally mapped to 500 for clients.

For schema manager and worker scaler:
- Errors are logged and counted in metrics.
- The next loop attempts recovery.

For native BR:
- Errors transition tasks to `Error` state.
- Clients must explicitly retry by re-submitting restore.

--------------------------------------------------------------------------
## Design Notes by Subsystem

This section documents tricky or non-obvious details
that are easy to miss when reading code.

### Worker runtime separation

- `hyper-server` runtime handles request IO.
- `worker-server` runtime handles heavy compute.
- This is critical because many operations read or write DFS.

If you merge the runtimes:
- A slow DFS call can block request processing.
- Latency spikes propagate to all endpoints.

### TxnChunkManager runtime ownership

- `TxnChunkManager` is created with the worker runtime handle.
- This avoids dropping a Tokio runtime inside an async context.
- If you refactor this, preserve the same ownership model.

### Snapshot construction in remote cop

- Snapshot building uses both memtable data and delta logs.
- The memory guard is held for the entire request.
- If you change this behavior, validate memory
  accounting in `kvengine::SnapAccess`.

### Native BR restore metadata

- Meta file writes are atomic via rename.
- A partial write will not be visible after crash.
- On restart, tasks are marked interrupted to
  avoid silent partial restore completions.

### Schema manager version rollback

- If a new schema file has a lower version,
  the manager clears local files to resync.
- This prevents a mismatch between stores and worker.
- Do not ignore this condition; it signals restore or
  a major process restart.

### Worker scaler cleanup safety

- Cleanup waits for pod warm-up to avoid false positives.
- Query failure counters are used to detect dead pods.
- This avoids leaking StatefulSets when pods disappear
  or API calls repeatedly fail.

### Txn chunk encryption

- Encryption keys are retrieved from shard metadata.
- The key is cached per keyspace to avoid repeated calls.
- If you change keyspace metadata, consider cache invalidation.

--------------------------------------------------------------------------
## Operational Guidance

### Startup checklist

- Ensure PD endpoints are configured and reachable.
- Ensure DFS credentials are valid.
- Ensure `data_dir` is writable if used.
- For schema manager, ensure `schema_manager.dir` is writable.
- For worker scaler, ensure Kubernetes RBAC allows
  StatefulSet, Service, and PVC operations.

### Scaling guidance

- Use multiple worker instances for high QPS.
- Increase `thread_pool_size_factor` cautiously;
  DFS and compaction may become the bottleneck.
- Monitor memory limiter metrics when scaling.

### Troubleshooting

Common symptoms and causes:
- **Remote cop 503**: memory or permit limits reached.
- **Compaction 503**: memory limit or vector-index throttle.
- **Load data redirect loops**: worker scaler failure
  or stale task id.
- **Schema manager stuck**: store stats API failing,
  schema manager blacklists, or schema store unavailable.
- **Restore conflict**: concurrent restore for same keyspace.

### Cleaning up leaked resources

- Worker scaler cleanup might miss resources if the worker
  is down for too long.
- Use Kubernetes to delete stale StatefulSets and PVCs
  with the `load-data-worker` label.
- Clean schema manager local dirs if you intentionally
  want to reset its state.

--------------------------------------------------------------------------
## Testing and Validation

The worker is covered by multiple test suites.
Use targeted tests during iteration:

- Remote coprocessor:
  - See `doc/features/remote_coprocessor.md`.
- Schema manager:
  - Unit tests in `schema_manager.rs`.
- Worker scaler:
  - Unit tests in `worker_scaler.rs`.
- Native BR and restore keyspace:
  - Integration tests under `tests/cloud_engine/`.
- Txn chunk:
  - Unit tests in `txn_chunk.rs`.

When adding new functionality:
- Add tests in the subsystem module when possible.
- Prefer integration tests for end-to-end flows.
- Use `tests/cloud_engine` for cloud-specific behavior.

--------------------------------------------------------------------------
## Quick Module Map (for navigation)

- `lib.rs`
  - process startup, config, background loops.
- `server.rs`
  - HTTP routing and request handling.
- `remote_cop.rs`
  - gRPC remote coprocessor server and delegate flow.
- `worker_limiter.rs`
  - concurrency control primitives.
- `worker_scaler.rs`
  - Kubernetes based load-data worker scaling.
- `load_data.rs`
  - load data HTTP API and task scheduling.
- `native_br.rs`
  - backup/restore handlers and restore state machine.
- `schema_manager.rs`
  - schema sync loop and schema file management.
- `txn_chunk.rs`
  - transaction chunk file creation.
- `local_gc.rs`
  - IA segment GC loop.
- `metrics.rs`
  - Prometheus metrics definitions.

--------------------------------------------------------------------------
## Known Tradeoffs and Future Work

These are inherent design tradeoffs or areas to improve.
They help explain current behavior and limits.

- Remote coprocessor uses full request bodies in memory.
  - It simplifies decoding but increases memory pressure.
- Schema manager uses local disk for schema files.
  - It avoids repeated DFS reads but requires GC and repair.
- Worker scaler uses StatefulSet per task.
  - It simplifies volume layout but increases k8s objects.
- Native BR restore is serialized by keyspace.
  - It avoids conflicts but reduces parallelism.
- Load data uses per-task threads.
  - It simplifies task isolation but can be thread heavy.

--------------------------------------------------------------------------
## Glossary

- **DFS**: Distributed file system abstraction backed by S3.
- **IA**: kvengine IA subsystem (segments and GC used by remote cop prefetch).
- **Schema file**: Compact representation of table schemas
  used by columnar reads.
- **Restore working dir**: Local directory per restore task.
- **Worker scaler**: Controller that spawns load-data workers.
- **Txn chunk**: File fragment of a transaction written to DFS.

--------------------------------------------------------------------------
## Maintainer Notes

When touching this component, keep these principles in mind:
- Avoid drive-by refactors that touch multiple subsystems.
- Keep request handlers thin and push heavy work to runtimes.
- Preserve error mappings (500 vs 503) for client behavior.
- Treat the meta file as authoritative for schema sync.
- Keep long loops interruptible via `RunningController`.

If you need to add new subsystems:
- Create a clear boundary in `server.rs`.
- Add config blocks at the end of `Config`.
- Ensure memory and concurrency limits are considered.
- Add metrics for long-running operations.
