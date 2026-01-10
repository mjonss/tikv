# cloud_server Maintainer Guide

This document is for maintainers of the `components/cloud_server` crate.
It focuses on design intent, invariants, and failure behavior.
It intentionally skips routine API details that are easy to read in code.
When you need exact struct fields, read the source under `components/cloud_server/src`.

This guide assumes you already understand TiKV and Raft at a high level.
It explains how cloud_server wires the cloud engine stack together.
The goal is to make it safe to change, debug, and extend this component.

## Quick index
- Scope and mental model
- Component boundaries and responsibility split
- High level architecture diagram
- Project map and reading order
- Threading model and workers
- Startup lifecycle (prepare -> setup -> run)
- Identity, StoreIdent, and bootstrap
- Engine initialization and DFS integration
- Flow control, quotas, and resource control
- RaftKv engine wrapper
- Custom raft log encoding and txn semantics
- Async write and snapshot flows
- Replica read lock check and read index handling
- Raft transport overview
- RaftClient queueing, batching, and backoff
- Address resolver and store cache
- gRPC server lifecycle
- gRPC service patterns (unary, streaming, batch)
- Request batching heuristics
- Import SST service flow
- Backup and diagnostics services
- Status server architecture
- Status endpoints (by family, not exhaustive)
- Status server security gating
- Recovery, blacklist, and panic region handling
- Observability and metrics
- Failure modes and invariants
- Testing and harnesses
- Debugging playbook
- Glossary
- References

## Scope and mental model
cloud_server is the server runtime for the cloud engine variant of TiKV.
It is the glue that wires the storage engine, Raft store, and RPC services.
It owns the startup sequence, gRPC server, status server, and transport.
It does not own Raft state machines or storage engines themselves.

Think of cloud_server as the process coordinator.
It configures and starts rfstore (Raft layer) and kvengine (data layer).
It exposes RPC endpoints used by TiDB, TiKV clients, and internal workers.
It also exposes administrative HTTP endpoints via the status server.

What this guide covers.
- How cloud_server initializes, owns, and shuts down core components.
- How it wires Raft transport, address resolution, and gRPC services.
- How async write, snapshot, and custom raft log flows work together.
- Why specific ordering and gating choices exist in startup and shutdown.

What this guide intentionally skips.
- Exhaustive RPC method lists and request/response structs.
- Routine config fields whose meaning is obvious from code.
- Implementation details inside kvengine, rfengine, and rfstore.

If you need deeper details.
- rfstore behavior: `components/rfstore/README.md`.
- kvengine behavior: `components/kvengine/README.md`.
- rfengine behavior: `components/rfengine/README.md`.
- txn file feature: `doc/features/txn_file.md`.

## Component boundaries and responsibility split
cloud_server sits between external callers and the storage stack.
Its responsibilities are process and network facing.
Its dependencies implement the storage semantics.

Boundary summary.
- rfstore owns Raft groups and apply logic.
- kvengine owns user data and shard metadata.
- rfengine owns Raft log persistence.
- cloud_server owns lifecycle, network, and service wiring.

Why this separation matters.
- It keeps Raft and storage correctness logic in dedicated crates.
- It keeps server orchestration changes localized and testable.
- It allows alternative server front ends without rewriting rfstore.

## High level architecture
cloud_server is the component that binds together IO, Raft, and RPC.
It also maintains a control plane (status server) separate from data plane.

Simplified diagram.
```ascii
+-------------------+        +-------------------+
|   RPC clients     |        |        PD         |
| (TiDB, tools,     |        |   metadata + ts   |
|  tikv-worker)     |        +---------+---------+
+---------+---------+                  ^
          |                            |
          v                            |
+---------+----------------------------+---------+
|                cloud_server                   |
|  - TikvServer lifecycle                        |
|  - gRPC server + services                      |
|  - Status server (HTTP)                        |
|  - Raft transport (RaftClient)                 |
+---------+----------------------------+---------+
          |                            |
          v                            v
+---------+---------+        +---------+---------+
|     rfstore       |        |     kvengine     |
|  Raft groups      |        |  data + shards   |
+---------+---------+        +---------+---------+
          |                            |
          v                            v
       rfengine                    DFS / local
```

Control plane vs data plane.
- Data plane: gRPC Tikv service, Raft transport, storage RPCs.
- Control plane: status server, config management, recovery endpoints.

## Project map and reading order
Start here if you are new to cloud_server.
This order mirrors the startup path and request flow.

Core lifecycle and wiring.
- `components/cloud_server/src/tikv_server.rs`.
- `components/cloud_server/src/setup.rs`.
- `components/cloud_server/src/node.rs`.

RPC and server side surface.
- `components/cloud_server/src/server.rs`.
- `components/cloud_server/src/service/kv.rs`.
- `components/cloud_server/src/service/batch.rs`.
- `components/cloud_server/src/service/sst_service.rs`.
- `components/cloud_server/src/service/diagnostics/mod.rs`.

Raft transport and address resolution.
- `components/cloud_server/src/raft_client.rs`.
- `components/cloud_server/src/transport.rs`.
- `components/cloud_server/src/resolve.rs`.

Engine wrapper and request encoding.
- `components/cloud_server/src/raftkv.rs`.

Status server.
- `components/cloud_server/src/status_server/mod.rs`.
- `components/cloud_server/src/status_server/profile.rs`.
- `components/cloud_server/src/status_server/metrics.rs`.

Entry points.
- `cmd/tikv-server/src/main.rs` uses `cloud_server::TikvServer`.

Related docs worth keeping open.
- `components/rfstore/README.md` for Raft store internals.
- `components/kvengine/README.md` for shard and file layout.
- `doc/features/txn_file.md` for txn file write flow details.

## Threading model and workers
cloud_server uses multiple thread pools and worker types.
These are configured to isolate workloads and avoid starvation.

Thread pools used by cloud_server.
- gRPC worker threads (grpcio env, sized by cpu_cores quota and grpc_concurrency_factor).
- Read pools (unified read pool, tokio or yatp based).
- Debug thread pool (used by diagnostics).
- Status server runtime thread pool (HTTP server).
- Import SST runtime (per importer).
- Background worker (general purpose).

Additional runtime loops.
- stats_pool for gRPC thread load and memory sampling.
- global timer tasks used by load statistics and sampling intervals.

Why stats_pool exists.
- The gRPC server itself should avoid expensive periodic work.
- Load statistics and memory sampling are isolated from request paths.

Worker loops and schedulers.
- PD worker (for PD tasks from rfstore).
- Address resolver worker (resolve store address via PD).
- Resource metering workers (recorder, reporter, single target).
- Overload protector worker.

Read pool selection.
- Unified read pool can run on tokio or yatp based on config.
- The choice is made during init_servers and affects storage reads.

Design rationale.
- Keep admin and debug work off the data plane.
- Avoid blocking gRPC threads with heavy IO.
- Preserve predictable latency under high load.

Key invariants.
- gRPC services are registered before server start.
- Workers that depend on engines are started after engines open.
- Background workers are stopped before engines are closed.

## Startup lifecycle
Startup in cloud_server is deliberate and sequential.
Many components depend on earlier initialization steps.
The startup is split across `prepare`, `setup`, and `run`.

### Phase 1: prepare
`TikvServer::prepare` does early environment and connection setup.
It runs before full config validation and before engines are opened.
Key reasons for this ordering.
- Logging must be configured before anything else can fail.
- PD connection is needed to derive cluster id and feature gate.
- DFS selection determines engine options and encryption setup.

Key steps.
- Initialize logging (`setup::initial_logger`).
- Print build version and resource quota.
- Check critical environment variables and set panic hooks.
- Build `SecurityManager` for TLS and certificate validation.
- Build gRPC environment with thread count from CPU quota.
- Connect to PD and fetch the cluster id.
- Choose DFS implementation based on config.

DFS selection logic (simplified).
- If S3 bucket and endpoint are empty, use builtin DFS.
- If endpoint is `memory`, use in-memory DFS for tests.
- Else use S3 DFS with configured endpoint and credentials.

Important design note.
- DFS choice happens before engines open because it affects engine paths.
- The choice also controls IA defaults and remote compactor behavior.

### Phase 2: setup
`TikvServer::setup` constructs the server object and opens engines.
It does config validation and initializes resource controllers.

Key steps.
- Validate config and persist it (`init_config`).
- Initialize flow control and store limiter.
- Build IO rate limiter and set the IO budget.
- Create master key for encryption from security config.
- Open rfengine and kvengine (see Engine initialization).
- Create the rfstore `RaftBatchSystem` and router.
- Create background workers (worker pool, resolver worker).
- Initialize concurrency manager using PD TSO.
- Create quota limiter and resource controller.
- Start overload protector worker in a background thread.

Design rationale.
- Config validation happens before engine open to prevent wrong path usage.
- Flow control and rate limiting must be ready before storage starts.
- Concurrency manager must use a recent PD TSO for lock correctness.

### Phase 3: run
`TikvServer::run` wires everything and starts serving.
It should be called only after `setup` has completed.

Key steps in run order.
- Register memory usage high water thresholds.
- Check for conflicting addresses in lock dir.
- Acquire file locks and check for panic mark files.
- Initialize Yatp metrics (one time global).
- Build `RaftKv` wrapper and local reader.
- Build servers, storage, and Raft node (`init_servers`).
- Register gRPC services (`register_services`).
- Start metrics flushing background task.
- Bind and start the gRPC server.
- Start status server if enabled.
- Start safe point watchers and metrics push (if configured).
- Start the resource controller.

Design rationale.
- gRPC services must be registered before the server is built and started.
- Status server is intentionally separate from gRPC server for isolation.
- The run order ensures the Raft node is started before exposing RPCs.

### Shutdown
`TikvServer::stop` and `TikvServer::force_stop` drive shutdown.
They ensure that services stop before engines are closed.

Key shutdown steps.
- Stop gRPC server and health service.
- Stop rfstore node and region info accessor.
- Stop lock manager, background workers, and status server.
- Close rfengine writer and stop its worker.
- Stop overload protector and resource controller.
- Release lock files (best effort).

Important invariant.
- Never close engines before stopping components that may access them.

## Config controller and online config
cloud_server uses ConfigController to manage runtime configuration changes.
It is created during init_config and passed to status server and modules.

Registration highlights.
- Quota limiter config manager is registered for dynamic quota updates.
- Resource metering config manager updates recorder and reporter behavior.
- Overload protector config manager updates overload thresholds.
- Resource control config manager updates resource controller policy.
- Backup endpoint config manager allows runtime tuning of backup settings.

Why this matters.
- Cloud workloads require safe online tuning without restarts.
- The status server exposes config updates and validation.

## Identity, StoreIdent, and bootstrap
cloud_server owns store identity and bootstrap decisions.
This logic lives in `node.rs` and is invoked during `init_servers`.

### Store identity
Each store is identified by a store id assigned by PD.
On disk, the store id and cluster id are stored in `StoreIdent`.
When starting, cloud_server checks for an existing StoreIdent.

Key behaviors.
- If the cluster id in StoreIdent mismatches PD, startup fails.
- If StoreIdent is missing, a new store id is allocated.
- The store id is also set on kvengine and rfengine.

### Cluster bootstrap
If the cluster is not bootstrapped, the first store may bootstrap it.
The sequence is explicitly guarded and retried.

Bootstrap flow (simplified).
1. Check if a prepared bootstrap region exists on disk.
2. Ask PD if the cluster is bootstrapped.
3. If not bootstrapped, allocate region id and peer id.
4. Prepare bootstrap state in engines.
5. Try PD bootstrap.
6. Clear prepared state on success or if PD has bootstrapped already.

Why this design.
- Prevents multiple stores bootstrapping simultaneously.
- Allows idempotent restart if the process dies mid bootstrap.
- The prepared bootstrap state survives restarts.

### Store start
Once identity is established, the Raft store is started.
`Node::start` spawns rfstore workers via `RaftBatchSystem`.
It also registers the store to PD and loads existing store list.

Important invariants.
- `Node::start` must run after store id and cluster id are set.
- The router and transports must exist before rfstore starts.

## Server wiring details
`init_servers` is where the core runtime wiring happens.
It builds storage, coprocessor, lock manager, and the gRPC server.

Key wiring steps.
- Create LockManager and register deadlock observer hooks.
- Build unified read pool (tokio or yatp).
- Create debug thread pool shared by diagnostics service.
- Initialize resource metering recorder, reporter, and sink workers.
- Register ConfigController modules for resource metering and overload.
- Create Storage backed by RaftKv and ConcurrencyManager.
- Register ReplicaReadLockChecker in CoprocessorHost.
- Create coprocessor Endpoint and configure remote execution settings.
- Create Server with gRPC environment, transport, and read pool.
- Initialize SstImporter and set compression types for CFs.
- Start Node with transports, store meta, and importer.

Why this structure.
- LockManager and concurrency manager must be in place before storage.
- The coprocessor endpoint needs the read pool and resource control.
- Importer and backup services depend on storage and router.

Resource metering detail.
- Recorder and reporter workers are started and stored for shutdown.
- A single target worker sends reports to a configured receiver.
- ConfigController updates metering behavior online.

Why this matters.
- Metering is used for billing and workload attribution in cloud deployments.
- It must be isolated from latency critical paths.

## Engine initialization and DFS integration
cloud_server opens and configures both rfengine and kvengine.
This is where cloud-specific storage behavior is bound in.

### rfengine
rfengine is opened by `init_raft_engine`.
It is given the raftdb path, data dir, and optional DFS handle.

Why DFS matters for rfengine.
- rfengine can place or access raft WAL chunks on DFS.
- The DFS handle must be available to avoid corrupting WAL layout.

### kvengine
kvengine is opened by `init_kv_engine`.
It needs DFS, multiple local directories, and config options.

Key initialization steps.
- Compute block cache capacity from total memory or config.
- Ensure all local directories exist (main + extra dirs).
- Apply RocksDB-derived settings (memtable, block size, compression).
- Enable flow control, IA, and columnar settings.
- Configure remote compactor addresses and concurrency limits.
- Choose txn file worker pool size based on memory.
- Build the PD id allocator and meta change listener.
- Pass recovery handler and blacklist to kvengine.

Important constraints.
- `max_mem_table_size` is capped to kvengine maximum.
- Flow control thresholds are derived from config and must be consistent.
- Some options are intentionally dynamic (IA capacity is always dynamic).

### DFS selection
DFS selection is done in `prepare`.
DFS controls where data files are stored and how reads are performed.

Paths and semantics.
- builtin DFS uses PD-backed metadata for local operations.
- S3 DFS uses an object store for file persistence.
- In-memory DFS is only for tests and does not persist data.

### IO rate limiter and store limiter
Two layers of IO control are initialized.
- `IoRateLimiter` enforces global bytes per second limit.
- `StoreLimiter` enforces storage flow control (write throttling).

Why two layers.
- The IO rate limiter is a global budget shared across operations.
- The store limiter is a dynamic throttle based on memory usage feedback.

IO metrics flusher.
- IO stats are collected from either the OS collector or the rate limiter.
- A background task periodically flushes IO metrics to Prometheus.

### Kubernetes disk capacity check
On Kubernetes, startup checks disk capacity for the data dir.
If capacity is too low, startup fails fast.

Why this exists.
- Prevents booting a pod before volumes are properly mounted.
- Avoids silent data loss from running on ephemeral filesystem.

### Panic regions and blacklist escalation
cloud_server uses panic region files to build a blacklist.
This is a recovery safety net after repeated panics.

Key inputs.
- Panic region files are discovered in the data directory.
- Region to peer mapping is loaded from rfengine.
- Snapshot metadata gives keyspace and table id.

Escalation logic.
- Count panics per table and keyspace.
- If counts exceed thresholds, add to blacklist.
- Merge with blacklist config files and recovery blacklist.
- Apply whitelist overrides (whitelist wins).

Design rationale.
- Prevents repeatedly crashing regions from being reopened immediately.
- Escalation to table and keyspace avoids many individual region entries.
- The logic is data driven and resilient to partial data loss.

## Flow control, quotas, and resource control
Several subsystems regulate workload and resource usage.
These are initialized in `setup` and registered in config controller.

### Flow control
Flow control uses a `StoreLimiter` and `FlowController`.
It ties memory usage to write throttling.

Key design points.
- The max speed is derived from hard limit and PD heartbeat interval.
- The min speed is clamped to `CLOUD_MIN_THROTTLE_SPEED`.
- Flow control is enabled via storage config.

### Quota limiter
`QuotaLimiter` enforces separate budgets for foreground and background.
Budgets cover CPU time and read and write bandwidth.

Why it exists.
- Cloud deployments separate OLTP traffic from background tasks.
- Ensures background tasks do not starve user traffic.

### Resource controller
`ResourceController` coordinates resource control subscriptions.
It registers subscribers for read and transfer leader tasks.

Where it matters.
- The unified read pool uses read limiter subscriptions.
- Transfer leader operations are throttled to avoid cluster churn.

### Overload protector
`OverloadProtector` is started in a background thread.
It uses config driven thresholds to protect the server under load.
It is also configured for the coprocessor endpoint.

Design note.
- Overload signals are consumed by request scheduling and admission.

### Concurrency manager and lock tracking
ConcurrencyManager is initialized using PD TSO.
It is used by RaftKv, replica read lock checking, and backup flows.

Key usage points.
- Update max ts on read index paths to enforce monotonicity.
- Track backup_ts to coordinate snapshot and backup safety.
- Provide lock checks for replica reads via ReadIndexObserver.

## RaftKv engine wrapper
`RaftKv` is the engine wrapper exposed to the Storage layer.
It bridges storage requests to rfstore via Raft commands.

### Purpose
- Convert Storage write and read calls into Raft commands.
- Provide async write and snapshot APIs for Storage.
- Track write progress and provide proposed and committed events.

### Async write flow
`async_write` is the central write path for KV operations.
It builds a `RaftCmdRequest` and sends it to rfstore.
It returns a stream of `WriteEvent` values.

Key stages.
1. Validate request (non empty writes, failpoints).
2. Convert `WriteData` into a custom raft log request.
3. Build request header and set flags (one_pc, stale read, etc).
4. Send to rfstore with a callback that completes the WriteRes stream.
5. Update metrics based on result and duration.

Why a stream.
- Some callers want proposed and committed events before apply.
- The stream model lets them subscribe to those events efficiently.

Safety handling.
- If the on_applied callback is dropped, the write becomes undetermined.
- The callback is wrapped with `must_call` to enforce completion.

### Async snapshot flow
`async_snapshot` issues a Raft `Snap` command.
It returns a future that resolves to a `RegionSnapshot`.

Key behaviors.
- The request may include key ranges and a read index start_ts.
- Stale reads set a special flag and include start_ts in flag_data.
- The response may contain either a snapshot or an error response.

Why it is async.
- Snapshot acquisition can be slow and should not block threads.
- It also allows cancellation and integrates with async scheduling.

### Custom raft log encoding
`modifies_to_requests` encodes logical txn operations into a custom log.
This is a key cloud engine optimization.

Design goals.
- Shrink Raft log size by storing only essential metadata.
- Avoid repeated value payloads when they can be fetched on apply.
- Preserve transactional semantics and rollback safety.

Key cases (conceptual).
- Prewrite merges default CF values into lock values when possible.
- Commit stores key + commit_ts and applies the value on apply.
- OnePc handles put, del, and lock in a single log entry.
- Rollback and CheckTxnStatus encode rollback semantics compactly.

Why this is complex.
- The same set of Modify operations can represent different txn stages.
- Some cases depend on whether one_pc is set or not.
- Correctness depends on matching the exact txn command type.

Common pitfalls.
- Prewrite must merge default CF values into lock values correctly.
- OnePc must detect deleted locks to preserve semantics.
- Rollback logic must avoid conflicting rollback and commit records.

### Txn file integration
When `WriteData` contains `txn_file`, `modifies_to_requests` stores a ref.
The ref is carried in the custom request to rfstore.
This integrates with the txn file feature described in `doc/features/txn_file.md`.

Design note.
- The custom request carries a `TxnFileRef`, not the file content.
- The actual file data is stored in DFS and loaded by kvengine.

### Replica read lock check
`ReplicaReadLockChecker` is registered as a read index observer.
It checks for lock conflicts during read index processing.

Key behavior.
- It runs only on leaders and only for MsgReadIndex.
- It parses the read index context and checks key ranges.
- It updates ReadIndexContext with lock info if conflicts exist.

Why this matters.
- Replica reads can bypass leader lease checks.
- This provides a safety guard to ensure a lock conflict is visible.

### Write path sequence (simplified)
This is a high level view of a write request through cloud_server.
It omits details inside rfstore and kvengine.

Sequence.
1. gRPC KvService receives a write request.
2. Storage builds WriteData and calls RaftKv::async_write.
3. RaftKv encodes WriteData into a custom raft log request.
4. rfstore proposes the Raft command and replicates it.
5. rfengine persists the log entry.
6. rfstore apply workers apply to kvengine.
7. RaftKv callback completes the WriteEvent stream.

Why the sequence matters.
- The custom log encoding must align with apply logic in rfstore.
- The callback is the only signal that the write is durable and applied.

### Read path sequence (simplified)
This is a high level view of a read request.
It focuses on linearizable reads and replica reads.

Sequence.
1. gRPC KvService receives a read request.
2. Storage chooses a read path and calls RaftKv::async_snapshot.
3. rfstore processes the read index request if needed.
4. ReplicaReadLockChecker injects lock info when leader handles read index.
5. RaftKv returns a RegionSnapshot or error.

Why the sequence matters.
- Read index contexts carry lock info that affects correctness.
- Replica reads must be checked against lock conflicts explicitly.

## Raft transport and address resolution
cloud_server provides rfstore with a transport implementation.
This transport is implemented by `RaftClient` and `ServerTransport`.

### ServerTransport
`ServerTransport` is a thin adapter.
It forwards `send`, `need_flush`, and `flush` to `RaftClient`.
rfstore owns a boxed Transport and calls these methods.

Two transports are created.
- `trans` for normal traffic.
- `trans_idle` for idle or lower priority traffic.

The separation helps rfstore differentiate workloads.
It also provides independent RaftClient instances for scheduling.

### Address resolver
`PdStoreAddrResolver` resolves store ids to addresses using PD.
It runs in a dedicated worker and caches results by TTL.

Key behaviors.
- Uses PD get_store and handles tombstone stores explicitly.
- Caches address for `STORE_ADDRESS_REFRESH_SECONDS`.
- Returns error for empty address (tests may set it empty).

Why caching exists.
- Raft message transport is hot path and must avoid PD hammering.
- Store address changes are infrequent compared to Raft traffic.

Resolver execution model.
- Each resolve call enqueues a Task onto a worker Scheduler.
- The callback is executed on the worker thread and returns a Result.

### RaftClient architecture
RaftClient manages gRPC streams per (store_id, conn_id).
It has a connection pool, a local LRU cache, and a queue per stream.

Key data structures.
- Queue holds outbound RaftMessage instances.
- ConnectionPool tracks active connections and tombstone stores.
- CachedQueue tracks dirtiness and full state for flushing.
- BatchMessageBuffer batches raft messages into BatchRaftMessage.

Why this architecture.
- Raft messages are frequent and must be batched.
- gRPC streams are long lived and need reconnection logic.
- A small cache avoids locking the connection pool on every send.

Connection sharding.
- The target connection id is derived from region id and grpc_raft_conn_num.
- This spreads regions across multiple streams to reduce head of line blocking.

Dynamic config tracking.
- BatchMessageBuffer uses a VersionTrack tracker to refresh limits.
- This allows adjusting batching behavior without restart.

### Connection lifecycle
Connection lifecycle is managed by `start` in `raft_client.rs`.
It is a loop that retries on failure with backoff.

Lifecycle sequence (simplified).
1. Resolve store address (PD resolver).
2. Connect gRPC channel to the address.
3. Start batch_raft stream.
4. If unsupported, fallback to legacy raft stream.
5. Wait until stream ends, then retry.

Failure handling.
- Resolve failure clears pending messages and increments metrics.
- Tombstone errors remove the connection and mark store as tombstoned.
- Connection timeouts broadcast unreachable to rfstore.
- Disconnection triggers backoff and stream rebuild.

Why fallback exists.
- Mixed version clusters may not support batch_raft.
- Fallback keeps compatibility with older TiKV nodes.

Connection config notes.
- ChannelBuilder applies keepalive, compression, and window sizes.
- Reconnect backoff is bounded by raft_client_* config.
- A random channel arg forces new connections on rebuild.

### Queue and flush semantics
`RaftClient::send` enqueues messages into a queue.
It does not send immediately to gRPC.
The caller must call `flush` to notify queues.

Why explicit flush.
- Reduces wakeups and allows higher batching efficiency.
- Lets rfstore send many messages and then flush once.

Queue states.
- Established: accepts messages.
- Paused: rejects messages with DiscardReason::Paused.
- Disconnected: rejects messages and triggers reconnection.

When queue becomes full.
- `send` returns DiscardReason::Full.
- The queue is notified so the sender drains it.
- Metrics are incremented for observability.

### Store allowlist
`RaftClient` can pause traffic to stores not in allowlist.
This is used to limit traffic in specific scenarios.

Behavior summary.
- If allowlist is empty, all stores are allowed.
- If allowlist is non empty, others are paused.
- Paused stores return DiscardReason::Paused on send.

### Raft message send sequence (simplified)
This illustrates the interaction between rfstore and RaftClient.

Sequence.
1. rfstore calls Transport::send for each RaftMessage.
2. RaftClient enqueues into Queue and marks it dirty.
3. rfstore calls Transport::flush after batching.
4. RaftClient notifies queues to wake the AsyncRaftSender.
5. AsyncRaftSender batches and flushes to gRPC stream.

Why this matters.
- Flush boundaries define batching efficiency.
- Queue capacity and pause state determine backpressure behavior.

## gRPC server lifecycle
`Server` encapsulates gRPC server creation and lifecycle.
It owns the gRPC environment, server builder, and load stats.

### Build vs start
`build_and_bind` creates the gRPC server and binds to a port.
`start` starts serving and initializes load tracking.

Why two phases.
- Services must be registered before the server is built.
- Tests may use port 0 and need the resolved address.

Registration behavior.
- register_service returns the service if called after start.
- This prevents late registration from silently being ignored.

### gRPC configuration
Several settings are tuned from config.
Examples include.
- Stream window size and message size limits.
- Memory quota for gRPC internal buffers.
- Keepalive settings and compression level.

Design rationale.
- Cloud workloads can have large messages (SST, backup).
- Keepalive tuning avoids idle connection drops.

### Load and memory metrics
The server tracks load on gRPC threads.
It periodically records thread load and memory usage.

Why this exists.
- Load spikes must be observed early for stability.
- Memory usage is sampled to support alerting and auto-tuning.

### Health service
A gRPC health service is registered and updated.
It reports Serving only after the server is fully started.
This avoids early routing to an unready node.

## gRPC service patterns
The `service` module implements the TiKV gRPC services.
It relies on Storage, coprocessor endpoint, and raft router.

### Proxy forwarding
KvService uses Proxy to optionally forward RPCs.
Forwarding is invoked at the start of each RPC.
It enables flexible routing without changing service code.

Design note.
- Forwarding is a cross cutting concern handled before request work.
- It keeps service logic focused on request execution.

### Unary request pattern
Most KV RPCs use a standard pattern.
- Convert request into a future (for example, future_get).
- Await and send response on sink.
- Record metrics and error counters.

Why this matters.
- Ensures consistent latency and failure metrics.
- Minimizes per method boilerplate using macros.

Thread load tracking.
- KvService receives a ThreadLoadPool used to record gRPC thread load.
- The load signal is used to report system pressure and guide batching.

### Streaming request pattern
Streaming RPCs (coprocessor_stream, batch_commands, raft) use streams.
They use async tasks and channels to decouple request handling.

Design rationale.
- Avoid blocking gRPC threads on heavy processing.
- Provide backpressure via stream consumption.

### Batch commands flow
batch_commands is a duplex stream for heterogeneous requests.
It accepts a stream of requests and returns a stream of responses.

Key behaviors.
- Request handlers are spawned on the tokio runtime.
- Responses are multiplexed by request id.
- A batcher may coalesce get requests into batch_get.

Why this exists.
- Reduces RPC overhead for high throughput workloads.
- Enables mixed request types in a single stream.

### Request batching heuristics
ReqBatcher coalesces get requests under certain conditions.
It is intentionally conservative to avoid latency spikes.

Heuristics used.
- Only batch normal priority GETs.
- Batch size is capped by MAX_BATCH_GET_REQUEST_COUNT.
- Batching depends on pool size and queue size.

Design rationale.
- Batch GET only helps when enough requests are present.
- Over batching can harm tail latency and fairness.

### Coprocessor requests
Coprocessor requests are handled by the Endpoint.
The endpoint can be configured for remote execution.

Why remote coprocessor.
- Cloud deployments may offload heavy scans to remote workers.
- The endpoint supports remote URLs and minimum thresholds.

### Raft and batch_raft RPCs
KvService handles both raft and batch_raft streams.
These are used by RaftClient and other nodes for replication.

Key points.
- batch_raft accepts BatchRaftMessage and splits them.
- Both streams forward messages to rfstore router.
- Observability counters track message counts and failures.

### Service registration order
Service registration happens in register_services.
The order matters because some services depend on others being ready.

Key registrations.
- ImportSST service and its runtime handle.
- Deadlock service from LockManager.
- Backup service and backup endpoint worker.
- Diagnostics service using the debug thread pool.

LockManager start.
- LockManager is started after gRPC service registration.
- It uses PD, resolver, and security manager for lock role changes.

Why the ordering.
- Deadlock service depends on LockManager availability.
- LockManager callbacks may rely on gRPC server readiness.

## Import SST service
ImportSstService handles SST ingestion into rfstore.
It uses a dedicated tokio runtime for importer tasks.

Key stages.
- Create a snapshot for the target region.
- Acquire a per SST lock to avoid duplicate ingest.
- Download or receive SST data and write to importer path.
- Send raft ingest command to rfstore.
- Release lock and report result.

Design reasons.
- Snapshot ensures the region is at a consistent term and epoch.
- Locks prevent two concurrent ingests for the same SST.
- Dedicated runtime isolates IO heavy ingest from gRPC threads.

Additional notes.
- Import threads use IO type tagging to isolate IO metrics.
- A periodic tick shrinks importer state to control memory usage.

## Backup and diagnostics services
cloud_server registers several additional gRPC services.
These are started during register_services.

### Backup service
Backup is handled by the backup crate.
It uses a background worker and a dedicated endpoint.

Key points.
- The endpoint uses RaftKv and region info accessor.
- Config is registered for online updates.
- The service is registered before server start.

### Diagnostics service
Diagnostics service exposes log search and server info.
It uses the debug thread pool and tokio runtime handle.

Why it exists.
- Helps operators inspect logs without SSH access.
- Provides hardware and load info for debugging.

## Status server architecture
The status server is an HTTP server for admin endpoints.
It is separate from the gRPC server by design.

### Runtime and threading
The status server creates its own tokio runtime.
It runs on a dedicated thread pool with a configurable size.
Requests are handled by hyper using this runtime.

Design rationale.
- Separates admin workload from user RPC workload.
- Allows blocking or heavy operations without gRPC impact.

### TLS and security
The status server accepts either plain or TLS connections.
It uses the PD security manager to build the acceptor.

Certificate gating.
- Most endpoints require a valid peer cert by CN allowlist.
- Some GET endpoints are allowed without cert.

Allowlist behavior.
- If cert_allowed_cn is empty, the check is skipped.
- If present, common name must match allowed patterns.

Endpoints allowed without cert.
- /metrics
- /status
- /config (GET)
- /debug/pprof/profile

Why gating exists.
- Status endpoints can mutate config or expose key ranges.
- Certificate checks prevent unintended access.

### Request dispatch
Requests are matched by method and path.
The handler uses a shared StatusContext with key dependencies.

Shared context includes.
- ConfigController for online config updates.
- RaftRouter to send administrative raft messages.
- kvengine and rfengine handles.
- ConcurrencyManager for backup and safe point logic.
- Store info cache for PD stats.

Shutdown behavior.
- Status server runs in a dedicated thread that listens for close_rx.
- Shutdown requests stop serving without killing the runtime abruptly.

### Endpoint families (high level)
The list below is not exhaustive.
It groups endpoints by behavior and purpose.

Observability and debugging.
- Metrics export (/metrics).
- PProf endpoints (/debug/pprof/*).
- Log level and config changes (/config).
- Failpoints (when enabled).

Cluster and region operations.
- Region sync and region info (/region/*).
- Unsafe recovery and split and merge support.
- Major compaction and flush control.

Storage engine operations.
- rfengine backup and WAL progress tracking.
- Restore shard (ChangeSet based).
- DFS file read and write helpers.
- Columnar build and cleanup.

Backup and WAL tracking notes.
- rfengine backup validates cluster id from StoreIdent.
- Optional backup_ts waits are coordinated via ConcurrencyManager.
- WAL progress is returned asynchronously via rfengine callbacks.

Recovery and blacklist.
- Recovery mode management (/recovery/*).
- Whitelist and blacklist updates and queries.

Design note.
- Many endpoints are intentionally rate limited or guarded.
- Expensive operations run on blocking threads or via worker tasks.

### Concurrency control in status server
Some endpoints limit concurrency explicitly.
The DFS file read path uses a semaphore to bound concurrency.

Why.
- DFS file reads can be large and block the runtime.
- Limiting concurrency avoids memory spikes and IO starvation.
- DFS reads are executed via spawn_blocking to avoid reactor stalls.

### Store info cache
Store stats are cached with a TTL.
The cache is per store id and protected by an async mutex.

Behavior summary.
- Fetch fresh stats if cache is older than TTL.
- If PD fails, return stale cache until max TTL.
- After max TTL, propagate PD error to the caller.

Why this design.
- Avoids PD hot loops under many status queries.
- Still allows stale information for best effort operations.

### Status request flow (simplified)
This is the typical control plane request flow.

Sequence.
1. Hyper accepts connection with or without TLS.
2. The service_fn builds a handler with StatusContext.
3. Certificate gating checks cert_allowed_cn when required.
4. Path and method dispatch select a handler.
5. Handler runs, often via spawn_blocking for heavy IO.
6. Response is returned and latency is recorded in histogram.

## Recovery, blacklist, and panic region handling
cloud_server uses multiple safeguards for unstable regions.
These safeguards are tied to the startup sequence.

### Panic region files
Panic region files are created on repeated failures.
Startup collects them and uses the counts to derive blacklists.

Why.
- Repeated panics often indicate corrupt data or inconsistent state.
- Blacklisting prevents the region from crashing the node repeatedly.

### Blacklist sources
Blacklists come from multiple sources.
- Static blacklist file (black_list_path).
- Recovery mode blacklist files.
- Auto blacklist derived from panic regions.
- Additional keyspace files in data dir.

Whitelist override.
- Whitelist file can remove keyspaces from blacklist.
- This allows targeted recovery and exception handling.

### Recovery mode
Recovery mode is toggled via config and status server.
It enables recovery oriented behavior in kvengine.

Design note.
- Recovery mode is intentionally explicit and opt in.
- Use it cautiously and document in runbooks.

## Observability and metrics
Observability is built into each layer.
cloud_server emits metrics and logs for critical paths.

Key metric groups.
- gRPC request latency and failure counts.
- Raft message batching and flush counters.
- Async write and snapshot counters.
- Status server request histograms.
- Memory usage and load statistics.

Where metrics come from.
- server.rs records gRPC thread load and memory usage.
- raftkv.rs records async write and snapshot metrics.
- raft_client.rs records flush and error counters.
- status_server/metrics.rs records HTTP endpoint latency.

Startup info metrics.
- SERVER_INFO_GAUGE_VEC records build version and startup timestamp.
- MEMORY_USAGE_GAUGE is updated on a periodic timer.

Logging practices.
- Startup logs record key configuration and addresses.
- Errors include store id, region id, and addresses where possible.
- Slow status requests are logged with path and elapsed time.

## Failure modes and invariants
This section lists failure modes that influence design choices.
Understanding them is key to safe changes.

### Startup invariants
- Config must be validated before engine open.
- build_and_bind must run before Server::start.
- Services must be registered before the gRPC server is built.
- Store id must be set before rfstore is started.

### Async write failure modes
- Dropped on_applied callback yields undetermined error.
- Empty write requests are rejected early.
- Early error injection is supported via failpoints.

### Transport failure modes
- Address resolve failures clear pending messages.
- Tombstone store resolution removes connection and pauses traffic.
- Queue full returns DiscardReason::Full and increments metrics.
- Paused stores return DiscardReason::Paused and do not reconnect.

### Status server failure modes
- Missing TLS or invalid cert returns 403 for sensitive endpoints.
- DFS file reads can fail with too many concurrent requests.
- PD stats failures return stale data until max TTL expires.

### Storage safety invariants
- Panic mark file indicates prior panic and blocks startup.
- LOCK file ensures only one process uses a data directory.
- Cluster id mismatch prevents accidental cross-cluster join.

## Testing and harnesses
Several crates and tests exercise cloud_server behavior.
Use them when making changes to the server or transport layers.

Local integration.
- components/test_cloud_server provides a test harness.
- It wraps cloud_server and offers cluster builders.

Integration suites.
- tests/cloud_engine exercises the storage engine.
- tests/cloud_engine_failpoints cover failpoint scenarios.
- tests/random uses test_cloud_server for randomized testing.

Why these matter.
- Many bugs only appear under multi node or failpoint scenarios.
- The harness simulates PD, store bootstrap, and raft traffic.

## Debugging playbook
This is a lightweight guide to common issues.
Keep it updated as new failure modes appear.

### Startup failures
If startup fails early.
- Check logs for config validation errors.
- Verify cluster id and StoreIdent match PD.
- Check data directory LOCK file and panic mark.
- Confirm disk capacity if running on Kubernetes.

### RPC issues
If client requests fail.
- Check gRPC server health status.
- Inspect gRPC request metrics and failure counters.
- Verify proxy forwarding rules if configured.

### Raft transport issues
If raft traffic stalls.
- Check resolve failures and unreachable logs.
- Inspect RAFT_MESSAGE_FLUSH_COUNTER for high full counts.
- Verify store allowlist settings.
- Check if any store is marked tombstone.

### Status server issues
If status endpoints fail.
- Verify TLS configuration and cert CN allowlist.
- Check for 403 responses due to cert gating.
- Review status server logs for slow requests.

### Backup and restore issues
For backup.
- Confirm cluster id match in backup config.
- Ensure concurrency manager backup ts is advanced.
- Check if lightweight backup is enabled when requested.

For restore.
- Validate ChangeSet input and shard range coverage.
- Check for unsafe recovery restrictions.

## Glossary
- cloud_server: this crate, process coordinator for cloud engine TiKV.
- rfstore: Raft store implementation for cloud engine.
- rfengine: Raft log and state persistence engine.
- kvengine: data engine storing shards and tables.
- DFS: distributed file system, builtin or S3.
- RaftKv: engine wrapper converting storage ops to Raft commands.
- Status server: HTTP server exposing admin and debug endpoints.

## References
- `components/cloud_server/src/tikv_server.rs` for lifecycle.
- `components/cloud_server/src/raftkv.rs` for custom raft logs.
- `components/cloud_server/src/raft_client.rs` for transport logic.
- `components/cloud_server/src/service/kv.rs` for gRPC service patterns.
- `components/cloud_server/src/status_server/mod.rs` for HTTP admin logic.
- `components/rfstore/README.md` for Raft store internals.
- `doc/features/txn_file.md` for txn file feature flow.
