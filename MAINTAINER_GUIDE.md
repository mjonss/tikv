# TiKV Cloud Engine Maintainer Architecture Guide

This guide is a maintainer-oriented architecture document for the cloud engine
stack in this repository.
It integrates the core component MAINTAINER_GUIDEs into a single mental model.
It focuses on complex cross-component logic and the reasons behind it.
It intentionally avoids trivial API listings or fields that are obvious in code.

Core components covered here:
- cloud_server
- cloud_worker
- rfstore
- rfengine
- kvengine
- native_br
- replication_worker
- merged_engine

This guide is accurate to the current code, but not self-updating.
When behavior changes, update this guide and the component MAINTAINER_GUIDE together.
For deep details on a single component, prefer its MAINTAINER_GUIDE under components/.

## Contents
- 1. Purpose and scope
- 2. System context and external dependencies
- 3. Core components and boundaries
- 4. Architectural views
- 5. Process lifecycle and startup sequencing
- 6. Data model and metadata contracts
- 7. Write path and Raft pipeline
- 8. Read path and coprocessor execution
- 9. Background IO, ChangeSets, and determinism
- 10. Snapshot, split/merge, and restore semantics
- 11. Storage engines deep dive
- 12. Cloud worker subsystems
- 13. Native backup and restore (native_br)
- 14. Replication worker and CDC architecture
- 15. Cross-component invariants and ordering rules
- 16. Failure modes and recovery playbook
- 17. Observability and operational signals
- 18. Change management guidance
- 19. Reading map and companion docs
- 20. Glossary

## 1. Purpose and scope

This guide exists to help new maintainers change the cloud engine stack safely.
It is intentionally opinionated about invariants, ordering, and failure modes.
It is not a replacement for component MAINTAINER_GUIDEs or source-level investigation.

Goals:
- Provide an end-to-end architectural model that links the core components.
- Explain why key design choices were made and which invariants they protect.
- Highlight where correctness or performance can be accidentally broken.
- Provide a single place to orient before diving into component details.

Non-goals:
- Reproducing every config field, API surface, or struct layout.
- Describing the full TiKV classic engine stack (RocksDB based).
- Explaining TiDB or PD internals beyond their impact on these components.

## 2. System context and external dependencies

The cloud engine stack is not a standalone island.
It depends on the following external systems and protocols.
Each dependency affects design choices inside the core components.

Placement Driver (PD):
- Provides cluster identity, store list, region metadata, and TSO.
- Drives splits, merges, and placement decisions used by rfstore.
- Hosts keyspace metadata used by native_br and replication_worker.

etcd (via PD):
- Stores keyspace metadata that native_br backs up and restores.
- Is treated as raw key-value data to avoid tight coupling to PD versions.

TiDB and TiKV clients:
- Drive gRPC requests through cloud_server.
- Expect standard TiKV semantics and error handling.

TiCDC:
- Connects to replication_worker, not to upstream TiKV stores.
- Expects TiCDC ordering semantics, resolved-ts behavior, and error flows.

Object storage (DFS / S3):
- Stores rfengine WAL chunks and snapshots when enabled.
- Stores native_br backup metadata, WAL data, and archives.
- Stores kvengine schema files and related artifacts (columnar paths).

Kubernetes (optional):
- Shapes replication_worker and cloud_worker keyspace service behavior.
- Provides templated addresses for keyspace PD and TiCDC endpoints.

Local disks:
- Hold rfengine WAL and rlog files.
- Cache kvengine data and indexes when DFS is remote.

## 3. Core components and boundaries

This section summarizes each component and how it fits into the system.
The emphasis is on boundary lines and why they matter to correctness.

### cloud_server

Role:
- Process-level orchestrator for the cloud engine server.
- Owns gRPC data plane, status server control plane, and Raft transport.
- Wires rfstore, rfengine, and kvengine together into a running node.

Why this separation exists:
- Keeps storage correctness in rfstore and engines, not in the server front end.
- Allows independent evolution of transport and service wiring.
- Provides a clean process boundary for testing and recovery behavior.

Key interactions:
- Starts rfstore with configured engines and transport.
- Hosts gRPC Tikv services, batch services, and SST import endpoints.
- Runs a status server for health and diagnostics.

### rfstore

Role:
- Raft layer for the cloud engine stack.
- Owns Raft groups, proposal lifecycle, and application to kvengine.
- Maps each Raft region to exactly one kvengine shard.

Why this separation exists:
- Raft correctness is complex and needs dedicated, testable logic.
- A clean Raft layer allows alternative server front ends.
- It localizes consensus and metadata invariants to a single crate.

Key interactions:
- Persists Raft log and state to rfengine.
- Applies committed entries to kvengine in deterministic order.
- Interacts with PD for heartbeats, splits, and statistics.

### rfengine

Role:
- Persistent storage engine for Raft logs and per-peer state.
- Provides WAL, compaction, and manifest-based recovery.
- Optionally ships WAL chunks and snapshots to object storage.

Why this separation exists:
- Raft logs have different durability and access patterns than user data.
- A dedicated engine allows custom encoding, compaction, and remote shipping.
- It enables lightweight backup and CDC without involving kvengine directly.

Key interactions:
- Receives write batches from rfstore IO workers.
- Exposes snapshot and WAL chunk interfaces for native_br and replication.
- Provides dependents tracking used by split/merge safety.

### kvengine

Role:
- State machine for committed Raft commands.
- Owns LSM trees, metadata ChangeSets, and background IO.
- Provides row store, columnar store, vector index, and FTS pipelines.

Why this separation exists:
- Deterministic application of metadata changes is required for correctness.
- Background IO must be structured to produce portable ChangeSets.
- DFS and caching considerations are specific to data storage, not Raft.

Key interactions:
- Applies committed writes and ChangeSets from rfstore.
- Produces ChangeSets for flush/compaction to be replicated by Raft.
- Supports remote compaction and schema file propagation via cloud_worker.

### cloud_worker

Role:
- Sidecar process that offloads heavy or specialized tasks from TiKV.
- Hosts remote coprocessor, load data, native BR, schema manager, and more.
- Provides HTTP and optional gRPC surfaces for these services.

Why this separation exists:
- Avoids dragging heavy dependencies into the core TiKV server.
- Keeps long-running tasks from blocking data plane request handling.
- Allows independent scaling of compute-heavy features.

Key interactions:
- Talks to PD, TiKV stores, and object storage.
- Manages caches, limiters, and background loops for offloaded work.
- Hosts replication_worker and native_br subsystems when enabled.

### native_br

Role:
- Native backup and restore orchestration for the cloud engine stack.
- Coordinates PD/etcd metadata, rfengine data, and keyspace restoration.

Why this separation exists:
- Backup/restore is operationally sensitive and needs explicit guardrails.
- It spans multiple systems and storage layers.
- It requires precise time and range handling that is easy to get wrong.

Key interactions:
- Triggers store-side rfengine backups via HTTP endpoints.
- Writes backup metadata and WAL chunks to object storage.
- Drives restore workflows, including keyspace-level restore.

### replication_worker

Role:
- CDC-compatible capture implemented as a standalone worker process.
- Reconstructs committed state by replaying WAL into a synthetic store.
- Exposes TiCDC gRPC and proxies HTTP API requests.

Why this separation exists:
- CDC logic is complex and benefits from isolation from the TiKV server.
- It reuses engine components without running real Raft.
- It needs custom ordering guarantees and safe point management.

Key interactions:
- Owns a merged_engine instance for WAL replay.
- Communicates with keyspace PD and TiCDC endpoints.
- Manages per-changefeed safepoints and resolved-ts emission.

### merged_engine

Role:
- Synthetic store that merges WAL from multiple upstream stores.
- Uses rfengine and kvengine without participating in Raft itself.

Why this separation exists:
- CDC requires a unified view of committed data across multiple stores.
- Running a new Raft cluster would be expensive and unnecessary.
- The merge heuristic provides a practical tradeoff between accuracy and cost.

Key interactions:
- Ingests WAL chunks from rfengine endpoints or object storage.
- Applies committed entries to kvengine using rfstore apply logic.
- Persists progress and uncommitted entries in a manifest for recovery.

## 4. Architectural views

This section provides a high-level picture of how the components fit together.
Diagrams are simplified; details are in later sections and component MAINTAINER_GUIDEs.

### 4.1 Data plane view (server-side)

```ascii
+------------------+              +------------------+
|   Clients        |              |        PD        |
| (TiDB, tools)    |              |  metadata + TSO  |
+---------+--------+              +---------+--------+
          |                                 ^
          v                                 |
+---------+---------------------------------+---------+
|                cloud_server                        |
|  gRPC services, Raft transport, status server       |
+---------+---------------------------------+---------+
          |                                 |
          v                                 v
+---------+---------+               +-------+---------+
|      rfstore      |               |     kvengine    |
| Raft groups, apply|               | LSM + ChangeSets|
+---------+---------+               +-------+---------+
          |
          v
      rfengine
```

This view highlights the core data path.
cloud_server is the process front end, rfstore owns consensus,
rfengine stores logs, and kvengine stores user data.

### 4.2 Backup, restore, and CDC view (sidecar)

```ascii
+------------------+       +------------------------+
|   TiKV stores    |       |   Object storage (S3)  |
| rfengine/kvengine|       | WAL chunks + snapshots |
+---------+--------+       +-----------+------------+
          |                            ^
          | HTTP backup / WAL chunks   |
          v                            |
+---------+------------------------------------------+
|             cloud_worker (tikv-worker)            |
|  native_br  |  replication_worker  |  load data    |
+---------+------------------------------------------+
          |                            ^
          | gRPC / HTTP                |
          v                            |
+---------+--------+       +-----------+------------+
|     TiCDC       |       |     PD / etcd           |
| (changefeeds)   |       | keyspace meta / GC safe |
+-----------------+       +-------------------------+
```

This view highlights how sidecar processes interact with stores,
object storage, and external control planes.

## 5. Process lifecycle and startup sequencing

Startup ordering is a correctness and operability concern.
Many bugs come from moving work earlier or later in the lifecycle.

### 5.1 cloud_server lifecycle

High-level sequence (simplified):
1) Parse config and create server context.
2) Initialize engines (kvengine and rfengine) and supporting IO.
3) Build rfstore, routers, and worker systems.
4) Register gRPC services and create the gRPC server.
5) Start background workers and stats loops.
6) Start gRPC server and status server.
7) Enter main run loop and wait for shutdown.

Why the ordering matters:
- gRPC services must be registered before server start to avoid missing RPCs.
- Workers that depend on engines must not start before engine open succeeds.
- Status server must not expose endpoints before initialization completes.

Shutdown principles:
- Stop background workers before closing engines.
- Stop transport and gRPC servers before tearing down rfstore.
- Keep ordering explicit to avoid use-after-close on engine handles.

### 5.2 cloud_worker lifecycle

cloud_worker intentionally uses a split runtime model.
It builds separate Tokio runtimes for heavy work and HTTP serving.
This prevents request parsing from being blocked by long-running tasks.

High-level sequence (simplified):
1) Build worker-server runtime for heavy tasks.
2) Build hyper-server runtime for HTTP handling.
3) Initialize DFS client and local cache directories.
4) Initialize security manager and master key.
5) Create background managers (load data, native BR, txn chunk, IA GC).
6) Create worker limiter and memory limiter.
7) Optionally start replication_worker.
8) Build server context and start HTTP and gRPC endpoints.
9) Start schema manager and remote compaction registration.

Shutdown principles:
- Use a central controller to stop background loops.
- Stop replication_worker and join its threads cleanly.
- Close runtimes after pending tasks finish or time out.

## 6. Data model and metadata contracts

The cloud engine stack relies on a small set of shared data concepts.
Most cross-component invariants reduce to consistency of these concepts.

Key concepts:
- Region: PD-level metadata for a key range and Raft group.
- Shard: kvengine unit of storage, mapped 1:1 with a region.
- Peer: local replica of a region in rfstore.
- ChangeSet: deterministic metadata update applied across replicas.
- WAL epoch: rfengine rotation unit for durability and compaction.
- Keyspace: logical key prefix that isolates ranges and metadata.

Important mapping invariants:
- A region maps to exactly one shard, and vice versa.
- Peer storage uses rfengine for Raft state and kvengine for user data.
- ChangeSets must apply in the same order on all replicas.
- Keyspace IDs are part of the metadata state used by backup and replication.

Sequencing model inside kvengine:
- write_sequence tracks applied Raft index for WriteBatch data.
- meta_seq tracks applied Raft index for ChangeSets.
- meta_seq can lag write_sequence because ChangeSets are applied by workers.
- base_version and SnapVersion prevent version regressions during split/merge.
- Apply may pause when pending ChangeSets would make metadata inconsistent.

Why these contracts exist:
- They allow deterministic metadata replay across replicas.
- They allow rfengine and kvengine to evolve without cross-contamination.
- They enable safe backup, restore, and CDC over multiple stores.

## 7. Write path and Raft pipeline

The write path is where most cross-component correctness guarantees live.
The sequence below is simplified but captures the critical ordering points.

### 7.1 End-to-end write path

1) Client sends a gRPC request to cloud_server.
2) cloud_server validates headers and routes to the rfstore router.
3) rfstore checks epoch and peer identity before proposing.
4) Peer FSM builds a Raft proposal and submits it to raft-rs.
5) Raft ready is handled by the Raft worker.
6) IO worker persists write batches to rfengine WAL.
7) rfstore notifies peers of persistence and advances apply pipeline.
8) Apply workers apply committed entries to kvengine.
9) kvengine updates memtables and metadata in deterministic order.

Why the IO worker is separate:
- Raft ready handling must not block on disk IO.
- Batching allows large write amplification reductions.
- Persisted callbacks preserve the invariant that apply follows durability.

### 7.2 Preprocess and apply ordering

rfstore introduces a preprocess stage before apply.
The preprocess stage updates metadata needed for apply correctness.
This is especially important when apply workers are slow or lagging.

Why preprocess exists:
- Metadata used for routing and validation must stay ahead of apply state.
- Split and merge decisions depend on region metadata staying monotonic.
- It avoids applying a ChangeSet to an unexpected shard state.

### 7.3 ChangeSets and deterministic metadata

kvengine cannot perform background IO unilaterally.
Every flush, compaction, or conversion that changes metadata must be packaged
as a ChangeSet and replicated through Raft.

Why ChangeSets are required:
- Background IO results must be consistent across replicas.
- Replicas may flush or compact at different times or not at all.
- ChangeSets ensure all replicas converge on the same file layout.

### 7.4 Custom Raft log encoding

rfstore uses a custom log encoding to reduce overhead and preserve ordering.
The encoding is optimized for rfengine write batches and deterministic replay.

Why this design exists:
- It avoids protobuf overhead in the hot path.
- It keeps raft log structure stable across versions of dependencies.
- It provides a predictable mapping between log entries and rfengine batches.

## 8. Read path and coprocessor execution

The read path balances latency and consistency with multiple mechanisms.
This section focuses on the non-obvious ordering and safety points.

### 8.1 Local read vs read index

rfstore supports leader lease reads when the lease is valid.
If the lease is invalid or uncertain, a read index request is used.
The read index flow ensures linearizability by consulting Raft quorum.

Why two paths exist:
- Leader lease reads avoid Raft round-trips for low latency.
- Read index provides safety when lease confidence is low.
- The combination offers high performance without sacrificing correctness.

### 8.2 Read pools and isolation

cloud_server isolates read execution through a dedicated read pool.
The read pool can be backed by a Tokio or Yatp runtime.
This avoids blocking gRPC threads with storage IO.

Why this matters:
- Read amplification and scan workloads can stall if not isolated.
- The read pool improves tail latency under load.

### 8.3 Remote coprocessor execution

cloud_worker can execute coprocessor requests remotely.
It provides an HTTP path and an optional gRPC delegate path.

Why remote cop exists:
- It isolates heavy compute from core TiKV server threads.
- It allows scaling compute independently from storage capacity.
- It can be deployed where dependencies and caches are better suited.

## 9. Background IO, ChangeSets, and determinism

Most performance-critical work happens in background IO.
The key design constraint is determinism across replicas.

### 9.1 Flush and compaction pipeline

kvengine flushes memtables into L0 tables and compacts across levels.
These operations produce ChangeSets that describe the metadata changes.

Why this design exists:
- Different replicas may flush at different times.
- Compaction decisions can differ by local resource pressure.
- ChangeSets make the outcome deterministic and replayable.

### 9.2 Remote compaction

Remote compaction is initiated by stores but executed by cloud_worker.
The worker uses DFS and local caches to produce compaction outputs.
The resulting ChangeSet is replicated through Raft for all replicas.

Why remote compaction exists:
- It offloads heavy IO and CPU from the storage node.
- It allows tighter control over resource usage and scheduling.
- It makes compaction output portable across replicas.

### 9.3 Storage class and infrequent-access (IA)

kvengine supports storage classes and an IA tier.
cloud_worker runs local GC for IA segments when enabled.

Why IA exists:
- It reduces cost by migrating cold data to cheaper storage.
- It allows policy-based control over storage tiers.
- It requires careful coordination to avoid breaking key ranges.

### 9.4 Schema files and columnar metadata

Columnar data is managed through schema files stored in DFS.
Schema manager in cloud_worker coordinates schema propagation.

Why schema manager exists:
- Columnar readers need consistent schema metadata across nodes.
- Schema files are not easily reconstructed from local state.
- Coordinating through the worker simplifies deployment and caching.

## 10. Snapshot, split/merge, and restore semantics

Snapshots and topology changes span rfstore, rfengine, and kvengine.
The ordering of metadata updates is critical for correctness.

### 10.1 Snapshot creation and application

rfstore snapshots embed kvengine ChangeSet data.
rfengine snapshots provide Raft log state for recovery and backup.

Why this matters:
- Snapshot data must align with the Raft state and shard metadata.
- Applying snapshots out of order can corrupt shard layout.

### 10.2 Split and merge safety

Splits and merges are coordinated through Raft proposals.
rfstore tracks dependents to prevent unsafe truncation or destruction.

Why dependents are necessary:
- After a split, the parent log must be retained until child data is safe.
- Merges require coordinating state across regions to avoid data loss.

### 10.3 Restore interactions

Restore flows rehydrate rfengine state and kvengine shards.
Keyspace restores align backup shards with live region topology.

Why alignment is required:
- Keyspace ranges use prefixes that change key ordering.
- Restored shards must match current region boundaries to avoid gaps.

## 11. Storage engines deep dive

This section describes the design of each engine component.
It emphasizes why the structure looks the way it does.

### 11.1 rfengine

Mental model:
- In-memory per-peer state is authoritative after recovery.
- WAL stores write batches in epoch-rotated files.
- WAL is compacted into immutable rlog files and a manifest.
- Optional DFS worker uploads WAL chunks and snapshots to object storage.

Key design choices:
- Separate apply and persist stages allow async IO without losing ordering.
- Epoch rotation is bounded by compaction lag to avoid overwrites.
- Manifest change sets preserve recoverability across compaction cycles.

Durability tiers:
- Sync WAL is authoritative and may live in wal_sync_dir when configured.
- Async WAL writes feed the DFS worker and can be rebuilt from sync WAL.
- Double writing (wal_secondary_dir) provides a second sync WAL location.

Why WAL plus rlog:
- WAL is optimized for sequential writes and short retention.
- rlog files provide compact, long-term storage per peer.
- The manifest ties rlog files together in a consistent timeline.

Remote durability and backup:
- WAL chunks are uploaded to object storage in epoch slices.
- Snapshots are only marked complete after WAL uploads finish.
- Snapshot rlog objects are uploaded last to prevent partial visibility.

Failure behavior highlights:
- The sync WAL is authoritative if async WAL is corrupted.
- Corruption in a non-last WAL file is fatal to protect correctness.
- Compaction lag triggers throttling to avoid WAL overwrite.

### 11.2 kvengine

Mental model:
- Each shard owns an LSM tree for a specific key range.
- Metadata changes are applied via ChangeSets in Raft order.
- Background IO is the primary producer of ChangeSets.

Key design choices:
- ChangeSets make background work deterministic across replicas.
- Shard versions and sequences prevent stale metadata from applying.
- SnapVersion pairs (base_version, data_sequence) track snapshot safety.

Row store and columnar store:
- Row store is the default MVCC path.
- Columnar data relies on schema files stored in DFS.
- Columnar and row paths share ChangeSet semantics for determinism.

Vector index and FTS:
- Vector and FTS features are built as pipeline stages in kvengine.
- Their metadata is also governed by ChangeSets to preserve ordering.

Storage class and DFS:
- DFS integration requires careful caching and IO scheduling.
- IA tiering relies on shard properties and coordinated GC.

### 11.3 merged_engine

Mental model:
- A synthetic store that replays WAL from multiple upstream stores.
- Uses rfengine for Raft logs and kvengine for applied state.
- Does not participate in Raft; commit is inferred by observation.

Key design choices:
- RegionProgress tracks log entries and commit index via counters.
- Commit advances when the same log index appears across stores (quorum).
- Manifest persists uncommitted entries to survive crashes.

Why this design exists:
- CDC requires a unified view of committed data.
- Running a new Raft cluster would be costly and redundant.
- The commit heuristic is a pragmatic tradeoff for operational CDC.

Recovery behavior:
- When manifest progress exists, merged rfengine is the source of truth.
- When progress is missing, recovery rebuilds from backups and WAL chunks.
- Tombstone regions are delayed until dependents are removed.

## 12. Cloud worker subsystems

cloud_worker is intentionally broad in scope.
This section maps the subsystems and their design constraints.

Cross-cutting controls:
- Worker limiter throttles concurrency globally and per keyspace.
- Memory limiter protects against snapshot and compaction spikes.
- These controls are enforced before heavy work is scheduled.

### 12.1 Remote coprocessor

- Handles coprocessor requests via HTTP and optional gRPC delegate.
- Uses its own runtime and caching to avoid interfering with storage nodes.
- Enforces concurrency and memory limits before heavy processing.

Design rationale:
- Coprocessor workloads are compute heavy and bursty.
- Isolating them prevents storage nodes from tail-latency spikes.

### 12.2 Load data ingestion

- Manages load data tasks and checkpointed progress.
- Persists state locally to survive restarts.

Design rationale:
- Load data tasks can be long-running and require resumability.

### 12.3 Native backup and restore

- Hosts native_br as a long-running background manager.
- Periodically reloads native_br config without full restart.

Design rationale:
- Backup and restore are operational workflows that should be isolated.

### 12.4 Schema manager

- Coordinates columnar schema files across stores and object storage.
- Uses local cache directories to reduce object storage traffic.

Design rationale:
- Schema files must be consistent and fast to access.

### 12.5 Transaction chunk handler

- Handles transaction chunk file creation for file-based transactions.
- Integrates with DFS and local caching.

Design rationale:
- Large transaction artifacts should not block storage node resources.

### 12.6 Replication worker integration

- cloud_worker can start replication_worker as a subsystem.
- This keeps CDC capture close to other offloaded services.

Design rationale:
- CDC has distinct runtime and IO patterns that are not a good fit for stores.

### 12.7 Local GC for IA

- Performs local GC for IA segments when enabled.
- Tied to remote cop enablement and data_dir configuration.

Design rationale:
- IA GC is only needed when IA tiering is active and remote cop is enabled.

## 13. Native backup and restore (native_br)

native_br orchestrates backup and restore with a strong focus on correctness.
It is conservative about time boundaries and failure tolerance.

### 13.1 Lightweight backup flow

High-level sequence:
1) Get backup_ts from PD TSO to anchor time consistency.
2) Fan out store backup requests via /rfengine/backup.
3) Back up keyspace metadata from PD/etcd as raw key-values.
4) Write ClusterBackupMeta to object storage.
5) Update GC service safe point after successful backup.

Why it is designed this way:
- Using PD TSO ensures a cluster-wide consistent time boundary.
- Safe point update after success avoids blocking GC on failed backups.
- Raw KV backup avoids tight coupling to PD metadata formats.

### 13.2 WAL and snapshot artifacts

Artifacts stored in object storage:
- Store snapshots and rlog objects for rfengine.
- WAL chunks per epoch, with a last-chunk marker when complete.
- Backup metadata describing stores and keyspace sizes.

Design rationale:
- WAL chunks provide incremental capture without full snapshots.
- Snapshot markers ensure restore does not see partial data.

### 13.3 Online WAL chunk fallback

native_br can request tail WAL chunks directly from stores.
This compensates for WAL chunks not yet uploaded to object storage.

Why this matters:
- Backup time windows are tight and WAL upload can lag.
- Online chunk fetch prevents a restore gap at the newest epoch.

### 13.4 Keyspace restore

Keyspace restore is the most complex flow.
It aligns backup shards to live region ranges and applies snapshots safely.

Key reasons for the design:
- Keyspace prefixing changes range boundaries and ordering.
- Existing cluster topology must be reshaped to match restored ranges.
- Lock resolution and consistency checks avoid silent data loss.

### 13.5 Error tolerance and missing stores

native_br allows a limited number of missing stores.
The backup metadata records tolerated missing stores explicitly.

Why this is allowed:
- Large clusters often have transiently unavailable stores.
- Strict failure would block backup progress unnecessarily.

## 14. Replication worker and CDC architecture

replication_worker provides CDC by replaying WAL into a synthetic store.
It behaves like a CDC capture and manages keyspaces and changefeeds.

### 14.1 Keyspace lifecycle

Add keyspace (simplified):
1) Validate TiCDC endpoint readiness for the keyspace.
2) Pause meta pack compaction to avoid racing with shard preparation.
3) Prepare shard metadata twice to reduce WAL race windows.
4) Load shards into merged_engine and persist keyspace state.
5) Start reporting regions to the keyspace PD.

Remove keyspace (simplified):
1) Reject if changefeeds still exist.
2) Unregister from safepoint management.
3) Remove regions and drop manifest state.
4) Tear down PD and TiCDC services if in Kubernetes mode.

Why the double preparation exists:
- rfengine continues to append WAL during keyspace add.
- A second pass reduces the risk of missing updates between stages.

### 14.2 WAL ingestion and sync

- WAL targets are fetched from stores or object storage.
- Store progress must be monotonic; mismatches are fatal.
- Sync applies committed entries to kvengine via merged_engine.

Why this design exists:
- CDC must preserve ordering and avoid data gaps.
- Monotonic progress is the only safe signal without Raft.

### 14.3 Apply observer and resolved-ts

- Apply observer converts applied write batches into CDC events.
- Lock scanning builds the resolver state for resolved-ts.
- resolved-ts only advances when regions are synced and lock scans complete.

Why these checks exist:
- TiCDC requires strict ordering for resolved-ts and event delivery.
- Advancing resolved-ts early can violate CDC correctness guarantees.

Resolved-ts timing detail:
- last_update_ts advances only after merged_engine has applied a full target.
- resolved-ts is derived from last_update_ts, not wall clock time.
- region_is_synced gates emission to avoid outrunning WAL replay.

Apply observer flush boundary:
- Flushing before scans avoids duplicate events and lock mis-ordering.
- It creates a clean boundary between historical scans and new applies.

### 14.4 Safepoint management

- replication_worker maintains per-changefeed service safepoints.
- It updates keyspace safepoints to prevent GC from removing needed data.

Why this is required:
- CDC needs historical data until changefeeds have safely advanced.

## 15. Cross-component invariants and ordering rules

These rules are the primary guardrails for correctness.
Breaking one almost always leads to data divergence or CDC corruption.

Raft and apply invariants:
- applied_index and last_preprocessed_index must be monotonic per peer.
- RegionEpoch must never move backward.
- Read index responses must preserve proposal order.

rfengine invariants:
- WAL epochs only overwrite after compaction has advanced.
- TRUNCATE_ALL_INDEX marks tombstone peers and must not be reused.
- Dependent tracking must gate truncation and destruction.

kvengine invariants:
- ChangeSets must be applied in strict sequence order.
- meta_seq must not move backward or skip ahead of change sets.
- SnapVersion for memtables must be monotonically increasing per shard.

merged_engine invariants:
- Store progress must be monotonic across epochs and offsets.
- commit_index must never be less than synced_index.
- Tombstone or uninitialized regions must never be applied.

CDC invariants:
- resolved-ts must not advance past synced regions.
- Event feed initialization must flush apply observer before scans.

## 16. Failure modes and recovery playbook

This section lists common failure modes and what they usually imply.
It is not exhaustive, but it highlights the most operationally relevant ones.

rfengine failures:
- WAL corruption in non-last file implies storage corruption or mis-rotation.
- Async WAL corruption is tolerated; sync WAL is authoritative.
- Persistent compaction lag leads to WAL write throttling.

kvengine failures:
- ChangeSet apply errors usually indicate stale metadata or version mismatch.
- Unexpected shard ranges imply split/merge ordering issues.

rfstore failures:
- Epoch mismatch errors indicate stale region metadata or reconfiguration.
- Snapshot apply stalls often involve dependents or pending apply states.

native_br failures:
- Missing stores beyond tolerance should halt backup or restore.
- Keyspace restore failures often relate to range alignment or locks.

replication_worker failures:
- StoreProgressMismatch indicates WAL ordering or duplication bugs.
- resolved-ts stalls imply missing sync progress or resolver state.

General recovery guidance:
- Prefer inspecting manifest and progress metadata before manual cleanup.
- Avoid deleting rfengine or kvengine files without understanding dependencies.
- Validate object storage consistency before running restore or CDC catch-up.

## 17. Observability and operational signals

Operational safety depends on the right signals at the right layers.
The component MAINTAINER_GUIDEs contain metric lists; this section focuses on intent.

Key rfengine signals:
- WAL compaction lag and throttling time indicate IO pressure.
- Snapshot upload duration indicates object storage health.

Key rfstore signals:
- Apply worker queue size indicates apply lag.
- Raft ready batching and persistence timing indicate IO pressure.

Key kvengine signals:
- Flush and compaction queue sizes indicate background IO backpressure.
- Columnar and vector pipeline metrics indicate analytics pressure.

Key replication_worker signals:
- WAL sync lag and store progress mismatch counts.
- resolved-ts advancement rate and blocked registrations.

Status server usage:
- Use status endpoints for runtime profiling, metrics, and diagnostics.
- Keep status access gated by security configuration.

## 18. Change management guidance

When you change one component, ask how it affects the others.
Small local changes often have hidden cross-component consequences.

General guidance:
- Update component MAINTAINER_GUIDE and this guide for any invariant changes.
- Preserve ordering between persistence, apply, and metadata updates.
- Treat ChangeSet encoding and Raft log encoding as stable interfaces.

Specific cautions:
- rfengine WAL format changes require explicit versioning and tooling updates.
- merged_engine commit heuristics are CDC correctness boundaries.
- native_br backup metadata format changes must preserve backward compatibility.
- cloud_worker runtime changes can alter load-shedding behavior.

Testing guidance:
- Prefer targeted tests in tests/cloud_engine or tests/cloud_engine_failpoints.
- Reproduce CDC workflows when changing merged_engine or replication_worker.
- Validate backup and restore flows when changing rfengine or native_br.

## 19. Reading map and companion docs

If you are new to the codebase, use this reading order.
It mirrors the major data paths and cross-component integration points.

Core MAINTAINER_GUIDEs:
- components/cloud_server/MAINTAINER_GUIDE.md
- components/cloud_worker/MAINTAINER_GUIDE.md
- components/rfstore/MAINTAINER_GUIDE.md
- components/rfengine/MAINTAINER_GUIDE.md
- components/kvengine/MAINTAINER_GUIDE.md
- components/native_br/MAINTAINER_GUIDE.md
- components/replication_worker/MAINTAINER_GUIDE.md
- components/merged_engine/MAINTAINER_GUIDE.md

Feature docs that explain relevant cross-cutting behavior:
- doc/features/txn_file.md
- doc/features/remote_coprocessor.md
- doc/features/schema_manager.md
- doc/features/worker-scaler.md
- doc/features/blob_store.md
- doc/features/archive.md

## 20. Glossary

- Apply: Applying committed Raft entries to kvengine state.
- ChangeSet: Deterministic metadata update replicated across replicas.
- CDC: Change data capture, served by replication_worker.
- Epoch: rfengine WAL rotation unit.
- Keyspace: Logical key prefix used for isolation and restore.
- Manifest: rfengine or merged_engine metadata log used for recovery.
- Peer: Local replica of a region.
- Region: PD metadata unit for a key range and Raft group.
- Resolved-ts: Timestamp where all prior transactions are known resolved.
- rlog: rfengine immutable log segment produced by WAL compaction.
- Shard: kvengine storage unit mapped 1:1 with a region.
- WAL: Write-ahead log used by rfengine for Raft state durability.
