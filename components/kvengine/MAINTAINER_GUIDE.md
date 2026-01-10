# kvengine Maintainer Guide

This document is for maintainers who need to reason about correctness, performance,
and evolution of the kvengine component in TiKV.
It focuses on the non-obvious design choices, invariants, and workflows.
Basic API signatures and simple struct fields are intentionally omitted.

Status: describe current behavior as implemented in `components/kvengine`.
If you change a design invariant, update this document in the same patch.

## Contents

- 1. Scope And Reading Strategy
- 2. System Context And Responsibility Boundaries
- 3. Core Concepts And Invariants
- 4. Key Encoding, Ranges, And Versioning
- 5. Engine And Shard Model
- 6. Write Path And Property Semantics
- 7. MemTable Lifecycle And Flush Scheduling
- 8. Read Path And Snapshot Semantics
- 9. Iterators And Multi-Source Merging
- 10. LSM Layout And File Types
- 11. Blob Storage Path
- 12. ChangeSet: Deterministic Metadata Updates
- 13. Recovery And Startup
- 14. Flush Implementation Details
- 15. Compaction Scheduler And Priorities
- 16. Row-Store Compaction Details
- 17. Columnar Pipeline And Schema Files
- 18. Columnar Compaction And Major Compaction
- 19. Vector Index Pipeline
- 20. Full-Text Search (FTS) Path
- 21. TxnFile And Lock Handling
- 22. Split, Prepare-Merge, Commit-Merge
- 23. Storage Class And Infrequent-Access (IA) Tier
- 24. DFS Layer, Local Files, And Caching
- 25. Resource Control And Flow Throttling
- 26. Observability And Debug Aids
- 27. Common Failure Modes And Pitfalls
- 28. Maintainer Playbook For Changes
- 29. Glossary

## 1. Scope And Reading Strategy

- This guide is opinionated toward maintainer work: correctness, performance,
  production recovery, and feature extension.
- It explains why the design is shaped as it is, not just what the code does.
- Use it as a map to find where invariants live and how to avoid regressions.
- Use source as ground truth for details not covered here.

Suggested reading order:

- Read sections 2-5 for architecture and invariants.
- Read sections 6-9 for data plane flows.
- Read sections 12-16 for metadata, flush, and compaction behavior.
- Read sections 17-20 for analytical and vector features.
- Read sections 22-24 for lifecycle and storage tiering.

## 2. System Context And Responsibility Boundaries

kvengine sits between Raft (rfstore) and storage media (DFS + local cache).
It is the state machine for committed Raft commands.
The separation of responsibilities is the core reason for several design choices.

Key boundaries:

- rfstore owns consensus and replication.
- kvengine owns deterministic application of committed changes and background IO.
- DFS provides durable storage; kvengine decides what to load and when.
- Metadata changes must be replicated via Raft to keep replicas consistent.

Why this matters:

- kvengine cannot apply background work (flush/compaction) unilaterally.
- Every change to the LSM tree must be captured as a ChangeSet and applied
  on all replicas in the same order.
- This allows heavy work (flush, compaction, conversion) to happen on one node
  while others just replay the result.

## 3. Core Concepts And Invariants

The following invariants show up repeatedly in correctness fixes.
If you violate one, you will see subtle divergence between replicas.

### 3.1 Shard As The Unit Of Consistency

- A shard is an independent LSM tree for a key range.
- A shard corresponds to a Raft region in rfstore.
- Shard metadata is versioned (shard id + shard ver).
- Shards can be split or merged, but files must remain range-consistent.

### 3.2 Deterministic Metadata Updates

- LSM structure changes are captured as ChangeSets and proposed through Raft.
- Applying the same ChangeSet on all replicas must yield the same metadata.
- Any background worker output that mutates files must produce a ChangeSet.

### 3.3 Versioning And Sequencing

There are multiple version counters that must stay consistent:

- `write_sequence`: applied Raft index for WriteBatch.
- `meta_seq`: applied Raft index for ChangeSet.
- `base_version`: base of SnapVersion for memtables.
- `base_version` is adjusted during split/merge to avoid version regressions.
- `SnapVersion`: (base_version, data_sequence) pair for file snapshots.
  - `meta_seq` can lag `write_sequence` because ChangeSets are applied
    by background workers.
- `data_sequence` in ShardMeta tracks the Raft index of data persisted
  in the latest L0 file.

Rules of thumb:

- A memtable snapshot version must monotonically increase per shard.
- A ChangeSet with sequence <= meta_seq must be treated as duplicate.
- L0 tables created from a memtable must carry the same SnapVersion.

### 3.4 Properties Are Part Of State

- Shard properties are not just metadata; they can alter read/write behavior.
- Some properties must be persisted before advancing data sequence.
- Some properties are intentionally skipped during initial flush.
- Property handling is a common source of divergence after restore/split/merge.

## 4. Key Encoding, Ranges, And Versioning

### 4.1 Outer Key vs Inner Key

- kvengine stores an `InnerKey` that is derived from the outer key.
- In API v2 mode, the outer key contains a keyspace prefix.
- `InnerKey` strips the keyspace prefix for internal ordering.
- The end key in ranges is exclusive; special handling is required for
  keyspace boundary end keys.

Why:

- Stripping keyspace prefix keeps LSM ordering stable and compatible with
  TiDB table key encoding.
- Range boundaries are used for overlap checks and file selection.

### 4.2 Range Representation

- `ShardRange` tracks outer start and outer end (exclusive).
- The inner key offset is preserved to handle mixed keyspace boundaries.
- Global end key is represented with a sentinel for empty/end-of-keyspace.

### 4.3 MVCC Metadata

- kvengine uses three CFs: write, lock, and extra.
- Managed CFs require non-zero versions; non-managed CF uses 0 or SnapVersion.
- `UserMeta` encodes (start_ts, commit_ts).
- Extra CF stores auxiliary transaction status keys and other side data.
- A Value includes meta, user_meta_len, version, user_meta, and value bytes.

Why:

- This keeps MVCC semantics compatible with TiKV while enabling multi-format
  storage and remote processing.
- Lock and write separation allows efficient lock conflict detection.

## 5. Engine And Shard Model

### 5.1 EngineCore Responsibilities

- Owns global caches (block cache, fd cache, value cache).
- Owns DFS handle and compaction client.
- Owns shard registry and keyspace -> shard mapping.
- Runs background workers: flush, compaction, free-mem.

### 5.2 Shard Structure

- `Shard` contains range, version, and in-memory state.
- `ShardData` holds memtables, L0 tables, L1+ tables, columnar levels,
  vector indexes, and blob table mapping.
- `ShardMeta` is persisted metadata used at startup and recovery.

Why separate ShardData and ShardMeta:

- ShardData holds live in-memory structures with file handles and caches.
- ShardMeta is a compact representation used for recovery and snapshots.
- ChangeSets are applied to ShardData, and ShardMeta is reconstructed as needed.

### 5.3 Active vs Inactive Shards

- Inactive shards are ignored by flush and compaction.
- During split/merge, shards may temporarily be inactive or parent-only.
- `initial_flushed` indicates whether the shard owns its own L0 tables.

Why:

- Split/merge reuses files across shards; initial flush is required to make
  each shard self-contained.
- Pausing background work prevents building files on stale versions.

### 5.4 Pending Operations And Shard Properties

- Shard pending operations track delayed actions such as delete prefixes,
  truncate_ts, trim_over_bound, manual major compaction, storage class changes,
  and GC of lock or extra CF files.
- Pending ops are derived from shard properties and updated on apply.
- They drive compaction priority selection and background tasks.

Why:

- It decouples Raft apply from background scheduling.
- It keeps per-shard decision state explicit and persistent across restarts.

## 6. Write Path And Property Semantics

### 6.1 WriteBatch Semantics

- A WriteBatch contains per-CF write batches and a property map.
- The write path is called after Raft commit, not before.
- Each entry has an InnerKey, meta flags, user_meta, and version.

Important version rules:

- Managed CFs (write/lock) require non-zero versions.
- Non-managed CFs use SnapVersion as a synthetic version.
- The write path updates versions for non-managed CFs when needed.

Why:

- This makes MVCC consistency independent of read or compaction order.
- It lets non-managed CFs participate in the same LSM structure.

### 6.2 Property Updates During Write

Some properties directly impact behavior and must be handled specially:

- `DEL_PREFIXES_KEY` influences destroy-range behavior.
- `TRIM_OVER_BOUND` triggers compaction trimming at range boundaries.
- `MANUAL_MAJOR_COMPACTION` schedules a major compaction.
- `TXN_FILE_REF` updates lock txn files and commit/rollback tracking.

Key behavior:

- Properties can be duplicated during restore or recovery.
- The write path checks duplication and still executes side effects
  (for example, switching memtables) to keep replicas deterministic.
- Some properties are tracked as "must persist" before advancing sequence.

Why:

- Property side effects must be deterministic across peers.
- Skipping a duplicated property can diverge memtable switching
  and cause later version mismatches.

### 6.3 Write Sequence And MemTable Switching

- The write sequence is updated after each WriteBatch.
- A memtable switch is triggered when:
  - The current memtable exceeds `max_mem_table_size`.
  - A property requires a switch (for example, delete range overlap).
  - Split/merge forces a switch to isolate old data.

Side effects of switching:

- The old memtable becomes immutable.
- A new writable memtable becomes the first in the list.
- The shard triggers a flush for the immutable memtable.

### 6.4 TxnFileRef Handling During Write

- Transaction file references are stored in the `TXN_FILE_REF` property.
- The write path loads txn files via TxnChunkManager.
- Locks are merged into shard state; commit entries are added to memtables.

Why:

- Large lock content is stored outside the main LSM tree.
- This keeps lock-heavy workloads from bloating memtables and L0 files.

### 6.5 DeletePrefixes And Destroy Range

- DeletePrefixes represent logical range tombstones by prefix.
- They are stored in shard properties and merged across split/merge.
- When a new delete prefix overlaps the writable memtable, the write path
  forces a memtable switch to isolate old data.
- Destroy range compaction consumes delete prefixes and materializes the
  deletion by removing or compacting affected files.

Why:

- Prefix tombstones are cheap to replicate through Raft compared to large
  delete batches.
- The memtable switch ensures the delete prefix applies deterministically
  across replicas, even if in-memory state differs.

## 7. MemTable Lifecycle And Flush Scheduling

### 7.1 MemTable List And Snapshot Versions

- A shard maintains a list of memtables, newest first.
- Each memtable has a SnapVersion assigned when it becomes immutable.
- The snapshot version is used to ensure the flush result matches metadata.

Invariant:

- The last (oldest) memtable must match the SnapVersion of L0 tables
  produced by flushing it.

### 7.2 Forced Switch And Initial Flush

- Split and merge require a forced memtable switch, even if empty.
- This guarantees that the new shard owns a clean boundary of data.
- Initial flush uses a specific ChangeSet type and sets shard metadata.

### 7.3 Flush Scheduling

- A background flush worker consumes FlushMsg events.
- Flush tasks are tagged with shard id/ver and range.
- Flush uses encryption key from shard state if present.

Why:

- Flush is IO-heavy and should be off the raft apply path.
- A single worker reduces ordering complexity and metadata races.

## 8. Read Path And Snapshot Semantics

### 8.1 SnapAccess

- `SnapAccess` is the primary read facade for a shard snapshot.
- It captures memtable pointers, L0 tables, L1+ tables, and schema files.
- It can be constructed from a ChangeSet (for remote coprocessor usage).

### 8.2 Read Timestamp Selection

- If a read_ts is provided, use it directly.
- For managed CFs, a managed safe ts can be injected.
- For non-managed CFs, use the shard memtable SnapVersion.

Why:

- Non-managed CFs must remain consistent with the same snapshot
  that produced their data.

### 8.3 Point Get Order

The read order is deterministic:

1. Lock txn files (lock CF only).
2. Memtables (newest to oldest).
3. L0 tables (newest to oldest).
4. L1+ levels (level order, non-overlapping).

This order ensures the latest visible version is returned.

### 8.4 Blob Reference Resolution

- Values can be stored as a BlobRef in the main LSM.
- Blob tables are looked up by file id and offset.
- Reads can prefetch blob data to reduce tail latency.

Why:

- Blob separation reduces write amplification for large values.
- It also allows different storage classes for large values.

### 8.5 Value Cache

- Optional cache stores latest visible values per (shard, keyspace, key).
- Cache entries include a valid SnapVersion.
- On shard change, cached values are validated via a read.

Why:

- It avoids repeated L0/L1+ traversals for hot keys.
- Validation prevents stale reads after memtable or L0 changes.

### 8.6 Snapshot Construction For Remote Reads

- Remote reads can build a snapshot from ChangeSet bytes plus memtable data.
- Memtable data supports multiple formats (V1 and V2) for compatibility.
- V2 encodes skiplist data plus txn file references for lock visibility.
- A MemoryLimiter guard is acquired before loading tables to avoid OOM.
- When IA is enabled, table size estimation is relaxed to avoid double-counting.

Why:

- Remote coprocessor requests must be fully self-contained.
- Memory limits prevent a single remote read from exhausting the process.

## 9. Iterators And Multi-Source Merging

### 9.1 Iterator Hierarchy

- TableIterator scans a single SSTable (or L0 sub-table).
- ConcatIterator stitches files in a level that are already non-overlapping.
- MergeIterator merges multiple sorted iterators into one stream.

### 9.2 Version Iteration

- `next()` advances to the next key.
- `next_version()` walks older versions of the same key.
- Read path uses `seek_to_version` to find the visible version.

Why:

- Keeping version iteration explicit avoids accidental skipping of MVCC data.
- Iterators are reused across row, columnar, and vector pipelines.

### 9.3 Async vs Sync Iterators

- Some tables are async-backed (IA or remote).
- The iterator traits provide sync and async variants for seek/next.
- Call sites must respect `is_next_sync` and `is_next_version_sync`.

Why:

- Async is necessary when reads can block on remote fetches.
- The dual-mode trait allows reusing the same logic in both paths.

## 10. LSM Layout And File Types

### 10.1 Levels And CF Layout

- There are three CFs: write, lock, extra.
- Each CF has a configured number of levels (CF_LEVELS).
- L0 files overlap by range; L1+ files are non-overlapping per level.

Why:

- L0 overlap allows fast flush without immediate reorganization.
- L1+ non-overlap enables efficient range scans and compaction decisions.

### 10.2 L0 File Format

- An L0 file can contain multiple CFs in a single physical file.
- A special L0 format is used for "split L0" where only write CF is present.
- L0 footer stores SnapVersion and CF offsets.

Why:

- Multi-CF L0 reduces file count and keeps flush atomic.
- Split L0 reduces read overhead when only write CF is populated.

### 10.3 L1+ SSTable Format

Each SSTable contains:

- Data blocks (sorted key/value entries).
- Optional old-version data blocks for MVCC history.
- Index blocks for data and old data.
- A BinaryFuse8 filter for key existence checks.
- Properties and footer with offsets and magic number.

Why:

- Separating old-version blocks reduces read amplification for latest reads.
- Filter and block cache allow high QPS on read-heavy workloads.

### 10.4 File Types

kvengine uses multiple file types with different lifecycles:

- SST (row-store data).
- Blob (large values).
- Columnar (analytical storage).
- Vector index (ANN search).
- Schema file (table schema for columnar/vector).
- Txn chunk (lock data segments).

Why:

- Separate file types allow independent caching and storage-class policies.
- It also enables different compaction and GC strategies per format.

## 11. Blob Storage Path

### 11.1 BlobRef In LSM

- Values above a threshold are stored in BlobTable.
- The main LSM stores a BlobRef pointing to (file id, offset, length).
- Blob data is fetched on demand during reads.

### 11.2 Blob Table Build

- Blob tables are created during compaction or flush when values exceed size.
- L0 and L1+ tables reference blob entries for large values.
- Blob table size contributes to shard estimated size and GC decisions.

Why:

- Large values dominate compaction cost when embedded in SSTs.
- Blob tables reduce write amplification and support tiering policies.

## 12. ChangeSet: Deterministic Metadata Updates

### 12.1 What A ChangeSet Represents

A ChangeSet is a Raft-replicated description of a metadata mutation.
Examples include:

- Flush (memtable -> L0).
- Compaction (L0/Ln -> new Ln).
- Destroy range or trim over bound.
- Initial flush after split/merge.
- Ingest files (bulk import).
- Columnar compaction or vector index update.
- Schema updates or storage class changes.

### 12.2 Apply Path Invariants

- ChangeSets are applied in order of Raft sequence.
- Duplicate sequences are ignored.
- Shard version must match, otherwise apply is rejected.
- After apply, shard states are refreshed and meta_seq is updated.
- Apply reloads the shard pointer because split/merge can replace it.

Why:

- Applying the same ChangeSet on every replica is the only way
  to keep LSM metadata consistent in a distributed system.

### 12.3 ChangeSet Materialization

- ChangeSets are prepared by loading referenced files from DFS.
- `prepare_change_set` resolves file ids and loads local handles.
- When prepared, the ChangeSet contains actual table objects.
- PrepareType can limit loading to row or columnar files.
- Table filters can skip files during restore or partial recovery.

Why:

- This isolates DFS latency and encryption handling from the apply path.
- It also allows filtering of files for restore scenarios.

### 12.4 Ingest And Restore ChangeSets

- IngestFiles ChangeSets introduce externally built tables (bulk import).
- An ingest id property is used to deduplicate repeated ingestion.
- Ingest updates can adjust `columnar_table_ids` to force re-derivation
  of columnar data for that table id.
- RestoreShard ChangeSets rebuild shard state from a snapshot and advance
  the shard version to invalidate earlier ChangeSets.

Why:

- Bulk ingestion must be idempotent under Raft retries.
- Restore must reset both data and metadata in a single deterministic step.

## 13. Recovery And Startup

### 13.1 Metadata Loading

- Engine startup reads shard metadata from a MetaIterator.
- ShardMeta is reconstructed from ChangeSets recorded in meta storage.
- Files in the blacklist are tracked to avoid reloading broken files.

### 13.2 Loading Parent Shards

- During recovery, parent shards are loaded if a child references them.
- Parent shards are only kept long enough to seed child data.
- Parent memtables can be shared to avoid data loss during split recovery.

Why:

- Split recovery needs parent data until children have their own L0 files.
- This prevents missing data on restart in split-in-progress scenarios.

### 13.3 Concurrency Controls

- Recovery uses a concurrency token bucket (RECOVERY_CONCURRENCY).
- DFS loading is globally limited by DfsLoadLimiter.
- Each request also has per-request concurrency limits.

Why:

- Recovery can spawn many DFS reads; concurrency limits prevent OOM.
- Concurrency defaults are scaled by CPU cores for practicality.

### 13.4 Meta Pack And Tiny Metadata

- The engine can consume a MetaPack that stores \"tiny\" metadata for SST files.
- Tiny metadata includes footer, properties, and optional IA segment offsets.
- When available, it avoids full footer reads during startup and preparation.

Why:

- DFS round trips dominate startup time when many small files exist.
- Tiny metadata keeps recovery fast without loading full table data.

## 14. Flush Implementation Details

### 14.1 Normal Flush

- A normal flush converts one immutable memtable into one or more L0 tables.
- L0 size can be split into multiple files if configured.
- Flush produces a ChangeSet with L0Create entries and properties.

Property handling during flush:

- Only properties marked as "need flush" are carried.
- TXN_FILE_REF is filtered to remove finished txn files.
- TERM_KEY is deliberately skipped in initial flush to preserve sequencing.

### 14.2 Initial Flush

- Initial flush is used for split and merge.
- It sets shard range boundaries and base version in metadata.
- It creates L0 files representing a clean shard snapshot.

### 14.3 Apply Flush Consistency Checks

- Apply flush verifies the last memtable SnapVersion matches L0 SnapVersion.
- Mismatch is treated as fatal to avoid silent divergence.
- After apply, the flushed memtable is freed asynchronously.

Why:

- Memtable version mismatch indicates out-of-order ChangeSet application
  or incorrect memtable switching.

## 15. Compaction Scheduler And Priorities

### 15.1 Compaction Runner

- Compaction is driven by a background runner consuming CompactMsg events.
- Messages include: Compact, Finish, Applied, Clear, Pause, UnblockKeyspace.
- Each shard has a compaction priority tracked in its state.

Why:

- A dedicated runner serializes compaction decisions and avoids
  conflicting file selections.

### 15.2 Priority Types

CompactionPriority includes:

- L0 compaction (score-based).
- L1+ compaction (per CF and level).
- Major compaction (manual or automatic).
- Destroy range, truncate_ts, trim_over_bound.
- GC of lock files or extra CF files.
- L0 to columnar conversion.
- Columnar L0/L1/major/clear.
- Vector index update (incremental or rebuild).

### 15.3 Scoring And Ordering

- Each priority has a score used for ordering.
- Some operations are treated as max priority (destroy range, truncate ts).
- Columnar and vector priorities can override row priorities.

Why:

- Overbound data and range cleanup must preempt size-based compaction.
- Columnar and vector features must stay consistent with row data.

### 15.4 Safe Point Integration

- Compaction uses GC safe point to drop old versions.
- API v2 safe points are preferred; fallback to v1 when allowed.
- Keyspace id is derived from shard range prefix.

Why:

- GC must respect transactional visibility across keyspaces.
- Safe point is globally coordinated, but applied locally during compaction.

### 15.5 Remote Compaction

- Compaction can be offloaded to a remote compactor service.
- The request is JSON-serialized and includes a required protocol version.
- The request carries exported encryption key material when needed.
- The response is a ChangeSet applied via the normal Raft path.

Fallback behavior:

- If no remote compactor is configured, compaction runs locally.
- Incompatible remote compactor can trigger a local fallback (if enabled).
- Temporary failures trigger retries with exponential backoff.
- Non-permanent remote endpoints can be removed after repeated failures.

Why:

- Remote compaction reduces compute pressure on TiKV nodes.
- The ChangeSet boundary keeps metadata deterministic even when work is remote.

## 16. Row-Store Compaction Details

### 16.1 L0 Compaction

- L0 files overlap; compaction merges them into level 1.
- Selection considers overlap size and L0 pressure.
- Input size is used to schedule file id allocation.

Why:

- L0 overlap leads to high read amplification.
- L0 compaction is the primary driver for read performance stability.

### 16.2 L1+ Compaction

- L1+ files are non-overlapping per level.
- Compaction selects a level and merges with overlapping next level files.
- Base size and level ratios determine pressure.

Why:

- L1+ compaction keeps size ratios stable and controls amplification.

### 16.3 Major Compaction

- Major compaction merges all levels into a target level.
- Can be triggered manually via property.
- Used for aggressive GC and reclaiming dead versions.

### 16.4 Tombstone-Based GC

- Compaction triggers when tombstone ratio or count exceeds thresholds.
- Tombstone stats are derived from WRITE_CF level 2+.
- Safe point determines the cutoff for removal.

Why:

- Tombstone ratio is a better signal for compaction than size alone.

### 16.5 Trim And Truncate Operations

- `truncate_ts` removes data newer than a threshold (restore use-case).
- `trim_over_bound` removes data outside shard range.
- Both are implemented as specialized compactions to keep metadata consistent.

## 17. Columnar Pipeline And Schema Files

### 17.1 SchemaFile Role

- SchemaFile stores table schemas used for columnar and vector building.
- It is a standalone file with checksum and format version.
- SchemaFile includes per-table and per-partition storage class specs.

Why:

- Columnar conversion needs schema to interpret row data.
- Schema can change independently of data files.

### 17.2 Columnar File Layout

- Columnar files store data in column packs.
- Each pack is a compressed batch of values.
- Special columns exist for handle and MVCC version.
- A metadata section maps tables and columns to pack offsets.

Why:

- Columnar layout allows efficient scans and predicate pushdown.
- Packs enable vectorized processing and compression efficiency.

### 17.3 Unconverted L0 Tracking

- Shards track `unconverted_l0s` for row files not yet converted.
- Conversion is triggered as a compaction priority when schema is ready.
- Columnar conversion uses the schema version as a guard.

Why:

- Decoupling conversion keeps the write path fast.
- Tracking unconverted L0 avoids losing visibility in analytical queries.

### 17.4 Schema Updates And Columnar Table Ids

- Schema updates arrive as UpdateSchemaMeta ChangeSets.
- ShardData keeps `schema_version` and `restore_version`
  even if the schema file is temporarily unavailable.
- `columnar_table_ids` tracks tables that have completed columnar major
  compaction, and influences conversion and vector index scheduling.

Why:

- Schema changes must be applied deterministically to derived data.
- Keeping explicit table id sets prevents partial columnar state.

## 18. Columnar Compaction And Major Compaction

### 18.1 Columnar L0 and L1 Compaction

- Columnar L0 files are ordered by SnapVersion (newest first).
- Columnar L1 files are also ordered by SnapVersion to preserve MVCC view.
- Columnar L2 files are ordered by key range to support non-overlapping scans.
- Compaction merges packs and rebuilds metadata indexes.

### 18.2 Columnar Major From SST

- Columnar major from SST is used when table id sets need add/clear actions
  or when a manual major compaction is requested from row data.
- Table ids to add or clear are computed from schema changes.
- The schema file version is validated to avoid rebuilding with stale schema.

Why:

- Columnar files must reflect the latest schema.
- Rebuild from SST avoids compounding errors from incremental conversion.

### 18.3 Columnar Major From Columnar

- A purely columnar major compaction merges columnar files without row data.
- Used for space reclamation when schema is stable.

### 18.4 Clear Columnar

- Clearing columnar removes all columnar files and resets L2 snap version.
- Used when columnar data is corrupted or intentionally dropped.

Why:

- Columnar data is derived; it can be rebuilt from row data when needed.

## 19. Vector Index Pipeline

### 19.1 Vector Index Files

- Each vector index file is tied to a table id, index id, and column id.
- Files are ordered by SnapVersion and then file id.
- Index data is stored in a usearch format with metadata sections.

### 19.2 Update Triggers

- Vector index updates are scheduled via compaction priority.
- Updates can be incremental or full rebuild.
- Schema version changes can force rebuilds.

Why:

- Vector indices are derived from columnar data and must track MVCC visibility.
- Rebuild is necessary when schema or vector column definition changes.

### 19.3 MVCC And Deletes

- Vector index includes versions and nulls bitmap to represent deletions.
- SnapVersion tracks the visibility horizon for the index.

Why:

- Vector results must match MVCC visibility of row data.

## 20. Full-Text Search (FTS) Path

- FTS is implemented as a columnar reader that scans a text column.
- The current implementation is a brute force scorer over the columnar data.
- The reader reconstructs an inner schema when the query omits the FTS column.
- FTS queries use `FtsQueryInfo` and operate within shard bounds.

Why:

- FTS is a derived feature; correctness requires consistency with row data.
- Brute-force is used where index structures are not yet available.

## 21. TxnFile And Lock Handling

### 21.1 Why TxnFile Exists

- Lock data can be large (for example, secondary locks).
- Storing lock payloads directly in the LSM inflates memtables and L0 files.
- TxnFile stores lock data in separate chunked files.

### 21.2 TxnFile Structure

- A TxnFile is identified by (shard_id, shard_ver, start_ts).
- It contains a list of TxnChunks, each with an index over key ranges.
- The TxnCtx records the key bounds and metadata used for reads.

### 21.3 TxnChunkManager

- Manages local or in-memory storage of txn chunks.
- Loads txn chunks from DFS when not cached locally.
- Maintains a GC worker when chunks are in-memory.

Why:

- Lock data is transient and does not need full SST semantics.
- Separate management allows specialized retention policies.

### 21.4 Lock Reads

- Read path checks txn files for lock CF before memtables.
- This ensures lock visibility is correct even when lock data is offloaded.

## 22. Split, Prepare-Merge, Commit-Merge

### 22.1 Split Flow

- rfstore issues a split ChangeSet after Raft commit.
- kvengine creates new shards for each split range.
- New shards share parent files until initial flush completes.

Key steps:

1. Switch memtables and clear pending background tasks.
2. Build new shard ranges and inherit relevant properties.
3. Split memtables, L0s, L1+ tables, blobs, columnar files, vector indexes.
4. Trigger initial flush to make each shard independent.
5. Filter columnar table ids by table id range derived from the shard bounds.
6. Keep only columnar and vector index files that overlap the new range.

Why:

- Sharing files avoids expensive re-compaction during split.
- Initial flush ensures future compactions are shard-local.

### 22.2 Merge Preconditions

`check_merge` enforces constraints before merge:

- Both source and target must be initial flushed.
- No txn file locks in source or target.
- No unconverted L0s (columnar consistency).
- Consistent encryption keys if in same keyspace.
- Storage class specs must be compatible.

Why:

- Merging with pending lock data or unconverted L0s can lose visibility.
- Encryption and storage class mismatches can break keyspace isolation.

### 22.3 Prepare Merge

- `prepare_merge` switches memtables and creates a new shard version.
- The new shard inherits parent id to enable initial flush.
- Background tasks are cleared to avoid using stale versions.

### 22.4 Commit Merge

- Merge combines source and target shard data and properties.
- Data can be cleared for source or target when keyspace boundaries differ.
- Range bounds are updated to cover both shards.
- Base version is advanced to be higher than both source and target.

Why:

- When inner key offsets differ (different keyspaces), old data can violate
  ordering; clearing prevents corruption.
- Advancing base version prevents SnapVersion regressions.

### 22.5 Rollback Merge

- If merge is rolled back, a new shard version is created.
- Shard is marked initial flushed to continue normal operation.

## 23. Storage Class And Infrequent-Access (IA) Tier

### 23.1 StorageClassSpec

- StorageClassSpec describes tiering rules per table or partition.
- It is encoded in schema metadata and persisted as a shard property.
- Shard pending ops track the current storage class spec.

### 23.2 IA Files

- IA files keep only metadata locally; data blocks are fetched on demand.
- IA files are built for SST, columnar, vector, and blob file types.
- Segment offsets are used to align remote fetches to block boundaries.

Why:

- This reduces local disk footprint and suits object storage backends.
- It preserves correctness while trading latency for cost.

### 23.3 IaAutoFile And Transitions

- IaAutoFile can transition between storage classes based on rules.
- The decision uses timestamps and configured transit rules.
- Transition logic is designed to be safe for concurrent reads.

### 23.4 IA Manager

- IaManager tracks cached segments using an S3-FIFO policy.
- It enforces memory and disk caps with dynamic capacity.
- It stores a local "meta file" for quick access to table metadata.

Why:

- Segment-level caching enables consistent performance
  without keeping full files locally.

### 23.5 Storage Class Updates

- Storage class changes are applied via a ChangeSet with `STORAGE_CLASS_KEY`.
- The ChangeSet can include reloaded L1+ tables with updated storage class.
- If the new spec is unspecified, the property is cleared and only reloads
  are applied.

Why:

- Storage class changes are external to data correctness but must remain
  consistent across replicas and restarts.

## 24. DFS Layer, Local Files, And Caching

### 24.1 DFS Abstraction

- The Dfs trait provides create/read/remove and runtime access.
- Local and in-memory DFS implementations are available.
- S3-backed DFS supports storage class operations.

### 24.2 File Naming And Directories

- File ids are allocated by an IdAllocator shared across components.
- Local directories can be multiple; files are distributed by hash.
- A global file lock prevents multiple engines from opening the same dir.

Why:

- Distribution across dirs reduces single-disk contention.
- File locking avoids destructive concurrent usage.

### 24.3 File Caches

- BlockCache caches data blocks for SST and L0 tables.
- FdCache caches open file descriptors.
- ColumnarMetaCache caches columnar metadata sections.
- Meta file cache reduces repeated reads of table metadata.

Why:

- Caches reduce object storage latency and local disk churn.
- Metadata caching is critical for IA and remote reads.

### 24.4 File Blacklist

- Files can be marked as blacklisted by the metadata source.
- Blacklisted file ids are tracked by the engine and consulted during load.
- This prevents repeated attempts to load known-bad files.

## 25. Resource Control And Flow Throttling

### 25.1 Store And Region Limiters

- Store limiter throttles write throughput based on memtable usage.
- Region limiter controls per-shard memtable and L0 growth.
- Throttling uses soft and hard limits with a dynamic speed limit.

Why:

- Throttling prevents memory spikes and reduces tail latency.
- It also avoids compaction backlog explosions during heavy writes.

### 25.2 DFS Load Limiter

- DFS loading is bounded by a global semaphore.
- Per-request concurrency also limits file load parallelism.

### 25.3 Low Space Threshold

- Engine tracks available space and can reject operations
  when below the threshold.

## 26. Observability And Debug Aids

### 26.1 Metrics

- Compaction trigger and result counters track scheduler behavior.
- Value cache hit/fill metrics indicate caching efficiency.
- IA metrics track sync reads and cache behavior.
- Stats APIs provide per-shard and per-level size information.

### 26.2 Debug Features

- `debug-trace-mem-table` logs memtable switches and writes.
- Failpoints exist for flush and compaction paths.
- Meta pack counters report usage of tiny metadata optimization.

### 26.3 Diagnostic Tips

- When investigating compaction issues, check shard compaction priority.
- For inconsistent reads, compare memtable SnapVersion vs L0 SnapVersion.
- For columnar issues, verify schema file version and columnar table ids.

## 27. Common Failure Modes And Pitfalls

- Memtable version mismatch during flush apply indicates missed switch.
- Applying ChangeSet out of order can silently drop files.
- Columnar conversion without schema file leads to inconsistent analytics.
- Merging shards with unconverted L0s risks losing columnar visibility.
- Using different encryption keys within a keyspace breaks merge safety.
- Clearing data on merge is required when keyspace boundaries differ.
- Loading files without respecting DfsLoadLimiter can trigger OOM.
- Skipping TXN_FILE_REF persistence can break lock visibility.

## 28. Maintainer Playbook For Changes

### 28.1 Adding A New File Type

- Extend FileType enum and suffix mapping.
- Implement metadata parsing for table footer and properties.
- Ensure ChangeSet preparation and apply can load and install the file.
- Add IA and storage class handling if the file can be tiered.

### 28.2 Extending ChangeSet

- Add protobuf fields in kvenginepb.
- Update prepare_change_set to load referenced files.
- Update apply_change_set with sequencing and state refresh logic.
- Update ShardMeta reconstruction and persistence.

### 28.3 Modifying Compaction

- Update CompactionPriority scoring and ordering.
- Ensure input selection avoids overlap and preserves ordering.
- Include safe point handling where MVCC data is removed.
- Add metrics for trigger and result.

### 28.4 Changing Columnar Or Vector Logic

- Ensure schema version gating is correct.
- Update snapshot and restore handling to keep derived data consistent.
- Validate table id range calculations for split/merge.

### 28.5 Storage Class And IA Changes

- Keep StorageClassSpec and pending ops consistent.
- Ensure transitions are idempotent and safe for concurrent reads.
- Update stats and metrics to reflect new tiers.

## 29. Glossary

- ChangeSet: Raft-replicated metadata mutation describing LSM changes.
- Shard: LSM tree bound to a key range and a Raft region.
- SnapVersion: (base_version, data_sequence) used to tag file snapshots.
- L0: Level 0 SSTs, overlapping by key range.
- L1+: Non-overlapping SST levels.
- Columnar: Derived analytic storage built from row data.
- Vector index: Derived ANN index built from columnar data.
- IA: Infrequent-access tier using metadata-only local files.
- TxnFile: Lock data stored outside the main LSM tree.
