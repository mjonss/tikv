# native_br Maintainer Guide

This document is for maintainers of the `components/native_br` crate.
It explains the complex flows, invariants, and design tradeoffs that are
not obvious from reading the code alone.
It intentionally skips trivial field lists and one-to-one code mappings.

## Scope and responsibilities

native_br provides TiKV native backup and restore logic used by the cloud
storage engine and related tooling.
It covers:
- Lightweight (incremental) backup orchestration.
- Full restore (TiKV + PD metadata) from lightweight backups.
- Keyspace-level restore with range alignment and region reshaping.
- WAL capture and replay, including online chunk fallback.
- Archive packaging of cold backup data for low-cost storage.
- Restore throttling and data-size calibration.

It does not implement the low-level storage engines themselves.
Instead, it orchestrates rfengine, kvengine, PD/etcd, and S3-compatible
object storage via HTTP and gRPC entrypoints.

## Big picture: data and control flow

```
        +-----------------+         +------------------+
        | PD / etcd       |         | TiKV stores      |
        | (cluster meta)  |         | (rfengine, kv)   |
        +--------+--------+         +--------+---------+
                 |                           |
                 | lightweight backup RPC    | /rfengine/backup
                 | (meta, GC safepoint)      |
                 v                           v
            +----+-------------------------------+
            | S3-compatible object storage       |
            | - backup/*.meta                    |
            | - store_backup/... (rlog/WAL data) |
            | - archive/* (packaged cold data)   |
            +----+-------------------------------+
                 ^                           |
                 | restore (WAL + snapshots) |
                 | + keyspace restore HTTP   | /restore-shard
                 |                           v
        +--------+---------+         +-------+---------+
        | PD / etcd        |         | TiKV stores     |
        | (restore meta)   |         | (apply snapshot)|
        +------------------+         +-----------------+
```

Two paths dominate maintenance work:
- **Lightweight backup** produces metadata and WAL/rlog data for each store.
- **Keyspace restore** reconstructs a target keyspace by replaying WAL,
  aligning backup shards to live regions, and restoring snapshots.

## Glossary and mental model

The terms below are used consistently across modules.
They are the minimum set you need to keep a correct mental model.

### Time markers
- **backup_ts**: The timestamp at which a backup is taken.
  This is the upper bound of data included in the backup.
- **safe_ts**: The GC safe point at backup time.
  A restore should not use data older than this without extra checks.
- **truncate_ts**: The target time for PITR-like restore.
  Must satisfy `safe_ts <= truncate_ts <= backup_ts`.

### Data artifacts
- **Snapshot meta / rlog**: rfengine snapshot metadata and raft log payload
  that bootstrap the raft engine during restore.
- **WAL chunk**: A compressed segment of WAL with `[start_off, end_off)`.
  WAL chunks are stored per epoch and may be incomplete in the last epoch.
- **Online WAL chunk**: A tail segment fetched directly from a TiKV store
  when the latest WAL chunks have not reached object storage yet.

### Topology and range concepts
- **Region**: PD-level region metadata (`metapb::Region`).
- **Shard**: kvengine/rfengine unit (mapped 1:1 to region in restore logic).
- **Keyspace range**: The byte range for a keyspace, including the prefix.
- **Inner key**: The logical key range inside a keyspace prefix.
  Alignment logic uses inner keys to compare ranges between keyspaces.

### Backup metadata
- **ClusterBackupMeta**: Top-level metadata for a backup.
  Includes backup_ts, safe_ts, store list, and keyspace metadata.
- **StoreBackupMeta**: Per-store metadata (snapshot epoch, WAL offsets,
  wal_chunks list, and per-keyspace sizes).

## External systems and APIs

native_br depends on several external APIs.
The interfaces and failure modes shape many design choices.

- **PD (gRPC)**
  - Get TSO, safe point, cluster ID, store list, region info.
  - Control endpoints for splitting and scattering regions.
  - Keyspace metadata and placement rules through PD control.

- **etcd (gRPC)**
  - Used for backing up keyspace metadata (raw key-value pairs).
  - Used for restoring PD state in a fresh cluster.

- **TiKV status HTTP**
  - `/rfengine/backup`: trigger store-side lightweight backup.
  - `/rfengine/wal_chunk`: fetch online WAL chunk (tail of epoch).
  - `/restore-shard`: restore snapshot data to a region.
  - `/kvengine/files_with_type`: list file IDs by type for throttling.
  - `/tiflash/sync-region/keyspace/<id>`: check TiFlash replica status.

- **S3-compatible object storage**
  - Stores backup meta, WAL chunks, and archive packages.
  - Accessed through `kvengine::dfs::S3Fs` with optional object cache.

## Module map (what is complex vs obvious)

You will spend most of your time in a few modules.
This is the only "map" section; details are elsewhere.

- `backup.rs` and `backup_worker.rs`:
  Complex orchestration, safe point updates, and batching logic.
- `restore_keyspace.rs`:
  The most complex flow: shard selection, preprocess, lock resolution,
  alignment, snapshot generation, and restore retry.
- `common.rs` and `wal.rs`:
  WAL chunk collection, integrity checks, online chunk fallback, and
  WAL assembly into a replayable stream.
- `archive.rs`:
  Packaging and index format for cold backups and file restoration.
- `lock.rs`:
  Lock resolution and transaction status reconstruction for correctness.
- `limiter.rs` and `tikv.rs`:
  Throughput limiter and store file existence cache.
- `rfengine_cache.rs`:
  Cache of pre-built raft engines for fast keyspace restore.

Simple or mechanical modules (`metrics.rs`, `error.rs`) are not expanded
beyond their interactions with complex flows.
## Lightweight backup: correctness and operational behavior

Lightweight backup is the "source of truth" for restore flows in this crate.
It is intentionally conservative in time and topology handling to avoid
silent data loss.

### High-level sequence

```
backup_worker -> backup_cluster -> backup_stores -> /rfengine/backup
             \-> backup_pd_keyspace_meta (etcd)
             \-> write ClusterBackupMeta to S3
             \-> update GC service safe point
```

Key design choices:
- **Backup_ts from PD**: The backup timestamp is anchored on PD's TSO
  so all stores share a consistent time boundary.
- **GC safe point update after success**: The safe point is only moved
  after the backup completes, so failed backups do not block GC.
- **Incremental keyspace meta**: PD keyspace metadata is backed up
  incrementally by revision to avoid full scans and preserve history.

### backup_cluster and store fan-out

`backup_cluster` drives the core flow and writes the meta file.
Important logic that often surprises maintainers:
- **Service safe point update** happens after a successful backup.
  This means the first backup is not immediately usable for restore,
  because the safe point was not set before it.
- **Tolerated store errors** are allowed to a configured limit.
  The backup meta records tolerated stores to inform later restore logic.
- **Missing stores are detected after fan-out** by comparing PD store
  list with the stores recorded in the meta.

`backup_stores` handles concurrency and error tolerance:
- Each store is contacted via `/rfengine/backup`.
- Errors are aggregated; if missing stores are within tolerance,
  the backup proceeds and records them.
- On retry, the missing stores list is narrowed to improve success rate.

Why tolerate store errors at all?
- Lightweight backup is often run frequently.
- A single store can be transiently unavailable.
- It is better to succeed with metadata that declares partial coverage
  than to fail and block all backup progress.

### backup_pd_keyspace_meta and etcd behavior

Keyspace metadata is stored in PD's etcd space.
native_br intentionally backs up **raw key-value pairs**, not parsed data:
- Parsing PD's internal formats would require tight coupling to PD versions.
- Raw KV preserves fidelity and makes restore safe across versions.

Incremental behavior is driven by `meta_revision`:
- Only keys with modification revision >= last revision are included.
- Deleted keys are not special-cased because keyspace meta is append-only
  in practice for this deployment model.

Why use several explicit PD paths?
- Keyspace metadata spans several logical features (rules, labels, groups).
- The code enumerates known prefixes to ensure completeness without
  relying on external PD helpers.

### Backup naming and conflict avoidance

Incremental backup files are named by UTC second.
Two subtle safeguards exist:
- `backup_worker` enforces `MIN_BACKUP_INTERVAL` to avoid same-second names.
- Batches are merged so requests within the same window share the same backup.

If you see repeated "backup_ts conflict" logs, the batching logic is working
as intended and intentionally requeues those requests.

### Backup worker batching and delay

The worker exists because callers need a cheap "instant backup" API.
Its behavior is more nuanced than it looks:
- **batch_interval** controls when a batch is collected.
- **backup_delay** defers execution to allow WAL flush to complete.
- **periodic_backup_interval** injects periodic requests into the queue.

Why introduce a delay at all?
- The backup RPC captures a boundary, but the latest WAL chunks
  may still be in flight to object storage.
- A short delay reduces the risk of having to fetch online WAL
  for the last epoch, which would slow restore.

### Failure classification and retriable errors

`backup_store` treats rfengine DFS worker health specially.
If the store reports a DFS worker unhealthy, the error is surfaced
so that higher-level logic can decide to tolerate or retry.

In the worker:
- Errors are returned to all batched requests.
- The worker itself keeps running; it is meant to be long-lived.

## Full restore: store and PD recovery

Full restore is less frequent but has strict correctness requirements.
It rehydrates the raft engine, replays WAL, and reconstructs PD metadata.

### Store ID remapping

The restore path always assumes a new cluster.
Store IDs in the backup must be remapped to avoid collisions:
- The new base is derived from the **latest** backup's alloc_id.
- A user-provided delta is added to avoid conflicts with post-backup stores.
- The mapping is deterministic: sorted store IDs map to increasing IDs.

Why is this needed?
- PD's alloc_id is monotonically increasing.
- A restored cluster should not reuse IDs that might still exist in PD
  or in object storage, or it can accidentally reuse old artifacts.

### setup_raft_engine in full restore

`restore_tikv` calls `setup_raft_engine` with `full_restore = true`:
- The rfengine snapshot (meta + rlog) is restored first.
- WAL for each epoch after the snapshot is replayed.
- Store and region metadata are rewritten with the new store IDs.

Key correctness checks:
- The snapshot epoch used is the latest valid snapshot before backup.
- WAL replay is bounded by backup_offset to avoid reading beyond backup_ts.

### PD metadata restore

`restore_pd` is intended for a **fresh** PD cluster.
It uses etcd transactions to guarantee atomic bootstrap:
- Writes cluster_id, alloc_id, and cluster meta in a single txn.
- Uses `create_revision == 0` to ensure the PD cluster is empty.

After bootstrap, keyspace metadata is written in batches because etcd
lacks a true bulk put API.

Design rationale:
- The check protects against accidental restore into a live PD cluster.
- The new alloc_id is offset beyond all known store IDs to avoid reuse.

## Keyspace restore: the most complex path

Keyspace restore is the main reason this crate exists.
It reconstructs a **subset** of the data by keyspace range, and
can optionally restore into a different target keyspace.

### Restore pipeline (major steps)

The steps are reported through `RestoreStep` in strict order.
The core loop is:

```
Init -> LoadBackupMeta -> ExtractBackupShards
-> ResolveLocks -> FlushShards -> TruncateTs
-> SplitRegions -> AlignRegions
-> RestoreSnapshotsToServers -> RetainSstFiles -> Finalize
```

Each step is idempotent or retried with backoff.
Only a few steps can safely fail fast (e.g., invalid backup range).

### In-place vs cross-keyspace restore

Two modes are supported:
- **In-place**: restore into the same keyspace ID.
- **Cross-keyspace**: restore into a different keyspace ID.

Cross-keyspace restore rewrites the keyspace prefix in range boundaries
and shard metadata.
It is intentionally blocked for the default keyspace to avoid
catastrophic misrouting of data.

### BackupCluster: shared state and invariants

`BackupCluster` is the workhorse that owns:
- A set of rfengine instances per store.
- A shared kvengine instance used for lock resolution and flushing.
- Shard metadata (range, files, raft state, and properties).
- Derived sets (shards needing flush, truncate, or columnar reset).

Important invariants:
- `sorted_shards` must cover the keyspace range without gaps.
- `raw_metas` is consumed when initializing kvengine and must be
  reconstructed if the keyspace changes.
- `shards_need_flush` and `shards_need_truncate` are computed only
  after shards are loaded and raft state is known.

### Shard selection and leader choice

Shard selection uses rfengine state directly rather than PD metadata.
Reasons:
- Backup is taken from store-local rfengine snapshots.
- PD metadata may be stale or missing in a partial restore scenario.

Leader selection uses raft state:
- The leader is chosen as the peer with max `(term, last_index)`.
- Store ID is a deterministic tie-breaker to avoid retry loops.
- If the chosen leader's commit index is behind its last index,
  commit is forced forward to allow preprocessing.

Why force commit forward?
- In tolerated-error scenarios, the true leader may be missing.
- The most up-to-date follower is still a safe replay source when
  its last index is committed in practice.

### Preprocessing raft logs

Preprocessing applies committed raft entries to rebuild in-memory
metadata that is not fully represented in the snapshot:
- `preprocess_shard` replays raft entries from `data_sequence + 1` to
  `commit + 1`.
- The resulting meta may include split or merge changes.
- If a split is detected (range changed), the shard set is reloaded
  to avoid complex in-place reconciliation.

This is intentionally conservative:
- Splits and merges are rare at the exact boundary of backup.
- Reloading provides a consistent view at the cost of extra I/O.

### Overlapping shards and the intact chain

Shard ranges can overlap when followers lag during split/merge.
The restore logic models shards as edges in a DAG:
- Nodes are inner start/end keys.
- A valid restore path is a chain from keyspace start to end.

`find_intact_shards` walks this DAG to find a continuous coverage.
If no chain exists, restore fails early.
This avoids restoring a keyspace with gaps or overlaps.

### Lock resolution and transaction correctness

After shard extraction, **all locks are resolved** before truncation.
This is crucial and easy to get wrong:
- Lock CF cannot be truncated by timestamp.
- A committed primary key without its secondary locks can be
  compacted away after restore, causing data loss.

Lock resolution has two parts:
- **Normal locks** in LOCK CF are committed or rolled back.
- **Txn file locks** are handled using `TxnFileRef` and custom logs.

Transaction status checks are expensive and cached:
- `TxnStatus` caches commit_ts per transaction using a DashMap.
- It reads primary lock and writes using `CloudReader` on snapshots.
- For async commit, it inspects secondary locks to compute commit_ts.

Concurrency pitfalls:
- `apply_custom_log_in_recover` conflicts with `load_unloaded_tables`.
- `ApplyLocks` enforces a per-shard mutex to prevent races.

Why use CloudReader instead of direct engine reads?
- It respects MVCC semantics and provides uniform access to locks
  and commit records across sharded storage.

### Flushing memtables and meta application

Some shards require flush because:
- Their raft last index exceeds snapshot data_sequence.
- They are derived from a parent snapshot (initial flush needed).

Flush logic:
- Each shard is flushed via kvengine to persist memtables.
- `MetaApplier` listens for change sets and applies them to shard meta.
- After flush, shard metadata is taken back from the applier.

Why a separate MetaApplier thread?
- The kvengine emits change sets asynchronously during flush.
- The applier serializes metadata updates and captures errors.

### Truncation to truncate_ts

Truncation is applied to shards where `max_ts >= truncate_ts`:
- It uses kvengine's `truncate_with_ts` and applies the returned
  change set to shard metadata.
- This is the main mechanism enabling PITR within a keyspace.

Truncation happens after lock resolution to avoid dropping locks
that are needed to resolve committed transactions.

### Region pre-splitting and scatter

Before snapshot restore, target regions are split to match backup shards.
Why this is needed:
- Restoring a large shard into a large region is expensive and can
  overload a single store.
- Region splits improve parallelism and reduce per-region restore cost.

The logic uses two levels:
- **Coarse split** at a configurable factor to avoid too many splits.
- **Fine split** for the remaining boundaries.

Scatter is best-effort:
- Failure to scatter is not fatal but may lead to temporary hotspots.

### Alignment of backup shards to target regions

Alignment maps backup shards onto the **current** target regions:
- Both are sorted by start key.
- Inner-key comparison handles cross-keyspace restore correctly.
- A region may map to multiple shards, or a shard may cover multiple regions.

Alignment is repeated on retry because region layout can change between
attempts (splits, merges, leadership changes).

### Snapshot generation and schema handling

For each target region, a new `ShardMeta` is constructed:
- File lists are filtered by overlap with the target range.
- Properties are merged with a helper to keep metadata consistent.
- Schema files are rewritten with `restore_version` and target keyspace ID.

Why rewrite schema files?
- Schema versioning is used to guard columnar and indexing metadata.
- A restore must produce a consistent schema view for the target keyspace.

Columnar and vector index metadata:
- Columnar L0 lists are filtered to only include files in range.
- Vector index files are grouped by table/column/index ID.
- Missing columnar or vector files can trigger a reset to avoid
  restoring inconsistent metadata.

### Snapshot restore requests

Each snapshot is sent to the **current leader** of the region:
- The region's epoch is verified before sending.
- The restore request is retried until timeout.
- Disk-full errors are detected and trigger region scatter + retry.

Concurrency is controlled by a semaphore to limit in-flight requests.
This prevents excessive memory and network usage.

### Throughput throttling (optional)

If enabled, the restore process estimates the size of each snapshot
and uses a limiter to throttle total restore throughput.
Two estimation paths exist:
- **Local estimate**: sum sizes from snapshot metadata.
- **Calibrated estimate**: consult TiKV stores for missing files
  when total size exceeds a threshold.

Calibration is designed to be conservative:
- If a store does not respond, all files are considered missing.
- This slows down restore, which is safer than overloading a cluster.

### Retaining restored files in object storage

After a successful restore, all referenced SST/metadata files are
explicitly "retained" in object storage.
This is a guard against external cleanup policies that might
otherwise delete files still needed by TiKV.

## WAL collection and replay

WAL handling is one of the most subtle parts of native_br.
It must bridge object storage, online store state, and rfengine replay
without gaps or overlaps.

### WAL epochs and chunk integrity

WAL data is organized by epoch.
For each epoch, WAL chunks are expected to be contiguous:
- Each chunk has `[start_off, end_off)`.
- The last chunk is marked with `last = true`.

`collect_wal_chunk_metas` enforces integrity:
- If `end_off == u64::MAX`, the last chunk must exist.
- If `end_off == 0`, continuity is required but "last" is not.

Why two modes?
- The last epoch may still be writing to DFS.
- Earlier epochs should be complete and verified strictly.

### Online WAL chunk fallback

The most error-prone case is the **last epoch**:
- DFS may not yet contain the tail of WAL.
- The restore still needs a complete WAL to replay up to backup_ts.

The fallback strategy is:
1. List chunk files from DFS.
2. If the last offset is short, fetch the tail directly from the store
   via `/rfengine/wal_chunk`.
3. If the store reports "epoch overwritten", wait for DFS to catch up
   and retry using DFS data only.

This handles the race between async WAL writer and DFS worker.
The design intentionally prefers DFS when possible to avoid
stale or overwritten online WAL.

### WAL assembly: memory vs local files

WAL chunks can be assembled in multiple modes:
- **In-memory** for normal restore.
- **Local files** when `lower_memory` is enabled.
- **Local chunks with cache hook** when object cache is used.

The assembly path is selected by `assemble_wal_chunks` based on:
- Whether chunks are memory or local files.
- Whether object cache is enabled (for decompressed cache mode).

Why support local-file mode?
- Some restores require replaying large amounts of WAL.
- Local files reduce memory usage at the cost of disk I/O.

### WAL readers and partial range reads

`LocalWalChunksReader` provides a streaming view of WAL:
- It lazily loads and decompresses each chunk.
- It supports reading a partial range `[start, end)`.
- Chunks are closed and released as the reader moves forward.

This is critical for large restores where WAL does not fit in memory.

### Replay into rfengine

Replay uses the rfengine API:
- The WAL reader is passed to `replay_wal_file`.
- For the last epoch, replay is truncated at `backup_offset`.
- Earlier epochs replay the full WAL range.

Full restore uses `full_restore = true`.
Keyspace restore uses `full_restore = false`, which allows rfengine
to apply keyspace-aware replay when supported (keyspace IDs are
provided during lightweight restore).

## Archive packaging for cold backups

Archive support exists to reduce object count and move cold backup
data to lower-cost storage classes.
It is intentionally separated from the main backup flow.

### Design overview

The archive process:
- Iterates daily backups in order.
- Computes **deleted files** between consecutive days.
- Writes deleted files and WAL/rlog into compact packages.
- Emits an index describing object addresses.

Why archive deleted files only?
- Unchanged files remain referenced by newer backups.
- Archiving only deleted files minimizes storage and avoids duplication.

### Archive index format and object addresses

Archive objects are addressed by `(package_id, offset, length)`.
The index supports two versions; v2 is the current format:
- v1: meta address + table file addresses.
- v2: meta address + per-store rlog/WAL addresses + table file addresses.

Store metadata includes:
- Snapshot meta and rlog addresses.
- A list of WAL epochs and chunk addresses.

Why store WAL separately?
- WAL is needed for replay and must be accessed by epoch.
- Keeping WAL addresses grouped enables streaming restore.

### ArchiveWriter behavior

`ArchiveWriter` appends objects into a buffer until it reaches
`max_archive_file_size`, then rotates to a new package:
- The first object is always the cluster backup meta.
- Each store's rlog/WAL data is appended before table files.
- Packages are stored in a cold storage class when supported.

Rotation is **not** optional:
- Keeping packages reasonably sized avoids huge object downloads
  during restore and fits within S3 request limits.

### ArchiveReader behavior

`ArchiveReader` builds an index map from the start date onward:
- The start date's index provides the meta address and store list.
- Later indices may provide more recent table file addresses.

Restoration uses the map as a lookup table:
- If a file is missing from the normal backup path, restore from archive.
- This is used in keyspace restore when older backups were archived.

Columnar files are special:
- If columnar or vector index files are missing, restore resets
  columnar metadata to avoid inconsistent state.

## rfengine cache for repeated restores

`RfEngineCache` accelerates repeated keyspace restores.
It caches per-store rfengine instances preloaded with backup data.

Key behaviors:
- Cache is only used when `truncate_ts` is within a conservative
  range `[max(backup_ts - 8min, safe_ts), backup_ts]`.
- Keyspaces must be registered before cache fill.
- Cache is rebuilt on newer backups; incremental updates are not
  supported yet.

Why use a conservative safe_ts?
- The safe_ts in backup meta may not reflect keyspace-level GC rules.
- Using `backup_ts - 8min` (and the recorded safe_ts if larger)
  reduces the risk of replaying data that has been GC'ed.

## Throughput limiter and store file cache

Throttling is optional but useful for large restores.
It keeps restore throughput under a configured maximum.

### Local size estimation

`ThroughputLimiter::estimate_snapshot_size_locally`:
- Sums L0, table, and blob sizes from snapshot metadata.
- For IA storage class, estimates table size by meta overhead only.

This is intentionally approximate.
Accuracy is improved by calibration when enabled.

### Calibration from TiKV stores

When the total estimated size exceeds a threshold:
- The restore queries TiKV stores for existing SST files.
- Only **missing** files are counted toward restore size.

`StoresFiles` caches store responses with a TTL.
If a store is unreachable:
- All files are treated as missing.
- The limiter slows down restore (safe but conservative).

Why tolerate missing store responses?
- The restore can continue even if one replica is down.
- Over-throttling is safer than under-throttling.

## Metrics that matter in practice

This crate exposes a small set of metrics that reflect real issues.

Backup:
- `native_br_backup_success`, `native_br_backup_error`.
- `native_br_backup_missing_commit_record`: backup consistency warning.
- `native_br_backup_batch_size`: batch sizes in backup worker.

Restore:
- `native_br_restore_error{type=...}`: categorized restore failures.
- `native_br_restore_pending_data_size`: throttler queue size.
- `native_br_restored_data_size`, `native_br_restored_kv_size`.

WAL and caching:
- `native_br_restore_rfengine_wal_epoch_overwritten_error`.
- `native_br_rfengine_cache_hit`, `native_br_rfengine_cache_miss`.

Interpretation hints:
- A rising WAL epoch overwritten count usually means DFS is lagging.
- A high cache miss rate suggests keyspace IDs are not registered
  before cache fill, or truncate_ts is out of range.

## Operational guidance and failure modes

This section is intentionally opinionated.
It is where most maintenance mistakes happen.

### Time boundaries and safety

- Always validate `truncate_ts` against `safe_ts` and `backup_ts`.
- For archive-based restore, `truncate_ts` is forced to `backup_ts`.
- Avoid restoring data earlier than safe_ts unless you fully control
  GC configuration and can prove safety.

### Tolerated errors during restore

`RestoreConfig.tolerate_err` and `strict_tolerate` are dangerous knobs:
- Tolerance is only safe for errors that do not affect data completeness
  (e.g., transient store unavailability during WAL fetch).
- If backup already tolerated errors, restore refuses to tolerate more.

When in doubt, keep tolerance at zero and fail fast.

### Lower-memory mode and WAL cache

`lower_memory` and `cache_for_decompressed_wal_chunks` are useful
for large restores but change performance characteristics:
- Lower memory mode writes WAL chunks to local disk.
- Decompressed cache can increase CPU efficiency but uses memory.

Always consider local disk capacity when enabling these options.

### Region split strategy

`coarse_split_regions_factor` trades off:
- Fewer splits (faster pre-split) vs larger restore units.
- More splits (slower pre-split) vs better restore parallelism.

Use higher values for large keyspaces to avoid split storms.

### TiFlash replica removal

In-place keyspace restore removes TiFlash replicas first:
- TiFlash may lag or keep stale data.
- The restore waits for TiFlash to acknowledge removal before proceeding.

If this step times out, investigate TiFlash availability and
placement rule configurations.

## Debugging and observability

### Step reporting

`step!` and `step_error!` emit structured log lines.
Set `STEP_TO_STDOUT` to mirror these to stdout for CLI tooling.

Use this when debugging interactive restore flows.

### Failpoints

Failpoints exist in backup store paths (e.g., `native_br::backup_store`).
Use them to simulate store failures or slow responses.

### Logs and crash analysis

The repository-level tip applies here too:
- Use `LOG_FILE=/tmp/test.log` to capture panics.
- Some panic hooks may call `_exit(1)` without flushing logs.

## Testing and validation

This crate has unit tests and depends heavily on integration tests.
Suggested workflows:
- `cargo test -p native_br` for unit tests.
- `cargo test -p native_br --features failpoints` for failpoint coverage.
- `cargo test -p tests --test cloud_engine` for integration flows.

When changing restore logic, also consider running:
- Targeted nextest runs for keyspace restore tests.

## Appendix: data layout in object storage

This is a conceptual layout.
Do not treat it as a contract for external tools.

```
<dfs-prefix>/backup/<YYYYmmdd>/<HHMMSS>.meta
<dfs-prefix>/store_backup/<store_id>/...   (rfengine data)
<dfs-prefix>/archive/index/<YYYYmmdd>.idx
<dfs-prefix>/archive/package/<YYYYmmdd>/<pkg>.pack
```

Paths are constructed by `S3Fs` and helpers in `backup.rs` and `archive.rs`.

## Appendix: error taxonomy (what to expect)

A few error classes are particularly important:
- **TopoChanged / MetaNotFound**: typically requires full backup.
- **WalChunkIntegrityError**: often a DFS lag or partial upload.
- **RegionVerNotMatch / RegionNotFoundOrNoLeader**: retriable,
  caused by region movement during restore.
- **StoreDiskFull**: triggers region scatter and backoff.

Errors are deliberately bubbled with context so callers can choose
whether to retry, tolerate, or abort.

## Appendix: invariants to protect when changing code

If you change any of these, re-evaluate correctness carefully:
- A backup must not advance GC safe point before it is fully written.
- WAL replay for the last epoch must not exceed backup_offset.
- A restore must not mix shards from different keyspace ranges.
- Lock resolution must happen before truncate_ts is applied.
- Shard ranges must cover the keyspace range without gaps.
- Schema files must be rewritten when keyspace ID changes.

Violating these invariants can lead to silent data loss.
