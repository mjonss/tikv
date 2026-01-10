# RfEngine Maintainer Guide

This document is for maintainers of the `rfengine` component in TiKV.
It focuses on design intent, invariants, and failure/recovery behavior.
It intentionally avoids repeating APIs and obvious struct layouts in code.
If you need API details, read the source in `components/rfengine/src`.

## Scope and assumptions

- This guide describes the current on-disk format and background workers.
- The primary goal is correctness, durability, and operability, not brevity.
- All statements are derived from the current code, not assumptions.
- All paths mentioned are relative to the rfengine data directory.

## What rfengine is (and is not)

- RfEngine is a persistent storage engine for multi-raft logs and states.
- It maintains a fast in-memory view of per-peer raft logs and state keys.
- It persists write batches to a WAL and compacts WAL epochs into rlog files.
- It can optionally ship WAL chunks and snapshots to object storage.
- It is not a general KV engine for user data; it only stores raft metadata.

## Mental model in one page

RfEngine has three layers of durability and access:

- In-memory layer:
  - Per-peer raft logs and state keys live in `RaftPeers`.
  - Reads come from this layer; it is authoritative after recovery.
- Local durable layer:
  - A write-ahead log (WAL) stores write batches in epoch-rotated files.
  - WAL is compacted into immutable rlog files and a MANIFEST change set.
- Optional remote durable layer (lightweight backup):
  - Async WAL writer feeds a DFS worker that uploads WAL chunks to S3.
  - Periodic snapshots upload manifest + rlog data to object storage.

Think of the WAL as the short-term journal, rlog files as long-term segments,
and the MANIFEST as the index tying everything together.

## Directory and module map (what to read first)

- `engine.rs`:
  - Public engine entry points, write/apply/persist flow.
  - Lifecycle: open, load, close, and worker wiring.
- `writer.rs`:
  - WAL file format, alignment, rotation, and double-writer logic.
- `iterator.rs`:
  - WAL iteration and integrity checking (checksums, compression).
- `write_batch.rs` and `log_batch.rs`:
  - Custom batch encoding and raft log op serialization.
- `peers.rs`:
  - In-memory peer data, read path, and truncation behavior.
- `compact_worker.rs`:
  - WAL compaction to rlog files and MANIFEST updates.
- `manifest.rs`:
  - Change set storage, replay, and rlog GC.
- `dfs_worker.rs`:
  - Lightweight backup, WAL chunking, snapshot state machine, health.
- `utils.rs`:
  - Key encoding, filenames, chunk key parsing, and helper algorithms.

Each module is intentionally cohesive; cross-module invariants are called out
in the sections below.

## Core concepts and invariants

### Peer, region, keyspace

- A `peer_id` identifies a raft peer; it is unique within the store.
- Each peer belongs to a `region_id` and optionally a `keyspace_id`.
- Keyspace is derived either from stored state or from region meta keys.
- If `keyspace_id` is missing, the keyspace is inferred from region state.

Invariant:
- A peer's in-memory `keyspace_id` is updated by manifest or state keys.
- Keyspace is used for snapshot grouping and restore filtering.

### Truncated index and tombstones

- `truncated_idx` is the smallest index that may still be retained.
- `TRUNCATE_ALL_INDEX` (u64::MAX) is a tombstone for a destroyed peer.
- A tombstoned peer is excluded from snapshots and region mapping.

Invariant:
- Truncation only moves forward unless a special restore path resets it.
- A tombstone peer should never be resurrected without a restore path.

### Dependents (split safety)

- `add_dependent(region_id, dependent_id)` registers a dependency edge.
- `remove_dependent` removes that edge once the dependent is safe.
- `has_dependents` and `with_dependents` allow callers to gate truncation.

Rationale:

- After a split, the old region's raft log must be retained until the new
  region is flushed or destroyed.
- RfEngine does not automatically enforce this; it provides the accounting
  so raftstore can make safe truncation decisions.

### Epochs

- WAL files are grouped into logical epochs.
- Epoch rotation is configured by `epoch_rotate_len` and WAL size.
- Epoch IDs increase monotonically; WAL files are reused by modulo index.

Invariant:
- A WAL epoch can only be overwritten once compaction has advanced.
- Rotation must not overwrite epochs that are not yet compacted.

### Manifest change sets

- The MANIFEST is a sequence of protobuf `ChangeSet` records.
- Each change set includes the epoch, delayed epochs, and peer metadata.
- Manifest replay rebuilds peer meta, truncated indexes, and rlog files.

Invariant:
- Change set epochs are strictly increasing.
- A rewrite replaces the whole file but preserves the same metadata state.

## On-disk and remote layout

### Local files

```
<rfengine_dir>/
  LOCK
  MANIFEST
  <idx>.wal              # WAL files (idx = epoch_id % epoch_rotate_len)
  <peer>_<first>_<last>.rlog
  upgrade_mark           # Only during wal_sync_dir migration
```

- WAL file names are stable indexes; they are reused cyclically.
- Rlog files are immutable and named by (peer_id, first_index, last_index).
- `LOCK` prevents concurrent opens of the same data directory.
- `upgrade_mark` exists only during WAL directory upgrade.

### WAL sync directories

- If `wal_sync_dir` is set, the sync WAL is written there.
- If `wal_secondary_dir` is set, double writers run for the sync WAL.
- The async WAL (if enabled) is stored in the main rfengine directory.

Upgrade note:

- When `wal_sync_dir` is enabled for an existing store, rfengine copies the
  pre-created WAL files into the sync directory.
- An `upgrade_mark` file makes the copy idempotent in case of interruption.

### Remote object storage (lightweight backup)

Object keys are generated in `utils.rs` and are stable API surfaces.

- Snapshot meta:
  - `store_backup/<store_id>/snapshots/m<epoch>.meta`
- Snapshot rlog object:
  - `store_backup/<store_id>/snapshots/r<epoch>.rlog`
- WAL chunks:
  - `store_backup/<store_id>/wal_chunks/e<epoch>_<start>_<end>.wal`
  - `... .wal.last` suffix means the last chunk of an epoch.

These keys are parsed and validated during restore and repair operations.

## Write path: from WriteBatch to WAL

### Why custom batching exists

RfEngine uses a custom batch format to avoid protobuf overhead and to preserve
stable, deterministic ordering across peers within a batch.
This reduces CPU cost and avoids the unpredictable layout of protobuf encoding.

### Write flow (sync writer + optional async writer)

```
Raft IO thread
  -> build WriteBatch (per peer)
  -> RfEngine::apply()  (in-memory update)
  -> RfEngine::persist()
       -> sync WAL write (direct I/O, aligned)
       -> if async WAL enabled: ServiceTask::Write
             -> async WAL write (buffered I/O)
             -> optional DFS worker sync
```

Key points:

- `apply` is logically separate from `persist` to enable async IO.
- `apply` returns truncated log blocks to be dropped in the worker thread.
- `persist` returns bytes written (or a synthetic size on rotation).

### WriteBatch and PeerBatch encoding

Write batches store per-peer entries and states to minimize cross-peer lock
contention and preserve per-peer invariants during apply.

A `PeerBatch` encodes:

- peer_id, region_id, truncated_idx
- first/last raft log index in the batch
- state key/value pairs (as BTreeMap entries)
- a compact index array of log end offsets
- raw RaftLogOp bytes (custom format) for each log

The log end offsets allow parsing without storing per-log lengths.
This keeps the format compact and allows zero-copy slicing during decode.

### RaftLogOp encoding rationale

`RaftLogOp` serializes an `eraftpb::Entry` in a custom format:

- Index (u64)
- Term (u32)
- Entry type (u8)
- Proposal context (u8)
- Raw data bytes

Rationale:

- Protobuf encoding is CPU heavy for very small messages.
- The entry type and context are fixed-size and fit in one byte each.
- Custom encoding enables fast, deterministic decode in compaction and load.

### WAL file format (local, aligned)

WAL files are direct-I/O aligned to 4KiB pages.
Every batch is padded to alignment, and an EOF marker (zero header) follows it.

```
WAL file
  +-------------------------------+
  | WalHeader (4KiB aligned)      |
  |   magic (u64)                 |
  |   version (u64)               |
  |   epoch_id (u32)              |
  |   padding                     |
  +-------------------------------+
  | BatchHeader                   |
  |   epoch_id (u32)              |
  |   checksum (u32)              |
  |   payload_len (u32)           |
  +-------------------------------+
  | BatchPayload                  |
  |   compression_flag (u32)      |
  |   [orig_len (u32), if compressed] |
  |   [compressed bytes, if compressed] |
  |   [raw bytes, if not]         |
  +-------------------------------+
  | padding to 4KiB               |
  +-------------------------------+
  | EOF header (all zeros)        |
  +-------------------------------+
```

- The checksum covers the payload only, not the header.
- Compression flag is `0` (no compression) or `1` (lz4).
- EOF header is used to detect the last valid batch during recovery.

### WAL compression

- Compression is per batch, not per peer.
- Compression is enabled if batch size >= `batch_compression_threshold`.
- LZ4 is used because it balances CPU and compression ratio.
- The uncompressed length is stored before the compressed bytes.

Rationale:
- Large raft entries (e.g., snapshots) benefit significantly from compression.
- Small batches skip compression to avoid CPU overhead.

## Epoch rotation and backpressure

### Rotation model

- WAL files are pre-created and reused by index (modulo rotation length).
- An epoch rotates only on the sync writer after exceeding target size.
- The async writer never rotates itself; it rotates on sync writer signals.

Rationale:
- Pre-creating files avoids directory fsync on every rotation.
- Async writer must follow sync writer epoch to keep DFS state consistent.

### Rotation safety

An epoch can only be overwritten when compaction has advanced far enough.
The safety check is:

```
compacted_epoch + epoch_rotate_len > current_epoch
```

If not safe:

- The writer waits (sleep loop) until compaction catches up.
- Write throttle metrics record this backpressure.

### Write throttling

When compaction lags but rotation is not yet unsafe, the writer throttles:

- Throttle if `compacted_epoch` is close to `current_epoch`.
- Duration increases when lag is larger.

Rationale:
- This slows the write path before a hard stop is needed.
- It provides a window for compaction to catch up.

## Sync and async WAL writers

### Sync writer

- Always active.
- Uses direct I/O and aligned buffers (`DmaBuffer`).
- Writes to `wal_sync_dir` if configured, else to the engine directory.
- Rotates WAL epochs based on file size and safety rules.

### Async writer

- Enabled only when `wal_sync_dir` is set.
- Writes to the engine directory (buffered I/O) for read and DFS usage.
- Syncs data every `ASYNC_WRITER_SYNC_SIZE` bytes.
- Rotates only when the sync writer signals a rotation.

Rationale:

- The sync writer uses optimized direct I/O for low latency writes.
- The async writer provides a buffered copy for recovery and remote sync.
- Separation avoids performance penalties on the sync path.

### Recovery with dual WALs

During load:

- The async WAL is scanned first to find the last clean offset.
- The sync WAL is then scanned and applied to rebuild in-memory state.
- Any batches beyond the async WAL offset are re-sent to the async writer.

This makes the sync WAL authoritative and tolerates async WAL corruption.

## Double writer (wal_secondary_dir)

When `wal_secondary_dir` is set, rfengine writes to two sync WALs.
The primary and secondary writers run in separate threads.

Behavior:

- Both writers receive the same write batch.
- A shared `total_size` tracks queued data size.
- If a writer falls behind more than `wal_double_write_unhealthy_size`,
  it is marked unhealthy and stops writing.
- The health state is exported as `raft_engine_double_write_healthy`.

Rationale:

- Double writing reduces tail latency by letting a faster path win.
- If one path slows down (e.g., degraded disk), it is automatically skipped.

Recovery and alignment:

- On startup, the two WAL directories are compared by epoch and offset.
- The slower directory is caught up by copying missing files or deltas.
- This preserves a consistent WAL view before new writes are accepted.

## In-memory data structures and read path

### RaftLogs and log blocks

In-memory raft logs are stored in `RaftLogs`, which is a deque of blocks.
Each `RaftLogBlock` holds a fixed-size deque of up to 255 entries.

Rationale:

- Dropping a large contiguous range of logs can be expensive in one thread.
- Smaller blocks allow bulk dropping in background workers with less pause.
- The block size aligns with VecDeque capacity behavior for efficiency.

### PeerData and PeerMeta

- `PeerData` holds raft logs and state keys for a peer.
- `PeerMeta` stores region id, truncated index, keyspace id, and state map.
- The state map is a `BTreeMap<Bytes, Bytes>` for ordered traversal.

Rationale:

- Ordered state traversal is needed for prefix scans and latest state lookup.
- `Bytes` keys allow cheap cloning for returns and snapshots.

### Read path invariants

- `get_term` and `get_raft_entry` read from in-memory logs only.
- `fetch_raft_entries_to` validates compaction and truncation boundaries.
- `get_last_state_with_prefix` relies on ordered BTreeMap iteration.

These APIs assume the in-memory layer is already consistent with the WAL.

## WAL compaction to rlog files

### Why compaction exists

- WAL is optimized for sequential writes and short retention.
- Long-term retention of WAL is costly and duplicates data across epochs.
- Compaction converts WAL batches into immutable rlog files per peer.

### Compaction trigger points

- A WAL epoch rotation triggers compaction of an earlier epoch.
- `delay_compaction_epoches` defers compaction to reduce rlog size.
- Truncated index updates are collected asynchronously.

Rationale:

- Delaying compaction allows more truncation information to accumulate.
- This reduces the amount of rlog data persisted for superseded entries.

### Compaction algorithm (high level)

```
Compact epoch N
  1) Iterate WAL file for epoch N
  2) Merge batches per peer into a WriteBatch
  3) Apply latest truncated indexes
  4) Write rlog files per peer
  5) Build ChangeSet and update MANIFEST
  6) GC obsolete rlog files
```

Step details:

- WAL iteration uses `WalIterator` and checks checksums.
- Peer batches are merged to keep only the latest state per key.
- Truncation is applied after merge to drop persisted logs.
- Rlog files are split by `rlog_file_size` for bounded object size.

### Rlog file format

Rlog files store raft log entries in a compact and addressable layout.

```
RLOG file
  +-------------------------------+
  | RlogHeader                    |
  |   magic (u64)                 |
  |   version (u64)               |
  |   count (u32)                 |
  +-------------------------------+
  | end_offsets[count] (u32...)   |
  +-------------------------------+
  | RaftLogOp + checksum          |
  | RaftLogOp + checksum          |
  | ...                           |
  +-------------------------------+
```

- Each log entry is followed by a crc32c checksum.
- The end offset array enables random access into the log list.

### Rlog file splitting

- The data portion of a file is limited by `rlog_file_size`.
- If a single entry exceeds the limit, it still forms a single file.
- This should be rare because raft entry size is bounded elsewhere.

### Rlog cache for snapshots

- Small rlog files can be cached in memory for snapshot assembly.
- Cache capacity and size threshold are configurable.
- Cached content is always overwritten to avoid stale data.

Rationale:

- Snapshot assembly would otherwise read many small files from disk.
- Cache reduces disk IO and speeds snapshot generation.

## MANIFEST design and rewrite policy

### What MANIFEST contains

The MANIFEST is a sequence of change sets that record:

- `epoch_id` and `delayed_epoches`
- Peer metadata: region id, keyspace id, truncated index
- Latest peer state key/value pairs
- List of rlog files per peer

`delayed_epoches` is used to compute `delayed_to_epoch` for snapshots:
`delayed_to_epoch = manifest.epoch_id + manifest.delayed_epoches`.

### Why a change log instead of a single snapshot

- Appending is faster and reduces fsync cost per compaction.
- The change log is easy to truncate on corruption (stop at last valid).
- Frequent full snapshots would be expensive at scale.

### Rewrite policy

- The MANIFEST is rewritten after `REWRITE_DIFF` epochs.
- Rewrite writes a full change set to a temporary file and renames it.

Rationale:

- This bounds MANIFEST size while keeping append-fast in steady state.
- A full rewrite also compacts away stale tombstones and old files.

### Rlog GC from MANIFEST

- After applying a change set, files whose `last_index <= truncated_idx`
  are removed.
- GC is logged with file sizes to a dedicated histogram.

Invariant:

- GC only occurs after a successful MANIFEST update.

## Service worker: concurrency and task flow

RfEngine uses a service worker to offload async WAL and background work.
This isolates IO-heavy tasks from the raft IO thread.

Tasks handled:

- `Write` (async WAL append)
- `Rotate` (trigger compaction and DFS rotation)
- `Dump` (read WAL chunk for HTTP and tools)
- `GetProgress` (async WAL progress)
- `Backup` (lightweight backup metadata)
- `Upload` (manual WAL chunk flush)
- `Close` (shutdown workers)

### Task ordering

- Tasks are processed sequentially in the service worker thread.
- This preserves WAL order and prevents cross-epoch interleaving.

### Async WAL sync granularity

- Async writer calls `sync_data()` after `ASYNC_WRITER_SYNC_SIZE` bytes.
- This bounds recovery window and keeps DFS uploads consistent.

## Lightweight backup and DFS worker

Lightweight backup is enabled when:

- `lightweight_backup = true`, and
- DFS is configured with S3 access, and
- The panic mark file is absent.

When enabled, the DFS worker uploads WAL chunks and periodic snapshots.
It exposes a health flag used by backup endpoints.

### WAL chunk format

A WAL chunk is a slice of an epoch WAL file.
Chunks are uploaded to object storage with a header and optional compression.

```
WAL chunk
  +-------------------------------+
  | ChunkHeader                   |
  |   version (u32)               |
  |   compression_type (u32)      |
  +-------------------------------+
  | compressed or raw WAL bytes   |
  +-------------------------------+
```

- The header is small and always present.
- Compression type is LZ4 or none.
- The chunk key encodes epoch, start offset, and end offset.
- The `.last` suffix indicates the final chunk for the epoch.

### WAL chunk assembly invariants

- Chunks must form a contiguous sequence from offset 0.
- If multiple chunks share the same start offset, the largest end wins.
- A missing chunk in the middle stops the sequence.

The helper `get_integral_wal_chunks` implements this greedy selection.

### DFS worker state machine

The DFS worker is modeled as a small state machine:

- `Idle`: no snapshot being processed.
- `Preparing`: waiting for compaction worker to prepare a snapshot.
- `WaitingWal`: snapshot prepared, waiting for WAL uploads to finish.
- `Persisting`: snapshot upload in progress.

This sequencing prevents snapshots from referencing missing WAL chunks.

### DFS worker initialization

On startup the worker:

1) Waits for the store id to be assigned.
2) Finds the latest snapshot in object storage.
3) Rebuilds chunk state for recent epochs by listing WAL chunks.
4) If chunks are too far behind and overwriting is imminent, it fails.

If initialization fails:

- The health flag remains unhealthy.
- A snapshot is requested to attempt recovery.

### Chunking and memory limits

- WAL data is buffered until it exceeds `wal_chunk_target_file_size`.
- Each chunk upload acquires memory from `MemoryLimiter`.
- If memory is exhausted, `MemoryLimitExceed` is returned and health degrades.

Rationale:

- Chunking keeps object sizes reasonable for S3 and retry behavior.
- Memory limiter prevents runaway upload concurrency and OOM risk.

### Handling rotation in DFS worker

When a sync writer rotates:

- The DFS worker may flush the current buffer as a `.last` chunk.
- It resets state to start the next epoch.
- If the epoch is a snapshot boundary, it requests a new snapshot.

`skip_sync_before_epoch`:

- When uploads fail, the worker marks itself unhealthy.
- It skips syncing epochs before the next snapshot boundary.
- This avoids overwriting already uploaded objects with partial data.

### Snapshot creation and persistence

The snapshot is prepared by the compaction worker:

- It uses the MANIFEST (excluding tombstones) to enumerate peers.
- It aggregates rlog files by keyspace into one rlog object.
- It appends a `StoreRaftLogBackupMeta` footer describing the object layout.
- The rlog object is capped at ~4.5 GiB to stay below S3 object limits.

The DFS worker then:

- Verifies all WAL chunks up to `delayed_to_epoch` are uploaded.
- Uploads snapshot meta first and rlog object last.
- Marks healthy only after a successful snapshot when all WAL uploads succeeded.

Rationale:

- Uploading rlog last makes the snapshot discoverable only when complete.
- The delayed epoch ensures snapshots reference a consistent WAL prefix.

## Backup and restore flows

### Lightweight backup (metadata only)

The backup endpoint returns a minimal `StoreBackupMeta` with:

- `store_id`
- current WAL epoch and offset

Other fields (e.g., keyspace sizing) are filled by higher-level components
outside rfengine.

If the DFS worker is unhealthy, the backup request fails with a specific error.
This surfaces the fact that WAL chunks or snapshots may be incomplete.

### Lightweight restore (snapshot + WAL replay)

`lightweight_restore` takes:

- store id, optional keyspace ids
- snapshot epoch, snapshot meta and rlog data
- epoch rotation length

Restore steps:

1) Recreate WAL files and write a new MANIFEST for the snapshot.
2) Rebuild rlog files from the snapshot rlog object.
3) Filter peers by keyspace if requested.
4) Return the restored epoch to allow WAL replay.

WAL replay:

- WAL chunks are decompressed and concatenated by epoch.
- `replay_wal_file` iterates batches and applies them to memory.
- Full restore persists WAL; keyspace restore may skip persistence.

### Keyspace filtering

Keyspace filtering relies on peer metadata or region local state.
During restore, peers not in the requested keyspaces are removed from the
manifest to reduce work and avoid loading irrelevant raft logs.

## Keyspace handling details

- `keyspace_id` is persisted in MANIFEST and in peer metadata.
- If a peer lacks explicit keyspace id, it is derived from RegionLocalState.
- Snapshot rlog data is aggregated by keyspace to allow targeted restore.

Invariant:

- A peer with missing keyspace info is treated as keyspace 0.
- This matches the behavior in `get_keyspace_id_from_peer` helpers.

## Failure modes and recovery behavior

### WAL corruption

- If a WAL header is unreadable and the file is empty, it is treated as EOF.
- If the last WAL file is corrupted, the corrupted tail is truncated to EOF.
- If a non-last WAL file is corrupted, recovery fails.

Rationale:

- Corruption in older epochs would break history consistency.
- Corruption in the current epoch is likely due to torn writes, so trimming
  to the last valid batch is acceptable.

### Async WAL corruption

- Async WAL corruption is tolerated by design.
- The sync WAL is authoritative; missing async data is rebuilt by replay.
- Corrupted async data is truncated, and the remainder is reconstructed.

### WAL epoch overwritten

- The service worker detects epochs that are too old to read safely.
- `WalEpochOverwritten` is returned to readers to avoid silent corruption.
- The DFS worker checks overwritten epochs before reading WAL data.

### Double writer degradation

- If a double writer falls behind, it stops accepting writes.
- `raft_engine_double_write_healthy` flips to 0.
- The remaining writer continues, preserving availability.

### DFS worker unhealthy

- Upload failures or missing chunks mark the worker unhealthy.
- Unhealthy state blocks lightweight backups.
- A successful snapshot (with all WAL uploads) restores health.

### Snapshot lag

- If snapshots fall behind more than `MAX_EPOCH_BACKWARD`, the worker
  refuses to continue and remains unhealthy.
- This prevents producing snapshots that cannot be correlated to WAL chunks.

### Rlog cache staleness

- Rlog cache entries are overwritten unconditionally to avoid stale data.
- If last_index mismatches are detected, the cache entry is ignored.

## Configuration and tuning guidance

This section focuses on non-obvious knobs and how they interact.
Defaults are in `config.rs`, but the intent is explained here.

### `epoch_rotate_len`

- Controls how many WAL files are pre-created and reused.
- Larger values allow longer retention before overwrite but add disk usage.
- Must remain in `[MIN_EPOCH_ROTATE_LEN, MAX_EPOCH_ROTATE_LEN]`.
- If WAL files already exist, rfengine adjusts the value to match their count.

Why it matters:

- Small values increase overwrite pressure and require faster compaction.
- Large values increase disk footprint and recovery scan time.

### `target_file_size`

- WAL rotation occurs when size exceeds this target.
- Smaller size yields more epochs and more frequent compaction.
- Larger size yields fewer rotations but bigger WAL scans on recovery.

### `delay_compaction_epoches`

- Defers compaction by N epochs to reduce rlog data size.
- Too large a delay increases WAL retention and overwrite pressure.

Rule of thumb:

- If compaction lags, reduce delay to avoid write throttling.

### `rlog_file_size`

- Limits per-file rlog data size during compaction.
- Too small: more files and higher metadata overhead.
- Too large: slower snapshot assembly and higher object sizes.

### `wal_sync_dir` and `wal_secondary_dir`

- `wal_sync_dir` enables the async WAL and DFS worker integration.
- `wal_secondary_dir` enables double writing for tail latency control.
- Both require extra disk space and IO bandwidth.

Operational guidance:

- Place `wal_sync_dir` on fast local SSD.
- Place `wal_secondary_dir` on a different disk if possible.

### `cli_mode`, `disable_compaction`, and `max_batch_size`

- `cli_mode` skips durable sync on the sync writer to speed CLI tools.
- `disable_compaction` disables background compaction (tests and debugging).
- `max_batch_size` rejects unexpectedly large batches and tags the region.

These should not be enabled in production servers unless you understand the
durability and retention implications.

### `wal_chunk_target_file_size`

- Controls the size of WAL chunks uploaded to object storage.
- Larger chunks reduce the number of objects but increase retry cost.
- Smaller chunks improve incremental recovery but increase object count.

### `rlog_cache_capacity` and `rlog_cache_size_threshold`

- Only used when lightweight backup is enabled.
- Cache capacity is a memory budget for snapshot acceleration.
- The size threshold controls which rlog files are cached.

### `enable_compact_rate_limiter` and `compact_bytes_per_sec`

- Rate limiting is applied to rlog file writes during compaction.
- Useful when compaction IO interferes with foreground traffic.
- If enabled, monitor compaction lag and write throttle metrics.

### `write_throttle_duration`

- Adds backpressure when compaction is close to overwrite boundary.
- If you see frequent throttling, compaction is the bottleneck.

### `dfs_worker_memory_limit`

- Limits in-flight WAL chunk upload memory.
- If too low, uploads fail with `MemoryLimitExceed` and worker goes unhealthy.
- If too high, memory pressure may impact raft or other components.

## Observability and key metrics

RfEngine exposes a focused set of metrics for operational health.
These are the primary signals to watch during incidents.

- `raft_engine_pending_compaction_wals`:
  - Number of WAL epochs waiting for compaction.
  - Growth indicates compaction lag and potential throttling.
- `raft_engine_write_throttle_duration_seconds`:
  - Time spent throttling WAL writes due to compaction lag.
- `raft_engine_double_write_healthy`:
  - 0 indicates a double writer has fallen behind and is disabled.
- `raft_engine_dfs_worker_healthy`:
  - 0 indicates lightweight backup is unhealthy.
- `raft_engine_dfs_running_uploads`:
  - Tracks concurrent upload count and memory pressure.
- `raft_engine_compact_wal_duration_seconds`:
  - Compaction time per epoch; watch for long tails.
- `raft_engine_take_snapshot_duration_seconds`:
  - Snapshot preparation time; influences DFS health recovery.

For debugging, log messages include the engine id and region id for context.

## Operational playbooks

### Compaction lag and write throttling

Symptoms:

- `pending_compaction_wals` grows.
- `write_throttle_duration` histogram accumulates high values.

Actions:

- Check disk IO saturation on the rfengine data path.
- Reduce `delay_compaction_epoches` to speed compaction.
- Increase compaction concurrency or enable rate limiter carefully.

### Double writer unhealthy

Symptoms:

- `raft_engine_double_write_healthy` drops to 0.

Actions:

- Inspect IO latency on the secondary WAL path.
- Verify that WAL directories are on separate physical devices.
- Consider disabling secondary writer if it is consistently slow.

### DFS worker unhealthy

Symptoms:

- `raft_engine_dfs_worker_healthy` drops to 0.
- Backup endpoints return `DfsWorkerUnhealthy` errors.

Actions:

- Check S3 availability and error logs.
- Verify WAL chunk uploads are not failing due to memory limit.
- Wait for the next snapshot boundary to recover health.

### WAL corruption on startup

Symptoms:

- Errors indicating checksum mismatch in non-last epoch.

Actions:

- Confirm whether the corrupted epoch is the last WAL file.
- If not the last epoch, recovery will fail by design.
- Investigate disk and filesystem integrity.

## Implementation gotchas for maintainers

### Changing the WAL format

If you modify WAL encoding, you must:

- Bump WAL version and update `WalHeader::decode`.
- Update `WalIterator` logic to handle the new format.
- Add forward/backward compatibility tests.

### Changing rlog format

If you modify rlog encoding, you must:

- Update `RlogHeader` and `decode` logic.
- Update snapshot assembly to include the correct meta.
- Ensure `load_raft_log_file` can read old files if required.

### Modifying MANIFEST fields

- Always preserve change set ordering semantics.
- Ensure rewrite logic preserves new fields.
- Update `lightweight_restore` and snapshot filtering if needed.

### Adjusting epoch rotation

- Consider how `epoch_rotate_len` interacts with DFS worker overwrites.
- Ensure overwrite checks use the same semantics in service and DFS workers.

### DFS worker task flow

- `ObjectStorageTask::Sync` is assumed to be monotonic in (epoch, offset).
- If you send out-of-order sync tasks, DFS worker may mark unhealthy.

## Appendix A: Data format cheat sheets

### PeerBatch encoding

```
PeerBatch
  peer_id (u64)
  region_id (u64)
  truncated_idx (u64)
  first_index (u64)
  end_index (u64)        # end = last_index + 1
  states_len (u32)
  repeated:
    key_len (u16)
    key bytes
    val_len (u32)
    val bytes
  repeated log_end_offset (u32)
  repeated RaftLogOp bytes
```

Notes:

- `end_index - first_index` determines log count.
- `log_end_offset` array enables fast slicing during decode.

### WAL batch payload

```
BatchPayload
  compression_flag (u32)  # 0 or 1
  [orig_len (u32)]        # if flag == 1
  [compressed bytes]      # if flag == 1
  [raw bytes]             # if flag == 0
```

### WAL chunk key

```
store_backup/<store_id>/wal_chunks/
  e<epoch>_<start_off>_<end_off>.wal
  e<epoch>_<start_off>_<end_off>.wal.last
```

### Snapshot key

```
store_backup/<store_id>/snapshots/
  m<delayed_to_epoch>.meta
  r<delayed_to_epoch>.rlog
```

## Appendix B: Example timelines

### WAL rotation and compaction timeline

```
Epoch 10 written (sync WAL)
  -> ServiceTask::Rotate(10)
  -> CompactWorker compacts epoch 10 - delay
  -> Manifest epoch advances
  -> rlog files for epoch are created
```

### Lightweight backup timeline

```
Async WAL write
  -> DFS worker Sync(epoch, off)
  -> buffer grows
  -> chunk flushed to S3
  -> epoch rotates
  -> .last chunk uploaded
  -> snapshot prepared at epoch boundary
  -> snapshot uploaded after WAL chunks complete
```

## Appendix C: Useful code entry points

- `components/rfengine/src/engine.rs`:
  - `RfEngine::open`, `write`, `persist`, `apply`, `backup`
- `components/rfengine/src/writer.rs`:
  - `WalWriter`, `WalHeader`, `DoubleWriter`
- `components/rfengine/src/compact_worker.rs`:
  - `CompactWorker::compact`, `write_raft_log_files`
- `components/rfengine/src/dfs_worker.rs`:
  - `ObjectStorageWorker::run`, `handle_sync`, `persist_snapshot`
- `components/rfengine/src/manifest.rs`:
  - `Manifest::open`, `handle_compaction`, `rewrite`

---

If you change behavior that affects durability or recovery,
update this document along with the code change.
