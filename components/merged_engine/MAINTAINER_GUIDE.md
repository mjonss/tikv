# Merged Engine Maintainer Guide

## Scope and assumptions
- This document explains the non-obvious design and invariants of merged_engine.
- It assumes familiarity with TiKV Raft log structure and rfengine concepts.
- It focuses on how we merge WAL from multiple stores into one synthetic store.
- It avoids restating trivial config fields and struct definitions.

## Purpose and mental model
- Merged_engine is a local, synthetic store used by replication_worker.
- It replays upstream Raft logs into a single rfengine+kvengine instance.
- Each region in the merged store has exactly one peer whose ID equals the
  region ID and whose store ID equals `merged_store_id`.
- It is not a Raft participant; it is a deterministic replay engine.
- Commit decisions are inferred by observing the same log index from multiple
  upstream stores (quorum heuristic).

## High-level architecture
- Rfengine stores the merged Raft log and region state.
- Kvengine stores the applied data (the snapshot visible to CDC clients).
- A manifest tracks store progress, keyspace states, and uncommitted entries.
- Preprocessors translate upstream Raft entries into merged-store semantics.
- Appliers reuse rfstore apply logic to update kvengine.

## Key data structures and why they exist
### StoreProgress
- Tracks `(epoch, offset)` per upstream store to ensure WAL is applied in order.
- Enforces strict monotonicity when ingesting WAL chunks.

### RegionProgress
- Tracks per-region log entries, commit index, and synced index.
- `entries` contains a `RaftLogOpWithCounter` per log index.
- The counter increments when the same log index is seen from multiple stores.
- When the counter reaches quorum, `commit_index` can advance.
- `synced_index` is the last log index actually applied to kvengine.

### TruncatedIndex
- Records the upstream truncated index plus the commit index that justified it.
- Prevents unsafe truncation before merged_engine has applied enough data.
- Required because we cannot derive truncation safety from kvengine alone when
  shards are not loaded.

### Manifest
- Persists store progress, keyspace states, and uncommitted entries.
- Stores `synced_target_ts` so WAL target selection can continue correctly.
- Uses a checksum to guard against partial writes.
- `uncommitted_entries` allows recovery of in-flight logs after restart.

### Manifest persistence timing
- `update_wal` mutates progress in memory but does not persist the manifest.
- The manifest is persisted at the end of `sync_merged` after apply completes.
- A crash before `sync_merged` simply discards in-memory WAL progress and
  triggers a fresh fetch on restart, which is safe.

### Preprocessor
- Rewrites Raft entries so region metadata matches the merged store.
- Tracks pending merge state and other apply-time metadata.
- Decrypts per-shard encryption keys so the applier can read old values.

## WAL ingestion (update_wal)
### Ordering and validation
- `update_wal` compares incoming `(epoch, start_off)` with stored progress.
- A mismatch is treated as fatal because it implies WAL reordering.
- The method updates store progress only after all batches are ingested.

### Per-batch processing
- WAL chunks are decoded into rfengine `WriteBatch` instances.
- For each region in the batch:
  - If the region is tombstone or uninitialized, it is skipped.
  - A new `RegionProgress` is created when the region is first seen.
  - The log entry is inserted or updated in the region's `entries` map.
  - The entry counter may advance the region commit index.

### Why we track entry counters
- The merged store has no Raft quorum of its own.
- We approximate commit by observing the same log index from multiple stores.
- The counter provides a cheap quorum signal without building a new consensus.

### Truncated index handling
- Upstream truncate indices are captured when available in a write batch.
- Truncated indices are ignored if the peer was restored from snapshot.
- The commit index from the same store is recorded to validate truncation.

## WAL rotation
- `rotate_wal` increments the epoch only when current progress exactly matches
  the rotation boundary.
- This preserves strict ordering across epochs and avoids gaps.

## Sync pipeline (sync_merged)
### Overview
- `sync_merged` applies committed entries from updated regions.
- It is the only path that advances `synced_index` and persists progress.
- It batches Raft state writes to bound latency and memory.

### Region processing loop
- Updated regions are placed in a queue and processed iteratively.
- Regions may be postponed when merge dependencies are not ready.
- Postponed regions are re-queued to the next sync cycle.

### Preprocessing and admin commands
- Each entry is rewritten by `update_entry` to match merged-store metadata.
- `prepare_merge` and `commit_merge` entries are rewritten with single-peer
  region meta.
- `commit_merge` is postponed until the source region's merge state is ready.
- Pending merge states are tracked across regions to enforce ordering.

### Applying entries to kvengine
- The preprocessor builds a `PreprocessContext` and generates apply messages.
- Each region has an `Applier` that uses rfstore logic in replication mode.
- Apply is skipped when the shard is not loaded (region outside keyspace set).
- The applier may pause; prepared messages are buffered and replayed later.

### Why we buffer prepared messages
- Some apply flows require interleaving region-level messages (e.g. merges).
- The router delivers these messages asynchronously.
- Buffering avoids deadlocks when another region's messages arrive first.

### Persisting Raft state
- Each region's raft state is updated as entries are processed.
- Batched `WriteBatch` objects are written to rfengine once size thresholds
  are exceeded.

## Progress updates and truncation
### Updating in-memory progress
- After syncing, entries at or below `synced_index` are dropped from memory.
- Uncommitted entries are retained for the next sync round.

### Truncation rules
- Truncation is allowed only when `synced_index` has passed the commit index
  that justified the upstream truncation.
- Regions with dependents are not truncated to avoid breaking parent recovery.
- Truncation respects the persisted log index and never truncates beyond it.

### Why dependents block truncation
- Parent regions can depend on child logs during merges or split recovery.
- Removing logs too early can break merge replay or recovery correctness.

## Region destruction
- Tombstone regions are removed from raft state and kvengine shards.
- If a region still has dependents, destruction is delayed.
- A delayed region is destroyed once its dependents are removed.
- Destroyed regions are marked as `TRUNCATE_ALL_INDEX` to prevent reuse.

## Recovery flows
### Recovery from backup
- Used when the manifest has no stored progress (fresh or wiped state).
- The latest backup meta is fetched from object storage.
- Per-store rfengine instances are restored from snapshot + WAL chunks.
- For each region:
  - Region state and raft state are loaded from the origin store.
  - Logs from the origin are collected into `RegionProgress`.
  - Commit index is inferred from counters across stores.
- A merged raft log is built and written into the local rfengine.
- The original per-store raft directories are deleted afterward.

### Recovery from merged rfengine
- Used when the manifest already has store progress.
- Region state and raft state are read from merged rfengine.
- `RegionProgress` is rebuilt from raft state and uncommitted entries.
- If commit index exceeds synced index, the missing entries are fetched so that
  `sync_merged` can continue safely.
- Tombstone regions are recorded for destruction during startup.

### Why uncommitted entries are persisted
- The worker may crash after ingesting WAL but before applying it.
- Persisting uncommitted entries preserves quorum counters and ordering.
- This avoids rewinding commit decisions after restart.

## Keyspace and shard loading
- Only shards whose keyspace IDs exist in the manifest are loaded.
- Snapshots are iterated via the recover handler to build shard metadata.
- Tombstone regions are skipped at load time and destroyed later.
- Meta packer exposes a compact keeper; replication_worker can pause compaction
  during shard preparation to avoid races with load.

## Encryption handling
- Shard meta includes encrypted data keys; these are decrypted by the
  preprocessor at startup.
- The applier uses the decrypted key when applying entries.
- This ensures old-value reads and writes use consistent encryption state.

## Invariants and failure modes
- Store progress must be monotonic; mismatches indicate WAL corruption.
- `commit_index` must never be less than `synced_index`.
- Tombstone or uninitialized regions are never applied to kvengine.
- `peer_is_restored_from_snapshot` avoids trusting truncated indices from
  snapshot-restored peers.

## Interfaces used by replication_worker
- `update_wal` ingests WAL bytes for a store and epoch.
- `rotate_wal` advances the store epoch after a full epoch is applied.
- `sync_merged` applies committed entries and persists the manifest.
- `load_shards` and `load_keyspace_shard_metas` manage shard metadata.
- `get_*` helpers expose store progress, synced target ts, and keyspace regions.

## Maintenance tips
- If sync stalls with postponed regions, inspect pending merge states and
  dependent counts for those regions.
- If manifest appears corrupted, verify checksum failures and check for
  partial writes or disk issues.
- If commit index never advances, verify that WAL logs from multiple stores
  are being ingested (quorum counter logic).
