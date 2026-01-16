# RooDB Locking Architecture

This document describes the synchronization primitives and locking conventions used in RooDB.

## Design Philosophy

RooDB uses a **Raft-as-WAL** architecture that naturally minimizes lock contention:
- Writes are buffered in session memory until COMMIT
- On COMMIT, changes are proposed to Raft
- Storage writes only occur in `apply()` after Raft consensus
- This means DML operations (INSERT/UPDATE/DELETE) don't hold database locks during execution

## Synchronization Primitives

### Lock Types Used

| Type | Library | Use Case |
|------|---------|----------|
| `RwLock` | parking_lot | Sync contexts (catalog, txn state, raft caches) |
| `Mutex` | tokio::sync | Async contexts where lock is held across await (LSM manifest) |
| `Mutex` | parking_lot | Short sync critical sections (I/O handles) |
| `Atomic*` | std::sync::atomic | Counters and flags |

### Why parking_lot?

- No lock poisoning (panics don't leave locks in invalid state)
- Better performance than std::sync
- Simpler API (no `.unwrap()` needed)

### Why tokio::sync::Mutex for manifest?

The LSM manifest lock is held across `await` points during flush/compaction. Using `parking_lot::Mutex` would block the async runtime. `tokio::sync::Mutex` is designed for this use case.

## Module Locking Inventory

### Server (`src/server/`)

| Component | Type | Purpose |
|-----------|------|---------|
| `next_conn_id` | AtomicU32 | Connection ID allocation |

No locks held during connection spawn.

### Protocol (`src/protocol/roodb/`)

| Component | Type | Purpose |
|-----------|------|---------|
| `catalog` | parking_lot::RwLock | Schema access for planning |

**Critical pattern:** Catalog lock is acquired in a closure and dropped before any `await`:
```rust
let plan_result = (|| {
    let catalog_guard = self.catalog.read();
    // ... planning ...
})();  // Guard dropped here, before await
self.execute_plan(physical).await
```

### Catalog (`src/catalog/`)

| Component | Type | Purpose |
|-----------|------|---------|
| `Catalog` | parking_lot::RwLock | Tables/indexes HashMap |

Always acquired alone (never nested with other locks).

### Transaction (`src/txn/`)

| Component | Type | Purpose |
|-----------|------|---------|
| `next_txn_id` | AtomicU64 | Transaction ID generation |
| `active_transactions` | parking_lot::RwLock | Active txn tracking |
| `committed_txns` | parking_lot::RwLock | Visibility checks |
| `timeout_config` | parking_lot::RwLock | Timeout settings |
| `is_leader` | AtomicBool | Raft leader status |
| `undo_log.records` | parking_lot::RwLock | Undo records |
| `undo_log.roll_ptr_index` | parking_lot::RwLock | Roll pointer lookup |
| `next_roll_ptr` | AtomicU64 | Roll pointer generation |

Multiple RwLocks but NEVER nested - each acquired/released independently.

### Storage (`src/storage/lsm/`)

| Component | Type | Purpose |
|-----------|------|---------|
| `memtable.data` | parking_lot::RwLock | In-memory KV store |
| `memtable.size` | AtomicUsize | Approximate size tracking |
| `imm_memtables` | parking_lot::RwLock | Pending flush memtables |
| `manifest` | tokio::sync::Mutex | SSTable metadata |
| `closed` | AtomicBool | Engine shutdown flag |

**Critical pattern:** Manifest lock is released before compaction:
```rust
if needs_l0_compaction(&manifest) {
    drop(manifest);  // Release before async work
    self.maybe_compact().await?;
}
```

### I/O (`src/io/`)

| Component | Type | Purpose |
|-----------|------|---------|
| `UringIO::ring` | parking_lot::Mutex | io_uring queue |
| `PosixIO::file` | parking_lot::Mutex | POSIX file handle |

Single lock per I/O instance; never nested.

### Raft (`src/raft/`)

| Component | Type | Purpose |
|-----------|------|---------|
| `cached_vote` | parking_lot::RwLock | Vote state cache |
| `cached_last_applied` | parking_lot::RwLock | Last applied log ID |
| `cached_membership` | parking_lot::RwLock | Cluster membership |
| `cached_last_log_id` | parking_lot::RwLock | Last log entry |
| `cached_last_purged` | parking_lot::RwLock | Last purged log ID |
| `catalog` | parking_lot::RwLock | Schema updates in apply() |
| `nodes` | parking_lot::RwLock | Peer addresses |

**Critical pattern:** Async work (storage scans) done before catalog lock:
```rust
// Scan storage first (no lock)
let table_defs = scan_system_tables().await;

// Then acquire lock for update (no await after this)
let mut catalog = self.catalog.write();
// ... update catalog ...
```

## Lock Ordering Rules

1. **Never hold sync locks across await** (except tokio::sync::Mutex)
2. **Catalog acquired alone** - no other locks held when catalog is locked
3. **Independent subsystem locks** - txn, storage, raft don't hold each other's locks
4. **No circular dependencies** - call graph naturally orders acquisitions

## Atomic Ordering Guidelines

| Use Case | Ordering |
|----------|----------|
| Counter increment (ID allocation) | SeqCst |
| Approximate size tracking | Relaxed |
| Leader flag | SeqCst |
| Closed/shutdown flags | SeqCst |

## Row ID Generation

All row IDs are generated from a single global counter (`src/storage/row_id.rs`):
```rust
pub fn next_row_id() -> u64 {
    GLOBAL_ROW_ID_GEN.next()
}
```

This ensures globally unique row IDs across all operations (INSERT, DDL, auth).

## Adding New Locks

When adding a new lock:

1. **Prefer parking_lot** for sync contexts
2. **Use tokio::sync::Mutex** only if lock must be held across await
3. **Document the lock** in this file
4. **Never nest** with existing locks unless absolutely necessary
5. **Keep critical sections short** - do work outside the lock when possible
6. **Follow the closure pattern** for catalog access to ensure guard is dropped
