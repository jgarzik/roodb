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
| `compaction_mutex` | tokio::sync::Mutex | Serializes compactions |
| `closed` | AtomicBool | Engine shutdown flag |

**Critical pattern:** Atomic memtable snapshots prevent rotation races:

Both `read_all()` and `scan()` must read the active memtable and immutable memtables atomically
to prevent entries from becoming temporarily invisible during memtable rotation:
```rust
// CRITICAL: Both must be read while holding memtable lock
let (memtable_snapshot, imm_snapshot) = {
    let mem_guard = self.memtable.read();
    let memtable_snapshot = Arc::clone(&*mem_guard);
    let imm_snapshot: Vec<Arc<Memtable>> = self.imm_memtables.read().clone();
    (memtable_snapshot, imm_snapshot)
};
// Use snapshots outside lock for actual reads
```

Without this, a concurrent `put()` could rotate the memtable between reading the two
structures, causing an entry to be missed (moved from memtable to imm_memtables
between our reads).

**Critical pattern:** Flush removes from imm_memtables only AFTER disk write:

When flushing memtables to disk, entries must remain visible in `imm_memtables`
until the disk write completes. Otherwise there's a visibility gap:
```rust
// WRONG: Entries invisible during flush
let imm_mems = std::mem::take(&mut *imm_memtables);  // Entries gone!
for mem in imm_mems {
    flush_to_disk(mem).await;  // Not on disk yet - entries invisible
}

// CORRECT: Entries visible until disk write completes
loop {
    let mem = imm_memtables.first().cloned();  // Peek, don't remove
    flush_to_disk(mem).await;  // Now on disk
    imm_memtables.remove(0);  // Safe to remove
}
```

**Critical pattern:** Compaction uses split-phase locking:
```rust
// Phase 1: Brief lock to prepare job
let job = {
    let mut manifest = self.manifest.lock().await;
    prepare_l0_compaction(&mut manifest)
    // Lock released here
};
// Phase 2: I/O without lock
let new_file = execute_l0_compaction(&factory, &job).await?;
// Phase 3: Re-acquire lock to finalize
let mut manifest = self.manifest.lock().await;
finalize_l0_compaction(&mut manifest, &job, new_file)?;
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
| `log_mutex` | tokio::sync::Mutex | Serializes log operations |
| `cached_vote` | parking_lot::RwLock | Vote state cache |
| `cached_last_applied` | parking_lot::RwLock | Last applied log ID |
| `cached_membership` | parking_lot::RwLock | Cluster membership |
| `cached_last_log_id` | parking_lot::RwLock | Last log entry |
| `cached_last_purged` | parking_lot::RwLock | Last purged log ID |
| `catalog` | parking_lot::RwLock | Schema updates in apply() |
| `nodes` | parking_lot::RwLock | Peer addresses |

**Critical pattern:** Log mutex serializes log operations to establish memory barrier ordering:

`LsmRaftStorage` is `Clone` with shared `Arc` fields. OpenRaft's `&mut self` methods imply
serialized access, but clones can call methods concurrently. The `log_mutex` ensures that
when `get_log_state()` reports log entry N exists, `try_get_log_entries()` will find it.

Without the mutex, the separate locks for storage (memtable) and cache (cached_last_log_id)
don't establish happens-before ordering across operations:
```rust
// Thread A: append()
storage.put(entry_49).await;    // Acquires memtable lock
cached_last_log_id = 49;        // Acquires cache lock

// Thread B: get_log_state() then try_get_log_entries()
// Without log_mutex, Thread B could see cache=49 but miss the memtable entry
```

**Critical pattern:** Snapshot methods protected by log_mutex for consistency:

The `applied_state()`, `get_current_snapshot()`, and `build_snapshot()` methods all acquire
`log_mutex` to ensure they see a consistent view of cached state alongside log operations.
Without this, concurrent log appends could modify cached_last_log_id while these methods
read cached_last_applied, leading to inconsistent state views under high concurrency.

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
