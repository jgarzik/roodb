# RooDB Locking Architecture

This document describes the synchronization primitives and locking conventions used in RooDB.

## Design Philosophy

RooDB uses a **Raft-as-WAL** architecture that naturally minimizes lock contention:
- Writes are buffered in session memory until COMMIT
- On COMMIT, changes are proposed to Raft and appended to the on-disk Raft log
- Storage writes only occur in `apply()` after Raft consensus, one atomic batch per log entry
- This means DML operations (INSERT/UPDATE/DELETE) don't hold database locks during execution

Transactions lock the rows they change, so that writers wait for each other
instead of colliding at commit (see Row locks below). Those are logical
locks between transactions, held across statements; everything else in this
document is about short-lived synchronization inside the server. Rows of
temporary tables never leave the session that owns them.

## Synchronization Primitives

### Lock Types Used

| Type | Library | Use Case |
|------|---------|----------|
| `RwLock` | parking_lot | Sync contexts (catalog, txn state, raft caches, memtables, manifest) |
| `Mutex` | tokio::sync | Async contexts where lock is held across await (flush, compaction, manifest write) |
| `Mutex` | parking_lot | Short sync critical sections (block cache shards, Raft log index) |
| `Notify`, channels | tokio::sync | Waking background tasks; handing work to the log writer |
| `Atomic*` | std::sync::atomic | Counters and flags |

### Why parking_lot?

- No lock poisoning (panics don't leave locks in invalid state)
- Better performance than std::sync
- Simpler API (no `.unwrap()` needed)

### Why tokio::sync::Mutex for flush, compaction and the manifest?

`flush_lock`, `compaction_lock` and `manifest_io` are held across `await` points (file I/O). Using `parking_lot::Mutex` would block the async runtime. `tokio::sync::Mutex` is designed for this use case.

## Module Locking Inventory

### Server (`src/server/`)

| Component | Type | Purpose |
|-----------|------|---------|
| `next_conn_id` | AtomicU32 | Connection ID allocation |

No locks held during connection spawn.

### Query engine (`src/server/query/`, `src/server/temp_store.rs`)

| Component | Type | Purpose |
|-----------|------|---------|
| `catalog` | parking_lot::RwLock | Schema access for planning |
| `SessionStorage::temp` | parking_lot::RwLock | One session's temporary tables and their rows |
| `SessionStorage::has_temp` | AtomicBool | Lets sessions without temporary tables skip the lock |
| session user variables | parking_lot::RwLock | `@variables`, and the session's copy of the stored functions |

`SessionStorage::temp` is never held across an `await`: reads of shared
storage happen before or after it is taken.

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
| `memtable` | parking_lot::RwLock | The active memtable. Writers insert under the read lock; rotation takes the write lock |
| memtable contents | lock-free skip list + AtomicUsize size | In-memory KV store |
| `imm_memtables` | parking_lot::RwLock | Frozen memtables waiting to be written out, oldest first |
| `manifest` | parking_lot::RwLock | Which tables exist at which level (in memory) |
| `manifest_io` | tokio::sync::Mutex | Held while the manifest is changed and written, so changes reach disk in order |
| `flush_lock` | tokio::sync::Mutex | Memtables reach disk one at a time, oldest first |
| `compaction_lock` | tokio::sync::Mutex | One compaction at a time |
| `reader_cache` | parking_lot::RwLock | An open reader for every table in the manifest |
| block cache shards | parking_lot::Mutex each | LRU of SSTable blocks, split into shards to spread contention |
| `work`, `compact_work`, `flushed` | tokio::sync::Notify | Wake the flush task, the compaction task, and stalled writers |
| `background_error` | parking_lot::Mutex | Last failure of a background task, reported to stalled writers |
| `dir_lock` | parking_lot::Mutex over a locked file | Exclusive lock on the data directory |
| `stopping`, `closed` | AtomicBool | Shutdown flags |

Lock order where more than one is held: `flush_lock` or `compaction_lock` →
`manifest_io` → `manifest` (briefly). `memtable` is taken before
`imm_memtables`.

**Critical pattern:** A write batch is atomic with respect to rotation.

`write_batch()` inserts every entry of a batch while holding the `memtable`
read lock; rotation needs the write lock. A batch therefore lands in one
memtable, and a flush can never capture half of it. The Raft state machine
relies on this: each log entry is one batch that includes the applied
position, so what is on disk is always an exact prefix of the log.

**Critical pattern:** Atomic memtable snapshots prevent rotation races:

Both `get()` and `scan()` must read the active memtable and the frozen
memtables under the memtable lock, so an entry cannot be missed while it moves
from one to the other:
```rust
let (memtable_snapshot, imm_snapshot) = {
    let mem_guard = self.memtable.read();
    let memtable_snapshot = Arc::clone(&*mem_guard);
    let imm_snapshot: Vec<Arc<Memtable>> = self.imm_memtables.read().clone();
    (memtable_snapshot, imm_snapshot)
};
// Use snapshots outside lock for actual reads
```

**Critical pattern:** A frozen memtable leaves `imm_memtables` only after its
table is on disk and in the manifest. Until then its entries stay readable
from memory; there is no moment when they are in neither place.

**Critical pattern:** Flush and compaction do their I/O without the manifest lock.

Each picks its work under a brief `manifest` read lock, reads and writes files
with only its own tokio mutex held, and then makes one manifest change under
`manifest_io`: the new manifest is written and synced first, the in-memory
manifest is swapped second, and input files are deleted last. A crash at any
point leaves either the old state or the new one; leftovers are removed when
the directory is next opened.

Flush and compaction run in separate background tasks with separate locks, so
a long compaction never holds up a flush that writers are waiting for. Writers
stall (without holding any lock) while four frozen memtables are waiting.

### I/O (`src/io/`)

| Component | Type | Purpose |
|-----------|------|---------|
| uring reactor channel | std::sync::mpsc | Requests from tasks to a reactor thread; no lock held across I/O |
| posix `Shared::queues` | parking_lot::Mutex + Condvar | Worker pool request queues |

Each io_uring ring is owned by one reactor thread and is never shared, so it
needs no lock. Neither backend holds a lock while a request is in the kernel.

### Raft (`src/raft/`)

| Component | Type | Purpose |
|-----------|------|---------|
| `LogStore` `mem` | parking_lot::Mutex | Index of log entries, recent entries, vote, purge and commit points |
| `LogStore` command channel | tokio mpsc (unbounded) | Appends and other log changes, handed to the single writer task |
| `WriterShared::files` | parking_lot::Mutex | Open segment files, for reading entries back |
| `WriterShared::written` | parking_lot::Mutex | How far the log has reached disk (segment, offset) |
| `WriterShared::failed` | parking_lot::Mutex | First write error; once set the log refuses further work |
| `preparing_spare` | AtomicBool | Only one task prepares the spare segment |
| `cached_last_applied` | parking_lot::RwLock | Last applied log ID |
| `cached_membership` | parking_lot::RwLock | Cluster membership |
| `applied_txn_floor`, `stored_txn_bound` | AtomicU64 | Highest applied transaction ID and the bound stored for restart |
| `catalog` | parking_lot::RwLock | Schema updates in apply() |
| `nodes` | parking_lot::RwLock | Peer addresses |

**Critical pattern:** One task writes the log.

Appends do not write. They record the entry in `mem`, send it to the writer
task, and return; the writer drains everything queued, writes it with one
request (plus a flush with `--durability fsync`) and then reports each append
as complete. No lock is held while the write is in the kernel, and commits
that arrive together share the write. Nothing in the log store is locked
across an `await`.

**Critical pattern:** The state machine is driven by one task.

OpenRaft applies entries, builds snapshots and installs them from a single
task, so the state machine needs no lock of its own for ordering. The cached
position is updated after the storage batch that carries it has been written.

**Critical pattern:** Async work (storage scans) done before catalog lock:
```rust
// Scan storage first (no lock)
let table_defs = scan_system_tables().await;

// Then acquire lock for update (no await after this)
let mut catalog = self.catalog.write();
// ... update catalog ...
```

### Row locks (`src/txn/lock.rs`)

`LockManager` is the table of row locks that transactions hold on each
other's behalf: shared and exclusive record locks on row keys, gap locks on
open key intervals, and insert intentions that wait for gap locks. Requests
on a record are granted in arrival order; a request that would close a cycle
of waiting transactions is refused (deadlock), and a wait can time out.
Everything a transaction holds is released together when it ends.

The whole table is guarded by one `parking_lot::Mutex`, held only to inspect
or change the table, never while a requester waits: a waiter parks on a
oneshot channel outside the mutex. No other lock is taken while it is held.
The locks exist only in memory on the node that granted them; they are not
replicated.

The lock manager belongs to the `TransactionManager`, which releases a
transaction's locks when it commits or rolls back, whichever path ends it.
`UPDATE`, `DELETE` and `INSERT` take record locks through the executor's
`TransactionContext` and then read the row as it stands (`get_current`),
not through the statement's snapshot. A deadlock is reported as error 1213
and the transaction rolled back; a timed-out wait (`innodb_lock_wait_timeout`)
as error 1205, failing the statement only.

Under REPEATABLE READ a statement also locks the gaps it read: a table scan
takes one gap lock over the table's whole key range before it reads; a
primary-key lookup that finds no row locks the gap between the neighbouring
rows (`MvccStorage::gap_around`, built on the storage engine's `next_key` /
`prev_key`) and gives back the record lock it took to look. Under READ
COMMITTED there are no gap locks, and a scanned row that does not match is
unlocked at once (`unlock_record`).

A statement that locks a range also locks the keys in it that other
transactions hold record locks on (`LockManager::others_locked_in`): a row
being inserted is not stored yet, and waiting on its key is how the reader
waits for it. An insert (or an `UPDATE` that changes the primary key) locks
its key, then checks the gap, and if the gap is locked gives the key back
before it waits (`TransactionContext::lock_for_insert`): the gap's holder
may want that key, and comes first.

`SELECT ... FOR UPDATE` / `FOR SHARE` lock in the scan executors
(`TableScan`, `RangeScan`, `PointGet`) through
`TransactionContext::locking_scan` / `locked_read`: gap first, then each row,
each read as it stands after it is locked. `NOWAIT` and `SKIP LOCKED` use
`try_lock_record`, which never waits. A locking read outside a transaction
runs in an implicit one that ends, releasing its locks, when the result has
been sent.

## Lock Ordering Rules

1. **Never hold sync locks across await** (except tokio::sync::Mutex)
2. **Catalog acquired alone** - no other locks held when catalog is locked
3. **Independent subsystem locks** - txn, storage, raft don't hold each other's locks
4. **No circular dependencies** - call graph naturally orders acquisitions

## Atomic Ordering Guidelines

| Use Case | Ordering |
|----------|----------|
| Transaction/row ID allocation | SeqCst |
| Connection ID allocation | Relaxed (uniqueness only, no ordering requirement) |
| Approximate size tracking | Relaxed |
| Leader flag | SeqCst |
| Closed/shutdown flags | SeqCst |

## Row ID Generation

Row IDs are cluster-unique via node-ID-in-high-bits encoding (`src/storage/row_id.rs`):

- **Format:** `[16-bit node_id][48-bit local counter]` — supports 65536 nodes, 281T rows/node
- **`NODE_ID: AtomicU64`** — static, set once at startup from the Raft node ID (SeqCst)
- **`GLOBAL_ROW_ID_GEN: RowIdGenerator`** — single global counter (`fetch_add`) plus a `limit`: the end of the range this node has reserved
- **`next_row_id()`** / **`allocate_row_id_batch(count)`** — never block and never wait on Raft

Ranges are reserved through Raft (`Command::ReserveRowIds`) ahead of use. An
ID may be handed out beyond the recorded reservation, but a commit that uses
one waits until a reservation covering it has been applied. After a restart
the counter resumes above everything reserved, so an ID that reached storage
is never issued again.

## Adding New Locks

When adding a new lock:

1. **Prefer parking_lot** for sync contexts
2. **Use tokio::sync::Mutex** only if lock must be held across await
3. **Document the lock** in this file
4. **Never nest** with existing locks unless absolutely necessary
5. **Keep critical sections short** - do work outside the lock when possible
6. **Follow the closure pattern** for catalog access to ensure guard is dropped
